"""Discovery Engine — Multi-source candidate gathering.

Discovers emerging artists by searching genres from the taste profile.
Sources:
  1. MusicBrainz tag search (free, no API key)
  2. Spotify search (for Spotify ID resolution)
  3. Last.fm similar artists + tag search (when API key available)
"""

import logging
import re
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import spotipy

import config
import db
import lastfm_client
import musicbrainz_client

logger = logging.getLogger(__name__)

# How many top genres to use for discovery
TOP_GENRES_FOR_DISCOVERY = 20
# How many artists to fetch per genre from MusicBrainz
ARTISTS_PER_GENRE = 50


def discover_from_musicbrainz(
    genre_weights: Dict[str, float],
    seed_ids: Set[str],
) -> List[Dict[str, Any]]:
    """Discover candidates by searching MusicBrainz for artists tagged with top genres.

    Args:
        genre_weights: Weighted genre map from taste profile
        seed_ids: Set of Spotify IDs for known/seed artists (to exclude)

    Returns:
        List of candidate dicts with: name, mb_id, genres, discovery_source, genre_weight
    """
    # Pick top genres, skipping non-music tags
    skip_tags = {"english", "british", "american", "canadian", "australian",
                 "german", "french", "swedish", "nuno", "2010s", "2000s",
                 "1990s", "1980s", "seen live", "favorites"}
    top_genres = [
        (genre, weight) for genre, weight in genre_weights.items()
        if genre.lower() not in skip_tags
    ][:TOP_GENRES_FOR_DISCOVERY]

    logger.info("Discovering from %d genres via MusicBrainz...", len(top_genres))

    candidates = {}  # name_lower -> candidate dict
    for genre, weight in top_genres:
        logger.info("  Searching genre: %s (weight=%.3f)", genre, weight)
        try:
            artists = _search_mb_tag(genre, limit=ARTISTS_PER_GENRE)
        except Exception as e:
            logger.warning("  MusicBrainz search failed for '%s': %s", genre, e)
            continue

        for artist in artists:
            name_key = artist["name"].lower().strip()
            # Skip "Various Artists" and similar
            if name_key in ("various artists", "[unknown]", "unknown artist"):
                continue

            if name_key in candidates:
                # Existing candidate found via another genre — boost source count
                existing = candidates[name_key]
                existing["discovery_sources"].add(f"mb_tag:{genre}")
                existing["genre_tags"].update(artist.get("tags", []))
            else:
                candidates[name_key] = {
                    "name": artist["name"],
                    "mb_id": artist.get("mb_id"),
                    "spotify_id": None,
                    "genres": list(artist.get("tags", [])),
                    "genre_tags": set(artist.get("tags", [])),
                    "discovery_sources": {f"mb_tag:{genre}"},
                    "discovery_genre_weight": weight,
                    "mb_score": artist.get("score", 0),
                }

    # Convert sets to lists for JSON serialization
    result = []
    for c in candidates.values():
        c["genres"] = list(c["genre_tags"])
        c["source_count"] = len(c["discovery_sources"])
        c["discovery_sources"] = list(c["discovery_sources"])
        del c["genre_tags"]
        result.append(c)

    logger.info("  MusicBrainz discovery: %d raw candidates from %d genres",
                len(result), len(top_genres))
    return result


def _search_mb_tag(tag: str, limit: int = 30) -> List[Dict[str, Any]]:
    """Search MusicBrainz for artists tagged with a genre."""
    import musicbrainzngs
    musicbrainz_client._rate_limit()

    results = musicbrainzngs.search_artists(tag=tag, limit=limit)
    artists = []
    for a in results.get("artist-list", []):
        tags = [t["name"].lower() for t in a.get("tag-list", [])]
        artists.append({
            "name": a["name"],
            "mb_id": a["id"],
            "tags": set(tags),
            "score": int(a.get("ext:score", 0)),
        })
    return artists


def discover_from_spotify_search(
    sp: spotipy.Spotify,
    genre_weights: Dict[str, float],
    seed_ids: Set[str],
) -> List[Dict[str, Any]]:
    """Discover candidates by searching Spotify for genre keywords.

    Spotify search works in dev mode even though related artists / recommendations don't.
    We search for artists using top genre names as keywords.
    """
    # Use fewer genres for Spotify search (it's less precise than MB tags)
    skip_tags = {"english", "british", "american", "canadian", "australian",
                 "german", "french", "swedish", "nuno", "2010s", "2000s",
                 "1990s", "1980s", "seen live", "favorites"}
    top_genres = [
        (genre, weight) for genre, weight in genre_weights.items()
        if genre.lower() not in skip_tags
    ][:10]

    logger.info("Discovering from %d genres via Spotify search...", len(top_genres))

    from spotify_client import _count_request, SpotifyRateLimitError

    candidates = {}
    for genre, weight in top_genres:
        try:
            _count_request("search")
            # Search Spotify with genre as keyword (genre: filter blocked in dev mode)
            result = sp.search(q=genre, type="artist", limit=10)
            items = result.get("artists", {}).get("items", [])

            for item in items:
                sid = item["id"]
                if sid in seed_ids:
                    continue
                name_key = item["name"].lower().strip()
                if name_key in ("various artists",):
                    continue

                if name_key in candidates:
                    candidates[name_key]["discovery_sources"].add(f"spotify_search:{genre}")
                else:
                    candidates[name_key] = {
                        "name": item["name"],
                        "mb_id": None,
                        "spotify_id": sid,
                        "genres": [],  # Spotify dev mode doesn't return genres
                        "discovery_sources": {f"spotify_search:{genre}"},
                        "mb_score": 50,  # neutral default
                    }

        except SpotifyRateLimitError as e:
            logger.warning("Approaching rate limit during Spotify discovery: %s", e)
            break
        except Exception as e:
            logger.warning("Spotify search failed for genre '%s': %s", genre, e)

    result = []
    for c in candidates.values():
        c["source_count"] = len(c["discovery_sources"])
        c["discovery_sources"] = list(c["discovery_sources"])
        result.append(c)

    logger.info("  Spotify search discovery: %d candidates from %d genres",
                len(result), len(top_genres))
    return result


def discover_from_lastfm(
    genre_weights: Dict[str, float],
    seed_artists: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Discover candidates via Last.fm similar artists and tag search.

    Uses two strategies:
    1. Similar artists for top seed artists (2nd degree connections)
    2. Tag/genre top artists (direct genre search)

    Returns empty list if LASTFM_API_KEY is not configured.
    """
    if not config.LASTFM_API_KEY:
        logger.info("Last.fm API key not configured — skipping Last.fm discovery")
        return []

    candidates = {}

    # Strategy 1: Similar artists for top seed artists
    # Use top 15 seed artists (by recency/importance)
    top_seeds = seed_artists[:15]
    logger.info("Discovering similar artists for %d seeds via Last.fm...", len(top_seeds))

    for seed in top_seeds:
        similar = lastfm_client.get_similar_artists(seed["name"], limit=15)
        for s in similar:
            name_key = s["name"].lower().strip()
            if name_key in ("various artists",):
                continue
            if name_key in candidates:
                candidates[name_key]["discovery_sources"].add(f"lastfm_similar:{seed['name']}")
            else:
                candidates[name_key] = {
                    "name": s["name"],
                    "mb_id": None,
                    "spotify_id": None,
                    "genres": [],
                    "discovery_sources": {f"lastfm_similar:{seed['name']}"},
                    "mb_score": 0,
                    "lastfm_match": s.get("match", 0),
                }

    # Strategy 2: Top artists by tag for top genres
    skip_tags = {"english", "british", "american", "canadian", "australian",
                 "german", "french", "swedish", "nuno", "2010s", "2000s",
                 "1990s", "1980s", "seen live", "favorites"}
    top_genres = [
        (genre, weight) for genre, weight in genre_weights.items()
        if genre.lower() not in skip_tags
    ][:10]

    logger.info("Discovering from %d genres via Last.fm tag search...", len(top_genres))
    for genre, weight in top_genres:
        tag_artists = lastfm_client.get_tag_top_artists(genre, limit=30)
        for a in tag_artists:
            name_key = a["name"].lower().strip()
            if name_key in ("various artists",):
                continue
            if name_key in candidates:
                candidates[name_key]["discovery_sources"].add(f"lastfm_tag:{genre}")
                # Store listener data if available from tag search
                if a.get("listeners"):
                    candidates[name_key]["lastfm_listeners"] = max(
                        candidates[name_key].get("lastfm_listeners", 0),
                        a["listeners"],
                    )
            else:
                candidates[name_key] = {
                    "name": a["name"],
                    "mb_id": None,
                    "spotify_id": None,
                    "genres": [],
                    "discovery_sources": {f"lastfm_tag:{genre}"},
                    "mb_score": 0,
                    "lastfm_listeners": a.get("listeners", 0),
                }

    result = []
    for c in candidates.values():
        c["source_count"] = len(c["discovery_sources"])
        c["discovery_sources"] = list(c["discovery_sources"])
        result.append(c)

    logger.info("  Last.fm discovery: %d candidates total", len(result))
    return result


def discover_from_bandcamp(
    genre_weights: Dict[str, float],
) -> List[Dict[str, Any]]:
    """Discover candidates from Bandcamp's public discover API.

    Uses lazy import — returns empty list if requests is unavailable.
    Zero Spotify API calls.
    """
    try:
        import bandcamp_client
    except ImportError:
        logger.info("bandcamp_client not available — skipping Bandcamp discovery")
        return []

    logger.info("Discovering from Bandcamp...")
    try:
        return bandcamp_client.discover_artists(genre_weights)
    except Exception as e:
        logger.warning("Bandcamp discovery failed: %s", e)
        return []


def discover_from_blogs() -> List[Dict[str, Any]]:
    """Discover candidates from music blog RSS feeds.

    Uses lazy import — returns empty list if feedparser is unavailable.
    Zero Spotify API calls.
    """
    try:
        import rss_client
    except ImportError:
        logger.info("rss_client not available — skipping blog RSS discovery")
        return []

    logger.info("Discovering from music blog RSS feeds...")
    try:
        return rss_client.extract_artists_from_feeds()
    except Exception as e:
        logger.warning("Blog RSS discovery failed: %s", e)
        return []


def discover_from_spotify_recommendations(
    sp: spotipy.Spotify,
    seed_artists: List[Dict[str, Any]],
    genre_weights: Dict[str, float],
    playlist_type: str = "rising_stars",
) -> List[Dict[str, Any]]:
    """Discover candidates via Spotify's recommendations endpoint.

    1 API call returns up to 100 tracks WITH full metadata — vastly more
    efficient than search→resolve→fetch per artist.

    Args:
        playlist_type: "rising_stars" (min_pop=30, max_pop=65) or
                       "deep_cuts" (max_pop=30)
    """
    from spotify_client import get_recommendations, SpotifyRateLimitError

    logger.info("Discovering from Spotify recommendations (type=%s)...", playlist_type)

    # Build seeds: top 5 artists from taste profile that have Spotify IDs
    artist_seeds = [
        a["spotify_id"] for a in seed_artists
        if a.get("spotify_id")
    ][:5]

    # Top genre seeds (Spotify uses specific genre names)
    skip_tags = {"english", "british", "american", "canadian", "australian",
                 "german", "french", "swedish", "nuno", "2010s", "2000s",
                 "1990s", "1980s", "seen live", "favorites"}
    genre_seeds = [
        g for g, _ in sorted(genre_weights.items(), key=lambda x: -x[1])
        if g.lower() not in skip_tags
    ][:3]

    # Set popularity params based on playlist type
    rec_kwargs = {}
    if playlist_type == "rising_stars":
        rec_kwargs = {"min_popularity": 30, "max_popularity": 65}
    elif playlist_type == "deep_cuts":
        rec_kwargs = {"max_popularity": 30}

    all_tracks = []

    # Strategy 1: Seed with top artists
    if artist_seeds:
        try:
            tracks = get_recommendations(
                sp, seed_artists=artist_seeds, limit=100, **rec_kwargs
            )
            all_tracks.extend(tracks)
        except SpotifyRateLimitError:
            raise
        except Exception as e:
            logger.warning("Spotify recs (artist seeds) failed: %s", e)

    # Strategy 2: Seed with genres (different results)
    if genre_seeds:
        try:
            tracks = get_recommendations(
                sp, seed_genres=genre_seeds, limit=100, **rec_kwargs
            )
            all_tracks.extend(tracks)
        except SpotifyRateLimitError:
            raise
        except Exception as e:
            logger.warning("Spotify recs (genre seeds) failed: %s", e)

    # Convert tracks to candidate format
    candidates = {}
    for track in all_tracks:
        aid = track.get("artist_spotify_id")
        if not aid:
            continue
        name_key = track["artist_name"].lower().strip()
        if name_key in ("various artists",):
            continue

        if name_key in candidates:
            candidates[name_key]["discovery_sources"].add(f"spotify_recs:{playlist_type}")
        else:
            candidates[name_key] = {
                "name": track["artist_name"],
                "spotify_id": aid,
                "mb_id": None,
                "genres": [],
                "discovery_sources": {f"spotify_recs:{playlist_type}"},
                "mb_score": 0,
                # Store the track directly — no separate fetch needed
                "_rec_track": {
                    "track_id": track["track_id"],
                    "track_name": track["track_name"],
                    "artist_name": track["artist_name"],
                    "duration_ms": track.get("duration_ms", 0),
                    "preview_url": track.get("preview_url"),
                    "release_date": track.get("release_date", ""),
                },
            }
            # Use the track's release_date for recency scoring
            if track.get("release_date"):
                candidates[name_key]["release_date"] = track["release_date"]

    result = []
    for c in candidates.values():
        c["source_count"] = len(c["discovery_sources"])
        c["discovery_sources"] = list(c["discovery_sources"])
        result.append(c)

    logger.info("  Spotify recommendations: %d unique candidates from %d tracks",
                len(result), len(all_tracks))
    return result


def resolve_spotify_ids(
    sp: spotipy.Spotify,
    candidates: List[Dict[str, Any]],
    seed_ids: Set[str],
) -> List[Dict[str, Any]]:
    """Search Spotify to get Spotify IDs for candidates and filter out seeds.

    Uses SQLite cache to avoid re-searching artists we've seen before.
    Tracks request count and stops before hitting rate limits.
    """
    from spotify_client import _count_request, SpotifyRateLimitError

    logger.info("Resolving Spotify IDs for %d candidates...", len(candidates))

    # Check cache first
    names_to_resolve = [c["name"] for c in candidates if not c.get("spotify_id")]
    cached = db.get_cached_spotify_ids(names_to_resolve) if names_to_resolve else {}
    cache_hits = 0

    resolved = []
    not_found = 0
    new_cache_entries = []

    for i, candidate in enumerate(candidates):
        # Already has a Spotify ID (from Spotify search discovery)
        if candidate.get("spotify_id"):
            if candidate["spotify_id"] not in seed_ids:
                resolved.append(candidate)
            continue

        # Check cache
        name_lower = candidate["name"].lower().strip()
        if name_lower in cached and cached[name_lower]:
            sid = cached[name_lower]
            if sid not in seed_ids:
                candidate["spotify_id"] = sid
                resolved.append(candidate)
                cache_hits += 1
            continue

        # Search Spotify
        try:
            _count_request("search")
            result = sp.search(q=f'artist:"{candidate["name"]}"', type="artist", limit=3)
            items = result.get("artists", {}).get("items", [])

            # Find best match by name
            match = None
            for item in items:
                if musicbrainz_client._name_match(candidate["name"], item["name"]):
                    match = item
                    break

            if match:
                sid = match["id"]
                new_cache_entries.append((candidate["name"], sid))
                if sid not in seed_ids:
                    candidate["spotify_id"] = sid
                    candidate["spotify_name"] = match["name"]
                    resolved.append(candidate)
            else:
                not_found += 1

        except SpotifyRateLimitError as e:
            logger.warning("Approaching rate limit at candidate %d: %s", i, e)
            break
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "rate" in err_str.lower():
                logger.warning("Spotify rate limit hit at candidate %d. Stopping.", i)
                break
            logger.warning("Spotify search failed for '%s': %s", candidate["name"], e)

        if (i + 1) % 25 == 0:
            logger.info("  Progress: %d/%d resolved", i + 1, len(candidates))

    # Save new cache entries
    if new_cache_entries:
        db.cache_spotify_ids_bulk(new_cache_entries)

    logger.info("  Resolved: %d (cache hits: %d, searched: %d, not found: %d)",
                len(resolved), cache_hits,
                len(new_cache_entries), not_found)
    return resolved


def filter_already_recommended(
    candidates: List[Dict[str, Any]],
    cooldown_weeks: int = None,
) -> List[Dict[str, Any]]:
    """Remove candidates that were recommended within the cooldown period."""
    if cooldown_weeks is None:
        cooldown_weeks = config.ARTIST_COOLDOWN_WEEKS

    conn = db.get_connection()
    try:
        rows = conn.execute(
            """SELECT DISTINCT artist_spotify_id FROM recommendations
               WHERE recommended_at > datetime('now', ?)""",
            (f"-{cooldown_weeks * 7} days",),
        ).fetchall()
        recent_ids = {r["artist_spotify_id"] for r in rows}
    finally:
        conn.close()

    if not recent_ids:
        return candidates

    before = len(candidates)
    filtered = [c for c in candidates if c.get("spotify_id") not in recent_ids]
    logger.info("  Cooldown filter: removed %d recently recommended artists",
                before - len(filtered))
    return filtered


def run_discovery(
    sp: spotipy.Spotify,
    run_index: int = None,
) -> List[Dict[str, Any]]:
    """Run the full discovery pipeline.

    Args:
        sp: Authenticated Spotify client
        run_index: 0, 1, or 2 — rotates which seeds/genres drive discovery.
                   None = use all (backwards compatible).

    Returns scored and ranked candidates ready for playlist building.
    Candidates from Spotify recommendations carry their track pre-attached
    in the `_rec_track` field (no separate track fetch needed).
    """
    # Load taste profile and seed artists from DB
    genre_weights = dict(db.get_taste_profile())
    seed_artists = db.get_seed_artists()
    seed_ids = {a["spotify_id"] for a in seed_artists}
    seed_names = {a["name"].lower().strip() for a in seed_artists}

    if not genre_weights:
        logger.error("No taste profile found. Run taste profiler first.")
        return []

    logger.info("Loaded taste profile: %d genres, %d seed artists (run_index=%s)",
                len(genre_weights), len(seed_ids), run_index)

    # Rotate seeds and genres based on run_index for variety across runs
    rotated_seeds = _rotate_seeds(seed_artists, run_index)
    rotated_weights = _rotate_genres(genre_weights, run_index)

    # Gather candidates from all sources
    all_candidates = []

    # Source 1: MusicBrainz tag search (free, no API key)
    mb_candidates = discover_from_musicbrainz(rotated_weights, seed_ids)
    all_candidates.extend(mb_candidates)

    # Source 2: Last.fm (if configured)
    lfm_candidates = discover_from_lastfm(rotated_weights, rotated_seeds)
    all_candidates.extend(lfm_candidates)

    # Source 3: Bandcamp (no API key, no Spotify calls)
    try:
        bc_candidates = discover_from_bandcamp(rotated_weights)
        all_candidates.extend(bc_candidates)
    except Exception as e:
        logger.warning("Bandcamp source failed (non-fatal): %s", e)

    # Source 4: Music blog RSS feeds (no API key, no Spotify calls)
    try:
        blog_candidates = discover_from_blogs()
        all_candidates.extend(blog_candidates)
    except Exception as e:
        logger.warning("Blog RSS source failed (non-fatal): %s", e)

    # Source 5: Spotify recommendations — rising stars candidates
    try:
        rising_recs = discover_from_spotify_recommendations(
            sp, seed_artists, rotated_weights, playlist_type="rising_stars"
        )
        all_candidates.extend(rising_recs)
    except Exception as e:
        logger.warning("Spotify recs (rising) failed (non-fatal): %s", e)

    # Source 6: Spotify recommendations — deep cuts candidates
    try:
        deep_recs = discover_from_spotify_recommendations(
            sp, seed_artists, rotated_weights, playlist_type="deep_cuts"
        )
        all_candidates.extend(deep_recs)
    except Exception as e:
        logger.warning("Spotify recs (deep) failed (non-fatal): %s", e)

    if not all_candidates:
        logger.error("No candidates discovered from any source")
        return []

    # Deduplicate across sources
    candidates = _merge_candidates(all_candidates)
    logger.info("After deduplication: %d unique candidates", len(candidates))

    # Filter out seed artists by name (in case Spotify ID wasn't set yet)
    before = len(candidates)
    candidates = [c for c in candidates if c["name"].lower().strip() not in seed_names]
    if len(candidates) < before:
        logger.info("  Removed %d seed artists by name", before - len(candidates))

    # Filter out mainstream artists
    candidates = _filter_mainstream(candidates)

    # Enrich with Last.fm listener data (best popularity signal)
    lastfm_client.enrich_with_listeners(candidates, "pre-scoring")

    # Enrich candidates missing genres via MusicBrainz (Spotify search ones have none)
    needs_genres = [c for c in candidates if not c.get("genres")]
    if needs_genres:
        logger.info("Enriching %d candidates with MusicBrainz genres...", len(needs_genres))
        musicbrainz_client.enrich_artists_with_genres(candidates, "discovery")

    # Score candidates BEFORE resolving Spotify IDs (to minimize API calls)
    from scorer import score_candidates
    scored = score_candidates(candidates, genre_weights)
    logger.info("Scored %d candidates pre-Spotify resolution", len(scored))

    # Resolve Spotify IDs for top 200 candidates — batch endpoints make this
    # affordable (4 calls for 200 artists vs 200 individual searches).
    top_n = min(len(scored), 200)
    top_candidates = scored[:top_n]
    logger.info("Resolving Spotify IDs for top %d candidates...", top_n)
    resolved = resolve_spotify_ids(sp, top_candidates, seed_ids)

    # Filter cooldown (global — per-playlist cooldown applied later in scanner)
    resolved = filter_already_recommended(resolved)

    # Re-sort after filtering
    resolved.sort(key=lambda c: c["composite_score"], reverse=True)

    logger.info("Discovery complete: %d scored candidates with Spotify IDs", len(resolved))
    return resolved


def _rotate_seeds(
    seed_artists: List[Dict[str, Any]],
    run_index: int = None,
) -> List[Dict[str, Any]]:
    """Rotate which seed artists drive similar-artist search.

    Run 0 (Sun): seeds 0-14
    Run 1 (Tue): seeds 15-29
    Run 2 (Thu): seeds 30-44
    None: all seeds (backwards compatible)
    """
    if run_index is None:
        return seed_artists

    start = run_index * 15
    end = start + 15
    rotated = seed_artists[start:end]

    # If not enough seeds in this slice, wrap around
    if len(rotated) < 10 and len(seed_artists) > 0:
        rotated = seed_artists[start:] + seed_artists[:max(10 - len(rotated), 0)]

    logger.info("  Run %d: using seeds %d-%d (%d artists)",
                run_index, start, start + len(rotated) - 1, len(rotated))
    return rotated


def _rotate_genres(
    genre_weights: Dict[str, float],
    run_index: int = None,
) -> Dict[str, float]:
    """Rotate which genre slice drives tag-based discovery.

    Run 0 (Sun): genres 0-19
    Run 1 (Tue): genres 5-24
    Run 2 (Thu): genres 10-29
    None: all genres (backwards compatible)
    """
    if run_index is None:
        return genre_weights

    sorted_genres = sorted(genre_weights.items(), key=lambda x: -x[1])
    start = run_index * 5
    end = start + 20
    sliced = sorted_genres[start:end]

    # If not enough genres, wrap
    if len(sliced) < 15 and len(sorted_genres) > 0:
        sliced = sorted_genres[start:] + sorted_genres[:max(15 - len(sliced), 0)]

    logger.info("  Run %d: using genres %d-%d (%d genres)",
                run_index, start, start + len(sliced) - 1, len(sliced))
    return dict(sliced)


def _filter_mainstream(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter out likely mainstream artists.

    Uses multiple signals:
    1. Hardcoded list of well-known mainstream acts
    2. Last.fm listener threshold (>5M = definitely mainstream)
    3. High MB score + many sources = likely mainstream
    """
    # Well-known acts that should NOT appear in "niche finds".
    # Three tiers: mainstream pop, major electronic, and well-known legacy acts.
    # This filter is the safety net — Last.fm listener data is the real filter
    # when available.
    mainstream_names = {
        # Pop / general mainstream
        "lady gaga", "britney spears", "madonna", "jennifer lopez",
        "dua lipa", "taylor swift", "beyonce", "beyoncé", "rihanna",
        "drake", "eminem", "ed sheeran", "ariana grande", "justin bieber",
        "katy perry", "miley cyrus", "the weeknd", "billie eilish",
        "harry styles", "adele", "bruno mars", "coldplay", "u2",
        "maroon 5", "imagine dragons", "post malone", "kanye west",
        "travis scott", "bad bunny", "olivia rodrigo", "sabrina carpenter",
        "sza", "doja cat", "lana del rey", "pink", "shakira",
        "elton john", "david bowie", "prince", "michael jackson",
        "queen", "the beatles", "the rolling stones", "fleetwood mac",
        "nirvana", "pearl jam", "foo fighters", "green day",
        "linkin park", "muse", "gorillaz",
        # Major electronic / DJ acts
        "avicii", "daft punk", "david guetta", "calvin harris",
        "tiësto", "martin garrix", "marshmello", "skrillex",
        "deadmau5", "the chainsmokers", "kygo", "diplo",
        "zedd", "alan walker", "major lazer", "swedish house mafia",
        "steve aoki", "afrojack", "armin van buuren", "hardwell",
        "dimitri vegas & like mike", "don diablo",
        "moby", "fatboy slim", "paul oakenfold", "paul van dyk",
        "sasha", "john digweed", "carl cox", "richie hawtin",
        # Legacy electronic/synth/alt acts too well-known for "niche finds"
        "depeche mode", "new order", "pet shop boys", "kraftwerk",
        "erasure", "gary numan", "jean-michel jarre", "tangerine dream",
        "björk", "massive attack", "portishead", "radiohead",
        "nine inch nails", "the cure", "cocteau twins",
        # Major dance/house acts
        "bob sinclar", "2 unlimited", "above & beyond", "faithless",
        "basement jaxx", "chemical brothers",
        "the chemical brothers", "the prodigy", "prodigy",
        "kylie minogue", "donna summer", "bee gees",
        # Well-known rock/alt/classic acts that pollute genre searches
        "the smashing pumpkins", "the beach boys", "pink floyd",
        "led zeppelin", "ac/dc", "metallica", "iron maiden",
        "black sabbath", "tool", "rage against the machine",
        "red hot chili peppers", "blur", "oasis",
        "elvis presley", "genesis", "paul mccartney", "the who",
        "eric clapton", "r.e.m.", "rem", "talking heads",
        "stevie wonder", "frank sinatra", "bob dylan", "bob marley",
        "jimi hendrix", "the doors", "bruce springsteen",
        "aerosmith", "eagles", "phil collins", "sting",
        "ellie goulding", "robyn", "kim wilde", "ace of base",
        "marillion", "mike oldfield", "yes", "rush",
    }

    before = len(candidates)
    filtered = []
    for c in candidates:
        name_lower = c["name"].lower().strip()
        if name_lower in mainstream_names:
            continue
        # Last.fm listener check: >5M unique listeners = mainstream
        if c.get("lastfm_listeners", 0) > 5_000_000:
            continue
        # High MB score + found in many genres = mainstream
        if c.get("mb_score", 0) >= 90 and c.get("source_count", 0) >= 3:
            continue
        filtered.append(c)

    removed = before - len(filtered)
    if removed:
        logger.info("  Mainstream filter: removed %d likely mainstream artists", removed)
    return filtered


def _merge_candidates(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Merge duplicate candidates found via different sources."""
    merged = {}
    for c in candidates:
        key = c["name"].lower().strip()
        if key in merged:
            existing = merged[key]
            # Merge sources
            existing_sources = set(existing.get("discovery_sources", []))
            new_sources = set(c.get("discovery_sources", []))
            existing["discovery_sources"] = list(existing_sources | new_sources)
            existing["source_count"] = len(existing["discovery_sources"])
            # Merge genres
            existing_genres = set(existing.get("genres", []))
            new_genres = set(c.get("genres", []))
            existing["genres"] = list(existing_genres | new_genres)
            # Keep higher mb_score
            existing["mb_score"] = max(
                existing.get("mb_score", 0), c.get("mb_score", 0)
            )
            # Keep Spotify ID if available
            if c.get("spotify_id") and not existing.get("spotify_id"):
                existing["spotify_id"] = c["spotify_id"]
            # Keep Last.fm data if available
            if c.get("lastfm_listeners"):
                existing["lastfm_listeners"] = max(
                    existing.get("lastfm_listeners", 0), c["lastfm_listeners"]
                )
            if c.get("lastfm_match"):
                existing["lastfm_match"] = max(
                    existing.get("lastfm_match", 0), c["lastfm_match"]
                )
            # Keep pre-fetched track from Spotify recommendations
            if c.get("_rec_track") and not existing.get("_rec_track"):
                existing["_rec_track"] = c["_rec_track"]
            # Keep release_date if available
            if c.get("release_date") and not existing.get("release_date"):
                existing["release_date"] = c["release_date"]
        else:
            merged[key] = c.copy()

    return list(merged.values())
