"""Discovery Engine — Multi-source candidate gathering.

Genre-first, two-funnel architecture:
  - Deep Cuts: hidden gems from any era, filtered to a specific genre cluster
  - Fresh Finds: what's breaking NOW, filtered to a specific genre cluster

Sources (all working in Spotify dev mode):
  1. MusicBrainz tag search (free, no API key)
  2. Last.fm similar artists + tag search
  3. Bandcamp discover API (no API key)
  4. Music blog RSS feeds
  5. Spotify search (for ID resolution + fresh finds discovery)
"""

import logging
import re
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import spotipy

from . import config
from . import db
from . import lastfm_client
from . import musicbrainz_client

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

    from .spotify_client import _count_request, SpotifyRateLimitError

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
        from . import bandcamp_client
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
        from . import rss_client
    except ImportError:
        logger.info("rss_client not available — skipping blog RSS discovery")
        return []

    logger.info("Discovering from music blog RSS feeds...")
    try:
        return rss_client.extract_artists_from_feeds()
    except Exception as e:
        logger.warning("Blog RSS discovery failed: %s", e)
        return []




def resolve_spotify_ids(
    sp: spotipy.Spotify,
    candidates: List[Dict[str, Any]],
    seed_ids: Set[str],
) -> List[Dict[str, Any]]:
    """Search Spotify to get Spotify IDs for candidates and filter out seeds.

    Uses SQLite cache to avoid re-searching artists we've seen before.
    Tracks request count and stops before hitting rate limits.
    """
    from .spotify_client import _count_request, SpotifyRateLimitError

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


def discover_deep_cuts(
    sp: spotipy.Spotify,
    genre_cluster: str,
    genre_keywords: set,
    genre_weights: Dict[str, float],
    seed_artists: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Discover hidden gems (any era) filtered to a genre cluster.

    Sources: Last.fm similar artists, MusicBrainz tag search,
    Bandcamp "top" sort, Last.fm tag.getTopArtists.

    Returns scored candidates with Spotify IDs, ready for playlist building.
    """
    from .genre_cluster import filter_candidates_to_cluster, get_cluster_bandcamp_slugs
    from .scorer import score_candidates

    seed_ids = {a["spotify_id"] for a in seed_artists if a.get("spotify_id")}
    seed_names = {a["name"].lower().strip() for a in seed_artists}

    # Filter seeds to those whose genres overlap with the cluster
    filtered_seeds = []
    for a in seed_artists:
        a_genres = a.get("genres", [])
        if isinstance(a_genres, str):
            import json
            try:
                a_genres = json.loads(a_genres)
            except (json.JSONDecodeError, TypeError):
                a_genres = []
        a_genres_lower = {g.lower().strip() for g in a_genres}
        if a_genres_lower & genre_keywords:
            filtered_seeds.append(a)
    if not filtered_seeds:
        filtered_seeds = seed_artists[:10]  # fallback
    logger.info("Deep Cuts: %d/%d seeds match cluster '%s'",
                len(filtered_seeds), len(seed_artists), genre_cluster)

    # Build genre weights filtered to cluster keywords
    cluster_weights = {g: w for g, w in genre_weights.items()
                       if g.lower().strip() in genre_keywords
                       or any(kw in g.lower() or g.lower() in kw for kw in genre_keywords)}
    if not cluster_weights:
        cluster_weights = genre_weights  # fallback

    all_candidates = []

    # Source 1: Last.fm similar artists (on filtered seeds)
    lfm_candidates = discover_from_lastfm(cluster_weights, filtered_seeds)
    all_candidates.extend(lfm_candidates)

    # Source 2: MusicBrainz tag search (cluster keywords only)
    mb_candidates = discover_from_musicbrainz(cluster_weights, seed_ids)
    all_candidates.extend(mb_candidates)

    # Source 3: Bandcamp "top" sort (proven quality, any era)
    try:
        from . import bandcamp_client
        bc_slugs = get_cluster_bandcamp_slugs(genre_cluster)
        for slug in bc_slugs:
            try:
                artists = bandcamp_client.get_discover_artists(slug, sort="top")
                for artist in artists:
                    name_key = artist["name"].lower().strip()
                    if name_key in ("various artists", "[unknown]", "unknown artist", "va"):
                        continue
                    all_candidates.append({
                        "name": artist["name"],
                        "mb_id": None,
                        "spotify_id": None,
                        "genres": [slug],
                        "discovery_sources": [f"bandcamp_top:{slug}"],
                        "source_count": 1,
                        "mb_score": 0,
                    })
            except Exception as e:
                logger.warning("Bandcamp top '%s' failed: %s", slug, e)
    except ImportError:
        pass

    # Source 4: Last.fm tag.getTopArtists for each keyword (top 5 keywords)
    if config.LASTFM_API_KEY:
        top_keywords = sorted(cluster_weights.items(), key=lambda x: -x[1])[:5]
        for kw, _ in top_keywords:
            try:
                tag_artists = lastfm_client.get_tag_top_artists(kw, limit=30)
                for a in tag_artists:
                    name_key = a["name"].lower().strip()
                    if name_key in ("various artists",):
                        continue
                    all_candidates.append({
                        "name": a["name"],
                        "mb_id": None,
                        "spotify_id": None,
                        "genres": [],
                        "discovery_sources": [f"lastfm_tag_top:{kw}"],
                        "source_count": 1,
                        "mb_score": 0,
                        "lastfm_listeners": a.get("listeners", 0),
                    })
            except Exception as e:
                logger.warning("Last.fm tag top '%s' failed: %s", kw, e)

    if not all_candidates:
        logger.warning("Deep Cuts: no candidates from any source")
        return []

    # Merge, filter, enrich, score, resolve
    candidates = _merge_candidates(all_candidates)
    candidates = [c for c in candidates if c["name"].lower().strip() not in seed_names]
    candidates = _filter_mainstream(candidates)

    # Genre coherence filter
    candidates = filter_candidates_to_cluster(candidates, genre_cluster)
    if len(candidates) < 15:
        logger.warning("Deep Cuts: only %d after genre filter, skipping strict filter", len(candidates))
        candidates = _merge_candidates(all_candidates)
        candidates = [c for c in candidates if c["name"].lower().strip() not in seed_names]
        candidates = _filter_mainstream(candidates)

    lastfm_client.enrich_with_listeners(candidates, "deep_cuts")
    musicbrainz_client.enrich_artists_with_genres(
        [c for c in candidates if not c.get("genres")], "deep_cuts"
    )

    scored = score_candidates(candidates, genre_weights, profile="deep_cuts")

    top_n = min(len(scored), 150)
    resolved = resolve_spotify_ids(sp, scored[:top_n], seed_ids)
    resolved = filter_already_recommended(resolved)
    resolved.sort(key=lambda c: c["composite_score"], reverse=True)

    logger.info("Deep Cuts discovery complete: %d candidates", len(resolved))
    return resolved


def discover_fresh_finds(
    sp: spotipy.Spotify,
    genre_cluster: str,
    genre_keywords: set,
    genre_weights: Dict[str, float],
    seed_artists: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Discover what's breaking NOW, filtered to a genre cluster.

    Sources: Blog RSS, Bandcamp "new" sort, Spotify search + year filter,
    Last.fm tags (momentum filter).

    Returns scored candidates with Spotify IDs, ready for playlist building.
    """
    from .genre_cluster import filter_candidates_to_cluster, get_cluster_bandcamp_slugs
    from .scorer import score_candidates
    from .spotify_client import _count_request, SpotifyRateLimitError
    from datetime import datetime

    seed_ids = {a["spotify_id"] for a in seed_artists if a.get("spotify_id")}
    seed_names = {a["name"].lower().strip() for a in seed_artists}

    cluster_weights = {g: w for g, w in genre_weights.items()
                       if g.lower().strip() in genre_keywords
                       or any(kw in g.lower() or g.lower() in kw for kw in genre_keywords)}
    if not cluster_weights:
        cluster_weights = genre_weights

    all_candidates = []

    # Source 1: Blog RSS → enrich genres via MusicBrainz → genre filter
    try:
        blog_candidates = discover_from_blogs()
        all_candidates.extend(blog_candidates)
    except Exception as e:
        logger.warning("Blog RSS failed (non-fatal): %s", e)

    # Source 2: Bandcamp "new" sort (newest releases)
    try:
        from . import bandcamp_client
        bc_slugs = get_cluster_bandcamp_slugs(genre_cluster)
        for slug in bc_slugs:
            try:
                artists = bandcamp_client.get_discover_artists(slug, sort="new")
                for artist in artists:
                    name_key = artist["name"].lower().strip()
                    if name_key in ("various artists", "[unknown]", "unknown artist", "va"):
                        continue
                    all_candidates.append({
                        "name": artist["name"],
                        "mb_id": None,
                        "spotify_id": None,
                        "genres": [slug],
                        "discovery_sources": [f"bandcamp_new:{slug}"],
                        "source_count": 1,
                        "mb_score": 0,
                    })
            except Exception as e:
                logger.warning("Bandcamp new '%s' failed: %s", slug, e)
    except ImportError:
        pass

    # Source 3: Spotify search with genre keywords + year filter
    current_year = datetime.now().year
    year_filter = f"{current_year - 1}-{current_year}"
    top_keywords = sorted(cluster_weights.items(), key=lambda x: -x[1])[:5]
    for kw, _ in top_keywords:
        try:
            _count_request("search")
            result = sp.search(q=f"{kw} year:{year_filter}", type="artist", limit=10)
            items = result.get("artists", {}).get("items", [])
            for item in items:
                sid = item["id"]
                if sid in seed_ids:
                    continue
                name_key = item["name"].lower().strip()
                if name_key in ("various artists",):
                    continue
                all_candidates.append({
                    "name": item["name"],
                    "mb_id": None,
                    "spotify_id": sid,
                    "genres": item.get("genres", []),
                    "discovery_sources": [f"spotify_fresh:{kw}"],
                    "source_count": 1,
                    "mb_score": 50,
                })
        except SpotifyRateLimitError:
            logger.warning("Rate limit during Fresh Finds Spotify search")
            break
        except Exception as e:
            logger.warning("Spotify search '%s' failed: %s", kw, e)

    # Source 4: Last.fm tag search (momentum/recent)
    if config.LASTFM_API_KEY:
        for kw, _ in top_keywords:
            try:
                tag_artists = lastfm_client.get_tag_top_artists(kw, limit=20)
                for a in tag_artists:
                    name_key = a["name"].lower().strip()
                    if name_key in ("various artists",):
                        continue
                    all_candidates.append({
                        "name": a["name"],
                        "mb_id": None,
                        "spotify_id": None,
                        "genres": [],
                        "discovery_sources": [f"lastfm_fresh:{kw}"],
                        "source_count": 1,
                        "mb_score": 0,
                        "lastfm_listeners": a.get("listeners", 0),
                    })
            except Exception as e:
                logger.warning("Last.fm fresh tag '%s' failed: %s", kw, e)

    if not all_candidates:
        logger.warning("Fresh Finds: no candidates from any source")
        return []

    # Merge, filter, enrich, score, resolve
    candidates = _merge_candidates(all_candidates)
    candidates = [c for c in candidates if c["name"].lower().strip() not in seed_names]
    candidates = _filter_mainstream(candidates)

    # Enrich genres for candidates that have none (blog/bandcamp/lastfm)
    needs_genres = [c for c in candidates if not c.get("genres")]
    if needs_genres:
        musicbrainz_client.enrich_artists_with_genres(needs_genres, "fresh_finds")

    # Genre coherence filter
    candidates = filter_candidates_to_cluster(candidates, genre_cluster)
    if len(candidates) < 15:
        logger.warning("Fresh Finds: only %d after genre filter, skipping strict filter", len(candidates))
        candidates = _merge_candidates(all_candidates)
        candidates = [c for c in candidates if c["name"].lower().strip() not in seed_names]
        candidates = _filter_mainstream(candidates)
        needs_genres = [c for c in candidates if not c.get("genres")]
        if needs_genres:
            musicbrainz_client.enrich_artists_with_genres(needs_genres, "fresh_finds_fallback")

    lastfm_client.enrich_with_listeners(candidates, "fresh_finds")

    scored = score_candidates(candidates, genre_weights, profile="fresh_finds")

    top_n = min(len(scored), 100)
    resolved = resolve_spotify_ids(sp, scored[:top_n], seed_ids)
    resolved = filter_already_recommended(resolved)
    resolved.sort(key=lambda c: c["composite_score"], reverse=True)

    logger.info("Fresh Finds discovery complete: %d candidates", len(resolved))
    return resolved


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
            # Keep release_date if available
            if c.get("release_date") and not existing.get("release_date"):
                existing["release_date"] = c["release_date"]
        else:
            merged[key] = c.copy()

    return list(merged.values())
