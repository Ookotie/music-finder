"""Taste Profiler — Builds a weighted genre map from Spotify listening data."""

import logging
from collections import defaultdict
from typing import Any, Dict, List, Tuple

import spotipy

import config
import musicbrainz_client
import spotify_client

logger = logging.getLogger(__name__)


def build_genre_weights(all_artists: List[Dict[str, Any]]) -> Dict[str, float]:
    """Build a normalized, weighted genre map from collected artists.

    Weighting factors:
    - Time range: short_term (0.5) > medium_term (0.3) > long_term (0.2)
    - Source: top_artists (1.0) > top_tracks (0.7) > followed (0.5)
    - Position: earlier in the list = higher weight (rank decay)
    """
    genre_scores = defaultdict(float)

    # Group by (source, time_range) to apply rank decay within each group
    groups = defaultdict(list)
    for artist in all_artists:
        key = (artist.get("source", "unknown"), artist.get("time_range"))
        groups[key].append(artist)

    for (source, time_range), artists in groups.items():
        source_weight = config.SOURCE_WEIGHTS.get(source, 0.3)
        time_weight = config.TIME_RANGE_WEIGHTS.get(time_range, 0.2) if time_range else 0.2

        for rank, artist in enumerate(artists):
            # Rank decay: first artist gets 1.0, 50th gets ~0.3
            rank_factor = 1.0 / (1.0 + rank * 0.05)
            artist_weight = source_weight * time_weight * rank_factor

            for genre in artist.get("genres", []):
                genre_scores[genre] += artist_weight

    if not genre_scores:
        return {}

    # Normalize to 0-1 range
    max_score = max(genre_scores.values())
    if max_score > 0:
        genre_scores = {g: s / max_score for g, s in genre_scores.items()}

    # Sort descending
    return dict(sorted(genre_scores.items(), key=lambda x: x[1], reverse=True))


def run_taste_profile(sp: spotipy.Spotify) -> Tuple[Dict[str, float], List[Dict[str, Any]]]:
    """Run the full taste profiling pipeline.

    Returns:
        (genre_weights, all_unique_artists)
    """
    all_artists = []
    seen_ids = set()

    # 1. Top artists across all three time ranges
    for time_range in ["short_term", "medium_term", "long_term"]:
        logger.info("Fetching top artists (%s)...", time_range)
        try:
            artists = spotify_client.get_top_artists(sp, time_range)
            logger.info("  Found %d artists", len(artists))
            all_artists.extend(artists)
            seen_ids.update(a["spotify_id"] for a in artists)
        except Exception as e:
            logger.warning("Failed to fetch top artists (%s): %s", time_range, e)

    # 2. Top tracks → extract artists (across all time ranges)
    track_artists = []
    for time_range in ["short_term", "medium_term", "long_term"]:
        logger.info("Fetching top tracks (%s)...", time_range)
        try:
            artists = spotify_client.get_top_tracks(sp, time_range)
            # Only keep artists we haven't seen from top_artists
            new = [a for a in artists if a["spotify_id"] not in seen_ids]
            logger.info("  Found %d new artists from tracks", len(new))
            track_artists.extend(new)
            seen_ids.update(a["spotify_id"] for a in new)
        except Exception as e:
            logger.warning("Failed to fetch top tracks (%s): %s", time_range, e)

    # Enrich track artists with genre/popularity data via Spotify (may fail in dev mode)
    if track_artists:
        logger.info("Enriching %d track artists with full data...", len(track_artists))
        track_artists = spotify_client.enrich_artists(sp, track_artists)
    all_artists.extend(track_artists)

    # 3. Followed artists
    logger.info("Fetching followed artists...")
    try:
        followed = spotify_client.get_followed_artists(sp)
        new_followed = [a for a in followed if a["spotify_id"] not in seen_ids]
        logger.info("  Found %d followed artists (%d new)", len(followed), len(new_followed))
        all_artists.extend(new_followed)
        seen_ids.update(a["spotify_id"] for a in new_followed)
    except Exception as e:
        logger.warning("Failed to fetch followed artists: %s", e)

    logger.info("Total unique artists collected: %d", len(seen_ids))

    # Enrich all artists missing genres via MusicBrainz
    # (Spotify dev mode strips genre data from artist objects)
    artists_without_genres = sum(1 for a in all_artists if not a.get("genres"))
    if artists_without_genres > 0:
        logger.info("%d artists missing genres — enriching via MusicBrainz...", artists_without_genres)
        musicbrainz_client.enrich_artists_with_genres(all_artists, "taste profile")

    # Build genre weights
    genre_weights = build_genre_weights(all_artists)
    logger.info("Genre map: %d unique genres", len(genre_weights))

    return genre_weights, all_artists
