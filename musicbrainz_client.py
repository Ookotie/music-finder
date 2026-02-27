"""MusicBrainz client for genre/tag lookups.

Free API, no key required. Rate limit: 1 request/second (enforced by library).
Used as primary genre source since Spotify's development mode strips genre data.
"""

import logging
import time
from typing import Any, Dict, List, Optional

import musicbrainzngs

logger = logging.getLogger(__name__)

# Initialize once
musicbrainzngs.set_useragent("MusicFinder", "0.1", "oni-ecosystem")
# Rate limit is enforced by the library (1 req/sec), but we add a small buffer
_MIN_REQUEST_INTERVAL = 1.1
_last_request_time = 0.0


def _rate_limit():
    """Ensure we don't exceed MusicBrainz rate limits."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < _MIN_REQUEST_INTERVAL:
        time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
    _last_request_time = time.time()


def _name_match(query: str, result_name: str) -> bool:
    """Check if a MusicBrainz result name is a close match for our query.

    Prevents false matches like "Chris Lorenzo" -> "Lorenzo" (blues artist)
    or "John Summit" -> "John Williams" (classical composer).
    """
    q = query.lower().strip()
    r = result_name.lower().strip()
    # Exact match
    if q == r:
        return True
    # One contains the other (handles "The" prefix, punctuation differences)
    if q in r or r in q:
        return True
    # Compare without common prefixes
    for prefix in ("the ", "a ", "dj "):
        q_stripped = q.removeprefix(prefix)
        r_stripped = r.removeprefix(prefix)
        if q_stripped == r_stripped:
            return True
    return False


def get_artist_tags(artist_name: str) -> List[Dict[str, Any]]:
    """Look up an artist on MusicBrainz and return their tags (genres).

    Returns a list of dicts with 'name' and 'count' keys, sorted by count descending.
    Returns empty list if not found, name mismatch, or on error.
    """
    _rate_limit()
    try:
        results = musicbrainzngs.search_artists(artist=artist_name, limit=5)
        artist_list = results.get("artist-list", [])
        if not artist_list:
            return []

        # Find the best matching result by name
        mb_artist = None
        for candidate in artist_list:
            if _name_match(artist_name, candidate.get("name", "")):
                mb_artist = candidate
                break

        if not mb_artist:
            logger.debug("No name match for '%s' in MusicBrainz results: %s",
                         artist_name, [c.get("name") for c in artist_list[:3]])
            return []

        # Use tags from search result (usually sufficient)
        tags = mb_artist.get("tag-list", [])
        if tags:
            return sorted(tags, key=lambda t: int(t.get("count", 0)), reverse=True)

        # If search didn't include tags, fetch full artist
        _rate_limit()
        full = musicbrainzngs.get_artist_by_id(mb_artist["id"], includes=["tags"])
        tags = full.get("artist", {}).get("tag-list", [])
        return sorted(tags, key=lambda t: int(t.get("count", 0)), reverse=True)

    except Exception as e:
        logger.warning("MusicBrainz lookup failed for '%s': %s", artist_name, e)
        return []


def tags_to_genres(tags: List[Dict[str, Any]], min_count: int = 0) -> List[str]:
    """Convert MusicBrainz tags to a list of genre strings.

    Args:
        tags: Tag list from get_artist_tags()
        min_count: Minimum tag count to include (0 = all)
    """
    return [
        t["name"].lower()
        for t in tags
        if int(t.get("count", 0)) >= min_count
    ]


def enrich_artists_with_genres(
    artists: List[Dict[str, Any]], batch_label: str = ""
) -> List[Dict[str, Any]]:
    """Add genre data to artists using MusicBrainz lookups.

    Modifies artists in-place and returns them.
    Only looks up artists that don't already have genres.
    """
    needs_genres = [a for a in artists if not a.get("genres")]
    if not needs_genres:
        return artists

    label = f" ({batch_label})" if batch_label else ""
    logger.info("Looking up genres for %d artists via MusicBrainz%s...", len(needs_genres), label)

    found = 0
    for i, artist in enumerate(needs_genres):
        tags = get_artist_tags(artist["name"])
        genres = tags_to_genres(tags)
        if genres:
            artist["genres"] = genres
            found += 1
        if (i + 1) % 20 == 0:
            logger.info("  Progress: %d/%d artists looked up (%d with genres)",
                        i + 1, len(needs_genres), found)

    logger.info("  MusicBrainz enrichment complete: %d/%d artists got genres",
                found, len(needs_genres))
    return artists
