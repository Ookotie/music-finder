"""Last.fm API client for artist popularity data and similar artist discovery.

Free API, key required (https://www.last.fm/api/account/create).
Provides real listener counts — the best available popularity signal
when Spotify dev mode strips popularity/follower data.
"""

import logging
import time
from typing import Any, Dict, List, Optional

import requests

from . import config

logger = logging.getLogger(__name__)

_BASE_URL = "https://ws.audioscrobbler.com/2.0/"
_MIN_REQUEST_INTERVAL = 0.25  # Last.fm allows ~5 req/sec
_last_request_time = 0.0


def _rate_limit():
    """Ensure we don't exceed Last.fm rate limits."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < _MIN_REQUEST_INTERVAL:
        time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
    _last_request_time = time.time()


def _api_call(method: str, **params) -> Optional[Dict]:
    """Make a Last.fm API call. Returns None on failure."""
    if not config.LASTFM_API_KEY:
        return None

    _rate_limit()
    params.update({
        "method": method,
        "api_key": config.LASTFM_API_KEY,
        "format": "json",
    })

    try:
        resp = requests.get(_BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            logger.debug("Last.fm API error for %s: %s", method, data.get("message"))
            return None
        return data
    except requests.RequestException as e:
        logger.warning("Last.fm API call failed (%s): %s", method, e)
        return None


def get_artist_info(artist_name: str) -> Optional[Dict[str, Any]]:
    """Get artist info including listener count and tags.

    Returns dict with: name, listeners, playcount, tags, similar_artists
    Returns None if not found or API unavailable.
    """
    data = _api_call("artist.getinfo", artist=artist_name, autocorrect="1")
    if not data or "artist" not in data:
        return None

    artist = data["artist"]
    stats = artist.get("stats", {})
    tags = [
        t["name"].lower()
        for t in artist.get("tags", {}).get("tag", [])
    ]
    similar = [
        {
            "name": s["name"],
            "match": float(s.get("match", 0)),
        }
        for s in artist.get("similar", {}).get("artist", [])
    ]

    return {
        "name": artist.get("name", artist_name),
        "listeners": int(stats.get("listeners", 0)),
        "playcount": int(stats.get("playcount", 0)),
        "tags": tags,
        "similar_artists": similar,
    }


def get_similar_artists(
    artist_name: str, limit: int = 20
) -> List[Dict[str, Any]]:
    """Get similar artists from Last.fm.

    Returns list of dicts with: name, match (0-1 similarity score)
    """
    data = _api_call("artist.getsimilar", artist=artist_name, limit=str(limit))
    if not data or "similarartists" not in data:
        return []

    return [
        {
            "name": a["name"],
            "match": float(a.get("match", 0)),
        }
        for a in data["similarartists"].get("artist", [])
    ]


def get_tag_top_artists(tag: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Get top artists for a tag/genre from Last.fm.

    Returns list of dicts with: name, listeners, playcount
    """
    data = _api_call("tag.gettopartists", tag=tag, limit=str(limit))
    if not data or "topartists" not in data:
        return []

    return [
        {
            "name": a["name"],
            "listeners": int(a.get("listeners", 0)),
            "playcount": int(a.get("playcount", 0)),
        }
        for a in data["topartists"].get("artist", [])
    ]


def enrich_with_listeners(
    candidates: List[Dict[str, Any]], batch_label: str = ""
) -> List[Dict[str, Any]]:
    """Enrich candidates with Last.fm listener counts.

    Modifies candidates in-place. Only queries artists missing listener data.
    Returns the candidates list.
    """
    if not config.LASTFM_API_KEY:
        logger.info("Last.fm API key not configured — skipping listener enrichment")
        return candidates

    needs_data = [c for c in candidates if not c.get("lastfm_listeners")]
    if not needs_data:
        return candidates

    label = f" ({batch_label})" if batch_label else ""
    logger.info("Enriching %d candidates with Last.fm listener data%s...",
                len(needs_data), label)

    found = 0
    for i, candidate in enumerate(needs_data):
        info = get_artist_info(candidate["name"])
        if info and info["listeners"] > 0:
            candidate["lastfm_listeners"] = info["listeners"]
            candidate["lastfm_playcount"] = info["playcount"]
            # Also grab tags if candidate has no genres
            if not candidate.get("genres") and info.get("tags"):
                candidate["genres"] = info["tags"]
            found += 1

        if (i + 1) % 25 == 0:
            logger.info("  Progress: %d/%d enriched (%d with data)",
                        i + 1, len(needs_data), found)

    logger.info("  Last.fm enrichment complete: %d/%d candidates got listener data",
                found, len(needs_data))
    return candidates
