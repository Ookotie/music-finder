"""Spotify API client with automatic token refresh and rate limit protection.

Wraps spotipy with:
  - Request counting (tracks total calls per session)
  - Configurable hard limit (stops before hitting rate limit)
  - Small delay between search calls to avoid bursts
"""

import logging
import time
from typing import Any, Dict, List, Optional

import spotipy
from spotipy.oauth2 import SpotifyOAuth

from . import config

logger = logging.getLogger(__name__)

# Rate limit protection
_request_count = 0
_SEARCH_DELAY = 0.15  # seconds between search-type calls
_MAX_REQUESTS_PER_SESSION = int(
    getattr(config, "SPOTIFY_MAX_REQUESTS", 350)
)


class SpotifyRateLimitError(Exception):
    """Raised when approaching the Spotify rate limit."""
    pass


def get_request_count() -> int:
    """Return the number of Spotify API calls made this session."""
    return _request_count


def reset_request_count() -> None:
    """Reset the request counter (call at start of each pipeline run)."""
    global _request_count
    _request_count = 0


def _count_request(call_type: str = "api") -> None:
    """Increment request counter and enforce limits.

    Adds a small delay for search calls to avoid bursts.
    Raises SpotifyRateLimitError if approaching the hard limit.
    """
    global _request_count
    _request_count += 1

    if _request_count >= _MAX_REQUESTS_PER_SESSION:
        raise SpotifyRateLimitError(
            f"Approaching rate limit: {_request_count} requests made "
            f"(limit: {_MAX_REQUESTS_PER_SESSION}). Stopping to prevent 24h ban."
        )

    if call_type == "search":
        time.sleep(_SEARCH_DELAY)

    if _request_count % 50 == 0:
        logger.info("Spotify API calls this session: %d / %d",
                    _request_count, _MAX_REQUESTS_PER_SESSION)


def get_client() -> spotipy.Spotify:
    """Create an authenticated Spotify client using the refresh token."""
    auth_manager = SpotifyOAuth(
        client_id=config.SPOTIFY_CLIENT_ID,
        client_secret=config.SPOTIFY_CLIENT_SECRET,
        redirect_uri=config.SPOTIFY_REDIRECT_URI,
        scope="user-top-read user-library-read user-follow-read "
              "playlist-modify-public playlist-modify-private",
    )

    # Manually inject the refresh token so we skip the browser flow
    auth_manager.refresh_access_token(config.SPOTIFY_REFRESH_TOKEN)

    return spotipy.Spotify(auth_manager=auth_manager)


def get_top_artists(
    sp: spotipy.Spotify, time_range: str, limit: int = 50
) -> List[Dict[str, Any]]:
    """Fetch user's top artists for a given time range.

    Args:
        time_range: 'short_term' (4 weeks), 'medium_term' (6 months), 'long_term' (all time)
        limit: Max 50 per request
    """
    results = sp.current_user_top_artists(limit=limit, time_range=time_range)
    artists = []
    for item in results.get("items", []):
        artists.append({
            "spotify_id": item["id"],
            "name": item["name"],
            "genres": item.get("genres", []),
            "popularity": item.get("popularity", 0),
            "followers": item.get("followers", {}).get("total", 0),
            "image_url": item["images"][0]["url"] if item.get("images") else None,
            "source": "top_artists",
            "time_range": time_range,
        })
    return artists


def get_top_tracks(
    sp: spotipy.Spotify, time_range: str, limit: int = 50
) -> List[Dict[str, Any]]:
    """Fetch user's top tracks and extract their artists."""
    results = sp.current_user_top_tracks(limit=limit, time_range=time_range)
    seen_ids = set()
    artists = []
    for item in results.get("items", []):
        for artist_ref in item.get("artists", []):
            aid = artist_ref["id"]
            if aid in seen_ids:
                continue
            seen_ids.add(aid)
            artists.append({
                "spotify_id": aid,
                "name": artist_ref["name"],
                "source": "top_tracks",
                "time_range": time_range,
            })
    return artists


def get_followed_artists(sp: spotipy.Spotify) -> List[Dict[str, Any]]:
    """Fetch all artists the user follows (paginated)."""
    artists = []
    after = None
    while True:
        results = sp.current_user_followed_artists(limit=50, after=after)
        items = results.get("artists", {}).get("items", [])
        if not items:
            break
        for item in items:
            artists.append({
                "spotify_id": item["id"],
                "name": item["name"],
                "genres": item.get("genres", []),
                "popularity": item.get("popularity", 0),
                "followers": item.get("followers", {}).get("total", 0),
                "image_url": item["images"][0]["url"] if item.get("images") else None,
                "source": "followed",
                "time_range": None,
            })
        after = items[-1]["id"]
        if len(items) < 50:
            break
    return artists


def enrich_artists(
    sp: spotipy.Spotify, artists: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Fill in missing genre/popularity data for artists from top_tracks.

    Spotify's top tracks endpoint only gives artist ID and name.
    We batch-fetch full artist objects to get genres and popularity.
    Falls back to individual lookups if the batch endpoint returns 403
    (common for apps in Spotify development mode).
    """
    needs_enrichment = [a for a in artists if not a.get("genres")]
    if not needs_enrichment:
        return artists

    ids = [a["spotify_id"] for a in needs_enrichment]
    enriched_map = {}

    # Try batch first (up to 50 per request), fall back to individual
    try:
        for i in range(0, len(ids), 50):
            batch = ids[i:i + 50]
            results = sp.artists(batch)
            for item in results.get("artists", []):
                if item:
                    enriched_map[item["id"]] = {
                        "genres": item.get("genres", []),
                        "popularity": item.get("popularity", 0),
                        "followers": item.get("followers", {}).get("total", 0),
                        "image_url": item["images"][0]["url"] if item.get("images") else None,
                    }
    except Exception as e:
        logger.warning("Batch artist fetch failed (%s), falling back to individual lookups", e)
        for aid in ids:
            if aid in enriched_map:
                continue
            try:
                item = sp.artist(aid)
                if item:
                    enriched_map[item["id"]] = {
                        "genres": item.get("genres", []),
                        "popularity": item.get("popularity", 0),
                        "followers": item.get("followers", {}).get("total", 0),
                        "image_url": item["images"][0]["url"] if item.get("images") else None,
                    }
            except Exception as inner_e:
                logger.warning("Failed to enrich artist %s: %s", aid, inner_e)

    for artist in artists:
        aid = artist["spotify_id"]
        if aid in enriched_map:
            artist.update(enriched_map[aid])

    return artists


def get_recommendations(
    sp: spotipy.Spotify,
    seed_artists: List[str] = None,
    seed_genres: List[str] = None,
    seed_tracks: List[str] = None,
    limit: int = 100,
    **kwargs,
) -> List[Dict[str, Any]]:
    """Get track recommendations from Spotify's recommendations endpoint.

    Returns up to `limit` tracks with full metadata (track ID, artist, popularity,
    release date). This is vastly more efficient than search→resolve→fetch per artist.

    kwargs can include: min_popularity, max_popularity, target_popularity, etc.

    Returns list of track dicts with artist info embedded.
    """
    _count_request("api")

    seeds = {}
    if seed_artists:
        seeds["seed_artists"] = seed_artists[:5]  # max 5 seeds total
    if seed_genres:
        seeds["seed_genres"] = seed_genres[:5]
    if seed_tracks:
        seeds["seed_tracks"] = seed_tracks[:5]

    if not seeds:
        return []

    try:
        result = sp.recommendations(limit=limit, **seeds, **kwargs)
        tracks = []
        for item in result.get("tracks", []):
            album = item.get("album", {})
            artist = item["artists"][0] if item.get("artists") else {}
            tracks.append({
                "track_id": item["id"],
                "track_name": item["name"],
                "artist_name": artist.get("name", "Unknown"),
                "artist_spotify_id": artist.get("id"),
                "duration_ms": item.get("duration_ms", 0),
                "preview_url": item.get("preview_url"),
                "release_date": album.get("release_date", ""),
                "popularity": item.get("popularity", 0),
                "album_name": album.get("name", ""),
            })
        logger.info("Spotify recommendations returned %d tracks", len(tracks))
        return tracks
    except Exception as e:
        logger.warning("Spotify recommendations failed: %s", e)
        return []


def get_artists_batch(
    sp: spotipy.Spotify, artist_ids: List[str]
) -> Dict[str, Dict[str, Any]]:
    """Batch-fetch artist objects. Returns {artist_id: artist_dict}.

    Uses sp.artists() which fetches up to 50 per call.
    """
    result = {}
    for i in range(0, len(artist_ids), 50):
        batch = artist_ids[i:i + 50]
        try:
            _count_request("api")
            response = sp.artists(batch)
            for item in response.get("artists", []):
                if item:
                    result[item["id"]] = {
                        "spotify_id": item["id"],
                        "name": item["name"],
                        "genres": item.get("genres", []),
                        "popularity": item.get("popularity", 0),
                        "followers": item.get("followers", {}).get("total", 0),
                    }
        except Exception as e:
            logger.warning("Batch artist fetch failed for batch %d: %s", i // 50, e)
    return result
