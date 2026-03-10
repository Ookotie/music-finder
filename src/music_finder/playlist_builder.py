"""Playlist Builder — Creates Spotify playlists from scored candidates.

Two-playlist pipeline: Deep Cuts + Fresh Finds, genre-coherent.
Uses track caching to minimize API calls on subsequent runs.

Note: Spotify's Feb 2026 API changes deprecated several endpoints used by
spotipy. We bypass spotipy for playlist creation and track addition, calling
the new /me/playlists and /playlists/{id}/items endpoints directly.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests as _requests
import spotipy

from . import config
from . import db

logger = logging.getLogger(__name__)


def get_top_track(
    sp: spotipy.Spotify, artist_name: str, artist_id: str
) -> Optional[Dict[str, Any]]:
    """Get the best track for an artist on Spotify.

    Returns dict with: track_id, track_name, artist_name, duration_ms,
                       preview_url, release_date
    Returns None if no track found.
    """
    from .spotify_client import _count_request, SpotifyRateLimitError

    try:
        _count_request("search")
        result = sp.search(
            q=f'artist:"{artist_name}"', type="track", limit=5
        )
        items = result.get("tracks", {}).get("items", [])
        # Pick the first track that matches the artist ID
        for item in items:
            for a in item.get("artists", []):
                if a["id"] == artist_id:
                    return _track_dict(item, artist_name)
        # If no exact ID match, take the first result
        if items:
            t = items[0]
            return _track_dict(t, t["artists"][0]["name"] if t.get("artists") else artist_name)
    except SpotifyRateLimitError:
        raise
    except Exception as e:
        err_str = str(e)
        if "429" in err_str or "rate" in err_str.lower():
            logger.warning("Spotify rate limit hit during track search for %s", artist_name)
            raise
        logger.warning("Track search failed for %s: %s", artist_name, e)

    return None


def _track_dict(item: Dict, artist_name: str) -> Dict[str, Any]:
    """Build a track dict from a Spotify search result item."""
    album = item.get("album", {})
    release_date = album.get("release_date", "")

    return {
        "track_id": item["id"],
        "track_name": item["name"],
        "artist_name": artist_name,
        "duration_ms": item.get("duration_ms", 0),
        "preview_url": item.get("preview_url"),
        "release_date": release_date,
    }


def fetch_tracks_for_candidates(
    sp: spotipy.Spotify,
    candidates: List[Dict[str, Any]],
    max_tracks: int = None,
) -> List[Dict[str, Any]]:
    """Fetch top tracks for candidates. Uses cache first, then API for misses."""
    if max_tracks is None:
        max_tracks = config.PLAYLIST_SIZE

    # Check track cache for all candidates with Spotify IDs
    artist_ids = [c["spotify_id"] for c in candidates if c.get("spotify_id")]
    cached_tracks = db.get_cached_tracks(artist_ids) if artist_ids else {}
    cache_hits = 0

    tracks = []
    skipped = 0
    new_cache_entries = []

    for candidate in candidates:
        if len(tracks) >= max_tracks:
            break

        sid = candidate.get("spotify_id")
        if not sid:
            skipped += 1
            continue

        # Priority 1: Cached track
        if sid in cached_tracks:
            track = cached_tracks[sid].copy()
            track["artist_spotify_id"] = sid
            track["composite_score"] = candidate.get("composite_score", 0)
            track["genre_match_score"] = candidate.get("genre_match_score", 0)
            track["artist_name"] = candidate.get("name", track.get("artist_name", ""))
            track["artist_genres"] = json.dumps(candidate.get("genres", []))
            tracks.append(track)
            cache_hits += 1
            continue

        # Priority 2: API call
        try:
            track = get_top_track(sp, candidate["name"], sid)
        except Exception:
            logger.warning("Stopping track fetch due to rate limit. Got %d tracks.", len(tracks))
            break

        if track:
            track["artist_spotify_id"] = sid
            track["composite_score"] = candidate.get("composite_score", 0)
            track["genre_match_score"] = candidate.get("genre_match_score", 0)
            track["artist_name"] = candidate.get("name", track.get("artist_name", ""))
            track["artist_genres"] = json.dumps(candidate.get("genres", []))
            tracks.append(track)
            new_cache_entries.append(track)
        else:
            skipped += 1

    # Save new tracks to cache
    if new_cache_entries:
        db.cache_tracks(new_cache_entries)

    logger.info("Fetched %d tracks (%d cache hits, %d skipped)", len(tracks), cache_hits, skipped)
    return tracks


def build_playlist_from_profile(
    sp: spotipy.Spotify,
    candidates: List[Dict[str, Any]],
    playlist_type: str,
    target_size: int,
    playlist_name: str,
    description: str = None,
) -> Optional[Dict[str, Any]]:
    """Build a Spotify playlist from scored candidates for a specific profile.

    Args:
        sp: Authenticated Spotify client
        candidates: Pre-scored and filtered candidates for this playlist type
        playlist_type: "rising_stars", "deep_cuts", or "genre_spotlight"
        target_size: Target number of tracks
        playlist_name: Full playlist name
        description: Playlist description

    Returns:
        Dict with playlist metadata or None on failure.
    """
    if not candidates:
        logger.warning("No candidates for %s playlist", playlist_type)
        return None

    # Fetch tracks (uses cache + pre-attached tracks from recommendations)
    tracks = fetch_tracks_for_candidates(sp, candidates, max_tracks=target_size)

    if not tracks:
        logger.warning("Could not find any tracks for %s playlist", playlist_type)
        return None

    if description is None:
        description = f"Discovered by Music Finder. {len(tracks)} tracks."

    result = _create_spotify_playlist(sp, playlist_name, tracks, description)
    if result:
        result["playlist_type"] = playlist_type
        _save_playlist_history(
            result["playlist_id"], result["playlist_name"],
            tracks, genre_cluster=playlist_type, playlist_type=playlist_type,
        )
    return result


# Keep legacy builders for backwards compatibility

def build_playlist(
    sp: spotipy.Spotify,
    candidates: List[Dict[str, Any]],
    playlist_size: int = None,
) -> Optional[Dict[str, Any]]:
    """Build a single Spotify playlist from top candidates (Phase 3 compatibility)."""
    if playlist_size is None:
        playlist_size = config.PLAYLIST_SIZE

    if not candidates:
        logger.error("No candidates to build playlist from")
        return None

    tracks = fetch_tracks_for_candidates(sp, candidates, playlist_size)
    if not tracks:
        logger.error("Could not find any tracks for candidates")
        return None

    now = datetime.now()
    playlist_name = f"Niche Finds -- Week of {now.strftime('%b %d, %Y')}"

    result = _create_spotify_playlist(sp, playlist_name, tracks)
    if result:
        _save_playlist_history(result["playlist_id"], result["playlist_name"],
                               tracks, genre_cluster=None)
    return result


def _create_spotify_playlist(
    sp: spotipy.Spotify,
    playlist_name: str,
    tracks: List[Dict[str, Any]],
    description: str = None,
) -> Optional[Dict[str, Any]]:
    """Create a Spotify playlist and add tracks using raw HTTP."""
    from .spotify_client import _count_request

    if description is None:
        description = (
            f"Underground & emerging artists matching your taste. "
            f"{len(tracks)} tracks discovered by Music Finder."
        )

    try:
        _count_request("api")
        token = sp.auth_manager.get_access_token(as_dict=False)
        resp = _requests.post(
            "https://api.spotify.com/v1/me/playlists",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={
                "name": playlist_name,
                "public": False,
                "description": description,
            },
        )
        resp.raise_for_status()
        playlist = resp.json()
        playlist_id = playlist["id"]
        playlist_url = playlist["external_urls"]["spotify"]
        logger.info("Created playlist: %s (%s)", playlist_name, playlist_url)
    except Exception as e:
        logger.error("Failed to create playlist '%s': %s", playlist_name, e)
        return None

    # Add tracks
    track_uris = [f"spotify:track:{t['track_id']}" for t in tracks]
    try:
        _count_request("api")
        for i in range(0, len(track_uris), 100):
            batch = track_uris[i:i + 100]
            resp = _requests.post(
                f"https://api.spotify.com/v1/playlists/{playlist_id}/items",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json={"uris": batch},
            )
            resp.raise_for_status()
        logger.info("Added %d tracks to '%s'", len(tracks), playlist_name)
    except Exception as e:
        logger.error("Failed to add tracks to '%s': %s", playlist_name, e)
        return {
            "playlist_id": playlist_id,
            "playlist_name": playlist_name,
            "playlist_url": playlist_url,
            "tracks": [],
            "error": str(e),
        }

    avg_score = sum(t.get("composite_score", 0) for t in tracks) / max(len(tracks), 1)
    total_duration_min = sum(t.get("duration_ms", 0) for t in tracks) / 60_000

    return {
        "playlist_id": playlist_id,
        "playlist_name": playlist_name,
        "playlist_url": playlist_url,
        "tracks": tracks,
        "stats": {
            "track_count": len(tracks),
            "avg_composite_score": round(avg_score, 4),
            "total_duration_min": round(total_duration_min, 1),
        },
    }


def _save_playlist_history(
    playlist_id: str,
    playlist_name: str,
    tracks: List[Dict[str, Any]],
    genre_cluster: str = None,
    playlist_type: str = None,
) -> None:
    """Save playlist and track recommendations to the database."""
    conn = db.get_connection()
    try:
        cursor = conn.execute(
            """INSERT INTO playlist_history
               (playlist_id, playlist_name, track_count, genre_cluster)
               VALUES (?, ?, ?, ?)""",
            (playlist_id, playlist_name, len(tracks), genre_cluster),
        )
        history_id = cursor.lastrowid

        for track in tracks:
            conn.execute(
                """INSERT INTO recommendations
                   (artist_spotify_id, track_spotify_id, playlist_history_id,
                    artist_name, artist_genres, release_date, playlist_type)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    track.get("artist_spotify_id"),
                    track["track_id"],
                    history_id,
                    track.get("artist_name"),
                    track.get("artist_genres"),
                    track.get("release_date"),
                    playlist_type,
                ),
            )

        conn.commit()
        logger.info("Saved playlist history (id=%d, type=%s) with %d recommendations",
                    history_id, playlist_type, len(tracks))
    except Exception as e:
        logger.error("Failed to save playlist history: %s", e)
    finally:
        conn.close()


def get_playlist_history(limit: int = 4) -> List[Dict[str, Any]]:
    """Get recent playlist history."""
    conn = db.get_connection()
    try:
        rows = conn.execute(
            """SELECT id, playlist_id, playlist_name, track_count,
                      genre_cluster, created_at
               FROM playlist_history ORDER BY created_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
