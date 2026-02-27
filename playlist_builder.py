"""Playlist Builder — Creates weekly Spotify playlists from top candidates.

Phase 3: Takes scored candidates, fetches their best track, creates a
Spotify playlist, and records the recommendations in the database.

Note: Spotify's Feb 2026 API changes deprecated several endpoints used by
spotipy. We bypass spotipy for playlist creation and track addition, calling
the new /me/playlists and /playlists/{id}/items endpoints directly.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests as _requests
import spotipy

import config
import db

logger = logging.getLogger(__name__)


def get_top_track(
    sp: spotipy.Spotify, artist_name: str, artist_id: str
) -> Optional[Dict[str, Any]]:
    """Get the best track for an artist on Spotify.

    Tries artist_top_tracks first, falls back to search if that endpoint
    is blocked (Spotify dev mode).

    Returns dict with: track_id, track_name, artist_name, duration_ms, preview_url
    Returns None if no track found.
    """
    from spotify_client import _count_request, SpotifyRateLimitError

    # Skip artist_top_tracks — blocked (403) in Spotify dev mode.
    # Go straight to search which works and saves an API call per artist.

    # Search for tracks by this artist
    try:
        _count_request("search")
        result = sp.search(
            q=f'artist:"{artist_name}"', type="track", limit=5
        )
        items = result.get("tracks", {}).get("items", [])
        # Pick the first track that matches the artist name
        for item in items:
            for a in item.get("artists", []):
                if a["id"] == artist_id:
                    return {
                        "track_id": item["id"],
                        "track_name": item["name"],
                        "artist_name": artist_name,
                        "duration_ms": item.get("duration_ms", 0),
                        "preview_url": item.get("preview_url"),
                    }
        # If no exact ID match, take the first result
        if items:
            t = items[0]
            return {
                "track_id": t["id"],
                "track_name": t["name"],
                "artist_name": t["artists"][0]["name"] if t.get("artists") else artist_name,
                "duration_ms": t.get("duration_ms", 0),
                "preview_url": t.get("preview_url"),
            }
    except SpotifyRateLimitError:
        raise  # propagate to caller
    except Exception as e:
        err_str = str(e)
        if "429" in err_str or "rate" in err_str.lower():
            logger.warning("Spotify rate limit hit during track search for %s", artist_name)
            raise  # propagate rate limit so caller can stop
        logger.warning("Track search failed for %s: %s", artist_name, e)

    return None


def build_playlist(
    sp: spotipy.Spotify,
    candidates: List[Dict[str, Any]],
    playlist_size: int = None,
) -> Optional[Dict[str, Any]]:
    """Build a Spotify playlist from top candidates.

    Args:
        sp: Authenticated Spotify client
        candidates: Scored and ranked candidates (must have spotify_id)
        playlist_size: Max tracks in the playlist (default from config)

    Returns:
        Dict with playlist metadata: playlist_id, playlist_url, tracks, stats
        Returns None if playlist creation fails.
    """
    if playlist_size is None:
        playlist_size = config.PLAYLIST_SIZE

    if not candidates:
        logger.error("No candidates to build playlist from")
        return None

    # Get tracks for top candidates
    logger.info("Fetching top tracks for %d candidates...",
                min(len(candidates), playlist_size + 10))

    tracks = []
    skipped = 0
    for candidate in candidates:
        if len(tracks) >= playlist_size:
            break

        sid = candidate.get("spotify_id")
        if not sid:
            skipped += 1
            continue

        try:
            track = get_top_track(sp, candidate["name"], sid)
        except Exception:
            # Rate limit — stop fetching
            logger.warning("Stopping track fetch due to rate limit. Got %d tracks.", len(tracks))
            break

        if track:
            track["artist_spotify_id"] = sid
            track["composite_score"] = candidate.get("composite_score", 0)
            track["genre_match_score"] = candidate.get("genre_match_score", 0)
            tracks.append(track)
        else:
            skipped += 1
            logger.debug("No track found for %s (%s)", candidate["name"], sid)

    if not tracks:
        logger.error("Could not find any tracks for candidates")
        return None

    logger.info("Found %d tracks (%d candidates skipped)", len(tracks), skipped)

    # Create the playlist
    now = datetime.now()
    playlist_name = f"Niche Finds -- Week of {now.strftime('%b %d, %Y')}"

    try:
        # Use POST /v1/me/playlists directly — spotipy's user_playlist_create
        # uses /v1/users/{id}/playlists which is deprecated (403 in dev mode).
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
                "description": (
                    f"Underground & emerging artists matching your taste. "
                    f"{len(tracks)} tracks discovered by Music Finder."
                ),
            },
        )
        resp.raise_for_status()
        playlist = resp.json()
        playlist_id = playlist["id"]
        playlist_url = playlist["external_urls"]["spotify"]
        logger.info("Created playlist: %s (%s)", playlist_name, playlist_url)
    except Exception as e:
        logger.error("Failed to create playlist: %s", e)
        return None

    # Add tracks to playlist — use new /items endpoint directly.
    # spotipy's playlist_add_items uses deprecated /tracks which is 403 in dev mode.
    track_uris = [f"spotify:track:{t['track_id']}" for t in tracks]
    try:
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
        logger.info("Added %d tracks to playlist", len(tracks))
    except Exception as e:
        logger.error("Failed to add tracks to playlist: %s", e)
        # Playlist was created but empty — still return it
        return {
            "playlist_id": playlist_id,
            "playlist_name": playlist_name,
            "playlist_url": playlist_url,
            "tracks": [],
            "error": str(e),
        }

    # Save to database
    _save_playlist_history(playlist_id, playlist_name, tracks)

    # Build summary stats
    avg_score = sum(t["composite_score"] for t in tracks) / len(tracks)
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
) -> None:
    """Save playlist and track recommendations to the database."""
    conn = db.get_connection()
    try:
        # Insert playlist record
        cursor = conn.execute(
            """INSERT INTO playlist_history (playlist_id, playlist_name, track_count)
               VALUES (?, ?, ?)""",
            (playlist_id, playlist_name, len(tracks)),
        )
        history_id = cursor.lastrowid

        # Insert recommendation records
        for track in tracks:
            conn.execute(
                """INSERT INTO recommendations
                   (artist_spotify_id, track_spotify_id, playlist_history_id)
                   VALUES (?, ?, ?)""",
                (track["artist_spotify_id"], track["track_id"], history_id),
            )

        conn.commit()
        logger.info("Saved playlist history (id=%d) with %d recommendations",
                    history_id, len(tracks))
    except Exception as e:
        logger.error("Failed to save playlist history: %s", e)
    finally:
        conn.close()


def get_playlist_history(limit: int = 4) -> List[Dict[str, Any]]:
    """Get recent playlist history."""
    conn = db.get_connection()
    try:
        rows = conn.execute(
            """SELECT id, playlist_id, playlist_name, track_count, created_at
               FROM playlist_history ORDER BY created_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
