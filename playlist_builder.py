"""Playlist Builder — Creates Spotify playlists from scored candidates.

Phase 3: Single playlist. Phase 5: Multi-playlist (genre clusters + fresh finds).

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

import config
import db

logger = logging.getLogger(__name__)


def get_top_track(
    sp: spotipy.Spotify, artist_name: str, artist_id: str
) -> Optional[Dict[str, Any]]:
    """Get the best track for an artist on Spotify.

    Returns dict with: track_id, track_name, artist_name, duration_ms,
                       preview_url, release_date
    Returns None if no track found.
    """
    from spotify_client import _count_request, SpotifyRateLimitError

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
    # Extract release date from album
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
    """Fetch top tracks for a list of candidates.

    Returns track dicts enriched with artist metadata.
    """
    if max_tracks is None:
        max_tracks = config.PLAYLIST_SIZE

    tracks = []
    skipped = 0
    for candidate in candidates:
        if len(tracks) >= max_tracks:
            break

        sid = candidate.get("spotify_id")
        if not sid:
            skipped += 1
            continue

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
            track["genre_cluster"] = candidate.get("genre_cluster", "Mixed")
            tracks.append(track)
        else:
            skipped += 1

    logger.info("Fetched %d tracks (%d skipped)", len(tracks), skipped)
    return tracks


def build_playlist(
    sp: spotipy.Spotify,
    candidates: List[Dict[str, Any]],
    playlist_size: int = None,
) -> Optional[Dict[str, Any]]:
    """Build a single Spotify playlist from top candidates (Phase 3 compatibility).

    Returns:
        Dict with playlist metadata: playlist_id, playlist_url, tracks, stats
        Returns None if playlist creation fails.
    """
    if playlist_size is None:
        playlist_size = config.PLAYLIST_SIZE

    if not candidates:
        logger.error("No candidates to build playlist from")
        return None

    logger.info("Fetching top tracks for %d candidates...",
                min(len(candidates), playlist_size + 10))

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


def build_clustered_playlists(
    sp: spotipy.Spotify,
    clusters: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """Build one Spotify playlist per genre cluster.

    Args:
        sp: Authenticated Spotify client
        clusters: {family_name: [candidates]} from genre_cluster.cluster_candidates()

    Returns:
        List of playlist result dicts.
    """
    now = datetime.now()
    date_str = now.strftime("%b %d")
    results = []

    # Sort clusters by size descending
    sorted_clusters = sorted(clusters.items(), key=lambda x: -len(x[1]))

    for family_name, candidates in sorted_clusters:
        if not candidates:
            continue

        # Fetch tracks for this cluster's candidates
        tracks = fetch_tracks_for_candidates(sp, candidates, max_tracks=len(candidates))
        if not tracks:
            logger.warning("No tracks found for cluster '%s'", family_name)
            continue

        # Short family name for playlist title
        short_name = family_name.split(" / ")[0] if " / " in family_name else family_name
        playlist_name = f"Niche Finds -- {short_name} -- {date_str}"

        result = _create_spotify_playlist(
            sp, playlist_name, tracks,
            description=f"{family_name} discoveries. {len(tracks)} tracks by Music Finder.",
        )
        if result:
            result["genre_cluster"] = family_name
            _save_playlist_history(result["playlist_id"], result["playlist_name"],
                                   tracks, genre_cluster=family_name)
            results.append(result)

    logger.info("Created %d genre cluster playlists", len(results))
    return results


def build_fresh_playlist(
    sp: spotipy.Spotify,
    all_tracks: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Build a playlist of tracks released within FRESH_RELEASE_MONTHS.

    Args:
        sp: Authenticated Spotify client
        all_tracks: All tracks from all cluster playlists

    Returns:
        Playlist result dict, or None if fewer than 5 fresh tracks.
    """
    cutoff = datetime.now() - timedelta(days=config.FRESH_RELEASE_MONTHS * 30)
    cutoff_str = cutoff.strftime("%Y-%m-%d")

    fresh_tracks = []
    for track in all_tracks:
        rd = track.get("release_date", "")
        if not rd:
            continue
        # Spotify release dates can be YYYY, YYYY-MM, or YYYY-MM-DD
        try:
            if len(rd) == 4:
                rd_full = f"{rd}-01-01"
            elif len(rd) == 7:
                rd_full = f"{rd}-01"
            else:
                rd_full = rd
            if rd_full >= cutoff_str:
                fresh_tracks.append(track)
        except (ValueError, TypeError):
            continue

    if len(fresh_tracks) < 5:
        logger.info("Only %d fresh tracks (need 5+) — skipping Fresh Finds playlist",
                     len(fresh_tracks))
        return None

    now = datetime.now()
    playlist_name = f"Fresh Finds -- {now.strftime('%b %d, %Y')}"

    result = _create_spotify_playlist(
        sp, playlist_name, fresh_tracks,
        description=f"New releases from the last {config.FRESH_RELEASE_MONTHS} months. "
                    f"{len(fresh_tracks)} tracks by Music Finder.",
    )
    if result:
        result["is_fresh"] = True
        _save_playlist_history(result["playlist_id"], result["playlist_name"],
                               fresh_tracks, genre_cluster="Fresh")

    return result


def _create_spotify_playlist(
    sp: spotipy.Spotify,
    playlist_name: str,
    tracks: List[Dict[str, Any]],
    description: str = None,
) -> Optional[Dict[str, Any]]:
    """Create a Spotify playlist and add tracks using raw HTTP.

    Returns playlist result dict or None on failure.
    """
    from spotify_client import _count_request

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
                    artist_name, artist_genres, release_date)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    track.get("artist_spotify_id"),
                    track["track_id"],
                    history_id,
                    track.get("artist_name"),
                    track.get("artist_genres"),
                    track.get("release_date"),
                ),
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
            """SELECT id, playlist_id, playlist_name, track_count,
                      genre_cluster, created_at
               FROM playlist_history ORDER BY created_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
