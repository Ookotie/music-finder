"""Scanner — Full pipeline orchestrator for Music Finder.

Phase 5 pipeline:
  0. Check feedback from previous playlists (auto-learn)
  1. Authenticate Spotify
  2. Discover candidates (MusicBrainz + Last.fm, rotated by run_index)
  3. Score and rank (with momentum + feedback boost)
  4. Cluster by genre family
  5. Build genre-separated playlists + fresh finds
  6. Format and send Telegram notification

Handles partial source failure, rate limits, and low-candidate fallback.
Returns structured result dict for status reporting.
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional

import config
import db
import spotify_client as sp_client
from discovery import run_discovery
from feedback import check_feedback, apply_feedback_to_taste_profile, get_feedback_summary
from genre_cluster import cluster_candidates
from notification import format_multi_notification, format_error_notification
from playlist_builder import (
    build_clustered_playlists, build_fresh_playlist,
    fetch_tracks_for_candidates, build_playlist,
)

logger = logging.getLogger(__name__)

# Day of week to run_index mapping
_DAY_TO_INDEX = {
    6: 0,  # Sunday
    1: 1,  # Tuesday
    3: 2,  # Thursday
}


def _try_track_error(e: Exception, context: str) -> None:
    """Track error to oni-hub's Error Encyclopedia if available."""
    try:
        from src.services.error_tracker import track_error
        track_error(e, project="Music Finder", context=context)
    except ImportError:
        pass
    except Exception:
        pass


def _try_send_telegram(message: str) -> bool:
    """Send Telegram notification if oni-hub's sender is available."""
    try:
        from src.scheduler.nudge_engine import send_telegram
        return send_telegram(message)
    except ImportError:
        logger.info("Telegram notification (not sent — standalone mode):\n%s", message)
        return False
    except Exception as e:
        logger.error("Telegram send failed: %s", e)
        return False


def run_music_scan(run_index: int = None) -> Dict[str, Any]:
    """Run the full music discovery pipeline.

    Args:
        run_index: 0 (Sun), 1 (Tue), 2 (Thu) — controls seed/genre rotation.
                   None = auto-detect from day of week, or use all.

    Returns:
        {
            "candidates_discovered": int,
            "candidates_scored": int,
            "playlists": [{"id", "name", "url", "track_count", "genre_cluster"}, ...],
            "fresh_playlist": {"id", "name", "url", "track_count"} | None,
            "feedback_summary": {...} | None,
            "notification_sent": bool,
            "notification_text": str,
            "errors": list[str],
            "duration_sec": float,
        }
    """
    start_time = time.time()
    errors = []

    # Auto-detect run_index from day of week
    if run_index is None:
        from datetime import datetime
        weekday = datetime.now().weekday()
        run_index = _DAY_TO_INDEX.get(weekday)
        # If run on a non-scheduled day (e.g., manual run), use None (all seeds)

    # Reset Spotify request counter for this run
    sp_client.reset_request_count()

    result = {
        "candidates_discovered": 0,
        "candidates_scored": 0,
        "playlists": [],
        "fresh_playlist": None,
        "feedback_summary": None,
        "notification_sent": False,
        "notification_text": "",
        "errors": errors,
        "duration_sec": 0,
    }

    # Step 0: Check taste profile
    profile = db.get_taste_profile()
    if not profile:
        msg = "No taste profile found. Run taste profiler first."
        logger.error(msg)
        errors.append(msg)
        result["duration_sec"] = round(time.time() - start_time, 1)
        return result

    logger.info("Loaded taste profile: %d genres", len(profile))

    # Step 1: Authenticate Spotify
    try:
        sp = sp_client.get_client()
        user = sp.current_user()
        logger.info("Spotify authenticated as: %s", user["display_name"])
    except Exception as e:
        msg = f"Spotify auth failed: {e}"
        logger.error(msg)
        errors.append(msg)
        _try_track_error(e, "music.spotify_auth")
        result["duration_sec"] = round(time.time() - start_time, 1)
        notif = format_error_notification(errors)
        result["notification_text"] = notif
        result["notification_sent"] = _try_send_telegram(notif)
        return result

    # Step 2: Check feedback from previous playlists
    feedback_result = None
    feedback_adjustments = None
    try:
        feedback_result = check_feedback(sp)
        if feedback_result["checked_count"] > 0:
            feedback_adjustments = apply_feedback_to_taste_profile(feedback_result)
            logger.info("Feedback check: %d saved, %d not saved",
                        len(feedback_result["saved"]),
                        len(feedback_result["not_saved"]))
            # Reload taste profile after adjustments
            profile = db.get_taste_profile()
        result["feedback_summary"] = get_feedback_summary()
    except Exception as e:
        logger.warning("Feedback check failed (non-fatal): %s", e)
        _try_track_error(e, "music.feedback")

    # Build liked genres dict for scoring boost
    liked_genres = None
    if result.get("feedback_summary") and result["feedback_summary"].get("top_liked_genres"):
        liked_genres = dict(result["feedback_summary"]["top_liked_genres"])

    # Step 3: Run discovery
    try:
        candidates = run_discovery(sp, run_index=run_index)
        result["candidates_discovered"] = len(candidates)
        logger.info("Discovery complete: %d candidates", len(candidates))
    except Exception as e:
        msg = f"Discovery failed: {e}"
        logger.error(msg)
        errors.append(msg)
        _try_track_error(e, "music.discovery")
        candidates = []

    if not candidates:
        if not errors:
            errors.append("Discovery returned 0 candidates")
        result["duration_sec"] = round(time.time() - start_time, 1)
        notif = format_error_notification(errors)
        result["notification_text"] = notif
        result["notification_sent"] = _try_send_telegram(notif)
        return result

    # Step 4: Save listener snapshots for future momentum scoring
    try:
        snapshots = [
            (c["spotify_id"], c.get("lastfm_listeners", 0))
            for c in candidates
            if c.get("spotify_id") and c.get("lastfm_listeners")
        ]
        if snapshots:
            # Enrich with previous snapshots for momentum scoring
            artist_ids = [s[0] for s in snapshots]
            prev_snapshots = db.get_listener_snapshots(artist_ids)
            for c in candidates:
                sid = c.get("spotify_id")
                if sid and sid in prev_snapshots:
                    c["previous_listeners"] = prev_snapshots[sid]

            db.save_listener_snapshots(snapshots)
    except Exception as e:
        logger.warning("Listener snapshot save failed (non-fatal): %s", e)

    # Step 5: Re-score with momentum data and feedback boost
    try:
        from scorer import score_candidates
        genre_weights = dict(profile)
        candidates = score_candidates(candidates, genre_weights, liked_genres)
        result["candidates_scored"] = len(candidates)
    except Exception as e:
        logger.warning("Re-scoring failed (using existing scores): %s", e)
        result["candidates_scored"] = len(candidates)

    # Save candidates to DB
    try:
        db.save_candidates(candidates)
        logger.info("Saved %d candidates to database", len(candidates))
    except Exception as e:
        msg = f"Failed to save candidates: {e}"
        logger.error(msg)
        errors.append(msg)
        _try_track_error(e, "music.db_save")

    # Step 6: Cluster candidates by genre family
    genre_weights = dict(profile)
    clusters = cluster_candidates(candidates, genre_weights)
    logger.info("Clustered into %d genre families", len(clusters))

    # Step 7: Fetch tracks for all candidates (shared across playlists)
    all_tracks = fetch_tracks_for_candidates(sp, candidates, max_tracks=config.PLAYLIST_SIZE)

    if not all_tracks:
        errors.append("Could not find any tracks for candidates")
        result["duration_sec"] = round(time.time() - start_time, 1)
        notif = format_error_notification(errors)
        result["notification_text"] = notif
        result["notification_sent"] = _try_send_telegram(notif)
        return result

    # Group tracks by their cluster assignment
    track_clusters: Dict[str, List] = {}
    for track in all_tracks:
        cluster_name = track.get("genre_cluster", "Mixed")
        track_clusters.setdefault(cluster_name, []).append(track)

    # Step 8: Build genre cluster playlists
    playlist_results = []
    try:
        # Build a candidates-by-cluster dict for build_clustered_playlists
        # We need to pass candidates (not tracks) to the builder,
        # but only those that already have tracks fetched
        tracks_by_artist = {t["artist_spotify_id"]: t for t in all_tracks}
        cluster_candidates_with_tracks = {}
        for cluster_name, cluster_cands in clusters.items():
            with_tracks = [
                c for c in cluster_cands
                if c.get("spotify_id") in tracks_by_artist
            ]
            if with_tracks:
                cluster_candidates_with_tracks[cluster_name] = with_tracks

        playlist_results = build_clustered_playlists(sp, cluster_candidates_with_tracks)
        for pr in playlist_results:
            result["playlists"].append({
                "id": pr["playlist_id"],
                "name": pr["playlist_name"],
                "url": pr["playlist_url"],
                "track_count": pr["stats"]["track_count"],
                "genre_cluster": pr.get("genre_cluster", ""),
            })
    except Exception as e:
        msg = f"Cluster playlist build failed: {e}"
        logger.error(msg)
        errors.append(msg)
        _try_track_error(e, "music.playlist_clusters")

    # Step 9: Build fresh finds playlist
    try:
        fresh_result = build_fresh_playlist(sp, all_tracks)
        if fresh_result:
            result["fresh_playlist"] = {
                "id": fresh_result["playlist_id"],
                "name": fresh_result["playlist_name"],
                "url": fresh_result["playlist_url"],
                "track_count": fresh_result["stats"]["track_count"],
            }
    except Exception as e:
        logger.warning("Fresh finds playlist failed (non-fatal): %s", e)
        _try_track_error(e, "music.playlist_fresh")

    # Step 10: Send notification
    if playlist_results:
        notif = format_multi_notification(
            playlist_results=playlist_results,
            fresh_result=fresh_result if result.get("fresh_playlist") else None,
            candidates=candidates,
            feedback_summary=result.get("feedback_summary"),
            feedback_result=feedback_result,
        )
    else:
        notif = format_error_notification(errors if errors else ["No playlists created"])

    result["notification_text"] = notif
    result["notification_sent"] = _try_send_telegram(notif)
    result["duration_sec"] = round(time.time() - start_time, 1)

    logger.info("Music scan complete in %.1fs. Playlists: %d, Fresh: %s, Errors: %d, Spotify calls: %d",
                result["duration_sec"],
                len(result["playlists"]),
                "yes" if result.get("fresh_playlist") else "no",
                len(errors),
                sp_client.get_request_count())

    return result
