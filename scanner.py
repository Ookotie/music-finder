"""Scanner — Full pipeline orchestrator for Music Finder.

Multi-playlist pipeline:
  0. Authenticate Spotify + check feedback
  1. Discover candidates (all sources including Spotify recommendations)
  2. Score candidates 3x with different profiles (Rising Stars, Deep Cuts, Genre Spotlight)
  3. Apply per-playlist cooldowns
  4. Build 3 playlists
  5. Send Telegram notification

Handles partial source failure, rate limits, and low-candidate fallback.
Returns structured result dict for status reporting.
"""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import config
import db
import spotify_client as sp_client
from discovery import run_discovery
from feedback import check_feedback, apply_feedback_to_taste_profile, get_feedback_summary
from genre_cluster import get_next_spotlight_genre, get_spotlight_keywords
from notification import format_scan_notification, format_error_notification
from playlist_builder import build_playlist_from_profile
from scorer import score_candidates

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

    Produces 3 playlists: Rising Stars, Deep Cuts, Genre Spotlight.

    Args:
        run_index: 0 (Sun), 1 (Tue), 2 (Thu) — controls seed/genre rotation.
                   None = auto-detect from day of week, or use all.

    Returns:
        {
            "candidates_discovered": int,
            "playlists": {
                "rising_stars": {"id", "name", "url", "track_count"} | None,
                "deep_cuts": {"id", "name", "url", "track_count"} | None,
                "genre_spotlight": {"id", "name", "url", "track_count", "genre"} | None,
            },
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
        weekday = datetime.now().weekday()
        run_index = _DAY_TO_INDEX.get(weekday)

    # Reset Spotify request counter for this run
    sp_client.reset_request_count()

    result = {
        "candidates_discovered": 0,
        "playlists": {
            "rising_stars": None,
            "deep_cuts": None,
            "genre_spotlight": None,
        },
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
    genre_weights = dict(profile)

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
    try:
        feedback_result = check_feedback(sp)
        if feedback_result["checked_count"] > 0:
            apply_feedback_to_taste_profile(feedback_result)
            logger.info("Feedback check: %d saved, %d not saved",
                        len(feedback_result["saved"]),
                        len(feedback_result["not_saved"]))
            # Reload taste profile after adjustments
            profile = db.get_taste_profile()
            genre_weights = dict(profile)
        result["feedback_summary"] = get_feedback_summary()
    except Exception as e:
        logger.warning("Feedback check failed (non-fatal): %s", e)
        _try_track_error(e, "music.feedback")

    # Build liked genres dict for scoring boost
    liked_genres = None
    if result.get("feedback_summary") and result["feedback_summary"].get("top_liked_genres"):
        liked_genres = dict(result["feedback_summary"]["top_liked_genres"])

    # Step 3: Run discovery (all sources)
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
            artist_ids = [s[0] for s in snapshots]
            prev_snapshots = db.get_listener_snapshots(artist_ids)
            for c in candidates:
                sid = c.get("spotify_id")
                if sid and sid in prev_snapshots:
                    c["previous_listeners"] = prev_snapshots[sid]
            db.save_listener_snapshots(snapshots)
    except Exception as e:
        logger.warning("Listener snapshot save failed (non-fatal): %s", e)

    # Save all candidates to DB
    try:
        db.save_candidates(candidates)
    except Exception as e:
        logger.warning("Failed to save candidates: %s", e)

    # Step 5: Score candidates 3x with different profiles
    now = datetime.now()
    date_str = now.strftime("%b %d, %Y")

    # Get per-playlist cooldown sets
    cooldown_weeks = config.ARTIST_COOLDOWN_WEEKS
    rising_cooldown = db.get_recently_recommended(cooldown_weeks, "rising_stars")
    deep_cooldown = db.get_recently_recommended(cooldown_weeks, "deep_cuts")
    spotlight_cooldown = db.get_recently_recommended(cooldown_weeks, "genre_spotlight")

    # --- Rising Stars ---
    try:
        rising_scored = score_candidates(
            [c.copy() for c in candidates], genre_weights, liked_genres,
            profile="rising_stars",
        )
        # Apply per-playlist cooldown
        rising_scored = [c for c in rising_scored
                         if c.get("spotify_id") not in rising_cooldown]
        logger.info("Rising Stars: %d candidates after scoring + cooldown", len(rising_scored))

        rising_result = build_playlist_from_profile(
            sp, rising_scored, "rising_stars",
            target_size=config.RISING_STARS_SIZE,
            playlist_name=f"Rising Stars -- {date_str}",
            description="New & trending artists matching your taste. Discovered by Music Finder.",
        )
        if rising_result:
            result["playlists"]["rising_stars"] = {
                "id": rising_result["playlist_id"],
                "name": rising_result["playlist_name"],
                "url": rising_result["playlist_url"],
                "track_count": rising_result["stats"]["track_count"],
                "tracks": rising_result.get("tracks", []),
            }
    except Exception as e:
        msg = f"Rising Stars playlist failed: {e}"
        logger.error(msg)
        errors.append(msg)
        _try_track_error(e, "music.playlist_rising")

    # --- Deep Cuts ---
    try:
        deep_scored = score_candidates(
            [c.copy() for c in candidates], genre_weights, liked_genres,
            profile="deep_cuts",
        )
        deep_scored = [c for c in deep_scored
                       if c.get("spotify_id") not in deep_cooldown]
        logger.info("Deep Cuts: %d candidates after scoring + cooldown", len(deep_scored))

        deep_result = build_playlist_from_profile(
            sp, deep_scored, "deep_cuts",
            target_size=config.DEEP_CUTS_SIZE,
            playlist_name=f"Deep Cuts -- {date_str}",
            description="Underground gems from your genre map. Discovered by Music Finder.",
        )
        if deep_result:
            result["playlists"]["deep_cuts"] = {
                "id": deep_result["playlist_id"],
                "name": deep_result["playlist_name"],
                "url": deep_result["playlist_url"],
                "track_count": deep_result["stats"]["track_count"],
                "tracks": deep_result.get("tracks", []),
            }
    except Exception as e:
        msg = f"Deep Cuts playlist failed: {e}"
        logger.error(msg)
        errors.append(msg)
        _try_track_error(e, "music.playlist_deep")

    # --- Genre Spotlight ---
    try:
        spotlight_genre = get_next_spotlight_genre(genre_weights)
        spotlight_keywords = get_spotlight_keywords(spotlight_genre)
        logger.info("Genre Spotlight: %s", spotlight_genre)

        # Filter candidates to those matching the spotlight genre family
        spotlight_candidates = []
        for c in candidates:
            c_copy = c.copy()
            candidate_genres = c_copy.get("genres", [])
            if isinstance(candidate_genres, str):
                try:
                    candidate_genres = json.loads(candidate_genres)
                except (json.JSONDecodeError, TypeError):
                    candidate_genres = []

            # Check if candidate matches the spotlight genre
            candidate_genres_lower = {g.lower().strip() for g in candidate_genres}
            match = False
            for cg in candidate_genres_lower:
                if cg in spotlight_keywords:
                    match = True
                    break
                for kw in spotlight_keywords:
                    if kw in cg or cg in kw:
                        match = True
                        break
                if match:
                    break
            if match:
                spotlight_candidates.append(c_copy)

        spotlight_scored = score_candidates(
            spotlight_candidates, genre_weights, liked_genres,
            profile="genre_spotlight",
        )
        spotlight_scored = [c for c in spotlight_scored
                           if c.get("spotify_id") not in spotlight_cooldown]
        logger.info("Genre Spotlight (%s): %d candidates after scoring + cooldown",
                    spotlight_genre, len(spotlight_scored))

        # Short genre name for playlist title
        short_genre = spotlight_genre.split(" / ")[0] if " / " in spotlight_genre else spotlight_genre
        spotlight_result = build_playlist_from_profile(
            sp, spotlight_scored, "genre_spotlight",
            target_size=config.GENRE_SPOTLIGHT_SIZE,
            playlist_name=f"Genre Spotlight: {short_genre} -- {date_str}",
            description=f"Deep dive into {spotlight_genre}. Discovered by Music Finder.",
        )
        if spotlight_result:
            result["playlists"]["genre_spotlight"] = {
                "id": spotlight_result["playlist_id"],
                "name": spotlight_result["playlist_name"],
                "url": spotlight_result["playlist_url"],
                "track_count": spotlight_result["stats"]["track_count"],
                "genre": spotlight_genre,
                "tracks": spotlight_result.get("tracks", []),
            }
    except Exception as e:
        msg = f"Genre Spotlight playlist failed: {e}"
        logger.error(msg)
        errors.append(msg)
        _try_track_error(e, "music.playlist_spotlight")

    # Step 6: Send notification
    playlists = result["playlists"]
    any_created = any(v is not None for v in playlists.values())

    if any_created:
        notif = format_scan_notification(
            playlists=playlists,
            candidates_count=len(candidates),
            feedback_summary=result.get("feedback_summary"),
            feedback_result=feedback_result,
        )
    else:
        notif = format_error_notification(errors if errors else ["No playlists created"])

    result["notification_text"] = notif
    result["notification_sent"] = _try_send_telegram(notif)
    result["duration_sec"] = round(time.time() - start_time, 1)

    playlist_count = sum(1 for v in playlists.values() if v is not None)
    logger.info(
        "Music scan complete in %.1fs. Playlists: %d/3, Errors: %d, Spotify calls: %d",
        result["duration_sec"], playlist_count, len(errors),
        sp_client.get_request_count(),
    )

    return result
