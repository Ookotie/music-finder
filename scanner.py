"""Scanner — Full pipeline orchestrator for Music Finder.

Runs the complete weekly pipeline:
  1. Authenticate Spotify
  2. Discover candidates (MusicBrainz + Last.fm + Spotify search)
  3. Score and rank
  4. Resolve Spotify IDs for top candidates
  5. Build playlist
  6. Format and send Telegram notification

Handles partial source failure, rate limits, and low-candidate fallback.
Returns structured result dict for status reporting.

In oni-hub integration, this is called by scheduler.py on CronTrigger.
"""

import logging
import time
from typing import Any, Dict, List, Optional

import config
import db
import spotify_client as sp_client
from discovery import run_discovery
from notification import format_notification, format_error_notification
from playlist_builder import build_playlist, get_playlist_history

logger = logging.getLogger(__name__)


def _try_track_error(e: Exception, context: str) -> None:
    """Track error to oni-hub's Error Encyclopedia if available."""
    try:
        from src.services.error_tracker import track_error
        track_error(e, project="Music Finder", context=context)
    except ImportError:
        pass  # Not running inside oni-hub
    except Exception:
        pass  # Error tracking should never crash the pipeline


def _try_send_telegram(message: str) -> bool:
    """Send Telegram notification if oni-hub's sender is available.

    Falls back to logging the message if not running inside oni-hub.
    """
    try:
        from src.scheduler.nudge_engine import send_telegram
        return send_telegram(message)
    except ImportError:
        logger.info("Telegram notification (not sent — standalone mode):\n%s", message)
        return False
    except Exception as e:
        logger.error("Telegram send failed: %s", e)
        return False


def run_music_scan() -> Dict[str, Any]:
    """Run the full music discovery pipeline.

    Returns:
        {
            "candidates_discovered": int,
            "candidates_scored": int,
            "playlist": {"id": str, "name": str, "url": str, "track_count": int} | None,
            "notification_sent": bool,
            "notification_text": str,
            "errors": list[str],
            "duration_sec": float,
        }
    """
    start_time = time.time()
    errors = []

    # Reset Spotify request counter for this run
    sp_client.reset_request_count()

    result = {
        "candidates_discovered": 0,
        "candidates_scored": 0,
        "playlist": None,
        "notification_sent": False,
        "notification_text": "",
        "errors": errors,
        "duration_sec": 0,
    }

    # Step 1: Check taste profile
    profile = db.get_taste_profile()
    if not profile:
        msg = "No taste profile found. Run taste profiler first."
        logger.error(msg)
        errors.append(msg)
        result["duration_sec"] = round(time.time() - start_time, 1)
        return result

    logger.info("Loaded taste profile: %d genres", len(profile))

    # Step 2: Authenticate Spotify
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

        # Send failure notification
        notif = format_error_notification(errors)
        result["notification_text"] = notif
        result["notification_sent"] = _try_send_telegram(notif)
        return result

    # Step 3: Run discovery
    try:
        candidates = run_discovery(sp)
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

    # Step 4: Save candidates to DB
    try:
        result["candidates_scored"] = len(candidates)
        db.save_candidates(candidates)
        logger.info("Saved %d candidates to database", len(candidates))
    except Exception as e:
        msg = f"Failed to save candidates: {e}"
        logger.error(msg)
        errors.append(msg)
        _try_track_error(e, "music.db_save")

    # Step 5: Build playlist
    playlist_result = None
    try:
        playlist_result = build_playlist(sp, candidates)
        if playlist_result:
            result["playlist"] = {
                "id": playlist_result["playlist_id"],
                "name": playlist_result["playlist_name"],
                "url": playlist_result["playlist_url"],
                "track_count": playlist_result["stats"]["track_count"],
            }
            logger.info("Playlist created: %s (%d tracks)",
                        playlist_result["playlist_name"],
                        playlist_result["stats"]["track_count"])
        else:
            errors.append("Playlist builder returned None")
    except Exception as e:
        msg = f"Playlist build failed: {e}"
        logger.error(msg)
        errors.append(msg)
        _try_track_error(e, "music.playlist_build")

    # Step 6: Send notification
    if playlist_result and not playlist_result.get("error"):
        notif = format_notification(playlist_result, candidates)
    elif playlist_result and playlist_result.get("error"):
        notif = format_notification(playlist_result, candidates)
        notif += f"\n\n(Playlist created but had issues: {playlist_result['error']})"
    else:
        notif = format_error_notification(errors)

    result["notification_text"] = notif
    result["notification_sent"] = _try_send_telegram(notif)
    result["duration_sec"] = round(time.time() - start_time, 1)

    logger.info("Music scan complete in %.1fs. Playlist: %s, Errors: %d, Spotify calls: %d",
                result["duration_sec"],
                "yes" if result["playlist"] else "no",
                len(errors),
                sp_client.get_request_count())

    return result
