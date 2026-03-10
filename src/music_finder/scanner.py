"""Scanner — Full pipeline orchestrator for Music Finder.

Genre-first, two-playlist pipeline:
  0. Auto-seed taste profile if empty
  1. Authenticate Spotify + check feedback
  2. Pick genre cluster (rotating)
  3. Discover Deep Cuts (hidden gems, any era, genre-coherent)
  4. Discover Fresh Finds (what's breaking now, genre-coherent)
  5. Score, cooldown, build 2 playlists
  6. Send Telegram notification

Handles partial source failure, rate limits, and low-candidate fallback.
Returns structured result dict for status reporting.
"""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from . import config
from . import db
from . import spotify_client as sp_client
from .discovery import discover_deep_cuts, discover_fresh_finds
from .feedback import check_feedback, apply_feedback_to_taste_profile, get_feedback_summary
from .genre_cluster import get_next_spotlight_genre, get_spotlight_keywords
from .notification import format_scan_notification, format_error_notification
from .playlist_builder import build_playlist_from_profile
from .scorer import score_candidates

logger = logging.getLogger(__name__)


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

    Produces 2 genre-coherent playlists: Deep Cuts + Fresh Finds.

    Args:
        run_index: Unused (kept for backwards compat with scheduler).

    Returns:
        {
            "candidates_discovered": int,
            "genre_cluster": str,
            "playlists": {
                "deep_cuts": {"id", "name", "url", "track_count"} | None,
                "fresh_finds": {"id", "name", "url", "track_count"} | None,
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

    # Reset Spotify request counter for this run
    sp_client.reset_request_count()

    result = {
        "candidates_discovered": 0,
        "genre_cluster": None,
        "playlists": {
            "deep_cuts": None,
            "fresh_finds": None,
        },
        "feedback_summary": None,
        "notification_sent": False,
        "notification_text": "",
        "errors": errors,
        "duration_sec": 0,
    }

    # Step 0: Check taste profile — auto-seed if empty
    profile = db.get_taste_profile()
    if not profile:
        logger.info("No taste profile found — auto-seeding from Spotify history...")
        try:
            sp = sp_client.get_client()
            from . import taste_profiler
            genre_weights_raw, all_artists = taste_profiler.run_taste_profile(sp)
            if genre_weights_raw:
                db.save_taste_profile(genre_weights_raw)
                db.save_seed_artists(all_artists)
                profile = db.get_taste_profile()
                logger.info("Auto-seeded taste profile: %d genres, %d artists",
                            len(genre_weights_raw), len(all_artists))
            else:
                msg = "Auto-seed failed: no genres found from Spotify history."
                logger.error(msg)
                errors.append(msg)
                result["duration_sec"] = round(time.time() - start_time, 1)
                return result
        except Exception as e:
            msg = f"Auto-seed failed: {e}"
            logger.error(msg)
            errors.append(msg)
            _try_track_error(e, "music.auto_seed")
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

    # Step 3: Pick genre cluster for this run
    genre_cluster = get_next_spotlight_genre(genre_weights)
    genre_keywords = get_spotlight_keywords(genre_cluster)
    result["genre_cluster"] = genre_cluster
    logger.info("Genre cluster for this run: %s", genre_cluster)

    # Load seed artists
    seed_artists = db.get_seed_artists()

    # Step 4: Discover Deep Cuts
    deep_candidates = []
    try:
        deep_candidates = discover_deep_cuts(
            sp, genre_cluster, genre_keywords, genre_weights, seed_artists
        )
        logger.info("Deep Cuts discovery: %d candidates", len(deep_candidates))
    except Exception as e:
        msg = f"Deep Cuts discovery failed: {e}"
        logger.error(msg)
        errors.append(msg)
        _try_track_error(e, "music.discovery_deep")

    # Step 5: Discover Fresh Finds
    fresh_candidates = []
    try:
        fresh_candidates = discover_fresh_finds(
            sp, genre_cluster, genre_keywords, genre_weights, seed_artists
        )
        logger.info("Fresh Finds discovery: %d candidates", len(fresh_candidates))
    except Exception as e:
        msg = f"Fresh Finds discovery failed: {e}"
        logger.error(msg)
        errors.append(msg)
        _try_track_error(e, "music.discovery_fresh")

    total_candidates = len(deep_candidates) + len(fresh_candidates)
    result["candidates_discovered"] = total_candidates

    if not deep_candidates and not fresh_candidates:
        if not errors:
            errors.append("Discovery returned 0 candidates for both funnels")
        result["duration_sec"] = round(time.time() - start_time, 1)
        notif = format_error_notification(errors)
        result["notification_text"] = notif
        result["notification_sent"] = _try_send_telegram(notif)
        return result

    # Save listener snapshots for momentum scoring
    all_candidates = deep_candidates + fresh_candidates
    try:
        snapshots = [
            (c["spotify_id"], c.get("lastfm_listeners", 0))
            for c in all_candidates
            if c.get("spotify_id") and c.get("lastfm_listeners")
        ]
        if snapshots:
            artist_ids = [s[0] for s in snapshots]
            prev_snapshots = db.get_listener_snapshots(artist_ids)
            for c in all_candidates:
                sid = c.get("spotify_id")
                if sid and sid in prev_snapshots:
                    c["previous_listeners"] = prev_snapshots[sid]
            db.save_listener_snapshots(snapshots)
    except Exception as e:
        logger.warning("Listener snapshot save failed (non-fatal): %s", e)

    # Save all candidates to DB
    try:
        db.save_candidates(all_candidates)
    except Exception as e:
        logger.warning("Failed to save candidates: %s", e)

    # Step 6: Build playlists
    now = datetime.now()
    date_str = now.strftime("%b %d, %Y")
    short_genre = genre_cluster.split(" / ")[0] if " / " in genre_cluster else genre_cluster

    cooldown_weeks = config.ARTIST_COOLDOWN_WEEKS
    deep_cooldown = db.get_recently_recommended(cooldown_weeks, "deep_cuts")
    fresh_cooldown = db.get_recently_recommended(cooldown_weeks, "fresh_finds")

    # Build liked genres dict for scoring boost
    liked_genres = None
    if result.get("feedback_summary") and result["feedback_summary"].get("top_liked_genres"):
        liked_genres = dict(result["feedback_summary"]["top_liked_genres"])

    # --- Deep Cuts ---
    if deep_candidates:
        try:
            # Re-score with liked_genres boost
            deep_scored = score_candidates(
                [c.copy() for c in deep_candidates], genre_weights, liked_genres,
                profile="deep_cuts",
            )
            deep_scored = [c for c in deep_scored
                           if c.get("spotify_id") not in deep_cooldown]
            logger.info("Deep Cuts: %d candidates after scoring + cooldown", len(deep_scored))

            deep_result = build_playlist_from_profile(
                sp, deep_scored, "deep_cuts",
                target_size=config.DEEP_CUTS_SIZE,
                playlist_name=f"Deep Cuts: {short_genre} -- {date_str}",
                description=f"Underground gems from {genre_cluster}. Discovered by Music Finder.",
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

    # --- Fresh Finds ---
    if fresh_candidates:
        try:
            fresh_scored = score_candidates(
                [c.copy() for c in fresh_candidates], genre_weights, liked_genres,
                profile="fresh_finds",
            )
            fresh_scored = [c for c in fresh_scored
                            if c.get("spotify_id") not in fresh_cooldown]
            logger.info("Fresh Finds: %d candidates after scoring + cooldown", len(fresh_scored))

            fresh_result = build_playlist_from_profile(
                sp, fresh_scored, "fresh_finds",
                target_size=config.FRESH_FINDS_SIZE,
                playlist_name=f"Fresh Finds: {short_genre} -- {date_str}",
                description=f"What's breaking now in {genre_cluster}. Discovered by Music Finder.",
            )
            if fresh_result:
                result["playlists"]["fresh_finds"] = {
                    "id": fresh_result["playlist_id"],
                    "name": fresh_result["playlist_name"],
                    "url": fresh_result["playlist_url"],
                    "track_count": fresh_result["stats"]["track_count"],
                    "tracks": fresh_result.get("tracks", []),
                }
        except Exception as e:
            msg = f"Fresh Finds playlist failed: {e}"
            logger.error(msg)
            errors.append(msg)
            _try_track_error(e, "music.playlist_fresh")

    # Step 7: Send notification
    playlists = result["playlists"]
    any_created = any(v is not None for v in playlists.values())

    if any_created:
        notif = format_scan_notification(
            playlists=playlists,
            candidates_count=total_candidates,
            genre_cluster=genre_cluster,
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
        "Music scan complete in %.1fs. Genre: %s, Playlists: %d/2, Errors: %d, Spotify calls: %d",
        result["duration_sec"], genre_cluster, playlist_count, len(errors),
        sp_client.get_request_count(),
    )

    return result
