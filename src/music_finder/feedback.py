"""Feedback System — Automatic detection of liked/ignored recommendations.

Before each discovery run, checks the user's Spotify saved tracks library
against past recommendations. Saved = positive signal, not saved after 7 days
= negative signal. Adjusts taste profile weights accordingly.

API cost: 1-2 Spotify calls per feedback check.
"""

import json
import logging
from typing import Any, Dict, List, Set

import spotipy

from . import config
from . import db
from .spotify_client import _count_request

logger = logging.getLogger(__name__)


def check_feedback(sp: spotipy.Spotify) -> Dict[str, Any]:
    """Cross-reference Spotify saved tracks with past recommendations.

    Returns:
        {
            "saved": [{"rec_id", "track_id", "artist_id", "genres"}, ...],
            "not_saved": [{"rec_id", "track_id", "artist_id", "genres"}, ...],
            "checked_count": int,
            "already_processed": int,
        }
    """
    result = {
        "saved": [],
        "not_saved": [],
        "checked_count": 0,
        "already_processed": 0,
    }

    # Get unchecked recommendations older than FEEDBACK_WAIT_DAYS
    unchecked = db.get_unchecked_recommendations(config.FEEDBACK_WAIT_DAYS)
    if not unchecked:
        logger.info("No unchecked recommendations old enough for feedback check")
        return result

    result["checked_count"] = len(unchecked)
    logger.info("Checking feedback for %d recommendations", len(unchecked))

    # Fetch user's saved tracks (1-2 API calls covers months of saves)
    saved_track_ids = _get_saved_track_ids(sp)

    # Cross-reference
    feedback_records = []
    rec_ids = []

    for rec in unchecked:
        track_id = rec["track_spotify_id"]
        artist_id = rec["artist_spotify_id"]
        genres = rec.get("artist_genres", "[]")
        rec_id = rec["id"]
        rec_ids.append(rec_id)

        if track_id in saved_track_ids:
            result["saved"].append({
                "rec_id": rec_id,
                "track_id": track_id,
                "artist_id": artist_id,
                "genres": genres,
            })
            feedback_records.append({
                "artist_spotify_id": artist_id,
                "track_spotify_id": track_id,
                "feedback_type": "saved",
                "genres": genres,
            })
        else:
            result["not_saved"].append({
                "rec_id": rec_id,
                "track_id": track_id,
                "artist_id": artist_id,
                "genres": genres,
            })
            feedback_records.append({
                "artist_spotify_id": artist_id,
                "track_spotify_id": track_id,
                "feedback_type": "not_saved",
                "genres": genres,
            })

    # Save feedback and mark as checked
    db.save_feedback_batch(feedback_records)
    db.mark_recommendations_checked(rec_ids)

    logger.info("Feedback: %d saved, %d not saved",
                len(result["saved"]), len(result["not_saved"]))
    return result


def apply_feedback_to_taste_profile(feedback_result: Dict[str, Any]) -> Dict[str, float]:
    """Adjust taste profile genre weights based on feedback.

    Saved tracks: boost artist's genres by FEEDBACK_BOOST per save.
    Not-saved tracks: penalize genres by FEEDBACK_PENALTY.

    Returns dict of adjustments applied: {genre: delta}.
    """
    adjustments: Dict[str, float] = {}

    for item in feedback_result.get("saved", []):
        genres = _parse_genres(item.get("genres", "[]"))
        for genre in genres:
            g = genre.lower().strip()
            if g:
                adjustments[g] = adjustments.get(g, 0.0) + config.FEEDBACK_BOOST

    for item in feedback_result.get("not_saved", []):
        genres = _parse_genres(item.get("genres", "[]"))
        for genre in genres:
            g = genre.lower().strip()
            if g:
                adjustments[g] = adjustments.get(g, 0.0) - config.FEEDBACK_PENALTY

    if adjustments:
        db.adjust_taste_profile(adjustments)
        logger.info("Applied %d genre adjustments from feedback", len(adjustments))
    else:
        logger.info("No genre adjustments from feedback")

    return adjustments


def get_feedback_summary() -> Dict[str, Any]:
    """Get feedback stats for notification display.

    Returns:
        {
            "total_saved": int,
            "total_not_saved": int,
            "save_rate": float,
            "top_liked_genres": [(genre, count), ...],
        }
    """
    conn = db.get_connection()
    try:
        saved = conn.execute(
            "SELECT COUNT(*) as c FROM feedback WHERE feedback_type = 'saved'"
        ).fetchone()["c"]
        not_saved = conn.execute(
            "SELECT COUNT(*) as c FROM feedback WHERE feedback_type = 'not_saved'"
        ).fetchone()["c"]

        total = saved + not_saved
        save_rate = saved / total if total > 0 else 0.0

        # Top liked genres from saved feedback
        rows = conn.execute(
            "SELECT genres FROM feedback WHERE feedback_type = 'saved' AND genres IS NOT NULL"
        ).fetchall()

        genre_counts: Dict[str, int] = {}
        for row in rows:
            for g in _parse_genres(row["genres"]):
                g = g.lower().strip()
                if g:
                    genre_counts[g] = genre_counts.get(g, 0) + 1

        top_genres = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:5]

        return {
            "total_saved": saved,
            "total_not_saved": not_saved,
            "save_rate": round(save_rate, 2),
            "top_liked_genres": top_genres,
        }
    finally:
        conn.close()


def _get_saved_track_ids(sp: spotipy.Spotify) -> Set[str]:
    """Fetch user's saved tracks IDs (1-2 API calls)."""
    saved_ids = set()
    try:
        _count_request("api")
        results = sp.current_user_saved_tracks(limit=50)
        for item in results.get("items", []):
            track = item.get("track", {})
            if track.get("id"):
                saved_ids.add(track["id"])

        # Second page if available
        if results.get("next"):
            _count_request("api")
            results = sp.next(results)
            if results:
                for item in results.get("items", []):
                    track = item.get("track", {})
                    if track.get("id"):
                        saved_ids.add(track["id"])

    except Exception as e:
        logger.warning("Failed to fetch saved tracks: %s", e)

    logger.info("Loaded %d saved track IDs from Spotify", len(saved_ids))
    return saved_ids


def _parse_genres(genres_value) -> List[str]:
    """Parse genres from DB value (JSON string or list)."""
    if isinstance(genres_value, list):
        return genres_value
    if isinstance(genres_value, str):
        try:
            parsed = json.loads(genres_value)
            return parsed if isinstance(parsed, list) else []
        except (json.JSONDecodeError, TypeError):
            return []
    return []
