"""Notification — Telegram message formatting for Music Finder.

Phase 5: Multi-playlist notifications with feedback summary,
genre cluster sections, and fresh finds.
"""

import json
import logging
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Day name for notification header
_DAY_NAMES = {
    0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday",
    4: "Friday", 5: "Saturday", 6: "Sunday",
}


def format_multi_notification(
    playlist_results: List[Dict[str, Any]],
    fresh_result: Optional[Dict[str, Any]],
    candidates: List[Dict[str, Any]],
    feedback_summary: Optional[Dict[str, Any]] = None,
    feedback_result: Optional[Dict[str, Any]] = None,
) -> str:
    """Format the multi-playlist Telegram notification.

    Args:
        playlist_results: List of dicts from build_clustered_playlists()
        fresh_result: Dict from build_fresh_playlist() or None
        candidates: Full scored candidate list (for stats)
        feedback_summary: Overall feedback stats
        feedback_result: Current run's feedback check result

    Returns:
        Formatted message string (plain text, Telegram-safe).
    """
    now = datetime.now()
    day_name = _DAY_NAMES.get(now.weekday(), "")
    lines = []

    # Header
    lines.append(f"Niche Finds -- {day_name} Night")
    lines.append("")

    # Feedback summary (if we checked feedback this run)
    if feedback_result and feedback_result.get("checked_count", 0) > 0:
        saved_count = len(feedback_result.get("saved", []))
        if saved_count > 0:
            # Show which genres were boosted
            boosted_genres = _extract_feedback_genres(feedback_result.get("saved", []))
            genre_str = ", ".join(f"+{g}" for g in boosted_genres[:3]) if boosted_genres else ""
            lines.append(f"Feedback: {saved_count} tracks saved from last run ({genre_str})")
        else:
            lines.append(f"Feedback: 0 tracks saved from {feedback_result['checked_count']} checked")
        lines.append("")

    # Genre cluster playlists
    for pr in playlist_results:
        tracks = pr.get("tracks", [])
        stats = pr.get("stats", {})
        cluster_name = pr.get("genre_cluster", pr.get("playlist_name", ""))
        track_count = stats.get("track_count", len(tracks))
        duration = stats.get("total_duration_min", 0)
        playlist_url = pr.get("playlist_url", "")

        lines.append(f"{cluster_name} ({track_count} tracks | ~{duration:.0f} min)")
        lines.append(f"  {playlist_url}")

        # Top 3 tracks from this cluster
        for i, t in enumerate(tracks[:3], 1):
            artist = t.get("artist_name", "Unknown")
            track = t.get("track_name", "Unknown")
            lines.append(f"  {i}. {artist} - {track}")
        lines.append("")

    # Fresh Finds
    if fresh_result:
        fresh_stats = fresh_result.get("stats", {})
        fresh_count = fresh_stats.get("track_count", 0)
        fresh_url = fresh_result.get("playlist_url", "")
        lines.append(f"Fresh Finds ({fresh_count} tracks, all released in last 6 months)")
        lines.append(f"  {fresh_url}")
        lines.append("")

    # Stats
    avg_score = 0
    if candidates:
        avg_score = sum(c.get("composite_score", 0) for c in candidates) / len(candidates)
    lines.append(f"Candidates scanned: {len(candidates)} | Avg match: {avg_score:.2f}")

    return "\n".join(lines)


def format_notification(
    playlist_result: Dict[str, Any],
    candidates: List[Dict[str, Any]],
) -> str:
    """Format a single-playlist notification (Phase 3 compatibility)."""
    tracks = playlist_result.get("tracks", [])
    stats = playlist_result.get("stats", {})
    playlist_url = playlist_result.get("playlist_url", "")

    lines = []
    lines.append("Niche Finds -- Weekly Playlist")
    lines.append("")

    track_count = stats.get("track_count", len(tracks))
    duration = stats.get("total_duration_min", 0)
    lines.append(f"{track_count} tracks | ~{duration:.0f} min")
    lines.append(playlist_url)
    lines.append("")

    if tracks:
        lines.append("Top picks:")
        for i, t in enumerate(tracks[:5], 1):
            artist = t.get("artist_name", "Unknown")
            track = t.get("track_name", "Unknown")
            lines.append(f"  {i}. {artist} - {track}")
        lines.append("")

    genre_summary = _get_genre_trends(candidates[:30])
    if genre_summary:
        lines.append(f"Genres: {genre_summary}")
        lines.append("")

    avg_score = stats.get("avg_composite_score", 0)
    lines.append(f"Avg match score: {avg_score:.2f} | "
                 f"Candidates scanned: {len(candidates)}")

    return "\n".join(lines)


def format_error_notification(errors: List[str]) -> str:
    """Format an error notification when the pipeline fails."""
    lines = [
        "Music Finder -- Scan Failed",
        "",
    ]

    if errors:
        for e in errors[:5]:
            lines.append(f"  - {e}")
    else:
        lines.append("  Unknown error — check logs")

    lines.append("")
    lines.append("Will retry next scheduled run.")

    return "\n".join(lines)


def _get_genre_trends(top_candidates: List[Dict[str, Any]]) -> str:
    """Extract the dominant genres from top candidates."""
    genre_counter: Counter = Counter()

    for c in top_candidates:
        genres = c.get("genres", [])
        if isinstance(genres, str):
            try:
                genres = json.loads(genres)
            except (json.JSONDecodeError, TypeError):
                genres = []

        for g in genres:
            genre_counter[g.lower().strip()] += 1

    if not genre_counter:
        return ""

    skip = {"english", "british", "american", "canadian", "australian",
            "german", "french", "swedish", "seen live", "favorites",
            "under 2000 listeners"}
    top = [
        genre for genre, _ in genre_counter.most_common(10)
        if genre not in skip
    ][:5]

    return ", ".join(top)


def _extract_feedback_genres(saved_items: List[Dict[str, Any]]) -> List[str]:
    """Extract genre names from saved feedback items for display."""
    genre_counts: Counter = Counter()
    for item in saved_items:
        genres_raw = item.get("genres", "[]")
        if isinstance(genres_raw, str):
            try:
                genres = json.loads(genres_raw)
            except (json.JSONDecodeError, TypeError):
                genres = []
        else:
            genres = genres_raw

        for g in genres:
            genre_counts[g.lower().strip()] += 1

    return [g for g, _ in genre_counts.most_common(5)]
