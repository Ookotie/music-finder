"""Notification — Telegram message formatting for Music Finder.

Two-playlist format: Deep Cuts + Fresh Finds, with genre cluster header.
"""

import json
import logging
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def format_scan_notification(
    playlists: Dict[str, Optional[Dict[str, Any]]],
    candidates_count: int = 0,
    genre_cluster: str = "",
    feedback_summary: Optional[Dict[str, Any]] = None,
    feedback_result: Optional[Dict[str, Any]] = None,
) -> str:
    """Format the two-playlist Telegram notification.

    Args:
        playlists: {"deep_cuts": {...}, "fresh_finds": {...}}
        candidates_count: Total candidates discovered
        genre_cluster: Genre family for this run
        feedback_summary: Overall feedback stats
        feedback_result: Current run's feedback check result

    Returns:
        Formatted message string (plain text, Telegram-safe).
    """
    now = datetime.now()
    day_name = now.strftime("%A")
    short_genre = genre_cluster.split(" / ")[0] if " / " in genre_cluster else genre_cluster
    lines = []

    # Header with genre vibe
    lines.append(f"Music Finder -- {short_genre} Night")
    lines.append("")

    # Feedback summary
    if feedback_result and feedback_result.get("checked_count", 0) > 0:
        saved_count = len(feedback_result.get("saved", []))
        if saved_count > 0:
            boosted_genres = _extract_feedback_genres(feedback_result.get("saved", []))
            genre_str = ", ".join(f"+{g}" for g in boosted_genres[:3]) if boosted_genres else ""
            lines.append(f"Feedback: {saved_count} tracks saved ({genre_str})")
        else:
            lines.append(f"Feedback: 0/{feedback_result['checked_count']} tracks saved")
        lines.append("")

    # Deep Cuts
    deep = playlists.get("deep_cuts")
    if deep:
        track_count = deep.get("track_count", 0)
        lines.append(f"Deep Cuts ({track_count} tracks)")
        lines.append(f"  Underground gems from {genre_cluster}")
        lines.append(f"  {deep.get('url', '')}")
        _add_highlights(lines, deep.get("tracks", []))
        lines.append("")

    # Fresh Finds
    fresh = playlists.get("fresh_finds")
    if fresh:
        track_count = fresh.get("track_count", 0)
        lines.append(f"Fresh Finds ({track_count} tracks)")
        lines.append(f"  What's breaking now in {genre_cluster}")
        lines.append(f"  {fresh.get('url', '')}")
        _add_highlights(lines, fresh.get("tracks", []))
        lines.append("")

    # Stats
    total_tracks = sum(
        p.get("track_count", 0)
        for p in playlists.values()
        if p is not None
    )
    lines.append(f"Total: {total_tracks} tracks | {candidates_count} candidates scanned")

    return "\n".join(lines)


def _add_highlights(lines: List[str], tracks: List[Dict[str, Any]], count: int = 3) -> None:
    """Add top track highlights to notification lines."""
    for i, t in enumerate(tracks[:count], 1):
        artist = t.get("artist_name", "Unknown")
        track = t.get("track_name", "Unknown")
        lines.append(f"  {i}. {artist} - {track}")


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
