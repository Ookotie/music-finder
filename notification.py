"""Notification — Telegram message formatting for Music Finder.

Formats the weekly playlist notification with:
  - Playlist link and basic stats
  - Top 5 artist highlights with listener counts
  - Genre trend summary
  - Discovery stats
"""

import logging
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def format_notification(
    playlist_result: Dict[str, Any],
    candidates: List[Dict[str, Any]],
) -> str:
    """Format the weekly Telegram notification.

    Args:
        playlist_result: Dict from playlist_builder.build_playlist()
        candidates: Full scored candidate list (for stats)

    Returns:
        Formatted message string (plain text, Telegram-safe).
    """
    tracks = playlist_result.get("tracks", [])
    stats = playlist_result.get("stats", {})
    playlist_url = playlist_result.get("playlist_url", "")
    playlist_name = playlist_result.get("playlist_name", "Niche Finds")

    lines = []

    # Header
    lines.append("Niche Finds -- Weekly Playlist")
    lines.append("")

    # Playlist link
    track_count = stats.get("track_count", len(tracks))
    duration = stats.get("total_duration_min", 0)
    lines.append(f"{track_count} tracks | ~{duration:.0f} min")
    lines.append(playlist_url)
    lines.append("")

    # Top 5 highlights
    if tracks:
        lines.append("Top picks:")
        for i, t in enumerate(tracks[:5], 1):
            artist = t.get("artist_name", "Unknown")
            track = t.get("track_name", "Unknown")
            lines.append(f"  {i}. {artist} - {track}")
        lines.append("")

    # Genre trends
    genre_summary = _get_genre_trends(candidates[:30])
    if genre_summary:
        lines.append(f"Genres: {genre_summary}")
        lines.append("")

    # Stats
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
    """Extract the dominant genres from top candidates.

    Returns a comma-separated string of the top 5 genres.
    """
    genre_counter: Counter = Counter()

    for c in top_candidates:
        genres = c.get("genres", [])
        if isinstance(genres, str):
            # Handle JSON-encoded genres from DB
            try:
                import json
                genres = json.loads(genres)
            except (json.JSONDecodeError, TypeError):
                genres = []

        for g in genres:
            genre_counter[g.lower().strip()] += 1

    if not genre_counter:
        return ""

    # Filter out non-music tags
    skip = {"english", "british", "american", "canadian", "australian",
            "german", "french", "swedish", "seen live", "favorites",
            "under 2000 listeners"}
    top = [
        genre for genre, _ in genre_counter.most_common(10)
        if genre not in skip
    ][:5]

    return ", ".join(top)
