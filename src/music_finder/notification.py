"""Notification — Telegram message formatting for Music Finder.

Multi-playlist format: Rising Stars, Deep Cuts, Genre Spotlight.
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
    feedback_summary: Optional[Dict[str, Any]] = None,
    feedback_result: Optional[Dict[str, Any]] = None,
) -> str:
    """Format the multi-playlist Telegram notification.

    Args:
        playlists: {"rising_stars": {...}, "deep_cuts": {...}, "genre_spotlight": {...}}
        candidates_count: Total candidates discovered
        feedback_summary: Overall feedback stats
        feedback_result: Current run's feedback check result

    Returns:
        Formatted message string (plain text, Telegram-safe).
    """
    now = datetime.now()
    date_str = now.strftime("%b %-d")
    lines = []

    # Header
    lines.append(f"Music Finder -- Week of {date_str}")
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

    # Rising Stars
    rising = playlists.get("rising_stars")
    if rising:
        track_count = rising.get("track_count", 0)
        lines.append(f"Rising Stars ({track_count} tracks)")
        lines.append(f"  New & trending artists matching your taste")
        lines.append(f"  {rising.get('url', '')}")
        _add_highlights(lines, rising.get("tracks", []))
        lines.append("")

    # Deep Cuts
    deep = playlists.get("deep_cuts")
    if deep:
        track_count = deep.get("track_count", 0)
        lines.append(f"Deep Cuts ({track_count} tracks)")
        lines.append(f"  Underground gems from your genre map")
        lines.append(f"  {deep.get('url', '')}")
        _add_highlights(lines, deep.get("tracks", []))
        lines.append("")

    # Genre Spotlight
    spotlight = playlists.get("genre_spotlight")
    if spotlight:
        track_count = spotlight.get("track_count", 0)
        genre = spotlight.get("genre", "")
        short_genre = genre.split(" / ")[0] if " / " in genre else genre
        lines.append(f"Genre Spotlight: {short_genre} ({track_count} tracks)")
        lines.append(f"  This week's deep dive")
        lines.append(f"  {spotlight.get('url', '')}")
        _add_highlights(lines, spotlight.get("tracks", []))
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


# Keep legacy format for backwards compatibility

def format_multi_notification(
    playlist_results: List[Dict[str, Any]],
    fresh_result: Optional[Dict[str, Any]],
    candidates: List[Dict[str, Any]],
    feedback_summary: Optional[Dict[str, Any]] = None,
    feedback_result: Optional[Dict[str, Any]] = None,
) -> str:
    """Legacy multi-playlist notification format (Phase 5 compat)."""
    now = datetime.now()
    lines = []
    lines.append(f"Niche Finds -- {now.strftime('%A')} Night")
    lines.append("")

    if feedback_result and feedback_result.get("checked_count", 0) > 0:
        saved_count = len(feedback_result.get("saved", []))
        if saved_count > 0:
            boosted_genres = _extract_feedback_genres(feedback_result.get("saved", []))
            genre_str = ", ".join(f"+{g}" for g in boosted_genres[:3]) if boosted_genres else ""
            lines.append(f"Feedback: {saved_count} tracks saved from last run ({genre_str})")
        else:
            lines.append(f"Feedback: 0 tracks saved from {feedback_result['checked_count']} checked")
        lines.append("")

    for pr in playlist_results:
        tracks = pr.get("tracks", [])
        stats = pr.get("stats", {})
        cluster_name = pr.get("genre_cluster", pr.get("playlist_name", ""))
        track_count = stats.get("track_count", len(tracks))
        duration = stats.get("total_duration_min", 0)
        playlist_url = pr.get("playlist_url", "")

        lines.append(f"{cluster_name} ({track_count} tracks | ~{duration:.0f} min)")
        lines.append(f"  {playlist_url}")

        for i, t in enumerate(tracks[:3], 1):
            artist = t.get("artist_name", "Unknown")
            track = t.get("track_name", "Unknown")
            lines.append(f"  {i}. {artist} - {track}")
        lines.append("")

    if fresh_result:
        fresh_stats = fresh_result.get("stats", {})
        fresh_count = fresh_stats.get("track_count", 0)
        fresh_url = fresh_result.get("playlist_url", "")
        lines.append(f"Fresh Finds ({fresh_count} tracks, all released in last 6 months)")
        lines.append(f"  {fresh_url}")
        lines.append("")

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
