"""Scheduler — Registers the 3x/week music scan jobs.

When integrated into oni-hub, this is imported in src/main.py:
    from src.monitors.music.scheduler import start_music_monitor
    start_music_monitor(scheduler)

Standalone usage (for dev/testing):
    python scheduler.py
"""

import logging

import config

logger = logging.getLogger(__name__)

_DAY_MAP = {
    "mon": "mon", "tue": "tue", "wed": "wed", "thu": "thu",
    "fri": "fri", "sat": "sat", "sun": "sun",
    "monday": "mon", "tuesday": "tue", "wednesday": "wed",
    "thursday": "thu", "friday": "fri", "saturday": "sat", "sunday": "sun",
}

# Run index for each day of the week (for seed/genre rotation)
_RUN_INDEX = {"sun": 0, "tue": 1, "thu": 2}


def _run_scan_job(run_index: int = None) -> None:
    """Job callback — runs the full music scan pipeline.

    Called by APScheduler on the configured schedule.
    All errors are handled internally; this function never raises.
    """
    logger.info("Scheduled music scan starting (run_index=%s)...", run_index)
    try:
        from scanner import run_music_scan
        result = run_music_scan(run_index=run_index)

        playlists = result.get("playlists", {})
        playlist_count = sum(1 for v in playlists.values() if v is not None)
        total_tracks = sum(
            p.get("track_count", 0) for p in playlists.values() if p is not None
        )
        if playlist_count > 0:
            logger.info("Scheduled scan complete: %d playlists, %d total tracks (%.1fs)",
                        playlist_count, total_tracks, result["duration_sec"])
        else:
            logger.warning("Scheduled scan complete but no playlists created. Errors: %s",
                           result.get("errors", []))
    except Exception as e:
        logger.error("Scheduled music scan crashed: %s", e)
        try:
            from notification import format_error_notification
            from scanner import _try_send_telegram, _try_track_error
            _try_track_error(e, "music.scheduler")
            msg = format_error_notification([f"Scanner crashed: {e}"])
            _try_send_telegram(msg)
        except Exception:
            pass


def start_music_monitor(scheduler) -> None:
    """Register the 3x/week music scan jobs on an existing APScheduler.

    Args:
        scheduler: APScheduler BackgroundScheduler instance (shared with oni-hub).

    Schedule is configured via env vars:
        MUSIC_SCAN_DAYS  (default: "sun,tue,thu")
        MUSIC_SCAN_HOUR  (default: 21 = 9 PM)
    """
    from apscheduler.triggers.cron import CronTrigger

    days_str = getattr(config, "MUSIC_SCAN_DAYS", "sun,tue,thu")
    hour = config.MUSIC_SCAN_HOUR
    tz = getattr(config, "TIMEZONE", "US/Eastern")

    days = [d.strip().lower() for d in days_str.split(",") if d.strip()]

    for day_raw in days:
        day = _DAY_MAP.get(day_raw, day_raw)
        run_index = _RUN_INDEX.get(day)
        job_id = f"music_scan_{day}"

        trigger = CronTrigger(
            day_of_week=day,
            hour=hour,
            minute=0,
            timezone=tz,
        )

        scheduler.add_job(
            _run_scan_job,
            trigger=trigger,
            id=job_id,
            name=f"Music Discovery Scan ({day.capitalize()})",
            kwargs={"run_index": run_index},
            replace_existing=True,
        )

        logger.info("Music scan scheduled: %s at %02d:00 (run_index=%s)", day, hour, run_index)


if __name__ == "__main__":
    """Standalone: run the scan immediately (for dev/testing)."""
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("musicbrainzngs").setLevel(logging.WARNING)

    print("Running music scan (standalone mode)...")
    from scanner import run_music_scan

    result = run_music_scan()

    print(f"\n{'='*60}")
    print(f"Candidates discovered: {result['candidates_discovered']}")

    playlists = result.get("playlists", {})
    for ptype, label in [
        ("rising_stars", "Rising Stars"),
        ("deep_cuts", "Deep Cuts"),
        ("genre_spotlight", "Genre Spotlight"),
    ]:
        p = playlists.get(ptype)
        if p:
            print(f"{label}: {p['name']} ({p['track_count']} tracks)")
            print(f"  URL: {p['url']}")
        else:
            print(f"{label}: not created")

    if result["errors"]:
        print(f"Errors: {result['errors']}")
    print(f"Duration: {result['duration_sec']}s")
    print(f"Notification sent: {result['notification_sent']}")
    if result["notification_text"]:
        print(f"\n--- Notification ---\n{result['notification_text']}")
    print(f"{'='*60}")
