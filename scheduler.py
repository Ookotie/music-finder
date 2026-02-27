"""Scheduler — Registers the weekly music scan job.

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


def _run_scan_job() -> None:
    """Job callback — runs the full music scan pipeline.

    Called by APScheduler on the configured schedule.
    All errors are handled internally; this function never raises.
    """
    logger.info("Scheduled music scan starting...")
    try:
        from scanner import run_music_scan
        result = run_music_scan()

        if result.get("playlist"):
            logger.info("Scheduled scan complete: playlist created with %d tracks (%.1fs)",
                        result["playlist"]["track_count"], result["duration_sec"])
        else:
            logger.warning("Scheduled scan complete but no playlist created. Errors: %s",
                           result.get("errors", []))
    except Exception as e:
        logger.error("Scheduled music scan crashed: %s", e)
        # Try to notify about the crash
        try:
            from notification import format_error_notification
            from scanner import _try_send_telegram, _try_track_error
            _try_track_error(e, "music.scheduler")
            msg = format_error_notification([f"Scanner crashed: {e}"])
            _try_send_telegram(msg)
        except Exception:
            pass


def start_music_monitor(scheduler) -> None:
    """Register the weekly music scan job on an existing APScheduler.

    Args:
        scheduler: APScheduler BackgroundScheduler instance (shared with oni-hub).

    Schedule is configured via env vars:
        MUSIC_SCAN_DAY  (default: fri)
        MUSIC_SCAN_HOUR (default: 18 = 6 PM)
    """
    from apscheduler.triggers.cron import CronTrigger

    day = _DAY_MAP.get(config.MUSIC_SCAN_DAY.lower(), "fri")
    hour = config.MUSIC_SCAN_HOUR

    trigger = CronTrigger(
        day_of_week=day,
        hour=hour,
        minute=0,
        timezone=getattr(config, "TIMEZONE", "US/Eastern"),
    )

    scheduler.add_job(
        _run_scan_job,
        trigger=trigger,
        id="music_scan",
        name="Weekly Music Discovery Scan",
        replace_existing=True,
    )

    logger.info("Music monitor scheduled: every %s at %02d:00", day, hour)


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
    print(f"Candidates scored:     {result['candidates_scored']}")
    if result["playlist"]:
        p = result["playlist"]
        print(f"Playlist: {p['name']} ({p['track_count']} tracks)")
        print(f"URL: {p['url']}")
    else:
        print("Playlist: not created")
    if result["errors"]:
        print(f"Errors: {result['errors']}")
    print(f"Duration: {result['duration_sec']}s")
    print(f"Notification sent: {result['notification_sent']}")
    if result["notification_text"]:
        print(f"\n--- Notification ---\n{result['notification_text']}")
    print(f"{'='*60}")
