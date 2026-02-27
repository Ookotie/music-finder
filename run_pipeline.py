"""Full pipeline runner — CLI entry point for Music Finder.

Usage:
    python run_pipeline.py               # Full pipeline: discover → playlist → notify
    python run_pipeline.py --discover    # Discovery only (no playlist)
    python run_pipeline.py --playlist    # Playlist from existing candidates
    python run_pipeline.py --dry-run     # Discovery + scoring, skip playlist creation
"""

import argparse
import json
import logging
import sys

import config
import db
import spotify_client
from discovery import run_discovery
from notification import format_notification
from playlist_builder import build_playlist
from scanner import run_music_scan

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("musicbrainzngs").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def print_candidates(candidates, limit=30):
    """Print a formatted table of top candidates."""
    print(f"\n{'#':<4} {'ARTIST':<30} {'COMP':>6} {'GENRE':>6} "
          f"{'POP':>5} {'SRC':>4} {'LISTENERS':>12}")
    print("-" * 80)

    for i, c in enumerate(candidates[:limit], 1):
        listeners = c.get("lastfm_listeners", 0)
        listeners_str = f"{listeners:,}" if listeners else "—"
        print(f"  {i:<3} {c['name'][:28]:<30} {c['composite_score']:>5.3f}  "
              f"{c['genre_match_score']:>5.3f} {c['popularity_score']:>5.2f} "
              f"{c.get('source_count', 1):>3}  {listeners_str:>12}")

    if len(candidates) > limit:
        print(f"\n  ... and {len(candidates) - limit} more candidates")

    total = len(candidates)
    if total:
        avg_score = sum(c["composite_score"] for c in candidates) / total
        multi_src = sum(1 for c in candidates if c.get("source_count", 1) > 1)
        print(f"\n  Total: {total} | Avg score: {avg_score:.3f} | Multi-source: {multi_src}")


def print_playlist(result):
    """Print playlist creation results."""
    print(f"\n{'='*60}")
    print("PLAYLIST CREATED")
    print(f"{'='*60}")
    print(f"  Name: {result['playlist_name']}")
    print(f"  URL:  {result['playlist_url']}")
    stats = result.get("stats", {})
    print(f"  Tracks: {stats.get('track_count', 0)}")
    print(f"  Duration: {stats.get('total_duration_min', 0):.0f} min")
    print(f"  Avg score: {stats.get('avg_composite_score', 0):.3f}")

    print(f"\n  {'#':<4} {'TRACK':<35} {'ARTIST':<25} {'SCORE':>6}")
    print("  " + "-" * 73)
    for i, t in enumerate(result.get("tracks", []), 1):
        print(f"  {i:<4} {t['track_name'][:33]:<35} "
              f"{t['artist_name'][:23]:<25} {t['composite_score']:>5.3f}")
    print(f"{'='*60}")


def run_full():
    """Run via scanner — the production path."""
    result = run_music_scan()

    print(f"\n{'='*60}")
    print("SCAN RESULTS")
    print(f"{'='*60}")
    print(f"  Candidates discovered: {result['candidates_discovered']}")
    print(f"  Candidates scored:     {result['candidates_scored']}")
    if result["playlist"]:
        p = result["playlist"]
        print(f"  Playlist: {p['name']} ({p['track_count']} tracks)")
        print(f"  URL: {p['url']}")
    else:
        print("  Playlist: not created")
    if result["errors"]:
        print(f"  Errors:")
        for e in result["errors"]:
            print(f"    - {e}")
    print(f"  Duration: {result['duration_sec']}s")

    if result["notification_text"]:
        print(f"\n--- Telegram Notification ---")
        print(result["notification_text"])
        print(f"--- End ---")

    return 0 if result.get("playlist") else 1


def run_discover_only():
    """Discovery + scoring only, no playlist."""
    profile = db.get_taste_profile()
    if not profile:
        logger.error("No taste profile. Run 'python run_taste_profile.py' first.")
        return 1

    logger.info("Connecting to Spotify...")
    try:
        sp = spotify_client.get_client()
        sp.current_user()
    except Exception as e:
        logger.error("Spotify auth failed: %s", e)
        return 1

    candidates = run_discovery(sp)
    if not candidates:
        logger.error("Discovery produced no candidates.")
        return 1

    db.save_candidates(candidates)
    print(f"\n{'='*60}")
    print("DISCOVERY RESULTS")
    print(f"{'='*60}")
    print_candidates(candidates)
    print(f"\n  Saved {len(candidates)} candidates to DB")
    return 0


def run_playlist_only():
    """Build playlist from existing DB candidates."""
    candidates = db.get_candidates(limit=config.PLAYLIST_SIZE + 10)
    if not candidates:
        logger.error("No candidates in database. Run discovery first.")
        return 1

    # Candidates from DB are dicts with string genres — parse them
    for c in candidates:
        if isinstance(c.get("genres"), str):
            try:
                c["genres"] = json.loads(c["genres"])
            except (json.JSONDecodeError, TypeError):
                c["genres"] = []

    logger.info("Loaded %d candidates from DB", len(candidates))
    logger.info("Connecting to Spotify...")
    try:
        sp = spotify_client.get_client()
        sp.current_user()
    except Exception as e:
        logger.error("Spotify auth failed: %s", e)
        return 1

    result = build_playlist(sp, candidates)
    if result:
        print_playlist(result)

        notif = format_notification(result, candidates)
        print(f"\n--- Telegram Notification ---")
        print(notif)
        print(f"--- End ---")
        return 0
    else:
        logger.error("Playlist creation failed.")
        return 1


def run_dry():
    """Discovery + scoring without Spotify ID resolution or playlist."""
    import lastfm_client
    import musicbrainz_client
    from discovery import (
        discover_from_musicbrainz, discover_from_lastfm,
        _merge_candidates, _filter_mainstream,
    )
    from scorer import score_candidates

    profile = dict(db.get_taste_profile())
    if not profile:
        logger.error("No taste profile. Run 'python run_taste_profile.py' first.")
        return 1

    seed_artists = db.get_seed_artists()
    seed_ids = {a["spotify_id"] for a in seed_artists}
    seed_names = {a["name"].lower().strip() for a in seed_artists}

    # Discover (no Spotify needed)
    mb_candidates = discover_from_musicbrainz(profile, seed_ids)
    lfm_candidates = discover_from_lastfm(profile, seed_artists)
    all_candidates = mb_candidates + lfm_candidates

    candidates = _merge_candidates(all_candidates)
    candidates = [c for c in candidates if c["name"].lower().strip() not in seed_names]
    candidates = _filter_mainstream(candidates)

    # Enrich + score
    lastfm_client.enrich_with_listeners(candidates, "dry-run")
    needs_genres = [c for c in candidates if not c.get("genres")]
    if needs_genres:
        musicbrainz_client.enrich_artists_with_genres(candidates[:200], "dry-run")
    scored = score_candidates(candidates, profile)

    print(f"\n{'='*60}")
    print("DRY RUN — Discovery + Scoring (no Spotify)")
    print(f"{'='*60}")
    print_candidates(scored, limit=40)
    return 0


def main():
    parser = argparse.ArgumentParser(description="Music Finder Pipeline")
    parser.add_argument("--discover", action="store_true",
                        help="Discovery + scoring only (no playlist)")
    parser.add_argument("--playlist", action="store_true",
                        help="Build playlist from existing DB candidates")
    parser.add_argument("--dry-run", action="store_true",
                        help="Discovery + scoring without Spotify (MB + Last.fm only)")
    args = parser.parse_args()

    if args.dry_run:
        sys.exit(run_dry())
    elif args.discover:
        sys.exit(run_discover_only())
    elif args.playlist:
        sys.exit(run_playlist_only())
    else:
        sys.exit(run_full())


if __name__ == "__main__":
    main()
