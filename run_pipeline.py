"""Full pipeline runner — CLI entry point for Music Finder.

Usage:
    python run_pipeline.py               # Full pipeline: feedback → discover → 3 playlists → notify
    python run_pipeline.py --discover    # Discovery only (no playlist)
    python run_pipeline.py --playlist    # Playlist from existing candidates
    python run_pipeline.py --dry-run     # Discovery + scoring, skip playlist creation
    python run_pipeline.py --feedback    # Run feedback check only
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
          f"{'POP':>5} {'SRC':>4} {'RECENCY':>7} {'LISTENERS':>12}")
    print("-" * 100)

    for i, c in enumerate(candidates[:limit], 1):
        listeners = c.get("lastfm_listeners", 0)
        listeners_str = f"{listeners:,}" if listeners else "—"
        recency = c.get("recency_score", 0)
        print(f"  {i:<3} {c['name'][:28]:<30} {c['composite_score']:>5.3f}  "
              f"{c['genre_match_score']:>5.3f} {c['popularity_score']:>5.2f} "
              f"{c.get('source_count', 1):>3}  {recency:>6.2f}  {listeners_str:>12}")

    if len(candidates) > limit:
        print(f"\n  ... and {len(candidates) - limit} more candidates")

    total = len(candidates)
    if total:
        avg_score = sum(c["composite_score"] for c in candidates) / total
        multi_src = sum(1 for c in candidates if c.get("source_count", 1) > 1)
        print(f"\n  Total: {total} | Avg score: {avg_score:.3f} | Multi-source: {multi_src}")


def print_playlists(result):
    """Print multi-playlist results."""
    print(f"\n{'='*60}")
    print("PLAYLISTS CREATED")
    print(f"{'='*60}")

    playlists = result.get("playlists", {})

    for ptype, label in [
        ("rising_stars", "Rising Stars"),
        ("deep_cuts", "Deep Cuts"),
        ("genre_spotlight", "Genre Spotlight"),
    ]:
        p = playlists.get(ptype)
        if p:
            genre_info = f" [{p['genre']}]" if p.get("genre") else ""
            print(f"\n  {label}{genre_info}")
            print(f"    Name:   {p['name']}")
            print(f"    URL:    {p['url']}")
            print(f"    Tracks: {p['track_count']}")
        else:
            print(f"\n  {label}: not created")

    print(f"{'='*60}")


def run_full():
    """Run via scanner — the production path."""
    result = run_music_scan()

    print(f"\n{'='*60}")
    print("SCAN RESULTS")
    print(f"{'='*60}")
    print(f"  Candidates discovered: {result['candidates_discovered']}")

    if result.get("feedback_summary"):
        fb = result["feedback_summary"]
        print(f"  Feedback: {fb['total_saved']} saved, {fb['total_not_saved']} not saved "
              f"(save rate: {fb['save_rate']:.0%})")

    playlists = result.get("playlists", {})
    total_tracks = 0
    for ptype, label in [
        ("rising_stars", "Rising Stars"),
        ("deep_cuts", "Deep Cuts"),
        ("genre_spotlight", "Genre Spotlight"),
    ]:
        p = playlists.get(ptype)
        if p:
            genre_info = f" [{p.get('genre', '')}]" if p.get("genre") else ""
            print(f"  {label}{genre_info}: {p['track_count']} tracks")
            print(f"    URL: {p['url']}")
            total_tracks += p["track_count"]
        else:
            print(f"  {label}: not created")

    print(f"  Total tracks: {total_tracks}")

    if result["errors"]:
        print(f"  Errors:")
        for e in result["errors"]:
            print(f"    - {e}")
    print(f"  Duration: {result['duration_sec']}s")
    print(f"  Spotify API calls: {spotify_client.get_request_count()}")

    if result["notification_text"]:
        print(f"\n--- Telegram Notification ---")
        try:
            print(result["notification_text"])
        except UnicodeEncodeError:
            print(result["notification_text"].encode("utf-8", errors="replace").decode("utf-8"))
        print(f"--- End ---")

    return 0 if total_tracks > 0 else 1


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
    print(f"  Spotify API calls: {spotify_client.get_request_count()}")
    return 0


def run_playlist_only():
    """Build playlist from existing DB candidates."""
    candidates = db.get_candidates(limit=config.PLAYLIST_SIZE + 10)
    if not candidates:
        logger.error("No candidates in database. Run discovery first.")
        return 1

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
        print(f"\n{'='*60}")
        print("PLAYLIST CREATED")
        print(f"{'='*60}")
        print(f"  Name: {result['playlist_name']}")
        print(f"  URL:  {result['playlist_url']}")
        stats = result.get("stats", {})
        print(f"  Tracks: {stats.get('track_count', 0)}")
        print(f"  Duration: {stats.get('total_duration_min', 0):.0f} min")
        print(f"{'='*60}")
        return 0
    else:
        logger.error("Playlist creation failed.")
        return 1


def run_dry():
    """Discovery + scoring without Spotify ID resolution or playlist.

    Shows how candidates would be scored under each profile.
    """
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

    print(f"\n{'='*60}")
    print("DRY RUN — Discovery + Multi-Profile Scoring (no Spotify)")
    print(f"{'='*60}")
    print(f"  Total candidates discovered: {len(candidates)}")

    for scoring_profile, label in [
        ("rising_stars", "RISING STARS"),
        ("deep_cuts", "DEEP CUTS"),
        ("default", "DEFAULT"),
    ]:
        scored = score_candidates([c.copy() for c in candidates], profile, profile=scoring_profile)
        print(f"\n--- {label} (profile={scoring_profile}) ---")
        print_candidates(scored, limit=20)

    return 0


def run_feedback_only():
    """Run feedback check only (no discovery or playlists)."""
    from feedback import check_feedback, apply_feedback_to_taste_profile, get_feedback_summary

    logger.info("Connecting to Spotify...")
    try:
        sp = spotify_client.get_client()
        sp.current_user()
    except Exception as e:
        logger.error("Spotify auth failed: %s", e)
        return 1

    result = check_feedback(sp)
    print(f"\n{'='*60}")
    print("FEEDBACK CHECK")
    print(f"{'='*60}")
    print(f"  Checked: {result['checked_count']} recommendations")
    print(f"  Saved (liked): {len(result['saved'])}")
    print(f"  Not saved: {len(result['not_saved'])}")

    if result["checked_count"] > 0:
        adjustments = apply_feedback_to_taste_profile(result)
        if adjustments:
            print(f"\n  Genre adjustments applied:")
            sorted_adj = sorted(adjustments.items(), key=lambda x: -abs(x[1]))
            for genre, delta in sorted_adj[:10]:
                sign = "+" if delta > 0 else ""
                print(f"    {genre}: {sign}{delta:.3f}")

    summary = get_feedback_summary()
    print(f"\n  Overall stats:")
    print(f"    Total saved: {summary['total_saved']}")
    print(f"    Total not saved: {summary['total_not_saved']}")
    print(f"    Save rate: {summary['save_rate']:.0%}")
    if summary.get("top_liked_genres"):
        genres = ", ".join(f"{g} ({c})" for g, c in summary["top_liked_genres"])
        print(f"    Top liked genres: {genres}")

    print(f"{'='*60}")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Music Finder Pipeline")
    parser.add_argument("--discover", action="store_true",
                        help="Discovery + scoring only (no playlist)")
    parser.add_argument("--playlist", action="store_true",
                        help="Build playlist from existing DB candidates")
    parser.add_argument("--dry-run", action="store_true",
                        help="Discovery + scoring without Spotify (MB + Last.fm only)")
    parser.add_argument("--feedback", action="store_true",
                        help="Run feedback check only")
    args = parser.parse_args()

    if args.feedback:
        sys.exit(run_feedback_only())
    elif args.dry_run:
        sys.exit(run_dry())
    elif args.discover:
        sys.exit(run_discover_only())
    elif args.playlist:
        sys.exit(run_playlist_only())
    else:
        sys.exit(run_full())


if __name__ == "__main__":
    main()
