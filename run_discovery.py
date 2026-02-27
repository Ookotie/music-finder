"""Phase 2 runner — Discover and score candidate artists.

Usage:
    python run_discovery.py
"""

import logging
import sys

import db
import spotify_client
from discovery import run_discovery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
# Suppress noisy MusicBrainz XML parser warnings
logging.getLogger("musicbrainzngs").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def main():
    # Check taste profile exists
    profile = db.get_taste_profile()
    if not profile:
        logger.error("No taste profile found. Run 'python run_taste_profile.py' first.")
        sys.exit(1)

    logger.info("Loaded taste profile: %d genres", len(profile))

    # Authenticate with Spotify
    logger.info("Connecting to Spotify...")
    try:
        sp = spotify_client.get_client()
        user = sp.current_user()
        logger.info("Authenticated as: %s", user["display_name"])
    except Exception as e:
        logger.error("Spotify auth failed: %s", e)
        sys.exit(1)

    # Run discovery
    candidates = run_discovery(sp)

    if not candidates:
        logger.error("No candidates discovered. Check logs for errors.")
        sys.exit(1)

    # Save to database
    logger.info("Saving %d candidates to database...", len(candidates))
    db.save_candidates(candidates)

    # Display results
    print("\n" + "=" * 80)
    print("DISCOVERY RESULTS")
    print("=" * 80)

    print(f"\n{'#':<4} {'ARTIST':<30} {'COMPOSITE':>9} {'GENRE':>6} "
          f"{'POP':>5} {'SRC':>5} {'SOURCES'}")
    print("-" * 95)

    for i, c in enumerate(candidates[:50], 1):
        sources = c.get("discovery_sources", [])
        # Shorten source labels
        src_labels = []
        for s in sources[:3]:
            if s.startswith("mb_tag:"):
                src_labels.append(s[7:])
        src_str = ", ".join(src_labels)
        if len(sources) > 3:
            src_str += f" +{len(sources) - 3}"

        print(f"  {i:<3} {c['name']:<30} {c['composite_score']:>7.4f}  "
              f"{c['genre_match_score']:>5.3f} {c['popularity_score']:>5.2f} "
              f"{c['source_count']:>3}   {src_str}")

    total = len(candidates)
    if total > 50:
        print(f"\n  ... and {total - 50} more candidates")

    # Summary stats
    print(f"\n  Total candidates: {total}")
    avg_score = sum(c["composite_score"] for c in candidates) / total if total else 0
    avg_genre = sum(c["genre_match_score"] for c in candidates) / total if total else 0
    print(f"  Average composite score: {avg_score:.4f}")
    print(f"  Average genre match:     {avg_genre:.4f}")
    print(f"  Multi-source candidates: {sum(1 for c in candidates if c['source_count'] > 1)}")

    saved = db.get_candidates(5)
    print(f"\n  Saved to DB: {len(db.get_candidates(9999))} candidates")
    print(f"  DB path: {db.config.DB_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
