"""Phase 1 runner — Build and display the taste profile.

Usage:
    python run_taste_profile.py
"""

import logging
import sys

import db
import spotify_client
import taste_profiler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    # Authenticate
    logger.info("Connecting to Spotify...")
    try:
        sp = spotify_client.get_client()
        user = sp.current_user()
        logger.info("Authenticated as: %s", user["display_name"])
    except Exception as e:
        logger.error("Spotify auth failed: %s", e)
        sys.exit(1)

    # Run taste profiler
    genre_weights, all_artists = taste_profiler.run_taste_profile(sp)

    if not genre_weights:
        logger.error("No genres found. Do you have listening history on Spotify?")
        sys.exit(1)

    # Save to SQLite
    logger.info("Saving to database...")
    db.save_taste_profile(genre_weights)
    db.save_seed_artists(all_artists)

    # Display results
    print("\n" + "=" * 60)
    print("TASTE PROFILE")
    print("=" * 60)

    print(f"\n{'GENRE':<35} {'WEIGHT':>8}")
    print("-" * 45)
    top_genres = list(genre_weights.items())[:30]
    for genre, weight in top_genres:
        bar = "#" * int(weight * 20)
        print(f"  {genre:<33} {weight:>6.3f}  {bar}")

    if len(genre_weights) > 30:
        print(f"  ... and {len(genre_weights) - 30} more genres")

    print(f"\n{'SEED ARTISTS':<35} {'POP':>5} {'GENRES'}")
    print("-" * 70)
    # Show top 20 by popularity
    sorted_artists = sorted(all_artists, key=lambda a: a.get("popularity", 0), reverse=True)
    for a in sorted_artists[:20]:
        genres_str = ", ".join(a.get("genres", [])[:3])
        print(f"  {a['name']:<33} {a.get('popularity', 0):>5}  {genres_str}")

    print(f"\n  Total artists: {len(all_artists)}")
    print(f"  Total genres:  {len(genre_weights)}")

    # Summary stats
    saved_profile = db.get_taste_profile()
    saved_artists = db.get_seed_artists()
    print(f"\n  Saved to DB: {len(saved_profile)} genres, {len(saved_artists)} artists")
    print(f"  DB path: {db.config.DB_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
