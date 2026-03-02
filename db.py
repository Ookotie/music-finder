"""SQLite database setup and operations for Music Finder."""

import json
import logging
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

import config

logger = logging.getLogger(__name__)

# Schema for Phase 1 tables (remaining tables added in later phases)
_SCHEMA = """
CREATE TABLE IF NOT EXISTS taste_profile (
    genre TEXT PRIMARY KEY,
    weight REAL NOT NULL DEFAULT 0.0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS seed_artists (
    spotify_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    genres TEXT,  -- JSON array
    popularity INTEGER DEFAULT 0,
    followers INTEGER DEFAULT 0,
    monthly_listeners INTEGER,
    image_url TEXT,
    source TEXT,  -- 'top_artists', 'top_tracks', 'followed'
    time_range TEXT,  -- 'short_term', 'medium_term', 'long_term', or NULL
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS candidates (
    spotify_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    genres TEXT,
    popularity INTEGER DEFAULT 0,
    followers INTEGER DEFAULT 0,
    monthly_listeners INTEGER,
    discovery_source TEXT,
    genre_match_score REAL DEFAULT 0.0,
    popularity_score REAL DEFAULT 0.0,
    source_diversity_bonus REAL DEFAULT 0.0,
    momentum_score REAL DEFAULT 0.0,
    composite_score REAL DEFAULT 0.0,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS playlist_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    playlist_id TEXT NOT NULL,
    playlist_name TEXT NOT NULL,
    track_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS recommendations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_spotify_id TEXT NOT NULL,
    track_spotify_id TEXT NOT NULL,
    playlist_history_id INTEGER REFERENCES playlist_history(id),
    recommended_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_spotify_id TEXT NOT NULL,
    feedback_type TEXT NOT NULL,  -- 'thumbs_up', 'thumbs_down', 'love'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS listener_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_spotify_id TEXT NOT NULL,
    monthly_listeners INTEGER,
    snapshot_date DATE DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS spotify_id_cache (
    artist_name_lower TEXT PRIMARY KEY,
    spotify_id TEXT,
    cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS rss_articles_seen (
    article_url TEXT PRIMARY KEY,
    blog_name TEXT,
    artist_extracted TEXT,
    seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS track_cache (
    artist_spotify_id TEXT NOT NULL,
    track_id TEXT NOT NULL,
    track_name TEXT,
    artist_name TEXT,
    duration_ms INTEGER DEFAULT 0,
    preview_url TEXT,
    release_date TEXT,
    cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (artist_spotify_id, track_id)
);

CREATE TABLE IF NOT EXISTS spotlight_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    genre_family TEXT NOT NULL,
    used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def get_connection() -> sqlite3.Connection:
    """Get a database connection, creating the DB and tables if needed."""
    os.makedirs(os.path.dirname(config.DB_PATH), exist_ok=True)
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    _migrate_schema(conn)
    return conn


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Add Phase 5 columns via ALTER TABLE (idempotent)."""
    migrations = [
        ("recommendations", "feedback_checked", "INTEGER DEFAULT 0"),
        ("recommendations", "artist_name", "TEXT"),
        ("recommendations", "artist_genres", "TEXT"),
        ("recommendations", "release_date", "TEXT"),
        ("feedback", "track_spotify_id", "TEXT"),
        ("feedback", "genres", "TEXT"),
        ("playlist_history", "genre_cluster", "TEXT"),
        ("recommendations", "playlist_type", "TEXT"),
    ]
    for table, column, col_type in migrations:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
        except sqlite3.OperationalError:
            pass  # Column already exists


def save_taste_profile(genre_weights: Dict[str, float]) -> None:
    """Upsert the weighted genre map into the taste_profile table."""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM taste_profile")
        conn.executemany(
            "INSERT INTO taste_profile (genre, weight) VALUES (?, ?)",
            [(genre, weight) for genre, weight in genre_weights.items()],
        )
        conn.commit()
        logger.info("Saved %d genres to taste_profile", len(genre_weights))
    finally:
        conn.close()


def save_seed_artists(artists: List[Dict[str, Any]]) -> None:
    """Upsert seed artists into the seed_artists table."""
    conn = get_connection()
    try:
        for a in artists:
            genres_json = json.dumps(a.get("genres", []))
            conn.execute(
                """INSERT INTO seed_artists
                   (spotify_id, name, genres, popularity, followers, image_url, source, time_range)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(spotify_id) DO UPDATE SET
                     name=excluded.name,
                     genres=excluded.genres,
                     popularity=excluded.popularity,
                     followers=excluded.followers,
                     image_url=excluded.image_url,
                     source=excluded.source,
                     time_range=excluded.time_range,
                     updated_at=CURRENT_TIMESTAMP
                """,
                (
                    a["spotify_id"], a["name"], genres_json,
                    a.get("popularity", 0), a.get("followers", 0),
                    a.get("image_url"), a.get("source"), a.get("time_range"),
                ),
            )
        conn.commit()
        logger.info("Saved %d seed artists", len(artists))
    finally:
        conn.close()


def get_taste_profile() -> List[Tuple[str, float]]:
    """Return the genre weights sorted descending."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT genre, weight FROM taste_profile ORDER BY weight DESC"
        ).fetchall()
        return [(r["genre"], r["weight"]) for r in rows]
    finally:
        conn.close()


def get_seed_artists() -> List[Dict[str, Any]]:
    """Return all seed artists."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM seed_artists ORDER BY popularity DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def save_candidates(candidates: List[Dict[str, Any]]) -> None:
    """Save scored candidates to the candidates table."""
    conn = get_connection()
    try:
        # Clear previous candidates
        conn.execute("DELETE FROM candidates")
        for c in candidates:
            genres_json = json.dumps(c.get("genres", []))
            sources_json = json.dumps(c.get("discovery_sources", []))
            conn.execute(
                """INSERT INTO candidates
                   (spotify_id, name, genres, discovery_source,
                    genre_match_score, popularity_score,
                    source_diversity_bonus, momentum_score, composite_score)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(spotify_id) DO UPDATE SET
                     name=excluded.name,
                     genres=excluded.genres,
                     discovery_source=excluded.discovery_source,
                     genre_match_score=excluded.genre_match_score,
                     popularity_score=excluded.popularity_score,
                     source_diversity_bonus=excluded.source_diversity_bonus,
                     momentum_score=excluded.momentum_score,
                     composite_score=excluded.composite_score,
                     discovered_at=CURRENT_TIMESTAMP
                """,
                (
                    c.get("spotify_id"), c["name"], genres_json, sources_json,
                    c.get("genre_match_score", 0), c.get("popularity_score", 0),
                    c.get("source_diversity_bonus", 0), c.get("momentum_score", 0),
                    c.get("composite_score", 0),
                ),
            )
        conn.commit()
        logger.info("Saved %d candidates", len(candidates))
    finally:
        conn.close()


def get_candidates(limit: int = 50) -> List[Dict[str, Any]]:
    """Return top candidates sorted by composite score."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM candidates ORDER BY composite_score DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# --- Spotify ID Cache ---

def get_cached_spotify_id(artist_name: str) -> Optional[str]:
    """Look up a cached Spotify ID by artist name. Returns None if not cached."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT spotify_id FROM spotify_id_cache WHERE artist_name_lower = ?",
            (artist_name.lower().strip(),),
        ).fetchone()
        return row["spotify_id"] if row else None
    finally:
        conn.close()


def get_cached_spotify_ids(artist_names: List[str]) -> Dict[str, Optional[str]]:
    """Bulk lookup cached Spotify IDs. Returns {name_lower: spotify_id or None}."""
    conn = get_connection()
    try:
        names_lower = [n.lower().strip() for n in artist_names]
        placeholders = ",".join("?" for _ in names_lower)
        rows = conn.execute(
            f"SELECT artist_name_lower, spotify_id FROM spotify_id_cache "
            f"WHERE artist_name_lower IN ({placeholders})",
            names_lower,
        ).fetchall()
        return {r["artist_name_lower"]: r["spotify_id"] for r in rows}
    finally:
        conn.close()


def cache_spotify_id(artist_name: str, spotify_id: str) -> None:
    """Cache a Spotify ID for an artist name."""
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO spotify_id_cache (artist_name_lower, spotify_id)
               VALUES (?, ?)
               ON CONFLICT(artist_name_lower) DO UPDATE SET
                 spotify_id=excluded.spotify_id,
                 cached_at=CURRENT_TIMESTAMP""",
            (artist_name.lower().strip(), spotify_id),
        )
        conn.commit()
    finally:
        conn.close()


def cache_spotify_ids_bulk(mappings: List[Tuple[str, str]]) -> None:
    """Bulk cache Spotify IDs. mappings = [(artist_name, spotify_id), ...]."""
    conn = get_connection()
    try:
        conn.executemany(
            """INSERT INTO spotify_id_cache (artist_name_lower, spotify_id)
               VALUES (?, ?)
               ON CONFLICT(artist_name_lower) DO UPDATE SET
                 spotify_id=excluded.spotify_id,
                 cached_at=CURRENT_TIMESTAMP""",
            [(name.lower().strip(), sid) for name, sid in mappings],
        )
        conn.commit()
        logger.info("Cached %d Spotify IDs", len(mappings))
    finally:
        conn.close()


# --- Phase 5: Feedback & Momentum ---

def get_unchecked_recommendations(min_age_days: int = 7) -> List[Dict[str, Any]]:
    """Get recommendations older than min_age_days that haven't been feedback-checked."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT id, artist_spotify_id, track_spotify_id, artist_name,
                      artist_genres, recommended_at
               FROM recommendations
               WHERE feedback_checked = 0
                 AND recommended_at < datetime('now', ?)""",
            (f"-{min_age_days} days",),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def mark_recommendations_checked(rec_ids: List[int]) -> None:
    """Flag recommendations as feedback-processed."""
    if not rec_ids:
        return
    conn = get_connection()
    try:
        placeholders = ",".join("?" for _ in rec_ids)
        conn.execute(
            f"UPDATE recommendations SET feedback_checked = 1 WHERE id IN ({placeholders})",
            rec_ids,
        )
        conn.commit()
        logger.info("Marked %d recommendations as feedback-checked", len(rec_ids))
    finally:
        conn.close()


def save_feedback_batch(records: List[Dict[str, Any]]) -> None:
    """Bulk insert feedback records.

    Each record: {artist_spotify_id, track_spotify_id, feedback_type, genres}
    """
    if not records:
        return
    conn = get_connection()
    try:
        conn.executemany(
            """INSERT INTO feedback
               (artist_spotify_id, feedback_type, track_spotify_id, genres)
               VALUES (?, ?, ?, ?)""",
            [
                (r["artist_spotify_id"], r["feedback_type"],
                 r.get("track_spotify_id"), r.get("genres"))
                for r in records
            ],
        )
        conn.commit()
        logger.info("Saved %d feedback records", len(records))
    finally:
        conn.close()


def adjust_taste_profile(adjustments: Dict[str, float]) -> None:
    """Apply genre weight deltas and renormalize to 0-1.

    adjustments: {genre: delta} where delta can be positive or negative.
    """
    if not adjustments:
        return
    conn = get_connection()
    try:
        # Load current profile
        rows = conn.execute("SELECT genre, weight FROM taste_profile").fetchall()
        weights = {r["genre"]: r["weight"] for r in rows}

        # Apply deltas
        for genre, delta in adjustments.items():
            current = weights.get(genre, 0.0)
            weights[genre] = max(current + delta, 0.01)  # floor at 0.01

        # Renormalize to 0-1
        max_w = max(weights.values()) if weights else 1.0
        if max_w > 0:
            weights = {g: w / max_w for g, w in weights.items()}

        # Save
        conn.execute("DELETE FROM taste_profile")
        conn.executemany(
            "INSERT INTO taste_profile (genre, weight) VALUES (?, ?)",
            list(weights.items()),
        )
        conn.commit()
        logger.info("Adjusted taste profile: %d changes, %d total genres",
                    len(adjustments), len(weights))
    finally:
        conn.close()


def save_listener_snapshots(snapshots: List[Tuple[str, int]]) -> None:
    """Save listener count snapshots. snapshots = [(artist_spotify_id, listeners), ...]."""
    if not snapshots:
        return
    conn = get_connection()
    try:
        conn.executemany(
            """INSERT INTO listener_snapshots (artist_spotify_id, monthly_listeners)
               VALUES (?, ?)""",
            snapshots,
        )
        conn.commit()
        logger.info("Saved %d listener snapshots", len(snapshots))
    finally:
        conn.close()


def get_listener_snapshots(artist_ids: List[str], days_back: int = 14) -> Dict[str, int]:
    """Get the most recent previous listener snapshot for each artist.

    Returns {artist_spotify_id: monthly_listeners} for snapshots within days_back.
    """
    if not artist_ids:
        return {}
    conn = get_connection()
    try:
        placeholders = ",".join("?" for _ in artist_ids)
        rows = conn.execute(
            f"""SELECT artist_spotify_id, monthly_listeners
                FROM listener_snapshots
                WHERE artist_spotify_id IN ({placeholders})
                  AND snapshot_date >= date('now', ?)
                ORDER BY snapshot_date DESC""",
            artist_ids + [f"-{days_back} days"],
        ).fetchall()
        # Return the most recent snapshot per artist
        result = {}
        for r in rows:
            aid = r["artist_spotify_id"]
            if aid not in result:
                result[aid] = r["monthly_listeners"]
        return result
    finally:
        conn.close()


# --- RSS Article Deduplication ---

def get_seen_article_urls() -> set:
    """Return set of article URLs already processed."""
    conn = get_connection()
    try:
        rows = conn.execute("SELECT article_url FROM rss_articles_seen").fetchall()
        return {r["article_url"] for r in rows}
    finally:
        conn.close()


def save_seen_articles(articles: List[Dict[str, str]]) -> None:
    """Save processed RSS articles.

    Each article: {url, blog_name, artist_extracted}
    """
    if not articles:
        return
    conn = get_connection()
    try:
        conn.executemany(
            """INSERT OR IGNORE INTO rss_articles_seen
               (article_url, blog_name, artist_extracted)
               VALUES (?, ?, ?)""",
            [(a["url"], a["blog_name"], a.get("artist_extracted", ""))
             for a in articles],
        )
        conn.commit()
        logger.info("Saved %d seen RSS articles", len(articles))
    finally:
        conn.close()


def cleanup_old_articles(max_age_days: int = 90) -> int:
    """Remove articles older than max_age_days. Returns count deleted."""
    conn = get_connection()
    try:
        cursor = conn.execute(
            "DELETE FROM rss_articles_seen WHERE seen_at < datetime('now', ?)",
            (f"-{max_age_days} days",),
        )
        conn.commit()
        deleted = cursor.rowcount
        if deleted:
            logger.info("Cleaned up %d old RSS articles", deleted)
        return deleted
    finally:
        conn.close()


# --- Track Cache ---

def get_cached_tracks(artist_ids: List[str], max_age_days: int = 7) -> Dict[str, Dict[str, Any]]:
    """Get cached tracks for artists. Returns {artist_spotify_id: track_dict}."""
    if not artist_ids:
        return {}
    conn = get_connection()
    try:
        placeholders = ",".join("?" for _ in artist_ids)
        rows = conn.execute(
            f"""SELECT artist_spotify_id, track_id, track_name, artist_name,
                       duration_ms, preview_url, release_date
                FROM track_cache
                WHERE artist_spotify_id IN ({placeholders})
                  AND cached_at > datetime('now', ?)""",
            artist_ids + [f"-{max_age_days} days"],
        ).fetchall()
        result = {}
        for r in rows:
            aid = r["artist_spotify_id"]
            if aid not in result:
                result[aid] = {
                    "track_id": r["track_id"],
                    "track_name": r["track_name"],
                    "artist_name": r["artist_name"],
                    "duration_ms": r["duration_ms"],
                    "preview_url": r["preview_url"],
                    "release_date": r["release_date"],
                }
        return result
    finally:
        conn.close()


def cache_tracks(tracks: List[Dict[str, Any]]) -> None:
    """Cache artist→track mappings. Each track: {artist_spotify_id, track_id, ...}."""
    if not tracks:
        return
    conn = get_connection()
    try:
        conn.executemany(
            """INSERT INTO track_cache
               (artist_spotify_id, track_id, track_name, artist_name,
                duration_ms, preview_url, release_date)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(artist_spotify_id, track_id) DO UPDATE SET
                 cached_at=CURRENT_TIMESTAMP""",
            [
                (t["artist_spotify_id"], t["track_id"], t.get("track_name"),
                 t.get("artist_name"), t.get("duration_ms", 0),
                 t.get("preview_url"), t.get("release_date"))
                for t in tracks
            ],
        )
        conn.commit()
        logger.info("Cached %d tracks", len(tracks))
    finally:
        conn.close()


# --- Per-Playlist Cooldowns ---

def get_recently_recommended(cooldown_weeks: int, playlist_type: str = None) -> set:
    """Get artist IDs recommended within cooldown period, optionally filtered by playlist type."""
    conn = get_connection()
    try:
        if playlist_type:
            rows = conn.execute(
                """SELECT DISTINCT artist_spotify_id FROM recommendations
                   WHERE recommended_at > datetime('now', ?)
                     AND playlist_type = ?""",
                (f"-{cooldown_weeks * 7} days", playlist_type),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT DISTINCT artist_spotify_id FROM recommendations
                   WHERE recommended_at > datetime('now', ?)""",
                (f"-{cooldown_weeks * 7} days",),
            ).fetchall()
        return {r["artist_spotify_id"] for r in rows}
    finally:
        conn.close()


# --- Spotlight History ---

def get_last_spotlight_genre() -> Optional[str]:
    """Get the most recently used spotlight genre family."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT genre_family FROM spotlight_history ORDER BY used_at DESC LIMIT 1"
        ).fetchone()
        return row["genre_family"] if row else None
    finally:
        conn.close()


def save_spotlight_genre(genre_family: str) -> None:
    """Record a genre family as used for spotlight."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO spotlight_history (genre_family) VALUES (?)",
            (genre_family,),
        )
        conn.commit()
    finally:
        conn.close()


def get_spotlight_history(limit: int = 10) -> List[str]:
    """Get recent spotlight genre families (most recent first)."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT genre_family FROM spotlight_history ORDER BY used_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [r["genre_family"] for r in rows]
    finally:
        conn.close()
