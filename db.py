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
"""


def get_connection() -> sqlite3.Connection:
    """Get a database connection, creating the DB and tables if needed."""
    os.makedirs(os.path.dirname(config.DB_PATH), exist_ok=True)
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    return conn


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
