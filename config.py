"""Music Finder — Configuration and tunable defaults."""

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Spotify credentials
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "")
SPOTIFY_REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN", "")
SPOTIFY_REDIRECT_URI = "http://127.0.0.1:8888/callback"

# Last.fm
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY", "")

# Scheduling (for oni-hub integration)
MUSIC_SCAN_DAY = os.getenv("MUSIC_SCAN_DAY", "fri")  # legacy single-day
MUSIC_SCAN_DAYS = os.getenv("MUSIC_SCAN_DAYS", "sun,tue,thu")  # 3x/week
MUSIC_SCAN_HOUR = int(os.getenv("MUSIC_SCAN_HOUR", "21"))

# Tunable defaults
MAX_MONTHLY_LISTENERS = int(os.getenv("MAX_MONTHLY_LISTENERS", "100000"))
PLAYLIST_SIZE = int(os.getenv("PLAYLIST_SIZE", "50"))
ARTIST_COOLDOWN_WEEKS = int(os.getenv("ARTIST_COOLDOWN_WEEKS", "4"))
SPOTIFY_MAX_REQUESTS = int(os.getenv("SPOTIFY_MAX_REQUESTS", "200"))

# Fresh releases
FRESH_RELEASE_MONTHS = int(os.getenv("FRESH_RELEASE_MONTHS", "6"))

# Feedback system
FEEDBACK_WAIT_DAYS = int(os.getenv("FEEDBACK_WAIT_DAYS", "7"))
FEEDBACK_BOOST = float(os.getenv("FEEDBACK_BOOST", "0.05"))
FEEDBACK_PENALTY = float(os.getenv("FEEDBACK_PENALTY", "0.02"))

# Genre clustering
MIN_CLUSTER_SIZE = int(os.getenv("MIN_CLUSTER_SIZE", "5"))

# Taste profiler weights — how much each time range contributes to genre map
TIME_RANGE_WEIGHTS = {
    "short_term": 0.5,   # Last 4 weeks — strongest signal of current taste
    "medium_term": 0.3,  # Last 6 months
    "long_term": 0.2,    # All time
}

# How much each source contributes to genre weighting
SOURCE_WEIGHTS = {
    "top_artists": 1.0,
    "top_tracks": 0.7,
    "followed_artists": 0.5,
}

# SQLite database path
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "music_finder.db")
