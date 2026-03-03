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
ARTIST_COOLDOWN_WEEKS = int(os.getenv("ARTIST_COOLDOWN_WEEKS", "3"))
SPOTIFY_MAX_REQUESTS = int(os.getenv("SPOTIFY_MAX_REQUESTS", "350"))

# Per-playlist target sizes
RISING_STARS_SIZE = int(os.getenv("RISING_STARS_SIZE", "30"))
DEEP_CUTS_SIZE = int(os.getenv("DEEP_CUTS_SIZE", "30"))
GENRE_SPOTLIGHT_SIZE = int(os.getenv("GENRE_SPOTLIGHT_SIZE", "20"))

# Rising Stars: only recent releases
RISING_STARS_MAX_AGE_MONTHS = int(os.getenv("RISING_STARS_MAX_AGE_MONTHS", "12"))

# Fresh releases (legacy, kept for backwards compat)
FRESH_RELEASE_MONTHS = int(os.getenv("FRESH_RELEASE_MONTHS", "6"))

# Feedback system
FEEDBACK_WAIT_DAYS = int(os.getenv("FEEDBACK_WAIT_DAYS", "7"))
FEEDBACK_BOOST = float(os.getenv("FEEDBACK_BOOST", "0.05"))
FEEDBACK_PENALTY = float(os.getenv("FEEDBACK_PENALTY", "0.02"))

# Genre clustering
MIN_CLUSTER_SIZE = int(os.getenv("MIN_CLUSTER_SIZE", "5"))

# Bandcamp discovery
BANDCAMP_GENRES_PER_RUN = int(os.getenv("BANDCAMP_GENRES_PER_RUN", "8"))
BANDCAMP_SORT = os.getenv("BANDCAMP_SORT", "new")  # "new", "top", "rec"

# Music blog RSS discovery
BLOG_RSS_MAX_AGE_DAYS = int(os.getenv("BLOG_RSS_MAX_AGE_DAYS", "30"))

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

# SQLite database path — configurable via env var for installed-package usage
DB_PATH = os.getenv("MUSIC_DB_PATH", os.path.join(os.path.dirname(__file__), "data", "music_finder.db"))
