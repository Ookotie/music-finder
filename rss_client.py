"""Music blog RSS client — extract artist names from music publication feeds.

Parses RSS feeds from music blogs and extracts artist names from article
titles using regex patterns. Deduplicates via the rss_articles_seen table.
No API keys required — just public RSS feeds.
"""

import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import config
import db

logger = logging.getLogger(__name__)

# Blog RSS feeds — each with a name and URL.
# These are all public, freely available RSS feeds from major music publications.
BLOG_FEEDS = [
    {
        "name": "Pitchfork Reviews",
        "url": "https://pitchfork.com/feed/feed-album-reviews/rss",
    },
    {
        "name": "Pitchfork Tracks",
        "url": "https://pitchfork.com/feed/feed-best-new-tracks/rss",
    },
    {
        "name": "Stereogum",
        "url": "https://www.stereogum.com/feed/",
    },
    {
        "name": "BrooklynVegan",
        "url": "https://www.brooklynvegan.com/feed/",
    },
    {
        "name": "The Quietus",
        "url": "https://thequietus.com/feed",
    },
    {
        "name": "CVLT Nation",
        "url": "https://cvltnation.com/feed/",
    },
]

# Regex patterns to extract artist names from article titles.
# Ordered by specificity — first match wins.
_TITLE_PATTERNS = [
    # "Artist Name - Album Title" or "Artist Name – Album Title"
    re.compile(r"^(.+?)\s*[-–—]\s*.+$"),
    # "Artist Name: Album/Track Review"
    re.compile(r"^(.+?):\s*.+$"),
    # "Artist Name Shares New Single/Album/Track/Video/Song"
    re.compile(r"^(.+?)\s+(?:shares?|releases?|announces?|debuts?|drops?|unveils?|premieres?)\s+", re.IGNORECASE),
    # "Artist Name's New Album/Single" (handles ASCII and Unicode apostrophes)
    re.compile(r"^(.+?)['\u2019]s\s+(?:new|latest|debut|upcoming)\s+", re.IGNORECASE),
    # "Listen to Artist Name's ..."
    re.compile(r"^(?:listen to|watch|hear|stream|check out)\s+(.+?)(?:['\u2019]s|\s+[-–—])", re.IGNORECASE),
    # "Review: Artist Name - Album"
    re.compile(r"^(?:review|album review|track review|premiere)[:\s]+(.+?)\s*[-–—]", re.IGNORECASE),
]

# Words/phrases that indicate the extracted text is NOT an artist name
_NOT_ARTIST_INDICATORS = {
    "the best", "album review", "track review", "best new",
    "song premiere", "video premiere", "daily roundup", "news roundup",
    "staff picks", "this week", "weekend", "interview",
    "playlist", "festival", "tour", "dates", "tickets",
    "rip", "r.i.p.", "obituary", "in memoriam",
}


def _parse_feed(feed_url: str, feed_name: str) -> List[Dict[str, Any]]:
    """Parse a single RSS feed and return raw article entries.

    Returns list of: {url, title, published, blog_name}
    """
    try:
        import feedparser
    except ImportError:
        logger.warning("feedparser not installed — skipping RSS discovery")
        return []

    try:
        feed = feedparser.parse(feed_url)
    except Exception as e:
        logger.warning("Failed to parse RSS feed '%s': %s", feed_name, e)
        return []

    if feed.bozo and not feed.entries:
        logger.warning("RSS feed '%s' returned no entries (bozo: %s)",
                        feed_name, feed.bozo_exception)
        return []

    articles = []
    for entry in feed.entries:
        url = entry.get("link", "")
        title = entry.get("title", "").strip()
        if not url or not title:
            continue

        # Parse published date
        published = None
        for date_field in ("published_parsed", "updated_parsed"):
            parsed = entry.get(date_field)
            if parsed:
                try:
                    published = datetime(*parsed[:6], tzinfo=timezone.utc)
                except (TypeError, ValueError):
                    pass
                break

        articles.append({
            "url": url,
            "title": title,
            "published": published,
            "blog_name": feed_name,
        })

    return articles


def _extract_artist_from_title(title: str) -> Optional[str]:
    """Extract an artist name from an article title using regex patterns.

    Returns the cleaned artist name or None if extraction fails.
    """
    # Skip titles that are clearly not about a specific artist
    title_lower = title.lower()
    for indicator in _NOT_ARTIST_INDICATORS:
        if title_lower.startswith(indicator):
            return None

    for pattern in _TITLE_PATTERNS:
        match = pattern.match(title)
        if match:
            artist = match.group(1).strip()
            # Clean up common prefixes
            for prefix in ("Premiere:", "Exclusive:", "Listen:", "Watch:", "Stream:"):
                if artist.lower().startswith(prefix.lower()):
                    artist = artist[len(prefix):].strip()

            # Validate: not too short, not too long, not all caps abbreviation
            # that's likely an acronym for a column name
            if len(artist) < 2 or len(artist) > 80:
                continue
            # Skip if it looks like a section header
            if artist.isupper() and len(artist) < 10:
                continue
            # Skip if it contains indicators of non-artist text
            artist_lower = artist.lower()
            if any(ind in artist_lower for ind in _NOT_ARTIST_INDICATORS):
                continue

            return artist

    return None


def extract_artists_from_feeds(
    max_age_days: int = None,
) -> List[Dict[str, Any]]:
    """Parse all configured blog RSS feeds and extract artist names.

    Args:
        max_age_days: Only process articles from the last N days
                      (default: config.BLOG_RSS_MAX_AGE_DAYS)

    Returns:
        List of candidate dicts in standard format:
        {name, genres: [], discovery_sources: {"blog:<name>"}, ...}
    """
    if max_age_days is None:
        max_age_days = config.BLOG_RSS_MAX_AGE_DAYS

    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

    # Load already-seen URLs to avoid reprocessing
    seen_urls = db.get_seen_article_urls()
    logger.info("RSS: %d articles already seen in DB", len(seen_urls))

    candidates: Dict[str, Dict[str, Any]] = {}  # name_lower -> candidate
    new_articles: List[Dict[str, str]] = []  # for saving to DB

    for feed_info in BLOG_FEEDS:
        feed_name = feed_info["name"]
        feed_url = feed_info["url"]

        logger.info("  Parsing RSS: %s", feed_name)
        articles = _parse_feed(feed_url, feed_name)

        processed = 0
        extracted = 0
        for article in articles:
            # Skip already-seen
            if article["url"] in seen_urls:
                continue

            # Skip old articles
            if article["published"] and article["published"] < cutoff:
                continue

            processed += 1
            artist = _extract_artist_from_title(article["title"])

            # Record as seen regardless of extraction success
            new_articles.append({
                "url": article["url"],
                "blog_name": feed_name,
                "artist_extracted": artist or "",
            })

            if not artist:
                continue

            extracted += 1
            name_key = artist.lower().strip()

            if name_key in ("various artists", "[unknown]", "unknown artist"):
                continue

            if name_key in candidates:
                candidates[name_key]["discovery_sources"].add(f"blog:{feed_name}")
            else:
                candidates[name_key] = {
                    "name": artist,
                    "mb_id": None,
                    "spotify_id": None,
                    "genres": [],
                    "discovery_sources": {f"blog:{feed_name}"},
                    "mb_score": 0,
                }

        logger.info("    %s: %d new articles, %d artists extracted",
                     feed_name, processed, extracted)

    # Save seen articles to DB
    if new_articles:
        db.save_seen_articles(new_articles)

    # Cleanup old articles periodically
    db.cleanup_old_articles(max_age_days=90)

    # Convert sets to lists
    result = []
    for c in candidates.values():
        c["source_count"] = len(c["discovery_sources"])
        c["discovery_sources"] = list(c["discovery_sources"])
        result.append(c)

    logger.info("  RSS discovery: %d unique artists from %d feeds",
                len(result), len(BLOG_FEEDS))
    return result
