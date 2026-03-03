"""Bandcamp discovery client — discover emerging artists by genre tag.

Uses Bandcamp's public discover v3 API to find artists releasing new music
in genres matching the user's taste profile.
No API key required. Rate limited to 1 request/second.
"""

import logging
import time
from typing import Any, Dict, List, Optional, Set

import requests

from . import config

logger = logging.getLogger(__name__)

_DISCOVER_URL = "https://bandcamp.com/api/discover/3/get_web"
_MIN_REQUEST_INTERVAL = 1.0  # 1s between requests — be polite
_last_request_time = 0.0

# Map common taste-profile genre names to Bandcamp's top-level genre slugs.
# Bandcamp uses a fixed set of slugs; we map the closest match.
_GENRE_TO_BANDCAMP_SLUG = {
    # Electronic family
    "electronic": "electronic",
    "house": "electronic",
    "techno": "electronic",
    "ambient": "ambient",
    "idm": "electronic",
    "downtempo": "electronic",
    "drum and bass": "electronic",
    "dubstep": "electronic",
    "electro": "electronic",
    "trance": "electronic",
    "synthwave": "electronic",
    "synthpop": "electronic",
    "electronica": "electronic",
    "trip-hop": "electronic",
    "trip hop": "electronic",
    "chillwave": "electronic",
    "glitch": "electronic",
    "vaporwave": "electronic",
    "industrial": "industrial",
    # Rock family
    "rock": "rock",
    "alternative rock": "alternative",
    "alternative": "alternative",
    "indie rock": "alternative",
    "indie": "alternative",
    "post-punk": "punk",
    "post punk": "punk",
    "punk": "punk",
    "punk rock": "punk",
    "hardcore": "punk",
    "shoegaze": "alternative",
    "dream pop": "alternative",
    "noise pop": "alternative",
    "noise rock": "rock",
    "grunge": "rock",
    "garage rock": "rock",
    "psychedelic rock": "rock",
    "psych": "rock",
    # Metal family
    "metal": "metal",
    "black metal": "metal",
    "death metal": "metal",
    "doom metal": "metal",
    "sludge metal": "metal",
    "stoner metal": "metal",
    "post-metal": "metal",
    "post metal": "metal",
    "progressive metal": "metal",
    "thrash metal": "metal",
    "heavy metal": "metal",
    # Hip-hop family
    "hip-hop": "hip-hop-rap",
    "hip hop": "hip-hop-rap",
    "rap": "hip-hop-rap",
    "trap": "hip-hop-rap",
    "lo-fi hip hop": "hip-hop-rap",
    # Other
    "experimental": "experimental",
    "pop": "pop",
    "r&b": "r-b-soul",
    "rnb": "r-b-soul",
    "soul": "r-b-soul",
    "jazz": "jazz",
    "folk": "folk",
    "classical": "classical",
    "country": "country",
    "blues": "blues",
    "reggae": "reggae",
    "world": "world",
    "latin": "latin",
    "soundtrack": "soundtrack",
    "acoustic": "acoustic",
    "singer-songwriter": "acoustic",
    "lo-fi": "lo-fi",
    "lofi": "lo-fi",
    "darkwave": "dark-wave",
    "dark wave": "dark-wave",
    "goth": "dark-wave",
    "gothic": "dark-wave",
}

# All valid Bandcamp genre slugs (for deduplication when mapping)
_VALID_SLUGS = {
    "electronic", "rock", "metal", "alternative", "hip-hop-rap",
    "experimental", "punk", "pop", "ambient", "soundtrack",
    "world", "jazz", "acoustic", "folk", "classical", "country",
    "blues", "latin", "reggae", "r-b-soul", "kids", "devotional",
    "industrial", "dark-wave", "lo-fi",
}

# Bandcamp sort options: "new" = newest, "top" = best sellers, "rec" = recommended
_SORT_MAP = {
    "new": "new",
    "top": "top",
    "rec": "rec",
    "most": "top",  # alias
}


def _rate_limit():
    """Enforce minimum interval between Bandcamp requests."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < _MIN_REQUEST_INTERVAL:
        time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
    _last_request_time = time.time()


def _map_genres_to_slugs(genre_weights: Dict[str, float]) -> List[str]:
    """Map taste-profile genres to unique Bandcamp slugs, ordered by weight."""
    slug_weights: Dict[str, float] = {}
    for genre, weight in genre_weights.items():
        slug = _GENRE_TO_BANDCAMP_SLUG.get(genre.lower().strip())
        if slug and slug in _VALID_SLUGS:
            slug_weights[slug] = max(slug_weights.get(slug, 0), weight)

    # Sort by weight descending, return slugs
    sorted_slugs = sorted(slug_weights.items(), key=lambda x: -x[1])
    return [s for s, _ in sorted_slugs]


def _build_bandcamp_url(url_hints: dict) -> str:
    """Build a Bandcamp URL from the v3 API url_hints object."""
    if not url_hints:
        return ""
    subdomain = url_hints.get("subdomain", "")
    custom = url_hints.get("custom_domain")
    slug = url_hints.get("slug", "")
    item_type = url_hints.get("item_type", "a")  # "a" = album, "t" = track

    if custom:
        base = f"https://{custom}"
    elif subdomain:
        base = f"https://{subdomain}.bandcamp.com"
    else:
        return ""

    type_path = "album" if item_type == "a" else "track"
    return f"{base}/{type_path}/{slug}" if slug else base


def get_discover_artists(
    genre_slug: str,
    sort: str = None,
    page: int = 0,
) -> List[Dict[str, Any]]:
    """Fetch artists from Bandcamp's discover v3 API.

    Args:
        genre_slug: Bandcamp genre slug (e.g., "electronic", "metal")
        sort: Sort order — "new", "top", or "rec"
        page: Page number (0-indexed)

    Returns:
        List of dicts with: name, album, bandcamp_url, genre_slug
    """
    if sort is None:
        sort = config.BANDCAMP_SORT
    bc_sort = _SORT_MAP.get(sort, "new")

    _rate_limit()

    params = {
        "s": bc_sort,
        "g": genre_slug,
        "f": "all",
        "lo": 0,
        "p": page,
    }

    try:
        resp = requests.get(
            _DISCOVER_URL,
            params=params,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.warning("Bandcamp discover failed for '%s': %s", genre_slug, e)
        return []

    if data.get("error"):
        logger.warning("Bandcamp API error for '%s'", genre_slug)
        return []

    items = data.get("items", [])
    artists = []
    for item in items:
        # v3 API: artist in "secondary_text", album/track in "primary_text"
        artist_name = (item.get("secondary_text") or "").strip()
        if not artist_name:
            continue
        album = (item.get("primary_text") or "").strip()
        bc_url = _build_bandcamp_url(item.get("url_hints", {}))

        artists.append({
            "name": artist_name,
            "album": album,
            "bandcamp_url": bc_url,
            "genre_slug": genre_slug,
        })

    return artists


def discover_artists(
    genre_weights: Dict[str, float],
    max_genres: int = None,
) -> List[Dict[str, Any]]:
    """Discover candidates from Bandcamp across top matching genres.

    Args:
        genre_weights: Weighted genre map from taste profile
        max_genres: Max genres to search (default: config.BANDCAMP_GENRES_PER_RUN)

    Returns:
        List of candidate dicts in standard format:
        {name, genres, discovery_sources: {"bandcamp_tag:<slug>"}, ...}
    """
    if max_genres is None:
        max_genres = config.BANDCAMP_GENRES_PER_RUN

    slugs = _map_genres_to_slugs(genre_weights)[:max_genres]
    if not slugs:
        logger.info("No Bandcamp genre slugs matched taste profile")
        return []

    logger.info("Discovering from %d Bandcamp genres: %s", len(slugs), slugs)

    candidates: Dict[str, Dict[str, Any]] = {}  # name_lower -> candidate

    for slug in slugs:
        try:
            artists = get_discover_artists(slug)
        except Exception as e:
            logger.warning("Bandcamp discovery failed for '%s': %s", slug, e)
            continue

        logger.info("  Bandcamp '%s': %d artists", slug, len(artists))

        for artist in artists:
            name_key = artist["name"].lower().strip()
            if name_key in ("various artists", "[unknown]", "unknown artist", "va"):
                continue

            if name_key in candidates:
                candidates[name_key]["discovery_sources"].add(f"bandcamp_tag:{slug}")
            else:
                candidates[name_key] = {
                    "name": artist["name"],
                    "mb_id": None,
                    "spotify_id": None,
                    "genres": [slug],
                    "discovery_sources": {f"bandcamp_tag:{slug}"},
                    "mb_score": 0,
                    "bandcamp_url": artist.get("bandcamp_url", ""),
                }

    # Convert sets to lists
    result = []
    for c in candidates.values():
        c["source_count"] = len(c["discovery_sources"])
        c["discovery_sources"] = list(c["discovery_sources"])
        result.append(c)

    logger.info("  Bandcamp discovery: %d unique candidates from %d genres",
                len(result), len(slugs))
    return result
