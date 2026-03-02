"""Scoring Algorithm — Ranks discovery candidates by fit with user's taste.

Supports per-playlist scoring profiles:
  - Rising Stars: momentum + recency heavy, relaxed genre threshold
  - Deep Cuts: genre match + obscurity heavy, no release age limit
  - Genre Spotlight: genre match dominant, must match spotlight genre
  - default: original balanced weights (backwards compatible)
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import config

logger = logging.getLogger(__name__)

# --- Scoring Profiles ---

SCORING_PROFILES = {
    "rising_stars": {
        "weights": {
            "genre": 0.25,
            "momentum": 0.30,
            "recency": 0.25,
            "source_diversity": 0.10,
            "popularity": 0.10,
        },
        "min_genre_match": 0.15,
        "listener_range": (100_000, 2_000_000),  # Last.fm unique listeners
        "max_release_age_months": 12,
        "invert_popularity": False,  # prefer breakout zone
    },
    "deep_cuts": {
        "weights": {
            "genre": 0.50,
            "popularity": 0.25,  # inverted — more obscure = better
            "source_diversity": 0.15,
            "momentum": 0.10,
            "recency": 0.0,
        },
        "min_genre_match": 0.20,
        "listener_range": (10_000, 500_000),
        "max_release_age_months": None,  # no limit
        "invert_popularity": True,
    },
    "genre_spotlight": {
        "weights": {
            "genre": 0.60,
            "popularity": 0.20,
            "source_diversity": 0.10,
            "momentum": 0.10,
            "recency": 0.0,
        },
        "min_genre_match": 0.25,
        "listener_range": (0, 2_000_000),
        "max_release_age_months": None,
        "invert_popularity": False,
    },
    "default": {
        "weights": {
            "genre": 0.4,
            "popularity": 0.3,
            "source_diversity": 0.2,
            "momentum": 0.1,
            "recency": 0.0,
        },
        "min_genre_match": 0.3,
        "listener_range": None,  # no filtering
        "max_release_age_months": None,
        "invert_popularity": False,
    },
}

# Last.fm listener tiers for popularity scoring.
LASTFM_TIERS = [
    # (max_listeners, score, label)
    (50_000, 0.6, "underground"),
    (200_000, 0.9, "growing indie"),
    (750_000, 1.0, "breakout zone"),
    (2_000_000, 0.5, "mid-tier"),
    (5_000_000, 0.2, "popular"),
    (float("inf"), 0.05, "mainstream"),
]

# Inverted tiers: more underground = higher score
LASTFM_TIERS_INVERTED = [
    (10_000, 0.7, "very underground"),
    (50_000, 1.0, "underground sweet spot"),
    (200_000, 0.9, "growing indie"),
    (500_000, 0.6, "breakout"),
    (2_000_000, 0.3, "mid-tier"),
    (float("inf"), 0.1, "mainstream"),
]


def compute_genre_match(
    candidate_genres: List[str],
    genre_weights: Dict[str, float],
) -> float:
    """Score 0-1 based on overlap between candidate's genres and taste profile."""
    if not candidate_genres or not genre_weights:
        return 0.0

    matched_weight = 0.0
    for genre in candidate_genres:
        g = genre.lower().strip()
        if g in genre_weights:
            matched_weight += genre_weights[g]
            continue
        best_partial = 0.0
        for taste_genre, weight in genre_weights.items():
            if g in taste_genre or taste_genre in g:
                best_partial = max(best_partial, weight * 0.5)
        matched_weight += best_partial

    if not matched_weight:
        return 0.0

    match_count = sum(
        1 for g in candidate_genres
        if g.lower().strip() in genre_weights
        or any(g.lower().strip() in tg or tg in g.lower().strip()
               for tg in genre_weights)
    )
    match_count = max(match_count, 1)

    avg_matched = matched_weight / match_count
    breadth_bonus = min(match_count / 5.0, 1.0)
    score = avg_matched * 0.6 + breadth_bonus * 0.4

    return min(score, 1.0)


def compute_popularity_score(
    candidate: Dict[str, Any], invert: bool = False
) -> float:
    """Score 0-1 based on popularity.

    If invert=True, more underground = higher score (for Deep Cuts).
    """
    tiers = LASTFM_TIERS_INVERTED if invert else LASTFM_TIERS

    lastfm_listeners = candidate.get("lastfm_listeners", 0)
    if lastfm_listeners > 0:
        for max_listeners, score, label in tiers:
            if lastfm_listeners < max_listeners:
                return score

    # Fallback: MB search relevance score (inverted)
    mb_score = candidate.get("mb_score", 0)
    source_count = candidate.get("source_count", 1)

    if mb_score >= 90 and source_count >= 3:
        return 0.1
    elif mb_score >= 80 and source_count >= 2:
        return 0.3
    elif mb_score >= 70:
        return 0.5
    elif mb_score >= 50:
        return 0.7
    elif mb_score >= 30:
        return 0.8
    else:
        return 0.6


def compute_source_diversity(candidate: Dict[str, Any]) -> float:
    """Score 0-1 based on how many sources found this candidate."""
    count = candidate.get("source_count", 1)
    if count >= 3:
        return 1.0
    elif count == 2:
        return 0.6
    else:
        return 0.3


def compute_momentum_score(candidate: Dict[str, Any]) -> float:
    """Score 0-1 based on listener growth momentum."""
    current = candidate.get("lastfm_listeners", 0)
    previous = candidate.get("previous_listeners", 0)

    if not current or not previous:
        return 0.5

    if previous == 0:
        return 0.8

    growth = (current - previous) / previous

    if growth > 0.20:
        return 1.0
    elif growth > 0.10:
        return 0.8
    elif growth > 0.0:
        return 0.6
    else:
        return 0.3


def compute_recency_score(candidate: Dict[str, Any], max_age_months: int = 12) -> float:
    """Score 0-1 based on how recently the artist's track was released.

    Newer releases score higher. Used primarily for Rising Stars.
    """
    release_date = candidate.get("release_date", "")
    if not release_date:
        return 0.3  # no data — slight penalty

    try:
        if len(release_date) == 4:
            rd = datetime.strptime(release_date, "%Y")
        elif len(release_date) == 7:
            rd = datetime.strptime(release_date, "%Y-%m")
        else:
            rd = datetime.strptime(release_date[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return 0.3

    age_days = (datetime.now() - rd).days
    max_age_days = max_age_months * 30

    if age_days <= 0:
        return 1.0
    elif age_days <= 90:
        return 1.0  # last 3 months — peak freshness
    elif age_days <= 180:
        return 0.8
    elif age_days <= max_age_days:
        return 0.6
    else:
        return 0.2  # older than max age


def compute_feedback_boost(
    candidate: Dict[str, Any],
    liked_genres: Dict[str, int] = None,
) -> float:
    """Small composite boost for candidates whose genres were positively received."""
    if not liked_genres:
        return 0.0

    candidate_genres = candidate.get("genres", [])
    if isinstance(candidate_genres, str):
        import json
        try:
            candidate_genres = json.loads(candidate_genres)
        except (json.JSONDecodeError, TypeError):
            return 0.0

    matches = sum(1 for g in candidate_genres if g.lower().strip() in liked_genres)
    if matches == 0:
        return 0.0

    return min(matches * 0.02, 0.05)


def score_candidates(
    candidates: List[Dict[str, Any]],
    genre_weights: Dict[str, float],
    liked_genres: Dict[str, int] = None,
    profile: str = "default",
) -> List[Dict[str, Any]]:
    """Score all candidates using the specified scoring profile.

    Filters out candidates below the profile's minimum genre match threshold
    and outside the listener range (if specified).

    Args:
        candidates: List of candidate dicts
        genre_weights: Taste profile weights
        liked_genres: {genre: count} from feedback for bonus scoring
        profile: Scoring profile name from SCORING_PROFILES
    """
    prof = SCORING_PROFILES.get(profile, SCORING_PROFILES["default"])
    weights = prof["weights"]
    min_genre = prof["min_genre_match"]
    listener_range = prof.get("listener_range")
    invert_pop = prof.get("invert_popularity", False)

    scored = []

    for candidate in candidates:
        # Listener range filter
        if listener_range:
            listeners = candidate.get("lastfm_listeners", 0)
            lo, hi = listener_range
            # Skip candidates outside range (but allow those with no data)
            if listeners > 0 and (listeners < lo or listeners > hi):
                continue

        genres = candidate.get("genres", [])
        genre_score = compute_genre_match(genres, genre_weights)

        if genre_score < min_genre:
            continue

        pop_score = compute_popularity_score(candidate, invert=invert_pop)
        source_score = compute_source_diversity(candidate)
        momentum_score = compute_momentum_score(candidate)
        recency_score = compute_recency_score(candidate)

        composite = (
            genre_score * weights.get("genre", 0)
            + pop_score * weights.get("popularity", 0)
            + source_score * weights.get("source_diversity", 0)
            + momentum_score * weights.get("momentum", 0)
            + recency_score * weights.get("recency", 0)
        )

        fb_boost = compute_feedback_boost(candidate, liked_genres)
        composite += fb_boost

        candidate["genre_match_score"] = round(genre_score, 4)
        candidate["popularity_score"] = round(pop_score, 4)
        candidate["source_diversity_bonus"] = round(source_score, 4)
        candidate["momentum_score"] = round(momentum_score, 4)
        candidate["recency_score"] = round(recency_score, 4)
        candidate["feedback_boost"] = round(fb_boost, 4)
        candidate["composite_score"] = round(composite, 4)
        scored.append(candidate)

    scored.sort(key=lambda c: c["composite_score"], reverse=True)

    logger.info("Scored %d candidates with profile '%s' (filtered %d below threshold)",
                len(scored), profile, len(candidates) - len(scored))

    return scored
