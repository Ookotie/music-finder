"""Scoring Algorithm — Ranks discovery candidates by fit with user's taste.

composite_score = (
    genre_match_score * 0.4      # overlap with weighted genre map
  + popularity_score * 0.3       # favors "breakout zone" (niche/emerging)
  + source_diversity_bonus * 0.2 # found via multiple sources = higher confidence
  + momentum_score * 0.1         # Phase 5: listener growth tracking
)
"""

import logging
from typing import Any, Dict, List

import config

logger = logging.getLogger(__name__)

# Scoring weights
WEIGHT_GENRE = 0.4
WEIGHT_POPULARITY = 0.3
WEIGHT_SOURCE_DIVERSITY = 0.2
WEIGHT_MOMENTUM = 0.1

# Minimum genre match to keep a candidate
MIN_GENRE_MATCH = 0.3

# Last.fm listener tiers for popularity scoring.
# Last.fm "listeners" = total unique listeners (not monthly), so thresholds
# are higher than Spotify monthly listeners. A niche artist with 50K Spotify
# monthly might have 200-500K Last.fm unique listeners.
LASTFM_TIERS = [
    # (max_listeners, score, label)
    (50_000, 0.6, "underground"),
    (200_000, 0.9, "growing indie"),
    (750_000, 1.0, "breakout zone"),      # sweet spot
    (2_000_000, 0.5, "mid-tier"),
    (5_000_000, 0.2, "popular"),
    (float("inf"), 0.05, "mainstream"),    # heavily penalize household names
]


def compute_genre_match(
    candidate_genres: List[str],
    genre_weights: Dict[str, float],
) -> float:
    """Score 0-1 based on overlap between candidate's genres and taste profile.

    Uses the taste profile weights — a match on a high-weight genre scores
    more than a match on a low-weight genre. Includes partial matching
    for sub-genre relationships (e.g., "electro house" partially matches "house").
    """
    if not candidate_genres or not genre_weights:
        return 0.0

    matched_weight = 0.0
    for genre in candidate_genres:
        g = genre.lower().strip()
        # Exact match
        if g in genre_weights:
            matched_weight += genre_weights[g]
            continue
        # Partial match: check if any taste genre is a substring or vice versa
        best_partial = 0.0
        for taste_genre, weight in genre_weights.items():
            if g in taste_genre or taste_genre in g:
                best_partial = max(best_partial, weight * 0.5)
        matched_weight += best_partial

    if not matched_weight:
        return 0.0

    # Normalize by the number of matched genres (not total genres).
    # This avoids penalizing artists who have many non-music tags.
    match_count = sum(
        1 for g in candidate_genres
        if g.lower().strip() in genre_weights
        or any(g.lower().strip() in tg or tg in g.lower().strip()
               for tg in genre_weights)
    )
    match_count = max(match_count, 1)

    # Scale: average weight of matched genres, boosted by having more matches
    avg_matched = matched_weight / match_count
    breadth_bonus = min(match_count / 5.0, 1.0)  # bonus for matching multiple genres
    score = avg_matched * 0.6 + breadth_bonus * 0.4

    return min(score, 1.0)


def compute_popularity_score(candidate: Dict[str, Any]) -> float:
    """Score 0-1 based on popularity, favoring niche/emerging artists.

    Priority order for data sources:
    1. Last.fm listeners (real data, best signal)
    2. MB score fallback (inverted — high relevance = more mainstream = lower score)

    The goal is to find artists in the "breakout zone" — popular enough to have
    good music on Spotify, but not yet mainstream household names.
    """
    # Best case: Last.fm listener data available
    lastfm_listeners = candidate.get("lastfm_listeners", 0)
    if lastfm_listeners > 0:
        for max_listeners, score, label in LASTFM_TIERS:
            if lastfm_listeners < max_listeners:
                return score

    # Fallback: MB search relevance score (inverted)
    # High MB score = matches many broad searches = more well-known
    # We INVERT this: high MB score -> LOW popularity score (penalize mainstream)
    mb_score = candidate.get("mb_score", 0)
    source_count = candidate.get("source_count", 1)

    # Multi-source + high MB score = very likely mainstream
    if mb_score >= 90 and source_count >= 3:
        return 0.1  # almost certainly mainstream
    elif mb_score >= 80 and source_count >= 2:
        return 0.3
    elif mb_score >= 70:
        return 0.5  # probably well-known
    elif mb_score >= 50:
        return 0.7  # could be breakout
    elif mb_score >= 30:
        return 0.8  # likely indie
    else:
        return 0.6  # very obscure (might be too niche)


def compute_source_diversity(candidate: Dict[str, Any]) -> float:
    """Score 0-1 based on how many sources found this candidate.

    Found by 1 source: 0.3
    Found by 2 sources: 0.6
    Found by 3+ sources: 1.0
    """
    count = candidate.get("source_count", 1)
    if count >= 3:
        return 1.0
    elif count == 2:
        return 0.6
    else:
        return 0.3


def compute_momentum_score(candidate: Dict[str, Any]) -> float:
    """Score 0-1 based on listener growth momentum.

    Compares current Last.fm listeners to a previous snapshot.
    >20% growth = 1.0 (breakout), 10-20% = 0.8, 0-10% = 0.6,
    declining = 0.3, no data = 0.5 (neutral).
    """
    current = candidate.get("lastfm_listeners", 0)
    previous = candidate.get("previous_listeners", 0)

    if not current or not previous:
        return 0.5  # no data — neutral

    if previous == 0:
        return 0.8  # new artist with listeners = positive signal

    growth = (current - previous) / previous

    if growth > 0.20:
        return 1.0  # breakout
    elif growth > 0.10:
        return 0.8
    elif growth > 0.0:
        return 0.6
    else:
        return 0.3  # declining


def compute_feedback_boost(
    candidate: Dict[str, Any],
    liked_genres: Dict[str, int] = None,
) -> float:
    """Small composite boost for candidates whose genres were positively received.

    Returns a bonus (0.0-0.05) based on how many of the candidate's genres
    appear in the liked genres from feedback.
    """
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

    # Cap at 0.05 boost
    return min(matches * 0.02, 0.05)


def score_candidates(
    candidates: List[Dict[str, Any]],
    genre_weights: Dict[str, float],
    liked_genres: Dict[str, int] = None,
) -> List[Dict[str, Any]]:
    """Score all candidates and return them sorted by composite score descending.

    Filters out candidates below the minimum genre match threshold.

    Args:
        candidates: List of candidate dicts
        genre_weights: Taste profile weights
        liked_genres: {genre: count} from feedback for bonus scoring (optional)
    """
    scored = []

    for candidate in candidates:
        genres = candidate.get("genres", [])
        genre_score = compute_genre_match(genres, genre_weights)

        # Skip candidates with poor genre fit
        if genre_score < MIN_GENRE_MATCH:
            continue

        pop_score = compute_popularity_score(candidate)
        source_score = compute_source_diversity(candidate)
        momentum_score = compute_momentum_score(candidate)

        composite = (
            genre_score * WEIGHT_GENRE
            + pop_score * WEIGHT_POPULARITY
            + source_score * WEIGHT_SOURCE_DIVERSITY
            + momentum_score * WEIGHT_MOMENTUM
        )

        # Feedback boost
        fb_boost = compute_feedback_boost(candidate, liked_genres)
        composite += fb_boost

        candidate["genre_match_score"] = round(genre_score, 4)
        candidate["popularity_score"] = round(pop_score, 4)
        candidate["source_diversity_bonus"] = round(source_score, 4)
        candidate["momentum_score"] = round(momentum_score, 4)
        candidate["feedback_boost"] = round(fb_boost, 4)
        candidate["composite_score"] = round(composite, 4)
        scored.append(candidate)

    # Sort descending by composite score
    scored.sort(key=lambda c: c["composite_score"], reverse=True)

    logger.info("Scored %d candidates (filtered %d below genre threshold %.2f)",
                len(scored), len(candidates) - len(scored), MIN_GENRE_MATCH)

    return scored
