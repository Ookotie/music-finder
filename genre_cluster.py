"""Genre Clustering — Groups candidates into coherent genre families.

Ensures playlists don't mix contrasting styles (techno with indie pop).
Each candidate is assigned to the best-matching genre family based on
keyword matching against their genre tags.
"""

import logging
from collections import defaultdict
from typing import Any, Dict, List, Tuple

import config

logger = logging.getLogger(__name__)

# Genre families based on Oni's actual taste profile.
# Keys are family names, values are keyword sets for matching.
GENRE_FAMILIES = {
    "Electronic / House": {
        "house", "deep house", "tech house", "bass house", "dance", "edm",
        "garage", "breakbeat", "future bass", "uk garage", "speed garage",
        "progressive house", "acid house", "chicago house", "disco house",
        "funky house", "soulful house", "tropical house", "electro house",
    },
    "Techno / Dark Electronic": {
        "techno", "industrial", "ebm", "darkwave", "dark ambient", "noise",
        "experimental electronic", "dark techno", "acid techno", "dub techno",
        "minimal techno", "hard techno", "industrial techno", "power electronics",
        "witch house", "dark electro",
    },
    "Indie / Alternative": {
        "indie rock", "indie pop", "alternative", "lo-fi", "post-punk",
        "shoegaze", "dream pop", "noise pop", "jangle pop", "twee pop",
        "math rock", "noise rock", "slowcore", "sadcore", "emo",
        "midwest emo", "indie folk", "chamber pop", "art rock",
        "alternative rock", "college rock",
    },
    "Synth & Electropop": {
        "synth-pop", "synthpop", "electropop", "new wave", "dance-pop",
        "eurodance", "italo disco", "synthwave", "retrowave", "darksynth",
        "future pop", "nu-disco", "electro", "minimal wave",
        "electronic body music",
    },
    "Ambient / Downtempo": {
        "ambient", "downtempo", "chillout", "trip-hop", "electronica",
        "drone", "space music", "new age", "idm", "glitch",
        "micro house", "minimal", "dub", "chillwave",
    },
    "Hip-Hop / R&B": {
        "hip hop", "rap", "r&b", "trap", "boom bap", "lo-fi hip hop",
        "abstract hip hop", "conscious hip hop", "underground hip hop",
        "instrumental hip hop", "neo-soul", "funk", "soul",
    },
    "Rock / Metal": {
        "rock", "metal", "punk", "hardcore", "post-rock", "stoner rock",
        "doom metal", "sludge metal", "black metal", "death metal",
        "progressive rock", "progressive metal", "grunge", "psychedelic rock",
        "garage rock", "surf rock", "punk rock", "post-metal",
        "heavy metal", "thrash metal",
    },
}


def assign_genre_cluster(
    candidate: Dict[str, Any],
    genre_weights: Dict[str, float] = None,
) -> str:
    """Assign a candidate to the best-matching genre family.

    Uses keyword matching between the candidate's genres and family keyword sets.
    Tie-breaks by taste profile weight (if provided).

    Returns the family name string, or "Mixed" if no match.
    """
    candidate_genres = candidate.get("genres", [])
    if isinstance(candidate_genres, str):
        import json
        try:
            candidate_genres = json.loads(candidate_genres)
        except (json.JSONDecodeError, TypeError):
            candidate_genres = []

    if not candidate_genres:
        return "Mixed"

    candidate_genres_lower = {g.lower().strip() for g in candidate_genres}

    family_scores: Dict[str, float] = {}
    for family_name, keywords in GENRE_FAMILIES.items():
        score = 0.0
        for cg in candidate_genres_lower:
            # Exact match
            if cg in keywords:
                score += 2.0
                continue
            # Partial match (substring)
            for kw in keywords:
                if kw in cg or cg in kw:
                    score += 1.0
                    break

        if score > 0:
            # Tie-break by taste profile weight for matched genres
            if genre_weights:
                taste_bonus = sum(
                    genre_weights.get(cg, 0) for cg in candidate_genres_lower
                )
                score += taste_bonus * 0.5
            family_scores[family_name] = score

    if not family_scores:
        return "Mixed"

    return max(family_scores, key=family_scores.get)


def cluster_candidates(
    candidates: List[Dict[str, Any]],
    genre_weights: Dict[str, float] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Group candidates into genre family clusters.

    Merges clusters below MIN_CLUSTER_SIZE into the nearest larger cluster.
    Remaining orphans go into "Mixed".

    Returns: {family_name: [candidates]}
    """
    clusters: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for candidate in candidates:
        family = assign_genre_cluster(candidate, genre_weights)
        candidate["genre_cluster"] = family
        clusters[family].append(candidate)

    # Log cluster sizes before merging
    for name, members in sorted(clusters.items(), key=lambda x: -len(x[1])):
        logger.info("  Cluster '%s': %d candidates", name, len(members))

    # Merge small clusters into nearest larger one or "Mixed"
    min_size = config.MIN_CLUSTER_SIZE
    small_clusters = [name for name, members in clusters.items()
                      if len(members) < min_size and name != "Mixed"]

    if small_clusters:
        # Find the largest cluster to absorb orphans
        large_clusters = {name: members for name, members in clusters.items()
                          if len(members) >= min_size}

        for small_name in small_clusters:
            orphans = clusters.pop(small_name)
            if large_clusters:
                # Find the cluster with the highest average genre overlap
                best_target = _find_nearest_cluster(orphans, large_clusters)
                clusters[best_target].extend(orphans)
                for c in orphans:
                    c["genre_cluster"] = best_target
                logger.info("  Merged '%s' (%d) into '%s'",
                            small_name, len(orphans), best_target)
            else:
                # No large clusters — put in Mixed
                clusters["Mixed"].extend(orphans)
                for c in orphans:
                    c["genre_cluster"] = "Mixed"

    # Remove empty Mixed cluster
    if "Mixed" in clusters and not clusters["Mixed"]:
        del clusters["Mixed"]

    return dict(clusters)


def _find_nearest_cluster(
    orphans: List[Dict[str, Any]],
    large_clusters: Dict[str, List[Dict[str, Any]]],
) -> str:
    """Find the large cluster most similar to the orphan group."""
    orphan_genres = set()
    for c in orphans:
        genres = c.get("genres", [])
        if isinstance(genres, str):
            import json
            try:
                genres = json.loads(genres)
            except (json.JSONDecodeError, TypeError):
                genres = []
        orphan_genres.update(g.lower().strip() for g in genres)

    best_name = list(large_clusters.keys())[0]
    best_overlap = 0

    for name, members in large_clusters.items():
        cluster_genres = set()
        for c in members:
            genres = c.get("genres", [])
            if isinstance(genres, str):
                import json
                try:
                    genres = json.loads(genres)
                except (json.JSONDecodeError, TypeError):
                    genres = []
            cluster_genres.update(g.lower().strip() for g in genres)

        overlap = len(orphan_genres & cluster_genres)
        if overlap > best_overlap:
            best_overlap = overlap
            best_name = name

    return best_name
