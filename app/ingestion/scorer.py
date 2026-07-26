"""
Lightweight scoring function for datasets in the ingestion pipeline.

Computes a quality_score (0–1) based on metadata completeness so the
search API can sort/boost richer records above sparse ones.
"""

from app.models.dataset import Dataset


def score_dataset(dataset: Dataset) -> float:
    """
    Return a quality score in [0, 1] based on available metadata fields.

    Heuristic (equal-weight categories):
      - Content: title + description presence and length
      - Richness: modality, species, keywords, subject_count
      - Trust:   trust_tier (verified > unverified > stale)
      - Link:    is_direct_link
    """
    if not dataset:
        return 0.0

    score = 0.0
    weights = 4  # four categories, each contributes up to 0.25

    # --- 1. Content (0–0.25) ---
    content = 0.0
    if dataset.title and len(dataset.title) > 5:
        content += 0.15
    if dataset.description and len(dataset.description) > 20:
        content += 0.10
    score += content

    # --- 2. Richness (0–0.25) ---
    richness = 0.0
    if dataset.modality:
        richness += 0.07
    if dataset.species:
        richness += 0.06
    if dataset.keywords:
        richness += 0.06
    if dataset.subject_count is not None and dataset.subject_count > 0:
        richness += 0.06
    score += min(richness, 0.25)

    # --- 3. Trust (0–0.25) ---
    trust_map = {"verified": 0.25, "unverified": 0.10, "stale": 0.0}
    score += trust_map.get(dataset.trust_tier.value if dataset.trust_tier else "unverified", 0.0)

    # --- 4. Link quality (0–0.25) ---
    score += 0.25 if dataset.is_direct_link else 0.0

    return round(min(score, 1.0), 4)
