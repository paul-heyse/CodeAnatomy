"""Relevance scoring for smart-search ranking."""

from __future__ import annotations

from tools.cq.search.pipeline.classifier import MatchCategory
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch

__all__ = ["KIND_WEIGHTS", "compute_relevance_score"]


KIND_WEIGHTS: dict[MatchCategory, float] = {
    "definition": 1.0,
    "callsite": 0.8,
    "import": 0.7,
    "from_import": 0.7,
    "reference": 0.6,
    "assignment": 0.5,
    "annotation": 0.5,
    "text_match": 0.3,
    "docstring_match": 0.2,
    "comment_match": 0.15,
    "string_match": 0.1,
}


def _classify_file_role(file_path: str) -> str:
    path_lower = file_path.lower()

    if "/test" in path_lower or "test_" in path_lower or "_test.py" in path_lower:
        return "test"
    if "/doc" in path_lower or "/docs/" in path_lower:
        return "doc"
    if "/vendor/" in path_lower or "/third_party/" in path_lower:
        return "lib"
    if "/src/" in path_lower or "/lib/" in path_lower:
        return "src"
    return "other"


def compute_relevance_score(match: EnrichedMatch) -> float:
    """Compute relevance score for ranking.

    Returns:
        float: Relevance score used for match ranking.
    """
    base = KIND_WEIGHTS.get(match.category, 0.3)
    role = _classify_file_role(match.file)
    role_mult = {
        "src": 1.0,
        "lib": 0.9,
        "other": 0.7,
        "test": 0.5,
        "doc": 0.3,
    }.get(role, 0.7)
    depth = match.file.count("/")
    depth_penalty = min(0.2, depth * 0.02)
    conf_factor = match.confidence
    return base * role_mult * conf_factor - depth_penalty
