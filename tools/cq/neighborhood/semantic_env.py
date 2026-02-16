"""Semantic environment extraction helpers for neighborhood rendering."""

from __future__ import annotations

from tools.cq.core.snb_schema import SemanticNeighborhoodBundleV1


def semantic_env_from_bundle(bundle: SemanticNeighborhoodBundleV1 | object) -> dict[str, object]:
    """Extract compact semantic environment flags from bundle metadata.

    Returns:
        dict[str, object]: Extracted semantic environment flags.
    """
    if not isinstance(bundle, SemanticNeighborhoodBundleV1):
        return {}
    if bundle.meta is None or not bundle.meta.semantic_sources:
        return {}

    first = bundle.meta.semantic_sources[0]
    env: dict[str, object] = {}
    for in_key, out_key in (
        ("workspace_health", "semantic_health"),
        ("quiescent", "semantic_quiescent"),
        ("position_encoding", "semantic_position_encoding"),
    ):
        value = first.get(in_key)
        if value is not None:
            env[out_key] = value
    return env


__all__ = ["semantic_env_from_bundle"]
