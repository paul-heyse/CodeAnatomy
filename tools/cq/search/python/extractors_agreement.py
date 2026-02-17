"""Cross-source agreement helpers for Python enrichment."""

from __future__ import annotations

from collections.abc import Mapping

_FULL_AGREEMENT_SOURCE_COUNT = 3


__all__ = ["build_agreement_section"]


def build_agreement_section(
    *,
    ast_fields: Mapping[str, object],
    python_resolution_fields: Mapping[str, object],
    tree_sitter_fields: Mapping[str, object],
    full_agreement_source_count: int = _FULL_AGREEMENT_SOURCE_COUNT,
) -> dict[str, object]:
    """Build deterministic cross-source agreement metadata.

    Returns:
        dict[str, object]: Agreement status, source list, and conflict details.
    """
    ast_values = dict(ast_fields)
    resolution_values = dict(python_resolution_fields)
    tree_sitter_values = dict(tree_sitter_fields)

    conflicts: list[dict[str, object]] = []
    sources = {
        "ast_grep": ast_values,
        "python_resolution": resolution_values,
        "tree_sitter": tree_sitter_values,
    }
    present_sources = [name for name, values in sources.items() if values]

    for key in sorted(set(ast_values).intersection(resolution_values)):
        left = ast_values.get(key)
        right = resolution_values.get(key)
        if left != right:
            conflicts.append({"field": key, "ast_grep": left, "python_resolution": right})

    for key in sorted(set(resolution_values).intersection(tree_sitter_values)):
        left = resolution_values.get(key)
        right = tree_sitter_values.get(key)
        if left != right:
            conflicts.append({"field": key, "python_resolution": left, "tree_sitter": right})

    if conflicts:
        status = "conflict"
    elif len(present_sources) >= full_agreement_source_count:
        status = "full"
    else:
        status = "partial"

    return {
        "status": status,
        "sources": present_sources,
        "conflicts": conflicts,
    }
