"""Cross-source agreement helpers for Python enrichment lanes."""

from __future__ import annotations

from collections.abc import Mapping


def _as_mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def build_agreement_summary(
    *,
    ast_grep_fields: Mapping[str, object] | None = None,
    native_fields: Mapping[str, object] | None = None,
    tree_sitter_fields: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Build deterministic agreement summary across enrichment sources."""
    ast_rows = _as_mapping(ast_grep_fields)
    native_rows = _as_mapping(native_fields)
    tree_rows = _as_mapping(tree_sitter_fields)

    compared_keys = sorted(set(ast_rows) | set(native_rows) | set(tree_rows))
    matched: list[str] = []
    conflicts: list[str] = []

    for key in compared_keys:
        present_values = [
            value
            for value in (
                ast_rows.get(key),
                native_rows.get(key),
                tree_rows.get(key),
            )
            if value is not None
        ]
        if not present_values:
            continue
        if all(value == present_values[0] for value in present_values[1:]):
            matched.append(key)
        else:
            conflicts.append(key)

    status = "full" if conflicts == [] else "partial"
    if not matched and conflicts:
        status = "conflict"
    return {
        "status": status,
        "matched_keys": matched,
        "conflicting_keys": conflicts,
        "compared_keys": compared_keys,
    }


__all__ = ["build_agreement_summary"]
