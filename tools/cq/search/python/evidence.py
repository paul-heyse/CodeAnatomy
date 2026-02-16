"""Python evidence: semantic signal evaluation and cross-source agreement."""

from __future__ import annotations

from collections.abc import Mapping

# --- From semantic_signal.py ---

_NO_SIGNAL_REASON_THRESHOLD = 3


def evaluate_python_semantic_signal_from_mapping(
    payload: Mapping[str, object],
) -> tuple[bool, tuple[str, ...]]:
    """Return whether a payload carries actionable static semantic signal."""
    reasons: list[str] = []

    grounding = payload.get("symbol_grounding")
    if isinstance(grounding, Mapping):
        defs = grounding.get("definition_targets")
        decls = grounding.get("declaration_targets")
        if not _has_rows(defs) and not _has_rows(decls):
            reasons.append("no_grounding")
    else:
        reasons.append("no_grounding")

    call_graph = payload.get("call_graph")
    if isinstance(call_graph, Mapping):
        incoming = _as_int(call_graph.get("incoming_total"))
        outgoing = _as_int(call_graph.get("outgoing_total"))
        if incoming <= 0 and outgoing <= 0:
            reasons.append("no_call_graph")
    else:
        reasons.append("no_call_graph")

    local_scope = payload.get("local_scope_context")
    diagnostics = payload.get("anchor_diagnostics")
    if isinstance(local_scope, Mapping):
        refs = local_scope.get("reference_locations")
        if not _has_rows(refs) and not _has_rows(diagnostics):
            reasons.append("no_local_context")
    elif not _has_rows(diagnostics):
        reasons.append("no_local_context")

    has_signal = len(reasons) < _NO_SIGNAL_REASON_THRESHOLD
    return has_signal, tuple(reasons)


def _has_rows(value: object) -> bool:
    return isinstance(value, list) and any(isinstance(item, Mapping) for item in value)


def _as_int(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0


# --- From agreement.py ---


def _as_mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def build_agreement_summary(
    *,
    ast_grep_fields: Mapping[str, object] | None = None,
    native_fields: Mapping[str, object] | None = None,
    tree_sitter_fields: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Build deterministic agreement summary across enrichment sources.

    Returns:
        dict[str, object]: Function return value.
    """
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


__all__ = [
    "build_agreement_summary",
    "evaluate_python_semantic_signal_from_mapping",
]
