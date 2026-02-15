"""Signal-evaluation helpers for Python static semantic payloads."""

from __future__ import annotations

from collections.abc import Mapping

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


__all__ = ["evaluate_python_semantic_signal_from_mapping"]
