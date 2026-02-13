"""Signal-evaluation helpers for Pyrefly enrichment payloads."""

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.search.pyrefly_contracts import (
    PyreflyEnrichmentPayload,
    coerce_pyrefly_payload,
)

_NO_SIGNAL_REASON_THRESHOLD = 3


def evaluate_pyrefly_signal(payload: PyreflyEnrichmentPayload) -> tuple[bool, tuple[str, ...]]:
    """Return whether a payload carries actionable semantic signal.

    Returns:
    -------
    tuple[bool, tuple[str, ...]]
        ``(has_signal, reasons)`` where ``reasons`` explains absent signal.
    """
    reasons: list[str] = []
    grounding = payload.symbol_grounding
    if not grounding.definition_targets and not grounding.declaration_targets:
        reasons.append("no_grounding")

    call_graph = payload.call_graph
    if call_graph.incoming_total <= 0 and call_graph.outgoing_total <= 0:
        reasons.append("no_call_graph")

    local_scope = payload.local_scope_context
    if not local_scope.reference_locations and not payload.anchor_diagnostics:
        reasons.append("no_local_context")

    has_signal = len(reasons) < _NO_SIGNAL_REASON_THRESHOLD
    return has_signal, tuple(reasons)


def evaluate_pyrefly_signal_from_mapping(
    payload: Mapping[str, object],
) -> tuple[bool, tuple[str, ...]]:
    """Typed signal-evaluation wrapper for builtin payload mappings.

    Returns:
    -------
    tuple[bool, tuple[str, ...]]
        ``(has_signal, reasons)`` based on the normalized typed payload.
    """
    typed = coerce_pyrefly_payload(payload)
    return evaluate_pyrefly_signal(typed)


__all__ = [
    "evaluate_pyrefly_signal",
    "evaluate_pyrefly_signal_from_mapping",
]
