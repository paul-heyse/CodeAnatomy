"""Unit tests for Pyrefly signal evaluation."""

from __future__ import annotations

from tools.cq.search.pyrefly_contracts import coerce_pyrefly_payload
from tools.cq.search.pyrefly_signal import (
    evaluate_pyrefly_signal,
    evaluate_pyrefly_signal_from_mapping,
)


def test_evaluate_pyrefly_signal_positive() -> None:
    payload = coerce_pyrefly_payload(
        {
            "symbol_grounding": {
                "definition_targets": [{"kind": "definition", "file": "a.py", "line": 1, "col": 0}],
            },
            "call_graph": {"incoming_total": 1, "outgoing_total": 0},
        }
    )
    has_signal, reasons = evaluate_pyrefly_signal(payload)
    assert has_signal is True
    assert reasons


def test_evaluate_pyrefly_signal_negative() -> None:
    has_signal, reasons = evaluate_pyrefly_signal_from_mapping(
        {
            "symbol_grounding": {},
            "call_graph": {"incoming_total": 0, "outgoing_total": 0},
            "local_scope_context": {"reference_locations": []},
            "anchor_diagnostics": [],
        }
    )
    assert has_signal is False
    assert "no_grounding" in reasons
    assert "no_call_graph" in reasons
