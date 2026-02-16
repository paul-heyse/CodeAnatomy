"""Tests for python evidence (merged from semantic_signal + agreement)."""

from __future__ import annotations

from typing import cast

from tools.cq.search.python.evidence import (
    build_agreement_summary,
    evaluate_python_semantic_signal_from_mapping,
)

MAX_NOISE_REASONS_FOR_PARTIAL_SIGNAL = 3


class TestEvaluatePythonSemanticSignal:
    """Tests for evaluate_python_semantic_signal_from_mapping."""

    def test_no_signal_when_all_missing(self) -> None:
        """Verify no signal when payload has no grounding, call_graph, or context."""
        has_signal, reasons = evaluate_python_semantic_signal_from_mapping({})
        assert has_signal is False
        assert "no_grounding" in reasons
        assert "no_call_graph" in reasons
        assert "no_local_context" in reasons

    def test_has_signal_with_grounding_and_call_graph(self) -> None:
        """Verify signal when grounding and call_graph have rows."""
        payload = {
            "symbol_grounding": {
                "definition_targets": [{"file": "a.py"}],
            },
            "call_graph": {
                "incoming_total": 2,
                "outgoing_total": 1,
            },
        }
        has_signal, reasons = evaluate_python_semantic_signal_from_mapping(payload)
        assert has_signal is True
        assert "no_grounding" not in reasons
        assert "no_call_graph" not in reasons

    def test_partial_signal(self) -> None:
        """Verify partial signal (1-2 reasons) still yields has_signal=True."""
        payload: dict[str, object] = {
            "symbol_grounding": {
                "definition_targets": [{"file": "a.py"}],
            },
            "call_graph": {},
        }
        has_signal, reasons = evaluate_python_semantic_signal_from_mapping(payload)
        assert has_signal is True
        assert len(reasons) < MAX_NOISE_REASONS_FOR_PARTIAL_SIGNAL


class TestBuildAgreementSummary:
    """Tests for build_agreement_summary."""

    def test_full_agreement(self) -> None:
        """Verify full agreement when all sources match."""
        result = build_agreement_summary(
            ast_grep_fields={"kind": "function"},
            native_fields={"kind": "function"},
            tree_sitter_fields={"kind": "function"},
        )
        matched_keys = cast("list[str]", result["matched_keys"])
        conflicting_keys = cast("list[str]", result["conflicting_keys"])
        assert result["status"] == "full"
        assert "kind" in matched_keys
        assert conflicting_keys == []

    def test_partial_agreement(self) -> None:
        """Verify partial agreement when sources disagree on some keys."""
        result = build_agreement_summary(
            ast_grep_fields={"kind": "function", "name": "foo"},
            native_fields={"kind": "function", "name": "bar"},
        )
        matched_keys = cast("list[str]", result["matched_keys"])
        conflicting_keys = cast("list[str]", result["conflicting_keys"])
        assert result["status"] == "partial"
        assert "kind" in matched_keys
        assert "name" in conflicting_keys

    def test_conflict_when_no_matches(self) -> None:
        """Verify conflict status when all keys disagree."""
        result = build_agreement_summary(
            ast_grep_fields={"kind": "function"},
            native_fields={"kind": "class"},
        )
        assert result["status"] == "conflict"

    def test_empty_inputs(self) -> None:
        """Verify full agreement with empty inputs."""
        result = build_agreement_summary()
        assert result["status"] == "full"
        assert result["matched_keys"] == []
