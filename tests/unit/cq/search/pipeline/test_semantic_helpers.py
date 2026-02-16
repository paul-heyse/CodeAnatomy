"""Tests for shared semantic pipeline helpers."""

from __future__ import annotations

from tools.cq.search.pipeline._semantic_helpers import (
    count_mapping_rows,
    normalize_python_semantic_degradation_reason,
)

EXPECTED_MAPPING_ROW_COUNT = 2


def test_count_mapping_rows_counts_dict_items_only() -> None:
    """`count_mapping_rows` should count only dictionary entries in lists."""
    assert count_mapping_rows([{"a": 1}, 2, "x", {"b": 2}]) == EXPECTED_MAPPING_ROW_COUNT


def test_count_mapping_rows_returns_zero_for_non_list() -> None:
    """`count_mapping_rows` should return zero for non-list payloads."""
    assert count_mapping_rows({"rows": []}) == 0


def test_normalize_python_semantic_degradation_reason_uses_coverage_reason() -> None:
    """Coverage timeout reason should normalize to request timeout."""
    reason = normalize_python_semantic_degradation_reason(
        reasons=("no_signal",),
        coverage_reason="no_python_semantic_signal:timeout",
    )
    assert reason == "request_timeout"


def test_normalize_python_semantic_degradation_reason_prefers_explicit_reason() -> None:
    """Explicit timeout reasons should take precedence over generic fallback."""
    reason = normalize_python_semantic_degradation_reason(
        reasons=("timeout", "no_signal"),
        coverage_reason=None,
    )
    assert reason == "request_timeout"


def test_normalize_python_semantic_degradation_reason_defaults_to_no_signal() -> None:
    """Unknown reasons should normalize to `no_signal`."""
    reason = normalize_python_semantic_degradation_reason(
        reasons=("unknown_reason",),
        coverage_reason="custom_reason",
    )
    assert reason == "no_signal"
