"""Tests for CLI result filtering helpers."""

from __future__ import annotations

import pytest
from tools.cq.cli_app.context import FilterConfig
from tools.cq.cli_app.result_filter import apply_result_filters
from tools.cq.core.schema import CqResult, RunMeta


def _result() -> CqResult:
    return CqResult(
        run=RunMeta(
            macro="search",
            argv=["cq", "search", "target"],
            root=".",
            started_ms=0.0,
            elapsed_ms=1.0,
            toolchain={},
        )
    )


def test_apply_result_filters_returns_same_result_without_filters() -> None:
    """Return the original result object when no filters are configured."""
    result = _result()

    filtered = apply_result_filters(result, FilterConfig())

    assert filtered is result


def test_apply_result_filters_uses_findings_table_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run through flatten/apply/rehydrate steps when filters are present."""
    result = _result()
    filters = FilterConfig(include=["callsite"])

    calls: dict[str, object] = {}

    def _flatten(payload: CqResult) -> list[dict[str, object]]:
        calls["flatten"] = payload
        return [{"id": 1}]

    def _frame(records: list[dict[str, object]]) -> object:
        calls["frame"] = records
        return "frame"

    def _apply(frame: object, _options: object) -> object:
        calls["apply"] = frame
        return "filtered"

    def _rehydrate(payload: CqResult, frame: object) -> CqResult:
        calls["rehydrate"] = (payload, frame)
        return payload

    monkeypatch.setattr("tools.cq.cli_app.result_filter.flatten_result", _flatten)
    monkeypatch.setattr("tools.cq.cli_app.result_filter.build_frame", _frame)
    monkeypatch.setattr("tools.cq.cli_app.result_filter.apply_filters", _apply)
    monkeypatch.setattr("tools.cq.cli_app.result_filter.rehydrate_result", _rehydrate)

    filtered = apply_result_filters(result, filters)

    assert filtered is result
    assert calls["flatten"] is result
    assert calls["frame"] == [{"id": 1}]
    assert calls["apply"] == "frame"
    assert calls["rehydrate"] == (result, "filtered")
