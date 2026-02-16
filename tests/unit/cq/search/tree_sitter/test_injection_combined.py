"""Tests for combined/separate injection runtime execution."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from tools.cq.search.tree_sitter.rust_lane.injection_runtime import parse_injected_ranges
from tools.cq.search.tree_sitter.rust_lane.injections import InjectionPlanV1

COMBINED_PLAN_COUNT = 3
SEPARATE_PLAN_COUNT = 2
SQL_COMBINED_RANGE_COUNT = 2
PYTHON_COMBINED_RANGE_COUNT = 1
RUST_SEPARATE_RANGE_COUNT = 1


def _plan(
    *,
    language: str,
    start: int,
    end: int,
    combined: bool,
) -> InjectionPlanV1:
    return InjectionPlanV1(
        language=language,
        start_byte=start,
        end_byte=end,
        start_row=0,
        start_col=start,
        end_row=0,
        end_col=end,
        combined=combined,
    )


def test_parse_injected_ranges_groups_combined_plans_by_language(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test parse injected ranges groups combined plans by language."""
    parse_runs: list[tuple[str, int]] = []

    class _Parser:
        def __init__(self, language: object | None = None) -> None:
            self.language = language
            self.included_ranges: list[object] = []

        def parse(self, _source_bytes: bytes) -> object:
            parse_runs.append(
                (str(getattr(self.language, "name", "unknown")), len(self.included_ranges))
            )
            return object()

    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.rust_lane.injection_runtime._TreeSitterParser",
        _Parser,
    )
    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.rust_lane.injection_runtime._TreeSitterPoint",
        lambda row, col: (row, col),
    )
    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.rust_lane.injection_runtime._TreeSitterRange",
        lambda *_args: object(),
    )
    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.rust_lane.injection_runtime.load_tree_sitter_language",
        lambda language: SimpleNamespace(name=language),
    )

    result = parse_injected_ranges(
        source_bytes=b"abc",
        language=cast("Any", SimpleNamespace(name="rust")),
        plans=(
            _plan(language="sql", start=0, end=4, combined=True),
            _plan(language="sql", start=6, end=10, combined=True),
            _plan(language="python", start=12, end=16, combined=True),
        ),
    )
    assert result.plan_count == COMBINED_PLAN_COUNT
    assert result.combined_count == COMBINED_PLAN_COUNT
    assert result.parsed is True
    assert result.included_ranges_applied is True
    assert result.metadata["combined_runs"] == SEPARATE_PLAN_COUNT
    assert result.metadata["separate_runs"] == 0
    assert parse_runs == [
        ("sql", SQL_COMBINED_RANGE_COUNT),
        ("python", PYTHON_COMBINED_RANGE_COUNT),
    ]


def test_parse_injected_ranges_runs_separate_plans_individually(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test parse injected ranges runs separate plans individually."""
    parse_runs: list[tuple[str, int]] = []

    class _Parser:
        def __init__(self, language: object | None = None) -> None:
            self.language = language
            self.included_ranges: list[object] = []

        def parse(self, _source_bytes: bytes) -> object:
            parse_runs.append(
                (str(getattr(self.language, "name", "unknown")), len(self.included_ranges))
            )
            return object()

    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.rust_lane.injection_runtime._TreeSitterParser",
        _Parser,
    )
    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.rust_lane.injection_runtime._TreeSitterPoint",
        lambda row, col: (row, col),
    )
    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.rust_lane.injection_runtime._TreeSitterRange",
        lambda *_args: object(),
    )
    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.rust_lane.injection_runtime.load_tree_sitter_language",
        lambda language: SimpleNamespace(name=language),
    )

    result = parse_injected_ranges(
        source_bytes=b"abc",
        language=cast("Any", SimpleNamespace(name="rust")),
        plans=(
            _plan(language="rust", start=0, end=4, combined=False),
            _plan(language="rust", start=6, end=10, combined=False),
        ),
    )
    assert result.plan_count == SEPARATE_PLAN_COUNT
    assert result.combined_count == 0
    assert result.metadata["combined_runs"] == 0
    assert result.metadata["separate_runs"] == SEPARATE_PLAN_COUNT
    assert parse_runs == [
        ("rust", RUST_SEPARATE_RANGE_COUNT),
        ("rust", RUST_SEPARATE_RANGE_COUNT),
    ]


def test_parse_injected_ranges_reports_errors_from_failed_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report parser runtime errors while preserving successful parser runs."""

    class _Parser:
        def __init__(self, language: object | None = None) -> None:
            self.language = language
            self.included_ranges: list[object] = []

        def parse(self, _source_bytes: bytes) -> object:
            if str(getattr(self.language, "name", "")) == "python":
                msg = "boom"
                raise RuntimeError(msg)
            return object()

    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.rust_lane.injection_runtime._TreeSitterParser",
        _Parser,
    )
    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.rust_lane.injection_runtime._TreeSitterPoint",
        lambda row, col: (row, col),
    )
    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.rust_lane.injection_runtime._TreeSitterRange",
        lambda *_args: object(),
    )
    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.rust_lane.injection_runtime.load_tree_sitter_language",
        lambda language: SimpleNamespace(name=language),
    )

    result = parse_injected_ranges(
        source_bytes=b"abc",
        language=cast("Any", SimpleNamespace(name="rust")),
        plans=(
            _plan(language="rust", start=0, end=4, combined=False),
            _plan(language="python", start=6, end=10, combined=False),
        ),
    )
    assert result.parsed is False
    assert result.included_ranges_applied is True
    assert result.errors == ("RuntimeError",)
