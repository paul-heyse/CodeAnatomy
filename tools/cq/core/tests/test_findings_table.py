"""Tests for findings_table module."""

from __future__ import annotations

from typing import Literal, cast

import polars as pl

from tools.cq.core.findings_table import (
    FindingsTableOptions,
    apply_filters,
    build_frame,
    flatten_result,
    rehydrate_result,
)
from tools.cq.core.schema import Anchor, CqResult, DetailPayload, Finding, RunMeta, Section
from tools.cq.core.summary_contract import summary_from_mapping

_TEST_IMPACT_SCORE_HIGH = 0.75
_TEST_CONFIDENCE_SCORE_HIGH = 0.95
_TEST_LINE_NUMBER = 42
_TEST_TOTAL_FINDINGS = 5
_TEST_FILTERED_COUNT_HIGH = 2
_TEST_FILTERED_COUNT_HIGH_MED = 4
_TEST_FILTERED_COUNT_CONF_HIGH = 1
_TEST_FILTERED_COUNT_INCLUDE_CORE = 3
_TEST_FILTERED_COUNT_INCLUDE_SRC = 4
_TEST_FILTERED_COUNT_EXCLUDE_TESTS = 4
_TEST_FILTERED_COUNT_EXCLUDE_ALL_FILES = 1
_TEST_LIMIT_COUNT = 2
_TEST_SINGLE_FINDING = 1
_TEST_SECTION_COUNT = 2


def _make_finding(**overrides: object) -> Finding:
    """Create a test finding with scoring details.

    Returns:
    -------
    Finding
        Constructed finding instance.
    """
    category = str(overrides.get("category", "test"))
    message = str(overrides.get("message", "test message"))
    file_value = overrides.get("file", "src/foo.py")
    file = None if file_value is None else str(file_value)
    line_value = cast("int | None", overrides.get("line", 10))
    line = line_value
    severity_value = cast("str", overrides.get("severity", "info"))
    if severity_value not in {"info", "warning", "error"}:
        severity_value = "info"
    severity: Literal["info", "warning", "error"] = cast(
        "Literal['info', 'warning', 'error']",
        severity_value,
    )
    impact_score = float(cast("float | int", overrides.get("impact_score", 0.5)))
    impact_bucket = str(overrides.get("impact_bucket", "med"))
    confidence_score = float(cast("float | int", overrides.get("confidence_score", 0.8)))
    confidence_bucket = str(overrides.get("confidence_bucket", "high"))
    evidence_kind = str(overrides.get("evidence_kind", "resolved_ast"))

    anchor = Anchor(file=file, line=line or 1) if file else None
    return Finding(
        category=category,
        message=message,
        anchor=anchor,
        severity=severity,
        details=DetailPayload.from_legacy(
            {
                "impact_score": impact_score,
                "impact_bucket": impact_bucket,
                "confidence_score": confidence_score,
                "confidence_bucket": confidence_bucket,
                "evidence_kind": evidence_kind,
            }
        ),
    )


def _make_result(
    key_findings: list[Finding] | None = None,
    sections: list[Section] | None = None,
    evidence: list[Finding] | None = None,
) -> CqResult:
    """Create a test CqResult.

    Returns:
    -------
    CqResult
        Minimal result for testing.
    """
    run = RunMeta(
        macro="test",
        argv=["test"],
        root="/test",
        started_ms=0,
        elapsed_ms=100,
    )
    return CqResult(
        run=run,
        key_findings=key_findings or [],
        sections=sections or [],
        evidence=evidence or [],
    )


def _make_test_df() -> pl.DataFrame:
    """Create a test DataFrame with various findings.

    Returns:
    -------
    pl.DataFrame
        DataFrame with representative findings.
    """
    findings = [
        _make_finding(
            file="src/core/foo.py",
            impact_bucket="high",
            confidence_bucket="high",
        ),
        _make_finding(
            file="src/core/bar.py",
            impact_bucket="med",
            confidence_bucket="med",
        ),
        _make_finding(
            file="tests/test_foo.py",
            impact_bucket="low",
            confidence_bucket="low",
        ),
        _make_finding(
            file="src/utils/helper.py",
            impact_bucket="high",
            confidence_bucket="low",
        ),
        _make_finding(
            file=None,
            impact_bucket="med",
            confidence_bucket="med",
        ),
    ]
    result = _make_result(key_findings=findings)
    records = flatten_result(result)
    return build_frame(records)


class TestFlattenResult:
    """Tests for flatten_result function."""

    @staticmethod
    def test_empty_result_returns_empty_list() -> None:
        """Return an empty list when no findings are present."""
        result = _make_result()
        records = flatten_result(result)
        assert records == []

    @staticmethod
    def test_key_findings_flattened() -> None:
        """Flatten key findings into records."""
        finding = _make_finding(category="summary", message="test summary")
        result = _make_result(key_findings=[finding])
        records = flatten_result(result)

        assert len(records) == 1
        assert records[0].group == "key_findings"
        assert records[0].category == "summary"
        assert records[0].message == "test summary"
        assert records[0].section_title is None

    @staticmethod
    def test_section_findings_flattened() -> None:
        """Flatten section findings into records."""
        finding = _make_finding(category="call", message="test call")
        section = Section(title="Call Sites", findings=[finding])
        result = _make_result(sections=[section])
        records = flatten_result(result)

        assert len(records) == 1
        assert records[0].group == "Call Sites"
        assert records[0].section_title == "Call Sites"
        assert records[0].section_idx == 0

    @staticmethod
    def test_evidence_flattened() -> None:
        """Flatten evidence findings into records."""
        finding = _make_finding(category="evidence", message="test evidence")
        result = _make_result(evidence=[finding])
        records = flatten_result(result)

        assert len(records) == 1
        assert records[0].group == "evidence"
        assert records[0].section_title is None

    @staticmethod
    def test_scoring_extracted() -> None:
        """Extract scoring fields from finding details."""
        finding = _make_finding(
            impact_score=_TEST_IMPACT_SCORE_HIGH,
            impact_bucket="high",
            confidence_score=_TEST_CONFIDENCE_SCORE_HIGH,
            confidence_bucket="high",
            evidence_kind="resolved_ast",
        )
        result = _make_result(key_findings=[finding])
        records = flatten_result(result)

        assert records[0].impact_score == _TEST_IMPACT_SCORE_HIGH
        assert records[0].impact_bucket == "high"
        assert records[0].confidence_score == _TEST_CONFIDENCE_SCORE_HIGH
        assert records[0].confidence_bucket == "high"
        assert records[0].evidence_kind == "resolved_ast"


class TestBuildFrame:
    """Tests for build_frame function."""

    @staticmethod
    def test_empty_records_returns_empty_frame() -> None:
        """Return an empty DataFrame with schema when no records."""
        df = build_frame([])
        assert df.shape[0] == 0
        assert "impact_score" in df.columns
        assert "file" in df.columns

    @staticmethod
    def test_records_to_dataframe() -> None:
        """Convert records into a DataFrame row."""
        finding = _make_finding(file="src/test.py", line=_TEST_LINE_NUMBER)
        result = _make_result(key_findings=[finding])
        records = flatten_result(result)
        df = build_frame(records)

        assert df.shape[0] == 1
        assert df["file"][0] == "src/test.py"
        assert df["line"][0] == _TEST_LINE_NUMBER


class TestApplyFilters:
    """Tests for apply_filters function."""

    @staticmethod
    def test_no_filters_returns_all() -> None:
        """Return all rows when no filters are applied."""
        df = _make_test_df()
        filtered = apply_filters(df)
        assert filtered.shape[0] == _TEST_TOTAL_FINDINGS

    @staticmethod
    def test_impact_filter_high() -> None:
        """Filter rows by high impact bucket."""
        df = _make_test_df()
        filtered = apply_filters(df, FindingsTableOptions(impact=["high"]))
        assert filtered.shape[0] == _TEST_FILTERED_COUNT_HIGH
        assert all(b == "high" for b in filtered["impact_bucket"].to_list())

    @staticmethod
    def test_impact_filter_multiple() -> None:
        """Filter rows by multiple impact buckets."""
        df = _make_test_df()
        filtered = apply_filters(df, FindingsTableOptions(impact=["high", "med"]))
        assert filtered.shape[0] == _TEST_FILTERED_COUNT_HIGH_MED

    @staticmethod
    def test_confidence_filter() -> None:
        """Filter rows by confidence bucket."""
        df = _make_test_df()
        filtered = apply_filters(df, FindingsTableOptions(confidence=["high"]))
        assert filtered.shape[0] == _TEST_FILTERED_COUNT_CONF_HIGH
        assert filtered["confidence_bucket"][0] == "high"

    @staticmethod
    def test_include_glob_pattern() -> None:
        """Include rows matching glob patterns."""
        df = _make_test_df()
        filtered = apply_filters(df, FindingsTableOptions(include=["src/core/*"]))
        # Should include src/core/foo.py, src/core/bar.py, and finding with no file
        assert filtered.shape[0] == _TEST_FILTERED_COUNT_INCLUDE_CORE

    @staticmethod
    def test_include_double_star_glob() -> None:
        """Include rows matching recursive glob patterns."""
        df = _make_test_df()
        filtered = apply_filters(df, FindingsTableOptions(include=["src/**/*.py"]))
        # Should include all src files + no-file finding
        assert filtered.shape[0] == _TEST_FILTERED_COUNT_INCLUDE_SRC

    @staticmethod
    def test_exclude_glob_pattern() -> None:
        """Exclude rows matching glob patterns."""
        df = _make_test_df()
        filtered = apply_filters(df, FindingsTableOptions(exclude=["tests/*"]))
        # Should exclude tests/test_foo.py
        assert filtered.shape[0] == _TEST_FILTERED_COUNT_EXCLUDE_TESTS
        assert "tests/test_foo.py" not in filtered["file"].to_list()

    @staticmethod
    def test_exclude_preserves_null_files() -> None:
        """Preserve findings without file when excluding."""
        df = _make_test_df()
        filtered = apply_filters(df, FindingsTableOptions(exclude=["*.py"]))
        # Exclude all files but keep null
        assert filtered.shape[0] == _TEST_FILTERED_COUNT_EXCLUDE_ALL_FILES
        assert filtered["file"][0] is None

    @staticmethod
    def test_include_regex_pattern() -> None:
        """Include rows matching regex patterns."""
        df = _make_test_df()
        # Regex pattern starts with ~
        filtered = apply_filters(df, FindingsTableOptions(include=["~core.*\\.py$"]))
        # Should match src/core/foo.py, src/core/bar.py, + null file
        assert filtered.shape[0] == _TEST_FILTERED_COUNT_INCLUDE_CORE

    @staticmethod
    def test_limit() -> None:
        """Limit number of rows returned."""
        df = _make_test_df()
        filtered = apply_filters(df, FindingsTableOptions(limit=_TEST_LIMIT_COUNT))
        assert filtered.shape[0] == _TEST_LIMIT_COUNT

    @staticmethod
    def test_combined_filters() -> None:
        """Apply combined include/exclude/impact filters."""
        df = _make_test_df()
        # High impact, in src/, not tests, limit 1
        filtered = apply_filters(
            df,
            FindingsTableOptions(
                impact=["high"],
                include=["src/**/*"],
                exclude=["tests/*"],
                limit=_TEST_SINGLE_FINDING,
            ),
        )
        assert filtered.shape[0] == _TEST_SINGLE_FINDING
        assert filtered["impact_bucket"][0] == "high"


class TestRehydrateResult:
    """Tests for rehydrate_result function."""

    @staticmethod
    def test_rehydrate_empty() -> None:
        """Rehydrate to an empty result when no rows."""
        original = _make_result()
        df = build_frame([])
        rehydrated = rehydrate_result(original, df)

        assert len(rehydrated.key_findings) == 0
        assert len(rehydrated.sections) == 0
        assert len(rehydrated.evidence) == 0

    @staticmethod
    def test_rehydrate_preserves_metadata() -> None:
        """Preserve run metadata on rehydrate."""
        original = _make_result()
        original.summary = summary_from_mapping({"query": "value"})
        df = build_frame([])
        rehydrated = rehydrate_result(original, df)

        assert rehydrated.run.macro == "test"
        assert rehydrated.summary.query == "value"

    @staticmethod
    def test_rehydrate_key_findings() -> None:
        """Rehydrate key findings from filtered data."""
        finding = _make_finding(category="summary", message="test", file="foo.py", line=10)
        original = _make_result(key_findings=[finding])
        records = flatten_result(original)
        df = build_frame(records)
        rehydrated = rehydrate_result(original, df)

        assert len(rehydrated.key_findings) == 1
        assert rehydrated.key_findings[0].category == "summary"
        assert rehydrated.key_findings[0].message == "test"

    @staticmethod
    def test_rehydrate_sections() -> None:
        """Rehydrate section findings from filtered data."""
        finding = _make_finding(category="call", message="call site")
        section = Section(title="Call Sites", findings=[finding])
        original = _make_result(sections=[section])
        records = flatten_result(original)
        df = build_frame(records)
        rehydrated = rehydrate_result(original, df)

        assert len(rehydrated.sections) == 1
        assert rehydrated.sections[0].title == "Call Sites"
        assert len(rehydrated.sections[0].findings) == 1

    @staticmethod
    def test_rehydrate_evidence() -> None:
        """Rehydrate evidence findings from filtered data."""
        finding = _make_finding(category="evidence", message="proof")
        original = _make_result(evidence=[finding])
        records = flatten_result(original)
        df = build_frame(records)
        rehydrated = rehydrate_result(original, df)

        assert len(rehydrated.evidence) == 1
        assert rehydrated.evidence[0].category == "evidence"

    @staticmethod
    def test_rehydrate_after_filter() -> None:
        """Rehydrate after applying a filter."""
        # Create findings with different impact buckets
        high = _make_finding(category="high", message="high impact", impact_bucket="high")
        low = _make_finding(category="low", message="low impact", impact_bucket="low")
        original = _make_result(key_findings=[high, low])

        records = flatten_result(original)
        df = build_frame(records)
        filtered_df = apply_filters(df, FindingsTableOptions(impact=["high"]))
        rehydrated = rehydrate_result(original, filtered_df)

        assert len(rehydrated.key_findings) == 1
        assert rehydrated.key_findings[0].category == "high"

    @staticmethod
    def test_rehydrate_preserves_section_order() -> None:
        """Preserve original section ordering when rehydrating."""
        finding1 = _make_finding(message="first")
        finding2 = _make_finding(message="second")
        section1 = Section(title="First Section", findings=[finding1])
        section2 = Section(title="Second Section", findings=[finding2])
        original = _make_result(sections=[section1, section2])

        records = flatten_result(original)
        df = build_frame(records)
        rehydrated = rehydrate_result(original, df)

        assert len(rehydrated.sections) == _TEST_SECTION_COUNT
        assert rehydrated.sections[0].title == "First Section"
        assert rehydrated.sections[1].title == "Second Section"
