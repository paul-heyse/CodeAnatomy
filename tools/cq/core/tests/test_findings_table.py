"""Tests for findings_table module."""

from __future__ import annotations

import polars as pl

from tools.cq.core.findings_table import (
    apply_filters,
    build_frame,
    flatten_result,
    rehydrate_result,
)
from tools.cq.core.schema import Anchor, CqResult, Finding, RunMeta, Section


def _make_finding(
    category: str = "test",
    message: str = "test message",
    file: str | None = "src/foo.py",
    line: int | None = 10,
    severity: str = "info",
    impact_score: float = 0.5,
    impact_bucket: str = "med",
    confidence_score: float = 0.8,
    confidence_bucket: str = "high",
    evidence_kind: str = "resolved_ast",
) -> Finding:
    """Create a test finding with scoring details."""
    anchor = Anchor(file=file, line=line or 1) if file else None
    return Finding(
        category=category,
        message=message,
        anchor=anchor,
        severity=severity,
        details={
            "impact_score": impact_score,
            "impact_bucket": impact_bucket,
            "confidence_score": confidence_score,
            "confidence_bucket": confidence_bucket,
            "evidence_kind": evidence_kind,
        },
    )


def _make_result(
    key_findings: list[Finding] | None = None,
    sections: list[Section] | None = None,
    evidence: list[Finding] | None = None,
) -> CqResult:
    """Create a test CqResult."""
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


class TestFlattenResult:
    """Tests for flatten_result function."""

    def test_empty_result_returns_empty_list(self) -> None:
        result = _make_result()
        records = flatten_result(result)
        assert records == []

    def test_key_findings_flattened(self) -> None:
        finding = _make_finding(category="summary", message="test summary")
        result = _make_result(key_findings=[finding])
        records = flatten_result(result)

        assert len(records) == 1
        assert records[0].group == "key_findings"
        assert records[0].category == "summary"
        assert records[0].message == "test summary"
        assert records[0]._section_title is None

    def test_section_findings_flattened(self) -> None:
        finding = _make_finding(category="call", message="test call")
        section = Section(title="Call Sites", findings=[finding])
        result = _make_result(sections=[section])
        records = flatten_result(result)

        assert len(records) == 1
        assert records[0].group == "Call Sites"
        assert records[0]._section_title == "Call Sites"
        assert records[0]._section_idx == 0

    def test_evidence_flattened(self) -> None:
        finding = _make_finding(category="evidence", message="test evidence")
        result = _make_result(evidence=[finding])
        records = flatten_result(result)

        assert len(records) == 1
        assert records[0].group == "evidence"
        assert records[0]._section_title is None

    def test_scoring_extracted(self) -> None:
        finding = _make_finding(
            impact_score=0.75,
            impact_bucket="high",
            confidence_score=0.95,
            confidence_bucket="high",
            evidence_kind="resolved_ast",
        )
        result = _make_result(key_findings=[finding])
        records = flatten_result(result)

        assert records[0].impact_score == 0.75
        assert records[0].impact_bucket == "high"
        assert records[0].confidence_score == 0.95
        assert records[0].confidence_bucket == "high"
        assert records[0].evidence_kind == "resolved_ast"


class TestBuildFrame:
    """Tests for build_frame function."""

    def test_empty_records_returns_empty_frame(self) -> None:
        df = build_frame([])
        assert df.shape[0] == 0
        assert "impact_score" in df.columns
        assert "file" in df.columns

    def test_records_to_dataframe(self) -> None:
        finding = _make_finding(file="src/test.py", line=42)
        result = _make_result(key_findings=[finding])
        records = flatten_result(result)
        df = build_frame(records)

        assert df.shape[0] == 1
        assert df["file"][0] == "src/test.py"
        assert df["line"][0] == 42


class TestApplyFilters:
    """Tests for apply_filters function."""

    def _make_test_df(self) -> pl.DataFrame:
        """Create a test DataFrame with various findings."""
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

    def test_no_filters_returns_all(self) -> None:
        df = self._make_test_df()
        filtered = apply_filters(df)
        assert filtered.shape[0] == 5

    def test_impact_filter_high(self) -> None:
        df = self._make_test_df()
        filtered = apply_filters(df, impact=["high"])
        assert filtered.shape[0] == 2
        assert all(b == "high" for b in filtered["impact_bucket"].to_list())

    def test_impact_filter_multiple(self) -> None:
        df = self._make_test_df()
        filtered = apply_filters(df, impact=["high", "med"])
        assert filtered.shape[0] == 4

    def test_confidence_filter(self) -> None:
        df = self._make_test_df()
        filtered = apply_filters(df, confidence=["high"])
        assert filtered.shape[0] == 1
        assert filtered["confidence_bucket"][0] == "high"

    def test_include_glob_pattern(self) -> None:
        df = self._make_test_df()
        filtered = apply_filters(df, include=["src/core/*"])
        # Should include src/core/foo.py, src/core/bar.py, and finding with no file
        assert filtered.shape[0] == 3

    def test_include_double_star_glob(self) -> None:
        df = self._make_test_df()
        filtered = apply_filters(df, include=["src/**/*.py"])
        # Should include all src files + no-file finding
        assert filtered.shape[0] == 4

    def test_exclude_glob_pattern(self) -> None:
        df = self._make_test_df()
        filtered = apply_filters(df, exclude=["tests/*"])
        # Should exclude tests/test_foo.py
        assert filtered.shape[0] == 4
        assert "tests/test_foo.py" not in filtered["file"].to_list()

    def test_exclude_preserves_null_files(self) -> None:
        df = self._make_test_df()
        filtered = apply_filters(df, exclude=["*.py"])
        # Exclude all files but keep null
        assert filtered.shape[0] == 1
        assert filtered["file"][0] is None

    def test_include_regex_pattern(self) -> None:
        df = self._make_test_df()
        # Regex pattern starts with ~
        filtered = apply_filters(df, include=["~core.*\\.py$"])
        # Should match src/core/foo.py, src/core/bar.py, + null file
        assert filtered.shape[0] == 3

    def test_limit(self) -> None:
        df = self._make_test_df()
        filtered = apply_filters(df, limit=2)
        assert filtered.shape[0] == 2

    def test_combined_filters(self) -> None:
        df = self._make_test_df()
        # High impact, in src/, not tests, limit 1
        filtered = apply_filters(
            df,
            impact=["high"],
            include=["src/**/*"],
            exclude=["tests/*"],
            limit=1,
        )
        assert filtered.shape[0] == 1
        assert filtered["impact_bucket"][0] == "high"


class TestRehydrateResult:
    """Tests for rehydrate_result function."""

    def test_rehydrate_empty(self) -> None:
        original = _make_result()
        df = build_frame([])
        rehydrated = rehydrate_result(original, df)

        assert len(rehydrated.key_findings) == 0
        assert len(rehydrated.sections) == 0
        assert len(rehydrated.evidence) == 0

    def test_rehydrate_preserves_metadata(self) -> None:
        original = _make_result()
        original.summary = {"test": "value"}
        df = build_frame([])
        rehydrated = rehydrate_result(original, df)

        assert rehydrated.run.macro == "test"
        assert rehydrated.summary == {"test": "value"}

    def test_rehydrate_key_findings(self) -> None:
        finding = _make_finding(category="summary", message="test", file="foo.py", line=10)
        original = _make_result(key_findings=[finding])
        records = flatten_result(original)
        df = build_frame(records)
        rehydrated = rehydrate_result(original, df)

        assert len(rehydrated.key_findings) == 1
        assert rehydrated.key_findings[0].category == "summary"
        assert rehydrated.key_findings[0].message == "test"

    def test_rehydrate_sections(self) -> None:
        finding = _make_finding(category="call", message="call site")
        section = Section(title="Call Sites", findings=[finding])
        original = _make_result(sections=[section])
        records = flatten_result(original)
        df = build_frame(records)
        rehydrated = rehydrate_result(original, df)

        assert len(rehydrated.sections) == 1
        assert rehydrated.sections[0].title == "Call Sites"
        assert len(rehydrated.sections[0].findings) == 1

    def test_rehydrate_evidence(self) -> None:
        finding = _make_finding(category="evidence", message="proof")
        original = _make_result(evidence=[finding])
        records = flatten_result(original)
        df = build_frame(records)
        rehydrated = rehydrate_result(original, df)

        assert len(rehydrated.evidence) == 1
        assert rehydrated.evidence[0].category == "evidence"

    def test_rehydrate_after_filter(self) -> None:
        # Create findings with different impact buckets
        high = _make_finding(category="high", message="high impact", impact_bucket="high")
        low = _make_finding(category="low", message="low impact", impact_bucket="low")
        original = _make_result(key_findings=[high, low])

        records = flatten_result(original)
        df = build_frame(records)
        filtered_df = apply_filters(df, impact=["high"])
        rehydrated = rehydrate_result(original, filtered_df)

        assert len(rehydrated.key_findings) == 1
        assert rehydrated.key_findings[0].category == "high"

    def test_rehydrate_preserves_section_order(self) -> None:
        finding1 = _make_finding(message="first")
        finding2 = _make_finding(message="second")
        section1 = Section(title="First Section", findings=[finding1])
        section2 = Section(title="Second Section", findings=[finding2])
        original = _make_result(sections=[section1, section2])

        records = flatten_result(original)
        df = build_frame(records)
        rehydrated = rehydrate_result(original, df)

        assert len(rehydrated.sections) == 2
        assert rehydrated.sections[0].title == "First Section"
        assert rehydrated.sections[1].title == "Second Section"
