"""Tests for diagnostics file-coverage builder."""

from __future__ import annotations

from datafusion import SessionContext

from semantics.diagnostics.coverage import build_file_coverage_report


def test_build_file_coverage_report_none_when_no_base() -> None:
    """Coverage report returns None when no base file table exists."""
    ctx = SessionContext()

    assert build_file_coverage_report(ctx) is None


def test_build_file_coverage_report_counts_sources() -> None:
    """Coverage report computes extraction_count from source presence."""
    ctx = SessionContext()
    ctx.from_pydict({"file_id": ["f1"]}, name="repo_files_v1")
    ctx.from_pydict({"file_id": ["f1"]}, name="cst_defs_norm")

    report = build_file_coverage_report(ctx)

    assert report is not None
    row = report.to_arrow_table().to_pylist()[0]
    assert row["file_id"] == "f1"
    assert row["has_cst"] == 1
    assert row["extraction_count"] == 1
