"""Tests for CQ result scope filtering helper."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.schema import Anchor, CqResult, Finding, RunMeta, Section
from tools.cq.core.scope_filter import filter_findings_by_scope
from tools.cq.core.summary_types import summary_from_mapping


def _result_with_files() -> CqResult:
    keep = Finding(
        category="def",
        message="keep",
        anchor=Anchor(file="tools/cq/a.py", line=1),
    )
    drop = Finding(
        category="def",
        message="drop",
        anchor=Anchor(file="src/a.py", line=1),
    )
    run = RunMeta(
        macro="search",
        argv=["cq", "search", "a"],
        root=".",
        started_ms=0.0,
        elapsed_ms=0.0,
        toolchain={},
    )
    return CqResult(
        run=run,
        summary=summary_from_mapping({}),
        key_findings=(keep, drop),
        evidence=(keep, drop),
        sections=(Section(title="All", findings=(keep, drop)),),
    )


def test_filter_findings_by_scope_applies_in_dir() -> None:
    """In-dir filter should keep only findings anchored under target directory."""
    result = _result_with_files()

    filtered = filter_findings_by_scope(
        result,
        root=Path(),
        in_dir="tools/cq",
    )

    assert len(filtered.key_findings) == 1
    assert filtered.key_findings[0].message == "keep"
    assert len(filtered.sections) == 1
    assert len(filtered.sections[0].findings) == 1


def test_filter_findings_by_scope_applies_exclude_globs() -> None:
    """Exclude globs should remove matched findings from result payload."""
    result = _result_with_files()

    filtered = filter_findings_by_scope(
        result,
        root=Path(),
        exclude=("tools/**",),
    )

    assert len(filtered.key_findings) == 1
    assert filtered.key_findings[0].message == "drop"


def test_filter_findings_by_scope_applies_path_filter() -> None:
    """Path predicate should gate findings by resolved absolute path."""
    result = _result_with_files()

    filtered = filter_findings_by_scope(
        result,
        root=Path(),
        path_filter=lambda path: path.as_posix().endswith("src/a.py"),
    )

    assert len(filtered.key_findings) == 1
    assert filtered.key_findings[0].message == "drop"
