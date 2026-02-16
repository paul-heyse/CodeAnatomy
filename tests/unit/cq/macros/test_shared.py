"""Tests for shared macro utility helpers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.macros.shared import (
    iter_files,
    macro_score_payload,
    macro_scoring_details,
    resolve_target_files,
    scope_filter_applied,
)

MAX_FILES_LIMIT = 3


def test_scope_filter_applied_detects_include() -> None:
    """Test scope filter applied detects include."""
    assert scope_filter_applied(include=["*.py"], exclude=None) is True


def test_scope_filter_applied_detects_exclude() -> None:
    """Test scope filter applied detects exclude."""
    assert scope_filter_applied(include=None, exclude=["tests/*"]) is True


def test_scope_filter_applied_returns_false_when_none() -> None:
    """Test scope filter applied returns false when none."""
    assert scope_filter_applied(include=None, exclude=None) is False


def test_macro_score_payload_populates_buckets() -> None:
    """Test macro score payload populates buckets."""
    payload = macro_score_payload(files=3, findings=5)
    assert payload.impact >= 0.0
    assert payload.confidence >= 0.0
    assert payload.impact_bucket in {"low", "med", "high"}
    assert payload.confidence_bucket in {"low", "med", "high"}


def test_macro_scoring_details_supports_breakage_signals() -> None:
    """Test macro scoring details supports breakage signals."""
    details = macro_scoring_details(
        sites=10,
        files=4,
        depth=1,
        breakages=2,
        ambiguities=3,
        evidence_kind="resolved_ast",
    )
    assert details.impact_score is not None
    assert details.confidence_score is not None
    assert details.evidence_kind == "resolved_ast"


def test_resolve_target_files_finds_explicit_path(tmp_path: Path) -> None:
    """Test resolve target files finds explicit path."""
    test_file = tmp_path / "test.py"
    test_file.write_text("def foo(): pass")

    result = resolve_target_files(
        root=tmp_path,
        target=str(test_file),
        max_files=10,
    )
    assert len(result) == 1
    assert result[0] == test_file.resolve()


def test_iter_files_respects_max_files(tmp_path: Path) -> None:
    """Test iter files respects max files."""
    for i in range(5):
        (tmp_path / f"file{i}.py").write_text(f"# File {i}")

    result = iter_files(
        root=tmp_path,
        max_files=MAX_FILES_LIMIT,
    )
    assert len(result) <= MAX_FILES_LIMIT
