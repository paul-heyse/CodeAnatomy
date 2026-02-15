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


def test_scope_filter_applied_detects_include() -> None:
    assert scope_filter_applied(include=["*.py"], exclude=None) is True


def test_scope_filter_applied_detects_exclude() -> None:
    assert scope_filter_applied(include=None, exclude=["tests/*"]) is True


def test_scope_filter_applied_returns_false_when_none() -> None:
    assert scope_filter_applied(include=None, exclude=None) is False


def test_macro_score_payload_populates_buckets() -> None:
    payload = macro_score_payload(files=3, findings=5)
    assert payload.impact >= 0.0
    assert payload.confidence >= 0.0
    assert payload.impact_bucket in {"low", "med", "high"}
    assert payload.confidence_bucket in {"low", "med", "high"}


def test_macro_scoring_details_supports_breakage_signals() -> None:
    details = macro_scoring_details(
        sites=10,
        files=4,
        depth=1,
        breakages=2,
        ambiguities=3,
        evidence_kind="resolved_ast",
    )
    assert details["impact_score"] is not None
    assert details["confidence_score"] is not None
    assert details["evidence_kind"] == "resolved_ast"


def test_resolve_target_files_finds_explicit_path(tmp_path: Path) -> None:
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
    for i in range(5):
        (tmp_path / f"file{i}.py").write_text(f"# File {i}")

    result = iter_files(
        root=tmp_path,
        max_files=3,
    )
    assert len(result) <= 3
