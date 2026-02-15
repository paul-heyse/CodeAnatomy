"""Tests for CQ result factory."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.result_factory import build_error_result
from tools.cq.core.toolchain import Toolchain


def test_build_error_result_with_string_error() -> None:
    """Test building error result with string error message."""
    result = build_error_result(
        macro="test",
        root=Path("/tmp/test"),
        argv=["cq", "test"],
        tc=None,
        started_ms=1000.0,
        error="test error message",
    )

    assert result.run.macro == "test"
    assert result.run.root == "/tmp/test"
    assert result.run.argv == ["cq", "test"]
    assert result.summary["error"] == "test error message"
    assert result.run.elapsed_ms >= 0


def test_build_error_result_with_exception() -> None:
    """Test building error result with Exception object."""
    exc = ValueError("invalid value")
    result = build_error_result(
        macro="query",
        root=Path("/tmp/repo"),
        argv=["cq", "q", "foo"],
        tc=None,
        started_ms=2000.0,
        error=exc,
    )

    assert result.run.macro == "query"
    assert result.summary["error"] == "invalid value"


def test_build_error_result_with_toolchain() -> None:
    """Test building error result with toolchain."""
    tc = Toolchain.detect()
    result = build_error_result(
        macro="run",
        root=Path("/tmp/workspace"),
        argv=["cq", "run", "--plan", "test.toml"],
        tc=tc,
        started_ms=3000.0,
        error=RuntimeError("plan failed"),
    )

    assert result.run.macro == "run"
    assert result.run.toolchain is not None
    assert "python" in result.run.toolchain
    assert result.summary["error"] == "plan failed"


def test_build_error_result_macro_appears_in_metadata() -> None:
    """Test that macro name appears in result run metadata."""
    result = build_error_result(
        macro="report",
        root=Path("/workspace"),
        argv=["cq", "report", "refactor-impact"],
        tc=None,
        started_ms=4000.0,
        error="parse error",
    )

    assert result.run.macro == "report"
    assert result.summary["error"] == "parse error"


def test_build_error_result_with_path_string() -> None:
    """Test building error result with root as string."""
    result = build_error_result(
        macro="test",
        root="/tmp/string_root",
        argv=["cq", "test"],
        tc=None,
        started_ms=5000.0,
        error="test error",
    )

    assert result.run.root == "/tmp/string_root"
    assert result.summary["error"] == "test error"


def test_build_error_result_preserves_run_id() -> None:
    """Test that error results have stable run IDs."""
    result = build_error_result(
        macro="test",
        root=Path("/tmp/test"),
        argv=["cq", "test"],
        tc=None,
        started_ms=6000.0,
        error="error",
    )

    assert result.run.run_id is not None
    assert result.run.run_uuid_version is not None
    assert result.run.run_created_ms is not None
