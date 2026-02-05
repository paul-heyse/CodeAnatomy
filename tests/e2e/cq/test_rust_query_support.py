"""E2E tests for Rust query support in CQ."""

from __future__ import annotations

import subprocess
from collections.abc import Callable

import msgspec
import pytest
from tools.cq.core.schema import CqResult
from tools.cq.query.language import RUST_QUERY_ENABLE_ENV


def test_rust_query_gate_off(
    run_query: Callable[[str], CqResult],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rust queries should return a structured gate error when disabled."""
    monkeypatch.delenv(RUST_QUERY_ENABLE_ENV, raising=False)
    result = run_query("entity=function lang=rust in=tests/e2e/cq/_fixtures/")
    error = str(result.summary.get("error", ""))
    assert "disabled" in error.lower()
    assert RUST_QUERY_ENABLE_ENV in error


def test_rust_core_entities(
    run_query: Callable[[str], CqResult],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Function/import/callsite entity queries should work for Rust when enabled."""
    monkeypatch.setenv(RUST_QUERY_ENABLE_ENV, "1")

    function_result = run_query(
        "entity=function name=build_graph lang=rust in=tests/e2e/cq/_fixtures/"
    )
    assert any("build_graph" in finding.message for finding in function_result.key_findings)

    import_result = run_query("entity=import name=Debug lang=rust in=tests/e2e/cq/_fixtures/")
    assert any("Debug" in finding.message for finding in import_result.key_findings)

    call_result = run_query("entity=callsite name=helper lang=rust in=tests/e2e/cq/_fixtures/")
    assert any("helper" in finding.message for finding in call_result.key_findings)


def test_run_step_rust_query(
    run_command: Callable[[list[str]], subprocess.CompletedProcess[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cq run q-step should support Rust lang queries."""
    monkeypatch.setenv(RUST_QUERY_ENABLE_ENV, "1")
    proc = run_command(
        [
            "uv",
            "run",
            "python",
            "-m",
            "tools.cq.cli",
            "run",
            "--step",
            '{"type":"q","query":"entity=function name=build_graph lang=rust in=tests/e2e/cq/_fixtures/"}',
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )
    assert proc.returncode == 0, proc.stderr
    result = msgspec.json.decode(proc.stdout.encode("utf-8"), type=CqResult)
    assert any("build_graph" in finding.message for finding in result.key_findings)
