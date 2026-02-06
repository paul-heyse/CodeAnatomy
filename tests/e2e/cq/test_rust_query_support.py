"""E2E tests for Rust query support in CQ."""

from __future__ import annotations

import subprocess
from collections.abc import Callable

import msgspec
from tools.cq.core.schema import CqResult


def test_rust_query_available_without_feature_gate(run_query: Callable[[str], CqResult]) -> None:
    """Rust queries should execute without feature-gate toggles."""
    result = run_query("entity=function lang=rust in=tests/e2e/cq/_fixtures/")
    assert "error" not in result.summary


def test_rust_core_entities(
    run_query: Callable[[str], CqResult],
) -> None:
    """Function/import/callsite entity queries should work for Rust."""
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
) -> None:
    """Cq run q-step should support Rust lang queries."""
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
