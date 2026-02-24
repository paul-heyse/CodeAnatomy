"""E2E regression tests for Rust-unsupported macro contracts."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from tools.cq.core.schema import CqResult


@pytest.mark.e2e
@pytest.mark.parametrize(
    ("argv", "expected_variant"),
    [
        (
            [
                "impact",
                "register_udf",
                "--param",
                "udf",
                "--include",
                "rust/**/*.rs",
                "--format",
                "json",
                "--no-save-artifact",
            ],
            "impact",
        ),
        (
            [
                "sig-impact",
                "register_udf",
                "--to",
                "register_udf(name: str)",
                "--include",
                "rust/**/*.rs",
                "--format",
                "json",
                "--no-save-artifact",
            ],
            "sig-impact",
        ),
        (
            [
                "exceptions",
                "--include",
                "rust/**/*.rs",
                "--format",
                "json",
                "--no-save-artifact",
            ],
            "search",
        ),
        (
            [
                "imports",
                "--include",
                "rust/**/*.rs",
                "--format",
                "json",
                "--no-save-artifact",
            ],
            "search",
        ),
    ],
)
def test_rust_none_macros_emit_capability_only_contract(
    run_cq_result: Callable[..., CqResult],
    argv: list[str],
    expected_variant: str,
) -> None:
    """Rust-none macros should return capability diagnostics without pseudo analysis."""
    result = run_cq_result(argv)
    assert result.summary.get("summary_variant") == expected_variant
    assert len(result.evidence) == 0
    assert len(result.sections) == 0
    assert result.key_findings
    assert all(f.category == "capability_limitation" for f in result.key_findings)
