"""E2E tests for Rust scope propagation in run/chain search steps."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from tools.cq.core.schema import CqResult


def _step_summary(result: CqResult, step_id: str) -> object:
    step_summaries = result.summary.get("step_summaries")
    if isinstance(step_summaries, dict):
        return step_summaries.get(step_id)
    return None


@pytest.mark.e2e
def test_run_search_step_aliases_preserve_rust_scope(
    run_cq_result: Callable[..., CqResult],
) -> None:
    """Run --steps search payloads should honor lang/in aliases for Rust scope."""
    result = run_cq_result(
        [
            "run",
            "--steps",
            '[{"type":"search","query":"register_udf","lang":"rust","in":"rust"}]',
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )
    steps = result.summary.get("steps")
    assert isinstance(steps, list)
    assert len(steps) == 1
    step = _step_summary(result, str(steps[0]))
    assert isinstance(step, dict)
    assert step.get("lang_scope") == "rust"
    languages = step.get("languages")
    assert isinstance(languages, dict)
    assert set(languages) == {"rust"}


@pytest.mark.e2e
def test_chain_search_step_preserves_explicit_rust_scope(
    run_cq_result: Callable[..., CqResult],
) -> None:
    """Chain search should forward --lang rust into search step execution."""
    result = run_cq_result(
        [
            "chain",
            "search",
            "register_udf",
            "--lang",
            "rust",
            "--in",
            "rust",
            "AND",
            "q",
            "entity=function name=register_udf lang=rust in=rust",
            "--format",
            "json",
            "--no-save-artifact",
        ]
    )
    steps = result.summary.get("steps")
    assert isinstance(steps, list)
    search_steps = [step_id for step_id in steps if str(step_id).startswith("search_")]
    assert search_steps
    summary = _step_summary(result, str(search_steps[0]))
    assert isinstance(summary, dict)
    assert summary.get("lang_scope") == "rust"
    languages = summary.get("languages")
    assert isinstance(languages, dict)
    assert set(languages) == {"rust"}
