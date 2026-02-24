"""E2E regression tests for Rust literal classification semantics."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from tools.cq.core.schema import CqResult


@pytest.mark.e2e
def test_rust_literal_search_honors_include_strings_toggle(
    run_cq_result: Callable[..., CqResult],
    repo_root: Path,
) -> None:
    """Rust string-literal matches should be non-code and include-strings sensitive."""
    target_dir = "tests/e2e/cq/_fixtures"
    query = "Async UDFs require the async-udf feature"

    without_strings = run_cq_result(
        [
            "search",
            query,
            "--literal",
            "--lang",
            "rust",
            "--in",
            target_dir,
            "--format",
            "json",
            "--no-save-artifact",
        ],
        cwd=repo_root,
    )

    with_strings = run_cq_result(
        [
            "search",
            query,
            "--literal",
            "--lang",
            "rust",
            "--in",
            target_dir,
            "--include-strings",
            "--format",
            "json",
            "--no-save-artifact",
        ],
        cwd=repo_root,
    )

    assert not any(f.category == "callsite" for f in with_strings.evidence)

    without_titles = [section.title for section in without_strings.sections]
    with_titles = [section.title for section in with_strings.sections]
    assert any("Non-Code Matches" in title for title in without_titles)
    assert all("Non-Code Matches" not in title for title in with_titles)

    without_occurrence_count = next(
        (
            len(section.findings)
            for section in without_strings.sections
            if section.title == "Occurrences"
        ),
        None,
    )
    with_occurrence_count = next(
        (
            len(section.findings)
            for section in with_strings.sections
            if section.title == "Occurrences"
        ),
        None,
    )
    assert without_occurrence_count == 0
    assert isinstance(with_occurrence_count, int)
    assert with_occurrence_count > 0
