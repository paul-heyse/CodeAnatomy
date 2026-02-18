"""Unit tests for language-scope orchestration helpers."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.orchestration.language_scope import (
    execute_by_language_scope,
    language_priority,
    merge_partitioned_items,
)


def test_language_priority_auto_scope_order() -> None:
    """Auto scope should keep canonical python-then-rust ordering."""
    assert language_priority("auto") == {"python": 0, "rust": 1}


def test_execute_by_language_scope_single_language() -> None:
    """Single-language scope should execute one callback without scheduler fanout."""
    calls: list[str] = []

    def run_one(language: str) -> str:
        calls.append(language)
        return f"{language}-ok"

    out = execute_by_language_scope("python", run_one)
    assert calls == ["python"]
    assert out == {"python": "python-ok"}


def test_merge_partitioned_items_orders_by_scope_then_score() -> None:
    """Merged rows should prioritize language order, then descending score."""
    @dataclass(frozen=True)
    class _Row:
        lang: str
        score: float
        loc: tuple[str, int, int]

    partitions = {
        "rust": [_Row(lang="rust", score=0.2, loc=("b.rs", 3, 1))],
        "python": [
            _Row(lang="python", score=0.1, loc=("a.py", 2, 1)),
            _Row(lang="python", score=0.9, loc=("a.py", 1, 1)),
        ],
    }
    merged = merge_partitioned_items(
        partitions=partitions,
        scope="auto",
        get_language=lambda row: row.lang,
        get_score=lambda row: row.score,
        get_location=lambda row: row.loc,
    )
    assert [row.score for row in merged] == [0.9, 0.1, 0.2]
