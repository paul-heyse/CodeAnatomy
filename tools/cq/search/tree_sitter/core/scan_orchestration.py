"""Scan orchestration helpers for tree-sitter workloads."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ScanTaskV1:
    """Single file/language scan task descriptor."""

    file_path: Path
    language: str


__all__ = ["ScanTaskV1", "orchestrate_scan"]


def orchestrate_scan(
    tasks: Iterable[ScanTaskV1],
    *,
    run_task: Callable[[ScanTaskV1], object],
) -> list[object]:
    """Run scan tasks sequentially and collect results.

    Returns:
        list[object]: Ordered task results.
    """
    return [run_task(task) for task in tasks]
