"""Tests for tree-sitter parallel lane helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.core.parallel import run_file_lanes_parallel


def _double(value: int) -> int:
    return value * 2


def test_run_file_lanes_parallel_preserves_input_order_with_process_pool() -> None:
    """Parallel lane runner should return results ordered by input index."""
    assert run_file_lanes_parallel([3, 1, 2], worker=_double, max_workers=2) == [6, 2, 4]


def test_run_file_lanes_parallel_uses_serial_mode_for_single_worker() -> None:
    """Single-worker mode should execute jobs serially."""
    assert run_file_lanes_parallel([1, 2], worker=_double, max_workers=1) == [2, 4]
