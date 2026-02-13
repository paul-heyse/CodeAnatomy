"""Unit tests for keyed LSP request queue behavior."""

from __future__ import annotations

import time

from tools.cq.search.lsp.request_queue import run_lsp_requests


def test_run_lsp_requests_preserves_keyed_results() -> None:
    batch = run_lsp_requests(
        {
            "first": lambda: ("a",),
            "second": lambda: ("b",),
        },
        timeout_seconds=1.0,
    )
    assert batch.results["first"] == ("a",)
    assert batch.results["second"] == ("b",)
    assert batch.timed_out == ()


def test_run_lsp_requests_tracks_timeout_and_partial_completion() -> None:
    def _slow() -> tuple[str]:
        time.sleep(0.08)
        return ("slow",)

    batch = run_lsp_requests(
        {
            "slow": _slow,
            "after_slow": lambda: ("never-runs-before-timeout",),
        },
        timeout_seconds=0.01,
    )

    assert "slow" in batch.results
    assert "after_slow" in batch.timed_out
