"""Unit tests for LSP request budget/retry helpers."""

from __future__ import annotations

from tools.cq.search.lsp_request_budget import budget_for_mode, call_with_retry


def test_budget_for_mode_profiles() -> None:
    assert budget_for_mode("search").probe_timeout_seconds > 0
    assert budget_for_mode("calls").max_attempts >= 1
    assert budget_for_mode("entity").startup_timeout_seconds >= 1.0


def test_call_with_retry_retries_timeout_then_succeeds() -> None:
    state = {"count": 0}

    def _flaky() -> str:
        state["count"] += 1
        if state["count"] == 1:
            msg = "timeout"
            raise TimeoutError(msg)
        return "ok"

    value, timed_out = call_with_retry(_flaky, max_attempts=2, retry_backoff_ms=0)
    assert value == "ok"
    assert timed_out is True


def test_call_with_retry_fail_fast_on_non_timeout() -> None:
    def _boom() -> str:
        msg = "boom"
        raise RuntimeError(msg)

    value, timed_out = call_with_retry(_boom, max_attempts=3, retry_backoff_ms=0)
    assert value is None
    assert timed_out is False
