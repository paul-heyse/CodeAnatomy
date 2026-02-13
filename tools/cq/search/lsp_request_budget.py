"""Shared request budget profiles for CQ LSP enrichment."""

from __future__ import annotations

import time
from collections.abc import Callable

from tools.cq.core.structs import CqStruct

_FAIL_OPEN_EXCEPTIONS = (OSError, RuntimeError, ValueError, TypeError)


class LspRequestBudgetV1(CqStruct, frozen=True):
    """Timeout and retry budget for one LSP request envelope."""

    startup_timeout_seconds: float = 3.0
    probe_timeout_seconds: float = 1.0
    max_attempts: int = 2
    retry_backoff_ms: int = 100


def call_with_retry(
    fn: Callable[[], object],
    *,
    max_attempts: int,
    retry_backoff_ms: int,
) -> tuple[object | None, bool]:
    """Call a function with timeout-only retry semantics.

    Returns:
    -------
    tuple[object | None, bool]
        (result, timed_out). Non-timeout exceptions fail fast without retries.
    """
    timed_out = False
    attempts = max(1, int(max_attempts))
    backoff_ms = max(0, int(retry_backoff_ms))
    for attempt in range(attempts):
        try:
            return fn(), timed_out
        except TimeoutError:
            timed_out = True
            if attempt + 1 >= attempts:
                return None, timed_out
            if backoff_ms > 0:
                time.sleep((backoff_ms / 1000.0) * (attempt + 1))
        except _FAIL_OPEN_EXCEPTIONS:
            return None, timed_out
    return None, timed_out


def budget_for_mode(mode: str) -> LspRequestBudgetV1:
    """Return standard budget profile by CQ command mode."""
    if mode == "calls":
        return LspRequestBudgetV1(
            startup_timeout_seconds=2.5,
            probe_timeout_seconds=1.25,
            max_attempts=2,
            retry_backoff_ms=120,
        )
    if mode == "entity":
        return LspRequestBudgetV1(
            startup_timeout_seconds=3.0,
            probe_timeout_seconds=1.25,
            max_attempts=2,
            retry_backoff_ms=120,
        )
    return LspRequestBudgetV1(
        startup_timeout_seconds=3.0,
        probe_timeout_seconds=1.0,
        max_attempts=2,
        retry_backoff_ms=100,
    )


__all__ = [
    "LspRequestBudgetV1",
    "budget_for_mode",
    "call_with_retry",
]
