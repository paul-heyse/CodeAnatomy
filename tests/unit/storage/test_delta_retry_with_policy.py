"""Tests for shared Delta retry helper extraction."""

from __future__ import annotations

from typing import Any, cast

import pytest

from storage.deltalake.config import DeltaRetryPolicy
from storage.deltalake.delta_runtime_ops import retry_with_policy

_INITIAL_ATTEMPT_COUNT = 0
_RETRY_THRESHOLD_ATTEMPTS = 3
_EXPECTED_RETRY_COUNT = 2
_EXPECTED_RESULT_VALUE = 42
_SLEEP_ERROR_MESSAGE = "permanent failure"


class _Span:
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    def set_attribute(self, key: str, value: object) -> None:
        self.events.append((key, value))


def test_retry_with_policy_returns_without_retries() -> None:
    """Retry helper should return immediately when no errors are raised."""
    policy = DeltaRetryPolicy(max_attempts=3, retryable_errors=("retry",), fatal_errors=("fatal",))
    value, attempts = retry_with_policy(lambda: "ok", policy=policy)
    assert value == "ok"
    assert attempts == _INITIAL_ATTEMPT_COUNT


def test_retry_with_policy_retries_retryable_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry helper should retry retryable exceptions and return final value."""
    monkeypatch.setattr("storage.deltalake.delta_runtime_ops.time.sleep", lambda _seconds: None)
    policy = DeltaRetryPolicy(
        max_attempts=4,
        base_delay_s=0.01,
        max_delay_s=0.01,
        retryable_errors=("retryable",),
        fatal_errors=("fatal",),
    )
    span = _Span()
    attempts_seen = {"count": 0}

    def _op() -> int:
        attempts_seen["count"] += 1
        if attempts_seen["count"] < _RETRY_THRESHOLD_ATTEMPTS:
            msg = "retryable conflict"
            raise RuntimeError(msg)
        return _EXPECTED_RESULT_VALUE

    value, attempts = retry_with_policy(_op, policy=policy, span=cast("Any", span))
    assert value == _EXPECTED_RESULT_VALUE
    assert attempts == _EXPECTED_RETRY_COUNT
    assert ("codeanatomy.retry_attempt", 1) in span.events
    assert ("codeanatomy.retry_attempt", _EXPECTED_RETRY_COUNT) in span.events


def test_retry_with_policy_raises_non_retryable() -> None:
    """Retry helper should raise immediately for non-retryable failures."""
    policy = DeltaRetryPolicy(
        max_attempts=3, retryable_errors=("retryable",), fatal_errors=("fatal",)
    )
    calls = {"count": 0}

    def _op() -> None:
        calls["count"] += 1
        raise RuntimeError(_SLEEP_ERROR_MESSAGE)

    with pytest.raises(RuntimeError, match=_SLEEP_ERROR_MESSAGE):
        retry_with_policy(_op, policy=policy)
    assert calls["count"] == 1
