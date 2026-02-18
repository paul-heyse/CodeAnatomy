"""Tests for shared Delta retry helper extraction."""

from __future__ import annotations

from storage.deltalake.config import DeltaRetryPolicy
from storage.deltalake.delta_runtime_ops import retry_with_policy


class _Span:
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    def set_attribute(self, key: str, value: object) -> None:
        self.events.append((key, value))


def test_retry_with_policy_returns_without_retries() -> None:
    policy = DeltaRetryPolicy(max_attempts=3, retryable_errors=("retry",), fatal_errors=("fatal",))
    value, attempts = retry_with_policy(lambda: "ok", policy=policy)
    assert value == "ok"
    assert attempts == 0


def test_retry_with_policy_retries_retryable_exception(monkeypatch) -> None:
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
        if attempts_seen["count"] < 3:
            raise RuntimeError("retryable conflict")
        return 42

    value, attempts = retry_with_policy(_op, policy=policy, span=span)
    assert value == 42
    assert attempts == 2
    assert ("codeanatomy.retry_attempt", 1) in span.events
    assert ("codeanatomy.retry_attempt", 2) in span.events


def test_retry_with_policy_raises_non_retryable() -> None:
    policy = DeltaRetryPolicy(max_attempts=3, retryable_errors=("retryable",), fatal_errors=("fatal",))
    calls = {"count": 0}

    def _op() -> None:
        calls["count"] += 1
        raise RuntimeError("permanent failure")

    try:
        retry_with_policy(_op, policy=policy)
    except RuntimeError as exc:
        assert "permanent failure" in str(exc)
    else:  # pragma: no cover - defensive guard for assertion clarity
        raise AssertionError("Expected RuntimeError")
    assert calls["count"] == 1
