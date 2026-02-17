"""Tests for OpenTelemetry provider reset behavior in test mode."""

from __future__ import annotations

import pytest

from obs.otel import bootstrap


class _FakeProviders:
    def __init__(self) -> None:
        self.shutdown_called = False

    def shutdown(self) -> None:
        self.shutdown_called = True


def _bootstrap_state() -> dict[str, object]:
    return bootstrap.__dict__["_STATE"]


def test_reset_providers_for_tests_clears_state_and_shuts_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reset shuts down configured providers and clears cached global state."""
    fake = _FakeProviders()
    state = _bootstrap_state()
    monkeypatch.setitem(state, "providers", fake)

    bootstrap.reset_providers_for_tests()

    assert fake.shutdown_called is True
    assert state["providers"] is None


def test_reset_providers_for_tests_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reset can be called repeatedly when no providers are configured."""
    state = _bootstrap_state()
    monkeypatch.setitem(state, "providers", None)
    bootstrap.reset_providers_for_tests()
    assert state["providers"] is None
