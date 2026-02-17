"""Tests for runtime state registry behavior."""

from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.runtime_state import RuntimeStateRegistry


def test_runtime_state_registry_returns_same_state_for_same_context() -> None:
    """State registry returns same mutable state for identical context."""
    registry = RuntimeStateRegistry()
    ctx = SessionContext()
    first = registry.state_for(ctx)
    first["x"] = 1
    second = registry.state_for(ctx)
    assert second["x"] == 1
    assert first is second


def test_runtime_state_registry_clear_resets_state() -> None:
    """Clearing registry removes per-context state entries."""
    registry = RuntimeStateRegistry()
    ctx = SessionContext()
    registry.state_for(ctx)["x"] = 1
    registry.clear()
    assert registry.state_for(ctx) == {}
