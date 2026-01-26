"""Unit tests for function registry snapshots."""

from __future__ import annotations

from engine.function_registry import default_function_registry


def test_function_registry_snapshot_is_stable() -> None:
    """Keep function registry snapshots deterministic."""
    first = default_function_registry()
    second = default_function_registry()
    assert first.payload() == second.payload()
    assert first.fingerprint() == second.fingerprint()


def test_function_registry_payload_excludes_legacy_lists() -> None:
    """Ensure legacy compute lists are not included in the payload."""
    registry = default_function_registry()
    payload = registry.payload()
    assert "pyarrow_compute" not in payload
    assert "pycapsule_ids" not in payload
