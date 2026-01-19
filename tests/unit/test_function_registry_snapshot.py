"""Unit tests for function registry snapshots."""

from __future__ import annotations

from engine.function_registry import default_function_registry


def test_function_registry_snapshot_is_stable() -> None:
    """Keep function registry snapshots deterministic."""
    first = default_function_registry()
    second = default_function_registry()
    assert first.payload() == second.payload()
    assert first.fingerprint() == second.fingerprint()


def test_function_registry_includes_pyarrow_compute_list() -> None:
    """Expose sorted PyArrow compute function names."""
    registry = default_function_registry()
    payload = registry.payload()
    compute = payload.get("pyarrow_compute", [])
    assert isinstance(compute, list)
    assert compute == sorted(compute)
    pycapsule_ids = payload.get("pycapsule_ids", [])
    assert isinstance(pycapsule_ids, list)
    assert pycapsule_ids == sorted(set(pycapsule_ids))
