"""Unit tests for routine introspection helper module."""

from __future__ import annotations

from datafusion_engine.schema import introspection_routines


def test_introspection_routines_exports() -> None:
    """Expose routine and parameter snapshot helpers from public exports."""
    assert "routines_snapshot_table" in introspection_routines.__all__
    assert "parameters_snapshot_table" in introspection_routines.__all__
