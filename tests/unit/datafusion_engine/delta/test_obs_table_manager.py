"""Tests for Delta observability table-manager exports."""

from __future__ import annotations

from datafusion_engine.delta import obs_table_manager


def test_obs_table_manager_exports() -> None:
    """Module should expose observability table-management helpers."""
    assert callable(obs_table_manager.ensure_observability_table)
    assert callable(obs_table_manager.ensure_observability_schema)
    assert callable(obs_table_manager.bootstrap_observability_table)
