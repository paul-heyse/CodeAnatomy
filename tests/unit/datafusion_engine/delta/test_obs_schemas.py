"""Tests for Delta observability schema builders."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.delta import obs_schemas


def test_obs_schema_builders_return_arrow_schema() -> None:
    """All observability schema builders should return Arrow schemas."""
    assert isinstance(obs_schemas.delta_snapshot_schema(), pa.Schema)
    assert isinstance(obs_schemas.delta_mutation_schema(), pa.Schema)
    assert isinstance(obs_schemas.delta_scan_plan_schema(), pa.Schema)
    assert isinstance(obs_schemas.delta_maintenance_schema(), pa.Schema)
