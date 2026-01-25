"""Integration tests for materialization surfaces."""

from __future__ import annotations

from pathlib import Path

import ibis
import pytest

from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.io_bridge import IbisMaterializeOptions, materialize_table
from ibis_engine.sources import SourceToIbisOptions, register_ibis_view
from obs.diagnostics import DiagnosticsCollector

pytest.importorskip("duckdb")

EXPECTED_ROW_COUNT = 2


@pytest.mark.integration
def test_materialization_surfaces_record_actions(tmp_path: Path) -> None:
    """Create tables, views, and inserts with namespace logging."""
    backend = ibis.duckdb.connect(str(tmp_path / "materialize.duckdb"))
    sink = DiagnosticsCollector()
    profile = DataFusionRuntimeProfile(diagnostics_sink=sink)

    expr = ibis.memtable({"id": [1, 2], "label": ["a", "b"]})
    _ = materialize_table(
        expr,
        options=IbisMaterializeOptions(
            backend=backend,
            name="materialized_table",
            overwrite=True,
            runtime_profile=profile,
        ),
    )
    view_plan = register_ibis_view(
        expr,
        options=SourceToIbisOptions(
            backend=backend,
            name="materialized_view",
            overwrite=True,
            runtime_profile=profile,
        ),
    )
    assert "materialized_table" in backend.list_tables()
    assert "materialized_view" in backend.list_tables()
    assert view_plan.expr.count().execute() == EXPECTED_ROW_COUNT
    actions = sink.artifacts_snapshot().get("ibis_namespace_actions_v1", [])
    action_names = [action.get("action") for action in actions]
    assert "create_table" in action_names
    assert "insert" in action_names
    assert "create_view" in action_names
