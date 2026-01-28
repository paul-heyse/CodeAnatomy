"""Integration tests for DataFusion table provider registry snapshots."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from datafusion_engine.dataset_registry import DatasetLocation
from datafusion_engine.expr_spec import ExprSpec
from datafusion_engine.registry_bridge import register_dataset_df
from datafusion_engine.runtime import DataFusionRuntimeProfile
from obs.diagnostics import DiagnosticsCollector
from schema_spec.specs import TableSchemaSpec
from schema_spec.system import DatasetSpec
from storage.deltalake import DeltaWriteOptions, write_delta_table

pytest.importorskip("datafusion")
pytest.importorskip("deltalake")

EXPECTED_ROW_COUNT = 2


@pytest.mark.integration
def test_table_provider_registry_records_delta_capsule(tmp_path: Path) -> None:
    """Record table provider capabilities for Delta-backed tables."""
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    delta_path = tmp_path / "delta_table"

    sink = DiagnosticsCollector()
    profile = DataFusionRuntimeProfile(diagnostics_sink=sink)
    ctx = profile.session_context()
    write_delta_table(
        table,
        str(delta_path),
        options=DeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
        ctx=ctx,
    )
    register_dataset_df(
        ctx,
        name="delta_tbl",
        location=DatasetLocation(path=str(delta_path), format="delta"),
        runtime_profile=profile,
    )
    df = ctx.sql("SELECT COUNT(*) AS row_count FROM delta_tbl")
    result = df.to_arrow_table()
    assert result.column("row_count")[0].as_py() == EXPECTED_ROW_COUNT
    artifacts = sink.artifacts_snapshot().get("datafusion_table_providers_v1", [])
    assert artifacts
    assert any(entry.get("name") == "delta_tbl" for entry in artifacts)


@pytest.mark.integration
def test_delta_ddl_registration_opt_in(tmp_path: Path) -> None:
    """Register Delta tables via DDL when explicitly enabled."""
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    delta_path = tmp_path / "delta_table"

    sink = DiagnosticsCollector()
    profile = DataFusionRuntimeProfile(
        diagnostics_sink=sink,
        enable_delta_ddl_registration=True,
    )
    ctx = profile.session_context()
    write_delta_table(
        table,
        str(delta_path),
        options=DeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
        ctx=ctx,
    )
    register_dataset_df(
        ctx,
        name="delta_tbl",
        location=DatasetLocation(path=str(delta_path), format="delta"),
        runtime_profile=profile,
    )
    artifacts = sink.artifacts_snapshot().get("datafusion_table_providers_v1", [])
    entry = next((item for item in artifacts if item.get("name") == "delta_tbl"), None)
    assert entry is not None
    assert entry.get("registration_path") == "ddl"


@pytest.mark.integration
def test_delta_pruning_predicate_from_dataset_spec(tmp_path: Path) -> None:
    """Apply file pruning when dataset specs include pushdown predicates."""
    table = pa.table({"part": ["a", "b"], "value": [1, 2]})
    delta_path = tmp_path / "delta_table"

    sink = DiagnosticsCollector()
    profile = DataFusionRuntimeProfile(diagnostics_sink=sink)
    ctx = profile.session_context()
    write_delta_table(
        table,
        str(delta_path),
        options=DeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            partition_by=("part",),
        ),
        ctx=ctx,
    )
    table_spec = TableSchemaSpec.from_schema("delta_tbl", table.schema)
    dataset_spec = DatasetSpec(
        table_spec=table_spec,
        pushdown_predicate=ExprSpec(sql="part = 'a'"),
    )
    register_dataset_df(
        ctx,
        name="delta_tbl",
        location=DatasetLocation(
            path=str(delta_path),
            format="delta",
            dataset_spec=dataset_spec,
        ),
        runtime_profile=profile,
    )
    artifacts = sink.artifacts_snapshot().get("datafusion_table_providers_v1", [])
    entry = next((item for item in artifacts if item.get("name") == "delta_tbl"), None)
    assert entry is not None
    assert entry.get("delta_pruning_predicate") == "part = 'a'"
    assert entry.get("delta_pruning_applied") is True
    assert entry.get("delta_pruned_files") is not None
