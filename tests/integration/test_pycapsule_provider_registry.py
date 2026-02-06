"""Integration tests for DataFusion table provider registry snapshots."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from datafusion_engine.dataset.registration import (
    DatasetRegistrationOptions,
    register_dataset_df,
)
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.expr.spec import ExprSpec
from schema_spec.specs import TableSchemaSpec
from schema_spec.system import DatasetSpec
from tests.test_helpers.delta_seed import DeltaSeedOptions, write_delta_table
from tests.test_helpers.diagnostics import diagnostic_profile
from tests.test_helpers.optional_deps import (
    require_datafusion_udfs,
    require_delta_extension,
    require_deltalake,
)

require_datafusion_udfs()
require_deltalake()
require_delta_extension()

EXPECTED_ROW_COUNT = 2


@pytest.mark.integration
def test_table_provider_registry_records_delta_capsule(tmp_path: Path) -> None:
    """Record table provider capabilities for Delta-backed tables."""
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    profile, sink = diagnostic_profile()
    profile, ctx, delta_path = write_delta_table(
        tmp_path,
        table=table,
        options=DeltaSeedOptions(
            profile=profile,
            table_name="delta_table",
            schema_mode="overwrite",
        ),
    )
    register_dataset_df(
        ctx,
        name="delta_tbl",
        location=DatasetLocation(path=str(delta_path), format="delta"),
        options=DatasetRegistrationOptions(runtime_profile=profile),
    )
    df = ctx.sql("SELECT COUNT(*) AS row_count FROM delta_tbl")
    result = df.to_arrow_table()
    assert result.column("row_count")[0].as_py() == EXPECTED_ROW_COUNT
    artifacts = sink.artifacts_snapshot().get("datafusion_table_providers_v1", [])
    assert artifacts
    entry = next((item for item in artifacts if item.get("name") == "delta_tbl"), None)
    assert entry is not None
    assert entry.get("ffi_table_provider") is True
    assert entry.get("provider_mode") != "dataset_fallback"


@pytest.mark.integration
def test_delta_pruning_predicate_from_dataset_spec(tmp_path: Path) -> None:
    """Apply file pruning when dataset specs include pushdown predicates."""
    table = pa.table({"part": ["a", "b"], "value": [1, 2]})
    profile, sink = diagnostic_profile()
    profile, ctx, delta_path = write_delta_table(
        tmp_path,
        table=table,
        options=DeltaSeedOptions(
            profile=profile,
            table_name="delta_table",
            partition_by=("part",),
            schema_mode="overwrite",
        ),
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
        options=DatasetRegistrationOptions(runtime_profile=profile),
    )
    artifacts = sink.artifacts_snapshot().get("datafusion_table_providers_v1", [])
    entry = next((item for item in artifacts if item.get("name") == "delta_tbl"), None)
    assert entry is not None
    assert entry.get("ffi_table_provider") is True
    assert entry.get("provider_mode") != "dataset_fallback"
    assert entry.get("delta_pruning_predicate") == "part = 'a'"
    assert entry.get("delta_pruning_applied") is True
    assert entry.get("delta_pruned_files") is not None
