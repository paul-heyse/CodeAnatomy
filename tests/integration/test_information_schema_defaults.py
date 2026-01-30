"""Integration checks for information_schema column defaults."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datafusion_engine.dataset_registration import register_dataset_df
from datafusion_engine.dataset_registry import DatasetLocation
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.schema_introspection import SchemaIntrospector
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import TableSchemaSpec

pytest.importorskip("datafusion")
pytest.importorskip("datafusion_ext")


@pytest.mark.integration
def test_information_schema_column_defaults(tmp_path: Path) -> None:
    """Ensure column defaults surface in information_schema.columns."""
    table_spec = TableSchemaSpec(
        name="defaults_tbl",
        fields=[
            FieldSpec(name="id", dtype=pa.int64(), nullable=False),
            FieldSpec(name="status", dtype=pa.string(), default_value="unknown"),
        ],
        key_fields=("id",),
    )
    schema = table_spec.to_arrow_schema()
    table = pa.table({"id": [1], "status": ["ok"]}, schema=schema)
    data_dir = tmp_path / "defaults_tbl"
    data_dir.mkdir()
    pq.write_table(table, data_dir / "part-0.parquet")

    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    register_dataset_df(
        ctx,
        name="defaults_tbl",
        location=DatasetLocation(path=str(data_dir), format="parquet", table_spec=table_spec),
        runtime_profile=profile,
    )
    defaults = SchemaIntrospector(ctx).table_column_defaults("defaults_tbl")
    assert defaults.get("status") is not None
