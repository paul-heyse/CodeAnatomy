"""Tests for contract-driven external table DDL generation."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.registry_bridge import datafusion_external_table_sql
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.registry import DatasetLocation
from schema_spec.specs import ArrowFieldSpec, TableSchemaSpec
from schema_spec.system import DataFusionScanOptions, DatasetSpec


def _dataset_location(
    *,
    path: str,
    table_spec: TableSchemaSpec,
    read_options: dict[str, object],
    partition_cols: tuple[tuple[str, pa.DataType], ...],
) -> DatasetLocation:
    scan = DataFusionScanOptions(partition_cols=partition_cols)
    dataset_spec = DatasetSpec(table_spec=table_spec, datafusion_scan=scan)
    return DatasetLocation(
        path=path,
        format="parquet",
        dataset_spec=dataset_spec,
        read_options=read_options,
    )


def test_external_table_ddl_contains_expected_clauses() -> None:
    """Emit external table DDL clauses from contract metadata."""
    table_spec = TableSchemaSpec(
        name="events",
        fields=[
            ArrowFieldSpec(name="id", dtype=pa.int64(), nullable=False),
            ArrowFieldSpec(name="day", dtype=pa.string()),
        ],
        key_fields=("id",),
    )
    location = _dataset_location(
        path="/tmp/events",
        table_spec=table_spec,
        read_options={"compression": "zstd", "coalesce_batches": True},
        partition_cols=(("day", pa.string()),),
    )
    sql = datafusion_external_table_sql(
        name="events",
        location=location,
        runtime_profile=DataFusionRuntimeProfile(),
    )
    assert sql is not None
    assert "CREATE EXTERNAL TABLE events" in sql
    assert "STORED AS PARQUET" in sql
    assert "LOCATION '/tmp/events'" in sql
    assert "PARTITIONED BY (day)" in sql
    assert "WITH ORDER (id)" in sql
    assert "COMPRESSION TYPE zstd" in sql
    assert "OPTIONS ('coalesce_batches' 'true')" in sql


def test_external_table_option_precedence() -> None:
    """Prefer statement overrides over table and session defaults."""
    table_spec = TableSchemaSpec(
        name="metrics",
        fields=[ArrowFieldSpec(name="metric_id", dtype=pa.int64())],
        key_fields=("metric_id",),
    )
    location = _dataset_location(
        path="/tmp/metrics",
        table_spec=table_spec,
        read_options={
            "compression": "zstd",
            "coalesce_batches": True,
            "rows_per_page": 2000,
        },
        partition_cols=(),
    )
    profile = DataFusionRuntimeProfile(
        external_table_options={
            "compression": "snappy",
            "coalesce_batches": False,
            "rows_per_page": 1000,
        }
    )
    sql = datafusion_external_table_sql(
        name="metrics",
        location=location,
        runtime_profile=profile,
        options_override={
            "compression": "gzip",
            "rows_per_page": 3000,
        },
    )
    assert sql is not None
    assert "COMPRESSION TYPE gzip" in sql
    assert "OPTIONS ('coalesce_batches' 'true', 'rows_per_page' '3000')" in sql
