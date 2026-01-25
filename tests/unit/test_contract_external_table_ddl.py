"""Tests for contract-driven external table DDL generation."""

from __future__ import annotations

import pyarrow as pa
import pytest
from sqlglot.errors import ErrorLevel

from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.registry import DatasetLocation
from ibis_engine.schema_utils import sqlglot_column_sql
from schema_spec.specs import ArrowFieldSpec, TableSchemaSpec
from schema_spec.system import DataFusionScanOptions, DatasetSpec
from sqlglot_tools.compat import parse_one
from sqlglot_tools.optimizer import canonical_ast_fingerprint

datafusion = pytest.importorskip("datafusion")


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
    adapter = DataFusionIOAdapter(
        ctx=datafusion.SessionContext(),
        profile=DataFusionRuntimeProfile(),
    )
    ddl = adapter.external_table_ddl(name="events", location=location)
    expr = parse_one(ddl, dialect="datafusion", error_level=ErrorLevel.IGNORE)
    fingerprint = canonical_ast_fingerprint(expr)
    assert fingerprint == "bf68b5c04ea11fc0a898e2e59626c198b6f4919fbba46ddf5b98893d28cb07f2"


def test_external_table_option_precedence() -> None:
    """Prefer location read options over profile defaults."""
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
    adapter = DataFusionIOAdapter(ctx=datafusion.SessionContext(), profile=profile)
    ddl = adapter.external_table_ddl(name="metrics", location=location)
    expr = parse_one(ddl, dialect="datafusion", error_level=ErrorLevel.IGNORE)
    fingerprint = canonical_ast_fingerprint(expr)
    assert fingerprint == "20cc4c9b63214bcdc31a51ab29af43108eebb9265ba4932a50d4e771262a9baa"


def test_sqlglot_column_defs_match_schema_utils() -> None:
    """Align SQLGlot column definitions across DDL entrypoints."""
    table_spec = TableSchemaSpec(
        name="events",
        fields=[
            ArrowFieldSpec(name="id", dtype=pa.int64(), nullable=True),
            ArrowFieldSpec(name="payload", dtype=pa.string(), nullable=True),
        ],
    )
    expected = [
        col.sql(dialect="datafusion")
        for col in table_spec.to_sqlglot_column_defs(dialect="datafusion")
    ]
    schema = table_spec.to_arrow_schema()
    assert sqlglot_column_sql(schema, dialect="datafusion") == expected
