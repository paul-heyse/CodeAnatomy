"""Tests for the Pandera Polars validation bridge."""

from __future__ import annotations

import polars as pl
import pyarrow as pa

from schema_spec.pandera_bridge import validate_dataframe
from schema_spec.specs import TableSchemaSpec
from schema_spec.system import ValidationPolicySpec


def _policy() -> ValidationPolicySpec:
    return ValidationPolicySpec(enabled=True, strict=True, coerce=False, lazy=True)


def test_validate_dataframe_polars_roundtrip() -> None:
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    spec = TableSchemaSpec.from_schema("polars_validate", schema)
    df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    result = validate_dataframe(df, schema_spec=spec, policy=_policy())

    assert result is df


def test_validate_dataframe_arrow_table_roundtrip() -> None:
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    spec = TableSchemaSpec.from_schema("arrow_validate", table.schema)

    result = validate_dataframe(table, schema_spec=spec, policy=_policy())

    assert result is table
