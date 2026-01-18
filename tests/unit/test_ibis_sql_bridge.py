"""Tests for SQL ingestion artifacts."""

from __future__ import annotations

import ibis
import pytest
from sqlglot import parse_one

from ibis_engine.sql_bridge import (
    SqlIngestSpec,
    execute_raw_sql,
    parse_sql_table,
    sql_ingest_artifacts,
)


def test_sql_ingest_artifacts_payload() -> None:
    """Capture SQL and AST artifacts for ingestion."""
    expr = ibis.memtable({"a": [1, 2]})
    artifacts = sql_ingest_artifacts(
        "select 1 as a",
        expr=expr,
        sqlglot_expr=parse_one("select 1 as a"),
        dialect="datafusion",
    )
    payload = artifacts.payload()
    assert payload["sql"]
    assert payload["decompiled_sql"]
    assert payload["sqlglot_ast"] is not None


def test_execute_raw_sql_accepts_sqlglot_expr() -> None:
    """Execute raw SQL with SQLGlot expression support."""
    backend = ibis.datafusion.connect()
    table = execute_raw_sql(
        backend,
        sql="select 1 as a",
        sqlglot_expr=parse_one("select 1 as a"),
        schema=ibis.schema({"a": "int64"}),
    )
    assert table.schema().names == ("a",)


def test_parse_sql_requires_schema() -> None:
    """Require schemas for SQL ingestion."""
    backend = ibis.datafusion.connect()
    with pytest.raises(ValueError, match="schema is required"):
        parse_sql_table(
            SqlIngestSpec(
                sql="select 1 as a",
                catalog=backend,
                schema=None,
            )
        )


def test_parse_sql_validates_schema() -> None:
    """Validate SQL ingestion schema matches expected shape."""
    backend = ibis.datafusion.connect()
    spec = SqlIngestSpec(
        sql="select 1 as a",
        catalog=backend,
        schema=ibis.schema({"a": "int64"}),
        dialect="datafusion",
    )
    table = parse_sql_table(spec)
    assert table.schema().names == ("a",)
