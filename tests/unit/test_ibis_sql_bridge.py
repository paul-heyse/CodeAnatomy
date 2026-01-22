"""Tests for SQL ingestion artifacts."""

from __future__ import annotations

import ibis
import pytest

from ibis_engine.sql_bridge import (
    SqlIngestSpec,
    SqlIngestSqlGlotContext,
    parse_sql_table,
    sql_ingest_artifacts,
)
from sqlglot_tools.compat import parse_one


def test_sql_ingest_artifacts_payload() -> None:
    """Capture SQL and SQLGlot artifacts for ingestion."""
    expr = ibis.memtable({"a": [1, 2]})
    artifacts = sql_ingest_artifacts(
        "select 1 as a",
        expr=expr,
        context=SqlIngestSqlGlotContext(
            sqlglot_expr=parse_one("select 1 as a"),
            dialect="datafusion",
        ),
    )
    payload = artifacts.payload()
    assert payload["sql"]
    assert payload["decompiled_sql"]
    assert payload["sqlglot_sql"] is not None


def test_parse_sql_requires_schema() -> None:
    """Require schemas for SQL ingestion."""
    with pytest.raises(ValueError, match="schema is required"):
        parse_sql_table(
            SqlIngestSpec(
                sql="select 1 as a",
                catalog={},
                schema=None,
            )
        )


def test_parse_sql_validates_schema() -> None:
    """Validate SQL ingestion schema matches expected shape."""
    spec = SqlIngestSpec(
        sql="select 1 as a",
        catalog={},
        schema=ibis.schema({"a": "int64"}),
        dialect="datafusion",
    )
    table = parse_sql_table(spec)
    assert table.schema().names == ("a",)
