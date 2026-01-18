"""Tests for SQL ingestion artifacts."""

from __future__ import annotations

import ibis
from sqlglot import parse_one

from ibis_engine.sql_bridge import sql_ingest_artifacts


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
