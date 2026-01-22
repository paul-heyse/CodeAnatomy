"""Golden tests for SQL ingestion SQL checkpoints."""

from __future__ import annotations

from collections.abc import Mapping

import ibis

from ibis_engine.sql_bridge import SqlIngestSpec, parse_sql_table
from sqlglot_tools.compat import parse_one

SQL_SELECT_ID = "SELECT id FROM events"
SQL_SELECT_FILTER = "SELECT id, label FROM events WHERE id > 1"


def _ingest(
    sql: str,
    schema: ibis.Schema,
    *,
    catalog_schema: ibis.Schema,
) -> Mapping[str, object]:
    payloads: list[Mapping[str, object]] = []

    def _hook(payload: Mapping[str, object]) -> None:
        payloads.append(payload)

    spec = SqlIngestSpec(
        sql=sql,
        catalog={"events": catalog_schema},
        schema=schema,
        artifacts_hook=_hook,
    )
    _ = parse_sql_table(spec)
    return payloads[-1]


def test_sql_ingest_golden_select() -> None:
    """Capture SQLGlot SQL for a basic select."""
    payload = _ingest(
        SQL_SELECT_ID,
        schema=ibis.schema({"id": "int64"}),
        catalog_schema=ibis.schema({"id": "int64", "label": "string"}),
    )
    expected = parse_one(SQL_SELECT_ID).sql()
    assert payload.get("sqlglot_sql") == expected
    assert payload.get("normalized_sql") == expected
    assert payload.get("sqlglot_ast") is not None


def test_sql_ingest_golden_select_filter() -> None:
    """Capture SQLGlot SQL for a filtered select."""
    payload = _ingest(
        SQL_SELECT_FILTER,
        schema=ibis.schema({"id": "int64", "label": "string"}),
        catalog_schema=ibis.schema({"id": "int64", "label": "string"}),
    )
    expected = parse_one(SQL_SELECT_FILTER).sql()
    assert payload.get("sqlglot_sql") == expected
    assert payload.get("normalized_sql") == expected
    assert payload.get("sqlglot_ast") is not None
