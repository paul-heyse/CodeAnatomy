"""Golden tests for SQL ingestion AST checkpoints."""

from __future__ import annotations

from collections.abc import Mapping

import ibis

from ibis_engine.sql_bridge import SqlIngestSpec, parse_sql_table

SQL_SELECT_ID = "SELECT id FROM events"
SQL_SELECT_FILTER = "SELECT id, label FROM events WHERE id > 1"

AST_SELECT_ID = (
    "[{'c': 'Select'}, {'i': 0, 'k': 'expressions', 'a': True, 'c': 'Column'}, "
    "{'i': 1, 'k': 'this', 'c': 'Identifier', 'm': {'line': 1, 'col': 9, "
    "'start': 7, 'end': 8}}, {'i': 2, 'k': 'this', 'v': 'id'}, "
    "{'i': 2, 'k': 'quoted', 'v': False}, {'i': 0, 'k': 'from_', 'c': 'From'}, "
    "{'i': 5, 'k': 'this', 'c': 'Table'}, {'i': 6, 'k': 'this', 'c': 'Identifier', "
    "'m': {'line': 1, 'col': 21, 'start': 15, 'end': 20}}, {'i': 7, 'k': 'this', "
    "'v': 'events'}, {'i': 7, 'k': 'quoted', 'v': False}]"
)
AST_SELECT_FILTER = (
    "[{'c': 'Select'}, {'i': 0, 'k': 'expressions', 'a': True, 'c': 'Column'}, "
    "{'i': 1, 'k': 'this', 'c': 'Identifier', 'm': {'line': 1, 'col': 9, "
    "'start': 7, 'end': 8}}, {'i': 2, 'k': 'this', 'v': 'id'}, "
    "{'i': 2, 'k': 'quoted', 'v': False}, {'i': 0, 'k': 'expressions', 'a': True, "
    "'c': 'Column'}, {'i': 5, 'k': 'this', 'c': 'Identifier', 'm': {'line': 1, "
    "'col': 16, 'start': 11, 'end': 15}}, {'i': 6, 'k': 'this', 'v': 'label'}, "
    "{'i': 6, 'k': 'quoted', 'v': False}, {'i': 0, 'k': 'from_', 'c': 'From'}, "
    "{'i': 9, 'k': 'this', 'c': 'Table'}, {'i': 10, 'k': 'this', 'c': 'Identifier', "
    "'m': {'line': 1, 'col': 28, 'start': 22, 'end': 27}}, {'i': 11, 'k': 'this', "
    "'v': 'events'}, {'i': 11, 'k': 'quoted', 'v': False}, {'i': 0, 'k': 'where', "
    "'c': 'Where'}, {'i': 14, 'k': 'this', 'c': 'GT'}, {'i': 15, 'k': 'this', "
    "'c': 'Column'}, {'i': 16, 'k': 'this', 'c': 'Identifier', 'm': {'line': 1, "
    "'col': 37, 'start': 35, 'end': 36}}, {'i': 17, 'k': 'this', 'v': 'id'}, "
    "{'i': 17, 'k': 'quoted', 'v': False}, {'i': 15, 'k': 'expression', 'c': "
    "'Literal', 'm': {'line': 1, 'col': 41, 'start': 40, 'end': 40}}, "
    "{'i': 20, 'k': 'this', 'v': '1'}, {'i': 20, 'k': 'is_string', 'v': False}]"
)


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
    """Capture SQLGlot AST for a basic select."""
    payload = _ingest(
        SQL_SELECT_ID,
        schema=ibis.schema({"id": "int64"}),
        catalog_schema=ibis.schema({"id": "int64", "label": "string"}),
    )
    assert payload.get("sqlglot_ast") == AST_SELECT_ID


def test_sql_ingest_golden_select_filter() -> None:
    """Capture SQLGlot AST for a filtered select."""
    payload = _ingest(
        SQL_SELECT_FILTER,
        schema=ibis.schema({"id": "int64", "label": "string"}),
        catalog_schema=ibis.schema({"id": "int64", "label": "string"}),
    )
    assert payload.get("sqlglot_ast") == AST_SELECT_FILTER
