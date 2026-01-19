"""Unit tests for SQL ingestion guardrails."""

from __future__ import annotations

from collections.abc import Mapping

import ibis
import pytest

from ibis_engine.sql_bridge import SqlIngestSpec, parse_sql_table


def test_sql_ingest_requires_schema() -> None:
    """Require explicit schemas for SQL ingestion."""
    spec = SqlIngestSpec(
        sql="SELECT id FROM events", catalog={"events": ibis.schema({"id": "int64"})}
    )
    with pytest.raises(ValueError, match="schema is required"):
        _ = parse_sql_table(spec)


def test_sql_ingest_records_sqlglot_sql() -> None:
    """Capture SQLGlot SQL payloads for SQL ingestion."""
    events_schema = ibis.schema({"id": "int64", "value": "string"})
    spec_schema = ibis.schema({"id": "int64"})
    payloads: list[Mapping[str, object]] = []

    def _hook(payload: Mapping[str, object]) -> None:
        payloads.append(payload)

    spec = SqlIngestSpec(
        sql="SELECT id FROM events",
        catalog={"events": events_schema},
        schema=spec_schema,
        artifacts_hook=_hook,
    )
    _ = parse_sql_table(spec)
    assert payloads
    assert payloads[-1].get("sqlglot_sql") is not None


def test_sql_ingest_reports_parse_errors() -> None:
    """Fail fast on SQL parse errors with artifacts."""
    payloads: list[Mapping[str, object]] = []

    def _hook(payload: Mapping[str, object]) -> None:
        payloads.append(payload)

    spec = SqlIngestSpec(
        sql="SELECT FROM",
        catalog={},
        schema=ibis.schema({"id": "int64"}),
        artifacts_hook=_hook,
    )
    with pytest.raises(ValueError, match="parse failed"):
        _ = parse_sql_table(spec)
    assert payloads
    assert payloads[-1].get("error")
