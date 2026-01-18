"""SQL ingestion and decompilation helpers for Ibis."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass
from typing import Protocol, cast

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import Table, Value
from sqlglot import Expression
from sqlglot.serde import dump


class _RawSqlResult(Protocol):
    def fetch_arrow_table(self) -> pa.Table:
        """Return an Arrow table result."""
        ...

    def fetchall(self) -> list[tuple[object, ...]]:
        """Return raw row tuples."""
        ...

    def close(self) -> None:
        """Release backend resources."""
        ...


class _SchemaProtocol(Protocol):
    def to_pyarrow(self) -> pa.Schema:
        """Return an equivalent PyArrow schema."""
        ...


@dataclass(frozen=True)
class SqlIngestSpec:
    """Specification for SQL ingestion into Ibis."""

    sql: str
    catalog: BaseBackend
    schema: _SchemaProtocol | None = None
    dialect: str | None = None


@dataclass(frozen=True)
class SqlIngestArtifacts:
    """Artifacts describing SQL ingestion round-trips."""

    sql: str
    decompiled_sql: str
    schema: Mapping[str, str] | None
    dialect: str | None = None
    sqlglot_ast: Sequence[Mapping[str, object]] | None = None

    def payload(self) -> dict[str, object]:
        """Return a JSON-friendly payload for diagnostics.

        Returns
        -------
        dict[str, object]
            JSON-ready SQL ingest artifacts payload.
        """
        return {
            "sql": self.sql,
            "decompiled_sql": self.decompiled_sql,
            "schema": dict(self.schema) if self.schema is not None else None,
            "dialect": self.dialect,
            "sqlglot_ast": list(self.sqlglot_ast) if self.sqlglot_ast is not None else None,
        }


def parse_sql_table(spec: SqlIngestSpec) -> Table:
    """Parse a SQL statement into an Ibis table expression.

    Returns
    -------
    ibis.expr.types.Table
        Parsed Ibis table expression.
    """
    return ibis.parse_sql(spec.sql, spec.catalog, dialect=spec.dialect)


def decompile_expr(expr: Table | Value) -> str:
    """Return a decompiled Ibis expression string.

    Returns
    -------
    str
        Decompiled Ibis expression source.
    """
    return ibis.decompile(expr)


def sql_ingest_artifacts(
    sql: str,
    *,
    expr: Table | Value,
    sqlglot_expr: Expression | None = None,
    dialect: str | None = None,
) -> SqlIngestArtifacts:
    """Return round-trip artifacts for SQL ingestion.

    Returns
    -------
    SqlIngestArtifacts
        Diagnostics artifacts for the SQL ingestion step.
    """
    decompiled_sql = decompile_expr(expr)
    schema = None
    if isinstance(expr, Table):
        expr_schema = expr.schema().to_pyarrow()
        schema = {field.name: str(field.type) for field in expr_schema}
    sqlglot_ast = dump(sqlglot_expr) if sqlglot_expr is not None else None
    return SqlIngestArtifacts(
        sql=sql,
        decompiled_sql=decompiled_sql,
        schema=schema,
        dialect=dialect,
        sqlglot_ast=sqlglot_ast,
    )


def execute_raw_sql(
    backend: object,
    *,
    sql: str,
    schema: _SchemaProtocol | None = None,
) -> Table:
    """Execute raw SQL and return an Ibis table.

    Returns
    -------
    ibis.expr.types.Table
        Ibis table expression representing the raw SQL results.

    Raises
    ------
    TypeError
        Raised when the backend lacks raw_sql or returns an unsupported result.
    ValueError
        Raised when schema is required to materialize cursor results.
    """
    raw_sql = getattr(backend, "raw_sql", None)
    if not callable(raw_sql):
        msg = "Backend does not expose raw_sql for SQL execution."
        raise TypeError(msg)
    result = raw_sql(sql)
    if hasattr(result, "schema") and hasattr(result, "to_pyarrow"):
        return cast("Table", result)
    if hasattr(result, "fetch_arrow_table"):
        table = cast("_RawSqlResult", result).fetch_arrow_table()
        return ibis.memtable(table)
    if schema is None:
        msg = "Schema is required to materialize raw SQL cursor results."
        raise ValueError(msg)
    if hasattr(result, "fetchall"):
        with closing(cast("_RawSqlResult", result)) as cursor:
            rows = cursor.fetchall()
        payload = _rows_to_pylist(rows, schema)
        return ibis.memtable(pa.Table.from_pylist(payload, schema=schema.to_pyarrow()))
    msg = "Unsupported raw_sql return type."
    raise TypeError(msg)


def _rows_to_pylist(
    rows: Iterable[tuple[object, ...]],
    schema: _SchemaProtocol,
) -> list[dict[str, object]]:
    names = list(schema.to_pyarrow().names)
    return [dict(zip(names, row, strict=False)) for row in rows]


__all__ = [
    "SqlIngestArtifacts",
    "SqlIngestSpec",
    "decompile_expr",
    "execute_raw_sql",
    "parse_sql_table",
    "sql_ingest_artifacts",
]
