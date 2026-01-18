"""SQL ingestion and decompilation helpers for Ibis."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass
from typing import Protocol, cast

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import Table, Value
from sqlglot import Expression
from sqlglot.serde import dump

from arrowdsl.core.interop import RecordBatchReaderLike
from ibis_engine.schema_utils import ibis_schema_from_arrow, validate_expr_schema


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

    Raises
    ------
    ValueError
        Raised when the SQL ingestion schema is missing.
    """
    if spec.schema is None:
        msg = "SqlIngestSpec.schema is required for SQL ingestion."
        raise ValueError(msg)
    expr = ibis.parse_sql(spec.sql, spec.catalog, dialect=spec.dialect)
    expected = spec.schema.to_pyarrow()
    validate_expr_schema(expr, expected=expected)
    return expr


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
    sqlglot_expr: Expression | None = None,
    schema: _SchemaProtocol | None = None,
) -> Table:
    """Execute raw SQL and return an Ibis table.

    Parameters
    ----------
    backend:
        Backend exposing ``raw_sql``.
    sql:
        SQL string used for fallback execution.
    sqlglot_expr:
        SQLGlot expression for raw execution when supported.
    schema:
        Required schema for cursor-based results.

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
    if schema is None:
        msg = "Schema is required for raw SQL execution."
        raise ValueError(msg)
    raw_sql = getattr(backend, "raw_sql", None)
    if not callable(raw_sql):
        msg = "Backend does not expose raw_sql for SQL execution."
        raise TypeError(msg)
    if sqlglot_expr is not None:
        try:
            result = raw_sql(sqlglot_expr)
        except TypeError:
            result = raw_sql(sql)
    else:
        result = raw_sql(sql)
    expected = schema.to_pyarrow()
    if isinstance(result, Table):
        validate_expr_schema(result, expected=expected)
        return result
    table = _table_from_raw_result(result, schema)
    if table is None:
        msg = "Unsupported raw_sql return type."
        raise TypeError(msg)
    _validate_table_schema(table, expected=expected)
    return ibis.memtable(table, schema=ibis_schema_from_arrow(expected))


def _validate_table_schema(table: pa.Table, *, expected: pa.Schema) -> None:
    if table.schema.equals(expected, check_metadata=False):
        return
    msg = "Raw SQL output schema does not match the declared schema."
    raise ValueError(msg)


def _table_from_raw_result(
    result: object,
    schema: _SchemaProtocol,
) -> pa.Table | None:
    to_arrow_table = getattr(result, "to_arrow_table", None)
    table: pa.Table | None = None
    if callable(to_arrow_table):
        table = cast("pa.Table", to_arrow_table())
    elif isinstance(result, RecordBatchReaderLike):
        table = cast("pa.Table", result.read_all())
    elif hasattr(result, "__arrow_c_stream__"):
        table = pa.table(result)
    elif isinstance(result, pa.Table):
        table = result
    elif hasattr(result, "fetch_arrow_table"):
        table = cast("_RawSqlResult", result).fetch_arrow_table()
    elif hasattr(result, "fetchall"):
        with closing(cast("_RawSqlResult", result)) as cursor:
            rows = cursor.fetchall()
        table = _rows_to_table(rows, schema)
    return table


def _rows_to_table(
    rows: Sequence[tuple[object, ...]],
    schema: _SchemaProtocol,
) -> pa.Table:
    arrow_schema = schema.to_pyarrow()
    column_count = len(arrow_schema)
    if not rows:
        arrays = [pa.array([], type=field.type) for field in arrow_schema]
        return pa.Table.from_arrays(arrays, schema=arrow_schema)
    columns: list[list[object]] = [[] for _ in range(column_count)]
    for row in rows:
        if len(row) != column_count:
            msg = "Raw SQL row width does not match the declared schema."
            raise ValueError(msg)
        for idx, value in enumerate(row):
            columns[idx].append(value)
    arrays = [
        pa.array(columns[idx], type=arrow_schema[idx].type) for idx in range(column_count)
    ]
    return pa.Table.from_arrays(arrays, schema=arrow_schema)


__all__ = [
    "SqlIngestArtifacts",
    "SqlIngestSpec",
    "decompile_expr",
    "execute_raw_sql",
    "parse_sql_table",
    "sql_ingest_artifacts",
]
