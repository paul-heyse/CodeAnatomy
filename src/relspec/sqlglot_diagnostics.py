"""SQLGlot diagnostics helpers for relspec plans."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from ibis.expr.types import Table as IbisTable
from sqlglot import ErrorLevel, Expression, exp

from sqlglot_tools.optimizer import CanonicalizationRules, normalize_expr


class SqlGlotCompiler(Protocol):
    """Protocol for Ibis backends exposing a SQLGlot compiler."""

    def to_sqlglot(self, expr: IbisTable) -> Expression:
        """Return a SQLGlot expression."""
        ...


class IbisCompilerBackend(Protocol):
    """Protocol for backends exposing a SQLGlot compiler."""

    compiler: SqlGlotCompiler


@dataclass(frozen=True)
class SqlGlotMetadata:
    """SQLGlot-derived metadata for plan diagnostics."""

    raw_sql: str
    optimized_sql: str
    tables: tuple[str, ...]
    columns: tuple[str, ...]


def compile_sqlglot(expr: IbisTable, *, backend: IbisCompilerBackend) -> Expression:
    """Compile an Ibis expression into SQLGlot.

    Returns
    -------
    sqlglot.Expression
        SQLGlot expression compiled from the Ibis expression.
    """
    return backend.compiler.to_sqlglot(expr)


def sqlglot_metadata(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    schema_map: Mapping[str, Mapping[str, str]] | None = None,
    rules: CanonicalizationRules | None = None,
    strict: bool = True,
) -> SqlGlotMetadata:
    """Return SQLGlot-derived metadata for an Ibis expression.

    Returns
    -------
    SqlGlotMetadata
        Extracted metadata including referenced tables and columns.
    """
    sg_expr = compile_sqlglot(expr, backend=backend)
    schema = None
    if schema_map is not None:
        schema = {name: dict(columns) for name, columns in schema_map.items()}
    optimized = normalize_expr(sg_expr, schema=schema, rules=rules)
    tables = _extract_tables(optimized)
    columns = _extract_columns(optimized)
    opts = {"unsupported_level": ErrorLevel.RAISE} if strict else {}
    return SqlGlotMetadata(
        raw_sql=sg_expr.sql(**opts),
        optimized_sql=optimized.sql(**opts),
        tables=tables,
        columns=columns,
    )


def _extract_tables(expr: Expression) -> tuple[str, ...]:
    tables = {table.name for table in expr.find_all(exp.Table)}
    return tuple(sorted(tables))


def _extract_columns(expr: Expression) -> tuple[str, ...]:
    columns = {_column_label(col) for col in expr.find_all(exp.Column)}
    return tuple(sorted(col for col in columns if col))


def _column_label(column: exp.Column) -> str:
    if column.table:
        return f"{column.table}.{column.name}"
    return column.name


def only_schema_columns(
    columns: Sequence[str],
    *,
    schema: Sequence[str],
) -> tuple[str, ...]:
    """Return column names that are not present in a schema.

    Returns
    -------
    tuple[str, ...]
        Missing column names in sorted order.
    """
    available = set(schema)
    missing = sorted(name for name in columns if name.split(".", maxsplit=1)[-1] not in available)
    return tuple(missing)


__all__ = [
    "IbisCompilerBackend",
    "SqlGlotCompiler",
    "SqlGlotMetadata",
    "compile_sqlglot",
    "only_schema_columns",
    "sqlglot_metadata",
]
