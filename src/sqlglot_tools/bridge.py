"""Bridge helpers between Ibis expressions and SQLGlot ASTs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from ibis.expr.types import Table as IbisTable
from ibis.expr.types import Value

from sqlglot_tools.compat import Expression, diff
from sqlglot_tools.lineage import (
    referenced_columns,
    referenced_identifiers,
    referenced_tables,
)
from sqlglot_tools.optimizer import (
    DEFAULT_WRITE_DIALECT,
    CanonicalizationRules,
    NormalizeExprOptions,
    SchemaMapping,
    SqlGlotPolicy,
    normalize_expr_with_stats,
)


class SqlGlotCompiler(Protocol):
    """Protocol for backends exposing SQLGlot compilation."""

    def to_sqlglot(
        self,
        expr: IbisTable,
        *,
        params: Mapping[Value, object] | None = None,
    ) -> Expression:
        """Return a SQLGlot expression."""
        ...


class IbisCompilerBackend(Protocol):
    """Protocol for Ibis backends exposing a SQLGlot compiler."""

    compiler: SqlGlotCompiler


@dataclass(frozen=True)
class SqlGlotDiagnostics:
    """AST and SQL metadata for SQLGlot diagnostics."""

    expression: Expression
    optimized: Expression
    tables: tuple[str, ...]
    columns: tuple[str, ...]
    identifiers: tuple[str, ...]
    ast_repr: str
    sql_dialect: str
    sql_text_raw: str
    sql_text_optimized: str
    normalization_distance: int | None
    normalization_max_distance: int | None
    normalization_applied: bool | None


@dataclass(frozen=True)
class SqlGlotRelationDiff:
    """Diff summary for SQLGlot-derived lineage metadata."""

    tables_added: tuple[str, ...]
    tables_removed: tuple[str, ...]
    columns_added: tuple[str, ...]
    columns_removed: tuple[str, ...]
    ast_diff: Mapping[str, int]


@dataclass(frozen=True)
class SqlGlotDiagnosticsOptions:
    """Options for compiling SQLGlot diagnostics."""

    schema_map: SchemaMapping | None = None
    rules: CanonicalizationRules | None = None
    policy: SqlGlotPolicy | None = None
    normalize: bool = True
    params: Mapping[Value, object] | None = None
    sql: str | None = None


def ibis_to_sqlglot(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    params: Mapping[Value, object] | None = None,
) -> Expression:
    """Compile an Ibis expression into SQLGlot.

    Returns
    -------
    sqlglot.Expression
        SQLGlot expression compiled from the Ibis expression.
    """
    return backend.compiler.to_sqlglot(expr, params=params)


def sqlglot_diagnostics(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    options: SqlGlotDiagnosticsOptions | None = None,
) -> SqlGlotDiagnostics:
    """Return AST-derived metadata for an Ibis expression.

    Returns
    -------
    SqlGlotDiagnostics
        Metadata extracted from the SQLGlot AST.
    """
    options = options or SqlGlotDiagnosticsOptions()
    compiled = ibis_to_sqlglot(expr, backend=backend, params=options.params)
    dialect = _resolved_dialect(options.policy)
    stats = None
    if options.normalize:
        normalize_result = normalize_expr_with_stats(
            compiled,
            options=NormalizeExprOptions(
                schema=options.schema_map,
                rules=options.rules,
                policy=options.policy,
                sql=options.sql,
            ),
        )
        optimized = normalize_result.expr
        stats = normalize_result.stats
    else:
        optimized = compiled
    return SqlGlotDiagnostics(
        expression=compiled,
        optimized=optimized,
        tables=referenced_tables(optimized),
        columns=referenced_columns(optimized),
        identifiers=referenced_identifiers(optimized),
        ast_repr=repr(optimized),
        sql_dialect=dialect,
        sql_text_raw=_sql_text(compiled, dialect=dialect),
        sql_text_optimized=_sql_text(optimized, dialect=dialect),
        normalization_distance=stats.distance if stats is not None else None,
        normalization_max_distance=stats.max_distance if stats is not None else None,
        normalization_applied=stats.applied if stats is not None else None,
    )


def relation_diff(
    left: SqlGlotDiagnostics,
    right: SqlGlotDiagnostics,
) -> SqlGlotRelationDiff:
    """Return a diff summary between two SQLGlot diagnostics.

    Returns
    -------
    SqlGlotRelationDiff
        Added and removed table/column references.
    """
    left_tables = set(left.tables)
    right_tables = set(right.tables)
    left_columns = set(left.columns)
    right_columns = set(right.columns)
    ast_diff = _ast_diff_summary(left.expression, right.optimized)
    return SqlGlotRelationDiff(
        tables_added=tuple(sorted(right_tables - left_tables)),
        tables_removed=tuple(sorted(left_tables - right_tables)),
        columns_added=tuple(sorted(right_columns - left_columns)),
        columns_removed=tuple(sorted(left_columns - right_columns)),
        ast_diff=ast_diff,
    )


def _resolved_dialect(policy: SqlGlotPolicy | None) -> str:
    return policy.write_dialect if policy is not None else DEFAULT_WRITE_DIALECT


def _sql_text(expr: Expression, *, dialect: str) -> str:
    return expr.sql(dialect=dialect)


def _ast_diff_summary(left: Expression, right: Expression) -> dict[str, int]:
    summary: dict[str, int] = {}
    for op in diff(left, right):
        key = op.__class__.__name__.lower()
        summary[key] = summary.get(key, 0) + 1
    return summary


def missing_schema_columns(
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
    "SchemaMapping",
    "SqlGlotCompiler",
    "SqlGlotDiagnostics",
    "SqlGlotDiagnosticsOptions",
    "SqlGlotRelationDiff",
    "ibis_to_sqlglot",
    "missing_schema_columns",
    "relation_diff",
    "sqlglot_diagnostics",
]
