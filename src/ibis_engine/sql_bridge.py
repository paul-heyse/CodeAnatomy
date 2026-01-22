"""SQL ingestion and decompilation helpers for Ibis."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Protocol

import ibis
import pyarrow as pa
from ibis.expr.types import Table, Value
from sqlglot.errors import ParseError
from sqlglot.serde import dump

from ibis_engine.schema_utils import validate_expr_schema
from sqlglot_tools.bridge import IbisCompilerBackend, ibis_to_sqlglot
from sqlglot_tools.compat import Expression
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    SchemaMapping,
    SqlGlotPolicy,
    default_sqlglot_policy,
    normalize_expr,
    parse_sql_strict,
    sqlglot_policy_snapshot_for,
    sqlglot_sql,
    transpile_sql,
)


class _SchemaProtocol(Protocol):
    def to_pyarrow(self) -> pa.Schema:
        """Return an equivalent PyArrow schema."""
        ...


@dataclass(frozen=True)
class SqlIngestSpec:
    """Specification for SQL ingestion into Ibis."""

    sql: str
    catalog: Mapping[str, _SchemaProtocol]
    schema: _SchemaProtocol | None = None
    dialect: str | None = None
    backend: IbisCompilerBackend | None = None
    artifacts_hook: Callable[[Mapping[str, object]], None] | None = None
    sqlglot_expr: Expression | None = None


@dataclass(frozen=True)
class SqlIngestArtifacts:
    """Artifacts describing SQL ingestion round-trips."""

    sql: str
    decompiled_sql: str
    schema: Mapping[str, str] | None
    dialect: str | None = None
    sqlglot_sql: str | None = None
    normalized_sql: str | None = None
    sqlglot_ast: object | None = None
    ibis_sqlglot_ast: object | None = None
    sqlglot_policy_hash: str | None = None
    sqlglot_policy_snapshot: Mapping[str, object] | None = None

    def payload(self) -> dict[str, object]:
        """Return a payload for diagnostics.

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
            "sqlglot_sql": self.sqlglot_sql,
            "normalized_sql": self.normalized_sql,
            "sqlglot_ast": self.sqlglot_ast,
            "ibis_sqlglot_ast": self.ibis_sqlglot_ast,
            "sqlglot_policy_hash": self.sqlglot_policy_hash,
            "sqlglot_policy_snapshot": (
                dict(self.sqlglot_policy_snapshot)
                if self.sqlglot_policy_snapshot is not None
                else None
            ),
        }


@dataclass(frozen=True)
class SqlIngestSqlGlotContext:
    """SQLGlot metadata captured during SQL ingestion."""

    sqlglot_expr: Expression | None = None
    ibis_sqlglot_expr: Expression | None = None
    normalized_sql: str | None = None
    policy_hash: str | None = None
    policy_snapshot: Mapping[str, object] | None = None
    dialect: str | None = None


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
    catalog = _catalog_schemas(spec.catalog)
    schema_map = _schema_map_from_catalog(catalog)
    policy = _sqlglot_policy_for_spec(spec)
    policy_snapshot = sqlglot_policy_snapshot_for(policy)
    policy_hash = policy_snapshot.policy_hash
    context = SqlIngestSqlGlotContext(
        policy_hash=policy_hash,
        policy_snapshot=policy_snapshot.payload(),
        dialect=policy.write_dialect,
    )
    try:
        normalized_expr = _resolve_ingest_expr(
            spec,
            schema_map=schema_map,
            policy=policy,
        )
    except (ParseError, TypeError, ValueError) as exc:
        _emit_sql_ingest_failure(spec, error=exc, context=context)
        msg = f"SQLGlot normalization failed: {exc}"
        raise ValueError(msg) from exc
    normalized_sql = sqlglot_sql(normalized_expr, policy=policy)
    context = replace(context, sqlglot_expr=normalized_expr, normalized_sql=normalized_sql)
    try:
        expr = ibis.parse_sql(normalized_sql, catalog, dialect=policy.write_dialect)
    except (TypeError, ValueError) as exc:
        _emit_sql_ingest_failure(
            spec,
            error=exc,
            context=context,
        )
        msg = f"SQL ingestion failed: {exc}"
        raise ValueError(msg) from exc
    expected = spec.schema.to_pyarrow()
    try:
        validate_expr_schema(expr, expected=expected)
    except ValueError as exc:
        _emit_sql_ingest_failure(
            spec,
            error=exc,
            context=context,
        )
        msg = f"SQL ingestion schema mismatch: {exc}"
        raise ValueError(msg) from exc
    if spec.backend is not None:
        try:
            ibis_expr = ibis_to_sqlglot(expr, backend=spec.backend, params=None)
        except (TypeError, ValueError):
            ibis_expr = None
        context = replace(context, ibis_sqlglot_expr=ibis_expr)
    if spec.artifacts_hook is not None:
        spec.artifacts_hook(
            sql_ingest_artifacts(
                spec.sql,
                expr=expr,
                context=context,
            ).payload()
        )
    return expr


def _catalog_schemas(
    catalog: Mapping[str, _SchemaProtocol],
) -> Mapping[str, _SchemaProtocol]:
    if isinstance(catalog, Mapping):
        return catalog
    msg = "SQL ingestion catalog must be a mapping of table schemas."
    raise TypeError(msg)


def _normalize_ingest_expr(
    sql: str,
    *,
    schema_map: SchemaMapping,
    policy: SqlGlotPolicy,
) -> Expression:
    transpiled_sql = transpile_sql(sql, policy=policy)
    expr = parse_sql_strict(
        transpiled_sql,
        dialect=policy.write_dialect,
        error_level=policy.error_level,
    )
    return normalize_expr(
        expr,
        options=NormalizeExprOptions(
            schema=schema_map,
            policy=policy,
            sql=transpiled_sql,
        ),
    )


def _normalize_ingest_expr_from_expr(
    expr: Expression,
    *,
    schema_map: SchemaMapping,
    policy: SqlGlotPolicy,
) -> Expression:
    return normalize_expr(
        expr.copy(),
        options=NormalizeExprOptions(
            schema=schema_map,
            policy=policy,
        ),
    )


def _resolve_ingest_expr(
    spec: SqlIngestSpec,
    *,
    schema_map: SchemaMapping,
    policy: SqlGlotPolicy,
) -> Expression:
    if spec.sqlglot_expr is not None:
        return _normalize_ingest_expr_from_expr(
            spec.sqlglot_expr,
            schema_map=schema_map,
            policy=policy,
        )
    return _normalize_ingest_expr(
        spec.sql,
        schema_map=schema_map,
        policy=policy,
    )


def _emit_sql_ingest_failure(
    spec: SqlIngestSpec,
    *,
    error: Exception,
    context: SqlIngestSqlGlotContext,
) -> None:
    if spec.artifacts_hook is None:
        return
    dialect = context.dialect or spec.dialect
    payload = {
        "sql": spec.sql,
        "dialect": spec.dialect,
        "error": str(error),
        "sqlglot_sql": _sql_text(context.sqlglot_expr, dialect=dialect),
        "normalized_sql": context.normalized_sql,
        "sqlglot_ast": _sqlglot_ast_payload(context.sqlglot_expr),
        "sqlglot_policy_hash": context.policy_hash,
        "sqlglot_policy_snapshot": (
            dict(context.policy_snapshot) if context.policy_snapshot is not None else None
        ),
    }
    spec.artifacts_hook(payload)


def _schema_map_from_catalog(
    catalog: Mapping[str, _SchemaProtocol],
) -> Mapping[str, Mapping[str, str]]:
    mapping: dict[str, dict[str, str]] = {}
    for name, schema in catalog.items():
        arrow_schema = schema.to_pyarrow()
        mapping[name] = {field.name: str(field.type) for field in arrow_schema}
    return mapping


def _sqlglot_policy_for_spec(spec: SqlIngestSpec) -> SqlGlotPolicy:
    policy = default_sqlglot_policy()
    if spec.dialect is None:
        return replace(policy, validate_qualify_columns=True, identify=True)
    return replace(
        policy,
        read_dialect=spec.dialect,
        write_dialect=spec.dialect,
        validate_qualify_columns=True,
        identify=True,
    )


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
    context: SqlIngestSqlGlotContext | None = None,
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
    context = context or SqlIngestSqlGlotContext()
    return SqlIngestArtifacts(
        sql=sql,
        decompiled_sql=decompiled_sql,
        schema=schema,
        dialect=context.dialect,
        sqlglot_sql=_sql_text(context.sqlglot_expr, dialect=context.dialect),
        normalized_sql=context.normalized_sql,
        sqlglot_ast=_sqlglot_ast_payload(context.sqlglot_expr),
        ibis_sqlglot_ast=_sqlglot_ast_payload(context.ibis_sqlglot_expr),
        sqlglot_policy_hash=context.policy_hash,
        sqlglot_policy_snapshot=context.policy_snapshot,
    )


def _sql_text(expr: Expression | None, *, dialect: str | None) -> str | None:
    if expr is None:
        return None
    try:
        return expr.sql(dialect=dialect)
    except (TypeError, ValueError):
        return str(expr)


def _sqlglot_ast_payload(expr: Expression | None) -> object | None:
    if expr is None:
        return None
    try:
        return dump(expr)
    except (TypeError, ValueError):
        return None


__all__ = [
    "SqlIngestArtifacts",
    "SqlIngestSpec",
    "decompile_expr",
    "parse_sql_table",
    "sql_ingest_artifacts",
]
