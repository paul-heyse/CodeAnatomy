"""SQL ingestion and decompilation helpers for Ibis."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Literal, Protocol, cast

import ibis
import pyarrow as pa
from ibis.expr.types import Expr, Table, Value
from sqlglot.errors import ParseError

from ibis_engine.schema_utils import ibis_schema_from_arrow, validate_expr_schema
from sqlglot_tools.bridge import IbisCompilerBackend, ibis_to_sqlglot, sqlglot_ast_payload
from sqlglot_tools.compat import Expression
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    SchemaMapping,
    SqlGlotPolicy,
    normalize_expr,
    parse_error_payload,
    parse_sql_strict,
    resolve_sqlglot_policy,
    sqlglot_policy_snapshot_for,
    sqlglot_sql,
    transpile_sql,
)


class _SchemaProtocol(Protocol):
    def to_pyarrow(self) -> pa.Schema:
        """Return an equivalent PyArrow schema."""
        ...


SqlIngestKind = Literal["parse_sql", "backend_sql", "table_sql"]


@dataclass(frozen=True)
class SqlIngestSpec:
    """Specification for SQL ingestion into Ibis."""

    sql: str
    catalog: Mapping[str, _SchemaProtocol]
    schema: _SchemaProtocol | None = None
    dialect: str | None = None
    ingest_kind: SqlIngestKind = "parse_sql"
    table: Table | None = None
    source_name: str | None = None
    backend: IbisCompilerBackend | None = None
    artifacts_hook: Callable[[Mapping[str, object]], None] | None = None
    sqlglot_expr: Expression | None = None


@dataclass(frozen=True)
class SqlIngestArtifacts:
    """Artifacts describing SQL ingestion round-trips."""

    event_time_unix_ms: int
    ingest_kind: SqlIngestKind
    source_name: str | None
    sql: str
    decompiled_sql: str
    schema: Mapping[str, str] | None
    dialect: str | None = None
    sqlglot_sql: str | None = None
    normalized_sql: str | None = None
    sqlglot_ast: bytes | None = None
    ibis_sqlglot_ast: bytes | None = None
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
            "event_time_unix_ms": self.event_time_unix_ms,
            "ingest_kind": self.ingest_kind,
            "source_name": self.source_name,
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

    event_time_unix_ms: int | None = None
    ingest_kind: SqlIngestKind | None = None
    source_name: str | None = None
    sqlglot_expr: Expression | None = None
    ibis_sqlglot_expr: Expression | None = None
    normalized_sql: str | None = None
    policy_hash: str | None = None
    policy_snapshot: Mapping[str, object] | None = None
    dialect: str | None = None
    policy: SqlGlotPolicy | None = None


def parse_sql_table(spec: SqlIngestSpec) -> Table:
    """Parse a SQL statement into an Ibis table expression.

    Returns
    -------
    ibis.expr.types.Table
        Parsed Ibis table expression.
    """
    if spec.ingest_kind != "parse_sql":
        spec = replace(spec, ingest_kind="parse_sql")
    return sql_to_ibis_expr(spec)


def backend_sql_table(spec: SqlIngestSpec) -> Table:
    """Build an Ibis table via Backend.sql ingestion.

    Returns
    -------
    ibis.expr.types.Table
        Ibis table expression produced by Backend.sql.
    """
    if spec.ingest_kind != "backend_sql":
        spec = replace(spec, ingest_kind="backend_sql")
    return sql_to_ibis_expr(spec)


def table_sql_table(spec: SqlIngestSpec) -> Table:
    """Build an Ibis table via Table.sql ingestion.

    Returns
    -------
    ibis.expr.types.Table
        Ibis table expression produced by Table.sql.
    """
    if spec.ingest_kind != "table_sql":
        spec = replace(spec, ingest_kind="table_sql")
    return sql_to_ibis_expr(spec)


def sql_to_ibis_expr(spec: SqlIngestSpec) -> Table:
    """Ingest SQL into an Ibis table expression.

    Returns
    -------
    ibis.expr.types.Table
        Ibis table expression produced by the ingestion pipeline.

    Raises
    ------
    ValueError
        Raised when SQL parsing, ingestion, or schema validation fails.
    """
    _validate_ingest_spec(spec)
    event_time = _sql_ingest_event_time()
    source_name = _resolve_source_name(spec)
    catalog = _catalog_schemas(spec.catalog)
    schema_map = _schema_map_from_catalog(catalog)
    policy = _sqlglot_policy_for_spec(spec)
    policy_snapshot = sqlglot_policy_snapshot_for(policy)
    context = SqlIngestSqlGlotContext(
        event_time_unix_ms=event_time,
        ingest_kind=spec.ingest_kind,
        source_name=source_name,
        policy_hash=policy_snapshot.policy_hash,
        policy_snapshot=policy_snapshot.payload(),
        dialect=policy.write_dialect,
        policy=policy,
    )
    try:
        normalized_expr = _resolve_ingest_expr(
            spec,
            schema_map=schema_map,
            policy=policy,
            source_name=source_name,
        )
    except (ParseError, TypeError, ValueError) as exc:
        _emit_sql_ingest_failure(spec, error=exc, context=context)
        msg = f"SQLGlot normalization failed: {exc}"
        raise ValueError(msg) from exc
    normalized_sql = sqlglot_sql(normalized_expr, policy=policy)
    context = replace(context, sqlglot_expr=normalized_expr, normalized_sql=normalized_sql)
    try:
        expr = _ingest_sql_expr(
            spec,
            normalized_sql=normalized_sql,
            catalog=catalog,
            policy=policy,
        )
    except (TypeError, ValueError) as exc:
        _emit_sql_ingest_failure(spec, error=exc, context=context)
        msg = f"SQL ingestion failed: {exc}"
        raise ValueError(msg) from exc
    if spec.schema is not None:
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


def _validate_ingest_spec(spec: SqlIngestSpec) -> None:
    if spec.ingest_kind == "parse_sql" and spec.schema is None:
        msg = "SqlIngestSpec.schema is required for SQL ingestion."
        raise ValueError(msg)
    if spec.ingest_kind == "backend_sql" and spec.backend is None:
        msg = "SqlIngestSpec.backend is required for backend SQL ingestion."
        raise ValueError(msg)
    if spec.ingest_kind == "table_sql" and spec.table is None:
        msg = "SqlIngestSpec.table is required for table SQL ingestion."
        raise ValueError(msg)


def _sql_ingest_event_time() -> int:
    return int(time.time() * 1000)


def _resolve_source_name(spec: SqlIngestSpec) -> str | None:
    if spec.source_name is not None:
        return spec.source_name
    if spec.table is None:
        return None
    getter = getattr(spec.table, "get_name", None)
    if callable(getter):
        try:
            name = getter()
            if name is None:
                return None
            return str(name)
        except (AttributeError, TypeError, ValueError):
            return None
    return None


def _table_sql_text(spec: SqlIngestSpec, *, normalized_sql: str) -> str:
    if "{self}" in spec.sql:
        return spec.sql
    return normalized_sql


def _backend_sql_expr(
    spec: SqlIngestSpec,
    *,
    normalized_sql: str,
    policy: SqlGlotPolicy,
) -> Table:
    backend = spec.backend
    if backend is None:
        msg = "SqlIngestSpec.backend is required for backend SQL ingestion."
        raise ValueError(msg)
    ibis_schema = None
    if spec.schema is not None:
        ibis_schema = ibis_schema_from_arrow(spec.schema.to_pyarrow())
    return backend.sql(normalized_sql, schema=ibis_schema, dialect=policy.write_dialect)


def _table_sql_expr(
    spec: SqlIngestSpec,
    *,
    normalized_sql: str,
    policy: SqlGlotPolicy,
) -> Table:
    table = spec.table
    if table is None:
        msg = "SqlIngestSpec.table is required for table SQL ingestion."
        raise ValueError(msg)
    sql_text = _table_sql_text(spec, normalized_sql=normalized_sql)
    return table.sql(sql_text, dialect=policy.write_dialect)


def _ingest_sql_expr(
    spec: SqlIngestSpec,
    *,
    normalized_sql: str,
    catalog: Mapping[str, _SchemaProtocol],
    policy: SqlGlotPolicy,
) -> Table:
    if spec.ingest_kind == "parse_sql":
        return ibis.parse_sql(normalized_sql, catalog, dialect=policy.write_dialect)
    if spec.ingest_kind == "backend_sql":
        return _backend_sql_expr(spec, normalized_sql=normalized_sql, policy=policy)
    if spec.ingest_kind == "table_sql":
        return _table_sql_expr(spec, normalized_sql=normalized_sql, policy=policy)
    msg = f"Unsupported SQL ingest kind: {spec.ingest_kind}"
    raise ValueError(msg)


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


def _sql_for_normalization(spec: SqlIngestSpec, *, source_name: str | None) -> str:
    if spec.ingest_kind != "table_sql":
        return spec.sql
    if "{self}" not in spec.sql:
        return spec.sql
    placeholder = source_name or "self"
    return spec.sql.replace("{self}", placeholder)


def _resolve_ingest_expr(
    spec: SqlIngestSpec,
    *,
    schema_map: SchemaMapping,
    policy: SqlGlotPolicy,
    source_name: str | None,
) -> Expression:
    if spec.sqlglot_expr is not None:
        return _normalize_ingest_expr_from_expr(
            spec.sqlglot_expr,
            schema_map=schema_map,
            policy=policy,
        )
    sql_text = _sql_for_normalization(spec, source_name=source_name)
    return _normalize_ingest_expr(
        sql_text,
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
    schema_payload = None
    if spec.schema is not None:
        schema_payload = {field.name: str(field.type) for field in spec.schema.to_pyarrow()}
    event_time = context.event_time_unix_ms or _sql_ingest_event_time()
    dialect = context.dialect or spec.dialect
    ingest_kind = context.ingest_kind if context.ingest_kind is not None else spec.ingest_kind
    parse_errors = parse_error_payload(error) if isinstance(error, ParseError) else None
    payload: dict[str, object] = {
        "event_time_unix_ms": event_time,
        "ingest_kind": ingest_kind,
        "source_name": context.source_name or spec.source_name,
        "sql": spec.sql,
        "decompiled_sql": None,
        "schema": schema_payload,
        "dialect": dialect,
        "error": str(error),
        "parse_errors": parse_errors,
        "sqlglot_sql": _sql_text(context.sqlglot_expr, dialect=dialect),
        "normalized_sql": context.normalized_sql,
        "sqlglot_ast": sqlglot_ast_payload(context.sqlglot_expr, policy=context.policy),
        "ibis_sqlglot_ast": None,
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
    """Return a resolved SQLGlot policy for a SQL ingest specification.

    Notes
    -----
    Uses ``resolve_sqlglot_policy`` and applies dialect overrides when provided.

    Returns
    -------
    SqlGlotPolicy
        Resolved SQLGlot policy configured for the ingestion spec.
    """
    policy = resolve_sqlglot_policy(name="datafusion_compile")
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


def _hash_sql(sql: str) -> str:
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def _safe_decompile_expr(expr: Expr) -> str:
    try:
        return ibis.decompile(expr)
    except (AttributeError, NotImplementedError, RuntimeError, TypeError, ValueError) as exc:
        return f"ERROR: {exc}"


def _safe_sql(expr: Expr, *, dialect: str | None, pretty: bool) -> str:
    try:
        return expr.to_sql(dialect=dialect, pretty=pretty)
    except (NotImplementedError, RuntimeError, TypeError, ValueError) as exc:
        return f"ERROR: {exc}"


def _compile_params_for_expr(
    params: Mapping[Value, object] | Mapping[str, object] | None,
) -> Mapping[Value, object] | None:
    if not params:
        return None
    if all(isinstance(key, Value) for key in params):
        return cast("Mapping[Value, object]", params)
    return None


def _safe_compile_expr(
    expr: Expr,
    *,
    params: Mapping[Value, object] | Mapping[str, object] | None,
    limit: int | None,
) -> tuple[str | None, str | None]:
    compile_params = _compile_params_for_expr(params)
    try:
        compiled_value = expr.compile(params=compile_params, limit=limit)
    except (AttributeError, NotImplementedError, RuntimeError, TypeError, ValueError) as exc:
        return f"ERROR: {exc}", None
    if compiled_value is None:
        return None, None
    if not isinstance(compiled_value, str):
        compiled_value = str(compiled_value)
    if compiled_value.startswith("ERROR: "):
        return compiled_value, None
    return compiled_value, _hash_sql(compiled_value)


def _compile_params_payload(
    params: Mapping[Value, object] | Mapping[str, object] | None,
) -> str | None:
    if not params:
        return None
    payload = {str(key): value for key, value in params.items()}
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, default=str)


def _compile_limit(limit: int | None) -> int | None:
    if limit is None or isinstance(limit, bool):
        return None
    return limit


def ibis_plan_artifacts(
    expr: Expr,
    *,
    dialect: str | None = None,
    params: Mapping[Value, object] | Mapping[str, object] | None = None,
    limit: int | None = None,
) -> dict[str, object]:
    """Return Ibis-level artifacts for diagnostics.

    Returns
    -------
    dict[str, object]
        Ibis decompile, SQL, compile, and optional graphviz artifacts.
    """
    decompile = _safe_decompile_expr(expr)
    sql = _safe_sql(expr, dialect=dialect, pretty=False)
    sql_pretty = _safe_sql(expr, dialect=dialect, pretty=True)
    compiled_sql, compiled_hash = _safe_compile_expr(expr, params=params, limit=limit)
    compile_params_payload = _compile_params_payload(params)
    compile_limit = _compile_limit(limit)
    graphviz: str | None = None
    try:
        from ibis.expr.visualize import to_graph
    except (ImportError, ModuleNotFoundError):
        graphviz = None
    else:
        try:
            graph = to_graph(expr)
            graphviz = graph.source if hasattr(graph, "source") else str(graph)
        except (NotImplementedError, RuntimeError, TypeError, ValueError) as exc:
            graphviz = f"ERROR: {exc}"
    return {
        "ibis_decompile": decompile,
        "ibis_sql": sql,
        "ibis_sql_pretty": sql_pretty,
        "ibis_graphviz": graphviz,
        "ibis_compiled_sql": compiled_sql,
        "ibis_compiled_sql_hash": compiled_hash,
        "ibis_compile_params": compile_params_payload,
        "ibis_compile_limit": compile_limit,
    }


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
    event_time = context.event_time_unix_ms or _sql_ingest_event_time()
    ingest_kind = context.ingest_kind if context.ingest_kind is not None else "parse_sql"
    return SqlIngestArtifacts(
        event_time_unix_ms=event_time,
        ingest_kind=ingest_kind,
        source_name=context.source_name,
        sql=sql,
        decompiled_sql=decompiled_sql,
        schema=schema,
        dialect=context.dialect,
        sqlglot_sql=_sql_text(context.sqlglot_expr, dialect=context.dialect),
        normalized_sql=context.normalized_sql,
        sqlglot_ast=sqlglot_ast_payload(context.sqlglot_expr, policy=context.policy),
        ibis_sqlglot_ast=sqlglot_ast_payload(context.ibis_sqlglot_expr, policy=context.policy),
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


__all__ = [
    "SqlIngestArtifacts",
    "SqlIngestKind",
    "SqlIngestSpec",
    "SqlIngestSqlGlotContext",
    "backend_sql_table",
    "decompile_expr",
    "ibis_plan_artifacts",
    "parse_sql_table",
    "sql_ingest_artifacts",
    "sql_to_ibis_expr",
    "table_sql_table",
]
