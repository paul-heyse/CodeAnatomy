"""Bridge helpers between Ibis expressions and SQLGlot ASTs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

import msgspec
from ibis.expr.types import Table as IbisTable
from ibis.expr.types import Value
from sqlglot.schema import MappingSchema

from datafusion_engine.sql_policy_engine import SQLPolicyProfile, compile_sql_policy
from sqlglot_tools.compat import Expression, diff
from sqlglot_tools.lineage import (
    LineageExtractionOptions,
    LineagePayload,
    extract_lineage_payload,
)
from sqlglot_tools.optimizer import (
    DEFAULT_WRITE_DIALECT,
    CanonicalizationRules,
    SchemaMapping,
    SqlGlotPolicy,
    _flatten_schema_mapping,
    ast_policy_fingerprint,
    ast_to_artifact,
    canonical_ast_fingerprint,
    resolve_sqlglot_policy,
    serialize_ast_artifact,
    sqlglot_policy_snapshot_for,
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

    def sql(
        self,
        sql: str,
        *,
        schema: object | None = None,
        dialect: str | None = None,
    ) -> IbisTable:
        """Return a table expression from a SQL string."""
        ...


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


@dataclass(frozen=True)
class SqlGlotPlanArtifacts:
    """Plan-level SQLGlot artifacts for diagnostics."""

    diagnostics: SqlGlotDiagnostics
    plan_hash: str
    policy_hash: str
    policy_rules_hash: str
    schema_map_hash: str | None
    lineage: LineagePayload | None
    canonical_fingerprint: str | None
    sqlglot_ast: bytes | None


@dataclass(frozen=True)
class SqlGlotPlanOptions:
    """Options for SQLGlot plan artifact collection."""

    schema_map: SchemaMapping | None = None
    schema_map_hash: str | None = None
    policy: SqlGlotPolicy | None = None
    policy_name: str = "datafusion_compile"
    diagnostics: SqlGlotDiagnosticsOptions | None = None


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
    from sqlglot_tools.lineage import (
        referenced_columns,
        referenced_identifiers,
        referenced_tables,
    )

    options = options or SqlGlotDiagnosticsOptions()
    compiled = ibis_to_sqlglot(expr, backend=backend, params=options.params)
    resolved_policy = resolve_sqlglot_policy(name="datafusion_compile", policy=options.policy)
    dialect = _resolved_dialect(resolved_policy)
    stats = None
    if options.normalize:
        compile_schema: SchemaMapping | MappingSchema
        compile_schema = (
            MappingSchema({}) if options.schema_map is None else options.schema_map
        )
        profile = SQLPolicyProfile(
            policy=resolved_policy,
            read_dialect=resolved_policy.read_dialect,
            write_dialect=resolved_policy.write_dialect,
            optimizer_rules=options.rules,
        )
        optimized, artifacts = compile_sql_policy(
            compiled,
            schema=compile_schema,
            profile=profile,
            original_sql=options.sql,
        )
        stats = artifacts
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
        normalization_distance=stats.normalization_distance if stats is not None else None,
        normalization_max_distance=(
            resolved_policy.normalization_distance if stats is not None else None
        ),
        normalization_applied=None if stats is None else not stats.normalization_skipped,
    )


def collect_sqlglot_plan_artifacts(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    options: SqlGlotPlanOptions | None = None,
) -> SqlGlotPlanArtifacts:
    """Collect SQLGlot diagnostics, hashes, and lineage for an Ibis expression.

    Returns
    -------
    SqlGlotPlanArtifacts
        Plan artifacts derived from SQLGlot compilation and optimization.
    """
    resolved_options = options or SqlGlotPlanOptions()
    resolved_policy = resolved_options.policy or resolve_sqlglot_policy(
        name=resolved_options.policy_name
    )
    snapshot = sqlglot_policy_snapshot_for(resolved_policy)
    diagnostics_options = resolved_options.diagnostics or SqlGlotDiagnosticsOptions()
    diagnostics = sqlglot_diagnostics(
        expr,
        backend=backend,
        options=SqlGlotDiagnosticsOptions(
            schema_map=resolved_options.schema_map,
            rules=diagnostics_options.rules,
            policy=resolved_policy,
            normalize=diagnostics_options.normalize,
            params=diagnostics_options.params,
            sql=diagnostics_options.sql,
        ),
    )
    ast_fingerprint = canonical_ast_fingerprint(diagnostics.optimized)
    plan_hash = ast_policy_fingerprint(
        ast_fingerprint=ast_fingerprint,
        policy_hash=snapshot.policy_hash,
    )
    lineage = _lineage_payload(
        diagnostics.optimized,
        schema_map=resolved_options.schema_map,
        policy=resolved_policy,
    )
    canonical = lineage.canonical_fingerprint if lineage is not None else ast_fingerprint
    sqlglot_ast = _ast_payload(
        diagnostics.optimized,
        sql=diagnostics.sql_text_optimized,
        policy=resolved_policy,
    )
    return SqlGlotPlanArtifacts(
        diagnostics=diagnostics,
        plan_hash=plan_hash,
        policy_hash=snapshot.policy_hash,
        policy_rules_hash=snapshot.rules_hash,
        schema_map_hash=resolved_options.schema_map_hash,
        lineage=lineage,
        canonical_fingerprint=canonical,
        sqlglot_ast=sqlglot_ast,
    )


def _lineage_payload(
    expr: Expression,
    *,
    schema_map: SchemaMapping | None,
    policy: SqlGlotPolicy,
) -> LineagePayload | None:
    try:
        schema = _flatten_schema_mapping(schema_map) if schema_map is not None else None
        return extract_lineage_payload(
            expr,
            options=LineageExtractionOptions(
                schema=schema,
                dialect=policy.write_dialect,
            ),
        )
    except (RuntimeError, TypeError, ValueError):
        return None


def _ast_payload(expr: Expression, *, sql: str, policy: SqlGlotPolicy) -> bytes | None:
    try:
        return serialize_ast_artifact(ast_to_artifact(expr, sql=sql, policy=policy))
    except (RuntimeError, TypeError, ValueError, msgspec.EncodeError):
        return None


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


def sqlglot_ast_payload(
    expr: Expression | None,
    *,
    policy: SqlGlotPolicy | None,
) -> bytes | None:
    """Return serialized SQLGlot AST diagnostics payload.

    Returns
    -------
    bytes | None
        Serialized AST payload for diagnostics, if available.
    """
    if expr is None:
        return None
    try:
        return serialize_ast_artifact(ast_to_artifact(expr, policy=policy))
    except (TypeError, ValueError, msgspec.EncodeError):
        return None


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
    "SqlGlotPlanArtifacts",
    "SqlGlotPlanOptions",
    "SqlGlotRelationDiff",
    "collect_sqlglot_plan_artifacts",
    "ibis_to_sqlglot",
    "missing_schema_columns",
    "relation_diff",
    "sqlglot_diagnostics",
]
