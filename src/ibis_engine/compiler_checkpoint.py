"""SQLGlot compiler checkpoints and plan hashing."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import cast

from ibis.expr.types import Table as IbisTable

from datafusion_engine.schema_introspection import (
    SchemaIntrospector,
    schema_map_fingerprint_from_mapping,
)
from ibis_engine.registry import datafusion_context
from sqlglot_tools.bridge import IbisCompilerBackend, ibis_to_sqlglot
from sqlglot_tools.compat import Expression
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    SchemaMapping,
    default_sqlglot_policy,
    normalize_expr,
    plan_fingerprint,
    sqlglot_policy_snapshot_for,
    sqlglot_sql,
)


@dataclass(frozen=True)
class CompilerCheckpoint:
    """SQLGlot compiler checkpoint with plan identity."""

    expression: Expression
    qualified: Expression
    normalized: Expression
    plan_hash: str
    sql: str
    policy_hash: str
    schema_map_hash: str | None


def compile_checkpoint(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    schema_map: SchemaMapping | None = None,
    dialect: str = "datafusion",
    normalization_budget: int = 1000,
) -> CompilerCheckpoint:
    """Compile an expression into a SQLGlot checkpoint and plan hash.

    Returns
    -------
    CompilerCheckpoint
        Checkpoint with normalized AST and plan hash.
    """
    compiled = ibis_to_sqlglot(expr, backend=backend, params=None)
    schema = schema_map or _schema_map_from_backend(backend)
    schema_map_hash = schema_map_fingerprint_from_mapping(schema) if schema is not None else None
    policy = default_sqlglot_policy()
    if dialect:
        policy = replace(policy, read_dialect=dialect, write_dialect=dialect)
    if normalization_budget is not None:
        policy = replace(policy, normalization_distance=normalization_budget)
    qualified = normalize_expr(
        compiled,
        options=NormalizeExprOptions(
            schema=schema,
            policy=policy,
        ),
    )
    normalized = qualified
    sql = sqlglot_sql(normalized, policy=policy)
    policy_hash = sqlglot_policy_snapshot_for(policy).policy_hash
    plan_hash = plan_fingerprint(
        normalized,
        dialect=policy.write_dialect,
        policy_hash=policy_hash,
        schema_map_hash=schema_map_hash,
    )
    return CompilerCheckpoint(
        expression=compiled,
        qualified=qualified,
        normalized=normalized,
        plan_hash=plan_hash,
        sql=sql,
        policy_hash=policy_hash,
        schema_map_hash=schema_map_hash,
    )


def _schema_map_from_backend(backend: IbisCompilerBackend) -> dict[str, dict[str, str]] | None:
    from datafusion_engine.runtime import sql_options_for_profile

    try:
        ctx = datafusion_context(backend)
    except (TypeError, ValueError):
        return None
    return SchemaIntrospector(ctx, sql_options=sql_options_for_profile(None)).schema_map()


def try_plan_hash(
    expr: IbisTable,
    *,
    backend: object | None,
    schema_map: SchemaMapping | None = None,
    dialect: str = "datafusion",
) -> str | None:
    """Return the plan hash when SQLGlot compilation is available.

    Returns
    -------
    str | None
        Plan hash when available, otherwise ``None``.
    """
    if backend is None or not hasattr(backend, "compiler"):
        return None
    checkpoint = compile_checkpoint(
        expr,
        backend=cast("IbisCompilerBackend", backend),
        schema_map=schema_map,
        dialect=dialect,
    )
    return checkpoint.plan_hash


__all__ = ["CompilerCheckpoint", "compile_checkpoint", "try_plan_hash"]
