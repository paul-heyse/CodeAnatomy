"""SQLGlot compiler checkpoints and plan hashing."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from ibis.expr.types import Table as IbisTable
from sqlglot import Expression
from sqlglot.optimizer.normalize import normalization_distance, normalize
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify import qualify

from sqlglot_tools.bridge import IbisCompilerBackend, ibis_to_sqlglot

SchemaMapping = Mapping[str, Mapping[str, str]]


@dataclass(frozen=True)
class CompilerCheckpoint:
    """SQLGlot compiler checkpoint with plan identity."""

    expression: Expression
    qualified: Expression
    normalized: Expression
    plan_hash: str
    sql: str


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
    schema = {name: dict(cols) for name, cols in schema_map.items()} if schema_map else None
    qualified = qualify(compiled, schema=schema, dialect=dialect)
    normalized = normalize_identifiers(qualified, dialect=dialect)
    if normalization_distance(normalized, max_=normalization_budget) <= normalization_budget:
        normalized = normalize(normalized)
    sql = normalized.sql(dialect=dialect)
    plan_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()
    return CompilerCheckpoint(
        expression=compiled,
        qualified=qualified,
        normalized=normalized,
        plan_hash=plan_hash,
        sql=sql,
    )


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
