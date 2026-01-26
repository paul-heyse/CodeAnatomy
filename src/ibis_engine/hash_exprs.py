"""Ibis helpers for HashExprSpec-based identifier expressions."""

from __future__ import annotations

from collections.abc import Sequence

from ibis.expr.types import Table, Value

from ibis_engine.expr_compiler import expr_ir_to_ibis
from ibis_engine.hashing import (
    HashExprSpec,
    hash_expr_ir,
    masked_stable_id_expr_ir,
    stable_id_expr_ir,
)
from sqlglot_tools.expr_spec import SqlExprSpec


def _expr_from_spec(table: Table, spec: SqlExprSpec) -> Value:
    expr_ir = spec.expr_ir
    if expr_ir is None:
        msg = "SqlExprSpec missing expr_ir; ExprIR-backed specs are required."
        raise ValueError(msg)
    return expr_ir_to_ibis(expr_ir, table)


def stable_id_expr_from_spec(
    table: Table,
    *,
    spec: HashExprSpec,
    use_128: bool | None = None,
) -> Value:
    """Return a stable_id Ibis expression for a HashExprSpec.

    Returns
    -------
    ibis.expr.types.Value
        Ibis expression representing the stable_id computation.
    """
    return _expr_from_spec(table, stable_id_expr_ir(spec=spec, use_128=use_128))


def masked_stable_id_expr_from_spec(
    table: Table,
    *,
    spec: HashExprSpec,
    required: Sequence[str],
    use_128: bool | None = None,
) -> Value:
    """Return a masked stable_id Ibis expression for a HashExprSpec.

    Returns
    -------
    ibis.expr.types.Value
        Ibis expression representing the masked stable_id computation.
    """
    return _expr_from_spec(
        table,
        masked_stable_id_expr_ir(spec=spec, required=required, use_128=use_128),
    )


def stable_key_hash_expr_from_spec(
    table: Table,
    *,
    spec: HashExprSpec,
    use_128: bool | None = False,
) -> Value:
    """Return a prefixed hash key expression for a HashExprSpec.

    Returns
    -------
    ibis.expr.types.Value
        Ibis expression representing the prefixed hash key.
    """
    return _expr_from_spec(table, hash_expr_ir(spec=spec, use_128=use_128))


__all__ = [
    "HashExprSpec",
    "masked_stable_id_expr_from_spec",
    "stable_id_expr_from_spec",
    "stable_key_hash_expr_from_spec",
]
