"""Policy helpers for normalize rule confidence and ambiguity."""

from __future__ import annotations

from collections.abc import Mapping

from arrowdsl.plan.ops import DedupeSpec, SortKey
from arrowdsl.spec.expr_ir import ExprIR
from normalize.rule_model import AmbiguityPolicy, ConfidencePolicy


def confidence_expr(policy: ConfidencePolicy, *, source_field: str | None = None) -> ExprIR:
    """Return an ExprIR computing confidence for a policy.

    Returns
    -------
    ExprIR
        Expression computing a confidence score.
    """
    base_expr = ExprIR(op="literal", value=float(policy.base))
    if policy.source_weight and source_field:
        source_expr = ExprIR(op="field", name=source_field)
        weight_expr = _source_weight_expr(source_expr, policy.source_weight)
        base_expr = ExprIR(op="call", name="add", args=(base_expr, weight_expr))
    if policy.penalty:
        penalty_expr = ExprIR(op="literal", value=float(policy.penalty))
        base_expr = ExprIR(op="call", name="subtract", args=(base_expr, penalty_expr))
    return base_expr


def ambiguity_kernels(policy: AmbiguityPolicy | None) -> tuple[DedupeSpec, ...]:
    """Return dedupe kernels enforcing ambiguity policy.

    Returns
    -------
    tuple[DedupeSpec, ...]
        Dedupe specs derived from the policy.
    """
    if policy is None or policy.winner_select is None:
        return ()
    winner = policy.winner_select
    tie_breakers = (
        SortKey(winner.score_col, winner.score_order),
        *winner.tie_breakers,
        *policy.tie_breakers,
    )
    spec = DedupeSpec(
        keys=winner.keys,
        strategy="KEEP_BEST_BY_SCORE",
        tie_breakers=tie_breakers,
    )
    return (spec,)


def _source_weight_expr(
    source_expr: ExprIR,
    weights: Mapping[str, float],
) -> ExprIR:
    expr = ExprIR(op="literal", value=0.0)
    for source, weight in weights.items():
        cond = ExprIR(
            op="call", name="equal", args=(source_expr, ExprIR(op="literal", value=source))
        )
        expr = ExprIR(
            op="call",
            name="if_else",
            args=(cond, ExprIR(op="literal", value=float(weight)), expr),
        )
    return expr


__all__ = ["ambiguity_kernels", "confidence_expr"]
