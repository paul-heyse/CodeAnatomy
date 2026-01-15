"""Materializers for normalize rule outputs."""

from __future__ import annotations

from collections.abc import Mapping

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.plan.plan import Plan
from normalize.rule_model import NormalizeRule


def materialize_rule_outputs(
    plans: Mapping[str, Plan],
    *,
    ctx: ExecutionContext,
    rules: Mapping[str, NormalizeRule] | None = None,
    attach_rule_meta: bool = False,
) -> dict[str, TableLike]:
    """Materialize rule plans into tables with optional rule metadata columns.

    Raises
    ------
    RuntimeError
        Raised because ArrowDSL normalize materializers are retired.
    """
    rule_count = "none" if rules is None else str(len(rules))
    msg = (
        "ArrowDSL normalize materializers are retired; use run_normalize or "
        "materialize_ibis_plan via the Ibis adapter. "
        f"plans={len(plans)}, rules={rule_count}, ctx_mode={ctx.mode}, "
        f"attach_rule_meta={attach_rule_meta}."
    )
    raise RuntimeError(msg)


__all__ = ["materialize_rule_outputs"]
