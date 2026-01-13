"""Spec validation rule helpers."""

from __future__ import annotations

from dataclasses import dataclass

import arrowdsl.core.interop as pa
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ComputeExpression, TableLike, ensure_expression, pc
from arrowdsl.plan.plan import Plan


@dataclass(frozen=True)
class SpecValidationRule:
    """Spec validation rule expressed as a compute predicate."""

    code: str
    predicate: ComputeExpression


@dataclass(frozen=True)
class SpecValidationSuite:
    """Collection of validation rules for spec tables."""

    rules: tuple[SpecValidationRule, ...] = ()

    def invalid_mask(self) -> ComputeExpression:
        """Return a combined invalid-row mask.

        Returns
        -------
        ComputeExpression
            Boolean expression marking invalid rows.
        """
        if not self.rules:
            return ensure_expression(pc.scalar(pa.scalar(value=False)))
        exprs = [rule.predicate for rule in self.rules]
        return ensure_expression(pc.or_(*exprs))

    def invalid_rows_plan(self, plan: Plan, *, ctx: ExecutionContext) -> Plan:
        """Return a plan filtering invalid rows.

        Returns
        -------
        Plan
            Plan yielding invalid rows.
        """
        return plan.filter(self.invalid_mask(), ctx=ctx)

    def invalid_rows_table(self, table: TableLike, *, ctx: ExecutionContext) -> TableLike:
        """Return invalid rows from a table.

        Returns
        -------
        TableLike
            Invalid rows table.
        """
        plan = Plan.table_source(table, label="spec_validate")
        return self.invalid_rows_plan(plan, ctx=ctx).to_table(ctx=ctx)


__all__ = ["SpecValidationRule", "SpecValidationSuite"]
