"""Predicate helpers for plan and kernel lanes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from arrowdsl.compute.expr import ExprSpec
from arrowdsl.compute.macros import (
    bitmask_is_set_expr,
    filter_non_empty_utf8,
    invalid_id_expr,
    null_if_empty_or_zero,
    predicate_spec,
    trimmed_non_empty_utf8,
    zero_expr,
)
from arrowdsl.core.interop import ArrayLike, ComputeExpression, TableLike

if TYPE_CHECKING:
    from arrowdsl.plan.plan import Plan


@dataclass(frozen=True)
class FilterSpec:
    """Filter specification usable in plan or kernel lanes."""

    predicate: ExprSpec

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the predicate.
        """
        return self.predicate.to_expression()

    def mask(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane boolean mask.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return self.predicate.materialize(table)

    def apply_plan(self, plan: Plan) -> Plan:
        """Apply the filter to a plan.

        Returns
        -------
        Plan
            Filtered plan.
        """
        return plan.filter(self.to_expression())

    def apply_kernel(self, table: TableLike) -> TableLike:
        """Apply the filter to a table.

        Returns
        -------
        TableLike
            Filtered table.
        """
        return table.filter(self.mask(table))


__all__ = [
    "FilterSpec",
    "bitmask_is_set_expr",
    "filter_non_empty_utf8",
    "invalid_id_expr",
    "null_if_empty_or_zero",
    "predicate_spec",
    "trimmed_non_empty_utf8",
    "zero_expr",
]
