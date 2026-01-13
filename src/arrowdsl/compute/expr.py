"""Expression helpers for building Arrow compute predicates."""

from __future__ import annotations

from typing import Literal, Protocol

from arrowdsl.core.interop import ArrayLike, ComputeExpression, ScalarLike, TableLike

type ScalarValue = bool | int | float | str | bytes | ScalarLike | None
type PredicateKind = Literal["in_set", "is_null", "not"]


class ExprSpec(Protocol):
    """Protocol for expressions usable in plan or kernel lanes."""

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for plan-lane projection or filtering.

        Returns
        -------
        ComputeExpression
            Plan-lane compute expression.
        """
        ...

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the expression against a table in kernel lane.

        Returns
        -------
        ArrayLike
            Kernel-lane array result.
        """
        ...

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe for scan projection.

        Returns
        -------
        bool
            ``True`` when the expression is scalar-safe.
        """
        ...


__all__ = ["ExprSpec", "PredicateKind", "ScalarValue"]
