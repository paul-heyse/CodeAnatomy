"""Predicate specs and validity mask helpers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from arrowdsl.compute.expr_ops import and_expr
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    ComputeExpression,
    TableLike,
    ensure_expression,
    pc,
)

if TYPE_CHECKING:
    from arrowdsl.compute.expr_core import ExprSpec, PredicateKind


def _false_mask(num_rows: int) -> ArrayLike:
    """Return a boolean mask of all False values.

    Returns
    -------
    ArrayLike
        Boolean mask with all values set to ``False``.
    """
    return pc.is_valid(pa.nulls(num_rows, type=pa.bool_()))


def valid_mask_array(
    values: Sequence[ArrayLike | ChunkedArrayLike],
) -> ArrayLike | ChunkedArrayLike:
    """Return a validity mask for a sequence of arrays.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Boolean mask where all inputs are valid.

    Raises
    ------
    ValueError
        Raised when no arrays are provided.
    """
    if not values:
        msg = "valid_mask_array requires at least one array."
        raise ValueError(msg)
    mask = pc.is_valid(values[0])
    for value in values[1:]:
        mask = pc.and_(mask, pc.is_valid(value))
    return mask


def valid_mask_for_columns(table: TableLike, cols: Sequence[str]) -> ArrayLike | ChunkedArrayLike:
    """Return a validity mask for columns, treating missing columns as invalid.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Boolean mask where all referenced columns are valid.

    Raises
    ------
    ValueError
        Raised when no column names are provided.
    """
    if not cols:
        msg = "valid_mask_for_columns requires at least one column."
        raise ValueError(msg)
    mask: ArrayLike | ChunkedArrayLike | None = None
    for name in cols:
        if name in table.column_names:
            next_mask = pc.is_valid(table[name])
        else:
            next_mask = _false_mask(table.num_rows)
        mask = next_mask if mask is None else pc.and_(mask, next_mask)
    return mask if mask is not None else _false_mask(table.num_rows)


def valid_mask_expr(
    cols: Sequence[str],
    *,
    available: Sequence[str] | None = None,
) -> ComputeExpression:
    """Return a validity mask expression for the provided columns.

    Returns
    -------
    ComputeExpression
        Boolean expression marking rows with all columns valid.

    Raises
    ------
    ValueError
        Raised when no column names are provided.
    """
    if not cols:
        msg = "valid_mask_expr requires at least one column."
        raise ValueError(msg)

    def _expr_for(name: str) -> ComputeExpression:
        if available is not None and name not in available:
            return ensure_expression(pc.scalar(pa.scalar(value=False)))
        return ensure_expression(cast("ComputeExpression", pc.is_valid(pc.field(name))))

    mask = _expr_for(cols[0])
    for name in cols[1:]:
        mask = and_expr(mask, _expr_for(name))
    return mask


@dataclass(frozen=True)
class IsNull:
    """Predicate testing nulls in a column."""

    col: str

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the predicate.
        """
        return ensure_expression(pc.is_null(pc.field(self.col)))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane predicate mask.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return pc.is_null(table[self.col])

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe predicates.
        """
        return self is not None


@dataclass(frozen=True)
class InSet:
    """Predicate testing membership in a static value set."""

    col: str
    values: tuple[object, ...]

    def to_expression(self) -> ComputeExpression:
        """Return the membership predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the predicate.
        """
        return ensure_expression(pc.is_in(pc.field(self.col), value_set=list(self.values)))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane predicate mask.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return pc.is_in(table[self.col], value_set=list(self.values))

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe predicates.
        """
        return self is not None


@dataclass(frozen=True)
class Not:
    """Logical NOT of a predicate."""

    pred: ExprSpec

    def to_expression(self) -> ComputeExpression:
        """Return the inverted predicate expression.

        Returns
        -------
        ComputeExpression
            Expression representing the inverted predicate.
        """
        return ensure_expression(pc.invert(self.pred.to_expression()))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Return the kernel-lane inverted mask.

        Returns
        -------
        ArrayLike
            Boolean mask for the predicate.
        """
        return pc.invert(self.pred.materialize(table))

    def is_scalar(self) -> bool:
        """Return whether this predicate is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe predicates.
        """
        return self is not None


def predicate_spec(
    kind: PredicateKind,
    *,
    col: str | None = None,
    values: Sequence[object] = (),
    predicate: ExprSpec | None = None,
) -> ExprSpec:
    """Return a predicate spec for the requested kind.

    Returns
    -------
    ExprSpec
        Predicate specification instance.

    Raises
    ------
    ValueError
        Raised when required arguments are missing or unknown.
    """
    if kind == "is_null":
        if col is None:
            msg = "predicate_spec requires col for is_null."
            raise ValueError(msg)
        return IsNull(col)
    if kind == "in_set":
        if col is None:
            msg = "predicate_spec requires col for in_set."
            raise ValueError(msg)
        return InSet(col, tuple(values))
    if kind == "not":
        if predicate is None:
            msg = "predicate_spec requires predicate for not."
            raise ValueError(msg)
        return Not(predicate)
    msg = f"Unknown predicate kind: {kind}."
    raise ValueError(msg)


__all__ = [
    "InSet",
    "IsNull",
    "Not",
    "predicate_spec",
    "valid_mask_array",
    "valid_mask_expr",
    "valid_mask_for_columns",
]
