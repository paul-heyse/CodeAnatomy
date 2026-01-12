"""ExprSpec helpers for extract QuerySpec derived columns."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.core.ids import HashSpec, hash_column_values, hash_expression
from arrowdsl.core.interop import ArrayLike, ComputeExpression, TableLike, ensure_expression, pc


@dataclass(frozen=True)
class HashExprSpec:
    """ExprSpec for hash-based identifiers."""

    spec: HashSpec

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane hash expression.

        Returns
        -------
        ComputeExpression
            Hash compute expression.
        """
        return hash_expression(self.spec)

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the hash expression against a table.

        Returns
        -------
        ArrayLike
            Hash array for the spec.
        """
        return hash_column_values(table, spec=self.spec)

    def is_scalar(self) -> bool:
        """Return whether the expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe expressions.
        """
        return self is not None


@dataclass(frozen=True)
class MaskedHashExprSpec:
    """ExprSpec for hash identifiers masked by required column validity."""

    spec: HashSpec
    required: Sequence[str]

    def to_expression(self) -> ComputeExpression:
        """Return the masked hash expression for plan-lane use.

        Returns
        -------
        ComputeExpression
            Masked hash compute expression.
        """
        expr = hash_expression(self.spec)
        if not self.required:
            return expr
        mask = pc.is_valid(pc.field(self.required[0]))
        for name in self.required[1:]:
            mask = pc.and_(mask, pc.is_valid(pc.field(name)))
        null_value = pa.scalar(None, type=pa.string() if self.spec.as_string else pa.int64())
        return ensure_expression(pc.if_else(mask, expr, null_value))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the masked hash expression against a table.

        Returns
        -------
        ArrayLike
            Masked hash array for the spec.
        """
        if not self.required:
            return hash_column_values(table, spec=self.spec)
        hashed = hash_column_values(table, spec=self.spec)
        mask = pc.is_valid(table[self.required[0]])
        for name in self.required[1:]:
            mask = pc.and_(mask, pc.is_valid(table[name]))
        null_value = pa.scalar(None, type=pa.string() if self.spec.as_string else pa.int64())
        return pc.if_else(mask, hashed, null_value)

    def is_scalar(self) -> bool:
        """Return whether the expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe expressions.
        """
        return self is not None


@dataclass(frozen=True)
class CoalesceStringExprSpec:
    """ExprSpec for string coalesce operations."""

    columns: Sequence[str]

    def to_expression(self) -> ComputeExpression:
        """Return a plan-lane coalesce expression for the columns.

        Returns
        -------
        ComputeExpression
            Coalesce compute expression.
        """
        exprs = [pc.field(col) for col in self.columns]
        return ensure_expression(pc.coalesce(*exprs))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the coalesce expression against a table.

        Returns
        -------
        ArrayLike
            Coalesced array.
        """
        exprs = [table[col] for col in self.columns]
        return pc.coalesce(*exprs)

    def is_scalar(self) -> bool:
        """Return whether the expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe expressions.
        """
        return self is not None


__all__ = [
    "CoalesceStringExprSpec",
    "HashExprSpec",
    "MaskedHashExprSpec",
]
