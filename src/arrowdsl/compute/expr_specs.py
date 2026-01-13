"""Canonical ExprSpec implementations for plan and kernel lanes."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from arrowdsl.compute.expr import ExprSpec
from arrowdsl.compute.ids import (
    HashSpec,
    hash64_from_arrays,
    hash_column_values,
    hash_expression,
    hash_expression_from_parts,
    masked_hash_array,
    masked_hash_expr,
    prefixed_hash_id,
)
from arrowdsl.compute.macros import (
    CoalesceStringExprSpec,
    ComputeExprSpec,
    DefUseKindExprSpec,
    TrimExprSpec,
    coalesce_string_expr,
    trimmed_non_empty_expr,
)
from arrowdsl.core.interop import ArrayLike, ComputeExpression, TableLike


@dataclass(frozen=True)
class HashExprSpec:
    """ExprSpec for hash-based identifiers from table columns."""

    spec: HashSpec

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane hash expression.

        Returns
        -------
        ComputeExpression
            Hash expression for the spec.
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
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe expressions.
        """
        return self is not None


@dataclass(frozen=True)
class MaskedHashExprSpec:
    """ExprSpec for hash-based identifiers with required column masking."""

    spec: HashSpec
    required: Sequence[str]

    def to_expression(self) -> ComputeExpression:
        """Return the masked hash expression for plan-lane use.

        Returns
        -------
        ComputeExpression
            Masked hash expression.
        """
        return masked_hash_expr(self.spec, required=tuple(self.required))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the masked hash expression against a table.

        Returns
        -------
        ArrayLike
            Masked hash array for the spec.
        """
        return masked_hash_array(table, spec=self.spec, required=self.required)

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe expressions.
        """
        return self is not None


@dataclass(frozen=True)
class HashFromExprsSpec:
    """ExprSpec for hash IDs built from expression parts."""

    prefix: str
    parts: Sequence[ExprSpec]
    as_string: bool = True
    null_sentinel: str = "None"

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane hash expression from parts.

        Returns
        -------
        ComputeExpression
            Hash expression for the parts.
        """
        exprs = [part.to_expression() for part in self.parts]
        return hash_expression_from_parts(
            exprs,
            prefix=self.prefix,
            null_sentinel=self.null_sentinel,
            as_string=self.as_string,
        )

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the hash expression against a table.

        Returns
        -------
        ArrayLike
            Hash array for the parts.
        """
        arrays = [part.materialize(table) for part in self.parts]
        if self.as_string:
            return prefixed_hash_id(arrays, prefix=self.prefix, null_sentinel=self.null_sentinel)
        return hash64_from_arrays(arrays, prefix=self.prefix, null_sentinel=self.null_sentinel)

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe expressions.
        """
        return self is not None


__all__ = [
    "CoalesceStringExprSpec",
    "ComputeExprSpec",
    "DefUseKindExprSpec",
    "HashExprSpec",
    "HashFromExprsSpec",
    "MaskedHashExprSpec",
    "TrimExprSpec",
    "coalesce_string_expr",
    "trimmed_non_empty_expr",
]
