"""Shared plan-lane expression helpers for normalize pipelines."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.compute.expr import ExprSpec
from arrowdsl.compute.kernels import def_use_kind_array
from arrowdsl.core.ids import (
    HashSpec,
    hash64_from_arrays,
    hash_column_values,
    hash_expression,
    prefixed_hash_id,
)
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    ComputeExpression,
    TableLike,
    ensure_expression,
    pc,
)
from arrowdsl.schema.arrays import CoalesceExpr, FieldExpr
from normalize.ids import masked_hash_expression

_NULL_SEPARATOR = "\x1f"


@dataclass(frozen=True)
class ComputeExprSpec:
    """ExprSpec backed by a compute expression and a materializer."""

    expr: ComputeExpression
    materialize_fn: Callable[[TableLike], ArrayLike]

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for plan-lane evaluation.

        Returns
        -------
        ComputeExpression
            Plan-lane compute expression.
        """
        return self.expr

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the expression against a table.

        Returns
        -------
        ArrayLike
            Kernel-lane array result.
        """
        return self.materialize_fn(table)

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe expressions.
        """
        return self is not None


@dataclass(frozen=True)
class TrimExprSpec:
    """ExprSpec for UTF-8 whitespace trimming."""

    column: str

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane trim expression.

        Returns
        -------
        ComputeExpression
            Expression trimming UTF-8 whitespace.
        """
        return ensure_expression(
            pc.call_function("utf8_trim_whitespace", [pc.field(self.column)])
        )

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the trim expression against a table.

        Returns
        -------
        ArrayLike
            Trimmed values array.

        Raises
        ------
        TypeError
            Raised when the compute kernel returns a non-array output.
        """
        values = pc.call_function("utf8_trim_whitespace", [table[self.column]])
        if isinstance(values, ChunkedArrayLike):
            return values.combine_chunks()
        if isinstance(values, ArrayLike):
            return values
        msg = "TrimExprSpec.materialize expected array-like output."
        raise TypeError(msg)

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe expressions.
        """
        return self is not None


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
        return masked_hash_expression(spec=self.spec, required=tuple(self.required))

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
        if not self.required:
            mask = pa.array([], pa.bool_())
        else:
            first = self.required[0]
            if first in table.column_names:
                mask = pc.is_valid(table[first])
            else:
                mask = pa.array([False] * table.num_rows, type=pa.bool_())
        for col in self.required[1:]:
            if col in table.column_names:
                mask = pc.and_(mask, pc.is_valid(table[col]))
            else:
                mask = pc.and_(mask, pa.array([False] * table.num_rows, type=pa.bool_()))
        null_value = pa.scalar(None, type=pa.string() if self.spec.as_string else pa.int64())
        return pc.if_else(mask, hashed, null_value)

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe expressions.
        """
        return self is not None


@dataclass(frozen=True)
class DefUseKindExprSpec:
    """ExprSpec that classifies opnames into def/use kinds."""

    column: str
    def_ops: Sequence[str]
    def_prefixes: Sequence[str]
    use_prefixes: Sequence[str]

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane def/use classification expression.

        Returns
        -------
        ComputeExpression
            Def/use classification expression.
        """
        opname = pc.cast(pc.field(self.column), pa.string())
        def_ops_arr = pa.array(sorted(self.def_ops), type=pa.string())
        is_def = pc.or_(
            pc.is_in(opname, value_set=def_ops_arr),
            pc.or_(
                pc.starts_with(opname, self.def_prefixes[0]),
                pc.starts_with(opname, self.def_prefixes[1]),
            ),
        )
        is_use = pc.starts_with(opname, self.use_prefixes[0])
        expr = pc.if_else(
            is_def,
            pc.scalar("def"),
            pc.if_else(
                is_use,
                pc.scalar("use"),
                pc.scalar(pa.scalar(None, type=pa.string())),
            ),
        )
        return ensure_expression(expr)

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the def/use classification against a table.

        Returns
        -------
        ArrayLike
            Def/use classification array.
        """
        return def_use_kind_array(
            table[self.column],
            def_ops=tuple(self.def_ops),
            def_prefixes=tuple(self.def_prefixes),
            use_prefixes=tuple(self.use_prefixes),
        )

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe expressions.
        """
        return self is not None


@dataclass(frozen=True)
class CoalesceStringExprSpec:
    """ExprSpec for coalescing string-like columns."""

    columns: Sequence[str]

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane coalesce expression.

        Returns
        -------
        ComputeExpression
            Coalesced string expression.
        """
        exprs = [FieldExpr(name=col).to_expression() for col in self.columns]
        return ensure_expression(pc.coalesce(*exprs))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the coalesce expression against a table.

        Returns
        -------
        ArrayLike
            Coalesced string array.
        """
        exprs = tuple(FieldExpr(name=col) for col in self.columns)
        return CoalesceExpr(exprs=exprs).materialize(table)

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe expressions.
        """
        return self is not None


def column_or_null_expr(name: str, dtype: pa.DataType, *, available: set[str]) -> ComputeExpression:
    """Return a field expression or typed null expression when missing.

    Returns
    -------
    ComputeExpression
        Field expression or typed null expression.
    """
    if name in available:
        return pc.field(name)
    return pc.scalar(pa.scalar(None, type=dtype))


def trimmed_non_empty_expr(col: str) -> tuple[ComputeExpression, ComputeExpression]:
    """Return trimmed expression and non-empty predicate for a UTF-8 column.

    Returns
    -------
    tuple[ComputeExpression, ComputeExpression]
        Trimmed expression and non-empty predicate.
    """
    trimmed = ensure_expression(pc.call_function("utf8_trim_whitespace", [pc.field(col)]))
    non_empty = ensure_expression(
        pc.and_(
            pc.is_valid(trimmed),
            pc.greater(ensure_expression(pc.call_function("utf8_length", [trimmed])), pc.scalar(0)),
        )
    )
    return trimmed, non_empty


def coalesce_string_expr(cols: Sequence[str]) -> ComputeExpression:
    """Return a coalesce expression for string-like columns.

    Returns
    -------
    ComputeExpression
        Coalesced string expression.
    """
    exprs = [pc.field(col) for col in cols]
    return ensure_expression(pc.coalesce(*exprs))


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
        _ = hash_expression(HashSpec(prefix="_normalize_seed", cols=("__seed__",), missing="null"))
        exprs = []
        for part in self.parts:
            expr = part.to_expression()
            expr = ensure_expression(pc.fill_null(pc.cast(expr, pa.string()), pc.scalar(self.null_sentinel)))
            exprs.append(expr)
        if self.prefix:
            exprs.insert(0, pc.scalar(self.prefix))
        joined = pc.binary_join_element_wise(*exprs, _NULL_SEPARATOR)
        hashed = pc.call_function("hash64_udf", [joined])
        if self.as_string:
            hashed = pc.cast(hashed, pa.string())
            hashed = pc.binary_join_element_wise(pc.scalar(self.prefix), hashed, ":")
        return ensure_expression(hashed)

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
    "column_or_null_expr",
    "trimmed_non_empty_expr",
]
