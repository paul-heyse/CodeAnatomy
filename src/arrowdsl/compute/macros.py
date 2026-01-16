"""Canonical expression macros for plan and kernel lanes."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol, cast

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.compile.expr_compiler import ExprCompiler
from arrowdsl.compute.expr_core import ExprSpec, cast_expr
from arrowdsl.compute.expr_ops import and_expr, or_expr
from arrowdsl.compute.position_encoding import (
    DEFAULT_POSITION_ENCODING,
    ENC_UTF8,
    ENC_UTF16,
    ENC_UTF32,
    VALID_POSITION_ENCODINGS,
)
from arrowdsl.compute.predicates import InSet, IsNull, Not, predicate_spec
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    ComputeExpression,
    DataTypeLike,
    TableLike,
    ensure_expression,
    pc,
)
from arrowdsl.ir.expr import ExprNode


class ColumnExpr(ExprSpec, Protocol):
    """Alias for expression specs used as column expressions."""


type _DefUseFn = Callable[..., ArrayLike]


def _def_use_kind_array_fn() -> _DefUseFn:
    module = importlib.import_module("arrowdsl.compute.kernels")
    return cast("_DefUseFn", module.def_use_kind_array)


@dataclass(frozen=True)
class ComputeExprSpec:
    """ExprSpec backed by a compute expression and a materializer."""

    expr: ComputeExpression | ExprNode
    materialize_fn: Callable[[TableLike], ArrayLike]
    scalar_safe: bool = True
    registry: object | None = None

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for plan-lane evaluation.

        Returns
        -------
        ComputeExpression
            Plan-lane compute expression.
        """
        if isinstance(self.expr, ExprNode):
            compiler = ExprCompiler(registry=self.registry)
            return compiler.to_compute_expr(self.expr)
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
        return self.scalar_safe


@dataclass(frozen=True)
class ConstExpr:
    """Column expression representing a constant literal."""

    value: object
    dtype: DataTypeLike | None = None

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for the constant value.

        Returns
        -------
        ComputeExpression
            Expression representing the constant value.
        """
        scalar = self.value if self.dtype is None else pa.scalar(self.value, type=self.dtype)
        return ensure_expression(pc.scalar(scalar))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the constant as a full-length array.

        Returns
        -------
        ArrayLike
            Array filled with the constant value.
        """
        values = [self.value] * table.num_rows
        scalar_type = pa.scalar(self.value, type=self.dtype).type
        return pa.array(values, type=scalar_type)

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for constant expressions.
        """
        return self is not None


@dataclass(frozen=True)
class FieldExpr:
    """Column expression referencing an existing column."""

    name: str

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for the column reference.

        Returns
        -------
        ComputeExpression
            Expression referencing the column.
        """
        return pc.field(self.name)

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the column values from the table.

        Returns
        -------
        ArrayLike
            Column values from the table.
        """
        return table[self.name]

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for field references.
        """
        return self is not None


@dataclass(frozen=True)
class ColumnOrNullExpr:
    """Column expression that falls back to typed nulls when missing."""

    name: str
    dtype: DataTypeLike
    available: frozenset[str] | None = None
    cast: bool = False
    safe: bool = False

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane expression or typed null when missing.

        Returns
        -------
        ComputeExpression
            Expression for the column or a typed null literal.
        """
        if self.available is not None and self.name not in self.available:
            return ensure_expression(pc.scalar(pa.scalar(None, type=self.dtype)))
        expr = pc.field(self.name)
        if self.cast:
            return cast_expr(expr, self.dtype, safe=self.safe)
        return ensure_expression(expr)

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the column values or typed nulls.

        Returns
        -------
        ArrayLike
            Column values or typed nulls when missing.
        """
        if self.name not in table.column_names:
            return pa.nulls(table.num_rows, type=self.dtype)
        values = table[self.name]
        if self.cast:
            return pc.cast(values, self.dtype, safe=self.safe)
        return values

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe expressions.
        """
        return self is not None


@dataclass(frozen=True)
class CoalesceExpr:
    """Column expression that coalesces multiple expressions."""

    exprs: tuple[ColumnExpr, ...]

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for the coalesce operation.

        Returns
        -------
        ComputeExpression
            Expression coalescing the inputs.
        """
        exprs = [expr.to_expression() for expr in self.exprs]
        return ensure_expression(pc.coalesce(*exprs))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the coalesced values from the table.

        Returns
        -------
        ArrayLike
            Coalesced array values.
        """
        arrays = [expr.materialize(table) for expr in self.exprs]
        out = arrays[0]
        for arr in arrays[1:]:
            out = pc.coalesce(out, arr)
        return out

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` when all expressions are scalar-safe.
        """
        return all(expr.is_scalar() for expr in self.exprs)


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
        return ensure_expression(pc.utf8_trim_whitespace(pc.field(self.column)))

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
        opname = cast_expr(pc.field(self.column), pa.string())
        def_ops_arr = pa.array(sorted(self.def_ops), type=pa.string())
        is_def = or_expr(
            ensure_expression(cast("ComputeExpression", pc.is_in(opname, value_set=def_ops_arr))),
            or_expr(
                ensure_expression(
                    cast("ComputeExpression", pc.starts_with(opname, self.def_prefixes[0]))
                ),
                ensure_expression(
                    cast("ComputeExpression", pc.starts_with(opname, self.def_prefixes[1]))
                ),
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
        def_use_kind_array = _def_use_kind_array_fn()
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


def trimmed_non_empty_expr(col: str) -> tuple[ComputeExpression, ComputeExpression]:
    """Return trimmed expression and non-empty predicate for a UTF-8 column.

    Returns
    -------
    tuple[ComputeExpression, ComputeExpression]
        Trimmed expression and non-empty predicate.
    """
    trimmed = ensure_expression(pc.utf8_trim_whitespace(pc.field(col)))
    non_empty = and_expr(
        ensure_expression(cast("ComputeExpression", pc.is_valid(trimmed))),
        ensure_expression(
            cast(
                "ComputeExpression",
                pc.greater(
                    ensure_expression(pc.utf8_length(trimmed)),
                    pc.scalar(0),
                ),
            )
        ),
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


def null_expr(dtype: DataTypeLike) -> ComputeExpression:
    """Return a typed null expression.

    Returns
    -------
    ComputeExpression
        Expression producing nulls of the requested type.
    """
    return cast_expr(pc.scalar(None), dtype, safe=False)


def scalar_expr(value: object, *, dtype: DataTypeLike) -> ComputeExpression:
    """Return a typed scalar expression.

    Returns
    -------
    ComputeExpression
        Expression producing the typed scalar value.
    """
    return cast_expr(pc.scalar(value), dtype, safe=False)


def expr_context_value(value: object) -> str | None:
    """Normalize expression context strings.

    Returns
    -------
    str | None
        Normalized context value or ``None``.
    """
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if "." in raw:
        raw = raw.rsplit(".", 1)[-1]
    return raw.upper()


def flag_to_bool(value: object | None) -> bool | None:
    """Normalize integer/bool flags to optional bools.

    Returns
    -------
    bool | None
        Normalized boolean flag or ``None``.
    """
    if isinstance(value, bool):
        return True if value else None
    if isinstance(value, int):
        return True if value == 1 else None
    return None


def expr_context_expr(expr: ComputeExpression) -> ComputeExpression:
    """Return a compute expression for expr-context normalization.

    Returns
    -------
    ComputeExpression
        Expression normalizing expr-context values.
    """
    base = cast_expr(ensure_expression(expr), pa.string(), safe=False)
    trimmed = ensure_expression(pc.utf8_trim_whitespace(base))
    parts = ensure_expression(pc.split_pattern(trimmed, ".", reverse=True))
    last = ensure_expression(pc.list_element(parts, 0))
    upper = ensure_expression(pc.utf8_upper(last))
    empty = ensure_expression(pc.equal(pc.utf8_length(upper), pc.scalar(0)))
    return ensure_expression(
        pc.if_else(
            empty,
            cast_expr(pc.scalar(None), pa.string(), safe=False),
            upper,
        )
    )


def position_encoding_expr(expr: ComputeExpression) -> ComputeExpression:
    """Return a compute expression for position-encoding normalization.

    Returns
    -------
    ComputeExpression
        Expression normalizing position encodings to SCIP enum values.
    """
    text = ensure_expression(
        pc.utf8_upper(pc.utf8_trim_whitespace(cast_expr(expr, pa.string(), safe=False)))
    )
    is_digits = ensure_expression(pc.match_substring_regex(text, "^[0-9]+$"))
    digits_text = ensure_expression(
        pc.if_else(
            is_digits,
            text,
            cast_expr(pc.scalar(None), pa.string(), safe=False),
        )
    )
    digits_value = cast_expr(digits_text, pa.int32(), safe=False)
    valid_values = pa.array(sorted(VALID_POSITION_ENCODINGS), type=pa.int32())
    valid_options = pc.SetLookupOptions(value_set=valid_values)
    false_scalar = pc.scalar(pa.scalar(value=False))
    valid_digits = ensure_expression(
        pc.coalesce(ensure_expression(pc.is_in(digits_value, options=valid_options)), false_scalar)
    )
    utf8_hit = ensure_expression(
        pc.coalesce(ensure_expression(pc.match_substring(text, "UTF8")), false_scalar)
    )
    utf16_hit = ensure_expression(
        pc.coalesce(ensure_expression(pc.match_substring(text, "UTF16")), false_scalar)
    )
    utf32_hit = ensure_expression(
        pc.coalesce(ensure_expression(pc.match_substring(text, "UTF32")), false_scalar)
    )
    default_value = cast_expr(pc.scalar(DEFAULT_POSITION_ENCODING), pa.int32(), safe=False)
    utf8_value = cast_expr(pc.scalar(ENC_UTF8), pa.int32(), safe=False)
    utf16_value = cast_expr(pc.scalar(ENC_UTF16), pa.int32(), safe=False)
    utf32_value = cast_expr(pc.scalar(ENC_UTF32), pa.int32(), safe=False)
    return ensure_expression(
        pc.if_else(
            valid_digits,
            digits_value,
            pc.if_else(
                utf8_hit,
                utf8_value,
                pc.if_else(
                    utf16_hit,
                    utf16_value,
                    pc.if_else(utf32_hit, utf32_value, default_value),
                ),
            ),
        )
    )


def flag_to_bool_expr(expr: ComputeExpression) -> ComputeExpression:
    """Return a compute expression for optional boolean flags.

    Returns
    -------
    ComputeExpression
        Expression normalizing optional boolean flags.
    """
    casted = cast_expr(expr, pa.int64(), safe=False)
    hit = pc.equal(casted, pa.scalar(1))
    return ensure_expression(
        pc.if_else(
            hit,
            cast_expr(pc.scalar(1), pa.bool_(), safe=False),
            cast_expr(pc.scalar(None), pa.bool_(), safe=False),
        )
    )


def normalize_string_items(items: Sequence[object]) -> list[str | None]:
    """Normalize a sequence of values into optional strings.

    Returns
    -------
    list[str | None]
        Normalized string values.
    """
    out: list[str | None] = []
    for item in items:
        if item is None:
            out.append(None)
        elif isinstance(item, str):
            out.append(item)
        else:
            out.append(str(item))
    return out


def null_if_empty_or_zero(expr: ComputeExpression) -> ComputeExpression:
    """Return ``expr`` with empty/zero strings normalized to null.

    Returns
    -------
    ComputeExpression
        Expression with empty/zero strings mapped to null.
    """
    empty = ensure_expression(pc.equal(expr, pc.scalar("")))
    zero = ensure_expression(pc.equal(expr, pc.scalar("0")))
    return ensure_expression(
        pc.if_else(
            or_expr(empty, zero),
            cast_expr(pc.scalar(None), pa.string(), safe=False),
            expr,
        )
    )


def zero_expr(values: ComputeExpression, *, dtype: DataTypeLike) -> ComputeExpression:
    """Return a boolean expression testing for zero values.

    Returns
    -------
    ComputeExpression
        Boolean expression indicating zero values.
    """
    if patypes.is_dictionary(dtype):
        values = cast_expr(values, pa.string(), safe=False)
        dtype = pa.string()
    if patypes.is_string(dtype) or patypes.is_large_string(dtype):
        return ensure_expression(pc.equal(values, pc.scalar("0")))
    if patypes.is_integer(dtype):
        return ensure_expression(pc.equal(values, pa.scalar(0, type=dtype)))
    if patypes.is_floating(dtype):
        return ensure_expression(pc.equal(values, pa.scalar(0.0, type=dtype)))
    values = cast_expr(values, pa.string(), safe=False)
    return ensure_expression(pc.equal(values, pc.scalar("0")))


def invalid_id_expr(values: ComputeExpression, *, dtype: DataTypeLike) -> ComputeExpression:
    """Return an expression for null-or-zero identifier checks.

    Returns
    -------
    ComputeExpression
        Expression identifying null or zero identifiers.
    """
    return or_expr(
        ensure_expression(cast("ComputeExpression", pc.is_null(values))),
        zero_expr(values, dtype=dtype),
    )


def bitmask_is_set_expr(values: ComputeExpression, *, mask: int) -> ComputeExpression:
    """Return an expression indicating whether a bitmask flag is set.

    Returns
    -------
    ComputeExpression
        Expression indicating whether the mask bit is set.
    """
    roles = cast_expr(values, pa.int64(), safe=False)
    masked = ensure_expression(pc.bit_wise_and(roles, pa.scalar(mask)))
    hit = ensure_expression(pc.not_equal(masked, pa.scalar(0)))
    false_expr = pc.scalar(pa.scalar(value=False))
    return ensure_expression(pc.coalesce(hit, false_expr))


def _compute_array(function_name: str, args: list[object]) -> ArrayLike | ChunkedArrayLike:
    """Call a compute kernel and cast the result to an array-like.

    Parameters
    ----------
    function_name
        Compute function name to invoke.
    args
        Arguments for the compute function.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Kernel result cast to an array-like.
    """
    return cast("ArrayLike | ChunkedArrayLike", pc.call_function(function_name, args))


def trimmed_non_empty_utf8(
    values: ArrayLike,
) -> tuple[ArrayLike | ChunkedArrayLike, ArrayLike | ChunkedArrayLike]:
    """Return (trimmed, non-empty) mask for UTF-8 string arrays.

    Returns
    -------
    tuple[ArrayLike | ChunkedArrayLike, ArrayLike | ChunkedArrayLike]
        Trimmed values and non-empty mask.
    """
    trimmed = _compute_array("utf8_trim_whitespace", [values])
    mask = pc.and_(
        pc.is_valid(trimmed),
        pc.greater(_compute_array("utf8_length", [trimmed]), 0),
    )
    return trimmed, mask


def filter_non_empty_utf8(
    table: TableLike,
    column: str,
) -> tuple[TableLike, ArrayLike | ChunkedArrayLike]:
    """Filter a table to rows with non-empty UTF-8 strings in column.

    Returns
    -------
    tuple[TableLike, ArrayLike | ChunkedArrayLike]
        Filtered table and trimmed values.
    """
    trimmed, mask = trimmed_non_empty_utf8(table[column])
    return table.filter(mask), _compute_array("filter", [trimmed, mask])


_PREDICATE_EXPORTS = (InSet, IsNull, Not, predicate_spec)


__all__ = [
    "CoalesceExpr",
    "CoalesceStringExprSpec",
    "ColumnExpr",
    "ColumnOrNullExpr",
    "ComputeExprSpec",
    "ConstExpr",
    "DefUseKindExprSpec",
    "FieldExpr",
    "TrimExprSpec",
    "bitmask_is_set_expr",
    "coalesce_string_expr",
    "expr_context_expr",
    "expr_context_value",
    "filter_non_empty_utf8",
    "flag_to_bool",
    "flag_to_bool_expr",
    "invalid_id_expr",
    "normalize_string_items",
    "null_expr",
    "null_if_empty_or_zero",
    "position_encoding_expr",
    "predicate_spec",
    "scalar_expr",
    "trimmed_non_empty_expr",
    "trimmed_non_empty_utf8",
    "zero_expr",
]
