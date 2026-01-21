"""Centralized PyArrow compute access for DataFusion integrations."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, cast, overload

import pyarrow as pa
import pyarrow.compute as _pc

from arrowdsl.core.interop import ComputeModule

if TYPE_CHECKING:
    from arrowdsl.core.interop import (
        ArrayLike,
        ChunkedArrayLike,
        ComputeExpression,
        ScalarLike,
        TableLike,
    )

    type ArrayInput = ArrayLike | ChunkedArrayLike
    type ScalarInput = ScalarLike | str | bytes | int | float | bool
    type ArrayOrScalar = ArrayInput | ScalarInput
    type ExprOrScalar = ComputeExpression | ScalarInput
    type CallResult = ArrayLike | ChunkedArrayLike | ScalarLike

pc = cast("ComputeModule", _pc)
SortOptions = _pc.SortOptions


def list_functions() -> list[str]:
    """Return the list of registered compute function names.

    Returns
    -------
    list[str]
        Compute function names available in the runtime.
    """
    return _pc.list_functions()


@overload
def cast_values(values: ArrayInput, dtype: object, *, safe: bool = True) -> ArrayLike: ...


@overload
def cast_values(
    values: ComputeExpression, dtype: object, *, safe: bool = True
) -> ComputeExpression: ...


def cast_values(
    values: ArrayInput | ComputeExpression, dtype: object, *, safe: bool = True
) -> object:
    """Cast array-like values to a target dtype.

    Returns
    -------
    object
        Casted values.
    """
    return pc.cast(values, dtype, safe=safe)


@overload
def equal(left: ArrayInput, right: ArrayOrScalar) -> ArrayLike: ...


@overload
def equal(left: ComputeExpression, right: ExprOrScalar) -> ComputeExpression: ...


def equal(left: ArrayInput | ComputeExpression, right: ArrayOrScalar | ExprOrScalar) -> object:
    """Return element-wise equality.

    Returns
    -------
    object
        Equality mask or expression.
    """
    return pc.equal(left, right)


@overload
def not_equal(left: ArrayInput, right: ArrayOrScalar) -> ArrayLike: ...


@overload
def not_equal(left: ComputeExpression, right: ExprOrScalar) -> ComputeExpression: ...


def not_equal(left: ArrayInput | ComputeExpression, right: ArrayOrScalar | ExprOrScalar) -> object:
    """Return element-wise inequality.

    Returns
    -------
    object
        Inequality mask or expression.
    """
    return pc.not_equal(left, right)


@overload
def greater(left: ArrayInput, right: ArrayOrScalar) -> ArrayLike: ...


@overload
def greater(left: ComputeExpression, right: ExprOrScalar) -> ComputeExpression: ...


def greater(left: ArrayInput | ComputeExpression, right: ArrayOrScalar | ExprOrScalar) -> object:
    """Return element-wise greater-than comparison.

    Returns
    -------
    object
        Greater-than mask or expression.
    """
    return pc.greater(left, right)


@overload
def is_null(values: ArrayInput) -> ArrayLike: ...


@overload
def is_null(values: ComputeExpression) -> ComputeExpression: ...


def is_null(values: ArrayInput | ComputeExpression) -> object:
    """Return element-wise null mask.

    Returns
    -------
    object
        Null mask or expression.
    """
    return pc.is_null(values)


@overload
def is_valid(values: ArrayInput) -> ArrayLike: ...


@overload
def is_valid(values: ComputeExpression) -> ComputeExpression: ...


def is_valid(values: ArrayInput | ComputeExpression) -> object:
    """Return element-wise validity mask.

    Returns
    -------
    object
        Validity mask or expression.
    """
    return pc.is_valid(values)


@overload
def and_(left: ArrayInput, right: ArrayInput) -> ArrayLike: ...


@overload
def and_(left: ComputeExpression, right: ComputeExpression) -> ComputeExpression: ...


def and_(left: ArrayInput | ComputeExpression, right: ArrayInput | ComputeExpression) -> object:
    """Return element-wise logical AND.

    Returns
    -------
    object
        AND mask or expression.
    """
    return pc.and_(left, right)


@overload
def or_(left: ArrayInput, right: ArrayInput) -> ArrayLike: ...


@overload
def or_(left: ComputeExpression, right: ComputeExpression) -> ComputeExpression: ...


def or_(left: ArrayInput | ComputeExpression, right: ArrayInput | ComputeExpression) -> object:
    """Return element-wise logical OR.

    Returns
    -------
    object
        OR mask or expression.
    """
    return pc.or_(left, right)


@overload
def invert(values: ArrayInput) -> ArrayLike: ...


@overload
def invert(values: ComputeExpression) -> ComputeExpression: ...


def invert(values: ArrayInput | ComputeExpression) -> object:
    """Return element-wise logical NOT.

    Returns
    -------
    object
        Inverted mask or expression.
    """
    return pc.invert(values)


@overload
def if_else(cond: ArrayInput, left: ArrayOrScalar, right: ArrayOrScalar) -> ArrayLike: ...


@overload
def if_else(
    cond: ComputeExpression, left: ExprOrScalar, right: ExprOrScalar
) -> ComputeExpression: ...


def if_else(
    cond: ArrayInput | ComputeExpression,
    left: ArrayOrScalar | ExprOrScalar,
    right: ArrayOrScalar | ExprOrScalar,
) -> object:
    """Return element-wise conditional selection.

    Returns
    -------
    object
        Selected values or expression.
    """
    return pc.if_else(cond, left, right)


@overload
def coalesce(*values: ArrayInput) -> ArrayLike: ...


@overload
def coalesce(*values: ComputeExpression) -> ComputeExpression: ...


def coalesce(*values: ArrayInput | ComputeExpression) -> object:
    """Return first non-null value.

    Returns
    -------
    object
        Coalesced values or expression.
    """
    return pc.coalesce(*values)


def fill_null(values: ArrayInput, *, fill_value: object) -> ArrayLike:
    """Fill null values with a scalar.

    Returns
    -------
    object
        Values with nulls filled.
    """
    return pc.fill_null(values, fill_value=fill_value)


def drop_null(values: ArrayInput) -> ArrayLike:
    """Drop null values from array-like inputs.

    Returns
    -------
    object
        Values without nulls.
    """
    return pc.drop_null(values)


@overload
def struct_field(values: ArrayInput, field: str | int) -> ArrayLike: ...


@overload
def struct_field(values: ComputeExpression, field: str | int) -> ComputeExpression: ...


def struct_field(values: ArrayInput | ComputeExpression, field: str | int) -> object:
    """Return a struct field from a struct array.

    Returns
    -------
    object
        Struct field values.
    """
    return pc.struct_field(values, field)


def filter_values(values: ArrayInput, mask: ArrayInput) -> ArrayLike:
    """Filter values by a boolean mask.

    Returns
    -------
    object
        Filtered values.
    """
    return pc.filter(values, mask)


def is_in(values: ArrayInput, *, value_set: Iterable[object]) -> ArrayLike:
    """Return element-wise membership mask.

    Returns
    -------
    object
        Membership mask or expression.
    """
    return pc.is_in(values, value_set=value_set)


def unique(values: ArrayInput) -> ArrayLike:
    """Return distinct values.

    Returns
    -------
    object
        Distinct values.
    """
    return pc.unique(values)


def case_when(conditions: Sequence[tuple[ArrayInput, object]], default: object) -> ArrayLike:
    """Return a case/when expression result.

    Returns
    -------
    object
        Case expression result.
    """
    return pc.case_when(list(conditions), default)


def take(values: ArrayInput, indices: ArrayLike) -> ArrayLike:
    """Take values at indices.

    Returns
    -------
    object
        Taken values.
    """
    return pc.take(values, indices)


def list_parent_indices(values: ArrayInput) -> ArrayLike:
    """Return parent indices for list arrays.

    Returns
    -------
    object
        Parent indices array.
    """
    return pc.list_parent_indices(values)


def list_flatten(values: ArrayInput) -> ArrayLike:
    """Flatten list arrays.

    Returns
    -------
    object
        Flattened list values.
    """
    return pc.list_flatten(values)


def list_value_length(values: ArrayInput) -> ArrayLike:
    """Return lengths of list values.

    Returns
    -------
    object
        List length values.
    """
    return pc.list_value_length(values)


def value_counts(values: ArrayInput) -> ArrayLike:
    """Return value counts for array-like inputs.

    Returns
    -------
    object
        Value counts.
    """
    return pc.value_counts(values)


def cumulative_sum(values: ArrayInput) -> ArrayLike:
    """Return cumulative sum for numeric arrays.

    Returns
    -------
    object
        Cumulative sum values.
    """
    return pc.cumulative_sum(values)


def dictionary_encode(values: ArrayInput) -> ArrayLike:
    """Return dictionary-encoded values.

    Returns
    -------
    object
        Dictionary-encoded values.
    """
    return pc.dictionary_encode(values)


def any_values(values: ArrayInput) -> ScalarLike:
    """Return whether any values are True.

    Returns
    -------
    object
        Aggregated boolean value.
    """
    return pc.any(values)


def field(name: str) -> ComputeExpression:
    """Return a compute expression field reference.

    Returns
    -------
    object
        Field expression.
    """
    return pc.field(name)


def scalar(value: object) -> ComputeExpression:
    """Return a compute expression scalar literal.

    Returns
    -------
    object
        Scalar expression.
    """
    return pc.scalar(value)


def sort_indices(
    values: ArrayInput | TableLike,
    *,
    options: object | None = None,
    sort_keys: object | None = None,
) -> ArrayLike:
    """Return sort indices for values.

    Returns
    -------
    object
        Sort indices array.
    """
    return pc.sort_indices(values, options=options, sort_keys=sort_keys)


def binary_join_element_wise(*values: ArrayOrScalar) -> ArrayLike:
    """Join values element-wise with a separator.

    Returns
    -------
    object
        Joined values.
    """
    return pc.binary_join_element_wise(*values)


def call_function(name: str, args: Sequence[ArrayOrScalar]) -> CallResult:
    """Call a named compute function.

    Returns
    -------
    object
        Compute function result.
    """
    return pc.call_function(name, list(args))


def function_options_type() -> type[object] | None:
    """Return the pyarrow.compute FunctionOptions type when available.

    Returns
    -------
    type[object] | None
        FunctionOptions type if present.
    """
    return getattr(pc, "FunctionOptions", None)


def flatten_list_struct_field(
    table: TableLike,
    *,
    list_col: str,
    field: str,
) -> ArrayLike:
    """Flatten a list<struct> column and return a field array.

    Returns
    -------
    ArrayLike
        Flattened field values.

    Raises
    ------
    TypeError
        Raised when the list column does not contain struct values.
    """
    column = table[list_col]
    if isinstance(column, pa.ChunkedArray):
        column = column.combine_chunks()
    values = getattr(column, "values", None)
    if values is None:
        msg = f"Column {list_col!r} does not expose list values."
        raise TypeError(msg)
    if not isinstance(values, pa.StructArray):
        msg = f"Column {list_col!r} does not contain struct values."
        raise TypeError(msg)
    return values.field(field)


__all__ = [
    "SortOptions",
    "and_",
    "any_values",
    "binary_join_element_wise",
    "call_function",
    "case_when",
    "cast_values",
    "coalesce",
    "cumulative_sum",
    "dictionary_encode",
    "drop_null",
    "equal",
    "field",
    "filter_values",
    "flatten_list_struct_field",
    "function_options_type",
    "greater",
    "if_else",
    "invert",
    "is_in",
    "is_null",
    "is_valid",
    "list_flatten",
    "list_functions",
    "list_parent_indices",
    "list_value_length",
    "not_equal",
    "or_",
    "pc",
    "scalar",
    "sort_indices",
    "struct_field",
    "take",
    "unique",
    "value_counts",
]
