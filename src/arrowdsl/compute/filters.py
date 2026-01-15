"""Filters, masks, and UDF registration helpers for ArrowDSL."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pyarrow.compute as pac

from arrowdsl.compute.kernel_utils import resolve_kernel
from arrowdsl.compute.macros import (
    bitmask_is_set_expr,
    filter_non_empty_utf8,
    invalid_id_expr,
    null_if_empty_or_zero,
    trimmed_non_empty_utf8,
    zero_expr,
)
from arrowdsl.compute.predicates import (
    predicate_spec,
    valid_mask_array,
    valid_mask_expr,
    valid_mask_for_columns,
)
from arrowdsl.compute.registry import (
    ComputeRegistry,
    UdfSpec,
    default_registry,
    ensure_udf,
    ensure_udfs,
)
from arrowdsl.compute.udf_helpers import (
    ensure_expr_context_udf,
    ensure_position_encoding_udf,
)
from arrowdsl.core.ids import iter_array_values
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    ComputeExpression,
    DataTypeLike,
    ScalarLike,
    TableLike,
)
from arrowdsl.json_factory import JsonPolicy, dumps_text

if TYPE_CHECKING:
    from arrowdsl.compute.expr_core import ExprSpec
    from arrowdsl.plan.plan import Plan

type ValuesLike = ArrayLike | ChunkedArrayLike | ScalarLike


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


def _json_stringify(value: object) -> str | None:
    """Serialize a Python value to a stable JSON string.

    Parameters
    ----------
    value
        Value to serialize.

    Returns
    -------
    str | None
        JSON string or ``None`` for null values.
    """
    if value is None:
        return None
    try:
        policy = JsonPolicy(sort_keys=True)
        return dumps_text(value, policy=policy)
    except (TypeError, ValueError):
        return dumps_text(str(value))


def _json_udf(ctx: pac.UdfContext, values: ValuesLike) -> ValuesLike:
    """UDF that JSON-stringifies scalars or arrays.

    Parameters
    ----------
    ctx
        UDF context (unused).
    values
        Scalar or array values to serialize.

    Returns
    -------
    ValuesLike
        JSON string values.
    """
    _ = ctx
    if isinstance(values, ScalarLike):
        return pa.scalar(_json_stringify(values.as_py()), type=pa.string())
    array_values = values.combine_chunks() if isinstance(values, ChunkedArrayLike) else values
    out = [_json_stringify(value) for value in iter_array_values(array_values)]
    return pa.array(out, type=pa.string())


def position_encoding_array(values: ArrayLike | ChunkedArrayLike) -> ArrayLike:
    """Normalize position encoding values to SCIP enum integers.

    Returns
    -------
    ValuesLike
        Normalized encoding values (int32).
    """
    func_name = ensure_position_encoding_udf()
    casted = pac.cast(values, pa.string(), safe=False)
    return cast("ArrayLike", pac.call_function(func_name, [casted]))


def _json_udf_name(dtype: DataTypeLike) -> str:
    """Return a stable UDF name for a JSON serializer.

    Parameters
    ----------
    dtype
        Data type to tag the UDF name.

    Returns
    -------
    str
        Deterministic UDF name.
    """
    token = hashlib.sha1(str(dtype).encode("utf-8")).hexdigest()[:8]
    return f"to_json_{token}"


def ensure_json_udf(dtype: DataTypeLike) -> str:
    """Ensure a JSON UDF is registered for the provided input type.

    Returns
    -------
    str
        Registered function name.
    """
    name = _json_udf_name(dtype)
    spec = UdfSpec(
        name=name,
        inputs={"value": dtype},
        output=pa.string(),
        fn=_json_udf,
        summary="JSON stringify",
        description=f"Serialize values to JSON for {dtype}.",
    )
    return ensure_udf(spec)


__all__ = [
    "ComputeRegistry",
    "FilterSpec",
    "UdfSpec",
    "bitmask_is_set_expr",
    "default_registry",
    "ensure_expr_context_udf",
    "ensure_json_udf",
    "ensure_position_encoding_udf",
    "ensure_udf",
    "ensure_udfs",
    "filter_non_empty_utf8",
    "invalid_id_expr",
    "null_if_empty_or_zero",
    "position_encoding_array",
    "predicate_spec",
    "resolve_kernel",
    "trimmed_non_empty_utf8",
    "valid_mask_array",
    "valid_mask_expr",
    "valid_mask_for_columns",
    "zero_expr",
]
