"""Filters, masks, and UDF registration helpers for ArrowDSL."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pyarrow.compute as pac

from arrowdsl.compute.expr_core import ExprSpec, normalize_position_encoding
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
from arrowdsl.core.ids import iter_array_values
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    ComputeExpression,
    DataTypeLike,
    ScalarLike,
    TableLike,
    pc,
)
from arrowdsl.json_factory import JsonPolicy, dumps_text

if TYPE_CHECKING:
    from arrowdsl.plan.plan import Plan

type ValuesLike = ArrayLike | ChunkedArrayLike | ScalarLike

_EXPR_CTX_FUNCTION = "expr_ctx_norm"
_POSITION_ENCODING_FUNCTION = "position_encoding_norm"


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


def resolve_kernel(
    name: str,
    *,
    fallbacks: Sequence[str] = (),
    required: bool = False,
) -> str | None:
    """Resolve a compute kernel name from candidates.

    Returns
    -------
    str | None
        Resolved kernel name or ``None`` when unavailable and not required.

    Raises
    ------
    KeyError
        Raised when no candidate kernels exist and ``required=True``.
    """
    for candidate in (name, *fallbacks):
        try:
            pc.get_function(candidate)
        except KeyError:
            continue
        return candidate
    if required:
        msg = f"Missing compute kernel: {name!r}."
        raise KeyError(msg)
    return None


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


def _normalize_expr_ctx(value: object) -> str | None:
    """Normalize expression context strings to upper-case tokens.

    Parameters
    ----------
    value
        Input value to normalize.

    Returns
    -------
    str | None
        Normalized token or ``None`` when input is not usable.
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    return text.upper() or None


def _expr_ctx_udf(ctx: pac.UdfContext, values: ValuesLike) -> ValuesLike:
    """UDF that normalizes expression context values.

    Parameters
    ----------
    ctx
        UDF context (unused).
    values
        Scalar or array values to normalize.

    Returns
    -------
    ValuesLike
        Normalized string values.
    """
    _ = ctx
    if isinstance(values, ScalarLike):
        return pa.scalar(_normalize_expr_ctx(values.as_py()), type=pa.string())
    array_values = values.combine_chunks() if isinstance(values, ChunkedArrayLike) else values
    out = [_normalize_expr_ctx(value) for value in iter_array_values(array_values)]
    return pa.array(out, type=pa.string())


def ensure_expr_context_udf() -> str:
    """Ensure the expr-context normalization UDF is registered.

    Returns
    -------
    str
        Registered function name.
    """
    spec = UdfSpec(
        name=_EXPR_CTX_FUNCTION,
        inputs={"value": pa.string()},
        output=pa.string(),
        fn=_expr_ctx_udf,
        summary="Normalize expr context",
        description="Normalize expr context values.",
    )
    return ensure_udf(spec)


def _position_encoding_udf(ctx: pac.UdfContext, values: ValuesLike) -> ValuesLike:
    """UDF that normalizes position encoding values to int32.

    Parameters
    ----------
    ctx
        UDF context (unused).
    values
        Scalar or array values to normalize.

    Returns
    -------
    ValuesLike
        Normalized int32 values.
    """
    _ = ctx
    if isinstance(values, ScalarLike):
        normalized = normalize_position_encoding(values.as_py())
        return pa.scalar(normalized, type=pa.int32())
    array_values = values.combine_chunks() if isinstance(values, ChunkedArrayLike) else values
    out = [normalize_position_encoding(value) for value in iter_array_values(array_values)]
    return pa.array(out, type=pa.int32())


def ensure_position_encoding_udf() -> str:
    """Ensure the position-encoding normalization UDF is registered.

    Returns
    -------
    str
        Registered function name.
    """
    spec = UdfSpec(
        name=_POSITION_ENCODING_FUNCTION,
        inputs={"value": pa.string()},
        output=pa.int32(),
        fn=_position_encoding_udf,
        summary="Normalize position encodings",
        description="Normalize position encodings to SCIP enum integers.",
    )
    return ensure_udf(spec)


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
