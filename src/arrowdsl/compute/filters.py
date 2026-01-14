"""Filters, masks, and UDF registration helpers for ArrowDSL."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pyarrow.compute as pac

from arrowdsl.compute.expr_core import ExprSpec, normalize_position_encoding
from arrowdsl.compute.macros import (
    bitmask_is_set_expr,
    filter_non_empty_utf8,
    invalid_id_expr,
    null_if_empty_or_zero,
    predicate_spec,
    trimmed_non_empty_utf8,
    zero_expr,
)
from arrowdsl.core.ids import iter_array_values
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    ComputeExpression,
    DataTypeLike,
    ScalarLike,
    TableLike,
    ensure_expression,
    pc,
)
from arrowdsl.json_factory import JsonPolicy, dumps_text

if TYPE_CHECKING:
    from arrowdsl.plan.plan import Plan

type UdfFn = Callable[..., object]
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


def _false_mask(num_rows: int) -> ArrayLike:
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
        Boolean expression where all columns are valid.

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
        return ensure_expression(pc.is_valid(pc.field(name)))

    mask = _expr_for(cols[0])
    for name in cols[1:]:
        mask = ensure_expression(pc.and_(mask, _expr_for(name)))
    return mask


@dataclass(frozen=True)
class UdfSpec:
    """Specification for a scalar UDF registration."""

    name: str
    inputs: Mapping[str, DataTypeLike]
    output: DataTypeLike
    fn: UdfFn
    summary: str | None = None
    description: str | None = None

    def metadata(self) -> dict[str, str]:
        """Return metadata for UDF registration.

        Returns
        -------
        dict[str, str]
            Metadata mapping for the UDF.
        """
        meta: dict[str, str] = {}
        if self.summary:
            meta["summary"] = self.summary
        if self.description:
            meta["description"] = self.description
        return meta


@dataclass
class ComputeRegistry:
    """Registry for cached compute UDF registrations."""

    registered: set[str] = field(default_factory=set)

    def ensure(self, spec: UdfSpec) -> str:
        """Ensure a UDF is registered and return its name.

        Returns
        -------
        str
            Registered UDF name.
        """
        if spec.name in self.registered:
            return spec.name
        try:
            pc.get_function(spec.name)
        except KeyError:
            pc.register_scalar_function(
                spec.fn,
                spec.name,
                spec.metadata(),
                spec.inputs,
                spec.output,
            )
        self.registered.add(spec.name)
        return spec.name


_DEFAULT_REGISTRY = ComputeRegistry()


def default_registry() -> ComputeRegistry:
    """Return the default compute registry.

    Returns
    -------
    ComputeRegistry
        Shared compute registry instance.
    """
    return _DEFAULT_REGISTRY


def ensure_udf(spec: UdfSpec) -> str:
    """Ensure a UDF is registered in the default registry.

    Returns
    -------
    str
        Registered UDF name.
    """
    return default_registry().ensure(spec)


def ensure_udfs(specs: Sequence[UdfSpec]) -> tuple[str, ...]:
    """Ensure a sequence of UDFs are registered in the default registry.

    Returns
    -------
    tuple[str, ...]
        Registered UDF names in input order.
    """
    return tuple(ensure_udf(spec) for spec in specs)


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
    if value is None:
        return None
    try:
        policy = JsonPolicy(sort_keys=True)
        return dumps_text(value, policy=policy)
    except (TypeError, ValueError):
        return dumps_text(str(value))


def _json_udf(ctx: pac.UdfContext, values: ValuesLike) -> ValuesLike:
    _ = ctx
    if isinstance(values, ScalarLike):
        return pa.scalar(_json_stringify(values.as_py()), type=pa.string())
    array_values = values.combine_chunks() if isinstance(values, ChunkedArrayLike) else values
    out = [_json_stringify(value) for value in iter_array_values(array_values)]
    return pa.array(out, type=pa.string())


def _normalize_expr_ctx(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    return text.upper() or None


def _expr_ctx_udf(ctx: pac.UdfContext, values: ValuesLike) -> ValuesLike:
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
