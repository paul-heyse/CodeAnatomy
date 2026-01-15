"""Compute UDF helpers shared across plan and kernel lanes."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pac

from arrowdsl.compute.position_encoding import normalize_position_encoding
from arrowdsl.compute.registry import UdfSpec, ensure_udf
from arrowdsl.core.ids import iter_array_values
from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike, ScalarLike

type ValuesLike = ArrayLike | ChunkedArrayLike | ScalarLike

_EXPR_CTX_FUNCTION = "expr_ctx_norm"
_POSITION_ENCODING_FUNCTION = "position_encoding_norm"


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


__all__ = [
    "ensure_expr_context_udf",
    "ensure_position_encoding_udf",
]
