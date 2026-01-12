"""User-defined compute functions for Arrow DSL."""

from __future__ import annotations

import hashlib
import json

import pyarrow as pa
import pyarrow.compute as pac

from arrowdsl.core.ids import iter_array_values
from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike, DataTypeLike, ScalarLike, pc

type ValuesLike = ArrayLike | ChunkedArrayLike | ScalarLike

_EXPR_CTX_FUNCTION = "expr_ctx_norm"
_JSON_CACHE: dict[str, str] = {}


def _json_stringify(value: object) -> str | None:
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except (TypeError, ValueError):
        return json.dumps(str(value), ensure_ascii=False)


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
    try:
        pc.get_function(_EXPR_CTX_FUNCTION)
    except KeyError:
        pc.register_scalar_function(
            _expr_ctx_udf,
            _EXPR_CTX_FUNCTION,
            {"summary": "Normalize expr context", "description": "Normalize expr context values."},
            {"value": pa.string()},
            pa.string(),
        )
    return _EXPR_CTX_FUNCTION


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
    cached = _JSON_CACHE.get(str(dtype))
    if cached == name:
        return name
    try:
        pc.get_function(name)
    except KeyError:
        pc.register_scalar_function(
            _json_udf,
            name,
            {
                "summary": "JSON stringify",
                "description": f"Serialize values to JSON for {dtype}.",
            },
            {"value": dtype},
            pa.string(),
        )
    _JSON_CACHE[str(dtype)] = name
    return name


__all__ = ["ensure_expr_context_udf", "ensure_json_udf"]
