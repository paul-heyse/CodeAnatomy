"""Ibis helpers for stable hash/id expressions."""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import ibis
import ibis.expr.datashape as ds
import ibis.expr.datatypes as dt
from ibis.expr import operations as ops
from ibis.expr.types import StringValue, Value

from ibis_engine.builtin_udfs import prefixed_hash64, stable_hash64, stable_hash128, stable_id

HASH_SEPARATOR = "\x1f"


def stable_hash64_expr(
    *parts: Value | str | None,
    prefix: str | None = None,
    null_sentinel: str = "None",
) -> Value:
    """Return a stable hash64 expression from string parts.

    Returns
    -------
    ibis.expr.types.Value
        Stable hash expression.
    """
    values = _parts_with_prefix(parts, prefix=prefix, null_sentinel=null_sentinel)
    joined = _join_with_separator(values)
    return stable_hash64(joined)


def stable_hash128_expr(
    *parts: Value | str | None,
    prefix: str | None = None,
    null_sentinel: str = "None",
) -> Value:
    """Return a stable hash128 expression from string parts.

    Returns
    -------
    ibis.expr.types.Value
        Stable hash expression.
    """
    values = _parts_with_prefix(parts, prefix=prefix, null_sentinel=null_sentinel)
    joined = _join_with_separator(values)
    return stable_hash128(joined)


def stable_id_expr(
    prefix: str,
    *parts: Value | str | None,
    null_sentinel: str = "None",
) -> Value:
    """Return a prefixed stable id expression.

    Returns
    -------
    ibis.expr.types.Value
        Stable id expression.
    """
    key = stable_key_expr(*parts, prefix=None, null_sentinel=null_sentinel)
    prefix_expr = cast("StringValue", ibis.literal(prefix))
    return stable_id(prefix_expr, key)


def stable_key_expr(
    *parts: Value | str | None,
    prefix: str | None = None,
    null_sentinel: str = "None",
) -> Value:
    """Return the natural key string used for stable hashing.

    Returns
    -------
    ibis.expr.types.Value
        Joined key expression prior to hashing.
    """
    values = _parts_with_prefix(parts, prefix=prefix, null_sentinel=null_sentinel)
    return _join_with_separator(values)


def stable_key_hash_expr(
    prefix: str,
    *parts: Value | str | None,
    null_sentinel: str = "None",
) -> Value:
    """Return a hashed key using the Rust prefixed_hash64 UDF.

    Returns
    -------
    ibis.expr.types.Value
        Hashed key expression.
    """
    values = _parts_with_prefix(parts, prefix=None, null_sentinel=null_sentinel)
    joined = _join_with_separator(values)
    prefix_expr = cast("StringValue", ibis.literal(prefix))
    return prefixed_hash64(prefix_expr, joined)


def masked_stable_id_expr(
    prefix: str,
    *,
    parts: Sequence[Value | str | None],
    required: Sequence[Value],
    null_sentinel: str = "None",
) -> Value:
    """Return a stable id expression masked by required column validity.

    Returns
    -------
    ibis.expr.types.Value
        Stable id expression masked by required column validity.
    """
    stable = stable_id_expr(prefix, *parts, null_sentinel=null_sentinel)
    if not required:
        return stable
    mask = _valid_mask(required)
    null_value = ibis.literal(None, type="string")
    return ibis.ifelse(mask, stable, null_value)


def _valid_mask(required: Sequence[Value]) -> Value:
    mask = required[0].notnull()
    for value in required[1:]:
        mask &= value.notnull()
    return mask


def _parts_with_prefix(
    parts: Sequence[Value | str | None],
    *,
    prefix: str | None,
    null_sentinel: str,
) -> list[StringValue]:
    values: list[StringValue] = []
    if prefix is not None:
        values.append(cast("StringValue", ibis.literal(prefix)))
    values.extend(_stringify(part, null_sentinel=null_sentinel) for part in parts)
    if not values:
        msg = "stable hash expressions require at least one part or prefix."
        raise ValueError(msg)
    return values


def _stringify(value: Value | str | None, *, null_sentinel: str) -> StringValue:
    if isinstance(value, Value):
        expr = value.cast("string")
    elif value is None:
        expr = ibis.literal(None, type="string")
    else:
        expr = ibis.literal(str(value))
    sentinel = ibis.literal(null_sentinel)
    return cast("StringValue", ibis.coalesce(expr, sentinel))


def _join_with_separator(parts: Sequence[StringValue]) -> StringValue:
    if len(parts) == 1:
        return parts[0]
    sep_value = cast("ops.Value[dt.String, ds.DataShape]", ibis.literal(HASH_SEPARATOR).op())
    resolved_parts = tuple(cast("ops.Value[dt.String, ds.DataShape]", part.op()) for part in parts)
    return cast("StringValue", ops.StringJoin(resolved_parts, sep_value).to_expr())


def _concat_values(parts: Sequence[StringValue]) -> StringValue:
    if not parts:
        msg = "concat requires at least one part."
        raise ValueError(msg)
    result = parts[0]
    for part in parts[1:]:
        result = result.concat(part)
    return result


__all__ = [
    "HASH_SEPARATOR",
    "masked_stable_id_expr",
    "stable_hash64_expr",
    "stable_hash128_expr",
    "stable_id_expr",
    "stable_key_expr",
    "stable_key_hash_expr",
]
