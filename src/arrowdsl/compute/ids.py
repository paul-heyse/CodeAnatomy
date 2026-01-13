"""Hashing helpers that combine ArrowDSL specs with validity masks."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from arrowdsl.compute.masks import valid_mask_array, valid_mask_expr, valid_mask_for_columns
from arrowdsl.core.ids import (
    HashSpec,
    hash64_from_arrays,
    hash_column_values,
    hash_expression,
    hash_expression_from_parts,
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


def masked_hash_expr(
    spec: HashSpec,
    *,
    required: Sequence[str],
    available: Sequence[str] | None = None,
) -> ComputeExpression:
    """Return a hash expression masked by required column validity.

    Returns
    -------
    ComputeExpression
        Masked compute expression for hash IDs.
    """
    expr = hash_expression(spec, available=available)
    if not required:
        return expr
    mask = valid_mask_expr(required, available=available)
    null_value = pa.scalar(None, type=pa.string() if spec.as_string else pa.int64())
    return ensure_expression(pc.if_else(mask, expr, null_value))


def masked_hash_array(
    table: TableLike,
    *,
    spec: HashSpec,
    required: Sequence[str],
) -> ArrayLike | ChunkedArrayLike:
    """Return hash values masked by required column validity.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Masked hash array for the spec.
    """
    hashed = hash_column_values(table, spec=spec)
    if not required:
        return hashed
    mask = valid_mask_for_columns(table, required)
    null_value = pa.scalar(None, type=pa.string() if spec.as_string else pa.int64())
    return pc.if_else(mask, hashed, null_value)


def masked_prefixed_hash(
    prefix: str,
    arrays: Sequence[ArrayLike | ChunkedArrayLike],
    *,
    required: Sequence[ArrayLike | ChunkedArrayLike],
) -> ArrayLike | ChunkedArrayLike:
    """Return prefixed hash IDs masked by required validity arrays.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Prefixed hash IDs masked by required validity.
    """
    hashed = prefixed_hash_id(arrays, prefix=prefix)
    if not required:
        return hashed
    mask = valid_mask_array(required)
    return pc.if_else(mask, hashed, pa.scalar(None, type=pa.string()))


__all__ = [
    "HashSpec",
    "hash64_from_arrays",
    "hash_column_values",
    "hash_expression",
    "hash_expression_from_parts",
    "masked_hash_array",
    "masked_hash_expr",
    "masked_prefixed_hash",
    "prefixed_hash_id",
]
