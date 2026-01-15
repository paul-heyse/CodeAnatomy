"""Hashing helpers that combine ArrowDSL specs with validity masks."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.compute.predicates import (
    valid_mask_array,
    valid_mask_expr,
    valid_mask_for_columns,
)
from arrowdsl.core.ids import (
    HashSpec,
    hash64_from_arrays,
    hash64_from_parts,
    hash_column_values,
    hash_expression,
    hash_expression_from_parts,
    prefixed_hash_id,
)
from arrowdsl.core.ids_registry import hash_spec_factory
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


def prefixed_hash64(
    prefix: str,
    arrays: Sequence[ArrayLike | ChunkedArrayLike],
) -> ArrayLike | ChunkedArrayLike:
    """Return prefixed hash IDs for the provided arrays.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Prefixed hash identifiers.
    """
    return prefixed_hash_id(arrays, prefix=prefix)


def _set_or_append_column(table: TableLike, name: str, values: ArrayLike) -> TableLike:
    if name in table.column_names:
        idx = table.schema.get_field_index(name)
        return table.set_column(idx, name, values)
    return table.append_column(name, values)


def apply_hash_column(
    table: TableLike,
    *,
    spec: HashSpec,
    required: Sequence[str] | None = None,
) -> TableLike:
    """Hash columns into a new id column.

    Returns
    -------
    TableLike
        Updated table with hashed id column.
    """
    out_col = spec.out_col or f"{spec.prefix}_id"
    req = required or ()
    hashed = masked_hash_array(table, spec=spec, required=req)
    return _set_or_append_column(table, out_col, hashed)


def apply_hash_columns(
    table: TableLike,
    *,
    specs: Sequence[HashSpec],
    required: Mapping[str, Sequence[str]] | None = None,
) -> TableLike:
    """Apply multiple hash specs to a table.

    Returns
    -------
    TableLike
        Updated table with hashed id columns.
    """
    out = table
    for spec in specs:
        out_col = spec.out_col or f"{spec.prefix}_id"
        req = required.get(out_col) if required else None
        out = apply_hash_column(out, spec=spec, required=req)
    return out


def hash_projection(
    spec: HashSpec,
    *,
    available: Sequence[str] | None = None,
    required: Sequence[str] | None = None,
) -> tuple[ComputeExpression, str]:
    """Return a compute expression and output column for plan-lane hashing.

    Returns
    -------
    tuple[ComputeExpression, str]
        Hash expression and output column name.
    """
    req = required or ()
    expr = masked_hash_expr(spec, required=req, available=available)
    out_col = spec.out_col or f"{spec.prefix}_id"
    return expr, out_col


def stable_id(prefix: str, *parts: str | None) -> str:
    """Build a deterministic string ID.

    Returns
    -------
    str
        Stable identifier with the requested prefix.
    """
    hashed = hash64_from_parts(*parts, prefix=prefix)
    return f"{prefix}:{hashed}"


def stable_int64(*parts: str | None) -> int:
    """Build a deterministic signed int64 from parts.

    Returns
    -------
    int
        Deterministic signed 64-bit integer.
    """
    return hash64_from_parts(*parts)


def span_id(path: str, bstart: int, bend: int, kind: str | None = None) -> str:
    """Build a stable ID for a source span.

    Returns
    -------
    str
        Stable span identifier.
    """
    if kind:
        return stable_id("span", kind, path, str(bstart), str(bend))
    return stable_id("span", path, str(bstart), str(bend))


@dataclass(frozen=True)
class SpanIdSpec:
    """Specification for span_id column generation."""

    path_col: str = "path"
    bstart_col: str = "bstart"
    bend_col: str = "bend"
    kind: str | None = None
    out_col: str = "span_id"


def add_span_id_column(table: TableLike, spec: SpanIdSpec | None = None) -> TableLike:
    """Add a span_id column computed with Arrow hash kernels.

    Returns
    -------
    TableLike
        Table with the span_id column appended.
    """
    spec = spec or SpanIdSpec()
    path_col = spec.path_col
    bstart_col = spec.bstart_col
    bend_col = spec.bend_col
    kind = spec.kind
    out_col = spec.out_col

    if out_col in table.column_names:
        return table
    if path_col in table.column_names:
        path_arr = table[path_col]
    else:
        path_arr = pa.nulls(table.num_rows, type=pa.string())
    if bstart_col in table.column_names:
        bstart_arr = table[bstart_col]
    else:
        bstart_arr = pa.nulls(table.num_rows, type=pa.int64())
    if bend_col in table.column_names:
        bend_arr = table[bend_col]
    else:
        bend_arr = pa.nulls(table.num_rows, type=pa.int64())

    hash_cols = (path_col, bstart_col, bend_col)
    extra = (kind,) if kind is not None else ()
    hash_spec = HashSpec(
        prefix="span",
        cols=hash_cols,
        as_string=True,
        null_sentinel="None",
        extra_literals=extra,
    )
    prefixed = hash_column_values(table, spec=hash_spec)
    valid = valid_mask_array([path_arr, bstart_arr, bend_arr])
    span_ids = pc.if_else(valid, prefixed, pa.scalar(None, type=pa.string()))
    return table.append_column(out_col, span_ids)


__all__ = [
    "HashSpec",
    "SpanIdSpec",
    "add_span_id_column",
    "apply_hash_column",
    "apply_hash_columns",
    "hash64_from_arrays",
    "hash_column_values",
    "hash_expression",
    "hash_expression_from_parts",
    "hash_projection",
    "hash_spec_factory",
    "masked_hash_array",
    "masked_hash_expr",
    "masked_prefixed_hash",
    "prefixed_hash64",
    "prefixed_hash_id",
    "span_id",
    "stable_id",
    "stable_int64",
]
