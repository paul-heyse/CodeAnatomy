"""Stable identifier helpers for normalization stages."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.core.ids import (
    HashSpec,
    hash64_from_parts,
    hash_column_values,
    hash_expression,
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


def stable_id(prefix: str, *parts: str | None) -> str:
    """Build a deterministic string ID.

    Must remain stable across runs/machines and should be used for any persisted identifiers.

    Returns
    -------
    str
        Stable identifier with the requested prefix.
    """
    hashed = hash64_from_parts(*parts, prefix=prefix)
    return f"{prefix}:{hashed}"


def stable_int64(*parts: str | None) -> int:
    """Build a deterministic signed int64 from parts.

    Useful as compact row ids for Arrow joins if you prefer numeric keys.

    Returns
    -------
    int
        Deterministic signed 64-bit integer.
    """
    return hash64_from_parts(*parts)


def span_id(path: str, bstart: int, bend: int, kind: str | None = None) -> str:
    """Build a stable ID for a source span.

    This is *not required* as a join key (path+bstart+bend is enough),
    but it is useful for:
      - CPG node ids anchored to code
      - reproducible edge ids
      - cache keys

    Returns
    -------
    str
        Stable span identifier.
    """
    if kind:
        return stable_id("span", kind, path, str(bstart), str(bend))
    return stable_id("span", path, str(bstart), str(bend))


def prefixed_hash64(
    prefix: str,
    arrays: list[ArrayLike | ChunkedArrayLike],
) -> ArrayLike | ChunkedArrayLike:
    """Return prefixed hash IDs for the provided arrays.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Prefixed hash identifiers.
    """
    return prefixed_hash_id(arrays, prefix=prefix)


def masked_prefixed_hash(
    prefix: str,
    arrays: list[ArrayLike | ChunkedArrayLike],
    *,
    required: list[ArrayLike | ChunkedArrayLike],
) -> ArrayLike | ChunkedArrayLike:
    """Return prefixed hash IDs masked by required validity.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Prefixed hash identifiers masked by required validity.
    """
    hashed = prefixed_hash_id(arrays, prefix=prefix)
    mask = pc.is_valid(required[0])
    for arr in required[1:]:
        mask = pc.and_(mask, pc.is_valid(arr))
    return pc.if_else(mask, hashed, pa.scalar(None, type=pa.string()))


def masked_hash_expression(
    *,
    spec: HashSpec,
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
    mask = pc.is_valid(pc.field(required[0]))
    for name in required[1:]:
        mask = pc.and_(mask, pc.is_valid(pc.field(name)))
    null_value = pa.scalar(None, type=pa.string() if spec.as_string else pa.int64())
    return ensure_expression(pc.if_else(mask, expr, null_value))


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
    valid = pc.and_(
        pc.is_valid(path_arr),
        pc.and_(pc.is_valid(bstart_arr), pc.is_valid(bend_arr)),
    )
    span_ids = pc.if_else(valid, prefixed, pa.scalar(None, type=pa.string()))
    return table.append_column(out_col, span_ids)
