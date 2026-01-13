"""Stable identifier helpers for normalization stages."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.compute.ids import (
    HashSpec,
    hash_column_values,
    prefixed_hash_id,
)
from arrowdsl.compute.ids import (
    masked_prefixed_hash as compute_masked_prefixed_hash,
)
from arrowdsl.compute.masks import valid_mask_array
from arrowdsl.core.ids import hash64_from_parts
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    TableLike,
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
    return compute_masked_prefixed_hash(prefix, arrays, required=required)


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
