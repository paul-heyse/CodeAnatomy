"""Stable identifier helpers for normalization stages."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

from arrowdsl.ids import hash64_from_arrays, hash64_from_parts


def stable_id(prefix: str, *parts: str) -> str:
    """Build a deterministic string ID.

    Must remain stable across runs/machines and should be used for any persisted identifiers.

    Returns
    -------
    str
        Stable identifier with the requested prefix.
    """
    hashed = hash64_from_parts(*parts, prefix=prefix)
    return f"{prefix}:{hashed}"


def stable_int64(*parts: str) -> int:
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


@dataclass(frozen=True)
class SpanIdSpec:
    """Specification for span_id column generation."""

    path_col: str = "path"
    bstart_col: str = "bstart"
    bend_col: str = "bend"
    kind: str | None = None
    out_col: str = "span_id"


def add_span_id_column(table: pa.Table, spec: SpanIdSpec | None = None) -> pa.Table:
    """Add a span_id column computed with Arrow hash kernels.

    Returns
    -------
    pa.Table
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
    arrays: list[pa.Array | pa.ChunkedArray] = []
    if kind is not None:
        arrays.append(pa.array([kind] * table.num_rows, type=pa.string()))
    if path_col in table.column_names:
        arrays.append(table[path_col])
    else:
        arrays.append(pa.nulls(table.num_rows, type=pa.string()))
    if bstart_col in table.column_names:
        arrays.append(table[bstart_col])
    else:
        arrays.append(pa.nulls(table.num_rows, type=pa.int64()))
    if bend_col in table.column_names:
        arrays.append(table[bend_col])
    else:
        arrays.append(pa.nulls(table.num_rows, type=pa.int64()))

    hashed = hash64_from_arrays(arrays, prefix="span")
    hashed_str = pc.cast(hashed, pa.string())
    prefixed = pc.binary_join_element_wise(pa.scalar("span"), hashed_str, ":")
    return table.append_column(out_col, prefixed)
