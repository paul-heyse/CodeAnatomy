"""Stable identifier helpers for normalization stages."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import pyarrow as pa


def stable_id(prefix: str, *parts: str) -> str:
    """Build a deterministic string ID.

    Must remain stable across runs/machines and should be used for any persisted identifiers.

    Returns
    -------
    str
        Stable identifier with the requested prefix.
    """
    h = hashlib.sha1()
    for p in parts:
        h.update(p.encode("utf-8"))
        h.update(b"\x1f")
    return f"{prefix}:{h.hexdigest()}"


def stable_int64(*parts: str) -> int:
    """Build a deterministic signed int64 from parts.

    Useful as compact row ids for Arrow joins if you prefer numeric keys.

    Returns
    -------
    int
        Deterministic signed 64-bit integer.
    """
    h = hashlib.sha1()
    for p in parts:
        h.update(p.encode("utf-8"))
        h.update(b"\x1f")
    # Use 63 bits to avoid negative sign complications in some engines
    v = int.from_bytes(h.digest()[:8], "big", signed=False) & ((1 << 63) - 1)
    return int(v)


def _row_value_int(value: object | None) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


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
    """Add a span_id column computed row-by-row.

    Note: this is Python-loop based; for very large tables you may want
    to compute ids later or in batches.

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

    path_arr = (
        table[path_col].to_pylist() if path_col in table.column_names else [None] * table.num_rows
    )
    bs_arr = (
        table[bstart_col].to_pylist()
        if bstart_col in table.column_names
        else [None] * table.num_rows
    )
    be_arr = (
        table[bend_col].to_pylist() if bend_col in table.column_names else [None] * table.num_rows
    )

    out: list[str | None] = []
    for p, bs, be in zip(path_arr, bs_arr, be_arr, strict=True):
        bs_int = _row_value_int(bs)
        be_int = _row_value_int(be)
        if p is None or bs_int is None or be_int is None:
            out.append(None)
            continue
        out.append(span_id(str(p), bs_int, be_int, kind=kind))

    return table.append_column(out_col, pa.array(out, type=pa.string()))
