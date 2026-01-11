"""Vectorized ID helpers using Arrow hash kernels."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import pyarrow as pa
import pyarrow.compute as pc

from arrowdsl.iter import iter_array_values

type ArrayLike = pa.Array | pa.ChunkedArray
type ArrayOrScalar = ArrayLike | pa.Scalar
type MissingPolicy = Literal["raise", "null"]


@dataclass(frozen=True)
class Hash64ColumnSpec:
    """Specification for appending a hash64-derived column."""

    cols: Sequence[str]
    out_col: str
    prefix: str | None = None
    as_string: bool = False
    null_sentinel: str = "None"
    missing: MissingPolicy = "raise"


_HASH64_FUNCTION = "hash64_udf"
_NULL_SEPARATOR = "\x1f"


def _hash64_int(value: str) -> int:
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
    unsigned = int.from_bytes(digest, "big", signed=False)
    return unsigned & ((1 << 63) - 1)


def _hash64_udf(
    ctx: pc.UdfContext,
    array: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    _ = ctx
    if isinstance(array, pa.Scalar):
        value = array.as_py()
        if value is None:
            return pa.scalar(None, type=pa.int64())
        return pa.scalar(_hash64_int(str(value)), type=pa.int64())
    out = [
        _hash64_int(str(value)) if value is not None else None for value in iter_array_values(array)
    ]
    return pa.array(out, type=pa.int64())


def _ensure_hash64_udf() -> None:
    try:
        pc.get_function(_HASH64_FUNCTION)
    except KeyError:
        pass
    else:
        return
    pc.register_scalar_function(
        _hash64_udf,
        _HASH64_FUNCTION,
        {"summary": "hash64 udf", "description": "Deterministic 64-bit hash for strings."},
        {"value": pa.string()},
        pa.int64(),
    )


def _hash64(values: ArrayLike) -> ArrayLike:
    _ensure_hash64_udf()
    result = pc.call_function(_HASH64_FUNCTION, [values])
    if isinstance(result, pa.Scalar):
        return pa.array([result.as_py()], type=pa.int64())
    return result


def _stringify(array: ArrayLike, *, null_sentinel: str) -> pa.Array:
    text = pc.cast(array, pa.string())
    return pc.fill_null(text, null_sentinel)


def hash64_from_parts(
    *parts: str,
    prefix: str | None = None,
    null_sentinel: str = "None",
) -> int:
    """Compute a hash64 from string parts.

    Parameters
    ----------
    *parts:
        String parts to hash.
    prefix:
        Optional prefix included in the hash input.
    null_sentinel:
        String used to represent null values.

    Returns
    -------
    int
        Deterministic signed int64 hash.

    Raises
    ------
    ValueError
        Raised when no parts or prefix are provided.
    """
    if not parts and prefix is None:
        msg = "hash64_from_parts requires at least one part or prefix."
        raise ValueError(msg)
    values = [prefix] if prefix is not None else []
    values.extend(parts)
    joined = _NULL_SEPARATOR.join(value if value is not None else null_sentinel for value in values)
    return _hash64_int(joined)


def hash64_from_arrays(
    arrays: Sequence[ArrayLike],
    *,
    prefix: str | None = None,
    null_sentinel: str = "None",
) -> ArrayLike:
    """Compute a hash64 from multiple arrays using a stable separator.

    Parameters
    ----------
    arrays:
        Arrays to join and hash.
    prefix:
        Optional prefix included in the hash input.
    null_sentinel:
        String used to represent null values.

    Returns
    -------
    pyarrow.Array
        Int64 hash array.

    Raises
    ------
    ValueError
        Raised when no input arrays are provided.
    """
    if not arrays:
        msg = "hash64_from_arrays requires at least one input array."
        raise ValueError(msg)
    parts: list[ArrayOrScalar] = [_stringify(arr, null_sentinel=null_sentinel) for arr in arrays]
    if prefix is not None:
        parts.insert(0, pa.scalar(prefix))
    joined = pc.binary_join_element_wise(*parts, _NULL_SEPARATOR)
    return _hash64(joined)


def hash64_from_columns(
    table: pa.Table,
    *,
    cols: Sequence[str],
    prefix: str | None = None,
    null_sentinel: str = "None",
    missing: MissingPolicy = "raise",
) -> ArrayLike:
    """Compute a hash64 array from table columns.

    Parameters
    ----------
    table:
        Input table.
    cols:
        Column names to hash.
    prefix:
        Optional prefix included in the hash input.
    null_sentinel:
        String used to represent null values.
    missing:
        How to handle missing columns ("raise" or "null").

    Returns
    -------
    pyarrow.Array
        Int64 hash array.

    Raises
    ------
    ValueError
        Raised when no column names are provided.
    KeyError
        Raised when a required column is missing and ``missing="raise"``.
    """
    if not cols:
        msg = "hash64_from_columns requires at least one column."
        raise ValueError(msg)
    arrays: list[ArrayLike] = []
    for col in cols:
        if col in table.column_names:
            arrays.append(table[col])
            continue
        if missing == "null":
            arrays.append(pa.nulls(table.num_rows, type=pa.string()))
            continue
        msg = f"Missing column for hash64: {col!r}."
        raise KeyError(msg)
    return hash64_from_arrays(arrays, prefix=prefix, null_sentinel=null_sentinel)


def add_hash64_column(table: pa.Table, *, spec: Hash64ColumnSpec) -> pa.Table:
    """Append a hash64-derived column to a table.

    Parameters
    ----------
    table:
        Input table.
    spec:
        Hash column specification.

    Returns
    -------
    pyarrow.Table
        Table with the hash column appended.
    """
    if spec.out_col in table.column_names:
        return table
    hashed = hash64_from_columns(
        table,
        cols=spec.cols,
        prefix=spec.prefix,
        null_sentinel=spec.null_sentinel,
        missing=spec.missing,
    )
    if spec.as_string:
        hashed = pc.cast(hashed, pa.string())
    return table.append_column(spec.out_col, hashed)
