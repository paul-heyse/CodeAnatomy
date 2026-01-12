"""Vectorized ID helpers using Arrow hash kernels."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence

import arrowdsl.pyarrow_core as pa
from arrowdsl.compute import pc
from arrowdsl.id_specs import HashSpec, MissingPolicy
from arrowdsl.iter import iter_array_values
from arrowdsl.pyarrow_protocols import (
    ArrayLike,
    ChunkedArrayLike,
    ScalarLike,
    TableLike,
    UdfContext,
)

type ArrayOrScalar = ArrayLike | ChunkedArrayLike | ScalarLike


_HASH64_FUNCTION = "hash64_udf"
_NULL_SEPARATOR = "\x1f"


def _hash64_int(value: str) -> int:
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
    unsigned = int.from_bytes(digest, "big", signed=False)
    return unsigned & ((1 << 63) - 1)


def _hash64_udf(
    ctx: UdfContext,
    array: ArrayLike | ChunkedArrayLike | ScalarLike,
) -> ArrayLike | ScalarLike:
    _ = ctx
    if isinstance(array, ScalarLike):
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
    if isinstance(result, ScalarLike):
        return pa.array([result.as_py()], type=pa.int64())
    return result


def _stringify(array: ArrayLike | ChunkedArrayLike, *, null_sentinel: str) -> ArrayLike:
    text = pc.cast(array, pa.string())
    return pc.fill_null(text, null_sentinel)


def hash64_from_parts(
    *parts: str | None,
    prefix: str | None = None,
    null_sentinel: str = "None",
) -> int:
    """Compute a hash64 from string parts.

    Parameters
    ----------
    *parts:
        String parts to hash. ``None`` values are replaced with ``null_sentinel``.
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
    values: list[str | None] = [prefix] if prefix is not None else []
    values.extend(parts)
    joined = _NULL_SEPARATOR.join(value if value is not None else null_sentinel for value in values)
    return _hash64_int(joined)


def prefixed_hash_id_from_parts(
    prefix: str,
    *parts: str | None,
    null_sentinel: str = "None",
) -> str:
    """Build a prefixed string ID from literal parts.

    Parameters
    ----------
    prefix:
        Prefix for the resulting identifier.
    *parts:
        Parts to hash.
    null_sentinel:
        String used to represent null values.

    Returns
    -------
    str
        Prefixed identifier string.
    """
    hashed = hash64_from_parts(*parts, prefix=prefix, null_sentinel=null_sentinel)
    return f"{prefix}:{hashed}"


def hash64_from_arrays(
    arrays: Sequence[ArrayLike | ChunkedArrayLike],
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


def prefixed_hash_id(
    arrays: Sequence[ArrayLike | ChunkedArrayLike],
    *,
    prefix: str,
    null_sentinel: str = "None",
) -> ArrayLike:
    """Return a prefixed string ID from hashed array inputs.

    Parameters
    ----------
    arrays:
        Arrays to hash.
    prefix:
        Prefix for the resulting string IDs.
    null_sentinel:
        Sentinel value for nulls in the hash input.

    Returns
    -------
    ArrayLike
        String array with prefixed hash IDs.
    """
    hashed = hash64_from_arrays(arrays, prefix=prefix, null_sentinel=null_sentinel)
    hashed_str = pc.cast(hashed, pa.string())
    return pc.binary_join_element_wise(pa.scalar(prefix), hashed_str, ":")


def hash64_from_columns(
    table: TableLike,
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


def hash_column_values(table: TableLike, *, spec: HashSpec) -> ArrayLike:
    """Compute hash column values for a table without appending.

    Parameters
    ----------
    table:
        Input table.
    spec:
        Hash column specification.

    Returns
    -------
    ArrayLike
        Hash values as an array (string or int64 depending on spec).

    Raises
    ------
    KeyError
        Raised when a required column is missing and ``spec.missing="raise"``.
    """
    if spec.extra_literals:
        arrays: list[ArrayLike] = [
            pa.array([literal] * table.num_rows, type=pa.string())
            for literal in spec.extra_literals
        ]
        for col in spec.cols:
            if col in table.column_names:
                arrays.append(table[col])
                continue
            if spec.missing == "null":
                arrays.append(pa.nulls(table.num_rows, type=pa.string()))
                continue
            msg = f"Missing column for hash64: {col!r}."
            raise KeyError(msg)
        hashed = hash64_from_arrays(arrays, prefix=spec.prefix, null_sentinel=spec.null_sentinel)
    else:
        hashed = hash64_from_columns(
            table,
            cols=spec.cols,
            prefix=spec.prefix,
            null_sentinel=spec.null_sentinel,
            missing=spec.missing,
        )
    if spec.as_string:
        hashed = pc.cast(hashed, pa.string())
        hashed = pc.binary_join_element_wise(pa.scalar(spec.prefix), hashed, ":")
    return hashed


def add_hash_column(table: TableLike, *, spec: HashSpec) -> TableLike:
    """Append a hash-based column to a table.

    Parameters
    ----------
    table:
        Input table.
    spec:
        Hash column specification.

    Returns
    -------
    TableLike
        Table with the hash column appended.
    """
    out_col = spec.out_col or f"{spec.prefix}_id"
    if out_col in table.column_names:
        return table
    hashed = hash_column_values(table, spec=spec)
    return table.append_column(out_col, hashed)
