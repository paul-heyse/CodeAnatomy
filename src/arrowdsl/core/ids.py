"""Arrow-native iteration helpers and hash ID specs."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Literal

import arrowdsl.core.interop as pa

type MissingPolicy = Literal["raise", "null"]


type ArrayOrScalar = pa.ArrayLike | pa.ChunkedArrayLike | pa.ScalarLike


@dataclass(frozen=True)
class HashSpec:
    """Specification for a hash-based ID column.

    Parameters
    ----------
    prefix:
        Prefix for the ID string.
    cols:
        Columns included in the hash input.
    extra_literals:
        Literal string parts to include in the hash input.
    as_string:
        When ``True``, return a prefixed string ID; otherwise an int64 hash.
    null_sentinel:
        Sentinel value for nulls in the hash input.
    out_col:
        Output column name override.
    """

    prefix: str
    cols: tuple[str, ...]
    extra_literals: tuple[str, ...] = ()
    as_string: bool = True
    null_sentinel: str = "__NULL__"
    out_col: str | None = None
    missing: MissingPolicy = "raise"


_HASH64_FUNCTION = "hash64_udf"
_NULL_SEPARATOR = "\x1f"


def iter_array_values(array: pa.ArrayLike) -> Iterator[object | None]:
    """Yield native Python values for an Arrow array.

    Parameters
    ----------
    array
        Arrow array or chunked array to iterate.

    Yields
    ------
    object | None
        Native Python values for each element.
    """
    for value in array:
        if isinstance(value, pa.ScalarLike):
            yield value.as_py()
        else:
            yield value


def iter_arrays(arrays: Sequence[pa.ArrayLike]) -> Iterator[tuple[object | None, ...]]:
    """Iterate arrays row-wise in lockstep.

    Parameters
    ----------
    arrays
        Arrays to iterate together.

    Yields
    ------
    tuple[object | None, ...]
        Row-wise tuples of values.
    """
    iters = [iter_array_values(array) for array in arrays]
    yield from zip(*iters, strict=True)


def iter_table_rows(table: pa.TableLike) -> Iterator[dict[str, object]]:
    """Iterate rows as dicts without materializing a full list.

    Parameters
    ----------
    table
        Table to iterate row-wise.

    Yields
    ------
    dict[str, object]
        Mapping of column name to row value.
    """
    columns = list(table.column_names)
    arrays = [table[col] for col in columns]
    iters = [iter_array_values(array) for array in arrays]
    for values in zip(*iters, strict=True):
        yield dict(zip(columns, values, strict=True))


def _hash64_int(value: str) -> int:
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
    unsigned = int.from_bytes(digest, "big", signed=False)
    return unsigned & ((1 << 63) - 1)


def _hash64_udf(
    ctx: pa.UdfContext,
    array: pa.ArrayLike | pa.ChunkedArrayLike | pa.ScalarLike,
) -> pa.ArrayLike | pa.ScalarLike:
    _ = ctx
    if isinstance(array, pa.ScalarLike):
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
        pa.pc.get_function(_HASH64_FUNCTION)
    except KeyError:
        pass
    else:
        return
    pa.pc.register_scalar_function(
        _hash64_udf,
        _HASH64_FUNCTION,
        {"summary": "hash64 udf", "description": "Deterministic 64-bit hash for strings."},
        {"value": pa.string()},
        pa.int64(),
    )


def _hash64(values: pa.ArrayLike) -> pa.ArrayLike:
    _ensure_hash64_udf()
    result = pa.pc.call_function(_HASH64_FUNCTION, [values])
    if isinstance(result, pa.ScalarLike):
        return pa.array([result.as_py()], type=pa.int64())
    return result


def _stringify(
    array: pa.ArrayLike | pa.ChunkedArrayLike,
    *,
    null_sentinel: str,
) -> pa.ArrayLike:
    text = pa.pc.cast(array, pa.string())
    return pa.pc.fill_null(text, null_sentinel)


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
    arrays: Sequence[pa.ArrayLike | pa.ChunkedArrayLike],
    *,
    prefix: str | None = None,
    null_sentinel: str = "None",
) -> pa.ArrayLike:
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
    joined = pa.pc.binary_join_element_wise(*parts, _NULL_SEPARATOR)
    return _hash64(joined)


def prefixed_hash_id(
    arrays: Sequence[pa.ArrayLike | pa.ChunkedArrayLike],
    *,
    prefix: str,
    null_sentinel: str = "None",
) -> pa.ArrayLike:
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
    hashed_str = pa.pc.cast(hashed, pa.string())
    return pa.pc.binary_join_element_wise(pa.scalar(prefix), hashed_str, ":")


def hash64_from_columns(
    table: pa.TableLike,
    *,
    cols: Sequence[str],
    prefix: str | None = None,
    null_sentinel: str = "None",
    missing: MissingPolicy = "raise",
) -> pa.ArrayLike:
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
    arrays: list[pa.ArrayLike] = []
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


def hash_column_values(table: pa.TableLike, *, spec: HashSpec) -> pa.ArrayLike:
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
        arrays: list[pa.ArrayLike] = [
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
        hashed = pa.pc.cast(hashed, pa.string())
        hashed = pa.pc.binary_join_element_wise(pa.scalar(spec.prefix), hashed, ":")
    return hashed


def add_hash_column(table: pa.TableLike, *, spec: HashSpec) -> pa.TableLike:
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


__all__ = [
    "HashSpec",
    "MissingPolicy",
    "add_hash_column",
    "hash64_from_arrays",
    "hash64_from_columns",
    "hash64_from_parts",
    "hash_column_values",
    "iter_array_values",
    "iter_arrays",
    "iter_table_rows",
    "prefixed_hash_id",
    "prefixed_hash_id_from_parts",
]
