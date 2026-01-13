"""Arrow-native iteration helpers and hash ID specs."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Literal

import arrowdsl.core.interop as pa
from arrowdsl.compute.filters import UdfSpec, ensure_udf, resolve_kernel
from arrowdsl.core.interop import ComputeExpression, ensure_expression, pc

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
    spec = UdfSpec(
        name=_HASH64_FUNCTION,
        inputs={"value": pa.string()},
        output=pa.int64(),
        fn=_hash64_udf,
        summary="hash64 udf",
        description="Deterministic 64-bit hash for strings.",
    )
    ensure_udf(spec)


def _hash64(values: pa.ArrayLike) -> pa.ArrayLike:
    func = resolve_kernel("hash64", fallbacks=("hash",))
    if func is not None:
        result = pa.pc.call_function(func, [values])
    else:
        _ensure_hash64_udf()
        result = pa.pc.call_function(_HASH64_FUNCTION, [values])
    if isinstance(result, pa.ScalarLike):
        return pa.array([result.as_py()], type=pa.int64())
    return pa.pc.cast(result, pa.int64(), safe=False)


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


def hash_expression(
    spec: HashSpec,
    *,
    available: Sequence[str] | None = None,
) -> ComputeExpression:
    """Build a compute expression for a hash-based ID column.

    Parameters
    ----------
    spec:
        Hash column specification.
    available:
        Optional column names present in the input plan.

    Returns
    -------
    ComputeExpression
        Compute expression producing the hash column values.

    Raises
    ------
    KeyError
        Raised when a required column is missing and ``spec.missing="raise"``.
    """
    parts: list[ComputeExpression] = []
    if spec.extra_literals:
        parts.extend(pc.scalar(literal) for literal in spec.extra_literals)
    for col in spec.cols:
        if available is not None and col not in available:
            if spec.missing == "null":
                expr = pc.scalar(None)
            else:
                msg = f"Missing column for hash64: {col!r}."
                raise KeyError(msg)
        else:
            expr = pc.field(col)
        expr = ensure_expression(
            pc.fill_null(pc.cast(expr, pa.string()), pc.scalar(spec.null_sentinel))
        )
        parts.append(expr)
    if spec.prefix:
        parts.insert(0, pc.scalar(spec.prefix))
    joined = pc.binary_join_element_wise(*parts, _NULL_SEPARATOR)
    func = resolve_kernel("hash64", fallbacks=("hash",))
    if func is not None:
        hashed = pc.call_function(func, [joined])
    else:
        _ensure_hash64_udf()
        hashed = pc.call_function(_HASH64_FUNCTION, [joined])
    hashed = pc.cast(hashed, pa.int64(), safe=False)
    if spec.as_string:
        hashed = pc.cast(hashed, pa.string())
        hashed = pc.binary_join_element_wise(pc.scalar(spec.prefix), hashed, ":")
    return ensure_expression(hashed)


def hash_expression_from_parts(
    parts: Sequence[ComputeExpression],
    *,
    prefix: str,
    null_sentinel: str = "None",
    as_string: bool = True,
) -> ComputeExpression:
    """Build a compute expression for hash IDs from expression parts.

    Returns
    -------
    ComputeExpression
        Compute expression producing hash IDs for the expression parts.
    """
    exprs: list[ComputeExpression] = []
    for part in parts:
        expr = ensure_expression(pc.fill_null(pc.cast(part, pa.string()), pc.scalar(null_sentinel)))
        exprs.append(expr)
    if prefix:
        exprs.insert(0, pc.scalar(prefix))
    joined = pc.binary_join_element_wise(*exprs, _NULL_SEPARATOR)
    func = resolve_kernel("hash64", fallbacks=("hash",))
    if func is not None:
        hashed = pc.call_function(func, [joined])
    else:
        _ensure_hash64_udf()
        hashed = pc.call_function(_HASH64_FUNCTION, [joined])
    hashed = pc.cast(hashed, pa.int64(), safe=False)
    if as_string:
        hashed = pc.cast(hashed, pa.string())
        hashed = pc.binary_join_element_wise(pc.scalar(prefix), hashed, ":")
    return ensure_expression(hashed)


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


def masked_hash_array(
    table: pa.TableLike,
    *,
    spec: HashSpec,
    required: Sequence[str],
) -> pa.ArrayLike:
    """Return hash values masked by required column validity.

    Returns
    -------
    pyarrow.Array
        Masked hash array for the spec.
    """
    if not required:
        return hash_column_values(table, spec=spec)
    hashed = hash_column_values(table, spec=spec)
    first = required[0]
    if first in table.column_names:
        mask = pc.is_valid(table[first])
    else:
        mask = pa.array([False] * table.num_rows, type=pa.bool_())
    for name in required[1:]:
        if name in table.column_names:
            mask = pc.and_(mask, pc.is_valid(table[name]))
        else:
            mask = pc.and_(mask, pa.array([False] * table.num_rows, type=pa.bool_()))
    null_value = pa.scalar(None, type=pa.string() if spec.as_string else pa.int64())
    return pc.if_else(mask, hashed, null_value)


__all__ = [
    "HashSpec",
    "MissingPolicy",
    "hash64_from_arrays",
    "hash64_from_columns",
    "hash64_from_parts",
    "hash_column_values",
    "hash_expression",
    "hash_expression_from_parts",
    "iter_array_values",
    "iter_arrays",
    "iter_table_rows",
    "masked_hash_array",
    "masked_hash_expression",
    "prefixed_hash_id",
]
