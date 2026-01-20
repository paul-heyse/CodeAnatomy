"""Arrow-native iteration helpers and hash ID specs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

import arrowdsl.core.interop as pa
from arrowdsl.core.array_iter import iter_array_values, iter_arrays, iter_table_rows
from arrowdsl.core.validity import valid_mask_array
from datafusion_engine.compute_ops import (
    and_,
    binary_join_element_wise,
    cast_values,
    fill_null,
    if_else,
    is_valid,
)
from datafusion_engine.hash_utils import (
    hash64_from_text as _hash64_from_text,
)
from datafusion_engine.hash_utils import (
    hash128_from_text as _hash128_from_text,
)
from datafusion_engine.udf_registry import stable_hash64_values, stable_hash128_values

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


_NULL_SEPARATOR = "\x1f"


def hash64_from_text(value: str | None) -> int | None:
    """Return a deterministic int64 hash for a string value.

    Returns
    -------
    int | None
        Deterministic hash value or ``None`` when input is ``None``.
    """
    if value is None:
        return None
    return _hash64_from_text(value)


def _hash64(values: pa.ArrayLike) -> pa.ArrayLike:
    """Compute hash64 values using the shared stable hash UDF logic.

    Parameters
    ----------
    values
        Input array to hash.

    Returns
    -------
    pa.ArrayLike
        Int64 hash array.
    """
    result = stable_hash64_values(values)
    if isinstance(result, pa.ScalarLike):
        return pa.array([result.as_py()], type=pa.int64())
    return cast_values(result, pa.int64(), safe=False)


def _hash128(values: pa.ArrayLike) -> pa.ArrayLike:
    """Compute hash128 values using the shared stable hash UDF logic.

    Parameters
    ----------
    values
        Input array to hash.

    Returns
    -------
    pa.ArrayLike
        String hash array.
    """
    result = stable_hash128_values(values)
    if isinstance(result, pa.ScalarLike):
        return pa.array([result.as_py()], type=pa.string())
    return cast_values(result, pa.string(), safe=False)


def _stringify(
    array: pa.ArrayLike | pa.ChunkedArrayLike,
    *,
    null_sentinel: str,
) -> pa.ArrayLike:
    """Cast an array to string and fill nulls with a sentinel.

    Parameters
    ----------
    array
        Array-like input to cast.
    null_sentinel
        Replacement value for nulls.

    Returns
    -------
    pa.ArrayLike
        String array with nulls filled.
    """
    text = cast_values(array, pa.string())
    return fill_null(text, fill_value=null_sentinel)


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
    return _hash64_from_text(joined)


def _hash128_from_parts(
    *parts: str | None,
    prefix: str | None,
    null_sentinel: str,
) -> str:
    values: list[str | None] = [prefix] if prefix is not None else []
    values.extend(parts)
    joined = _NULL_SEPARATOR.join(value if value is not None else null_sentinel for value in values)
    return _hash128_from_text(joined)


def _hash_from_arrays(
    arrays: Sequence[pa.ArrayLike | pa.ChunkedArrayLike],
    *,
    prefix: str | None,
    null_sentinel: str,
    use_128: bool,
) -> pa.ArrayLike:
    if not arrays:
        msg = "hash_from_arrays requires at least one input array."
        raise ValueError(msg)
    parts: list[ArrayOrScalar] = [_stringify(arr, null_sentinel=null_sentinel) for arr in arrays]
    if prefix is not None:
        parts.insert(0, pa.scalar(prefix))
    joined = binary_join_element_wise(*parts, _NULL_SEPARATOR)
    return _hash128(joined) if use_128 else _hash64(joined)


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
    return _hash_from_arrays(
        arrays,
        prefix=prefix,
        null_sentinel=null_sentinel,
        use_128=False,
    )


def prefixed_hash_id(
    arrays: Sequence[pa.ArrayLike | pa.ChunkedArrayLike],
    *,
    prefix: str,
    null_sentinel: str = "None",
    use_128: bool = True,
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
    use_128:
        When ``True``, use the 128-bit stable hash for string IDs.

    Returns
    -------
    ArrayLike
        String array with prefixed hash IDs.
    """
    hashed = _hash_from_arrays(
        arrays,
        prefix=prefix,
        null_sentinel=null_sentinel,
        use_128=use_128,
    )
    hashed_str = cast_values(hashed, pa.string())
    return binary_join_element_wise(pa.scalar(prefix), hashed_str, ":")


def _arrays_from_columns(
    table: pa.TableLike,
    *,
    cols: Sequence[str],
    missing: MissingPolicy,
) -> list[pa.ArrayLike]:
    arrays: list[pa.ArrayLike] = []
    for col in cols:
        if col in table.column_names:
            arrays.append(table[col])
            continue
        if missing == "null":
            arrays.append(pa.nulls(table.num_rows, type=pa.string()))
            continue
        msg = f"Missing column for hash: {col!r}."
        raise KeyError(msg)
    return arrays


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
    if missing == "raise":
        for col in cols:
            if col not in table.column_names:
                msg = f"Missing column for hash64: {col!r}."
                raise KeyError(msg)
    arrays = _arrays_from_columns(table, cols=cols, missing=missing)
    return _hash_from_arrays(
        arrays,
        prefix=prefix,
        null_sentinel=null_sentinel,
        use_128=False,
    )


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
    if spec.missing == "raise":
        for col in spec.cols:
            if col not in table.column_names:
                msg = f"Missing column for hash: {col!r}."
                raise KeyError(msg)
    if spec.extra_literals:
        arrays: list[pa.ArrayLike] = [
            pa.array([literal] * table.num_rows, type=pa.string())
            for literal in spec.extra_literals
        ]
        arrays.extend(_arrays_from_columns(table, cols=spec.cols, missing=spec.missing))
    else:
        arrays = _arrays_from_columns(table, cols=spec.cols, missing=spec.missing)
    hashed = _hash_from_arrays(
        arrays,
        prefix=spec.prefix,
        null_sentinel=spec.null_sentinel,
        use_128=spec.as_string,
    )
    if spec.as_string:
        hashed = cast_values(hashed, pa.string())
        hashed = binary_join_element_wise(pa.scalar(spec.prefix), hashed, ":")
    return hashed


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
        mask = is_valid(table[first])
    else:
        mask = pa.array([False] * table.num_rows, type=pa.bool_())
    for name in required[1:]:
        if name in table.column_names:
            mask = and_(mask, is_valid(table[name]))
        else:
            mask = and_(mask, pa.array([False] * table.num_rows, type=pa.bool_()))
    null_value = pa.scalar(None, type=pa.string() if spec.as_string else pa.int64())
    return if_else(mask, hashed, null_value)


def masked_prefixed_hash(
    prefix: str,
    arrays: Sequence[pa.ArrayLike | pa.ChunkedArrayLike],
    *,
    required: Sequence[pa.ArrayLike | pa.ChunkedArrayLike],
) -> pa.ArrayLike | pa.ChunkedArrayLike:
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
    return if_else(mask, hashed, pa.scalar(None, type=pa.string()))


def prefixed_hash64(
    prefix: str,
    arrays: Sequence[pa.ArrayLike | pa.ChunkedArrayLike],
) -> pa.ArrayLike | pa.ChunkedArrayLike:
    """Return prefixed hash IDs for the provided arrays.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Prefixed hash identifiers.
    """
    return prefixed_hash_id(arrays, prefix=prefix, use_128=False)


def apply_hash_column(
    table: pa.TableLike,
    *,
    spec: HashSpec,
    required: Sequence[str] | None = None,
) -> pa.TableLike:
    """Hash columns into a new id column.

    Returns
    -------
    TableLike
        Updated table with hashed id column.
    """
    out_col = spec.out_col or f"{spec.prefix}_id"
    req = required or ()
    hashed = masked_hash_array(table, spec=spec, required=req)
    if out_col in table.column_names:
        idx = table.schema.get_field_index(out_col)
        return table.set_column(idx, out_col, hashed)
    return table.append_column(out_col, hashed)


def apply_hash_columns(
    table: pa.TableLike,
    *,
    specs: Sequence[HashSpec],
    required: Mapping[str, Sequence[str]] | None = None,
) -> pa.TableLike:
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


def stable_id(prefix: str, *parts: str | None) -> str:
    """Build a deterministic string ID.

    Returns
    -------
    str
        Stable identifier with the requested prefix.
    """
    hashed = _hash128_from_parts(*parts, prefix=prefix, null_sentinel="None")
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


def add_span_id_column(table: pa.TableLike, spec: SpanIdSpec | None = None) -> pa.TableLike:
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
    span_ids = if_else(valid, prefixed, pa.scalar(None, type=pa.string()))
    return table.append_column(out_col, span_ids)


__all__ = [
    "HashSpec",
    "MissingPolicy",
    "SpanIdSpec",
    "add_span_id_column",
    "apply_hash_column",
    "apply_hash_columns",
    "hash64_from_arrays",
    "hash64_from_columns",
    "hash64_from_parts",
    "hash64_from_text",
    "hash_column_values",
    "iter_array_values",
    "iter_arrays",
    "iter_table_rows",
    "masked_hash_array",
    "masked_prefixed_hash",
    "prefixed_hash64",
    "prefixed_hash_id",
    "span_id",
    "stable_id",
    "stable_int64",
]
