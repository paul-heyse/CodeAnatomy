"""Arrow-native iteration helpers and hash identifiers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal
from uuid import uuid4

import pyarrow as pa
from datafusion import SessionContext

import arrowdsl.core.interop as arrow_pa
from arrowdsl.core.array_iter import iter_array_values, iter_arrays, iter_table_rows
from datafusion_engine.hash_utils import (
    hash64_from_text as _hash64_from_text,
)
from datafusion_engine.hash_utils import (
    hash128_from_text as _hash128_from_text,
)
from datafusion_engine.introspection import invalidate_introspection_cache
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.sql_options import sql_options_for_profile
from sqlglot_tools.compat import Expression, exp

type MissingPolicy = Literal["raise", "null"]


type ArrayOrScalar = arrow_pa.ArrayLike | arrow_pa.ChunkedArrayLike | arrow_pa.ScalarLike


_NULL_SEPARATOR = "\x1f"


def _datafusion_context() -> SessionContext:
    profile = DataFusionRuntimeProfile()
    return profile.ephemeral_context()


def _expr_table(ctx: SessionContext, expr: Expression) -> pa.Table:
    from datafusion_engine.compile_options import DataFusionCompileOptions, DataFusionSqlPolicy
    from datafusion_engine.execution_facade import DataFusionExecutionFacade

    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=None)
    plan = facade.compile(
        expr,
        options=DataFusionCompileOptions(
            sql_options=sql_options_for_profile(None),
            sql_policy=DataFusionSqlPolicy(),
        ),
    )
    result = facade.execute(plan)
    if result.dataframe is None:
        msg = "ID SQL execution did not return a DataFusion DataFrame."
        raise ValueError(msg)
    return result.dataframe.to_arrow_table()


def _register_table(ctx: SessionContext, table: pa.Table, *, prefix: str) -> str:
    name = f"_{prefix}_{uuid4().hex}"
    from datafusion_engine.io_adapter import DataFusionIOAdapter

    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_record_batches(name, [list(table.to_batches())])
    return name


def _deregister_table(ctx: SessionContext, name: str) -> None:
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        deregister(name)
        invalidate_introspection_cache(ctx)


def _string_literal(value: str) -> Expression:
    return exp.Literal.string(value)


def _coalesce_cast(expr: Expression, *, null_sentinel: str) -> Expression:
    return exp.func(
        "coalesce",
        exp.Cast(this=expr, to=exp.DataType.build("string")),
        _string_literal(null_sentinel),
    )


def _concat_ws(parts: Sequence[Expression]) -> Expression:
    if len(parts) == 1:
        return parts[0]
    return exp.func("concat_ws", _string_literal(_NULL_SEPARATOR), *parts)


def _hash_expression(
    column_names: Sequence[str],
    *,
    prefix: str | None,
    null_sentinel: str,
    use_128: bool,
    extra_literals: Sequence[str] = (),
) -> Expression:
    parts: list[Expression] = []
    if prefix is not None:
        parts.append(_string_literal(prefix))
    parts.extend(_string_literal(value) for value in extra_literals)
    parts.extend(
        _coalesce_cast(exp.column(name), null_sentinel=null_sentinel) for name in column_names
    )
    joined = _concat_ws(parts)
    func = "stable_hash128" if use_128 else "stable_hash64"
    return exp.func(func, joined)


def _prefixed_hash_expression(
    column_names: Sequence[str],
    *,
    prefix: str,
    null_sentinel: str,
    use_128: bool,
    extra_literals: Sequence[str] = (),
) -> Expression:
    hash_expr = _hash_expression(
        column_names,
        prefix=prefix,
        null_sentinel=null_sentinel,
        use_128=use_128,
        extra_literals=extra_literals,
    )
    return exp.func(
        "concat_ws",
        _string_literal(":"),
        _string_literal(prefix),
        exp.Cast(this=hash_expr, to=exp.DataType.build("string")),
    )


def _is_null(expr: Expression) -> Expression:
    return exp.Is(this=expr, expression=exp.null())


def _is_not_null(expr: Expression) -> Expression:
    return exp.Not(this=_is_null(expr))


def _and_expressions(expressions: Sequence[Expression]) -> Expression | None:
    if not expressions:
        return None
    if len(expressions) == 1:
        return expressions[0]
    return exp.and_(*expressions)


def _ensure_table(value: arrow_pa.TableLike) -> pa.Table:
    if isinstance(value, arrow_pa.RecordBatchReaderLike):
        return pa.Table.from_batches(list(value))
    if isinstance(value, pa.Table):
        return value
    return pa.Table.from_pydict(value.to_pydict())


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
    arrays: Sequence[arrow_pa.ArrayLike | arrow_pa.ChunkedArrayLike],
    *,
    prefix: str | None,
    null_sentinel: str,
    use_128: bool,
) -> arrow_pa.ArrayLike:
    if not arrays:
        msg = "hash_from_arrays requires at least one input array."
        raise ValueError(msg)
    ctx = _datafusion_context()
    columns = {f"col_{idx}": value for idx, value in enumerate(arrays)}
    table = pa.Table.from_pydict(columns)
    name = _register_table(ctx, table, prefix="hash_arrays")
    try:
        col_names = tuple(columns.keys())
        hash_expr = _hash_expression(
            col_names,
            prefix=prefix,
            null_sentinel=null_sentinel,
            use_128=use_128,
        )
        select_expr = exp.select(hash_expr.as_("hash_value")).from_(name)
        result = _expr_table(ctx, select_expr)
    finally:
        _deregister_table(ctx, name)
    return result["hash_value"]


def hash64_from_arrays(
    arrays: Sequence[arrow_pa.ArrayLike | arrow_pa.ChunkedArrayLike],
    *,
    prefix: str | None = None,
    null_sentinel: str = "None",
) -> arrow_pa.ArrayLike:
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
    arrays: Sequence[arrow_pa.ArrayLike | arrow_pa.ChunkedArrayLike],
    *,
    prefix: str,
    null_sentinel: str = "None",
    use_128: bool = True,
) -> arrow_pa.ArrayLike:
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

    Raises
    ------
    ValueError
        Raised when no input arrays are provided.
    """
    if not arrays:
        msg = "prefixed_hash_id requires at least one input array."
        raise ValueError(msg)
    ctx = _datafusion_context()
    columns = {f"col_{idx}": value for idx, value in enumerate(arrays)}
    table = pa.Table.from_pydict(columns)
    name = _register_table(ctx, table, prefix="prefixed_hash")
    try:
        col_names = tuple(columns.keys())
        hash_expr = _prefixed_hash_expression(
            col_names,
            prefix=prefix,
            null_sentinel=null_sentinel,
            use_128=use_128,
        )
        select_expr = exp.select(hash_expr.as_("hash_value")).from_(name)
        result = _expr_table(ctx, select_expr)
    finally:
        _deregister_table(ctx, name)
    return result["hash_value"]


def _arrays_from_columns(
    table: arrow_pa.TableLike,
    *,
    cols: Sequence[str],
    missing: MissingPolicy,
) -> list[arrow_pa.ArrayLike]:
    arrays: list[arrow_pa.ArrayLike] = []
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
    table: arrow_pa.TableLike,
    *,
    cols: Sequence[str],
    prefix: str | None = None,
    null_sentinel: str = "None",
    missing: MissingPolicy = "raise",
) -> arrow_pa.ArrayLike:
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


def masked_prefixed_hash(
    prefix: str,
    arrays: Sequence[arrow_pa.ArrayLike | arrow_pa.ChunkedArrayLike],
    *,
    required: Sequence[arrow_pa.ArrayLike | arrow_pa.ChunkedArrayLike],
) -> arrow_pa.ArrayLike | arrow_pa.ChunkedArrayLike:
    """Return prefixed hash IDs masked by required validity arrays.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Prefixed hash IDs masked by required validity.

    Raises
    ------
    ValueError
        Raised when no input arrays are provided.
    """
    if not arrays:
        msg = "masked_prefixed_hash requires at least one input array."
        raise ValueError(msg)
    ctx = _datafusion_context()
    columns: dict[str, arrow_pa.ArrayLike | arrow_pa.ChunkedArrayLike] = {
        f"col_{idx}": value for idx, value in enumerate(arrays)
    }
    required_names: list[str] = []
    for idx, value in enumerate(required):
        name = f"req_{idx}"
        columns[name] = value
        required_names.append(name)
    table = pa.Table.from_pydict(columns)
    name = _register_table(ctx, table, prefix="masked_hash")
    try:
        col_names = tuple(key for key in columns if key.startswith("col_"))
        hash_expr = _prefixed_hash_expression(
            col_names,
            prefix=prefix,
            null_sentinel="None",
            use_128=True,
        )
        mask_expr = (
            _and_expressions([_is_not_null(exp.column(col)) for col in required_names])
            or exp.true()
        )
        case_expr = exp.Case().when(mask_expr, hash_expr).else_(exp.null())
        select_expr = exp.select(case_expr.as_("hash_value")).from_(name)
        result = _expr_table(ctx, select_expr)
    finally:
        _deregister_table(ctx, name)
    return result["hash_value"]


def prefixed_hash64(
    prefix: str,
    arrays: Sequence[arrow_pa.ArrayLike | arrow_pa.ChunkedArrayLike],
) -> arrow_pa.ArrayLike | arrow_pa.ChunkedArrayLike:
    """Return prefixed hash IDs for the provided arrays.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Prefixed hash identifiers.
    """
    return prefixed_hash_id(arrays, prefix=prefix, use_128=False)


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


def add_span_id_column(
    table: arrow_pa.TableLike,
    spec: SpanIdSpec | None = None,
) -> arrow_pa.TableLike:
    """Add a span_id column computed with DataFusion hash UDFs.

    Returns
    -------
    TableLike
        Table with the span_id column appended.
    """
    spec = spec or SpanIdSpec()
    resolved = _ensure_table(table)
    if spec.out_col in resolved.column_names:
        return resolved
    updated = _ensure_span_id_columns(resolved, spec=spec)
    ctx = _datafusion_context()
    name = _register_table(ctx, updated, prefix="span_ids")
    try:
        columns = (spec.path_col, spec.bstart_col, spec.bend_col)
        hash_expr = _prefixed_hash_expression(
            columns,
            prefix="span",
            null_sentinel="None",
            use_128=True,
            extra_literals=(spec.kind,) if spec.kind is not None else (),
        )
        mask_expr = _and_expressions([_is_not_null(exp.column(col)) for col in columns])
        if mask_expr is None:
            mask_expr = exp.true()
        span_expr = exp.Case().when(mask_expr, hash_expr).else_(exp.null())
        select_expr = exp.select(exp.Star(), span_expr.as_(spec.out_col)).from_(name)
        result = _expr_table(ctx, select_expr)
    finally:
        _deregister_table(ctx, name)
    return result


def _ensure_span_id_columns(table: pa.Table, *, spec: SpanIdSpec) -> pa.Table:
    updated = table
    required = (
        (spec.path_col, pa.string()),
        (spec.bstart_col, pa.int64()),
        (spec.bend_col, pa.int64()),
    )
    for name, dtype in required:
        if name in updated.column_names:
            continue
        updated = updated.append_column(name, pa.nulls(updated.num_rows, type=dtype))
    return updated


__all__ = [
    "MissingPolicy",
    "SpanIdSpec",
    "add_span_id_column",
    "hash64_from_arrays",
    "hash64_from_columns",
    "hash64_from_parts",
    "hash64_from_text",
    "iter_array_values",
    "iter_arrays",
    "iter_table_rows",
    "masked_prefixed_hash",
    "prefixed_hash64",
    "prefixed_hash_id",
    "span_id",
    "stable_id",
    "stable_int64",
]
