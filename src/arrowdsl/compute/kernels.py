"""Kernel-lane transforms for Arrow tables."""

from __future__ import annotations

import functools
import importlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, cast

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.core.context import ExecutionContext, Ordering, OrderingLevel
from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.expr_types import ExplodeSpec
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    DataTypeLike,
    SchemaLike,
    TableLike,
    pc,
)
from arrowdsl.core.joins import (
    JoinConfig,
    JoinOutputSpec,
    interval_join_candidates,
    join_spec,
    resolve_join_outputs,
)
from arrowdsl.core.plan_ops import AsofJoinSpec, DedupeSpec, IntervalAlignOptions, JoinSpec, SortKey
from arrowdsl.core.schema_constants import PROVENANCE_COLS
from arrowdsl.kernel.registry import KernelLane, kernel_def
from arrowdsl.schema.chunking import ChunkPolicy
from arrowdsl.schema.metadata import metadata_spec_from_schema, ordering_from_schema
from arrowdsl.schema.schema import SchemaMetadataSpec


def _const_array(
    n: int,
    value: object,
    *,
    dtype: DataTypeLike | None = None,
) -> ArrayLike:
    scalar = pa.scalar(value) if dtype is None else pa.scalar(value, type=dtype)
    return pa.array([value] * n, type=scalar.type)


def _set_or_append_column(table: TableLike, name: str, values: ArrayLike) -> TableLike:
    if name in table.column_names:
        idx = table.schema.get_field_index(name)
        return table.set_column(idx, name, values)
    return table.append_column(name, values)


_NUMERIC_REGEX = r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$"

type KernelFn = Callable[..., TableLike]


@dataclass(frozen=True)
class KernelCapability:
    """Capability metadata for kernel selection."""

    name: str
    lane: KernelLane
    volatility: Literal["stable", "volatile", "immutable"] = "stable"
    requires_ordering: bool = False

    def with_lane(self, lane: KernelLane) -> KernelCapability:
        """Return a copy with the lane updated.

        Returns
        -------
        KernelCapability
            Capability metadata updated with the provided lane.
        """
        return KernelCapability(
            name=self.name,
            lane=lane,
            volatility=self.volatility,
            requires_ordering=self.requires_ordering,
        )


def kernel_registry() -> dict[str, KernelFn]:
    """Return the Arrow kernel registry.

    Returns
    -------
    dict[str, KernelFn]
        Mapping of kernel names to Arrow implementations.
    """
    return {
        "interval_align": interval_align_table,
        "canonical_sort": canonical_sort,
        "explode_list": explode_list_column,
        "dedupe": apply_dedupe,
        "winner_select": winner_select_by_score,
    }


def make_struct_column(fields: Mapping[str, ArrayLike]) -> ArrayLike:
    """Return a struct array constructed from named fields.

    Returns
    -------
    ArrayLike
        Struct array with the provided field values.
    """
    names = list(fields)
    arrays = [fields[name] for name in names]
    return pc.make_struct(*arrays, field_names=names)


def make_list_column(values: Sequence[Sequence[object]]) -> ArrayLike:
    """Return a list array constructed from nested Python values.

    Returns
    -------
    ArrayLike
        List array with values for each row.
    """
    return pa.array(values)


def make_map_column(
    entries: Sequence[Mapping[object, object]],
    *,
    key_type: pa.DataType,
    item_type: pa.DataType,
) -> ArrayLike:
    """Return a map array constructed from entry dictionaries.

    Returns
    -------
    ArrayLike
        Map array with the provided key/value entries.
    """
    map_type = pa.map_(key_type, item_type)
    return pa.array(entries, type=map_type)


def kernel_capability(name: str, *, ctx: ExecutionContext) -> KernelCapability:
    """Return the capability metadata for a kernel given a runtime context.

    Returns
    -------
    KernelCapability
        Capability metadata for the requested kernel.

    """
    definition = kernel_def(name)
    capability = KernelCapability(
        name=definition.name,
        lane=definition.lane,
        volatility=definition.volatility,
        requires_ordering=definition.requires_ordering,
    )
    if ctx.runtime.datafusion is None:
        return capability.with_lane(KernelLane.ARROW_FALLBACK)
    if _datafusion_kernel(name) is None:
        return capability.with_lane(KernelLane.ARROW_FALLBACK)
    return capability


def resolve_kernel(name: str, *, ctx: ExecutionContext) -> KernelFn:
    """Return the best available kernel implementation for the runtime.

    Returns
    -------
    KernelFn
        Kernel implementation for the requested name.

    Raises
    ------
    KeyError
        Raised when the kernel name is unknown.
    """
    if ctx.runtime.datafusion is not None:
        datafusion_kernel = _datafusion_kernel(name)
        if datafusion_kernel is not None:
            return functools.partial(datafusion_kernel, _ctx=ctx)
    registry = kernel_registry()
    if name not in registry:
        msg = f"Unknown kernel name: {name!r}."
        raise KeyError(msg)
    return registry[name]


def _datafusion_kernel(name: str) -> KernelFn | None:
    module = importlib.import_module("datafusion_engine.kernels")
    registry = getattr(module, "datafusion_kernel_registry", None)
    if not callable(registry):
        return None
    return cast("dict[str, KernelFn]", registry()).get(name)


@dataclass(frozen=True)
class _IntervalAlignPrepared:
    left: TableLike
    right: TableLike
    left_keep: list[str]
    right_keep: list[str]
    right_keep_set: set[str]
    left_key_col: str
    right_key_col: str
    left_id_col: str
    score_col: str
    tie_breaker_cols: list[str]


@dataclass(frozen=True)
class _IntervalAlignOutputSpec:
    output_names: Sequence[str]
    output_schema: pa.Schema
    right_keep: set[str]
    cfg: IntervalAlignOptions
    score_col: str


def provenance_sort_keys() -> tuple[str, ...]:
    """Return provenance columns used for ordering tie-breakers.

    Returns
    -------
    tuple[str, ...]
        Provenance column names.
    """
    return tuple(PROVENANCE_COLS)


def cast_array(
    values: ArrayLike | ChunkedArrayLike,
    dtype: DataTypeLike,
    *,
    safe: bool = True,
) -> ArrayLike | ChunkedArrayLike:
    """Cast an array or chunked array to the requested type.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Casted values.
    """
    return pc.cast(values, dtype, safe=safe)


def distinct_sorted(values: ArrayLike | ChunkedArrayLike) -> ArrayLike:
    """Return distinct values sorted ascending.

    Returns
    -------
    ArrayLike
        Sorted unique values.
    """
    unique = _unique_values(values)
    options = _sort_options_for_keys(None)
    indices = pc.sort_indices(unique, options=options)
    return pc.take(unique, indices)


def flatten_list_struct_field(
    table: TableLike,
    *,
    list_col: str,
    field: str,
) -> ArrayLike:
    """Flatten a list<struct> column and return a field array.

    Returns
    -------
    ArrayLike
        Flattened field values.
    """
    flattened = pc.list_flatten(table[list_col])
    return pc.struct_field(flattened, field)


def def_use_kind_array(
    opname: ArrayLike | ChunkedArrayLike,
    *,
    def_ops: Sequence[str],
    def_prefixes: Sequence[str],
    use_prefixes: Sequence[str],
) -> ArrayLike:
    """Classify opnames into def/use kinds.

    Returns
    -------
    ArrayLike
        String array with "def", "use", or null.
    """
    opname_str = cast_array(opname, pa.string())
    def_ops_arr = pa.array(sorted(def_ops), type=pa.string())
    is_def = pc.or_(
        pc.is_in(opname_str, value_set=def_ops_arr),
        pc.or_(
            pc.starts_with(opname_str, def_prefixes[0]),
            pc.starts_with(opname_str, def_prefixes[1]),
        ),
    )
    is_use = pc.starts_with(opname_str, use_prefixes[0])
    none_str = pa.scalar(None, type=pa.string())
    return pc.if_else(
        is_def,
        pa.scalar("def"),
        pc.if_else(is_use, pa.scalar("use"), none_str),
    )


def _append_provenance_keys(table: TableLike, sort_keys: Sequence[SortKey]) -> list[SortKey]:
    existing = {sk.column for sk in sort_keys}
    extras = [
        SortKey(col, "ascending")
        for col in PROVENANCE_COLS
        if col in table.column_names and col not in existing
    ]
    return [*sort_keys, *extras]


def _sort_key_tuples(sort_keys: Sequence[SortKey]) -> list[tuple[str, str]]:
    return [(sk.column, sk.order) for sk in sort_keys]


def _unique_values(values: ArrayLike | ChunkedArrayLike) -> ArrayLike:
    unique_fn = getattr(pc, "unique", None)
    if unique_fn is None:
        msg = "pyarrow.compute.unique is unavailable."
        raise RuntimeError(msg)
    return unique_fn(values)


def _sort_options_for_keys(sort_keys: Sequence[SortKey] | None) -> object:
    sort_options = getattr(pc, "SortOptions", None)
    if sort_options is None:
        msg = "pyarrow.compute.SortOptions is unavailable."
        raise RuntimeError(msg)
    kwargs: dict[str, object] = {"null_placement": "at_end"}
    if sort_keys:
        kwargs["sort_keys"] = _sort_key_tuples(sort_keys)
    return sort_options(**kwargs)


def _sort_options(sort_keys: Sequence[SortKey]) -> object:
    return _sort_options_for_keys(sort_keys)


def _unique_sort_keys(sort_keys: Sequence[SortKey]) -> list[SortKey]:
    seen: set[str] = set()
    unique: list[SortKey] = []
    for key in sort_keys:
        if key.column in seen:
            continue
        seen.add(key.column)
        unique.append(key)
    return unique


def _sort_table_for_keys(table: TableLike, sort_keys: Sequence[SortKey]) -> pa.Table:
    names: list[str] = []
    columns: list[ChunkedArrayLike] = []
    for key in sort_keys:
        name = key.column
        field = table.schema.field(name)
        col = table[name]
        if patypes.is_dictionary(field.type):
            dict_type = cast("pa.DictionaryType", field.type)
            col = cast("ChunkedArrayLike", pc.cast(col, dict_type.value_type))
        names.append(name)
        columns.append(col)
    return pa.table(columns, names=names)


def canonical_sort(table: TableLike, *, sort_keys: Sequence[SortKey]) -> TableLike:
    """Return a canonically ordered table using stable sort indices.

    Parameters
    ----------
    table:
        Input table to sort.
    sort_keys:
        Sort keys for canonical ordering.

    Returns
    -------
    pyarrow.Table
        Sorted table.
    """
    if not sort_keys:
        return table
    normalized_keys = _unique_sort_keys(sort_keys)
    if not normalized_keys:
        return table
    options = _sort_options(normalized_keys)
    sort_table = _sort_table_for_keys(table, normalized_keys)
    idx = pc.sort_indices(sort_table, options=options)
    return table.take(idx)


def _metadata_spec_from_table(table: TableLike) -> SchemaMetadataSpec | None:
    spec = metadata_spec_from_schema(table.schema)
    if not spec.schema_metadata and not spec.field_metadata:
        return None
    return spec


def _apply_metadata(table: TableLike, *, metadata: SchemaMetadataSpec | None) -> TableLike:
    if metadata is None:
        return table
    schema = metadata.apply(table.schema)
    return table.cast(schema)


def _require_explicit_ordering(schema: SchemaLike, *, kernel: str) -> Ordering:
    ordering = ordering_from_schema(schema)
    if ordering.level != OrderingLevel.EXPLICIT or not ordering.keys:
        msg = f"{kernel} requires explicit ordering metadata."
        raise ValueError(msg)
    return ordering


def _dedupe_spec_with_ordering(spec: DedupeSpec, ordering: Ordering) -> DedupeSpec:
    if ordering.level != OrderingLevel.EXPLICIT or not ordering.keys:
        return spec
    existing = {sk.column for sk in spec.tie_breakers}
    extras = [
        SortKey(col, _normalize_sort_order(order))
        for col, order in ordering.keys
        if col not in existing
    ]
    if not extras:
        return spec
    return DedupeSpec(
        keys=spec.keys,
        strategy=spec.strategy,
        tie_breakers=tuple(spec.tie_breakers) + tuple(extras),
    )


def _normalize_sort_order(order: str) -> Literal["ascending", "descending"]:
    return "descending" if order == "descending" else "ascending"


def apply_join(
    left: TableLike,
    right: TableLike,
    *,
    spec: JoinSpec,
    use_threads: bool = True,
    chunk_policy: ChunkPolicy | None = None,
) -> TableLike:
    """Join two tables using a shared JoinSpec.

    Parameters
    ----------
    left:
        Left table.
    right:
        Right table.
    spec:
        Join specification.
    use_threads:
        Whether to use threaded join execution.
    chunk_policy:
        Optional chunk normalization policy applied to inputs.

    Returns
    -------
    pyarrow.Table
        Joined table.

    Raises
    ------
    ValueError
        Raised when join keys are missing from the spec.
    """
    if not spec.left_keys:
        msg = "JoinSpec.left_keys must be provided for table joins."
        raise ValueError(msg)
    policy = chunk_policy or ChunkPolicy()
    left = policy.apply(left)
    right = policy.apply(right)
    right_keys = list(spec.right_keys) if spec.right_keys else list(spec.left_keys)
    join_type = spec.join_type.replace("_", " ")
    joined = left.join(
        right,
        keys=list(spec.left_keys),
        right_keys=right_keys,
        join_type=join_type,
        left_suffix=spec.output_suffix_for_left,
        right_suffix=spec.output_suffix_for_right,
        use_threads=use_threads,
    )
    return _select_join_outputs(joined, spec=spec)


def apply_asof_join(
    left: TableLike,
    right: TableLike,
    *,
    spec: AsofJoinSpec,
    use_threads: bool = True,
    chunk_policy: ChunkPolicy | None = None,
) -> TableLike:
    """Join two tables using an as-of join.

    Parameters
    ----------
    left:
        Left table.
    right:
        Right table.
    spec:
        As-of join specification.
    use_threads:
        Unused (kept for API symmetry).
    chunk_policy:
        Optional chunk normalization policy applied to inputs.

    Returns
    -------
    pyarrow.Table
        Joined table.
    """
    _ = use_threads
    policy = chunk_policy or ChunkPolicy()
    left = policy.apply(left)
    right = policy.apply(right)
    by = list(spec.by) if spec.by else None
    right_by = list(spec.right_by) if spec.right_by else None
    return left.join_asof(
        right,
        on=spec.on,
        by=by,
        tolerance=spec.tolerance,
        right_on=spec.right_on,
        right_by=right_by,
    )


def _select_join_outputs(joined: TableLike, *, spec: JoinSpec) -> TableLike:
    plan = resolve_join_outputs(joined.column_names, spec=spec)
    if not plan.output_names:
        return joined
    return joined.select(list(plan.output_names))


def canonical_sort_if_canonical(
    table: TableLike,
    *,
    sort_keys: Sequence[SortKey],
    ctx: ExecutionContext,
) -> TableLike:
    """Sort only when determinism is canonical.

    Parameters
    ----------
    table:
        Input table to sort.
    sort_keys:
        Sort keys for canonical ordering.
    ctx:
        Execution context controlling determinism tier.

    Returns
    -------
    pyarrow.Table
        Sorted table when canonical; otherwise unchanged.
    """
    if ctx.determinism == DeterminismTier.CANONICAL and sort_keys:
        keys = _append_provenance_keys(table, sort_keys)
        return canonical_sort(table, sort_keys=keys)
    return table


def _decode_dictionary_columns(
    table: pa.Table,
) -> tuple[pa.Table, dict[str, pa.DictionaryType]]:
    dict_cols: dict[str, pa.DictionaryType] = {}
    columns: list[ChunkedArrayLike] = []
    names: list[str] = []
    for field in table.schema:
        col = table[field.name]
        if patypes.is_dictionary(field.type):
            dict_type = cast("pa.DictionaryType", field.type)
            dict_cols[field.name] = dict_type
            col = cast("ChunkedArrayLike", pc.cast(col, dict_type.value_type))
        columns.append(col)
        names.append(field.name)
    if not dict_cols:
        return table, {}
    return pa.table(columns, names=names), dict_cols


def _reencode_dictionary_columns(
    table: pa.Table,
    dict_cols: dict[str, pa.DictionaryType],
) -> pa.Table:
    if not dict_cols:
        return table
    columns: list[ChunkedArrayLike] = []
    names: list[str] = []
    for name in table.schema.names:
        col = table[name]
        dict_type = dict_cols.get(name)
        if dict_type is not None:
            encoded = pc.dictionary_encode(col)
            col = cast("ChunkedArrayLike", pc.cast(encoded, dict_type))
        columns.append(col)
        names.append(name)
    return pa.table(columns, names=names)


def dedupe_keep_first_after_sort(
    table: TableLike,
    *,
    keys: Sequence[str],
    tie_breakers: Sequence[SortKey],
) -> TableLike:
    """Select the first row per key after stable sorting.

    Parameters
    ----------
    table:
        Input table.
    keys:
        Key columns for dedupe groups.
    tie_breakers:
        Additional sort keys for deterministic ordering.

    Returns
    -------
    pyarrow.Table
        Deduplicated table.

    """
    if not keys:
        return table

    sort_keys = [SortKey(key, "ascending") for key in keys] + list(tie_breakers)
    t_sorted = canonical_sort(table, sort_keys=sort_keys)

    t_group, dict_cols = _decode_dictionary_columns(cast("pa.Table", t_sorted))
    non_keys = [col for col in t_group.column_names if col not in keys]
    aggs = [(col, "first") for col in non_keys]
    out = t_group.group_by(list(keys), use_threads=False).aggregate(aggs)

    new_names: list[str] = []
    for name in out.schema.names:
        if name.endswith("_first") and name[: -len("_first")] in non_keys:
            new_names.append(name[: -len("_first")])
        else:
            new_names.append(name)
    out = out.rename_columns(new_names)
    if dict_cols:
        out = _reencode_dictionary_columns(out, dict_cols)
    return out


def dedupe_keep_arbitrary(table: TableLike, *, keys: Sequence[str]) -> TableLike:
    """Select an arbitrary row per key (fast, non-deterministic).

    Parameters
    ----------
    table:
        Input table.
    keys:
        Key columns for dedupe groups.

    Returns
    -------
    pyarrow.Table
        Deduplicated table.

    """
    if not keys:
        return table
    non_keys = [col for col in table.column_names if col not in keys]
    aggs = [(col, "first") for col in non_keys]
    out = table.group_by(list(keys), use_threads=True).aggregate(aggs)

    new_names: list[str] = []
    for name in out.schema.names:
        if name.endswith("_first") and name[: -len("_first")] in non_keys:
            new_names.append(name[: -len("_first")])
        else:
            new_names.append(name)
    return out.rename_columns(new_names)


def dedupe_keep_best_by_score(
    table: TableLike,
    *,
    keys: Sequence[str],
    score_col: str,
    score_order: Literal["ascending", "descending"] = "descending",
    tie_breakers: Sequence[SortKey] = (),
) -> TableLike:
    """Select the best row per key based on a score column.

    Parameters
    ----------
    table:
        Input table.
    keys:
        Key columns for dedupe groups.
    score_col:
        Score column name.
    score_order:
        Sort order for the score ("ascending" or "descending").
    tie_breakers:
        Additional sort keys for deterministic ordering.

    Returns
    -------
    pyarrow.Table
        Deduplicated table.

    Raises
    ------
    KeyError
        Raised when the aggregated score column cannot be located.
    """
    if not keys:
        return table

    agg_fn = "max" if score_order == "descending" else "min"
    best = table.group_by(list(keys), use_threads=False).aggregate([(score_col, agg_fn)])

    best_score_name = f"{score_col}_{agg_fn}"
    if best_score_name not in best.column_names:
        candidates = [
            col for col in best.column_names if col.startswith(score_col) and col != score_col
        ]
        if not candidates:
            msg = f"Could not find aggregated score column for {score_col!r} using {agg_fn!r}."
            raise KeyError(msg)
        best_score_name = candidates[0]

    joined = apply_join(
        table,
        best,
        spec=join_spec(
            join_type="inner",
            left_keys=keys,
            right_keys=keys,
            output=JoinOutputSpec(
                left_output=table.column_names,
                right_output=(best_score_name,),
            ),
        ),
        use_threads=True,
    )
    mask = pc.equal(joined[score_col], joined[best_score_name])
    mask = pc.fill_null(mask, fill_value=False)
    filtered = joined.filter(mask).drop([best_score_name])

    if tie_breakers:
        return dedupe_keep_first_after_sort(filtered, keys=keys, tie_breakers=tie_breakers)

    return dedupe_keep_first_after_sort(
        filtered,
        keys=keys,
        tie_breakers=(SortKey(score_col, score_order),),
    )


def dedupe_collapse_list(table: TableLike, *, keys: Sequence[str]) -> TableLike:
    """Collapse duplicate rows into list-valued columns.

    Parameters
    ----------
    table:
        Input table.
    keys:
        Key columns for dedupe groups.

    Returns
    -------
    pyarrow.Table
        Deduplicated table with list-valued columns.
    """
    if not keys:
        return table
    non_keys = [col for col in table.column_names if col not in keys]
    aggs = [(col, "list") for col in non_keys]
    out = table.group_by(list(keys), use_threads=False).aggregate(aggs)

    new_names: list[str] = []
    for name in out.schema.names:
        if name.endswith("_list") and name[: -len("_list")] in non_keys:
            new_names.append(name[: -len("_list")])
        else:
            new_names.append(name)
    return out.rename_columns(new_names)


def apply_dedupe(
    table: TableLike,
    *,
    spec: DedupeSpec,
    _ctx: ExecutionContext | None = None,
) -> TableLike:
    """Apply the configured dedupe strategy.

    Parameters
    ----------
    table:
        Input table.
    spec:
        Dedupe specification.
    _ctx:
        Optional execution context (reserved for future use).

    Returns
    -------
    pyarrow.Table
        Deduplicated table.

    Raises
    ------
    ValueError
        Raised when the dedupe strategy is unknown or misconfigured.
    """
    ordering = _require_explicit_ordering(table.schema, kernel="dedupe")
    spec = _dedupe_spec_with_ordering(spec, ordering)
    metadata = _metadata_spec_from_table(table)
    strategy = spec.strategy
    if strategy == "KEEP_FIRST_AFTER_SORT":
        result = dedupe_keep_first_after_sort(
            table,
            keys=spec.keys,
            tie_breakers=spec.tie_breakers,
        )
        return _apply_metadata(result, metadata=metadata)
    if strategy == "KEEP_ARBITRARY":
        result = dedupe_keep_arbitrary(table, keys=spec.keys)
        return _apply_metadata(result, metadata=metadata)
    if strategy == "COLLAPSE_LIST":
        result = dedupe_collapse_list(table, keys=spec.keys)
        return _apply_metadata(result, metadata=metadata)
    if strategy == "KEEP_BEST_BY_SCORE":
        if not spec.tie_breakers:
            msg = "KEEP_BEST_BY_SCORE requires tie_breakers with a score column as the first entry."
            raise ValueError(msg)
        score = spec.tie_breakers[0]
        rest = spec.tie_breakers[1:]
        result = dedupe_keep_best_by_score(
            table,
            keys=spec.keys,
            score_col=score.column,
            score_order=score.order,
            tie_breakers=rest,
        )
        return _apply_metadata(result, metadata=metadata)
    msg = f"Unknown dedupe strategy: {strategy!r}."
    raise ValueError(msg)


def winner_select_by_score(
    table: TableLike,
    *,
    keys: Sequence[str],
    score_col: str = "score",
    score_order: Literal["ascending", "descending"] = "descending",
    tie_breakers: Sequence[SortKey] = (),
) -> TableLike:
    """Select a single winner per key group based on score and tie breakers.

    Returns
    -------
    TableLike
        Winner-selected table.
    """
    spec = DedupeSpec(
        keys=tuple(keys),
        strategy="KEEP_BEST_BY_SCORE",
        tie_breakers=(SortKey(score_col, score_order), *tuple(tie_breakers)),
    )
    return apply_dedupe(table, spec=spec, _ctx=None)


def _unique_columns(names: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for name in names:
        if name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def _temp_name(base: str, existing: set[str]) -> str:
    name = base
    idx = 1
    while name in existing:
        name = f"{base}_{idx}"
        idx += 1
    existing.add(name)
    return name


def _ensure_column(
    table: TableLike,
    name: str,
    *,
    dtype: DataTypeLike | None = None,
) -> TableLike:
    if name in table.column_names:
        return table
    col_type = dtype if dtype is not None else pa.null()
    return _set_or_append_column(table, name, pa.nulls(table.num_rows, type=col_type))


def _call_function(name: str, args: Sequence[object]) -> ArrayLike:
    return cast("ArrayLike", pc.call_function(name, list(args)))


def _normalize_span_values(
    values: ArrayLike | ChunkedArrayLike,
) -> ArrayLike | ChunkedArrayLike:
    dtype = values.type
    if patypes.is_null(dtype) or patypes.is_boolean(dtype):
        return pa.nulls(len(values), type=pa.int64())
    if patypes.is_dictionary(dtype):
        decoded = _call_function("dictionary_decode", [values])
        return _normalize_span_values(decoded)
    if patypes.is_integer(dtype) or patypes.is_floating(dtype):
        return pc.cast(values, pa.int64(), safe=False)
    if patypes.is_string(dtype) or patypes.is_large_string(dtype):
        text = _call_function("utf8_trim", [pc.cast(values, pa.string(), safe=False)])
        mask = pc.match_substring_regex(text, _NUMERIC_REGEX)
        mask = pc.fill_null(mask, fill_value=False)
        sanitized = pc.if_else(mask, text, pa.scalar(None, type=pa.string()))
        numeric = pc.cast(sanitized, pa.float64(), safe=False)
        return pc.cast(numeric, pa.int64(), safe=False)
    return pa.nulls(len(values), type=pa.int64())


def _interval_match_mask(
    cfg: IntervalAlignOptions,
    *,
    left_start: ArrayLike | ChunkedArrayLike,
    left_end: ArrayLike | ChunkedArrayLike,
    right_start: ArrayLike | ChunkedArrayLike,
    right_end: ArrayLike | ChunkedArrayLike,
) -> ArrayLike | ChunkedArrayLike:
    if cfg.mode == "EXACT":
        return pc.and_(
            pc.equal(right_start, left_start),
            pc.equal(right_end, left_end),
        )
    if cfg.mode == "CONTAINED_BEST":
        return pc.and_(
            pc.greater_equal(right_start, left_start),
            pc.less_equal(right_end, left_end),
        )
    overlap_left = pc.less_equal(right_end, left_start)
    overlap_right = pc.greater_equal(right_start, left_end)
    return pc.invert(pc.or_(overlap_left, overlap_right))


def _resolve_right_name(names: Sequence[str], name: str, suffix: str) -> str:
    suffixed = f"{name}{suffix}"
    return suffixed if suffixed in names else name


def _column_type(table: TableLike, name: str) -> DataTypeLike | None:
    if name in table.column_names:
        return table.schema.field(name).type
    return None


def _build_output_names(
    left_keep: Sequence[str],
    right_keep: Sequence[str],
    cfg: IntervalAlignOptions,
) -> list[str]:
    output = _unique_columns([*left_keep, *right_keep])
    if cfg.emit_match_meta:
        if cfg.match_kind_col not in output:
            output.append(cfg.match_kind_col)
        if cfg.match_score_col not in output:
            output.append(cfg.match_score_col)
    return output


def _build_output_spec(
    prepared: _IntervalAlignPrepared,
    cfg: IntervalAlignOptions,
) -> _IntervalAlignOutputSpec:
    output_names = _build_output_names(prepared.left_keep, prepared.right_keep, cfg)
    fields: list[pa.Field] = []
    for name in output_names:
        if cfg.emit_match_meta and name == cfg.match_kind_col:
            dtype = pa.string()
        elif cfg.emit_match_meta and name == cfg.match_score_col:
            dtype = pa.float64()
        elif name in prepared.right_keep_set:
            dtype = _column_type(prepared.right, name)
        else:
            dtype = _column_type(prepared.left, name)
        if dtype is None:
            dtype = pa.null()
        fields.append(pa.field(name, dtype))
    return _IntervalAlignOutputSpec(
        output_names=output_names,
        output_schema=pa.schema(fields),
        right_keep=prepared.right_keep_set,
        cfg=cfg,
        score_col=prepared.score_col,
    )


def _build_matched_output(
    matched: TableLike,
    spec: _IntervalAlignOutputSpec,
) -> TableLike:
    arrays: list[ArrayLike] = []
    column_names = list(matched.column_names)
    for name in spec.output_names:
        field_type = spec.output_schema.field(name).type
        if spec.cfg.emit_match_meta and name == spec.cfg.match_kind_col:
            if name in matched.column_names:
                arrays.append(matched[name])
            else:
                arrays.append(pa.nulls(matched.num_rows, type=field_type))
            continue
        if spec.cfg.emit_match_meta and name == spec.cfg.match_score_col:
            if spec.score_col in matched.column_names:
                arrays.append(matched[spec.score_col])
            else:
                arrays.append(pa.nulls(matched.num_rows, type=field_type))
            continue
        if name in spec.right_keep:
            resolved = _resolve_right_name(column_names, name, spec.cfg.right_suffix)
            if resolved in matched.column_names:
                arrays.append(matched[resolved])
            else:
                arrays.append(pa.nulls(matched.num_rows, type=field_type))
            continue
        if name in matched.column_names:
            arrays.append(matched[name])
            continue
        arrays.append(pa.nulls(matched.num_rows, type=field_type))
    return pa.Table.from_arrays(arrays, schema=spec.output_schema)


def _left_only_match_kind(left_only: TableLike, cfg: IntervalAlignOptions) -> ArrayLike:
    left_path = left_only[cfg.left_path_col]
    left_start = _normalize_span_values(left_only[cfg.left_start_col])
    left_end = _normalize_span_values(left_only[cfg.left_end_col])
    has_path = pc.invert(pc.is_null(left_path))
    has_span = pc.and_(
        pc.invert(pc.is_null(left_start)),
        pc.invert(pc.is_null(left_end)),
    )
    valid = pc.and_(has_path, has_span)
    return pc.if_else(
        valid,
        pa.scalar("NO_MATCH", type=pa.string()),
        pa.scalar("NO_PATH_OR_SPAN", type=pa.string()),
    )


def _build_left_only_output(
    left_only: TableLike,
    spec: _IntervalAlignOutputSpec,
    match_kind: ArrayLike | None,
) -> TableLike:
    arrays: list[ArrayLike] = []
    for name in spec.output_names:
        field_type = spec.output_schema.field(name).type
        if spec.cfg.emit_match_meta and name == spec.cfg.match_kind_col:
            arrays.append(match_kind or pa.nulls(left_only.num_rows, type=field_type))
            continue
        if spec.cfg.emit_match_meta and name == spec.cfg.match_score_col:
            arrays.append(pa.nulls(left_only.num_rows, type=field_type))
            continue
        if name in spec.right_keep:
            arrays.append(pa.nulls(left_only.num_rows, type=field_type))
            continue
        if name in left_only.column_names:
            arrays.append(left_only[name])
            continue
        arrays.append(pa.nulls(left_only.num_rows, type=field_type))
    return pa.Table.from_arrays(arrays, schema=spec.output_schema)


def _prepare_interval_tables(
    left: TableLike,
    right: TableLike,
    cfg: IntervalAlignOptions,
) -> _IntervalAlignPrepared:
    left_keep = list(cfg.select_left or left.column_names)
    right_keep = list(cfg.select_right or right.column_names)
    right_keep_set = set(right_keep)
    tie_breaker_cols = [sk.column for sk in cfg.tie_breakers]

    existing = set(left.column_names) | set(right.column_names)
    left_key_col = _temp_name("__left_path_key", existing)
    right_key_col = _temp_name("__right_path_key", existing)
    left_id_col = _temp_name("__left_id", existing)
    score_col = (
        cfg.match_score_col if cfg.emit_match_meta else _temp_name("__match_score", existing)
    )

    left_required = set(left_keep) | {cfg.left_path_col, cfg.left_start_col, cfg.left_end_col}
    right_required = set(right_keep) | {
        cfg.right_path_col,
        cfg.right_start_col,
        cfg.right_end_col,
        *tie_breaker_cols,
    }

    left_table = left
    for name in left_required:
        left_table = _ensure_column(left_table, name)
    right_table = right
    for name in right_required:
        right_table = _ensure_column(right_table, name)

    left_table = _set_or_append_column(
        left_table,
        left_key_col,
        pc.cast(left_table[cfg.left_path_col], pa.string(), safe=False),
    )
    right_table = _set_or_append_column(
        right_table,
        right_key_col,
        pc.cast(right_table[cfg.right_path_col], pa.string(), safe=False),
    )
    left_table = _set_or_append_column(
        left_table,
        left_id_col,
        pa.array(range(left_table.num_rows), type=pa.int64()),
    )

    return _IntervalAlignPrepared(
        left=left_table,
        right=right_table,
        left_keep=left_keep,
        right_keep=right_keep,
        right_keep_set=right_keep_set,
        left_key_col=left_key_col,
        right_key_col=right_key_col,
        left_id_col=left_id_col,
        score_col=score_col,
        tie_breaker_cols=tie_breaker_cols,
    )


def _join_interval_candidates(
    prepared: _IntervalAlignPrepared,
    cfg: IntervalAlignOptions,
) -> TableLike:
    left_join_cols = _unique_columns(
        [
            *prepared.left_keep,
            cfg.left_start_col,
            cfg.left_end_col,
            prepared.left_id_col,
        ]
    )
    right_join_cols = _unique_columns(
        [
            *prepared.right_keep,
            cfg.right_start_col,
            cfg.right_end_col,
            *prepared.tie_breaker_cols,
        ]
    )
    join_config = JoinConfig.from_sequences(
        left_keys=(prepared.left_key_col,),
        right_keys=(prepared.right_key_col,),
        left_output=left_join_cols,
        right_output=right_join_cols,
        output_suffix_for_right=cfg.right_suffix,
    )
    return interval_join_candidates(
        prepared.left,
        prepared.right,
        config=join_config,
        join_type="inner",
        use_threads=True,
    )


def _select_best_interval_matches(
    joined: TableLike,
    prepared: _IntervalAlignPrepared,
    cfg: IntervalAlignOptions,
) -> TableLike:
    right_start_name = _resolve_right_name(
        joined.column_names, cfg.right_start_col, cfg.right_suffix
    )
    right_end_name = _resolve_right_name(joined.column_names, cfg.right_end_col, cfg.right_suffix)

    match_mask = _interval_match_mask(
        cfg,
        left_start=_normalize_span_values(joined[cfg.left_start_col]),
        left_end=_normalize_span_values(joined[cfg.left_end_col]),
        right_start=_normalize_span_values(joined[right_start_name]),
        right_end=_normalize_span_values(joined[right_end_name]),
    )
    matched = joined.filter(match_mask)

    right_start = _normalize_span_values(matched[right_start_name])
    right_end = _normalize_span_values(matched[right_end_name])
    span_len = _call_function("subtract", [right_end, right_start])
    span_len = _call_function("max_element_wise", [span_len, pa.scalar(0, type=pa.int64())])
    match_score = _call_function(
        "multiply",
        [pc.cast(span_len, pa.float64()), pa.scalar(-1.0)],
    )
    matched = _set_or_append_column(matched, prepared.score_col, match_score)
    if cfg.emit_match_meta:
        matched = _set_or_append_column(
            matched,
            cfg.match_kind_col,
            _const_array(matched.num_rows, cfg.mode, dtype=pa.string()),
        )

    resolved_tie_breakers = tuple(
        SortKey(_resolve_right_name(matched.column_names, sk.column, cfg.right_suffix), sk.order)
        for sk in cfg.tie_breakers
    )
    return dedupe_keep_best_by_score(
        matched,
        keys=(prepared.left_id_col,),
        score_col=prepared.score_col,
        score_order="descending",
        tie_breakers=resolved_tie_breakers,
    )


def _interval_unmatched_left(
    prepared: _IntervalAlignPrepared,
    best: TableLike,
) -> TableLike:
    if best.num_rows == 0:
        return prepared.left
    matched_ids = best[prepared.left_id_col]
    matched_mask = pc.is_in(prepared.left[prepared.left_id_col], value_set=matched_ids)
    matched_mask = pc.fill_null(matched_mask, fill_value=False)
    return prepared.left.filter(pc.invert(matched_mask))


def interval_align_table(
    left: TableLike,
    right: TableLike,
    *,
    cfg: IntervalAlignOptions,
) -> TableLike:
    """Return interval-aligned rows using Arrow compute + join primitives.

    Returns
    -------
    TableLike
        Interval-aligned table.
    """
    prepared = _prepare_interval_tables(left, right, cfg)
    joined = _join_interval_candidates(prepared, cfg)
    best = _select_best_interval_matches(joined, prepared, cfg)
    output_spec = _build_output_spec(prepared, cfg)
    matched_out = _build_matched_output(best, output_spec)

    if cfg.how != "left":
        return matched_out

    unmatched = _interval_unmatched_left(prepared, best)
    match_kind = _left_only_match_kind(unmatched, cfg) if cfg.emit_match_meta else None
    left_only_out = _build_left_only_output(unmatched, output_spec, match_kind)
    if matched_out.num_rows == 0:
        return left_only_out
    if left_only_out.num_rows == 0:
        return matched_out
    return pa.concat_tables([matched_out, left_only_out], promote=True)


def _explode_list_indices(
    list_values: ArrayLike | ChunkedArrayLike,
    *,
    parent_idx: ArrayLike | ChunkedArrayLike,
    values_flat: ArrayLike | ChunkedArrayLike,
) -> ArrayLike | ChunkedArrayLike:
    lengths = pc.list_value_length(list_values)
    lengths_filled = pc.fill_null(lengths, 0)
    lengths_int = pc.cast(lengths_filled, pa.int64())
    end_offsets = pc.cumulative_sum(lengths_int)
    start_offsets = pc.subtract(end_offsets, lengths_int)
    positions = pa.array(range(len(values_flat)), type=pa.int64())
    parent_offsets = pc.take(start_offsets, parent_idx)
    return pc.subtract(positions, parent_offsets)


def _empty_list_indices(list_values: ArrayLike | ChunkedArrayLike) -> ArrayLike | ChunkedArrayLike:
    lengths = pc.list_value_length(list_values)
    lengths_filled = pc.fill_null(lengths, 0)
    is_empty = pc.equal(lengths_filled, 0)
    is_empty = pc.fill_null(is_empty, value=False)
    is_null = pc.is_null(lengths)
    empty_mask = pc.or_(is_empty, is_null)
    return pc.indices_nonzero(empty_mask)


def _explode_arrays(
    table: TableLike,
    *,
    spec: ExplodeSpec,
) -> tuple[list[ArrayLike | ChunkedArrayLike], list[str], ArrayLike | ChunkedArrayLike]:
    parent_idx = pc.list_parent_indices(table[spec.list_col])
    values_flat = pc.list_flatten(table[spec.list_col])
    arrays: list[ArrayLike | ChunkedArrayLike] = [
        pc.take(table[key], parent_idx) for key in spec.parent_keys
    ]
    names = list(spec.parent_keys)
    arrays.append(values_flat)
    names.append(spec.value_col)
    if spec.idx_col is not None:
        idx_values = _explode_list_indices(
            table[spec.list_col],
            parent_idx=parent_idx,
            values_flat=values_flat,
        )
        arrays.append(idx_values)
        names.append(spec.idx_col)
    return arrays, names, parent_idx


def _append_empty_rows(
    table: TableLike,
    *,
    result: pa.Table,
    spec: ExplodeSpec,
    row_id_name: str,
) -> pa.Table:
    empty_indices = _empty_list_indices(table[spec.list_col])
    empty_count = len(empty_indices)
    if not empty_count:
        return result
    empty_arrays: list[ArrayLike | ChunkedArrayLike] = [
        pc.take(table[key], empty_indices) for key in spec.parent_keys
    ]
    empty_names = list(spec.parent_keys)
    empty_arrays.append(pa.nulls(empty_count, type=result[spec.value_col].type))
    empty_names.append(spec.value_col)
    if spec.idx_col is not None:
        empty_arrays.append(pa.nulls(empty_count, type=pa.int64()))
        empty_names.append(spec.idx_col)
    empty_arrays.append(empty_indices)
    empty_names.append(row_id_name)
    empty_table = pa.Table.from_arrays(empty_arrays, names=empty_names)
    return pa.concat_tables([result, empty_table], promote=True)


def _sorted_explode_table(
    result: pa.Table,
    *,
    row_id_name: str,
    idx_col: str | None,
) -> pa.Table:
    order_keys = [(row_id_name, "ascending")]
    if idx_col is not None:
        order_keys.append((idx_col, "ascending"))
    return result.sort_by(order_keys).drop([row_id_name])


def explode_list_column(
    table: TableLike,
    *,
    spec: ExplodeSpec,
    out_parent_col: str | None = None,
) -> TableLike:
    """Explode a list column into parent/value/index rows.

    Parameters
    ----------
    table:
        Input table.
    spec:
        Explode specification describing parent keys and output columns.
    out_parent_col:
        Output parent column name.

    Returns
    -------
    pyarrow.Table
        Exploded table with deterministic ordering.
    """
    parent_rename = (
        out_parent_col
        if out_parent_col is not None and out_parent_col != spec.parent_keys[0]
        else None
    )
    if spec.list_col not in table.column_names:
        return table
    arrays, names, row_id = _explode_arrays(table, spec=spec)
    row_id_name = _temp_name("__row_id", set(names))
    arrays.append(row_id)
    names.append(row_id_name)
    result = pa.Table.from_arrays(arrays, names=names)

    if spec.keep_empty:
        result = _append_empty_rows(table, result=result, spec=spec, row_id_name=row_id_name)
    result = _sorted_explode_table(result, row_id_name=row_id_name, idx_col=spec.idx_col)
    if parent_rename is None:
        return result
    names = [parent_rename if name == spec.parent_keys[0] else name for name in result.column_names]
    return result.rename_columns(names)
