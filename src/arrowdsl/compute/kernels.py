"""Kernel-lane transforms for Arrow tables."""

from __future__ import annotations

import functools
import importlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, cast

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.core.context import DeterminismTier, ExecutionContext
from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike, DataTypeLike, TableLike, pc
from arrowdsl.plan.joins import JoinConfig, JoinOutputSpec, interval_join_candidates, join_spec
from arrowdsl.plan.ops import DedupeSpec, IntervalAlignOptions, JoinSpec, SortKey
from arrowdsl.schema.build import const_array, set_or_append_column
from schema_spec.specs import PROVENANCE_COLS


@dataclass(frozen=True)
class ChunkPolicy:
    """Normalization policy for dictionary encoding and chunking."""

    unify_dictionaries: bool = True
    combine_chunks: bool = True

    def apply(self, table: TableLike) -> TableLike:
        """Apply dictionary unification and chunk combination.

        Returns
        -------
        TableLike
            Normalized table with unified dictionaries and combined chunks.
        """
        out = table
        if self.unify_dictionaries:
            out = out.unify_dictionaries()
        if self.combine_chunks:
            out = out.combine_chunks()
        return out


_NUMERIC_REGEX = r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$"

type KernelFn = Callable[..., TableLike]


class KernelLane(StrEnum):
    """Execution lane for a kernel."""

    BUILTIN = "builtin"
    DF_UDF = "df_udf"
    ARROW_FALLBACK = "arrow_fallback"


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


_KERNEL_CAPABILITIES: dict[str, KernelCapability] = {
    "interval_align": KernelCapability(
        name="interval_align",
        lane=KernelLane.DF_UDF,
        volatility="stable",
        requires_ordering=True,
    ),
    "explode_list": KernelCapability(
        name="explode_list",
        lane=KernelLane.BUILTIN,
        volatility="stable",
        requires_ordering=False,
    ),
    "dedupe": KernelCapability(
        name="dedupe",
        lane=KernelLane.BUILTIN,
        volatility="stable",
        requires_ordering=True,
    ),
    "winner_select": KernelCapability(
        name="winner_select",
        lane=KernelLane.ARROW_FALLBACK,
        volatility="stable",
        requires_ordering=True,
    ),
}


def kernel_registry() -> dict[str, KernelFn]:
    """Return the Arrow kernel registry.

    Returns
    -------
    dict[str, KernelFn]
        Mapping of kernel names to Arrow implementations.
    """
    return {
        "interval_align": interval_align_table,
        "explode_list": explode_list_column,
        "dedupe": apply_dedupe,
        "winner_select": winner_select_by_score,
    }


def kernel_capability(name: str, *, ctx: ExecutionContext) -> KernelCapability:
    """Return the capability metadata for a kernel given a runtime context.

    Returns
    -------
    KernelCapability
        Capability metadata for the requested kernel.

    Raises
    ------
    KeyError
        Raised when the kernel name is unknown.
    """
    capability = _KERNEL_CAPABILITIES.get(name)
    if capability is None:
        msg = f"Unknown kernel capability: {name!r}."
        raise KeyError(msg)
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
    unique = pc.unique(values)
    indices = pc.sort_indices(unique)
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
    idx = pc.sort_indices(table, sort_keys=_sort_key_tuples(sort_keys))
    return table.take(idx)


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


def _select_join_outputs(joined: TableLike, *, spec: JoinSpec) -> TableLike:
    if not spec.left_output and not spec.right_output:
        return joined

    left_names = set(spec.left_output) | set(spec.left_keys)
    prefer_right_suffix = bool(spec.output_suffix_for_right) and any(
        name in left_names for name in spec.right_output
    )
    left_cols = _resolve_join_outputs(
        joined,
        outputs=spec.left_output,
        suffix=spec.output_suffix_for_left,
        prefer_suffix=False,
    )
    right_cols = _resolve_join_outputs(
        joined,
        outputs=spec.right_output,
        suffix=spec.output_suffix_for_right,
        prefer_suffix=prefer_right_suffix,
    )
    keep = [*left_cols, *right_cols]
    if keep:
        return joined.select(keep)
    return joined


def _resolve_join_outputs(
    joined: TableLike,
    *,
    outputs: Sequence[str],
    suffix: str,
    prefer_suffix: bool,
) -> list[str]:
    resolved: list[str] = []
    for name in outputs:
        if suffix and prefer_suffix:
            suffixed = f"{name}{suffix}"
            if suffixed in joined.column_names:
                resolved.append(suffixed)
                continue
        if name in joined.column_names:
            resolved.append(name)
            continue
        if suffix and not prefer_suffix:
            suffixed = f"{name}{suffix}"
            if suffixed in joined.column_names:
                resolved.append(suffixed)
    return resolved


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

    non_keys = [col for col in t_sorted.column_names if col not in keys]
    aggs = [(col, "first") for col in non_keys]
    out = t_sorted.group_by(list(keys), use_threads=False).aggregate(aggs)

    new_names: list[str] = []
    for name in out.schema.names:
        if name.endswith("_first") and name[: -len("_first")] in non_keys:
            new_names.append(name[: -len("_first")])
        else:
            new_names.append(name)
    return out.rename_columns(new_names)


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
    strategy = spec.strategy
    if strategy == "KEEP_FIRST_AFTER_SORT":
        return dedupe_keep_first_after_sort(table, keys=spec.keys, tie_breakers=spec.tie_breakers)
    if strategy == "KEEP_ARBITRARY":
        return dedupe_keep_arbitrary(table, keys=spec.keys)
    if strategy == "COLLAPSE_LIST":
        return dedupe_collapse_list(table, keys=spec.keys)
    if strategy == "KEEP_BEST_BY_SCORE":
        if not spec.tie_breakers:
            msg = "KEEP_BEST_BY_SCORE requires tie_breakers with a score column as the first entry."
            raise ValueError(msg)
        score = spec.tie_breakers[0]
        rest = spec.tie_breakers[1:]
        return dedupe_keep_best_by_score(
            table,
            keys=spec.keys,
            score_col=score.column,
            score_order=score.order,
            tie_breakers=rest,
        )
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
    return set_or_append_column(table, name, pa.nulls(table.num_rows, type=col_type))


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

    left_table = set_or_append_column(
        left_table,
        left_key_col,
        pc.cast(left_table[cfg.left_path_col], pa.string(), safe=False),
    )
    right_table = set_or_append_column(
        right_table,
        right_key_col,
        pc.cast(right_table[cfg.right_path_col], pa.string(), safe=False),
    )
    left_table = set_or_append_column(
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
    matched = set_or_append_column(matched, prepared.score_col, match_score)
    if cfg.emit_match_meta:
        matched = set_or_append_column(
            matched,
            cfg.match_kind_col,
            const_array(matched.num_rows, cfg.mode, dtype=pa.string()),
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


def explode_list_column(
    table: TableLike,
    *,
    parent_id_col: str,
    list_col: str,
    out_parent_col: str = "src_id",
    out_value_col: str = "dst_id",
) -> TableLike:
    """Explode a list column into parent/value pairs.

    Parameters
    ----------
    table:
        Input table.
    parent_id_col:
        Column containing parent IDs.
    list_col:
        Column containing list values.
    out_parent_col:
        Output parent column name.
    out_value_col:
        Output value column name.

    Returns
    -------
    pyarrow.Table
        Exploded table.
    """
    parent_ids = table[parent_id_col]
    dst_lists = table[list_col]
    parent_idx = pc.list_parent_indices(dst_lists)
    dst_flat = pc.list_flatten(dst_lists)
    parent_rep = pc.take(parent_ids, parent_idx)
    return pa.Table.from_arrays([parent_rep, dst_flat], names=[out_parent_col, out_value_col])
