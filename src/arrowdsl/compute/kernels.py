"""Kernel-lane transforms for Arrow tables."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import pyarrow as pa
from schema_spec.specs import PROVENANCE_COLS

from arrowdsl.core.context import DeterminismTier, ExecutionContext
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    DataTypeLike,
    ScalarLike,
    TableLike,
    pc,
)
from arrowdsl.plan.ops import AggregateSpec, DedupeSpec, JoinSpec, SortKey


def provenance_sort_keys() -> tuple[str, ...]:
    """Return provenance columns used as canonical sort tie-breakers.

    Returns
    -------
    tuple[str, ...]
        Provenance column names for tie-breaking sorts.
    """
    return PROVENANCE_COLS


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


def coalesce_arrays(
    arrays: Sequence[ArrayLike | ChunkedArrayLike | ScalarLike],
) -> ArrayLike | ChunkedArrayLike:
    """Coalesce multiple arrays, taking the first non-null value per row.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Coalesced array.

    Raises
    ------
    ValueError
        Raised when no arrays are provided.
    """
    if not arrays:
        msg = "coalesce_arrays requires at least one array."
        raise ValueError(msg)
    return pc.coalesce(*arrays)


def drop_nulls(values: ArrayLike | ChunkedArrayLike) -> ArrayLike:
    """Drop null values from an array.

    Returns
    -------
    ArrayLike
        Array with nulls removed.
    """
    return pc.drop_null(values)


def severity_score_array(
    severity: ArrayLike | ChunkedArrayLike,
) -> ArrayLike | ChunkedArrayLike:
    """Map severity labels to float scores.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Float32 score array.
    """
    severity_str = cast_array(severity, pa.string())
    severity_str = pc.fill_null(severity_str, fill_value="ERROR")
    is_error = pc.equal(severity_str, pa.scalar("ERROR"))
    is_warning = pc.equal(severity_str, pa.scalar("WARNING"))
    score = pc.if_else(
        is_error, pa.scalar(1.0), pc.if_else(is_warning, pa.scalar(0.7), pa.scalar(0.5))
    )
    return cast_array(score, pa.float32())


def bitmask_flag_array(
    values: ArrayLike | ChunkedArrayLike,
    *,
    mask: int,
) -> ArrayLike | ChunkedArrayLike:
    """Return an int32 flag where the bitmask is set.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Int32 array with 1 where the bit is set, else 0.
    """
    roles = cast_array(values, pa.int64())
    flag = pc.not_equal(pc.bit_wise_and(roles, pa.scalar(mask)), pa.scalar(0))
    return cast_array(flag, pa.int32())


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


def valid_pair_mask(
    left: ArrayLike | ChunkedArrayLike,
    right: ArrayLike | ChunkedArrayLike,
) -> ArrayLike | ChunkedArrayLike:
    """Return a mask where both inputs are valid.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Boolean mask.
    """
    return pc.and_(pc.is_valid(left), pc.is_valid(right))


def masked_values(
    values: ArrayLike | ChunkedArrayLike,
    *,
    mask: ArrayLike | ChunkedArrayLike,
    dtype: DataTypeLike,
) -> ArrayLike | ChunkedArrayLike:
    """Return values where mask is true; null elsewhere.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Masked values.
    """
    return pc.if_else(mask, values, pa.scalar(None, type=dtype))


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


def apply_aggregate(table: TableLike, *, spec: AggregateSpec) -> TableLike:
    """Apply a group-by aggregation with shared naming policy.

    Parameters
    ----------
    table:
        Input table.
    spec:
        Aggregate specification.

    Returns
    -------
    pyarrow.Table
        Aggregated table.
    """
    out = table.group_by(list(spec.keys), use_threads=spec.use_threads).aggregate(list(spec.aggs))
    if not spec.rename_aggregates:
        return out
    rename_map = {f"{col}_{agg}": col for col, agg in spec.aggs if col not in spec.keys}
    new_names = [rename_map.get(name, name) for name in out.schema.names]
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
        spec=JoinSpec(
            join_type="inner",
            left_keys=tuple(keys),
            right_keys=tuple(keys),
            left_output=tuple(table.column_names),
            right_output=(best_score_name,),
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
