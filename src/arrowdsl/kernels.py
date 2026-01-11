"""Kernel-lane transforms for Arrow tables."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

import pyarrow as pa
import pyarrow.compute as pc

from arrowdsl.contracts import DedupeSpec, SortKey
from arrowdsl.runtime import DeterminismTier, ExecutionContext


def _sort_key_tuples(sort_keys: Sequence[SortKey]) -> list[tuple[str, str]]:
    return [(sk.column, sk.order) for sk in sort_keys]


def canonical_sort(table: pa.Table, *, sort_keys: Sequence[SortKey]) -> pa.Table:
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


def canonical_sort_if_canonical(
    table: pa.Table,
    *,
    sort_keys: Sequence[SortKey],
    ctx: ExecutionContext,
) -> pa.Table:
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
        return canonical_sort(table, sort_keys=sort_keys)
    return table


def dedupe_keep_first_after_sort(
    table: pa.Table,
    *,
    keys: Sequence[str],
    tie_breakers: Sequence[SortKey],
) -> pa.Table:
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


def dedupe_keep_arbitrary(table: pa.Table, *, keys: Sequence[str]) -> pa.Table:
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
    table: pa.Table,
    *,
    keys: Sequence[str],
    score_col: str,
    score_order: Literal["ascending", "descending"] = "descending",
    tie_breakers: Sequence[SortKey] = (),
) -> pa.Table:
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

    joined = table.join(best, keys=list(keys), join_type="inner", use_threads=True)
    mask = pc.equal(joined[score_col], joined[best_score_name])
    mask = pc.fill_null(mask, replacement=False)
    filtered = joined.filter(mask).drop([best_score_name])

    if tie_breakers:
        return dedupe_keep_first_after_sort(filtered, keys=keys, tie_breakers=tie_breakers)

    return dedupe_keep_first_after_sort(
        filtered,
        keys=keys,
        tie_breakers=(SortKey(score_col, score_order),),
    )


def dedupe_collapse_list(table: pa.Table, *, keys: Sequence[str]) -> pa.Table:
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
    table: pa.Table,
    *,
    spec: DedupeSpec,
    _ctx: ExecutionContext | None = None,
) -> pa.Table:
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
    table: pa.Table,
    *,
    parent_id_col: str,
    list_col: str,
    out_parent_col: str = "src_id",
    out_value_col: str = "dst_id",
) -> pa.Table:
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
