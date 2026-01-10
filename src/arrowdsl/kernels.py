"""Arrow table kernels for sorting, deduplication, and list helpers."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from .contracts import DedupeSpec, SortKey
from .runtime import DeterminismTier, ExecutionContext

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow as pa


def _sort_key_tuples(sort_keys: Sequence[SortKey]) -> list[tuple[str, str]]:
    """Build sort key tuples for Arrow sort_indices.

    Returns
    -------
    list[tuple[str, str]]
        Sort key tuples in (column, order) form.
    """
    return [(sk.column, sk.order) for sk in sort_keys]


def canonical_sort(table: pa.Table, *, sort_keys: Sequence[SortKey]) -> pa.Table:
    """Return a stable canonical ordering using sort indices.

    Returns
    -------
    pa.Table
        Sorted table when sort keys are provided.
    """
    import pyarrow.compute as pc

    if not sort_keys:
        return table
    idx = pc.sort_indices(table, sort_keys=_sort_key_tuples(sort_keys))
    return table.take(idx)


def canonical_sort_if_canonical(
    table: pa.Table, *, sort_keys: Sequence[SortKey], ctx: ExecutionContext
) -> pa.Table:
    """Sort only when determinism tier requires canonical ordering.

    Returns
    -------
    pa.Table
        Sorted table when canonical determinism is enabled.
    """
    if ctx.determinism == DeterminismTier.CANONICAL and sort_keys:
        return canonical_sort(table, sort_keys=sort_keys)
    return table


def dedupe_keep_first_after_sort(
    table: pa.Table, *, keys: Sequence[str], tie_breakers: Sequence[SortKey]
) -> pa.Table:
    """Select the first row per key after a stable sort.

    Returns
    -------
    pa.Table
        Table with one row per key.
    """
    if not keys:
        return table

    sort_keys = [SortKey(k, "ascending") for k in keys] + list(tie_breakers)
    t_sorted = canonical_sort(table, sort_keys=sort_keys)

    non_keys = [c for c in t_sorted.column_names if c not in keys]
    aggs = [(c, "first") for c in non_keys]
    out = t_sorted.group_by(list(keys), use_threads=False).aggregate(aggs)

    new_names: list[str] = []
    for name in out.schema.names:
        if name.endswith("_first") and name[: -len("_first")] in non_keys:
            new_names.append(name[: -len("_first")])
        else:
            new_names.append(name)
    return out.rename_columns(new_names)


def dedupe_keep_arbitrary(table: pa.Table, *, keys: Sequence[str]) -> pa.Table:
    """Select an arbitrary row per key.

    Returns
    -------
    pa.Table
        Table with one row per key.
    """
    if not keys:
        return table
    non_keys = [c for c in table.column_names if c not in keys]
    aggs = [(c, "first") for c in non_keys]
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
    score_order: str = "descending",
    tie_breakers: Sequence[SortKey] = (),
) -> pa.Table:
    """Select best-by-score rows with deterministic tie-breaks.

    Returns
    -------
    pa.Table
        Table with one row per key, prioritized by score.

    Raises
    ------
    KeyError
        Raised when the aggregated score column cannot be found.
    """
    import pyarrow.compute as pc

    if not keys:
        return table

    agg_fn = "max" if score_order == "descending" else "min"
    best = table.group_by(list(keys), use_threads=False).aggregate([(score_col, agg_fn)])

    best_score_name = f"{score_col}_{agg_fn}"
    if best_score_name not in best.column_names:
        candidates = [c for c in best.column_names if c.startswith(score_col) and c != score_col]
        if not candidates:
            raise KeyError(
                f"Could not find aggregated score column for {score_col!r} using {agg_fn!r}"
            )
        best_score_name = candidates[0]

    joined = table.join(best, keys=list(keys), join_type="inner", use_threads=True)

    mask = pc.equal(joined[score_col], joined[best_score_name])
    mask = pc.fill_null(mask, False)
    filtered = joined.filter(mask).drop([best_score_name])

    if tie_breakers:
        return dedupe_keep_first_after_sort(filtered, keys=keys, tie_breakers=tie_breakers)

    return dedupe_keep_first_after_sort(
        filtered,
        keys=keys,
        tie_breakers=(SortKey(score_col, score_order),),
    )


def dedupe_collapse_list(table: pa.Table, *, keys: Sequence[str]) -> pa.Table:
    """Collapse multiple rows per key into list-valued payload columns.

    Returns
    -------
    pa.Table
        Table with list-valued payloads per key.
    """
    if not keys:
        return table
    non_keys = [c for c in table.column_names if c not in keys]
    aggs = [(c, "list") for c in non_keys]
    out = table.group_by(list(keys), use_threads=False).aggregate(aggs)

    new_names: list[str] = []
    for name in out.schema.names:
        if name.endswith("_list") and name[: -len("_list")] in non_keys:
            new_names.append(name[: -len("_list")])
        else:
            new_names.append(name)
    return out.rename_columns(new_names)


def apply_dedupe(
    table: pa.Table, *, spec: DedupeSpec, ctx: ExecutionContext | None = None
) -> pa.Table:
    """Apply the configured dedupe strategy to a table.

    Returns
    -------
    pa.Table
        Deduplicated table.

    Raises
    ------
    ValueError
        Raised when the strategy is unknown or misconfigured.
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
            raise ValueError(
                "KEEP_BEST_BY_SCORE requires tie_breakers with a score column as the first entry"
            )
        score = spec.tie_breakers[0]
        rest = spec.tie_breakers[1:]
        return dedupe_keep_best_by_score(
            table,
            keys=spec.keys,
            score_col=score.column,
            score_order=score.order,
            tie_breakers=rest,
        )
    raise ValueError(f"Unknown dedupe strategy: {strategy!r}")


# --------------------------
# Nested/list helpers
# --------------------------


def explode_list_column(
    table: pa.Table,
    *,
    parent_id_col: str,
    list_col: str,
    out_parent_col: str = "src_id",
    out_value_col: str = "dst_id",
) -> pa.Table:
    """Explode a list column into parent/value rows.

    Returns
    -------
    pa.Table
        Table with repeated parent IDs and flattened values.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    parent_ids = table[parent_id_col]
    dst_lists = table[list_col]
    parent_idx = pc.list_parent_indices(dst_lists)
    dst_flat = pc.list_flatten(dst_lists)
    parent_rep = pc.take(parent_ids, parent_idx)
    return pa.Table.from_arrays([parent_rep, dst_flat], names=[out_parent_col, out_value_col])
