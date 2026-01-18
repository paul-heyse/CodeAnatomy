"""DataFusion kernel adapters backed by DataFrame operations.

These kernels prefer DataFusion-native operations (windowing, aggregate, unnest)
and preserve schema metadata after execution. When a kernel cannot be expressed
via DataFusion operations, callers should fall back to Arrow kernel lanes.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, cast

import pyarrow as pa
from datafusion import SessionContext, col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr, SortExpr
from datafusion.expr import SortKey as DFSortKey

from arrowdsl.compute.expr_core import ExplodeSpec
from arrowdsl.core.context import ExecutionContext, Ordering, OrderingLevel
from arrowdsl.core.interop import SchemaLike, TableLike, pc
from arrowdsl.plan.ops import DedupeSpec, IntervalAlignOptions
from arrowdsl.plan.ops import SortKey as PlanSortKey
from arrowdsl.schema.metadata import (
    merge_metadata_specs,
    metadata_spec_from_schema,
    ordering_from_schema,
)
from arrowdsl.schema.schema import SchemaMetadataSpec
from datafusion_engine.bridge import datafusion_from_arrow
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.udf_registry import (
    _NORMALIZE_SPAN_UDF,
    _register_kernel_udfs,
    register_datafusion_udfs,
)

type KernelFn = Callable[..., TableLike]


def _session_context(ctx: ExecutionContext | None) -> SessionContext:
    if ctx is None or ctx.runtime.datafusion is None:
        session = DataFusionRuntimeProfile().session_context()
    else:
        session = ctx.runtime.datafusion.session_context()
    _register_kernel_udfs(session)
    return session


def _df_from_table(
    ctx: SessionContext,
    table: TableLike,
    *,
    name: str,
    batch_size: int | None = None,
) -> DataFrame:
    existing = _existing_table_names(ctx)
    table_name = _temp_name(name, existing) if name in existing else name
    return datafusion_from_arrow(ctx, name=table_name, value=table, batch_size=batch_size)


def _batch_size_from_ctx(ctx: ExecutionContext | None) -> int | None:
    if ctx is None or ctx.runtime.datafusion is None:
        return None
    return ctx.runtime.datafusion.batch_size


def _existing_table_names(ctx: SessionContext) -> set[str]:
    names: set[str] = set()
    for catalog_name in ctx.catalog_names():
        try:
            catalog = ctx.catalog(catalog_name)
        except KeyError:
            continue
        for schema_name in catalog.schema_names():
            schema = catalog.schema(schema_name)
            if schema is None:
                continue
            names.update(schema.table_names())
    return names


def _metadata_spec_from_tables(tables: Sequence[TableLike]) -> SchemaMetadataSpec | None:
    specs = [metadata_spec_from_schema(table.schema) for table in tables]
    merged = merge_metadata_specs(*specs)
    if not merged.schema_metadata and not merged.field_metadata:
        return None
    return merged


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
        PlanSortKey(col, _normalize_sort_order(order))
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


def _apply_metadata(
    table: TableLike,
    *,
    metadata: SchemaMetadataSpec | None,
) -> TableLike:
    if metadata is None:
        return table
    schema = metadata.apply(table.schema)
    return table.cast(schema)


def _ensure_column(table: TableLike, name: str, *, dtype: pa.DataType | None = None) -> TableLike:
    if name in table.column_names:
        return table
    col_type = dtype if dtype is not None else pa.null()
    return table.append_column(name, pa.nulls(table.num_rows, type=col_type))


def _temp_name(base: str, existing: set[str]) -> str:
    name = base
    idx = 1
    while name in existing:
        name = f"{base}_{idx}"
        idx += 1
    existing.add(name)
    return name


def _unique_columns(names: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for name in names:
        if name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def _sort_exprs(keys: Sequence[PlanSortKey]) -> list[SortExpr]:
    out: list[SortExpr] = []
    for key in keys:
        expr = col(key.column).sort(
            ascending=key.order == "ascending",
            nulls_first=key.order == "ascending",
        )
        out.append(expr)
    return out


def _order_exprs_for_dedupe(spec: DedupeSpec) -> list[SortExpr]:
    if spec.tie_breakers:
        return _sort_exprs(spec.tie_breakers)
    return _sort_exprs(tuple(PlanSortKey(key, "ascending") for key in spec.keys))


def _dedupe_dataframe(
    df: DataFrame,
    *,
    spec: DedupeSpec,
    columns: Sequence[str],
) -> DataFrame:
    if not spec.keys:
        return df
    if spec.strategy == "COLLAPSE_LIST":
        return _dedupe_collapse_list_dataframe(df, spec=spec, columns=columns)
    order_by: list[DFSortKey] | None = None
    if spec.strategy != "KEEP_ARBITRARY":
        order_by = []
        for expr in _order_exprs_for_dedupe(spec):
            order_by.append(expr)
    row_expr = f.row_number(partition_by=[col(key) for key in spec.keys], order_by=order_by)
    exprs = [col(name) for name in columns]
    exprs.append(row_expr.alias("__dedupe_rank"))
    ranked = df.select(*exprs)
    filtered = ranked.filter(col("__dedupe_rank") == lit(1))
    return filtered.select(*columns)


def _dedupe_collapse_list_dataframe(
    df: DataFrame,
    *,
    spec: DedupeSpec,
    columns: Sequence[str],
) -> DataFrame:
    if not spec.keys:
        return df
    group_by = [col(key) for key in spec.keys]
    non_keys = [name for name in columns if name not in spec.keys]
    if not non_keys:
        return df.select(*spec.keys).distinct()
    aggs = [f.array_agg(col(name)).alias(name) for name in non_keys]
    return df.aggregate(group_by, aggs)


def dedupe_kernel(
    table: TableLike,
    *,
    spec: DedupeSpec,
    _ctx: ExecutionContext | None = None,
) -> TableLike:
    """Apply DataFusion-native dedupe using window + aggregate ops.

    Returns
    -------
    TableLike
        Deduplicated table.
    """
    ctx = _session_context(_ctx)
    ordering = _require_explicit_ordering(table.schema, kernel="dedupe")
    resolved_spec = _dedupe_spec_with_ordering(spec, ordering)
    df = _df_from_table(
        ctx,
        table,
        name="dedupe",
        batch_size=_batch_size_from_ctx(_ctx),
    )
    columns = list(table.schema.names)
    result_df = _dedupe_dataframe(df, spec=resolved_spec, columns=columns)
    out = result_df.to_arrow_table()
    return _apply_metadata(out, metadata=_metadata_spec_from_tables([table]))


def explode_list_kernel(
    table: TableLike,
    *,
    spec: ExplodeSpec,
    out_parent_col: str | None = None,
    _ctx: ExecutionContext | None = None,
) -> TableLike:
    """Explode a list column using DataFusion unnest support.

    Parameters
    ----------
    table:
        Input table.
    spec:
        Explode specification describing parent keys and output columns.
    out_parent_col:
        Optional parent output column override.

    Returns
    -------
    TableLike
        Exploded table with optional index column.
    """
    parent_rename = (
        out_parent_col
        if out_parent_col is not None and out_parent_col != spec.parent_keys[0]
        else None
    )
    if spec.list_col not in table.column_names:
        return table
    ctx = _session_context(_ctx)
    df = _df_from_table(
        ctx,
        table,
        name="explode_list",
        batch_size=_batch_size_from_ctx(_ctx),
    )
    idx_list_name = _temp_name("__idx_list", set(table.column_names))
    df = df.with_column(
        idx_list_name,
        f.range(lit(0), f.cardinality(col(spec.list_col)), lit(1)),
    )
    exploded = df.unnest_columns(
        spec.list_col,
        idx_list_name,
        preserve_nulls=spec.keep_empty,
    )
    selected_exprs: list[Expr] = []
    for key in spec.parent_keys:
        if parent_rename is not None and key == spec.parent_keys[0]:
            selected_exprs.append(col(key).alias(parent_rename))
        else:
            selected_exprs.append(col(key))
    selected_exprs.append(col(spec.list_col).alias(spec.value_col))
    if spec.idx_col is not None:
        selected_exprs.append(col(idx_list_name).alias(spec.idx_col))
    selected = exploded.select(*selected_exprs)
    order_keys = (parent_rename,) if parent_rename is not None else spec.parent_keys
    order_exprs: list[SortExpr] = [
        col(key).sort(ascending=True, nulls_first=True) for key in order_keys
    ]
    if spec.idx_col is not None:
        order_exprs.append(col(spec.idx_col).sort(ascending=True, nulls_first=False))
    if order_exprs:
        selected = selected.sort(*order_exprs)
    out = selected.to_arrow_table()
    result = _apply_metadata(out, metadata=_metadata_spec_from_tables([table]))
    if parent_rename is None:
        return result
    names = [parent_rename if name == spec.parent_keys[0] else name for name in result.column_names]
    return result.rename_columns(names)


@dataclass(frozen=True)
class _IntervalAlignPrepared:
    left: pa.Table
    right: pa.Table
    left_keep: tuple[str, ...]
    right_keep: tuple[str, ...]
    left_key_col: str
    right_key_col: str
    left_id_col: str
    score_col: str
    right_suffix: str


@dataclass(frozen=True)
class _IntervalOutputContext:
    output_names: Sequence[str]
    prepared: _IntervalAlignPrepared
    cfg: IntervalAlignOptions
    right_name_map: Mapping[str, str]


def _prepare_interval_tables(
    left: TableLike,
    right: TableLike,
    cfg: IntervalAlignOptions,
) -> _IntervalAlignPrepared:
    left_keep = tuple(cfg.select_left or left.column_names)
    right_keep = tuple(cfg.select_right or right.column_names)
    existing = set(left.column_names) | set(right.column_names)
    left_key_col = _temp_name("__left_path_key", existing)
    right_key_col = _temp_name("__right_path_key", existing)
    left_id_col = _temp_name("__left_id", existing)
    score_col = (
        cfg.match_score_col if cfg.emit_match_meta else _temp_name("__match_score", existing)
    )

    left_required = set(left_keep) | {
        cfg.left_path_col,
        cfg.left_start_col,
        cfg.left_end_col,
    }
    right_required = set(right_keep) | {
        cfg.right_path_col,
        cfg.right_start_col,
        cfg.right_end_col,
        *(sk.column for sk in cfg.tie_breakers),
    }

    left_table = cast("pa.Table", left)
    for name in left_required:
        left_table = _ensure_column(left_table, name)
    right_table = cast("pa.Table", right)
    for name in right_required:
        right_table = _ensure_column(right_table, name)

    left_table = left_table.append_column(
        left_key_col,
        pc.cast(left_table[cfg.left_path_col], pa.string(), safe=False),
    )
    right_table = right_table.append_column(
        right_key_col,
        pc.cast(right_table[cfg.right_path_col], pa.string(), safe=False),
    )
    left_table = left_table.append_column(
        left_id_col,
        pa.array(range(left_table.num_rows), type=pa.int64()),
    )

    return _IntervalAlignPrepared(
        left=left_table,
        right=right_table,
        left_keep=left_keep,
        right_keep=right_keep,
        left_key_col=left_key_col,
        right_key_col=right_key_col,
        left_id_col=left_id_col,
        score_col=score_col,
        right_suffix=cfg.right_suffix,
    )


def _rename_right_columns(
    df: DataFrame,
    *,
    left_names: set[str],
    right_suffix: str,
) -> tuple[DataFrame, Mapping[str, str]]:
    mapping: dict[str, str] = {}
    current = set(df.schema().names) | set(left_names)
    for name in list(df.schema().names):
        if name in left_names:
            candidate = f"{name}{right_suffix}"
            new_name = _temp_name(candidate, current)
            df = df.with_column_renamed(name, new_name)
            mapping[name] = new_name
        else:
            mapping[name] = name
    return df, mapping


def _interval_match_mask(
    cfg: IntervalAlignOptions,
    *,
    left_start: Expr,
    left_end: Expr,
    right_start: Expr,
    right_end: Expr,
) -> Expr:
    if cfg.mode == "EXACT":
        return (right_start == left_start) & (right_end == left_end)
    if cfg.mode == "CONTAINED_BEST":
        return (right_start >= left_start) & (right_end <= left_end)
    overlap_left = right_end <= left_start
    overlap_right = right_start >= left_end
    return ~(overlap_left | overlap_right)


def _normalize_span_expr(column: str) -> Expr:
    return _NORMALIZE_SPAN_UDF(col(column).cast(pa.string()))


def _interval_order_exprs(
    *,
    score_col: str,
    tie_breakers: Sequence[PlanSortKey],
    right_name_map: Mapping[str, str],
) -> list[SortExpr]:
    order_exprs = [col(score_col).sort(ascending=False, nulls_first=False)]
    for tie in tie_breakers:
        resolved = right_name_map.get(tie.column, tie.column)
        order_exprs.append(
            col(resolved).sort(
                ascending=tie.order == "ascending",
                nulls_first=tie.order == "ascending",
            )
        )
    return order_exprs


def _interval_output_schema(
    prepared: _IntervalAlignPrepared,
    cfg: IntervalAlignOptions,
) -> pa.Schema:
    output_names = _unique_columns([*prepared.left_keep, *prepared.right_keep])
    if cfg.emit_match_meta:
        if cfg.match_kind_col not in output_names:
            output_names.append(cfg.match_kind_col)
        if cfg.match_score_col not in output_names:
            output_names.append(cfg.match_score_col)
    fields: list[pa.Field] = []
    left_schema = prepared.left.schema
    right_schema = prepared.right.schema
    for name in output_names:
        if cfg.emit_match_meta and name == cfg.match_kind_col:
            dtype = pa.string()
        elif cfg.emit_match_meta and name == cfg.match_score_col:
            dtype = pa.float64()
        elif name in prepared.right_keep and name in right_schema.names:
            dtype = right_schema.field(name).type
        elif name in left_schema.names:
            dtype = left_schema.field(name).type
        else:
            dtype = pa.null()
        fields.append(pa.field(name, dtype))
    return pa.schema(fields)


def _interval_join_frames(
    prepared: _IntervalAlignPrepared,
    *,
    ctx: SessionContext,
    batch_size: int | None = None,
) -> tuple[DataFrame, DataFrame, Mapping[str, str]]:
    left_df = _df_from_table(ctx, prepared.left, name="interval_left", batch_size=batch_size)
    right_df = _df_from_table(ctx, prepared.right, name="interval_right", batch_size=batch_size)
    right_df, right_name_map = _rename_right_columns(
        right_df,
        left_names=set(left_df.schema().names),
        right_suffix=prepared.right_suffix,
    )
    right_key_col = right_name_map.get(prepared.right_key_col, prepared.right_key_col)
    joined = left_df.join(
        right_df,
        how="inner",
        left_on=[prepared.left_key_col],
        right_on=[right_key_col],
        coalesce_duplicate_keys=False,
    )
    return left_df, joined, right_name_map


def _interval_best_matches(
    joined: DataFrame,
    *,
    prepared: _IntervalAlignPrepared,
    cfg: IntervalAlignOptions,
    right_name_map: Mapping[str, str],
) -> DataFrame:
    left_start = _normalize_span_expr(cfg.left_start_col)
    left_end = _normalize_span_expr(cfg.left_end_col)
    right_start = _normalize_span_expr(right_name_map.get(cfg.right_start_col, cfg.right_start_col))
    right_end = _normalize_span_expr(right_name_map.get(cfg.right_end_col, cfg.right_end_col))

    match_mask = _interval_match_mask(
        cfg,
        left_start=left_start,
        left_end=left_end,
        right_start=right_start,
        right_end=right_end,
    )
    matched = joined.filter(match_mask)
    span_len = right_end - right_start
    match_score = span_len * lit(-1.0)
    matched = matched.with_column(prepared.score_col, match_score)
    if cfg.emit_match_meta:
        matched = matched.with_column(cfg.match_kind_col, lit(cfg.mode))
    order_exprs = _interval_order_exprs(
        score_col=prepared.score_col,
        tie_breakers=cfg.tie_breakers,
        right_name_map=right_name_map,
    )
    order_by: list[DFSortKey] = list(order_exprs)
    rank_expr = f.row_number(
        partition_by=[col(prepared.left_id_col)],
        order_by=order_by,
    ).alias("__match_rank")
    ranked = matched.select(*[col(name) for name in matched.schema().names], rank_expr)
    return ranked.filter(col("__match_rank") == lit(1))


def _left_only_match_kind_expr(cfg: IntervalAlignOptions) -> Expr:
    has_path = col(cfg.left_path_col).is_not_null()
    has_span = _normalize_span_expr(cfg.left_start_col).is_not_null() & (
        _normalize_span_expr(cfg.left_end_col).is_not_null()
    )
    return (
        f.case(lit(value=True))
        .when(has_path & has_span, lit("NO_MATCH"))
        .otherwise(lit("NO_PATH_OR_SPAN"))
    )


def _interval_select_output(
    df: DataFrame,
    *,
    output_ctx: _IntervalOutputContext,
    left_only: bool,
) -> DataFrame:
    exprs: list[Expr] = []
    for name in output_ctx.output_names:
        if output_ctx.cfg.emit_match_meta and name == output_ctx.cfg.match_kind_col:
            if left_only:
                exprs.append(_left_only_match_kind_expr(output_ctx.cfg).alias(name))
            else:
                exprs.append(col(output_ctx.cfg.match_kind_col))
            continue
        if output_ctx.cfg.emit_match_meta and name == output_ctx.cfg.match_score_col:
            if left_only:
                exprs.append(lit(None).alias(name))
            else:
                exprs.append(col(output_ctx.prepared.score_col).alias(name))
            continue
        if name in output_ctx.prepared.right_keep:
            if left_only:
                exprs.append(lit(None).alias(name))
            else:
                resolved = output_ctx.right_name_map.get(name, name)
                exprs.append(col(resolved).alias(name))
            continue
        exprs.append(
            col(name) if left_only or name in output_ctx.prepared.left.schema.names else lit(None)
        )
    return df.select(*exprs)


def _interval_left_only_output(
    left_df: DataFrame,
    best: DataFrame,
    *,
    output_ctx: _IntervalOutputContext,
) -> DataFrame:
    matched_ids = best.select(col(output_ctx.prepared.left_id_col)).distinct()
    left_only = left_df.join(
        matched_ids,
        how="anti",
        left_on=[output_ctx.prepared.left_id_col],
        right_on=[output_ctx.prepared.left_id_col],
        coalesce_duplicate_keys=False,
    )
    return _interval_select_output(
        left_only,
        output_ctx=output_ctx,
        left_only=True,
    )


def interval_align_kernel(
    left: TableLike,
    right: TableLike,
    *,
    cfg: IntervalAlignOptions,
    _ctx: ExecutionContext | None = None,
) -> TableLike:
    """Align intervals using DataFusion joins + window ordering.

    Returns
    -------
    TableLike
        Interval-aligned table.
    """
    prepared = _prepare_interval_tables(left, right, cfg)
    ctx = _session_context(_ctx)
    left_df, joined, right_name_map = _interval_join_frames(prepared, ctx=ctx)
    best = _interval_best_matches(
        joined,
        prepared=prepared,
        cfg=cfg,
        right_name_map=right_name_map,
    )
    output_schema = _interval_output_schema(prepared, cfg)
    output_names = list(output_schema.names)
    output_ctx = _IntervalOutputContext(
        output_names=output_names,
        prepared=prepared,
        cfg=cfg,
        right_name_map=right_name_map,
    )
    matched_out = _interval_select_output(
        best,
        output_ctx=output_ctx,
        left_only=False,
    )
    if cfg.how == "left":
        left_only_out = _interval_left_only_output(
            left_df,
            best,
            output_ctx=output_ctx,
        )
        combined = matched_out.union(left_only_out)
    else:
        combined = matched_out
    out = combined.to_arrow_table()
    schema = output_schema
    metadata = _metadata_spec_from_tables([prepared.left, prepared.right])
    if metadata is not None:
        schema = metadata.apply(schema)
    return out.cast(schema)


def datafusion_kernel_registry() -> dict[str, KernelFn]:
    """Return available DataFusion kernel adapters.

    Returns
    -------
    dict[str, KernelFn]
        Kernel adapter registry.
    """
    return {
        "interval_align": interval_align_kernel,
        "explode_list": explode_list_kernel,
        "dedupe": dedupe_kernel,
    }


__all__ = [
    "datafusion_kernel_registry",
    "dedupe_kernel",
    "explode_list_kernel",
    "interval_align_kernel",
    "register_datafusion_udfs",
]
