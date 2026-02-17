"""DataFusion kernel adapters backed by DataFrame operations.

These kernels prefer DataFusion-native operations (windowing, aggregate, unnest)
and preserve schema metadata after execution. When a kernel cannot be expressed
via DataFusion operations, callers should fall back to Arrow kernel lanes.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa
from datafusion import SessionContext, col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr, SortExpr
from datafusion.expr import SortKey as DFSortKey

from arrow_utils.core.expr_types import ExplodeSpec
from arrow_utils.core.ordering import Ordering, OrderingLevel
from arrow_utils.core.schema_constants import PROVENANCE_COLS
from core_types import DeterminismTier
from datafusion_engine.arrow.interop import SchemaLike, TableLike
from datafusion_engine.arrow.metadata import (
    SchemaMetadataSpec,
    merge_metadata_specs,
    metadata_spec_from_schema,
    ordering_from_schema,
)
from datafusion_engine.sql.helpers import sql_identifier as _sql_identifier
from serde_msgspec import StructBaseStrict

# =============================================================================
# Kernel Specification Types (formerly kernel_specs.py)
# =============================================================================


class SortKey(StructBaseStrict, frozen=True):
    """Sort key specification for deterministic ordering."""

    column: str
    order: Literal["ascending", "descending"] = "ascending"


type DedupeStrategy = Literal[
    "KEEP_FIRST_AFTER_SORT",
    "KEEP_BEST_BY_SCORE",
    "COLLAPSE_LIST",
    "KEEP_ARBITRARY",
]


class DedupeSpec(StructBaseStrict, frozen=True):
    """Dedupe semantics for a table."""

    keys: tuple[str, ...]
    tie_breakers: tuple[SortKey, ...] = ()
    strategy: DedupeStrategy = "KEEP_FIRST_AFTER_SORT"


class IntervalAlignOptions(StructBaseStrict, frozen=True):
    """Interval alignment configuration."""

    mode: Literal["EXACT", "CONTAINED_BEST", "OVERLAP_BEST"] = "CONTAINED_BEST"
    how: Literal["inner", "left"] = "inner"

    left_path_col: str = "path"
    left_start_col: str = "bstart"
    left_end_col: str = "bend"

    right_path_col: str = "path"
    right_start_col: str = "bstart"
    right_end_col: str = "bend"

    select_left: tuple[str, ...] = ()
    select_right: tuple[str, ...] = ()

    tie_breakers: tuple[SortKey, ...] = ()

    emit_match_meta: bool = True
    match_kind_col: str = "match_kind"
    match_score_col: str = "match_score"
    right_suffix: str = "__r"


class AsofJoinSpec(StructBaseStrict, frozen=True):
    """As-of join specification for nearest-match joins."""

    on: str
    by: tuple[str, ...] = ()
    tolerance: object | None = None
    right_on: str | None = None
    right_by: tuple[str, ...] = ()


# Alias for backward compatibility
PlanSortKey = SortKey


# =============================================================================
# Kernel Implementation
# =============================================================================

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

type KernelFn = Callable[..., TableLike]

_SPAN_NUMERIC_REGEX = r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$"
MIN_JOIN_PARTITIONS: int = 2


def _session_context(runtime_profile: DataFusionRuntimeProfile | None) -> SessionContext:
    if runtime_profile is not None:
        return runtime_profile.session_runtime().ctx
    return SessionContext()


def _ensure_required_udfs(
    ctx: SessionContext,
    *,
    required: Sequence[str],
    allow_missing: bool = False,
) -> bool:
    if not required:
        return True
    from datafusion_engine.udf.extension_core import rust_udf_snapshot, validate_required_udfs

    try:
        snapshot = rust_udf_snapshot(ctx)
        validate_required_udfs(snapshot, required=required)
    except (ImportError, RuntimeError, TypeError, ValueError):
        if allow_missing:
            return False
        raise
    return True


def _df_from_table(
    ctx: SessionContext,
    table: TableLike,
    *,
    name: str,
    batch_size: int | None = None,
    ingest_hook: Callable[[Mapping[str, object]], None] | None = None,
) -> DataFrame:
    from datafusion_engine.io.ingest import datafusion_from_arrow

    existing = _existing_table_names(ctx)
    table_name = _temp_name(name, existing) if name in existing else name
    return datafusion_from_arrow(
        ctx,
        name=table_name,
        value=table,
        batch_size=batch_size,
        ingest_hook=ingest_hook,
    )


def _batch_size_from_profile(runtime_profile: DataFusionRuntimeProfile | None) -> int | None:
    if runtime_profile is None:
        return None
    return runtime_profile.execution.batch_size


def _arrow_ingest_hook(
    runtime_profile: DataFusionRuntimeProfile | None,
) -> Callable[[Mapping[str, object]], None] | None:
    from datafusion_engine.session.runtime_hooks import diagnostics_arrow_ingest_hook

    if runtime_profile is None:
        return None
    diagnostics = runtime_profile.diagnostics_sink()
    if diagnostics is None:
        return None
    return diagnostics_arrow_ingest_hook(diagnostics)


def _repartition_for_join(
    df: DataFrame,
    *,
    keys: Sequence[str],
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFrame:
    if not keys or runtime_profile is None:
        return df
    target_partitions = runtime_profile.execution.target_partitions
    if target_partitions is None or target_partitions < MIN_JOIN_PARTITIONS:
        return df
    join_policy = runtime_profile.policies.join_policy
    if join_policy is not None and not join_policy.repartition_joins:
        return df
    repartition_by_hash = getattr(df, "repartition_by_hash", None)
    if not callable(repartition_by_hash):
        return df
    exprs = [col(key) for key in keys]
    try:
        result = repartition_by_hash(*exprs, num=target_partitions)
    except (RuntimeError, TypeError, ValueError):
        return df
    return result if isinstance(result, DataFrame) else df


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


def _append_provenance_keys(
    table: TableLike,
    sort_keys: Sequence[SortKey],
) -> list[SortKey]:
    existing = {sk.column for sk in sort_keys}
    extras = [
        SortKey(col, "ascending")
        for col in PROVENANCE_COLS
        if col in table.column_names and col not in existing
    ]
    return [*sort_keys, *extras]


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


def _sort_exprs(keys: Sequence[PlanSortKey]) -> list[DFSortKey]:
    out: list[DFSortKey] = []
    for key in keys:
        expr = col(key.column).sort(
            ascending=key.order == "ascending",
            nulls_first=key.order == "ascending",
        )
        out.append(expr)
    return out


def _order_exprs_for_dedupe(spec: DedupeSpec) -> list[DFSortKey]:
    if spec.tie_breakers:
        return _sort_exprs(spec.tie_breakers)
    return _sort_exprs(tuple(PlanSortKey(key, "ascending") for key in spec.keys))


def _order_keys_for_dedupe(spec: DedupeSpec) -> list[SortKey]:
    if spec.tie_breakers:
        return list(spec.tie_breakers)
    return [SortKey(key, "ascending") for key in spec.keys]


def _dedupe_best_by_score_expr(df: DataFrame, *, spec: DedupeSpec) -> Expr:
    partition_by = ", ".join(_sql_identifier(key) for key in spec.keys)
    order_tokens: list[str] = []
    for key in _order_keys_for_dedupe(spec):
        direction = "DESC" if key.order == "descending" else "ASC"
        nulls = "NULLS FIRST" if key.order == "ascending" else "NULLS LAST"
        order_tokens.append(f"{_sql_identifier(key.column)} {direction} {nulls}")
    order_by = ", ".join(order_tokens)
    if order_by:
        sql = f"dedupe_best_by_score() OVER (PARTITION BY {partition_by} ORDER BY {order_by})"
    else:
        sql = f"dedupe_best_by_score() OVER (PARTITION BY {partition_by})"
    return df.parse_sql_expr(sql)


def _dedupe_dataframe(
    df: DataFrame,
    *,
    spec: DedupeSpec,
    columns: Sequence[str],
    allow_best_by_score: bool = True,
) -> DataFrame:
    if not spec.keys:
        return df
    if spec.strategy == "COLLAPSE_LIST":
        return _dedupe_collapse_list_dataframe(df, spec=spec, columns=columns)
    if spec.strategy == "KEEP_BEST_BY_SCORE" and allow_best_by_score:
        row_expr = _dedupe_best_by_score_expr(df, spec=spec)
    else:
        order_by: list[DFSortKey] | None = None
        if spec.strategy != "KEEP_ARBITRARY":
            order_by = []
            for expr in _order_exprs_for_dedupe(spec):
                order_by.append(expr)
        row_expr = f.row_number(
            partition_by=[col(key) for key in spec.keys],
            order_by=order_by,
        )
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
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> TableLike:
    """Apply DataFusion-native dedupe using window + aggregate ops.

    Returns:
    -------
    TableLike
        Deduplicated table.
    """
    ctx = _session_context(runtime_profile)
    ordering = _require_explicit_ordering(table.schema, kernel="dedupe")
    resolved_spec = _dedupe_spec_with_ordering(spec, ordering)
    allow_best_by_score = True
    if resolved_spec.strategy == "KEEP_BEST_BY_SCORE":
        allow_best_by_score = _ensure_required_udfs(
            ctx,
            required=("dedupe_best_by_score",),
            allow_missing=True,
        )
    df = _df_from_table(
        ctx,
        table,
        name="dedupe",
        batch_size=_batch_size_from_profile(runtime_profile),
        ingest_hook=_arrow_ingest_hook(runtime_profile),
    )
    columns = list(table.schema.names)
    result_df = _dedupe_dataframe(
        df,
        spec=resolved_spec,
        columns=columns,
        allow_best_by_score=allow_best_by_score,
    )
    out = result_df.to_arrow_table()
    return _apply_metadata(out, metadata=_metadata_spec_from_tables([table]))


@dataclass(frozen=True)
class WinnerSelectRequest:
    """Inputs required for winner-select kernel execution."""

    keys: Sequence[str]
    score_col: str = "score"
    score_order: Literal["ascending", "descending"] = "descending"
    tie_breakers: Sequence[SortKey] = ()
    runtime_profile: DataFusionRuntimeProfile | None = None


def winner_select_kernel(
    table: TableLike,
    *,
    request: WinnerSelectRequest,
) -> TableLike:
    """Select a single winner per key group based on score and tie breakers.

    Returns:
    -------
    TableLike
        Winner-selected table.
    """
    spec = DedupeSpec(
        keys=tuple(request.keys),
        strategy="KEEP_BEST_BY_SCORE",
        tie_breakers=(
            SortKey(request.score_col, request.score_order),
            *tuple(request.tie_breakers),
        ),
    )
    return dedupe_kernel(table, spec=spec, runtime_profile=request.runtime_profile)


def canonical_sort_if_canonical(
    table: TableLike,
    *,
    sort_keys: Sequence[SortKey],
    determinism_tier: DeterminismTier,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> TableLike:
    """Sort only when determinism is canonical.

    Returns:
    -------
    TableLike
        Sorted table when canonical; otherwise unchanged.
    """
    if determinism_tier != DeterminismTier.CANONICAL or not sort_keys:
        return table
    keys = _append_provenance_keys(table, sort_keys)
    df = _df_from_table(
        _session_context(runtime_profile),
        table,
        name="canonical_sort",
        batch_size=_batch_size_from_profile(runtime_profile),
        ingest_hook=_arrow_ingest_hook(runtime_profile),
    )
    order_exprs = [
        col(key.column).sort(
            ascending=key.order != "descending",
            nulls_first=False,
        )
        for key in keys
    ]
    sorted_df = df.sort(*order_exprs) if order_exprs else df
    out = sorted_df.to_arrow_table()
    return _apply_metadata(out, metadata=_metadata_spec_from_tables([table]))


def explode_list_kernel(
    table: TableLike,
    *,
    spec: ExplodeSpec,
    out_parent_col: str | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
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
    runtime_profile:
        Optional runtime profile for DataFusion execution.

    Returns:
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
    ctx = _session_context(runtime_profile)
    df = _df_from_table(
        ctx,
        table,
        name="explode_list",
        batch_size=_batch_size_from_profile(runtime_profile),
        ingest_hook=_arrow_ingest_hook(runtime_profile),
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
    left_path_col: str
    right_path_col: str
    left_start_col: str
    left_end_col: str


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
        left_path_col=cfg.left_path_col,
        right_path_col=cfg.right_path_col,
        left_start_col=cfg.left_start_col,
        left_end_col=cfg.left_end_col,
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
    text = f.trim(col(column).cast(pa.string()))
    is_numeric = f.regexp_like(text, lit(_SPAN_NUMERIC_REGEX))
    normalized = text.cast(pa.int64())
    default_value = lit(None).cast(pa.int64())
    return f.case(lit(value=True)).when(is_numeric, normalized).otherwise(default_value)


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
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> tuple[DataFrame, DataFrame, Mapping[str, str]]:
    left_df = _df_from_table(
        ctx,
        prepared.left,
        name="interval_left",
        batch_size=batch_size,
        ingest_hook=None,
    )
    left_df = left_df.with_column(
        prepared.left_key_col,
        col(prepared.left_path_col).cast(pa.string()),
    )
    left_id_order = _sort_exprs(
        (
            PlanSortKey(prepared.left_key_col, "ascending"),
            PlanSortKey(prepared.left_start_col, "ascending"),
            PlanSortKey(prepared.left_end_col, "ascending"),
        )
    )
    left_df = left_df.with_column(
        prepared.left_id_col,
        f.row_number(order_by=left_id_order) - lit(1),
    )
    right_df = _df_from_table(
        ctx,
        prepared.right,
        name="interval_right",
        batch_size=batch_size,
        ingest_hook=None,
    )
    right_df, right_name_map = _rename_right_columns(
        right_df,
        left_names=set(left_df.schema().names),
        right_suffix=prepared.right_suffix,
    )
    right_path_col = right_name_map.get(prepared.right_path_col, prepared.right_path_col)
    right_df = right_df.with_column(
        prepared.right_key_col,
        col(right_path_col).cast(pa.string()),
    )
    left_df = _repartition_for_join(
        left_df,
        keys=[prepared.left_key_col],
        runtime_profile=runtime_profile,
    )
    right_df = _repartition_for_join(
        right_df,
        keys=[prepared.right_key_col],
        runtime_profile=runtime_profile,
    )
    right_key_col = prepared.right_key_col
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
    from datafusion_engine.udf.expr import udf_expr

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
    match_score = udf_expr("interval_align_score", left_start, left_end, right_start, right_end)
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
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> TableLike:
    """Align intervals using DataFusion joins + window ordering.

    Returns:
    -------
    TableLike
        Interval-aligned table.
    """
    prepared = _prepare_interval_tables(left, right, cfg)
    ctx = _session_context(runtime_profile)
    _ensure_required_udfs(ctx, required=("interval_align_score",))
    left_df, joined, right_name_map = _interval_join_frames(
        prepared,
        ctx=ctx,
        runtime_profile=runtime_profile,
    )
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

    Returns:
    -------
    dict[str, KernelFn]
        Kernel adapter registry.
    """
    return {
        "interval_align": interval_align_kernel,
        "explode_list": explode_list_kernel,
        "dedupe": dedupe_kernel,
        "winner_select": winner_select_kernel,
    }


__all__ = [
    # Kernel specification types (formerly kernel_specs.py)
    "AsofJoinSpec",
    "DedupeSpec",
    "DedupeStrategy",
    "IntervalAlignOptions",
    "PlanSortKey",
    "SortKey",
    # Kernel functions
    "canonical_sort_if_canonical",
    "datafusion_kernel_registry",
    "dedupe_kernel",
    "explode_list_kernel",
    "interval_align_kernel",
    "winner_select_kernel",
]
