"""Quality-aware semantic relationship compilation helpers."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from obs.otel.scopes import SCOPE_SEMANTICS
from obs.otel.tracing import stage_span

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext
    from datafusion.expr import Expr

    from semantics.exprs import ExprContextImpl, ExprSpec
    from semantics.ir import SemanticIRJoinGroup
    from semantics.quality import JoinHow, QualityRelationshipSpec


_LEFT_ALIAS_PREFIX = "l__"
_RIGHT_ALIAS_PREFIX = "r__"
_FILE_QUALITY_PREFIX = "fq__"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _QualityOutputContext:
    """Context for projecting quality relationship outputs."""

    rank_available: set[str]
    hard_columns: list[str]
    feature_columns: set[str]
    required_columns: set[str]


class TableInfoLike(Protocol):
    """Minimal table-info contract consumed by quality compilation."""

    df: DataFrame


class SemanticCompilerLike(Protocol):
    """Minimal compiler surface required by quality compilation helpers."""

    ctx: SessionContext

    def get_or_register(self, name: str) -> TableInfoLike: ...

    def resolve_join_keys(
        self,
        *,
        left_info: TableInfoLike,
        right_info: TableInfoLike,
        left_on: Sequence[str],
        right_on: Sequence[str],
    ) -> tuple[Sequence[str], Sequence[str]]: ...

    def validate_join_keys(
        self,
        *,
        left_info: TableInfoLike,
        right_info: TableInfoLike,
        left_on: Sequence[str],
        right_on: Sequence[str],
    ) -> None: ...

    def prefix_df(self, df: DataFrame, prefix: str) -> DataFrame: ...

    def schema_names(self, df: DataFrame) -> tuple[str, ...]: ...


def _build_joined_tables(
    compiler: SemanticCompilerLike,
    *,
    left_view: str,
    right_view: str,
    left_on: Sequence[str],
    right_on: Sequence[str],
    how: JoinHow,
) -> DataFrame:
    left_info = compiler.get_or_register(left_view)
    right_info = compiler.get_or_register(right_view)

    left_on, right_on = compiler.resolve_join_keys(
        left_info=left_info,
        right_info=right_info,
        left_on=left_on,
        right_on=right_on,
    )

    compiler.validate_join_keys(
        left_info=left_info,
        right_info=right_info,
        left_on=left_on,
        right_on=right_on,
    )

    left_aliased = compiler.prefix_df(left_info.df, _LEFT_ALIAS_PREFIX)
    right_aliased = compiler.prefix_df(right_info.df, _RIGHT_ALIAS_PREFIX)

    left_keys = [f"{_LEFT_ALIAS_PREFIX}{key}" for key in left_on]
    right_keys = [f"{_RIGHT_ALIAS_PREFIX}{key}" for key in right_on]

    return left_aliased.join(
        right_aliased,
        left_on=left_keys,
        right_on=right_keys,
        how=how,
    )


def build_join_group(
    compiler: SemanticCompilerLike,
    group: SemanticIRJoinGroup,
) -> DataFrame:
    """Build a shared join group for multiple relationships.

    Returns:
        DataFrame: Joined table shared by all relationships in the join group.
    """
    return _build_joined_tables(
        compiler,
        left_view=group.left_view,
        right_view=group.right_view,
        left_on=group.left_on,
        right_on=group.right_on,
        how=group.how,
    )


def compile_relationship_from_join(
    compiler: SemanticCompilerLike,
    joined: DataFrame,
    spec: QualityRelationshipSpec,
    *,
    file_quality_df: DataFrame | None = None,
) -> DataFrame:
    """Compile a relationship from a pre-joined DataFrame.

    Returns:
        DataFrame: Final projected relationship output with quality columns.
    """
    return compile_relationship_with_quality(
        compiler,
        spec,
        file_quality_df=file_quality_df,
        joined=joined,
    )


def _maybe_join_file_quality(
    compiler: SemanticCompilerLike,
    joined: DataFrame,
    *,
    spec: QualityRelationshipSpec,
    file_quality_df: DataFrame | None,
) -> DataFrame:
    if not spec.join_file_quality:
        return joined
    fq_df = file_quality_df
    if fq_df is None:
        try:
            fq_df = compiler.ctx.table(spec.file_quality_view)
        except (KeyError, RuntimeError, TypeError, ValueError):
            return joined
    if fq_df is None:
        return joined
    fq_aliased = compiler.prefix_df(fq_df, _FILE_QUALITY_PREFIX)
    left_file_id = f"{_LEFT_ALIAS_PREFIX}file_id"
    fq_file_id = f"{_FILE_QUALITY_PREFIX}file_id"
    return joined.join(
        fq_aliased,
        left_on=[left_file_id],
        right_on=[fq_file_id],
        how="left",
    )


def _record_expr_issue(exc: Exception, *, expr_label: str) -> None:
    logger.warning(
        "Semantic expression validation failed for %s (%s).",
        expr_label,
        exc,
    )


def _collect_expr_columns(
    expr_spec: ExprSpec,
    *,
    available_columns: set[str],
    expr_label: str,
) -> set[str] | None:
    from semantics.exprs import ExprValidationContext, validate_expr_spec

    try:
        validate_expr_spec(
            expr_spec,
            available_columns=available_columns,
            expr_label=expr_label,
        )
    except ValueError as exc:
        _record_expr_issue(exc, expr_label=expr_label)
        return None
    validation_ctx = ExprValidationContext()
    _ = expr_spec(validation_ctx)
    return validation_ctx.used_columns()


def _feature_columns(spec: QualityRelationshipSpec) -> set[str]:
    return {f"feat_{feature.name}" for feature in spec.signals.features}


def _rank_available(
    schema_names: set[str],
    feature_columns: set[str],
    spec: QualityRelationshipSpec,
) -> set[str]:
    rank_available = {
        *schema_names,
        *feature_columns,
        "score",
        "confidence",
        "origin",
        "provider",
    }
    if spec.rule_name is not None:
        rank_available.add("rule_name")
    return rank_available


def _initial_required_columns(spec: QualityRelationshipSpec) -> set[str]:
    required_columns: set[str] = set()
    required_columns.update({f"{_LEFT_ALIAS_PREFIX}{key}" for key in spec.left_on})
    required_columns.update({f"{_RIGHT_ALIAS_PREFIX}{key}" for key in spec.right_on})
    return required_columns


def _apply_hard_predicates(
    joined: DataFrame,
    *,
    spec: QualityRelationshipSpec,
    expr_ctx: ExprContextImpl,
    schema_names: set[str],
    required_columns: set[str],
) -> tuple[DataFrame, list[str], set[str]]:
    from datafusion import col

    hard_columns: list[str] = []
    for index, hard_pred in enumerate(spec.signals.hard, start=1):
        hard_required = _collect_expr_columns(
            hard_pred.predicate,
            available_columns=schema_names,
            expr_label=f"{spec.name}.hard[{index}]",
        )
        if hard_required is None:
            continue
        required_columns.update(hard_required)
        pred_expr = hard_pred.predicate(expr_ctx)
        hard_col = f"hard_{index}"
        try:
            joined = joined.with_column(hard_col, pred_expr)
            joined = joined.filter(col(hard_col))
        except (RuntimeError, TypeError, ValueError) as exc:
            _record_expr_issue(exc, expr_label=f"{spec.name}.hard[{index}]")
            continue
        hard_columns.append(hard_col)
    return joined, hard_columns, required_columns


def _apply_feature_columns(
    joined: DataFrame,
    *,
    spec: QualityRelationshipSpec,
    expr_ctx: ExprContextImpl,
    schema_names: set[str],
    required_columns: set[str],
) -> tuple[DataFrame, set[str]]:
    from datafusion import col, lit

    base_score = lit(spec.signals.base_score)
    for feature in spec.signals.features:
        feature_required = _collect_expr_columns(
            feature.expr,
            available_columns=schema_names,
            expr_label=f"{spec.name}.feature[{feature.name}]",
        )
        if feature_required is None:
            continue
        required_columns.update(feature_required)
        feature_expr = feature.expr(expr_ctx)
        feature_col = f"feat_{feature.name}"
        try:
            joined = joined.with_column(feature_col, feature_expr)
        except (RuntimeError, TypeError, ValueError) as exc:
            _record_expr_issue(exc, expr_label=f"{spec.name}.feature[{feature.name}]")
            continue
        base_score += col(feature_col) * lit(feature.weight)
    joined = joined.with_column("score", base_score)
    return joined, required_columns


def _apply_confidence(
    compiler: SemanticCompilerLike,
    joined: DataFrame,
    *,
    spec: QualityRelationshipSpec,
) -> DataFrame:
    from datafusion import col, functions, lit

    from semantics.exprs import clamp

    base_conf = lit(spec.signals.base_confidence)
    quality_col = f"{_FILE_QUALITY_PREFIX}{spec.signals.quality_score_column}"
    schema_names = set(compiler.schema_names(joined))
    if quality_col in schema_names:
        quality_adj = functions.coalesce(col(quality_col), lit(1000.0)) * lit(
            spec.signals.quality_weight
        )
        raw_conf = base_conf + (col("score") / lit(10000.0)) + quality_adj
    else:
        raw_conf = base_conf + (col("score") / lit(10000.0))
    conf_expr = clamp(raw_conf, min_value=lit(0.0), max_value=lit(1.0))
    return joined.with_column("confidence", conf_expr)


def _apply_metadata(joined: DataFrame, *, spec: QualityRelationshipSpec) -> DataFrame:
    from datafusion import lit

    joined = joined.with_column("origin", lit(spec.origin))
    joined = joined.with_column("provider", lit(spec.provider))
    if spec.rule_name is not None:
        joined = joined.with_column("rule_name", lit(spec.rule_name))
    return joined


def _apply_ranking(
    compiler: SemanticCompilerLike,
    joined: DataFrame,
    *,
    spec: QualityRelationshipSpec,
    expr_ctx: ExprContextImpl,
    rank_available: set[str],
    required_columns: set[str],
) -> tuple[DataFrame, set[str]]:
    if spec.rank is None:
        return joined, required_columns
    from datafusion import col, functions, lit
    from datafusion.expr import SortKey

    key_required = _collect_expr_columns(
        spec.rank.ambiguity_key_expr,
        available_columns=set(compiler.schema_names(joined)),
        expr_label=f"{spec.name}.rank.ambiguity_key_expr",
    )
    if key_required is None:
        return joined, required_columns
    required_columns.update(key_required)
    group_key = spec.rank.ambiguity_key_expr(expr_ctx)
    group_id_expr = group_key
    if spec.rank.ambiguity_group_id_expr is not None:
        group_id_required = _collect_expr_columns(
            spec.rank.ambiguity_group_id_expr,
            available_columns=set(compiler.schema_names(joined)),
            expr_label=f"{spec.name}.rank.ambiguity_group_id_expr",
        )
        if group_id_required is not None:
            required_columns.update(group_id_required)
            group_id_expr = spec.rank.ambiguity_group_id_expr(expr_ctx)
    joined = joined.with_column("ambiguity_group_id", group_id_expr)
    if not spec.rank.order_by:
        return joined, required_columns
    order_exprs: list[SortKey] = []
    for index, order in enumerate(spec.rank.order_by, start=1):
        order_required = _collect_expr_columns(
            order.expr,
            available_columns=rank_available,
            expr_label=f"{spec.name}.rank.order_by[{index}]",
        )
        if order_required is None:
            return joined, required_columns
        required_columns.update(order_required)
        order_exprs.append(order.expr(expr_ctx).sort(ascending=(order.direction == "asc")))
    row_num = functions.row_number(
        partition_by=[col("ambiguity_group_id")],
        order_by=order_exprs,
    )
    joined = joined.with_column("_rn", row_num)
    if spec.rank.keep == "best":
        joined = joined.filter(col("_rn") <= lit(spec.rank.top_k))
    joined = joined.drop("_rn")
    return joined, required_columns


def _existing_cols(schema_names: set[str], names: Sequence[str]) -> list[Expr]:
    from datafusion import col

    return [col(name) for name in names if name in schema_names]


def _select_quality_expr_output(
    compiler: SemanticCompilerLike,
    joined: DataFrame,
    *,
    spec: QualityRelationshipSpec,
    expr_ctx: ExprContextImpl,
    output_ctx: _QualityOutputContext,
) -> DataFrame:
    schema_names = set(compiler.schema_names(joined))
    select_available = {
        *output_ctx.rank_available,
        "ambiguity_group_id",
    }
    select_cols: list[Expr] = []
    for select_expr in spec.select_exprs:
        select_required = _collect_expr_columns(
            select_expr.expr,
            available_columns=select_available,
            expr_label=f"{spec.name}.select[{select_expr.alias}]",
        )
        if select_required is None:
            continue
        select_cols.append(select_expr.expr(expr_ctx).alias(select_expr.alias))
    select_cols.extend(_existing_cols(schema_names, ("confidence", "score", "origin", "provider")))
    if spec.rank is not None:
        select_cols.extend(_existing_cols(schema_names, ("ambiguity_group_id",)))
    if spec.rule_name is not None:
        select_cols.extend(_existing_cols(schema_names, ("rule_name",)))
    select_cols.extend(_existing_cols(schema_names, output_ctx.hard_columns))
    select_cols.extend(_existing_cols(schema_names, sorted(output_ctx.feature_columns)))
    return joined.select(*select_cols)


def _select_quality_default_output(
    compiler: SemanticCompilerLike,
    joined: DataFrame,
    *,
    spec: QualityRelationshipSpec,
    output_ctx: _QualityOutputContext,
) -> DataFrame:
    from datafusion import col

    schema_names = set(compiler.schema_names(joined))
    select_cols: list[Expr] = []
    seen: set[str] = set()

    def _append_col(name: str) -> None:
        if name in seen or name not in schema_names:
            return
        select_cols.append(col(name))
        seen.add(name)

    join_cols = [f"{_LEFT_ALIAS_PREFIX}{key}" for key in spec.left_on] + [
        f"{_RIGHT_ALIAS_PREFIX}{key}" for key in spec.right_on
    ]
    for name in join_cols:
        _append_col(name)
    for name in sorted(output_ctx.required_columns):
        _append_col(name)
    for name in ("confidence", "score", "origin", "provider"):
        _append_col(name)
    if spec.rule_name is not None:
        _append_col("rule_name")
    if spec.rank is not None:
        _append_col("ambiguity_group_id")
    for hard_col in output_ctx.hard_columns:
        _append_col(hard_col)
    for feature_name in sorted(output_ctx.feature_columns):
        _append_col(feature_name)
    return joined.select(*select_cols)


def _project_quality_output(
    compiler: SemanticCompilerLike,
    joined: DataFrame,
    *,
    spec: QualityRelationshipSpec,
    expr_ctx: ExprContextImpl,
    output_ctx: _QualityOutputContext,
) -> DataFrame:
    if spec.select_exprs:
        return _select_quality_expr_output(
            compiler,
            joined,
            spec=spec,
            expr_ctx=expr_ctx,
            output_ctx=output_ctx,
        )
    return _select_quality_default_output(
        compiler,
        joined,
        spec=spec,
        output_ctx=output_ctx,
    )


def _compile_relationship_with_quality_inner(
    compiler: SemanticCompilerLike,
    spec: QualityRelationshipSpec,
    *,
    file_quality_df: DataFrame | None,
    joined: DataFrame | None,
) -> DataFrame:
    if joined is None:
        joined = _build_joined_tables(
            compiler,
            left_view=spec.left_view,
            right_view=spec.right_view,
            left_on=spec.left_on,
            right_on=spec.right_on,
            how=spec.how,
        )

    joined = _maybe_join_file_quality(
        compiler,
        joined,
        spec=spec,
        file_quality_df=file_quality_df,
    )

    from semantics.exprs import ExprContextImpl

    expr_ctx = ExprContextImpl(left_alias="l", right_alias="r")
    schema_names = set(compiler.schema_names(joined))
    feature_columns = _feature_columns(spec)
    rank_available = _rank_available(schema_names, feature_columns, spec)
    required_columns = _initial_required_columns(spec)

    joined, hard_columns, required_columns = _apply_hard_predicates(
        joined,
        spec=spec,
        expr_ctx=expr_ctx,
        schema_names=schema_names,
        required_columns=required_columns,
    )

    joined, required_columns = _apply_feature_columns(
        joined,
        spec=spec,
        expr_ctx=expr_ctx,
        schema_names=schema_names,
        required_columns=required_columns,
    )

    joined = _apply_confidence(compiler, joined, spec=spec)
    joined = _apply_metadata(joined, spec=spec)

    joined, required_columns = _apply_ranking(
        compiler,
        joined,
        spec=spec,
        expr_ctx=expr_ctx,
        rank_available=rank_available,
        required_columns=required_columns,
    )

    output_ctx = _QualityOutputContext(
        rank_available=rank_available,
        hard_columns=hard_columns,
        feature_columns=feature_columns,
        required_columns=required_columns,
    )
    return _project_quality_output(
        compiler,
        joined,
        spec=spec,
        expr_ctx=expr_ctx,
        output_ctx=output_ctx,
    )


def compile_relationship_with_quality(
    compiler: SemanticCompilerLike,
    spec: QualityRelationshipSpec,
    *,
    file_quality_df: DataFrame | None = None,
    joined: DataFrame | None = None,
) -> DataFrame:
    """Compile a relationship with quality signals.

    Returns:
        DataFrame: Relationship output enriched with quality predicates and scores.
    """
    with stage_span(
        "semantics.compile_relationship_with_quality",
        stage="semantics",
        scope_name=SCOPE_SEMANTICS,
        attributes={
            "codeanatomy.spec_name": spec.name,
            "codeanatomy.left_view": spec.left_view,
            "codeanatomy.right_view": spec.right_view,
            "codeanatomy.join_how": spec.how,
            "codeanatomy.join_file_quality": spec.join_file_quality,
        },
    ):
        return _compile_relationship_with_quality_inner(
            compiler,
            spec,
            file_quality_df=file_quality_df,
            joined=joined,
        )


__all__ = [
    "build_join_group",
    "compile_relationship_from_join",
    "compile_relationship_with_quality",
]
