"""Semantic pipeline diagnostics helpers."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from typing import TYPE_CHECKING

from semantics.diagnostics import dataframe_row_count
from semantics.diagnostics_emission import (
    SemanticQualityDiagnosticsRequest,
    build_semantic_diagnostics_context,
    emit_semantic_quality_views,
    run_context_guard,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.ir import SemanticIR


def emit_semantic_quality_diagnostics(request: SemanticQualityDiagnosticsRequest) -> None:
    """Emit semantic quality diagnostics through canonical emission module."""
    if not request.runtime_profile.diagnostics.emit_semantic_quality_diagnostics:
        return
    context = build_semantic_diagnostics_context(
        request.ctx,
        runtime_profile=request.runtime_profile,
        dataset_resolver=request.dataset_resolver,
        schema_policy=request.schema_policy,
    )
    if context is None:
        return
    with run_context_guard():
        emit_semantic_quality_views(
            context,
            requested_outputs=request.requested_outputs,
            manifest=request.manifest,
            finalize_builder=request.finalize_builder,
        )


def _view_plan_stats(
    semantic_ir: SemanticIR,
    *,
    plan_bundles: Mapping[str, DataFusionPlanArtifact] | None,
) -> list[dict[str, object]]:
    if not plan_bundles:
        return []
    from datafusion_engine.identity import schema_identity_hash
    from datafusion_engine.views.bundle_extraction import arrow_schema_from_df

    rows: list[dict[str, object]] = []
    for view in semantic_ir.views:
        bundle = plan_bundles.get(view.name)
        if bundle is None:
            continue
        schema_hash = schema_identity_hash(arrow_schema_from_df(bundle.df))
        rows.append(
            {
                "name": view.name,
                "kind": view.kind,
                "inputs": view.inputs,
                "outputs": view.outputs,
                "plan_fingerprint": bundle.plan_fingerprint,
                "plan_identity_hash": bundle.plan_identity_hash,
                "required_udfs": tuple(bundle.required_udfs),
                "required_rewrite_tags": tuple(bundle.required_rewrite_tags),
                "schema_hash": schema_hash,
            }
        )
    return rows


def _semantic_explain_markdown(
    semantic_ir: SemanticIR,
    *,
    view_stats: Sequence[Mapping[str, object]],
) -> str:
    lines: list[str] = [
        "# Semantic Explain Plan",
        "",
        f"- semantic_model_hash: {semantic_ir.model_hash}",
        f"- semantic_ir_hash: {semantic_ir.ir_hash}",
        f"- view_count: {len(semantic_ir.views)}",
        f"- join_group_count: {len(semantic_ir.join_groups)}",
        "",
        "## Views",
        "| name | kind | inputs | outputs | plan_fingerprint |",
        "| --- | --- | --- | --- | --- |",
    ]
    stats_by_name = {row.get("name"): row for row in view_stats}
    view_lines = [
        "| "
        + " | ".join(
            [
                view.name,
                view.kind,
                ", ".join(view.inputs),
                ", ".join(view.outputs),
                str((stats_by_name.get(view.name, {}) or {}).get("plan_fingerprint") or ""),
            ]
        )
        + " |"
        for view in semantic_ir.views
    ]
    lines.extend(view_lines)
    if semantic_ir.join_groups:
        lines.extend(
            [
                "",
                "## Join Groups",
                "| name | left_view | right_view | how | relationships |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        group_lines = [
            "| "
            + " | ".join(
                [
                    group.name,
                    group.left_view,
                    group.right_view,
                    str(group.how),
                    ", ".join(group.relationship_names),
                ]
            )
            + " |"
            for group in semantic_ir.join_groups
        ]
        lines.extend(group_lines)
    return "\n".join(lines)


def record_semantic_compile_artifacts(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    semantic_ir: SemanticIR,
    requested_outputs: Collection[str] | None,
    plan_bundles: Mapping[str, DataFusionPlanArtifact] | None = None,
) -> None:
    """Record semantic compile artifacts and view/join diagnostics."""
    from datafusion_engine.identity import schema_identity_hash
    from datafusion_engine.lineage.diagnostics import record_artifact
    from datafusion_engine.views.bundle_extraction import arrow_schema_from_df
    from serde_artifact_specs import (
        SEMANTIC_EXPLAIN_PLAN_REPORT_SPEC,
        SEMANTIC_EXPLAIN_PLAN_SPEC,
        SEMANTIC_IR_FINGERPRINT_SPEC,
        SEMANTIC_JOIN_GROUP_STATS_SPEC,
        SEMANTIC_VIEW_PLAN_STATS_SPEC,
    )

    if semantic_ir.model_hash is None or semantic_ir.ir_hash is None:
        return

    payload = {
        "semantic_model_hash": semantic_ir.model_hash,
        "semantic_ir_hash": semantic_ir.ir_hash,
        "view_count": len(semantic_ir.views),
        "join_group_count": len(semantic_ir.join_groups),
        "requested_outputs": tuple(sorted(requested_outputs or ())),
    }
    record_artifact(runtime_profile, SEMANTIC_IR_FINGERPRINT_SPEC, payload)

    explain_payload = {
        "semantic_model_hash": semantic_ir.model_hash,
        "semantic_ir_hash": semantic_ir.ir_hash,
        "views": [
            {
                "name": view.name,
                "kind": view.kind,
                "inputs": view.inputs,
                "outputs": view.outputs,
            }
            for view in semantic_ir.views
        ],
        "join_groups": [
            {
                "name": group.name,
                "left_view": group.left_view,
                "right_view": group.right_view,
                "left_on": group.left_on,
                "right_on": group.right_on,
                "how": group.how,
                "relationship_names": group.relationship_names,
            }
            for group in semantic_ir.join_groups
        ],
    }
    record_artifact(runtime_profile, SEMANTIC_EXPLAIN_PLAN_SPEC, explain_payload)

    view_stats = _view_plan_stats(semantic_ir, plan_bundles=plan_bundles)
    if view_stats:
        record_artifact(
            runtime_profile,
            SEMANTIC_VIEW_PLAN_STATS_SPEC,
            {
                "semantic_model_hash": semantic_ir.model_hash,
                "semantic_ir_hash": semantic_ir.ir_hash,
                "views": view_stats,
            },
        )
    report = _semantic_explain_markdown(semantic_ir, view_stats=view_stats)
    record_artifact(
        runtime_profile,
        SEMANTIC_EXPLAIN_PLAN_REPORT_SPEC,
        {
            "semantic_model_hash": semantic_ir.model_hash,
            "semantic_ir_hash": semantic_ir.ir_hash,
            "markdown": report,
        },
    )

    for group in semantic_ir.join_groups:
        if not ctx.table_exist(group.name):
            continue
        join_df = ctx.table(group.name)
        row_count = dataframe_row_count(join_df)
        schema_hash = schema_identity_hash(arrow_schema_from_df(join_df))
        left_count = None
        right_count = None
        if ctx.table_exist(group.left_view):
            left_count = dataframe_row_count(ctx.table(group.left_view))
        if ctx.table_exist(group.right_view):
            right_count = dataframe_row_count(ctx.table(group.right_view))
        selectivity = None
        if left_count and right_count:
            denom = left_count * right_count
            if denom:
                selectivity = row_count / denom
        record_artifact(
            runtime_profile,
            SEMANTIC_JOIN_GROUP_STATS_SPEC,
            {
                "join_group": group.name,
                "left_view": group.left_view,
                "right_view": group.right_view,
                "row_count": row_count,
                "left_row_count": left_count,
                "right_row_count": right_count,
                "selectivity": selectivity,
                "schema_hash": schema_hash,
                "semantic_model_hash": semantic_ir.model_hash,
                "semantic_ir_hash": semantic_ir.ir_hash,
            },
        )


__all__ = [
    "SemanticQualityDiagnosticsRequest",
    "emit_semantic_quality_diagnostics",
    "record_semantic_compile_artifacts",
]
