"""Builder and dispatch helpers for semantic pipeline construction."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from semantics import pipeline_cache as _pipeline_cache
from semantics import pipeline_dispatch as _pipeline_dispatch
from semantics.diagnostics import semantic_diagnostic_view_builders
from semantics.quality import QualityRelationshipSpec
from semantics.specs import RelationshipSpec
from semantics.view_kinds import ViewKind
from utils.hashing import hash_msgpack_canonical

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from datafusion_engine.plan.bundle_artifact import DataFrameBuilder, DataFusionPlanArtifact
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.views.artifacts import CachePolicy
    from semantics.config import SemanticConfig
    from semantics.ir import SemanticIR, SemanticIRJoinGroup
    from semantics.program_manifest import SemanticProgramManifest
    from semantics.registry import SemanticNormalizationSpec, SemanticSpecIndex


def _cache_policy_for(
    name: str,
    policy: Mapping[str, CachePolicy] | None,
) -> CachePolicy:
    return _pipeline_cache.cache_policy_for(name, policy)


def _normalize_cache_policy(policy: str) -> CachePolicy:
    return _pipeline_cache.normalize_cache_policy(policy)


def _bundle_for_builder(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    builder: DataFrameBuilder,
    view_name: str,
    semantic_ir: SemanticIR | None,
) -> DataFusionPlanArtifact:
    """Build a plan bundle for a DataFrame builder.

    Returns:
    -------
    DataFusionPlanArtifact
        Plan bundle for the builder output.
    """
    from datafusion_engine.plan.bundle_artifact import PlanBundleOptions, build_plan_artifact

    session_runtime = runtime_profile.session_runtime()
    df = builder(ctx)
    bundle = build_plan_artifact(
        ctx,
        df,
        options=PlanBundleOptions(
            compute_execution_plan=True,
            validate_udfs=True,
            session_runtime=session_runtime,
        ),
    )
    if semantic_ir is None:
        return bundle
    model_hash = semantic_ir.model_hash
    ir_hash = semantic_ir.ir_hash
    if model_hash is None or ir_hash is None:
        return bundle
    base_identity = bundle.plan_identity_hash or bundle.plan_fingerprint
    cache_payload = {
        "base_plan_hash": base_identity,
        "view_name": view_name,
        "semantic_model_hash": model_hash,
        "semantic_ir_hash": ir_hash,
    }
    semantic_cache_hash = hash_msgpack_canonical(cache_payload)
    return replace(bundle, plan_identity_hash=semantic_cache_hash)


def _normalize_builder(
    table: str,
    *,
    prefix: str,
    config: SemanticConfig | None,
) -> DataFrameBuilder:
    """Return a builder that normalizes one evidence table."""

    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.compiler import SemanticCompiler
        from semantics.table_registry import TableRegistry

        return SemanticCompiler(
            inner_ctx,
            config=config,
            table_registry=TableRegistry(),
        ).normalize(table, prefix=prefix)

    return _builder


def _normalize_spec_builder(
    spec: SemanticNormalizationSpec,
    *,
    input_mapping: Mapping[str, str],
    config: SemanticConfig | None,
) -> DataFrameBuilder:
    """Return a builder that applies one declarative normalization spec."""

    def _builder(inner_ctx: SessionContext) -> DataFrame:
        import msgspec

        from semantics.compiler import SemanticCompiler
        from semantics.table_registry import TableRegistry

        resolved_table = input_mapping.get(spec.source_table, spec.source_table)
        resolved_spec = msgspec.structs.replace(spec.spec, table=resolved_table)
        return SemanticCompiler(
            inner_ctx,
            config=config,
            table_registry=TableRegistry(),
        ).normalize_from_spec(resolved_spec)

    return _builder


def _relationship_builder(
    spec: RelationshipSpec | QualityRelationshipSpec,
    *,
    config: SemanticConfig | None,
    use_cdf: bool,
    join_group: SemanticIRJoinGroup | None = None,
) -> DataFrameBuilder:
    """Build a DataFrame builder from a declarative RelationshipSpec.

    Parameters
    ----------
    spec
        The declarative relationship specification.
    config
        Optional semantic configuration.
    use_cdf
        Whether to enable CDF-aware incremental joins.
    join_group
        Optional join-fusion group for shared joins.

    Returns:
    -------
    DataFrameBuilder
        A callable that builds the relationship DataFrame.
    """

    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.compiler import RelationOptions, SemanticCompiler
        from semantics.table_registry import TableRegistry

        compiler = SemanticCompiler(
            inner_ctx,
            config=config,
            table_registry=TableRegistry(),
        )
        if isinstance(spec, QualityRelationshipSpec):
            file_quality_df = None
            if spec.join_file_quality:
                from semantics.signals import build_file_quality_view

                try:
                    file_quality_df = inner_ctx.table(spec.file_quality_view)
                except (KeyError, OSError, RuntimeError, TypeError, ValueError):
                    file_quality_df = build_file_quality_view(inner_ctx)
            if join_group is None:
                return compiler.compile_relationship_with_quality(
                    spec,
                    file_quality_df=file_quality_df,
                )
            joined = inner_ctx.table(join_group.name)
            return compiler.compile_relationship_from_join(
                joined,
                spec,
                file_quality_df=file_quality_df,
            )

        return compiler.relate(
            spec.left_table,
            spec.right_table,
            options=RelationOptions(
                strategy_hint=spec.to_strategy_type(),
                filter_sql=spec.filter_sql,
                origin=spec.origin,
                use_cdf=use_cdf,
                output_name=spec.name,
            ),
        )

    return _builder


def _relation_output_builder(
    *,
    relationship_names: Sequence[str],
    relationship_by_name: Mapping[str, QualityRelationshipSpec],
) -> DataFrameBuilder:
    """Return a builder that unions relationship views into relation output."""

    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.catalog.projections import RelationOutputSpec, relation_output_projection

        frames: list[DataFrame] = []
        for rel_name in sorted(relationship_names):
            rel_df = inner_ctx.table(rel_name)
            spec = relationship_by_name.get(rel_name)
            origin = spec.origin if spec is not None else "semantic_compiler"
            output_spec = RelationOutputSpec(
                src_col="entity_id",
                dst_col="symbol",
                kind=rel_name,
                origin=origin,
                qname_source_col="qname_source",
                ambiguity_group_col="ambiguity_group_id",
            )
            frames.append(relation_output_projection(rel_df, output_spec))
        if not frames:
            msg = "No relationship views available for relation_output."
            raise ValueError(msg)
        result = frames[0]
        for frame in frames[1:]:
            result = result.union(frame)
        return result

    return _builder


def _finalize_df_to_contract(
    ctx: SessionContext,
    *,
    df: DataFrame,
    view_name: str,
) -> DataFrame:
    from datafusion import col, lit

    from datafusion_engine.expr.cast import safe_cast
    from schema_spec.dataset_spec import dataset_spec_contract
    from semantics.catalog.dataset_specs import dataset_spec

    _ = ctx
    spec = dataset_spec(view_name)
    contract = dataset_spec_contract(spec)
    target_schema = contract.schema
    existing = set(df.schema().names)
    selections = [
        (
            safe_cast(col(field.name), field.type).alias(field.name)
            if field.name in existing
            else safe_cast(lit(None), field.type).alias(field.name)
        )
        for field in target_schema
    ]
    return df.select(*selections)


def _finalize_output_builder(
    view_name: str,
    builder: DataFrameBuilder,
) -> DataFrameBuilder:
    """Return a builder that casts/selects output rows to dataset contract."""

    def _builder(inner_ctx: SessionContext) -> DataFrame:
        df = builder(inner_ctx)
        return _finalize_df_to_contract(inner_ctx, df=df, view_name=view_name)

    return _builder


def _join_group_builder(
    group: SemanticIRJoinGroup,
    *,
    config: SemanticConfig | None,
) -> DataFrameBuilder:
    """Return a builder that computes one fused join-group view."""

    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.compiler import SemanticCompiler
        from semantics.table_registry import TableRegistry

        return SemanticCompiler(
            inner_ctx,
            config=config,
            table_registry=TableRegistry(),
        ).build_join_group(group)

    return _builder


def _union_nodes_builder(
    names: list[str],
    *,
    config: SemanticConfig | None,
) -> DataFrameBuilder:
    """Return a builder that unions node-like semantic views."""

    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.compiler import SemanticCompiler
        from semantics.table_registry import TableRegistry

        return SemanticCompiler(
            inner_ctx,
            config=config,
            table_registry=TableRegistry(),
        ).union_nodes(
            names,
            discriminator="node_kind",
        )

    return _builder


def _union_edges_builder(
    names: list[str],
    *,
    config: SemanticConfig | None,
) -> DataFrameBuilder:
    """Return a builder that unions edge-like semantic views."""

    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.compiler import SemanticCompiler
        from semantics.table_registry import TableRegistry

        return SemanticCompiler(
            inner_ctx,
            config=config,
            table_registry=TableRegistry(),
        ).union_edges(
            names,
            discriminator="edge_kind",
        )

    return _builder


def _scip_norm_builder(
    table: str,
    *,
    line_index_table: str,
) -> DataFrameBuilder:
    """Return a builder that converts SCIP locations to byte spans."""

    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.scip_normalize import scip_to_byte_offsets

        return scip_to_byte_offsets(
            inner_ctx,
            occurrences_table=table,
            line_index_table=line_index_table,
        )

    return _builder


def _bytecode_line_table_builder(
    line_table: str,
    *,
    line_index_table: str,
) -> DataFrameBuilder:
    """Return a builder that enriches bytecode line-table spans."""

    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.bytecode_line_table import py_bc_line_table_with_bytes

        return py_bc_line_table_with_bytes(
            inner_ctx,
            line_table=line_table,
            line_index_table=line_index_table,
        )

    return _builder


def _ordered_semantic_specs(
    specs: Sequence[SemanticSpecIndex],
) -> tuple[SemanticSpecIndex, ...]:
    spec_by_name: dict[str, SemanticSpecIndex] = {}
    for spec in specs:
        if spec.name in spec_by_name:
            msg = f"Duplicate semantic spec name: {spec.name!r}."
            raise ValueError(msg)
        spec_by_name[spec.name] = spec

    outputs = set(spec_by_name)
    deps: dict[str, set[str]] = {}
    for name, spec in spec_by_name.items():
        deps[name] = {input_name for input_name in spec.inputs if input_name in outputs}

    ordered: list[SemanticSpecIndex] = []
    ready = sorted([name for name, required in deps.items() if not required])
    while ready:
        name = ready.pop(0)
        ordered.append(spec_by_name[name])
        for other, required in deps.items():
            if name not in required:
                continue
            required.remove(name)
            if not required and other not in ready:
                ready.append(other)
        ready.sort()

    if len(ordered) != len(spec_by_name):
        missing = sorted(name for name, required in deps.items() if required)
        msg = f"Semantic spec dependency cycle detected: {missing}."
        raise ValueError(msg)

    return tuple(ordered)


@dataclass(frozen=True)
class _SemanticSpecContext:
    normalization_by_output: Mapping[str, SemanticNormalizationSpec]
    relationship_by_name: Mapping[str, QualityRelationshipSpec]
    input_mapping: Mapping[str, str]
    config: SemanticConfig | None
    use_cdf: bool
    normalization_spec_for_output: Callable[[str], SemanticNormalizationSpec | None]
    join_groups_by_name: Mapping[str, SemanticIRJoinGroup]
    join_group_by_relationship: Mapping[str, SemanticIRJoinGroup]
    runtime_profile: DataFusionRuntimeProfile
    manifest: SemanticProgramManifest


def _semantic_view_specs(
    *,
    ordered_specs: Sequence[SemanticSpecIndex],
    context: _SemanticSpecContext,
) -> list[tuple[str, DataFrameBuilder]]:
    return _pipeline_dispatch.semantic_view_specs(
        ordered_specs=ordered_specs,
        context=context,
        builder_for_semantic_spec=lambda spec, resolved_context: _builder_for_semantic_spec(
            spec,
            context=resolved_context,
        ),
    )


# ---------------------------------------------------------------------------
# Builder dispatch factory
# ---------------------------------------------------------------------------


def _dispatch_from_registry(
    registry_factory: Callable[[_SemanticSpecContext], Mapping[str, DataFrameBuilder]],
    context_label: str,
    *,
    finalize: bool = False,
) -> Callable[[SemanticSpecIndex, _SemanticSpecContext], DataFrameBuilder]:
    return _pipeline_dispatch.dispatch_from_registry(
        registry_factory,
        context_label,
        finalize_builder=_finalize_output_builder if finalize else None,
    )


# ---------------------------------------------------------------------------
# Per-kind handlers (unique logic retained as individual functions)
# ---------------------------------------------------------------------------


def _builder_for_normalize_spec(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    norm_spec = context.normalization_by_output.get(
        spec.name
    ) or context.normalization_spec_for_output(spec.name)
    if norm_spec is None:
        msg = f"Missing normalization spec for output {spec.name!r}."
        raise KeyError(msg)
    return _normalize_spec_builder(
        norm_spec,
        input_mapping=context.input_mapping,
        config=context.config,
    )


def _builder_for_scip_normalize_spec(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    _ = spec
    return _scip_norm_builder(
        _input_table(context.input_mapping, "scip_occurrences"),
        line_index_table=_input_table(context.input_mapping, "file_line_index_v1"),
    )


def _builder_for_bytecode_line_index_spec(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    line_table = spec.inputs[0] if spec.inputs else "py_bc_line_table"
    line_index = spec.inputs[1] if len(spec.inputs) > 1 else "file_line_index_v1"
    return _bytecode_line_table_builder(
        _input_table(context.input_mapping, line_table),
        line_index_table=_input_table(context.input_mapping, line_index),
    )


def _span_unnest_registry(
    _context: _SemanticSpecContext,
) -> Mapping[str, DataFrameBuilder]:
    """Return the span-unnest builder registry (lazy imports)."""
    from semantics.span_unnest import (
        ast_span_unnest_df_builder,
        py_bc_instruction_span_unnest_df_builder,
        symtable_span_unnest_df_builder,
        ts_span_unnest_df_builder,
    )

    return {
        "ast_span_unnest": ast_span_unnest_df_builder,
        "ts_span_unnest": ts_span_unnest_df_builder,
        "symtable_span_unnest": symtable_span_unnest_df_builder,
        "py_bc_instruction_span_unnest": py_bc_instruction_span_unnest_df_builder,
    }


def _symtable_registry(
    _context: _SemanticSpecContext,
) -> Mapping[str, DataFrameBuilder]:
    """Return the symtable builder registry (lazy imports)."""
    from datafusion_engine.symtable import views as symtable_views

    return {
        "symtable_bindings": symtable_views.symtable_bindings_df,
        "symtable_def_sites": symtable_views.symtable_def_sites_df,
        "symtable_use_sites": symtable_views.symtable_use_sites_df,
        "symtable_type_params": symtable_views.symtable_type_params_df,
        "symtable_type_param_edges": symtable_views.symtable_type_param_edges_df,
    }


def _builder_for_join_group_spec(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    join_group = context.join_groups_by_name.get(spec.name)
    if join_group is None:
        msg = f"Missing join group spec for output {spec.name!r}."
        raise KeyError(msg)
    return _join_group_builder(join_group, config=context.config)


def _builder_for_relate_spec(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    rel_spec = context.relationship_by_name.get(spec.name)
    if rel_spec is None:
        msg = f"Missing relationship spec for output {spec.name!r}."
        raise KeyError(msg)
    join_group = context.join_group_by_relationship.get(spec.name)
    return _relationship_builder(
        rel_spec,
        config=context.config,
        use_cdf=context.use_cdf,
        join_group=join_group,
    )


def _builder_for_union_edges_spec(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    return _union_edges_builder(list(spec.inputs), config=context.config)


def _builder_for_union_nodes_spec(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    return _union_nodes_builder(list(spec.inputs), config=context.config)


def _builder_for_projection_spec(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    builder = _relation_output_builder(
        relationship_names=tuple(context.relationship_by_name),
        relationship_by_name=context.relationship_by_name,
    )
    return _finalize_output_builder(spec.name, builder)


def _diagnostic_registry(
    _context: _SemanticSpecContext,
) -> Mapping[str, DataFrameBuilder]:
    """Return the diagnostic builder registry."""
    return semantic_diagnostic_view_builders()


def _builder_for_export_spec(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    _ = context
    if spec.name == "dim_exported_defs":
        from semantics.incremental.export_builders import exported_defs_df_builder

        return _finalize_output_builder(spec.name, exported_defs_df_builder)
    msg = f"Unsupported export output {spec.name!r}."
    raise ValueError(msg)


def _builder_for_normalize_kind(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    if spec.kind == ViewKind.NORMALIZE:
        return _builder_for_normalize_spec(spec, context)
    if spec.kind == ViewKind.SCIP_NORMALIZE:
        return _builder_for_scip_normalize_spec(spec, context)
    if spec.kind == ViewKind.BYTECODE_LINE_INDEX:
        return _builder_for_bytecode_line_index_spec(spec, context)
    if spec.kind == ViewKind.SPAN_UNNEST:
        return _dispatch_from_registry(_span_unnest_registry, "span-unnest")(spec, context)
    msg = f"Unsupported normalize-kind spec kind: {spec.kind!r}."
    raise ValueError(msg)


def _builder_for_derive_kind(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    if spec.kind == ViewKind.SYMTABLE:
        return _dispatch_from_registry(_symtable_registry, "symtable")(spec, context)
    msg = f"Unsupported derive-kind spec kind: {spec.kind!r}."
    raise ValueError(msg)


def _builder_for_relate_kind(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    if spec.kind == ViewKind.JOIN_GROUP:
        return _builder_for_join_group_spec(spec, context)
    if spec.kind == ViewKind.RELATE:
        return _builder_for_relate_spec(spec, context)
    msg = f"Unsupported relate-kind spec kind: {spec.kind!r}."
    raise ValueError(msg)


def _builder_for_union_kind(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    if spec.kind == ViewKind.UNION_EDGES:
        return _builder_for_union_edges_spec(spec, context)
    if spec.kind == ViewKind.UNION_NODES:
        return _builder_for_union_nodes_spec(spec, context)
    msg = f"Unsupported union-kind spec kind: {spec.kind!r}."
    raise ValueError(msg)


def _builder_for_project_kind(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    if spec.kind == ViewKind.PROJECTION:
        return _builder_for_projection_spec(spec, context)
    if spec.kind == ViewKind.EXPORT:
        return _builder_for_export_spec(spec, context)
    if spec.kind == ViewKind.FINALIZE:
        msg = (
            "Python FINALIZE builders were removed. "
            "CPG finalize outputs must be emitted via Rust CpgEmit transforms."
        )
        raise ValueError(msg)
    if spec.kind == ViewKind.ARTIFACT:
        msg = f"Unsupported artifact output {spec.name!r}."
        raise ValueError(msg)
    msg = f"Unsupported project-kind spec kind: {spec.kind!r}."
    raise ValueError(msg)


def _builder_for_diagnostic_kind(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    if spec.kind == ViewKind.DIAGNOSTIC:
        return _dispatch_from_registry(_diagnostic_registry, "diagnostic", finalize=True)(
            spec,
            context,
        )
    msg = f"Unsupported diagnostic-kind spec kind: {spec.kind!r}."
    raise ValueError(msg)


_CONSOLIDATED_BUILDER_HANDLERS: dict[
    str, Callable[[SemanticSpecIndex, _SemanticSpecContext], DataFrameBuilder]
] = {
    "normalize": _builder_for_normalize_kind,
    "derive": _builder_for_derive_kind,
    "relate": _builder_for_relate_kind,
    "union": _builder_for_union_kind,
    "project": _builder_for_project_kind,
    "diagnostic": _builder_for_diagnostic_kind,
}


def _builder_for_semantic_spec(
    spec: SemanticSpecIndex,
    *,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    return _pipeline_dispatch.builder_for_semantic_spec(
        spec,
        context=context,
        builder_handlers=_CONSOLIDATED_BUILDER_HANDLERS,
    )


def _input_table(input_mapping: Mapping[str, str], name: str) -> str:
    return input_mapping.get(name, name)
