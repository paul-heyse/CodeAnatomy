"""CPG pipeline using semantic operations.

This module shows how the SemanticCompiler is used to build the full CPG.
The pipeline is just function calls - DataFusion handles execution.

The entire pipeline in ~50 lines:
1. Normalize extraction tables (Rule 1 + 2)
2. Build relationships (Rule 5 + 7 or Rule 6 + 7)
3. Union to final outputs (Rule 8)
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, cast

from datafusion_engine.delta.schema_guard import SchemaEvolutionPolicy
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.io.write import WritePipeline
from datafusion_engine.views.bundle_extraction import arrow_schema_from_df
from obs.diagnostics import (
    SemanticQualityArtifact,
    record_semantic_quality_artifact,
    record_semantic_quality_events,
)
from obs.metrics import record_quality_issue_counts
from obs.otel.run_context import get_run_id, reset_run_id, set_run_id
from obs.otel.scopes import SCOPE_SEMANTICS
from obs.otel.tracing import stage_span
from semantics.diagnostics import (
    DEFAULT_MAX_ISSUE_ROWS,
    SEMANTIC_DIAGNOSTIC_VIEW_NAMES,
    dataframe_row_count,
    semantic_diagnostic_view_builders,
    semantic_quality_issue_batches,
)
from semantics.incremental.metadata import (
    SemanticDiagnosticsSnapshot,
    write_semantic_diagnostics_snapshots,
)
from semantics.incremental.runtime import IncrementalRuntime
from semantics.incremental.state_store import StateStore
from semantics.naming import canonical_output_name
from semantics.quality import QualityRelationshipSpec
from semantics.registry import SEMANTIC_MODEL
from semantics.runtime import CachePolicy, SemanticRuntimeConfig
from semantics.specs import RelationshipSpec
from utils.env_utils import env_value
from utils.hashing import hash_msgpack_canonical
from utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.io.write import WritePipeline
    from datafusion_engine.lineage.diagnostics import DiagnosticsSink
    from datafusion_engine.plan.bundle import DataFrameBuilder, DataFusionPlanBundle
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile, SessionRuntime
    from datafusion_engine.views.graph import ViewNode
    from semantics.adapters import RelationshipProjectionOptions
    from semantics.config import SemanticConfig
    from semantics.ir import SemanticIR, SemanticIRJoinGroup, SemanticIRView
    from semantics.spec_registry import SemanticNormalizationSpec, SemanticSpecIndex


CPG_INPUT_TABLES: tuple[str, ...] = (
    "cst_refs",
    "cst_defs",
    "cst_imports",
    "cst_callsites",
    "cst_call_args",
    "cst_docstrings",
    "cst_decorators",
    "scip_occurrences",
    "file_line_index_v1",
)


@dataclass(frozen=True)
class CpgBuildOptions:
    """Configuration for semantic CPG construction."""

    cache_policy: Mapping[str, CachePolicy] | None = None
    validate_schema: bool = True
    config: SemanticConfig | None = None
    use_cdf: bool | None = None
    cdf_inputs: Mapping[str, str] | None = None
    materialize_outputs: bool = True
    requested_outputs: Collection[str] | None = None
    schema_policy: SchemaEvolutionPolicy = field(default_factory=SchemaEvolutionPolicy)


@dataclass(frozen=True)
class CpgViewNodesRequest:
    """Inputs required to build semantic view nodes."""

    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile
    runtime_config: SemanticRuntimeConfig
    cache_policy: Mapping[str, CachePolicy] | None
    config: SemanticConfig | None
    input_mapping: Mapping[str, str]
    use_cdf: bool
    requested_outputs: Collection[str] | None
    semantic_ir: SemanticIR


@dataclass(frozen=True)
class CpgViewSpecsRequest:
    """Inputs required to build semantic view specs."""

    input_mapping: Mapping[str, str]
    config: SemanticConfig | None
    use_cdf: bool
    runtime_profile: DataFusionRuntimeProfile | None
    requested_outputs: Collection[str] | None
    semantic_ir: SemanticIR


@dataclass(frozen=True)
class _IncrementalOutputRequest:
    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile
    runtime_config: SemanticRuntimeConfig
    input_mapping: Mapping[str, str]
    use_cdf: bool
    requested_outputs: Collection[str] | None


@dataclass(frozen=True)
class SemanticOutputWriteContext:
    """Inputs required to materialize semantic outputs."""

    ctx: SessionContext
    pipeline: WritePipeline
    runtime_profile: DataFusionRuntimeProfile
    runtime_config: SemanticRuntimeConfig
    schema_policy: SchemaEvolutionPolicy


def _cache_policy_for(
    name: str,
    policy: Mapping[str, CachePolicy] | None,
) -> CachePolicy:
    if policy is None:
        return "none"
    return policy.get(name, "none")


def _normalize_cache_policy(policy: str) -> CachePolicy:
    valid = {"none", "delta_staging", "delta_output"}
    if policy in valid:
        return cast("CachePolicy", policy)
    msg = f"Unsupported cache policy: {policy!r}."
    raise ValueError(msg)


def _normalize_cache_policy_mapping(
    policy: Mapping[str, str],
) -> dict[str, CachePolicy]:
    return {name: _normalize_cache_policy(value) for name, value in policy.items()}


def _default_semantic_cache_policy(
    *,
    view_names: Sequence[str],
    runtime_config: SemanticRuntimeConfig,
) -> dict[str, CachePolicy]:
    resolved: dict[str, CachePolicy] = {}
    for name in view_names:
        if name.startswith("cpg_"):
            if runtime_config.output_path(name) is not None:
                resolved[name] = "delta_output"
            else:
                resolved[name] = "delta_staging"
            continue
        if name in {"relation_output_v1", "dim_exported_defs_v1"}:
            resolved[name] = "delta_staging"
            continue
        if name in {"semantic_nodes_union_v1", "semantic_edges_union_v1"}:
            resolved[name] = "delta_staging"
            continue
        if name.startswith("rel_") or name.endswith("_norm_v1"):
            resolved[name] = "delta_staging"
            continue
        if name.startswith("join_"):
            resolved[name] = "delta_staging"
            continue
        resolved[name] = "none"
    return resolved


def _bundle_for_builder(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    builder: DataFrameBuilder,
    view_name: str,
    semantic_ir: SemanticIR | None,
) -> DataFusionPlanBundle:
    """Build a plan bundle for a DataFrame builder.

    Returns
    -------
    DataFusionPlanBundle
        Plan bundle for the builder output.
    """
    from datafusion_engine.plan.bundle import PlanBundleOptions, build_plan_bundle

    session_runtime = runtime_profile.session_runtime()
    df = builder(ctx)
    bundle = build_plan_bundle(
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
    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.compiler import SemanticCompiler

        return SemanticCompiler(inner_ctx, config=config).normalize(table, prefix=prefix)

    return _builder


def _normalize_spec_builder(
    spec: SemanticNormalizationSpec,
    *,
    input_mapping: Mapping[str, str],
    config: SemanticConfig | None,
) -> DataFrameBuilder:
    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from dataclasses import replace

        from semantics.compiler import SemanticCompiler

        resolved_table = input_mapping.get(spec.source_table, spec.source_table)
        resolved_spec = replace(spec.spec, table=resolved_table)
        return SemanticCompiler(inner_ctx, config=config).normalize_from_spec(resolved_spec)

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

    Returns
    -------
    DataFrameBuilder
        A callable that builds the relationship DataFrame.
    """

    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.compiler import RelationOptions, SemanticCompiler

        compiler = SemanticCompiler(inner_ctx, config=config)
        if isinstance(spec, QualityRelationshipSpec):
            file_quality_df = None
            if spec.join_file_quality:
                from semantics.signals import build_file_quality_view

                try:
                    file_quality_df = inner_ctx.table(spec.file_quality_view)
                except Exception:  # noqa: BLE001
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


def _projected_relationship_builder(
    builder: DataFrameBuilder,
    *,
    options: RelationshipProjectionOptions,
) -> DataFrameBuilder:
    def _build(inner_ctx: SessionContext) -> DataFrame:
        from semantics.adapters import project_semantic_to_legacy

        return project_semantic_to_legacy(builder(inner_ctx), options=options)

    return _build


def _relation_output_builder(
    *,
    relationship_names: Sequence[str],
    relationship_by_name: Mapping[str, QualityRelationshipSpec],
    projection_options: Mapping[str, RelationshipProjectionOptions],
) -> DataFrameBuilder:
    def _builder(inner_ctx: SessionContext) -> DataFrame:
        import pyarrow as pa
        from datafusion import lit

        from semantics.catalog.projections import RelationOutputSpec, relation_output_projection

        frames: list[DataFrame] = []
        for rel_name in sorted(relationship_names):
            if rel_name not in projection_options:
                msg = f"Missing projection options for relationship {rel_name!r}."
                raise KeyError(msg)
            rel_df = inner_ctx.table(rel_name)
            schema_names = set(rel_df.schema().names)
            for col_name, dtype in (
                ("binding_kind", pa.string()),
                ("def_site_kind", pa.string()),
                ("use_kind", pa.string()),
                ("reason", pa.string()),
                ("diag_source", pa.string()),
                ("severity", pa.string()),
                ("ambiguity_group_id", pa.string()),
                ("qname_source", pa.string()),
            ):
                if col_name not in schema_names:
                    rel_df = rel_df.with_column(col_name, lit(None).cast(dtype))
                    schema_names.add(col_name)
            options = projection_options[rel_name]
            spec = relationship_by_name.get(rel_name)
            origin = spec.origin if spec is not None else "semantic_compiler"
            output_spec = RelationOutputSpec(
                src_col=options.entity_id_alias,
                dst_col="symbol",
                kind=str(options.edge_kind),
                origin=origin,
                qname_source_col="qname_source",
                ambiguity_group_col="ambiguity_group_id",
            )
            frames.append(relation_output_projection(rel_df, output_spec))
        if not frames:
            msg = "No relationship views available for relation_output_v1."
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
    from semantics.catalog.dataset_specs import dataset_spec

    _ = ctx
    spec = dataset_spec(view_name)
    contract = spec.contract()
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
    def _builder(inner_ctx: SessionContext) -> DataFrame:
        df = builder(inner_ctx)
        return _finalize_df_to_contract(inner_ctx, df=df, view_name=view_name)

    return _builder


def _join_group_builder(
    group: SemanticIRJoinGroup,
    *,
    config: SemanticConfig | None,
) -> DataFrameBuilder:
    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.compiler import SemanticCompiler

        return SemanticCompiler(inner_ctx, config=config).build_join_group(group)

    return _builder


def _union_nodes_builder(
    names: list[str],
    *,
    config: SemanticConfig | None,
) -> DataFrameBuilder:
    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.compiler import SemanticCompiler

        return SemanticCompiler(inner_ctx, config=config).union_nodes(
            names,
            discriminator="node_kind",
        )

    return _builder


def _union_edges_builder(
    names: list[str],
    *,
    config: SemanticConfig | None,
) -> DataFrameBuilder:
    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.compiler import SemanticCompiler

        return SemanticCompiler(inner_ctx, config=config).union_edges(
            names,
            discriminator="edge_kind",
        )

    return _builder


def _scip_norm_builder(
    table: str,
    *,
    line_index_table: str,
) -> DataFrameBuilder:
    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.scip_normalize import scip_to_byte_offsets

        return scip_to_byte_offsets(
            inner_ctx,
            occurrences_table=table,
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


def _resolve_requested_outputs(
    requested_outputs: Collection[str] | None,
) -> set[str] | None:
    if requested_outputs is None:
        return {spec.name for spec in SEMANTIC_MODEL.outputs if spec.kind == "table"}
    return {canonical_output_name(name) for name in requested_outputs}


def _incremental_requested_outputs(
    request: _IncrementalOutputRequest,
) -> set[str] | None:
    if request.requested_outputs is not None or not request.use_cdf:
        return None
    changed_inputs = _cdf_changed_inputs(
        request.ctx,
        runtime_profile=request.runtime_profile,
        runtime_config=request.runtime_config,
        input_mapping=request.input_mapping,
    )
    if changed_inputs is None:
        return None
    if not changed_inputs:
        return set()
    return _outputs_from_changed_inputs(changed_inputs)


def _cdf_changed_inputs(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    runtime_config: SemanticRuntimeConfig,
    input_mapping: Mapping[str, str],
) -> set[str] | None:
    from semantics.incremental.cdf_cursors import CdfCursorStore
    from storage.deltalake import delta_cdf_enabled, delta_table_version

    _ = ctx
    cursor_store = runtime_config.cdf_cursor_store or runtime_profile.cdf_cursor_store
    if cursor_store is None:
        return None
    if not isinstance(cursor_store, CdfCursorStore):
        return None
    changed: set[str] = set()
    for canonical, source in input_mapping.items():
        if canonical == "file_line_index_v1":
            continue
        location = runtime_profile.dataset_location(canonical)
        if location is None:
            location = runtime_profile.dataset_location(source)
        if location is None:
            continue
        storage_options = dict(location.storage_options)
        log_options = dict(location.delta_log_storage_options)
        if not _cdf_enabled_for_location(
            location,
            storage_options=storage_options,
            log_storage_options=log_options,
            cdf_enabled_fn=delta_cdf_enabled,
        ):
            continue
        latest_version = delta_table_version(
            str(location.path),
            storage_options=storage_options,
            log_storage_options=log_options,
        )
        if latest_version is None:
            continue
        cursor = cursor_store.load_cursor(canonical)
        if cursor is None or latest_version > cursor.last_version:
            changed.add(canonical)
    return changed


def _cdf_enabled_for_location(
    location: DatasetLocation,
    *,
    storage_options: Mapping[str, str],
    log_storage_options: Mapping[str, str],
    cdf_enabled_fn: Callable[..., bool],
) -> bool:
    if location.delta_cdf_options is not None:
        return True
    if location.datafusion_provider == "delta_cdf":
        return True
    return cdf_enabled_fn(
        str(location.path),
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )


def _outputs_from_changed_inputs(changed_inputs: Collection[str]) -> set[str]:
    from semantics.ir_pipeline import compile_semantics

    compiled = compile_semantics(SEMANTIC_MODEL)
    impacted_views = _views_downstream_of_inputs(compiled.views, changed_inputs)
    output_names = {spec.name for spec in SEMANTIC_MODEL.outputs}
    return {name for name in impacted_views if name in output_names}


def _views_downstream_of_inputs(
    views: Sequence[SemanticIRView],
    seeds: Collection[str],
) -> set[str]:
    dependents: dict[str, list[str]] = {}
    for view in views:
        for dep in view.inputs:
            dependents.setdefault(dep, []).append(view.name)
    impacted: set[str] = set()
    stack = list(seeds)
    while stack:
        current = stack.pop()
        for view_name in dependents.get(current, ()):
            if view_name in impacted:
                continue
            impacted.add(view_name)
            stack.append(view_name)
    return impacted


def _cpg_view_specs(
    request: CpgViewSpecsRequest,
) -> list[tuple[str, DataFrameBuilder]]:
    """Build view specs for the CPG pipeline.

    Uses the declarative RELATIONSHIP_SPECS registry to generate relationship
    builders. New relationships can be added to the registry without modifying
    this function.

    All output names use the canonical naming policy from semantics.naming
    to ensure consistent versioned output names across all consumers.

    Parameters
    ----------
    request
        Inputs required to build semantic view specs.

    Returns
    -------
    list[tuple[str, DataFrameBuilder]]
        List of (view_name, builder) tuples for the CPG pipeline.

    Raises
    ------
    ValueError
        Raised when the runtime profile is unavailable.

    """
    from relspec.view_defs import (
        DEFAULT_REL_TASK_PRIORITY,
        REL_CALLSITE_SYMBOL_OUTPUT,
        REL_DEF_SYMBOL_OUTPUT,
        REL_IMPORT_SYMBOL_OUTPUT,
        REL_NAME_SYMBOL_OUTPUT,
    )
    from semantics.catalog.projections import (
        SemanticProjectionConfig,
        semantic_projection_options,
    )
    from semantics.spec_registry import (
        RELATIONSHIP_SPECS,
        SEMANTIC_NORMALIZATION_SPECS,
        SemanticSpecIndex,
        normalization_spec_for_output,
    )

    normalization_by_output = {spec.output_name: spec for spec in SEMANTIC_NORMALIZATION_SPECS}
    relationship_by_name = {spec.name: spec for spec in RELATIONSHIP_SPECS}
    semantic_ir = request.semantic_ir
    ir = semantic_ir
    join_groups_by_name = {group.name: group for group in ir.join_groups}
    join_group_by_relationship = {
        rel_name: group for group in ir.join_groups for rel_name in group.relationship_names
    }
    projection_options = semantic_projection_options(
        SemanticProjectionConfig(
            default_priority=DEFAULT_REL_TASK_PRIORITY,
            rel_name_output=REL_NAME_SYMBOL_OUTPUT,
            rel_import_output=REL_IMPORT_SYMBOL_OUTPUT,
            rel_def_output=REL_DEF_SYMBOL_OUTPUT,
            rel_call_output=REL_CALLSITE_SYMBOL_OUTPUT,
            relationship_specs=RELATIONSHIP_SPECS,
        )
    )

    if request.runtime_profile is None:
        msg = "Runtime profile is required for CPG output views."
        raise ValueError(msg)

    context = _SemanticSpecContext(
        normalization_by_output=normalization_by_output,
        relationship_by_name=relationship_by_name,
        input_mapping=request.input_mapping,
        config=request.config,
        use_cdf=request.use_cdf,
        normalization_spec_for_output=normalization_spec_for_output,
        join_groups_by_name=join_groups_by_name,
        join_group_by_relationship=join_group_by_relationship,
        projection_options=projection_options,
        runtime_profile=request.runtime_profile,
    )
    spec_index = tuple(
        SemanticSpecIndex(
            name=view.name,
            kind=view.kind,
            inputs=view.inputs,
            outputs=view.outputs,
        )
        for view in ir.views
    )
    return _semantic_view_specs(
        ordered_specs=_ordered_semantic_specs(spec_index),
        context=context,
    )


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
    projection_options: Mapping[str, RelationshipProjectionOptions]
    runtime_profile: DataFusionRuntimeProfile


def _semantic_view_specs(
    *,
    ordered_specs: Sequence[SemanticSpecIndex],
    context: _SemanticSpecContext,
) -> list[tuple[str, DataFrameBuilder]]:
    view_specs: list[tuple[str, DataFrameBuilder]] = []
    for spec in ordered_specs:
        builder = _builder_for_semantic_spec(
            spec,
            context=context,
        )
        view_specs.append((spec.name, builder))
    return view_specs


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


def _builder_for_symtable_spec(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    _ = context
    from datafusion_engine.symtable import views as symtable_views

    builders: dict[str, DataFrameBuilder] = {
        "symtable_bindings": symtable_views.symtable_bindings_df,
        "symtable_def_sites": symtable_views.symtable_def_sites_df,
        "symtable_use_sites": symtable_views.symtable_use_sites_df,
        "symtable_type_params": symtable_views.symtable_type_params_df,
        "symtable_type_param_edges": symtable_views.symtable_type_param_edges_df,
    }
    builder = builders.get(spec.name)
    if builder is None:
        msg = f"Missing symtable builder for output {spec.name!r}."
        raise KeyError(msg)
    return builder


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
    builder = _relationship_builder(
        rel_spec,
        config=context.config,
        use_cdf=context.use_cdf,
        join_group=join_group,
    )
    options = context.projection_options.get(spec.name)
    if options is None:
        msg = f"Missing projection options for relationship {spec.name!r}."
        raise KeyError(msg)
    return _projected_relationship_builder(builder, options=options)


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
        projection_options=context.projection_options,
    )
    return _finalize_output_builder(spec.name, builder)


def _builder_for_diagnostic_spec(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    _ = context
    builders = semantic_diagnostic_view_builders()
    builder = builders.get(spec.name)
    if builder is None:
        msg = f"Missing diagnostic builder for output {spec.name!r}."
        raise KeyError(msg)
    return _finalize_output_builder(spec.name, builder)


def _builder_for_export_spec(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    _ = context
    if spec.name == "dim_exported_defs_v1":
        from semantics.incremental.export_builders import exported_defs_df_builder

        return _finalize_output_builder(spec.name, exported_defs_df_builder)
    msg = f"Unsupported export output {spec.name!r}."
    raise ValueError(msg)


def _builder_for_finalize_spec(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    cpg_builders = dict(_cpg_output_view_specs(context.runtime_profile))
    builder = cpg_builders.get(spec.name)
    if builder is None:
        msg = f"Missing finalize builder for output {spec.name!r}."
        raise KeyError(msg)
    return _finalize_output_builder(spec.name, builder)


def _builder_for_artifact_spec(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    _ = context
    msg = f"Unsupported artifact spec output: {spec.name!r}."
    raise ValueError(msg)


def _builder_for_semantic_spec(
    spec: SemanticSpecIndex,
    *,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    handlers: dict[str, Callable[[SemanticSpecIndex, _SemanticSpecContext], DataFrameBuilder]] = {
        "normalize": _builder_for_normalize_spec,
        "scip_normalize": _builder_for_scip_normalize_spec,
        "symtable": _builder_for_symtable_spec,
        "join_group": _builder_for_join_group_spec,
        "relate": _builder_for_relate_spec,
        "union_edges": _builder_for_union_edges_spec,
        "union_nodes": _builder_for_union_nodes_spec,
        "projection": _builder_for_projection_spec,
        "diagnostic": _builder_for_diagnostic_spec,
        "export": _builder_for_export_spec,
        "finalize": _builder_for_finalize_spec,
        "artifact": _builder_for_artifact_spec,
    }
    handler = handlers.get(spec.kind)
    if handler is None:
        msg = f"Unsupported semantic spec kind: {spec.kind!r}."
        raise ValueError(msg)
    return handler(spec, context)


def _input_table(input_mapping: Mapping[str, str], name: str) -> str:
    return input_mapping.get(name, name)


def _cpg_output_view_specs(
    runtime_profile: DataFusionRuntimeProfile,
) -> list[tuple[str, DataFrameBuilder]]:
    from cpg.view_builders_df import (
        build_cpg_edges_by_dst_df,
        build_cpg_edges_by_src_df,
        build_cpg_edges_df,
        build_cpg_nodes_df,
        build_cpg_props_df,
        build_cpg_props_map_df,
    )
    from semantics.naming import canonical_output_name

    session_runtime = runtime_profile.session_runtime()

    def _wrap_cpg_builder(builder: Callable[[SessionRuntime], DataFrame]) -> DataFrameBuilder:
        def _builder(inner_ctx: SessionContext) -> DataFrame:
            _ = inner_ctx
            return builder(session_runtime)

        return _builder

    return [
        (
            canonical_output_name("cpg_nodes"),
            _wrap_cpg_builder(build_cpg_nodes_df),
        ),
        (
            canonical_output_name("cpg_edges"),
            _wrap_cpg_builder(build_cpg_edges_df),
        ),
        (
            canonical_output_name("cpg_props"),
            _wrap_cpg_builder(build_cpg_props_df),
        ),
        (
            canonical_output_name("cpg_props_map"),
            _wrap_cpg_builder(build_cpg_props_map_df),
        ),
        (
            canonical_output_name("cpg_edges_by_src"),
            _wrap_cpg_builder(build_cpg_edges_by_src_df),
        ),
        (
            canonical_output_name("cpg_edges_by_dst"),
            _wrap_cpg_builder(build_cpg_edges_by_dst_df),
        ),
    ]


def _view_nodes_for_cpg(request: CpgViewNodesRequest) -> list[ViewNode]:
    from datafusion_engine.views.graph import ViewNode

    view_specs = _cpg_view_specs(
        CpgViewSpecsRequest(
            input_mapping=request.input_mapping,
            config=request.config,
            use_cdf=request.use_cdf,
            runtime_profile=request.runtime_profile,
            requested_outputs=request.requested_outputs,
            semantic_ir=request.semantic_ir,
        )
    )
    resolved_cache = request.cache_policy
    if resolved_cache is None:
        resolved_cache = _default_semantic_cache_policy(
            view_names=[name for name, _builder in view_specs],
            runtime_config=request.runtime_config,
        )
    if request.runtime_config.cache_policy_overrides:
        resolved_cache = {**resolved_cache, **request.runtime_config.cache_policy_overrides}
    normalized_cache = _normalize_cache_policy_mapping(resolved_cache)
    nodes: list[ViewNode] = []

    for name, builder in view_specs:
        nodes.append(
            ViewNode(
                name=name,
                deps=(),
                builder=builder,
                plan_bundle=_bundle_for_builder(
                    request.ctx,
                    runtime_profile=request.runtime_profile,
                    builder=builder,
                    view_name=name,
                    semantic_ir=request.semantic_ir,
                ),
                cache_policy=_cache_policy_for(name, normalized_cache),
            )
        )
    return nodes


def _record_semantic_compile_artifacts(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    semantic_ir: SemanticIR,
    requested_outputs: Collection[str] | None,
    plan_bundles: Mapping[str, DataFusionPlanBundle] | None = None,
) -> None:
    from datafusion_engine.identity import schema_identity_hash
    from datafusion_engine.lineage.diagnostics import record_artifact
    from datafusion_engine.views.bundle_extraction import arrow_schema_from_df

    if semantic_ir.model_hash is None or semantic_ir.ir_hash is None:
        return

    payload = {
        "semantic_model_hash": semantic_ir.model_hash,
        "semantic_ir_hash": semantic_ir.ir_hash,
        "view_count": len(semantic_ir.views),
        "join_group_count": len(semantic_ir.join_groups),
        "requested_outputs": tuple(sorted(requested_outputs or ())),
    }
    record_artifact(runtime_profile, "semantic_ir_fingerprint_v1", payload)

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
    record_artifact(runtime_profile, "semantic_explain_plan_v1", explain_payload)

    view_stats = _view_plan_stats(semantic_ir, plan_bundles=plan_bundles)
    if view_stats:
        record_artifact(
            runtime_profile,
            "semantic_view_plan_stats_v1",
            {
                "semantic_model_hash": semantic_ir.model_hash,
                "semantic_ir_hash": semantic_ir.ir_hash,
                "views": view_stats,
            },
        )
    report = _semantic_explain_markdown(
        semantic_ir,
        view_stats=view_stats,
    )
    record_artifact(
        runtime_profile,
        "semantic_explain_plan_report_v1",
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
            "semantic_join_group_stats_v1",
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


def _view_plan_stats(
    semantic_ir: SemanticIR,
    *,
    plan_bundles: Mapping[str, DataFusionPlanBundle] | None,
) -> list[dict[str, object]]:
    if not plan_bundles:
        return []
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


def build_cpg(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    runtime_config: SemanticRuntimeConfig,
    options: CpgBuildOptions | None = None,
) -> None:
    """Build the complete CPG from extraction tables.

    Expects these tables registered in ctx:
    - cst_refs, cst_defs, cst_imports, cst_callsites (CST extraction)
    - scip_occurrences (SCIP index)
    - file_line_index_v1 (line index for SCIP byte offsets)

    Registers these views (using canonical _v1 output names):
    - scip_occurrences_norm_v1
    - cst_refs_norm_v1, cst_defs_norm_v1, cst_imports_norm_v1, cst_calls_norm_v1
    - rel_name_symbol_v1, rel_def_symbol_v1, rel_import_symbol_v1, rel_callsite_symbol_v1
    - semantic_nodes_union_v1, semantic_edges_union_v1
    - cpg_nodes_v1, cpg_edges_v1

    Parameters
    ----------
    ctx
        DataFusion session context with extraction tables registered.
    runtime_profile
        Runtime configuration for building cached view graphs (required).
    runtime_config
        Semantic runtime configuration for cache policy and output locations.
    options
        Optional build settings for cache policy, schema validation, and CDF inputs.

    Raises
    ------
    ValueError
        Raised when runtime_profile is missing.
    """
    resolved = options or CpgBuildOptions()
    if runtime_profile is None:
        msg = "build_cpg requires a runtime_profile for view-graph execution."
        raise ValueError(msg)
    effective_use_cdf = (
        resolved.use_cdf if resolved.use_cdf is not None else runtime_config.cdf_enabled
    )

    with stage_span(
        "semantics.build_cpg",
        stage="semantics",
        scope_name=SCOPE_SEMANTICS,
        attributes={
            "codeanatomy.validate_schema": resolved.validate_schema,
            "codeanatomy.use_cdf": resolved.use_cdf,
            "codeanatomy.has_cache_policy": resolved.cache_policy is not None,
            "codeanatomy.has_config": resolved.config is not None,
        },
    ):
        input_mapping, use_cdf = _resolve_semantic_input_mapping(
            ctx,
            runtime_profile=runtime_profile,
            use_cdf=effective_use_cdf,
            cdf_inputs=resolved.cdf_inputs,
        )
        from semantics.validation import require_semantic_inputs

        require_semantic_inputs(ctx, input_mapping=input_mapping)

        from datafusion_engine.udf.runtime import rust_udf_snapshot
        from datafusion_engine.views.graph import (
            ViewGraphOptions,
            ViewGraphRuntimeOptions,
            register_view_graph,
        )
        from semantics.ir_pipeline import build_semantic_ir

        snapshot = rust_udf_snapshot(ctx)
        resolved_outputs = _resolve_requested_outputs(resolved.requested_outputs)
        incremental_outputs = _incremental_requested_outputs(
            _IncrementalOutputRequest(
                ctx=ctx,
                runtime_profile=runtime_profile,
                runtime_config=runtime_config,
                input_mapping=input_mapping,
                use_cdf=use_cdf,
                requested_outputs=resolved.requested_outputs,
            )
        )
        if incremental_outputs is not None:
            resolved_outputs = incremental_outputs
        semantic_ir = build_semantic_ir(
            outputs=resolved_outputs,
        )
        nodes = _view_nodes_for_cpg(
            CpgViewNodesRequest(
                ctx=ctx,
                runtime_profile=runtime_profile,
                runtime_config=runtime_config,
                cache_policy=resolved.cache_policy,
                config=resolved.config,
                input_mapping=input_mapping,
                use_cdf=use_cdf,
                requested_outputs=resolved_outputs,
                semantic_ir=semantic_ir,
            )
        )
        register_view_graph(
            ctx,
            nodes=nodes,
            snapshot=snapshot,
            runtime_options=ViewGraphRuntimeOptions(runtime_profile=runtime_profile),
            options=ViewGraphOptions(
                overwrite=True,
                temporary=False,
                validate_schema=resolved.validate_schema,
            ),
        )
        plan_bundles: dict[str, DataFusionPlanBundle] = {
            node.name: node.plan_bundle for node in nodes if node.plan_bundle is not None
        }
        _record_semantic_compile_artifacts(
            ctx,
            runtime_profile=runtime_profile,
            semantic_ir=semantic_ir,
            requested_outputs=resolved_outputs,
            plan_bundles=plan_bundles,
        )
        if resolved.materialize_outputs:
            _materialize_semantic_outputs(
                ctx,
                runtime_profile=runtime_profile,
                runtime_config=runtime_config,
                schema_policy=resolved.schema_policy,
                requested_outputs=resolved_outputs,
            )
        _emit_semantic_quality_diagnostics(
            ctx,
            runtime_profile=runtime_profile,
            runtime_config=runtime_config,
            schema_policy=resolved.schema_policy,
            requested_outputs=resolved_outputs,
        )


def _semantic_output_view_names(
    *,
    requested_outputs: Collection[str] | None = None,
) -> list[str]:
    """Return semantic output view names.

    Returns
    -------
    list[str]
        Semantic output view names, including the relation output.
    """
    from relspec.view_defs import RELATION_OUTPUT_NAME

    if requested_outputs is None:
        view_names = [spec.name for spec in SEMANTIC_MODEL.outputs]
        if RELATION_OUTPUT_NAME not in view_names:
            view_names.append(RELATION_OUTPUT_NAME)
        return view_names

    resolved = {canonical_output_name(name) for name in requested_outputs}
    return [spec.name for spec in SEMANTIC_MODEL.outputs if spec.name in resolved]


def _ensure_canonical_output_locations(runtime_config: SemanticRuntimeConfig) -> None:
    """Validate that semantic output locations use canonical names.

    Parameters
    ----------
    runtime_config
        Semantic runtime configuration.

    Raises
    ------
    ValueError
        Raised when non-canonical names are supplied.
    """
    from semantics.naming import canonical_output_name

    non_canonical = {
        name: canonical_output_name(name)
        for name in runtime_config.output_locations
        if canonical_output_name(name) != name
    }
    if non_canonical:
        msg = f"Semantic outputs must use canonical names: {non_canonical!r}."
        raise ValueError(msg)


def _semantic_output_locations(
    view_names: Sequence[str],
    runtime_config: SemanticRuntimeConfig,
) -> dict[str, DatasetLocation]:
    """Resolve dataset locations for semantic outputs.

    Parameters
    ----------
    view_names
        Semantic view names to materialize.
    runtime_config
        Semantic runtime configuration providing output locations.

    Returns
    -------
    dict[str, DatasetLocation]
        Mapping of view name to dataset location.
    """
    from datafusion_engine.dataset.registry import DatasetLocation
    from semantics.catalog.dataset_specs import dataset_spec

    storage_options = (
        dict(runtime_config.storage_options) if runtime_config.storage_options is not None else {}
    )
    output_locations: dict[str, DatasetLocation] = {}
    for name in view_names:
        output_path = runtime_config.output_path(name)
        if output_path is None:
            continue
        spec = dataset_spec(name)
        output_locations[name] = DatasetLocation(
            path=output_path,
            format="delta",
            storage_options=storage_options,
            dataset_spec=spec,
            delta_write_policy=spec.delta_write_policy,
            delta_schema_policy=spec.delta_schema_policy,
            delta_maintenance_policy=spec.delta_maintenance_policy,
            delta_feature_gate=spec.delta_feature_gate,
        )
    return output_locations


def _ensure_semantic_output_locations(
    view_names: Sequence[str],
    output_locations: Mapping[str, DatasetLocation],
) -> None:
    """Ensure all semantic outputs have explicit dataset locations.

    Parameters
    ----------
    view_names
        Expected semantic view names.
    output_locations
        Resolved output locations.

    Raises
    ------
    ValueError
        Raised when any output location is missing.
    """
    missing_outputs = [name for name in view_names if name not in output_locations]
    if missing_outputs:
        missing = ", ".join(sorted(missing_outputs))
        msg = (
            "Semantic outputs require explicit dataset locations. "
            f"Missing locations for: {missing}."
        )
        raise ValueError(msg)


def _write_semantic_output(
    *,
    view_name: str,
    output_location: DatasetLocation,
    write_context: SemanticOutputWriteContext,
) -> None:
    """Materialize a single semantic output view.

    Parameters
    ----------
    view_name
        Semantic view name to materialize.
    output_location
        Destination dataset location.
    write_context
        Shared context for output materialization.

    Raises
    ------
    ValueError
        Raised when the resolved output format is not Delta.
    """
    from datafusion_engine.delta.schema_guard import enforce_schema_policy
    from datafusion_engine.delta.store_policy import apply_delta_store_policy
    from datafusion_engine.io.write import WriteFormat, WriteMode, WriteViewRequest
    from datafusion_engine.views.bundle_extraction import arrow_schema_from_df
    from semantics.catalog.dataset_specs import dataset_spec

    ctx = write_context.ctx
    pipeline = write_context.pipeline
    runtime_profile = write_context.runtime_profile
    runtime_config = write_context.runtime_config
    schema_policy = write_context.schema_policy
    spec = dataset_spec(view_name)
    location = apply_delta_store_policy(
        output_location,
        policy=runtime_profile.delta_store_policy,
    )
    if location.format != "delta":
        msg = f"Semantic output {view_name!r} must be stored as Delta."
        raise ValueError(msg)
    df = ctx.table(view_name)
    schema = arrow_schema_from_df(df)
    resolved_schema_policy = schema_policy
    if spec.delta_schema_policy is not None and spec.delta_schema_policy.schema_mode == "merge":
        resolved_schema_policy = SchemaEvolutionPolicy(mode="additive")
    if not runtime_config.schema_evolution_enabled:
        resolved_schema_policy = SchemaEvolutionPolicy(mode="strict")
    schema_hash = enforce_schema_policy(
        expected_schema=schema,
        dataset_location=location,
        policy=resolved_schema_policy,
    )
    commit_metadata: dict[str, object] = {
        "semantic_schema_hash": schema_hash,
        "semantic_view": view_name,
    }
    from semantics.diagnostics import SEMANTIC_DIAGNOSTIC_VIEW_NAMES

    if view_name in SEMANTIC_DIAGNOSTIC_VIEW_NAMES:
        commit_metadata["snapshot_kind"] = view_name
    format_options: dict[str, object] = {
        "delta_commit_metadata": commit_metadata,
        "delta_write_policy": spec.delta_write_policy,
        "delta_schema_policy": spec.delta_schema_policy,
        "delta_maintenance_policy": spec.delta_maintenance_policy,
    }
    partition_by = (
        tuple(spec.delta_write_policy.partition_by) if spec.delta_write_policy is not None else ()
    )
    pipeline.write_view(
        WriteViewRequest(
            view_name=view_name,
            destination=str(location.path),
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
            partition_by=partition_by,
            format_options=format_options,
        )
    )


def _materialize_semantic_outputs(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    runtime_config: SemanticRuntimeConfig,
    schema_policy: SchemaEvolutionPolicy,
    requested_outputs: Collection[str] | None,
) -> None:
    from datafusion_engine.io.write import WritePipeline

    view_names = _semantic_output_view_names(requested_outputs=requested_outputs)
    _ensure_canonical_output_locations(runtime_config)
    output_locations = _semantic_output_locations(view_names, runtime_config)
    _ensure_semantic_output_locations(view_names, output_locations)

    pipeline = WritePipeline(ctx, runtime_profile=runtime_profile)
    write_context = SemanticOutputWriteContext(
        ctx=ctx,
        pipeline=pipeline,
        runtime_profile=runtime_profile,
        runtime_config=runtime_config,
        schema_policy=schema_policy,
    )
    for view_name in view_names:
        _write_semantic_output(
            view_name=view_name,
            output_location=output_locations[view_name],
            write_context=write_context,
        )


@dataclass
class _SemanticDiagnosticsContext:
    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile
    runtime_config: SemanticRuntimeConfig
    schema_policy: SchemaEvolutionPolicy
    diagnostics_sink: DiagnosticsSink | None
    output_locations: dict[str, DatasetLocation]
    write_context: SemanticOutputWriteContext | None
    state_store: StateStore | None
    storage_options: Mapping[str, str] | None
    incremental_runtime: IncrementalRuntime | None = None

    def ensure_incremental_runtime(self) -> IncrementalRuntime | None:
        if self.incremental_runtime is not None:
            return self.incremental_runtime
        try:
            self.incremental_runtime = IncrementalRuntime.build(profile=self.runtime_profile)
        except ValueError:
            self.incremental_runtime = None
        return self.incremental_runtime

    def write_snapshot(self, view_name: str, df: DataFrame) -> str | None:
        if view_name in self.output_locations and self.write_context is not None:
            _write_semantic_output(
                view_name=view_name,
                output_location=self.output_locations[view_name],
                write_context=self.write_context,
            )
            return str(self.output_locations[view_name].path)
        if self.state_store is None:
            return None
        runtime = self.ensure_incremental_runtime()
        if runtime is None:
            return None
        snapshot = SemanticDiagnosticsSnapshot(
            name=view_name,
            table=df.to_arrow_table(),
            destination=self.state_store.semantic_diagnostics_path(view_name),
        )
        updated = write_semantic_diagnostics_snapshots(
            runtime=runtime,
            snapshots={view_name: snapshot},
            storage_options=self.storage_options,
        )
        return updated.get(view_name)


@contextmanager
def _run_context_guard() -> Iterator[None]:
    token = None
    if get_run_id() is None:
        token = set_run_id(uuid7_str())
    try:
        yield
    finally:
        if token is not None:
            reset_run_id(token)


def _resolve_semantic_diagnostics_state_store() -> StateStore | None:
    state_dir_value = env_value("CODEANATOMY_STATE_DIR")
    if not state_dir_value:
        return None
    store = StateStore(root=Path(state_dir_value).expanduser())
    store.ensure_dirs()
    return store


def _build_semantic_diagnostics_context(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    runtime_config: SemanticRuntimeConfig,
    schema_policy: SchemaEvolutionPolicy,
) -> _SemanticDiagnosticsContext | None:
    diagnostics_sink = runtime_profile.diagnostics_sink
    output_locations = _semantic_output_locations(
        view_names=SEMANTIC_DIAGNOSTIC_VIEW_NAMES,
        runtime_config=runtime_config,
    )
    state_store = _resolve_semantic_diagnostics_state_store()
    if diagnostics_sink is None and not output_locations and state_store is None:
        return None
    write_context = None
    if output_locations:
        pipeline = WritePipeline(ctx, runtime_profile=runtime_profile)
        write_context = SemanticOutputWriteContext(
            ctx=ctx,
            pipeline=pipeline,
            runtime_profile=runtime_profile,
            runtime_config=runtime_config,
            schema_policy=schema_policy,
        )
    storage_options = (
        dict(runtime_config.storage_options) if runtime_config.storage_options is not None else None
    )
    return _SemanticDiagnosticsContext(
        ctx=ctx,
        runtime_profile=runtime_profile,
        runtime_config=runtime_config,
        schema_policy=schema_policy,
        diagnostics_sink=diagnostics_sink,
        output_locations=output_locations,
        write_context=write_context,
        state_store=state_store,
        storage_options=storage_options,
    )


def _emit_semantic_quality_view(
    context: _SemanticDiagnosticsContext,
    *,
    view_name: str,
    builder: Callable[[SessionContext], DataFrame],
) -> None:
    try:
        df = builder(context.ctx)
    except ValueError:
        return
    schema_hash = schema_identity_hash(arrow_schema_from_df(df))
    row_count = dataframe_row_count(df)
    artifact_uri = context.write_snapshot(view_name, df)
    if context.diagnostics_sink is None:
        return
    record_semantic_quality_artifact(
        context.diagnostics_sink,
        artifact=SemanticQualityArtifact(
            name=view_name,
            row_count=row_count,
            schema_hash=schema_hash,
            artifact_uri=artifact_uri,
            run_id=get_run_id(),
        ),
    )
    for batch in semantic_quality_issue_batches(
        view_name=view_name,
        df=df,
        max_rows=DEFAULT_MAX_ISSUE_ROWS,
    ):
        record_quality_issue_counts(
            issue_kind=batch.issue_kind,
            count=len(batch.rows),
        )
        record_semantic_quality_events(
            context.diagnostics_sink,
            name="semantic_quality_issues_v1",
            rows=batch.rows,
        )


def _emit_semantic_quality_views(
    context: _SemanticDiagnosticsContext,
    *,
    requested_outputs: Collection[str] | None,
) -> None:
    builders = semantic_diagnostic_view_builders()
    view_names = SEMANTIC_DIAGNOSTIC_VIEW_NAMES
    if requested_outputs is not None:
        resolved = {canonical_output_name(name) for name in requested_outputs}
        view_names = tuple(name for name in view_names if name in resolved)
    for view_name in view_names:
        builder = builders.get(view_name)
        if builder is None:
            continue
        finalized = _finalize_output_builder(view_name, builder)
        _emit_semantic_quality_view(context, view_name=view_name, builder=finalized)


def _emit_semantic_quality_diagnostics(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    runtime_config: SemanticRuntimeConfig,
    schema_policy: SchemaEvolutionPolicy,
    requested_outputs: Collection[str] | None,
) -> None:
    if not runtime_profile.emit_semantic_quality_diagnostics:
        return
    context = _build_semantic_diagnostics_context(
        ctx,
        runtime_profile=runtime_profile,
        runtime_config=runtime_config,
        schema_policy=schema_policy,
    )
    if context is None:
        return
    with _run_context_guard():
        _emit_semantic_quality_views(context, requested_outputs=requested_outputs)


def build_cpg_from_inferred_deps(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    runtime_config: SemanticRuntimeConfig,
    options: CpgBuildOptions | None = None,
) -> dict[str, object]:
    """Build CPG and extract dependency information for rustworkx.

    Returns InferredDeps-compatible data for scheduling.

    Parameters
    ----------
    ctx
        DataFusion session context.
    runtime_profile
        Runtime configuration for building cached view graphs.
    runtime_config
        Semantic runtime configuration for cache policy and output locations.
    options
        Optional build settings for cache policy, schema validation, and CDF inputs.

    Returns
    -------
    dict[str, object]
        Mapping of view names to dependency information.
    """
    from datafusion_engine.views.bundle_extraction import extract_lineage_from_bundle
    from semantics.ir_pipeline import build_semantic_ir

    resolved = options or CpgBuildOptions()
    effective_use_cdf = (
        resolved.use_cdf if resolved.use_cdf is not None else runtime_config.cdf_enabled
    )

    with stage_span(
        "semantics.build_cpg_from_inferred_deps",
        stage="semantics",
        scope_name=SCOPE_SEMANTICS,
        attributes={
            "codeanatomy.validate_schema": resolved.validate_schema,
            "codeanatomy.use_cdf": effective_use_cdf,
            "codeanatomy.has_cache_policy": resolved.cache_policy is not None,
        },
    ):
        build_cpg(
            ctx,
            runtime_profile=runtime_profile,
            runtime_config=runtime_config,
            options=resolved,
        )

        deps: dict[str, object] = {}
        input_mapping, use_cdf = _resolve_semantic_input_mapping(
            ctx,
            runtime_profile=runtime_profile,
            use_cdf=effective_use_cdf,
            cdf_inputs=resolved.cdf_inputs,
        )
        resolved_outputs = _resolve_requested_outputs(resolved.requested_outputs)
        incremental_outputs = _incremental_requested_outputs(
            _IncrementalOutputRequest(
                ctx=ctx,
                runtime_profile=runtime_profile,
                runtime_config=runtime_config,
                input_mapping=input_mapping,
                use_cdf=use_cdf,
                requested_outputs=resolved.requested_outputs,
            )
        )
        if incremental_outputs is not None:
            resolved_outputs = incremental_outputs
        semantic_ir = build_semantic_ir(outputs=resolved_outputs)
        nodes = _view_nodes_for_cpg(
            CpgViewNodesRequest(
                ctx=ctx,
                runtime_profile=runtime_profile,
                runtime_config=runtime_config,
                cache_policy=resolved.cache_policy,
                config=resolved.config,
                input_mapping=input_mapping,
                use_cdf=use_cdf,
                requested_outputs=resolved_outputs,
                semantic_ir=semantic_ir,
            )
        )
        for node in nodes:
            bundle = node.plan_bundle
            if bundle is None:
                continue
            lineage = extract_lineage_from_bundle(bundle)
            deps[node.name] = {
                "task_name": node.name,
                "output": node.name,
                "inputs": lineage.referenced_tables,
                "required_columns": lineage.required_columns_by_dataset,
                "plan_fingerprint": bundle.plan_fingerprint,
                "plan_identity_hash": bundle.plan_identity_hash,
            }

        return deps


def _resolve_semantic_input_mapping(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    use_cdf: bool | None,
    cdf_inputs: Mapping[str, str] | None,
) -> tuple[dict[str, str], bool]:
    from semantics.input_registry import require_semantic_inputs

    resolved_inputs = require_semantic_inputs(ctx)
    if use_cdf is False:
        return dict(resolved_inputs), False

    cdf_candidates = tuple(
        source for canonical, source in resolved_inputs.items() if canonical != "file_line_index_v1"
    )
    resolved_cdf = _resolve_cdf_inputs(
        ctx,
        runtime_profile=runtime_profile,
        use_cdf=use_cdf,
        cdf_inputs=cdf_inputs,
        inputs=cdf_candidates,
    )

    if not resolved_cdf:
        return dict(resolved_inputs), False

    mapped: dict[str, str] = {}
    for canonical, source in resolved_inputs.items():
        override = resolved_cdf.get(source) or resolved_cdf.get(canonical)
        mapped[canonical] = override if override is not None else source
    return mapped, True


def _resolve_cdf_inputs(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    use_cdf: bool | None,
    cdf_inputs: Mapping[str, str] | None,
    inputs: Sequence[str],
) -> Mapping[str, str] | None:
    if use_cdf is False:
        return None
    if runtime_profile is None:
        if use_cdf:
            msg = "CDF input registration requires a runtime profile."
            raise ValueError(msg)
        return dict(cdf_inputs) if cdf_inputs else None
    if use_cdf is None and not _has_cdf_inputs(runtime_profile, inputs=inputs):
        return dict(cdf_inputs) if cdf_inputs else None
    from datafusion_engine.session.runtime import register_cdf_inputs_for_profile

    mapping = register_cdf_inputs_for_profile(runtime_profile, ctx, table_names=inputs)
    if cdf_inputs:
        mapping = {**mapping, **cdf_inputs}
    return mapping or None


def _has_cdf_inputs(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    inputs: Sequence[str],
) -> bool:
    from datafusion_engine.dataset.registry import resolve_datafusion_provider

    for name in inputs:
        location = runtime_profile.dataset_location(name)
        if location is None:
            continue
        if location.delta_cdf_options is not None:
            return True
        if resolve_datafusion_provider(location) == "delta_cdf":
            return True
    return False


__all__ = [
    "CPG_INPUT_TABLES",
    "CachePolicy",
    "CpgBuildOptions",
    "RelationshipSpec",
    "build_cpg",
    "build_cpg_from_inferred_deps",
]
