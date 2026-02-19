"""CPG pipeline using semantic operations.

This module shows how the SemanticCompiler is used to build the full CPG.
The pipeline is just function calls - DataFusion handles execution.

The entire pipeline in ~50 lines:
1. Normalize extraction tables (Rule 1 + 2)
2. Build relationships (Rule 5 + 7 or Rule 6 + 7)
3. Union to final outputs (Rule 8)
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from datafusion_engine.delta.schema_guard import SchemaEvolutionPolicy
from datafusion_engine.views.artifacts import CachePolicy
from obs.otel import SCOPE_SEMANTICS, stage_span
from semantics import pipeline_cache as _pipeline_cache
from semantics.cdf_resolution import (
    cdf_changed_inputs,
    outputs_from_changed_inputs,
    resolve_registered_cdf_inputs,
)
from semantics.naming import canonical_output_name
from semantics.output_materialization import (
    SemanticOutputWriteContext,
    materialize_semantic_outputs,
)
from semantics.pipeline_builders import (
    SemanticSpecContext,
    bundle_for_builder,
    cache_policy_for,
    finalize_output_builder,
    normalize_cache_policy,
    ordered_semantic_specs,
    semantic_view_specs,
)
from semantics.pipeline_diagnostics import (
    SemanticQualityDiagnosticsRequest,
    emit_semantic_quality_diagnostics,
)
from semantics.pipeline_diagnostics import (
    record_semantic_compile_artifacts as _record_semantic_compile_artifacts,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.plan.bundle_artifact import DataFrameBuilder, DataFusionPlanArtifact
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.views.graph import ViewNode
    from semantics.compile_context import SemanticExecutionContext
    from semantics.config import SemanticConfig
    from semantics.ir import SemanticIR
    from semantics.program_manifest import (
        ManifestDatasetResolver,
        SemanticProgramManifest,
    )
    from semantics.registry import SemanticModel


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
    """Configuration for semantic CPG construction.

    Attributes:
    ----------
    cache_policy
        Explicit per-view cache policy override.  When provided this takes
        precedence over ``compiled_cache_policy``.
    compiled_cache_policy
        Topology-derived cache policy mapping produced by the policy
        compiler.  Used as the fallback when explicit ``cache_policy`` is not
        provided. Typically populated from
        ``CompiledExecutionPolicy.cache_policy_by_view``.
    validate_schema
        Whether to validate output schemas.
    config
        Optional semantic pipeline configuration.
    use_cdf
        Whether to enable CDF-aware incremental joins.
    cdf_inputs
        CDF input overrides.
    materialize_outputs
        Whether to materialize semantic output tables.
    requested_outputs
        Subset of output views to build.
    schema_policy
        Schema evolution policy for Delta writes.
    """

    cache_policy: Mapping[str, CachePolicy] | None = None
    compiled_cache_policy: Mapping[str, CachePolicy] | None = None
    validate_schema: bool = True
    config: SemanticConfig | None = None
    use_cdf: bool | None = None
    cdf_inputs: Mapping[str, str] | None = None
    materialize_outputs: bool = True
    requested_outputs: Collection[str] | None = None
    schema_policy: SchemaEvolutionPolicy = field(default_factory=SchemaEvolutionPolicy)

    def __post_init__(self) -> None:
        """Normalize compiled cache policy values to canonical cache-policy strings."""
        if self.compiled_cache_policy is None:
            return
        normalized = {
            name: normalize_cache_policy(str(value))
            for name, value in self.compiled_cache_policy.items()
        }
        object.__setattr__(self, "compiled_cache_policy", normalized)


@dataclass(frozen=True)
class CpgViewNodesRequest:
    """Inputs required to build semantic view nodes."""

    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile
    output_locations: Mapping[str, DatasetLocation]
    cache_policy: Mapping[str, CachePolicy] | None
    compiled_cache_policy: Mapping[str, CachePolicy] | None
    config: SemanticConfig | None
    input_mapping: Mapping[str, str]
    use_cdf: bool
    requested_outputs: Collection[str] | None
    manifest: SemanticProgramManifest
    semantic_ir: SemanticIR


@dataclass(frozen=True)
class CpgViewSpecsRequest:
    """Inputs required to build semantic view specs."""

    input_mapping: Mapping[str, str]
    config: SemanticConfig | None
    use_cdf: bool
    runtime_profile: DataFusionRuntimeProfile | None
    requested_outputs: Collection[str] | None
    manifest: SemanticProgramManifest
    semantic_ir: SemanticIR


@dataclass(frozen=True)
class _IncrementalOutputRequest:
    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile
    input_mapping: Mapping[str, str]
    use_cdf: bool
    requested_outputs: Collection[str] | None
    model: SemanticModel
    dataset_resolver: ManifestDatasetResolver | None = None


@dataclass(frozen=True)
class _CpgCompileResolution:
    """Resolved compile artifacts for CPG pipeline entry points."""

    resolver: ManifestDatasetResolver
    manifest: SemanticProgramManifest
    input_mapping: Mapping[str, str]
    use_cdf: bool
    resolved_outputs: Collection[str] | None


@dataclass(frozen=True)
class _CpgBuildExecutionResult:
    """Execution outputs from a single semantic CPG build pass."""

    nodes: tuple[ViewNode, ...]
    semantic_ir: SemanticIR
    requested_outputs: Collection[str] | None
    manifest: SemanticProgramManifest
    dataset_resolver: ManifestDatasetResolver


def _resolve_cpg_compile_artifacts(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    effective_use_cdf: bool,
    resolved: CpgBuildOptions,
    execution_context: SemanticExecutionContext,
) -> _CpgCompileResolution:
    """Resolve dataset resolver and manifest from execution context or compile boundary.

    Encapsulates the common resolver + manifest resolution pattern shared by
    ``build_cpg()`` and ``build_cpg_from_inferred_deps()``, eliminating
    redundant semantic-context reconstruction at each call site.

    Returns:
    -------
    _CpgCompileResolution
        Resolved compile artifacts including resolver, manifest, and mapping.
    """
    early_resolver = execution_context.dataset_resolver
    manifest = execution_context.manifest
    model = execution_context.model
    input_mapping, use_cdf = _resolve_semantic_input_mapping(
        ctx,
        runtime_profile=runtime_profile,
        use_cdf=effective_use_cdf,
        cdf_inputs=resolved.cdf_inputs,
        dataset_resolver=early_resolver,
    )
    resolved_outputs = _resolve_requested_outputs(
        resolved.requested_outputs,
        model=model,
        manifest=manifest,
    )
    incremental_outputs = _incremental_requested_outputs(
        _IncrementalOutputRequest(
            ctx=ctx,
            runtime_profile=runtime_profile,
            input_mapping=input_mapping,
            use_cdf=use_cdf,
            requested_outputs=resolved.requested_outputs,
            model=model,
            dataset_resolver=early_resolver,
        )
    )
    if incremental_outputs is not None:
        resolved_outputs = incremental_outputs
    return _CpgCompileResolution(
        resolver=early_resolver,
        manifest=manifest,
        input_mapping=input_mapping,
        use_cdf=use_cdf,
        resolved_outputs=resolved_outputs,
    )


def _resolve_requested_outputs(
    requested_outputs: Collection[str] | None,
    *,
    model: SemanticModel,
    manifest: SemanticProgramManifest,
) -> set[str] | None:
    if requested_outputs is None:
        return {spec.name for spec in model.outputs if spec.kind == "table"}
    return {canonical_output_name(name, manifest=manifest) for name in requested_outputs}


def _incremental_requested_outputs(
    request: _IncrementalOutputRequest,
) -> set[str] | None:
    if request.requested_outputs is not None or not request.use_cdf:
        return None
    changed_inputs = cdf_changed_inputs(
        request.ctx,
        runtime_profile=request.runtime_profile,
        input_mapping=request.input_mapping,
        dataset_resolver=request.dataset_resolver,
    )
    if changed_inputs is None:
        return None
    if not changed_inputs:
        return set()
    return outputs_from_changed_inputs(changed_inputs, model=request.model)


def cpg_view_specs(
    request: CpgViewSpecsRequest,
) -> list[tuple[str, DataFrameBuilder]]:
    """Build view specs for the CPG pipeline.

    Args:
        request: Description.

    Returns:
        list[tuple[str, DataFrameBuilder]]: Result.

    Raises:
        ValueError: If no semantic normalization spec exists for a requested output.

    """
    from semantics.registry import (
        RELATIONSHIP_SPECS,
        SemanticSpecIndex,
        normalization_spec_for_output,
    )

    relationship_by_name = {spec.name: spec for spec in RELATIONSHIP_SPECS}
    semantic_ir = request.semantic_ir
    ir = semantic_ir
    join_groups_by_name = {group.name: group for group in ir.join_groups}
    join_group_by_relationship = {
        rel_name: group for group in ir.join_groups for rel_name in group.relationship_names
    }

    if request.runtime_profile is None:
        msg = "Runtime profile is required for CPG output views."
        raise ValueError(msg)

    context = SemanticSpecContext(
        relationship_by_name=relationship_by_name,
        input_mapping=request.input_mapping,
        config=request.config,
        use_cdf=request.use_cdf,
        normalization_spec_for_output=normalization_spec_for_output,
        join_groups_by_name=join_groups_by_name,
        join_group_by_relationship=join_group_by_relationship,
        runtime_profile=request.runtime_profile,
        manifest=request.manifest,
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
    return semantic_view_specs(
        ordered_specs=ordered_semantic_specs(spec_index),
        context=context,
    )


def _resolve_cache_policy_hierarchy(
    *,
    explicit_policy: Mapping[str, CachePolicy] | None,
    compiled_policy: Mapping[str, CachePolicy] | None,
) -> Mapping[str, CachePolicy]:
    return _pipeline_cache.resolve_cache_policy_hierarchy(
        explicit_policy=explicit_policy,
        compiled_policy=compiled_policy,
    )


def _view_nodes_for_cpg(request: CpgViewNodesRequest) -> list[ViewNode]:
    from datafusion_engine.views.graph import ViewNode

    view_specs = cpg_view_specs(
        CpgViewSpecsRequest(
            input_mapping=request.input_mapping,
            config=request.config,
            use_cdf=request.use_cdf,
            runtime_profile=request.runtime_profile,
            requested_outputs=request.requested_outputs,
            manifest=request.manifest,
            semantic_ir=request.semantic_ir,
        )
    )
    resolved_cache: dict[str, CachePolicy] = dict(
        _resolve_cache_policy_hierarchy(
            explicit_policy=request.cache_policy,
            compiled_policy=request.compiled_cache_policy,
        )
    )
    cache_overrides = request.runtime_profile.semantic_cache_overrides()
    if cache_overrides:
        normalized_overrides: dict[str, CachePolicy] = {
            name: normalize_cache_policy(value) for name, value in cache_overrides.items()
        }
        resolved_cache.update(normalized_overrides)
    nodes: list[ViewNode] = []

    for name, builder in view_specs:
        nodes.append(
            ViewNode(
                name=name,
                deps=(),
                builder=builder,
                plan_bundle=bundle_for_builder(
                    request.ctx,
                    runtime_profile=request.runtime_profile,
                    builder=builder,
                    view_name=name,
                    semantic_ir=request.semantic_ir,
                ),
                cache_policy=cache_policy_for(name, resolved_cache),
            )
        )
    return nodes


def _execute_cpg_build(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    resolved: CpgBuildOptions,
    effective_use_cdf: bool,
    execution_context: SemanticExecutionContext,
    emit_quality_diagnostics: bool = True,
) -> _CpgBuildExecutionResult:
    """Run one semantic CPG build pass and return resolved build artifacts.

    Returns:
        _CpgBuildExecutionResult: Materialized execution artifacts for this build pass.

    Raises:
        ValueError: If compiled semantic manifest is missing validation details.
        SemanticInputValidationError: If semantic input validation fails.
    """
    compile_resolution = _resolve_cpg_compile_artifacts(
        ctx,
        runtime_profile=runtime_profile,
        effective_use_cdf=effective_use_cdf,
        resolved=resolved,
        execution_context=execution_context,
    )
    early_resolver = compile_resolution.resolver
    manifest = compile_resolution.manifest
    input_mapping = compile_resolution.input_mapping
    use_cdf = compile_resolution.use_cdf
    resolved_outputs = compile_resolution.resolved_outputs
    from datafusion_engine.udf.extension_runtime import rust_udf_snapshot
    from datafusion_engine.views.graph import (
        ViewGraphOptions,
        ViewGraphRuntimeOptions,
        register_view_graph,
    )
    from semantics.validation import SemanticInputValidationError

    snapshot = rust_udf_snapshot(ctx)
    validation = manifest.validation
    if validation is None:
        msg = "Semantic manifest compile must include input validation results."
        raise ValueError(msg)
    if not validation.valid:
        raise SemanticInputValidationError(validation)
    semantic_ir = manifest.semantic_ir
    from datafusion_engine.session.runtime_dataset_io import semantic_output_locations_for_profile

    nodes = _view_nodes_for_cpg(
        CpgViewNodesRequest(
            ctx=ctx,
            runtime_profile=runtime_profile,
            output_locations=semantic_output_locations_for_profile(runtime_profile),
            cache_policy=resolved.cache_policy,
            compiled_cache_policy=resolved.compiled_cache_policy,
            config=resolved.config,
            input_mapping=input_mapping,
            use_cdf=use_cdf,
            requested_outputs=resolved_outputs,
            manifest=manifest,
            semantic_ir=semantic_ir,
        )
    )
    register_view_graph(
        ctx,
        nodes=nodes,
        snapshot=snapshot,
        runtime_options=ViewGraphRuntimeOptions(
            runtime_profile=runtime_profile,
            dataset_resolver=early_resolver,
        ),
        options=ViewGraphOptions(
            overwrite=True,
            temporary=False,
            validate_schema=resolved.validate_schema,
        ),
    )
    plan_bundles: dict[str, DataFusionPlanArtifact] = {
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
        materialize_semantic_outputs(
            ctx,
            runtime_profile=runtime_profile,
            schema_policy=resolved.schema_policy,
            model=execution_context.model,
            requested_outputs=resolved_outputs,
            manifest=manifest,
        )
    if emit_quality_diagnostics:
        emit_semantic_quality_diagnostics(
            SemanticQualityDiagnosticsRequest(
                ctx=ctx,
                runtime_profile=runtime_profile,
                dataset_resolver=early_resolver,
                schema_policy=resolved.schema_policy,
                requested_outputs=resolved_outputs,
                manifest=manifest,
                finalize_builder=finalize_output_builder,
            )
        )
    return _CpgBuildExecutionResult(
        nodes=tuple(nodes),
        semantic_ir=semantic_ir,
        requested_outputs=resolved_outputs,
        manifest=manifest,
        dataset_resolver=early_resolver,
    )


def build_cpg(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    options: CpgBuildOptions | None = None,
    execution_context: SemanticExecutionContext,
) -> None:
    """Build the complete CPG from extraction tables.

    Parameters
    ----------
    ctx
        DataFusion session context.
    runtime_profile
        Runtime configuration for building cached view graphs.
    options
        Optional build settings for cache policy, schema validation, and CDF inputs.
    execution_context
        Pre-compiled semantic execution context used for manifest and
        dataset resolver authority.

    Raises:
        ValueError: If runtime profile is missing.
    """
    resolved = options or CpgBuildOptions()
    if runtime_profile is None:
        msg = "build_cpg requires a runtime_profile for view-graph execution."
        raise ValueError(msg)
    effective_use_cdf = (
        resolved.use_cdf
        if resolved.use_cdf is not None
        else runtime_profile.features.enable_delta_cdf
    )

    with stage_span(
        "semantics.build_cpg",
        stage="semantics",
        scope_name=SCOPE_SEMANTICS,
        attributes={
            "codeanatomy.validate_schema": resolved.validate_schema,
            "codeanatomy.use_cdf": resolved.use_cdf,
            "codeanatomy.has_cache_policy": resolved.cache_policy is not None,
            "codeanatomy.has_compiled_cache_policy": resolved.compiled_cache_policy is not None,
            "codeanatomy.has_config": resolved.config is not None,
        },
    ):
        _execute_cpg_build(
            ctx,
            runtime_profile=runtime_profile,
            resolved=resolved,
            effective_use_cdf=effective_use_cdf,
            execution_context=execution_context,
        )


def build_cpg_from_inferred_deps(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    options: CpgBuildOptions | None = None,
    execution_context: SemanticExecutionContext,
) -> dict[str, object]:
    """Build CPG and extract dependency information for rustworkx.

    Return InferredDeps-compatible data for scheduling.

    Parameters
    ----------
    ctx
        DataFusion session context.
    runtime_profile
        Runtime configuration for building cached view graphs.
    options
        Optional build settings for cache policy, schema validation, and CDF inputs.
    execution_context
        Pre-compiled semantic execution context used for manifest and
        dataset resolver authority.

    Returns:
    -------
    dict[str, object]
        Mapping of view names to dependency information.

    """
    from datafusion_engine.views.bundle_extraction import extract_lineage_from_bundle

    resolved = options or CpgBuildOptions()
    effective_use_cdf = (
        resolved.use_cdf
        if resolved.use_cdf is not None
        else runtime_profile.features.enable_delta_cdf
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
        # Inferred-dependency compilation should preserve a single logical DAG
        # and avoid extra materialization boundaries.
        inference_options = replace(resolved, materialize_outputs=False)
        build_result = _execute_cpg_build(
            ctx,
            runtime_profile=runtime_profile,
            resolved=inference_options,
            effective_use_cdf=effective_use_cdf,
            execution_context=execution_context,
            emit_quality_diagnostics=False,
        )

        deps: dict[str, object] = {}
        for node in build_result.nodes:
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
    dataset_resolver: ManifestDatasetResolver,
) -> tuple[dict[str, str], bool]:
    from semantics.validation import resolve_semantic_input_mapping

    resolved_inputs = resolve_semantic_input_mapping(ctx)
    if use_cdf is False:
        return dict(resolved_inputs), False

    cdf_candidates = tuple(
        source for canonical, source in resolved_inputs.items() if canonical != "file_line_index_v1"
    )
    resolved_cdf = resolve_registered_cdf_inputs(
        ctx,
        runtime_profile=runtime_profile,
        use_cdf=use_cdf,
        cdf_inputs=cdf_inputs,
        inputs=cdf_candidates,
        dataset_resolver=dataset_resolver,
    )

    if not resolved_cdf:
        return dict(resolved_inputs), False

    mapped: dict[str, str] = {}
    for canonical, source in resolved_inputs.items():
        override = resolved_cdf.get(source) or resolved_cdf.get(canonical)
        mapped[canonical] = override if override is not None else source
    return mapped, True


__all__ = [
    "CPG_INPUT_TABLES",
    "CachePolicy",
    "CpgBuildOptions",
    "CpgViewNodesRequest",
    "CpgViewSpecsRequest",
    "SemanticOutputWriteContext",
    "build_cpg",
    "build_cpg_from_inferred_deps",
    "cpg_view_specs",
]
