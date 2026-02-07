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
from typing import TYPE_CHECKING, Protocol, cast

from datafusion_engine.delta.schema_guard import SchemaEvolutionPolicy
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.io.write import WritePipeline
from datafusion_engine.views.artifacts import CachePolicy
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
from semantics.incremental.runtime import IncrementalRuntime, IncrementalRuntimeBuildRequest
from semantics.incremental.state_store import StateStore
from semantics.naming import canonical_output_name
from semantics.quality import QualityRelationshipSpec
from semantics.registry import SEMANTIC_MODEL
from semantics.specs import RelationshipSpec
from utils.env_utils import env_value
from utils.hashing import hash_msgpack_canonical
from utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.lineage.diagnostics import DiagnosticsSink
    from datafusion_engine.plan.bundle import DataFrameBuilder, DataFusionPlanBundle
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile, SessionRuntime
    from datafusion_engine.views.graph import ViewNode
    from semantics.compile_context import SemanticExecutionContext
    from semantics.config import SemanticConfig
    from semantics.ir import SemanticIR, SemanticIRJoinGroup, SemanticIRView
    from semantics.program_manifest import (
        ManifestDatasetResolver,
        SemanticProgramManifest,
    )
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
    output_locations: Mapping[str, DatasetLocation]
    cache_policy: Mapping[str, CachePolicy] | None
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
    dataset_resolver: ManifestDatasetResolver | None = None


@dataclass(frozen=True)
class _CpgCompileResolution:
    """Resolved compile artifacts for CPG pipeline entry points."""

    resolver: ManifestDatasetResolver
    manifest: SemanticProgramManifest
    input_mapping: Mapping[str, str]
    use_cdf: bool
    resolved_outputs: Collection[str] | None


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
    redundant ``CompileContext`` construction at each call site.

    Returns:
    -------
    _CpgCompileResolution
        Resolved compile artifacts including resolver, manifest, and mapping.
    """
    early_resolver = execution_context.dataset_resolver
    manifest = execution_context.manifest
    input_mapping, use_cdf = _resolve_semantic_input_mapping(
        ctx,
        runtime_profile=runtime_profile,
        use_cdf=effective_use_cdf,
        cdf_inputs=resolved.cdf_inputs,
        dataset_resolver=early_resolver,
    )
    resolved_outputs = _resolve_requested_outputs(
        resolved.requested_outputs,
        manifest=manifest,
    )
    incremental_outputs = _incremental_requested_outputs(
        _IncrementalOutputRequest(
            ctx=ctx,
            runtime_profile=runtime_profile,
            input_mapping=input_mapping,
            use_cdf=use_cdf,
            requested_outputs=resolved.requested_outputs,
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


class _CdfCursorLike(Protocol):
    last_version: int


class _CdfCursorStoreLike(Protocol):
    def load_cursor(self, name: str) -> _CdfCursorLike | None: ...


class _DeltaServiceLike(Protocol):
    def cdf_enabled(
        self,
        path: str,
        *,
        storage_options: Mapping[str, str],
        log_storage_options: Mapping[str, str],
    ) -> bool: ...

    def table_version(
        self,
        *,
        path: str,
        storage_options: Mapping[str, str],
        log_storage_options: Mapping[str, str],
    ) -> int | None: ...


@dataclass(frozen=True)
class SemanticOutputWriteContext:
    """Inputs required to materialize semantic outputs."""

    ctx: SessionContext
    pipeline: WritePipeline
    runtime_profile: DataFusionRuntimeProfile
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
    output_locations: Mapping[str, DatasetLocation],
) -> dict[str, CachePolicy]:
    resolved: dict[str, CachePolicy] = {}
    for name in view_names:
        if name.startswith("cpg_"):
            if name in output_locations:
                resolved[name] = "delta_output"
            else:
                resolved[name] = "delta_staging"
            continue
        if name in {"relation_output", "dim_exported_defs"}:
            resolved[name] = "delta_staging"
            continue
        if name in {"semantic_nodes_union", "semantic_edges_union"}:
            resolved[name] = "delta_staging"
            continue
        if name.startswith("rel_") or name.endswith("_norm"):
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

    Returns:
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
        import msgspec

        from semantics.compiler import SemanticCompiler

        resolved_table = input_mapping.get(spec.source_table, spec.source_table)
        resolved_spec = msgspec.structs.replace(spec.spec, table=resolved_table)
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

    Returns:
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
    from schema_spec.dataset_spec_ops import dataset_spec_contract
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


def _bytecode_line_table_builder(
    line_table: str,
    *,
    line_index_table: str,
) -> DataFrameBuilder:
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


def _resolve_requested_outputs(
    requested_outputs: Collection[str] | None,
    *,
    manifest: SemanticProgramManifest,
) -> set[str] | None:
    if requested_outputs is None:
        return {spec.name for spec in SEMANTIC_MODEL.outputs if spec.kind == "table"}
    return {canonical_output_name(name, manifest=manifest) for name in requested_outputs}


def _incremental_requested_outputs(
    request: _IncrementalOutputRequest,
) -> set[str] | None:
    if request.requested_outputs is not None or not request.use_cdf:
        return None
    changed_inputs = _cdf_changed_inputs(
        request.ctx,
        runtime_profile=request.runtime_profile,
        input_mapping=request.input_mapping,
        dataset_resolver=request.dataset_resolver,
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
    input_mapping: Mapping[str, str],
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> set[str] | None:
    from semantics.incremental.cdf_cursors import CdfCursorStore

    _ = ctx
    if dataset_resolver is None:
        msg = "dataset_resolver is required for _cdf_changed_inputs."
        raise ValueError(msg)
    cursor_store = runtime_profile.data_sources.cdf_cursor_store
    if cursor_store is None:
        return None
    if not isinstance(cursor_store, CdfCursorStore):
        return None
    typed_cursor_store = cast("_CdfCursorStoreLike", cursor_store)
    delta_service = cast("_DeltaServiceLike", runtime_profile.delta_ops.delta_service())
    return {
        canonical
        for canonical, source in input_mapping.items()
        if _input_has_cdf_changes(
            canonical=canonical,
            source=source,
            dataset_resolver=dataset_resolver,
            cursor_store=typed_cursor_store,
            delta_service=delta_service,
        )
    }


def _resolve_cdf_location(
    *,
    canonical: str,
    source: str,
    dataset_resolver: ManifestDatasetResolver,
) -> DatasetLocation | None:
    if canonical == "file_line_index_v1":
        return None
    location = dataset_resolver.location(canonical)
    if location is not None:
        return location
    return dataset_resolver.location(source)


def _input_has_cdf_changes(
    *,
    canonical: str,
    source: str,
    dataset_resolver: ManifestDatasetResolver,
    cursor_store: _CdfCursorStoreLike,
    delta_service: _DeltaServiceLike,
) -> bool:
    location = _resolve_cdf_location(
        canonical=canonical,
        source=source,
        dataset_resolver=dataset_resolver,
    )
    if location is None:
        return False
    storage_options = dict(location.storage_options)
    log_options = dict(location.delta_log_storage_options)
    if not _cdf_enabled_for_location(
        location,
        storage_options=storage_options,
        log_storage_options=log_options,
        cdf_enabled_fn=delta_service.cdf_enabled,
    ):
        return False
    latest_version = delta_service.table_version(
        path=str(location.path),
        storage_options=storage_options,
        log_storage_options=log_options,
    )
    if latest_version is None:
        return False
    cursor = cursor_store.load_cursor(canonical)
    return cursor is None or latest_version > cursor.last_version


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

    Args:
        request: Description.

    Returns:
        list[tuple[str, DataFrameBuilder]]: Result.

    Raises:
        ValueError: If no semantic normalization spec exists for a requested output.

    """
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
    runtime_profile: DataFusionRuntimeProfile
    manifest: SemanticProgramManifest


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


# ---------------------------------------------------------------------------
# Builder dispatch factory
# ---------------------------------------------------------------------------


def _dispatch_from_registry(
    registry_factory: Callable[[_SemanticSpecContext], Mapping[str, DataFrameBuilder]],
    context_label: str,
    *,
    finalize: bool = False,
) -> Callable[[SemanticSpecIndex, _SemanticSpecContext], DataFrameBuilder]:
    """Create a handler that resolves builders from a name-keyed registry.

    Parameters
    ----------
    registry_factory
        Callable that receives the spec context and returns a mapping from
        spec name to builder.  For context-independent registries, ignore
        the context argument.
    context_label
        Human-readable label used in ``KeyError`` messages.
    finalize
        When ``True``, wrap the resolved builder in
        ``_finalize_output_builder``.

    Returns:
    -------
    Callable[[SemanticSpecIndex, _SemanticSpecContext], DataFrameBuilder]
        Handler compatible with the ``_builder_for_semantic_spec`` dispatch
        table.
    """

    def _handler(
        spec: SemanticSpecIndex,
        context: _SemanticSpecContext,
    ) -> DataFrameBuilder:
        mapping = registry_factory(context)
        builder = mapping.get(spec.name)
        if builder is None:
            msg = f"Missing {context_label} builder for output {spec.name!r}."
            raise KeyError(msg)
        if finalize:
            return _finalize_output_builder(spec.name, builder)
        return builder

    return _handler


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


def _finalize_registry(
    context: _SemanticSpecContext,
) -> Mapping[str, DataFrameBuilder]:
    """Return the CPG finalize builder registry from context."""
    return dict(
        _cpg_output_view_specs(
            context.runtime_profile,
            manifest=context.manifest,
        )
    )


def _builder_for_artifact_spec(
    spec: SemanticSpecIndex,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    _ = context
    msg = f"Unsupported artifact spec output: {spec.name!r}."
    raise ValueError(msg)


_BUILDER_HANDLERS: dict[
    str, Callable[[SemanticSpecIndex, _SemanticSpecContext], DataFrameBuilder]
] = {
    "normalize": _builder_for_normalize_spec,
    "scip_normalize": _builder_for_scip_normalize_spec,
    "bytecode_line_index": _builder_for_bytecode_line_index_spec,
    "span_unnest": _dispatch_from_registry(_span_unnest_registry, "span-unnest"),
    "symtable": _dispatch_from_registry(_symtable_registry, "symtable"),
    "join_group": _builder_for_join_group_spec,
    "relate": _builder_for_relate_spec,
    "union_edges": _builder_for_union_edges_spec,
    "union_nodes": _builder_for_union_nodes_spec,
    "projection": _builder_for_projection_spec,
    "diagnostic": _dispatch_from_registry(_diagnostic_registry, "diagnostic", finalize=True),
    "export": _builder_for_export_spec,
    "finalize": _dispatch_from_registry(_finalize_registry, "finalize", finalize=True),
    "artifact": _builder_for_artifact_spec,
}


def _builder_for_semantic_spec(
    spec: SemanticSpecIndex,
    *,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    handler = _BUILDER_HANDLERS.get(spec.kind)
    if handler is None:
        msg = f"Unsupported semantic spec kind: {spec.kind!r}."
        raise ValueError(msg)
    return handler(spec, context)


def _input_table(input_mapping: Mapping[str, str], name: str) -> str:
    return input_mapping.get(name, name)


def _cpg_output_view_specs(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    manifest: SemanticProgramManifest,
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

    def _maybe_validate_view_df(
        view_name: str,
        df: DataFrame,
        *,
        profile: DataFusionRuntimeProfile | None,
    ) -> DataFrame:
        if view_name.startswith("cpg_"):
            return df
        try:
            from semantics.catalog.dataset_specs import maybe_dataset_spec
        except (ImportError, RuntimeError):
            return df
        spec = maybe_dataset_spec(view_name)
        if spec is None:
            return df
        from schema_spec.dataset_spec_ops import dataset_spec_delta_constraints

        policy = spec.policies.dataframe_validation
        constraints: tuple[str, ...] = ()
        if spec.contract_spec is not None:
            constraints = tuple(spec.contract_spec.constraints)
        delta_constraints = dataset_spec_delta_constraints(spec)
        if delta_constraints:
            constraints = (*constraints, *delta_constraints)
        diagnostics = None
        if profile is not None:
            diagnostics = profile.diagnostics.diagnostics_sink
        from schema_spec.pandera_bridge import DataframeValidationRequest, validate_with_policy

        request = DataframeValidationRequest(
            df=df,
            schema_spec=spec.table_spec,
            policy=policy,
            constraints=constraints,
            diagnostics=diagnostics,
            name=view_name,
        )
        return validate_with_policy(request)

    def _wrap_cpg_builder(
        view_name: str,
        builder: Callable[[SessionRuntime], DataFrame],
    ) -> DataFrameBuilder:
        def _builder(inner_ctx: SessionContext) -> DataFrame:
            _ = inner_ctx
            df = builder(session_runtime)
            return _maybe_validate_view_df(view_name, df, profile=runtime_profile)

        return _builder

    return [
        (
            canonical_output_name("cpg_nodes", manifest=manifest),
            _wrap_cpg_builder(
                canonical_output_name("cpg_nodes", manifest=manifest),
                build_cpg_nodes_df,
            ),
        ),
        (
            canonical_output_name("cpg_edges", manifest=manifest),
            _wrap_cpg_builder(
                canonical_output_name("cpg_edges", manifest=manifest),
                build_cpg_edges_df,
            ),
        ),
        (
            canonical_output_name("cpg_props", manifest=manifest),
            _wrap_cpg_builder(
                canonical_output_name("cpg_props", manifest=manifest),
                build_cpg_props_df,
            ),
        ),
        (
            canonical_output_name("cpg_props_map", manifest=manifest),
            _wrap_cpg_builder(
                canonical_output_name("cpg_props_map", manifest=manifest),
                build_cpg_props_map_df,
            ),
        ),
        (
            canonical_output_name("cpg_edges_by_src", manifest=manifest),
            _wrap_cpg_builder(
                canonical_output_name("cpg_edges_by_src", manifest=manifest),
                build_cpg_edges_by_src_df,
            ),
        ),
        (
            canonical_output_name("cpg_edges_by_dst", manifest=manifest),
            _wrap_cpg_builder(
                canonical_output_name("cpg_edges_by_dst", manifest=manifest),
                build_cpg_edges_by_dst_df,
            ),
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
            manifest=request.manifest,
            semantic_ir=request.semantic_ir,
        )
    )
    resolved_cache = request.cache_policy
    if resolved_cache is None:
        resolved_cache = _default_semantic_cache_policy(
            view_names=[name for name, _builder in view_specs],
            output_locations=request.output_locations,
        )
    cache_overrides = request.runtime_profile.data_sources.semantic_output.cache_overrides
    if cache_overrides:
        resolved_cache = {**resolved_cache, **cache_overrides}
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
    from serde_artifact_specs import (
        SEMANTIC_EXPLAIN_PLAN_REPORT_SPEC,
        SEMANTIC_EXPLAIN_PLAN_SPEC,
        SEMANTIC_IR_FINGERPRINT_SPEC,
        SEMANTIC_JOIN_GROUP_STATS_SPEC,
        SEMANTIC_VIEW_PLAN_STATS_SPEC,
    )

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
    report = _semantic_explain_markdown(
        semantic_ir,
        view_stats=view_stats,
    )
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
        SemanticInputValidationError: If required semantic inputs fail validation.
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
            "codeanatomy.has_config": resolved.config is not None,
        },
    ):
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
        from datafusion_engine.udf.runtime import rust_udf_snapshot
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
        from datafusion_engine.session.runtime import semantic_output_locations_for_profile

        nodes = _view_nodes_for_cpg(
            CpgViewNodesRequest(
                ctx=ctx,
                runtime_profile=runtime_profile,
                output_locations=semantic_output_locations_for_profile(runtime_profile),
                cache_policy=resolved.cache_policy,
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
                schema_policy=resolved.schema_policy,
                requested_outputs=resolved_outputs,
                manifest=manifest,
            )
        _emit_semantic_quality_diagnostics(
            ctx,
            runtime_profile=runtime_profile,
            dataset_resolver=early_resolver,
            schema_policy=resolved.schema_policy,
            requested_outputs=resolved_outputs,
            manifest=manifest,
        )


def _semantic_output_view_names(
    *,
    requested_outputs: Collection[str] | None = None,
    manifest: SemanticProgramManifest,
) -> list[str]:
    """Return semantic output view names.

    Returns:
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

    resolved = {canonical_output_name(name, manifest=manifest) for name in requested_outputs}
    return [spec.name for spec in SEMANTIC_MODEL.outputs if spec.name in resolved]


def _ensure_canonical_output_locations(
    output_locations: Mapping[str, DatasetLocation],
    *,
    manifest: SemanticProgramManifest,
) -> None:
    """Validate that semantic output locations use canonical names.

    Args:
        output_locations: Description.
        manifest: Compiled semantic manifest with canonical output name map.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    from semantics.naming import canonical_output_name

    non_canonical = {
        name: canonical_output_name(name, manifest=manifest)
        for name in output_locations
        if canonical_output_name(name, manifest=manifest) != name
    }
    if non_canonical:
        msg = f"Semantic outputs must use canonical names: {non_canonical!r}."
        raise ValueError(msg)


def _semantic_output_locations(
    view_names: Sequence[str],
    runtime_profile: DataFusionRuntimeProfile,
) -> dict[str, DatasetLocation]:
    """Resolve dataset locations for semantic outputs.

    Parameters
    ----------
    view_names
        Semantic view names to materialize.
    runtime_profile
        Runtime profile providing semantic output locations.

    Returns:
    -------
    dict[str, DatasetLocation]
        Mapping of view name to dataset location.
    """
    import msgspec

    from datafusion_engine.dataset.registry import DatasetLocationOverrides
    from datafusion_engine.session.runtime import semantic_output_locations_for_profile
    from schema_spec.dataset_spec_ops import (
        dataset_spec_delta_feature_gate,
        dataset_spec_delta_maintenance_policy,
        dataset_spec_delta_schema_policy,
        dataset_spec_delta_write_policy,
    )
    from schema_spec.system import DeltaPolicyBundle
    from semantics.catalog.dataset_specs import dataset_spec

    base_locations = semantic_output_locations_for_profile(runtime_profile)
    output_locations: dict[str, DatasetLocation] = {}
    for name in view_names:
        base = base_locations.get(name)
        if base is None:
            continue
        spec = dataset_spec(name)
        delta_bundle = DeltaPolicyBundle(
            write_policy=dataset_spec_delta_write_policy(spec),
            schema_policy=dataset_spec_delta_schema_policy(spec),
            maintenance_policy=dataset_spec_delta_maintenance_policy(spec),
            feature_gate=dataset_spec_delta_feature_gate(spec),
        )
        existing_overrides = base.overrides
        if existing_overrides is not None:
            enriched_overrides = msgspec.structs.replace(existing_overrides, delta=delta_bundle)
        else:
            enriched_overrides = DatasetLocationOverrides(delta=delta_bundle)
        output_locations[name] = msgspec.structs.replace(
            base,
            dataset_spec=spec,
            overrides=enriched_overrides,
        )
    return output_locations


def _ensure_semantic_output_locations(
    view_names: Sequence[str],
    output_locations: Mapping[str, DatasetLocation],
) -> None:
    """Ensure all semantic outputs have explicit dataset locations.

    Args:
        view_names: Description.
        output_locations: Description.

    Raises:
        ValueError: If the operation cannot be completed.
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

    Args:
        view_name: Description.
        output_location: Description.
        write_context: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    from datafusion_engine.delta.schema_guard import enforce_schema_policy
    from datafusion_engine.delta.store_policy import apply_delta_store_policy
    from datafusion_engine.io.write import WriteFormat, WriteMode, WriteViewRequest
    from datafusion_engine.views.bundle_extraction import arrow_schema_from_df
    from schema_spec.dataset_spec_ops import (
        dataset_spec_delta_maintenance_policy,
        dataset_spec_delta_schema_policy,
        dataset_spec_delta_write_policy,
    )
    from semantics.catalog.dataset_specs import dataset_spec

    spec = dataset_spec(view_name)
    location = apply_delta_store_policy(
        output_location,
        policy=write_context.runtime_profile.policies.delta_store_policy,
    )
    if location.format != "delta":
        msg = f"Semantic output {view_name!r} must be stored as Delta."
        raise ValueError(msg)
    schema = arrow_schema_from_df(write_context.ctx.table(view_name))
    resolved_schema_policy = write_context.schema_policy
    delta_schema_policy = dataset_spec_delta_schema_policy(spec)
    if delta_schema_policy is not None and delta_schema_policy.schema_mode == "merge":
        resolved_schema_policy = SchemaEvolutionPolicy(mode="additive")
    if not write_context.runtime_profile.features.enable_schema_evolution_adapter:
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
    delta_write_policy = dataset_spec_delta_write_policy(spec)
    format_options: dict[str, object] = {
        "delta_commit_metadata": commit_metadata,
        "delta_write_policy": delta_write_policy,
        "delta_schema_policy": delta_schema_policy,
        "delta_maintenance_policy": dataset_spec_delta_maintenance_policy(spec),
    }
    partition_by = tuple(delta_write_policy.partition_by) if delta_write_policy is not None else ()
    write_context.pipeline.write_view(
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
    schema_policy: SchemaEvolutionPolicy,
    requested_outputs: Collection[str] | None,
    manifest: SemanticProgramManifest,
) -> None:
    from datafusion_engine.io.write import WritePipeline
    from datafusion_engine.session.runtime import semantic_output_locations_for_profile

    view_names = _semantic_output_view_names(
        requested_outputs=requested_outputs,
        manifest=manifest,
    )
    base_locations = semantic_output_locations_for_profile(runtime_profile)
    _ensure_canonical_output_locations(base_locations, manifest=manifest)
    output_locations = _semantic_output_locations(view_names, runtime_profile)
    _ensure_semantic_output_locations(view_names, output_locations)

    pipeline = WritePipeline(ctx, runtime_profile=runtime_profile)
    write_context = SemanticOutputWriteContext(
        ctx=ctx,
        pipeline=pipeline,
        runtime_profile=runtime_profile,
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
    dataset_resolver: ManifestDatasetResolver
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
        self.incremental_runtime = IncrementalRuntime.build(
            IncrementalRuntimeBuildRequest(
                profile=self.runtime_profile,
                dataset_resolver=self.dataset_resolver,
            )
        )
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
    dataset_resolver: ManifestDatasetResolver,
    schema_policy: SchemaEvolutionPolicy,
) -> _SemanticDiagnosticsContext | None:
    diagnostics_sink = runtime_profile.diagnostics.diagnostics_sink
    output_locations = _semantic_output_locations(
        view_names=SEMANTIC_DIAGNOSTIC_VIEW_NAMES,
        runtime_profile=runtime_profile,
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
            schema_policy=schema_policy,
        )
    storage_options: Mapping[str, str] | None = None
    if output_locations:
        first_loc = next(iter(output_locations.values()))
        loc_opts = first_loc.storage_options
        if loc_opts:
            storage_options = dict(loc_opts)
    return _SemanticDiagnosticsContext(
        ctx=ctx,
        runtime_profile=runtime_profile,
        dataset_resolver=dataset_resolver,
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
    manifest: SemanticProgramManifest,
) -> None:
    builders = semantic_diagnostic_view_builders()
    view_names = SEMANTIC_DIAGNOSTIC_VIEW_NAMES
    if requested_outputs is not None:
        resolved = {canonical_output_name(name, manifest=manifest) for name in requested_outputs}
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
    dataset_resolver: ManifestDatasetResolver,
    schema_policy: SchemaEvolutionPolicy,
    requested_outputs: Collection[str] | None,
    manifest: SemanticProgramManifest,
) -> None:
    if not runtime_profile.diagnostics.emit_semantic_quality_diagnostics:
        return
    context = _build_semantic_diagnostics_context(
        ctx,
        runtime_profile=runtime_profile,
        dataset_resolver=dataset_resolver,
        schema_policy=schema_policy,
    )
    if context is None:
        return
    with _run_context_guard():
        _emit_semantic_quality_views(
            context,
            requested_outputs=requested_outputs,
            manifest=manifest,
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

    Raises:
        SemanticInputValidationError: If required semantic inputs fail validation.
        ValueError: If runtime profile or semantic configuration is invalid.
    """
    from datafusion_engine.views.bundle_extraction import extract_lineage_from_bundle
    from semantics.validation import SemanticInputValidationError

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
        build_cpg(
            ctx,
            runtime_profile=runtime_profile,
            options=resolved,
            execution_context=execution_context,
        )

        deps: dict[str, object] = {}
        # Reuse the same compile resolution helper that build_cpg uses,
        # eliminating redundant CompileContext construction.
        compile_resolution = _resolve_cpg_compile_artifacts(
            ctx,
            runtime_profile=runtime_profile,
            effective_use_cdf=effective_use_cdf,
            resolved=resolved,
            execution_context=execution_context,
        )
        manifest = compile_resolution.manifest
        validation = manifest.validation
        if validation is None:
            msg = "Semantic manifest compile must include input validation results."
            raise ValueError(msg)
        if not validation.valid:
            raise SemanticInputValidationError(validation)
        semantic_ir = manifest.semantic_ir
        from datafusion_engine.session.runtime import semantic_output_locations_for_profile

        nodes = _view_nodes_for_cpg(
            CpgViewNodesRequest(
                ctx=ctx,
                runtime_profile=runtime_profile,
                output_locations=semantic_output_locations_for_profile(runtime_profile),
                cache_policy=resolved.cache_policy,
                config=resolved.config,
                input_mapping=compile_resolution.input_mapping,
                use_cdf=compile_resolution.use_cdf,
                requested_outputs=compile_resolution.resolved_outputs,
                manifest=manifest,
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
    dataset_resolver: ManifestDatasetResolver,
) -> tuple[dict[str, str], bool]:
    from semantics.validation import resolve_semantic_input_mapping

    resolved_inputs = resolve_semantic_input_mapping(ctx)
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
        dataset_resolver=dataset_resolver,
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
    dataset_resolver: ManifestDatasetResolver,
) -> Mapping[str, str] | None:
    if use_cdf is False:
        return None
    if runtime_profile is None:
        if use_cdf:
            msg = "CDF input registration requires a runtime profile."
            raise ValueError(msg)
        return dict(cdf_inputs) if cdf_inputs else None
    if use_cdf is None and not _has_cdf_inputs(
        runtime_profile, inputs=inputs, dataset_resolver=dataset_resolver
    ):
        return dict(cdf_inputs) if cdf_inputs else None
    from datafusion_engine.session.runtime import register_cdf_inputs_for_profile

    mapping = register_cdf_inputs_for_profile(
        runtime_profile, ctx, table_names=inputs, dataset_resolver=dataset_resolver
    )
    if cdf_inputs:
        mapping = {**mapping, **cdf_inputs}
    return mapping or None


def _has_cdf_inputs(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    inputs: Sequence[str],
    dataset_resolver: ManifestDatasetResolver,
) -> bool:
    from datafusion_engine.dataset.registry import resolve_datafusion_provider

    _ = runtime_profile
    for name in inputs:
        location = dataset_resolver.location(name)
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
