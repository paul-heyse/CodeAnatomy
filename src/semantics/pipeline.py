"""CPG pipeline using semantic operations.

This module shows how the SemanticCompiler is used to build the full CPG.
The pipeline is just function calls - DataFusion handles execution.

The entire pipeline in ~50 lines:
1. Normalize extraction tables (Rule 1 + 2)
2. Build relationships (Rule 5 + 7 or Rule 6 + 7)
3. Union to final outputs (Rule 8)
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

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
from semantics.quality import QualityRelationshipSpec
from semantics.runtime import CachePolicy, SemanticRuntimeConfig
from semantics.specs import RelationshipSpec
from utils.env_utils import env_value
from utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.io.write import WritePipeline
    from datafusion_engine.lineage.diagnostics import DiagnosticsSink
    from datafusion_engine.plan.bundle import DataFrameBuilder, DataFusionPlanBundle
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.views.graph import ViewNode
    from semantics.config import SemanticConfig
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


@dataclass(frozen=True)
class SemanticOutputWriteContext:
    """Inputs required to materialize semantic outputs."""

    ctx: SessionContext
    pipeline: WritePipeline
    runtime_profile: DataFusionRuntimeProfile
    runtime_config: SemanticRuntimeConfig
    schema_policy: SchemaEvolutionPolicy


@dataclass(frozen=True)
class RelateSpec:
    """Relation specification for semantic joins.

    .. deprecated::
        Use :class:`semantics.specs.RelationshipSpec` and the
        ``RELATIONSHIP_SPECS`` registry instead. This class is retained
        for backward compatibility with existing code.
    """

    left: str
    right: str
    join_type: Literal["overlap", "contains"]
    filter_sql: str | None
    origin: str

    @classmethod
    def from_relationship_spec(
        cls,
        spec: RelationshipSpec,
    ) -> RelateSpec:
        """Convert a RelationshipSpec to a RelateSpec.

        Parameters
        ----------
        spec
            The declarative relationship specification.

        Returns
        -------
        RelateSpec
            Equivalent RelateSpec for backward compatibility.
        """
        return cls(
            left=spec.left_table,
            right=spec.right_table,
            join_type=spec.join_type(),
            filter_sql=spec.filter_sql,
            origin=spec.origin,
        )


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
        if name in {"cpg_nodes_v1", "cpg_edges_v1"}:
            if runtime_config.output_path(name) is not None:
                resolved[name] = "delta_output"
            else:
                resolved[name] = "delta_staging"
            continue
        if name.startswith("rel_") or name.endswith("_norm_v1"):
            resolved[name] = "delta_staging"
            continue
        resolved[name] = "none"
    return resolved


def _bundle_for_builder(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    builder: DataFrameBuilder,
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
    return build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(
            compute_execution_plan=True,
            validate_udfs=True,
            session_runtime=session_runtime,
        ),
    )


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


def _relate_builder(
    spec: RelateSpec,
    *,
    config: SemanticConfig | None,
) -> DataFrameBuilder:
    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.compiler import RelationOptions, SemanticCompiler

        return SemanticCompiler(inner_ctx, config=config).relate(
            spec.left,
            spec.right,
            options=RelationOptions(
                join_type=spec.join_type,
                filter_sql=spec.filter_sql,
                origin=spec.origin,
            ),
        )

    return _builder


def _relationship_builder(
    spec: RelationshipSpec | QualityRelationshipSpec,
    *,
    config: SemanticConfig | None,
    use_cdf: bool,
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
            return compiler.compile_relationship_with_quality(
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


def _cpg_view_specs(
    *,
    input_mapping: Mapping[str, str],
    config: SemanticConfig | None,
    use_cdf: bool,
) -> list[tuple[str, DataFrameBuilder]]:
    """Build view specs for the CPG pipeline.

    Uses the declarative RELATIONSHIP_SPECS registry to generate relationship
    builders. New relationships can be added to the registry without modifying
    this function.

    All output names use the canonical naming policy from semantics.naming
    to ensure consistent versioned output names across all consumers.

    Parameters
    ----------
    input_mapping
        Mapping of canonical input names to registered table names.
    config
        Optional semantic configuration.
    use_cdf
        Whether to enable CDF-aware incremental joins.

    Returns
    -------
    list[tuple[str, DataFrameBuilder]]
        List of (view_name, builder) tuples for the CPG pipeline.

    Raises
    ------
    KeyError
        Raised when required normalization or relationship specs are missing.
    ValueError
        Raised when an unsupported semantic spec kind is encountered.
    """
    from semantics.spec_registry import (
        RELATIONSHIP_SPECS,
        SEMANTIC_NORMALIZATION_SPECS,
        normalization_spec_for_output,
        semantic_spec_index,
    )

    def _input(name: str) -> str:
        return input_mapping.get(name, name)

    normalization_by_output = {spec.output_name: spec for spec in SEMANTIC_NORMALIZATION_SPECS}
    relationship_by_name = {spec.name: spec for spec in RELATIONSHIP_SPECS}

    ordered_specs = _ordered_semantic_specs(semantic_spec_index())
    view_specs: list[tuple[str, DataFrameBuilder]] = []
    for spec in ordered_specs:
        if spec.kind == "normalize":
            norm_spec = normalization_by_output.get(spec.name) or normalization_spec_for_output(
                spec.name
            )
            if norm_spec is None:
                msg = f"Missing normalization spec for output {spec.name!r}."
                raise KeyError(msg)
            view_specs.append(
                (
                    spec.name,
                    _normalize_spec_builder(
                        norm_spec,
                        input_mapping=input_mapping,
                        config=config,
                    ),
                )
            )
            continue
        if spec.kind == "scip_normalize":
            view_specs.append(
                (
                    spec.name,
                    _scip_norm_builder(
                        _input("scip_occurrences"),
                        line_index_table=_input("file_line_index_v1"),
                    ),
                )
            )
            continue
        if spec.kind == "relate":
            rel_spec = relationship_by_name.get(spec.name)
            if rel_spec is None:
                msg = f"Missing relationship spec for output {spec.name!r}."
                raise KeyError(msg)
            view_specs.append(
                (
                    spec.name,
                    _relationship_builder(rel_spec, config=config, use_cdf=use_cdf),
                )
            )
            continue
        if spec.kind == "union_edges":
            view_specs.append(
                (
                    spec.name,
                    _union_edges_builder(list(spec.inputs), config=config),
                )
            )
            continue
        if spec.kind == "union_nodes":
            view_specs.append(
                (
                    spec.name,
                    _union_nodes_builder(list(spec.inputs), config=config),
                )
            )
            continue
        msg = f"Unsupported semantic spec kind: {spec.kind!r}."
        raise ValueError(msg)

    return view_specs


def _view_nodes_for_cpg(request: CpgViewNodesRequest) -> list[ViewNode]:
    from datafusion_engine.views.graph import ViewNode

    view_specs = _cpg_view_specs(
        input_mapping=request.input_mapping,
        config=request.config,
        use_cdf=request.use_cdf,
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
                ),
                cache_policy=_cache_policy_for(name, normalized_cache),
            )
        )
    return nodes


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

        snapshot = rust_udf_snapshot(ctx)
        nodes = _view_nodes_for_cpg(
            CpgViewNodesRequest(
                ctx=ctx,
                runtime_profile=runtime_profile,
                runtime_config=runtime_config,
                cache_policy=resolved.cache_policy,
                config=resolved.config,
                input_mapping=input_mapping,
                use_cdf=use_cdf,
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
        if resolved.materialize_outputs:
            _materialize_semantic_outputs(
                ctx,
                runtime_profile=runtime_profile,
                runtime_config=runtime_config,
                schema_policy=resolved.schema_policy,
            )
        _emit_semantic_quality_diagnostics(
            ctx,
            runtime_profile=runtime_profile,
            runtime_config=runtime_config,
            schema_policy=resolved.schema_policy,
        )


def _semantic_output_view_names() -> list[str]:
    """Return semantic output view names.

    Returns
    -------
    list[str]
        Semantic output view names, including the relation output.
    """
    from relspec.view_defs import RELATION_OUTPUT_NAME
    from semantics.naming import SEMANTIC_VIEW_NAMES

    view_names = list(SEMANTIC_VIEW_NAMES)
    if RELATION_OUTPUT_NAME not in view_names:
        view_names.append(RELATION_OUTPUT_NAME)
    return view_names


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
) -> None:
    from datafusion_engine.io.write import WritePipeline

    view_names = _semantic_output_view_names()
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


def _emit_semantic_quality_views(context: _SemanticDiagnosticsContext) -> None:
    builders = semantic_diagnostic_view_builders()
    for view_name in SEMANTIC_DIAGNOSTIC_VIEW_NAMES:
        builder = builders.get(view_name)
        if builder is None:
            continue
        _emit_semantic_quality_view(context, view_name=view_name, builder=builder)


def _emit_semantic_quality_diagnostics(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    runtime_config: SemanticRuntimeConfig,
    schema_policy: SchemaEvolutionPolicy,
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
        _emit_semantic_quality_views(context)


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
        nodes = _view_nodes_for_cpg(
            CpgViewNodesRequest(
                ctx=ctx,
                runtime_profile=runtime_profile,
                runtime_config=runtime_config,
                cache_policy=resolved.cache_policy,
                config=resolved.config,
                input_mapping=input_mapping,
                use_cdf=use_cdf,
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
    "RelateSpec",
    "RelationshipSpec",
    "build_cpg",
    "build_cpg_from_inferred_deps",
]
