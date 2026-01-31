"""CPG pipeline using semantic operations.

This module shows how the SemanticCompiler is used to build the full CPG.
The pipeline is just function calls - DataFusion handles execution.

The entire pipeline in ~50 lines:
1. Normalize extraction tables (Rule 1 + 2)
2. Build relationships (Rule 5 + 7 or Rule 6 + 7)
3. Union to final outputs (Rule 8)
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from obs.otel.scopes import SCOPE_SEMANTICS
from obs.otel.tracing import stage_span
from semantics.specs import RelationshipSpec

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from datafusion_engine.plan.bundle import DataFrameBuilder, DataFusionPlanBundle
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.views.graph import ViewNode
    from semantics.config import SemanticConfig
    from semantics.spec_registry import SemanticNormalizationSpec, SemanticSpecIndex


CachePolicy = Literal["none", "delta_staging", "delta_output"]
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


@dataclass(frozen=True)
class CpgViewNodesRequest:
    """Inputs required to build semantic view nodes."""

    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile
    cache_policy: Mapping[str, CachePolicy] | None
    config: SemanticConfig | None
    input_mapping: Mapping[str, str]
    use_cdf: bool


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


def _default_semantic_cache_policy(
    *,
    view_names: Sequence[str],
    runtime_profile: DataFusionRuntimeProfile,
) -> dict[str, CachePolicy]:
    resolved: dict[str, CachePolicy] = {}
    for name in view_names:
        if name in {"cpg_nodes_v1", "cpg_edges_v1"}:
            if runtime_profile.dataset_location(name) is not None:
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
    spec: RelationshipSpec,
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

        return SemanticCompiler(inner_ctx, config=config).relate(
            spec.left_table,
            spec.right_table,
            options=RelationOptions(
                strategy_hint=spec.to_strategy_type(),
                filter_sql=spec.filter_sql,
                origin=spec.origin,
                use_cdf=use_cdf,
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

    normalization_by_output = {
        spec.output_name: spec for spec in SEMANTIC_NORMALIZATION_SPECS
    }
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
            runtime_profile=request.runtime_profile,
        )
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
                cache_policy=_cache_policy_for(name, resolved_cache),
            )
        )
    return nodes


def build_cpg(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile | None = None,
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
            use_cdf=resolved.use_cdf,
            cdf_inputs=resolved.cdf_inputs,
        )

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


def build_cpg_from_inferred_deps(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
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
    options
        Optional build settings for cache policy, schema validation, and CDF inputs.

    Returns
    -------
    dict[str, object]
        Mapping of view names to dependency information.
    """
    from datafusion_engine.views.bundle_extraction import extract_lineage_from_bundle

    resolved = options or CpgBuildOptions()

    with stage_span(
        "semantics.build_cpg_from_inferred_deps",
        stage="semantics",
        scope_name=SCOPE_SEMANTICS,
        attributes={
            "codeanatomy.validate_schema": resolved.validate_schema,
            "codeanatomy.use_cdf": resolved.use_cdf,
            "codeanatomy.has_cache_policy": resolved.cache_policy is not None,
        },
    ):
        build_cpg(ctx, runtime_profile=runtime_profile, options=resolved)

        deps: dict[str, object] = {}
        input_mapping, use_cdf = _resolve_semantic_input_mapping(
            ctx,
            runtime_profile=runtime_profile,
            use_cdf=resolved.use_cdf,
            cdf_inputs=resolved.cdf_inputs,
        )
        nodes = _view_nodes_for_cpg(
            CpgViewNodesRequest(
                ctx=ctx,
                runtime_profile=runtime_profile,
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
        source
        for canonical, source in resolved_inputs.items()
        if canonical != "file_line_index_v1"
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
