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

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from datafusion_engine.plan.bundle import DataFrameBuilder, DataFusionPlanBundle
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.views.graph import ViewNode
    from semantics.config import SemanticConfig


CachePolicy = Literal["none", "delta_staging", "delta_output"]
CPG_INPUT_TABLES: tuple[str, ...] = (
    "cst_refs",
    "cst_defs",
    "cst_imports",
    "cst_callsites",
    "scip_occurrences",
)


@dataclass(frozen=True)
class CpgBuildOptions:
    """Configuration for semantic CPG construction."""

    cache_policy: Mapping[str, CachePolicy] | None = None
    validate_schema: bool = True
    config: SemanticConfig | None = None
    use_cdf: bool | None = None
    cdf_inputs: Mapping[str, str] | None = None


def _cache_policy_for(
    name: str,
    policy: Mapping[str, CachePolicy] | None,
) -> CachePolicy:
    if policy is None:
        return "none"
    return policy.get(name, "none")


def _view_nodes_for_cpg(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    cache_policy: Mapping[str, CachePolicy] | None,
    config: SemanticConfig | None,
    cdf_inputs: Mapping[str, str] | None,
) -> list[ViewNode]:
    from datafusion_engine.plan.bundle import PlanBundleOptions, build_plan_bundle
    from datafusion_engine.views.graph import ViewNode
    from semantics.compiler import SemanticCompiler

    def _bundle(builder: DataFrameBuilder) -> DataFusionPlanBundle:
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

    def _normalize_builder(table: str, *, prefix: str) -> DataFrameBuilder:
        def _builder(inner_ctx: SessionContext) -> DataFrame:
            return SemanticCompiler(inner_ctx, config=config).normalize(table, prefix=prefix)

        return _builder

    def _relate_builder(
        left: str,
        right: str,
        *,
        join_type: Literal["overlap", "contains"],
        filter_sql: str | None,
        origin: str,
    ) -> DataFrameBuilder:
        def _builder(inner_ctx: SessionContext) -> DataFrame:
            return SemanticCompiler(inner_ctx, config=config).relate(
                left,
                right,
                join_type=join_type,
                filter_sql=filter_sql,
                origin=origin,
            )

        return _builder

    def _union_nodes_builder(names: list[str]) -> DataFrameBuilder:
        def _builder(inner_ctx: SessionContext) -> DataFrame:
            return SemanticCompiler(inner_ctx, config=config).union_nodes(
                names, discriminator="node_kind"
            )

        return _builder

    def _union_edges_builder(names: list[str]) -> DataFrameBuilder:
        def _builder(inner_ctx: SessionContext) -> DataFrame:
            return SemanticCompiler(inner_ctx, config=config).union_edges(
                names, discriminator="edge_kind"
            )

        return _builder

    def _scip_norm_builder(table: str) -> DataFrameBuilder:
        def _builder(inner_ctx: SessionContext) -> DataFrame:
            from semantics.scip_normalize import scip_to_byte_offsets

            return scip_to_byte_offsets(inner_ctx, occurrences_table=table)

        return _builder

    nodes: list[ViewNode] = []

    def _input(name: str) -> str:
        return _cdf_input_name(name, cdf_inputs)

    view_specs: list[tuple[str, DataFrameBuilder]] = [
        ("scip_occurrences_norm", _scip_norm_builder(_input("scip_occurrences"))),
        ("cst_refs_norm", _normalize_builder(_input("cst_refs"), prefix="ref")),
        ("cst_defs_norm", _normalize_builder(_input("cst_defs"), prefix="def")),
        ("cst_imports_norm", _normalize_builder(_input("cst_imports"), prefix="import")),
        ("cst_calls_norm", _normalize_builder(_input("cst_callsites"), prefix="call")),
        (
            "rel_name_symbol",
            _relate_builder(
                "cst_refs_norm",
                "scip_occurrences_norm",
                join_type="overlap",
                filter_sql="is_read = true",
                origin="cst_ref",
            ),
        ),
        (
            "rel_def_symbol",
            _relate_builder(
                "cst_defs_norm",
                "scip_occurrences_norm",
                join_type="contains",
                filter_sql="is_definition = true",
                origin="cst_def",
            ),
        ),
        (
            "rel_import_symbol",
            _relate_builder(
                "cst_imports_norm",
                "scip_occurrences_norm",
                join_type="overlap",
                filter_sql="is_import = true",
                origin="cst_import",
            ),
        ),
        (
            "rel_call_symbol",
            _relate_builder(
                "cst_calls_norm",
                "scip_occurrences_norm",
                join_type="overlap",
                filter_sql=None,
                origin="cst_call",
            ),
        ),
        (
            "cpg_edges",
            _union_edges_builder(
                ["rel_name_symbol", "rel_def_symbol", "rel_import_symbol", "rel_call_symbol"],
            ),
        ),
        (
            "cpg_nodes",
            _union_nodes_builder(
                ["cst_refs_norm", "cst_defs_norm", "cst_imports_norm", "cst_calls_norm"],
            ),
        ),
    ]

    for name, builder in view_specs:
        nodes.append(
            ViewNode(
                name=name,
                deps=(),
                builder=builder,
                plan_bundle=_bundle(builder),
                cache_policy=_cache_policy_for(name, cache_policy),
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

    Registers these views:
    - scip_occurrences_norm
    - cst_refs_norm, cst_defs_norm, cst_imports_norm, cst_calls_norm
    - rel_name_symbol, rel_def_symbol, rel_import_symbol, rel_call_symbol
    - cpg_nodes, cpg_edges

    Parameters
    ----------
    ctx
        DataFusion session context with extraction tables registered.
    runtime_profile
        Runtime configuration for building cached view graphs (required).
    options
        Optional build settings for cache policy, schema validation, and CDF inputs.
    """
    resolved = options or CpgBuildOptions()
    if runtime_profile is None:
        msg = "build_cpg requires a runtime_profile for view-graph execution."
        raise ValueError(msg)
    resolved_cdf = _resolve_cdf_inputs(
        ctx,
        runtime_profile=runtime_profile,
        use_cdf=resolved.use_cdf,
        cdf_inputs=resolved.cdf_inputs,
        inputs=CPG_INPUT_TABLES,
    )

    from datafusion_engine.udf.runtime import rust_udf_snapshot
    from datafusion_engine.views.graph import (
        ViewGraphOptions,
        ViewGraphRuntimeOptions,
        register_view_graph,
    )

    snapshot = rust_udf_snapshot(ctx)
    nodes = _view_nodes_for_cpg(
        ctx,
        runtime_profile=runtime_profile,
        cache_policy=resolved.cache_policy,
        config=resolved.config,
        cdf_inputs=resolved_cdf,
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
    build_cpg(ctx, runtime_profile=runtime_profile, options=resolved)

    deps: dict[str, object] = {}
    resolved_cdf = _resolve_cdf_inputs(
        ctx,
        runtime_profile=runtime_profile,
        use_cdf=resolved.use_cdf,
        cdf_inputs=resolved.cdf_inputs,
        inputs=CPG_INPUT_TABLES,
    )
    nodes = _view_nodes_for_cpg(
        ctx,
        runtime_profile=runtime_profile,
        cache_policy=resolved.cache_policy,
        config=resolved.config,
        cdf_inputs=resolved_cdf,
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


def _resolve_cdf_inputs(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    use_cdf: bool | None,
    cdf_inputs: Mapping[str, str] | None,
    inputs: Sequence[str],
) -> Mapping[str, str] | None:
    if cdf_inputs is not None:
        return dict(cdf_inputs)
    if runtime_profile is None:
        if use_cdf:
            msg = "CDF input registration requires a runtime profile."
            raise ValueError(msg)
        return None
    if use_cdf is False:
        return None
    if use_cdf is None and not _has_cdf_inputs(runtime_profile, inputs=inputs):
        return None
    from datafusion_engine.session.runtime import register_cdf_inputs_for_profile

    return register_cdf_inputs_for_profile(runtime_profile, ctx, table_names=inputs)


def _cdf_input_name(name: str, mapping: Mapping[str, str] | None) -> str:
    if mapping is None:
        return name
    return mapping.get(name, name)


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
    "build_cpg",
    "build_cpg_from_inferred_deps",
]
