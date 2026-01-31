"""View node definitions for view-driven normalize/relspec/CPG pipelines."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Literal

import pyarrow as pa

from arrow_utils.core.ordering import OrderingLevel
from cpg.specs import TaskIdentity
from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.arrow.metadata import (
    SchemaMetadataSpec,
    function_requirements_metadata_spec,
    merge_metadata_specs,
    ordering_metadata_spec,
)
from datafusion_engine.plan.bundle import PlanBundleOptions, build_plan_bundle
from datafusion_engine.schema.contracts import SchemaContract
from datafusion_engine.udf.runtime import validate_rust_udf_snapshot
from datafusion_engine.views.bundle_extraction import (
    arrow_schema_from_df,
    extract_lineage_from_bundle,
    resolve_required_udfs_from_bundle,
)
from datafusion_engine.views.graph import ViewNode

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.plan.bundle import DataFusionPlanBundle
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile, SessionRuntime
    from schema_spec.system import DatasetSpec
    from semantics.adapters import RelationshipProjectionOptions
    from semantics.plans.fingerprints import PlanFingerprint
    from semantics.specs import RelationshipSpec
    from semantics.stats import ViewStats
    from semantics.types import AnnotatedSchema


DataFrameBuilder = Callable[["SessionContext"], "DataFrame"]


def _merge_metadata_specs(specs: Sequence[SchemaMetadataSpec]) -> SchemaMetadataSpec:
    if not specs:
        return SchemaMetadataSpec()
    if len(specs) == 1:
        return specs[0]
    return merge_metadata_specs(*specs)


def _metadata_with_required_udfs(
    base: SchemaMetadataSpec | None,
    required: Sequence[str],
) -> SchemaMetadataSpec | None:
    if base is None and not required:
        return None
    specs: list[SchemaMetadataSpec] = []
    if base is not None:
        specs.append(base)
    if required:
        specs.append(function_requirements_metadata_spec(required=required))
    return _merge_metadata_specs(specs)


def _contract_builder(
    name: str,
    *,
    metadata_spec: SchemaMetadataSpec | None = None,
    expected_schema: pa.Schema | None = None,
    enforce_columns: bool = False,
) -> Callable[[pa.Schema], SchemaContract]:
    def _build(schema: pa.Schema) -> SchemaContract:
        base_schema = expected_schema if expected_schema is not None else schema
        resolved = metadata_spec.apply(base_schema) if metadata_spec is not None else base_schema
        return SchemaContract.from_arrow_schema(
            name,
            resolved,
            enforce_columns=enforce_columns,
        )

    return _build


def _ordering_metadata_for_dataset(spec: DatasetSpec) -> SchemaMetadataSpec | None:
    contract = spec.contract_spec
    table_spec = spec.table_spec
    if contract is not None and contract.canonical_sort:
        keys = tuple((key.column, key.order) for key in contract.canonical_sort)
        return ordering_metadata_spec(OrderingLevel.EXPLICIT, keys=keys)
    if table_spec.key_fields:
        keys = tuple((name, "ascending") for name in table_spec.key_fields)
        return ordering_metadata_spec(OrderingLevel.IMPLICIT, keys=keys)
    return None


def _metadata_from_dataset_spec(spec: DatasetSpec) -> SchemaMetadataSpec:
    ordering = _ordering_metadata_for_dataset(spec)
    if ordering is None:
        return spec.metadata_spec
    return _merge_metadata_specs([spec.metadata_spec, ordering])


def _semantic_cache_policy(
    view_name: str,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> Literal["none", "delta_staging", "delta_output"]:
    if view_name in {"cpg_nodes_v1", "cpg_edges_v1", "relation_output_v1"}:
        if runtime_profile is not None and runtime_profile.dataset_location(view_name) is not None:
            return "delta_output"
        return "delta_staging"
    if view_name.startswith("rel_") or view_name.endswith("_norm_v1"):
        return "delta_staging"
    return "none"


@dataclass(frozen=True)
class RelationOutputSpec:
    """Parameters for relation_output projection."""

    src_col: str
    dst_col: str
    kind: str
    origin: str
    qname_source_col: str | None = None
    ambiguity_group_col: str | None = None


def _relation_output_from_semantic(
    df: DataFrame,
    spec: RelationOutputSpec,
) -> DataFrame:
    from datafusion import col, lit
    from datafusion import functions as f

    null_str = lit(None).cast(pa.string())
    qname_source = col(spec.qname_source_col) if spec.qname_source_col else null_str
    ambiguity_group = col(spec.ambiguity_group_col) if spec.ambiguity_group_col else null_str

    return df.select(
        col(spec.src_col).alias("src"),
        col(spec.dst_col).alias("dst"),
        col("path").alias("path"),
        f.coalesce(col("edge_owner_file_id"), col("file_id")).alias("edge_owner_file_id"),
        col("bstart").alias("bstart"),
        col("bend").alias("bend"),
        lit(spec.origin).alias("origin"),
        col("resolution_method").alias("resolution_method"),
        col("binding_kind").alias("binding_kind"),
        col("def_site_kind").alias("def_site_kind"),
        col("use_kind").alias("use_kind"),
        lit(spec.kind).alias("kind"),
        col("reason").alias("reason"),
        col("confidence").alias("confidence"),
        col("score").alias("score"),
        col("symbol_roles").alias("symbol_roles"),
        qname_source.alias("qname_source"),
        ambiguity_group.alias("ambiguity_group_id"),
        col("diag_source").alias("diag_source"),
        col("severity").alias("severity"),
        col("task_name").alias("task_name"),
        col("task_priority").alias("task_priority"),
    )


def _bundle_deps_and_udfs(
    ctx: SessionContext,
    builder: DataFrameBuilder,
    snapshot: Mapping[str, object],
    *,
    label: str,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> tuple[DataFusionPlanBundle, tuple[str, ...], tuple[str, ...]]:
    df = builder(ctx)
    if runtime_profile is None:
        msg = f"Runtime profile is required for view planning: {label!r}."
        raise ValueError(msg)
    session_runtime = runtime_profile.session_runtime()
    bundle = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(
            compute_execution_plan=True,
            validate_udfs=True,
            session_runtime=session_runtime,
        ),
    )
    try:
        lineage = extract_lineage_from_bundle(bundle)
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to extract lineage for view {label!r}."
        raise ValueError(msg) from exc
    deps = lineage.referenced_tables
    required = resolve_required_udfs_from_bundle(bundle, snapshot=snapshot)
    return bundle, deps, required


def _deps_and_udfs_from_bundle(
    bundle: DataFusionPlanBundle,
    snapshot: Mapping[str, object],
    *,
    label: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    try:
        lineage = extract_lineage_from_bundle(bundle)
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to extract lineage for view {label!r}."
        raise ValueError(msg) from exc
    deps = lineage.referenced_tables
    required = resolve_required_udfs_from_bundle(bundle, snapshot=snapshot)
    return deps, required


def _arrow_schema_from_contract(schema: SchemaLike) -> pa.Schema:
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Failed to resolve contract schema."
    raise TypeError(msg)


def view_graph_nodes(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    runtime_profile: DataFusionRuntimeProfile | None = None,
    stage: Literal["all", "pre_cpg", "cpg"] = "all",
) -> tuple[ViewNode, ...]:
    """Return view graph nodes for normalize + relspec + CPG outputs.

    Raises
    ------
    ValueError
        Raised when an unsupported view graph stage is requested.

    Returns
    -------
    tuple[ViewNode, ...]
        View nodes for the view-driven pipeline.
    """
    if stage not in {"all", "pre_cpg", "cpg"}:
        msg = f"Unsupported view graph stage: {stage!r}."
        raise ValueError(msg)
    validate_rust_udf_snapshot(snapshot)
    nodes: list[ViewNode] = []
    if stage in {"all", "pre_cpg"}:
        nodes.extend(
            _normalize_view_nodes(
                ctx,
                snapshot=snapshot,
                runtime_profile=runtime_profile,
            )
        )
        nodes.extend(
            _symtable_view_nodes(
                ctx,
                snapshot=snapshot,
                runtime_profile=runtime_profile,
            )
        )
        nodes.extend(
            _semantics_view_nodes(
                ctx,
                snapshot=snapshot,
                runtime_profile=runtime_profile,
            )
        )
    if stage in {"all", "cpg"}:
        nodes.extend(
            _cpg_view_nodes(
                ctx,
                snapshot=snapshot,
                runtime_profile=runtime_profile,
            )
        )
    nodes.extend(
        _alias_nodes(
            nodes,
            ctx=ctx,
            snapshot=snapshot,
            runtime_profile=runtime_profile,
        )
    )
    return tuple(nodes)


def _normalize_view_nodes(
    _ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> list[ViewNode]:
    from normalize.dataset_rows import DATASET_ROWS
    from normalize.dataset_specs import dataset_contract_schema, dataset_spec
    from normalize.df_view_builders import VIEW_BUILDERS, VIEW_BUNDLE_BUILDERS

    nodes: list[ViewNode] = []
    for row in DATASET_ROWS:
        if not row.register_view:
            continue
        builder = VIEW_BUILDERS.get(row.name)
        if builder is None:
            msg = f"Missing DataFusion builder for normalize view {row.name!r}."
            raise KeyError(msg)
        bundle_builder = VIEW_BUNDLE_BUILDERS.get(row.name)
        if bundle_builder is None:
            msg = f"Missing DataFusion bundle builder for normalize view {row.name!r}."
            raise KeyError(msg)
        if runtime_profile is None:
            msg = f"Runtime profile is required for view planning: {row.name!r}."
            raise ValueError(msg)
        bundle = bundle_builder(runtime_profile.session_runtime())
        dataset = dataset_spec(row.name)
        metadata = _metadata_from_dataset_spec(dataset)
        expected_schema = _arrow_schema_from_contract(dataset_contract_schema(row.name))
        deps, required = _deps_and_udfs_from_bundle(
            bundle,
            snapshot,
            label=row.name,
        )
        metadata = _metadata_with_required_udfs(metadata, required)
        nodes.append(
            ViewNode(
                name=row.name,
                deps=deps,
                builder=builder,
                contract_builder=_contract_builder(
                    row.name,
                    metadata_spec=metadata,
                    expected_schema=expected_schema,
                    enforce_columns=True,
                ),
                required_udfs=required,
                plan_bundle=bundle,
                cache_policy="delta_output",
            )
        )
    return nodes


def _symtable_view_nodes(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> list[ViewNode]:
    from datafusion_engine.symtable.views import (
        symtable_binding_resolutions_df,
        symtable_bindings_df,
        symtable_def_sites_df,
        symtable_type_param_edges_df,
        symtable_type_params_df,
        symtable_use_sites_df,
    )

    view_specs: tuple[tuple[str, DataFrameBuilder], ...] = (
        ("symtable_bindings", symtable_bindings_df),
        ("symtable_def_sites", symtable_def_sites_df),
        ("symtable_use_sites", symtable_use_sites_df),
        ("symtable_type_params", symtable_type_params_df),
        ("symtable_type_param_edges", symtable_type_param_edges_df),
        ("symtable_binding_resolutions", symtable_binding_resolutions_df),
    )

    nodes: list[ViewNode] = []
    for name, builder in view_specs:
        bundle, deps, required = _bundle_deps_and_udfs(
            ctx,
            builder,
            snapshot,
            label=name,
            runtime_profile=runtime_profile,
        )
        metadata = _metadata_with_required_udfs(None, required)
        nodes.append(
            ViewNode(
                name=name,
                deps=deps,
                builder=builder,
                contract_builder=_contract_builder(name, metadata_spec=metadata),
                required_udfs=required,
                plan_bundle=bundle,
            )
        )
    return nodes


def _semantic_evidence_tier(view_name: str, *, relation_output_name: str) -> int:
    if view_name.endswith("_norm_v1"):
        return 1
    if view_name.startswith("rel_"):
        return 2
    if view_name.startswith("cpg_") or view_name == relation_output_name:
        return 3
    return 2


@dataclass(frozen=True)
class SemanticProjectionConfig:
    """Configuration for semantic relationship projection options."""

    default_priority: int
    rel_name_output: str
    rel_import_output: str
    rel_def_output: str
    rel_call_output: str
    relationship_specs: Sequence[RelationshipSpec]


@dataclass(frozen=True)
class SemanticRegistryRequest:
    """Inputs required to register semantic view nodes."""

    ctx: SessionContext
    snapshot: Mapping[str, object]
    runtime_profile: DataFusionRuntimeProfile
    view_specs: list[tuple[str, DataFrameBuilder]]
    dataset_specs: Mapping[str, DatasetSpec]
    relation_output_name: str


def _projected_builder(
    builder: DataFrameBuilder,
    *,
    options: RelationshipProjectionOptions,
) -> DataFrameBuilder:
    def _build(inner_ctx: SessionContext) -> DataFrame:
        from semantics.adapters import project_semantic_to_legacy

        return project_semantic_to_legacy(builder(inner_ctx), options=options)

    return _build


def _semantic_projection_options(
    config: SemanticProjectionConfig,
) -> dict[str, RelationshipProjectionOptions]:
    from cpg.kind_catalog import (
        EDGE_KIND_PY_CALLS_SYMBOL,
        EDGE_KIND_PY_DEFINES_SYMBOL,
        EDGE_KIND_PY_IMPORTS_SYMBOL,
        EDGE_KIND_PY_REFERENCES_SYMBOL,
    )
    from semantics.adapters import RelationshipProjectionOptions

    projection_map: dict[str, RelationshipProjectionOptions] = {
        config.rel_name_output: RelationshipProjectionOptions(
            entity_id_alias="ref_id",
            edge_kind=str(EDGE_KIND_PY_REFERENCES_SYMBOL),
            task_name="rel.name_symbol",
            task_priority=config.default_priority,
        ),
        config.rel_import_output: RelationshipProjectionOptions(
            entity_id_alias="import_alias_id",
            edge_kind=str(EDGE_KIND_PY_IMPORTS_SYMBOL),
            task_name="rel.import_symbol",
            task_priority=config.default_priority,
        ),
        config.rel_def_output: RelationshipProjectionOptions(
            entity_id_alias="def_id",
            edge_kind=str(EDGE_KIND_PY_DEFINES_SYMBOL),
            task_name="rel.def_symbol",
            task_priority=config.default_priority,
        ),
        config.rel_call_output: RelationshipProjectionOptions(
            entity_id_alias="call_id",
            edge_kind=str(EDGE_KIND_PY_CALLS_SYMBOL),
            task_name="rel.callsite_symbol",
            task_priority=config.default_priority,
        ),
    }
    required = {spec.name for spec in config.relationship_specs}
    missing = required - set(projection_map)
    if missing:
        msg = f"Missing relationship projection metadata for: {sorted(missing)!r}."
        raise KeyError(msg)
    return projection_map


def _semantic_view_specs(
    base_specs: list[tuple[str, DataFrameBuilder]],
    *,
    relationship_specs: Sequence[RelationshipSpec],
    projection_options: Mapping[str, RelationshipProjectionOptions],
) -> tuple[list[tuple[str, DataFrameBuilder]], dict[str, DataFrameBuilder]]:
    relationship_names = {spec.name for spec in relationship_specs}
    relationship_builders: dict[str, DataFrameBuilder] = {
        name: builder for name, builder in base_specs if name in relationship_names
    }
    view_specs: list[tuple[str, DataFrameBuilder]] = []
    for name, builder in base_specs:
        if name in relationship_names:
            view_specs.append((name, _projected_builder(builder, options=projection_options[name])))
        else:
            view_specs.append((name, builder))
    return view_specs, relationship_builders


def _relation_output_builder(
    relationship_builders: Mapping[str, DataFrameBuilder],
    projection_options: Mapping[str, RelationshipProjectionOptions],
) -> DataFrameBuilder:
    def _build(inner_ctx: SessionContext) -> DataFrame:
        from dataclasses import replace

        from semantics.adapters import project_semantic_to_legacy

        frames: list[DataFrame] = []
        for rel_name in sorted(relationship_builders):
            builder = relationship_builders[rel_name]
            options = projection_options[rel_name]
            extended_opts = replace(options, include_extended_columns=True)
            rel_df = project_semantic_to_legacy(builder(inner_ctx), options=extended_opts)
            output_spec = RelationOutputSpec(
                src_col=options.entity_id_alias,
                dst_col="symbol",
                kind=str(options.edge_kind),
                origin="cst",
            )
            frames.append(_relation_output_from_semantic(rel_df, output_spec))
        if not frames:
            msg = "Semantic relationship builders did not produce any relation outputs."
            raise ValueError(msg)
        result = frames[0]
        for frame in frames[1:]:
            result = result.union(frame)
        return result

    return _build


def _semantic_dataset_specs() -> dict[str, DatasetSpec]:
    from relspec.view_defs import RELATION_OUTPUT_NAME
    from schema_spec.relationship_specs import (
        relation_output_spec,
        relationship_dataset_specs,
    )

    specs = {spec.name: spec for spec in relationship_dataset_specs()}
    specs[RELATION_OUTPUT_NAME] = relation_output_spec()
    return specs


def _dataset_contract_for(
    name: str,
    *,
    dataset_specs: Mapping[str, DatasetSpec],
) -> tuple[pa.Schema | None, bool]:
    dataset_spec = dataset_specs.get(name)
    if dataset_spec is None:
        return None, False
    return _arrow_schema_from_contract(dataset_spec.schema()), True


def _semantic_fingerprint_stats(
    ctx: SessionContext,
    *,
    df: DataFrame,
    view_name: str,
) -> tuple[PlanFingerprint, AnnotatedSchema, ViewStats]:
    from semantics.plans import compute_plan_fingerprint
    from semantics.stats import collect_view_stats
    from semantics.types import AnnotatedSchema

    fingerprint = compute_plan_fingerprint(df, view_name=view_name, ctx=ctx)
    annotated_schema = AnnotatedSchema.from_dataframe(df)
    stats = collect_view_stats(
        df,
        view_name=view_name,
        execute_for_exact=False,
        schema_fingerprint=fingerprint.schema_hash,
    )
    return fingerprint, annotated_schema, stats


@dataclass
class SemanticPayloads:
    """Collected payloads for semantic view artifacts."""

    metrics: list[dict[str, object]]
    fingerprints: list[dict[str, object]]
    stats: list[dict[str, object]]


def _register_semantic_nodes(
    request: SemanticRegistryRequest,
) -> tuple[dict[str, ViewNode], list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    from dataclasses import asdict

    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from semantics.catalog import SEMANTIC_CATALOG
    from semantics.metrics import SemanticOperationMetrics

    adapter = DataFusionIOAdapter(ctx=request.ctx, profile=request.runtime_profile)
    SEMANTIC_CATALOG.clear()

    nodes_by_name: dict[str, ViewNode] = {}
    payloads = SemanticPayloads(metrics=[], fingerprints=[], stats=[])

    @dataclass(frozen=True)
    class _CatalogBuilder:
        name: str
        evidence_tier: int
        upstream_deps: tuple[str, ...]
        builder: DataFrameBuilder

        def build(self, ctx: SessionContext) -> DataFrame:
            return self.builder(ctx)

    for name, builder in request.view_specs:
        bundle, deps, required = _bundle_deps_and_udfs(
            request.ctx,
            builder,
            request.snapshot,
            label=name,
            runtime_profile=request.runtime_profile,
        )
        metadata = _metadata_with_required_udfs(None, required)
        expected_schema, enforce_columns = _dataset_contract_for(
            name,
            dataset_specs=request.dataset_specs,
        )

        cache_policy = _semantic_cache_policy(name, runtime_profile=request.runtime_profile)

        fingerprint, annotated_schema, stats = _semantic_fingerprint_stats(
            request.ctx,
            df=bundle.df,
            view_name=name,
        )
        SEMANTIC_CATALOG.register(
            _CatalogBuilder(
                name=name,
                evidence_tier=_semantic_evidence_tier(
                    name,
                    relation_output_name=request.relation_output_name,
                ),
                upstream_deps=deps,
                builder=builder,
            ),
            overwrite=True,
            plan_fingerprint=fingerprint,
            annotated_schema=annotated_schema,
        )
        SEMANTIC_CATALOG.update_stats(name, stats)

        metrics = SemanticOperationMetrics(
            operation="semantic_view",
            input_table="",
            output_name=name,
        ).with_plan_fingerprint(fingerprint.logical_plan_hash)
        payloads.metrics.append(asdict(metrics))
        payloads.fingerprints.append(
            {
                "view_name": name,
                "logical_plan_hash": fingerprint.logical_plan_hash,
                "substrait_hash": fingerprint.substrait_hash,
                "schema_hash": fingerprint.schema_hash,
            }
        )
        payloads.stats.append(stats.as_dict())

        # Register temporary view so downstream semantic builders can resolve dependencies.
        adapter.register_view(name, bundle.df, overwrite=True, temporary=True)

        nodes_by_name[name] = ViewNode(
            name=name,
            deps=deps,
            builder=builder,
            contract_builder=_contract_builder(
                name,
                metadata_spec=metadata,
                expected_schema=expected_schema,
                enforce_columns=enforce_columns,
            ),
            required_udfs=required,
            plan_bundle=bundle,
            cache_policy=cache_policy,
        )

    return nodes_by_name, payloads.metrics, payloads.fingerprints, payloads.stats


def _record_semantic_artifacts(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    metrics_payloads: list[dict[str, object]],
    fingerprint_payloads: list[dict[str, object]],
    stats_payloads: list[dict[str, object]],
) -> None:
    from datafusion_engine.lineage.diagnostics import record_artifact

    if metrics_payloads:
        record_artifact(
            runtime_profile,
            "semantic_pipeline_metrics_v1",
            {"views": metrics_payloads},
        )
    if fingerprint_payloads:
        record_artifact(
            runtime_profile,
            "semantic_plan_fingerprints_v1",
            {"views": fingerprint_payloads},
        )
    if stats_payloads:
        record_artifact(
            runtime_profile,
            "semantic_view_stats_v1",
            {"views": stats_payloads},
        )


def _semantics_view_nodes(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> list[ViewNode]:
    """Build semantic pipeline view nodes with inferred dependencies.

    **SOLE REGISTRATION POINT FOR SEMANTIC VIEWS**

    This function is the single source of truth for semantic view registration.
    Hamilton nodes consume these views via ``source()`` inputs but do NOT
    re-register them. The execution authority pattern is:

    - View Graph (this module): Registers views via ``view_graph_nodes()``
    - Hamilton Pipeline: Consumes registered views, never duplicates registration

    Integrates semantic CPG builders from semantics.pipeline into the
    view graph infrastructure. Dependencies and required UDFs are
    extracted from DataFusion plan bundles via lineage analysis.

    Parameters
    ----------
    ctx
        DataFusion session context.
    snapshot
        Rust UDF snapshot for UDF validation.
    runtime_profile
        Runtime configuration for building plan bundles.

    Returns
    -------
    list[ViewNode]
        Semantic view nodes with proper plan bundles and dependencies.

    Raises
    ------
    ValueError
        Raised when required runtime context is unavailable.

    See Also
    --------
    semantics.pipeline._cpg_view_specs : Source of semantic view definitions.
    hamilton_pipeline.modules.subdags : Hamilton consumer of semantic outputs.
    """
    from relspec.view_defs import (
        DEFAULT_REL_TASK_PRIORITY,
        REL_CALLSITE_SYMBOL_OUTPUT,
        REL_DEF_SYMBOL_OUTPUT,
        REL_IMPORT_SYMBOL_OUTPUT,
        REL_NAME_SYMBOL_OUTPUT,
        RELATION_OUTPUT_NAME,
    )
    from semantics.catalog import SEMANTIC_CATALOG
    from semantics.pipeline import _cpg_view_specs, _resolve_semantic_input_mapping
    from semantics.spec_registry import RELATIONSHIP_SPECS

    if runtime_profile is None:
        msg = "Runtime profile is required for semantic view planning."
        raise ValueError(msg)

    input_mapping, use_cdf = _resolve_semantic_input_mapping(
        ctx,
        runtime_profile=runtime_profile,
        use_cdf=None,
        cdf_inputs=None,
    )
    base_specs = _cpg_view_specs(
        input_mapping=input_mapping,
        config=None,
        use_cdf=use_cdf,
    )
    projection_options = _semantic_projection_options(
        SemanticProjectionConfig(
            default_priority=DEFAULT_REL_TASK_PRIORITY,
            rel_name_output=REL_NAME_SYMBOL_OUTPUT,
            rel_import_output=REL_IMPORT_SYMBOL_OUTPUT,
            rel_def_output=REL_DEF_SYMBOL_OUTPUT,
            rel_call_output=REL_CALLSITE_SYMBOL_OUTPUT,
            relationship_specs=RELATIONSHIP_SPECS,
        )
    )
    view_specs, relationship_builders = _semantic_view_specs(
        base_specs,
        relationship_specs=RELATIONSHIP_SPECS,
        projection_options=projection_options,
    )
    view_specs.append(
        (
            RELATION_OUTPUT_NAME,
            _relation_output_builder(relationship_builders, projection_options),
        )
    )
    dataset_specs = _semantic_dataset_specs()
    nodes_by_name, metrics_payloads, fingerprint_payloads, stats_payloads = _register_semantic_nodes(
        SemanticRegistryRequest(
            ctx=ctx,
            snapshot=snapshot,
            runtime_profile=runtime_profile,
            view_specs=view_specs,
            dataset_specs=dataset_specs,
            relation_output_name=RELATION_OUTPUT_NAME,
        )
    )
    _record_semantic_artifacts(
        runtime_profile,
        metrics_payloads=metrics_payloads,
        fingerprint_payloads=fingerprint_payloads,
        stats_payloads=stats_payloads,
    )

    ordered = SEMANTIC_CATALOG.topological_order()
    return [nodes_by_name[name] for name in ordered if name in nodes_by_name]
def _cpg_view_nodes(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> list[ViewNode]:
    from cpg.view_builders_df import (
        build_cpg_edges_by_dst_df,
        build_cpg_edges_by_src_df,
        build_cpg_props_df,
        build_cpg_props_map_df,
    )

    if runtime_profile is None:
        msg = "Runtime profile is required for CPG view planning."
        raise ValueError(msg)
    session_runtime = runtime_profile.session_runtime()

    def _wrap(builder: Callable[[SessionRuntime], DataFrame]) -> DataFrameBuilder:
        def _build(_ctx: SessionContext) -> DataFrame:
            return builder(session_runtime)

        return _build

    priority = 100
    props_identity = TaskIdentity(name="cpg.props", priority=priority)

    view_specs: tuple[tuple[str, DataFrameBuilder], ...] = (
        ("cpg_props_v1", _wrap(partial(build_cpg_props_df, task_identity=props_identity))),
        ("cpg_props_map_v1", _wrap(build_cpg_props_map_df)),
        ("cpg_edges_by_src_v1", _wrap(build_cpg_edges_by_src_df)),
        ("cpg_edges_by_dst_v1", _wrap(build_cpg_edges_by_dst_df)),
    )

    nodes: list[ViewNode] = []
    for name, builder in view_specs:
        bundle, deps, required = _bundle_deps_and_udfs(
            ctx,
            builder,
            snapshot,
            label=name,
            runtime_profile=runtime_profile,
        )
        metadata = _metadata_with_required_udfs(None, required)
        schema = arrow_schema_from_df(bundle.df)
        cache_policy = "none"
        if name in {"cpg_edges_v1", "cpg_props_v1"}:
            cache_policy = "delta_staging"
        nodes.append(
            ViewNode(
                name=name,
                deps=deps,
                builder=builder,
                contract_builder=_contract_builder(
                    name,
                    metadata_spec=metadata,
                    expected_schema=schema,
                    enforce_columns=True,
                ),
                required_udfs=required,
                plan_bundle=bundle,
                cache_policy=cache_policy,
            )
        )
    return nodes


def _alias_nodes(
    nodes: Sequence[ViewNode],
    *,
    ctx: SessionContext,
    snapshot: Mapping[str, object],
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> list[ViewNode]:
    registered = {node.name for node in nodes}
    alias_nodes: list[ViewNode] = []
    for name in registered:
        alias = _resolve_alias(name)
        if alias == name or alias in registered:
            continue
        builder = _alias_builder(source=name)
        bundle, deps, required = _bundle_deps_and_udfs(
            ctx,
            builder,
            snapshot,
            label=alias,
            runtime_profile=runtime_profile,
        )
        alias_nodes.append(
            ViewNode(
                name=alias,
                deps=deps,
                builder=builder,
                contract_builder=_contract_builder(alias, metadata_spec=None),
                required_udfs=required,
                plan_bundle=bundle,
            )
        )
    return alias_nodes


def _resolve_alias(name: str) -> str:
    from normalize.dataset_specs import dataset_alias

    try:
        return dataset_alias(name)
    except KeyError:
        return _strip_version(name)


def _strip_version(name: str) -> str:
    base, sep, suffix = name.rpartition("_v")
    if sep and suffix.isdigit():
        return base
    return name


def _alias_builder(source: str) -> DataFrameBuilder:
    def _build(ctx: SessionContext) -> DataFrame:
        return ctx.table(source)

    return _build


__all__ = ["view_graph_nodes"]
