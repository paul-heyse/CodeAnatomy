"""View node definitions for view-driven normalize/relspec/CPG pipelines."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
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

    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from datafusion_engine.plan.bundle import DataFusionPlanBundle
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile, SessionRuntime
    from schema_spec.system import DatasetSpec
    from semantics.catalog.dataset_rows import SemanticDatasetRow
    from semantics.metrics import SemanticOperationMetrics
    from semantics.plans.fingerprints import PlanFingerprint
    from semantics.stats import ViewStats
    from semantics.types import AnnotatedSchema


DataFrameBuilder = Callable[["SessionContext"], "DataFrame"]


@dataclass(frozen=True)
class ContractBuilderOptions:
    """Configuration for building schema contracts."""

    metadata_spec: SchemaMetadataSpec | None = None
    expected_schema: pa.Schema | None = None
    enforce_columns: bool = False
    annotated_schema: AnnotatedSchema | None = None
    enforce_semantic_types: bool = False


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
    options: ContractBuilderOptions | None = None,
) -> Callable[[pa.Schema], SchemaContract]:
    def _build(schema: pa.Schema) -> SchemaContract:
        resolved_options = options or ContractBuilderOptions()
        base_schema = (
            resolved_options.expected_schema
            if resolved_options.expected_schema is not None
            else schema
        )
        resolved = (
            resolved_options.metadata_spec.apply(base_schema)
            if resolved_options.metadata_spec is not None
            else base_schema
        )
        contract = SchemaContract.from_arrow_schema(
            name,
            resolved,
            enforce_columns=resolved_options.enforce_columns,
        )
        if resolved_options.annotated_schema is None or not resolved_options.enforce_semantic_types:
            return contract
        return replace(
            contract,
            annotated_schema=resolved_options.annotated_schema,
            enforce_semantic_types=True,
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
    cdf_enabled: bool | None = None,
) -> Literal["none", "delta_staging", "delta_output"]:
    """Determine cache policy for a semantic view.

    Parameters
    ----------
    view_name
        Name of the view.
    runtime_profile
        Runtime configuration for dataset location lookup.
    cdf_enabled
        Whether CDF is enabled for this dataset. When provided, influences
        the cache policy selection for incremental processing support.

    Returns
    -------
    Literal["none", "delta_staging", "delta_output"]
        Cache policy for the view.
    """
    # Final output views get delta_output when location is configured
    if view_name in {"cpg_nodes_v1", "cpg_edges_v1", "relation_output_v1"}:
        if runtime_profile is not None and runtime_profile.dataset_location(view_name) is not None:
            return "delta_output"
        return "delta_staging"

    # CDF-enabled views benefit from delta_staging for incremental reads
    if cdf_enabled:
        return "delta_staging"

    # Relationship and normalization views use delta_staging
    if view_name.startswith("rel_") or view_name.endswith("_norm_v1"):
        return "delta_staging"
    return "none"


# Import projection types and functions from semantic catalog
from semantics.catalog.projections import (
    SemanticProjectionConfig,
    relation_output_builder,
    semantic_projection_options,
    semantic_view_specs,
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
    from semantics.catalog.dataset_rows import get_all_dataset_rows
    from semantics.catalog.view_builders import view_builders
    from semantics.input_registry import validate_semantic_inputs

    if runtime_profile is None:
        msg = "Runtime profile is required for normalize view planning."
        raise ValueError(msg)
    validation = validate_semantic_inputs(_ctx)
    input_mapping = validation.resolved_names
    builders = view_builders(input_mapping=input_mapping, config=None)
    semantic_view_names = _semantic_view_name_set()

    nodes: list[ViewNode] = []
    for row in get_all_dataset_rows():
        if not row.register_view or row.name in semantic_view_names:
            continue
        node = _normalize_view_node(
            row,
            builders=builders,
            snapshot=snapshot,
            runtime_profile=runtime_profile,
        )
        if node is not None:
            nodes.append(node)
    return nodes


def _semantic_view_name_set() -> set[str]:
    from relspec.view_defs import RELATION_OUTPUT_NAME
    from semantics.naming import SEMANTIC_VIEW_NAMES

    semantic_view_names = set(SEMANTIC_VIEW_NAMES)
    semantic_view_names.add(RELATION_OUTPUT_NAME)
    return semantic_view_names


def _normalize_view_node(
    row: SemanticDatasetRow,
    *,
    builders: Mapping[str, DataFrameBuilder],
    snapshot: Mapping[str, object],
    runtime_profile: DataFusionRuntimeProfile,
) -> ViewNode:
    from semantics.catalog.dataset_specs import dataset_contract_schema, dataset_spec
    from semantics.catalog.view_builders import VIEW_BUNDLE_BUILDERS

    builder = builders.get(row.name)
    if builder is None:
        msg = f"Missing DataFusion builder for normalize view {row.name!r}."
        raise KeyError(msg)
    bundle_builder = VIEW_BUNDLE_BUILDERS.get(row.name)
    if bundle_builder is None:
        msg = f"Missing DataFusion bundle builder for normalize view {row.name!r}."
        raise KeyError(msg)
    bundle = bundle_builder(runtime_profile.session_runtime())
    deps, required = _deps_and_udfs_from_bundle(
        bundle,
        snapshot,
        label=row.name,
    )
    metadata = _metadata_with_required_udfs(
        _metadata_from_dataset_spec(dataset_spec(row.name)),
        required,
    )
    expected_schema = _arrow_schema_from_contract(dataset_contract_schema(row.name))
    annotated_contract: AnnotatedSchema | None = None
    if expected_schema is not None:
        from semantics.types import AnnotatedSchema

        annotated_contract = AnnotatedSchema.from_arrow_schema(expected_schema)
    return ViewNode(
        name=row.name,
        deps=deps,
        builder=builder,
        contract_builder=_contract_builder(
            row.name,
            options=ContractBuilderOptions(
                metadata_spec=metadata,
                expected_schema=expected_schema,
                enforce_columns=True,
                annotated_schema=annotated_contract,
                enforce_semantic_types=annotated_contract is not None,
            ),
        ),
        required_udfs=required,
        plan_bundle=bundle,
        cache_policy="delta_output",
    )


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
                contract_builder=_contract_builder(
                    name,
                    options=ContractBuilderOptions(metadata_spec=metadata),
                ),
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
class SemanticRegistryRequest:
    """Inputs required to register semantic view nodes."""

    ctx: SessionContext
    snapshot: Mapping[str, object]
    runtime_profile: DataFusionRuntimeProfile
    view_specs: list[tuple[str, DataFrameBuilder]]
    dataset_specs: Mapping[str, DatasetSpec]
    relation_output_name: str


def _semantic_dataset_specs() -> dict[str, DatasetSpec]:
    from semantics.catalog.dataset_specs import dataset_specs

    return {spec.name: spec for spec in dataset_specs()}


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


@dataclass(frozen=True)
class _CatalogBuilder:
    name: str
    evidence_tier: int
    upstream_deps: tuple[str, ...]
    builder: DataFrameBuilder

    def build(self, ctx: SessionContext) -> DataFrame:
        return self.builder(ctx)


def _register_semantic_nodes(
    request: SemanticRegistryRequest,
) -> tuple[
    dict[str, ViewNode], list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]
]:
    from dataclasses import asdict

    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from semantics.catalog import SEMANTIC_CATALOG

    adapter = DataFusionIOAdapter(ctx=request.ctx, profile=request.runtime_profile)
    SEMANTIC_CATALOG.clear()

    nodes_by_name: dict[str, ViewNode] = {}
    payloads = SemanticPayloads(metrics=[], fingerprints=[], stats=[])

    for name, builder in request.view_specs:
        node, metrics_payload, fingerprint_payload, stats_payload = _register_semantic_view(
            request=request,
            adapter=adapter,
            name=name,
            builder=builder,
        )
        nodes_by_name[name] = node
        payloads.metrics.append(asdict(metrics_payload))
        payloads.fingerprints.append(fingerprint_payload)
        payloads.stats.append(stats_payload)

    return nodes_by_name, payloads.metrics, payloads.fingerprints, payloads.stats


def _register_semantic_view(
    *,
    request: SemanticRegistryRequest,
    adapter: DataFusionIOAdapter,
    name: str,
    builder: DataFrameBuilder,
) -> tuple[ViewNode, SemanticOperationMetrics, dict[str, object], dict[str, object]]:
    from semantics.catalog import SEMANTIC_CATALOG
    from semantics.metrics import SemanticOperationMetrics

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
    annotated_contract: AnnotatedSchema | None = None
    if expected_schema is not None:
        from semantics.types import AnnotatedSchema

        annotated_contract = AnnotatedSchema.from_arrow_schema(expected_schema)

    cdf_enabled = _cdf_enabled_from_spec(request.dataset_specs.get(name))
    cache_policy = _semantic_cache_policy(
        name,
        runtime_profile=request.runtime_profile,
        cdf_enabled=cdf_enabled,
    )

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
    fingerprint_payload: dict[str, object] = {
        "view_name": name,
        "logical_plan_hash": fingerprint.logical_plan_hash,
        "substrait_hash": fingerprint.substrait_hash,
        "schema_hash": fingerprint.schema_hash,
    }

    # Register temporary view so downstream semantic builders can resolve dependencies.
    adapter.register_view(name, bundle.df, overwrite=True, temporary=True)

    node = ViewNode(
        name=name,
        deps=deps,
        builder=builder,
        contract_builder=_contract_builder(
            name,
            options=ContractBuilderOptions(
                metadata_spec=metadata,
                expected_schema=expected_schema,
                enforce_columns=enforce_columns,
                annotated_schema=annotated_contract,
                enforce_semantic_types=annotated_contract is not None,
            ),
        ),
        required_udfs=required,
        plan_bundle=bundle,
        cache_policy=cache_policy,
    )

    return node, metrics, fingerprint_payload, stats.as_dict()


def _cdf_enabled_from_spec(spec: DatasetSpec | None) -> bool | None:
    if spec is None or spec.delta_cdf_policy is None:
        return None
    return spec.delta_cdf_policy.required


def _record_semantic_artifacts(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    metrics_payloads: list[dict[str, object]],
    fingerprint_payloads: list[dict[str, object]],
    stats_payloads: list[dict[str, object]],
    nodes_by_name: Mapping[str, ViewNode],
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
    if nodes_by_name:
        record_artifact(
            runtime_profile,
            "semantic_lineage_v1",
            {
                "views": {
                    name: {
                        "deps": list(node.deps),
                        "required_udfs": list(node.required_udfs),
                    }
                    for name, node in nodes_by_name.items()
                }
            },
        )
        record_artifact(
            runtime_profile,
            "semantic_cache_policy_v1",
            {"views": {name: node.cache_policy for name, node in nodes_by_name.items()}},
        )


def _validated_semantic_inputs(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> tuple[dict[str, str], bool]:
    from datafusion_engine.lineage.diagnostics import record_artifact
    from semantics.pipeline import _resolve_semantic_input_mapping
    from semantics.validation import validate_semantic_input_columns

    input_mapping, use_cdf = _resolve_semantic_input_mapping(
        ctx,
        runtime_profile=runtime_profile,
        use_cdf=None,
        cdf_inputs=None,
    )
    validation = validate_semantic_input_columns(
        ctx,
        input_mapping=input_mapping,
    )
    if validation.valid:
        return input_mapping, use_cdf
    record_artifact(
        runtime_profile,
        "semantic_input_schema_validation_v1",
        {
            "missing_tables": list(validation.missing_tables),
            "missing_columns": {
                table: list(columns) for table, columns in validation.missing_columns.items()
            },
            "resolved_tables": dict(validation.resolved_tables),
        },
    )
    msg = (
        "Semantic input validation failed. "
        f"Missing tables: {validation.missing_tables!r}. "
        f"Missing columns: {validation.missing_columns!r}."
    )
    raise ValueError(msg)


def _semantic_view_specs_for_registration(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> list[tuple[str, DataFrameBuilder]]:
    from relspec.view_defs import (
        DEFAULT_REL_TASK_PRIORITY,
        REL_CALLSITE_SYMBOL_OUTPUT,
        REL_DEF_SYMBOL_OUTPUT,
        REL_IMPORT_SYMBOL_OUTPUT,
        REL_NAME_SYMBOL_OUTPUT,
        RELATION_OUTPUT_NAME,
    )
    from semantics.naming import canonical_output_name
    from semantics.pipeline import _cpg_view_specs
    from semantics.spec_registry import RELATIONSHIP_SPECS

    input_mapping, use_cdf = _validated_semantic_inputs(
        ctx,
        runtime_profile=runtime_profile,
    )
    base_specs = _cpg_view_specs(
        input_mapping=input_mapping,
        config=None,
        use_cdf=use_cdf,
    )
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
    view_specs, relationship_builders = semantic_view_specs(
        base_specs,
        relationship_specs=RELATIONSHIP_SPECS,
        projection_options=projection_options,
    )
    view_specs.append(
        (
            RELATION_OUTPUT_NAME,
            relation_output_builder(relationship_builders, projection_options),
        )
    )
    canonical_specs: list[tuple[str, DataFrameBuilder]] = []
    seen_names: set[str] = set()
    for name, builder in view_specs:
        canonical = canonical_output_name(name)
        if canonical in seen_names:
            msg = f"Duplicate semantic view name after canonicalization: {canonical!r}."
            raise ValueError(msg)
        seen_names.add(canonical)
        canonical_specs.append((canonical, builder))
    return canonical_specs


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
    from relspec.view_defs import RELATION_OUTPUT_NAME
    from semantics.catalog import SEMANTIC_CATALOG

    if runtime_profile is None:
        msg = "Runtime profile is required for semantic view planning."
        raise ValueError(msg)

    view_specs = _semantic_view_specs_for_registration(
        ctx,
        runtime_profile=runtime_profile,
    )
    dataset_specs = _semantic_dataset_specs()
    nodes_by_name, metrics_payloads, fingerprint_payloads, stats_payloads = (
        _register_semantic_nodes(
            SemanticRegistryRequest(
                ctx=ctx,
                snapshot=snapshot,
                runtime_profile=runtime_profile,
                view_specs=view_specs,
                dataset_specs=dataset_specs,
                relation_output_name=RELATION_OUTPUT_NAME,
            )
        )
    )
    _record_semantic_artifacts(
        runtime_profile,
        metrics_payloads=metrics_payloads,
        fingerprint_payloads=fingerprint_payloads,
        stats_payloads=stats_payloads,
        nodes_by_name=nodes_by_name,
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
                    options=ContractBuilderOptions(
                        metadata_spec=metadata,
                        expected_schema=schema,
                        enforce_columns=True,
                    ),
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
                contract_builder=_contract_builder(
                    alias,
                    options=ContractBuilderOptions(),
                ),
                required_udfs=required,
                plan_bundle=bundle,
            )
        )
    return alias_nodes


def _resolve_alias(name: str) -> str:
    from semantics.catalog.dataset_specs import dataset_alias
    from semantics.naming import SEMANTIC_OUTPUT_ALIASES

    legacy_by_canonical = {
        canonical: legacy for legacy, canonical in SEMANTIC_OUTPUT_ALIASES.items()
    }
    legacy_alias = legacy_by_canonical.get(name)
    if legacy_alias is not None:
        return legacy_alias
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
