"""View node definitions for view-driven normalize/relspec/CPG pipelines."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal

import pyarrow as pa

from arrow_utils.core.ordering import OrderingLevel
from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.arrow.metadata import (
    SchemaMetadataSpec,
    function_requirements_metadata_spec,
    merge_metadata_specs,
    ordering_metadata_spec,
)
from datafusion_engine.plan.bundle import PlanBundleOptions, build_plan_bundle
from datafusion_engine.schema.contracts import SchemaContract
from datafusion_engine.semantics_runtime import semantic_runtime_from_profile
from datafusion_engine.udf.runtime import validate_rust_udf_snapshot
from datafusion_engine.views.bundle_extraction import (
    extract_lineage_from_bundle,
    resolve_required_udfs_from_bundle,
)
from datafusion_engine.views.graph import ViewNode

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from datafusion_engine.plan.bundle import DataFusionPlanBundle
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from schema_spec.system import DatasetSpec
    from semantics.catalog.dataset_rows import SemanticDatasetRow
    from semantics.ir import SemanticIR
    from semantics.runtime import SemanticRuntimeConfig
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


@dataclass(frozen=True)
class SemanticViewNodeContext:
    """Context needed to build a semantic view node."""

    ctx: SessionContext
    snapshot: Mapping[str, object]
    runtime_profile: DataFusionRuntimeProfile
    runtime_config: SemanticRuntimeConfig
    adapter: DataFusionIOAdapter
    dataset_specs: Mapping[str, DatasetSpec]


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


def _semantic_cache_policy_for_row(
    row: SemanticDatasetRow,
    *,
    runtime_config: SemanticRuntimeConfig,
) -> Literal["none", "delta_staging", "delta_output"]:
    """Determine cache policy for a semantic dataset row.

    Cache policy is derived from semantic metadata (CDF support, category,
    merge keys) and runtime output configuration.

    Returns
    -------
    Literal["none", "delta_staging", "delta_output"]
        Cache policy for the semantic dataset row.
    """
    override = runtime_config.cache_policy_overrides.get(row.name)
    if override is not None:
        return override
    if runtime_config.output_path(row.name) is not None:
        return "delta_output"
    if runtime_config.cdf_enabled and row.supports_cdf and row.merge_keys:
        return "delta_staging"
    return "none"


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
    try:
        bundle = build_plan_bundle(
            ctx,
            df,
            options=PlanBundleOptions(
                compute_execution_plan=True,
                validate_udfs=True,
                session_runtime=session_runtime,
            ),
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to build plan bundle for view {label!r}."
        raise ValueError(msg) from exc
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
    semantic_ir: SemanticIR,
) -> tuple[ViewNode, ...]:
    """Return view graph nodes for IR-driven semantic outputs.

    Parameters
    ----------
    ctx
        DataFusion session context.
    snapshot
        Rust UDF snapshot used for required UDF validation.
    runtime_profile
        Runtime profile used for plan bundle compilation.
    semantic_ir
        Compiled semantic IR artifact that defines view ordering and outputs.

    Returns
    -------
    tuple[ViewNode, ...]
        View graph nodes ordered by the semantic IR.

    Raises
    ------
    ValueError
        Raised when the runtime profile is unavailable.
    """
    validate_rust_udf_snapshot(snapshot)
    if runtime_profile is None:
        msg = "Runtime profile is required for semantic view planning."
        raise ValueError(msg)
    runtime_config = semantic_runtime_from_profile(runtime_profile)
    nested_nodes = _nested_view_nodes(
        ctx,
        snapshot=snapshot,
        runtime_profile=runtime_profile,
    )
    semantic_nodes = _semantics_view_nodes(
        ctx,
        snapshot=snapshot,
        runtime_profile=runtime_profile,
        runtime_config=runtime_config,
        semantic_ir=semantic_ir,
    )
    return (*nested_nodes, *semantic_nodes)


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


def _nested_view_nodes(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> list[ViewNode]:
    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from datafusion_engine.schema.registry import extract_nested_schema_for, nested_view_specs

    if runtime_profile is None:
        msg = "Runtime profile is required for nested view planning."
        raise ValueError(msg)

    if not callable(getattr(ctx, "table_exist", None)):
        return []

    view_specs = nested_view_specs(ctx)
    if not view_specs:
        return []

    adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
    nodes: list[ViewNode] = []
    for spec in view_specs:
        if spec.builder is None:
            continue
        bundle, deps, required = _bundle_deps_and_udfs(
            ctx,
            spec.builder,
            snapshot,
            label=spec.name,
            runtime_profile=runtime_profile,
        )
        expected_schema = extract_nested_schema_for(spec.name)
        metadata = _metadata_with_required_udfs(None, required)
        adapter.register_view(spec.name, bundle.df, overwrite=True, temporary=True)
        nodes.append(
            ViewNode(
                name=spec.name,
                deps=deps,
                builder=spec.builder,
                contract_builder=_contract_builder(
                    spec.name,
                    options=ContractBuilderOptions(
                        metadata_spec=metadata,
                        expected_schema=expected_schema,
                        enforce_columns=True,
                    ),
                ),
                required_udfs=required,
                plan_bundle=bundle,
                cache_policy="none",
            )
        )
    return nodes


def _validated_semantic_inputs(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    runtime_config: SemanticRuntimeConfig,
) -> tuple[dict[str, str], bool]:
    from datafusion_engine.lineage.diagnostics import record_artifact
    from semantics.pipeline import _resolve_semantic_input_mapping
    from semantics.validation import (
        SemanticInputValidationError,
        require_semantic_inputs,
    )

    input_mapping, use_cdf = _resolve_semantic_input_mapping(
        ctx,
        runtime_profile=runtime_profile,
        use_cdf=runtime_config.cdf_enabled,
        cdf_inputs=None,
    )
    try:
        validation = require_semantic_inputs(ctx, input_mapping=input_mapping)
    except SemanticInputValidationError as exc:
        validation = exc.validation
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
        raise
    return input_mapping, use_cdf


def _semantic_view_specs_for_registration(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    runtime_config: SemanticRuntimeConfig,
    semantic_ir: SemanticIR,
) -> list[tuple[str, DataFrameBuilder]]:
    from semantics.pipeline import CpgViewSpecsRequest, _cpg_view_specs

    input_mapping, use_cdf = _validated_semantic_inputs(
        ctx,
        runtime_profile=runtime_profile,
        runtime_config=runtime_config,
    )
    return _cpg_view_specs(
        CpgViewSpecsRequest(
            input_mapping=input_mapping,
            config=None,
            use_cdf=use_cdf,
            runtime_profile=runtime_profile,
            requested_outputs=None,
            semantic_ir=semantic_ir,
        )
    )


def _build_semantic_view_node(
    *,
    context: SemanticViewNodeContext,
    name: str,
    builder: DataFrameBuilder,
) -> ViewNode:
    from semantics.catalog.dataset_rows import dataset_row

    bundle, deps, required = _bundle_deps_and_udfs(
        context.ctx,
        builder,
        context.snapshot,
        label=name,
        runtime_profile=context.runtime_profile,
    )
    dataset_spec = context.dataset_specs.get(name)
    metadata_spec = _metadata_from_dataset_spec(dataset_spec) if dataset_spec else None
    metadata = _metadata_with_required_udfs(metadata_spec, required)
    expected_schema, enforce_columns = _dataset_contract_for(
        name,
        dataset_specs=context.dataset_specs,
    )
    annotated_contract: AnnotatedSchema | None = None
    if expected_schema is not None:
        from semantics.types import AnnotatedSchema

        annotated_contract = AnnotatedSchema.from_arrow_schema(expected_schema)

    row = dataset_row(name, strict=False)
    if row is None:
        cache_policy = context.runtime_config.cache_policy_overrides.get(name, "none")
    else:
        cache_policy = _semantic_cache_policy_for_row(
            row,
            runtime_config=context.runtime_config,
        )

    context.adapter.register_view(name, bundle.df, overwrite=True, temporary=True)

    return ViewNode(
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


def _semantics_view_nodes(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    runtime_profile: DataFusionRuntimeProfile | None = None,
    runtime_config: SemanticRuntimeConfig | None = None,
    semantic_ir: SemanticIR,
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
    runtime_config
        Semantic runtime configuration for cache policy and output locations.
    semantic_ir
        Compiled semantic IR artifact.

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
    from datafusion_engine.io.adapter import DataFusionIOAdapter

    if runtime_profile is None or runtime_config is None:
        msg = "Runtime profile is required for semantic view planning."
        raise ValueError(msg)

    view_specs = _semantic_view_specs_for_registration(
        ctx,
        runtime_profile=runtime_profile,
        runtime_config=runtime_config,
        semantic_ir=semantic_ir,
    )
    dataset_specs = _semantic_dataset_specs()
    adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
    context = SemanticViewNodeContext(
        ctx=ctx,
        snapshot=snapshot,
        runtime_profile=runtime_profile,
        runtime_config=runtime_config,
        adapter=adapter,
        dataset_specs=dataset_specs,
    )

    return [
        _build_semantic_view_node(
            context=context,
            name=name,
            builder=builder,
        )
        for name, builder in view_specs
    ]


__all__ = ["view_graph_nodes"]
