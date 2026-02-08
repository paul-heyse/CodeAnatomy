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
from datafusion_engine.udf.runtime import validate_rust_udf_snapshot
from datafusion_engine.views.bundle_extraction import (
    extract_lineage_from_bundle,
    resolve_required_udfs_from_bundle,
)
from datafusion_engine.views.graph import ViewNode
from serde_artifact_specs import SCHEMA_DIVERGENCE_SPEC, SEMANTIC_INPUT_SCHEMA_VALIDATION_SPEC
from utils.env_utils import env_bool

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from datafusion_engine.plan.bundle import DataFusionPlanBundle
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from schema_spec.system import DatasetSpec
    from semantics.catalog.dataset_rows import SemanticDatasetRow
    from semantics.ir import SemanticIR
    from semantics.program_manifest import SemanticProgramManifest
    from semantics.types import AnnotatedSchema


DataFrameBuilder = Callable[["SessionContext"], "DataFrame"]
_SCHEMA_DIVERGENCE_STRICT_ENV = "CODEANATOMY_SCHEMA_DIVERGENCE_STRICT"
_CI_ENV = "CI"


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
    semantic_manifest: SemanticProgramManifest
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
    runtime_profile: DataFusionRuntimeProfile,
    manifest: SemanticProgramManifest,
) -> Literal["none", "delta_staging", "delta_output"]:
    """Determine cache policy for a semantic dataset row.

    Cache policy is derived from semantic metadata (CDF support, category,
    merge keys) and runtime output configuration.

    Returns:
    -------
    Literal["none", "delta_staging", "delta_output"]
        Cache policy for the semantic dataset row.
    """
    override = runtime_profile.data_sources.semantic_output.cache_overrides.get(row.name)
    if override is not None:
        if override in {"none", "delta_staging", "delta_output"}:
            return override
        return "none"
    if manifest.dataset_bindings.locations.get(row.name) is not None:
        return "delta_output"
    if runtime_profile.features.enable_delta_cdf and row.supports_cdf and row.merge_keys:
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
    manifest: SemanticProgramManifest,
) -> tuple[ViewNode, ...]:
    """Return view graph nodes for IR-driven semantic outputs.

    Args:
        ctx: Description.
        snapshot: Description.
        runtime_profile: Description.
        semantic_ir: Description.
        manifest: Compiled semantic manifest with resolved dataset bindings.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    validate_rust_udf_snapshot(snapshot)
    if runtime_profile is None:
        msg = "Runtime profile is required for semantic view planning."
        raise ValueError(msg)
    nested_nodes = _nested_view_nodes(
        ctx,
        snapshot=snapshot,
        runtime_profile=runtime_profile,
    )
    semantic_nodes = _semantics_view_nodes(
        ctx,
        snapshot=snapshot,
        runtime_profile=runtime_profile,
        semantic_ir=semantic_ir,
        manifest=manifest,
    )
    return (*nested_nodes, *semantic_nodes)


def _semantic_dataset_specs() -> dict[str, DatasetSpec]:
    from schema_spec.dataset_spec_ops import dataset_spec_name
    from semantics.catalog.dataset_specs import dataset_specs

    return {dataset_spec_name(spec): spec for spec in dataset_specs()}


def _dataset_contract_for(
    name: str,
    *,
    dataset_specs: Mapping[str, DatasetSpec],
) -> tuple[pa.Schema | None, bool]:
    dataset_spec = dataset_specs.get(name)
    if dataset_spec is None:
        return None, False
    from schema_spec.dataset_spec_ops import dataset_spec_schema

    return _arrow_schema_from_contract(dataset_spec_schema(dataset_spec)), True


def _schema_divergence_strict_mode(*, dataset_spec: DatasetSpec | None) -> bool:
    if dataset_spec is not None:
        from schema_spec.dataset_spec_ops import dataset_spec_strict_schema_validation

        strict_setting = dataset_spec_strict_schema_validation(dataset_spec)
        if strict_setting is not None:
            return strict_setting
    strict_override = env_bool(
        _SCHEMA_DIVERGENCE_STRICT_ENV,
        default=None,
        on_invalid="none",
    )
    if strict_override is not None:
        return strict_override
    return bool(env_bool(_CI_ENV, default=False, on_invalid="false"))


def _schema_divergence_error_message(
    *,
    view_name: str,
    added_columns: Sequence[str],
    removed_columns: Sequence[str],
    type_mismatches: Sequence[tuple[str, str, str]],
) -> str:
    details: list[str] = []
    if added_columns:
        details.append(f"added_columns={list(added_columns)}")
    if removed_columns:
        details.append(f"removed_columns={list(removed_columns)}")
    if type_mismatches:
        details.append(
            "type_mismatches="
            + str(
                [
                    {"column": col, "spec_type": spec_type, "plan_type": plan_type}
                    for col, spec_type, plan_type in type_mismatches
                ]
            )
        )
    details_text = "; ".join(details) if details else "no divergence details available"
    return f"Schema divergence detected for semantic view {view_name!r}: {details_text}"


def _nested_view_nodes(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> list[ViewNode]:
    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from datafusion_engine.schema.registry import extract_schema_for, nested_view_specs

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
        bundle, deps, required = _bundle_deps_and_udfs(
            ctx,
            spec.builder,
            snapshot,
            label=spec.name,
            runtime_profile=runtime_profile,
        )
        expected_schema = extract_schema_for(spec.name)
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
    manifest: SemanticProgramManifest,
) -> tuple[dict[str, str], bool]:
    from datafusion_engine.lineage.diagnostics import record_artifact
    from semantics.validation import (
        SemanticInputValidationError,
        validate_semantic_inputs,
    )

    validation = manifest.validation
    if validation is None:
        validation = validate_semantic_inputs(
            ctx=ctx,
            manifest=manifest,
            policy=manifest.validation_policy,
        )
    if not validation.valid:
        record_artifact(
            runtime_profile,
            SEMANTIC_INPUT_SCHEMA_VALIDATION_SPEC,
            {
                "missing_tables": list(validation.missing_tables),
                "missing_columns": {
                    table: list(columns) for table, columns in validation.missing_columns.items()
                },
                "resolved_tables": dict(validation.resolved_tables),
            },
        )
        raise SemanticInputValidationError(validation)
    input_mapping = dict(manifest.input_mapping)
    use_cdf = any(name.endswith("__cdf") for name in input_mapping.values())
    return input_mapping, use_cdf


def _semantic_view_specs_for_registration(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    semantic_ir: SemanticIR,
    manifest: SemanticProgramManifest,
) -> list[tuple[str, DataFrameBuilder]]:
    from semantics.pipeline import CpgViewSpecsRequest, _cpg_view_specs

    input_mapping, use_cdf = _validated_semantic_inputs(
        ctx,
        runtime_profile=runtime_profile,
        manifest=manifest,
    )
    return _cpg_view_specs(
        CpgViewSpecsRequest(
            input_mapping=input_mapping,
            config=None,
            use_cdf=use_cdf,
            runtime_profile=runtime_profile,
            requested_outputs=None,
            manifest=manifest,
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
    strict_schema_validation = _schema_divergence_strict_mode(dataset_spec=dataset_spec)
    # Schema divergence detection (10.2): compare spec vs plan output
    if expected_schema is not None and bundle is not None:
        from datafusion_engine.plan.signals import extract_plan_signals
        from datafusion_engine.schema.contracts import compute_schema_divergence

        plan_schema = extract_plan_signals(bundle).schema
        if plan_schema is not None:
            divergence = compute_schema_divergence(expected_schema, plan_schema)
            if divergence.has_divergence:
                from datafusion_engine.lineage.diagnostics import record_artifact

                record_artifact(
                    context.runtime_profile,
                    SCHEMA_DIVERGENCE_SPEC,
                    {
                        "view_name": name,
                        "strict": strict_schema_validation,
                        "severity": "error" if strict_schema_validation else "warning",
                        "spec_columns": list(expected_schema.names),
                        "plan_columns": list(plan_schema.names),
                        "added_columns": list(divergence.added_columns),
                        "removed_columns": list(divergence.removed_columns),
                        "type_mismatches": [
                            {"column": col, "spec_type": st, "plan_type": pt}
                            for col, st, pt in divergence.type_mismatches
                        ],
                    },
                )
                if strict_schema_validation:
                    raise ValueError(
                        _schema_divergence_error_message(
                            view_name=name,
                            added_columns=divergence.added_columns,
                            removed_columns=divergence.removed_columns,
                            type_mismatches=divergence.type_mismatches,
                        )
                    )
    annotated_contract: AnnotatedSchema | None = None
    if expected_schema is not None:
        from semantics.types import AnnotatedSchema

        annotated_contract = AnnotatedSchema.from_arrow_schema(expected_schema)

    row = dataset_row(name, strict=False)
    if row is None:
        override = context.runtime_profile.data_sources.semantic_output.cache_overrides.get(name)
        cache_policy = override if override in {"none", "delta_staging", "delta_output"} else "none"
    else:
        cache_policy = _semantic_cache_policy_for_row(
            row,
            runtime_profile=context.runtime_profile,
            manifest=context.semantic_manifest,
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
    semantic_ir: SemanticIR,
    manifest: SemanticProgramManifest,
) -> list[ViewNode]:
    """Build semantic pipeline view nodes with inferred dependencies.

    Args:
        ctx: DataFusion session context.
        snapshot: UDF snapshot payload.
        runtime_profile: Optional runtime profile.
        semantic_ir: Semantic IR for view planning.
        manifest: Compiled semantic manifest with resolved dataset bindings.

    Returns:
        list[ViewNode]: Result.

    Raises:
        ValueError: If runtime profile is missing.
    """
    from datafusion_engine.io.adapter import DataFusionIOAdapter

    if runtime_profile is None:
        msg = "Runtime profile is required for semantic view planning."
        raise ValueError(msg)

    view_specs = _semantic_view_specs_for_registration(
        ctx,
        runtime_profile=runtime_profile,
        semantic_ir=semantic_ir,
        manifest=manifest,
    )
    dataset_specs = _semantic_dataset_specs()
    adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
    context = SemanticViewNodeContext(
        ctx=ctx,
        snapshot=snapshot,
        runtime_profile=runtime_profile,
        semantic_manifest=manifest,
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
