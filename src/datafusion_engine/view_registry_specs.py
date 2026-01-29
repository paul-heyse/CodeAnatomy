"""View node definitions for view-driven normalize/relspec/CPG pipelines."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from functools import partial
from typing import TYPE_CHECKING, Literal

import pyarrow as pa

from arrow_utils.core.ordering import OrderingLevel
from cpg.specs import TaskIdentity
from datafusion_engine.arrow_interop import SchemaLike
from datafusion_engine.arrow_schema.metadata import (
    SchemaMetadataSpec,
    function_requirements_metadata_spec,
    merge_metadata_specs,
    ordering_metadata_spec,
)
from datafusion_engine.lineage_datafusion import extract_lineage
from datafusion_engine.plan_bundle import PlanBundleOptions, build_plan_bundle
from datafusion_engine.schema_contracts import SchemaContract
from datafusion_engine.udf_runtime import udf_names_from_snapshot, validate_rust_udf_snapshot
from datafusion_engine.view_graph_registry import ViewNode

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.plan_bundle import DataFusionPlanBundle
    from datafusion_engine.runtime import DataFusionRuntimeProfile, SessionRuntime
    from schema_spec.system import DatasetSpec


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


def _required_udfs_from_plan_bundle(
    bundle: DataFusionPlanBundle,
    snapshot: Mapping[str, object],
) -> tuple[str, ...]:
    required = bundle.required_udfs
    if not required and bundle.optimized_logical_plan is not None:
        lineage = extract_lineage(
            bundle.optimized_logical_plan,
            udf_snapshot=bundle.artifacts.udf_snapshot,
        )
        required = lineage.required_udfs
    if not required:
        return ()
    snapshot_names = udf_names_from_snapshot(snapshot)
    lookup = {name.lower(): name for name in snapshot_names}
    resolved = {lookup[name.lower()] for name in required if name.lower() in lookup}
    return tuple(sorted(resolved))


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
        lineage = extract_lineage(
            bundle.optimized_logical_plan,
            udf_snapshot=bundle.artifacts.udf_snapshot,
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to extract lineage for view {label!r}."
        raise ValueError(msg) from exc
    deps = lineage.referenced_tables
    required = _required_udfs_from_plan_bundle(bundle, snapshot)
    return bundle, deps, required


def _deps_and_udfs_from_bundle(
    bundle: DataFusionPlanBundle,
    snapshot: Mapping[str, object],
    *,
    label: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    try:
        lineage = extract_lineage(
            bundle.optimized_logical_plan,
            udf_snapshot=bundle.artifacts.udf_snapshot,
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to extract lineage for view {label!r}."
        raise ValueError(msg) from exc
    deps = lineage.referenced_tables
    required = _required_udfs_from_plan_bundle(bundle, snapshot)
    return deps, required


def _arrow_schema_from_df(df: DataFrame) -> pa.Schema:
    schema = df.schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Failed to resolve DataFusion schema."
    raise TypeError(msg)


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
            _relspec_view_nodes(
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
            )
        )
    return nodes


def _relspec_view_nodes(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> list[ViewNode]:
    from relspec.contracts import (
        rel_callsite_qname_metadata_spec,
        rel_callsite_symbol_metadata_spec,
        rel_def_symbol_metadata_spec,
        rel_import_symbol_metadata_spec,
        rel_name_symbol_metadata_spec,
        relation_output_metadata_spec,
    )
    from relspec.relationship_datafusion import (
        build_rel_callsite_qname_df,
        build_rel_callsite_symbol_df,
        build_rel_def_symbol_df,
        build_rel_import_symbol_df,
        build_rel_name_symbol_df,
        build_relation_output_df,
    )
    from relspec.view_defs import (
        DEFAULT_REL_TASK_PRIORITY,
        REL_CALLSITE_QNAME_OUTPUT,
        REL_CALLSITE_SYMBOL_OUTPUT,
        REL_DEF_SYMBOL_OUTPUT,
        REL_IMPORT_SYMBOL_OUTPUT,
        REL_NAME_SYMBOL_OUTPUT,
        RELATION_OUTPUT_NAME,
    )

    priority = DEFAULT_REL_TASK_PRIORITY
    view_specs = (
        (
            REL_NAME_SYMBOL_OUTPUT,
            partial(
                build_rel_name_symbol_df,
                task_name="rel.name_symbol",
                task_priority=priority,
            ),
            rel_name_symbol_metadata_spec(),
        ),
        (
            REL_IMPORT_SYMBOL_OUTPUT,
            partial(
                build_rel_import_symbol_df,
                task_name="rel.import_symbol",
                task_priority=priority,
            ),
            rel_import_symbol_metadata_spec(),
        ),
        (
            REL_DEF_SYMBOL_OUTPUT,
            partial(
                build_rel_def_symbol_df,
                task_name="rel.def_symbol",
                task_priority=priority,
            ),
            rel_def_symbol_metadata_spec(),
        ),
        (
            REL_CALLSITE_SYMBOL_OUTPUT,
            partial(
                build_rel_callsite_symbol_df,
                task_name="rel.callsite_symbol",
                task_priority=priority,
            ),
            rel_callsite_symbol_metadata_spec(),
        ),
        (
            REL_CALLSITE_QNAME_OUTPUT,
            partial(
                build_rel_callsite_qname_df,
                task_name="rel.callsite_qname",
                task_priority=priority,
            ),
            rel_callsite_qname_metadata_spec(),
        ),
        (
            RELATION_OUTPUT_NAME,
            build_relation_output_df,
            relation_output_metadata_spec(),
        ),
    )

    nodes: list[ViewNode] = []
    for name, builder, metadata in view_specs:
        bundle, deps, required = _bundle_deps_and_udfs(
            ctx,
            builder,
            snapshot,
            label=name,
            runtime_profile=runtime_profile,
        )
        metadata_with_udfs = _metadata_with_required_udfs(metadata, required)
        nodes.append(
            ViewNode(
                name=name,
                deps=deps,
                builder=builder,
                contract_builder=_contract_builder(name, metadata_spec=metadata_with_udfs),
                required_udfs=required,
                plan_bundle=bundle,
            )
        )
    return nodes


def _symtable_view_nodes(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> list[ViewNode]:
    from datafusion_engine.symtable_views import (
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


def _cpg_view_nodes(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> list[ViewNode]:
    from cpg.view_builders_df import (
        build_cpg_edges_by_dst_df,
        build_cpg_edges_by_src_df,
        build_cpg_edges_df,
        build_cpg_nodes_df,
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
    nodes_identity = TaskIdentity(name="cpg.nodes", priority=priority)
    props_identity = TaskIdentity(name="cpg.props", priority=priority)

    view_specs: tuple[tuple[str, DataFrameBuilder], ...] = (
        ("cpg_nodes_v1", _wrap(partial(build_cpg_nodes_df, task_identity=nodes_identity))),
        ("cpg_edges_v1", _wrap(build_cpg_edges_df)),
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
        schema = _arrow_schema_from_df(bundle.df)
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
