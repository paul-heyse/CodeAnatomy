"""View node definitions for view-driven normalize/relspec/CPG pipelines."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

import ibis
import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame
from ibis.backends import BaseBackend

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.runtime_profiles import runtime_profile_factory
from cpg.schemas import (
    CPG_EDGES_SCHEMA_CONTRACT,
    CPG_NODES_SCHEMA_CONTRACT,
    CPG_PROPS_SCHEMA_CONTRACT,
)
from cpg.specs import TaskIdentity
from datafusion_engine.nested_tables import ViewReference
from datafusion_engine.schema_contracts import SchemaContract, schema_contract_from_dataset_spec
from datafusion_engine.schema_introspection import SchemaIntrospector, table_names_snapshot
from datafusion_engine.udf_runtime import validate_rust_udf_snapshot
from datafusion_engine.view_graph_registry import ViewNode
from engine.unified_registry import build_unified_function_registry
from ibis_engine.catalog import IbisPlanCatalog
from ibis_engine.plan import IbisPlan

if TYPE_CHECKING:

    from ibis.backends.datafusion import Backend as DataFusionBackend

    from sqlglot_tools.bridge import IbisCompilerBackend


_HASH_UDFS: tuple[str, ...] = (
    "prefixed_hash64",
    "stable_hash64",
    "stable_hash128",
    "stable_id",
)


@dataclass(frozen=True)
class ViewBuildContext:
    """Shared context for building Ibis-backed view DataFrames."""

    ctx: SessionContext
    snapshot: Mapping[str, object]
    backend: BaseBackend
    exec_ctx: ExecutionContext
    facade: DataFusionExecutionFacade

    @classmethod
    def from_session(
        cls,
        ctx: SessionContext,
        *,
        snapshot: Mapping[str, object],
    ) -> ViewBuildContext:
        validate_rust_udf_snapshot(snapshot)
        backend = _ibis_backend(ctx, snapshot=snapshot)
        runtime = replace(runtime_profile_factory("default"), datafusion=None)
        exec_ctx = ExecutionContext(runtime=runtime)
        from datafusion_engine.execution_facade import DataFusionExecutionFacade

        facade = DataFusionExecutionFacade(
            ctx=ctx,
            runtime_profile=None,
            ibis_backend=cast("IbisCompilerBackend", backend),
        )
        return cls(ctx=ctx, snapshot=snapshot, backend=backend, exec_ctx=exec_ctx, facade=facade)

    def builder(
        self,
        plan_builder: Callable[..., IbisPlan | None],
        *,
        builder_kwargs: Mapping[str, object] | None = None,
        label: str | None = None,
    ) -> Callable[[SessionContext], DataFrame]:
        kwargs = dict(builder_kwargs or {})

        def _build(actual_ctx: SessionContext) -> DataFrame:
            _ensure_ctx(actual_ctx, self.ctx, label=label)
            catalog = _catalog_for_ctx(actual_ctx, self.backend)
            plan = plan_builder(catalog, self.exec_ctx, self.backend, **kwargs)
            if plan is None:
                msg = f"View builder {label or plan_builder.__name__} returned None."
                raise ValueError(msg)
            return _plan_to_dataframe(actual_ctx, plan, self.facade)

        return _build


def view_graph_nodes(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
) -> tuple[ViewNode, ...]:
    """Return view graph nodes for normalize + relspec + CPG outputs."""
    build_ctx = ViewBuildContext.from_session(ctx, snapshot=snapshot)
    nodes: list[ViewNode] = []
    nodes.extend(_normalize_view_nodes(build_ctx))
    nodes.extend(_relspec_view_nodes(build_ctx))
    nodes.extend(_cpg_view_nodes(build_ctx))
    nodes.extend(_normalize_alias_nodes(nodes))
    return tuple(nodes)


def _normalize_view_nodes(build_ctx: ViewBuildContext) -> list[ViewNode]:
    from normalize.dataset_specs import dataset_spec
    from normalize.ibis_plan_builders import (
        CFG_BLOCKS_NAME,
        CFG_EDGES_NAME,
        DEF_USE_NAME,
        DIAG_NAME,
        REACHES_NAME,
        TYPE_EXPRS_NAME,
        TYPE_NODES_NAME,
        cfg_blocks_plan_ibis,
        cfg_edges_plan_ibis,
        def_use_events_plan_ibis,
        diagnostics_plan_ibis,
        reaching_defs_plan_ibis,
        span_errors_plan_ibis,
        type_exprs_plan_ibis,
        type_nodes_plan_ibis,
    )

    normalize_nodes = [
        ViewNode(
            name=TYPE_EXPRS_NAME,
            deps=(),
            builder=build_ctx.builder(type_exprs_plan_ibis, label=TYPE_EXPRS_NAME),
            schema_contract=schema_contract_from_dataset_spec(
                name=TYPE_EXPRS_NAME, spec=dataset_spec(TYPE_EXPRS_NAME)
            ),
            required_udfs=_HASH_UDFS,
        ),
        ViewNode(
            name=TYPE_NODES_NAME,
            deps=(TYPE_EXPRS_NAME,),
            builder=build_ctx.builder(type_nodes_plan_ibis, label=TYPE_NODES_NAME),
            schema_contract=schema_contract_from_dataset_spec(
                name=TYPE_NODES_NAME, spec=dataset_spec(TYPE_NODES_NAME)
            ),
            required_udfs=_HASH_UDFS,
        ),
        ViewNode(
            name=CFG_BLOCKS_NAME,
            deps=(),
            builder=build_ctx.builder(cfg_blocks_plan_ibis, label=CFG_BLOCKS_NAME),
            schema_contract=schema_contract_from_dataset_spec(
                name=CFG_BLOCKS_NAME, spec=dataset_spec(CFG_BLOCKS_NAME)
            ),
            required_udfs=_HASH_UDFS,
        ),
        ViewNode(
            name=CFG_EDGES_NAME,
            deps=(),
            builder=build_ctx.builder(cfg_edges_plan_ibis, label=CFG_EDGES_NAME),
            schema_contract=schema_contract_from_dataset_spec(
                name=CFG_EDGES_NAME, spec=dataset_spec(CFG_EDGES_NAME)
            ),
            required_udfs=_HASH_UDFS,
        ),
        ViewNode(
            name=DEF_USE_NAME,
            deps=(),
            builder=build_ctx.builder(def_use_events_plan_ibis, label=DEF_USE_NAME),
            schema_contract=schema_contract_from_dataset_spec(
                name=DEF_USE_NAME, spec=dataset_spec(DEF_USE_NAME)
            ),
            required_udfs=_HASH_UDFS,
        ),
        ViewNode(
            name=REACHES_NAME,
            deps=(DEF_USE_NAME,),
            builder=build_ctx.builder(reaching_defs_plan_ibis, label=REACHES_NAME),
            schema_contract=schema_contract_from_dataset_spec(
                name=REACHES_NAME, spec=dataset_spec(REACHES_NAME)
            ),
            required_udfs=_HASH_UDFS,
        ),
        ViewNode(
            name=DIAG_NAME,
            deps=(),
            builder=build_ctx.builder(diagnostics_plan_ibis, label=DIAG_NAME),
            schema_contract=schema_contract_from_dataset_spec(
                name=DIAG_NAME, spec=dataset_spec(DIAG_NAME)
            ),
            required_udfs=_HASH_UDFS,
        ),
        ViewNode(
            name="span_errors_v1",
            deps=(),
            builder=build_ctx.builder(span_errors_plan_ibis, label="span_errors_v1"),
            schema_contract=schema_contract_from_dataset_spec(
                name="span_errors_v1", spec=dataset_spec("span_errors_v1")
            ),
            required_udfs=(),
        ),
    ]
    return normalize_nodes


def _relspec_view_nodes(build_ctx: ViewBuildContext) -> list[ViewNode]:
    from relspec.contracts import (
        rel_callsite_qname_schema_contract,
        rel_callsite_symbol_schema_contract,
        rel_def_symbol_schema_contract,
        rel_import_symbol_schema_contract,
        rel_name_symbol_schema_contract,
        relation_output_schema_contract,
    )
    from relspec.relationship_plans import (
        REL_CALLSITE_QNAME_OUTPUT,
        REL_CALLSITE_SYMBOL_OUTPUT,
        REL_DEF_SYMBOL_OUTPUT,
        REL_IMPORT_SYMBOL_OUTPUT,
        REL_NAME_SYMBOL_OUTPUT,
        RELATION_OUTPUT_NAME,
        build_rel_callsite_qname_plan,
        build_rel_callsite_symbol_plan,
        build_rel_def_symbol_plan,
        build_rel_import_symbol_plan,
        build_rel_name_symbol_plan,
        build_relation_output_plan,
    )

    priority = 100
    rel_nodes = [
        ViewNode(
            name=REL_NAME_SYMBOL_OUTPUT,
            deps=(),
            builder=build_ctx.builder(
                build_rel_name_symbol_plan,
                builder_kwargs={"task_name": "rel.name_symbol", "task_priority": priority},
                label=REL_NAME_SYMBOL_OUTPUT,
            ),
            schema_contract=rel_name_symbol_schema_contract(),
            required_udfs=_HASH_UDFS,
        ),
        ViewNode(
            name=REL_IMPORT_SYMBOL_OUTPUT,
            deps=(),
            builder=build_ctx.builder(
                build_rel_import_symbol_plan,
                builder_kwargs={"task_name": "rel.import_symbol", "task_priority": priority},
                label=REL_IMPORT_SYMBOL_OUTPUT,
            ),
            schema_contract=rel_import_symbol_schema_contract(),
            required_udfs=_HASH_UDFS,
        ),
        ViewNode(
            name=REL_DEF_SYMBOL_OUTPUT,
            deps=(),
            builder=build_ctx.builder(
                build_rel_def_symbol_plan,
                builder_kwargs={"task_name": "rel.def_symbol", "task_priority": priority},
                label=REL_DEF_SYMBOL_OUTPUT,
            ),
            schema_contract=rel_def_symbol_schema_contract(),
            required_udfs=_HASH_UDFS,
        ),
        ViewNode(
            name=REL_CALLSITE_SYMBOL_OUTPUT,
            deps=(),
            builder=build_ctx.builder(
                build_rel_callsite_symbol_plan,
                builder_kwargs={
                    "task_name": "rel.callsite_symbol",
                    "task_priority": priority,
                },
                label=REL_CALLSITE_SYMBOL_OUTPUT,
            ),
            schema_contract=rel_callsite_symbol_schema_contract(),
            required_udfs=_HASH_UDFS,
        ),
        ViewNode(
            name=REL_CALLSITE_QNAME_OUTPUT,
            deps=(),
            builder=build_ctx.builder(
                build_rel_callsite_qname_plan,
                builder_kwargs={
                    "task_name": "rel.callsite_qname",
                    "task_priority": priority,
                },
                label=REL_CALLSITE_QNAME_OUTPUT,
            ),
            schema_contract=rel_callsite_qname_schema_contract(),
            required_udfs=_HASH_UDFS,
        ),
        ViewNode(
            name=RELATION_OUTPUT_NAME,
            deps=(
                REL_NAME_SYMBOL_OUTPUT,
                REL_IMPORT_SYMBOL_OUTPUT,
                REL_DEF_SYMBOL_OUTPUT,
                REL_CALLSITE_SYMBOL_OUTPUT,
                REL_CALLSITE_QNAME_OUTPUT,
            ),
            builder=build_ctx.builder(build_relation_output_plan, label=RELATION_OUTPUT_NAME),
            schema_contract=relation_output_schema_contract(),
            required_udfs=_HASH_UDFS,
        ),
    ]
    return rel_nodes


def _cpg_view_nodes(build_ctx: ViewBuildContext) -> list[ViewNode]:
    from cpg.plan_builders import build_cpg_edges_plan, build_cpg_nodes_plan, build_cpg_props_plan
    from cpg.spec_registry import node_plan_specs, prop_table_specs
    from normalize.dataset_specs import dataset_alias
    from relspec.relationship_plans import RELATION_OUTPUT_NAME

    priority = 100
    node_identity = TaskIdentity(name="cpg.nodes", priority=priority)
    prop_identity = TaskIdentity(name="cpg.props", priority=priority)
    node_tables = {spec.table_ref for spec in node_plan_specs()}
    prop_tables = {spec.table_ref for spec in prop_table_specs(source_columns_lookup=None)}
    deps: set[str] = set()
    for table in (*node_tables, *prop_tables):
        try:
            deps.add(dataset_alias(table))
        except KeyError:
            continue
    normalized_deps = tuple(sorted(deps))
    return [
        ViewNode(
            name="cpg_nodes_v1",
            deps=normalized_deps,
            builder=build_ctx.builder(
                build_cpg_nodes_plan,
                builder_kwargs={"task_identity": node_identity},
                label="cpg_nodes_v1",
            ),
            schema_contract=CPG_NODES_SCHEMA_CONTRACT,
            required_udfs=_HASH_UDFS,
        ),
        ViewNode(
            name="cpg_edges_v1",
            deps=(RELATION_OUTPUT_NAME,),
            builder=build_ctx.builder(build_cpg_edges_plan, label="cpg_edges_v1"),
            schema_contract=CPG_EDGES_SCHEMA_CONTRACT,
            required_udfs=_HASH_UDFS,
        ),
        ViewNode(
            name="cpg_props_v1",
            deps=normalized_deps,
            builder=build_ctx.builder(
                build_cpg_props_plan,
                builder_kwargs={"task_identity": prop_identity},
                label="cpg_props_v1",
            ),
            schema_contract=CPG_PROPS_SCHEMA_CONTRACT,
            required_udfs=_HASH_UDFS,
        ),
    ]


def _normalize_alias_nodes(nodes: Sequence[ViewNode]) -> list[ViewNode]:
    from normalize.dataset_specs import dataset_alias, dataset_schema

    registered = {node.name for node in nodes}
    alias_nodes: list[ViewNode] = []
    for name in registered:
        try:
            alias = dataset_alias(name)
        except KeyError:
            continue
        if alias == name or alias in registered:
            continue
        schema = dataset_schema(name)
        alias_nodes.append(
            ViewNode(
                name=alias,
                deps=(name,),
                builder=_alias_builder(source=name),
                schema_contract=SchemaContract.from_arrow_schema(alias, _resolve_schema(schema)),
                required_udfs=(),
            )
        )
    return alias_nodes


def _ibis_backend(ctx: SessionContext, *, snapshot: Mapping[str, object]) -> BaseBackend:
    validate_rust_udf_snapshot(snapshot)
    backend = ibis.datafusion.connect(ctx)
    introspector = SchemaIntrospector(ctx)
    catalog = introspector.function_catalog_snapshot(include_parameters=True)
    unified = build_unified_function_registry(
        datafusion_function_catalog=catalog,
        snapshot=introspector.snapshot,
        registry_snapshot=snapshot,
    )
    unified.udf_registry.apply_to_backend(cast("DataFusionBackend", backend))
    return backend


def _catalog_for_ctx(ctx: SessionContext, backend: BaseBackend) -> IbisPlanCatalog:
    names = table_names_snapshot(ctx)
    if not names:
        msg = "No DataFusion tables registered; cannot build view catalog."
        raise ValueError(msg)
    tables = {name: ViewReference(name) for name in names}
    return IbisPlanCatalog(backend=backend, tables=tables)


def _plan_to_dataframe(
    ctx: SessionContext,
    plan: IbisPlan,
    facade: DataFusionExecutionFacade,
) -> DataFrame:
    _ = ctx
    compiled = facade.compile(plan.expr)
    result = facade.execute(compiled)
    return result.require_dataframe()


def _alias_builder(source: str) -> Callable[[SessionContext], DataFrame]:
    def _build(ctx: SessionContext) -> DataFrame:
        return ctx.table(source)

    return _build


def _ensure_ctx(actual: SessionContext, expected: SessionContext, *, label: str | None) -> None:
    if actual is expected:
        return
    msg = f"View builder {label or 'unknown'} received an unexpected SessionContext."
    raise ValueError(msg)


def _resolve_schema(schema: object) -> pa.Schema:
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Failed to resolve schema for alias view."
    raise TypeError(msg)


__all__ = ["ViewBuildContext", "view_graph_nodes"]
