"""View node definitions for view-driven normalize/relspec/CPG pipelines."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal, cast

import ibis
import pyarrow as pa

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import SchemaLike
from arrowdsl.core.ordering import Ordering, OrderingLevel
from arrowdsl.core.runtime_profiles import runtime_profile_factory
from arrowdsl.schema.build import empty_table
from arrowdsl.schema.metadata import (
    function_requirements_metadata_spec,
    merge_metadata_specs,
    ordering_metadata_spec,
)
from arrowdsl.schema.schema import SchemaMetadataSpec
from cpg.specs import TaskIdentity
from datafusion_engine.nested_tables import ViewReference
from datafusion_engine.schema_contracts import SchemaContract
from datafusion_engine.schema_introspection import table_names_snapshot
from datafusion_engine.udf_runtime import udf_names_from_snapshot, validate_rust_udf_snapshot
from datafusion_engine.view_graph_registry import ViewNode
from ibis_engine.catalog import IbisPlanCatalog
from ibis_engine.plan import IbisPlan
from ibis_engine.sources import SourceToIbisOptions, register_ibis_table
from sqlglot_tools.compat import Expression
from sqlglot_tools.lineage import referenced_tables, referenced_udf_calls
from sqlglot_tools.optimizer import (
    StrictParseOptions,
    parse_sql_strict,
    register_datafusion_dialect,
    resolve_sqlglot_policy,
    sqlglot_sql,
)
from sqlglot_tools.view_builders import select_all

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame
    from ibis.backends import BaseBackend
    from ibis.expr.types import Table

    from datafusion_engine.execution_facade import DataFusionExecutionFacade
    from ibis_engine.catalog import IbisPlanSource
    from schema_spec.system import DatasetSpec
    from sqlglot_tools.bridge import IbisCompilerBackend

try:
    from datafusion.plan import LogicalPlan as DataFusionLogicalPlan
    from datafusion.unparser import Dialect as DataFusionDialect
    from datafusion.unparser import Unparser as DataFusionUnparser
except ImportError:  # pragma: no cover - optional dependency
    DataFusionDialect = None
    DataFusionUnparser = None
    DataFusionLogicalPlan = None


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


def _required_udfs_from_ast(
    expr: Expression,
    snapshot: Mapping[str, object],
) -> tuple[str, ...]:
    udf_calls = referenced_udf_calls(expr)
    if not udf_calls:
        return ()
    snapshot_names = udf_names_from_snapshot(snapshot)
    lookup = {name.lower(): name for name in snapshot_names}
    required = {
        lookup[name.lower()]
        for name in udf_calls
        if isinstance(name, str) and name.lower() in lookup
    }
    return tuple(sorted(required))


def _deps_from_ast(expr: Expression) -> tuple[str, ...]:
    return tuple(referenced_tables(expr))


def _sqlglot_from_dataframe(df: DataFrame) -> Expression | None:
    if DataFusionUnparser is None or DataFusionDialect is None or DataFusionLogicalPlan is None:
        return None
    logical_plan = getattr(df, "logical_plan", None)
    if not callable(logical_plan):
        return None
    sql = ""
    try:
        plan_obj = logical_plan()
        if isinstance(plan_obj, DataFusionLogicalPlan):
            unparser = DataFusionUnparser(DataFusionDialect.default())
            sql = str(unparser.plan_to_sql(plan_obj))
    except (RuntimeError, TypeError, ValueError):
        return None
    if not sql:
        return None
    policy = resolve_sqlglot_policy(name="datafusion")
    register_datafusion_dialect()
    try:
        ast = parse_sql_strict(
            sql,
            dialect=policy.write_dialect,
            options=StrictParseOptions(error_level=policy.error_level),
        )
    except (TypeError, ValueError):
        return None
    return ast


def _ibis_expr_from_sqlglot(
    build_ctx: ViewBuildContext,
    expr: Expression,
    *,
    label: str,
) -> Table:
    policy = resolve_sqlglot_policy(name="datafusion_compile")
    sql = sqlglot_sql(expr, policy=policy)
    sql_func = getattr(build_ctx.backend, "sql", None)
    if not callable(sql_func):
        msg = f"Backend does not support SQL compilation for view {label!r}."
        raise TypeError(msg)
    try:
        return cast("Table", sql_func(sql))
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to build Ibis expression for view {label!r}."
        raise ValueError(msg) from exc


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


def _arrow_schema_from_ibis(plan: IbisPlan | Table) -> pa.Schema:
    expr = plan.expr if isinstance(plan, IbisPlan) else plan
    schema = expr.schema()
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Failed to resolve Ibis schema."
    raise TypeError(msg)


@dataclass(frozen=True)
class ViewBuildContext:
    """Shared context for building Ibis-backed view DataFrames."""

    ctx: SessionContext
    snapshot: Mapping[str, object]
    backend: BaseBackend
    exec_ctx: ExecutionContext
    facade: DataFusionExecutionFacade
    catalog: IbisPlanCatalog

    @classmethod
    def from_session(
        cls,
        ctx: SessionContext,
        *,
        snapshot: Mapping[str, object],
    ) -> ViewBuildContext:
        """Build a view context from a SessionContext and snapshot.

        Parameters
        ----------
        ctx
            DataFusion session context for view registration.
        snapshot
            Rust UDF registry snapshot for validation and wiring.

        Returns
        -------
        ViewBuildContext
            Ready-to-use view build context.
        """
        validate_rust_udf_snapshot(snapshot)
        backend = _ibis_backend(ctx, snapshot=snapshot)
        runtime = replace(runtime_profile_factory("default"), datafusion=None)
        exec_ctx = ExecutionContext(runtime=runtime)
        catalog = _catalog_for_ctx(ctx, backend)
        from datafusion_engine.execution_facade import DataFusionExecutionFacade

        facade = DataFusionExecutionFacade(
            ctx=ctx,
            runtime_profile=None,
            ibis_backend=cast("IbisCompilerBackend", backend),
        )
        return cls(
            ctx=ctx,
            snapshot=snapshot,
            backend=backend,
            exec_ctx=exec_ctx,
            facade=facade,
            catalog=catalog,
        )

    def plan_and_ast(
        self,
        plan_builder: Callable[..., IbisPlan | Table | None],
        *,
        builder_kwargs: Mapping[str, object] | None = None,
        label: str | None = None,
    ) -> tuple[IbisPlan | Table, Expression]:
        """Build a plan and SQLGlot AST from a plan builder.

        Parameters
        ----------
        plan_builder
            Callable returning an Ibis plan for the view.
        builder_kwargs
            Optional keyword arguments passed into the plan builder.
        label
            Optional label for diagnostics/errors.

        Returns
        -------
        tuple[IbisPlan | Table, Expression]
            The built plan and compiled SQLGlot AST.

        Raises
        ------
        ValueError
            Raised when the plan builder returns None.
        """
        kwargs = dict(builder_kwargs or {})
        plan = plan_builder(self.catalog, self.exec_ctx, self.backend, **kwargs)
        if plan is None:
            msg = f"View builder {label or plan_builder.__name__} returned None."
            raise ValueError(msg)
        expr = plan.expr if isinstance(plan, IbisPlan) else plan
        from datafusion_engine.compile_options import DataFusionCompileOptions
        from datafusion_engine.sql_policy_engine import SQLPolicyProfile

        compiled = self.facade.compile(
            expr,
            options=DataFusionCompileOptions(sql_policy_profile=SQLPolicyProfile()),
        )
        return plan, compiled.compiled.sqlglot_ast

    def register_plan(self, name: str, plan: IbisPlan | Table) -> None:
        """Register a derived plan into the shared catalog."""
        if isinstance(plan, IbisPlan):
            self.catalog.add(name, plan)
            return
        ibis_plan = IbisPlan(expr=plan, ordering=Ordering.unordered())
        self.catalog.add(name, ibis_plan)

    def register_schema(self, name: str, schema: pa.Schema) -> None:
        """Register an empty-schema placeholder for a derived view."""
        contract = SchemaContract.from_arrow_schema(name, schema)
        empty = empty_table(contract.to_arrow_schema())
        plan = register_ibis_table(
            empty,
            options=SourceToIbisOptions(
                backend=self.backend,
                name=None,
                ordering=Ordering.unordered(),
                runtime_profile=self.exec_ctx.runtime.datafusion,
            ),
        )
        self.catalog.add(name, plan)

    def builder_from_plan(
        self,
        plan: IbisPlan | Table,
        *,
        label: str | None = None,
    ) -> Callable[[SessionContext], DataFrame]:
        """Wrap a pre-built plan into a DataFusion view builder.

        Parameters
        ----------
        plan
            Ibis plan or table expression for the view.
        label
            Optional label for diagnostics/errors.

        Returns
        -------
        Callable[[SessionContext], DataFrame]
            View builder that returns a DataFusion DataFrame.
        """

        def _build(actual_ctx: SessionContext) -> DataFrame:
            _ensure_ctx(actual_ctx, self.ctx, label=label)
            return _plan_to_dataframe(actual_ctx, plan, self.facade)

        return _build

    def builder(
        self,
        plan_builder: Callable[..., IbisPlan | Table | None],
        *,
        builder_kwargs: Mapping[str, object] | None = None,
        label: str | None = None,
    ) -> Callable[[SessionContext], DataFrame]:
        """Wrap a plan builder into a DataFusion view builder.

        Parameters
        ----------
        plan_builder
            Callable returning an Ibis plan for the view.
        builder_kwargs
            Optional keyword arguments passed into the plan builder.
        label
            Optional label for diagnostics/errors.

        Returns
        -------
        Callable[[SessionContext], DataFrame]
            View builder function that returns a DataFusion DataFrame.
        """
        plan, _ = self.plan_and_ast(
            plan_builder,
            builder_kwargs=builder_kwargs,
            label=label,
        )
        return self.builder_from_plan(plan, label=label)


def view_graph_nodes(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
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
    build_ctx = ViewBuildContext.from_session(ctx, snapshot=snapshot)
    nodes: list[ViewNode] = []
    if stage in {"all", "pre_cpg"}:
        nodes.extend(_normalize_view_nodes(build_ctx))
        nodes.extend(_relspec_view_nodes(build_ctx))
        nodes.extend(_symtable_view_nodes(build_ctx))
    if stage in {"all", "cpg"}:
        nodes.extend(_cpg_view_nodes(build_ctx))
    nodes.extend(_alias_nodes(nodes, build_ctx=build_ctx))
    return tuple(nodes)


def _normalize_view_nodes(build_ctx: ViewBuildContext) -> list[ViewNode]:
    from normalize.dataset_specs import dataset_contract_schema, dataset_spec
    from normalize.view_builders import normalize_view_specs

    nodes: list[ViewNode] = []
    for spec in normalize_view_specs():
        plan, ast = build_ctx.plan_and_ast(spec.builder, label=spec.label)
        build_ctx.register_plan(spec.name, plan)
        required = _required_udfs_from_ast(ast, build_ctx.snapshot)
        dataset = dataset_spec(spec.name)
        metadata = _metadata_from_dataset_spec(dataset)
        metadata = _metadata_with_required_udfs(metadata, required)
        expected_schema = _arrow_schema_from_contract(dataset_contract_schema(spec.name))
        ibis_expr = plan.expr if isinstance(plan, IbisPlan) else plan
        nodes.append(
            ViewNode(
                name=spec.name,
                deps=_deps_from_ast(ast),
                builder=build_ctx.builder_from_plan(plan, label=spec.label),
                contract_builder=_contract_builder(
                    spec.name,
                    metadata_spec=metadata,
                    expected_schema=expected_schema,
                    enforce_columns=True,
                ),
                required_udfs=required,
                sqlglot_ast=ast,
                ibis_expr=ibis_expr,
            )
        )
    return nodes


def _view_node_from_sqlglot(
    build_ctx: ViewBuildContext,
    *,
    name: str,
    expr: Expression,
    schema_metadata: SchemaMetadataSpec | None = None,
) -> ViewNode:
    compiled = build_ctx.facade.compile(expr)
    canonical_ast = compiled.compiled.sqlglot_ast
    ibis_expr = _ibis_expr_from_sqlglot(build_ctx, canonical_ast, label=name)

    def _build(actual_ctx: SessionContext) -> DataFrame:
        _ensure_ctx(actual_ctx, build_ctx.ctx, label=name)
        result = build_ctx.facade.execute(compiled)
        return result.require_dataframe()

    required = _required_udfs_from_ast(canonical_ast, build_ctx.snapshot)
    metadata = _metadata_with_required_udfs(schema_metadata, required)
    return ViewNode(
        name=name,
        deps=_deps_from_ast(canonical_ast),
        builder=_build,
        contract_builder=_contract_builder(name, metadata_spec=metadata),
        required_udfs=required,
        sqlglot_ast=canonical_ast,
        ibis_expr=ibis_expr,
    )


def _relspec_view_nodes(build_ctx: ViewBuildContext) -> list[ViewNode]:
    from relspec.contracts import (
        rel_callsite_qname_metadata_spec,
        rel_callsite_symbol_metadata_spec,
        rel_def_symbol_metadata_spec,
        rel_import_symbol_metadata_spec,
        rel_name_symbol_metadata_spec,
        relation_output_metadata_spec,
    )
    from relspec.relationship_sql import (
        build_rel_callsite_qname_sql,
        build_rel_callsite_symbol_sql,
        build_rel_def_symbol_sql,
        build_rel_import_symbol_sql,
        build_rel_name_symbol_sql,
        build_relation_output_sql,
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

    rel_name_expr = build_rel_name_symbol_sql(
        task_name="rel.name_symbol",
        task_priority=priority,
    )
    rel_import_expr = build_rel_import_symbol_sql(
        task_name="rel.import_symbol",
        task_priority=priority,
    )
    rel_def_expr = build_rel_def_symbol_sql(
        task_name="rel.def_symbol",
        task_priority=priority,
    )
    rel_callsite_expr = build_rel_callsite_symbol_sql(
        task_name="rel.callsite_symbol",
        task_priority=priority,
    )
    rel_callsite_qname_expr = build_rel_callsite_qname_sql(
        task_name="rel.callsite_qname",
        task_priority=priority,
    )
    relation_output_expr = build_relation_output_sql()

    def _rel_view_node(
        name: str,
        *,
        expr: Expression,
        metadata: SchemaMetadataSpec,
    ) -> ViewNode:
        node = _view_node_from_sqlglot(
            build_ctx,
            name=name,
            expr=expr,
            schema_metadata=metadata,
        )
        df = node.builder(build_ctx.ctx)
        build_ctx.register_schema(name, _arrow_schema_from_df(df))
        return node

    return [
        _rel_view_node(
            REL_NAME_SYMBOL_OUTPUT,
            expr=rel_name_expr,
            metadata=rel_name_symbol_metadata_spec(),
        ),
        _rel_view_node(
            REL_IMPORT_SYMBOL_OUTPUT,
            expr=rel_import_expr,
            metadata=rel_import_symbol_metadata_spec(),
        ),
        _rel_view_node(
            REL_DEF_SYMBOL_OUTPUT,
            expr=rel_def_expr,
            metadata=rel_def_symbol_metadata_spec(),
        ),
        _rel_view_node(
            REL_CALLSITE_SYMBOL_OUTPUT,
            expr=rel_callsite_expr,
            metadata=rel_callsite_symbol_metadata_spec(),
        ),
        _rel_view_node(
            REL_CALLSITE_QNAME_OUTPUT,
            expr=rel_callsite_qname_expr,
            metadata=rel_callsite_qname_metadata_spec(),
        ),
        _rel_view_node(
            RELATION_OUTPUT_NAME,
            expr=relation_output_expr,
            metadata=relation_output_metadata_spec(),
        ),
    ]


def _symtable_view_nodes(_build_ctx: ViewBuildContext) -> list[ViewNode]:
    from datafusion_engine.symtable_views import (
        symtable_binding_resolutions_df,
        symtable_bindings_df,
        symtable_def_sites_df,
        symtable_type_param_edges_df,
        symtable_type_params_df,
        symtable_use_sites_df,
    )

    def _metadata(required: Sequence[str]) -> SchemaMetadataSpec | None:
        return _metadata_with_required_udfs(None, required)

    def _df_view_node(
        name: str,
        *,
        builder: Callable[[SessionContext], DataFrame],
    ) -> ViewNode:
        df = builder(_build_ctx.ctx)
        _build_ctx.register_schema(name, _arrow_schema_from_df(df))
        ast = _sqlglot_from_dataframe(df)
        if ast is None:
            msg = f"Missing SQLGlot AST for symtable view {name!r}."
            raise ValueError(msg)
        deps = _deps_from_ast(ast)
        required = _required_udfs_from_ast(ast, _build_ctx.snapshot)
        ibis_expr = _ibis_expr_from_sqlglot(_build_ctx, ast, label=name)
        return ViewNode(
            name=name,
            deps=deps,
            builder=builder,
            contract_builder=_contract_builder(name, metadata_spec=_metadata(required)),
            required_udfs=required,
            sqlglot_ast=ast,
            ibis_expr=ibis_expr,
        )

    return [
        _df_view_node(
            "symtable_bindings",
            builder=symtable_bindings_df,
        ),
        _df_view_node(
            "symtable_def_sites",
            builder=symtable_def_sites_df,
        ),
        _df_view_node(
            "symtable_use_sites",
            builder=symtable_use_sites_df,
        ),
        _df_view_node(
            "symtable_type_params",
            builder=symtable_type_params_df,
        ),
        _df_view_node(
            "symtable_type_param_edges",
            builder=symtable_type_param_edges_df,
        ),
        _df_view_node(
            "symtable_binding_resolutions",
            builder=symtable_binding_resolutions_df,
        ),
    ]


def _cpg_view_nodes(build_ctx: ViewBuildContext) -> list[ViewNode]:
    from cpg.view_builders import build_cpg_edges_expr, build_cpg_nodes_expr, build_cpg_props_expr

    priority = 100
    plan_specs = (
        ("cpg_nodes_v1", build_cpg_nodes_expr, TaskIdentity(name="cpg.nodes", priority=priority)),
        ("cpg_edges_v1", build_cpg_edges_expr, None),
        ("cpg_props_v1", build_cpg_props_expr, TaskIdentity(name="cpg.props", priority=priority)),
    )
    resolved: list[tuple[str, IbisPlan | Table, Expression]] = []
    for name, builder, identity in plan_specs:
        kwargs = {"task_identity": identity} if identity is not None else {}
        plan, ast = build_ctx.plan_and_ast(builder, builder_kwargs=kwargs, label=name)
        build_ctx.register_plan(name, plan)
        resolved.append((name, plan, ast))
    return [
        _cpg_view_node(build_ctx, name=name, plan=plan, ast=ast) for name, plan, ast in resolved
    ]


def _cpg_view_node(
    build_ctx: ViewBuildContext,
    *,
    name: str,
    plan: IbisPlan | Table,
    ast: Expression,
) -> ViewNode:
    required = _required_udfs_from_ast(ast, build_ctx.snapshot)
    metadata = _metadata_with_required_udfs(None, required)
    schema = _arrow_schema_from_ibis(plan)
    return ViewNode(
        name=name,
        deps=_deps_from_ast(ast),
        builder=build_ctx.builder_from_plan(plan, label=name),
        contract_builder=_contract_builder(
            name,
            metadata_spec=metadata,
            expected_schema=schema,
            enforce_columns=True,
        ),
        required_udfs=required,
        sqlglot_ast=ast,
        ibis_expr=plan.expr if isinstance(plan, IbisPlan) else plan,
    )


def _alias_nodes(nodes: Sequence[ViewNode], *, build_ctx: ViewBuildContext) -> list[ViewNode]:
    registered = {node.name for node in nodes}
    alias_nodes: list[ViewNode] = []
    for name in registered:
        alias = _resolve_alias(name)
        if alias == name or alias in registered:
            continue
        expr = select_all(name)
        ibis_expr = _ibis_expr_from_sqlglot(build_ctx, expr, label=alias)
        alias_nodes.append(
            ViewNode(
                name=alias,
                deps=_deps_from_ast(expr),
                builder=_alias_builder(source=name),
                contract_builder=_contract_builder(alias, metadata_spec=None),
                required_udfs=(),
                sqlglot_ast=expr,
                ibis_expr=ibis_expr,
            )
        )
    return alias_nodes


def _resolve_alias(name: str) -> str:
    from normalize.dataset_specs import dataset_alias

    try:
        return dataset_alias(name)
    except KeyError:
        pass
    return _strip_version(name)


def _strip_version(name: str) -> str:
    base, sep, suffix = name.rpartition("_v")
    if sep and suffix.isdigit():
        return base
    return name


def _ibis_backend(ctx: SessionContext, *, snapshot: Mapping[str, object]) -> BaseBackend:
    validate_rust_udf_snapshot(snapshot)
    backend = ibis.datafusion.connect(ctx)
    from ibis_engine.builtin_udfs import register_ibis_udf_snapshot

    register_ibis_udf_snapshot(snapshot)
    return backend


def _catalog_for_ctx(ctx: SessionContext, backend: BaseBackend) -> IbisPlanCatalog:
    names = table_names_snapshot(ctx)
    tables: dict[str, IbisPlanSource] = {name: ViewReference(name) for name in names}
    if not tables:
        msg = "No DataFusion tables registered; cannot build view catalog."
        raise ValueError(msg)
    return IbisPlanCatalog(backend=backend, tables=tables)


def _plan_to_dataframe(
    ctx: SessionContext,
    plan: IbisPlan | Table,
    facade: DataFusionExecutionFacade,
) -> DataFrame:
    _ = ctx
    expr = plan.expr if isinstance(plan, IbisPlan) else plan
    compiled = facade.compile(expr)
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


__all__ = ["ViewBuildContext", "view_graph_nodes"]
