"""Build CPG properties tables from extracted metadata."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import cast

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import IntegerValue, Table, Value

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike, concat_tables
from arrowdsl.core.ordering import Ordering
from arrowdsl.schema.schema import empty_table
from cpg.constants import (
    ROLE_FLAG_SPECS,
    CpgBuildArtifacts,
    concat_quality_tables,
    quality_from_ids,
)
from cpg.spec_registry import prop_table_specs
from cpg.specs import PropFieldSpec, PropTableSpec, filter_fields, resolve_prop_include
from cpg.table_utils import align_table_to_schema, assert_schema_metadata
from datafusion_engine.nested_tables import ViewReference
from datafusion_engine.runtime import (
    AdapterExecutionPolicy,
    dataset_schema_from_context,
    dataset_spec_from_context,
)
from engine.materialize import resolve_prefer_reader
from engine.plan_policy import ExecutionSurfacePolicy
from engine.session import EngineSession
from ibis_engine.execution import IbisExecutionContext, materialize_ibis_plan, stream_ibis_plan
from ibis_engine.plan import IbisPlan
from ibis_engine.scan_io import DatasetSource
from ibis_engine.sources import (
    SourceToIbisOptions,
    namespace_recorder_from_ctx,
    register_ibis_table,
    register_ibis_view,
    source_to_ibis,
)
from relspec.cpg.emit_props_ibis import (
    filter_prop_fields,
)
from relspec.rules.handlers.cpg_emit import PropEmitRuleHandler
from schema_spec.system import DatasetSpec


def _prop_table_specs() -> tuple[PropTableSpec, ...]:
    return prop_table_specs()


def _resolve_props_build_context(
    ctx: ExecutionContext,
    *,
    config: PropsBuildConfig | None,
) -> PropsBuildContext:
    resolved = config or PropsBuildConfig()
    options = resolved.options or PropsBuildOptions()
    if options.merge_json_props and not options.include_heavy_json_props:
        msg = "merge_json_props requires include_heavy_json_props to be enabled."
        raise ValueError(msg)
    props_spec = dataset_spec_from_context("cpg_props_v1")
    props_schema = dataset_schema_from_context("cpg_props_v1")
    if resolved.ibis_backend is None:
        msg = "Ibis backend is required for CPG property builds."
        raise ValueError(msg)
    return PropsBuildContext(
        ctx=ctx,
        config=resolved,
        options=options,
        props_spec=props_spec,
        props_schema=props_schema,
    )


def _resolve_ctx_for_session(
    ctx: ExecutionContext | None,
    session: EngineSession | None,
) -> ExecutionContext:
    if session is None:
        if ctx is None:
            msg = "Either ctx or session must be provided for CPG props builds."
            raise ValueError(msg)
        return ctx
    if ctx is not None and ctx is not session.ctx:
        msg = "Provided ctx must match session.ctx when session is supplied."
        raise ValueError(msg)
    return session.ctx


def _merge_props_config(
    config: PropsBuildConfig | None,
    session: EngineSession | None,
) -> PropsBuildConfig | None:
    if session is None:
        return config
    resolved = config or PropsBuildConfig()
    if resolved.ibis_backend is None:
        resolved = replace(resolved, ibis_backend=session.ibis_backend)
    if resolved.surface_policy is None:
        resolved = replace(resolved, surface_policy=session.surface_policy)
    return resolved


@dataclass(frozen=True)
class PropsBuildOptions:
    """Configure which property families are emitted."""

    include_node_props: bool = True
    include_edge_props: bool = True
    include_heavy_json_props: bool = False
    merge_json_props: bool = False


@dataclass(frozen=True)
class PropsBuildConfig:
    """Configuration for property build entrypoints."""

    inputs: PropsInputTables | None = None
    options: PropsBuildOptions | None = None
    execution_policy: AdapterExecutionPolicy | None = None
    ibis_backend: BaseBackend | None = None
    surface_policy: ExecutionSurfacePolicy | None = None


@dataclass(frozen=True)
class PropsBuildContext:
    """Resolved context for property builds."""

    ctx: ExecutionContext
    config: PropsBuildConfig
    options: PropsBuildOptions
    props_spec: DatasetSpec
    props_schema: pa.Schema


@dataclass(frozen=True)
class PropsEmitIbisContext:
    """Inputs required to emit property plans with Ibis."""

    ctx: ExecutionContext
    inputs: PropsInputTables
    options: PropsBuildOptions
    backend: BaseBackend
    props_spec: DatasetSpec
    props_json_schema: pa.Schema


@dataclass(frozen=True)
class IbisMaterializeContext:
    """Execution context for Ibis materialization."""

    execution: IbisExecutionContext
    prefer_reader: bool


@dataclass(frozen=True)
class PropsInputTables:
    """Bundle of input tables for property extraction."""

    repo_files: TableLike | DatasetSource | ViewReference | None = None
    cst_refs: TableLike | DatasetSource | ViewReference | None = None
    cst_imports: TableLike | DatasetSource | ViewReference | None = None
    cst_callsites: TableLike | DatasetSource | ViewReference | None = None
    cst_defs: TableLike | DatasetSource | ViewReference | None = None
    dim_qualified_names: TableLike | DatasetSource | ViewReference | None = None
    scip_symbol_information: TableLike | DatasetSource | ViewReference | None = None
    scip_occurrences: TableLike | DatasetSource | ViewReference | None = None
    scip_external_symbol_information: TableLike | DatasetSource | ViewReference | None = None
    symtable_scopes: TableLike | DatasetSource | ViewReference | None = None
    symtable_symbols: TableLike | DatasetSource | ViewReference | None = None
    symtable_bindings: TableLike | DatasetSource | ViewReference | None = None
    symtable_def_sites: TableLike | DatasetSource | ViewReference | None = None
    symtable_use_sites: TableLike | DatasetSource | ViewReference | None = None
    symtable_type_params: TableLike | DatasetSource | ViewReference | None = None
    ts_nodes: TableLike | DatasetSource | ViewReference | None = None
    ts_errors: TableLike | DatasetSource | ViewReference | None = None
    ts_missing: TableLike | DatasetSource | ViewReference | None = None
    type_exprs_norm: TableLike | DatasetSource | ViewReference | None = None
    types_norm: TableLike | DatasetSource | ViewReference | None = None
    diagnostics_norm: TableLike | DatasetSource | ViewReference | None = None
    rt_objects: TableLike | DatasetSource | ViewReference | None = None
    rt_signatures: TableLike | DatasetSource | ViewReference | None = None
    rt_signature_params: TableLike | DatasetSource | ViewReference | None = None
    rt_members: TableLike | DatasetSource | ViewReference | None = None
    cpg_edges: TableLike | DatasetSource | ViewReference | None = None


def _materialize_reader(value: TableLike | RecordBatchReaderLike) -> TableLike:
    if isinstance(value, RecordBatchReaderLike):
        return value.read_all()
    return value


def _source_to_ibis_plan(
    source: TableLike | DatasetSource | ViewReference,
    *,
    _ctx: ExecutionContext,
    backend: BaseBackend,
    name: str,
) -> IbisPlan:
    if isinstance(source, ViewReference):
        expr = _view_reference_expr(backend, fragment=source)
        return IbisPlan(expr=expr, ordering=Ordering.unordered())
    if isinstance(source, DatasetSource):
        msg = f"DatasetSource {name!r} must be materialized before CPG property builds."
        raise TypeError(msg)
    table = source
    return source_to_ibis(
        table,
        options=SourceToIbisOptions(
            backend=backend,
            name=name,
            ordering=Ordering.unordered(),
            namespace_recorder=namespace_recorder_from_ctx(_ctx),
        ),
    )


def _view_reference_expr(backend: BaseBackend, *, fragment: ViewReference) -> Table:
    return backend.table(fragment.name)


def _cst_defs_norm_ibis(cst_defs: IbisPlan) -> IbisPlan:
    expr = cst_defs.expr
    if "def_kind_norm" in expr.columns:
        return cst_defs
    if "def_kind" in expr.columns and "kind" in expr.columns:
        def_kind_norm = ibis.coalesce(expr["def_kind"], expr["kind"])
    elif "def_kind" in expr.columns:
        def_kind_norm = expr["def_kind"]
    elif "kind" in expr.columns:
        def_kind_norm = expr["kind"]
    else:
        return cst_defs
    output = expr.mutate(def_kind_norm=def_kind_norm.cast("string"))
    return IbisPlan(expr=output, ordering=cst_defs.ordering)


def _scip_role_flags_ibis(scip_occurrences: IbisPlan) -> IbisPlan | None:
    expr = scip_occurrences.expr
    if "symbol" not in expr.columns:
        return None
    flag_names = [flag_name for flag_name, _, _ in ROLE_FLAG_SPECS]
    if all(name in expr.columns for name in flag_names):
        projected: dict[str, Value] = {
            "symbol": expr["symbol"].cast("string"),
        }
        for flag_name in flag_names:
            projected[flag_name] = expr[flag_name].cast("int64")
        base = expr.select(**projected)
        aggregates = {name: base[name].max() for name in flag_names}
        grouped = base.group_by("symbol").aggregate(**aggregates)
        return IbisPlan(expr=grouped, ordering=Ordering.unordered())
    if "symbol_roles" not in expr.columns:
        return None
    symbol_roles = cast("IntegerValue", expr["symbol_roles"].cast("int64"))
    projected: dict[str, Value] = {
        "symbol": expr["symbol"].cast("string"),
    }
    for flag_name, mask, _ in ROLE_FLAG_SPECS:
        mask_value = cast("IntegerValue", ibis.literal(mask).cast("int64"))
        hit = (symbol_roles & mask_value) != ibis.literal(0)
        projected[flag_name] = ibis.ifelse(hit, ibis.literal(1), ibis.literal(0))
    base = expr.select(**projected)
    aggregates = {name: base[name].max() for name in projected if name != "symbol"}
    grouped = base.group_by("symbol").aggregate(**aggregates)
    return IbisPlan(expr=grouped, ordering=Ordering.unordered())


def _ibis_prop_tables(
    inputs: PropsInputTables,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
) -> dict[str, IbisPlan]:
    tables: dict[str, IbisPlan] = {}
    for name, table in {
        "repo_files": inputs.repo_files,
        "cst_refs": inputs.cst_refs,
        "cst_imports": inputs.cst_imports,
        "cst_callsites": inputs.cst_callsites,
        "dim_qualified_names": inputs.dim_qualified_names,
        "scip_symbol_information": inputs.scip_symbol_information,
        "scip_external_symbol_information": inputs.scip_external_symbol_information,
        "symtable_scopes": inputs.symtable_scopes,
        "symtable_symbols": inputs.symtable_symbols,
        "symtable_bindings": inputs.symtable_bindings,
        "symtable_def_sites": inputs.symtable_def_sites,
        "symtable_use_sites": inputs.symtable_use_sites,
        "symtable_type_params": inputs.symtable_type_params,
        "ts_nodes": inputs.ts_nodes,
        "ts_errors": inputs.ts_errors,
        "ts_missing": inputs.ts_missing,
        "type_exprs_norm": inputs.type_exprs_norm,
        "types_norm": inputs.types_norm,
        "diagnostics_norm": inputs.diagnostics_norm,
        "rt_objects": inputs.rt_objects,
        "rt_signatures": inputs.rt_signatures,
        "rt_signature_params": inputs.rt_signature_params,
        "rt_members": inputs.rt_members,
        "cpg_edges": inputs.cpg_edges,
    }.items():
        if table is None:
            continue
        tables[name] = _source_to_ibis_plan(table, _ctx=ctx, backend=backend, name=name)

    if inputs.cst_defs is not None:
        cst_defs = _source_to_ibis_plan(
            inputs.cst_defs,
            _ctx=ctx,
            backend=backend,
            name="cst_defs",
        )
        tables["cst_defs"] = cst_defs
        tables["cst_defs_norm"] = _cst_defs_norm_ibis(cst_defs)

    if inputs.scip_occurrences is not None:
        scip_occurrences = _source_to_ibis_plan(
            inputs.scip_occurrences,
            _ctx=ctx,
            backend=backend,
            name="scip_occurrences",
        )
        tables["scip_occurrences"] = scip_occurrences
        scip_role_flags = _scip_role_flags_ibis(scip_occurrences)
        if scip_role_flags is not None:
            tables["scip_role_flags"] = scip_role_flags

    return tables


def _empty_props_ibis(schema: pa.Schema, *, backend: BaseBackend) -> IbisPlan:
    table = empty_table(schema)
    return register_ibis_table(
        table,
        options=SourceToIbisOptions(
            backend=backend,
            name=None,
            ordering=Ordering.unordered(),
        ),
    )


def _union_props_ibis(parts: list[IbisPlan]) -> IbisPlan:
    combined = parts[0].expr
    for part in parts[1:]:
        combined = combined.union(part.expr)
    return IbisPlan(expr=combined, ordering=Ordering.unordered())


def _materialize_ibis_plan(
    plan: IbisPlan,
    *,
    name: str,
    context: IbisMaterializeContext,
) -> TableLike:
    backend = context.execution.ibis_backend
    if backend is None:
        msg = "Ibis backend is required for Ibis property materialization."
        raise ValueError(msg)
    registered = register_ibis_view(
        plan.expr,
        options=SourceToIbisOptions(
            backend=backend,
            name=name,
            ordering=plan.ordering,
        ),
    )
    if context.prefer_reader:
        return _materialize_reader(stream_ibis_plan(registered, execution=context.execution))
    return materialize_ibis_plan(registered, execution=context.execution)


def _should_emit_prop_spec(spec: PropTableSpec, options: PropsBuildOptions) -> bool:
    enabled = getattr(options, spec.option_flag, None)
    if enabled is None:
        msg = f"Unknown option flag: {spec.option_flag}"
        raise ValueError(msg)
    if not enabled:
        return False
    include_if = resolve_prop_include(spec.include_if_id)
    return include_if is None or include_if(options)


def _emit_prop_spec_plans(
    plan: IbisPlan,
    *,
    spec: PropTableSpec,
    schema_version: int | None,
    options: PropsBuildOptions,
    props_json_schema: pa.Schema,
) -> tuple[list[IbisPlan], list[IbisPlan]]:
    filtered_fields = filter_fields(spec.fields, options=options)
    if not filtered_fields and spec.node_kind is None:
        return [], []
    fast_fields = filter_prop_fields(filtered_fields, json_mode=False)
    fast_plans: list[IbisPlan] = []
    handler = PropEmitRuleHandler()
    if fast_fields or spec.node_kind is not None:
        fast_plans = handler.compile_fast_ibis(
            plan,
            spec=spec,
            fields=fast_fields,
            schema_version=schema_version,
        )
    json_plans: list[IbisPlan] = []
    if options.include_heavy_json_props:
        json_fields = filter_prop_fields(filtered_fields, json_mode=True)
        if json_fields:
            json_plans = handler.compile_json_ibis(
                plan,
                spec=spec,
                fields=json_fields,
                schema_version=schema_version,
                schema=props_json_schema,
            )
    return fast_plans, json_plans


def _prop_emitted_plans_ibis(
    context: PropsEmitIbisContext,
) -> tuple[
    list[tuple[PropTableSpec, list[IbisPlan]]],
    list[tuple[PropTableSpec, list[IbisPlan]]],
]:
    props_spec = context.props_spec
    props_schema = props_spec.schema()
    schema_version = _props_schema_version(props_schema, props_spec)
    catalog = _ibis_prop_tables(context.inputs, ctx=context.ctx, backend=context.backend)
    emitted_fast: list[tuple[PropTableSpec, list[IbisPlan]]] = []
    emitted_json: list[tuple[PropTableSpec, list[IbisPlan]]] = []
    for spec in _prop_table_specs():
        if not _should_emit_prop_spec(spec, context.options):
            continue
        plan = catalog.get(spec.table_ref)
        if plan is None:
            continue
        fast_plans, json_plans = _emit_prop_spec_plans(
            plan,
            spec=spec,
            schema_version=schema_version,
            options=context.options,
            props_json_schema=context.props_json_schema,
        )
        if fast_plans:
            emitted_fast.append((spec, fast_plans))
        if json_plans:
            emitted_json.append((spec, json_plans))
    return emitted_fast, emitted_json


def _ibis_materialize_named(
    plan: IbisPlan,
    *,
    name: str,
    context: IbisMaterializeContext,
) -> TableLike:
    return _materialize_ibis_plan(plan, name=name, context=context)


def _ibis_props_quality_tables(
    raw: TableLike,
    emitted_fast: list[tuple[PropTableSpec, list[IbisPlan]]],
    *,
    context: IbisMaterializeContext,
) -> TableLike:
    quality_tables = [
        quality_from_ids(
            raw,
            id_col="entity_id",
            entity_kind="prop",
            issue="invalid_entity_id",
            source_table="cpg_props_raw",
        )
    ]
    for spec, spec_plans in emitted_fast:
        for plan in spec_plans:
            table = _ibis_materialize_named(
                plan,
                name=f"{spec.name}_props_raw",
                context=context,
            )
            quality_tables.append(
                quality_from_ids(
                    table,
                    id_col="entity_id",
                    entity_kind="prop",
                    issue="invalid_entity_id_emitter",
                    source_table=_emit_quality_source(spec),
                )
            )
    return concat_quality_tables(quality_tables)


def _materialize_props_json(
    emitted_json: list[tuple[PropTableSpec, list[IbisPlan]]],
    *,
    props_json_schema: pa.Schema,
    context: IbisMaterializeContext,
) -> TableLike | None:
    json_parts = [plan for _, plans in emitted_json for plan in plans]
    if not json_parts:
        return None
    json_plan = _union_props_ibis(json_parts)
    json_table = _ibis_materialize_named(
        json_plan,
        name="cpg_props_json_raw",
        context=context,
    )
    return align_table_to_schema(
        json_table,
        schema=props_json_schema,
        keep_extra_columns=context.execution.ctx.debug,
    )


def _raw_props_plan_from_emitted(
    emitted_fast: list[tuple[PropTableSpec, list[IbisPlan]]],
    *,
    schema: pa.Schema,
    backend: BaseBackend,
) -> IbisPlan:
    parts = [plan for _, spec_plans in emitted_fast for plan in spec_plans]
    if not parts:
        return _empty_props_ibis(schema, backend=backend)
    return _union_props_ibis(parts)


def _build_cpg_props_ibis(
    context: PropsBuildContext,
) -> CpgBuildArtifacts:
    config = context.config
    backend = config.ibis_backend
    if backend is None:
        msg = "Ibis backend is required when building props with Ibis."
        raise ValueError(msg)
    prefer_reader = resolve_prefer_reader(
        ctx=context.ctx,
        policy=config.surface_policy or ExecutionSurfacePolicy(),
    )
    props_json_schema = _props_json_schema()
    emitted_fast, emitted_json = _prop_emitted_plans_ibis(
        PropsEmitIbisContext(
            ctx=context.ctx,
            inputs=config.inputs or PropsInputTables(),
            options=context.options,
            backend=backend,
            props_spec=context.props_spec,
            props_json_schema=props_json_schema,
        )
    )
    raw_plan = _raw_props_plan_from_emitted(
        emitted_fast,
        schema=context.props_schema,
        backend=backend,
    )
    execution = IbisExecutionContext(
        ctx=context.ctx,
        execution_policy=config.execution_policy,
        ibis_backend=backend,
    )
    materialize_context = IbisMaterializeContext(
        execution=execution,
        prefer_reader=prefer_reader,
    )
    raw = _ibis_materialize_named(
        raw_plan,
        name=f"{context.props_spec.name}_raw",
        context=materialize_context,
    )
    raw = align_table_to_schema(
        raw,
        schema=context.props_schema,
        keep_extra_columns=context.ctx.debug,
    )
    extra_outputs: dict[str, TableLike] = {}
    json_table = _materialize_props_json(
        emitted_json,
        props_json_schema=props_json_schema,
        context=materialize_context,
    )
    if json_table is not None:
        extra_outputs["cpg_props_json"] = json_table
    if context.options.merge_json_props and json_table is not None:
        raw = align_table_to_schema(
            concat_tables([raw, json_table], promote_options="default"),
            schema=context.props_schema,
            keep_extra_columns=context.ctx.debug,
        )
    quality = _ibis_props_quality_tables(
        raw,
        emitted_fast,
        context=materialize_context,
    )
    finalize = context.props_spec.finalize_context(context.ctx).run(raw, ctx=context.ctx)
    if context.ctx.debug:
        assert_schema_metadata(finalize.good, schema=context.props_spec.schema())
    return CpgBuildArtifacts(
        finalize=finalize,
        quality=quality,
        pipeline_breakers=(),
        extra_outputs=extra_outputs,
    )


def _build_cpg_props_raw_ibis(
    context: PropsBuildContext,
) -> IbisPlan:
    backend = context.config.ibis_backend
    if backend is None:
        msg = "Ibis backend is required when building props with Ibis."
        raise ValueError(msg)
    props_json_schema = _props_json_schema()
    emitted_fast, _ = _prop_emitted_plans_ibis(
        PropsEmitIbisContext(
            ctx=context.ctx,
            inputs=context.config.inputs or PropsInputTables(),
            options=context.options,
            backend=backend,
            props_spec=context.props_spec,
            props_json_schema=props_json_schema,
        )
    )
    parts = [plan for _, spec_plans in emitted_fast for plan in spec_plans]
    if not parts:
        return _empty_props_ibis(context.props_schema, backend=backend)
    return _union_props_ibis(parts)


def _props_schema_version(props_schema: pa.Schema, props_spec: DatasetSpec) -> int | None:
    if "schema_version" in props_schema.names:
        return props_spec.table_spec.version
    return None


def _props_json_schema() -> pa.Schema:
    return dataset_schema_from_context("cpg_props_json_v1")


def _updated_prop_spec(
    spec: PropTableSpec,
    *,
    filtered_fields: list[PropFieldSpec],
) -> PropTableSpec:
    if filtered_fields == list(spec.fields):
        return spec
    return replace(spec, fields=tuple(filtered_fields))


def _emit_quality_source(spec: PropTableSpec) -> str:
    id_cols = ",".join(spec.id_cols)
    if id_cols:
        return f"{spec.name}:{spec.table_ref}[{id_cols}]"
    return f"{spec.name}:{spec.table_ref}"


def build_cpg_props_raw(
    *,
    ctx: ExecutionContext | None = None,
    session: EngineSession | None = None,
    config: PropsBuildConfig | None = None,
) -> IbisPlan:
    """Build CPG properties as a plan without finalization.

    Parameters
    ----------
    ctx:
        Execution context for plan evaluation.
    session:
        Optional engine session supplying context and backends.
    config:
        Property build configuration bundle.

    Returns
    -------
    IbisPlan
        Plan producing the raw properties table.
    """
    exec_ctx = _resolve_ctx_for_session(ctx, session)
    config = _merge_props_config(config, session)
    build_context = _resolve_props_build_context(exec_ctx, config=config)
    return _build_cpg_props_raw_ibis(build_context)


def build_cpg_props(
    *,
    ctx: ExecutionContext | None = None,
    session: EngineSession | None = None,
    config: PropsBuildConfig | None = None,
) -> CpgBuildArtifacts:
    """Build and finalize CPG properties with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalize result plus quality table.
    """
    exec_ctx = _resolve_ctx_for_session(ctx, session)
    config = _merge_props_config(config, session)
    build_context = _resolve_props_build_context(exec_ctx, config=config)
    return _build_cpg_props_ibis(build_context)
