"""Build CPG node tables from extraction outputs."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import StringValue, Table

from arrowdsl.core.array_iter import iter_array_values
from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
)
from arrowdsl.core.ordering import Ordering
from arrowdsl.schema.build import const_array, set_or_append_column, table_from_arrays
from arrowdsl.schema.metadata import normalize_dictionaries
from arrowdsl.schema.schema import empty_table
from cpg.constants import CpgBuildArtifacts, concat_quality_tables, quality_from_ids
from cpg.spec_registry import node_plan_specs
from cpg.specs import NodePlanSpec
from datafusion_engine.nested_tables import ViewReference
from datafusion_engine.runtime import (
    AdapterExecutionPolicy,
    align_table_to_schema,
    assert_schema_metadata,
    dataset_schema_from_context,
    dataset_spec_from_context,
)
from engine.materialize_pipeline import resolve_prefer_reader
from engine.plan_policy import ExecutionSurfacePolicy
from engine.session import EngineSession
from ibis_engine.execution import materialize_ibis_plan, stream_ibis_plan
from ibis_engine.execution_factory import ibis_execution_from_ctx
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import ibis_null_literal
from ibis_engine.sources import (
    DatasetSource,
    SourceToIbisOptions,
    namespace_recorder_from_ctx,
    register_ibis_table,
    register_ibis_view,
    source_to_ibis,
)
from relspec.errors import RelspecValidationError
from relspec.rules.handlers.cpg_emit import NodeEmitRuleHandler
from schema_spec.system import DatasetSpec

if TYPE_CHECKING:
    from ibis_engine.execution import IbisExecutionContext


def _file_span_arrays(
    n: int,
    size: ChunkedArrayLike | None,
    *,
    use_size_bytes: bool,
) -> tuple[ArrayLike, ArrayLike]:
    if use_size_bytes and size is not None:
        bends: list[int | None] = []
        for value in iter_array_values(size):
            if isinstance(value, bool):
                bends.append(None)
            elif isinstance(value, int):
                bends.append(value)
            elif (isinstance(value, float) and value.is_integer()) or (
                isinstance(value, str) and value.isdigit()
            ):
                bends.append(int(value))
            else:
                bends.append(None)
        return const_array(n, 0, dtype=pa.int64()), pa.array(bends, type=pa.int64())
    return const_array(n, None, dtype=pa.int64()), const_array(n, None, dtype=pa.int64())


def _file_nodes_table(
    repo_files: TableLike | None,
    *,
    use_size_bytes: bool,
) -> TableLike | None:
    if repo_files is None or repo_files.num_rows == 0:
        return None
    if "file_id" not in repo_files.column_names or "path" not in repo_files.column_names:
        return None
    n = repo_files.num_rows
    size = repo_files["size_bytes"] if "size_bytes" in repo_files.column_names else None
    bstart, bend = _file_span_arrays(n, size, use_size_bytes=use_size_bytes)
    table = set_or_append_column(repo_files, "bstart", bstart)
    return set_or_append_column(table, "bend", bend)


def _collect_symbols(table: TableLike | None, *, column: str = "symbol") -> set[str]:
    if table is None or column not in table.column_names:
        return set()
    return {str(sym) for sym in iter_array_values(table[column]) if sym}


def _symbol_nodes_table(
    scip_symbol_information: TableLike | None,
    scip_occurrences: TableLike | None,
    scip_external_symbol_information: TableLike | None,
    scip_symbol_relationships: TableLike | None,
) -> TableLike | None:
    symbols = _collect_symbols(scip_symbol_information)
    symbols.update(_collect_symbols(scip_occurrences))
    symbols.update(_collect_symbols(scip_external_symbol_information))
    symbols.update(_collect_symbols(scip_symbol_relationships))
    symbols.update(_collect_symbols(scip_symbol_relationships, column="related_symbol"))
    if not symbols:
        return None
    uniq = sorted(symbols)
    schema = pa.schema([pa.field("symbol", pa.string())])
    columns = {"symbol": pa.array(uniq, type=pa.string())}
    table = table_from_arrays(schema, columns=columns, num_rows=len(uniq))
    return normalize_dictionaries(table)


def _file_nodes_ibis(
    repo_files: IbisPlan,
    *,
    use_size_bytes: bool,
) -> IbisPlan | None:
    expr = repo_files.expr
    if "file_id" not in expr.columns or "path" not in expr.columns:
        return None
    bstart = ibis_null_literal(pa.int64())
    bend = ibis_null_literal(pa.int64())
    if use_size_bytes and "size_bytes" in expr.columns:
        size_expr = expr["size_bytes"]
        size_type = size_expr.type()
        if size_type.is_integer() or size_type.is_floating():
            bstart = ibis.literal(0).cast("int64")
            bend = size_expr.cast("int64")
    output = expr.mutate(bstart=bstart, bend=bend)
    return IbisPlan(expr=output, ordering=Ordering.unordered())


def _symbol_nodes_ibis(
    scip_symbol_information: IbisPlan | None,
    scip_occurrences: IbisPlan | None,
    scip_external_symbol_information: IbisPlan | None,
    scip_symbol_relationships: IbisPlan | None,
) -> IbisPlan | None:
    sources: list[Table] = []
    sources.extend(_symbol_sources_ibis(scip_symbol_information, column="symbol"))
    sources.extend(_symbol_sources_ibis(scip_occurrences, column="symbol"))
    sources.extend(_symbol_sources_ibis(scip_external_symbol_information, column="symbol"))
    sources.extend(_symbol_sources_ibis(scip_symbol_relationships, column="symbol"))
    sources.extend(_symbol_sources_ibis(scip_symbol_relationships, column="related_symbol"))
    if not sources:
        return None
    combined = sources[0]
    for source in sources[1:]:
        combined = combined.union(source)
    symbol_text = cast("StringValue", combined.symbol.cast("string"))
    filtered = combined.filter(symbol_text.notnull() & (symbol_text.length() > ibis.literal(0)))
    return IbisPlan(expr=filtered.distinct(), ordering=Ordering.unordered())


def _symbol_sources_ibis(plan: IbisPlan | None, *, column: str) -> list[Table]:
    if plan is None:
        return []
    expr = plan.expr
    if column not in expr.columns:
        return []
    return [expr.select(symbol=expr[column].cast("string"))]


def _emit_quality_source(spec: NodePlanSpec) -> str:
    id_cols = ",".join(spec.emit.id_cols)
    if id_cols:
        return f"{spec.name}:{spec.table_ref}[{id_cols}]"
    return f"{spec.name}:{spec.table_ref}"


def _node_plan_specs() -> tuple[NodePlanSpec, ...]:
    return node_plan_specs()


def _resolve_node_build_context(
    ctx: ExecutionContext,
    *,
    config: NodeBuildConfig | None,
) -> NodeBuildContext:
    resolved = config or NodeBuildConfig()
    options = resolved.options or NodeBuildOptions()
    nodes_spec = dataset_spec_from_context("cpg_nodes_v1")
    nodes_schema = dataset_schema_from_context("cpg_nodes_v1")
    if resolved.ibis_backend is None:
        msg = "Ibis backend is required for CPG node builds."
        raise RelspecValidationError(msg)
    return NodeBuildContext(
        ctx=ctx,
        config=resolved,
        options=options,
        nodes_spec=nodes_spec,
        nodes_schema=nodes_schema,
    )


def _resolve_ctx_for_session(
    ctx: ExecutionContext | None,
    session: EngineSession | None,
) -> ExecutionContext:
    if session is None:
        if ctx is None:
            msg = "Either ctx or session must be provided for CPG node builds."
            raise RelspecValidationError(msg)
        return ctx
    if ctx is not None and ctx is not session.ctx:
        msg = "Provided ctx must match session.ctx when session is supplied."
        raise RelspecValidationError(msg)
    return session.ctx


def _merge_node_config(
    config: NodeBuildConfig | None,
    session: EngineSession | None,
) -> NodeBuildConfig | None:
    if session is None:
        return config
    resolved = config or NodeBuildConfig()
    if resolved.ibis_backend is None:
        resolved = replace(resolved, ibis_backend=session.ibis_backend)
    if resolved.surface_policy is None:
        resolved = replace(resolved, surface_policy=session.surface_policy)
    return resolved


@dataclass(frozen=True)
class NodeBuildOptions:
    """Configure which node families are emitted."""

    include_file_nodes: bool = True
    include_ref_nodes: bool = True
    include_import_alias_nodes: bool = True
    include_callsite_nodes: bool = True
    include_def_nodes: bool = True
    include_symbol_nodes: bool = True
    include_qname_nodes: bool = True
    include_sym_scope_nodes: bool = True
    include_sym_symbol_nodes: bool = True
    include_py_scope_nodes: bool = True
    include_py_binding_nodes: bool = True
    include_py_def_site_nodes: bool = True
    include_py_use_site_nodes: bool = True
    include_type_param_nodes: bool = True
    include_tree_sitter_nodes: bool = True
    include_type_nodes: bool = True
    include_diagnostic_nodes: bool = True
    include_runtime_nodes: bool = True
    file_span_from_size_bytes: bool = True


@dataclass(frozen=True)
class NodeBuildConfig:
    """Configuration for node build entrypoints."""

    inputs: NodeInputTables | None = None
    options: NodeBuildOptions | None = None
    execution_policy: AdapterExecutionPolicy | None = None
    ibis_backend: BaseBackend | None = None
    surface_policy: ExecutionSurfacePolicy | None = None


@dataclass(frozen=True)
class NodeBuildContext:
    """Resolved context for node builds."""

    ctx: ExecutionContext
    config: NodeBuildConfig
    options: NodeBuildOptions
    nodes_spec: DatasetSpec
    nodes_schema: SchemaLike


@dataclass(frozen=True)
class NodeEmitIbisContext:
    """Inputs required to emit node plans with Ibis."""

    ctx: ExecutionContext
    inputs: NodeInputTables
    options: NodeBuildOptions
    backend: BaseBackend


@dataclass(frozen=True)
class IbisMaterializeContext:
    """Execution context for Ibis materialization."""

    execution: IbisExecutionContext
    prefer_reader: bool


@dataclass(frozen=True)
class NodeInputTables:
    """Bundle of input tables for node construction."""

    repo_files: TableLike | DatasetSource | ViewReference | None = None
    cst_refs: TableLike | DatasetSource | ViewReference | None = None
    cst_imports: TableLike | DatasetSource | ViewReference | None = None
    cst_callsites: TableLike | DatasetSource | ViewReference | None = None
    cst_defs: TableLike | DatasetSource | ViewReference | None = None
    dim_qualified_names: TableLike | DatasetSource | ViewReference | None = None
    scip_symbol_information: TableLike | DatasetSource | ViewReference | None = None
    scip_occurrences: TableLike | DatasetSource | ViewReference | None = None
    scip_external_symbol_information: TableLike | DatasetSource | ViewReference | None = None
    scip_symbol_relationships: TableLike | DatasetSource | ViewReference | None = None
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
        msg = f"DatasetSource {name!r} must be materialized before CPG node builds."
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


def _ibis_node_tables(
    inputs: NodeInputTables,
    *,
    options: NodeBuildOptions,
    ctx: ExecutionContext,
    backend: BaseBackend,
) -> dict[str, IbisPlan]:
    tables: dict[str, IbisPlan] = {}
    if inputs.repo_files is not None:
        repo_files = _source_to_ibis_plan(
            inputs.repo_files,
            _ctx=ctx,
            backend=backend,
            name="repo_files",
        )
        tables["repo_files"] = repo_files
        file_nodes = _file_nodes_ibis(
            repo_files,
            use_size_bytes=options.file_span_from_size_bytes,
        )
        if file_nodes is not None:
            tables["repo_files_nodes"] = file_nodes

    for name, table in {
        "cst_refs": inputs.cst_refs,
        "cst_imports": inputs.cst_imports,
        "cst_callsites": inputs.cst_callsites,
        "cst_defs": inputs.cst_defs,
        "dim_qualified_names": inputs.dim_qualified_names,
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
    }.items():
        if table is None:
            continue
        tables[name] = _source_to_ibis_plan(table, _ctx=ctx, backend=backend, name=name)

    scip_symbol_information = (
        None
        if inputs.scip_symbol_information is None
        else _source_to_ibis_plan(
            inputs.scip_symbol_information,
            _ctx=ctx,
            backend=backend,
            name="scip_symbol_information",
        )
    )
    scip_occurrences = (
        None
        if inputs.scip_occurrences is None
        else _source_to_ibis_plan(
            inputs.scip_occurrences,
            _ctx=ctx,
            backend=backend,
            name="scip_occurrences",
        )
    )
    scip_external_symbol_information = (
        None
        if inputs.scip_external_symbol_information is None
        else _source_to_ibis_plan(
            inputs.scip_external_symbol_information,
            _ctx=ctx,
            backend=backend,
            name="scip_external_symbol_information",
        )
    )
    scip_symbol_relationships = (
        None
        if inputs.scip_symbol_relationships is None
        else _source_to_ibis_plan(
            inputs.scip_symbol_relationships,
            _ctx=ctx,
            backend=backend,
            name="scip_symbol_relationships",
        )
    )
    symbol_nodes = _symbol_nodes_ibis(
        scip_symbol_information,
        scip_occurrences,
        scip_external_symbol_information,
        scip_symbol_relationships,
    )
    if symbol_nodes is not None:
        tables["scip_symbols"] = symbol_nodes
    return tables


def _empty_nodes_ibis(schema: SchemaLike, *, backend: BaseBackend) -> IbisPlan:
    table = empty_table(schema)
    return register_ibis_table(
        table,
        options=SourceToIbisOptions(
            backend=backend,
            name=None,
            ordering=Ordering.unordered(),
        ),
    )


def _union_nodes_ibis(parts: list[IbisPlan]) -> IbisPlan:
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
        msg = "Ibis backend is required for Ibis node materialization."
        raise RelspecValidationError(msg)
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


def _node_emitted_plans_ibis(
    context: NodeEmitIbisContext,
) -> list[tuple[NodePlanSpec, IbisPlan]]:
    catalog = _ibis_node_tables(
        context.inputs,
        options=context.options,
        ctx=context.ctx,
        backend=context.backend,
    )
    emitted: list[tuple[NodePlanSpec, IbisPlan]] = []
    handler = NodeEmitRuleHandler()
    for spec in _node_plan_specs():
        enabled = getattr(context.options, spec.option_flag, None)
        if enabled is None:
            msg = f"Unknown option flag: {spec.option_flag}"
            raise RelspecValidationError(msg)
        if not enabled:
            continue
        plan = catalog.get(spec.table_ref)
        if plan is None:
            continue
        if spec.preprocessor_id is not None:
            msg = f"Node preprocessor {spec.preprocessor_id!r} is not supported for Ibis."
            raise RelspecValidationError(msg)
        emitted.append((spec, handler.compile_ibis(plan, spec=spec)))
    return emitted


def _ibis_materialize_named(
    plan: IbisPlan,
    *,
    name: str,
    context: IbisMaterializeContext,
) -> TableLike:
    return _materialize_ibis_plan(plan, name=name, context=context)


def _ibis_quality_tables(
    raw: TableLike,
    emitted: list[tuple[NodePlanSpec, IbisPlan]],
    *,
    context: IbisMaterializeContext,
) -> TableLike:
    quality_tables = [
        quality_from_ids(
            raw,
            id_col="node_id",
            entity_kind="node",
            issue="invalid_node_id",
            source_table="cpg_nodes_raw",
        )
    ]
    for spec, plan in emitted:
        table = _ibis_materialize_named(
            plan,
            name=f"{spec.name}_nodes_raw",
            context=context,
        )
        quality_tables.append(
            quality_from_ids(
                table,
                id_col="node_id",
                entity_kind="node",
                issue="invalid_node_id_emitter",
                source_table=_emit_quality_source(spec),
            )
        )
    return concat_quality_tables(quality_tables)


def _build_cpg_nodes_ibis(
    context: NodeBuildContext,
) -> CpgBuildArtifacts:
    config = context.config
    backend = config.ibis_backend
    if backend is None:
        msg = "Ibis backend is required when building nodes with Ibis."
        raise RelspecValidationError(msg)
    policy = config.surface_policy or ExecutionSurfacePolicy()
    prefer_reader = resolve_prefer_reader(ctx=context.ctx, policy=policy)
    emit_context = NodeEmitIbisContext(
        ctx=context.ctx,
        inputs=config.inputs or NodeInputTables(),
        options=context.options,
        backend=backend,
    )
    emitted = _node_emitted_plans_ibis(emit_context)
    parts = [plan for _, plan in emitted]
    raw_plan = (
        _empty_nodes_ibis(context.nodes_schema, backend=backend)
        if not parts
        else _union_nodes_ibis(parts)
    )
    execution = ibis_execution_from_ctx(
        context.ctx,
        backend=backend,
        execution_policy=config.execution_policy,
    )
    materialize_context = IbisMaterializeContext(
        execution=execution,
        prefer_reader=prefer_reader,
    )
    raw = _ibis_materialize_named(
        raw_plan,
        name=f"{context.nodes_spec.name}_raw",
        context=materialize_context,
    )
    raw = align_table_to_schema(
        raw,
        schema=context.nodes_schema,
        keep_extra_columns=context.ctx.debug,
    )
    quality = _ibis_quality_tables(raw, emitted, context=materialize_context)
    finalize = context.nodes_spec.finalize_context(context.ctx).run(raw, ctx=context.ctx)
    if context.ctx.debug:
        assert_schema_metadata(finalize.good, schema=context.nodes_spec.schema())
    return CpgBuildArtifacts(
        finalize=finalize,
        quality=quality,
        pipeline_breakers=(),
    )


def build_cpg_nodes_raw(
    *,
    ctx: ExecutionContext | None = None,
    session: EngineSession | None = None,
    config: NodeBuildConfig | None = None,
) -> IbisPlan:
    """Build CPG nodes as a plan without finalization.

    Parameters
    ----------
    ctx:
        Execution context for plan evaluation.
    session:
        Optional engine session supplying context and backends.
    config:
        Node build configuration bundle.

    Returns
    -------
    IbisPlan
        Plan producing the raw nodes table.

    Raises
    ------
    RelspecValidationError
        Raised when the Ibis backend is unavailable.
    """
    exec_ctx = _resolve_ctx_for_session(ctx, session)
    config = _merge_node_config(config, session)
    build_context = _resolve_node_build_context(exec_ctx, config=config)
    backend = build_context.config.ibis_backend
    if backend is None:
        msg = "Ibis backend is required when building nodes with Ibis."
        raise RelspecValidationError(msg)
    emitted = _node_emitted_plans_ibis(
        NodeEmitIbisContext(
            ctx=exec_ctx,
            inputs=build_context.config.inputs or NodeInputTables(),
            options=build_context.options,
            backend=backend,
        )
    )
    parts = [plan for _, plan in emitted]
    if not parts:
        return _empty_nodes_ibis(build_context.nodes_schema, backend=backend)
    return _union_nodes_ibis(parts)


def build_cpg_nodes(
    *,
    ctx: ExecutionContext | None = None,
    session: EngineSession | None = None,
    config: NodeBuildConfig | None = None,
) -> CpgBuildArtifacts:
    """Build and finalize CPG nodes with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalize result plus quality table.
    """
    exec_ctx = _resolve_ctx_for_session(ctx, session)
    config = _merge_node_config(config, session)
    build_context = _resolve_node_build_context(exec_ctx, config=config)
    return _build_cpg_nodes_ibis(build_context)
