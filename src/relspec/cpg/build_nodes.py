"""Build CPG node tables from extraction outputs."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import cast

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
from cpg.registry import CpgRegistry, default_cpg_registry
from cpg.spec_tables import node_plan_specs_from_table
from cpg.specs import NodePlanSpec
from cpg.table_utils import align_table_to_schema, assert_schema_metadata
from datafusion_engine.query_fragments import SqlFragment
from datafusion_engine.runtime import AdapterExecutionPolicy
from engine.materialize import resolve_prefer_reader
from engine.plan_policy import ExecutionSurfacePolicy
from engine.session import EngineSession
from ibis_engine.execution import IbisExecutionContext, materialize_ibis_plan, stream_ibis_plan
from ibis_engine.plan import IbisPlan
from ibis_engine.scan_io import DatasetSource
from ibis_engine.schema_utils import ibis_null_literal
from ibis_engine.sources import (
    SourceToIbisOptions,
    namespace_recorder_from_ctx,
    register_ibis_view,
    source_to_ibis,
)
from relspec.rules.handlers.cpg_emit import NodeEmitRuleHandler
from schema_spec.system import DatasetSpec


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


def _node_plan_specs(
    node_spec_table: pa.Table | None,
    *,
    registry: CpgRegistry,
) -> tuple[NodePlanSpec, ...]:
    table = node_spec_table or registry.node_plan_spec_table
    return node_plan_specs_from_table(table)


def _resolve_node_build_context(
    ctx: ExecutionContext,
    *,
    config: NodeBuildConfig | None,
) -> NodeBuildContext:
    resolved = config or NodeBuildConfig()
    registry = resolved.registry or default_cpg_registry()
    options = resolved.options or NodeBuildOptions()
    nodes_spec = registry.nodes_spec()
    nodes_schema = nodes_spec.schema()
    if resolved.ibis_backend is None:
        msg = "Ibis backend is required for CPG node builds."
        raise ValueError(msg)
    return NodeBuildContext(
        ctx=ctx,
        config=resolved,
        registry=registry,
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
            raise ValueError(msg)
        return ctx
    if ctx is not None and ctx is not session.ctx:
        msg = "Provided ctx must match session.ctx when session is supplied."
        raise ValueError(msg)
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
    include_name_ref_nodes: bool = True
    include_import_alias_nodes: bool = True
    include_callsite_nodes: bool = True
    include_def_nodes: bool = True
    include_symbol_nodes: bool = True
    include_qname_nodes: bool = True
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
    node_spec_table: pa.Table | None = None
    registry: CpgRegistry | None = None
    execution_policy: AdapterExecutionPolicy | None = None
    ibis_backend: BaseBackend | None = None
    surface_policy: ExecutionSurfacePolicy | None = None


@dataclass(frozen=True)
class NodeBuildContext:
    """Resolved context for node builds."""

    ctx: ExecutionContext
    config: NodeBuildConfig
    registry: CpgRegistry
    options: NodeBuildOptions
    nodes_spec: DatasetSpec
    nodes_schema: SchemaLike


@dataclass(frozen=True)
class NodeEmitIbisContext:
    """Inputs required to emit node plans with Ibis."""

    ctx: ExecutionContext
    inputs: NodeInputTables
    options: NodeBuildOptions
    node_spec_table: pa.Table | None
    registry: CpgRegistry
    backend: BaseBackend


@dataclass(frozen=True)
class IbisMaterializeContext:
    """Execution context for Ibis materialization."""

    execution: IbisExecutionContext
    prefer_reader: bool


@dataclass(frozen=True)
class NodeInputTables:
    """Bundle of input tables for node construction."""

    repo_files: TableLike | DatasetSource | SqlFragment | None = None
    cst_name_refs: TableLike | DatasetSource | SqlFragment | None = None
    cst_imports: TableLike | DatasetSource | SqlFragment | None = None
    cst_callsites: TableLike | DatasetSource | SqlFragment | None = None
    cst_defs: TableLike | DatasetSource | SqlFragment | None = None
    dim_qualified_names: TableLike | DatasetSource | SqlFragment | None = None
    scip_symbol_information: TableLike | DatasetSource | SqlFragment | None = None
    scip_occurrences: TableLike | DatasetSource | SqlFragment | None = None
    scip_external_symbol_information: TableLike | DatasetSource | SqlFragment | None = None
    scip_symbol_relationships: TableLike | DatasetSource | SqlFragment | None = None
    ts_nodes: TableLike | DatasetSource | SqlFragment | None = None
    ts_errors: TableLike | DatasetSource | SqlFragment | None = None
    ts_missing: TableLike | DatasetSource | SqlFragment | None = None
    type_exprs_norm: TableLike | DatasetSource | SqlFragment | None = None
    types_norm: TableLike | DatasetSource | SqlFragment | None = None
    diagnostics_norm: TableLike | DatasetSource | SqlFragment | None = None
    rt_objects: TableLike | DatasetSource | SqlFragment | None = None
    rt_signatures: TableLike | DatasetSource | SqlFragment | None = None
    rt_signature_params: TableLike | DatasetSource | SqlFragment | None = None
    rt_members: TableLike | DatasetSource | SqlFragment | None = None


def _materialize_reader(value: TableLike | RecordBatchReaderLike) -> TableLike:
    if isinstance(value, RecordBatchReaderLike):
        return value.read_all()
    return value


def _source_to_ibis_plan(
    source: TableLike | DatasetSource | SqlFragment,
    *,
    _ctx: ExecutionContext,
    backend: BaseBackend,
    name: str,
) -> IbisPlan:
    if isinstance(source, SqlFragment):
        expr = _sql_fragment_expr(backend, fragment=source)
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


def _sql_fragment_expr(backend: BaseBackend, *, fragment: SqlFragment) -> Table:
    sql_method = getattr(backend, "sql", None)
    if not callable(sql_method):
        msg = "Ibis backend does not support raw SQL fragments."
        raise TypeError(msg)
    return cast("Table", sql_method(fragment.sql))


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
        "cst_name_refs": inputs.cst_name_refs,
        "cst_imports": inputs.cst_imports,
        "cst_callsites": inputs.cst_callsites,
        "cst_defs": inputs.cst_defs,
        "dim_qualified_names": inputs.dim_qualified_names,
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


def _empty_nodes_ibis(schema: SchemaLike) -> IbisPlan:
    table = empty_table(schema)
    return IbisPlan(expr=ibis.memtable(table), ordering=Ordering.unordered())


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
    for spec in _node_plan_specs(context.node_spec_table, registry=context.registry):
        enabled = getattr(context.options, spec.option_flag, None)
        if enabled is None:
            msg = f"Unknown option flag: {spec.option_flag}"
            raise ValueError(msg)
        if not enabled:
            continue
        plan = catalog.get(spec.table_ref)
        if plan is None:
            continue
        if spec.preprocessor_id is not None:
            msg = f"Node preprocessor {spec.preprocessor_id!r} is not supported for Ibis."
            raise ValueError(msg)
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
        raise ValueError(msg)
    policy = config.surface_policy or ExecutionSurfacePolicy()
    prefer_reader = resolve_prefer_reader(ctx=context.ctx, policy=policy)
    emit_context = NodeEmitIbisContext(
        ctx=context.ctx,
        inputs=config.inputs or NodeInputTables(),
        options=context.options,
        node_spec_table=config.node_spec_table,
        registry=context.registry,
        backend=backend,
    )
    emitted = _node_emitted_plans_ibis(emit_context)
    parts = [plan for _, plan in emitted]
    raw_plan = _empty_nodes_ibis(context.nodes_schema) if not parts else _union_nodes_ibis(parts)
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
        name=f"{context.nodes_spec.name}_raw",
        context=materialize_context,
    )
    raw = align_table_to_schema(
        raw,
        schema=context.nodes_schema,
        safe_cast=context.ctx.safe_cast,
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
    ValueError
        Raised when the Ibis backend is unavailable.
    """
    exec_ctx = _resolve_ctx_for_session(ctx, session)
    config = _merge_node_config(config, session)
    build_context = _resolve_node_build_context(exec_ctx, config=config)
    backend = build_context.config.ibis_backend
    if backend is None:
        msg = "Ibis backend is required when building nodes with Ibis."
        raise ValueError(msg)
    emitted = _node_emitted_plans_ibis(
        NodeEmitIbisContext(
            ctx=exec_ctx,
            inputs=build_context.config.inputs or NodeInputTables(),
            options=build_context.options,
            node_spec_table=build_context.config.node_spec_table,
            registry=build_context.registry,
            backend=backend,
        )
    )
    parts = [plan for _, plan in emitted]
    if not parts:
        return _empty_nodes_ibis(build_context.nodes_schema)
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
