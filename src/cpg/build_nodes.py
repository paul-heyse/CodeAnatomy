"""Build CPG node tables from extraction outputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import ibis
import pyarrow as pa
import pyarrow.types as patypes
from ibis.backends import BaseBackend
from ibis.expr.types import StringValue, Table

from arrowdsl.core.context import ExecutionContext, Ordering
from arrowdsl.core.ids import iter_array_values
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
)
from arrowdsl.plan.plan import Plan, union_all_plans
from arrowdsl.plan.scan_io import DatasetSource
from arrowdsl.plan_helpers import FinalizePlanAdapterOptions, finalize_plan_adapter
from arrowdsl.schema.build import const_array, set_or_append_column, table_from_arrays
from arrowdsl.schema.metadata import (
    DICT_INDEX_META,
    DICT_ORDERED_META,
    ENCODING_META,
    normalize_dictionaries,
)
from arrowdsl.schema.schema import empty_table
from config import AdapterMode
from cpg.catalog import PlanCatalog, resolve_plan_source
from cpg.constants import (
    CpgBuildArtifacts,
    QualityPlanSpec,
    concat_quality_tables,
    quality_from_ids,
    quality_plan_from_ids,
)
from cpg.emit_nodes import emit_node_plan
from cpg.plan_specs import (
    align_table_to_schema,
    assert_schema_metadata,
    empty_plan,
    ensure_plan,
    finalize_context_for_plan,
    finalize_plan,
)
from cpg.registry import CpgRegistry, default_cpg_registry
from cpg.spec_tables import node_plan_specs_from_table
from cpg.specs import NodePlanSpec, resolve_preprocessor
from datafusion_engine.runtime import AdapterExecutionPolicy
from ibis_engine.plan import IbisPlan
from ibis_engine.plan_bridge import SourceToIbisOptions, register_ibis_view, source_to_ibis
from ibis_engine.schema_utils import ibis_null_literal
from relspec.cpg.emit_nodes_ibis import emit_nodes_ibis
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


def _strip_encoding_metadata(metadata: dict[bytes, bytes] | None) -> dict[bytes, bytes] | None:
    if not metadata:
        return None
    encoding_keys = {
        ENCODING_META.encode("utf-8"),
        DICT_INDEX_META.encode("utf-8"),
        DICT_ORDERED_META.encode("utf-8"),
    }
    stripped = {key: value for key, value in metadata.items() if key not in encoding_keys}
    return stripped or None


def _raw_nodes_schema(nodes_schema: SchemaLike) -> pa.Schema:
    names = ["node_id", "node_kind", "path", "bstart", "bend", "file_id"]
    schema = pa.schema(nodes_schema)
    field_map = {field.name: field for field in schema}
    fields: list[pa.Field] = []
    for name in names:
        field = field_map[name]
        dtype = field.type
        if patypes.is_dictionary(dtype):
            dict_type = cast("pa.DictionaryType", dtype)
            dtype = dict_type.value_type
        fields.append(
            pa.field(
                name,
                dtype,
                nullable=field.nullable,
                metadata=_strip_encoding_metadata(field.metadata),
            )
        )
    return pa.schema(fields)


def _schema_name_diff_lines(expected: pa.Schema, actual: pa.Schema) -> list[str]:
    lines: list[str] = []
    if expected.names != actual.names:
        lines.append(f"column_order: expected={expected.names}, actual={actual.names}")
    missing = [name for name in expected.names if name not in actual.names]
    extra = [name for name in actual.names if name not in expected.names]
    if missing:
        lines.append(f"missing_fields: {missing}")
    if extra:
        lines.append(f"extra_fields: {extra}")
    return lines


def _schema_field_diff_lines(expected: pa.Schema, actual: pa.Schema) -> list[str]:
    type_mismatches: list[str] = []
    metadata_mismatches: list[str] = []
    nullability_mismatches: list[str] = []
    for name in sorted(set(expected.names) & set(actual.names)):
        expected_field = expected.field(name)
        actual_field = actual.field(name)
        if expected_field.type != actual_field.type:
            type_mismatches.append(
                f"{name}: expected={expected_field.type}, actual={actual_field.type}"
            )
        if (expected_field.metadata or {}) != (actual_field.metadata or {}):
            metadata_mismatches.append(
                f"{name}: expected={expected_field.metadata!r}, actual={actual_field.metadata!r}"
            )
        if expected_field.nullable != actual_field.nullable:
            nullability_mismatches.append(
                f"{name}: expected={expected_field.nullable}, actual={actual_field.nullable}"
            )
    lines: list[str] = []
    if type_mismatches:
        lines.append("type_mismatches:")
        lines.extend(type_mismatches)
    if metadata_mismatches:
        lines.append("metadata_mismatches:")
        lines.extend(metadata_mismatches)
    if nullability_mismatches:
        lines.append("nullability_mismatches:")
        lines.extend(nullability_mismatches)
    return lines


def _schema_diff(expected: pa.Schema, actual: pa.Schema) -> str:
    lines: list[str] = []
    lines.extend(_schema_name_diff_lines(expected, actual))
    lines.extend(_schema_field_diff_lines(expected, actual))
    if (expected.metadata or {}) != (actual.metadata or {}):
        lines.append(f"schema_metadata: expected={expected.metadata!r}, actual={actual.metadata!r}")
    return "\n".join(lines)


def _schemas_match(expected: pa.Schema, actual: pa.Schema) -> bool:
    if expected.names != actual.names:
        return False
    for name in expected.names:
        expected_field = expected.field(name)
        actual_field = actual.field(name)
        if expected_field.type != actual_field.type:
            return False
        if (expected_field.metadata or {}) != (actual_field.metadata or {}):
            return False
    return (expected.metadata or {}) == (actual.metadata or {})


def _emit_quality_source(spec: NodePlanSpec) -> str:
    id_cols = ",".join(spec.emit.id_cols)
    if id_cols:
        return f"{spec.name}:{spec.table_ref}[{id_cols}]"
    return f"{spec.name}:{spec.table_ref}"


def _assert_emit_schema(
    plan: Plan,
    *,
    expected: pa.Schema,
    ctx: ExecutionContext,
    label: str,
) -> None:
    actual = pa.schema(plan.schema(ctx=ctx))
    if _schemas_match(expected, actual):
        return
    diff = _schema_diff(expected, actual)
    msg = f"Node emit schema mismatch for {label}.\n{diff}"
    raise ValueError(msg)


def _node_emitted_plans(
    *,
    ctx: ExecutionContext,
    inputs: NodeInputTables,
    options: NodeBuildOptions,
    node_spec_table: pa.Table | None,
    registry: CpgRegistry,
) -> list[tuple[NodePlanSpec, Plan]]:
    nodes_spec = registry.nodes_spec()
    expected_schema = _raw_nodes_schema(nodes_spec.schema())
    catalog = _node_tables(inputs, options=options, ctx=ctx)
    emitted: list[tuple[NodePlanSpec, Plan]] = []
    for spec in _node_plan_specs(node_spec_table, registry=registry):
        enabled = getattr(options, spec.option_flag, None)
        if enabled is None:
            msg = f"Unknown option flag: {spec.option_flag}"
            raise ValueError(msg)
        if not enabled:
            continue
        plan_source = resolve_plan_source(catalog, spec.table_ref, ctx=ctx)
        if plan_source is None:
            continue
        plan = ensure_plan(plan_source, label=spec.name, ctx=ctx)
        preprocessor = resolve_preprocessor(spec.preprocessor_id)
        if preprocessor is not None:
            plan = preprocessor(plan)
        plan = emit_node_plan(plan, spec=spec.emit, ctx=ctx)
        _assert_emit_schema(plan, expected=expected_schema, ctx=ctx, label=spec.name)
        emitted.append((spec, plan))
    return emitted


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
    use_ibis = bool(
        resolved.ibis_backend is not None
        and (resolved.adapter_mode is None or resolved.adapter_mode.use_ibis_bridge)
    )
    return NodeBuildContext(
        ctx=ctx,
        config=resolved,
        registry=registry,
        options=options,
        nodes_spec=nodes_spec,
        nodes_schema=nodes_schema,
        use_ibis=use_ibis,
    )


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
    adapter_mode: AdapterMode | None = None
    execution_policy: AdapterExecutionPolicy | None = None
    ibis_backend: BaseBackend | None = None


@dataclass(frozen=True)
class NodeBuildContext:
    """Resolved context for node builds."""

    ctx: ExecutionContext
    config: NodeBuildConfig
    registry: CpgRegistry
    options: NodeBuildOptions
    nodes_spec: DatasetSpec
    nodes_schema: SchemaLike
    use_ibis: bool


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

    ctx: ExecutionContext
    backend: BaseBackend
    adapter_mode: AdapterMode | None
    execution_policy: AdapterExecutionPolicy | None


@dataclass(frozen=True)
class NodeInputTables:
    """Bundle of input tables for node construction."""

    repo_files: TableLike | DatasetSource | None = None
    cst_name_refs: TableLike | DatasetSource | None = None
    cst_imports: TableLike | DatasetSource | None = None
    cst_callsites: TableLike | DatasetSource | None = None
    cst_defs: TableLike | DatasetSource | None = None
    dim_qualified_names: TableLike | DatasetSource | None = None
    scip_symbol_information: TableLike | DatasetSource | None = None
    scip_occurrences: TableLike | DatasetSource | None = None
    scip_external_symbol_information: TableLike | DatasetSource | None = None
    scip_symbol_relationships: TableLike | DatasetSource | None = None
    ts_nodes: TableLike | DatasetSource | None = None
    ts_errors: TableLike | DatasetSource | None = None
    ts_missing: TableLike | DatasetSource | None = None
    type_exprs_norm: TableLike | DatasetSource | None = None
    types_norm: TableLike | DatasetSource | None = None
    diagnostics_norm: TableLike | DatasetSource | None = None
    rt_objects: TableLike | DatasetSource | None = None
    rt_signatures: TableLike | DatasetSource | None = None
    rt_signature_params: TableLike | DatasetSource | None = None
    rt_members: TableLike | DatasetSource | None = None


def _materialize_table(
    source: TableLike | DatasetSource | None,
    *,
    ctx: ExecutionContext,
) -> TableLike | None:
    if source is None:
        return None
    if isinstance(source, DatasetSource):
        plan = ensure_plan(source, label="dataset_source", ctx=ctx)
        return finalize_plan(plan, ctx=ctx)
    return source


def _node_tables(
    inputs: NodeInputTables,
    *,
    options: NodeBuildOptions,
    ctx: ExecutionContext,
) -> PlanCatalog:
    catalog = PlanCatalog()
    repo_files = _materialize_table(inputs.repo_files, ctx=ctx)
    if inputs.repo_files is not None:
        catalog.add("repo_files", inputs.repo_files)
        file_nodes = _file_nodes_table(repo_files, use_size_bytes=options.file_span_from_size_bytes)
        catalog.add("repo_files_nodes", file_nodes)

    catalog.extend(
        {
            name: table
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
            }.items()
            if table is not None
        }
    )

    symbol_nodes = _symbol_nodes_table(
        _materialize_table(inputs.scip_symbol_information, ctx=ctx),
        _materialize_table(inputs.scip_occurrences, ctx=ctx),
        _materialize_table(inputs.scip_external_symbol_information, ctx=ctx),
        _materialize_table(inputs.scip_symbol_relationships, ctx=ctx),
    )
    catalog.add("scip_symbols", symbol_nodes)
    return catalog


def _materialize_reader(value: TableLike | RecordBatchReaderLike) -> TableLike:
    if isinstance(value, RecordBatchReaderLike):
        return value.read_all()
    return value


def _source_to_ibis_plan(
    source: TableLike | DatasetSource,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    name: str,
) -> IbisPlan:
    plan = ensure_plan(source, label=name, ctx=ctx)
    return source_to_ibis(
        plan,
        options=SourceToIbisOptions(
            ctx=ctx,
            backend=backend,
            name=name,
            ordering=plan.ordering,
        ),
    )


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
            ctx=ctx,
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
        tables[name] = _source_to_ibis_plan(table, ctx=ctx, backend=backend, name=name)

    scip_symbol_information = (
        None
        if inputs.scip_symbol_information is None
        else _source_to_ibis_plan(
            inputs.scip_symbol_information,
            ctx=ctx,
            backend=backend,
            name="scip_symbol_information",
        )
    )
    scip_occurrences = (
        None
        if inputs.scip_occurrences is None
        else _source_to_ibis_plan(
            inputs.scip_occurrences,
            ctx=ctx,
            backend=backend,
            name="scip_occurrences",
        )
    )
    scip_external_symbol_information = (
        None
        if inputs.scip_external_symbol_information is None
        else _source_to_ibis_plan(
            inputs.scip_external_symbol_information,
            ctx=ctx,
            backend=backend,
            name="scip_external_symbol_information",
        )
    )
    scip_symbol_relationships = (
        None
        if inputs.scip_symbol_relationships is None
        else _source_to_ibis_plan(
            inputs.scip_symbol_relationships,
            ctx=ctx,
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
    registered = register_ibis_view(
        plan.expr,
        backend=context.backend,
        name=name,
        ordering=plan.ordering,
    )
    table = finalize_plan_adapter(
        registered,
        ctx=context.ctx,
        options=FinalizePlanAdapterOptions(
            adapter_mode=context.adapter_mode,
            prefer_reader=True,
            execution_policy=context.execution_policy,
            ibis_backend=context.backend,
        ),
    )
    return _materialize_reader(table)


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
        preprocessor = resolve_preprocessor(spec.preprocessor_id)
        if preprocessor is not None:
            msg = f"Node preprocessor {spec.preprocessor_id!r} is not supported for Ibis."
            raise ValueError(msg)
        emitted.append((spec, emit_nodes_ibis(plan, spec=spec.emit)))
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
    materialize_context = IbisMaterializeContext(
        ctx=context.ctx,
        backend=backend,
        adapter_mode=config.adapter_mode,
        execution_policy=config.execution_policy,
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
    ctx: ExecutionContext,
    config: NodeBuildConfig | None = None,
) -> Plan | IbisPlan:
    """Build CPG nodes as a plan without finalization.

    Parameters
    ----------
    ctx:
        Execution context for plan evaluation.
    config:
        Node build configuration bundle.

    Returns
    -------
    Plan | IbisPlan
        Plan producing the raw nodes table.

    Raises
    ------
    ValueError
        Raised when Ibis execution is requested without a backend.
    """
    build_context = _resolve_node_build_context(ctx, config=config)
    if build_context.use_ibis:
        backend = build_context.config.ibis_backend
        if backend is None:
            msg = "Ibis backend is required when building nodes with Ibis."
            raise ValueError(msg)
        emitted = _node_emitted_plans_ibis(
            NodeEmitIbisContext(
                ctx=ctx,
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
    emitted = _node_emitted_plans(
        ctx=ctx,
        inputs=build_context.config.inputs or NodeInputTables(),
        options=build_context.options,
        node_spec_table=build_context.config.node_spec_table,
        registry=build_context.registry,
    )
    parts = [plan for _, plan in emitted]

    if not parts:
        return empty_plan(build_context.nodes_schema, label="cpg_nodes_raw")

    return union_all_plans(parts, label="cpg_nodes_raw")


def build_cpg_nodes(
    *,
    ctx: ExecutionContext,
    config: NodeBuildConfig | None = None,
) -> CpgBuildArtifacts:
    """Build and finalize CPG nodes with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalize result plus quality table.
    """
    build_context = _resolve_node_build_context(ctx, config=config)
    if build_context.use_ibis:
        return _build_cpg_nodes_ibis(build_context)
    emitted = _node_emitted_plans(
        ctx=ctx,
        inputs=build_context.config.inputs or NodeInputTables(),
        options=build_context.options,
        node_spec_table=build_context.config.node_spec_table,
        registry=build_context.registry,
    )
    parts = [plan for _, plan in emitted]
    if not parts:
        raw_plan = empty_plan(build_context.nodes_schema, label="cpg_nodes_raw")
    else:
        raw_plan = union_all_plans(parts, label="cpg_nodes_raw")
    quality_plans = [
        quality_plan_from_ids(
            raw_plan,
            spec=QualityPlanSpec(
                id_col="node_id",
                entity_kind="node",
                issue="invalid_node_id",
                source_table="cpg_nodes_raw",
            ),
            ctx=ctx,
        )
    ]
    for spec, plan in emitted:
        quality_plans.append(
            quality_plan_from_ids(
                plan,
                spec=QualityPlanSpec(
                    id_col="node_id",
                    entity_kind="node",
                    issue="invalid_node_id_emitter",
                    source_table=_emit_quality_source(spec),
                ),
                ctx=ctx,
            )
        )
    quality_plan = union_all_plans(quality_plans, label="cpg_nodes_quality")
    raw = finalize_plan(raw_plan, ctx=ctx)
    quality = finalize_plan(quality_plan, ctx=ctx)
    finalize_ctx = finalize_context_for_plan(
        raw_plan,
        contract=build_context.nodes_spec.contract(),
        ctx=ctx,
    )
    finalize = build_context.nodes_spec.finalize_context(ctx).run(raw, ctx=finalize_ctx)
    if ctx.debug:
        assert_schema_metadata(finalize.good, schema=build_context.nodes_spec.schema())
    return CpgBuildArtifacts(
        finalize=finalize,
        quality=quality,
        pipeline_breakers=raw_plan.pipeline_breakers,
    )
