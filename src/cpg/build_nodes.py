"""Build CPG node tables from extraction outputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.ids import iter_array_values
from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike, SchemaLike, TableLike
from arrowdsl.plan.plan import Plan, union_all_plans
from arrowdsl.plan.scan_io import DatasetSource
from arrowdsl.schema.build import const_array, set_or_append_column, table_from_arrays
from arrowdsl.schema.metadata import (
    DICT_INDEX_META,
    DICT_ORDERED_META,
    ENCODING_META,
    normalize_dictionaries,
)
from cpg.catalog import PlanCatalog, resolve_plan_source
from cpg.constants import CpgBuildArtifacts, QualityPlanSpec, quality_plan_from_ids
from cpg.emit_nodes import emit_node_plan
from cpg.plan_specs import (
    assert_schema_metadata,
    empty_plan,
    ensure_plan,
    finalize_context_for_plan,
    finalize_plan,
)
from cpg.registry import CpgRegistry, default_cpg_registry
from cpg.spec_tables import node_plan_specs_from_table
from cpg.specs import NodePlanSpec, resolve_preprocessor


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


def _collect_symbols(table: TableLike | None) -> set[str]:
    if table is None or "symbol" not in table.column_names:
        return set()
    return {str(sym) for sym in iter_array_values(table["symbol"]) if sym}


def _symbol_nodes_table(
    scip_symbol_information: TableLike | None,
    scip_occurrences: TableLike | None,
) -> TableLike | None:
    symbols = _collect_symbols(scip_symbol_information)
    symbols.update(_collect_symbols(scip_occurrences))
    if not symbols:
        return None
    uniq = sorted(symbols)
    schema = pa.schema([pa.field("symbol", pa.string())])
    columns = {"symbol": pa.array(uniq, type=pa.string())}
    table = table_from_arrays(schema, columns=columns, num_rows=len(uniq))
    return normalize_dictionaries(table)


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
    )
    catalog.add("scip_symbols", symbol_nodes)
    return catalog


def build_cpg_nodes_raw(
    *,
    ctx: ExecutionContext,
    inputs: NodeInputTables | None = None,
    options: NodeBuildOptions | None = None,
    node_spec_table: pa.Table | None = None,
    registry: CpgRegistry | None = None,
) -> Plan:
    """Build CPG nodes as a plan without finalization.

    Parameters
    ----------
    ctx:
        Execution context for plan evaluation.
    inputs:
        Bundle of input tables for node construction.
    options:
        Node build options.
    node_spec_table:
        Optional spec table overriding the default node plan specs.
    registry:
        Optional CPG registry overriding spec tables and dataset specs.

    Returns
    -------
    Plan
        Plan producing the raw nodes table.
    """
    registry = registry or default_cpg_registry()
    nodes_spec = registry.nodes_spec()
    nodes_schema = nodes_spec.schema()
    options = options or NodeBuildOptions()
    emitted = _node_emitted_plans(
        ctx=ctx,
        inputs=inputs or NodeInputTables(),
        options=options,
        node_spec_table=node_spec_table,
        registry=registry,
    )
    parts = [plan for _, plan in emitted]

    if not parts:
        return empty_plan(nodes_schema, label="cpg_nodes_raw")

    return union_all_plans(parts, label="cpg_nodes_raw")


def build_cpg_nodes(
    *,
    ctx: ExecutionContext,
    inputs: NodeInputTables | None = None,
    options: NodeBuildOptions | None = None,
    node_spec_table: pa.Table | None = None,
    registry: CpgRegistry | None = None,
) -> CpgBuildArtifacts:
    """Build and finalize CPG nodes with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalize result plus quality table.
    """
    registry = registry or default_cpg_registry()
    nodes_spec = registry.nodes_spec()
    nodes_schema = nodes_spec.schema()
    options = options or NodeBuildOptions()
    emitted = _node_emitted_plans(
        ctx=ctx,
        inputs=inputs or NodeInputTables(),
        options=options,
        node_spec_table=node_spec_table,
        registry=registry,
    )
    parts = [plan for _, plan in emitted]
    if not parts:
        raw_plan = empty_plan(nodes_schema, label="cpg_nodes_raw")
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
        contract=nodes_spec.contract(),
        ctx=ctx,
    )
    finalize = nodes_spec.finalize_context(ctx).run(raw, ctx=finalize_ctx)
    if ctx.debug:
        assert_schema_metadata(finalize.good, schema=nodes_spec.schema())
    return CpgBuildArtifacts(
        finalize=finalize,
        quality=quality,
        pipeline_breakers=raw_plan.pipeline_breakers,
    )
