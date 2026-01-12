"""Build CPG node tables from extraction outputs."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.ids import iter_array_values
from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike, TableLike
from arrowdsl.schema.arrays import const_array, set_or_append_column
from arrowdsl.schema.schema import EncodingSpec, encode_columns
from cpg.artifacts import CpgBuildArtifacts
from cpg.builders import NodeBuilder
from cpg.catalog import TableCatalog
from cpg.merge import unify_tables
from cpg.quality import quality_from_ids
from cpg.schemas import CPG_NODES_SCHEMA, CPG_NODES_SPEC, SCHEMA_VERSION, empty_nodes
from cpg.spec_registry import node_plan_specs


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
    return pa.Table.from_arrays([pa.array(uniq, type=pa.string())], names=["symbol"])


NODE_PLAN_SPECS = node_plan_specs()

NODE_ENCODING_SPECS: tuple[EncodingSpec, ...] = (EncodingSpec(column="node_kind"),)

NODE_BUILDER = NodeBuilder(
    emitters=NODE_PLAN_SPECS,
    schema_version=SCHEMA_VERSION,
    node_schema=CPG_NODES_SCHEMA,
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
class NodeInputTables:
    """Bundle of input tables for node construction."""

    repo_files: TableLike | None = None
    cst_name_refs: TableLike | None = None
    cst_imports: TableLike | None = None
    cst_callsites: TableLike | None = None
    cst_defs: TableLike | None = None
    dim_qualified_names: TableLike | None = None
    scip_symbol_information: TableLike | None = None
    scip_occurrences: TableLike | None = None
    ts_nodes: TableLike | None = None
    ts_errors: TableLike | None = None
    ts_missing: TableLike | None = None
    type_exprs_norm: TableLike | None = None
    types_norm: TableLike | None = None
    diagnostics_norm: TableLike | None = None
    rt_objects: TableLike | None = None
    rt_signatures: TableLike | None = None
    rt_signature_params: TableLike | None = None
    rt_members: TableLike | None = None


def _node_tables(inputs: NodeInputTables, *, options: NodeBuildOptions) -> dict[str, TableLike]:
    catalog = TableCatalog()
    repo_files = inputs.repo_files
    if repo_files is not None:
        catalog.add("repo_files", repo_files)
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

    symbol_nodes = _symbol_nodes_table(inputs.scip_symbol_information, inputs.scip_occurrences)
    catalog.add("scip_symbols", symbol_nodes)
    return catalog.snapshot()


def build_cpg_nodes_raw(
    *,
    inputs: NodeInputTables | None = None,
    options: NodeBuildOptions | None = None,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Build CPG nodes without finalization.

    Parameters
    ----------
    inputs:
        Bundle of input tables for node construction.
    options:
        Node build options.
    ctx:
        Optional execution context for schema evolution.

    Returns
    -------
    pyarrow.Table
        Raw nodes table.
    """
    options = options or NodeBuildOptions()
    tables = _node_tables(inputs or NodeInputTables(), options=options)
    parts = NODE_BUILDER.build(tables=tables, options=options)
    parts = [part for part in parts if part.num_rows]

    if not parts:
        return empty_nodes()

    combined = unify_tables(spec=CPG_NODES_SPEC, tables=parts, ctx=ctx)
    return encode_columns(combined, specs=NODE_ENCODING_SPECS)


def build_cpg_nodes(
    *,
    ctx: ExecutionContext,
    inputs: NodeInputTables | None = None,
    options: NodeBuildOptions | None = None,
) -> CpgBuildArtifacts:
    """Build and finalize CPG nodes with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalize result plus quality table.
    """
    raw = build_cpg_nodes_raw(inputs=inputs, options=options, ctx=ctx)
    quality = quality_from_ids(
        raw,
        id_col="node_id",
        entity_kind="node",
        issue="invalid_node_id",
        source_table="cpg_nodes_raw",
    )
    finalize = CPG_NODES_SPEC.finalize_context(ctx).run(raw, ctx=ctx)
    return CpgBuildArtifacts(finalize=finalize, quality=quality)
