"""Build CPG node tables from extraction outputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import arrowdsl.pyarrow_core as pa
from arrowdsl.finalize import FinalizeResult, finalize
from arrowdsl.iter import iter_array_values
from arrowdsl.pyarrow_protocols import ArrayLike, ChunkedArrayLike, TableLike
from arrowdsl.runtime import ExecutionContext
from cpg.builders import NodeBuilder
from cpg.kinds import NodeKind
from cpg.schemas import CPG_NODES_CONTRACT, CPG_NODES_SCHEMA, SCHEMA_VERSION, empty_nodes
from cpg.specs import NodeEmitSpec, NodePlanSpec, TableGetter


def _set_or_append_column(
    table: TableLike,
    name: str,
    values: ArrayLike | ChunkedArrayLike,
) -> TableLike:
    if name in table.column_names:
        idx = table.schema.get_field_index(name)
        return table.set_column(idx, name, values)
    return table.append_column(name, values)


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
        return pa.array([0] * n, type=pa.int64()), pa.array(bends, type=pa.int64())
    return pa.array([None] * n, type=pa.int64()), pa.array([None] * n, type=pa.int64())


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
    table = _set_or_append_column(repo_files, "bstart", bstart)
    return _set_or_append_column(table, "bend", bend)


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


def _table_getter(name: str) -> TableGetter:
    def _get_table(tables: Mapping[str, TableLike]) -> TableLike | None:
        return tables.get(name)

    return _get_table


NODE_PLAN_SPECS: tuple[NodePlanSpec, ...] = (
    NodePlanSpec(
        name="file_nodes",
        option_flag="include_file_nodes",
        table_getter=_table_getter("repo_files_nodes"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.PY_FILE,
            id_cols=("file_id",),
            path_cols=("path",),
            bstart_cols=("bstart",),
            bend_cols=("bend",),
            file_id_cols=("file_id",),
        ),
    ),
    NodePlanSpec(
        name="name_ref_nodes",
        option_flag="include_name_ref_nodes",
        table_getter=_table_getter("cst_name_refs"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.CST_NAME_REF,
            id_cols=("name_ref_id",),
            path_cols=("path",),
            bstart_cols=("bstart",),
            bend_cols=("bend",),
            file_id_cols=("file_id",),
        ),
    ),
    NodePlanSpec(
        name="import_alias_nodes",
        option_flag="include_import_alias_nodes",
        table_getter=_table_getter("cst_imports"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.CST_IMPORT_ALIAS,
            id_cols=("import_alias_id", "import_id"),
            path_cols=("path",),
            bstart_cols=("bstart", "alias_bstart"),
            bend_cols=("bend", "alias_bend"),
            file_id_cols=("file_id",),
        ),
    ),
    NodePlanSpec(
        name="callsite_nodes",
        option_flag="include_callsite_nodes",
        table_getter=_table_getter("cst_callsites"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.CST_CALLSITE,
            id_cols=("call_id",),
            path_cols=("path",),
            bstart_cols=("call_bstart", "bstart"),
            bend_cols=("call_bend", "bend"),
            file_id_cols=("file_id",),
        ),
    ),
    NodePlanSpec(
        name="def_nodes",
        option_flag="include_def_nodes",
        table_getter=_table_getter("cst_defs"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.CST_DEF,
            id_cols=("def_id",),
            path_cols=("path",),
            bstart_cols=("bstart", "name_bstart"),
            bend_cols=("bend", "name_bend"),
            file_id_cols=("file_id",),
        ),
    ),
    NodePlanSpec(
        name="qname_nodes",
        option_flag="include_qname_nodes",
        table_getter=_table_getter("dim_qualified_names"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.PY_QUALIFIED_NAME,
            id_cols=("qname_id",),
            path_cols=("path",),
            bstart_cols=("bstart",),
            bend_cols=("bend",),
            file_id_cols=(),
        ),
    ),
    NodePlanSpec(
        name="symbol_nodes",
        option_flag="include_symbol_nodes",
        table_getter=_table_getter("scip_symbols"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.SCIP_SYMBOL,
            id_cols=("symbol",),
            path_cols=(),
            bstart_cols=(),
            bend_cols=(),
            file_id_cols=(),
        ),
    ),
    NodePlanSpec(
        name="tree_sitter_nodes",
        option_flag="include_tree_sitter_nodes",
        table_getter=_table_getter("ts_nodes"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.TS_NODE,
            id_cols=("ts_node_id",),
            path_cols=("path",),
            bstart_cols=("start_byte",),
            bend_cols=("end_byte",),
            file_id_cols=("file_id",),
        ),
    ),
    NodePlanSpec(
        name="tree_sitter_error_nodes",
        option_flag="include_tree_sitter_nodes",
        table_getter=_table_getter("ts_errors"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.TS_ERROR,
            id_cols=("ts_error_id",),
            path_cols=("path",),
            bstart_cols=("start_byte",),
            bend_cols=("end_byte",),
            file_id_cols=("file_id",),
        ),
    ),
    NodePlanSpec(
        name="tree_sitter_missing_nodes",
        option_flag="include_tree_sitter_nodes",
        table_getter=_table_getter("ts_missing"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.TS_MISSING,
            id_cols=("ts_missing_id",),
            path_cols=("path",),
            bstart_cols=("start_byte",),
            bend_cols=("end_byte",),
            file_id_cols=("file_id",),
        ),
    ),
    NodePlanSpec(
        name="type_expr_nodes",
        option_flag="include_type_nodes",
        table_getter=_table_getter("type_exprs_norm"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.TYPE_EXPR,
            id_cols=("type_expr_id",),
            path_cols=("path",),
            bstart_cols=("bstart",),
            bend_cols=("bend",),
            file_id_cols=("file_id",),
        ),
    ),
    NodePlanSpec(
        name="type_nodes",
        option_flag="include_type_nodes",
        table_getter=_table_getter("types_norm"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.TYPE,
            id_cols=("type_id",),
            path_cols=(),
            bstart_cols=(),
            bend_cols=(),
            file_id_cols=(),
        ),
    ),
    NodePlanSpec(
        name="diagnostic_nodes",
        option_flag="include_diagnostic_nodes",
        table_getter=_table_getter("diagnostics_norm"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.DIAG,
            id_cols=("diag_id",),
            path_cols=("path",),
            bstart_cols=("bstart",),
            bend_cols=("bend",),
            file_id_cols=("file_id",),
        ),
    ),
    NodePlanSpec(
        name="runtime_object_nodes",
        option_flag="include_runtime_nodes",
        table_getter=_table_getter("rt_objects"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.RT_OBJECT,
            id_cols=("rt_id",),
            path_cols=(),
            bstart_cols=(),
            bend_cols=(),
            file_id_cols=(),
        ),
    ),
    NodePlanSpec(
        name="runtime_signature_nodes",
        option_flag="include_runtime_nodes",
        table_getter=_table_getter("rt_signatures"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.RT_SIGNATURE,
            id_cols=("sig_id",),
            path_cols=(),
            bstart_cols=(),
            bend_cols=(),
            file_id_cols=(),
        ),
    ),
    NodePlanSpec(
        name="runtime_param_nodes",
        option_flag="include_runtime_nodes",
        table_getter=_table_getter("rt_signature_params"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.RT_SIGNATURE_PARAM,
            id_cols=("param_id",),
            path_cols=(),
            bstart_cols=(),
            bend_cols=(),
            file_id_cols=(),
        ),
    ),
    NodePlanSpec(
        name="runtime_member_nodes",
        option_flag="include_runtime_nodes",
        table_getter=_table_getter("rt_members"),
        emit=NodeEmitSpec(
            node_kind=NodeKind.RT_MEMBER,
            id_cols=("member_id",),
            path_cols=(),
            bstart_cols=(),
            bend_cols=(),
            file_id_cols=(),
        ),
    ),
)

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
    tables: dict[str, TableLike] = {}
    repo_files = inputs.repo_files
    if repo_files is not None:
        tables["repo_files"] = repo_files
        file_nodes = _file_nodes_table(repo_files, use_size_bytes=options.file_span_from_size_bytes)
        if file_nodes is not None:
            tables["repo_files_nodes"] = file_nodes

    tables.update(
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
    if symbol_nodes is not None:
        tables["scip_symbols"] = symbol_nodes
    return tables


def build_cpg_nodes_raw(
    *,
    inputs: NodeInputTables | None = None,
    options: NodeBuildOptions | None = None,
) -> TableLike:
    """Build CPG nodes without finalization.

    Parameters
    ----------
    inputs:
        Bundle of input tables for node construction.
    options:
        Node build options.

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

    return pa.concat_tables(parts, promote=True)


def build_cpg_nodes(
    *,
    ctx: ExecutionContext,
    inputs: NodeInputTables | None = None,
    options: NodeBuildOptions | None = None,
) -> FinalizeResult:
    """Build and finalize CPG nodes.

    Returns
    -------
    FinalizeResult
        Finalized nodes tables and stats.
    """
    raw = build_cpg_nodes_raw(
        inputs=inputs,
        options=options,
    )
    return finalize(raw, contract=CPG_NODES_CONTRACT, ctx=ctx)
