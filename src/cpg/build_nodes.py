"""Build CPG node tables from extraction outputs."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.finalize import FinalizeResult, finalize
from arrowdsl.runtime import ExecutionContext
from cpg.kinds import NodeKind
from cpg.schemas import CPG_NODES_CONTRACT, CPG_NODES_SCHEMA, SCHEMA_VERSION, empty_nodes


def _const_str(n: int, value: str) -> pa.Array:
    return pa.array([value] * n, type=pa.string())


def _const_i32(n: int, value: int) -> pa.Array:
    return pa.array([int(value)] * n, type=pa.int32())


def _pick_col(table: pa.Table, names: Sequence[str]) -> pa.ChunkedArray | None:
    for name in names:
        if name in table.column_names:
            return table[name]
    return None


@dataclass(frozen=True)
class AnchoredNodeSpec:
    """Specification for anchored node extraction."""

    id_col: str
    kind: NodeKind
    path_col: str = "path"
    bstart_col: str = "bstart"
    bend_col: str = "bend"
    file_id_col: str | None = "file_id"


def _nodes_from_anchored(
    table: pa.Table,
    spec: AnchoredNodeSpec,
) -> pa.Table:
    n = table.num_rows
    if n == 0:
        return empty_nodes()

    node_id = table[spec.id_col]
    path = (
        table[spec.path_col]
        if spec.path_col in table.column_names
        else pa.array([None] * n, type=pa.string())
    )
    bstart = (
        table[spec.bstart_col]
        if spec.bstart_col in table.column_names
        else pa.array([None] * n, type=pa.int64())
    )
    bend = (
        table[spec.bend_col]
        if spec.bend_col in table.column_names
        else pa.array([None] * n, type=pa.int64())
    )

    if spec.file_id_col is not None and spec.file_id_col in table.column_names:
        file_id = table[spec.file_id_col]
    else:
        file_id = pa.array([None] * n, type=pa.string())

    return pa.Table.from_arrays(
        [
            _const_i32(n, SCHEMA_VERSION),
            node_id,
            _const_str(n, spec.kind.value),
            path,
            bstart,
            bend,
            file_id,
        ],
        schema=CPG_NODES_SCHEMA,
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

    repo_files: pa.Table | None = None
    cst_name_refs: pa.Table | None = None
    cst_imports: pa.Table | None = None
    cst_callsites: pa.Table | None = None
    cst_defs: pa.Table | None = None
    dim_qualified_names: pa.Table | None = None
    scip_symbol_information: pa.Table | None = None
    scip_occurrences: pa.Table | None = None
    ts_nodes: pa.Table | None = None
    ts_errors: pa.Table | None = None
    ts_missing: pa.Table | None = None
    type_exprs_norm: pa.Table | None = None
    types_norm: pa.Table | None = None
    diagnostics_norm: pa.Table | None = None
    rt_objects: pa.Table | None = None
    rt_signatures: pa.Table | None = None
    rt_signature_params: pa.Table | None = None
    rt_members: pa.Table | None = None


@dataclass(frozen=True)
class NodePartSpec:
    """Configure a node builder tied to a build option."""

    flag: str
    builder: Callable[[NodeInputTables, NodeBuildOptions], list[pa.Table]]


def _collect_parts(parts: Sequence[pa.Table | None]) -> list[pa.Table]:
    return [part for part in parts if part is not None]


def _build_file_nodes(tables: NodeInputTables, options: NodeBuildOptions) -> list[pa.Table]:
    return _collect_parts([_file_nodes(tables.repo_files, options)])


def _build_name_ref_nodes(tables: NodeInputTables, options: NodeBuildOptions) -> list[pa.Table]:
    _ = options
    return _collect_parts([_name_ref_nodes(tables.cst_name_refs)])


def _build_import_alias_nodes(tables: NodeInputTables, options: NodeBuildOptions) -> list[pa.Table]:
    _ = options
    return _collect_parts([_import_alias_nodes(tables.cst_imports)])


def _build_callsite_nodes(tables: NodeInputTables, options: NodeBuildOptions) -> list[pa.Table]:
    _ = options
    return _collect_parts([_callsite_nodes(tables.cst_callsites)])


def _build_def_nodes(tables: NodeInputTables, options: NodeBuildOptions) -> list[pa.Table]:
    _ = options
    return _collect_parts([_def_nodes(tables.cst_defs)])


def _build_qname_nodes(tables: NodeInputTables, options: NodeBuildOptions) -> list[pa.Table]:
    _ = options
    return _collect_parts([_qname_nodes(tables.dim_qualified_names)])


def _build_symbol_nodes(tables: NodeInputTables, options: NodeBuildOptions) -> list[pa.Table]:
    _ = options
    return _collect_parts(
        [_symbol_nodes(tables.scip_symbol_information, tables.scip_occurrences)]
    )


def _build_tree_sitter_nodes(tables: NodeInputTables, options: NodeBuildOptions) -> list[pa.Table]:
    _ = options
    return _collect_parts(
        [
            _ts_nodes(tables.ts_nodes),
            _ts_error_nodes(tables.ts_errors),
            _ts_missing_nodes(tables.ts_missing),
        ]
    )


def _build_type_nodes(tables: NodeInputTables, options: NodeBuildOptions) -> list[pa.Table]:
    _ = options
    return _collect_parts([_type_expr_nodes(tables.type_exprs_norm), _type_nodes(tables.types_norm)])


def _build_diagnostic_nodes(tables: NodeInputTables, options: NodeBuildOptions) -> list[pa.Table]:
    _ = options
    return _collect_parts([_diagnostic_nodes(tables.diagnostics_norm)])


def _build_runtime_nodes(tables: NodeInputTables, options: NodeBuildOptions) -> list[pa.Table]:
    _ = options
    return _runtime_nodes(
        tables.rt_objects,
        tables.rt_signatures,
        tables.rt_signature_params,
        tables.rt_members,
    )


NODE_PART_SPECS: tuple[NodePartSpec, ...] = (
    NodePartSpec(flag="include_file_nodes", builder=_build_file_nodes),
    NodePartSpec(flag="include_name_ref_nodes", builder=_build_name_ref_nodes),
    NodePartSpec(flag="include_import_alias_nodes", builder=_build_import_alias_nodes),
    NodePartSpec(flag="include_callsite_nodes", builder=_build_callsite_nodes),
    NodePartSpec(flag="include_def_nodes", builder=_build_def_nodes),
    NodePartSpec(flag="include_qname_nodes", builder=_build_qname_nodes),
    NodePartSpec(flag="include_symbol_nodes", builder=_build_symbol_nodes),
    NodePartSpec(flag="include_tree_sitter_nodes", builder=_build_tree_sitter_nodes),
    NodePartSpec(flag="include_type_nodes", builder=_build_type_nodes),
    NodePartSpec(flag="include_diagnostic_nodes", builder=_build_diagnostic_nodes),
    NodePartSpec(flag="include_runtime_nodes", builder=_build_runtime_nodes),
)


def _append_part(parts: list[pa.Table], table: pa.Table | None) -> None:
    if table is not None and table.num_rows:
        parts.append(table)


def _file_span_arrays(
    n: int,
    size: pa.ChunkedArray | None,
    *,
    use_size_bytes: bool,
) -> tuple[pa.Array, pa.Array]:
    if use_size_bytes and size is not None:
        raw_values = size.to_pylist()
        bends: list[int | None] = []
        for value in raw_values:
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


def _file_nodes(repo_files: pa.Table | None, options: NodeBuildOptions) -> pa.Table | None:
    if repo_files is None or repo_files.num_rows == 0:
        return None
    n = repo_files.num_rows
    file_id = _pick_col(repo_files, ["file_id"])
    path = _pick_col(repo_files, ["path"])
    size = _pick_col(repo_files, ["size_bytes"])
    if file_id is None or path is None:
        return None

    bstart, bend = _file_span_arrays(
        n,
        size,
        use_size_bytes=options.file_span_from_size_bytes,
    )
    return pa.Table.from_arrays(
        [
            _const_i32(n, SCHEMA_VERSION),
            file_id,
            _const_str(n, NodeKind.PY_FILE.value),
            path,
            bstart,
            bend,
            file_id,
        ],
        schema=CPG_NODES_SCHEMA,
    )


def _name_ref_nodes(cst_name_refs: pa.Table | None) -> pa.Table | None:
    if cst_name_refs is None or cst_name_refs.num_rows == 0:
        return None
    file_id_col = "file_id" if "file_id" in cst_name_refs.column_names else None
    return _nodes_from_anchored(
        cst_name_refs,
        AnchoredNodeSpec(
            id_col="name_ref_id",
            kind=NodeKind.CST_NAME_REF,
            path_col="path",
            bstart_col="bstart",
            bend_col="bend",
            file_id_col=file_id_col,
        ),
    )


def _import_alias_nodes(cst_imports: pa.Table | None) -> pa.Table | None:
    if cst_imports is None or cst_imports.num_rows == 0:
        return None
    id_col = "import_alias_id" if "import_alias_id" in cst_imports.column_names else "import_id"
    bstart_col = "bstart" if "bstart" in cst_imports.column_names else "alias_bstart"
    bend_col = "bend" if "bend" in cst_imports.column_names else "alias_bend"
    file_id_col = "file_id" if "file_id" in cst_imports.column_names else None
    return _nodes_from_anchored(
        cst_imports,
        AnchoredNodeSpec(
            id_col=id_col,
            kind=NodeKind.CST_IMPORT_ALIAS,
            path_col="path",
            bstart_col=bstart_col,
            bend_col=bend_col,
            file_id_col=file_id_col,
        ),
    )


def _callsite_span_columns(cst_callsites: pa.Table) -> tuple[str, str]:
    bstart_col = "call_bstart" if "call_bstart" in cst_callsites.column_names else "bstart"
    bend_col = "call_bend" if "call_bend" in cst_callsites.column_names else "bend"
    return bstart_col, bend_col


def _callsite_nodes(cst_callsites: pa.Table | None) -> pa.Table | None:
    if cst_callsites is None or cst_callsites.num_rows == 0:
        return None
    bstart_col, bend_col = _callsite_span_columns(cst_callsites)
    file_id_col = "file_id" if "file_id" in cst_callsites.column_names else None
    return _nodes_from_anchored(
        cst_callsites,
        AnchoredNodeSpec(
            id_col="call_id",
            kind=NodeKind.CST_CALLSITE,
            path_col="path",
            bstart_col=bstart_col,
            bend_col=bend_col,
            file_id_col=file_id_col,
        ),
    )


def _def_nodes(cst_defs: pa.Table | None) -> pa.Table | None:
    if cst_defs is None or cst_defs.num_rows == 0:
        return None
    bstart_col = "bstart" if "bstart" in cst_defs.column_names else "name_bstart"
    bend_col = "bend" if "bend" in cst_defs.column_names else "name_bend"
    file_id_col = "file_id" if "file_id" in cst_defs.column_names else None
    return _nodes_from_anchored(
        cst_defs,
        AnchoredNodeSpec(
            id_col="def_id",
            kind=NodeKind.CST_DEF,
            path_col="path",
            bstart_col=bstart_col,
            bend_col=bend_col,
            file_id_col=file_id_col,
        ),
    )


def _qname_nodes(dim_qualified_names: pa.Table | None) -> pa.Table | None:
    if (
        dim_qualified_names is None
        or dim_qualified_names.num_rows == 0
        or "qname_id" not in dim_qualified_names.column_names
    ):
        return None
    return _nodes_from_anchored(
        dim_qualified_names,
        AnchoredNodeSpec(
            id_col="qname_id",
            kind=NodeKind.PY_QUALIFIED_NAME,
            path_col="path",
            bstart_col="bstart",
            bend_col="bend",
            file_id_col=None,
        ),
    )


def _collect_symbols(table: pa.Table | None) -> set[str]:
    if table is None or "symbol" not in table.column_names:
        return set()
    return {str(sym) for sym in table["symbol"].to_pylist() if sym}


def _symbol_nodes(
    scip_symbol_information: pa.Table | None,
    scip_occurrences: pa.Table | None,
) -> pa.Table | None:
    symbols = _collect_symbols(scip_symbol_information)
    symbols.update(_collect_symbols(scip_occurrences))
    if not symbols:
        return None

    uniq = sorted(symbols)
    n = len(uniq)
    return pa.Table.from_arrays(
        [
            _const_i32(n, SCHEMA_VERSION),
            pa.array(uniq, type=pa.string()),
            _const_str(n, NodeKind.SCIP_SYMBOL.value),
            pa.array([None] * n, type=pa.string()),
            pa.array([None] * n, type=pa.int64()),
            pa.array([None] * n, type=pa.int64()),
            pa.array([None] * n, type=pa.string()),
        ],
        schema=CPG_NODES_SCHEMA,
    )


def _nodes_from_unanchored(table: pa.Table, *, id_col: str, kind: NodeKind) -> pa.Table:
    return _nodes_from_anchored(
        table,
        AnchoredNodeSpec(
            id_col=id_col,
            kind=kind,
            path_col="path",
            bstart_col="bstart",
            bend_col="bend",
            file_id_col=None,
        ),
    )


def _ts_nodes(ts_nodes: pa.Table | None) -> pa.Table | None:
    if ts_nodes is None or ts_nodes.num_rows == 0:
        return None
    file_id_col = "file_id" if "file_id" in ts_nodes.column_names else None
    return _nodes_from_anchored(
        ts_nodes,
        AnchoredNodeSpec(
            id_col="ts_node_id",
            kind=NodeKind.TS_NODE,
            path_col="path",
            bstart_col="start_byte",
            bend_col="end_byte",
            file_id_col=file_id_col,
        ),
    )


def _ts_error_nodes(ts_errors: pa.Table | None) -> pa.Table | None:
    if ts_errors is None or ts_errors.num_rows == 0:
        return None
    file_id_col = "file_id" if "file_id" in ts_errors.column_names else None
    return _nodes_from_anchored(
        ts_errors,
        AnchoredNodeSpec(
            id_col="ts_error_id",
            kind=NodeKind.TS_ERROR,
            path_col="path",
            bstart_col="start_byte",
            bend_col="end_byte",
            file_id_col=file_id_col,
        ),
    )


def _ts_missing_nodes(ts_missing: pa.Table | None) -> pa.Table | None:
    if ts_missing is None or ts_missing.num_rows == 0:
        return None
    file_id_col = "file_id" if "file_id" in ts_missing.column_names else None
    return _nodes_from_anchored(
        ts_missing,
        AnchoredNodeSpec(
            id_col="ts_missing_id",
            kind=NodeKind.TS_MISSING,
            path_col="path",
            bstart_col="start_byte",
            bend_col="end_byte",
            file_id_col=file_id_col,
        ),
    )


def _type_expr_nodes(type_exprs_norm: pa.Table | None) -> pa.Table | None:
    if type_exprs_norm is None or type_exprs_norm.num_rows == 0:
        return None
    file_id_col = "file_id" if "file_id" in type_exprs_norm.column_names else None
    return _nodes_from_anchored(
        type_exprs_norm,
        AnchoredNodeSpec(
            id_col="type_expr_id",
            kind=NodeKind.TYPE_EXPR,
            path_col="path",
            bstart_col="bstart",
            bend_col="bend",
            file_id_col=file_id_col,
        ),
    )


def _type_nodes(types_norm: pa.Table | None) -> pa.Table | None:
    if types_norm is None or types_norm.num_rows == 0:
        return None
    return _nodes_from_unanchored(types_norm, id_col="type_id", kind=NodeKind.TYPE)


def _diagnostic_nodes(diagnostics_norm: pa.Table | None) -> pa.Table | None:
    if diagnostics_norm is None or diagnostics_norm.num_rows == 0:
        return None
    file_id_col = "file_id" if "file_id" in diagnostics_norm.column_names else None
    return _nodes_from_anchored(
        diagnostics_norm,
        AnchoredNodeSpec(
            id_col="diag_id",
            kind=NodeKind.DIAG,
            path_col="path",
            bstart_col="bstart",
            bend_col="bend",
            file_id_col=file_id_col,
        ),
    )


def _runtime_nodes(
    rt_objects: pa.Table | None,
    rt_signatures: pa.Table | None,
    rt_signature_params: pa.Table | None,
    rt_members: pa.Table | None,
) -> list[pa.Table]:
    parts: list[pa.Table] = []
    if rt_objects is not None and rt_objects.num_rows:
        parts.append(_nodes_from_unanchored(rt_objects, id_col="rt_id", kind=NodeKind.RT_OBJECT))
    if rt_signatures is not None and rt_signatures.num_rows:
        parts.append(
            _nodes_from_unanchored(rt_signatures, id_col="sig_id", kind=NodeKind.RT_SIGNATURE)
        )
    if rt_signature_params is not None and rt_signature_params.num_rows:
        parts.append(
            _nodes_from_unanchored(
                rt_signature_params,
                id_col="param_id",
                kind=NodeKind.RT_SIGNATURE_PARAM,
            )
        )
    if rt_members is not None and rt_members.num_rows:
        parts.append(
            _nodes_from_unanchored(rt_members, id_col="member_id", kind=NodeKind.RT_MEMBER)
        )
    return parts


def build_cpg_nodes_raw(
    *,
    inputs: NodeInputTables | None = None,
    options: NodeBuildOptions | None = None,
) -> pa.Table:
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
    tables = inputs or NodeInputTables()
    parts: list[pa.Table] = []

    for spec in NODE_PART_SPECS:
        if not getattr(options, spec.flag):
            continue
        for part in spec.builder(tables, options):
            _append_part(parts, part)

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
