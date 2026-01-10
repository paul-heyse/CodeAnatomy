from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from ..arrowdsl.finalize import FinalizeResult, finalize
from ..arrowdsl.runtime import ExecutionContext
from .kinds import NodeKind
from .schemas import CPG_NODES_CONTRACT, CPG_NODES_SCHEMA, SCHEMA_VERSION, empty_nodes


def _const_str(n: int, value: str) -> pa.Array:
    return pa.array([value] * n, type=pa.string())


def _const_i32(n: int, value: int) -> pa.Array:
    return pa.array([int(value)] * n, type=pa.int32())


def _pick_col(t: pa.Table, names: Sequence[str]) -> pa.ChunkedArray | None:
    for n in names:
        if n in t.column_names:
            return t[n]
    return None


def _nodes_from_anchored(
    t: pa.Table,
    *,
    id_col: str,
    kind: NodeKind,
    path_col: str = "path",
    bstart_col: str = "bstart",
    bend_col: str = "bend",
    file_id_col: str | None = "file_id",
) -> pa.Table:
    n = t.num_rows
    if n == 0:
        return empty_nodes()

    node_id = t[id_col]
    path = t[path_col] if path_col in t.column_names else pa.array([None] * n, type=pa.string())
    bstart = (
        t[bstart_col] if bstart_col in t.column_names else pa.array([None] * n, type=pa.int64())
    )
    bend = t[bend_col] if bend_col in t.column_names else pa.array([None] * n, type=pa.int64())

    file_id = None
    if file_id_col is not None and file_id_col in t.column_names:
        file_id = t[file_id_col]
    else:
        file_id = pa.array([None] * n, type=pa.string())

    out = pa.Table.from_arrays(
        [
            _const_i32(n, SCHEMA_VERSION),
            node_id,
            _const_str(n, kind.value),
            path,
            bstart,
            bend,
            file_id,
        ],
        schema=CPG_NODES_SCHEMA,
    )
    return out


@dataclass(frozen=True)
class NodeBuildOptions:
    include_file_nodes: bool = True
    include_name_ref_nodes: bool = True
    include_import_alias_nodes: bool = True
    include_callsite_nodes: bool = True
    include_def_nodes: bool = True
    include_symbol_nodes: bool = True
    include_qname_nodes: bool = True

    # For file nodes: anchor to full file byte span (0..size_bytes)
    file_span_from_size_bytes: bool = True


def build_cpg_nodes_raw(
    *,
    repo_files: pa.Table | None = None,
    cst_name_refs: pa.Table | None = None,
    cst_imports: pa.Table | None = None,
    cst_callsites: pa.Table | None = None,
    cst_defs: pa.Table | None = None,
    dim_qualified_names: pa.Table | None = None,
    scip_symbol_information: pa.Table | None = None,
    scip_occurrences: pa.Table | None = None,
    options: NodeBuildOptions | None = None,
) -> pa.Table:
    """
    Build cpg_nodes without finalization (schema alignment/dedupe/sort happens in build_cpg_nodes()).
    """
    options = options or NodeBuildOptions()
    parts: list[pa.Table] = []

    # PY_FILE nodes
    if options.include_file_nodes and repo_files is not None and repo_files.num_rows:
        # node_id = file_id, path = path, bstart=0, bend=size_bytes (if configured)
        n = repo_files.num_rows
        file_id = _pick_col(repo_files, ["file_id"])
        path = _pick_col(repo_files, ["path"])
        size = _pick_col(repo_files, ["size_bytes"])
        if file_id is None or path is None:
            pass
        else:
            if options.file_span_from_size_bytes and size is not None:
                bstart = pa.array([0] * n, type=pa.int64())
                bend = pa.array(
                    [int(x) if x is not None else None for x in size.to_pylist()], type=pa.int64()
                )
            else:
                bstart = pa.array([None] * n, type=pa.int64())
                bend = pa.array([None] * n, type=pa.int64())

            t = pa.Table.from_arrays(
                [
                    _const_i32(n, SCHEMA_VERSION),
                    file_id,
                    _const_str(n, NodeKind.PY_FILE.value),
                    path,
                    bstart,
                    bend,
                    file_id,  # file_id column self-ref
                ],
                schema=CPG_NODES_SCHEMA,
            )
            parts.append(t)

    # PY_NAME_REF nodes
    if options.include_name_ref_nodes and cst_name_refs is not None and cst_name_refs.num_rows:
        parts.append(
            _nodes_from_anchored(
                cst_name_refs,
                id_col="name_ref_id",
                kind=NodeKind.PY_NAME_REF,
                path_col="path",
                bstart_col="bstart",
                bend_col="bend",
                file_id_col="file_id" if "file_id" in cst_name_refs.column_names else None,
            )
        )

    # PY_IMPORT_ALIAS nodes (id column may be import_id or import_alias_id)
    if options.include_import_alias_nodes and cst_imports is not None and cst_imports.num_rows:
        id_col = "import_alias_id" if "import_alias_id" in cst_imports.column_names else "import_id"
        # ensure (bstart,bend) exists; normalize layer usually adds it
        parts.append(
            _nodes_from_anchored(
                cst_imports,
                id_col=id_col,
                kind=NodeKind.PY_IMPORT_ALIAS,
                path_col="path",
                bstart_col="bstart" if "bstart" in cst_imports.column_names else "alias_bstart",
                bend_col="bend" if "bend" in cst_imports.column_names else "alias_bend",
                file_id_col="file_id" if "file_id" in cst_imports.column_names else None,
            )
        )

    # PY_CALLSITE nodes
    if options.include_callsite_nodes and cst_callsites is not None and cst_callsites.num_rows:
        # Anchor callsite to call span (not callee span)
        bstart_col = "call_bstart" if "call_bstart" in cst_callsites.column_names else "bstart"
        bend_col = "call_bend" if "call_bend" in cst_callsites.column_names else "bend"
        parts.append(
            _nodes_from_anchored(
                cst_callsites,
                id_col="call_id",
                kind=NodeKind.PY_CALLSITE,
                path_col="path",
                bstart_col=bstart_col,
                bend_col=bend_col,
                file_id_col="file_id" if "file_id" in cst_callsites.column_names else None,
            )
        )

    # PY_DEF nodes (functions/classes)
    if options.include_def_nodes and cst_defs is not None and cst_defs.num_rows:
        bstart_col = "bstart" if "bstart" in cst_defs.column_names else "name_bstart"
        bend_col = "bend" if "bend" in cst_defs.column_names else "name_bend"
        parts.append(
            _nodes_from_anchored(
                cst_defs,
                id_col="def_id",
                kind=NodeKind.PY_DEF,
                path_col="path",
                bstart_col=bstart_col,
                bend_col=bend_col,
                file_id_col="file_id" if "file_id" in cst_defs.column_names else None,
            )
        )

    # PY_QUALIFIED_NAME nodes (dimension)
    if (
        options.include_qname_nodes
        and dim_qualified_names is not None
        and dim_qualified_names.num_rows
    ):
        # dim_qualified_names expected to contain qname_id and qname
        if "qname_id" in dim_qualified_names.column_names:
            parts.append(
                _nodes_from_anchored(
                    dim_qualified_names,
                    id_col="qname_id",
                    kind=NodeKind.PY_QUALIFIED_NAME,
                    path_col="path",  # likely missing; will become null
                    bstart_col="bstart",
                    bend_col="bend",
                    file_id_col=None,
                )
            )

    # PY_SYMBOL nodes
    if options.include_symbol_nodes:
        sym_ids: list[str] = []
        if scip_symbol_information is not None and "symbol" in scip_symbol_information.column_names:
            sym_ids.extend([s for s in scip_symbol_information["symbol"].to_pylist() if s])
        if scip_occurrences is not None and "symbol" in scip_occurrences.column_names:
            sym_ids.extend([s for s in scip_occurrences["symbol"].to_pylist() if s])

        if sym_ids:
            # distinct, deterministic
            seen = set()
            uniq = []
            for s in sym_ids:
                if s not in seen:
                    seen.add(s)
                    uniq.append(s)
            uniq.sort()

            n = len(uniq)
            t = pa.Table.from_arrays(
                [
                    _const_i32(n, SCHEMA_VERSION),
                    pa.array(uniq, type=pa.string()),
                    _const_str(n, NodeKind.PY_SYMBOL.value),
                    pa.array([None] * n, type=pa.string()),
                    pa.array([None] * n, type=pa.int64()),
                    pa.array([None] * n, type=pa.int64()),
                    pa.array([None] * n, type=pa.string()),
                ],
                schema=CPG_NODES_SCHEMA,
            )
            parts.append(t)

    if not parts:
        return empty_nodes()

    return pa.concat_tables(parts, promote=True)


def build_cpg_nodes(
    *,
    ctx: ExecutionContext,
    repo_files: pa.Table | None = None,
    cst_name_refs: pa.Table | None = None,
    cst_imports: pa.Table | None = None,
    cst_callsites: pa.Table | None = None,
    cst_defs: pa.Table | None = None,
    dim_qualified_names: pa.Table | None = None,
    scip_symbol_information: pa.Table | None = None,
    scip_occurrences: pa.Table | None = None,
    options: NodeBuildOptions | None = None,
) -> FinalizeResult:
    """
    Build + finalize cpg_nodes (schema alignment, dedupe, canonical sort, error table).
    """
    raw = build_cpg_nodes_raw(
        repo_files=repo_files,
        cst_name_refs=cst_name_refs,
        cst_imports=cst_imports,
        cst_callsites=cst_callsites,
        cst_defs=cst_defs,
        dim_qualified_names=dim_qualified_names,
        scip_symbol_information=scip_symbol_information,
        scip_occurrences=scip_occurrences,
        options=options,
    )
    return finalize(raw, contract=CPG_NODES_CONTRACT, ctx=ctx)
