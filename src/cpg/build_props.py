from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

import pyarrow as pa

from ..arrowdsl.finalize import FinalizeResult, finalize
from ..arrowdsl.runtime import ExecutionContext
from .kinds import EntityKind, NodeKind, EdgeKind
from .schemas import SCHEMA_VERSION, CPG_PROPS_CONTRACT, CPG_PROPS_SCHEMA, empty_props


def _add_prop(rows: list[dict], *, entity_kind: EntityKind, entity_id: str, key: str, value: Any) -> None:
    if entity_id is None or key is None:
        return

    rec = {
        "schema_version": SCHEMA_VERSION,
        "entity_kind": entity_kind.value,
        "entity_id": str(entity_id),
        "prop_key": str(key),
        "value_str": None,
        "value_int": None,
        "value_float": None,
        "value_bool": None,
        "value_json": None,
    }

    if value is None:
        rows.append(rec)
        return

    if isinstance(value, bool):
        rec["value_bool"] = bool(value)
    elif isinstance(value, int) and not isinstance(value, bool):
        rec["value_int"] = int(value)
    elif isinstance(value, float):
        rec["value_float"] = float(value)
    elif isinstance(value, str):
        rec["value_str"] = value
    else:
        # lists/structs/dicts -> json
        try:
            rec["value_json"] = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        except Exception:
            rec["value_json"] = json.dumps(str(value), ensure_ascii=False)
    rows.append(rec)


def _empty() -> pa.Table:
    return pa.Table.from_arrays([pa.array([], type=f.type) for f in CPG_PROPS_SCHEMA], schema=CPG_PROPS_SCHEMA)


@dataclass(frozen=True)
class PropsBuildOptions:
    include_node_props: bool = True
    include_edge_props: bool = True

    # If True, include “heavy” JSON properties (qname candidate lists, docs, etc.)
    include_heavy_json_props: bool = True


def build_cpg_props_raw(
    *,
    repo_files: Optional[pa.Table] = None,
    cst_name_refs: Optional[pa.Table] = None,
    cst_imports: Optional[pa.Table] = None,
    cst_callsites: Optional[pa.Table] = None,
    cst_defs: Optional[pa.Table] = None,
    dim_qualified_names: Optional[pa.Table] = None,
    scip_symbol_information: Optional[pa.Table] = None,
    cpg_edges: Optional[pa.Table] = None,
    options: Optional[PropsBuildOptions] = None,
) -> pa.Table:
    """
    Build cpg_props without finalization.

    This intentionally does NOT require cpg_nodes. We derive entity_id from source tables
    (file_id/name_ref_id/import_id/call_id/def_id/symbol/qname_id) which are the canonical IDs.
    """
    options = options or PropsBuildOptions()
    rows: list[dict] = []

    if options.include_node_props:
        # --- File props ---
        if repo_files is not None and repo_files.num_rows:
            for r in repo_files.to_pylist():
                fid = r.get("file_id")
                if not fid:
                    continue
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=fid, key="node_kind", value=NodeKind.PY_FILE.value)
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=fid, key="path", value=r.get("path"))
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=fid, key="size_bytes", value=r.get("size_bytes"))
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=fid, key="file_sha256", value=r.get("file_sha256"))
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=fid, key="encoding", value=r.get("encoding"))

        # --- Name ref props ---
        if cst_name_refs is not None and cst_name_refs.num_rows:
            for r in cst_name_refs.to_pylist():
                nid = r.get("name_ref_id")
                if not nid:
                    continue
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=nid, key="node_kind", value=NodeKind.PY_NAME_REF.value)
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=nid, key="name", value=r.get("name"))
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=nid, key="expr_ctx", value=r.get("expr_ctx"))

        # --- Import alias props ---
        if cst_imports is not None and cst_imports.num_rows:
            id_key = "import_alias_id" if "import_alias_id" in cst_imports.column_names else "import_id"
            for r in cst_imports.to_pylist():
                iid = r.get(id_key)
                if not iid:
                    continue
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=iid, key="node_kind", value=NodeKind.PY_IMPORT_ALIAS.value)
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=iid, key="import_kind", value=r.get("kind"))
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=iid, key="module", value=r.get("module"))
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=iid, key="relative_level", value=r.get("relative_level"))
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=iid, key="name", value=r.get("name"))
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=iid, key="asname", value=r.get("asname"))
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=iid, key="is_star", value=r.get("is_star"))

        # --- Callsite props ---
        if cst_callsites is not None and cst_callsites.num_rows:
            for r in cst_callsites.to_pylist():
                cid = r.get("call_id")
                if not cid:
                    continue
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=cid, key="node_kind", value=NodeKind.PY_CALLSITE.value)
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=cid, key="callee_dotted", value=r.get("callee_dotted"))
                if options.include_heavy_json_props:
                    _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=cid, key="callee_qnames", value=r.get("callee_qnames"))

        # --- Def props ---
        if cst_defs is not None and cst_defs.num_rows:
            for r in cst_defs.to_pylist():
                did = r.get("def_id")
                if not did:
                    continue
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=did, key="node_kind", value=NodeKind.PY_DEF.value)
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=did, key="def_kind", value=r.get("kind"))
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=did, key="name", value=r.get("name"))
                if options.include_heavy_json_props:
                    _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=did, key="qnames", value=r.get("qnames"))
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=did, key="container_def_id", value=r.get("container_def_id"))

        # --- Qualified name props ---
        if dim_qualified_names is not None and dim_qualified_names.num_rows:
            for r in dim_qualified_names.to_pylist():
                qid = r.get("qname_id")
                if not qid:
                    continue
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=qid, key="node_kind", value=NodeKind.PY_QUALIFIED_NAME.value)
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=qid, key="qname", value=r.get("qname"))

        # --- Symbol props ---
        if scip_symbol_information is not None and scip_symbol_information.num_rows:
            for r in scip_symbol_information.to_pylist():
                sym = r.get("symbol")
                if not sym:
                    continue
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=sym, key="node_kind", value=NodeKind.PY_SYMBOL.value)
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=sym, key="display_name", value=r.get("display_name"))
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=sym, key="symbol_kind", value=r.get("kind"))
                _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=sym, key="enclosing_symbol", value=r.get("enclosing_symbol"))
                if options.include_heavy_json_props:
                    _add_prop(rows, entity_kind=EntityKind.NODE, entity_id=sym, key="documentation", value=r.get("documentation"))

    if options.include_edge_props and cpg_edges is not None and cpg_edges.num_rows:
        # Edge properties from edge columns (lightweight + query-friendly)
        for r in cpg_edges.to_pylist():
            eid = r.get("edge_id")
            if not eid:
                continue
            _add_prop(rows, entity_kind=EntityKind.EDGE, entity_id=eid, key="edge_kind", value=r.get("edge_kind"))
            _add_prop(rows, entity_kind=EntityKind.EDGE, entity_id=eid, key="origin", value=r.get("origin"))
            _add_prop(rows, entity_kind=EntityKind.EDGE, entity_id=eid, key="resolution_method", value=r.get("resolution_method"))
            _add_prop(rows, entity_kind=EntityKind.EDGE, entity_id=eid, key="confidence", value=r.get("confidence"))
            _add_prop(rows, entity_kind=EntityKind.EDGE, entity_id=eid, key="score", value=r.get("score"))
            _add_prop(rows, entity_kind=EntityKind.EDGE, entity_id=eid, key="symbol_roles", value=r.get("symbol_roles"))
            _add_prop(rows, entity_kind=EntityKind.EDGE, entity_id=eid, key="qname_source", value=r.get("qname_source"))
            _add_prop(rows, entity_kind=EntityKind.EDGE, entity_id=eid, key="ambiguity_group_id", value=r.get("ambiguity_group_id"))
            _add_prop(rows, entity_kind=EntityKind.EDGE, entity_id=eid, key="rule_name", value=r.get("rule_name"))
            _add_prop(rows, entity_kind=EntityKind.EDGE, entity_id=eid, key="rule_priority", value=r.get("rule_priority"))

    if not rows:
        return empty_props()

    # Let Arrow infer; finalize will align.
    return pa.Table.from_pylist(rows, schema=CPG_PROPS_SCHEMA)


def build_cpg_props(
    *,
    ctx: ExecutionContext,
    repo_files: Optional[pa.Table] = None,
    cst_name_refs: Optional[pa.Table] = None,
    cst_imports: Optional[pa.Table] = None,
    cst_callsites: Optional[pa.Table] = None,
    cst_defs: Optional[pa.Table] = None,
    dim_qualified_names: Optional[pa.Table] = None,
    scip_symbol_information: Optional[pa.Table] = None,
    cpg_edges: Optional[pa.Table] = None,
    options: Optional[PropsBuildOptions] = None,
) -> FinalizeResult:
    raw = build_cpg_props_raw(
        repo_files=repo_files,
        cst_name_refs=cst_name_refs,
        cst_imports=cst_imports,
        cst_callsites=cst_callsites,
        cst_defs=cst_defs,
        dim_qualified_names=dim_qualified_names,
        scip_symbol_information=scip_symbol_information,
        cpg_edges=cpg_edges,
        options=options,
    )
    return finalize(raw, contract=CPG_PROPS_CONTRACT, ctx=ctx)
