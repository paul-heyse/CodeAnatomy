from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from .repo_scan import stable_id

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class SymtableExtractOptions:
    """
    Implements SYM-ALG-1 (per file) and emits:
      - py_sym_scopes
      - py_sym_symbols
      - py_sym_scope_edges
      - py_sym_namespace_edges
      - py_sym_function_partitions

    SYM-ALG-1 reference: :contentReference[oaicite:16]{index=16}
    """

    compile_type: str = "exec"


@dataclass(frozen=True)
class SymtableExtractResult:
    py_sym_scopes: pa.Table
    py_sym_symbols: pa.Table
    py_sym_scope_edges: pa.Table
    py_sym_namespace_edges: pa.Table
    py_sym_function_partitions: pa.Table


SCOPES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("scope_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("table_id", pa.int64()),
        ("scope_type", pa.string()),
        ("scope_name", pa.string()),
        ("lineno", pa.int32()),
        ("is_nested", pa.bool_()),
        ("is_optimized", pa.bool_()),
        ("has_children", pa.bool_()),
        ("scope_role", pa.string()),  # "runtime" | "type_meta" etc.
    ]
)

SYMBOLS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("symbol_row_id", pa.string()),
        ("scope_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("name", pa.string()),
        ("is_referenced", pa.bool_()),
        ("is_assigned", pa.bool_()),
        ("is_imported", pa.bool_()),
        ("is_annotated", pa.bool_()),
        ("is_parameter", pa.bool_()),
        ("is_global", pa.bool_()),
        ("is_declared_global", pa.bool_()),
        ("is_nonlocal", pa.bool_()),
        ("is_local", pa.bool_()),
        ("is_free", pa.bool_()),
        ("is_namespace", pa.bool_()),
    ]
)

SCOPE_EDGES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("edge_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("parent_scope_id", pa.string()),
        ("child_scope_id", pa.string()),
    ]
)

NAMESPACE_EDGES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("edge_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("scope_id", pa.string()),
        ("symbol_row_id", pa.string()),
        ("child_scope_id", pa.string()),
    ]
)

FUNC_PARTS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("scope_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("parameters", pa.list_(pa.string())),
        ("locals", pa.list_(pa.string())),
        ("globals", pa.list_(pa.string())),
        ("nonlocals", pa.list_(pa.string())),
        ("frees", pa.list_(pa.string())),
    ]
)


def extract_symtable(
    repo_files: pa.Table, options: SymtableExtractOptions | None = None
) -> SymtableExtractResult:
    import symtable as _symtable

    options = options or SymtableExtractOptions()

    scope_rows: list[dict] = []
    symbol_rows: list[dict] = []
    scope_edge_rows: list[dict] = []
    ns_edge_rows: list[dict] = []
    func_parts_rows: list[dict] = []

    # Per-file table cache: symtable objects are returned by get_children/get_namespaces.
    # We assign a stable scope_id per table_id within a file.
    for rf in repo_files.to_pylist():
        file_id = rf["file_id"]
        path = rf["path"]
        file_sha256 = rf.get("file_sha256")
        text = rf.get("text")
        if not text:
            # symtable needs decoded text
            b = rf.get("bytes")
            if b is not None:
                try:
                    text = b.decode(rf.get("encoding") or "utf-8", errors="replace")
                except Exception:
                    text = None
        if not text:
            continue

        try:
            top = _symtable.symtable(text, path, options.compile_type)
        except Exception:
            continue

        table_to_scope_id: dict[int, str] = {}

        def _scope_role(scope_type_str: str) -> str:
            # Non-module/function/class are meta scopes in 3.13+ typing/annotation system.
            if scope_type_str in ("MODULE", "FUNCTION", "CLASS"):
                return "runtime"
            return "type_meta"

        def ensure_scope_id(tbl) -> str:
            tid = int(tbl.get_id())
            sid = table_to_scope_id.get(tid)
            if sid is None:
                st = tbl.get_type()
                st_str = str(st).split(".")[-1]  # SymbolTableType.FUNCTION -> FUNCTION
                name = tbl.get_name()
                lineno = int(tbl.get_lineno() or 0)
                sid = stable_id("sym_scope", file_id, str(tid), st_str, name, str(lineno))
                table_to_scope_id[tid] = sid
            return sid

        # DFS walk of scope tree (SYM-ALG-1)
        stack = [(top, None)]
        while stack:
            tbl, parent_tbl = stack.pop()
            sid = ensure_scope_id(tbl)

            st = tbl.get_type()
            st_str = str(st).split(".")[-1]
            scope_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "scope_id": sid,
                    "file_id": file_id,
                    "path": path,
                    "file_sha256": file_sha256,
                    "table_id": int(tbl.get_id()),
                    "scope_type": st_str,
                    "scope_name": tbl.get_name(),
                    "lineno": int(tbl.get_lineno() or 0),
                    "is_nested": bool(tbl.is_nested()),
                    "is_optimized": bool(tbl.is_optimized()),
                    "has_children": bool(tbl.has_children()),
                    "scope_role": _scope_role(st_str),
                }
            )

            if parent_tbl is not None:
                psid = ensure_scope_id(parent_tbl)
                scope_edge_rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "edge_id": stable_id("sym_scope_edge", psid, sid),
                        "file_id": file_id,
                        "path": path,
                        "file_sha256": file_sha256,
                        "parent_scope_id": psid,
                        "child_scope_id": sid,
                    }
                )

            # Emit symbols for this table
            for s in tbl.get_symbols():
                name = s.get_name()
                sym_row_id = stable_id("sym_symbol", sid, name)
                row = {
                    "schema_version": SCHEMA_VERSION,
                    "symbol_row_id": sym_row_id,
                    "scope_id": sid,
                    "file_id": file_id,
                    "path": path,
                    "file_sha256": file_sha256,
                    "name": name,
                    "is_referenced": bool(s.is_referenced()),
                    "is_assigned": bool(s.is_assigned()),
                    "is_imported": bool(s.is_imported()),
                    "is_annotated": bool(s.is_annotated()),
                    "is_parameter": bool(s.is_parameter()),
                    "is_global": bool(s.is_global()),
                    "is_declared_global": bool(s.is_declared_global()),
                    "is_nonlocal": bool(s.is_nonlocal()),
                    "is_local": bool(s.is_local()),
                    "is_free": bool(s.is_free()),
                    "is_namespace": bool(s.is_namespace()),
                }
                symbol_rows.append(row)

                # Namespace edges: symbol -> namespace tables
                if s.is_namespace():
                    try:
                        for nt in s.get_namespaces():
                            child_sid = ensure_scope_id(nt)
                            ns_edge_rows.append(
                                {
                                    "schema_version": SCHEMA_VERSION,
                                    "edge_id": stable_id("sym_ns_edge", sym_row_id, child_sid),
                                    "file_id": file_id,
                                    "path": path,
                                    "file_sha256": file_sha256,
                                    "scope_id": sid,
                                    "symbol_row_id": sym_row_id,
                                    "child_scope_id": child_sid,
                                }
                            )
                    except Exception:
                        pass

            # Function partitions
            if st_str == "FUNCTION":
                try:
                    func_parts_rows.append(
                        {
                            "schema_version": SCHEMA_VERSION,
                            "scope_id": sid,
                            "file_id": file_id,
                            "path": path,
                            "file_sha256": file_sha256,
                            "parameters": list(tbl.get_parameters()),
                            "locals": list(tbl.get_locals()),
                            "globals": list(tbl.get_globals()),
                            "nonlocals": list(tbl.get_nonlocals()),
                            "frees": list(tbl.get_frees()),
                        }
                    )
                except Exception:
                    pass

            # Children
            try:
                for child in tbl.get_children():
                    stack.append((child, tbl))
            except Exception:
                pass

    return SymtableExtractResult(
        py_sym_scopes=pa.Table.from_pylist(scope_rows, schema=SCOPES_SCHEMA),
        py_sym_symbols=pa.Table.from_pylist(symbol_rows, schema=SYMBOLS_SCHEMA),
        py_sym_scope_edges=pa.Table.from_pylist(scope_edge_rows, schema=SCOPE_EDGES_SCHEMA),
        py_sym_namespace_edges=pa.Table.from_pylist(ns_edge_rows, schema=NAMESPACE_EDGES_SCHEMA),
        py_sym_function_partitions=pa.Table.from_pylist(func_parts_rows, schema=FUNC_PARTS_SCHEMA),
    )
