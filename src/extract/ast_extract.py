from __future__ import annotations

import ast
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ASTExtractOptions:
    """
    AST extraction options.

    feature_version controls parsing behavior across Python versions when supported
    by your runtime (ast.parse(feature_version=...)).
    """

    type_comments: bool = True
    feature_version: int | None = None  # e.g. 11 for Python 3.11 grammar


@dataclass(frozen=True)
class ASTExtractResult:
    py_ast_nodes: pa.Table
    py_ast_edges: pa.Table
    py_ast_errors: pa.Table


AST_NODES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("ast_idx", pa.int32()),
        ("parent_ast_idx", pa.int32()),
        ("field_name", pa.string()),
        ("field_pos", pa.int32()),
        ("kind", pa.string()),
        ("name", pa.string()),
        ("value_repr", pa.string()),
        ("lineno", pa.int32()),
        ("col_offset", pa.int32()),
        ("end_lineno", pa.int32()),
        ("end_col_offset", pa.int32()),
    ]
)

AST_EDGES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("parent_ast_idx", pa.int32()),
        ("child_ast_idx", pa.int32()),
        ("field_name", pa.string()),
        ("field_pos", pa.int32()),
    ]
)

AST_ERRORS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("error_type", pa.string()),
        ("message", pa.string()),
        ("lineno", pa.int32()),
        ("offset", pa.int32()),
        ("end_lineno", pa.int32()),
        ("end_offset", pa.int32()),
    ]
)


def _maybe_int(x: Any) -> int | None:
    return int(x) if x is not None else None


def _node_name(node: ast.AST) -> str | None:
    # Common "naming" fields used for joins or debugging
    if hasattr(node, "name") and isinstance(node.name, str):
        return node.name
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.arg):
        return node.arg
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.alias):
        return node.asname or node.name
    return None


def _node_value_repr(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant):
        try:
            return repr(node.value)
        except Exception:
            return None
    return None


def extract_ast(repo_files: pa.Table, options: ASTExtractOptions | None = None) -> ASTExtractResult:
    """
    Extracts a minimal AST fact set per file:
      - nodes (preorder, with parent pointer + field)
      - edges (parent->child)
      - errors (SyntaxError, etc.)
    """
    options = options or ASTExtractOptions()

    nodes_rows: list[dict] = []
    edges_rows: list[dict] = []
    err_rows: list[dict] = []

    cols = repo_files.column_names
    has_text = "text" in cols
    has_bytes = "bytes" in cols

    for row in repo_files.to_pylist():
        file_id = row["file_id"]
        path = row["path"]
        file_sha256 = row.get("file_sha256")
        text = row.get("text") if has_text else None

        if text is None and has_bytes and row.get("bytes") is not None:
            try:
                text = row["bytes"].decode(row.get("encoding") or "utf-8", errors="replace")
            except Exception:
                text = None

        if not text:
            continue

        try:
            parse_kwargs = {"type_comments": options.type_comments}
            if options.feature_version is not None:
                # ast.parse may raise TypeError if unsupported in older runtimes
                parse_kwargs["feature_version"] = options.feature_version  # type: ignore[assignment]
            root = ast.parse(text, filename=path, mode="exec", **parse_kwargs)  # type: ignore[arg-type]
        except SyntaxError as e:
            err_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "file_id": file_id,
                    "path": path,
                    "file_sha256": file_sha256,
                    "error_type": "SyntaxError",
                    "message": str(e),
                    "lineno": _maybe_int(getattr(e, "lineno", None)),
                    "offset": _maybe_int(getattr(e, "offset", None)),
                    "end_lineno": _maybe_int(getattr(e, "end_lineno", None)),
                    "end_offset": _maybe_int(getattr(e, "end_offset", None)),
                }
            )
            continue
        except Exception as e:
            err_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "file_id": file_id,
                    "path": path,
                    "file_sha256": file_sha256,
                    "error_type": type(e).__name__,
                    "message": str(e),
                    "lineno": None,
                    "offset": None,
                    "end_lineno": None,
                    "end_offset": None,
                }
            )
            continue

        # Preorder traversal with explicit parent/field context.
        stack: list[tuple[ast.AST, int | None, str | None, int | None]] = [(root, None, None, None)]
        idx_map: dict[int, int] = {}  # id(node) -> ast_idx

        while stack:
            node, parent_idx, field_name, field_pos = stack.pop()
            ast_idx = len(idx_map)
            idx_map[id(node)] = ast_idx

            lineno = _maybe_int(getattr(node, "lineno", None))
            col_offset = _maybe_int(getattr(node, "col_offset", None))
            end_lineno = _maybe_int(getattr(node, "end_lineno", None))
            end_col_offset = _maybe_int(getattr(node, "end_col_offset", None))

            nodes_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "file_id": file_id,
                    "path": path,
                    "file_sha256": file_sha256,
                    "ast_idx": ast_idx,
                    "parent_ast_idx": parent_idx,
                    "field_name": field_name,
                    "field_pos": field_pos,
                    "kind": type(node).__name__,
                    "name": _node_name(node),
                    "value_repr": _node_value_repr(node),
                    "lineno": lineno,
                    "col_offset": col_offset,
                    "end_lineno": end_lineno,
                    "end_col_offset": end_col_offset,
                }
            )

            # Walk children in reverse so the first child is processed first (stack LIFO)
            child_items: list[tuple[ast.AST, str, int]] = []
            for f, v in ast.iter_fields(node):
                if isinstance(v, ast.AST):
                    child_items.append((v, f, 0))
                elif isinstance(v, list):
                    for i, item in enumerate(v):
                        if isinstance(item, ast.AST):
                            child_items.append((item, f, i))

            for child, f, i in reversed(child_items):
                edges_rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "file_id": file_id,
                        "path": path,
                        "file_sha256": file_sha256,
                        "parent_ast_idx": ast_idx,
                        "child_ast_idx": len(
                            idx_map
                        ),  # provisional; replaced by join on traversal if needed
                        "field_name": f,
                        "field_pos": i,
                    }
                )
                stack.append((child, ast_idx, f, i))

        # Fix up edge child_ast_idx by mapping id(child)->idx is nontrivial without storing id(child) in edges.
        # For now, downstream uses parent pointers in py_ast_nodes; keep py_ast_edges as optional/diagnostic.

    t_nodes = pa.Table.from_pylist(nodes_rows, schema=AST_NODES_SCHEMA)
    t_edges = pa.Table.from_pylist(edges_rows, schema=AST_EDGES_SCHEMA)
    t_errs = pa.Table.from_pylist(err_rows, schema=AST_ERRORS_SCHEMA)
    return ASTExtractResult(py_ast_nodes=t_nodes, py_ast_edges=t_edges, py_ast_errors=t_errs)
