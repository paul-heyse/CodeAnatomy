"""Extract Python AST facts into Arrow tables."""

from __future__ import annotations

import ast
from collections.abc import Iterable
from dataclasses import dataclass

import arrowdsl.pyarrow_core as pa
from arrowdsl.predicates import FilterSpec, InSet
from arrowdsl.pyarrow_protocols import TableLike
from extract.file_context import FileContext, iter_file_contexts
from schema_spec.core import ArrowFieldSpec
from schema_spec.factories import make_table_spec
from schema_spec.fields import file_identity_bundle
from schema_spec.registry import GLOBAL_SCHEMA_REGISTRY

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ASTExtractOptions:
    """Define AST extraction options."""

    type_comments: bool = True
    feature_version: int | None = None


@dataclass(frozen=True)
class ASTExtractResult:
    """Hold extracted AST tables for nodes, edges, and errors."""

    py_ast_nodes: TableLike
    py_ast_edges: TableLike
    py_ast_errors: TableLike


AST_NODES_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
        name="py_ast_nodes_v1",
        version=SCHEMA_VERSION,
        bundles=(file_identity_bundle(),),
        fields=[
            ArrowFieldSpec(name="ast_idx", dtype=pa.int32()),
            ArrowFieldSpec(name="parent_ast_idx", dtype=pa.int32()),
            ArrowFieldSpec(name="field_name", dtype=pa.string()),
            ArrowFieldSpec(name="field_pos", dtype=pa.int32()),
            ArrowFieldSpec(name="kind", dtype=pa.string()),
            ArrowFieldSpec(name="name", dtype=pa.string()),
            ArrowFieldSpec(name="value_repr", dtype=pa.string()),
            ArrowFieldSpec(name="lineno", dtype=pa.int32()),
            ArrowFieldSpec(name="col_offset", dtype=pa.int32()),
            ArrowFieldSpec(name="end_lineno", dtype=pa.int32()),
            ArrowFieldSpec(name="end_col_offset", dtype=pa.int32()),
        ],
    )
)

AST_EDGES_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
        name="py_ast_edges_v1",
        version=SCHEMA_VERSION,
        bundles=(file_identity_bundle(),),
        fields=[
            ArrowFieldSpec(name="parent_ast_idx", dtype=pa.int32()),
            ArrowFieldSpec(name="child_ast_idx", dtype=pa.int32()),
            ArrowFieldSpec(name="field_name", dtype=pa.string()),
            ArrowFieldSpec(name="field_pos", dtype=pa.int32()),
        ],
    )
)

AST_ERRORS_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
        name="py_ast_errors_v1",
        version=SCHEMA_VERSION,
        bundles=(file_identity_bundle(),),
        fields=[
            ArrowFieldSpec(name="error_type", dtype=pa.string()),
            ArrowFieldSpec(name="message", dtype=pa.string()),
            ArrowFieldSpec(name="lineno", dtype=pa.int32()),
            ArrowFieldSpec(name="offset", dtype=pa.int32()),
            ArrowFieldSpec(name="end_lineno", dtype=pa.int32()),
            ArrowFieldSpec(name="end_offset", dtype=pa.int32()),
        ],
    )
)

AST_NODES_SCHEMA = AST_NODES_SPEC.to_arrow_schema()
AST_EDGES_SCHEMA = AST_EDGES_SPEC.to_arrow_schema()
AST_ERRORS_SCHEMA = AST_ERRORS_SPEC.to_arrow_schema()


def _maybe_int(value: object | None) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value) if value.isdigit() else None
    return None


def _node_name(node: ast.AST) -> str | None:
    name = getattr(node, "name", None)
    if isinstance(name, str):
        return name
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
        return repr(node.value)
    return None


def _row_text(file_ctx: FileContext) -> str | None:
    text = file_ctx.text
    if text is not None:
        return text
    if file_ctx.data is None:
        return None
    encoding = file_ctx.encoding or "utf-8"
    try:
        return file_ctx.data.decode(encoding, errors="replace")
    except UnicodeError:
        return None


def _syntax_error_row(
    file_id: object,
    path: object,
    file_sha256: object,
    exc: SyntaxError,
) -> dict[str, object]:
    return {
        "file_id": file_id,
        "path": path,
        "file_sha256": file_sha256,
        "error_type": "SyntaxError",
        "message": str(exc),
        "lineno": _maybe_int(getattr(exc, "lineno", None)),
        "offset": _maybe_int(getattr(exc, "offset", None)),
        "end_lineno": _maybe_int(getattr(exc, "end_lineno", None)),
        "end_offset": _maybe_int(getattr(exc, "end_offset", None)),
    }


def _exception_error_row(
    file_id: object,
    path: object,
    file_sha256: object,
    exc: Exception,
) -> dict[str, object]:
    return {
        "file_id": file_id,
        "path": path,
        "file_sha256": file_sha256,
        "error_type": type(exc).__name__,
        "message": str(exc),
        "lineno": None,
        "offset": None,
        "end_lineno": None,
        "end_offset": None,
    }


def _parse_ast_text(
    text: str,
    *,
    path: object,
    file_id: object,
    file_sha256: object,
    options: ASTExtractOptions,
) -> tuple[ast.AST | None, dict[str, object] | None]:
    try:
        return (
            ast.parse(
                text,
                filename=str(path),
                mode="exec",
                type_comments=options.type_comments,
                feature_version=options.feature_version,
            ),
            None,
        )
    except SyntaxError as exc:
        return None, _syntax_error_row(file_id, path, file_sha256, exc)
    except (TypeError, ValueError) as exc:
        return None, _exception_error_row(file_id, path, file_sha256, exc)


def _iter_child_items(node: ast.AST) -> list[tuple[ast.AST, str, int]]:
    items: list[tuple[ast.AST, str, int]] = []
    for field, value in ast.iter_fields(node):
        if isinstance(value, ast.AST):
            items.append((value, field, 0))
        elif isinstance(value, list):
            items.extend(
                (item, field, idx) for idx, item in enumerate(value) if isinstance(item, ast.AST)
            )
    return items


def _walk_ast(
    root: ast.AST,
    *,
    file_id: object,
    path: object,
    file_sha256: object,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    nodes_rows: list[dict[str, object]] = []
    edges_rows: list[dict[str, object]] = []
    stack: list[tuple[ast.AST, int | None, str | None, int | None]] = [(root, None, None, None)]
    idx_map: dict[int, int] = {}

    while stack:
        node, parent_idx, field_name, field_pos = stack.pop()
        node_id = id(node)
        ast_idx = idx_map.get(node_id)
        if ast_idx is None:
            ast_idx = len(idx_map)
            idx_map[node_id] = ast_idx

        nodes_rows.append(
            {
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
                "lineno": _maybe_int(getattr(node, "lineno", None)),
                "col_offset": _maybe_int(getattr(node, "col_offset", None)),
                "end_lineno": _maybe_int(getattr(node, "end_lineno", None)),
                "end_col_offset": _maybe_int(getattr(node, "end_col_offset", None)),
            }
        )

        for child, field, pos in reversed(_iter_child_items(node)):
            child_id = id(child)
            child_idx = idx_map.get(child_id)
            if child_idx is None:
                child_idx = len(idx_map)
                idx_map[child_id] = child_idx
            edges_rows.append(
                {
                    "file_id": file_id,
                    "path": path,
                    "file_sha256": file_sha256,
                    "parent_ast_idx": ast_idx,
                    "child_ast_idx": child_idx,
                    "field_name": field,
                    "field_pos": pos,
                }
            )
            stack.append((child, ast_idx, field, pos))

    return nodes_rows, edges_rows


def _extract_ast_for_context(
    file_ctx: FileContext,
    *,
    options: ASTExtractOptions,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    if not file_ctx.file_id or not file_ctx.path:
        return [], [], []
    text = _row_text(file_ctx)
    if not text:
        return [], [], []

    root, err = _parse_ast_text(
        text,
        path=file_ctx.path,
        file_id=file_ctx.file_id,
        file_sha256=file_ctx.file_sha256,
        options=options,
    )
    if err is not None:
        return [], [], [err]
    if root is None:
        return [], [], []

    nodes_rows, edges_rows = _walk_ast(
        root,
        file_id=file_ctx.file_id,
        path=file_ctx.path,
        file_sha256=file_ctx.file_sha256,
    )
    return nodes_rows, edges_rows, []

def _extract_ast_for_row(
    row: dict[str, object],
    *,
    options: ASTExtractOptions,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    file_ctx = FileContext.from_repo_row(row)
    return _extract_ast_for_context(file_ctx, options=options)


def extract_ast(
    repo_files: TableLike,
    options: ASTExtractOptions | None = None,
    *,
    file_contexts: Iterable[FileContext] | None = None,
) -> ASTExtractResult:
    """Extract a minimal AST fact set per file.

    Returns
    -------
    ASTExtractResult
        Tables of AST nodes, edges, and errors.
    """
    options = options or ASTExtractOptions()

    nodes_rows: list[dict[str, object]] = []
    edges_rows: list[dict[str, object]] = []
    err_rows: list[dict[str, object]] = []

    contexts = file_contexts if file_contexts is not None else iter_file_contexts(repo_files)
    for file_ctx in contexts:
        nodes, edges, errs = _extract_ast_for_context(file_ctx, options=options)
        nodes_rows.extend(nodes)
        edges_rows.extend(edges)
        err_rows.extend(errs)

    t_nodes = pa.Table.from_pylist(nodes_rows, schema=AST_NODES_SCHEMA)
    t_edges = pa.Table.from_pylist(edges_rows, schema=AST_EDGES_SCHEMA)
    t_errs = pa.Table.from_pylist(err_rows, schema=AST_ERRORS_SCHEMA)
    return ASTExtractResult(py_ast_nodes=t_nodes, py_ast_edges=t_edges, py_ast_errors=t_errs)


def extract_ast_tables(
    *,
    repo_root: str | None,
    repo_files: TableLike,
    file_contexts: Iterable[FileContext] | None = None,
    ctx: object | None = None,
) -> dict[str, TableLike]:
    """Extract AST tables as a name-keyed bundle.

    Parameters
    ----------
    repo_root:
        Optional repository root (unused).
    repo_files:
        Repo files table.
    file_contexts:
        Optional pre-built file contexts for extraction.
    ctx:
        Execution context (unused).

    Returns
    -------
    dict[str, pyarrow.Table]
        Extracted AST tables keyed by output name.
    """
    _ = (repo_root, ctx)
    result = extract_ast(repo_files, file_contexts=file_contexts)
    if result.py_ast_nodes.num_rows:
        predicate = InSet(
            col="kind",
            values=("FunctionDef", "AsyncFunctionDef", "ClassDef"),
        )
        defs_table = FilterSpec(predicate).apply_kernel(result.py_ast_nodes)
    else:
        defs_table = result.py_ast_nodes

    return {
        "ast_nodes": result.py_ast_nodes,
        "ast_edges": result.py_ast_edges,
        "ast_defs": defs_table,
    }
