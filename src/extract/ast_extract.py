"""Extract Python AST facts into Arrow tables using shared helpers."""

from __future__ import annotations

import ast
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Literal, Required, TypedDict, Unpack, overload

from arrowdsl.core.context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.runner import materialize_plan, run_plan_bundle
from arrowdsl.plan.scan_io import plan_from_rows
from extract.helpers import (
    FileContext,
    align_plan,
    ast_def_nodes_plan,
    file_identity_row,
    iter_contexts,
    text_from_file_ctx,
)
from extract.registry_specs import (
    dataset_metadata_with_options,
    dataset_query,
    dataset_row_schema,
    dataset_schema,
    normalize_options,
)


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


AST_NODES_QUERY = dataset_query("py_ast_nodes_v1")
AST_EDGES_QUERY = dataset_query("py_ast_edges_v1")
AST_ERRORS_QUERY = dataset_query("py_ast_errors_v1")

AST_NODES_SCHEMA = dataset_schema("py_ast_nodes_v1")
AST_EDGES_SCHEMA = dataset_schema("py_ast_edges_v1")
AST_ERRORS_SCHEMA = dataset_schema("py_ast_errors_v1")

AST_NODES_ROW_SCHEMA = dataset_row_schema("py_ast_nodes_v1")
AST_EDGES_ROW_SCHEMA = dataset_row_schema("py_ast_edges_v1")
AST_ERRORS_ROW_SCHEMA = dataset_row_schema("py_ast_errors_v1")


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
    file_ctx: FileContext,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    nodes_rows: list[dict[str, object]] = []
    edges_rows: list[dict[str, object]] = []
    stack: list[tuple[ast.AST, int | None, str | None, int | None]] = [(root, None, None, None)]
    idx_map: dict[int, int] = {}
    identity = file_identity_row(file_ctx)

    while stack:
        node, parent_idx, field_name, field_pos = stack.pop()
        node_id = id(node)
        ast_idx = idx_map.get(node_id)
        if ast_idx is None:
            ast_idx = len(idx_map)
            idx_map[node_id] = ast_idx

        nodes_rows.append(
            {
                **identity,
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
                    **identity,
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
    text = text_from_file_ctx(file_ctx)
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
        file_ctx=file_ctx,
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
    ctx: ExecutionContext | None = None,
) -> ASTExtractResult:
    """Extract a minimal AST fact set per file.

    Returns
    -------
    ASTExtractResult
        Tables of AST nodes, edges, and errors.
    """
    normalized_options = normalize_options("ast", options, ASTExtractOptions)
    ctx = ctx or execution_context_factory("default")
    plans = extract_ast_plans(
        repo_files,
        options=normalized_options,
        file_contexts=file_contexts,
        ctx=ctx,
    )
    nodes_meta = dataset_metadata_with_options("py_ast_nodes_v1", options=normalized_options)
    edges_meta = dataset_metadata_with_options("py_ast_edges_v1", options=normalized_options)
    errors_meta = dataset_metadata_with_options("py_ast_errors_v1", options=normalized_options)
    return ASTExtractResult(
        py_ast_nodes=materialize_plan(
            plans["ast_nodes"],
            ctx=ctx,
            metadata_spec=nodes_meta,
            attach_ordering_metadata=True,
        ),
        py_ast_edges=materialize_plan(
            plans["ast_edges"],
            ctx=ctx,
            metadata_spec=edges_meta,
            attach_ordering_metadata=True,
        ),
        py_ast_errors=materialize_plan(
            plans["ast_errors"],
            ctx=ctx,
            metadata_spec=errors_meta,
            attach_ordering_metadata=True,
        ),
    )


def extract_ast_plans(
    repo_files: TableLike,
    options: ASTExtractOptions | None = None,
    *,
    file_contexts: Iterable[FileContext] | None = None,
    ctx: ExecutionContext | None = None,
) -> dict[str, Plan]:
    """Extract AST plans for nodes, edges, and errors.

    Returns
    -------
    dict[str, Plan]
        Plan bundle keyed by ``ast_nodes``, ``ast_edges``, and ``ast_errors``.
    """
    normalized_options = normalize_options("ast", options, ASTExtractOptions)
    ctx = ctx or execution_context_factory("default")

    nodes_rows: list[dict[str, object]] = []
    edges_rows: list[dict[str, object]] = []
    err_rows: list[dict[str, object]] = []

    for file_ctx in iter_contexts(repo_files, file_contexts):
        nodes, edges, errs = _extract_ast_for_context(file_ctx, options=normalized_options)
        nodes_rows.extend(nodes)
        edges_rows.extend(edges)
        err_rows.extend(errs)

    nodes_plan = plan_from_rows(nodes_rows, schema=AST_NODES_ROW_SCHEMA, label="ast_nodes")
    nodes_plan = AST_NODES_QUERY.apply_to_plan(nodes_plan, ctx=ctx)
    nodes_plan = align_plan(
        nodes_plan,
        schema=AST_NODES_SCHEMA,
        available=AST_NODES_SCHEMA.names,
        ctx=ctx,
    )

    edges_plan = plan_from_rows(edges_rows, schema=AST_EDGES_ROW_SCHEMA, label="ast_edges")
    edges_plan = AST_EDGES_QUERY.apply_to_plan(edges_plan, ctx=ctx)
    edges_plan = align_plan(
        edges_plan,
        schema=AST_EDGES_SCHEMA,
        available=AST_EDGES_SCHEMA.names,
        ctx=ctx,
    )

    errs_plan = plan_from_rows(err_rows, schema=AST_ERRORS_ROW_SCHEMA, label="ast_errors")
    errs_plan = AST_ERRORS_QUERY.apply_to_plan(errs_plan, ctx=ctx)
    errs_plan = align_plan(
        errs_plan,
        schema=AST_ERRORS_SCHEMA,
        available=AST_ERRORS_SCHEMA.names,
        ctx=ctx,
    )

    return {
        "ast_nodes": nodes_plan,
        "ast_edges": edges_plan,
        "ast_errors": errs_plan,
    }


class _AstTablesKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: ASTExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    ctx: ExecutionContext | None
    prefer_reader: bool


class _AstTablesKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: ASTExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    ctx: ExecutionContext | None
    prefer_reader: Literal[False]


class _AstTablesKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: ASTExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    ctx: ExecutionContext | None
    prefer_reader: Required[Literal[True]]


@overload
def extract_ast_tables(
    **kwargs: Unpack[_AstTablesKwargsTable],
) -> Mapping[str, TableLike]: ...


@overload
def extract_ast_tables(
    **kwargs: Unpack[_AstTablesKwargsReader],
) -> Mapping[str, TableLike | RecordBatchReaderLike]: ...


def extract_ast_tables(
    **kwargs: Unpack[_AstTablesKwargs],
) -> Mapping[str, TableLike | RecordBatchReaderLike]:
    """Extract AST tables as a name-keyed bundle.

    Parameters
    ----------
    kwargs:
        Keyword-only arguments for extraction (repo_files, options, file_contexts, ctx,
        prefer_reader).

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Extracted AST outputs keyed by name.
    """
    repo_files = kwargs["repo_files"]
    normalized_options = normalize_options("ast", kwargs.get("options"), ASTExtractOptions)
    file_contexts = kwargs.get("file_contexts")
    ctx = kwargs.get("ctx") or execution_context_factory("default")
    prefer_reader = kwargs.get("prefer_reader", False)
    plans = extract_ast_plans(
        repo_files,
        options=normalized_options,
        file_contexts=file_contexts,
        ctx=ctx,
    )
    defs_plan = ast_def_nodes_plan(plan=plans["ast_nodes"])
    nodes_meta = dataset_metadata_with_options(
        "py_ast_nodes_v1",
        options=normalized_options,
    )
    edges_meta = dataset_metadata_with_options(
        "py_ast_edges_v1",
        options=normalized_options,
    )

    return run_plan_bundle(
        {
            "ast_nodes": plans["ast_nodes"],
            "ast_edges": plans["ast_edges"],
            "ast_defs": defs_plan,
        },
        ctx=ctx,
        prefer_reader=prefer_reader,
        metadata_specs={
            "ast_nodes": nodes_meta,
            "ast_edges": edges_meta,
            "ast_defs": nodes_meta,
        },
        attach_ordering_metadata=True,
    )
