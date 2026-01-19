"""Extract Python AST facts into Arrow tables using shared helpers."""

from __future__ import annotations

import ast
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, cast, overload

from arrowdsl.core.execution_context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike
from extract.helpers import (
    ExtractExecutionContext,
    ExtractMaterializeOptions,
    FileContext,
    apply_query_and_project,
    ast_def_nodes_plan,
    file_identity_row,
    ibis_plan_from_rows,
    iter_contexts,
    materialize_extract_plan,
    text_from_file_ctx,
)
from extract.registry_specs import dataset_row_schema, normalize_options
from extract.schema_ops import ExtractNormalizeOptions
from ibis_engine.plan import IbisPlan

if TYPE_CHECKING:
    from extract.evidence_plan import EvidencePlan


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


AST_NODES_ROW_SCHEMA = dataset_row_schema("py_ast_nodes_v1")
AST_EDGES_ROW_SCHEMA = dataset_row_schema("py_ast_edges_v1")
AST_ERRORS_ROW_SCHEMA = dataset_row_schema("py_ast_errors_v1")

AST_LINE_BASE = 1
AST_COL_UNIT = "utf32"
AST_END_EXCLUSIVE = True


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
        "line_base": AST_LINE_BASE,
        "col_unit": AST_COL_UNIT,
        "end_exclusive": AST_END_EXCLUSIVE,
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
        "line_base": AST_LINE_BASE,
        "col_unit": AST_COL_UNIT,
        "end_exclusive": AST_END_EXCLUSIVE,
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
                "line_base": AST_LINE_BASE,
                "col_unit": AST_COL_UNIT,
                "end_exclusive": AST_END_EXCLUSIVE,
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
    context: ExtractExecutionContext | None = None,
) -> ASTExtractResult:
    """Extract a minimal AST fact set per file.

    Returns
    -------
    ASTExtractResult
        Tables of AST nodes, edges, and errors.
    """
    normalized_options = normalize_options("ast", options, ASTExtractOptions)
    exec_context = context or ExtractExecutionContext()
    ctx = exec_context.ensure_ctx()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    plans = extract_ast_plans(
        repo_files,
        options=normalized_options,
        context=exec_context,
    )
    return ASTExtractResult(
        py_ast_nodes=cast(
            "TableLike",
            materialize_extract_plan(
                "py_ast_nodes_v1",
                plans["ast_nodes"],
                ctx=ctx,
                options=ExtractMaterializeOptions(
                    normalize=normalize,
                    apply_post_kernels=True,
                ),
            ),
        ),
        py_ast_edges=cast(
            "TableLike",
            materialize_extract_plan(
                "py_ast_edges_v1",
                plans["ast_edges"],
                ctx=ctx,
                options=ExtractMaterializeOptions(
                    normalize=normalize,
                    apply_post_kernels=True,
                ),
            ),
        ),
        py_ast_errors=cast(
            "TableLike",
            materialize_extract_plan(
                "py_ast_errors_v1",
                plans["ast_errors"],
                ctx=ctx,
                options=ExtractMaterializeOptions(
                    normalize=normalize,
                    apply_post_kernels=True,
                ),
            ),
        ),
    )


def extract_ast_plans(
    repo_files: TableLike,
    options: ASTExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> dict[str, IbisPlan]:
    """Extract AST plans for nodes, edges, and errors.

    Returns
    -------
    dict[str, IbisPlan]
        Ibis plan bundle keyed by ``ast_nodes``, ``ast_edges``, and ``ast_errors``.
    """
    normalized_options = normalize_options("ast", options, ASTExtractOptions)
    exec_context = context or ExtractExecutionContext()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    nodes_rows, edges_rows, err_rows = _collect_ast_rows(
        repo_files,
        file_contexts=exec_context.file_contexts,
        options=normalized_options,
    )
    evidence_plan = exec_context.evidence_plan
    return {
        "ast_nodes": _build_ast_plan(
            "py_ast_nodes_v1",
            nodes_rows,
            row_schema=AST_NODES_ROW_SCHEMA,
            normalize=normalize,
            evidence_plan=evidence_plan,
        ),
        "ast_edges": _build_ast_plan(
            "py_ast_edges_v1",
            edges_rows,
            row_schema=AST_EDGES_ROW_SCHEMA,
            normalize=normalize,
            evidence_plan=evidence_plan,
        ),
        "ast_errors": _build_ast_plan(
            "py_ast_errors_v1",
            err_rows,
            row_schema=AST_ERRORS_ROW_SCHEMA,
            normalize=normalize,
            evidence_plan=evidence_plan,
        ),
    }


def _collect_ast_rows(
    repo_files: TableLike,
    *,
    file_contexts: Iterable[FileContext] | None,
    options: ASTExtractOptions,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    nodes_rows: list[dict[str, object]] = []
    edges_rows: list[dict[str, object]] = []
    err_rows: list[dict[str, object]] = []
    for file_ctx in iter_contexts(repo_files, file_contexts):
        nodes, edges, errs = _extract_ast_for_context(file_ctx, options=options)
        nodes_rows.extend(nodes)
        edges_rows.extend(edges)
        err_rows.extend(errs)
    return nodes_rows, edges_rows, err_rows


def _build_ast_plan(
    name: str,
    rows: list[dict[str, object]],
    *,
    row_schema: SchemaLike,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None,
) -> IbisPlan:
    raw = ibis_plan_from_rows(name, rows, row_schema=row_schema)
    return apply_query_and_project(
        name,
        raw.expr,
        normalize=normalize,
        evidence_plan=evidence_plan,
        repo_id=normalize.repo_id,
    )


class _AstTablesKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: ASTExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
    profile: str
    prefer_reader: bool


class _AstTablesKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: ASTExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
    profile: str
    prefer_reader: Literal[False]


class _AstTablesKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: ASTExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
    profile: str
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
        Keyword-only arguments for extraction (repo_files, options, file_contexts, ctx, profile,
        prefer_reader).

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Extracted AST outputs keyed by name.
    """
    repo_files = kwargs["repo_files"]
    normalized_options = normalize_options("ast", kwargs.get("options"), ASTExtractOptions)
    file_contexts = kwargs.get("file_contexts")
    evidence_plan = kwargs.get("evidence_plan")
    profile = kwargs.get("profile", "default")
    ctx = kwargs.get("ctx") or execution_context_factory(profile)
    prefer_reader = kwargs.get("prefer_reader", False)
    exec_context = ExtractExecutionContext(
        file_contexts=file_contexts,
        evidence_plan=evidence_plan,
        ctx=ctx,
        profile=profile,
    )
    normalize = ExtractNormalizeOptions(options=normalized_options)
    plans = extract_ast_plans(
        repo_files,
        options=normalized_options,
        context=exec_context,
    )
    defs_plan = ast_def_nodes_plan(plan=plans["ast_nodes"])
    return {
        "ast_nodes": materialize_extract_plan(
            "py_ast_nodes_v1",
            plans["ast_nodes"],
            ctx=ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=True,
            ),
        ),
        "ast_edges": materialize_extract_plan(
            "py_ast_edges_v1",
            plans["ast_edges"],
            ctx=ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=True,
            ),
        ),
        "ast_defs": materialize_extract_plan(
            "py_ast_nodes_v1",
            defs_plan,
            ctx=ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=True,
            ),
        ),
    }
