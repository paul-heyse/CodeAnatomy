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
    SpanSpec,
    apply_query_and_project,
    attrs_map,
    ibis_plan_from_rows,
    iter_contexts,
    materialize_extract_plan,
    span_dict,
    text_from_file_ctx,
)
from extract.registry_specs import dataset_schema, normalize_options
from extract.schema_ops import ExtractNormalizeOptions
from ibis_engine.plan import IbisPlan

if TYPE_CHECKING:
    from extract.evidence_plan import EvidencePlan


@dataclass(frozen=True)
class ASTExtractOptions:
    """Define AST extraction options."""

    type_comments: bool = True
    feature_version: int | None = None
    repo_id: str | None = None


@dataclass(frozen=True)
class ASTExtractResult:
    """Hold extracted AST tables for nodes, edges, and errors."""

    ast_files: TableLike


AST_FILES_SCHEMA = dataset_schema("ast_files_v1")

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


def _syntax_error_row(exc: SyntaxError) -> dict[str, object]:
    lineno = _maybe_int(getattr(exc, "lineno", None))
    offset = _maybe_int(getattr(exc, "offset", None))
    end_lineno = _maybe_int(getattr(exc, "end_lineno", None))
    end_offset = _maybe_int(getattr(exc, "end_offset", None))
    return {
        "error_type": "SyntaxError",
        "message": str(exc),
        "span": span_dict(
            SpanSpec(
                start_line0=lineno - AST_LINE_BASE if lineno is not None else None,
                start_col=offset,
                end_line0=end_lineno - AST_LINE_BASE if end_lineno is not None else None,
                end_col=end_offset,
                end_exclusive=AST_END_EXCLUSIVE,
                col_unit=AST_COL_UNIT,
            )
        ),
        "attrs": attrs_map({}),
    }


def _exception_error_row(exc: Exception) -> dict[str, object]:
    return {
        "error_type": type(exc).__name__,
        "message": str(exc),
        "span": None,
        "attrs": attrs_map({}),
    }


def _parse_ast_text(
    text: str,
    *,
    filename: str,
    options: ASTExtractOptions,
) -> tuple[ast.AST | None, dict[str, object] | None]:
    try:
        return (
            ast.parse(
                text,
                filename=filename,
                mode="exec",
                type_comments=options.type_comments,
                feature_version=options.feature_version,
            ),
            None,
        )
    except SyntaxError as exc:
        return None, _syntax_error_row(exc)
    except (TypeError, ValueError) as exc:
        return None, _exception_error_row(exc)


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
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    nodes_rows: list[dict[str, object]] = []
    edges_rows: list[dict[str, object]] = []
    stack: list[tuple[ast.AST, int | None, str | None, int | None]] = [(root, None, None, None)]
    idx_map: dict[int, int] = {}

    while stack:
        node, parent_idx, field_name, field_pos = stack.pop()
        node_id = id(node)
        ast_id = idx_map.get(node_id)
        if ast_id is None:
            ast_id = len(idx_map)
            idx_map[node_id] = ast_id

        lineno = _maybe_int(getattr(node, "lineno", None))
        col_offset = _maybe_int(getattr(node, "col_offset", None))
        end_lineno = _maybe_int(getattr(node, "end_lineno", None))
        end_col_offset = _maybe_int(getattr(node, "end_col_offset", None))
        node_attrs = attrs_map(
            {
                "field_name": field_name,
                "field_pos": field_pos,
            }
        )

        nodes_rows.append(
            {
                "ast_id": ast_id,
                "parent_ast_id": parent_idx,
                "kind": type(node).__name__,
                "name": _node_name(node),
                "value": _node_value_repr(node),
                "span": span_dict(
                    SpanSpec(
                        start_line0=lineno - AST_LINE_BASE if lineno is not None else None,
                        start_col=col_offset,
                        end_line0=end_lineno - AST_LINE_BASE if end_lineno is not None else None,
                        end_col=end_col_offset,
                        end_exclusive=AST_END_EXCLUSIVE,
                        col_unit=AST_COL_UNIT,
                    )
                ),
                "attrs": node_attrs,
            }
        )

        if parent_idx is not None:
            edges_rows.append(
                {
                    "src": parent_idx,
                    "dst": ast_id,
                    "kind": "CHILD",
                    "slot": field_name,
                    "idx": field_pos,
                    "attrs": attrs_map({}),
                }
            )

        for child, field, pos in reversed(_iter_child_items(node)):
            stack.append((child, ast_id, field, pos))

    return nodes_rows, edges_rows


def _extract_ast_for_context(
    file_ctx: FileContext,
    *,
    options: ASTExtractOptions,
) -> dict[str, object] | None:
    if not file_ctx.file_id or not file_ctx.path:
        return None
    text = text_from_file_ctx(file_ctx)
    if not text:
        return None

    root, err = _parse_ast_text(
        text,
        filename=str(file_ctx.path),
        options=options,
    )
    nodes_rows: list[dict[str, object]] = []
    edges_rows: list[dict[str, object]] = []
    error_rows: list[dict[str, object]] = []
    if err is not None:
        error_rows.append(err)
    if root is not None:
        nodes_rows, edges_rows = _walk_ast(root)

    return {
        "repo": options.repo_id,
        "path": file_ctx.path,
        "file_id": file_ctx.file_id,
        "nodes": nodes_rows,
        "edges": edges_rows,
        "errors": error_rows,
        "attrs": attrs_map({"file_sha256": file_ctx.file_sha256}),
    }


def _extract_ast_for_row(
    row: dict[str, object],
    *,
    options: ASTExtractOptions,
) -> dict[str, object] | None:
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
        ast_files=cast(
            "TableLike",
            materialize_extract_plan(
                "ast_files_v1",
                plans["ast_files"],
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
    """Extract AST plans for nested file records.

    Returns
    -------
    dict[str, IbisPlan]
        Ibis plan bundle keyed by ``ast_files``.
    """
    normalized_options = normalize_options("ast", options, ASTExtractOptions)
    exec_context = context or ExtractExecutionContext()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    rows = _collect_ast_rows(
        repo_files,
        file_contexts=exec_context.file_contexts,
        options=normalized_options,
    )
    evidence_plan = exec_context.evidence_plan
    return {
        "ast_files": _build_ast_plan(
            "ast_files_v1",
            rows,
            row_schema=AST_FILES_SCHEMA,
            normalize=normalize,
            evidence_plan=evidence_plan,
        ),
    }


def _collect_ast_rows(
    repo_files: TableLike,
    *,
    file_contexts: Iterable[FileContext] | None,
    options: ASTExtractOptions,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for file_ctx in iter_contexts(repo_files, file_contexts):
        row = _extract_ast_for_context(file_ctx, options=options)
        if row is not None:
            rows.append(row)
    return rows


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
    return {
        "ast_files": materialize_extract_plan(
            "ast_files_v1",
            plans["ast_files"],
            ctx=ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=True,
            ),
        ),
    }
