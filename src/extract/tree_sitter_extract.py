"""Extract tree-sitter nodes and diagnostics into Arrow tables using shared helpers."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, cast, overload

import tree_sitter_python
from tree_sitter import Language, Parser

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.ids import span_id
from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike
from extract.helpers import (
    ExtractExecutionContext,
    ExtractMaterializeOptions,
    FileContext,
    SpanSpec,
    apply_query_and_project,
    attrs_map,
    bytes_from_file_ctx,
    ibis_plan_from_rows,
    iter_contexts,
    materialize_extract_plan,
    span_dict,
)
from extract.registry_specs import dataset_schema, normalize_options
from extract.schema_ops import ExtractNormalizeOptions
from ibis_engine.plan import IbisPlan

if TYPE_CHECKING:
    from extract.evidence_plan import EvidencePlan

type Row = dict[str, object]


@dataclass(frozen=True)
class TreeSitterExtractOptions:
    """Configure tree-sitter extraction options."""

    include_nodes: bool = True
    include_errors: bool = True
    include_missing: bool = True
    repo_id: str | None = None


@dataclass(frozen=True)
class TreeSitterExtractResult:
    """Extracted tree-sitter tables for nodes and diagnostics."""

    tree_sitter_files: TableLike


TREE_SITTER_FILES_SCHEMA = dataset_schema("tree_sitter_files_v1")


def _parser() -> Parser:
    language = Language(tree_sitter_python.language())
    return Parser(language)


def _iter_nodes(root: object) -> Iterator[tuple[object, object | None]]:
    stack: list[tuple[object, object | None]] = [(root, None)]
    while stack:
        node, parent = stack.pop()
        yield node, parent
        children = getattr(node, "children", [])
        stack.extend((child, node) for child in reversed(children))


def _node_entry(
    node: object,
    *,
    file_ctx: FileContext,
    parent: object | None,
) -> Row:
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", 0))
    ts_type = str(getattr(node, "type", ""))
    node_id = span_id(file_ctx.path, start, end, kind=ts_type)
    parent_id = None
    if parent is not None:
        parent_start = int(getattr(parent, "start_byte", 0))
        parent_end = int(getattr(parent, "end_byte", 0))
        parent_type = str(getattr(parent, "type", ""))
        parent_id = span_id(file_ctx.path, parent_start, parent_end, kind=parent_type)
    return {
        "node_id": node_id,
        "parent_id": parent_id,
        "kind": ts_type,
        "span": span_dict(
            SpanSpec(
                start_line0=None,
                start_col=None,
                end_line0=None,
                end_col=None,
                end_exclusive=True,
                col_unit="byte",
                byte_start=start,
                byte_len=end - start,
            )
        ),
        "flags": {
            "is_named": bool(getattr(node, "is_named", False)),
            "has_error": bool(getattr(node, "has_error", False)),
            "is_error": bool(getattr(node, "is_error", False)),
            "is_missing": bool(getattr(node, "is_missing", False)),
        },
        "attrs": attrs_map({}),
    }


def _error_entry(node: object, *, file_ctx: FileContext) -> Row:
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", 0))
    error_id = span_id(file_ctx.path, start, end, kind="ts_error")
    node_id = span_id(file_ctx.path, start, end, kind=str(getattr(node, "type", "")))
    return {
        "error_id": error_id,
        "node_id": node_id,
        "span": span_dict(
            SpanSpec(
                start_line0=None,
                start_col=None,
                end_line0=None,
                end_col=None,
                end_exclusive=True,
                col_unit="byte",
                byte_start=start,
                byte_len=end - start,
            )
        ),
        "attrs": attrs_map({}),
    }


def _missing_entry(node: object, *, file_ctx: FileContext) -> Row:
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", 0))
    missing_id = span_id(file_ctx.path, start, end, kind="ts_missing")
    node_id = span_id(file_ctx.path, start, end, kind=str(getattr(node, "type", "")))
    return {
        "missing_id": missing_id,
        "node_id": node_id,
        "span": span_dict(
            SpanSpec(
                start_line0=None,
                start_col=None,
                end_line0=None,
                end_col=None,
                end_exclusive=True,
                col_unit="byte",
                byte_start=start,
                byte_len=end - start,
            )
        ),
        "attrs": attrs_map({}),
    }


def extract_ts(
    repo_files: TableLike,
    *,
    options: TreeSitterExtractOptions | None = None,
    context: ExtractExecutionContext | None = None,
) -> TreeSitterExtractResult:
    """Extract tree-sitter nodes and diagnostics from repo files.

    Parameters
    ----------
    repo_files:
        Repo files table with bytes/text.
    options:
        Extraction options.
    context:
        Shared execution context bundle for extraction.

    Returns
    -------
    TreeSitterExtractResult
        Extracted tree-sitter file table.
    """
    normalized_options = normalize_options("tree_sitter", options, TreeSitterExtractOptions)
    exec_context = context or ExtractExecutionContext()
    exec_ctx = exec_context.ensure_ctx()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    plans = extract_ts_plans(
        repo_files,
        options=normalized_options,
        context=exec_context,
    )
    return TreeSitterExtractResult(
        tree_sitter_files=cast(
            "TableLike",
            materialize_extract_plan(
                "tree_sitter_files_v1",
                plans["tree_sitter_files"],
                ctx=exec_ctx,
                options=ExtractMaterializeOptions(
                    normalize=normalize,
                    apply_post_kernels=True,
                ),
            ),
        ),
    )


def extract_ts_plans(
    repo_files: TableLike,
    *,
    options: TreeSitterExtractOptions | None = None,
    context: ExtractExecutionContext | None = None,
) -> dict[str, IbisPlan]:
    """Extract tree-sitter plans for nested file records.

    Returns
    -------
    dict[str, IbisPlan]
        Ibis plan bundle keyed by ``tree_sitter_files``.
    """
    normalized_options = normalize_options("tree_sitter", options, TreeSitterExtractOptions)
    exec_context = context or ExtractExecutionContext()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    rows = _collect_ts_rows(
        repo_files,
        options=normalized_options,
        file_contexts=exec_context.file_contexts,
    )
    evidence_plan = exec_context.evidence_plan
    return {
        "tree_sitter_files": _build_ts_plan(
            "tree_sitter_files_v1",
            rows,
            row_schema=TREE_SITTER_FILES_SCHEMA,
            normalize=normalize,
            evidence_plan=evidence_plan,
        ),
    }


def _collect_ts_rows(
    repo_files: TableLike,
    *,
    options: TreeSitterExtractOptions,
    file_contexts: Iterable[FileContext] | None,
) -> list[Row]:
    parser = _parser()
    rows: list[Row] = []
    for file_ctx in iter_contexts(repo_files, file_contexts):
        row = _extract_ts_file_row(
            file_ctx,
            parser=parser,
            options=options,
        )
        if row is not None:
            rows.append(row)
    return rows


def _build_ts_plan(
    name: str,
    rows: list[Row],
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


def _extract_ts_file_row(
    file_ctx: FileContext,
    *,
    parser: Parser,
    options: TreeSitterExtractOptions,
) -> Row | None:
    if not file_ctx.file_id or not file_ctx.path:
        return None
    data = bytes_from_file_ctx(file_ctx)
    if data is None:
        return None
    tree = parser.parse(data)
    root = tree.root_node
    include_nodes = options.include_nodes
    include_errors = options.include_errors
    include_missing = options.include_missing
    node_rows: list[Row] = []
    error_rows: list[Row] = []
    missing_rows: list[Row] = []
    for node, parent in _iter_nodes(root):
        if include_nodes:
            node_rows.append(
                _node_entry(
                    node,
                    file_ctx=file_ctx,
                    parent=parent,
                )
            )
        if include_errors and bool(getattr(node, "is_error", False)):
            error_rows.append(_error_entry(node, file_ctx=file_ctx))
        if include_missing and bool(getattr(node, "is_missing", False)):
            missing_rows.append(_missing_entry(node, file_ctx=file_ctx))

    return {
        "repo": options.repo_id,
        "path": file_ctx.path,
        "file_id": file_ctx.file_id,
        "nodes": node_rows if include_nodes else [],
        "errors": error_rows,
        "missing": missing_rows,
        "attrs": attrs_map({"file_sha256": file_ctx.file_sha256}),
    }


class _TreeSitterTablesKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: TreeSitterExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
    profile: str
    context: ExtractExecutionContext | None
    prefer_reader: bool


class _TreeSitterTablesKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: TreeSitterExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
    profile: str
    context: ExtractExecutionContext | None
    prefer_reader: Literal[False]


class _TreeSitterTablesKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: TreeSitterExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
    profile: str
    context: ExtractExecutionContext | None
    prefer_reader: Required[Literal[True]]


@overload
def extract_ts_tables(
    **kwargs: Unpack[_TreeSitterTablesKwargsTable],
) -> Mapping[str, TableLike]: ...


@overload
def extract_ts_tables(
    **kwargs: Unpack[_TreeSitterTablesKwargsReader],
) -> Mapping[str, TableLike | RecordBatchReaderLike]: ...


def extract_ts_tables(
    **kwargs: Unpack[_TreeSitterTablesKwargs],
) -> Mapping[str, TableLike | RecordBatchReaderLike]:
    """Extract tree-sitter tables as a name-keyed bundle.

    Parameters
    ----------
    kwargs:
        Keyword-only arguments for extraction (repo_files, options, context, file_contexts,
        evidence_plan, ctx, profile, prefer_reader).

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Extracted tree-sitter outputs keyed by output name.
    """
    repo_files = kwargs["repo_files"]
    normalized_options = normalize_options(
        "tree_sitter",
        kwargs.get("options"),
        TreeSitterExtractOptions,
    )
    context = kwargs.get("context")
    if context is None:
        context = ExtractExecutionContext(
            file_contexts=kwargs.get("file_contexts"),
            evidence_plan=kwargs.get("evidence_plan"),
            ctx=kwargs.get("ctx"),
            profile=kwargs.get("profile", "default"),
        )
    exec_ctx = context.ensure_ctx()
    prefer_reader = kwargs.get("prefer_reader", False)
    normalize = ExtractNormalizeOptions(options=normalized_options)
    plans = extract_ts_plans(
        repo_files,
        options=normalized_options,
        context=context,
    )
    return {
        "tree_sitter_files": materialize_extract_plan(
            "tree_sitter_files_v1",
            plans["tree_sitter_files"],
            ctx=exec_ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=True,
            ),
        ),
    }
