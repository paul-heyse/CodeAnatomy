"""Extract tree-sitter nodes and diagnostics into Arrow tables using shared helpers."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, cast, overload

import tree_sitter_python
from tree_sitter import Language, Parser

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike
from extract.helpers import (
    ExtractExecutionContext,
    ExtractMaterializeOptions,
    FileContext,
    apply_query_and_project,
    bytes_from_file_ctx,
    file_identity_row,
    ibis_plan_from_rows,
    iter_contexts,
    materialize_extract_plan,
)
from extract.registry_specs import dataset_enabled, dataset_row_schema, normalize_options
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


@dataclass(frozen=True)
class TreeSitterExtractResult:
    """Extracted tree-sitter tables for nodes and diagnostics."""

    ts_nodes: TableLike
    ts_errors: TableLike
    ts_missing: TableLike


@dataclass
class TreeSitterRowBuffers:
    """Mutable buffers for tree-sitter extraction."""

    node_rows: list[Row]
    error_rows: list[Row]
    missing_rows: list[Row]


TS_NODES_ROW_SCHEMA = dataset_row_schema("ts_nodes_v1")
TS_ERRORS_ROW_SCHEMA = dataset_row_schema("ts_errors_v1")
TS_MISSING_ROW_SCHEMA = dataset_row_schema("ts_missing_v1")


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


def _node_row(
    node: object,
    *,
    file_ctx: FileContext,
    parent: object | None,
) -> Row:
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", 0))
    ts_type = str(getattr(node, "type", ""))
    parent_type = str(getattr(parent, "type", "")) if parent is not None else None
    parent_start = int(getattr(parent, "start_byte", 0)) if parent is not None else None
    parent_end = int(getattr(parent, "end_byte", 0)) if parent is not None else None
    return {
        **file_identity_row(file_ctx),
        "ts_type": ts_type,
        "start_byte": start,
        "end_byte": end,
        "parent_ts_type": parent_type,
        "parent_start_byte": parent_start,
        "parent_end_byte": parent_end,
        "is_named": bool(getattr(node, "is_named", False)),
        "has_error": bool(getattr(node, "has_error", False)),
        "is_error": bool(getattr(node, "is_error", False)),
        "is_missing": bool(getattr(node, "is_missing", False)),
    }


def _error_row(node_row: Row) -> Row:
    return {
        "file_id": node_row.get("file_id"),
        "path": node_row.get("path"),
        "file_sha256": node_row.get("file_sha256"),
        "ts_type": node_row.get("ts_type"),
        "start_byte": node_row.get("start_byte"),
        "end_byte": node_row.get("end_byte"),
        "is_error": True,
    }


def _missing_row(node_row: Row) -> Row:
    return {
        "file_id": node_row.get("file_id"),
        "path": node_row.get("path"),
        "file_sha256": node_row.get("file_sha256"),
        "ts_type": node_row.get("ts_type"),
        "start_byte": node_row.get("start_byte"),
        "end_byte": node_row.get("end_byte"),
        "is_missing": True,
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
        Extracted node and diagnostic tables.
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
        ts_nodes=cast(
            "TableLike",
            materialize_extract_plan(
                "ts_nodes_v1",
                plans["ts_nodes"],
                ctx=exec_ctx,
                options=ExtractMaterializeOptions(
                    normalize=normalize,
                    apply_post_kernels=True,
                ),
            ),
        ),
        ts_errors=cast(
            "TableLike",
            materialize_extract_plan(
                "ts_errors_v1",
                plans["ts_errors"],
                ctx=exec_ctx,
                options=ExtractMaterializeOptions(
                    normalize=normalize,
                    apply_post_kernels=True,
                ),
            ),
        ),
        ts_missing=cast(
            "TableLike",
            materialize_extract_plan(
                "ts_missing_v1",
                plans["ts_missing"],
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
    """Extract tree-sitter plans for nodes and diagnostics.

    Returns
    -------
    dict[str, IbisPlan]
        Ibis plan bundle keyed by ``ts_nodes``, ``ts_errors``, and ``ts_missing``.
    """
    normalized_options = normalize_options("tree_sitter", options, TreeSitterExtractOptions)
    exec_context = context or ExtractExecutionContext()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    buffers = _collect_ts_buffers(
        repo_files,
        options=normalized_options,
        file_contexts=exec_context.file_contexts,
    )
    evidence_plan = exec_context.evidence_plan
    return {
        "ts_nodes": _build_ts_plan(
            "ts_nodes_v1",
            buffers.node_rows,
            row_schema=TS_NODES_ROW_SCHEMA,
            normalize=normalize,
            evidence_plan=evidence_plan,
        ),
        "ts_errors": _build_ts_plan(
            "ts_errors_v1",
            buffers.error_rows,
            row_schema=TS_ERRORS_ROW_SCHEMA,
            normalize=normalize,
            evidence_plan=evidence_plan,
        ),
        "ts_missing": _build_ts_plan(
            "ts_missing_v1",
            buffers.missing_rows,
            row_schema=TS_MISSING_ROW_SCHEMA,
            normalize=normalize,
            evidence_plan=evidence_plan,
        ),
    }


def _collect_ts_buffers(
    repo_files: TableLike,
    *,
    options: TreeSitterExtractOptions,
    file_contexts: Iterable[FileContext] | None,
) -> TreeSitterRowBuffers:
    parser = _parser()
    buffers = TreeSitterRowBuffers(
        node_rows=[],
        error_rows=[],
        missing_rows=[],
    )
    for file_ctx in iter_contexts(repo_files, file_contexts):
        _extract_ts_for_row(
            file_ctx,
            parser=parser,
            options=options,
            buffers=buffers,
        )
    return buffers


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


def _extract_ts_for_row(
    file_ctx: FileContext,
    *,
    parser: Parser,
    options: TreeSitterExtractOptions,
    buffers: TreeSitterRowBuffers,
) -> None:
    if not file_ctx.file_id or not file_ctx.path:
        return
    data = bytes_from_file_ctx(file_ctx)
    if data is None:
        return
    tree = parser.parse(data)
    root = tree.root_node
    include_nodes = dataset_enabled("ts_nodes_v1", options)
    include_errors = dataset_enabled("ts_errors_v1", options)
    include_missing = dataset_enabled("ts_missing_v1", options)
    for node, parent in _iter_nodes(root):
        row = _node_row(
            node,
            file_ctx=file_ctx,
            parent=parent,
        )
        if include_nodes:
            buffers.node_rows.append(row)
        if include_errors and bool(row.get("is_error")):
            buffers.error_rows.append(_error_row(row))
        if include_missing and bool(row.get("is_missing")):
            buffers.missing_rows.append(_missing_row(row))


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
        "ts_nodes": materialize_extract_plan(
            "ts_nodes_v1",
            plans["ts_nodes"],
            ctx=exec_ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=True,
            ),
        ),
        "ts_errors": materialize_extract_plan(
            "ts_errors_v1",
            plans["ts_errors"],
            ctx=exec_ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=True,
            ),
        ),
        "ts_missing": materialize_extract_plan(
            "ts_missing_v1",
            plans["ts_missing"],
            ctx=exec_ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=True,
            ),
        ),
    }
