"""Extract tree-sitter nodes and diagnostics into Arrow tables using shared helpers."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Literal, Required, TypedDict, Unpack, overload

import tree_sitter_python
from tree_sitter import Language, Parser

from arrowdsl.core.context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.runner import materialize_plan, run_plan_bundle
from arrowdsl.plan.scan_io import plan_from_rows
from arrowdsl.schema.schema import SchemaMetadataSpec
from extract.helpers import (
    FileContext,
    align_plan,
    bytes_from_file_ctx,
    file_identity_row,
    iter_contexts,
)
from extract.registry_specs import (
    dataset_enabled,
    dataset_metadata_with_options,
    dataset_query,
    dataset_row_schema,
    dataset_schema,
)

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


TS_NODES_QUERY = dataset_query("ts_nodes_v1")
TS_ERRORS_QUERY = dataset_query("ts_errors_v1")
TS_MISSING_QUERY = dataset_query("ts_missing_v1")

TS_NODES_SCHEMA = dataset_schema("ts_nodes_v1")
TS_ERRORS_SCHEMA = dataset_schema("ts_errors_v1")
TS_MISSING_SCHEMA = dataset_schema("ts_missing_v1")

TS_NODES_ROW_SCHEMA = dataset_row_schema("ts_nodes_v1")
TS_ERRORS_ROW_SCHEMA = dataset_row_schema("ts_errors_v1")
TS_MISSING_ROW_SCHEMA = dataset_row_schema("ts_missing_v1")


def _ts_metadata_specs(
    options: TreeSitterExtractOptions,
) -> dict[str, SchemaMetadataSpec]:
    return {
        "ts_nodes": dataset_metadata_with_options("ts_nodes_v1", options=options),
        "ts_errors": dataset_metadata_with_options("ts_errors_v1", options=options),
        "ts_missing": dataset_metadata_with_options("ts_missing_v1", options=options),
    }


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
    file_contexts: Iterable[FileContext] | None = None,
    ctx: ExecutionContext | None = None,
) -> TreeSitterExtractResult:
    """Extract tree-sitter nodes and diagnostics from repo files.

    Parameters
    ----------
    repo_files:
        Repo files table with bytes/text.
    options:
        Extraction options.
    file_contexts:
        Optional pre-built file contexts for extraction.
    options:
        Tree-sitter extraction options.
    ctx:
        Execution context for plan execution.

    Returns
    -------
    TreeSitterExtractResult
        Extracted node and diagnostic tables.
    """
    options = options or TreeSitterExtractOptions()
    exec_ctx = ctx or execution_context_factory("default")
    plans = extract_ts_plans(
        repo_files,
        options=options,
        file_contexts=file_contexts,
        ctx=exec_ctx,
    )
    metadata_specs = _ts_metadata_specs(options)
    return TreeSitterExtractResult(
        ts_nodes=materialize_plan(
            plans["ts_nodes"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["ts_nodes"],
            attach_ordering_metadata=True,
        ),
        ts_errors=materialize_plan(
            plans["ts_errors"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["ts_errors"],
            attach_ordering_metadata=True,
        ),
        ts_missing=materialize_plan(
            plans["ts_missing"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["ts_missing"],
            attach_ordering_metadata=True,
        ),
    )


def extract_ts_plans(
    repo_files: TableLike,
    *,
    options: TreeSitterExtractOptions | None = None,
    file_contexts: Iterable[FileContext] | None = None,
    ctx: ExecutionContext | None = None,
) -> dict[str, Plan]:
    """Extract tree-sitter plans for nodes and diagnostics.

    Returns
    -------
    dict[str, Plan]
        Plan bundle keyed by ``ts_nodes``, ``ts_errors``, and ``ts_missing``.
    """
    options = options or TreeSitterExtractOptions()
    exec_ctx = ctx or execution_context_factory("default")
    parser = _parser()

    node_rows: list[Row] = []
    error_rows: list[Row] = []
    missing_rows: list[Row] = []
    buffers = TreeSitterRowBuffers(
        node_rows=node_rows,
        error_rows=error_rows,
        missing_rows=missing_rows,
    )

    for file_ctx in iter_contexts(repo_files, file_contexts):
        _extract_ts_for_row(
            file_ctx,
            parser=parser,
            options=options,
            buffers=buffers,
        )

    nodes_plan = plan_from_rows(node_rows, schema=TS_NODES_ROW_SCHEMA, label="ts_nodes")
    nodes_plan = TS_NODES_QUERY.apply_to_plan(nodes_plan, ctx=exec_ctx)
    nodes_plan = align_plan(nodes_plan, schema=TS_NODES_SCHEMA, ctx=exec_ctx)

    errors_plan = plan_from_rows(error_rows, schema=TS_ERRORS_ROW_SCHEMA, label="ts_errors")
    errors_plan = TS_ERRORS_QUERY.apply_to_plan(errors_plan, ctx=exec_ctx)
    errors_plan = align_plan(errors_plan, schema=TS_ERRORS_SCHEMA, ctx=exec_ctx)

    missing_plan = plan_from_rows(missing_rows, schema=TS_MISSING_ROW_SCHEMA, label="ts_missing")
    missing_plan = TS_MISSING_QUERY.apply_to_plan(missing_plan, ctx=exec_ctx)
    missing_plan = align_plan(missing_plan, schema=TS_MISSING_SCHEMA, ctx=exec_ctx)
    return {
        "ts_nodes": nodes_plan,
        "ts_errors": errors_plan,
        "ts_missing": missing_plan,
    }


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
    ctx: ExecutionContext | None
    prefer_reader: bool


class _TreeSitterTablesKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: TreeSitterExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    ctx: ExecutionContext | None
    prefer_reader: Literal[False]


class _TreeSitterTablesKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: TreeSitterExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    ctx: ExecutionContext | None
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
        Keyword-only arguments for extraction (repo_files, options, file_contexts, ctx,
        prefer_reader).

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Extracted tree-sitter outputs keyed by output name.
    """
    repo_files = kwargs["repo_files"]
    options = kwargs.get("options") or TreeSitterExtractOptions()
    file_contexts = kwargs.get("file_contexts")
    exec_ctx = kwargs.get("ctx") or execution_context_factory("default")
    prefer_reader = kwargs.get("prefer_reader", False)
    plans = extract_ts_plans(
        repo_files,
        options=options,
        file_contexts=file_contexts,
        ctx=exec_ctx,
    )
    return run_plan_bundle(
        plans,
        ctx=exec_ctx,
        prefer_reader=prefer_reader,
        metadata_specs=_ts_metadata_specs(options),
        attach_ordering_metadata=True,
    )
