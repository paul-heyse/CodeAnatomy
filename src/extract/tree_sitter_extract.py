"""Extract tree-sitter nodes and diagnostics into Arrow tables using shared helpers."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass

import pyarrow as pa
import tree_sitter_python
from tree_sitter import Language, Parser

from arrowdsl.core.context import ExecutionContext, RuntimeProfile
from arrowdsl.core.ids import HashSpec
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.plan import Plan
from extract.common import bytes_from_file_ctx, file_identity_row, iter_contexts
from extract.file_context import FileContext
from extract.hashing import apply_hash_projection
from extract.spec_helpers import register_dataset
from extract.tables import (
    align_plan,
    apply_query_spec,
    finalize_plan_bundle,
    materialize_plan,
    plan_from_rows,
    query_for_schema,
)
from schema_spec.specs import ArrowFieldSpec, file_identity_bundle

SCHEMA_VERSION = 1

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


TS_NODES_SPEC = register_dataset(
    name="ts_nodes_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=[
        ArrowFieldSpec(name="ts_node_id", dtype=pa.string()),
        ArrowFieldSpec(name="parent_ts_id", dtype=pa.string()),
        ArrowFieldSpec(name="ts_type", dtype=pa.string()),
        ArrowFieldSpec(name="start_byte", dtype=pa.int64()),
        ArrowFieldSpec(name="end_byte", dtype=pa.int64()),
        ArrowFieldSpec(name="is_named", dtype=pa.bool_()),
        ArrowFieldSpec(name="has_error", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_error", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_missing", dtype=pa.bool_()),
    ],
)

TS_ERRORS_SPEC = register_dataset(
    name="ts_errors_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=[
        ArrowFieldSpec(name="ts_error_id", dtype=pa.string()),
        ArrowFieldSpec(name="ts_node_id", dtype=pa.string()),
        ArrowFieldSpec(name="ts_type", dtype=pa.string()),
        ArrowFieldSpec(name="start_byte", dtype=pa.int64()),
        ArrowFieldSpec(name="end_byte", dtype=pa.int64()),
        ArrowFieldSpec(name="is_error", dtype=pa.bool_()),
    ],
)

TS_MISSING_SPEC = register_dataset(
    name="ts_missing_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=[
        ArrowFieldSpec(name="ts_missing_id", dtype=pa.string()),
        ArrowFieldSpec(name="ts_node_id", dtype=pa.string()),
        ArrowFieldSpec(name="ts_type", dtype=pa.string()),
        ArrowFieldSpec(name="start_byte", dtype=pa.int64()),
        ArrowFieldSpec(name="end_byte", dtype=pa.int64()),
        ArrowFieldSpec(name="is_missing", dtype=pa.bool_()),
    ],
)

TS_NODES_SCHEMA = TS_NODES_SPEC.table_spec.to_arrow_schema()
TS_ERRORS_SCHEMA = TS_ERRORS_SPEC.table_spec.to_arrow_schema()
TS_MISSING_SCHEMA = TS_MISSING_SPEC.table_spec.to_arrow_schema()

TS_NODE_ROWS_SCHEMA = pa.schema(
    [
        *TS_NODES_SCHEMA,
        pa.field("parent_ts_type", pa.string()),
        pa.field("parent_start_byte", pa.int64()),
        pa.field("parent_end_byte", pa.int64()),
    ]
)

TS_NODES_QUERY = query_for_schema(TS_NODES_SCHEMA)
TS_ERRORS_QUERY = query_for_schema(TS_ERRORS_SCHEMA)
TS_MISSING_QUERY = query_for_schema(TS_MISSING_SCHEMA)


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
    ctx:
        Execution context for plan execution.

    Returns
    -------
    TreeSitterExtractResult
        Extracted node and diagnostic tables.
    """
    options = options or TreeSitterExtractOptions()
    exec_ctx = ctx or ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))
    plans = extract_ts_plans(
        repo_files,
        options=options,
        file_contexts=file_contexts,
        ctx=exec_ctx,
    )
    return TreeSitterExtractResult(
        ts_nodes=materialize_plan(plans["ts_nodes"], ctx=exec_ctx),
        ts_errors=materialize_plan(plans["ts_errors"], ctx=exec_ctx),
        ts_missing=materialize_plan(plans["ts_missing"], ctx=exec_ctx),
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
    exec_ctx = ctx or ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))
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

    nodes_plan = plan_from_rows(node_rows, schema=TS_NODE_ROWS_SCHEMA, label="ts_nodes")
    nodes_plan = apply_hash_projection(
        nodes_plan,
        specs=(
            HashSpec(
                prefix="ts_node",
                cols=("path", "start_byte", "end_byte", "ts_type"),
                out_col="ts_node_id",
            ),
            HashSpec(
                prefix="ts_node",
                cols=("path", "parent_start_byte", "parent_end_byte", "parent_ts_type"),
                out_col="parent_ts_id",
            ),
        ),
        available=TS_NODE_ROWS_SCHEMA.names,
        required={
            "ts_node_id": ("path", "start_byte", "end_byte", "ts_type"),
            "parent_ts_id": ("path", "parent_start_byte", "parent_end_byte", "parent_ts_type"),
        },
        ctx=exec_ctx,
    )
    nodes_plan = apply_query_spec(nodes_plan, spec=TS_NODES_QUERY, ctx=exec_ctx)
    nodes_plan = align_plan(
        nodes_plan,
        schema=TS_NODES_SCHEMA,
        available=TS_NODES_SCHEMA.names,
        ctx=exec_ctx,
    )
    errors_plan = plan_from_rows(error_rows, schema=TS_ERRORS_SCHEMA, label="ts_errors")
    errors_plan = apply_hash_projection(
        errors_plan,
        specs=(
            HashSpec(
                prefix="ts_node",
                cols=("path", "start_byte", "end_byte", "ts_type"),
                out_col="ts_node_id",
            ),
            HashSpec(
                prefix="ts_error",
                cols=("path", "start_byte", "end_byte"),
                out_col="ts_error_id",
            ),
        ),
        available=TS_ERRORS_SCHEMA.names,
        required={
            "ts_node_id": ("path", "start_byte", "end_byte", "ts_type"),
            "ts_error_id": ("path", "start_byte", "end_byte"),
        },
        ctx=exec_ctx,
    )
    errors_plan = apply_query_spec(errors_plan, spec=TS_ERRORS_QUERY, ctx=exec_ctx)
    errors_plan = align_plan(
        errors_plan,
        schema=TS_ERRORS_SCHEMA,
        available=TS_ERRORS_SCHEMA.names,
        ctx=exec_ctx,
    )
    missing_plan = plan_from_rows(missing_rows, schema=TS_MISSING_SCHEMA, label="ts_missing")
    missing_plan = apply_hash_projection(
        missing_plan,
        specs=(
            HashSpec(
                prefix="ts_node",
                cols=("path", "start_byte", "end_byte", "ts_type"),
                out_col="ts_node_id",
            ),
            HashSpec(
                prefix="ts_missing",
                cols=("path", "start_byte", "end_byte"),
                out_col="ts_missing_id",
            ),
        ),
        available=TS_MISSING_SCHEMA.names,
        required={
            "ts_node_id": ("path", "start_byte", "end_byte", "ts_type"),
            "ts_missing_id": ("path", "start_byte", "end_byte"),
        },
        ctx=exec_ctx,
    )
    missing_plan = apply_query_spec(missing_plan, spec=TS_MISSING_QUERY, ctx=exec_ctx)
    missing_plan = align_plan(
        missing_plan,
        schema=TS_MISSING_SCHEMA,
        available=TS_MISSING_SCHEMA.names,
        ctx=exec_ctx,
    )
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
    for node, parent in _iter_nodes(root):
        row = _node_row(
            node,
            file_ctx=file_ctx,
            parent=parent,
        )
        if options.include_nodes:
            buffers.node_rows.append(row)
        if options.include_errors and bool(row.get("is_error")):
            buffers.error_rows.append(_error_row(row))
        if options.include_missing and bool(row.get("is_missing")):
            buffers.missing_rows.append(_missing_row(row))


def extract_ts_tables(
    *,
    repo_root: str | None,
    repo_files: TableLike,
    file_contexts: Iterable[FileContext] | None = None,
    ctx: ExecutionContext | None = None,
    prefer_reader: bool = False,
) -> dict[str, TableLike | RecordBatchReaderLike]:
    """Extract tree-sitter tables as a name-keyed bundle.

    Parameters
    ----------
    repo_root:
        Optional repository root (unused).
    repo_files:
        Repo files table.
    file_contexts:
        Optional pre-built file contexts for extraction.
    ctx:
        Execution context for plan execution.

    prefer_reader:
        When True, return streaming readers when possible.

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Extracted tree-sitter outputs keyed by output name.
    """
    _ = repo_root
    exec_ctx = ctx or ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))
    plans = extract_ts_plans(
        repo_files,
        options=TreeSitterExtractOptions(),
        file_contexts=file_contexts,
        ctx=exec_ctx,
    )
    return finalize_plan_bundle(plans, ctx=exec_ctx, prefer_reader=prefer_reader)
