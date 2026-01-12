"""Extract tree-sitter nodes and diagnostics into Arrow tables using shared helpers."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass

import pyarrow as pa
import tree_sitter_python
from tree_sitter import Language, Parser

from arrowdsl.core.ids import HashSpec
from arrowdsl.core.interop import TableLike, pc
from arrowdsl.schema.arrays import set_or_append_column
from extract.common import bytes_from_file_ctx, file_identity_row, iter_contexts
from extract.file_context import FileContext
from extract.hashing import apply_hash_column
from extract.spec_helpers import register_dataset
from extract.tables import rows_to_table
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
    parent_indices: list[int | None]


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
) -> Row:
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", 0))
    ts_type = str(getattr(node, "type", ""))
    return {
        **file_identity_row(file_ctx),
        "ts_type": ts_type,
        "start_byte": start,
        "end_byte": end,
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

    Returns
    -------
    TreeSitterExtractResult
        Extracted node and diagnostic tables.
    """
    options = options or TreeSitterExtractOptions()
    parser = _parser()

    node_rows: list[Row] = []
    error_rows: list[Row] = []
    missing_rows: list[Row] = []
    buffers = TreeSitterRowBuffers(
        node_rows=node_rows,
        error_rows=error_rows,
        missing_rows=missing_rows,
        parent_indices=[],
    )

    for file_ctx in iter_contexts(repo_files, file_contexts):
        _extract_ts_for_row(
            file_ctx,
            parser=parser,
            options=options,
            buffers=buffers,
        )

    ts_nodes = rows_to_table(node_rows, TS_NODES_SCHEMA)
    if ts_nodes.num_rows > 0:
        ts_nodes = apply_hash_column(
            ts_nodes,
            spec=HashSpec(
                prefix="ts_node",
                cols=("path", "start_byte", "end_byte", "ts_type"),
                out_col="ts_node_id",
            ),
        )
        parent_indices = pa.array(buffers.parent_indices, type=pa.int64())
        parent_ids = pc.take(ts_nodes["ts_node_id"], parent_indices)
        ts_nodes = set_or_append_column(ts_nodes, "parent_ts_id", parent_ids)

    ts_errors = rows_to_table(error_rows, TS_ERRORS_SCHEMA)
    if ts_errors.num_rows > 0:
        ts_errors = apply_hash_column(
            ts_errors,
            spec=HashSpec(
                prefix="ts_node",
                cols=("path", "start_byte", "end_byte", "ts_type"),
                out_col="ts_node_id",
            ),
        )
        ts_errors = apply_hash_column(
            ts_errors,
            spec=HashSpec(
                prefix="ts_error",
                cols=("path", "start_byte", "end_byte"),
                out_col="ts_error_id",
            ),
        )

    ts_missing = rows_to_table(missing_rows, TS_MISSING_SCHEMA)
    if ts_missing.num_rows > 0:
        ts_missing = apply_hash_column(
            ts_missing,
            spec=HashSpec(
                prefix="ts_node",
                cols=("path", "start_byte", "end_byte", "ts_type"),
                out_col="ts_node_id",
            ),
        )
        ts_missing = apply_hash_column(
            ts_missing,
            spec=HashSpec(
                prefix="ts_missing",
                cols=("path", "start_byte", "end_byte"),
                out_col="ts_missing_id",
            ),
        )

    return TreeSitterExtractResult(
        ts_nodes=ts_nodes,
        ts_errors=ts_errors,
        ts_missing=ts_missing,
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
    row_by_node_index: dict[object, int] = {}
    for node, parent in _iter_nodes(root):
        parent_index = row_by_node_index.get(parent) if parent is not None else None
        row = _node_row(
            node,
            file_ctx=file_ctx,
        )
        if options.include_nodes:
            index = len(buffers.node_rows)
            buffers.node_rows.append(row)
            buffers.parent_indices.append(parent_index)
            row_by_node_index[node] = index
        if options.include_errors and bool(row.get("is_error")):
            buffers.error_rows.append(_error_row(row))
        if options.include_missing and bool(row.get("is_missing")):
            buffers.missing_rows.append(_missing_row(row))


def extract_ts_tables(
    *,
    repo_root: str | None,
    repo_files: TableLike,
    file_contexts: Iterable[FileContext] | None = None,
    ctx: object | None = None,
) -> dict[str, TableLike]:
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
        Execution context (unused).

    Returns
    -------
    dict[str, pyarrow.Table]
        Extracted tree-sitter tables keyed by output name.
    """
    _ = repo_root
    _ = ctx
    result = extract_ts(
        repo_files,
        options=TreeSitterExtractOptions(),
        file_contexts=file_contexts,
    )
    return {
        "ts_nodes": result.ts_nodes,
        "ts_errors": result.ts_errors,
        "ts_missing": result.ts_missing,
    }
