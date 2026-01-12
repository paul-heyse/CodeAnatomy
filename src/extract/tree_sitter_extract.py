"""Extract tree-sitter nodes and diagnostics into Arrow tables."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass

import tree_sitter_python
from tree_sitter import Language, Parser

import arrowdsl.pyarrow_core as pa
from arrowdsl.column_ops import set_or_append_column
from arrowdsl.compute import pc
from arrowdsl.empty import empty_table
from arrowdsl.id_specs import HashSpec
from arrowdsl.ids import hash_column_values
from arrowdsl.pyarrow_protocols import TableLike
from extract.file_context import FileContext, iter_file_contexts
from schema_spec.core import ArrowFieldSpec
from schema_spec.factories import make_table_spec
from schema_spec.fields import file_identity_bundle
from schema_spec.registry import GLOBAL_SCHEMA_REGISTRY

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


TS_NODES_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
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
)

TS_ERRORS_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
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
)

TS_MISSING_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
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
)

TS_NODES_SCHEMA = TS_NODES_SPEC.to_arrow_schema()
TS_ERRORS_SCHEMA = TS_ERRORS_SPEC.to_arrow_schema()
TS_MISSING_SCHEMA = TS_MISSING_SPEC.to_arrow_schema()


def _parser() -> Parser:
    language = Language(tree_sitter_python.language())
    return Parser(language)


def _row_bytes(file_ctx: FileContext) -> bytes | None:
    if file_ctx.data is not None:
        return file_ctx.data
    if file_ctx.text is None:
        return None
    encoding = file_ctx.encoding or "utf-8"
    return file_ctx.text.encode(encoding, errors="replace")


def _iter_nodes(root: object) -> Iterator[tuple[object, object | None]]:
    stack: list[tuple[object, object | None]] = [(root, None)]
    while stack:
        node, parent = stack.pop()
        yield node, parent
        children = getattr(node, "children", [])
        stack.extend((child, node) for child in reversed(children))


def _apply_hash_column(table: TableLike, *, spec: HashSpec) -> TableLike:
    hashed = hash_column_values(table, spec=spec)
    out_col = spec.out_col or f"{spec.prefix}_id"
    return set_or_append_column(table, out_col, hashed)


def _node_row(
    node: object,
    *,
    file_id: str,
    path: str,
    file_sha256: str | None,
) -> Row:
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", 0))
    ts_type = str(getattr(node, "type", ""))
    return {
        "file_id": file_id,
        "path": path,
        "file_sha256": file_sha256,
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

    contexts = file_contexts if file_contexts is not None else iter_file_contexts(repo_files)
    for file_ctx in contexts:
        _extract_ts_for_row(
            file_ctx,
            parser=parser,
            options=options,
            buffers=buffers,
        )

    ts_nodes = (
        pa.Table.from_pylist(node_rows, schema=TS_NODES_SCHEMA)
        if node_rows
        else empty_table(TS_NODES_SCHEMA)
    )
    if ts_nodes.num_rows > 0:
        ts_nodes = _apply_hash_column(
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

    ts_errors = (
        pa.Table.from_pylist(error_rows, schema=TS_ERRORS_SCHEMA)
        if error_rows
        else empty_table(TS_ERRORS_SCHEMA)
    )
    if ts_errors.num_rows > 0:
        ts_errors = _apply_hash_column(
            ts_errors,
            spec=HashSpec(
                prefix="ts_node",
                cols=("path", "start_byte", "end_byte", "ts_type"),
                out_col="ts_node_id",
            ),
        )
        ts_errors = _apply_hash_column(
            ts_errors,
            spec=HashSpec(
                prefix="ts_error",
                cols=("path", "start_byte", "end_byte"),
                out_col="ts_error_id",
            ),
        )

    ts_missing = (
        pa.Table.from_pylist(missing_rows, schema=TS_MISSING_SCHEMA)
        if missing_rows
        else empty_table(TS_MISSING_SCHEMA)
    )
    if ts_missing.num_rows > 0:
        ts_missing = _apply_hash_column(
            ts_missing,
            spec=HashSpec(
                prefix="ts_node",
                cols=("path", "start_byte", "end_byte", "ts_type"),
                out_col="ts_node_id",
            ),
        )
        ts_missing = _apply_hash_column(
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
    data = _row_bytes(file_ctx)
    if data is None:
        return
    tree = parser.parse(data)
    root = tree.root_node
    row_by_node_index: dict[object, int] = {}
    file_sha = file_ctx.file_sha256
    for node, parent in _iter_nodes(root):
        parent_index = row_by_node_index.get(parent) if parent is not None else None
        row = _node_row(
            node,
            file_id=file_ctx.file_id,
            path=file_ctx.path,
            file_sha256=file_sha,
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
