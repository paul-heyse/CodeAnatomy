"""Extract tree-sitter nodes and diagnostics into Arrow tables."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import tree_sitter_python
from tree_sitter import Language, Parser

import arrowdsl.pyarrow_core as pa
from arrowdsl.empty import empty_table
from arrowdsl.ids import prefixed_hash_id_from_parts
from arrowdsl.iter import iter_table_rows
from arrowdsl.pyarrow_protocols import TableLike
from schema_spec.core import ArrowFieldSpec, TableSchemaSpec

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


TS_NODES_SPEC = TableSchemaSpec(
    name="ts_nodes_v1",
    fields=[
        ArrowFieldSpec(name="schema_version", dtype=pa.int32(), nullable=False),
        ArrowFieldSpec(name="ts_node_id", dtype=pa.string()),
        ArrowFieldSpec(name="parent_ts_id", dtype=pa.string()),
        ArrowFieldSpec(name="file_id", dtype=pa.string()),
        ArrowFieldSpec(name="path", dtype=pa.string()),
        ArrowFieldSpec(name="file_sha256", dtype=pa.string()),
        ArrowFieldSpec(name="ts_type", dtype=pa.string()),
        ArrowFieldSpec(name="start_byte", dtype=pa.int64()),
        ArrowFieldSpec(name="end_byte", dtype=pa.int64()),
        ArrowFieldSpec(name="is_named", dtype=pa.bool_()),
        ArrowFieldSpec(name="has_error", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_error", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_missing", dtype=pa.bool_()),
    ],
)

TS_ERRORS_SPEC = TableSchemaSpec(
    name="ts_errors_v1",
    fields=[
        ArrowFieldSpec(name="schema_version", dtype=pa.int32(), nullable=False),
        ArrowFieldSpec(name="ts_error_id", dtype=pa.string()),
        ArrowFieldSpec(name="ts_node_id", dtype=pa.string()),
        ArrowFieldSpec(name="file_id", dtype=pa.string()),
        ArrowFieldSpec(name="path", dtype=pa.string()),
        ArrowFieldSpec(name="file_sha256", dtype=pa.string()),
        ArrowFieldSpec(name="ts_type", dtype=pa.string()),
        ArrowFieldSpec(name="start_byte", dtype=pa.int64()),
        ArrowFieldSpec(name="end_byte", dtype=pa.int64()),
        ArrowFieldSpec(name="is_error", dtype=pa.bool_()),
    ],
)

TS_MISSING_SPEC = TableSchemaSpec(
    name="ts_missing_v1",
    fields=[
        ArrowFieldSpec(name="schema_version", dtype=pa.int32(), nullable=False),
        ArrowFieldSpec(name="ts_missing_id", dtype=pa.string()),
        ArrowFieldSpec(name="ts_node_id", dtype=pa.string()),
        ArrowFieldSpec(name="file_id", dtype=pa.string()),
        ArrowFieldSpec(name="path", dtype=pa.string()),
        ArrowFieldSpec(name="file_sha256", dtype=pa.string()),
        ArrowFieldSpec(name="ts_type", dtype=pa.string()),
        ArrowFieldSpec(name="start_byte", dtype=pa.int64()),
        ArrowFieldSpec(name="end_byte", dtype=pa.int64()),
        ArrowFieldSpec(name="is_missing", dtype=pa.bool_()),
    ],
)

TS_NODES_SCHEMA = TS_NODES_SPEC.to_arrow_schema()
TS_ERRORS_SCHEMA = TS_ERRORS_SPEC.to_arrow_schema()
TS_MISSING_SCHEMA = TS_MISSING_SPEC.to_arrow_schema()


def _parser() -> Parser:
    language = Language(tree_sitter_python.language())
    return Parser(language)


def _row_bytes(rf: Row) -> bytes | None:
    data = rf.get("bytes")
    if isinstance(data, (bytes, bytearray, memoryview)):
        return bytes(data)
    text = rf.get("text")
    if not isinstance(text, str):
        return None
    encoding = rf.get("encoding")
    enc = encoding if isinstance(encoding, str) else "utf-8"
    return text.encode(enc, errors="replace")


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
    parent_id: str | None,
    file_id: str,
    path: str,
    file_sha256: str | None,
) -> Row:
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", 0))
    ts_type = str(getattr(node, "type", ""))
    ts_node_id = prefixed_hash_id_from_parts("ts_node", path, str(start), str(end), ts_type)
    return {
        "schema_version": SCHEMA_VERSION,
        "ts_node_id": ts_node_id,
        "parent_ts_id": parent_id,
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
        "schema_version": SCHEMA_VERSION,
        "ts_error_id": prefixed_hash_id_from_parts(
            "ts_error",
            str(node_row.get("path", "")),
            str(node_row.get("start_byte", "")),
            str(node_row.get("end_byte", "")),
        ),
        "ts_node_id": node_row.get("ts_node_id"),
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
        "schema_version": SCHEMA_VERSION,
        "ts_missing_id": prefixed_hash_id_from_parts(
            "ts_missing",
            str(node_row.get("path", "")),
            str(node_row.get("start_byte", "")),
            str(node_row.get("end_byte", "")),
        ),
        "ts_node_id": node_row.get("ts_node_id"),
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
) -> TreeSitterExtractResult:
    """Extract tree-sitter nodes and diagnostics from repo files.

    Parameters
    ----------
    repo_files:
        Repo files table with bytes/text.
    options:
        Extraction options.

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
    )

    for rf in iter_table_rows(repo_files):
        _extract_ts_for_row(
            rf,
            parser=parser,
            options=options,
            buffers=buffers,
        )

    ts_nodes = (
        pa.Table.from_pylist(node_rows, schema=TS_NODES_SCHEMA)
        if node_rows
        else empty_table(TS_NODES_SCHEMA)
    )
    ts_errors = (
        pa.Table.from_pylist(error_rows, schema=TS_ERRORS_SCHEMA)
        if error_rows
        else empty_table(TS_ERRORS_SCHEMA)
    )
    ts_missing = (
        pa.Table.from_pylist(missing_rows, schema=TS_MISSING_SCHEMA)
        if missing_rows
        else empty_table(TS_MISSING_SCHEMA)
    )

    return TreeSitterExtractResult(
        ts_nodes=ts_nodes,
        ts_errors=ts_errors,
        ts_missing=ts_missing,
    )


def _extract_ts_for_row(
    rf: Row,
    *,
    parser: Parser,
    options: TreeSitterExtractOptions,
    buffers: TreeSitterRowBuffers,
) -> None:
    file_id_val = rf.get("file_id")
    path_val = rf.get("path")
    if not isinstance(file_id_val, str) or not isinstance(path_val, str):
        return
    data = _row_bytes(rf)
    if data is None:
        return
    tree = parser.parse(data)
    root = tree.root_node
    row_by_node_id: dict[object, str] = {}
    file_sha256 = rf.get("file_sha256")
    file_sha = file_sha256 if isinstance(file_sha256, str) else None
    for node, parent in _iter_nodes(root):
        parent_id = row_by_node_id.get(parent) if parent is not None else None
        row = _node_row(
            node,
            parent_id=parent_id,
            file_id=file_id_val,
            path=path_val,
            file_sha256=file_sha,
        )
        row_by_node_id[node] = str(row["ts_node_id"])
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
    ctx: object | None = None,
) -> dict[str, TableLike]:
    """Extract tree-sitter tables as a name-keyed bundle.

    Parameters
    ----------
    repo_root:
        Optional repository root (unused).
    repo_files:
        Repo files table.
    ctx:
        Execution context (unused).

    Returns
    -------
    dict[str, pyarrow.Table]
        Extracted tree-sitter tables keyed by output name.
    """
    _ = repo_root
    _ = ctx
    result = extract_ts(repo_files, options=TreeSitterExtractOptions())
    return {
        "ts_nodes": result.ts_nodes,
        "ts_errors": result.ts_errors,
        "ts_missing": result.ts_missing,
    }
