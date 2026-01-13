"""Extract tree-sitter nodes and diagnostics into Arrow tables using shared helpers."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Literal, Required, TypedDict, Unpack, overload

import pyarrow as pa
import tree_sitter_python
from tree_sitter import Language, Parser

from arrowdsl.compute.expr_core import MaskedHashExprSpec
from arrowdsl.core.context import ExecutionContext, OrderingLevel, execution_context_factory
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import ProjectionSpec, QuerySpec
from arrowdsl.plan.runner import materialize_plan, run_plan_bundle
from arrowdsl.plan.scan_io import plan_from_rows
from arrowdsl.schema.schema import SchemaMetadataSpec
from extract.hash_specs import (
    TS_ERROR_ID_SPEC,
    TS_MISSING_ID_SPEC,
    TS_NODE_ID_SPEC,
    TS_PARENT_NODE_ID_SPEC,
)
from extract.helpers import (
    DatasetRegistration,
    FileContext,
    align_plan,
    bytes_from_file_ctx,
    file_identity_row,
    infer_ordering_keys,
    iter_contexts,
    merge_metadata_specs,
    options_metadata_spec,
    ordering_metadata_spec,
    register_dataset,
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


_TS_NODES_FIELDS = [
    ArrowFieldSpec(name="ts_node_id", dtype=pa.string()),
    ArrowFieldSpec(name="parent_ts_id", dtype=pa.string()),
    ArrowFieldSpec(name="ts_type", dtype=pa.string()),
    ArrowFieldSpec(name="start_byte", dtype=pa.int64()),
    ArrowFieldSpec(name="end_byte", dtype=pa.int64()),
    ArrowFieldSpec(name="is_named", dtype=pa.bool_()),
    ArrowFieldSpec(name="has_error", dtype=pa.bool_()),
    ArrowFieldSpec(name="is_error", dtype=pa.bool_()),
    ArrowFieldSpec(name="is_missing", dtype=pa.bool_()),
]

_TS_ERRORS_FIELDS = [
    ArrowFieldSpec(name="ts_error_id", dtype=pa.string()),
    ArrowFieldSpec(name="ts_node_id", dtype=pa.string()),
    ArrowFieldSpec(name="ts_type", dtype=pa.string()),
    ArrowFieldSpec(name="start_byte", dtype=pa.int64()),
    ArrowFieldSpec(name="end_byte", dtype=pa.int64()),
    ArrowFieldSpec(name="is_error", dtype=pa.bool_()),
]

_TS_MISSING_FIELDS = [
    ArrowFieldSpec(name="ts_missing_id", dtype=pa.string()),
    ArrowFieldSpec(name="ts_node_id", dtype=pa.string()),
    ArrowFieldSpec(name="ts_type", dtype=pa.string()),
    ArrowFieldSpec(name="start_byte", dtype=pa.int64()),
    ArrowFieldSpec(name="end_byte", dtype=pa.int64()),
    ArrowFieldSpec(name="is_missing", dtype=pa.bool_()),
]

_TS_NODES_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_TS_NODES_FIELDS)
)
_TS_ERRORS_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_TS_ERRORS_FIELDS)
)
_TS_MISSING_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_TS_MISSING_FIELDS)
)

_TS_METADATA_EXTRA = {
    b"extractor_name": b"tree_sitter",
    b"extractor_version": str(SCHEMA_VERSION).encode("utf-8"),
}

_TS_NODES_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_TS_NODES_BASE_COLUMNS),
    extra=_TS_METADATA_EXTRA,
)
_TS_ERRORS_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_TS_ERRORS_BASE_COLUMNS),
    extra=_TS_METADATA_EXTRA,
)
_TS_MISSING_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_TS_MISSING_BASE_COLUMNS),
    extra=_TS_METADATA_EXTRA,
)

TS_NODES_SPEC = register_dataset(
    name="ts_nodes_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_TS_NODES_FIELDS,
    registration=DatasetRegistration(
        query_spec=QuerySpec(
            projection=ProjectionSpec(
                base=tuple(
                    field.name
                    for field in (*file_identity_bundle().fields, *_TS_NODES_FIELDS)
                    if field.name not in {"ts_node_id", "parent_ts_id"}
                ),
                derived={
                    "ts_node_id": MaskedHashExprSpec(
                        spec=TS_NODE_ID_SPEC,
                        required=("path", "start_byte", "end_byte", "ts_type"),
                    ),
                    "parent_ts_id": MaskedHashExprSpec(
                        spec=TS_PARENT_NODE_ID_SPEC,
                        required=("path", "parent_start_byte", "parent_end_byte", "parent_ts_type"),
                    ),
                },
            )
        ),
        metadata_spec=_TS_NODES_METADATA,
    ),
)

TS_ERRORS_SPEC = register_dataset(
    name="ts_errors_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_TS_ERRORS_FIELDS,
    registration=DatasetRegistration(
        query_spec=QuerySpec(
            projection=ProjectionSpec(
                base=tuple(
                    field.name
                    for field in (*file_identity_bundle().fields, *_TS_ERRORS_FIELDS)
                    if field.name not in {"ts_error_id", "ts_node_id"}
                ),
                derived={
                    "ts_node_id": MaskedHashExprSpec(
                        spec=TS_NODE_ID_SPEC,
                        required=("path", "start_byte", "end_byte", "ts_type"),
                    ),
                    "ts_error_id": MaskedHashExprSpec(
                        spec=TS_ERROR_ID_SPEC,
                        required=("path", "start_byte", "end_byte"),
                    ),
                },
            )
        ),
        metadata_spec=_TS_ERRORS_METADATA,
    ),
)

TS_MISSING_SPEC = register_dataset(
    name="ts_missing_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_TS_MISSING_FIELDS,
    registration=DatasetRegistration(
        query_spec=QuerySpec(
            projection=ProjectionSpec(
                base=tuple(
                    field.name
                    for field in (*file_identity_bundle().fields, *_TS_MISSING_FIELDS)
                    if field.name not in {"ts_missing_id", "ts_node_id"}
                ),
                derived={
                    "ts_node_id": MaskedHashExprSpec(
                        spec=TS_NODE_ID_SPEC,
                        required=("path", "start_byte", "end_byte", "ts_type"),
                    ),
                    "ts_missing_id": MaskedHashExprSpec(
                        spec=TS_MISSING_ID_SPEC,
                        required=("path", "start_byte", "end_byte"),
                    ),
                },
            )
        ),
        metadata_spec=_TS_MISSING_METADATA,
    ),
)

TS_NODES_SCHEMA = TS_NODES_SPEC.schema()
TS_ERRORS_SCHEMA = TS_ERRORS_SPEC.schema()
TS_MISSING_SCHEMA = TS_MISSING_SPEC.schema()


def _ts_metadata_specs(
    options: TreeSitterExtractOptions,
) -> dict[str, SchemaMetadataSpec]:
    run_meta = options_metadata_spec(options=options)
    return {
        "ts_nodes": merge_metadata_specs(_TS_NODES_METADATA, run_meta),
        "ts_errors": merge_metadata_specs(_TS_ERRORS_METADATA, run_meta),
        "ts_missing": merge_metadata_specs(_TS_MISSING_METADATA, run_meta),
    }


TS_NODE_ROWS_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.string()),
        pa.field("path", pa.string()),
        pa.field("file_sha256", pa.string()),
        pa.field("ts_type", pa.string()),
        pa.field("start_byte", pa.int64()),
        pa.field("end_byte", pa.int64()),
        pa.field("is_named", pa.bool_()),
        pa.field("has_error", pa.bool_()),
        pa.field("is_error", pa.bool_()),
        pa.field("is_missing", pa.bool_()),
        pa.field("parent_ts_type", pa.string()),
        pa.field("parent_start_byte", pa.int64()),
        pa.field("parent_end_byte", pa.int64()),
    ]
)

TS_ERRORS_ROWS_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.string()),
        pa.field("path", pa.string()),
        pa.field("file_sha256", pa.string()),
        pa.field("ts_type", pa.string()),
        pa.field("start_byte", pa.int64()),
        pa.field("end_byte", pa.int64()),
        pa.field("is_error", pa.bool_()),
    ]
)

TS_MISSING_ROWS_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.string()),
        pa.field("path", pa.string()),
        pa.field("file_sha256", pa.string()),
        pa.field("ts_type", pa.string()),
        pa.field("start_byte", pa.int64()),
        pa.field("end_byte", pa.int64()),
        pa.field("is_missing", pa.bool_()),
    ]
)

TS_NODES_QUERY = TS_NODES_SPEC.query()
TS_ERRORS_QUERY = TS_ERRORS_SPEC.query()
TS_MISSING_QUERY = TS_MISSING_SPEC.query()


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

    nodes_plan = plan_from_rows(node_rows, schema=TS_NODE_ROWS_SCHEMA, label="ts_nodes")
    nodes_plan = TS_NODES_QUERY.apply_to_plan(nodes_plan, ctx=exec_ctx)
    nodes_plan = align_plan(nodes_plan, schema=TS_NODES_SCHEMA, ctx=exec_ctx)

    errors_plan = plan_from_rows(error_rows, schema=TS_ERRORS_ROWS_SCHEMA, label="ts_errors")
    errors_plan = TS_ERRORS_QUERY.apply_to_plan(errors_plan, ctx=exec_ctx)
    errors_plan = align_plan(errors_plan, schema=TS_ERRORS_SCHEMA, ctx=exec_ctx)

    missing_plan = plan_from_rows(missing_rows, schema=TS_MISSING_ROWS_SCHEMA, label="ts_missing")
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
