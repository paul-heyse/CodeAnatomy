"""Extract Python AST facts into Arrow tables using shared helpers."""

from __future__ import annotations

import ast
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Literal, Required, TypedDict, Unpack, overload

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext, OrderingLevel, RuntimeProfile
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import QuerySpec
from extract.common import file_identity_row, iter_contexts, text_from_file_ctx
from extract.derived_views import ast_def_nodes_plan
from extract.file_context import FileContext
from extract.spec_helpers import (
    DatasetRegistration,
    infer_ordering_keys,
    merge_metadata_specs,
    options_metadata_spec,
    ordering_metadata_spec,
    register_dataset,
)
from extract.tables import (
    align_plan,
    finalize_plan_bundle,
    materialize_plan,
    plan_from_rows,
)
from schema_spec.specs import ArrowFieldSpec, file_identity_bundle

SCHEMA_VERSION = 1


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


_AST_NODES_FIELDS = [
    ArrowFieldSpec(name="ast_idx", dtype=pa.int32()),
    ArrowFieldSpec(name="parent_ast_idx", dtype=pa.int32()),
    ArrowFieldSpec(name="field_name", dtype=pa.string()),
    ArrowFieldSpec(name="field_pos", dtype=pa.int32()),
    ArrowFieldSpec(name="kind", dtype=pa.string()),
    ArrowFieldSpec(name="name", dtype=pa.string()),
    ArrowFieldSpec(name="value_repr", dtype=pa.string()),
    ArrowFieldSpec(name="lineno", dtype=pa.int32()),
    ArrowFieldSpec(name="col_offset", dtype=pa.int32()),
    ArrowFieldSpec(name="end_lineno", dtype=pa.int32()),
    ArrowFieldSpec(name="end_col_offset", dtype=pa.int32()),
]

_AST_EDGES_FIELDS = [
    ArrowFieldSpec(name="parent_ast_idx", dtype=pa.int32()),
    ArrowFieldSpec(name="child_ast_idx", dtype=pa.int32()),
    ArrowFieldSpec(name="field_name", dtype=pa.string()),
    ArrowFieldSpec(name="field_pos", dtype=pa.int32()),
]

_AST_ERRORS_FIELDS = [
    ArrowFieldSpec(name="error_type", dtype=pa.string()),
    ArrowFieldSpec(name="message", dtype=pa.string()),
    ArrowFieldSpec(name="lineno", dtype=pa.int32()),
    ArrowFieldSpec(name="offset", dtype=pa.int32()),
    ArrowFieldSpec(name="end_lineno", dtype=pa.int32()),
    ArrowFieldSpec(name="end_offset", dtype=pa.int32()),
]

_AST_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_AST_NODES_FIELDS)
)
_AST_EDGES_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_AST_EDGES_FIELDS)
)
_AST_ERRORS_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_AST_ERRORS_FIELDS)
)

AST_NODES_QUERY = QuerySpec.simple(*_AST_BASE_COLUMNS)
AST_EDGES_QUERY = QuerySpec.simple(*_AST_EDGES_BASE_COLUMNS)
AST_ERRORS_QUERY = QuerySpec.simple(*_AST_ERRORS_BASE_COLUMNS)

_AST_METADATA_EXTRA = {
    b"extractor_name": b"ast",
    b"extractor_version": str(SCHEMA_VERSION).encode("utf-8"),
}

_AST_NODES_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_AST_BASE_COLUMNS),
    extra=_AST_METADATA_EXTRA,
)
_AST_EDGES_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_AST_EDGES_BASE_COLUMNS),
    extra=_AST_METADATA_EXTRA,
)
_AST_ERRORS_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_AST_ERRORS_BASE_COLUMNS),
    extra=_AST_METADATA_EXTRA,
)

AST_NODES_SPEC = register_dataset(
    name="py_ast_nodes_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_AST_NODES_FIELDS,
    registration=DatasetRegistration(
        query_spec=AST_NODES_QUERY,
        metadata_spec=_AST_NODES_METADATA,
    ),
)

AST_EDGES_SPEC = register_dataset(
    name="py_ast_edges_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_AST_EDGES_FIELDS,
    registration=DatasetRegistration(
        query_spec=AST_EDGES_QUERY,
        metadata_spec=_AST_EDGES_METADATA,
    ),
)

AST_ERRORS_SPEC = register_dataset(
    name="py_ast_errors_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_AST_ERRORS_FIELDS,
    registration=DatasetRegistration(
        query_spec=AST_ERRORS_QUERY,
        metadata_spec=_AST_ERRORS_METADATA,
    ),
)

AST_NODES_SCHEMA = AST_NODES_SPEC.schema()
AST_EDGES_SCHEMA = AST_EDGES_SPEC.schema()
AST_ERRORS_SCHEMA = AST_ERRORS_SPEC.schema()


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
    file_contexts: Iterable[FileContext] | None = None,
    ctx: ExecutionContext | None = None,
) -> ASTExtractResult:
    """Extract a minimal AST fact set per file.

    Returns
    -------
    ASTExtractResult
        Tables of AST nodes, edges, and errors.
    """
    options = options or ASTExtractOptions()
    ctx = ctx or ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))
    plans = extract_ast_plans(
        repo_files,
        options=options,
        file_contexts=file_contexts,
        ctx=ctx,
    )
    run_meta = options_metadata_spec(options=options)
    return ASTExtractResult(
        py_ast_nodes=materialize_plan(
            plans["ast_nodes"],
            ctx=ctx,
            metadata_spec=merge_metadata_specs(_AST_NODES_METADATA, run_meta),
        ),
        py_ast_edges=materialize_plan(
            plans["ast_edges"],
            ctx=ctx,
            metadata_spec=merge_metadata_specs(_AST_EDGES_METADATA, run_meta),
        ),
        py_ast_errors=materialize_plan(
            plans["ast_errors"],
            ctx=ctx,
            metadata_spec=merge_metadata_specs(_AST_ERRORS_METADATA, run_meta),
        ),
    )


def extract_ast_plans(
    repo_files: TableLike,
    options: ASTExtractOptions | None = None,
    *,
    file_contexts: Iterable[FileContext] | None = None,
    ctx: ExecutionContext | None = None,
) -> dict[str, Plan]:
    """Extract AST plans for nodes, edges, and errors.

    Returns
    -------
    dict[str, Plan]
        Plan bundle keyed by ``ast_nodes``, ``ast_edges``, and ``ast_errors``.
    """
    options = options or ASTExtractOptions()
    ctx = ctx or ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))

    nodes_rows: list[dict[str, object]] = []
    edges_rows: list[dict[str, object]] = []
    err_rows: list[dict[str, object]] = []

    for file_ctx in iter_contexts(repo_files, file_contexts):
        nodes, edges, errs = _extract_ast_for_context(file_ctx, options=options)
        nodes_rows.extend(nodes)
        edges_rows.extend(edges)
        err_rows.extend(errs)

    nodes_plan = plan_from_rows(nodes_rows, schema=AST_NODES_SCHEMA, label="ast_nodes")
    nodes_plan = AST_NODES_QUERY.apply_to_plan(nodes_plan, ctx=ctx)
    nodes_plan = align_plan(
        nodes_plan,
        schema=AST_NODES_SCHEMA,
        available=AST_NODES_SCHEMA.names,
        ctx=ctx,
    )

    edges_plan = plan_from_rows(edges_rows, schema=AST_EDGES_SCHEMA, label="ast_edges")
    edges_plan = AST_EDGES_QUERY.apply_to_plan(edges_plan, ctx=ctx)
    edges_plan = align_plan(
        edges_plan,
        schema=AST_EDGES_SCHEMA,
        available=AST_EDGES_SCHEMA.names,
        ctx=ctx,
    )

    errs_plan = plan_from_rows(err_rows, schema=AST_ERRORS_SCHEMA, label="ast_errors")
    errs_plan = AST_ERRORS_QUERY.apply_to_plan(errs_plan, ctx=ctx)
    errs_plan = align_plan(
        errs_plan,
        schema=AST_ERRORS_SCHEMA,
        available=AST_ERRORS_SCHEMA.names,
        ctx=ctx,
    )

    return {
        "ast_nodes": nodes_plan,
        "ast_edges": edges_plan,
        "ast_errors": errs_plan,
    }


class _AstTablesKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: ASTExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    ctx: ExecutionContext | None
    prefer_reader: bool


class _AstTablesKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: ASTExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    ctx: ExecutionContext | None
    prefer_reader: Literal[False]


class _AstTablesKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: ASTExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    ctx: ExecutionContext | None
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
        Keyword-only arguments for extraction (repo_files, options, file_contexts, ctx,
        prefer_reader).

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Extracted AST outputs keyed by name.
    """
    repo_files = kwargs["repo_files"]
    options = kwargs.get("options") or ASTExtractOptions()
    file_contexts = kwargs.get("file_contexts")
    ctx = kwargs.get("ctx") or ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))
    prefer_reader = kwargs.get("prefer_reader", False)
    plans = extract_ast_plans(
        repo_files,
        options=options,
        file_contexts=file_contexts,
        ctx=ctx,
    )
    defs_plan = ast_def_nodes_plan(plan=plans["ast_nodes"])
    run_meta = options_metadata_spec(options=options)

    return finalize_plan_bundle(
        {
            "ast_nodes": plans["ast_nodes"],
            "ast_edges": plans["ast_edges"],
            "ast_defs": defs_plan,
        },
        ctx=ctx,
        prefer_reader=prefer_reader,
        metadata_specs={
            "ast_nodes": merge_metadata_specs(_AST_NODES_METADATA, run_meta),
            "ast_edges": merge_metadata_specs(_AST_EDGES_METADATA, run_meta),
            "ast_defs": merge_metadata_specs(_AST_NODES_METADATA, run_meta),
        },
    )
