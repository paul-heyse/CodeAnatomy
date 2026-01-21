"""Extract Python AST facts into Arrow tables using shared helpers."""

from __future__ import annotations

import ast
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, cast, overload

from arrowdsl.core.execution_context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.extract_registry import normalize_options
from extract.helpers import (
    ExtractExecutionContext,
    ExtractMaterializeOptions,
    FileContext,
    SpanSpec,
    apply_query_and_project,
    attrs_map,
    ibis_plan_from_row_batches,
    ibis_plan_from_rows,
    iter_contexts,
    materialize_extract_plan,
    span_dict,
    text_from_file_ctx,
)
from extract.schema_ops import ExtractNormalizeOptions
from ibis_engine.plan import IbisPlan

if TYPE_CHECKING:
    from extract.evidence_plan import EvidencePlan


@dataclass(frozen=True)
class ASTExtractOptions:
    """Define AST extraction options."""

    type_comments: bool = True
    feature_version: tuple[int, int] | int | None = None
    mode: Literal["exec", "eval", "single", "func_type"] = "exec"
    optimize: Literal[-1, 0, 1, 2] | None = None
    allow_top_level_await: bool = False
    dont_inherit: bool = False
    batch_size: int | None = None
    max_bytes: int | None = 50_000_000
    max_nodes: int | None = 1_000_000
    cache_by_sha: bool = True
    repo_id: str | None = None


@dataclass(frozen=True)
class ASTExtractResult:
    """Hold extracted AST tables for nodes, edges, and errors."""

    ast_files: TableLike


@dataclass(frozen=True)
class _AstWalkResult:
    nodes: list[dict[str, object]]
    edges: list[dict[str, object]]
    docstrings: list[dict[str, object]]
    imports: list[dict[str, object]]
    defs: list[dict[str, object]]
    calls: list[dict[str, object]]
    type_ignores: list[dict[str, object]]


@dataclass
class _AstWalkAccumulator:
    nodes: list[dict[str, object]] = dataclass_field(default_factory=list)
    edges: list[dict[str, object]] = dataclass_field(default_factory=list)
    docstrings: list[dict[str, object]] = dataclass_field(default_factory=list)
    imports: list[dict[str, object]] = dataclass_field(default_factory=list)
    defs: list[dict[str, object]] = dataclass_field(default_factory=list)
    calls: list[dict[str, object]] = dataclass_field(default_factory=list)
    type_ignores: list[dict[str, object]] = dataclass_field(default_factory=list)

    def to_result(self) -> _AstWalkResult:
        return _AstWalkResult(
            nodes=self.nodes,
            edges=self.edges,
            docstrings=self.docstrings,
            imports=self.imports,
            defs=self.defs,
            calls=self.calls,
            type_ignores=self.type_ignores,
        )


AST_LINE_BASE = 1
AST_COL_UNIT = "byte"
AST_END_EXCLUSIVE = True


class AstLimitError(ValueError):
    """Raised when AST extraction exceeds configured limits."""


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


def _stringify_attr_value(value: object) -> str | None:
    if isinstance(value, (str, int, bool)):
        return str(value)
    if (
        isinstance(value, list)
        and value
        and all(isinstance(item, (str, int, bool)) for item in value)
    ):
        return json.dumps(value, ensure_ascii=True)
    return None


def _node_scalar_attrs(node: ast.AST) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for field in getattr(node, "_fields", ()):
        value = getattr(node, field, None)
        if isinstance(value, ast.AST):
            continue
        if isinstance(value, list) and (
            not value or any(isinstance(item, ast.AST) for item in value)
        ):
            continue
        serialized = _stringify_attr_value(value)
        if serialized is not None:
            attrs[field] = serialized
    return attrs


def _span_spec_from_node(node: ast.AST) -> SpanSpec:
    lineno = _maybe_int(getattr(node, "lineno", None))
    col_offset = _maybe_int(getattr(node, "col_offset", None))
    end_lineno = _maybe_int(getattr(node, "end_lineno", None))
    end_col_offset = _maybe_int(getattr(node, "end_col_offset", None))
    return SpanSpec(
        start_line0=lineno - AST_LINE_BASE if lineno is not None else None,
        start_col=col_offset,
        end_line0=end_lineno - AST_LINE_BASE if end_lineno is not None else None,
        end_col=end_col_offset,
        end_exclusive=AST_END_EXCLUSIVE,
        col_unit=AST_COL_UNIT,
    )


def _docstring_literal(node: ast.AST) -> ast.Expr | None:
    body = getattr(node, "body", None)
    if not isinstance(body, list) or not body:
        return None
    first = body[0]
    if not isinstance(first, ast.Expr):
        return None
    value = first.value
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return first
    return None


def _docstring_row(
    node: ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef,
    *,
    ast_id: int,
    source: str,
) -> dict[str, object] | None:
    literal = _docstring_literal(node)
    if literal is None:
        return None
    docstring = ast.get_docstring(node, clean=True)
    if docstring is None:
        return None
    segment = ast.get_source_segment(source, literal, padded=False)
    return {
        "owner_ast_id": ast_id,
        "owner_kind": type(node).__name__,
        "owner_name": _node_name(node),
        "docstring": docstring,
        "span": span_dict(_span_spec_from_node(literal)),
        "source": segment,
        "attrs": attrs_map({}),
    }


def _def_row(
    node: ast.AST,
    *,
    ast_id: int,
    parent_ast_id: int | None,
) -> dict[str, object] | None:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        type_params = getattr(node, "type_params", None)
        attrs: dict[str, object] = {
            "decorator_count": len(node.decorator_list),
            "arg_count": len(node.args.args),
            "posonly_count": len(node.args.posonlyargs),
            "kwonly_count": len(node.args.kwonlyargs),
            "type_params_count": len(type_params) if isinstance(type_params, list) else None,
            "is_async": isinstance(node, ast.AsyncFunctionDef),
        }
    elif isinstance(node, ast.ClassDef):
        type_params = getattr(node, "type_params", None)
        attrs = {
            "decorator_count": len(node.decorator_list),
            "base_count": len(node.bases),
            "keyword_count": len(node.keywords),
            "type_params_count": len(type_params) if isinstance(type_params, list) else None,
        }
    else:
        return None
    return {
        "ast_id": ast_id,
        "parent_ast_id": parent_ast_id,
        "kind": type(node).__name__,
        "name": _node_name(node),
        "span": span_dict(_span_spec_from_node(node)),
        "attrs": attrs_map(attrs),
    }


def _import_rows(
    node: ast.AST,
    *,
    ast_id: int,
    parent_ast_id: int | None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    if isinstance(node, ast.Import):
        module = None
        level = None
        kind = "Import"
        names = node.names
    elif isinstance(node, ast.ImportFrom):
        module = node.module
        level = node.level
        kind = "ImportFrom"
        names = node.names
    else:
        return rows
    span = span_dict(_span_spec_from_node(node))
    for idx, alias in enumerate(names):
        rows.append(
            {
                "ast_id": ast_id,
                "parent_ast_id": parent_ast_id,
                "kind": kind,
                "module": module,
                "name": alias.name,
                "asname": alias.asname,
                "alias_index": idx,
                "level": level,
                "span": span,
                "attrs": attrs_map({}),
            }
        )
    return rows


def _call_row(
    node: ast.AST,
    *,
    ast_id: int,
    parent_ast_id: int | None,
) -> dict[str, object] | None:
    if not isinstance(node, ast.Call):
        return None
    func = node.func
    starred_count = sum(isinstance(arg, ast.Starred) for arg in node.args)
    kw_star_count = sum(kw.arg is None for kw in node.keywords)
    attrs: dict[str, object] = {
        "arg_count": len(node.args),
        "keyword_count": len(node.keywords),
        "starred_count": starred_count,
        "kw_star_count": kw_star_count,
    }
    return {
        "ast_id": ast_id,
        "parent_ast_id": parent_ast_id,
        "func_kind": type(func).__name__,
        "func_name": _node_name(func),
        "span": span_dict(_span_spec_from_node(node)),
        "attrs": attrs_map(attrs),
    }


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


def _normalize_limit(value: int | None, *, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"{name} must be an integer."
        raise AstLimitError(msg)
    if value <= 0:
        msg = f"{name} must be positive."
        raise AstLimitError(msg)
    return value


def _text_size_bytes(text: str, encoding: str | None) -> int:
    codec = encoding or "utf-8"
    try:
        return len(text.encode(codec, errors="replace"))
    except LookupError:
        return len(text.encode("utf-8", errors="replace"))


def _file_size_bytes(file_ctx: FileContext, text: str) -> int:
    if file_ctx.data is not None:
        return len(file_ctx.data)
    if file_ctx.text is not None:
        return _text_size_bytes(file_ctx.text, file_ctx.encoding)
    return _text_size_bytes(text, file_ctx.encoding)


def _limit_errors(
    file_ctx: FileContext,
    *,
    text: str,
    options: ASTExtractOptions,
) -> tuple[int | None, list[dict[str, object]]]:
    error_rows: list[dict[str, object]] = []
    try:
        max_bytes = _normalize_limit(options.max_bytes, name="max_bytes")
        max_nodes = _normalize_limit(options.max_nodes, name="max_nodes")
    except AstLimitError as exc:
        error_rows.append(_exception_error_row(exc))
        return None, error_rows
    if max_bytes is not None:
        size_bytes = _file_size_bytes(file_ctx, text)
        if size_bytes > max_bytes:
            msg = f"AST input size {size_bytes} exceeds max_bytes {max_bytes}."
            error_rows.append(_exception_error_row(AstLimitError(msg)))
            return max_nodes, error_rows
    return max_nodes, error_rows


def _normalize_optimize(value: int | None) -> Literal[-1, 0, 1, 2] | None:
    if value is None:
        return -1
    if value in {-1, 0, 1, 2}:
        return cast("Literal[-1, 0, 1, 2]", value)
    return None


def _parse_via_compile(
    text: str,
    *,
    filename: str,
    options: ASTExtractOptions,
    optimize: Literal[-1, 0, 1, 2],
) -> tuple[ast.AST | None, dict[str, object] | None]:
    flags = ast.PyCF_ONLY_AST if optimize <= 0 else ast.PyCF_OPTIMIZED_AST
    if options.type_comments:
        flags |= ast.PyCF_TYPE_COMMENTS
    if options.allow_top_level_await:
        flags |= ast.PyCF_ALLOW_TOP_LEVEL_AWAIT
    try:
        tree = compile(
            text,
            filename,
            options.mode,
            flags=flags,
            dont_inherit=options.dont_inherit,
            optimize=optimize,
        )
        return cast("ast.AST", tree), None
    except SyntaxError as exc:
        return None, _syntax_error_row(exc)
    except (RecursionError, MemoryError, TypeError, ValueError) as exc:
        return None, _exception_error_row(exc)


def _parse_via_ast_parse(
    text: str,
    *,
    filename: str,
    options: ASTExtractOptions,
    optimize: Literal[-1, 0, 1, 2],
) -> tuple[ast.AST | None, dict[str, object] | None]:
    try:
        return (
            ast.parse(
                text,
                filename=filename,
                mode=options.mode,
                type_comments=options.type_comments,
                feature_version=options.feature_version,
                optimize=optimize,
            ),
            None,
        )
    except SyntaxError as exc:
        return None, _syntax_error_row(exc)
    except (RecursionError, MemoryError, TypeError, ValueError) as exc:
        return None, _exception_error_row(exc)


def _parse_ast_text(
    text: str,
    *,
    filename: str,
    options: ASTExtractOptions,
) -> tuple[ast.AST | None, dict[str, object] | None]:
    if options.feature_version is not None and (
        options.allow_top_level_await or options.dont_inherit
    ):
        msg = "feature_version cannot be combined with allow_top_level_await or dont_inherit."
        return None, _exception_error_row(ValueError(msg))
    optimize = _normalize_optimize(options.optimize)
    if optimize is None:
        msg = "optimize must be -1, 0, 1, or 2."
        return None, _exception_error_row(ValueError(msg))
    if options.allow_top_level_await or options.dont_inherit:
        return _parse_via_compile(text, filename=filename, options=options, optimize=optimize)
    return _parse_via_ast_parse(text, filename=filename, options=options, optimize=optimize)


def _cache_key(file_ctx: FileContext, *, options: ASTExtractOptions) -> tuple[object, ...] | None:
    if not options.cache_by_sha or not file_ctx.file_sha256:
        return None
    return (
        file_ctx.file_sha256,
        options.mode,
        options.feature_version,
        options.type_comments,
        options.optimize,
        options.allow_top_level_await,
        options.dont_inherit,
        options.max_bytes,
        options.max_nodes,
    )


def _ast_row_from_walk(
    file_ctx: FileContext,
    *,
    options: ASTExtractOptions,
    walk: _AstWalkResult | None,
    errors: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "repo": options.repo_id,
        "path": file_ctx.path,
        "file_id": file_ctx.file_id,
        "nodes": walk.nodes if walk is not None else [],
        "edges": walk.edges if walk is not None else [],
        "errors": errors,
        "docstrings": walk.docstrings if walk is not None else [],
        "imports": walk.imports if walk is not None else [],
        "defs": walk.defs if walk is not None else [],
        "calls": walk.calls if walk is not None else [],
        "type_ignores": walk.type_ignores if walk is not None else [],
        "attrs": attrs_map(
            {
                "file_sha256": file_ctx.file_sha256,
                "parse_mode": options.mode,
                "feature_version": options.feature_version,
                "optimize": options.optimize,
                "type_comments": options.type_comments,
                "allow_top_level_await": options.allow_top_level_await,
                "dont_inherit": options.dont_inherit,
                "max_bytes": options.max_bytes,
                "max_nodes": options.max_nodes,
            }
        ),
    }


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


def _ast_id_for_node(idx_map: dict[int, int], node: ast.AST) -> int:
    node_id = id(node)
    ast_id = idx_map.get(node_id)
    if ast_id is None:
        ast_id = len(idx_map)
        idx_map[node_id] = ast_id
    return ast_id


def _node_row(
    node: ast.AST,
    *,
    ast_id: int,
    parent_ast_id: int | None,
    field_name: str | None,
    field_pos: int | None,
) -> dict[str, object]:
    node_attr_values: dict[str, object] = {
        "field_name": field_name,
        "field_pos": field_pos,
    }
    node_attr_values.update(_node_scalar_attrs(node))
    return {
        "ast_id": ast_id,
        "parent_ast_id": parent_ast_id,
        "kind": type(node).__name__,
        "name": _node_name(node),
        "value": _node_value_repr(node),
        "span": span_dict(_span_spec_from_node(node)),
        "attrs": attrs_map(node_attr_values),
    }


def _edge_row(
    *,
    parent_ast_id: int | None,
    ast_id: int,
    field_name: str | None,
    field_pos: int | None,
) -> dict[str, object] | None:
    if parent_ast_id is None:
        return None
    return {
        "src": parent_ast_id,
        "dst": ast_id,
        "kind": "CHILD",
        "slot": field_name,
        "idx": field_pos,
        "attrs": attrs_map({}),
    }


def _append_docstring(
    rows: _AstWalkAccumulator,
    node: ast.AST,
    *,
    ast_id: int,
    source: str,
) -> None:
    if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
        return
    row = _docstring_row(node, ast_id=ast_id, source=source)
    if row is not None:
        rows.docstrings.append(row)


def _append_def(
    rows: _AstWalkAccumulator,
    node: ast.AST,
    *,
    ast_id: int,
    parent_ast_id: int | None,
) -> None:
    row = _def_row(node, ast_id=ast_id, parent_ast_id=parent_ast_id)
    if row is not None:
        rows.defs.append(row)


def _append_call(
    rows: _AstWalkAccumulator,
    node: ast.AST,
    *,
    ast_id: int,
    parent_ast_id: int | None,
) -> None:
    row = _call_row(node, ast_id=ast_id, parent_ast_id=parent_ast_id)
    if row is not None:
        rows.calls.append(row)


def _append_type_ignore(
    rows: _AstWalkAccumulator,
    node: ast.AST,
    *,
    ast_id: int,
) -> None:
    if not isinstance(node, ast.TypeIgnore):
        return
    tag = getattr(node, "tag", None)
    rows.type_ignores.append(
        {
            "ast_id": ast_id,
            "tag": tag if isinstance(tag, str) else None,
            "span": span_dict(_span_spec_from_node(node)),
            "attrs": attrs_map({}),
        }
    )


def _walk_ast(
    root: ast.AST,
    *,
    source: str,
    max_nodes: int | None,
) -> _AstWalkResult:
    rows = _AstWalkAccumulator()
    stack: list[tuple[ast.AST, int | None, str | None, int | None]] = [(root, None, None, None)]
    idx_map: dict[int, int] = {}
    node_count = 0

    while stack:
        if max_nodes is not None and node_count >= max_nodes:
            msg = f"AST node limit exceeded: {max_nodes}."
            raise AstLimitError(msg)
        node, parent_idx, field_name, field_pos = stack.pop()
        node_count += 1
        ast_id = _ast_id_for_node(idx_map, node)
        rows.nodes.append(
            _node_row(
                node,
                ast_id=ast_id,
                parent_ast_id=parent_idx,
                field_name=field_name,
                field_pos=field_pos,
            )
        )
        edge = _edge_row(
            parent_ast_id=parent_idx,
            ast_id=ast_id,
            field_name=field_name,
            field_pos=field_pos,
        )
        if edge is not None:
            rows.edges.append(edge)
        _append_docstring(rows, node, ast_id=ast_id, source=source)
        rows.imports.extend(_import_rows(node, ast_id=ast_id, parent_ast_id=parent_idx))
        _append_def(rows, node, ast_id=ast_id, parent_ast_id=parent_idx)
        _append_call(rows, node, ast_id=ast_id, parent_ast_id=parent_idx)
        _append_type_ignore(rows, node, ast_id=ast_id)

        for child, field, pos in reversed(_iter_child_items(node)):
            stack.append((child, ast_id, field, pos))

    return rows.to_result()


def _parse_and_walk(
    text: str,
    *,
    filename: str,
    options: ASTExtractOptions,
    max_nodes: int | None,
) -> tuple[_AstWalkResult | None, list[dict[str, object]]]:
    error_rows: list[dict[str, object]] = []
    root, err = _parse_ast_text(
        text,
        filename=filename,
        options=options,
    )
    if err is not None:
        error_rows.append(err)
    if root is None:
        return None, error_rows
    try:
        walk = _walk_ast(root, source=text, max_nodes=max_nodes)
    except AstLimitError as exc:
        error_rows.append(_exception_error_row(exc))
        return None, error_rows
    return walk, error_rows


def _extract_ast_for_context(
    file_ctx: FileContext,
    *,
    options: ASTExtractOptions,
    cache: dict[tuple[object, ...], _AstWalkResult] | None = None,
) -> dict[str, object] | None:
    if not file_ctx.file_id or not file_ctx.path:
        return None
    cache_key = _cache_key(file_ctx, options=options)
    if cache is not None and cache_key is not None:
        cached = cache.get(cache_key)
        if cached is not None:
            return _ast_row_from_walk(file_ctx, options=options, walk=cached, errors=[])
    text = text_from_file_ctx(file_ctx)
    if text is None:
        return None
    max_nodes, error_rows = _limit_errors(file_ctx, text=text, options=options)
    walk: _AstWalkResult | None = None
    if not error_rows:
        walk, parse_errors = _parse_and_walk(
            text,
            filename=str(file_ctx.path),
            options=options,
            max_nodes=max_nodes,
        )
        error_rows.extend(parse_errors)
        if walk is not None and cache is not None and cache_key is not None and not parse_errors:
            cache[cache_key] = walk
    return _ast_row_from_walk(file_ctx, options=options, walk=walk, errors=error_rows)


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
    batch_size = _resolve_batch_size(normalized_options)
    row_batches: Iterable[Sequence[Mapping[str, object]]] | None = None
    rows: list[dict[str, object]] | None = None
    if batch_size is None:
        rows = _collect_ast_rows(
            repo_files,
            file_contexts=exec_context.file_contexts,
            options=normalized_options,
        )
    else:
        row_batches = _iter_ast_row_batches(
            repo_files,
            file_contexts=exec_context.file_contexts,
            options=normalized_options,
            batch_size=batch_size,
        )
    evidence_plan = exec_context.evidence_plan
    return {
        "ast_files": _build_ast_plan(
            "ast_files_v1",
            rows,
            row_batches=row_batches,
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
    return list(_iter_ast_rows(repo_files, file_contexts=file_contexts, options=options))


def _iter_ast_row_batches(
    repo_files: TableLike,
    *,
    file_contexts: Iterable[FileContext] | None,
    options: ASTExtractOptions,
    batch_size: int,
) -> Iterable[list[dict[str, object]]]:
    batch: list[dict[str, object]] = []
    for row in _iter_ast_rows(repo_files, file_contexts=file_contexts, options=options):
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _iter_ast_rows(
    repo_files: TableLike,
    *,
    file_contexts: Iterable[FileContext] | None,
    options: ASTExtractOptions,
) -> Iterable[dict[str, object]]:
    cache: dict[tuple[object, ...], _AstWalkResult] | None = None
    if options.cache_by_sha:
        cache = {}
    for file_ctx in iter_contexts(repo_files, file_contexts):
        row = _extract_ast_for_context(file_ctx, options=options, cache=cache)
        if row is not None:
            yield row


def _resolve_batch_size(options: ASTExtractOptions) -> int | None:
    if options.batch_size is None:
        return None
    if options.batch_size <= 0:
        msg = "batch_size must be a positive integer."
        raise ValueError(msg)
    return options.batch_size


def _build_ast_plan(
    name: str,
    rows: list[dict[str, object]] | None,
    *,
    row_batches: Iterable[Sequence[Mapping[str, object]]] | None = None,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None,
) -> IbisPlan:
    if row_batches is not None:
        raw = ibis_plan_from_row_batches(name, row_batches)
    else:
        raw = ibis_plan_from_rows(name, rows or [])
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
