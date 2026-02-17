"""Extract Python AST facts into Arrow tables using shared helpers."""

from __future__ import annotations

import ast
import json
from collections.abc import Sequence
from typing import Literal, cast

from extract.coordination.context import (
    FileContext,
    SpanSpec,
    attrs_map,
    span_dict,
)
from extract.coordination.line_offsets import LineOffsets
from extract.extractors.ast.setup import (
    AstExtractOptions,
)
from extract.extractors.ast.visitors import (
    AstLimitError,
)
from extract.infrastructure.schema_cache import ast_files_fingerprint

AST_LINE_BASE = 1
AST_COL_UNIT = "byte"
AST_END_EXCLUSIVE = True


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


def _span_spec_from_node(
    node: ast.AST,
    *,
    line_offsets: LineOffsets | None = None,
) -> SpanSpec:
    lineno = _maybe_int(getattr(node, "lineno", None))
    col_offset = _maybe_int(getattr(node, "col_offset", None))
    end_lineno = _maybe_int(getattr(node, "end_lineno", None))
    end_col_offset = _maybe_int(getattr(node, "end_col_offset", None))
    start_line0 = lineno - AST_LINE_BASE if lineno is not None else None
    end_line0 = end_lineno - AST_LINE_BASE if end_lineno is not None else None
    byte_start = None
    byte_len = None
    if line_offsets is not None:
        start_offset = line_offsets.byte_offset(start_line0, col_offset)
        end_offset = line_offsets.byte_offset(end_line0, end_col_offset)
        if start_offset is not None and end_offset is not None:
            byte_start = start_offset
            byte_len = max(0, end_offset - start_offset)
    return SpanSpec(
        start_line0=start_line0,
        start_col=col_offset,
        end_line0=end_line0,
        end_col=end_col_offset,
        end_exclusive=AST_END_EXCLUSIVE,
        col_unit=AST_COL_UNIT,
        byte_start=byte_start,
        byte_len=byte_len,
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
    line_offsets: LineOffsets | None,
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
        "span": span_dict(_span_spec_from_node(literal, line_offsets=line_offsets)),
        "source": segment,
        "attrs": attrs_map({}),
    }


def _segment_list(source: str, nodes: Sequence[ast.AST]) -> list[str]:
    segments: list[str] = []
    for node in nodes:
        segment = ast.get_source_segment(source, node, padded=False)
        if segment is None:
            continue
        segments.append(segment)
    return segments


def _annotation_nodes(args: ast.arguments) -> list[ast.expr]:
    nodes: list[ast.expr] = [
        item.annotation
        for item in args.posonlyargs + args.args + args.kwonlyargs
        if item.annotation is not None
    ]
    if args.vararg is not None and args.vararg.annotation is not None:
        nodes.append(args.vararg.annotation)
    if args.kwarg is not None and args.kwarg.annotation is not None:
        nodes.append(args.kwarg.annotation)
    return nodes


def _def_row(
    node: ast.AST,
    *,
    ast_id: int,
    parent_ast_id: int | None,
    source: str,
    line_offsets: LineOffsets | None,
) -> dict[str, object] | None:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        type_params = getattr(node, "type_params", None)
        decorator_segments = _segment_list(source, node.decorator_list)
        default_nodes = [default for default in node.args.defaults if default is not None]
        default_nodes += [default for default in node.args.kw_defaults if default is not None]
        default_segments = _segment_list(source, default_nodes)
        annotation_segments = _segment_list(source, _annotation_nodes(node.args))
        attrs: dict[str, object] = {
            "decorator_count": len(node.decorator_list),
            "arg_count": len(node.args.args),
            "posonly_count": len(node.args.posonlyargs),
            "kwonly_count": len(node.args.kwonlyargs),
            "type_params_count": len(type_params) if isinstance(type_params, list) else None,
            "is_async": isinstance(node, ast.AsyncFunctionDef),
        }
        if decorator_segments:
            attrs["decorator_sources"] = json.dumps(decorator_segments)
        if default_segments:
            attrs["default_sources"] = json.dumps(default_segments)
        if annotation_segments:
            attrs["annotation_sources"] = json.dumps(annotation_segments)
        if node.returns is not None:
            returns_segment = ast.get_source_segment(source, node.returns, padded=False)
            if returns_segment is not None:
                attrs["returns_source"] = returns_segment
    elif isinstance(node, ast.ClassDef):
        type_params = getattr(node, "type_params", None)
        decorator_segments = _segment_list(source, node.decorator_list)
        base_segments = _segment_list(source, node.bases)
        attrs = {
            "decorator_count": len(node.decorator_list),
            "base_count": len(node.bases),
            "keyword_count": len(node.keywords),
            "type_params_count": len(type_params) if isinstance(type_params, list) else None,
        }
        if decorator_segments:
            attrs["decorator_sources"] = json.dumps(decorator_segments)
        if base_segments:
            attrs["base_sources"] = json.dumps(base_segments)
    else:
        return None
    return {
        "ast_id": ast_id,
        "parent_ast_id": parent_ast_id,
        "kind": type(node).__name__,
        "name": _node_name(node),
        "span": span_dict(_span_spec_from_node(node, line_offsets=line_offsets)),
        "attrs": attrs_map(attrs),
    }


def _import_rows(
    node: ast.AST,
    *,
    ast_id: int,
    parent_ast_id: int | None,
    line_offsets: LineOffsets | None,
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
    span = span_dict(_span_spec_from_node(node, line_offsets=line_offsets))
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
    line_offsets: LineOffsets | None,
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
        "span": span_dict(_span_spec_from_node(node, line_offsets=line_offsets)),
        "attrs": attrs_map(attrs),
    }


def _syntax_error_row(exc: SyntaxError) -> dict[str, object]:
    lineno = _maybe_int(getattr(exc, "lineno", None))
    offset = _maybe_int(getattr(exc, "offset", None))
    end_lineno = _maybe_int(getattr(exc, "end_lineno", None))
    end_offset = _maybe_int(getattr(exc, "end_offset", None))
    text = getattr(exc, "text", None)
    attrs: dict[str, object] = {}
    if isinstance(text, str) and text:
        attrs["text"] = text
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
        "attrs": attrs_map(attrs),
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
    options: AstExtractOptions,
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
    options: AstExtractOptions,
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
    options: AstExtractOptions,
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
    options: AstExtractOptions,
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


def _cache_key(file_ctx: FileContext, *, options: AstExtractOptions) -> tuple[object, ...] | None:
    if not options.cache_by_sha or not file_ctx.file_sha256:
        return None
    return (
        file_ctx.file_sha256,
        ast_files_fingerprint(),
        options.mode,
        options.feature_version,
        options.type_comments,
        options.optimize,
        options.allow_top_level_await,
        options.dont_inherit,
        options.max_bytes,
        options.max_nodes,
    )


from extract.extractors.ast.builders_runtime import (
    _extract_ast_for_context,
    extract_ast,
    extract_ast_plans,
    extract_ast_tables,
)

__all__ = [
    "_extract_ast_for_context",
    "extract_ast",
    "extract_ast_plans",
    "extract_ast_tables",
]
