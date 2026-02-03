"""Extract Python AST facts into Arrow tables using shared helpers."""

from __future__ import annotations

import ast
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from dataclasses import field as dataclass_field
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, cast, overload

from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.extract.registry import normalize_options
from datafusion_engine.plan.bundle import DataFusionPlanBundle
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from extract.coordination.line_offsets import LineOffsets
from extract.coordination.schema_ops import ExtractNormalizeOptions
from extract.git.context import discover_repo_root_from_paths
from extract.helpers import (
    ExtractExecutionContext,
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    FileContext,
    SpanSpec,
    attrs_map,
    bytes_from_file_ctx,
    extract_plan_from_row_batches,
    extract_plan_from_rows,
    materialize_extract_plan,
    span_dict,
    text_from_file_ctx,
)
from extract.infrastructure.cache_utils import (
    CacheSetOptions,
    cache_for_extract,
    cache_get,
    cache_lock,
    cache_set,
    cache_ttl_seconds,
    diskcache_profile_from_ctx,
    stable_cache_key,
)
from extract.infrastructure.options import ParallelOptions, RepoOptions, WorklistQueueOptions
from extract.infrastructure.parallel import parallel_map, resolve_max_workers
from extract.infrastructure.result_types import ExtractResult
from extract.infrastructure.schema_cache import ast_files_fingerprint
from extract.infrastructure.worklists import (
    WorklistRequest,
    iter_worklist_contexts,
    worklist_queue_name,
)
from obs.otel.scopes import SCOPE_EXTRACT
from obs.otel.tracing import stage_span
from utils.file_io import read_pyproject_toml

if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache

    from cache.diskcache_factory import DiskCacheProfile
    from extract.coordination.evidence_plan import EvidencePlan
    from extract.scanning.scope_manifest import ScopeManifest
    from extract.session import ExtractSession


@dataclass(frozen=True)
class AstExtractOptions(RepoOptions, WorklistQueueOptions, ParallelOptions):
    """Define AST extraction options."""

    type_comments: bool = True
    feature_version: tuple[int, int] | int | None = None
    mode: Literal["exec", "eval", "single", "func_type"] = "exec"
    optimize: Literal[-1, 0, 1, 2] | None = None
    allow_top_level_await: bool = False
    dont_inherit: bool = True
    max_bytes: int | None = 50_000_000
    max_nodes: int | None = 1_000_000
    cache_by_sha: bool = True


_PYTHON_VERSION_RE = re.compile(r"(\\d+)\\.(\\d+)")


def _parse_requires_python(spec: str) -> tuple[int, int] | None:
    match = _PYTHON_VERSION_RE.search(spec)
    if match is None:
        return None
    return int(match.group(1)), int(match.group(2))


def _feature_version_from_pyproject(repo_root: Path) -> tuple[int, int] | None:
    pyproject = repo_root / "pyproject.toml"
    if not pyproject.exists():
        return None
    try:
        data = read_pyproject_toml(pyproject)
    except (OSError, ValueError):
        return None
    project = data.get("project")
    if isinstance(project, Mapping):
        requires = project.get("requires-python")
        if isinstance(requires, str):
            parsed = _parse_requires_python(requires)
            if parsed is not None:
                return parsed
    tool = data.get("tool")
    if isinstance(tool, Mapping):
        poetry = tool.get("poetry")
        if isinstance(poetry, Mapping):
            dependencies = poetry.get("dependencies")
            if isinstance(dependencies, Mapping):
                requires = dependencies.get("python")
                if isinstance(requires, str):
                    return _parse_requires_python(requires)
    return None


def _infer_repo_root(contexts: Sequence[FileContext]) -> Path | None:
    candidate_paths = [ctx.abs_path for ctx in contexts if ctx.abs_path]
    if candidate_paths:
        git_root = discover_repo_root_from_paths(candidate_paths)
        if git_root is not None:
            return git_root
    for ctx in contexts:
        if not ctx.abs_path or not ctx.path:
            continue
        rel_path = Path(ctx.path)
        if rel_path.is_absolute():
            continue
        abs_path = Path(ctx.abs_path).resolve()
        rel_parts = rel_path.parts
        if len(rel_parts) == 0:
            continue
        if abs_path.parts[-len(rel_parts) :] != rel_parts:
            continue
        return abs_path.parents[len(rel_parts) - 1]
    return None


def _resolve_feature_version(
    options: AstExtractOptions,
    contexts: Sequence[FileContext],
) -> AstExtractOptions:
    if options.feature_version is not None:
        return options
    repo_root = _infer_repo_root(contexts)
    if repo_root is None:
        return options
    feature_version = _feature_version_from_pyproject(repo_root)
    if feature_version is None:
        return options
    resolved = replace(options, feature_version=feature_version)
    if resolved.dont_inherit:
        resolved = replace(resolved, dont_inherit=False)
    if resolved.allow_top_level_await:
        resolved = replace(resolved, allow_top_level_await=False)
    return resolved


def _format_feature_version(value: tuple[int, int] | int | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, tuple):
        return f"{value[0]}.{value[1]}"
    return str(value)


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


def _line_offsets(file_ctx: FileContext) -> LineOffsets | None:
    data = bytes_from_file_ctx(file_ctx)
    if data is None:
        return None
    return LineOffsets.from_bytes(data)


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


def _ast_row_worker(
    file_ctx: FileContext,
    *,
    options: AstExtractOptions,
    cache_profile: DiskCacheProfile | None,
    cache_ttl: float | None,
) -> dict[str, object] | None:
    cache = cache_for_extract(cache_profile) if options.cache_by_sha else None
    return _extract_ast_for_context(
        file_ctx,
        options=options,
        cache=cache,
        cache_ttl=cache_ttl,
    )


def _ast_row_from_walk(
    file_ctx: FileContext,
    *,
    options: AstExtractOptions,
    walk: _AstWalkResult | None,
    errors: list[dict[str, object]],
) -> dict[str, object]:
    parse_manifest = [
        {
            "parse_mode": options.mode,
            "feature_version": _format_feature_version(options.feature_version),
            "optimize": options.optimize,
            "type_comments": options.type_comments,
            "allow_top_level_await": options.allow_top_level_await,
            "dont_inherit": options.dont_inherit,
        }
    ]
    return {
        "repo": options.repo_id,
        "path": file_ctx.path,
        "file_id": file_ctx.file_id,
        "file_sha256": file_ctx.file_sha256,
        "nodes": walk.nodes if walk is not None else [],
        "edges": walk.edges if walk is not None else [],
        "errors": errors,
        "docstrings": walk.docstrings if walk is not None else [],
        "imports": walk.imports if walk is not None else [],
        "defs": walk.defs if walk is not None else [],
        "calls": walk.calls if walk is not None else [],
        "type_ignores": walk.type_ignores if walk is not None else [],
        "parse_manifest": parse_manifest,
        "attrs": attrs_map(
            {
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


@dataclass(frozen=True)
class AstNodeContext:
    ast_id: int
    parent_ast_id: int | None
    field_name: str | None
    field_pos: int | None
    line_offsets: LineOffsets | None


def _node_row(node: ast.AST, *, ctx: AstNodeContext) -> dict[str, object]:
    node_attr_values: dict[str, object] = {
        "field_name": ctx.field_name,
        "field_pos": ctx.field_pos,
    }
    node_attr_values.update(_node_scalar_attrs(node))
    return {
        "ast_id": ctx.ast_id,
        "parent_ast_id": ctx.parent_ast_id,
        "kind": type(node).__name__,
        "name": _node_name(node),
        "value": _node_value_repr(node),
        "span": span_dict(_span_spec_from_node(node, line_offsets=ctx.line_offsets)),
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
    line_offsets: LineOffsets | None,
) -> None:
    if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
        return
    row = _docstring_row(node, ast_id=ast_id, source=source, line_offsets=line_offsets)
    if row is not None:
        rows.docstrings.append(row)


def _append_def(
    rows: _AstWalkAccumulator,
    node: ast.AST,
    *,
    ctx: AstNodeContext,
    source: str,
) -> None:
    row = _def_row(
        node,
        ast_id=ctx.ast_id,
        parent_ast_id=ctx.parent_ast_id,
        source=source,
        line_offsets=ctx.line_offsets,
    )
    if row is not None:
        rows.defs.append(row)


def _append_call(
    rows: _AstWalkAccumulator,
    node: ast.AST,
    *,
    ast_id: int,
    parent_ast_id: int | None,
    line_offsets: LineOffsets | None,
) -> None:
    row = _call_row(
        node,
        ast_id=ast_id,
        parent_ast_id=parent_ast_id,
        line_offsets=line_offsets,
    )
    if row is not None:
        rows.calls.append(row)


def _append_type_ignore(
    rows: _AstWalkAccumulator,
    node: ast.AST,
    *,
    ast_id: int,
    line_offsets: LineOffsets | None,
) -> None:
    if not isinstance(node, ast.TypeIgnore):
        return
    tag = getattr(node, "tag", None)
    rows.type_ignores.append(
        {
            "ast_id": ast_id,
            "tag": tag if isinstance(tag, str) else None,
            "span": span_dict(_span_spec_from_node(node, line_offsets=line_offsets)),
            "attrs": attrs_map({}),
        }
    )


def _walk_ast(
    root: ast.AST,
    *,
    source: str,
    max_nodes: int | None,
    line_offsets: LineOffsets | None,
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
        ctx = AstNodeContext(
            ast_id=ast_id,
            parent_ast_id=parent_idx,
            field_name=field_name,
            field_pos=field_pos,
            line_offsets=line_offsets,
        )
        rows.nodes.append(
            _node_row(
                node,
                ctx=ctx,
            )
        )
        edge = _edge_row(
            parent_ast_id=ctx.parent_ast_id,
            ast_id=ctx.ast_id,
            field_name=ctx.field_name,
            field_pos=ctx.field_pos,
        )
        if edge is not None:
            rows.edges.append(edge)
        _append_docstring(
            rows,
            node,
            ast_id=ast_id,
            source=source,
            line_offsets=line_offsets,
        )
        rows.imports.extend(
            _import_rows(
                node,
                ast_id=ast_id,
                parent_ast_id=parent_idx,
                line_offsets=line_offsets,
            )
        )
        _append_def(
            rows,
            node,
            ctx=ctx,
            source=source,
        )
        _append_call(
            rows,
            node,
            ast_id=ast_id,
            parent_ast_id=parent_idx,
            line_offsets=line_offsets,
        )
        _append_type_ignore(rows, node, ast_id=ast_id, line_offsets=line_offsets)

        for child, field, pos in reversed(_iter_child_items(node)):
            stack.append((child, ast_id, field, pos))

    return rows.to_result()


def _parse_and_walk(
    text: str,
    *,
    filename: str,
    options: AstExtractOptions,
    max_nodes: int | None,
    line_offsets: LineOffsets | None,
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
        walk = _walk_ast(
            root,
            source=text,
            max_nodes=max_nodes,
            line_offsets=line_offsets,
        )
    except AstLimitError as exc:
        error_rows.append(_exception_error_row(exc))
        return None, error_rows
    return walk, error_rows


def _extract_ast_for_context(
    file_ctx: FileContext,
    *,
    options: AstExtractOptions,
    cache: Cache | FanoutCache | None = None,
    cache_ttl: float | None = None,
) -> dict[str, object] | None:
    if not file_ctx.file_id or not file_ctx.path:
        return None
    cache_key = _cache_key(file_ctx, options=options)
    use_cache = cache is not None and cache_key is not None
    cache_key_str = stable_cache_key("ast", {"key": cache_key}) if use_cache else None
    if use_cache and cache_key_str is not None:
        cached = cache_get(cache, key=cache_key_str, default=None)
        if isinstance(cached, _AstWalkResult):
            return _ast_row_from_walk(file_ctx, options=options, walk=cached, errors=[])

    def _build_row() -> dict[str, object] | None:
        text = text_from_file_ctx(file_ctx)
        if text is None:
            return None
        line_offsets = _line_offsets(file_ctx)
        max_nodes, error_rows = _limit_errors(file_ctx, text=text, options=options)
        walk: _AstWalkResult | None = None
        if not error_rows:
            walk, parse_errors = _parse_and_walk(
                text,
                filename=str(file_ctx.path),
                options=options,
                max_nodes=max_nodes,
                line_offsets=line_offsets,
            )
            error_rows.extend(parse_errors)
            if (
                use_cache
                and cache is not None
                and cache_key_str is not None
                and walk is not None
                and not error_rows
            ):
                cache_set(
                    cache,
                    key=cache_key_str,
                    value=walk,
                    options=CacheSetOptions(
                        expire=cache_ttl,
                        tag=options.repo_id,
                    ),
                )
        return _ast_row_from_walk(file_ctx, options=options, walk=walk, errors=error_rows)

    if use_cache and cache_key_str is not None:
        with cache_lock(cache, key=cache_key_str):
            cached = cache_get(cache, key=cache_key_str, default=None)
            if isinstance(cached, _AstWalkResult):
                return _ast_row_from_walk(file_ctx, options=options, walk=cached, errors=[])
            return _build_row()
    return _build_row()


def _extract_ast_for_row(
    row: dict[str, object],
    *,
    options: AstExtractOptions,
) -> dict[str, object] | None:
    file_ctx = FileContext.from_repo_row(row)
    return _extract_ast_for_context(file_ctx, options=options)


def extract_ast(
    repo_files: TableLike,
    options: AstExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> ExtractResult[TableLike]:
    """Extract a minimal AST fact set per file.

    Returns
    -------
    ExtractResult[TableLike]
        Tables of AST nodes, edges, and errors.
    """
    normalized_options = normalize_options("ast", options, AstExtractOptions)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    determinism_tier = exec_context.determinism_tier()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    plans = extract_ast_plans(
        repo_files,
        options=normalized_options,
        context=exec_context,
    )
    table = cast(
        "TableLike",
        materialize_extract_plan(
            "ast_files_v1",
            plans["ast_files"],
            runtime_profile=runtime_profile,
            determinism_tier=determinism_tier,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
    )
    return ExtractResult(table=table, extractor_name="ast")


def extract_ast_plans(
    repo_files: TableLike,
    options: AstExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> dict[str, DataFusionPlanBundle]:
    """Extract AST plans for nested file records.

    Returns
    -------
    dict[str, DataFusionPlanBundle]
        Plan bundle keyed by ``ast_files``.
    """
    normalized_options = normalize_options("ast", options, AstExtractOptions)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    batch_size = _resolve_batch_size(normalized_options)
    row_batches: Iterable[Sequence[Mapping[str, object]]] | None = None
    rows: list[dict[str, object]] | None = None
    request = _AstRowRequest(
        repo_files=repo_files,
        file_contexts=exec_context.file_contexts,
        scope_manifest=exec_context.scope_manifest,
        options=normalized_options,
        runtime_profile=runtime_profile,
    )
    if batch_size is None:
        rows = _collect_ast_rows(
            repo_files,
            file_contexts=exec_context.file_contexts,
            scope_manifest=exec_context.scope_manifest,
            options=normalized_options,
            runtime_profile=runtime_profile,
        )
    else:
        row_batches = _iter_ast_row_batches(request, batch_size=batch_size)
    evidence_plan = exec_context.evidence_plan
    plan_context = _AstPlanContext(
        normalize=normalize,
        evidence_plan=evidence_plan,
        session=session,
    )
    return {
        "ast_files": _build_ast_plan(
            "ast_files_v1",
            rows,
            row_batches=row_batches,
            plan_context=plan_context,
        ),
    }


@dataclass(frozen=True)
class _AstRowRequest:
    repo_files: TableLike
    file_contexts: Iterable[FileContext] | None
    scope_manifest: ScopeManifest | None
    options: AstExtractOptions
    runtime_profile: DataFusionRuntimeProfile | None


def _collect_ast_rows(
    repo_files: TableLike,
    *,
    file_contexts: Iterable[FileContext] | None,
    scope_manifest: ScopeManifest | None,
    options: AstExtractOptions,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> list[dict[str, object]]:
    request = _AstRowRequest(
        repo_files=repo_files,
        file_contexts=file_contexts,
        scope_manifest=scope_manifest,
        options=options,
        runtime_profile=runtime_profile,
    )
    return list(_iter_ast_rows(request))


def _iter_ast_row_batches(
    request: _AstRowRequest,
    *,
    batch_size: int,
) -> Iterable[list[dict[str, object]]]:
    batch: list[dict[str, object]] = []
    for row in _iter_ast_rows(request):
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _iter_ast_rows(request: _AstRowRequest) -> Iterable[dict[str, object]]:
    contexts = list(
        iter_worklist_contexts(
            WorklistRequest(
                repo_files=request.repo_files,
                output_table="ast_files_v1",
                runtime_profile=request.runtime_profile,
                file_contexts=request.file_contexts,
                queue_name=(
                    worklist_queue_name(
                        output_table="ast_files_v1", repo_id=request.options.repo_id
                    )
                    if request.options.use_worklist_queue
                    else None
                ),
                scope_manifest=request.scope_manifest,
            )
        )
    )
    if not contexts:
        return
    resolved_options = _resolve_feature_version(request.options, contexts)
    cache_profile = diskcache_profile_from_ctx(request.runtime_profile)
    cache_ttl = cache_ttl_seconds(cache_profile, "extract")
    if not resolved_options.parallel:
        for file_ctx in contexts:
            row = _ast_row_worker(
                file_ctx,
                options=resolved_options,
                cache_profile=cache_profile,
                cache_ttl=cache_ttl,
            )
            if row is not None:
                yield row
        return
    runner = partial(
        _ast_row_worker,
        options=resolved_options,
        cache_profile=cache_profile,
        cache_ttl=cache_ttl,
    )
    max_workers = resolve_max_workers(
        resolved_options.max_workers,
        kind="cpu",
    )
    for row in parallel_map(contexts, runner, max_workers=max_workers):
        if row is not None:
            yield row


def _resolve_batch_size(options: AstExtractOptions) -> int | None:
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
    plan_context: _AstPlanContext,
) -> DataFusionPlanBundle:
    plan_options = ExtractPlanOptions(
        normalize=plan_context.normalize,
        evidence_plan=plan_context.evidence_plan,
    )
    if row_batches is not None:
        return extract_plan_from_row_batches(
            name,
            row_batches,
            session=plan_context.session,
            options=plan_options,
        )
    return extract_plan_from_rows(
        name,
        rows or [],
        session=plan_context.session,
        options=plan_options,
    )


@dataclass(frozen=True)
class _AstPlanContext:
    normalize: ExtractNormalizeOptions
    evidence_plan: EvidencePlan | None
    session: ExtractSession


class _AstTablesKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: AstExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    prefer_reader: bool


class _AstTablesKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: AstExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    prefer_reader: Literal[False]


class _AstTablesKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: AstExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
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
    with stage_span(
        "extract.ast_tables",
        stage="extract",
        scope_name=SCOPE_EXTRACT,
        attributes={"codeanatomy.extractor": "ast"},
    ):
        repo_files = kwargs["repo_files"]
        normalized_options = normalize_options("ast", kwargs.get("options"), AstExtractOptions)
        file_contexts = kwargs.get("file_contexts")
        evidence_plan = kwargs.get("evidence_plan")
        profile = kwargs.get("profile", "default")
        prefer_reader = kwargs.get("prefer_reader", False)
        exec_context = ExtractExecutionContext(
            file_contexts=file_contexts,
            evidence_plan=evidence_plan,
            scope_manifest=kwargs.get("scope_manifest"),
            session=kwargs.get("session"),
            profile=profile,
        )
        session = exec_context.ensure_session()
        exec_context = replace(exec_context, session=session)
        runtime_profile = exec_context.ensure_runtime_profile()
        determinism_tier = exec_context.determinism_tier()
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
                runtime_profile=runtime_profile,
                determinism_tier=determinism_tier,
                options=ExtractMaterializeOptions(
                    normalize=normalize,
                    prefer_reader=prefer_reader,
                    apply_post_kernels=True,
                ),
            ),
        }
