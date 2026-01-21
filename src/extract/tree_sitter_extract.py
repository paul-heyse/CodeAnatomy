"""Extract tree-sitter nodes and diagnostics into Arrow tables using shared helpers."""

from __future__ import annotations

import ast
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, cast, overload

import tree_sitter_python
from tree_sitter import (
    LANGUAGE_VERSION,
    MIN_COMPATIBLE_LANGUAGE_VERSION,
    Language,
    Node,
    Parser,
    QueryCursor,
    Range,
)

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.ids import span_id
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.extract_registry import normalize_options
from extract.helpers import (
    ExtractExecutionContext,
    ExtractMaterializeOptions,
    FileContext,
    SpanSpec,
    apply_query_and_project,
    attrs_map,
    bytes_from_file_ctx,
    ibis_plan_from_rows,
    iter_contexts,
    materialize_extract_plan,
    span_dict,
)
from extract.tree_sitter_cache import TreeSitterCache, TreeSitterParseResult
from extract.tree_sitter_queries import TreeSitterQueryPack, compile_query_pack
from ibis_engine.plan import IbisPlan

if TYPE_CHECKING:
    from extract.evidence_plan import EvidencePlan

type Row = dict[str, object]

PY_LANGUAGE = Language(tree_sitter_python.language())


@dataclass(frozen=True)
class TreeSitterExtractOptions:
    """Configure tree-sitter extraction options."""

    include_nodes: bool = True
    include_errors: bool = True
    include_missing: bool = True
    include_edges: bool = True
    include_captures: bool = True
    include_defs: bool = True
    include_calls: bool = True
    include_imports: bool = True
    include_docstrings: bool = True
    include_stats: bool = True
    extensions: tuple[str, ...] = (".py", ".pyi")
    parser_timeout_micros: int | None = None
    query_match_limit: int = 10_000
    query_timeout_micros: int | None = None
    max_text_bytes: int = 256
    max_docstring_bytes: int = 2048
    incremental: bool = False
    incremental_cache_size: int = 256
    repo_id: str | None = None


@dataclass(frozen=True)
class TreeSitterExtractResult:
    """Extracted tree-sitter tables for nodes and diagnostics."""

    tree_sitter_files: TableLike


@dataclass
class _QueryStats:
    match_count: int = 0
    capture_count: int = 0
    match_limit_exceeded: bool = False


@dataclass
class _QueryRows:
    captures: list[Row]
    defs: list[Row]
    calls: list[Row]
    imports: list[Row]
    docstrings: list[Row]
    stats: _QueryStats


@dataclass
class _ParseStats:
    parse_ms: int
    parse_timed_out: bool
    used_incremental: bool


@dataclass
class _NodeStats:
    node_count: int = 0
    named_count: int = 0
    error_count: int = 0
    missing_count: int = 0


def _assert_language_abi(lang: Language) -> None:
    if not (MIN_COMPATIBLE_LANGUAGE_VERSION <= lang.abi_version <= LANGUAGE_VERSION):
        msg = f"Tree-sitter ABI mismatch: {lang.abi_version}"
        raise ValueError(msg)


def _parser(options: TreeSitterExtractOptions) -> Parser:
    _assert_language_abi(PY_LANGUAGE)
    if options.parser_timeout_micros is None:
        return Parser(PY_LANGUAGE)
    return Parser(PY_LANGUAGE, timeout_micros=int(options.parser_timeout_micros))


@cache
def _default_cache(max_entries: int) -> TreeSitterCache:
    return TreeSitterCache(max_entries=max_entries)


@cache
def _query_pack() -> TreeSitterQueryPack:
    return compile_query_pack(PY_LANGUAGE)


def _should_parse(file_ctx: FileContext, options: TreeSitterExtractOptions) -> bool:
    if not file_ctx.path:
        return False
    path = file_ctx.path.lower()
    return any(path.endswith(ext) for ext in options.extensions)


def _maybe_int(value: object) -> int | None:
    return value if isinstance(value, int) else None


def _point_parts(point: object | None) -> tuple[int | None, int | None]:
    if point is None:
        return None, None
    return _maybe_int(getattr(point, "row", None)), _maybe_int(getattr(point, "column", None))


def _span_spec(node: Node) -> SpanSpec:
    start = int(node.start_byte)
    end = int(node.end_byte)
    start_line, start_col = _point_parts(node.start_point)
    end_line, end_col = _point_parts(node.end_point)
    return SpanSpec(
        start_line0=start_line,
        start_col=start_col,
        end_line0=end_line,
        end_col=end_col,
        end_exclusive=True,
        col_unit="byte",
        byte_start=start,
        byte_len=end - start,
    )


def _node_id(file_ctx: FileContext, node: Node) -> str:
    return span_id(file_ctx.path, int(node.start_byte), int(node.end_byte), kind=node.type)


def _node_text(
    node: Node,
    *,
    data: bytes,
    max_bytes: int,
    allow_non_leaf: bool = False,
) -> tuple[str | None, bool]:
    if max_bytes <= 0:
        return None, False
    if not allow_non_leaf and node.child_count != 0:
        return None, False
    if not node.is_named:
        return None, False
    start = int(node.start_byte)
    end = int(node.end_byte)
    if end <= start:
        return None, False
    text_bytes = data[start:end]
    truncated = False
    if len(text_bytes) > max_bytes:
        text_bytes = text_bytes[:max_bytes]
        truncated = True
    return text_bytes.decode("utf-8", errors="replace"), truncated


def _node_entry(
    node: Node,
    *,
    file_ctx: FileContext,
    data: bytes,
    options: TreeSitterExtractOptions,
    parent: Node | None,
) -> Row:
    node_id = _node_id(file_ctx, node)
    parent_id = _node_id(file_ctx, parent) if parent is not None else None
    text, truncated = _node_text(node, data=data, max_bytes=options.max_text_bytes)
    attrs: dict[str, object] = {}
    if text is not None:
        attrs["text"] = text
    if truncated:
        attrs["text_truncated"] = True
    return {
        "node_id": node_id,
        "node_uid": int(node.id),
        "parent_id": parent_id,
        "kind": node.type,
        "kind_id": int(node.kind_id),
        "grammar_id": int(node.grammar_id),
        "grammar_name": node.grammar_name,
        "span": span_dict(_span_spec(node)),
        "flags": {
            "is_named": bool(node.is_named),
            "has_error": bool(node.has_error),
            "is_error": bool(node.is_error),
            "is_missing": bool(node.is_missing),
            "is_extra": bool(node.is_extra),
            "has_changes": bool(node.has_changes),
        },
        "attrs": attrs_map(attrs),
    }


def _edge_entry(
    *,
    file_ctx: FileContext,
    parent: Node,
    child: Node,
    field_name: str | None,
    child_index: int | None,
) -> Row:
    return {
        "parent_id": _node_id(file_ctx, parent),
        "child_id": _node_id(file_ctx, child),
        "field_name": field_name,
        "child_index": child_index,
        "attrs": attrs_map({}),
    }


def _error_entry(node: Node, *, file_ctx: FileContext) -> Row:
    start = int(node.start_byte)
    end = int(node.end_byte)
    error_id = span_id(file_ctx.path, start, end, kind="ts_error")
    node_id = span_id(file_ctx.path, start, end, kind=node.type)
    return {
        "error_id": error_id,
        "node_id": node_id,
        "span": span_dict(_span_spec(node)),
        "attrs": attrs_map({}),
    }


def _missing_entry(node: Node, *, file_ctx: FileContext) -> Row:
    start = int(node.start_byte)
    end = int(node.end_byte)
    missing_id = span_id(file_ctx.path, start, end, kind="ts_missing")
    node_id = span_id(file_ctx.path, start, end, kind=node.type)
    return {
        "missing_id": missing_id,
        "node_id": node_id,
        "span": span_dict(_span_spec(node)),
        "attrs": attrs_map({}),
    }


def _capture_entry(
    node: Node,
    *,
    file_ctx: FileContext,
    data: bytes,
    options: TreeSitterExtractOptions,
    query_name: str,
    capture_name: str,
    pattern_index: int,
) -> Row:
    start = int(node.start_byte)
    end = int(node.end_byte)
    capture_id = span_id(
        file_ctx.path,
        start,
        end,
        kind=f"ts_capture:{query_name}:{capture_name}",
    )
    text, truncated = _node_text(
        node,
        data=data,
        max_bytes=options.max_text_bytes,
        allow_non_leaf=True,
    )
    attrs: dict[str, object] = {}
    if text is not None:
        attrs["text"] = text
    if truncated:
        attrs["text_truncated"] = True
    return {
        "capture_id": capture_id,
        "query_name": query_name,
        "capture_name": capture_name,
        "pattern_index": pattern_index,
        "node_id": _node_id(file_ctx, node),
        "node_kind": node.type,
        "span": span_dict(_span_spec(node)),
        "attrs": attrs_map(attrs),
    }


def _def_row(
    node: Node,
    *,
    file_ctx: FileContext,
    name: str | None,
) -> Row:
    return {
        "node_id": _node_id(file_ctx, node),
        "parent_id": _node_id(file_ctx, node.parent) if node.parent is not None else None,
        "kind": node.type,
        "name": name,
        "span": span_dict(_span_spec(node)),
        "attrs": attrs_map({}),
    }


def _call_row(
    node: Node,
    *,
    file_ctx: FileContext,
    callee: Node,
    data: bytes,
    options: TreeSitterExtractOptions,
) -> Row:
    callee_text, _ = _node_text(
        callee,
        data=data,
        max_bytes=options.max_text_bytes,
        allow_non_leaf=True,
    )
    return {
        "node_id": _node_id(file_ctx, node),
        "parent_id": _node_id(file_ctx, node.parent) if node.parent is not None else None,
        "callee_kind": callee.type,
        "callee_text": callee_text,
        "callee_node_id": _node_id(file_ctx, callee),
        "span": span_dict(_span_spec(node)),
        "attrs": attrs_map({}),
    }


def _import_row(
    node: Node,
    *,
    file_ctx: FileContext,
    kind: str,
    module: str | None,
    name: str | None,
    asname: str | None,
    alias_index: int | None,
    level: int | None,
) -> Row:
    return {
        "node_id": _node_id(file_ctx, node),
        "parent_id": _node_id(file_ctx, node.parent) if node.parent is not None else None,
        "kind": kind,
        "module": module,
        "name": name,
        "asname": asname,
        "alias_index": alias_index,
        "level": level,
        "span": span_dict(_span_spec(node)),
        "attrs": attrs_map({}),
    }


def _docstring_value(text: str) -> str | None:
    try:
        value = ast.literal_eval(text)
    except (SyntaxError, ValueError):
        return None
    return value if isinstance(value, str) else None


def _docstring_row(
    *,
    owner: Node,
    doc_node: Node,
    file_ctx: FileContext,
    data: bytes,
    options: TreeSitterExtractOptions,
) -> Row:
    raw_text, truncated = _node_text(
        doc_node,
        data=data,
        max_bytes=options.max_docstring_bytes,
        allow_non_leaf=True,
    )
    docstring = _docstring_value(raw_text) if raw_text is not None else None
    owner_name = None
    name_node = owner.child_by_field_name("name")
    if name_node is not None:
        owner_name, _ = _node_text(name_node, data=data, max_bytes=options.max_text_bytes)
    attrs: dict[str, object] = {}
    if truncated:
        attrs["text_truncated"] = True
    return {
        "owner_node_id": _node_id(file_ctx, owner),
        "owner_kind": owner.type,
        "owner_name": owner_name,
        "doc_node_id": _node_id(file_ctx, doc_node),
        "docstring": docstring,
        "source": raw_text,
        "span": span_dict(_span_spec(doc_node)),
        "attrs": attrs_map(attrs),
    }


def _iter_nodes(root: Node) -> Iterator[tuple[Node, Node | None, str | None, int | None]]:
    cursor = root.walk()
    child_indices: list[int] = []
    while True:
        node = cursor.node
        parent = node.parent
        child_index = child_indices[-1] if child_indices else None
        yield node, parent, cursor.field_name, child_index
        if cursor.goto_first_child():
            child_indices.append(0)
            continue
        if cursor.goto_next_sibling():
            if child_indices:
                child_indices[-1] += 1
            continue
        while True:
            if not cursor.goto_parent():
                return
            if child_indices:
                child_indices.pop()
            if cursor.goto_next_sibling():
                if child_indices:
                    child_indices[-1] += 1
                break


def _query_capture_nodes(value: object) -> list[Node]:
    if isinstance(value, list):
        return [node for node in value if isinstance(node, Node)]
    if isinstance(value, Node):
        return [value]
    return []


def _first_capture_node(value: object) -> Node | None:
    nodes = _query_capture_nodes(value)
    return nodes[0] if nodes else None


def _match_key(pattern_index: int, captures: Mapping[str, object]) -> tuple[int, tuple[int, ...]]:
    ids: list[int] = []
    for name in sorted(captures):
        for node in _query_capture_nodes(captures[name]):
            ids.append(int(node.id))
    return pattern_index, tuple(ids)


def _iter_query_matches(
    cursor: QueryCursor,
    *,
    root: Node,
    ranges: Sequence[Range],
) -> Iterator[tuple[int, Mapping[str, object]]]:
    if not ranges:
        yield from cursor.matches(root)
        return
    seen: set[tuple[int, tuple[int, ...]]] = set()
    for span in ranges:
        cursor.set_byte_range(span.start_byte, span.end_byte)
        for pattern_index, captures in cursor.matches(root):
            key = _match_key(pattern_index, captures)
            if key in seen:
                continue
            seen.add(key)
            yield pattern_index, captures


def _collect_queries(
    *,
    root: Node,
    data: bytes,
    file_ctx: FileContext,
    options: TreeSitterExtractOptions,
    query_pack: TreeSitterQueryPack,
    ranges: Sequence[Range],
) -> _QueryRows:
    captures: list[Row] = []
    defs: list[Row] = []
    calls: list[Row] = []
    imports: list[Row] = []
    docstrings: list[Row] = []
    stats = _QueryStats()
    import_alias_index: dict[str, int] = {}
    for query_name, query in query_pack.queries.items():
        cursor = QueryCursor(
            query,
            match_limit=options.query_match_limit,
            timeout_micros=options.query_timeout_micros,
        )
        for pattern_index, capture_map in _iter_query_matches(
            cursor,
            root=root,
            ranges=ranges,
        ):
            stats.match_count += 1
            if options.include_captures:
                for capture_name, capture_value in capture_map.items():
                    for node in _query_capture_nodes(capture_value):
                        captures.append(
                            _capture_entry(
                                node,
                                file_ctx=file_ctx,
                                data=data,
                                options=options,
                                query_name=query_name,
                                capture_name=capture_name,
                                pattern_index=pattern_index,
                            )
                        )
                        stats.capture_count += 1
            if query_name == "defs" and options.include_defs:
                def_node = _first_capture_node(capture_map.get("def.node"))
                name_node = _first_capture_node(capture_map.get("def.name"))
                if def_node is not None:
                    name_text = None
                    if name_node is not None:
                        name_text, _ = _node_text(
                            name_node,
                            data=data,
                            max_bytes=options.max_text_bytes,
                        )
                    defs.append(_def_row(def_node, file_ctx=file_ctx, name=name_text))
            if query_name == "calls" and options.include_calls:
                call_node = _first_capture_node(capture_map.get("call.node"))
                callee_node = _first_capture_node(
                    capture_map.get("call.name") or capture_map.get("call.attr")
                )
                if call_node is not None and callee_node is not None:
                    calls.append(
                        _call_row(
                            call_node,
                            file_ctx=file_ctx,
                            callee=callee_node,
                            data=data,
                            options=options,
                        )
                    )
            if query_name == "imports" and options.include_imports:
                import_node = _first_capture_node(capture_map.get("import.node"))
                if import_node is None:
                    continue
                module_node = _first_capture_node(capture_map.get("import.module"))
                from_node = _first_capture_node(capture_map.get("import.from"))
                relative_node = _first_capture_node(capture_map.get("import.relative"))
                name_node = _first_capture_node(capture_map.get("import.name"))
                alias_node = _first_capture_node(capture_map.get("import.alias"))
                module = None
                level = None
                kind = "Import"
                if from_node is not None:
                    kind = "ImportFrom"
                    module, _ = _node_text(
                        from_node,
                        data=data,
                        max_bytes=options.max_text_bytes,
                        allow_non_leaf=True,
                    )
                    level = 0
                if relative_node is not None:
                    kind = "ImportFrom"
                    rel_text, _ = _node_text(
                        relative_node,
                        data=data,
                        max_bytes=options.max_text_bytes,
                        allow_non_leaf=True,
                    )
                    if rel_text is not None:
                        dot_count = len(rel_text) - len(rel_text.lstrip("."))
                        level = dot_count
                        module = rel_text[dot_count:] or None
                if module_node is not None and kind == "Import":
                    module, _ = _node_text(
                        module_node,
                        data=data,
                        max_bytes=options.max_text_bytes,
                        allow_non_leaf=True,
                    )
                name = None
                asname = None
                if name_node is not None:
                    name, _ = _node_text(
                        name_node,
                        data=data,
                        max_bytes=options.max_text_bytes,
                        allow_non_leaf=True,
                    )
                if alias_node is not None:
                    asname, _ = _node_text(
                        alias_node,
                        data=data,
                        max_bytes=options.max_text_bytes,
                        allow_non_leaf=True,
                    )
                alias_key = _node_id(file_ctx, import_node)
                alias_index = import_alias_index.get(alias_key, 0)
                import_alias_index[alias_key] = alias_index + 1
                if kind == "Import":
                    imports.append(
                        _import_row(
                            import_node,
                            file_ctx=file_ctx,
                            kind=kind,
                            module=None,
                            name=module or name,
                            asname=asname,
                            alias_index=alias_index,
                            level=None,
                        )
                    )
                else:
                    imports.append(
                        _import_row(
                            import_node,
                            file_ctx=file_ctx,
                            kind=kind,
                            module=module,
                            name=name,
                            asname=asname,
                            alias_index=alias_index,
                            level=level,
                        )
                    )
            if query_name == "docstrings" and options.include_docstrings:
                owner_node = _first_capture_node(capture_map.get("doc.owner"))
                doc_node = _first_capture_node(capture_map.get("doc.string"))
                if owner_node is not None and doc_node is not None:
                    docstrings.append(
                        _docstring_row(
                            owner=owner_node,
                            doc_node=doc_node,
                            file_ctx=file_ctx,
                            data=data,
                            options=options,
                        )
                    )
        if cursor.did_exceed_match_limit:
            stats.match_limit_exceeded = True
    return _QueryRows(
        captures=captures,
        defs=defs,
        calls=calls,
        imports=imports,
        docstrings=docstrings,
        stats=stats,
    )


def extract_ts(
    repo_files: TableLike,
    *,
    options: TreeSitterExtractOptions | None = None,
    context: ExtractExecutionContext | None = None,
) -> TreeSitterExtractResult:
    """Extract tree-sitter nodes and diagnostics from repo files.

    Parameters
    ----------
    repo_files:
        Repo files table with bytes/text.
    options:
        Extraction options.
    context:
        Shared execution context bundle for extraction.

    Returns
    -------
    TreeSitterExtractResult
        Extracted tree-sitter file table.
    """
    normalized_options = normalize_options("tree_sitter", options, TreeSitterExtractOptions)
    exec_context = context or ExtractExecutionContext()
    exec_ctx = exec_context.ensure_ctx()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    plans = extract_ts_plans(
        repo_files,
        options=normalized_options,
        context=exec_context,
    )
    return TreeSitterExtractResult(
        tree_sitter_files=cast(
            "TableLike",
            materialize_extract_plan(
                "tree_sitter_files_v1",
                plans["tree_sitter_files"],
                ctx=exec_ctx,
                options=ExtractMaterializeOptions(
                    normalize=normalize,
                    apply_post_kernels=True,
                ),
            ),
        ),
    )


def extract_ts_plans(
    repo_files: TableLike,
    *,
    options: TreeSitterExtractOptions | None = None,
    context: ExtractExecutionContext | None = None,
) -> dict[str, IbisPlan]:
    """Extract tree-sitter plans for nested file records.

    Returns
    -------
    dict[str, IbisPlan]
        Ibis plan bundle keyed by ``tree_sitter_files``.
    """
    normalized_options = normalize_options("tree_sitter", options, TreeSitterExtractOptions)
    exec_context = context or ExtractExecutionContext()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    rows = _collect_ts_rows(
        repo_files,
        options=normalized_options,
        file_contexts=exec_context.file_contexts,
    )
    evidence_plan = exec_context.evidence_plan
    return {
        "tree_sitter_files": _build_ts_plan(
            "tree_sitter_files_v1",
            rows,
            normalize=normalize,
            evidence_plan=evidence_plan,
        ),
    }


def _collect_ts_rows(
    repo_files: TableLike,
    *,
    options: TreeSitterExtractOptions,
    file_contexts: Iterable[FileContext] | None,
) -> list[Row]:
    parser = _parser(options)
    cache = _default_cache(options.incremental_cache_size) if options.incremental else None
    query_pack = _query_pack() if _should_run_queries(options) else None
    rows: list[Row] = []
    for file_ctx in iter_contexts(repo_files, file_contexts):
        row = _extract_ts_file_row(
            file_ctx,
            parser=parser,
            cache=cache,
            options=options,
            query_pack=query_pack,
        )
        if row is not None:
            rows.append(row)
    return rows


def _should_run_queries(options: TreeSitterExtractOptions) -> bool:
    return (
        options.include_captures
        or options.include_defs
        or options.include_calls
        or options.include_imports
        or options.include_docstrings
    )


def _parse_tree(
    *,
    parser: Parser,
    data: bytes,
    options: TreeSitterExtractOptions,
    cache: TreeSitterCache | None,
    cache_key: str,
) -> tuple[TreeSitterParseResult, _ParseStats]:
    start = time.monotonic()
    if options.incremental and cache is not None:
        parse_result = cache.parse(parser=parser, key=cache_key, source=data)
        tree = parse_result.tree
        used_incremental = parse_result.used_incremental
    else:
        tree = parser.parse(data)
        used_incremental = False
        parse_result = TreeSitterParseResult(
            tree=tree,
            changed_ranges=(),
            used_incremental=False,
        )
    parse_ms = int((time.monotonic() - start) * 1000)
    parse_timed_out = tree is None
    if parse_timed_out:
        parser.reset()
    return parse_result, _ParseStats(
        parse_ms=parse_ms,
        parse_timed_out=parse_timed_out,
        used_incremental=used_incremental,
    )


def _extract_ts_file_row(
    file_ctx: FileContext,
    *,
    parser: Parser,
    cache: TreeSitterCache | None,
    options: TreeSitterExtractOptions,
    query_pack: TreeSitterQueryPack | None,
) -> Row | None:
    if not file_ctx.file_id or not file_ctx.path:
        return None
    if not _should_parse(file_ctx, options):
        return None
    data = bytes_from_file_ctx(file_ctx)
    if data is None:
        return None
    cache_key = file_ctx.file_id or file_ctx.path
    parse_result, parse_stats = _parse_tree(
        parser=parser,
        data=data,
        options=options,
        cache=cache,
        cache_key=cache_key,
    )
    tree = parse_result.tree if parse_result is not None else None
    if tree is None:
        attrs = _file_attrs(
            file_ctx=file_ctx,
            query_pack=query_pack,
            parse_stats=parse_stats,
            node_stats=_NodeStats(),
            query_stats=None,
        )
        return {
            "repo": options.repo_id,
            "path": file_ctx.path,
            "file_id": file_ctx.file_id,
            "nodes": [],
            "edges": [],
            "errors": [],
            "missing": [],
            "captures": [],
            "defs": [],
            "calls": [],
            "imports": [],
            "docstrings": [],
            "stats": _stats_row(
                node_stats=_NodeStats(),
                parse_stats=parse_stats,
                query_stats=None,
            )
            if options.include_stats
            else None,
            "attrs": attrs_map(attrs),
        }
    root = tree.root_node
    node_rows: list[Row] = []
    edge_rows: list[Row] = []
    error_rows: list[Row] = []
    missing_rows: list[Row] = []
    node_stats = _NodeStats()
    for node, parent, field_name, child_index in _iter_nodes(root):
        node_stats.node_count += 1
        if node.is_named:
            node_stats.named_count += 1
        if node.is_error:
            node_stats.error_count += 1
        if node.is_missing:
            node_stats.missing_count += 1
        if options.include_nodes:
            node_rows.append(
                _node_entry(
                    node,
                    file_ctx=file_ctx,
                    data=data,
                    options=options,
                    parent=parent,
                )
            )
        if options.include_edges and parent is not None:
            edge_rows.append(
                _edge_entry(
                    file_ctx=file_ctx,
                    parent=parent,
                    child=node,
                    field_name=field_name,
                    child_index=child_index,
                )
            )
        if options.include_errors and node.is_error:
            error_rows.append(_error_entry(node, file_ctx=file_ctx))
        if options.include_missing and node.is_missing:
            missing_rows.append(_missing_entry(node, file_ctx=file_ctx))
    query_rows = None
    query_ranges = parse_result.changed_ranges if options.incremental else ()
    if query_pack is not None:
        query_rows = _collect_queries(
            root=root,
            data=data,
            file_ctx=file_ctx,
            options=options,
            query_pack=query_pack,
            ranges=query_ranges,
        )
    attrs = _file_attrs(
        file_ctx=file_ctx,
        query_pack=query_pack,
        parse_stats=parse_stats,
        node_stats=node_stats,
        query_stats=query_rows.stats if query_rows is not None else None,
    )
    return {
        "repo": options.repo_id,
        "path": file_ctx.path,
        "file_id": file_ctx.file_id,
        "nodes": node_rows if options.include_nodes else [],
        "edges": edge_rows if options.include_edges else [],
        "errors": error_rows if options.include_errors else [],
        "missing": missing_rows if options.include_missing else [],
        "captures": query_rows.captures if query_rows and options.include_captures else [],
        "defs": query_rows.defs if query_rows and options.include_defs else [],
        "calls": query_rows.calls if query_rows and options.include_calls else [],
        "imports": query_rows.imports if query_rows and options.include_imports else [],
        "docstrings": query_rows.docstrings if query_rows and options.include_docstrings else [],
        "stats": _stats_row(
            node_stats=node_stats,
            parse_stats=parse_stats,
            query_stats=query_rows.stats if query_rows is not None else None,
        )
        if options.include_stats
        else None,
        "attrs": attrs_map(attrs),
    }


def _file_attrs(
    *,
    file_ctx: FileContext,
    query_pack: TreeSitterQueryPack | None,
    parse_stats: _ParseStats,
    node_stats: _NodeStats,
    query_stats: _QueryStats | None,
) -> dict[str, object]:
    attrs: dict[str, object] = {
        "file_sha256": file_ctx.file_sha256,
        "language_name": PY_LANGUAGE.name,
        "language_abi_version": PY_LANGUAGE.abi_version,
        "language_semantic_version": _semantic_version(PY_LANGUAGE.semantic_version),
        "parse_ms": parse_stats.parse_ms,
        "parse_timed_out": parse_stats.parse_timed_out,
        "node_count": node_stats.node_count,
        "named_count": node_stats.named_count,
        "error_count": node_stats.error_count,
        "missing_count": node_stats.missing_count,
        "incremental_used": parse_stats.used_incremental,
    }
    if query_pack is not None:
        attrs.update(query_pack.metadata())
    if query_stats is not None:
        attrs["query_match_count"] = query_stats.match_count
        attrs["query_capture_count"] = query_stats.capture_count
        attrs["query_match_limit_exceeded"] = query_stats.match_limit_exceeded
    return attrs


def _semantic_version(version: object) -> str | None:
    if isinstance(version, tuple) and len(version) == 3:
        major, minor, patch = version
        if all(isinstance(part, int) for part in (major, minor, patch)):
            return f"{major}.{minor}.{patch}"
    return None


def _stats_row(
    *,
    node_stats: _NodeStats,
    parse_stats: _ParseStats,
    query_stats: _QueryStats | None,
) -> Row:
    return {
        "node_count": node_stats.node_count,
        "named_count": node_stats.named_count,
        "error_count": node_stats.error_count,
        "missing_count": node_stats.missing_count,
        "parse_ms": parse_stats.parse_ms,
        "parse_timed_out": parse_stats.parse_timed_out,
        "incremental_used": parse_stats.used_incremental,
        "query_match_count": query_stats.match_count if query_stats else None,
        "query_capture_count": query_stats.capture_count if query_stats else None,
        "match_limit_exceeded": query_stats.match_limit_exceeded if query_stats else None,
    }


def _build_ts_plan(
    name: str,
    rows: list[Row],
    *,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None,
) -> IbisPlan:
    raw = ibis_plan_from_rows(name, rows)
    return apply_query_and_project(
        name,
        raw.expr,
        normalize=normalize,
        evidence_plan=evidence_plan,
        repo_id=normalize.repo_id,
    )


class _TreeSitterTablesKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: TreeSitterExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
    profile: str
    context: ExtractExecutionContext | None
    prefer_reader: bool


class _TreeSitterTablesKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: TreeSitterExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
    profile: str
    context: ExtractExecutionContext | None
    prefer_reader: Literal[False]


class _TreeSitterTablesKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: TreeSitterExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
    profile: str
    context: ExtractExecutionContext | None
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
        Keyword-only arguments for extraction (repo_files, options, context, file_contexts,
        evidence_plan, ctx, profile, prefer_reader).

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Extracted tree-sitter outputs keyed by output name.
    """
    repo_files = kwargs["repo_files"]
    normalized_options = normalize_options(
        "tree_sitter",
        kwargs.get("options"),
        TreeSitterExtractOptions,
    )
    context = kwargs.get("context")
    if context is None:
        context = ExtractExecutionContext(
            file_contexts=kwargs.get("file_contexts"),
            evidence_plan=kwargs.get("evidence_plan"),
            ctx=kwargs.get("ctx"),
            profile=kwargs.get("profile", "default"),
        )
    exec_ctx = context.ensure_ctx()
    prefer_reader = kwargs.get("prefer_reader", False)
    normalize = ExtractNormalizeOptions(options=normalized_options)
    plans = extract_ts_plans(
        repo_files,
        options=normalized_options,
        context=context,
    )
    return {
        "tree_sitter_files": materialize_extract_plan(
            "tree_sitter_files_v1",
            plans["tree_sitter_files"],
            ctx=exec_ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=True,
            ),
        ),
    }
