"""Extract tree-sitter nodes and diagnostics into Arrow tables using shared helpers."""

from __future__ import annotations

import ast
import contextlib
import mmap
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path

import tree_sitter_python
from tree_sitter import (
    LANGUAGE_VERSION,
    MIN_COMPATIBLE_LANGUAGE_VERSION,
    Language,
    Node,
    Parser,
    Point,
    QueryCursor,
    Range,
    TreeCursor,
)

from core_types import RowPermissive as Row
from datafusion_engine.hashing import span_id
from extract.coordination.context import (
    FileContext,
    SpanSpec,
    attrs_map,
    span_dict,
)
from extract.coordination.line_offsets import LineOffsets
from extract.extractors.tree_sitter.cache import TreeSitterCache
from extract.extractors.tree_sitter.queries import TreeSitterQueryPack, compile_query_pack
from extract.extractors.tree_sitter.setup import TreeSitterExtractOptions
from extract.extractors.tree_sitter.visitors import (
    _CaptureInfo,
    _ImportInfo,
    _QueryRows,
    _QueryStats,
)

type SourceBuffer = bytes | bytearray | memoryview

PY_LANGUAGE = Language(tree_sitter_python.language())
SEMVER_PARTS = 3


@dataclass
class _QueryCollector:
    file_ctx: FileContext
    data: SourceBuffer
    options: TreeSitterExtractOptions
    captures: list[Row] = field(default_factory=list)
    defs: list[Row] = field(default_factory=list)
    calls: list[Row] = field(default_factory=list)
    imports: list[Row] = field(default_factory=list)
    docstrings: list[Row] = field(default_factory=list)
    stats: _QueryStats = field(default_factory=_QueryStats)
    import_alias_index: dict[str, int] = field(default_factory=dict)

    def record_match(
        self,
        *,
        query_name: str,
        pattern_index: int,
        capture_map: Mapping[str, object],
    ) -> None:
        self.stats.match_count += 1
        if self.options.include_captures:
            self._record_captures(query_name, pattern_index, capture_map)
        if query_name == "defs" and self.options.include_defs:
            self._record_defs(capture_map)
        if query_name == "calls" and self.options.include_calls:
            self._record_calls(capture_map)
        if query_name == "imports" and self.options.include_imports:
            self._record_imports(capture_map)
        if query_name == "docstrings" and self.options.include_docstrings:
            self._record_docstrings(capture_map)

    def _record_captures(
        self,
        query_name: str,
        pattern_index: int,
        capture_map: Mapping[str, object],
    ) -> None:
        for capture_name, capture_value in capture_map.items():
            for node in _query_capture_nodes(capture_value):
                self.captures.append(
                    _capture_entry(
                        node,
                        file_ctx=self.file_ctx,
                        data=self.data,
                        options=self.options,
                        info=_CaptureInfo(
                            query_name=query_name,
                            capture_name=capture_name,
                            pattern_index=pattern_index,
                        ),
                    )
                )
                self.stats.capture_count += 1

    def _record_defs(self, capture_map: Mapping[str, object]) -> None:
        def_node = _first_capture_node(capture_map.get("def.node"))
        name_node = _first_capture_node(capture_map.get("def.name"))
        if def_node is None:
            return
        name_text = None
        if name_node is not None:
            name_text = _node_text_value(name_node, data=self.data, options=self.options)
        self.defs.append(_def_row(def_node, file_ctx=self.file_ctx, name=name_text))

    def _record_calls(self, capture_map: Mapping[str, object]) -> None:
        call_node = _first_capture_node(capture_map.get("call.node"))
        callee_node = _first_capture_node(
            capture_map.get("call.name") or capture_map.get("call.attr")
        )
        if call_node is None or callee_node is None:
            return
        self.calls.append(
            _call_row(
                call_node,
                file_ctx=self.file_ctx,
                callee=callee_node,
                data=self.data,
                options=self.options,
            )
        )

    def _record_imports(self, capture_map: Mapping[str, object]) -> None:
        import_node = _first_capture_node(capture_map.get("import.node"))
        if import_node is None:
            return
        module_node = _first_capture_node(capture_map.get("import.module"))
        from_node = _first_capture_node(capture_map.get("import.from"))
        relative_node = _first_capture_node(capture_map.get("import.relative"))
        name_node = _first_capture_node(capture_map.get("import.name"))
        alias_node = _first_capture_node(capture_map.get("import.alias"))
        kind, module, level = _resolve_import_module(
            module_node=module_node,
            from_node=from_node,
            relative_node=relative_node,
            data=self.data,
            options=self.options,
        )
        name = _node_text_value(name_node, data=self.data, options=self.options)
        asname = _node_text_value(alias_node, data=self.data, options=self.options)
        alias_key = _node_id(self.file_ctx, import_node)
        alias_index = self.import_alias_index.get(alias_key, 0)
        self.import_alias_index[alias_key] = alias_index + 1
        if kind == "Import":
            info = _ImportInfo(
                kind=kind,
                module=None,
                name=module or name,
                asname=asname,
                alias_index=alias_index,
                level=None,
            )
        else:
            info = _ImportInfo(
                kind=kind,
                module=module,
                name=name,
                asname=asname,
                alias_index=alias_index,
                level=level,
            )
        self.imports.append(_import_row(import_node, file_ctx=self.file_ctx, info=info))

    def _record_docstrings(self, capture_map: Mapping[str, object]) -> None:
        owner_node = _first_capture_node(capture_map.get("doc.owner"))
        doc_node = _first_capture_node(capture_map.get("doc.string"))
        if owner_node is None or doc_node is None:
            return
        self.docstrings.append(
            _docstring_row(
                owner=owner_node,
                doc_node=doc_node,
                file_ctx=self.file_ctx,
                data=self.data,
                options=self.options,
            )
        )

    def build(self) -> _QueryRows:
        return _QueryRows(
            captures=self.captures,
            defs=self.defs,
            calls=self.calls,
            imports=self.imports,
            docstrings=self.docstrings,
            stats=self.stats,
        )


def _assert_language_abi(lang: Language) -> None:
    if not (MIN_COMPATIBLE_LANGUAGE_VERSION <= lang.abi_version <= LANGUAGE_VERSION):
        msg = f"Tree-sitter ABI mismatch: {lang.abi_version}"
        raise ValueError(msg)


def _parser(options: TreeSitterExtractOptions) -> Parser:
    _assert_language_abi(PY_LANGUAGE)
    parser = Parser(PY_LANGUAGE)
    if options.parser_timeout_micros is not None:
        set_timeout = getattr(parser, "set_timeout_micros", None)
        if callable(set_timeout):
            set_timeout(int(options.parser_timeout_micros))
    return parser


@cache
def _default_cache(max_entries: int) -> TreeSitterCache:
    return TreeSitterCache(max_entries=max_entries)


@cache
def _query_pack() -> TreeSitterQueryPack:
    return compile_query_pack(PY_LANGUAGE)


@dataclass(frozen=True)
class _TsWorkerState:
    parser: Parser
    cache: TreeSitterCache | None
    query_pack: TreeSitterQueryPack | None


@cache
def _worker_state(options: TreeSitterExtractOptions) -> _TsWorkerState:
    parser = _parser(options)
    cache = _default_cache(options.incremental_cache_size) if options.incremental else None
    query_pack = _query_pack() if _should_run_queries(options) else None
    return _TsWorkerState(parser=parser, cache=cache, query_pack=query_pack)


def _should_parse(file_ctx: FileContext, options: TreeSitterExtractOptions) -> bool:
    if not file_ctx.path:
        return False
    if options.extensions is None:
        return True
    path = file_ctx.path.lower()
    return any(path.endswith(ext) for ext in options.extensions)


@contextlib.contextmanager
def _source_buffer(
    file_ctx: FileContext,
    options: TreeSitterExtractOptions,
) -> Iterator[tuple[SourceBuffer | None, bool]]:
    data = file_ctx.data
    if data is not None:
        yield data, False
        return
    if file_ctx.text is not None:
        encoding = file_ctx.encoding or "utf-8"
        yield file_ctx.text.encode(encoding, errors="replace"), False
        return
    if not file_ctx.abs_path:
        yield None, False
        return
    path = Path(file_ctx.abs_path)
    try:
        size_bytes = path.stat().st_size
    except OSError:
        yield None, False
        return
    threshold = options.parse_callback_threshold_bytes
    use_callback = False
    if _resolve_parse_callback_options(options):
        if threshold is None:
            msg = "parse_callback_threshold_bytes must be configured for parse callbacks."
            raise ValueError(msg)
        use_callback = size_bytes >= threshold
    if use_callback:
        with path.open("rb") as handle:
            mm = mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ)
            view = memoryview(mm)
            try:
                yield view, True
            finally:
                view.release()
                mm.close()
        return
    try:
        yield path.read_bytes(), False
    except OSError:
        yield None, False


def _parse_callback(
    data: SourceBuffer,
    *,
    chunk_size: int,
) -> Callable[[int, Point], bytes]:
    def _callback(byte_offset: int, _point: Point) -> bytes:
        start = max(0, int(byte_offset))
        if start >= len(data):
            return b""
        end = min(len(data), start + chunk_size)
        return bytes(data[start:end])

    return _callback


def _maybe_int(value: object) -> int | None:
    return value if isinstance(value, int) else None


def _point_parts(point: object | None) -> tuple[int | None, int | None]:
    if point is None:
        return None, None
    return _maybe_int(getattr(point, "row", None)), _maybe_int(getattr(point, "column", None))


def _included_ranges(
    data: SourceBuffer,
    options: TreeSitterExtractOptions,
    *,
    offsets: LineOffsets,
) -> tuple[Range, ...] | None:
    ranges = options.included_ranges
    if not ranges:
        return None
    resolved: list[Range] = []
    for start, end in ranges:
        start_byte = min(len(data), max(0, int(start)))
        end_byte = min(len(data), max(start_byte, int(end)))
        if end_byte == start_byte:
            continue
        start_line0, start_col = offsets.point_from_byte(start_byte)
        end_line0, end_col = offsets.point_from_byte(end_byte)
        resolved.append(
            Range(
                Point(start_line0, start_col),
                Point(end_line0, end_col),
                start_byte,
                end_byte,
            )
        )
    return tuple(resolved) if resolved else None


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
    data: SourceBuffer,
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
    text_bytes = bytes(data[start:end])
    truncated = False
    if len(text_bytes) > max_bytes:
        text_bytes = text_bytes[:max_bytes]
        truncated = True
    return text_bytes.decode("utf-8", errors="replace"), truncated


def _node_text_value(
    node: Node | None,
    *,
    data: SourceBuffer,
    options: TreeSitterExtractOptions,
    allow_non_leaf: bool = True,
) -> str | None:
    if node is None:
        return None
    text, _ = _node_text(
        node,
        data=data,
        max_bytes=options.max_text_bytes,
        allow_non_leaf=allow_non_leaf,
    )
    return text


def _resolve_import_module(
    *,
    module_node: Node | None,
    from_node: Node | None,
    relative_node: Node | None,
    data: SourceBuffer,
    options: TreeSitterExtractOptions,
) -> tuple[str, str | None, int | None]:
    kind = "Import"
    module = None
    level = None
    if from_node is not None:
        kind = "ImportFrom"
        module = _node_text_value(
            from_node,
            data=data,
            options=options,
            allow_non_leaf=True,
        )
        level = 0
    if relative_node is not None:
        kind = "ImportFrom"
        rel_text = _node_text_value(
            relative_node,
            data=data,
            options=options,
            allow_non_leaf=True,
        )
        if rel_text is not None:
            dot_count = len(rel_text) - len(rel_text.lstrip("."))
            level = dot_count
            module = rel_text[dot_count:] or None
    if module_node is not None and kind == "Import":
        module = _node_text_value(
            module_node,
            data=data,
            options=options,
            allow_non_leaf=True,
        )
    return kind, module, level


def _node_entry(
    node: Node,
    *,
    file_ctx: FileContext,
    data: SourceBuffer,
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
    data: SourceBuffer,
    options: TreeSitterExtractOptions,
    info: _CaptureInfo,
) -> Row:
    start = int(node.start_byte)
    end = int(node.end_byte)
    capture_id = span_id(
        file_ctx.path,
        start,
        end,
        kind=f"ts_capture:{info.query_name}:{info.capture_name}",
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
        "query_name": info.query_name,
        "capture_name": info.capture_name,
        "pattern_index": info.pattern_index,
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
    data: SourceBuffer,
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
    info: _ImportInfo,
) -> Row:
    return {
        "node_id": _node_id(file_ctx, node),
        "parent_id": _node_id(file_ctx, node.parent) if node.parent is not None else None,
        "kind": info.kind,
        "module": info.module,
        "name": info.name,
        "asname": info.asname,
        "alias_index": info.alias_index,
        "level": info.level,
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
    data: SourceBuffer,
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
        if node is None:
            return
        parent = node.parent
        child_index = child_indices[-1] if child_indices else None
        yield node, parent, cursor.field_name, child_index
        if not _advance_cursor(cursor, child_indices):
            return


def _advance_cursor(cursor: TreeCursor, child_indices: list[int]) -> bool:
    if cursor.goto_first_child():
        child_indices.append(0)
        return True
    if cursor.goto_next_sibling():
        if child_indices:
            child_indices[-1] += 1
        return True
    return _ascend_to_next_sibling(cursor, child_indices)


def _ascend_to_next_sibling(cursor: TreeCursor, child_indices: list[int]) -> bool:
    while True:
        if not cursor.goto_parent():
            return False
        if child_indices:
            child_indices.pop()
        if cursor.goto_next_sibling():
            if child_indices:
                child_indices[-1] += 1
            return True


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
        ids.extend(int(node.id) for node in _query_capture_nodes(captures[name]))
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


from extract.extractors.tree_sitter.builders_runtime import (
    _resolve_parse_callback_options,
    _should_run_queries,
    extract_ts,
    extract_ts_plans,
    extract_ts_tables,
)

__all__ = [
    "extract_ts",
    "extract_ts_plans",
    "extract_ts_tables",
]
