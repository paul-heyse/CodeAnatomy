"""Extract tree-sitter nodes and diagnostics into Arrow tables using shared helpers."""

from __future__ import annotations

import ast
import contextlib
import mmap
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from functools import cache, partial
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, cast, overload

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
from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.extract.registry import normalize_options
from datafusion_engine.hashing import span_id
from datafusion_engine.plan.bundle import DataFusionPlanBundle
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from extract.coordination.line_offsets import LineOffsets
from extract.coordination.schema_ops import ExtractNormalizeOptions
from extract.extractors.tree_sitter.cache import TreeSitterCache, TreeSitterParseResult
from extract.extractors.tree_sitter.queries import TreeSitterQueryPack, compile_query_pack
from extract.helpers import (
    ExtractExecutionContext,
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    FileContext,
    SpanSpec,
    attrs_map,
    extract_plan_from_row_batches,
    extract_plan_from_rows,
    materialize_extract_plan,
    span_dict,
)
from extract.infrastructure.options import ParallelOptions, RepoOptions, WorklistQueueOptions
from extract.infrastructure.parallel import parallel_map, resolve_max_workers
from extract.infrastructure.result_types import ExtractResult
from extract.infrastructure.worklists import (
    WorklistRequest,
    iter_worklist_contexts,
    worklist_queue_name,
)
from obs.otel.scopes import SCOPE_EXTRACT
from obs.otel.tracing import stage_span

if TYPE_CHECKING:
    from extract.coordination.evidence_plan import EvidencePlan
    from extract.scanning.scope_manifest import ScopeManifest
    from extract.session import ExtractSession

type SourceBuffer = bytes | bytearray | memoryview

PY_LANGUAGE = Language(tree_sitter_python.language())
SEMVER_PARTS = 3


@dataclass(frozen=True)
class TreeSitterExtractOptions(RepoOptions, WorklistQueueOptions, ParallelOptions):
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
    extensions: tuple[str, ...] | None = None
    parser_timeout_micros: int | None = None
    query_match_limit: int = 10_000
    query_timeout_micros: int | None = None
    max_text_bytes: int = 256
    max_docstring_bytes: int = 2048
    incremental: bool = False
    incremental_cache_size: int = 256
    included_ranges: tuple[tuple[int, int], ...] | None = None
    parse_callback_threshold_bytes: int | None = 5_000_000
    parse_callback_chunk_size: int = 65_536


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


@dataclass(frozen=True)
class _CaptureInfo:
    query_name: str
    capture_name: str
    pattern_index: int


@dataclass(frozen=True)
class _ImportInfo:
    kind: str
    module: str | None
    name: str | None
    asname: str | None
    alias_index: int | None
    level: int | None


@dataclass(frozen=True)
class _ParseContext:
    parser: Parser
    data: SourceBuffer
    cache: TreeSitterCache | None
    cache_key: str
    use_callback: bool


@dataclass(frozen=True)
class _QueryContext:
    root: Node
    data: SourceBuffer
    file_ctx: FileContext
    options: TreeSitterExtractOptions
    query_pack: TreeSitterQueryPack
    ranges: Sequence[Range]


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


def _collect_queries(context: _QueryContext) -> _QueryRows:
    collector = _QueryCollector(
        file_ctx=context.file_ctx,
        data=context.data,
        options=context.options,
    )
    for query_name, query in context.query_pack.queries.items():
        cursor = QueryCursor(
            query,
            match_limit=context.options.query_match_limit,
        )
        if context.options.query_timeout_micros is not None:
            set_timeout = getattr(cursor, "set_timeout_micros", None)
            if callable(set_timeout):
                set_timeout(int(context.options.query_timeout_micros))
        for pattern_index, capture_map in _iter_query_matches(
            cursor,
            root=context.root,
            ranges=context.ranges,
        ):
            collector.record_match(
                query_name=query_name,
                pattern_index=pattern_index,
                capture_map=capture_map,
            )
        if cursor.did_exceed_match_limit:
            collector.stats.match_limit_exceeded = True
    return collector.build()


def extract_ts(
    repo_files: TableLike,
    *,
    options: TreeSitterExtractOptions | None = None,
    context: ExtractExecutionContext | None = None,
) -> ExtractResult[TableLike]:
    """Extract tree-sitter nodes and diagnostics from repo files.

    Parameters
    ----------
    repo_files:
        Repo files table with bytes/text.
    options:
        Extraction options.
    context:
        Shared execution context bundle for extraction.

    Returns:
    -------
    ExtractResult[TableLike]
        Extracted tree-sitter file table.
    """
    normalized_options = normalize_options("tree_sitter", options, TreeSitterExtractOptions)
    normalized_options = _normalize_ts_options(normalized_options)
    normalized_options = _normalize_ts_options(normalized_options)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    determinism_tier = exec_context.determinism_tier()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    plans = extract_ts_plans(
        repo_files,
        options=normalized_options,
        context=exec_context,
    )
    table = cast(
        "TableLike",
        materialize_extract_plan(
            "tree_sitter_files_v1",
            plans["tree_sitter_files"],
            runtime_profile=runtime_profile,
            determinism_tier=determinism_tier,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
    )
    return ExtractResult(table=table, extractor_name="tree_sitter")


def extract_ts_plans(
    repo_files: TableLike,
    *,
    options: TreeSitterExtractOptions | None = None,
    context: ExtractExecutionContext | None = None,
) -> dict[str, DataFusionPlanBundle]:
    """Extract tree-sitter plans for nested file records.

    Returns:
    -------
    dict[str, DataFusionPlanBundle]
        Plan bundle keyed by ``tree_sitter_files``.
    """
    normalized_options = normalize_options("tree_sitter", options, TreeSitterExtractOptions)
    normalized_options = _normalize_ts_options(normalized_options)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    rows: list[Row] | None = None
    row_batches: Iterable[Sequence[Mapping[str, object]]] | None = None
    batch_size = _resolve_batch_size(normalized_options)
    request = _TsRowRequest(
        repo_files=repo_files,
        options=normalized_options,
        file_contexts=exec_context.file_contexts,
        scope_manifest=exec_context.scope_manifest,
        runtime_profile=runtime_profile,
    )
    if batch_size is None:
        rows = _collect_ts_rows(request)
    else:
        row_batches = _iter_ts_row_batches(request, batch_size=batch_size)
    evidence_plan = exec_context.evidence_plan
    plan_context = _TreeSitterPlanContext(
        normalize=normalize,
        evidence_plan=evidence_plan,
        session=session,
    )
    return {
        "tree_sitter_files": _build_ts_plan(
            "tree_sitter_files_v1",
            rows,
            row_batches=row_batches,
            plan_context=plan_context,
        ),
    }


@dataclass(frozen=True)
class _TsRowRequest:
    repo_files: TableLike
    options: TreeSitterExtractOptions
    file_contexts: Iterable[FileContext] | None
    scope_manifest: ScopeManifest | None
    runtime_profile: DataFusionRuntimeProfile | None


def _collect_ts_rows(request: _TsRowRequest) -> list[Row]:
    rows: list[Row] = []
    contexts = list(
        iter_worklist_contexts(
            WorklistRequest(
                repo_files=request.repo_files,
                output_table="tree_sitter_files_v1",
                runtime_profile=request.runtime_profile,
                file_contexts=request.file_contexts,
                queue_name=(
                    worklist_queue_name(
                        output_table="tree_sitter_files_v1",
                        repo_id=request.options.repo_id,
                    )
                    if request.options.use_worklist_queue
                    else None
                ),
                scope_manifest=request.scope_manifest,
            )
        )
    )
    if not contexts:
        return rows
    rows.extend(_iter_ts_rows_for_contexts(contexts, options=request.options))
    return rows


def _iter_ts_rows_for_contexts(
    contexts: Sequence[FileContext],
    *,
    options: TreeSitterExtractOptions,
) -> Iterator[Row]:
    if not options.parallel:
        for file_ctx in contexts:
            row = _ts_row_worker(file_ctx, options=options)
            if row is not None:
                yield row
        return
    runner = partial(_ts_row_worker, options=options)
    max_workers = resolve_max_workers(options.max_workers, kind="cpu")
    for row in parallel_map(contexts, runner, max_workers=max_workers):
        if row is not None:
            yield row


def _iter_ts_row_batches(
    request: _TsRowRequest,
    *,
    batch_size: int,
) -> Iterable[Sequence[Mapping[str, object]]]:
    batch: list[Row] = []
    contexts = list(
        iter_worklist_contexts(
            WorklistRequest(
                repo_files=request.repo_files,
                output_table="tree_sitter_files_v1",
                runtime_profile=request.runtime_profile,
                file_contexts=request.file_contexts,
                queue_name=(
                    worklist_queue_name(
                        output_table="tree_sitter_files_v1",
                        repo_id=request.options.repo_id,
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
    for row in _iter_ts_rows_for_contexts(contexts, options=request.options):
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _ts_row_worker(file_ctx: FileContext, *, options: TreeSitterExtractOptions) -> Row | None:
    state = _worker_state(options)
    return _extract_ts_file_row(
        file_ctx,
        parser=state.parser,
        cache=state.cache,
        options=options,
        query_pack=state.query_pack,
    )


def _resolve_batch_size(options: TreeSitterExtractOptions) -> int | None:
    if options.batch_size is None:
        return None
    if options.batch_size <= 0:
        msg = "batch_size must be a positive integer."
        raise ValueError(msg)
    return options.batch_size


def _normalize_ts_options(options: TreeSitterExtractOptions) -> TreeSitterExtractOptions:
    ranges = options.included_ranges
    if ranges is None:
        return options
    normalized = tuple((int(start), int(end)) for start, end in ranges)
    if normalized == ranges:
        return options
    return replace(options, included_ranges=normalized)


def _resolve_parse_callback_options(options: TreeSitterExtractOptions) -> bool:
    if options.parse_callback_threshold_bytes is None:
        return False
    if options.parse_callback_threshold_bytes <= 0:
        msg = "parse_callback_threshold_bytes must be a positive integer."
        raise ValueError(msg)
    if options.parse_callback_chunk_size <= 0:
        msg = "parse_callback_chunk_size must be a positive integer."
        raise ValueError(msg)
    return True


def _should_run_queries(options: TreeSitterExtractOptions) -> bool:
    return (
        options.include_captures
        or options.include_defs
        or options.include_calls
        or options.include_imports
        or options.include_docstrings
    )


def _parse_tree(
    context: _ParseContext,
    *,
    options: TreeSitterExtractOptions,
) -> tuple[TreeSitterParseResult, _ParseStats]:
    start = time.monotonic()
    data = context.data
    parser = context.parser
    cache = context.cache
    ranges = None
    if options.included_ranges:
        ranges = _included_ranges(
            data,
            options,
            offsets=LineOffsets.from_bytes(bytes(data)),
        )
    if ranges is not None:
        parser.included_ranges = list(ranges)
    try:
        if (
            cache is not None
            and options.incremental
            and ranges is None
            and not context.use_callback
            and isinstance(data, (bytes, bytearray))
        ):
            source = data if isinstance(data, bytes) else bytes(data)
            parse_result = cache.parse(
                parser=parser,
                key=context.cache_key,
                source=source,
            )
            tree = parse_result.tree
            used_incremental = parse_result.used_incremental
        else:
            if context.use_callback:
                callback = _parse_callback(data, chunk_size=options.parse_callback_chunk_size)
                tree = parser.parse(callback)
            else:
                tree = parser.parse(data)
            used_incremental = False
            parse_result = TreeSitterParseResult(
                tree=tree,
                changed_ranges=(),
                used_incremental=False,
            )
    finally:
        if ranges is not None:
            parser.included_ranges = []
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
    cache_key = file_ctx.file_id or file_ctx.path
    with _source_buffer(file_ctx, options) as (data, use_callback):
        if data is None:
            return None
        parse_result, parse_stats = _parse_tree(
            _ParseContext(
                parser=parser,
                data=data,
                cache=cache,
                cache_key=cache_key,
                use_callback=use_callback,
            ),
            options=options,
        )
        tree = parse_result.tree if parse_result is not None else None
        if tree is None:
            return _empty_ts_file_row(
                file_ctx=file_ctx,
                options=options,
                query_pack=query_pack,
                parse_stats=parse_stats,
            )
        root = tree.root_node
        node_rows, edge_rows, error_rows, missing_rows, node_stats = _collect_node_rows(
            root,
            file_ctx=file_ctx,
            data=data,
            options=options,
        )
        query_rows = None
        query_ranges = parse_result.changed_ranges if options.incremental else ()
        if query_pack is not None:
            query_rows = _collect_queries(
                _QueryContext(
                    root=root,
                    data=data,
                    file_ctx=file_ctx,
                    options=options,
                    query_pack=query_pack,
                    ranges=query_ranges,
                )
            )
    attrs = _file_attrs(
        _file_ctx=file_ctx,
        query_pack=query_pack,
        parse_stats=parse_stats,
        node_stats=node_stats,
        query_stats=query_rows.stats if query_rows is not None else None,
    )
    return {
        "repo": options.repo_id,
        "path": file_ctx.path,
        "file_id": file_ctx.file_id,
        "file_sha256": file_ctx.file_sha256,
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


def _empty_ts_file_row(
    *,
    file_ctx: FileContext,
    options: TreeSitterExtractOptions,
    query_pack: TreeSitterQueryPack | None,
    parse_stats: _ParseStats,
) -> Row:
    node_stats = _NodeStats()
    attrs = _file_attrs(
        _file_ctx=file_ctx,
        query_pack=query_pack,
        parse_stats=parse_stats,
        node_stats=node_stats,
        query_stats=None,
    )
    return {
        "repo": options.repo_id,
        "path": file_ctx.path,
        "file_id": file_ctx.file_id,
        "file_sha256": file_ctx.file_sha256,
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
            node_stats=node_stats,
            parse_stats=parse_stats,
            query_stats=None,
        )
        if options.include_stats
        else None,
        "attrs": attrs_map(attrs),
    }


def _collect_node_rows(
    root: Node,
    *,
    file_ctx: FileContext,
    data: SourceBuffer,
    options: TreeSitterExtractOptions,
) -> tuple[list[Row], list[Row], list[Row], list[Row], _NodeStats]:
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
    return node_rows, edge_rows, error_rows, missing_rows, node_stats


def _file_attrs(
    *,
    _file_ctx: FileContext,
    query_pack: TreeSitterQueryPack | None,
    parse_stats: _ParseStats,
    node_stats: _NodeStats,
    query_stats: _QueryStats | None,
) -> dict[str, object]:
    attrs: dict[str, object] = {
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
    if isinstance(version, tuple) and len(version) == SEMVER_PARTS:
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
    rows: list[Row] | None,
    *,
    row_batches: Iterable[Sequence[Mapping[str, object]]] | None,
    plan_context: _TreeSitterPlanContext,
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
class _TreeSitterPlanContext:
    normalize: ExtractNormalizeOptions
    evidence_plan: EvidencePlan | None
    session: ExtractSession


class _TreeSitterTablesKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: TreeSitterExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    context: ExtractExecutionContext | None
    prefer_reader: bool


class _TreeSitterTablesKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: TreeSitterExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    context: ExtractExecutionContext | None
    prefer_reader: Literal[False]


class _TreeSitterTablesKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: TreeSitterExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
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

    Returns:
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Extracted tree-sitter outputs keyed by output name.
    """
    with stage_span(
        "extract.tree_sitter_tables",
        stage="extract",
        scope_name=SCOPE_EXTRACT,
        attributes={"codeanatomy.extractor": "tree_sitter"},
    ):
        repo_files = kwargs["repo_files"]
        normalized_options = normalize_options(
            "tree_sitter",
            kwargs.get("options"),
            TreeSitterExtractOptions,
        )
        normalized_options = _normalize_ts_options(normalized_options)
        context = kwargs.get("context")
        if context is None:
            context = ExtractExecutionContext(
                file_contexts=kwargs.get("file_contexts"),
                evidence_plan=kwargs.get("evidence_plan"),
                scope_manifest=kwargs.get("scope_manifest"),
                session=kwargs.get("session"),
                profile=kwargs.get("profile", "default"),
            )
        session = context.ensure_session()
        context = replace(context, session=session)
        runtime_profile = context.ensure_runtime_profile()
        determinism_tier = context.determinism_tier()
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
                runtime_profile=runtime_profile,
                determinism_tier=determinism_tier,
                options=ExtractMaterializeOptions(
                    normalize=normalize,
                    prefer_reader=prefer_reader,
                    apply_post_kernels=True,
                ),
            ),
        }
