"""Optional Python context enrichment using tree-sitter query packs.

This module is best-effort only. Any parser/query failure must degrade to a
payload that never alters ranking, counts, or category classification.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from functools import lru_cache
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.search._shared.core import node_text as _shared_node_text
from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryExecutionSettingsV1,
    QueryWindowV1,
)
from tools.cq.search.tree_sitter.contracts.query_models import QueryPackPlanV1, load_pack_rules
from tools.cq.search.tree_sitter.core.adaptive_runtime import adaptive_query_budget_ms
from tools.cq.search.tree_sitter.core.change_windows import (
    contains_window,
    ensure_query_windows,
    windows_from_changed_ranges,
)
from tools.cq.search.tree_sitter.core.parse import clear_parse_session, get_parse_session
from tools.cq.search.tree_sitter.core.runtime import (
    QueryExecutionCallbacksV1,
    run_bounded_query_captures,
)
from tools.cq.search.tree_sitter.core.work_queue import enqueue_windows
from tools.cq.search.tree_sitter.diagnostics.collector import collect_tree_sitter_diagnostics
from tools.cq.search.tree_sitter.query.planner import build_pack_plan, sort_pack_plans
from tools.cq.search.tree_sitter.query.predicates import (
    has_custom_predicates,
    make_query_predicate,
)
from tools.cq.search.tree_sitter.query.registry import load_query_pack_sources
from tools.cq.search.tree_sitter.query.specialization import specialize_query

if TYPE_CHECKING:
    from tree_sitter import Language, Node, Parser, Query, Tree

try:
    import tree_sitter_python as _tree_sitter_python
    from tree_sitter import Language as _TreeSitterLanguage
    from tree_sitter import Parser as _TreeSitterParser
    from tree_sitter import Query as _TreeSitterQuery
    from tree_sitter import QueryCursor as _TreeSitterQueryCursor
except ImportError:  # pragma: no cover - optional dependency
    _tree_sitter_python = None
    _TreeSitterLanguage = None
    _TreeSitterParser = None
    _TreeSitterQuery = None
    _TreeSitterQueryCursor = None

_MAX_SOURCE_BYTES = 5 * 1024 * 1024
_MAX_CAPTURE_ITEMS = 8
_MAX_CAPTURE_TEXT_LEN = 120
_DEFAULT_MATCH_LIMIT = 4_096
_ENRICHMENT_ERRORS = (RuntimeError, TypeError, ValueError, AttributeError, UnicodeError)
_STOP_CONTEXT_KINDS: frozenset[str] = frozenset({"module", "source_file"})


from tools.cq.search.tree_sitter.python_lane.fallback_support import (
    _capture_binding_candidates,
    _capture_import_alias_chain,
    _capture_named_definition,
    _capture_qualified_name_candidates,
    _default_parse_quality,
    _fallback_resolution_fields,
)


def is_tree_sitter_python_available() -> bool:
    """Return whether tree-sitter Python runtime dependencies are available."""
    return all(
        obj is not None
        for obj in (
            _tree_sitter_python,
            _TreeSitterLanguage,
            _TreeSitterParser,
            _TreeSitterQuery,
            _TreeSitterQueryCursor,
        )
    )


@lru_cache(maxsize=1)
def _python_language() -> Language:
    if _tree_sitter_python is None or _TreeSitterLanguage is None:
        msg = "tree_sitter_python language bindings are unavailable"
        raise RuntimeError(msg)
    return _TreeSitterLanguage(_tree_sitter_python.language())


def _make_parser() -> Parser:
    if _TreeSitterParser is None:
        msg = "tree_sitter parser bindings are unavailable"
        raise RuntimeError(msg)
    return _TreeSitterParser(_python_language())


def _parse_tree(source_bytes: bytes) -> Tree:
    parser = _make_parser()
    tree = parser.parse(source_bytes)
    if tree is None:
        msg = "tree-sitter parser returned no tree"
        raise RuntimeError(msg)
    return tree


def _parse_with_session(
    source: str,
    *,
    cache_key: str | None,
) -> tuple[Tree | None, bytes, tuple[object, ...]]:
    source_bytes = source.encode("utf-8", errors="replace")
    if not is_tree_sitter_python_available():
        return None, source_bytes, ()
    session = get_parse_session(language="python", parser_factory=_make_parser)
    tree, changed_ranges, _reused = session.parse(file_key=cache_key, source_bytes=source_bytes)
    return tree, source_bytes, tuple(changed_ranges) if changed_ranges is not None else ()


def parse_python_tree_with_ranges(
    source: str,
    *,
    cache_key: str | None = None,
) -> tuple[Tree | None, tuple[object, ...]]:
    """Parse source and return tree with changed ranges when incremental parse is used.

    Returns:
        tuple[Tree | None, tuple[object, ...]]: Parsed tree and changed ranges
            (empty when caching is unavailable or no changes are tracked).
    """
    source_bytes = source.encode("utf-8", errors="replace")
    if not is_tree_sitter_python_available():
        return None, ()
    session = get_parse_session(language="python", parser_factory=_make_parser)
    tree, changed_ranges, _reused = session.parse(file_key=cache_key, source_bytes=source_bytes)
    return tree, changed_ranges


def parse_python_tree(source: str, *, cache_key: str | None = None) -> Tree | None:
    """Parse and optionally cache a tree-sitter Python tree.

    Returns:
        Tree | None: Parsed syntax tree, or ``None`` when unavailable or parsing
            fails.
    """
    if not is_tree_sitter_python_available():
        return None
    try:
        tree, _, _ = _parse_with_session(source, cache_key=cache_key)
    except _ENRICHMENT_ERRORS:
        return None
    return tree


def _build_query_windows(
    *,
    anchor_window: QueryWindowV1,
    source_byte_len: int,
    changed_ranges: tuple[object, ...],
) -> tuple[QueryWindowV1, ...]:
    windows = windows_from_changed_ranges(
        changed_ranges,
        source_byte_len=source_byte_len,
        pad_bytes=96,
    )
    windows = ensure_query_windows(windows, fallback=anchor_window)
    if windows and not contains_window(
        windows,
        value=anchor_window.start_byte,
        width=anchor_window.end_byte - anchor_window.start_byte,
    ):
        return (*windows, anchor_window)
    return windows


def clear_tree_sitter_python_cache() -> None:
    """Clear parser/query caches and reset observability counters."""
    clear_parse_session(language="python")
    _python_language.cache_clear()
    _compile_query.cache_clear()
    _pack_source_rows.cache_clear()


def get_tree_sitter_python_cache_stats() -> dict[str, int]:
    """Return tree-sitter Python cache counters."""
    session = get_parse_session(language="python", parser_factory=_make_parser)
    stats = session.stats()
    return {
        "entries": stats.entries,
        "cache_hits": stats.cache_hits,
        "cache_misses": stats.cache_misses,
        "cache_evictions": 0,
        "parse_count": stats.parse_count,
        "reparse_count": stats.reparse_count,
        "edit_failures": stats.edit_failures,
    }


@lru_cache(maxsize=1)
def _pack_source_rows() -> tuple[tuple[str, str, QueryPackPlanV1], ...]:
    sources: dict[str, str] = {}
    for source in load_query_pack_sources("python", include_distribution=False):
        if source.pack_name.endswith(".scm"):
            sources[source.pack_name] = source.source
    packed = [
        (
            pack_name,
            source,
            build_pack_plan(
                pack_name=pack_name,
                query=_compile_query(
                    pack_name,
                    source,
                    request_surface="artifact",
                ),
                query_text=source,
            ),
        )
        for pack_name, source in sorted(sources.items())
    ]
    return sort_pack_plans(packed)


def _query_sources() -> tuple[tuple[str, str], ...]:
    return tuple((pack_name, source) for pack_name, source, _ in _pack_source_rows())


@lru_cache(maxsize=64)
def _compile_query(
    _filename: str,
    source: str,
    *,
    request_surface: str = "artifact",
) -> Query:
    if _TreeSitterQuery is None:
        msg = "tree_sitter query bindings are unavailable"
        raise RuntimeError(msg)
    query = _TreeSitterQuery(_python_language(), source)
    rules = load_pack_rules("python")
    pattern_count = int(getattr(query, "pattern_count", 0))
    for pattern_idx in range(pattern_count):
        if rules.require_rooted and not bool(query.is_pattern_rooted(pattern_idx)):
            msg = f"python query pattern not rooted: {pattern_idx}"
            raise ValueError(msg)
        if rules.forbid_non_local and bool(query.is_pattern_non_local(pattern_idx)):
            msg = f"python query pattern non-local: {pattern_idx}"
            raise ValueError(msg)
    return specialize_query(query, request_surface=request_surface)


def _safe_cursor_captures(
    query: Query,
    root: Node,
    *,
    windows: tuple[QueryWindowV1, ...],
    match_limit: int,
    predicate_callback: object | None = None,
    budget_ms: int | None = None,
) -> tuple[dict[str, list[Node]], bool, bool]:
    typed_predicate = (
        cast(
            "Callable[[str, object, int, Mapping[str, Sequence[object]]], bool]",
            predicate_callback,
        )
        if callable(predicate_callback)
        else None
    )
    callbacks = (
        QueryExecutionCallbacksV1(predicate_callback=typed_predicate)
        if typed_predicate is not None
        else None
    )
    captures, telemetry = run_bounded_query_captures(
        query,
        root,
        windows=windows,
        settings=QueryExecutionSettingsV1(match_limit=match_limit, budget_ms=budget_ms),
        callbacks=callbacks,
    )
    return captures, telemetry.exceeded_match_limit, telemetry.cancelled


def _node_text(node: Node, source_bytes: bytes) -> str:
    return _shared_node_text(node, source_bytes)


def _extract_parse_quality(
    root: Node,
    source_bytes: bytes,
    *,
    windows: tuple[QueryWindowV1, ...],
    query_budget_ms: int | None = None,
) -> dict[str, object]:
    quality: dict[str, object] = {"has_error": bool(getattr(root, "has_error", False))}
    captures: dict[str, list[Node]]
    did_exceed_match_limit = False
    try:
        query = _compile_query(
            "__errors__.scm",
            "(ERROR) @error (MISSING) @missing",
            request_surface="diagnostic",
        )
        captures, did_exceed_match_limit, _cancelled = _safe_cursor_captures(
            query,
            root,
            windows=windows,
            match_limit=1_024,
            budget_ms=query_budget_ms,
        )
    except _ENRICHMENT_ERRORS:
        captures = {}

    def _collect(name: str) -> list[str]:
        nodes = captures.get(name, [])
        rows: list[str] = []
        for node in nodes[:_MAX_CAPTURE_ITEMS]:
            text = _node_text(node, source_bytes)
            if text:
                rows.append(text[:_MAX_CAPTURE_TEXT_LEN])
            else:
                rows.append(str(getattr(node, "type", "unknown")))
        return rows

    quality["error_nodes"] = _collect("error")
    quality["missing_nodes"] = _collect("missing")
    quality["did_exceed_match_limit"] = did_exceed_match_limit
    return quality


def _capture_gap_fill_fields(
    root: Node,
    source_bytes: bytes,
    *,
    windows: tuple[QueryWindowV1, ...],
    match_limit: int,
    query_budget_ms: int | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {}
    did_exceed_match_limit = False
    cancelled = False
    fallback_budget = query_budget_ms if query_budget_ms is not None else 200
    effective_budget_ms = adaptive_query_budget_ms(
        language="python",
        fallback_budget_ms=fallback_budget,
    )
    for filename, source in _query_sources():
        try:
            query = _compile_query(filename, source, request_surface="artifact")
            predicate_callback = (
                make_query_predicate(source_bytes=source_bytes)
                if has_custom_predicates(source)
                else None
            )
            captures, exceeded, cancelled_one = _safe_cursor_captures(
                query,
                root,
                windows=windows,
                match_limit=match_limit,
                predicate_callback=predicate_callback,
                budget_ms=effective_budget_ms,
            )
            did_exceed_match_limit = did_exceed_match_limit or exceeded
            cancelled = cancelled or cancelled_one
        except _ENRICHMENT_ERRORS:
            continue
        _apply_gap_fill_from_captures(payload, captures, source_bytes)
    _finalize_gap_fill_payload(payload)
    payload["query_runtime"] = {
        "match_limit": match_limit,
        "did_exceed_match_limit": did_exceed_match_limit,
        "cancelled": cancelled,
    }
    return payload


def _set_payload_field(
    payload: dict[str, object],
    *,
    key: str,
    value: object | None,
) -> None:
    if key in payload:
        return
    if value is None:
        return
    if isinstance(value, (list, dict)) and not value:
        return
    payload[key] = value


def _apply_gap_fill_from_captures(
    payload: dict[str, object],
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> None:
    _set_payload_field(
        payload,
        key="call_target",
        value=_capture_call_target(captures, source_bytes),
    )
    _set_payload_field(
        payload,
        key="enclosing_callable",
        value=_capture_named_definition(
            captures.get("def.function.name", []),
            captures.get("def.function", []),
            source_bytes,
        ),
    )
    _set_payload_field(
        payload,
        key="enclosing_class",
        value=_capture_named_definition(
            captures.get("class.definition.name", []),
            captures.get("class.definition", []),
            source_bytes,
        ),
    )
    _set_payload_field(
        payload,
        key="import_alias_chain",
        value=_capture_import_alias_chain(captures, source_bytes),
    )
    _set_payload_field(
        payload,
        key="binding_candidates",
        value=_capture_binding_candidates(captures, source_bytes),
    )
    _set_payload_field(
        payload,
        key="qualified_name_candidates",
        value=_capture_qualified_name_candidates(captures, source_bytes),
    )


def _finalize_gap_fill_payload(payload: dict[str, object]) -> None:
    payload.setdefault("qualified_name_candidates", [])
    payload.setdefault("binding_candidates", [])
    payload.setdefault("import_alias_chain", [])
    call_target = payload.get("call_target")
    if not payload["qualified_name_candidates"] and isinstance(call_target, str) and call_target:
        payload["qualified_name_candidates"] = [{"name": call_target, "source": "tree_sitter"}]
    if not payload["binding_candidates"]:
        binding_candidate = _binding_candidate_from_call_target(call_target)
        if binding_candidate is not None:
            payload["binding_candidates"] = [binding_candidate]


def _binding_candidate_from_call_target(call_target: object) -> dict[str, object] | None:
    if not isinstance(call_target, str):
        return None
    head = call_target.split(".", maxsplit=1)[0].strip()
    if not head:
        return None
    return {
        "name": head[:_MAX_CAPTURE_TEXT_LEN],
        "kind": "tree_sitter_call_target",
        "byte_start": -1,
    }


def _capture_call_target(captures: dict[str, list[Node]], source_bytes: bytes) -> str | None:
    for capture_name in (
        "call.target.identifier",
        "call.target.attribute",
        "call.function.identifier",
        "call.function.attribute",
    ):
        nodes = captures.get(capture_name, [])
        if not nodes:
            continue
        text = _node_text(nodes[0], source_bytes)
        if text:
            return text[:_MAX_CAPTURE_TEXT_LEN]

    call_nodes = captures.get("call.expression", [])
    if not call_nodes:
        return None
    function_node = call_nodes[0].child_by_field_name("function")
    if function_node is None:
        return None
    text = _node_text(function_node, source_bytes)
    return text[:_MAX_CAPTURE_TEXT_LEN] if text else None

    return {
        "has_error": False,
        "error_nodes": list[str](),
        "missing_nodes": list[str](),
        "did_exceed_match_limit": False,
    }


def _lift_anchor(node: Node) -> Node:
    current = node
    while current.parent is not None:
        parent = current.parent
        if parent.type in {
            "call",
            "attribute",
            "assignment",
            "import_statement",
            "import_from_statement",
            "function_definition",
            "class_definition",
        }:
            return parent
        if current.type in _STOP_CONTEXT_KINDS:
            return current
        current = parent
    return node


def _effective_capture_window(root: Node, *, byte_start: int, byte_end: int) -> tuple[int, int]:
    anchor = root.named_descendant_for_byte_range(byte_start, byte_end)
    if anchor is None:
        return byte_start, byte_end
    lifted = _lift_anchor(anchor)
    start = int(getattr(lifted, "start_byte", byte_start))
    end = int(getattr(lifted, "end_byte", byte_end))
    if end <= start:
        return byte_start, byte_end
    return start, end


def _base_enrichment_payload() -> dict[str, object]:
    return {
        "language": "python",
        "enrichment_status": "applied",
        "enrichment_sources": ["tree_sitter"],
    }


def _mark_degraded(payload: dict[str, object], *, reason: str) -> dict[str, object]:
    payload["enrichment_status"] = "degraded"
    payload["degrade_reason"] = reason
    payload["parse_quality"] = _default_parse_quality()
    return payload


def _parse_tree_for_enrichment(
    payload: dict[str, object],
    *,
    source: str,
    cache_key: str | None,
) -> tuple[Node, bytes, tuple[object, ...]] | None:
    try:
        tree, source_bytes, changed_ranges = _parse_with_session(source, cache_key=cache_key)
    except _ENRICHMENT_ERRORS as exc:
        _mark_degraded(payload, reason=type(exc).__name__)
        return None
    if tree is None:
        _mark_degraded(payload, reason="parse_unavailable")
        return None
    return tree.root_node, source_bytes, changed_ranges


def _merge_fallback_resolution(
    payload: dict[str, object],
    fallback: dict[str, object],
) -> None:
    if not fallback:
        return
    if not payload.get("import_alias_chain"):
        payload["import_alias_chain"] = fallback.get("import_alias_chain", [])
    if not payload.get("call_target"):
        payload["call_target"] = fallback.get("call_target")
    if not payload.get("qualified_name_candidates"):
        payload["qualified_name_candidates"] = fallback.get("qualified_name_candidates", [])
    if not payload.get("binding_candidates"):
        payload["binding_candidates"] = fallback.get("binding_candidates", [])


def _apply_capture_fields(
    payload: dict[str, object],
    *,
    root: Node,
    source_bytes: bytes,
    windows: tuple[QueryWindowV1, ...],
    match_limit: int,
    query_budget_ms: int | None,
) -> None:
    try:
        payload.update(
            _capture_gap_fill_fields(
                root,
                source_bytes,
                windows=windows,
                match_limit=match_limit,
                query_budget_ms=query_budget_ms,
            )
        )
    except _ENRICHMENT_ERRORS as exc:
        payload["enrichment_status"] = "degraded"
        payload["degrade_reason"] = type(exc).__name__


def enrich_python_context_by_byte_range(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
    cache_key: str | None = None,
    match_limit: int = _DEFAULT_MATCH_LIMIT,
    query_budget_ms: int | None = None,
) -> dict[str, object] | None:
    """Best-effort tree-sitter Python enrichment for a byte-anchored match.

    Returns:
    -------
    dict[str, object] | None
        Enrichment payload, or ``None`` when enrichment is unavailable.
    """
    if byte_start < 0 or byte_end <= byte_start or not is_tree_sitter_python_available():
        return None

    source_bytes = source.encode("utf-8", errors="replace")
    if byte_end > len(source_bytes):
        return None

    payload = _base_enrichment_payload()

    if len(source_bytes) > _MAX_SOURCE_BYTES:
        payload["enrichment_status"] = "skipped"
        payload["degrade_reason"] = "source_too_large"
        payload["parse_quality"] = _default_parse_quality()
        return payload

    parsed = _parse_tree_for_enrichment(
        payload,
        source=source,
        cache_key=cache_key,
    )
    if parsed is None:
        return payload

    root, source_bytes, changed_ranges = parsed
    window_start, window_end = _effective_capture_window(
        root,
        byte_start=byte_start,
        byte_end=byte_end,
    )
    anchor_window = QueryWindowV1(start_byte=window_start, end_byte=window_end)
    query_windows = _build_query_windows(
        anchor_window=anchor_window,
        source_byte_len=len(source_bytes),
        changed_ranges=changed_ranges,
    )
    enqueue_windows(
        language="python",
        file_key=cache_key or "<memory>",
        windows=query_windows,
    )
    parse_quality = _extract_parse_quality(
        root,
        source_bytes,
        windows=query_windows,
        query_budget_ms=query_budget_ms,
    )
    payload["parse_quality"] = parse_quality
    diagnostics = collect_tree_sitter_diagnostics(
        language="python",
        root=root,
        windows=query_windows,
        match_limit=1024,
    )
    payload["tree_sitter_diagnostics"] = [msgspec.to_builtins(row) for row in diagnostics]

    _apply_capture_fields(
        payload,
        root=root,
        source_bytes=source_bytes,
        windows=query_windows,
        match_limit=match_limit,
        query_budget_ms=query_budget_ms,
    )
    fallback = _fallback_resolution_fields(
        source=source,
        source_bytes=source_bytes,
        byte_start=byte_start,
        byte_end=byte_end,
    )
    _merge_fallback_resolution(payload, fallback)
    return payload


__all__ = [
    "clear_tree_sitter_python_cache",
    "enrich_python_context_by_byte_range",
    "get_tree_sitter_python_cache_stats",
    "is_tree_sitter_python_available",
    "parse_python_tree",
    "parse_python_tree_with_ranges",
]
