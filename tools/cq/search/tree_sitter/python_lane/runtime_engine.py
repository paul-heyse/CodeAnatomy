"""Optional Python context enrichment using tree-sitter query packs.

This module is best-effort only. Any parser/query failure must degrade to a
payload that never alters ranking, counts, or category classification.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryExecutionSettingsV1,
    QueryExecutionTelemetryV1,
    QueryWindowV1,
)
from tools.cq.search.tree_sitter.contracts.lane_payloads import canonicalize_python_lane_payload
from tools.cq.search.tree_sitter.contracts.query_models import QueryPackPlanV1
from tools.cq.search.tree_sitter.core.infrastructure import child_by_field
from tools.cq.search.tree_sitter.core.lane_support import (
    ENRICHMENT_ERRORS,
    build_query_windows,
    lift_anchor,
    make_parser_from_language,
)
from tools.cq.search.tree_sitter.core.language_registry import load_tree_sitter_language
from tools.cq.search.tree_sitter.core.node_utils import node_text
from tools.cq.search.tree_sitter.core.parse import clear_parse_session, get_parse_session
from tools.cq.search.tree_sitter.core.work_queue import enqueue_windows
from tools.cq.search.tree_sitter.diagnostics import collect_tree_sitter_diagnostics
from tools.cq.search.tree_sitter.python_lane.constants import (
    DEFAULT_MATCH_LIMIT as _DEFAULT_MATCH_LIMIT,
)
from tools.cq.search.tree_sitter.python_lane.constants import (
    MAX_CAPTURE_ITEMS as _MAX_CAPTURE_ITEMS,
)
from tools.cq.search.tree_sitter.python_lane.constants import (
    PYTHON_LIFT_ANCHOR_TYPES as _PYTHON_LIFT_ANCHOR_TYPES,
)
from tools.cq.search.tree_sitter.python_lane.constants import (
    get_python_field_ids,
)
from tools.cq.search.tree_sitter.query.compiler import compile_query
from tools.cq.search.tree_sitter.query.planner import (
    resolve_pack_source_rows_cached,
)
from tools.cq.search.tree_sitter.query.predicates import (
    has_custom_predicates,
    make_query_predicate,
)
from tools.cq.search.tree_sitter.query.registry import load_query_pack_sources

if TYPE_CHECKING:
    from tree_sitter import Language, Node, Query, Tree

    from tools.cq.search.tree_sitter.core.parse import ParseSession

try:
    from tree_sitter import Parser as _TreeSitterParser
    from tree_sitter import Query as _TreeSitterQuery
    from tree_sitter import QueryCursor as _TreeSitterQueryCursor
except ImportError:  # pragma: no cover - optional dependency
    _TreeSitterParser = None
    _TreeSitterQuery = None
    _TreeSitterQueryCursor = None

_MAX_SOURCE_BYTES = 5 * 1024 * 1024
_MAX_CAPTURE_TEXT_LEN = 120
logger = logging.getLogger(__name__)


from tools.cq.search.tree_sitter.python_lane.fallback_support import (
    capture_binding_candidates,
    capture_import_alias_chain,
    capture_named_definition,
    capture_qualified_name_candidates,
    default_parse_quality,
    fallback_resolution_fields,
)


@dataclass(frozen=True, slots=True)
class PythonLaneEnrichmentSettingsV1:
    """Execution settings for Python tree-sitter byte-range enrichment."""

    match_limit: int = _DEFAULT_MATCH_LIMIT
    query_budget_ms: int | None = None


@dataclass(frozen=True, slots=True)
class PythonLaneRuntimeDepsV1:
    """Runtime dependency overrides for Python tree-sitter enrichment."""

    parse_session: ParseSession | None = None
    cache_backend: object | None = None


def is_tree_sitter_python_available() -> bool:
    """Return whether tree-sitter Python runtime dependencies are available."""
    return all(
        obj is not None
        for obj in (
            load_tree_sitter_language("python"),
            _TreeSitterParser,
            _TreeSitterQuery,
            _TreeSitterQueryCursor,
        )
    )


@lru_cache(maxsize=1)
def _python_language() -> Language:
    resolved = load_tree_sitter_language("python")
    if resolved is None:
        msg = "tree_sitter_python language bindings are unavailable"
        raise RuntimeError(msg)
    return cast("Language", resolved)


def _parse_with_session(
    source: str,
    *,
    cache_key: str | None,
    parse_session: ParseSession | None = None,
) -> tuple[Tree | None, bytes, tuple[object, ...]]:
    source_bytes = source.encode("utf-8", errors="replace")
    if not is_tree_sitter_python_available():
        return None, source_bytes, ()
    session = parse_session or get_parse_session(
        language="python", parser_factory=lambda: make_parser_from_language(_python_language())
    )
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
    session = get_parse_session(
        language="python", parser_factory=lambda: make_parser_from_language(_python_language())
    )
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
    except ENRICHMENT_ERRORS:
        return None
    return tree


def clear_tree_sitter_python_cache() -> None:
    """Clear parser/query caches and reset observability counters."""
    logger.debug("Clearing tree-sitter Python cache state")
    clear_parse_session(language="python")
    _python_language.cache_clear()
    compile_query.cache_clear()
    _pack_source_rows.cache_clear()


def get_tree_sitter_python_cache_stats() -> dict[str, int]:
    """Return tree-sitter Python cache counters."""
    session = get_parse_session(
        language="python", parser_factory=lambda: make_parser_from_language(_python_language())
    )
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
    return resolve_pack_source_rows_cached(
        language="python",
        source_rows=tuple(
            (source.pack_name, source.source)
            for source in load_query_pack_sources("python", include_distribution=False)
        ),
        dedupe_by_pack_name=True,
        request_surface="artifact",
    )


def _query_sources() -> tuple[tuple[str, str], ...]:
    return tuple((pack_name, source) for pack_name, source, _ in _pack_source_rows())


def _safe_cursor_captures(
    query: Query,
    root: Node,
    *,
    windows: tuple[QueryWindowV1, ...],
    match_limit: int,
    predicate_callback: object | None = None,
    budget_ms: int | None = None,
) -> tuple[dict[str, list[Node]], QueryExecutionTelemetryV1]:
    from tools.cq.search.tree_sitter.core.runtime_engine import (
        QueryExecutionCallbacksV1,
        run_bounded_query_captures,
    )

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
        settings=QueryExecutionSettingsV1(
            match_limit=match_limit,
            budget_ms=budget_ms,
            window_mode="containment_preferred",
        ),
        callbacks=callbacks,
    )
    return captures, telemetry


def _extract_parse_quality(
    root: Node,
    source_bytes: bytes,
    *,
    windows: tuple[QueryWindowV1, ...],
    query_budget_ms: int | None = None,
) -> dict[str, object]:
    quality: dict[str, object] = {"has_error": bool(getattr(root, "has_error", False))}
    captures: dict[str, list[Node]]
    telemetry = QueryExecutionTelemetryV1()
    try:
        query = compile_query(
            language="python",
            pack_name="__errors__.scm",
            source="(ERROR) @error (MISSING) @missing",
            request_surface="diagnostic",
            validate_rules=False,
        )
        captures, telemetry = _safe_cursor_captures(
            query,
            root,
            windows=windows,
            match_limit=1_024,
            budget_ms=query_budget_ms,
        )
    except ENRICHMENT_ERRORS:
        captures = {}

    def _collect(name: str) -> list[str]:
        nodes = captures.get(name, [])
        rows: list[str] = []
        for node in nodes[:_MAX_CAPTURE_ITEMS]:
            text = node_text(node, source_bytes)
            if text:
                rows.append(text[:_MAX_CAPTURE_TEXT_LEN])
            else:
                rows.append(str(getattr(node, "type", "unknown")))
        return rows

    quality["error_nodes"] = _collect("error")
    quality["missing_nodes"] = _collect("missing")
    quality["did_exceed_match_limit"] = telemetry.exceeded_match_limit
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
    effective_budget_ms = query_budget_ms
    degrade_reasons: set[str] = set()
    window_split_count = 0
    for filename, source in _query_sources():
        try:
            query = compile_query(
                language="python",
                pack_name=filename,
                source=source,
                request_surface="artifact",
            )
            predicate_callback = (
                make_query_predicate(source_bytes=source_bytes)
                if has_custom_predicates(source)
                else None
            )
            captures, telemetry = _safe_cursor_captures(
                query,
                root,
                windows=windows,
                match_limit=match_limit,
                predicate_callback=predicate_callback,
                budget_ms=effective_budget_ms,
            )
            did_exceed_match_limit = did_exceed_match_limit or telemetry.exceeded_match_limit
            cancelled = cancelled or telemetry.cancelled
            window_split_count += int(telemetry.window_split_count)
            if isinstance(telemetry.degrade_reason, str) and telemetry.degrade_reason:
                degrade_reasons.add(telemetry.degrade_reason)
        except ENRICHMENT_ERRORS:
            continue
        _apply_gap_fill_from_captures(payload, captures, source_bytes)
    _finalize_gap_fill_payload(payload)
    payload["query_runtime"] = {
        "match_limit": match_limit,
        "did_exceed_match_limit": did_exceed_match_limit,
        "cancelled": cancelled,
        "window_split_count": window_split_count,
        "degrade_reasons": sorted(degrade_reasons),
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
        value=capture_named_definition(
            captures.get("def.function.name", []),
            captures.get("def.function", []),
            source_bytes,
        ),
    )
    _set_payload_field(
        payload,
        key="enclosing_class",
        value=capture_named_definition(
            captures.get("class.definition.name", []),
            captures.get("class.definition", []),
            source_bytes,
        ),
    )
    _set_payload_field(
        payload,
        key="import_alias_chain",
        value=capture_import_alias_chain(captures, source_bytes),
    )
    _set_payload_field(
        payload,
        key="binding_candidates",
        value=capture_binding_candidates(captures, source_bytes),
    )
    _set_payload_field(
        payload,
        key="qualified_name_candidates",
        value=capture_qualified_name_candidates(captures, source_bytes),
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
        text = node_text(nodes[0], source_bytes)
        if text:
            return text[:_MAX_CAPTURE_TEXT_LEN]

    call_nodes = captures.get("call.expression", [])
    if not call_nodes:
        return None
    function_node = child_by_field(call_nodes[0], "function", get_python_field_ids())
    if function_node is None:
        return None
    text = node_text(function_node, source_bytes)
    return text[:_MAX_CAPTURE_TEXT_LEN] if text else None


def _effective_capture_window(root: Node, *, byte_start: int, byte_end: int) -> tuple[int, int]:
    anchor = root.named_descendant_for_byte_range(byte_start, byte_end)
    if anchor is None:
        return byte_start, byte_end
    lifted = lift_anchor(anchor, parent_types=_PYTHON_LIFT_ANCHOR_TYPES)
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
    logger.warning("Python tree-sitter enrichment degraded: %s", reason)
    payload["enrichment_status"] = "degraded"
    payload["degrade_reason"] = reason
    payload["parse_quality"] = default_parse_quality()
    return payload


def _parse_tree_for_enrichment(
    payload: dict[str, object],
    *,
    source: str,
    cache_key: str | None,
    parse_session: ParseSession | None = None,
) -> tuple[Node, bytes, tuple[object, ...]] | None:
    try:
        tree, source_bytes, changed_ranges = _parse_with_session(
            source,
            cache_key=cache_key,
            parse_session=parse_session,
        )
    except ENRICHMENT_ERRORS as exc:
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
    except ENRICHMENT_ERRORS as exc:
        logger.warning("Python capture-field extraction failed: %s", type(exc).__name__)
        _mark_degraded(payload, reason=type(exc).__name__)


def enrich_python_context_by_byte_range(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
    cache_key: str | None = None,
    settings: PythonLaneEnrichmentSettingsV1 | None = None,
    runtime_deps: PythonLaneRuntimeDepsV1 | None = None,
) -> dict[str, object] | None:
    """Best-effort tree-sitter Python enrichment for a byte-anchored match.

    Returns:
    -------
    dict[str, object] | None
        Enrichment payload, or ``None`` when enrichment is unavailable.
    """
    if byte_start < 0 or byte_end <= byte_start or not is_tree_sitter_python_available():
        return None
    effective_settings = settings or PythonLaneEnrichmentSettingsV1()
    effective_runtime = runtime_deps or PythonLaneRuntimeDepsV1()
    _ = effective_runtime.cache_backend

    source_bytes = source.encode("utf-8", errors="replace")
    if byte_end > len(source_bytes):
        return None

    payload = _base_enrichment_payload()

    if len(source_bytes) > _MAX_SOURCE_BYTES:
        logger.warning(
            "Skipping Python tree-sitter enrichment for oversized source (%d bytes)",
            len(source_bytes),
        )
        payload["enrichment_status"] = "skipped"
        payload["degrade_reason"] = "source_too_large"
        payload["parse_quality"] = default_parse_quality()
        canonical = canonicalize_python_lane_payload(payload)
        builtins_value = msgspec.to_builtins(canonical, str_keys=True)
        return builtins_value if isinstance(builtins_value, dict) else payload

    parsed = _parse_tree_for_enrichment(
        payload,
        source=source,
        cache_key=cache_key,
        parse_session=effective_runtime.parse_session,
    )
    if parsed is None:
        canonical = canonicalize_python_lane_payload(payload)
        builtins_value = msgspec.to_builtins(canonical, str_keys=True)
        return builtins_value if isinstance(builtins_value, dict) else payload

    root, source_bytes, changed_ranges = parsed
    capture_window = _effective_capture_window(
        root,
        byte_start=byte_start,
        byte_end=byte_end,
    )
    query_windows = build_query_windows(
        anchor_window=QueryWindowV1(
            start_byte=capture_window[0],
            end_byte=capture_window[1],
        ),
        source_byte_len=len(source_bytes),
        changed_ranges=changed_ranges,
    )
    enqueue_windows(
        language="python",
        file_key=cache_key or "<memory>",
        windows=query_windows,
    )
    payload["parse_quality"] = _extract_parse_quality(
        root,
        source_bytes,
        windows=query_windows,
        query_budget_ms=effective_settings.query_budget_ms,
    )
    payload["cst_diagnostics"] = [
        msgspec.to_builtins(row)
        for row in collect_tree_sitter_diagnostics(
            language="python",
            root=root,
            windows=query_windows,
            match_limit=1024,
        )
    ]

    _apply_capture_fields(
        payload,
        root=root,
        source_bytes=source_bytes,
        windows=query_windows,
        match_limit=effective_settings.match_limit,
        query_budget_ms=effective_settings.query_budget_ms,
    )
    _merge_fallback_resolution(
        payload,
        fallback_resolution_fields(
            source=source,
            source_bytes=source_bytes,
            byte_start=byte_start,
            byte_end=byte_end,
        ),
    )
    canonical = canonicalize_python_lane_payload(payload)
    builtins_value = msgspec.to_builtins(canonical, str_keys=True)
    return builtins_value if isinstance(builtins_value, dict) else payload


__all__ = [
    "PythonLaneEnrichmentSettingsV1",
    "PythonLaneRuntimeDepsV1",
    "clear_tree_sitter_python_cache",
    "enrich_python_context_by_byte_range",
    "get_python_field_ids",
    "get_tree_sitter_python_cache_stats",
    "is_tree_sitter_python_available",
    "parse_python_tree",
    "parse_python_tree_with_ranges",
]
