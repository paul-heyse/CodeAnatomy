"""Optional Rust context enrichment using ``tree-sitter-rust``.

This module is best-effort only. Any parser or runtime failure must degrade
to ``None`` so core search/query behavior remains unchanged.

Enrichment Contract
-------------------
All fields produced by this module are strictly additive.
They never affect: confidence scores, match counts, category classification,
or relevance ranking.
They may affect: containing_scope display (used only for grouping in output).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.search.tree_sitter.contracts.core_models import (
    ObjectEvidenceRowV1,
    QueryExecutionSettingsV1,
    QueryWindowV1,
    TreeSitterDiagnosticV1,
    TreeSitterQueryHitV1,
)
from tools.cq.search.tree_sitter.core.adaptive_runtime import adaptive_query_budget_ms
from tools.cq.search.tree_sitter.core.lane_support import (
    ENRICHMENT_ERRORS,
    build_query_windows,
)
from tools.cq.search.tree_sitter.core.query_pack_executor import (
    QueryPackExecutionContextV1,
    execute_pack_rows_with_matches,
)
from tools.cq.search.tree_sitter.core.runtime_engine import (
    QueryExecutionCallbacksV1,
)
from tools.cq.search.tree_sitter.core.work_queue import enqueue_windows
from tools.cq.search.tree_sitter.query.compiler import compile_query
from tools.cq.search.tree_sitter.query.predicates import (
    has_custom_predicates,
    make_query_predicate,
)
from tools.cq.search.tree_sitter.rust_lane.availability import (
    is_tree_sitter_rust_available as _is_tree_sitter_rust_available,
)
from tools.cq.search.tree_sitter.rust_lane.bundle import (
    load_rust_grammar_bundle,
)
from tools.cq.search.tree_sitter.rust_lane.extractor_dispatch import (
    _build_enrichment_payload as _build_enrichment_payload_from_dispatch,
)
from tools.cq.search.tree_sitter.rust_lane.extractor_dispatch import (
    canonicalize_payload as _canonicalize_payload,
)
from tools.cq.search.tree_sitter.rust_lane.fact_extraction import (
    _extend_rust_fact_lists_from_rows,
    _import_rows_from_matches,
    _macro_expansion_requests,
    _module_rows_from_matches,
    _rust_fact_lists,
    _rust_fact_payload,
)
from tools.cq.search.tree_sitter.rust_lane.injection_runtime import parse_injected_ranges
from tools.cq.search.tree_sitter.rust_lane.injections import (
    InjectionPlanV1,
    build_injection_plan_from_matches,
)
from tools.cq.search.tree_sitter.rust_lane.query_cache import _pack_sources
from tools.cq.search.tree_sitter.rust_lane.runtime_cache import (
    _parse_with_session,
    _rust_language,
    clear_tree_sitter_rust_cache,
    get_tree_sitter_rust_cache_stats,
)
from tools.cq.search.tree_sitter.structural.exports import collect_diagnostic_rows
from tools.cq.search.tree_sitter.tags import RustTagEventV1, build_tag_events

try:
    from tree_sitter import Point as _TreeSitterPoint
except ImportError:  # pragma: no cover - availability guard
    _TreeSitterPoint = None

if TYPE_CHECKING:
    from tree_sitter import Node

    from tools.cq.search.tree_sitter.core.parse import ParseSession

_DEFAULT_SCOPE_DEPTH = 24
MAX_SOURCE_BYTES = 5 * 1024 * 1024  # 5 MB
logger = logging.getLogger(__name__)
_REQUIRED_PAYLOAD_KEYS: tuple[str, ...] = (
    "language",
    "enrichment_status",
    "enrichment_sources",
)


@dataclass(frozen=True, slots=True)
class RustLaneEnrichmentSettingsV1:
    """Execution settings for Rust tree-sitter enrichment."""

    max_scope_depth: int = _DEFAULT_SCOPE_DEPTH
    query_budget_ms: int | None = None


@dataclass(frozen=True, slots=True)
class RustLaneRuntimeDepsV1:
    """Runtime dependency overrides for Rust tree-sitter enrichment."""

    parse_session: ParseSession | None = None
    cache_backend: object | None = None


def _coerce_timings(payload: Mapping[str, object], *, key: str) -> dict[str, float]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        return {}
    return {
        stage: float(duration)
        for stage, duration in value.items()
        if isinstance(stage, str) and isinstance(duration, (int, float))
    }


def _coerce_status(payload: Mapping[str, object], *, key: str) -> dict[str, str]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        return {}
    return {
        stage: stage_status
        for stage, stage_status in value.items()
        if isinstance(stage, str) and isinstance(stage_status, str)
    }


@dataclass(frozen=True, slots=True)
class _RustPackRunResultV1:
    pack_name: str
    query: object
    captures: dict[str, list[Node]]
    matches: list[tuple[int, dict[str, list[Node]]]]
    rows: tuple[ObjectEvidenceRowV1, ...]
    query_hits: tuple[TreeSitterQueryHitV1, ...]
    capture_telemetry: object
    match_telemetry: object


@dataclass(slots=True)
class _RustPackAccumulatorV1:
    captures: dict[str, list[Node]] = field(default_factory=dict)
    rows: list[ObjectEvidenceRowV1] = field(default_factory=list)
    query_hits: list[TreeSitterQueryHitV1] = field(default_factory=list)
    injection_plans: list[InjectionPlanV1] = field(default_factory=list)
    tag_events: list[RustTagEventV1] = field(default_factory=list)
    query_telemetry: dict[str, object] = field(default_factory=dict)

    def merge(self, *, result: _RustPackRunResultV1, source_bytes: bytes) -> None:
        self.query_telemetry[result.pack_name] = {
            "captures": msgspec.to_builtins(result.capture_telemetry),
            "matches": msgspec.to_builtins(result.match_telemetry),
        }
        for capture_name, nodes in result.captures.items():
            self.captures.setdefault(capture_name, []).extend(nodes)
        if "injection.content" in result.captures:
            self.injection_plans.extend(
                build_injection_plan_from_matches(
                    query=result.query,
                    matches=result.matches,
                    source_bytes=source_bytes,
                    default_language="rust",
                )
            )
        self.tag_events.extend(build_tag_events(matches=result.matches, source_bytes=source_bytes))
        self.rows.extend(result.rows)
        self.query_hits.extend(result.query_hits)

    def finalize(
        self,
    ) -> tuple[
        dict[str, list[Node]],
        tuple[ObjectEvidenceRowV1, ...],
        tuple[TreeSitterQueryHitV1, ...],
        dict[str, object],
        tuple[InjectionPlanV1, ...],
        tuple[RustTagEventV1, ...],
    ]:
        return (
            self.captures,
            tuple(self.rows),
            tuple(self.query_hits),
            self.query_telemetry,
            tuple(self.injection_plans),
            tuple(self.tag_events),
        )


@dataclass(frozen=True, slots=True)
class _RustQueryPackArtifactsV1:
    rows: tuple[ObjectEvidenceRowV1, ...]
    query_hits: tuple[TreeSitterQueryHitV1, ...]
    diagnostics: tuple[TreeSitterDiagnosticV1, ...]
    injection_plan: tuple[InjectionPlanV1, ...]
    tag_events: tuple[RustTagEventV1, ...]
    source_bytes: bytes
    file_key: str | None


@dataclass(frozen=True, slots=True)
class _RustQueryCollectionV1:
    windows: tuple[QueryWindowV1, ...]
    captures: dict[str, list[Node]]
    rows: tuple[ObjectEvidenceRowV1, ...]
    query_hits: tuple[TreeSitterQueryHitV1, ...]
    query_telemetry: dict[str, object]
    injection_plan: tuple[InjectionPlanV1, ...]
    tag_events: tuple[RustTagEventV1, ...]
    diagnostics: tuple[TreeSitterDiagnosticV1, ...]


@dataclass(frozen=True, slots=True)
class _RustQueryExecutionPlanV1:
    windows: tuple[QueryWindowV1, ...]
    settings: QueryExecutionSettingsV1


@dataclass(frozen=True, slots=True)
class _RustPayloadBuildRequestV1:
    node: Node
    tree_root: Node
    source_bytes: bytes
    changed_ranges: tuple[object, ...]
    byte_span: tuple[int, int]
    max_scope_depth: int
    query_budget_ms: int | None
    file_key: str | None


@dataclass(frozen=True, slots=True)
class _RustPipelineRequestV1:
    source: str
    cache_key: str | None
    max_scope_depth: int
    query_budget_ms: int | None
    resolve_node: Callable[[Node], Node | None]
    byte_span_for_node: Callable[[Node], tuple[int, int]]
    error_prefix: str
    parse_session: ParseSession | None = None
    cache_backend: object | None = None


# ---------------------------------------------------------------------------
# Runtime availability
# ---------------------------------------------------------------------------


def is_tree_sitter_rust_available() -> bool:
    """Return whether tree-sitter Rust enrichment dependencies are available.

    Returns:
    -------
    bool
        True when runtime dependencies for Rust tree-sitter enrichment exist.
    """
    return _is_tree_sitter_rust_available()


# ---------------------------------------------------------------------------
# Node / scope utilities
# ---------------------------------------------------------------------------


def _build_enrichment_payload(
    node: Node,
    source_bytes: bytes,
    *,
    max_scope_depth: int,
) -> dict[str, object]:
    """Build the Rust enrichment payload via extracted dispatch helpers.

    Returns:
        dict[str, object]: Normalized enrichment payload for one Rust node match.
    """
    return _build_enrichment_payload_from_dispatch(
        node,
        source_bytes,
        max_scope_depth=max_scope_depth,
    )


def _assert_required_payload_keys(payload: dict[str, object]) -> None:
    missing = [key for key in _REQUIRED_PAYLOAD_KEYS if key not in payload]
    if missing:
        msg = f"Rust enrichment payload missing required keys: {missing}"
        raise ValueError(msg)


# ---------------------------------------------------------------------------
# Query pack execution
# ---------------------------------------------------------------------------


def _pack_callbacks(*, query_source: str, source_bytes: bytes) -> QueryExecutionCallbacksV1 | None:
    if not has_custom_predicates(query_source):
        return None
    return QueryExecutionCallbacksV1(
        predicate_callback=make_query_predicate(source_bytes=source_bytes)
    )


def _run_query_pack(
    *,
    pack_name: str,
    query_source: str,
    root: Node,
    source_bytes: bytes,
    context: QueryPackExecutionContextV1,
) -> _RustPackRunResultV1 | None:
    try:
        query = compile_query(
            language="rust",
            pack_name=pack_name,
            source=query_source,
            request_surface="artifact",
        )
        (
            pack_captures,
            pack_matches,
            pack_rows,
            pack_hits,
            capture_telemetry,
            match_telemetry,
        ) = execute_pack_rows_with_matches(
            query=query,
            query_name=pack_name,
            root=root,
            source_bytes=source_bytes,
            context=context,
        )
    except ENRICHMENT_ERRORS:
        logger.warning("Rust query pack execution failed: %s", pack_name)
        return None
    return _RustPackRunResultV1(
        pack_name=pack_name,
        query=query,
        captures=pack_captures,
        matches=pack_matches,
        rows=pack_rows,
        query_hits=pack_hits,
        capture_telemetry=capture_telemetry,
        match_telemetry=match_telemetry,
    )


def _collect_query_pack_captures(
    *,
    root: Node,
    source_bytes: bytes,
    windows: tuple[QueryWindowV1, ...],
    settings: QueryExecutionSettingsV1,
) -> tuple[
    dict[str, list[Node]],
    tuple[ObjectEvidenceRowV1, ...],
    tuple[TreeSitterQueryHitV1, ...],
    dict[str, object],
    tuple[InjectionPlanV1, ...],
    tuple[RustTagEventV1, ...],
]:
    accumulator = _RustPackAccumulatorV1()
    for pack_name, query_source in _pack_sources():
        result = _run_query_pack(
            pack_name=pack_name,
            query_source=query_source,
            root=root,
            source_bytes=source_bytes,
            context=QueryPackExecutionContextV1(
                windows=windows,
                settings=settings,
                callbacks=_pack_callbacks(query_source=query_source, source_bytes=source_bytes),
            ),
        )
        if result is None:
            continue
        accumulator.merge(result=result, source_bytes=source_bytes)
    return accumulator.finalize()


def collect_query_pack_captures(
    *,
    root: Node,
    source_bytes: bytes,
    windows: tuple[QueryWindowV1, ...],
    settings: QueryExecutionSettingsV1,
) -> tuple[
    dict[str, list[Node]],
    tuple[ObjectEvidenceRowV1, ...],
    tuple[TreeSitterQueryHitV1, ...],
    dict[str, object],
    tuple[InjectionPlanV1, ...],
    tuple[RustTagEventV1, ...],
]:
    """Collect captures, telemetry, and lane artifacts for Rust query packs.

    Returns:
    -------
    tuple[dict[str, list[Node]], tuple[ObjectEvidenceRowV1, ...], tuple[TreeSitterQueryHitV1, ...], dict[str, object], tuple[InjectionPlanV1, ...], tuple[RustTagEventV1, ...]]
        Aggregated captures, rows, telemetry, and lane artifacts.
    """
    return _collect_query_pack_captures(
        root=root,
        source_bytes=source_bytes,
        windows=windows,
        settings=settings,
    )


def _query_windows_for_span(
    *,
    byte_start: int,
    byte_end: int,
    source_byte_len: int,
    changed_ranges: tuple[object, ...],
) -> tuple[QueryWindowV1, ...]:
    anchor_window = QueryWindowV1(start_byte=byte_start, end_byte=byte_end)
    return build_query_windows(
        anchor_window=anchor_window,
        source_byte_len=source_byte_len,
        changed_ranges=changed_ranges,
    )


def _query_execution_plan(
    *,
    byte_start: int,
    byte_end: int,
    source_bytes: bytes,
    changed_ranges: tuple[object, ...],
    query_budget_ms: int | None,
    file_key: str | None,
) -> _RustQueryExecutionPlanV1:
    windows = _query_windows_for_span(
        byte_start=byte_start,
        byte_end=byte_end,
        source_byte_len=len(source_bytes),
        changed_ranges=changed_ranges,
    )
    enqueue_windows(
        language="rust",
        file_key=file_key or "<memory>",
        windows=windows,
    )
    effective_budget_ms = adaptive_query_budget_ms(
        language="rust",
        fallback_budget_ms=query_budget_ms if query_budget_ms is not None else 200,
    )
    return _RustQueryExecutionPlanV1(
        windows=windows,
        settings=QueryExecutionSettingsV1(
            budget_ms=effective_budget_ms,
            has_change_context=bool(changed_ranges),
            window_mode="containment_preferred",
        ),
    )


def _collect_query_bundle(
    *,
    root: Node,
    source_bytes: bytes,
    plan: _RustQueryExecutionPlanV1,
) -> _RustQueryCollectionV1:
    captures, rows, query_hits, query_telemetry, injection_plan, tag_events = (
        _collect_query_pack_captures(
            root=root,
            source_bytes=source_bytes,
            windows=plan.windows,
            settings=plan.settings,
        )
    )
    diagnostics = collect_diagnostic_rows(
        language="rust",
        root=root,
        windows=plan.windows,
        match_limit=1024,
    )
    return _RustQueryCollectionV1(
        windows=plan.windows,
        captures=captures,
        rows=rows,
        query_hits=query_hits,
        query_telemetry=query_telemetry,
        injection_plan=injection_plan,
        tag_events=tag_events,
        diagnostics=diagnostics,
    )


def _base_query_pack_payload(query_telemetry: dict[str, object]) -> dict[str, object]:
    payload: dict[str, object] = (
        {"query_pack_telemetry": query_telemetry} if query_telemetry else {}
    )
    if query_telemetry:
        payload["query_runtime"] = _aggregate_query_runtime(query_telemetry)
    return payload


def _fact_payload_from_collection(
    *,
    captures: dict[str, list[Node]],
    rows: tuple[ObjectEvidenceRowV1, ...],
    source_bytes: bytes,
) -> dict[str, list[str]]:
    definitions, references, calls, imports, modules = _rust_fact_lists(captures, source_bytes)
    _extend_rust_fact_lists_from_rows(
        rows=rows,
        definitions=definitions,
        references=references,
        calls=calls,
        imports=imports,
        modules=modules,
    )
    return _rust_fact_payload(
        definitions=definitions,
        references=references,
        calls=calls,
        imports=imports,
        modules=modules,
    )


def _collect_query_pack_payload(
    *,
    root: Node,
    source_bytes: bytes,
    byte_span: tuple[int, int],
    changed_ranges: tuple[object, ...] = (),
    query_budget_ms: int | None = None,
    file_key: str | None = None,
) -> dict[str, object]:
    byte_start, byte_end = byte_span
    if byte_end <= byte_start:
        return {}

    plan = _query_execution_plan(
        byte_start=byte_start,
        byte_end=byte_end,
        source_bytes=source_bytes,
        changed_ranges=changed_ranges,
        query_budget_ms=query_budget_ms,
        file_key=file_key,
    )
    collection = _collect_query_bundle(
        root=root,
        source_bytes=source_bytes,
        plan=plan,
    )
    payload = _base_query_pack_payload(collection.query_telemetry)
    payload["rust_tree_sitter_facts"] = _fact_payload_from_collection(
        captures=collection.captures,
        rows=collection.rows,
        source_bytes=source_bytes,
    )
    module_rows = _module_rows_from_matches(rows=collection.rows, file_key=file_key)
    import_rows = _import_rows_from_matches(rows=collection.rows, module_rows=module_rows)
    payload["rust_module_rows"] = module_rows
    payload["rust_import_rows"] = import_rows
    _attach_query_pack_payload(
        payload=payload,
        artifacts=_RustQueryPackArtifactsV1(
            rows=collection.rows,
            query_hits=collection.query_hits,
            diagnostics=collection.diagnostics,
            injection_plan=collection.injection_plan,
            tag_events=collection.tag_events,
            source_bytes=source_bytes,
            file_key=file_key,
        ),
    )
    return payload


def collect_query_pack_payload(
    *,
    root: Node,
    source_bytes: bytes,
    byte_span: tuple[int, int],
    changed_ranges: tuple[object, ...] = (),
    query_budget_ms: int | None = None,
    file_key: str | None = None,
) -> dict[str, object]:
    """Collect query-pack payload for the provided byte span.

    Returns:
    -------
    dict[str, object]
        Query-pack payload for the selected source range.
    """
    return _collect_query_pack_payload(
        root=root,
        source_bytes=source_bytes,
        byte_span=byte_span,
        changed_ranges=changed_ranges,
        query_budget_ms=query_budget_ms,
        file_key=file_key,
    )


def _aggregate_query_runtime(query_telemetry: dict[str, object]) -> dict[str, object]:
    did_exceed_match_limit = False
    cancelled = False
    window_split_count = 0
    degrade_reasons: set[str] = set()
    for telemetry_row in query_telemetry.values():
        if not isinstance(telemetry_row, dict):
            continue
        for phase in ("captures", "matches"):
            phase_row = telemetry_row.get(phase)
            if not isinstance(phase_row, dict):
                continue
            did_exceed_match_limit = did_exceed_match_limit or bool(
                phase_row.get("exceeded_match_limit")
            )
            cancelled = cancelled or bool(phase_row.get("cancelled"))
            split = phase_row.get("window_split_count")
            if isinstance(split, int) and not isinstance(split, bool):
                window_split_count += split
            reason = phase_row.get("degrade_reason")
            if isinstance(reason, str) and reason:
                degrade_reasons.add(reason)
    return {
        "did_exceed_match_limit": did_exceed_match_limit,
        "cancelled": cancelled,
        "window_split_count": window_split_count,
        "degrade_reasons": sorted(degrade_reasons),
    }


def _attach_query_pack_payload(
    *,
    payload: dict[str, object],
    artifacts: _RustQueryPackArtifactsV1,
) -> None:
    payload["query_pack_bundle"] = msgspec.to_builtins(
        load_rust_grammar_bundle(profile_name="rust_search_enriched")
    )
    payload["cst_query_hits"] = [msgspec.to_builtins(row) for row in artifacts.query_hits]
    payload["cst_diagnostics"] = [msgspec.to_builtins(row) for row in artifacts.diagnostics]
    payload["query_pack_injections"] = [
        msgspec.to_builtins(row) for row in artifacts.injection_plan
    ]
    payload["query_pack_tags"] = [msgspec.to_builtins(row) for row in artifacts.tag_events]
    payload["query_pack_tag_summary"] = {
        "definitions": sum(1 for row in artifacts.tag_events if row.role == "definition"),
        "references": sum(1 for row in artifacts.tag_events if row.role == "reference"),
    }
    macro_requests = _macro_expansion_requests(
        rows=artifacts.rows,
        source_bytes=artifacts.source_bytes,
        file_key=artifacts.file_key,
    )
    if macro_requests:
        payload["macro_expansion_requests"] = [msgspec.to_builtins(row) for row in macro_requests]
    payload["query_pack_injection_profiles"] = sorted(
        {
            str(row.profile_name)
            for row in artifacts.injection_plan
            if isinstance(getattr(row, "profile_name", None), str)
        }
    )
    rust_plan = tuple(row for row in artifacts.injection_plan if row.language == "rust")
    if rust_plan:
        payload["query_pack_injection_runtime"] = msgspec.to_builtins(
            parse_injected_ranges(
                source_bytes=artifacts.source_bytes,
                language=_rust_language(),
                plans=rust_plan,
            )
        )


def _collect_payload_with_timings(request: _RustPayloadBuildRequestV1) -> dict[str, object]:
    payload_build_started = time.perf_counter()
    payload = _build_enrichment_payload(
        request.node,
        request.source_bytes,
        max_scope_depth=request.max_scope_depth,
    )
    payload_build_ms = max(0.0, (time.perf_counter() - payload_build_started) * 1000.0)
    query_pack_started = time.perf_counter()
    payload.update(
        _collect_query_pack_payload(
            root=request.tree_root,
            source_bytes=request.source_bytes,
            byte_span=request.byte_span,
            changed_ranges=request.changed_ranges,
            query_budget_ms=request.query_budget_ms,
            file_key=request.file_key,
        )
    )
    query_pack_ms = max(0.0, (time.perf_counter() - query_pack_started) * 1000.0)
    payload["stage_timings_ms"] = {
        "query_pack": query_pack_ms,
        "payload_build": payload_build_ms,
    }
    payload["stage_status"] = {
        "query_pack": "applied",
        "payload_build": "applied",
    }
    _assert_required_payload_keys(payload)
    return payload


def collect_payload_with_timings(request: object) -> dict[str, object]:
    """Collect enrichment payload with stage timing metadata.

    Returns:
    -------
    dict[str, object]
        Enrichment payload annotated with stage timings and status.
    """
    return _collect_payload_with_timings(cast("_RustPayloadBuildRequestV1", request))


def _finalize_enrichment_payload(
    *,
    payload: dict[str, object],
    total_started: float,
) -> dict[str, object]:
    attachment_started = time.perf_counter()
    canonical = _canonicalize_payload(payload)
    attachment_ms = max(0.0, (time.perf_counter() - attachment_started) * 1000.0)
    timings = _coerce_timings(canonical, key="stage_timings_ms")
    timings["attachment"] = attachment_ms
    timings["total"] = max(0.0, (time.perf_counter() - total_started) * 1000.0)
    canonical["stage_timings_ms"] = timings
    stage_status = _coerce_status(canonical, key="stage_status")
    stage_status.setdefault("ast_grep", "skipped")
    stage_status.setdefault("query_pack", "applied")
    stage_status.setdefault("payload_build", "applied")
    stage_status["attachment"] = "applied"
    canonical["stage_status"] = stage_status
    return canonical


def _run_rust_enrichment_pipeline(request: _RustPipelineRequestV1) -> dict[str, object] | None:
    total_started = time.perf_counter()
    tree_sitter_started = time.perf_counter()
    try:
        _ = request.cache_backend
        tree, source_bytes, changed_ranges = _parse_with_session(
            request.source,
            cache_key=request.cache_key,
            parse_session=request.parse_session,
        )
        if tree is None:
            return None
        node = request.resolve_node(tree.root_node)
        if node is None:
            return None
        payload = _collect_payload_with_timings(
            _RustPayloadBuildRequestV1(
                node=node,
                tree_root=tree.root_node,
                source_bytes=source_bytes,
                changed_ranges=changed_ranges,
                byte_span=request.byte_span_for_node(node),
                max_scope_depth=request.max_scope_depth,
                query_budget_ms=request.query_budget_ms,
                file_key=request.cache_key,
            )
        )
    except ENRICHMENT_ERRORS as exc:
        logger.warning("%s failed: %s", request.error_prefix, type(exc).__name__)
        return None
    timings = _coerce_timings(payload, key="stage_timings_ms")
    timings.setdefault("ast_grep", 0.0)
    timings["tree_sitter"] = max(0.0, (time.perf_counter() - tree_sitter_started) * 1000.0)
    payload["stage_timings_ms"] = timings
    status = _coerce_status(payload, key="stage_status")
    status.setdefault("ast_grep", "skipped")
    status["tree_sitter"] = "applied"
    payload["stage_status"] = status
    return _finalize_enrichment_payload(payload=payload, total_started=total_started)


def run_rust_enrichment_pipeline(request: object) -> dict[str, object] | None:
    """Run full Rust enrichment pipeline for a source request.

    Returns:
    -------
    dict[str, object] | None
        Canonical payload on success, or ``None`` when enrichment is unavailable.
    """
    return _run_rust_enrichment_pipeline(cast("_RustPipelineRequestV1", request))


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def enrich_rust_context(
    source: str,
    *,
    line: int,
    col: int,
    cache_key: str | None = None,
    settings: RustLaneEnrichmentSettingsV1 | None = None,
    runtime_deps: RustLaneRuntimeDepsV1 | None = None,
) -> dict[str, object] | None:
    """Extract optional Rust context details for a match location.

    Parameters
    ----------
    source
        Full source text of the Rust file.
    line
        1-based line number of the match.
    col
        0-based column of the match.
    cache_key
        Stable file key for parse-tree cache reuse.
    max_scope_depth
        Maximum ancestor levels to inspect.

    Returns:
    -------
    dict[str, object] | None
        Best-effort context payload, or ``None`` when unavailable.
    """
    if not is_tree_sitter_rust_available() or line < 1 or col < 0 or len(source) > MAX_SOURCE_BYTES:
        if len(source) > MAX_SOURCE_BYTES:
            logger.warning(
                "Skipping Rust tree-sitter enrichment for oversized source (%d chars)",
                len(source),
            )
        return None

    if _TreeSitterPoint is None:
        return None
    effective_settings = settings or RustLaneEnrichmentSettingsV1()
    effective_runtime = runtime_deps or RustLaneRuntimeDepsV1()
    point = _TreeSitterPoint(max(0, line - 1), max(0, col))
    return _run_rust_enrichment_pipeline(
        _RustPipelineRequestV1(
            source=source,
            cache_key=cache_key,
            max_scope_depth=effective_settings.max_scope_depth,
            query_budget_ms=effective_settings.query_budget_ms,
            resolve_node=lambda root: root.named_descendant_for_point_range(point, point),
            byte_span_for_node=lambda node: (
                int(getattr(node, "start_byte", 0)),
                int(getattr(node, "end_byte", 0)),
            ),
            error_prefix="Rust context enrichment",
            parse_session=effective_runtime.parse_session,
            cache_backend=effective_runtime.cache_backend,
        ),
    )


def enrich_rust_context_by_byte_range(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
    cache_key: str | None = None,
    settings: RustLaneEnrichmentSettingsV1 | None = None,
    runtime_deps: RustLaneRuntimeDepsV1 | None = None,
) -> dict[str, object] | None:
    """Extract optional Rust context using byte offsets instead of line/col.

    Use this entry point when the caller already has canonical byte offsets,
    avoiding line/column conversion inaccuracies with multi-byte characters.

    Parameters
    ----------
    source
        Full source text of the Rust file.
    byte_start
        0-based byte offset of the match start.
    byte_end
        0-based byte offset of the match end (exclusive).
    cache_key
        Stable file key for parse-tree cache reuse.
    max_scope_depth
        Maximum ancestor levels to inspect.

    Returns:
    -------
    dict[str, object] | None
        Best-effort context payload, or ``None`` when unavailable.
    """
    if not is_tree_sitter_rust_available() or byte_start < 0 or byte_end <= byte_start:
        return None

    source_byte_len = len(source.encode("utf-8", errors="replace"))
    if source_byte_len > MAX_SOURCE_BYTES or byte_end > source_byte_len:
        if source_byte_len > MAX_SOURCE_BYTES:
            logger.warning(
                "Skipping Rust byte-range enrichment for oversized source (%d bytes)",
                source_byte_len,
            )
        return None

    effective_settings = settings or RustLaneEnrichmentSettingsV1()
    effective_runtime = runtime_deps or RustLaneRuntimeDepsV1()
    return _run_rust_enrichment_pipeline(
        _RustPipelineRequestV1(
            source=source,
            cache_key=cache_key,
            max_scope_depth=effective_settings.max_scope_depth,
            query_budget_ms=effective_settings.query_budget_ms,
            resolve_node=lambda root: root.named_descendant_for_byte_range(byte_start, byte_end),
            byte_span_for_node=lambda _node: (byte_start, byte_end),
            error_prefix="Rust byte-range enrichment",
            parse_session=effective_runtime.parse_session,
            cache_backend=effective_runtime.cache_backend,
        )
    )


__all__ = [
    "MAX_SOURCE_BYTES",
    "RustLaneEnrichmentSettingsV1",
    "RustLaneRuntimeDepsV1",
    "clear_tree_sitter_rust_cache",
    "collect_payload_with_timings",
    "collect_query_pack_captures",
    "collect_query_pack_payload",
    "enrich_rust_context",
    "enrich_rust_context_by_byte_range",
    "get_tree_sitter_rust_cache_stats",
    "is_tree_sitter_rust_available",
    "run_rust_enrichment_pipeline",
]
