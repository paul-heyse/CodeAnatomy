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
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import msgspec

from tools.cq.search.rust.extractors_shared import RUST_SCOPE_KINDS
from tools.cq.search.tree_sitter.contracts.core_models import (
    ObjectEvidenceRowV1,
    QueryExecutionSettingsV1,
    QueryWindowV1,
    TreeSitterDiagnosticV1,
    TreeSitterQueryHitV1,
)
from tools.cq.search.tree_sitter.contracts.lane_payloads import canonicalize_rust_lane_payload
from tools.cq.search.tree_sitter.core.adaptive_runtime import adaptive_query_budget_ms
from tools.cq.search.tree_sitter.core.infrastructure import child_by_field
from tools.cq.search.tree_sitter.core.lane_support import (
    ENRICHMENT_ERRORS,
    build_query_windows,
)
from tools.cq.search.tree_sitter.core.node_utils import node_text
from tools.cq.search.tree_sitter.core.query_pack_executor import (
    QueryPackExecutionContextV1,
    execute_pack_rows_with_matches,
)
from tools.cq.search.tree_sitter.core.runtime import (
    QueryExecutionCallbacksV1,
)
from tools.cq.search.tree_sitter.core.work_queue import enqueue_windows
from tools.cq.search.tree_sitter.query.compiler import compile_query
from tools.cq.search.tree_sitter.query.predicates import (
    has_custom_predicates,
    make_query_predicate,
)
from tools.cq.search.tree_sitter.rust_lane.bundle import (
    load_rust_grammar_bundle,
)
from tools.cq.search.tree_sitter.rust_lane.enrichment_extractors import (
    _extract_attributes_dict,
    _extract_call_target,
    _extract_enum_shape,
    _extract_function_signature,
    _extract_impl_context,
    _extract_struct_shape,
    _extract_visibility_dict,
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
from tools.cq.search.tree_sitter.rust_lane.role_classification import _classify_item_role
from tools.cq.search.tree_sitter.rust_lane.runtime_cache import (
    _parse_with_session,
    _rust_field_ids,
    _rust_language,
    clear_tree_sitter_rust_cache,
    get_tree_sitter_rust_cache_stats,
)
from tools.cq.search.tree_sitter.structural.exports import collect_diagnostic_rows
from tools.cq.search.tree_sitter.tags import RustTagEventV1, build_tag_events

if TYPE_CHECKING:
    from tree_sitter import Node

try:
    from tree_sitter import Point as _TreeSitterPoint
except ImportError:  # pragma: no cover - exercised via availability checks
    _TreeSitterPoint = None

_SCOPE_KINDS: tuple[str, ...] = tuple(sorted(RUST_SCOPE_KINDS - {"block"}))

_DEFAULT_SCOPE_DEPTH = 24
_MAX_SCOPE_NODES = 256
MAX_SOURCE_BYTES = 5 * 1024 * 1024  # 5 MB
logger = logging.getLogger(__name__)


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
    from tools.cq.search.tree_sitter.core.language_registry import load_tree_sitter_language

    try:
        from tree_sitter import Parser as _TreeSitterParser
    except ImportError:  # pragma: no cover
        return False

    return all(
        obj is not None
        for obj in (
            load_tree_sitter_language("rust"),
            _TreeSitterParser,
            _TreeSitterPoint,
        )
    )


# ---------------------------------------------------------------------------
# Node / scope utilities
# ---------------------------------------------------------------------------


def _scope_name(scope_node: Node, source_bytes: bytes) -> str | None:
    kind = scope_node.type
    if kind == "impl_item":
        return node_text(child_by_field(scope_node, "type", _rust_field_ids()), source_bytes)
    if kind == "macro_invocation":
        return node_text(
            child_by_field(scope_node, "macro", _rust_field_ids()), source_bytes
        ) or node_text(
            child_by_field(scope_node, "name", _rust_field_ids()),
            source_bytes,
        )
    return node_text(child_by_field(scope_node, "name", _rust_field_ids()), source_bytes)


def _scope_chain(node: Node, source_bytes: bytes, *, max_depth: int) -> list[str]:
    chain: list[str] = []
    current: Node | None = node
    depth = 0
    nodes_visited = 0
    while current is not None and depth < max_depth:
        if nodes_visited >= _MAX_SCOPE_NODES:
            break
        if current.type in _SCOPE_KINDS:
            name = _scope_name(current, source_bytes)
            chain.append(f"{current.type}:{name}" if name else current.type)
        current = current.parent
        depth += 1
        nodes_visited += 1
    return chain


def _find_scope(node: Node, *, max_depth: int) -> Node | None:
    current: Node | None = node
    depth = 0
    while current is not None and depth < max_depth:
        if current.type in _SCOPE_KINDS:
            return current
        current = current.parent
        depth += 1
    return None


def _find_ancestor(node: Node, kind: str, *, max_depth: int) -> Node | None:
    """Walk up from *node* to find the nearest ancestor of the given kind.

    Parameters
    ----------
    node
        Starting node.
    kind
        Target node ``type`` string.
    max_depth
        Maximum parent levels to inspect.

    Returns:
    -------
    Node | None
        The ancestor node, or ``None`` if not found within *max_depth*.
    """
    current: Node | None = node.parent
    depth = 0
    while current is not None and depth < max_depth:
        if current.type == kind:
            return current
        current = current.parent
        depth += 1
    return None


# ---------------------------------------------------------------------------
# Shared enrichment builder
# ---------------------------------------------------------------------------

_DEFINITION_SCOPE_KINDS: frozenset[str] = frozenset(_SCOPE_KINDS)

_CALL_NODE_KINDS: frozenset[str] = frozenset({"call_expression", "macro_invocation"})


def _try_extract(
    label: str,
    extractor: Callable[[Node, bytes], dict[str, object]],
    target: Node,
    source_bytes: bytes,
) -> tuple[dict[str, object], str | None]:
    """Call *extractor* on *target*, returning results or a degrade reason.

    Parameters
    ----------
    label
        Human label for the extractor (used in degradation messages).
    extractor
        Callable ``(Node, bytes) -> dict[str, object]``.
    target
        Node argument forwarded to extractor.
    source_bytes
        Source bytes forwarded to extractor.

    Returns:
    -------
    tuple[dict[str, object], str | None]
        Extracted fields and an optional degrade reason on failure.
    """
    try:
        result = extractor(target, source_bytes)
    except ENRICHMENT_ERRORS as exc:
        logger.warning("Rust extractor degraded (%s): %s", label, type(exc).__name__)
        return {}, f"{label}: {exc}"
    else:
        return result, None


def _resolve_target(node: Node, scope: Node | None, kind: str) -> Node | None:
    """Return the node or scope matching *kind*, preferring node.

    Parameters
    ----------
    node
        The direct match node.
    scope
        The enclosing scope node (may be ``None``).
    kind
        The target node type string.

    Returns:
    -------
    Node | None
        The matching node, or ``None``.
    """
    if node.type == kind:
        return node
    if scope is not None and scope.type == kind:
        return scope
    return None


def _resolve_definition_target(node: Node, scope: Node | None) -> Node | None:
    """Find the best definition-category node for visibility/attribute extraction.

    Parameters
    ----------
    node
        The direct match node.
    scope
        The enclosing scope node (may be ``None``).

    Returns:
    -------
    Node | None
        The first matching definition-scope node, or ``None``.
    """
    for kind in _DEFINITION_SCOPE_KINDS:
        target = _resolve_target(node, scope, kind)
        if target is not None:
            return target
    return None


def _merge_result(
    payload: dict[str, object],
    reasons: list[str],
    result: tuple[dict[str, object], str | None],
) -> None:
    """Merge an extractor result into *payload* and record any degrade reason.

    Parameters
    ----------
    payload
        Target dict to update.
    reasons
        Accumulator for degradation reason strings.
    result
        Return value from ``_try_extract``.
    """
    fields, reason = result
    payload.update(fields)
    if reason is not None:
        reasons.append(reason)


def _apply_extractors(
    payload: dict[str, object],
    node: Node,
    scope: Node | None,
    source_bytes: bytes,
    *,
    max_scope_depth: int,
) -> None:
    """Dispatch all applicable extractors, updating *payload* in place.

    Parameters
    ----------
    payload
        Target dict to populate with extractor fields.
    node
        The tree-sitter node at the match location.
    scope
        The nearest enclosing scope node, or ``None``.
    source_bytes
        Full source bytes.
    max_scope_depth
        Maximum ancestor levels for ancestor searches.
    """
    reasons: list[str] = []

    sig_target = _resolve_target(node, scope, "function_item")
    if sig_target is not None:
        _merge_result(
            payload,
            reasons,
            _try_extract("signature", _extract_function_signature, sig_target, source_bytes),
        )

    vis_target = _resolve_definition_target(node, scope)
    if vis_target is not None:
        _enrich_visibility_and_attrs(payload, reasons, vis_target, source_bytes)

    impl_target = _resolve_impl_ancestor(node, scope, max_scope_depth=max_scope_depth)
    if impl_target is not None:
        _merge_result(
            payload,
            reasons,
            _try_extract("impl_context", _extract_impl_context, impl_target, source_bytes),
        )

    if scope is not None and scope.type == "struct_item":
        _merge_result(
            payload,
            reasons,
            _try_extract("struct_shape", _extract_struct_shape, scope, source_bytes),
        )
    if scope is not None and scope.type == "enum_item":
        _merge_result(
            payload, reasons, _try_extract("enum_shape", _extract_enum_shape, scope, source_bytes)
        )

    if node.type in _CALL_NODE_KINDS:
        _merge_result(
            payload, reasons, _try_extract("call_target", _extract_call_target, node, source_bytes)
        )

    # Classify item role using already-extracted attributes
    extracted_attrs = payload.get("attributes")
    attr_list: list[str] = extracted_attrs if isinstance(extracted_attrs, list) else []
    try:
        payload["item_role"] = _classify_item_role(
            node, scope, attr_list, max_scope_depth=max_scope_depth
        )
    except ENRICHMENT_ERRORS as exc:
        logger.warning("Rust item_role classification degraded: %s", type(exc).__name__)
        reasons.append(f"item_role: {exc}")

    if reasons:
        logger.warning("Rust tree-sitter enrichment degraded: %s", "; ".join(reasons))
        payload["enrichment_status"] = "degraded"
        payload["degrade_reason"] = "; ".join(reasons)


def _enrich_visibility_and_attrs(
    payload: dict[str, object],
    reasons: list[str],
    target: Node,
    source_bytes: bytes,
) -> None:
    """Extract visibility and attributes for a definition node.

    Parameters
    ----------
    payload
        Target dict to update.
    reasons
        Accumulator for degradation reason strings.
    target
        A definition-category scope node.
    source_bytes
        Full source bytes.
    """
    fields, reason = _try_extract("visibility", _extract_visibility_dict, target, source_bytes)
    payload.update(fields)
    if reason is not None:
        reasons.append(reason)

    fields, reason = _try_extract("attributes", _extract_attributes_dict, target, source_bytes)
    payload.update(fields)
    if reason is not None:
        reasons.append(reason)


def _resolve_impl_ancestor(
    node: Node,
    scope: Node | None,
    *,
    max_scope_depth: int,
) -> Node | None:
    """Find the nearest ``impl_item`` from scope or ancestors.

    Parameters
    ----------
    node
        Match node.
    scope
        Enclosing scope node.
    max_scope_depth
        Maximum ancestor search depth.

    Returns:
    -------
    Node | None
        The impl node, or ``None``.
    """
    if scope is not None and scope.type == "impl_item":
        return scope
    return _find_ancestor(node, "impl_item", max_depth=max_scope_depth)


def _build_enrichment_payload(
    node: Node,
    scope: Node | None,
    source_bytes: bytes,
    *,
    max_scope_depth: int,
) -> dict[str, object]:
    """Build the full enrichment payload from resolved node and scope.

    This is the shared implementation called by both ``enrich_rust_context``
    and ``enrich_rust_context_by_byte_range``.

    Parameters
    ----------
    node
        The tree-sitter node at the match location.
    scope
        The nearest enclosing scope node, or ``None``.
    source_bytes
        Full source bytes.
    max_scope_depth
        Maximum ancestor levels for scope chain traversal.

    Returns:
    -------
    dict[str, object]
        The enrichment payload with all applicable fields.
    """
    chain = _scope_chain(node, source_bytes, max_depth=max_scope_depth)
    payload: dict[str, object] = {
        "node_kind": node.type,
        "scope_chain": chain,
        "language": "rust",
        "enrichment_status": "applied",
        "enrichment_sources": ["tree_sitter"],
    }

    if scope is not None:
        payload["scope_kind"] = scope.type
        scope_nm = _scope_name(scope, source_bytes)
        if scope_nm:
            payload["scope_name"] = scope_nm

    _apply_extractors(payload, node, scope, source_bytes, max_scope_depth=max_scope_depth)
    return payload


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


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def enrich_rust_context(
    source: str,
    *,
    line: int,
    col: int,
    cache_key: str | None = None,
    max_scope_depth: int = _DEFAULT_SCOPE_DEPTH,
    query_budget_ms: int | None = None,
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

    try:
        tree, source_bytes, changed_ranges = _parse_with_session(
            source,
            cache_key=cache_key,
        )
        if tree is None:
            return None
        if _TreeSitterPoint is None:
            return None
        point = _TreeSitterPoint(max(0, line - 1), max(0, col))
        node = tree.root_node.named_descendant_for_point_range(point, point)
        if node is None:
            return None
        scope = _find_scope(node, max_depth=max_scope_depth)
        payload = _build_enrichment_payload(
            node, scope, source_bytes, max_scope_depth=max_scope_depth
        )
        payload.update(
            _collect_query_pack_payload(
                root=tree.root_node,
                source_bytes=source_bytes,
                byte_span=(
                    int(getattr(node, "start_byte", 0)),
                    int(getattr(node, "end_byte", 0)),
                ),
                changed_ranges=changed_ranges,
                query_budget_ms=query_budget_ms,
                file_key=cache_key,
            )
        )
    except ENRICHMENT_ERRORS as exc:
        logger.warning("Rust context enrichment failed: %s", type(exc).__name__)
        return None
    else:
        return canonicalize_rust_lane_payload(payload)


def enrich_rust_context_by_byte_range(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
    cache_key: str | None = None,
    max_scope_depth: int = _DEFAULT_SCOPE_DEPTH,
    query_budget_ms: int | None = None,
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

    try:
        tree, source_bytes, changed_ranges = _parse_with_session(
            source,
            cache_key=cache_key,
        )
        if tree is None:
            return None
        node = tree.root_node.named_descendant_for_byte_range(byte_start, byte_end)
        if node is None:
            return None
        scope = _find_scope(node, max_depth=max_scope_depth)
        payload = _build_enrichment_payload(
            node, scope, source_bytes, max_scope_depth=max_scope_depth
        )
        payload.update(
            _collect_query_pack_payload(
                root=tree.root_node,
                source_bytes=source_bytes,
                byte_span=(byte_start, byte_end),
                changed_ranges=changed_ranges,
                query_budget_ms=query_budget_ms,
                file_key=cache_key,
            )
        )
    except ENRICHMENT_ERRORS as exc:
        logger.warning("Rust byte-range enrichment failed: %s", type(exc).__name__)
        return None
    else:
        return canonicalize_rust_lane_payload(payload)


__all__ = [
    "MAX_SOURCE_BYTES",
    "clear_tree_sitter_rust_cache",
    "enrich_rust_context",
    "enrich_rust_context_by_byte_range",
    "get_tree_sitter_rust_cache_stats",
    "is_tree_sitter_rust_available",
]
