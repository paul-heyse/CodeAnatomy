"""Rust lane query payload-builder ownership module."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import msgspec

from tools.cq.search.tree_sitter.contracts.core_models import (
    ObjectEvidenceRowV1,
    QueryExecutionSettingsV1,
    QueryWindowV1,
    TreeSitterDiagnosticV1,
    TreeSitterQueryHitV1,
)
from tools.cq.search.tree_sitter.core.adaptive_runtime import adaptive_query_budget_ms
from tools.cq.search.tree_sitter.core.lane_support import build_query_windows
from tools.cq.search.tree_sitter.core.work_queue import enqueue_windows
from tools.cq.search.tree_sitter.rust_lane.bundle import load_rust_grammar_bundle
from tools.cq.search.tree_sitter.rust_lane.fact_extraction import (
    _extend_rust_fact_lists_from_rows,
    _import_rows_from_matches,
    _macro_expansion_requests,
    _module_rows_from_matches,
    _rust_fact_lists,
    _rust_fact_payload,
)
from tools.cq.search.tree_sitter.rust_lane.injection_runtime import parse_injected_ranges
from tools.cq.search.tree_sitter.rust_lane.injections import InjectionPlanV1
from tools.cq.search.tree_sitter.rust_lane.runtime_cache import _rust_language
from tools.cq.search.tree_sitter.rust_lane.runtime_query_execution import (
    collect_query_pack_captures,
)
from tools.cq.search.tree_sitter.structural.exports import collect_diagnostic_rows
from tools.cq.search.tree_sitter.tags import RustTagEventV1

if TYPE_CHECKING:
    from tree_sitter import Node


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
        collect_query_pack_captures(
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


__all__ = ["collect_query_pack_payload"]
