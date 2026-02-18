"""Stage-runner ownership module for Python enrichment entrypoints."""

from __future__ import annotations

import os
from time import perf_counter
from typing import TYPE_CHECKING

from tools.cq.search._shared.error_boundaries import ENRICHMENT_ERRORS
from tools.cq.search.python.analysis_session import PythonAnalysisSession
from tools.cq.search.python.extractors_budget import trim_payload_to_budget
from tools.cq.search.python.resolution_index import enrich_python_resolution_by_byte_range
from tools.cq.search.tree_sitter.python_lane.facts_runtime import build_python_tree_sitter_facts

if TYPE_CHECKING:
    from ast_grep_py import SgNode

    from tools.cq.search.python.extractors_entrypoints import _PythonEnrichmentState


def run_ast_grep_stage(
    state: _PythonEnrichmentState,
    *,
    node: SgNode,
    node_kind: str,
    source_bytes: bytes,
) -> None:
    """Run ast-grep tier and merge results into enrichment state."""
    from tools.cq.search.python import extractors_entrypoints as entrypoints

    ast_started = perf_counter()
    sg_fields, degrade_reasons = entrypoints.enrich_ast_grep_tier(
        node,
        node_kind,
        source_bytes,
        context=state.context,
    )
    stage_patch = entrypoints.build_stage_fact_patch(sg_fields)
    state.stage_timings_ms["ast_grep"] = (perf_counter() - ast_started) * 1000.0
    state.stage_status["ast_grep"] = "degraded" if degrade_reasons else "applied"
    entrypoints.ingest_stage_fact_patch(state, stage_patch)
    state.ast_stage = entrypoints.build_stage_facts_from_enrichment(stage_patch.facts)
    state.degrade_reasons.extend(degrade_reasons)


def run_python_ast_stage(
    state: _PythonEnrichmentState,
    *,
    node: SgNode,
    source_bytes: bytes,
    cache_key: str,
) -> None:
    """Run Python AST tier for function-like nodes."""
    from tools.cq.search.python import extractors_entrypoints as entrypoints

    if not entrypoints.is_function_node(node):
        state.stage_status["python_ast"] = "skipped"
        state.stage_timings_ms["python_ast"] = 0.0
        return

    py_ast_started = perf_counter()
    ast_extra_fields, ast_extra_reasons = entrypoints.enrich_python_ast_tier(
        node,
        source_bytes,
        cache_key,
    )
    stage_patch = entrypoints.build_stage_fact_patch(ast_extra_fields)
    state.stage_timings_ms["python_ast"] = (perf_counter() - py_ast_started) * 1000.0
    entrypoints.ingest_stage_fact_patch(state, stage_patch, source="python_ast")
    state.degrade_reasons.extend(ast_extra_reasons)
    state.stage_status["python_ast"] = "degraded" if ast_extra_reasons else "applied"


def run_import_stage(
    state: _PythonEnrichmentState,
    *,
    node: SgNode,
    node_kind: str,
    source_bytes: bytes,
    cache_key: str,
    line: int,
) -> None:
    """Run import-detail tier for import statement nodes."""
    from tools.cq.search.python import extractors_entrypoints as entrypoints

    if node_kind not in {"import_statement", "import_from_statement"}:
        state.stage_status["import_detail"] = "skipped"
        state.stage_timings_ms["import_detail"] = 0.0
        return

    import_started = perf_counter()
    imp_fields, imp_reasons = entrypoints.enrich_import_tier(node, source_bytes, cache_key, line)
    stage_patch = entrypoints.build_stage_fact_patch(imp_fields)
    state.stage_timings_ms["import_detail"] = (perf_counter() - import_started) * 1000.0
    entrypoints.ingest_stage_fact_patch(state, stage_patch, source="python_ast")
    state.degrade_reasons.extend(imp_reasons)
    state.stage_status["import_detail"] = "degraded" if imp_reasons else "applied"


def _decode_python_source_text(
    *,
    source_bytes: bytes,
    session: PythonAnalysisSession | None,
) -> str:
    """Decode source bytes using session source when available.

    Returns:
        str: Decoded source text.
    """
    return session.source if session is not None else source_bytes.decode("utf-8", errors="replace")


def run_python_resolution_stage(
    state: _PythonEnrichmentState,
    *,
    source_bytes: bytes,
    byte_start: int | None,
    byte_end: int | None,
    cache_key: str,
    session: PythonAnalysisSession | None,
) -> None:
    """Run byte-range Python resolution tier and merge stage facts."""
    from tools.cq.search.python import extractors_entrypoints as entrypoints

    if byte_start is None or byte_end is None:
        state.stage_status["python_resolution"] = "skipped"
        state.stage_timings_ms["python_resolution"] = 0.0
        return

    resolution_started = perf_counter()
    resolution_reasons: list[str] = []
    resolution_payload: dict[str, object] = {}
    try:
        source_text = _decode_python_source_text(source_bytes=source_bytes, session=session)
        resolution_payload = enrich_python_resolution_by_byte_range(
            source_text,
            source_bytes=source_bytes,
            file_path=cache_key,
            byte_start=byte_start,
            byte_end=byte_end,
            session=session,
        )
    except ENRICHMENT_ERRORS as exc:
        entrypoints.logger.warning("Python resolution enrichment failed: %s", type(exc).__name__)
        resolution_payload = {}
        resolution_reasons.append(f"python_resolution: {type(exc).__name__}")
    stage_patch = entrypoints.build_stage_fact_patch(resolution_payload)
    state.python_resolution_stage = entrypoints.build_stage_facts_from_enrichment(stage_patch.facts)
    state.stage_timings_ms["python_resolution"] = (perf_counter() - resolution_started) * 1000.0
    state.degrade_reasons.extend(resolution_reasons)
    if resolution_payload:
        entrypoints.ingest_stage_fact_patch(state, stage_patch, source="python_resolution")
        state.stage_status["python_resolution"] = "applied"
        return
    state.stage_status["python_resolution"] = "degraded" if resolution_reasons else "skipped"


def run_tree_sitter_stage(
    state: _PythonEnrichmentState,
    *,
    source_bytes: bytes,
    byte_span: tuple[int | None, int | None],
    cache_key: str,
    query_budget_ms: int | None,
    session: PythonAnalysisSession | None,
) -> None:
    """Run Python tree-sitter tier and merge stage outputs."""
    from tools.cq.search.python import extractors_entrypoints as entrypoints

    byte_start, byte_end = byte_span
    if byte_start is None or byte_end is None:
        state.stage_status["tree_sitter"] = "skipped"
        state.stage_timings_ms["tree_sitter"] = 0.0
        return

    ts_started = perf_counter()
    tree_sitter_reasons: list[str] = []
    tree_sitter_payload: dict[str, object] = {}
    try:
        source_text = _decode_python_source_text(source_bytes=source_bytes, session=session)
        ts_payload = build_python_tree_sitter_facts(
            source_text,
            byte_start=byte_start,
            byte_end=byte_end,
            cache_key=cache_key,
            query_budget_ms=query_budget_ms,
        )
        if ts_payload:
            tree_sitter_payload = {
                key: value for key, value in ts_payload.items() if isinstance(key, str)
            }
            stage_patch = entrypoints.build_stage_fact_patch(tree_sitter_payload)
            state.tree_sitter_stage = entrypoints.build_stage_facts_from_enrichment(stage_patch.facts)
            entrypoints.ingest_stage_fact_patch(state, stage_patch, source="tree_sitter")
            ts_status = ts_payload.get("enrichment_status")
            state.stage_status["tree_sitter"] = (
                ts_status if isinstance(ts_status, str) else "applied"
            )
            ts_reason = ts_payload.get("degrade_reason")
            if isinstance(ts_reason, str) and ts_reason:
                tree_sitter_reasons.append(f"tree_sitter: {ts_reason}")
        else:
            state.stage_status["tree_sitter"] = "skipped"
    except ENRICHMENT_ERRORS as exc:
        entrypoints.logger.warning(
            "Python tree-sitter enrichment stage failed: %s", type(exc).__name__
        )
        state.stage_status["tree_sitter"] = "degraded"
        tree_sitter_reasons.append(f"tree_sitter: {type(exc).__name__}")
    if not tree_sitter_payload:
        state.tree_sitter_stage = entrypoints.new_python_agreement_stage()
    state.stage_timings_ms["tree_sitter"] = (perf_counter() - ts_started) * 1000.0
    state.degrade_reasons.extend(tree_sitter_reasons)


def finalize_python_enrichment_payload(state: _PythonEnrichmentState) -> dict[str, object]:
    """Build final payload dict from staged enrichment state.

    Returns:
        dict[str, object]: Final bounded enrichment payload.
    """
    from tools.cq.search.python import extractors_entrypoints as entrypoints

    payload = {
        **entrypoints.flatten_python_enrichment_facts(state.facts),
        **state.metadata,
    }

    agreement = entrypoints.build_agreement_section(
        ast_stage=state.ast_stage,
        python_resolution_stage=state.python_resolution_stage,
        tree_sitter_stage=state.tree_sitter_stage,
    )
    payload["agreement"] = agreement
    if (
        os.getenv(entrypoints.python_enrichment_crosscheck_env()) == "1"
        and agreement.get("status") == "conflict"
    ):
        conflicts = agreement.get("conflicts")
        if isinstance(conflicts, list):
            payload["crosscheck_mismatches"] = conflicts
        state.degrade_reasons.append("crosscheck mismatch")

    if state.degrade_reasons:
        payload["enrichment_status"] = "degraded"
        payload["degrade_reason"] = "; ".join(state.degrade_reasons)

    payload["stage_status"] = state.stage_status
    payload["stage_timings_ms"] = state.stage_timings_ms

    if state.context.truncations:
        payload["truncated_fields"] = list(state.context.truncations)

    payload, dropped_fields, size_hint = trim_payload_to_budget(
        payload,
        max_payload_bytes=entrypoints.max_python_payload_bytes(),
    )
    payload["payload_size_hint"] = size_hint
    if dropped_fields:
        payload["dropped_fields"] = dropped_fields
    return payload


__all__ = [
    "finalize_python_enrichment_payload",
    "run_ast_grep_stage",
    "run_import_stage",
    "run_python_ast_stage",
    "run_python_resolution_stage",
    "run_tree_sitter_stage",
]
