"""Call discovery command entry point and orchestration.

Coordinates scanning, analysis, enrichment, and result construction
for the calls macro command.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.cache.run_lifecycle import maybe_evict_run_cache_tag
from tools.cq.core.schema import (
    CqResult,
    Finding,
    ScoreDetails,
    Section,
    append_result_key_finding,
    assign_result_finding_ids,
    insert_result_section,
    ms,
    update_result_summary,
)
from tools.cq.core.scoring import build_detail_payload
from tools.cq.core.summary_contract import build_semantic_telemetry, summary_from_mapping
from tools.cq.core.summary_update_contracts import CallsSummaryUpdateV1
from tools.cq.macros.calls.analysis import (
    CallSite,
    _analyze_sites,
    _collect_call_sites_from_records,
)
from tools.cq.macros.calls.insight import (
    CallsFrontDoorState,
    CallsInsightSummary,
    _add_context_section,
    _add_hazard_section,
    _add_kw_section,
    _add_shape_section,
    _add_sites_section,
    _build_call_scoring,
    _build_calls_confidence,
    _build_calls_front_door_insight,
    _finalize_calls_semantic_state,
    _find_function_signature,
)
from tools.cq.macros.calls.neighborhood import (
    CallAnalysisSummary,
    CallsNeighborhoodRequest,
    _build_calls_neighborhood,
)
from tools.cq.macros.calls.scanning import _group_candidates, _rg_find_candidates
from tools.cq.macros.calls.semantic import CallsSemanticRequest, _apply_calls_semantic
from tools.cq.macros.calls_target import (
    AttachTargetMetadataRequestV1,
    apply_target_metadata,
    infer_target_language,
    resolve_target_metadata,
)
from tools.cq.macros.constants import (
    CALLS_TARGET_CALLEE_PREVIEW,
    FRONT_DOOR_PREVIEW_PER_SLICE,
)
from tools.cq.macros.contracts import CallsRequest
from tools.cq.macros.result_builder import MacroResultBuilder
from tools.cq.macros.rust_fallback_policy import RustFallbackPolicyV1, apply_rust_fallback_policy
from tools.cq.query.sg_parser import SgRecord, list_scan_files, sg_scan
from tools.cq.search.pipeline.profiles import INTERACTIVE
from tools.cq.search.rg.adapter import FilePatternSearchOptions, find_files_with_pattern

if TYPE_CHECKING:
    from tools.cq.core.front_door_schema import FrontDoorInsightV1
    from tools.cq.core.toolchain import Toolchain
    from tools.cq.core.types import QueryLanguage
    from tools.cq.macros.calls.analysis import CallSite

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CallsContext:
    """Execution context for the calls macro."""

    tc: Toolchain
    root: Path
    argv: list[str]
    function_name: str


@dataclass(frozen=True)
class CallScanResult:
    """Bundle results from scanning call sites."""

    candidate_files: list[Path]
    scan_files: list[Path]
    total_py_files: int
    call_records: list[SgRecord]
    used_fallback: bool
    all_sites: list[CallSite]
    files_with_calls: int
    rg_candidates: int
    signature_info: str


def _scan_call_sites(root_path: Path, function_name: str) -> CallScanResult:
    """Scan for call sites using ast-grep, falling back to ripgrep if needed.

    Returns:
    -------
    CallScanResult
        Summary of scan inputs, outputs, and fallback status.
    """
    search_name = function_name.rsplit(".", maxsplit=1)[-1]
    pattern = rf"\b{search_name}\s*\("
    target_language = infer_target_language(root_path, function_name) or "python"
    candidate_files = find_files_with_pattern(
        root_path,
        pattern,
        options=FilePatternSearchOptions(
            limits=INTERACTIVE,
            lang_scope=target_language,
        ),
    )

    all_scan_files = list_scan_files([root_path], root=root_path, lang=target_language)
    total_py_files = len(all_scan_files)

    scan_files = candidate_files if candidate_files else all_scan_files

    used_fallback = False
    call_records: list[SgRecord] = []
    if scan_files:
        try:
            call_records = sg_scan(
                paths=scan_files,
                record_types={"call"},
                root=root_path,
                lang=target_language,
            )
        except (OSError, RuntimeError, ValueError) as exc:
            logger.warning("Calls ast-grep scan failed for '%s': %s", function_name, exc)
            used_fallback = True
    else:
        used_fallback = True

    signature_info = (
        _find_function_signature(root_path, function_name) if target_language == "python" else ""
    )

    if not used_fallback:
        all_sites, files_with_calls = _collect_call_sites_from_records(
            root_path,
            call_records,
            function_name,
        )
        rg_candidates = 0
    else:
        from tools.cq.macros.calls.analysis import collect_call_sites

        candidates = _rg_find_candidates(
            function_name,
            root_path,
            lang_scope=target_language,
        )
        by_file = _group_candidates(candidates)
        all_sites = collect_call_sites(root_path, by_file, function_name)
        files_with_calls = len({site.file for site in all_sites})
        rg_candidates = len(candidates)

    return CallScanResult(
        candidate_files=candidate_files,
        scan_files=scan_files,
        total_py_files=total_py_files,
        call_records=call_records,
        used_fallback=used_fallback,
        all_sites=all_sites,
        files_with_calls=files_with_calls,
        rg_candidates=rg_candidates,
        signature_info=signature_info,
    )


def scan_call_sites(root_path: Path, function_name: str) -> CallScanResult:
    """Public scan helper for call-site discovery.

    Returns:
        CallScanResult: Aggregated scan inputs and discovered call-site rows.
    """
    return _scan_call_sites(root_path, function_name)


def _build_calls_summary(
    function_name: str,
    scan_result: CallScanResult,
) -> CallsSummaryUpdateV1:
    return CallsSummaryUpdateV1(
        query=function_name,
        mode="macro:calls",
        function=function_name,
        signature=scan_result.signature_info,
        total_sites=len(scan_result.all_sites),
        files_with_calls=scan_result.files_with_calls,
        total_py_files=scan_result.total_py_files,
        candidate_files=len(scan_result.candidate_files),
        scanned_files=len(scan_result.scan_files),
        call_records=len(scan_result.call_records),
        rg_candidates=scan_result.rg_candidates,
        scan_method="ast-grep" if not scan_result.used_fallback else "rg",
    )


def _summarize_sites(all_sites: list[CallSite]) -> CallAnalysisSummary:
    arg_shapes, kwarg_usage, forwarding_count, contexts, hazard_counts = _analyze_sites(all_sites)
    return CallAnalysisSummary(
        arg_shapes=arg_shapes,
        kwarg_usage=kwarg_usage,
        forwarding_count=forwarding_count,
        contexts=contexts,
        hazard_counts=hazard_counts,
    )


def _append_calls_findings(
    result: CqResult,
    ctx: CallsContext,
    scan_result: CallScanResult,
    analysis: CallAnalysisSummary,
    score: ScoreDetails | None,
) -> CqResult:
    function_name = ctx.function_name
    all_sites = scan_result.all_sites

    result = append_result_key_finding(
        result,
        Finding(
            category="summary",
            message=(
                f"Found {len(all_sites)} calls to {function_name} across "
                f"{scan_result.files_with_calls} files"
            ),
            severity="info",
            details=build_detail_payload(score=score),
        ),
    )

    if analysis.forwarding_count > 0:
        result = append_result_key_finding(
            result,
            Finding(
                category="forwarding",
                message=f"{analysis.forwarding_count} calls use *args/**kwargs forwarding",
                severity="warning",
                details=build_detail_payload(score=score),
            ),
        )

    result = _add_shape_section(result, analysis.arg_shapes, score)
    result = _add_kw_section(result, analysis.kwarg_usage, score)
    result = _add_context_section(result, analysis.contexts, score)
    result = _add_hazard_section(result, analysis.hazard_counts, score)
    return _add_sites_section(result, function_name, all_sites, score)


def _init_calls_result(
    ctx: CallsContext,
    scan_result: CallScanResult,
    *,
    started_ms: float,
) -> CqResult:
    builder = MacroResultBuilder(
        "calls",
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.tc,
        started_ms=started_ms,
    )
    summary_mapping = msgspec.to_builtins(
        _build_calls_summary(ctx.function_name, scan_result),
        order="deterministic",
    )
    summary_mapping_dict: dict[str, object]
    if not isinstance(summary_mapping, dict):
        summary_mapping_dict = {}
    else:
        summary_mapping_dict = {str(key): value for key, value in summary_mapping.items()}
    updated_summary = summary_from_mapping(summary_mapping_dict)
    return update_result_summary(builder.result, updated_summary.to_dict())


def _analyze_calls_sites(
    result: CqResult,
    *,
    ctx: CallsContext,
    scan_result: CallScanResult,
) -> tuple[CqResult, CallAnalysisSummary, ScoreDetails | None]:
    analysis = CallAnalysisSummary(
        arg_shapes=Counter(),
        kwarg_usage=Counter(),
        forwarding_count=0,
        contexts=Counter(),
        hazard_counts=Counter(),
    )
    score: ScoreDetails | None = None
    if not scan_result.all_sites:
        result = append_result_key_finding(
            result,
            Finding(
                category="info",
                message=f"No call sites found for '{ctx.function_name}'",
                severity="info",
            ),
        )
        return result, analysis, score
    analysis = _summarize_sites(scan_result.all_sites)
    score = _build_call_scoring(
        scan_result.all_sites,
        scan_result.files_with_calls,
        analysis.forwarding_count,
        analysis.hazard_counts,
        used_fallback=scan_result.used_fallback,
    )
    result = _append_calls_findings(result, ctx, scan_result, analysis, score)
    return result, analysis, score


def _build_calls_front_door_state(
    result: CqResult,
    *,
    ctx: CallsContext,
    analysis: CallAnalysisSummary,
    score: ScoreDetails | None,
) -> tuple[CqResult, CallsFrontDoorState]:
    from tools.cq.core.front_door_schema import InsightNeighborhoodV1
    from tools.cq.search.semantic.models import infer_language_for_path

    resolved_target_language = infer_target_language(ctx.root, ctx.function_name)
    metadata = resolve_target_metadata(
        AttachTargetMetadataRequestV1(
            root=ctx.root,
            function_name=ctx.function_name,
            score=score,
            preview_limit=CALLS_TARGET_CALLEE_PREVIEW,
            target_language=resolved_target_language,
            run_id=result.run.run_id,
        ),
    )
    result = apply_target_metadata(
        result,
        metadata,
        score=score,
        preview_limit=CALLS_TARGET_CALLEE_PREVIEW,
    )
    target_location = metadata.target_location
    target_callees = metadata.target_callees
    target_language_hint = metadata.resolved_language
    neighborhood, neighborhood_findings, degradation_notes = _build_calls_neighborhood(
        CallsNeighborhoodRequest(
            root=ctx.root,
            function_name=ctx.function_name,
            target_location=target_location,
            target_callees=target_callees,
            analysis=analysis,
            score=score,
            target_language=target_language_hint,
            preview_per_slice=FRONT_DOOR_PREVIEW_PER_SLICE,
        )
    )
    result = _attach_calls_neighborhood_section(result, neighborhood_findings)
    target_file_path, target_line = _target_file_path_and_line(ctx.root, target_location)
    target_language = (
        infer_language_for_path(target_file_path)
        if target_file_path is not None
        else target_language_hint
    )
    return result, CallsFrontDoorState(
        target_location=target_location,
        target_callees=target_callees,
        neighborhood=neighborhood
        if isinstance(neighborhood, InsightNeighborhoodV1)
        else InsightNeighborhoodV1(),
        degradation_notes=degradation_notes,
        target_file_path=target_file_path,
        target_line=target_line,
        target_language=target_language,
    )


def _attach_calls_neighborhood_section(
    result: CqResult, neighborhood_findings: list[Finding]
) -> CqResult:
    if neighborhood_findings:
        return insert_result_section(
            result,
            0,
            Section(title="Neighborhood Preview", findings=tuple(neighborhood_findings)),
        )
    return result


def _target_file_path_and_line(
    root: Path,
    target_location: tuple[str, int] | None,
) -> tuple[Path | None, int]:
    if target_location is None:
        return None, 1
    return root / target_location[0], int(target_location[1])


def _apply_calls_semantic_with_telemetry(
    result: CqResult,
    *,
    insight: FrontDoorInsightV1,
    state: CallsFrontDoorState,
    symbol_hint: str,
) -> tuple[
    FrontDoorInsightV1,
    tuple[int, int, int, int],
    tuple[str, ...],
    dict[str, object] | None,
]:
    semantic_outcome = _apply_calls_semantic(
        insight=insight,
        request=CallsSemanticRequest(
            root=Path(result.run.root),
            target_file_path=state.target_file_path,
            target_line=state.target_line,
            target_language=state.target_language,
            symbol_hint=symbol_hint,
            preview_per_slice=FRONT_DOOR_PREVIEW_PER_SLICE,
            run_id=result.run.run_id,
        ),
    )
    insight, _language, attempted, applied, failed, timed_out, reasons, semantic_planes = (
        semantic_outcome
    )
    return insight, (attempted, applied, failed, timed_out), reasons, semantic_planes


def _attach_calls_semantic_summary(
    result: CqResult,
    target_language: QueryLanguage | None,
    semantic_telemetry: tuple[int, int, int, int],
) -> CqResult:
    semantic_attempted, semantic_applied, semantic_failed, semantic_timed_out = semantic_telemetry
    return update_result_summary(
        result,
        {
            "python_semantic_telemetry": build_semantic_telemetry(
                attempted=semantic_attempted if target_language == "python" else 0,
                applied=semantic_applied if target_language == "python" else 0,
                failed=semantic_failed if target_language == "python" else 0,
                timed_out=semantic_timed_out if target_language == "python" else 0,
            ),
            "rust_semantic_telemetry": build_semantic_telemetry(
                attempted=semantic_attempted if target_language == "rust" else 0,
                applied=semantic_applied if target_language == "rust" else 0,
                failed=semantic_failed if target_language == "rust" else 0,
                timed_out=semantic_timed_out if target_language == "rust" else 0,
            ),
        },
    )


def _build_calls_result(
    ctx: CallsContext,
    scan_result: CallScanResult,
    *,
    started_ms: float,
) -> CqResult:
    from tools.cq.core.front_door_render import to_public_front_door_insight_dict

    result = _init_calls_result(ctx, scan_result, started_ms=started_ms)
    result, analysis, score = _analyze_calls_sites(result, ctx=ctx, scan_result=scan_result)
    result, state = _build_calls_front_door_state(result, ctx=ctx, analysis=analysis, score=score)
    confidence = _build_calls_confidence(score)
    summary_input = CallsInsightSummary(
        function_name=ctx.function_name,
        signature_info=scan_result.signature_info,
        files_with_calls=scan_result.files_with_calls,
        arg_shape_count=len(analysis.arg_shapes),
        forwarding_count=analysis.forwarding_count,
        hazard_counts=dict(analysis.hazard_counts),
    )
    insight = _build_calls_front_door_insight(
        summary=summary_input,
        confidence=confidence,
        state=state,
        used_fallback=scan_result.used_fallback,
    )
    insight, semantic_telemetry, semantic_reasons, semantic_planes = (
        _apply_calls_semantic_with_telemetry(
            result,
            insight=insight,
            state=state,
            symbol_hint=ctx.function_name.rsplit(".", maxsplit=1)[-1],
        )
    )
    if semantic_planes is not None:
        result = update_result_summary(result, {"semantic_planes": semantic_planes})
    result = _attach_calls_semantic_summary(result, state.target_language, semantic_telemetry)
    insight = _finalize_calls_semantic_state(
        insight=insight,
        summary=result.summary,
        target_language=state.target_language,
        top_level_attempted=semantic_telemetry[0],
        top_level_applied=semantic_telemetry[1],
        reasons=semantic_reasons,
    )
    return update_result_summary(
        result,
        {"front_door_insight": to_public_front_door_insight_dict(insight)},
    )


def build_calls_result(
    ctx: CallsContext,
    scan_result: CallScanResult,
    *,
    started_ms: float,
) -> CqResult:
    """Public output builder for calls macro result assembly.

    Returns:
        CqResult: Fully assembled calls macro result payload.
    """
    return _build_calls_result(ctx, scan_result, started_ms=started_ms)


def cmd_calls(request: CallsRequest) -> CqResult:
    """Census all call sites for a function.

    Parameters
    ----------
    request : CallsRequest
        Request contract for calls execution.

    Returns:
    -------
    CqResult
        Analysis result.
    """
    started = ms()
    ctx = CallsContext(
        tc=request.tc,
        root=Path(request.root),
        argv=request.argv,
        function_name=request.function_name,
    )
    logger.debug("Running calls macro function=%s root=%s", ctx.function_name, ctx.root)
    scan_result = _scan_call_sites(ctx.root, ctx.function_name)
    if scan_result.used_fallback:
        logger.warning("Calls macro used ripgrep fallback for function=%s", ctx.function_name)
    result = _build_calls_result(ctx, scan_result, started_ms=started)
    result = apply_rust_fallback_policy(
        result,
        root=ctx.root,
        policy=RustFallbackPolicyV1(
            macro_name="calls",
            pattern=ctx.function_name,
            query=ctx.function_name,
            fallback_matches_summary_key="total_sites",
        ),
    )
    result = assign_result_finding_ids(result)
    if result.run.run_id:
        maybe_evict_run_cache_tag(root=ctx.root, language="python", run_id=result.run.run_id)
        maybe_evict_run_cache_tag(root=ctx.root, language="rust", run_id=result.run.run_id)
    logger.debug(
        "Calls macro completed function=%s total_sites=%d",
        ctx.function_name,
        len(scan_result.all_sites),
    )
    return result


__all__ = [
    "build_calls_result",
    "cmd_calls",
    "scan_call_sites",
]
