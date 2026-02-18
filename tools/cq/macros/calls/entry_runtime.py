"""Runtime ownership module for calls macro assembly.

Holds scan/analyze/front-door/result-state helpers so ``entry.py`` can remain
command-boundary only.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.schema import (
    CqResult,
    Finding,
    ScoreDetails,
    Section,
    append_result_key_finding,
    insert_result_section,
    update_result_summary,
)
from tools.cq.core.scoring import build_detail_payload
from tools.cq.core.summary_update_contracts import summary_update_mapping
from tools.cq.macros.calls.analysis import CallSite
from tools.cq.macros.calls.analysis_scan import analyze_sites as _analyze_sites
from tools.cq.macros.calls.entry_dispatch import scan_call_sites as _scan_call_sites_impl
from tools.cq.macros.calls.entry_summary import build_calls_summary
from tools.cq.macros.calls.insight import (
    CallsFrontDoorState,
    _add_context_section,
    _add_hazard_section,
    _add_kw_section,
    _add_shape_section,
    _add_sites_section,
    _build_call_scoring,
)
from tools.cq.macros.calls.neighborhood import (
    CallAnalysisSummary,
    CallsNeighborhoodRequest,
    _build_calls_neighborhood,
)
from tools.cq.macros.calls.target_runtime import (
    AttachTargetMetadataRequestV1,
    apply_target_metadata,
    infer_target_language,
    resolve_target_metadata,
)
from tools.cq.macros.constants import (
    CALLS_TARGET_CALLEE_PREVIEW,
    FRONT_DOOR_PREVIEW_PER_SLICE,
)
from tools.cq.query.sg_parser import SgRecord

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain


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


def scan_call_sites(root_path: Path, function_name: str) -> CallScanResult:
    """Scan for call sites using ast-grep, with ripgrep fallback.

    Returns:
        Scan output bundle with candidates, records, and discovered call sites.
    """
    return _scan_call_sites_impl(root_path, function_name)


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


def init_calls_result(
    ctx: CallsContext,
    scan_result: CallScanResult,
    *,
    started_ms: float,
) -> CqResult:
    """Initialize calls result with typed summary updates.

    Returns:
        Result skeleton with run metadata and summary counters.
    """
    from tools.cq.macros.result_builder import MacroResultBuilder

    builder = MacroResultBuilder(
        "calls",
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.tc,
        started_ms=started_ms,
    )
    return update_result_summary(
        builder.result,
        summary_update_mapping(
            build_calls_summary(
                function_name=ctx.function_name,
                signature=scan_result.signature_info,
                total_sites=len(scan_result.all_sites),
                files_with_calls=scan_result.files_with_calls,
                total_py_files=scan_result.total_py_files,
                candidate_files=len(scan_result.candidate_files),
                scanned_files=len(scan_result.scan_files),
                call_records=len(scan_result.call_records),
                rg_candidates=scan_result.rg_candidates,
                used_fallback=scan_result.used_fallback,
            )
        ),
    )


def analyze_calls_sites(
    result: CqResult,
    *,
    ctx: CallsContext,
    scan_result: CallScanResult,
) -> tuple[CqResult, CallAnalysisSummary, ScoreDetails | None]:
    """Analyze discovered call sites and append findings.

    Returns:
        Updated result, aggregate call analysis summary, and scoring payload.
    """
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


def build_calls_front_door_state(
    result: CqResult,
    *,
    ctx: CallsContext,
    analysis: CallAnalysisSummary,
    score: ScoreDetails | None,
) -> tuple[CqResult, CallsFrontDoorState]:
    """Build neighborhood/target front-door state for calls macro.

    Returns:
        Updated result and front-door state consumed by output assembly.
    """
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


__all__ = [
    "CallScanResult",
    "CallsContext",
    "analyze_calls_sites",
    "build_calls_front_door_state",
    "init_calls_result",
    "scan_call_sites",
]
