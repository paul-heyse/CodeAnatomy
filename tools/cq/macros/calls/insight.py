"""Insight card building and result construction.

Assembles front-door insights, scoring, and telemetry for call analysis results.
"""

from __future__ import annotations

import ast
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    ScoreDetails,
    Section,
    append_result_section,
)
from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    build_detail_payload,
    build_score_details,
)
from tools.cq.core.summary_contract import SummaryEnvelopeV1, coerce_semantic_telemetry
from tools.cq.macros.constants import FRONT_DOOR_PREVIEW_PER_SLICE
from tools.cq.search.pipeline.profiles import INTERACTIVE
from tools.cq.search.rg.adapter import FilePatternSearchOptions, find_files_with_pattern

if TYPE_CHECKING:
    from tools.cq.core.front_door_contracts import (
        FrontDoorInsightV1,
        InsightConfidenceV1,
        InsightNeighborhoodV1,
    )
    from tools.cq.core.types import QueryLanguage
    from tools.cq.macros.calls.analysis import CallSite

_FRONT_DOOR_TOP_CANDIDATES = 3


@dataclass(frozen=True)
class CallsFrontDoorState:
    """Computed front-door state for calls insight assembly."""

    target_location: tuple[str, int] | None
    target_callees: Counter[str]
    neighborhood: InsightNeighborhoodV1
    degradation_notes: list[str]
    target_file_path: Path | None
    target_line: int
    target_language: QueryLanguage | None


@dataclass(frozen=True)
class CallsInsightSummary:
    """Summary counters and identifiers needed to build calls front-door insight."""

    function_name: str
    signature_info: str
    files_with_calls: int
    arg_shape_count: int
    forwarding_count: int
    hazard_counts: dict[str, int]


def _find_function_signature(
    root: Path,
    function_name: str,
) -> str:
    """Find function signature on-demand using ripgrep.

    Avoids full repo scan by finding only the file containing the
    function definition and parsing just that file.

    Parameters
    ----------
    root : Path
        Repository root.
    function_name : str
        Name of the function to find.

    Returns:
    -------
    str
        Signature string like "(x, y, z)" or empty string if not found.
    """
    # Extract base name (handle qualified names like "MyClass.method")
    base_name = function_name.rsplit(".", maxsplit=1)[-1]

    # Find files containing the function definition
    pattern = rf"\bdef {base_name}\s*\("
    def_files = find_files_with_pattern(
        root,
        pattern,
        options=FilePatternSearchOptions(limits=INTERACTIVE),
    )

    if not def_files:
        return ""

    # Parse first matching file and extract parameters
    for filepath in def_files:
        try:
            source = filepath.read_text(encoding="utf-8")
            tree = ast.parse(source)
        except (SyntaxError, OSError, UnicodeDecodeError):
            continue

        # Walk AST to find matching function
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == base_name:
                params = [arg.arg for arg in node.args.args]
                return f"({', '.join(params)})"

    return ""


def _build_call_scoring(
    all_sites: list[CallSite],
    files_with_calls: int,
    forwarding_count: int,
    hazard_counts: dict[str, int],
    *,
    used_fallback: bool,
) -> ScoreDetails | None:
    """Compute scoring details for call-site findings.

    Returns:
    -------
    dict[str, object]
        Scoring details for impact/confidence.
    """
    hazard_sites = sum(hazard_counts.values())
    imp_signals = ImpactSignals(
        sites=len(all_sites),
        files=files_with_calls,
        depth=0,
        breakages=0,
        ambiguities=forwarding_count,
    )
    has_closures = any(
        site.symtable_info and site.symtable_info.get("is_closure") for site in all_sites
    )
    has_bytecode = any(site.bytecode_info for site in all_sites)
    if used_fallback:
        evidence_kind = "rg_only"
    elif has_closures:
        evidence_kind = "resolved_ast_closure"
    elif forwarding_count or hazard_sites:
        evidence_kind = "resolved_ast_heuristic"
    elif has_bytecode:
        evidence_kind = "resolved_ast_bytecode"
    else:
        evidence_kind = "resolved_ast"
    conf_signals = ConfidenceSignals(evidence_kind=evidence_kind)
    return build_score_details(
        impact=imp_signals,
        confidence=conf_signals,
    )


def _add_shape_section(
    result: CqResult,
    arg_shapes: Counter[str],
    score: ScoreDetails | None,
) -> CqResult:
    shape_section = Section(title="Argument Shape Histogram")
    for shape, count in arg_shapes.most_common(10):
        shape_section.findings.append(
            Finding(
                category="shape",
                message=f"{shape}: {count} calls",
                severity="info",
                details=build_detail_payload(score=score),
            )
        )
    return append_result_section(result, shape_section)


def _add_kw_section(
    result: CqResult,
    kwarg_usage: Counter[str],
    score: ScoreDetails | None,
) -> CqResult:
    if not kwarg_usage:
        return result
    kw_section = Section(title="Keyword Argument Usage")
    for kw, count in kwarg_usage.most_common(15):
        kw_section.findings.append(
            Finding(
                category="kwarg",
                message=f"{kw}: {count} uses",
                severity="info",
                details=build_detail_payload(score=score),
            )
        )
    return append_result_section(result, kw_section)


def _add_context_section(
    result: CqResult,
    contexts: Counter[str],
    score: ScoreDetails | None,
) -> CqResult:
    ctx_section = Section(title="Calling Contexts")
    for ctx, count in contexts.most_common(10):
        ctx_section.findings.append(
            Finding(
                category="context",
                message=f"{ctx}: {count} calls",
                severity="info",
                details=build_detail_payload(score=score),
            )
        )
    return append_result_section(result, ctx_section)


def _add_hazard_section(
    result: CqResult,
    hazard_counts: Counter[str],
    score: ScoreDetails | None,
) -> CqResult:
    if not hazard_counts:
        return result
    hazard_section = Section(title="Hazards")
    for label, count in hazard_counts.most_common():
        hazard_section.findings.append(
            Finding(
                category="hazard",
                message=f"{label}: {count} calls",
                severity="warning",
                details=build_detail_payload(score=score),
            )
        )
    return append_result_section(result, hazard_section)


def _add_sites_section(
    result: CqResult,
    function_name: str,
    all_sites: list[CallSite],
    score: ScoreDetails | None,
) -> CqResult:
    """Add call sites section to result.

    Parameters
    ----------
    result : CqResult
        The result to add to.
    function_name : str
        Name of the function.
    all_sites : list[CallSite]
        All call sites.
    score : ScoreDetails | None
        Scoring metadata.

    Returns:
    -------
    CqResult
        Result with call-site section appended.
    """
    sites_section = Section(title="Call Sites")
    for site in all_sites:
        details = {
            "context": site.context,
            "callee": site.callee,
            "receiver": site.receiver,
            "resolution_confidence": site.resolution_confidence,
            "resolution_path": site.resolution_path,
            "binding": site.binding,
            "target_names": site.target_names,
            "num_args": site.num_args,
            "num_kwargs": site.num_kwargs,
            "kwargs": site.kwargs,
            "has_star_args": site.has_star_args,
            "has_star_kwargs": site.has_star_kwargs,
            "forwarding": site.has_star_args or site.has_star_kwargs,
            "call_id": site.call_id,
            "hazards": site.hazards,
            "context_window": site.context_window,
            "context_snippet": site.context_snippet,
            "symtable": site.symtable_info,
            "bytecode": site.bytecode_info,
        }
        sites_section.findings.append(
            Finding(
                category="call",
                message=f"{function_name}({site.arg_preview})",
                anchor=Anchor(file=site.file, line=site.line, col=site.col),
                severity="info",
                details=build_detail_payload(score=score, data=details),
            )
        )
    return append_result_section(result, sites_section)


def _build_calls_confidence(score: ScoreDetails | None) -> InsightConfidenceV1:
    from tools.cq.core.front_door_contracts import InsightConfidenceV1

    return InsightConfidenceV1(
        evidence_kind=(score.evidence_kind if score and score.evidence_kind else "resolved_ast"),
        score=float(score.confidence_score)
        if score and score.confidence_score is not None
        else 0.0,
        bucket=score.confidence_bucket if score and score.confidence_bucket else "low",
    )


def _build_calls_front_door_insight(
    *,
    summary: CallsInsightSummary,
    confidence: InsightConfidenceV1,
    state: CallsFrontDoorState,
    used_fallback: bool,
) -> FrontDoorInsightV1:
    from tools.cq.core.front_door_assembly import (
        build_calls_insight,
    )
    from tools.cq.core.front_door_contracts import (
        CallsInsightBuildRequestV1,
        InsightBudgetV1,
        InsightDegradationV1,
        InsightLocationV1,
    )
    from tools.cq.core.semantic_contracts import (
        SemanticContractStateInputV1,
        derive_semantic_contract_state,
    )
    from tools.cq.search.semantic.models import provider_for_language

    semantic_provider = (
        provider_for_language(state.target_language) if state.target_language else "none"
    )
    semantic_available = state.target_language in {"python", "rust"}
    location = (
        InsightLocationV1(file=state.target_location[0], line=state.target_location[1], col=0)
        if state.target_location is not None
        else None
    )
    return build_calls_insight(
        CallsInsightBuildRequestV1(
            function_name=summary.function_name,
            signature=summary.signature_info or None,
            location=location,
            neighborhood=state.neighborhood,
            files_with_calls=summary.files_with_calls,
            arg_shape_count=summary.arg_shape_count,
            forwarding_count=summary.forwarding_count,
            hazard_counts=summary.hazard_counts,
            confidence=confidence,
            budget=InsightBudgetV1(
                top_candidates=_FRONT_DOOR_TOP_CANDIDATES,
                preview_per_slice=FRONT_DOOR_PREVIEW_PER_SLICE,
                semantic_targets=1,
            ),
            degradation=InsightDegradationV1(
                semantic=derive_semantic_contract_state(
                    SemanticContractStateInputV1(
                        provider=semantic_provider,
                        available=semantic_available,
                    )
                ).status,
                scan="fallback" if used_fallback else "ok",
                scope_filter="none",
                notes=tuple(state.degradation_notes),
            ),
        )
    )


def _finalize_calls_semantic_state(
    *,
    insight: FrontDoorInsightV1,
    summary: SummaryEnvelopeV1,
    target_language: QueryLanguage | None,
    top_level_attempted: int,
    top_level_applied: int,
    reasons: tuple[str, ...],
) -> FrontDoorInsightV1:
    import msgspec

    from tools.cq.core.semantic_contracts import (
        SemanticContractStateInputV1,
        derive_semantic_contract_state,
    )
    from tools.cq.search.semantic.models import provider_for_language

    telemetry_key = (
        "python_semantic_telemetry" if target_language == "python" else "rust_semantic_telemetry"
    )
    telemetry = coerce_semantic_telemetry(getattr(summary, telemetry_key))
    attempted = 0
    applied = 0
    failed = 0
    timed_out = 0
    if telemetry is not None:
        attempted = telemetry.attempted
        applied = telemetry.applied
        failed = telemetry.failed
        timed_out = telemetry.timed_out

    provider = provider_for_language(target_language) if target_language else "none"
    state_reasons = list(reasons)
    if provider == "none":
        state_reasons.append("provider_unavailable")
    elif attempted <= 0 and "not_attempted_runtime_disabled" not in state_reasons:
        state_reasons.append("not_attempted_by_design")
    if top_level_attempted > 0 and top_level_applied <= 0 and applied > 0:
        state_reasons.append("top_target_failed")

    semantic_state = derive_semantic_contract_state(
        SemanticContractStateInputV1(
            provider=provider,
            available=provider != "none",
            attempted=attempted,
            applied=applied,
            failed=max(failed, attempted - applied if attempted > applied else 0),
            timed_out=timed_out,
            reasons=tuple(dict.fromkeys(state_reasons)),
        )
    )
    return msgspec.structs.replace(
        insight,
        degradation=msgspec.structs.replace(
            insight.degradation,
            semantic=semantic_state.status,
            notes=tuple(dict.fromkeys([*insight.degradation.notes, *semantic_state.reasons])),
        ),
    )


__all__ = [
    "CallsFrontDoorState",
]
