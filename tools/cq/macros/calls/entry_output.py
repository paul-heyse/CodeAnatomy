"""Call-analysis output assembly helpers."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.schema import CqResult, update_result_summary
from tools.cq.core.summary_types import build_semantic_telemetry
from tools.cq.macros.calls.insight import (
    CallsInsightSummary,
    _build_calls_confidence,
    _build_calls_front_door_insight,
    _finalize_calls_semantic_state,
)
from tools.cq.macros.calls.semantic import CallsSemanticRequest, _apply_calls_semantic
from tools.cq.macros.constants import FRONT_DOOR_PREVIEW_PER_SLICE

if TYPE_CHECKING:
    from tools.cq.core.front_door_schema import FrontDoorInsightV1
    from tools.cq.core.types import QueryLanguage
    from tools.cq.macros.calls.entry_runtime import CallScanResult, CallsContext
    from tools.cq.macros.calls.insight import CallsFrontDoorState

__all__ = [
    "apply_calls_semantic_with_telemetry",
    "attach_calls_semantic_summary",
    "build_calls_result",
]


def apply_calls_semantic_with_telemetry(
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
    """Apply semantic overlays and return insight + telemetry tuple.

    Returns:
        tuple[FrontDoorInsightV1, tuple[int, int, int, int], tuple[str, ...], dict[str, object] | None]:
            Updated insight, semantic telemetry counters, degradation reasons, and optional plane payload.
    """
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


def attach_calls_semantic_summary(
    result: CqResult,
    target_language: QueryLanguage | None,
    semantic_telemetry: tuple[int, int, int, int],
) -> CqResult:
    """Attach typed semantic telemetry summary fields for calls results.

    Returns:
        CqResult: Result with language-specific semantic telemetry summary fields.
    """
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


def build_calls_result(
    ctx: CallsContext,
    scan_result: CallScanResult,
    *,
    started_ms: float,
) -> CqResult:
    """Build full calls result payload from scan and analysis stages.

    Returns:
        CqResult: Fully assembled calls result with front-door and semantic overlays.
    """
    from tools.cq.core.front_door_render import to_public_front_door_insight_dict
    from tools.cq.macros.calls import entry_runtime as runtime_impl

    result = runtime_impl.init_calls_result(ctx, scan_result, started_ms=started_ms)
    result, analysis, score = runtime_impl.analyze_calls_sites(
        result,
        ctx=ctx,
        scan_result=scan_result,
    )
    result, state = runtime_impl.build_calls_front_door_state(
        result,
        ctx=ctx,
        analysis=analysis,
        score=score,
    )
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
        apply_calls_semantic_with_telemetry(
            result,
            insight=insight,
            state=state,
            symbol_hint=ctx.function_name.rsplit(".", maxsplit=1)[-1],
        )
    )
    if semantic_planes is not None:
        result = update_result_summary(result, {"semantic_planes": semantic_planes})
    result = attach_calls_semantic_summary(result, state.target_language, semantic_telemetry)
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
