"""Run summary metadata population and aggregation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

from tools.cq.core.schema import CqResult
from tools.cq.query.language import (
    DEFAULT_QUERY_LANGUAGE_SCOPE,
    QueryLanguage,
    QueryLanguageScope,
    expand_language_scope,
)


def populate_run_summary_metadata(
    merged: CqResult,
    executed_results: list[tuple[str, CqResult]],
    *,
    total_steps: int,
) -> None:
    """Populate run summary metadata from executed step results.

    Parameters
    ----------
    merged : CqResult
        Merged result to populate.
    executed_results : list[tuple[str, CqResult]]
        List of (step_id, result) tuples.
    total_steps : int
        Total number of steps in the plan.
    """
    if not (
        isinstance(merged.summary.get("query"), str) and isinstance(merged.summary.get("mode"), str)
    ):
        mode, query = _derive_run_summary_metadata(executed_results, total_steps=total_steps)
        merged.summary.setdefault("mode", mode)
        merged.summary.setdefault("query", query)
    lang_scope, language_order = _derive_run_scope_metadata(executed_results)
    merged.summary.setdefault("lang_scope", lang_scope)
    merged.summary.setdefault("language_order", list(language_order))
    merged.summary.setdefault("python_semantic_overview", {})
    step_summaries = merged.summary.get("step_summaries")
    merged.summary["python_semantic_telemetry"] = _aggregate_run_semantic_telemetry(
        step_summaries,
        telemetry_key="python_semantic_telemetry",
    )
    merged.summary["rust_semantic_telemetry"] = _aggregate_run_semantic_telemetry(
        step_summaries,
        telemetry_key="rust_semantic_telemetry",
    )
    merged.summary["semantic_planes"] = _select_run_semantic_planes(
        step_summaries,
        step_order=merged.summary.get("steps"),
    )
    merged.summary.setdefault("python_semantic_diagnostics", [])


def _aggregate_run_semantic_telemetry(
    step_summaries: object,
    *,
    telemetry_key: str,
) -> dict[str, int]:
    totals = {
        "attempted": 0,
        "applied": 0,
        "failed": 0,
        "skipped": 0,
        "timed_out": 0,
    }
    if not isinstance(step_summaries, dict):
        return totals
    for step_summary in step_summaries.values():
        if not isinstance(step_summary, dict):
            continue
        raw = step_summary.get(telemetry_key)
        if not isinstance(raw, dict):
            continue
        for key in totals:
            value = raw.get(key)
            if isinstance(value, int):
                totals[key] += value
    return totals


def _semantic_plane_signal_score(payload: dict[str, object]) -> int:
    score = 0
    for value in payload.values():
        if isinstance(value, Mapping) or (
            isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))
        ):
            score += len(value)
        elif isinstance(value, str) and value:
            score += 1
        elif isinstance(value, int):
            score += max(0, value)
    return score


def _select_run_semantic_planes(
    step_summaries: object,
    *,
    step_order: object,
) -> dict[str, object]:
    if not isinstance(step_summaries, dict):
        return {}
    ordered_steps = (
        [step for step in step_order if isinstance(step, str)]
        if isinstance(step_order, list)
        else list(step_summaries)
    )
    best_planes: dict[str, object] = {}
    best_score = -1
    for step_id in ordered_steps:
        summary = step_summaries.get(step_id)
        if not isinstance(summary, dict):
            continue
        raw = summary.get("semantic_planes")
        if not isinstance(raw, dict) or not raw:
            continue
        score = _semantic_plane_signal_score(raw)
        if score > best_score:
            best_planes = dict(raw)
            best_score = score
    return best_planes


def _derive_run_summary_metadata(
    executed_results: list[tuple[str, CqResult]],
    *,
    total_steps: int,
) -> tuple[str, str]:
    if not executed_results:
        return "run", "multi-step plan (0 steps)"

    summaries = [result.summary for _, result in executed_results]
    modes = [
        value for summary in summaries if isinstance((value := summary.get("mode")), str) and value
    ]
    queries = [
        value for summary in summaries if isinstance((value := summary.get("query")), str) and value
    ]
    unique_modes = list(dict.fromkeys(modes))
    unique_queries = list(dict.fromkeys(queries))

    if len(executed_results) == 1:
        mode = unique_modes[0] if unique_modes else "run"
        query = unique_queries[0] if unique_queries else "multi-step plan (1 step)"
        return mode, query

    if len(unique_modes) == 1 and unique_modes[0] in {
        "entity",
        "pattern",
        "identifier",
        "regex",
        "literal",
    }:
        if unique_queries:
            joined_query = " | ".join(unique_queries)
            return unique_modes[0], joined_query
        return unique_modes[0], f"multi-step plan ({total_steps} steps)"

    return "run", f"multi-step plan ({total_steps} steps)"


def _derive_scope_from_orders(
    language_orders: list[tuple[QueryLanguage, ...]],
) -> QueryLanguageScope | None:
    languages = {lang for order in language_orders for lang in order}
    if languages == {"python"}:
        return "python"
    if languages == {"rust"}:
        return "rust"
    if languages:
        return "auto"
    return None


def _derive_run_scope_metadata(
    executed_results: list[tuple[str, CqResult]],
) -> tuple[QueryLanguageScope, tuple[QueryLanguage, ...]]:
    scopes: list[QueryLanguageScope] = []
    language_orders: list[tuple[QueryLanguage, ...]] = []
    for _, result in executed_results:
        summary = result.summary
        raw_scope = summary.get("lang_scope")
        if raw_scope in {"auto", "python", "rust"}:
            scopes.append(cast("QueryLanguageScope", raw_scope))
        raw_order = summary.get("language_order")
        if isinstance(raw_order, list):
            normalized: tuple[QueryLanguage, ...] = tuple(
                cast("QueryLanguage", item) for item in raw_order if item in {"python", "rust"}
            )
            if normalized:
                language_orders.append(normalized)

    unique_scopes = list(dict.fromkeys(scopes))
    if len(unique_scopes) == 1:
        scope = unique_scopes[0]
    elif len(unique_scopes) > 1:
        scope = DEFAULT_QUERY_LANGUAGE_SCOPE
    else:
        inferred = _derive_scope_from_orders(language_orders)
        scope = inferred if inferred is not None else DEFAULT_QUERY_LANGUAGE_SCOPE
    return scope, expand_language_scope(scope)
