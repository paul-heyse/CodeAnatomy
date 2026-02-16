"""Enrichment telemetry accumulation for smart search.

This module contains telemetry accumulation and statistics functions extracted
from smart_search.py for better modularity.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from tools.cq.search.enrichment.language_registry import get_language_adapter

if TYPE_CHECKING:
    from tools.cq.search.pipeline.smart_search_types import EnrichedMatch


def status_from_enrichment(payload: dict[str, object] | None) -> str:
    """Extract enrichment status from payload.

    Parameters
    ----------
    payload
        Enrichment payload.

    Returns:
    -------
    str
        Status string (applied, degraded, or skipped).
    """
    if payload is None:
        return "skipped"
    meta = payload.get("meta")
    status = None
    if isinstance(meta, dict):
        status = meta.get("enrichment_status")
    if status is None:
        status = payload.get("enrichment_status")
    if status in {"applied", "degraded", "skipped"}:
        return str(status)
    return "applied"


def empty_enrichment_telemetry() -> dict[str, object]:
    """Create empty enrichment telemetry structure.

    Returns:
    -------
    dict[str, object]
        Empty telemetry dictionary with initialized counters.
    """
    return {
        "python": {
            "applied": 0,
            "degraded": 0,
            "skipped": 0,
            "query_runtime": {
                "did_exceed_match_limit": 0,
                "cancelled": 0,
            },
            "stages": {
                "ast_grep": {"applied": 0, "degraded": 0, "skipped": 0},
                "python_ast": {"applied": 0, "degraded": 0, "skipped": 0},
                "import_detail": {"applied": 0, "degraded": 0, "skipped": 0},
                "python_resolution": {"applied": 0, "degraded": 0, "skipped": 0},
                "tree_sitter": {"applied": 0, "degraded": 0, "skipped": 0},
            },
            "timings_ms": {
                "ast_grep": 0.0,
                "python_ast": 0.0,
                "import_detail": 0.0,
                "python_resolution": 0.0,
                "tree_sitter": 0.0,
            },
        },
        "rust": {
            "applied": 0,
            "degraded": 0,
            "skipped": 0,
            "query_runtime": {
                "did_exceed_match_limit": 0,
                "cancelled": 0,
            },
            "query_pack_tags": 0,
            "distribution_profile_hits": 0,
            "drift_breaking_profile_hits": 0,
            "drift_removed_node_kinds": 0,
            "drift_removed_fields": 0,
        },
    }


def accumulate_stage_status(
    stages_bucket: dict[str, object], stage_status: dict[str, object]
) -> None:
    """Accumulate stage status counters.

    Parameters
    ----------
    stages_bucket
        Bucket to accumulate status into.
    stage_status
        Stage status to accumulate.
    """
    for stage, stage_state in stage_status.items():
        if not isinstance(stage, str) or not isinstance(stage_state, str):
            continue
        stage_bucket = stages_bucket.get(stage)
        if isinstance(stage_bucket, dict) and stage_state in {"applied", "degraded", "skipped"}:
            stage_bucket[stage_state] = int(stage_bucket.get(stage_state, 0)) + 1


def accumulate_stage_timings(
    timings_bucket: dict[str, object],
    stage_timings: dict[str, object],
) -> None:
    """Accumulate stage timing data.

    Parameters
    ----------
    timings_bucket
        Bucket to accumulate timings into.
    stage_timings
        Stage timings to accumulate.
    """
    for stage, stage_ms in stage_timings.items():
        if isinstance(stage, str) and isinstance(stage_ms, (int, float)):
            existing = timings_bucket.get(stage)
            base_ms = float(existing) if isinstance(existing, (int, float)) else 0.0
            timings_bucket[stage] = base_ms + float(stage_ms)


def accumulate_python_enrichment(
    lang_bucket: dict[str, object], payload: dict[str, object]
) -> None:
    """Accumulate Python enrichment telemetry.

    Parameters
    ----------
    lang_bucket
        Language bucket to accumulate into.
    payload
        Enrichment payload to accumulate from.
    """
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        return
    stage_status = meta.get("stage_status")
    stages_bucket = lang_bucket.get("stages")
    if isinstance(stage_status, dict) and isinstance(stages_bucket, dict):
        accumulate_stage_status(stages_bucket, stage_status)
    stage_timings = meta.get("stage_timings_ms")
    timings_bucket = lang_bucket.get("timings_ms")
    if isinstance(stage_timings, dict) and isinstance(timings_bucket, dict):
        accumulate_stage_timings(timings_bucket, stage_timings)
    runtime_payload = payload.get("query_runtime")
    runtime_bucket = lang_bucket.get("query_runtime")
    if isinstance(runtime_payload, dict) and isinstance(runtime_bucket, dict):
        if bool(runtime_payload.get("did_exceed_match_limit")):
            runtime_bucket["did_exceed_match_limit"] = (
                int(runtime_bucket.get("did_exceed_match_limit", 0)) + 1
            )
        if bool(runtime_payload.get("cancelled")):
            runtime_bucket["cancelled"] = int(runtime_bucket.get("cancelled", 0)) + 1


def attach_enrichment_cache_stats(telemetry: dict[str, object]) -> None:
    """Attach cache statistics to telemetry.

    Parameters
    ----------
    telemetry
        Telemetry dictionary to update with cache stats.
    """
    rust_bucket = telemetry.get("rust")
    if isinstance(rust_bucket, dict):
        from tools.cq.search.tree_sitter.rust_lane.runtime import get_tree_sitter_rust_cache_stats

        rust_bucket.update(get_tree_sitter_rust_cache_stats())
    python_bucket = telemetry.get("python")
    if isinstance(python_bucket, dict):
        from tools.cq.search.tree_sitter.python_lane.runtime import (
            get_tree_sitter_python_cache_stats,
        )

        python_bucket["tree_sitter_cache"] = get_tree_sitter_python_cache_stats()


def accumulate_rust_enrichment(
    lang_bucket: dict[str, object],
    payload: dict[str, object],
) -> None:
    """Accumulate Rust enrichment telemetry.

    Parameters
    ----------
    lang_bucket
        Language bucket to accumulate into.
    payload
        Enrichment payload to accumulate from.
    """

    def int_counter(value: object) -> int:
        """Convert value to an integer counter with a safe default.

        Returns:
            int: ``value`` when it is a non-bool ``int``; otherwise ``0``.
        """
        return value if isinstance(value, int) and not isinstance(value, bool) else 0

    tags = payload.get("query_pack_tags")
    if isinstance(tags, list):
        lang_bucket["query_pack_tags"] = int_counter(lang_bucket.get("query_pack_tags")) + len(tags)
    accumulate_rust_runtime(lang_bucket=lang_bucket, payload=payload, counter=int_counter)
    accumulate_rust_bundle(lang_bucket=lang_bucket, payload=payload, counter=int_counter)


def accumulate_rust_runtime(
    *,
    lang_bucket: dict[str, object],
    payload: dict[str, object],
    counter: Callable[[object], int],
) -> None:
    """Accumulate Rust runtime telemetry.

    Parameters
    ----------
    lang_bucket
        Language bucket to accumulate into.
    payload
        Enrichment payload to accumulate from.
    counter
        Counter function for int coercion.
    """
    runtime_payload = payload.get("query_runtime")
    runtime_bucket = lang_bucket.get("query_runtime")
    if not isinstance(runtime_payload, dict) or not isinstance(runtime_bucket, dict):
        return
    if bool(runtime_payload.get("did_exceed_match_limit")):
        runtime_bucket["did_exceed_match_limit"] = (
            counter(runtime_bucket.get("did_exceed_match_limit")) + 1
        )
    if bool(runtime_payload.get("cancelled")):
        runtime_bucket["cancelled"] = counter(runtime_bucket.get("cancelled")) + 1


def accumulate_rust_bundle(
    *,
    lang_bucket: dict[str, object],
    payload: dict[str, object],
    counter: Callable[[object], int],
) -> None:
    """Accumulate Rust bundle telemetry.

    Parameters
    ----------
    lang_bucket
        Language bucket to accumulate into.
    payload
        Enrichment payload to accumulate from.
    counter
        Counter function for int coercion.
    """
    bundle = payload.get("query_pack_bundle")
    if not isinstance(bundle, dict):
        return
    if bool(bundle.get("distribution_included")):
        lang_bucket["distribution_profile_hits"] = (
            counter(lang_bucket.get("distribution_profile_hits")) + 1
        )
    if bundle.get("drift_compatible") is False:
        lang_bucket["drift_breaking_profile_hits"] = (
            counter(lang_bucket.get("drift_breaking_profile_hits")) + 1
        )
    schema_diff = bundle.get("drift_schema_diff")
    if not isinstance(schema_diff, dict):
        return
    removed_nodes = schema_diff.get("removed_node_kinds")
    removed_fields = schema_diff.get("removed_fields")
    if isinstance(removed_nodes, list):
        lang_bucket["drift_removed_node_kinds"] = counter(
            lang_bucket.get("drift_removed_node_kinds")
        ) + len(removed_nodes)
    if isinstance(removed_fields, list):
        lang_bucket["drift_removed_fields"] = counter(
            lang_bucket.get("drift_removed_fields")
        ) + len(removed_fields)


def build_enrichment_telemetry(matches: list[EnrichedMatch]) -> dict[str, object]:
    """Build additive observability stats for enrichment stages.

    Parameters
    ----------
    matches
        List of enriched matches to build telemetry from.

    Returns:
    -------
    dict[str, object]
        Per-language enrichment status counters and Rust cache metrics.
    """
    from typing import cast

    telemetry: dict[str, object] = empty_enrichment_telemetry()
    for match in matches:
        adapter = get_language_adapter(match.language)
        if adapter is None:
            continue
        lang_bucket = telemetry.get(match.language)
        if not isinstance(lang_bucket, dict):
            continue
        payload = adapter.payload_from_match(match)
        status = status_from_enrichment(payload)
        lang_bucket[status] = cast("int", lang_bucket.get(status, 0)) + 1
        if payload is None:
            continue
        adapter.accumulate_telemetry(lang_bucket, payload)

    attach_enrichment_cache_stats(telemetry)
    return telemetry


def new_python_semantic_telemetry() -> dict[str, int]:
    """Create new Python semantic telemetry counters.

    Returns:
    -------
    dict[str, int]
        Initialized telemetry counters.
    """
    return {
        "attempted": 0,
        "applied": 0,
        "failed": 0,
        "skipped": 0,
        "timed_out": 0,
    }


__all__ = [
    "accumulate_python_enrichment",
    "accumulate_rust_bundle",
    "accumulate_rust_enrichment",
    "accumulate_rust_runtime",
    "accumulate_stage_status",
    "accumulate_stage_timings",
    "attach_enrichment_cache_stats",
    "build_enrichment_telemetry",
    "empty_enrichment_telemetry",
    "new_python_semantic_telemetry",
    "status_from_enrichment",
]
