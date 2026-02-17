"""Enrichment telemetry accumulation for smart search.

This module contains telemetry accumulation and statistics functions extracted
from smart_search.py for better modularity.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.search._shared.helpers import safe_int_counter
from tools.cq.search.enrichment.language_registry import get_language_adapter
from tools.cq.search.enrichment.telemetry import (
    accumulate_rust_bundle_drift as _accumulate_rust_bundle_drift,
)
from tools.cq.search.enrichment.telemetry import (
    accumulate_stage_status as _accumulate_stage_status,
)
from tools.cq.search.enrichment.telemetry import (
    accumulate_stage_timings as _accumulate_stage_timings,
)
from tools.cq.search.enrichment.telemetry_schema import default_enrichment_telemetry_mapping

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
    return default_enrichment_telemetry_mapping()


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
        _accumulate_stage_status(stages_bucket=stages_bucket, stage_status=stage_status)
    stage_timings = meta.get("stage_timings_ms")
    timings_bucket = lang_bucket.get("timings_ms")
    if isinstance(stage_timings, dict) and isinstance(timings_bucket, dict):
        _accumulate_stage_timings(timings_bucket=timings_bucket, stage_timings_ms=stage_timings)
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
    tags = payload.get("query_pack_tags")
    if isinstance(tags, list):
        lang_bucket["query_pack_tags"] = safe_int_counter(lang_bucket.get("query_pack_tags")) + len(
            tags
        )
    runtime_payload = payload.get("query_runtime")
    runtime_bucket = lang_bucket.get("query_runtime")
    if isinstance(runtime_payload, dict) and isinstance(runtime_bucket, dict):
        if bool(runtime_payload.get("did_exceed_match_limit")):
            runtime_bucket["did_exceed_match_limit"] = (
                safe_int_counter(runtime_bucket.get("did_exceed_match_limit")) + 1
            )
        if bool(runtime_payload.get("cancelled")):
            runtime_bucket["cancelled"] = safe_int_counter(runtime_bucket.get("cancelled")) + 1

    _accumulate_rust_bundle_drift(
        lang_bucket=lang_bucket,
        bundle=payload.get("query_pack_bundle"),
    )


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
    "accumulate_rust_enrichment",
    "attach_enrichment_cache_stats",
    "build_enrichment_telemetry",
    "empty_enrichment_telemetry",
    "new_python_semantic_telemetry",
    "status_from_enrichment",
]
