"""Canonical enrichment telemetry accumulation helpers."""

from __future__ import annotations

from tools.cq.search._shared.helpers import coerce_count


def accumulate_stage_status(
    *,
    stages_bucket: dict[str, object],
    stage_status: object,
) -> None:
    """Accumulate per-stage status counters from one payload."""
    if not isinstance(stage_status, dict):
        return
    for stage_name, status in stage_status.items():
        if not isinstance(stage_name, str) or not isinstance(status, str):
            continue
        stage_bucket = stages_bucket.get(stage_name)
        if not isinstance(stage_bucket, dict):
            continue
        if status not in {"applied", "degraded", "skipped"}:
            continue
        stage_bucket[status] = coerce_count(stage_bucket.get(status, 0)) + 1


def accumulate_stage_timings(
    *,
    timings_bucket: dict[str, object],
    stage_timings_ms: object,
) -> None:
    """Accumulate per-stage elapsed timings in milliseconds."""
    if not isinstance(stage_timings_ms, dict):
        return
    for stage_name, elapsed_ms in stage_timings_ms.items():
        if not isinstance(stage_name, str):
            continue
        if isinstance(elapsed_ms, bool) or not isinstance(elapsed_ms, (int, float)):
            continue
        base = timings_bucket.get(stage_name)
        base_ms = (
            float(base) if isinstance(base, (int, float)) and not isinstance(base, bool) else 0.0
        )
        timings_bucket[stage_name] = base_ms + float(elapsed_ms)


def accumulate_rust_bundle_drift(
    *,
    lang_bucket: dict[str, object],
    bundle: object,
) -> None:
    """Accumulate Rust query-pack drift counters."""
    if not isinstance(bundle, dict):
        return
    if bool(bundle.get("distribution_included")):
        lang_bucket["distribution_profile_hits"] = (
            coerce_count(lang_bucket.get("distribution_profile_hits")) + 1
        )
    if bundle.get("drift_compatible") is False:
        lang_bucket["drift_breaking_profile_hits"] = (
            coerce_count(lang_bucket.get("drift_breaking_profile_hits")) + 1
        )
    schema_diff = bundle.get("drift_schema_diff")
    if not isinstance(schema_diff, dict):
        return
    removed_nodes = schema_diff.get("removed_node_kinds")
    removed_fields = schema_diff.get("removed_fields")
    if isinstance(removed_nodes, list):
        lang_bucket["drift_removed_node_kinds"] = coerce_count(
            lang_bucket.get("drift_removed_node_kinds")
        ) + len(removed_nodes)
    if isinstance(removed_fields, list):
        lang_bucket["drift_removed_fields"] = coerce_count(
            lang_bucket.get("drift_removed_fields")
        ) + len(removed_fields)


__all__ = [
    "accumulate_rust_bundle_drift",
    "accumulate_stage_status",
    "accumulate_stage_timings",
]
