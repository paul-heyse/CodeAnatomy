"""Rust bridge helpers for plan bundle capture/build."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec

from datafusion_engine.extensions import datafusion_ext


def capture_plan_bundle_runtime(
    ctx: object,
    payload: Mapping[str, object],
    *,
    df: object,
) -> Mapping[str, object]:
    """Capture plan bundle runtime payload via native extension bridge.

    Returns:
        Mapping[str, object]: Runtime payload returned from the native bridge.
    """
    return datafusion_ext.capture_plan_bundle_runtime(ctx, payload, df=df)


def build_plan_bundle_artifact_with_warnings(
    ctx: object,
    payload: Mapping[str, object],
    *,
    df: object,
) -> Mapping[str, object]:
    """Build plan bundle artifact payload via native extension bridge.

    Returns:
        Mapping[str, object]: Artifact payload returned from the native bridge.
    """
    return datafusion_ext.build_plan_bundle_artifact_with_warnings(ctx, payload, df=df)


def extract_lineage_from_plan(plan: object) -> Mapping[str, object]:
    """Extract structured lineage by delegating LogicalPlan analysis to Rust.

    Returns:
    -------
    Mapping[str, object]
        Decoded lineage payload mapping.

    Raises:
        TypeError: If the extension returns a non-string JSON payload or
            decodes to a non-mapping value.
    """
    raw_json = datafusion_ext.extract_lineage_json(plan)
    if not isinstance(raw_json, str):
        msg = "extract_lineage_json returned a non-string payload."
        raise TypeError(msg)
    payload = msgspec.json.decode(raw_json.encode("utf-8"))
    if not isinstance(payload, Mapping):
        msg = "extract_lineage_json returned a non-mapping payload."
        raise TypeError(msg)
    return {str(key): value for key, value in payload.items()}


__all__ = [
    "build_plan_bundle_artifact_with_warnings",
    "capture_plan_bundle_runtime",
    "extract_lineage_from_plan",
]
