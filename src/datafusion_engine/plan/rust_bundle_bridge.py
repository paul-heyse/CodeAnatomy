"""Rust bridge helpers for plan bundle capture/build."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from datafusion_engine.extensions import datafusion_ext


def _call_bridge(
    bridge_fn: Any,
    ctx: Any,
    payload: Mapping[str, object],
    *,
    df: Any,
) -> Mapping[str, object]:
    try:
        return bridge_fn(ctx, payload, df=df)
    except TypeError:
        try:
            return bridge_fn(ctx, payload, _df=df)
        except TypeError:
            return bridge_fn(ctx, payload, df)


def capture_plan_bundle_runtime(
    ctx: Any,
    payload: Mapping[str, object],
    *,
    df: Any,
) -> Mapping[str, object]:
    """Capture plan bundle runtime payload via native extension bridge.

    Returns:
        Mapping[str, object]: Runtime payload returned from the native bridge.
    """
    return _call_bridge(
        datafusion_ext.capture_plan_bundle_runtime,
        ctx,
        payload,
        df=df,
    )


def build_plan_bundle_artifact_with_warnings(
    ctx: Any,
    payload: Mapping[str, object],
    *,
    df: Any,
) -> Mapping[str, object]:
    """Build plan bundle artifact payload via native extension bridge.

    Returns:
        Mapping[str, object]: Artifact payload returned from the native bridge.
    """
    return _call_bridge(
        datafusion_ext.build_plan_bundle_artifact_with_warnings,
        ctx,
        payload,
        df=df,
    )


__all__ = ["build_plan_bundle_artifact_with_warnings", "capture_plan_bundle_runtime"]
