"""Shared runtime capability snapshots for DataFusion extension diagnostics."""

from __future__ import annotations

import importlib
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from datafusion import SessionContext

from datafusion_engine.delta.capabilities import is_delta_extension_compatible
from datafusion_engine.extensions.plugin_manifest import resolve_plugin_manifest


@dataclass(frozen=True)
class DeltaCompatibilitySnapshot:
    """Delta extension compatibility diagnostics."""

    entrypoint: str
    module: str | None
    ctx_kind: str | None
    probe_result: str | None
    compatible: bool
    available: bool
    error: str | None


@dataclass(frozen=True)
class ExtensionPluginSnapshot:
    """Plugin-manifest and capabilities snapshot diagnostics."""

    manifest: Mapping[str, object] | None
    capabilities_snapshot: Mapping[str, object] | None
    error: str | None


@dataclass(frozen=True)
class RuntimeCapabilitiesSnapshot:
    """Runtime capabilities payload assembled from extension diagnostics."""

    event_time_unix_ms: int
    profile_name: str | None
    settings_hash: str
    strict_native_provider_enabled: bool
    delta: DeltaCompatibilitySnapshot
    extension_capabilities: Mapping[str, object]
    plugin: ExtensionPluginSnapshot
    execution_metrics: Mapping[str, object] | None


def collect_delta_compatibility(
    ctx: SessionContext,
    *,
    require_non_fallback: bool,
) -> DeltaCompatibilitySnapshot:
    """Collect Delta extension compatibility diagnostics.

    Returns:
    -------
    DeltaCompatibilitySnapshot
        Compatibility status and probe metadata for the Delta extension.
    """
    compatibility = is_delta_extension_compatible(
        ctx,
        entrypoint="delta_table_provider_from_session",
        require_non_fallback=require_non_fallback,
    )
    return DeltaCompatibilitySnapshot(
        entrypoint=compatibility.entrypoint,
        module=compatibility.module,
        ctx_kind=compatibility.ctx_kind,
        probe_result=compatibility.probe_result,
        compatible=compatibility.compatible,
        available=compatibility.available,
        error=compatibility.error,
    )


def collect_extension_plugin_snapshot(
    module_name: str = "datafusion_ext",
) -> ExtensionPluginSnapshot:
    """Collect plugin-manifest and capability snapshot diagnostics.

    Returns:
    -------
    ExtensionPluginSnapshot
        Plugin manifest data, capability snapshot payload, and probe errors.
    """
    plugin_resolution = resolve_plugin_manifest(module_name)
    plugin_error = plugin_resolution.error
    capabilities_snapshot, import_error = _capabilities_snapshot_from_module(module_name)
    if plugin_error is None and import_error is not None:
        plugin_error = import_error
    return ExtensionPluginSnapshot(
        manifest=plugin_resolution.manifest,
        capabilities_snapshot=capabilities_snapshot,
        error=plugin_error,
    )


def collect_runtime_execution_metrics(
    ctx: SessionContext,
    *,
    module_name: str = "datafusion_ext",
) -> Mapping[str, object] | None:
    """Collect runtime execution metrics from the extension when available.

    Returns:
    -------
    Mapping[str, object] | None
        Structured metrics payload, or None when unavailable.
    """
    try:
        module = importlib.import_module(module_name)
    except ImportError:
        return None
    snapshot_fn = getattr(module, "runtime_execution_metrics_snapshot", None)
    if not callable(snapshot_fn):
        return None
    payload, error = _invoke_extension_with_context(snapshot_fn, ctx)
    if isinstance(payload, Mapping):
        return dict(payload)
    if error is None:
        return None
    return {"error": error}


def build_runtime_capabilities_snapshot(
    ctx: SessionContext,
    *,
    profile_name: str | None,
    settings_hash: str,
    strict_native_provider_enabled: bool,
    event_time_unix_ms: int | None = None,
) -> RuntimeCapabilitiesSnapshot:
    """Build the runtime capabilities snapshot payload.

    Returns:
    -------
    RuntimeCapabilitiesSnapshot
        Canonical runtime-capabilities snapshot payload.
    """
    if event_time_unix_ms is None:
        event_time_unix_ms = int(time.time() * 1000)
    return RuntimeCapabilitiesSnapshot(
        event_time_unix_ms=event_time_unix_ms,
        profile_name=profile_name,
        settings_hash=settings_hash,
        strict_native_provider_enabled=strict_native_provider_enabled,
        delta=collect_delta_compatibility(
            ctx,
            require_non_fallback=strict_native_provider_enabled,
        ),
        extension_capabilities=_extension_capabilities_report(),
        plugin=collect_extension_plugin_snapshot(),
        execution_metrics=collect_runtime_execution_metrics(ctx),
    )


def runtime_capabilities_payload(snapshot: RuntimeCapabilitiesSnapshot) -> dict[str, object]:
    """Return a legacy-compatible runtime capabilities payload mapping.

    Returns:
    -------
    dict[str, object]
        Event payload emitted to diagnostics sinks.
    """
    return {
        "event_time_unix_ms": snapshot.event_time_unix_ms,
        "profile_name": snapshot.profile_name,
        "settings_hash": snapshot.settings_hash,
        "strict_native_provider_enabled": snapshot.strict_native_provider_enabled,
        "delta_entrypoint": snapshot.delta.entrypoint,
        "delta_module": snapshot.delta.module,
        "delta_ctx_kind": snapshot.delta.ctx_kind,
        "delta_probe_result": snapshot.delta.probe_result,
        "delta_compatible": snapshot.delta.compatible,
        "delta_available": snapshot.delta.available,
        "delta_error": snapshot.delta.error,
        "extension_capabilities": dict(snapshot.extension_capabilities),
        "plugin_manifest": dict(snapshot.plugin.manifest)
        if snapshot.plugin.manifest is not None
        else None,
        "capabilities_snapshot": dict(snapshot.plugin.capabilities_snapshot)
        if snapshot.plugin.capabilities_snapshot is not None
        else None,
        "plugin_error": snapshot.plugin.error,
        "execution_metrics": (
            dict(snapshot.execution_metrics)
            if isinstance(snapshot.execution_metrics, Mapping)
            else None
        ),
    }


def _extension_capabilities_report() -> dict[str, object]:
    try:
        from datafusion_engine.udf.runtime import extension_capabilities_report
    except ImportError:
        return {}
    try:
        payload = extension_capabilities_report()
    except (RuntimeError, TypeError, ValueError):
        return {}
    if isinstance(payload, Mapping):
        return dict(payload)
    return {}


def _capabilities_snapshot_from_module(
    module_name: str,
) -> tuple[Mapping[str, object] | None, str | None]:
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        return None, str(exc)
    snapshot_fn = getattr(module, "capabilities_snapshot", None)
    if not callable(snapshot_fn):
        return None, None
    try:
        payload = snapshot_fn()
    except (RuntimeError, TypeError, ValueError):
        return None, None
    if isinstance(payload, Mapping):
        return dict(payload), None
    return None, None


def _invoke_extension_with_context(
    fn: Callable[[object], object],
    ctx: SessionContext,
) -> tuple[object | None, str | None]:
    candidates = [ctx, getattr(ctx, "ctx", None)]
    error: str | None = None
    for candidate in candidates:
        if candidate is None:
            continue
        try:
            return fn(candidate), None
        except (RuntimeError, TypeError, ValueError) as exc:
            error = str(exc)
            continue
    return None, error


__all__ = [
    "DeltaCompatibilitySnapshot",
    "ExtensionPluginSnapshot",
    "RuntimeCapabilitiesSnapshot",
    "build_runtime_capabilities_snapshot",
    "collect_delta_compatibility",
    "collect_extension_plugin_snapshot",
    "runtime_capabilities_payload",
]
