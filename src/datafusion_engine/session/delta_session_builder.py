"""Delta session builder helpers for session-factory orchestration."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from datafusion import RuntimeEnvBuilder, SessionContext

from datafusion_engine.session.runtime_config_policies import (
    planning_surface_policy_identity_for_profile,
)

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

_RUNTIME_PREFIX = "datafusion.runtime."
_RUNTIME_SIZE_SUFFIXES: Mapping[str, int] = {
    "kb": 1024,
    "k": 1024,
    "mb": 1024 * 1024,
    "m": 1024 * 1024,
    "gb": 1024 * 1024 * 1024,
    "g": 1024 * 1024 * 1024,
    "tb": 1024 * 1024 * 1024 * 1024,
    "t": 1024 * 1024 * 1024 * 1024,
}
_RUNTIME_DURATION_SUFFIXES: Mapping[str, int] = {
    "s": 1,
    "m": 60,
    "h": 60 * 60,
    "d": 24 * 60 * 60,
}


@dataclass(frozen=True)
class DeltaRuntimePolicyBridgeResult:
    """Resolved runtime-policy options and bridge diagnostics payload."""

    options: object | None
    payload: dict[str, object] | None


@dataclass(frozen=True)
class DeltaSessionBuilderResolution:
    """Resolved Delta session builder function and module diagnostics."""

    available: bool
    builder: Callable[..., SessionContext] | None
    module_name: str | None
    module_owner: object | None
    error: str | None
    cause: Exception | None


@dataclass(frozen=True)
class DeltaSessionBuildResult:
    """Session build outcome for Delta session-default construction."""

    ctx: SessionContext | None
    available: bool
    installed: bool
    error: str | None
    cause: Exception | None
    runtime_policy_bridge: Mapping[str, object] | None = None


def split_runtime_settings(
    settings: Mapping[str, str],
) -> tuple[dict[str, str], dict[str, str]]:
    """Split settings into runtime-prefixed and non-runtime payloads.

    Returns:
    -------
    tuple[dict[str, str], dict[str, str]]
        Non-runtime settings followed by runtime-prefixed settings.
    """
    non_runtime: dict[str, str] = {}
    runtime: dict[str, str] = {}
    for key, value in settings.items():
        target = runtime if key.startswith(_RUNTIME_PREFIX) else non_runtime
        target[key] = value
    return non_runtime, runtime


def parse_runtime_size(value: object) -> int | None:
    """Parse a human-readable runtime-size value into bytes.

    Returns:
    -------
    int | None
        Parsed byte value, or ``None`` when the value is invalid.
    """
    if not isinstance(value, str):
        return None
    stripped = value.strip().lower().replace("_", "")
    if not stripped:
        return None
    if stripped.isdigit():
        return int(stripped)
    return _parse_suffixed_runtime_size(stripped)


def parse_runtime_duration_seconds(value: object) -> int | None:
    """Parse a runtime duration value into whole seconds.

    Accepted formats:
    - integer seconds (``"30"``)
    - suffixed values (``"30s"``, ``"2m"``, ``"1h"``, ``"1d"``)

    Returns:
    -------
    int | None
        Parsed duration in seconds, or ``None`` when invalid.
    """
    stripped = _normalized_runtime_value(value)
    if stripped is None:
        return None
    if stripped.isdigit():
        return int(stripped)
    return _parse_suffixed_runtime_duration_seconds(stripped)


def resolve_delta_session_builder(
    module_names: tuple[str, ...] = ("datafusion_engine.extensions.datafusion_ext",),
) -> DeltaSessionBuilderResolution:
    """Resolve an installed Delta session builder with diagnostics.

    Returns:
    -------
    DeltaSessionBuilderResolution
        Module resolution diagnostics and callable builder when available.
    """
    available = True
    error: str | None = None
    cause: Exception | None = None
    for module_name in module_names:
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            available = False
            error = str(exc)
            cause = exc
            continue
        builder = getattr(module, "delta_session_context", None)
        if callable(builder):
            return DeltaSessionBuilderResolution(
                available=available,
                builder=cast("Callable[..., SessionContext]", builder),
                module_name=module_name,
                module_owner=module,
                error=None,
                cause=None,
            )
        error = f"{module_name}.delta_session_context is unavailable."
        cause = TypeError(error)
    return DeltaSessionBuilderResolution(
        available=available,
        builder=None,
        module_name=None,
        module_owner=None,
        error=error,
        cause=cause,
    )


def build_runtime_policy_options(
    module: object | None,
    runtime_settings: Mapping[str, str],
) -> DeltaRuntimePolicyBridgeResult:
    """Build runtime-policy options object and bridge metadata payload.

    Returns:
    -------
    DeltaRuntimePolicyBridgeResult
        Resolved options object and bridge diagnostics payload.
    """
    if module is None or not runtime_settings:
        return DeltaRuntimePolicyBridgeResult(options=None, payload=None)
    options_cls = getattr(module, "DeltaSessionRuntimePolicyOptions", None)
    if not callable(options_cls):
        return DeltaRuntimePolicyBridgeResult(
            options=None,
            payload={
                "enabled": False,
                "reason": "options_type_unavailable",
                "runtime_settings_seen": dict(runtime_settings),
            },
        )
    options = options_cls()
    consumed: dict[str, object] = {}
    unsupported: dict[str, str] = {}
    handled_keys = _apply_cache_runtime_policy_settings(
        options=options,
        runtime_settings=runtime_settings,
        consumed=consumed,
        unsupported=unsupported,
    )
    _apply_standard_runtime_policy_settings(
        options=options,
        runtime_settings=runtime_settings,
        handled_keys=handled_keys,
        consumed=consumed,
        unsupported=unsupported,
    )
    return _runtime_policy_bridge_result(
        options=options,
        runtime_settings=runtime_settings,
        consumed=consumed,
        unsupported=unsupported,
    )


def build_delta_session_context(
    profile: DataFusionRuntimeProfile,
    runtime_env: RuntimeEnvBuilder,
) -> DeltaSessionBuildResult:
    """Build a SessionContext via Delta extension entrypoints when available.

    Returns:
    -------
    DeltaSessionBuildResult
        Session construction result with runtime-policy bridge diagnostics.
    """
    from datafusion_engine.session.runtime_ops import delta_runtime_env_options

    resolution = resolve_delta_session_builder()
    if resolution.builder is None:
        return DeltaSessionBuildResult(
            ctx=None,
            available=resolution.available,
            installed=False,
            error=resolution.error,
            cause=resolution.cause,
        )
    bridge = DeltaRuntimePolicyBridgeResult(options=None, payload=None)
    planning_surface_policy_identity = planning_surface_policy_identity_for_profile(profile)
    try:
        settings = profile.settings_payload()
        bridge = _bridge_payload_for_runtime_policy(resolution, settings)
        settings["datafusion.catalog.information_schema"] = str(
            profile.catalog.enable_information_schema
        ).lower()
        delta_runtime = delta_runtime_env_options(profile)
        runtime_policy_bridge: dict[str, object] = (
            dict(bridge.payload) if bridge.payload is not None else {"enabled": False}
        )
        runtime_policy_bridge.update(planning_surface_policy_identity)
        if resolution.module_name != "datafusion_engine.extensions.datafusion_ext":
            ctx = resolution.builder(
                list(settings.items()),
                runtime_env,
                delta_runtime,
            )
        else:
            try:
                ctx = resolution.builder(
                    list(settings.items()),
                    None,
                    delta_runtime,
                    bridge.options,
                )
            except TypeError:
                ctx = resolution.builder(
                    list(settings.items()),
                    None,
                    delta_runtime,
                )
                runtime_policy_bridge = {
                    "enabled": False,
                    "reason": "legacy_builder_signature",
                    **planning_surface_policy_identity,
                }
    except (RuntimeError, TypeError, ValueError) as exc:
        return DeltaSessionBuildResult(
            ctx=None,
            available=resolution.available,
            installed=False,
            error=str(exc),
            cause=exc,
            runtime_policy_bridge=bridge.payload,
        )
    normalized = _normalize_session_context(ctx)
    if normalized is None:
        message = "Delta session context must return a SessionContext."
        return DeltaSessionBuildResult(
            ctx=None,
            available=resolution.available,
            installed=False,
            error=message,
            cause=TypeError(message),
            runtime_policy_bridge=runtime_policy_bridge,
        )
    return DeltaSessionBuildResult(
        ctx=normalized,
        available=resolution.available,
        installed=True,
        error=None,
        cause=None,
        runtime_policy_bridge=runtime_policy_bridge,
    )


def _bridge_payload_for_runtime_policy(
    resolution: DeltaSessionBuilderResolution,
    settings: dict[str, str],
) -> DeltaRuntimePolicyBridgeResult:
    if resolution.module_name != "datafusion_engine.extensions.datafusion_ext":
        return DeltaRuntimePolicyBridgeResult(options=None, payload=None)
    non_runtime_settings, runtime_settings = split_runtime_settings(settings)
    settings.clear()
    settings.update(non_runtime_settings)
    return build_runtime_policy_options(resolution.module_owner, runtime_settings)


def _normalize_session_context(ctx: object) -> SessionContext | None:
    if isinstance(ctx, SessionContext):
        return ctx
    if hasattr(ctx, "table") and not hasattr(ctx, "ctx"):
        wrapper = SessionContext.__new__(SessionContext)
        wrapper.ctx = ctx
        return wrapper
    return None


def _parse_suffixed_runtime_size(value: str) -> int | None:
    for suffix in sorted(_RUNTIME_SIZE_SUFFIXES, key=len, reverse=True):
        if not value.endswith(suffix):
            continue
        number = value[: -len(suffix)].strip()
        if not number:
            return None
        try:
            magnitude = float(number)
        except ValueError:
            return None
        if magnitude < 0:
            return None
        return int(magnitude * _RUNTIME_SIZE_SUFFIXES[suffix])
    return None


def _normalized_runtime_value(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip().lower().replace("_", "")
    return stripped or None


def _parse_suffixed_runtime_duration_seconds(value: str) -> int | None:
    for suffix in sorted(_RUNTIME_DURATION_SUFFIXES, key=len, reverse=True):
        if not value.endswith(suffix):
            continue
        number = value[: -len(suffix)].strip()
        if not number:
            return None
        try:
            magnitude = float(number)
        except ValueError:
            return None
        if magnitude < 0:
            return None
        return int(magnitude * _RUNTIME_DURATION_SUFFIXES[suffix])
    return None


def _apply_cache_runtime_policy_settings(
    *,
    options: object,
    runtime_settings: Mapping[str, str],
    consumed: dict[str, object],
    unsupported: dict[str, str],
) -> set[str]:
    from datafusion_engine.session.cache_manager_contract import (
        cache_manager_contract_from_settings,
    )

    cache_contract = cache_manager_contract_from_settings(
        enabled=True,
        settings=runtime_settings,
    )
    cache_attr_rules: tuple[tuple[str, str, object | None], ...] = (
        (
            "datafusion.runtime.metadata_cache_limit",
            "metadata_cache_limit",
            cache_contract.metadata_cache_limit_bytes,
        ),
        (
            "datafusion.runtime.list_files_cache_limit",
            "list_files_cache_limit",
            cache_contract.list_files_cache_limit_bytes,
        ),
        (
            "datafusion.runtime.list_files_cache_ttl",
            "list_files_cache_ttl_seconds",
            cache_contract.list_files_cache_ttl_seconds,
        ),
    )
    handled_keys: set[str] = set()
    for runtime_key, attr_name, parsed_value in cache_attr_rules:
        if runtime_key not in runtime_settings:
            continue
        handled_keys.add(runtime_key)
        if parsed_value is None or not hasattr(options, attr_name):
            unsupported[runtime_key] = runtime_settings[runtime_key]
            continue
        setattr(options, attr_name, parsed_value)
        consumed[runtime_key] = parsed_value
    return handled_keys


def _apply_standard_runtime_policy_settings(
    *,
    options: object,
    runtime_settings: Mapping[str, str],
    handled_keys: set[str],
    consumed: dict[str, object],
    unsupported: dict[str, str],
) -> None:
    for key, raw_value in runtime_settings.items():
        if key in handled_keys:
            continue
        parsed: object | None = None
        attr_name: str | None = None
        if key == "datafusion.runtime.memory_limit":
            attr_name = "memory_limit"
            parsed = parse_runtime_size(raw_value)
        elif key == "datafusion.runtime.max_temp_directory_size":
            attr_name = "max_temp_directory_size"
            parsed = parse_runtime_size(raw_value)
        elif key == "datafusion.runtime.temp_directory":
            attr_name = "temp_directory"
            parsed = _non_empty_string(raw_value)
        if attr_name is None or parsed is None:
            unsupported[key] = raw_value
            continue
        if not hasattr(options, attr_name):
            unsupported[key] = raw_value
            continue
        setattr(options, attr_name, parsed)
        consumed[key] = parsed


def _runtime_policy_bridge_result(
    *,
    options: object,
    runtime_settings: Mapping[str, str],
    consumed: dict[str, object],
    unsupported: dict[str, str],
) -> DeltaRuntimePolicyBridgeResult:
    if not consumed:
        return DeltaRuntimePolicyBridgeResult(
            options=None,
            payload={
                "enabled": False,
                "reason": "no_supported_runtime_settings",
                "runtime_settings_seen": dict(runtime_settings),
                "unsupported_runtime_settings": unsupported,
            },
        )
    return DeltaRuntimePolicyBridgeResult(
        options=options,
        payload={
            "enabled": True,
            "consumed_runtime_settings": consumed,
            "unsupported_runtime_settings": unsupported,
        },
    )


def _non_empty_string(value: str) -> str | None:
    if value.strip():
        return value
    return None


__all__ = [
    "DeltaRuntimePolicyBridgeResult",
    "DeltaSessionBuildResult",
    "DeltaSessionBuilderResolution",
    "build_delta_session_context",
    "build_runtime_policy_options",
    "parse_runtime_duration_seconds",
    "parse_runtime_size",
    "resolve_delta_session_builder",
    "split_runtime_settings",
]
