"""Delta session builder helpers for session-factory orchestration."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from datafusion import RuntimeEnvBuilder, SessionContext

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


@dataclass(frozen=True)
class _RuntimePolicySettingRule:
    attr: str
    parser: Callable[[str], object | None]


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
    for key, raw_value in runtime_settings.items():
        parsed = _parse_runtime_policy_value(key, raw_value)
        if parsed is None:
            unsupported[key] = raw_value
            continue
        setattr(options, _runtime_policy_rules()[key].attr, parsed)
        consumed[key] = parsed
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
    from datafusion_engine.session.runtime import delta_runtime_env_options

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
    try:
        settings = profile.settings_payload()
        bridge = _bridge_payload_for_runtime_policy(resolution, settings)
        settings["datafusion.catalog.information_schema"] = str(
            profile.catalog.enable_information_schema
        ).lower()
        delta_runtime = delta_runtime_env_options(profile)
        runtime_policy_bridge = bridge.payload
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


def _runtime_policy_rules() -> Mapping[str, _RuntimePolicySettingRule]:
    return {
        "datafusion.runtime.memory_limit": _RuntimePolicySettingRule(
            attr="memory_limit",
            parser=parse_runtime_size,
        ),
        "datafusion.runtime.max_temp_directory_size": _RuntimePolicySettingRule(
            attr="max_temp_directory_size",
            parser=parse_runtime_size,
        ),
        "datafusion.runtime.metadata_cache_limit": _RuntimePolicySettingRule(
            attr="metadata_cache_limit",
            parser=parse_runtime_size,
        ),
        "datafusion.runtime.temp_directory": _RuntimePolicySettingRule(
            attr="temp_directory",
            parser=_non_empty_string,
        ),
    }


def _parse_runtime_policy_value(key: str, raw_value: str) -> object | None:
    rule = _runtime_policy_rules().get(key)
    if rule is None:
        return None
    return rule.parser(raw_value)


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
    "parse_runtime_size",
    "resolve_delta_session_builder",
    "split_runtime_settings",
]
