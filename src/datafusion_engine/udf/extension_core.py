"""Rust UDF registration helpers."""

from __future__ import annotations

import importlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import ModuleType
from typing import TYPE_CHECKING
from weakref import WeakKeyDictionary, WeakSet

from datafusion import SessionContext

from datafusion_engine.extensions.context_adaptation import (
    ExtensionEntrypointInvocation,
    invoke_entrypoint_with_adapted_context,
)
from datafusion_engine.udf.constants import (
    ABI_LOAD_FAILURE_MSG,
    EXTENSION_MODULE_LABEL,
    EXTENSION_MODULE_PATH,
    REBUILD_WHEELS_HINT,
)
from datafusion_engine.udf.runtime_snapshot_types import normalize_runtime_install_snapshot

if TYPE_CHECKING:
    from typing import Protocol

    from datafusion_engine.udf.factory import CreateFunctionConfig

    class RegisterFunction(Protocol):
        def __call__(self, ctx: SessionContext, *, config: CreateFunctionConfig) -> None: ...


@dataclass
class ExtensionRegistries:
    """Injectable registries for Rust UDF runtime state."""

    udf_contexts: WeakSet[SessionContext] = field(default_factory=WeakSet)
    udf_snapshots: WeakKeyDictionary[SessionContext, Mapping[str, object]] = field(
        default_factory=WeakKeyDictionary
    )
    udf_docs: WeakKeyDictionary[SessionContext, Mapping[str, object]] = field(
        default_factory=WeakKeyDictionary
    )
    runtime_payloads: WeakKeyDictionary[SessionContext, Mapping[str, object]] = field(
        default_factory=WeakKeyDictionary
    )
    udf_policies: WeakKeyDictionary[SessionContext, AsyncUdfPolicy] = field(
        default_factory=WeakKeyDictionary
    )
    udf_validated: WeakSet[SessionContext] = field(default_factory=WeakSet)
    udf_ddl: WeakSet[SessionContext] = field(default_factory=WeakSet)


def _resolve_registries(registries: ExtensionRegistries | None) -> ExtensionRegistries:
    return registries or ExtensionRegistries()


RustUdfSnapshot = Mapping[str, object]


@dataclass(frozen=True)
class AsyncUdfPolicy:
    """Policy for async UDF execution."""

    enabled: bool
    timeout_ms: int | None = None
    batch_size: int | None = None


_EXPECTED_PLUGIN_ABI_MAJOR = 1
_EXPECTED_PLUGIN_ABI_MINOR = 1
_REGISTRY_SNAPSHOT_VERSION = 1
_RUNTIME_INSTALL_ENTRYPOINT = "install_codeanatomy_runtime"


def _module_supports_runtime_install(module: ModuleType) -> bool:
    return callable(getattr(module, _RUNTIME_INSTALL_ENTRYPOINT, None))


def _invoke_runtime_entrypoint(
    internal: ModuleType,
    entrypoint: str,
    *,
    ctx: SessionContext,
    args: Sequence[object] = (),
    kwargs: Mapping[str, object] | None = None,
) -> object:
    _selection, payload = invoke_entrypoint_with_adapted_context(
        internal.__name__,
        internal,
        entrypoint,
        ExtensionEntrypointInvocation(
            ctx=ctx,
            internal_ctx=getattr(ctx, "ctx", None),
            args=args,
            kwargs=kwargs,
            allow_fallback=False,
        ),
    )
    return payload


def _install_codeanatomy_runtime_snapshot(
    ctx: SessionContext,
    *,
    enable_async: bool,
    async_udf_timeout_ms: int | None,
    async_udf_batch_size: int | None,
    registries: ExtensionRegistries,
) -> Mapping[str, object]:
    internal = _datafusion_internal()
    installer = getattr(internal, _RUNTIME_INSTALL_ENTRYPOINT, None)
    if not callable(installer):
        msg = (
            f"{EXTENSION_MODULE_LABEL} is missing required runtime entrypoint "
            f"{_RUNTIME_INSTALL_ENTRYPOINT!r}. {REBUILD_WHEELS_HINT}"
        )
        raise TypeError(msg)
    expected = {"major": _EXPECTED_PLUGIN_ABI_MAJOR, "minor": _EXPECTED_PLUGIN_ABI_MINOR}
    try:
        payload = _invoke_runtime_entrypoint(
            internal,
            _RUNTIME_INSTALL_ENTRYPOINT,
            ctx=ctx,
            kwargs={
                "enable_async_udfs": enable_async,
                "async_udf_timeout_ms": async_udf_timeout_ms,
                "async_udf_batch_size": async_udf_batch_size,
            },
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = (
            "Rust runtime install failed due to SessionContext ABI mismatch. "
            f"expected_plugin_abi={expected}. "
            f"{REBUILD_WHEELS_HINT}"
        )
        raise RuntimeError(msg) from exc
    if not isinstance(payload, Mapping):
        msg = "Rust runtime installer returned a non-mapping payload."
        raise TypeError(msg)
    payload_mapping = dict(payload)
    snapshot = payload_mapping.get("snapshot")
    if snapshot is None:
        msg = "Rust runtime installer returned a payload without a snapshot."
        raise RuntimeError(msg)
    normalized_snapshot = _normalize_registry_snapshot(
        snapshot,
        ctx=ctx,
        registries=registries,
    )
    payload_mapping.setdefault("contract_version", 3)
    payload_mapping.setdefault("runtime_install_mode", "unified")
    payload_mapping.setdefault("udf_installed", True)
    payload_mapping.setdefault("function_factory_installed", True)
    payload_mapping.setdefault("expr_planners_installed", True)
    payload_mapping.setdefault(
        "cache_registrar_available",
        callable(getattr(internal, "register_cache_tables", None)),
    )
    payload_mapping["snapshot"] = normalized_snapshot
    normalized_payload = normalize_runtime_install_snapshot(payload_mapping)
    registries.runtime_payloads[ctx] = {
        "contract_version": normalized_payload.contract_version,
        "runtime_install_mode": normalized_payload.runtime_install_mode,
        "udf_installed": normalized_payload.udf_installed,
        "function_factory_installed": normalized_payload.function_factory_installed,
        "expr_planners_installed": normalized_payload.expr_planners_installed,
        "cache_registrar_available": normalized_payload.cache_registrar_available,
        "snapshot": dict(normalized_payload.snapshot),
    }
    return normalized_snapshot


def _build_registry_snapshot(
    ctx: SessionContext,
    *,
    registries: ExtensionRegistries,
) -> Mapping[str, object]:
    policy = registries.udf_policies.get(ctx, AsyncUdfPolicy(enabled=False))
    return _install_codeanatomy_runtime_snapshot(
        ctx,
        enable_async=policy.enabled,
        async_udf_timeout_ms=policy.timeout_ms,
        async_udf_batch_size=policy.batch_size,
        registries=registries,
    )


def _normalize_registry_snapshot(
    snapshot: object,
    *,
    ctx: SessionContext,
    registries: ExtensionRegistries,
) -> Mapping[str, object]:
    if not isinstance(snapshot, Mapping):
        msg = "datafusion extension registry_snapshot returned a non-mapping payload."
        raise TypeError(msg)
    payload = dict(snapshot)
    raw_version = payload.get("version", _REGISTRY_SNAPSHOT_VERSION)
    if not isinstance(raw_version, int):
        msg = "datafusion extension registry_snapshot.version must be an integer."
        raise TypeError(msg)
    if raw_version != _REGISTRY_SNAPSHOT_VERSION:
        msg = (
            "datafusion extension registry_snapshot.version "
            f"{raw_version} is unsupported (expected {_REGISTRY_SNAPSHOT_VERSION})."
        )
        raise ValueError(msg)
    payload["version"] = raw_version
    payload.pop("pycapsule_udfs", None)
    # Preserve the key for diagnostics/tests while dropping non-serializable payloads.
    payload.setdefault("pycapsule_udfs", [])
    payload.setdefault("scalar", [])
    payload.setdefault("aggregate", [])
    payload.setdefault("window", [])
    payload.setdefault("table", [])
    payload.setdefault("aliases", {})
    payload.setdefault("parameter_names", {})
    payload.setdefault("volatility", {})
    payload.setdefault("rewrite_tags", {})
    payload.setdefault("signature_inputs", {})
    payload.setdefault("return_types", {})
    payload.setdefault("simplify", {})
    payload.setdefault("coerce_types", {})
    payload.setdefault("short_circuits", {})
    payload.setdefault("config_defaults", {})
    payload.setdefault("custom_udfs", [])
    names = _snapshot_names(payload)
    param_names = _mutable_mapping(payload, "parameter_names")
    volatility = _mutable_mapping(payload, "volatility")
    signature_inputs = _mutable_mapping(payload, "signature_inputs")
    return_types = _mutable_mapping(payload, "return_types")
    for name in names:
        param_names.setdefault(name, ())
        volatility.setdefault(name, "volatile")
        signature_inputs.setdefault(name, ())
        return_types.setdefault(name, ())
    payload["parameter_names"] = param_names
    payload["volatility"] = volatility
    payload["signature_inputs"] = signature_inputs
    payload["return_types"] = return_types
    if ctx in registries.udf_policies:
        policy = registries.udf_policies[ctx]
        payload["async_udf_policy"] = {
            "enabled": policy.enabled,
            "timeout_ms": policy.timeout_ms,
            "batch_size": policy.batch_size,
        }
    return payload


def _install_rust_udfs(
    ctx: SessionContext,
    *,
    enable_async: bool,
    async_udf_timeout_ms: int | None,
    async_udf_batch_size: int | None,
    registries: ExtensionRegistries,
) -> None:
    unified_snapshot = _install_codeanatomy_runtime_snapshot(
        ctx,
        enable_async=enable_async,
        async_udf_timeout_ms=async_udf_timeout_ms,
        async_udf_batch_size=async_udf_batch_size,
        registries=registries,
    )
    registries.udf_snapshots[ctx] = unified_snapshot
    registries.udf_validated.add(ctx)
    _notify_udf_snapshot(unified_snapshot)


def _datafusion_internal() -> ModuleType:
    try:
        module = importlib.import_module(EXTENSION_MODULE_PATH)
    except ImportError as exc:
        msg = ABI_LOAD_FAILURE_MSG.format(module=EXTENSION_MODULE_LABEL, error=exc)
        details = (
            f"The {EXTENSION_MODULE_LABEL} extension module exposing "
            "install_codeanatomy_runtime is required. "
            f"{REBUILD_WHEELS_HINT}"
        )
        error_message = f"{msg} {details}"
        raise ImportError(error_message) from exc
    if _module_supports_runtime_install(module):
        return module
    msg = f"{EXTENSION_MODULE_LABEL} is missing required runtime entrypoints. {REBUILD_WHEELS_HINT}"
    raise ImportError(msg)


def _extension_module_with_capabilities() -> ModuleType:
    try:
        module = importlib.import_module(EXTENSION_MODULE_PATH)
    except ImportError as exc:
        msg = ABI_LOAD_FAILURE_MSG.format(module=EXTENSION_MODULE_LABEL, error=exc)
        details = f"The {EXTENSION_MODULE_LABEL} module exposing capabilities_snapshot is required."
        error_message = f"{msg} {details}"
        raise ImportError(error_message) from exc
    if not callable(getattr(module, "capabilities_snapshot", None)):
        msg = f"{EXTENSION_MODULE_LABEL} is missing required entrypoint capabilities_snapshot."
        raise TypeError(msg)
    return module


def expected_plugin_abi() -> dict[str, int]:
    """Return the expected plugin ABI version for extension compatibility checks.

    Returns:
        dict[str, int]: Expected plugin ABI major/minor mapping.
    """
    return {
        "major": _EXPECTED_PLUGIN_ABI_MAJOR,
        "minor": _EXPECTED_PLUGIN_ABI_MINOR,
    }


def invoke_runtime_entrypoint(
    internal: ModuleType,
    entrypoint: str,
    *,
    ctx: SessionContext,
    args: Sequence[object] = (),
    kwargs: Mapping[str, object] | None = None,
) -> object:
    """Invoke a runtime extension entrypoint with adapted SessionContext payload.

    Returns:
        object: Runtime entrypoint return payload.
    """
    return _invoke_runtime_entrypoint(
        internal,
        entrypoint,
        ctx=ctx,
        args=args,
        kwargs=kwargs,
    )


def extension_module_with_capabilities() -> ModuleType:
    """Return the extension module exposing capabilities_snapshot()."""
    return _extension_module_with_capabilities()


def udf_backend_available() -> bool:
    """Return whether the native CodeAnatomy UDF backend is available.

    Returns:
    -------
    bool
        True when native UDF hooks can be registered.
    """
    try:
        internal = _datafusion_internal()
    except ImportError:
        return False
    return _module_supports_runtime_install(internal)


from datafusion_engine.udf.extension_ddl import _register_udf_aliases, _register_udf_specs
from datafusion_engine.udf.extension_snapshot_runtime import (
    _mutable_mapping,
    _notify_udf_snapshot,
    _require_bool_mapping,
    _require_mapping,
    _snapshot_names,
    rust_runtime_install_payload,
    rust_udf_docs,
    rust_udf_snapshot,
    rust_udf_snapshot_bytes,
    rust_udf_snapshot_hash,
    rust_udf_snapshot_payload,
    snapshot_alias_mapping,
    snapshot_function_names,
    snapshot_parameter_names,
    snapshot_return_types,
    udf_names_from_snapshot,
    validate_required_udfs,
    validate_rust_udf_snapshot,
)


def register_rust_udfs(
    ctx: SessionContext,
    *,
    enable_async: bool = False,
    async_udf_timeout_ms: int | None = None,
    async_udf_batch_size: int | None = None,
    registries: ExtensionRegistries | None = None,
) -> Mapping[str, object]:
    """Ensure Rust UDF snapshots are available for a session context.

    Args:
        ctx: DataFusion session context.
        enable_async: Whether async UDF execution is enabled.
        async_udf_timeout_ms: Optional async UDF timeout override.
        async_udf_batch_size: Optional async UDF batch-size override.
        registries: Optional registry container for runtime UDF state.

    Returns:
        Mapping[str, object]: Result.

    """
    from datafusion_engine.udf.extension_registry import register_rust_udfs as _register_rust_udfs

    return _register_rust_udfs(
        ctx,
        enable_async=enable_async,
        async_udf_timeout_ms=async_udf_timeout_ms,
        async_udf_batch_size=async_udf_batch_size,
        registries=registries,
    )


def register_udfs_via_ddl(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    replace: bool = True,
    registries: ExtensionRegistries | None = None,
) -> None:
    """Register Rust UDFs via CREATE FUNCTION DDL for catalog visibility.

    Parameters
    ----------
    ctx
        DataFusion session context used for DDL registration.
    snapshot
        Rust UDF registry snapshot payload.
    replace
        Whether to replace existing CREATE FUNCTION entries.
    """
    resolved_registries = _resolve_registries(registries)
    if ctx in resolved_registries.udf_ddl:
        return
    from datafusion_engine.udf.factory import register_function
    from datafusion_engine.udf.metadata import datafusion_udf_specs

    specs = datafusion_udf_specs(registry_snapshot=snapshot)
    spec_map = {spec.engine_name: spec for spec in specs}
    _register_udf_specs(ctx, specs=specs, replace=replace, register_fn=register_function)
    _register_udf_aliases(
        ctx,
        spec_map=spec_map,
        snapshot=snapshot,
        replace=replace,
        register_fn=register_function,
    )
    resolved_registries.udf_ddl.add(ctx)


def udf_audit_payload(snapshot: Mapping[str, object]) -> dict[str, object]:
    """Return a diagnostics payload describing UDF volatility and fast-path coverage.

    Parameters
    ----------
    snapshot
        Rust UDF snapshot mapping.

    Returns:
    -------
    dict[str, object]
        Diagnostics payload for volatility and fast-path audit.
    """
    validate_rust_udf_snapshot(snapshot)
    names = _snapshot_names(snapshot)
    volatility = _require_mapping(snapshot, name="volatility")
    simplify = _require_bool_mapping(snapshot, name="simplify")
    short_circuits = _require_bool_mapping(snapshot, name="short_circuits")
    counts: dict[str, int] = {}
    for value in volatility.values():
        label = str(value)
        counts[label] = counts.get(label, 0) + 1
    simplify_enabled = sum(1 for name in names if simplify.get(name) is True)
    short_circuit_enabled = sum(1 for name in names if short_circuits.get(name) is True)
    missing_volatility = sorted(name for name in names if name not in volatility)
    return {
        "total_udfs": len(names),
        "volatility_counts": counts,
        "simplify_enabled": simplify_enabled,
        "short_circuit_enabled": short_circuit_enabled,
        "missing_volatility": missing_volatility,
    }


__all__ = [
    "AsyncUdfPolicy",
    "ExtensionRegistries",
    "RustUdfSnapshot",
    "expected_plugin_abi",
    "extension_module_with_capabilities",
    "invoke_runtime_entrypoint",
    "register_rust_udfs",
    "register_udfs_via_ddl",
    "rust_runtime_install_payload",
    "rust_udf_docs",
    "rust_udf_snapshot",
    "rust_udf_snapshot_bytes",
    "rust_udf_snapshot_hash",
    "rust_udf_snapshot_payload",
    "snapshot_alias_mapping",
    "snapshot_function_names",
    "snapshot_parameter_names",
    "snapshot_return_types",
    "udf_audit_payload",
    "udf_names_from_snapshot",
    "validate_required_udfs",
    "validate_rust_udf_snapshot",
]
