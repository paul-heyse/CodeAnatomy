"""Rust UDF registration helpers."""

from __future__ import annotations

import importlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from types import ModuleType
from typing import TYPE_CHECKING
from weakref import WeakKeyDictionary, WeakSet

from datafusion import SessionContext

from datafusion_engine.extensions.context_adaptation import (
    ExtensionEntrypointInvocation,
    invoke_entrypoint_with_adapted_context,
)
from datafusion_engine.extensions.required_entrypoints import REQUIRED_RUNTIME_ENTRYPOINTS
from datafusion_engine.udf.constants import (
    ABI_LOAD_FAILURE_MSG,
    ABI_VERSION_MISMATCH_MSG,
    EXTENSION_MODULE_LABEL,
    EXTENSION_MODULE_PATH,
    REBUILD_WHEELS_HINT,
)
from datafusion_engine.udf.runtime_snapshot_types import (
    coerce_rust_udf_snapshot,
    decode_rust_udf_snapshot_msgpack,
    normalize_runtime_install_snapshot,
    rust_udf_snapshot_mapping,
)
from serde_msgspec import dumps_msgpack
from utils.hashing import hash_sha256_hex
from utils.validation import validate_required_items

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
_EXPECTED_PLUGIN_ABI_MINOR = 2
_REGISTRY_SNAPSHOT_VERSION = 1
_RUNTIME_INSTALL_ENTRYPOINT = "install_codeanatomy_runtime"
_EXPECTED_RUNTIME_INSTALL_CONTRACT_VERSION = 4
_REQUIRED_MODULAR_ENTRYPOINTS: tuple[str, ...] = (
    "register_codeanatomy_udfs",
    "install_function_factory",
    "install_expr_planners",
    "install_relation_planner",
    "install_type_planner",
    "registry_snapshot",
    "registry_snapshot_msgpack",
)


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
    snapshot_msgpack = payload_mapping.get("snapshot_msgpack")
    if not isinstance(snapshot_msgpack, (bytes, bytearray, memoryview)):
        msg = "Rust runtime installer returned a payload without snapshot_msgpack bytes."
        raise TypeError(msg)
    try:
        typed_snapshot = decode_rust_udf_snapshot_msgpack(bytes(snapshot_msgpack))
    except Exception as exc:  # pragma: no cover - defensive decode wrapper
        msg = "Rust runtime installer returned invalid snapshot_msgpack payload."
        raise TypeError(msg) from exc
    normalized_snapshot = _normalize_registry_snapshot(
        rust_udf_snapshot_mapping(typed_snapshot),
        ctx=ctx,
        registries=registries,
    )
    payload_mapping["snapshot_msgpack"] = bytes(snapshot_msgpack)
    payload_mapping.setdefault("contract_version", _EXPECTED_RUNTIME_INSTALL_CONTRACT_VERSION)
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
    if normalized_payload.contract_version != _EXPECTED_RUNTIME_INSTALL_CONTRACT_VERSION:
        msg = (
            "Rust runtime install contract_version is incompatible: "
            f"expected={_EXPECTED_RUNTIME_INSTALL_CONTRACT_VERSION} "
            f"actual={normalized_payload.contract_version}"
        )
        raise RuntimeError(msg)
    missing_modular = [
        name
        for name in _REQUIRED_MODULAR_ENTRYPOINTS
        if not callable(getattr(internal, name, None))
    ]
    if missing_modular:
        missing_csv = ", ".join(sorted(missing_modular))
        msg = f"Rust runtime extension is missing required modular entrypoints: {missing_csv}"
        raise RuntimeError(msg)
    registries.runtime_payloads[ctx] = {
        "contract_version": normalized_payload.contract_version,
        "runtime_install_mode": normalized_payload.runtime_install_mode,
        "udf_installed": normalized_payload.udf_installed,
        "function_factory_installed": normalized_payload.function_factory_installed,
        "expr_planners_installed": normalized_payload.expr_planners_installed,
        "cache_registrar_available": normalized_payload.cache_registrar_available,
        "snapshot_msgpack": normalized_payload.snapshot_msgpack,
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
    payload = rust_udf_snapshot_mapping(coerce_rust_udf_snapshot(payload))
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


def extension_capabilities_snapshot() -> Mapping[str, object]:
    """Return the Rust extension capabilities snapshot when available.

    Returns:
        Mapping[str, object]: Snapshot payload reported by the extension.

    Raises:
        TypeError: If the extension returns a non-mapping snapshot payload.
    """
    module = extension_module_with_capabilities()
    snapshot = module.capabilities_snapshot()
    if isinstance(snapshot, Mapping):
        return dict(snapshot)
    msg = f"{EXTENSION_MODULE_LABEL}.capabilities_snapshot returned a non-mapping payload."
    raise TypeError(msg)


def extension_capabilities_report() -> Mapping[str, object]:
    """Return extension capability diagnostics report."""
    expected = expected_plugin_abi()
    try:
        snapshot = extension_capabilities_snapshot()
    except (ImportError, TypeError, RuntimeError, ValueError) as exc:
        return {
            "available": False,
            "compatible": False,
            "expected_plugin_abi": expected,
            "error": str(exc),
            "snapshot": None,
        }

    plugin_abi = snapshot.get("plugin_abi") if isinstance(snapshot, Mapping) else None
    major = None
    minor = None
    if isinstance(plugin_abi, Mapping):
        major = plugin_abi.get("major")
        minor = plugin_abi.get("minor")
    compatible = major == expected["major"] and minor == expected["minor"]
    error = None
    if not compatible:
        error = ABI_VERSION_MISMATCH_MSG.format(
            expected=expected,
            actual={"major": major, "minor": minor},
        )
    return {
        "available": True,
        "compatible": compatible,
        "expected_plugin_abi": expected,
        "observed_plugin_abi": {"major": major, "minor": minor},
        "snapshot": snapshot,
        "error": error,
    }


def validate_extension_capabilities(
    *,
    strict: bool = True,
    ctx: SessionContext | None = None,
) -> Mapping[str, object]:
    """Validate extension capabilities for the runtime profile.

    Returns:
        Mapping[str, object]: Normalized extension capability report.

    Raises:
        RuntimeError: If strict validation fails ABI, entrypoint, or probe checks.
    """
    report = extension_capabilities_report()
    if strict and not report.get("compatible", False):
        msg = report.get("error") or "Extension ABI compatibility check failed."
        raise RuntimeError(msg)
    module = extension_module_with_capabilities()
    missing = [
        name for name in REQUIRED_RUNTIME_ENTRYPOINTS if not callable(getattr(module, name, None))
    ]
    if strict and missing:
        missing_csv = ", ".join(sorted(missing))
        msg = (
            "Required runtime entrypoints are missing from extension module: "
            f"{missing_csv}. {REBUILD_WHEELS_HINT}"
        )
        raise RuntimeError(msg)
    probe = getattr(module, "session_context_contract_probe", None) if module is not None else None
    if strict and module is not None and callable(probe):
        probe_ctx = ctx if ctx is not None else SessionContext()
        try:
            invoke_runtime_entrypoint(module, "session_context_contract_probe", ctx=probe_ctx)
        except (RuntimeError, TypeError, ValueError) as exc:
            expected = expected_plugin_abi()
            msg = (
                "SessionContext ABI mismatch detected by extension contract probe. "
                f"expected_plugin_abi={expected}. "
                f"{REBUILD_WHEELS_HINT}"
            )
            raise RuntimeError(msg) from exc
    return report


def validate_runtime_capabilities(
    *,
    strict: bool = True,
    ctx: SessionContext | None = None,
) -> Mapping[str, object]:
    """Compatibility alias for runtime capability validation.

    Returns:
        Mapping[str, object]: Runtime capability report.
    """
    return validate_extension_capabilities(strict=strict, ctx=ctx)


def capability_report() -> Mapping[str, object]:
    """Compatibility alias for extension capability reports.

    Returns:
        Mapping[str, object]: Extension capability report payload.
    """
    return extension_capabilities_report()


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
    "capability_report",
    "expected_plugin_abi",
    "extension_capabilities_report",
    "extension_capabilities_snapshot",
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
    "validate_extension_capabilities",
    "validate_required_udfs",
    "validate_runtime_capabilities",
    "validate_rust_udf_snapshot",
]


# Snapshot/runtime helpers merged from legacy extension_snapshot_runtime.py.


def _empty_registry_snapshot() -> dict[str, object]:
    return {
        "scalar": [],
        "aggregate": [],
        "window": [],
        "table": [],
        "pycapsule_udfs": [],
        "aliases": {},
        "parameter_names": {},
        "volatility": {},
        "rewrite_tags": {},
        "signature_inputs": {},
        "return_types": {},
        "simplify": {},
        "coerce_types": {},
        "short_circuits": {},
        "config_defaults": {},
        "custom_udfs": [],
    }


def _mutable_mapping(payload: Mapping[str, object], key: str) -> dict[str, object]:
    mapping = payload.get(key)
    if isinstance(mapping, Mapping):
        return {str(name): value for name, value in mapping.items()}
    return {}


def _require_sequence(snapshot: Mapping[str, object], *, name: str) -> Sequence[object]:
    value = snapshot.get(name)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return value
    msg = f"Rust UDF snapshot field {name!r} must be a sequence."
    raise TypeError(msg)


def _require_mapping(snapshot: Mapping[str, object], *, name: str) -> Mapping[str, object]:
    value = snapshot.get(name)
    if isinstance(value, Mapping):
        return value
    msg = f"Rust UDF snapshot field {name!r} must be a mapping."
    raise TypeError(msg)


def _require_bool_mapping(snapshot: Mapping[str, object], *, name: str) -> Mapping[str, bool]:
    value = _require_mapping(snapshot, name=name)
    for key, flag in value.items():
        if not isinstance(flag, bool):
            msg = f"Rust UDF snapshot {name!r} entry {key!r} must be bool."
            raise TypeError(msg)
    return {str(key): bool(flag) for key, flag in value.items()}


def _coerce_config_default_value(field_value: object) -> bool | int | str:
    if isinstance(field_value, bool):
        return field_value
    if isinstance(field_value, int) and not isinstance(field_value, bool):
        return field_value
    if isinstance(field_value, str):
        return field_value
    if isinstance(field_value, Mapping) and len(field_value) == 1:
        tag, value = next(iter(field_value.items()))
        tag_text = str(tag).strip().lower()
        if tag_text in {"bool", "boolean"} and isinstance(value, bool):
            return value
        if (
            tag_text in {"int", "int32", "int64", "uint", "uint32", "uint64"}
            and isinstance(value, int)
            and not isinstance(value, bool)
        ):
            return value
        if tag_text in {"str", "string"} and isinstance(value, str):
            return value
    msg = "Rust UDF snapshot config_defaults entries must be bool, int, or str values."
    raise TypeError(msg)


def _require_config_defaults(
    snapshot: Mapping[str, object],
) -> Mapping[str, Mapping[str, object]]:
    raw_defaults = snapshot.get("config_defaults")
    if raw_defaults is None:
        return {}
    if not isinstance(raw_defaults, Mapping):
        msg = "Rust UDF snapshot field 'config_defaults' must be a mapping."
        raise TypeError(msg)
    normalized: dict[str, dict[str, object]] = {}
    for section_name, section_payload in raw_defaults.items():
        if not isinstance(section_payload, Mapping):
            msg = "Rust UDF snapshot config_defaults sections must contain mapping payloads."
            raise TypeError(msg)
        section_dict: dict[str, object] = {}
        for field_name, field_value in section_payload.items():
            section_dict[str(field_name)] = _coerce_config_default_value(field_value)
        normalized[str(section_name)] = section_dict
    return normalized


def _snapshot_names(snapshot: Mapping[str, object]) -> frozenset[str]:
    names: set[str] = set()
    for key in ("scalar", "aggregate", "window", "table"):
        values = snapshot.get(key, [])
        if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
            names.update(str(value) for value in values if value is not None)
    return frozenset(names)


def _alias_to_canonical(snapshot: Mapping[str, object]) -> dict[str, str]:
    aliases = _mutable_mapping(snapshot, "aliases")
    names = _snapshot_names(snapshot)
    canonical: dict[str, str] = {}
    for alias, target in aliases.items():
        alias_name = str(alias)
        target_name: str | None = None
        if isinstance(target, str):
            target_name = target
        elif isinstance(target, Sequence) and not isinstance(target, (str, bytes, bytearray)):
            values = [str(item) for item in target if item is not None]
            if values:
                target_name = values[0]
        if target_name is None:
            continue
        if target_name in names:
            canonical[alias_name] = target_name
    return canonical


def _iter_snapshot_values(values: object) -> set[str]:
    if isinstance(values, Iterable) and not isinstance(values, (str, bytes)):
        return {str(value) for value in values if value is not None}
    return set()


def _snapshot_alias_names(snapshot: Mapping[str, object]) -> set[str]:
    raw = snapshot.get("aliases")
    if not isinstance(raw, Mapping):
        return set()
    names: set[str] = set()
    for alias, target in raw.items():
        if alias is not None:
            names.add(str(alias))
        if target is None:
            continue
        if isinstance(target, str):
            names.add(target)
        elif isinstance(target, Sequence) and not isinstance(target, (str, bytes, bytearray)):
            names.update({str(value) for value in target if value is not None})
    return names


def snapshot_function_names(
    snapshot: Mapping[str, object],
    *,
    include_aliases: bool = False,
    include_custom: bool = False,
) -> frozenset[str]:
    """Return function names from a registry snapshot.

    Parameters
    ----------
    snapshot
        Registry snapshot payload.
    include_aliases
        Whether to include alias names from the snapshot.
    include_custom
        Whether to include custom UDF names.

    Returns:
    -------
    frozenset[str]
        Function names extracted from the snapshot.
    """
    from datafusion_engine.udf.extension_registry import (
        snapshot_function_names as _snapshot_function_names,
    )

    return _snapshot_function_names(
        snapshot,
        include_aliases=include_aliases,
        include_custom=include_custom,
    )


def snapshot_parameter_names(snapshot: Mapping[str, object]) -> dict[str, tuple[str, ...]]:
    """Return a mapping of UDF parameter names from a registry snapshot.

    Returns:
    -------
    dict[str, tuple[str, ...]]
        Mapping of function name to parameter names.
    """
    raw = snapshot.get("parameter_names")
    if not isinstance(raw, Mapping):
        return {}
    resolved: dict[str, tuple[str, ...]] = {}
    for name, params in raw.items():
        if name is None:
            continue
        if params is None or isinstance(params, str):
            continue
        if isinstance(params, Sequence) and not isinstance(params, (str, bytes, bytearray)):
            resolved[str(name)] = tuple(str(param) for param in params if param is not None)
    return resolved


def snapshot_return_types(snapshot: Mapping[str, object]) -> dict[str, tuple[str, ...]]:
    """Return a mapping of UDF return types from a registry snapshot.

    Returns:
    -------
    dict[str, tuple[str, ...]]
        Mapping of function name to return type names.
    """
    raw = snapshot.get("return_types")
    if not isinstance(raw, Mapping):
        return {}
    resolved: dict[str, tuple[str, ...]] = {}
    for name, entries in raw.items():
        if name is None:
            continue
        if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes, bytearray)):
            continue
        resolved[str(name)] = tuple(str(item) for item in entries if item is not None)
    return resolved


def snapshot_alias_mapping(snapshot: Mapping[str, object]) -> dict[str, str]:
    """Return alias-to-canonical mapping from a registry snapshot.

    Returns:
    -------
    dict[str, str]
        Mapping of alias name to canonical name.
    """
    return _alias_to_canonical(snapshot)


def _validate_required_snapshot_keys(snapshot: Mapping[str, object]) -> None:
    required = (
        "scalar",
        "aggregate",
        "window",
        "table",
        "aliases",
        "parameter_names",
        "volatility",
        "simplify",
        "coerce_types",
        "short_circuits",
        "signature_inputs",
        "return_types",
    )
    missing = sorted(name for name in required if name not in snapshot)
    if missing:
        msg = f"Missing Rust UDF snapshot keys: {missing}."
        raise ValueError(msg)


def _require_snapshot_metadata(
    snapshot: Mapping[str, object],
) -> tuple[
    Mapping[str, object],
    Mapping[str, object],
    Mapping[str, object],
    Mapping[str, object],
    frozenset[str],
]:
    _require_sequence(snapshot, name="scalar")
    _require_sequence(snapshot, name="aggregate")
    _require_sequence(snapshot, name="window")
    _require_sequence(snapshot, name="table")
    _require_mapping(snapshot, name="aliases")
    param_names = _require_mapping(snapshot, name="parameter_names")
    volatility = _require_mapping(snapshot, name="volatility")
    _require_bool_mapping(snapshot, name="simplify")
    _require_bool_mapping(snapshot, name="coerce_types")
    _require_bool_mapping(snapshot, name="short_circuits")
    _require_config_defaults(snapshot)
    signature_inputs = _require_mapping(snapshot, name="signature_inputs")
    return_types = _require_mapping(snapshot, name="return_types")
    names = _snapshot_names(snapshot)
    return param_names, volatility, signature_inputs, return_types, names


def _validate_udf_entries(
    snapshot: Mapping[str, object],
    *,
    list_name: str,
    param_names: Mapping[str, object],
    volatility: Mapping[str, object],
) -> None:
    for name in _require_sequence(snapshot, name=list_name):
        if not isinstance(name, str):
            msg = f"Rust UDF snapshot {list_name} entries must be strings."
            raise TypeError(msg)
        if name not in param_names:
            msg = f"Rust UDF snapshot missing parameter names for {name!r}."
            raise ValueError(msg)
        if name not in volatility:
            msg = f"Rust UDF snapshot missing volatility for {name!r}."
            raise ValueError(msg)


def _validate_signature_metadata(
    *,
    names: frozenset[str],
    signature_inputs: Mapping[str, object],
    return_types: Mapping[str, object],
) -> None:
    if names and not signature_inputs:
        msg = "Rust UDF snapshot missing signature_inputs entries."
        raise ValueError(msg)
    if names and not return_types:
        msg = "Rust UDF snapshot missing return_types entries."
        raise ValueError(msg)


def validate_rust_udf_snapshot(snapshot: Mapping[str, object]) -> None:
    """Validate structural requirements for a Rust UDF snapshot.

    Parameters
    ----------
    snapshot
        Snapshot payload returned by the extension runtime.

    Raises:
        TypeError: If the snapshot contains structurally invalid types.
        ValueError: If required UDF metadata is missing.
    """
    _validate_required_snapshot_keys(snapshot)
    try:
        typed = coerce_rust_udf_snapshot(snapshot)
    except Exception as exc:
        msg = f"Invalid Rust UDF snapshot types: {exc}"
        raise TypeError(msg) from exc

    names = frozenset((*typed.scalar, *typed.aggregate, *typed.window, *typed.table))
    param_names = typed.parameter_names
    volatility = typed.volatility
    signature_inputs = typed.signature_inputs
    return_types = typed.return_types

    for list_name, entries in (
        ("scalar", typed.scalar),
        ("aggregate", typed.aggregate),
        ("window", typed.window),
    ):
        for name in entries:
            if name not in param_names:
                msg = f"Rust UDF snapshot missing parameter names for {name!r} in {list_name}."
                raise ValueError(msg)
            if name not in volatility:
                msg = f"Rust UDF snapshot missing volatility for {name!r} in {list_name}."
                raise ValueError(msg)

    _validate_signature_metadata(
        names=names,
        signature_inputs=signature_inputs,
        return_types=return_types,
    )


def validate_required_udfs(
    snapshot: Mapping[str, object],
    *,
    required: Sequence[str],
) -> None:
    """Validate required UDFs against a registry snapshot.

    Parameters
    ----------
    snapshot
        Snapshot payload returned by the extension runtime.
    required
        Required UDF names that must be present in the snapshot.

    Raises:
        ValueError: If required UDFs or required metadata are missing.
    """
    if not required:
        return
    names = _snapshot_names(snapshot)
    aliases = _alias_to_canonical(snapshot)
    available = names | set(aliases)
    validate_required_items(
        required,
        available,
        item_label="Rust UDFs",
        error_type=ValueError,
    )
    signature_inputs = _require_mapping(snapshot, name="signature_inputs")
    return_types = _require_mapping(snapshot, name="return_types")
    missing_signatures: list[str] = []
    missing_returns: list[str] = []
    for name in required:
        canonical = aliases.get(name, name)
        if canonical not in signature_inputs and name not in signature_inputs:
            missing_signatures.append(name)
        if canonical not in return_types and name not in return_types:
            missing_returns.append(name)
    if missing_signatures:
        msg = f"Missing Rust UDF signature metadata for: {sorted(missing_signatures)}."
        raise ValueError(msg)
    if missing_returns:
        msg = f"Missing Rust UDF return metadata for: {sorted(missing_returns)}."
        raise ValueError(msg)


def udf_names_from_snapshot(snapshot: Mapping[str, object]) -> frozenset[str]:
    """Return all UDF names derived from a registry snapshot.

    Returns:
    -------
    frozenset[str]
        Canonical UDF names present in the snapshot.
    """
    validate_rust_udf_snapshot(snapshot)
    return _snapshot_names(snapshot)


def _notify_udf_snapshot(snapshot: Mapping[str, object]) -> None:
    _ = snapshot


def _build_docs_snapshot(ctx: SessionContext) -> Mapping[str, object]:
    internal = _datafusion_internal()
    snapshot = _invoke_runtime_entrypoint(internal, "udf_docs_snapshot", ctx=ctx)
    if not isinstance(snapshot, Mapping):
        msg = f"{EXTENSION_MODULE_LABEL}.udf_docs_snapshot returned a non-mapping payload."
        raise TypeError(msg)
    return dict(snapshot)


def rust_udf_snapshot(
    ctx: SessionContext,
    *,
    registries: ExtensionRegistries | None = None,
) -> Mapping[str, object]:
    """Return cached Rust UDF registry snapshot for a session.

    Parameters
    ----------
    ctx
        DataFusion session context.

    Returns:
    -------
    Mapping[str, object]
        Registry snapshot payload for diagnostics.
    """
    from datafusion_engine.udf.extension_registry import rust_udf_snapshot as _rust_udf_snapshot

    return _rust_udf_snapshot(ctx, registries=registries)


def _validated_snapshot(
    ctx: SessionContext,
    *,
    registries: ExtensionRegistries,
) -> Mapping[str, object]:
    """Return a validated Rust UDF snapshot for a session.

    Args:
        ctx: Description.
        registries: Registry container for snapshot validation state.

    Raises:
        RuntimeError: If the operation cannot be completed.
    """
    snapshot = rust_udf_snapshot(ctx, registries=registries)
    if ctx not in registries.udf_validated:
        msg = "Rust UDF snapshot validation missing for the session context."
        raise RuntimeError(msg)
    return snapshot


def rust_udf_docs(
    ctx: SessionContext,
    *,
    registries: ExtensionRegistries | None = None,
) -> Mapping[str, object]:
    """Return cached Rust UDF documentation snapshot for a session.

    Parameters
    ----------
    ctx
        DataFusion session context.

    Returns:
    -------
    Mapping[str, object]
        Documentation snapshot payload for diagnostics.
    """
    resolved_registries = _resolve_registries(registries)
    cached = resolved_registries.udf_docs.get(ctx)
    if cached is not None:
        return cached
    snapshot = _build_docs_snapshot(ctx)
    resolved_registries.udf_docs[ctx] = snapshot
    return snapshot


def rust_runtime_install_payload(
    ctx: SessionContext,
    *,
    registries: ExtensionRegistries | None = None,
) -> Mapping[str, object]:
    """Return cached payload from install_codeanatomy_runtime for a session context."""
    resolved_registries = _resolve_registries(registries)
    payload = resolved_registries.runtime_payloads.get(ctx)
    if payload is not None:
        return dict(payload)
    _ = rust_udf_snapshot(ctx, registries=resolved_registries)
    payload = resolved_registries.runtime_payloads.get(ctx)
    if payload is None:
        return {}
    return dict(payload)


def _normalize_snapshot_value(value: object) -> object:
    if isinstance(value, Mapping):
        ordered: dict[str, object] = {}
        for key, item in sorted(value.items(), key=lambda pair: str(pair[0])):
            ordered[str(key)] = _normalize_snapshot_value(item)
        return ordered
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, set):
        return sorted((_normalize_snapshot_value(item) for item in value), key=str)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize_snapshot_value(item) for item in value]
    return value


def rust_udf_snapshot_payload(snapshot: Mapping[str, object]) -> Mapping[str, object]:
    """Return a normalized Rust UDF snapshot payload for hashing/serialization.

    Args:
        snapshot: Description.

    Raises:
        TypeError: If the operation cannot be completed.
    """
    validate_rust_udf_snapshot(snapshot)
    normalized = _normalize_snapshot_value(snapshot)
    if not isinstance(normalized, Mapping):
        msg = "Normalized Rust UDF snapshot must be a mapping."
        raise TypeError(msg)
    return normalized


def rust_udf_snapshot_bytes(snapshot: Mapping[str, object]) -> bytes:
    """Return a deterministic msgpack payload for a Rust UDF snapshot.

    Returns:
    -------
    bytes
        Serialized snapshot payload.
    """
    payload = rust_udf_snapshot_payload(snapshot)
    return dumps_msgpack(payload)


def rust_udf_snapshot_hash(snapshot: Mapping[str, object]) -> str:
    """Return a stable hash for a Rust UDF snapshot payload.

    Returns:
    -------
    str
        SHA-256 hash of the snapshot payload.
    """
    payload = rust_udf_snapshot_bytes(snapshot)
    return hash_sha256_hex(payload)


def _async_udf_policy(
    *,
    enable_async: bool,
    async_udf_timeout_ms: int | None,
    async_udf_batch_size: int | None,
) -> AsyncUdfPolicy:
    if not enable_async and (async_udf_timeout_ms is not None or async_udf_batch_size is not None):
        msg = "Async UDF policy provided but enable_async is False."
        raise ValueError(msg)
    if enable_async:
        if async_udf_timeout_ms is None or async_udf_timeout_ms <= 0:
            msg = "async_udf_timeout_ms must be a positive integer when async UDFs are enabled."
            raise ValueError(msg)
        if async_udf_batch_size is None or async_udf_batch_size <= 0:
            msg = "async_udf_batch_size must be a positive integer when async UDFs are enabled."
            raise ValueError(msg)
    return AsyncUdfPolicy(
        enabled=enable_async,
        timeout_ms=async_udf_timeout_ms,
        batch_size=async_udf_batch_size,
    )


def _registered_snapshot(
    ctx: SessionContext,
    *,
    policy: AsyncUdfPolicy,
    registries: ExtensionRegistries,
) -> Mapping[str, object] | None:
    if ctx not in registries.udf_contexts:
        return None
    existing = registries.udf_policies.get(ctx)
    if existing is not None and existing != policy:
        msg = "Rust UDFs already registered with a different async policy."
        raise ValueError(msg)
    return _validated_snapshot(ctx, registries=registries)
