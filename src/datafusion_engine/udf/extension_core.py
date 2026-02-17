"""Rust UDF registration helpers."""
# NOTE(size-exception): This module is temporarily >800 LOC during hard-cutover
# decomposition. Remaining extraction and contraction work is tracked in
# docs/plans/src_design_improvements_implementation_plan_v1_2026-02-16.md.

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
    udf_policies: WeakKeyDictionary[SessionContext, tuple[bool, int | None, int | None]] = field(
        default_factory=WeakKeyDictionary
    )
    udf_validated: WeakSet[SessionContext] = field(default_factory=WeakSet)
    udf_ddl: WeakSet[SessionContext] = field(default_factory=WeakSet)


def _resolve_registries(registries: ExtensionRegistries | None) -> ExtensionRegistries:
    return registries or ExtensionRegistries()


RustUdfSnapshot = Mapping[str, object]

_DDL_TYPE_ALIASES: dict[str, str] = {
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INT",
    "int64": "BIGINT",
    "uint8": "TINYINT",
    "uint16": "SMALLINT",
    "uint32": "INT",
    "uint64": "BIGINT",
    "float32": "FLOAT",
    "float64": "DOUBLE",
    "utf8": "VARCHAR",
    "large_utf8": "VARCHAR",
    "large_string": "VARCHAR",
    "string": "VARCHAR",
    "null": "VARCHAR",
    "bool": "BOOLEAN",
    "boolean": "BOOLEAN",
}

_DDL_COMPLEX_TYPE_TOKENS: tuple[str, ...] = (
    "struct<",
    "list<",
    "large_list<",
    "fixed_size_list<",
    "map<",
    "dictionary<",
    "binary",
    "large_binary",
    "fixed_size_binary",
    "union<",
)

_EXPECTED_PLUGIN_ABI_MAJOR = 1
_EXPECTED_PLUGIN_ABI_MINOR = 1
_REGISTRY_SNAPSHOT_VERSION = 1
_RUNTIME_INSTALL_ENTRYPOINT = "install_codeanatomy_runtime"
_RUNTIME_MODULAR_ENTRYPOINTS: tuple[str, ...] = (
    "register_codeanatomy_udfs",
    "install_function_factory",
    "install_expr_planners",
    "registry_snapshot",
)

_EXPR_SURFACE_SNAPSHOT_ENTRIES: Mapping[str, Mapping[str, object]] = {
    "stable_id": {
        "probe_args": ("prefix", "part1"),
        "parameter_names": ("prefix", "part1"),
        "signature_inputs": (("string", "string"),),
        "return_types": ("string",),
        "volatility": "stable",
    },
    "stable_id_parts": {
        "probe_args": ("prefix", "part1"),
        "parameter_names": ("prefix", "part1"),
        "signature_inputs": (("string", "string"),),
        "return_types": ("string",),
        "volatility": "stable",
    },
    "col_to_byte": {
        "probe_args": ("abcdef", 3, "BYTE"),
        "parameter_names": ("line_text", "col", "col_unit"),
        "signature_inputs": (("string", "int64", "string"),),
        "return_types": ("int64",),
        "volatility": "stable",
    },
    "span_make": {
        "probe_args": (0, 0),
        "parameter_names": ("bstart", "bend"),
        "signature_inputs": (("int64", "int64"),),
        "return_types": (
            "struct<bstart: int64, bend: int64, line_base: int64, col_unit: string, end_exclusive: bool>",
        ),
        "volatility": "stable",
    },
}


def _module_supports_runtime_install(module: ModuleType) -> bool:
    if callable(getattr(module, _RUNTIME_INSTALL_ENTRYPOINT, None)):
        return True
    if all(callable(getattr(module, name, None)) for name in _RUNTIME_MODULAR_ENTRYPOINTS):
        return True
    return all(
        callable(getattr(module, name, None))
        for name in (
            "install_codeanatomy_udf_config",
            "install_function_factory",
            "install_expr_planners",
        )
    )


def _invoke_runtime_entrypoint(
    internal: ModuleType,
    entrypoint: str,
    *,
    ctx: SessionContext,
    args: Sequence[object] = (),
) -> object:
    _selection, payload = invoke_entrypoint_with_adapted_context(
        internal.__name__,
        internal,
        entrypoint,
        ExtensionEntrypointInvocation(
            ctx=ctx,
            internal_ctx=getattr(ctx, "ctx", None),
            args=args,
            allow_fallback=False,
        ),
    )
    return payload


def _build_runtime_install_payload(
    *,
    install_mode: str,
    snapshot: Mapping[str, object],
    internal: ModuleType,
    udf_installed: bool,
    planner_names: Sequence[str],
    async_config: Mapping[str, object],
) -> dict[str, object]:
    return {
        "contract_version": 3,
        "runtime_install_mode": install_mode,
        "snapshot": snapshot,
        "udf_installed": udf_installed,
        "function_factory_installed": True,
        "expr_planners_installed": True,
        "expr_planner_names": tuple(planner_names),
        "cache_registrar_available": callable(getattr(internal, "register_cache_tables", None)),
        "async": dict(async_config),
    }


def _missing_runtime_modular_entrypoints(internal: ModuleType) -> list[str]:
    return [
        name for name in _RUNTIME_MODULAR_ENTRYPOINTS if not callable(getattr(internal, name, None))
    ]


def _install_runtime_via_modular_entrypoints(
    internal: ModuleType,
    *,
    ctx: SessionContext,
    enable_async: bool,
    async_udf_timeout_ms: int | None,
    async_udf_batch_size: int | None,
    registries: ExtensionRegistries,
) -> Mapping[str, object]:
    register_udfs = getattr(internal, "register_codeanatomy_udfs", None)
    snapshot_fn = getattr(internal, "registry_snapshot", None)
    if not callable(register_udfs) or not callable(snapshot_fn):
        missing = _missing_runtime_modular_entrypoints(internal)
        missing_csv = ", ".join(missing)
        msg = (
            f"{internal.__name__} is missing modular runtime entrypoints: {missing_csv}. "
            f"{REBUILD_WHEELS_HINT}"
        )
        raise TypeError(msg)

    from datafusion_engine.expr.domain_planner import domain_planner_names_from_snapshot
    from datafusion_engine.expr.planner import install_expr_planners
    from datafusion_engine.udf.factory import (
        function_factory_policy_from_snapshot,
        install_function_factory,
    )

    install_udf_config = getattr(internal, "install_codeanatomy_udf_config", None)
    if callable(install_udf_config):
        _invoke_runtime_entrypoint(internal, "install_codeanatomy_udf_config", ctx=ctx)

    _invoke_runtime_entrypoint(
        internal,
        "register_codeanatomy_udfs",
        ctx=ctx,
        args=(enable_async, async_udf_timeout_ms, async_udf_batch_size),
    )
    raw_snapshot = _invoke_runtime_entrypoint(internal, "registry_snapshot", ctx=ctx)
    if not isinstance(raw_snapshot, Mapping):
        msg = f"{internal.__name__}.registry_snapshot returned a non-mapping payload."
        raise TypeError(msg)

    normalized_snapshot = _normalize_registry_snapshot(
        raw_snapshot,
        ctx=ctx,
        registries=registries,
    )

    policy = function_factory_policy_from_snapshot(
        normalized_snapshot,
        allow_async=enable_async,
    )
    install_function_factory(ctx, policy=policy)

    planner_names = domain_planner_names_from_snapshot(normalized_snapshot) or (
        "codeanatomy_domain",
    )
    install_expr_planners(ctx, planner_names=planner_names)
    return _build_runtime_install_payload(
        install_mode="modular",
        snapshot=normalized_snapshot,
        internal=internal,
        udf_installed=True,
        planner_names=planner_names,
        async_config={
            "enabled": enable_async,
            "timeout_ms": async_udf_timeout_ms,
            "batch_size": async_udf_batch_size,
        },
    )


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
    expected = {"major": _EXPECTED_PLUGIN_ABI_MAJOR, "minor": _EXPECTED_PLUGIN_ABI_MINOR}
    install_mode = "unified"
    if callable(installer):
        try:
            payload = _invoke_runtime_entrypoint(
                internal,
                _RUNTIME_INSTALL_ENTRYPOINT,
                ctx=ctx,
                args=(
                    enable_async,
                    async_udf_timeout_ms,
                    async_udf_batch_size,
                ),
            )
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = (
                "Rust runtime install failed due to SessionContext ABI mismatch. "
                f"expected_plugin_abi={expected}. "
                f"{REBUILD_WHEELS_HINT}"
            )
            raise RuntimeError(msg) from exc
    else:
        install_mode = "modular"
        try:
            payload = _install_runtime_via_modular_entrypoints(
                internal,
                ctx=ctx,
                enable_async=enable_async,
                async_udf_timeout_ms=async_udf_timeout_ms,
                async_udf_batch_size=async_udf_batch_size,
                registries=registries,
            )
        except (ImportError, RuntimeError, TypeError, ValueError) as exc:
            msg = (
                "Rust runtime modular install failed due to SessionContext ABI mismatch "
                "or missing runtime entrypoints. "
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
    payload_mapping.setdefault("runtime_install_mode", install_mode)
    payload_mapping.setdefault("udf_installed", True)
    payload_mapping.setdefault("function_factory_installed", True)
    payload_mapping.setdefault("expr_planners_installed", True)
    payload_mapping.setdefault(
        "cache_registrar_available",
        callable(getattr(internal, "register_cache_tables", None)),
    )
    payload_mapping["snapshot"] = normalized_snapshot
    registries.runtime_payloads[ctx] = payload_mapping
    return normalized_snapshot


def _build_registry_snapshot(
    ctx: SessionContext,
    *,
    registries: ExtensionRegistries,
) -> Mapping[str, object]:
    policy = registries.udf_policies.get(ctx, (False, None, None))
    return _install_codeanatomy_runtime_snapshot(
        ctx,
        enable_async=policy[0],
        async_udf_timeout_ms=policy[1],
        async_udf_batch_size=policy[2],
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
    payload = _supplement_expr_surface_snapshot(payload, ctx=ctx)
    if ctx in registries.udf_policies:
        enable_async, timeout_ms, batch_size = registries.udf_policies[ctx]
        payload["async_udf_policy"] = {
            "enabled": enable_async,
            "timeout_ms": timeout_ms,
            "batch_size": batch_size,
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
            f"The {EXTENSION_MODULE_LABEL} extension module exposing install_codeanatomy_runtime or "
            "the modular runtime entrypoint contract is required. "
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
    _async_udf_policy,
    _build_docs_snapshot,
    _mutable_mapping,
    _notify_udf_snapshot,
    _registered_snapshot,
    _require_bool_mapping,
    _require_mapping,
    _snapshot_names,
    _supplement_expr_surface_snapshot,
    _validated_snapshot,
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
    "ExtensionRegistries",
    "RustUdfSnapshot",
    "_async_udf_policy",
    "_build_docs_snapshot",
    "_registered_snapshot",
    "_validated_snapshot",
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
