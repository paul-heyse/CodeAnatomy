"""Rust UDF registration helpers."""

from __future__ import annotations

import contextlib
import importlib
from collections.abc import Callable, Iterable, Mapping, Sequence
from types import ModuleType
from typing import TYPE_CHECKING
from weakref import WeakKeyDictionary, WeakSet

import pyarrow as pa
import pyarrow.types as patypes
from datafusion import SessionContext

from datafusion_engine.extensions.context_adaptation import (
    ExtensionEntrypointInvocation,
    invoke_entrypoint_with_adapted_context,
)

if TYPE_CHECKING:
    from typing import Protocol

    from datafusion_engine.udf.catalog import DataFusionUdfSpec
    from datafusion_engine.udf.factory import CreateFunctionConfig, FunctionArgSpec

    class RegisterFunction(Protocol):
        def __call__(self, ctx: SessionContext, *, config: CreateFunctionConfig) -> None: ...


from serde_msgspec import dumps_msgpack
from utils.hashing import hash_sha256_hex
from utils.validation import validate_required_items

_RUST_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_RUST_UDF_SNAPSHOTS: WeakKeyDictionary[SessionContext, Mapping[str, object]] = WeakKeyDictionary()
_RUST_UDF_DOCS: WeakKeyDictionary[SessionContext, Mapping[str, object]] = WeakKeyDictionary()
_RUST_RUNTIME_PAYLOADS: WeakKeyDictionary[SessionContext, Mapping[str, object]] = (
    WeakKeyDictionary()
)
_RUST_UDF_POLICIES: WeakKeyDictionary[
    SessionContext,
    tuple[bool, int | None, int | None],
] = WeakKeyDictionary()
_RUST_UDF_VALIDATED: WeakSet[SessionContext] = WeakSet()
_RUST_UDF_DDL: WeakSet[SessionContext] = WeakSet()

_REQUIRED_SNAPSHOT_KEYS: tuple[str, ...] = (
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


def _install_function_factory_with_async_fallback(
    *,
    install_with_async: Callable[[], None],
    install_without_async: Callable[[], None],
    enable_async: bool,
) -> bool:
    try:
        if enable_async:
            install_with_async()
        else:
            install_without_async()
    except (TypeError, RuntimeError) as exc:
        if enable_async and "async-udf feature" in str(exc).lower():
            install_without_async()
            return False
        raise
    return enable_async


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
) -> Mapping[str, object]:
    register_udfs = getattr(internal, "register_codeanatomy_udfs", None)
    snapshot_fn = getattr(internal, "registry_snapshot", None)
    if not callable(register_udfs) or not callable(snapshot_fn):
        missing = _missing_runtime_modular_entrypoints(internal)
        missing_csv = ", ".join(missing)
        msg = (
            f"{internal.__name__} is missing modular runtime entrypoints: {missing_csv}. "
            "Rebuild and install matching datafusion/datafusion_ext wheels "
            "(scripts/build_datafusion_wheels.sh + uv sync)."
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

    normalized_snapshot = _normalize_registry_snapshot(raw_snapshot, ctx=ctx)

    def _install_async_policy() -> None:
        policy = function_factory_policy_from_snapshot(
            normalized_snapshot,
            allow_async=True,
        )
        install_function_factory(ctx, policy=policy)

    def _install_sync_policy() -> None:
        policy = function_factory_policy_from_snapshot(
            normalized_snapshot,
            allow_async=False,
        )
        install_function_factory(ctx, policy=policy)

    async_enabled = _install_function_factory_with_async_fallback(
        install_with_async=_install_async_policy,
        install_without_async=_install_sync_policy,
        enable_async=enable_async,
    )

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
            "enabled": async_enabled,
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
                "Rebuild and install matching datafusion/datafusion_ext wheels "
                "(scripts/build_datafusion_wheels.sh + uv sync)."
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
            )
        except (ImportError, RuntimeError, TypeError, ValueError) as exc:
            msg = (
                "Rust runtime modular install failed due to SessionContext ABI mismatch "
                "or missing runtime entrypoints. "
                f"expected_plugin_abi={expected}. "
                "Rebuild and install matching datafusion/datafusion_ext wheels "
                "(scripts/build_datafusion_wheels.sh + uv sync)."
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
    normalized_snapshot = _normalize_registry_snapshot(snapshot, ctx=ctx)
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
    _RUST_RUNTIME_PAYLOADS[ctx] = payload_mapping
    return normalized_snapshot


def _build_registry_snapshot(ctx: SessionContext) -> Mapping[str, object]:
    policy = _RUST_UDF_POLICIES.get(ctx, (False, None, None))
    return _install_codeanatomy_runtime_snapshot(
        ctx,
        enable_async=policy[0],
        async_udf_timeout_ms=policy[1],
        async_udf_batch_size=policy[2],
    )


def _normalize_registry_snapshot(
    snapshot: object,
    *,
    ctx: SessionContext,
) -> Mapping[str, object]:
    if not isinstance(snapshot, Mapping):
        msg = "datafusion extension registry_snapshot returned a non-mapping payload."
        raise TypeError(msg)
    payload = dict(snapshot)
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
    if ctx in _RUST_UDF_POLICIES:
        enable_async, timeout_ms, batch_size = _RUST_UDF_POLICIES[ctx]
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
) -> None:
    unified_snapshot = _install_codeanatomy_runtime_snapshot(
        ctx,
        enable_async=enable_async,
        async_udf_timeout_ms=async_udf_timeout_ms,
        async_udf_batch_size=async_udf_batch_size,
    )
    _RUST_UDF_SNAPSHOTS[ctx] = unified_snapshot
    _RUST_UDF_VALIDATED.add(ctx)
    _notify_udf_snapshot(unified_snapshot)


def _datafusion_internal() -> ModuleType:
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError as exc:
        msg = (
            "The datafusion_ext extension module exposing install_codeanatomy_runtime or "
            "the modular runtime entrypoint contract is required. "
            "Rebuild and install matching datafusion/datafusion_ext wheels "
            "(scripts/build_datafusion_wheels.sh + uv sync)."
        )
        raise ImportError(msg) from exc
    if _module_supports_runtime_install(module):
        return module
    msg = (
        "datafusion_ext is missing required runtime entrypoints. "
        "Rebuild and install matching datafusion/datafusion_ext wheels "
        "(scripts/build_datafusion_wheels.sh + uv sync)."
    )
    raise ImportError(msg)


def _extension_module_with_capabilities() -> ModuleType | None:
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError:
        return None
    if callable(getattr(module, "capabilities_snapshot", None)):
        return module
    return None


def extension_capabilities_snapshot() -> Mapping[str, object] | None:
    """Return the Rust extension capabilities snapshot when available.

    Returns:
    -------
    Mapping[str, object] | None
        Snapshot payload or ``None`` when unavailable.
    """
    module = _extension_module_with_capabilities()
    if module is None:
        return None
    snapshot = module.capabilities_snapshot()
    if isinstance(snapshot, Mapping):
        return dict(snapshot)
    return None


def extension_capabilities_report() -> dict[str, object]:
    """Return compatibility report for the Rust extension ABI snapshot.

    Returns:
    -------
    dict[str, object]
        Compatibility report including snapshot and errors.
    """
    expected = {"major": _EXPECTED_PLUGIN_ABI_MAJOR, "minor": _EXPECTED_PLUGIN_ABI_MINOR}
    snapshot = extension_capabilities_snapshot()
    if snapshot is None:
        return {
            "available": False,
            "compatible": False,
            "expected_plugin_abi": expected,
            "error": "Extension capabilities snapshot unavailable.",
            "snapshot": None,
        }
    plugin_abi = snapshot.get("plugin_abi") if isinstance(snapshot, Mapping) else None
    major = None
    minor = None
    if isinstance(plugin_abi, Mapping):
        major = plugin_abi.get("major")
        minor = plugin_abi.get("minor")
    compatible = major == _EXPECTED_PLUGIN_ABI_MAJOR and minor == _EXPECTED_PLUGIN_ABI_MINOR
    error = None
    if not compatible:
        error = (
            "Extension ABI mismatch: "
            f"expected={expected} observed={{'major': {major}, 'minor': {minor}}}."
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
) -> dict[str, object]:
    """Validate extension ABI snapshot and optionally raise on mismatch.

    Args:
        strict: Whether to raise immediately when ABI checks fail.
        ctx: Optional session context used for strict SessionContext contract probing.

    Returns:
        dict[str, object]: Result.

    Raises:
        RuntimeError: If the operation cannot be completed.
    """
    report = extension_capabilities_report()
    if strict and not report.get("compatible", False):
        msg = report.get("error") or "Extension ABI compatibility check failed."
        raise RuntimeError(msg)
    module = _extension_module_with_capabilities()
    probe = getattr(module, "session_context_contract_probe", None) if module is not None else None
    if strict and module is not None and callable(probe):
        probe_ctx = ctx if ctx is not None else SessionContext()
        try:
            _invoke_runtime_entrypoint(module, "session_context_contract_probe", ctx=probe_ctx)
        except (RuntimeError, TypeError, ValueError) as exc:
            expected = {"major": _EXPECTED_PLUGIN_ABI_MAJOR, "minor": _EXPECTED_PLUGIN_ABI_MINOR}
            msg = (
                "SessionContext ABI mismatch detected by extension contract probe. "
                f"expected_plugin_abi={expected}. "
                "Rebuild and install matching datafusion/datafusion_ext wheels "
                "(scripts/build_datafusion_wheels.sh + uv sync)."
            )
            raise RuntimeError(msg) from exc
    return report


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


def _coerce_nonstring_sequence(value: object) -> tuple[object, ...] | None:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return None
    return tuple(value)


def _coerce_signature_inputs(value: object) -> tuple[tuple[str, ...], ...] | None:
    entries = _coerce_nonstring_sequence(value)
    if entries is None:
        return None
    normalized: list[tuple[str, ...]] = []
    for entry in entries:
        signature_entry = _coerce_nonstring_sequence(entry)
        if signature_entry is None:
            continue
        normalized.append(tuple(str(item) for item in signature_entry))
    return tuple(normalized)


def _normalize_expr_surface_metadata(
    metadata: Mapping[str, object],
) -> (
    tuple[tuple[object, ...], tuple[str, ...], tuple[tuple[str, ...], ...], tuple[str, ...], str]
    | None
):
    probe_args = _coerce_nonstring_sequence(metadata.get("probe_args"))
    parameter_names = _coerce_nonstring_sequence(metadata.get("parameter_names"))
    signature_inputs = _coerce_signature_inputs(metadata.get("signature_inputs"))
    return_types = _coerce_nonstring_sequence(metadata.get("return_types"))
    volatility_value = metadata.get("volatility")
    if (
        probe_args is None
        or parameter_names is None
        or signature_inputs is None
        or return_types is None
        or not isinstance(volatility_value, str)
    ):
        return None
    return (
        probe_args,
        tuple(str(value) for value in parameter_names),
        signature_inputs,
        tuple(str(value) for value in return_types),
        volatility_value,
    )


def _probe_expr_surface_udf(
    *,
    name: str,
    probe_args: tuple[object, ...],
    ctx: SessionContext,
) -> bool:
    try:
        from datafusion import lit

        from datafusion_engine.udf.expr import udf_expr
    except ImportError:
        return False
    except (RuntimeError, TypeError, ValueError):
        return False
    try:
        _ = udf_expr(name, *(lit(arg) for arg in probe_args), ctx=ctx)
    except (RuntimeError, TypeError, ValueError):
        return False
    return True


def _supplement_expr_surface_snapshot(
    payload: dict[str, object],
    *,
    ctx: SessionContext,
) -> dict[str, object]:
    """Augment registry snapshots with expression-surface UDF metadata.

    Some UDF-like expression entrypoints are available through extension
    expression builders but are omitted from ``registry_snapshot``.
    Including these names keeps downstream required-UDF validation aligned
    with what the runtime can actually build.

    Returns:
    -------
    dict[str, object]
        Snapshot payload augmented with available expression-surface metadata.
    """
    scalar_values = payload.get("scalar")
    scalar: list[str] = []
    existing = _coerce_nonstring_sequence(scalar_values)
    if existing is not None:
        scalar.extend(value for value in existing if isinstance(value, str))
    names = set(scalar)
    param_names = _mutable_mapping(payload, "parameter_names")
    volatility = _mutable_mapping(payload, "volatility")
    signature_inputs = _mutable_mapping(payload, "signature_inputs")
    return_types = _mutable_mapping(payload, "return_types")

    for name, metadata in _EXPR_SURFACE_SNAPSHOT_ENTRIES.items():
        if name in names:
            continue
        normalized = _normalize_expr_surface_metadata(metadata)
        if normalized is None:
            continue
        probe_args, parameter_names, signature, returns, volatility_value = normalized
        if not _probe_expr_surface_udf(name=name, probe_args=probe_args, ctx=ctx):
            continue

        scalar.append(name)
        names.add(name)
        param_names[name] = parameter_names
        signature_inputs[name] = signature
        return_types[name] = returns
        volatility[name] = volatility_value

    payload["scalar"] = scalar
    payload["parameter_names"] = param_names
    payload["volatility"] = volatility
    payload["signature_inputs"] = signature_inputs
    payload["return_types"] = return_types
    return payload


def _mutable_mapping(payload: Mapping[str, object], key: str) -> dict[str, object]:
    """Return a mutable mapping from a payload entry.

    Returns:
    -------
    dict[str, object]
        Mutable mapping for the requested key.
    """
    value = payload.get(key)
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _require_sequence(snapshot: Mapping[str, object], *, name: str) -> Sequence[object]:
    value = snapshot.get(name, ())
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        msg = f"Rust UDF snapshot field {name!r} must be a sequence."
        raise TypeError(msg)
    return value


def _require_mapping(snapshot: Mapping[str, object], *, name: str) -> Mapping[str, object]:
    value = snapshot.get(name, {})
    if not isinstance(value, Mapping):
        msg = f"Rust UDF snapshot field {name!r} must be a mapping."
        raise TypeError(msg)
    return value


def _require_bool_mapping(snapshot: Mapping[str, object], *, name: str) -> Mapping[str, bool]:
    value = _require_mapping(snapshot, name=name)
    output: dict[str, bool] = {}
    for key, entry in value.items():
        if not isinstance(key, str):
            msg = f"Rust UDF snapshot field {name!r} must use string keys."
            raise TypeError(msg)
        if not isinstance(entry, bool):
            msg = f"Rust UDF snapshot field {name!r} must contain boolean values."
            raise TypeError(msg)
        output[key] = entry
    return output


def _require_config_defaults(
    snapshot: Mapping[str, object],
) -> Mapping[str, Mapping[str, object]]:
    value = _require_mapping(snapshot, name="config_defaults")
    output: dict[str, Mapping[str, object]] = {}
    for key, entry in value.items():
        if not isinstance(key, str):
            msg = "Rust UDF snapshot config_defaults must use string keys."
            raise TypeError(msg)
        if not isinstance(entry, Mapping):
            msg = "Rust UDF snapshot config_defaults must map to mappings."
            raise TypeError(msg)
        for field, field_value in entry.items():
            if not isinstance(field, str):
                msg = "Rust UDF snapshot config_defaults fields must use string keys."
                raise TypeError(msg)
            if not isinstance(field_value, (bool, int, str)):
                msg = "Rust UDF snapshot config_defaults values must be bool, int, or str."
                raise TypeError(msg)
        output[key] = entry
    return output


def _snapshot_names(snapshot: Mapping[str, object]) -> frozenset[str]:
    names: set[str] = set()
    for key in ("scalar", "aggregate", "window", "table", "custom_udfs"):
        entries = _require_sequence(snapshot, name=key)
        for entry in entries:
            if not isinstance(entry, str):
                msg = f"Rust UDF snapshot field {key!r} contains non-string entries."
                raise TypeError(msg)
            names.add(entry)
    aliases = _require_mapping(snapshot, name="aliases")
    for alias, target in aliases.items():
        if not isinstance(alias, str):
            msg = "Rust UDF snapshot aliases must use string keys."
            raise TypeError(msg)
        names.add(alias)
        if isinstance(target, str):
            names.add(target)
            continue
        if isinstance(target, Sequence) and not isinstance(target, (str, bytes, bytearray)):
            for value in target:
                if not isinstance(value, str):
                    msg = "Rust UDF snapshot aliases must contain string values."
                    raise TypeError(msg)
                names.add(value)
            continue
        msg = "Rust UDF snapshot aliases must map to strings or string lists."
        raise TypeError(msg)
    return frozenset(names)


def _alias_to_canonical(snapshot: Mapping[str, object]) -> dict[str, str]:
    aliases = _require_mapping(snapshot, name="aliases")
    mapping: dict[str, str] = {}
    for canonical, entries in aliases.items():
        if not isinstance(canonical, str):
            msg = "Rust UDF snapshot aliases must use string keys."
            raise TypeError(msg)
        if isinstance(entries, str):
            mapping[canonical] = entries
            continue
        if isinstance(entries, Sequence) and not isinstance(entries, (str, bytes, bytearray)):
            for alias in entries:
                if not isinstance(alias, str):
                    msg = "Rust UDF snapshot aliases must contain string values."
                    raise TypeError(msg)
                mapping[alias] = canonical
            continue
        msg = "Rust UDF snapshot aliases must map to strings or string lists."
        raise TypeError(msg)
    return mapping


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
    keys: tuple[str, ...] = ("scalar", "aggregate", "window", "table")
    if include_custom:
        keys = (*keys, "custom_udfs")
    names: set[str] = set()
    for key in keys:
        names.update(_iter_snapshot_values(snapshot.get(key)))
    if include_aliases:
        names.update(_snapshot_alias_names(snapshot))
    return frozenset(names)


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
        if isinstance(params, Iterable) and not isinstance(params, (str, bytes)):
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
        if not isinstance(entries, Iterable) or isinstance(entries, (str, bytes)):
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
    validate_required_items(
        _REQUIRED_SNAPSHOT_KEYS,
        snapshot,
        item_label="Rust UDF snapshot keys",
        error_type=ValueError,
    )


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

    Args:
        snapshot: Description.

    Raises:
        TypeError: If the operation cannot be completed.
        ValueError: If the operation cannot be completed.
    """
    try:
        _validate_required_snapshot_keys(snapshot)
        param_names, volatility, signature_inputs, return_types, names = _require_snapshot_metadata(
            snapshot,
        )
        for list_name in ("scalar", "aggregate", "window"):
            _validate_udf_entries(
                snapshot,
                list_name=list_name,
                param_names=param_names,
                volatility=volatility,
            )
        _validate_signature_metadata(
            names=names,
            signature_inputs=signature_inputs,
            return_types=return_types,
        )
    except TypeError as exc:
        msg = f"Invalid Rust UDF snapshot types: {exc}"
        raise TypeError(msg) from exc
    except ValueError as exc:
        msg = f"Invalid Rust UDF snapshot values: {exc}"
        raise ValueError(msg) from exc


def validate_required_udfs(
    snapshot: Mapping[str, object],
    *,
    required: Sequence[str],
) -> None:
    """Validate required UDFs against a registry snapshot.

    Args:
        snapshot: Description.
        required: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    if not required:
        return
    names = _snapshot_names(snapshot)
    validate_required_items(
        required,
        names,
        item_label="Rust UDFs",
        error_type=ValueError,
    )
    aliases = _alias_to_canonical(snapshot)
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
    try:
        internal = _datafusion_internal()
        snapshot = _invoke_runtime_entrypoint(internal, "udf_docs_snapshot", ctx=ctx)
    except (ImportError, AttributeError, TypeError, ValueError):
        return {}
    if not isinstance(snapshot, Mapping):
        msg = "datafusion_ext.udf_docs_snapshot returned a non-mapping payload."
        raise TypeError(msg)
    return dict(snapshot)


def rust_udf_snapshot(ctx: SessionContext) -> Mapping[str, object]:
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
    cached = _RUST_UDF_SNAPSHOTS.get(ctx)
    if cached is not None:
        if ctx not in _RUST_UDF_VALIDATED:
            validate_rust_udf_snapshot(cached)
            _RUST_UDF_VALIDATED.add(ctx)
        return cached
    snapshot = _build_registry_snapshot(ctx)
    docs = None
    with contextlib.suppress(ImportError, RuntimeError, TypeError, ValueError):
        docs = _build_docs_snapshot(ctx)
    if docs:
        snapshot = dict(snapshot)
        snapshot["documentation"] = docs
    validate_rust_udf_snapshot(snapshot)
    _RUST_UDF_VALIDATED.add(ctx)
    _notify_udf_snapshot(snapshot)
    _RUST_UDF_SNAPSHOTS[ctx] = snapshot
    return snapshot


def _validated_snapshot(ctx: SessionContext) -> Mapping[str, object]:
    """Return a validated Rust UDF snapshot for a session.

    Args:
        ctx: Description.

    Raises:
        RuntimeError: If the operation cannot be completed.
    """
    snapshot = rust_udf_snapshot(ctx)
    if ctx not in _RUST_UDF_VALIDATED:
        msg = "Rust UDF snapshot validation missing for the session context."
        raise RuntimeError(msg)
    return snapshot


def rust_udf_docs(ctx: SessionContext) -> Mapping[str, object]:
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
    cached = _RUST_UDF_DOCS.get(ctx)
    if cached is not None:
        return cached
    snapshot = _build_docs_snapshot(ctx)
    _RUST_UDF_DOCS[ctx] = snapshot
    return snapshot


def rust_runtime_install_payload(ctx: SessionContext) -> Mapping[str, object]:
    """Return cached payload from install_codeanatomy_runtime for a session context."""
    payload = _RUST_RUNTIME_PAYLOADS.get(ctx)
    if payload is not None:
        return dict(payload)
    _ = rust_udf_snapshot(ctx)
    payload = _RUST_RUNTIME_PAYLOADS.get(ctx)
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
) -> tuple[bool, int | None, int | None]:
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
    return (enable_async, async_udf_timeout_ms, async_udf_batch_size)


def _registered_snapshot(
    ctx: SessionContext,
    *,
    policy: tuple[bool, int | None, int | None],
) -> Mapping[str, object] | None:
    if ctx not in _RUST_UDF_CONTEXTS:
        return None
    existing = _RUST_UDF_POLICIES.get(ctx)
    if existing is not None and existing != policy:
        msg = "Rust UDFs already registered with a different async policy."
        raise ValueError(msg)
    return _validated_snapshot(ctx)


def register_rust_udfs(
    ctx: SessionContext,
    *,
    enable_async: bool = False,
    async_udf_timeout_ms: int | None = None,
    async_udf_batch_size: int | None = None,
) -> Mapping[str, object]:
    """Ensure Rust UDF snapshots are available for a session context.

    Args:
        ctx: DataFusion session context.
        enable_async: Whether async UDF execution is enabled.
        async_udf_timeout_ms: Optional async UDF timeout override.
        async_udf_batch_size: Optional async UDF batch-size override.

    Returns:
        Mapping[str, object]: Result.

    Raises:
        ValueError: If async UDF policy settings are invalid.
    """
    try:
        policy = _async_udf_policy(
            enable_async=enable_async,
            async_udf_timeout_ms=async_udf_timeout_ms,
            async_udf_batch_size=async_udf_batch_size,
        )
    except ValueError as exc:
        msg = f"Invalid async UDF policy: {exc}"
        raise ValueError(msg) from exc
    existing = _registered_snapshot(
        ctx,
        policy=policy,
    )
    if existing is not None:
        return existing
    _install_rust_udfs(
        ctx,
        enable_async=enable_async,
        async_udf_timeout_ms=async_udf_timeout_ms,
        async_udf_batch_size=async_udf_batch_size,
    )
    _RUST_UDF_CONTEXTS.add(ctx)
    _RUST_UDF_POLICIES[ctx] = policy
    return _validated_snapshot(ctx)


def register_udfs_via_ddl(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    replace: bool = True,
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
    if ctx in _RUST_UDF_DDL:
        return
    from datafusion_engine.udf.catalog import datafusion_udf_specs
    from datafusion_engine.udf.factory import register_function

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
    _RUST_UDF_DDL.add(ctx)


def _register_udf_specs(
    ctx: SessionContext,
    *,
    specs: Sequence[DataFusionUdfSpec],
    replace: bool,
    register_fn: RegisterFunction,
) -> None:
    for spec in specs:
        if spec.kind == "table":
            continue
        config = _ddl_config_for_spec(spec, target_name=spec.engine_name, replace=replace)
        register_fn(ctx, config=config)


def _register_udf_aliases(
    ctx: SessionContext,
    *,
    spec_map: Mapping[str, DataFusionUdfSpec],
    snapshot: Mapping[str, object],
    replace: bool,
    register_fn: RegisterFunction,
) -> None:
    alias_map = snapshot.get("aliases")
    if not isinstance(alias_map, Mapping):
        return
    for base_name, aliases in alias_map.items():
        if not isinstance(base_name, str):
            continue
        spec = spec_map.get(base_name)
        if spec is None or spec.kind == "table":
            continue
        for alias in _alias_list(aliases):
            if alias == base_name:
                continue
            config = _ddl_config_for_spec(
                spec,
                target_name=base_name,
                name_override=alias,
                replace=replace,
            )
            register_fn(ctx, config=config)


def _alias_list(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(str(item) for item in value if item is not None)
    return ()


def _ddl_type_name_from_arrow(dtype: pa.DataType) -> str | None:
    if patypes.is_dictionary(dtype):
        return _ddl_type_name(dtype.value_type)
    result: str | None = None
    if patypes.is_decimal(dtype):
        result = f"DECIMAL({dtype.precision},{dtype.scale})"
    elif patypes.is_timestamp(dtype):
        result = "TIMESTAMP"
    elif patypes.is_date(dtype):
        result = "DATE"
    elif patypes.is_time(dtype):
        result = "TIME"
    varchar_checks = (
        patypes.is_struct,
        patypes.is_list,
        patypes.is_large_list,
        patypes.is_fixed_size_list,
        patypes.is_map,
        patypes.is_union,
        patypes.is_binary,
        patypes.is_large_binary,
        patypes.is_fixed_size_binary,
    )
    if result is None and any(check(dtype) for check in varchar_checks):
        result = "VARCHAR"
    return result


def _ddl_type_name_from_string(dtype_name: str) -> str:
    if dtype_name.startswith("timestamp"):
        return "TIMESTAMP"
    if dtype_name.startswith("date"):
        return "DATE"
    if dtype_name.startswith("time"):
        return "TIME"
    if any(token in dtype_name for token in _DDL_COMPLEX_TYPE_TOKENS) or dtype_name in {
        "any",
        "variant",
        "json",
    }:
        return "VARCHAR"
    alias = _DDL_TYPE_ALIASES.get(dtype_name)
    return alias or dtype_name.upper()


def _ddl_type_name(dtype: object) -> str:
    if isinstance(dtype, pa.DataType):
        arrow_name = _ddl_type_name_from_arrow(dtype)
        if arrow_name is not None:
            return arrow_name
    dtype_name = str(dtype).strip().lower()
    return _ddl_type_name_from_string(dtype_name)


def _ddl_config_for_spec(
    spec: DataFusionUdfSpec,
    *,
    target_name: str,
    name_override: str | None = None,
    replace: bool,
) -> CreateFunctionConfig:
    from datafusion_engine.udf.factory import CreateFunctionConfig

    args = _ddl_args(spec)
    return CreateFunctionConfig(
        name=name_override or spec.engine_name,
        args=args,
        return_type=_ddl_return_type(spec),
        returns_table=spec.kind == "table",
        body_sql=_ddl_body_sql(target_name, len(args), kind=spec.kind),
        language=None,
        volatility=spec.volatility,
        replace=replace,
    )


def _ddl_args(spec: DataFusionUdfSpec) -> tuple[FunctionArgSpec, ...]:
    from datafusion_engine.udf.factory import FunctionArgSpec

    arg_names = spec.arg_names
    if arg_names is None or len(arg_names) != len(spec.input_types):
        arg_names = tuple(f"arg{idx}" for idx in range(len(spec.input_types)))
    return tuple(
        FunctionArgSpec(name=name, dtype=_ddl_type_name(dtype))
        for name, dtype in zip(arg_names, spec.input_types, strict=False)
    )


def _ddl_return_type(spec: DataFusionUdfSpec) -> str | None:
    if spec.kind == "table":
        return None
    return _ddl_type_name(spec.return_type)


def _ddl_body_sql(target_name: str, arg_count: int, *, kind: str) -> str:
    if kind in {"window", "table"}:
        return f"'{target_name}'"
    if arg_count == 0:
        return f"{target_name}()"
    placeholders = ", ".join(f"${index}" for index in range(1, arg_count + 1))
    return f"{target_name}({placeholders})"


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
    "RustUdfSnapshot",
    "extension_capabilities_report",
    "extension_capabilities_snapshot",
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
    "validate_rust_udf_snapshot",
]
