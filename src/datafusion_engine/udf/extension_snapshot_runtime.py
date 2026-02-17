"""Extension snapshot/runtime helpers extracted from extension_core."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from datafusion import SessionContext

from datafusion_engine.udf.extension_core import (
    _EXPR_SURFACE_SNAPSHOT_ENTRIES,
    EXTENSION_MODULE_LABEL,
    ExtensionRegistries,
    _build_registry_snapshot,
    _datafusion_internal,
    _install_rust_udfs,
    _invoke_runtime_entrypoint,
    _normalize_registry_snapshot,
    _resolve_registries,
)
from serde_msgspec import dumps_msgpack
from utils.hashing import hash_sha256_hex


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
    from datafusion_engine.udf.extension_validation import _mutable_mapping as _impl

    return _impl(payload, key)


def _require_sequence(snapshot: Mapping[str, object], *, name: str) -> Sequence[object]:
    from datafusion_engine.udf.extension_validation import _require_sequence as _impl

    return _impl(snapshot, name=name)


def _require_mapping(snapshot: Mapping[str, object], *, name: str) -> Mapping[str, object]:
    from datafusion_engine.udf.extension_validation import _require_mapping as _impl

    return _impl(snapshot, name=name)


def _require_bool_mapping(snapshot: Mapping[str, object], *, name: str) -> Mapping[str, bool]:
    from datafusion_engine.udf.extension_validation import _require_bool_mapping as _impl

    return _impl(snapshot, name=name)


def _coerce_config_default_value(field_value: object) -> bool | int | str:
    from datafusion_engine.udf.extension_validation import _coerce_config_default_value as _impl

    return _impl(field_value)


def _require_config_defaults(
    snapshot: Mapping[str, object],
) -> Mapping[str, Mapping[str, object]]:
    from datafusion_engine.udf.extension_validation import _require_config_defaults as _impl

    return _impl(snapshot)


def _snapshot_names(snapshot: Mapping[str, object]) -> frozenset[str]:
    from datafusion_engine.udf.extension_validation import _snapshot_names as _impl

    return _impl(snapshot)


def _alias_to_canonical(snapshot: Mapping[str, object]) -> dict[str, str]:
    from datafusion_engine.udf.extension_validation import _alias_to_canonical as _impl

    return _impl(snapshot)


def _iter_snapshot_values(values: object) -> set[str]:
    from datafusion_engine.udf.extension_validation import _iter_snapshot_values as _impl

    return _impl(values)


def _snapshot_alias_names(snapshot: Mapping[str, object]) -> set[str]:
    from datafusion_engine.udf.extension_validation import _snapshot_alias_names as _impl

    return _impl(snapshot)


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
    from datafusion_engine.udf.extension_validation import (
        snapshot_parameter_names as _snapshot_parameter_names,
    )

    return _snapshot_parameter_names(snapshot)


def snapshot_return_types(snapshot: Mapping[str, object]) -> dict[str, tuple[str, ...]]:
    """Return a mapping of UDF return types from a registry snapshot.

    Returns:
    -------
    dict[str, tuple[str, ...]]
        Mapping of function name to return type names.
    """
    from datafusion_engine.udf.extension_validation import (
        snapshot_return_types as _snapshot_return_types,
    )

    return _snapshot_return_types(snapshot)


def snapshot_alias_mapping(snapshot: Mapping[str, object]) -> dict[str, str]:
    """Return alias-to-canonical mapping from a registry snapshot.

    Returns:
    -------
    dict[str, str]
        Mapping of alias name to canonical name.
    """
    from datafusion_engine.udf.extension_validation import (
        snapshot_alias_mapping as _snapshot_alias_mapping,
    )

    return _snapshot_alias_mapping(snapshot)


def _validate_required_snapshot_keys(snapshot: Mapping[str, object]) -> None:
    from datafusion_engine.udf.extension_validation import (
        _validate_required_snapshot_keys as _impl,
    )

    _impl(snapshot)


def _require_snapshot_metadata(
    snapshot: Mapping[str, object],
) -> tuple[
    Mapping[str, object],
    Mapping[str, object],
    Mapping[str, object],
    Mapping[str, object],
    frozenset[str],
]:
    from datafusion_engine.udf.extension_validation import _require_snapshot_metadata as _impl

    return _impl(snapshot)


def _validate_udf_entries(
    snapshot: Mapping[str, object],
    *,
    list_name: str,
    param_names: Mapping[str, object],
    volatility: Mapping[str, object],
) -> None:
    from datafusion_engine.udf.extension_validation import _validate_udf_entries as _impl

    _impl(
        snapshot,
        list_name=list_name,
        param_names=param_names,
        volatility=volatility,
    )


def _validate_signature_metadata(
    *,
    names: frozenset[str],
    signature_inputs: Mapping[str, object],
    return_types: Mapping[str, object],
) -> None:
    from datafusion_engine.udf.extension_validation import _validate_signature_metadata as _impl

    _impl(
        names=names,
        signature_inputs=signature_inputs,
        return_types=return_types,
    )


def validate_rust_udf_snapshot(snapshot: Mapping[str, object]) -> None:
    """Validate structural requirements for a Rust UDF snapshot.

    Parameters
    ----------
    snapshot
        Snapshot payload returned by the extension runtime.
    """
    from datafusion_engine.udf.extension_validation import (
        validate_rust_udf_snapshot as _validate_rust_udf_snapshot,
    )

    _validate_rust_udf_snapshot(snapshot)


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
    """
    from datafusion_engine.udf.extension_validation import (
        validate_required_udfs as _validate_required_udfs,
    )

    _validate_required_udfs(snapshot, required=required)


def udf_names_from_snapshot(snapshot: Mapping[str, object]) -> frozenset[str]:
    """Return all UDF names derived from a registry snapshot.

    Returns:
    -------
    frozenset[str]
        Canonical UDF names present in the snapshot.
    """
    from datafusion_engine.udf.extension_validation import (
        udf_names_from_snapshot as _udf_names_from_snapshot,
    )

    return _udf_names_from_snapshot(snapshot)


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
    registries: ExtensionRegistries,
) -> Mapping[str, object] | None:
    if ctx not in registries.udf_contexts:
        return None
    existing = registries.udf_policies.get(ctx)
    if existing is not None and existing != policy:
        msg = "Rust UDFs already registered with a different async policy."
        raise ValueError(msg)
    return _validated_snapshot(ctx, registries=registries)


__all__ = [
    "_alias_to_canonical",
    "_async_udf_policy",
    "_build_docs_snapshot",
    "_build_registry_snapshot",
    "_coerce_config_default_value",
    "_coerce_nonstring_sequence",
    "_coerce_signature_inputs",
    "_empty_registry_snapshot",
    "_install_rust_udfs",
    "_iter_snapshot_values",
    "_mutable_mapping",
    "_normalize_expr_surface_metadata",
    "_normalize_registry_snapshot",
    "_normalize_snapshot_value",
    "_notify_udf_snapshot",
    "_probe_expr_surface_udf",
    "_registered_snapshot",
    "_require_bool_mapping",
    "_require_config_defaults",
    "_require_mapping",
    "_require_sequence",
    "_require_snapshot_metadata",
    "_snapshot_alias_names",
    "_snapshot_names",
    "_supplement_expr_surface_snapshot",
    "_validate_required_snapshot_keys",
    "_validate_signature_metadata",
    "_validate_udf_entries",
    "_validated_snapshot",
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
    "udf_names_from_snapshot",
    "validate_required_udfs",
    "validate_rust_udf_snapshot",
]
