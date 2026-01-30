"""Rust UDF registration helpers."""

from __future__ import annotations

import contextlib
from collections.abc import Mapping, Sequence
from weakref import WeakKeyDictionary, WeakSet

from datafusion import SessionContext, _internal as datafusion_internal
from datafusion_engine.plugin_discovery import assert_plugin_available
from serde_msgspec import dumps_msgpack
from utils.hashing import hash_sha256_hex
from utils.validation import validate_required_items

_RUST_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_RUST_UDF_SNAPSHOTS: WeakKeyDictionary[SessionContext, Mapping[str, object]] = WeakKeyDictionary()
_RUST_UDF_DOCS: WeakKeyDictionary[SessionContext, Mapping[str, object]] = WeakKeyDictionary()
_RUST_UDF_POLICIES: WeakKeyDictionary[
    SessionContext,
    tuple[bool, int | None, int | None],
] = WeakKeyDictionary()
_RUST_UDF_VALIDATED: WeakSet[SessionContext] = WeakSet()

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
    "config_defaults",
)

RustUdfSnapshot = Mapping[str, object]


def _build_registry_snapshot(ctx: SessionContext) -> Mapping[str, object]:
    assert_plugin_available()
    snapshot = datafusion_internal.registry_snapshot(ctx)
    if not isinstance(snapshot, Mapping):
        msg = "datafusion._internal.registry_snapshot returned a non-mapping payload."
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
    if ctx in _RUST_UDF_POLICIES:
        enable_async, timeout_ms, batch_size = _RUST_UDF_POLICIES[ctx]
        payload["async_udf_policy"] = {
            "enabled": enable_async,
            "timeout_ms": timeout_ms,
            "batch_size": batch_size,
        }
    return payload


def _mutable_mapping(payload: Mapping[str, object], key: str) -> dict[str, object]:
    """Return a mutable mapping from a payload entry.

    Returns
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

    Raises
    ------
    TypeError
        Raised when snapshot fields use invalid types.
    ValueError
        Raised when required snapshot keys or metadata are missing.
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

    Raises
    ------
    ValueError
        Raised when required UDFs or signature metadata are missing.
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

    Returns
    -------
    frozenset[str]
        Canonical UDF names present in the snapshot.
    """
    validate_rust_udf_snapshot(snapshot)
    return _snapshot_names(snapshot)


def _notify_udf_snapshot(snapshot: Mapping[str, object]) -> None:
    _ = snapshot


def _build_docs_snapshot(ctx: SessionContext) -> Mapping[str, object]:
    assert_plugin_available()
    snapshot = datafusion_internal.udf_docs_snapshot(ctx)
    if not isinstance(snapshot, Mapping):
        msg = "datafusion._internal.udf_docs_snapshot returned a non-mapping payload."
        raise TypeError(msg)
    return dict(snapshot)


def rust_udf_snapshot(ctx: SessionContext) -> Mapping[str, object]:
    """Return cached Rust UDF registry snapshot for a session.

    Parameters
    ----------
    ctx
        DataFusion session context.

    Returns
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

    Returns
    -------
    Mapping[str, object]
        Snapshot payload validated for required keys and metadata.

    Raises
    ------
    RuntimeError
        Raised when the Rust UDF snapshot was not validated.
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

    Returns
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

    Returns
    -------
    Mapping[str, object]
        Normalized snapshot payload.

    Raises
    ------
    TypeError
        Raised when the normalized payload is not a mapping.
    """
    validate_rust_udf_snapshot(snapshot)
    normalized = _normalize_snapshot_value(snapshot)
    if not isinstance(normalized, Mapping):
        msg = "Normalized Rust UDF snapshot must be a mapping."
        raise TypeError(msg)
    return normalized


def rust_udf_snapshot_bytes(snapshot: Mapping[str, object]) -> bytes:
    """Return a deterministic msgpack payload for a Rust UDF snapshot.

    Returns
    -------
    bytes
        Serialized snapshot payload.
    """
    payload = rust_udf_snapshot_payload(snapshot)
    return dumps_msgpack(payload)


def rust_udf_snapshot_hash(snapshot: Mapping[str, object]) -> str:
    """Return a stable hash for a Rust UDF snapshot payload.

    Returns
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


def _install_udf_config(ctx: SessionContext) -> None:
    installer = getattr(datafusion_internal, "install_codeanatomy_udf_config", None)
    if callable(installer):
        with contextlib.suppress(RuntimeError, TypeError, ValueError):
            installer(ctx)


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

    Returns
    -------
    Mapping[str, object]
        Snapshot payload for diagnostics.

    Raises
    ------
    ValueError
        Raised when async UDF policy configuration is invalid.
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
    _install_udf_config(ctx)
    existing = _registered_snapshot(
        ctx,
        policy=policy,
    )
    if existing is not None:
        return existing
    _RUST_UDF_CONTEXTS.add(ctx)
    _RUST_UDF_POLICIES[ctx] = policy
    return _validated_snapshot(ctx)


__all__ = [
    "RustUdfSnapshot",
    "register_rust_udfs",
    "rust_udf_docs",
    "rust_udf_snapshot",
    "rust_udf_snapshot_bytes",
    "rust_udf_snapshot_hash",
    "rust_udf_snapshot_payload",
    "udf_names_from_snapshot",
    "validate_required_udfs",
    "validate_rust_udf_snapshot",
]
