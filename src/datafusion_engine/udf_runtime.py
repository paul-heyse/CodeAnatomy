"""Rust UDF registration helpers."""

from __future__ import annotations

import contextlib
import hashlib
from collections.abc import Mapping, Sequence
from weakref import WeakKeyDictionary, WeakSet

from datafusion import SessionContext

import datafusion_ext
from serde_msgspec import dumps_msgpack

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
    "signature_inputs",
    "return_types",
)


def _build_registry_snapshot(ctx: SessionContext) -> Mapping[str, object]:
    snapshot = datafusion_ext.registry_snapshot(ctx)
    if not isinstance(snapshot, Mapping):
        msg = "datafusion_ext.registry_snapshot returned a non-mapping payload."
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
    payload.setdefault("custom_udfs", [])
    return payload


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
        if not isinstance(alias, str) or not isinstance(target, str):
            msg = "Rust UDF snapshot aliases must map strings to strings."
            raise TypeError(msg)
        names.add(alias)
        names.add(target)
    return frozenset(names)


def validate_rust_udf_snapshot(snapshot: Mapping[str, object]) -> None:
    """Validate structural requirements for a Rust UDF snapshot.

    Raises
    ------
    ValueError
        Raised when required snapshot keys or metadata are missing.
    """
    missing = [key for key in _REQUIRED_SNAPSHOT_KEYS if key not in snapshot]
    if missing:
        msg = f"Rust UDF snapshot missing required keys: {missing}."
        raise ValueError(msg)
    _require_sequence(snapshot, name="scalar")
    _require_sequence(snapshot, name="aggregate")
    _require_sequence(snapshot, name="window")
    _require_sequence(snapshot, name="table")
    _require_mapping(snapshot, name="aliases")
    _require_mapping(snapshot, name="parameter_names")
    signature_inputs = _require_mapping(snapshot, name="signature_inputs")
    return_types = _require_mapping(snapshot, name="return_types")
    names = _snapshot_names(snapshot)
    if names and not signature_inputs:
        msg = "Rust UDF snapshot missing signature_inputs entries."
        raise ValueError(msg)
    if names and not return_types:
        msg = "Rust UDF snapshot missing return_types entries."
        raise ValueError(msg)


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
    missing = [name for name in required if name not in names]
    if missing:
        msg = f"Missing required Rust UDFs: {sorted(missing)}."
        raise ValueError(msg)
    aliases = _require_mapping(snapshot, name="aliases")
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
    snapshot = datafusion_ext.udf_docs_snapshot(ctx)
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
    return hashlib.sha256(payload).hexdigest()


def register_rust_udfs(
    ctx: SessionContext,
    *,
    enable_async: bool = False,
    async_udf_timeout_ms: int | None = None,
    async_udf_batch_size: int | None = None,
) -> Mapping[str, object]:
    """Register Rust-backed UDFs once per session context.

    Returns
    -------
    Mapping[str, object]
        Snapshot payload for diagnostics.

    Raises
    ------
    ValueError
        Raised when async UDF policy configuration is invalid.
    """
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
    policy = (enable_async, async_udf_timeout_ms, async_udf_batch_size)
    if ctx in _RUST_UDF_CONTEXTS:
        existing = _RUST_UDF_POLICIES.get(ctx)
        if (
            existing is not None
            and existing != policy
            and not (
                not enable_async and async_udf_timeout_ms is None and async_udf_batch_size is None
            )
        ):
            msg = "Rust UDFs already registered with a different async policy."
            raise ValueError(msg)
        return _validated_snapshot(ctx)
    datafusion_ext.register_udfs(
        ctx,
        enable_async,
        async_udf_timeout_ms,
        async_udf_batch_size,
    )
    _RUST_UDF_CONTEXTS.add(ctx)
    _RUST_UDF_POLICIES[ctx] = policy
    return _validated_snapshot(ctx)


__all__ = [
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
