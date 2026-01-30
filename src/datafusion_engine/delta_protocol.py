"""Delta protocol and feature-gate helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Annotated

import msgspec

from datafusion_engine.errors import DataFusionEngineError, ErrorKind
from datafusion_engine.generated.delta_types import DeltaFeatureGate
from serde_msgspec import StructBaseCompat, StructBaseStrict

NonNegInt = Annotated[int, msgspec.Meta(ge=0)]


class DeltaProtocolSupport(StructBaseStrict, frozen=True):
    """Runtime support bounds for Delta protocol and features.

    Parameters
    ----------
    max_reader_version:
        Maximum reader protocol version supported.
    max_writer_version:
        Maximum writer protocol version supported.
    supported_reader_features:
        Reader features supported by the runtime.
    supported_writer_features:
        Writer features supported by the runtime.
    """

    max_reader_version: NonNegInt | None = None
    max_writer_version: NonNegInt | None = None
    supported_reader_features: tuple[str, ...] = ()
    supported_writer_features: tuple[str, ...] = ()


class DeltaProtocolSnapshot(StructBaseCompat, frozen=True):
    """Snapshot of Delta protocol versions and feature flags."""

    min_reader_version: NonNegInt | None = None
    min_writer_version: NonNegInt | None = None
    reader_features: tuple[str, ...] = ()
    writer_features: tuple[str, ...] = ()


class DeltaProtocolCompatibility(StructBaseCompat, frozen=True):
    """Compatibility evaluation for a Delta protocol snapshot."""

    compatible: bool | None
    reason: str | None = None
    required_reader_version: NonNegInt | None = None
    required_writer_version: NonNegInt | None = None
    supported_reader_version: NonNegInt | None = None
    supported_writer_version: NonNegInt | None = None
    required_reader_features: tuple[str, ...] = ()
    required_writer_features: tuple[str, ...] = ()
    supported_reader_features: tuple[str, ...] = ()
    supported_writer_features: tuple[str, ...] = ()
    missing_reader_features: tuple[str, ...] = ()
    missing_writer_features: tuple[str, ...] = ()
    reader_version_ok: bool | None = None
    writer_version_ok: bool | None = None
    feature_support_ok: bool | None = None


def delta_protocol_compatibility(
    protocol: DeltaProtocolSnapshot | Mapping[str, object] | None,
    support: DeltaProtocolSupport | None,
) -> DeltaProtocolCompatibility:
    """Evaluate Delta protocol compatibility against runtime support.

    Parameters
    ----------
    protocol
        Delta protocol payload (min versions/features).
    support
        Runtime support bounds for protocol versions/features.

    Returns
    -------
    Mapping[str, object]
        Compatibility payload describing mismatches and status.
    """
    if support is None:
        return DeltaProtocolCompatibility(
            compatible=None,
            reason="support_unconfigured",
        )
    snapshot = _protocol_snapshot(protocol)
    if snapshot is None:
        return DeltaProtocolCompatibility(
            compatible=True,
            reason="protocol_unavailable",
        )
    required_reader = snapshot.min_reader_version
    required_writer = snapshot.min_writer_version
    reader_features = set(snapshot.reader_features)
    writer_features = set(snapshot.writer_features)
    supported_reader = set(support.supported_reader_features)
    supported_writer = set(support.supported_writer_features)
    missing_reader = tuple(sorted(reader_features - supported_reader))
    missing_writer = tuple(sorted(writer_features - supported_writer))
    reader_ok = (
        support.max_reader_version is None
        or required_reader is None
        or required_reader <= support.max_reader_version
    )
    writer_ok = (
        support.max_writer_version is None
        or required_writer is None
        or required_writer <= support.max_writer_version
    )
    features_ok = not missing_reader and not missing_writer
    compatible = bool(reader_ok and writer_ok and features_ok)
    return DeltaProtocolCompatibility(
        compatible=compatible,
        required_reader_version=required_reader,
        required_writer_version=required_writer,
        supported_reader_version=support.max_reader_version,
        supported_writer_version=support.max_writer_version,
        required_reader_features=tuple(sorted(reader_features)),
        required_writer_features=tuple(sorted(writer_features)),
        supported_reader_features=tuple(sorted(supported_reader)),
        supported_writer_features=tuple(sorted(supported_writer)),
        missing_reader_features=missing_reader,
        missing_writer_features=missing_writer,
        reader_version_ok=reader_ok,
        writer_version_ok=writer_ok,
        feature_support_ok=features_ok,
    )


def validate_delta_gate(
    snapshot: DeltaProtocolSnapshot | Mapping[str, object],
    gate: DeltaFeatureGate,
) -> None:
    """Validate Delta protocol and feature gates against snapshot metadata.

    Raises
    ------
    DataFusionEngineError
        Raised when the snapshot is missing or the extension is unavailable.
    """
    resolved = _protocol_snapshot(snapshot)
    if resolved is None:
        msg = "Delta protocol snapshot is required for gate validation."
        raise DataFusionEngineError(msg, kind=ErrorKind.DELTA)
    try:
        import datafusion_ext
    except ImportError as exc:  # pragma: no cover - extension optional in tests
        msg = "datafusion_ext is required for Delta protocol gate validation."
        raise DataFusionEngineError(msg, kind=ErrorKind.PLUGIN) from exc
    snapshot_payload = {
        "min_reader_version": resolved.min_reader_version,
        "min_writer_version": resolved.min_writer_version,
        "reader_features": list(resolved.reader_features),
        "writer_features": list(resolved.writer_features),
    }
    gate_payload = {
        "min_reader_version": gate.min_reader_version,
        "min_writer_version": gate.min_writer_version,
        "required_reader_features": list(gate.required_reader_features),
        "required_writer_features": list(gate.required_writer_features),
    }
    snapshot_msgpack = msgspec.msgpack.encode(snapshot_payload)
    gate_msgpack = msgspec.msgpack.encode(gate_payload)
    try:
        datafusion_ext.validate_protocol_gate(snapshot_msgpack, gate_msgpack)
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = "Delta protocol gate validation failed."
        raise DataFusionEngineError(msg, kind=ErrorKind.DELTA) from exc


def delta_feature_gate_payload(
    gate: object | None,
) -> dict[str, object] | None:
    """Return a JSON-ready payload for a Delta feature gate.

    Returns
    -------
    dict[str, object] | None
        JSON-ready Delta feature gate payload, or ``None`` when unavailable.
    """
    normalized = _delta_gate_values(gate)
    if normalized is None:
        return None
    min_reader, min_writer, reader_features, writer_features = normalized
    return {
        "min_reader_version": min_reader,
        "min_writer_version": min_writer,
        "required_reader_features": list(reader_features),
        "required_writer_features": list(writer_features),
    }


def delta_feature_gate_tuple(
    gate: object | None,
) -> tuple[int | None, int | None, tuple[str, ...], tuple[str, ...]] | None:
    """Return a tuple payload for Delta feature gates.

    Returns
    -------
    tuple[int | None, int | None, tuple[str, ...], tuple[str, ...]] | None
        Canonical tuple payload for feature gates, or ``None`` when unavailable.
    """
    return _delta_gate_values(gate)


def delta_feature_gate_rust_payload(
    gate: DeltaFeatureGate | None,
) -> tuple[int | None, int | None, list[str] | None, list[str] | None]:
    """Return a Rust-compatible Delta feature gate payload.

    Returns
    -------
    tuple[int | None, int | None, list[str] | None, list[str] | None]
        Rust-compatible gate payload with optional feature lists.
    """
    if gate is None:
        return None, None, None, None
    normalized = _delta_gate_values(gate)
    if normalized is None:
        return None, None, None, None
    min_reader, min_writer, reader_features, writer_features = normalized
    reader_list = list(reader_features) or None
    writer_list = list(writer_features) or None
    return min_reader, min_writer, reader_list, writer_list


def _protocol_snapshot(
    snapshot: DeltaProtocolSnapshot | Mapping[str, object] | None,
) -> DeltaProtocolSnapshot | None:
    if snapshot is None:
        return None
    if isinstance(snapshot, DeltaProtocolSnapshot):
        if (
            snapshot.min_reader_version is None
            and snapshot.min_writer_version is None
            and not snapshot.reader_features
            and not snapshot.writer_features
        ):
            return None
        return snapshot
    if not isinstance(snapshot, Mapping):
        return None
    min_reader_version = _coerce_int(snapshot.get("min_reader_version"))
    min_writer_version = _coerce_int(snapshot.get("min_writer_version"))
    reader_features = tuple(_feature_set(snapshot.get("reader_features")))
    writer_features = tuple(_feature_set(snapshot.get("writer_features")))
    if (
        min_reader_version is None
        and min_writer_version is None
        and not reader_features
        and not writer_features
    ):
        return None
    return DeltaProtocolSnapshot(
        min_reader_version=min_reader_version,
        min_writer_version=min_writer_version,
        reader_features=reader_features,
        writer_features=writer_features,
    )


def _coerce_int(value: object) -> int | None:
    result: int | None = None
    if value is None:
        return result
    if isinstance(value, int):
        result = value
    elif isinstance(value, float):
        result = int(value)
    elif isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                result = int(stripped)
            except ValueError:
                result = None
    return result


def _feature_set(value: object) -> set[str]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return {str(item) for item in value if str(item)}
    return set()


def _feature_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(str(item) for item in value)
    return ()


def _delta_gate_values(
    gate: object | None,
) -> tuple[int | None, int | None, tuple[str, ...], tuple[str, ...]] | None:
    if gate is None:
        return None
    if isinstance(gate, Mapping):
        min_reader_version = _coerce_int(gate.get("min_reader_version"))
        min_writer_version = _coerce_int(gate.get("min_writer_version"))
        required_reader_features = _feature_tuple(gate.get("required_reader_features"))
        required_writer_features = _feature_tuple(gate.get("required_writer_features"))
        return (
            min_reader_version,
            min_writer_version,
            required_reader_features,
            required_writer_features,
        )
    min_reader_version = _coerce_int(getattr(gate, "min_reader_version", None))
    min_writer_version = _coerce_int(getattr(gate, "min_writer_version", None))
    required_reader_features = _feature_tuple(getattr(gate, "required_reader_features", ()))
    required_writer_features = _feature_tuple(getattr(gate, "required_writer_features", ()))
    return (
        min_reader_version,
        min_writer_version,
        required_reader_features,
        required_writer_features,
    )


__all__ = [
    "DeltaFeatureGate",
    "DeltaProtocolCompatibility",
    "DeltaProtocolSnapshot",
    "DeltaProtocolSupport",
    "delta_feature_gate_payload",
    "delta_feature_gate_rust_payload",
    "delta_feature_gate_tuple",
    "delta_protocol_compatibility",
    "validate_delta_gate",
]
