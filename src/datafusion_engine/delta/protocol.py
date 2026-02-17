"""Delta protocol and feature-gate helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import msgspec

from datafusion_engine.delta.shared_types import DeltaFeatureGate, DeltaProtocolSnapshot, NonNegInt
from datafusion_engine.errors import DataFusionEngineError, ErrorKind
from datafusion_engine.extensions.context_adaptation import resolve_extension_module
from serde_msgspec import StructBaseCompat, StructBaseStrict
from utils.value_coercion import coerce_int


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

    Returns:
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


def combined_table_features(
    compatibility: DeltaProtocolCompatibility,
) -> tuple[str, ...]:
    """Return the sorted union of required reader and writer features.

    Parameters
    ----------
    compatibility
        Compatibility evaluation result.

    Returns:
    -------
    tuple[str, ...]
        Sorted union of all required features.
    """
    return tuple(
        sorted(
            set(compatibility.required_reader_features)
            | set(compatibility.required_writer_features)
        )
    )


def delta_protocol_artifact_payload(
    compatibility: DeltaProtocolCompatibility,
    *,
    table_uri: str | None = None,
    dataset_name: str | None = None,
) -> dict[str, object]:
    """Build a canonical Delta protocol artifact payload from a compatibility result.

    The returned dict conforms to the ``DeltaProtocolArtifact`` schema defined in
    ``serde_artifacts``.

    Parameters
    ----------
    compatibility
        Compatibility evaluation result.
    table_uri
        Optional table URI for context.
    dataset_name
        Optional dataset name for context.

    Returns:
    -------
    dict[str, object]
        Payload matching the ``DeltaProtocolArtifact`` schema.
    """
    from serde_msgspec import to_builtins_mapping

    result = dict(to_builtins_mapping(compatibility, str_keys=True))
    if table_uri is not None:
        result["table_uri"] = table_uri
    if dataset_name is not None:
        result["dataset_name"] = dataset_name
    return result


def validate_delta_gate(
    snapshot: DeltaProtocolSnapshot | Mapping[str, object],
    gate: DeltaFeatureGate,
) -> None:
    """Validate Delta protocol and feature gates against snapshot metadata.

    Args:
        snapshot: Description.
        gate: Description.

    Raises:
        DataFusionEngineError: If the operation cannot be completed.
    """
    resolved = _protocol_snapshot(snapshot)
    if resolved is None:
        msg = "Delta protocol snapshot is required for gate validation."
        raise DataFusionEngineError(msg, kind=ErrorKind.DELTA)
    module_names = ("datafusion_engine.extensions.datafusion_ext",)
    resolved_module = resolve_extension_module(module_names, entrypoint="validate_protocol_gate")
    if resolved_module is None:
        msg = "Delta protocol gate validation requires datafusion_ext."
        raise DataFusionEngineError(msg, kind=ErrorKind.PLUGIN)
    _module_name, module = resolved_module
    validator = getattr(module, "validate_protocol_gate", None)
    if not callable(validator):
        msg = "Delta protocol gate validation hook is unavailable in the extension module."
        raise DataFusionEngineError(msg, kind=ErrorKind.PLUGIN)
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
        validator(snapshot_msgpack, gate_msgpack)
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = "Delta protocol gate validation failed."
        raise DataFusionEngineError(msg, kind=ErrorKind.DELTA) from exc


def delta_feature_gate_rust_payload(
    gate: DeltaFeatureGate | None,
) -> tuple[int | None, int | None, list[str] | None, list[str] | None]:
    """Return a Rust-compatible Delta feature gate payload.

    Returns:
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
        resolved = snapshot
    elif isinstance(snapshot, Mapping):
        try:
            resolved = msgspec.convert(snapshot, type=DeltaProtocolSnapshot, strict=False)
        except msgspec.ValidationError:
            resolved = None
    else:
        resolved = None
    if resolved is None:
        return None
    if (
        resolved.min_reader_version is None
        and resolved.min_writer_version is None
        and not resolved.reader_features
        and not resolved.writer_features
    ):
        return None
    return resolved


def _feature_set(value: object) -> set[str]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return {str(item) for item in value if str(item)}
    return set()


def _feature_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(str(item) for item in value)
    return ()


def _delta_gate_values(
    gate: DeltaFeatureGate | Mapping[str, object] | None,
) -> tuple[int | None, int | None, tuple[str, ...], tuple[str, ...]] | None:
    if gate is None:
        return None
    if isinstance(gate, Mapping):
        min_reader_version = coerce_int(gate.get("min_reader_version"))
        min_writer_version = coerce_int(gate.get("min_writer_version"))
        required_reader_features = _feature_tuple(gate.get("required_reader_features"))
        required_writer_features = _feature_tuple(gate.get("required_writer_features"))
        return (
            min_reader_version,
            min_writer_version,
            required_reader_features,
            required_writer_features,
        )
    min_reader_version = coerce_int(getattr(gate, "min_reader_version", None))
    min_writer_version = coerce_int(getattr(gate, "min_writer_version", None))
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
    "combined_table_features",
    "delta_feature_gate_rust_payload",
    "delta_protocol_artifact_payload",
    "delta_protocol_compatibility",
    "validate_delta_gate",
]
