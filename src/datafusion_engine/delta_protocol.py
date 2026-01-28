"""Delta protocol and feature-gate helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class DeltaFeatureGate:
    """Protocol and feature-gating requirements for Delta tables.

    Parameters
    ----------
    min_reader_version:
        Minimum reader protocol version required.
    min_writer_version:
        Minimum writer protocol version required.
    required_reader_features:
        Required Delta reader features.
    required_writer_features:
        Required Delta writer features.
    """

    min_reader_version: int | None = None
    min_writer_version: int | None = None
    required_reader_features: tuple[str, ...] = ()
    required_writer_features: tuple[str, ...] = ()


@dataclass(frozen=True)
class DeltaProtocolSupport:
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

    max_reader_version: int | None = None
    max_writer_version: int | None = None
    supported_reader_features: tuple[str, ...] = ()
    supported_writer_features: tuple[str, ...] = ()


def delta_protocol_compatibility(
    protocol: Mapping[str, object] | None,
    support: DeltaProtocolSupport | None,
) -> Mapping[str, object]:
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
        return {"compatible": None, "reason": "support_unconfigured"}
    if protocol is None:
        return {"compatible": True, "reason": "protocol_unavailable"}
    required_reader = _coerce_int(protocol.get("min_reader_version"))
    required_writer = _coerce_int(protocol.get("min_writer_version"))
    reader_features = _feature_set(protocol.get("reader_features"))
    writer_features = _feature_set(protocol.get("writer_features"))
    supported_reader = set(support.supported_reader_features)
    supported_writer = set(support.supported_writer_features)
    missing_reader = sorted(reader_features - supported_reader)
    missing_writer = sorted(writer_features - supported_writer)
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
    return {
        "compatible": compatible,
        "required_reader_version": required_reader,
        "required_writer_version": required_writer,
        "supported_reader_version": support.max_reader_version,
        "supported_writer_version": support.max_writer_version,
        "required_reader_features": sorted(reader_features),
        "required_writer_features": sorted(writer_features),
        "supported_reader_features": sorted(supported_reader),
        "supported_writer_features": sorted(supported_writer),
        "missing_reader_features": missing_reader,
        "missing_writer_features": missing_writer,
        "reader_version_ok": reader_ok,
        "writer_version_ok": writer_ok,
        "feature_support_ok": features_ok,
    }


def validate_delta_gate(snapshot: Mapping[str, object], gate: DeltaFeatureGate) -> None:
    """Validate Delta protocol and feature gates against snapshot metadata.

    Parameters
    ----------
    snapshot
        Delta snapshot metadata from the control plane.
    gate
        Protocol and feature gate requirements.

    Raises
    ------
    ValueError
        Raised when protocol versions or feature requirements are not satisfied.
    """
    reader_version = _coerce_int(snapshot.get("min_reader_version"))
    writer_version = _coerce_int(snapshot.get("min_writer_version"))
    reader_features = _feature_set(snapshot.get("reader_features"))
    writer_features = _feature_set(snapshot.get("writer_features"))
    if gate.min_reader_version is not None and (
        reader_version is None or reader_version < gate.min_reader_version
    ):
        msg = "Delta reader protocol gate failed."
        raise ValueError(msg)
    if gate.min_writer_version is not None and (
        writer_version is None or writer_version < gate.min_writer_version
    ):
        msg = "Delta writer protocol gate failed."
        raise ValueError(msg)
    if gate.required_reader_features:
        required = set(gate.required_reader_features)
        if not required.issubset(reader_features):
            msg = "Delta reader feature gate failed."
            raise ValueError(msg)
    if gate.required_writer_features:
        required = set(gate.required_writer_features)
        if not required.issubset(writer_features):
            msg = "Delta writer feature gate failed."
            raise ValueError(msg)


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


__all__ = [
    "DeltaFeatureGate",
    "DeltaProtocolSupport",
    "delta_protocol_compatibility",
    "validate_delta_gate",
]
