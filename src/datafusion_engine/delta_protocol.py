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
    if gate.min_reader_version is not None:
        if reader_version is None or reader_version < gate.min_reader_version:
            msg = "Delta reader protocol gate failed."
            raise ValueError(msg)
    if gate.min_writer_version is not None:
        if writer_version is None or writer_version < gate.min_writer_version:
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
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            return None
    return None


def _feature_set(value: object) -> set[str]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return {str(item) for item in value if str(item)}
    return set()


__all__ = ["DeltaFeatureGate", "validate_delta_gate"]
