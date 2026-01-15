"""Shared compute FunctionOptions helpers."""

from __future__ import annotations

from typing import Protocol, cast, runtime_checkable

from arrowdsl.core.interop import pc


@runtime_checkable
class FunctionOptionsProto(Protocol):
    """Protocol for Arrow compute function options."""

    def serialize(self) -> bytes:
        """Serialize options to bytes."""
        ...

    @classmethod
    def deserialize(cls, payload: bytes) -> FunctionOptionsProto:
        """Deserialize options from bytes."""
        ...


type FunctionOptionsPayload = bytes | bytearray | FunctionOptionsProto


def serialize_options(options: FunctionOptionsPayload | None) -> bytes | None:
    """Serialize function options to bytes.

    Returns
    -------
    bytes | None
        Serialized payload, or ``None`` when no options are supplied.

    Raises
    ------
    TypeError
        Raised when the options payload is unsupported.
    """
    if options is None:
        return None
    if isinstance(options, bytearray):
        return bytes(options)
    if isinstance(options, bytes):
        return options
    if isinstance(options, FunctionOptionsProto):
        return options.serialize()
    msg = "Function options must be bytes or FunctionOptions."
    raise TypeError(msg)


def deserialize_options(payload: bytes | None) -> FunctionOptionsProto | None:
    """Deserialize function options from bytes.

    Returns
    -------
    FunctionOptionsProto | None
        Deserialized options, or ``None`` when no payload is provided.

    Raises
    ------
    TypeError
        Raised when Arrow compute FunctionOptions are unavailable.
    """
    if payload is None:
        return None
    options_type = cast("type[FunctionOptionsProto] | None", getattr(pc, "FunctionOptions", None))
    if options_type is None:
        msg = "Arrow compute FunctionOptions is unavailable."
        raise TypeError(msg)
    return options_type.deserialize(payload)


__all__ = [
    "FunctionOptionsPayload",
    "FunctionOptionsProto",
    "deserialize_options",
    "serialize_options",
]
