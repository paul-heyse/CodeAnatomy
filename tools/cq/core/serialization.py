"""CQ serialization helpers using msgspec."""

from __future__ import annotations

from typing import Any

import msgspec

from tools.cq.core.schema import CqResult

_JSON_ENCODER = msgspec.json.Encoder(order="deterministic")
_JSON_DECODER = msgspec.json.Decoder(type=CqResult)
_MSGPACK_ENCODER = msgspec.msgpack.Encoder()
_MSGPACK_DECODER = msgspec.msgpack.Decoder(type=CqResult)
_GENERIC_MSGPACK_DECODER = msgspec.msgpack.Decoder(type=object)


def dumps_json(result: CqResult, *, indent: int | None = 2) -> str:
    """Serialize a CqResult to JSON.

    Parameters
    ----------
    result
        Result to serialize.
    indent
        Indentation level for pretty printing. Use None for compact JSON.

    Returns
    -------
    str
        JSON string representation.
    """
    payload = _JSON_ENCODER.encode(result)
    if indent is None:
        return payload.decode("utf-8")
    formatted = msgspec.json.format(payload, indent=indent)
    return formatted.decode("utf-8")


def loads_json(payload: bytes | str) -> CqResult:
    """Deserialize a CqResult from JSON bytes or string.

    Returns
    -------
    CqResult
        Parsed CQ result.
    """
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return _JSON_DECODER.decode(payload)


def dumps_msgpack(value: Any) -> bytes:
    """Serialize an arbitrary value to msgpack bytes.

    Returns
    -------
    bytes
        MessagePack-encoded bytes.
    """
    return _MSGPACK_ENCODER.encode(value)


def loads_msgpack(payload: bytes | bytearray | memoryview) -> Any:
    """Deserialize msgpack bytes to a Python value.

    Returns
    -------
    Any
        Decoded Python value.
    """
    return _GENERIC_MSGPACK_DECODER.decode(payload)


def loads_msgpack_result(payload: bytes | bytearray | memoryview) -> CqResult:
    """Deserialize msgpack bytes into a CqResult.

    Returns
    -------
    CqResult
        Decoded CQ result.
    """
    return _MSGPACK_DECODER.decode(payload)


def to_builtins(value: Any) -> Any:
    """Convert a value to builtins for generic JSON handling.

    Returns
    -------
    Any
        Builtins-only representation.
    """
    return msgspec.to_builtins(value)


__all__ = [
    "dumps_json",
    "dumps_msgpack",
    "loads_json",
    "loads_msgpack",
    "loads_msgpack_result",
    "to_builtins",
]
