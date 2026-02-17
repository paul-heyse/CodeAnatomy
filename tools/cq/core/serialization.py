"""CQ serialization helpers using msgspec."""

from __future__ import annotations

from tools.cq.core.contract_codec import (
    decode_json_result,
    decode_msgpack,
    decode_msgpack_result,
    encode_json,
    encode_msgpack,
    to_contract_builtins,
)
from tools.cq.core.schema import CqResult


def dumps_json(result: CqResult, *, indent: int | None = 2) -> str:
    """Serialize a CqResult to JSON.

    Parameters
    ----------
    result
        Result to serialize.
    indent
        Indentation level for pretty printing. Use None for compact JSON.

    Returns:
    -------
    str
        JSON string representation.
    """
    return encode_json(result, indent=indent)


def loads_json(payload: bytes | str) -> CqResult:
    """Deserialize a CqResult from JSON bytes or string.

    Returns:
    -------
    CqResult
        Parsed CQ result.
    """
    return decode_json_result(payload)


def dumps_msgpack(value: object) -> bytes:
    """Serialize an arbitrary value to msgpack bytes.

    Returns:
    -------
    bytes
        MessagePack-encoded bytes.
    """
    return encode_msgpack(value)


def loads_msgpack(payload: bytes | bytearray | memoryview) -> object:
    """Deserialize msgpack bytes to a Python value.

    Returns:
    -------
    object
        Decoded Python value.
    """
    return decode_msgpack(payload)


def loads_msgpack_result(payload: bytes | bytearray | memoryview) -> CqResult:
    """Deserialize msgpack bytes into a CqResult.

    Returns:
    -------
    CqResult
        Decoded CQ result.
    """
    return decode_msgpack_result(payload)


def to_builtins(value: object) -> object:
    """Convert a value to builtins for generic JSON handling.

    Returns:
    -------
    object
        Builtins-only representation.
    """
    return to_contract_builtins(value)


__all__ = [
    "dumps_json",
    "dumps_msgpack",
    "loads_json",
    "loads_msgpack",
    "loads_msgpack_result",
    "to_builtins",
]
