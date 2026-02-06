"""Shared JSON/MsgPack codecs for CQ."""

from __future__ import annotations

from typing import Any

import msgspec

from tools.cq.core.schema import CqResult

JSON_ENCODER = msgspec.json.Encoder(order="deterministic")
JSON_DECODER = msgspec.json.Decoder(strict=True)
JSON_RESULT_DECODER = msgspec.json.Decoder(type=CqResult, strict=True)
MSGPACK_ENCODER = msgspec.msgpack.Encoder()
MSGPACK_DECODER = msgspec.msgpack.Decoder(type=object)
MSGPACK_RESULT_DECODER = msgspec.msgpack.Decoder(type=CqResult)


def dumps_json_value(value: Any, *, indent: int | None = None) -> str:
    """Encode a value to JSON with deterministic ordering.

    Parameters
    ----------
    value
        Value to encode.
    indent
        Optional indentation level.

    Returns:
    -------
    str
        JSON string.
    """
    payload = JSON_ENCODER.encode(value)
    if indent is None:
        return payload.decode("utf-8")
    formatted = msgspec.json.format(payload, indent=indent)
    return formatted.decode("utf-8")


def loads_json_value(payload: bytes | str) -> Any:
    """Decode JSON into a Python value.

    Returns:
    -------
    Any
        Decoded value.
    """
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return JSON_DECODER.decode(payload)


def loads_json_result(payload: bytes | str) -> CqResult:
    """Decode JSON into a CqResult.

    Returns:
    -------
    CqResult
        Decoded result.
    """
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return JSON_RESULT_DECODER.decode(payload)


__all__ = [
    "JSON_DECODER",
    "JSON_ENCODER",
    "JSON_RESULT_DECODER",
    "MSGPACK_DECODER",
    "MSGPACK_ENCODER",
    "MSGPACK_RESULT_DECODER",
    "dumps_json_value",
    "loads_json_result",
    "loads_json_value",
]
