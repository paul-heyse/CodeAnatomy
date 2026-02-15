"""Shared JSON/MsgPack codecs for CQ."""

from __future__ import annotations

from typing import Any

from tools.cq.core.contract_codec import (
    JSON_DECODER,
    JSON_ENCODER,
    JSON_RESULT_DECODER,
    MSGPACK_DECODER,
    MSGPACK_ENCODER,
    MSGPACK_RESULT_DECODER,
    decode_json,
    decode_json_result,
    encode_json,
)
from tools.cq.core.schema import CqResult


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
    return encode_json(value, indent=indent)


def loads_json_value(payload: bytes | str) -> Any:
    """Decode JSON into a Python value.

    Returns:
    -------
    Any
        Decoded value.
    """
    return decode_json(payload)


def loads_json_result(payload: bytes | str) -> CqResult:
    """Decode JSON into a CqResult.

    Returns:
    -------
    CqResult
        Decoded result.
    """
    return decode_json_result(payload)


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
