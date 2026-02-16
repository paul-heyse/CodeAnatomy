"""Canonical contract codec and conversion helpers for CQ boundaries."""

from __future__ import annotations

from collections.abc import Iterable
from typing import cast

import msgspec

from tools.cq.core.contracts_constraints import enforce_mapping_constraints
from tools.cq.core.schema import CqResult

JSON_ENCODER = msgspec.json.Encoder(order="deterministic")
JSON_DECODER = msgspec.json.Decoder(strict=True)
JSON_RESULT_DECODER = msgspec.json.Decoder(type=CqResult, strict=True)
MSGPACK_ENCODER = msgspec.msgpack.Encoder()
MSGPACK_DECODER = msgspec.msgpack.Decoder(type=object)
MSGPACK_RESULT_DECODER = msgspec.msgpack.Decoder(type=CqResult)


def encode_json(value: object, *, indent: int | None = None) -> str:
    """Encode any contract payload to deterministic JSON.

    Returns:
        str: Function return value.
    """
    payload = JSON_ENCODER.encode(to_contract_builtins(value))
    if indent is None:
        return payload.decode("utf-8")
    return msgspec.json.format(payload, indent=indent).decode("utf-8")


def decode_json(payload: bytes | str) -> object:
    """Decode JSON payload to builtins value.

    Returns:
        object: Function return value.
    """
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return JSON_DECODER.decode(payload)


def decode_json_result(payload: bytes | str) -> CqResult:
    """Decode JSON payload to typed CQ result.

    Returns:
        CqResult: Function return value.
    """
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return JSON_RESULT_DECODER.decode(payload)


def encode_msgpack(value: object) -> bytes:
    """Encode payload to msgpack bytes.

    Returns:
        bytes: Function return value.
    """
    return MSGPACK_ENCODER.encode(value)


def decode_msgpack(payload: bytes | bytearray | memoryview) -> object:
    """Decode msgpack payload to builtins value.

    Returns:
        object: Function return value.
    """
    return MSGPACK_DECODER.decode(payload)


def decode_msgpack_result(payload: bytes | bytearray | memoryview) -> CqResult:
    """Decode msgpack payload to typed CQ result.

    Returns:
        CqResult: Function return value.
    """
    return MSGPACK_RESULT_DECODER.decode(payload)


def to_contract_builtins(value: object) -> object:
    """Convert a CQ value to builtins with deterministic contract settings.

    Returns:
        object: Function return value.
    """
    return msgspec.to_builtins(value, order="deterministic", str_keys=True)


def to_public_dict(value: msgspec.Struct) -> dict[str, object]:
    """Convert one msgspec Struct into mapping payload.

    Returns:
        dict[str, object]: Function return value.

    Raises:
        TypeError: Raised when the payload is not map-like.
    """
    payload = to_contract_builtins(value)
    if isinstance(payload, dict):
        return cast("dict[str, object]", payload)
    msg = f"Expected dict payload, got {type(payload).__name__}"
    raise TypeError(msg)


def to_public_list(values: Iterable[msgspec.Struct]) -> list[dict[str, object]]:
    """Convert iterable of structs into mapping rows.

    Returns:
        list[dict[str, object]]: Function return value.
    """
    return [to_public_dict(value) for value in values]


def require_mapping(value: object) -> dict[str, object]:
    """Require mapping-shaped builtins payload.

    Returns:
        dict[str, object]: Function return value.

    Raises:
        TypeError: Raised when the payload cannot be represented as a mapping.
    """
    payload = to_contract_builtins(value)
    if isinstance(payload, dict):
        enforce_mapping_constraints(payload)
        return cast("dict[str, object]", payload)
    msg = f"Expected mapping payload, got {type(payload).__name__}"
    raise TypeError(msg)


def dumps_json_value(value: object, *, indent: int | None = None) -> str:
    """Encode a value to JSON with deterministic ordering.

    Returns:
        str: Function return value.
    """
    return encode_json(value, indent=indent)


def loads_json_value(payload: bytes | str) -> object:
    """Decode JSON into a Python value.

    Returns:
        object: Function return value.
    """
    return decode_json(payload)


def loads_json_result(payload: bytes | str) -> CqResult:
    """Decode JSON into a CqResult.

    Returns:
        CqResult: Function return value.
    """
    return decode_json_result(payload)


__all__ = [
    "JSON_DECODER",
    "JSON_ENCODER",
    "JSON_RESULT_DECODER",
    "MSGPACK_DECODER",
    "MSGPACK_ENCODER",
    "MSGPACK_RESULT_DECODER",
    "decode_json",
    "decode_json_result",
    "decode_msgpack",
    "decode_msgpack_result",
    "dumps_json_value",
    "encode_json",
    "encode_msgpack",
    "loads_json_result",
    "loads_json_value",
    "require_mapping",
    "to_contract_builtins",
    "to_public_dict",
    "to_public_list",
]
