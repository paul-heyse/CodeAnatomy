"""Codec helpers for msgspec contract tests."""

from __future__ import annotations

from typing import TypeVar

import msgspec

from serde_msgspec import (
    JSON_ENCODER,
    MSGPACK_ENCODER,
    dumps_json,
    dumps_msgpack,
    loads_json,
    loads_msgpack,
    to_builtins,
)

T = TypeVar("T")


def encode_json(obj: object) -> bytes:
    return JSON_ENCODER.encode(obj)


def encode_json_pretty(obj: object, *, indent: int = 2) -> str:
    raw = encode_json(obj)
    formatted = msgspec.json.format(raw, indent=indent)
    return formatted.decode("utf-8")


def decode_json[T](buf: bytes | str, *, target_type: type[T], strict: bool = True) -> T:
    return loads_json(buf, target_type=target_type, strict=strict)


def encode_msgpack(obj: object) -> bytes:
    return MSGPACK_ENCODER.encode(obj)


def decode_msgpack[T](buf: bytes, *, target_type: type[T], strict: bool = True) -> T:
    return loads_msgpack(buf, target_type=target_type, strict=strict)


def to_builtins_deterministic(obj: object) -> object:
    return to_builtins(obj, str_keys=True)


def dumps_json_bytes(obj: object, *, pretty: bool = False) -> bytes:
    return dumps_json(obj, pretty=pretty)


def dumps_msgpack_bytes(obj: object) -> bytes:
    return dumps_msgpack(obj)
