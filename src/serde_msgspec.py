"""Shared msgspec policy and helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, TypeVar

import msgspec

T = TypeVar("T")


class StructBase(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    omit_defaults=True,
    repr_omit_defaults=True,
    forbid_unknown_fields=True,
):
    """Base struct for internal artifacts."""


_DEFAULT_ORDER: str = "deterministic"


def _json_enc_hook(obj: object) -> object:
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, bytes):
        return obj.hex()
    raise TypeError


def _msgpack_enc_hook(obj: object) -> object:
    if isinstance(obj, Path):
        return str(obj)
    raise TypeError


def _dec_hook(type_hint: Any, obj: object) -> object:
    if type_hint is Path and isinstance(obj, str):
        return Path(obj)
    return obj


JSON_ENCODER = msgspec.json.Encoder(enc_hook=_json_enc_hook, order=_DEFAULT_ORDER)
MSGPACK_ENCODER = msgspec.msgpack.Encoder(enc_hook=_msgpack_enc_hook, order=_DEFAULT_ORDER)


def dumps_json(obj: object, *, pretty: bool = False) -> bytes:
    raw = JSON_ENCODER.encode(obj)
    if not pretty:
        return raw
    return msgspec.json.format(raw, indent=2)


def loads_json(buf: bytes | str, *, type: type[T], strict: bool = True) -> T:
    decoder = msgspec.json.Decoder(type=type, dec_hook=_dec_hook, strict=strict)
    return decoder.decode(buf)


def dumps_msgpack(obj: object) -> bytes:
    return MSGPACK_ENCODER.encode(obj)


def loads_msgpack(buf: bytes, *, type: type[T], strict: bool = True) -> T:
    decoder = msgspec.msgpack.Decoder(type=type, dec_hook=_dec_hook, strict=strict)
    return decoder.decode(buf)


def encode_json_into(obj: object, buf: bytearray) -> None:
    JSON_ENCODER.encode_into(obj, buf)


def encode_json_lines(items: list[object]) -> bytes:
    return JSON_ENCODER.encode_lines(items)


def convert(
    obj: object,
    *,
    type: type[T],
    strict: bool = True,
    from_attributes: bool = False,
) -> T:
    return msgspec.convert(
        obj,
        type=type,
        strict=strict,
        from_attributes=from_attributes,
        dec_hook=_dec_hook,
    )


def to_builtins(obj: object, *, str_keys: bool = True) -> object:
    return msgspec.to_builtins(
        obj,
        order=_DEFAULT_ORDER,
        str_keys=str_keys,
        enc_hook=_json_enc_hook,
    )
