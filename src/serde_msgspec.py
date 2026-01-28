"""Shared msgspec policy and helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, Protocol, cast

import msgspec

from serde_msgspec_ext import SCHEMA_EXT_CODE


class StructBase(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    omit_defaults=True,
    repr_omit_defaults=True,
    forbid_unknown_fields=True,
):
    """Base struct for internal artifacts."""


_DEFAULT_ORDER: Literal["deterministic"] = "deterministic"


class _MsgpackBuffer(Protocol):
    def to_pybytes(self) -> bytes:
        """Return the buffer as raw bytes."""
        ...


class _ArrowSchema(Protocol):
    def serialize(self) -> _MsgpackBuffer:
        """Serialize schema into a bytes-like buffer."""
        ...


def _json_enc_hook(obj: object) -> object:
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return bytes(obj).hex()
    raise TypeError


def _msgpack_enc_hook(obj: object) -> object:
    if isinstance(obj, Path):
        return str(obj)
    try:
        import pyarrow as pa
    except ImportError:
        pa = None
    if pa is not None and isinstance(obj, pa.Schema):
        schema = cast("_ArrowSchema", obj)
        return msgspec.msgpack.Ext(
            SCHEMA_EXT_CODE,
            schema.serialize().to_pybytes(),
        )
    raise TypeError


def _dec_hook(type_hint: Any, obj: object) -> object:
    if type_hint is Path and isinstance(obj, str):
        return Path(obj)
    return obj


def _msgpack_ext_hook(code: int, data: memoryview) -> object:
    if code == SCHEMA_EXT_CODE:
        try:
            import pyarrow as pa
        except ImportError:
            return msgspec.msgpack.Ext(code, data.tobytes())
        return pa.ipc.read_schema(data)
    return msgspec.msgpack.Ext(code, data.tobytes())


JSON_ENCODER = msgspec.json.Encoder(enc_hook=_json_enc_hook, order=_DEFAULT_ORDER)
MSGPACK_ENCODER = msgspec.msgpack.Encoder(enc_hook=_msgpack_enc_hook, order=_DEFAULT_ORDER)


def json_default(obj: object) -> object:
    """Return a JSON-serializable representation for custom objects.

    Parameters
    ----------
    obj
        Object to encode.

    Returns
    -------
    object
        JSON-serializable value.

    Raises
    ------
    TypeError
        Raised when the object cannot be serialized.
    """
    if isinstance(obj, msgspec.Struct):
        return msgspec.to_builtins(obj)
    return _json_enc_hook(obj)


def dumps_json(obj: object, *, pretty: bool = False) -> bytes:
    """Serialize an object to JSON bytes.

    Parameters
    ----------
    obj
        Object to serialize.
    pretty
        Whether to format with indentation.

    Returns
    -------
    bytes
        JSON payload.
    """
    raw = JSON_ENCODER.encode(obj)
    if not pretty:
        return raw
    return msgspec.json.format(raw, indent=2)


def loads_json[T](buf: bytes | str, *, target_type: type[T], strict: bool = True) -> T:
    """Deserialize JSON bytes into the requested type.

    Parameters
    ----------
    buf
        JSON payload.
    target_type
        Target type for decoding.
    strict
        Whether to enforce strict decoding.

    Returns
    -------
    T
        Decoded payload.
    """
    decoder = msgspec.json.Decoder(
        type=target_type,
        dec_hook=_dec_hook,
        strict=strict,
    )
    return decoder.decode(buf)


def dumps_msgpack(obj: object) -> bytes:
    """Serialize an object to MessagePack bytes.

    Parameters
    ----------
    obj
        Object to serialize.

    Returns
    -------
    bytes
        MessagePack payload.
    """
    return MSGPACK_ENCODER.encode(obj)


def loads_msgpack[T](buf: bytes, *, target_type: type[T], strict: bool = True) -> T:
    """Deserialize MessagePack bytes into the requested type.

    Parameters
    ----------
    buf
        MessagePack payload.
    target_type
        Target type for decoding.
    strict
        Whether to enforce strict decoding.

    Returns
    -------
    T
        Decoded payload.
    """
    decoder = msgspec.msgpack.Decoder(
        type=target_type,
        dec_hook=_dec_hook,
        ext_hook=_msgpack_ext_hook,
        strict=strict,
    )
    return decoder.decode(buf)


def encode_json_into(obj: object, buf: bytearray) -> None:
    """Encode an object into a provided byte buffer.

    Parameters
    ----------
    obj
        Object to encode.
    buf
        Byte buffer to extend.
    """
    JSON_ENCODER.encode_into(obj, buf)


def encode_json_lines(items: list[object]) -> bytes:
    """Serialize items to JSON Lines bytes.

    Parameters
    ----------
    items
        Items to serialize.

    Returns
    -------
    bytes
        JSON Lines payload.
    """
    return JSON_ENCODER.encode_lines(items)


def convert[T](
    obj: object,
    *,
    target_type: type[T],
    strict: bool = True,
    from_attributes: bool = False,
) -> T:
    """Convert an object into a target type.

    Parameters
    ----------
    obj
        Object to convert.
    target_type
        Target type for conversion.
    strict
        Whether to enforce strict conversion.
    from_attributes
        Whether to read attributes from objects.

    Returns
    -------
    T
        Converted payload.
    """
    return msgspec.convert(
        obj,
        type=target_type,
        strict=strict,
        from_attributes=from_attributes,
        dec_hook=_dec_hook,
    )


def to_builtins(obj: object, *, str_keys: bool = True) -> object:
    """Convert an object into builtin JSON-friendly types.

    Parameters
    ----------
    obj
        Object to convert.
    str_keys
        Whether to coerce mapping keys to strings.

    Returns
    -------
    object
        Builtin-friendly representation.
    """
    return msgspec.to_builtins(
        obj,
        order=_DEFAULT_ORDER,
        str_keys=str_keys,
        enc_hook=_json_enc_hook,
    )


def is_unset(value: object) -> bool:
    """Return True when the value is msgspec.UNSET.

    Parameters
    ----------
    value
        Value to inspect.

    Returns
    -------
    bool
        True when the value is msgspec.UNSET.
    """
    return value is msgspec.UNSET


def coalesce_unset[T](value: T | msgspec.UnsetType, default: T) -> T:
    """Return a default when value is msgspec.UNSET.

    Parameters
    ----------
    value
        Value that may be msgspec.UNSET.
    default
        Default to use when value is msgspec.UNSET.

    Returns
    -------
    T
        Value or the provided default.
    """
    return default if value is msgspec.UNSET else value


def coalesce_unset_or_none[T](value: T | msgspec.UnsetType | None, default: T) -> T:
    """Return a default when value is msgspec.UNSET or None.

    Parameters
    ----------
    value
        Value that may be msgspec.UNSET or None.
    default
        Default to use when value is msgspec.UNSET or None.

    Returns
    -------
    T
        Value or the provided default.
    """
    return default if value is msgspec.UNSET or value is None else value


def unset_to_none[T](value: T | msgspec.UnsetType | None) -> T | None:
    """Normalize msgspec.UNSET to None.

    Parameters
    ----------
    value
        Value that may be msgspec.UNSET.

    Returns
    -------
    T | None
        Value or None when the input was msgspec.UNSET.
    """
    return None if value is msgspec.UNSET else value
