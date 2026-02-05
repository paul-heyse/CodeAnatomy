"""Shared msgspec policy and helpers."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Literal, Protocol, cast

import msgspec

from serde_msgspec_ext import (
    EXECUTION_PLAN_PROTO_EXT_CODE,
    LOGICAL_PLAN_PROTO_EXT_CODE,
    OPTIMIZED_PLAN_PROTO_EXT_CODE,
    SCHEMA_EXT_CODE,
    SUBSTRAIT_EXT_CODE,
    ExecutionPlanProtoBytes,
    LogicalPlanProtoBytes,
    OptimizedPlanProtoBytes,
    SubstraitBytes,
)
from serde_msgspec_inspect import inspect_to_builtins


class StructBaseStrict(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    omit_defaults=True,
    repr_omit_defaults=True,
    forbid_unknown_fields=True,
):
    """Base struct for strict contracts."""


class StructBaseCompat(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    omit_defaults=True,
    repr_omit_defaults=True,
    forbid_unknown_fields=False,
):
    """Base struct for forward-compatible persisted artifacts."""


class StructBaseHotPath(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    omit_defaults=True,
    repr_omit_defaults=True,
    forbid_unknown_fields=False,
    gc=False,
    cache_hash=True,
):
    """Base struct for high-volume, immutable artifacts."""


StructBase = StructBaseStrict


_DEFAULT_ORDER: Literal["deterministic"] = "deterministic"

_VALIDATION_RE = re.compile(r"^(?P<summary>.*?)(?:\\s+-\\s+at\\s+`(?P<path>[^`]+)`)?$")


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
    if isinstance(obj, type):
        return f"{obj.__module__}.{obj.__qualname__}"
    if obj is msgspec.NODEFAULT:
        return "NODEFAULT"
    if isinstance(
        obj,
        (
            SubstraitBytes,
            LogicalPlanProtoBytes,
            OptimizedPlanProtoBytes,
            ExecutionPlanProtoBytes,
        ),
    ):
        return obj.data.hex()
    if isinstance(obj, msgspec.Raw):
        return bytes(obj).hex()
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
    if isinstance(obj, SubstraitBytes):
        return msgspec.msgpack.Ext(SUBSTRAIT_EXT_CODE, obj.data)
    if isinstance(obj, LogicalPlanProtoBytes):
        return msgspec.msgpack.Ext(LOGICAL_PLAN_PROTO_EXT_CODE, obj.data)
    if isinstance(obj, OptimizedPlanProtoBytes):
        return msgspec.msgpack.Ext(OPTIMIZED_PLAN_PROTO_EXT_CODE, obj.data)
    if isinstance(obj, ExecutionPlanProtoBytes):
        return msgspec.msgpack.Ext(EXECUTION_PLAN_PROTO_EXT_CODE, obj.data)
    raise TypeError


def _dec_hook(type_hint: Any, obj: object) -> object:
    if not isinstance(obj, str):
        return obj
    converters: dict[object, Callable[[str], object]] = {
        Path: Path,
        SubstraitBytes: lambda value: SubstraitBytes(bytes.fromhex(value)),
        LogicalPlanProtoBytes: lambda value: LogicalPlanProtoBytes(bytes.fromhex(value)),
        OptimizedPlanProtoBytes: lambda value: OptimizedPlanProtoBytes(bytes.fromhex(value)),
        ExecutionPlanProtoBytes: lambda value: ExecutionPlanProtoBytes(bytes.fromhex(value)),
        msgspec.Raw: lambda value: msgspec.Raw(bytes.fromhex(value)),
    }
    handler = converters.get(type_hint)
    if handler is None:
        return obj
    return handler(obj)


def _msgpack_ext_hook(code: int, data: memoryview) -> object:
    if code == SCHEMA_EXT_CODE:
        try:
            import pyarrow as pa
        except ImportError:
            return msgspec.msgpack.Ext(code, data.tobytes())
        return pa.ipc.read_schema(data)
    handler_map = {
        SUBSTRAIT_EXT_CODE: SubstraitBytes,
        LOGICAL_PLAN_PROTO_EXT_CODE: LogicalPlanProtoBytes,
        OPTIMIZED_PLAN_PROTO_EXT_CODE: OptimizedPlanProtoBytes,
        EXECUTION_PLAN_PROTO_EXT_CODE: ExecutionPlanProtoBytes,
    }
    handler = handler_map.get(code)
    if handler is None:
        return msgspec.msgpack.Ext(code, data.tobytes())
    return handler(data.tobytes())


JSON_ENCODER = msgspec.json.Encoder(
    enc_hook=_json_enc_hook,
    order=_DEFAULT_ORDER,
    decimal_format="string",
    uuid_format="canonical",
)
JSON_ENCODER_SORTED = msgspec.json.Encoder(
    enc_hook=_json_enc_hook,
    order="sorted",
    decimal_format="string",
    uuid_format="canonical",
)
MSGPACK_ENCODER = msgspec.msgpack.Encoder(
    enc_hook=_msgpack_enc_hook,
    order=_DEFAULT_ORDER,
    decimal_format="string",
    uuid_format="canonical",
)


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
    """
    if isinstance(obj, msgspec.Struct):
        return msgspec.to_builtins(obj)
    return _json_enc_hook(obj)


def json_schema(struct: type[msgspec.Struct]) -> Mapping[str, object]:
    """Return a JSON Schema 2020-12 payload for a msgspec struct.

    Parameters
    ----------
    struct
        msgspec struct type.

    Returns
    -------
    Mapping[str, object]
        JSON Schema payload.
    """
    return cast("Mapping[str, object]", msgspec.json.schema(struct))


def export_json_schemas(
    structs: Sequence[type[msgspec.Struct]],
    *,
    output_dir: Path,
) -> tuple[Path, ...]:
    """Export JSON Schema payloads for msgspec structs.

    Parameters
    ----------
    structs
        Sequence of msgspec struct types to export.
    output_dir
        Directory to write schema files into.

    Returns
    -------
    tuple[Path, ...]
        Paths to the generated schema files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for struct in structs:
        schema_payload = json_schema(struct)
        payload_bytes = msgspec.json.encode(schema_payload)
        target = output_dir / f"{struct.__name__}.schema.json"
        target.write_text(payload_bytes.decode("utf-8"))
        paths.append(target)
    return tuple(paths)


def validation_error_payload(exc: msgspec.ValidationError) -> dict[str, str]:
    """Normalize a msgspec ValidationError for diagnostics.

    Parameters
    ----------
    exc
        ValidationError raised by msgspec decoding/conversion.

    Returns
    -------
    dict[str, str]
        Normalized error payload containing type, summary, and optional path.
    """
    message = str(exc).strip()
    match = _VALIDATION_RE.match(message)
    payload: dict[str, str] = {"type": exc.__class__.__name__}
    if match:
        summary = (match.group("summary") or "").strip()
        if summary:
            payload["summary"] = summary
        path = match.group("path")
        if path:
            payload["path"] = path
        return payload
    payload["summary"] = message
    return payload


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


def dumps_json_sorted(obj: object, *, pretty: bool = False) -> bytes:
    """Serialize an object to JSON bytes with sorted keys.

    Parameters
    ----------
    obj
        Object to serialize.
    pretty
        Whether to format with indentation.

    Returns
    -------
    bytes
        JSON payload with sorted keys.
    """
    raw = JSON_ENCODER_SORTED.encode(obj)
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


def decode_json_lines[T](buf: bytes, *, target_type: type[T], strict: bool = True) -> list[T]:
    """Deserialize JSON Lines bytes into a list of typed payloads.

    Parameters
    ----------
    buf
        JSON Lines payload.
    target_type
        Target type for decoding.
    strict
        Whether to enforce strict decoding.

    Returns
    -------
    list[T]
        Decoded payloads.
    """
    decoder = msgspec.json.Decoder(
        type=target_type,
        dec_hook=_dec_hook,
        strict=strict,
    )
    return decoder.decode_lines(buf)


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


def convert_from_attributes[T](
    obj: object,
    *,
    target_type: type[T],
    strict: bool = True,
) -> T:
    """Convert an object into a target type using attribute access.

    Parameters
    ----------
    obj
        Object to convert.
    target_type
        Target type for conversion.
    strict
        Whether to enforce strict conversion.

    Returns
    -------
    T
        Converted payload.
    """
    return convert(
        obj,
        target_type=target_type,
        strict=strict,
        from_attributes=True,
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


def to_builtins_sorted(obj: object, *, str_keys: bool = True) -> object:
    """Convert an object into builtin JSON-friendly types with sorted keys.

    Parameters
    ----------
    obj
        Object to convert.
    str_keys
        Whether to coerce mapping keys to strings.

    Returns
    -------
    object
        Builtin-friendly representation with sorted mapping keys.
    """
    if obj.__class__.__module__ == "msgspec.inspect":
        return inspect_to_builtins(obj, str_keys=str_keys)
    if isinstance(obj, (list, tuple)) and obj:
        module = obj[0].__class__.__module__
        if module == "msgspec.inspect":
            return inspect_to_builtins(obj, str_keys=str_keys)
    return msgspec.to_builtins(
        obj,
        order="sorted",
        str_keys=str_keys,
        enc_hook=_json_enc_hook,
    )


def ensure_raw(payload: bytes | msgspec.Raw, *, copy: bool = False) -> msgspec.Raw:
    """Return a msgspec.Raw wrapper, optionally detaching the buffer.

    Parameters
    ----------
    payload
        Bytes or existing Raw payload.
    copy
        Whether to detach the Raw buffer with ``Raw.copy()``.

    Returns
    -------
    msgspec.Raw
        Raw wrapper for the payload.
    """
    raw = payload if isinstance(payload, msgspec.Raw) else msgspec.Raw(payload)
    return raw.copy() if copy else raw


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
