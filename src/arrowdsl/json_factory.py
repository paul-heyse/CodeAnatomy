"""Shared JSON serialization helpers with orjson-backed policies."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path

import orjson

from arrowdsl.core.interop import DataTypeLike, SchemaLike, TableLike
from arrowdsl.schema.schema import schema_to_dict

_UNSET = object()


@dataclass(frozen=True)
class JsonPolicy:
    """Serialization policy for JSON payloads."""

    pretty: bool = False
    sort_keys: bool = False
    append_newline: bool = False
    ascii_only: bool = False
    allow_non_str_keys: bool = False
    strict_integer: bool = False
    naive_utc: bool = False
    utc_z: bool = False
    omit_microseconds: bool = False
    serialize_numpy: bool = False

    @classmethod
    def human(cls) -> JsonPolicy:
        """Return a human-readable JSON policy.

        Returns
        -------
        JsonPolicy
            Policy with pretty formatting and sorted keys.
        """
        return cls(pretty=True, sort_keys=True)

    @classmethod
    def compact(cls) -> JsonPolicy:
        """Return a compact JSON policy with sorted keys.

        Returns
        -------
        JsonPolicy
            Policy with sorted keys and no extra whitespace.
        """
        return cls(sort_keys=True)

    @classmethod
    def canonical_ascii(cls) -> JsonPolicy:
        """Return a JSON policy that enforces ASCII-only output.

        Returns
        -------
        JsonPolicy
            Policy that emits ASCII-only JSON.
        """
        return cls(ascii_only=True)


def _convert_simple(obj: object) -> object:
    if isinstance(obj, SchemaLike):
        return schema_to_dict(obj)
    if isinstance(obj, DataTypeLike):
        return str(obj)
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Decimal):
        return str(obj)
    return _UNSET


def _convert_collection(obj: object) -> object:
    if isinstance(obj, (set, frozenset)):
        return sorted((_json_default(item) for item in obj), key=str)
    if isinstance(obj, tuple):
        return [_json_default(item) for item in obj]
    if isinstance(obj, Mapping):
        return {str(key): _json_default(value) for key, value in obj.items()}

    obj_dict = getattr(obj, "__dict__", None)
    if isinstance(obj_dict, Mapping):
        return {str(key): _json_default(value) for key, value in obj_dict.items()}

    return _UNSET


def _json_default(obj: object) -> object:
    if isinstance(obj, TableLike):
        msg = "Use Arrow table export helpers instead of json_factory."
        raise TypeError(msg)

    result = _convert_simple(obj)
    if result is not _UNSET:
        return result

    result = _convert_collection(obj)
    if result is not _UNSET:
        return result

    return str(obj)


def json_default(value: object) -> object:
    """Return a JSON-serializable representation of ``value``.

    Returns
    -------
    object
        JSON-ready value using ArrowDSL default conversion.
    """
    return _json_default(value)


def _orjson_option(policy: JsonPolicy) -> int:
    option = 0
    if policy.pretty:
        option |= orjson.OPT_INDENT_2
    if policy.sort_keys:
        option |= orjson.OPT_SORT_KEYS
    if policy.append_newline:
        option |= orjson.OPT_APPEND_NEWLINE
    if policy.allow_non_str_keys:
        option |= orjson.OPT_NON_STR_KEYS
    if policy.strict_integer:
        option |= orjson.OPT_STRICT_INTEGER
    if policy.naive_utc:
        option |= orjson.OPT_NAIVE_UTC
    if policy.utc_z:
        option |= orjson.OPT_UTC_Z
    if policy.omit_microseconds:
        option |= orjson.OPT_OMIT_MICROSECONDS
    if policy.serialize_numpy:
        option |= orjson.OPT_SERIALIZE_NUMPY
    return option


def dumps_bytes(value: object, *, policy: JsonPolicy | None = None) -> bytes:
    """Serialize ``value`` to UTF-8 JSON bytes.

    Returns
    -------
    bytes
        JSON-encoded bytes.
    """
    resolved = JsonPolicy() if policy is None else policy
    if resolved.ascii_only:
        text = json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=resolved.sort_keys,
            indent=2 if resolved.pretty else None,
            default=_json_default,
        )
        if resolved.append_newline:
            text = f"{text}\n"
        return text.encode("utf-8")
    return orjson.dumps(value, default=_json_default, option=_orjson_option(resolved))


def dumps_text(value: object, *, policy: JsonPolicy | None = None) -> str:
    """Serialize ``value`` to a UTF-8 JSON string.

    Returns
    -------
    str
        JSON-encoded text.
    """
    return dumps_bytes(value, policy=policy).decode("utf-8")


def dump_path(
    path: str | Path,
    value: object,
    *,
    policy: JsonPolicy | None = None,
    overwrite: bool = True,
) -> str:
    """Write JSON to ``path`` and return the path string.

    Returns
    -------
    str
        Written path as text.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if overwrite and target.exists():
        target.unlink()
    target.write_bytes(dumps_bytes(value, policy=policy))
    return str(target)


def loads(payload: bytes | bytearray | memoryview | str) -> object:
    """Deserialize JSON bytes or text.

    Returns
    -------
    object
        Parsed JSON object.
    """
    return orjson.loads(payload)


__all__ = ["JsonPolicy", "dump_path", "dumps_bytes", "dumps_text", "json_default", "loads"]
