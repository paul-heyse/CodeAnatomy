"""Normalize OpenTelemetry attributes for CodeAnatomy telemetry."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from typing import cast

from opentelemetry.util.types import AttributeValue

_ATTRIBUTE_COUNT_LIMIT = os.environ.get("OTEL_ATTRIBUTE_COUNT_LIMIT")
_ATTRIBUTE_VALUE_LENGTH_LIMIT = os.environ.get("OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT")
_REDACT_KEYS_ENV = os.environ.get("CODEANATOMY_OTEL_REDACT_KEYS")


def _limit_to_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value.strip())
    except ValueError:
        return None


_MAX_ATTRIBUTES = _limit_to_int(_ATTRIBUTE_COUNT_LIMIT)
_MAX_ATTRIBUTE_LENGTH = _limit_to_int(_ATTRIBUTE_VALUE_LENGTH_LIMIT)
_DEFAULT_REDACT_KEYS = "authorization,cookie,password,secret,token,api_key"
_REDACT_KEYS = {
    part.strip().lower()
    for part in (_REDACT_KEYS_ENV or _DEFAULT_REDACT_KEYS).split(",")
    if part.strip()
}


def _should_redact(key: str) -> bool:
    lowered = key.lower()
    return any(part in lowered for part in _REDACT_KEYS)


def _is_scalar(value: object) -> bool:
    return isinstance(value, (str, bool, int, float))


def _normalize_sequence(values: Sequence[object]) -> AttributeValue:
    normalized: list[object] = []
    for item in values:
        if item is None:
            continue
        if _is_scalar(item):
            normalized.append(item)
        else:
            normalized.append(str(item))
    if not normalized:
        return []
    if all(isinstance(item, bool) for item in normalized):
        return [bool(item) for item in normalized]
    if all(isinstance(item, int) and not isinstance(item, bool) for item in normalized):
        return [cast("int", item) for item in normalized]
    if all(isinstance(item, float) for item in normalized):
        return [cast("float", item) for item in normalized]
    if all(isinstance(item, (int, float)) and not isinstance(item, bool) for item in normalized):
        return [float(cast("int | float", item)) for item in normalized]
    return [_truncate_str(str(item)) for item in normalized]


def _truncate_str(value: str) -> str:
    if _MAX_ATTRIBUTE_LENGTH is None:
        return value
    if _MAX_ATTRIBUTE_LENGTH <= 0:
        return ""
    return value[: _MAX_ATTRIBUTE_LENGTH]


def _normalize_value(value: object) -> AttributeValue:
    if _is_scalar(value):
        if isinstance(value, str):
            return _truncate_str(value)
        return cast("AttributeValue", value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return _truncate_str(bytes(value).hex())
    if isinstance(value, Mapping):
        return _truncate_str(json.dumps(value, sort_keys=True, default=str))
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview)):
        return _normalize_sequence(list(value))
    return _truncate_str(str(value))


def normalize_attributes(attrs: Mapping[str, object] | None) -> dict[str, AttributeValue]:
    """Normalize raw attribute values into OpenTelemetry-safe types.

    Parameters
    ----------
    attrs
        Raw attribute mapping.

    Returns
    -------
    dict[str, AttributeValue]
        Normalized attribute mapping with OpenTelemetry-safe values.
    """
    if not attrs:
        return {}
    normalized: dict[str, AttributeValue] = {}
    for key, value in attrs.items():
        if value is None:
            continue
        key_str = str(key)
        if _should_redact(key_str):
            normalized[key_str] = _truncate_str("[redacted]")
            continue
        normalized[key_str] = _normalize_value(value)
    if _MAX_ATTRIBUTES is None:
        return normalized
    if _MAX_ATTRIBUTES <= 0:
        return {}
    if len(normalized) <= _MAX_ATTRIBUTES:
        return normalized
    limited: dict[str, AttributeValue] = {}
    for key in sorted(normalized.keys())[: _MAX_ATTRIBUTES]:
        limited[key] = normalized[key]
    return limited


__all__ = ["normalize_attributes"]
