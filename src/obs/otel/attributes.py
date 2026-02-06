"""Normalize OpenTelemetry attributes for CodeAnatomy telemetry."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import cast

from opentelemetry.util.types import AttributeValue

from utils.env_utils import env_list, env_value

_ATTRIBUTE_COUNT_LIMIT = env_value("OTEL_ATTRIBUTE_COUNT_LIMIT")
_ATTRIBUTE_VALUE_LENGTH_LIMIT = env_value("OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT")
_LOGRECORD_ATTRIBUTE_COUNT_LIMIT = env_value("OTEL_LOGRECORD_ATTRIBUTE_COUNT_LIMIT")
_LOGRECORD_ATTRIBUTE_VALUE_LENGTH_LIMIT = env_value("OTEL_LOGRECORD_ATTRIBUTE_VALUE_LENGTH_LIMIT")


def _limit_to_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value.strip())
    except ValueError:
        return None


_MAX_ATTRIBUTES = _limit_to_int(_ATTRIBUTE_COUNT_LIMIT)
_MAX_ATTRIBUTE_LENGTH = _limit_to_int(_ATTRIBUTE_VALUE_LENGTH_LIMIT)
_MAX_LOGRECORD_ATTRIBUTES = _limit_to_int(_LOGRECORD_ATTRIBUTE_COUNT_LIMIT)
_MAX_LOGRECORD_ATTRIBUTE_LENGTH = _limit_to_int(_LOGRECORD_ATTRIBUTE_VALUE_LENGTH_LIMIT)
_DEFAULT_REDACT_KEYS = [
    "authorization",
    "cookie",
    "password",
    "secret",
    "token",
    "api_key",
]
_REDACT_KEYS = {
    key.lower()
    for key in env_list(
        "CODEANATOMY_OTEL_REDACT_KEYS",
        default=_DEFAULT_REDACT_KEYS,
    )
}


def _should_redact(key: str) -> bool:
    lowered = key.lower()
    return any(part in lowered for part in _REDACT_KEYS)


def _is_scalar(value: object) -> bool:
    return isinstance(value, (str, bool, int, float))


def _normalize_sequence(
    values: Sequence[object], *, value_length_limit: int | None
) -> AttributeValue:
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
    return [_truncate_str(str(item), value_length_limit=value_length_limit) for item in normalized]


def _truncate_str(value: str, *, value_length_limit: int | None) -> str:
    if value_length_limit is None:
        return value
    if value_length_limit <= 0:
        return ""
    return value[:value_length_limit]


def _normalize_value(value: object, *, value_length_limit: int | None) -> AttributeValue:
    if _is_scalar(value):
        if isinstance(value, str):
            return _truncate_str(value, value_length_limit=value_length_limit)
        return cast("AttributeValue", value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return _truncate_str(bytes(value).hex(), value_length_limit=value_length_limit)
    if isinstance(value, Mapping):
        return _truncate_str(
            json.dumps(value, sort_keys=True, default=str),
            value_length_limit=value_length_limit,
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview)):
        return _normalize_sequence(list(value), value_length_limit=value_length_limit)
    return _truncate_str(str(value), value_length_limit=value_length_limit)


def _normalize_attributes(
    attrs: Mapping[str, object] | None,
    *,
    count_limit: int | None,
    value_length_limit: int | None,
) -> dict[str, AttributeValue]:
    """Normalize raw attribute values into OpenTelemetry-safe types.

    Parameters
    ----------
    attrs
        Raw attribute mapping.
    count_limit
        Maximum number of attributes to retain, or ``None`` for no limit.
    value_length_limit
        Maximum string length to retain, or ``None`` for no limit.

    Returns:
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
            normalized[key_str] = _truncate_str(
                "[redacted]",
                value_length_limit=value_length_limit,
            )
            continue
        normalized[key_str] = _normalize_value(value, value_length_limit=value_length_limit)
    if count_limit is None:
        return normalized
    if count_limit <= 0:
        return {}
    if len(normalized) <= count_limit:
        return normalized
    limited: dict[str, AttributeValue] = {}
    for key in sorted(normalized.keys())[:count_limit]:
        limited[key] = normalized[key]
    return limited


def normalize_attributes(attrs: Mapping[str, object] | None) -> dict[str, AttributeValue]:
    """Normalize attributes for spans/metrics.

    Returns:
    -------
    dict[str, AttributeValue]
        Normalized attribute mapping with OpenTelemetry-safe values.
    """
    return _normalize_attributes(
        attrs,
        count_limit=_MAX_ATTRIBUTES,
        value_length_limit=_MAX_ATTRIBUTE_LENGTH,
    )


def normalize_log_attributes(attrs: Mapping[str, object] | None) -> dict[str, AttributeValue]:
    """Normalize attributes for log records with log-record limits.

    Returns:
    -------
    dict[str, AttributeValue]
        Normalized attribute mapping with OpenTelemetry-safe values.
    """
    count_limit = _MAX_LOGRECORD_ATTRIBUTES
    value_length_limit = _MAX_LOGRECORD_ATTRIBUTE_LENGTH
    if count_limit is None:
        count_limit = _MAX_ATTRIBUTES
    if value_length_limit is None:
        value_length_limit = _MAX_ATTRIBUTE_LENGTH
    return _normalize_attributes(
        attrs,
        count_limit=count_limit,
        value_length_limit=value_length_limit,
    )


__all__ = ["normalize_attributes", "normalize_log_attributes"]
