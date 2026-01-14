"""Shared spec-table parsing and literal codec helpers."""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from typing import Literal, cast

from arrowdsl.compute.expr_core import ScalarValue
from arrowdsl.core.interop import ScalarLike
from arrowdsl.json_factory import JsonPolicy, dumps_text
from arrowdsl.plan.ops import DedupeStrategy

ASCII_POLICY = JsonPolicy(ascii_only=True)


def parse_sort_order(value: object) -> Literal["ascending", "descending"]:
    """Parse a sort order value.

    Returns
    -------
    Literal["ascending", "descending"]
        Normalized sort order.

    Raises
    ------
    ValueError
        Raised when the sort order value is unsupported.
    """
    if value is None:
        return "ascending"
    normalized = str(value).lower()
    if normalized == "ascending":
        return "ascending"
    if normalized == "descending":
        return "descending"
    msg = f"Unsupported sort order: {value!r}"
    raise ValueError(msg)


def parse_string_tuple(value: object, *, label: str) -> tuple[str, ...]:
    """Parse an optional list of strings into a tuple.

    Returns
    -------
    tuple[str, ...]
        Tuple of string values.

    Raises
    ------
    TypeError
        Raised when the input is not a list or tuple.
    """
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    msg = f"{label} must be a list of strings."
    raise TypeError(msg)


def parse_mapping_sequence(
    value: object,
    *,
    label: str,
) -> tuple[Mapping[str, object], ...]:
    """Parse a list of mappings into a tuple.

    Returns
    -------
    tuple[Mapping[str, object], ...]
        Tuple of mapping entries.

    Raises
    ------
    TypeError
        Raised when the input is not a list of mappings.
    """
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        items: list[Mapping[str, object]] = []
        for item in value:
            if isinstance(item, Mapping):
                items.append(item)
            else:
                msg = f"{label} entries must be mappings."
                raise TypeError(msg)
        return tuple(items)
    msg = f"{label} must be a list of mappings."
    raise TypeError(msg)


def parse_dedupe_strategy(value: object) -> DedupeStrategy:
    """Parse a dedupe strategy enum value.

    Returns
    -------
    DedupeStrategy
        Parsed dedupe strategy.

    Raises
    ------
    ValueError
        Raised when the strategy value is unsupported.
    """
    if value is None:
        return "KEEP_FIRST_AFTER_SORT"
    normalized = str(value)
    allowed: set[DedupeStrategy] = {
        "KEEP_FIRST_AFTER_SORT",
        "KEEP_BEST_BY_SCORE",
        "COLLAPSE_LIST",
        "KEEP_ARBITRARY",
    }
    if normalized in allowed:
        return cast("DedupeStrategy", normalized)
    msg = f"Unsupported dedupe strategy: {value!r}"
    raise ValueError(msg)


def encode_strict(*, strict: bool | Literal["filter"]) -> str:
    """Encode strict values for schema spec tables.

    Returns
    -------
    str
        Encoded strict value.
    """
    if strict is True:
        return "true"
    if strict is False:
        return "false"
    return "filter"


def decode_strict(value: str) -> bool | Literal["filter"]:
    """Decode strict values from schema spec tables.

    Returns
    -------
    bool | Literal["filter"]
        Decoded strict value.

    Raises
    ------
    ValueError
        Raised when the strict value is unsupported.
    """
    normalized = value.lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    if normalized == "filter":
        return "filter"
    msg = f"Unsupported strict value: {value!r}"
    raise ValueError(msg)


def parse_scalar_value(value: object) -> ScalarValue | None:
    """Parse a scalar literal value.

    Returns
    -------
    ScalarValue | None
        Parsed scalar literal.

    Raises
    ------
    TypeError
        Raised when the value is not a supported scalar type.
    """
    if value is None:
        return None
    if isinstance(value, (bool, int, float, str, bytes)):
        return value
    if isinstance(value, ScalarLike):
        return value
    msg = "Scalar literal must be a supported scalar type."
    raise TypeError(msg)


def encode_json_payload(value: object | None) -> object | None:
    """Encode a JSON payload with bytes handling.

    Returns
    -------
    object | None
        Encoded payload with a discriminator.
    """
    if value is None:
        return None
    if isinstance(value, ScalarLike):
        value = value.as_py()
    if isinstance(value, bytes):
        return {"type": "bytes", "value": base64.b64encode(value).decode("ascii")}
    return {"type": "json", "value": value}


def decode_json_payload(payload: object | None) -> object | None:
    """Decode a JSON payload with bytes handling.

    Returns
    -------
    object | None
        Decoded payload value.

    Raises
    ------
    ValueError
        Raised when the bytes payload is not base64 encoded.
    """
    if payload is None:
        return None
    if isinstance(payload, dict) and payload.get("type") == "bytes":
        value = payload.get("value", "")
        if not isinstance(value, str):
            msg = "Encoded bytes payload must contain a base64 string."
            raise ValueError(msg)
        return base64.b64decode(value.encode("ascii"))
    if isinstance(payload, dict) and "value" in payload:
        return payload.get("value")
    return payload


def encode_json_text(value: object | None) -> str | None:
    """Encode a JSON payload as text.

    Returns
    -------
    str | None
        JSON text payload.
    """
    payload = encode_json_payload(value)
    return None if payload is None else dumps_text(payload, policy=ASCII_POLICY)


def decode_json_text(payload: str | None) -> object | None:
    """Decode a JSON payload from text.

    Returns
    -------
    object | None
        Decoded payload value.
    """
    if payload is None:
        return None
    return decode_json_payload(json.loads(payload))


def encode_options_payload(value: bytes | bytearray | None) -> object | None:
    """Encode serialized FunctionOptions payloads.

    Returns
    -------
    object | None
        Encoded payload with a discriminator.
    """
    if value is None:
        return None
    return encode_json_payload(bytes(value))


def decode_options_payload(payload: object | None) -> bytes | None:
    """Decode serialized FunctionOptions payloads.

    Returns
    -------
    bytes | None
        Decoded options payload.

    Raises
    ------
    TypeError
        Raised when the payload does not decode to bytes.
    """
    decoded = decode_json_payload(payload)
    if decoded is None:
        return None
    if isinstance(decoded, bytearray):
        return bytes(decoded)
    if isinstance(decoded, bytes):
        return decoded
    msg = "Options payload must decode to bytes."
    raise TypeError(msg)


def encode_scalar_payload(value: ScalarValue | None) -> object | None:
    """Encode a scalar payload with bytes handling.

    Returns
    -------
    object | None
        Encoded scalar payload.
    """
    if value is None:
        return None
    py_value = value.as_py() if isinstance(value, ScalarLike) else value
    parsed = parse_scalar_value(py_value)
    return encode_json_payload(parsed)


def decode_scalar_payload(payload: object | None) -> ScalarValue | None:
    """Decode a scalar payload with bytes handling.

    Returns
    -------
    ScalarValue | None
        Decoded scalar payload.
    """
    decoded = decode_json_payload(payload)
    return parse_scalar_value(decoded)


def encode_scalar_union(value: ScalarValue | None) -> ScalarValue | None:
    """Encode a scalar payload for union-typed storage.

    Returns
    -------
    ScalarValue | None
        Validated scalar payload.
    """
    return parse_scalar_value(value)


def decode_scalar_union(payload: object | None) -> ScalarValue | None:
    """Decode a scalar payload from union-typed storage.

    Returns
    -------
    ScalarValue | None
        Decoded scalar payload.
    """
    return parse_scalar_value(payload)


def encode_scalar_json(value: ScalarValue | None) -> str | None:
    """Encode a scalar payload as JSON text.

    Returns
    -------
    str | None
        JSON text payload.
    """
    payload = encode_scalar_payload(value)
    return None if payload is None else dumps_text(payload, policy=ASCII_POLICY)


def decode_scalar_json(payload: str | None) -> ScalarValue | None:
    """Decode a scalar payload from JSON text.

    Returns
    -------
    ScalarValue | None
        Decoded scalar payload.
    """
    if payload is None:
        return None
    return decode_scalar_payload(json.loads(payload))


__all__ = [
    "decode_json_payload",
    "decode_json_text",
    "decode_options_payload",
    "decode_scalar_json",
    "decode_scalar_payload",
    "decode_scalar_union",
    "decode_strict",
    "encode_json_payload",
    "encode_json_text",
    "encode_options_payload",
    "encode_scalar_json",
    "encode_scalar_payload",
    "encode_scalar_union",
    "encode_strict",
    "parse_dedupe_strategy",
    "parse_mapping_sequence",
    "parse_scalar_value",
    "parse_sort_order",
    "parse_string_tuple",
]
