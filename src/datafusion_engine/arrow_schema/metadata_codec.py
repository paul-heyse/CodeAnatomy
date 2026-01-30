"""MessagePack metadata encoding helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import msgspec

from serde_msgspec import dumps_msgpack, loads_msgpack

METADATA_PAYLOAD_VERSION: int = 1


def encode_metadata_map(entries: Mapping[str, str]) -> bytes:
    """Encode string mapping metadata as MessagePack bytes.

    Parameters
    ----------
    entries:
        Mapping of string keys to string values.

    Returns
    -------
    bytes
        MessagePack payload for the mapping.
    """
    payload = {
        "version": METADATA_PAYLOAD_VERSION,
        "entries": [
            {"key": str(key), "value": str(value) if value is not None else ""}
            for key, value in sorted(entries.items(), key=lambda item: str(item[0]))
        ],
    }
    return dumps_msgpack(payload)


def decode_metadata_map(payload: bytes | None) -> dict[str, str]:
    """Decode MessagePack metadata bytes into a string mapping.

    Parameters
    ----------
    payload:
        MessagePack payload bytes.

    Returns
    -------
    dict[str, str]
        Decoded string mapping.

    Raises
    ------
    TypeError
        Raised when the payload shape is invalid.
    """
    decoded = _decode_mapping_payload(payload, label="metadata_map")
    if decoded is None:
        return {}
    entries = decoded.get("entries")
    if entries is None:
        return {}
    if not isinstance(entries, list):
        msg = "metadata_map entries must be a list."
        raise TypeError(msg)
    results: dict[str, str] = {}
    for item in entries:
        if not isinstance(item, Mapping):
            msg = "metadata_map entries must be mappings."
            raise TypeError(msg)
        key = item.get("key")
        value = item.get("value")
        if key is None:
            continue
        results[str(key)] = str(value) if value is not None else ""
    return results


def encode_metadata_list(entries: Sequence[str]) -> bytes:
    """Encode string list metadata as MessagePack bytes.

    Parameters
    ----------
    entries:
        Sequence of string values.

    Returns
    -------
    bytes
        MessagePack payload for the list.
    """
    payload = {
        "version": METADATA_PAYLOAD_VERSION,
        "entries": [str(item) for item in entries],
    }
    return dumps_msgpack(payload)


def decode_metadata_list(payload: bytes | None) -> list[str]:
    """Decode MessagePack metadata bytes into a list of strings.

    Parameters
    ----------
    payload:
        MessagePack payload bytes.

    Returns
    -------
    list[str]
        Decoded list of strings.

    Raises
    ------
    TypeError
        Raised when the payload shape is invalid.
    """
    decoded = _decode_mapping_payload(payload, label="metadata_list")
    if decoded is None:
        return []
    entries = decoded.get("entries")
    if entries is None:
        return []
    if not isinstance(entries, list):
        msg = "metadata_list entries must be a list."
        raise TypeError(msg)
    return [str(item) for item in entries if item is not None]


def encode_metadata_scalar_map(entries: Mapping[str, object]) -> bytes:
    """Encode scalar mapping metadata as MessagePack bytes.

    Parameters
    ----------
    entries:
        Mapping of scalar values keyed by string.

    Returns
    -------
    bytes
        MessagePack payload for the scalar mapping.
    """
    payload = {
        "version": METADATA_PAYLOAD_VERSION,
        "entries": [
            {"key": str(key), "value": _normalize_scalar_value(value)}
            for key, value in sorted(entries.items(), key=lambda item: str(item[0]))
        ],
    }
    return dumps_msgpack(payload)


def decode_metadata_scalar_map(payload: bytes | None) -> dict[str, object]:
    """Decode MessagePack metadata bytes into a scalar mapping.

    Parameters
    ----------
    payload:
        MessagePack payload bytes.

    Returns
    -------
    dict[str, object]
        Decoded scalar mapping.

    Raises
    ------
    TypeError
        Raised when the payload shape is invalid.
    """
    decoded = _decode_mapping_payload(payload, label="metadata_scalar_map")
    if decoded is None:
        return {}
    entries = decoded.get("entries")
    if entries is None:
        return {}
    if not isinstance(entries, list):
        msg = "metadata_scalar_map entries must be a list."
        raise TypeError(msg)
    results: dict[str, object] = {}
    for item in entries:
        if not isinstance(item, Mapping):
            msg = "metadata_scalar_map entries must be mappings."
            raise TypeError(msg)
        key = item.get("key")
        if key is None:
            continue
        results[str(key)] = item.get("value")
    return results


def _decode_mapping_payload(
    payload: bytes | None,
    *,
    label: str,
) -> Mapping[str, object] | None:
    if not payload:
        return None
    decoded = loads_msgpack(payload, target_type=object, strict=False)
    if not isinstance(decoded, Mapping):
        msg = f"{label} payload must contain a mapping."
        raise TypeError(msg)
    return decoded


def _normalize_scalar_value(value: object) -> object:
    if isinstance(value, msgspec.Struct):
        resolved = msgspec.to_builtins(value)
    else:
        as_py = getattr(value, "as_py", None)
        resolved = as_py() if callable(as_py) else value
    if resolved is None:
        return None
    if isinstance(resolved, (bool, int, float, str, bytes, bytearray)):
        return bytes(resolved) if isinstance(resolved, (bytes, bytearray)) else resolved
    return str(resolved)


__all__ = [
    "METADATA_PAYLOAD_VERSION",
    "decode_metadata_list",
    "decode_metadata_map",
    "decode_metadata_scalar_map",
    "encode_metadata_list",
    "encode_metadata_map",
    "encode_metadata_scalar_map",
]
