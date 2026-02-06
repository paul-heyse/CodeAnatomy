"""MessagePack metadata encoding helpers."""

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence

import msgspec

from serde_msgspec import StructBaseCompat, dumps_msgpack, loads_msgpack

METADATA_PAYLOAD_VERSION: int = 1
_MSG_PACK_B64_PREFIX = "msgpack_b64:"


class MetadataMapEntry(StructBaseCompat, frozen=True):
    """Key/value entry for metadata map payloads."""

    key: str
    value: str


class MetadataMapPayload(StructBaseCompat, frozen=True):
    """Versioned metadata map payload."""

    version: int
    entries: tuple[MetadataMapEntry, ...]


class MetadataListPayload(StructBaseCompat, frozen=True):
    """Versioned metadata list payload."""

    version: int
    entries: tuple[str, ...]


class MetadataScalarEntry(StructBaseCompat, frozen=True):
    """Key/value entry for scalar metadata payloads."""

    key: str
    value: object | None


class MetadataScalarPayload(StructBaseCompat, frozen=True):
    """Versioned scalar metadata payload."""

    version: int
    entries: tuple[MetadataScalarEntry, ...]


def encode_metadata_map(entries: Mapping[str, str]) -> bytes:
    """Encode string mapping metadata as MessagePack bytes.

    Parameters
    ----------
    entries:
        Mapping of string keys to string values.

    Returns:
    -------
    bytes
        MessagePack payload for the mapping.
    """
    payload = MetadataMapPayload(
        version=METADATA_PAYLOAD_VERSION,
        entries=tuple(
            MetadataMapEntry(
                key=str(key),
                value=str(value) if value is not None else "",
            )
            for key, value in sorted(entries.items(), key=lambda item: str(item[0]))
        ),
    )
    return encode_metadata_payload(payload)


def decode_metadata_map(payload: bytes | None) -> dict[str, str]:
    """Decode MessagePack metadata bytes into a string mapping.

    Args:
        payload: Description.

    Returns:
        dict[str, str]: Result.

    Raises:
        TypeError: If the operation cannot be completed.
    """
    decoded = _decode_payload(payload, target_type=MetadataMapPayload, label="metadata_map")
    if decoded is None:
        return {}
    if decoded.version != METADATA_PAYLOAD_VERSION:
        msg = "metadata_map payload version is unsupported."
        raise TypeError(msg)
    return {entry.key: entry.value for entry in decoded.entries}


def encode_metadata_list(entries: Sequence[str]) -> bytes:
    """Encode string list metadata as MessagePack bytes.

    Parameters
    ----------
    entries:
        Sequence of string values.

    Returns:
    -------
    bytes
        MessagePack payload for the list.
    """
    payload = MetadataListPayload(
        version=METADATA_PAYLOAD_VERSION,
        entries=tuple(str(item) for item in entries),
    )
    return encode_metadata_payload(payload)


def decode_metadata_list(payload: bytes | None) -> list[str]:
    """Decode MessagePack metadata bytes into a list of strings.

    Args:
        payload: Description.

    Returns:
        list[str]: Result.

    Raises:
        TypeError: If the operation cannot be completed.
    """
    decoded = _decode_payload(payload, target_type=MetadataListPayload, label="metadata_list")
    if decoded is None:
        return []
    if decoded.version != METADATA_PAYLOAD_VERSION:
        msg = "metadata_list payload version is unsupported."
        raise TypeError(msg)
    return [str(item) for item in decoded.entries if item is not None]


def encode_metadata_scalar_map(entries: Mapping[str, object]) -> bytes:
    """Encode scalar mapping metadata as MessagePack bytes.

    Parameters
    ----------
    entries:
        Mapping of scalar values keyed by string.

    Returns:
    -------
    bytes
        MessagePack payload for the scalar mapping.
    """
    payload = MetadataScalarPayload(
        version=METADATA_PAYLOAD_VERSION,
        entries=tuple(
            MetadataScalarEntry(
                key=str(key),
                value=_normalize_scalar_value(value),
            )
            for key, value in sorted(entries.items(), key=lambda item: str(item[0]))
        ),
    )
    return encode_metadata_payload(payload)


def encode_metadata_payload(payload: object) -> bytes:
    """Encode a payload into UTF-8 safe MessagePack bytes.

    Parameters
    ----------
    payload
        Arbitrary metadata payload to encode.

    Returns:
    -------
    bytes
        UTF-8 safe MessagePack payload prefixed for decoding.
    """
    raw = dumps_msgpack(payload)
    encoded = base64.b64encode(raw).decode("utf-8")
    return f"{_MSG_PACK_B64_PREFIX}{encoded}".encode()


def decode_metadata_payload(payload: bytes) -> bytes:
    """Decode UTF-8 safe MessagePack bytes into raw MessagePack bytes.

    Parameters
    ----------
    payload
        UTF-8 safe metadata payload to decode.

    Returns:
    -------
    bytes
        Raw MessagePack payload bytes.
    """
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        return payload
    if not text.startswith(_MSG_PACK_B64_PREFIX):
        return payload
    encoded = text[len(_MSG_PACK_B64_PREFIX) :]
    try:
        return base64.b64decode(encoded)
    except (ValueError, TypeError):
        return payload


def decode_metadata_scalar_map(payload: bytes | None) -> dict[str, object]:
    """Decode MessagePack metadata bytes into a scalar mapping.

    Args:
        payload: Description.

    Returns:
        dict[str, object]: Result.

    Raises:
        TypeError: If the operation cannot be completed.
    """
    decoded = _decode_payload(
        payload,
        target_type=MetadataScalarPayload,
        label="metadata_scalar_map",
    )
    if decoded is None:
        return {}
    if decoded.version != METADATA_PAYLOAD_VERSION:
        msg = "metadata_scalar_map payload version is unsupported."
        raise TypeError(msg)
    return {entry.key: entry.value for entry in decoded.entries}


def _decode_payload[
    T,
](
    payload: bytes | None,
    *,
    target_type: type[T],
    label: str,
) -> T | None:
    if not payload:
        return None
    try:
        decoded = loads_msgpack(
            decode_metadata_payload(payload),
            target_type=target_type,
            strict=False,
        )
    except msgspec.ValidationError as exc:
        msg = f"{label} payload must be a {target_type.__name__}."
        raise TypeError(msg) from exc
    if not isinstance(decoded, target_type):
        msg = f"{label} payload must be a {target_type.__name__}."
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
    "decode_metadata_payload",
    "decode_metadata_scalar_map",
    "encode_metadata_list",
    "encode_metadata_map",
    "encode_metadata_payload",
    "encode_metadata_scalar_map",
]
