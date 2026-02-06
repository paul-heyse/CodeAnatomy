"""Explicit hash utilities with stable serialization semantics."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import msgspec

from serde_msgspec import JSON_ENCODER, JSON_ENCODER_SORTED, MSGPACK_ENCODER, to_builtins

if TYPE_CHECKING:
    from pathlib import Path


def hash_sha256_hex(payload: bytes, *, length: int | None = None) -> str:
    """Return SHA-256 hex digest, optionally truncated.

    Parameters
    ----------
    payload
        Raw bytes to hash.
    length
        Optional length of hex digest to return.

    Returns:
    -------
    str
        Hex digest string (possibly truncated).
    """
    digest = hashlib.sha256(payload).hexdigest()
    return digest if length is None else digest[:length]


def hash64_from_text(value: str) -> int:
    """Return a deterministic signed 64-bit hash for a string.

    Parameters
    ----------
    value
        Input string to hash.

    Returns:
    -------
    int
        Deterministic signed 64-bit hash value.
    """
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
    unsigned = int.from_bytes(digest, "big", signed=False)
    return unsigned & ((1 << 63) - 1)


def hash128_from_text(value: str) -> str:
    """Return a deterministic 128-bit hash hex string for a string input.

    Parameters
    ----------
    value
        Input string to hash.

    Returns:
    -------
    str
        Deterministic 128-bit hex digest string.
    """
    return hashlib.blake2b(value.encode("utf-8"), digest_size=16).hexdigest()


# -----------------------------------------------------------------------------
# Payload hashing (msgpack)
# -----------------------------------------------------------------------------


def hash_msgpack_default(payload: object) -> str:
    """Return SHA-256 hexdigest using msgspec.msgpack.encode.

    Parameters
    ----------
    payload
        Payload to encode.

    Returns:
    -------
    str
        SHA-256 hexdigest.
    """
    return hash_sha256_hex(msgspec.msgpack.encode(payload))


def hash_msgpack_canonical(payload: object) -> str:
    """Return SHA-256 hexdigest using MSGPACK_ENCODER (deterministic order).

    Parameters
    ----------
    payload
        Payload to encode.

    Returns:
    -------
    str
        SHA-256 hexdigest.
    """
    return hash_sha256_hex(MSGPACK_ENCODER.encode(payload))


# -----------------------------------------------------------------------------
# Payload hashing (JSON via msgspec encoders)
# -----------------------------------------------------------------------------


def _json_hash(
    payload: object,
    *,
    str_keys: bool = False,
    sorted_keys: bool = False,
) -> str:
    buffer = bytearray()
    resolved = to_builtins(payload, str_keys=str_keys)
    if sorted_keys:
        JSON_ENCODER_SORTED.encode_into(resolved, buffer)
    else:
        JSON_ENCODER.encode_into(resolved, buffer)
    return hash_sha256_hex(bytes(buffer))


def hash_json_default(payload: object, *, str_keys: bool = False) -> str:
    """Return SHA-256 hexdigest using JSON_ENCODER (deterministic order).

    Parameters
    ----------
    payload
        Payload to encode.
    str_keys
        Whether to coerce mapping keys to strings.

    Returns:
    -------
    str
        SHA-256 hexdigest.
    """
    return _json_hash(payload, str_keys=str_keys, sorted_keys=False)


def hash_json_canonical(payload: object, *, str_keys: bool = False) -> str:
    """Return SHA-256 hexdigest using JSON_ENCODER_SORTED.

    Parameters
    ----------
    payload
        Payload to encode.
    str_keys
        Whether to coerce mapping keys to strings.

    Returns:
    -------
    str
        SHA-256 hexdigest.
    """
    return _json_hash(payload, str_keys=str_keys, sorted_keys=True)


def hash_json_stdlib(payload: object, *, sort_keys: bool = False) -> str:
    """Return SHA-256 hexdigest using stdlib json.dumps.

    Parameters
    ----------
    payload
        JSON-serializable payload.
    sort_keys
        Whether to sort keys in dictionaries.

    Returns:
    -------
    str
        SHA-256 hexdigest.
    """
    raw = json.dumps(payload, sort_keys=sort_keys).encode("utf-8")
    return hash_sha256_hex(raw)


# -----------------------------------------------------------------------------
# Settings/Config hashing
# -----------------------------------------------------------------------------


def hash_settings(settings: Mapping[str, str]) -> str:
    """Return SHA-256 hexdigest of sorted settings mapping.

    Parameters
    ----------
    settings
        Settings mapping.

    Returns:
    -------
    str
        SHA-256 hexdigest.
    """
    payload = tuple(sorted(settings.items()))
    return hash_msgpack_canonical(payload)


# -----------------------------------------------------------------------------
# Cache key builder
# -----------------------------------------------------------------------------


@dataclass
class CacheKeyBuilder:
    """Builder for deterministic cache keys."""

    prefix: str = ""
    _components: dict[str, object] = field(default_factory=dict)

    def add(self, name: str, value: object) -> CacheKeyBuilder:
        """Add a component to the cache key.

        Parameters
        ----------
        name
            Component name.
        value
            Component value.

        Returns:
        -------
        CacheKeyBuilder
            The updated builder instance.
        """
        self._components[name] = value
        return self

    def build(self) -> str:
        """Return the cache key string.

        Returns:
        -------
        str
            Cache key string.
        """
        digest = hash_msgpack_canonical(self._components)
        return f"{self.prefix}:{digest}" if self.prefix else digest


def hash_storage_options(
    storage_options: Mapping[str, str] | None,
    log_storage_options: Mapping[str, str] | None = None,
) -> str | None:
    """Return SHA-256 hexdigest of storage options (None if all inputs None).

    Parameters
    ----------
    storage_options
        Storage options mapping.
    log_storage_options
        Log storage options mapping.

    Returns:
    -------
    str | None
        SHA-256 hexdigest or None when no options are present.
    """
    if storage_options is None and log_storage_options is None:
        return None
    payload = {
        "storage": dict(storage_options or {}),
        "log_storage": dict(log_storage_options or {}),
    }
    return hash_json_stdlib(payload, sort_keys=True)


# -----------------------------------------------------------------------------
# File content hashing
# -----------------------------------------------------------------------------


def hash_file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Return SHA-256 hexdigest of file contents (chunked reading).

    Parameters
    ----------
    path
        File path to hash.
    chunk_size
        Read chunk size in bytes.

    Returns:
    -------
    str
        SHA-256 hexdigest.
    """
    h = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


__all__ = [
    "CacheKeyBuilder",
    "hash64_from_text",
    "hash128_from_text",
    "hash_file_sha256",
    "hash_json_canonical",
    "hash_json_default",
    "hash_json_stdlib",
    "hash_msgpack_canonical",
    "hash_msgpack_default",
    "hash_settings",
    "hash_sha256_hex",
    "hash_storage_options",
]
