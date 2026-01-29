"""Shared hash utilities for DataFusion UDFs and ID helpers."""

from __future__ import annotations

import hashlib

from utils.hashing import (
    config_fingerprint,
    hash_file_sha256,
    hash_json_canonical,
    hash_json_default,
    hash_json_stdlib,
    hash_msgpack_canonical,
    hash_msgpack_default,
    hash_settings,
    hash_sha256_hex,
    hash_storage_options,
)


def hash64_from_text(value: str) -> int:
    """Return a deterministic signed 64-bit hash for a string.

    Parameters
    ----------
    value
        Input string to hash.

    Returns
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

    Returns
    -------
    str
        Deterministic 128-bit hex digest string.
    """
    return hashlib.blake2b(value.encode("utf-8"), digest_size=16).hexdigest()


__all__ = [
    "config_fingerprint",
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
