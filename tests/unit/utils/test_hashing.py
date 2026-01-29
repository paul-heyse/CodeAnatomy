"""Tests for hashing helpers and compatibility with legacy semantics."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import msgspec

from serde_msgspec import JSON_ENCODER, JSON_ENCODER_SORTED, MSGPACK_ENCODER, to_builtins
from utils.hashing import (
    config_fingerprint,
    hash_file_sha256,
    hash_json_canonical,
    hash_json_default,
    hash_json_stdlib,
    hash_msgpack_canonical,
    hash_msgpack_default,
    hash_sha256_hex,
    hash_storage_options,
)


def test_hash_sha256_hex_length() -> None:
    """Verify SHA-256 hashing and truncation."""
    payload = b"abc"
    expected = hashlib.sha256(payload).hexdigest()
    assert hash_sha256_hex(payload) == expected
    assert hash_sha256_hex(payload, length=16) == expected[:16]


def test_hash_msgpack_default_matches_msgspec() -> None:
    """Verify msgpack default hashing matches msgspec encoding."""
    payload = {"b": 2, "a": 1}
    expected = hashlib.sha256(msgspec.msgpack.encode(payload)).hexdigest()
    assert hash_msgpack_default(payload) == expected


def test_hash_msgpack_canonical_matches_encoder() -> None:
    """Verify msgpack canonical hashing matches encoder output."""
    payload = {"b": 2, "a": 1}
    expected = hashlib.sha256(MSGPACK_ENCODER.encode(payload)).hexdigest()
    assert hash_msgpack_canonical(payload) == expected


def test_hash_json_default_matches_encoder() -> None:
    """Verify JSON default hashing matches encoder output."""
    payload = {"b": 2, "a": 1}
    buffer = bytearray()
    JSON_ENCODER.encode_into(to_builtins(payload, str_keys=True), buffer)
    expected = hashlib.sha256(buffer).hexdigest()
    assert hash_json_default(payload, str_keys=True) == expected


def test_hash_json_canonical_matches_sorted_encoder() -> None:
    """Verify JSON canonical hashing matches sorted encoder output."""
    payload = {"b": 2, "a": 1}
    buffer = bytearray()
    JSON_ENCODER_SORTED.encode_into(to_builtins(payload, str_keys=True), buffer)
    expected = hashlib.sha256(buffer).hexdigest()
    assert hash_json_canonical(payload, str_keys=True) == expected


def test_hash_json_stdlib_matches_json_dumps() -> None:
    """Verify stdlib JSON hashing matches json.dumps output."""
    payload = {"b": 2, "a": 1}
    expected = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
    assert hash_json_stdlib(payload, sort_keys=True) == expected


def test_hash_storage_options_matches_stdlib_json() -> None:
    """Verify storage options hashing matches stdlib JSON semantics."""
    storage = {"x": "1"}
    log_storage = {"y": "2"}
    payload = {"storage": storage, "log_storage": log_storage}
    expected = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
    assert hash_storage_options(storage, log_storage) == expected


def test_hash_file_sha256(tmp_path: Path) -> None:
    """Verify file hashing matches direct SHA-256 of contents."""
    path = tmp_path / "sample.txt"
    data = b"hash-me"
    path.write_bytes(data)
    expected = hashlib.sha256(data).hexdigest()
    assert hash_file_sha256(path) == expected


def test_config_fingerprint_uses_sorted_json() -> None:
    """Verify config fingerprint uses canonical JSON hashing."""
    payload = {"b": 2, "a": 1}
    expected = hash_json_canonical(payload, str_keys=True)
    assert config_fingerprint(payload) == expected
