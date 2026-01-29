"""Parity tests for hashing helpers against legacy call-site semantics."""

from __future__ import annotations

import hashlib
import json

import msgspec

from serde_msgspec import JSON_ENCODER, MSGPACK_ENCODER, dumps_msgpack, to_builtins
from utils.hashing import (
    hash_json_default,
    hash_msgpack_canonical,
    hash_msgpack_default,
    hash_settings,
    hash_sha256_hex,
    hash_storage_options,
)


def test_legacy_msgpack_default_hash_parity() -> None:
    """Match legacy msgspec.msgpack.encode hashing for payloads."""
    payload = {"b": 2, "a": 1}
    legacy = hashlib.sha256(msgspec.msgpack.encode(payload)).hexdigest()
    assert hash_msgpack_default(payload) == legacy


def test_legacy_msgpack_canonical_truncated_parity() -> None:
    """Match legacy truncated msgpack hashes used in scan keys."""
    payload = {"scan": {"b": 2, "a": 1}}
    legacy = hashlib.sha256(dumps_msgpack(payload)).hexdigest()[:16]
    assert hash_msgpack_canonical(payload)[:16] == legacy


def test_legacy_msgpack_canonical_settings_parity() -> None:
    """Match legacy settings hashing for sorted tuples."""
    settings = {"b": "2", "a": "1"}
    payload = tuple(sorted(settings.items()))
    legacy = hashlib.sha256(MSGPACK_ENCODER.encode(payload)).hexdigest()
    assert hash_settings(settings) == legacy


def test_legacy_json_encoder_parity() -> None:
    """Match legacy msgspec JSON encoder hashing used for plan identity."""
    payload = {"b": 2, "a": 1}
    buffer = bytearray()
    JSON_ENCODER.encode_into(to_builtins(payload, str_keys=True), buffer)
    legacy = hashlib.sha256(buffer).hexdigest()
    assert hash_json_default(payload, str_keys=True) == legacy


def test_legacy_storage_options_hash_parity() -> None:
    """Match legacy storage options JSON hashing semantics."""
    storage = {"x": "1"}
    log_storage = {"y": "2"}
    payload = {"storage": storage, "log_storage": log_storage}
    legacy = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
    assert hash_storage_options(storage, log_storage) == legacy


def test_legacy_bytes_hash_parity() -> None:
    """Match legacy SHA-256 hashing for raw bytes payloads."""
    payload = b"substrait-payload"
    legacy = hashlib.sha256(payload).hexdigest()
    assert hash_sha256_hex(payload) == legacy
