"""Tests for shared cache payload decoding helpers."""

from __future__ import annotations

from pathlib import Path

import msgspec
from tools.cq.core.cache.blob_store import encode_blob_pointer, write_blob
from tools.cq.core.cache.cache_decode import decode_cached_payload
from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend


class _Payload(msgspec.Struct, frozen=True):
    value: int
    label: str


def test_decode_cached_payload_msgpack(tmp_path: Path) -> None:
    """Decodes inline msgpack payloads into the requested typed contract."""
    payload = _Payload(value=7, label="alpha")
    raw = msgspec.msgpack.encode(payload)
    backend = get_cq_cache_backend(root=tmp_path)
    decoded, attempted = decode_cached_payload(
        root=tmp_path,
        backend=backend,
        payload=raw,
        type_=_Payload,
    )
    assert attempted
    assert decoded == payload


def test_decode_cached_payload_mapping(tmp_path: Path) -> None:
    """Decodes mapping payloads through typed mapping conversion."""
    backend = get_cq_cache_backend(root=tmp_path)
    decoded, attempted = decode_cached_payload(
        root=tmp_path,
        backend=backend,
        payload={"value": 5, "label": "beta"},
        type_=_Payload,
    )
    assert attempted
    assert decoded == _Payload(value=5, label="beta")


def test_decode_cached_payload_blob_pointer(tmp_path: Path) -> None:
    """Decodes blob-pointer payloads by loading msgpack bytes from blob storage."""
    payload = _Payload(value=11, label="gamma")
    raw = msgspec.msgpack.encode(payload)
    backend = get_cq_cache_backend(root=tmp_path)
    ref = write_blob(root=tmp_path, backend=backend, payload=raw)
    pointer = encode_blob_pointer(ref)
    decoded, attempted = decode_cached_payload(
        root=tmp_path,
        backend=backend,
        payload=pointer,
        type_=_Payload,
    )
    assert attempted
    assert decoded == payload


def test_decode_cached_payload_non_decodable_value(tmp_path: Path) -> None:
    """Marks decode as not-attempted for values without supported payload shapes."""
    backend = get_cq_cache_backend(root=tmp_path)
    decoded, attempted = decode_cached_payload(
        root=tmp_path,
        backend=backend,
        payload=123,
        type_=_Payload,
    )
    assert not attempted
    assert decoded is None
