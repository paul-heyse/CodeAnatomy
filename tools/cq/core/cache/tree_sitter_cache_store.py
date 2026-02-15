"""Deterministic msgpack storage for tree-sitter enrichment payloads."""

from __future__ import annotations

from pathlib import Path
from typing import Final

import msgspec

from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend
from tools.cq.core.cache.key_builder import build_cache_key
from tools.cq.core.cache.namespaces import resolve_namespace_ttl_seconds
from tools.cq.core.cache.policy import default_cache_policy
from tools.cq.core.cache.telemetry import (
    record_cache_decode_failure,
    record_cache_get,
    record_cache_set,
)
from tools.cq.core.cache.tree_sitter_blob_store import (
    decode_blob_pointer,
    encode_blob_pointer,
    read_blob,
    write_blob,
)
from tools.cq.core.cache.tree_sitter_cache_store_contracts import TreeSitterCacheEnvelopeV1
from tools.cq.core.cache.typed_codecs import convert_mapping_typed, decode_msgpack_typed

_NAMESPACE: Final[str] = "tree_sitter"
_VERSION: Final[str] = "v3"
_BLOB_THRESHOLD_BYTES: Final[int] = 64 * 1024

_ENCODER = msgspec.msgpack.Encoder()


def build_tree_sitter_cache_key(
    *,
    workspace: str,
    language: str,
    target: str,
    fingerprints: dict[str, str],
) -> str:
    """Build deterministic cache key for one tree-sitter payload.

    Returns:
        str: The canonical cache key.
    """
    return build_cache_key(
        _NAMESPACE,
        version=_VERSION,
        workspace=workspace,
        language=language,
        target=target,
        extras={
            "file_hash": fingerprints.get("file_hash"),
            "grammar_hash": fingerprints.get("grammar_hash"),
            "query_pack_hash": fingerprints.get("query_pack_hash"),
            "scope_hash": fingerprints.get("scope_hash"),
        },
    )


def persist_tree_sitter_payload(
    *,
    root: Path,
    cache_key: str,
    envelope: TreeSitterCacheEnvelopeV1,
    tag: str | None = None,
) -> bool:
    """Persist tree-sitter payload envelope as msgpack bytes.

    Returns:
        bool: ``True`` if the write was accepted by the backend.
    """
    backend = get_cq_cache_backend(root=root)
    policy = default_cache_policy(root=root)
    ttl_seconds = resolve_namespace_ttl_seconds(policy=policy, namespace=_NAMESPACE)
    encoded = _ENCODER.encode(envelope)
    payload: object
    if len(encoded) > _BLOB_THRESHOLD_BYTES:
        blob_ref = write_blob(root=root, payload=encoded)
        payload = encode_blob_pointer(blob_ref)
    else:
        payload = encoded
    ok = backend.set(
        cache_key,
        payload,
        expire=ttl_seconds,
        tag=tag,
    )
    record_cache_set(namespace=_NAMESPACE, ok=ok, key=cache_key)
    return ok


def load_tree_sitter_payload(
    *,
    root: Path,
    cache_key: str,
) -> TreeSitterCacheEnvelopeV1 | None:
    """Load tree-sitter payload envelope from cache.

    Returns:
        TreeSitterCacheEnvelopeV1 | None: Decoded payload envelope, or ``None``
            when missing or invalid.
    """
    backend = get_cq_cache_backend(root=root)
    cached = backend.get(cache_key)
    hit = isinstance(cached, (bytes, bytearray, memoryview, dict))
    record_cache_get(namespace=_NAMESPACE, hit=hit, key=cache_key)
    envelope, attempted_decode = _decode_tree_sitter_payload(root=root, payload=cached)
    if envelope is None and attempted_decode:
        record_cache_decode_failure(namespace=_NAMESPACE)
    return envelope


def _decode_tree_sitter_payload(
    *,
    root: Path,
    payload: object,
) -> tuple[TreeSitterCacheEnvelopeV1 | None, bool]:
    envelope: TreeSitterCacheEnvelopeV1 | None = None
    attempted = False
    if isinstance(payload, (bytes, bytearray, memoryview)):
        attempted = True
        envelope = decode_msgpack_typed(payload, type_=TreeSitterCacheEnvelopeV1)
    elif isinstance(payload, dict):
        attempted = True
        blob_ref = decode_blob_pointer(payload)
        if blob_ref is not None:
            blob_payload = read_blob(root=root, ref=blob_ref)
            if isinstance(blob_payload, (bytes, bytearray, memoryview)):
                envelope = decode_msgpack_typed(blob_payload, type_=TreeSitterCacheEnvelopeV1)
            else:
                envelope = None
        else:
            envelope = convert_mapping_typed(payload, type_=TreeSitterCacheEnvelopeV1)
    return envelope, attempted


__all__ = [
    "build_tree_sitter_cache_key",
    "load_tree_sitter_payload",
    "persist_tree_sitter_payload",
]
