"""Shared typed cache payload decoding helpers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.cache.blob_store import decode_blob_pointer, read_blob
from tools.cq.core.cache.interface import CqCacheBackend
from tools.cq.core.cache.typed_codecs import convert_mapping_typed, decode_msgpack_typed


def decode_cached_payload[T](
    *,
    root: Path,
    backend: CqCacheBackend,
    payload: object,
    type_: type[T],
) -> tuple[T | None, bool]:
    """Decode cache payload from inline bytes/mappings or blob pointers.

    Returns:
    -------
    tuple[T | None, bool]
        Tuple ``(decoded, attempted)`` where attempted marks whether a decode
        path was actually tried.
    """
    if isinstance(payload, (bytes, bytearray, memoryview)):
        return decode_msgpack_typed(payload, type_=type_), True
    if isinstance(payload, dict):
        blob_ref = decode_blob_pointer(payload)
        if blob_ref is not None:
            blob_payload = read_blob(root=root, backend=backend, ref=blob_ref)
            if isinstance(blob_payload, (bytes, bytearray, memoryview)):
                return decode_msgpack_typed(blob_payload, type_=type_), True
            return None, True
        return convert_mapping_typed(payload, type_=type_), True
    return None, False


__all__ = ["decode_cached_payload"]
