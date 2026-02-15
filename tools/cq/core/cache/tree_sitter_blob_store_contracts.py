"""Contracts for file-backed tree-sitter blob payload storage."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class TreeSitterBlobRefV1(CqStruct, frozen=True):
    """Reference metadata for one persisted blob payload."""

    blob_id: str
    storage_key: str
    size_bytes: int
    path: str | None = None


__all__ = ["TreeSitterBlobRefV1"]
