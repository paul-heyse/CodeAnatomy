"""Contracts for deterministic tree-sitter cache store payloads."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqCacheStruct


class TreeSitterCacheEnvelopeV1(CqCacheStruct, frozen=True):
    """Canonical cache envelope for tree-sitter enrichment payloads."""

    language: str
    file_hash: str
    grammar_hash: str = ""
    query_pack_hash: str = ""
    scope_hash: str = ""
    payload: dict[str, object] = msgspec.field(default_factory=dict)


__all__ = ["TreeSitterCacheEnvelopeV1"]
