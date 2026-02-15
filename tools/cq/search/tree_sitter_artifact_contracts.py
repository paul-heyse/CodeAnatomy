"""msgspec contracts for tree-sitter artifact payloads."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqCacheStruct
from tools.cq.search.tree_sitter_event_contracts import TreeSitterEventBatchV1
from tools.cq.search.tree_sitter_structural_contracts import (
    TreeSitterCstTokenV1,
    TreeSitterStructuralExportV1,
)


class TreeSitterArtifactBundleV1(CqCacheStruct, frozen=True):
    """Cacheable artifact bundle for tree-sitter execution outputs."""

    run_id: str
    query: str
    language: str
    files: list[str] = msgspec.field(default_factory=list)
    batches: list[TreeSitterEventBatchV1] = msgspec.field(default_factory=list)
    structural_exports: list[TreeSitterStructuralExportV1] = msgspec.field(default_factory=list)
    cst_tokens: list[TreeSitterCstTokenV1] = msgspec.field(default_factory=list)
    telemetry: dict[str, object] = msgspec.field(default_factory=dict)
    run_uuid_version: int | None = None
    run_created_ms: float | None = None


__all__ = [
    "TreeSitterArtifactBundleV1",
]
