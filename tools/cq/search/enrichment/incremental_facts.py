"""Typed incremental enrichment fact structs."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqStruct


class IncrementalAnchorFacts(CqStruct, frozen=True):
    """Anchor metadata for incremental enrichment."""

    file: str | None = None
    line: int | None = None
    col: int | None = None
    match_text: str | None = None


class IncrementalFacts(CqStruct, frozen=True):
    """Aggregated incremental enrichment facts across execution planes."""

    anchor: IncrementalAnchorFacts | None = None
    semantic: dict[str, object] = msgspec.field(default_factory=dict)
    sym: dict[str, object] = msgspec.field(default_factory=dict)
    dis: dict[str, object] = msgspec.field(default_factory=dict)
    inspect: dict[str, object] = msgspec.field(default_factory=dict)
    compound: dict[str, object] = msgspec.field(default_factory=dict)
    details: list[dict[str, object]] = msgspec.field(default_factory=list)
    stage_status: dict[str, object] = msgspec.field(default_factory=dict)
    stage_errors: dict[str, object] = msgspec.field(default_factory=dict)
    timings_ms: dict[str, object] = msgspec.field(default_factory=dict)
    session_errors: dict[str, object] = msgspec.field(default_factory=dict)


__all__ = ["IncrementalAnchorFacts", "IncrementalFacts"]
