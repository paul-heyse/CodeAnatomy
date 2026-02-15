"""Consolidated contracts for tree-sitter runtime, diagnostics, and structural artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import msgspec

from tools.cq.core.structs import CqCacheStruct, CqStruct

PointV1 = tuple[int, int]


# Runtime execution contracts
class QueryWindowV1(CqStruct, frozen=True):
    """Inclusive-exclusive byte window for query execution."""

    start_byte: int
    end_byte: int


class QueryPointWindowV1(CqStruct, frozen=True):
    """Inclusive-exclusive point window for row/column anchored execution."""

    start_row: int
    start_col: int
    end_row: int
    end_col: int


class QueryExecutionSettingsV1(CqStruct, frozen=True):
    """Execution bounds for one query run."""

    match_limit: int = 4096
    max_start_depth: int | None = None
    budget_ms: int | None = None
    timeout_micros: int | None = None
    require_containment: bool = False
    window_mode: Literal[
        "intersection",
        "containment_preferred",
        "containment_required",
    ] = "intersection"


class QueryExecutionTelemetryV1(CqStruct, frozen=True):
    """Telemetry emitted by bounded query runners."""

    windows_total: int = 0
    windows_executed: int = 0
    capture_count: int = 0
    match_count: int = 0
    exceeded_match_limit: bool = False
    cancelled: bool = False
    window_split_count: int = 0
    degrade_reason: str | None = None


class AdaptiveRuntimeSnapshotV1(CqStruct, frozen=True):
    """Adaptive runtime snapshot for one language lane."""

    language: str
    average_latency_ms: float = 0.0
    sample_count: int = 0
    recommended_budget_ms: int = 0


class QueryWindowSourceV1(CqStruct, frozen=True):
    """Serializable byte window row."""

    start_byte: int
    end_byte: int


class QueryWindowSetV1(CqStruct, frozen=True):
    """Window set emitted by changed-range planning."""

    windows: tuple[QueryWindowSourceV1, ...] = ()


@dataclass(frozen=True, slots=True)
class TreeSitterInputEditV1:
    """One ``Tree.edit`` payload for incremental parsing."""

    start_byte: int
    old_end_byte: int
    new_end_byte: int
    start_point: PointV1
    old_end_point: PointV1
    new_end_point: PointV1


class ParseSessionStatsV1(CqStruct, frozen=True):
    """Parse-session counters for observability."""

    entries: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    parse_count: int = 0
    reparse_count: int = 0
    edit_failures: int = 0


class TreeSitterWorkItemV1(CqStruct, frozen=True):
    """Queued changed-range window work item."""

    language: str
    file_key: str
    start_byte: int
    end_byte: int


class InjectionRuntimeResultV1(CqStruct, frozen=True):
    """Result of parsing injected ranges for one parser language."""

    language: str
    plan_count: int = 0
    combined_count: int = 0
    parsed: bool = False
    included_ranges_applied: bool = False
    errors: tuple[str, ...] = ()
    metadata: dict[str, object] = msgspec.field(default_factory=dict)


# Diagnostics contracts
class TreeSitterDiagnosticV1(CqStruct, frozen=True):
    """A tree-sitter syntax diagnostic row."""

    kind: str
    start_byte: int
    end_byte: int
    start_line: int
    start_col: int
    end_line: int
    end_col: int
    message: str
    metadata: dict[str, object] = msgspec.field(default_factory=dict)


# Structural export contracts
class TreeSitterStructuralNodeV1(CqStruct, frozen=True):
    """Tree-sitter structural node record."""

    node_id: str
    kind: str
    start_byte: int
    end_byte: int
    start_line: int
    start_col: int
    end_line: int
    end_col: int
    field_name: str | None = None
    child_index: int | None = None


class TreeSitterStructuralEdgeV1(CqStruct, frozen=True):
    """Tree-sitter structural edge record."""

    edge_id: str
    source_node_id: str
    target_node_id: str
    kind: str = "parent_child"
    field_name: str | None = None


class TreeSitterCstTokenV1(CqStruct, frozen=True):
    """Leaf-level CST token record for structural ABI output."""

    token_id: str
    kind: str
    text: str
    start_byte: int
    end_byte: int
    start_line: int
    start_col: int
    end_line: int
    end_col: int


class TreeSitterQueryHitV1(CqStruct, frozen=True):
    """Typed query-capture hit row for CST artifact exports."""

    query_name: str
    pattern_index: int
    capture_name: str
    node_id: str
    start_byte: int
    end_byte: int


class TreeSitterStructuralExportV1(CqStruct, frozen=True):
    """Container for deterministic structural export payload."""

    nodes: list[TreeSitterStructuralNodeV1] = msgspec.field(default_factory=list)
    edges: list[TreeSitterStructuralEdgeV1] = msgspec.field(default_factory=list)
    tokens: list[TreeSitterCstTokenV1] = msgspec.field(default_factory=list)


class ObjectEvidenceRowV1(CqStruct, frozen=True):
    """One metadata-backed object evidence row derived from query matches."""

    emit: str = "unknown"
    kind: str = "unknown"
    anchor_start_byte: int = 0
    anchor_end_byte: int = 0
    pattern_index: int = 0
    captures: dict[str, str] = msgspec.field(default_factory=dict)


class TreeSitterEventV1(CqStruct, frozen=True):
    """One normalized tree-sitter event row."""

    file: str
    language: str
    kind: str
    start_byte: int
    end_byte: int
    start_line: int | None = None
    end_line: int | None = None
    captures: dict[str, str] = msgspec.field(default_factory=dict)
    event_id: str | None = None
    event_uuid_version: int | None = None
    event_created_ms: int | None = None


class TreeSitterEventBatchV1(CqStruct, frozen=True):
    """Batch of tree-sitter events for one file/language lane."""

    file: str
    language: str
    events: list[TreeSitterEventV1] = msgspec.field(default_factory=list)


class TreeSitterArtifactBundleV1(CqCacheStruct, frozen=True):
    """Cacheable artifact bundle for tree-sitter execution outputs."""

    run_id: str
    query: str
    language: str
    files: list[str] = msgspec.field(default_factory=list)
    batches: list[TreeSitterEventBatchV1] = msgspec.field(default_factory=list)
    structural_exports: list[TreeSitterStructuralExportV1] = msgspec.field(default_factory=list)
    cst_tokens: list[TreeSitterCstTokenV1] = msgspec.field(default_factory=list)
    cst_diagnostics: list[TreeSitterDiagnosticV1] = msgspec.field(default_factory=list)
    cst_query_hits: list[TreeSitterQueryHitV1] = msgspec.field(default_factory=list)
    telemetry: dict[str, object] = msgspec.field(default_factory=dict)
    run_uuid_version: int | None = None
    run_created_ms: float | None = None


__all__ = [
    "AdaptiveRuntimeSnapshotV1",
    "InjectionRuntimeResultV1",
    "ObjectEvidenceRowV1",
    "ParseSessionStatsV1",
    "PointV1",
    "QueryExecutionSettingsV1",
    "QueryExecutionTelemetryV1",
    "QueryPointWindowV1",
    "QueryWindowSetV1",
    "QueryWindowSourceV1",
    "QueryWindowV1",
    "TreeSitterArtifactBundleV1",
    "TreeSitterCstTokenV1",
    "TreeSitterDiagnosticV1",
    "TreeSitterEventBatchV1",
    "TreeSitterEventV1",
    "TreeSitterInputEditV1",
    "TreeSitterQueryHitV1",
    "TreeSitterStructuralEdgeV1",
    "TreeSitterStructuralExportV1",
    "TreeSitterStructuralNodeV1",
    "TreeSitterWorkItemV1",
]
