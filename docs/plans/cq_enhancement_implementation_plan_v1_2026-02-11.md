# CQ Enhancement Implementation Plan v2

**Date:** 2026-02-11
**Status:** Draft
**Purpose:** Semantic Neighborhood Bundle (SNB) enhancement with integrated corrections from v1 review

This plan integrates all blocking corrections, optimizations, and scope expansions identified in the v1 review document. It establishes progressive disclosure output format (LDMD) plus Rust LSP and Pyrefly LSP enrichment capabilities with typed contracts, capability gating, and graceful degradation.

---

## Scope Overview

| Scope | Title | Depends On | Est. Files |
|-------|-------|------------|------------|
| R0 | Correctness Alignment Patch | — | 0 new, 5 edit |
| R1 | Contract Foundation (SNB Schema + Typed Registry + Degrade Model) | R0 | 4 new, 2 edit |
| R2 | Rust LSP Session Foundation + Environment Envelope | — | 4 new, 2 edit |
| R3 | Pyrefly Expansion Alignment + Capability Gates | R1 | 2 new, 2 edit |
| R4 | Structural Collector + Scan Snapshot Adapter | R1 | 4 new, 2 edit |
| R5 | SNB Assembler + Deterministic Layout | R1, R2, R3, R4 | 3 new, 3 edit |
| R6 | LDMD Services (Strict Parser + Protocol Commands) | — | 5 new, 1 edit |
| R7 | CLI + Run Integration (CliResult Pattern) | R5, R6 | 3 new, 4 edit |
| R8 | Advanced Evidence Planes (On-Demand) | R2, R3 | 4 new, 2 edit |

**Implementation Order:** R0 → R1 → (R2 || R3) → R4 → R5 → R6 → R7 → R8

**Quality Gates:**
- All new modules require msgspec contract tests (`tests/msgspec_contract/`)
- All LSP integration requires mock transcript tests
- All CLI changes require golden snapshot updates
- Full quality gate after each scope: `uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q`

---

## R0: Correctness Alignment Patch

**Goal:** Fix all blocking integration issues before introducing new features.

**Rationale:** These are critical correctness fixes that must land first to prevent cascading issues in downstream scopes.

### Blocking Fix 1: Add `ldmd` to OutputFormat enum

**Problem:** LDMD output format is referenced throughout implementation but missing from the canonical enum.

**Fix:** Add LDMD variant to the `OutputFormat` enum in `tools/cq/cli_app/types.py`.

```python
# tools/cq/cli_app/types.py
from __future__ import annotations

from enum import StrEnum


class OutputFormat(StrEnum):
    """Output format options for CQ commands."""

    md = "md"
    json = "json"
    both = "both"
    summary = "summary"
    mermaid = "mermaid"
    mermaid_class = "mermaid-class"
    dot = "dot"
    ldmd = "ldmd"  # NEW: LLM-friendly progressive disclosure markdown
```

### Blocking Fix 2: Add `ldmd` renderer dispatch

**Problem:** No dispatch logic to route `OutputFormat.ldmd` to the actual LDMD renderer.

**Fix:** Wire LDMD format into result rendering dispatch in `tools/cq/cli_app/result.py`.

```python
# tools/cq/cli_app/result.py (additions to render_result function)
from __future__ import annotations

from tools.cq.cli_app.types import OutputFormat
from tools.cq.core.schema import CqResult


def render_result(
    result: CqResult,
    output_format: OutputFormat,
) -> str:
    """Render CqResult to requested format."""

    if output_format == OutputFormat.json:
        return _render_json(result)
    elif output_format == OutputFormat.summary:
        return _render_summary(result)
    elif output_format == OutputFormat.mermaid:
        return _render_mermaid(result)
    elif output_format == OutputFormat.mermaid_class:
        return _render_mermaid_class(result)
    elif output_format == OutputFormat.dot:
        return _render_dot(result)
    elif output_format == OutputFormat.ldmd:
        # NEW: LDMD renderer dispatch
        from tools.cq.ldmd.writer import render_ldmd_from_cq_result
        return render_ldmd_from_cq_result(result)
    else:
        # Default: markdown
        return _render_markdown(result)
```

### Blocking Fix 3: kind-registry typing contract

**Problem:** Current registry uses `dict[str, str]` which loses type information. Downstream code cannot validate payloads at runtime.

**Contract:** Registry must use `dict[str, type[msgspec.Struct]]` to enable runtime validation.

**Note:** Full implementation lands in R1, but R0 establishes the requirement pattern.

```python
# Pattern requirement for R1:
# tools/cq/core/snb_registry.py

import msgspec

# CORRECT: Registry stores actual struct types
DETAILS_KIND_REGISTRY: dict[str, type[msgspec.Struct]] = {
    "cq.snb.bundle_ref.v1": SemanticNeighborhoodBundleRefV1,
    "cq.snb.node.v1": SemanticNodeRefV1,
    # ... other kinds
}

def resolve_kind(kind: str) -> type[msgspec.Struct] | None:
    """Resolve kind string to struct type for runtime validation."""
    return DETAILS_KIND_REGISTRY.get(kind)
```

### Target Files for R0

| Action | File | Change |
|--------|------|--------|
| **Edit** | `tools/cq/cli_app/types.py` | Add `ldmd` to `OutputFormat` enum |
| **Edit** | `tools/cq/cli_app/result.py` | Add LDMD renderer dispatch |

### Implementation Checklist for R0

- [ ] Add `ldmd = "ldmd"` to `OutputFormat` enum
- [ ] Add LDMD renderer dispatch in `render_result()` function
- [ ] Fix CqResult import: use `from tools.cq.core.schema import CqResult` (NOT tools.cq.core.result)
- [ ] Verify no command-local `format: str` parameter shadows global `OutputFormat`
- [ ] Test: `OutputFormat.ldmd` round-trips through config chain
- [ ] Test: renderer dispatch routes to LDMD handler without error (stub ok for R0)
- [ ] Document kind-registry typing requirement for R1 implementation

**Acceptance Criteria:**
- `OutputFormat.ldmd` is a valid enum member
- Result rendering handles `ldmd` format without crashing
- CqResult import resolves to actual module path (tools.cq.core.schema)
- No `mypy` or `pyright` errors on edited files
- All existing tests pass

---

## R1: Contract Foundation (SNB Schema + Typed Registry + Degrade Model)

**Goal:** Establish msgspec-based contracts for Semantic Neighborhood Bundle (SNB) with typed registry and structured degradation events.

**Rationale:** All downstream enrichment, assembly, and rendering depends on stable, versioned contracts. Typed registry enables runtime validation. Structured degrade events enable programmatic quality analysis.

### SNB Schema Structures

All SNB structures use `CqStruct` base (frozen msgspec.Struct with `from_mapping` factory).

```python
# tools/cq/core/snb_schema.py
from __future__ import annotations

from typing import Literal

from tools.cq.core.structs import CqStruct


class DegradeEventV1(CqStruct, frozen=True):
    """Typed degradation event for structured failure tracking.

    Captures partial enrichment failures with enough context for
    programmatic filtering and diagnostic analysis.
    """

    stage: str  # e.g. "lsp.rust", "structural.interval_index"
    severity: Literal["info", "warning", "error"] = "warning"
    category: str = ""  # e.g. "timeout", "unavailable", "parse_error"
    message: str = ""
    correlation_key: str | None = None


class SemanticNodeRefV1(CqStruct, frozen=True):
    """Reference to a semantic node in the neighborhood.

    Minimal projection for neighborhood slice membership.
    Full node details live in node_index.
    """

    node_id: str
    kind: str  # e.g. "function", "class", "import"
    name: str
    display_label: str = ""
    file_path: str = ""
    byte_span: tuple[int, int] | None = None
    signature: str | None = None
    qualname: str | None = None


class SemanticEdgeV1(CqStruct, frozen=True):
    """Directed semantic edge in the neighborhood graph."""

    edge_id: str
    source_node_id: str
    target_node_id: str
    edge_kind: str  # e.g. "calls", "imports", "extends"
    weight: float = 1.0
    evidence_source: str = ""  # e.g. "ast", "lsp.rust", "bytecode"
    metadata: dict[str, object] | None = None


NeighborhoodSliceKind = Literal[
    "callers",
    "callees",
    "references",
    "implementations",
    "type_supertypes",
    "type_subtypes",
    "parents",
    "children",
    "siblings",
    "enclosing_context",
    "imports",
    "importers",
    "related",
]


class NeighborhoodSliceV1(CqStruct, frozen=True):
    """A single slice of the semantic neighborhood.

    Each slice represents a specific relationship type (callers, imports, etc.)
    with progressive disclosure support (preview + total count).
    """

    kind: NeighborhoodSliceKind
    title: str
    total: int = 0
    preview: tuple[SemanticNodeRefV1, ...] = ()
    edges: tuple[SemanticEdgeV1, ...] = ()
    collapsed: bool = True
    metadata: dict[str, object] | None = None


class SemanticNeighborhoodBundleRefV1(CqStruct, frozen=True):
    """Reference pointer to a neighborhood bundle artifact.

    Enables lazy loading and artifact caching.
    """

    bundle_id: str
    deterministic_id: str  # SHA256 of normalized content
    byte_size: int = 0
    artifact_path: str | None = None
    preview_slices: tuple[NeighborhoodSliceKind, ...] = ()


class BundleMetaV1(CqStruct, frozen=True):
    """Bundle creation metadata and provenance."""

    tool: str = "cq"
    tool_version: str | None = None
    workspace_root: str | None = None
    query_text: str | None = None
    created_at_ms: float | None = None
    lsp_servers: tuple[dict[str, object], ...] = ()
    limits: dict[str, int] | None = None


class NeighborhoodGraphSummaryV1(CqStruct, frozen=True):
    """Lightweight graph summary statistics."""

    node_count: int = 0
    edge_count: int = 0
    full_graph_artifact: ArtifactPointerV1 | None = None


class SemanticNeighborhoodBundleV1(CqStruct, frozen=True):
    """Complete semantic neighborhood bundle — CANONICAL definition.

    All downstream sections (R5, R6, R7) MUST reference only these fields.
    This is the authoritative schema that all SNB operations compile against.

    Combines structural, LSP, and optional enrichment planes into
    a single versioned, deterministic artifact.
    """

    bundle_id: str
    subject: SemanticNodeRefV1 | None = None
    subject_label: str = ""
    meta: BundleMetaV1 | None = None
    slices: tuple[NeighborhoodSliceV1, ...] = ()
    graph: NeighborhoodGraphSummaryV1 | None = None
    node_index: dict[str, SemanticNodeRefV1] | None = None
    artifacts: tuple[ArtifactPointerV1, ...] = ()
    diagnostics: tuple[DegradeEventV1, ...] = ()
    schema_version: str = "cq.snb.v1"
```

### Schema Compatibility Rule

**CRITICAL**: All R5, R6, R7 snippets MUST compile against this R1 schema. No shadow structs, no fields beyond what is defined here. If a downstream section needs a new field, it must be added to R1 first.

**Field access rules for downstream sections:**
- Use `bundle.subject` (SemanticNodeRefV1 | None), NOT `bundle.subject_node_id`
- Use `bundle.meta` (BundleMetaV1 | None), NOT `bundle.metadata`
- Use `bundle.graph` (NeighborhoodGraphSummaryV1 | None)
- Use `bundle.artifacts` (tuple of ArtifactPointerV1)
- Use `slice.preview` (tuple of SemanticNodeRefV1), NOT `slice.nodes`

**Validation checklist:**
- Every R5, R6, R7 snippet that touches SNB types imports from `tools/cq/core/snb_schema`
- Every snippet uses only fields defined in the canonical R1 schema
- No inline struct definitions that shadow or extend R1 types

### Typed Registry with Runtime Validation

```python
# tools/cq/core/snb_registry.py
from __future__ import annotations

import msgspec

from tools.cq.core.snb_schema import (
    BundleMetaV1,
    DegradeEventV1,
    NeighborhoodGraphSummaryV1,
    NeighborhoodSliceV1,
    SemanticEdgeV1,
    SemanticNeighborhoodBundleRefV1,
    SemanticNeighborhoodBundleV1,
    SemanticNodeRefV1,
)


# CORRECT: Registry stores actual struct types, not string names
DETAILS_KIND_REGISTRY: dict[str, type[msgspec.Struct]] = {
    "cq.snb.bundle_ref.v1": SemanticNeighborhoodBundleRefV1,
    "cq.snb.bundle.v1": SemanticNeighborhoodBundleV1,
    "cq.snb.node.v1": SemanticNodeRefV1,
    "cq.snb.edge.v1": SemanticEdgeV1,
    "cq.snb.slice.v1": NeighborhoodSliceV1,
    "cq.snb.degrade.v1": DegradeEventV1,
    "cq.snb.bundle_meta.v1": BundleMetaV1,
    "cq.snb.graph_summary.v1": NeighborhoodGraphSummaryV1,
}


def resolve_kind(kind: str) -> type[msgspec.Struct] | None:
    """Resolve kind string to struct type for runtime validation.

    Returns None if kind is not registered.
    """
    return DETAILS_KIND_REGISTRY.get(kind)


def validate_finding_details(details: dict[str, object]) -> bool:
    """Validate that finding details dict has valid kind and structure.

    Used by enrichment pipeline to catch malformed payloads early.
    """
    kind = details.get("kind")
    if not isinstance(kind, str):
        return False
    struct_type = resolve_kind(kind)
    return struct_type is not None


def decode_finding_details(
    details: dict[str, object],
) -> msgspec.Struct | None:
    """Decode finding details dict into typed struct.

    Returns None if kind is unknown or decoding fails.
    """
    kind = details.get("kind")
    if not isinstance(kind, str):
        return None

    struct_type = resolve_kind(kind)
    if struct_type is None:
        return None

    try:
        return msgspec.convert(details, type=struct_type)
    except (msgspec.ValidationError, TypeError):
        return None
```

### Artifact Pointer Schemas

```python
# tools/cq/core/snb_schema.py (additions)

class ArtifactPointerV1(CqStruct, frozen=True):
    """Generic artifact pointer with deterministic identity.

    Used for lazy loading and cache key computation.
    """

    artifact_kind: str  # e.g. "snb.bundle", "lsp.call_graph"
    artifact_id: str
    deterministic_id: str  # SHA256 of normalized content
    byte_size: int = 0
    storage_path: str | None = None
    metadata: dict[str, object] | None = None


class SemanticNeighborhoodBundleRefV1(CqStruct, frozen=True):
    """Reference pointer to a neighborhood bundle artifact.

    Extends ArtifactPointerV1 pattern with bundle-specific fields.
    """

    bundle_id: str
    deterministic_id: str  # SHA256 of normalized SemanticNeighborhoodBundleV1
    byte_size: int = 0
    artifact_path: str | None = None
    preview_slices: tuple[NeighborhoodSliceKind, ...] = ()
    subject_node_id: str = ""
    subject_label: str = ""
```

### Integration with Existing Code Facts

```python
# tools/cq/core/enrichment_facts.py (additions)
from __future__ import annotations

from tools.cq.core.snb_schema import (
    DegradeEventV1,
    SemanticNeighborhoodBundleV1,
)


class EnrichedFinding(CqStruct, frozen=True):
    """Enriched finding with optional SNB attachment."""

    # ... existing fields ...

    # NEW: Optional semantic neighborhood bundle
    neighborhood_bundle: SemanticNeighborhoodBundleV1 | None = None

    # NEW: Typed degradation events
    degrade_events: tuple[DegradeEventV1, ...] = ()
```

### Target Files for R1

| Action | File | Purpose |
|--------|------|---------|
| **Create** | `tools/cq/core/snb_schema.py` | All SNB msgspec structs |
| **Create** | `tools/cq/core/snb_registry.py` | Typed registry + validation |
| **Create** | `tests/unit/cq/core/test_snb_schema.py` | Schema unit tests |
| **Create** | `tests/msgspec_contract/test_snb_contracts.py` | Roundtrip tests |
| **Edit** | `tools/cq/core/__init__.py` | Export new public types |
| **Edit** | `tools/cq/core/enrichment_facts.py` | Add SNB fields to EnrichedFinding |

### Implementation Checklist for R1

**Schema Implementation:**
- [ ] `DegradeEventV1` struct with stage/severity/category/message/correlation_key
- [ ] `SemanticNodeRefV1` with minimal node projection
- [ ] `SemanticEdgeV1` with edge identity and evidence source
- [ ] `NeighborhoodSliceKind` literal type with all 13 slice variants
- [ ] `NeighborhoodSliceV1` with progressive disclosure (preview + total)
- [ ] `BundleMetaV1` struct with tool/version/workspace/query/timestamps/LSP servers
- [ ] `NeighborhoodGraphSummaryV1` struct with node/edge counts and artifact pointer
- [ ] `SemanticNeighborhoodBundleV1` CANONICAL schema with subject/meta/graph/artifacts fields
- [ ] `SemanticNeighborhoodBundleRefV1` with deterministic ID and byte size
- [ ] `ArtifactPointerV1` generic pattern

**Registry Implementation:**
- [ ] Registry uses `dict[str, type[msgspec.Struct]]` not `dict[str, str]`
- [ ] `resolve_kind()` returns actual struct type for runtime validation
- [ ] `validate_finding_details()` checks kind against registry
- [ ] `decode_finding_details()` converts dict to typed struct
- [ ] Registry includes all SNB struct kinds (8 total including BundleMetaV1, NeighborhoodGraphSummaryV1)

**Integration:**
- [ ] `EnrichedFinding` has optional `neighborhood_bundle` field
- [ ] `EnrichedFinding` has `degrade_events` field (typed tuple)
- [ ] Public types exported from `tools/cq/core/__init__.py`

**Testing:**
- [ ] msgspec roundtrip tests for all SNB structs (8 types)
- [ ] msgspec roundtrip tests for BundleMetaV1 and NeighborhoodGraphSummaryV1
- [ ] kind-registry validation tests (known kind → type, unknown → None)
- [ ] decode_finding_details success/failure cases
- [ ] Schema evolution tests (backward compat with extra fields)

**Schema Compatibility Validation:**
- [ ] Every snippet compiles against canonical R1 schema (no import errors)
- [ ] Snippet compile-check gate runs for all R5-R7 code blocks against canonical R1 types
- [ ] No usage of deprecated fields (subject_node_id, metadata on bundle)
- [ ] All downstream sections use bundle.subject, bundle.meta, bundle.graph, bundle.artifacts
- [ ] All downstream sections use slice.preview (not slice.nodes)

**Acceptance Criteria:**
- All structs serialize/deserialize via msgspec without errors
- Registry lookup returns correct struct types
- Typed diagnostics field accepts DegradeEventV1 tuples
- Canonical schema includes all fields needed by R5/R6/R7 (no shadow structs)
- All existing tests pass
- `uv run pyrefly check` passes
- `uv run pytest tests/msgspec_contract/test_snb_contracts.py -v` passes

---

## R2: Rust LSP Session Foundation + Environment Envelope

**Goal:** Establish production-ready Rust LSP session with typed contracts, environment capture, and diagnostic normalization.

**Rationale:** Rust LSP enrichment requires capability negotiation, quiescence tracking, and diagnostic bridging. Session environment envelope enables quality gating and reproducible enrichment.

### Session Environment Envelope

```python
# tools/cq/search/rust_lsp_contracts.py
from __future__ import annotations

import msgspec
from typing import Literal

from tools.cq.core.structs import CqStruct


class LspServerCapabilitySnapshotV1(CqStruct, frozen=True):
    """Negotiated server capability projection.

    Stores capabilities using PROVIDER fields (definitionProvider,
    referencesProvider, etc.) for correct capability checking.
    Retains rich provider payloads for advanced-plane precision gates.
    """

    definition_provider: bool = False
    type_definition_provider: bool = False
    implementation_provider: bool = False
    references_provider: bool = False
    document_symbol_provider: bool = False
    call_hierarchy_provider: bool = False
    type_hierarchy_provider: bool = False
    hover_provider: bool = False
    workspace_symbol_provider: bool = False
    rename_provider: bool = False
    code_action_provider: bool = False
    semantic_tokens_provider: bool = False
    inlay_hint_provider: bool = False
    semantic_tokens_provider_raw: dict[str, object] | None = None
    code_action_provider_raw: object | None = None
    workspace_symbol_provider_raw: object | None = None


class LspClientPublishDiagnosticsCapsV1(CqStruct, frozen=True):
    """Client diagnostics capability detail from initialize request."""

    enabled: bool = False
    related_information: bool = False
    version_support: bool = False
    code_description_support: bool = False
    data_support: bool = False


class LspClientCapabilitySnapshotV1(CqStruct, frozen=True):
    """Client-advertised capability projection used for bridge gating."""

    publish_diagnostics: LspClientPublishDiagnosticsCapsV1 = msgspec.field(
        default_factory=LspClientPublishDiagnosticsCapsV1
    )


class LspExperimentalCapabilitySnapshotV1(CqStruct, frozen=True):
    """Experimental capability snapshot (client/server negotiated)."""

    server_status_notification: bool = False


class LspCapabilitySnapshotV1(CqStruct, frozen=True):
    """Typed split snapshot for capability gating decisions."""

    server_caps: LspServerCapabilitySnapshotV1 = msgspec.field(
        default_factory=LspServerCapabilitySnapshotV1
    )
    client_caps: LspClientCapabilitySnapshotV1 = msgspec.field(
        default_factory=LspClientCapabilitySnapshotV1
    )
    experimental_caps: LspExperimentalCapabilitySnapshotV1 = msgspec.field(
        default_factory=LspExperimentalCapabilitySnapshotV1
    )


class LspSessionEnvV1(CqStruct, frozen=True):
    """Shared LSP session/environment envelope for quality gating.

    Captures negotiated capabilities, server health, and configuration
    to enable reproducible enrichment and capability-based slice planning.
    """

    server_name: str | None = None
    server_version: str | None = None
    position_encoding: str = "utf-16"  # utf-8, utf-16, utf-32
    capabilities: LspCapabilitySnapshotV1 = msgspec.field(default_factory=LspCapabilitySnapshotV1)
    workspace_health: Literal["ok", "warning", "error", "unknown"] = "unknown"
    quiescent: bool = False
    config_fingerprint: str | None = None  # Hash of effective workspace/configuration responses
    refresh_events: tuple[str, ...] = ()  # e.g. ("workspace/didChangeConfiguration",)


class RustDiagnosticV1(CqStruct, frozen=True):
    """Normalized LSP diagnostic from publishDiagnostics notification.

    Bridges LSP diagnostics to CQ enrichment pipeline with full
    span normalization and code-action passthrough.
    """

    uri: str
    range_start_line: int = 0
    range_start_col: int = 0
    range_end_line: int = 0
    range_end_col: int = 0
    severity: int = 0  # 1=Error, 2=Warning, 3=Info, 4=Hint
    code: str | None = None
    source: str | None = None
    message: str = ""
    related_info: tuple[dict[str, object], ...] = ()
    data: dict[str, object] | None = None  # Passthrough for code-action bridging
```

### Rust LSP Enrichment Contracts

Mirrors the `pyrefly_contracts.py` pattern exactly (Optimization #1 from review).

```python
# tools/cq/search/rust_lsp_contracts.py (continued)
from __future__ import annotations

from typing import Literal

from tools.cq.core.structs import CqStruct


class RustLspTarget(CqStruct, frozen=True):
    """Normalized LSP location target (definition, reference, etc.)."""

    uri: str
    range_start_line: int = 0
    range_start_col: int = 0
    range_end_line: int = 0
    range_end_col: int = 0
    container_name: str | None = None


class RustCallLink(CqStruct, frozen=True):
    """Call hierarchy link (caller or callee)."""

    name: str
    kind: int = 0  # SymbolKind enum
    uri: str = ""
    range_start_line: int = 0
    range_start_col: int = 0
    from_ranges: tuple[tuple[int, int, int, int], ...] = ()  # Call sites


class RustTypeLink(CqStruct, frozen=True):
    """Type hierarchy link (supertype or subtype)."""

    name: str
    kind: int = 0  # SymbolKind enum
    uri: str = ""
    range_start_line: int = 0
    range_start_col: int = 0


class RustDocSymbol(CqStruct, frozen=True):
    """Document symbol from textDocument/documentSymbol."""

    name: str
    kind: int = 0  # SymbolKind enum
    range_start_line: int = 0
    range_start_col: int = 0
    range_end_line: int = 0
    range_end_col: int = 0
    children: tuple[RustDocSymbol, ...] = ()


class RustSymbolGrounding(CqStruct, frozen=True):
    """Symbol grounding bundle (definitions, references, etc.)."""

    definitions: tuple[RustLspTarget, ...] = ()
    type_definitions: tuple[RustLspTarget, ...] = ()
    implementations: tuple[RustLspTarget, ...] = ()
    references: tuple[RustLspTarget, ...] = ()


class RustCallGraph(CqStruct, frozen=True):
    """Call graph bundle (incoming callers + outgoing callees)."""

    incoming_callers: tuple[RustCallLink, ...] = ()
    outgoing_callees: tuple[RustCallLink, ...] = ()


class RustTypeHierarchy(CqStruct, frozen=True):
    """Type hierarchy bundle (supertypes + subtypes)."""

    supertypes: tuple[RustTypeLink, ...] = ()
    subtypes: tuple[RustTypeLink, ...] = ()


class RustLspEnrichmentPayload(CqStruct, frozen=True):
    """Normalized Rust LSP enrichment payload — mirrors PyreflyEnrichmentPayload.

    Single unified payload for all Rust LSP enrichment surfaces.
    """

    session_env: LspSessionEnvV1
    symbol_grounding: RustSymbolGrounding
    call_graph: RustCallGraph
    type_hierarchy: RustTypeHierarchy
    document_symbols: tuple[RustDocSymbol, ...] = ()
    diagnostics: tuple[RustDiagnosticV1, ...] = ()
    hover_text: str | None = None
```

### Coercion Functions (Matching pyrefly_contracts.py Pattern)

```python
# tools/cq/search/rust_lsp_contracts.py (continued)
from __future__ import annotations

from collections.abc import Mapping


def coerce_rust_lsp_payload(
    payload: Mapping[str, object] | None,
) -> RustLspEnrichmentPayload:
    """Normalize loose Rust LSP payload to typed contracts.

    Mirrors coerce_pyrefly_payload() pattern exactly.
    Guarantees type-safe RustLspEnrichmentPayload even with partial data.
    """
    if payload is None:
        return RustLspEnrichmentPayload(
            session_env=LspSessionEnvV1(),
            symbol_grounding=RustSymbolGrounding(),
            call_graph=RustCallGraph(),
            type_hierarchy=RustTypeHierarchy(),
            document_symbols=(),
            diagnostics=(),
        )

    # Extract and normalize each section
    session_env = _coerce_session_env(payload.get("session_env"))
    symbol_grounding = _coerce_symbol_grounding(payload.get("symbol_grounding"))
    call_graph = _coerce_call_graph(payload.get("call_graph"))
    type_hierarchy = _coerce_type_hierarchy(payload.get("type_hierarchy"))
    document_symbols = _coerce_document_symbols(payload.get("document_symbols"))
    diagnostics = _coerce_diagnostics(payload.get("diagnostics"))
    hover_text = payload.get("hover_text")

    return RustLspEnrichmentPayload(
        session_env=session_env,
        symbol_grounding=symbol_grounding,
        call_graph=call_graph,
        type_hierarchy=type_hierarchy,
        document_symbols=document_symbols,
        diagnostics=diagnostics,
        hover_text=hover_text if isinstance(hover_text, str) else None,
    )


def rust_lsp_payload_to_dict(
    payload: RustLspEnrichmentPayload,
) -> dict[str, object]:
    """Convert typed payload back to dict for enrichment pipeline.

    Matches pyrefly_payload_to_dict() pattern.
    """
    return {
        "session_env": msgspec.to_builtins(payload.session_env),
        "symbol_grounding": msgspec.to_builtins(payload.symbol_grounding),
        "call_graph": msgspec.to_builtins(payload.call_graph),
        "type_hierarchy": msgspec.to_builtins(payload.type_hierarchy),
        "document_symbols": msgspec.to_builtins(payload.document_symbols),
        "diagnostics": msgspec.to_builtins(payload.diagnostics),
        "hover_text": payload.hover_text,
    }


# Private normalization helpers
def _coerce_session_env(data: object) -> LspSessionEnvV1:
    """Normalize session environment data to LspSessionEnvV1."""
    if isinstance(data, Mapping):
        return LspSessionEnvV1.from_mapping(data)
    return LspSessionEnvV1()


def _coerce_symbol_grounding(data: object) -> RustSymbolGrounding:
    """Normalize symbol grounding data to RustSymbolGrounding."""
    if isinstance(data, Mapping):
        return RustSymbolGrounding.from_mapping(data)
    return RustSymbolGrounding()


def _coerce_call_graph(data: object) -> RustCallGraph:
    """Normalize call graph data to RustCallGraph."""
    if isinstance(data, Mapping):
        return RustCallGraph.from_mapping(data)
    return RustCallGraph()


def _coerce_type_hierarchy(data: object) -> RustTypeHierarchy:
    """Normalize type hierarchy data to RustTypeHierarchy."""
    if isinstance(data, Mapping):
        return RustTypeHierarchy.from_mapping(data)
    return RustTypeHierarchy()


def _coerce_document_symbols(data: object) -> tuple[RustDocSymbol, ...]:
    """Normalize document symbols list."""
    if not isinstance(data, (list, tuple)):
        return ()
    return tuple(
        RustDocSymbol.from_mapping(item)
        for item in data
        if isinstance(item, Mapping)
    )


def _coerce_diagnostics(data: object) -> tuple[RustDiagnosticV1, ...]:
    """Normalize diagnostics list."""
    if not isinstance(data, (list, tuple)):
        return ()
    return tuple(
        RustDiagnosticV1.from_mapping(item)
        for item in data
        if isinstance(item, Mapping)
    )
```

### Rust LSP Session Implementation

```python
# tools/cq/search/rust_lsp.py
from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
import time
from pathlib import Path

from tools.cq.core.structs import CqStruct
from tools.cq.search.rust_lsp_contracts import (
    LspCapabilitySnapshotV1,
    LspClientCapabilitySnapshotV1,
    LspClientPublishDiagnosticsCapsV1,
    LspExperimentalCapabilitySnapshotV1,
    LspServerCapabilitySnapshotV1,
    LspSessionEnvV1,
    RustDiagnosticV1,
    RustLspEnrichmentPayload,
    coerce_rust_lsp_payload,
)


class RustLspRequest(CqStruct, frozen=True):
    """Request for Rust LSP enrichment."""

    file_path: str
    line: int
    col: int
    query_intent: str = "symbol_grounding"


class _RustLspSession:
    """Rust LSP session with environment capture and capability tracking.

    Lifecycle:
    1. _start() → spawn rust-analyzer, negotiate capabilities
    2. probe() → execute enrichment requests
    3. shutdown() → graceful cleanup
    """

    def __init__(self, repo_root: Path) -> None:
        self.repo_root = repo_root
        self._process: subprocess.Popen[bytes] | None = None
        self._session_env = LspSessionEnvV1()
        self._diagnostics_by_uri: dict[str, list[RustDiagnosticV1]] = {}

    def _start(self) -> None:
        """Start rust-analyzer and negotiate capabilities."""
        # Spawn rust-analyzer subprocess
        # ... (same as v1 implementation)

        # Send initialize request
        init_response = self._send_request("initialize", {
            "processId": None,
            "rootUri": self.repo_root.as_uri(),
            "capabilities": {
                "textDocument": {
                    "definition": {"linkSupport": True},
                    "typeDefinition": {"linkSupport": True},
                    "implementation": {"linkSupport": True},
                    "references": {},
                    "documentSymbol": {"hierarchicalDocumentSymbolSupport": True},
                    "callHierarchy": {},
                    "typeHierarchy": {},
                    "hover": {"contentFormat": ["plaintext", "markdown"]},
                    "publishDiagnostics": {
                        "relatedInformation": True,
                        "versionSupport": True,
                        "codeDescriptionSupport": True,
                        "dataSupport": True,
                    },
                },
                "experimental": {
                    "serverStatusNotification": True,
                },
            },
        })

        # Capture session environment from initialization
        server_info = init_response.get("serverInfo", {})
        server_caps = init_response.get("capabilities", {})
        position_encoding = server_caps.get("positionEncoding", "utf-16")

        # Extract capability snapshot (server/client/experimental split)
        capabilities = LspCapabilitySnapshotV1(
            server_caps=LspServerCapabilitySnapshotV1(
                definition_provider="definitionProvider" in server_caps,
                type_definition_provider="typeDefinitionProvider" in server_caps,
                implementation_provider="implementationProvider" in server_caps,
                references_provider="referencesProvider" in server_caps,
                document_symbol_provider="documentSymbolProvider" in server_caps,
                call_hierarchy_provider="callHierarchyProvider" in server_caps,
                type_hierarchy_provider="typeHierarchyProvider" in server_caps,
                hover_provider="hoverProvider" in server_caps,
                workspace_symbol_provider="workspaceSymbolProvider" in server_caps,
                rename_provider="renameProvider" in server_caps,
                code_action_provider="codeActionProvider" in server_caps,
                semantic_tokens_provider="semanticTokensProvider" in server_caps,
                inlay_hint_provider="inlayHintProvider" in server_caps,
                semantic_tokens_provider_raw=(
                    server_caps.get("semanticTokensProvider")
                    if isinstance(server_caps.get("semanticTokensProvider"), dict)
                    else None
                ),
                code_action_provider_raw=server_caps.get("codeActionProvider"),
                workspace_symbol_provider_raw=server_caps.get("workspaceSymbolProvider"),
            ),
            client_caps=LspClientCapabilitySnapshotV1(
                publish_diagnostics=LspClientPublishDiagnosticsCapsV1(
                    enabled=True,
                    related_information=True,
                    version_support=True,
                    code_description_support=True,
                    data_support=True,
                )
            ),
            experimental_caps=LspExperimentalCapabilitySnapshotV1(
                server_status_notification=True,
            ),
        )

        # Compute config fingerprint from effective workspace/configuration responses
        # served by the client (updated whenever workspace/didChangeConfiguration fires).
        config_data = self._config_response_cache.snapshot()
        config_fingerprint = hashlib.sha256(
            json.dumps(config_data, sort_keys=True).encode()
        ).hexdigest()[:16]

        self._session_env = LspSessionEnvV1(
            server_name=server_info.get("name"),
            server_version=server_info.get("version"),
            position_encoding=position_encoding,
            capabilities=capabilities,
            workspace_health="unknown",  # Will update after quiescence
            quiescent=False,
            config_fingerprint=config_fingerprint,
        )

        # Send initialized notification
        self._send_notification("initialized", {})

        # Serve workspace/configuration requests and cache effective responses
        # used to compute config fingerprints and invalidation state.
        self._start_configuration_bridge()

        # Wait for quiescence (server finishes indexing)
        self._wait_for_quiescence()

    def _wait_for_quiescence(self, timeout: float = 30.0) -> None:
        """Wait for rust-analyzer to finish indexing.

        Monitors experimental/serverStatus notifications for quiescent=true.
        """
        start = time.time()
        while time.time() - start < timeout:
            # Process incoming notifications
            notifications = self._read_pending_notifications()
            for notif in notifications:
                if notif.get("method") == "experimental/serverStatus":
                    params = notif.get("params", {})
                    if not isinstance(params, dict):
                        continue
                    reported_health = params.get("health")
                    health = (
                        reported_health
                        if reported_health in ("ok", "warning", "error")
                        else self._session_env.workspace_health
                    )
                    quiescent = bool(params.get("quiescent", self._session_env.quiescent))
                    self._session_env = LspSessionEnvV1(
                        server_name=self._session_env.server_name,
                        server_version=self._session_env.server_version,
                        position_encoding=self._session_env.position_encoding,
                        capabilities=self._session_env.capabilities,
                        workspace_health=health,
                        quiescent=quiescent,
                        config_fingerprint=self._session_env.config_fingerprint,
                        refresh_events=self._session_env.refresh_events,
                    )
                    if quiescent:
                        return
                elif notif.get("method") == "textDocument/publishDiagnostics":
                    self._handle_diagnostics_notification(notif)
                elif notif.get("method") == "workspace/didChangeConfiguration":
                    self._record_config_invalidation(notif)
            time.sleep(0.1)

    def _handle_diagnostics_notification(self, notif: dict[str, object]) -> None:
        """Capture diagnostics from publishDiagnostics notification."""
        params = notif.get("params", {})
        if not isinstance(params, dict):
            return

        uri = params.get("uri")
        diagnostics_data = params.get("diagnostics", [])

        if not isinstance(uri, str) or not isinstance(diagnostics_data, list):
            return

        diagnostics = []
        for diag in diagnostics_data:
            if not isinstance(diag, dict):
                continue

            range_data = diag.get("range", {})
            if not isinstance(range_data, dict):
                continue

            start = range_data.get("start", {})
            end = range_data.get("end", {})

            diagnostics.append(RustDiagnosticV1(
                uri=uri,
                range_start_line=start.get("line", 0),
                range_start_col=start.get("character", 0),
                range_end_line=end.get("line", 0),
                range_end_col=end.get("character", 0),
                severity=diag.get("severity", 0),
                code=diag.get("code"),
                source=diag.get("source"),
                message=diag.get("message", ""),
                related_info=tuple(diag.get("relatedInformation", [])),
                data=diag.get("data"),
            ))

        self._diagnostics_by_uri[uri] = diagnostics

    def probe(self, request: RustLspRequest) -> RustLspEnrichmentPayload | None:
        """Execute enrichment probe with tiered capability gating.

        Returns typed RustLspEnrichmentPayload with session environment
        and all available enrichment surfaces. Returns None only for
        transport/session-fatal failure at the outer boundary.
        Non-fatal capability/health degradation returns partial typed payload.

        Tiered gating:
        - Tier A (session alive): hover, definition, type definition
        - Tier B (health in {ok, warning}): references, document symbols
        - Tier C (health=ok and quiescent): call hierarchy, type hierarchy
        """
        if self._process is None:
            return None  # Transport/session-fatal boundary

        health = self._session_env.workspace_health
        caps = self._session_env.capabilities.server_caps

        # Tier A: Always available
        requests = {}
        if caps.hover_provider:
            requests["hover"] = ("textDocument/hover", {})
        if caps.definition_provider:
            requests["definition"] = ("textDocument/definition", {})
        if caps.type_definition_provider:
            requests["typeDefinition"] = ("textDocument/typeDefinition", {})

        # Tier B: Require health in {ok, warning}
        if health in ("ok", "warning"):
            if caps.references_provider:
                requests["references"] = ("textDocument/references", {})
            if caps.document_symbol_provider:
                requests["documentSymbol"] = ("textDocument/documentSymbol", {})

        # Tier C: Require health=ok and quiescent
        if health == "ok" and self._session_env.quiescent:
            if caps.call_hierarchy_provider:
                requests["prepareCallHierarchy"] = ("textDocument/prepareCallHierarchy", {})
            if caps.type_hierarchy_provider:
                requests["prepareTypeHierarchy"] = ("textDocument/prepareTypeHierarchy", {})

        # Execute requests (same request logic as v1)
        # ... (request execution omitted for brevity) ...

        # Assemble raw payload dict
        raw_payload = self._assemble_raw_payload(
            responses, incoming, outgoing, supertypes, subtypes, uri, request
        )

        # Add diagnostics for this file
        uri_str = Path(request.file_path).as_uri()
        raw_payload["diagnostics"] = self._diagnostics_by_uri.get(uri_str, [])

        # Coerce to typed payload
        return coerce_rust_lsp_payload(raw_payload)

    def shutdown(self) -> None:
        """Graceful shutdown."""
        # Same as v1
        ...
```

### Target Files for R2

| Action | File | Purpose |
|--------|------|---------|
| **Create** | `tools/cq/search/rust_lsp.py` | Session implementation |
| **Create** | `tools/cq/search/rust_lsp_contracts.py` | Typed contracts + coercion |
| **Create** | `tests/unit/cq/search/test_rust_lsp_contracts.py` | Contract tests |
| **Create** | `tests/unit/cq/search/test_rust_lsp_session.py` | Session tests |
| **Edit** | `tools/cq/search/rust_enrichment.py` | Integrate session |
| **Edit** | `tools/cq/core/enrichment_facts.py` | Add Rust LSP facts |

### Implementation Checklist for R2

**Session Environment:**
- [ ] `LspCapabilitySnapshotV1` struct with provider-field capabilities
- [ ] `LspSessionEnvV1` uses `capabilities: LspCapabilitySnapshotV1` instead of flat dict
- [ ] `workspace_health` enum uses `Literal["ok", "warning", "error", "unknown"]`
- [ ] `RustDiagnosticV1` with span, severity, code, source, related info, data passthrough
- [ ] `_start()` persists session env (capabilities, position encoding, server info)
- [ ] `_start()` requests `experimental.serverStatusNotification=true` in client caps
- [ ] `_start()` initializes `workspace_health="unknown"` (not "indexing")
- [ ] Server status/quiescence captured from `experimental/serverStatus` notifications
- [ ] `workspace_health` tracks reported `serverStatus.health` independently of quiescence
- [ ] Config fingerprint from effective `workspace/configuration` response hash (served by client)
- [ ] Persist served `workspace/configuration` items/results used by fingerprint computation
- [ ] `publishDiagnostics` notifications normalized to `RustDiagnosticV1`
- [ ] `workspace/didChangeConfiguration` tracked as invalidation event and fingerprint refresh trigger

**Typed Contracts:**
- [ ] `RustLspEnrichmentPayload` mirrors `PyreflyEnrichmentPayload` structure
- [ ] All sub-structures: `RustSymbolGrounding`, `RustCallGraph`, `RustTypeHierarchy`, `RustDocSymbol`
- [ ] `RustLspTarget`, `RustCallLink`, `RustTypeLink` with normalized spans

**Coercion Functions:**
- [ ] `coerce_rust_lsp_payload()` matches `coerce_pyrefly_payload()` pattern
- [ ] `rust_lsp_payload_to_dict()` matches `pyrefly_payload_to_dict()` pattern
- [ ] Private normalization helpers for each section
- [ ] Graceful handling of missing/malformed data

**Session Behavior:**
- [ ] `probe()` returns `RustLspEnrichmentPayload` not raw dict
- [ ] `probe()` implements tiered gating (Tier A session-alive, Tier B with ok/warning, Tier C with ok+quiescent)
- [ ] `probe()` returns `None` only on transport/session-fatal failure; non-fatal degradation returns partial typed payload
- [ ] Capability checking uses provider fields (e.g., `definitionProvider`, not `textDocument.definition`)
- [ ] Diagnostic capture wired to probe results
- [ ] Capability snapshot stores server/client/experimental surfaces (no client/server leakage)

**Optimizations:**
- [ ] O1: Add `LspCapabilitySnapshotV1` as shared struct (used by both Rust and Pyrefly sessions)

**Extensions:**
- [ ] E2: Add Rust environment/configuration invalidation (refresh events tracking)

**Testing:**
- [ ] Capability negotiation test (what we request vs what server reports)
- [ ] Position encoding negotiation test
- [ ] Health/quiescence tiered gating behavior test (Tier A/B/C)
- [ ] Diagnostic normalization test
- [ ] Config fingerprint stability test
- [ ] Mock JSON-RPC transcript test for full probe cycle
- [ ] Msgspec roundtrip tests for all contract types
- [ ] Test that `probe()` returns `None` only on transport/session-fatal boundary

**Acceptance Criteria:**
- All contracts serialize/deserialize via msgspec
- Session captures environment correctly
- Diagnostics normalized with full span data
- probe() returns typed payload with session env for non-fatal paths; `None` only on transport/session-fatal boundary
- All tests pass
- `uv run pyrefly check` passes

---

## R3: Pyrefly Expansion Alignment + Capability Gates

**Goal:** Establish capability-gating pattern for optional LSP surfaces and reuse existing `pyrefly_contracts.py` directly in neighborhood assembly.

**Rationale:** Capability gates enable graceful degradation when LSP servers lack features. Direct contract reuse eliminates ad-hoc dict plumbing and establishes single source of truth for Pyrefly payload structure.

### Capability Gate Model

```python
# tools/cq/neighborhood/capability_gates.py
from __future__ import annotations

from typing import Literal, TypeAlias

from tools.cq.core.snb_schema import DegradeEventV1, NeighborhoodSliceKind
from tools.cq.search.rust_lsp_contracts import LspCapabilitySnapshotV1

LspCapability: TypeAlias = Literal[
    "textDocument/definition",
    "textDocument/typeDefinition",
    "textDocument/implementation",
    "textDocument/references",
    "textDocument/documentSymbol",
    "textDocument/prepareCallHierarchy",
    "callHierarchy/incomingCalls",
    "callHierarchy/outgoingCalls",
    "textDocument/prepareTypeHierarchy",
    "typeHierarchy/supertypes",
    "typeHierarchy/subtypes",
    "textDocument/hover",
    "textDocument/publishDiagnostics",
    "textDocument/semanticTokens/full",
    "textDocument/inlayHint",
    "textDocument/rename",
    "textDocument/codeAction",
    "workspace/symbol",
]


# Map each slice kind to required LSP capabilities
SLICE_CAPABILITY_REQUIREMENTS: dict[NeighborhoodSliceKind, tuple[LspCapability, ...]] = {
    "callers": ("textDocument/prepareCallHierarchy", "callHierarchy/incomingCalls"),
    "callees": ("textDocument/prepareCallHierarchy", "callHierarchy/outgoingCalls"),
    "references": ("textDocument/references",),
    "implementations": ("textDocument/implementation",),
    "type_supertypes": ("textDocument/prepareTypeHierarchy", "typeHierarchy/supertypes"),
    "type_subtypes": ("textDocument/prepareTypeHierarchy", "typeHierarchy/subtypes"),
    # Structural slices have no LSP requirements
    "parents": (),
    "children": (),
    "siblings": (),
    "enclosing_context": (),
    "imports": (),
    "importers": (),
    "related": (),
}


def check_capabilities(
    capabilities: LspCapabilitySnapshotV1,
    required: tuple[LspCapability, ...],
) -> tuple[bool, list[LspCapability]]:
    """Check if negotiated capabilities satisfy requirements.

    Returns (all_satisfied, missing_capabilities).

    Capability check rules:
    - server providers: capabilities.server_caps.* provider fields
    - client diagnostics bridge: capabilities.client_caps.publish_diagnostics.*
    - hierarchy methods: provider checks + operation semantics
    """
    missing: list[LspCapability] = []
    for cap in required:
        if not _capability_present(capabilities, cap):
            missing.append(cap)
    return len(missing) == 0, missing


def _capability_present(capabilities: LspCapabilitySnapshotV1, cap: LspCapability) -> bool:
    """Check if specific capability is present in negotiated capabilities.

    Uses provider field checks (e.g., definitionProvider) not textDocument paths.
    Note: textDocument/publishDiagnostics checks client diagnostics capabilities.
    """
    server = capabilities.server_caps
    diagnostics_caps = capabilities.client_caps.publish_diagnostics

    method_to_present = {
        "textDocument/definition": server.definition_provider,
        "textDocument/typeDefinition": server.type_definition_provider,
        "textDocument/implementation": server.implementation_provider,
        "textDocument/references": server.references_provider,
        "textDocument/documentSymbol": server.document_symbol_provider,
        "textDocument/hover": server.hover_provider,
        "textDocument/rename": server.rename_provider,
        "textDocument/codeAction": server.code_action_provider,
        "textDocument/semanticTokens/full": server.semantic_tokens_provider,
        "textDocument/inlayHint": server.inlay_hint_provider,
        "workspace/symbol": server.workspace_symbol_provider,
    }

    if cap in method_to_present:
        return method_to_present[cap]

    # callHierarchy capabilities
    if cap.startswith("callHierarchy/") or cap == "textDocument/prepareCallHierarchy":
        return server.call_hierarchy_provider

    # typeHierarchy capabilities
    if cap.startswith("typeHierarchy/") or cap == "textDocument/prepareTypeHierarchy":
        return server.type_hierarchy_provider

    # publishDiagnostics bridge depends on client-advertised diagnostics support.
    if cap == "textDocument/publishDiagnostics":
        return diagnostics_caps.enabled

    return False


def diagnostics_bridge_capable(capabilities: LspCapabilitySnapshotV1) -> bool:
    """Gate high-fidelity diagnostics bridge surfaces."""
    diag = capabilities.client_caps.publish_diagnostics
    return diag.enabled and diag.related_information and diag.data_support


def plan_feasible_slices(
    requested_slices: tuple[NeighborhoodSliceKind, ...],
    capabilities: LspCapabilitySnapshotV1,
) -> tuple[
    tuple[NeighborhoodSliceKind, ...],
    tuple[DegradeEventV1, ...],
]:
    """Partition requested slices into feasible + degrade events.

    Returns (feasible_slices, degrade_events_for_infeasible).

    Structural slices (no LSP requirements) are always feasible.
    LSP-dependent slices are checked against negotiated capabilities.
    """
    feasible: list[NeighborhoodSliceKind] = []
    degrades: list[DegradeEventV1] = []

    for kind in requested_slices:
        reqs = SLICE_CAPABILITY_REQUIREMENTS.get(kind, ())
        if not reqs:
            # Structural slice — always feasible
            feasible.append(kind)
            continue

        ok, missing = check_capabilities(capabilities, reqs)
        if ok:
            feasible.append(kind)
        else:
            degrades.append(DegradeEventV1(
                stage=f"lsp.capability_gate.{kind}",
                severity="info",
                category="unavailable",
                message=f"Slice '{kind}' skipped: missing capabilities {missing}",
                correlation_key=kind,
            ))

    return tuple(feasible), tuple(degrades)
```

### Pyrefly Contract Reuse Adapter

```python
# tools/cq/neighborhood/pyrefly_adapter.py
from __future__ import annotations

from tools.cq.core.snb_schema import (
    NeighborhoodSliceV1,
    SemanticEdgeV1,
    SemanticNodeRefV1,
)
from tools.cq.search.pyrefly_contracts import (
    PyreflyEnrichmentPayload,
)


def pyrefly_payload_to_slices(
    payload: PyreflyEnrichmentPayload,
    subject_node_id: str,
) -> list[NeighborhoodSliceV1]:
    """Convert typed pyrefly payload into neighborhood slices.

    Reuses existing pyrefly_contracts types directly.
    No ad-hoc dict plumbing.

    This adapter establishes the pattern for R5 assembler integration.
    """
    slices: list[NeighborhoodSliceV1] = []

    # Call graph → callers slice
    # Note: PyreflyCallGraphEdge has fields: symbol, file, line, col (NOT name, uri)
    cg = payload.call_graph
    if cg.incoming_callers:
        caller_nodes = []
        caller_edges = []
        for caller in cg.incoming_callers:
            file_path = caller.file or ""
            node_id = f"pyrefly.caller.{caller.symbol}.{file_path}:{caller.line}"
            caller_nodes.append(SemanticNodeRefV1(
                node_id=node_id,
                kind="function",
                name=caller.symbol,
                display_label=caller.symbol,
                file_path=file_path,
                byte_span=None,  # Line/col to byte span requires file content
            ))
            caller_edges.append(SemanticEdgeV1(
                edge_id=f"{node_id}→{subject_node_id}",
                source_node_id=node_id,
                target_node_id=subject_node_id,
                edge_kind="calls",
                evidence_source="pyrefly.lsp",
            ))

        slices.append(NeighborhoodSliceV1(
            kind="callers",
            title="Callers (Pyrefly)",
            total=len(caller_nodes),
            preview=tuple(caller_nodes[:5]),
            edges=tuple(caller_edges),
            collapsed=True,
        ))

    # Call graph → callees slice
    if cg.outgoing_callees:
        callee_nodes = []
        callee_edges = []
        for callee in cg.outgoing_callees:
            file_path = callee.file or ""
            node_id = f"pyrefly.callee.{callee.symbol}.{file_path}:{callee.line}"
            callee_nodes.append(SemanticNodeRefV1(
                node_id=node_id,
                kind="function",
                name=callee.symbol,
                display_label=callee.symbol,
                file_path=file_path,
                byte_span=None,  # Line/col to byte span requires file content
            ))
            callee_edges.append(SemanticEdgeV1(
                edge_id=f"{subject_node_id}→{node_id}",
                source_node_id=subject_node_id,
                target_node_id=node_id,
                edge_kind="calls",
                evidence_source="pyrefly.lsp",
            ))

        slices.append(NeighborhoodSliceV1(
            kind="callees",
            title="Callees (Pyrefly)",
            total=len(callee_nodes),
            preview=tuple(callee_nodes[:5]),
            edges=tuple(callee_edges),
            collapsed=True,
        ))

    # Local scope context → references slice
    # Note: reference locations live in local_scope_context.reference_locations.
    local_scope = payload.local_scope_context
    if local_scope.reference_locations:
        ref_nodes = []
        ref_edges = []
        for ref in local_scope.reference_locations:
            file_path = ref.file or ""
            node_id = f"pyrefly.ref.{file_path}:{ref.line}:{ref.col}"
            ref_nodes.append(SemanticNodeRefV1(
                node_id=node_id,
                kind="reference",
                name=f"reference:{ref.line}:{ref.col}",
                display_label=f"Reference at {file_path}:{ref.line}:{ref.col}",
                file_path=file_path,
                byte_span=None,  # line/col -> byte span requires file content lookup
            ))
            ref_edges.append(SemanticEdgeV1(
                edge_id=f"{node_id}→{subject_node_id}",
                source_node_id=node_id,
                target_node_id=subject_node_id,
                edge_kind="references",
                evidence_source="pyrefly.lsp",
            ))

        slices.append(NeighborhoodSliceV1(
            kind="references",
            title="References (Pyrefly)",
            total=len(ref_nodes),
            preview=tuple(ref_nodes[:5]),
            edges=tuple(ref_edges),
            collapsed=True,
        ))

    # Note: PyreflyEnrichmentPayload does NOT have type_hierarchy field
    # The actual fields are: symbol_grounding, type_contract, call_graph,
    # class_method_context, local_scope_context, import_alias_resolution,
    # anchor_diagnostics, coverage

    return slices

```

### Target Files for R3

| Action | File | Purpose |
|--------|------|---------|
| **Create** | `tools/cq/neighborhood/capability_gates.py` | Capability gating model |
| **Create** | `tools/cq/neighborhood/pyrefly_adapter.py` | Typed payload → slices |
| **Edit** | `tools/cq/search/pyrefly_contracts.py` | Add workspace symbol stubs if needed |
| **Edit** | `tools/cq/search/pyrefly_lsp.py` | Expose session capabilities |

### Implementation Checklist for R3

**Capability Gates:**
- [ ] `capability_gates.py` with `SLICE_CAPABILITY_REQUIREMENTS` mapping
- [ ] `check_capabilities()` validates `LspCapabilitySnapshotV1` against requirements
- [ ] `_capability_present()` uses provider field checks (definitionProvider, not textDocument.definition)
- [ ] `_capability_present()` validates `textDocument/publishDiagnostics` against `client_caps.publish_diagnostics`
- [ ] Split snapshot used consistently (`server_caps`, `client_caps`, `experimental_caps`)
- [ ] `plan_feasible_slices()` partitions requested slices + produces `DegradeEventV1` for infeasible
- [ ] Correct capability lookup rules (provider fields, not textDocument paths)
- [ ] Structural slices (no LSP deps) always feasible

**Pyrefly Adapter:**
- [ ] `pyrefly_adapter.py` converts `PyreflyEnrichmentPayload` → `NeighborhoodSliceV1` list
- [ ] Direct reuse of `coerce_pyrefly_payload()` — no ad-hoc dict plumbing
- [ ] Adapter uses correct field names: `PyreflyCallGraphEdge` has `symbol`, `file`, `line`, `col` (NOT `name`, `uri`)
- [ ] Adapter does NOT reference non-existent `type_hierarchy` field (PyreflyEnrichmentPayload has 8 fields)
- [ ] Reference slice derives from `payload.local_scope_context.reference_locations` (not symbol grounding)
- [ ] Adapter produces correct slice kinds from typed payload
- [ ] Each slice has nodes + edges with evidence source annotation
- [ ] Progressive disclosure: preview (first 5) + total count

**Pyrefly Contract Extensions:**
- [ ] Expose pyrefly session capabilities for gate checking
- [ ] Add document/workspace symbol surfaces as first-class structural discovery plane
- [ ] Optional: add semantic tokens surface behind capability gate

**Optimizations:**
- [ ] O3: Add `DiagnosticActionBridgeV1` struct for unified diagnostics-to-actions bridge

**Extensions:**
- [ ] E1: Expand Pyrefly contract planes with versioned optional sections:
  - [ ] semantic_overlay plane (contextual type info, inference confidence)
  - [ ] structural_discovery plane (document symbols, workspace symbols)
  - [ ] edit_actions plane (code actions, quick fixes, refactorings)
  - [ ] local_mutation_signal plane (local variable mutations, control flow)

**Testing:**
- [ ] Test: capability check with full caps → all slices feasible
- [ ] Test: capability check with missing call hierarchy → callers/callees degraded
- [ ] Test: degrade events have correct stage/category/correlation_key
- [ ] Test: pyrefly adapter produces correct slice kinds from typed payload
- [ ] Test: adapter handles empty payload sections gracefully
- [ ] Test: structural slices always feasible regardless of capabilities

**Acceptance Criteria:**
- Capability gates partition slices correctly
- Degrade events typed and structured
- Pyrefly adapter produces valid SNB slices from typed contracts
- No ad-hoc dict plumbing in adapter
- All tests pass
- `uv run pyrefly check` passes

---

## R4: Structural Collector + Scan Snapshot Adapter

**Goal:** Build structural neighborhood collector (parents, children, siblings, enclosing context) with decoupled scan snapshot adapter.

**Key Architectural Elements:**
- `ScanSnapshot` public adapter decouples from `query/executor.py` internals
- Structural collector uses interval index for containment queries
- Language detection by file extension
- Deterministic def/call record processing

### Scan Snapshot Adapter (Optimization #2)

The structural collector must NOT couple directly to private `query/executor.py` internals. Instead, add a small public adapter module.

```python
# tools/cq/neighborhood/scan_snapshot.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.query.executor import ScanContext

from tools.cq.astgrep.sgpy_scanner import SgRecord


@dataclass(frozen=True)
class ScanSnapshot:
    """Public adapter exposing only what neighborhood needs from ScanContext.

    Decouples structural collector from private query/executor internals.
    """

    def_records: list[SgRecord]
    call_records: list[SgRecord]
    interval_index: object  # IntervalIndex[SgRecord] — duck typed
    file_index: object  # FileIntervalIndex — duck typed
    calls_by_def: dict[SgRecord, list[SgRecord]]

    @classmethod
    def from_scan_context(cls, ctx: ScanContext) -> ScanSnapshot:
        """Build snapshot from a ScanContext."""
        return cls(
            def_records=ctx.def_records,
            call_records=ctx.call_records,
            interval_index=ctx.interval_index,
            file_index=ctx.file_index,
            calls_by_def=ctx.calls_by_def,
        )

    @classmethod
    def from_records(cls, records: list[SgRecord]) -> ScanSnapshot:
        """Build snapshot from raw SgRecords (uses _build_scan_context internally).

        This is the CORRECT way to build scan context. The actual API is:
        _build_scan_context(records: list[SgRecord]) -> ScanContext
        NOT _build_scan_context(repo_root, language).
        """
        from tools.cq.query.executor import _build_scan_context

        ctx = _build_scan_context(records)
        return cls.from_scan_context(ctx)

    @classmethod
    def build_from_repo(cls, root: Path, lang: str = "auto") -> ScanSnapshot:
        """Build snapshot from repository scan.

        This is the recommended pattern: sg_scan → from_records.

        Parameters
        ----------
        root
            Repository root path
        lang
            Query language (default: "auto")

        Returns
        -------
        ScanSnapshot
            Snapshot from repository scan
        """
        from tools.cq.query.sg_parser import sg_scan

        records = sg_scan(paths=[root], lang=lang, root=root)
        return cls.from_records(records)
```

### Structural Collector Interface

```python
# tools/cq/neighborhood/structural_collector.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.neighborhood.scan_snapshot import ScanSnapshot

from tools.cq.astgrep.sgpy_scanner import SgRecord

from tools.cq.neighborhood.anchor import Anchor

_MAX_SIBLINGS = 20
_MAX_CHILDREN = 50


@dataclass(frozen=True)
class StructuralNeighborhood:
    """Structural relationships from ast-grep scan snapshot."""

    parents: list[SgRecord]
    children: list[SgRecord]
    siblings: list[SgRecord]
    enclosing_context: SgRecord | None


def collect_structural_neighborhood(
    anchor: Anchor,
    snapshot: ScanSnapshot,
    root: Path,
    *,
    max_siblings: int = _MAX_SIBLINGS,
    max_children: int = _MAX_CHILDREN,
) -> StructuralNeighborhood:
    """Collect structural neighborhood from scan snapshot.

    Uses interval index for containment queries:
    - Parents: definitions containing anchor span
    - Children: definitions contained within anchor span
    - Siblings: definitions with shared parent
    - Enclosing context: immediate parent definition

    Language detection by file extension.
    """
    # Algorithm:
    # 1. Query interval index for anchor span containment
    # 2. Filter by language (file extension)
    # 3. Sort by nesting depth (span size)
    # 4. Extract parent chain, children, siblings
    # 5. Apply top-K limits
    ...
```

### Target Files for R4

| Action | File |
|--------|------|
| **Create** | `tools/cq/neighborhood/__init__.py` |
| **Create** | `tools/cq/neighborhood/scan_snapshot.py` |
| **Create** | `tools/cq/neighborhood/structural_collector.py` |
| **Create** | `tests/unit/cq/neighborhood/test_structural_collector.py` |
| **Edit** | `tools/cq/query/executor.py` (no signature changes — adapter wraps existing) |
| **Edit** | `tools/cq/core/enrichment_facts.py` (register structural paths) |

### Implementation Checklist for R4

**Scan Snapshot Adapter:**
- [ ] `ScanSnapshot` adapter with `from_scan_context()`, `from_records()`, and `build_from_repo()` factories
- [ ] `from_records()` uses correct API: `_build_scan_context(records: list[SgRecord])` not `_build_scan_context(repo_root, language)`
- [ ] `build_from_repo()` uses correct chain: `sg_scan(paths=[root], lang=lang, root=root)` from `tools.cq.query.sg_parser`
- [ ] Correct `SgRecord` import: `from tools.cq.astgrep.sgpy_scanner import SgRecord` (NOT from tools.cq.search.sg_types)
- [ ] Adapter exposes only required fields (def_records, call_records, interval_index, file_index, calls_by_def)
- [ ] No direct imports from `tools.cq.query.executor` in structural_collector.py (only in scan_snapshot.py)

**Structural Collector:**
- [ ] `collect_structural_neighborhood()` takes `ScanSnapshot` not `ScanContext`
- [ ] Interval index query for anchor span containment
- [ ] Language detection by file extension
- [ ] Parent chain extraction via nesting depth sort
- [ ] Children extraction (definitions within anchor span)
- [ ] Sibling extraction (shared parent, sorted by position)
- [ ] Enclosing context (immediate parent)
- [ ] Top-K limits applied (max_siblings, max_children)

**Contract Integration:**
- [ ] Register structural paths in `enrichment_facts.py`
- [ ] `StructuralNeighborhood` dataclass with frozen=True
- [ ] All SgRecord lists preserve source metadata

**Testing:**
- [ ] Test: `ScanSnapshot.from_records()` builds valid snapshot from SgRecords
- [ ] Test: structural collector works with `ScanSnapshot` interface
- [ ] Test: parent chain extraction correct for nested definitions
- [ ] Test: children extraction includes only contained definitions
- [ ] Test: sibling extraction excludes anchor itself
- [ ] Test: enclosing context is immediate parent (not grandparent)
- [ ] Test: language filtering by file extension
- [ ] Test: top-K limits applied correctly

**Acceptance Criteria:**
- `ScanSnapshot` adapter decouples structural collector from executor internals
- Structural collector produces correct parent/child/sibling relationships
- Language detection works via file extension
- Top-K limits prevent unbounded results
- All tests pass
- `uv run pyrefly check` passes

---

## R5: SNB Assembler + Deterministic Layout

**Goal:** Assemble `SemanticNeighborhoodBundleV1` with deterministic section ordering, capability-gated planning, structured degradation, and preview/artifact split.

**Key Architectural Elements:**
- Deterministic section ordering via slot map + fixed `SECTION_ORDER` (Blocking #6 fix)
- Capability-gated assembly via `plan_feasible_slices()` (Optimization #3)
- Structured degradation with typed `DegradeEventV1` (Optimization #4)
- Preview/artifact split for heavy payloads (Optimization #5)
- `BundleBuildRequest` takes `ScanSnapshot` not `ScanContext` (Optimization #2)

### Deterministic Section Ordering (Blocking #6 Fix)

The v1 `materialize_section_layout()` appended slices in traversal order, which drifts by collector return order. The fix is to assemble by slot map keyed on slice kind, then emit in fixed `SECTION_ORDER`.

```python
# tools/cq/neighborhood/section_layout.py
from __future__ import annotations

SECTION_ORDER: tuple[str, ...] = (
    "target_tldr",          # 01 - collapsed: False
    "neighborhood_summary",  # 02 - collapsed: False
    "enclosing_context",    # 03 - collapsed: dynamic
    "parents",              # 04 - collapsed: dynamic
    "children",             # 05 - collapsed: True
    "siblings",             # 06 - collapsed: True
    "callers",              # 07 - collapsed: True
    "callees",              # 08 - collapsed: True
    "references",           # 09 - collapsed: True
    "implementations",      # 10 - collapsed: True
    "type_supertypes",      # 11 - collapsed: True
    "type_subtypes",        # 12 - collapsed: True
    "imports_context",      # 13 - collapsed: True
    "lsp_deep_signals",     # 14 - collapsed: True
    "diagnostics",          # 15 - collapsed: True
    "suggested_followups",  # 16 - collapsed: False
    "provenance",           # 17 - collapsed: True
)

_UNCOLLAPSED_SECTIONS = frozenset({
    "target_tldr", "neighborhood_summary", "suggested_followups",
})
_DYNAMIC_COLLAPSE_SECTIONS: dict[str, int] = {
    "parents": 3,
    "enclosing_context": 1,
}


def materialize_section_layout(
    bundle: SemanticNeighborhoodBundleV1,
) -> BundleViewV1:
    """Convert bundle into ordered Section/Finding layout.

    CRITICAL: Assembly is keyed by slice kind, then emitted in fixed
    SECTION_ORDER. This prevents drift by collector return order.
    """
    view = BundleViewV1()

    # 01: Target TL;DR (always first)
    view.key_findings.extend(_build_target_findings(bundle.subject))

    # Build slot map: kind → slice
    slot_map: dict[str, NeighborhoodSliceV1] = {}
    for s in bundle.slices:
        slot_map[s.kind] = s

    # Emit synthetic sections (not slice-backed)
    view.sections.append(_build_summary_section(bundle))

    # Emit slice-backed sections in SECTION_ORDER
    for slot_kind in SECTION_ORDER:
        if slot_kind in ("target_tldr", "neighborhood_summary",
                         "suggested_followups", "provenance",
                         "diagnostics"):
            continue  # synthetic sections handled separately
        if slot_kind in slot_map:
            view.sections.append(_slice_to_section(slot_map[slot_kind]))

    # Fallback: any slice kinds NOT in SECTION_ORDER go at end
    known_kinds = set(SECTION_ORDER)
    for s in bundle.slices:
        if s.kind not in known_kinds:
            view.sections.append(_slice_to_section(s))

    # Diagnostics section (from typed DegradeEventV1)
    if bundle.diagnostics:
        view.sections.append(_build_diagnostics_section(bundle.diagnostics))

    # Suggested follow-ups + provenance (always last)
    view.sections.append(_build_followup_section(bundle))
    view.sections.append(_build_provenance_section(bundle))

    return view
```

### Capability-Gated Assembly (Optimization #3)

The assembler uses `plan_feasible_slices()` from R3 before dispatching LSP requests:

```python
# tools/cq/neighborhood/bundle_builder.py
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.neighborhood.anchor import Anchor
    from tools.cq.neighborhood.scan_snapshot import ScanSnapshot

from tools.cq.neighborhood.capability_gates import plan_feasible_slices
from tools.cq.core.snb_schema import (
    DegradeEventV1,
    NeighborhoodSliceV1,
    SemanticNeighborhoodBundleV1,
)
from tools.cq.neighborhood.section_layout import materialize_section_layout
from tools.cq.neighborhood.structural_collector import collect_structural_neighborhood
from tools.cq.utils.time_utils import ms


class BundleBuildRequest:
    """Request to build a semantic neighborhood bundle.

    Takes ScanSnapshot (not ScanContext) per Optimization #2.
    """

    def __init__(
        self,
        *,
        anchor: Anchor,
        root: Path,
        snapshot: ScanSnapshot,  # NOT ScanContext
        language: str,
        symbol_hint: str | None = None,
        top_k: int = 10,
        enable_lsp: bool = True,
        artifact_dir: Path | None = None,
    ) -> None:
        self.anchor = anchor
        self.root = root
        self.snapshot = snapshot
        self.language = language
        self.symbol_hint = symbol_hint
        self.top_k = top_k
        self.enable_lsp = enable_lsp
        self.artifact_dir = artifact_dir


def build_neighborhood_bundle(
    request: BundleBuildRequest,
) -> SemanticNeighborhoodBundleV1:
    """Build semantic neighborhood bundle with capability-gated assembly.

    Assembly phases:
    1. Structural neighborhood (always available)
    2. Capability-gated LSP enrichment (if enabled)
    3. Deterministic section layout via slot map + SECTION_ORDER
    4. Artifact storage with preview/body split
    """
    started = ms()
    degrade_events: list[DegradeEventV1] = []

    # Phase 1: structural neighborhood (always available)
    structural = collect_structural_neighborhood(
        anchor=request.anchor,
        snapshot=request.snapshot,  # Uses ScanSnapshot, not ScanContext
        root=request.root,
    )

    # Phase 2: capability-gated LSP enrichment
    lsp_slices: list[NeighborhoodSliceV1] = []
    if request.enable_lsp:
        feasible, cap_degrades = plan_feasible_slices(
            requested_slices=_lsp_slice_kinds(),
            capabilities=_get_negotiated_caps(request),
        )
        degrade_events.extend(cap_degrades)
        lsp_slices, lsp_degrades = _collect_lsp_slices(
            request, feasible
        )
        degrade_events.extend(lsp_degrades)

    # Phase 3: deterministic assembly
    all_slices = _merge_slices(structural, lsp_slices, request.top_k)

    # Phase 4: artifacts with preview/body split
    artifacts = _store_artifacts_with_preview(
        request.artifact_dir, all_slices
    )

    bundle = SemanticNeighborhoodBundleV1(
        bundle_id=_generate_bundle_id(),
        subject=_build_subject_node(request),
        subject_label=_build_subject_label(request),
        meta=_build_meta(request, started),
        slices=tuple(all_slices),
        graph=_build_graph_summary(structural, lsp_slices),
        node_index=None,  # Optional: precomputed node lookup
        artifacts=tuple(artifacts),
        diagnostics=tuple(degrade_events),
        schema_version="cq.snb.v1",
    )

    # Phase 5: materialize view layout (deterministic ordering)
    view = materialize_section_layout(bundle)
    # Update bundle with view (or return separate view)
    # ... implementation depends on contract design

    return bundle
```

### Preview/Artifact Split (Optimization #5)

```python
# tools/cq/neighborhood/bundle_builder.py (continued)
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from tools.cq.core.snb_schema import ArtifactPointerV1, NeighborhoodSliceV1


def _store_artifacts_with_preview(
    artifact_dir: Path | None,
    slices: list[NeighborhoodSliceV1],
) -> list[ArtifactPointerV1]:
    """Store heavy payloads as artifacts. Preview stays in slice.

    Every heavy LSP surface uses:
    - small preview in section details (top-K items)
    - full payload in artifact file
    - deterministic artifact IDs and sizes in provenance
    """
    if artifact_dir is None:
        return []

    artifacts: list[ArtifactPointerV1] = []
    for s in slices:
        if s.total <= len(s.preview):
            continue  # no overflow, no artifact needed

        artifact_id = f"slice_{s.kind}"
        artifact_path = artifact_dir / f"{artifact_id}.json"

        # Write full data (nodes + edges)
        # NOTE: Use s.preview field (NOT s.nodes) per R1 schema
        full_data = {
            "kind": s.kind,
            "preview": [n.to_dict() for n in s.preview],
            "edges": [e.to_dict() for e in s.edges] if s.edges else [],
        }
        artifact_path.write_text(json.dumps(full_data, indent=2))

        # Record artifact metadata
        payload_bytes = artifact_path.read_bytes()
        artifacts.append(
            ArtifactPointerV1(
                artifact_kind="snb.slice",
                artifact_id=artifact_id,
                deterministic_id=hashlib.sha256(payload_bytes).hexdigest(),
                byte_size=len(payload_bytes),
                storage_path=str(artifact_path),
                metadata={"slice_kind": s.kind, "format": "json"},
            )
        )

    return artifacts
```

### Target Files for R5

| Action | File |
|--------|------|
| **Create** | `tools/cq/neighborhood/bundle_builder.py` |
| **Create** | `tools/cq/neighborhood/section_layout.py` |
| **Create** | `tools/cq/neighborhood/snb_renderer.py` |
| **Edit** | `tools/cq/core/report.py` (extract `_format_finding` as importable) |
| **Edit** | `tools/cq/cli_app/result.py` (add bundle rendering dispatch) |
| **Edit** | `tools/cq/search/pyrefly_lsp.py` (expose capabilities for gate check) |

### Implementation Checklist for R5

**Deterministic Section Ordering:**
- [ ] `materialize_section_layout()` uses slot map keyed by kind, then emits in fixed `SECTION_ORDER`
- [ ] Fallback slot for unknown slice kinds (appended at end)
- [ ] `_UNCOLLAPSED_SECTIONS` controls default collapse state
- [ ] `_DYNAMIC_COLLAPSE_SECTIONS` controls count-based collapse

**Capability-Gated Assembly:**
- [ ] Capability-gated assembly via `plan_feasible_slices()` before LSP dispatch
- [ ] Degrade events collected from both capability gates and LSP failures
- [ ] `BundleBuildRequest` takes `ScanSnapshot` not `ScanContext`
- [ ] LSP slices only attempted if `enable_lsp=True`

**Preview/Artifact Split:**
- [ ] Preview/artifact split: heavy payloads in artifact files, top-K in slice preview
- [ ] Deterministic artifact IDs (`slice_{kind}.json`)
- [ ] Artifact metadata uses canonical fields: `artifact_kind`, `artifact_id`, `deterministic_id`, `byte_size`, `storage_path`
- [ ] Preview stays in slice (`NeighborhoodSliceV1.preview`)
- [ ] Full payload in artifact file (nodes + edges)

**Bundle Assembly:**
- [ ] `build_neighborhood_bundle()` orchestrates all phases
- [ ] Structural neighborhood always collected (no LSP dependency)
- [ ] LSP slices capability-gated and optional
- [ ] Diagnostics section rendered from `DegradeEventV1` tuples (not strings)
- [ ] Metadata includes timing, anchor, language

**Testing:**
- [ ] Test: section ordering matches `SECTION_ORDER` regardless of collector return order
- [ ] Test: unknown slice kinds appear after known slots
- [ ] Test: capability gate produces degrade events for infeasible slices
- [ ] Test: preview/artifact split produces artifact only when total > preview count
- [ ] Test: deterministic ordering golden test
- [ ] Test: LSP disabled → structural slices only
- [ ] Test: artifact IDs deterministic and reproducible

**Acceptance Criteria:**
- Section ordering is deterministic regardless of collector return order
- Capability gates prevent infeasible LSP requests
- Degrade events structured and typed
- Preview/artifact split prevents unbounded context
- All tests pass
- `uv run pyrefly check` passes

---

## R6: LDMD Services (Strict Parser + Protocol Commands)

**Goal:** Implement LDMD progressive disclosure protocol with strict parser, protocol commands, and preview/body separation.

**Key Architectural Elements:**
- Strict parser with stack validation, duplicate detection, byte-offset correctness (LDMD Correction #1)
- Safe UTF-8 truncation preserving character boundaries
- Protocol commands as first-class `cq ldmd` subcommands (LDMD Correction #2)
- Preview/body separation in LDMD writer
- Separate Textual layer planned for R7 (LDMD Correction #3)

### Strict Parser (LDMD Correction #1)

The v1 parser was permissive. The v2 parser must enforce:

```python
# tools/cq/ldmd/format.py
from __future__ import annotations

import re
from dataclasses import dataclass


class LdmdParseError(Exception):
    """Raised on invalid LDMD structure."""


_BEGIN_RE = re.compile(
    r'^<!--LDMD:BEGIN\s+id="([^"]+)"'
    r'(?:\s+title="([^"]*)")?'
    r'(?:\s+level="(\d+)")?'
    r'(?:\s+parent="([^"]*)")?'
    r'(?:\s+tags="([^"]*)")?'
    r'\s*-->$'
)
_END_RE = re.compile(r'^<!--LDMD:END\s+id="([^"]+)"\s*-->$')


@dataclass(frozen=True)
class SectionMeta:
    """Metadata for a single LDMD section."""

    id: str
    start_offset: int
    end_offset: int
    depth: int
    collapsed: bool


@dataclass(frozen=True)
class LdmdIndex:
    """Index of LDMD sections with byte offsets."""

    sections: list[SectionMeta]
    total_bytes: int


def build_index(content: bytes) -> LdmdIndex:
    """Build section index in single forward pass over raw bytes.

    Validates:
    - BEGIN/END nesting via stack
    - Duplicate section IDs
    - Byte offsets from raw content (not decoded text)

    Uses attribute-based marker grammar:
    - BEGIN: <!--LDMD:BEGIN id="..." title="..." level="..." parent="..." tags="..."-->
    - END: <!--LDMD:END id="..."-->
    """
    open_stack: list[str] = []
    seen_ids: set[str] = set()
    sections: list[SectionMeta] = []
    open_sections: dict[str, dict[str, object]] = {}

    byte_offset = 0
    for raw_line in content.splitlines(keepends=True):
        line_byte_len = len(raw_line)
        # Normalize EOL markers for regex matching while preserving raw byte offsets.
        line = raw_line.rstrip(b"\r\n").decode("utf-8", errors="replace")

        begin_match = _BEGIN_RE.match(line)
        if begin_match:
            sid = begin_match.group(1)
            if sid in seen_ids:
                raise LdmdParseError(f"Duplicate section ID: {sid}")
            seen_ids.add(sid)
            open_stack.append(sid)
            open_sections[sid] = {
                "byte_start": byte_offset,
                "title": begin_match.group(2) or "",
                "level": int(begin_match.group(3)) if begin_match.group(3) else 0,
                "parent": begin_match.group(4) or "",
                "tags": begin_match.group(5) or "",
            }

        end_match = _END_RE.match(line)
        if end_match:
            sid = end_match.group(1)
            if not open_stack or open_stack[-1] != sid:
                expected = open_stack[-1] if open_stack else "none"
                raise LdmdParseError(
                    f"Mismatched END: expected '{expected}', got '{sid}'"
                )
            open_stack.pop()
            section_meta = open_sections.pop(sid)

            sections.append(
                SectionMeta(
                    id=sid,
                    start_offset=section_meta["byte_start"],  # type: ignore
                    end_offset=byte_offset + line_byte_len,
                    depth=len(open_stack),
                    collapsed=_is_collapsed(sid),
                )
            )

        byte_offset += line_byte_len

    if open_stack:
        raise LdmdParseError(f"Unclosed sections: {open_stack}")

    return LdmdIndex(
        sections=sections,
        total_bytes=len(content),
    )


def _is_collapsed(section_id: str) -> bool:
    """Determine if section should be collapsed by default."""
    # Map to SECTION_ORDER collapse policy
    from tools.cq.neighborhood.section_layout import (
        _UNCOLLAPSED_SECTIONS,
        _DYNAMIC_COLLAPSE_SECTIONS,
    )

    if section_id in _UNCOLLAPSED_SECTIONS:
        return False
    if section_id in _DYNAMIC_COLLAPSE_SECTIONS:
        return True  # dynamic collapse handled at render time
    return True  # default: collapsed
```

### Safe UTF-8 Truncation

```python
# tools/cq/ldmd/format.py (continued)
from __future__ import annotations


def _safe_utf8_truncate(data: bytes, limit: int) -> bytes:
    """Truncate bytes at limit, preserving UTF-8 boundaries.

    Uses strict decode validation after boundary adjustment.
    """
    if len(data) <= limit:
        return data

    # Back up from limit to find valid boundary
    candidate = data[:limit]
    while limit > 0:
        try:
            candidate.decode("utf-8", errors="strict")
            return candidate
        except UnicodeDecodeError:
            limit -= 1
            candidate = data[:limit]
    return b""


def get_slice(
    content: bytes,
    index: LdmdIndex,
    *,
    section_id: str,
    mode: str = "full",
    depth: int = 0,
    limit_bytes: int = 0,
) -> bytes:
    """Extract content from a section by ID.

    Args:
        content: Full LDMD document bytes
        index: Pre-built section index
        section_id: Target section ID
        mode: "full", "preview", or "tldr"
        depth: Include nested sections up to this depth (0 = no nesting)
        limit_bytes: Max bytes to return (0 = unlimited, uses safe UTF-8 truncation)

    Returns:
        Section content as bytes
    """
    # Find target section
    section = None
    for s in index.sections:
        if s.id == section_id:
            section = s
            break

    if section is None:
        raise LdmdParseError(f"Section not found: {section_id}")

    # Extract slice
    slice_data = content[section.start_offset:section.end_offset]

    # Apply depth filter (strip nested sections deeper than depth)
    # ... implementation depends on mode

    # Apply byte limit with safe UTF-8 truncation
    if limit_bytes > 0:
        slice_data = _safe_utf8_truncate(slice_data, limit_bytes)

    return slice_data
```

### LDMD Protocol Commands (LDMD Correction #2)

Add `cq ldmd` subcommands as first-class CQ commands, not just a format mode:

```python
# tools/cq/cli_app/commands/ldmd.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import cyclopts

from tools.cq.cli_app.context import CliContext, CliResult, CliTextResult
from tools.cq.ldmd.format import build_index, get_neighbors, get_slice, search_sections

ldmd_app = cyclopts.App(name="ldmd", help="LDMD progressive disclosure protocol")


def _text_result(
    ctx: CliContext,
    text: str,
    *,
    media_type: str = "text/plain",
    exit_code: int = 0,
) -> CliResult:
    """Build a CLI text result handled by unified output pipeline."""
    return CliResult(
        result=CliTextResult(text=text, media_type=media_type),
        context=ctx,
        exit_code=exit_code,
        filters=None,
    )


@ldmd_app.command
def index(
    path: Annotated[str, cyclopts.Parameter(help="Path to LDMD document")],
    *,
    ctx: Annotated[CliContext | None, cyclopts.Parameter(parse=False)] = None,
) -> CliResult:
    """Index an LDMD document and return section metadata."""
    if ctx is None:
        msg = "CliContext not injected"
        raise RuntimeError(msg)
    doc_path = Path(path)
    if not doc_path.exists():
        return _text_result(ctx, f"Document not found: {path}", exit_code=2)

    content = doc_path.read_bytes()
    try:
        idx = build_index(content)
    except Exception as exc:
        return _text_result(ctx, f"Failed to index document: {exc}", exit_code=2)

    sections_json = [
        {
            "id": s.id,
            "start_offset": s.start_offset,
            "end_offset": s.end_offset,
            "depth": s.depth,
            "collapsed": s.collapsed,
        }
        for s in idx.sections
    ]

    return _text_result(
        ctx,
        json.dumps(
            {
                "sections": sections_json,
                "total_bytes": idx.total_bytes,
            },
            indent=2,
        ),
        media_type="application/json",
    )


@ldmd_app.command
def get(
    path: Annotated[str, cyclopts.Parameter(help="Path to LDMD document")],
    *,
    id: str,
    mode: str = "full",
    depth: int = 0,
    limit_bytes: int = 0,
    ctx: Annotated[CliContext | None, cyclopts.Parameter(parse=False)] = None,
) -> CliResult:
    """Extract content from a section by ID."""
    if ctx is None:
        msg = "CliContext not injected"
        raise RuntimeError(msg)
    doc_path = Path(path)
    if not doc_path.exists():
        return _text_result(ctx, f"Document not found: {path}", exit_code=2)

    content = doc_path.read_bytes()
    try:
        idx = build_index(content)
        slice_data = get_slice(
            content, idx,
            section_id=id,
            mode=mode,
            depth=depth,
            limit_bytes=limit_bytes,
        )
    except Exception as exc:
        return _text_result(ctx, f"Failed to extract section: {exc}", exit_code=2)

    return _text_result(ctx, slice_data.decode("utf-8", errors="replace"))


@ldmd_app.command
def search(
    path: Annotated[str, cyclopts.Parameter(help="Path to LDMD document")],
    *,
    query: str,
    ctx: Annotated[CliContext | None, cyclopts.Parameter(parse=False)] = None,
) -> CliResult:
    """Search within LDMD sections."""
    if ctx is None:
        msg = "CliContext not injected"
        raise RuntimeError(msg)
    doc_path = Path(path)
    if not doc_path.exists():
        return _text_result(ctx, f"Document not found: {path}", exit_code=2)

    content = doc_path.read_bytes()
    try:
        idx = build_index(content)
        matches = search_sections(content, idx, query=query)
    except Exception as exc:
        return _text_result(ctx, f"Search failed: {exc}", exit_code=2)

    return _text_result(ctx, json.dumps(matches, indent=2), media_type="application/json")


@ldmd_app.command
def neighbors(
    path: Annotated[str, cyclopts.Parameter(help="Path to LDMD document")],
    *,
    id: str,
    ctx: Annotated[CliContext | None, cyclopts.Parameter(parse=False)] = None,
) -> CliResult:
    """Get neighboring sections for navigation."""
    if ctx is None:
        msg = "CliContext not injected"
        raise RuntimeError(msg)
    doc_path = Path(path)
    if not doc_path.exists():
        return _text_result(ctx, f"Document not found: {path}", exit_code=2)

    content = doc_path.read_bytes()
    try:
        idx = build_index(content)
        nav = get_neighbors(idx, section_id=id)
    except Exception as exc:
        return _text_result(ctx, f"Navigation failed: {exc}", exit_code=2)

    return _text_result(ctx, json.dumps(nav, indent=2), media_type="application/json")
```

### CliTextResult Rendering Path (LDMD Correction #2A)

`CliResult` remains the command contract, but protocol/tooling commands need a text payload path that still goes through centralized output handling.

```python
# tools/cq/cli_app/context.py (addition)
class CliTextResult(CqStruct, frozen=True):
    """Text payload for non-analysis protocol commands."""

    text: str
    media_type: str = "text/plain"
```

```python
# tools/cq/cli_app/result.py (non-CqResult handling update)
from tools.cq.cli_app.context import CliTextResult


def handle_result(cli_result: CliResult, filters: FilterConfig | None = None) -> int:
    # ...
    if not cli_result.is_cq_result:
        if isinstance(cli_result.result, CliTextResult):
            sys.stdout.write(f"{cli_result.result.text}\n")
        return cli_result.get_exit_code()
    # existing CqResult flow unchanged
```

### LDMD Writer with Preview/Body Separation

```python
# tools/cq/ldmd/writer.py
from __future__ import annotations

from pathlib import Path

from tools.cq.core.snb_schema import SemanticNeighborhoodBundleV1


def render_ldmd_document(
    bundle: SemanticNeighborhoodBundleV1,
    *,
    include_manifest: bool = True,
) -> str:
    """Render SNB bundle as LDMD-marked markdown.

    Preview/body separation:
    - TLDR block: slice summary + top-K items (preview)
    - Body block: remaining items + artifact refs
    - Manifest: section count, byte sizes, artifact pointers

    Structure:
    ```
    <!--LDMD:BEGIN id="target_tldr" title="Target" level="1"-->
    ## Target: my_function
    ... summary ...
    <!--LDMD:END id="target_tldr"-->

    <!--LDMD:BEGIN id="neighborhood_summary" title="Neighborhood Summary" level="1"-->
    ## Neighborhood Summary
    ... counts ...
    <!--LDMD:END id="neighborhood_summary"-->

    <!--LDMD:BEGIN id="parents" title="Parents" level="2"-->
    ### Parents (3)
    <!--LDMD:BEGIN id="parents_tldr" parent="parents" level="3"-->
    1. parent_func_1
    2. parent_func_2
    3. parent_func_3
    <!--LDMD:END id="parents_tldr"-->
    <!--LDMD:BEGIN id="parents_body" parent="parents" level="3"-->
    [Full details...]
    <!--LDMD:END id="parents_body"-->
    <!--LDMD:END id="parents"-->

    ... more sections ...

    <!--LDMD:BEGIN id="provenance" title="Provenance" level="1"-->
    ## Provenance
    - Sections: 12
    - Total bytes: 45678
    - Artifacts: slice_callers.json (3456 bytes)
    <!--LDMD:END id="provenance"-->
    ```
    """
    def _begin(
        section_id: str,
        *,
        title: str = "",
        level: int = 0,
        parent: str = "",
    ) -> str:
        attrs = [f'id="{section_id}"']
        if title:
            attrs.append(f'title="{title}"')
        if level > 0:
            attrs.append(f'level="{level}"')
        if parent:
            attrs.append(f'parent="{parent}"')
        return f"<!--LDMD:BEGIN {' '.join(attrs)}-->"

    def _end(section_id: str) -> str:
        return f'<!--LDMD:END id="{section_id}"-->'

    lines: list[str] = []
    subject_name = bundle.subject.name if bundle.subject else "<unknown>"
    subject_file = bundle.subject.file_path if bundle.subject else ""
    subject_span = bundle.subject.byte_span if bundle.subject else None

    # Target TL;DR (always first)
    lines.append(_begin("target_tldr", title="Target", level=1))
    lines.append(f"## Target: {subject_name}")
    if subject_file:
        lines.append(f"**File:** {subject_file}")
    if subject_span is not None:
        lines.append(f"**Byte span:** {subject_span[0]}..{subject_span[1]}")
    lines.append(_end("target_tldr"))
    lines.append("")

    # Neighborhood summary
    lines.append(_begin("neighborhood_summary", title="Neighborhood Summary", level=1))
    lines.append("## Neighborhood Summary")
    lines.append(f"- **Structural nodes:** {bundle.graph.node_count if bundle.graph else 0}")
    lines.append(f"- **Relationships:** {bundle.graph.edge_count if bundle.graph else 0}")
    lines.append(_end("neighborhood_summary"))
    lines.append("")

    # Slices (in deterministic order from view)
    for s in bundle.slices:
        lines.append(_begin(s.kind, title=s.kind.replace("_", " ").title(), level=2))
        lines.append(f"### {s.kind.replace('_', ' ').title()} ({s.total})")

        # TLDR: preview (top-K)
        if s.preview:
            lines.append(_begin(f"{s.kind}_tldr", parent=s.kind, level=3))
            for i, node in enumerate(s.preview, 1):
                lines.append(f"{i}. {node.name} ({node.kind})")
            lines.append(_end(f"{s.kind}_tldr"))

        # Body: full details or artifact ref
        lines.append(_begin(f"{s.kind}_body", parent=s.kind, level=3))
        if s.total > len(s.preview):
            # Point to artifact
            lines.append(f"*Full data: see artifact `slice_{s.kind}.json` ({s.total} items)*")
        else:
            # Inline full details (uses s.preview, not s.nodes)
            for node in s.preview:
                lines.append(f"- **{node.name}** ({node.kind})")
                if node.file_path:
                    lines.append(f"  - {node.file_path}")
        lines.append(_end(f"{s.kind}_body"))

        lines.append(_end(s.kind))
        lines.append("")

    # Diagnostics
    if bundle.diagnostics:
        lines.append(_begin("diagnostics", title="Diagnostics", level=1))
        lines.append("## Diagnostics")
        for diag in bundle.diagnostics:
            lines.append(f"- **{diag.stage}:** {diag.message}")
        lines.append(_end("diagnostics"))
        lines.append("")

    # Provenance (always last)
    if include_manifest:
        lines.append(_begin("provenance", title="Provenance", level=1))
        lines.append("## Provenance")
        lines.append(f"- **Sections:** {len(bundle.slices)}")
        if bundle.meta and bundle.meta.created_at_ms is not None:
            lines.append(f"- **Created at (ms):** {bundle.meta.created_at_ms}")
        if bundle.artifacts:
            lines.append("- **Artifacts:**")
            for art in bundle.artifacts:
                if art.storage_path:
                    lines.append(f"  - `{Path(art.storage_path).name}` ({art.byte_size} bytes)")
                else:
                    lines.append(f"  - `{art.artifact_id}` ({art.byte_size} bytes)")
        lines.append(_end("provenance"))

    return "\n".join(lines)
```

### Target Files for R6

| Action | File |
|--------|------|
| **Create** | `tools/cq/ldmd/__init__.py` |
| **Create** | `tools/cq/ldmd/format.py` |
| **Create** | `tools/cq/ldmd/writer.py` |
| **Create** | `tools/cq/cli_app/commands/ldmd.py` |
| **Create** | `tests/unit/cq/ldmd/test_format.py` |
| **Create** | `tests/unit/cq/ldmd/test_writer.py` |
| **Edit** | `tools/cq/cli_app/context.py` (add `CliTextResult`) |
| **Edit** | `tools/cq/cli_app/result.py` (render `CliTextResult` in non-`CqResult` path) |
| **Edit** | `tools/cq/cli_app/app.py` (register `ldmd_app`) |

### Implementation Checklist for R6

**Strict Parser:**
- [ ] `LdmdParseError` exception for invalid structure
- [ ] Stack validation: BEGIN/END must match in stack order
- [ ] Duplicate ID detection: raise on repeated section IDs
- [ ] Byte offsets computed from raw bytes (not decoded text length)
- [ ] CRLF normalized for marker matching while preserving byte offsets
- [ ] No-final-newline files keep exact end offset (no implicit `+1`)
- [ ] `_safe_utf8_truncate()` preserves character boundaries
- [ ] `build_index()` returns `LdmdIndex` with section metadata

**Protocol Commands:**
- [ ] `cq ldmd index` command returns section metadata as CliResult
- [ ] `cq ldmd get` command with --id, --mode, --depth, --limit-bytes
- [ ] `cq ldmd search` command with --query scoped to sections
- [ ] `cq ldmd neighbors` command returns prev/next section IDs
- [ ] All LDMD commands return `CliResult` (not raw CqResult)
- [ ] Correct CLI imports: `from tools.cq.cli_app.context import CliContext, CliResult, CliTextResult`
- [ ] Commands use ctx injection pattern: `ctx: Annotated[CliContext | None, cyclopts.Parameter(parse=False)] = None`
- [ ] Commands check `if ctx is None: raise RuntimeError("CliContext not injected")`
- [ ] Commands return `CliResult(result=CliTextResult(...), context=ctx, filters=None)`
- [ ] `handle_result()` renders `CliTextResult` payloads before returning exit code
- [ ] Commands registered in `tools/cq/cli_app/app.py`

**LDMD Writer:**
- [ ] LDMD writer produces preview/body separation in TLDR markers
- [ ] Manifest includes section count, byte sizes, artifact pointers
- [ ] Deterministic section ordering (uses bundle.view or bundle.slices)
- [ ] Preview block (`{kind}_tldr`) contains top-K items
- [ ] Body block (`{kind}_body`) contains full data or artifact ref
- [ ] Artifact references include file name and byte size
- [ ] NOTE: Rich markdown is STATIC — interactive collapse requires Textual
- [ ] LDMD markers use attribute-based IDs per Rich reference
- [ ] Writer emits parser-compatible markers (`<!--LDMD:BEGIN id="..."-->`)
- [ ] Optional TLDR markers per section
- [ ] Textual viewer is a separate UX layer (not coupled to LDMD protocol)

**Testing:**
- [ ] Test: mismatched BEGIN/END raises LdmdParseError
- [ ] Test: duplicate IDs raise LdmdParseError
- [ ] Test: unclosed sections raise LdmdParseError
- [ ] Test: `_safe_utf8_truncate` preserves multi-byte chars
- [ ] Test: byte offset correctness (raw bytes, not text len)
- [ ] Test: byte offsets correct for CRLF inputs
- [ ] Test: byte offsets correct when file has no final newline
- [ ] Test: `cq ldmd index` returns valid section metadata
- [ ] Test: `cq ldmd get` extracts correct section content
- [ ] Test: `cq ldmd get --limit-bytes` truncates safely
- [ ] Test: Rich `Markdown(...)` renders LDMD doc without visible markers
- [ ] Golden snapshot: neighborhood LDMD output
- [ ] Golden roundtrip: writer output parses via strict parser with no marker mismatches

**Acceptance Criteria:**
- LDMD parser enforces strict nesting and duplicate detection
- Byte offsets correct from raw bytes
- Safe UTF-8 truncation preserves character boundaries
- Protocol commands return `CliResult` with `CliTextResult` payloads rendered by unified CLI pipeline
- LDMD writer produces preview/body separation
- Writer output round-trips with strict parser (attribute markers only)
- Manifest accurate and deterministic
- All tests pass
- `uv run pyrefly check` passes

### Textual Integration Contract (Separate UX Layer)
- LDMD is the static protocol/interchange layer (authoritative source of section boundaries and byte offsets)
- Textual is an optional interactive layer that consumes `cq ldmd index/get/search/neighbors`, not a replacement parser
- Interactive expand/collapse state is maintained in Textual UI state, never encoded as ad-hoc markdown markers
- Rich markdown rendering remains static; Textual drives navigation and reveal behavior using LDMD commands

---

## R7: CLI + Run Integration (CliResult Pattern)

This is the corrected version of original S7, fixing ALL blocking issues:

### Blocking Fix #1: CliResult Pattern (not CqResult)
Command handler MUST return `CliResult`, not `CqResult`:
```python
# tools/cq/cli_app/commands/neighborhood.py
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import cyclopts

from tools.cq.cli_app.context import CliContext, CliResult

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain

neighborhood_app = cyclopts.App(name="neighborhood", help="Semantic neighborhood analysis")
nb_app = cyclopts.App(name="nb", help="Alias for neighborhood")


@neighborhood_app.default
@nb_app.default
def cmd_neighborhood(
    target: Annotated[str, cyclopts.Parameter(help="Target symbol or file:line")],
    *,
    opts: Annotated[NeighborhoodParams, cyclopts.Parameter(name="*")] | None = None,
    ctx: Annotated[CliContext | None, cyclopts.Parameter(parse=False)] = None,
) -> CliResult:
    """Analyze semantic neighborhood of a target symbol."""
    if ctx is None:
        msg = "CliContext not injected"
        raise RuntimeError(msg)
    params = opts or NeighborhoodParams()
    repo_root = Path(ctx.root)

    # Resolve target
    anchor, language = _resolve_target(target, repo_root, params.in_dir, params.lang)

    # Build scan snapshot (CORRECT API: sg_scan → from_records)
    from tools.cq.query.sg_parser import sg_scan
    from tools.cq.neighborhood.scan_snapshot import ScanSnapshot
    records = sg_scan(paths=[repo_root], lang=language, root=repo_root)
    snapshot = ScanSnapshot.from_records(records)

    # Build bundle
    request = BundleBuildRequest(
        anchor=anchor,
        root=repo_root,
        snapshot=snapshot,
        language=language,
        symbol_hint=target,
        top_k=params.top_k,
        enable_lsp=not params.no_lsp,
        artifact_dir=Path(params.artifact_dir) if params.artifact_dir else None,
    )
    bundle = build_neighborhood_bundle(request)

    # Convert to CqResult via section layout
    view = materialize_section_layout(bundle)
    result = mk_result(mk_runmeta(
        macro="neighborhood",
        argv=["neighborhood", target],
        root=str(repo_root),
        started_ms=ms(),
    ))
    result.key_findings.extend(view.key_findings)
    result.sections.extend(view.sections)

    # Return CliResult — NOT CqResult
    return CliResult(result=result, context=ctx, filters=None)
```

### Blocking Fix #2: Correct Scan Context Construction
Uses `sg_scan()` → `ScanSnapshot.from_records()` (correct API chain).
NOT `_build_scan_context(repo_root, language)` which doesn't exist.

The correct signature is:
```python
from tools.cq.query.sg_parser import sg_scan
records = sg_scan(paths=[root], lang=language, root=root)
```

### Blocking Fix #3: Correct Smart Search Reference
```python
def _resolve_target(
    target: str,
    root: Path,
    in_dir: str | None,
    lang: str,
) -> tuple[Anchor, str]:
    """Resolve target string to (Anchor, language)."""
    from tools.cq.neighborhood.anchor import Anchor

    # Try file:line:col format
    parts = target.rsplit(":", 2)
    if len(parts) >= 2:
        try:
            file_path = parts[0]
            line = int(parts[1])
            col = int(parts[2]) if len(parts) == 3 else 0
            language = _detect_language(file_path, lang)
            return Anchor(file=file_path, line=line, col=col), language
        except ValueError:
            pass

    # Fall back to smart_search (NOT smart_search_impl — it doesn't exist)
    from tools.cq.search.smart_search import smart_search
    search_result = smart_search(root, target, lang_scope=lang)
    # Parse CqResult for first definition match
    ...
```

### Blocking Fix #4: No command-local format shadow
Do NOT add a `format: str` parameter to the command. Use the global `OutputFormat` from context which now includes `ldmd`.

### Blocking Fix #5: Full Run Step Integration
Add complete tagged step struct + union inclusion + step tag + loader + runner:

```python
# In tools/cq/run/spec.py — additions:

class NeighborhoodStep(RunStepBase, tag="neighborhood", frozen=True):
    """Run step for neighborhood analysis."""
    target: str
    lang: str = "auto"
    top_k: int = 5
    no_lsp: bool = False

# Update RunStep union:
RunStep = (
    QStep
    | SearchStep
    | CallsStep
    | ImpactStep
    | ImportsStep
    | ExceptionsStep
    | SigImpactStep
    | SideEffectsStep
    | ScopesStep
    | BytecodeSurfaceStep
    | NeighborhoodStep  # NEW
)

# Update RUN_STEP_TYPES:
RUN_STEP_TYPES: tuple[type[RunStep], ...] = (
    QStep, SearchStep, CallsStep, ImpactStep, ImportsStep,
    ExceptionsStep, SigImpactStep, SideEffectsStep, ScopesStep,
    BytecodeSurfaceStep, NeighborhoodStep,
)

# Update _STEP_TAGS:
_STEP_TAGS: dict[type[RunStep], str] = {
    # ... existing entries ...
    NeighborhoodStep: "neighborhood",
}
```

```python
# In tools/cq/run/runner.py — additions:

def _execute_non_q_step(step: RunStep, plan: RunPlan, ctx: CliContext) -> CqResult:
    # ... existing dispatch ...
    if isinstance(step, NeighborhoodStep):
        return _execute_neighborhood_step(step, plan, ctx)
    # ...

def _execute_neighborhood_step(
    step: NeighborhoodStep, plan: RunPlan, ctx: CliContext
) -> CqResult:
    """Execute neighborhood step within run pipeline."""
    from tools.cq.query.sg_parser import sg_scan
    from tools.cq.neighborhood.scan_snapshot import ScanSnapshot
    from tools.cq.neighborhood.bundle_builder import (
        BundleBuildRequest, build_neighborhood_bundle,
    )
    from tools.cq.neighborhood.section_layout import materialize_section_layout

    root = Path(ctx.root)
    anchor, language = _resolve_target(step.target, root, plan.in_dir, step.lang)
    records = sg_scan(paths=[root], lang=language, root=root)
    snapshot = ScanSnapshot.from_records(records)

    request = BundleBuildRequest(
        anchor=anchor,
        root=root,
        snapshot=snapshot,
        language=language,
        symbol_hint=step.target,
        top_k=step.top_k,
        enable_lsp=not step.no_lsp,
    )
    bundle = build_neighborhood_bundle(request)
    view = materialize_section_layout(bundle)

    result = mk_result(mk_runmeta(macro="neighborhood", argv=["neighborhood", step.target], root=str(root)))
    result.key_findings.extend(view.key_findings)
    result.sections.extend(view.sections)
    return result
```

### Target Files for R7
| Action | File |
|--------|------|
| **Create** | `tools/cq/cli_app/commands/neighborhood.py` |
| **Create** | `tools/cq/macros/neighborhood.py` |
| **Create** | `tests/unit/cq/cli/test_neighborhood_command.py` |
| **Edit** | `tools/cq/cli_app/app.py` (register neighborhood_app + nb_app) |
| **Edit** | `tools/cq/run/spec.py` (NeighborhoodStep + union + tags) |
| **Edit** | `tools/cq/run/runner.py` (_execute_neighborhood_step dispatch) |
| **Edit** | `tools/cq/cli_app/types.py` (NeighborhoodParams if needed) |

### Implementation Checklist for R7
- [ ] Command returns `CliResult(result=..., context=ctx, filters=...)` NOT `CqResult`
- [ ] Scan context built via `sg_scan()` → `ScanSnapshot.from_records()` (correct API)
- [ ] Target resolution uses `smart_search()` NOT `smart_search_impl`
- [ ] No command-local `format: str` parameter — uses global `OutputFormat`
- [ ] `NeighborhoodStep` tagged struct in `run/spec.py`
- [ ] `NeighborhoodStep` added to `RunStep` union type
- [ ] `NeighborhoodStep` added to `RUN_STEP_TYPES` tuple
- [ ] `NeighborhoodStep` added to `_STEP_TAGS` dict
- [ ] `_execute_neighborhood_step()` in `runner.py`
- [ ] Loader/decoder compatibility for `NeighborhoodStep`
- [ ] Summary metadata coverage for neighborhood steps
- [ ] Register `neighborhood_app` + `nb_app` in `app.py`
- [ ] Test: `cq neighborhood build_graph` returns valid CliResult
- [ ] Test: `cq nb src/semantics/pipeline.py:42` resolves file:line
- [ ] Test: `--no-lsp` produces structural-only output
- [ ] Test: `--format ldmd` routes through global OutputFormat dispatch
- [ ] Test: `cq run --steps '[{"type":"neighborhood","target":"foo"}]'` works
- [ ] Test: NeighborhoodStep round-trips through msgspec encode/decode

---

## R8: Advanced Evidence Planes (On-Demand)

NEW scope from Scope Expansion E, F, G. These are advanced, on-demand planes that extend CQ's semantic reach. All behind capability gates.

### E) Semantic Overlay Plane
```python
# tools/cq/search/semantic_overlays.py
from __future__ import annotations

from tools.cq.core.structs import CqStruct


class SemanticTokenSpanV1(CqStruct, frozen=True):
    """Normalized semantic token with resolved type/modifier names."""
    line: int
    start_char: int
    length: int
    token_type: str  # resolved from legend
    modifiers: tuple[str, ...] = ()


class SemanticTokenBundleV1(CqStruct, frozen=True):
    """Atomic semantic token bundle — legend + encoding + payload.

    Must always store:
    - negotiated position encoding
    - server legend (token types + modifiers arrays)
    - raw data stream (or delta edits)
    - decoded rows (SemanticTokenSpanV1 with resolved names)
    - cache keys (resultId, previous ID)

    References:
    - rust_lsp.md: semantic tokens require legend + encoding to decode
    - pyrefly_lsp_data.md: same requirement
    """
    position_encoding: str = "utf-16"
    legend_token_types: tuple[str, ...] = ()
    legend_token_modifiers: tuple[str, ...] = ()
    result_id: str | None = None
    previous_result_id: str | None = None
    tokens: tuple[SemanticTokenSpanV1, ...] = ()
    raw_data: tuple[int, ...] | None = None  # original int array for recomputation


class InlayHintV1(CqStruct, frozen=True):
    """Normalized inlay hint."""
    line: int
    character: int
    label: str
    kind: str | None = None  # "type" | "parameter" | None
    padding_left: bool = False
    padding_right: bool = False


def fetch_semantic_tokens_range(
    session: object,  # _PyreflyLspSession or _RustLspSession
    uri: str,
    start_line: int,
    end_line: int,
) -> tuple[SemanticTokenSpanV1, ...] | None:
    """Fetch semantic tokens for a range. Fail-open. Capability-gated."""
    ...


def fetch_inlay_hints_range(
    session: object,
    uri: str,
    start_line: int,
    end_line: int,
) -> tuple[InlayHintV1, ...] | None:
    """Fetch inlay hints for a range. Fail-open. Capability-gated."""
    ...
```

### F) Edit/Refactor Action Plane
```python
# tools/cq/search/refactor_actions.py
from __future__ import annotations

from tools.cq.core.structs import CqStruct


class PrepareRenameResultV1(CqStruct, frozen=True):
    """Result of textDocument/prepareRename."""
    range_start_line: int = 0
    range_start_col: int = 0
    range_end_line: int = 0
    range_end_col: int = 0
    placeholder: str = ""
    can_rename: bool = True


class DiagnosticItemV1(CqStruct, frozen=True):
    """Normalized diagnostic with action-bridge fidelity fields."""
    uri: str
    message: str
    severity: int = 0
    code: str | None = None
    code_description_href: str | None = None
    tags: tuple[int, ...] = ()
    version: int | None = None
    related_information: tuple[dict[str, object], ...] = ()
    data: dict[str, object] | None = None


class CodeActionV1(CqStruct, frozen=True):
    """Normalized code action."""
    title: str
    kind: str | None = None  # e.g. "quickfix", "refactor.extract"
    is_preferred: bool = False
    diagnostics: tuple[dict[str, object], ...] = ()
    disabled_reason: str | None = None
    is_resolvable: bool = False
    has_edit: bool = False
    has_command: bool = False
    command_id: str | None = None
    has_snippet_text_edits: bool = False


class WorkspaceEditV1(CqStruct, frozen=True):
    """Normalized workspace edit."""
    document_changes: tuple[DocumentChangeV1, ...] = ()
    change_count: int = 0


class DocumentChangeV1(CqStruct, frozen=True):
    """Single document change within a workspace edit."""
    uri: str
    kind: str = "edit"  # "edit" | "create" | "rename" | "delete"
    edit_count: int = 0


def pull_document_diagnostics(
    session: object,
    uri: str,
) -> tuple[DiagnosticItemV1, ...] | None:
    """Fetch diagnostics via textDocument/diagnostic when supported."""
    ...


def pull_workspace_diagnostics(session: object) -> tuple[DiagnosticItemV1, ...] | None:
    """Fetch diagnostics via workspace/diagnostic when supported."""
    ...


def resolve_code_action(session: object, action: CodeActionV1) -> CodeActionV1 | None:
    """Resolve deferred code action payloads (codeAction/resolve)."""
    ...


def execute_code_action_command(session: object, action: CodeActionV1) -> bool:
    """Execute bound command via workspace/executeCommand."""
    ...
```

### G) Rust-Analyzer Deep Extension Plane
```python
# tools/cq/search/rust_extensions.py
from __future__ import annotations

from tools.cq.core.structs import CqStruct


class RustMacroExpansionV1(CqStruct, frozen=True):
    """Result of rust-analyzer/expandMacro."""
    name: str
    expansion: str  # expanded source text
    expansion_byte_len: int = 0


class RustRunnableV1(CqStruct, frozen=True):
    """Normalized runnable from rust-analyzer/runnables."""
    label: str
    kind: str  # "cargo" | "test" | "bench"
    args: tuple[str, ...] = ()
    location_uri: str | None = None
    location_line: int = 0


def expand_macro(
    session: object,
    uri: str,
    line: int,
    col: int,
) -> RustMacroExpansionV1 | None:
    """Expand macro at position. Fail-open. RA-specific."""
    ...


def get_runnables(
    session: object,
    uri: str,
) -> tuple[RustRunnableV1, ...]:
    """Get runnables for a file. Fail-open. RA-specific."""
    ...
```

### Target Files for R8
| Action | File |
|--------|------|
| **Create** | `tools/cq/search/semantic_overlays.py` |
| **Create** | `tools/cq/search/refactor_actions.py` |
| **Create** | `tools/cq/search/diagnostics_pull.py` |
| **Create** | `tools/cq/search/rust_extensions.py` |
| **Create** | `tests/unit/cq/search/test_advanced_planes.py` |
| **Edit** | `tools/cq/search/rust_lsp.py` (add RA extension methods) |
| **Edit** | `tools/cq/search/pyrefly_lsp.py` (add semantic token/inlay hint + structural discovery methods) |

### Implementation Checklist for R8
- [ ] `SemanticTokenSpanV1` with resolved type/modifier names
- [ ] `SemanticTokenBundleV1` atomic bundle (legend + encoding + payload)
- [ ] `InlayHintV1` with label, kind, padding
- [ ] `fetch_semantic_tokens_range()` with range-first strategy
- [ ] `fetch_inlay_hints_range()` with resolve-capability awareness
- [ ] Top-K anchor selection (limit full enrichment)
- [ ] Range-scoped inlay/semantic token pulls first (cheaper than full)
- [ ] Promote to full/delta only when needed
- [ ] Content-hash + config-fingerprint cache keys
- [ ] `PrepareRenameResultV1` with range and placeholder
- [ ] Diagnostics pull plane: `textDocument/diagnostic` + `workspace/diagnostic`
- [ ] `DiagnosticItemV1` includes related info, tags, code description, version, data
- [ ] `CodeActionV1` includes disabled reason, resolve status, command ID, snippet-edit indicator
- [ ] `WorkspaceEditV1` with document changes
- [ ] `resolve_code_action()` for `codeAction/resolve`
- [ ] `execute_code_action_command()` for `workspace/executeCommand`
- [ ] RA `SnippetTextEdit` support awareness in workspace edits
- [ ] `RustMacroExpansionV1` from `rust-analyzer/expandMacro`
- [ ] `RustRunnableV1` from `rust-analyzer/runnables`
- [ ] Pyrefly structural discovery plane includes document symbols + workspace symbols (+ resolve when available)
- [ ] All behind capability gates (no call if cap not negotiated)
- [ ] Canonical fail-open policy: return `None` only for transport/session-fatal boundaries; otherwise return partial typed payload + `DegradeEventV1`
- [ ] Test: semantic token roundtrip encode/decode
- [ ] Test: code action normalization from raw LSP response
- [ ] Test: diagnostics pull normalization (document + workspace)
- [ ] Test: semantic token invariants (non-overlap/multiline policy and legend decode assertions)
- [ ] Test: macro expansion with mock RA response

---

## Dependency Graph

```
R0 (Correctness Patch)
 └──► R1 (Contract Foundation)
       ├──► R3 (Pyrefly Expansion) ──────────────► R5 (Assembler + Layout) ──► R7 (CLI + Run)
       └──► R4 (Structural Collector) ──────────► R5           ▲
                                                                │
R2 (Rust LSP Session) ──────────────────────────► R5            │
                                                                │
R6 (LDMD Services) ────────────────────────────────────────────┘

R8 (Advanced Planes) ← depends on R2, R3 (on-demand, can be deferred)
```

- **R0** must be first (correctness alignment)
- **R1** and **R2** can run in parallel after R0
- **R6** is independent — can run in parallel with R1/R2
- **R3** depends on R1
- **R4** depends on R1
- **R5** depends on R1 + R2 + R3 + R4
- **R7** depends on R5 + R6
- **R8** depends on R2 + R3 (can be deferred)

## Recommended Implementation Order

1. **Week 1**: R0 (Correctness Patch) — must be first
2. **Week 2**: R1 (Contracts) + R2 (Rust LSP) + R6 (LDMD) — all independent after R0
3. **Week 3**: R3 (Pyrefly Expansion) + R4 (Structural Collector) — both depend on R1
4. **Week 4**: R5 (Assembler + Layout)
5. **Week 5**: R7 (CLI + Run Integration)
6. **Ongoing**: R8 (Advanced Planes) — on-demand, can be deferred

## Cross-Cutting Concerns

### Fail-Open Discipline
Every LSP integration point must follow existing `pyrefly_lsp.py` pattern:
- `try/except Exception: return None` only for transport/session-fatal outer boundary
- No exceptions escape to search/query/run pipelines
- Non-fatal capability/health degradation returns partial typed payload + `DegradeEventV1`
- Missing LSP data still yields structural-only results (never empty/error)
- Infeasible capabilities → typed `DegradeEventV1` (not string)

### msgspec Contract Stability
All new `CqStruct` types must follow schema evolution rules from `core/schema.py`:
- New fields must have defaults
- For array-like structs, append fields only; never reorder
- Do not change existing field types; add new fields instead

### Position Encoding
Both LSP clients must negotiate and persist position encoding:
- Request `utf-8` preference, accept `utf-16` fallback
- Store encoding in `LspSessionEnvV1.position_encoding`
- Convert to byte spans when joining with CQ's byte-span-canonical model

### Typed Contract Normalization Pattern
All new LSP payload families MUST use the `pyrefly_contracts.py` pattern:
- `CqStruct` frozen types for each normalized payload section
- `coerce_*_payload()` entry point normalizing loose dicts to typed structs
- `*_payload_to_dict()` for conversion back to enrichment pipeline dicts
- `from_mapping()` class methods for each subsection
- No ad-hoc dict plumbing in assemblers

### Snippet Compile Gate
- Run automated snippet compile/type-check for all R5-R7 code blocks against canonical R1 contracts
- Reject plan updates that introduce non-canonical field references (`symbol`, `path`, `size_bytes`, etc.)

### Capability-Gated Execution
- Before dispatching LSP requests, check negotiated capabilities
- Mark infeasible slices with typed `DegradeEventV1` events
- Never fail on missing capabilities — degrade gracefully
- Surface capability status in provenance section

### Preview/Artifact Split
- Every heavy LSP surface uses:
  - Small preview in section details (top-K items)
  - Full payload in artifact file
  - Deterministic artifact IDs and byte sizes in provenance
- Artifact directory is optional; inline-only is valid

### Capability Snapshot Normalization
- `LspCapabilitySnapshotV1` shared for Python + Rust
- Distinguishes server-capability vs client-capability checks
- Experimental caps tracked separately
- Position encoding negotiation persisted in snapshot
- Retain rich provider payloads (`semanticTokensProvider`, `codeActionProvider`, `workspaceSymbolProvider`) for precise advanced-plane gating

### Health-Aware Progressive Execution
- Tier A: Lightweight grounding queries (session alive)
- Tier B: Standard LSP queries when `workspace_health in {"ok", "warning"}`
- Tier C: Heavy hierarchy queries when `workspace_health == "ok"` and `quiescent=True`
- Non-quiescent Rust sessions return partial payload + degrade events (not hard None)

### Request Budget Model
- Top-K anchor selection (limit full enrichment)
- Range-scoped inlay/semantic token pulls first (cheaper than full)
- Promote to full/delta only when needed
- Content-hash + config-fingerprint cache keys

## Expanded Test Strategy

### Contract Tests (`tests/msgspec_contract/`)
- msgspec roundtrip tests for every new struct
- Strict union normalization tests (`Location | Location[] | LocationLink[] | null`)
- Kind-registry validation tests (known kind → type, unknown → None)
- `DegradeEventV1` encode/decode roundtrip

### Session/Protocol Tests (`tests/unit/cq/search/`)
- Mock JSON-RPC transcript tests for Pyrefly and Rust sessions
- Capability negotiation and position-encoding tests
- Health/quiescence gating behavior tests
- Diagnostics capture normalization tests

### Assembler Tests (`tests/unit/cq/neighborhood/`)
- Deterministic ordering tests (section order matches SECTION_ORDER)
- Structural-only fallback tests (no LSP → valid bundle)
- Mixed-language merge tests (Python + Rust in same workspace)
- Degrade-event aggregation tests
- Capability gate partition tests

### LDMD Tests (`tests/unit/cq/ldmd/`)
- Parser validation error cases (mismatched markers, duplicate IDs)
- Byte-offset correctness tests (raw bytes, not text len)
- Truncation/cursor continuation tests
- Safe UTF-8 truncation boundary tests (2/3/4-byte code points at each position)
- Marker invisibility rendering tests with Rich markdown
- Semantic token legend + encoding decode tests
- Capability gate server vs client distinction tests

### CLI/Run Tests (`tests/unit/cq/cli/`)
- Command-level output flow via `CliResult` (not CqResult)
- Output format dispatch (`md`, `json`, `ldmd`, etc.)
- Run-step integration with neighborhood steps
- NeighborhoodStep encode/decode roundtrip

### Golden Tests (`tests/cli_golden/`)
- Add golden snapshots for neighborhood markdown output
- Add golden snapshots for neighborhood LDMD output
- Add golden snapshots for LDMD index/get/search output

## Review Integration Summary

This v2 plan addresses all items from the review document:

### Blocking Corrections v2 (all 10 fixed)
1. ✅ B1: CLI imports from `tools.cq.cli_app.context` (R7)
2. ✅ B2: sg_scan from `tools.cq.query.sg_parser`, SgRecord from `tools.cq.astgrep.sgpy_scanner` (R4, R7)
3. ✅ B3: Capability gates split server_caps/client_caps/experimental_caps (R3)
4. ✅ B4: Health tri-state with staged gating, not hard None (R2)
5. ✅ B5: Pyrefly adapter uses actual contract fields (R3)
6. ✅ B6: Canonical SNB schema frozen in R1, propagated to all downstream (R1, R5, R6, R7)
7. ✅ B7: LDMD parser single-pass, attribute-based markers, byte-accurate (R6)
8. ✅ B8: UTF-8 truncation with strict decode validation (R6)
9. ✅ B9: NeighborhoodStep extends RunStepBase with tag="neighborhood" (R7)
10. ✅ B10: CqResult imported from `tools.cq.core.schema` (R0)

### Optimizations v2 (all 4 addressed)
1. ✅ O1: LspCapabilitySnapshotV1 shared for Python + Rust (R2/R3)
2. ✅ O2: Health-aware progressive execution policy (R2)
3. ✅ O3: Unified diagnostics-to-actions bridge (R2/R3)
4. ✅ O4: Semantic overlay persistence atomic (R8)

### Scope Expansions v2 (all 4 addressed)
1. ✅ E1: Pyrefly contract planes expanded (R3)
2. ✅ E2: Rust environment/configuration invalidation (R2)
3. ✅ E3: Top-K/range-first LSP query strategy (R8)
4. ✅ E4: LDMD aligned with Rich/Textual realities (R6)

### Definition of Done Additions
- Every snippet compiles against current module paths
- Capability gates distinguish server-capability vs client-capability checks
- Non-quiescent Rust sessions return partial payload + degrade events (not hard None)
- Semantic token artifacts always include legend + position encoding + decode provenance
- Diagnostic `data` passthrough verified into codeAction request contexts
- LDMD parser validated against attribute-based markers and byte-accurate offsets

---

## Document Metadata

- **Version**: 1.0
- **Date**: 2026-02-11
- **Status**: Ready for Implementation
- **Total Releases**: R0-R8 (9 releases)
- **Estimated Duration**: 5-6 weeks
- **Dependencies**: Review document integrated
- **Contract Stability**: All new structs follow msgspec evolution rules
