# 08 — Neighborhood Subsystem Architecture

**Version**: 0.5.0
**Status**: Phase 3 Documentation
**Scope**: Targeted semantic neighborhood analysis with tree-sitter structural collection and progressive disclosure

This document describes CQ's neighborhood subsystem (`tools/cq/neighborhood/`, ~2,020 LOC), which assembles rich contextual bundles around code anchors. It combines tree-sitter-based structural relationships and static semantic enrichment into versioned SNB artifacts with deterministic section layouts.

**Cross-References**:
- Doc 06: FrontDoorInsightV1 integration, SNB schema definitions
- Doc 07: Tree-sitter query engine internals used by structural collector
- Doc 10: Artifact caching and storage

**Target Audience**: Advanced LLM programmers proposing architectural improvements.

---

## Executive Summary

The neighborhood subsystem provides targeted semantic neighborhood analysis around code anchors, emitting structured `SemanticNeighborhoodBundleV1` (SNB) artifacts with 3-phase assembly:

1. **Structural Collection** — Tree-sitter-based AST relationships (parents/children/siblings/callers/callees)
2. **Merge & Deduplication** — Kind-keyed slice merging with top-k enforcement
3. **Artifact Split** — Progressive disclosure via externalized heavy slices

**Key Characteristics**:
- Anchor-first target resolution with symbol fallback (file:line:col → symbol)
- Deterministic 17-slot section layout with dynamic collapse rules
- Multi-language support (Python/Rust via tree-sitter)
- Typed degradation events with stage/category/severity classification
- Static semantic enrichment (tree-sitter structural planes only)

**Removed**: LSP enrichment phase has been removed. The subsystem now relies entirely on tree-sitter structural collection with optional static semantic enrichment flags.

---

## Module Map

| Module | LOC | Responsibility |
|--------|-----|----------------|
| `tree_sitter_collector.py` | 668 | Tree-sitter-first structural neighborhood collection |
| `section_layout.py` | 473 | Deterministic 17-slot section ordering, collapse rules |
| `bundle_builder.py` | 300 | 3-phase bundle assembly orchestration |
| `target_resolution.py` | 186 | Anchor/symbol target parsing and resolution with degradation |
| `snb_renderer.py` | 160 | SNB → CqResult markdown rendering |
| `tree_sitter_neighborhood_query_engine.py` | 116 | Query-driven caller/callee extraction using tree-sitter runtime |
| `contracts.py` | 100 | Tree-sitter collector contracts and capability gating |
| `__init__.py` | 17 | Public API exports |

**Total**: ~2,020 LOC

**Integration Points**:
- `cli_app/commands/neighborhood.py` (145 LOC) — CLI command handler
- `run/spec.py` (NeighborhoodStep) — Run-plan integration
- `core/snb_schema.py` (322 LOC) — Canonical SNB schema definitions
- `core/snb_registry.py` (101 LOC) — Typed kind registry for runtime validation

---

## SNB Data Model

**Authority**: `tools/cq/core/snb_schema.py` (322 LOC)

All SNB structures use frozen `msgspec.Struct` for deterministic serialization. This module is the canonical schema authority. See **Doc 06: Data Models** for full schema reference.

### SemanticNeighborhoodBundleV1

**Definition**: `snb_schema.py:266-309`

```python
class SemanticNeighborhoodBundleV1(CqStruct, frozen=True):
    """Complete semantic neighborhood bundle — CANONICAL definition.

    Combines structural and optional static enrichment planes into
    a single versioned, deterministic artifact.
    """

    bundle_id: str                                      # Deterministic SHA256 hash
    subject: SemanticNodeRefV1 | None = None            # Resolved target node
    subject_label: str = ""
    meta: BundleMetaV1 | None = None
    slices: tuple[NeighborhoodSliceV1, ...] = ()        # Ordered by kind
    graph: NeighborhoodGraphSummaryV1 | None = None
    node_index: dict[str, SemanticNodeRefV1] | None = None
    artifacts: tuple[ArtifactPointerV1, ...] = ()       # Externalized heavy slices
    diagnostics: tuple[DegradeEventV1, ...] = ()        # Degradation events
    schema_version: str = "cq.snb.v1"
```

**Key Invariants**:
- `bundle_id` is deterministic hash of target coordinates
- `slices` ordered by slice kind (not insertion order)
- `diagnostics` accumulate events from all 3 phases
- `artifacts` enable progressive disclosure

### SemanticNodeRefV1

**Definition**: `snb_schema.py:70-104`

Minimal node projection for slice membership. Full details live in `node_index`.

```python
class SemanticNodeRefV1(CqStruct, frozen=True):
    """Reference to a semantic node in the neighborhood.

    Minimal projection for neighborhood slice membership.
    Full node details live in node_index.
    """

    node_id: str                                        # Unique identifier
    kind: str                                           # "function", "class", etc.
    name: str                                           # Node name
    display_label: str = ""
    file_path: str = ""
    byte_span: tuple[int, int] | None = None            # (bstart, bend)
    signature: str | None = None
    qualname: str | None = None
```

**Node ID Convention**:
- Structural: `structural.{kind}.{file}:{line}:{col}`

### NeighborhoodSliceKind

**Definition**: `snb_schema.py:136-150`

13 literal slice kinds:

```python
NeighborhoodSliceKind = Literal[
    "callers",              # Functions calling this target
    "callees",              # Functions called by this target
    "references",           # All references to this symbol
    "implementations",      # Implementation targets
    "type_supertypes",      # Base classes
    "type_subtypes",        # Derived classes / overriding methods
    "parents",              # Containing scopes (structural)
    "children",             # Nested definitions (structural)
    "siblings",             # Peer definitions (structural)
    "enclosing_context",    # Innermost parent scope (structural)
    "imports",              # Import statements (future)
    "importers",            # Files importing this symbol (future)
    "related",              # Related symbols (future)
]
```

**Evidence Sources**:
- `structural.ast` — Tree-sitter-based collection

### NeighborhoodSliceV1

**Definition**: `snb_schema.py:153-184`

```python
class NeighborhoodSliceV1(CqStruct, frozen=True):
    """A single slice of the semantic neighborhood.

    Each slice represents a specific relationship type (callers, imports, etc.)
    with progressive disclosure support (preview + total count).
    """

    kind: NeighborhoodSliceKind
    title: str
    total: int = 0                                      # Full count
    preview: tuple[SemanticNodeRefV1, ...] = ()         # Top-k preview
    edges: tuple[SemanticEdgeV1, ...] = ()
    collapsed: bool = True
    metadata: dict[str, object] | None = None
```

**Progressive Disclosure**: If `total > len(preview)`, remaining nodes externalized to artifact.

### DegradeEventV1

**Definition**: `snb_schema.py:43-68`

```python
class DegradeEventV1(CqStruct, frozen=True):
    """Typed degradation event for structured failure tracking.

    Captures partial enrichment failures with enough context for
    programmatic filtering and diagnostic analysis.
    """

    stage: str                                          # "structural.target_resolution"
    severity: Literal["info", "warning", "error"] = "warning"
    category: str = ""                                  # "timeout", "unavailable", etc.
    message: str = ""
    correlation_key: str | None = None
```

**Common Stages**:
- `target_resolution` — Target parsing/resolution failures
- `structural.parse_error` — Parse failure
- `structural.timeout` — Query timeout
- `semantic.enrichment` — Static enrichment informational events

### BundleMetaV1

**Definition**: `snb_schema.py:218-246`

```python
class BundleMetaV1(CqStruct, frozen=True):
    """Bundle creation metadata and provenance."""

    tool: str = "cq"
    tool_version: str | None = None
    workspace_root: str | None = None
    query_text: str | None = None
    created_at_ms: float | None = None
    semantic_sources: tuple[dict[str, object], ...] = ()
    limits: dict[str, int] | None = None
```

See **Doc 06** for full schema details: `SemanticEdgeV1`, `ArtifactPointerV1`, `NeighborhoodGraphSummaryV1`.

---

## 3-Phase Bundle Assembly Pipeline

**Location**: `tools/cq/neighborhood/bundle_builder.py` (300 LOC)

**Entry Point**: `build_neighborhood_bundle(request: BundleBuildRequest) -> SemanticNeighborhoodBundleV1`

### BundleBuildRequest

**Definition**: `bundle_builder.py:30-46`

```python
class BundleBuildRequest(CqStruct, frozen=True):
    """Request to build a semantic neighborhood bundle."""

    target_name: str
    target_file: str
    root: Path
    language: str = "python"
    symbol_hint: str | None = None
    top_k: int = 10                                     # Max items per slice
    enable_semantic_enrichment: bool = True
    artifact_dir: Path | None = None
    target_line: int | None = None
    target_col: int | None = None
    target_uri: str | None = None
    allow_symbol_fallback: bool = True
    target_degrade_events: tuple[DegradeEventV1, ...] = ()
```

### Phase 1: Structural Collection (Tree-Sitter)

**Module**: `tree_sitter_collector.py` (668 LOC)

**Function**: `collect_tree_sitter_neighborhood()`

**Pipeline**:

1. **Parse Source**: Parse target file with tree-sitter (`python_lane` or `rust_lane`)
2. **Resolve Anchor**: Find anchor node at `target_line:target_col` via tree-sitter cursor
3. **Collect Relationships**:
   - **Parents**: Walk up tree collecting containing scopes (functions/classes)
   - **Children**: Query nested definitions via structural export
   - **Siblings**: Collect peer definitions in same enclosing scope
   - **Enclosing Context**: Innermost parent scope (smallest span)
   - **Callers/Callees**: Use `tree_sitter_neighborhood_query_engine.collect_callers_callees()`
4. **Convert to Slices**: Build `NeighborhoodSliceV1` with `evidence_source="structural.ast"`

**Tree-Sitter Query Engine Integration**:

The query engine (`tree_sitter_neighborhood_query_engine.py`, 116 LOC) uses CQ's tree-sitter runtime (see **Doc 07**):

```python
def collect_callers_callees(
    *,
    language: str,
    tree_root: Node,
    anchor: Node,
    source_bytes: bytes,
    anchor_name: str,
) -> tuple[list[Node], list[Node]]:
    """Collect caller and callee nodes using neighborhood query packs.

    Returns:
        tuple[list[Node], list[Node]]: Callers and callees.
    """
    query = _load_query(language, "10_calls.scm")
    if query is None or not anchor_name:
        return [], []

    # Run bounded query matches with budget constraints
    settings = QueryExecutionSettingsV1(
        max_matches_global=500,
        max_nodes_visited_global=10_000,
        timeout_ms=2000,
    )
    matches = run_bounded_query_matches(
        tree_root=tree_root,
        query=query,
        source_bytes=source_bytes,
        settings=settings,
    )
    # ... filter and classify as callers/callees
```

**Runtime Dependencies** (see **Doc 07: Tree-Sitter Subsystem**):
- `core/infrastructure.py` — `make_parser()` for multi-language parser instantiation with language binding
- `core/node_utils.py` — `node_text()` for UTF-8 byte-span extraction with max_len truncation
- `query/support.py` — `query_pack_path()` for canonical query pack path resolution
- `query/compiler.py` — `compile_query()` for query compilation with validation

**Cross-Reference**: See **Doc 07** for `run_bounded_query_matches()`, `QueryExecutionSettingsV1`, and tree-sitter runtime details.

**Key Data Structures**:

```python
@dataclass(frozen=True, slots=True)
class _SliceBuildSpec:
    kind: NeighborhoodSliceKind
    title: str
    edge_kind: str
    edge_direction: Literal["inbound", "outbound", "none"]
    metadata: dict[str, object] | None = None
```

**Degradation Tracking**:
- `DegradeEventV1(stage="target_resolution", category="not_found")` — Target not found
- `DegradeEventV1(stage="structural.parse_error", category="syntax_error")` — Parse failure
- `DegradeEventV1(stage="structural.timeout", category="timeout")` — Query timeout

### Phase 2: Merge and Deduplication

**Function**: `_merge_slices()` in `bundle_builder.py:157-196`

**Algorithm**:
1. Build kind-keyed map: `merged: dict[str, NeighborhoodSliceV1]`
2. For each slice:
   - If kind not in map, insert slice
   - If kind exists, merge nodes by `node_id` deduplication
   - Merge edges by `edge_id` deduplication
   - Update total count (sum of individual slice totals)
   - Truncate preview to `top_k`
3. Return merged list sorted by kind

**Rationale**: Deduplication prevents duplicate nodes when multiple collectors find the same entity.

### Phase 3: Artifact Split and Section Layout

**Artifact Externalization**: `_store_artifacts_with_preview()` in `bundle_builder.py:233-265`

For slices where `total > len(preview)`:
- Serialize full slice to JSON at `artifact_dir/slice_{kind}.json`
- Compute SHA256 deterministic ID
- Create `ArtifactPointerV1` with `storage_path`, `byte_size`, `deterministic_id`

**Section Layout**: See next section.

---

## Section Layout System

**Location**: `tools/cq/neighborhood/section_layout.py` (473 LOC)

### SECTION_ORDER (17 Slots)

**Definition**: `section_layout.py:18-36`

```python
SECTION_ORDER: tuple[str, ...] = (
    "target_tldr",          # 01 - collapsed: False
    "neighborhood_summary", # 02 - collapsed: False
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
    "imports",              # 13 - collapsed: True
    "semantic_deep_signals",# 14 - collapsed: True
    "diagnostics",          # 15 - collapsed: True
    "suggested_followups",  # 16 - collapsed: False
    "provenance",           # 17 - collapsed: True
)
```

**Rationale**: Fixed ordering ensures deterministic rendering for LLM consumption and diff-based caching.

### Collapse Rules

**Uncollapsed Sections**:
```python
_UNCOLLAPSED_SECTIONS = frozenset({
    "target_tldr",
    "neighborhood_summary",
    "suggested_followups",
})
```

**Dynamic Collapse**:
```python
_DYNAMIC_COLLAPSE_SECTIONS: dict[str, int] = {
    "parents": 3,           # Collapse if total > 3
    "enclosing_context": 1, # Collapse if total > 1
}
```

### BundleViewV1

**Definition**: `section_layout.py:94-107`

```python
class BundleViewV1(CqStruct, frozen=True):
    """Complete bundle view with deterministic section ordering."""

    key_findings: tuple[FindingV1, ...] = ()
    sections: tuple[SectionV1, ...] = ()

class SectionV1(CqStruct, frozen=True):
    """A single section in the bundle view."""

    kind: str
    title: str
    items: tuple[str, ...] = ()
    collapsed: bool = True
    metadata: dict[str, object] | None = None

class FindingV1(CqStruct, frozen=True):
    """A key finding entry for quick reference."""

    category: str
    label: str
    value: str
```

### materialize_section_layout()

**Function**: `section_layout.py:109-175`

**Algorithm**:
1. Build slot map: `slot_map: dict[str, NeighborhoodSliceV1]` from `bundle.slices`
2. Collect key findings from `bundle.subject`
3. Build synthetic sections: `neighborhood_summary`
4. Emit slice-backed sections in `SECTION_ORDER`
5. Emit diagnostics section
6. Emit follow-ups + provenance

**Section Rendering**:

```python
def _slice_to_section(slice_: NeighborhoodSliceV1) -> SectionV1:
    items: list[str] = []

    for node in slice_.preview:
        display = node.display_label or node.name
        if node.file_path:
            items.append(f"- **{display}** ({node.file_path})")
        else:
            items.append(f"- **{display}**")

    # Overflow indicator
    if slice_.total > len(slice_.preview):
        overflow = slice_.total - len(slice_.preview)
        items.append(f"_... and {overflow} more_")

    # Determine collapse state
    collapsed = True
    if slice_.kind in _UNCOLLAPSED_SECTIONS:
        collapsed = False
    elif slice_.kind in _DYNAMIC_COLLAPSE_SECTIONS:
        threshold = _DYNAMIC_COLLAPSE_SECTIONS[slice_.kind]
        collapsed = slice_.total > threshold

    return SectionV1(
        kind=slice_.kind,
        title=slice_.title,
        items=tuple(items),
        collapsed=collapsed,
        metadata=slice_.metadata,
    )
```

---

## Target Resolution

**Location**: `tools/cq/neighborhood/target_resolution.py` (186 LOC)

### ResolvedTarget

**Definition**: `target_resolution.py:16-27`

```python
class ResolvedTarget(CqStruct, frozen=True):
    """Resolved target used for bundle assembly."""

    target_name: str
    target_file: str
    target_line: int | None = None
    target_col: int | None = None
    target_uri: str | None = None
    symbol_hint: str | None = None
    resolution_kind: str = "unresolved"
    degrade_events: tuple[DegradeEventV1, ...] = ()
```

**Resolution Kinds**:
- `anchor` — Resolved via file:line:col anchor
- `file_symbol` — Resolved via name+file
- `symbol_fallback` — Resolved via symbol-only (ambiguous)
- `unresolved` — Complete resolution failure

### resolve_target()

**Function**: `target_resolution.py:29-118`

**Resolution Priority**:

1. **File-based** (file:line:col or file-only):
   - Normalize file path relative to root
   - Validate file exists
   - Return as `resolution_kind="anchor"` (if line present) or `"file_symbol"` (file-only)

2. **Symbol fallback** (if file missing and `allow_symbol_fallback=True`):
   - Use ripgrep (`find_symbol_candidates`) to find definitions matching `target_name`
   - Return first match as `resolution_kind="symbol_fallback"`
   - Emit `DegradeEventV1(severity="info", category="symbol_fallback")`

3. **Unresolved**:
   - Return `ResolvedTarget(resolution_kind="unresolved")` with error degradation

---

## Tree-Sitter Integration

The neighborhood subsystem leverages CQ's tree-sitter engine (see **Doc 07**) for structural collection.

### Tree-Sitter Collector

**Location**: `tree_sitter_collector.py` (668 LOC)

**Entry Point**: `collect_tree_sitter_neighborhood()`

**Key Operations**:

1. **Parse**: Use `make_parser(language)` from `core/infrastructure.py`
2. **Anchor Resolution**: Cursor-based node-at-position lookup
3. **Structural Queries**: Execute `.scm` queries via `run_bounded_query_matches()`
4. **Relationship Extraction**: Walk tree for parents/children/siblings

**Runtime Dependencies** (see **Doc 07**):
- `core/infrastructure.py` — `make_parser()` for parser instantiation with language binding
- `core/node_utils.py` — `node_text()` for UTF-8 text extraction with strip/max_len support
- `structural/export.py` — `export_structural_rows()` for definition extraction

**Structural Export Integration**:

The collector uses `export_structural_rows()` to extract definitions from the parse tree:

```python
from tools.cq.search.tree_sitter.structural.export import export_structural_rows

# Export structural rows for child/sibling extraction
export = export_structural_rows(
    root=tree_root,
    source_bytes=source_bytes,
    language=language,
    file_path=file_path,
)
```

### Tree-Sitter Neighborhood Query Engine

**Location**: `tree_sitter_neighborhood_query_engine.py` (116 LOC)

**Function**: `collect_callers_callees()`

**Pipeline**:
1. Load query: `_load_query(language, "10_calls.scm")` → `compile_query()` from `query/compiler.py`
2. Execute bounded query: `run_bounded_query_matches(tree_root, query, source_bytes, settings)`
3. Extract node text: `node_text(node, source_bytes)` from `core/node_utils.py`
4. Filter matches to `anchor_name`
5. Classify as callers (node references anchor) or callees (anchor calls node)

**Runtime Dependencies** (see **Doc 07**):
- `query/compiler.py` — `compile_query()` replaces inline query compilation
- `core/node_utils.py` — `node_text()` replaces inline byte-span extraction logic
- `query/support.py` — `query_pack_path()` replaces inline `Path` construction for query pack resolution
- `core/runtime.py` — `run_bounded_query_matches()` provides bounded query execution

**Budget Constraints**:

```python
settings = QueryExecutionSettingsV1(
    max_matches_global=500,
    max_nodes_visited_global=10_000,
    timeout_ms=2000,
)
```

**Key Refactoring**:
The query engine now delegates query compilation, text extraction, and path resolution to shared utilities from the tree-sitter subsystem (Doc 07), eliminating code duplication and ensuring consistency with other tree-sitter consumers.

**Cross-Reference**: See **Doc 07** for:
- `run_bounded_query_matches()` implementation
- `QueryExecutionSettingsV1` fields
- Budget enforcement and telemetry
- `compile_query()`, `node_text()`, and `query_pack_path()` implementations

---

## SNB Rendering

**Location**: `tools/cq/neighborhood/snb_renderer.py` (160 LOC)

### RenderSnbRequest

**Definition**: `snb_renderer.py:21-31`

```python
class RenderSnbRequest(CqStruct, frozen=True):
    """Request envelope for rendering an SNB bundle to CQ result."""

    run: RunMeta
    bundle: SemanticNeighborhoodBundleV1
    target: str
    language: str
    top_k: int
    enable_semantic_enrichment: bool
    semantic_env: Mapping[str, object] | None = None
```

### render_snb_result()

**Function**: `snb_renderer.py:33-52`

**Pipeline**:
1. Create `CqResult` from `RunMeta`
2. Materialize section layout: `view = materialize_section_layout(bundle)`
3. Populate summary: target, language, top_k, bundle_id, slice counts, graph stats
4. Populate findings: Convert `FindingV1` → `Finding`, `SectionV1` → `Section`
5. Populate artifacts: Convert `ArtifactPointerV1` → `Artifact`

**Summary Fields**:
```python
result.summary["target"] = target
result.summary["language"] = language
result.summary["top_k"] = top_k
result.summary["enable_semantic_enrichment"] = enable_semantic_enrichment
result.summary["bundle_id"] = bundle.bundle_id
result.summary["total_slices"] = len(bundle.slices)
result.summary["total_diagnostics"] = len(bundle.diagnostics)
if bundle.graph is not None:
    result.summary["total_nodes"] = bundle.graph.node_count
    result.summary["total_edges"] = bundle.graph.edge_count
```

**Evidence Enrichment**:

Bundle metadata added to `result.evidence` with enrichment payload:

```python
enrichment_payload: dict[str, object] = {
    "neighborhood_bundle": {
        "bundle_id": bundle.bundle_id,
        "subject_label": bundle.subject_label,
        "slice_count": len(bundle.slices),
        "diagnostic_count": len(bundle.diagnostics),
    },
    "degrade_events": [_degrade_event_dict(event) for event in bundle.diagnostics],
}
```

---

## Front-Door Insight Integration

The neighborhood subsystem's structural data feeds the `FrontDoorInsightV1` contract used by front-door commands (search, calls, entity). This integration reuses existing infrastructure rather than duplicating neighborhood logic.

**Cross-Reference**: See **Doc 06: Data Models** for `FrontDoorInsightV1` schema and `InsightSliceV1` details.

### Structural Adapter Reuse

Front-door commands reuse tree-sitter scan state to extract structural neighborhood data. The `build_search_insight()`, `build_calls_insight()`, and `build_entity_insight()` functions produce `InsightSliceV1` objects with:

- `total`: Full count from structural scan
- `preview`: Bounded preview (up to `budget.preview_per_slice` items)
- `availability`: "full" | "partial" | "unavailable" based on scan completeness
- `source`: "structural" | "heuristic" | "none" tracking data provenance

### InsightSliceV1 vs NeighborhoodSliceV1

| Aspect | InsightSliceV1 | NeighborhoodSliceV1 |
|--------|---------------|---------------------|
| Purpose | Compact front-door preview | Full neighborhood detail |
| Preview size | 5 items (budgeted) | 10+ items (configurable) |
| Edge data | None | Full edges with evidence |
| Collapse state | Always visible | Dynamic collapse rules |
| Metadata | availability + source | Full metadata dict |
| Overflow | artifact_ref pointer | In-bundle |

### Overflow Artifact Handling

When insight preview slices are truncated, `save_neighborhood_overflow_artifact()` persists the full neighborhood list. The artifact path is stored in `InsightArtifactRefsV1.neighborhood_overflow`.

---

## CLI and Run Integration

### CLI Command

**Location**: `tools/cq/cli_app/commands/neighborhood.py` (145 LOC)

**Command**:
```bash
cq neighborhood <target> [--lang python] [--top-k 10] [--semantic-enrichment]
cq nb <target>  # Alias
```

**Pipeline**:
1. Parse target: `spec = parse_target_spec(target)`
2. Resolve target: `resolved = resolve_target(spec, root=ctx.root, language=lang)`
3. Build bundle: `bundle = build_neighborhood_bundle(request)`
4. Render to CqResult: `result = render_snb_result(request)`

**Usage**:
```bash
cq neighborhood src/graph.py:42:4 --lang python --top-k 10
cq nb build_graph_product --lang python --semantic-enrichment
```

### Run Integration (NeighborhoodStep)

**Location**: `tools/cq/run/spec.py:106-113`

**Definition**:
```python
class NeighborhoodStep(RunStepBase, tag="neighborhood", frozen=True):
    target: str
    lang: str = "python"
    top_k: int = 10
    semantic_enrichment: bool = True
```

**Example TOML Plan**:
```toml
[[steps]]
type = "neighborhood"
target = "src/graph.py:42:4"
lang = "python"
top_k = 15
semantic_enrichment = true
```

**Execution**: Same pipeline as CLI, dispatched via `tools/cq/run/executor.py`.

---

## Architectural Design

### Design Rationale

#### 1. Shared Tree-Sitter Runtime Dependencies

The neighborhood subsystem delegates tree-sitter primitives to shared utilities from the tree-sitter subsystem (**Doc 07**):

- **Parser Creation**: `make_parser()` from `core/infrastructure.py` replaces inline grammar loading
- **Text Extraction**: `node_text()` from `core/node_utils.py` replaces inline byte-span slicing
- **Query Path Resolution**: `query_pack_path()` from `query/support.py` replaces inline `Path` construction
- **Query Compilation**: `compile_query()` from `query/compiler.py` replaces inline query creation
- **Structural Export**: `export_structural_rows()` from `structural/export.py` provides definition extraction

**Rationale**: Eliminates code duplication across tree-sitter consumers (neighborhood, search, structural export) and ensures consistency in language binding, text handling, and query pack resolution.

**Migration**: The `tree_sitter_neighborhood_query_engine.py` and `tree_sitter_collector.py` modules now import shared utilities instead of maintaining parallel implementations.

#### 2. Tree-Sitter-First Structural Collection

The tree-sitter-based collector provides:

- **Precision**: Exact byte-span anchoring instead of line-based matching
- **Performance**: Single-pass tree walking vs. multiple external tool invocations
- **Completeness**: Access to full AST for relationship queries

**Trade-off**: Requires tree-sitter grammar maintenance, but gains deeper structural access.

#### 3. Static Semantic Enrichment

The `enable_semantic_enrichment` flag controls whether static semantic planes are collected:

- When `True`: Adds informational `DegradeEventV1` noting tree-sitter structural assembly
- When `False`: Pure structural collection without enrichment metadata

**Design Tension**:
- **Current**: Flag is informational-only, actual enrichment is tree-sitter structural
- **Future**: Could gate additional static analysis planes (bytecode, type inference)

**Current Policy**: Static enrichment flag enables future extensibility without changing the core pipeline.

#### 4. 17-Slot Deterministic Section Layout

Fixed `SECTION_ORDER` prevents drift from collector return order. Critical for:
- LLM consumption (consistent section ordering across queries)
- Diff-based caching (deterministic ordering enables content hashing)
- Reading contract (target → summary → structure → references → diagnostics)

**Improvement Vectors**:
- User-configurable section order via `.cq.toml`
- Conditional section emission (skip empty sections)

#### 5. 3-Phase Sequential Assembly

**Why not parallel execution?**
- Phase 1 must complete before Phase 2 (merge needs all slices)
- Phase 2 depends on Phase 1 (all slices must be collected)
- Phase 3 depends on Phase 2 (merged slices determine artifact split)

**Potential Optimization**: Parallel collection of independent slice kinds within Phase 1 (currently not implemented).

#### 6. Progressive Disclosure via Artifact Pointers

Heavy slices (where `total > len(preview)`) are externalized to JSON artifacts. This prevents bloating the main bundle.

**Trade-off**:
- **Pro**: Keeps main bundle compact, enables lazy loading
- **Con**: Requires artifact storage (currently filesystem-only), no streaming

**Extension Point**: Replace `_store_artifacts_with_preview()` with object-store adapter (S3, GCS).

#### 7. Anchor-First Target Resolution

Prioritizes file:line:col anchors over symbol names for:
- **Disambiguation**: Multiple definitions with same name in same file
- **Precision**: Tree-sitter requires exact position for node resolution

**Fallback Hierarchy**:
1. Anchor (file:line:col) — most precise
2. File-only — scoped to file
3. Symbol-only — ambiguous, requires warning

**Improvement Vector**: Add column-aware resolution (currently `target_col` is advisory).

#### 8. Typed Degradation Events

`DegradeEventV1` provides structured failure tracking with:
- `stage` — Where failure occurred
- `severity` — Impact level (error/warning/info)
- `category` — Failure mode (timeout, unavailable, ambiguous)

**Rationale**: Enables programmatic filtering and diagnostic analysis. LLMs can suppress `severity="info"` or highlight `severity="error"`.

**Extension Point**: Add `correlation_key` for grouping related events.

### Performance Characteristics

**Bottlenecks**:
- Tree-sitter parsing: 50-200ms for medium files
- Query execution: 10-50ms for bounded queries
- Structural export: 20-100ms for definition extraction

**Optimization Opportunities**:
- Scan caching: Cache tree-sitter parse trees between queries
- Lazy slice collection: Only collect slices requested by user
- Parallel slice collection: Run independent collectors concurrently

---

## Extension Points

### 1. New Slice Kinds

Add new `NeighborhoodSliceKind` literals:
- `imports` / `importers` — Import graph relationships (partially implemented)
- `related` — Similar code via embedding distance
- `tests` — Test coverage for target symbol

**Requirements**:
1. Add literal to `NeighborhoodSliceKind` in `snb_schema.py`
2. Add collector function (structural-based)
3. Add to `SECTION_ORDER` in `section_layout.py`

### 2. New Evidence Sources

Add new structural collectors:
- Bytecode-based caller/callee detection
- Type inference relationships
- Dependency graph extraction

**Pattern**:
```python
def collect_bytecode_slices(
    *,
    root: Path,
    target_file: str,
    target_line: int,
    ...
) -> tuple[tuple[NeighborhoodSliceV1, ...], tuple[DegradeEventV1, ...]]:
    # Analyze bytecode
    # Extract slices
    # Return (slices, degrades)
```

Register in `collect_tree_sitter_neighborhood()` or as separate enrichment phase.

### 3. Artifact Storage Backends

Replace filesystem storage with object store:
- S3 / GCS / Azure Blob
- Redis (ephemeral artifacts)
- Embedded SQLite (local persistence)

**Interface**:
```python
class ArtifactStore(Protocol):
    def store(self, artifact_id: str, payload: bytes) -> str:
        """Store artifact, return storage path."""
        ...

    def retrieve(self, storage_path: str) -> bytes:
        """Retrieve artifact by storage path."""
        ...
```

### 4. Custom Section Layouts

Allow `.cq.toml` to override `SECTION_ORDER`:

```toml
[neighborhood.section_order]
order = [
    "target_tldr",
    "callers",          # Prioritize callers
    "callees",
    "references",
    ...
]
```

---

## Testing Recommendations

**Unit Tests**:
- Target resolution edge cases (ambiguous symbols, file missing, symbol fallback)
- Section layout determinism (verify fixed order across runs)
- Degradation event accumulation (verify stage/severity/category)
- Slice merging logic (deduplication, top-k truncation)

**Integration Tests**:
- Full pipeline (parse → resolve → build → render)
- Artifact externalization (verify preview/full split)
- Tree-sitter collector (verify slice generation)

**Golden Tests**:
- CLI output snapshots for deterministic rendering
- Bundle serialization round-trip (msgspec → JSON → msgspec)

---

## Summary

The neighborhood subsystem provides a robust, extensible framework for semantic neighborhood analysis with:

**Key Strengths**:
- Deterministic output via fixed section ordering
- Graceful degradation via typed events
- Progressive disclosure via artifact externalization
- Multi-language support (Python/Rust) with unified schema
- Tree-sitter-first structural collection for precision
- Shared runtime utilities from tree-sitter subsystem (eliminates code duplication)
- Static semantic enrichment flag for future extensibility

**Key Extension Points**:
- New slice kinds (imports, tests, related code)
- New structural collectors (bytecode, type inference)
- Custom section layouts via config
- Alternative artifact storage backends

**Performance Bottlenecks**:
- Tree-sitter parsing
- Query execution budgets
- Structural export

**Recommended Next Steps**:
1. Read `bundle_builder.py` for pipeline orchestration
2. Read `tree_sitter_collector.py` for structural collection
3. Read `target_resolution.py` for anchor/symbol resolution
4. Experiment with adding new slice kinds or structural collectors
