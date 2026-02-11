# CQ Neighborhood Subsystem Architecture

## Executive Summary

The CQ neighborhood subsystem (`tools/cq/neighborhood/`) provides targeted semantic neighborhood analysis around code anchors. It assembles rich contextual bundles that combine structural relationships (AST-based), LSP-derived signals (references, type hierarchies), and diagnostic metadata into a deterministic, versioned artifact format.

**Key characteristics:**

- 4-phase bundle assembly pipeline (structural → LSP → merge → artifact split)
- Capability-gated LSP enrichment with structured degradation tracking
- Deterministic 17-slot section layout with dynamic collapse rules
- Anchor-first target resolution with symbol fallback (file:line:col → symbol)
- ScanSnapshot adapter decoupling from query executor internals
- SemanticNeighborhoodBundleV1 (SNB) canonical schema with progressive disclosure
- Multi-language support (Python via Pyrefly, Rust via rust-analyzer)
- Typed degradation events with stage/category/severity classification

**Target audience:** Advanced LLM programmers proposing architectural improvements.

---

## 1. Module Map

### Core Modules

| Module | Size (LOC) | Responsibility |
|--------|------------|----------------|
| `bundle_builder.py` | ~646 | 4-phase bundle assembly orchestration, LSP coordination |
| `structural_collector.py` | ~409 | AST-based neighborhood collection (parents/children/siblings/callers/callees) |
| `section_layout.py` | ~473 | Deterministic 17-slot section ordering, collapse rules, view materialization |
| `target_resolution.py` | ~372 | Anchor/symbol target parsing and resolution with degradation tracking |
| `pyrefly_adapter.py` | ~269 | Pyrefly LSP → SNB slice adapter for Python |
| `snb_renderer.py` | ~158 | SNB → CqResult markdown rendering |
| `capability_gates.py` | ~134 | LSP capability gating and feasible slice planning |
| `scan_snapshot.py` | ~134 | ScanSnapshot adapter decoupling structural collector from executor internals |

### Schema Modules

| Module | Size (LOC) | Responsibility |
|--------|------------|----------------|
| `core/snb_schema.py` | ~322 | Canonical SNB schema definitions (all frozen msgspec.Struct) |
| `core/snb_registry.py` | ~100 | Typed kind registry for runtime validation and decoding |

### Integration Modules

| Module | Size (LOC) | Responsibility |
|--------|------------|----------------|
| `cli_app/commands/neighborhood.py` | ~109 | CLI command handler for `/cq neighborhood` and `/cq nb` |
| `run/spec.py` (NeighborhoodStep) | ~223 (full file) | Run-plan integration via NeighborhoodStep |

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                   CLI / Run Integration Layer                   │
│  neighborhood.py (CLI)          NeighborhoodStep (run/spec.py)  │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Target Resolution Layer                       │
│  target_resolution.py: parse_target_spec() → resolve_target()   │
│  • file:line:col anchor parsing                                 │
│  • symbol fallback resolution                                   │
│  • degradation event tracking                                   │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                ScanSnapshot Adapter (decoupling)                 │
│  scan_snapshot.py: ScanSnapshot.from_records() / .build_from_repo()│
│  • def_records, call_records, interval_index                    │
│  • calls_by_def index                                           │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                4-Phase Bundle Assembly Pipeline                  │
│  bundle_builder.py: build_neighborhood_bundle()                 │
│                                                                  │
│  Phase 1: Structural Collection                                 │
│    structural_collector.py: collect_structural_neighborhood()   │
│    • parents, children, siblings (containment)                  │
│    • callers, callees (call graph)                              │
│    • enclosing_context (innermost parent)                       │
│                                                                  │
│  Phase 2: LSP Enrichment (capability-gated)                     │
│    capability_gates.py: plan_feasible_slices()                  │
│    pyrefly_adapter.py: collect_pyrefly_slices() (Python)        │
│    rust_lsp.py: enrich_with_rust_lsp() (Rust)                   │
│    • references, implementations, type_supertypes, type_subtypes│
│                                                                  │
│  Phase 3: Merge and Deduplication                               │
│    _merge_slices(): kind-keyed merging, top_k enforcement       │
│                                                                  │
│  Phase 4: Artifact Split and Section Layout                     │
│    _store_artifacts_with_preview(): split heavy slices          │
│    section_layout.py: materialize_section_layout()              │
│                                                                  │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│          SemanticNeighborhoodBundleV1 (SNB Schema)              │
│  snb_schema.py: canonical frozen structs                        │
│  • subject: SemanticNodeRefV1                                   │
│  • slices: tuple[NeighborhoodSliceV1, ...]                      │
│  • diagnostics: tuple[DegradeEventV1, ...]                      │
│  • artifacts: tuple[ArtifactPointerV1, ...]                     │
│  • graph: NeighborhoodGraphSummaryV1                            │
│  • meta: BundleMetaV1                                           │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Rendering and Output                           │
│  snb_renderer.py: render_snb_result()                           │
│  section_layout.py: BundleViewV1 with 17-slot ordering          │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. SNB Data Model

### Core Schema Authority

**Location:** `tools/cq/core/snb_schema.py` (322 LOC)

All SNB structures use frozen `msgspec.Struct` for deterministic serialization. This module is the CANONICAL schema authority for all downstream SNB operations.

### SemanticNeighborhoodBundleV1

**Definition:** `snb_schema.py:266-309`

```python
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

**Key invariants:**

- `bundle_id` is a deterministic SHA256 hash of target coordinates
- `subject` is the resolved target node reference
- `slices` are ordered by kind (not insertion order — see section layout)
- `diagnostics` accumulate degradation events from all phases
- `artifacts` point to externalized heavy payloads (progressive disclosure)
- `schema_version` enables forward compatibility

### SemanticNodeRefV1

**Definition:** `snb_schema.py:70-104`

Minimal projection for neighborhood slice membership. Full node details live in node_index.

```python
class SemanticNodeRefV1(CqStruct, frozen=True):
    """Reference to a semantic node in the neighborhood."""

    node_id: str                        # Unique identifier
    kind: str                           # e.g. "function", "class", "import"
    name: str                           # Node name or identifier
    display_label: str = ""             # Human-readable label
    file_path: str = ""                 # Source file path
    byte_span: tuple[int, int] | None = None  # (bstart, bend)
    signature: str | None = None        # Function/method signature
    qualname: str | None = None         # Qualified name
```

**Node ID convention:**

- Structural: `structural.{kind}.{file}:{line}:{col}`
- LSP Pyrefly: `lsp.pyrefly.{kind}.{file}:{line}:{col}:{name}`
- LSP Rust: `lsp.rust.symbol.{file}:{line}:{col}:{name}`
- Target fallback: `target.{file}:{name}`

### NeighborhoodSliceKind

**Definition:** `snb_schema.py:136-151`

14 literal slice kinds (typed union):

```python
NeighborhoodSliceKind = Literal[
    "callers",              # Functions calling this target
    "callees",              # Functions called by this target
    "references",           # All references to this symbol (LSP)
    "implementations",      # Implementation targets (LSP)
    "type_supertypes",      # Base classes (LSP)
    "type_subtypes",        # Derived classes / overriding methods (LSP)
    "parents",              # Containing scopes (structural)
    "children",             # Nested definitions (structural)
    "siblings",             # Peer definitions (structural)
    "enclosing_context",    # Innermost parent scope (structural)
    "imports",              # Import statements (future)
    "importers",            # Files importing this symbol (future)
    "related",              # Related symbols (future)
]
```

**Evidence sources:**

- `structural.ast` — ast-grep based collection
- `lsp.pyrefly` — Pyrefly LSP adapter for Python
- `lsp.rust` — rust-analyzer LSP adapter for Rust

### NeighborhoodSliceV1

**Definition:** `snb_schema.py:153-184`

```python
class NeighborhoodSliceV1(CqStruct, frozen=True):
    """A single slice of the semantic neighborhood.

    Each slice represents a specific relationship type (callers, imports, etc.)
    with progressive disclosure support (preview + total count).
    """

    kind: NeighborhoodSliceKind
    title: str                          # Human-readable title
    total: int = 0                      # Total count of nodes
    preview: tuple[SemanticNodeRefV1, ...] = ()  # Preview subset (top_k)
    edges: tuple[SemanticEdgeV1, ...] = ()       # Edges to slice members
    collapsed: bool = True              # UI collapse state
    metadata: dict[str, object] | None = None    # Optional metadata
```

**Progressive disclosure pattern:**

- `preview` contains first `top_k` nodes
- `total` indicates full count
- If `total > len(preview)`, remaining nodes are externalized to artifact
- Renderer shows `"... and N more"` when overflow occurs

### SemanticEdgeV1

**Definition:** `snb_schema.py:106-134`

```python
class SemanticEdgeV1(CqStruct, frozen=True):
    """Directed semantic edge in the neighborhood graph."""

    edge_id: str                        # Unique identifier
    source_node_id: str                 # Source node ID
    target_node_id: str                 # Target node ID
    edge_kind: str                      # "calls", "contains", "references", etc.
    weight: float = 1.0                 # Ranking or confidence
    evidence_source: str = ""           # "structural.ast", "lsp.pyrefly", etc.
    metadata: dict[str, object] | None = None
```

**Edge kind conventions:**

- `calls` — caller → callee relationship
- `contains` — parent → child containment
- `references` — reference site → definition
- `implements` — implementation → interface
- `extends` — subtype → supertype
- `overrides` — override → base method

### DegradeEventV1

**Definition:** `snb_schema.py:43-68`

Typed degradation event for structured failure tracking.

```python
class DegradeEventV1(CqStruct, frozen=True):
    """Typed degradation event for structured failure tracking."""

    stage: str                          # e.g. "structural.target_resolution"
    severity: Literal["info", "warning", "error"] = "warning"
    category: str = ""                  # e.g. "timeout", "unavailable", "ambiguous"
    message: str = ""                   # Human-readable diagnostic
    correlation_key: str | None = None  # Grouping key for related events
```

**Common stage identifiers:**

- `target_resolution` — Target parsing/resolution failures
- `structural.target_resolution` — Anchor/symbol resolution issues
- `structural.interval_index` — Interval index unavailable
- `lsp.planning` — Capability gating decisions
- `lsp.pyrefly` — Pyrefly enrichment issues
- `lsp.rust` — Rust LSP enrichment issues

**Severity levels:**

- `error` — Critical failure, likely incomplete results
- `warning` — Partial failure, degraded quality
- `info` — Informational, not a failure

### BundleMetaV1

**Definition:** `snb_schema.py:218-246`

```python
class BundleMetaV1(CqStruct, frozen=True):
    """Bundle creation metadata and provenance."""

    tool: str = "cq"
    tool_version: str | None = None
    workspace_root: str | None = None
    query_text: str | None = None       # Original query
    created_at_ms: float | None = None  # Duration in ms
    lsp_servers: tuple[dict[str, object], ...] = ()
    limits: dict[str, int] | None = None  # e.g. {"top_k": 10}
```

### ArtifactPointerV1

**Definition:** `snb_schema.py:14-41`

Generic artifact pointer for lazy loading and cache keying.

```python
class ArtifactPointerV1(CqStruct, frozen=True):
    """Generic artifact pointer with deterministic identity."""

    artifact_kind: str                  # e.g. "snb.slice"
    artifact_id: str                    # Unique identifier
    deterministic_id: str               # SHA256 of normalized content
    byte_size: int = 0                  # Payload size
    storage_path: str | None = None     # Filesystem path
    metadata: dict[str, object] | None = None
```

### Typed Registry

**Location:** `tools/cq/core/snb_registry.py` (100 LOC)

Provides runtime validation and decoding:

```python
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
```

Functions:

- `resolve_kind(kind: str) -> type[msgspec.Struct] | None` — Resolve kind to struct type
- `validate_finding_details(details: dict) -> bool` — Validate kind presence
- `decode_finding_details(details: dict) -> msgspec.Struct | None` — Decode to typed struct

---

## 3. 4-Phase Bundle Assembly Pipeline

**Location:** `tools/cq/neighborhood/bundle_builder.py` (646 LOC)

**Entry point:** `build_neighborhood_bundle(request: BundleBuildRequest) -> SemanticNeighborhoodBundleV1`

### BundleBuildRequest

**Definition:** `bundle_builder.py:44-64`

```python
class BundleBuildRequest(CqStruct, frozen=True):
    """Request to build a semantic neighborhood bundle."""

    target_name: str
    target_file: str
    root: Path
    snapshot: ScanSnapshot              # Decoupled from ScanContext
    language: str = "python"
    symbol_hint: str | None = None
    top_k: int = 10                     # Max items per slice
    enable_lsp: bool = True
    artifact_dir: Path | None = None
    target_line: int | None = None
    target_col: int | None = None
    target_uri: str | None = None
    allow_symbol_fallback: bool = True
    target_degrade_events: tuple[DegradeEventV1, ...] = ()
```

### Phase 1: Structural Collection

**Function:** `collect_structural_neighborhood()` in `structural_collector.py:38-201`

**Signature:**

```python
def collect_structural_neighborhood(
    target_name: str,
    target_file: str,
    snapshot: ScanSnapshot,
    *,
    target_line: int | None = None,
    target_col: int | None = None,
    max_per_slice: int = 50,
    slice_limits: Mapping[str, int] | None = None,
) -> tuple[tuple[NeighborhoodSliceV1, ...], tuple[DegradeEventV1, ...]]
```

**Pipeline:**

1. **Find target definition** (`_find_target_definition()`)
   - Anchor-based resolution (file:line:col) — preferred
   - If anchor fails, fall back to name+file resolution
   - If name+file fails, return error degradation

2. **Collect neighborhood relationships** (`_collect_neighborhood()`)
   - **Parents**: Use interval index to find containing scopes
   - **Children**: Filter def_records for nested definitions
   - **Siblings**: Peer definitions in same enclosing context
   - **Enclosing context**: Innermost parent (smallest span)
   - **Callees**: Lookup `calls_by_def[target_key]`
   - **Callers**: Reverse lookup — find defs whose calls include target

3. **Apply slice limits**
   - Per-kind limits from `slice_limits` or default `max_per_slice`
   - Each slice is truncated to `limit` items

4. **Convert to NeighborhoodSliceV1**
   - Build `SemanticNodeRefV1` for each record
   - Build `SemanticEdgeV1` with `evidence_source="structural.ast"`
   - Set `collapsed=True` for all slices except `enclosing_context`

**Key data structures:**

```python
class StructuralNeighborhood(CqStruct, frozen=True):
    """Structural relationships from ast-grep scan snapshot."""

    parents: tuple[SgRecord, ...] = ()
    children: tuple[SgRecord, ...] = ()
    siblings: tuple[SgRecord, ...] = ()
    enclosing_context: SgRecord | None = None
    callers: tuple[SgRecord, ...] = ()
    callees: tuple[SgRecord, ...] = ()
```

**Interval index usage:**

```python
interval_index: IntervalIndex[SgRecord] | None = (
    snapshot.interval_index if isinstance(snapshot.interval_index, IntervalIndex) else None
)

if interval_index is not None:
    candidates = interval_index.find_candidates(target.start_line)
    parents = [
        candidate
        for candidate in candidates
        if candidate != target
        and candidate.file == target.file
        and candidate.start_line <= target.start_line <= candidate.end_line
    ]
```

**Degradation tracking:**

- `DegradeEventV1(stage="structural.target_resolution", severity="error", category="not_found")` — Target not found
- `DegradeEventV1(stage="structural.target_resolution", severity="warning", category="ambiguous")` — Multiple matches, choosing innermost

### Phase 2: LSP Enrichment (Capability-Gated)

**Orchestration:** `bundle_builder.py:84-96`

```python
lsp_slices: list[NeighborhoodSliceV1] = []
lsp_env: dict[str, object] = {}
if request.enable_lsp:
    negotiated_caps = _get_negotiated_caps(request)
    feasible_slices, cap_degrades = plan_feasible_slices(
        requested_slices=_lsp_slice_kinds(),
        capabilities=negotiated_caps,
    )
    degrade_events.extend(cap_degrades)

    lsp_slices_result, lsp_degrades, lsp_env = _collect_lsp_slices(request, feasible_slices)
    lsp_slices.extend(lsp_slices_result)
    degrade_events.extend(lsp_degrades)
```

**Requested LSP slice kinds:**

```python
def _lsp_slice_kinds() -> tuple[NeighborhoodSliceKind, ...]:
    return (
        "references",
        "implementations",
        "type_supertypes",
        "type_subtypes",
    )
```

#### Capability Gating

**Location:** `capability_gates.py:63-90`

**Function:** `plan_feasible_slices()`

```python
def plan_feasible_slices(
    requested_slices: tuple[NeighborhoodSliceKind, ...],
    capabilities: LspCapabilitySnapshotV1 | Mapping[str, object] | None,
    *,
    stage: str = "lsp.planning",
) -> tuple[tuple[NeighborhoodSliceKind, ...], tuple[DegradeEventV1, ...]]
```

**Capability requirements:**

```python
_SLICE_CAPABILITY_REQUIREMENTS: dict[NeighborhoodSliceKind, SlicePredicate] = {
    "references": lambda snapshot: snapshot.server_caps.references_provider,
    "implementations": lambda snapshot: snapshot.server_caps.implementation_provider,
    "type_supertypes": _supports_type_hierarchy,
    "type_subtypes": _supports_type_hierarchy,
}

def _supports_type_hierarchy(snapshot: LspCapabilitySnapshotV1) -> bool:
    server = snapshot.server_caps
    return server.type_hierarchy_provider or server.type_definition_provider
```

**Normalization:**

Raw capability maps (from LSP `initialize` response) are normalized to `LspCapabilitySnapshotV1`:

```python
def normalize_capability_snapshot(
    capabilities: LspCapabilitySnapshotV1 | Mapping[str, object] | None,
) -> LspCapabilitySnapshotV1
```

Handles both camelCase and snake_case keys, extracts provider flags.

**Degradation on unavailable capabilities:**

```python
degrades.append(
    DegradeEventV1(
        stage=stage,
        severity="info",
        category="unavailable",
        message=f"Slice '{kind}' unavailable for negotiated LSP capabilities",
    )
)
```

#### Python LSP (Pyrefly Adapter)

**Location:** `pyrefly_adapter.py:20-145`

**Function:** `collect_pyrefly_slices()`

```python
def collect_pyrefly_slices(
    *,
    root: Path,
    target_file: str,
    target_line: int | None,
    target_col: int | None,
    target_name: str,
    feasible_slices: tuple[NeighborhoodSliceKind, ...],
    top_k: int,
    symbol_hint: str | None = None,
) -> tuple[tuple[NeighborhoodSliceV1, ...], tuple[DegradeEventV1, ...], dict[str, object]]
```

**Pipeline:**

1. Validate anchor: `target_file` and `target_line` are required
2. Call `enrich_with_pyrefly_lsp(PyreflyLspRequest(...))`
3. Extract slice data from payload:
   - `references` — `payload["local_scope_context"]["reference_locations"]`
   - `implementations` — `payload["symbol_grounding"]["implementation_targets"]`
   - `type_supertypes` — `payload["class_method_context"]["base_classes"]`
   - `type_subtypes` — `payload["class_method_context"]["overriding_methods"]`
4. Build `NeighborhoodSliceV1` with `evidence_source="lsp.pyrefly"`
5. Extract LSP environment metadata:
   - `lsp_position_encoding` — `payload["coverage"]["position_encoding"]`
   - `lsp_health` — `"ok"` (hardcoded)
   - `lsp_quiescent` — `True` (hardcoded)

**Node conversion:**

```python
def _row_to_node(row: Mapping[str, object]) -> SemanticNodeRefV1:
    symbol = row.get("symbol") or row.get("name") or "<unknown>"
    file_path = row.get("file") if isinstance(row.get("file"), str) else ""
    line = row.get("line") if isinstance(row.get("line"), int) else 0
    col = row.get("col") if isinstance(row.get("col"), int) else 0
    kind = row.get("kind") if isinstance(row.get("kind"), str) else "symbol"

    return SemanticNodeRefV1(
        node_id=f"lsp.pyrefly.{kind}.{file_path}:{line}:{col}:{symbol}",
        kind=kind,
        name=symbol,
        display_label=symbol,
        file_path=file_path,
    )
```

#### Rust LSP (rust-analyzer)

**Location:** `bundle_builder.py:211-333`

**Function:** `_collect_rust_slices()`

Similar pattern to Pyrefly, but uses `enrich_with_rust_lsp()` from `tools/cq/search/rust_lsp.py`:

1. Validate anchor presence
2. Call `enrich_with_rust_lsp(RustLspRequest(...))`
3. Extract from payload:
   - `references` — `payload["symbol_grounding"]["references"]`
   - `implementations` — `payload["symbol_grounding"]["implementations"]`
   - `type_supertypes` — `payload["type_hierarchy"]["supertypes"]`
   - `type_subtypes` — `payload["type_hierarchy"]["subtypes"]`
4. Build slices with `evidence_source="lsp.rust"`

### Phase 3: Merge and Deduplication

**Function:** `_merge_slices()` in `bundle_builder.py:423-452`

```python
def _merge_slices(
    structural: tuple[NeighborhoodSliceV1, ...],
    lsp: tuple[NeighborhoodSliceV1, ...],
    top_k: int,
) -> list[NeighborhoodSliceV1]
```

**Algorithm:**

1. Build kind-keyed map: `kind_map: dict[str, NeighborhoodSliceV1]`
2. Structural slices are inserted first
3. LSP slices overwrite structural slices with same kind
4. For each slice, if `len(preview) > top_k`, truncate to `top_k`
5. Return merged list

**Rationale:** LSP slices take precedence over structural slices for same kinds (e.g., `callers` from LSP is more precise than AST-based).

### Phase 4: Artifact Split and Section Layout

#### Artifact Externalization

**Function:** `_store_artifacts_with_preview()` in `bundle_builder.py:455-491`

```python
def _store_artifacts_with_preview(
    artifact_dir: Path | None,
    slices: list[NeighborhoodSliceV1],
) -> list[ArtifactPointerV1]
```

**Logic:**

- If `artifact_dir` is None, return empty list
- For each slice where `total > len(preview)`:
  - Serialize full slice to JSON at `artifact_dir/slice_{kind}.json`
  - Compute SHA256 deterministic ID
  - Create `ArtifactPointerV1` with `storage_path`, `byte_size`, `deterministic_id`

**Progressive disclosure pattern:**

- Bundle contains only `preview` (first `top_k` items)
- Full slice is externalized to artifact
- Client can lazy-load full slice via artifact pointer

#### Subject Node Construction

**Function:** `_build_subject_node()` in `bundle_builder.py:519-570`

**Resolution priority:**

1. **Anchor-based**: If `target_file` and `target_line` are available, find innermost def matching span
2. **Name+file fallback**: If anchor fails, find def by name in target_file
3. **Generic fallback**: Build placeholder node with `kind="unknown"`

**Sort key for anchor matching:**

```python
candidates.sort(
    key=lambda record: (
        max(0, record.end_line - record.start_line),  # Prefer smallest span (innermost)
        record.start_line,
        record.start_col,
        record.text,
    )
)
subject = candidates[0]  # Innermost definition
```

#### Graph Summary

**Function:** `_build_graph_summary()` in `bundle_builder.py:586-598`

```python
def _build_graph_summary(
    structural: tuple[NeighborhoodSliceV1, ...],
    lsp: tuple[NeighborhoodSliceV1, ...],
) -> NeighborhoodGraphSummaryV1:
    total_nodes = sum(s.total for s in structural) + sum(s.total for s in lsp)
    total_edges = sum(len(s.edges) for s in structural) + sum(len(s.edges) for s in lsp)
    return NeighborhoodGraphSummaryV1(node_count=total_nodes, edge_count=total_edges)
```

#### Bundle Metadata

**Function:** `_build_meta()` in `bundle_builder.py:601-631`

Assembles `BundleMetaV1` with:

- `tool="cq"`, `workspace_root`, `created_at_ms` (duration)
- `query_text` — symbol_hint
- `lsp_servers` — If LSP enabled, include normalized capabilities + environment
- `limits` — `{"top_k": request.top_k}`

**LSP server metadata:**

```python
lsp_servers = (
    {
        "language": request.language,
        "position_encoding": lsp_env.get("lsp_position_encoding", "utf-16"),
        "workspace_health": lsp_env.get("lsp_health", "unknown"),
        "quiescent": bool(lsp_env.get("lsp_quiescent")),
        "capabilities": {
            "server_caps": normalized_caps.server_caps,
            "client_caps": normalized_caps.client_caps,
            "experimental_caps": normalized_caps.experimental_caps,
        },
    },
)
```

---

## 4. Section Layout System

**Location:** `tools/cq/neighborhood/section_layout.py` (473 LOC)

### SECTION_ORDER (17 Slots)

**Definition:** `section_layout.py:18-36`

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
    "lsp_deep_signals",     # 14 - collapsed: True
    "diagnostics",          # 15 - collapsed: True
    "suggested_followups",  # 16 - collapsed: False
    "provenance",           # 17 - collapsed: True
)
```

**Rationale:** Fixed ordering prevents drift from collector return order. Assembly uses a slot map keyed by slice kind, then emits sections in SECTION_ORDER.

### Collapse Rules

**Uncollapsed sections:**

```python
_UNCOLLAPSED_SECTIONS = frozenset({
    "target_tldr",
    "neighborhood_summary",
    "suggested_followups",
})
```

**Dynamic collapse sections:**

```python
_DYNAMIC_COLLAPSE_SECTIONS: dict[str, int] = {
    "parents": 3,           # Collapse if total > 3
    "enclosing_context": 1, # Collapse if total > 1
}
```

For all other sections, `collapsed=True` by default.

### BundleViewV1

**Definition:** `section_layout.py:94-107`

```python
class BundleViewV1(CqStruct, frozen=True):
    """Complete bundle view with deterministic section ordering."""

    key_findings: tuple[FindingV1, ...] = ()
    sections: tuple[SectionV1, ...] = ()
```

**FindingV1:**

```python
class FindingV1(CqStruct, frozen=True):
    """A key finding entry for quick reference."""

    category: str       # e.g., "target", "scope", "signature"
    label: str          # e.g., "Symbol", "Kind", "File"
    value: str          # e.g., "build_graph_product", "function", "graph.py"
```

**SectionV1:**

```python
class SectionV1(CqStruct, frozen=True):
    """A single section in the bundle view."""

    kind: str
    title: str
    items: tuple[str, ...] = ()
    collapsed: bool = True
    metadata: dict[str, object] | None = None
```

### materialize_section_layout()

**Function:** `section_layout.py:109-175`

**Signature:**

```python
def materialize_section_layout(
    bundle: SemanticNeighborhoodBundleV1,
) -> BundleViewV1
```

**Algorithm:**

1. Build slot map: `slot_map: dict[str, NeighborhoodSliceV1]` from `bundle.slices`
2. Collect key findings from `bundle.subject`
3. Build synthetic sections: `neighborhood_summary`
4. Emit slice-backed sections in `SECTION_ORDER`:
   - Skip synthetic kinds (handled separately)
   - Convert each slice to `SectionV1` via `_slice_to_section()`
5. Emit remaining slices not in `SECTION_ORDER` (fallback)
6. Emit diagnostics section (if `bundle.diagnostics`)
7. Emit follow-ups + provenance (always last)

**Example section rendering:**

```python
def _slice_to_section(slice_: NeighborhoodSliceV1) -> SectionV1:
    items: list[str] = []

    # Preview nodes
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

### Target TL;DR Findings

**Function:** `_build_target_findings()` in `section_layout.py:178-236`

Extracts key findings from subject node:

- Symbol name
- Kind (function, class, etc.)
- File path
- Signature (if available)
- Qualified name (if available)

### Neighborhood Summary Section

**Function:** `_build_summary_section()` in `section_layout.py:239-287`

Synthetic section with:

- Graph statistics (node count, edge count)
- Slice count and kinds
- Diagnostics summary (error/warning/info counts)

### Diagnostics Section

**Function:** `_build_diagnostics_section()` in `section_layout.py:335-370`

Formats `DegradeEventV1` with severity markers:

```python
severity_marker = {
    "error": "❌",
    "warning": "⚠️",
    "info": "ⓘ",
}.get(event.severity, "•")

item = f"{severity_marker} **{event.stage}** ({event.category})"
if event.message:
    item += f": {event.message}"
```

### Suggested Follow-ups Section

**Function:** `_build_followup_section()` in `section_layout.py:376-420`

Suggests:

- Exploring high-count relationships (threshold: 10)
- Investigating errors from diagnostics

### Provenance Section

**Function:** `_build_provenance_section()` in `section_layout.py:423-464`

Metadata:

- Tool name, version, workspace
- Creation duration
- LSP server count
- Schema version
- Bundle ID
- Artifact count

---

## 5. Capability Gating

**Location:** `tools/cq/neighborhood/capability_gates.py` (134 LOC)

### LspCapabilitySnapshotV1

**Source:** `tools/cq/search/rust_lsp_contracts.py`

Normalized capability snapshot with server/client/experimental caps.

### normalize_capability_snapshot()

**Function:** `capability_gates.py:16-60`

**Signature:**

```python
def normalize_capability_snapshot(
    capabilities: LspCapabilitySnapshotV1 | Mapping[str, object] | None,
) -> LspCapabilitySnapshotV1
```

**Normalization logic:**

1. If already `LspCapabilitySnapshotV1`, return as-is
2. If `Mapping` with `server_caps` key, attempt `msgspec.convert()`
3. Otherwise, build `LspCapabilitySnapshotV1.server_caps` from camelCase/snake_case keys:
   - `definitionProvider` / `definition_provider` → `definition_provider`
   - `typeDefinitionProvider` / `type_definition_provider` → `type_definition_provider`
   - etc.

**Helper:**

```python
def _provider_enabled(mapping: Mapping[str, object], camel_key: str) -> bool:
    if camel_key in mapping:
        return bool(mapping.get(camel_key))
    snake_key = _camel_to_snake(camel_key)
    return bool(mapping.get(snake_key))
```

### plan_feasible_slices()

**Function:** `capability_gates.py:63-90`

**Signature:**

```python
def plan_feasible_slices(
    requested_slices: tuple[NeighborhoodSliceKind, ...],
    capabilities: LspCapabilitySnapshotV1 | Mapping[str, object] | None,
    *,
    stage: str = "lsp.planning",
) -> tuple[tuple[NeighborhoodSliceKind, ...], tuple[DegradeEventV1, ...]]
```

**Algorithm:**

1. Normalize capabilities to `LspCapabilitySnapshotV1`
2. For each requested slice kind:
   - Lookup predicate in `_SLICE_CAPABILITY_REQUIREMENTS`
   - If no predicate (unconditional), add to feasible
   - If predicate passes, add to feasible
   - Otherwise, emit `DegradeEventV1` with `severity="info"`, `category="unavailable"`
3. Return feasible slices + degradation events

**Capability requirements:**

```python
_SLICE_CAPABILITY_REQUIREMENTS: dict[NeighborhoodSliceKind, SlicePredicate] = {
    "references": lambda snapshot: snapshot.server_caps.references_provider,
    "implementations": lambda snapshot: snapshot.server_caps.implementation_provider,
    "type_supertypes": _supports_type_hierarchy,
    "type_subtypes": _supports_type_hierarchy,
}

def _supports_type_hierarchy(snapshot: LspCapabilitySnapshotV1) -> bool:
    server = snapshot.server_caps
    return server.type_hierarchy_provider or server.type_definition_provider
```

---

## 6. Target Resolution

**Location:** `tools/cq/neighborhood/target_resolution.py` (372 LOC)

### TargetSpec

**Definition:** `target_resolution.py:18-26`

Parsed target specification:

```python
class TargetSpec(CqStruct, frozen=True):
    """Parsed neighborhood target spec."""

    raw: str
    target_name: str | None = None
    target_file: str | None = None
    target_line: int | None = None
    target_col: int | None = None
```

### ResolvedTarget

**Definition:** `target_resolution.py:28-38`

Resolved target coordinates:

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

**Resolution kinds:**

- `anchor` — Resolved via file:line:col anchor
- `file_symbol` — Resolved via name+file
- `symbol_fallback` — Resolved via symbol-only (ambiguous)
- `file_anchor_unresolved` — Anchor failed, no name available
- `unresolved` — Complete resolution failure
- `invalid` — Invalid target specification

### parse_target_spec()

**Function:** `target_resolution.py:41-74`

**Signature:**

```python
def parse_target_spec(target: str) -> TargetSpec
```

**Parsing logic:**

1. `path/to/file.py:line:col` → file + line + col
2. `path/to/file.py:line` → file + line
3. `symbol_name` → name only

**Examples:**

- `"src/graph.py:42:4"` → `TargetSpec(target_file="src/graph.py", target_line=42, target_col=4)`
- `"src/graph.py:42"` → `TargetSpec(target_file="src/graph.py", target_line=42)`
- `"build_graph_product"` → `TargetSpec(target_name="build_graph_product")`

### resolve_target()

**Function:** `target_resolution.py:76-264`

**Signature:**

```python
def resolve_target(
    spec: TargetSpec,
    snapshot: ScanSnapshot,
    *,
    root: Path | None = None,
    allow_symbol_fallback: bool = True,
) -> ResolvedTarget
```

**Resolution priority:**

1. **Anchor-based** (file:line:col):
   - Find all defs in `target_file` containing `target_line`
   - Sort by span size (prefer innermost)
   - If `target_col` provided, filter by column bounds
   - Return first match as `resolution_kind="anchor"`
   - If multiple matches, emit `DegradeEventV1(severity="warning", category="ambiguous")`

2. **Name+file** (if anchor fails or name provided):
   - Find all defs in `target_file` matching `target_name`
   - Sort by source order (deterministic)
   - Return first match as `resolution_kind="file_symbol"`
   - If multiple matches, emit `DegradeEventV1(severity="warning", category="ambiguous")`

3. **Symbol-only** (if name+file fails and `allow_symbol_fallback=True`):
   - Find all defs matching `target_name` (any file)
   - Sort by source order (deterministic)
   - Return first match as `resolution_kind="symbol_fallback"`
   - If multiple matches, emit `DegradeEventV1(severity="warning", category="ambiguous")`

4. **Unresolved**:
   - Return `ResolvedTarget(resolution_kind="unresolved")` with error degradation

**Anchor candidate sort key:**

```python
def _anchor_sort_key(record: SgRecord) -> tuple[int, int, int, str]:
    span = max(0, record.end_line - record.start_line)
    return (
        span,               # Prefer smallest span (innermost)
        record.start_line,
        record.start_col,
        record.text,
    )
```

**Name extraction:**

```python
def _extract_name_from_text(text: str) -> str:
    raw = text.strip()
    if raw.startswith(("def ", "class ")):
        head = raw.split("(", 1)[0].strip()
        return head.split()[-1] if head else raw
    if "(" in raw:
        call_head = raw.split("(", 1)[0].strip()
        if "." in call_head:
            return call_head.split(".")[-1]
        return call_head
    return raw
```

**URI conversion:**

```python
def _to_uri(root: Path | None, file_path: str | None) -> str | None:
    if not file_path:
        return None
    candidate = Path(file_path)
    if root is not None and not candidate.is_absolute():
        candidate = root / candidate
    try:
        return candidate.resolve().as_uri()  # file:///absolute/path
    except ValueError:
        return None
```

---

## 7. ScanSnapshot Adapter

**Location:** `tools/cq/neighborhood/scan_snapshot.py` (134 LOC)

### Purpose

Decouples structural collector from private `ScanContext` internals in `query/executor.py`. Exposes only what neighborhood needs.

### ScanSnapshot

**Definition:** `scan_snapshot.py:20-68`

```python
class ScanSnapshot(CqStruct, frozen=True):
    """Public adapter exposing only what neighborhood needs from ScanContext."""

    def_records: tuple[SgRecord, ...] = ()
    call_records: tuple[SgRecord, ...] = ()
    interval_index: object = None       # Duck-typed IntervalIndex
    file_index: object = None           # Duck-typed per-file index
    calls_by_def: dict[str, tuple[SgRecord, ...]] = msgspec.field(default_factory=dict)
```

**Key:** `calls_by_def` is keyed by `"file:line:col"` string, not `SgRecord` object. This enables serialization.

### Factory Methods

#### from_scan_context()

**Signature:**

```python
@classmethod
def from_scan_context(cls, ctx: ScanContext) -> ScanSnapshot
```

Builds snapshot from a `ScanContext` (internal query executor context).

**Conversion:**

```python
calls_by_def_keyed: dict[str, tuple[SgRecord, ...]] = {
    _record_key(def_rec): tuple(calls) for def_rec, calls in ctx.calls_by_def.items()
}
```

Where `_record_key(record)` returns `f"{record.file}:{record.start_line}:{record.start_col}"`.

#### from_records()

**Signature:**

```python
@classmethod
def from_records(cls, records: list[SgRecord]) -> ScanSnapshot
```

Builds snapshot from raw `SgRecord` list. Uses internal `_build_scan_context()` to construct indexes.

#### build_from_repo()

**Signature:**

```python
@classmethod
def build_from_repo(cls, root: Path, lang: str = "auto") -> ScanSnapshot
```

Recommended pattern: `sg_scan() → from_records()`.

**Example:**

```python
from tools.cq.neighborhood.scan_snapshot import ScanSnapshot

snapshot = ScanSnapshot.build_from_repo(Path("."), lang="python")
```

---

## 8. SNB Rendering

**Location:** `tools/cq/neighborhood/snb_renderer.py` (158 LOC)

### render_snb_result()

**Function:** `snb_renderer.py:21-46`

**Signature:**

```python
def render_snb_result(
    *,
    run: RunMeta,
    bundle: SemanticNeighborhoodBundleV1,
    target: str,
    language: str,
    top_k: int,
    enable_lsp: bool,
    lsp_env: Mapping[str, object] | None = None,
) -> CqResult
```

**Pipeline:**

1. Create `CqResult` from `RunMeta`
2. Materialize section layout: `view = materialize_section_layout(bundle)`
3. Populate summary: target, language, top_k, bundle_id, slice counts, graph stats
4. Populate findings: Convert `FindingV1` → `Finding`, `SectionV1` → `Section`
5. Populate artifacts: Convert `ArtifactPointerV1` → `Artifact`

**Summary fields:**

```python
result.summary["target"] = target
result.summary["language"] = language
result.summary["top_k"] = top_k
result.summary["enable_lsp"] = enable_lsp
result.summary["bundle_id"] = bundle.bundle_id
result.summary["total_slices"] = len(bundle.slices)
result.summary["total_diagnostics"] = len(bundle.diagnostics)
if bundle.subject is not None:
    result.summary["target_file"] = bundle.subject.file_path
    result.summary["target_name"] = bundle.subject.name
if bundle.graph is not None:
    result.summary["total_nodes"] = bundle.graph.node_count
    result.summary["total_edges"] = bundle.graph.edge_count
```

**Findings conversion:**

```python
for finding_v1 in view.key_findings:
    result.key_findings.append(
        Finding(
            category=finding_v1.category,
            message=f"{finding_v1.label}: {finding_v1.value}",
        )
    )

for section_v1 in view.sections:
    result.sections.append(
        Section(
            title=section_v1.title,
            collapsed=section_v1.collapsed,
            findings=[
                Finding(
                    category="neighborhood",
                    message=item,
                )
                for item in section_v1.items
            ],
        )
    )
```

**Evidence enrichment:**

Bundle metadata is added to `result.evidence` with `category="neighborhood_bundle"`:

```python
enrichment_payload: dict[str, object] = {
    "neighborhood_bundle": {
        "bundle_id": bundle.bundle_id,
        "subject_label": bundle.subject_label,
        "slice_count": len(bundle.slices),
        "diagnostic_count": len(bundle.diagnostics),
        "artifact_count": len(bundle.artifacts),
    },
    "degrade_events": [_degrade_event_dict(event) for event in bundle.diagnostics],
}
```

---

## 9. Pyrefly Adapter

**Location:** `tools/cq/neighborhood/pyrefly_adapter.py` (269 LOC)

### collect_pyrefly_slices()

**Function:** `pyrefly_adapter.py:20-145`

**Signature:**

```python
def collect_pyrefly_slices(
    *,
    root: Path,
    target_file: str,
    target_line: int | None,
    target_col: int | None,
    target_name: str,
    feasible_slices: tuple[NeighborhoodSliceKind, ...],
    top_k: int,
    symbol_hint: str | None = None,
) -> tuple[tuple[NeighborhoodSliceV1, ...], tuple[DegradeEventV1, ...], dict[str, object]]
```

**Validation:**

```python
if not target_file or target_line is None:
    return (
        (),
        (
            DegradeEventV1(
                stage="lsp.pyrefly",
                severity="warning",
                category="missing_anchor",
                message="Pyrefly neighborhood slices require a resolved file:line target",
            ),
        ),
        {},
    )
```

**LSP call:**

```python
payload = enrich_with_pyrefly_lsp(
    PyreflyLspRequest(
        root=root,
        file_path=file_path,
        line=target_line,
        col=max(0, target_col or 0),
        symbol_hint=symbol_hint or target_name,
    )
)
```

**Slice extraction:**

- `references` — `payload["local_scope_context"]["reference_locations"]`
- `implementations` — `payload["symbol_grounding"]["implementation_targets"]`
- `type_supertypes` — `payload["class_method_context"]["base_classes"]`
- `type_subtypes` — `payload["class_method_context"]["overriding_methods"]`

**Row conversion:**

```python
def _coerce_ref_rows(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
    rows = _nested_rows(payload, ("local_scope_context", "reference_locations"))
    normalized: list[Mapping[str, object]] = []
    for row in rows:
        file_value = row.get("file")
        line_value = row.get("line")
        col_value = row.get("col")
        if not isinstance(file_value, str):
            continue
        normalized.append(
            {
                "file": file_value,
                "line": line_value if isinstance(line_value, int) else 0,
                "col": col_value if isinstance(col_value, int) else 0,
                "kind": "reference",
                "symbol": file_value.split("/")[-1],
            }
        )
    return normalized
```

**LSP environment:**

```python
lsp_env = {
    "lsp_position_encoding": _nested_scalar(payload, ("coverage", "position_encoding")),
    "lsp_health": "ok",
    "lsp_quiescent": True,
}
```

---

## 10. CLI + Run Integration

### CLI Command

**Location:** `tools/cq/cli_app/commands/neighborhood.py` (109 LOC)

**Command definition:**

```python
@neighborhood_app.default
@nb_app.default
def neighborhood(
    target: Annotated[str, Parameter(help="Target location (file:line[:col] or symbol)")],
    *,
    lang: Annotated[str, Parameter(name="--lang", help="Query language")] = "python",
    top_k: Annotated[int, Parameter(name="--top-k", help="Max items per slice")] = 10,
    no_lsp: Annotated[bool, Parameter(name="--no-lsp", help="Disable LSP enrichment")] = False,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult
```

**Pipeline:**

```python
# 1. Scan repository
records = sg_scan(paths=[ctx.root], lang=lang, root=ctx.root)
snapshot = ScanSnapshot.from_records(records)

# 2. Parse and resolve target
spec = parse_target_spec(target)
resolved = resolve_target(spec, snapshot, root=ctx.root, allow_symbol_fallback=True)

# 3. Build bundle
request = BundleBuildRequest(
    target_name=resolved.target_name,
    target_file=resolved.target_file,
    target_line=resolved.target_line,
    target_col=resolved.target_col,
    target_uri=resolved.target_uri,
    root=ctx.root,
    snapshot=snapshot,
    language=lang,
    symbol_hint=resolved.symbol_hint,
    top_k=top_k,
    enable_lsp=not no_lsp,
    artifact_dir=ctx.artifact_dir,
    allow_symbol_fallback=True,
    target_degrade_events=resolved.degrade_events,
)
bundle = build_neighborhood_bundle(request)

# 4. Render to CqResult
run = mk_runmeta(macro="neighborhood", argv=ctx.argv, root=str(ctx.root), ...)
result = render_snb_result(run=run, bundle=bundle, target=target, ...)
result.summary["target_resolution_kind"] = resolved.resolution_kind
return CliResult(result=result, context=ctx, filters=None)
```

**Usage:**

```bash
cq neighborhood src/graph.py:42:4 --lang python --top-k 10
cq nb build_graph_product --lang python --no-lsp
```

### Run Integration (NeighborhoodStep)

**Location:** `tools/cq/run/spec.py:106-113`

**Definition:**

```python
class NeighborhoodStep(RunStepBase, tag="neighborhood", frozen=True):
    """Run step describing a neighborhood query."""

    target: str
    lang: str = "python"
    top_k: int = 10
    no_lsp: bool = False
```

**Example run plan (TOML):**

```toml
[[steps]]
type = "neighborhood"
target = "src/graph.py:42:4"
lang = "python"
top_k = 15
no_lsp = false
```

**Execution:** See `tools/cq/run/executor.py` for step dispatch logic (not covered in this doc — neighborhood step executes the same pipeline as CLI).

---

## 11. Typed Registry

**Location:** `tools/cq/core/snb_registry.py` (100 LOC)

### DETAILS_KIND_REGISTRY

**Definition:** `snb_registry.py:18-27`

```python
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
```

### resolve_kind()

**Function:** `snb_registry.py:30-43`

```python
def resolve_kind(kind: str) -> type[msgspec.Struct] | None:
    """Resolve kind string to struct type for runtime validation."""
    return DETAILS_KIND_REGISTRY.get(kind)
```

### validate_finding_details()

**Function:** `snb_registry.py:46-65`

```python
def validate_finding_details(details: dict[str, object]) -> bool:
    """Validate that finding details dict has valid kind and structure."""
    kind = details.get("kind")
    if not isinstance(kind, str):
        return False
    struct_type = resolve_kind(kind)
    return struct_type is not None
```

### decode_finding_details()

**Function:** `snb_registry.py:68-92`

```python
def decode_finding_details(details: dict[str, object]) -> msgspec.Struct | None:
    """Decode finding details dict into typed struct."""
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

**Usage:**

```python
details = {"kind": "cq.snb.node.v1", "node_id": "...", ...}
if validate_finding_details(details):
    node = decode_finding_details(details)
    assert isinstance(node, SemanticNodeRefV1)
```

---

## 12. Architectural Observations

### Design Rationale

#### 1. Decoupling via ScanSnapshot

The `ScanSnapshot` adapter decouples the neighborhood subsystem from the query executor's internal `ScanContext`. This prevents leakage of private implementation details and enables independent testing of structural collection.

**Trade-off:**

- **Pro:** Clean separation of concerns, easier to test structural collector in isolation
- **Con:** Adds conversion overhead (ScanContext → ScanSnapshot)

#### 2. Capability-Gated LSP Enrichment

The capability gating system (`plan_feasible_slices()`) allows graceful degradation when LSP servers lack required capabilities. This prevents hard failures when:

- LSP server doesn't support `textDocument/references`
- LSP server doesn't support `textDocument/typeHierarchy`

**Design tension:**

- **Conservative gating** (current): Only request slices the server explicitly supports → fewer failures, but may miss slices from partial implementations
- **Optimistic fallback**: Request all slices, catch failures → more failures, but may discover partial support

**Current policy:** Conservative. Explicit capability checks prevent wasted LSP calls.

#### 3. 17-Slot Deterministic Section Layout

The fixed `SECTION_ORDER` prevents drift from collector return order. This is critical for LLM consumption because:

- LLMs benefit from consistent section ordering across queries
- Deterministic ordering enables diff-based caching
- Fixed order enforces a "reading contract" (target → summary → structure → references → diagnostics)

**Improvement vectors:**

- **User-configurable section order** — Allow `.cq.toml` to override `SECTION_ORDER`
- **Conditional section emission** — Skip empty sections entirely (currently they're still emitted with `items=()`)

#### 4. 4-Phase Assembly Pipeline

The pipeline is split into 4 sequential phases:

1. Structural (AST-based)
2. LSP enrichment (capability-gated)
3. Merge and deduplication
4. Artifact split

**Why not parallel execution?**

- **Phase 1** must complete before Phase 2 (LSP needs target anchor from structural resolution)
- **Phase 2** depends on Phase 1 (subject node must be resolved)
- **Phase 3** depends on Phases 1+2 (all slices must be collected)
- **Phase 4** depends on Phase 3 (merged slices determine artifact split)

**Potential optimization:** Parallel LSP calls within Phase 2 (e.g., `references` and `implementations` can run concurrently). Currently not implemented.

#### 5. Progressive Disclosure via Artifact Pointers

Heavy slices (where `total > len(preview)`) are externalized to JSON artifacts. This prevents bloating the main bundle with thousands of references.

**Trade-off:**

- **Pro:** Keeps main bundle compact, enables lazy loading
- **Con:** Requires artifact storage (currently filesystem-only), no streaming support

**Extension point:** Replace `_store_artifacts_with_preview()` with object-store adapter (S3, GCS, etc.).

#### 6. Anchor-First Target Resolution

The target resolver prioritizes file:line:col anchors over symbol names. This is critical for:

- **Disambiguation** — Multiple definitions with same name in same file (e.g., nested functions)
- **Precision** — LSP requires exact position for hover/references

**Fallback hierarchy:**

1. Anchor (file:line:col) — most precise
2. Name+file — scoped to file
3. Symbol-only — ambiguous, requires warning

**Improvement vector:** Add column-aware resolution (currently `target_col` is advisory, not mandatory for anchor matching).

#### 7. Typed Degradation Events

`DegradeEventV1` provides structured failure tracking with:

- `stage` — Where failure occurred
- `severity` — Impact level (error/warning/info)
- `category` — Failure mode (timeout, unavailable, ambiguous)

**Rationale:** Enables programmatic filtering and diagnostic analysis. LLMs can suppress `severity="info"` events or highlight `severity="error"` events.

**Extension point:** Add `correlation_key` for grouping related events (e.g., all Pyrefly failures share same correlation key).

### Performance Characteristics

**Bottlenecks:**

- **sg_scan()** — Dominates execution time (200ms-2s for medium repos)
- **LSP enrichment** — 50-500ms per language server call
- **Interval index construction** — 10-50ms for 10k records

**Optimization opportunities:**

- **Scan caching** — Cache `ScanSnapshot` between neighborhood queries
- **Parallel LSP calls** — Run `references`, `implementations`, `type_hierarchy` concurrently
- **Lazy slice collection** — Only collect slices requested by user (not all 14 kinds)

### Extension Points

#### 1. New Slice Kinds

Add new `NeighborhoodSliceKind` literals:

- `imports` / `importers` — Import graph relationships
- `related` — "Similar code" via embedding distance
- `tests` — Test coverage for target symbol

**Requirements:**

- Add literal to `NeighborhoodSliceKind` in `snb_schema.py`
- Add collector function (structural or LSP-based)
- Add to `SECTION_ORDER` in `section_layout.py`
- Add capability predicate (if LSP-based) to `capability_gates.py`

#### 2. New Evidence Sources

Add new LSP adapters:

- `lsp.jedi` — Jedi LSP for Python
- `lsp.clangd` — clangd for C/C++
- `lsp.gopls` — gopls for Go

**Pattern:**

```python
def collect_jedi_slices(
    *,
    root: Path,
    target_file: str,
    target_line: int,
    ...
) -> tuple[tuple[NeighborhoodSliceV1, ...], tuple[DegradeEventV1, ...], dict[str, object]]:
    # Call Jedi LSP
    # Extract slices
    # Return (slices, degrades, lsp_env)
```

Register in `_collect_lsp_slices()` in `bundle_builder.py`.

#### 3. Artifact Storage Backends

Replace filesystem storage with object store:

- S3 / GCS / Azure Blob
- Redis (for ephemeral artifacts)
- Embedded SQLite (for local persistence)

**Interface:**

```python
class ArtifactStore(Protocol):
    def store(self, artifact_id: str, payload: bytes) -> str:
        """Store artifact, return storage path."""
        ...

    def retrieve(self, storage_path: str) -> bytes:
        """Retrieve artifact by storage path."""
        ...
```

#### 4. Custom Section Layouts

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

**Implementation:**

```python
def get_section_order(config: Config) -> tuple[str, ...]:
    custom_order = config.get("neighborhood.section_order.order")
    return tuple(custom_order) if custom_order else SECTION_ORDER
```

### Testing Recommendations

**Unit tests:**

- Target resolution edge cases (ambiguous symbols, column-aware anchoring)
- Capability gating logic (all 14 slice kinds)
- Section layout determinism (verify fixed order across runs)
- Degradation event accumulation (verify stage/severity/category)

**Integration tests:**

- Full pipeline (sg_scan → resolve → build → render)
- LSP adapter round-trips (Python/Rust)
- Artifact externalization (verify preview/full split)

**Golden tests:**

- CLI output snapshots for deterministic rendering
- Bundle serialization round-trip (msgspec → JSON → msgspec)

### Future Improvements

#### 1. Streaming Artifact Support

Replace batch artifact storage with streaming:

```python
def stream_artifacts(bundle: SemanticNeighborhoodBundleV1) -> Iterator[bytes]:
    # Yield artifacts as they're produced
    for slice_ in bundle.slices:
        if slice_.total > len(slice_.preview):
            yield serialize_slice(slice_)
```

#### 2. Incremental Bundle Updates

Cache bundles and update only changed slices:

```python
def update_bundle(
    old_bundle: SemanticNeighborhoodBundleV1,
    changed_files: set[str],
) -> SemanticNeighborhoodBundleV1:
    # Re-collect only slices affected by changed files
    ...
```

#### 3. Cross-Language Neighborhood

Support multi-language targets (e.g., Python calling Rust):

- Detect FFI boundaries (pyo3, cffi)
- Resolve cross-language call graphs
- Merge Python+Rust slices into unified bundle

#### 4. Graph Traversal Queries

Add query DSL for graph traversal:

```toml
[[steps]]
type = "neighborhood"
target = "src/graph.py:42"
expand = "callers(depth=2)"  # Transitive callers
```

**Implementation:** Recursively collect slices, merge into single bundle with depth metadata.

---

## Summary

The Neighborhood subsystem provides a robust, extensible framework for semantic neighborhood analysis. Its 4-phase assembly pipeline, capability-gated LSP enrichment, and deterministic section layout enable high-quality contextual bundles for LLM consumption. The system gracefully degrades when LSP servers are unavailable and provides structured diagnostics for programmatic filtering.

**Key strengths:**

- Deterministic output via fixed section ordering
- Graceful degradation via capability gating and typed events
- Progressive disclosure via artifact externalization
- Multi-language support (Python/Rust) with unified schema

**Key extension points:**

- New slice kinds (imports, tests, related code)
- New LSP adapters (Jedi, clangd, gopls)
- Custom section layouts via config
- Alternative artifact storage backends

**Performance bottlenecks:**

- Repository scanning (sg_scan)
- LSP enrichment calls
- Interval index construction

**Recommended next steps for contributors:**

1. Read `bundle_builder.py` for pipeline orchestration
2. Read `snb_schema.py` for canonical schema definitions
3. Read `target_resolution.py` for anchor/symbol resolution logic
4. Experiment with adding new slice kinds or LSP adapters
