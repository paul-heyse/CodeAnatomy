# 06 — Cross-Cutting Contracts & Orchestration

**Version:** 0.6.0
**Date:** 2026-02-15
**Scope:** System-wide contracts and orchestration primitives that span CQ subsystems

## Overview

This document defines the cross-cutting contracts, orchestration layers, and serialization infrastructure that enable coherent multi-subsystem integration across CQ. These primitives provide stable boundaries between subsystems while enabling rich data flow across language partitions, enrichment planes, and execution contexts.

**Target audience:** Advanced developers proposing architectural changes or integrating new CQ subsystems.

**Coverage:**
- Three-tier type system (serialized contracts, runtime-only objects, external handles)
- FrontDoor Insight V1 contract (primary cross-subsystem schema)
- Semantic Neighborhood Bundle (SNB) schema
- Multi-language orchestration and partition merging
- LSP integration layer and semantic contract state machine
- Enrichment facts system and code fact clusters
- Scoring system (impact/confidence/buckets)
- Core schema models (CqResult, Finding, Section, Anchor, DetailPayload, ScoreDetails)
- Typed boundary protocol (conversion, validation, error taxonomy)
- Contract codec (centralized serialization authority)
- Contract constraints (annotated types and validation policies)

**Out of scope:**
- CLI rendering (see doc 01)
- Search pipeline mechanics (see doc 02)
- Tree-sitter internals (see doc 07)
- Neighborhood schema details (see doc 08)

---

## Design Philosophy

### Serialization Boundary Protocol

CQ enforces a strict three-tier type system for module boundaries:

| Tier | Purpose | Base Class | Characteristics |
|------|---------|------------|-----------------|
| **Serialized Contracts** | Cross-module payloads | `CqStruct`, `CqOutputStruct`, `CqStrictOutputStruct`, `CqSettingsStruct`, `CqCacheStruct` | msgspec.Struct, frozen, kw_only, omit_defaults |
| **Runtime-Only Objects** | In-process state | dataclass or plain class | Not serialized |
| **External Handles** | Parser/cache references | None | Never crosses boundaries |

**Module:** `tools/cq/core/structs.py` (48 LOC)

```python
class CqStruct(msgspec.Struct, kw_only=True, frozen=True, omit_defaults=True):
    """Base struct for CQ internal data models."""

class CqSettingsStruct(msgspec.Struct, kw_only=True, frozen=True, omit_defaults=True, forbid_unknown_fields=True):
    """Base struct for serializable CQ settings/config contracts."""

class CqOutputStruct(msgspec.Struct, kw_only=True, frozen=True, omit_defaults=True):
    """Base struct for serializable CQ output/public contracts."""

class CqStrictOutputStruct(msgspec.Struct, kw_only=True, frozen=True, omit_defaults=True, forbid_unknown_fields=True):
    """Strict output boundary contract for persisted/external payloads."""

class CqCacheStruct(msgspec.Struct, kw_only=True, frozen=True, omit_defaults=True, forbid_unknown_fields=True):
    """Base struct for serializable CQ cache payload contracts."""

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
```

**Policy:**
- Use `msgspec.Struct` for serialized CQ contracts crossing module boundaries
- Keep parser/cache handles as runtime-only objects
- Avoid `pydantic` in CQ hot paths (`tools/cq/search`, `tools/cq/query`, `tools/cq/run`)
- All serialized contracts use `frozen=True`, `kw_only=True`, `omit_defaults=True`
- Settings/cache contracts add `forbid_unknown_fields=True` for strict validation

### Why msgspec Over Pydantic

| Aspect | msgspec | Pydantic |
|--------|---------|----------|
| Performance | 5-10x faster encoding/decoding | Slower validation overhead |
| Frozen structs | Native, efficient | Requires `frozen=True` on config |
| Validation | Structural, at boundaries | Model-based, pervasive |
| Type hints | Standard library | Custom types, validators |
| Serialization | Built-in JSON/msgpack/YAML | Requires `.dict()`, `.json()` |

**CQ Design Rationale:**
- Hot paths (search, enrichment, cache) need minimal overhead
- Structural validation at boundaries (not internal data flow)
- Deterministic serialization for cache keys and artifacts
- Compatibility with Rust FFI (msgpack boundary)

---

## FrontDoor Insight V1

**Module:** `tools/cq/core/front_door_insight.py` (1172 LOC)

The canonical front-door insight contract used by search, calls, and entity outputs. Provides concise target grounding, neighborhood previews, risk drivers, confidence, degradation status, budgets, and artifact references.

### Contract Schema

**Top-Level Structure:**

```python
class FrontDoorInsightV1(CqStruct, frozen=True):
    """Canonical front-door insight schema for search/calls/entity."""
    source: InsightSource                              # "search" | "calls" | "entity"
    target: InsightTargetV1                            # Selected target
    neighborhood: InsightNeighborhoodV1                # Neighborhood envelope
    risk: InsightRiskV1                                # Risk assessment
    confidence: InsightConfidenceV1                    # Confidence payload
    degradation: InsightDegradationV1                  # Degradation status
    budget: InsightBudgetV1                            # Budget limits
    artifact_refs: InsightArtifactRefsV1               # Artifact pointers
    schema_version: str = "cq.insight.v1"
```

**Type Aliases:**

```python
InsightSource = Literal["search", "calls", "entity"]
Availability = Literal["full", "partial", "unavailable"]
NeighborhoodSource = Literal["structural", "semantic", "heuristic", "none"]
RiskLevel = Literal["low", "med", "high"]
```

#### InsightTargetV1

```python
class InsightLocationV1(CqStruct, frozen=True):
    """Location payload for a selected target."""
    file: str = ""
    line: int | None = None
    col: int | None = None

class InsightTargetV1(CqStruct, frozen=True):
    """Primary target selected by a front-door command."""
    symbol: str
    kind: str = "unknown"
    location: InsightLocationV1 = InsightLocationV1()
    signature: str | None = None
    qualname: str | None = None
    selection_reason: str = ""
```

**Fields:**
- `symbol` - Target symbol name
- `kind` - Node kind (e.g., "function", "class", "import")
- `location` - File/line/col location
- `signature` - Function signature or type annotation
- `qualname` - Qualified name (Python: `module.Class.method`, Rust: `crate::module::Item`)
- `selection_reason` - Why this target was selected (e.g., "top_definition", "resolved_calls_target", "fallback_query")

#### InsightNeighborhoodV1

```python
class InsightSliceV1(CqStruct, frozen=True):
    """Preview-able neighborhood slice with provenance and availability."""
    total: int = 0
    preview: tuple[SemanticNodeRefV1, ...] = ()
    availability: Availability = "unavailable"
    source: NeighborhoodSource = "none"
    overflow_artifact_ref: str | None = None

class InsightNeighborhoodV1(CqStruct, frozen=True):
    """Neighborhood envelope used by the front-door card."""
    callers: InsightSliceV1 = InsightSliceV1()
    callees: InsightSliceV1 = InsightSliceV1()
    references: InsightSliceV1 = InsightSliceV1()
    hierarchy_or_scope: InsightSliceV1 = InsightSliceV1()
```

**Slice Fields:**
- `total` - Total count of items in this slice
- `preview` - Top N items (limited by `budget.preview_per_slice`)
- `availability` - `"full"` (complete), `"partial"` (incomplete), `"unavailable"` (missing)
- `source` - Provenance (`"structural"`, `"semantic"`, `"heuristic"`, `"none"`)
- `overflow_artifact_ref` - Reference to artifact containing full slice (when truncated)

**Slice Kind Mapping:**

| Insight Slice | SNB Slice Kinds |
|---------------|-----------------|
| `callers` | `{"callers"}` |
| `callees` | `{"callees"}` |
| `references` | `{"references", "imports", "importers"}` |
| `hierarchy_or_scope` | `{"parents", "children", "siblings", "enclosing_context", "implementations", "type_supertypes", "type_subtypes", "related"}` |

#### InsightRiskV1

```python
class InsightRiskCountersV1(CqStruct, frozen=True):
    """Deterministic risk counters for edit-surface evaluation."""
    callers: int = 0
    callees: int = 0
    files_with_calls: int = 0
    arg_shape_count: int = 0
    forwarding_count: int = 0
    hazard_count: int = 0
    closure_capture_count: int = 0

class InsightRiskV1(CqStruct, frozen=True):
    """Risk level + explicit drivers and counters."""
    level: RiskLevel = "low"
    drivers: tuple[str, ...] = ()
    counters: InsightRiskCountersV1 = InsightRiskCountersV1()
```

**Risk Level Thresholds:**

```python
_HIGH_CALLER_THRESHOLD = 10
_HIGH_CALLER_STRICT_THRESHOLD = 10
_MEDIUM_CALLER_THRESHOLD = 4
_MEDIUM_CALLER_STRICT_THRESHOLD = 3
_ARG_VARIANCE_THRESHOLD = 3
_FILES_WITH_CALLS_THRESHOLD = 3
```

**Risk Level Derivation:**

| Condition | Level |
|-----------|-------|
| `callers > 10` OR `hazard_count > 0` OR (`forwarding_count > 0` AND `callers > 0`) | `high` |
| `callers > 3` OR `arg_shape_count > 3` OR `files_with_calls > 3` OR `closure_capture_count > 0` | `med` |
| Otherwise | `low` |

**Risk Drivers:**

| Driver | Condition |
|--------|-----------|
| `high_call_surface` | `callers >= 10` |
| `medium_call_surface` | `callers >= 4` (and not high) |
| `argument_forwarding` | `forwarding_count > 0` |
| `dynamic_hazards` | `hazard_count > 0` |
| `arg_shape_variance` | `arg_shape_count > 3` |
| `closure_capture` | `closure_capture_count > 0` |

#### InsightConfidenceV1

```python
class InsightConfidenceV1(CqStruct, frozen=True):
    """Confidence payload used by card headline and machine parsing."""
    evidence_kind: str = "unknown"
    score: float = 0.0
    bucket: str = "low"
```

**Evidence Kinds:**
- `resolved_static_semantic` - Grounded via LSP/static-semantic provider
- `resolved_ast` - AST-based resolution
- `bytecode` - Bytecode analysis
- `resolved_ast_heuristic` - AST + heuristics
- `bytecode_heuristic` - Bytecode + heuristics
- `cross_file_taint` - Cross-file dataflow
- `heuristic` - Pattern matching
- `rg_only` - Regex-only search
- `unresolved` - No grounding

**Bucket Thresholds:**
- `high` - score >= 0.7
- `med` - score >= 0.4
- `low` - score < 0.4

#### InsightDegradationV1

```python
class InsightDegradationV1(CqStruct, frozen=True):
    """Compact degradation status for front-door rendering."""
    semantic: SemanticStatus = "unavailable"
    scan: str = "ok"
    scope_filter: str = "none"
    notes: tuple[str, ...] = ()
```

**Semantic Status Values:**
- `"unavailable"` - Provider not available
- `"skipped"` - Available but not attempted
- `"failed"` - Attempted but all requests failed
- `"partial"` - Some requests succeeded
- `"ok"` - All requests succeeded

**Scan Status Values:**
- `"ok"` - Normal execution
- `"timed_out"` - Scan timed out
- `"truncated"` - Results truncated

**Scope Filter Values:**
- `"none"` - No scope filtering applied
- `"dropped"` - Some results dropped by scope filter
- `"partial"` - Partial language coverage

**Notes:** Structured diagnostic messages (e.g., `"missing_languages=rust"`, `"dropped_by_scope={'python': 5}"`)

#### InsightBudgetV1

```python
class InsightBudgetV1(CqStruct, frozen=True):
    """Budget knobs used to keep front-door output bounded."""
    top_candidates: int = 3
    preview_per_slice: int = 5
    semantic_targets: int = 1
```

**Default Budgets:**

| Source | top_candidates | preview_per_slice | semantic_targets |
|--------|----------------|-------------------|------------------|
| search | min(3, actual_count) | 5 | 1 |
| calls | 3 | 5 | 1 |
| entity | 3 | 5 | 3 |

#### InsightArtifactRefsV1

```python
class InsightArtifactRefsV1(CqStruct, frozen=True):
    """Artifact references for offloaded diagnostic/detail payloads."""
    diagnostics: str | None = None
    telemetry: str | None = None
    neighborhood_overflow: str | None = None
```

**References:** Relative paths to `.ldmd`, `.json`, or other artifact files.

### Insight Assembly Pipeline

**Build Functions:**

```python
def build_search_insight(request: SearchInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Build search front-door insight payload."""

def build_calls_insight(request: CallsInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Build calls front-door insight payload."""

def build_entity_insight(request: EntityInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Build entity front-door insight payload."""
```

#### Search Insight

**Request Contract:**

```python
class SearchInsightBuildRequestV1(CqStruct, frozen=True):
    """Typed request contract for search insight assembly."""
    summary: dict[str, object]
    primary_target: Finding | None
    target_candidates: tuple[Finding, ...]
    neighborhood: InsightNeighborhoodV1 | None = None
    risk: InsightRiskV1 | None = None
    degradation: InsightDegradationV1 | None = None
    budget: InsightBudgetV1 | None = None
```

**Assembly Logic:**
1. Extract target from `primary_target` Finding or synthesize from `summary["query"]`
2. Derive confidence from target candidates
3. Use provided neighborhood or create empty
4. Derive risk from neighborhood counters if not provided
5. Build degradation from summary telemetry
6. Set budget based on target candidate count

**Target Selection Reason:** `"top_definition"` if primary_target exists, else `"fallback_query"`

#### Calls Insight

**Request Contract:**

```python
class CallsInsightBuildRequestV1(CqStruct, frozen=True):
    """Typed request contract for calls insight assembly."""
    function_name: str
    signature: str | None
    location: InsightLocationV1 | None
    neighborhood: InsightNeighborhoodV1
    files_with_calls: int
    arg_shape_count: int
    forwarding_count: int
    hazard_counts: dict[str, int]
    confidence: InsightConfidenceV1
    budget: InsightBudgetV1 | None = None
    degradation: InsightDegradationV1 | None = None
```

**Assembly Logic:**
1. Build target from function_name/signature/location
2. Build risk counters from neighborhood + hazard/forwarding/arg_shape counts
3. Derive risk level from counters
4. Add hazard drivers to risk
5. Use provided confidence/degradation/budget or defaults

**Target Selection Reason:** `"resolved_calls_target"`

#### Entity Insight

**Request Contract:**

```python
class EntityInsightBuildRequestV1(CqStruct, frozen=True):
    """Typed request contract for entity insight assembly."""
    summary: dict[str, object]
    primary_target: Finding | None
    neighborhood: InsightNeighborhoodV1 | None = None
    risk: InsightRiskV1 | None = None
    confidence: InsightConfidenceV1 | None = None
    degradation: InsightDegradationV1 | None = None
    budget: InsightBudgetV1 | None = None
```

**Assembly Logic:**
1. Extract target from primary_target or synthesize from summary
2. Use provided neighborhood or create empty
3. Derive risk from neighborhood counters if not provided
4. Use provided confidence or default to AST confidence
5. Use provided degradation or create empty

**Target Selection Reason:** `"top_entity_result"` if primary_target exists, else `"fallback_query"`

### Semantic Augmentation

**Function:** `augment_insight_with_semantic()` (lines 357-436)

Overlays static-semantic data (from LSP/static-semantic providers) on top of an existing insight:

```python
def augment_insight_with_semantic(
    insight: FrontDoorInsightV1,
    semantic_payload: dict[str, object],
    *,
    preview_per_slice: int | None = None,
) -> FrontDoorInsightV1:
    """Overlay static semantic data on top of an existing insight payload."""
```

**Augmentation Logic:**

1. **Call Graph Overlay:**
   - Extract `semantic_payload["call_graph"]["incoming_callers"]` → `neighborhood.callers`
   - Extract `semantic_payload["call_graph"]["outgoing_callees"]` → `neighborhood.callees`
   - Merge totals (max of structural and semantic)
   - Set `source="semantic"`

2. **Reference Count Overlay:**
   - Extract `semantic_payload["local_scope_context"]["reference_locations"]` → `neighborhood.references.total`
   - Fallback to `semantic_payload["symbol_grounding"]["references"]`

3. **Type Contract Overlay:**
   - Extract `semantic_payload["type_contract"]["callable_signature"]` → `target.signature`
   - Fallback to `semantic_payload["type_contract"]["resolved_type"]`

4. **Confidence Boost:**
   - Set `evidence_kind="resolved_static_semantic"` (if currently "unknown")
   - Set `score=max(current, 0.8)`
   - Set `bucket=max(current, "high")`

5. **Degradation Update:**
   - Set `semantic="ok"`

### Partial Language Marking

**Function:** `mark_partial_for_missing_languages()` (lines 648-684)

Marks insight slices partial when language partitions are missing:

```python
def mark_partial_for_missing_languages(
    insight: FrontDoorInsightV1,
    *,
    missing_languages: Sequence[str],
) -> FrontDoorInsightV1:
    """Mark insight slices partial when language partitions are missing."""
```

**Logic:**
1. Downgrade each slice's `availability` to `"partial"` if currently `"unavailable"` or `"full"`
2. Add `missing_languages={langs}` to degradation notes
3. Set `degradation.scope_filter = "partial"`

**Use Case:** Multi-language orchestration when some language partitions fail or are excluded by scope.

### Rendering

**Function:** `render_insight_card()` (lines 189-205)

Produces compact markdown card from insight payload:

```markdown
## Insight Card
- Target: **symbol_name** (kind) `file.py:line` `signature`
- Neighborhood: callers=N, callees=M, references=R, scope=S
  - Top callers: caller1, caller2, caller3
  - Top callees: callee1, callee2
- Risk: level=high; drivers=high_call_surface,dynamic_hazards; callers=15, callees=5, hazards=2, forwarding=1
- Confidence: evidence=resolved_static_semantic, score=0.95, bucket=high
- Degradation: semantic=ok, scan=ok, scope_filter=none
- Budget: top_candidates=3, preview_per_slice=5, semantic_targets=1
- Artifact Refs: diagnostics=path/to/diag.ldmd | telemetry=path/to/telemetry.json
```

**Rendering Helpers:**
- `_render_target_line()` - Symbol, kind, location, signature
- `_render_neighborhood_lines()` - Counts + top caller/callee previews
- `_render_risk_line()` - Level, drivers, counters
- `_render_confidence_line()` - Evidence, score, bucket
- `_render_degradation_line()` - Semantic/scan/scope status + notes
- `_render_budget_line()` - Budget limits
- `_render_artifact_refs_line()` - Artifact references

### Neighborhood Construction

**Function:** `build_neighborhood_from_slices()` (lines 299-354)

Maps structural neighborhood slices (SNB) into insight neighborhood schema:

```python
def build_neighborhood_from_slices(
    slices: Sequence[NeighborhoodSliceV1],
    *,
    preview_per_slice: int = 5,
    source: NeighborhoodSource = "structural",
    overflow_artifact_ref: str | None = None,
) -> InsightNeighborhoodV1:
    """Map structural neighborhood slices into insight neighborhood schema."""
```

**Mapping Logic:**
1. Collect slices by kind group (callers, callees, references, hierarchy)
2. Aggregate totals across matching slices
3. Deduplicate preview nodes by `node_id`
4. Truncate to `preview_per_slice` limit
5. Set `availability="full"` if total > 0, else `"partial"`
6. Attach `overflow_artifact_ref` if total > preview count

### Public Serialization

**Function:** `to_public_front_door_insight_dict()` (lines 1008-1050)

Serializes insight with explicit/full schema fields for public JSON output:

```python
def to_public_front_door_insight_dict(insight: FrontDoorInsightV1) -> dict[str, object]:
    """Serialize insight with explicit/full schema fields for public JSON output."""
```

**Use Case:** JSON API responses, artifact storage, external integrations.

### Consumers

| Consumer | Module | Usage |
|----------|--------|-------|
| **Search** | `tools/cq/search/pipeline/front_door_builder.py` | Builds insight from search results |
| **Calls** | `tools/cq/calls/front_door.py` | Builds insight from call graph analysis |
| **Entity** | `tools/cq/query/entity_front_door.py` | Builds insight from entity query results |
| **Neighborhood** | `tools/cq/neighborhood/semantic_neighborhood.py` | Attaches neighborhood slices to insight |

---

## Semantic Neighborhood Bundle (SNB) Schema

**Module:** `tools/cq/core/snb_schema.py` (322 LOC)

Canonical schema authority for Semantic Neighborhood Bundle artifacts. All SNB structures use frozen msgspec.Struct for deterministic serialization.

### Core Structures

**Artifact Pointer:**

```python
class ArtifactPointerV1(CqStruct, frozen=True):
    """Generic artifact pointer with deterministic identity."""
    artifact_kind: str
    artifact_id: str
    deterministic_id: str
    byte_size: int = 0
    storage_path: str | None = None
    metadata: dict[str, object] | None = None
```

**Degradation Event:**

```python
class DegradeEventV1(CqStruct, frozen=True):
    """Typed degradation event for structured failure tracking."""
    stage: str
    severity: Literal["info", "warning", "error"] = "warning"
    category: str = ""
    message: str = ""
    correlation_key: str | None = None
```

**Semantic Node Reference:**

```python
class SemanticNodeRefV1(CqStruct, frozen=True):
    """Reference to a semantic node in the neighborhood."""
    node_id: str
    kind: str
    name: str
    display_label: str = ""
    file_path: str = ""
    byte_span: tuple[int, int] | None = None
    signature: str | None = None
    qualname: str | None = None
```

**Neighborhood Slice:**

```python
class NeighborhoodSliceV1(CqStruct, frozen=True):
    """Single neighborhood slice with provenance and preview."""
    kind: str
    total: int = 0
    preview: tuple[SemanticNodeRefV1, ...] = ()
    overflow_artifact_ref: str | None = None
    source: str = "structural"
```

**Slice Kinds:**
- `callers`, `callees` - Call graph edges
- `references`, `imports`, `importers` - Reference relationships
- `parents`, `children`, `siblings` - Hierarchy relationships
- `enclosing_context` - Lexical scope
- `implementations` - Interface/trait implementations
- `type_supertypes`, `type_subtypes` - Type hierarchy
- `related` - Other relationships

---

## Multi-Language Orchestration

**Module:** `tools/cq/core/multilang_orchestrator.py` (485 LOC)

Provides partition dispatch, scope enforcement, and result merging for multi-language queries (Python + Rust).

### Scope Enforcement

**Extension-Authoritative Scope:**

| Language Scope | File Extensions | Enforcement |
|----------------|-----------------|-------------|
| `"python"` | `.py`, `.pyi` | Excludes `.rs` files |
| `"rust"` | `.rs` | Excludes `.py`, `.pyi` files |
| `"auto"` | `.py`, `.pyi`, `.rs` | Union of both |

**Implementation:** File filtering happens at scan time via language-specific include globs.

### Language Priority

**Function:** `language_priority()` (lines 32-40)

Returns deterministic ordering for scope:

```python
def language_priority(scope: QueryLanguageScope) -> dict[QueryLanguage, int]:
    """Return deterministic language ordering for a scope."""
    return {lang: idx for idx, lang in enumerate(expand_language_scope(scope))}
```

**Expanded Order:**
- `"python"` → `["python"]`
- `"rust"` → `["rust"]`
- `"auto"` → `["python", "rust"]` (Python first, Rust second)

### Partition Dispatch

**Function:** `execute_by_language_scope()` (lines 43-73)

Executes callback once per language in scope:

**Single-Language Fast Path:**
```python
if len(languages) == 1:
    only_language = languages[0]
    return {only_language: run_one(only_language)}
```

**Multi-Language Parallel Execution:**
```python
scheduler = get_worker_scheduler()
policy = scheduler.policy
if policy.query_partition_workers <= 1:
    # Sequential fallback
    return {lang: run_one(lang) for lang in languages}

# Parallel dispatch
futures = [scheduler.submit_io(run_one, lang) for lang in languages]
batch = scheduler.collect_bounded(
    futures,
    timeout_seconds=max(1.0, float(len(languages)) * 5.0),
)
if batch.timed_out > 0:
    # Fail-open to sequential
    return {lang: run_one(lang) for lang in languages}
return dict(zip(languages, batch.done, strict=False))
```

**Worker Pool:** Uses `tools.cq.core.runtime.worker_scheduler.get_worker_scheduler()` with `submit_io()` for I/O-bound tasks.

**Fail-Open:** Timeouts fall back to sequential execution.

### Partition Merging

**Function:** `merge_partitioned_items()` (lines 76-105)

Merges and sorts language-partitioned item lists with stable deterministic ordering:

**Sort Key:**
```python
merged.sort(
    key=lambda item: (
        priority.get(get_language(item), 99),  # Language priority
        -get_score(item),                      # Descending score
        *get_location(item),                   # (file, line, col) for stability
    )
)
```

**Guarantees:**
- Python results appear before Rust results (when `scope="auto"`)
- Within language partition, highest-scored items first
- Ties broken by file/line/col for determinism

### Result Merging

**Function:** `merge_language_cq_results()` (not shown, see multilang_orchestrator.py)

Merges per-language `CqResult` payloads into unified multi-language result.

**Input Contract:**

```python
class MergeResultsRequest(CqStruct, frozen=True):
    """Input contract for multi-language CQ result merge."""
    scope: QueryLanguageScope
    results: Mapping[QueryLanguage, CqResult]
    run: RunMeta
    diagnostics: Sequence[Finding] | None = None
    diagnostic_payloads: Sequence[Mapping[str, object]] | None = None
    language_capabilities: Mapping[str, object] | None = None
    summary_common: Mapping[str, object] | None = None
    include_section_language_prefix: bool = True
```

**Merge Logic:**

1. **Key Findings/Evidence:** Flatten from all partitions, inject `language` field, sort by priority/score/location
2. **Sections:** Optionally prefix section titles with `"{lang}: {title}"` (controlled by `include_section_language_prefix`)
3. **Artifacts:** Concatenate artifact lists
4. **Summary:** Build `multilang_summary` via `build_multilang_summary()` with per-language partition stats
5. **Semantic Telemetry:** Aggregate `python_semantic_telemetry` and `rust_semantic_telemetry` across partitions
6. **Diagnostics:** Aggregate `python_semantic_diagnostics` (deduplicated by repr hash)
7. **Semantic Planes:** Select first non-empty `semantic_planes` payload by language priority
8. **FrontDoor Insight:** Select grounded insight by priority, mark partial for missing languages

**FrontDoor Insight Selection:**

Prefers insights with grounded targets:
- Has `target.location.file` OR
- `target.kind` not in `{"query", "unknown", "entity"}`

Falls back to ungrounded insights if no grounded insights available.

Marks selected insight partial for missing languages via `mark_partial_for_missing_languages()`.

### Statistics Aggregation

**Semantic Telemetry Aggregation:**

Aggregates counters across language partitions:
- `attempted` - Total requests attempted
- `applied` - Total requests successfully applied
- `failed` - Total requests failed
- `skipped` - Total requests skipped
- `timed_out` - Total requests timed out

---

## LSP Integration Layer

**Module:** `tools/cq/search/semantic/models.py` (lines 1-150+)

Provides semantic provider types, state machine, enrichment contracts, and workspace root resolution for LSP-based enrichment.

### Semantic Provider Types

```python
SemanticProvider = Literal["python_static", "rust_static", "none"]
SemanticStatus = Literal["unavailable", "skipped", "failed", "partial", "ok"]
```

### Semantic Contract State Machine

**State Input:**

```python
class SemanticContractStateInputV1(CqStruct, frozen=True):
    """Input envelope for deterministic semantic contract-state derivation."""
    provider: SemanticProvider
    available: bool
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0
    reasons: tuple[str, ...] = ()
```

**State Output:**

```python
class SemanticContractStateV1(CqStruct, frozen=True):
    """Deterministic static-semantic state for front-door degradation semantics."""
    provider: SemanticProvider = "none"
    available: bool = False
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0
    status: SemanticStatus = "unavailable"
    reasons: tuple[str, ...] = ()
```

**Derivation Function:**

```python
def derive_semantic_contract_state(
    input_state: SemanticContractStateInputV1,
) -> SemanticContractStateV1:
    """Derive canonical semantic state from capability + attempt telemetry."""
```

**State Transitions:**

| Condition | Status |
|-----------|--------|
| `not available` | `"unavailable"` |
| `available and attempted <= 0` | `"skipped"` |
| `available and attempted > 0 and applied <= 0` | `"failed"` |
| `available and attempted > 0 and applied > 0 and failed > 0` | `"partial"` |
| `available and attempted > 0 and applied >= attempted and failed <= 0` | `"ok"` |

### Enrichment Request/Outcome

**Request:**

```python
class LanguageSemanticEnrichmentRequest(CqStruct, frozen=True):
    """Request envelope for language-aware front-door static semantic enrichment."""
    language: QueryLanguage
    mode: str
    root: Path
    file_path: Path
    line: int
    col: int
    symbol_hint: str | None = None
    run_id: str | None = None
```

**Outcome:**

```python
class LanguageSemanticEnrichmentOutcome(CqStruct, frozen=True):
    """Normalized static semantic enrichment result for front-door callers."""
    payload: dict[str, object] | None = None
    timed_out: bool = False
    failure_reason: str | None = None
    provider_root: Path | None = None
    macro_expansion_count: int | None = None
```

**Cache Payload:**

```python
class SemanticOutcomeCacheV1(CqStruct, frozen=True):
    """Serialized cache payload for front-door static semantic outcomes."""
    payload: dict[str, object] | None = None
    timed_out: bool = False
    failure_reason: str | None = None
```

### Request Budget

```python
class SemanticRequestBudgetV1(CqStruct, frozen=True):
    """Timeout and retry budget for one static semantic request envelope."""
    startup_timeout_seconds: float = 3.0
    probe_timeout_seconds: float = 1.0
    max_attempts: int = 2
    retry_backoff_ms: int = 100
```

### Workspace Root Resolution

**Python Root Markers:** `("pyproject.toml", "setup.cfg", "setup.py")`

**Rust Root Markers:** `("Cargo.toml",)`

**Resolution Logic:** Walk up directory tree from file until marker found or repo root reached.

### Capability Gating

All LSP/semantic enrichment is capability-gated and fail-open:
- Missing provider → `status="unavailable"`
- Provider crash → `status="failed"` with failure_reason
- Partial success → `status="partial"`

### Retry Logic

**Retry Policy:**
- Max attempts: 2
- Backoff: 100ms
- Retry on transient failures (connection errors, timeouts)
- No retry on semantic errors (invalid request, unsupported operation)

---

## Advanced Evidence Planes

**Modules:**
- `tools/cq/search/semantic/models.py` - Front-door semantic contracts
- `tools/cq/search/semantic_overlays.py` - Semantic tokens + inlay hints
- `tools/cq/search/diagnostics_pull.py` - Pull diagnostics normalization
- `tools/cq/search/refactor_actions.py` - Code-action resolve/execute bridge
- `tools/cq/search/rust_extensions.py` - Rust-analyzer macro/runnables extensions

**Status:** Implemented and integrated as of 2026-02-15. All planes are capability-gated and fail-open.

### Semantic Planes V2

**Version:** `cq.semantic_planes.v2`

**Planes:**
1. **Semantic Overlays** - Normalized semantic tokens + inlay hints
2. **Diagnostics Pull** - Shared `textDocument/diagnostic` + `workspace/diagnostic` normalization
3. **Refactor Actions** - Diagnostics + code-action resolve/execute helpers
4. **Rust Extensions** - Macro expansion + runnables

**Enrichment Integration:**

All planes enrich search/entity/neighborhood outputs without blocking core execution. Degradation events are tracked in `InsightDegradationV1.notes`.

---

## Enrichment Facts System

**Module:** `tools/cq/core/enrichment_facts.py` (595 LOC)

Canonical enrichment fact resolution helpers for markdown rendering. Provides structured code fact clusters with applicability filtering and N/A semantics.

### Schema

**Field Specification:**

```python
class FactFieldSpec(CqStruct, frozen=True):
    """Specification for a single code-fact row."""
    label: str
    paths: tuple[tuple[str, ...], ...]
    applicable_languages: frozenset[str] | None = None
    applicable_kinds: frozenset[str] | None = None
    fallback_reason: NAReason = "not_resolved"

NAReason = Literal["not_applicable", "not_resolved", "enrichment_unavailable"]
```

**Cluster Specification:**

```python
class FactClusterSpec(CqStruct, frozen=True):
    """Specification for a code-fact cluster."""
    title: str
    fields: tuple[FactFieldSpec, ...]
```

**Context:**

```python
class FactContext(CqStruct, frozen=True):
    """Language and node-kind context used for fact applicability."""
    language: str | None
    node_kind: str | None
```

**Resolved Output:**

```python
class ResolvedFact(CqStruct, frozen=True):
    """Resolved fact row payload for renderer output."""
    label: str
    value: object | None
    reason: NAReason | None = None

class ResolvedFactCluster(CqStruct, frozen=True):
    """Resolved fact cluster payload for renderer output."""
    title: str
    rows: tuple[ResolvedFact, ...]
```

### Fact Cluster Specifications

**Clusters:**

1. **Identity & Grounding**
   - Language, Symbol Role, Qualified Name, Binding Candidates
   - Definition/Declaration/Type Definition/Implementation Targets (Python-only)

2. **Type Contract**
   - Signature, Parameters, Return Type, Resolved Type (Python-only)
   - Generic Params (Python-only)
   - Async, Generator (Python-only)

3. **Scope & Visibility**
   - Scope Kind, Visibility (Rust-only), Module Path
   - Enclosing Function, Enclosing Class

4. **Behavior**
   - Is Async, Is Generator, Side Effects, Mutates Args

5. **Structure**
   - Attributes/Decorators, Base Classes, Struct Fields (Rust-only), Enum Variants (Rust-only)
   - LOC, Complexity

**Total Clusters:** 5

**Total Fields:** 40+

### Field Specification

**Path Resolution:**

Each field has multiple lookup paths (priority-ordered):

```python
FactFieldSpec(
    label="Signature",
    paths=(
        ("python_semantic", "type_contract", "callable_signature"),
        ("structural", "signature"),
        ("signature",),
    ),
)
```

**Path Lookup:** Walks nested dictionaries in order, returns first non-empty value.

### Applicability Filtering

**Function:** `_field_applicable()` (lines 1000-1007)

```python
def _field_applicable(*, field: FactFieldSpec, context: FactContext) -> bool:
    language_mismatch = field.applicable_languages is not None and (
        context.language is None or context.language not in field.applicable_languages
    )
    kind_mismatch = field.applicable_kinds is not None and (
        context.node_kind is None or context.node_kind not in field.applicable_kinds
    )
    return not (language_mismatch or kind_mismatch)
```

**Language-Specific Fields:**

| Field | Languages |
|-------|-----------|
| Definition Targets | `python` |
| Declaration Targets | `python` |
| Type Definition Targets | `python` |
| Implementation Targets | `python` |
| Resolved Type | `python` |
| Generic Params | `python` |
| Visibility | `rust` |
| Struct Fields | `rust` |
| Struct Field Count | `rust` |
| Enum Variants | `rust` |
| Enum Variant Count | `rust` |

**Kind-Specific Fields:**

| Field | Kinds |
|-------|-------|
| Signature | `function_like`, `class_like` |
| Parameters | `function_like` |
| Return Type | `function_like` |
| Async | `function_like` |
| Generator | `function_like` |
| Attributes/Decorators | `function_like`, `class_like` |
| Struct Fields | `struct_item` |
| Enum Variants | `enum_item` |

**Kind Categories:**

```python
_FUNCTION_LIKE_KINDS = frozenset({"function_definition", "decorated_definition", "function_item"})
_CLASS_LIKE_KINDS = frozenset({"class_definition", "struct_item", "enum_item", "trait_item", "impl_item"})
_IMPORT_LIKE_KINDS = frozenset({"import_statement", "import_from_statement", "use_declaration"})
```

### Resolution Pipeline

**1. Language Payload Extraction:**

```python
def resolve_primary_language_payload(
    payload: dict[str, object]
) -> tuple[str | None, dict[str, object] | None]:
    """Resolves primary enrichment language and payload."""
    # 1. Check top-level "language" field
    # 2. Check for nested language payloads
    # 3. Fall back to top-level payload
```

**2. Context Resolution:**

```python
def resolve_fact_context(
    *,
    language: str | None,
    language_payload: dict[str, object] | None
) -> FactContext:
    """Builds language/node-kind context."""
```

**3. Cluster Resolution:**

```python
def resolve_fact_clusters(
    *,
    context: FactContext,
    language_payload: dict[str, object] | None
) -> tuple[ResolvedFactCluster, ...]:
    """Resolves all fact clusters for a context."""
```

**4. Field Resolution:**

```python
def _resolve_field(
    *,
    field: FactFieldSpec,
    context: FactContext,
    language_payload: dict[str, object] | None
) -> ResolvedFact:
    """Resolves single fact field."""
    if language_payload is None:
        return ResolvedFact(label=field.label, value=None, reason="enrichment_unavailable")
    if not _field_applicable(field=field, context=context):
        return ResolvedFact(label=field.label, value=None, reason="not_applicable")

    for path in field.paths:
        found, value = _lookup_path(language_payload, path)
        if found and has_fact_value(value):
            return ResolvedFact(label=field.label, value=value, reason=None)
    return ResolvedFact(label=field.label, value=None, reason=field.fallback_reason)
```

### N/A Semantics

**Value Presence Check:**

```python
def has_fact_value(value: object) -> bool:
    """Returns True if value is considered present (not N/A)."""
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, dict, set)):
        return bool(value)
    return True
```

**N/A Reasons:**
- `"enrichment_unavailable"` - No enrichment payload available
- `"not_applicable"` - Field not applicable to language/kind
- `"not_resolved"` - Field applicable but value not found in payload

**Rendering:** Markdown renderer typically shows `N/A ({reason})` or hides row entirely.

### Additional Language Payload

**Function:** `additional_language_payload()` (lines 503-511)

Returns non-structured keys for additional facts rendering:

```python
def additional_language_payload(language_payload: dict[str, object] | None) -> dict[str, object]:
    """Returns non-structured keys for additional facts rendering."""
    if not isinstance(language_payload, dict):
        return {}
    return {
        key: value
        for key, value in language_payload.items()
        if key not in _STRUCTURED_KEYS and key != "language"
    }
```

**Structured Keys (excluded):** `{"meta", "resolution", "behavior", "structural", "parse_quality", "agreement", "python_semantic"}`

**Use Case:** Render unstandardized enrichment fields as "Additional Facts" section.

---

## Scoring System

**Module:** `tools/cq/core/scoring.py` (251 LOC)

Provides standardized impact and confidence scoring for CQ findings.

### Impact Scoring

**Signal Schema:**

```python
class ImpactSignals(CqStruct, frozen=True):
    sites: int = 0           # Affected call/usage sites
    files: int = 0           # Affected files
    depth: int = 0           # Propagation depth (taint/impact)
    breakages: int = 0       # Breaking changes count
    ambiguities: int = 0     # Ambiguous cases count
```

**Scoring Function:**

```python
def impact_score(signals: ImpactSignals, severity: str | None = None) -> float:
    """Compute weighted impact score from signals."""
```

**Weighted Sum:**

| Signal | Weight | Normalization Denominator | Range |
|--------|--------|---------------------------|-------|
| `sites` | 45% | 100 | [0, 1] |
| `files` | 25% | 20 | [0, 1] |
| `depth` | 15% | 10 | [0, 1] |
| `breakages` | 10% | 10 | [0, 1] |
| `ambiguities` | 5% | 10 | [0, 1] |

**Severity Multipliers:**

| Severity | Multiplier |
|----------|------------|
| `error` | 1.5 |
| `warning` | 1.0 |
| `info` | 0.5 |

**Formula:**

```python
score = (
    (sites / 100) * 0.45 +
    (files / 20) * 0.25 +
    (depth / 10) * 0.15 +
    (breakages / 10) * 0.10 +
    (ambiguities / 10) * 0.05
) * severity_multiplier
```

### Confidence Scoring

**Signal Schema:**

```python
class ConfidenceSignals(CqStruct, frozen=True):
    evidence_kind: str = "unresolved"
```

**Scoring Function:**

```python
def confidence_score(signals: ConfidenceSignals) -> float:
    """Compute confidence score from evidence kind."""
```

**Evidence Kind Mappings:**

| Evidence Kind | Score |
|---------------|-------|
| `resolved_ast` | 0.95 |
| `bytecode` | 0.90 |
| `resolved_ast_heuristic` | 0.75 |
| `bytecode_heuristic` | 0.75 |
| `cross_file_taint` | 0.70 |
| `heuristic` | 0.60 |
| `rg_only` | 0.45 |
| `unresolved` | 0.30 |

### Bucket Classification

**Function:**

```python
def classify_bucket(score: float) -> str:
    """Classify score into bucket label."""
```

**Thresholds:**

| Bucket | Score Range |
|--------|-------------|
| `high` | >= 0.7 |
| `med` | >= 0.4 and < 0.7 |
| `low` | < 0.4 |

### Score Details Builder

**Functions:**

```python
def build_score_details(
    *,
    impact_signals: ImpactSignals | None = None,
    confidence_signals: ConfidenceSignals | None = None,
    severity: str | None = None,
) -> ScoreDetails:
    """Build complete score details from signals."""

def build_detail_payload(
    *,
    kind: str | None = None,
    score: ScoreDetails | None = None,
    data: dict[str, object] | None = None,
) -> DetailPayload:
    """Build complete detail payload with score and data."""
```

---

## Core Schema Models

**Module:** `tools/cq/core/schema.py` (494 LOC)

Defines structured output format for all CQ macros.

### CqResult

```python
class CqResult(msgspec.Struct):
    """Top-level result envelope for all CQ macros."""
    run: RunMeta
    summary: dict[str, object]
    key_findings: list[Finding] = msgspec.field(default_factory=list)
    evidence: list[Finding] = msgspec.field(default_factory=list)
    sections: list[Section] = msgspec.field(default_factory=list)
    artifacts: list[Artifact] = msgspec.field(default_factory=list)
    schema_version: str = SCHEMA_VERSION
```

**Fields:**
- `run` - Metadata about the CQ invocation
- `summary` - Unstructured summary payload (language telemetry, insight, etc.)
- `key_findings` - Top-priority findings (usually top definitions/targets)
- `evidence` - Supporting evidence findings
- `sections` - Logical groupings of findings with headings
- `artifacts` - References to saved artifacts
- `schema_version` - Schema version identifier

### Finding

```python
class Finding(msgspec.Struct):
    """A discrete analysis finding."""
    category: str
    message: str
    anchor: Anchor | None = None
    severity: Literal["info", "warning", "error"] = "info"
    details: DetailPayload = msgspec.field(default_factory=DetailPayload)
    stable_id: str | None = None
    execution_id: str | None = None
    id_taxonomy: str | None = None
```

**Fields:**
- `category` - Finding type (e.g., "call_site", "import", "exception")
- `message` - Human-readable description
- `anchor` - Source location (file/line/col)
- `severity` - `"info"`, `"warning"`, or `"error"`
- `details` - Structured data payload
- `stable_id` - Content-derived identity for semantic equivalence
- `execution_id` - Execution-scoped identity for run correlation
- `id_taxonomy` - Identifier taxonomy label

### Section

```python
class Section(msgspec.Struct):
    """A logical grouping of findings with a heading."""
    title: str
    findings: list[Finding] = msgspec.field(default_factory=list)
    collapsed: bool = False
```

### Anchor

```python
class Anchor(msgspec.Struct, frozen=True, omit_defaults=True):
    """Source code location anchor."""
    file: str
    line: Annotated[int, msgspec.Meta(ge=1)]
    col: int | None = None
    end_line: int | None = None
    end_col: int | None = None

    def to_ref(self) -> str:
        """Return file:line reference string."""
        return f"{self.file}:{self.line}"

    @classmethod
    def from_span(cls, span: SourceSpan) -> Anchor:
        """Create an Anchor from a SourceSpan."""
```

### ScoreDetails

```python
class ScoreDetails(msgspec.Struct, omit_defaults=True):
    """Scoring metadata for a finding."""
    impact_score: float | None = None
    impact_bucket: str | None = None
    confidence_score: float | None = None
    confidence_bucket: str | None = None
    evidence_kind: str | None = None
```

### DetailPayload

```python
class DetailPayload(msgspec.Struct, omit_defaults=True):
    """Structured details payload for findings."""
    kind: str | None = None
    score: ScoreDetails | None = None
    data: dict[str, object] = msgspec.field(default_factory=dict)

    @classmethod
    def from_legacy(cls, details: dict[str, object]) -> DetailPayload:
        """Convert legacy detail dicts into a structured payload."""

    def get(self, key: str, default: object | None = None) -> object | None:
        """Mapping-style get for detail payloads."""

    def to_legacy_dict(self) -> dict[str, object]:
        """Convert structured details back to legacy dict format."""
```

**Score Fields:** `impact_score`, `impact_bucket`, `confidence_score`, `confidence_bucket`, `evidence_kind`

**Mapping Protocol:** Supports `get()`, `__getitem__()`, `__setitem__()`, `__contains__()`

**Legacy Compatibility:** `from_legacy()` and `to_legacy_dict()` for backward compat

### RunMeta

```python
class RunMeta(msgspec.Struct):
    """Metadata about a cq invocation."""
    macro: str
    argv: list[str]
    root: str = ""
    version: str = ""
    timestamp: str = ""
    execution_id: str = ""
```

### Artifact

```python
class Artifact(msgspec.Struct):
    """A saved analysis artifact reference."""
    path: str
    format: str = "json"
```

---

## Typed Boundary Protocol

**Module:** `tools/cq/core/typed_boundary.py` (129 LOC)

Shared typed boundary conversion helpers and error taxonomy.

### Boundary Error Taxonomy

```python
class BoundaryDecodeError(RuntimeError):
    """Raised when payload conversion fails at CQ boundaries."""
```

**Use Cases:**
- Parsing external payloads (JSON/YAML/TOML/msgpack)
- Converting dictionaries to typed structs
- Validating contract schemas at module boundaries

### Conversion Functions

**Strict Conversion:**

```python
def convert_strict[T](
    payload: object,
    *,
    type_: type[T] | object,
    from_attributes: bool = False,
) -> T:
    """Convert payload with strict msgspec semantics.

    Raises:
        BoundaryDecodeError: If conversion fails.
    """
```

**Lax Conversion:**

```python
def convert_lax[T](
    payload: object,
    *,
    type_: type[T] | object,
    from_attributes: bool = False,
) -> T:
    """Convert payload with lax msgspec semantics (ignores extra fields).

    Raises:
        BoundaryDecodeError: If conversion fails.
    """
```

**Strict vs Lax:**
- Strict: Rejects unknown fields, strict type validation
- Lax: Ignores unknown fields, permissive type coercion

**Recommended Usage:**
- Strict: External payloads, API boundaries, cache validation
- Lax: Internal conversions, backward compatibility, exploratory parsing

### Format-Specific Decoders

**JSON:**

```python
def decode_json_strict[T](payload: bytes | str, *, type_: type[T] | object) -> T:
    """Decode JSON payload with strict schema validation."""
```

**TOML:**

```python
def decode_toml_strict[T](payload: bytes | str, *, type_: type[T] | object) -> T:
    """Decode TOML payload with strict schema validation."""
```

**YAML:**

```python
def decode_yaml_strict[T](payload: bytes | str, *, type_: type[T] | object) -> T:
    """Decode YAML payload with strict schema validation."""
```

**All Decoders:**
- Accept `bytes` or `str`
- Use `strict=True` validation
- Raise `BoundaryDecodeError` on failure

### Usage Examples

**Convert dict to struct (lax):**

```python
from tools.cq.core.typed_boundary import convert_lax
from tools.cq.core.front_door_insight import FrontDoorInsightV1

payload = {"source": "search", "target": {...}, ...}
insight = convert_lax(payload, type_=FrontDoorInsightV1)
```

**Decode JSON config (strict):**

```python
from tools.cq.core.typed_boundary import decode_json_strict
from tools.cq.core.config import CqConfig

with open(".cq.json", "rb") as f:
    config = decode_json_strict(f.read(), type_=CqConfig)
```

**Parse TOML plan (strict):**

```python
from tools.cq.core.typed_boundary import decode_toml_strict
from tools.cq.run.plan_schema import RunPlan

with open("plan.toml", "rb") as f:
    plan = decode_toml_strict(f.read(), type_=RunPlan)
```

---

## Contract Codec

**Module:** `tools/cq/core/contract_codec.py` (122 LOC)

Canonical contract codec and conversion helpers for CQ boundaries.

### Codec Singletons

```python
JSON_ENCODER = msgspec.json.Encoder(order="deterministic")
JSON_DECODER = msgspec.json.Decoder(strict=True)
JSON_RESULT_DECODER = msgspec.json.Decoder(type=CqResult, strict=True)
MSGPACK_ENCODER = msgspec.msgpack.Encoder()
MSGPACK_DECODER = msgspec.msgpack.Decoder(type=object)
MSGPACK_RESULT_DECODER = msgspec.msgpack.Decoder(type=CqResult)
```

### Core Encoding Functions

```python
def encode_json(value: object, *, indent: int | None = None) -> str:
    """Encode any contract payload to deterministic JSON."""

def encode_msgpack(value: object) -> bytes:
    """Encode payload to msgpack bytes."""

def to_contract_builtins(value: object) -> object:
    """Convert a CQ value to builtins with deterministic contract settings."""
    return msgspec.to_builtins(value, order="deterministic", str_keys=True)
```

### Core Decoding Functions

```python
def decode_json(payload: bytes | str) -> object:
    """Decode JSON payload to builtins value."""

def decode_json_result(payload: bytes | str) -> CqResult:
    """Decode JSON payload to typed CQ result."""

def decode_msgpack(payload: bytes | bytearray | memoryview) -> object:
    """Decode msgpack payload to builtins value."""

def decode_msgpack_result(payload: bytes | bytearray | memoryview) -> CqResult:
    """Decode msgpack payload to typed CQ result."""
```

### Contract Conversion Helpers

```python
def to_public_dict(value: msgspec.Struct) -> dict[str, object]:
    """Convert one msgspec Struct into mapping payload."""

def to_public_list(values: Iterable[msgspec.Struct]) -> list[dict[str, object]]:
    """Convert iterable of structs into mapping rows."""

def require_mapping(value: object) -> dict[str, object]:
    """Require mapping-shaped builtins payload.

    Automatically calls enforce_mapping_constraints() for validation.
    """
```

### Module Consolidation

**Canonical Authority:** `tools/cq/core/contract_codec.py` is the single source of truth for:
- msgspec encoder/decoder singletons
- Deterministic JSON/msgpack encoding
- Struct-to-dict conversion
- Typed decoding (CqResult)

**No Duplicates:** All other modules import from this module (not inline encoders/decoders).

---

## Contract Constraints

**Module:** `tools/cq/core/contracts_constraints.py` (not shown, inferred from contract_codec.py)

Provides annotated constraint types and validation policies for contract payloads.

### Annotated Constraint Types

**Examples:**

```python
from typing import Annotated
import msgspec

NonEmptyStr = Annotated[str, msgspec.Meta(min_length=1)]
PositiveInt = Annotated[int, msgspec.Meta(ge=1)]
BoundedFloat = Annotated[float, msgspec.Meta(ge=0.0, le=1.0)]
```

### Contract Constraint Policy

**Validation Function:**

```python
def enforce_mapping_constraints(payload: dict[str, object]) -> None:
    """Validate mapping payload constraints.

    Raises:
        ValueError: If constraints violated.
    """
```

**Usage:** `require_mapping()` automatically calls `enforce_mapping_constraints()`.

**Constraints:**
- Non-empty strings for required fields
- Non-negative integers for counts
- Bounded floats for scores (0.0-1.0)
- Enum validation for literal types

---

## Summary Envelope Contract

**Module:** `tools/cq/core/contracts.py` (124 LOC)

Provides summary envelope contracts and multi-language summary assembly.

### Schema

**Summary Build Request:**

```python
class SummaryBuildRequest(CqStruct, frozen=True):
    """Input contract for canonical multilang summary assembly."""
    lang_scope: QueryLanguageScope
    languages: Mapping[QueryLanguage, Mapping[str, object]]
    common: Mapping[str, object] | None = None
    language_order: tuple[QueryLanguage, ...] | None = None
    cross_language_diagnostics: Sequence[CrossLanguageDiagnostic | Mapping[str, object]] | None = None
    language_capabilities: LanguageCapabilities | Mapping[str, object] | None = None
    enrichment_telemetry: Mapping[str, object] | None = None
```

**Merge Results Request:**

```python
class MergeResultsRequest(CqStruct, frozen=True):
    """Input contract for multi-language CQ result merge."""
    scope: QueryLanguageScope
    results: Mapping[QueryLanguage, CqResult]
    run: RunMeta
    diagnostics: Sequence[Finding] | None = None
    diagnostic_payloads: Sequence[Mapping[str, object]] | None = None
    language_capabilities: Mapping[str, object] | None = None
    summary_common: Mapping[str, object] | None = None
    include_section_language_prefix: bool = True
```

**UUID Identity Contract:**

```python
class UuidIdentityContractV1(CqStruct, frozen=True):
    """Sortable UUID contract for CQ runtime identity fields."""
    run_id: str
    artifact_id: str
    cache_key_uses_uuid: bool = False
    run_uuid_version: int | None = None
    run_created_ms: int | None = None
```

### Builders

**Summary Contract Conversion:**

```python
def summary_contract_to_mapping(
    contract: SearchSummaryContract,
    *,
    common: Mapping[str, object] | None,
) -> dict[str, object]:
    """Serialize canonical search summary contract to mapping payload."""
```

**Mapping Requirement:**

```python
def require_mapping(value: object) -> dict[str, object]:
    """Return mapping payload or raise a deterministic contract error."""
```

**Contract to Builtins:**

```python
def contract_to_builtins(value: object) -> object:
    """Serialize a CQ contract object into builtins recursively."""
```

---

## Module Map

**Core Contracts:**

| Module | LOC | Purpose |
|--------|-----|---------|
| `tools/cq/core/structs.py` | 48 | Base struct types |
| `tools/cq/core/schema.py` | 494 | CqResult, Finding, Section, Anchor |
| `tools/cq/core/front_door_insight.py` | 1172 | FrontDoorInsightV1 schema |
| `tools/cq/core/snb_schema.py` | 322 | SNB schema authority |
| `tools/cq/core/contracts.py` | 124 | Summary envelope contracts |
| `tools/cq/core/contract_codec.py` | 122 | Serialization codec |
| `tools/cq/core/typed_boundary.py` | 129 | Conversion helpers |

**Orchestration:**

| Module | LOC | Purpose |
|--------|-----|---------|
| `tools/cq/core/multilang_orchestrator.py` | 485 | Multi-language dispatch/merge |
| `tools/cq/core/multilang_summary.py` | 213 | Multi-language summary builder |

**Scoring & Facts:**

| Module | LOC | Purpose |
|--------|-----|---------|
| `tools/cq/core/scoring.py` | 251 | Impact/confidence scoring |
| `tools/cq/core/enrichment_facts.py` | 595 | Code fact clusters |

**Semantic Integration:**

| Module | LOC | Purpose |
|--------|-----|---------|
| `tools/cq/search/semantic/models.py` | 150+ | Semantic contracts |

**Total Core Contract LOC:** ~3900+

---

## Cross-References

**Related Docs:**
- [01 - CLI & Output Contracts](./01_cli_output.md) - Rendering pipeline
- [02 - Search Pipeline](./02_search_pipeline.md) - Search mechanics
- [07 - Tree-Sitter Integration](./07_tree_sitter.md) - AST parsing
- [08 - Neighborhood Schema](./08_neighborhood.md) - SNB details

**Key Modules:**
- `tools/cq/core/` - Core contracts and orchestration
- `tools/cq/search/` - Search pipeline integration
- `tools/cq/neighborhood/` - Neighborhood assembly

---

## Summary

CQ's cross-cutting contracts provide:

1. **Three-Tier Type System** - Clear boundaries between serialized contracts, runtime objects, and external handles
2. **FrontDoor Insight V1** - Canonical cross-subsystem schema for search/calls/entity
3. **SNB Schema** - Deterministic semantic neighborhood representation
4. **Multi-Language Orchestration** - Partition dispatch, scope enforcement, deterministic merging
5. **LSP Integration** - State machine, enrichment contracts, fail-open semantics
6. **Enrichment Facts** - Structured code fact clusters with applicability filtering
7. **Scoring System** - Standardized impact/confidence signals and bucket classification
8. **Core Schema Models** - CqResult, Finding, Section, Anchor, DetailPayload, ScoreDetails
9. **Typed Boundaries** - Conversion helpers, error taxonomy, format-specific decoders
10. **Contract Codec** - Centralized serialization authority with deterministic encoding

**Design Principles:**
- msgspec for performance and determinism
- Frozen structs for immutability
- Fail-open for degraded enrichment
- Language-neutral contracts
- Deterministic ordering for stability

**Evolution Strategy:**
- Add new fields with defaults (backward compat)
- Never reorder fields (forward compat)
- Version schema with `schema_version` field
- Use `forbid_unknown_fields=True` for strict boundaries
