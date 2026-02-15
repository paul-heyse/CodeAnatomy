# 06 — Cross-Cutting Contracts & Orchestration

**Version:** 0.5.0
**Date:** 2026-02-15
**Scope:** System-wide contracts and orchestration primitives that span CQ subsystems

## Overview

This document defines the cross-cutting contracts, orchestration layers, and serialization infrastructure that enable coherent multi-subsystem integration across CQ. These primitives provide stable boundaries between subsystems while enabling rich data flow across language partitions, enrichment planes, and execution contexts.

**Target audience:** Advanced developers proposing architectural changes or integrating new CQ subsystems.

**Coverage:**
- FrontDoor Insight V1 contract (primary cross-subsystem schema)
- Multi-language orchestration and partition merging
- LSP integration layer and semantic contract state machine
- Advanced evidence planes (semantic overlays, diagnostics, refactor actions, Rust extensions)
- Enrichment facts system and code fact clusters
- Scoring system (impact/confidence/buckets)
- Core schema models (CqResult, Finding, Section, Anchor)
- Typed boundary protocol (conversion, validation, error taxonomy)
- Contract codec (centralized serialization authority)
- Contract constraints (annotated types and validation policies)
- Serialization infrastructure and module boundary protocol

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
| **Serialized Contracts** | Cross-module payloads | `CqStruct`, `CqOutputStruct`, `CqStrictOutputStruct`, `CqSettingsStruct` | msgspec.Struct, frozen, kw_only |
| **Runtime-Only Objects** | In-process state | dataclass or plain class | Not serialized |
| **External Handles** | Parser/cache references | None | Never crosses boundaries |

**Module:** `tools/cq/core/structs.py` (43 LOC)

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
```

**Contract Properties:**
- `kw_only=True` prevents positional argument confusion
- `frozen=True` ensures immutability for thread safety and caching
- `omit_defaults=True` produces compact JSON output
- `forbid_unknown_fields=True` (strict output/settings/cache) rejects invalid payloads

### Why msgspec Over Pydantic

**Pydantic is excluded from CQ hot paths** (`tools/cq/search`, `tools/cq/query`, `tools/cq/run`):

| Criterion | msgspec | Pydantic |
|-----------|---------|----------|
| **Serialization speed** | 10-50x faster | Baseline |
| **Runtime overhead** | Minimal (compiled) | Heavy (validation, model construction) |
| **Contract enforcement** | Strict, fail-fast | Implicit coercion masks violations |
| **Thread safety** | Frozen by default | Mutable unless configured |

**Pydantic remains acceptable for:**
- CLI configuration parsing (not performance-critical)
- External API boundaries where implicit validation is desired

---

## FrontDoor Insight V1

**Module:** `tools/cq/core/front_door_insight.py` (1,171 LOC)

FrontDoor Insight V1 is the **canonical cross-subsystem contract** for high-level code analysis results. It provides a unified schema for search, calls, and entity front-door outputs with concise target grounding, neighborhood previews, risk drivers, confidence metrics, degradation status, budgets, and artifact references.

### Contract Schema

#### Top-Level Structure

```python
class FrontDoorInsightV1(CqStruct, frozen=True):
    """Canonical front-door insight schema for search/calls/entity."""

    source: InsightSource                     # "search" | "calls" | "entity"
    target: InsightTargetV1                   # Selected target identity
    neighborhood: InsightNeighborhoodV1       # Caller/callee/reference previews
    risk: InsightRiskV1                       # Risk level + drivers + counters
    confidence: InsightConfidenceV1           # Evidence kind + score + bucket
    degradation: InsightDegradationV1         # Semantic/scan/scope degradation
    budget: InsightBudgetV1                   # Output truncation budgets
    artifact_refs: InsightArtifactRefsV1      # Overflow/diagnostic artifact refs
    schema_version: str = "cq.insight.v1"     # Schema evolution marker
```

**Type Aliases:**

```python
InsightSource = Literal["search", "calls", "entity"]
Availability = Literal["full", "partial", "unavailable"]
NeighborhoodSource = Literal["structural", "semantic", "heuristic", "none"]
RiskLevel = Literal["low", "med", "high"]
```

#### InsightTargetV1

Primary target selected by front-door command:

```python
class InsightTargetV1(CqStruct, frozen=True):
    symbol: str                              # Symbol name
    kind: str = "unknown"                    # Entity kind (function, class, etc.)
    location: InsightLocationV1              # File/line/col location
    signature: str | None = None             # Function/class signature
    qualname: str | None = None              # Fully qualified name
    selection_reason: str = ""               # Why this target was selected

class InsightLocationV1(CqStruct, frozen=True):
    file: str = ""
    line: int | None = None
    col: int | None = None
```

**Selection Reasons (observed):**
- `"top_definition"` - Highest-ranked definition in search results
- `"resolved_calls_target"` - Direct resolution in calls analysis
- `"top_entity_result"` - Top entity query match
- `"fallback_query"` - Query string used when no grounded target exists

#### InsightNeighborhoodV1

Neighborhood envelope with four slice categories:

```python
class InsightNeighborhoodV1(CqStruct, frozen=True):
    callers: InsightSliceV1 = InsightSliceV1()
    callees: InsightSliceV1 = InsightSliceV1()
    references: InsightSliceV1 = InsightSliceV1()
    hierarchy_or_scope: InsightSliceV1 = InsightSliceV1()

class InsightSliceV1(CqStruct, frozen=True):
    total: int = 0                           # Total count in slice
    preview: tuple[SemanticNodeRefV1, ...] = ()  # Top-K nodes for preview
    availability: Availability = "unavailable"   # Data availability status
    source: NeighborhoodSource = "none"          # Provenance (structural/semantic)
    overflow_artifact_ref: str | None = None     # LDMD ref for full slice
```

**Slice Mapping from NeighborhoodSliceV1:**

| Insight Slice | Source Slice Kinds |
|---------------|-------------------|
| `callers` | `{"callers"}` |
| `callees` | `{"callees"}` |
| `references` | `{"references", "imports", "importers"}` |
| `hierarchy_or_scope` | `{"parents", "children", "siblings", "enclosing_context", "implementations", "type_supertypes", "type_subtypes", "related"}` |

**Builder:** `build_neighborhood_from_slices()` (lines 298-354)

#### InsightRiskV1

Risk assessment with explicit drivers and counters:

```python
class InsightRiskV1(CqStruct, frozen=True):
    level: RiskLevel = "low"                 # Computed risk level
    drivers: tuple[str, ...] = ()            # Explicit risk drivers
    counters: InsightRiskCountersV1          # Deterministic risk counters

class InsightRiskCountersV1(CqStruct, frozen=True):
    callers: int = 0
    callees: int = 0
    files_with_calls: int = 0
    arg_shape_count: int = 0                 # Argument variance
    forwarding_count: int = 0                # Argument forwarding sites
    hazard_count: int = 0                    # Dynamic hazards (eval, exec, etc.)
    closure_capture_count: int = 0           # Closure variable captures
```

**Risk Derivation Logic:** `risk_from_counters()` (lines 702-728)

| Condition | Driver Label | Risk Contribution |
|-----------|-------------|-------------------|
| `callers >= 10` | `"high_call_surface"` | High |
| `callers >= 4` | `"medium_call_surface"` | Medium |
| `forwarding_count > 0` | `"argument_forwarding"` | Medium |
| `hazard_count > 0` | `"dynamic_hazards"` | High |
| `arg_shape_count > 3` | `"arg_shape_variance"` | Medium |
| `closure_capture_count > 0` | `"closure_capture"` | Medium |
| `files_with_calls > 3` | (implicit) | Medium |

**Risk Level Logic:**

```python
def _risk_level_from_counters(counters: InsightRiskCountersV1) -> RiskLevel:
    # High if: >10 callers OR hazards OR (forwarding + callers)
    if counters.callers > 10 or counters.hazard_count > 0 or (counters.forwarding_count > 0 and counters.callers > 0):
        return "high"
    # Med if: >3 callers OR arg variance OR >3 files OR closures
    if counters.callers > 3 or counters.arg_shape_count > 3 or counters.files_with_calls > 3 or counters.closure_capture_count > 0:
        return "med"
    return "low"
```

#### InsightConfidenceV1

Confidence assessment with evidence provenance:

```python
class InsightConfidenceV1(CqStruct, frozen=True):
    evidence_kind: str = "unknown"           # Evidence source classification
    score: float = 0.0                       # Confidence score [0.0, 1.0]
    bucket: str = "low"                      # Bucket label (low/med/high)
```

**Evidence Kinds (observed):**
- `"resolved_ast"` - AST-based resolution
- `"resolved_static_semantic"` - Static semantic resolution (Pyrefly/rust-analyzer)
- `"bytecode"` - Bytecode analysis
- `"heuristic"` - Pattern-based heuristic
- `"rg_only"` - Ripgrep text match only

**Builder:** `_confidence_from_findings()` (lines 747-765) selects best score/bucket from finding set.

#### InsightDegradationV1

Compact degradation status for diagnostic transparency:

```python
class InsightDegradationV1(CqStruct, frozen=True):
    semantic: SemanticStatus = "unavailable"  # Semantic provider status
    scan: str = "ok"                          # Scan completion status
    scope_filter: str = "none"                # Scope filter degradation
    notes: tuple[str, ...] = ()               # Freeform degradation notes
```

**Semantic Status Values:** `"unavailable" | "skipped" | "failed" | "partial" | "ok"`

**Scan Status Values:** `"ok" | "timed_out" | "truncated"`

**Scope Filter Values:** `"none" | "dropped" | "partial"`

**Builder:** `_degradation_from_summary()` (lines 768-813) derives from summary telemetry.

#### InsightBudgetV1

Output truncation budgets:

```python
class InsightBudgetV1(CqStruct, frozen=True):
    top_candidates: int = 3                  # Max target candidates in preview
    preview_per_slice: int = 5               # Max nodes per neighborhood slice
    semantic_targets: int = 1                # Max semantic enrichment targets
```

**Budget Presets:**
- Search: `top_candidates=min(3, candidate_count)`, `semantic_targets=1`
- Calls: `top_candidates=3`, `semantic_targets=1`
- Entity: `top_candidates=3`, `semantic_targets=3`

#### InsightArtifactRefsV1

References to offloaded diagnostic/overflow artifacts:

```python
class InsightArtifactRefsV1(CqStruct, frozen=True):
    diagnostics: str | None = None           # Diagnostic artifact ref (LDMD)
    telemetry: str | None = None             # Telemetry artifact ref
    neighborhood_overflow: str | None = None # Full neighborhood artifact ref
```

### Insight Assembly Pipeline

#### Search Insight

**Builder:** `build_search_insight()` (lines 438-482)

**Input Contract:**

```python
class SearchInsightBuildRequestV1(CqStruct, frozen=True):
    summary: dict[str, object]               # Search summary payload
    primary_target: Finding | None           # Top definition finding
    target_candidates: tuple[Finding, ...]   # All target candidates
    neighborhood: InsightNeighborhoodV1 | None = None
    risk: InsightRiskV1 | None = None
    degradation: InsightDegradationV1 | None = None
    budget: InsightBudgetV1 | None = None
```

**Assembly Logic:**
1. Extract target from `primary_target` or fall back to query string
2. Derive confidence from `target_candidates` findings
3. Use provided `neighborhood` or empty default
4. Use provided `risk` or derive from neighborhood totals
5. Build degradation from summary telemetry
6. Build budget based on candidate count

#### Calls Insight

**Builder:** `build_calls_insight()` (lines 485-522)

**Input Contract:**

```python
class CallsInsightBuildRequestV1(CqStruct, frozen=True):
    function_name: str
    signature: str | None
    location: InsightLocationV1 | None
    neighborhood: InsightNeighborhoodV1
    files_with_calls: int
    arg_shape_count: int
    forwarding_count: int
    hazard_counts: dict[str, int]            # Hazard type -> count mapping
    confidence: InsightConfidenceV1
    budget: InsightBudgetV1 | None = None
    degradation: InsightDegradationV1 | None = None
```

**Assembly Logic:**
1. Build target from `function_name`, `signature`, `location`
2. Build risk counters from neighborhood + hazard_counts + arg_shape_count + forwarding_count
3. Derive risk level and drivers from counters
4. Use provided confidence
5. Apply default calls budget

#### Entity Insight

**Builder:** `build_entity_insight()` (lines 525-564)

**Input Contract:**

```python
class EntityInsightBuildRequestV1(CqStruct, frozen=True):
    summary: dict[str, object]
    primary_target: Finding | None
    neighborhood: InsightNeighborhoodV1 | None = None
    risk: InsightRiskV1 | None = None
    confidence: InsightConfidenceV1 | None = None
    degradation: InsightDegradationV1 | None = None
    budget: InsightBudgetV1 | None = None
```

**Assembly Logic:**
1. Extract target from `primary_target` or fall back to entity kind
2. Use provided neighborhood or empty default
3. Derive risk from neighborhood if not provided
4. Use provided confidence or default to `resolved_ast` at 0.8 score
5. Apply entity-specific budget (semantic_targets=3)

### Semantic Augmentation

**Function:** `augment_insight_with_semantic()` (lines 356-435)

Overlays static semantic data on existing insight payload:

**Semantic Payload Fields:**

| Field | Usage | Target Field |
|-------|-------|--------------|
| `call_graph.incoming_callers` | Caller node refs | `neighborhood.callers.preview` |
| `call_graph.incoming_total` | Caller count | `neighborhood.callers.total` |
| `call_graph.outgoing_callees` | Callee node refs | `neighborhood.callees.preview` |
| `call_graph.outgoing_total` | Callee count | `neighborhood.callees.total` |
| `type_contract.callable_signature` | Signature string | `target.signature` |
| `type_contract.resolved_type` | Type string | `target.signature` (fallback) |
| (reference totals from various sources) | Reference count | `neighborhood.references.total` |

**Confidence Boost:**
- Evidence kind: `"resolved_static_semantic"` if previously `"unknown"`
- Score: `max(current_score, 0.8)`
- Bucket: `max(current_bucket, "high")`

**Degradation Update:**
- `semantic` status: `"ok"`

### Partial Language Marking

**Function:** `mark_partial_for_missing_languages()` (lines 647-683)

Marks insight slices as `"partial"` when language partitions are missing:

**Logic:**
1. Downgrade each slice's `availability` to `"partial"` if currently `"unavailable"` or `"full"`
2. Add `missing_languages={langs}` to degradation notes
3. Set `degradation.scope_filter = "partial"`

**Use Case:** Multi-language orchestration when some language partitions fail or are excluded by scope.

### Rendering

**Function:** `render_insight_card()` (lines 188-204)

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

### Consumers

| Consumer | Module | Usage |
|----------|--------|-------|
| **Search** | `tools/cq/search/pipeline/front_door_builder.py` | Builds insight from search results |
| **Calls** | `tools/cq/calls/front_door.py` | Builds insight from call graph analysis |
| **Entity** | `tools/cq/query/entity_front_door.py` | Builds insight from entity query results |
| **Neighborhood** | `tools/cq/neighborhood/semantic_neighborhood.py` | Attaches neighborhood slices to insight |

---

## Multi-Language Orchestration

**Module:** `tools/cq/core/multilang_orchestrator.py` (481 LOC)

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

**Function:** `language_priority()` (lines 31-39)

Returns deterministic ordering for scope:

```python
def language_priority(scope: QueryLanguageScope) -> dict[QueryLanguage, int]:
    """Return deterministic language ordering for a scope."""
    return {lang: idx for idx, lang in enumerate(expand_language_scope(scope))}
```

**Expanded Order:**
- `"python"` → `["python"]`
- `"rust"` → `["rust"]`
- `"auto"` → `["python", "rust"]`  (Python first, Rust second)

### Partition Dispatch

**Function:** `execute_by_language_scope()` (lines 42-72)

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
if policy.query_partition_workers <= 1:
    # Sequential fallback
    return {lang: run_one(lang) for lang in languages}

# Parallel dispatch
futures = [scheduler.submit_io(run_one, lang) for lang in languages]
batch = scheduler.collect_bounded(futures, timeout_seconds=max(1.0, float(len(languages)) * 5.0))
if batch.timed_out > 0:
    # Fail-open to sequential
    return {lang: run_one(lang) for lang in languages}
return dict(zip(languages, batch.done, strict=False))
```

**Worker Pool:** Uses `tools.cq.core.runtime.worker_scheduler.get_worker_scheduler()` with `submit_io()` for I/O-bound tasks.

**Fail-Open:** Timeouts fall back to sequential execution.

### Partition Merging

**Function:** `merge_partitioned_items()` (lines 75-104)

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

**Function:** `merge_language_cq_results()` (lines 319-451)

Merges per-language `CqResult` payloads into unified multi-language result:

**Merge Logic:**

1. **Key Findings/Evidence:** Flatten from all partitions, inject `language` field, sort by priority/score/location
2. **Sections:** Optionally prefix section titles with `"{lang}: {title}"` (controlled by `include_section_language_prefix`)
3. **Artifacts:** Concatenate artifact lists
4. **Summary:** Build `multilang_summary` via `build_multilang_summary()` with per-language partition stats
5. **Semantic Telemetry:** Aggregate `python_semantic_telemetry` and `rust_semantic_telemetry` across partitions
6. **Diagnostics:** Aggregate `python_semantic_diagnostics` (deduplicated by repr hash)
7. **Semantic Planes:** Select first non-empty `semantic_planes` payload by language priority
8. **FrontDoor Insight:** Select grounded insight by priority, mark partial for missing languages

**Input Contract:**

```python
class MergeResultsRequest(CqStruct, frozen=True):
    run: RunMeta                             # Run metadata
    scope: QueryLanguageScope                # Language scope
    results: Mapping[QueryLanguage, CqResult]  # Per-language results
    diagnostics: Sequence[Finding] | None = None
    diagnostic_payloads: list[dict[str, object]] | None = None
    summary_common: dict[str, object] | None = None
    language_capabilities: dict[str, dict[str, bool]] | None = None
    include_section_language_prefix: bool = False
```

**FrontDoor Insight Selection:**

```python
def _select_front_door_insight(scope, results):
    order = list(expand_language_scope(scope))
    by_language = _collect_insights_by_language(order, results)

    # Prefer grounded insights (have file location or non-fallback kind)
    selected = _select_ordered_insight(order, by_language, require_grounded=True)
    if selected is None:
        # Fall back to ungrounded
        selected = _select_ordered_insight(order, by_language, require_grounded=False)

    if selected is not None and missing_languages:
        selected = mark_partial_for_missing_languages(selected, missing_languages=missing_languages)
    return selected
```

**Grounded Insight Criteria:**
- Has `target.location.file` OR
- `target.kind` not in `{"query", "unknown", "entity"}`

### Statistics Aggregation

**Semantic Telemetry Aggregation:**

```python
def _aggregate_semantic_telemetry(results, *, key):
    aggregate = {"attempted": 0, "applied": 0, "failed": 0, "skipped": 0, "timed_out": 0}
    for result in results.values():
        telemetry = result.summary.get(key)
        if isinstance(telemetry, dict):
            for field in aggregate:
                aggregate[field] += int(telemetry.get(field, 0))
    return aggregate
```

**Diagnostics Deduplication:**

```python
def _aggregate_python_semantic_diagnostics(order, results):
    merged = []
    seen = set()
    for lang in order:
        rows = results[lang].summary.get("python_semantic_diagnostics")
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict):
                    key = repr(row)
                    if key not in seen:
                        seen.add(key)
                        merged.append(dict(row))
    return merged
```

---

## LSP Integration Layer

**Module:** `tools/cq/search/semantic/models.py` (507 LOC)

Provides shared LSP integration contracts, semantic contract state machine, and capability-gating patterns for Pyrefly (Python) and rust-analyzer (Rust) backends.

### Semantic Provider Types

```python
SemanticProvider = Literal["python_static", "rust_static", "none"]
SemanticStatus = Literal["unavailable", "skipped", "failed", "partial", "ok"]
```

**Provider-Language Mapping:**

```python
def provider_for_language(language: QueryLanguage | str) -> SemanticProvider:
    if language == "python":
        return "python_static"
    if language == "rust":
        return "rust_static"
    return "none"
```

### Semantic Contract State Machine

**State Model:**

```python
class SemanticContractStateV1(CqStruct, frozen=True):
    provider: SemanticProvider = "none"
    available: bool = False
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0
    status: SemanticStatus = "unavailable"
    reasons: tuple[str, ...] = ()
```

**State Derivation:** `derive_semantic_contract_state()` (lines 104-151)

```python
def derive_semantic_contract_state(input_state: SemanticContractStateInputV1) -> SemanticContractStateV1:
    # If provider unavailable -> "unavailable"
    if not input_state.available:
        return SemanticContractStateV1(provider=input_state.provider, available=False, status="unavailable", reasons=input_state.reasons)

    # If not attempted -> "skipped"
    if input_state.attempted <= 0:
        return SemanticContractStateV1(provider=input_state.provider, available=True, status="skipped", reasons=input_state.reasons)

    # If attempted but not applied -> "failed"
    if input_state.applied <= 0:
        return SemanticContractStateV1(provider=input_state.provider, available=True, attempted=input_state.attempted, failed=max(input_state.failed, input_state.attempted), timed_out=input_state.timed_out, status="failed", reasons=input_state.reasons)

    # Otherwise "ok" or "partial"
    status = "ok" if input_state.failed <= 0 and input_state.applied >= input_state.attempted else "partial"
    return SemanticContractStateV1(provider=input_state.provider, available=True, attempted=input_state.attempted, applied=input_state.applied, failed=input_state.failed, timed_out=input_state.timed_out, status=status, reasons=input_state.reasons)
```

**State Transition Table:**

| Condition | Status | Rationale |
|-----------|--------|-----------|
| `available == False` | `"unavailable"` | LSP server not available |
| `attempted <= 0` | `"skipped"` | Not attempted by design (budget/filter) |
| `applied <= 0` | `"failed"` | Attempted but all failed |
| `failed <= 0 && applied >= attempted` | `"ok"` | All attempted succeeded |
| Otherwise | `"partial"` | Some succeeded, some failed |

### Enrichment Request/Outcome

**Request Contract:**

```python
class LanguageSemanticEnrichmentRequest(CqStruct, frozen=True):
    language: QueryLanguage                  # "python" | "rust"
    mode: str                                # CQ command mode (search/calls/entity)
    root: Path                               # CQ command root
    file_path: Path                          # Target file
    line: int                                # Target line (1-indexed)
    col: int                                 # Target column (0-indexed)
    symbol_hint: str | None = None           # Optional symbol hint
    run_id: str | None = None                # Run correlation ID
```

**Outcome Contract:**

```python
class LanguageSemanticEnrichmentOutcome(CqStruct, frozen=True):
    payload: dict[str, object] | None = None  # Enrichment payload
    timed_out: bool = False                   # Timeout occurred
    failure_reason: str | None = None         # Failure reason
    provider_root: Path | None = None         # Resolved workspace root
    macro_expansion_count: int | None = None  # Rust macro expansions (if applicable)
```

**Public Outcome (for summary):**

```python
class SemanticOutcomeV1(CqOutputStruct, frozen=True):
    payload: dict[str, object] | None = None
    timed_out: bool = False
    failure_reason: str | None = None
    metadata: dict[str, object] = msgspec.field(default_factory=dict)
```

### Request Budget

**Budget Contract:**

```python
class SemanticRequestBudgetV1(CqStruct, frozen=True):
    startup_timeout_seconds: float = 3.0     # LSP server startup timeout
    probe_timeout_seconds: float = 1.0       # Per-request timeout
    max_attempts: int = 2                    # Retry count
    retry_backoff_ms: int = 100              # Backoff between retries
```

**Budget Presets by Mode:**

```python
def budget_for_mode(mode: str) -> SemanticRequestBudgetV1:
    if mode == "calls":
        return SemanticRequestBudgetV1(startup_timeout_seconds=2.5, probe_timeout_seconds=1.25, max_attempts=2, retry_backoff_ms=120)
    if mode == "entity":
        return SemanticRequestBudgetV1(startup_timeout_seconds=3.0, probe_timeout_seconds=1.25, max_attempts=2, retry_backoff_ms=120)
    # Default (search)
    return SemanticRequestBudgetV1(startup_timeout_seconds=3.0, probe_timeout_seconds=1.0, max_attempts=2, retry_backoff_ms=100)
```

### Workspace Root Resolution

**Function:** `resolve_language_provider_root()` (lines 410-425)

Finds nearest workspace root with language-specific markers:

**Python Markers:** `("pyproject.toml", "setup.cfg", "setup.py")`
**Rust Markers:** `("Cargo.toml",)`

**Resolution Logic:**
1. Normalize `command_root` and `file_path` (resolve symlinks)
2. Walk upward from `file_path.parent` to `command_root`
3. Return first directory containing language-specific marker
4. Fall back to `command_root` if no marker found

**Use Case:** Ensures LSP server operates at correct workspace boundary for module resolution.

### Capability Gating

**Runtime Enable/Disable:**

```python
def semantic_runtime_enabled() -> bool:
    """Return whether semantic enrichment is enabled for the current process."""
    raw = os.getenv("CQ_ENABLE_SEMANTIC_ENRICHMENT")
    if raw is None:
        return True  # Enabled by default
    return raw.strip().lower() not in {"0", "false", "no", "off"}
```

**Fail-Open Outcome:**

```python
def fail_open(reason: str) -> SemanticOutcomeV1:
    """Return a fail-open semantic outcome for graceful degradation."""
    return SemanticOutcomeV1(payload=None, timed_out=False, failure_reason=reason)
```

**Design Pattern:**
- All semantic enrichment is optional and fail-open
- Missing/failed semantic data degrades to structural-only analysis
- `semantic_runtime_enabled()` provides global kill switch via env var

### Retry Logic

**Function:** `call_with_retry()` (lines 328-349)

Retry wrapper with timeout backoff:

```python
def call_with_retry(fn: Callable[[], object], *, max_attempts: int, retry_backoff_ms: int) -> tuple[object | None, bool]:
    timed_out = False
    for attempt in range(max_attempts):
        try:
            return fn(), timed_out
        except TimeoutError:
            timed_out = True
            if attempt + 1 >= max_attempts:
                return None, timed_out
            if retry_backoff_ms > 0:
                time.sleep((retry_backoff_ms / 1000.0) * (attempt + 1))
        except (OSError, RuntimeError, ValueError, TypeError):
            return None, timed_out
    return None, timed_out
```

**Fail-Open Exceptions:** `OSError`, `RuntimeError`, `ValueError`, `TypeError`

---

## Advanced Evidence Planes

CQ implements four advanced evidence planes on top of LSP foundation. All planes are **capability-gated** and **fail-open** — base search/query/run execution is non-blocking.

**Implementation Status:** All four planes are **implemented** as of 2026-02-15.

### Semantic Planes V2

**Builder:** `build_static_semantic_planes()` (lines 276-325 in `semantic/models.py`)

Produces enriched semantic overlay from static enrichment payload:

**Schema:**

```python
{
    "version": "cq.semantic_planes.v2",
    "language": "python" | "rust",
    "counts": {
        "semantic_tokens": int,              # Token overlay count
        "locals": int,                       # Local scope symbol count
        "diagnostics": int,                  # Diagnostic count
        "injections": int                    # Injection count (Rust macros)
    },
    "preview": {
        "semantic_tokens": list[dict],       # Top semantic token samples
        "locals": list[dict],                # Top local scope samples
        "diagnostics": list[dict],           # Top diagnostic samples
        "injections": list[dict]             # Top injection samples (Rust)
    },
    "degradation": list[str],                # ["scope_resolution_partial", "parse_error"]
    "sources": list[str]                     # Enrichment source provenance
}
```

**Semantic Tokens Preview:** `_semantic_tokens_preview()` extracts `node_kind`, `item_role`, `scope_kind`, `signature` (truncated to 180 chars)

**Locals Preview:** `_locals_preview()` extracts `scope_chain` and `qualified_name_candidates`

**Diagnostics Preview:**
- Python: `_python_diagnostics()` extracts tree-sitter diagnostics + parse quality errors + degrade reasons
- Rust: `_rust_diagnostics()` extracts tree-sitter diagnostics + degrade events

**Injections Preview:** `_injections_preview()` extracts Rust macro invocations (`macro_name`)

**Degradation Signals:**
- `"scope_resolution_partial"` if `enrichment_status == "degraded"`
- `"parse_error"` if diagnostics present

### Semantic Overlays

**Not a standalone module** — semantic overlays are implemented via `build_static_semantic_planes()` above. The overlays include:

1. **Semantic Tokens:** Node kind, item role, scope kind classifications
2. **Inlay Hints:** Type hints, parameter hints (embedded in `locals` preview)

**Use Case:** Enriches search findings with LSP-derived semantic classifications for improved categorization.

### Diagnostics Pull

**Not a standalone module** — diagnostics are normalized and aggregated via `_python_diagnostics()` and `_rust_diagnostics()` in `build_static_semantic_planes()`.

**Sources:**
- Tree-sitter parse errors (`tree_sitter_diagnostics`)
- Pyrefly parse quality errors (`parse_quality.error_nodes`, `parse_quality.missing_nodes`)
- Rust-analyzer degrade events (`degrade_events`)
- Explicit degrade reasons (`degrade_reasons`, `degrade_reason`)

**Normalization:**

```python
{
    "kind": str,              # Diagnostic kind (tree_sitter, degrade_reason, etc.)
    "message": str,           # Diagnostic message
    "line": int | None,       # Optional line number
    "col": int | None         # Optional column number
}
```

**Aggregation:** Multi-language diagnostics are aggregated via `_aggregate_python_semantic_diagnostics()` in multilang orchestrator.

### Refactor Actions

**Not a standalone module** — refactor actions are embedded in semantic enrichment payloads but not yet exposed as a first-class plane in current implementation.

**Future Integration:** Code action resolve/execute bridge to support automated refactoring suggestions.

### Rust Extensions

**Macro Expansions:** Captured in `LanguageSemanticEnrichmentOutcome.macro_expansion_count` and injections preview.

**Runnables:** Not yet exposed in current schema.

**Future Integration:** Rust-analyzer runnables (tests, benchmarks, binaries) for executable context discovery.

---

## Enrichment Facts System

**Module:** `tools/cq/core/enrichment_facts.py` (596 LOC)

Provides canonical resolution of enrichment payloads into structured Code Facts clusters for markdown rendering.

### Schema

**Top-Level Resolution:**

```python
class FactContext(CqStruct, frozen=True):
    language: str | None                     # Resolved language (python/rust)
    node_kind: str | None                    # Node kind (function_definition, etc.)

class ResolvedFact(CqStruct, frozen=True):
    label: str                               # Fact label
    value: object | None                     # Resolved value
    reason: NAReason | None = None           # N/A reason if absent

class ResolvedFactCluster(CqStruct, frozen=True):
    title: str                               # Cluster title
    rows: tuple[ResolvedFact, ...]           # Resolved fact rows

NAReason = Literal["not_applicable", "not_resolved", "enrichment_unavailable"]
```

### Fact Cluster Specifications

**Cluster Catalog:** `FACT_CLUSTERS` (lines 84-416) defines 7 standard clusters:

| Cluster Title | Field Count | Purpose |
|--------------|-------------|---------|
| **Identity & Grounding** | 8 | Language, symbol role, qualified name, binding candidates, definition/declaration/type targets |
| **Type Contract** | 8 | Signature, resolved type, parameters, return type, generic params, async, generator, visibility, attributes |
| **Call Graph** | 2 | Incoming callers, outgoing callees |
| **Class/Method Context** | 8 | Enclosing class, base classes, overridden/overriding methods, struct fields, enum variants |
| **Local Scope Context** | 4 | Enclosing callable, same-scope symbols, nearest assignments, narrowing hints, reference locations |
| **Imports/Aliases** | 2 | Import alias chain, resolved import path |
| **Diagnostics** | 1 | Anchor diagnostics |
| **Neighborhood Bundle** | 7 | Bundle ID, slice count, diagnostic count, degrade events, semantic health, quiescent status, position encoding |

### Field Specification

**Field Spec Schema:**

```python
class FactFieldSpec(CqStruct, frozen=True):
    label: str                               # Human-readable label
    paths: tuple[tuple[str, ...], ...]       # JSON path alternatives
    applicable_languages: frozenset[str] | None = None  # Language filter
    applicable_kinds: frozenset[str] | None = None      # Node kind filter
    fallback_reason: NAReason = "not_resolved"          # Default N/A reason
```

**Example Field:**

```python
FactFieldSpec(
    label="Qualified Name",
    paths=(
        ("python_semantic", "symbol_grounding", "definition_targets"),
        ("resolution", "qualified_name_candidates"),
        ("qualified_name_candidates",),
    ),
)
```

**Path Resolution Logic:** Tries each path in order until a non-empty value is found.

### Applicability Filtering

**Function:** `_field_applicable()` (lines 532-539)

Filters fields by language and node kind:

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

**1. Language Payload Extraction:** `resolve_primary_language_payload()` (lines 430-456)

Resolves primary enrichment language and payload:

```python
def resolve_primary_language_payload(payload: dict[str, object]) -> tuple[str | None, dict[str, object] | None]:
    # 1. Check top-level "language" field
    language = payload.get("language") if isinstance(payload.get("language"), str) else None
    if language in ("python", "rust"):
        direct = payload.get(language)
        if isinstance(direct, dict) and direct:
            return language, direct

    # 2. Check for nested language payloads
    for key in ("python", "rust"):
        candidate = payload.get(key)
        if isinstance(candidate, dict) and candidate:
            return key, candidate

    # 3. Fall back to top-level payload
    return language, payload if isinstance(payload, dict) else None
```

**2. Context Resolution:** `resolve_fact_context()` (lines 459-474)

Builds language/node-kind context:

```python
def resolve_fact_context(*, language: str | None, language_payload: dict[str, object] | None) -> FactContext:
    if language_payload is None:
        return FactContext(language=language, node_kind=None)
    node_kind = _extract_node_kind(language_payload)
    return FactContext(language=language, node_kind=node_kind)
```

**Node Kind Extraction:** `_extract_node_kind()` (lines 561-583)

Priority-ordered extraction:
1. `("structural", "node_kind")`
2. `("resolution", "node_kind")`
3. `("node_kind",)`
4. Heuristic fallbacks:
   - `item_role in {"import", "from_import", "use_import"}` → `"import_statement"`
   - `call_target` present → `"call_expression"`
   - `signature` + `params` present → `"function_definition"`
   - `struct_fields` present → `"struct_item"`
   - `enum_variants` present → `"enum_item"`

**3. Cluster Resolution:** `resolve_fact_clusters()` (lines 477-500)

Resolves all fact clusters for a context:

```python
def resolve_fact_clusters(*, context: FactContext, language_payload: dict[str, object] | None) -> tuple[ResolvedFactCluster, ...]:
    clusters = []
    for cluster in FACT_CLUSTERS:
        rows = tuple(_resolve_field(field=field, context=context, language_payload=language_payload) for field in cluster.fields)
        clusters.append(ResolvedFactCluster(title=cluster.title, rows=rows))
    return tuple(clusters)
```

**Field Resolution:** `_resolve_field()` (lines 514-529)

```python
def _resolve_field(*, field: FactFieldSpec, context: FactContext, language_payload: dict[str, object] | None) -> ResolvedFact:
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

**Value Presence Check:** `has_fact_value()` (lines 419-427)

```python
def has_fact_value(value: object) -> bool:
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

**Module:** `tools/cq/core/scoring.py` (252 LOC)

Provides standardized impact and confidence scoring for CQ findings.

### Impact Scoring

**Signal Schema:**

```python
class ImpactSignals(CqStruct, frozen=True):
    sites: int = 0                           # Affected call/usage sites
    files: int = 0                           # Affected files
    depth: int = 0                           # Propagation depth (taint/impact)
    breakages: int = 0                       # Breaking changes count
    ambiguities: int = 0                     # Ambiguous cases count
```

**Scoring Function:** `impact_score()` (lines 90-138)

Weighted sum of normalized signals:

| Signal | Weight | Normalization Denominator | Range |
|--------|--------|---------------------------|-------|
| `sites` | 45% | 100 | [0, 1] |
| `files` | 25% | 20 | [0, 1] |
| `depth` | 15% | 10 | [0, 1] |
| `breakages` | 10% | 10 | [0, 1] |
| `ambiguities` | 5% | 10 | [0, 1] |

**Formula:**

```python
score = (
    0.45 * min(sites / 100, 1.0) +
    0.25 * min(files / 20, 1.0) +
    0.15 * min(depth / 10, 1.0) +
    0.10 * min(breakages / 10, 1.0) +
    0.05 * min(ambiguities / 10, 1.0)
)
```

**Severity Multiplier:**

| Severity | Multiplier |
|----------|-----------|
| `"error"` | 1.5x |
| `"warning"` | 1.0x |
| `"info"` | 0.5x |

**Final Score:** `min(max(score * multiplier, 0.0), 1.0)`

### Confidence Scoring

**Signal Schema:**

```python
class ConfidenceSignals(CqStruct, frozen=True):
    evidence_kind: str = "unresolved"        # Evidence classification
```

**Scoring Function:** `confidence_score()` (lines 141-154)

Direct mapping from evidence kind to score:

| Evidence Kind | Score |
|---------------|-------|
| `"resolved_ast"` | 0.95 |
| `"bytecode"` | 0.90 |
| `"resolved_ast_heuristic"` | 0.75 |
| `"bytecode_heuristic"` | 0.75 |
| `"cross_file_taint"` | 0.70 |
| `"heuristic"` | 0.60 |
| `"rg_only"` | 0.45 |
| `"unresolved"` | 0.30 |

### Bucket Classification

**Function:** `bucket()` (lines 157-174)

Converts score to categorical bucket:

| Score Range | Bucket |
|-------------|--------|
| `>= 0.7` | `"high"` |
| `>= 0.4` | `"med"` |
| `< 0.4` | `"low"` |

### Score Details Builder

**Function:** `build_score_details()` (lines 177-200)

Combines impact and confidence signals into `ScoreDetails` struct:

```python
def build_score_details(*, impact: ImpactSignals | None = None, confidence: ConfidenceSignals | None = None, severity: str | None = None) -> ScoreDetails | None:
    if impact is None and confidence is None:
        return None
    impact_value = impact_score(impact, severity=severity) if impact is not None else None
    confidence_value = confidence_score(confidence) if confidence is not None else None
    return ScoreDetails(
        impact_score=impact_value,
        impact_bucket=bucket(impact_value) if impact_value is not None else None,
        confidence_score=confidence_value,
        confidence_bucket=bucket(confidence_value) if confidence_value is not None else None,
        evidence_kind=confidence.evidence_kind if confidence is not None else None,
    )
```

### Detail Payload Builder

**Function:** `build_detail_payload()` (lines 203-224)

Constructs `DetailPayload` from scoring signals and arbitrary data:

```python
def build_detail_payload(*, data: Mapping[str, object] | None = None, score: ScoreDetails | None = None, scoring: Mapping[str, object] | None = None, kind: str | None = None) -> DetailPayload:
    if score is None and scoring is not None:
        score = _score_details_from_mapping(scoring)
    payload_data = dict(data) if data else {}
    return DetailPayload(kind=kind, score=score, data=payload_data)
```

**Legacy Compatibility:** `_score_details_from_mapping()` (lines 227-251) converts dict-based scoring to `ScoreDetails`.

---

## Core Schema Models

**Module:** `tools/cq/core/schema.py` (approx. 600+ LOC)

Defines the fundamental result structures used across all CQ macros.

### CqResult

Top-level result container for all CQ commands:

```python
class CqResult(msgspec.Struct):
    run: RunMeta                             # Run metadata
    key_findings: list[Finding] = []         # Primary findings
    evidence: list[Finding] = []             # Supporting evidence
    sections: list[Section] = []             # Grouped findings by category
    summary: dict[str, object] = {}          # Structured summary payload
    artifacts: list[Artifact] = []           # Saved artifact references
```

**Usage:**
- `key_findings` - High-importance results (definitions, primary matches)
- `evidence` - Lower-priority results (comments, string mentions)
- `sections` - Categorical groupings (imports, callsites, etc.)
- `summary` - Machine-readable metadata (counts, telemetry, insight, etc.)
- `artifacts` - References to LDMD/JSON/CSV outputs

### Finding

Individual analysis result (already covered in FrontDoor Insight context):

```python
class Finding(msgspec.Struct):
    category: str                            # "call_site", "import", "definition", etc.
    message: str                             # Human-readable description
    anchor: Anchor | None = None             # Source location
    severity: Literal["info", "warning", "error"] = "info"
    details: DetailPayload = msgspec.field(default_factory=DetailPayload)
    stable_id: str | None = None             # Deterministic semantic ID
    execution_id: str | None = None          # Run-correlated ID
    id_taxonomy: str | None = None           # ID interpretation taxonomy
```

### Section

Grouped findings under a heading:

```python
class Section(msgspec.Struct):
    title: str                               # Section heading
    findings: list[Finding] = []             # Findings in this section
    collapsed: bool = False                  # Render collapsed by default
```

**Common Titles:**
- `"Definitions"` - Symbol definitions
- `"Imports"` - Import statements
- `"Callsites"` - Function invocations
- `"References"` - Variable/attribute references
- `"Comments"` - Comment mentions
- `"Strings"` - String literal matches

### Anchor

Source location reference (already covered in FrontDoor Insight context):

```python
class Anchor(msgspec.Struct, frozen=True, omit_defaults=True):
    file: str                                # Relative file path
    line: Annotated[int, msgspec.Meta(ge=1)] # 1-indexed line
    col: int | None = None                   # 0-indexed column
    end_line: int | None = None              # Optional range end
    end_col: int | None = None               # Optional range end
```

### ScoreDetails

Scoring metadata (already covered in Scoring System):

```python
class ScoreDetails(msgspec.Struct, omit_defaults=True):
    impact_score: float | None = None
    impact_bucket: str | None = None         # "high" | "med" | "low"
    confidence_score: float | None = None
    confidence_bucket: str | None = None     # "high" | "med" | "low"
    evidence_kind: str | None = None         # Evidence classification
```

### DetailPayload

Flexible details container (already covered in Scoring System):

```python
class DetailPayload(msgspec.Struct, omit_defaults=True):
    kind: str | None = None                  # Detail kind
    score: ScoreDetails | None = None        # Scoring metadata
    data: dict[str, object] = {}             # Arbitrary key-value data
```

### RunMeta

Run metadata for correlation:

```python
class RunMeta(msgspec.Struct):
    macro: str                               # Command macro (search/calls/entity/etc.)
    argv: list[str]                          # Command-line arguments
    started_ms: int                          # Start timestamp (milliseconds since epoch)
    ended_ms: int | None = None              # End timestamp
    run_id: str                              # Unique run identifier
    version: str = SCHEMA_VERSION            # CQ schema version
```

### Artifact

Reference to saved artifact:

```python
class Artifact(msgspec.Struct):
    path: str                                # Relative path to artifact
    format: str = "json"                     # File format (json/csv/ldmd)
```

---

## Typed Boundary Protocol

**Module:** `tools/cq/core/typed_boundary.py` (129 LOC)

The typed boundary protocol provides centralized conversion helpers and error taxonomy for all CQ module boundaries. It enforces strict type validation at deserialization points and provides format-specific decoders for JSON, TOML, and YAML payloads.

### Boundary Error Taxonomy

**Exception Hierarchy:**

```python
class BoundaryDecodeError(RuntimeError):
    """Raised when payload conversion fails at CQ boundaries."""
```

All conversion failures (msgspec validation errors, decode errors, type errors) are normalized to `BoundaryDecodeError` for consistent error handling across module boundaries.

### Conversion Functions

**Strict Conversion:**

```python
def convert_strict[T](
    payload: object,
    *,
    type_: object,
    from_attributes: bool = False,
) -> T:
    """Convert payload with strict msgspec semantics.

    Raises:
        BoundaryDecodeError: If conversion fails.
    """
```

**Strict conversion** rejects:
- Extra fields in mappings (when struct uses `forbid_unknown_fields=True`)
- Type mismatches (no implicit coercion)
- Missing required fields

**Lax Conversion:**

```python
def convert_lax[T](
    payload: object,
    *,
    type_: object,
    from_attributes: bool = False,
) -> T:
    """Convert payload with lax msgspec semantics.

    Raises:
        BoundaryDecodeError: If conversion fails.
    """
```

**Lax conversion** tolerates:
- Extra fields in mappings (ignored)
- Some type coercions (int → float, etc.)
- Missing optional fields

**Use Cases:**

| Scenario | Use |
|----------|-----|
| External config files (TOML/YAML) | `convert_lax` |
| Internal contract boundaries | `convert_strict` |
| Cache payload deserialization | `convert_strict` |
| Legacy payload migration | `convert_lax` |

### Format-Specific Decoders

**JSON Decoder:**

```python
def decode_json_strict[T](payload: bytes | str, *, type_: object) -> T:
    """Decode JSON payload with strict schema validation.

    Raises:
        BoundaryDecodeError: If decoding fails.
    """
```

**TOML Decoder:**

```python
def decode_toml_strict[T](payload: bytes | str, *, type_: object) -> T:
    """Decode TOML payload with strict schema validation.

    Raises:
        BoundaryDecodeError: If decoding fails.
    """
```

**YAML Decoder:**

```python
def decode_yaml_strict[T](payload: bytes | str, *, type_: object) -> T:
    """Decode YAML payload with strict schema validation.

    Raises:
        BoundaryDecodeError: If decoding fails.
    """
```

**Design Pattern:**
- All decoders accept `bytes | str` payloads for convenience
- All decoders use strict validation by default
- All decoders normalize errors to `BoundaryDecodeError`
- String payloads are automatically encoded to UTF-8 bytes

### Usage Examples

**Strict Boundary Validation:**

```python
from tools.cq.core.typed_boundary import convert_strict, BoundaryDecodeError

try:
    config = convert_strict(raw_dict, type_=SearchConfig)
except BoundaryDecodeError as exc:
    # Handle invalid payload at boundary
    logger.error(f"Config validation failed: {exc}")
```

**Lax External Config:**

```python
from tools.cq.core.typed_boundary import decode_toml_strict

# TOML files use lax semantics by default
config = decode_toml_strict(toml_bytes, type_=RunPlan)
```

---

## Contract Codec

**Module:** `tools/cq/core/contract_codec.py` (104 LOC)

The contract codec module is the **single source of truth** for all CQ serialization operations. All other serialization modules (`serialization.py`, `codec.py`, `contracts.py`) delegate to this module.

### Codec Singletons

**JSON Codecs:**

```python
JSON_ENCODER = msgspec.json.Encoder(order="deterministic")
JSON_DECODER = msgspec.json.Decoder(strict=True)
JSON_RESULT_DECODER = msgspec.json.Decoder(type=CqResult, strict=True)
```

**MessagePack Codecs:**

```python
MSGPACK_ENCODER = msgspec.msgpack.Encoder()
MSGPACK_DECODER = msgspec.msgpack.Decoder(type=object)
MSGPACK_RESULT_DECODER = msgspec.msgpack.Decoder(type=CqResult)
```

**Design Pattern:**
- All encoders/decoders are module-level singletons (no per-call instantiation)
- JSON encoder uses deterministic key ordering for stable output
- JSON decoder uses strict validation by default
- Typed decoders (`JSON_RESULT_DECODER`, `MSGPACK_RESULT_DECODER`) provide fast-path for CqResult

### Core Encoding Functions

**JSON Encoding:**

```python
def encode_json(value: object, *, indent: int | None = None) -> str:
    """Encode any contract payload to deterministic JSON."""
```

**Encoding Pipeline:**
1. Convert value to builtins via `to_contract_builtins(value)`
2. Encode to JSON bytes with deterministic key ordering
3. Optionally pretty-print with `msgspec.json.format(payload, indent=indent)`
4. Decode bytes to UTF-8 string

**MessagePack Encoding:**

```python
def encode_msgpack(value: object) -> bytes:
    """Encode payload to msgpack bytes."""
```

### Core Decoding Functions

**JSON Decoding:**

```python
def decode_json(payload: bytes | str) -> object:
    """Decode JSON payload to builtins value."""

def decode_json_result(payload: bytes | str) -> CqResult:
    """Decode JSON payload to typed CQ result."""
```

**MessagePack Decoding:**

```python
def decode_msgpack(payload: bytes | bytearray | memoryview) -> object:
    """Decode msgpack payload to builtins value."""

def decode_msgpack_result(payload: bytes | bytearray | memoryview) -> CqResult:
    """Decode msgpack payload to typed CQ result."""
```

### Contract Conversion Helpers

**Builtins Conversion:**

```python
def to_contract_builtins(value: object) -> object:
    """Convert a CQ value to builtins with deterministic contract settings."""
```

**Conversion Policy:**
- Deterministic key ordering (`order="deterministic"`)
- String keys enforced (`str_keys=True`)
- Nested structs recursively converted to dicts
- Collections (list, tuple, set) preserved

**Struct-to-Dict:**

```python
def to_public_dict(value: msgspec.Struct) -> dict[str, object]:
    """Convert one msgspec Struct into mapping payload."""

def to_public_list(values: Iterable[msgspec.Struct]) -> list[dict[str, object]]:
    """Convert iterable of structs into mapping rows."""
```

**Mapping Validation:**

```python
def require_mapping(value: object) -> dict[str, object]:
    """Require mapping-shaped builtins payload."""
```

**Validation Pipeline:**
1. Convert value to builtins via `to_contract_builtins(value)`
2. Assert result is dict-shaped
3. Enforce mapping constraints via `enforce_mapping_constraints(payload)`
4. Return validated mapping

### Module Consolidation

**All serialization modules now delegate to contract_codec:**

| Module | Delegation Target |
|--------|------------------|
| `tools/cq/core/serialization.py` | `encode_json`, `decode_json_result`, `encode_msgpack`, `decode_msgpack`, `decode_msgpack_result`, `to_contract_builtins` |
| `tools/cq/core/codec.py` | `encode_json`, `decode_json`, `decode_json_result`, codec singletons |
| `tools/cq/core/contracts.py` | `to_contract_builtins`, `to_public_dict`, `require_mapping` |
| `tools/cq/core/diagnostics_contracts.py` | `convert_lax` (from `typed_boundary`) |
| `tools/cq/core/cache/typed_codecs.py` | Uses `convert_lax` for typed cache boundaries |

**Design Rationale:**
- Centralizes all codec instantiation (prevents duplicate encoder/decoder instances)
- Ensures consistent serialization settings across CQ
- Provides single point for instrumentation/logging of serialization operations
- Simplifies testing (mock one module instead of many)

---

## Contract Constraints

**Module:** `tools/cq/core/contracts_constraints.py` (57 LOC)

Provides reusable annotated constraint types and validation policies for public CQ contract payloads.

### Annotated Constraint Types

**Numeric Constraints:**

```python
PositiveInt = Annotated[int, msgspec.Meta(ge=1)]
NonNegativeInt = Annotated[int, msgspec.Meta(ge=0)]
PositiveFloat = Annotated[float, msgspec.Meta(gt=0.0)]
BoundedRatio = Annotated[float, msgspec.Meta(ge=0.0, le=1.0)]
```

**String Constraints:**

```python
NonEmptyStr = Annotated[str, msgspec.Meta(min_length=1)]
```

**Usage in Contract Definitions:**

```python
class SearchLimits(CqStruct, frozen=True):
    max_results: PositiveInt = 100              # Must be >= 1
    timeout_seconds: PositiveFloat = 30.0       # Must be > 0.0
    confidence_threshold: BoundedRatio = 0.5    # Must be [0.0, 1.0]
    run_id: NonEmptyStr                         # Must have length >= 1
```

**Validation:**
- msgspec enforces constraints during struct construction
- Constraint violations raise `msgspec.ValidationError`
- Constraints are type-system visible (improve IDE hints)

### Contract Constraint Policy

**Global Policy:**

```python
class ContractConstraintPolicyV1(CqStruct, frozen=True):
    """Global constraints applied to mapping-like output contracts."""

    max_key_count: int = 10_000              # Prevent DoS via huge payloads
    max_key_length: int = 128                # Prevent malformed keys
    max_string_length: int = 100_000         # Prevent unbounded strings
```

**Enforcement Function:**

```python
def enforce_mapping_constraints(
    payload: Mapping[str, object],
    *,
    policy: ContractConstraintPolicyV1 | None = None,
) -> None:
    """Validate mapping payload against shared contract constraints."""
```

**Validation Rules:**
1. Total key count <= `max_key_count`
2. Each key length <= `max_key_length`
3. Each string value length <= `max_string_length`

**Use Cases:**
- External config file validation (prevent malicious payloads)
- Cache payload validation (prevent cache poisoning)
- API boundary validation (enforce resource limits)

**Integration:**

```python
from tools.cq.core.contract_codec import require_mapping

# require_mapping() automatically calls enforce_mapping_constraints()
validated_dict = require_mapping(external_payload)
```

---

## Summary Envelope Contract

**Module:** `tools/cq/core/summary_contracts.py` (45 LOC)

Provides canonical summary envelope for report/render boundaries with strict output validation.

### Schema

**Summary Envelope:**

```python
class SummaryEnvelopeV1(CqStrictOutputStruct, frozen=True):
    """Canonical summary envelope for report/render boundaries."""

    summary: dict[str, Any] = msgspec.field(default_factory=dict)
    diagnostics: list[dict[str, Any]] = msgspec.field(default_factory=list)
    telemetry: dict[str, Any] = msgspec.field(default_factory=dict)
```

**Contract Properties:**
- Inherits from `CqStrictOutputStruct` (forbids unknown fields)
- Three well-defined surfaces: summary, diagnostics, telemetry
- All fields use factory defaults (never None)

### Builders

**Envelope Construction:**

```python
def build_summary_envelope(
    *,
    summary: Mapping[str, Any],
    diagnostics: list[dict[str, Any]] | None = None,
    telemetry: Mapping[str, Any] | None = None,
) -> SummaryEnvelopeV1:
    """Build a typed summary envelope from mapping surfaces."""
```

**Envelope Conversion:**

```python
def summary_envelope_to_mapping(envelope: SummaryEnvelopeV1) -> dict[str, Any]:
    """Convert summary envelope to mapping payload."""
```

**Design Pattern:**
- Builders accept nullable inputs and normalize to non-null defaults
- Conversion to mapping uses `require_mapping()` (enforces constraints)
- Strict output contract prevents accidental field additions

---

## Typed Cache Codecs

**Module:** `tools/cq/core/cache/typed_codecs.py` (74 LOC)

Provides typed cache boundary helpers for msgpack and mapping payload boundaries.

### Msgpack Typed Decoding

**Typed Decoder:**

```python
def decode_msgpack_typed[T](payload: object, *, type_: type[T]) -> T | None:
    """Decode msgpack payload into a typed struct or container.

    Returns:
        Decoded typed payload when possible, otherwise None.
    """
```

**Decoder Caching:**
- Uses `@lru_cache(maxsize=64)` for decoder instance caching
- One cached decoder per type (prevents redundant instantiation)
- Thread-safe (decoder instances are immutable)

**Fail-Open Semantics:**
- Returns `None` on decode failure (no exceptions)
- Gracefully handles invalid payloads, type mismatches, malformed data
- Suitable for cache hit paths (missing/corrupt cache → cache miss)

### Mapping Typed Conversion

**Typed Conversion:**

```python
def convert_mapping_typed[T](payload: object, *, type_: type[T]) -> T | None:
    """Convert mapping-like payload into a typed contract.

    Returns:
        Converted typed payload when possible, otherwise None.
    """
```

**Conversion Pipeline:**
- Uses `convert_lax()` from typed boundary (tolerates extra fields)
- Catches `BoundaryDecodeError` and returns `None`
- Suitable for legacy cache payloads (schema evolution)

### Msgpack Encoding

**Encoding Functions:**

```python
def encode_msgpack_payload(payload: object) -> bytes | object:
    """Encode payload to msgpack, falling back to builtins on failure.

    Returns:
        Encoded bytes on success, or builtins payload fallback on failure.
    """

def encode_msgpack_into(payload: object, *, buffer: bytearray) -> int:
    """Encode payload into a provided buffer.

    Returns:
        Number of bytes appended to buffer.
    """
```

**Design Patterns:**
- `encode_msgpack_payload()` uses fail-open fallback (returns builtins on encode failure)
- `encode_msgpack_into()` uses pre-allocated buffer (reduces allocations in hot paths)

### Cache Integration

**Cache Hit Path:**

```python
from tools.cq.core.cache.typed_codecs import decode_msgpack_typed

# Cache lookup
raw_payload = cache_store.get(cache_key)
if raw_payload is not None:
    # Typed decode with fail-open
    cached_result = decode_msgpack_typed(raw_payload, type_=CqResult)
    if cached_result is not None:
        return cached_result
# Cache miss or decode failure → recompute
```

**Cache Write Path:**

```python
from tools.cq.core.cache.typed_codecs import encode_msgpack_payload

# Encode result with fail-open fallback
encoded = encode_msgpack_payload(result)
cache_store.set(cache_key, encoded)
```

---

## Serialization Infrastructure

### msgspec Codecs

**All CQ serialization now delegates to `contract_codec.py`.**

**JSON Encoder/Decoder:**

```python
from tools.cq.core.contract_codec import JSON_ENCODER, JSON_DECODER

# Encode struct to JSON bytes
json_bytes = JSON_ENCODER.encode(finding)

# Decode JSON bytes to object
value = JSON_DECODER.decode(json_bytes)
```

**Typed Decoding:**

```python
from tools.cq.core.contract_codec import JSON_RESULT_DECODER

# Decode JSON directly to CqResult
result = JSON_RESULT_DECODER.decode(json_bytes)
```

**Type-Safe Conversion:**

```python
from tools.cq.core.typed_boundary import convert_strict, convert_lax

# Strict conversion (rejects extra fields)
finding = convert_strict(raw_dict, type_=Finding)

# Lax conversion (ignores extra fields)
config = convert_lax(raw_dict, type_=SearchConfig)
```

**Contract Conversion:**

```python
from tools.cq.core.contract_codec import to_contract_builtins, to_public_dict

# Convert struct to dict
raw_dict = to_public_dict(finding)

# Convert any value to builtins
builtins_value = to_contract_builtins(finding)
```

### Boundary Contracts

**What Crosses Module Boundaries:**

| Contract Type | Examples | Base Class |
|--------------|----------|------------|
| **Commands/Requests** | `SearchInsightBuildRequestV1`, `LanguageSemanticEnrichmentRequest`, `MergeResultsRequest` | `CqStruct` |
| **Outcomes/Responses** | `FrontDoorInsightV1`, `SemanticOutcomeV1`, `CqResult` | `CqStruct`, `CqOutputStruct` |
| **Strict Outputs** | `SummaryEnvelopeV1`, persisted artifacts | `CqStrictOutputStruct` |
| **Settings/Config** | `InsightBudgetV1`, `SemanticRequestBudgetV1` | `CqSettingsStruct` |
| **Cache Payloads** | `SemanticOutcomeCacheV1` | `CqCacheStruct` |

**What Does NOT Cross Module Boundaries:**

| Type | Examples | Storage |
|------|----------|---------|
| **Runtime State** | `ScanContext`, `EnrichmentPipeline`, `MatchClassifier` | dataclass, plain class |
| **External Handles** | `TreeSitter.Parser`, `RgRunner`, `LSPSession` | Never serialized |
| **Intermediate Results** | `RawMatch`, `ParsedNode`, `TokenStream` | Local variables only |

### Module Boundary Policy

**Serialization Principles:**

1. **Centralized Codec Authority:** All serialization delegates to `contract_codec.py`
2. **Explicit Contracts:** All cross-module data uses msgspec.Struct subclasses
3. **Frozen by Default:** All contracts are immutable (frozen=True)
4. **Keyword-Only:** All contracts use kw_only=True to prevent positional confusion
5. **Compact Defaults:** omit_defaults=True reduces JSON payload size
6. **Strict Validation:** Strict output/settings/cache structs use forbid_unknown_fields=True
7. **Typed Boundaries:** Use `convert_strict` for internal boundaries, `convert_lax` for external boundaries
8. **Constraint Enforcement:** All mapping-shaped payloads validated via `enforce_mapping_constraints()`

**Anti-Patterns:**

- ❌ Passing `dict[str, object]` across module boundaries (use typed struct)
- ❌ Mutating shared state (use frozen structs)
- ❌ Serializing parser/cache handles (keep runtime-only)
- ❌ Using Pydantic in hot paths (use msgspec)
- ❌ Creating new encoder/decoder instances (use singletons from `contract_codec`)
- ❌ Bypassing `contract_codec` for serialization (delegate to canonical authority)

---

## Module Map

**Cross-Cutting Contracts & Serialization:**

| Module | LOC | Purpose |
|--------|-----|---------|
| `tools/cq/core/structs.py` | 43 | Base msgspec struct classes |
| `tools/cq/core/typed_boundary.py` | 129 | Typed boundary protocol (conversion, validation, error taxonomy) |
| `tools/cq/core/contract_codec.py` | 104 | Centralized serialization authority (encoders, decoders, conversion) |
| `tools/cq/core/contracts_constraints.py` | 57 | Annotated constraint types and validation policies |
| `tools/cq/core/summary_contracts.py` | 45 | Summary envelope contract for report/render boundaries |
| `tools/cq/core/cache/typed_codecs.py` | 74 | Typed cache boundary helpers (msgpack, mapping conversion) |
| `tools/cq/core/serialization.py` | ~99 | Public serialization API (delegates to contract_codec) |
| `tools/cq/core/codec.py` | ~71 | JSON/msgpack codec API (delegates to contract_codec) |
| `tools/cq/core/contracts.py` | ~75 | Contract conversion helpers (delegates to contract_codec) |
| `tools/cq/core/diagnostics_contracts.py` | ~96 | Diagnostics artifact contracts (uses typed boundary) |
| `tools/cq/core/schema_export.py` | ~110 | JSON Schema export for CQ contracts |

**Cross-Subsystem Contracts:**

| Module | LOC | Purpose |
|--------|-----|---------|
| `tools/cq/core/front_door_insight.py` | 1,171 | FrontDoor Insight V1 canonical schema |
| `tools/cq/core/multilang_orchestrator.py` | 481 | Multi-language partition dispatch and merging |
| `tools/cq/search/semantic/models.py` | 507 | LSP integration contracts and semantic state machine |
| `tools/cq/core/enrichment_facts.py` | 596 | Enrichment facts system and resolution pipeline |
| `tools/cq/core/scoring.py` | 252 | Impact/confidence scoring and bucketing |
| `tools/cq/core/schema.py` | ~600 | Core schema models (CqResult, Finding, Section, Anchor) |

---

## Cross-References

This document is the **primary home** for:
- FrontDoor Insight V1 schema and assembly
- Multi-language orchestration and partition merging
- LSP integration contracts and semantic state machine
- Advanced evidence planes (semantic overlays, diagnostics, refactor actions, Rust extensions)
- Enrichment facts system and resolution pipeline
- Scoring system (impact/confidence/buckets)
- Core schema models (CqResult, Finding, Section, Anchor, ScoreDetails, DetailPayload)
- Typed boundary protocol (conversion, validation, error taxonomy)
- Contract codec (centralized serialization authority)
- Contract constraints (annotated types and validation policies)
- Summary envelope contract
- Typed cache codecs
- Serialization infrastructure and module boundary protocol

**Other documents will cross-reference here:**

| Document | Cross-Reference Target |
|----------|----------------------|
| **Doc 01 (CLI)** | Rendering of FrontDoor Insight cards |
| **Doc 02 (Search)** | Search insight assembly via `build_search_insight()` |
| **Doc 03 (Calls)** | Calls insight assembly via `build_calls_insight()` |
| **Doc 04 (Entity)** | Entity insight assembly via `build_entity_insight()` |
| **Doc 07 (Tree-sitter)** | Enrichment facts extraction from tree-sitter payloads |
| **Doc 08 (Neighborhood)** | Neighborhood slice mapping to insight slices |
| **Doc 10 (Run)** | Multi-language orchestration for run plans |
| **All Docs** | Typed boundary protocol for all module boundaries |
| **All Docs** | Contract codec for all serialization operations |

---

## Summary

This document has established the foundational cross-cutting contracts and orchestration primitives that enable coherent multi-subsystem integration across CQ:

1. **FrontDoor Insight V1** provides a unified schema for high-level analysis results with target grounding, neighborhood previews, risk assessment, confidence metrics, and degradation transparency.

2. **Multi-language orchestration** enables deterministic partition dispatch, scope enforcement, and result merging across Python and Rust codebases.

3. **LSP integration layer** provides semantic contract state machine, capability gating, and fail-open semantics for Pyrefly (Python) and rust-analyzer (Rust) backends.

4. **Advanced evidence planes** overlay semantic tokens, diagnostics, refactor actions, and Rust macro expansions on top of structural analysis.

5. **Enrichment facts system** resolves enrichment payloads into structured Code Facts clusters with language/kind applicability filtering and N/A semantics.

6. **Scoring system** provides standardized impact and confidence scoring with categorical bucketing for machine-readable prioritization.

7. **Core schema models** define the fundamental result structures (CqResult, Finding, Section, Anchor) used across all CQ macros.

8. **Typed boundary protocol** (`typed_boundary.py`) provides centralized conversion helpers and error taxonomy for all CQ module boundaries, with strict/lax semantics and format-specific decoders (JSON, TOML, YAML).

9. **Contract codec** (`contract_codec.py`) serves as the single source of truth for all CQ serialization operations, providing codec singletons, encoding/decoding functions, and contract conversion helpers that all other serialization modules delegate to.

10. **Contract constraints** (`contracts_constraints.py`) provides reusable annotated constraint types (PositiveInt, NonEmptyStr, BoundedRatio) and validation policies for public contract payloads.

11. **Summary envelope contract** (`summary_contracts.py`) provides canonical summary envelope for report/render boundaries with strict output validation.

12. **Typed cache codecs** (`cache/typed_codecs.py`) provides typed cache boundary helpers for msgpack and mapping payload boundaries with fail-open semantics.

13. **Serialization infrastructure** enforces strict module boundary protocol with msgspec.Struct base classes (CqStruct, CqOutputStruct, CqStrictOutputStruct, CqSettingsStruct, CqCacheStruct), centralized codec authority, typed boundary validation, and constraint enforcement.

These primitives form the stable foundation upon which all CQ subsystems integrate. The recent consolidation of serialization infrastructure into `contract_codec.py` ensures consistent serialization behavior, eliminates duplicate codec instances, and provides a single point for instrumentation and testing of all CQ serialization operations.
