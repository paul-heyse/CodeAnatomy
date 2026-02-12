# CQ Front-Door Enrichment Embedding Plan v2

**Status:** Draft
**Date:** 2026-02-12
**Builds on:** `docs/plans/cq_more_upfront_enrichment.md`
**Goal:** Embed high-value insights from neighborhood, impact, scopes, and other deep commands into the three front-door commands (search, calls, entity) so LLM programming agents get actionable context in a single call.

---

## Executive Summary

The original plan (`cq_more_upfront_enrichment.md`) correctly identifies the three front doors and proposes a Target Card + Neighborhood Preview + Risk Flags structure. This revised plan makes four key improvements based on deep review of the actual CQ codebase:

1. **Leverage existing infrastructure** rather than building new data models. The SNB schema (`SemanticNeighborhoodBundleV1`, `NeighborhoodSliceV1`, `SemanticNodeRefV1`) already implements exactly the progressive disclosure model the plan describes. Reuse it.

2. **Fix the output ordering problem first** (zero new computation). The biggest win is restructuring *what we already compute* so the most actionable data appears in the first 10-15 lines of output. Currently, search buries Code Facts clusters deep in section nesting; calls puts the summary at the bottom; entity doesn't show caller/callee counts at all despite having them in `ScanContext.calls_by_def`.

3. **Tier enrichments by cost** with explicit latency budgets. The plan should distinguish between "free from scan state" (structural neighborhood), "one LSP call" (Pyrefly for top target), and "N LSP calls" (Pyrefly for all results). Only the first tier is unconditional.

4. **Push purely diagnostic data to artifacts** and keep the in-band output dense with decision-relevant signals.

---

## What the Original Plan Gets Right

- **Three front doors** (search, calls, entity) are the correct embedding targets
- **Target Card + Neighborhood Preview + Risk Flags** is the right structure
- **Budget everything; push the rest to artifacts** is the right principle
- **Stable machine-readable shape** in summary is needed
- **The SNB "totals + preview + artifact pointer"** model proven by neighborhood should be reused everywhere

## What the Original Plan Misses

### 1. Search already computes most of what's proposed

The 5-stage enrichment pipeline (ast_grep → python_ast → import_detail → libcst → tree_sitter) plus Pyrefly LSP enrichment for top 8 matches already produces:

- **Target Card data**: kind, file:line:col, signature, decorators, qualified name, scope chain (all in `enrichment.python.structural` and `enrichment.python.resolution`)
- **Neighborhood data**: incoming callers, outgoing callees, base classes, overriding methods (in `enrichment.pyrefly.call_graph` and `enrichment.pyrefly.class_method_context`)
- **Risk flags**: is_async, is_generator, raises_exception, closure capture flags (in `enrichment.python.behavior` and symtable `binding_flags`)

The problem isn't missing data — it's that this data is buried in `Finding.details.data.enrichment.python.*` sub-trees and rendered as deep Code Facts clusters that agents skip past. The fix is primarily about **output restructuring and headline synthesis**, not new computation.

### 2. Entity has cheap access to exactly the data needed

`ScanContext` (built during every entity query) contains:
- `def_records`: all definitions in scope
- `call_records`: all call sites in scope
- `interval_index`: O(log n) containment queries
- `calls_by_def`: pre-computed call-site-per-definition mapping

This means for every entity result, we can compute **callers count, callees count, calls_within, enclosing context** essentially for free — no additional file parsing needed. The original plan proposes this but doesn't note it's nearly zero-cost because the data is already in memory.

### 3. The calls command already has most of its "embed" proposals

The calls command computes arg shapes, kwargs, forwarding counts, hazards, and calling contexts. The original plan's "embed" proposals for calls are mostly about *promoting existing sections to headlines* and adding a single piece of missing data: "callees of the target function" (what it depends on).

### 4. No need for a new `auto_insights` key

CQ already has `result.summary`, `result.key_findings`, and `result.sections` as the standard output structure. Adding `summary["auto_insights"]` creates a parallel data path. Instead, use a standardized `result.summary["insight_card"]` sub-key that all three commands populate consistently, plus promote existing data into `key_findings` headlines.

---

## Revised Plan Structure

### Phase 0: Output Density Restructuring (Zero New Computation)

**Goal:** Make the first 10-15 lines of every search/calls/entity output contain the most decision-relevant data an agent needs.

**Rationale:** This is the highest-ROI change. LLM agents pattern-match on early output. Everything below the fold gets progressively less attention.

#### 0A. Define a standard "Insight Card" output block

All three commands emit a consistent block at the very top of output (before any sections):

```
## Insight Card

**Target:** `function build_graph` in `src/semantics/pipeline.py:874`
**Signature:** `(request: GraphProductBuildRequest, *, config: SemanticConfig | None = None) -> GraphProduct`
**Kind:** function | **Async:** no | **Generator:** no

**Neighborhood:** 12 callers | 8 callees | 23 references | 2 implementations
**Risk:** impact=high (12 callers across 7 files) | forwarding=2 | hazards=0
**Confidence:** resolved_ast (0.95)
```

This block is:
- Fixed-format (agents can parse it deterministically)
- Populated from data already computed (Pyrefly overview for search; scan census for calls/entity)
- Present *only* when there's a clear primary target (definition-mode search with a top hit; calls with a resolved target; entity with 1-3 results)

#### 0B. Restructure section ordering in all commands

**Search (new section order):**
1. Insight Card (for top definition match) — **NEW, uncollapsed**
2. Target Candidates (top 3 definitions) — **EXISTING "Definitions", promoted, uncollapsed**
3. Neighborhood Preview (callers/callees/refs/impl counts + top 5 each) — **NEW from Pyrefly data**
4. Risk Flags (one-liner) — **NEW synthesized**
5. Top Contexts — **EXISTING, uncollapsed**
6. Imports — **EXISTING, collapsed**
7. Callsites — **EXISTING, collapsed**
8. Uses by Kind — **EXISTING, collapsed**
9. Non-Code Matches — **EXISTING, collapsed**
10. Hot Files — **EXISTING, collapsed**
11. Suggested Follow-ups — **EXISTING, uncollapsed**

**Calls (new section order):**
1. Insight Card (target signature + surface headline) — **NEW, uncollapsed**
2. Target Neighborhood Preview (callees of target) — **NEW from scan data**
3. Argument Shape Histogram — **EXISTING, uncollapsed**
4. Keyword Usage — **EXISTING, collapsed**
5. Hazards — **EXISTING, uncollapsed (promoted from collapsed)**
6. Calling Contexts — **EXISTING, collapsed**
7. Call Sites — **EXISTING, collapsed**

**Entity (new section order):**
1. Insight Card (for top 1-3 results) — **NEW, uncollapsed**
2. Results with mini-neighborhood — **EXISTING findings, enriched with counts**
3. Callers section (if expand=callers) — **EXISTING**
4. Callees section (if expand=callees) — **EXISTING**
5. Other expansion sections — **EXISTING**

#### 0C. Push purely diagnostic data to artifacts

Move these from in-band output to JSON artifacts (accessible via `--format json` or LDMD):
- Enrichment telemetry (stage timings, cache stats)
- Parse quality metrics
- Cross-source agreement details (keep status one-liner, push details)
- Full context snippets for non-top-3 results
- Detailed provenance metadata
- Language capability matrix

Keep in-band:
- Enrichment status one-liner (e.g., "Pyrefly: 8/12 enriched | degraded: 2")
- Scope diagnostics one-liner (e.g., "dropped_by_scope: 3 files")
- Confidence bucket per finding

**Implementation notes:**
- Modify `build_sections()` in `smart_search.py` to emit the Insight Card as position-0 section
- Modify `_append_calls_findings()` in `calls.py` to promote headline data
- Modify `_apply_entity_handlers()` in `executor.py` to include mini-neighborhood counts
- Modify `render_markdown()` in `report.py` to render Insight Card with fixed format
- Add artifact offloading for telemetry/diagnostics data in `handle_result()`

---

### Phase 1: Embedded Structural Mini-Neighborhood (Cheap, from scan state)

**Goal:** Every definition-mode result includes caller/callee/containment counts from data already in memory.

**Cost:** Near-zero — queries against `ScanContext.calls_by_def` and `interval_index` that are already built.

#### 1A. Search: structural neighborhood for top definition match

When search identifies a top definition match with high confidence:

1. Use the match's file:line to query `interval_index` for containing scope (parent)
2. Use `calls_by_def` to get callees count for that definition
3. Scan `call_records` for callers of that definition name (heuristic name-match, same as entity callers)
4. Count siblings at same nesting level via `interval_index`

Emit as a `NeighborhoodSliceV1`-shaped section in the Insight Card:
```
Callers: 12 (top: process_request, handle_event, run_pipeline)
Callees: 8 (top: validate_input, compile_spec, emit_result)
Contains: 3 nested defs (helper_a, helper_b, _validate)
Parent: class SemanticCompiler
```

**Implementation:** Extract `collect_structural_neighborhood()` from `tools/cq/neighborhood/structural_collector.py` into a reusable function that accepts a `ScanSnapshot` + target coordinates and returns `list[NeighborhoodSliceV1]`. Call it from `smart_search.py` after classification, bounded to the top definition match.

**Budget:** max 5 items per slice in preview. Totals always shown.

#### 1B. Calls: callees of the target definition

The calls command finds all call *sites* of a function but doesn't show what the function itself *calls*. This is the #1 missing piece agents need.

1. After finding the target definition (via `_find_function_signature()`), parse the target file
2. Use ast-grep to scan the definition body for call records
3. Emit as a "Target Callees" section after the Insight Card

```
## Target Callees (what `build_graph` depends on)
Total: 8 | Top: validate_input (3 sites), compile_spec (2), emit_result (1), ...
```

**Implementation:** Add `_scan_target_callees()` to `calls.py` that does a single-file ast-grep scan of the definition body. This is cheap because it's one file, not the whole repo.

**Budget:** Top 10 callees with call counts within the target body.

#### 1C. Entity: per-result mini-neighborhood counts

For each entity result (up to a cap), add counts from scan state:

```python
# For each definition finding:
finding.details.data["calls_within"] = len(calls_by_def.get(record_key, []))
finding.details.data["caller_count"] = _count_callers(record, call_records)
finding.details.data["callee_count"] = len(calls_by_def.get(record_key, []))
finding.details.data["enclosing_scope"] = _find_enclosing(record, interval_index)
```

**Implementation:** Modify `_build_entity_finding()` in `executor.py` to add these fields. `calls_within` is already computed; add `caller_count` by scanning `call_records` for name matches (same logic as `_build_callers_section` but counts-only).

**Budget:** Counts for all results (cheap). Preview items only for top 3 results.

---

### Phase 2: Risk Flags Synthesis

**Goal:** Every front-door output includes a compact risk assessment that helps agents choose safer edit strategies.

**Cost:** Low — derived from data already computed in Phase 0/1.

#### 2A. Define `RiskFlags` model

```python
class RiskFlags(CqStruct, frozen=True):
    """Compact risk assessment for a target symbol."""
    caller_count: int = 0
    callee_count: int = 0
    files_with_callers: int = 0
    arg_shape_count: int = 0           # Distinct arg shapes (from calls)
    forwarding_count: int = 0           # *args/**kwargs forwarding
    hazard_count: int = 0               # Dynamic dispatch/eval/exec
    is_closure: bool = False            # Captures free variables
    has_globals: bool = False           # References global state
    is_async: bool = False
    is_generator: bool = False
    raises_exception: bool = False
    risk_level: str = "low"             # "low" | "medium" | "high"
```

Risk level computed as:
- **high**: caller_count > 10 OR forwarding_count > 0 with hazards OR is_closure with many free_vars
- **medium**: caller_count > 3 OR arg_shape_count > 3 OR has_globals
- **low**: everything else

#### 2B. Search risk flags

Populate from:
- `enrichment.pyrefly.call_graph` → caller/callee counts (for top match with Pyrefly)
- `enrichment.python.behavior` → is_async, is_generator, raises_exception
- `enrichment.python.structural` → is in closure scope chain
- Structural mini-neighborhood from Phase 1A → caller_count, callee_count

Emit as one-liner in Insight Card: `Risk: medium (7 callers, 2 arg-shapes, async)`

#### 2C. Calls risk flags

Populate from existing analysis (already computed):
- `scan_result.files_with_calls` → files_with_callers
- `analysis.forwarding_count` → forwarding_count
- `analysis.hazard_counts` → hazard_count
- `analysis.arg_shapes` → arg_shape_count (len of Counter)

The calls command already has all this data — it just needs to be synthesized into a headline.

Emit as one-liner in Insight Card: `Risk: high (42 callers / 8 files / 5 arg-shapes / 2 forwarding / 1 hazard)`

#### 2D. Entity risk flags

Populate from scan state (Phase 1C data):
- caller_count from `_count_callers()`
- callee_count from `calls_by_def`
- is_closure from symtable (if `need_symtable=True`)

Emit per-result in the finding message: `build_graph [func] - 12 callers, 8 callees, risk=medium`

---

### Phase 3: Selective LSP Enrichment for Calls and Entity

**Goal:** Add Pyrefly-quality data to calls and entity for their top targets, without adding N LSP calls.

**Cost:** Moderate — one Pyrefly call per command invocation (not per result).

#### 3A. Calls: Pyrefly enrichment for the target definition

Currently calls doesn't use Pyrefly at all. Add a single LSP call for the resolved target:

1. After resolving the target definition location, request Pyrefly enrichment for that anchor
2. Extract: `type_contract` (full signature with types), `call_graph` (incoming/outgoing), `class_method_context` (overrides)
3. Add to Insight Card: typed signature, implementation/override indicators

**Implementation:** Import `enrich_with_pyrefly_lsp()` from `tools/cq/search/pyrefly_lsp.py`. Call once for the target anchor. This is the same function search uses, so it's tested and fail-open.

**Budget:** One LSP call. Timeout: 2 seconds. Fail-open: degrades to structural-only data.

#### 3B. Entity: Pyrefly enrichment for top 3 results

Similar to 3A but for the top 3 entity results (by calls_within or caller_count ranking):

1. For each of the top 3 results, request Pyrefly enrichment
2. Add typed signatures and call graph hints to those findings
3. Remaining results get structural-only data

**Implementation:** Same as 3A but in a loop bounded to 3. Can be parallelized with the existing worker pool (`multiprocessing.spawn`).

**Budget:** 3 LSP calls max. Timeout: 2 seconds each. Fail-open per-call.

#### 3C. Capability gating

All LSP enrichment must be capability-gated using the existing `capability_gates.py` infrastructure:
- Check `LspCapabilitySnapshotV1` before attempting calls
- Track degradation events in findings
- Show one-liner degradation status in output footer

---

### Phase 4: Dependency Context (Import Chain)

**Goal:** Show import-level coupling for the target symbol to prevent refactor failures at the import boundary.

**Cost:** Low for structural; moderate for reverse importers.

#### 4A. Forward imports (cheap)

For the target file, show:
- Imports used by the target function/class (from ast-grep `import` records in the same file)
- Resolved import path (from Pyrefly `import_alias_resolution` when available)

This is nearly free: filter `import_records` from the scan to those in the target file.

#### 4B. Reverse importers (moderate)

Show which files import the target's module:
- Scan `import_records` across all scanned files for imports matching the target module
- Emit as a count + top 5 preview

**Implementation:** Add `_find_reverse_importers()` to a shared enrichment utility. Uses existing `call_records` filtered to `record="import"`.

**Budget:** Count always shown. Top 5 importers in preview. Full list in artifact.

---

### Phase 5: Standardized `insight_card` in Summary

**Goal:** Every CQ front-door result includes a machine-parseable `summary["insight_card"]` dict that agents can consume programmatically.

#### 5A. Define the schema

```python
class InsightCard(CqStruct, frozen=True):
    """Standardized insight card emitted by all front-door commands."""

    # Target identity
    target_name: str = ""
    target_kind: str = ""                # function/class/method/module
    target_file: str = ""
    target_line: int = 0
    target_signature: str | None = None
    target_qualname: str | None = None

    # Neighborhood counts
    caller_count: int = 0
    callee_count: int = 0
    reference_count: int = 0
    implementation_count: int = 0

    # Risk assessment
    risk: RiskFlags | None = None

    # Confidence
    confidence_score: float = 0.0
    confidence_bucket: str = "low"
    evidence_kind: str = "unresolved"

    # Provenance
    resolution_kind: str = ""           # "anchor" | "file_symbol" | "symbol_fallback"
    lsp_enriched: bool = False
    degradations: tuple[str, ...] = ()
```

#### 5B. Populate per command

- **Search:** From top definition match + Pyrefly overview
- **Calls:** From target resolution + call census + analysis summary
- **Entity:** From top result + scan state counts

#### 5C. Emit in `result.summary`

```python
result.summary["insight_card"] = msgspec.to_builtins(insight_card)
```

This makes it available in JSON output for programmatic consumption while the markdown rendering uses the same data for the human-readable Insight Card block.

---

## What NOT to Embed (Push to Artifacts / On-Demand Commands)

These signals are too expensive or too noisy for automatic embedding. Keep them as separate commands with artifact references:

| Signal | Why Not Embed | Access Via |
|--------|---------------|------------|
| Full impact analysis (transitive callers) | O(n) graph traversal | `/cq impact` |
| Sig-impact breakage classification | Requires old+new signature | `/cq sig-impact` |
| Bytecode surface (all opcodes) | Noisy, large output | `/cq q "expand=bytecode_surface"` |
| Full scope graph (cell/free vars) | Only relevant for closure work | `/cq scopes` or `/cq q "scope=closure"` |
| Exception flow analysis | Per-function, not per-query | `/cq exceptions` (reserved) |
| Side-effect detection | Expensive, needs whole-function analysis | `/cq side-effects` (reserved) |
| Full import dependency graph | Module-level, not symbol-level | `/cq q "entity=import"` |
| Enrichment telemetry details | Diagnostic, not decision-relevant | `--format json` artifact |
| Parse quality metrics | Diagnostic | `--format json` artifact |
| Stage timing data | Diagnostic | `--format json` artifact |
| Cross-source agreement conflicts | Diagnostic (keep status one-liner) | `--format json` artifact |

---

## Implementation Priority & Phasing

### Batch 1: Highest ROI (output restructuring + cheap structural data)
**Phases 0 + 1 + 2A-2D**

These require no new infrastructure and no LSP calls. They restructure existing data for maximum front-of-output density and add structural neighborhood counts from data already in memory.

**Files to modify:**
- `tools/cq/search/smart_search.py` — Insight Card synthesis, section reordering
- `tools/cq/macros/calls.py` — Insight Card synthesis, target callees, headline promotion
- `tools/cq/query/executor.py` — Mini-neighborhood counts per entity result
- `tools/cq/core/report.py` — Insight Card markdown renderer
- `tools/cq/core/schema.py` — `InsightCard` and `RiskFlags` structs (if not inline)
- `tools/cq/neighborhood/structural_collector.py` — Extract reusable mini-neighborhood function

**New files:**
- `tools/cq/core/insight_card.py` — Shared `InsightCard`, `RiskFlags` models + synthesis logic

**Estimated scope:** ~800 LOC across modifications, ~200 LOC new.

### Batch 2: LSP enrichment for calls/entity
**Phase 3**

Adds Pyrefly calls (1 for calls, up to 3 for entity). Requires importing the existing Pyrefly enrichment function and wiring it into the call/entity executors.

**Files to modify:**
- `tools/cq/macros/calls.py` — Add Pyrefly call for target
- `tools/cq/query/executor.py` — Add Pyrefly calls for top 3 entities

**Estimated scope:** ~200 LOC modifications.

### Batch 3: Dependency context + standardized summary
**Phases 4 + 5**

Adds import chain analysis and the machine-readable `insight_card` summary key.

**Files to modify:**
- `tools/cq/search/smart_search.py` — Forward/reverse import context
- `tools/cq/macros/calls.py` — Import context for target
- `tools/cq/query/executor.py` — Import context for entity results
- All three commands — Emit `summary["insight_card"]`

**Estimated scope:** ~400 LOC across modifications.

---

## Output Structure: Before and After

### Search: Before
```
## Code Overview
query: build_graph | mode: identifier | scope: python | ...

## Top Contexts
- [conf:0.95] `build_graph` definition in pipeline.py:874 (function_definition)
  Code Facts:
    Identity & Grounding: ...
    Type Contract: ...
    ...
  Context (lines 870-920): ...

## Definitions
- build_graph in pipeline.py:874
- build_graph in test_pipeline.py:42

## Imports (collapsed)
...
## Callsites (collapsed)
...
```

### Search: After
```
## Insight Card
**Target:** `function build_graph` in `src/semantics/pipeline.py:874`
**Signature:** `(request: GraphProductBuildRequest, *, config: SemanticConfig | None = None) -> GraphProduct`
**Kind:** function | **Async:** no | **Generator:** no
**Neighborhood:** 12 callers | 8 callees | 23 references | 2 implementations
**Risk:** high (12 callers across 7 files) | forwarding=0 | hazards=0
**Confidence:** resolved_ast (0.95) | Pyrefly: enriched

## Target Candidates
1. `build_graph` in `src/semantics/pipeline.py:874` [definition] — 12 callers, 8 callees
2. `build_graph` in `tests/test_pipeline.py:42` [definition] — 0 callers, 3 callees
3. `build_graph` in `src/cpg/builder.py:156` [definition] — 5 callers, 4 callees

## Neighborhood Preview (for #1)
Callers (12): process_request, handle_event, run_pipeline, test_build, ...
Callees (8): validate_input, compile_spec, emit_result, ...
References (23): 7 files
Implementations: SemanticCompiler.build_graph, CpgBuilder.build_graph

## Top Contexts
...
## Callsites (collapsed)
...
## Non-Code Matches (collapsed)
...
## Suggested Follow-ups
...
```

### Calls: Before
```
## Key Findings
- Found 42 calls to `build_graph` across 8 files
- 2 calls use *args/**kwargs forwarding

## Argument Shape Histogram
...
## Keyword Argument Usage
...
## Calling Contexts
...
## Hazards
...
## Call Sites
(42 individual findings)
```

### Calls: After
```
## Insight Card
**Target:** `function build_graph` in `src/semantics/pipeline.py:874`
**Signature:** `(request: GraphProductBuildRequest, *, config: SemanticConfig | None = None) -> GraphProduct`
**Call Surface:** 42 calls / 8 files / 5 arg-shapes / 2 forwarding / 0 hazards
**Risk:** high (42 callers across 8 files, 2 forwarding)

## Target Callees (what `build_graph` depends on)
Total: 8 | validate_input (3), compile_spec (2), emit_result (1), register_view (1), build_plan (1)

## Argument Shape Histogram
...
## Hazards
...
## Calling Contexts (collapsed)
...
## Call Sites (collapsed)
...
```

---

## Differences from Original Plan

| Original Plan Proposal | This Plan's Position | Rationale |
|------------------------|---------------------|-----------|
| New `auto_insights` summary key | Use `summary["insight_card"]` sub-key | Avoids parallel data path; stays in existing summary structure |
| Target Card as new concept | Insight Card (richer, fixed-format) | Includes risk + neighborhood counts, not just identity |
| Neighborhood Preview: build new | Reuse `structural_collector` + `NeighborhoodSliceV1` | Infrastructure already exists and is tested |
| Risk Flags: boolean/enum only | `RiskFlags` struct with counts + level | Agents need counts for decision-making, not just booleans |
| Dependency Context: import chain | Phase 4 (batch 3, lower priority) | Import chain is useful but less actionable than neighborhood/risk |
| Reliability/Safety Glance (exceptions + side-effects) | Push to artifacts / on-demand commands | Too expensive for auto-embed; rarely changes edit decisions |
| Provenance as top-level concern | One-liner in Insight Card footer | Full provenance is diagnostic, not decision-relevant |
| Budget: 3 target cards for search | Keep 3 target candidates | Agreed — but with mini-neighborhood counts, not full cards |
| Budget: 10 target cards for entity | Keep 10 results, but mini-neighborhood for top 3 only | Full enrichment for 10 is too expensive |
| SNB model reuse: not mentioned | Core principle — reuse `NeighborhoodSliceV1` everywhere | Prevents model proliferation |
| Output ordering: not addressed | Phase 0 is the highest ROI change | Agents pattern-match on early output |

---

## Success Criteria

1. **Single-call orientation:** An agent using `cq search <symbol>` gets target identity, neighborhood summary, and risk assessment without a follow-up call in >80% of definition-mode queries.
2. **Calls context completeness:** An agent using `cq calls <function>` sees the target's dependencies (callees) alongside its call surface in every invocation.
3. **Entity counts:** Every entity query result includes caller/callee counts from scan state.
4. **No latency regression:** Search p95 latency increases by <200ms (structural neighborhood is nearly free). Calls increases by <500ms (one optional Pyrefly call). Entity increases by <100ms (count queries on existing indexes).
5. **Context efficiency:** Total markdown output size stays within 2x of current (artifact offloading compensates for new sections).
6. **Backward compatibility:** JSON output structure is additive (new keys only). No existing keys removed or renamed.
