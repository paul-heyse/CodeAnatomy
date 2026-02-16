# Design Review Synthesis: tools/cq

**Date:** 2026-02-15
**Scope:** Full `tools/cq` codebase (~72K LOC, 257 files, 13+ subsystems)
**Methodology:** 7 parallel design-reviewer agents, each auditing 24 principles across a cohesive subsystem scope. This document synthesizes cross-cutting themes, ranks improvement opportunities, and identifies systemic patterns.

## Individual Review Documents

| # | Subsystem | File | LOC | Files |
|---|-----------|------|-----|-------|
| 1 | [Search Pipeline](design_review_tools_cq_search_pipeline_2026-02-15.md) | `search/pipeline/` | 6,126 | 10 |
| 2 | [Tree-Sitter Engine](design_review_tools_cq_search_tree_sitter_2026-02-15.md) | `search/tree_sitter/` | 10,456 | 46 |
| 3 | [Search Lanes](design_review_tools_cq_search_lanes_2026-02-15.md) | `search/{_shared,objects,python,rg,rust,semantic,...}` | 10,840 | 35 |
| 4 | [Query Subsystem](design_review_tools_cq_query_2026-02-15.md) | `query/` | 9,034 | 16 |
| 5 | [Core Infrastructure](design_review_tools_cq_core_2026-02-15.md) | `core/` | 14,901 | 72 |
| 6 | [Analysis Commands](design_review_tools_cq_macros_2026-02-15.md) | `macros/` + `index/` + `introspection/` | 10,556 | 23 |
| 7 | [CLI/Execution](design_review_tools_cq_cli_execution_2026-02-15.md) | `cli_app/` + `run/` + `neighborhood/` + `ldmd/` + peripherals | 12,921 | 55 |

---

## Principle Alignment Heatmap

Scores per subsystem (0=significant gap, 1=weak, 2=mostly aligned, 3=well aligned):

| # | Principle | Pipeline | Tree-Sitter | Lanes | Query | Core | Macros | CLI/Exec | Avg |
|---|-----------|----------|-------------|-------|-------|------|--------|----------|-----|
| 1 | Information hiding | 1 | 2 | 2 | 1 | 2 | 2 | 2 | 1.7 |
| 2 | Separation of concerns | 1 | 1 | 2 | 1 | 1 | 1 | 1 | 1.1 |
| 3 | SRP | **0** | 1 | 2 | **0** | 1 | 1 | 1 | 0.9 |
| 4 | High cohesion / low coupling | 1 | 2 | 2 | 1 | 2 | 2 | 2 | 1.7 |
| 5 | Dependency direction | 2 | **3** | 1 | 2 | 2 | 2 | 2 | 2.0 |
| 6 | Ports & Adapters | 2 | 2 | 2 | 2 | **3** | 1 | 2 | 2.0 |
| 7 | DRY (knowledge) | 1 | **0** | 1 | 1 | 2 | 2 | 2 | 1.3 |
| 8 | Design by contract | 2 | **3** | 2 | 2 | **3** | 2 | 2 | 2.3 |
| 9 | Parse, don't validate | 2 | 2 | 1 | **3** | **3** | 2 | **3** | 2.3 |
| 10 | Illegal states | 2 | 2 | 2 | 2 | 2 | 1 | 2 | 1.9 |
| 11 | CQS | 1 | 2 | 2 | 2 | 2 | 2 | 2 | 1.9 |
| 12 | DI + composition | 1 | 2 | 2 | 1 | **3** | 1 | 2 | 1.7 |
| 13 | Composition > inheritance | **3** | **3** | **3** | **3** | **3** | **3** | **3** | **3.0** |
| 14 | Law of Demeter | 1 | 2 | 2 | 2 | 2 | 2 | 2 | 1.9 |
| 15 | Tell, don't ask | 1 | 2 | 1 | 2 | 2 | 2 | 2 | 1.7 |
| 16 | Functional core / imperative shell | 1 | 2 | 2 | 1 | 2 | 2 | 2 | 1.7 |
| 17 | Idempotency | 2 | **3** | **3** | **3** | **3** | **3** | **3** | **2.9** |
| 18 | Determinism | 2 | **3** | 2 | **3** | **3** | 2 | **3** | **2.6** |
| 19 | KISS | 1 | 2 | 2 | 1 | 2 | 2 | 2 | 1.7 |
| 20 | YAGNI | 2 | 2 | **3** | 2 | **3** | 2 | 2 | 2.3 |
| 21 | Least astonishment | 1 | 2 | 2 | 2 | 2 | 2 | 2 | 1.9 |
| 22 | Public contracts | 2 | **3** | 2 | 2 | **3** | 2 | 2 | 2.3 |
| 23 | Testability | 1 | 2 | 2 | 1 | 2 | 1 | 2 | 1.6 |
| 24 | Observability | 2 | 2 | 2 | 1 | 2 | 2 | 2 | 1.9 |
| | **Subsystem Avg** | **1.4** | **2.1** | **2.0** | **1.7** | **2.3** | **1.8** | **2.1** | **1.9** |

### Strongest Principles (system-wide)

| Principle | Avg | Notes |
|-----------|-----|-------|
| **P13: Composition > Inheritance** | **3.0** | Perfect score across all 7 subsystems. No class hierarchies; all behavior via function composition and frozen structs. |
| **P17: Idempotency** | **2.9** | All operations are read-only analyses. Cache writes are content-addressed. Re-execution is safe. |
| **P18: Determinism** | **2.6** | Same inputs produce same outputs. Parallel results sorted by index. Hash-based IDs. |
| **P9: Parse, don't validate** | **2.3** | Excellent at CLI boundary (`convert_strict`), query boundary (parser → IR), and YAML/TOML boundaries. |
| **P8: Design by contract** | **2.3** | Frozen msgspec structs, annotated types (`PositiveInt`, `BoundedRatio`), versioned contracts (`V1` suffix). |

### Weakest Principles (system-wide)

| Principle | Avg | Notes |
|-----------|-----|-------|
| **P3: SRP** | **0.9** | Every subsystem has a God Module. Two files scored 0/3. |
| **P2: Separation of concerns** | **1.1** | Direct consequence of God Modules concentrating multiple responsibilities. |
| **P7: DRY** | **1.3** | Systemic duplication between Python/Rust lanes, across utility functions, and in type coercion helpers. |
| **P23: Testability** | **1.6** | Module-global caches, inline dependency construction, and monolithic files block unit testing. |

---

## Cross-Cutting Themes

### Theme 1: The God Module Epidemic

**Every subsystem contains at least one oversized file** that concentrates too many responsibilities and is the root cause of multiple principle violations.

| File | LOC | Functions | Subsystem | Principles Violated |
|------|-----|-----------|-----------|-------------------|
| `smart_search.py` | 3,914 | 100+ | Search Pipeline | P1, P2, P3, P4, P11, P16, P19, P21, P23 |
| `executor.py` | 3,457 | 117 | Query | P1, P2, P3, P4, P12, P16, P19, P23 |
| `calls.py` | 2,274 | 60+ | Macros | P2, P3, P4, P16, P19 |
| `extractors.py` | 2,251 | 50+ | Search Lanes | P2, P3, P16 |
| `rust_lane/runtime.py` | 1,976 | 40+ | Tree-Sitter | P2, P3, P4, P19 |
| `report.py` | 1,773 | 50+ | Core | P2, P3, P4, P5, P11, P16, P23 |
| `runner.py` | 1,297 | 30+ | CLI/Execution | P2, P3, P4, P12, P16, P23 |

**Combined LOC:** 16,942 (~23% of total codebase)

**Root cause:** Organic growth — new features were added to existing entry-point files rather than extracted into focused modules when complexity thresholds were crossed.

**Impact:** These 7 files are the root cause of **68% of all principle violations** identified across the review. Decomposing them is the single highest-leverage improvement.

**Recommended target:** No file should exceed ~800 LOC. Each God Module should be decomposed into 3-5 focused sub-modules behind a thin orchestration layer.

---

### Theme 2: Untyped Dict Payloads

**`dict[str, object]` is the system's de facto interchange format** for enrichment data and result metadata. This single pattern cascades into violations of 7+ principles.

| Location | Payload | Consumers |
|----------|---------|-----------|
| `EnrichedMatch.python_enrichment` | `dict[str, object] \| None` | Pipeline, Lanes, Objects |
| `EnrichedMatch.rust_tree_sitter` | `dict[str, object] \| None` | Pipeline, Lanes, Objects |
| `PythonEnrichmentPayload.data` | `dict[str, object]` | Enrichment, Objects |
| `RustEnrichmentPayload.data` | `dict[str, object]` | Enrichment, Objects |
| `CqResult.summary` | `dict[str, object]` | Runner, CLI, LDMD, Report |
| `CallSite.symtable_info` | `dict[str, object] \| None` | Macros |
| `macro_scoring_details()` return | `dict[str, object]` | Macros, Scoring |
| `DetailPayload.data` | `dict[str, object]` | Core, Report |

**Consequences:**
- **P7 (DRY):** Key-set constants (`_PY_RESOLUTION_KEYS`, `_PY_BEHAVIOR_KEYS`, etc.) duplicate schema knowledge
- **P9 (Parse, don't validate):** Consumers do extensive `isinstance` checking at use-site instead of parsing at boundary
- **P10 (Illegal states):** Any key-value combination is representable, including invalid ones
- **P14 (Demeter):** Nested `.get().get()` chains to navigate payload structure
- **P15 (Tell, don't ask):** Payloads are passive data bags; consumers interrogate rather than delegate
- **P19 (KISS):** Complex partitioning functions classify dict keys into 4-5 buckets via frozenset membership

**Recommended approach:** Define typed msgspec structs for each payload family (`PythonResolutionFacts`, `PythonBehaviorFacts`, `RustEnrichmentFacts`, `RunSummaryV1`, `ScoringDetailsV1`). Build them at the boundary where data is produced. Consumers receive typed structs instead of dicts. This is a large but high-ROI migration that resolves the most principle violations per unit of effort.

---

### Theme 3: Module-Level Mutable State

**At least 12 module-level mutable caches** exist across the codebase, creating test isolation issues, concurrency hazards, and hidden coupling.

| Cache | Module | Type | Eviction |
|-------|--------|------|----------|
| `_sg_cache` | `classifier_runtime.py` | `dict` | Manual `clear()` |
| `_source_cache` | `classifier_runtime.py` | `dict` | Manual `clear()` |
| `_def_lines_cache` | `classifier_runtime.py` | `dict` | Manual `clear()` |
| `_symtable_cache` | `classifier_runtime.py` | `dict` | Manual `clear()` |
| `_record_context_cache` | `classifier_runtime.py` | `dict` | Manual `clear()` |
| `_node_index_cache` | `classifier_runtime.py` | `dict` | Manual `clear()` |
| `_SEARCH_OBJECT_VIEW_REGISTRY` | `smart_search.py` | `dict` | Manual |
| `_SESSION_CACHE` | `analysis_session.py` | `dict` | Size-bounded eviction |
| `_AST_CACHE` | `extractors.py` | `dict` | Size-bounded eviction |
| `_AST_CACHE` | `rust/enrichment.py` | `dict` | Size-bounded eviction |
| `_truncation_tracker` | `extractors.py` | `list` | Manual clear |
| `_TREE_CACHE` | `rust_lane/runtime.py` | `OrderedDict` | Size-bounded eviction |

**Impact:**
- **P1 (Information hiding):** Cache internals exposed as module-level state
- **P7 (DRY):** Same LRU eviction pattern duplicated 4+ times
- **P18 (Determinism):** `_truncation_tracker` leaks state between calls
- **P23 (Testability):** Tests must call `clear_*()` between cases; no per-test isolation

**Recommended approach:** Extract a shared `BoundedCache[K, V]` class into `_shared/bounded_cache.py`. Replace all manual eviction patterns. Make caches injectable via parameters with defaults, enabling per-test isolation.

---

### Theme 4: Dependency Direction Inversions

**8+ modules in the search lanes import upward from `pipeline/`**, and **core imports from the search subsystem**, inverting the intended layering.

**Upward imports into `pipeline/`:**

| Module | Imports From | Type |
|--------|-------------|------|
| `_shared/core.py` | `pipeline/classifier` (`QueryMode`) | TYPE_CHECKING |
| `rg/runner.py` | `pipeline/profiles` (`SearchLimits`) | Runtime |
| `rg/adapter.py` | `pipeline/` (`QueryMode`, `SearchLimits`, `DEFAULT`, `INTERACTIVE`) | Runtime |
| `rg/prefilter.py` | `pipeline/` (`QueryMode`, `SearchLimits`) | Runtime |
| `rg/collector.py` | `pipeline/` (`SearchLimits`, `RawMatch`) | TYPE_CHECKING |
| `rust/enrichment.py` | `pipeline/classifier` (`get_node_index`) | Runtime |
| `python/analysis_session.py` | `pipeline/classifier` (`get_sg_root`, `get_node_index`) | Deferred |
| `semantic/front_door.py` | `pipeline/classifier` (`get_sg_root`) | Runtime |

**Core importing from search:**

| Module | Imports From | What |
|--------|-------------|------|
| `core/front_door_insight.py` | `search/semantic/models.py` | `SemanticContractStateInputV1`, `SemanticStatus` |
| `core/report.py` | `search/pipeline/smart_search.py` | `RawMatch`, `classify_match`, `build_finding` |

**Recommended approach:**
1. Move `QueryMode` and `SearchLimits` to `_shared/types.py` (vocabulary types belong in the foundation layer)
2. Move `SemanticContractStateInputV1` and `SemanticStatus` to `core/semantic_contracts.py`
3. Inject enrichment callbacks into `report.py` instead of importing search pipeline internals
4. Define `SgRootProvider` protocol in `_shared/` for lane modules

---

### Theme 5: Python/Rust Lane Duplication

**The tree-sitter subsystem has the worst DRY score (0/3)** due to 6+ functions duplicated across Python and Rust lanes.

| Function | Python Lane | Rust Lane | Shared? |
|----------|------------|-----------|---------|
| `_build_query_windows` | `runtime.py:162` + `facts.py:226` | `runtime.py:1427` | No |
| `_lift_anchor` | `runtime.py:487` + `facts.py:115` | N/A | No |
| `_make_parser` / `_parse_tree` | `runtime.py:98` | `runtime.py:285` | No |
| `_pack_source_rows` | `runtime.py:207` + `facts.py:198` | `runtime.py:356` | No |
| `_python_field_ids` | 4 locations | N/A | No |
| `_ENRICHMENT_ERRORS` | `runtime.py:57` | `runtime.py:94` | No |
| `NodeLike` protocol | 5 independent definitions | | No |
| `_normalize_semantic_version` | 3 independent definitions | | No |

**Recommended approach:** Create `core/lane_support.py` with shared implementations. Consolidate `NodeLike` into `contracts/core_models.py`. This is ~10 small mechanical moves.

---

### Theme 6: String-Typed Categoricals

**At least 6 categorical fields use bare `str`** instead of `Literal` unions or enums, allowing invalid values.

| Field | Module | Valid Values |
|-------|--------|-------------|
| `ResolvedCall.confidence` | `call_resolver.py` | "exact", "likely", "ambiguous", "unresolved" |
| `TaintedSite.kind` | `impact.py` | "source", "call", "return", "assign" |
| `CallSite.binding` | `calls.py` | "ok", "ambiguous", "would_break", "unresolved" |
| `CFGEdge.edge_type` | `cfg_builder.py` | "fallthrough", "jump", "exception" |
| `EnrichmentStatus` | `enrichment/contracts.py` | "applied", "degraded", "skipped" |
| `CliResult.result` | `context.py` | `CqResult \| CliTextResult \| int` (typed as `Any`) |

**Recommended approach:** Define `Literal` unions or enums and apply at construction. This is a mechanical change with high type-safety payoff.

---

## Ranked Improvement Opportunities

### Tier 1: Quick Wins (small effort, immediate impact)

| # | Change | Effort | Principles Improved | Subsystems |
|---|--------|--------|-------------------|------------|
| 1 | Consolidate 5 `NodeLike` protocols into one | small | P7 | Tree-Sitter |
| 2 | Extract `_normalize_semantic_version` to one location | small | P7 | Tree-Sitter |
| 3 | Extract `_python_field_ids` to one location | small | P7 | Tree-Sitter |
| 4 | Define `Literal` types for categorical fields (6 types) | small | P9, P10 | Macros, Lanes |
| 5 | Move `_truncation_tracker` into per-call state | small | P18, P23 | Search Lanes |
| 6 | Deduplicate `_count_result_matches` + `_missing_languages_from_summary` + `_extract_def_name` | small | P7 | Query |
| 7 | Add `__all__` to `executor.py`, `enrichment.py`, `planner.py` | small | P22 | Query |
| 8 | Consolidate dual context-injection (`@require_ctx` + `require_context`) | small | P7, P19 | CLI/Execution |
| 9 | Move `SemanticContractStateInputV1`/`SemanticStatus` from search to core | small | P5 | Core |
| 10 | Extract shared AST helpers (`_node_byte_span`, `_ast_node_priority`) | small | P7 | Search Lanes |
| 11 | Remove `DefIndex.load_or_build` (dead code) | small | P20, P21 | Macros |
| 12 | Extract shared `_coerce_float`/`_coerce_str` helpers | small | P7 | Core |
| 13 | Fix `canonicalize_*_lane_payload` to not mutate input | small | P21 | Tree-Sitter |
| 14 | Rename `NeighborhoodStep.no_semantic_enrichment` to positive form | small | P21 | CLI/Execution |
| 15 | Export `is_section_collapsed()` from neighborhood | small | P1, P5 | CLI/Execution |

### Tier 2: Targeted Extractions (medium effort, high impact)

| # | Change | Effort | Principles Improved | Subsystems |
|---|--------|--------|-------------------|------------|
| 16 | Move `QueryMode` + `SearchLimits` to `_shared/types.py` | medium | P4, P5, P12 | Search Lanes (8 modules) |
| 17 | Create `core/lane_support.py` with shared lane utilities | medium | P7 | Tree-Sitter |
| 18 | Extract data types from `smart_search.py` to `types.py` | medium | P3, P4 | Search Pipeline |
| 19 | Extract ast-grep match execution from `executor.py` to `ast_grep_match.py` | medium | P1, P4 | Query |
| 20 | Extract enrichment telemetry from `smart_search.py` | medium | P2, P19 | Search Pipeline |
| 21 | Extract `render_enrichment.py` from `report.py` | medium | P2, P3, P16 | Core |
| 22 | Extract shared `BoundedCache[K, V]` class | medium | P1, P7, P23 | Cross-cutting |
| 23 | Define typed enrichment payload structs | medium | P9, P15, P19 | Search Lanes, Pipeline |
| 24 | Introduce `SymbolIndex` protocol for `DefIndex` | medium | P6, P12, P23 | Macros |
| 25 | Resolve runtime services once per run (not per step) | medium | P12 | CLI/Execution |

### Tier 3: God Module Decompositions (large effort, transformative impact)

| # | Change | Effort | Target | Current LOC | Target LOC |
|---|--------|--------|--------|-------------|------------|
| 26 | Decompose `smart_search.py` | large | 4 modules | 3,914 | ~1,000 + 3x~600 |
| 27 | Decompose `executor.py` | large | 5 modules | 3,457 | ~800 + 4x~500 |
| 28 | Decompose `calls.py` | large | 6 modules | 2,274 | ~300 + 5x~350 |
| 29 | Decompose `extractors.py` | large | 4 modules | 2,251 | ~500 + 3x~500 |
| 30 | Decompose `rust_lane/runtime.py` | large | 4 modules | 1,976 | ~400 + 3x~500 |
| 31 | Decompose `report.py` | medium | 3 modules | 1,773 | ~700 + 2x~500 |
| 32 | Decompose `runner.py` | medium | 4 modules | 1,297 | ~400 + 3x~300 |

---

## Recommended Execution Sequence

### Phase 1: Foundation (Tier 1, weeks 1-2)

Execute quick wins #1-15 in parallel where possible. These are mechanical, zero-risk changes that collectively address 10+ principles with no behavioral change. **Estimated: 15 small PRs, each independently mergeable.**

Key dependencies:
- #1-3 (tree-sitter DRY) can be a single PR
- #4 (Literal types) and #6 (query DRY) are independent
- #8 (context injection) and #14-15 (CLI naming) are independent

### Phase 2: Structural Preparation (Tier 2 items 16-18, weeks 2-3)

These medium-effort changes create the conditions for safe God Module decomposition:

1. **Move vocabulary types** (#16): `QueryMode` and `SearchLimits` to `_shared/types.py`. This unblocks clean imports for the pipeline decomposition.
2. **Extract data types from `smart_search.py`** (#18): Eliminates the `Any`-typed `_smart_search_module()` hack in `partition_pipeline.py`.
3. **Create `core/lane_support.py`** (#17): Shared utilities for both tree-sitter lanes.

### Phase 3: Targeted Extractions (Tier 2 items 19-25, weeks 3-5)

Each extraction can proceed independently:
- #19 (ast-grep extraction from executor.py) unblocks query decomposition
- #20-21 (enrichment/rendering extraction) unblocks pipeline/core decomposition
- #22 (BoundedCache) addresses the cross-cutting cache pattern
- #23 (typed enrichment structs) addresses the deepest design debt

### Phase 4: God Module Decomposition (Tier 3, weeks 5-8)

Execute in order of impact:
1. `executor.py` (#27) — benefits most from Phase 2-3 preparations
2. `smart_search.py` (#26) — benefits from #18 and #20
3. `calls.py` (#28) — benefits from #24 (SymbolIndex protocol)
4. `rust_lane/runtime.py` (#30) — benefits from #17
5. `report.py` (#31) and `runner.py` (#32) — smaller, can parallelize

---

## What's Working Well

The review identified several **systemic strengths** that should be preserved and extended:

1. **Composition over Inheritance (3.0/3):** Perfect score across all 7 subsystems. The codebase consistently uses function composition, frozen structs, and dataclass aggregation. Zero class hierarchies outside of stdlib patterns (`ast.NodeVisitor`, `ValueError` exceptions). This is the project's strongest design signal.

2. **Idempotency (2.9/3):** All operations are read-only analyses with content-addressed caching. Re-execution is always safe. Cache writes never corrupt on retry.

3. **Determinism (2.6/3):** Same inputs produce same outputs. Parallel results sorted by index. Hash-based IDs. `JSON_ENCODER` uses `order="deterministic"`. No non-deterministic paths in the core pipeline.

4. **Parse-Don't-Validate at Boundaries (2.3/3):** The typed boundary protocol (`convert_strict`, `decode_json_strict`, `decode_yaml_strict`) is consistently applied at CLI, TOML, and JSON boundaries. The query parser (text → frozen IR) is a textbook implementation.

5. **Contract-First Design (2.3/3):** Frozen msgspec structs, annotated constraint types (`PositiveInt`, `BoundedRatio`), V1 versioned contracts, and `__all__` exports on every module provide a strong foundation.

6. **Hexagonal Core (3/3 in Core):** The `ports.py` Protocol definitions, `CqCacheBackend` Protocol, and `bootstrap.py` composition root form a clean hexagonal architecture.

7. **Introspection Layer:** Exemplary separation of concerns, pure functions, stdlib-only dependencies. The best-designed module group in the codebase.

8. **LDMD and Neighborhood:** Clean, focused subsystems with good contract stability and minimal design debt.

---

## Metrics Summary

| Metric | Value |
|--------|-------|
| Total principles evaluated | 24 x 7 = 168 scores |
| Scores at 3/3 (well aligned) | 46 (27%) |
| Scores at 2/3 (mostly aligned) | 75 (45%) |
| Scores at 1/3 (significant gaps) | 42 (25%) |
| Scores at 0/3 (severe gaps) | 5 (3%) |
| Overall average | 1.9/3 |
| God Modules identified | 7 files, 16,942 combined LOC |
| Unique cross-cutting themes | 6 |
| Quick wins identified | 15 |
| Total improvement items | 32 |
