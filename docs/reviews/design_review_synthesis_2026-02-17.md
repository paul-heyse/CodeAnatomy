# Design Review Synthesis: `tools/cq` Comprehensive Assessment

**Date:** 2026-02-17
**Scope:** `tools/cq/` — 5 review agents covering ~80K LOC, 368 files across 14 subsystems
**Individual Reviews:**
- [Foundation Layer (core/)](design_review_core_foundation_2026-02-17.md)
- [Search Infrastructure](design_review_search_infrastructure_2026-02-17.md)
- [Search Pipeline + Enrichment](design_review_search_pipeline_2026-02-17.md)
- [Query Engine](design_review_query_engine_2026-02-17.md)
- [Integration Shell](design_review_integration_shell_2026-02-17.md)

---

## Unified 24-Principle Heatmap

Scores are per-agent (1-3 scale). **Bold** = systemic issue (1/3 in 3+ agents).

| # | Principle | Core | Search Infra | Search Pipeline | Query Engine | Integration Shell | Avg |
|---|-----------|------|-------------|----------------|-------------|-------------------|-----|
| **1** | Information hiding | **1** | 2 | 2 | 2 | 2 | 1.8 |
| **2** | Separation of concerns | 2 | 2 | **1** | **1** | **1** | 1.4 |
| **3** | SRP | 2 | 2 | **1** | **1** | 2 | 1.6 |
| 4 | High cohesion, low coupling | 2 | 2 | 2 | 2 | 2 | 2.0 |
| **5** | Dependency direction | **1** | 2 | 2 | 2 | 2 | 1.8 |
| 6 | Ports & Adapters | 3 | 2 | 2 | 2 | 2 | 2.2 |
| **7** | DRY (knowledge) | **1** | **1** | **1** | **1** | **1** | 1.0 |
| 8 | Design by contract | 2 | 3 | 2 | 2 | 2 | 2.2 |
| **9** | Parse, don't validate | 2 | 2 | **1** | 3 | 2 | 2.0 |
| **10** | Illegal states | **1** | 2 | 2 | 2 | 2 | 1.8 |
| **11** | CQS | 2 | 3 | 2 | **1** | **1** | 1.8 |
| 12 | DI + explicit composition | 2 | **1** | 2 | 2 | 2 | 1.8 |
| 13 | Composition over inheritance | 2 | 3 | 3 | 3 | 3 | 2.8 |
| 14 | Law of Demeter | 2 | 2 | 2 | 2 | 2 | 2.0 |
| **15** | Tell, don't ask | **1** | 2 | 2 | 2 | 2 | 1.8 |
| 16 | Functional core / imperative shell | 2 | 2 | 2 | 2 | **1** | 1.8 |
| 17 | Idempotency | 3 | 3 | 3 | 3 | 2 | 2.8 |
| 18 | Determinism / reproducibility | 3 | 3 | 2 | 3 | 2 | 2.6 |
| 19 | KISS | 2 | 2 | 2 | 2 | 2 | 2.0 |
| 20 | YAGNI | 2 | 3 | 2 | 2 | 2 | 2.2 |
| **21** | Least astonishment | **1** | 2 | 2 | **1** | 2 | 1.6 |
| 22 | Public contracts | 2 | 3 | 2 | 2 | 2 | 2.2 |
| 23 | Testability | 2 | 2 | 2 | 2 | **1** | 1.8 |
| 24 | Observability | 2 | 2 | 2 | 2 | 2 | 2.0 |
| | **Overall Average** | **1.83** | **2.25** | **1.92** | **2.00** | **1.83** | **1.97** |

### Heatmap Summary

**Systemic weaknesses (avg <= 1.5, 1/3 in 3+ agents):**
- **P7 DRY** (1.0) — Universal: cache policy duplication, lane canonicalization duplication, enrichment path duplication, entity pattern duplication, params/options duplication
- **P2 Separation of concerns** (1.4) — Three God modules: `assembly.py`, `executor_runtime.py`, and macros embedding domain logic

**Moderate weaknesses (avg 1.6-1.8):**
- **P3 SRP** (1.6) — `executor_runtime.py` (5 reasons to change), `smart_search.py` (entry point + utility library)
- **P21 Least astonishment** (1.6) — Dual `SummaryEnvelopeV1` names, dual dispatch facades, `_evidence_to_bucket` "medium"/"med" mismatch
- **P11 CQS** (1.8) — Pervasive `result.key_findings.append()` mutation, executor functions that mutate+return

**Systemic strengths (avg >= 2.5):**
- **P13 Composition over inheritance** (2.8) — Consistently excellent: protocols, adapters, and function composition throughout
- **P17 Idempotency** (2.8) — Content-hashed caching, deterministic IDs, additive operations
- **P18 Determinism** (2.6) — Explicit sorting, deterministic JSON encoding, canonical digests

---

## Cross-Agent Systemic Themes

### Theme 1: The `dict[str, object]` Payload Leak (4 agents)

**Where it appears:**
- **Core:** `DetailPayload.data` is a public mutable `dict[str, object]`; renderers reach in with `.get()` chains
- **Search Infra:** Enrichment payloads flow as untyped dicts through `objects/resolve.py` (8 functions doing `isinstance` guards)
- **Search Pipeline:** `PythonEnrichmentV1.payload` is `dict[str, object]` despite typed fact structs existing
- **Query Engine:** `Finding.details` interrogated with `.get()` and manual type checks in `entity_front_door.py`

**Root cause:** Enrichment pipelines were built bottom-up (accumulate dicts, wrap in typed struct at boundary). Typed fact structs were added later but never integrated into the accumulation path.

**Impact:** Undermines P9 (parse don't validate), P10 (illegal states), P14 (Law of Demeter), P15 (tell don't ask) across the entire codebase.

**Resolution path:** Have enrichment stages build typed structs directly. Convert to `dict[str, object]` only at serialization boundaries. Define typed view structs for `objects/resolve.py` to consume.

---

### Theme 2: Mutable Core Schema Types (3 agents)

**Where it appears:**
- **Core:** `Finding`, `CqResult`, `SummaryEnvelopeV1`, `DetailPayload` all lack `frozen=True` — any of 232 consuming files can mutate shared state
- **Search Pipeline:** `assembly.py` mutates `summary.tree_sitter_neighborhood` and `result.sections` inline during assembly
- **Integration Shell:** 61 occurrences of `result.(key_findings|evidence|sections).(append|extend|insert)` across 12 macro files

**Root cause:** Core schema types predate the `CqStruct(frozen=True)` convention. They were designed for incremental construction, conflicting with the immutability discipline applied everywhere else.

**Impact:** Undermines P1 (information hiding), P10 (illegal states), P11 (CQS), P15 (tell don't ask) — the highest-leverage single fix.

**Resolution path:** Make core types frozen. Adopt `msgspec.structs.replace()` for mutations. Extend `MacroResultBuilder` to absorb all result construction. The `assign_result_finding_ids()` function already demonstrates the pattern.

---

### Theme 3: God Modules (3 agents)

**Where it appears:**
- **Search Pipeline:** `assembly.py` (686 LOC) — object resolution, neighborhood preview, insight card, cache maintenance, result packaging
- **Query Engine:** `executor_runtime.py` (1126 LOC, 28 imports) — scan, cache, dispatch, summary, execution
- **Integration Shell:** `calls/entry.py` (22 import statements, 15+ modules) — scan + analysis + neighborhood + semantic + insight + result

**Root cause:** Convergence points that naturally accumulate responsibilities without decomposition gates.

**Impact:** Undermines P2 (separation of concerns), P3 (SRP), P4 (coupling), P23 (testability).

**Resolution path:** Extract focused modules along concern axes: scan, cache/fragments, summary, and analysis. Each God module becomes a thin orchestrator delegating to extracted modules.

---

### Theme 4: Knowledge Duplication Across Language Paths (3 agents)

**Where it appears:**
- **Search Infra:** Python/Rust `canonicalize_*_lane_payload` functions are near-identical
- **Search Pipeline:** `_accumulate_runtime_flags` duplicated verbatim between Python/Rust adapters; diagnostics row construction duplicated between `semantic/models.py` and both adapters; `_evidence_to_bucket` returns "medium" in one file and "med" in another
- **Query Engine:** Entity-to-pattern maps duplicated between `planner.py` and `executor_definitions.py`; parser validation tuples duplicate IR `Literal` types

**Root cause:** Rust support was added by copying Python patterns without extracting shared abstractions.

**Impact:** Drift between paths causes inconsistent behavior, silent bugs (the "medium"/"med" mismatch), and duplicated maintenance burden.

**Resolution path:** Extract shared lane protocol functions, shared diagnostic builders, and derive validation constants from canonical type definitions.

---

### Theme 5: CQS Violations in Result Construction (3 agents)

**Where it appears:**
- **Core:** `apply_summary_mapping()` mutates in place within what looks like a factory; `merge_enrichment_details()` mutates during rendering
- **Query Engine:** `_apply_entity_handlers`, `_process_decorator_query`, `_process_call_query` all mutate `result.key_findings` and `result.summary`
- **Integration Shell:** 61 `result.*.append()` calls across 12 macro files; `_build_launch_context` mutates `app.config` while returning a value

**Root cause:** Mutable `CqResult` fields (`list[Finding]`, `list[Section]`) invite scatter-shot mutation. `MacroResultBuilder` exists but is only partially adopted.

**Impact:** Makes it impossible to reason about result state at any point during execution.

**Resolution path:** Freeze core types (Theme 2) and fully adopt `MacroResultBuilder`. Executor functions should return `list[Finding]` and let callers merge. Assembly should be the single mutation point.

---

### Theme 6: Macros as Thick Shell (Integration Shell only, but high leverage)

**Where it appears:**
- **Integration Shell:** `TaintVisitor` (290 lines of pure AST analysis), `_parse_signature` (standalone parser), call-site classification — all interleaved with filesystem IO in the shell layer

**Root cause:** Macros grew as feature-specific modules; analysis logic was never extracted.

**Impact:** Domain logic cannot be unit tested without filesystem setup. Changes to analysis require modifying the integration layer.

**Resolution path:** Introduce `tools/cq/analysis/` package with pure analysis functions (taint, signature, call classification). Macros become thin orchestrators: read → analyze (pure) → build result.

---

## Unified Quick Wins (Ranked by Cross-Agent Impact)

| Rank | Principles | Finding | Effort | Impact | Source |
|------|-----------|---------|--------|--------|--------|
| 1 | P7, P21 | Delete duplicate `_category_message`/`_evidence_to_bucket` from `smart_search.py` (fixes "medium"/"med" semantic mismatch) | small | Eliminates active bug across pipeline | Pipeline |
| 2 | P21 | Rename `summary_contracts.py:SummaryEnvelopeV1` to `SummaryOutputEnvelopeV1` | small | Prevents import confusion for all developers | Core |
| 3 | P21, P1 | Remove dual dispatch facades (`executor.py` + `executor_dispatch.py`) in query engine | small | Eliminates the most confusing navigation in the codebase | Query |
| 4 | P7 | Unify `canonicalize_python_lane_payload`/`canonicalize_rust_lane_payload` into generic function | small | Eliminates duplicated canonicalization knowledge | Search Infra |
| 5 | P10 | Replace `enrichment_status: str` with `Literal["applied", "degraded", "skipped"]` | small | Prevents silent typo bugs in status tracking | Search Infra |
| 6 | P7 | Extract `_accumulate_runtime_flags` to `enrichment/core.py` as shared helper | small | Single source of truth for telemetry accumulation | Pipeline |
| 7 | P20 | Delete dead `_apply_prefetched_search_semantic_outcome` stub | small | Removes dead code path | Pipeline |
| 8 | P8 | Add `NonEmptyStr` to `Finding.category`, `NonNegativeFloat` to `RunMeta` timestamps | small | Catches invalid data at construction | Core |
| 9 | P8 | Add assertions at executor entry points for plan mode validation | small | Documents contracts, prevents dispatch errors | Query |
| 10 | P7 | Derive parser validation tuples from IR `Literal` types via `get_args()` | small | Single source of truth for entity/mode types | Query |
| 11 | P19 | Fix `run_search_partition` return type, delete `enrichment_phase.py` | small | Eliminates unnecessary indirection module | Pipeline |
| 12 | P11 | Remove `app.config` mutation from `_build_launch_context` | small | Eliminates hidden global side effect | Shell |
| 13 | P10 | Default `CliContext.output_format` to `OutputFormat.md` instead of `None` | small | Removes scattered None-handling | Shell |
| 14 | P5 | Move `DEFAULT`/`INTERACTIVE` profiles from `pipeline/profiles` to `_shared/` | small | Corrects inverted dependency | Search Infra |
| 15 | P22 | Add `__all__` to `impact.py` and `options.py` | small | Declares public API surface | Shell |

---

## Combined Action Plan (Dependency-Ordered)

### Phase 1: Safe Deletions and Renames (all small effort, zero risk)

1. **Delete duplicates:** Remove `_category_message`, `_evidence_to_bucket` from `smart_search.py`. Remove dead `_apply_prefetched_search_semantic_outcome`. Standardize "medium"/"med" bucket naming.
2. **Rename collision:** Rename `summary_contracts.py:SummaryEnvelopeV1` to `SummaryOutputEnvelopeV1`.
3. **Remove dispatch facades:** Delete `query/executor.py` and `query/executor_dispatch.py`. Update imports to form a single dispatch chain.
4. **Add constraints:** `NonEmptyStr` on `Finding.category`, `NonNegativeFloat` on `RunMeta` timestamps. Add executor entry-point assertions.
5. **Tighten types:** Replace bare `str` with `Literal` for `enrichment_status`, `resolution_quality`, `coverage_level`. Default `CliContext.output_format` to `OutputFormat.md`.
6. **Fix minor inversions:** Move profile constants to `_shared/`. Derive parser validation from IR types. Add `__all__` exports.

### Phase 2: DRY Consolidation (small-medium effort, low risk)

7. **Unify lane canonicalization:** Extract generic `_canonicalize_lane_payload(payload, target_type)` in `contracts/lane_payloads.py`.
8. **Consolidate enrichment helpers:** Move `_accumulate_runtime_flags` and diagnostic row construction to `enrichment/core.py`. Update both adapters and `semantic/models.py`.
9. **Consolidate cache policy:** Make `RuntimeExecutionPolicy.cache` use `CqCachePolicyV1` directly. Remove `CacheRuntimePolicy`.
10. **Derive payload field sets from structs:** Replace `_PY_RESOLUTION_KEYS` etc. with values derived from `__struct_fields__`.

### Phase 3: Structural Extraction (medium effort, moderate risk)

11. **Extract from executor_runtime.py:** Create `query_scan.py`, `query_summary.py`, `fragment_cache.py`. Runtime becomes thin orchestrator.
12. **Extract from assembly.py:** Create `target_resolution.py` and `neighborhood_preview.py`. Extract cache maintenance to post-assembly hook.
13. **Extract from macros:** Move `TaintVisitor` to `analysis/taint.py`, `_parse_signature` to `analysis/signature.py`, call classification to `analysis/calls.py`.
14. **Split smart_search.py:** Move scoring to `relevance.py`, coercion helpers to `request_parsing.py`.
15. **Fix core→search reverse deps:** Move `IncrementalEnrichmentModeV1` to core. Accept primitive types in `SearchServiceRequest`.

### Phase 4: Immutability Migration (medium-large effort, high leverage)

16. **Freeze core schema types:** Make `Finding`, `CqResult`, `SummaryEnvelopeV1`, `DetailPayload` frozen. Replace all mutation with `msgspec.structs.replace()`.
17. **Extend MacroResultBuilder:** Add semantic methods for all finding/section/evidence additions. Migrate all 12 macro files.
18. **Refactor executor CQS:** `_apply_entity_handlers` etc. return `list[Finding]` instead of mutating result. Centralize result mutation.

### Phase 5: Typed Internal Flow (medium effort, deepest improvement)

19. **Define enrichment payload view structs:** `SymbolGroundingV1`, `ResolutionPayloadV1`, `StructuralPayloadV1` in `objects/payload_views.py`.
20. **Build typed enrichment facts directly:** Refactor `enrich_python_context` to build `PythonEnrichmentFacts` from stages, converting to dict only at serialization.
21. **Add DI seams:** Optional `parse_session`, `cache_backend` parameters on enrichment entry points.
22. **Add Rust enrichment stage timings:** Instrument to match Python observability parity.

---

## Architectural Strengths to Preserve

These patterns scored consistently high (2.5+/3) across multiple agents:

1. **Composition over inheritance (2.8)** — Protocols, adapters, and function composition are used throughout. No deep class hierarchies. Preserve this.
2. **Idempotency (2.8)** — Content-hashed caching, deterministic IDs, additive operations. The cache infrastructure is robust.
3. **Determinism (2.6)** — Explicit sorting, deterministic JSON encoding (`order="deterministic"`), canonical SHA-256 digests. Reproducibility is baked in.
4. **IR pipeline design (Query Engine)** — `ir.py` → `parser.py` → `planner.py` is a model of frozen structs, parse-don't-validate, and minimal dependencies. Other subsystems should emulate this.
5. **Contract versioning (Search Infra)** — `V1` suffixes, frozen `msgspec.Struct` base classes, explicit `__all__`. The strongest contract discipline in the codebase.
6. **Hexagonal ports (Core)** — `ports.py` with Protocol-based interfaces, `NoopCacheBackend` for testing. Clean adapter implementations.

---

## Risk Assessment

| Phase | Files Touched | Breaking Change Risk | Recommended Testing |
|-------|--------------|---------------------|-------------------|
| 1 | ~15 | None | Unit tests for renamed/deleted symbols |
| 2 | ~20 | None (internal consolidation) | Unit tests for consolidated helpers |
| 3 | ~30 | Low (internal restructuring) | Full test suite + integration |
| 4 | ~50+ | Medium (mutation sites become errors) | Full test suite; freeze one type at a time |
| 5 | ~25 | Low (additive typed wrappers) | Unit tests for new structs + integration |
