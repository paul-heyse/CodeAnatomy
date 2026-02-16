# Design Review: CQ Search Pipeline and Supporting Infrastructure

**Date:** 2026-02-16
**Scope:** `tools/cq/search/pipeline/` + `tools/cq/search/_shared/` + `tools/cq/search/rg/` + `tools/cq/search/objects/` + `tools/cq/search/semantic/` + `tools/cq/search/cache/` + `tools/cq/search/generated/`
**Focus:** Boundaries (1-6), Knowledge (7-11), Correctness (16-18)
**Depth:** moderate
**Files reviewed:** 20 of 49 total

## Executive Summary

The CQ search pipeline demonstrates strong contract typing (msgspec structs, frozen dataclasses) and a well-reasoned phased architecture (candidate, classify, enrich, assemble). However, the central `smart_search.py` (1,981 LOC) concentrates too many responsibilities -- candidate collection, classification orchestration, relevance scoring, section rendering, summary construction, and followup generation all live in a single module. This creates a systemic SRP violation that the recent extraction of `candidate_phase.py`, `classify_phase.py`, `enrichment_phase.py`, `smart_search_sections.py`, `smart_search_summary.py`, and `smart_search_followups.py` has begun to address, but the original implementations remain duplicated in `smart_search.py`. The `classifier_runtime.py` module uses a process-level singleton cache (`_DEFAULT_CACHE_CONTEXT`) that is well-encapsulated in a `ClassifierCacheContext` class but reaches callers via module-level fallback resolution, creating implicit coupling. Enrichment payloads use `dict[str, object]` throughout, forgoing the type safety that msgspec provides elsewhere in the pipeline.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `smart_search.py` exports 18 symbols including internal helpers; cache context uses module-level singleton |
| 2 | Separation of concerns | 1 | large | high | `smart_search.py` mixes candidate collection, classification, scoring, rendering, and summary in one module |
| 3 | SRP | 1 | large | high | `smart_search.py` has at least 6 reasons to change; extracted phase modules duplicate rather than replace |
| 4 | High cohesion, low coupling | 1 | medium | medium | Phase modules import heavily from `smart_search.py` and each other in circular patterns |
| 5 | Dependency direction | 2 | medium | medium | Pipeline phases depend on shared types correctly; `assembly.py` imports from `smart_search.py` for `classify_match` |
| 6 | Ports & Adapters | 2 | medium | low | `rg/runner.py` is a clean adapter; `semantic/front_door.py` is a clean port; enrichment providers lack port abstraction |
| 7 | DRY | 0 | medium | high | `collect_candidates`, `_build_search_stats`, `_identifier_pattern`, `_symtable_flags`, `_merge_enrichment_payloads` are duplicated verbatim between `smart_search.py` and extracted phase modules |
| 8 | Design by contract | 2 | small | low | Strong use of frozen msgspec structs and typed dataclasses; `SearchLimits` uses constrained types |
| 9 | Parse, don't validate | 1 | medium | medium | Enrichment payloads remain `dict[str, object]` throughout; `_coerce_search_request` manually validates kwargs |
| 10 | Make illegal states unrepresentable | 2 | small | low | `MatchCategory` is a Literal union; `QueryMode` is an Enum; `SearchResultAssembly.partition_results` typed as `list[object]` |
| 11 | CQS | 2 | small | low | `_build_search_context` calls `clear_caches()` as side effect before returning config; minor violation |
| 16 | Functional core, imperative shell | 2 | medium | low | Classification and scoring logic is pure; cache/IO in `partition_pipeline.py` is well-separated |
| 17 | Idempotency | 3 | - | - | Search operations are read-only; cache writes use content-hash keys; re-running produces same results |
| 18 | Determinism | 2 | small | low | `build_rg_command` is deterministic; match ordering depends on `rg` output order; final sort by relevance is deterministic |

## Detailed Findings

### Category: Boundaries (Principles 1-6)

#### P1. Information Hiding -- Alignment: 2/3

**Current state:**
The pipeline's internal cache machinery is well-encapsulated inside `ClassifierCacheContext` (`classifier_runtime.py:64-93`). However, the module-level singleton `_DEFAULT_CACHE_CONTEXT` at line 95 is accessed via `_resolve_cache_context()` as implicit fallback in every cache-aware function (e.g., `get_sg_root` at line 424, `get_cached_source` at line 459). This means callers get correct behavior by default but cannot reason about which cache instance is active without reading the fallback logic.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classifier_runtime.py:95`: `_DEFAULT_CACHE_CONTEXT = ClassifierCacheContext()` is a module-level mutable singleton.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classifier_runtime.py:103-106`: `_resolve_cache_context()` silently falls back to global singleton when `None` is passed.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:1964-1981`: `__all__` exports 18 symbols including `assemble_smart_search_result` (assembly internals) and `pop_search_object_view_for_run` (cache side-effect).
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/runtime_context.py:23-33`: `build_search_runtime_context` creates a `SearchRuntimeContext` but its `classifier_cache` is never threaded to the actual classification functions -- they still use the global singleton.

**Suggested improvement:**
Thread the `ClassifierCacheContext` from `SearchRuntimeContext` through phase execution so that each search invocation operates on an explicit cache instance. Remove the `_resolve_cache_context` fallback pattern and make the `cache_context` parameter required in the four cache-aware functions (`get_sg_root`, `get_cached_source`, `get_symtable_table`, `get_def_lines_cached`).

**Effort:** medium
**Risk if unaddressed:** low -- The global singleton works correctly for single-threaded use, but process-pool classification creates fresh instances per worker anyway.

---

#### P2. Separation of Concerns -- Alignment: 1/3

**Current state:**
`smart_search.py` is 1,981 LOC and contains code for at least six distinct concerns: (1) candidate collection (`collect_candidates`, `_run_candidate_phase`), (2) match classification (`classify_match`, `_resolve_match_classification`), (3) enrichment orchestration (`_maybe_python_enrichment`, `_maybe_rust_tree_sitter_enrichment`), (4) relevance scoring (`compute_relevance_score`, `KIND_WEIGHTS`), (5) section rendering (`build_sections`, `build_followups`), and (6) summary construction (`build_summary`, `_build_summary`).

Recent extractions created dedicated modules (`candidate_phase.py`, `classify_phase.py`, `enrichment_phase.py`, `smart_search_sections.py`, `smart_search_summary.py`, `smart_search_followups.py`), but the original implementations remain in `smart_search.py` as the canonical versions that the extracted modules import from or duplicate.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:358-410`: `collect_candidates` -- candidate collection logic.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:413-517`: `classify_match` -- 104-line classification orchestrator.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:910-943`: `compute_relevance_score` -- scoring logic.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:1208-1310`: `_build_summary` -- 100-line summary constructor.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:1313-1375`: `build_sections` -- section assembly.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:1129-1205`: `build_followups` -- followup generation.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classify_phase.py:23`: `_classify_partition_batch` imports `classify_match` from `smart_search` via lazy import inside function body.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/partition_pipeline.py:493`: `_classify_enrichment_miss_batch` also imports `classify_match` from `smart_search` lazily.

**Suggested improvement:**
Complete the extraction: make the dedicated phase modules (`candidate_phase.py`, `classify_phase.py`, `smart_search_sections.py`, `smart_search_summary.py`, `smart_search_followups.py`) the authoritative implementations and reduce `smart_search.py` to a thin orchestrator that imports from them. Move `classify_match` to a `classification.py` module to break the circular import pattern where phase modules lazily import from the orchestrator.

**Effort:** large
**Risk if unaddressed:** high -- Every new feature or bugfix to any pipeline phase requires understanding the full 1,981-line file; bugs from stale duplicate logic are likely.

---

#### P3. SRP (One Reason to Change) -- Alignment: 1/3

**Current state:**
`smart_search.py` would need to change for any of: (a) ripgrep candidate collection changes, (b) AST classification logic changes, (c) enrichment provider changes, (d) relevance scoring algorithm changes, (e) output rendering format changes, (f) summary payload schema changes.

**Findings:**
- Same evidence as P2 -- the six concerns represent six distinct change vectors.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:883-907`: `_classify_file_role` is a rendering concern embedded in the classification module.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:1039-1105`: `_build_match_data`, `_populate_optional_fields`, `_merge_enrichment_payloads` are output serialization concerns.

**Suggested improvement:**
Same as P2: complete the extraction. Additionally, move `_classify_file_role` and relevance scoring to a dedicated `scoring.py` module, and move `_build_match_data`/`_merge_enrichment_payloads` to `smart_search_sections.py` (where duplicates already exist).

**Effort:** large
**Risk if unaddressed:** high -- Changes to any concern risk regressions in unrelated concerns.

---

#### P4. High Cohesion, Low Coupling -- Alignment: 1/3

**Current state:**
The extracted phase modules have high cohesion individually but are tightly coupled to `smart_search.py` through lazy imports. The circular dependency pattern (`classify_phase.py` -> `smart_search.py:classify_match` -> `classifier.py` -> `classifier_runtime.py`) creates a fragile import graph.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classify_phase.py:23`: Lazy import of `classify_match` from `smart_search` inside `_classify_partition_batch`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classify_phase.py:60`: Second lazy import of `classify_match` inside single-threaded fallback path.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classify_phase.py:80`: Third lazy import in timeout fallback path.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classify_phase.py:94`: Fourth lazy import in exception fallback path.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/partition_pipeline.py:493`: Fifth lazy import of `classify_match` in enrichment miss batch.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/assembly.py:19`: `assembly.py` imports `get_sg_root` from `classifier` and `classify_match` indirectly.

**Suggested improvement:**
Extract `classify_match` and its immediate helpers (`_resolve_match_classification`, `_classify_from_node`, `_maybe_symtable_enrichment`, `_build_context_enrichment`, `_maybe_python_enrichment`, `_maybe_rust_tree_sitter_enrichment`, `_maybe_python_semantic_enrichment`) into a dedicated `classification.py` module. This module would depend on `classifier.py` and `classifier_runtime.py` (downward), and the phase modules would import from it (no circular dependency).

**Effort:** medium
**Risk if unaddressed:** medium -- Lazy imports mask circular dependencies that could break under import-order changes.

---

#### P5. Dependency Direction -- Alignment: 2/3

**Current state:**
The dependency direction is mostly correct: types flow from `smart_search_types.py` and `contracts.py` (leaf modules) upward through classifier, enrichment, and orchestration layers. The `_shared/` package provides foundation types (`QueryMode`, `SearchLimits`) that higher layers depend on. The main violation is that `assembly.py` (assembly layer) reaches back into `classifier.py` for `get_sg_root` to check AST parsability.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/assembly.py:19`: Assembly imports `get_sg_root` from classifier layer -- assembly should not depend on classification internals.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/assembly.py:252-280`: `_ast_grep_prefilter_scope_paths` uses `get_sg_root` to check parsability, which is a classification-layer concern leaking into assembly.

**Suggested improvement:**
Move the `_ast_grep_prefilter_scope_paths` helper into the candidate or enrichment phase where AST parsability is a natural concern, and pass pre-filtered scope paths into assembly.

**Effort:** small
**Risk if unaddressed:** low -- The dependency direction violation is isolated.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The ripgrep integration (`rg/runner.py`) is a well-structured adapter with clear request/response contracts (`RgRunRequest`, `RgProcessResult`, `RgCountRequest`). The semantic enrichment front door (`semantic/front_door.py`) provides a clean language-dispatched facade. However, the enrichment providers for Python and Rust are called directly via concrete functions (`enrich_python_context_by_byte_range`, `enrich_rust_context_by_byte_range`) without a port abstraction, making it harder to add new language enrichment providers.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/rg/runner.py:181-210`: `build_rg_command` cleanly separates command construction from execution -- good adapter pattern.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/semantic/front_door.py:214-218`: `_execute_provider` dispatches via dict lookup `{"python": ..., "rust": ...}` -- ad-hoc dispatch rather than a protocol.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:762-789`: `_maybe_rust_tree_sitter_enrichment` calls `enrich_rust_context_by_byte_range` directly.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:792-851`: `_maybe_python_enrichment` calls `enrich_python_context_by_byte_range` directly.

**Suggested improvement:**
Define a `LanguageEnrichmentProvider` protocol with a `enrich_by_byte_range(source_bytes, byte_start, byte_end, cache_key, budget_ms)` method. Register concrete implementations for Python and Rust. This would let the classification code dispatch to a provider interface instead of language-conditional branches.

**Effort:** medium
**Risk if unaddressed:** low -- The current two-language approach works; this becomes important if/when more languages are added.

---

### Category: Knowledge (Principles 7-11)

#### P7. DRY (Knowledge, Not Lines) -- Alignment: 0/3

**Current state:**
This is the most severe violation in the scope. The recent phase extraction created duplicate implementations of core pipeline functions. The same business logic exists in two places, and both are actively reachable.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:358-410` duplicates `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/candidate_phase.py:88-123` -- `collect_candidates` is implemented in both files with identical logic.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:325-355` duplicates `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/candidate_phase.py:55-85` -- `_build_search_stats` is implemented in both.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:286-292` duplicates `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/candidate_phase.py:26-27` -- `_identifier_pattern`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:1108-1126` duplicates `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_sections.py:61-79` -- `_symtable_flags`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:1076-1105` duplicates `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_sections.py:97-115` -- `_merge_enrichment_payloads`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:1052-1073` duplicates `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_sections.py:82-94` -- `_populate_optional_fields`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:1129-1205` duplicates `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_followups.py:10-77` -- `build_followups`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:1219-1310` duplicates `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_summary.py:38-134` -- `_build_summary` / `build_language_summary`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:1550-1601` duplicates `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classify_phase.py:35-99` -- `_run_classification_phase` / `run_classify_phase`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:1796-1879` duplicates `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_summary.py:206-296` -- `_build_search_summary` / `build_search_summary`.

**Suggested improvement:**
Complete the extraction by making the extracted modules authoritative and removing the duplicate implementations from `smart_search.py`. `smart_search.py` should import from the extracted modules rather than maintaining its own copies. This is a single coordinated change that removes ~500 lines of duplication.

**Effort:** medium
**Risk if unaddressed:** high -- Any bugfix applied to one copy but not the other will create silent behavioral divergence. This is already the kind of drift that is hardest to debug.

---

#### P8. Design by Contract -- Alignment: 2/3

**Current state:**
The pipeline makes good use of frozen msgspec structs with constrained types for its core contracts. `SearchLimits` uses `PositiveInt` and `PositiveFloat` constraints. `RawMatch`, `EnrichedMatch`, `SearchStats` are frozen structs with clear field semantics. `SearchPartitionPlanV1` and `SearchConfig` provide well-documented configuration contracts.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/_shared/types.py:22-53`: `SearchLimits` uses `PositiveInt` and `PositiveFloat` constraints -- strong contract enforcement.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/contracts.py:21-33`: `SearchPartitionPlanV1` is a clean serializable contract.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/cache/contracts.py:43-52`: `SearchEnrichmentAnchorCacheV1` uses `NonNegativeInt` constraints on line/col.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:1882-1939`: `smart_search()` accepts `**kwargs: object` and manually coerces each field in `_coerce_search_request` -- the public API lacks typed contract enforcement.

**Suggested improvement:**
Replace the `**kwargs: object` signature of `smart_search()` with the already-existing `SearchRequest` struct as the public API entry point. The coercion logic in `_coerce_search_request` can remain as an internal compatibility adapter for legacy callers.

**Effort:** small
**Risk if unaddressed:** low -- The current coercion works but doesn't document or enforce the contract at the API surface.

---

#### P9. Parse, Don't Validate -- Alignment: 1/3

**Current state:**
Enrichment payloads flow through the pipeline as `dict[str, object]`, requiring repeated `isinstance` checks at every access point. The `_payload_views` function in `objects/resolve.py` extracts structured sub-payloads but returns them as untyped dicts. The `search_semantic.py` module contains extensive defensive type checking (`isinstance(status, str)`, `isinstance(coverage, dict)`, etc.) because payloads are never parsed into typed representations.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_types.py:127-129`: `MatchEnrichment` contains `rust_tree_sitter: dict[str, object] | None`, `python_enrichment: dict[str, object] | None`, `python_semantic_enrichment: dict[str, object] | None` -- three untyped payload fields.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_types.py:219-222`: `EnrichedMatch` carries the same three `dict[str, object]` fields through the entire pipeline.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/search_semantic.py:44-69`: `_payload_coverage_status` performs 8 `isinstance` checks to extract two strings from an untyped dict.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/search_semantic.py:72-104`: `_rust_payload_has_signal` performs 10+ `isinstance` checks on nested dict fields.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/objects/resolve.py:144-192`: `_payload_views` performs 15+ `isinstance` checks to decompose enrichment payloads.

**Suggested improvement:**
Define msgspec structs for the three enrichment payload types: `RustTreeSitterEnrichmentV1`, `PythonEnrichmentV1`, `PythonSemanticEnrichmentV1`. Parse raw dict payloads into these structs at the enrichment boundary (where `normalize_python_payload`/`normalize_rust_payload` already run), and carry the typed structs through the pipeline. This eliminates hundreds of scattered `isinstance` checks downstream.

**Effort:** medium
**Risk if unaddressed:** medium -- Untyped payloads are the primary source of defensive coding throughout the pipeline. Missing an `isinstance` check on a new field path silently produces `None` or wrong-type values.

---

#### P10. Make Illegal States Unrepresentable -- Alignment: 2/3

**Current state:**
Core types are well-modeled. `MatchCategory` is a `Literal` union preventing invalid categories. `QueryMode` is an `Enum`. `SearchLimits` uses constrained positive types. However, `SearchResultAssembly.partition_results` is typed as `list[object]` (line 338 of `smart_search_types.py`), which permits any list content.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_types.py:333-338`: `SearchResultAssembly.partition_results: list[object]` -- should be `list[LanguageSearchResult]`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:1949`: `assemble_result` casts `assembly.partition_results` to `list[LanguageSearchResult]` -- the cast would be unnecessary with correct typing.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/partition_pipeline.py:90-95`: `run_search_partition` returns `object` instead of `LanguageSearchResult` -- the cast in `enrichment_phase.py:26` compensates.

**Suggested improvement:**
Change `SearchResultAssembly.partition_results` to `list[LanguageSearchResult]` and `run_search_partition` return type to `LanguageSearchResult`. Remove the compensating casts.

**Effort:** small
**Risk if unaddressed:** low -- The casts work but hide type errors that could be caught at definition time.

---

#### P11. CQS (Command-Query Separation) -- Alignment: 2/3

**Current state:**
Most pipeline functions follow CQS well. The one notable violation is `_build_search_context` which performs a side effect (`clear_caches()`) before returning a value.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:1406-1434`: `_build_search_context` calls `clear_caches()` at line 1414 as a side effect while constructing and returning `SearchConfig`. Cache clearing is a command; config construction is a query.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/assembly.py:688-696`: `_assemble_smart_search_result` calls `register_search_object_view` (a command) as part of result construction (a query) -- minor CQS violation.

**Suggested improvement:**
Move `clear_caches()` to the beginning of `smart_search()` (the top-level orchestrator) before calling `_build_search_context`, making the side effect visible at the orchestration level rather than hidden inside a construction function.

**Effort:** small
**Risk if unaddressed:** low -- The violation is localized and does not affect correctness.

---

### Category: Correctness (Principles 16-18)

#### P16. Functional Core, Imperative Shell -- Alignment: 2/3

**Current state:**
The classification logic (`classify_heuristic`, `classify_from_node`, `classify_from_records`) is pure and deterministic, forming a genuine functional core. Scoring (`compute_relevance_score`) is pure. The imperative shell is concentrated in `partition_pipeline.py` (cache I/O, worker scheduling) and `rg/runner.py` (subprocess execution). The boundary between pure and impure is somewhat blurred in `classify_match` which mixes pure classification with impure file I/O (`get_cached_source` reads files).

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classifier.py:310-388`: `classify_heuristic` is pure -- good functional core.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classifier.py:391-460`: `classify_from_node` and `classify_from_resolved_node` are pure given a parsed AST.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:413-517`: `classify_match` mixes pure classification with impure I/O (`get_cached_source`, `get_sg_root` read files).
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/rg/runner.py:225-276`: `run_rg_json` is a clean imperative shell function.

**Suggested improvement:**
Extract the pure classification decision logic from `classify_match` into a function that takes pre-loaded source and AST as parameters. Let the impure caller (`classify_match` or `_classify_enrichment_miss_batch`) handle file loading.

**Effort:** medium
**Risk if unaddressed:** low -- The current structure works but makes unit-testing classification logic harder than necessary.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Search operations are inherently idempotent: re-running the same query with the same inputs produces the same outputs. Cache writes use content-hash-based keys, so writing the same result twice is a no-op. The `file_content_hash` in enrichment cache keys ensures stale results are never returned for changed files.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/partition_pipeline.py:344-371`: Enrichment cache keys include `file_content_hash` -- ensures cache correctness after file changes.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/partition_pipeline.py:196-209`: Candidate cache keys include `snapshot_digest` -- ensures partition-level correctness.

No action needed.

---

#### P18. Determinism / Reproducibility -- Alignment: 2/3

**Current state:**
Ripgrep command construction is deterministic (`build_rg_command`). Match output order depends on `rg`'s traversal order, which can vary across runs (file modification times, filesystem ordering). The pipeline applies a deterministic relevance sort afterward, but intermediate processing order is not guaranteed. The parallel classification phase preserves original index ordering via `indexed_results.sort()`.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/rg/runner.py:181-210`: `build_rg_command` builds a deterministic command from structured inputs.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classify_phase.py:98`: Classification results are sorted by original index after parallel execution -- preserves deterministic ordering.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:1600`: Same pattern in the duplicate classification phase.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/objects/resolve.py:117-132`: Summaries and occurrences are sorted deterministically by occurrence count, symbol, file, and line.

**Suggested improvement:**
If fully reproducible output is required regardless of filesystem traversal order, add `--sort path` to the default `SearchLimits` configuration (currently `sort_by_path: bool = False`). This trades some performance for guaranteed determinism.

**Effort:** small
**Risk if unaddressed:** low -- Non-deterministic intermediate ordering is masked by final deterministic sort.

---

## Cross-Cutting Themes

### Theme 1: Incomplete Extraction (Root Cause of P2, P3, P4, P7)

The recent extraction of phase modules (`candidate_phase.py`, `classify_phase.py`, `smart_search_sections.py`, `smart_search_summary.py`, `smart_search_followups.py`, `enrichment_phase.py`) was structurally sound but incomplete. The original implementations remain in `smart_search.py`, and the extracted modules either duplicate the code or lazily import from the original. This is the single root cause behind the SRP, DRY, coupling, and separation-of-concerns violations.

**Affected principles:** P2, P3, P4, P7
**Suggested approach:** Complete the extraction in a single coordinated change:
1. Move `classify_match` and its helpers to a new `classification.py` module
2. Make extracted phase modules the authoritative implementations
3. Remove duplicates from `smart_search.py`
4. Reduce `smart_search.py` to a thin orchestrator (target: ~400 LOC)

### Theme 2: Untyped Enrichment Payloads (Root Cause of P9)

The three enrichment payload fields on `EnrichedMatch` (`rust_tree_sitter`, `python_enrichment`, `python_semantic_enrichment`) are all `dict[str, object]`. This forces every downstream consumer to perform defensive type checking. The normalization functions (`normalize_python_payload`, `normalize_rust_payload`) already exist at the boundary -- they just produce dicts instead of typed structs.

**Affected principles:** P9, P8
**Suggested approach:** Define three msgspec structs for enrichment payloads and parse into them at the normalization boundary. This is a medium-effort change that eliminates ~200 scattered `isinstance` checks.

### Theme 3: Well-Designed Foundation Types

Despite the issues above, the foundation type layer (`_shared/types.py`, `smart_search_types.py`, `contracts.py`, `cache/contracts.py`) is well-designed. Frozen structs with constrained types, clear separation of serializable contracts from runtime handles, and consistent use of `CqStruct`/`CqSettingsStruct`/`CqCacheStruct` base classes demonstrate strong type discipline. The `_shared/core.py` module cleanly separates serializable settings from runtime-only handles via `to_settings()`/`to_runtime()` methods.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Remove duplicate `_symtable_flags`, `_merge_enrichment_payloads`, `_populate_optional_fields`, `build_followups` from `smart_search.py` -- import from extracted modules instead | small | Eliminates 4 duplicate function pairs (~80 lines) |
| 2 | P10 | Change `SearchResultAssembly.partition_results` from `list[object]` to `list[LanguageSearchResult]`; fix `run_search_partition` return type | small | Removes 3 compensating casts; catches type errors at definition |
| 3 | P11 | Move `clear_caches()` from `_build_search_context` to `smart_search()` top | small | Makes side effect visible at orchestration level |
| 4 | P8 | Replace `smart_search(**kwargs: object)` with `smart_search(request: SearchRequest)` as primary API | small | Self-documenting API; removes ~40 lines of coercion |
| 5 | P5 | Move `_ast_grep_prefilter_scope_paths` from `assembly.py` to enrichment phase | small | Removes classifier dependency from assembly layer |

## Recommended Action Sequence

1. **Quick wins first** (P7 small, P10, P11, P8, P5) -- These are independent, low-risk changes that improve code health immediately. Complete all 5 in a single PR.

2. **Extract `classification.py`** (P2, P3, P4) -- Move `classify_match` and its 8 helper functions out of `smart_search.py` into a dedicated module. This breaks the circular import pattern and enables step 3.

3. **Complete phase extraction** (P2, P3, P7) -- Make `candidate_phase.py`, `classify_phase.py`, `smart_search_sections.py`, `smart_search_summary.py` authoritative. Remove ~500 lines of duplicates from `smart_search.py`.

4. **Type enrichment payloads** (P9) -- Define msgspec structs for `RustTreeSitterEnrichmentV1`, `PythonEnrichmentV1`, `PythonSemanticEnrichmentV1`. Parse at normalization boundary. Update `EnrichedMatch` fields to use typed structs.

5. **Thread `ClassifierCacheContext`** (P1) -- Pass the cache context from `SearchRuntimeContext` through classification functions. Remove the global singleton fallback pattern.

6. **Define `LanguageEnrichmentProvider` protocol** (P6) -- Only if/when a third language enrichment provider is needed. Design the seam now but defer the abstraction per YAGNI.
