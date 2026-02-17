# Design Review: Search Pipeline + Enrichment + Language Analysis

**Date:** 2026-02-17
**Scope:** `tools/cq/search/pipeline/`, `tools/cq/search/enrichment/`, `tools/cq/search/python/`, `tools/cq/search/rust/`, `tools/cq/search/semantic/`
**Focus:** All principles (1-24)
**Depth:** moderate
**Files reviewed:** 30 of 61 (representative: contracts, entry points, adapters, orchestration, extractors, incremental planes, semantic front door)

## Executive Summary

This layer is the intelligence core of CQ search: it orchestrates classification, enrichment, and assembly across Python and Rust language partitions. The architecture demonstrates strong contract design (versioned `msgspec` structs), a clear phase pipeline (candidate -> classify -> enrich -> assemble), and a well-executed adapter pattern for language-specific enrichment. The primary design risks center on (1) duplicated knowledge between Python and Rust paths -- particularly around diagnostics extraction, telemetry accumulation, and failure-reason normalization -- (2) heavy reliance on untyped `dict[str, object]` payloads deep inside the enrichment core, undermining the typed contracts at the boundaries, and (3) the `assembly.py` module carrying too many responsibilities (object resolution, neighborhood preview, insight construction, cache maintenance). The most impactful improvements would be consolidating the duplicated diagnostic/telemetry logic into shared abstractions and replacing internal `dict[str, object]` payloads with the existing typed fact structs.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `_SearchAssemblyInputs` and `_SearchSemanticOutcome` are semi-public via TYPE_CHECKING imports |
| 2 | Separation of concerns | 1 | large | medium | `assembly.py` mixes object resolution, neighborhood preview, insight card, and cache maintenance |
| 3 | SRP (one reason to change) | 1 | medium | medium | `smart_search.py` is both entry point and utility library (scoring, coercion, building) |
| 4 | High cohesion, low coupling | 2 | medium | low | Enrichment adapters are cohesive; pipeline-to-enrichment coupling is narrow |
| 5 | Dependency direction | 2 | small | low | Core pipeline types in `smart_search_types.py` depend on enrichment contracts -- acceptable |
| 6 | Ports & Adapters | 2 | medium | low | `LanguageEnrichmentPort` protocol is well-designed; semantic front door lacks equivalent |
| 7 | DRY (knowledge) | 1 | medium | high | Diagnostics extraction, telemetry accumulation, and `_accumulate_runtime_flags` duplicated |
| 8 | Design by contract | 2 | small | low | Versioned `V1` structs with `frozen=True`; enrichment mode enum has semantic properties |
| 9 | Parse, don't validate | 1 | medium | medium | `dict[str, object]` payloads validated ad-hoc deep inside enrichment core |
| 10 | Make illegal states unrepresentable | 2 | small | low | `IncrementalEnrichmentModeV1` enum constrains mode combinations well |
| 11 | CQS | 2 | small | low | `_prepare_search_assembly_inputs` both computes and mutates sections via `insert_*` |
| 12 | DI + explicit composition | 2 | small | low | `language_registry.py` provides explicit adapter registration |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; all composition via protocol + adapter |
| 14 | Law of Demeter | 2 | small | low | `assembly.py` reaches into `object_runtime.view.summaries[0].object_ref.object_id` |
| 15 | Tell, don't ask | 2 | medium | low | `_build_enriched_match` asks enrichment for raw fields then reconstructs |
| 16 | Functional core, imperative shell | 2 | medium | low | Classification and enrichment are mostly pure; cache orchestration at edges |
| 17 | Idempotency | 3 | - | - | Content-hashed cache keys ensure re-run produces same results |
| 18 | Determinism / reproducibility | 2 | small | low | `_build_agreement_section` uses `sorted()` for deterministic output |
| 19 | KISS | 2 | small | low | Enrichment pipeline is conceptually clear; some over-engineering in coercion helpers |
| 20 | YAGNI | 2 | small | low | `_apply_prefetched_search_semantic_outcome` is a dead stub (always returns False) |
| 21 | Least astonishment | 2 | small | low | `enrichment_phase.py` is a 1-function cast wrapper -- surprising indirection |
| 22 | Public contracts | 2 | small | low | `__all__` exports are comprehensive; `_v1` suffixes signal versioning |
| 23 | Design for testability | 2 | medium | low | Pure classification functions testable; cache-heavy partition pipeline needs DI |
| 24 | Observability | 2 | small | low | Stage timings, stage status, degrade reasons -- solid but inconsistent key names |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Modules generally hide internals behind `__all__` exports and use underscore-prefixed functions. However, several "private" types leak across module boundaries.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_types.py:270-298` -- `_SearchSemanticOutcome` and `_SearchAssemblyInputs` are prefixed with `_` but imported by `assembly.py` and `search_semantic.py`, making them de facto public contracts without the explicit commitment.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/assembly.py:24` -- imports `_NeighborhoodPreviewInputs` and `_SearchAssemblyInputs` directly.

**Suggested improvement:**
Drop the underscore prefix from `SearchAssemblyInputs`, `SearchSemanticOutcome`, and `NeighborhoodPreviewInputs` to acknowledge their cross-module role. Add them to `__all__` in `smart_search_types.py`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`assembly.py` (686 lines) handles at least five distinct concerns: (1) object resolution and candidate collection, (2) neighborhood preview construction, (3) insight card assembly, (4) cache maintenance, and (5) final CqResult packaging.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/assembly.py:523-606` -- `_prepare_search_assembly_inputs` performs object resolution, section building, candidate collection, neighborhood preview, and target matching in a single 83-line function.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/assembly.py:649-657` -- Cache backend metrics snapshot and maintenance tick are embedded in result assembly, mixing IO concern with pure assembly logic.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/assembly.py:279-370` -- `_build_structural_neighborhood_preview` imports from 4 different packages and does tree-sitter collection, slice processing, finding construction, and degradation tracking.

**Suggested improvement:**
Extract three focused modules: (1) `target_resolution.py` for `_collect_definition_candidates`, `_resolve_primary_target_match`, and `_resolve_primary_target_finding`; (2) move `_build_structural_neighborhood_preview` and `_build_tree_sitter_neighborhood_preview` into `neighborhood_preview.py`; (3) extract cache maintenance into a post-assembly hook. Keep `assembly.py` as the thin orchestrator that sequences these steps.

**Effort:** large
**Risk if unaddressed:** medium -- this module is the most likely place for merge conflicts and unintended coupling.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`smart_search.py` (745 lines) serves as both the public entry point and a utility library containing scoring functions, mode detection, coercion helpers, and partition orchestration.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:234-267` -- `compute_relevance_score` is a pure scoring function embedded in the pipeline entry module. It changes for scoring-policy reasons, not pipeline-flow reasons.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:400-476` -- 10 `_coerce_*` helper functions exist solely to convert kwargs into typed fields. This is request-parsing logic, not pipeline logic.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:292-323` -- `_category_message` and `_evidence_to_bucket` are rendering helpers also present in `smart_search_sections.py:30-55`, creating duplication.

**Suggested improvement:**
Move scoring functions (`compute_relevance_score`, `KIND_WEIGHTS`) to `scoring.py` or `relevance.py`. Move the `_coerce_*` helpers into `contracts.py` as a `SearchRequest.from_kwargs()` classmethod or standalone `parse_search_request()`. Remove the duplicate `_category_message` and `_evidence_to_bucket` from `smart_search.py` (they already exist in `smart_search_sections.py`).

**Effort:** medium
**Risk if unaddressed:** medium -- scattered responsibilities make it harder to understand change impact.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The enrichment adapter pattern creates good module boundaries. The `enrichment/` package is well-isolated behind the `LanguageEnrichmentPort` protocol. However, `classification.py` pulls from many packages.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classification.py:1-61` -- 17 import statements spanning 8 different sub-packages. This module is a coupling nexus between classifier, enrichment, tree-sitter, python analysis, and rust enrichment.

**Suggested improvement:**
The current coupling is mostly functional (classification genuinely needs all these capabilities). To reduce import surface, consider creating a `ClassificationDependencies` protocol or dataclass that bundles the required capabilities, passed in rather than imported directly.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Core pipeline types (`RawMatch`, `EnrichedMatch`, `SearchStats`) in `smart_search_types.py` depend on enrichment contracts (`IncrementalEnrichmentV1`, `PythonEnrichmentV1`, `RustTreeSitterEnrichmentV1`). This is acceptable since enrichment payloads are part of the match data model.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_types.py:17-22` -- `EnrichedMatch` directly embeds three enrichment contract types as optional fields. This creates a dependency from core types toward enrichment contracts, but enrichment contracts are stable `V1` versioned structs, so the risk is low.

**Suggested improvement:**
No action needed -- the dependency is acceptable because enrichment payloads are genuinely part of the match representation.

**Effort:** -
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`LanguageEnrichmentPort` in `enrichment/contracts.py` is a well-designed port with two concrete adapters. The semantic front door (`semantic/front_door.py`) uses a language dispatch dict (`_execute_provider`) but lacks an equivalent protocol.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/enrichment/contracts.py:42-58` -- `LanguageEnrichmentPort` is a clean `Protocol` with three methods. Adapters for Python and Rust implement it correctly.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/semantic/front_door.py:214-218` -- `_execute_provider` uses a language-keyed dict dispatch (`{"python": _execute_python_provider, "rust": _execute_rust_provider}`) without a formal protocol, making it harder to test or extend.

**Suggested improvement:**
Define a `SemanticProviderPort` protocol with a `execute(request, context) -> outcome` method, mirroring the enrichment adapter pattern. Register providers in the semantic models module rather than using inline dict dispatch.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
This is the most significant design principle violation in the scope. Several pieces of knowledge are duplicated between Python and Rust paths, and between `enrichment/core.py` and `semantic/models.py`.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/enrichment/python_adapter.py:89-101` vs `/Users/paulheyse/CodeAnatomy/tools/cq/search/enrichment/rust_adapter.py:100-109` -- `_accumulate_runtime_flags` is duplicated verbatim between the two adapters. Both check `did_exceed_match_limit` and `cancelled` with identical logic.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/semantic/models.py:130-158` vs `/Users/paulheyse/CodeAnatomy/tools/cq/search/enrichment/python_adapter.py:128-140` -- `_python_diagnostics` in `models.py` and `_tree_sitter_diagnostics` in `python_adapter.py` contain nearly identical tree-sitter diagnostic row construction (checking `cst_diagnostics`, extracting `kind`/`message`/`line`/`col`).
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/semantic/models.py:161-180` vs `/Users/paulheyse/CodeAnatomy/tools/cq/search/enrichment/rust_adapter.py:62-93` -- `_rust_diagnostics` in `models.py` and `build_diagnostics` in `rust_adapter.py` both extract `degrade_events` and `cst_diagnostics` with identical logic.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:270-289` vs `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_sections.py:30-35` -- `_evidence_to_bucket` is defined in both files with slightly different bucket names ("medium" vs "med"), creating a semantic mismatch.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:292-323` vs `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_sections.py:38-55` -- `_category_message` is fully duplicated.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/enrichment/core.py:182-251` -- `_PY_RESOLUTION_KEYS`, `_PY_BEHAVIOR_KEYS`, `_PY_STRUCTURAL_KEYS` encode the same knowledge as the typed fact structs in `enrichment/python_facts.py`. These frozensets should be derived from the struct field names.

**Suggested improvement:**
1. Extract `_accumulate_runtime_flags` into `enrichment/core.py` as a shared helper.
2. Create a `build_tree_sitter_diagnostic_rows(cst_diagnostics: object) -> list[dict[str, object]]` in `enrichment/core.py` and use it from both adapters and `semantic/models.py`.
3. Delete `_category_message` and `_evidence_to_bucket` from `smart_search.py` (they are already present in `smart_search_sections.py`).
4. Derive `_PY_RESOLUTION_KEYS` etc. from `PythonResolutionFacts.__struct_fields__` instead of maintaining parallel frozensets.

**Effort:** medium
**Risk if unaddressed:** high -- these duplications will drift, causing inconsistent diagnostics and telemetry.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Versioned `msgspec.Struct` types (`SearchPartitionPlanV1`, `IncrementalEnrichmentV1`, `RustTreeSitterEnrichmentV1`) provide strong boundary contracts with `frozen=True`, `forbid_unknown_fields=True`, and `omit_defaults=True`. The `IncrementalEnrichmentModeV1` enum has semantic property methods (`includes_symtable`, `includes_dis`, `includes_inspect`).

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/enrichment_contracts.py:11-40` -- `IncrementalEnrichmentModeV1` is an exemplary contract: each mode documents exactly which enrichment planes it enables via `@property` methods.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/enrichment_contracts.py:65-99` -- `RustTreeSitterEnrichmentV1`, `PythonEnrichmentV1`, and `IncrementalEnrichmentV1` all use `dict[str, object]` as their `payload` field, which weakens the contract. The payload contents are untyped despite having well-defined schemas in `python_facts.py` and `rust_facts.py`.

**Suggested improvement:**
Replace `payload: dict[str, object]` in `PythonEnrichmentV1` with `payload: PythonEnrichmentFacts | dict[str, object]` (or better, just `PythonEnrichmentFacts`). Similarly for Rust. The fact structs already exist and define the valid shapes.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 1/3

**Current state:**
The boundary parsing is strong (ripgrep JSON -> `RawMatch`, `msgspec.convert` at boundaries). However, deep inside the enrichment pipeline, payloads remain as `dict[str, object]` and are validated ad-hoc with `isinstance` checks throughout.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/enrichment/core.py:254-287` -- `_partition_python_payload_fields` iterates over flat dict keys and classifies them by checking membership in frozensets. This is essentially "validate on every use" rather than "parse once into typed structure."
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/python/extractors.py:706-810` -- `_enrich_ast_grep_core` builds a payload by mutating a `dict[str, object]` accumulator, then downstream code extracts fields from it with `isinstance` guards. The knowledge of valid keys is spread across the mutation site and all reading sites.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/enrichment/python_adapter.py:48-67` -- `accumulate_telemetry` reaches into nested dicts with `isinstance` checks: `meta = payload.get("meta"); if isinstance(meta, dict): ...`

**Suggested improvement:**
In `enrich_python_context`, build a `PythonEnrichmentFacts` struct directly from the extraction stages instead of accumulating into a flat dict. The `_PythonEnrichmentState` already has the right shape -- return a `PythonEnrichmentFacts` from it and convert to dict only at the serialization boundary. This would eliminate hundreds of `isinstance` guards downstream.

**Effort:** medium
**Risk if unaddressed:** medium -- ad-hoc validation leads to silent data loss when payload shapes evolve.

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`IncrementalEnrichmentModeV1` is an excellent example of constraining states via enum. `MatchCategory` as a `Literal` type prevents invalid categories. However, `EnrichedMatch` allows all enrichment fields to be `None` simultaneously, which could represent "unenriched" matches that shouldn't exist at this pipeline stage.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_types.py:176-222` -- `EnrichedMatch` has `category`, `confidence`, and `evidence_kind` as required fields, correctly preventing un-classified matches. However, all enrichment-specific fields (`rust_tree_sitter`, `python_enrichment`, `incremental_enrichment`) are optional, which is correct since a match may be for a language that doesn't have a particular enrichment type.

**Suggested improvement:**
No action needed -- the current optionality is correct for the cross-language data model.

**Effort:** -
**Risk if unaddressed:** low

---

#### P11. CQS (Command-Query Separation) -- Alignment: 2/3

**Current state:**
Most functions are queries (return data) or commands (mutate state) but not both. A few assembly functions blur this.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/assembly.py:373-429` -- `_build_tree_sitter_neighborhood_preview` both returns a neighborhood/notes tuple and mutates `summary.tree_sitter_neighborhood` and `sections` list via `insert_neighborhood_preview`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/enrichment/core.py:85-93` -- `append_source` mutates `payload` in place. Used pervasively. While technically a command, the mutation of shared dict state makes reasoning harder.

**Suggested improvement:**
Have `_build_tree_sitter_neighborhood_preview` return all its outputs as a data structure. Let the caller (`_prepare_search_assembly_inputs`) handle mutations to `summary` and `sections`. This keeps the function pure.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
`language_registry.py` provides explicit adapter registration with lazy default initialization. The `SearchPipeline` class accepts callables for partition running and assembly, enabling DI for the pipeline phases.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/enrichment/language_registry.py:16-23` -- `_ensure_defaults()` lazily registers adapters. This is a reasonable approach but means the first call to `get_language_adapter` has a hidden import side effect.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/orchestration.py:68-108` -- `SearchPipeline` accepts `partition_runner` and `assembler` as callables. This is good DI.

**Suggested improvement:**
No critical changes needed. For improved testability, consider making `_ensure_defaults` explicit by calling it during module initialization rather than on first access.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
Excellent. No inheritance hierarchies in scope. All behavior composition uses protocols, adapters, and function composition.

**Suggested improvement:**
No action needed.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most access chains are short. A few spots reach deep.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/assembly.py:170-176` -- `object_runtime.view.summaries[0].object_ref.object_id` is a 5-level chain. Similarly `object_runtime.representative_matches.get(object_ref.object_id)`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/assembly.py:345-349` -- `slice_item.preview[:5]` accessed within a list comprehension that also accesses `node.display_label or node.name`.

**Suggested improvement:**
Add a `get_primary_candidate(self) -> tuple[SearchObjectSummaryV1, EnrichedMatch | None] | None` method to `ObjectResolutionRuntime`. This would encapsulate the pattern of "get the first/best summary with its representative match."

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The enrichment adapters follow "tell" pattern -- callers pass match objects and get structured payloads back. However, some post-enrichment logic in `classification.py` asks for raw fields and reconstructs.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classification.py:370-377` -- After enrichment, `_build_enriched_match` asks `rust_enrichment_payload(enrichment.rust_tree_sitter)` for `impl_type` and `scope_name`, then reconstructs `containing_scope`. The Rust enrichment adapter should provide `containing_scope` directly.

**Suggested improvement:**
Add `containing_scope` as a top-level optional field to `RustTreeSitterEnrichmentV1`, set during enrichment. Then `_build_enriched_match` can read it directly instead of reverse-engineering it from payload internals.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Classification functions (`classify_heuristic`, `classify_from_node`, `classify_from_records`) are pure. Enrichment functions are mostly pure (take source bytes, return dicts). Cache orchestration and process pool management form the imperative shell in `partition_pipeline.py` and `classify_phase.py`.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/partition_pipeline.py:97-131` -- `run_search_partition` cleanly separates scope context setup, candidate phase, and enrichment phase. Cache I/O is in the shell.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/assembly.py:649-657` -- Cache maintenance (`maintenance_tick`, `snapshot_backend_metrics`) is mixed into the assembly function. This should be in the shell.

**Suggested improvement:**
Extract cache maintenance from `_assemble_smart_search_result` into a post-assembly step in `smart_search.py:smart_search()`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Content-hash-keyed caching in `partition_pipeline.py` ensures that identical inputs produce identical cache keys and thus identical results. The `file_content_hash` function ensures cache invalidation on file changes.

**Suggested improvement:**
No action needed.

---

#### P18. Determinism / reproducibility -- Alignment: 2/3

**Current state:**
Most outputs are deterministic. `_build_agreement_section` uses `sorted()` for key ordering. However, dict iteration order in telemetry accumulation depends on insertion order, which depends on match processing order.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_telemetry.py` (referenced from summary) -- Telemetry accumulation iterates over enriched matches, which are ordered by partition processing. This is deterministic for single-threaded execution but could vary with parallel worker scheduling.

**Suggested improvement:**
Add a `sorted()` call to telemetry output keys or document that telemetry order depends on match processing order.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The pipeline concept (candidate -> classify -> enrich -> assemble) is clear and simple. However, some implementation complexity is unnecessary.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/enrichment_phase.py:1-29` -- This entire module is a single function that casts `run_search_partition`'s return type. The cast exists because `run_search_partition` declares `-> object` at `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/partition_pipeline.py:102`. Fixing the return type annotation would eliminate this module.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:326-345` -- Three thin wrappers (`build_finding`, `build_followups`, `build_summary`) import and delegate with identical signatures, adding an indirection layer without value.

**Suggested improvement:**
1. Change `run_search_partition` return type from `object` to `LanguageSearchResult` and delete `enrichment_phase.py`.
2. Remove the thin delegating wrappers in `smart_search.py` and have callers import from the source modules directly.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most abstractions have clear current use. One dead code path exists.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/search_semantic.py:191-213` -- `_apply_prefetched_search_semantic_outcome` has a comment "Hard-cutover: search matches no longer carry prefetched python semantic payloads" and unconditionally returns `False`. This dead code path should be removed.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/__init__.py:7-8` -- `SearchContext: Any = None` and `SearchPipeline: Any = None` are lazy-loading stubs typed as `Any`, which defeats type checking for any consumer using the package-level imports.

**Suggested improvement:**
Delete `_apply_prefetched_search_semantic_outcome` and its call site. Replace `Any` annotations in `__init__.py` with proper TYPE_CHECKING conditional types.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
APIs are generally predictable. A few naming inconsistencies exist.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/enrichment_phase.py` -- Named `enrichment_phase.py` but calls `run_search_partition`, which includes candidate collection AND enrichment. The name suggests it only does enrichment.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:270-289` vs `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_sections.py:30-35` -- `_evidence_to_bucket` returns "medium" in one file and "med" in the other for the same logical concept.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/contracts.py:61-78` -- `SearchRequest` and `SearchConfig` are very similar shapes but serve different lifecycle stages (input vs resolved). The distinction is not immediately obvious from names.

**Suggested improvement:**
1. Rename `enrichment_phase.py` to `partition_phase.py` or inline it.
2. Standardize `_evidence_to_bucket` to a single authoritative definition.
3. Add a module docstring to `contracts.py` explaining the `SearchRequest` (user input) -> `SearchConfig` (resolved pipeline state) lifecycle.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
`__all__` exports are present and comprehensive in most modules. Versioned `V1` suffixes on contracts signal stability intent.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py:727-744` -- `__all__` includes `assemble_smart_search_result` which is actually defined in `assembly.py`, creating a re-export that could confuse tooling.

**Suggested improvement:**
Remove re-exports from `__all__` when the canonical location is a different module, or document them as re-exports.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Pure classification functions (`classify_heuristic`, `classify_from_node`) are highly testable. The enrichment pipeline's dependence on file I/O, cache backends, and process pools makes integration testing heavier.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/partition_pipeline.py:133-184` -- `_scope_context` creates a `CqCacheBackend` and `CqCachePolicyV1` via module-level functions (`get_cq_cache_backend`, `default_cache_policy`). These are not injectable, requiring monkeypatching to test.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classify_phase.py:95-141` -- Process pool dispatch (`get_worker_scheduler().submit_cpu(...)`) is tightly coupled. The fail-open fallback to sequential execution is good, but testing the parallel path requires real process pool setup.

**Suggested improvement:**
Accept `cache_backend` and `cache_policy` as optional parameters to `run_search_partition` (defaulting to the current module-level functions). This would allow unit tests to inject a no-op cache backend.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Stage timings (`stage_timings_ms`), stage status (`stage_status`), and degrade reasons are tracked consistently through the Python enrichment pipeline. The `agreement` section tracks cross-source conflicts.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/python/extractors.py:1077-1085` -- `_run_ast_grep_stage` records timing as `stage_timings_ms["ast_grep"]` while the Rust enrichment uses `stage_timings_ms` as a flat dict with different key conventions. The key naming is not standardized.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/rust/enrichment.py` -- No per-stage timing instrumentation exists for Rust enrichment. Only the Python path records `stage_timings_ms`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/partition_pipeline.py` -- Cache hit/miss telemetry uses `record_cache_get` and `record_cache_set` consistently.

**Suggested improvement:**
Add `stage_timings_ms` recording to Rust enrichment (`enrich_rust_context_by_byte_range`), measuring `ast_grep` and `tree_sitter` phases separately. Standardize timing key names across Python and Rust paths.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Python/Rust Enrichment Path Duplication

**Description:** The Python and Rust enrichment paths duplicate knowledge in three areas: (1) runtime flag accumulation, (2) diagnostics row construction, and (3) failure reason normalization. Both adapters independently implement the same telemetry patterns.

**Root cause:** The two adapters were likely developed in sequence, with the Rust adapter copying patterns from the Python adapter without extracting shared abstractions.

**Affected principles:** P7 (DRY), P24 (Observability consistency)

**Suggested approach:** Extract shared telemetry accumulation helpers into `enrichment/core.py`. The adapter protocol already supports language-specific behavior -- the shared parts should live in the core.

### Theme 2: Typed Contracts at Boundaries, Untyped Dicts Internally

**Description:** The boundary contracts (`PythonEnrichmentV1`, `RustTreeSitterEnrichmentV1`, `IncrementalEnrichmentV1`) are well-typed `msgspec.Struct` types. However, internally, enrichment payloads flow as `dict[str, object]`, and the typed fact structs (`PythonEnrichmentFacts`, `RustEnrichmentFacts`) are only used for optional parsing in `enrichment/core.py:parse_python_enrichment`.

**Root cause:** The enrichment pipeline was built bottom-up (accumulate dicts, wrap in typed struct at boundary). The fact structs were added later as a parsing target but not integrated into the accumulation path.

**Affected principles:** P8 (Design by contract), P9 (Parse don't validate), P10 (Illegal states)

**Suggested approach:** Have the Python enrichment pipeline build `PythonEnrichmentFacts` directly. Convert to `dict[str, object]` only at the serialization boundary. This would make the internal flow typed end-to-end.

### Theme 3: Assembly Module Concentration

**Description:** `assembly.py` has accumulated responsibilities for object resolution, neighborhood preview, insight construction, cache maintenance, and result packaging. This creates a bottleneck for changes.

**Root cause:** Assembly is the natural convergence point where all enrichment outputs meet. Without deliberate extraction, responsibilities accumulate here.

**Affected principles:** P2 (Separation of concerns), P3 (SRP), P16 (Functional core)

**Suggested approach:** Treat assembly as a thin orchestrator. Extract target resolution, neighborhood preview, and cache maintenance into separate focused modules.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Delete duplicate `_category_message` and `_evidence_to_bucket` from `smart_search.py` | small | Eliminates semantic mismatch ("medium" vs "med") |
| 2 | P20 (YAGNI) | Delete dead `_apply_prefetched_search_semantic_outcome` stub | small | Removes dead code and simplifies semantic flow |
| 3 | P19 (KISS) | Fix `run_search_partition` return type to `LanguageSearchResult`, delete `enrichment_phase.py` | small | Eliminates unnecessary indirection module |
| 4 | P7 (DRY) | Extract `_accumulate_runtime_flags` to `enrichment/core.py` | small | Single source of truth for telemetry accumulation |
| 5 | P24 (Observability) | Add stage timing to Rust enrichment path | small | Parity with Python observability |

## Recommended Action Sequence

1. **Delete duplicates (P7, P21):** Remove `_category_message`, `_evidence_to_bucket` from `smart_search.py`. Remove dead `_apply_prefetched_search_semantic_outcome`. Standardize "medium"/"med" bucket naming to a single value. These are safe, zero-risk changes.

2. **Eliminate unnecessary indirection (P19):** Fix the return type annotation on `run_search_partition` from `object` to `LanguageSearchResult`. Delete `enrichment_phase.py`. Remove the three thin delegating wrappers in `smart_search.py`.

3. **Consolidate shared enrichment helpers (P7):** Move `_accumulate_runtime_flags` and tree-sitter diagnostic row construction into `enrichment/core.py`. Update both adapters and `semantic/models.py` to use the shared helpers.

4. **Derive payload field sets from structs (P7, P9):** Replace `_PY_RESOLUTION_KEYS`, `_PY_BEHAVIOR_KEYS`, `_PY_STRUCTURAL_KEYS` in `enrichment/core.py` with values derived from `PythonResolutionFacts.__struct_fields__` etc.

5. **Add Rust enrichment stage timings (P24):** Instrument `enrich_rust_context_by_byte_range` with `ast_grep` and `tree_sitter` stage timings matching the Python pattern.

6. **Extract assembly sub-responsibilities (P2, P3):** Create `target_resolution.py` and `neighborhood_preview.py` from `assembly.py`. Extract cache maintenance into a post-assembly hook. This is the largest change and should come after the simpler consolidations.

7. **Move scoring and coercion out of smart_search.py (P3):** Move `compute_relevance_score` and `KIND_WEIGHTS` to a scoring module. Move `_coerce_*` helpers into `contracts.py` or a `request_parsing.py` module.

8. **Introduce typed internal enrichment flow (P9):** Refactor `enrich_python_context` to build `PythonEnrichmentFacts` directly from enrichment stages, converting to dict only at serialization. This is the deepest change and depends on steps 3-4 being complete first.
