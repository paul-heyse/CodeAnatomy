# Design Review: tools/cq/search/pipeline

**Date:** 2026-02-15
**Scope:** `tools/cq/search/pipeline/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 10 (6,126 LOC total)

| File | LOC | Role |
|------|-----|------|
| `smart_search.py` | 3,914 | Main entry point, classification, enrichment, assembly |
| `classifier.py` | 659 | Three-tier classification pipeline |
| `partition_pipeline.py` | 550 | Per-language partition with cache orchestration |
| `classifier_runtime.py` | 450 | Runtime caches and node resolution |
| `context_window.py` | 118 | Context snippet extraction |
| `orchestration.py` | 114 | Pipeline facade and section builders |
| `candidate_normalizer.py` | 101 | Definition candidate selection |
| `contracts.py` | 99 | SearchConfig, SearchRequest, SmartSearchContext |
| `profiles.py` | 80 | SearchLimits presets |
| `__init__.py` | 41 | Lazy-loading package facade |

## Executive Summary

The pipeline subsystem is functionally rich and well-instrumented, with strong contracts at the boundary layer (`contracts.py`, `profiles.py`) and a well-separated classification tier (`classifier.py`, `classifier_runtime.py`). However, `smart_search.py` at 3,914 lines is the single largest file in the codebase and concentrates at least six distinct responsibilities: request coercion, candidate collection, classification orchestration, enrichment dispatch, telemetry accumulation, and result assembly. This violates SRP, information hiding, and testability principles simultaneously. The module-level mutable caches in `classifier_runtime.py` and `smart_search.py` create hidden coupling that undermines the parallel execution model. Quick wins include extracting enrichment telemetry accumulation (~300 LOC) and Python semantic enrichment management (~600 LOC) from `smart_search.py`, and encapsulating the six bare `dict` caches in `classifier_runtime.py` behind a cache manager.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | high | 6 module-global caches + `_SEARCH_OBJECT_VIEW_REGISTRY` exposed as mutable module state |
| 2 | Separation of concerns | 1 | large | high | `smart_search.py` mixes orchestration, enrichment, telemetry, assembly, and semantic integration |
| 3 | SRP | 0 | large | high | `smart_search.py` changes for 6+ independent reasons |
| 4 | High cohesion, low coupling | 1 | medium | medium | 30+ external imports in `smart_search.py`; `partition_pipeline.py` imports `smart_search` as `Any` module |
| 5 | Dependency direction | 2 | small | low | Core contracts in `contracts.py` have clean inward deps; `smart_search.py` is the outward edge |
| 6 | Ports & Adapters | 2 | medium | low | Ripgrep integration is well-isolated in `rg/`; semantic providers use request/outcome pattern |
| 7 | DRY (knowledge) | 1 | medium | medium | Telemetry counter initialization duplicated in 4+ places; Python semantic degradation reason normalization duplicated |
| 8 | Design by contract | 2 | small | low | `SearchLimits` uses `PositiveInt`/`PositiveFloat` constraints; `CqStruct` frozen types |
| 9 | Parse, don't validate | 2 | small | low | `_coerce_search_request` parses kwargs once at boundary |
| 10 | Illegal states | 2 | small | low | `QueryMode` enum prevents invalid modes; `MatchCategory` is a `Literal` type |
| 11 | CQS | 1 | medium | medium | `_build_search_summary` both builds summary dict and mutates it with side-effect keys; `_populate_optional_fields` mutates `data` dict in place |
| 12 | DI + explicit composition | 1 | medium | medium | `partition_pipeline.py` uses `_smart_search_module()` dynamic import returning `Any`; no DI for caches |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; all behavior via composition |
| 14 | Law of Demeter | 1 | small | medium | Chain `runtime.view.summaries[0].object_ref.object_id` at line 3082; `match.python_semantic_enrichment.get(...)` chains |
| 15 | Tell, don't ask | 1 | medium | medium | Extensive `isinstance` interrogation of `dict` payloads (`_accumulate_python_enrichment`, `_status_from_enrichment`) |
| 16 | Functional core / imperative shell | 1 | large | medium | Pure classification logic mixed with IO (file reads, subprocess) and cache mutation in same call paths |
| 17 | Idempotency | 2 | small | low | Search is read-only; cache writes are content-addressed; `clear_caches()` ensures clean runs |
| 18 | Determinism | 2 | small | low | Parallel classification results sorted by index; ProcessPool uses `spawn` context |
| 19 | KISS | 1 | large | high | 3,914-line file with 100+ private functions; `_assemble_smart_search_result` orchestrates 10+ sub-steps |
| 20 | YAGNI | 2 | small | low | `SearchPipeline` facade is minimal; no unused extension points observed |
| 21 | Least astonishment | 1 | small | medium | `build_summary(*args)` accepts both `SearchSummaryInputs` and 5 positional args; `_build_search_context` clears all caches as side effect |
| 22 | Public contracts | 2 | small | low | `__all__` declared in every file; `CqStruct` frozen types for serialization |
| 23 | Testability | 1 | medium | high | Module-global caches require `clear_caches()` between tests; parallel paths hard to test in isolation |
| 24 | Observability | 2 | small | low | Enrichment telemetry, stage timings, cache metrics all collected; structured `diagnostics` list |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
`classifier_runtime.py` exposes six module-level mutable `dict` caches (`_sg_cache`, `_source_cache`, `_def_lines_cache`, `_symtable_cache`, `_record_context_cache`, `_node_index_cache`) at `classifier_runtime.py:65-70`. These are directly accessed by cache key from any caller. The `__all__` export list at `classifier_runtime.py:438` includes private-prefixed names (`_find_containing_scope`, `_find_node_at_position`, `_is_docstring_context`), leaking implementation details.

**Findings:**
- `classifier_runtime.py:65-70`: Six bare `dict` caches with no size limit, no eviction policy, and no encapsulation. Any module can populate or corrupt these caches.
- `smart_search.py:204`: `_SEARCH_OBJECT_VIEW_REGISTRY: dict[str, SearchObjectResolvedViewV1] = {}` is a module-global mutable registry used for cross-run state sharing with no access control.
- `classifier_runtime.py:438-449`: `__all__` exports three `_`-prefixed functions, breaking the convention that underscore means private.
- `classifier_runtime.py:174-177`: `_resolve_sg_root_path` iterates the entire `_sg_cache` dict by identity comparison -- a linear scan that exposes cache internals.

**Suggested improvement:**
Encapsulate the six caches in `classifier_runtime.py` behind a `ClassifierCacheManager` class that owns size limits, eviction, and clear semantics. Expose only `get_*` accessor methods and `clear()`. Move `_SEARCH_OBJECT_VIEW_REGISTRY` into a dedicated `SearchObjectViewStore` with explicit `register`/`pop` methods. Remove `_`-prefixed names from `__all__`.

**Effort:** medium
**Risk if unaddressed:** high -- Module-global mutable state is the primary barrier to safe parallel execution and test isolation.

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`smart_search.py` intermingles at least six distinct concerns:
1. Request coercion and context construction (lines 1742-1801)
2. Candidate collection via ripgrep subprocess (lines 609-661)
3. Classification orchestration including parallel ProcessPool (lines 1855-1939)
4. Language-specific enrichment dispatch (lines 1011-1130)
5. Python semantic enrichment management with prefetch/merge/telemetry (lines 2359-2883)
6. Result assembly with insight construction, neighborhood preview, and section building (lines 3656-3809)

These concerns are not separated by module boundaries -- they all live in a single 3,914-line file.

**Findings:**
- `smart_search.py:2149-2337`: `_empty_enrichment_telemetry()`, `_accumulate_stage_status()`, `_accumulate_stage_timings()`, `_accumulate_python_enrichment()`, `_accumulate_rust_enrichment()`, `_accumulate_rust_runtime()`, `_accumulate_rust_bundle()`, `_attach_enrichment_cache_stats()`, `_build_enrichment_telemetry()` -- ~190 lines of telemetry accumulation logic that is pure data transformation with no dependency on search orchestration.
- `smart_search.py:2359-2883`: ~525 lines dedicated to Python semantic enrichment prefetching, merging, signal evaluation, and diagnostics -- a self-contained domain that deserves its own module.
- `smart_search.py:3091-3165`: ~75 lines for structural neighborhood preview construction, importing from `tools.cq.neighborhood`.
- `smart_search.py:3202-3514`: ~312 lines for semantic insight application, outcome collection, and telemetry update -- distinct from the base search logic.

**Suggested improvement:**
Extract into at least three new modules:
1. `enrichment_telemetry.py` (~200 LOC): All `_accumulate_*`, `_empty_enrichment_telemetry`, `_build_enrichment_telemetry`, `_status_from_enrichment`.
2. `python_semantic.py` (~550 LOC): All `_prefetch_python_semantic_*`, `_merge_match_with_python_semantic_*`, `_attach_python_semantic_enrichment`, `_build_python_semantic_overview`, `_seed_python_semantic_state`, `_python_semantic_*_diagnostic` functions.
3. `assembly.py` (~400 LOC): `_prepare_search_assembly_inputs`, `_assemble_smart_search_result`, `_assemble_search_insight`, `_build_search_result_key_findings`, `_collect_definition_candidates`, `_build_tree_sitter_neighborhood_preview`.

**Effort:** large
**Risk if unaddressed:** high -- A 3,914-line file is the most significant structural risk in the codebase; it will continue to accumulate responsibilities and become harder to modify safely.

---

#### P3. SRP (one reason to change) -- Alignment: 0/3

**Current state:**
`smart_search.py` changes for at least six independent reasons:
1. Ripgrep command construction or candidate collection changes
2. Classification algorithm changes (tiers, confidence weights)
3. Python enrichment pipeline changes (stages, payloads)
4. Rust enrichment pipeline changes
5. Result assembly or section structure changes
6. Python semantic enrichment protocol changes
7. Telemetry/observability format changes
8. Neighborhood preview integration changes

This is the most severe SRP violation in the reviewed scope.

**Findings:**
- `smart_search.py` has 100+ `def` statements -- more than any file should have under SRP.
- The file has 30+ direct external imports (lines 1-153), indicating it is a coupling hub.
- Any change to enrichment telemetry format (e.g., adding a new stage) requires editing this file, even though the telemetry logic is conceptually independent of search orchestration.
- Any change to Python semantic enrichment protocol requires editing this file, even though the semantic integration has its own `models.py` and `front_door.py`.

**Suggested improvement:**
Same as P2 decomposition. The target state is `smart_search.py` containing only the top-level `smart_search()` function, pipeline context construction, and the orchestration skeleton that calls into extracted modules.

**Effort:** large
**Risk if unaddressed:** high -- The file is a merge-conflict magnet and a cognitive overload barrier for contributors.

---

#### P4. High cohesion, low coupling -- Alignment: 1/3

**Current state:**
`smart_search.py` has high fan-in (29+ importers) and high fan-out (30+ imports). Types defined here (`RawMatch`, `EnrichedMatch`, `SearchStats`) are imported throughout the search subsystem, creating a bidirectional dependency between the orchestrator and its consumers.

**Findings:**
- `smart_search.py:1-153`: 30+ import statements spanning 8 different package trees (`core`, `search._shared`, `search.rg`, `search.objects`, `search.python`, `search.rust`, `search.semantic`, `search.tree_sitter`).
- `partition_pipeline.py:129-132`: `_smart_search_module()` returns `Any` to break a circular import, then accesses attributes dynamically (`smart_search_mod.RawMatch`, `smart_search_mod.classify_match`, etc.). This is a symptom of `smart_search.py` defining both data types and orchestration logic.
- `candidate_normalizer.py:17`: Imports `EnrichedMatch` from `smart_search` under `TYPE_CHECKING` to avoid the cycle, but the type dependency still exists.
- `search.objects.resolve.py:22`: Also imports `EnrichedMatch` from `smart_search` under `TYPE_CHECKING`.

**Suggested improvement:**
Move data types (`RawMatch`, `EnrichedMatch`, `SearchStats`, `ClassificationResult`, `MatchEnrichment`, `MatchClassifyOptions`, `SearchSummaryInputs`) to `contracts.py` or a new `types.py` module. This breaks the cycle where `partition_pipeline.py` and `candidate_normalizer.py` need to import from the orchestrator just to reference data types.

**Effort:** medium
**Risk if unaddressed:** medium -- The `Any`-typed dynamic module import in `partition_pipeline.py` defeats type checking and enables subtle runtime errors.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The dependency direction is mostly correct: `contracts.py` and `profiles.py` (core abstractions) have no dependency on `smart_search.py` (the orchestration detail). `classifier.py` depends on `classifier_runtime.py` (lower-level), not the reverse.

**Findings:**
- `contracts.py:16`: Imports `QueryMode` from `classifier.py`, which is appropriate (contracts depend on domain type).
- `partition_pipeline.py:40`: Imports `run_search_partition` is the main export; it imports `SmartSearchContext` from `contracts.py` which is correct direction.
- The one inversion: `orchestration.py:26` imports `_assemble_smart_search_result` (a private function) from `smart_search.py` via lazy import, creating an upward dependency from the facade into the implementation.

**Suggested improvement:**
Have `orchestration.py:assemble_result` accept a callback rather than importing the private `_assemble_smart_search_result` from `smart_search.py`. Alternatively, move the assembly logic to a dedicated module that `orchestration.py` can depend on without reaching into the monolithic file.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The ripgrep integration is well-isolated behind the `rg/` package with its own contracts (`RgRunRequest`, `RgCollector`). The semantic enrichment uses a `LanguageSemanticEnrichmentRequest`/`LanguageSemanticEnrichmentOutcome` pattern that acts as a port. The cache backend is accessed through `get_cq_cache_backend()` which acts as a factory/adapter.

**Findings:**
- `smart_search.py:634-644`: `collect_candidates` properly delegates to `run_rg_json` via `RgRunRequest` -- clean port usage.
- `partition_pipeline.py:13-28`: 15 cache-related imports from `tools.cq.core.cache` -- the cache subsystem acts as an adapter but the import surface is wide.
- `smart_search.py:1011-1038`: `_maybe_rust_tree_sitter_enrichment` directly calls `enrich_rust_context_by_byte_range` -- this is a direct adapter call, not going through a port, but acceptable given the single implementation.

**Suggested improvement:**
No immediate action required. The current adapter boundaries are pragmatic and well-suited to the single-implementation scenario.

**Effort:** medium (only if introducing protocol-based ports)
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
Several knowledge items are duplicated across the pipeline.

**Findings:**
- `smart_search.py:2340-2347` and `smart_search.py:2350-2356`: `_new_python_semantic_telemetry()` and `new_python_semantic_telemetry()` are a private/public pair, but the same counter shape `{"attempted": 0, "applied": 0, "failed": 0, "skipped": 0, "timed_out": 0}` also appears at `smart_search.py:1584-1590` (inside `_build_summary`) and at `smart_search.py:2961-2963` (for rust_semantic_telemetry). The authoritative shape is not declared once.
- `smart_search.py:2447-2483`: `_normalize_python_semantic_degradation_reason` contains a set of explicit reason strings that must stay in sync with the `SemanticContractStateV1` model in `semantic/models.py`. If a new reason is added, both locations must be updated.
- `classifier.py:232-236` and `classifier.py:255-258`: The regex patterns for extracting definition names are duplicated in `_extract_def_name_from_record` and `_definition_name_span` -- identical tuple of three patterns.
- `smart_search.py:801` and `smart_search.py:1113`: The set `{".py", ".pyi"}` for Python file extensions appears in at least two locations. The canonical source should be in `query/language.py`.

**Suggested improvement:**
1. Define `SEMANTIC_TELEMETRY_SHAPE` as a factory function or frozen struct in `_shared/search_contracts.py` and use it everywhere.
2. Extract the duplicated regex patterns in `classifier.py:232-236` into a module-level constant `_DEFINITION_NAME_PATTERNS`.
3. Reference `PYTHON_EXTENSIONS` from `query/language.py` instead of inline sets.

**Effort:** medium
**Risk if unaddressed:** medium -- Telemetry shape drift can cause silent data loss in observability pipelines.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Boundary types use strong contracts: `SearchLimits` uses `PositiveInt`/`PositiveFloat` constraints, all pipeline structs are `frozen=True`, and `CqStruct` provides consistent serialization.

**Findings:**
- `profiles.py:12-41`: `SearchLimits` with `PositiveInt`/`PositiveFloat` constraints ensures invalid limits cannot be constructed.
- `contracts.py:22-33`: `SearchPartitionPlanV1` is frozen and typed.
- `smart_search.py:360-407`: `EnrichedMatch` is frozen with typed fields, but `python_enrichment: dict[str, object] | None` and `rust_tree_sitter: dict[str, object] | None` are untyped dicts that carry no contract. Payloads could contain anything.

**Suggested improvement:**
Define `PythonEnrichmentPayloadV1` and `RustEnrichmentPayloadV1` as msgspec structs (even with `object`-typed extensible fields) to provide at least a named contract for the enrichment payloads, rather than bare `dict[str, object]`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`_coerce_search_request` at `smart_search.py:1742-1762` properly parses raw kwargs into a typed `SearchRequest` at the boundary. Each coercion function (`_coerce_query_mode`, `_coerce_lang_scope`, etc.) converts or defaults.

**Findings:**
- `smart_search.py:1742-1801`: Clean parse-at-boundary pattern with individual coercion functions.
- `smart_search.py:2135-2146`: `_status_from_enrichment` repeatedly validates dict structure at use-site rather than parsing the payload into a typed representation at the boundary. This is a scattered validation anti-pattern.

**Suggested improvement:**
Parse enrichment payloads into a typed `EnrichmentStatusView` at the point they are produced, rather than repeatedly interrogating untyped dicts downstream.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`QueryMode` enum and `MatchCategory` Literal type prevent invalid category/mode values. `SearchLimits` constraint types prevent negative limits.

**Findings:**
- `classifier.py:36-42`: `QueryMode` enum with three valid modes.
- `classifier.py:44-60`: `MatchCategory` as `Literal[...]` type prevents invalid categories at type-check time.
- `smart_search.py:360-407`: `EnrichedMatch` allows `python_enrichment` and `rust_tree_sitter` to both be non-None simultaneously, but an enriched match should be for one language. The `language` field technically constrains this, but the type system does not enforce mutual exclusivity.

**Suggested improvement:**
Consider a tagged union or at minimum a runtime assertion that only the language-appropriate enrichment field is populated. Low priority given the `language` field serves as a discriminator.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 1/3

**Current state:**
Several functions both compute a result and mutate state, violating CQS.

**Findings:**
- `smart_search.py:2886-2969`: `_build_search_summary` builds a summary dict (query) but then mutates it by assigning keys like `summary["query_pack_lint"]`, `summary["cross_language_diagnostics"]`, `summary["language_capabilities"]`, `summary["enrichment_telemetry"]`, etc. It also returns the summary plus diagnostics. This is both a command and a query.
- `smart_search.py:3779-3809`: `_assemble_smart_search_result` mutates `result.summary` after constructing the CqResult (lines 3805-3808), violating CQS on what should be a pure assembly step.
- `smart_search.py:1301-1322`: `_populate_optional_fields` and `_merge_enrichment_payloads` mutate the `data` dict in-place. While documented, this pattern makes the assembly harder to reason about.
- `smart_search.py:1718-1719`: `_build_search_context` calls `clear_caches()` as a side effect -- constructing a context (query) also clears global state (command).
- `classifier_runtime.py:428-435`: `clear_classifier_caches` is a pure command, which is correct CQS. But the caches themselves are populated as side effects of `get_*` query functions.

**Suggested improvement:**
1. Split `_build_search_summary` into a pure summary builder that returns a complete dict, and a separate function that attaches diagnostic/telemetry keys.
2. Move cache clearing out of `_build_search_context` into the caller (`smart_search()`) where the imperative shell lives.
3. Adopt a builder pattern for the summary dict: accumulate keys via method calls, then freeze.

**Effort:** medium
**Risk if unaddressed:** medium -- Mixed query/command functions make caching unsafe and complicate reasoning about when state changes.

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 1/3

**Current state:**
The most significant DI violation is `partition_pipeline.py`'s approach to importing `smart_search.py`.

**Findings:**
- `partition_pipeline.py:129-132`: `_smart_search_module()` imports the entire `smart_search` module and returns `Any`. All subsequent access (`smart_search_mod.RawMatch`, `smart_search_mod.classify_match`, `smart_search_mod.LanguageSearchResult`) is untyped. This defeats static analysis and makes refactoring fragile.
- `partition_pipeline.py:458-459`: `getattr(smart_search_mod, "MAX_SEARCH_CLASSIFY_WORKERS", 1)` -- dynamic attribute access with fallback, further defeating type safety.
- `classifier_runtime.py:65-70`: Caches are created at module level with no injection point. Tests must call `clear_classifier_caches()` and hope no concurrent test shares state.
- `smart_search.py:204`: `_SEARCH_OBJECT_VIEW_REGISTRY` is a module-global dict with no injection mechanism.

**Suggested improvement:**
1. Move data types out of `smart_search.py` so `partition_pipeline.py` can import them directly with full typing.
2. Pass a `ClassifierCacheManager` instance through the call chain instead of relying on module globals.
3. Pass `MAX_SEARCH_CLASSIFY_WORKERS` as a configuration value on `SmartSearchContext` rather than reading it dynamically from a module.

**Effort:** medium
**Risk if unaddressed:** medium -- The `Any`-typed module pattern is a runtime error waiting to happen and prevents IDE navigation.

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies exist in the pipeline. All behavior is composed via function calls, dataclasses, and msgspec structs.

**Findings:**
- `orchestration.py:67`: `SearchPipeline` is a simple dataclass with composed behavior via callback parameters.
- All structs use `frozen=True` composition semantics.

No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
Several locations navigate deep object chains.

**Findings:**
- `smart_search.py:3082`: `object_runtime.view.summaries[0].object_ref.object_id` -- four-level chain through `ObjectResolutionRuntime.view.summaries[].object_ref.object_id`.
- `smart_search.py:3039-3043`: `object_ref.canonical_file`, `object_ref.canonical_line` -- accessing through `summary.object_ref.*` repeatedly.
- `smart_search.py:2822-2828`: `payload.get("type_contract").get("callable_signature")` -- two-level dict navigation with isinstance guards, repeated for multiple payload fields.
- `smart_search.py:3105-3106`: `primary_target_finding.details.get("name", "")` -- reaching into Finding's detail dict.

**Suggested improvement:**
Add helper methods or accessor functions on `ObjectResolutionRuntime` for common queries like "get representative match for top object" and "get primary target". For payload dict navigation, extract helper functions like `_payload_type_contract_signature(payload)`.

**Effort:** small
**Risk if unaddressed:** medium -- Deep chains break when intermediate structures change.

---

#### P15. Tell, don't ask -- Alignment: 1/3

**Current state:**
The pipeline extensively interrogates untyped `dict[str, object]` payloads with `isinstance` checks before acting.

**Findings:**
- `smart_search.py:2213-2236`: `_accumulate_python_enrichment` performs 8 `isinstance` checks on dict values before accumulating counters. The enrichment payload should know how to report its own stage status.
- `smart_search.py:2247-2310`: `_accumulate_rust_enrichment` similarly interrogates payload dicts with defensive isinstance guards.
- `smart_search.py:2135-2146`: `_status_from_enrichment` asks the payload dict "do you have a meta key? does it have an enrichment_status?" -- the payload should expose `.status` directly.
- `smart_search.py:3279-3292`: `_payload_coverage_status` asks the payload about its own coverage status through dict inspection.
- `smart_search.py:3295-3314`: `_rust_payload_has_signal` interrogates the payload with 8+ nested dict accesses.

**Suggested improvement:**
Define typed wrapper structs (e.g., `EnrichmentPayloadView`) that parse the `dict[str, object]` at the enrichment boundary and expose typed accessors (`.status`, `.stage_timings`, `.has_signal`). The downstream code then tells the view what to report rather than asking the raw dict.

**Effort:** medium
**Risk if unaddressed:** medium -- Scattered isinstance checks are fragile and duplicated.

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 1/3

**Current state:**
Classification logic (pure transforms) is mixed with IO (file reads via `get_cached_source`, subprocess execution via ripgrep) and mutable state (cache population) in the same call paths.

**Findings:**
- `classifier_runtime.py:370-400`: `get_sg_root` reads a file from disk (`file_path.read_text`), parses it, and populates a module-global cache -- all in a "getter" function. This mixes IO, parsing (pure), and state mutation.
- `smart_search.py:664-766`: `classify_match` calls `get_cached_source` (IO), `get_sg_root` (IO + cache mutation), `enrich_python_context_by_byte_range` (IO), and `enrich_rust_context_by_byte_range` (IO) -- the entire enrichment path is an impure function.
- `classifier.py:318-396`: `classify_heuristic` is a genuinely pure function (takes line/col/match_text, returns HeuristicResult) -- this is the functional core done right.
- `classifier.py:399-467`: `classify_from_node` is pure given a pre-resolved `SgRoot` and `SgNode`.

**Suggested improvement:**
Separate the cache-populating IO layer from the pure classification transforms. The pattern should be: (1) imperative shell reads files and populates caches, (2) pure functions classify from pre-loaded data. `classify_heuristic` and `classify_from_resolved_node` already follow this pattern. Apply it to the full enrichment pipeline by having the caller ensure data is loaded before calling pure enrichment functions.

**Effort:** large
**Risk if unaddressed:** medium -- Mixing IO and logic makes unit testing require filesystem access or monkeypatching.

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
The search pipeline is largely read-only: it reads source files, runs ripgrep, and produces results. Cache writes are content-addressed. The `clear_caches()` call at context construction ensures a fresh start.

**Findings:**
- `smart_search.py:1718-1719`: `clear_caches()` ensures each `smart_search()` invocation starts clean.
- `partition_pipeline.py:412-435`: Cache writes use content-hashed keys with TTL -- writing the same key twice is safe.
- `smart_search.py:2783`: `_register_search_object_view` is additive (dict assignment) and idempotent for same run_id.

No immediate action needed.

**Effort:** small
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 2/3

**Current state:**
Parallel classification results are sorted by index to preserve deterministic ordering. ProcessPool uses `spawn` context for clean process state.

**Findings:**
- `smart_search.py:1905`: `indexed_results.sort(key=lambda pair: pair[0])` ensures deterministic output ordering after parallel classification.
- `partition_pipeline.py:501`: `rows.sort(key=lambda item: item.idx)` similarly sorts enrichment results.
- Potential concern: `_resolve_search_worker_count` at `smart_search.py:1923-1926` bases worker count on runtime partition count, meaning the same query could use 1 or 4 workers depending on file distribution, but output ordering is preserved regardless.

No immediate action needed.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 1/3

**Current state:**
`smart_search.py` at 3,914 lines with 100+ functions is the primary simplicity violation. Understanding the search pipeline requires holding a mental model of all six concerns simultaneously.

**Findings:**
- `smart_search.py`: 3,914 LOC -- the largest file in the entire codebase. A new contributor cannot reasonably understand this file in a single reading session.
- `smart_search.py:3656-3719`: `_prepare_search_assembly_inputs` calls 8 helper functions and constructs a 12-field `_SearchAssemblyInputs` dataclass. The assembly logic requires tracing through multiple levels of indirection.
- `smart_search.py:1462-1521`: `build_summary` with legacy positional argument support adds complexity for backward compatibility. The dual calling convention (SearchSummaryInputs vs 5 positional args) increases cognitive load.
- `smart_search.py:2447-2483`: `_normalize_python_semantic_degradation_reason` has multiple branches for normalizing reason strings, with prefix stripping and explicit reason set membership checks -- complex for what is essentially a string normalization.

**Suggested improvement:**
The file decomposition from P2/P3 is the primary simplicity improvement. After extraction, `smart_search.py` should be under 1,000 lines, containing only the top-level entry points and the orchestration skeleton.

**Effort:** large
**Risk if unaddressed:** high -- Complexity compounds: each new feature added to this file makes all existing features harder to understand and maintain.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
The codebase is lean. No speculative abstractions were observed.

**Findings:**
- `orchestration.py:67-106`: `SearchPipeline` is a thin facade that delegates immediately. It does not add speculative extension points.
- `profiles.py:44-80`: Five named profiles (DEFAULT, INTERACTIVE, AUDIT, CI, LITERAL) each serve a documented use case.
- `smart_search.py:1462`: The dual calling convention for `build_summary` could be considered legacy baggage, but it serves documented backward compatibility.

No immediate action needed.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
Several API behaviors may surprise competent readers.

**Findings:**
- `smart_search.py:1462-1521`: `build_summary()` accepts either a `SearchSummaryInputs` or 5 positional args with kwargs. This dual-arity API is surprising -- a reader cannot determine the expected arguments without reading the implementation.
- `smart_search.py:1718-1719`: `_build_search_context` calls `clear_caches()` as a side effect. A function named "build context" should construct a context, not clear global state.
- `__init__.py:7-11`: Module-level `SearchContext: Any = None` assignments followed by `__getattr__` lazy loading means that `from tools.cq.search.pipeline import SearchContext` returns `None` unless accessed via attribute. This could surprise callers who import the name at module level.
- `classifier_runtime.py:438-449`: Exporting `_find_containing_scope`, `_find_node_at_position`, `_is_docstring_context` in `__all__` despite underscore prefix violates naming convention expectations.

**Suggested improvement:**
1. Deprecate the legacy 5-arg `build_summary` signature; accept only `SearchSummaryInputs`.
2. Move `clear_caches()` out of `_build_search_context` into the caller.
3. Remove underscore-prefixed names from `__all__` or rename them without the underscore prefix.

**Effort:** small
**Risk if unaddressed:** medium -- Surprising APIs lead to bugs in callers.

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
Every file declares `__all__`. Data types use `CqStruct` with `frozen=True` for stable serialization. Version suffixes (`V1`) appear on serialized contracts.

**Findings:**
- `contracts.py:22`: `SearchPartitionPlanV1` has a version suffix.
- All 10 files declare `__all__`.
- `smart_search.py:3893-3914`: `__all__` lists 15 public names, providing a clear contract surface.
- `smart_search.py:207-258`: `RawMatch(CqStruct, frozen=True)` is a versioned, serializable contract.

No immediate action needed. The versioning convention is consistent.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
Module-global caches, tight coupling to the filesystem, and the monolithic `smart_search.py` make unit testing difficult.

**Findings:**
- `classifier_runtime.py:65-70`: Tests must call `clear_classifier_caches()` before/after each test to avoid state leakage. Without this, cached SgRoot objects from one test can affect another.
- `smart_search.py:664-766`: `classify_match` cannot be tested without filesystem access because it calls `get_cached_source` (reads files) and `get_sg_root` (parses files). There is no injection point for pre-loaded source.
- `partition_pipeline.py:129-132`: The `Any`-typed `_smart_search_module()` pattern prevents IDE-assisted test writing and requires `cast` or type: ignore in tests.
- `smart_search.py:204`: `_SEARCH_OBJECT_VIEW_REGISTRY` persists across tests unless explicitly cleared, creating hidden cross-test dependencies.
- `smart_search.py:1855-1906`: `_run_classification_phase` spawns ProcessPool workers that import modules in fresh processes, making it impossible to mock dependencies in the worker context.

**Suggested improvement:**
1. Make caches injectable: `classify_match(raw, root, cache=default_cache)`.
2. Accept pre-loaded source in enrichment functions: `_maybe_python_enrichment(..., source=source, sg_root=sg_root)` -- partially done with `resolved_python` parameter.
3. Add a `SearchPipelineConfig` that includes `max_workers` and `cache_manager` so tests can override parallel execution and cache behavior.

**Effort:** medium
**Risk if unaddressed:** high -- Difficult-to-test code accumulates bugs; flaky tests from shared global state erode CI confidence.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
The pipeline has thorough observability infrastructure: enrichment telemetry with per-stage status/timing counters, cache hit/miss metrics, cross-language diagnostics, query pack lint status, and semantic enrichment telemetry.

**Findings:**
- `smart_search.py:2149-2188`: `_empty_enrichment_telemetry` provides a comprehensive per-language telemetry shape.
- `smart_search.py:2313-2337`: `_build_enrichment_telemetry` accumulates status from every match.
- `smart_search.py:2090-2132`: `_build_tree_sitter_runtime_diagnostics` emits structured findings for runtime limit exceedances.
- `smart_search.py:3860-3864`: Stage timings (partition, assemble, total) are recorded in the summary.
- `partition_pipeline.py:218-253`: Cache hit/miss recording via `record_cache_get`/`record_cache_set`.

The observability data is well-structured. The main gap is that telemetry accumulation logic is scattered across `smart_search.py` rather than consolidated.

**Suggested improvement:**
Consolidate all telemetry accumulation into the proposed `enrichment_telemetry.py` module for single-point auditing of telemetry shape changes.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: The `smart_search.py` Monolith

**Description:** At 3,914 lines, `smart_search.py` is the gravitational center of the pipeline, defining core data types, orchestration logic, enrichment dispatch, telemetry accumulation, and result assembly. It is both the most-imported and the most-importing module in the subsystem.

**Root cause:** Organic growth. New features (Python semantic enrichment, Rust enrichment, neighborhood preview, semantic insight) were added to the existing entry-point file rather than extracted into focused modules.

**Affected principles:** P1 (information hiding), P2 (separation of concerns), P3 (SRP), P4 (coupling), P11 (CQS), P16 (functional core), P19 (KISS), P21 (least astonishment), P23 (testability).

**Suggested approach:** Phase the decomposition:
1. **Phase 1 (medium effort):** Extract data types to `types.py`. Eliminates circular import hacks and `Any`-typed module access.
2. **Phase 2 (medium effort):** Extract enrichment telemetry to `enrichment_telemetry.py` and Python semantic management to `python_semantic.py`.
3. **Phase 3 (medium effort):** Extract result assembly to `assembly.py`.
4. **Target:** `smart_search.py` reduced to under 1,000 lines containing `smart_search()`, `classify_match()`, `collect_candidates()`, and the orchestration skeleton.

### Theme 2: Module-Global Mutable State

**Description:** Six bare dict caches in `classifier_runtime.py` plus `_SEARCH_OBJECT_VIEW_REGISTRY` in `smart_search.py` create hidden mutable state that is shared across invocations and test cases.

**Root cause:** Performance optimization (caching) implemented via the simplest mechanism (module globals) without encapsulation.

**Affected principles:** P1 (information hiding), P12 (DI), P16 (functional core), P17 (idempotency), P23 (testability).

**Suggested approach:** Encapsulate caches in a `ClassifierCacheManager` class with `get`/`clear` methods and an injectable constructor. Thread it through the call chain or use a context-local pattern. This enables per-test isolation without global `clear_caches()` calls.

### Theme 3: Untyped Dict Payloads

**Description:** Enrichment payloads (`python_enrichment`, `rust_tree_sitter`, `python_semantic_enrichment`) are `dict[str, object]` throughout the pipeline. Downstream code interrogates these dicts with extensive `isinstance` checks, duplicating structural knowledge.

**Root cause:** Enrichment payloads originate from multiple independent subsystems (tree-sitter, Python AST, semantic provider) with different schemas. The dicts serve as a lowest-common-denominator interchange format.

**Affected principles:** P7 (DRY), P9 (parse don't validate), P10 (illegal states), P15 (tell don't ask).

**Suggested approach:** Define typed "view" structs for each enrichment payload family that parse the dict at the enrichment boundary. Downstream code operates on typed accessors rather than raw dict interrogation. This is incremental: start with `PythonEnrichmentView` and `RustEnrichmentView`.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P3, P4 | Extract data types (`RawMatch`, `EnrichedMatch`, `SearchStats`, etc.) from `smart_search.py` to `types.py` | medium | Eliminates circular imports, enables typed access in `partition_pipeline.py`, reduces `smart_search.py` by ~200 LOC |
| 2 | P7 | Extract duplicated classifier regex patterns in `classifier.py:232-236` and `classifier.py:255-258` into `_DEFINITION_NAME_PATTERNS` constant | small | Eliminates drift between name extraction and name span |
| 3 | P21 | Move `clear_caches()` from `_build_search_context` to `smart_search()` caller; remove `_`-prefixed names from `classifier_runtime.py` `__all__` | small | Reduces surprise in API behavior |
| 4 | P11 | Split `_build_search_summary` into pure builder + separate mutator for diagnostic/telemetry keys | small | Clearer data flow, enables summary caching |
| 5 | P2, P19 | Extract enrichment telemetry functions (~190 LOC) from `smart_search.py` to `enrichment_telemetry.py` | medium | First step of monolith decomposition with zero external API change |

## Recommended Action Sequence

1. **Extract data types from `smart_search.py` to `types.py`** (P3, P4). Move `RawMatch`, `EnrichedMatch`, `SearchStats`, `ClassificationResult`, `MatchEnrichment`, `MatchClassifyOptions`, `SearchSummaryInputs`, `ClassificationBatchTask`, `ClassificationBatchResult`, `KIND_WEIGHTS`, and `SMART_SEARCH_LIMITS` to a new `tools/cq/search/pipeline/types.py`. Update all importers. This eliminates the `Any`-typed `_smart_search_module()` pattern in `partition_pipeline.py`.

2. **Consolidate duplicated knowledge** (P7). Extract `_DEFINITION_NAME_PATTERNS` in `classifier.py`. Define `SEMANTIC_TELEMETRY_SHAPE` factory in `_shared/search_contracts.py`. Use `PYTHON_EXTENSIONS` from `query/language.py`.

3. **Fix CQS violations** (P11, P21). Move `clear_caches()` out of `_build_search_context`. Split `_build_search_summary` into pure builder + mutator. Remove `_`-prefixed names from `classifier_runtime.py` `__all__`.

4. **Extract enrichment telemetry** (P2, P19). Move `_empty_enrichment_telemetry`, `_accumulate_*`, `_build_enrichment_telemetry`, `_status_from_enrichment`, `_attach_enrichment_cache_stats` to `enrichment_telemetry.py`.

5. **Extract Python semantic management** (P2, P3). Move `_prefetch_python_semantic_*`, `_merge_match_with_python_semantic_*`, `_attach_python_semantic_enrichment`, `_build_python_semantic_overview`, `_seed_python_semantic_state`, all `_python_semantic_*_diagnostic` functions to `python_semantic.py`.

6. **Extract result assembly** (P2, P3). Move `_prepare_search_assembly_inputs`, `_assemble_smart_search_result`, `_assemble_search_insight`, `_build_search_result_key_findings`, `_collect_definition_candidates`, `_build_tree_sitter_neighborhood_preview` to `assembly.py`.

7. **Encapsulate classifier caches** (P1, P12, P23). Replace the six module-global dicts in `classifier_runtime.py` with a `ClassifierCacheManager` class. Make it injectable in `classify_match` and the classification phase.

8. **Define typed enrichment views** (P7, P9, P15). Create `PythonEnrichmentView` and `RustEnrichmentView` structs that parse `dict[str, object]` at the enrichment boundary. Replace downstream isinstance chains with typed accessor calls.

Steps 1-3 are independent quick wins. Steps 4-6 form the monolith decomposition sequence (do in order). Steps 7-8 are deeper structural improvements that can proceed in parallel after steps 1-6.
