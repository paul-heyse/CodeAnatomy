# Design Review: tools/cq/search/ (Pipeline & Infrastructure)

**Date:** 2026-02-16
**Scope:** `tools/cq/search/{pipeline,_shared,rg,enrichment,semantic,objects}`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 49

## Executive Summary

The search pipeline demonstrates strong ports-and-adapters design in the enrichment subsystem and clean contract usage via msgspec Structs. However, the pipeline layer is dominated by `smart_search.py` (~2046 LOC, 24 exports) which concentrates candidate collection, classification, enrichment, summary building, and section rendering into a single module. This god-module pattern cascades into SRP violations, testability gaps, and three thin wrapper modules that exist only because the extraction was incomplete. Six module-level mutable caches in `classifier_runtime.py` with manual clear lifecycle create hidden coupling across the pipeline. The highest-priority improvements are decomposing `smart_search.py`, replacing mutable global caches with an injected cache context, and eliminating `Any` types from `partition_pipeline.py`.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | medium | 6 module-level mutable caches exposed for manual clearing |
| 2 | Separation of concerns | 1 | large | high | `smart_search.py` mixes 5+ concerns in 2046 lines |
| 3 | SRP | 1 | large | high | `smart_search.py` changes for candidate, classify, enrich, summary, section reasons |
| 4 | High cohesion, low coupling | 1 | medium | medium | `_shared/core.py` is a grab-bag; thin wrappers import back into god module |
| 5 | Dependency direction | 2 | small | low | Core contracts are clean; some reverse imports in wrappers |
| 6 | Ports & Adapters | 3 | - | - | `LanguageEnrichmentPort` protocol + adapter registry is well done |
| 7 | DRY | 1 | medium | medium | Rust telemetry accumulation duplicated across 3 modules |
| 8 | Design by contract | 1 | medium | medium | `Any` types in `partition_pipeline.py`; `*args/**kwargs` in `build_summary` |
| 9 | Parse, don't validate | 2 | small | low | Boundary coercion present but `_coerce_summary_inputs` uses runtime casts |
| 10 | Illegal states | 2 | small | low | Frozen msgspec Structs enforce invariants; some dict-typed payloads remain |
| 11 | CQS | 2 | small | low | `append_source` and `set_degraded` are clean mutators; minor mixed returns in caches |
| 12 | DI + explicit composition | 1 | medium | medium | Module-level caches are hidden creation; no injection path |
| 13 | Composition over inheritance | 3 | - | - | Adapter pattern used throughout; no deep hierarchies |
| 14 | Law of Demeter | 2 | small | low | Some deep dict drilling in telemetry; mostly clean |
| 15 | Tell, don't ask | 2 | small | low | `_resolution_key_payload` in resolve.py does quality-gated identity extraction |
| 16 | Functional core, imperative shell | 2 | medium | low | Pure transforms exist but interleaved with cache I/O in partition_pipeline |
| 17 | Idempotency | 3 | - | - | Search is read-only; cache writes are idempotent by key |
| 18 | Determinism | 3 | - | - | `build_rg_command` is deterministic; `_JSON_ENCODER` uses deterministic ordering |
| 19 | KISS | 2 | small | low | `_coerce_summary_inputs` adds complexity for backward compat |
| 20 | YAGNI | 2 | small | low | `_SUMMARY_ARGS_COUNT` legacy path may be removable |
| 21 | Least astonishment | 1 | medium | medium | `build_summary(*args, **kwargs)` signature hides expected types |
| 22 | Declare public contracts | 2 | small | low | `__all__` lists present; some internal types leak via re-exports |
| 23 | Design for testability | 1 | medium | high | Module-level caches require `clear_caches()` calls; no DI seam |
| 24 | Observability | 2 | small | low | Telemetry well-structured but duplicated across modules |

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
Six module-level mutable dict caches in `classifier_runtime.py:65-70` (`_sg_cache`, `_source_cache`, `_def_lines_cache`, `_symtable_cache`, `_record_context_cache`, `_node_index_cache`) are directly accessed by functions in the same module but require external callers to invoke `clear_classifier_caches()` (`classifier_runtime.py:428`) to manage their lifecycle. The `classifier.py:620` `clear_caches()` function further chains this with `clear_python_enrichment_cache()` and `clear_tree_sitter_rust_cache()`, coupling cache lifecycle knowledge across three modules.

**Findings:**
- `classifier_runtime.py:65-70`: Six bare module-level dicts hold mutable cache state with no size bounds or eviction policy.
- `classifier.py:620-627`: `clear_caches()` reaches across module boundaries to clear caches in `python/extractors.py` and `tree_sitter/rust_lane/runtime.py`.
- `_shared/core.py:29-43`: `_RUNTIME_ONLY_ATTR_NAMES` frozenset leaks internal attribute naming conventions to callers.

**Suggested improvement:**
Introduce a `ClassificationCacheContext` dataclass that owns all six caches and is passed through classification functions via a parameter rather than module globals. This allows test isolation without `clear_caches()` calls and enables bounded eviction via `BoundedCache` (already available at `_shared/bounded_cache.py`).

**Effort:** medium
**Risk if unaddressed:** medium -- test pollution between runs, unbounded memory growth in long-running sessions.

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`smart_search.py` (~2046 lines) contains candidate collection (`collect_candidates`, `run_candidate_phase`), match classification (`classify_match`, `_run_classification_phase`), enrichment orchestration (`_maybe_python_enrichment`, `_maybe_rust_tree_sitter_enrichment`, `_maybe_python_semantic_enrichment`), summary construction (`build_summary`, `_build_summary`, `_build_search_summary`), section rendering (`build_sections`, `build_finding`), follow-up generation (`build_followups`), scoring (`compute_relevance_score`), and the main entry point (`smart_search`). These are independent concerns that change for different reasons.

**Findings:**
- `smart_search.py:1-2046`: Single module with 24 exports in `__all__` spanning 5+ distinct concerns.
- `smart_search_followups.py:20-22`: `generate_followup_suggestions` is a 1-line wrapper that imports from `smart_search.build_followups` -- evidence of incomplete extraction.
- `smart_search_sections.py`: Similar thin wrapper pattern, delegating back to `smart_search.build_sections`.
- `smart_search_summary.py`: Delegates to `smart_search._build_search_summary` and `build_summary`.

**Suggested improvement:**
Complete the extraction that was started. Move candidate collection to a dedicated `candidate_phase.py`, classification to `classify_phase.py`, enrichment to `enrichment_phase.py`, and summary/section builders into their existing wrapper modules (removing the circular delegation). The `smart_search.py` entry point should become a thin orchestrator (~200 lines) that composes phases.

**Effort:** large
**Risk if unaddressed:** high -- any change to scoring, classification, or rendering risks breaking unrelated functionality.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`smart_search.py` changes for at least five independent reasons: ripgrep candidate collection logic, AST/symtable classification rules, Python/Rust enrichment orchestration, summary/section output format, and relevance scoring weights. `_shared/core.py` similarly mixes byte-offset helpers, JSON encoder singletons, serializable contracts, runtime-only dataclass handles, request envelopes, and timeout utilities.

**Findings:**
- `smart_search.py:2024-2045`: 24 symbols in `__all__` spanning candidate, classify, enrich, summary, section, and scoring concerns.
- `_shared/core.py:1`: Module docstring explicitly names "helpers, contracts, runtime handles, requests, and timeout utilities" -- five responsibilities.
- `semantic/models.py:1`: "Consolidated semantic contracts, state, helpers, and front-door adapter functions" -- four responsibilities.

**Suggested improvement:**
For `_shared/core.py`: split into `_shared/helpers.py` (byte offset, truncate, hash), `_shared/encoding.py` (JSON encoder/decoder -- already has a facade file that could become the owner), `_shared/requests.py` (RgRunRequest, enrichment request types), and `_shared/timeout.py` (already has a facade). For `semantic/models.py`: separate contracts (Structs) from helper functions (`call_with_retry`, `budget_for_mode`, `resolve_language_provider_root`).

**Effort:** large (smart_search.py), medium (_shared/core.py, semantic/models.py)
**Risk if unaddressed:** high -- maintainers cannot reason about change impact when a single file owns unrelated concerns.

---

#### P4. High cohesion, low coupling -- Alignment: 1/3

**Current state:**
The thin wrapper modules (`smart_search_followups.py`, `smart_search_sections.py`, `smart_search_summary.py`) create a circular dependency pattern: they import from `smart_search.py` to re-export functions that were nominally "extracted" but never actually moved. This makes `smart_search.py` the gravitational center with high fan-in from its own supposed decomposition.

**Findings:**
- `smart_search_followups.py:20`: `from tools.cq.search.pipeline.smart_search import build_followups` -- the "extracted" module imports back from the monolith.
- `partition_pipeline.py:46`: Imports `run_file_lanes_parallel` from `tree_sitter.core.infrastructure` -- cross-cutting coupling to tree-sitter internals.
- `_shared/core.py` re-export facades (`encoding.py`, `timeout.py`, `rg_request.py`): These add indirection without cohesion benefit since the implementations remain in `core.py`.

**Suggested improvement:**
Move the function implementations from `smart_search.py` into their respective wrapper modules, then have `smart_search.py` import from them (inverting the current direction). Promote `_shared/encoding.py`, `_shared/timeout.py`, and `_shared/rg_request.py` from re-export facades to owning modules by moving the relevant code out of `core.py`.

**Effort:** medium
**Risk if unaddressed:** medium -- the circular delegation pattern confuses contributors about where to make changes.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Core contracts (`contracts.py`, `smart_search_types.py`, `_shared/types.py`) are properly dependency-free. The `LanguageEnrichmentPort` protocol in `enrichment/contracts.py` defines the abstraction that adapters implement. However, `front_door.py:33-34` imports concrete extractors (`enrich_python_context_by_byte_range`, `enrich_rust_context_by_byte_range`) directly rather than going through the port abstraction.

**Findings:**
- `enrichment/contracts.py:42-61`: `LanguageEnrichmentPort` protocol is well-defined with three methods.
- `semantic/front_door.py:33-35`: Imports concrete Python and Rust extractors directly, bypassing the adapter pattern.
- `partition_pipeline.py:9`: Imports `Any` from typing -- the `_EnrichmentMissTask.items` field at line 78 uses `list[tuple[int, Any, str, str]]` instead of a typed tuple.

**Suggested improvement:**
Have `front_door.py` dispatch to language-specific providers via the `LanguageEnrichmentPort` registry rather than importing concrete extractors. This would make the semantic front door extensible to new languages without modification.

**Effort:** small
**Risk if unaddressed:** low -- currently only Python and Rust are supported, so the concrete imports work.

---

#### P6. Ports & Adapters -- Alignment: 3/3

**Current state:**
The enrichment subsystem is a clean ports-and-adapters implementation. `LanguageEnrichmentPort` (protocol at `enrichment/contracts.py:42`) defines three methods. `PythonEnrichmentAdapter` and `RustEnrichmentAdapter` implement the protocol. `language_registry.py` provides a simple registry with lazy default registration. This is the strongest design in the scope.

**Findings:**
- No significant gaps. The port/adapter/registry pattern is well-executed.

**Effort:** --
**Risk if unaddressed:** --

---

### Category: Knowledge

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
Rust telemetry accumulation logic is implemented three times: `rust_adapter.py:86-125` (`_accumulate_runtime_flags`, `_accumulate_bundle_drift`), `smart_search_telemetry.py:218-287` (`accumulate_rust_runtime`, `accumulate_rust_bundle`), and partially in `smart_search_telemetry.py:189-216` (`accumulate_rust_enrichment`). The `_counter` / `int_counter` helper appears twice with identical semantics (`rust_adapter.py:86-87` and `smart_search_telemetry.py:203-209`).

Additionally, the set of metadata/excluded keys is repeated: `enrichment/core.py:20-22` (`_DEFAULT_METADATA_KEYS`), `enrichment/core.py:239-251` (`_PY_FLAT_EXCLUDED_KEYS`), and `enrichment/core.py:363-385` (inline set literal in `normalize_rust_payload`).

**Findings:**
- `rust_adapter.py:86-87` vs `smart_search_telemetry.py:203-209`: Identical `_counter`/`int_counter` helper duplicated.
- `rust_adapter.py:90-125` vs `smart_search_telemetry.py:218-287`: Same runtime flag and bundle drift accumulation logic duplicated.
- `enrichment/core.py:239-251` vs `enrichment/core.py:373-385`: Excluded key set duplicated as frozenset vs inline literal.

**Suggested improvement:**
The `build_enrichment_telemetry` function in `smart_search_telemetry.py:290-321` already delegates to `adapter.accumulate_telemetry()` via the port. Remove the duplicated `accumulate_rust_runtime` and `accumulate_rust_bundle` functions from `smart_search_telemetry.py` -- they are only called from `accumulate_rust_enrichment` which is itself bypassed by the port-based path. Extract the metadata key exclusion set to a single constant in `enrichment/contracts.py`.

**Effort:** medium
**Risk if unaddressed:** medium -- divergent telemetry logic will produce inconsistent metrics.

---

#### P8. Design by contract -- Alignment: 1/3

**Current state:**
`partition_pipeline.py` uses `Any` types extensively, undermining static analysis. The `build_summary` function at `smart_search.py:1215` accepts `*args: object, **kwargs: object` and performs runtime type coercion via `_coerce_summary_inputs` with unchecked `cast()` calls, creating a dynamically-typed boundary in an otherwise statically-typed codebase.

**Findings:**
- `partition_pipeline.py:78`: `_EnrichmentMissTask.items: list[tuple[int, Any, str, str]]` -- the `Any` hides what the second element actually is.
- `partition_pipeline.py:84,87`: `_EnrichmentMissResult.raw: Any` and `enriched_match: Any` -- concrete types are known but not declared.
- `smart_search.py:1215-1228`: `build_summary(*args, **kwargs)` with `_coerce_summary_inputs` doing runtime type negotiation.
- `smart_search.py:1242-1274`: 10+ `cast()` calls to coerce `object` to specific types without validation.

**Suggested improvement:**
Replace `Any` in `_EnrichmentMissTask` and `_EnrichmentMissResult` with the concrete types (`RawMatch`, `EnrichedMatch`). Replace `build_summary(*args, **kwargs)` with two explicit overloads: `build_summary(inputs: SearchSummaryInputs)` and a deprecated `build_summary_legacy(query, mode, stats, matches, limits, **kwargs)` with proper types.

**Effort:** medium
**Risk if unaddressed:** medium -- type errors in enrichment cache payloads will not be caught statically.

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Boundary parsing is generally well done. `rg/codec.py` uses msgspec tagged union decoders (`_RG_TYPED_DECODER`) to parse ripgrep JSON into typed events at the boundary. `enrichment/core.py:57-82` provides `parse_python_enrichment` and `parse_rust_enrichment` using `msgspec.convert()` for boundary coercion. However, `_coerce_summary_inputs` at `smart_search.py:1230-1274` re-validates and casts types at an internal boundary rather than parsing once.

**Findings:**
- `rg/codec.py:70-85`: Clean tagged union parsing with fallback decoder -- good boundary design.
- `smart_search.py:1230-1274`: `_coerce_summary_inputs` performs runtime type negotiation instead of requiring callers to provide the parsed type.

**Suggested improvement:**
Require all callers of `build_summary` to construct `SearchSummaryInputs` directly, eliminating the coercion layer.

**Effort:** small
**Risk if unaddressed:** low -- the coercion layer works but adds maintenance burden.

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Frozen msgspec Structs (`SearchConfig`, `SearchRequest`, `EnrichedMatch`, `RawMatch`) prevent mutation after construction. `SearchLimits` uses `PositiveInt` and `PositiveFloat` constrained types. However, many internal data structures use `dict[str, object]` for payloads (e.g., `EnrichmentMeta.data`, telemetry buckets), allowing arbitrary key/value combinations.

**Findings:**
- `_shared/types.py:39-54`: `SearchLimits` with positively-constrained fields -- good design.
- `enrichment/contracts.py:32,39`: `PythonEnrichmentPayload.data` and `RustEnrichmentPayload.data` are `dict[str, object]` -- no structural constraint on the data plane.
- `smart_search_telemetry.py:44-90`: `empty_enrichment_telemetry` builds a nested dict template -- the shape is only enforced by convention, not types.

**Suggested improvement:**
Replace the telemetry template dict with a frozen msgspec Struct (`EnrichmentTelemetryV1`) that makes the expected shape explicit. This prevents typos in key names and enables static validation.

**Effort:** small
**Risk if unaddressed:** low -- current tests catch shape errors, but the risk grows with new languages.

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. `enrichment/core.py:85-103` has clean command functions (`append_source`, `set_degraded`) that mutate payloads without returning values. `enrichment/core.py:127-151` `enforce_payload_budget` is a command that also returns `(dropped_keys, final_size)` -- this is a minor CQS tension but the return value reports the effect of the mutation.

**Findings:**
- `enrichment/core.py:127-151`: `enforce_payload_budget` mutates `payload` in-place AND returns `(dropped, size)`. The return could be separated.
- `classifier_runtime.py:77-100`: `get_record_context` queries cache AND populates it on miss -- acceptable cache pattern.

**Suggested improvement:**
Consider splitting `enforce_payload_budget` into a query (`compute_fields_to_drop`) and a command (`drop_fields`), but this is low priority given the pattern is common for budget-enforcement APIs.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition

#### P12. DI + explicit composition -- Alignment: 1/3

**Current state:**
The six module-level caches in `classifier_runtime.py:65-70` are the primary DI gap. These are created implicitly at module import time and accessed as hidden global state. `front_door.py:12` calls `get_cq_cache_backend()` as a service locator rather than receiving a cache instance. `smart_search.py:128` imports `get_python_analysis_session` as another service locator call.

**Findings:**
- `classifier_runtime.py:65-70`: Six module-level mutable dicts -- no injection path, no lifecycle management beyond `clear()`.
- `front_door.py:12`: `get_cq_cache_backend()` service locator pattern rather than DI.
- `semantic/models.py:439`: `enrich_with_language_semantics` performs a deferred import of `run_language_semantic_enrichment` -- hiding the dependency.

**Suggested improvement:**
Define a `SearchRuntimeContext` protocol or dataclass that bundles cache backend, analysis session, and classifier caches. Thread it through `smart_search()` -> `_run_language_partitions()` -> `classify_match()`. This enables test isolation and explicit lifecycle management.

**Effort:** medium
**Risk if unaddressed:** medium -- test isolation requires manual `clear_caches()` calls, which are easy to forget.

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies exist in this scope. The enrichment subsystem uses protocol-based composition. `SearchPipeline` (`orchestration.py:66`) is a simple dataclass facade. All type contracts use frozen msgspec Structs without inheritance depth.

**Findings:**
- No issues. The codebase correctly favors composition and protocols.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most access chains are short. The telemetry accumulation functions drill into nested dicts (`payload.get("meta").get("stage_status")`), but this is mitigated by isinstance guards at each level.

**Findings:**
- `smart_search_telemetry.py:145-155`: `accumulate_python_enrichment` navigates `payload -> meta -> stage_status -> stages_bucket` with isinstance guards -- acceptable for dict-typed data but fragile.
- `smart_search.py:684-689`: `enrichment.rust_tree_sitter.get("impl_type")` -- reaching into enrichment dict internals.

**Suggested improvement:**
Extract accessor methods on the enrichment adapter or enrichment types that encapsulate the dict traversal, e.g., `adapter.containing_scope(enrichment)` rather than inline dict drilling.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The object resolution pipeline in `objects/resolve.py` follows a "ask then decide" pattern: `_payload_views` extracts views, then `_resolution_key_payload` inspects them to determine quality level. This could be refactored to have the payload objects self-describe their resolution quality.

**Findings:**
- `objects/resolve.py:37-43`: `_PayloadViews` is a passive data holder; resolution quality logic is external.

**Suggested improvement:**
Add a `resolution_quality` computed property or factory method to `_PayloadViews` that encapsulates the quality assessment.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Pure transform functions exist (`compute_relevance_score`, `_build_summary`, `build_static_semantic_planes`). However, `partition_pipeline.py` interleaves pure match processing with cache I/O operations (cache probe, writeback, telemetry recording) throughout `run_search_partition`. The cache orchestration is tightly coupled with the search logic.

**Findings:**
- `partition_pipeline.py:90-100`: `run_search_partition` mixes cache context setup with search execution in a single flow.
- `enrichment/core.py:106-124`: `merge_gap_fill_payload` is a clean pure function -- good example.
- `semantic/models.py:236-290`: `build_static_semantic_planes` is a clean pure function.

**Suggested improvement:**
Separate `run_search_partition` into a pure phase-planning function that returns a partition plan, and an imperative executor that handles cache I/O. This would allow testing the partition logic without cache infrastructure.

**Effort:** medium
**Risk if unaddressed:** low -- the current design works but makes partition logic hard to test in isolation.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Search operations are read-only. Cache writes use content-hash-based keys, making them naturally idempotent. `file_content_hash` at `partition_pipeline.py:13` ensures cache keys are deterministic.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
`rg/runner.py` `build_rg_command` constructs deterministic argument lists. `_shared/core.py:45` uses `msgspec.json.Encoder(order="deterministic")`. `enrichment/core.py:288` uses `dict.fromkeys(degrade)` for ordered dedup. The pipeline produces reproducible results given the same inputs.

---

### Category: Simplicity

#### P19. KISS -- Alignment: 2/3

**Current state:**
The `_coerce_summary_inputs` function at `smart_search.py:1230-1274` adds substantial complexity to support a legacy calling convention alongside the typed `SearchSummaryInputs` path. The three thin wrapper modules add indirection without simplification.

**Findings:**
- `smart_search.py:1230-1274`: 44 lines of runtime type coercion to support two calling conventions.
- `smart_search_followups.py`: 26 lines total; the function body is a single import + delegation.

**Suggested improvement:**
Deprecate and remove the legacy `build_summary` calling convention. Audit callers to migrate to `SearchSummaryInputs`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
The `SearchLimits` at `_shared/types.py` has reasonable defaults. The `profiles.py` presets (DEFAULT, INTERACTIVE, AUDIT, CI, LITERAL) serve documented use cases. Minor concern: `_SUMMARY_ARGS_COUNT = 5` legacy support may be unused.

**Findings:**
- `smart_search.py:1239`: Legacy 5-argument path may have no remaining callers.

**Suggested improvement:**
Search for callers of `build_summary` with positional arguments. If none exist, remove the legacy path.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
`build_summary(*args: object, **kwargs: object)` at `smart_search.py:1215` has a signature that provides no type information to callers. A developer reading the API would have no idea what arguments are expected without reading the implementation. The `run_search_partition` return type is `object` (`partition_pipeline.py:95`), hiding the actual `LanguageSearchResult` type.

**Findings:**
- `smart_search.py:1215`: `build_summary(*args, **kwargs)` -- opaque signature.
- `partition_pipeline.py:95`: `run_search_partition(...) -> object` -- return type hides actual type.
- `semantic/models.py:293-318`: `call_with_retry` returns `tuple[object | None, bool]` -- the first element's type depends on the callback.

**Suggested improvement:**
Type `build_summary` with explicit parameters. Change `run_search_partition` return type to `LanguageSearchResult`. Parameterize `call_with_retry` with a TypeVar for the return type.

**Effort:** medium
**Risk if unaddressed:** medium -- developers will make incorrect assumptions about expected arguments and return types.

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
All modules have `__all__` lists. Contract types use version suffixes (`SearchPartitionPlanV1`, `SemanticOutcomeV1`, `SemanticOutcomeCacheV1`). However, `smart_search.py` exports 24 symbols including internal helpers like `pop_search_object_view_for_run` that belong to other modules.

**Findings:**
- `smart_search.py:2024-2045`: `__all__` re-exports `pop_search_object_view_for_run` which is imported from `search_object_view_store.py` -- leaking another module's API.
- `_shared/__init__.py`: Re-exports from `core.py` but the actual boundaries are unclear.

**Suggested improvement:**
Remove re-exports of symbols that belong to other modules. Each module should export only its own symbols.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
Testing `classify_match` or `_run_classification_phase` requires either the module-level caches to be populated or `clear_caches()` to be called between tests. `run_search_partition` depends on `get_cq_cache_backend()` service locator, making it impossible to test without disk cache infrastructure. The `*args/**kwargs` signature on `build_summary` makes it difficult to write type-safe test calls.

**Findings:**
- `classifier_runtime.py:65-70`: Six module-level mutable caches that persist across test cases.
- `classifier_runtime.py:428-435`: `clear_classifier_caches()` must be called explicitly in test teardown.
- `partition_pipeline.py:90-100`: `run_search_partition` creates cache backend internally -- no injection seam.
- `front_door.py:12`: `get_cq_cache_backend()` service locator prevents test substitution.

**Suggested improvement:**
Accept a `CqCacheBackend` parameter in `run_search_partition` (defaulting to `get_cq_cache_backend()` for backward compatibility). Bundle classifier caches into an injectable context object. This enables fast unit tests with in-memory substitutes.

**Effort:** medium
**Risk if unaddressed:** high -- test isolation failures lead to flaky tests and slow test suites requiring process-level isolation.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Enrichment telemetry is well-structured via `build_enrichment_telemetry` and the adapter-based accumulation pattern. `SearchStats` captures scan-level metrics (scanned files, matched files, timeouts, caps hit). However, the duplicated telemetry paths (adapter-based vs direct accumulation in `smart_search_telemetry.py`) could produce inconsistent metrics.

**Findings:**
- `smart_search_telemetry.py:290-321`: `build_enrichment_telemetry` uses the port-based path (clean).
- `smart_search_telemetry.py:133-164`: `accumulate_python_enrichment` is a separate non-port path that duplicates logic.
- `assembly.py:39`: `MAX_EVIDENCE = 100` cap applied silently -- no diagnostic emitted when evidence is truncated.

**Suggested improvement:**
Remove the non-port telemetry accumulation functions (`accumulate_python_enrichment`, `accumulate_rust_enrichment`) and route all telemetry through the adapter protocol. Add a diagnostic finding when `MAX_EVIDENCE` truncation occurs.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Incomplete decomposition of smart_search.py

The god module `smart_search.py` has undergone partial extraction: telemetry, types, followups, sections, and summary modules exist but contain thin wrappers that import back into the monolith. This creates the worst of both worlds -- more modules to navigate without actual separation of concerns. The root cause is that function implementations were not moved during extraction.

**Affected principles:** P2, P3, P4, P21, P23
**Suggested approach:** Complete the extraction by moving implementations into the wrapper modules, then reduce `smart_search.py` to a thin orchestrator that composes phases.

### Theme 2: Global mutable state as hidden coupling

Six classifier caches, service locator calls (`get_cq_cache_backend()`, `get_python_analysis_session()`), and deferred imports (`semantic/models.py:439`) create implicit dependencies that cannot be statically traced. This makes the system hard to test, hard to reason about lifecycle, and prone to state leakage.

**Affected principles:** P1, P12, P23
**Suggested approach:** Introduce a `SearchRuntimeContext` that bundles all runtime dependencies and thread it through the pipeline.

### Theme 3: Duplicated telemetry knowledge

Rust telemetry accumulation is implemented in three locations with near-identical logic. The `_counter`/`int_counter` helper appears twice. This creates a maintenance risk where a fix in one location is not applied to others.

**Affected principles:** P7, P24
**Suggested approach:** Consolidate all telemetry accumulation into the adapter protocol path (`build_enrichment_telemetry` -> `adapter.accumulate_telemetry()`), removing the direct accumulation functions.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P8 | Replace `Any` types in `_EnrichmentMissTask` and `_EnrichmentMissResult` with concrete types | small | Enables static type checking for cache payloads |
| 2 | P21 | Type `build_summary` with explicit parameters, deprecate `*args/**kwargs` | small | API is self-documenting, IDE support works |
| 3 | P7 | Remove duplicated `accumulate_rust_runtime`/`accumulate_rust_bundle` from `smart_search_telemetry.py` | small | Single source of truth for Rust telemetry |
| 4 | P22 | Remove re-exports of foreign symbols from `smart_search.py.__all__` | small | Clear module ownership boundaries |
| 5 | P21 | Change `run_search_partition` return type from `object` to `LanguageSearchResult` | small | Callers get type safety without casts |

## Recommended Action Sequence

1. **Replace `Any` types in `partition_pipeline.py`** (P8, P10). Change `_EnrichmentMissTask.items` to `list[tuple[int, RawMatch, str, str]]` and `_EnrichmentMissResult.raw`/`enriched_match` to `RawMatch`/`EnrichedMatch`. Fix `run_search_partition` return type to `LanguageSearchResult`. These are safe, local changes.

2. **Type and deprecate `build_summary`** (P8, P21). Create `build_summary_from_inputs(inputs: SearchSummaryInputs) -> dict[str, object]` with the clean signature. Mark the `*args/**kwargs` variant as deprecated. Migrate callers.

3. **Consolidate Rust telemetry** (P7, P24). Remove `accumulate_rust_runtime`, `accumulate_rust_bundle`, and `accumulate_rust_enrichment` from `smart_search_telemetry.py`. The port-based `build_enrichment_telemetry` already delegates to `RustEnrichmentAdapter.accumulate_telemetry`.

4. **Extract classifier cache context** (P1, P12, P23). Create `ClassifierCacheContext` dataclass owning the six caches. Pass it through `get_record_context`, `get_sg_root`, `get_node_index`, etc. This unblocks test isolation and bounded eviction.

5. **Complete smart_search.py decomposition** (P2, P3, P4). Move candidate collection, classification, enrichment, summary, and section implementations into their respective modules. Reduce `smart_search.py` to a ~200-line orchestrator. This depends on steps 2-4 being complete first to avoid merge conflicts.

6. **Split `_shared/core.py`** (P3, P4). Promote the re-export facades (`encoding.py`, `timeout.py`, `rg_request.py`) to owning modules by moving implementations from `core.py`. Reduce `core.py` to byte-offset helpers only.

7. **Introduce `SearchRuntimeContext`** (P12, P23). Bundle cache backend, analysis session, and classifier cache context into a single injectable object. Thread through `smart_search()` -> partition -> classify -> enrich. This is the capstone change that enables full test isolation.
