# Design Review: tools/cq/search/{pipeline, enrichment, _shared}

**Date:** 2026-02-17
**Scope:** `tools/cq/search/pipeline/` (30 files), `tools/cq/search/enrichment/` (13 files), `tools/cq/search/_shared/` (12 files)
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 55 (all files in scope, ~10,187 LOC)

## Executive Summary

The CQ search pipeline subsystem is architecturally well-decomposed into phased stages (candidate collection, classification, enrichment, assembly) with a clean language adapter protocol. However, the subsystem suffers from a pervasive `dict[str, object]` anti-pattern that undermines the typed fact structs (`python_facts.py`, `rust_facts.py`) that already exist -- 191 occurrences of untyped dict payloads across pipeline and enrichment modules. Telemetry accumulation logic is semantically duplicated across four modules with manual counter incrementing on untyped dicts. Module-level mutable singletons and a dependency inversion violation (enrichment adapters importing from pipeline contracts) are the two structural blockers that constrain testability and layer discipline.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | medium | Module-level mutable registries exposed; `__init__.py` re-exports internals as `Any` |
| 2 | Separation of concerns | 2 | medium | low | `partition_pipeline.py` mixes cache orchestration with classification dispatch |
| 3 | SRP | 2 | medium | low | `smart_search_telemetry.py` owns both telemetry schema and accumulation logic for two languages |
| 4 | High cohesion, low coupling | 2 | medium | medium | Enrichment adapters tightly coupled to pipeline-layer `enrichment_contracts.py` |
| 5 | Dependency direction | 1 | medium | high | Enrichment layer imports from pipeline layer (inward dependency violation) |
| 6 | Ports & Adapters | 2 | small | low | `LanguageEnrichmentPort` protocol is well-defined; adapter impls import from wrong layer |
| 7 | DRY | 0 | medium | high | Telemetry accumulation duplicated across 4 modules; `_coerce_count` duplicated verbatim |
| 8 | Design by contract | 2 | small | low | Frozen structs enforce invariants; `dict[str, object]` payloads bypass all contracts |
| 9 | Parse, don't validate | 1 | large | high | 191 `dict[str, object]` occurrences; typed facts exist but are not used at boundaries |
| 10 | Make illegal states | 1 | large | high | Enrichment wrappers contain `payload: dict[str, object]`; impossible states representable |
| 11 | CQS | 2 | small | low | `accumulate_*` functions mutate dicts in-place (commands); `build_*` are queries; mostly clean |
| 12 | DI + explicit composition | 2 | small | low | `LanguageEnrichmentPort` protocol with registry; lazy singleton pattern weakens DI |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; composition via protocols and adapters throughout |
| 14 | Law of Demeter | 1 | medium | medium | `getattr(match, "python_enrichment", None)` and deep `.get()` chains on untyped dicts |
| 15 | Tell, don't ask | 1 | medium | medium | Adapters extract raw data then re-implement logic externally; anemic enrichment payloads |
| 16 | Functional core, imperative shell | 2 | small | low | Classification is mostly pure; cache/IO concentrated in `partition_pipeline.py` |
| 17 | Idempotency | 3 | - | - | Cache key includes content hash; re-runs produce same results |
| 18 | Determinism | 3 | - | - | Deterministic classification pipeline; no random/time-dependent logic in core |
| 19 | KISS | 2 | small | low | Overall clean; `_thaw_summary_mappings` in `smart_search.py` is complex for what it does |
| 20 | YAGNI | 2 | small | low | `generate_followup_suggestions` compatibility alias adds no value |
| 21 | Least astonishment | 1 | small | medium | `SearchContext: Any = None` re-maps to `SearchConfig`; `_shared/timeout.py` is trivial re-export |
| 22 | Declare/version contracts | 2 | small | low | `__all__` everywhere; versioned cache keys; but `__all__` in `smart_search.py` exports private `_run_candidate_phase` |
| 23 | Design for testability | 2 | medium | medium | Pure classification functions testable; module-level singletons and inline imports hinder isolation |
| 24 | Observability | 2 | medium | low | Rich telemetry structure but all on untyped dicts; no structured logging in pipeline |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
Module internals are exposed through several mechanisms that allow callers to depend on implementation details.

**Findings:**
- `tools/cq/search/pipeline/search_object_view_store.py:8`: Module-level mutable dict `_SEARCH_OBJECT_VIEW_REGISTRY` is the sole mechanism for inter-run view passing. Any module can import and mutate this dict directly. No encapsulation boundary.
- `tools/cq/search/enrichment/language_registry.py:8`: Module-level mutable dict `_LANGUAGE_ADAPTERS` with lazy initialization via `_ensure_defaults()` at line 16. The registry is globally mutable with no protection against concurrent modification.
- `tools/cq/search/pipeline/__init__.py:7-10`: Module-level `Any = None` sentinel assignments (`SearchContext`, `SearchPipeline`, `SearchResultAssembly`, `assemble_result`) expose untyped names that resolve to different types via `__getattr__` at line 20. Callers see `Any` types, losing all type safety.
- `tools/cq/search/pipeline/smart_search.py:250-268`: `__all__` exports the private function `_run_candidate_phase` (line 257), leaking an implementation detail into the public surface.

**Suggested improvement:**
Replace module-level mutable dicts with proper registry classes that control access. For `search_object_view_store.py`, wrap the dict in a class with `register()` and `pop()` methods and expose only those methods. For `language_registry.py`, the same pattern with thread-safe lazy initialization. For `__init__.py`, replace `Any = None` sentinels with proper lazy-import stubs that preserve type information, or remove the facade entirely and have callers import from specific submodules.

**Effort:** medium
**Risk if unaddressed:** medium -- global mutable state creates hidden coupling and makes concurrent use unsafe.

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
The pipeline is generally well-decomposed into phases (candidate, classify, enrich, assemble), but some modules mix concerns.

**Findings:**
- `tools/cq/search/pipeline/partition_pipeline.py:96-129` (`run_search_partition`): This function orchestrates scope context construction, candidate collection, and enrichment in a single flow. The enrichment phase (line 275) mixes cache probe/persist logic with classification dispatch and parallel worker management, spanning ~245 lines.
- `tools/cq/search/pipeline/partition_pipeline.py:374-413` (`_compute_and_persist_enrichment_misses`): Cache persistence logic (transact, encode, record telemetry) is interleaved with classification result collection. The `transact()` context manager at line 390 couples cache infrastructure to enrichment logic.
- `tools/cq/search/pipeline/smart_search_summary.py:207-299` (`build_search_summary`): This function builds summary payload, computes cross-language diagnostics, runs query pack linting, builds enrichment telemetry, and assembles semantic diagnostics -- five distinct concerns in one function.

**Suggested improvement:**
Extract cache orchestration from `partition_pipeline.py` into a dedicated `enrichment_cache.py` module that handles probe/persist/transact, leaving `partition_pipeline.py` as a pure phase coordinator. Split `build_search_summary` into `build_base_summary`, `build_diagnostics_payload`, and `build_telemetry_payload` functions that are composed at the call site.

**Effort:** medium
**Risk if unaddressed:** low -- the current structure works but makes the enrichment cache logic harder to test in isolation.

---

#### P3. SRP (one reason to change) -- Alignment: 2/3

**Current state:**
Most modules have clear single responsibilities. A few modules serve as "dumping grounds" for related but distinct responsibilities.

**Findings:**
- `tools/cq/search/pipeline/smart_search_telemetry.py`: Changes for three distinct reasons: (1) the telemetry schema shape (`empty_enrichment_telemetry` at line 44), (2) Python accumulation logic (lines 141-173), and (3) Rust accumulation logic (lines 197-295). The Python and Rust accumulation functions are structurally parallel but independently maintained.
- `tools/cq/search/enrichment/core.py`: Changes for four reasons: (1) payload normalization (`normalize_python_payload` at line 342, `normalize_rust_payload` at line 371), (2) merge policy (`merge_gap_fill_payload` at line 156), (3) budget enforcement (`enforce_payload_budget` at line 177), and (4) field partitioning (`_partition_python_payload_fields` at line 266).
- `tools/cq/search/_shared/search_contracts.py` (~542 LOC): Contains 10+ distinct contract types spanning language stats, capabilities, diagnostics, telemetry, and semantic overview. This changes whenever any of these concern areas evolves.

**Suggested improvement:**
Split `smart_search_telemetry.py` into `telemetry_schema.py` (shape definitions) and language-specific accumulation that lives with the respective adapters. Split `core.py` into `payload_normalization.py`, `payload_merge.py`, and `payload_budget.py`. Consider splitting `search_contracts.py` by concern domain.

**Effort:** medium
**Risk if unaddressed:** low -- the modules are readable today but will accumulate complexity as new enrichment planes are added.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Cohesion within individual modules is generally good. Coupling between the enrichment and pipeline layers is tighter than the architecture suggests.

**Findings:**
- `tools/cq/search/enrichment/python_adapter.py:14-19`: Imports `IncrementalEnrichmentV1`, `PythonEnrichmentV1`, `incremental_enrichment_payload`, `python_enrichment_payload` from `tools.cq.search.pipeline.enrichment_contracts`. The enrichment adapter layer depends on pipeline-layer contract definitions.
- `tools/cq/search/enrichment/rust_adapter.py:14-17`: Same pattern -- imports `RustTreeSitterEnrichmentV1` and `rust_enrichment_payload` from the pipeline layer.
- `tools/cq/search/pipeline/classification.py:44-52`: Imports 8 symbols from `pipeline.enrichment_contracts` for wrapping/unwrapping. The classification module is coupled to the enrichment contract wire format.
- `tools/cq/search/pipeline/smart_search_sections.py:21-25`: Imports 3 enrichment payload extractors from `enrichment_contracts` for section rendering. Section building is coupled to enrichment wire format.

**Suggested improvement:**
Move `enrichment_contracts.py` from `pipeline/` to `enrichment/` (or to `_shared/`) since it defines enrichment-layer concerns. This eliminates the dependency inversion where enrichment imports from pipeline. Both pipeline and enrichment would then depend on a shared contract layer.

**Effort:** medium
**Risk if unaddressed:** medium -- the current coupling means changes to enrichment wire format require coordinated changes across both layers.

---

#### P5. Dependency direction -- Alignment: 1/3

**Current state:**
The intended layer ordering is `_shared` (foundation) -> `enrichment` (domain) -> `pipeline` (orchestration). This ordering is violated in the enrichment layer.

**Findings:**
- `tools/cq/search/enrichment/python_adapter.py:14`: `from tools.cq.search.pipeline.enrichment_contracts import (...)` -- lower layer (enrichment) depends on higher layer (pipeline).
- `tools/cq/search/enrichment/rust_adapter.py:14`: Same violation -- `from tools.cq.search.pipeline.enrichment_contracts import (...)`.
- `tools/cq/search/pipeline/classification.py:21-22`: Pipeline correctly depends on enrichment (`from tools.cq.search.enrichment.core import normalize_python_payload, normalize_rust_payload`). This is the correct direction.
- `tools/cq/search/pipeline/classification.py:22-25`: Pipeline also correctly depends on `enrichment.incremental_provider`. This confirms the intended direction is pipeline -> enrichment.

The violation creates a circular conceptual dependency: pipeline defines contracts that enrichment consumes, but pipeline also consumes enrichment logic.

**Suggested improvement:**
Relocate `enrichment_contracts.py` from `tools/cq/search/pipeline/` to `tools/cq/search/_shared/` or `tools/cq/search/enrichment/`. The wrapper structs (`RustTreeSitterEnrichmentV1`, `PythonEnrichmentV1`, `IncrementalEnrichmentV1`) are enrichment-domain concepts, not pipeline orchestration concepts. Both layers would then depend downward on `_shared`.

**Effort:** medium
**Risk if unaddressed:** high -- the inversion makes the enrichment layer non-independent and prevents isolated testing or reuse of enrichment adapters outside the pipeline context.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The `LanguageEnrichmentPort` protocol at `enrichment/contracts.py:42` is a well-defined port. `PythonEnrichmentAdapter` and `RustEnrichmentAdapter` are clean adapter implementations.

**Findings:**
- `tools/cq/search/enrichment/contracts.py:42-61`: `LanguageEnrichmentPort` Protocol with three methods (`payload_from_match`, `accumulate_telemetry`, `build_diagnostics`). All operate on `dict[str, object]` rather than typed structs, which means the port itself does not enforce type safety.
- `tools/cq/search/enrichment/language_registry.py:11-13`: `register_language_adapter` function provides the adapter registration mechanism. Clean and simple.
- `tools/cq/search/pipeline/smart_search_telemetry.py:315`: `adapter = get_language_adapter(match.language)` -- the pipeline consumes adapters through the port, which is the correct hexagonal pattern.

**Suggested improvement:**
Evolve `LanguageEnrichmentPort` methods to accept and return typed fact structs (`PythonEnrichmentFacts`, `RustEnrichmentFacts`) instead of `dict[str, object]`. This would make the port enforce type contracts at the boundary.

**Effort:** small
**Risk if unaddressed:** low -- the port pattern is correctly applied; the dict-based signatures are the only weakness.

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 0/3

**Current state:**
Telemetry accumulation logic is semantically duplicated across four modules. The same counter-incrementing pattern on untyped dicts is reimplemented independently in each location.

**Findings:**
- **Runtime flag accumulation (3 copies):**
  - `tools/cq/search/enrichment/core.py:66-80` (`accumulate_runtime_flags`): Canonical implementation.
  - `tools/cq/search/pipeline/smart_search_telemetry.py:164-172`: Inline duplication within `accumulate_python_enrichment` -- same `did_exceed_match_limit` / `cancelled` counter logic.
  - `tools/cq/search/pipeline/smart_search_telemetry.py:226-252` (`accumulate_rust_runtime`): Parameterized variant with `counter` callback, but same semantic logic.

- **Stage status accumulation (3 copies):**
  - `tools/cq/search/pipeline/smart_search_telemetry.py:101-118` (`accumulate_stage_status`): Module-level function.
  - `tools/cq/search/enrichment/python_adapter.py:93-105` (`_accumulate_stage_status`): Private function with identical logic.
  - Both iterate stage_status dict, check for `{"applied", "degraded", "skipped"}`, increment counters.

- **Stage timings accumulation (3 copies):**
  - `tools/cq/search/pipeline/smart_search_telemetry.py:121-138` (`accumulate_stage_timings`): Module-level function.
  - `tools/cq/search/enrichment/python_adapter.py:108-117` (`_accumulate_stage_timings`): Private function with identical logic.
  - `tools/cq/search/enrichment/rust_adapter.py:119-138` (`_accumulate_stage_timings`): Rust variant with slightly different dict traversal but same semantic operation.

- **Bundle drift accumulation (2 copies):**
  - `tools/cq/search/pipeline/smart_search_telemetry.py:255-295` (`accumulate_rust_bundle`): Module-level function.
  - `tools/cq/search/enrichment/rust_adapter.py:93-116` (`_accumulate_bundle_drift`): Private function with identical logic.

- **`_coerce_count` (2 verbatim copies):**
  - `tools/cq/search/enrichment/incremental_compound_plane.py:8-16`
  - `tools/cq/search/enrichment/incremental_dis_plane.py:23-30`
  - Identical function body in both files.

- **`int_counter` / `_counter` (2 copies):**
  - `tools/cq/search/pipeline/smart_search_telemetry.py:211-217`: `int_counter` local function inside `accumulate_rust_enrichment`.
  - `tools/cq/search/enrichment/rust_adapter.py:89-90`: `_counter` module-level function. Same logic (int coercion excluding bool).

**Suggested improvement:**
Consolidate all accumulation helpers into `enrichment/core.py` or a new `enrichment/telemetry.py` module. The adapters and `smart_search_telemetry.py` should delegate to these shared implementations. Move `_coerce_count` to `_shared/helpers.py`. Move `int_counter`/`_counter` to `_shared/helpers.py` as `safe_int_counter`.

**Effort:** medium
**Risk if unaddressed:** high -- the accumulation logic has already drifted (e.g., `accumulate_rust_runtime` accepts a `counter` callback while `accumulate_runtime_flags` does inline `int()` coercion). Further drift will cause subtle telemetry inconsistencies between the adapter and direct-call paths.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Frozen msgspec structs enforce structural invariants well. However, the `dict[str, object]` payloads that flow through the system bypass all contract enforcement.

**Findings:**
- `tools/cq/search/_shared/types.py:22-51`: `SearchLimits` uses `PositiveInt` and `PositiveFloat` constrained types -- good contract enforcement at construction time.
- `tools/cq/search/pipeline/contracts.py:18-98`: `SearchPartitionPlanV1`, `SearchConfig`, `SearchRequest` are all frozen CqStruct with typed fields. Strong contracts.
- `tools/cq/search/pipeline/enrichment_contracts.py:25,37,50`: `payload: dict[str, object]` fields on all three enrichment wrapper structs. The contract wraps an untyped blob -- it guarantees the envelope shape but not the contents.
- `tools/cq/search/enrichment/core.py:66-80` (`accumulate_runtime_flags`): All parameters are `dict[str, object]`. No postcondition on what keys are modified or what values are set. The caller must know the expected dict shape.

**Suggested improvement:**
Define typed `EnrichmentTelemetryBucket` and `RuntimeFlagsBucket` structs to replace the `dict[str, object]` parameters in accumulation functions. This converts runtime key-lookup failures into construction-time type errors.

**Effort:** small
**Risk if unaddressed:** low -- the current code works because the dict shapes are de facto stable, but there is no compile-time or construction-time enforcement.

---

#### P9. Parse, don't validate -- Alignment: 1/3

**Current state:**
Typed fact structs exist in `python_facts.py` and `rust_facts.py` but are not used at the enrichment pipeline boundaries. Data enters as `dict[str, object]`, flows through the pipeline as `dict[str, object]`, and is only optionally parsed into typed facts at the very end (in `enrichment/core.py:107-132`).

**Findings:**
- `tools/cq/search/enrichment/python_facts.py:18-161`: Nine well-typed frozen fact structs: `PythonResolutionFacts`, `PythonBehaviorFacts`, `PythonStructureFacts`, `PythonSignatureFacts`, `PythonCallFacts`, `PythonImportFacts`, `PythonClassShapeFacts`, `PythonLocalsFacts`, `PythonParseQualityFacts`, composed into `PythonEnrichmentFacts`.
- `tools/cq/search/enrichment/rust_facts.py`: Four typed fact structs: `RustStructureFacts`, `RustDefinitionFacts`, `RustMacroExpansionFacts`, composed into `RustEnrichmentFacts`.
- `tools/cq/search/pipeline/enrichment_contracts.py:25`: `RustTreeSitterEnrichmentV1` wraps `payload: dict[str, object]` -- the typed facts are not used.
- `tools/cq/search/pipeline/enrichment_contracts.py:37`: `PythonEnrichmentV1` wraps `payload: dict[str, object]` -- same issue.
- `tools/cq/search/pipeline/enrichment_contracts.py:50`: `IncrementalEnrichmentV1` wraps `payload: dict[str, object]`.
- `tools/cq/search/pipeline/enrichment_contracts.py:123-156`: The `*_enrichment_payload()` extractor functions return `dict[str, object]`, immediately discarding the wrapper's type safety.
- `tools/cq/search/enrichment/core.py:107-118` (`parse_python_enrichment`): This function exists to parse `dict[str, object]` into `PythonEnrichmentFacts`, but it is called optionally and only for read-side operations. The write side never uses it.
- `tools/cq/search/enrichment/core.py:262-295` (`_partition_python_payload_fields`): Manually partitions a flat dict into resolution/behavior/structural/parse_quality/agreement buckets using frozenset key membership checks. This is effectively re-implementing what the typed fact structs would do automatically.

**Suggested improvement:**
Replace `payload: dict[str, object]` in `RustTreeSitterEnrichmentV1`, `PythonEnrichmentV1`, and `IncrementalEnrichmentV1` with the typed fact structs (`RustEnrichmentFacts`, `PythonEnrichmentFacts`, etc.). Parse enrichment outputs into typed facts at the enrichment boundary (in `classification.py` where enrichments are first attached to matches), and flow typed facts through the rest of the pipeline. This eliminates the 191 `dict[str, object]` occurrences and the manual field partitioning in `core.py`.

**Effort:** large
**Risk if unaddressed:** high -- the untyped payloads are the single largest source of fragility in this subsystem. Key typos, missing fields, and type mismatches are only caught at runtime (if at all). The manual partitioning in `_partition_python_payload_fields` will silently mis-route new fields.

---

#### P10. Make illegal states unrepresentable -- Alignment: 1/3

**Current state:**
The typed fact structs model legal states well. The wrapper structs and the broader pipeline do not.

**Findings:**
- `tools/cq/search/enrichment/python_facts.py:11`: `ScopeKind = Literal["module", "class", "function", "closure"]` -- good use of Literal to constrain values.
- `tools/cq/search/enrichment/python_facts.py:15`: `ClassKind = Literal["dataclass", "pydantic", "protocol", "abc", "enum", "regular"]` -- good.
- `tools/cq/search/pipeline/enrichment_contracts.py:25`: `payload: dict[str, object]` can represent any shape, including shapes that violate enrichment invariants. A `PythonEnrichmentV1` with `payload={"not_a_real_field": 42}` is a legal value.
- `tools/cq/search/pipeline/smart_search_types.py:269-281` (`_SearchSemanticOutcome`): Mutable dataclass with `payload: dict[str, object] | None`. This is an internal type that can hold any arbitrary state.
- `tools/cq/search/pipeline/smart_search_telemetry.py:44-98` (`empty_enrichment_telemetry`): Returns a deeply nested `dict[str, object]` with a complex schema that is only documented by the structure of the returned dict literal. Nothing prevents callers from inserting invalid keys or values.

**Suggested improvement:**
Define `EnrichmentTelemetrySchema` as a frozen msgspec struct with nested `PythonTelemetryBucket` and `RustTelemetryBucket` structs. Replace the `empty_enrichment_telemetry()` dict factory with struct construction. For enrichment wrappers, adopt the typed facts as described in P9.

**Effort:** large
**Risk if unaddressed:** high -- the dict-based telemetry schema is a live source of bugs whenever new stages are added (the nested dict shape must be manually kept in sync with accumulation functions).

---

#### P11. CQS (Command-Query Separation) -- Alignment: 2/3

**Current state:**
Most functions follow CQS. Accumulation functions are explicitly commands (mutate state, return None). Build functions are queries (return values, no mutation).

**Findings:**
- `tools/cq/search/enrichment/core.py:135-143` (`append_source`): Command -- mutates `payload` dict in place, returns None. Clean.
- `tools/cq/search/enrichment/core.py:146-153` (`set_degraded`): Command -- mutates `payload` dict in place, returns None. Clean.
- `tools/cq/search/enrichment/core.py:177-201` (`enforce_payload_budget`): Mixed -- mutates `payload` via `pop()` (command) AND returns `(dropped, size)` (query). This is a CQS violation, though the return value reports what the command did.
- `tools/cq/search/pipeline/search_object_view_store.py:30-45` (`pop_search_object_view_for_run`): Mixed -- removes entry from registry (command) AND returns the value (query). This is a standard pop-and-return pattern that is acceptable as an atomic operation.

**Suggested improvement:**
For `enforce_payload_budget`, consider separating into `compute_drop_plan(payload, budget) -> list[str]` (query) and `apply_drop_plan(payload, keys) -> None` (command). The current combined form is pragmatic but makes testing harder.

**Effort:** small
**Risk if unaddressed:** low -- the violations are minor and follow common idiomatic patterns.

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
The `LanguageEnrichmentPort` protocol provides proper dependency inversion for language adapters. However, the composition relies on module-level lazy singletons rather than explicit wiring.

**Findings:**
- `tools/cq/search/enrichment/contracts.py:42`: `LanguageEnrichmentPort` Protocol -- clean abstraction boundary.
- `tools/cq/search/enrichment/language_registry.py:16-23` (`_ensure_defaults`): Lazy initialization that silently creates adapter instances on first access. The composition is implicit -- there is no explicit "wire up the adapters" step.
- `tools/cq/search/pipeline/partition_pipeline.py:138` (`_scope_context`): `get_cq_cache_backend(root=resolved_root)` -- cache backend is obtained via a global factory rather than injected. This couples the partition pipeline to a specific cache backend resolution strategy.

**Suggested improvement:**
Accept `CqCacheBackend` as a parameter to `run_search_partition` or via `SearchConfig`, rather than calling `get_cq_cache_backend` internally. For the language registry, consider making adapter registration explicit at pipeline startup rather than relying on lazy initialization.

**Effort:** small
**Risk if unaddressed:** low -- the current lazy initialization works correctly but makes it harder to substitute test doubles.

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No class inheritance hierarchies exist in the reviewed scope. All extension is via protocol implementation and composition.

**Findings:**
- `PythonEnrichmentAdapter` and `RustEnrichmentAdapter` implement `LanguageEnrichmentPort` via structural typing (Protocol), not inheritance.
- `SearchPipeline` at `orchestration.py:68` is a simple dataclass that composes `SearchConfig` -- no inheritance.
- All fact structs extend `CqStruct` which is a msgspec.Struct base -- this is framework-required, not design inheritance.

No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
Deep navigation into untyped dict payloads is pervasive. Adapters use `getattr` to probe match objects rather than receiving typed parameters.

**Findings:**
- `tools/cq/search/enrichment/python_adapter.py:36-37`: `getattr(match, "python_enrichment", None)` and `getattr(match, "incremental_enrichment", None)` -- the adapter reaches into match attributes dynamically rather than receiving typed enrichment data.
- `tools/cq/search/enrichment/rust_adapter.py:34`: `getattr(match, "rust_tree_sitter", None)` -- same pattern.
- `tools/cq/search/pipeline/smart_search_telemetry.py:33-38` (`status_from_enrichment`): `payload.get("meta")` then `meta.get("enrichment_status")` then `payload.get("enrichment_status")` -- three levels of dict navigation with fallback, accessing a collaborator's collaborator.
- `tools/cq/search/enrichment/core.py:72-80` (`accumulate_runtime_flags`): `lang_bucket.get("query_runtime")` then `runtime_bucket.get("did_exceed_match_limit")` -- deep dict traversal.
- `tools/cq/search/enrichment/rust_adapter.py:93-116` (`_accumulate_bundle_drift`): `bundle.get("drift_schema_diff")` then `schema_diff.get("removed_node_kinds")` -- three levels deep into untyped dicts.
- `tools/cq/search/pipeline/smart_search_summary.py:50-51` (`build_language_summary`): Direct access to `stat.scanned_files`, `stat.scanned_files_is_estimate`, etc. -- this is accessing typed struct fields, which is fine, but the function builds a dict by extracting every field individually rather than delegating to the stat object.

**Suggested improvement:**
For adapter `payload_from_match`, change the `LanguageEnrichmentPort.payload_from_match` signature to accept typed enrichment fields directly instead of an opaque `object`. For dict navigation, typed structs (as suggested in P9/P10) would eliminate the deep `.get()` chains entirely.

**Effort:** medium
**Risk if unaddressed:** medium -- the deep dict navigation is fragile and produces silent `None` propagation when keys are misspelled or renamed.

---

#### P15. Tell, don't ask -- Alignment: 1/3

**Current state:**
Enrichment payloads are "anemic" dicts that expose raw data. External functions re-implement logic on top of them rather than the payloads encapsulating their own behavior.

**Findings:**
- `tools/cq/search/enrichment/core.py:266-295` (`_partition_python_payload_fields`): Asks a flat dict for all its keys, then re-implements partitioning logic externally based on frozenset membership. The fact structs (`PythonResolutionFacts`, `PythonBehaviorFacts`, etc.) already define this partitioning, but the code manually rebuilds it.
- `tools/cq/search/pipeline/smart_search_telemetry.py:141-173` (`accumulate_python_enrichment`): Asks the payload for `"meta"`, then asks meta for `"stage_status"`, then asks the lang_bucket for `"stages"`, then manually increments counters. The telemetry bucket should know how to accumulate a payload.
- `tools/cq/search/pipeline/smart_search_sections.py:77-94` (`_merge_enrichment_payloads`): Asks the match for `rust_tree_sitter`, `python_enrichment`, `incremental_enrichment`, then externally constructs the merged payload. The match object should provide a method to produce its enrichment summary.
- `tools/cq/search/enrichment/core.py:298-312` (`_derive_behavior_flags`): Asks behavior dict for `"awaits"`, `"yields"`, `"raises_exception"` and then sets derived flags externally. This derivation logic should live with the behavior data structure.

**Suggested improvement:**
Add `to_enrichment_summary()` method to `EnrichedMatch` (or a standalone function) that produces the merged enrichment payload. Add `accumulate(payload)` method to a typed telemetry bucket struct. Move `_derive_behavior_flags` into `PythonBehaviorFacts` as a classmethod or post-init hook.

**Effort:** medium
**Risk if unaddressed:** medium -- the ask-then-process pattern leads to duplicated conditional logic across multiple call sites, which is already happening with the accumulation functions.

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Classification logic is mostly pure. IO and caching are concentrated in `partition_pipeline.py`. Assembly in `smart_search.py` orchestrates the imperative shell.

**Findings:**
- `tools/cq/search/pipeline/classifier.py:274-350` (`classify_heuristic`): Pure function taking match data and returning classification. Good.
- `tools/cq/search/pipeline/classifier.py:355-425` (`classify_from_node`): Takes node context and returns classification. Good.
- `tools/cq/search/pipeline/partition_pipeline.py:186-249` (`_candidate_phase`): Mixed -- interleaves cache reads/writes with pure candidate collection. The pure `run_candidate_phase` call at line 271 is wrapped in cache orchestration.
- `tools/cq/search/pipeline/smart_search.py:181-247` (`smart_search`): Imperative shell that orchestrates the full pipeline. Correctly delegates to pure functions for building context, sections, and summaries.
- `tools/cq/search/pipeline/smart_search_telemetry.py:175-194` (`attach_enrichment_cache_stats`): Inline imports from runtime modules at lines 185 and 190-191. This mixes IO (accessing runtime cache state) with telemetry building.

**Suggested improvement:**
In `_candidate_phase`, separate the cache read, the pure candidate collection, and the cache write into three sequential steps rather than interleaving them. This would make the pure core testable without cache infrastructure.

**Effort:** small
**Risk if unaddressed:** low -- the current structure is functional but could be cleaner.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
The pipeline is naturally idempotent. Cache keys include content hashes and scope hashes, so re-running with the same inputs produces the same cache interactions.

**Findings:**
- `tools/cq/search/pipeline/partition_pipeline.py:329-345`: Cache keys include `file_content_hash` (line 341), `match_text`, `match_start`, `match_end`, and `incremental_enrichment_mode`. Re-enriching the same match produces the same cache key and result.
- `tools/cq/search/pipeline/partition_pipeline.py:390`: `context.cache.transact()` ensures atomic cache writes -- partial failures don't leave inconsistent state.

No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
The classification pipeline is deterministic. The classifier uses a fixed priority chain (heuristic -> node -> record) with deterministic tie-breaking. No random or time-dependent logic in the core path.

**Findings:**
- `tools/cq/search/pipeline/classifier.py:95-131`: `NODE_KIND_MAP` is a static dict mapping node kinds to categories. Deterministic.
- `tools/cq/search/pipeline/partition_pipeline.py:486`: `rows.sort(key=lambda item: item.idx)` -- parallel worker results are sorted by original index to ensure deterministic ordering regardless of worker completion order.
- `tools/cq/search/pipeline/smart_search.py:49-56`: `SMART_SEARCH_LIMITS` is derived from `INTERACTIVE` profile at module load time -- deterministic for a given profile.

No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The overall pipeline design is clean and understandable. A few areas introduce unnecessary complexity.

**Findings:**
- `tools/cq/search/pipeline/smart_search.py:60-84` (`_thaw_summary_mappings`): Uses `getattr(summary, "__struct_fields__", ())` introspection to detect msgspec structs and recursively thaw frozen mapping values. This is a workaround for a serialization edge case that adds cognitive overhead.
- `tools/cq/search/pipeline/enrichment_contracts.py:57-80` (`_convert_contract`): Generic contract converter using msgspec.convert with type parameter. The complexity is justified by the three wrapper types it serves.
- `tools/cq/search/pipeline/partition_pipeline.py:416-487` (`_classify_enrichment_misses`): Three code paths for miss classification: single-item, sequential batch, and parallel batch. The sequential fallback on parallel failure (lines 461-485) is defensive but adds code surface.

**Suggested improvement:**
Move `_thaw_summary_mappings` to a utility module and document the specific edge case it addresses. The parallel/sequential fallback in `_classify_enrichment_misses` is appropriate defensive coding but could be extracted to a reusable `run_with_sequential_fallback` helper.

**Effort:** small
**Risk if unaddressed:** low -- the complexity is manageable but adds cognitive load for new contributors.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
The codebase is generally lean. A few compatibility aliases and re-export modules exist without clear justification.

**Findings:**
- `tools/cq/search/pipeline/smart_search_followups.py:81-93` (`generate_followup_suggestions`): Compatibility alias that simply calls `build_followups`. No evidence of external callers using the old name.
- `tools/cq/search/_shared/timeout.py` (~10 LOC): Re-exports `search_sync_with_timeout` and `search_async_with_timeout` from `_shared/timeouts.py`. A trivial forwarding module.
- `tools/cq/search/_shared/rg_request.py` (~7 LOC): Re-exports from `_shared/requests.py`. Another trivial forwarding module.
- `tools/cq/search/_shared/encoding.py` (~23 LOC): Re-exports from `_shared/helpers.py`. Third trivial forwarding module.
- `tools/cq/search/pipeline/__init__.py:7-10`: `SearchContext: Any = None` maps to `SearchConfig` via lazy import. This alias presumably exists for backward compatibility but provides no type information.

**Suggested improvement:**
Remove the compatibility alias `generate_followup_suggestions` if no external callers exist. Remove the trivial re-export modules (`timeout.py`, `rg_request.py`, `encoding.py`) and update import sites to reference the canonical locations. Remove the `SearchContext` alias in `__init__.py` or add a deprecation warning.

**Effort:** small
**Risk if unaddressed:** low -- the re-export modules add noise but are not harmful.

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
Several naming and behavioral choices violate the principle of least astonishment.

**Findings:**
- `tools/cq/search/pipeline/__init__.py:7,30-33`: `SearchContext: Any = None` resolves to `SearchConfig` via `__getattr__`. A competent reader importing `SearchContext` from this package would not expect to receive a `SearchConfig` class. The name mismatch is confusing.
- `tools/cq/search/pipeline/enrichment_contracts.py:123-156`: Functions named `rust_enrichment_payload()`, `python_enrichment_payload()`, `incremental_enrichment_payload()` accept a *wrapper struct* and return a *dict*. The name suggests they return an "enrichment payload" object, but they actually destructure a typed wrapper into an untyped dict. A reader would expect the reverse direction.
- `tools/cq/search/enrichment/python_adapter.py:35`: `_ = self` -- the adapter methods use `_ = self` to silence unused-self warnings, suggesting these could be module-level functions rather than methods on a class. The class exists solely to satisfy the Protocol, but the `_ = self` pattern is surprising.
- `tools/cq/search/pipeline/smart_search.py:257`: `__all__` exports `"_run_candidate_phase"` -- a function prefixed with underscore (private by convention) exported in the public surface. This sends contradictory signals.

**Suggested improvement:**
Rename `SearchContext` alias to `SearchConfig` or remove the alias entirely. Rename the `*_enrichment_payload()` functions to `unwrap_*_enrichment()` or `extract_*_payload()` to clarify the direction. Remove `_run_candidate_phase` from `__all__`. For the `_ = self` pattern, consider making adapter methods `@staticmethod` if the Protocol allows it, or document why instance methods are required.

**Effort:** small
**Risk if unaddressed:** medium -- the `SearchContext` -> `SearchConfig` mismatch and the private-exported function create ongoing confusion for contributors.

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
All modules have `__all__` lists. Cache keys include version strings. The enrichment wrappers include `schema_version` fields.

**Findings:**
- `tools/cq/search/pipeline/enrichment_contracts.py:24`: `schema_version: int = 1` on `RustTreeSitterEnrichmentV1`. Versioned contract.
- `tools/cq/search/pipeline/partition_pipeline.py:196-209`: Cache keys include `version="v2"`. Contract versioning for cache compatibility.
- `tools/cq/search/pipeline/smart_search.py:257`: `_run_candidate_phase` in `__all__` blurs the private/public boundary.
- `tools/cq/search/pipeline/smart_search.py:250-268`: `__all__` exports `run_classify_phase` which is imported from `classify_phase.py` and re-exported. The canonical location is ambiguous.

**Suggested improvement:**
Remove private functions from `__all__`. Establish a convention that symbols should only appear in the `__all__` of their defining module, not in re-export facades.

**Effort:** small
**Risk if unaddressed:** low -- the current state is mostly correct.

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Pure classification functions are highly testable. Module-level singletons and inline imports reduce testability of the enrichment and assembly layers.

**Findings:**
- `tools/cq/search/pipeline/classifier.py:274` (`classify_heuristic`): Pure function -- accepts data, returns result. Highly testable.
- `tools/cq/search/pipeline/classifier.py:355` (`classify_from_node`): Pure function. Highly testable.
- `tools/cq/search/pipeline/partition_pipeline.py:138` (`_scope_context`): Calls `get_cq_cache_backend(root=resolved_root)` -- global factory. Cannot substitute test cache without monkeypatching.
- `tools/cq/search/pipeline/neighborhood_preview.py:38-39` (`_build_structural_neighborhood_preview`): Inline imports from `tools.cq.core.front_door_assembly` and `tools.cq.neighborhood.contracts` inside the function body. These deferred imports make it harder to see dependencies and to mock them in tests.
- `tools/cq/search/pipeline/smart_search_telemetry.py:185,190-191` (`attach_enrichment_cache_stats`): Inline imports from `tree_sitter.rust_lane.runtime` and `tree_sitter.python_lane.runtime`. Same issue.
- `tools/cq/search/enrichment/language_registry.py:16-23`: `_ensure_defaults()` lazy initialization with inline imports makes it impossible to test adapter lookup without the real Python and Rust adapter implementations.
- `tools/cq/search/pipeline/search_object_view_store.py:8`: Module-level mutable dict -- test isolation requires manual cleanup between tests.

**Suggested improvement:**
Accept `CqCacheBackend` as a parameter to `run_search_partition`. Move inline imports to module-level `TYPE_CHECKING` blocks where possible, or accept the imported types as parameters. For `language_registry.py`, provide a `register_defaults()` function that tests can skip when providing their own adapters.

**Effort:** medium
**Risk if unaddressed:** medium -- the inline imports and global state make it progressively harder to write isolated unit tests as the module grows.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
The enrichment telemetry system provides rich per-stage, per-language observability. However, the telemetry is entirely dict-based with no structured logging integration.

**Findings:**
- `tools/cq/search/pipeline/smart_search_telemetry.py:44-98` (`empty_enrichment_telemetry`): Rich telemetry structure with per-stage status counters, timing buckets, and runtime flags. Good observability coverage.
- `tools/cq/search/pipeline/smart_search_telemetry.py:298-329` (`build_enrichment_telemetry`): Aggregates per-match telemetry via the adapter pattern. Good.
- `tools/cq/search/pipeline/smart_search.py:233-240`: Search stage timings (`partition`, `assemble`, `total`) are recorded in the result summary. Good.
- No structured logging (e.g., `logger.info`, `structlog`, OpenTelemetry spans) in any of the 55 reviewed files. All observability is via the return-value telemetry dicts. There are no log statements for debugging classification decisions, enrichment failures, or cache behavior.
- `tools/cq/search/pipeline/partition_pipeline.py:214-218,347-350`: Cache hit/miss recording via `record_cache_get` and `record_cache_set` -- these are observability-aware. Good.
- `tools/cq/search/enrichment/incremental_provider.py`: Each incremental stage has try/except boundaries that silently catch errors. No logging of what failed or why -- only the `degrade_reason` is set on the payload.

**Suggested improvement:**
Add structured logging at key decision points: classification outcome, enrichment stage failure, cache miss reasons, and parallel worker fallback to sequential. Consider OpenTelemetry spans for the candidate, classification, and enrichment phases to enable distributed tracing.

**Effort:** medium
**Risk if unaddressed:** low -- the current telemetry is adequate for post-hoc analysis but provides no real-time observability during pipeline execution.

---

## Cross-Cutting Themes

### Theme 1: The `dict[str, object]` Anti-Pattern

**Root cause:** The enrichment pipeline was initially built around flat dict payloads for flexibility and serialization ease. Typed fact structs were added later but the boundary contracts were not updated to use them.

**Affected principles:** P9 (Parse, don't validate), P10 (Make illegal states unrepresentable), P14 (Law of Demeter), P15 (Tell, don't ask), P8 (Design by contract), P24 (Observability).

**Scale:** 191 occurrences across pipeline (61) and enrichment (130) directories. The `dict[str, object]` type is the single most-used type annotation in the reviewed scope.

**Suggested approach:** Phase the migration over three stages:
1. Replace the three enrichment wrapper `payload` fields with typed fact structs
2. Update adapters and accumulation functions to work on typed structs
3. Replace the telemetry dict factory with typed structs

### Theme 2: Telemetry Accumulation Duplication

**Root cause:** When the `LanguageEnrichmentPort` pattern was introduced, adapters reimplemented accumulation logic that already existed in `smart_search_telemetry.py`. The two code paths were not consolidated.

**Affected principles:** P7 (DRY), P3 (SRP).

**Scale:** 8 duplicated functions across 4 modules: `accumulate_stage_status` (2 copies), `accumulate_stage_timings` (3 copies), `accumulate_runtime_flags`/`accumulate_rust_runtime` (3 copies), `accumulate_rust_bundle`/`_accumulate_bundle_drift` (2 copies), plus `_coerce_count`/`_counter`/`int_counter` (3 copies).

**Suggested approach:** Move all accumulation helpers to `enrichment/telemetry.py`. Have both adapters and `smart_search_telemetry.py` delegate to this single authority.

### Theme 3: Layer Boundary Misplacement

**Root cause:** `enrichment_contracts.py` was placed in `pipeline/` because it was created during pipeline development, but it defines enrichment-domain concepts.

**Affected principles:** P5 (Dependency direction), P4 (High cohesion, low coupling).

**Scale:** 2 files in enrichment import from pipeline. 5 files in pipeline import from the same module. Moving it would be a single-file relocation with import path updates.

**Suggested approach:** Move `enrichment_contracts.py` to `_shared/` (shared foundation) or `enrichment/` (domain layer). Update all import paths. This is a mechanical change with high value.

### Theme 4: Module-Level Mutable State

**Root cause:** Python's module-level variables are the simplest way to implement lazy singletons, but they create hidden global state.

**Affected principles:** P1 (Information hiding), P23 (Design for testability), P12 (DI + explicit composition).

**Scale:** 3 instances: `search_object_view_store.py:8`, `language_registry.py:8`, `pipeline/__init__.py:7-10`.

**Suggested approach:** Wrap mutable state in proper registry classes. For the language registry, make initialization explicit. For the object view store, consider passing it as a parameter rather than using global state.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Consolidate `_coerce_count` from `incremental_compound_plane.py:8` and `incremental_dis_plane.py:23` into `_shared/helpers.py` | small | Eliminates verbatim duplication |
| 2 | P5 (Dependency direction) | Move `enrichment_contracts.py` from `pipeline/` to `_shared/` | small | Fixes layer inversion for 2 files |
| 3 | P21 (Least astonishment) | Remove `_run_candidate_phase` from `smart_search.py:__all__`; rename `SearchContext` alias | small | Clearer public surface |
| 4 | P20 (YAGNI) | Remove trivial re-export modules (`timeout.py`, `rg_request.py`, `encoding.py`) | small | Reduces module count by 3 |
| 5 | P7 (DRY) | Consolidate `int_counter`/`_counter` into `_shared/helpers.py` as `safe_int_counter` | small | Eliminates 3 copies of same logic |

## Recommended Action Sequence

1. **Consolidate trivial duplications (P7):** Move `_coerce_count` and `int_counter`/`_counter` to `_shared/helpers.py`. Update all import sites. (Dependencies: none)

2. **Fix layer boundary (P5, P4):** Relocate `enrichment_contracts.py` from `pipeline/` to `_shared/`. Update all import paths in enrichment adapters and pipeline modules. (Dependencies: none)

3. **Clean public surface (P21, P22, P20):** Remove `_run_candidate_phase` from `__all__`. Remove trivial re-export modules. Remove or deprecate `generate_followup_suggestions` alias. Rename `SearchContext` alias. (Dependencies: none)

4. **Consolidate telemetry accumulation (P7, P3):** Create `enrichment/telemetry.py` with canonical accumulation helpers. Update `smart_search_telemetry.py` and both adapters to delegate to the canonical implementations. (Dependencies: step 1 for `safe_int_counter`)

5. **Type the telemetry schema (P10, P8):** Define `EnrichmentTelemetrySchema` as a frozen msgspec struct hierarchy. Replace `empty_enrichment_telemetry()` dict factory with struct construction. Update accumulation functions to operate on typed structs. (Dependencies: step 4)

6. **Type the enrichment payloads (P9, P10, P14, P15):** Replace `payload: dict[str, object]` in the three enrichment wrapper structs with typed fact struct references (`PythonEnrichmentFacts`, `RustEnrichmentFacts`). Update the classification and assembly code to flow typed facts through the pipeline. (Dependencies: steps 2, 5)

7. **Improve testability (P23, P1):** Wrap module-level registries in proper classes. Accept `CqCacheBackend` as a parameter. Move inline imports to module level or accept as parameters. (Dependencies: step 6 reduces the surface area of changes needed)

8. **Add structured logging (P24):** Add structured log statements at classification decision points, enrichment stage boundaries, cache miss reasons, and parallel worker fallback events. (Dependencies: step 7 for cleaner injection points)
