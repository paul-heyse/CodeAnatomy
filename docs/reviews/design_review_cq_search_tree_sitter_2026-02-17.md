# Design Review: tools/cq/search/tree_sitter/

**Date:** 2026-02-17
**Scope:** `tools/cq/search/tree_sitter/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 58 (all Python files in scope, 11,398 LOC total)

## Executive Summary

The tree-sitter subsystem provides a well-structured, multi-language enrichment pipeline with strong contract typing (frozen msgspec structs), clean adapter patterns, and robust graceful degradation. The primary design challenges are: (1) `rust_lane/runtime_core.py` is a 1,209-line God module with 8+ distinct responsibilities, (2) significant knowledge duplication between Python and Rust lane implementations (`_pack_source_rows`, `_collect_query_pack_captures`, `_extract_provenance`, `_build_node_id`), (3) pervasive use of `dict[str, object]` for intermediate payloads where typed contracts would prevent runtime errors, and (4) module-level mutable singletons that complicate testing and violate dependency inversion. The highest-impact improvement is decomposing `runtime_core.py` into focused modules, which would address P3, P1, P4, and P19 simultaneously.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | high | `__dict__` access bypasses private boundaries in `query_orchestration.py:41` and `payload_assembly.py:27` |
| 2 | Separation of concerns | 1 | large | medium | `runtime_core.py` mixes parsing, enrichment, query execution, payload building, and timing |
| 3 | SRP | 0 | large | high | `runtime_core.py` (1,209 LOC) has 8+ distinct change reasons; `python_lane/facts.py` (738 LOC) similar |
| 4 | High cohesion, low coupling | 1 | large | medium | `runtime_core.py` imports from 20+ modules; lanes tightly coupled to shared mutable state |
| 5 | Dependency direction | 1 | medium | medium | `query_orchestration.py` and `payload_assembly.py` reach into private internals of `runtime_core.py` |
| 6 | Ports & Adapters | 2 | small | low | `enrichment_extractors.py` uses clean adapter pattern; lanes share core via `lane_support.py` |
| 7 | DRY (knowledge) | 0 | medium | high | `_extract_provenance` in 3 files, `_pack_source_rows` in 3 files, `_build_node_id` in 2 files |
| 8 | Design by contract | 2 | small | low | Frozen contracts with validation (e.g., `QueryWindowV1.__post_init__`); some gaps in `dict[str, object]` returns |
| 9 | Parse, don't validate | 2 | medium | low | `canonicalize_*_lane_payload()` validates but still returns `dict[str, Any]` instead of typed struct |
| 10 | Make illegal states unrepresentable | 1 | medium | medium | `dict[str, object]` payload anti-pattern allows arbitrary keys; `lane_payloads.py` uses `dict[str, Any]` fields |
| 11 | CQS | 1 | small | medium | `load_query_pack_sources()` mutates `runtime_state.last_drift_reports` as side effect (`registry.py:210`) |
| 12 | DI + explicit composition | 1 | medium | medium | Module-level mutable singletons (`_SESSIONS`, `_GLOBAL_STATE_HOLDER`, `_TREE_CACHE`) bypass DI |
| 13 | Prefer composition over inheritance | 3 | - | - | No inheritance hierarchies; composition used throughout |
| 14 | Law of Demeter | 1 | small | medium | `__dict__["_private_fn"]` chains in `query_orchestration.py:41`, `payload_assembly.py:27` |
| 15 | Tell, don't ask | 2 | small | low | Most extractors follow tell pattern; some `getattr` probing for optional tree-sitter APIs |
| 16 | Functional core, imperative shell | 2 | medium | low | Pure transforms in extractors; timing/caching at edges; `runtime_core.py` mixes both |
| 17 | Idempotency | 3 | - | - | All enrichment is additive and re-runnable; cache operations are idempotent |
| 18 | Determinism | 2 | small | low | Mostly deterministic; `time.perf_counter()` in payloads and `_TREE_CACHE_EVICTIONS` mutable dict |
| 19 | KISS | 1 | medium | medium | `runtime_core.py` has 7 internal `@dataclass` types used only within the file; excess indirection layers |
| 20 | YAGNI | 2 | small | low | `parser_controls.py` and `streaming_parse.py` abstractions are used; minor unused flexibility in `QueryPointWindowV1` |
| 21 | Least astonishment | 1 | small | medium | `__all__` in `fallback_support.py:345-352` exports underscore-prefixed private names as public API |
| 22 | Declare and version public contracts | 2 | small | low | `__all__` used consistently; `TreeSitterInputEditV1` uses `dataclass` instead of `CqStruct` (inconsistency) |
| 23 | Design for testability | 1 | medium | medium | Module-level singletons require monkeypatching; `_TREE_CACHE_EVICTIONS` mutable dict pattern |
| 24 | Observability | 2 | small | low | Structured logging throughout; telemetry contracts; `stage_timings_ms` and `stage_status` patterns |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
Two modules bypass Python's private-name convention by accessing internal functions through `__dict__` lookups on `runtime_core.py`. This creates a fragile coupling where internal refactoring of `runtime_core.py` could silently break these callers without any import-time errors.

**Findings:**
- `tools/cq/search/tree_sitter/rust_lane/query_orchestration.py:41` accesses `_runtime_core.__dict__["_collect_query_pack_captures"]` -- a private function accessed via dict introspection to avoid import-time name mangling
- `tools/cq/search/tree_sitter/rust_lane/payload_assembly.py:27` accesses `_runtime_core.__dict__["_collect_query_pack_payload"]` -- same pattern
- `tools/cq/search/tree_sitter/rust_lane/runtime_core.py:82-84` imports private functions from `runtime_cache.py`: `_parse_with_session`, `_rust_field_ids`, `_rust_language` -- while less severe since both are in `rust_lane/`, these cross-module private imports reduce information hiding

**Suggested improvement:**
Make `_collect_query_pack_captures` and `_collect_query_pack_payload` public API on a dedicated module (e.g., `rust_lane/query_pack_execution.py`) with stable signatures. Replace `__dict__` introspection with normal imports. If the functions need to remain in `runtime_core.py` temporarily, expose them via `__all__` with public names.

**Effort:** medium
**Risk if unaddressed:** high -- any rename or refactor of `runtime_core.py` internals silently breaks these callers at runtime only

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`rust_lane/runtime_core.py` (1,209 lines) conflates at least 8 distinct concerns into a single module: runtime availability checking, node/scope utilities, enrichment payload building, extractor dispatch, query pack execution, execution planning, payload finalization with timing, and public entry points.

**Findings:**
- `runtime_core.py:272-294` -- availability checking (`is_tree_sitter_rust_available`)
- `runtime_core.py:302-341` -- node/scope utilities (`_scope_name`, `_scope_chain`, `_find_scope`)
- `runtime_core.py:353-529` -- enrichment extractor dispatch (`_try_extract`, `_apply_extractors`)
- `runtime_core.py:596-640` -- enrichment payload building (`_build_enrichment_payload`)
- `runtime_core.py:663-737` -- query pack execution (`_run_query_pack`, `_collect_query_pack_captures`)
- `runtime_core.py:740-902` -- execution planning and bundle collection (`_query_execution_plan`, `_collect_query_bundle`, `_collect_query_pack_payload`)
- `runtime_core.py:978-1071` -- timing-instrumented pipeline orchestration (`_collect_payload_with_timings`, `_run_rust_enrichment_pipeline`)
- `runtime_core.py:1079-1197` -- public entry points (`enrich_rust_context`, `enrich_rust_context_by_byte_range`)

**Suggested improvement:**
Decompose `runtime_core.py` into focused modules within `rust_lane/`:
1. `rust_lane/availability.py` -- `is_tree_sitter_rust_available()`
2. `rust_lane/scope_utils.py` -- `_scope_name`, `_scope_chain`, `_find_scope`
3. `rust_lane/extractor_dispatch.py` -- `_try_extract`, `_apply_extractors`, `_build_enrichment_payload`
4. `rust_lane/query_pack_execution.py` -- `_run_query_pack`, `_collect_query_pack_captures` (now public)
5. `rust_lane/pipeline.py` -- `_run_rust_enrichment_pipeline`, `_collect_payload_with_timings`
Keep `runtime_core.py` as a thin facade re-exporting public API for backward compatibility.

**Effort:** large
**Risk if unaddressed:** medium -- difficulty understanding, testing, and modifying any single concern without reading 1,200+ lines

---

#### P3. SRP (one reason to change) -- Alignment: 0/3

**Current state:**
Three modules exceed the 600 LOC threshold with multiple distinct responsibilities:

**Findings:**
- `rust_lane/runtime_core.py` (1,209 LOC) -- 8+ responsibilities as detailed in P2. Contains 7 internal `@dataclass` types (`_RustPackRunResultV1`, `_RustPackAccumulatorV1`, `_RustQueryPackArtifactsV1`, `_RustQueryCollectionV1`, `_RustQueryExecutionPlanV1`, `_RustPayloadBuildRequestV1`, `_RustPipelineRequestV1`) that exist solely because the module handles too many concerns
- `python_lane/facts.py` (738 LOC) -- mixes query pack capture execution, fact extraction, parse quality analysis, and source row caching
- `python_lane/runtime.py` (685 LOC) -- mixes language loading, parse session management, enrichment pipeline orchestration, and cache statistics
- `core/runtime.py` (644 LOC) -- mixes bounded query execution, windowing, autotuning, deduplication, and telemetry emission

**Suggested improvement:**
For `runtime_core.py`, apply the decomposition from P2. For `python_lane/facts.py`, extract `_collect_query_pack_captures` into a shared `core/query_pack_pipeline.py` module alongside the Rust equivalent, since both follow the same pattern. For `python_lane/runtime.py`, extract cache management into `python_lane/runtime_cache.py` (mirroring the Rust lane structure).

**Effort:** large
**Risk if unaddressed:** high -- each change risks introducing regressions across unrelated concerns in the same file

---

#### P4. High cohesion, low coupling -- Alignment: 1/3

**Current state:**
`runtime_core.py` has an extremely high fan-out with 20+ import targets, creating tight coupling to nearly every module in the subsystem.

**Findings:**
- `runtime_core.py:24-89` -- imports from 20 distinct modules spanning `contracts`, `core`, `query`, `rust_lane`, and `structural` packages
- `runtime_core.py:67-73` imports private functions from `fact_extraction.py`: `_extend_rust_fact_lists_from_rows`, `_import_rows_from_matches`, `_macro_expansion_requests`, `_module_rows_from_matches`, `_rust_fact_lists`, `_rust_fact_payload` -- 6 private imports from a single sibling
- `runtime_core.py:79` imports `_pack_sources` from `query_cache.py` -- private name crossing module boundary

**Suggested improvement:**
After decomposing `runtime_core.py` (P2), each sub-module will have a focused, narrow import set. The `fact_extraction.py` private functions should be exposed via a public API (e.g., `extract_rust_facts(captures, rows, source_bytes)` facade) to reduce the coupling surface.

**Effort:** large (addressed by P2 decomposition)
**Risk if unaddressed:** medium -- changes to any imported module may cascade through `runtime_core.py`

---

#### P5. Dependency direction -- Alignment: 1/3

**Current state:**
Two modules in `rust_lane/` reach into the private internals of `runtime_core.py` using `__dict__` access, creating an inverted dependency where extraction modules depend on implementation details of the orchestrator.

**Findings:**
- `rust_lane/query_orchestration.py:39-42` -- late import + `__dict__` access creates a runtime-only coupling to `_collect_query_pack_captures`
- `rust_lane/payload_assembly.py:25-27` -- same pattern for `_collect_query_pack_payload`
- Both modules were created to provide "stable orchestration boundaries" (per their docstrings) but paradoxically couple to the most unstable surface possible (private function names accessed by string)

**Suggested improvement:**
Extract `_collect_query_pack_captures` and `_collect_query_pack_payload` into a dedicated `rust_lane/query_pack_execution.py` with public signatures. Have `query_orchestration.py` and `payload_assembly.py` import from it normally. Alternatively, if these modules exist solely to break circular imports, resolve the cycle structurally.

**Effort:** medium
**Risk if unaddressed:** medium -- fragile coupling will break silently on refactoring

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The subsystem demonstrates good adapter patterns in several areas but could improve port definition consistency.

**Findings:**
- `rust_lane/enrichment_extractors.py` (140 LOC) -- clean adapter pattern wrapping shared extractors with `TreeSitterRustNodeAccess`; well-aligned
- `core/lane_support.py` -- provides shared lane infrastructure (window building, parser creation, error types); functions as an implicit port
- `contracts/core_models.py:16-52` -- `NodeLike` protocol serves as a well-defined structural port for tree-sitter node access
- `core/adaptive_runtime.py:19-22` -- `_resolve_backend()` uses duck-typed `getattr` protocol for cache backends instead of defining an explicit port/protocol

**Suggested improvement:**
Define a `CacheBackendProtocol` in contracts for the cache backend duck-typing in `adaptive_runtime.py`. The existing `NodeLike` protocol is a good model to follow.

**Effort:** small
**Risk if unaddressed:** low -- duck typing works but lacks documentation and type-checking support

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 0/3

**Current state:**
Multiple pieces of domain knowledge are duplicated across the Python and Rust lanes, and across structural/query subsystems.

**Findings:**

*`_extract_provenance` -- 3 copies:*
- `core/language_registry.py:84-96` -- canonical copy
- `query/planner.py:22-40` -- near-identical copy with `*` keyword-only parameter style
- `schema/node_schema.py:92-109` -- near-identical copy with `runtime_language_obj` parameter name

*`_pack_source_rows` -- 3 copies:*
- `python_lane/runtime.py:202-229` -- Python lane version
- `python_lane/facts.py:235-257` -- separate Python copy (different dedup strategy)
- `rust_lane/query_cache.py:18-54` -- Rust lane version (structurally identical pattern)

*`_build_node_id` / `build_node_id` -- 2 copies:*
- `structural/export.py:20-32` -- public `build_node_id()`
- `structural/exports.py:23-35` -- private `_build_node_id()` with identical logic

*`_collect_query_pack_captures` -- 2 structural duplicates:*
- `python_lane/facts.py:264-275` -- Python lane version
- `rust_lane/runtime_core.py:707-737` -- Rust lane version (same accumulator pattern)

**Suggested improvement:**
1. Move `_extract_provenance` to `core/language_registry.py` exclusively and import it in `planner.py` and `node_schema.py`
2. Create `core/query_pack_pipeline.py` with a generic `collect_query_pack_captures()` that takes a language parameter and pack source loader, used by both lanes
3. Remove `_build_node_id` from `exports.py` and import `build_node_id` from `export.py`
4. Extract `_pack_source_rows` into a shared factory in `query/registry.py` parameterized by language

**Effort:** medium
**Risk if unaddressed:** high -- bug fixes or invariant changes must be replicated across all copies

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The contract layer is well-designed with frozen msgspec structs and validation, but some internal APIs use untyped dicts that bypass contract enforcement.

**Findings:**
- `contracts/core_models.py:55-69` -- `QueryWindowV1.__post_init__` validates `start_byte <= end_byte`; good explicit precondition
- `runtime_core.py:643-647` -- `_assert_required_payload_keys()` validates payload contains required keys; good postcondition check
- `runtime_core.py:625-640` -- `_build_enrichment_payload` returns `dict[str, object]` without contract validation; caller must trust shape
- `core/adaptive_runtime.py:38-39` -- `getattr(backend, "get", None)` duck-typing without explicit contract on cache backend

**Suggested improvement:**
Define a `RustEnrichmentPayloadV1` msgspec struct for the return type of `_build_enrichment_payload()` instead of `dict[str, object]`. This would catch key typos and type mismatches at construction time.

**Effort:** small
**Risk if unaddressed:** low -- current runtime validation catches most issues

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
The `canonicalize_*_lane_payload()` functions validate payloads against typed structs but then return `dict[str, Any]` instead of the struct itself, losing the type guarantee.

**Findings:**
- `contracts/lane_payloads.py:57` -- `_ = msgspec.convert(payload, type=target_type, strict=False)` validates structure but discards the typed result, returning raw dict
- `contracts/lane_payloads.py:51` -- `payload = dict(payload)` creates a mutable copy that the validator cannot enforce constraints on after validation
- `runtime_core.py:1018` -- `canonical = canonicalize_rust_lane_payload(payload)` receives a `dict[str, Any]` back despite validation

**Suggested improvement:**
Have `_canonicalize_lane_payload()` return the validated struct instance (`PythonTreeSitterPayloadV1` or `RustTreeSitterPayloadV1`) instead of `dict[str, Any]`. Callers that need dict access can use `msgspec.to_builtins()`. This converts validation into parsing at the boundary.

**Effort:** medium
**Risk if unaddressed:** low -- validation currently catches most issues; the remaining risk is post-validation mutation

---

#### P10. Make illegal states unrepresentable -- Alignment: 1/3

**Current state:**
The `dict[str, object]` intermediate payload pattern throughout the enrichment pipeline allows arbitrary key/value combinations that typed structs would prevent.

**Findings:**
- `runtime_core.py:625` -- `payload: dict[str, object] = {...}` allows any key to be set or omitted
- `runtime_core.py:854-902` -- `_collect_query_pack_payload()` returns `dict[str, object]` with 10+ expected keys but no structural enforcement
- `contracts/lane_payloads.py:20-22` -- `PythonTreeSitterPayloadV1` uses `list[dict[str, Any]]` for `cst_diagnostics` and `cst_query_hits` instead of `list[TreeSitterDiagnosticV1]` and `list[TreeSitterQueryHitV1]`
- `contracts/lane_payloads.py:32` -- `query_runtime: dict[str, Any]` instead of a typed `QueryRuntimeSummaryV1` struct
- `contracts/core_models.py:298` -- `TreeSitterArtifactBundleV1.telemetry: dict[str, object]` -- untyped telemetry bag

**Suggested improvement:**
Replace the `dict[str, object]` payload pattern in `runtime_core.py` with a typed `RustEnrichmentPayloadV1` struct that explicitly declares all fields. Update `PythonTreeSitterPayloadV1` and `RustTreeSitterPayloadV1` to use their typed counterparts (`list[TreeSitterDiagnosticV1]`, `list[TreeSitterQueryHitV1]`) instead of `list[dict[str, Any]]`.

**Effort:** medium
**Risk if unaddressed:** medium -- typos in dict keys produce silent `None` values at runtime

---

#### P11. CQS -- Alignment: 1/3

**Current state:**
`load_query_pack_sources()` in `query/registry.py` both returns data and mutates global runtime state as a side effect.

**Findings:**
- `query/registry.py:206-210` -- `load_query_pack_sources()` calls `get_query_runtime_state()` and mutates `runtime_state.last_drift_reports[normalized] = report`. This function returns query pack sources (query) while also mutating global drift report state (command)
- `query/registry.py:213-216` -- on drift incompatibility, it mutates `runtime_state.last_drift_reports` a second time with fallback data
- `_RustPackAccumulatorV1.merge()` at `runtime_core.py:173-191` -- mutates internal state and returns `None`; acceptable for a builder pattern but the class also has a `finalize()` method that returns data, so the overall pattern mixes command and query across the lifecycle

**Suggested improvement:**
Split `load_query_pack_sources()` into two functions: one pure function that loads and returns sources with drift info, and a separate command that stores drift reports in runtime state. Callers can compose them explicitly.

**Effort:** small
**Risk if unaddressed:** medium -- callers cannot load sources without unintended side effects on global state

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 1/3

**Current state:**
Module-level mutable singletons create hidden dependencies that are difficult to override for testing.

**Findings:**
- `core/parse.py:270` -- `_SESSIONS: dict[str, ParseSession] = {}` is a module-level mutable singleton; `get_parse_session()` at line 273 is a singleton factory with no DI seam
- `query/runtime_state.py:29` -- `_GLOBAL_STATE_HOLDER: dict[str, QueryRuntimeState | None] = {"value": None}` is a module-level mutable singleton
- `rust_lane/runtime_cache.py:32-33` -- `_TREE_CACHE: BoundedCache[str, None]` and `_TREE_CACHE_EVICTIONS = {"value": 0}` are module-level mutable state
- `rust_lane/runtime_cache.py:34` -- `CACHE_REGISTRY.register_cache("rust", ...)` executes at import time as a side effect
- `rust_lane/runtime_cache.py:132` -- `CACHE_REGISTRY.register_clear_callback("rust", ...)` executes at import time
- `rust_lane/query_cache.py:73` -- `CACHE_REGISTRY.register_clear_callback("rust", clear_query_cache)` -- another import-time side effect

**Suggested improvement:**
Introduce a `TreeSitterRuntimeContext` that holds parse sessions, caches, and runtime state. Pass it explicitly through the pipeline. For backward compatibility, provide a `get_default_context()` function that returns a module-level default, but allow callers (especially tests) to provide their own context. This follows the pattern already established by `RustLaneRuntimeDepsV1` at `runtime_core.py:122-128`.

**Effort:** medium
**Risk if unaddressed:** medium -- tests must use `monkeypatch` to override module-level state; parallel test execution risks interference

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies exist in the subsystem. All behavior composition uses function composition, dataclass aggregation, and protocol-based structural typing.

**Findings:**
- `contracts/core_models.py:16-52` -- `NodeLike` protocol used for structural typing (not inheritance)
- All contract types inherit only from `CqStruct`/`CqCacheStruct`/`CqOutputStruct` for serialization, not for behavior

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
The `__dict__` access pattern and deep chain access into tree-sitter node internals via `getattr` violate the Law of Demeter.

**Findings:**
- `query_orchestration.py:41` -- `_runtime_core.__dict__["_collect_query_pack_captures"]` -- accessing a module's internal dict to reach a private function
- `payload_assembly.py:27` -- `_runtime_core.__dict__["_collect_query_pack_payload"]` -- same violation
- `structural/export.py:178-183` -- `_language_name_for_root()` chains `root.tree.language.name` through 3 levels of attribute access using defensive `getattr`
- `core/language_registry.py:89-91` -- `getattr(language_obj, "name", None)`, `getattr(language_obj, "semantic_version", None)`, `getattr(language_obj, "abi_version", None)` -- probing attributes on an untyped `object`

**Suggested improvement:**
For the `__dict__` access, make the target functions public (P1 fix). For the `getattr` chains on tree-sitter objects, the defensive approach is justified given the optional-dependency nature of tree-sitter, but consider centralizing provenance extraction into a single utility (which also addresses P7).

**Effort:** small (for `__dict__` fixes; `getattr` chains are acceptable for optional deps)
**Risk if unaddressed:** medium

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Most enrichment follows the "tell" pattern: extractors are called with nodes and source bytes, and they return enrichment dicts. Some `getattr`-probing patterns for optional API detection are necessary but could be more encapsulated.

**Findings:**
- `core/runtime.py` and `core/runtime_support.py` -- use `getattr` to probe for optional `QueryCursor` capabilities (`containing_byte_range`, `containing_point_range`); justified for optional API detection
- `structural/export.py:86-99` -- `_advance_cursor()` contains ask-then-act on `scratch_cursor` capabilities; the function probes whether methods exist before calling them

**Suggested improvement:**
Consider encapsulating capability detection into a `QueryCursorCapabilities` struct (already exists as `TreeSitterRuntimeCapabilitiesV1` in `language_registry.py:49-58`) and passing it through rather than probing at each call site.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The enrichment extractors in `rust_lane/enrichment_extractors.py` and `python_lane/fallback_support.py` are pure functions (take node + bytes, return dict). Timing, caching, and IO are at the edges. However, `runtime_core.py` mixes pure transforms with timing instrumentation.

**Findings:**
- `rust_lane/enrichment_extractors.py` -- all 6 extractors (`extract_function_signature`, `extract_visibility_dict`, etc.) are pure functions; well-aligned
- `runtime_core.py:978-1009` -- `_collect_payload_with_timings()` mixes pure payload building with `time.perf_counter()` timing; the timing concern should be in the shell
- `runtime_core.py:1033-1071` -- `_run_rust_enrichment_pipeline()` further layers timing on top of timing, mixing IO (cache lookup, logging) with transforms

**Suggested improvement:**
Separate timing instrumentation into a decorator or wrapper function, keeping `_build_enrichment_payload()` and `_collect_query_pack_payload()` as pure transforms. Apply timing at the public entry point only.

**Effort:** medium
**Risk if unaddressed:** low -- current approach works but makes testing pure logic harder

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All enrichment operations are additive and idempotent. Re-running enrichment with the same inputs produces the same outputs (modulo timing values). Cache operations use put-if-absent semantics.

**Findings:**
- `rust_lane/runtime_cache.py:80-86` -- `_touch_tree_cache()` uses put semantics that are safe to repeat
- All enrichment payloads are constructed fresh per invocation; no destructive mutations

**Effort:** -
**Risk if unaddressed:** -

---

#### P18. Determinism / reproducibility -- Alignment: 2/3

**Current state:**
Most operations are deterministic. Non-determinism exists only in timing values embedded in payloads and in the mutable eviction counter.

**Findings:**
- `runtime_core.py:1000-1007` -- `payload["stage_timings_ms"]` and `payload["stage_status"]` embed wall-clock timing that varies between runs
- `rust_lane/runtime_cache.py:33` -- `_TREE_CACHE_EVICTIONS = {"value": 0}` -- mutable counter using dict-in-a-dict pattern instead of a proper counter object
- `core/parse.py:99-103` -- `ParseSession` has mutable internal counters (`_cache_hits`, `_cache_misses`) that are observation-only and don't affect parse results

**Suggested improvement:**
Timing values are inherently non-deterministic and appropriate for observability; no change needed. The `_TREE_CACHE_EVICTIONS` dict pattern is unusual -- consider using a simple `int` counter attribute on a stats object.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 1/3

**Current state:**
`runtime_core.py` defines 7 internal `@dataclass` types and contains complex multi-stage pipeline logic that could be simplified by decomposition.

**Findings:**
- `runtime_core.py:114-265` -- 7 frozen `@dataclass` types (`_RustPackRunResultV1`, `_RustPackAccumulatorV1`, `_RustQueryPackArtifactsV1`, `_RustQueryCollectionV1`, `_RustQueryExecutionPlanV1`, `_RustPayloadBuildRequestV1`, `_RustPipelineRequestV1`) defined as internal types; their scope suggests they should live in separate modules alongside the functions that use them
- `structural/export.py:78-110` -- `_advance_cursor()` has 5 parameters including mutable stack arguments passed by reference; complex state management for tree traversal
- `core/query_pack_executor.py` (161 LOC) -- thin wrapper around `core/runtime.py` that adds an indirection layer without substantial additional logic

**Suggested improvement:**
The internal `@dataclass` types in `runtime_core.py` would naturally migrate to their respective sub-modules during the P2 decomposition. Consider whether `core/query_pack_executor.py` can be merged back into `core/runtime.py` or justified with clearer separation.

**Effort:** medium (addressed by P2 decomposition)
**Risk if unaddressed:** medium -- cognitive load of reading 1,200+ lines to understand any single concern

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most abstractions are actively used. Minor speculative flexibility exists.

**Findings:**
- `contracts/core_models.py:72-78` -- `QueryPointWindowV1` is defined but only used in `core/runtime_support.py`; however, it serves a clear purpose for point-based window queries
- `query/specialization.py` and `query/lint.py` -- well-scoped utilities that are actively used by the query planner
- No unused modules or dead code paths detected at the file level

**Suggested improvement:**
No action needed. Existing abstractions earn their keep.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
The `__all__` export list in `fallback_support.py` exports underscore-prefixed names, which violates Python convention that underscore-prefixed names are private.

**Findings:**
- `python_lane/fallback_support.py:345-352` -- `__all__` contains 6 entries, all prefixed with underscore: `"_capture_binding_candidates"`, `"_capture_import_alias_chain"`, `"_capture_named_definition"`, `"_capture_qualified_name_candidates"`, `"_default_parse_quality"`, `"_fallback_resolution_fields"`. A developer encountering these in `from fallback_support import *` would be surprised that the "public" API is all underscore-prefixed
- `rust_lane/runtime_core.py:1100-1101` -- docstring mentions `max_scope_depth` parameter that does not exist in the function signature (it was replaced by `settings` parameter); stale documentation

**Suggested improvement:**
Rename the exported functions in `fallback_support.py` to drop the underscore prefix, since they are intended to be imported by other modules. Update the `runtime_core.py` docstring to reference `settings` instead of `max_scope_depth`.

**Effort:** small
**Risk if unaddressed:** medium -- confusing API surface for developers

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
The `__all__` declaration pattern is used consistently across all modules. Contract types use version suffixes (`V1`). One inconsistency in contract base class usage.

**Findings:**
- `contracts/core_models.py:131-141` -- `TreeSitterInputEditV1` uses `@dataclass(frozen=True, slots=True)` instead of `CqStruct` like all other contracts in the same file. This breaks the uniform serialization surface
- All other contracts consistently use `CqStruct`, `CqCacheStruct`, or `CqOutputStruct` as base classes
- `rust_lane/runtime.py` -- clean re-export facade for `runtime_core.py` public API; well-aligned

**Suggested improvement:**
Migrate `TreeSitterInputEditV1` from `@dataclass(frozen=True, slots=True)` to `CqStruct(frozen=True)` for consistency with sibling types. Verify that `tree-sitter`'s `Tree.edit()` method can accept the struct fields by position (it can, since the call at `parse.py:254-261` unpacks fields individually).

**Effort:** small
**Risk if unaddressed:** low -- minor inconsistency

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
Module-level mutable singletons require `monkeypatch` for testing. The `_TREE_CACHE_EVICTIONS` pattern using a mutable dict wrapper around an integer is particularly difficult to test cleanly.

**Findings:**
- `core/parse.py:270` -- `_SESSIONS` module-level dict requires clearing between tests; `clear_parse_session()` at line 288 provides a clearing API but tests must call it explicitly
- `query/runtime_state.py:29` -- `_GLOBAL_STATE_HOLDER` with `set_query_runtime_state(None)` at line 48 provides a test reset seam; better than no seam but still global mutable state
- `rust_lane/runtime_cache.py:33` -- `_TREE_CACHE_EVICTIONS = {"value": 0}` uses dict-wrapping-int pattern to allow mutation from a module-level binding; this is a test smell
- `rust_lane/runtime_cache.py:34,132` -- `CACHE_REGISTRY.register_cache()` and `CACHE_REGISTRY.register_clear_callback()` execute at import time, making it impossible to import the module without side effects
- `runtime_core.py:1079-1136` -- `enrich_rust_context()` calls `is_tree_sitter_rust_available()` internally; tests must ensure tree-sitter dependencies are installed or mock the availability check

**Suggested improvement:**
The `RustLaneRuntimeDepsV1` pattern at `runtime_core.py:122-128` is the right direction -- it allows test injection of parse session and cache backend. Extend this pattern to hold all runtime state (tree cache, eviction counters, parse sessions) and pass it through the pipeline. Guard import-time side effects behind lazy initialization.

**Effort:** medium
**Risk if unaddressed:** medium -- test isolation requires careful teardown; parallel test execution at risk

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
The subsystem has structured logging, telemetry contracts, and timing instrumentation throughout.

**Findings:**
- `runtime_core.py:106` -- `logger = logging.getLogger(__name__)` -- standard structured logger
- `runtime_core.py:1000-1007` -- `stage_timings_ms` and `stage_status` dicts provide per-stage timing and status observability
- `contracts/core_models.py:96-107` -- `QueryExecutionTelemetryV1` provides structured telemetry for bounded query execution
- `contracts/core_models.py:143-152` -- `ParseSessionStatsV1` provides structured cache statistics
- `core/parse.py:300-308` -- `clear_parse_session()` logs session statistics at DEBUG level before clearing
- `diagnostics.py` -- dedicated module for collecting tree-sitter syntax diagnostics; well-aligned

**Suggested improvement:**
Consider defining `StageTimingsV1` and `StageStatusV1` structs instead of using `dict[str, float]` and `dict[str, str]` for the timing/status payloads in `runtime_core.py`. This would align with the typed telemetry pattern used elsewhere.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: The `dict[str, object]` Payload Anti-Pattern

**Description:** Throughout the enrichment pipeline, intermediate payloads are built as `dict[str, object]` and mutated in place. This pattern is concentrated in `runtime_core.py` (lines 625, 822, 854, 940-966, 978-1009) and `python_lane/runtime.py`. While flexible, it sacrifices the type safety that the frozen msgspec contracts provide at the API boundary.

**Root cause:** The enrichment pipeline was likely built incrementally, adding keys to dicts as new extractors were added. Migrating to a typed struct requires defining a comprehensive payload type that accounts for all optional fields across all extractors.

**Affected principles:** P8, P9, P10, P22

**Suggested approach:** Define `RustEnrichmentPayloadV1` and `PythonEnrichmentPayloadV1` structs with all known fields as `| None` optional. Build payloads via struct construction instead of dict mutation. This is a medium-effort change that pays compound dividends across 4 principles.

### Theme 2: Python/Rust Lane Duplication

**Description:** The Python and Rust enrichment lanes share identical patterns for query pack loading, capture collection, fact extraction, and source row caching. Each lane implements these independently, creating maintenance burden where changes must be synchronized across lanes.

**Root cause:** The lanes were developed as parallel implementations for different grammars, but the core query-pack execution logic is language-agnostic. Only the extractors and node access patterns are language-specific.

**Affected principles:** P7 (directly), P3, P4, P19

**Suggested approach:** Extract shared query-pack pipeline logic into `core/query_pack_pipeline.py` parameterized by language. Keep language-specific extractors, node access adapters, and grammar bundles in their respective lanes. This reduces 3 copies of `_pack_source_rows` and 2 copies of `_collect_query_pack_captures` to 1 each.

### Theme 3: Module-Level Mutable Singletons

**Description:** At least 5 distinct module-level mutable singletons exist across the subsystem (`_SESSIONS`, `_GLOBAL_STATE_HOLDER`, `_TREE_CACHE`, `_TREE_CACHE_EVICTIONS`, import-time `CACHE_REGISTRY` registrations). These create hidden global state that complicates testing, prevents dependency injection, and risks state leakage between test cases.

**Root cause:** Process-level caching (parse trees, compiled queries) requires shared mutable state. The current pattern uses the simplest possible approach (module-level dicts/objects) without abstraction.

**Affected principles:** P12, P23, P16

**Suggested approach:** Introduce a `TreeSitterRuntimeContext` (or extend the existing `RustLaneRuntimeDepsV1` pattern) that aggregates all mutable runtime state into an explicit, injectable object. Provide a `get_default_context()` for production use and allow tests to create isolated contexts. Move `CACHE_REGISTRY` registrations from import-time to an explicit `initialize()` function.

### Theme 4: The `runtime_core.py` God Module

**Description:** `rust_lane/runtime_core.py` at 1,209 lines is the single largest file in the subsystem and has the highest fan-in and fan-out. It defines 7 internal dataclass types, 20+ functions spanning 8 responsibilities, and imports from 20+ modules. Two other modules (`query_orchestration.py`, `payload_assembly.py`) use `__dict__` introspection to access its private functions, indicating that it contains functionality that should be public but is trapped in a monolithic module.

**Root cause:** The Rust enrichment pipeline is complex (parse, resolve node, extract scope, run extractors, execute query packs, collect facts, build injection plans, finalize timing). All of this was assembled in one module as features were added.

**Affected principles:** P1, P2, P3, P4, P5, P14, P19

**Suggested approach:** This is the single highest-impact structural improvement. Decompose into 5 focused modules (availability, scope utils, extractor dispatch, query pack execution, pipeline orchestration) as detailed in P2. Keep `runtime_core.py` as a thin re-export facade for backward compatibility with the `runtime.py` re-export module.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 | Consolidate `_extract_provenance` into single copy in `language_registry.py`; remove from `planner.py:22` and `node_schema.py:92` | small | Eliminates 3-way duplication of grammar introspection logic |
| 2 | P7 | Remove `_build_node_id` from `structural/exports.py:23`; import `build_node_id` from `structural/export.py:20` | small | Eliminates duplicate node ID construction |
| 3 | P21 | Rename `fallback_support.py` exported functions to drop underscore prefix; update callers | small | API surface follows Python conventions |
| 4 | P22 | Migrate `TreeSitterInputEditV1` from `@dataclass` to `CqStruct` at `contracts/core_models.py:131` | small | Consistent contract base class |
| 5 | P11 | Split `load_query_pack_sources()` into pure loader + separate drift state setter at `registry.py:206-210` | small | CQS compliance; callers can load without side effects |

## Recommended Action Sequence

1. **Consolidate `_extract_provenance` (P7)** -- Small, zero-risk change. Remove duplicates in `query/planner.py:22-40` and `schema/node_schema.py:92-109`; import from `core/language_registry.py:84-96`. Update callers to use the canonical import.

2. **Remove `_build_node_id` duplicate (P7)** -- Small change. Remove `structural/exports.py:23-35`; import `build_node_id` from `structural/export.py:20` in `exports.py`.

3. **Fix `fallback_support.py` naming (P21)** -- Small change. Rename exported functions to drop underscore prefix. Update importers in `python_lane/facts.py` and `python_lane/runtime.py`.

4. **Fix `__dict__` access pattern (P1, P5, P14)** -- Medium change. Make `_collect_query_pack_captures` and `_collect_query_pack_payload` public in `runtime_core.py` or extract to `rust_lane/query_pack_execution.py`. Replace `__dict__` lookups in `query_orchestration.py:41` and `payload_assembly.py:27` with normal imports.

5. **Migrate `TreeSitterInputEditV1` to `CqStruct` (P22)** -- Small change. Update `contracts/core_models.py:131` from `@dataclass(frozen=True, slots=True)` to `CqStruct(frozen=True)`.

6. **Split `load_query_pack_sources` CQS violation (P11)** -- Small change. Extract drift state mutation from `registry.py:206-216` into a separate function.

7. **Extract shared `_pack_source_rows` (P7)** -- Medium change. Create parameterized factory in `query/registry.py` or `core/query_pack_pipeline.py` that both Python and Rust lanes call.

8. **Decompose `runtime_core.py` (P2, P3, P4, P19)** -- Large change, depends on items 1-4. Split into 5 focused modules per the P2 suggested improvement. This is the highest-impact structural change and should be done after the smaller cleanups are in place to reduce merge conflicts.

9. **Extract shared `_collect_query_pack_captures` (P7)** -- Medium change, depends on item 8. Unify the Python (`facts.py:264`) and Rust (`runtime_core.py:707`) implementations into a generic pipeline in `core/query_pack_pipeline.py`.

10. **Replace `dict[str, object]` payload pattern (P10)** -- Medium change, depends on items 8-9. Define typed payload structs for Python and Rust enrichment outputs. Update the pipeline to construct structs instead of mutating dicts.

11. **Introduce `TreeSitterRuntimeContext` (P12, P23)** -- Medium change, depends on item 8. Aggregate mutable singletons into an injectable context object. Guard import-time side effects behind lazy initialization.
