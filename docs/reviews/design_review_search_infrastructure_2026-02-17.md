# Design Review: Search Infrastructure

**Date:** 2026-02-17
**Scope:** `tools/cq/search/tree_sitter/`, `tools/cq/search/_shared/`, `tools/cq/search/rg/`, `tools/cq/search/cache/`, `tools/cq/search/objects/`
**Focus:** All principles (1-24)
**Depth:** moderate
**Files reviewed:** 22 of 82 (entry points, contracts, core runtime, lanes, adapters, cross-boundary interfaces)

## Executive Summary

The search infrastructure layer demonstrates strong contract discipline and deliberate architectural layering. The `tree_sitter/contracts/` sub-package provides a well-versioned, msgspec-based contract surface that cleanly separates serializable payloads from runtime handles. The language lane architecture (Python lane, Rust lane) shares core runtime primitives through `core/` while allowing lane-specific specialization. The primary design weaknesses are: (1) heavy reliance on `dict[str, object]` as an intermediate payload type in the enrichment pipeline and object resolution layer, which undermines the otherwise strong contract system; (2) module-level mutable singletons for parse sessions and cache registries that create hidden coupling and reduce testability; (3) near-identical code paths in the Python and Rust canonicalization functions that represent duplicated knowledge. Overall alignment is solid (average ~2.3/3), with most principles well-satisfied and a small number of concentrated improvements that would yield significant gains.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `_shared/core.py` exposes `_RUNTIME_ONLY_ATTR_NAMES` frozenset as module-level detail |
| 2 | Separation of concerns | 2 | medium | medium | `rust_lane/runtime_core.py` mixes IO timing instrumentation with core enrichment logic |
| 3 | SRP | 2 | medium | low | `_shared/core.py` (431 LOC) handles helpers, contracts, requests, timeouts, runtime handles |
| 4 | High cohesion, low coupling | 2 | medium | medium | `objects/resolve.py` couples to pipeline, enrichment, core, and tree_sitter contracts |
| 5 | Dependency direction | 2 | small | low | `rg/adapter.py` imports from `search/pipeline/profiles` -- infrastructure depends on pipeline |
| 6 | Ports & Adapters | 2 | medium | low | `rg/runner.py` subprocess interaction is well-isolated but not behind a protocol |
| 7 | DRY (knowledge) | 1 | small | medium | Python/Rust lane payload canonicalization functions are near-identical |
| 8 | Design by contract | 3 | - | - | Excellent: `QueryWindowV1.__post_init__` validates invariants; versioned contracts throughout |
| 9 | Parse, don't validate | 2 | medium | medium | `objects/resolve.py` repeatedly extracts and re-validates `dict[str, object]` payloads |
| 10 | Make illegal states unrepresentable | 2 | medium | medium | `enrichment_status` is a bare `str` rather than a Literal or enum |
| 11 | CQS | 3 | - | - | Functions are cleanly separated into queries and commands |
| 12 | DI + explicit composition | 1 | medium | medium | Module-level singletons (`_SESSIONS`, `CACHE_REGISTRY`) bypass DI |
| 13 | Composition over inheritance | 3 | - | - | Minimal inheritance; composition and protocols used throughout |
| 14 | Law of Demeter | 2 | small | low | `objects/resolve.py` deep-dives into nested payload dicts |
| 15 | Tell, don't ask | 2 | medium | low | Multiple functions extract-then-decide on `dict[str, object]` contents |
| 16 | Functional core, imperative shell | 2 | medium | medium | `runtime_core.py` interleaves `time.perf_counter()` with pure enrichment transforms |
| 17 | Idempotency | 3 | - | - | Parsing and enrichment are stateless given same inputs; cache is additive |
| 18 | Determinism | 3 | - | - | `source_hash` uses BLAKE2; query compilation is LRU-cached; sorts are explicit |
| 19 | KISS | 2 | small | low | `_combined_progress_callback` has redundant null checks after narrowing |
| 20 | YAGNI | 3 | - | - | No speculative abstractions observed; features are demand-driven |
| 21 | Least astonishment | 2 | small | low | `canonicalize_*_lane_payload` returns `dict[str, Any]` not the typed struct |
| 22 | Public contracts | 3 | - | - | All contracts are versioned (`V1`), frozen, with explicit `__all__` exports |
| 23 | Testability | 2 | medium | medium | Module-level singletons require monkeypatching; no DI seam for parse sessions |
| 24 | Observability | 2 | small | low | Logging is present but `runtime_core.py` telemetry is ad-hoc dict construction |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The `tree_sitter/contracts/` sub-package provides an excellent public surface with 30+ versioned structs exported through `contracts/__init__.py`. Internal implementation details (e.g., `_BoundedQueryStateV1`, `_BoundedQueryRequestV1`) are correctly underscore-prefixed.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/_shared/core.py:30-44`: `_RUNTIME_ONLY_ATTR_NAMES` is a module-level frozenset that encodes a policy about which attribute names are "runtime-only." This policy is consumed by `has_runtime_only_keys()` and `assert_no_runtime_only_keys()`, which are public. The frozenset itself leaks an implementation detail (specific attribute names) that should be encapsulated behind the function interface alone.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/core/runtime.py:32-35`: The `try/except ImportError` pattern for `QueryCursor` is repeated in multiple files (`runtime.py:32`, `python_lane/runtime.py:58-65`, `language_registry.py:22-28`). Each file independently decides the fallback behavior for missing optional dependencies, making the "is tree-sitter available" decision a distributed secret.

**Suggested improvement:**
Consolidate the "is tree-sitter available" decision into a single capability-check module (partially done in `language_registry.py` but not universally consumed). Have `core/infrastructure.py` or a dedicated `core/availability.py` be the single authority for all optional dependency availability checks.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
The architecture cleanly separates contracts from runtime. The `core/` layer handles parsing and query execution; lanes handle language-specific enrichment; `_shared/` provides cross-cutting utilities.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/rust_lane/runtime_core.py:936-963`: `_collect_payload_with_timings` interleaves `time.perf_counter()` instrumentation directly into the enrichment payload construction. The timing concern is woven into the data transformation concern, making it impossible to test the enrichment logic without also triggering timing side effects.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/rust_lane/runtime_core.py:966-978`: `_finalize_enrichment_payload` similarly mixes canonicalization with timing measurement. The `stage_timings_ms` dict is constructed inline rather than being composed from a separate timing context.

**Suggested improvement:**
Extract timing instrumentation into a composable context or decorator (e.g., a `@timed_stage("query_pack")` decorator or a `StageTiming` context manager) that wraps the pure enrichment functions. The enrichment functions themselves would accept and return typed payloads without timing concerns.

**Effort:** medium
**Risk if unaddressed:** medium -- Makes it harder to test enrichment logic in isolation.

---

#### P3. SRP -- Alignment: 2/3

**Current state:**
Most modules have clear single responsibilities. `tree_sitter/contracts/core_models.py` defines contracts; `core/parse.py` handles incremental parsing; `core/runtime.py` handles bounded query execution.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/_shared/core.py:1-431`: This file has at least five distinct responsibilities: (1) shared helper functions (`line_col_to_byte_offset`, `source_hash`, `truncate`, `node_text`), (2) serializable contracts (`PythonNodeEnrichmentSettingsV1`, `PythonByteRangeEnrichmentSettingsV1`), (3) runtime-only handle containers (`PythonNodeRuntimeV1`, `PythonByteRangeRuntimeV1`), (4) request contracts (`RgRunRequest`, `CandidateCollectionRequest`, enrichment requests), (5) timeout wrappers (`search_sync_with_timeout`, `search_async_with_timeout`). This file changes for five independent reasons.

**Suggested improvement:**
Split `_shared/core.py` into focused modules: `_shared/helpers.py` (pure utility functions), `_shared/enrichment_contracts.py` (settings + runtime + request types), `_shared/rg_request.py` (already partially exists -- note there is an unused `_shared/rg_request.py` that appears to be a partial extraction), `_shared/timeout.py` (already partially exists as a separate file). The `_shared/__init__.py` re-exports would maintain backward compatibility.

**Effort:** medium
**Risk if unaddressed:** low -- Current state is manageable but accumulates complexity over time.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Within `tree_sitter/`, cohesion is high: `contracts/` groups all contract types, `core/` groups shared runtime primitives, lanes group language-specific code. Cross-sub-package coupling is moderate.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/objects/resolve.py:13-25`: This module imports from `enrichment.core`, `objects.render`, `pipeline.context_window`, and `pipeline.enrichment_contracts`. It depends on four sibling sub-packages, making it a high-fan-in coupling point. Any change to the enrichment payload format ripples through this module.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/rg/adapter.py:14`: `adapter.py` imports `INTERACTIVE` from `search/pipeline/profiles`, which is a pipeline-layer concern. Infrastructure (`rg/`) should not depend on pipeline.

**Suggested improvement:**
For `rg/adapter.py`, extract the `INTERACTIVE` and `DEFAULT` profile constants to `_shared/` or accept them as parameters. For `objects/resolve.py`, consider defining a `ResolvedPayloadView` protocol that the enrichment layer implements, rather than having `resolve.py` reach into raw `dict[str, object]` payloads from multiple sources.

**Effort:** medium
**Risk if unaddressed:** medium -- Current coupling means enrichment format changes require coordinated updates across four sub-packages.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The general direction flows inward: contracts are at the center, core runtime depends on contracts, lanes depend on core, and external callers depend on lanes. This is mostly correct.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/rg/adapter.py:14`: `from tools.cq.search.pipeline.profiles import DEFAULT, INTERACTIVE` -- The `rg/` adapter layer (infrastructure) depends on `pipeline/` (orchestration). Dependency should flow the other direction.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/query/registry.py:13`: `from tools.cq.search.tree_sitter.query.drift import build_grammar_drift_report` -- The registry (data loading) depends on drift detection (validation). This is acceptable but creates a mild circular concern where loading always triggers validation.

**Suggested improvement:**
Move `DEFAULT` and `INTERACTIVE` profile constants from `pipeline/profiles.py` to `_shared/types.py` or a new `_shared/profiles.py`. They represent infrastructure-level configuration, not pipeline orchestration.

**Effort:** small
**Risk if unaddressed:** low -- The dependency inversion is localized to one import.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The `rg/` sub-package acts as an adapter for the ripgrep subprocess. The `tree_sitter/core/` layer wraps the tree-sitter C library bindings. The `cache/registry.py` provides a basic port for cache lifecycle.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/rg/runner.py:238-276`: The `run_rg_json` function directly calls `subprocess.Popen`, `process.communicate`, and `process.kill`. While this is acceptable for a concrete adapter, there is no protocol or abstraction that would allow testing the command-building logic without actually spawning a process. The `build_rg_command` function is testable in isolation, which partially mitigates this.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/core/adaptive_runtime.py:19-22`: `_resolve_backend` uses `getattr(backend, "get", None)` / `getattr(backend, "set", None)` as a duck-typed protocol. This is effectively an implicit port, but it would be clearer as an explicit `CacheBackend` protocol.

**Suggested improvement:**
Define a `CacheBackend` protocol in `_shared/` or `cache/contracts.py` with `get(key: str) -> object | None` and `set(key: str, value: object, *, expire: int, tag: str | None = None) -> None`. Have `adaptive_runtime.py` depend on the protocol rather than duck-typing.

**Effort:** medium
**Risk if unaddressed:** low -- Duck typing works but makes the implicit contract invisible.

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
Contract definitions are well-centralized. However, canonicalization logic and availability checks exhibit semantic duplication.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/contracts/lane_payloads.py:39-68`: `canonicalize_python_lane_payload` and `canonicalize_rust_lane_payload` are nearly identical functions. They share the same logic: (1) copy the payload dict, (2) pop legacy `tree_sitter_diagnostics` key, (3) coerce `cst_diagnostics` and `cst_query_hits` via `_coerce_mapping_rows`, (4) validate against the typed struct. The only difference is the target type (`PythonTreeSitterPayloadV1` vs `RustTreeSitterPayloadV1`). This is duplicated knowledge about the canonicalization protocol.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/python_lane/runtime.py:82-92` and `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/rust_lane/runtime_core.py:230-252`: Both lanes independently implement `is_tree_sitter_*_available()` with similar logic patterns (check language object, check Parser, check optional bindings). The availability-check pattern is the same; only the language name differs.

**Suggested improvement:**
For canonicalization: extract a generic `_canonicalize_lane_payload(payload: dict[str, Any], *, target_type: type) -> dict[str, Any]` that both language-specific functions delegate to. For availability: create `core/availability.py` with `is_tree_sitter_available(language: str) -> bool` that consolidates the pattern.

**Effort:** small
**Risk if unaddressed:** medium -- Drift between Python and Rust canonicalization could cause subtle inconsistencies in enrichment payloads.

---

#### P8. Design by contract -- Alignment: 3/3

**Current state:**
The contract system is exemplary. All public types use versioned suffixes (`V1`), frozen `msgspec.Struct` base classes, and explicit `__all__` declarations. `QueryWindowV1.__post_init__` validates byte-window ordering. `SearchLimits` uses constrained types (`PositiveInt`, `PositiveFloat`). The `assert_no_runtime_only_keys` function enforces the serialization boundary.

**Findings:**
No significant gaps. The contract discipline is one of the strongest aspects of this codebase.

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Boundary parsing is well-implemented for tree-sitter inputs (source bytes are parsed once into trees, then queried). However, the enrichment payload flow uses raw dicts that are repeatedly interrogated.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/objects/resolve.py:150-193`: `_payload_views` extracts six different view dicts from an `EnrichedMatch` by repeatedly calling `.get()` on `dict[str, object]` payloads and checking `isinstance`. This is validation-style extraction that happens deep in the pipeline rather than at the boundary.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/objects/resolve.py:526-536`: `_first_definition_target` navigates `payload.get("symbol_grounding") -> .get("definition_targets") -> isinstance check -> iterate`. This nested dict traversal pattern occurs frequently throughout `resolve.py` (at least 8 functions follow this pattern).

**Suggested improvement:**
Define typed structs for the enrichment payload views (e.g., `SymbolGroundingV1`, `ResolutionPayloadV1`, `StructuralPayloadV1`) and parse the raw `dict[str, object]` into these structs once at the `objects/resolve.py` entry point. The rest of `resolve.py` would operate on typed, validated data.

**Effort:** medium
**Risk if unaddressed:** medium -- Scattered validation increases the risk of inconsistent handling of missing or malformed fields.

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Many states are well-modeled: `QueryMode` is an enum, `QueryWindowV1` validates ordering, `EvictionPolicy` is a Literal type. However, some string-typed fields represent finite state spaces.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/contracts/lane_payloads.py:17`: `enrichment_status: str = "applied"` is a bare string. The valid values are `"applied"`, `"degraded"`, and `"skipped"` (observed in `python_lane/runtime.py:486,578`). This should be a `Literal["applied", "degraded", "skipped"]` to prevent typos and make the state space explicit.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/objects/render.py:36-37`: `resolution_quality: str = "weak"` on `ResolvedObjectRef` accepts any string, but the actual values are `"strong"`, `"medium"`, and `"weak"` (from `resolve.py:278,294,306,345`).

**Suggested improvement:**
Replace bare `str` fields with `Literal` types or dedicated enums for `enrichment_status`, `resolution_quality`, and `coverage_level`. This makes the state machine explicit and enables exhaustive matching.

**Effort:** medium
**Risk if unaddressed:** medium -- Typos in string literals produce silent incorrect behavior instead of type errors.

---

#### P11. CQS -- Alignment: 3/3

**Current state:**
Functions are well-separated into queries (e.g., `load_language_registry`, `build_rg_command`, `source_hash`) and commands (e.g., `clear_parse_session`, `record_runtime_sample`, `enqueue_windows`). No functions were observed that both return information and mutate state in surprising ways.

**Findings:**
`ParseSession.parse()` returns a result tuple and updates internal cache state, but this is appropriate for a cache-on-read pattern and the mutation (cache population) is expected by callers.

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 1/3

**Current state:**
Several module-level singletons manage runtime state without dependency injection seams.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/core/parse.py:270`: `_SESSIONS: dict[str, ParseSession] = {}` is a module-level mutable dict that acts as a global registry of parse sessions. `get_parse_session()` creates sessions on demand and stores them here. Tests cannot inject a mock session without monkeypatching this private dict.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/cache/registry.py:42`: `CACHE_REGISTRY = CacheRegistry()` is a module-level singleton. Multiple modules (`python/extractors.py`, `rust/enrichment.py`, `rust_lane/runtime_cache.py`, `rust_lane/query_cache.py`) register caches at import time via side effects (e.g., line 261: `CACHE_REGISTRY.register_cache("python", "python_enrichment:ast", _AST_CACHE)`).
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/core/adaptive_runtime.py:22`: `_resolve_backend()` falls back to `get_cq_cache_backend(root=Path.cwd())`, hardcoding the cache resolution strategy into the adaptive runtime module.

**Suggested improvement:**
Accept `ParseSession` and `CacheRegistry` instances as parameters in the functions that need them, with module-level functions providing default singleton access for convenience. For example, `enrich_python_context_by_byte_range` could accept an optional `parse_session: ParseSession | None = None` parameter, using the global singleton only as a default. This creates a natural DI seam for testing.

**Effort:** medium
**Risk if unaddressed:** medium -- Singleton state makes tests order-dependent and forces monkeypatching for isolation.

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
The codebase strongly prefers composition. `CqStruct` and `CqOutputStruct` base classes are used only for msgspec struct configuration, not for behavioral inheritance. Language lanes compose shared utilities from `core/` rather than inheriting from a base lane class. Protocols (`NodeLike`, `PythonClassifierSessionLike`, `RuntimeLanguage`) provide structural typing.

**Findings:**
No inheritance hierarchies beyond the structural `msgspec.Struct` base classes were observed. The `BoundedCache` generic class is standalone with no subclasses.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most module interactions go through direct collaborators. However, the `objects/resolve.py` module frequently navigates deeply into nested dict structures.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/objects/resolve.py:527-531`: `payload.get("symbol_grounding") -> .get("definition_targets") -> iterate -> row.get("file")` is a four-level chain through raw dicts. This pattern repeats across `_first_definition_target`, `_first_binding_candidate`, `_first_qualified_name_candidate`, `_resolved_import_path`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/objects/resolve.py:585-611`: `_resolved_import_path` navigates `semantic_payload.get("import_alias_resolution") -> .get("resolved_path")` and `resolution_payload.get("import_alias_chain") -> iterate -> row.get("module")` -- two separate four-level chains.

**Suggested improvement:**
Define accessor functions or typed view structs that encapsulate the navigation. For example, a `SymbolGroundingAccessor` that provides `.first_definition_target()` and `.definition_file()` methods, hiding the dict traversal.

**Effort:** small
**Risk if unaddressed:** low -- Primarily affects maintainability of `resolve.py`.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The enrichment payloads are "asked" for their contents by external code rather than being told to produce their own views.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/objects/resolve.py:240-256`: `_build_identity_seed` extracts data from views, then externally decides the `qualified_name`, `symbol`, and `kind`. The enrichment payload could instead expose a `build_identity_seed()` method that encapsulates this decision.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/objects/resolve.py:444-475`: `_coverage_for_match` inspects multiple payload dicts to determine coverage level, repeating the `isinstance(x, dict) and x` pattern. The match object itself could expose `.coverage_level()`.

**Suggested improvement:**
Where feasible, add computed-property methods to the enrichment payload contracts or view structs so that downstream code can "tell" the object to produce a result rather than "asking" for raw fields and deciding externally.

**Effort:** medium
**Risk if unaddressed:** low -- Current pattern works but concentrates logic in resolve.py rather than distributing it to data owners.

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The tree-sitter query execution (`core/runtime.py`) is largely functional: `run_bounded_query_captures` takes inputs and returns results with telemetry. Parsing (`core/parse.py`) correctly isolates the stateful session behind a clean interface.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/rust_lane/runtime_core.py:936-963`: `_collect_payload_with_timings` weaves `time.perf_counter()` calls around pure enrichment logic. The timing is an imperative concern mixed into a function that could be a pure transform.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/core/runtime.py:516`: `record_runtime_sample("query_captures", ...)` is a side effect (cache write) called at the end of the bounded query execution function, which is otherwise a pure query.

**Suggested improvement:**
Return timing data as part of the result tuple rather than calling `time.perf_counter()` inline. Have the caller (the imperative shell) wrap the pure transform with timing. For `record_runtime_sample`, move the call to the lane-level caller rather than embedding it in the core runtime function.

**Effort:** medium
**Risk if unaddressed:** medium -- Embedded side effects make it harder to test core query logic in isolation.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Enrichment operations are naturally idempotent: re-parsing the same source with the same byte offsets produces identical results. The `ParseSession` cache uses content-equality checks (`entry.source_bytes == source_bytes`) to detect unchanged files. `_match_fingerprint` deduplication in `run_bounded_query_matches` prevents duplicate results across windowed execution.

**Findings:**
No idempotency concerns. The `clear_parse_session` function is explicitly provided for session reset, and cache population is additive.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Determinism is well-enforced. `source_hash` uses BLAKE2 with a fixed digest size. `_JSON_ENCODER` is configured with `order="deterministic"`. Query compilation is LRU-cached by `(language, pack_name, source, request_surface, validate_rules)` tuple. Match deduplication uses structural fingerprints based on `(pattern_idx, sorted_capture_spans)`.

**Findings:**
`_local_query_pack_hash` at `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/query/registry.py:67-79` incorporates `st_mtime_ns` in the hash, which correctly detects file changes for cache invalidation without introducing nondeterminism in outputs.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most functions are straightforward. The bounded query execution in `core/runtime.py` is inherently complex (windowed execution with budget, containment, and deduplication) but the complexity is justified.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/core/runtime.py:108-133`: `_combined_progress_callback` has a redundant null check pattern. After line 113 establishes `callback = progress_callback if callable(progress_callback) else None`, line 129 checks `if callback is None: return True` inside the closure -- but the closure is only created when `callback is not None` (line 115-116 would have returned `None` otherwise). The inner null check is dead code.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/query/registry.py:171-224`: `load_query_pack_sources` has a complex flow: try stamped loader -> fall back to uncached -> build drift report -> conditionally fall back again on incompatibility. The control flow has three levels of fallback.

**Suggested improvement:**
Remove the redundant null check in `_combined_progress_callback`. For `load_query_pack_sources`, consider extracting the drift-check-and-fallback logic into a separate function to reduce the main function's cyclomatic complexity.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 3/3

**Current state:**
No speculative generality was observed. The `QueryPackProfileV1` exists because it has a concrete consumer (`load_query_pack_sources_for_profile`). The `BoundedCache` supports both FIFO and LRU because both policies are needed. The adaptive runtime exists because query budgets need tuning.

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Most APIs behave as expected. Naming is consistent and descriptive.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/contracts/lane_payloads.py:39-52`: `canonicalize_python_lane_payload` accepts `dict[str, Any]` and returns `dict[str, Any]`. A caller might expect it to return a `PythonTreeSitterPayloadV1` struct, since the function validates against it internally (line 51: `_ = msgspec.convert(payload, type=PythonTreeSitterPayloadV1, strict=False)`). The validation result is discarded, and the raw dict is returned. This is surprising: the function validates but does not parse.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/_shared/search_contracts.py:44-45`: `LanguagePartitionStats` has both `matches` and `total_matches` fields, and both `files_scanned` and `scanned_files` fields. The overlap is not immediately obvious to a new reader.

**Suggested improvement:**
For canonicalization: either return the typed struct (preferred) or rename to `validate_python_lane_payload` to set expectations. For `LanguagePartitionStats`: add docstring clarification explaining the distinction between `matches`/`total_matches` and `files_scanned`/`scanned_files`, or deprecate the redundant fields.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 3/3

**Current state:**
This is a strength of the codebase. All contracts use `V1` suffixes. `CqStruct`, `CqOutputStruct`, `CqCacheStruct`, and `CqSettingsStruct` base classes enforce frozen, slotted structs. Every module has an explicit `__all__` list. The `contracts/__init__.py` consolidates all exports with a complete `__all__`.

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Pure functions like `build_rg_command`, `_compute_input_edit`, `_split_window`, and all contract construction are directly testable. The `compile_query` function is LRU-cached, which aids performance but complicates test isolation.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/core/parse.py:270-285`: `_SESSIONS` is a module-level dict with no public accessor for test injection. Tests that need to verify parse session behavior must either: (a) call the real tree-sitter parser, or (b) monkeypatch `_SESSIONS`. Neither is ideal.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/core/adaptive_runtime.py:22`: `_resolve_backend` hardcodes `Path.cwd()` as the cache root. Tests running in different working directories get different cache backends.
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/python_lane/runtime.py:31-33`: `_parser_controls()` imports and calls `SettingsFactory.parser_controls()` at function call time (lazy import), creating a hidden dependency on global settings state.

**Suggested improvement:**
Add optional `parse_session` and `cache_backend` parameters to the enrichment entry points (`enrich_python_context_by_byte_range`, `enrich_rust_context_by_byte_range`). Default to the module-level singletons for production use, but allow tests to inject isolated instances. This is the standard "constructor injection with default" pattern.

**Effort:** medium
**Risk if unaddressed:** medium -- Test isolation requires monkeypatching, which couples tests to implementation details.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Logging is present throughout via `logging.getLogger(__name__)`. The `QueryExecutionTelemetryV1` struct captures windowed query execution metrics. `ParseSessionStatsV1` tracks cache hit/miss/reparse counters.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/rust_lane/runtime_core.py:863-890`: `_aggregate_query_runtime` constructs a telemetry dict by iterating over `query_telemetry.values()` and pulling fields with `.get()` and `isinstance` checks. This ad-hoc construction could benefit from using a typed struct (similar to `QueryExecutionTelemetryV1`).
- Observability data is consistently structured within `tree_sitter/` (typed telemetry structs), but the `rg/` sub-package relies solely on logging for observability with no structured telemetry emission.

**Suggested improvement:**
Define an `RgExecutionTelemetryV1` struct in `rg/contracts.py` that captures command, returncode, event count, and timed_out status in a structured form. This would align `rg/` observability with the `tree_sitter/` pattern.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: The `dict[str, object]` Payload Boundary Problem

**Root cause:** The enrichment pipeline produces `dict[str, object]` payloads at the tree-sitter lane level, and these untyped dicts flow through multiple layers (enrichment -> pipeline -> objects) without being parsed into typed structs. This contradicts the otherwise strong contract discipline.

**Affected principles:** P9 (Parse don't validate), P10 (Illegal states), P14 (Law of Demeter), P15 (Tell don't ask).

**Suggested approach:** Define typed "view" structs for the enrichment payload's major sub-sections (resolution, structural, agreement, semantic). Parse the `dict[str, object]` into these views once at the `objects/resolve.py` entry point. Downstream code operates on typed data. This is a medium effort with high impact across four principles.

### Theme 2: Module-Level Singleton State

**Root cause:** Several critical runtime components (`_SESSIONS`, `CACHE_REGISTRY`, `_resolve_backend`) use module-level mutable state without DI seams. This pattern was likely adopted for simplicity and is workable in production but creates test isolation challenges.

**Affected principles:** P12 (DI), P23 (Testability).

**Suggested approach:** Add optional parameters to top-level entry points that default to the existing singletons. This preserves backward compatibility while opening DI seams. Start with `ParseSession` (highest test impact), then `CacheRegistry`.

### Theme 3: Python/Rust Lane Symmetry

**Root cause:** The Python and Rust lanes follow the same enrichment protocol (parse -> query -> collect -> canonicalize) but implement it independently, leading to duplicated canonicalization logic and availability checks.

**Affected principles:** P7 (DRY).

**Suggested approach:** Extract shared lane protocol functions: `canonicalize_lane_payload(payload, target_type)`, `is_tree_sitter_available(language)`. The lanes retain their language-specific logic but delegate shared steps.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Unify `canonicalize_python_lane_payload` / `canonicalize_rust_lane_payload` into generic function | small | Eliminates duplicated canonicalization knowledge |
| 2 | P10 (Illegal states) | Replace `enrichment_status: str` with `Literal["applied", "degraded", "skipped"]` | small | Prevents silent typo bugs in status tracking |
| 3 | P5 (Dependency direction) | Move `DEFAULT`/`INTERACTIVE` profiles from `pipeline/profiles` to `_shared/` | small | Corrects inverted dependency between infrastructure and pipeline |
| 4 | P19 (KISS) | Remove dead null check in `_combined_progress_callback` | small | Reduces cognitive load in already-complex runtime module |
| 5 | P21 (Least astonishment) | Have canonicalize functions return typed struct, not raw dict | small | Aligns behavior with function name and internal validation |

## Recommended Action Sequence

1. **Unify lane canonicalization (P7):** Extract `_canonicalize_lane_payload(payload, target_type)` in `contracts/lane_payloads.py`. Both language-specific functions become one-line delegations. No external callers change.

2. **Tighten string-typed state fields (P10):** Replace `enrichment_status`, `resolution_quality`, and `coverage_level` bare strings with `Literal` types across contracts in `lane_payloads.py`, `objects/render.py`, and related modules.

3. **Fix dependency direction (P5):** Move `DEFAULT` and `INTERACTIVE` profile constants from `pipeline/profiles.py` to `_shared/profiles.py` (or `_shared/types.py`). Update imports in `rg/adapter.py` and `rg/runner.py`.

4. **Add DI seams for testability (P12, P23):** Add optional `parse_session` parameter to `enrich_python_context_by_byte_range` and `enrich_rust_context_by_byte_range`. Default to module-level singleton. This unblocks isolated testing without changing production behavior.

5. **Define enrichment payload view structs (P9, P14, P15):** Create typed structs for resolution, structural, and agreement views in a new `objects/payload_views.py`. Parse once at the `build_object_resolved_view` entry point. Refactor `resolve.py` internal functions to accept typed views.

6. **Split `_shared/core.py` (P3):** Distribute the 431-LOC file into focused modules: helpers, enrichment contracts, and timeout utilities. The existing `_shared/__init__.py` re-exports maintain compatibility.

7. **Separate timing from enrichment transforms (P2, P16):** Extract `time.perf_counter()` instrumentation from `runtime_core.py` enrichment functions into a composable timing wrapper. The enrichment functions become pure transforms.
