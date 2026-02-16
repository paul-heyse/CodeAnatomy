# Design Review: tools/cq/core/

**Date:** 2026-02-15
**Scope:** `tools/cq/core/` (all files, including `cache/`, `runtime/`, `renderers/`, `tests/`)
**Focus:** All principles (1-24)
**Depth:** moderate
**Files reviewed:** 20 of 72 total (high-fan-in files, public interfaces, and largest modules)

## Executive Summary

The `tools/cq/core/` subsystem is a well-structured backbone with strong contract-first design, clean hexagonal ports, and an excellent three-tier type system. Its primary strengths are deterministic serialization via msgspec, frozen immutable structs, and explicit boundary enforcement. The two most significant improvement areas are: (1) `report.py` at 1,773 LOC is a monolithic renderer that conflates formatting, enrichment, and orchestration concerns, and (2) the `DetailPayload` struct in `schema.py` exposes mutable mapping semantics (`__setitem__`) on what should be a value object, undermining the immutability guarantees of the broader contract system. Cache infrastructure is well-decomposed into 24 files but has duplicated NoopCacheBackend boilerplate relative to the CqCacheBackend protocol.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `DetailPayload.__setitem__` exposes mutability; `_SCHEDULER_STATE` uses mutable holder pattern |
| 2 | Separation of concerns | 1 | medium | med | `report.py` mixes rendering, enrichment orchestration, and worker dispatch |
| 3 | SRP (one reason to change) | 1 | medium | med | `report.py` changes for rendering, enrichment, scoring, and parallel execution reasons |
| 4 | High cohesion, low coupling | 2 | small | low | Core contracts are cohesive; `report.py` couples to `smart_search`, `classify_match` |
| 5 | Dependency direction | 2 | small | low | `front_door_insight.py` imports from `search/semantic/models.py` (outward dependency) |
| 6 | Ports & Adapters | 3 | - | - | Clean `ports.py` with Protocol-based ports; `CqCacheBackend` Protocol |
| 7 | DRY (knowledge) | 2 | small | low | `_coerce_float`/`_coerce_str` duplicated in `schema.py` and `scoring.py` |
| 8 | Design by contract | 3 | - | - | Annotated types (`PositiveInt`, `BoundedRatio`), `enforce_mapping_constraints`, `BoundaryDecodeError` |
| 9 | Parse, don't validate | 3 | - | - | `typed_boundary.py` provides `convert_strict`/`convert_lax`; boundary parsing is excellent |
| 10 | Illegal states unrepresentable | 2 | small | low | `DetailPayload` is mutable despite frozen parent types; `CqResult` uses `list` fields |
| 11 | CQS | 2 | small | low | `_maybe_attach_render_enrichment` mutates finding + returns nothing (command OK) but called inside query-like render |
| 12 | DI + explicit composition | 3 | - | - | `bootstrap.py` composition root; `SettingsFactory` explicit wiring |
| 13 | Composition over inheritance | 3 | - | - | No deep hierarchies; structs use composition; services are flat |
| 14 | Law of Demeter | 2 | small | low | `report.py:204-206` accesses `finding.details['impact_bucket']` via `__getitem__` chain |
| 15 | Tell, don't ask | 2 | small | low | `_format_finding_prefix` interrogates finding details instead of delegating to finding |
| 16 | Functional core, imperative shell | 2 | medium | med | `report.py` interleaves pure rendering with IO (file reads, process pool dispatch) |
| 17 | Idempotency | 3 | - | - | `assign_result_finding_ids` is deterministic; `build_*_insight` are pure transforms |
| 18 | Determinism | 3 | - | - | `JSON_ENCODER` uses `order="deterministic"`; cache keys are SHA-256 hashed |
| 19 | KISS | 2 | small | low | `front_door_insight.py` is clean at 1,172 LOC; `report.py` has unnecessary complexity |
| 20 | YAGNI | 3 | - | - | No speculative abstractions; cache interface has many methods but all are used |
| 21 | Least astonishment | 2 | small | low | `DetailPayload.__setitem__` on a struct is surprising; `Finding.__post_init__` mutation |
| 22 | Declare and version contracts | 3 | - | - | `schema_version` fields; `__all__` exports; `CqStrictOutputStruct` with `forbid_unknown_fields` |
| 23 | Design for testability | 2 | small | low | Worker scheduler uses process-global singleton; `report.py` hard to unit test |
| 24 | Observability | 2 | small | low | `cache/telemetry.py` exists; `render_enrichment_metrics` tracked; no structured logging in core |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Most modules maintain clean information hiding through `__all__` exports and internal `_`-prefixed helpers. The contract base types in `structs.py:8-47` use `frozen=True` consistently. However, `DetailPayload` in `schema.py:40-167` breaks this by exposing `__setitem__` and `__getitem__` on a struct that should be a value object.

**Findings:**
- `tools/cq/core/schema.py:124-133` -- `DetailPayload.__setitem__` allows mutation of `kind`, `score`, and `data` fields, contradicting the frozen-struct convention used everywhere else in the contract system. Callers can mutate payloads after construction.
- `tools/cq/core/runtime/worker_scheduler.py:162-170` -- `_SchedulerState` uses a mutable holder pattern for the process-global singleton. While functional, internal state (`_cpu_pool`, `_io_pool`) is accessible via the returned `WorkerScheduler` instance's `cpu_pool()` and `io_pool()` methods.

**Suggested improvement:**
Replace `DetailPayload.__setitem__` with a builder pattern or `msgspec.structs.replace()` usage at call sites. This aligns `DetailPayload` with the immutability guarantees of the rest of the contract system.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`report.py` at 1,773 LOC is the primary concern-mixing hotspot. It simultaneously handles: markdown formatting, render-time enrichment orchestration (lines 785-838), parallel worker dispatch (lines 973-1001), file IO for context snippets (lines 763-775), and summary compaction (lines 1410-1439). These are at least four distinct concerns.

**Findings:**
- `tools/cq/core/report.py:785-838` -- `_compute_render_enrichment_payload_from_anchor` imports `RawMatch`, `build_finding`, and `classify_match` from `tools.cq.search.pipeline.smart_search`. This creates a circular conceptual dependency: core depends on search pipeline for rendering.
- `tools/cq/core/report.py:973-1001` -- `_populate_render_enrichment_cache` manages parallel worker dispatch with fallback-to-sequential logic, which is infrastructure orchestration unrelated to report formatting.
- `tools/cq/core/report.py:1410-1439` -- `compact_summary_for_rendering` mixes data transformation (splitting summary) with rendering policy decisions.

**Suggested improvement:**
Extract three modules from `report.py`: (1) `render_enrichment.py` for enrichment orchestration and worker dispatch (lines 785-1062), (2) `render_summary.py` for summary compaction and status derivation (lines 1410-1593), and (3) keep `report.py` as a pure markdown renderer that consumes pre-enriched data. This would reduce `report.py` from 1,773 to approximately 700 LOC.

**Effort:** medium
**Risk if unaddressed:** medium -- Any change to enrichment logic, summary compaction, or markdown formatting requires touching the same 1,773-line file.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`report.py` changes for at least four reasons: (1) markdown output format changes, (2) enrichment pipeline changes, (3) worker scheduling policy changes, (4) summary compaction rules changes. `front_door_insight.py` is better -- it changes primarily for insight schema reasons, with rendering being a minor secondary concern.

**Findings:**
- `tools/cq/core/report.py` -- Changes for rendering format, enrichment strategy, worker dispatch, and summary compaction. The file has been modified in recent commits alongside both search pipeline changes and rendering changes.
- `tools/cq/core/front_door_insight.py:189-205` -- `render_insight_card` is a rendering concern embedded in a schema-definition module. Minor SRP violation but acceptable given its small size (17 lines).

**Suggested improvement:**
Same as P2 extraction. Additionally, move `render_insight_card` to a dedicated `render_insight.py` or into the existing `renderers/` subdirectory.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The contract modules (`schema.py`, `structs.py`, `contracts.py`, `contracts_constraints.py`, `contract_codec.py`, `typed_boundary.py`) form a highly cohesive contract core. The cache subsystem (`cache/`) is well-decomposed into 24 files with clear responsibility boundaries. The coupling hotspot is `report.py`.

**Findings:**
- `tools/cq/core/report.py:799` -- Imports `RawMatch`, `build_finding`, `classify_match` from `tools.cq.search.pipeline.smart_search`. This couples core rendering to the search pipeline implementation.
- `tools/cq/core/report.py:28` -- Imports `is_applicability_not_applicable` from `tools.cq.search.objects.render`. Core module depends on a search-specific renderer utility.

**Suggested improvement:**
Inject an enrichment callback into the render function rather than importing search pipeline internals directly. The callback could have the signature `Callable[[Path, str, int, int, str, list[str]], dict[str, object]]`, matching the existing `_compute_render_enrichment_payload_from_anchor` contract.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Core contracts generally depend on nothing outside core. The dependency direction is mostly correct: search/query/calls depend on core, not the reverse. Two exceptions exist.

**Findings:**
- `tools/cq/core/front_door_insight.py:19-23` -- Imports `SemanticContractStateInputV1`, `SemanticStatus`, `derive_semantic_contract_state` from `tools.cq.search.semantic.models`. This makes the core insight schema depend on the search subsystem's semantic models.
- `tools/cq/core/report.py:799` -- Imports search pipeline internals (`smart_search.RawMatch`, `classify_match`, `build_finding`).

**Suggested improvement:**
Move `SemanticContractStateInputV1`, `SemanticStatus`, and `derive_semantic_contract_state` from `tools/cq/search/semantic/models.py` into `tools/cq/core/` (e.g., a `semantic_contracts.py`), since they are used by the core insight schema. This restores proper inward dependency direction.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 3/3

**Current state:**
Excellent hexagonal architecture. `ports.py` defines `SearchServicePort`, `EntityServicePort`, `CallsServicePort`, and `CachePort` as `Protocol` classes. `cache/interface.py` defines `CqCacheBackend` as a `Protocol` with `NoopCacheBackend` as a null adapter. `services.py` provides concrete implementations. `bootstrap.py` wires everything in a composition root.

**Findings:**
No significant gaps. The port-adapter pattern is cleanly implemented.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge) -- Alignment: 2/3

**Current state:**
Most knowledge is well-centralized. The scoring thresholds live in `scoring.py`, risk thresholds in `front_door_insight.py`, and cache namespace TTLs in `cache/namespaces.py`. Two duplication instances exist.

**Findings:**
- `tools/cq/core/schema.py:48-53` and `tools/cq/core/scoring.py:231-236` -- Both define `_coerce_float` and `_coerce_str` helper functions with identical logic. The `DetailPayload` class has static versions; `scoring.py` has local versions.
- `tools/cq/core/front_door_insight.py:969-971` and `tools/cq/core/scoring.py:157-174` -- Bucket classification logic (`_max_bucket` in insight, `bucket()` in scoring) encodes the same thresholds (0.7/high, 0.4/med, low) but in different representations. The insight version uses string comparison; the scoring version uses float comparison.

**Suggested improvement:**
Extract `_coerce_float`/`_coerce_str` into a shared utility (e.g., `tools/cq/core/type_coercion.py`). Reuse `scoring.bucket()` from `front_door_insight._max_bucket` or vice versa.

**Effort:** small
**Risk if unaddressed:** low -- Drift risk if thresholds change in one place but not the other.

---

#### P8. Design by contract -- Alignment: 3/3

**Current state:**
Excellent. `contracts_constraints.py` defines `PositiveInt`, `NonNegativeInt`, `PositiveFloat`, `NonEmptyStr`, `BoundedRatio` as `Annotated` types with `msgspec.Meta` validation. `enforce_mapping_constraints` validates key counts and string lengths. `typed_boundary.py` provides `BoundaryDecodeError` taxonomy and strict/lax conversion functions. `Anchor.line` uses `Annotated[int, msgspec.Meta(ge=1)]`.

**Findings:**
No significant gaps.

**Effort:** -
**Risk if unaddressed:** -

---

#### P9. Parse, don't validate -- Alignment: 3/3

**Current state:**
`typed_boundary.py` exemplifies this principle with `convert_strict`, `convert_lax`, `decode_json_strict`, `decode_toml_strict`, and `decode_yaml_strict`. Messy inputs (dicts, JSON bytes, TOML payloads) are converted to typed structs at the boundary, and the rest of the code operates on well-formed values.

**Findings:**
No significant gaps.

**Effort:** -
**Risk if unaddressed:** -

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Frozen structs prevent many illegal states. `InsightSource`, `Availability`, `NeighborhoodSource`, `RiskLevel` use `Literal` types. However, `DetailPayload` and `CqResult` allow mutable states.

**Findings:**
- `tools/cq/core/schema.py:40` -- `DetailPayload(msgspec.Struct, omit_defaults=True)` is NOT frozen. It supports `__setitem__`, allowing post-construction mutation. This means a `DetailPayload` can represent states that violate its own invariants (e.g., `score` inconsistent with `data`).
- `tools/cq/core/schema.py:331` -- `CqResult(msgspec.Struct)` uses mutable `list` fields (`key_findings`, `evidence`, `sections`, `artifacts`). This is intentional for builder-pattern assembly but means a `CqResult` can be partially constructed.
- `tools/cq/core/schema.py:253-256` -- `Finding.__post_init__` mutates `self.details` from `dict` to `DetailPayload`. This side-effect during construction is a hidden state transition.

**Suggested improvement:**
Consider making `DetailPayload` frozen and using a builder function instead of `__setitem__`. For `CqResult`, document the "builder phase" vs "sealed phase" lifecycle explicitly, or provide a `seal()` method that converts to a frozen variant.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions are pure queries or explicit commands. The scoring functions (`impact_score`, `confidence_score`, `bucket`) are pure. The `build_*_insight` functions are pure transforms. The main CQS violation is in rendering.

**Findings:**
- `tools/cq/core/report.py:1083-1112` -- `_maybe_attach_render_enrichment` is a command (mutates `finding.details`) called inside `_format_finding` (line 176-177), which looks like a query (returns a string). The mutation is a side effect hidden inside rendering.
- `tools/cq/core/schema.py:442-465` -- `assign_result_finding_ids` mutates findings in-place (setting `stable_id`, `execution_id`, `id_taxonomy`) AND returns the result. This is a mixed command-query.

**Suggested improvement:**
For `_maybe_attach_render_enrichment`, move the enrichment attachment to a separate pre-processing pass before rendering begins (partially done via `_precompute_render_enrichment_cache`, but the fallback path still mutates during render). For `assign_result_finding_ids`, either return a new result (pure) or return `None` (pure command).

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 3/3

**Current state:**
`bootstrap.py` is a clean composition root. `CqRuntimeServices` is assembled from `SearchService`, `EntityService`, `CallsService`, `CqCacheBackend`, and `RuntimeExecutionPolicy`. `SettingsFactory` provides explicit factory methods. `resolve_runtime_services` manages workspace-scoped singletons.

**Findings:**
No significant gaps. The composition root pattern is well-executed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No deep inheritance hierarchies. The struct base types (`CqStruct`, `CqSettingsStruct`, `CqOutputStruct`, `CqStrictOutputStruct`, `CqCacheStruct`) use single-level inheritance from `msgspec.Struct` with configuration-only parent purpose. Services use static methods, not inheritance. `WorkerScheduler` is composed of pools, not inherited from executor.

**Findings:**
No significant gaps.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code respects LoD. The main violations occur in `report.py` where deep attribute access patterns are used for finding details.

**Findings:**
- `tools/cq/core/report.py:204-205` -- `finding.details['impact_bucket']` and `finding.details['confidence_bucket']` reach through `Finding.details` to access score attributes. This chains through the `DetailPayload.__getitem__` proxy.
- `tools/cq/core/multilang_orchestrator.py:380` -- `finding.details.data.get("language", "python")` chains through `Finding.details.data`. Three levels of navigation.

**Suggested improvement:**
Add helper methods on `Finding` or `DetailPayload` for commonly accessed properties. For example, `finding.language` or `finding.impact_bucket` as computed properties.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The insight builder functions (`build_search_insight`, `build_calls_insight`, `build_entity_insight`) are well-designed tell-style APIs: you provide a request, they produce the result. The scoring module follows tell-style: provide signals, get score. The rendering code is more ask-heavy.

**Findings:**
- `tools/cq/core/report.py:195-207` -- `_format_finding_prefix` interrogates `finding.details` to determine what prefix to use, then assembles the prefix externally. The finding could own its prefix representation.
- `tools/cq/core/report.py:236-250` -- `_extract_enrichment_payload` interrogates `finding.details` through multiple keys (`"enrichment"`, `"python_enrichment"`, `"rust_tree_sitter"`, `"language"`) to reconstruct enrichment state. This knowledge about enrichment data layout is duplicated outside the enrichment system.

**Suggested improvement:**
Consider adding a `Finding.enrichment_payload` computed property or a standalone `extract_enrichment_from_finding(finding)` function in the enrichment module rather than in the renderer.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The schema definitions, scoring, insight builders, and typed boundary converters form a solid functional core. The imperative shell includes `bootstrap.py`, `worker_scheduler.py`, and cache backends. The problem is `report.py`, which interleaves pure formatting with imperative IO.

**Findings:**
- `tools/cq/core/report.py:763-775` -- `_read_line_text` performs filesystem IO (reading source files) inside the rendering pipeline.
- `tools/cq/core/report.py:823-834` -- `classify_match` call inside rendering triggers AST analysis, which is a heavyweight computation with potential IO side effects.
- `tools/cq/core/report.py:981-987` -- Worker pool submission and bounded collection are imperative orchestration embedded in the render pipeline.

**Suggested improvement:**
Separate rendering into two phases: (1) an imperative "prepare" phase that pre-computes all enrichment data, reading files and running classify_match, and (2) a pure "format" phase that takes the pre-computed data and produces markdown. The existing `_precompute_render_enrichment_cache` is a step in this direction but the fallback path (`_maybe_attach_render_enrichment`) still runs IO during formatting.

**Effort:** medium
**Risk if unaddressed:** medium -- Mixing IO with formatting makes the renderer difficult to test and reason about.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
`assign_result_finding_ids` produces deterministic IDs from content hashes, making re-assignment idempotent. Cache operations use content-addressed keys. Insight builders produce the same output for the same input. Worker scheduler initialization is guarded by locks with check-then-act patterns.

**Findings:**
No significant gaps.

**Effort:** -
**Risk if unaddressed:** -

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Excellent. `JSON_ENCODER` uses `order="deterministic"`. Cache keys use `sha256` hashing of msgpack-encoded payloads. `build_cache_key` in `key_builder.py` produces deterministic keys. Finding IDs use `stable_digest24`. Merge ordering in `multilang_orchestrator.py` uses explicit sort keys with tie-breaking by file/line/col.

**Findings:**
No significant gaps.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are appropriately simple. `structs.py` is 48 LOC. `typed_boundary.py` is 126 LOC. `scoring.py` is 252 LOC. The complexity concentrations are `report.py` (1,773 LOC) and `front_door_insight.py` (1,172 LOC). The front_door module is complex but justified by its role. `report.py` has unnecessary complexity from mixing concerns.

**Findings:**
- `tools/cq/core/report.py` -- 1,773 LOC with 50+ functions. The `_format_finding` function alone has 5 parameters including 2 optional caches. The rendering pipeline involves 3 levels of enrichment attachment (precompute, cache lookup, fallback compute).
- `tools/cq/core/cache/interface.py` -- `NoopCacheBackend` at 165 LOC duplicates every method signature from `CqCacheBackend` Protocol (176 LOC). The class could be auto-generated or simplified to a single `__getattr__` delegation.

**Suggested improvement:**
After extracting concerns from `report.py`, the remaining renderer should be significantly simpler. For `NoopCacheBackend`, consider using a factory function that returns a minimal implementation.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 3/3

**Current state:**
No speculative abstractions detected. The five struct base types in `structs.py` each serve a distinct purpose. Cache namespaces map to actual usage. Environment variable overrides have documented use cases. `CachePort` in `ports.py` is minimal (get/set only).

**Findings:**
No significant gaps.

**Effort:** -
**Risk if unaddressed:** -

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Most APIs behave as expected. `convert_strict` raises `BoundaryDecodeError`, not raw msgspec errors. `build_search_insight` takes a typed request and returns a typed insight. Two surprises exist.

**Findings:**
- `tools/cq/core/schema.py:124-133` -- `DetailPayload.__setitem__` on a `msgspec.Struct` is surprising. Structs are typically value objects; supporting dict-style mutation breaks that expectation. The `from_legacy` and `to_legacy_dict` methods reinforce the mapping facade but this is a confusing hybrid.
- `tools/cq/core/schema.py:253-256` -- `Finding.__post_init__` silently converts `dict` details to `DetailPayload`. This means `Finding(category="x", message="y", details={"kind": "z"})` silently transforms the `details` argument. Callers might not expect this coercion.

**Suggested improvement:**
For `DetailPayload`, deprecate `__setitem__` in favor of explicit builder functions. For `Finding.__post_init__`, document the coercion prominently or require callers to pass `DetailPayload` instances explicitly.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version contracts -- Alignment: 3/3

**Current state:**
Excellent. `FrontDoorInsightV1` has `schema_version = "cq.insight.v1"`. `CqResult` references `SCHEMA_VERSION`. `SummaryEnvelopeV1` uses `CqStrictOutputStruct` with `forbid_unknown_fields=True`. All modules have `__all__` exports declaring the public surface. Schema evolution notes in `schema.py:17-20` document compatibility rules.

**Findings:**
No significant gaps.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
The contract types, scoring, and insight builders are highly testable -- pure functions with typed inputs and outputs. The runtime services use DI via the composition root. The testability gaps are in singleton-dependent and IO-heavy code.

**Findings:**
- `tools/cq/core/runtime/worker_scheduler.py:173-179` -- `get_worker_scheduler()` is a process-global singleton. Tests must use `close_worker_scheduler()` for cleanup, and cannot easily inject a test-specific policy without monkeypatching the environment.
- `tools/cq/core/report.py:785-838` -- `_compute_render_enrichment_payload_from_anchor` imports and calls search pipeline functions at call time, making it impossible to unit test the renderer without the full search stack available.
- `tools/cq/core/report.py:763-775` -- `_read_line_text` performs direct filesystem reads, making render tests require actual files on disk.

**Suggested improvement:**
For `get_worker_scheduler`, add an optional `policy` parameter to allow test injection: `get_worker_scheduler(policy: ParallelismPolicy | None = None)`. For `report.py`, inject the enrichment callback and file-reading function as parameters.

**Effort:** small
**Risk if unaddressed:** low

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
`cache/telemetry.py` provides `record_cache_get`, `record_cache_set`, and `record_cache_decode_failure` for cache hit/miss tracking. `report.py` tracks `render_enrichment_attempted/applied/failed/skipped` metrics. `InsightDegradationV1` provides structured degradation reporting. Missing: structured logging in core modules and consistent telemetry across all operations.

**Findings:**
- `tools/cq/core/report.py:1048-1061` -- Render enrichment metrics are computed but only attached to the summary dict, not emitted as structured telemetry events.
- `tools/cq/core/runtime/worker_scheduler.py` -- No logging or metrics for pool creation, task submission, timeouts, or pool saturation. The `collect_bounded` method silently cancels timed-out futures with no telemetry.

**Suggested improvement:**
Add structured diagnostic events (using the existing `DegradeEventV1` pattern from `snb_schema.py`) for worker pool timeouts and cache misses. These could be collected and attached to the result's diagnostic artifacts.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: report.py as a God Module

**Description:** `report.py` at 1,773 LOC is the largest file in core and the most frequently modified. It conflates four concerns: markdown formatting, render-time enrichment orchestration, parallel worker dispatch, and summary compaction. This is the root cause of violations in P2 (separation of concerns), P3 (SRP), P4 (coupling to search pipeline), P16 (functional core/imperative shell mixing), and P23 (testability).

**Root cause:** The renderer was incrementally extended with enrichment capabilities and parallel execution rather than being decomposed when these concerns were added.

**Affected principles:** P2, P3, P4, P5, P11, P16, P23

**Suggested approach:** Extract `render_enrichment.py` and `render_summary.py` from `report.py`, injecting enrichment as a callback parameter. This single refactor addresses 7 principle violations.

### Theme 2: DetailPayload Mutability Anomaly

**Description:** `DetailPayload` is the only mutable struct in the entire contract system. It supports `__setitem__`, `__getitem__`, and `__contains__` as a mapping facade, plus `from_legacy` and `to_legacy_dict` for backward compatibility. This breaks the immutability contract that all other structs enforce.

**Root cause:** Legacy compatibility -- `DetailPayload` was originally a plain `dict`, and the mapping protocol was added during the structured migration to avoid breaking all call sites.

**Affected principles:** P1, P10, P11, P21

**Suggested approach:** Audit all `DetailPayload.__setitem__` call sites. If they occur only during construction, replace with builder-pattern construction. If they occur during processing, refactor to use `msgspec.structs.replace` or produce new instances. Deprecate `__setitem__` once call sites are migrated.

### Theme 3: Outward Dependencies from Core

**Description:** Two files in core import from the search subsystem: `front_door_insight.py` imports semantic models from `search/semantic/models.py`, and `report.py` imports `classify_match` and `RawMatch` from `search/pipeline/smart_search.py`. This inverts the expected dependency direction where search depends on core.

**Root cause:** The semantic contract state machine was originally developed inside the search subsystem but is now needed by the core insight schema. The render-time enrichment feature was added to report.py without introducing a proper abstraction boundary.

**Affected principles:** P4, P5

**Suggested approach:** Move the 3 semantic contract types from `search/semantic/models.py` to `core/`. Inject the enrichment computation as a callback into the renderer.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P5 | Move `SemanticContractStateInputV1`, `SemanticStatus`, `derive_semantic_contract_state` from `search/semantic/models.py` to `core/` | small | Eliminates outward dependency from core insight schema |
| 2 | P7 | Extract shared `_coerce_float`/`_coerce_str` into `core/type_coercion.py` and reuse in `schema.py` and `scoring.py` | small | Eliminates knowledge duplication |
| 3 | P21 | Document `Finding.__post_init__` coercion behavior in docstring; add explicit `DetailPayload.from_dict()` factory | small | Reduces surprise for callers |
| 4 | P23 | Add optional `policy` parameter to `get_worker_scheduler()` for test injection | small | Improves testability without changing production behavior |
| 5 | P14 | Add `Finding.language` and `Finding.impact_bucket` helper properties | small | Reduces LoD violations across codebase |

## Recommended Action Sequence

1. **Move semantic contract types to core** (P5, small). Move `SemanticContractStateInputV1`, `SemanticStatus`, and `derive_semantic_contract_state` from `tools/cq/search/semantic/models.py` to a new `tools/cq/core/semantic_contracts.py`. Update imports in `front_door_insight.py`. This removes the outward dependency.

2. **Extract render_enrichment.py from report.py** (P2, P3, P16, medium). Extract lines 785-1062 of `report.py` into `tools/cq/core/render_enrichment.py`. Define an `EnrichmentCallback` protocol and inject it into `render_markdown`. This decouples core rendering from search pipeline internals.

3. **Extract render_summary.py from report.py** (P2, P3, small). Extract lines 1410-1593 (compact summary, status derivers) into `tools/cq/core/render_summary.py`. This further reduces `report.py` to pure formatting.

4. **Consolidate type coercion helpers** (P7, small). Create `tools/cq/core/type_coercion.py` with shared `coerce_float`, `coerce_str`, `coerce_int` helpers. Update `schema.py:48-61` and `scoring.py:231-243` to use the shared versions.

5. **Add Finding helper properties** (P14, P15, small). Add `Finding.language -> str | None`, `Finding.impact_bucket -> str | None`, `Finding.confidence_bucket -> str | None` as computed properties that delegate to `details.data` and `details.score` respectively.

6. **Document and constrain DetailPayload mutability** (P10, P21, small). Add deprecation warnings to `DetailPayload.__setitem__`. Document the intended builder pattern. Audit call sites to determine which can be converted to construction-time assignment.

7. **Add worker scheduler test injection** (P23, small). Add optional `policy` parameter to `get_worker_scheduler()` and `set_worker_scheduler(scheduler)` for test use.

8. **Add structured telemetry to worker scheduler** (P24, small). Emit `DegradeEventV1`-style diagnostics when `collect_bounded` times out or when pool creation fails, making these events visible in result diagnostics.
