# Design Review: tools/cq/core/

**Date:** 2026-02-16
**Scope:** `tools/cq/core/` (including `cache/`, `runtime/`, `renderers/`, `tests/`)
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 76

## Executive Summary

The `tools/cq/core/` module demonstrates strong contract-first design with well-defined msgspec structs, explicit versioned schemas (`V1` suffixes), and clean hexagonal port definitions. However, three systemic issues undermine its quality: (1) widespread function-level code duplication across rendering files (`_na`, `_clean_scalar`, `_safe_int`, `_format_location`, `_extract_symbol_hint`, `_iter_result_findings` -- 6 functions duplicated 2-4 times each), (2) dependency direction violations where `core` imports from `search`, `macros`, and `query` (the reverse of the intended direction), and (3) duplicated constrained type aliases (`PositiveInt`, `NonNegativeInt`) defined in 4 separate files rather than imported from the single canonical source in `contracts_constraints.py`.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | Cache `__init__.py` re-exports 70+ symbols, leaking internal structure |
| 2 | Separation of concerns | 2 | medium | medium | `report.py` mixes rendering logic with IO (file reads, multiprocessing) |
| 3 | SRP | 1 | medium | medium | `front_door_insight.py` (1171 LOC) owns schema, building, rendering, and augmentation |
| 4 | High cohesion, low coupling | 2 | medium | medium | `report.py` duplicates helpers from `render_overview.py` and `render_enrichment.py` |
| 5 | Dependency direction | 0 | large | high | `core` imports from `search`, `macros`, `query` -- dependency inversion violated |
| 6 | Ports & Adapters | 2 | small | low | `ports.py` defines clean protocols but `services.py` uses deferred imports |
| 7 | DRY | 0 | medium | high | 6 utility functions duplicated 2-4x across rendering files; `PositiveInt` alias in 4 files |
| 8 | Design by contract | 3 | - | - | `contracts_constraints.py` enforces mapping limits; msgspec `Meta` constraints throughout |
| 9 | Parse, don't validate | 2 | small | low | `typed_boundary.py` provides canonical parse-at-boundary; but `_safe_dict`/`_safe_list_of_dict` are scattered |
| 10 | Make illegal states unrepresentable | 3 | - | - | `Literal` types for severity, status, risk; frozen structs; annotated constraints |
| 11 | CQS | 2 | small | low | `assign_result_finding_ids` mutates findings and returns the result |
| 12 | DI + explicit composition | 2 | medium | medium | Singletons (`_BACKEND_STATE`, `_SCHEDULER_STATE`) with module-level mutation |
| 13 | Prefer composition over inheritance | 3 | - | - | No inheritance hierarchies; struct bases are config-only mixins |
| 14 | Law of Demeter | 2 | small | low | Chain access into `finding.details.score.confidence_score` in multiple renderers |
| 15 | Tell, don't ask | 2 | small | low | Renderers extract raw data from findings via `get()` chains |
| 16 | Functional core, imperative shell | 1 | medium | medium | `report.py` mixes file IO, multiprocessing, and rendering transforms |
| 17 | Idempotency | 2 | small | low | `assign_result_finding_ids` is idempotent (deterministic hashes) but mutates |
| 18 | Determinism / reproducibility | 3 | - | - | `JSON_ENCODER` uses `order="deterministic"`; stable hashing everywhere |
| 19 | KISS | 2 | small | low | `CqCachePolicyV1` has 18 fields; `default_cache_policy` resolves each from env |
| 20 | YAGNI | 2 | small | low | `decode_yaml_strict` and `decode_toml_strict` may be unused |
| 21 | Least astonishment | 2 | small | low | `serialization.py` re-exports `contract_codec.py` with different names (`dumps_json` vs `encode_json`) |
| 22 | Declare and version contracts | 3 | - | - | All structs versioned (`V1`); `__all__` on every module; `schema_version` fields |
| 23 | Design for testability | 2 | medium | medium | `report.py` reads filesystem and spawns processes inline; not injectable |
| 24 | Observability | 3 | - | - | Cache telemetry is structured and comprehensive; `record_cache_*` functions |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The cache subsystem exposes its full internal vocabulary through `tools/cq/core/cache/__init__.py`, which re-exports 70+ symbols from 15 internal modules. This means any change to an internal cache module (e.g., renaming `FragmentHitV1`) becomes a public API change.

**Findings:**
- `tools/cq/core/cache/__init__.py:1-166` re-exports 70+ symbols, making every internal struct part of the public surface. Consumers can reach `FragmentPersistRuntimeV1`, `ScopeFileStatV1`, and `LaneCoordinationPolicyV1` -- all implementation details.
- `tools/cq/core/cache/diskcache_backend.py:93` exposes `self.cache` (the raw `FanoutCache` instance) as a public attribute on `DiskcacheBackend`, leaking the vendor type.

**Suggested improvement:**
Reduce the cache `__init__.py` to export only the public-facing types and functions: `CqCacheBackend`, `NoopCacheBackend`, `CqCachePolicyV1`, `default_cache_policy`, `get_cq_cache_backend`, `close_cq_cache_backend`. Internal consumers that need `FragmentEntryV1` etc. should import directly from submodules. Mark `DiskcacheBackend.cache` as `_cache`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
Rendering files generally separate concern areas well (overview, summary, enrichment, diagnostics). However, `report.py` blends rendering logic with filesystem IO and process pool management.

**Findings:**
- `tools/cq/core/report.py:346-358` (`_read_line_text`) performs direct filesystem reads inside the rendering pipeline.
- `tools/cq/core/report.py:553-581` (`_populate_render_enrichment_cache`) manages multiprocessing pool submission and timeout handling directly within the renderer.
- `tools/cq/core/report.py:375-376` imports from `tools.cq.search.pipeline.smart_search` at function scope -- mixing search pipeline execution with rendering.

**Suggested improvement:**
Extract the render-time enrichment computation into a separate `render_enrichment_resolver.py` that owns filesystem access and process scheduling. `report.py` should receive pre-computed enrichment payloads via a callback or injected service, not perform IO itself.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`front_door_insight.py` (1171 LOC) serves as schema definition, builder factory, rendering logic, semantic augmentation logic, and serialization logic -- five distinct responsibilities in one file.

**Findings:**
- `tools/cq/core/front_door_insight.py:44-147` defines 12 schema structs (InsightLocationV1 through FrontDoorInsightV1).
- `tools/cq/core/front_door_insight.py:149-187` defines 3 builder request contracts.
- `tools/cq/core/front_door_insight.py:189-296` implements rendering logic (`render_insight_card` and 7 sub-renderers).
- `tools/cq/core/front_door_insight.py:299-436` implements semantic augmentation (`build_neighborhood_from_slices`, `augment_insight_with_semantic`).
- `tools/cq/core/front_door_insight.py:439-565` implements 3 builder functions (search, calls, entity insight).
- `tools/cq/core/front_door_insight.py:1008-1087` implements manual serialization (`to_public_front_door_insight_dict`, `_serialize_target`, etc.).

**Suggested improvement:**
Split `front_door_insight.py` into three files: `front_door_schema.py` (structs + type aliases), `front_door_builders.py` (build_search/calls/entity_insight + augmentation), and `front_door_render.py` (render_insight_card + serialization). Each file changes for one reason: schema evolution, build logic, or output format.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The rendering files (`report.py`, `render_overview.py`, `render_enrichment.py`) share significant internal helpers that are copy-pasted rather than shared, indicating low cohesion in the rendering subsystem.

**Findings:**
- `_na()` is defined identically in 4 files: `report.py:193`, `render_overview.py:17`, `render_enrichment.py:109`, `render_summary.py:58`.
- `_clean_scalar()` is defined identically in 3 files: `report.py:197`, `render_overview.py:21`, `render_enrichment.py:120`.
- `_iter_result_findings()` is defined identically in 2 files: `report.py:417`, `render_overview.py:242`.
- `_extract_symbol_hint()` is defined identically in 2 files: `report.py:227`, `render_overview.py:32`.

**Suggested improvement:**
Create a `tools/cq/core/render_utils.py` containing the shared rendering helpers (`_na`, `_clean_scalar`, `_safe_int`, `_format_location`, `_extract_symbol_hint`, `_iter_result_findings`). Import them from all rendering modules.

**Effort:** small
**Risk if unaddressed:** medium (drift between copies leads to subtle rendering inconsistencies)

---

#### P5. Dependency direction -- Alignment: 0/3

**Current state:**
`core` is intended to be the leaf dependency layer that other CQ modules (`search`, `query`, `macros`) depend on. Instead, `core` has import-time and function-scope imports flowing backward into all three sibling packages. This is the most severe architectural violation in the scope.

**Findings:**
- `tools/cq/core/report.py:375-376` imports `classify_match` and `build_finding` from `tools.cq.search.pipeline.smart_search` at function scope.
- `tools/cq/core/render_enrichment.py:20` imports `is_applicability_not_applicable` from `tools.cq.search.objects.render` at module scope.
- `tools/cq/core/contracts.py:22-26` TYPE_CHECKING imports from `tools.cq.search._shared.search_contracts`.
- `tools/cq/core/multilang_summary.py:16` imports from `tools.cq.search._shared.search_contracts` at module scope.
- `tools/cq/core/schema_export.py:14-19` imports from `tools.cq.search` and `tools.cq.query` at module scope.
- `tools/cq/core/bundles.py:17-27` imports from `tools.cq.macros` and `tools.cq.query` at module scope.
- `tools/cq/core/cache/contracts.py:22-23` imports from `tools.cq.search.objects.render` and `tools.cq.search.tree_sitter`.
- `tools/cq/core/services.py:58,76,96` imports from `tools.cq.query` and `tools.cq.macros` at function scope.
- `tools/cq/core/report.py:38` imports `QueryLanguage` from `tools.cq.query.language` at module scope.
- `tools/cq/core/multilang_orchestrator.py:18` imports from `tools.cq.query.language` at module scope.

**Suggested improvement:**
1. Move `multilang_orchestrator.py`, `multilang_summary.py`, `schema_export.py`, `bundles.py`, and `request_factory.py` out of `core/` into `tools/cq/orchestration/` or their respective consumer packages. These files are orchestration glue, not core infrastructure.
2. For `report.py`, the render-time enrichment functions that import from `search.pipeline.smart_search` should be extracted into a callback protocol defined in `core/` and implemented by `search/`. Pass the callback into `render_markdown()`.
3. For `render_enrichment.py:20`, replace the module-scope import of `is_applicability_not_applicable` with a local inline check or move the function to `core/`.
4. For `cache/contracts.py:22-23`, move `SearchObjectSummaryV1` and `TreeSitterArtifactBundleV1` references behind a protocol or generic `dict[str, object]`.

**Effort:** large
**Risk if unaddressed:** high (circular dependency risk, inability to use `core` independently, violates the package hierarchy)

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`ports.py` defines clean Protocol-based ports (`SearchServicePort`, `EntityServicePort`, `CallsServicePort`, `CachePort`). However, the `services.py` implementations use deferred imports rather than proper DI, and the ports are not consistently used at call sites.

**Findings:**
- `tools/cq/core/ports.py:15-55` defines 4 well-structured protocols.
- `tools/cq/core/services.py:55-111` implements services with `@staticmethod` methods using deferred imports -- the implementations bypass DI by hard-importing concrete modules.
- `tools/cq/core/cache/interface.py:9-166` defines `CqCacheBackend` protocol with 14 methods -- a good port definition.

**Suggested improvement:**
Wire the service implementations through DI (constructor injection) rather than having `SearchService.execute()` do a deferred import of `smart_search` at call time. This would make the ports testable without mocking import machinery.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 0/3

**Current state:**
This is the most frequently violated principle in the scope. Multiple forms of semantic duplication exist across the rendering pipeline and configuration layer.

**Findings:**
- **Utility function duplication (6 functions, 2-4 copies each):**
  - `_na()` in 4 files: `report.py:193`, `render_overview.py:17`, `render_enrichment.py:109`, `render_summary.py:58`
  - `_clean_scalar()` in 3 files: `report.py:197`, `render_overview.py:21`, `render_enrichment.py:120`
  - `_safe_int()` in 2 files: `report.py:208`, `render_enrichment.py:131`
  - `_format_location()` in 2 files: `report.py:214`, `render_enrichment.py:137`
  - `_extract_symbol_hint()` in 2 files: `report.py:227`, `render_overview.py:32`
  - `_iter_result_findings()` in 2 files: `report.py:417`, `render_overview.py:242`

- **Constrained type alias duplication:**
  - `PositiveInt = Annotated[int, msgspec.Meta(ge=1)]` defined in `contracts_constraints.py:12`, `cache/policy.py:21`, `runtime/env_namespace.py:12`, `cache/snapshot_fingerprint.py:36` (via `NonNegativeInt`)
  - `NonNegativeInt = Annotated[int, msgspec.Meta(ge=0)]` defined in `contracts_constraints.py:13`, `cache/policy.py:22`, `cache/snapshot_fingerprint.py:36`, `cache/fragment_contracts.py:11`

- **`coerce_float` duplication:**
  - `schema.py:30` defines `coerce_float(value: object) -> float | None`
  - `type_coercion.py:6` defines `coerce_float(value: object) -> float` (different return type)
  - `scoring.py:245-250` defines a local `_coerce_float` inside `_score_details_from_mapping`

- **`_env_bool` / `_env_int` duplication:**
  - `cache/policy.py:68-76` defines `_env_bool`
  - `runtime/execution_policy.py:114-118` defines `_env_bool` with slightly different signature
  - `cache/policy.py:79-86` defines `_env_int`
  - `runtime/execution_policy.py:92-100` defines `_env_int` with slightly different signature

- **`CacheRuntimePolicy` duplication:**
  - `runtime/execution_policy.py:52-66` defines `CacheRuntimePolicy` with cache settings
  - `cache/policy.py:29-49` defines `CqCachePolicyV1` with near-identical fields plus extras. `default_cache_policy` reads from `CacheRuntimePolicy` and re-maps to `CqCachePolicyV1`.

**Suggested improvement:**
1. Create `tools/cq/core/render_utils.py` for the 6 duplicated rendering helpers.
2. Remove local `PositiveInt`/`NonNegativeInt` definitions from `cache/policy.py`, `cache/snapshot_fingerprint.py`, `cache/fragment_contracts.py`, and `runtime/env_namespace.py`. Import from `contracts_constraints.py`.
3. Consolidate `coerce_float` to a single authoritative version in `type_coercion.py` with the `float | None` return type from `schema.py`. Remove the version in `schema.py`.
4. Consolidate `_env_bool`/`_env_int` into `runtime/env_namespace.py` and import from both `cache/policy.py` and `runtime/execution_policy.py`.

**Effort:** medium
**Risk if unaddressed:** high (semantic drift between copies; `_clean_scalar` in `report.py` vs `render_enrichment.py` could diverge silently)

---

#### P8. Design by contract -- Alignment: 3/3

**Current state:**
Contracts are well-defined throughout. `contracts_constraints.py` provides canonical constraint types. `ContractConstraintPolicyV1` enforces payload size limits. `typed_boundary.py` provides centralized boundary validation.

**Findings:**
- `tools/cq/core/contracts_constraints.py:30-50` enforces `max_key_count`, `max_key_length`, `max_string_length` on all mapping payloads.
- `tools/cq/core/typed_boundary.py:14-67` provides `convert_strict` and `convert_lax` with typed `BoundaryDecodeError`.
- Annotated constraints on struct fields throughout (`Annotated[int, msgspec.Meta(ge=1)]`).

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`typed_boundary.py` provides excellent parse-at-boundary semantics. However, some modules use ad-hoc validation internally rather than parsing into typed structs.

**Findings:**
- `tools/cq/core/diagnostics_contracts.py:78-89` defines `_safe_dict` and `_safe_list_of_dict` as local parse-or-default functions rather than using `typed_boundary.convert_lax` with defaults.
- `tools/cq/core/render_summary.py:123` checks `isinstance(result.summary, dict)` before using it -- the `CqResult.summary` is typed as `dict[str, object]`, so this check is redundant.
- `tools/cq/core/summary_contracts.py:65-88` (`run_summary_from_dict`) manually validates each field with `isinstance` checks rather than using `convert_lax`.

**Suggested improvement:**
Replace `run_summary_from_dict` manual field checking with `convert_lax(raw, type_=RunSummaryV1)` and a fallback. Replace `_safe_dict`/`_safe_list_of_dict` with a generic `parse_or_default(value, type_, default_factory)` helper.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 3/3

**Current state:**
Excellent use of `Literal` types, frozen structs, and annotated constraints throughout.

**Findings:**
- `tools/cq/core/front_door_insight.py:28-31` uses `Literal` for `InsightSource`, `Availability`, `NeighborhoodSource`, `RiskLevel`.
- `tools/cq/core/semantic_contracts.py:14-15` uses `Literal` for `SemanticProvider` and `SemanticStatus`.
- `tools/cq/core/schema.py:271` constrains `severity` to `Literal["info", "warning", "error"]`.
- `tools/cq/core/snb_schema.py:136-150` defines `NeighborhoodSliceKind` as a 13-variant `Literal`.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. One notable violation exists where a function both mutates and returns.

**Findings:**
- `tools/cq/core/schema.py:466-489` (`assign_result_finding_ids`) mutates finding objects in-place (`finding.stable_id = stable_id`) AND returns the result. Callers cannot tell whether the function is a query or a command.
- `tools/cq/core/render_enrichment.py:93-106` (`merge_enrichment_details`) mutates the finding in-place -- this is a legitimate command with no return, so it follows CQS.

**Suggested improvement:**
Either make `assign_result_finding_ids` a pure function that returns a new `CqResult` with cloned findings, or change it to return `None` (command-only).

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
Two process-global singletons manage shared state with module-level lock + mutable holder patterns.

**Findings:**
- `tools/cq/core/cache/diskcache_backend.py:401-411` uses `_BackendState` singleton with `_BACKEND_LOCK` for process-global cache backend management. Backend is created on first access, registered with `atexit`.
- `tools/cq/core/runtime/worker_scheduler.py:159-170` uses `_SchedulerState` singleton with `_SCHEDULER_LOCK` for process-global scheduler management.
- Both patterns make substitution in tests difficult -- `set_worker_scheduler` exists but `set_cq_cache_backend` does not.

**Suggested improvement:**
Add a `set_cq_cache_backend` function for test injection, mirroring the `set_worker_scheduler` pattern. Consider consolidating both singletons into a single `CqRuntimeContext` that manages all process-global state.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies. Struct bases (`CqStruct`, `CqSettingsStruct`, `CqOutputStruct`, `CqStrictOutputStruct`, `CqCacheStruct`) are configuration-bearing base classes that set msgspec options -- a valid pattern.

**Findings:**
- `tools/cq/core/structs.py:8-47` defines 5 base struct classes. Each configures `kw_only`, `frozen`, `omit_defaults`, and optionally `forbid_unknown_fields`. No method inheritance.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Some rendering functions reach deep into collaborator chains.

**Findings:**
- `tools/cq/core/front_door_insight.py:753-761` (`_confidence_from_findings`) accesses `finding.details.score.confidence_score`, `finding.details.score.confidence_bucket`, and `finding.details.score.evidence_kind` -- three levels deep.
- `tools/cq/core/report.py:161-162` accesses `finding.details['impact_bucket']` and `finding.details['confidence_bucket']` via dictionary-style access on `DetailPayload`.

**Suggested improvement:**
Add a `finding.score` shortcut property that returns `finding.details.score` directly, reducing one level of chain access.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Rendering functions repeatedly ask findings for raw data via `details.get()` chains, then apply formatting logic externally.

**Findings:**
- `tools/cq/core/render_overview.py:32-46` (`_extract_symbol_hint`) iterates over 5 detail keys via `finding.details.get()` to derive a display string. This logic is external to the `Finding` model.
- `tools/cq/core/render_summary.py:457-461` (`get_impact_confidence_summary`) extracts `impact_bucket` and `confidence_bucket` from findings externally rather than asking the finding for its own summary.

**Suggested improvement:**
Add a `Finding.symbol_hint` property and a `Finding.impact_confidence_summary` property to encapsulate the extraction logic. The rendering code would call `finding.symbol_hint` instead of reimplementing extraction logic.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 1/3

**Current state:**
`report.py` is the primary violation point. It mixes pure rendering transforms with IO (file reading), process pool management, and mutable cache state.

**Findings:**
- `tools/cq/core/report.py:346-358` (`_read_line_text`) reads files from disk during rendering.
- `tools/cq/core/report.py:553-581` (`_populate_render_enrichment_cache`) submits tasks to process pools, collects results with timeouts, and falls back to sequential execution.
- `tools/cq/core/report.py:647-676` (`_maybe_attach_render_enrichment`) mutates findings in-place during rendering.
- `tools/cq/core/report.py:920-1000` (`render_markdown`) orchestrates all of the above in a single 80-line function.

**Suggested improvement:**
Separate `render_markdown` into two phases: (1) an imperative shell that resolves all enrichment data (file reads, process pool work) into a `RenderContext` data structure, and (2) a pure function `render_markdown_from_context(result, context)` that produces markdown from pre-computed data without any IO.

**Effort:** medium
**Risk if unaddressed:** medium (hard to test rendering without filesystem; hard to reason about side effects)

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
Most operations are idempotent. `assign_result_finding_ids` produces deterministic hashes, so re-running is safe. Cache operations are fail-open (idempotent degradation).

**Findings:**
- `tools/cq/core/schema.py:445-489` hashing is deterministic via `stable_digest24` -- same input produces same ID.
- `tools/cq/core/cache/diskcache_backend.py:162-184` `add` uses insert-if-absent semantics -- naturally idempotent.
- `tools/cq/core/report.py:647-676` `_maybe_attach_render_enrichment` skips enrichment if already present -- effectively idempotent.

**Suggested improvement:**
No immediate action needed. Minor: document the idempotency contract for `assign_result_finding_ids` explicitly.

**Effort:** small
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Determinism is a first-class concern. JSON encoding uses `order="deterministic"`. Cache keys use content hashing. Finding IDs use SHA256.

**Findings:**
- `tools/cq/core/contract_codec.py:13` configures `JSON_ENCODER = msgspec.json.Encoder(order="deterministic")`.
- `tools/cq/core/schema.py:445-463` (`_stable_finding_id`) produces deterministic SHA256-based IDs from finding content.
- `tools/cq/core/cache/content_hash.py` provides `file_content_hash` for cache invalidation.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The cache policy resolution is disproportionately complex for its purpose.

**Findings:**
- `tools/cq/core/cache/policy.py:29-49` defines `CqCachePolicyV1` with 18 fields. `default_cache_policy` (line 229-265) then resolves each field individually from environment variables, producing a 37-line function of repetitive env-int/env-bool calls.
- `tools/cq/core/cache/policy.py:151-226` (`_resolve_cache_scalar_settings`) is a 75-line function that reads 13 env vars with verbose getattr fallbacks. Each field follows the identical pattern: `_env_int(os.getenv("CQ_CACHE_X"), default=int(getattr(runtime, "x", default)), minimum=min)`.

**Suggested improvement:**
Use a declarative field-to-env mapping table and a single resolution loop rather than 13 separate `_env_int`/`_env_float` calls. This would reduce `_resolve_cache_scalar_settings` from 75 lines to approximately 20.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
A few utilities appear to have no current callers or are speculative.

**Findings:**
- `tools/cq/core/typed_boundary.py:86-99` (`decode_toml_strict`) and `tools/cq/core/typed_boundary.py:102-115` (`decode_yaml_strict`) -- TOML and YAML decoders may not have callers within the scope. These are small and low-risk.
- `tools/cq/core/cache/interface.py:68-84` defines `incr`/`decr` on the `CqCacheBackend` protocol. These are used by the coordination module, so they are justified.

**Suggested improvement:**
Verify whether `decode_toml_strict` and `decode_yaml_strict` have callers. If not, remove them.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Two naming/API surprises exist in the serialization layer.

**Findings:**
- `tools/cq/core/serialization.py` re-exports `contract_codec.py` functions under different names: `dumps_json` wraps `encode_json`, `loads_json` wraps `decode_json_result`, `to_builtins` wraps `to_contract_builtins`. This creates two parallel APIs for the same operations.
- `tools/cq/core/report.py:1004` defines `render_summary = render_summary_condensed` -- a module-level alias that makes `render_summary` in `report.py` mean something different from `render_summary` in `render_summary.py`.

**Suggested improvement:**
Consolidate the serialization layer into a single module with one set of names. Rename the `report.py` alias to `render_condensed_summary` to avoid shadowing.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 3/3

**Current state:**
Contract versioning is excellent throughout the module.

**Findings:**
- Every significant struct carries a `V1` suffix (`FrontDoorInsightV1`, `SemanticNeighborhoodBundleV1`, `FragmentEntryV1`, etc.).
- `schema.py:349` includes `schema_version` in `RunMeta`.
- `snb_schema.py:308` includes `schema_version: str = "cq.snb.v1"` in `SemanticNeighborhoodBundleV1`.
- Every module defines `__all__` explicitly.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Core schemas and scoring are highly testable. However, the rendering pipeline requires filesystem access and process pools, making unit testing expensive.

**Findings:**
- `tools/cq/core/report.py:346-358` (`_read_line_text`) reads from the filesystem inline during rendering. Testing `render_markdown` requires a real filesystem.
- `tools/cq/core/report.py:561-563` uses `get_worker_scheduler()` -- the global singleton. Tests must use `set_worker_scheduler()`.
- `tools/cq/core/cache/diskcache_backend.py:449-462` (`get_cq_cache_backend`) uses a global singleton. No `set_cq_cache_backend` exists for test injection.
- `tools/cq/core/scoring.py` is purely functional and easily testable.
- `tools/cq/core/enrichment_facts.py` is purely functional and easily testable.

**Suggested improvement:**
Add a `set_cq_cache_backend` function. Make filesystem access in `report.py` injectable via a `FileReader` protocol parameter.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
The cache subsystem has structured, comprehensive telemetry.

**Findings:**
- `tools/cq/core/cache/telemetry.py` provides 11 recording functions (`record_cache_get`, `record_cache_set`, `record_cache_timeout`, etc.) with `CacheNamespaceTelemetry` typed containers.
- `tools/cq/core/cache/diagnostics.py` provides `snapshot_backend_metrics` for cache health monitoring.
- `tools/cq/core/render_diagnostics.py` tracks render enrichment metrics (`attempted`, `applied`, `failed`, `skipped`).

**Effort:** N/A
**Risk if unaddressed:** N/A

---

## Cross-Cutting Themes

### Theme 1: Rendering Module Fragmentation and Code Duplication

**Root cause:** The rendering pipeline was split from a monolithic `report.py` into `render_summary.py`, `render_overview.py`, `render_enrichment.py`, and `render_diagnostics.py`. The extraction left duplicated private helpers in both the original and the extracted files.

**Affected principles:** P4 (cohesion), P7 (DRY), P3 (SRP)

**Suggested approach:** Create a `render_utils.py` with the 6 duplicated helpers. This is the single highest-ROI change. All 4 rendering files import from it. Additionally complete the extraction by removing the redundant copies from `report.py`.

### Theme 2: Core-Is-Not-A-Leaf Dependency Inversion

**Root cause:** Files like `bundles.py`, `multilang_orchestrator.py`, `multilang_summary.py`, `schema_export.py`, and `request_factory.py` were placed in `core/` for convenience but are actually orchestration/integration modules that depend on `search`, `query`, and `macros`. This inverts the intended dependency direction where `search`/`query`/`macros` depend on `core`, not vice versa.

**Affected principles:** P5 (dependency direction), P6 (ports & adapters), P23 (testability)

**Suggested approach:** Move orchestration files (`bundles.py`, `multilang_orchestrator.py`, `multilang_summary.py`, `schema_export.py`, `request_factory.py`) to a dedicated `tools/cq/orchestration/` package or to their respective consumer packages. For the remaining core files that need search functionality (`report.py`, `render_enrichment.py`), use callback injection via protocols defined in `core/`.

### Theme 3: Duplicated Type Aliases and Env Parsing

**Root cause:** The cache subsystem and runtime policy system were developed somewhat independently, leading to parallel definitions of the same constrained type aliases and environment parsing utilities.

**Affected principles:** P7 (DRY), P19 (KISS)

**Suggested approach:** Establish `contracts_constraints.py` as the single authority for constrained type aliases. Establish `runtime/env_namespace.py` as the single authority for environment variable parsing. Import from these canonical locations everywhere.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 DRY | Extract 6 duplicated rendering helpers into `render_utils.py` | small | Eliminates 18+ duplicate function bodies; prevents silent drift |
| 2 | P7 DRY | Import `PositiveInt`/`NonNegativeInt` from `contracts_constraints.py` in 4 files | small | Single authority for constrained types |
| 3 | P11 CQS | Make `assign_result_finding_ids` either pure or void-returning | small | Eliminates query/command ambiguity |
| 4 | P21 Astonishment | Remove `render_summary = render_summary_condensed` alias in `report.py` | small | Eliminates naming confusion |
| 5 | P1 Info hiding | Reduce cache `__init__.py` exports from 70+ to ~10 public symbols | small | Reduces blast radius of cache internal changes |

## Recommended Action Sequence

1. **Create `render_utils.py`** with the 6 duplicated helpers (`_na`, `_clean_scalar`, `_safe_int`, `_format_location`, `_extract_symbol_hint`, `_iter_result_findings`). Update all rendering files to import from it. (Addresses P7, P4)

2. **Consolidate constrained type aliases** by removing local `PositiveInt`/`NonNegativeInt` definitions from `cache/policy.py`, `cache/snapshot_fingerprint.py`, `cache/fragment_contracts.py`, and `runtime/env_namespace.py`. Import from `contracts_constraints.py`. (Addresses P7)

3. **Consolidate `coerce_float`** by keeping the `float | None` version in `type_coercion.py` and updating `schema.py` to import from it. Remove the local `_coerce_float` in `scoring.py`. (Addresses P7)

4. **Reduce cache `__init__.py`** to export only the 10 truly public symbols. Internal modules import directly from submodules. (Addresses P1)

5. **Move orchestration files** (`bundles.py`, `multilang_orchestrator.py`, `multilang_summary.py`, `schema_export.py`, `request_factory.py`) out of `core/`. These belong in a `tools/cq/orchestration/` package. (Addresses P5)

6. **Extract render-time enrichment IO** from `report.py` into a protocol-based callback, eliminating the core -> search dependency for file-based enrichment. (Addresses P5, P16, P23)

7. **Split `front_door_insight.py`** into schema, builders, and renderers (3 files). (Addresses P3)

8. **Add `set_cq_cache_backend`** for test injection symmetry with `set_worker_scheduler`. (Addresses P23)
