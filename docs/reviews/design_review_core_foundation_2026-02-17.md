# Design Review: tools/cq/core/ (Foundation Layer)

**Date:** 2026-02-17
**Scope:** `tools/cq/core/` -- all files
**Focus:** All principles (1-24)
**Depth:** moderate
**Files reviewed:** 20 of 86 (entry points, high-fan-in modules, public interfaces, cache subsystem representatives)

## Executive Summary

The `tools/cq/core/` foundation layer is well-structured in its use of hexagonal ports, frozen `msgspec` structs for contracts, and Protocol-based interfaces. However, three systemic issues undermine its quality as a universal dependency: (1) **the core schema types `DetailPayload`, `Finding`, `CqResult`, and `SummaryEnvelopeV1` are mutable**, violating information hiding and enabling uncontrolled mutation from any dependent subsystem; (2) **cache policy knowledge is duplicated** across `CacheRuntimePolicy` in `execution_policy.py` and `CqCachePolicyV1` in `cache/policy.py`, with both encoding identical semantics; (3) **`core/` has reverse imports from `search/`**, undermining the intended dependency direction. The highest-leverage improvements are making core schema types frozen and consolidating the cache policy duplication.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | high | Core schema types are mutable; any consumer can mutate `Finding.details`, `CqResult.sections`, `SummaryEnvelopeV1` fields |
| 2 | Separation of concerns | 2 | small | medium | `report.py` mixes rendering with enrichment orchestration |
| 3 | SRP (one reason to change) | 2 | small | medium | `summary_contract.py` is both schema definition and variant resolution logic |
| 4 | High cohesion, low coupling | 2 | medium | medium | Cache subsystem is cohesive; but two `SummaryEnvelopeV1` types exist in different files |
| 5 | Dependency direction | 1 | medium | high | `core/services.py` and `core/settings_factory.py` import from `search/` at module level |
| 6 | Ports & Adapters | 3 | -- | low | Well-implemented: `ports.py` defines protocol interfaces, adapters are separate |
| 7 | DRY (knowledge) | 1 | medium | high | Cache policy semantics duplicated across `CacheRuntimePolicy` and `CqCachePolicyV1` |
| 8 | Design by contract | 2 | small | medium | `contracts_constraints.py` provides constraint types, but core structs lack `frozen=True` enforcement |
| 9 | Parse, don't validate | 2 | small | low | `DetailPayload.from_legacy()` and `summary_from_mapping()` parse well at boundaries |
| 10 | Illegal states | 1 | medium | high | Mutable `Finding`, `CqResult`, `SummaryEnvelopeV1` allow arbitrary post-construction mutation |
| 11 | CQS | 2 | small | medium | `apply_summary_mapping()` mutates + returns void (command); `merge_enrichment_details()` mutates finding |
| 12 | DI + explicit composition | 2 | small | low | `bootstrap.py` is a clean composition root; but global singletons exist in `backend_lifecycle.py` |
| 13 | Composition over inheritance | 2 | small | low | Cache backend uses mixin inheritance correctly; `SummaryEnvelopeV1` subclassing is appropriate |
| 14 | Law of Demeter | 2 | small | low | `finding.details.get("enrichment")` chains are common but within same boundary |
| 15 | Tell, don't ask | 1 | medium | medium | `DetailPayload` exposes `data` dict directly; renderers reach deep into finding internals |
| 16 | Functional core | 2 | small | low | Scoring, enrichment facts, contract codec are pure; but `report.py` mixes IO and transforms |
| 17 | Idempotency | 3 | -- | low | `assign_result_finding_ids` is deterministic; cache operations are idempotent by design |
| 18 | Determinism | 3 | -- | low | Cache keys built from canonical digests; JSON encoding is deterministic-ordered |
| 19 | KISS | 2 | small | medium | Fragment cache engine adds complexity for marginal gain vs. simple key-value |
| 20 | YAGNI | 2 | small | low | `CqCacheStreamingMixin` and blob store may be underused |
| 21 | Least astonishment | 1 | medium | medium | Two `SummaryEnvelopeV1` types with same name in different files; `DetailPayload.__setitem__` surprising on a struct |
| 22 | Public contracts | 2 | small | low | `__all__` is consistently declared; schema version is tracked |
| 23 | Testability | 2 | small | low | `NoopCacheBackend` enables test substitution; global singletons slightly reduce testability |
| 24 | Observability | 2 | small | low | Cache telemetry is structured; but no structured logging outside cache namespace |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
The core schema types `DetailPayload`, `Finding`, `CqResult`, and `SummaryEnvelopeV1` in `summary_contract.py` are all mutable `msgspec.Struct` instances without `frozen=True`. This means any module that receives these objects can mutate them, destroying the information-hiding boundary between the foundation layer and its 232 consuming files.

**Findings:**
- `tools/cq/core/schema.py:62` -- `DetailPayload(msgspec.Struct, omit_defaults=True)` is mutable. It exposes `__setitem__` (line 130) allowing arbitrary key mutation, and `data` is a public mutable dict.
- `tools/cq/core/schema.py:240` -- `Finding(msgspec.Struct)` is mutable. `__post_init__` (line 272) mutates `self.details`. The `merge_enrichment_details()` function in `render_enrichment.py:113` directly mutates `finding.details[key]`.
- `tools/cq/core/schema.py:350` -- `CqResult(msgspec.Struct)` is mutable. Fields `key_findings`, `evidence`, `sections` are mutable lists. `result_factory.py:46` mutates `result.summary.error`.
- `tools/cq/core/summary_contract.py:23` -- `SummaryEnvelopeV1(msgspec.Struct, omit_defaults=True)` is mutable. `apply_summary_mapping()` (line 512) uses `setattr()` to mutate fields.

This stands in contrast to the well-designed frozen structs used elsewhere: `CqStruct`, `CqSettingsStruct`, `CqOutputStruct`, `CqCacheStruct` in `structs.py:8-44` all declare `frozen=True`.

**Suggested improvement:**
Make `Finding`, `CqResult`, and `SummaryEnvelopeV1` frozen. Replace mutation patterns with `msgspec.structs.replace()` (already used in `assign_result_finding_ids` at `schema.py:469`). For `DetailPayload`, remove `__setitem__` and provide a `with_data()` builder method that returns a new instance. The `apply_summary_mapping()` function should return a new summary rather than mutating in place.

**Effort:** medium
**Risk if unaddressed:** high -- Any consumer can silently corrupt shared state across subsystem boundaries, creating hard-to-diagnose bugs.

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
Most modules have clean concern boundaries. The main violation is `report.py`, which is simultaneously a markdown renderer AND an enrichment orchestrator.

**Findings:**
- `tools/cq/core/report.py:466-505` -- `_prepare_render_enrichment_session()` orchestrates enrichment computation (precomputing caches, selecting files, counting tasks, computing metrics). This is enrichment-pipeline logic embedded in a rendering module.
- `tools/cq/core/report.py:139-146` -- `_format_finding()` calls `_maybe_attach_render_enrichment_orchestrator()` as a side effect during rendering, mixing mutation (enrichment attachment) with presentation.

**Suggested improvement:**
Extract enrichment session preparation into `render_enrichment_orchestrator.py` (which already exists and owns related logic). The `render_markdown()` function should receive a pre-computed enrichment session rather than orchestrating it internally.

**Effort:** small
**Risk if unaddressed:** medium -- Changes to enrichment logic require modifying the renderer, and vice versa.

---

#### P3. SRP -- Alignment: 2/3

**Current state:**
Most files follow SRP well. The primary exception is `summary_contract.py`.

**Findings:**
- `tools/cq/core/summary_contract.py` -- This 580-line file serves four distinct purposes: (1) schema definitions for 5 summary variants (lines 23-254), (2) variant resolution logic (lines 261-391), (3) factory functions (lines 393-419), and (4) mapping conversion with mutation (lines 422-530). It changes when any summary variant evolves, when mode-to-variant mapping changes, or when serialization logic changes.

**Suggested improvement:**
Split into `summary_schema.py` (struct definitions and type alias), `summary_resolution.py` (variant name resolution and mapping tables), and keep factory/conversion functions in a slim `summary_contract.py` that re-exports the public surface.

**Effort:** small
**Risk if unaddressed:** medium -- The file is already the most-imported module in `core/` and will continue growing.

---

#### P5. Dependency direction -- Alignment: 1/3

**Current state:**
`tools/cq/core/` is meant to be a foundation layer that other subsystems depend on, not the reverse. However, multiple core files import from `tools/cq/search/`.

**Findings:**
- `tools/cq/core/services.py:11` -- `from tools.cq.search.pipeline.enrichment_contracts import IncrementalEnrichmentModeV1, parse_incremental_enrichment_mode` at module level. This forces core to depend on the search pipeline.
- `tools/cq/core/services.py:19` -- TYPE_CHECKING import of `from tools.cq.search._shared.types import QueryMode, SearchLimits`.
- `tools/cq/core/services.py:95` -- Dynamic import `from tools.cq.search.pipeline.smart_search import smart_search` inside method body (better, but still a dependency).
- `tools/cq/core/settings_factory.py:18` -- `from tools.cq.search.tree_sitter.core.infrastructure import ParserControlSettingsV1, parser_controls_from_env` at module level. The `SettingsFactory` class depends on search-specific tree-sitter infrastructure.
- `tools/cq/core/contracts.py:21` -- TYPE_CHECKING import of `SearchSummaryContract` from `tools.cq.search._shared.search_contracts`.
- `tools/cq/core/contracts.py:60` -- Dynamic import of `summary_contract_to_dict` from search.

**Suggested improvement:**
Move `IncrementalEnrichmentModeV1` to `core/` since it is a runtime policy enum, not search-specific logic. Move `ParserControlSettingsV1` and `parser_controls_from_env()` to `core/runtime/` or accept them as injection parameters in `SettingsFactory`. For `SearchServiceRequest`, accept `mode` as a string and let the search module parse it, rather than importing search types into core.

**Effort:** medium
**Risk if unaddressed:** high -- The foundation layer loses its status as a universal dependency and creates import cycle risk. Every subsystem that imports from `core/` transitively imports search pipeline modules.

---

#### P6. Ports & Adapters -- Alignment: 3/3

**Current state:**
The hexagonal architecture is well-implemented. `ports.py` defines clean Protocol-based interfaces (`SearchServicePort`, `EntityServicePort`, `CallsServicePort`, `CachePort`, `RenderEnrichmentPort`). The cache backend hierarchy in `interface.py` decomposes capabilities into `CqCacheReadWriteBackend`, `CqCacheMaintenanceBackend`, `CqCacheStreamingBackend`, and `CqCacheCoordinationBackend` with Protocol-based structural typing and `runtime_checkable` decorators. `NoopCacheBackend` provides a test-friendly null adapter. `DiskcacheBackend` is a clean adapter implementation.

No action needed.

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge) -- Alignment: 1/3

**Current state:**
Cache policy semantics are encoded in two parallel struct hierarchies that share the same field names and defaults.

**Findings:**
- `tools/cq/core/cache/policy.py:35-55` -- `CqCachePolicyV1` defines 16 fields: `enabled`, `directory`, `shards`, `timeout_seconds`, `ttl_seconds`, `evict_run_tag_on_exit`, `namespace_ttl_seconds`, `namespace_enabled`, `namespace_ephemeral`, `size_limit_bytes`, `cull_limit`, `eviction_policy`, `statistics_enabled`, `max_tree_sitter_lanes`, `lane_lock_ttl_seconds`, `sqlite_mmap_size`, `sqlite_cache_size`, `transaction_batch_size`.
- `tools/cq/core/runtime/execution_policy.py:58-72` -- `CacheRuntimePolicy` duplicates 12 of these fields: `enabled`, `ttl_seconds`, `shards`, `timeout_seconds`, `evict_run_tag_on_exit`, `namespace_ttl_seconds`, `namespace_enabled`, `namespace_ephemeral`, `size_limit_bytes`, `cull_limit`, `eviction_policy`, `statistics_enabled`.
- Both import the same defaults from `cache/defaults.py` and use the same `env_int`/`env_bool` parsing from `runtime/env_namespace.py`.
- The `_resolve_namespace_ttl_from_env()` logic in `policy.py:74-94` and `_env_namespace_ttls()` in `execution_policy.py:98-113` implement the same namespace override resolution with nearly identical code.
- Additionally, `tools/cq/core/summary_contracts.py:14` defines a `SummaryEnvelopeV1` class that is entirely different from `tools/cq/core/summary_contract.py:23` `SummaryEnvelopeV1`. Two types with the same name serve different purposes.

**Suggested improvement:**
Consolidate cache policy into a single canonical type (keep `CqCachePolicyV1`). Have `RuntimeExecutionPolicy.cache` reference `CqCachePolicyV1` directly rather than duplicating fields in `CacheRuntimePolicy`. Rename the `summary_contracts.py` `SummaryEnvelopeV1` to `SummaryOutputEnvelopeV1` or similar to eliminate the naming collision.

**Effort:** medium
**Risk if unaddressed:** high -- Drift between the two cache policy types will cause subtle behavior differences depending on which code path resolves settings.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The codebase uses `msgspec.Meta` annotations (`PositiveInt`, `NonNegativeInt`, `PositiveFloat`, `BoundedRatio`, `NonEmptyStr` in `contracts_constraints.py:12-16`) for constraint enforcement at deserialization boundaries. `Anchor.line` has `Annotated[int, msgspec.Meta(ge=1)]` at `schema.py:207`. `ContractConstraintPolicyV1` enforces mapping-level constraints.

**Findings:**
- `tools/cq/core/schema.py:263` -- `Finding.category` and `Finding.severity` lack constraint annotations. `severity` is declared as `Literal["info", "warning", "error"]` which is good, but `category` is bare `str` -- no validation prevents empty strings or arbitrary values.
- `tools/cq/core/schema.py:311` -- `RunMeta.started_ms` and `RunMeta.elapsed_ms` are bare `float` with no non-negative constraint.

**Suggested improvement:**
Add `NonEmptyStr` constraint to `Finding.category` and `NonNegativeFloat = Annotated[float, msgspec.Meta(ge=0.0)]` to `RunMeta.started_ms` and `RunMeta.elapsed_ms`.

**Effort:** small
**Risk if unaddressed:** medium -- Invalid empty-category findings or negative timestamps can propagate silently.

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
The boundary parsing is generally good. `DetailPayload.from_legacy()` (schema.py:70) converts unstructured dicts into structured payloads. `summary_from_mapping()` (summary_contract.py:422) normalizes arbitrary mappings into typed summary variants. `_decode_result_payload()` in contract_codec.py uses `msgspec.convert()` for type-safe deserialization.

**Findings:**
- `tools/cq/core/report.py:175-206` -- `_format_context_block()` repeatedly validates `context_window` at rendering time: checking `isinstance(context_window, dict)`, `hasattr(context_window, "start_line")`, etc. This indicates the upstream data was not parsed into a structured type at the boundary.

**Suggested improvement:**
Define a `ContextWindow` struct (with `start_line` and `end_line` fields) and parse `context_window` into it during `DetailPayload` construction rather than doing duck-type checking at render time.

**Effort:** small
**Risk if unaddressed:** low -- Scattered validation is an annoyance, not a correctness risk here.

---

#### P10. Make illegal states unrepresentable -- Alignment: 1/3

**Current state:**
The mutable core types are the primary violation. Beyond that, several modeling choices allow illegal combinations.

**Findings:**
- `tools/cq/core/schema.py:350` -- `CqResult` allows `summary` to be any `SummaryV1` variant regardless of `run.macro`. A `CqResult` with `run.macro="calls"` and `summary=SearchSummaryV1()` is representable but semantically illegal.
- `tools/cq/core/schema.py:62` -- `DetailPayload` has a public `data: dict[str, object]` that can contain arbitrary keys. Combined with `__setitem__`, any code can inject arbitrary state.
- `tools/cq/core/front_door_contracts.py:131-142` -- `FrontDoorInsightV1` has well-constrained Literal types for `source`, `Availability`, `RiskLevel`, and `NeighborhoodSource` -- this is a good model of making illegal states unrepresentable.

**Suggested improvement:**
For the macro/summary mismatch, consider a factory that enforces the correct variant: `CqResult.for_search(...)`, `CqResult.for_calls(...)`, etc. For `DetailPayload`, make `data` private (prefix with `_`) and expose read-only access through methods.

**Effort:** medium
**Risk if unaddressed:** high -- Mismatched macro/summary combinations can produce rendering errors or incorrect output format selection.

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. Scoring functions are pure queries. Cache operations return acknowledgment booleans (commands that return status, which is acceptable). Factory functions are pure.

**Findings:**
- `tools/cq/core/summary_contract.py:512-530` -- `apply_summary_mapping()` mutates a `SummaryEnvelopeV1` in place and returns `None`. This is a command, but it is called from within `summary_from_mapping()` which is expected to be a query (it returns a value). The mutation is hidden inside what looks like a pure factory function.
- `tools/cq/core/render_enrichment.py:113-127` -- `merge_enrichment_details()` mutates `finding.details` in place. This is called from `_format_finding()` which is a rendering function -- mixing mutation with presentation.
- `tools/cq/core/result_factory.py:46` -- `result.summary.error = str(error)` mutates the result after construction.

**Suggested improvement:**
Change `apply_summary_mapping()` to return a new summary instance. Change `merge_enrichment_details()` to return a new `Finding` with merged details. Change `build_error_result()` to construct the result with the error in the summary from the start.

**Effort:** small
**Risk if unaddressed:** medium -- Hidden mutations make it hard to reason about state flow.

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
`bootstrap.py` serves as a clean composition root: `build_runtime_services()` and `resolve_runtime_services()` wire the service graph. `CqRuntimeServices` is a frozen dataclass holding all services.

**Findings:**
- `tools/cq/core/cache/backend_lifecycle.py:20-21` -- `_BACKEND_LOCK` and `_BACKEND_STATE` are process-global mutable singletons. While thread-safe, they make it impossible to have two independent cache configurations in the same process (e.g., for parallel test suites).
- `tools/cq/core/bootstrap.py:43-44` -- `_RUNTIME_SERVICES_LOCK` and `_RUNTIME_SERVICES` are another process-global singleton cache.
- Three separate `atexit.register()` calls across `backend_lifecycle.py:91`, `bootstrap.py:74`, and `worker_scheduler.py:207` register cleanup hooks independently. These are not coordinated.

**Suggested improvement:**
Consider consolidating singleton management into a single `CqProcessState` object that owns all global state (cache backends, runtime services, worker schedulers) and has a single `atexit` cleanup. This would make the cleanup order deterministic and simplify testing.

**Effort:** medium
**Risk if unaddressed:** low -- Current approach works; the risk is cleanup ordering in complex shutdown scenarios.

---

#### P13. Composition over inheritance -- Alignment: 2/3

**Current state:**
The cache backend uses mixin inheritance (`_DiskcacheReadWriteMixin`, `_DiskcacheMaintenanceMixin`, etc.) composed into `DiskcacheBackend`. This is borderline -- it's composition implemented via inheritance. The mixin pattern works here because each mixin is a capability slice with no shared mutable state beyond `cache`, `default_ttl_seconds`, and `transaction_batch_size`.

The `SummaryEnvelopeV1` hierarchy uses subclassing for variant specialization: `SearchSummaryV1`, `CallsSummaryV1`, etc. extend `SummaryEnvelopeV1`. This is appropriate discriminated-union modeling.

No immediate action needed. The mixin approach for the cache backend could eventually be refactored to delegation, but the current pattern is functional.

**Effort:** small
**Risk if unaddressed:** low

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Chain access patterns like `finding.details.get("enrichment")` and `finding.anchor.to_ref()` are common. These are within the same logical boundary (core schema types) so are acceptable.

**Findings:**
- `tools/cq/core/report.py:169-170` -- `finding.details['impact_bucket']` and `finding.details['confidence_bucket']` reach into the detail payload's score fields through the dict-like interface.

**Suggested improvement:**
Add `Finding.impact_bucket` and `Finding.confidence_bucket` convenience properties that delegate to `details.score`, reducing the chain depth.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 1/3

**Current state:**
`DetailPayload` exposes its internal `data` dict publicly and provides dict-like access (`__getitem__`, `__setitem__`, `__contains__`, `get`). This encourages "ask" patterns where callers reach into the payload to extract data and make decisions.

**Findings:**
- `tools/cq/core/schema.py:67` -- `data: dict[str, object] = msgspec.field(default_factory=dict)` is a public mutable field. Callers access it directly: `to_builtins(finding.details.data)` in `render_enrichment.py:102`.
- `tools/cq/core/report.py:185-206` -- `_format_context_block()` asks `finding.details.get("context_snippet")`, `finding.details.get("context_window")`, `finding.details.get("language")` -- extracting three separate values to reconstruct context. The detail payload should be able to format its own context block.
- `tools/cq/core/render_enrichment.py:50-86` -- `extract_enrichment_payload()` reaches into `finding.details.get("enrichment")`, `finding.details.get("python_enrichment")`, `finding.details.get("incremental_enrichment")`, `finding.details.get("rust_tree_sitter")` -- four separate probes.

**Suggested improvement:**
Add methods to `DetailPayload` or `Finding` that encapsulate common data extraction patterns: `finding.enrichment_payload()`, `finding.context_window()`, `finding.language()`. This moves the logic closer to the data and reduces scattered interrogation.

**Effort:** medium
**Risk if unaddressed:** medium -- Logic for interpreting detail payloads is scattered across renderers, enrichment modules, and front-door assembly, making changes to the data layout expensive.

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The scoring module (`scoring.py`) is entirely pure -- `impact_score()`, `confidence_score()`, `bucket()`, `build_score_details()` are all deterministic transforms. `enrichment_facts.py` is also pure. `contract_codec.py` encodes/decodes without side effects.

**Findings:**
- `tools/cq/core/report.py:139-146` -- `_format_finding()` has a side effect: `_maybe_attach_render_enrichment_orchestrator()` mutates the finding during rendering. This embeds an imperative action in what should be a pure transform.

**Suggested improvement:**
Pre-compute all enrichment before entering the render pass. The render phase should operate on immutable, fully-enriched findings.

**Effort:** small
**Risk if unaddressed:** low -- The current pattern works but makes the rendering pass non-idempotent.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Cache operations are idempotent by design (`set` overwrites, `add` is conditional). `assign_result_finding_ids()` produces deterministic IDs from content hashes. `build_cache_key()` produces deterministic keys from canonical digests.

No action needed.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
JSON encoding uses `order="deterministic"` via `JSON_ENCODER = msgspec.json.Encoder(order="deterministic")` in `contract_codec.py:100`. Cache key builders use `canonicalize_payload()` before hashing. Finding IDs use SHA-256 of canonicalized content (`_stable_finding_id` at `schema.py:440`).

No action needed.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are appropriately simple. The cache subsystem adds necessary complexity for the fail-open resilience pattern.

**Findings:**
- `tools/cq/core/cache/fragment_engine.py` -- The fragment cache engine introduces 8 callable type aliases, 2 runtime bundle dataclasses, and 2 orchestration functions for what is essentially "check cache, return hit or miss, write results back." The abstraction level is high for a layer that serves as internal plumbing.
- `tools/cq/core/cache/policy.py:136-218` -- `_resolve_cache_scalar_settings()` is 82 lines of `getattr(runtime, ...)` and `os.getenv(...)` chains. This could be simplified with a table-driven approach.

**Suggested improvement:**
For `_resolve_cache_scalar_settings()`, consider a declarative field-resolution table: `[(field_name, env_var, default_attr, type, min_value), ...]` iterated in a loop. For the fragment engine, document that its complexity is justified by the fail-open and batch-write requirements, or simplify if those requirements have softened.

**Effort:** small
**Risk if unaddressed:** medium -- New contributors will find the cache policy resolution logic hard to follow.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
The `CqCacheStreamingBackend` protocol and `_DiskcacheStreamingMixin` provide blob streaming capability. The `blob_store.py` and `tree_sitter_cache_store.py` use this, so it is not purely speculative.

**Findings:**
- `tools/cq/core/cache/interface.py:249-382` -- Four no-op mixin classes (`_NoopReadWriteMixin`, `_NoopMaintenanceMixin`, `_NoopStreamingMixin`, `_NoopCoordinationMixin`) duplicate the Protocol method signatures with no-op bodies. These are 133 lines of boilerplate that exactly mirror the Protocol definitions above.

**Suggested improvement:**
Use `__init_subclass__` or a factory function to generate the no-op implementations from the Protocol definitions, or accept the duplication as the cost of explicit no-op behavior. If keeping the duplication, add a comment noting they are intentionally mirrored.

**Effort:** small
**Risk if unaddressed:** low -- Boilerplate is annoying but not harmful.

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
Two classes named `SummaryEnvelopeV1` exist in the same package with different schemas and purposes.

**Findings:**
- `tools/cq/core/summary_contract.py:23` -- `SummaryEnvelopeV1(msgspec.Struct, omit_defaults=True)` with 64 fields (matches, files_scanned, front_door_insight, etc.). This is the primary summary type used throughout the pipeline.
- `tools/cq/core/summary_contracts.py:14` -- `SummaryEnvelopeV1(CqStrictOutputStruct, frozen=True)` with 3 fields (summary, diagnostics, telemetry). This is an output boundary envelope.
- These have the same class name but completely different schemas, inheritance chains, and purposes. An import of `SummaryEnvelopeV1` from the wrong file will silently produce type errors at runtime.
- `tools/cq/core/schema.py:130` -- `DetailPayload.__setitem__` makes a `msgspec.Struct` behave like a mutable dict. This is surprising -- `msgspec` structs are typically expected to be value types, not mutable containers.

**Suggested improvement:**
Rename `summary_contracts.py:SummaryEnvelopeV1` to `SummaryOutputEnvelopeV1` or `RenderSummaryEnvelopeV1` to distinguish it from the pipeline summary type. For `DetailPayload`, either commit to the dict-like interface by documenting it prominently, or remove `__setitem__` and use builder methods.

**Effort:** medium
**Risk if unaddressed:** medium -- Name collision will cause confusion and import errors as the codebase grows.

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
Every module declares `__all__`. The `__init__.py` re-exports a curated set of public symbols. `SCHEMA_VERSION` is tracked. Schema evolution notes are documented at `schema.py:20-22`.

**Findings:**
- `tools/cq/core/__init__.py` -- Exports 11 symbols. This is a narrow, intentional public surface. However, consumers frequently import from internal submodules directly (e.g., `from tools.cq.core.scoring import ...`, `from tools.cq.core.contract_codec import ...`), bypassing the declared public surface.

**Suggested improvement:**
Either expand `__init__.py` to include the commonly-imported symbols (scoring, contract_codec, summary_contract), or document that direct submodule imports are the expected pattern and `__init__.py` is for the minimum convenience surface.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
`NoopCacheBackend` provides zero-overhead test substitution for the cache layer. `CqRuntimeServices` is a frozen dataclass that can be constructed with mock services. Protocol-based ports enable substitution.

**Findings:**
- `tools/cq/core/cache/backend_lifecycle.py:20-21` -- Process-global singleton `_BACKEND_STATE` with `_BACKEND_LOCK`. Tests that need isolated cache backends must call `set_cq_cache_backend()` and `close_cq_cache_backend()` for cleanup. Missing cleanup causes cross-test contamination.
- `tools/cq/core/bootstrap.py:43-44` -- Same pattern: `_RUNTIME_SERVICES` global dict. `clear_runtime_services()` must be called in test teardown.

**Suggested improvement:**
Document the required test fixture pattern (setup/teardown for global singletons) or provide a context manager: `with isolated_cache_backend(noop=True): ...` that handles injection and cleanup.

**Effort:** small
**Risk if unaddressed:** low -- The pattern works if test authors know about it.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Cache telemetry is well-structured: `record_cache_timeout()`, `record_cache_abort()`, `record_cache_cull()`, `record_cache_volume()` in `cache/telemetry.py` provide namespace-scoped metrics. `SemanticTelemetryV1` in `summary_contract.py:13-20` tracks attempted/applied/failed/skipped/timed_out counters.

**Findings:**
- Outside the cache namespace, there is no structured logging or metrics. Service execution (`services.py`), rendering (`report.py`), and bootstrap (`bootstrap.py`) have no instrumentation.
- `tools/cq/core/report.py:494-499` -- Render enrichment metrics are computed (attempted, applied, failed, skipped) but embedded in the summary payload rather than emitted as separate telemetry.

**Suggested improvement:**
Add structured logging at service boundaries: when `SearchService.execute()` is called, log the request parameters and elapsed time. When `render_markdown()` completes, log rendering metrics. Use the same telemetry pattern established by the cache namespace.

**Effort:** small
**Risk if unaddressed:** low -- Cache telemetry is adequate for debugging cache issues; the gap is in service-level observability.

---

## Cross-Cutting Themes

### Theme 1: Mutable core types undermine the frozen-struct discipline

**Root cause:** The four primary schema types (`DetailPayload`, `Finding`, `CqResult`, `SummaryEnvelopeV1`) predate the `CqStruct(frozen=True)` convention established in `structs.py`. They were designed for incremental construction and mutation, but this conflicts with the information-hiding and immutability principles applied everywhere else.

**Affected principles:** P1, P8, P10, P11, P15, P16

**Suggested approach:** This is the highest-leverage change. Make these types frozen and adopt `msgspec.structs.replace()` for all mutations. This will surface all mutation sites as type errors, making the transition self-documenting. The `assign_result_finding_ids()` function already demonstrates the pattern.

### Theme 2: Reverse dependency from core to search

**Root cause:** `SearchServiceRequest` in `services.py` needs search-specific types (`IncrementalEnrichmentModeV1`, `QueryMode`, `SearchLimits`). `SettingsFactory` needs tree-sitter parser controls. These were placed in core for convenience but violate the layering.

**Affected principles:** P5, P4

**Suggested approach:** Move search-specific types that core needs into core (they are simple enums/literals). Alternatively, make `SearchServiceRequest` accept primitive types (strings, ints) and let the search module parse them.

### Theme 3: Cache policy knowledge duplication

**Root cause:** `RuntimeExecutionPolicy` was designed as a unified policy object, but cache policy already existed as `CqCachePolicyV1`. Rather than referencing it, a parallel `CacheRuntimePolicy` was created.

**Affected principles:** P7, P3

**Suggested approach:** Have `RuntimeExecutionPolicy.cache` reference `CqCachePolicyV1` directly. The extra 4 fields in `CqCachePolicyV1` (tree-sitter lanes, SQLite tuning, etc.) are harmless for the runtime policy to carry.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P21 | Rename `summary_contracts.py:SummaryEnvelopeV1` to eliminate naming collision | small | Prevents import confusion and runtime errors |
| 2 | P8 | Add `NonEmptyStr` to `Finding.category`, `NonNegativeFloat` to `RunMeta` timestamps | small | Catches invalid data at construction time |
| 3 | P11 | Change `build_error_result()` to construct summary with error at creation, not via mutation | small | Eliminates hidden mutation in factory |
| 4 | P22 | Document that direct submodule imports are the expected pattern for core | small | Reduces confusion about the public surface |
| 5 | P2 | Move enrichment session preparation out of `report.py` into the orchestrator module | small | Clean separation of rendering from enrichment |

## Recommended Action Sequence

1. **Rename `summary_contracts.py:SummaryEnvelopeV1`** to `SummaryOutputEnvelopeV1` (P21). No dependencies will break because this type is only used in `summary_contracts.py` and its direct consumers. This is the fastest win with the most confusion eliminated.

2. **Add constraint annotations to core schema fields** (P8). Add `NonEmptyStr` to `Finding.category`, `NonNegativeFloat` to `RunMeta.started_ms` and `RunMeta.elapsed_ms`. This is a non-breaking change that tightens the contract.

3. **Consolidate cache policy types** (P7). Make `RuntimeExecutionPolicy.cache` use `CqCachePolicyV1` directly. Remove `CacheRuntimePolicy`. Update `default_runtime_execution_policy()` to construct `CqCachePolicyV1` instead. This eliminates ~60 lines of duplicated code and a drift risk.

4. **Fix reverse dependencies from core to search** (P5). Move `IncrementalEnrichmentModeV1` into `core/types.py`. Change `SearchServiceRequest.mode` to `str | None` and let search parse it. Remove `SettingsFactory.parser_controls()` (let search construct its own settings).

5. **Make core schema types frozen** (P1, P10, P15). This is the largest change but the highest leverage. Make `DetailPayload`, `Finding`, `CqResult`, and `SummaryEnvelopeV1` frozen. Replace all `setattr()`, `__setitem__`, and direct field mutation with `msgspec.structs.replace()`. This will require changes in consuming modules but will surface all mutation sites as compile-time errors.

6. **Extract enrichment orchestration from report.py** (P2, P16). Move `_prepare_render_enrichment_session()` to `render_enrichment_orchestrator.py`. Change `render_markdown()` to accept a pre-computed session.

7. **Split summary_contract.py** (P3). Extract variant resolution tables and mapping logic into `summary_resolution.py`, keeping struct definitions and factories in `summary_contract.py`.
