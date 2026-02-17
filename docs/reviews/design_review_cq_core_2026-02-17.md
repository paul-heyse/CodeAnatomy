# Design Review: tools/cq/core/

**Date:** 2026-02-17
**Scope:** `tools/cq/core/` (all Python files excluding `tests/`)
**Focus:** All principles (1-24)
**Depth:** Deep (all files)
**Files reviewed:** 87 (15,467 LOC)

## Executive Summary

`tools/cq/core/` is a well-structured contract and rendering layer built on frozen `msgspec.Struct` conventions with deterministic serialization, copy-on-write result mutation, and Protocol-based ports. The strongest areas are determinism/reproducibility (P18), composition over inheritance (P13), and declared public contracts (P22). The most significant gaps are dependency direction violations (P5) where `core` imports from higher-level modules (`search`, `macros`, `query`, `orchestration`), a mutable `Section` struct that breaks the frozen contract invariant (P10), and several God modules exceeding 600 LOC that bundle multiple concerns (P3/P19). The `report.py:538` bypass of frozen CqResult via `object.__setattr__` is the single highest-risk finding; it undermines the immutability guarantees that the entire contract system depends on.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | Module-level encoder/decoder singletons are exposed in `__all__`; internal `_infer_summary_variant` hints leaked as module constants |
| 2 | Separation of concerns | 1 | large | medium | `report.py` mixes rendering, enrichment orchestration, deduplication, and section ordering in 614 LOC |
| 3 | SRP | 1 | large | medium | 6 modules exceed 500 LOC with multiple reasons to change |
| 4 | High cohesion, low coupling | 2 | medium | medium | Cache subsystem well-cohesive; core-to-search coupling via runtime imports |
| 5 | Dependency direction | 0 | medium | high | 12+ reverse imports from core into search/macros/query/orchestration |
| 6 | Ports & Adapters | 2 | small | low | `ports.py` defines Protocol ports; `services.py` adapter implementations import concrete modules |
| 7 | DRY | 1 | medium | medium | Cache policy env resolution duplicated; 7 identical `_derive_*_status()` patterns; builtins conversion duplicated |
| 8 | Design by contract | 2 | small | low | `CqStruct` hierarchy enforces `frozen=True`, `kw_only=True`; `NonNegativeInt`/`NonNegativeFloat` constraint types |
| 9 | Parse, don't validate | 2 | small | low | `typed_boundary.py` `convert_lax` provides boundary parsing; `contract_codec.py` parse-at-boundary pattern |
| 10 | Make illegal states unrepresentable | 1 | medium | high | `Section` is mutable; `report.py:538-541` bypasses frozen CqResult; `dict[str, object]` used 150+ times |
| 11 | CQS | 2 | small | low | Copy-on-write helpers in `schema.py` are query-safe; `_apply_render_enrichment_in_place` violates CQS |
| 12 | DI + explicit composition | 2 | small | low | `bootstrap.py` serves as composition root; `SettingsFactory` centralizes construction |
| 13 | Composition over inheritance | 3 | - | - | No deep hierarchies; `CqStruct` is 1-level deep; cache uses mixin composition |
| 14 | Law of Demeter | 2 | small | low | `insight.neighborhood.callers.total` chains in `front_door_render.py:61-64`; generally well-bounded |
| 15 | Tell, don't ask | 2 | small | low | `SummaryEnvelopeV1` exposes mapping protocol (`__getitem__`, `get`, `keys`, `items`) for raw data access |
| 16 | Functional core, imperative shell | 2 | medium | medium | Most logic is pure transforms; `_apply_render_enrichment_in_place` and cache singletons are exceptions |
| 17 | Idempotency | 3 | - | - | Copy-on-write pattern ensures re-rendering is safe; cache writes are content-addressed |
| 18 | Determinism/reproducibility | 3 | - | - | `order="deterministic"` in encoders; SHA256 content hashing; sorted payload normalization |
| 19 | KISS | 1 | medium | medium | God modules; `_to_contract_builtins_recursive` handles 8 type branches; fragment cache has 6-layer stack |
| 20 | YAGNI | 2 | small | low | `CqCacheCoordinationBackend` semaphore/barrier abstractions may be speculative |
| 21 | Least astonishment | 1 | small | medium | Two different `RunSummaryV1` classes; `Section` mutable while siblings frozen; `serialization.py` uses bare `Any` |
| 22 | Declare/version contracts | 3 | - | - | `__all__` on every module; `V1` suffix on all contracts; `SCHEMA_VERSION` constant |
| 23 | Design for testability | 2 | medium | low | Process-global singletons (`_SCHEDULER_STATE`, `_BACKEND_STATE`, `_TELEMETRY`) require reset functions |
| 24 | Observability | 2 | small | low | Cache telemetry is comprehensive; no structured logging in rendering pipeline |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Most internal decisions are well-hidden behind `__all__` declarations and private-prefixed helpers. However, several internals leak through the public surface.

**Findings:**
- `contract_codec.py:137-142` exposes six module-level encoder/decoder singletons (`JSON_ENCODER`, `JSON_DECODER`, `MSGPACK_ENCODER`, etc.) in `__all__` at line 338. These are implementation details of the codec; callers should use `encode_json()`/`decode_json()` instead.
- `contract_codec.py:20-47` exposes summary variant inference hint sets (`_RUN_SUMMARY_HINTS`, `_NEIGHBORHOOD_SUMMARY_HINTS`, etc.) as module constants. While private-prefixed, the inference logic they encode is tightly coupled to `summary_contract.py` variant definitions.
- `entity_kinds.py:114` exposes `ENTITY_KINDS` singleton in `__all__`, coupling callers to the specific `EntityKindRegistry` instance rather than the registry interface.
- `cache/telemetry.py:36-37` uses module-level mutable dicts `_TELEMETRY` and `_SEEN_KEYS` as process-global state, hidden from callers but not encapsulated behind a class boundary.

**Suggested improvement:**
Remove `JSON_ENCODER`, `JSON_DECODER`, `MSGPACK_ENCODER`, `MSGPACK_DECODER`, `JSON_RESULT_DECODER`, `MSGPACK_RESULT_DECODER` from `contract_codec.py`'s `__all__`. Callers should use the function-level API exclusively.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
Several modules mix policy/domain logic with IO, rendering, and orchestration concerns.

**Findings:**
- `report.py` (614 LOC) combines: (1) render enrichment session preparation (lines 492-536), (2) in-place frozen struct mutation (lines 538-541), (3) section rendering with deduplication (lines 375-460), (4) markdown assembly and footer generation (lines 544-600). These are four distinct concerns that change for different reasons.
- `render_summary.py` (557 LOC) mixes: (1) summary key-value rendering, (2) insight card extraction and rendering, (3) diagnostic status derivation (`_derive_*_status()` at lines ~350-520), (4) condensed rendering for compact output.
- `front_door_assembly.py` (769 LOC) combines: (1) search insight assembly, (2) entity insight assembly, (3) calls insight assembly, (4) neighborhood merging, (5) degradation derivation, (6) semantic augmentation.
- `summary_contract.py` (663 LOC) bundles: (1) five summary variant class definitions, (2) the `SummaryV1` union type, (3) variant resolution and inference, (4) summary mapping application, (5) the `SummaryEnvelopeV1` wrapper with mapping protocol.

**Suggested improvement:**
Extract `report.py`'s render enrichment orchestration (lines 492-541) into a dedicated `render_enrichment_apply.py` module. Extract `render_summary.py`'s diagnostic status derivation functions into `diagnostics_status.py`. Split `front_door_assembly.py` into per-macro assemblers (`front_door_search.py`, `front_door_entity.py`, `front_door_calls.py`).

**Effort:** large
**Risk if unaddressed:** medium -- Changes to rendering, enrichment, or insight assembly have high blast radius due to co-location.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
Six modules exceed 500 LOC with multiple reasons to change.

**Findings:**
- `front_door_assembly.py` (769 LOC): Changes for search insight format, entity insight format, calls insight format, neighborhood merging logic, or degradation scoring.
- `diskcache_backend.py` (700 LOC): Changes for read/write operations, maintenance operations, streaming operations, or coordination primitives.
- `summary_contract.py` (663 LOC): Changes for new summary variants, variant resolution logic, mapping application, or envelope protocol.
- `enrichment_facts.py` (642 LOC): Changes for new fact cluster definitions (~380 LOC of declarative data), fact resolution logic, or fact rendering.
- `report.py` (614 LOC): Changes for rendering format, enrichment orchestration, deduplication logic, or section ordering.
- `render_summary.py` (557 LOC): Changes for summary display format, insight card format, diagnostic status derivation, or condensed output format.

**Suggested improvement:**
Priority decomposition targets: (1) Split `front_door_assembly.py` by macro type. (2) Move `enrichment_facts.py`'s `FACT_CLUSTERS` declarative data into a separate `fact_cluster_specs.py`. (3) Extract `diskcache_backend.py` mixins into standalone modules under `cache/`.

**Effort:** large
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The cache subsystem demonstrates good internal cohesion with clean module boundaries (`key_builder.py`, `namespaces.py`, `telemetry.py`, `fragment_contracts.py`, etc.). The core schema/contract layer is also well-cohesive. Coupling issues arise at the core-to-search boundary.

**Findings:**
- `services.py:14` imports `CallsRequest` from `tools.cq.macros.contracts` at module scope (not deferred), creating a hard coupling from core to macros.
- `services.py:19` imports `SearchLimits` from `tools.cq.search._shared.types` under TYPE_CHECKING, but `services.py:48` uses it as a runtime type annotation in `SearchServiceRequest.limits` field.
- `contracts.py:21` imports `SearchSummaryContract` from `tools.cq.search._shared.search_contracts` under TYPE_CHECKING, coupling the core contract boundary types to search internals.
- Cache subsystem has 28 files -- while internally cohesive, the sheer number of modules creates navigation overhead.

**Suggested improvement:**
Move `CallsRequest` type definition to `tools/cq/core/contracts.py` or create a shared protocol that both core and macros depend on. The `SearchLimits` type should either be defined in core or replaced with a core-owned protocol.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P5. Dependency direction -- Alignment: 0/3

**Current state:**
`tools/cq/core/` is positioned as the foundational layer that higher-level modules (`search`, `query`, `macros`, `orchestration`) depend on. However, core itself imports from these higher layers in 12+ locations, creating circular conceptual dependencies.

**Findings:**
- `services.py:14` -- module-scope import: `from tools.cq.macros.contracts import CallsRequest`
- `services.py:66` -- runtime import: `from tools.cq.query.entity_front_door import attach_entity_front_door_insight`
- `services.py:84` -- runtime import: `from tools.cq.macros.calls import cmd_calls`
- `services.py:99-100` -- runtime imports: `from tools.cq.search._shared.types import QueryMode` and `from tools.cq.search.pipeline.smart_search import smart_search`
- `contracts.py:21` -- TYPE_CHECKING import: `from tools.cq.search._shared.search_contracts import SearchSummaryContract`
- `contracts.py:60` -- runtime import: `from tools.cq.search._shared.search_contracts import summary_contract_to_dict`
- `scoring.py:16` -- TYPE_CHECKING import: `from tools.cq.macros.contracts import ...`
- `render_context.py:22` -- runtime import: `from tools.cq.orchestration.render_enrichment import ...`
- `settings_factory.py:21` -- TYPE_CHECKING import: `from tools.cq.search.tree_sitter.core.infrastructure import ParserControlSettingsV1`
- `settings_factory.py:66-67` -- runtime import: `from tools.cq.search.tree_sitter.core.infrastructure import parser_controls_from_env`
- `report.py:606` -- module-scope re-export: `from tools.cq.core.render_summary import ARTIFACT_ONLY_KEYS`

**Suggested improvement:**
Define Protocol-based ports in `core/ports.py` for every capability that core needs from higher layers (search execution, calls execution, entity front-door, parser controls). The `services.py` module should become a pure adapter that receives these implementations via DI at the `bootstrap.py` composition root, rather than importing concrete implementations. Move `CallsRequest` to `core/contracts.py`. Define a `SearchSummaryContractProtocol` in core.

**Effort:** medium
**Risk if unaddressed:** high -- Circular dependencies prevent independent testing, make the dependency graph fragile, and violate the architectural layering contract that all other modules assume.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`ports.py` defines clean Protocol-based ports (`SearchServicePort`, `EntityServicePort`, `CallsServicePort`, `CachePort`, `RenderEnrichmentPort`). `services.py` provides concrete implementations. However, the services themselves contain deferred imports to concrete modules, which partially defeats the purpose.

**Findings:**
- `ports.py` (84 LOC) is well-designed with `Protocol` ports that define narrow interfaces.
- `services.py` adapters use deferred runtime imports (lines 66, 84, 99-100) rather than receiving implementations via constructor injection. This means the port/adapter boundary is "leaky" -- the adapters know their concrete implementations rather than receiving them.
- `bootstrap.py:44` defines `_RUNTIME_SERVICES` as a module-level mutable dict serving as the composition root, but construction logic is inline rather than declarative.
- `cache/interface.py` defines `CqCacheBackend`, `CqCacheStreamingBackend`, `CqCacheCoordinationBackend` as Protocol types with a `NoopCacheBackend` implementation -- a clean ports pattern.

**Suggested improvement:**
Refactor `services.py` to accept concrete implementations via constructor parameters rather than deferred imports. Wire implementations at `bootstrap.py` composition root using explicit DI.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
Several semantic duplications exist where the same business truth is encoded in multiple places.

**Findings:**
- Cache policy env resolution is duplicated between `cache/policy.py` (lines 92-169, `_resolve_cache_scalar_settings`) and `runtime/execution_policy.py` (lines 40-90, `default_runtime_execution_policy`). Both independently parse `CQ_CACHE_*` environment variables with overlapping logic.
- `render_summary.py` contains 7 nearly-identical `_derive_*_status()` functions (approximately lines 350-520) that each follow the same pattern: extract a field from summary, test a condition, return a status string. These encode the same structural rule with different field names.
- `_to_contract_builtins_recursive` in `contract_codec.py:145-171` and `_summary_value_to_builtins` in `summary_contract.py` both implement recursive normalization of msgspec structs to built-in types, with overlapping type-dispatch logic.
- `snapshot_fingerprint.py:84-88` (`_safe_rel_path`) and `scope_services.py:113-117` (`_to_rel_path`) implement identical path-relative-to-root logic.
- `parse_namespace_int_overrides` and `parse_namespace_bool_overrides` in `runtime/env_namespace.py:71-128` share identical loop structure differing only in the parse step.

**Suggested improvement:**
Extract a shared `resolve_cache_env_scalars()` function used by both `cache/policy.py` and `runtime/execution_policy.py`. Extract a generic `_derive_status_from_summary(summary, key, condition_fn, status_labels)` helper to replace the 7 `_derive_*_status()` functions. Consolidate `_safe_rel_path` and `_to_rel_path` into a single helper in `pathing.py`.

**Effort:** medium
**Risk if unaddressed:** medium -- Drift between duplicated env parsing or status derivation logic causes inconsistent behavior.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The `CqStruct` hierarchy enforces `frozen=True` and `kw_only=True` at the base level. Constraint types like `NonNegativeInt`, `NonNegativeFloat`, `PositiveInt` in `contracts_constraints.py` provide boundary validation. `enforce_mapping_constraints` validates mapping payloads.

**Findings:**
- `contracts_constraints.py:14-30` defines `NonNegativeInt`, `NonNegativeFloat`, `PositiveInt` as `Annotated` types with `msgspec.Meta(ge=0)` constraints -- clean contract enforcement.
- `schema.py:290` `Section` lacks `frozen=True`, breaking the immutability postcondition that `CqResult.sections` implies (since `CqResult` at line 362 IS frozen).
- `typed_boundary.py` provides `convert_lax` and `convert_strict` with `BoundaryDecodeError` -- explicit precondition/postcondition for boundary crossing.
- Docstrings consistently document Parameters/Returns/Raises sections (NumPy convention) across most modules.

**Suggested improvement:**
Add `frozen=True` to `Section` class at `schema.py:290`. Change `Section.findings` from `list[Finding]` to `tuple[Finding, ...]` to match the immutable tuple convention used by `CqResult.key_findings` and `CqResult.evidence`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Boundary parsing is generally well-implemented via `typed_boundary.py`'s `convert_lax`/`convert_strict` and `contract_codec.py`'s decode functions. The `_decode_result_payload` function in `contract_codec.py:174-223` demonstrates good parse-at-boundary practice.

**Findings:**
- `contract_codec.py:174-223` (`_decode_result_payload`) parses raw mapping payloads into typed `CqResult` using `msgspec.convert` for each field -- a textbook parse-don't-validate pattern.
- `front_door_render.py:137-155` (`coerce_front_door_insight`) properly converts raw payloads to typed `FrontDoorInsightV1` at boundary using `convert_lax`.
- `cache/fragment_codecs.py:17-28` (`decode_fragment_payload`) implements two-stage decode (msgpack first, mapping fallback) -- correct boundary handling.
- Scattered `isinstance` checks persist in rendering code (e.g., `render_overview.py:123`, `render_overview.py:152`, `render_overview.py:207-208`) that re-validate already-parsed types.

**Suggested improvement:**
The scattered `isinstance` checks in rendering code could be reduced by ensuring the summary envelope exposes typed accessors that return `None` for missing fields, removing the need for repeated type narrowing.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 1/3

**Current state:**
The frozen struct hierarchy and constraint types prevent many illegal states. However, two significant gaps exist: the mutable `Section` struct and the pervasive `dict[str, object]` anti-pattern.

**Findings:**
- `schema.py:290` -- `Section` is `class Section(msgspec.Struct):` without `frozen=True`, while its container `CqResult` at line 362 IS frozen. This means a frozen `CqResult` holds references to mutable `Section` objects, creating an inconsistency where the outer container promises immutability but inner sections can be silently mutated.
- `report.py:538` -- `object.__setattr__(result, "key_findings", ...)` bypasses `CqResult`'s frozen guarantee. Line 541 directly mutates `section.findings = [...]`. This is an intentional workaround for in-place render enrichment, but it breaks the frozen contract that all other code depends on.
- `dict[str, object]` appears 150+ times across 34 files as a payload type. This untyped intermediate representation allows arbitrary key/value combinations, making it impossible to enforce field presence, type, or constraint invariants at the type level. Examples: `ContractEnvelope.payload` at `contracts.py:30`, `SummaryEnvelopeV1.step_summaries` field type, `diagnostics_contracts.py` diagnostic payloads.
- `RunContext.argv` at `run_context.py:23` is `list[str]` -- a mutable field on a frozen struct. While msgspec freezes the struct slot, the list itself can still be mutated by callers holding a reference.
- `SemanticNeighborhoodBundleV1.node_index` at `snb_schema.py:289` is `dict[str, SemanticNodeRefV1] | None` -- mutable dict on a frozen struct.

**Suggested improvement:**
(1) Make `Section` frozen and change `findings` to `tuple[Finding, ...]`. (2) Replace `report.py:538-541`'s `object.__setattr__` with a proper copy-on-write approach using `msgspec.structs.replace`. (3) Progressively replace `dict[str, object]` with typed `msgspec.Struct` contracts at boundary crossing points, starting with `ContractEnvelope.payload`. (4) Change mutable collection fields on frozen structs to immutable equivalents (`tuple` instead of `list`, `types.MappingProxyType` or frozen dict pattern instead of `dict`).

**Effort:** medium
**Risk if unaddressed:** high -- The `object.__setattr__` bypass can cause silent corruption of shared frozen instances; the mutable `Section` breaks assumptions of downstream code that expects `CqResult` immutability.

---

#### P11. CQS (Command-Query Separation) -- Alignment: 2/3

**Current state:**
The copy-on-write helpers in `schema.py` (e.g., `append_result_key_finding`, `extend_result_evidence`) correctly implement CQS by returning new instances rather than mutating. One significant violation exists.

**Findings:**
- `schema.py` copy-on-write helpers (lines 439-595) are well-designed: they return new `CqResult` instances via `msgspec.structs.replace` without mutating inputs.
- `report.py:538-541` (`_apply_render_enrichment_in_place`) violates CQS by mutating its input (`result`) via `object.__setattr__` while being called from `render_markdown` (line 568). The function name includes "in_place" which at least signals the mutation, but the calling context treats it as a void command that modifies a supposedly-frozen input.
- `cache/telemetry.py` recording functions (`record_cache_get`, `record_cache_set`, etc.) are pure commands that mutate global state -- correctly separated from the `snapshot_cache_telemetry` query function.
- `cache/content_hash.py:25` `file_content_hash` reads filesystem (query) and updates a process-local cache (command) in the same call. The caching is an internal optimization, so this is a minor violation.

**Suggested improvement:**
Replace `_apply_render_enrichment_in_place` with a pure function that returns a new `CqResult` with enrichment applied, using the existing copy-on-write helpers.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
`bootstrap.py` serves as the composition root with `build_cq_runtime_services()` constructing the service bundle. `SettingsFactory` in `settings_factory.py` centralizes construction of policy objects.

**Findings:**
- `bootstrap.py:44` uses a module-level mutable `_RUNTIME_SERVICES: dict[str, CqRuntimeServices]` for process-scoped caching. The composition root pattern is sound but the singleton caching adds hidden state.
- `settings_factory.py` provides static factory methods but uses deferred imports (lines 66, 83) to avoid circular dependencies, indicating the dependency graph is not cleanly acyclic.
- `cache/backend_lifecycle.py:21` uses `_BACKEND_STATE` singleton with `_BACKEND_LOCK` for thread-safe backend management -- necessary for process-global cache state but adds hidden coupling.

**Suggested improvement:**
Consider making `_RUNTIME_SERVICES` accept explicitly provided service bundles (for testing) via a `set_runtime_services` function, reducing the need for monkeypatching in tests.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
The codebase strongly favors composition. The `CqStruct` hierarchy is exactly one level deep (base -> leaf). Cache capabilities are composed via mixin classes. No deep inheritance trees exist.

**Findings:**
- `structs.py:12-48` defines a flat hierarchy: `CqStruct` -> `CqSettingsStruct`, `CqOutputStruct`, `CqStrictOutputStruct`, `CqCacheStruct`. All are leaves -- no further subclassing observed.
- `cache/diskcache_backend.py` uses mixin composition (`_DiskcacheReadWriteMixin`, `_DiskcacheMaintenanceMixin`, `_DiskcacheStreamingMixin`, `_DiskcacheCoordinationMixin`) to compose the final `_DiskcacheBackend` class. While the mixins are large, the composition pattern itself is sound.
- `fragment_engine.py:31-50` uses `@dataclass(frozen=True, slots=True)` for `FragmentProbeRuntimeV1` and `FragmentPersistRuntimeV1` -- function bundles composed at runtime rather than inherited.
- No use of `abc.ABC` or deep class hierarchies anywhere in the 87 files.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code accesses direct collaborators. A few chains reach through nested structs.

**Findings:**
- `front_door_render.py:61-64` chains through `neighborhood.callers.total`, `neighborhood.callees.total`, `neighborhood.references.total`, `neighborhood.hierarchy_or_scope.total` -- four levels of attribute access on nested frozen structs.
- `artifacts.py:122-126` chains through `insight.neighborhood.callers`, `insight.neighborhood.callees`, etc., and then accesses `.preview`, `.total`, `.source`, `.availability` on each.
- `render_overview.py:122-123` accesses `summary.python_semantic_overview` then iterates its keys -- two levels through the summary envelope.
- These chains are on frozen value objects (not service collaborators), which significantly reduces the coupling concern.

**Suggested improvement:**
The chains are on value objects, so the Demeter risk is primarily about structural coupling to the insight data model. Consider adding convenience accessors on `InsightNeighborhoodV1` (e.g., `summary_counts() -> dict[str, int]`) to reduce direct field traversal in rendering code.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`SummaryEnvelopeV1` intentionally exposes a mapping protocol for backward compatibility, which encourages "ask" patterns in rendering code.

**Findings:**
- `summary_contract.py`'s `SummaryEnvelopeV1` implements `__getitem__`, `get`, `keys`, `items` -- exposing raw data access that encourages callers to pull data and make decisions externally rather than asking the summary to render itself.
- `render_summary.py` extensively uses `summary_value(result, key=...)` pattern to extract individual summary fields for rendering decisions -- a classic "ask" pattern.
- `enrichment_facts.py` `FACT_CLUSTERS` declarative data encapsulates fact resolution rules well, but `_resolve_fact_value` at the resolution layer still asks the finding for individual attribute values.

**Suggested improvement:**
This is a deliberate design choice for rendering flexibility -- the summary needs to be inspectable from multiple rendering paths. No immediate change recommended, but adding `render_*` methods to `SummaryEnvelopeV1` for common rendering patterns would reduce scattered ask-then-render logic.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Most core logic consists of pure transformations on frozen structs. IO and side effects are concentrated at the edges (cache backend, filesystem, subprocess).

**Findings:**
- Copy-on-write helpers in `schema.py` are pure functions returning new instances -- textbook functional core.
- `_apply_render_enrichment_in_place` in `report.py:538-541` is an imperative mutation embedded in the rendering pipeline, breaking the functional core pattern.
- `cache/telemetry.py` uses module-level mutable state (`_TELEMETRY`, `_SEEN_KEYS`) as a process-global side-effect sink. The recording functions are imperative commands; `snapshot_cache_telemetry` is a pure query.
- `cache/content_hash.py` mixes filesystem IO (reading file bytes, stat) with memoization cache mutation in `file_content_hash`.
- `worker_scheduler.py:170` `_SCHEDULER_STATE` is a process-global mutable singleton that manages executor lifecycle.
- `toolchain.py` `Toolchain.detect()` runs `subprocess` for ripgrep detection -- correctly at the shell edge.

**Suggested improvement:**
Replace `_apply_render_enrichment_in_place` with a pure transformation that returns a new result, matching the copy-on-write pattern used elsewhere. The cache telemetry and content hash memoization are acceptable imperative shell concerns.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Operations are naturally idempotent due to the copy-on-write pattern and content-addressed caching.

**Findings:**
- Rendering the same `CqResult` multiple times produces the same markdown output (given the same render context). The copy-on-write helpers do not mutate inputs.
- Cache writes use content-addressed keys (`build_cache_key` in `key_builder.py:73-98`) with SHA256 digests, making repeated writes idempotent.
- `blob_store.py:99` checks `if not path.exists()` before writing, preventing duplicate filesystem writes.
- The only idempotency risk is the `_apply_render_enrichment_in_place` mutation, which if applied twice could corrupt findings.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P18. Determinism/reproducibility -- Alignment: 3/3

**Current state:**
Determinism is a first-class design concern throughout the module.

**Findings:**
- `contract_codec.py:137` `JSON_ENCODER = msgspec.json.Encoder(order="deterministic")` -- canonical deterministic encoding.
- `contract_codec.py:155-158` (`_to_contract_builtins_recursive`) sorts mapping items by key and set/frozenset items by repr for deterministic output.
- `key_builder.py:97` uses `msgspec.json.encode` on canonicalized payloads with `hashlib.sha256` for deterministic cache key generation.
- `snapshot_fingerprint.py:292` computes digest from deterministically-ordered payload.
- `id.py` provides `canonicalize_payload` for deterministic payload normalization.
- `uuid_temporal_contracts` (imported by `run_context.py:10`) uses `uuid7` for temporal ordering.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 1/3

**Current state:**
Several areas introduce unnecessary complexity through over-layered abstractions or oversized modules.

**Findings:**
- The fragment cache system spans 6 files with 4 layers: `fragment_contracts.py` -> `fragment_engine.py` -> `fragment_orchestrator.py` -> `fragment_codecs.py` plus `typed_codecs.py` and `cache_decode.py`. This is a lot of abstraction for a cache probe/persist cycle.
- `_to_contract_builtins_recursive` in `contract_codec.py:145-171` handles 8 type branches (`Struct`, `Mapping`, `list/tuple`, `set/frozenset`, `bytes`, primitives, and fallback `msgspec.to_builtins`). The function is conceptually simple but structurally complex.
- `search_artifact_store.py` (358 LOC) uses diskcache's `Deque` and `Index` types with Protocol wrappers, transaction context managers, and global/run-scoped index/order structures -- significant complexity for artifact storage.
- `_transaction_context` in `search_artifact_store.py:123-137` has 4 nested try/except blocks for a context manager resolution.
- `cache/cache_runtime_tuning.py:64-84` `apply_cache_runtime_tuning` uses `getattr` + `suppress(...)` pattern 6 times for best-effort tuning application -- defensive but adds cognitive load.

**Suggested improvement:**
Consider collapsing the fragment cache layers. `fragment_engine.py` + `fragment_orchestrator.py` could merge into a single module. The `search_artifact_store.py` complexity is inherent to the diskcache integration but could benefit from extracting the index management into a dedicated `search_artifact_index.py`.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most abstractions serve clear, current use cases. A few may be speculative.

**Findings:**
- `CqCacheCoordinationBackend` in `cache/interface.py` defines `semaphore`, `lock`, `rlock`, and `barrier` coordination primitives. `coordination.py` uses only `tree_sitter_lane_guard` and `publish_once_per_barrier`. The full semaphore/lock/rlock trio may be over-specified for the single use case of tree-sitter lane limiting.
- `cache/base_contracts.py` defines `LaneCoordinationPolicyV1` with 5 configurable parameters (`semaphore_key`, `lock_key_suffix`, `reentrant_key_suffix`, `lane_limit`, `ttl_seconds`) for what is currently a single fixed coordination pattern.
- `ArtifactPointerV1` in `snb_schema.py:14-41` includes `storage_path` and `metadata` fields that appear to be forward-looking for future artifact storage scenarios.

**Suggested improvement:**
No immediate action needed. The abstractions are small and don't add significant maintenance burden. Monitor for unused coordination capabilities over time.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
Several naming and behavioral inconsistencies would surprise a competent reader.

**Findings:**
- Two different `RunSummaryV1` classes exist: `summary_contract.py:289` defines a `RunSummaryV1` that is a summary variant for run-type results, while `summary_contracts.py:49` (note the plural filename) defines a DIFFERENT `RunSummaryV1` for run command summary payloads. A developer searching for "RunSummaryV1" would find two classes with the same name but different purposes and schemas.
- `Section` at `schema.py:290` is mutable while its siblings (`Finding`, `CqResult`, `Artifact`, `RunMeta`) are all frozen. A reader familiar with the frozen convention would naturally assume `Section` is also frozen.
- `serialization.py:47,58,80` uses bare `Any` type annotations (`def dumps_msgpack(value: Any) -> bytes`, `def loads_msgpack(...) -> Any`, `def to_builtins(value: Any) -> Any`) which violates the project's "no bare `Any`" rule and is inconsistent with the `object` type used elsewhere in the codebase.
- `report.py:604` has a module-scope re-export `render_summary_compact = render_summary_condensed` creating an alias that might confuse readers about which name is canonical.
- `front_door_render.py:158-168` `to_public_front_door_insight_dict` defers to a DIFFERENT module (`front_door_serialization.py`) for the actual implementation -- a reader of the render module would expect the serialization to live there.

**Suggested improvement:**
(1) Rename `summary_contracts.py`'s `RunSummaryV1` to `RunCommandSummaryV1` to disambiguate from the summary variant type. (2) Make `Section` frozen. (3) Replace `Any` with `object` in `serialization.py`. (4) Remove the `render_summary_compact` alias or document its deprecation.

**Effort:** small
**Risk if unaddressed:** medium -- Naming collisions cause import confusion and bugs when the wrong type is used.

---

#### P22. Declare and version public contracts -- Alignment: 3/3

**Current state:**
Contract versioning and public surface declaration are excellent throughout.

**Findings:**
- Every module has an `__all__` list explicitly declaring its public surface.
- All contract structs use `V1` suffix (`CqResult`, `SummaryEnvelopeV1`, `FrontDoorInsightV1`, `FragmentRequestV1`, `CacheNamespaceTelemetry`, etc.) for explicit versioning.
- `schema.py:15` defines `SCHEMA_VERSION = "cq.result.v1.1"` as a versioned constant.
- `snb_schema.py:308` defines `schema_version: str = "cq.snb.v1"` on `SemanticNeighborhoodBundleV1`.
- `snb_registry.py:19-28` maps kind strings to struct types for runtime validation.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Most logic is testable via pure function inputs/outputs on frozen structs. Process-global singletons require explicit reset functions for test isolation.

**Findings:**
- `cache/telemetry.py:169` provides `reset_cache_telemetry()` for test cleanup.
- `cache/content_hash.py:65` provides `reset_file_content_hash_cache()` for test cleanup.
- `cache/backend_lifecycle.py:63-64` provides `set_cq_cache_backend()` for test injection and `close_cq_cache_backend()` for cleanup.
- `worker_scheduler.py:170` `_SCHEDULER_STATE` is a module-level dict requiring `close_worker_scheduler()` for cleanup, but no `reset` function exists for test isolation.
- `bootstrap.py:44` `_RUNTIME_SERVICES` is a module-level dict with no `reset` function. Tests must monkeypatch or manage state manually.
- `details_kinds.py:19` `DETAILS_KIND_REGISTRY` is a mutable module-level dict that tests could accidentally modify without reset.
- `report.py:538-541`'s in-place mutation makes render testing stateful -- calling `render_markdown` twice on the same result could produce different outputs due to the mutation side effect.

**Suggested improvement:**
Add `reset_runtime_services()` to `bootstrap.py` and `reset_worker_scheduler()` to `worker_scheduler.py` for test isolation. Make `DETAILS_KIND_REGISTRY` in `details_kinds.py` an `ImmutableRegistry` or frozen dict.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Cache telemetry is comprehensive with per-namespace counters for gets, hits, misses, sets, failures, evictions, timeouts, and aborts. Rendering and contract boundary operations lack structured observability.

**Findings:**
- `cache/telemetry.py` provides 11 recording functions covering all cache operation types with per-namespace granularity -- excellent cache observability.
- `cache/diagnostics.py` provides `snapshot_backend_metrics` for runtime cache state inspection.
- `diagnostics_contracts.py` defines structured diagnostic artifact payloads with `build_diagnostics_artifact_payload`.
- No structured logging exists in the rendering pipeline (`report.py`, `render_summary.py`, `render_overview.py`). Rendering failures would be silent.
- No timing/tracing instrumentation in `contract_codec.py` encode/decode paths or `front_door_assembly.py` insight assembly.
- `worker_scheduler.py` pool creation and task dispatch has no observability beyond the pool creation itself.

**Suggested improvement:**
Add structured timing to `render_markdown` in `report.py` (total render time, enrichment time, assembly time) as summary-level telemetry fields. Add structured logging for cache backend initialization failures in `backend_lifecycle.py`.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Dependency Direction Inversion Needed at Core Boundary

**Root cause:** `tools/cq/core/` was designed as the foundational layer but gradually accumulated imports from higher-level modules (`search`, `macros`, `query`, `orchestration`) to support service execution, summary contract serialization, and settings construction.

**Affected principles:** P5 (dependency direction), P6 (ports & adapters), P4 (coupling), P23 (testability).

**Suggested approach:** Define Protocol-based ports in `core/ports.py` for all capabilities that core needs from higher layers. Move type definitions (`CallsRequest`, `SearchLimits`, `SearchSummaryContract`) that core depends on into core-owned contracts or shared Protocol definitions. Wire concrete implementations at the `bootstrap.py` composition root.

### Theme 2: Frozen Contract Integrity Breach

**Root cause:** `Section` was left mutable (likely for backward compatibility with the render enrichment pipeline), and `report.py` uses `object.__setattr__` to bypass frozen CqResult as a performance optimization for in-place enrichment.

**Affected principles:** P10 (illegal states), P11 (CQS), P16 (functional core), P17 (idempotency), P21 (least astonishment).

**Suggested approach:** Make `Section` frozen with `tuple[Finding, ...]` findings field. Replace `_apply_render_enrichment_in_place` with a pure copy-on-write transformation. This aligns with the existing `append_result_key_finding` pattern that already demonstrates the correct approach.

### Theme 3: God Modules Need Decomposition

**Root cause:** Several modules organically grew to exceed 600 LOC by accumulating related but independently-changing concerns (e.g., `front_door_assembly.py` handling three different macro types, `report.py` handling rendering + enrichment + deduplication).

**Affected principles:** P2 (separation of concerns), P3 (SRP), P19 (KISS).

**Suggested approach:** Decompose by axis of change: (1) `front_door_assembly.py` splits by macro type (search, entity, calls). (2) `report.py` splits by concern (rendering, enrichment, assembly). (3) `summary_contract.py` splits by variant definitions vs. resolution logic. (4) `diskcache_backend.py` mixin modules become standalone. Target: no module exceeds 400 LOC.

### Theme 4: Untyped `dict[str, object]` as Intermediate Payload

**Root cause:** The codec/serialization boundary uses `dict[str, object]` as a universal intermediate representation for JSON/msgpack payloads. While convenient for serialization boundaries, this type carries no structural information.

**Affected principles:** P10 (illegal states), P8 (design by contract), P9 (parse don't validate).

**Suggested approach:** Progressively replace `dict[str, object]` with typed `msgspec.Struct` contracts at the highest-traffic boundary crossings. Start with `ContractEnvelope.payload` and `SummaryEnvelopeV1.step_summaries`. Keep `dict[str, object]` only at the codec layer where it is the natural wire representation.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P10 | Make `Section` frozen at `schema.py:290`; change `findings` to `tuple[Finding, ...]` | small | Restores frozen contract invariant across entire result tree |
| 2 | P10/P11 | Replace `object.__setattr__` at `report.py:538-541` with copy-on-write | small | Eliminates the only frozen bypass in the codebase |
| 3 | P21 | Rename `summary_contracts.py:49` `RunSummaryV1` to `RunCommandSummaryV1` | small | Eliminates naming collision that causes import confusion |
| 4 | P1 | Remove encoder/decoder singletons from `contract_codec.py` `__all__` | small | Reduces exposed internal implementation details |
| 5 | P21 | Replace `Any` with `object` in `serialization.py:47,58,80` | small | Aligns with project `no bare Any` rule |

## Recommended Action Sequence

1. **Freeze `Section` and fix `report.py` mutation** (P10, P11, P16) -- Make `Section` frozen, change `findings` to `tuple[Finding, ...]`, and replace `object.__setattr__` in `report.py:538-541` with a copy-on-write `msgspec.structs.replace` chain. This is the foundation: all other immutability assumptions depend on this.

2. **Rename conflicting `RunSummaryV1`** (P21) -- Rename `summary_contracts.py:49`'s `RunSummaryV1` to `RunCommandSummaryV1` (or similar) and update imports. This prevents confusion in the existing codebase and for future contributors.

3. **Clean up `contract_codec.py` public surface** (P1) -- Remove raw encoder/decoder singletons from `__all__`. Replace `Any` with `object` in `serialization.py`.

4. **Extract render enrichment from `report.py`** (P2, P3) -- Move `_prepare_render_enrichment_session` and the enrichment application logic into a dedicated `render_enrichment_apply.py`. This reduces `report.py` to ~400 LOC focused on markdown assembly.

5. **Consolidate cache policy env resolution** (P7) -- Extract shared `_resolve_cache_env_scalar()` helper used by both `cache/policy.py` and `runtime/execution_policy.py`. This eliminates the most dangerous semantic duplication (environment variable parsing drift).

6. **Extract `_derive_*_status()` pattern** (P7) -- Replace 7 nearly-identical status derivation functions in `render_summary.py` with a generic `_derive_status_from_summary(summary, key, condition, labels)` helper.

7. **Fix dependency direction violations in `services.py`** (P5) -- Move `CallsRequest` to `core/contracts.py`. Define protocols for search/entity/calls execution. Wire concrete implementations via DI at `bootstrap.py` rather than deferred imports.

8. **Decompose `front_door_assembly.py`** (P3, P19) -- Split into `front_door_search.py`, `front_door_entity.py`, `front_door_calls.py`, and `front_door_neighborhood.py`.

9. **Simplify fragment cache layers** (P19) -- Merge `fragment_engine.py` and `fragment_orchestrator.py` into a single module. The 4-layer orchestration stack is over-engineered for the current probe/persist pattern.

10. **Add test reset functions for global singletons** (P23) -- Add `reset_runtime_services()` to `bootstrap.py`, `reset_worker_scheduler()` to `worker_scheduler.py`, and freeze `DETAILS_KIND_REGISTRY` in `details_kinds.py`.
