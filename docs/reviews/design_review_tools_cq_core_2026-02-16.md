# Design Review: tools/cq/core/

**Date:** 2026-02-16
**Scope:** `tools/cq/core/` (excluding `tools/cq/core/cache/`)
**Focus:** Boundaries (1-6), Knowledge (7-11), Simplicity (19-22)
**Depth:** deep
**Files reviewed:** 44 (all Python files in `tools/cq/core/` excluding `cache/`, `tests/`, `renderers/`)

## Executive Summary

The CQ core module provides a well-structured contract and rendering infrastructure with clear msgspec-based schema definitions and a coherent front-door insight pipeline. The strongest design areas are the typed boundary layer (`typed_boundary.py`), the `CqStruct` hierarchy (`structs.py`), and the contract codec (`contract_codec.py`), which together enforce disciplined serialization boundaries. The primary design gaps are: (1) `front_door_builders.py` conflates schema definitions, builder logic, rendering, and policy in a single 1,113-LOC file; (2) module-level mutable singletons in `report.py` (`_RENDER_ENRICHMENT_PORT_STATE`) and `bootstrap.py` (`_RUNTIME_SERVICES`) bypass the otherwise clean dependency injection architecture; (3) the `CqSummary` struct (168 fields) has become a catch-all with dict-like mutation interfaces that undermine the frozen-struct contract philosophy.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | medium | Module singletons `_RENDER_ENRICHMENT_PORT_STATE` and `_RUNTIME_SERVICES` expose mutable lifecycle through free functions |
| 2 | Separation of concerns | 1 | medium | medium | `front_door_builders.py` mixes schemas, builders, rendering, and risk policy |
| 3 | SRP (one reason to change) | 1 | medium | medium | `front_door_builders.py` changes for schema evolution, risk policy, rendering, or builder logic |
| 4 | High cohesion, low coupling | 2 | small | low | `front_door_schema.py`, `front_door_contracts.py`, `front_door_render.py` are pure re-export shims |
| 5 | Dependency direction | 2 | small | low | Core ports protocol is clean; `report.py` depends on `render_enrichment_orchestrator` which reaches into `query.language` |
| 6 | Ports & Adapters | 2 | small | low | `ports.py` defines clean protocols; adapter wiring in `bootstrap.py` is explicit |
| 7 | DRY (knowledge) | 1 | medium | medium | `_summary_value()` helper duplicated in `front_door_builders.py:46` and `render_summary.py:58` |
| 8 | Design by contract | 2 | small | low | `typed_boundary.py` enforces msgspec conversion; `CqSummary` mutation silently accepts unknown-key writes via `_set_known_field` |
| 9 | Parse, don't validate | 2 | small | low | `DetailPayload.from_legacy()` and `coerce_front_door_insight()` parse at boundaries |
| 10 | Make illegal states unrepresentable | 1 | large | medium | `CqSummary` has 168 optional fields; `DetailPayload` exposes `dict[str, object]` as mutable `data` |
| 11 | CQS | 1 | small | medium | `assign_result_finding_ids()` mutates findings in-place; `merge_enrichment_details()` mutates findings |
| 19 | KISS | 2 | small | low | Fact cluster spec system is complex but justified by multi-language enrichment |
| 20 | YAGNI | 2 | small | low | `decode_yaml_strict` in typed_boundary.py appears unused but is low-cost |
| 21 | Least astonishment | 1 | medium | medium | `CqSummary` looks frozen but supports `__setitem__`; `Finding` struct has `__post_init__` mutation |
| 22 | Declare and version public contracts | 2 | small | low | Schemas carry `schema_version` and `_v1` suffixes; `SCHEMA_VERSION` centralized |

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
Module-level mutable state is exposed through setter functions rather than being encapsulated within a service lifetime.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/report.py:51`: `_RENDER_ENRICHMENT_PORT_STATE: dict[str, RenderEnrichmentPort | None] = {"port": None}` is a module-level mutable singleton accessed via `set_render_enrichment_port()` at line 78. The rendering pipeline reads this global state at lines 127 and 492, coupling the render path to ambient mutable state rather than receiving the port as a parameter.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/bootstrap.py:48`: `_RUNTIME_SERVICES: dict[str, CqRuntimeServices] = {}` is a workspace-keyed cache with a lock at line 47. While the lock ensures thread safety, the pattern means any code with access to `resolve_runtime_services()` can silently trigger side effects (service construction, `atexit` registration at line 79). The lifecycle is hidden behind what appears to be a simple lookup.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_builders.py:37-44`: Nine module-level threshold constants (`_HIGH_CALLER_THRESHOLD`, `_MEDIUM_CALLER_THRESHOLD`, etc.) encode risk policy decisions. These are not configurable and are not grouped into a policy struct, making them invisible to tests that need to validate threshold behavior.

**Suggested improvement:**
Replace `_RENDER_ENRICHMENT_PORT_STATE` with a parameter threaded through `render_markdown()`. The function already accepts `CqResult`; adding a `render_context` parameter containing the port would eliminate the global. For the risk thresholds, group them into a `RiskThresholdPolicyV1` frozen struct that `risk_from_counters()` accepts as an optional parameter, defaulting to the current values.

**Effort:** medium
**Risk if unaddressed:** medium -- The global port state makes `render_markdown()` non-deterministic relative to its explicit inputs, complicating parallel test execution and any future multi-workspace rendering.

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`front_door_builders.py` is the largest file in the module at 1,113 LOC and contains four distinct concerns.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_builders.py:52-154`: Schema definitions (11 frozen structs: `InsightLocationV1`, `InsightTargetV1`, `InsightSliceV1`, `InsightNeighborhoodV1`, `InsightRiskCountersV1`, `InsightRiskV1`, `InsightConfidenceV1`, `InsightDegradationV1`, `InsightBudgetV1`, `InsightArtifactRefsV1`, `FrontDoorInsightV1`).
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_builders.py:157-194`: Build request contracts (`SearchInsightBuildRequestV1`, `CallsInsightBuildRequestV1`, `EntityInsightBuildRequestV1`).
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_builders.py:197-304`: Rendering logic (`render_insight_card()` and its 7 helper functions).
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_builders.py:447-574`: Builder functions (`build_search_insight()`, `build_calls_insight()`, `build_entity_insight()`).
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_builders.py:712-754`: Risk policy logic (`risk_from_counters()`, `_risk_level_from_counters()`).
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_builders.py:757-823`: Degradation derivation from summary (`_degradation_from_summary()`, `_semantic_provider_and_availability()`).

The file has already been partially split: `front_door_contracts.py`, `front_door_schema.py`, `front_door_render.py`, and `front_door_serialization.py` all exist but are pure re-export shims that import everything from `front_door_builders.py`.

**Suggested improvement:**
Complete the decomposition that the re-export shims started. Move schema structs (lines 52-154) into `front_door_contracts.py` as the canonical home. Move build request contracts (lines 157-194) alongside them. Move rendering (lines 197-304) into `front_door_render.py`. Move builder functions into a new `front_door_assembly.py`. Move risk policy into a dedicated `front_door_risk.py`. The existing re-export shims provide backward compatibility during transition.

**Effort:** medium
**Risk if unaddressed:** medium -- The current file changes for any of four independent reasons, increasing merge conflict frequency and cognitive load for contributors.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
Several files serve multiple distinct purposes.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_builders.py`: Changes for schema evolution, risk threshold tuning, rendering format changes, builder logic updates, or degradation derivation changes. This is five reasons to change.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/report.py:466-551`: The `render_markdown()` function orchestrates rendering, enrichment precomputation, section reordering, deduplication, and summary compaction in a single 85-line function. It changes when any of these sub-concerns evolve.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/schema.py:62-174`: `DetailPayload` implements `__getitem__`, `__setitem__`, `__contains__` to emulate `dict` access while being a `msgspec.Struct`. It changes for both schema evolution and dict-compat behavior.

**Suggested improvement:**
For `render_markdown()`, extract the enrichment orchestration (lines 479-508) into a `RenderContext` dataclass that is built once and threaded through. For `DetailPayload`, the dict-like interface is a pragmatic legacy bridge; document that it is frozen once finding IDs are assigned, and consider a deprecation path toward direct `.score` and `.data` access.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The re-export shim pattern creates a layered import graph that is coherent but introduces unnecessary indirection.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_schema.py:1-47`: Pure re-export module that imports from both `front_door_builders` and `front_door_contracts`. It adds no logic.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_render.py:1-15`: Pure re-export module that imports `render_insight_card` from `front_door_builders` and `to_public_front_door_insight_dict` from `front_door_serialization`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_contracts.py:1-31`: Pure re-export of all 11 struct types from `front_door_builders`.

These shims exist because the decomposition was started (the target modules were created) but the source code was never actually moved out of `front_door_builders.py`.

**Suggested improvement:**
Complete the split described in P2 above, which would make these shims genuine module homes rather than pass-throughs. Until then, the shims serve a useful purpose as stable import points for external consumers.

**Effort:** small (follows naturally from P2 work)
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The dependency graph generally flows inward (core contracts have no outward dependencies, rendering depends on contracts). The `ports.py` protocol file correctly defines abstract interfaces for external services.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/render_enrichment_orchestrator.py:15`: `from tools.cq.query.language import QueryLanguage` -- core module depends on query module for a simple type alias. This inverts the expected direction (core should define primitives that query consumes).
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/contracts.py:21`: `from tools.cq.search._shared.search_contracts import SearchSummaryContract` in TYPE_CHECKING block. This is correctly gated but conceptually questionable -- core contracts should not reference search-specific types.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/settings_factory.py:21`: `from tools.cq.search.tree_sitter.core.infrastructure import ParserControlSettingsV1, parser_controls_from_env` -- core settings depend on search infrastructure. This violates the intended direction.

**Suggested improvement:**
Move `QueryLanguage` type alias to `tools/cq/core/types.py` (which currently only contains `LdmdSliceMode`). Move `ParserControlSettingsV1` contract to core or have `SettingsFactory` accept it as a parameter rather than importing from search.

**Effort:** small
**Risk if unaddressed:** low -- The circular potential is mitigated by TYPE_CHECKING guards, but the conceptual inversion adds confusion.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`ports.py` defines five clean protocol interfaces (`SearchServicePort`, `EntityServicePort`, `CallsServicePort`, `CachePort`, `RenderEnrichmentPort`). Service implementations in `services.py` use lazy imports to avoid pulling in heavy dependencies at import time. The composition root in `bootstrap.py` wires adapters explicitly.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/ports.py:1-83`: Well-designed protocol interfaces with clear contracts.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/bootstrap.py:37`: `set_render_enrichment_port(SmartSearchRenderEnrichmentAdapter())` -- adapter wiring happens via a global setter rather than constructor injection, weakening the port/adapter boundary.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/services.py:50-104`: Service classes (`EntityService`, `CallsService`, `SearchService`) use `@staticmethod` methods with lazy imports. This is a pragmatic adapter pattern, though `@staticmethod` makes DI harder in tests.

**Suggested improvement:**
Have `CqRuntimeServices` hold the `RenderEnrichmentPort` directly instead of setting it through a global. The port is already available at construction time in `build_runtime_services()` at `bootstrap.py:37`.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Knowledge

#### P7. DRY (knowledge) -- Alignment: 1/3

**Current state:**
Several knowledge items are duplicated across modules.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_builders.py:46-49` and `/Users/paulheyse/CodeAnatomy/tools/cq/core/render_summary.py:58-61`: Both define `_summary_value(summary, key)` with identical semantics (dispatch between `CqSummary` attribute access and dict `.get()`). This is duplicated knowledge about how to access summary payloads polymorphically.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/schema.py:32-49` and `/Users/paulheyse/CodeAnatomy/tools/cq/core/type_coercion.py:44-62`: Both define `coerce_str()` with slightly different signatures (the schema version returns `str | None`, the type_coercion version raises `TypeError`). This duplicates the "how to safely extract a string" knowledge.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/scoring.py:248-253` defines a local `_coerce_str()` inside `_score_details_from_mapping()`, creating a third copy.
- Bucket ordering logic: `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_builders.py:978-980` (`_max_bucket()`) and `/Users/paulheyse/CodeAnatomy/tools/cq/core/render_summary.py:467-471` (`impact_order`/`conf_order` dicts) both encode the same `none < low < med < high` ordering knowledge.

**Suggested improvement:**
Extract `_summary_value()` into `render_utils.py` or a new `summary_access.py` since it bridges the `CqSummary`/dict boundary used by multiple renderers. Consolidate `coerce_str` variants into `type_coercion.py` with two signatures (strict raising, optional returning). Add a `BUCKET_ORDER` constant to a shared location.

**Effort:** medium
**Risk if unaddressed:** medium -- Drift between the duplicated bucket orderings or summary access patterns would produce silently inconsistent behavior.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The `typed_boundary.py` module provides a clean contract enforcement layer using msgspec strict/lax conversion with a domain-specific `BoundaryDecodeError`. The `contracts_constraints.py` module defines annotated types (`PositiveInt`, `NonNegativeInt`, etc.) that enforce value constraints at decode time.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/typed_boundary.py:14-67`: `convert_strict()` and `convert_lax()` wrap msgspec conversion with clean error taxonomy. This is a well-designed boundary enforcement pattern.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/contracts_constraints.py:12-16`: Annotated constraint types are reusable and clear.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/summary_contract.py:175-179`: `CqSummary._set_known_field()` uses `setattr()` to mutate a non-frozen struct, but silently ignores type mismatches. Setting `summary.matches = "not_an_int"` would succeed at runtime and only fail downstream.

**Suggested improvement:**
Add a runtime type check in `_set_known_field()` that validates the assigned value matches the field's declared type, at least for primitive fields. This prevents garbage-in propagation through the summary pathway.

**Effort:** small
**Risk if unaddressed:** low -- Most paths use the typed summary constructors correctly; the mutation interface is primarily used in merge/update flows.

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Boundary parsing is generally well-implemented.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/schema.py:259-262`: `Finding.__post_init__()` converts legacy `dict` details to `DetailPayload` at construction time, which is a correct parse-at-boundary pattern.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_builders.py:696-709`: `coerce_front_door_insight()` uses `convert_lax()` to parse dict payloads into typed `FrontDoorInsightV1`, returning `None` on failure. This is a clean parse-or-reject pattern.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_builders.py:379-404`: `augment_insight_with_semantic()` accepts `dict[str, object]` for `semantic_payload` and does isinstance checks at every access point (lines 379, 380, 419, 420). This payload should be parsed into a typed struct at the caller boundary.

**Suggested improvement:**
Define a `SemanticAugmentPayloadV1` struct with optional typed fields (`call_graph: SemanticCallGraphV1 | None`, `type_contract: SemanticTypeContractV1 | None`) and parse the raw dict at the call boundary of `augment_insight_with_semantic()` instead of sprinkling isinstance checks throughout the function body.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 1/3

**Current state:**
`CqSummary` is the primary offender. It has 168 fields, nearly all optional with defaults, making it possible to construct summaries that represent impossible combinations.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/summary_contract.py:45-168`: `CqSummary` has 123 fields spanning search metrics, entity metrics, calls metrics, run metrics, semantic telemetry, cache telemetry, and error state. A search-mode summary would have `would_break`, `ambiguous`, `ok` all defaulting to 0, which is semantically nonsensical for search. Similarly, a calls-mode summary can carry `total_imports` and `entity_kind`.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/schema.py:62-139`: `DetailPayload` has a `data: dict[str, object]` field that is mutable (not frozen) despite `DetailPayload` using `omit_defaults=True`. The `__setitem__` at line 130 mutates the struct, which is surprising given the msgspec ecosystem's preference for immutability.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/schema.py:227-262`: `Finding` is not frozen (`msgspec.Struct` without `frozen=True`), enabling mutation of `stable_id`, `execution_id`, and `id_taxonomy` after construction (done at `schema.py:457-458`). This means findings exist in two states: pre-ID-assignment and post-ID-assignment, with no type-level distinction.

**Suggested improvement:**
For `CqSummary`, consider introducing typed summary variants (e.g., `SearchSummaryV1`, `CallsSummaryV1`, `EntitySummaryV1`) that extend a minimal `BaseSummaryV1` with only the fields relevant to each mode. The `RunSummaryV1` in `summary_contracts.py:49` already hints at this pattern. This is a large effort but the most impactful improvement. In the shorter term, document which field groups are valid for which macro modes.

For `Finding` mutation: `assign_result_finding_ids()` could return new `Finding` instances with IDs populated rather than mutating in-place, using `msgspec.structs.replace()`.

**Effort:** large (for CqSummary refactor), small (for Finding immutability)
**Risk if unaddressed:** medium -- The flat CqSummary makes it impossible to statically verify that a summary is well-formed for its macro mode.

---

#### P11. CQS -- Alignment: 1/3

**Current state:**
Several functions mix querying and mutation in a single call.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/schema.py:448-466`: `assign_result_finding_ids()` mutates `finding.stable_id`, `finding.execution_id`, and `finding.id_taxonomy` in-place for all findings in the result. This is a command disguised as a void function -- callers have no signal that the result was modified.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/render_enrichment.py:102-115`: `merge_enrichment_details()` mutates `finding.details[key]` in-place as a side effect during rendering.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/render_enrichment_orchestrator.py:320-354`: `maybe_attach_render_enrichment()` mutates finding details and also writes to the cache dict -- a combined command+query that modifies two objects.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/merge.py:27-52`: `merge_step_results()` mutates the `merged` CqResult by appending to its lists. This is explicit (the parameter name `merged` suggests mutation), but still a CQS violation.

**Suggested improvement:**
For `assign_result_finding_ids()`, return a new `CqResult` with findings replaced. For the render-time enrichment mutations, accept that these are performance-motivated mutations during a rendering pass and document them clearly as "render-phase side effects" -- but ensure the mutation window is bounded (no mutation after the render pass completes).

**Effort:** small
**Risk if unaddressed:** medium -- Finding mutation during rendering means the same `CqResult` cannot be rendered twice with different enrichment configurations without getting contaminated state.

---

### Category: Simplicity

#### P19. KISS -- Alignment: 2/3

**Current state:**
The overall architecture is appropriately complex for its domain. The enrichment fact resolution system (`enrichment_facts.py`) is the most intricate component but is justified by its multi-language, multi-source enrichment requirements.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/enrichment_facts.py:84-416`: The `FACT_CLUSTERS` constant defines 7 clusters with 30+ field specs, each with multiple fallback paths. This is complex but represents genuine domain complexity (multiple enrichment sources for Python and Rust).
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/report.py:466-551`: `render_markdown()` orchestrates enrichment precomputation, summary compaction, section reordering, finding deduplication, and multi-pass rendering in a single function. The cognitive load is high, though each step is necessary.

**Suggested improvement:**
Extract the enrichment precomputation block (lines 479-508) into a `RenderPrecomputeContext` builder, reducing `render_markdown()` to orchestration of named phases.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
The module is generally lean, with abstractions justified by near-term use cases.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/typed_boundary.py:86-115`: `decode_toml_strict()` and `decode_yaml_strict()` exist alongside `decode_json_strict()`. TOML is used for plan files; YAML may be speculative. However, the cost of these thin wrappers is negligible.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/structs.py:22-47`: Five struct base classes (`CqStruct`, `CqSettingsStruct`, `CqOutputStruct`, `CqStrictOutputStruct`, `CqCacheStruct`) provide graduated strictness. This is justified by the different boundary requirements.

**Suggested improvement:**
No changes recommended. The YAML decoder is low-cost insurance, and the struct hierarchy serves documented purposes.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
Several API surfaces behave in ways that conflict with reader expectations.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/summary_contract.py:45-260`: `CqSummary` inherits from `msgspec.Struct` with `omit_defaults=True` but is NOT frozen. It implements `__setitem__`, `__getitem__`, `__contains__`, `__iter__`, `__len__`, `keys()`, `values()`, `items()`, and `update()` -- making it behave like a `dict` while being a struct. A reader encountering `summary["query"] = "foo"` would not expect struct mutation. The `__iter__` override (line 219) has a `# type: ignore[override]` comment, signaling the API is fighting its own type system.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/schema.py:130-138`: `DetailPayload.__setitem__()` mutates a struct that uses `omit_defaults=True`, which conventionally suggests the struct is a data-transfer object, not a mutable container. The score field mutation at line 137 uses `msgspec.structs.replace()` to mutate `self.score`, replacing the entire sub-struct to set one field.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/schema.py:259-262`: `Finding.__post_init__()` exists on a `msgspec.Struct`, which is unusual. msgspec structs do not natively support `__post_init__`; this works only because msgspec calls it if defined, but it is an under-documented feature that may surprise contributors.

**Suggested improvement:**
Add a class-level docstring to `CqSummary` explicitly stating it is a "mutable summary accumulator with dict-like access for backward compatibility." Consider renaming the mutation methods to `set_field()` / `get_field()` to distinguish from standard dict semantics, though this is a larger breaking change. For `Finding.__post_init__`, add a comment referencing the msgspec behavior.

**Effort:** medium
**Risk if unaddressed:** medium -- The hybrid dict/struct API is a frequent source of confusion when contributors from outside the CQ module interact with summary payloads.

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
Versioning discipline is generally good.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_builders.py:154`: `FrontDoorInsightV1` carries `schema_version: str = "cq.insight.v1"`. This is a well-declared contract version.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/schema.py:331`: `RunMeta.schema_version` defaults to `SCHEMA_VERSION` from `tools.cq`.
- All contract structs use the `V1` suffix convention consistently (`InsightSliceV1`, `SemanticContractStateV1`, `DiagnosticsArtifactPayloadV1`, etc.).
- Schema evolution notes at `/Users/paulheyse/CodeAnatomy/tools/cq/core/schema.py:19-22` document the append-only, defaults-required policy.
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/summary_contract.py:45`: `CqSummary` lacks a version suffix and does not carry an explicit schema version field, despite being the most widely serialized contract.

**Suggested improvement:**
Add a `schema_version` field to `CqSummary` (defaulting to `"cq.summary.v1"`) to make summary schema evolution explicit and machine-parseable.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Incomplete front-door decomposition

The `front_door_builders.py` file was clearly intended to be decomposed -- `front_door_contracts.py`, `front_door_schema.py`, `front_door_render.py`, and `front_door_serialization.py` were created as target modules, but only `front_door_serialization.py` actually contains unique logic. The others are pure re-exports. This half-completed split is the root cause of P2 and P3 violations and contributes to P4 (re-export shims reduce apparent cohesion).

**Root cause:** The decomposition was started as a refactoring step but the actual code movement was deferred.
**Affected principles:** P1, P2, P3, P4
**Suggested approach:** Complete the code movement in one pass. The re-export shims already serve as stable import paths, so external consumers won't break.

### Theme 2: Module-level mutable singletons undermining DI

Three separate module-level mutable state holders exist: `_RENDER_ENRICHMENT_PORT_STATE` in `report.py`, `_RUNTIME_SERVICES` in `bootstrap.py`, and `_BACKEND_STATE`/`_SCHEDULER_STATE` in the cache/runtime subdirectories (noted but excluded from this review's scope). These all use the same pattern: a module-level dict/object + lock + getter + setter. The pattern works but creates hidden coupling -- functions appear pure from their signatures but depend on ambient state.

**Root cause:** The module started with function-based APIs before service objects were introduced. The global state was the simplest wiring mechanism.
**Affected principles:** P1, P6, P11, P21
**Suggested approach:** Thread `RenderEnrichmentPort` through `render_markdown()` explicitly. Keep `_RUNTIME_SERVICES` in `bootstrap.py` (workspace-scoped caching is a legitimate singleton concern) but consider making it a proper service locator class rather than a module-level dict.

### Theme 3: CqSummary as a god object

`CqSummary` at 168 fields is the most structurally problematic type in the module. It accumulates metrics from all macro modes, creating a flat namespace where any field can be set regardless of context. Its dict-like mutation interface compounds the problem by making it impossible to distinguish between "field not applicable to this mode" and "field not yet set."

**Root cause:** The summary started as a simple `dict[str, object]` and was migrated to a typed struct for serialization safety, but the flat structure was preserved for backward compatibility.
**Affected principles:** P10, P21, P8
**Suggested approach:** Phase 1 (small): Add docstring documenting field groups by macro mode. Phase 2 (medium): Extract `SearchSummaryV1`, `CallsSummaryV1`, `RunSummaryV1` typed summaries that the mode-specific builders produce. Phase 3 (large): `CqSummary` becomes a discriminated union or uses a `mode`-tagged envelope.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Deduplicate `_summary_value()` from `front_door_builders.py:46` and `render_summary.py:58` into `render_utils.py` | small | Eliminates drift risk in summary access logic |
| 2 | P5 (Dependency direction) | Move `QueryLanguage` type alias from `tools/cq/query/language.py` to `tools/cq/core/types.py` | small | Fixes core-depends-on-query inversion used by `render_enrichment_orchestrator.py` |
| 3 | P22 (Contracts) | Add `schema_version` field to `CqSummary` | small | Makes the most-serialized contract version-discoverable |
| 4 | P1 (Information hiding) | Group risk threshold constants into `RiskThresholdPolicyV1` struct | small | Makes risk policy testable and configurable |
| 5 | P21 (Least astonishment) | Add explicit class docstring to `CqSummary` documenting its mutable accumulator semantics | small | Reduces contributor confusion about dict-like struct mutation |

## Recommended Action Sequence

1. **Deduplicate `_summary_value()`** (P7) -- Establishes a single source of truth for summary access before any further summary refactoring.

2. **Move `QueryLanguage` to `core/types.py`** (P5) -- Fixes the dependency inversion that currently forces `render_enrichment_orchestrator.py` to import from `tools/cq/query/`.

3. **Complete front-door decomposition** (P2, P3, P4) -- Move schema structs to `front_door_contracts.py`, rendering to `front_door_render.py`, builders to `front_door_assembly.py`, risk policy to `front_door_risk.py`. This is the highest-impact structural improvement.

4. **Thread `RenderEnrichmentPort` through `render_markdown()`** (P1, P6) -- Eliminates the `_RENDER_ENRICHMENT_PORT_STATE` global by passing the port through the render call chain.

5. **Extract `RiskThresholdPolicyV1` struct** (P1, P8) -- Groups the nine threshold constants and makes risk policy a first-class testable contract.

6. **Add schema_version to CqSummary** (P22) -- Small change with long-term value for schema evolution tracking.

7. **Document CqSummary field groups** (P10, P21) -- Short-term mitigation while the god-object decomposition is planned.

8. **Return new Finding instances from `assign_result_finding_ids()`** (P11) -- Converts the most visible CQS violation to a pure transformation.
