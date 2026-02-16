# Design Review: tools/cq/core/

**Date:** 2026-02-16
**Scope:** `tools/cq/core/` (entire directory)
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 75 (~22,307 LOC)

## Executive Summary

The `tools/cq/core/` module demonstrates strong design across boundaries, contracts, and caching infrastructure. The hexagonal ports layer (`ports.py`), the `CqCacheBackend` protocol with `NoopCacheBackend` fallback, and the well-structured `msgspec.Struct` hierarchy (`CqStruct`, `CqSettingsStruct`, `CqOutputStruct`, `CqCacheStruct`) provide a solid foundation for the rest of the CQ system. The primary areas for improvement are: (1) the `front_door_builders.py` mega-module (1,172 LOC) which mixes schema, builders, rendering, and serialization logic; (2) duplicated cache policy resolution logic between `execution_policy.py` and `cache/policy.py`; (3) global mutable singletons for runtime state (`_BACKEND_STATE`, `_SCHEDULER_STATE`, `_RENDER_ENRICHMENT_PORT_STATE`); and (4) the `report.py` module (927 LOC) which couples rendering with IO, enrichment computation, and multiprocessing orchestration.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `DiskcacheBackend.cache` is a public attribute; `_BACKEND_STATE` module-level mutable |
| 2 | Separation of concerns | 1 | medium | med | `report.py` mixes rendering, IO, enrichment computation, and multiprocessing |
| 3 | SRP (one reason to change) | 1 | medium | med | `front_door_builders.py` (1,172 LOC): schema + build + render + serialize |
| 4 | High cohesion, low coupling | 2 | small | low | Cache sub-package is well-cohesive; rendering modules are scattered |
| 5 | Dependency direction | 2 | small | low | `contracts.py:21` imports from `search._shared.search_contracts` at module level |
| 6 | Ports & Adapters | 3 | - | - | `ports.py` defines clean Protocol-based ports; adapter split is explicit |
| 7 | DRY (knowledge, not lines) | 1 | medium | med | Cache policy defaults duplicated between `execution_policy.py` and `cache/policy.py` |
| 8 | Design by contract | 2 | small | low | `contracts_constraints.py` provides typed constraints; `Anchor.line` uses `Meta(ge=1)` |
| 9 | Parse, don't validate | 2 | small | low | `typed_boundary.py` provides boundary decode; `Finding.__post_init__` normalizes legacy |
| 10 | Make illegal states unrepresentable | 2 | small | low | `FrontDoorInsightV1` has well-typed enums; `DetailPayload` allows too-open `data: dict[str, object]` |
| 11 | CQS | 2 | small | low | `assign_result_finding_ids` mutates in-place and returns None (command); `report.py:_maybe_attach_render_enrichment` mutates finding as side effect |
| 12 | Dependency inversion + explicit composition | 2 | small | low | `bootstrap.py` is explicit composition root; singleton accessors reduce DI clarity |
| 13 | Prefer composition over inheritance | 3 | - | - | `CqStruct` hierarchy uses struct config, not method inheritance |
| 14 | Law of Demeter | 2 | small | low | `coordination.py:25` uses `getattr(backend, "cache", None)` to reach through adapter |
| 15 | Tell, don't ask | 2 | small | low | `_degradation_from_summary` extensively interrogates `dict[str, object]` summary payload |
| 16 | Functional core, imperative shell | 2 | small | low | Scoring, ID generation are pure; rendering mixes IO reads with transforms |
| 17 | Idempotency | 3 | - | - | Cache key generation is deterministic; `add()` is inherently idempotent |
| 18 | Determinism / reproducibility | 3 | - | - | `JSON_ENCODER` uses `order="deterministic"`; `canonicalize_payload` sorts keys |
| 19 | KISS | 2 | small | low | `fragment_engine.py` function-bundle dataclasses are simple; telemetry counter accumulation is manual |
| 20 | YAGNI | 2 | small | low | `CqCacheBackend` protocol has 14 methods; `NoopCacheBackend` duplicates all 14 |
| 21 | Least astonishment | 2 | small | low | `contract_codec.py` exposes both `encode_json`/`dumps_json_value` (aliases) |
| 22 | Declare and version public contracts | 3 | - | - | V1 suffixes on all contracts; `schema_version` field on `FrontDoorInsightV1` |
| 23 | Design for testability | 2 | small | low | Singleton state requires `set_*` functions for test injection; pure scoring is testable |
| 24 | Observability | 2 | small | low | `telemetry.py` provides structured counters; no structured logging in cache/render paths |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The module generally hides internal decisions well. `CqCacheBackend` is a Protocol that hides the diskcache implementation. However, the `DiskcacheBackend` class at `tools/cq/core/cache/diskcache_backend.py:93` exposes `self.cache` as a public attribute, and callers in `coordination.py:25` use `getattr(backend, "cache", None)` to reach through to the underlying `FanoutCache`.

**Findings:**
- `tools/cq/core/cache/diskcache_backend.py:93`: `self.cache = cache` is a public attribute exposing the raw `FanoutCache` handle. Any consumer can bypass the fail-open wrapper.
- `tools/cq/core/cache/coordination.py:25`: `_fanout_cache()` uses `getattr(backend, "cache", None)` to extract the internal cache handle, creating an implicit coupling to the `DiskcacheBackend` implementation.
- `tools/cq/core/cache/diskcache_backend.py:411`: `_BACKEND_STATE` is a module-level mutable singleton with `dict[str, CqCacheBackend]` that is effectively global state.

**Suggested improvement:**
Rename `DiskcacheBackend.cache` to `DiskcacheBackend._cache` (private) and add a dedicated `raw_cache()` method on the backend protocol (or a separate `CoordinationCapable` protocol) for coordination primitives that need direct FanoutCache access. This leverages `diskcache` coordination recipes (`Lock`, `BoundedSemaphore`) through a narrow, intentional surface.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`report.py` (927 LOC) is the primary violation. It mixes markdown rendering logic, file IO (`_read_line_text` at line 305), multiprocessing orchestration (`_populate_render_enrichment_cache` at line 466), enrichment computation dispatch, deduplication, and section reordering into a single module.

**Findings:**
- `tools/cq/core/report.py:305-317`: `_read_line_text()` performs filesystem IO within a rendering module.
- `tools/cq/core/report.py:466-494`: `_populate_render_enrichment_cache()` orchestrates `ProcessPoolExecutor` workers inside a rendering module. This couples render-time to multiprocessing availability and error handling.
- `tools/cq/core/report.py:99-101`: `_RENDER_ENRICHMENT_PORT_STATE` is global mutable state for render-time enrichment injection, mixing configuration concern with rendering.
- `tools/cq/core/report.py:919`: Bare `from tools.cq.core.render_summary import ARTIFACT_ONLY_KEYS` at module level outside imports block.

**Suggested improvement:**
Extract three concerns from `report.py`: (1) a `render_enrichment_orchestrator.py` module for the multiprocessing enrichment pipeline (`_build_render_enrichment_tasks`, `_populate_render_enrichment_cache`, `_precompute_render_enrichment_cache`); (2) move `_read_line_text` to `render_utils.py` or a dedicated IO helper; (3) move the `_RENDER_ENRICHMENT_PORT_STATE` into `bootstrap.py` where other runtime state lives.

**Effort:** medium
**Risk if unaddressed:** medium -- changes to multiprocessing, rendering, or enrichment all force changes to this single 927-LOC file.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`front_door_builders.py` (1,172 LOC) has at least four distinct reasons to change: (1) schema contract definitions (15 struct classes), (2) insight build logic (`build_search_insight`, `build_calls_insight`, `build_entity_insight`), (3) rendering logic (`render_insight_card`, `_render_target_line`, etc.), and (4) serialization (`to_public_front_door_insight_dict`, `_serialize_target`, etc.).

**Findings:**
- `tools/cq/core/front_door_builders.py:44-147`: 15 schema contracts defined inline with build/render functions.
- `tools/cq/core/front_door_builders.py:189-205`: `render_insight_card()` is rendering logic living alongside schema definitions and builders.
- `tools/cq/core/front_door_builders.py:1008-1087`: Serialization helpers (`to_public_front_door_insight_dict`, `_serialize_target`, `_serialize_slice`, `_serialize_risk_counters`) could use `msgspec.to_builtins` directly but instead manually construct dicts.
- `tools/cq/core/front_door_schema.py` and `tools/cq/core/front_door_render.py` exist as thin re-export facades but do not decompose the actual logic.

**Suggested improvement:**
Split `front_door_builders.py` into three modules: (1) `front_door_contracts.py` for the 15 schema structs and literal type aliases; (2) `front_door_builders.py` (trimmed) for the three `build_*_insight` functions and `risk_from_counters`; (3) `front_door_render.py` (replace facade) for `render_insight_card` and `to_public_front_door_insight_dict`. The serialization in `_serialize_*` can be replaced with `msgspec.to_builtins(insight, order="deterministic")` which handles frozen struct serialization natively.

**Effort:** medium
**Risk if unaddressed:** medium -- the 1,172 LOC file is the most-imported module in core and changes for any of four reasons.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The `cache/` sub-package is well-cohesive: `interface.py` defines the protocol, `diskcache_backend.py` implements it, `key_builder.py` handles key construction, `telemetry.py` tracks metrics. However, the rendering sub-domain is scattered across 7 files (`report.py`, `render_enrichment.py`, `render_diagnostics.py`, `render_overview.py`, `render_summary.py`, `render_utils.py`, `renderers/`) without a clear package boundary.

**Findings:**
- `tools/cq/core/report.py` imports from 5 other `render_*` siblings, creating a web of intra-module dependencies.
- `tools/cq/core/cache/contracts.py:12-22` imports from `tools.cq.astgrep.sgpy_scanner` and `tools.cq.search.tree_sitter.contracts.core_models`, pulling external dependencies into core cache contracts.

**Suggested improvement:**
Move `contracts.py` (the heavy-dependency cache contracts) to a `cache/search_contracts.py` or keep it in the search domain. The base contracts in `base_contracts.py` already exist for dependency-free cache contracts. This preserves the clean dependency direction from core outward.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Core generally depends inward. However, there are two outward dependency violations.

**Findings:**
- `tools/cq/core/contracts.py:21`: `from tools.cq.search._shared.search_contracts import SearchSummaryContract` appears in `TYPE_CHECKING` block but `summary_contract_to_mapping` at line 60 performs a runtime import of `tools.cq.search._shared.search_contracts.summary_contract_to_dict`. Core should not depend on search internals.
- `tools/cq/core/cache/contracts.py:12`: `from tools.cq.astgrep.sgpy_scanner import RecordType` -- core cache depends on astgrep module.
- `tools/cq/core/cache/contracts.py:22`: `from tools.cq.search.tree_sitter.contracts.core_models import TreeSitterArtifactBundleV1` -- core cache depends on search tree-sitter module.
- `tools/cq/core/settings_factory.py:18-21`: `from tools.cq.search.tree_sitter.core.infrastructure import ParserControlSettingsV1, parser_controls_from_env` -- core depends on search internals.

**Suggested improvement:**
For `cache/contracts.py`, the search-dependent contracts (`SgRecordCacheV1`, `SearchPartitionCacheV1`, etc.) should live in `tools/cq/search/cache_contracts.py` or a similar search-owned location, and the re-exports in `cache/contracts.py:__all__` should be removed. For `settings_factory.py`, move the `parser_controls` method to a search-domain factory.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 3/3

**Current state:**
`ports.py` defines five clean Protocol-based ports (`SearchServicePort`, `EntityServicePort`, `CallsServicePort`, `CachePort`, `RenderEnrichmentPort`). The `CqCacheBackend` Protocol in `interface.py` is the cache port. Adapters (`DiskcacheBackend`, `NoopCacheBackend`) implement the protocol. `bootstrap.py` serves as the explicit composition root. This is well-aligned with hexagonal architecture.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
Cache policy defaults are semantically duplicated across two independent modules that encode the same truth about default values.

**Findings:**
- `tools/cq/core/runtime/execution_policy.py:21-28`: `_DEFAULT_CACHE_TTL_SECONDS = 900`, `_DEFAULT_CACHE_SIZE_LIMIT_BYTES = 2_147_483_648`, `_DEFAULT_CACHE_CULL_LIMIT = 16`, `_DEFAULT_CACHE_EVICTION_POLICY = "least-recently-stored"` -- these exact same defaults appear in `CacheRuntimePolicy` defaults.
- `tools/cq/core/cache/policy.py:24-44`: `CqCachePolicyV1` repeats the identical defaults: `ttl_seconds: PositiveInt = 900`, `size_limit_bytes: PositiveInt = 2_147_483_648`, `cull_limit: NonNegativeInt = 16`, `eviction_policy: str = "least-recently-stored"`.
- `tools/cq/core/cache/policy.py:125-200`: `_resolve_cache_scalar_settings()` re-reads the same `CQ_CACHE_*` env vars that `execution_policy.py:201-234` also reads, with slightly different fallback chains -- the env-override logic is semantically duplicated.
- `tools/cq/core/cache/policy.py:63-122`: Three functions (`_resolve_namespace_ttl_from_env`, `_resolve_namespace_enabled_from_env`, `_resolve_namespace_ephemeral_from_env`) structurally duplicate the same parse-namespace-overrides pattern from `execution_policy.py:94-135`.

**Suggested improvement:**
Make `CacheRuntimePolicy` the single source of truth for cache defaults. Have `CqCachePolicyV1` derive its defaults from `CacheRuntimePolicy` or share a `_CACHE_DEFAULTS` constants object. The namespace env override resolution should be factored into one function parameterized by pattern/field, rather than three nearly identical functions in each module.

**Effort:** medium
**Risk if unaddressed:** medium -- a default change in one location but not the other creates silent behavioral drift.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Good use of `msgspec.Meta` constraints: `PositiveInt = Annotated[int, msgspec.Meta(ge=1)]`, `NonNegativeInt = Annotated[int, msgspec.Meta(ge=0)]`, `BoundedRatio = Annotated[float, msgspec.Meta(ge=0.0, le=1.0)]` in `contracts_constraints.py`. `Anchor.line` uses `Meta(ge=1)`. `enforce_mapping_constraints` validates output contracts. The `typed_boundary.py` module provides `BoundaryDecodeError` with strict and lax decode paths.

**Findings:**
- `tools/cq/core/schema.py:129-138`: `DetailPayload.__setitem__` allows mutation of a struct that is not declared `frozen=True`. The struct has mutable `data: dict[str, object]` and the `__setitem__` implementation mutates `self.score` via `msgspec.structs.replace`, but also directly assigns `self.kind`. This mixed mutability model is surprising.
- `tools/cq/core/schema.py:258-261`: `Finding.__post_init__` coerces `details: dict` to `DetailPayload` at runtime, which is a "parse, don't validate" pattern, but the type annotation declares `details: DetailPayload` while accepting `dict` -- the contract is violated at construction time.

**Suggested improvement:**
Use `msgspec` `forbid_unknown_fields=True` on the strict output contracts where applicable. For `DetailPayload`, consider making it `frozen=True` with an explicit `with_update()` method using `msgspec.structs.replace` instead of `__setitem__`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`typed_boundary.py` provides `convert_strict`, `convert_lax`, `decode_json_strict`, `decode_toml_strict`, `decode_yaml_strict` -- a comprehensive boundary parse layer using `msgspec.convert` and `msgspec.json.decode`. `Finding.__post_init__` normalizes legacy `dict` details into `DetailPayload`.

**Findings:**
- `tools/cq/core/front_door_builders.py:769-814`: `_degradation_from_summary(summary: dict[str, object])` manually interrogates a `dict[str, object]` with 20+ `.get()` calls and type checks. This is "validate" not "parse" -- the summary payload should be parsed into a typed struct at the boundary.
- `tools/cq/core/front_door_builders.py:912-920`: `_node_refs_from_semantic_entries` manually walks `list[dict]` with defensive type checks instead of parsing into typed structures.

**Suggested improvement:**
Define a `SummaryPayloadV1` struct (or use `msgspec.convert` with `from_attributes=True`) to parse the `dict[str, object]` summary into a typed representation at the entry point of `_degradation_from_summary`, eliminating the scattered `.get()` chains.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Good use of `Literal` types: `InsightSource = Literal["search", "calls", "entity"]`, `RiskLevel = Literal["low", "med", "high"]`, `SemanticStatus = Literal["unavailable", "skipped", "failed", "partial", "ok"]`. `Anchor.line` is constrained to `ge=1`.

**Findings:**
- `tools/cq/core/schema.py:249`: `Finding.severity` is `Literal["info", "warning", "error"]` -- good.
- `tools/cq/core/schema.py:61-66`: `DetailPayload` has `data: dict[str, object]` which is maximally open -- any key/value is allowed. This makes it impossible to statically know what data fields exist.
- `tools/cq/core/cache/policy.py:38`: `eviction_policy: str = "least-recently-stored"` is an untyped string where only a fixed set of values is valid (`least-recently-stored`, `least-recently-used`, `least-frequently-used`, `none`). This should be a `Literal` type.

**Suggested improvement:**
Define `EvictionPolicy = Literal["least-recently-stored", "least-recently-used", "least-frequently-used", "none"]` and use it for the `eviction_policy` field. This leverages `msgspec` validation to reject invalid values at construction time rather than at `FanoutCache` initialization.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. `scoring.py` functions are pure queries. Cache backend methods generally follow CQS (queries return values, commands return acknowledgement booleans).

**Findings:**
- `tools/cq/core/schema.py:447-465`: `assign_result_finding_ids(result)` mutates findings in-place (command) and returns `None` -- correct CQS.
- `tools/cq/core/report.py:560-589`: `_maybe_attach_render_enrichment()` mutates `finding.details` as a side effect while also computing enrichment -- it mixes query (compute enrichment) with command (mutate finding).
- `tools/cq/core/render_enrichment.py:102-115`: `merge_enrichment_details(finding, payload)` mutates `finding.details` in-place. It is a command but could surprise callers expecting rendering to be read-only.

**Suggested improvement:**
Return enriched findings as new objects instead of mutating in place, or clearly document the mutation contract. `msgspec.structs.replace` could be used if `Finding` were frozen.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
`bootstrap.py` is an explicit composition root that wires `SearchService`, `EntityService`, `CallsService`, cache backend, and runtime policy into `CqRuntimeServices`. The `SettingsFactory` provides static factory methods. However, three module-level singletons (`_BACKEND_STATE`, `_SCHEDULER_STATE`, `_RENDER_ENRICHMENT_PORT_STATE`) and their `get_*/set_*` accessors create an implicit service locator pattern.

**Findings:**
- `tools/cq/core/cache/diskcache_backend.py:404-411`: `_BACKEND_STATE` is a mutable global singleton.
- `tools/cq/core/runtime/worker_scheduler.py:160-170`: `_SCHEDULER_STATE` is a mutable global singleton.
- `tools/cq/core/report.py:51`: `_RENDER_ENRICHMENT_PORT_STATE` is a mutable global dict.
- `tools/cq/core/bootstrap.py:47-48`: `_RUNTIME_SERVICES_LOCK` and `_RUNTIME_SERVICES` add a fourth layer of global state.

**Suggested improvement:**
Consolidate the singleton management into `bootstrap.py` as the single holder of runtime state. Have `CqRuntimeServices` own the scheduler and cache backend directly, and thread them through the call chain instead of accessing globals. This makes the dependency graph explicit and testable without `set_*` escape hatches.

**Effort:** medium (requires threading services through call chains)
**Risk if unaddressed:** low

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
The `CqStruct` hierarchy (`CqStruct`, `CqSettingsStruct`, `CqOutputStruct`, `CqStrictOutputStruct`, `CqCacheStruct`) uses `msgspec.Struct` configuration keywords (`kw_only`, `frozen`, `omit_defaults`, `forbid_unknown_fields`) rather than method inheritance. No deep class hierarchies exist. Services compose rather than inherit.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code talks to direct collaborators. The main violation is the coordination module reaching through the backend to access the raw cache.

**Findings:**
- `tools/cq/core/cache/coordination.py:23-33`: `_fanout_cache(backend)` uses `getattr(backend, "cache", None)` to reach through the `CqCacheBackend` protocol to the `FanoutCache` implementation. This couples coordination primitives to the internal structure of `DiskcacheBackend`.
- `tools/cq/core/report.py:860-861`: `enrich_cache.get((task.file, task.line, task.col, task.language))` -- the tuple key reaching into task internals is acceptable but the 4-element tuple key pattern is fragile.

**Suggested improvement:**
Add a `coordination_handle()` method to the `CqCacheBackend` protocol that returns an opaque coordination object, or create a separate `CoordinationPort` protocol that `DiskcacheBackend` implements. The `diskcache` library's `Lock`, `RLock`, `BoundedSemaphore` recipes all accept a `Cache` instance -- this could be provided via the port contract.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`scoring.py` follows tell-don't-ask: callers pass `ImpactSignals` and receive a score. `build_search_insight`, `build_calls_insight` take request objects and return complete insights.

**Findings:**
- `tools/cq/core/front_door_builders.py:769-814`: `_degradation_from_summary()` extensively asks a `dict[str, object]` for 10+ keys, type-checks results, and reconstructs domain objects. The summary dict is an anemic data carrier with all logic external.
- `tools/cq/core/report.py:162-174`: `_format_finding_prefix()` asks `finding.details` for `"impact_bucket"` and `"confidence_bucket"` to decide formatting. The finding should know its own display prefix.

**Suggested improvement:**
For `_degradation_from_summary`, define a typed input struct that carries the necessary fields and move the derivation logic into a method on that struct or a pure function that takes the struct. This eliminates the dictionary interrogation pattern.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
`scoring.py`, `id.py`, `definition_parser.py`, `semantic_contracts.py`, `front_door_builders.py` (builders/risk) are largely pure deterministic transforms. The imperative shell is in `report.py` (IO, multiprocessing), `diskcache_backend.py` (disk IO), and `bootstrap.py` (state initialization).

**Findings:**
- `tools/cq/core/report.py:305-317`: `_read_line_text()` performs filesystem IO inside the rendering pipeline, mixing pure rendering with IO effects.
- `tools/cq/core/report.py:466-494`: Multiprocessing orchestration (`ProcessPoolExecutor`) inside the rendering call chain.
- `tools/cq/core/schema.py:393-395`: `mk_runmeta()` calls `time.time()` inside what looks like a construction function, making it impure.

**Suggested improvement:**
Extract IO (`_read_line_text`) and multiprocessing (`_populate_render_enrichment_cache`) from `report.py` into a separate imperative orchestration layer. Pass pre-read line text and pre-computed enrichment payloads into the pure rendering functions.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Cache key generation via `build_cache_key` in `key_builder.py` is deterministic and idempotent. `DiskcacheBackend.add()` is inherently idempotent (write-if-absent). `canonicalize_payload()` produces the same output for the same input. `stable_digest24()` is a pure function.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
`contract_codec.py:13`: `JSON_ENCODER = msgspec.json.Encoder(order="deterministic")`. `id.py:20-21`: `canonicalize_payload()` sorts dict keys and set elements. `key_builder.py:97`: `hashlib.sha256(msgspec.json.encode(payload)).hexdigest()` ensures deterministic cache keys. `to_contract_builtins` uses `order="deterministic", str_keys=True`.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Many modules are admirably simple: `id.py` (61 LOC), `structs.py` (48 LOC), `typed_boundary.py` (126 LOC), `definition_parser.py` (106 LOC). The complexity concentrates in `front_door_builders.py` and `report.py`.

**Findings:**
- `tools/cq/core/cache/telemetry.py:43-54`: Manual counter accumulation with `_bucket()` and `_incr()` using `dict[str, dict[str, int]]`. This is a reimplementation of what `diskcache` statistics provide natively via `cache.stats(enable=True)`.
- `tools/cq/core/front_door_builders.py:731-745`: `_risk_level_from_counters` has a multi-branch conditional with 7 conditions. The threshold logic is clear but could be a table-driven approach.

**Suggested improvement:**
For telemetry, consider whether `diskcache.FanoutCache(statistics=True)` with `cache.stats()` can replace the manual counter tracking for basic hit/miss/timeout metrics, reducing the custom telemetry code. Keep the namespace-level granularity only for what `diskcache` stats cannot provide.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most code serves clear current use cases. The `CqCacheBackend` protocol has 14 methods, and `NoopCacheBackend` must implement all 14 as no-ops.

**Findings:**
- `tools/cq/core/cache/interface.py:9-167`: `CqCacheBackend` Protocol has 14 methods. The `NoopCacheBackend` at lines 175-338 duplicates all 14 with trivial no-op implementations. Some methods like `cull()`, `expire()`, `check()` are administrative and could be on a separate `CacheAdminPort` protocol.
- `tools/cq/core/contract_codec.py:133-157`: `dumps_json_value`, `loads_json_value`, `loads_json_result` are thin aliases for `encode_json`, `decode_json`, `decode_json_result`. The dual naming adds surface area without distinct functionality.

**Suggested improvement:**
Split `CqCacheBackend` into a core protocol (`CqCachePort`: get/set/add/delete/transact) and an admin protocol (`CqCacheAdminPort`: stats/volume/cull/expire/check/close). Most consumers only need the core protocol. Remove the alias functions in `contract_codec.py` and standardize on one naming convention.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
APIs are generally predictable. The fail-open cache behavior is well-documented and consistent.

**Findings:**
- `tools/cq/core/contract_codec.py:133-157`: Having both `encode_json` and `dumps_json_value` (same function) is confusing. Similarly `decode_json` vs `loads_json_value`.
- `tools/cq/core/schema.py:129-138`: `DetailPayload.__setitem__` on a `msgspec.Struct` is surprising -- callers expect structs to be immutable or at least not support dict-like mutation syntax.
- `tools/cq/core/front_door_schema.py` and `tools/cq/core/front_door_render.py` are re-export facades that import everything from `front_door_builders.py`. A reader might expect them to contain distinct implementations.

**Suggested improvement:**
Remove the alias functions in `contract_codec.py`. For `DetailPayload`, if mutability is required, document it clearly or switch to a dedicated mutable container pattern.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 3/3

**Current state:**
All contract structs use V1 suffixes (`FrontDoorInsightV1`, `SemanticContractStateV1`, `CqCachePolicyV1`, etc.). `FrontDoorInsightV1` includes `schema_version: str = "cq.insight.v1"`. `CqResult.run.schema_version` tracks the schema version. `__all__` exports are declared in every module.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Pure functions in `scoring.py`, `id.py`, `definition_parser.py`, `semantic_contracts.py` are trivially testable. `bootstrap.py` provides `set_worker_scheduler(None)` and `set_cq_cache_backend()` for test injection. The `NoopCacheBackend` serves as a test double.

**Findings:**
- `tools/cq/core/report.py:466-494`: Testing `_populate_render_enrichment_cache` requires either a real `ProcessPoolExecutor` or careful mocking of `get_worker_scheduler()`.
- `tools/cq/core/cache/policy.py:63-200`: `default_cache_policy()` reads `os.environ` directly in 15+ places, making it hard to test without `monkeypatch`.
- `tools/cq/core/cache/diskcache_backend.py:469-483`: `get_cq_cache_backend()` creates a real `FanoutCache` with filesystem side effects. Tests must use `set_cq_cache_backend()` to inject mocks.

**Suggested improvement:**
For `default_cache_policy()`, accept an `env: Mapping[str, str]` parameter (defaulting to `os.environ`) so tests can pass a dict without monkeypatching. This follows the pattern already established in `env_namespace.py` where `parse_namespace_int_overrides` takes an `env` parameter.

**Effort:** small
**Risk if unaddressed:** low

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
`telemetry.py` provides structured namespace-level counters (`CacheNamespaceTelemetry`) with `snapshot_cache_telemetry()` for periodic observation. `render_diagnostics.py` adds render enrichment metrics to the summary. Cache timeouts and aborts are recorded.

**Findings:**
- `tools/cq/core/cache/diskcache_backend.py:113-118`: `record_timeout` and `record_abort` are recorded but there is no structured logging -- only counter increments. A `logger.warning(...)` with namespace and operation context would aid debugging.
- `tools/cq/core/report.py`: No logging in the render pipeline. If enrichment computation fails or times out, it is silently handled.
- Cache telemetry is process-local (`_TELEMETRY` dict) with no export to structured logging or OpenTelemetry.

**Suggested improvement:**
Add structured `logger.warning()` calls for cache timeouts and aborts with namespace, operation, and key context. The existing counter-based telemetry is good for aggregation but provides no per-event traceability.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Mega-module accumulation
`front_door_builders.py` (1,172 LOC) and `report.py` (927 LOC) have accumulated responsibilities over time. Both modules have more than three distinct reasons to change. The two re-export facades (`front_door_schema.py`, `front_door_render.py`) indicate awareness of the problem but do not address the root cause. Splitting these along responsibility lines (contracts/build/render/serialize for front-door; render/IO/enrichment-orchestration for report) would improve maintainability and make the modules easier to test in isolation.

**Affected principles:** P2, P3, P16, P23

### Theme 2: Duplicated cache policy knowledge
Cache default values and environment override resolution logic exists in both `runtime/execution_policy.py` and `cache/policy.py`. The two modules read overlapping sets of `CQ_CACHE_*` and `CQ_RUNTIME_CACHE_*` environment variables with slightly different fallback chains. This creates a risk of behavioral drift if one module is updated but not the other.

**Affected principles:** P7

### Theme 3: Global mutable singletons
Four separate module-level singletons (`_BACKEND_STATE`, `_SCHEDULER_STATE`, `_RENDER_ENRICHMENT_PORT_STATE`, `_RUNTIME_SERVICES`) require `set_*` escape hatches for testing and make the dependency graph implicit. The composition root in `bootstrap.py` partially addresses this but callers can bypass it by calling `get_cq_cache_backend()` or `get_worker_scheduler()` directly.

**Affected principles:** P1, P12, P23

### Theme 4: Core-to-search dependency leakage
`cache/contracts.py` and `settings_factory.py` import from `tools.cq.search` and `tools.cq.astgrep`, violating the expectation that core is a dependency-free foundation. The `base_contracts.py` module was created to address this for lightweight contracts but the heavier contracts still pull in search dependencies.

**Affected principles:** P4, P5

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 | Consolidate cache policy defaults into single source of truth | small | Eliminates behavioral drift risk between two modules |
| 2 | P21 | Remove `dumps_json_value`/`loads_json_value`/`loads_json_result` aliases from `contract_codec.py` | small | Reduces API surface confusion; 3 fewer functions to maintain |
| 3 | P5 | Move search-dependent cache contracts (`SgRecordCacheV1`, etc.) out of `core/cache/contracts.py` to search domain | small | Restores clean dependency direction for core |
| 4 | P10 | Define `EvictionPolicy` Literal type for cache policy `eviction_policy` field | small | Prevents invalid eviction policy values at construction time |
| 5 | P1 | Rename `DiskcacheBackend.cache` to `DiskcacheBackend._cache` | small | Prevents accidental bypass of fail-open wrapper |

## Recommended Action Sequence

1. **Consolidate cache defaults (P7).** Define shared cache default constants in a single location (e.g., `cache/defaults.py`) and have both `CacheRuntimePolicy` and `CqCachePolicyV1` derive their defaults from it. This is the highest-value change because it eliminates a semantic duplication that can silently drift.

2. **Move search-dependent contracts out of core (P5).** Relocate `SgRecordCacheV1`, `SearchPartitionCacheV1`, `SearchCandidatesCacheV1`, `SearchEnrichmentAnchorCacheV1`, `QueryEntityScanCacheV1`, `PatternFragmentCacheV1`, and `TreeSitterArtifactBundleV1` re-export from `cache/contracts.py` to their owning domains. Keep `base_contracts.py` as the core-safe cache contracts module.

3. **Clean up `contract_codec.py` aliases (P21).** Remove `dumps_json_value`, `loads_json_value`, `loads_json_result` and update the 5-10 call sites to use the canonical names.

4. **Split `front_door_builders.py` into three modules (P3).** Extract schema contracts, rendering, and serialization into separate files. Replace the manual `_serialize_*` functions with `msgspec.to_builtins(insight, order="deterministic")`.

5. **Extract enrichment orchestration from `report.py` (P2).** Move `_build_render_enrichment_tasks`, `_populate_render_enrichment_cache`, `_precompute_render_enrichment_cache`, and `_read_line_text` into a dedicated `render_enrichment_orchestrator.py`.

6. **Add `EvictionPolicy` Literal type (P10).** Small type-safety improvement across `CqCachePolicyV1` and `CacheRuntimePolicy`.

7. **Make `DiskcacheBackend.cache` private (P1).** Rename to `_cache` and add a `coordination_handle()` method or separate `CoordinationPort` for `coordination.py`.

8. **Accept `env` parameter in `default_cache_policy()` (P23).** Follow the pattern from `env_namespace.py` to improve testability without monkeypatching.
