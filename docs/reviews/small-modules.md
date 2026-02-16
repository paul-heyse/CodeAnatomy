# Design Review: Small Utility/Support Modules in `src/`

**Date:** 2026-02-16
**Scope:** `src/graph/`, `src/extraction/`, `src/utils/`, `src/cache/`, `src/runtime_models/`, `src/arrow_utils/`, `src/planning_engine/`, `src/validation/`, `src/core/`, `src/test_support/`
**Focus:** All principles (1-24), with emphasis on DRY, SRP, dependency direction, and module justification
**Depth:** Deep (all files examined exhaustively)
**Files reviewed:** 51

## Executive Summary

The small utility/support modules are generally well-designed, with clean separation of concerns in `src/utils/`, `src/validation/`, `src/core/`, and `src/planning_engine/`. The strongest alignment is in the shared utility modules (`utils/`, `core/`) which provide focused, single-purpose helpers with correct dependency direction. The most significant issues are: (1) duplicated boolean-parsing knowledge between `src/utils/env_utils.py` and `src/extraction/runtime_profile.py`, (2) duplicated constrained-type definitions between `src/runtime_models/types.py` (pydantic) and `src/core_types.py` (msgspec), (3) the `src/extraction/` module is an orchestration layer that has grown large and mixes concerns, and (4) `src/extraction/materialize_pipeline.py` is a 595-line file with mixed responsibilities spanning cache decisions, view execution, and output writing.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `_CACHE_POOL` module-global in `cache/diskcache_factory.py` is accessible |
| 2 | Separation of concerns | 1 | medium | medium | `extraction/materialize_pipeline.py` mixes cache decisions, execution, and writing |
| 3 | SRP | 1 | medium | medium | `extraction/runtime_profile.py` handles profiles, env patching, Rust interop, and hashing |
| 4 | High cohesion, low coupling | 2 | small | low | `arrow_utils/` has low cohesion -- scattered helpers with thin wrappers |
| 5 | Dependency direction | 2 | small | medium | `utils/validation.py` depends on `datafusion_engine.arrow.coercion` |
| 6 | Ports & Adapters | 2 | small | low | `test_support/datafusion_ext_stub.py` is a proper adapter |
| 7 | DRY | 1 | small | medium | Boolean parsing constants duplicated; constrained types duplicated |
| 8 | Design by contract | 2 | small | low | Contracts well-defined in `planning_engine/spec_contracts.py` |
| 9 | Parse, don't validate | 2 | small | low | `extraction/options.py` parses messy inputs into typed structs |
| 10 | Illegal states | 2 | small | low | `PlanProduct` allows both `stream` and `table` to be None |
| 11 | CQS | 2 | small | low | `EngineEventRecorder.record_diskcache_stats()` is a command, as expected |
| 12 | DI + explicit composition | 2 | small | low | `build_engine_session` and `build_engine_runtime` use DI properly |
| 13 | Composition over inheritance | 3 | - | - | No deep inheritance anywhere; composition used throughout |
| 14 | Law of Demeter | 1 | medium | medium | Chain violations in `extraction/` modules accessing deep profile internals |
| 15 | Tell, don't ask | 2 | small | low | `PlanProduct.value()` is a good example; some raw data exposure remains |
| 16 | Functional core, imperative shell | 2 | small | low | `utils/` modules are nearly pure; `extraction/` mixes IO throughout |
| 17 | Idempotency | 2 | small | low | `cache_for_kind` is idempotent via fingerprint-keyed pool |
| 18 | Determinism | 3 | - | - | Hashing uses canonical encoders; fingerprinting is version-tagged |
| 19 | KISS | 2 | small | low | `ImmutableRegistry.get()` is O(n) linear scan instead of dict lookup |
| 20 | YAGNI | 2 | small | low | `MappingRegistryAdapter` appears unused in scope -- potential dead code |
| 21 | Least astonishment | 2 | small | low | `utils/__init__.py` re-exports only UUID helpers, not all utils |
| 22 | Public contracts | 2 | small | low | `__all__` consistently declared; versioned structs |
| 23 | Design for testability | 2 | small | low | Pure util functions easily testable; `extraction/` harder due to session deps |
| 24 | Observability | 2 | small | low | `EngineEventRecorder` centralizes diagnostics; some raw `time.time()` calls |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Most modules expose clean public surfaces via `__all__` declarations and underscore-prefixed private helpers. The `cache/diskcache_factory.py` module-level `_CACHE_POOL` dict is a singleton that holds open cache connections.

**Findings:**
- `src/cache/diskcache_factory.py:203` -- `_CACHE_POOL: dict[str, Cache | FanoutCache] = {}` is a module-global mutable singleton. While prefixed with underscore, the `close_cache_pool()` function at line 206 operates on it, and `cache_for_kind()` at line 331 mutates it. This is acceptable as an internal pooling mechanism, but the pool lifetime is implicit.
- `src/extraction/engine_session.py:44` -- `EngineSession.df_ctx()` directly navigates `self.engine_runtime.datafusion_profile.session_runtime().ctx`, a 3-level chain that exposes internal structure.
- `src/graph/product_build.py:637-640` -- `_parse_build_result` uses `getattr(build_result, "cpg_outputs", {})` to access attributes on an untyped `object`, bypassing type safety.

**Suggested improvement:**
Add a typed protocol or return type for `orchestrate_build()` results instead of using `object` and `getattr` in `_parse_build_result`. For `EngineSession`, expose a direct `.session_context()` method that hides the navigation chain.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
The `extraction/materialize_pipeline.py` module (595 lines) is the most concerning file in scope. It mixes three distinct responsibilities: (1) cache decision logic, (2) view execution/plan building, and (3) output writing with detailed diagnostics recording.

**Findings:**
- `src/extraction/materialize_pipeline.py:74-183` -- `MaterializationCacheDecision` and its resolution logic (`_resolve_materialization_cache_decision`) are cache policy concerns that are interleaved with execution code.
- `src/extraction/materialize_pipeline.py:278-396` -- `build_view_product` is a 119-line function that handles view validation, scan unit resolution, cache decision, DataFrame execution, and result packaging -- at least 4 distinct concerns in one function.
- `src/extraction/materialize_pipeline.py:436-585` -- `write_extract_outputs` is a 150-line function that handles reader coercion, location resolution, quality diagnostics, Delta writing, and post-write diagnostics -- mixing IO with policy and diagnostics.
- `src/graph/build_pipeline.py:363-411` -- `_execute_engine_phase` is a 49-line function combining spec validation, JSON encoding, engine invocation, response parsing, and timing.

**Suggested improvement:**
Split `materialize_pipeline.py` into three modules: (1) `extraction/cache_policy.py` for `MaterializationCacheDecision` and resolution logic, (2) `extraction/view_execution.py` for `build_view_product` and plan-building helpers, (3) keep `write_extract_outputs` in `materialize_pipeline.py` focused solely on the write path.

**Effort:** medium
**Risk if unaddressed:** medium -- the mixed concerns make the module hard to test in isolation and create a gravity well that attracts further unrelated changes.

---

#### P3. SRP -- Alignment: 1/3

**Current state:**
`extraction/runtime_profile.py` (623 lines) is the largest single file in scope and changes for at least four distinct reasons: (1) profile resolution logic, (2) environment variable patching, (3) Rust engine interop, and (4) hashing/snapshot generation.

**Findings:**
- `src/extraction/runtime_profile.py:42-82` -- `RuntimeProfileSpec` is a data model with hash computation methods.
- `src/extraction/runtime_profile.py:84-186` -- Environment variable patch types (`RuntimeProfileEnvPatch`, `_RuntimeProfileEnvPatchRuntime`, `_runtime_profile_env_patch()`) form a self-contained concern that could be its own module.
- `src/extraction/runtime_profile.py:313-365` -- Named profile overrides (`_apply_named_profile_overrides`) encode specific profile policies (dev_debug, prod_fast, memory_tight) inline.
- `src/extraction/runtime_profile.py:490-523` -- Rust factory interop (`_collect_rust_profile_snapshot`, `_extract_rust_profile_payload`) is a separate integration concern.
- `src/extraction/diagnostics.py:121-325` -- `EngineEventRecorder` combines 7 recording methods for different event types. While unified recording is the stated purpose, the class changes whenever any event schema changes.

**Suggested improvement:**
Extract from `runtime_profile.py`: (1) `extraction/runtime_env.py` for env-patch logic, (2) `extraction/rust_profile_interop.py` for Rust factory integration. The core `resolve_runtime_profile` function and `RuntimeProfileSpec` remain in `runtime_profile.py`.

**Effort:** medium
**Risk if unaddressed:** medium -- the file is already 623 lines and growing; SRP violations create merge conflicts and harder code review.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Most modules are internally cohesive. The `arrow_utils/` package is the weakest -- it contains five small files that are thin wrappers or type aliases around `datafusion_engine.arrow` internals.

**Findings:**
- `src/arrow_utils/__init__.py` and `src/arrow_utils/core/__init__.py` -- Both are empty `__all__` exports. The package serves as a namespace but provides no cohesive API surface.
- `src/arrow_utils/core/expr_types.py:9` -- `type ScalarValue = ScalarLiteralInput` is a single type alias re-export from `datafusion_engine.expr.spec`.
- `src/arrow_utils/core/array_iter.py` -- Contains 3 iteration helpers that depend on `datafusion_engine.arrow.interop`, creating a thin forwarding layer.
- `src/arrow_utils/core/schema_constants.py` -- Schema metadata constants (`DEFAULT_VALUE_META`, `KEY_FIELDS_META`, etc.) are well-cohesive and standalone.

**Suggested improvement:**
Evaluate whether `arrow_utils/core/expr_types.py` (1 type alias) and `arrow_utils/core/array_iter.py` (3 functions depending on `datafusion_engine`) should be moved directly into `datafusion_engine.arrow` where their dependencies live, leaving only the standalone `schema_constants.py` and `ordering.py` in `arrow_utils/`.

**Effort:** small
**Risk if unaddressed:** low -- mostly an organizational concern.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The utility modules (`utils/`, `core/`, `validation/`) generally have correct dependency direction -- they depend on foundational libraries (msgspec, pyarrow) and not on domain modules. There is one violation.

**Findings:**
- `src/utils/validation.py:114` -- `ensure_table()` imports `from datafusion_engine.arrow.coercion import to_arrow_table`. A general-purpose validation utility should not depend on the DataFusion engine layer. This creates a dependency from a leaf utility to a mid-level domain module.
- `src/arrow_utils/core/array_iter.py:7` -- `import datafusion_engine.arrow.interop as pa` -- an arrow utility imports from `datafusion_engine`, inverting the expected direction (utilities should be depended upon, not depend on domain).
- `src/arrow_utils/core/streaming.py:12` -- `from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike, coerce_table_like` -- same pattern.

**Suggested improvement:**
Move `ensure_table()` from `utils/validation.py` to `datafusion_engine.arrow.coercion` or a dedicated `datafusion_engine.validation` module, since it fundamentally belongs to the DataFusion layer. For `arrow_utils/`, either move the modules into `datafusion_engine.arrow` or define the type aliases locally.

**Effort:** small
**Risk if unaddressed:** medium -- wrong dependency direction makes future refactoring harder and creates circular dependency risk.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The `test_support/datafusion_ext_stub.py` is a textbook adapter: it provides stub implementations of the Rust extension API surface, enabling testing without the Rust build. The pattern is well-applied.

**Findings:**
- `src/test_support/datafusion_ext_stub.py` -- 1205 lines providing ~40 stub functions. Each mirrors the Rust extension API with `IS_STUB = True` marker. This is a proper adapter implementation.
- `src/extraction/semantic_boundary.py` -- Functions `ensure_semantic_views_registered` and `is_semantic_view` form a boundary guard between extraction and semantics, which is a port-like pattern.

**Suggested improvement:**
No structural improvement needed. The stub module could benefit from a shared `_stub_return_expr` factory to reduce the 40+ repetitive `_stub_expr` wrappers, but this is a minor DRY concern within the adapter itself.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY -- Alignment: 1/3

**Current state:**
There are three clear cases of duplicated knowledge across the reviewed modules.

**Findings:**
- **Boolean parsing constants duplicated.** `src/utils/env_utils.py:14-15` defines `_TRUE_VALUES = frozenset({"1", "true", "yes", "y"})` and `_FALSE_VALUES = frozenset({"0", "false", "no", "n"})`. `src/extraction/runtime_profile.py:38-39` defines `_ENV_TRUE_VALUES = frozenset({"1", "true", "yes", "y"})` and `_ENV_FALSE_VALUES = frozenset({"0", "false", "no", "n"})` -- identical sets under different names. The `_env_patch_bool()` function at line 146-157 re-implements the same logic as `utils.env_utils.env_bool()`.
- **Constrained type aliases duplicated.** `src/runtime_models/types.py:10-13` defines `NonNegativeInt`, `PositiveInt`, `PositiveFloat` using pydantic `Field` constraints. `src/core_types.py:35-37` defines the same names using msgspec `Meta` constraints. While they serve different serialization frameworks, the semantic meaning is identical and the names are identical, creating confusion about which to import.
- **Boolean coercion duplicated.** `src/extraction/options.py:187-191` has a private `_coerce_bool(value, *, default)` function. `src/utils/value_coercion.py:75-95` has a more capable `coerce_bool(value)` function. `src/datafusion_engine/session/runtime_compile.py` has yet another private `_coerce_bool`. The knowledge of "how to coerce an object to bool" is scattered across three files.

**Suggested improvement:**
(1) Have `extraction/runtime_profile.py:_env_patch_bool` delegate to `utils.env_utils.env_bool` -- the logic is identical. (2) Add a docstring to `runtime_models/types.py` and `core_types.py` explicitly stating which framework each serves (pydantic vs msgspec) and cross-reference each other. (3) Consolidate `_coerce_bool` callsites to use `utils.value_coercion.coerce_bool` with a default wrapper.

**Effort:** small
**Risk if unaddressed:** medium -- boolean parsing logic that drifts between modules creates subtle validation inconsistencies.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Contract definitions are strong in the `planning_engine/spec_contracts.py` module, using frozen msgspec Structs with explicit field types and defaults. The `runtime_models/` package provides pydantic-based validation with model validators.

**Findings:**
- `src/planning_engine/spec_contracts.py` -- `SemanticExecutionSpec`, `InputRelation`, `ViewDefinition`, etc. are well-defined frozen contracts with explicit field types and defaults.
- `src/runtime_models/compile.py:37-45` -- `_validate_cache` model validator enforces cross-field constraints (cache_max_columns requires cache=True).
- `src/extraction/options.py:159-167` -- `_validate_diff_options` enforces the invariant that `changed_only=True` requires both refs. Good explicit precondition.
- `src/extraction/plan_product.py:34-45` -- `PlanProduct.value()` raises `ValueError` when neither stream nor table is set, but the dataclass allows construction of this invalid state.

**Suggested improvement:**
Consider adding a `__post_init__` validation to `PlanProduct` that ensures at least one of `stream` or `table` is not None, or restructure as a tagged union to prevent the invalid state.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`extraction/options.py` is a good example: `normalize_extraction_options()` accepts `ExtractionRunOptions | Mapping | None` and always returns a typed `ExtractionRunOptions`. The boundary parsing happens once.

**Findings:**
- `src/extraction/options.py:44-128` -- `normalize_extraction_options` is a proper parse-don't-validate boundary: messy inputs are converted to typed struct once.
- `src/extraction/contracts.py:59-91` -- `with_compat_aliases` and `resolve_semantic_input_locations` resolve legacy names to canonical names at the boundary.
- `src/graph/product_build.py:446-460` -- `_int_field` extracts int from `JsonDict` with type coercion, but the result is an untyped `int` rather than a parsed struct. The raw `JsonDict` flows through multiple functions without being parsed into a typed object.

**Suggested improvement:**
Parse the `build_result` from `orchestrate_build()` into a typed `BuildResult` struct immediately, rather than passing raw `dict[str, object]` through `_parse_build_result`, `_require_cpg_output`, `_int_field`, etc.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Most data models use frozen dataclasses or msgspec Structs, which prevents mutation after construction. A few models allow invalid states.

**Findings:**
- `src/extraction/plan_product.py:20-33` -- `PlanProduct` has both `stream: RecordBatchReaderLike | None` and `table: TableLike | None`, both defaulting to None. The `value()` method at line 34 raises ValueError if both are None, but construction doesn't prevent this.
- `src/extraction/engine_session.py:20-29` -- `EngineSession` has `diagnostics: DiagnosticsCollector | None = None` and `settings_hash: str | None = None`. The None defaults are appropriate for optional features, not an illegal-state concern.

**Suggested improvement:**
Consider restructuring `PlanProduct` to use a union type or a factory method that requires at least one of stream/table. Alternatively, add a `__post_init__` guard.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. The `EngineEventRecorder` methods are commands (they record events, return None). The `cache_for_kind` function is a query with a caching side effect (creates and pools the cache if not already present).

**Findings:**
- `src/cache/diskcache_factory.py:331-362` -- `cache_for_kind` both queries the pool and mutates it (creates + stores a new cache). This is a standard lazy-initialization pattern and acceptable, but worth noting.
- `src/cache/diskcache_factory.py:290-328` -- `bulk_cache_set` returns `int` (count of entries written) while also mutating cache state. This mixes command and query, though the return value is informational.

**Suggested improvement:**
No structural change needed. The `bulk_cache_set` return value is informational and consistent with the `diskcache` library's API conventions.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
`build_engine_runtime()` and `build_engine_session()` are proper composition roots that wire dependencies explicitly. The `extraction/engine_session_factory.py` module serves as a clear composition point.

**Findings:**
- `src/extraction/engine_session_factory.py:59-153` -- `build_engine_session` is a well-structured composition root: it accepts a spec and options, assembles an `EngineSession` with wired dependencies, and records diagnostics.
- `src/extraction/orchestrator.py:358-419` -- `_run_repo_scan` creates `ExtractSession` and `ExtractExecutionContext` internally rather than accepting them as parameters. This makes the function harder to test.
- `src/extraction/orchestrator.py:508-566` -- `_build_stage1_extractors` similarly creates `ExtractSession` internally. Three separate functions in `orchestrator.py` each call `resolve_runtime_profile("default")` and `build_engine_session()` independently (lines 384-386, 541-543, 850-852).

**Suggested improvement:**
Hoist the `RuntimeProfileSpec` and `ExtractSession` creation to `run_extraction()` and pass them as parameters to the stage functions. This eliminates three redundant session-creation calls and improves testability.

**Effort:** medium
**Risk if unaddressed:** low -- redundant session creation is wasteful but not currently broken.

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No deep inheritance hierarchies exist in any reviewed module. `RuntimeBase(BaseModel)` in `runtime_models/base.py` is a single-level inheritance that configures pydantic, which is appropriate.

**Findings:**
No violations found.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
Several modules navigate deep into collaborator structures, particularly when accessing DataFusion runtime profile internals.

**Findings:**
- `src/extraction/engine_session.py:44` -- `self.engine_runtime.datafusion_profile.session_runtime().ctx` -- 4-level chain.
- `src/extraction/engine_session.py:54` -- `self.engine_runtime.datafusion_profile.session_runtime()` -- 3-level chain.
- `src/extraction/engine_session.py:64-68` -- `datafusion_facade()` navigates the same chain plus constructs a facade.
- `src/extraction/diagnostics.py:136` -- `self.runtime_profile.diagnostics.diagnostics_sink` -- 2-level chain through internal struct.
- `src/extraction/diagnostics.py:231` -- `profile.policies.diskcache_profile` then `profile.diagnostics.diagnostics_sink` -- multiple chain accesses.
- `src/extraction/materialize_pipeline.py:346-348` -- `profile.view_registry.entries[view_name].cache_policy` then fallback `profile.data_sources.semantic_output.cache_overrides.get(view_name)` -- deep chain with conditional branching.
- `src/extraction/engine_session_factory.py:105-117` -- Deep nested `msgspec.structs.replace` calls navigating `df_profile.catalog.registry_catalogs`, `df_profile.policies.input_plugins`, etc.

**Suggested improvement:**
Add convenience methods to `DataFusionRuntimeProfile` such as `.session_context()`, `.diagnostics_sink()`, `.diskcache_profile()`, and `.cache_policy_for(view_name)` that encapsulate the internal navigation. The `EngineSession` methods already attempt this but end up re-implementing the chains.

**Effort:** medium
**Risk if unaddressed:** medium -- deep chains break when intermediate structures are refactored, causing cascading changes across modules.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`PlanProduct.value()` and `PlanProduct.materialize_table()` are good "tell" examples -- they encapsulate the decision of stream vs. table. However, several patterns expose raw data for external logic.

**Findings:**
- `src/extraction/materialize_pipeline.py:346-348` -- External code inspects `profile.view_registry.entries[view_name].cache_policy` and `profile.data_sources.semantic_output.cache_overrides.get(view_name)` to determine cache policy. This should be a method on the profile: `profile.cache_policy_for(view_name)`.
- `src/extraction/diagnostics.py:230-258` -- `record_diskcache_stats` iterates over hardcoded kind strings and manually accesses `settings.size_limit_bytes`, `settings.eviction_policy`, etc. The settings object should provide a `to_diagnostics_payload()` method.

**Suggested improvement:**
Add `DiskCacheSettings.to_diagnostics_payload()` to eliminate the manual field extraction in `record_diskcache_stats`. Add `DataFusionRuntimeProfile.cache_policy_for(view_name)` to encapsulate the fallback logic.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The `utils/` modules are nearly pure: `hashing.py`, `value_coercion.py`, `validation.py`, `storage_options.py` are all deterministic transforms with no IO. The `extraction/` modules mix IO (file writes, Delta operations, otel configuration) throughout.

**Findings:**
- `src/utils/hashing.py` -- All functions are pure deterministic transforms. Exemplary.
- `src/utils/env_utils.py` -- Reads `os.environ` (an IO boundary) but does so consistently with clear function boundaries.
- `src/extraction/materialize_pipeline.py:305-308` -- `build_view_product` calls `configure_otel()` as a side effect at the top of a function that should be about view execution.
- `src/extraction/orchestrator.py:321-344` -- `_write_delta` performs filesystem IO (mkdir, deltalake write, logging) inline within the orchestration flow.

**Suggested improvement:**
Remove the `configure_otel()` call from `build_view_product` -- it should be configured once at the build entry point, not on every view execution.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
`cache_for_kind` is idempotent via fingerprint-keyed pooling. `_write_delta` uses `mode="overwrite"` which is idempotent for the same input. `configure_otel` is called multiple times from different entry points but is designed to be idempotent.

**Findings:**
- `src/cache/diskcache_factory.py:339-341` -- `cache_for_kind` checks fingerprint before creating, ensuring idempotent access.
- `src/extraction/orchestrator.py:342` -- `deltalake.write_deltalake(loc_str, table, mode="overwrite")` is idempotent.
- `src/extraction/engine_session_factory.py:129` -- `if not resolved.diagnostics.artifacts_snapshot().get("engine_runtime_v2")` guards against duplicate artifact recording.

**Suggested improvement:**
No structural change needed.

**Effort:** small
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Excellent. The hashing module uses canonical encoders (`MSGPACK_ENCODER`, `JSON_ENCODER_SORTED`), fingerprints include version tags (`PROFILE_HASH_VERSION = 3`), and `CompositeFingerprint` provides a structured approach to composable fingerprints.

**Findings:**
- `src/utils/hashing.py` -- Three hash variants (msgpack_default, msgpack_canonical, json_canonical) with clear semantics.
- `src/core/fingerprinting.py:20-25` -- `CompositeFingerprint` with version and sorted components.
- `src/core/config_base.py:17-28` -- `FingerprintableConfig` protocol with clear contract.
- `src/extraction/runtime_profile.py:27` -- `PROFILE_HASH_VERSION: int = 3` ensures schema evolution is tracked.

**Suggested improvement:**
No change needed. This is well-designed.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are appropriately simple. Two minor complexity concerns exist.

**Findings:**
- `src/utils/registry_protocol.py:166-182` -- `ImmutableRegistry.get()` uses a linear scan over `_entries: tuple[tuple[K, V], ...]`. For a registry that might hold many entries, this is O(n) per lookup instead of O(1) with a dict. The tuple storage prevents efficient lookup.
- `src/runtime_models/__init__.py:52-61` -- Lazy-loading `__getattr__` pattern with `_EXPORT_MAP` is clever but adds complexity for what could be simple imports. The pattern is justified if import cost is a concern (pydantic TypeAdapters can be expensive to construct).
- `src/graph/product_build.py:550-563` -- `_resolve_packed_ref` is a custom git packed-refs parser. While simple, this is a concern that git libraries handle more robustly.

**Suggested improvement:**
Change `ImmutableRegistry._entries` to store a `dict` internally (or `types.MappingProxyType` for true immutability) while keeping the frozen dataclass. The tuple representation causes needless O(n) lookups.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most abstractions earn their keep through actual use. A few elements appear speculative or unused.

**Findings:**
- `src/utils/registry_protocol.py:236-335` -- `MappingRegistryAdapter` with `read_only` and `allow_overwrite` modes. It is unclear whether this adapter is used anywhere in the reviewed scope. If unused, it is speculative generality.
- `src/extraction/engine_session_factory.py:27-46` -- `SemanticBuildOptions` with 4 boolean fields. Line 79 shows `_ = resolved.build_options  # Reserved for future use` -- explicitly unused and speculative.
- `src/core/fingerprinting.py:82-98` -- `CompositeFingerprint.extend()` provides a merge capability. If no callers exist, it is speculative.

**Suggested improvement:**
Remove or deprecate `SemanticBuildOptions` if it has no callers. If `MappingRegistryAdapter` has no callers in the codebase, consider removing it.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
API naming is generally predictable. One naming inconsistency stands out.

**Findings:**
- `src/utils/__init__.py` -- Re-exports only `uuid_factory` and `schema_from_struct` helpers but NOT `env_utils`, `hashing`, `validation`, `value_coercion`, `storage_options`, or `file_io`. A developer importing `from utils import env_bool` would be surprised to find it unavailable. The `__init__.py` surface does not represent the module's actual breadth.
- `src/extraction/` vs `src/extract/` -- Two packages with similar names. `extraction/` is the orchestration layer that imports from `extract/`, while `extract/` contains the actual extractor implementations. The dependency flows `extraction/ -> extract/` (confirmed by grep). The naming is confusing: "extraction" and "extract" are near-synonyms.

**Suggested improvement:**
Either rename `src/extraction/` to `src/extraction_orchestrator/` or `src/pipeline/` to clearly distinguish it from `src/extract/`, or add prominent module docstrings explaining the relationship. For `utils/__init__.py`, either re-export the key submodule APIs or keep it minimal with a clear docstring explaining that submodules should be imported directly.

**Effort:** small
**Risk if unaddressed:** low -- naming confusion increases onboarding friction.

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
All modules consistently declare `__all__` exports. Versioned contracts use frozen msgspec Structs (`OrchestrateBuildRequestV1`, `RunExtractionRequestV1`).

**Findings:**
- All 51 reviewed files declare `__all__`.
- Request types use V1 suffixes (`OrchestrateBuildRequestV1`, `RunExtractionRequestV1`).
- `src/planning_engine/spec_contracts.py:145-158` -- `SemanticExecutionSpec` has `version: int` as an explicit contract version field.

**Suggested improvement:**
No change needed. Contract versioning is well-applied.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Pure utility functions in `utils/` are trivially testable. The `extraction/` orchestration modules are harder to test because they create sessions and profiles internally.

**Findings:**
- `src/utils/hashing.py`, `src/utils/value_coercion.py`, `src/utils/validation.py` -- All pure functions, trivially unit-testable.
- `src/extraction/orchestrator.py:358-419` -- `_run_repo_scan` creates `RuntimeProfileSpec`, `EngineSession`, `ExtractSession`, and `ExtractExecutionContext` internally. Testing this function requires the full runtime stack or extensive mocking.
- `src/extraction/orchestrator.py:508-566` -- `_build_stage1_extractors` similarly creates sessions internally. Each lambda captures the session, making it impossible to inject test doubles.
- `src/cache/diskcache_factory.py:203` -- Module-level `_CACHE_POOL` makes testing cache behavior require careful teardown (calling `close_cache_pool()`).

**Suggested improvement:**
Accept `RuntimeProfileSpec` and `ExtractSession` as parameters in `_run_repo_scan`, `_build_stage1_extractors`, `_run_python_imports`, and `_run_python_external`. This eliminates the internal session creation and enables testing with lightweight stubs.

**Effort:** medium
**Risk if unaddressed:** low -- but reduced testability means fewer unit tests and more reliance on integration testing.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
`EngineEventRecorder` centralizes diagnostics recording with typed event payloads (`PlanExecutionEvent`, `ExtractWriteEvent`, `ExtractQualityEvent`). The `graph/build_pipeline.py` uses structured `emit_diagnostics_event` calls.

**Findings:**
- `src/extraction/diagnostics.py:17-42` -- `PlanExecutionEvent` with typed fields and `to_payload()` method. Good structured approach.
- `src/extraction/diagnostics.py:44-76` -- `ExtractWriteEvent` uses `time.time() * 1000` for timestamps (line 67). This is repeated in `ExtractQualityEvent` (line 102) and `record_delta_maintenance` (line 283). The timestamp generation should be a shared utility.
- `src/graph/build_pipeline.py:159-203` -- `orchestrate_build` uses `stage_span` for OpenTelemetry tracing. Well-structured.
- `src/extraction/orchestrator.py:168-181` -- Error handling records `{"extractor": "repo_scan", "error": str(exc)}` as unstructured dicts. These should use typed error events.

**Suggested improvement:**
Extract `_event_time_ms()` as a shared utility in the diagnostics module rather than repeating `int(time.time() * 1000)`. Define a typed `ExtractionErrorEvent` instead of using raw dicts for error recording in the orchestrator.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: `extraction/` is an orchestration layer that has outgrown its structure

**Description:** The `src/extraction/` package started as a thin orchestration layer between `src/extract/` (actual extractors) and `src/graph/` (build pipeline). It has grown to 13 files and ~3,200 lines, with `materialize_pipeline.py` (595 lines) and `runtime_profile.py` (623 lines) being the largest. These two files each violate SRP by combining multiple distinct concerns.

**Root cause:** Orchestration logic naturally attracts related concerns (cache policy, diagnostics, profile management) because it sits at the intersection of multiple layers.

**Affected principles:** P2 (Separation of concerns), P3 (SRP), P14 (Law of Demeter), P23 (Testability).

**Suggested approach:** Apply internal decomposition: split `materialize_pipeline.py` into cache-policy, view-execution, and write-path modules. Split `runtime_profile.py` into profile-resolution, env-patching, and Rust-interop modules. This keeps the package boundary stable while improving internal cohesion.

### Theme 2: Duplicated knowledge across serialization framework boundaries

**Description:** The codebase uses two serialization frameworks (msgspec for hot paths and pydantic for runtime validation). This creates parallel type definitions -- `NonNegativeInt` is defined in both `core_types.py` (msgspec) and `runtime_models/types.py` (pydantic). Boolean parsing constants are duplicated in `env_utils.py` and `runtime_profile.py`.

**Root cause:** The deliberate dual-framework strategy (msgspec for performance, pydantic for validation) naturally creates parallel type hierarchies.

**Affected principles:** P7 (DRY).

**Suggested approach:** Accept the parallel types as a necessary cost of the dual-framework strategy, but add explicit cross-references in docstrings. For boolean parsing, consolidate to a single canonical source (`env_utils`) and have other modules delegate to it.

### Theme 3: `extract/` vs `extraction/` naming confusion

**Description:** Two packages with near-synonym names exist: `src/extract/` (extractor implementations) and `src/extraction/` (orchestration layer). They have a clear dependency relationship (`extraction/ -> extract/`), but the naming does not communicate this distinction.

**Root cause:** Likely historical -- `extract/` was the original package, and `extraction/` was added later as an orchestration layer.

**Affected principles:** P21 (Least astonishment).

**Suggested approach:** Add prominent module-level docstrings to both `extraction/__init__.py` and `extract/__init__.py` documenting the relationship. A rename of `extraction/` to something more distinct (e.g., `extract_pipeline/` or `extract_orchestrator/`) would be ideal but has high blast radius.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Consolidate boolean parsing: have `extraction/runtime_profile.py:_env_patch_bool` delegate to `utils.env_utils.env_bool` instead of reimplementing | small | Eliminates duplicated knowledge that can drift |
| 2 | P5 (Dep direction) | Move `ensure_table()` from `utils/validation.py` to `datafusion_engine.arrow` where its dependency lives | small | Fixes dependency inversion in utility layer |
| 3 | P16 (FC/IS) | Remove `configure_otel()` call from `materialize_pipeline.py:build_view_product` (line 305) -- OTel should be configured once at build entry | small | Eliminates repeated side effect in execution path |
| 4 | P19 (KISS) | Change `ImmutableRegistry._entries` from tuple to dict for O(1) lookups | small | Improves performance without changing API |
| 5 | P20 (YAGNI) | Remove `SemanticBuildOptions` (unused, line 79: `_ = resolved.build_options  # Reserved for future use`) | small | Removes dead code and speculative abstraction |

## Recommended Action Sequence

1. **Consolidate boolean parsing constants** (P7) -- Make `extraction/runtime_profile.py` import from `utils.env_utils` instead of defining its own `_ENV_TRUE_VALUES`/`_ENV_FALSE_VALUES` and `_env_patch_bool`. No blast radius; purely internal.

2. **Fix utility dependency direction** (P5) -- Move `ensure_table()` from `utils/validation.py` to `datafusion_engine.arrow.coercion` or a new `datafusion_engine.validation` module. Update the ~2 import sites.

3. **Remove unused `SemanticBuildOptions`** (P20) -- Delete the class and the `_ = resolved.build_options` line in `engine_session_factory.py`. Update `EngineSessionOptions` to remove the field.

4. **Remove `configure_otel` from `build_view_product`** (P16) -- This side effect should happen once at build entry, not per-view. Check that the build entry point (`build_graph_product`) already calls it (it does, at line 137).

5. **Fix `ImmutableRegistry` to use dict** (P19) -- Replace `_entries: tuple[tuple[K, V], ...]` with a `_entries: Mapping[K, V]` backed by a `MappingProxyType(dict(...))` for O(1) lookups while maintaining immutability.

6. **Split `materialize_pipeline.py`** (P2, P3) -- Extract cache decision logic into `extraction/cache_policy.py` and view execution into `extraction/view_execution.py`. This is a larger refactor with ~3 new files and updates to import sites.

7. **Split `runtime_profile.py`** (P3) -- Extract env-patch logic into `extraction/runtime_env.py` and Rust interop into `extraction/rust_profile_interop.py`. Approximately 200 lines move to each new module.

8. **Hoist session creation in orchestrator** (P12, P23) -- Pass `RuntimeProfileSpec` and `ExtractSession` as parameters to `_run_repo_scan`, `_build_stage1_extractors`, `_run_python_imports`, and `_run_python_external` instead of creating them internally. This eliminates 3 redundant session-creation calls and improves testability.

9. **Add convenience methods to `DataFusionRuntimeProfile`** (P14) -- Add `.session_context()`, `.diagnostics_sink()`, `.cache_policy_for(view_name)` methods to eliminate Law of Demeter chain violations across `extraction/` modules. This requires changes in `datafusion_engine/session/runtime.py`.

10. **Evaluate `arrow_utils/` consolidation** (P4, P5) -- Move `array_iter.py` and `streaming.py` into `datafusion_engine.arrow` where their dependencies live. Keep `schema_constants.py` and `ordering.py` in `arrow_utils/` as standalone Arrow-native helpers.
