# Design Review: src/datafusion_engine/session/, bootstrap/, and root-level files

**Date:** 2026-02-16
**Scope:** `src/datafusion_engine/session/` (33 files), `src/datafusion_engine/bootstrap/` (2 files), `src/datafusion_engine/*.py` (8 root-level files)
**Focus:** All principles (1-24), with emphasis on SRP across the 33 session files, information hiding of runtime internals, and dependency direction from session to other submodules
**Depth:** moderate (20 files deep-read, remaining files header-scanned)
**Files reviewed:** 43

## Executive Summary

The `session/` package shows clear evidence of a recent decomposition: a formerly monolithic `runtime.py` was split into 12+ sub-modules (`runtime_compile.py`, `runtime_hooks.py`, `runtime_telemetry.py`, etc.). However, the split was mechanical rather than architectural -- `runtime.py` remains a 1,293-line "re-export hub" that imports and re-exports ~115 names from all sub-modules, preserving the original coupling surface rather than establishing clean boundaries. The core `DataFusionRuntimeProfile` class inherits from 4 mixins via `cast("DataFusionRuntimeProfile", self)` anti-patterns, duplicated constants appear across 3+ modules, and module-level mutable state (session caches, settings overlays) is scattered without central ownership. The most impactful improvements would be: (1) making callers import from the specific sub-modules instead of the re-export hub, (2) consolidating duplicated constants into a single shared-constants module, and (3) replacing mixin-cast patterns with composition or explicit delegation.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | high | `runtime.py` re-exports 115 names including private `_`-prefixed functions |
| 2 | Separation of concerns | 1 | large | medium | `DataFusionRuntimeProfile` mixes config, lifecycle, I/O, Delta ops, diagnostics, telemetry |
| 3 | SRP | 1 | large | medium | `runtime_dataset_io.py` (890 LOC) handles dataset I/O, schema alignment, cache config, readiness checks |
| 4 | High cohesion, low coupling | 1 | medium | high | runtime.py couples to 12 sibling modules via star-like re-import pattern |
| 5 | Dependency direction | 2 | medium | medium | Sub-modules use TYPE_CHECKING for profile but still have runtime references |
| 6 | Ports & Adapters | 2 | medium | low | `protocols.py` defines clean protocols but `DataFusionRuntimeProfile` does not implement them |
| 7 | DRY | 0 | small | high | `_parse_major_version` duplicated (2x), `CACHE_PROFILES` (2x), `KIB/MIB` (2x), `_DATAFUSION_SQL_ERROR` (3x), `_create_schema_introspector` (2x), `_RUNTIME_SESSION_ID` (2x), `_SESSION_CONTEXT_CACHE` (2x), `_EXTENSION_MODULE_NAMES` (2x) |
| 8 | Design by contract | 2 | small | low | Good `__post_init__` validation in `DataFusionRuntimeProfile`; config structs are frozen |
| 9 | Parse, don't validate | 2 | small | low | Config parsed once via msgspec Struct constructors |
| 10 | Make illegal states unrepresentable | 2 | small | low | Frozen dataclasses and enums used; `IdentifierNormalizationMode` is a StrEnum |
| 11 | CQS | 2 | small | low | Minor: `session_context()` both creates and caches (side-effect + return) |
| 12 | DI + explicit composition | 1 | medium | medium | Mixin-cast pattern bypasses DI; 4 mixins inherit into `DataFusionRuntimeProfile` |
| 13 | Composition over inheritance | 1 | medium | medium | 4-mixin diamond inheritance: `_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`, `_RuntimeProfileDeltaFacadeMixin`, `_RuntimeDiagnosticsMixin` |
| 14 | Law of Demeter | 1 | medium | medium | `profile.policies.diskcache_profile.ttl_for("schema")` chains in 6+ places |
| 15 | Tell, don't ask | 2 | small | low | Operations helpers (`RuntimeProfileDeltaOps`, `RuntimeProfileIO`) move in right direction |
| 16 | Functional core, imperative shell | 2 | medium | low | Config structs are pure; but session creation mixes IO and state |
| 17 | Idempotency | 2 | small | low | Session caching is idempotent; bootstrap supports `strict_zero_rows` mode |
| 18 | Determinism | 2 | small | low | `session_runtime_hash` captures UDF and settings snapshots for reproducibility |
| 19 | KISS | 1 | medium | medium | 33 session files is excessive; many are thin facades (lifecycle.py: 32 LOC, diagnostics.py: 34 LOC) |
| 20 | YAGNI | 2 | small | low | Feature gates exist but are used; no speculative abstractions detected |
| 21 | Least astonishment | 1 | medium | medium | `runtime.py` re-exports private `_`-prefixed names publicly; `_SESSION_CONTEXT_CACHE` shadowed |
| 22 | Declare public contracts | 1 | medium | medium | `__all__` has 115 entries mixing config structs, hook types, IO functions, and constants |
| 23 | Design for testability | 1 | medium | medium | Module-level mutable caches (`_SESSION_CONTEXT_CACHE`, `_SESSION_RUNTIME_CACHE`) make test isolation hard |
| 24 | Observability | 2 | small | low | Telemetry, diagnostics hooks, artifact recording all present; structured payloads |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
`runtime.py` (1,293 LOC) contains only ~60 lines of original logic (the `DataFusionRuntimeProfile` class definition and two thin wrapper functions). The remaining ~1,230 lines are import statements re-exporting names from 12 sibling `runtime_*.py` modules. It re-exports 115 names in `__all__`, including private `_`-prefixed symbols like `_introspection_cache_for_ctx`, `_effective_catalog_autoload_for_profile`, and `_resolved_config_policy_for_profile`.

**Findings:**
- `runtime.py:64-261` -- 197 lines of re-import statements from 12 sub-modules, explicitly importing both public and private names
- `runtime.py:1162-1277` -- `__all__` list with 115 entries, including names from config policies, hooks, telemetry, dataset I/O, session management, compile helpers, and introspection
- 27 files across `src/` import `from datafusion_engine.session.runtime import X`, treating runtime.py as the single gateway to the entire session subsystem
- `runtime.py:302` defines `_SESSION_CONTEXT_CACHE: dict[str, SessionContext] = {}` which shadows the same variable at `runtime_session.py:51` -- both are module-level mutable dicts but they are separate Python objects, creating a latent correctness risk

**Suggested improvement:**
Migrate callers to import directly from the specific sub-modules (`runtime_config_policies`, `runtime_hooks`, `runtime_telemetry`, etc.) rather than through the `runtime.py` re-export hub. Reduce `runtime.py` to only export `DataFusionRuntimeProfile`, `SessionRuntime`, and the few functions that directly operate on them. Remove private `_`-prefixed names from `__all__` entirely.

**Effort:** medium
**Risk if unaddressed:** high -- Any internal restructuring of sub-modules requires updating the re-export list in runtime.py, and the 27 callers all couple to the 115-name surface area

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`DataFusionRuntimeProfile` at `runtime.py:331-1160` is an 830-line class that directly handles: configuration holding (fields), session context construction (`session_context()`), session caching (`_cached_context`/`_cache_context`), Delta operations (via `delta_ops` property), I/O operations (via `io_ops` property), UDF catalog management (`udf_catalog()`), SQL options resolution (`sql_options()`), schema introspection (`_schema_introspector`), zero-row bootstrap validation (`run_zero_row_bootstrap_validation`), filesystem registration (`_register_local_filesystem`), and context pool management (`context_pool()`).

**Findings:**
- `runtime.py:550-584` -- `session_context()` method is a 35-line orchestration sequence mixing context creation, URL table enablement, filesystem registration, plugin installation, registry catalog setup, view schema, UDF platform, planner rules, schema registry, prepared statements, Delta codecs, extension parity, physical expr adapters, tracing, and cache tables -- all in one method
- `runtime.py:703-773` -- `run_zero_row_bootstrap_validation()` is a 70-line method that combines semantic context creation, manifest building, artifact recording, and bootstrap execution -- this is pipeline orchestration inside a configuration object
- `runtime_dataset_io.py:1-890` -- 890 lines mixing dataset I/O, schema alignment, cache configuration, readiness payload construction, and Delta snapshot management

**Suggested improvement:**
Extract `session_context()` orchestration into a `SessionContextBuilder` that accepts a `DataFusionRuntimeProfile` as input. Move `run_zero_row_bootstrap_validation()` to the `bootstrap/` package where it belongs. Split `runtime_dataset_io.py` into at least three modules: dataset schema ops, cache configuration, and readiness diagnostics.

**Effort:** large
**Risk if unaddressed:** medium -- Adding any new session initialization step requires modifying the already-complex `session_context()` method

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
The 33 session files show uneven granularity. Some are overly large and multi-purpose; others are trivially thin facades that add no value.

**Findings:**
- `runtime_dataset_io.py` (890 LOC) -- Changes for: dataset location resolution, schema alignment, cache prefix generation, readiness payload construction, Delta snapshot caching, introspection cache access. At least 5 distinct reasons to change.
- `runtime_schema_registry.py` (1,579 LOC) -- The largest file; handles schema registry installation, dataset registration, prepared statement execution, constraint validation, and diagnostics snapshot recording.
- `runtime_extensions.py` (1,432 LOC) -- Handles UDF platform installation, planner rule installation, tracing, cache management, Delta plan codecs, physical expr adapters, and extension parity validation.
- `lifecycle.py` (32 LOC), `diagnostics.py` (34 LOC), `api.py` (13 LOC) -- Trivially thin one-function wrappers that delegate entirely to `runtime.py`. These add import indirection without adding encapsulation value.

**Suggested improvement:**
Merge trivial facade files (`lifecycle.py`, `diagnostics.py`, `api.py`) back into their natural homes. Split the 1,000+ LOC files along clear responsibility lines: `runtime_schema_registry.py` should separate schema installation from constraint validation from diagnostics recording.

**Effort:** large
**Risk if unaddressed:** medium -- The large files become increasingly difficult to navigate and test independently

---

#### P4. High cohesion, low coupling -- Alignment: 1/3

**Current state:**
`runtime.py` sits at the center of a star topology, importing from and re-exporting names from 12 sibling modules. Every sibling module depends on `DataFusionRuntimeProfile` (defined in `runtime.py`), and `runtime.py` imports from every sibling. This creates a circular-dependency-like coupling structure that is resolved by `TYPE_CHECKING` guards but remains conceptually coupled.

**Findings:**
- `runtime.py:64-261` -- Imports from 12 distinct `runtime_*.py` sibling modules in a single file
- Every `runtime_*.py` file has `if TYPE_CHECKING: from datafusion_engine.session.runtime import DataFusionRuntimeProfile` -- the type dependency is universal
- `runtime_diagnostics_mixin.py` imports from 5 other session sub-modules: `runtime_compile`, `runtime_config_policies`, `runtime_telemetry`, `features`, and `cache_policy`
- `context_pool.py:36` and `runtime_config_policies.py:51` both independently define `_parse_major_version` rather than sharing

**Suggested improvement:**
Move `DataFusionRuntimeProfile` into its own `profile.py` or `profile_class.py` module that does NOT import from sibling modules. Have the profile class use a builder or explicit composition to wire in capabilities from sub-modules. This breaks the star-dependency pattern.

**Effort:** medium
**Risk if unaddressed:** high -- Any change to any sub-module risks breaking the re-export chain in runtime.py

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Sub-modules correctly use `TYPE_CHECKING` to avoid runtime circular imports with `DataFusionRuntimeProfile`. Config structs in `runtime_profile_config.py` and `contracts.py` have no dependency on the runtime class. The `protocols.py` file defines clean protocols at the boundary.

**Findings:**
- `runtime_compile.py:29-34` -- Uses TYPE_CHECKING guard correctly for profile dependency
- `runtime_hooks.py:32-38` -- Same pattern, clean TYPE_CHECKING boundary
- However, `runtime_ops.py:94` defines `RuntimeProfileDeltaOps` with a runtime `profile: DataFusionRuntimeProfile` field, meaning this is a runtime dependency, not a type-only dependency. The `DataFusionRuntimeProfile` type annotation is used at runtime.
- `runtime.py:289-291` -- `DatasetLocation` imported at runtime from `datafusion_engine.dataset.registry`, pulling in dataset layer into session construction

**Suggested improvement:**
For the `RuntimeProfileDeltaOps`, `RuntimeProfileIO`, and `RuntimeProfileCatalog` helper classes, define a `RuntimeProfileLike` protocol that specifies the minimal surface they need (e.g., `session_context()`, `policies`, `execution`), and depend on the protocol instead of the concrete class.

**Effort:** medium
**Risk if unaddressed:** medium -- Concrete dependencies make it harder to test or reuse helper classes independently

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`protocols.py` defines three clean protocol interfaces (`RuntimeSettingsProvider`, `RuntimeTelemetryProvider`, `RuntimeArtifactRecorder`). These are well-designed ports. However, `DataFusionRuntimeProfile` does not explicitly implement them; the mixin classes informally satisfy them through duck typing.

**Findings:**
- `protocols.py:10-38` -- Three clean, minimal protocols are defined
- `DataFusionRuntimeProfile` at `runtime.py:331` inherits from `_RuntimeDiagnosticsMixin` which provides `record_artifact` and `settings_payload`, informally satisfying `RuntimeSettingsProvider` and `RuntimeArtifactRecorder`, but this is accidental structural compatibility, not declared intent
- `facade.py:123` -- `DataFusionExecutionFacade` is a proper ports pattern, wrapping ctx + profile and providing compile/execute/write operations

**Suggested improvement:**
Have `DataFusionRuntimeProfile` explicitly declare protocol conformance (e.g., by listing protocols in the class bases or using an explicit protocol check in tests). This makes the contract intentional rather than accidental.

**Effort:** medium
**Risk if unaddressed:** low -- Current duck typing works but makes refactoring risky since breaking a protocol method won't produce a clear error

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 0/3

**Current state:**
The most severe alignment gap in the entire review. Multiple constants and helper functions are independently defined in 2-3 separate modules with identical semantics.

**Findings:**
- `_parse_major_version`: defined identically at `runtime_config_policies.py:51` and `context_pool.py:36`
- `CACHE_PROFILES` dict: defined identically at `runtime_config_policies.py:447` and `runtime_telemetry.py:57` -- both are `Mapping[str, Mapping[str, str]]` with the same three cache profile presets
- `KIB: int = 1024` / `MIB: int = 1024 * KIB`: defined independently at `runtime_config_policies.py:46-47` and `runtime_telemetry.py:47-48`
- `_DATAFUSION_SQL_ERROR = Exception`: defined at `runtime_extensions.py:56`, `runtime_udf.py:34`, and `runtime.py:272`
- `_create_schema_introspector`: defined independently at `runtime_extensions.py:61` and `runtime_schema_registry.py:78` with near-identical bodies
- `_RUNTIME_SESSION_ID: Final[str] = uuid7_str()`: defined at `runtime_session.py:33` and `runtime_hooks.py:50` -- each call to `uuid7_str()` produces a DIFFERENT ID, so these are two independent session IDs with the same name
- `_EXTENSION_MODULE_NAMES`: defined at `runtime_ops.py:36` and `runtime.py:270`
- `_SESSION_CONTEXT_CACHE: dict[str, SessionContext] = {}`: defined at both `runtime.py:302` and `runtime_session.py:51` -- two separate dictionaries

**Suggested improvement:**
Create a `session/_constants.py` module that owns all shared constants (`KIB`, `MIB`, `GIB`, `CACHE_PROFILES`, `_DATAFUSION_SQL_ERROR`, `_EXTENSION_MODULE_NAMES`, `_parse_major_version`). For `_RUNTIME_SESSION_ID`, decide which module is authoritative and have the other import from it. For `_create_schema_introspector`, extract to a shared `session/_introspection_factory.py`. Remove the shadowed `_SESSION_CONTEXT_CACHE` in `runtime_session.py`.

**Effort:** small
**Risk if unaddressed:** high -- The dual `_RUNTIME_SESSION_ID` means hooks and session management may record different session IDs for the same logical session. The dual `_SESSION_CONTEXT_CACHE` is a latent correctness bug.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Config structs use frozen msgspec Structs with typed fields. `DataFusionRuntimeProfile.__post_init__` at `runtime.py:448-475` validates information_schema enablement, catalog name consistency, and async UDF policy.

**Findings:**
- `runtime.py:405-428` -- `_validate_information_schema` and `_validate_catalog_names` are proper precondition checks
- `runtime.py:472-475` -- Async UDF policy validation in `__post_init__` is a good contract enforcement pattern
- `cache_policy.py:12-51` -- `CachePolicyConfig` is a well-designed value object with fingerprint support
- Minor gap: `session_context()` at `runtime.py:550` does not validate that the profile is in a consistent state before building -- it relies entirely on `__post_init__` having run

**Suggested improvement:**
No major changes needed. Consider adding a `_validate_policies()` step in `__post_init__` that checks for conflicting policy combinations (e.g., spill enabled but no spill directory).

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Configuration is parsed once through msgspec Struct constructors. `RuntimeProfileConfig` at `config_structs.py:18-29` is a frozen struct with typed default factories. `IdentifierNormalizationMode` at `contracts.py:10-15` is a StrEnum that constrains valid values at construction time.

**Findings:**
- `contracts.py:10-15` -- `IdentifierNormalizationMode` uses StrEnum, properly constraining values
- `delta_session_builder.py:66-80` -- `split_runtime_settings` parses settings into structured categories at boundary
- Minor gap: `runtime_config_policies.py:58-66` -- `_catalog_autoload_settings()` reads from `os.environ` and returns raw strings without parsing into a structured type

**Suggested improvement:**
No major action needed. The env-var reading in `_catalog_autoload_settings` could return a typed `CatalogAutoloadConfig` struct instead of a raw dict.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Good use of frozen dataclasses and msgspec Structs prevents mutation after construction. `ZeroRowRole` at `bootstrap/zero_row.py:64-70` uses `Literal` types to constrain valid roles.

**Findings:**
- `runtime.py:331-338` -- `DataFusionRuntimeProfile` is frozen=True, preventing post-construction mutation (with explicit `object.__setattr__` only in `__post_init__`)
- `runtime.py:456-471` -- Uses `object.__setattr__` to set defaults in `__post_init__` on a frozen struct, which is a well-known msgspec pattern but technically bypasses the frozen guarantee
- `runtime_profile_config.py:51` -- `MemoryPool = Literal["greedy", "fair", "unbounded"]` properly constrains valid pool types

**Suggested improvement:**
The `object.__setattr__` pattern in `__post_init__` is standard for msgspec frozen Structs. No changes needed.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. Config query methods like `settings_payload()`, `fingerprint()`, and `telemetry_payload()` are pure queries. Session construction methods have clear side effects.

**Findings:**
- `runtime.py:550-584` -- `session_context()` violates CQS: it both creates/configures a session context AND caches it in `_SESSION_CONTEXT_CACHE`. Callers expect a pure "get" but the first call triggers extensive side effects.
- `runtime.py:693-701` -- `session_runtime()` similarly both builds and caches.
- `runtime_session.py:170-180` -- `record_runtime_setting_override` is a clean command (no return value).

**Suggested improvement:**
Rename `session_context()` to `ensure_session_context()` or split into `build_session_context()` (command) and `get_session_context()` (query). This makes the caching/creation side effect explicit in the name.

**Effort:** small
**Risk if unaddressed:** low -- The current behavior works but surprises callers who expect idempotent queries

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 1/3

**Current state:**
`DataFusionRuntimeProfile` uses a 4-mixin inheritance pattern where mixins access the profile via `cast("DataFusionRuntimeProfile", self)`. This is an anti-pattern: the mixins nominally don't know their host type but must cast to it, creating an implicit circular dependency.

**Findings:**
- `runtime.py:331-338` -- `DataFusionRuntimeProfile` inherits from `_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`, `_RuntimeProfileDeltaFacadeMixin`, `_RuntimeDiagnosticsMixin`, plus `StructBaseStrict`
- `runtime_diagnostics_mixin.py:59,64,75,88,116,131,162,327,428` -- 9 instances of `cast("DataFusionRuntimeProfile", self)` in a single mixin
- `runtime_ops.py:428,471` -- 2 more instances of the same cast pattern in the ops mixins
- The mixins cannot be tested independently because they require being mixed into `DataFusionRuntimeProfile` to function

**Suggested improvement:**
Replace mixins with composed helper objects that receive the profile as an explicit parameter. The existing `RuntimeProfileDeltaOps`, `RuntimeProfileIO`, and `RuntimeProfileCatalog` at `runtime_ops.py:91-483` already follow this better pattern. Apply the same approach to `_RuntimeDiagnosticsMixin` by extracting it into a `RuntimeProfileDiagnostics(profile)` composition class.

**Effort:** medium
**Risk if unaddressed:** medium -- The cast pattern silently breaks if a mixin method is accidentally used on a non-profile instance

---

#### P13. Prefer composition over inheritance -- Alignment: 1/3

**Current state:**
The 4-mixin + base class inheritance chain is the primary composition mechanism for `DataFusionRuntimeProfile`. This is inheritance-heavy design.

**Findings:**
- `runtime.py:331-338` -- 5-deep inheritance chain: `_RuntimeProfileIOFacadeMixin` > `_RuntimeProfileCatalogFacadeMixin` > `_RuntimeProfileDeltaFacadeMixin` > `_RuntimeDiagnosticsMixin` > `StructBaseStrict`
- The mixins add methods that conceptually belong to separate concerns (I/O, catalog, Delta, diagnostics) but they all end up on the same object surface, inflating the class API
- `runtime.py:372-403` -- Properties `delta_ops`, `io_ops`, `catalog_ops` show the transition toward composition is underway but incomplete (these return composed helper objects)

**Suggested improvement:**
Complete the transition from mixins to composed helpers. The `delta_ops`, `io_ops`, `catalog_ops` pattern should be extended to diagnostics (replacing `_RuntimeDiagnosticsMixin`). Eventually, the profile class should have no mixin bases and instead provide access to capability objects via properties.

**Effort:** medium
**Risk if unaddressed:** medium -- The mixin surface adds ~40 methods to `DataFusionRuntimeProfile`, making it a "God class" by method count

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
Multi-hop attribute chains are common throughout the session package.

**Findings:**
- `runtime_diagnostics_mixin.py:93-94` -- `profile.policies.cache_policy` and then `cache_policy_settings(profile.policies.cache_policy)` -- accessing nested policy attributes
- `runtime_extensions.py:73-75` -- `profile.policies.diskcache_profile` accessed, then `diskcache_profile.ttl_for("schema")` -- 3-hop chain
- `introspection.py:55-57` -- `profile.policies.diskcache_profile` -> `cache_for_kind(cache_profile, "schema")` -> `cache_profile.ttl_for("schema")` -- same 3-hop pattern repeated
- `runtime_diagnostics_mixin.py:174-194` -- `profile.data_sources.dataset_templates.items()` -> `location.resolved.datafusion_scan` -> 12+ attribute accesses on the scan object
- `runtime.py:734-742` -- `self.zero_row_bootstrap.include_semantic_outputs`, `self.zero_row_bootstrap.include_internal_tables`, etc. -- repeated traversal of nested config

**Suggested improvement:**
Add convenience methods on `PolicyBundleConfig` like `schema_cache()` and `schema_cache_ttl()` that encapsulate the `diskcache_profile` traversal. For the `zero_row_bootstrap` pattern, convert `ZeroRowBootstrapConfig` to a method that produces a `ZeroRowBootstrapRequest` directly.

**Effort:** medium
**Risk if unaddressed:** medium -- Internal restructuring of `PolicyBundleConfig` would require updating 6+ call sites

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The existing `RuntimeProfileDeltaOps`, `RuntimeProfileIO`, and `RuntimeProfileCatalog` helper classes at `runtime_ops.py` demonstrate the "tell" pattern well -- they encapsulate operations behind meaningful method names. However, the mixin methods and standalone functions still "ask" the profile for raw config attributes.

**Findings:**
- `runtime_ops.py:91-117` -- `RuntimeProfileDeltaOps` encapsulates Delta operations behind `delta_runtime_ctx()` and `delta_service()` -- good "tell" pattern
- `runtime_diagnostics_mixin.py:80-106` -- `settings_payload()` queries 6 different profile sub-objects to assemble a settings dict -- this is "ask the profile for everything, then assemble externally"
- `runtime_compile.py:55-74` -- `record_artifact()` takes a profile and delegates to `_lineage_record_artifact()` -- proper "tell" delegation

**Suggested improvement:**
Move `settings_payload()` assembly logic into `PolicyBundleConfig` where the policy objects live, rather than reaching into the profile from a mixin.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Config structs (`RuntimeProfileConfig`, `ExecutionConfig`, `CatalogConfig`, `PolicyBundleConfig`, etc.) form a clean functional core -- they are frozen, deterministic, and side-effect-free. Session construction (`session_context()`) is the imperative shell. The boundary between them is reasonably clear.

**Findings:**
- `runtime_profile_config.py` (661 LOC) -- Pure data definitions with fingerprint methods. No side effects. Good functional core.
- `runtime.py:550-584` -- `session_context()` is properly imperative: it creates, configures, registers, installs, and caches. All side effects are concentrated here.
- `runtime_telemetry.py` -- Mixed: some functions are pure payload builders (good), others access `os.environ` at runtime (minor impurity)

**Suggested improvement:**
Move `os.environ` access in `runtime_telemetry.py` to initialization time rather than runtime access, to keep telemetry functions pure.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
Session caching via `_SESSION_CONTEXT_CACHE` makes `session_context()` idempotent for the same profile. `_SESSION_RUNTIME_CACHE` does the same for `session_runtime()`.

**Findings:**
- `runtime.py:1129-1137` -- `_cached_context()` / `_cache_context()` provide idempotent session context access
- `runtime_session.py:244,274` -- `_SESSION_RUNTIME_CACHE` provides idempotent runtime access
- `bootstrap/zero_row.py` -- `ZeroRowBootstrapMode` supports `strict_zero_rows` for idempotent bootstrap

**Suggested improvement:**
No changes needed.

**Effort:** small
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 2/3

**Current state:**
`session_runtime_hash` at `runtime_session.py` captures UDF snapshot hash and DataFusion settings snapshot to ensure reproducibility. Feature state snapshots include determinism tier.

**Findings:**
- `runtime_session.py:99-130` -- `_build_session_runtime_from_context` captures `udf_snapshot_hash`, `rewrite_tags`, `planner_names`, and `df_settings` for deterministic replay
- `features.py:27-31` -- `FeatureStateSnapshot` includes `determinism_tier` from `core_types.DeterminismTier`
- Minor gap: The dual `_RUNTIME_SESSION_ID` at `runtime_session.py:33` and `runtime_hooks.py:50` generates two different UUIDs, potentially causing non-deterministic diagnostics

**Suggested improvement:**
Ensure `_RUNTIME_SESSION_ID` is generated once and shared across modules.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 1/3

**Current state:**
The package has 33 files for the session subsystem. Several are trivially thin:

**Findings:**
- `api.py` (13 LOC) -- Imports and re-exports 3 names from other modules. No logic.
- `lifecycle.py` (32 LOC) -- Two functions that each call a single method on the profile.
- `diagnostics.py` (34 LOC) -- Two functions that each call a single function from `runtime.py`.
- `profiles.py` (49 LOC) -- Two functions that wrap single method calls on profile/runtime.
- `config.py` (31 LOC) -- Re-exports from `config_structs` and `contracts`.
- These 5 files total ~160 LOC and add 5 import targets without adding any abstraction.

**Suggested improvement:**
Merge `api.py`, `lifecycle.py`, `diagnostics.py`, `profiles.py`, and `config.py` into `__init__.py` as the public API surface, or simply remove them and have callers import from the actual source modules. This reduces the file count by 5 without losing any functionality.

**Effort:** medium
**Risk if unaddressed:** medium -- New developers must navigate 33 files to understand session management; most files are just import indirection

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Feature gates in `FeatureGatesConfig` exist and are actively used. No speculative abstraction layers detected.

**Findings:**
- `runtime_profile_config.py` -- Feature gates like `enable_url_table`, `enable_function_factory`, `enable_cache_manager` are all referenced in session construction logic
- `streaming.py` (323 LOC) -- `StreamingExecutionResult` provides concrete streaming capabilities that are used in the pipeline

**Suggested improvement:**
No changes needed.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
Several naming and structural choices are surprising.

**Findings:**
- `runtime.py` re-exports private `_`-prefixed names like `_introspection_cache_for_ctx`, `_effective_catalog_autoload_for_profile` -- consumers importing from `runtime.py` can access "private" internals of sub-modules
- `runtime.py:263-264` -- `_identifier_normalization_mode = _telemetry_identifier_normalization_mode` / `_effective_ident_normalization = _telemetry_effective_ident_normalization` -- aliasing private names from telemetry module for "mixin compatibility" is surprising
- `_SESSION_CONTEXT_CACHE` defined in both `runtime.py:302` and `runtime_session.py:51` -- same name, different objects, different modules
- `_RUNTIME_SESSION_ID` defined in both `runtime_session.py:33` and `runtime_hooks.py:50` -- same name, different UUIDs generated at import time
- `runtime_diagnostics_mixin.py:59` -- Using `cast("DataFusionRuntimeProfile", self)` in a mixin is surprising; a reader expects `self` to already be the correct type without casting

**Suggested improvement:**
Stop re-exporting private `_`-prefixed names through `runtime.py`. Consolidate `_SESSION_CONTEXT_CACHE` and `_RUNTIME_SESSION_ID` to single authoritative locations. Replace `cast` patterns with explicit parameter passing.

**Effort:** medium
**Risk if unaddressed:** medium -- Surprising behavior increases debugging time and maintenance risk

---

#### P22. Declare and version public contracts -- Alignment: 1/3

**Current state:**
The `__all__` list in `runtime.py` contains 115 entries mixing fundamentally different categories: configuration types, hook type aliases, standalone functions, constants, and internal helpers.

**Findings:**
- `runtime.py:1162-1277` -- 115 entries in `__all__` without clear category boundaries; comments attempt categorization but the list is still a flat namespace
- `__init__.py:13-17` -- Package `__all__` correctly exports only 3 names (`DataFusionExecutionFacade`, `DataFusionRuntimeProfile`, `RuntimeProfileConfig`), showing what the public API should look like
- `contracts.py:26` -- Clean, minimal `__all__` with only 2 entries
- No versioning of the public API surface; no deprecation markers on re-exported names

**Suggested improvement:**
Use the `__init__.py` pattern as the model: the public API of the session package should be `DataFusionRuntimeProfile`, `RuntimeProfileConfig`, `DataFusionExecutionFacade`, `SessionRuntime`, and the protocol types. Everything else should be imported from specific sub-modules by callers who need it.

**Effort:** medium
**Risk if unaddressed:** medium -- Without a clear public/private boundary, all 115 names are implicitly stable contracts

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
Module-level mutable state makes test isolation difficult. The mixin pattern requires constructing full `DataFusionRuntimeProfile` instances to test any mixin method.

**Findings:**
- `runtime.py:302` -- `_SESSION_CONTEXT_CACHE: dict[str, SessionContext] = {}` is module-level mutable state; tests must manually clear it or risk cross-test contamination
- `runtime_session.py:49-51` -- Three module-level mutable dicts: `_SESSION_RUNTIME_CACHE`, `_RUNTIME_SETTINGS_OVERLAY`, `_SESSION_CONTEXT_CACHE`
- `runtime_diagnostics_mixin.py` -- Cannot test any mixin method without constructing a full `DataFusionRuntimeProfile`; every method starts with `cast("DataFusionRuntimeProfile", self)`
- `runtime.py:550-584` -- `session_context()` creates real DataFusion sessions, registers real filesystems, installs real UDFs -- no seam for test injection
- The `SessionFactory` at `context_pool.py` is injectable, but `session_context()` hardcodes `SessionFactory(self).build()`

**Suggested improvement:**
Replace module-level caches with a `SessionCacheManager` class that can be injected and cleared. Make `session_context()` accept an optional `SessionFactory` parameter for test injection. Convert mixin methods to standalone functions that accept the profile as a parameter, enabling unit testing without constructing the full class.

**Effort:** medium
**Risk if unaddressed:** medium -- Test isolation issues lead to flaky tests and expensive test setup

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
The observability infrastructure is thorough. `runtime_telemetry.py` (828 LOC), `runtime_hooks.py` (423 LOC), and `runtime_diagnostics_mixin.py` (434 LOC) provide structured telemetry payloads, hook chaining, artifact recording, and diagnostics snapshots. The `facade.py` uses OpenTelemetry spans.

**Findings:**
- `facade.py:36-37` -- OpenTelemetry integration with `record_datafusion_duration`, `record_error`, `record_write_duration`
- `runtime_hooks.py:43-48` -- Typed hook aliases (`ExplainHook`, `PlanArtifactsHook`, etc.) provide structured observability extension points
- `runtime_telemetry.py:50-51` -- `SETTINGS_HASH_VERSION` and `TELEMETRY_PAYLOAD_VERSION` versioned telemetry payloads
- `diagnostics.py` at root level re-exports diagnostics functions but adds no observability value

**Suggested improvement:**
Consolidate logging to use a single logger per module (most files already do this correctly). Consider adding a telemetry contract version check to catch payload schema drift.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: The Re-Export Hub Anti-Pattern

**Root cause:** `runtime.py` was the original monolithic implementation. When it was decomposed into 12 sub-modules, all imports were preserved as re-exports to maintain backward compatibility. The result is that `runtime.py` is a 1,293-line file that is almost entirely import statements, acting as a "namespace namespace" rather than a proper module.

**Affected principles:** P1 (information hiding), P4 (coupling), P19 (KISS), P21 (astonishment), P22 (public contracts)

**Suggested approach:** Deprecate `from datafusion_engine.session.runtime import X` patterns and migrate callers to import from specific sub-modules. This can be done incrementally by adding deprecation warnings to the `__getattr__` of runtime.py.

### Theme 2: Duplicated Constants and Helpers

**Root cause:** When the monolithic `runtime.py` was split, some constants and helper functions were copied into multiple sub-modules rather than being extracted to a shared location. This appears to have been done to avoid circular imports.

**Affected principles:** P7 (DRY) -- the most severe violation in the review

**Suggested approach:** Create `session/_constants.py` (for `KIB`, `MIB`, `GIB`, `CACHE_PROFILES`, `_DATAFUSION_SQL_ERROR`, `_EXTENSION_MODULE_NAMES`, `_parse_major_version`) and `session/_introspection_factory.py` (for `_create_schema_introspector`). These leaf modules can be imported by all sub-modules without creating circular dependencies.

### Theme 3: Mixin-Cast Composition Pattern

**Root cause:** `DataFusionRuntimeProfile` was too large for a single file, so behavior was extracted into mixins. The mixins need the profile's full API surface, so they use `cast("DataFusionRuntimeProfile", self)` to recover the concrete type.

**Affected principles:** P12 (DI), P13 (composition over inheritance), P21 (astonishment), P23 (testability)

**Suggested approach:** Replace mixins with composed helper objects. The existing `RuntimeProfileDeltaOps(profile)` pattern at `runtime_ops.py:91` is the correct target pattern. Apply it to `_RuntimeDiagnosticsMixin` -> `RuntimeProfileDiagnostics(profile)`.

### Theme 4: Scattered Module-Level Mutable State

**Root cause:** Session caching (`_SESSION_CONTEXT_CACHE`, `_SESSION_RUNTIME_CACHE`) and settings tracking (`_RUNTIME_SETTINGS_OVERLAY`) are implemented as module-level dicts rather than being centralized in a managed cache object.

**Affected principles:** P23 (testability), P18 (determinism), P21 (astonishment)

**Suggested approach:** Consolidate all session caches into a `SessionStateManager` class (similar to `RuntimeStateRegistry` at `runtime_state.py:13-30`, which already exists in the root package). Make the state manager injectable for test control.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Create `session/_constants.py` to deduplicate `KIB/MIB/GIB`, `CACHE_PROFILES`, `_DATAFUSION_SQL_ERROR`, `_parse_major_version`, `_EXTENSION_MODULE_NAMES` (10 duplications across 8 files) | small | Eliminates most severe DRY violations and potential for constant drift |
| 2 | P7 (DRY) | Consolidate `_RUNTIME_SESSION_ID` to single authoritative location -- currently two independent UUIDs with the same name at `runtime_session.py:33` and `runtime_hooks.py:50` | small | Fixes latent diagnostics correctness issue where hooks and sessions record different IDs |
| 3 | P7 (DRY) | Remove shadow `_SESSION_CONTEXT_CACHE` at `runtime_session.py:51` (defined but unused; the real one is at `runtime.py:302`) | small | Eliminates confusion and prevents accidental use of wrong cache |
| 4 | P19 (KISS) | Merge trivial facade files (`api.py`, `lifecycle.py`, `diagnostics.py`, `profiles.py`, `config.py` -- totaling ~160 LOC) into `__init__.py` or delete them | small | Reduces file count by 5, removes import indirection with no logic |
| 5 | P11 (CQS) | Rename `session_context()` to `ensure_session_context()` to signal that it creates-or-returns rather than being a pure accessor | small | Reduces surprise for callers who don't expect side effects from a getter-like name |

## Recommended Action Sequence

1. **Create `session/_constants.py`** (Quick Win 1). Extract all duplicated constants. No callers change; only internal imports update. Validates the approach with zero external risk. [P7]

2. **Consolidate `_RUNTIME_SESSION_ID`** (Quick Win 2). Choose `runtime_hooks.py` as the authoritative location (it uses the ID more). Have `runtime_session.py` import from there. [P7]

3. **Remove shadow `_SESSION_CONTEXT_CACHE` from `runtime_session.py`** (Quick Win 3). The real cache is in `runtime.py`. [P7, P21]

4. **Merge trivial facade files** (Quick Win 4). Reduce cognitive overhead of navigating 33 files. [P19]

5. **Replace `_RuntimeDiagnosticsMixin` with `RuntimeProfileDiagnostics(profile)`** composition class. Follow the existing `RuntimeProfileDeltaOps` pattern. This eliminates 9 `cast("DataFusionRuntimeProfile", self)` calls and makes diagnostics independently testable. [P12, P13, P23]

6. **Reduce `runtime.py` re-exports**. Start migrating callers from `from runtime import X` to import from specific sub-modules. Begin with the 7 intra-package callers (`api.py`, `config_structs.py`, `diagnostics.py`, `facade.py`, `lifecycle.py`, `profiles.py`, `registration.py`). [P1, P4, P22]

7. **Split the three largest files**. `runtime_schema_registry.py` (1,579 LOC), `runtime_extensions.py` (1,432 LOC), and `runtime_dataset_io.py` (890 LOC) each handle 4-5 distinct concerns. Split along responsibility boundaries identified in P3 findings. [P3, P19]

8. **Consolidate module-level mutable state** into a `SessionStateManager` injectable class. Replace `_SESSION_CONTEXT_CACHE`, `_SESSION_RUNTIME_CACHE`, and `_RUNTIME_SETTINGS_OVERLAY` with a centralized, clearable cache. [P23, P18]
