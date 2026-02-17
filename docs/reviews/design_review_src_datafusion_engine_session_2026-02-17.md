# Design Review: src/datafusion_engine/session + extensions + bootstrap + top-level

**Date:** 2026-02-17
**Scope:** `src/datafusion_engine/session/`, `src/datafusion_engine/extensions/`, `src/datafusion_engine/bootstrap/`, `src/datafusion_engine/*.py`
**Focus:** All principles (1-24) with emphasis on boundaries (1-6) and composition (12-15)
**Depth:** deep
**Files reviewed:** 54

## Executive Summary

The DataFusion session/runtime module has undergone significant architectural improvement since its prior state. The three previously-reported known issues -- split-cache bug, duplicated `_RUNTIME_SESSION_ID`, and the 1292-LOC re-export hub -- are all **resolved**. The codebase demonstrates strong separation of operational concerns across well-named submodules (`runtime_ops.py`, `runtime_compile.py`, `runtime_telemetry.py`, `runtime_extensions.py`, etc.) and clean dependency direction with no outer-ring imports.

However, three systemic design gaps remain. First, **DRY violations** across four duplicated helper functions (`_identifier_normalization_mode`, `_catalog_autoload_settings`, `_effective_catalog_autoload_for_profile`, and `ExplainRows` type alias defined in 5 files). Second, the **6-mixin inheritance hierarchy** on `DataFusionRuntimeProfile` combined with 33 occurrences of `self: Any` typing in `_RuntimeContextMixin` undermines type safety. Third, **hidden `DataFusionRuntimeProfile()` construction** in 7+ helper functions creates invisible coupling that violates dependency inversion.

## Known Issues Verification

| Issue | Status | Evidence |
|-------|--------|----------|
| Split-cache bug (`_SESSION_CONTEXT_CACHE` defined twice) | **RESOLVED** | Single definition at `_session_caches.py:9` |
| Duplicated `_RUNTIME_SESSION_ID` | **RESOLVED** | Single definition at `_session_identity.py:12` as `RUNTIME_SESSION_ID` |
| 1292-LOC re-export hub with 102-entry `__all__` | **RESOLVED** | `runtime.py` is 660 LOC with `__all__ = ["DataFusionRuntimeProfile"]`; `__init__.py` is 318 LOC with 47 lazy exports |

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | medium | medium | Mixin `self: Any` bypasses type-safe interface contracts |
| 2 | Separation of concerns | 2 | medium | medium | `_RuntimeContextMixin` aggregates 5+ distinct concerns |
| 3 | SRP (one reason to change) | 2 | medium | low | `PolicyBundleConfig` (53+ fields) changes for many reasons |
| 4 | High cohesion, low coupling | 2 | medium | medium | 6-mixin hierarchy couples profile to all operational concerns |
| 5 | Dependency direction | 2 | medium | medium | Hidden `DataFusionRuntimeProfile()` construction in helpers |
| 6 | Ports & Adapters | 2 | small | low | `DeltaServicePort` protocol used well; extension module resolution still uses `getattr`/`importlib` |
| 7 | DRY (knowledge, not lines) | 1 | small | medium | 4 helper functions duplicated across modules; `ExplainRows` in 5 files |
| 8 | Design by contract | 2 | small | low | Protocol types defined but underutilized |
| 9 | Parse, don't validate | 2 | small | low | `parse_runtime_size` does parse-not-validate well |
| 10 | Make illegal states unrepresentable | 2 | medium | low | `PolicyBundleConfig` allows contradictory field combinations |
| 11 | CQS | 2 | small | low | `session_context()` both builds/caches and returns |
| 12 | Dependency inversion + explicit composition | 1 | medium | high | 7+ hidden default profile constructions |
| 13 | Prefer composition over inheritance | 1 | large | medium | 6-mixin hierarchy; `self: Any` in 33 locations |
| 14 | Law of Demeter | 2 | small | low | `self.profile.diagnostics.diagnostics_sink` chains |
| 15 | Tell, don't ask | 2 | small | low | `facade.py` queries `self.runtime_profile is None` repeatedly |
| 16 | Functional core, imperative shell | 2 | medium | low | Pure config structs at center; IO at edges |
| 17 | Idempotency | 3 | - | - | `session_context()` caching is idempotent; Delta commit versioning |
| 18 | Determinism / reproducibility | 3 | - | - | Fingerprinting throughout; deterministic config payloads |
| 19 | KISS | 2 | medium | low | `compile_options_for_profile` is 140 LOC with 21-element unchanged tuple |
| 20 | YAGNI | 3 | - | - | No speculative abstractions observed |
| 21 | Least astonishment | 2 | small | low | `lifecycle.py` re-delegates trivially; `profiles.py` ignores `use_cache` parameter |
| 22 | Declare and version public contracts | 2 | small | low | `__all__` well-maintained; some facade mixins are public but undocumented |
| 23 | Design for testability | 2 | medium | medium | Hidden profile construction defeats DI in tests |
| 24 | Observability | 3 | - | - | Comprehensive diagnostics, telemetry, artifact recording |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The module successfully hides internal implementation details behind well-defined `__all__` exports. `runtime.py` exports only `DataFusionRuntimeProfile`. However, the mixin pattern using `self: Any` typing effectively removes compile-time interface enforcement.

**Findings:**
- `runtime_context.py:49-416` -- `_RuntimeContextMixin` uses `self: Any` 33 times across all methods. This means any attribute access on `self` is unchecked, allowing silent breakage when profile fields change. For example, `self.policies.cache_output_root` at line 255 has zero type-checker coverage.
- `runtime_diagnostics_mixin.py:88-432` -- `_RuntimeDiagnosticsMixin` also uses `self: Any`, making the diagnostics interface invisible to static analysis.
- `runtime_ops.py:456-507` -- Facade mixins (`_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`, `_RuntimeProfileDeltaFacadeMixin`) have minimal surface but still bypass type checking.

**Suggested improvement:**
Define a `RuntimeProfileProtocol` that declares the fields each mixin requires (`policies`, `features`, `diagnostics`, `execution`, `catalog`, etc.) and type the `self` parameter against that protocol. This preserves mixin flexibility while restoring static analysis coverage.

**Effort:** medium
**Risk if unaddressed:** medium -- silent attribute access errors only caught at runtime.

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
Good separation at the file level: config, context, ops, compile, telemetry, hooks, extensions. However, `_RuntimeContextMixin` in `runtime_context.py` aggregates 5+ distinct concerns.

**Findings:**
- `runtime_context.py:49-416` -- This single mixin handles: (1) session context building/caching, (2) filesystem registration, (3) catalog/schema installation, (4) UDF management, (5) SQL options, (6) diskcache access, (7) schema introspection, (8) view definition recording. Each concern would benefit from its own module.
- `runtime_extensions.py` (1414 LOC) -- Handles both extension installation AND validation AND diagnostics recording. The 15 `_defer_import_*` functions (e.g., lines 72-143) are a cross-cutting pattern that could be factored.

**Suggested improvement:**
Split `_RuntimeContextMixin` into focused mixins or, better, delegate to separate helper objects. For example: `_ContextCacheMixin` (build/cache logic), `_CatalogSetupMixin` (installation), `_UdfAccessMixin` (UDF catalog queries). The `_defer_import_*` pattern in `runtime_extensions.py` could use a generic deferred-spec factory.

**Effort:** medium
**Risk if unaddressed:** medium -- changes to one concern (e.g., SQL options) risk breaking unrelated behavior (e.g., diskcache access).

---

#### P3. SRP (one reason to change) -- Alignment: 2/3

**Current state:**
Most files have a clear single responsibility. The primary exception is `PolicyBundleConfig`.

**Findings:**
- `runtime_profile_config.py:260-460` -- `PolicyBundleConfig` has 53+ fields spanning cache policy, delta store policy, scan policy, diskcache profile, join policy, SQL policy, filesystem roots, plugin configs, and more. This struct changes whenever any policy subsystem changes. Its `fingerprint_payload()` method is 100+ lines.
- `runtime_compile_options.py:166-308` -- `compile_options_for_profile()` is 140 LOC merging 21+ option fields, functioning as a policy merger with concern spanning compile hooks, SQL policy, and prepared statements.

**Suggested improvement:**
Decompose `PolicyBundleConfig` into 4-5 focused config groups: `CachePolicy`, `DeltaStorePolicy`, `SqlPolicy`, `FileSystemPolicy`, `PluginPolicy`. The `PolicyBundleConfig` becomes a composition root that delegates `fingerprint_payload()` to each sub-policy.

**Effort:** medium
**Risk if unaddressed:** low -- the current struct works but makes it harder to reason about which fields affect which subsystem.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The 6-mixin hierarchy on `DataFusionRuntimeProfile` creates tight coupling between all operational concerns.

**Findings:**
- `runtime.py:145-158` -- `DataFusionRuntimeProfile` inherits from `_RuntimeProfileIdentityMixin`, `_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`, `_RuntimeProfileDeltaFacadeMixin`, `_RuntimeDiagnosticsMixin`, `_RuntimeContextMixin`, and `StructBaseStrict`. Every mixin has implicit access to every field.
- `runtime_ops.py:116-139` -- `RuntimeProfileDeltaOps` stores a back-reference to `profile`, then reaches through it to access `profile.diagnostics.diagnostics_sink`, `profile.delta_commit_runs`, etc. High fan-in on the profile object.

**Suggested improvement:**
Replace inheritance-based mixins with explicit delegation objects that are injected with only the fields they need. For instance, `RuntimeProfileDeltaOps` already works this way but receives the full profile; it could instead receive only `delta_commit_runs`, `diagnostics_sink`, and `delta_service`.

**Effort:** medium
**Risk if unaddressed:** medium -- adding a field to the profile struct can break any mixin unexpectedly.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
No outer-ring imports (`cli`, `graph`) from the session module -- good. However, helper functions create hidden upward dependencies.

**Findings:**
- `runtime_dataset_io.py:170,279,335,407` -- Four functions instantiate `DataFusionRuntimeProfile()` as a default, creating a hidden dependency from leaf I/O helpers back to the central profile class. This is a dependency direction violation: low-level I/O utilities depend on the high-level composition root.
- `kernels.py:115-123` -- `_session_context()` creates `DataFusionRuntimeProfile()` when no profile is provided. Same hidden upward dependency.
- `encoding/policy.py:144` -- Another hidden default construction.
- `udf/platform.py:16` -- Yet another hidden default construction.
- `schema/validation.py:111` -- Uses `DataFusionRuntimeProfile()` as a fallback.

**Suggested improvement:**
Remove all hidden `DataFusionRuntimeProfile()` default constructions. Instead, make `runtime_profile` a required parameter (or use a dedicated `SessionContext` factory function that doesn't depend on the full profile). The profile should only be constructed at composition roots.

**Effort:** medium
**Risk if unaddressed:** medium -- these hidden constructions make it impossible to test leaf functions in isolation and create circular dependency risk.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The `DeltaServicePort` protocol in `delta/service_protocol.py` is a well-designed port with adapter binding via `bind_delta_service()`. Extension module resolution is functional but relies on `getattr`/`importlib` patterns.

**Findings:**
- `runtime_ops.py:41-61` -- `bind_delta_service()` and `_resolve_delta_service()` implement a clean port/adapter pattern via `WeakKeyDictionary`.
- `extensions/datafusion_ext.py:1-145` -- The extension wrapper normalizes the Rust FFI boundary well, with `_normalize_ctx` handling the SessionContext ABI mismatch. No-op shims provide graceful degradation.
- `extensions/context_adaptation.py:1-153` -- Extension module resolution uses `resolve_extension_module()` with capability probing. Well-structured but the `required_attr` / `entrypoint` parameters could be typed more precisely.

**Suggested improvement:**
Consider defining an `ExtensionCapability` enum or typed protocol for extension probing instead of string-based `required_attr` checks, which would make the port contract more explicit.

**Effort:** small
**Risk if unaddressed:** low -- current approach works but is stringly-typed.

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
Multiple helper functions implementing the same business logic are duplicated across modules. This is not just code duplication but **knowledge duplication** -- each copy embodies the same policy decision.

**Findings:**
- `_identifier_normalization_mode` is defined identically at `runtime_compile.py:263` and `runtime_telemetry.py:609`. Both compute the same normalization mode from the profile's catalog configuration. The `runtime_diagnostics_mixin.py:34` imports from `runtime_compile.py`, proving one authoritative source is possible.
- `_catalog_autoload_settings` is defined identically at `runtime_config_policies.py:54` and `runtime_telemetry.py:370`. Both read the same environment variables (`DATAFUSION_CATALOG_LOCATION`, `DATAFUSION_CATALOG_FORMAT`).
- `_effective_catalog_autoload_for_profile` is defined at `runtime_config_policies.py:485` and `runtime_telemetry.py:381`. The telemetry version reimplements the same logic.
- `ExplainRows` type alias is defined in 5 files: `runtime_profile_config.py:47`, `runtime_hooks.py:39`, `runtime.py:127`, `runtime_compile.py:38`, and `compile/options.py:21`. Each uses the same `TableLike | RecordBatchReaderLike` conditional pattern.

**Suggested improvement:**
1. Consolidate `_identifier_normalization_mode` into `runtime_compile.py` (already authoritative) and import in `runtime_telemetry.py`.
2. Consolidate `_catalog_autoload_settings` and `_effective_catalog_autoload_for_profile` into `runtime_config_policies.py` and import elsewhere.
3. Define `ExplainRows` once in a shared `types.py` module and import everywhere.

**Effort:** small
**Risk if unaddressed:** medium -- when a policy changes, only some copies get updated, causing silent divergence.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Protocol types exist (`RuntimeSettingsProvider`, `RuntimeTelemetryProvider`, `RuntimeArtifactRecorder` in `protocols.py`) but are underutilized. Function docstrings include Parameters/Returns/Raises sections consistently.

**Findings:**
- `protocols.py:1-45` -- Three `@runtime_checkable` protocols define clear contracts but are not referenced by the profile or facade classes. They could serve as the protocol base for the mixin `self` types.
- `cache_policy.py:12-88` -- `CachePolicyConfig` with `fingerprint_payload()` demonstrates good contract discipline.

**Suggested improvement:**
Use the existing protocol types in `protocols.py` as the `self` type for mixins, rather than `Any`. This would enforce the contract at definition time.

**Effort:** small
**Risk if unaddressed:** low -- the protocols exist and are correct; they just need to be referenced.

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Configuration parsing generally follows parse-don't-validate. `delta_session_builder.py` demonstrates this well.

**Findings:**
- `delta_session_builder.py:84-99` -- `parse_runtime_size()` converts string inputs to `int | None` at the boundary, rejecting invalid inputs cleanly.
- `runtime_profile_config.py` -- Frozen structs enforce validity at construction time. Good alignment.
- `context_pool.py:55-175` -- `SessionFactory.build_config()` applies settings sequentially but validates them implicitly through DataFusion's SessionConfig API. No explicit validation before setting.

**Suggested improvement:**
Add a `validate()` classmethod on `RuntimeProfileConfig` that checks cross-field invariants at construction time, rather than discovering them at session build time.

**Effort:** small
**Risk if unaddressed:** low -- invalid configs surface during session construction, which is acceptable.

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Frozen structs prevent mutation. However, some config types allow contradictory states.

**Findings:**
- `runtime_profile_config.py:260-460` -- `PolicyBundleConfig` allows setting both `sql_policy` and `sql_policy_name`, which creates ambiguity about which takes precedence. The resolution logic is in `runtime_context.py:199-211`.
- `runtime_profile_config.py:120-148` -- `FeatureGatesConfig` has 18 boolean flags. Some combinations are meaningless (e.g., `enable_async_udfs=True` with `enable_function_factory=False`).

**Suggested improvement:**
Use `__post_init__` validation or a factory method on `PolicyBundleConfig` to reject contradictory states. For `FeatureGatesConfig`, document which flag combinations are valid.

**Effort:** medium
**Risk if unaddressed:** low -- the resolution logic handles ambiguity, but it is not obvious to callers.

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. The primary exception is `session_context()`.

**Findings:**
- `runtime.py:280-310` -- `session_context()` both builds/caches a SessionContext (command) and returns it (query). This combines a side effect (caching, registration phase execution) with a return value.
- `runtime_context.py:386-389` -- `_cache_context()` is a pure command (good).
- `runtime_context.py:381-384` -- `_cached_context()` is a pure query (good).

**Suggested improvement:**
Split `session_context()` into `ensure_session_context()` (command that builds/caches) and `session_context()` (query that returns the cached value or raises). The current implementation is pragmatic but conflates the two operations.

**Effort:** small
**Risk if unaddressed:** low -- the current API is idiomatic and well-understood.

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 1/3

**Current state:**
The most significant design gap. Seven or more helper functions create default `DataFusionRuntimeProfile()` instances internally, hiding the dependency from callers.

**Findings:**
- `runtime_dataset_io.py:170` -- `align_table_to_schema()` creates `DataFusionRuntimeProfile().session_context()` as a default `ctx`.
- `runtime_dataset_io.py:279` -- `dataset_schema_from_context()` creates a default profile.
- `runtime_dataset_io.py:335` -- `read_delta_as_reader()` creates a default profile.
- `runtime_dataset_io.py:407` -- `_datafusion_type_name()` creates a default profile to get an ephemeral context.
- `kernels.py:115-123` -- `_session_context()` creates a default profile when `runtime_profile is None`.
- `encoding/policy.py:144` -- Creates a default profile.
- `udf/platform.py:16` -- Creates a default profile for `session_context()`.

These hidden constructions mean:
1. Unit tests cannot inject mock profiles.
2. Each construction initializes a full DataFusion SessionContext, which is expensive.
3. The dependency graph has invisible edges.

**Suggested improvement:**
Make `runtime_profile` or `ctx: SessionContext` a required parameter in all helper functions. For convenience, provide a module-level `default_session_context()` function that is explicitly called by composition roots, not hidden inside leaf helpers.

**Effort:** medium
**Risk if unaddressed:** high -- untestable code paths, expensive hidden initialization, and circular dependency risk.

---

#### P13. Prefer composition over inheritance -- Alignment: 1/3

**Current state:**
`DataFusionRuntimeProfile` relies on 6-mixin inheritance hierarchy. The `self: Any` pattern in mixins defeats the purpose of type-safe composition.

**Findings:**
- `runtime.py:145-158` -- Six mixin bases plus `StructBaseStrict`. The MRO is complex and hard to reason about.
- `runtime_context.py:49-416` -- `_RuntimeContextMixin` uses `self: Any` 33 times. Every method accesses arbitrary attributes on `self` without type-checking.
- `runtime_diagnostics_mixin.py:88-432` -- `_RuntimeDiagnosticsMixin` uses the same `self: Any` pattern extensively.
- `runtime_ops.py:440-453` -- `_runtime_profile_io_ops()` and `_runtime_profile_delta_ops()` use `getattr()` to discover operations on arbitrary objects.

**Suggested improvement:**
Transition from inheritance-based mixins to explicit delegation. `DataFusionRuntimeProfile` would own `self.context_ops: RuntimeContextOps`, `self.diagnostics_ops: RuntimeDiagnosticsOps`, etc. Each ops object receives only the config structs it needs (not the full profile). This eliminates `self: Any` and makes dependencies explicit.

**Effort:** large
**Risk if unaddressed:** medium -- the current approach works but undermines the value of static type checking across the entire session module.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Moderate violations through configuration chain traversals.

**Findings:**
- `runtime_ops.py:155-191` -- `reserve_delta_commit()` accesses `self.profile.delta_commit_runs`, `self.profile.diagnostics.diagnostics_sink`, and `self.profile.record_artifact()` -- 2-3 levels of navigation.
- `facade.py` -- Throughout, methods access `self.runtime_profile.policies.X`, `self.runtime_profile.features.Y`, `self.runtime_profile.diagnostics.Z` -- consistent 3-level chains.
- `runtime_compile_options.py:201-234` -- `compile_options_for_profile()` reaches into `profile.diagnostics.capture_explain`, `profile.diagnostics.explain_collector.hook`, `profile.policies.cache_max_columns` etc.

**Suggested improvement:**
Introduce convenience query methods on the profile (e.g., `profile.should_capture_explain()`) that encapsulate the chain traversals. The existing facade mixins do this partially but incompletely.

**Effort:** small
**Risk if unaddressed:** low -- the chains are consistently structured and well-understood within the codebase.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The facade pattern in `facade.py` mostly delegates, but repeatedly queries `self.runtime_profile is None` before delegating.

**Findings:**
- `facade.py` -- The pattern `if self.runtime_profile is None: <default behavior>; else: <profile behavior>` appears 8+ times. The facade should encapsulate this distinction.
- `profiles.py:18` -- `runtime_for_profile()` ignores the `use_cache` parameter (`_ = use_cache`). This is a dead parameter that misleads callers.

**Suggested improvement:**
For `facade.py`, consider a NullObject pattern: provide a `NullRuntimeProfile` that supplies safe defaults instead of checking `None` repeatedly. For `profiles.py`, remove the `use_cache` parameter or implement it.

**Effort:** small
**Risk if unaddressed:** low -- cosmetic, but the repeated None checks add cognitive overhead.

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Good structural separation. Config structs are pure (frozen, fingerprint-able). IO and registration happen at the shell (facade, context installation, session building).

**Findings:**
- `runtime_profile_config.py` -- All config structs are `frozen=True` with `fingerprint_payload()` methods. Pure core.
- `cache_policy.py` -- `CachePolicyConfig` and `cache_policy_settings()` are pure transforms.
- `context_pool.py:55-175` -- `SessionFactory.build_config()` is imperative (setting config values on a mutable SessionConfig object). This is expected at the shell.
- `runtime_extensions.py` -- Extension installation functions are shell operations (IO + registration). Correctly placed.

**Suggested improvement:**
No action needed. The boundary between pure config and imperative context building is well-defined.

**Effort:** medium (for any cleanup)
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Session context caching is idempotent. Delta commit versioning uses explicit `commit_sequence` tracking.

**Findings:**
- `runtime_context.py:381-389` -- `_cached_context()` / `_cache_context()` ensure that repeated calls return the same context.
- `runtime_ops.py:141-192` -- `reserve_delta_commit()` uses `run.next_commit_version()` with monotonic commit sequences.
- `registry_facade.py:81-106` -- `RegistrationPhaseOrchestrator` checks for duplicate phase names.

**Suggested improvement:**
No action needed. Idempotency is well-maintained.

**Effort:** -
**Risk if unaddressed:** -

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Fingerprinting is comprehensive and deterministic throughout the configuration system.

**Findings:**
- `runtime_profile_config.py` -- Every config struct has `fingerprint_payload()` returning canonical dictionaries.
- `runtime_diagnostics_mixin.py:278-300` -- `fingerprint_payload()` aggregates sub-fingerprints from execution, catalog, data_sources, features, diagnostics, and policies.
- `identity.py` -- `identity_fingerprint()` uses `hash_msgpack_canonical()` for deterministic hashing.
- `hashing.py` -- `HashExprSpec` with `stable_id()` provides deterministic identifier generation.

**Suggested improvement:**
No action needed. Determinism is a first-class concern.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are straightforward. Two areas are unnecessarily complex.

**Findings:**
- `runtime_compile_options.py:249-273` -- A 21-element `unchanged` tuple is constructed to detect whether options changed. This is fragile (must be updated whenever a field is added) and could be replaced with `dataclasses.fields()` iteration or a simple equality check on the original vs. new options.
- `runtime_extensions.py:72-143` -- 15 `_defer_import_*` functions with near-identical structure (each imports a single constant from a different module). A generic `_defer_import(module, attr)` factory would reduce this to a single function.

**Suggested improvement:**
1. Replace the 21-element `unchanged` tuple with `resolved == options` equality or a diff-based approach.
2. Create `_defer_import(module_path: str, attr: str) -> Callable[[], object]` to replace the 15 identical deferred-import functions.

**Effort:** medium
**Risk if unaddressed:** low -- the current code works but is error-prone when fields are added.

---

#### P20. YAGNI -- Alignment: 3/3

**Current state:**
No speculative generality observed. All abstractions serve current use cases.

**Suggested improvement:**
No action needed.

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Most APIs behave as expected. Two minor surprises.

**Findings:**
- `profiles.py:18` -- `runtime_for_profile(profile, use_cache=True)` silently ignores the `use_cache` parameter (`_ = use_cache`). Callers who pass `use_cache=False` expect uncached behavior but get cached behavior.
- `lifecycle.py:1-32` -- This 32-LOC module simply delegates to `profile.session_context()` and `profile.context_cache_key()`. It adds no value and may confuse developers looking for session lifecycle management.

**Suggested improvement:**
Remove the dead `use_cache` parameter from `runtime_for_profile()` or implement it. Consider removing `lifecycle.py` if it serves no distinct purpose beyond what the profile provides directly.

**Effort:** small
**Risk if unaddressed:** low -- minor confusion risk.

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
`__all__` is consistently maintained across all modules. The lazy export hub in `__init__.py` is well-organized.

**Findings:**
- `__init__.py:1-318` -- 47 lazy exports via `_EXPORTS` dict. All imports are in `TYPE_CHECKING` block. Clean pattern.
- `runtime_udf.py:36-64` -- `__all__` includes 24+ entries, many of which are private (`_PlannerRuleInstallers`, `_constraint_drift_entries`, etc.). Private names in `__all__` send mixed signals about the public surface.

**Suggested improvement:**
Remove private names (prefixed with `_`) from `runtime_udf.py.__all__`. If they need to be importable, make them public or use a separate `_internal` module.

**Effort:** small
**Risk if unaddressed:** low -- documentation clarity issue.

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Config structs are easily testable (frozen, no IO). However, hidden profile construction in helpers defeats dependency injection.

**Findings:**
- The 7+ hidden `DataFusionRuntimeProfile()` constructions (see P12) mean test code cannot inject mock profiles without monkeypatching.
- `runtime_context.py` -- `_RuntimeContextMixin` methods cannot be tested in isolation because they require the full profile struct via `self: Any`.
- `streaming.py:218-324` -- `StreamingExecutor` accepts `ctx` and `sql_options` via constructor injection. Good testability.
- `delta_session_builder.py:198-280` -- `build_delta_session_context()` accepts `profile` and `runtime_env` explicitly. Good testability.

**Suggested improvement:**
Require explicit injection of `DataFusionRuntimeProfile` or `SessionContext` in all helper functions (see P12 recommendation). This makes every function testable with mock inputs.

**Effort:** medium
**Risk if unaddressed:** medium -- untestable code paths accumulate technical debt.

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
Comprehensive and well-structured observability. Diagnostics, telemetry, artifact recording, and extension validation are all wired.

**Findings:**
- `runtime_telemetry.py` (806 LOC) -- Extensive telemetry payload construction with 12 PyArrow struct schemas. Every runtime decision is captured.
- `runtime_diagnostics_mixin.py` -- `settings_payload()`, `fingerprint_payload()`, `telemetry_payload()`, `telemetry_payload_v1()` provide multiple granularity levels.
- `runtime_extensions.py` -- 10+ `_record_*` functions capture extension installation events.
- `runtime_ops.py:176-191` -- Delta commit reservation captures event_time, run_id, app_id, version, commit_sequence.
- `hooks.py:25-50` -- `chain_optional_hooks` and `record_registration` provide composable observability.

**Suggested improvement:**
No action needed. Observability is excellent.

---

## Cross-Cutting Themes

### Theme 1: `self: Any` Mixin Anti-Pattern

**Root cause:** The mixin pattern was chosen to decompose a large class, but Python's type system cannot express "the self parameter has these specific fields" without Protocol types. The result is `self: Any` everywhere, which silently disables type checking for the entire mixin.

**Affected principles:** P1 (information hiding), P4 (coupling), P8 (design by contract), P13 (composition over inheritance), P23 (testability).

**Suggested approach:** Define a `RuntimeProfileInterface` Protocol with all required fields and use it as the self type. Or, better, switch from mixins to delegation objects that receive only the config structs they need.

### Theme 2: Hidden Default Construction

**Root cause:** Convenience functions create `DataFusionRuntimeProfile()` internally to avoid requiring callers to pass an explicit profile. This creates invisible dependencies and prevents DI.

**Affected principles:** P5 (dependency direction), P12 (dependency inversion), P23 (testability).

**Suggested approach:** Establish a convention: leaf functions require explicit context/profile. Provide `default_profile()` as a top-level helper for composition roots only.

### Theme 3: Knowledge Duplication in Telemetry

**Root cause:** `runtime_telemetry.py` reimplements helper functions from `runtime_compile.py` and `runtime_config_policies.py` rather than importing them, likely due to circular import avoidance.

**Affected principles:** P7 (DRY).

**Suggested approach:** Extract shared helpers into a dependency-free leaf module (e.g., `_session_policy_helpers.py`) that both `runtime_telemetry.py` and `runtime_compile.py` can import without circular dependencies.

### Theme 4: ExplainRows Type Alias Scatter

**Root cause:** The `ExplainRows` conditional type alias (`TableLike | RecordBatchReaderLike` if TYPE_CHECKING else `object`) is defined in 5 files because each file needs the type but cannot import it without potential circularity.

**Affected principles:** P7 (DRY), P22 (public contracts).

**Suggested approach:** Define `ExplainRows` once in `runtime_profile_config.py` (or a dedicated types module) and import it everywhere. The conditional pattern is the same in all files.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Consolidate `ExplainRows` type alias from 5 files to 1 | small | Eliminates 4 copies of identical conditional type definition |
| 2 | P7 (DRY) | Import `_identifier_normalization_mode` in `runtime_telemetry.py` from `runtime_compile.py` | small | Single authoritative source for normalization mode logic |
| 3 | P21 (Least astonishment) | Remove dead `use_cache` parameter from `profiles.py:runtime_for_profile()` | small | Eliminates caller confusion |
| 4 | P19 (KISS) | Replace 15 `_defer_import_*` functions with generic factory in `runtime_extensions.py` | small | Reduces 70 LOC to ~10 LOC |
| 5 | P22 (Contracts) | Remove private names from `runtime_udf.py.__all__` | small | Clarifies public API surface |

## Recommended Action Sequence

1. **Consolidate `ExplainRows` type alias** (P7) -- Define once in `runtime_profile_config.py`, import in all other files. No behavior change, pure cleanup.

2. **Eliminate DRY violations** (P7) -- Extract `_identifier_normalization_mode`, `_catalog_autoload_settings`, and `_effective_catalog_autoload_for_profile` into `_session_policy_helpers.py` and import everywhere. This breaks the circular import chain that caused the duplication.

3. **Replace deferred import pattern** (P19) -- Create `_defer_spec_import(module_path, attr)` factory in `runtime_extensions.py`. Reduces 15 nearly-identical functions to 15 calls to the factory.

4. **Remove hidden `DataFusionRuntimeProfile()` constructions** (P5, P12, P23) -- Make `runtime_profile` or `ctx` required in `runtime_dataset_io.py`, `kernels.py`, `encoding/policy.py`, `udf/platform.py`, and `schema/validation.py`. Provide an explicit `default_session_context()` at the module level for composition roots.

5. **Define `RuntimeProfileInterface` Protocol** (P1, P8, P13) -- Create a Protocol declaring all fields that mixins access. Use it as the `self` type in `_RuntimeContextMixin` and `_RuntimeDiagnosticsMixin`, replacing `self: Any`.

6. **Decompose `PolicyBundleConfig`** (P3, P10) -- Split into 4-5 focused sub-configs. The parent struct becomes a composition of sub-configs with delegated fingerprinting.

7. **Transition mixins to delegation** (P4, P13) -- Long-term: replace `_RuntimeContextMixin` and `_RuntimeDiagnosticsMixin` with ops objects that receive only their required config structs. This is the largest change but eliminates the `self: Any` anti-pattern entirely.
