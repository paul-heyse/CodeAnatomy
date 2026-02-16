# Design Review: src/datafusion_engine (session, bootstrap, compile core)

**Date:** 2026-02-16
**Scope:** `src/datafusion_engine/session/`, `src/datafusion_engine/bootstrap/`, `src/datafusion_engine/compile/`, plus `errors.py`, `materialize_policy.py`, `identity.py`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 16

## Executive Summary

The DataFusion engine layer contains a well-designed periphery (`streaming.py`, `features.py`, `helpers.py`, `errors.py`, `identity.py`, `materialize_policy.py`, `cache_policy.py`) surrounding a severely overloaded core: `session/runtime.py` at 8,229 lines. This single file concentrates 20+ classes, 60+ exported symbols, configuration structs, policy presets, session lifecycle management, diagnostics mixins, hook factories, telemetry builders, and registration logic. It is the primary driver of nearly every design principle violation found in this review. The `DataFusionRuntimeProfile` class inherits from four mixins and uses `cast("DataFusionRuntimeProfile", self)` extensively, indicating that the mixin decomposition is syntactic rather than structural. Extracting configuration structs, policy presets, diagnostics, and registration logic into dedicated modules would address the majority of findings across all six principle categories.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | medium | `runtime.py` exposes 60+ symbols; config internals accessed by facade and pool |
| 2 | Separation of concerns | 0 | large | high | `runtime.py` mixes config, lifecycle, diagnostics, telemetry, hooks, registration |
| 3 | SRP | 0 | large | high | 8,229-line file with 20+ classes changes for config, lifecycle, diagnostics, hooks, registration |
| 4 | High cohesion, low coupling | 1 | large | high | `PolicyBundleConfig` (50+ fields) couples unrelated policy domains |
| 5 | Dependency direction | 2 | small | low | Core types in runtime.py; details at edges; deferred imports manage cycles |
| 6 | Ports & Adapters | 2 | medium | low | `DataFusionIOAdapter` and `WritePipeline` are proper adapters; diagnostics sink is a port |
| 7 | DRY | 0 | medium | high | Three near-identical `_record_*_registration` methods; five identical `_chain_*_hooks` functions |
| 8 | Design by contract | 2 | small | low | Config structs are frozen with explicit defaults; validation at construction |
| 9 | Parse, don't validate | 2 | small | low | Config structs with Literal types and frozen semantics parse at boundary |
| 10 | Make illegal states unrepresentable | 1 | medium | medium | `PolicyBundleConfig` allows contradictory combinations; facade accepts `None` profile |
| 11 | CQS | 2 | small | low | Most methods are clearly query or command; `__post_init__` side effects are notable |
| 12 | DI + explicit composition | 1 | medium | medium | Module-level caches create hidden state; `SessionFactory` uses deferred imports |
| 13 | Composition over inheritance | 1 | medium | medium | 4-mixin inheritance with `cast()` pattern; could be composition |
| 14 | Law of Demeter | 1 | medium | medium | `profile.policies.feature_gates.enable_*` chains throughout |
| 15 | Tell, don't ask | 1 | medium | medium | Facade repeatedly checks `if self.runtime_profile is None` then branches |
| 16 | Functional core, imperative shell | 2 | small | low | Config structs are pure data; session lifecycle is imperative shell |
| 17 | Idempotency | 2 | small | low | Session caching is key-based and idempotent; UDF installation is guarded |
| 18 | Determinism | 3 | n/a | n/a | Fingerprinting, hash versioning, and reproducibility are first-class |
| 19 | KISS | 1 | medium | medium | `PolicyBundleConfig` with 50+ fields; multiple telemetry payload variants |
| 20 | YAGNI | 2 | small | low | Most abstractions serve current use cases; some unused feature gates |
| 21 | Least astonishment | 1 | medium | medium | `config.py` re-exports everything from `runtime.py`; two telemetry payload functions |
| 22 | Public contracts | 1 | medium | medium | `__all__` exports 60+ symbols from one module; no versioned contract boundary |
| 23 | Testability | 1 | medium | high | Module-level caches, mixin casts, and 8K file make isolation difficult |
| 24 | Observability | 2 | small | low | OTel instrumentation is consistent; diagnostics sink pattern is well-designed |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
`runtime.py` exports 60+ symbols via `__all__` (lines 8113-8174). Internal implementation details like `_SESSION_CONTEXT_CACHE` (line 1681), `_SESSION_RUNTIME_CACHE` (line 3384), and `_RUNTIME_SETTINGS_OVERLAY` (line 3385) are module-level mutable dictionaries. While prefixed with underscores, they are accessible to any importer and their existence is observable through cache-hit behavior.

**Findings:**
- `runtime.py:8113-8174`: `__all__` with 60+ symbols exposes configuration structs, policy presets, helper functions, and lifecycle managers from a single module. Callers cannot distinguish stable API from implementation details.
- `runtime.py:1681`: `_SESSION_CONTEXT_CACHE` is a module-level `dict[str, SessionContext]` with no encapsulation -- any code importing the module could depend on cache-hit timing.
- `runtime.py:3384-3385`: `_SESSION_RUNTIME_CACHE` and `_RUNTIME_SETTINGS_OVERLAY` are similarly exposed mutable state.
- `context_pool.py:279-286`: `SessionFactory.build_config()` uses deferred imports from `runtime.py` to access `effective_catalog_autoload`, `effective_ident_normalization`, etc., reaching into runtime internals.
- `facade.py:171-172`: `DataFusionExecutionFacade.__post_init__` directly accesses `self.runtime_profile.features` and `self.runtime_profile.policies` sub-structs.

**Suggested improvement:**
Extract a `session/public_api.py` module that re-exports only the truly public symbols (the profile class, factory functions, and result types). Internal helpers, caches, and hook factories should remain private to their own modules. Consider wrapping module-level caches behind a `SessionCacheManager` class that can be injected.

**Effort:** medium
**Risk if unaddressed:** medium -- callers build implicit dependencies on 60+ symbols, making refactoring of internals risky.

---

#### P2. Separation of concerns -- Alignment: 0/3

**Current state:**
`runtime.py` is the most significant SoC violation in this scope. A single 8,229-line file contains:
- **Configuration structs** (7 classes: `ExecutionConfig`, `CatalogConfig`, `DataSourceConfig`, `ZeroRowBootstrapConfig`, `FeatureGatesConfig`, `DiagnosticsConfig`, `PolicyBundleConfig`) at lines 3730-4013
- **Policy presets** (`DEFAULT_DF_POLICY`, `CST_AUTOLOAD_DF_POLICY`, `SYMTABLE_DF_POLICY`, `DEV_DF_POLICY`, `PROD_DF_POLICY`) at lines 1530-1611
- **Session lifecycle management** (`_SESSION_CONTEXT_CACHE`, `session_context()`, `build_session_runtime()`) scattered throughout
- **Diagnostics mixin** (`_RuntimeDiagnosticsMixin`, lines 2948-3369) with `record_artifact`, `settings_payload`, `telemetry_payload`, etc.
- **Hook chaining factories** (lines 2199-2266)
- **Registration logic** (`_record_ast_registration`, `_record_bytecode_registration`, `_record_scip_registration`, lines 5792-5915)
- **View registry** (`DataFusionViewRegistry`, line 1378)
- **Helper classes** (`RuntimeProfileDeltaOps`, `RuntimeProfileIO`, `RuntimeProfileCatalog`)
- **Three facade mixins** (`_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`, `_RuntimeProfileDeltaFacadeMixin`)

**Findings:**
- `runtime.py:1-8229`: The entire file conflates configuration, session lifecycle, diagnostics, telemetry, hook chaining, registration, view management, and Delta operations.
- `runtime.py:3730-4013`: Seven configuration structs embedded within the lifecycle module instead of a dedicated config module.
- `runtime.py:2948-3369`: `_RuntimeDiagnosticsMixin` (420 lines) handles telemetry payloads, settings hashing, fingerprinting, and artifact recording -- all diagnostics concerns that could live in `session/diagnostics.py`.
- `runtime.py:2199-2266`: Five hook chaining functions that are pure composition utilities embedded in the runtime lifecycle module.

**Suggested improvement:**
Decompose `runtime.py` into at least five focused modules:
1. `session/config_structs.py` -- All `*Config` structs and `PolicyBundleConfig`
2. `session/policy_presets.py` -- `DATAFUSION_POLICY_PRESETS`, `CACHE_PROFILES`, `SCHEMA_HARDENING_PRESETS`
3. `session/diagnostics_mixin.py` -- `_RuntimeDiagnosticsMixin` and its helper functions
4. `session/hook_factories.py` -- All `_chain_*_hooks` functions
5. `session/registration.py` -- `_record_*_registration` methods and snapshot types

**Effort:** large
**Risk if unaddressed:** high -- the monolithic file is the root cause of violations in P1, P3, P4, P7, P13, P19, P21, P22, and P23.

---

#### P3. SRP (one reason to change) -- Alignment: 0/3

**Current state:**
`runtime.py` changes for at least six distinct reasons:
1. Adding/modifying DataFusion configuration knobs
2. Changing session lifecycle management (caching, pooling)
3. Evolving diagnostics/telemetry payload schemas
4. Adding new hook types or modifying hook chaining
5. Changing dataset registration logic
6. Modifying Delta operations or view management

**Findings:**
- `runtime.py:3730-4013`: Config struct changes (reason 1) occur in the same file as session cache management at line 1681 (reason 2) and diagnostics at line 2948 (reason 3).
- `runtime.py:4547-4554`: `DataFusionRuntimeProfile` inherits from four mixins, making it the hub for all six change reasons. The class definition itself becomes a change hotspot for any modification to any concern.
- `runtime.py:5792-5915`: Registration payload builders change when dataset schemas evolve -- a different change reason from session lifecycle.

**Suggested improvement:**
Same decomposition as P2. The `DataFusionRuntimeProfile` class should be a composition root that delegates to injected collaborators rather than inheriting behavior from mixins.

**Effort:** large
**Risk if unaddressed:** high -- every change to any concern risks regression in the others, and merge conflicts are likely in a team setting.

---

#### P4. High cohesion, low coupling -- Alignment: 1/3

**Current state:**
The peripheral modules (`streaming.py`, `features.py`, `helpers.py`, `introspection.py`, `cache_policy.py`) demonstrate good cohesion -- each owns a single, well-defined concept. However, `runtime.py` and `PolicyBundleConfig` are the opposite.

**Findings:**
- `runtime.py:3958-4013`: `PolicyBundleConfig` has 50+ fields spanning config policies, cache settings, SQL policies, Delta settings, hook callables, UDF policies, disk cache profiles, and runtime env hooks. This struct conflates at least 8 distinct policy domains.
- `runtime.py:4547-4554`: `DataFusionRuntimeProfile` couples configuration, diagnostics, IO, catalog operations, and Delta operations through multiple-inheritance.
- `context_pool.py:266-286`: `SessionFactory` is tightly coupled to `runtime.py` internals via deferred imports of 6 helper functions (`effective_catalog_autoload`, `effective_ident_normalization`, `performance_policy_settings`, `resolved_config_policy`, `resolved_schema_hardening`, `supports_explain_analyze_level`).
- `facade.py:129-130`: `DataFusionExecutionFacade` holds both `ctx: SessionContext` and `runtime_profile: DataFusionRuntimeProfile | None`, coupling the execution surface to the full runtime profile.

**Suggested improvement:**
Split `PolicyBundleConfig` into focused policy groups: `CachePolicyBundle`, `DeltaPolicyBundle`, `UdfPolicyBundle`, `HookPolicyBundle`. This reduces the field count per struct and makes each policy domain independently testable.

**Effort:** large
**Risk if unaddressed:** high -- the 50+ field struct is the second-largest coupling surface after the file itself.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The dependency direction is mostly correct. Core types (`DataFusionRuntimeProfile`, config structs) are defined in `runtime.py` and depended upon by detail modules (`facade.py`, `context_pool.py`, `bootstrap/zero_row.py`). The deferred import block at `runtime.py:8186-8229` manages circular dependencies with `serde_artifact_specs`.

**Findings:**
- `runtime.py:8186-8229`: 40+ deferred imports of artifact spec constants at end-of-file to avoid circular imports. This is a pragmatic solution but indicates that the module is depended upon by too many downstream modules.
- `features.py:15`: `TYPE_CHECKING`-guarded import of `DataFusionRuntimeProfile` keeps the dependency direction correct.
- `facade.py:34`: Import of `session_runtime_for_context` from `runtime.py` is appropriately detail-depends-on-core.

**Suggested improvement:**
Extracting config structs and artifact spec constants to their own modules would eliminate the need for the deferred import block entirely, simplifying the import graph.

**Effort:** small
**Risk if unaddressed:** low -- the deferred imports work correctly; the risk is maintainability.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The module demonstrates partial Ports & Adapters alignment. `DiagnosticsSink` (referenced in `DiagnosticsConfig` at line 3948) acts as a port for diagnostics recording. `DataFusionIOAdapter` (used in `facade.py:190-198`) is a proper adapter. `WritePipeline` and `WriteRequest` (facade.py imports) separate write concerns. However, the `DataFusionRuntimeProfile` directly knows about Delta, disk cache, and filesystem concerns.

**Findings:**
- `runtime.py:3948`: `diagnostics_sink: DiagnosticsSink | None` is a well-designed port -- the core declares what it needs without specifying how.
- `facade.py:17-22`: `DataFusionIOAdapter`, `WritePipeline`, `WriteRequest`, `WriteViewRequest` form a proper adapter layer for IO operations.
- `runtime.py:4001-4003`: `diskcache_profile: DiskCacheProfile | None` in `PolicyBundleConfig` embeds a specific storage technology directly in the policy struct rather than behind a port.
- `runtime.py:3997-4000`: Delta-specific fields (`delta_store_policy`, `delta_mutation_policy`, `delta_protocol_support`, `delta_protocol_mode`) embedded directly in `PolicyBundleConfig` rather than behind a port abstraction.

**Suggested improvement:**
Extract Delta-specific and disk-cache-specific fields into their own policy structs that `PolicyBundleConfig` references by composition. This keeps technology-specific details one level of indirection away from the core policy surface.

**Effort:** medium
**Risk if unaddressed:** low -- the current design works but increases the cost of replacing Delta or disk-cache implementations.

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 0/3

**Current state:**
This is the most severe DRY violation in the scope. Three registration payload builders and five hook chaining functions encode the same knowledge in nearly identical code.

**Findings:**
- `runtime.py:5792-5829` (`_record_ast_registration`), `runtime.py:5831-5868` (`_record_bytecode_registration`), `runtime.py:5870-5915` (`_record_scip_registration`): These three methods construct nearly identical payload dictionaries from `DatasetLocation` objects. The only differences are the dataset name string and the artifact spec constant. If the payload schema changes, all three must be updated in lockstep.
- `runtime.py:2199-2210` (`_chain_explain_hooks`), `runtime.py:2213-2224` (`_chain_plan_artifacts_hooks`), `runtime.py:2227-2238` (`_chain_sql_ingest_hooks`), `runtime.py:2241-2252` (`_chain_cache_hooks`), `runtime.py:2255-2266` (`_chain_substrait_fallback_hooks`): Five functions with identical structure -- filter `None` hooks, return `None` if empty, return a closure that iterates and calls each hook. They differ only in the callback signature type.
- `runtime.py:3751-3777` (`ExecutionConfig.fingerprint_payload`), `runtime.py:3794-3814` (`CatalogConfig.fingerprint_payload`), `runtime.py:3856-3872` (`ZeroRowBootstrapConfig.fingerprint_payload`), `runtime.py:3899-3928` (`FeatureGatesConfig.fingerprint_payload`): Each config struct manually lists all its fields in `fingerprint_payload()`. This duplicates the field definitions and must be updated whenever a field is added.

**Suggested improvement:**
1. Extract a generic `_build_registration_payload(location: DatasetLocation, name: str, spec: ArtifactSpec)` function that the three `_record_*_registration` methods delegate to.
2. Create a generic `chain_hooks(hooks: Iterable[Callable | None]) -> Callable | None` that is parameterized by the hook signature type (using `TypeVar` or `ParamSpec`).
3. Consider a `fingerprint_payload_from_struct(struct: StructBaseStrict) -> dict[str, object]` utility that introspects `msgspec.Struct` fields, eliminating per-struct manual listing.

**Effort:** medium
**Risk if unaddressed:** high -- payload schema drift across the three registration methods is a likely future bug.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Configuration structs use frozen `msgspec.Struct` bases with explicit defaults, `Literal` type constraints, and validation methods. `DataFusionRuntimeProfile` has `_validate_information_schema` and `_validate_catalog_names` methods.

**Findings:**
- `runtime.py:4613-4616`: `_validate_information_schema()` enforces that `enable_information_schema` must be `True`, but this is called lazily rather than at construction time.
- `runtime.py:3848`: `ZeroRowBootstrapConfig.validation_mode: Literal["off", "bootstrap"]` uses Literal to constrain valid values at the type level.
- `compile/options.py:79-100`: `DataFusionCompileOptionsSpec` has clear parameter documentation and defaults.
- `errors.py:14-29`: `ErrorKind` StrEnum and `DataFusionEngineError` provide a clear error contract.

**Suggested improvement:**
Move `_validate_information_schema` and `_validate_catalog_names` into a `__post_init__` hook or dedicated validation phase that runs at construction time rather than lazily.

**Effort:** small
**Risk if unaddressed:** low -- the validators exist but are called at the right times in practice.

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Configuration is parsed into frozen `msgspec.Struct` instances at construction time. `Literal` types, default values, and frozen semantics ensure that after construction, configuration is well-formed. `compile/options.py:79` (`DataFusionCompileOptionsSpec`) demonstrates the parse-at-boundary pattern well.

**Findings:**
- `compile/options.py:79-145`: `DataFusionCompileOptionsSpec` is parsed at construction and then transformed into `DataFusionCompileOptions` via `compile_options_from_spec()` -- a textbook parse-then-use pattern.
- `runtime.py:3958-4013`: `PolicyBundleConfig` accepts `str | None` for `config_policy_name` and `schema_hardening_name`, which are later resolved to objects via lookup functions. These could be parsed to the resolved type at construction.

**Suggested improvement:**
Resolve `config_policy_name` and `schema_hardening_name` to their actual policy objects at construction time rather than deferring resolution to usage sites.

**Effort:** small
**Risk if unaddressed:** low -- the current approach works but scatters resolution logic.

---

#### P10. Make illegal states unrepresentable -- Alignment: 1/3

**Current state:**
`PolicyBundleConfig` allows several contradictory combinations that should be prevented by construction.

**Findings:**
- `runtime.py:3961-3962`: `config_policy_name: str | None` and `config_policy: DataFusionConfigPolicy | None` can both be set simultaneously, creating ambiguity about which takes precedence. This pattern is repeated for `schema_hardening_name`/`schema_hardening` (lines 3974-3975) and `sql_policy_name`/`sql_policy` (lines 3976-3977).
- `facade.py:130`: `runtime_profile: DataFusionRuntimeProfile | None = None` means the facade must handle the `None` case in every method, leading to repeated `if self.runtime_profile is None` guards. If a facade always needs a profile, the type should not allow `None`.
- `runtime.py:3878-3879`: `enable_ident_normalization: bool` and `force_disable_ident_normalization: bool` in `FeatureGatesConfig` can both be `True` simultaneously, which is contradictory. This should be a three-value enum: `off`, `on`, `force_off`.

**Suggested improvement:**
1. Replace name/object pairs with a union type: `config_policy: str | DataFusionConfigPolicy | None` that is resolved at construction.
2. If `DataFusionExecutionFacade` always requires a profile, remove the `None` option and create a separate `MinimalExecutionFacade` for the no-profile case.
3. Replace `enable_ident_normalization` + `force_disable_ident_normalization` with `ident_normalization: Literal["off", "on", "force_off"]`.

**Effort:** medium
**Risk if unaddressed:** medium -- contradictory config states can produce hard-to-diagnose runtime behavior.

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most methods are clearly queries (returning data) or commands (mutating state). Config struct methods like `fingerprint_payload()`, `to_row()` are pure queries. Registration methods are pure commands.

**Findings:**
- `facade.py:132-188`: `__post_init__` both constructs state AND installs planner extensions as a side effect. This mixes initialization (command) with construction (query-like). Callers creating a `DataFusionExecutionFacade` may not expect side effects.
- `context_pool.py:194-199`: `DataFusionContextPool.__post_init__` eagerly allocates pooled contexts -- a side effect during construction.
- `streaming.py:41-216`: `StreamingExecutionResult` cleanly separates queries (`schema`, `to_table`, `to_pandas`) from the construction-time state. Good CQS.

**Suggested improvement:**
Document the `__post_init__` side effects clearly in the class docstrings, or consider an explicit `initialize()` method that callers invoke after construction.

**Effort:** small
**Risk if unaddressed:** low -- the pattern is common in Python dataclasses and well-understood.

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 1/3

**Current state:**
Module-level mutable caches create hidden implicit state that is difficult to control in tests or alternative configurations.

**Findings:**
- `runtime.py:1681`: `_SESSION_CONTEXT_CACHE: dict[str, SessionContext] = {}` is global mutable state with no injection point. Tests cannot control cache behavior without patching module internals.
- `runtime.py:3384-3385`: `_SESSION_RUNTIME_CACHE` and `_RUNTIME_SETTINGS_OVERLAY` are similarly global. Their state persists across test runs unless explicitly cleared.
- `context_pool.py:279-286`: `SessionFactory.build_config()` uses 6 deferred imports from `runtime.py` instead of receiving these as injected dependencies or using a protocol.
- `facade.py:132-188`: `DataFusionExecutionFacade.__post_init__` imports and calls `install_rust_udf_platform` directly rather than receiving a UDF installer as a dependency.

**Suggested improvement:**
Wrap the three module-level caches in a `SessionCacheManager` class that can be injected. For `SessionFactory`, define a `ConfigPolicyProvider` protocol that abstracts the 6 helper functions, allowing test substitution without deferred imports.

**Effort:** medium
**Risk if unaddressed:** medium -- global caches make test isolation fragile and prevent parallel test execution.

---

#### P13. Composition over inheritance -- Alignment: 1/3

**Current state:**
`DataFusionRuntimeProfile` uses multiple inheritance from four mixins plus `StructBaseStrict`. The mixins use `cast("DataFusionRuntimeProfile", self)` in every method, which is a strong signal that composition would be more appropriate.

**Findings:**
- `runtime.py:4547-4554`: `DataFusionRuntimeProfile` inherits from `_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`, `_RuntimeProfileDeltaFacadeMixin`, `_RuntimeDiagnosticsMixin`, and `StructBaseStrict`.
- `runtime.py:2951,2956,2967,2980,3008,3026`: `_RuntimeDiagnosticsMixin` uses `cast("DataFusionRuntimeProfile", self)` in every method body. This cast is necessary because the mixin does not declare that it IS a `DataFusionRuntimeProfile` -- it just assumes it. This is a classic sign that the mixin pattern is fighting the type system.
- `runtime.py:4580-4611`: `DataFusionRuntimeProfile` already provides composition-style accessors (`delta_ops`, `io_ops`, `catalog_ops`) that return helper objects. This proves the composition approach works -- but the mixin methods are still inherited alongside.

**Suggested improvement:**
Replace the four mixins with composed collaborator classes. `DataFusionRuntimeProfile` would hold `diagnostics: RuntimeDiagnostics`, `io: RuntimeProfileIO`, `catalog: RuntimeProfileCatalog`, `delta: RuntimeProfileDeltaOps` as fields. The existing `delta_ops`, `io_ops`, `catalog_ops` properties (lines 4580-4611) already demonstrate this pattern -- extend it to diagnostics and remove the mixin inheritance.

**Effort:** medium
**Risk if unaddressed:** medium -- the `cast()` pattern is fragile and invisible to type checkers. If a mixin method is called on a non-profile object, the cast would silently produce incorrect behavior.

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
Multi-level attribute traversals are pervasive, especially for accessing feature gates and policy settings through the `DataFusionRuntimeProfile`.

**Findings:**
- `facade.py:171-183`: `self.runtime_profile.features.enable_udfs`, `self.runtime_profile.features.enable_async_udfs`, `self.runtime_profile.policies.async_udf_timeout_ms`, `self.runtime_profile.policies.function_factory_hook`, etc. -- chained access through `profile.features.*` and `profile.policies.*`.
- `features.py:80-87`: `runtime_profile.policies.feature_gates.enable_dynamic_filter_pushdown`, `runtime_profile.policies.feature_gates.enable_join_dynamic_filter_pushdown`, etc. -- three-level deep access into the feature gates sub-sub-struct.
- `context_pool.py:290-296`: `profile.catalog.default_catalog`, `profile.catalog.default_schema`, `profile.catalog.enable_information_schema` -- reaching through the profile into catalog config.
- `runtime.py:2985-2997`: `profile.policies.cache_policy`, `profile.policies.settings_overrides` -- direct sub-struct access in the diagnostics mixin.

**Suggested improvement:**
Add delegation methods to `DataFusionRuntimeProfile` that expose commonly-needed configuration without requiring callers to traverse the struct tree. For example, `profile.are_dynamic_filters_enabled()` instead of `profile.policies.feature_gates.enable_dynamic_filter_pushdown and profile.policies.feature_gates.enable_join_dynamic_filter_pushdown and ...`.

**Effort:** medium
**Risk if unaddressed:** medium -- refactoring the internal config struct layout would require updating every call site that traverses the chain.

---

#### P15. Tell, don't ask -- Alignment: 1/3

**Current state:**
The `DataFusionExecutionFacade` repeatedly inspects `runtime_profile` state to decide behavior rather than delegating decisions to the profile.

**Findings:**
- `facade.py:155-168`: `DataFusionExecutionFacade.__post_init__` checks `if self.runtime_profile is None` and builds `RustUdfPlatformOptions` with hardcoded defaults, then falls through to the else branch where it reads `self.runtime_profile.features` and `self.runtime_profile.policies` to build options differently. This ask-then-act pattern could be a `tell` on the profile: `profile.build_udf_platform_options()`.
- `facade.py:171-183`: The facade reads 9 individual fields from `self.runtime_profile.features` and `self.runtime_profile.policies` to construct `RustUdfPlatformOptions`. This is "asking" the profile for raw data and reassembling it externally.
- `features.py:80-93`: `feature_state_snapshot()` reads 6 fields from the runtime profile to build a `FeatureStateSnapshot`. This could be a method on the profile itself.

**Suggested improvement:**
Add a `DataFusionRuntimeProfile.build_udf_platform_options() -> RustUdfPlatformOptions` method that encapsulates the options-building logic. Similarly, add `DataFusionRuntimeProfile.feature_state_snapshot() -> FeatureStateSnapshot`. This moves decision-making closer to the data.

**Effort:** medium
**Risk if unaddressed:** medium -- scattered logic that interprets profile state is harder to maintain and test.

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The configuration structs (`ExecutionConfig`, `CatalogConfig`, etc.) are frozen/pure data, forming a functional core. Session lifecycle management (`session_context()`, `build_session_runtime()`) is appropriately imperative. `streaming.py` demonstrates good FC/IS with `StreamingExecutionResult` as pure data and `StreamingExecutor` as the imperative shell.

**Findings:**
- `streaming.py:41-216`: `StreamingExecutionResult` is a frozen dataclass wrapping a DataFrame with deferred materialization methods. The executor at line 218 is the imperative shell. Good separation.
- `runtime.py:3730-4013`: All config structs are frozen and provide pure `fingerprint_payload()` methods. Good functional core.
- `runtime.py:1681-1692`: Module-level caches muddy the boundary between functional and imperative. Session creation functions are imperative (they mutate the cache) but are called from contexts that might expect functional behavior.

**Suggested improvement:**
No major changes needed. The module-level cache concern is addressed by P12 recommendations.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
Session caching is key-based (by `session_context_key`), making repeated calls idempotent. UDF installation in `facade.py` runs in `__post_init__` and produces the same result regardless of how many times the facade is constructed with the same context.

**Findings:**
- `context_pool.py:214`: `DataFusionContextPool.checkout()` creates a new context if the pool is empty, which is idempotent for the caller but may create session contexts with different internal state if pool exhaustion is unexpected.
- `runtime.py:1681`: `_SESSION_CONTEXT_CACHE` provides at-most-once session creation per key, ensuring idempotent session retrieval.

**Suggested improvement:**
No significant changes needed.

**Effort:** small
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Determinism is a first-class architectural concern. Every config struct provides `fingerprint_payload()`. `SessionRuntime` includes `udf_snapshot_hash`, `udf_rewrite_tags`, and `df_settings` for full reproducibility. The `identity.py` module provides canonical hashing. `materialize_policy.py` includes fingerprinting. This is well-aligned.

**Findings:**
- `runtime.py:3000-3013`: `settings_hash()` includes a `SETTINGS_HASH_VERSION` for versioned hash stability.
- `runtime.py:3015-3037`: `fingerprint_payload()` composes hashes from all sub-config fingerprints.
- `identity.py:1-90`: Canonical identity hashing using `payload_hash` with explicit schemas.
- `materialize_policy.py:1-52`: `MaterializationPolicy` includes fingerprinting.

**Suggested improvement:**
No changes needed. This is well-designed.

**Effort:** n/a
**Risk if unaddressed:** n/a

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 1/3

**Current state:**
The peripheral modules are simple and well-focused. However, the central `runtime.py` and `PolicyBundleConfig` are unnecessarily complex.

**Findings:**
- `runtime.py:3958-4013`: `PolicyBundleConfig` with 50+ fields requires callers to understand the entire policy surface to construct or modify a profile. A new contributor cannot read this struct top-down without domain expertise.
- `runtime.py:2948-3369`: `_RuntimeDiagnosticsMixin` has overlapping methods: `telemetry_payload()`, `telemetry_payload_v1()`, `_build_telemetry_payload_row()`. It is unclear which to use and when.
- `runtime.py:1530-1611`: Five policy presets differ only in numeric tuning parameters. The preset pattern itself is simple, but embedding it in the 8K-line file makes discovery difficult.
- `helpers.py:1-97`: Clean, focused utility module. Good KISS.
- `errors.py:1-29`: Minimal error hierarchy. Good KISS.

**Suggested improvement:**
Split `PolicyBundleConfig` into domain-specific policy groups (see P4). Consolidate the three telemetry payload methods into one with an explicit version parameter.

**Effort:** medium
**Risk if unaddressed:** medium -- onboarding cost is high when the central module requires 8K lines of context.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most abstractions serve current use cases. The extracted modules (`features.py`, `helpers.py`, `introspection.py`, `cache_policy.py`) are lean. Some feature gates in `FeatureGatesConfig` may be speculative.

**Findings:**
- `runtime.py:3888-3896`: `enable_async_udfs`, `enable_delta_cdf`, `enable_delta_querybuilder`, `enable_delta_data_checker`, `enable_delta_plan_codecs` are all `False` by default with no obvious current callers toggling them to `True`. These may be speculative placeholders.
- `runtime.py:1634-1676`: `CST_DIAGNOSTIC_STATEMENTS` and `INFO_SCHEMA_STATEMENTS` are fully specified prepared statement specs that serve the current pipeline.

**Suggested improvement:**
Audit the feature gates that default to `False` and have no current callers. If they are purely speculative, document them as planned-but-unimplemented or remove them.

**Effort:** small
**Risk if unaddressed:** low -- unused feature gates add minor cognitive load.

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
Several naming and structural choices may surprise competent readers.

**Findings:**
- `config.py:1-53`: This module is entirely re-exports from `runtime.py` with a comment "Canonical definitions remain in runtime.py to avoid circular imports." A reader expecting `config.py` to contain configuration definitions finds only re-exports. The module name suggests ownership but provides none.
- `runtime.py:2972-2998` vs `runtime.py:3039-3070` vs `runtime.py:3073-3126`: `settings_payload()`, `fingerprint_payload()`, `telemetry_payload()`, and `telemetry_payload_v1()` have similar names but different return types and purposes. The relationship between them is not obvious from names alone.
- `facade.py:130`: `runtime_profile: DataFusionRuntimeProfile | None = None` -- a "facade" that may not have a profile is surprising. The name implies a simplified interface, but `None` handling adds complexity.
- `context_pool.py:186-199`: `DataFusionContextPool.__post_init__` eagerly allocates sessions. A reader might expect pool allocation to be lazy.

**Suggested improvement:**
1. Rename `config.py` to `config_compat.py` or add a deprecation warning, making it clear this is a compatibility shim.
2. Rename `telemetry_payload_v1()` to reflect its purpose distinctly from `telemetry_payload()`, or deprecate one.
3. Document the eager allocation behavior in the `DataFusionContextPool` docstring.

**Effort:** medium
**Risk if unaddressed:** medium -- naming confusion slows onboarding and increases the risk of using the wrong function.

---

#### P22. Public contracts -- Alignment: 1/3

**Current state:**
`runtime.py` exports 60+ symbols via `__all__` with no distinction between stable public API and internal implementation details. There is no versioning of the public surface beyond the `architecture_version` field on `DataFusionRuntimeProfile`.

**Findings:**
- `runtime.py:8113-8174`: `__all__` includes config structs, policy presets, helper functions, factory functions, and diagnostics hooks -- all at the same export level. A caller cannot tell which symbols are safe to depend on.
- `runtime.py:4562`: `architecture_version: str = "v2"` provides a version marker for the profile, but the module's public surface is unversioned.
- `facade.py:104-106`: Comment indicates `ExecutionResult` and `ExecutionResultKind` were "extracted to break circular dependency" and "re-exported for backward compatibility." This suggests an evolving public surface without a clear contract.

**Suggested improvement:**
Create a `session/__init__.py` that explicitly exports only the stable public API. Mark internal helpers with a `_private` naming convention or move them to `session/_internal/`. Consider a `STABLE_API` list separate from `__all__`.

**Effort:** medium
**Risk if unaddressed:** medium -- downstream callers may depend on symbols that are implementation details, preventing safe refactoring.

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
The monolithic `runtime.py` with module-level caches and mixin-based composition creates significant testing obstacles.

**Findings:**
- `runtime.py:1681,3384-3385`: Three module-level mutable caches (`_SESSION_CONTEXT_CACHE`, `_SESSION_RUNTIME_CACHE`, `_RUNTIME_SETTINGS_OVERLAY`) persist state across tests. Testing session lifecycle requires patching or clearing these caches.
- `runtime.py:4547-4554`: `DataFusionRuntimeProfile` with four mixin parents cannot be tested in isolation -- each mixin's methods require the full `DataFusionRuntimeProfile` type via `cast()`.
- `facade.py:132-188`: `__post_init__` imports and installs UDF platforms as a side effect, making it impossible to construct a `DataFusionExecutionFacade` without triggering Rust extension loading. This prevents lightweight unit testing.
- `context_pool.py:194-199`: `DataFusionContextPool.__post_init__` eagerly creates sessions via `SessionFactory`, which requires a full `DataFusionRuntimeProfile`. No way to provide a mock factory.

**Suggested improvement:**
1. Wrap module-level caches in an injectable `SessionCacheManager`.
2. Replace mixin inheritance with composition (see P13), making each collaborator independently testable.
3. Accept a `udf_installer: Callable` parameter in `DataFusionExecutionFacade` so tests can provide a no-op installer.
4. Accept a `session_factory: SessionFactory` parameter in `DataFusionContextPool` to allow mock factories.

**Effort:** medium
**Risk if unaddressed:** high -- the current design discourages thorough unit testing of session lifecycle code, which is the most critical path in the DataFusion integration.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Observability is well-designed at the architectural level. OTel tracing is consistent in `facade.py`. `DiagnosticsSink` provides a structured port for diagnostics. `FeatureStateSnapshot` captures runtime feature state for diagnostics tables.

**Findings:**
- `facade.py:15-16,36-37`: Consistent use of `opentelemetry.semconv.attributes` and `obs.otel.tracing` for span instrumentation.
- `runtime.py:3931-3955`: `DiagnosticsConfig` provides structured configuration for explain capture, plan artifacts, metrics, and tracing.
- `features.py:33-47`: `FeatureStateSnapshot.to_row()` provides a structured diagnostic payload.
- `runtime.py:2972-3126`: Multiple overlapping telemetry payload methods (`settings_payload`, `telemetry_payload`, `telemetry_payload_v1`) create confusion about which telemetry data is canonical.

**Suggested improvement:**
Consolidate the telemetry payload methods into a single versioned method with a clear contract. Document which payload is consumed by which downstream system.

**Effort:** small
**Risk if unaddressed:** low -- the observability infrastructure works; the risk is confusion over which payload to use.

---

## Cross-Cutting Themes

### Theme 1: The runtime.py Monolith

**Root cause:** `runtime.py` grew organically as the single home for DataFusion configuration and session management. Each new feature (Delta support, diagnostics, schema hardening, UDF management) was added to the existing file rather than extracted into a focused module.

**Affected principles:** P1, P2, P3, P4, P7, P13, P19, P21, P22, P23

**Suggested approach:** A phased decomposition:
1. First, extract config structs (pure data, no behavioral dependencies) -- this is safe and low-risk.
2. Then, extract policy presets (pure constants, reference only config structs).
3. Then, extract diagnostics mixin (depends on config structs and artifact specs).
4. Finally, refactor `DataFusionRuntimeProfile` from mixin inheritance to composition.

Each phase can be validated independently. The first two phases eliminate ~500 lines from `runtime.py` with zero behavioral change.

### Theme 2: PolicyBundleConfig as a God Struct

**Root cause:** `PolicyBundleConfig` serves as the catch-all for any policy or hook that needs to be configurable. The 50+ fields span cache, Delta, UDF, SQL, disk, hook, and schema concerns.

**Affected principles:** P4, P10, P14, P19

**Suggested approach:** Group the fields into domain-specific policy structs: `CachePolicyBundle` (~8 fields), `DeltaPolicyBundle` (~7 fields), `UdfPolicyBundle` (~5 fields), `HookPolicyBundle` (~6 fields), `SchemaPolicyBundle` (~4 fields). `PolicyBundleConfig` becomes a composition of these smaller structs.

### Theme 3: Duplicated Payload Construction

**Root cause:** Registration payload builders and hook chaining functions were implemented as separate methods because they handle different dataset types, but they share 95%+ of their logic.

**Affected principles:** P7, P19

**Suggested approach:** Extract a generic payload builder function that accepts the varying parts (name, artifact spec, extra fields) as parameters. For hook chaining, create a single generic `chain_hooks` function parameterized by the callback type.

### Theme 4: Testing Impedance from Global State

**Root cause:** Module-level caches and `__post_init__` side effects create hidden global state that persists across test runs.

**Affected principles:** P12, P23

**Suggested approach:** Wrap caches in injectable managers. Accept factory/installer callables as parameters to classes that currently import and call them directly.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Extract generic `_build_registration_payload()` from three near-identical methods at `runtime.py:5792-5915` | small | Eliminates ~80 duplicated lines and prevents payload schema drift |
| 2 | P7 (DRY) | Extract generic `chain_hooks()` from five identical functions at `runtime.py:2199-2266` | small | Eliminates ~55 duplicated lines and simplifies adding new hook types |
| 3 | P10 (Illegal states) | Replace `enable_ident_normalization` + `force_disable_ident_normalization` with `ident_normalization: Literal["off", "on", "force_off"]` at `runtime.py:3878-3879` | small | Prevents contradictory config combinations |
| 4 | P21 (Least astonishment) | Rename or deprecate `config.py` which is pure re-exports from `runtime.py` | small | Reduces confusion about where config definitions live |
| 5 | P24 (Observability) | Consolidate `telemetry_payload()`, `telemetry_payload_v1()`, and `_build_telemetry_payload_row()` | small | Clarifies which telemetry method is canonical |

## Recommended Action Sequence

1. **Extract generic payload builder** (P7) -- Create `_build_registration_payload()` and refactor `_record_ast_registration`, `_record_bytecode_registration`, `_record_scip_registration` to delegate to it. Zero behavioral change. Validates with existing tests.

2. **Extract generic hook chainer** (P7) -- Create `chain_hooks()` and refactor the five `_chain_*_hooks` functions. Zero behavioral change.

3. **Fix ident normalization enum** (P10) -- Replace the boolean pair with a `Literal` type. Small change, improves type safety.

4. **Extract config structs to `session/config_structs.py`** (P2, P3) -- Move `ExecutionConfig`, `CatalogConfig`, `DataSourceConfig`, `ZeroRowBootstrapConfig`, `FeatureGatesConfig`, `DiagnosticsConfig`, `PolicyBundleConfig` and their associated types. Update `runtime.py` to import from the new module. ~300 lines moved.

5. **Extract policy presets to `session/policy_presets.py`** (P2, P19) -- Move `DEFAULT_DF_POLICY`, `CST_AUTOLOAD_DF_POLICY`, `SYMTABLE_DF_POLICY`, `DEV_DF_POLICY`, `PROD_DF_POLICY`, `DATAFUSION_POLICY_PRESETS`, `CACHE_PROFILES`, `SCHEMA_HARDENING_PRESETS`. ~130 lines moved.

6. **Extract diagnostics mixin** (P2, P13) -- Move `_RuntimeDiagnosticsMixin` and related helpers to `session/diagnostics_mixin.py`. ~420 lines moved.

7. **Split PolicyBundleConfig** (P4, P10, P14) -- Decompose the 50+ field struct into domain-specific policy groups. This is the most impactful structural change and should be done after steps 4-6 to minimize merge conflicts.

8. **Refactor mixins to composition** (P13) -- Replace the four mixin parents of `DataFusionRuntimeProfile` with composed collaborator fields. This depends on step 6 (diagnostics extraction) being complete first.

9. **Wrap module-level caches** (P12, P23) -- Create `SessionCacheManager` and inject it where needed. This improves testability without changing external behavior.

10. **Add delegation methods to DataFusionRuntimeProfile** (P14, P15) -- Expose commonly-needed configuration through profile methods rather than requiring multi-level struct traversal. This can be done incrementally as call sites are updated.
