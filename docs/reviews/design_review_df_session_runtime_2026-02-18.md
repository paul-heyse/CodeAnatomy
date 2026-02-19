# Design Review: DataFusion Session, Schema, and Extensions Layer

**Date:** 2026-02-18
**Scope:** `src/datafusion_engine/session/`, `src/datafusion_engine/bootstrap/`, `src/datafusion_engine/schema/`, `src/datafusion_engine/extensions/`, `src/datafusion_engine/encoding/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 82 Python files

---

## Executive Summary

The DataFusion session and schema layer is architecturally mature and has made deliberate, well-reasoned tradeoffs to manage the complexity of bridging Python to a Rust/DataFusion backend. The dominant design strength is a rigorously fingerprintable, frozen `DataFusionRuntimeProfile` struct that provides clear session identity and determinism guarantees. The most significant structural concerns are: (1) a large mixin-inheritance stack on `DataFusionRuntimeProfile` that splits behavioral cohesion across six files while sharing one public `self`; (2) two classes of module-level mutable global state — one unsafe (`_DDL_CATALOG_WARNING_STATE`) and two idempotency-safe (`WeakKeyDictionary` guards) — that violate P11 (CQS) and P16 (functional core); (3) three independently-maintained bool-coercion variants that should all delegate to `utils.value_coercion.coerce_bool`; and (4) a `SessionFactory.build_config()` method at 130+ lines of sequential `if/then` construction that should be decomposed into named sub-builders. The schema introspection sub-package is genuinely well-structured, and the extension adapter pattern in `datafusion_ext.py` exemplifies good Ports & Adapters design.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `_coerce_required_bool` and several `_` prefixed helpers leaked into `runtime.py` imports |
| 2 | Separation of concerns | 2 | medium | low | `DataFusionRuntimeProfile.__post_init__` mixes validation, cache init, and side-effect enforcement |
| 3 | SRP | 2 | medium | low | `runtime_extensions.py` owns UDF install, tracing install, cache tables, parity checks, and artifact recording |
| 4 | High cohesion, low coupling | 2 | medium | medium | Six-mixin class inheritance creates cohesion illusion; lateral coupling through `_as_runtime_profile` cast |
| 5 | Dependency direction | 2 | small | low | `runtime.py` imports from `serde_artifact_specs` at module bottom to break a cycle — cycle still exists |
| 6 | Ports & Adapters | 3 | — | — | `datafusion_ext.py` is a textbook adapter; `PlannerExtensionPort` protocol is well-defined |
| 7 | DRY | 1 | small | medium | Three independent bool-coercion implementations across `scip/config.py`, `file_pruning.py`, `extraction/options.py` |
| 8 | Design by contract | 2 | small | low | `DataFusionRuntimeProfile.__post_init__` validates well; `_build_session_context` swallows failure silently |
| 9 | Parse, don't validate | 2 | small | low | `_set_config_if_supported` uses bare `BaseException` catch on config-key probing |
| 10 | Make illegal states unrepresentable | 2 | small | low | `DiagnosticsConfig` holds mutable collector objects in a `frozen=True` msgspec struct |
| 11 | CQS | 1 | small | high | `session_context()`, `build_session_runtime()`, and `_refresh_udf_catalog()` return values AND mutate caches |
| 12 | Dependency inversion + explicit composition | 2 | medium | low | `SessionFactory.build_config()` reads profile fields directly; no abstraction seam for config sources |
| 13 | Prefer composition over inheritance | 1 | medium | medium | Six-mixin chain on `DataFusionRuntimeProfile` — `_RuntimeContextMixin`, `_RuntimeDiagnosticsMixin`, `_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`, `_RuntimeProfileDeltaFacadeMixin`, `_RuntimeProfileIdentityMixin` |
| 14 | Law of Demeter | 2 | small | low | `SessionFactory.build_config()` accesses `profile.execution.target_partitions`, `profile.policies.cache_policy`, `profile.catalog.default_catalog` — three-hop chains |
| 15 | Tell, don't ask | 2 | small | low | `runtime_ops.py:336-408` extracts raw values from location then applies policies externally |
| 16 | Functional core, imperative shell | 1 | medium | high | `_DDL_CATALOG_WARNING_STATE` global dict mutated as side effect in installation logic; `build_session_runtime()` mutates `profile.session_runtime_cache` |
| 17 | Idempotency | 2 | small | low | `_PLANNER_RULES_INSTALLED` and `_CACHE_TABLES_INSTALLED` WeakKeyDictionary guards make installation idempotent; `reserve_delta_commit` accumulates side-effectful state correctly |
| 18 | Determinism / reproducibility | 3 | — | — | Fingerprint pipeline is rigorous; `session_runtime_hash()` covers UDF snapshot, settings, and planner names |
| 19 | KISS | 2 | medium | low | `runtime.py` deferred-import at module bottom (line 690-693) is a necessary but confusing workaround |
| 20 | YAGNI | 3 | — | — | No speculative generality observed; all abstractions have clear active second uses |
| 21 | Least astonishment | 2 | small | medium | `session_context()` returns a cached context but silently creates a new one if `diagnostics_sink` is set; surprising cache-bypass behavior |
| 22 | Declare and version public contracts | 2 | small | low | `session/` `__init__.py` exports only three symbols; many useful public types are not in `__all__` |
| 23 | Design for testability | 2 | medium | medium | Module-level mutable state (`_DDL_CATALOG_WARNING_STATE`, `_BOUND_DELTA_SERVICES`) requires test isolation teardown |
| 24 | Observability | 3 | — | — | Structured artifact recording is comprehensive; OTel spans on compile/execute paths; telemetry payload covers all profile dimensions |

---

## Detailed Findings

### Category: Boundaries (P1-P6)

#### P1. Information hiding — Alignment: 2/3

**Current state:**
The session package exposes a clean public surface via `session/__init__.py`, which exports only `DataFusionExecutionFacade`, `DataFusionRuntimeProfile`, and `RuntimeProfileConfig` via a lazy-loader. However, the primary implementation file `runtime.py` imports and re-exports several private helpers with leading underscores directly into its namespace.

**Findings:**
- `src/datafusion_engine/session/runtime.py:36-38` imports `_effective_catalog_autoload_for_profile`, `_resolved_config_policy_for_profile`, and `_resolved_schema_hardening_for_profile` — all private symbols — from `runtime_config_policies.py` into `runtime.py`'s namespace, making them indirectly callable from callers who have a reference to the `runtime` module.
- `src/datafusion_engine/session/runtime.py:28-29` imports the private `_supports_explain_analyze_level` from `runtime_compile`, then wraps it in a `@staticmethod` on the profile class itself. The private helper is accessible as `DataFusionRuntimeProfile._supports_explain_analyze_level`.
- `src/datafusion_engine/session/runtime_context.py:49-55` defines `_as_runtime_profile()` using `cast()` to project the mixin's `self` to the concrete profile type. This is a necessary workaround for the mixin design but is fragile — any mixin reuse outside `DataFusionRuntimeProfile` would silently produce an invalid cast.

**Suggested improvement:**
Private helpers used solely as implementation detail of `DataFusionRuntimeProfile` should be fully enclosed in the module where they are defined and not imported into the profile's own namespace. The `_supports_explain_analyze_level` wrapper on the profile class should be a module-level function in `runtime_compile.py` called directly by consumers. The `_as_runtime_profile` cast pattern is a design smell arising from the mixin stack; consolidating behaviour onto the concrete class would eliminate the need for it.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns — Alignment: 2/3

**Current state:**
`DataFusionRuntimeProfile.__post_init__` in `runtime.py:332-364` performs three distinct categories of work: input validation (`_validate_information_schema`, `_validate_catalog_names`), cache object initialisation (`_resolve_plan_cache`, `_resolve_plan_proto_cache`), and async-UDF policy enforcement (`_validate_async_udf_policy`). This mixes policy validation with object construction side-effects.

**Findings:**
- `src/datafusion_engine/session/runtime.py:332-364` — `__post_init__` calls `_validate_information_schema()`, `_validate_catalog_names()`, `_resolve_plan_cache()`, `_resolve_plan_proto_cache()`, `_resolve_diagnostics_sink()`, and `_validate_async_udf_policy()` — six distinct concerns in one method.
- `src/datafusion_engine/session/runtime_context.py:106-130` — `_install_catalogs_for_context` calls `_install_input_plugins`, `_install_registry_catalogs`, and `_install_view_schema` in sequence. This is IO-side-effect orchestration mixed into what is otherwise a mixin providing pure configuration behaviour.
- `src/datafusion_engine/session/runtime_diagnostics_mixin.py:58-107` — The `settings_payload()` method builds the settings dict by calling `_resolved_config_policy_for_profile`, `cache_policy_settings`, `_cache_profile_settings`, `_runtime_settings_payload`, `performance_policy_settings`, and `_extra_settings_payload` — six sub-computations aggregated in a single method. This is acceptable but would be cleaner if each sub-domain had an authoritative builder.

**Suggested improvement:**
Extract the three validation calls from `__post_init__` into a single `_validate()` private method, and the three cache-init calls into a `_initialize_caches()` method. This preserves ordering while making the three concerns legible and separately testable.

**Effort:** small
**Risk if unaddressed:** low

---

#### P3. SRP — Alignment: 2/3

**Current state:**
`runtime_extensions.py` is the most prominent SRP violation in the layer. It owns: UDF-platform installation, planner-rule installation, schema-registry validation, function-factory installation, expression-planner installation, physical-expression-adapter installation, tracing installation, cache-table installation, parity-validation reporting, delta-plan-codec installation, and multiple artifact recording helpers. By the "one reason to change" criterion this file has at least seven distinct reasons to change.

**Findings:**
- `src/datafusion_engine/session/runtime_extensions.py:126-207` — `_install_udf_platform()` is 82 lines mixing UDF platform installation, DDL catalog registration, UDF catalog refresh, and platform-level artifact recording.
- `src/datafusion_engine/session/runtime_extensions.py:855-907` — `_install_tracing()` resolves an extension module, adapts context types, installs tracing, and records artifacts.
- `src/datafusion_engine/session/runtime_extensions.py:613-646` — `_record_extension_parity_validation()` calls three validation functions and records three distinct artifact types in one method — mixing command and query concerns.
- `src/datafusion_engine/session/facade.py:238` — `except Exception` is used broadly inside `compile_to_bundle` (justified by the comment "DataFusion backend errors are dynamic") but this should be documented more explicitly and narrowed where possible.

**Suggested improvement:**
Split `runtime_extensions.py` into at minimum: `_install_udf_stack.py` (UDF platform, function factory, expr planners), `_install_session_hooks.py` (tracing, physical expr adapter, delta codecs), and retain `runtime_extensions.py` as a thin orchestration module that calls the sub-modules. This would also resolve the 1200+ line file length.

**Effort:** medium
**Risk if unaddressed:** low (cognitive cost only)

---

#### P4. High cohesion, low coupling — Alignment: 2/3

**Current state:**
`DataFusionRuntimeProfile` inherits from six mixins: `_RuntimeProfileIdentityMixin`, `_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`, `_RuntimeProfileDeltaFacadeMixin`, `_RuntimeDiagnosticsMixin`, and `_RuntimeContextMixin`. These mixins share the same `self` reference, use `_as_runtime_profile(self)` casts to access the concrete type, and produce a class that is hard to reason about in isolation. The coupling is low between the mixins themselves but artificially high between each mixin and the full `DataFusionRuntimeProfile` type.

**Findings:**
- `src/datafusion_engine/session/runtime.py:158-166` — The MRO is six levels deep before reaching `StructBaseStrict`. Any method resolution requires mentally walking all six classes.
- `src/datafusion_engine/session/runtime_context.py:49-55` — `_as_runtime_profile()` is a structural cast used in every mixin that needs to call profile-specific methods. This is coupling in disguise — each mixin is implicitly tightly coupled to the exact concrete class.
- `src/datafusion_engine/session/runtime_ops.py:440-508` — Three `_RuntimeProfileXFacadeMixin` classes each contain only one or two methods and delegate immediately to a helper dataclass (`RuntimeProfileIO`, `RuntimeProfileCatalog`). The delegation dataclasses already exist; the mixins add a redundant indirection layer.

**Suggested improvement:**
The delegation dataclasses `RuntimeProfileDeltaOps`, `RuntimeProfileIO`, and `RuntimeProfileCatalog` already have good cohesion. The corresponding facade mixins are a redundant layer. Consider exposing these dataclasses as typed properties on `DataFusionRuntimeProfile` (which already does this for `delta_ops`, `io_ops`, `catalog_ops`) and removing the mixin inheritance in favour of direct property delegation. The `_RuntimeDiagnosticsMixin` and `_RuntimeContextMixin` are heavier and can remain as-is or be refactored more incrementally.

**Effort:** medium
**Risk if unaddressed:** medium (maintenance cost scales with the team's growth)

---

#### P5. Dependency direction — Alignment: 2/3

**Current state:**
`runtime.py` contains a deferred bottom-of-file import from `serde_artifact_specs` (lines 690-693) to break a circular import chain. The comment in the file is honest about the reason. This is a symptom of `DataFusionRuntimeProfile` being too central — it knows about artifact specs (`SEMANTIC_PROGRAM_MANIFEST_SPEC`, `ZERO_ROW_BOOTSTRAP_VALIDATION_SPEC`) that should be the concern of a higher-level orchestration layer.

**Findings:**
- `src/datafusion_engine/session/runtime.py:690-693` — Bottom-of-file import of `SEMANTIC_PROGRAM_MANIFEST_SPEC` and `ZERO_ROW_BOOTSTRAP_VALIDATION_SPEC`. These serde specs are produced by a layer (`serde_artifact_specs`) that in turn depends on session types, creating a cycle. The deferred import is the current resolution.
- `src/datafusion_engine/session/runtime.py:584-654` — `run_zero_row_bootstrap_validation()` reaches into `semantics.compile_context` and constructs `SemanticProgramManifest`. The session runtime profile knows about the semantic layer, inverting the expected dependency direction (semantics should depend on session, not vice versa).
- `src/datafusion_engine/session/runtime_ops.py:37-38` — `_EXTENSION_MODULE_NAMES` is hardcoded as `("datafusion_engine.extensions.datafusion_ext",)`. This is a module-level policy constant inside a module that should not need to know the extension module path — this belongs in the extension adapter layer itself.

**Suggested improvement:**
`run_zero_row_bootstrap_validation()` on `DataFusionRuntimeProfile` is a convenience method that inverts dependencies. Move this responsibility to a standalone function in `datafusion_engine/bootstrap/zero_row.py` that takes a profile as an argument. This eliminates the need for the deferred `serde_artifact_specs` import.

**Effort:** medium
**Risk if unaddressed:** low (cycle is managed; risk is ongoing maintenance complexity)

---

#### P6. Ports & Adapters — Alignment: 3/3

**Current state:**
The extension adapter design in `datafusion_ext.py` is a textbook implementation of the Ports & Adapters pattern. It defines explicit required entrypoints (`REQUIRED_RUNTIME_ENTRYPOINTS`), handles the primary/fallback module pattern cleanly, normalises `SessionContext` wrappers transparently, and raises clear `AttributeError` for missing entrypoints.

**Findings:**
- `src/datafusion_engine/extensions/datafusion_ext.py:16-72` — The `_load_internal_module()` function cleanly implements a primary/stub fallback with explicit validation of required entrypoints. The `IS_STUB` export makes the adapter's state inspectable.
- `src/datafusion_engine/session/protocols.py` — Four `Protocol` classes (`RuntimeSettingsProvider`, `RuntimeTelemetryProvider`, `RuntimeArtifactRecorder`, `PlannerExtensionPort`) define clean port contracts for injection.
- `src/datafusion_engine/extensions/context_adaptation.py` — The `resolve_extension_module` and `invoke_entrypoint_with_adapted_context` functions provide a reusable adapter invocation seam shared by `schema_evolution.py` and `datafusion_ext.py`.

**Suggested improvement:** None. This is the strongest part of the layer.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Knowledge (P7-P11)

#### P7. DRY — Alignment: 1/3

**Current state:**
Three independently-maintained bool-coercion implementations exist across the codebase. Each handles the string-to-bool case differently, creating divergent semantics that will drift over time.

**Findings:**

Implementation 1 — `src/utils/value_coercion.py:84-115` (`coerce_bool`):
- Handles `None`, `bool`, `int`, and strings `{"true","1","yes","on","y"}` / `{"false","0","no","off","n",""}`.
- Returns `None` on failure; raises `TypeError` if `default` is provided.

Implementation 2 — `src/extract/extractors/scip/config.py:14-17` (`_coerce_bool`):
- Handles only `bool` type; returns `default` for everything else. Does not handle integer or string forms.

Implementation 3 — `src/storage/deltalake/file_pruning.py:422-427` (`_coerce_bool_value`):
- Handles `str` (`== "true"` only), `bool`, `int`, `float`. Returns `None` on failure.
- String comparison is case-sensitive; `"True"` would return `False`. This is a bug compared to the canonical implementation.

- `src/extraction/options.py:234-239` (`_coerce_bool_with_default`) wraps `coerce_bool` correctly but adds a redundant `None` check. This wrapper should not exist; `coerce_bool(value, default=default, label=field_name)` already raises `TypeError` when `default` is provided and coercion fails.

**Suggested improvement:**
All three private implementations should be deleted and their call sites updated to use `utils.value_coercion.coerce_bool`. Specifically:
- `scip/config.py:_coerce_bool` → `coerce_bool(value, default=default)`
- `file_pruning.py:_coerce_bool_value` → `coerce_bool(value)` (returns `None` on failure, consistent with its current behaviour except for the case-sensitivity bug)
- `extraction/options.py:_coerce_bool_with_default` → inline call to `coerce_bool(value, default=default, label=field_name)`

**Effort:** small
**Risk if unaddressed:** medium (semantic divergence will cause silent incorrect behaviour in the file-pruning case, where `"True"` currently resolves to `False`)

---

#### P8. Design by contract — Alignment: 2/3

**Current state:**
`DataFusionRuntimeProfile.__post_init__` enforces two explicit preconditions (`_validate_information_schema`, `_validate_catalog_names`) and one policy invariant (`_validate_async_udf_policy`) raising `ValueError` on violation. This is good contract enforcement. However, some helper functions swallow exceptions broadly, degrading downstream contract enforceability.

**Findings:**
- `src/datafusion_engine/session/runtime_session.py:101-104` — `_build_session_runtime_from_context` wraps `rust_udf_snapshot` in `except (RuntimeError, TypeError, ValueError)` and silently falls back to `_empty_udf_snapshot_payload()`. A planning session built on an empty UDF snapshot will produce incorrect plan fingerprints. The fallback should at minimum log a warning with the caught exception.
- `src/datafusion_engine/session/context_pool.py:39-58` — `_set_config_if_supported` catches `BaseException`, which is broader than `Exception`. This catches `KeyboardInterrupt` and `SystemExit`. The intent is to catch config-key-not-found errors; `Exception` is the correct bound.
- `src/datafusion_engine/session/runtime_context.py:157-169` — `_install_view_schema` swallows `(KeyError, RuntimeError, TypeError, ValueError)` from `ctx.catalog()` and returns silently. If a catalog is mis-configured, this produces a silent no-op instead of an explicit diagnostic.

**Suggested improvement:**
For `_build_session_runtime_from_context`, change the bare fallback to call `logger.warning("UDF snapshot unavailable: %s", exc)` before using the empty payload. For `_set_config_if_supported`, narrow `BaseException` to `Exception`. For `_install_view_schema`, record a diagnostic via the profile artifact system when the catalog lookup fails.

**Effort:** small
**Risk if unaddressed:** low (failures surface later with less context)

---

#### P9. Parse, don't validate — Alignment: 2/3

**Current state:**
The profile configuration structs (`ExecutionConfig`, `CatalogConfig`, etc.) are `frozen=True` msgspec structs with typed fields — this is good parsing. Configuration values enter as structured Python types rather than raw strings. Session config key application in `context_pool.py` uses a probe-and-ignore pattern that is effectively validation-not-parsing.

**Findings:**
- `src/datafusion_engine/session/context_pool.py:39-58` — `_set_config_if_supported` attempts `config.set(key, value)` and catches the exception to detect unsupported keys. This is a validate-by-probing pattern. A structured capability set (available keys list) queried once at startup would be cleaner, but this is constrained by the DataFusion API not exposing a key-enumeration API.
- `src/datafusion_engine/session/runtime_compile.py:131-140` — `_coerce_required_bool` applies bool coercion to kwargs values rather than parsed typed inputs. This is a parse step happening inside a function body rather than at the boundary (profile construction). The kwargs it processes come from the extension module, which returns untyped `dict` — so this is an appropriate boundary parse.
- `src/datafusion_engine/session/runtime_session.py:63-69` — `_settings_rows_to_mapping` explicitly probes three candidate field names (`name`, `setting_name`, `key`) to handle schema variability from different DataFusion versions. This is parse-then-use applied correctly.

**Suggested improvement:**
The probe-and-ignore pattern in `_set_config_if_supported` is acceptable given the DataFusion API constraint. However, the catch should be `Exception` not `BaseException`. The dual-field-name fallback in `_settings_rows_to_mapping` is already cleanly encapsulated.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable — Alignment: 2/3

**Current state:**
The `frozen=True` msgspec structs for all config sub-objects (`ExecutionConfig`, `CatalogConfig`, `DiagnosticsConfig`, etc.) enforce immutability at the type level. However, `DataFusionRuntimeProfile` itself carries mutable cache objects (`session_context_cache`, `session_runtime_cache`, `delta_commit_runs`, `udf_catalog_cache`) and mutable diagnostics collectors (`_DataFusionExplainCollector`, `_DataFusionPlanCollector`) inside a `frozen=True` type.

**Findings:**
- `src/datafusion_engine/session/runtime.py:196-204` — The profile declares `udf_catalog_cache: WeakKeyDictionary[SessionContext, UdfCatalog]`, `session_context_cache: dict[str, SessionContext]`, `session_runtime_cache: dict[str, object]`, `runtime_settings_overlay: WeakKeyDictionary[...]`, and `delta_commit_runs: dict[str, DataFusionRun]`. These are all mutable containers inside a `frozen=True` struct. This is a common pattern with msgspec frozen structs, but the frozen annotation is misleading — the struct's identity is immutable, its state is not.
- `src/datafusion_engine/session/runtime_profile_config.py:427-438` — `DiagnosticsConfig` contains `explain_collector: _DataFusionExplainCollector | None` and `plan_collector: _DataFusionPlanCollector | None`. These collectors are mutated in place. A `frozen=True` struct that contains mutable collector objects cannot be safely shared across threads or cloned via `msgspec.structs.replace`.
- `src/datafusion_engine/session/runtime.py:357-360` — `__post_init__` resets `session_context_cache`, `session_runtime_cache`, and `runtime_settings_overlay` using `object.__setattr__`. This is the documented workaround for msgspec frozen structs, but it signals that the caching state should not live inside the frozen struct.

**Suggested improvement:**
Separate the immutable configuration from the mutable runtime caches by introducing a `SessionRuntimeState` dataclass (non-frozen) that holds `session_context_cache`, `session_runtime_cache`, `runtime_settings_overlay`, and `delta_commit_runs`. `DataFusionRuntimeProfile` remains frozen and holds an explicit reference to `SessionRuntimeState`. This makes the "what is frozen" invariant accurate and makes the mutable state explicit.

**Effort:** large (affects many call sites through the mixin stack)
**Risk if unaddressed:** medium (thread-safety issues would be hard to diagnose; `frozen` annotation misleads readers into believing the struct is truly immutable)

---

#### P11. CQS — Alignment: 1/3

**Current state:**
Several key functions both return information and mutate caches as a combined operation. This makes them unsafe to call in purely observational contexts and makes the caching behaviour invisible to callers.

**Findings:**
- `src/datafusion_engine/session/runtime.py:439-461` — `session_context()` returns a `SessionContext` AND populates `self.session_context_cache` (via `_cache_context`). Callers reading the context also trigger cache population as a side effect.
- `src/datafusion_engine/session/runtime_session.py:228-273` — `build_session_runtime()` populates `profile.session_runtime_cache[cache_key]` AND returns the built `SessionRuntime`. A function named `build_` should be a command; alternatively a function that returns a value should be a pure query. The hybrid is problematic.
- `src/datafusion_engine/session/runtime_session.py:276-308` — `refresh_session_runtime()` similarly writes to `profile.session_runtime_cache` and returns the runtime.
- `src/datafusion_engine/session/runtime_extensions.py:256-296` — `_refresh_udf_catalog()` writes to `profile.udf_catalog_cache[cache_key]` with no return value. This is correctly a command, but its name implies a refresh (idempotent re-read) not an unconditional write.

**Suggested improvement:**
For `session_context()`, split into a query `get_session_context() -> SessionContext | None` (returns from cache or None) and a command `ensure_session_context() -> SessionContext` (creates and caches). For `build_session_runtime()`, the command form should be `ensure_session_runtime()` with no return value, and a separate `get_session_runtime() -> SessionRuntime | None` should query the cache. This is a significant refactor but directly enables safe observational calls in diagnostics and test contexts.

**Effort:** medium
**Risk if unaddressed:** high (callers who intend to observe state are silently triggering cache-population side effects; test isolation is complicated)

---

### Category: Composition (P12-P15)

#### P12. Dependency inversion + explicit composition — Alignment: 2/3

**Current state:**
Most dependencies are injected through the `DataFusionRuntimeProfile` constructor. The `SessionFactory` pattern correctly encapsulates context construction. However, `SessionFactory.build_config()` directly reads from `profile.execution`, `profile.catalog`, `profile.policies`, and `profile.features` via attribute chains, binding it concretely to the profile's layout.

**Findings:**
- `src/datafusion_engine/session/context_pool.py:256-385` — `SessionFactory.build_config()` is 130 lines of sequential `if/then` config-key applications. Each block reads a raw profile sub-field and conditionally applies it. This is correct functionally but couples the factory tightly to the profile's internal field layout. Adding a new execution field requires adding another conditional block here.
- `src/datafusion_engine/session/runtime_ops.py:37-38` — `_EXTENSION_MODULE_NAMES = ("datafusion_engine.extensions.datafusion_ext",)` is hardcoded. The extension module path should be passed in via the profile's policy bundle rather than hardcoded in the implementation module.
- `src/datafusion_engine/session/runtime_extensions.py:70-71` — `_PLANNER_RULES_INSTALLED` and `_CACHE_TABLES_INSTALLED` are module-level `WeakKeyDictionary` instances. Their state is implicit and not injectable. In tests, installation state from prior tests may persist if the same `SessionContext` object is reused.

**Suggested improvement:**
Decompose `SessionFactory.build_config()` into named sub-functions, one per concern: `_apply_execution_settings(config, execution)`, `_apply_catalog_settings(config, catalog)`, `_apply_policy_settings(config, policies)`. Each sub-function takes only the config sub-object it needs, breaking the direct coupling to the full profile shape. The extension module name should be a field on `PolicyBundleConfig`.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P13. Prefer composition over inheritance — Alignment: 1/3

**Current state:**
`DataFusionRuntimeProfile` uses six-level mixin inheritance rather than composition. The three delegation dataclasses (`RuntimeProfileDeltaOps`, `RuntimeProfileIO`, `RuntimeProfileCatalog`) that already exist as properties on the profile class represent the correct composition pattern — but they are redundantly wrapped in three additional facade mixins (`_RuntimeProfileDeltaFacadeMixin`, `_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`).

**Findings:**
- `src/datafusion_engine/session/runtime.py:158-166` — MRO: `DataFusionRuntimeProfile > _RuntimeProfileIdentityMixin > _RuntimeProfileIOFacadeMixin > _RuntimeProfileCatalogFacadeMixin > _RuntimeProfileDeltaFacadeMixin > _RuntimeDiagnosticsMixin > _RuntimeContextMixin > StructBaseStrict`.
- `src/datafusion_engine/session/runtime_ops.py:456-508` — `_RuntimeProfileIOFacadeMixin.cache_root()` is a one-line delegation to `_runtime_profile_io_ops(self).cache_root()`. This delegation adds a method name, a `getattr` call, and a type check for a single-line underlying method. The same outcome is achieved by calling `profile.io_ops.cache_root()` directly, which is already available via the `io_ops` property on the profile.
- `src/datafusion_engine/session/runtime_ops.py:470-493` — `_RuntimeProfileCatalogFacadeMixin.dataset_location()` is a `@staticmethod` that simply calls `dataset_resolver.location(name)` — effectively ignoring the fact that it is on a mixin at all. It does not forward to `self`, making the mixin membership semantically meaningless for this method.

**Suggested improvement:**
Remove `_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`, and `_RuntimeProfileDeltaFacadeMixin`. The underlying behaviour is already exposed through `self.delta_ops`, `self.io_ops`, and `self.catalog_ops` properties. Any caller using `profile.cache_root()` can be updated to `profile.io_ops.cache_root()`. The `_RuntimeDiagnosticsMixin` and `_RuntimeContextMixin` cannot be removed as easily because they contain non-trivial methods, but they could eventually be converted to composed helper objects.

**Effort:** medium
**Risk if unaddressed:** medium (mixin inheritance obscures what the profile actually does; every new feature must decide which mixin to add to)

---

#### P14. Law of Demeter — Alignment: 2/3

**Current state:**
Multi-hop attribute chains are common throughout the session layer, particularly in `SessionFactory.build_config()` and the mixin methods. These are structurally motivated by the profile's deep config hierarchy.

**Findings:**
- `src/datafusion_engine/session/context_pool.py:290-295` — `profile.catalog.default_catalog`, `profile.catalog.default_schema`, `profile.catalog.enable_information_schema` — three-hop chains that couple `SessionFactory` to the nested layout of `CatalogConfig`.
- `src/datafusion_engine/session/runtime_ops.py:108-109` — `self.profile.execution.delta_max_spill_size`, `self.profile.execution.delta_max_temp_directory_size` — reaching through profile to execution to a nested field.
- `src/datafusion_engine/session/runtime_context.py:139-149` — `self.catalog.registry_catalog_name or self.catalog.default_catalog`, `self.catalog.registry_catalogs`, `self.catalog.default_schema` — three fields accessed via `self.catalog`.

**Suggested improvement:**
Each `Config` struct (`ExecutionConfig`, `CatalogConfig`, `PolicyBundleConfig`) should expose summary methods for the combinations that are most commonly accessed. For example, `CatalogConfig.effective_catalog_name() -> str` would collapse `registry_catalog_name or default_catalog` into one place. This is consistent with the existing `PolicyBundleConfig.cache_policy_group()`, `sql_policy_group()`, and `delta_policy_group()` pattern which already demonstrates this correctly.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask — Alignment: 2/3

**Current state:**
`RuntimeProfileCatalog.dataset_location()` in `runtime_ops.py:350-408` extracts sub-fields from a `DatasetLocation`, applies policies, and reassembles a new location — a classic ask-then-reassemble pattern.

**Findings:**
- `src/datafusion_engine/session/runtime_ops.py:370-408` — The method extracts `location`, checks `location.format`, checks `policies.delta_store_policy`, `policies.scan_policy`, calls `apply_scan_policy_defaults`, then manually reassembles `DatasetLocationOverrides`. This logic belongs on `DatasetLocation` itself or in a `apply_profile_policies(location, profile)` standalone function rather than in the catalog ops class.
- `src/datafusion_engine/session/runtime_ops.py:337-348` — `extract_dataset_location()` conditionally enriches a location with a `dataset_spec` by calling the extract registry, then using `msgspec.structs.replace`. This is also ask-then-reconstruct.

**Suggested improvement:**
The dataset policy application logic in `dataset_location()` should be moved to a standalone `apply_profile_policies_to_location(location, profile)` function in `datafusion_engine/dataset/policies.py`, making it a composable transformation rather than catalog-class behaviour.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (P16-P18)

#### P16. Functional core, imperative shell — Alignment: 1/3

**Current state:**
The most significant structural correctness concern in the layer is the placement of mutable global state in what is otherwise a well-structured imperative-shell design. Two issues stand out: the `_DDL_CATALOG_WARNING_STATE` dict and the `_BOUND_DELTA_SERVICES` `WeakKeyDictionary`.

**Findings:**
- `src/datafusion_engine/session/runtime_extensions.py:69` — `_DDL_CATALOG_WARNING_STATE: dict[str, bool] = {"emitted": False}` is a process-level singleton that records whether a warning has been emitted. This is a side effect embedded in UDF-platform installation logic (`_install_udf_platform`, lines 197-202). Because it is a module-level dict, it persists across tests and cannot be reset without directly mutating the module.
- `src/datafusion_engine/session/runtime_ops.py:38` — `_BOUND_DELTA_SERVICES: WeakKeyDictionary[object, object] = WeakKeyDictionary()` is a process-level registry for delta services. Its use of `WeakKeyDictionary` is appropriate (prevents cycles), but the `bind_delta_service` / `_resolve_delta_service` pair creates an implicit global dependency that is not visible in `DataFusionRuntimeProfile`'s constructor signature.
- `src/datafusion_engine/session/runtime_session.py:272` — `profile.session_runtime_cache[cache_key] = runtime` — a cache mutation inside `build_session_runtime()`, which is also a query that returns the runtime. This embeds an imperative-shell concern (caching) inside a core computation function.

**Suggested improvement:**
Replace `_DDL_CATALOG_WARNING_STATE` with a local flag passed via parameter or a returned diagnostic event rather than a module-level state mutation. For `_BOUND_DELTA_SERVICES`, make the delta service an explicit optional field on `DataFusionRuntimeProfile` (or `PolicyBundleConfig`) so the dependency is visible at construction time. The cache mutation in `build_session_runtime` should be separated as described under P11.

**Effort:** medium
**Risk if unaddressed:** high (test isolation failures; process-level state contamination in multi-profile scenarios)

---

#### P17. Idempotency — Alignment: 2/3

**Current state:**
The planner-rules and cache-table installation idempotency guards (`_PLANNER_RULES_INSTALLED`, `_CACHE_TABLES_INSTALLED`) correctly use `WeakKeyDictionary[SessionContext, bool]` so re-running installation with the same context is a no-op. Delta commit sequencing via `reserve_delta_commit` / `finalize_delta_commit` is correctly designed to produce monotonically increasing version IDs.

**Findings:**
- `src/datafusion_engine/session/runtime_extensions.py:219-252` — `_install_planner_rules` checks `_PLANNER_RULES_INSTALLED.get(ctx)` before installing. This is idempotent for the same context object. Score: correct.
- `src/datafusion_engine/session/runtime_ops.py:141-192` — `reserve_delta_commit` creates a new run context on first call and returns the next monotonic version. Re-running with the same key increments the sequence. This is correctly idempotent in the "safe to retry" sense.
- `src/datafusion_engine/session/runtime_context.py:399-402` — `_cache_context` overwrites any existing cache entry with a new `SessionContext` if `share_context=True`. If a caller calls `session_context()` twice with a profile where the cache key matches but the context has been evicted externally, a new context is installed silently. This is generally correct but could surprise callers who retain a reference to the old context.

**Suggested improvement:** Minor. Consider logging a debug-level message when `_cache_context` overwrites an existing entry.

**Effort:** small
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility — Alignment: 3/3

**Current state:**
The determinism infrastructure is the strongest aspect of this layer. Every profile produces a stable `fingerprint()` via `_RuntimeDiagnosticsMixin.fingerprint_payload()` that covers settings hash, telemetry hash, execution config, catalog config, feature gates, and policy bundle. `session_runtime_hash()` incorporates UDF snapshot hash, rewrite tags, domain planner names, and DataFusion settings — exactly the dimensions that affect plan reproducibility.

**Findings:**
- `src/datafusion_engine/session/runtime_session.py:324-349` — `session_runtime_hash()` builds a deterministic payload covering six dimensions and passes it through `payload_hash()`. This is comprehensive.
- `src/datafusion_engine/session/runtime_profile_config.py:222-269` — `ExecutionConfig.fingerprint_payload()` lists every field explicitly. No fields are omitted.
- `src/datafusion_engine/session/runtime_diagnostics_mixin.py:109-122` — `settings_hash()` combines `settings_payload()` with a version tag. This guarantees that any change to the applied settings produces a different hash.

**Suggested improvement:** None. This is well-executed.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Simplicity (P19-P22)

#### P19. KISS — Alignment: 2/3

**Current state:**
The most complex single artefact is `SessionFactory.build_config()` at approximately 130 lines. It is sequential and mechanical rather than algorithmically complex, but its length makes it difficult to navigate and extend. The deferred bottom-of-file import in `runtime.py` is technically necessary but adds cognitive overhead.

**Findings:**
- `src/datafusion_engine/session/context_pool.py:256-385` — `SessionFactory.build_config()` applies ten distinct execution settings, one catalog config, one config policy, one cache policy, one performance policy, one schema hardening profile, two overrides maps, one feature-gates application, one join-policy application, and one explain-level application — all in a single flat function. This is not algorithmically complex but is dense.
- `src/datafusion_engine/session/runtime.py:680-693` — The deferred import comment and import block at the bottom of the file is a pragmatic solution to a circular import, but it is unusual enough to require a long explanatory comment. New contributors will find this confusing.
- `src/datafusion_engine/session/runtime_extensions.py` — 1239 lines. The length alone is a KISS signal: this file contains too much.

**Suggested improvement:**
`SessionFactory.build_config()` should be decomposed into named helper functions (`_apply_execution_config(config, execution: ExecutionConfig)`, `_apply_catalog_config(config, catalog: CatalogConfig)`, etc.). The circular import should be resolved by moving the methods that require `serde_artifact_specs` out of `DataFusionRuntimeProfile` into a higher-level orchestration module.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P20. YAGNI — Alignment: 3/3

**Current state:**
No speculative abstractions observed. All protocols (`RuntimeSettingsProvider`, `RuntimeTelemetryProvider`, `RuntimeArtifactRecorder`) have concrete current implementations. `DataFusionViewRegistry` is used by the profile. The extension entrypoint validation is directly exercised.

**Suggested improvement:** None.

---

#### P21. Least astonishment — Alignment: 2/3

**Current state:**
The `session_context()` cache-bypass behaviour when `diagnostics_sink` is set is a hidden semantic rule that will surprise callers. The docstring on `session_context()` documents the caching but does not mention the diagnostics-sink bypass.

**Findings:**
- `src/datafusion_engine/session/runtime_context.py:394-397` — `_cached_context()` returns `None` (forcing a fresh context creation) when `self.diagnostics.diagnostics_sink is not None`. This means two calls to `session_context()` on the same profile with a diagnostics sink configured will return two different `SessionContext` objects. This is intentional (the comment explains the reason: caching would skip diagnostics recording) but it is surprising behaviour for a method named `session_context()`.
- `src/datafusion_engine/session/runtime.py:463-475` — `build_ephemeral_context()` creates a context that goes through the full registration phase but is not cached. Its name (`ephemeral`) makes this clear. However, `io_ops.ephemeral_context()` (in `runtime_ops.py:281-291`) calls `build_ephemeral_context()` and THEN re-runs the orchestration phases, potentially doubling registration work.
- `src/datafusion_engine/session/runtime_session.py:373-374` — `_sql_with_options` uses `read_only_sql_options()` as its default when `sql_options` is `None`. This is correct but the name `_sql_with_options` suggests the options are always supplied — the default semantics are not captured in the name.

**Suggested improvement:**
Add a note to the `session_context()` docstring: "When `diagnostics_sink` is configured, each call creates a new context; the context cache is bypassed." For `io_ops.ephemeral_context()`, verify that double-registration is not occurring and add a comment if the second phase call is intentional.

**Effort:** small
**Risk if unaddressed:** medium (callers holding profile references with diagnostics_sink will get unexpected multiple contexts)

---

#### P22. Declare and version public contracts — Alignment: 2/3

**Current state:**
The `session/__init__.py` `__all__` and lazy-loader export map correctly exposes three stable symbols. However, several widely-used types (`SessionRuntime`, `DataFusionViewRegistry`, `SessionFactory`, `DataFusionContextPool`) are not exported from any package `__init__.py` and are imported directly from sub-modules by consumers.

**Findings:**
- `src/datafusion_engine/session/__init__.py:14-18` — `__all__` lists only `DataFusionExecutionFacade`, `DataFusionRuntimeProfile`, `RuntimeProfileConfig`. `SessionRuntime` (used by plan-bundle builders), `SessionFactory` (used by test setup), and `DataFusionContextPool` are all widely-consumed but not part of the declared surface.
- `src/datafusion_engine/schema/__init__.py` — Not read, but likely has a similar issue given that `SchemaIntrospector`, `SchemaContract`, and schema derivation functions are used directly from sub-module paths by many consumers.
- `src/datafusion_engine/session/runtime_profile_config.py:61-78` — The `__all__` list is explicit and current for that module, which is good practice.

**Suggested improvement:**
Add `SessionRuntime`, `SessionFactory`, `DataFusionContextPool`, and `DataFusionViewRegistry` to `session/__init__.py`'s `__all__` and lazy-loader map. Add a comment distinguishing "stable public API" from "semi-public implementation" exports.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (P23-P24)

#### P23. Design for testability — Alignment: 2/3

**Current state:**
`DataFusionRuntimeProfile` is constructable without a live DataFusion session — the session is created lazily. Configuration is injectable. However, the module-level state in `runtime_extensions.py` and `runtime_ops.py` creates test isolation problems.

**Findings:**
- `src/datafusion_engine/session/runtime_extensions.py:69` — `_DDL_CATALOG_WARNING_STATE = {"emitted": False}` is not resetable without importing and mutating the module dict. A test that exercises DDL catalog registration will permanently disable the warning emission for subsequent tests in the same process.
- `src/datafusion_engine/session/runtime_ops.py:38` — `_BOUND_DELTA_SERVICES: WeakKeyDictionary` is a process-level service registry. Tests that bind a delta service must ensure the profile objects are garbage-collected to clean up, or actively call `bind_delta_service(profile, service=None)`.
- `src/datafusion_engine/session/runtime_extensions.py:70-71` — `_PLANNER_RULES_INSTALLED` and `_CACHE_TABLES_INSTALLED` have correct WeakKeyDictionary semantics; they clean up when the `SessionContext` is GC'd, so they do not create test isolation issues if tests create fresh contexts.
- `src/datafusion_engine/session/runtime_session.py:92-123` — `_build_session_runtime_from_context` silently falls back to an empty UDF snapshot. A test that expects UDF-based behaviour will pass with the wrong snapshot without any assertion failure.

**Suggested improvement:**
Introduce a `reset_installation_state()` function (private, test-only, documented as such) that clears `_DDL_CATALOG_WARNING_STATE`. Alternatively, convert `_DDL_CATALOG_WARNING_STATE` to a class-level attribute on a singleton class that can be reset in `setUp`. For `_BOUND_DELTA_SERVICES`, provide a context manager `bound_delta_service(profile, service)` for test use that automatically cleans up.

**Effort:** small
**Risk if unaddressed:** medium (test pollution in CI; flaky tests when test execution order changes)

---

#### P24. Observability — Alignment: 3/3

**Current state:**
The observability infrastructure is thorough. Every significant operation produces structured artifact payloads. OTel spans cover the compile and execute paths in `facade.py`. The telemetry payload (`telemetry_payload()`, `telemetry_payload_v1()`) covers all profile dimensions. The view artifact registry (`DataFusionViewRegistry`) provides reproducibility evidence.

**Findings:**
- `src/datafusion_engine/session/facade.py:216-262` — `compile_to_bundle` wraps in an OTel span with `db.system.name`, `db.operation.name`, `plan_kind`, `plan_fingerprint`, and `duration_s`. This is well-structured.
- `src/datafusion_engine/session/runtime_extensions.py:613-646` — `_record_extension_parity_validation` produces structured payloads for `DATAFUSION_EXTENSION_PARITY_SPEC`, `DATAFUSION_RUNTIME_CAPABILITIES_SPEC`, and `PERFORMANCE_POLICY_SPEC`. Each payload is typed via an artifact spec.
- `src/datafusion_engine/session/runtime_diagnostics_mixin.py:155-200+` — `telemetry_payload()` covers execution settings, catalog settings, data-source templates, feature gates, policy bundles, and schema hardening.

**Suggested improvement:** None. The observability layer is comprehensive and well-aligned to architectural boundaries.

---

## Cross-Cutting Themes

### Theme 1: Query-Command Confusion at the Session Cache Layer
The caching pattern used by `session_context()`, `build_session_runtime()`, and `udf_catalog()` conflates "retrieve and initialise if absent" (a command) with "return current value" (a query). This pattern — sometimes called "lazy initialisation" — is a well-known source of CQS violations. It affects P11, P16, and P23. The root cause is that mutable cache state lives inside the frozen profile struct, which is the same issue driving P10. Resolving P10 (extracting caches into a separate `SessionRuntimeState`) would allow CQS-clean methods on the profile: `profile.state.get_context()` (query) and `profile.state.ensure_context(factory)` (command). This is the highest-leverage structural fix.

### Theme 2: Mixin Inheritance Masking Composition
The six-mixin class hierarchy on `DataFusionRuntimeProfile` is technically correct but architecturally misleading. The underlying composition pattern already exists: `delta_ops`, `io_ops`, and `catalog_ops` properties return typed delegation objects. The three facade mixins (`_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`, `_RuntimeProfileDeltaFacadeMixin`) are a redundant indirection layer that exists for historical reasons. Removing them would have zero behaviour change but would reduce the MRO to three levels and eliminate the `_as_runtime_profile` cast pattern. This affects P4, P13, and partially P3.

### Theme 3: Module-Level Global State as Process-Lifetime Singletons
Three module-level mutable objects exist: `_DDL_CATALOG_WARNING_STATE` (unsafe for test isolation), `_BOUND_DELTA_SERVICES` (implicit global dependency), and the extension module import cache in `datafusion_ext.py` (`_INTERNAL`, `_FALLBACK_INTERNAL`). The extension cache is correctly designed (module-load time, immutable after that). The other two are process-lifetime mutable state that violates P16 and P23. They can be resolved without large restructuring.

### Theme 4: Bool Coercion DRY Violation Across Packages
Three independent bool-coercion implementations exist in `scip/config.py`, `file_pruning.py`, and `extraction/options.py`. The canonical implementation in `utils/value_coercion.py` already handles all cases correctly. The divergence is a DRY violation that has already produced one observable bug (`file_pruning.py` is case-sensitive while `value_coercion.coerce_bool` is not). This is a small, high-confidence fix.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 | Delete `scip/config.py:_coerce_bool`, `file_pruning.py:_coerce_bool_value`, replace with `utils.value_coercion.coerce_bool` | small | Eliminates case-sensitivity bug; consolidates semantics |
| 2 | P8 | Change `context_pool.py:_set_config_if_supported` catch from `BaseException` to `Exception` | small | Prevents swallowing `KeyboardInterrupt`/`SystemExit` |
| 3 | P21 | Document cache-bypass behaviour in `session_context()` docstring when `diagnostics_sink` is set | small | Prevents caller confusion about multiple-context creation |
| 4 | P22 | Add `SessionRuntime`, `SessionFactory`, `DataFusionContextPool`, `DataFusionViewRegistry` to `session/__init__.py` `__all__` | small | Clarifies stable public surface; reduces deep import paths |
| 5 | P12 | Decompose `SessionFactory.build_config()` into `_apply_execution_config`, `_apply_catalog_config`, `_apply_policy_config` sub-functions | medium | Reduces 130-line monolith; improves targeted testability |

---

## Recommended Action Sequence

1. **[P7] Fix bool-coercion DRY violation.** Delete the three private implementations, redirect call sites to `utils.value_coercion.coerce_bool`. Run the file-pruning tests to confirm the case-sensitivity fix. This is risk-free and delivers immediate correctness improvement.

2. **[P8, P9] Narrow broad exception catches.** Change `BaseException` to `Exception` in `_set_config_if_supported`. Add a `logger.warning` in `_build_session_runtime_from_context` when the UDF snapshot falls back to empty. These are one-line changes.

3. **[P21, P22] Improve public contract documentation.** Document the diagnostics-sink cache-bypass in `session_context()`. Add missing symbols to `session/__init__.py` exports. No behaviour changes.

4. **[P11, P16] Isolate `_DDL_CATALOG_WARNING_STATE` global.** Convert from a module-level dict to a parameter passed down through the UDF installation call chain, or convert to a process-local context variable (`contextvars.ContextVar`). This eliminates the test isolation issue without restructuring.

5. **[P3, P19] Decompose `SessionFactory.build_config()`.** Extract the ten conditional execution-config blocks into `_apply_execution_config(config, execution: ExecutionConfig) -> SessionConfig`, and similarly for catalog and policy. Reduces cognitive load and enables targeted testing.

6. **[P13, P4] Remove redundant facade mixins.** After confirming all callers of `profile.cache_root()`, `profile.delta_service()`, and `profile.dataset_location()` (the mixin-exposed methods) are updated to use `profile.io_ops.cache_root()`, `profile.delta_ops.delta_service()`, and `profile.catalog_ops.dataset_location()` respectively, remove the three facade mixins from the MRO. This is a pure refactor with no semantics change.

7. **[P10, P11] Extract mutable cache state.** This is the highest-impact but highest-effort item. Introduce `SessionRuntimeState` as a separate mutable dataclass to hold `session_context_cache`, `session_runtime_cache`, `runtime_settings_overlay`, and `delta_commit_runs`. `DataFusionRuntimeProfile` retains a reference to it. This resolves P10 (frozen annotation becomes accurate), P11 (CQS-clean method separation becomes possible), and partially P16 (functional core / imperative shell).

8. **[P5] Move `run_zero_row_bootstrap_validation` out of the profile.** Relocate this method to `datafusion_engine/bootstrap/zero_row.py` as a standalone function. This removes the upward dependency from `DataFusionRuntimeProfile` to the semantic layer, eliminates the deferred bottom-of-file import, and respects the intended dependency direction.
