# Design Review: src/datafusion_engine/session/

**Date:** 2026-02-17
**Scope:** src/datafusion_engine/session/
**Focus:** Boundaries (P1-P6), Correctness (P16-P18)
**Depth:** deep
**Files reviewed:** 40

---

## Executive Summary

The `session/` package is the most sophisticated subsystem in the Python codebase: it wraps DataFusion's `SessionContext`/`SessionConfig`/`RuntimeEnvBuilder` construction behind a rich configuration model and uses a phase-ordered installation pipeline to produce planning-ready `SessionRuntime` objects. The boundary and correctness design is largely mature — the key public surface (`DataFusionRuntimeProfile`, `DataFusionExecutionFacade`, `SessionFactory`) is cleanly separated, the config model is parsed-at-construction-time into frozen structs, and the session-context lifecycle is guarded by a deterministic cache keying scheme.

Three structural problems warrant attention. First, a parallel Python `SessionFactory` + `_apply_setting` scaffolding reimplements incremental `SessionConfig` building that DataFusion's own `SessionConfig.set()` chaining can already provide; the dual `method/key` resolution pattern is fragile and will accumulate DF52 drift. Second, the 40-installation-step pipeline in `session_context()` mixes orthogonal concerns — UDF platform, tracing, cache tables, extension parity validation — into a single imperative sequence with no formal boundary between "planning-critical" and "observability" phases, violating SRP and reducing testability. Third, the Python side duplicates significant session-orchestration logic that `rust/codeanatomy_engine/src/session/factory.rs` already implements more reliably via `SessionStateBuilder`, creating a maintenance surface that will diverge.

The highest-priority improvements are: (1) consolidate the `_apply_setting` dual-path into a single `SessionConfig.set()` call, eliminating the method-reflection pattern; (2) formalize the installation phases via the already-present `_RuntimeContextMixin._ephemeral_context_phases` as the canonical path for *all* session construction, not just ephemeral contexts; and (3) audit and decommission the Python-side planner-rule installation wiring once the Rust `install_planner_rules` / `install_codeanatomy_policy_config` path is the sole surface.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `_apply_setting` method-reflection pattern leaks DF API shape into policy layer |
| 2 | Separation of concerns | 1 | medium | medium | `session_context()` 15-step pipeline mixes planning-critical and observability concerns |
| 3 | SRP | 1 | medium | medium | `DataFusionRuntimeProfile` is config struct, factory, cache manager, and artifact recorder |
| 4 | High cohesion, low coupling | 2 | small | low | Module decomposition solid; `runtime_extensions.py` fan-out is wide but contained |
| 5 | Dependency direction | 3 | — | — | Core profile structs depend on nothing; adapters/extensions depend on core |
| 6 | Ports & Adapters | 2 | medium | medium | Planner-rule installation probes extension module by attribute name rather than via port |
| 16 | Functional core, imperative shell | 1 | large | high | Session construction is deeply imperative with side effects and mutation sprinkled throughout |
| 17 | Idempotency | 2 | small | medium | `session_context()` cache logic prevents double-build but extension installs are not individually idempotent |
| 18 | Determinism / reproducibility | 2 | medium | medium | Settings-overlay mechanism for `datafusion.runtime.*` keys is an out-of-band mutation path |

---

## Detailed Findings

### Category: Boundaries

#### P1. Information Hiding — Alignment: 2/3

**Current state:**
`context_pool.py:47-66` defines `_apply_setting()` which probes `SessionConfig` by both a named method (`with_target_partitions`, `with_batch_size`, etc.) and a fallback `set(key, value)` call. This means the mapping between DataFusion's `SessionConfig` builder methods and their string-key equivalents is encoded in `SessionFactory.build_config()` across ~15 `_apply_setting()` invocations.

**Findings:**
- `context_pool.py:47-66` (`_apply_setting`): the `method` parameter encodes a DataFusion API surface decision — the method name of `SessionConfig` — that callers shouldn't need to specify. When DF52 deprecates or renames a builder method (e.g., `with_repartition_aggregations` moving to a config key), every call site in `build_config()` must be updated.
- `context_pool.py:263-382` (`SessionFactory.build_config`): the 15-call sequence passes both `method` and `key` redundantly, double-exposing the DF API shape to the Python layer.
- `context_pool.py:76-81` (`_apply_settings_overrides`): correctly skips `datafusion.runtime.*` keys for DF ≥ 51 but uses a hard-coded sentinel `_DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION = 51`, embedding version logic inline.

**Suggested improvement:**
Consolidate all `_apply_setting()` calls to use only `config.set(key, str_value)`. DataFusion's Python bindings map `SessionConfig.set()` to the same underlying config store as the named builder methods. Remove the `method` parameter and the `getattr(config, method)` reflection path entirely. This reduces the call from `_apply_setting(config, method="with_target_partitions", key="...", value=...)` to a single `config.set("datafusion.execution.target_partitions", str(value))` guard. The named builder methods (`with_information_schema`, `with_default_catalog_and_schema`) that return structurally important session topology should remain explicit calls, not be collapsed into `set()`.

**Effort:** small
**Risk if unaddressed:** medium — DF52 renames a `SessionConfig` method → `_apply_setting` silently falls through to `set()` with no error, or silently applies the wrong key.

---

#### P2. Separation of Concerns — Alignment: 1/3

**Current state:**
`runtime.py:403-437` (`DataFusionRuntimeProfile.session_context`) is a 15-step imperative pipeline: `_build_session_context`, `_apply_url_table`, `_register_local_filesystem`, `_install_input_plugins`, `_install_registry_catalogs`, `_install_view_schema`, `_install_udf_platform`, `_install_planner_rules`, `_install_schema_registry`, `_validate_rule_function_allowlist`, `_prepare_statements`, `delta_ops.ensure_delta_plan_codecs`, `_record_extension_parity_validation`, `_install_physical_expr_adapter_factory`, `_install_tracing`, `_install_cache_tables`, `_record_cache_diagnostics`, `_cache_context`. These steps are not grouped by concern.

**Findings:**
- `runtime.py:403-437`: planning-critical steps (UDF platform, planner rules, schema registry, Delta codecs) are interleaved with observability/diagnostics steps (extension parity validation, cache diagnostics recording, tracing), making it impossible to test the pure planning path without triggering telemetry side effects.
- `runtime_context.py:64-105` (`_ephemeral_context_phases`) defines a cleaner 8-phase grouping (`filesystems`, `catalogs`, `udf_stack`, `schema_guards`, `planning_extensions`, `extension_hooks`, `observability`) but this phase model is only used for ephemeral contexts. The primary `session_context()` path in `runtime.py:403-437` does not use `_ephemeral_context_phases` — it duplicates the same steps inline.
- `runtime_extensions.py` is 1,215 lines handling: UDF platform installation, planner-rule installation, schema evolution adapter, Delta codecs, tracing, cache tables, cache diagnostics, and performance-policy recording. These are five distinct concerns sharing one file.

**Suggested improvement:**
Use `_ephemeral_context_phases` as the canonical session-construction path for all contexts, not just ephemeral ones. Refactor `session_context()` to call `_ephemeral_context_phases(ctx)` and execute each phase in order. This eliminates the inline duplication in `runtime.py:403-437` and makes phase ordering testable in isolation. Split `runtime_extensions.py` along the existing phase boundary: move UDF/planner installation into a `runtime_udf_install.py` module and move diagnostics/observability recording into a `runtime_observability.py` module.

**Effort:** medium
**Risk if unaddressed:** medium — the inline pipeline accumulates divergence from `_ephemeral_context_phases`; a new step added to one path is silently absent from the other.

---

#### P3. SRP — Alignment: 1/3

**Current state:**
`DataFusionRuntimeProfile` (`runtime.py:166-648`) inherits from six mixin classes and provides: (a) frozen configuration storage, (b) `SessionContext` construction and caching, (c) `SessionRuntime` building, (d) `ZeroRowBootstrapReport` orchestration (lines 556-626), (e) artifact recording delegation (via `_RuntimeDiagnosticsMixin`), (f) context pool management (lines 628-647), (g) Delta-specific session context construction (lines 507-532), and (h) dataset location resolution (lines 477-505). That is eight distinct responsibilities.

**Findings:**
- `runtime.py:556-626` (`run_zero_row_bootstrap_validation`): this method imports `build_semantic_execution_context` from `semantics.compile_context` directly inside `DataFusionRuntimeProfile`. This is a planning-phase orchestration concern that creates a hard upward dependency from the session config layer to the semantic compilation layer.
- `runtime.py:628-647` (`context_pool`): constructing a `DataFusionContextPool` is a separate lifecycle concern that belongs in a session-factory helper, not on the profile struct.
- `runtime.py:209-241` (`delta_ops`, `io_ops`, `catalog_ops` properties): these delegate to `RuntimeProfileDeltaOps`, `RuntimeProfileIO`, `RuntimeProfileCatalog` which is good decomposition — but then `session_context()` also calls `self.delta_ops.ensure_delta_plan_codecs(ctx)` (line 430), mixing the config struct's responsibility with operational runtime plumbing.

**Suggested improvement:**
Extract `run_zero_row_bootstrap_validation` from `DataFusionRuntimeProfile` into a standalone `bootstrap_session(profile, request)` free function in `datafusion_engine.session.bootstrap`. Extract `context_pool()` into `SessionFactory.pool(size)` or a module-level `create_context_pool(profile, size)`. Keep `DataFusionRuntimeProfile` as a frozen config struct with a single responsibility: describing what session to build.

**Effort:** medium
**Risk if unaddressed:** medium — the profile struct becomes a dumping ground for every new integration concern; the semantic-layer import in `run_zero_row_bootstrap_validation` creates a circular-import risk that is already deferred via a local import (line 584).

---

#### P4. High Cohesion, Low Coupling — Alignment: 2/3

**Current state:**
The module decomposition across 40 files is generally good. `runtime_profile_config.py` holds all frozen config structs. `context_pool.py` holds `SessionFactory` and `DataFusionContextPool`. `runtime_session.py` holds `SessionRuntime` and snapshot helpers. `runtime_extensions.py` holds extension installation.

**Findings:**
- `runtime_extensions.py` has a wide import fan-out (17 imports from other session submodules) and handles five distinct installation concerns. At 1,215 lines it exceeds the cohesion threshold.
- `runtime.py` re-exports 32 symbols from downstream submodules in its import block (lines 33-94), acting as an aggregation façade. While this avoids circular imports, it makes `runtime.py` a high-fan-in sink that complicates dependency tracing.
- `context_pool.py:20-32` imports four symbols from `delta_session_builder.py` under aliased names (e.g., `_build_delta_session_context_impl`) then re-wraps them in `_DeltaSessionBuildResult`. This thin re-wrapping adds a coupling layer with no behavioral change.

**Suggested improvement:**
Split `runtime_extensions.py` into `runtime_udf_install.py` (UDF platform, planner rules, schema evolution adapter — ~400 LOC) and `runtime_observability.py` (tracing, cache tables, diagnostics recording, parity validation — ~800 LOC). Remove the redundant re-wrap layer in `context_pool.py` — call `delta_session_builder` functions directly.

**Effort:** small
**Risk if unaddressed:** low — existing decomposition is functional; the main cost is navigational complexity.

---

#### P5. Dependency Direction — Alignment: 3/3

**Current state:**
The dependency flow is correct: `runtime_profile_config.py` (pure structs, no DF imports) → `context_pool.py` (DF SessionConfig/SessionContext) → `runtime_extensions.py` (extension-module probing) → `runtime.py` (orchestration). The `DataFusionRuntimeProfile` does not import domain-specific modules at the top level; the `semantics` import in `run_zero_row_bootstrap_validation` is deferred to a local import.

The `DataFusionRuntimeProfile` correctly inherits from `StructBaseStrict` (a serializable config base) rather than from any DataFusion runtime type.

**Suggested improvement:** N/A — principle is well-satisfied. The deferred local import of `semantics.compile_context` (noted under P3) is a workaround rather than a structural fix, but dependency direction itself is not violated.

**Effort:** —
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters — Alignment: 2/3

**Current state:**
The Ports & Adapters pattern is partially implemented. Extension capabilities (Rust UDF platform, expr planners, function factory) are probed via `extension_capabilities_report()` and attribute-based dynamic dispatch (`getattr(module, "install_tracing", None)`). This is adapter behavior but without a formal port contract.

**Findings:**
- `runtime_extensions.py:65-114` (`_resolve_tracing_context`): resolves which `SessionContext` instance to pass to the tracing installer by probing `ctx.ctx`, checking module names with string matching, and comparing `isinstance`. This is adapter code that encodes knowledge about the ABI boundary between Python and Rust DataFusion sessions — the exact kind of "vendor quirk" that should be hidden behind a port.
- `runtime_udf.py:86-115` (`_resolve_planner_rule_installers`): probes `datafusion_engine.extensions.datafusion_ext` for four specific callables by attribute name. If the extension module is absent or partially present, the function returns `None` and installation is silently skipped. There is no contract type that `datafusion_ext` is expected to conform to.
- `runtime_extensions.py:996-1004` (`_install_physical_expr_adapter_factory`): uses `getattr(ctx, "register_physical_expr_adapter_factory", None)` to probe whether the session context supports the adapter factory API. This is the correct fail-soft behavior but lacks a formal interface.

**Suggested improvement:**
Define a `PlannerExtensionPort` protocol in `session/protocols.py` with four methods: `install_config`, `install_physical_config`, `install_rules`, `install_physical_rules`. Require `datafusion_ext` to provide a `get_planner_extension_port() -> PlannerExtensionPort` function instead of bare attribute access. Similarly, define a `TracingPort` protocol and require `datafusion_ext` to expose `get_tracing_port() -> TracingPort`. This converts fragile attribute probing into a validated capability contract.

**Effort:** medium
**Risk if unaddressed:** medium — attribute-name coupling to extension modules means a module rename silently disables planner rules or tracing without error.

---

### Category: Correctness

#### P16. Functional Core, Imperative Shell — Alignment: 1/3

**Current state:**
The session construction pipeline (`session_context()`, lines 403-437) is almost entirely imperative: it mutates `ctx` in place through 15 sequential `_install_*` calls, each of which may additionally write to global caches (`SESSION_CONTEXT_CACHE`, `RUNTIME_SETTINGS_OVERLAY`), record artifacts, and install Rust extensions. There is no functional core that could be tested without triggering these side effects.

**Findings:**
- `_session_caches.py:9-11`: three module-level mutable globals (`SESSION_CONTEXT_CACHE: dict[str, SessionContext]`, `SESSION_RUNTIME_CACHE: dict[str, object]`, `RUNTIME_SETTINGS_OVERLAY: WeakKeyDictionary`). These are process-global state that makes session construction non-idempotent from the test perspective and prevents isolated testing.
- `runtime_extensions.py:154-197` (`_install_udf_platform`): installs UDF platform, then conditionally records a snapshot artifact, then calls `register_udfs_via_ddl`, then calls `_refresh_udf_catalog`. Four distinct side effects in one function, none of which can be tested without a live `SessionContext`.
- `runtime_session.py:223-268` (`build_session_runtime`): calls `profile.session_context()` (which triggers the full installation pipeline), then re-reads `rust_udf_snapshot` from the built context, then queries `information_schema.df_settings` via SQL. There is no pure-function path to compute a `SessionRuntime` from a snapshot.
- `context_pool.py:225-239` (`cleanup_ephemeral_objects`): uses `ctx.sql("SHOW TABLES")` to discover run-scoped tables to deregister. This embeds a live SQL query inside a cleanup operation, making it impossible to test without a real session.

**Suggested improvement:**
The full pipeline cannot be made purely functional given DataFusion's mutation model, but the key change is to separate the "compute what should be installed" step from "install it". Specifically: (a) introduce a `SessionInstallPlan` dataclass that captures which extensions/UDFs/rules are enabled, produced from the profile alone without touching a context; (b) move all artifact recording out of `_install_*` functions into a post-installation `_record_session_installation(profile, ctx, plan)` pass; (c) replace the three module-level globals with an injectable `SessionCache` abstraction that defaults to the process-global dicts but can be replaced in tests. This makes the planning-critical installation testable via a mock `SessionContext` and confines observability writes to a single post-installation call.

**Effort:** large
**Risk if unaddressed:** high — the imperative pipeline with global state makes unit testing prohibitively complex; bugs in extension installation are only caught at integration level.

---

#### P17. Idempotency — Alignment: 2/3

**Current state:**
`session_context()` uses `_cached_context()` / `_cache_context()` to return the same context on repeated calls for profiles with `share_context=True`. This prevents double-construction. However, individual `_install_*` steps are not idempotent in isolation.

**Findings:**
- `runtime_extensions.py:200-240` (`_install_planner_rules`): calls `installers.config_installer(ctx, ...)`, `installers.physical_config_installer(ctx, ...)`, `installers.rule_installer(ctx)`, `installers.physical_installer(ctx)`. If called twice on the same context (e.g., after a cache miss with a colliding cache key), Rust-side rules may be registered twice. There is no guard.
- `runtime_extensions.py:813-836` (`_install_cache_tables`): calls `_register_cache_introspection_functions(ctx)`. No guard against double-registration.
- `context_pool.py:186-212` (`DataFusionContextPool.checkout`): the pool re-uses the same `SessionContext` across calls and cleans up run-scoped tables via `cleanup_ephemeral_objects`. If `cleanup_ephemeral_objects` raises (e.g., `SHOW TABLES` fails), the context is returned to the pool in a potentially dirty state via the unconditional `self._queue.append(ctx)` in the `finally` block.

**Suggested improvement:**
For `_install_planner_rules` and `_install_cache_tables`, add a context-keyed sentinel stored in `WeakKeyDictionary` (similar to `RUNTIME_SETTINGS_OVERLAY`) to guard against double-installation. For `context_pool.py`, catch exceptions from `cleanup_ephemeral_objects` and log a warning rather than silently returning a dirty context; optionally rebuild the context slot from scratch when cleanup fails.

**Effort:** small
**Risk if unaddressed:** medium — duplicate rule registration may silently corrupt plan semantics; dirty contexts in the pool cause non-reproducible test failures.

---

#### P18. Determinism / Reproducibility — Alignment: 2/3

**Current state:**
The profile uses content-based cache keys (`fingerprint()`) to derive `context_cache_key()`, ensuring that profiles with the same configuration share the same `SessionContext`. `session_runtime_hash()` in `runtime_session.py:319-344` hashes UDF snapshot, planner names, rewrite tags, and DF settings into a stable identity string used for plan fingerprinting.

**Findings:**
- `runtime_session.py:155-165` (`_runtime_settings_from_profile`): `datafusion.runtime.*` keys are filtered from `settings_payload()` and injected as synthetic rows into the `information_schema.df_settings` snapshot — because DataFusion 51+ does not surface these settings via SQL. This means the session-runtime hash includes settings that were not actually applied to the context (they were applied via `RuntimeEnvBuilder`, not `SessionConfig`). If a runtime setting's effective value differs from the profile value (e.g., due to OS-level resource limits), the hash is still computed from the profile value. This is a reproducibility gap.
- `context_pool.py:222` (`next_run_prefix`): uses `int(time.time() * 1000)` to generate a run-scoped prefix. This is deliberately nondeterministic for isolation, which is correct — but the prefix is also used in `cleanup_ephemeral_objects` to identify tables to deregister. If the timestamp changes mid-run (e.g., millisecond boundary), a table created with `prefix_1706000000001` may not be cleaned up by a cleanup pass using `prefix_1706000000000`.
- `runtime_session.py:306-316` (`_session_runtime_entries`): sorts settings by key before hashing, which is correct deterministic behavior.

**Suggested improvement:**
For the `datafusion.runtime.*` settings gap: capture the `RuntimeEnvBuilder`'s effective configuration immediately after construction (before `SessionContext` creation) and include a hash of it in `session_runtime_hash`. DataFusion's `RuntimeEnvBuilder` exposes the configured parameters; serialize them to a stable dict before `build()`. For `next_run_prefix`: separate the "generate prefix" step (done at checkout time) from the "cleanup by prefix" step (done at return time) and pass the prefix explicitly rather than regenerating it.

**Effort:** medium
**Risk if unaddressed:** medium — plan cache hits may occur for sessions whose effective runtime environment differs from the cached hash; cleanup failures may leave orphaned tables in the pool.

---

## Cross-Cutting Themes

### Theme 1: Imperative Installation Pipeline Without Phase Contracts

**Root cause:** `session_context()` predates the `_ephemeral_context_phases` mechanism. When it was written, all installation was inline. `_ephemeral_context_phases` was added later to support ephemeral contexts but never retrofitted to the primary path.

**Affected principles:** P2, P3, P16, P23 (testability — not in scope but implied)

**Approach:** Adopt `_ephemeral_context_phases` as the sole construction path. Remove the inline sequence from `session_context()`. Each phase function becomes a self-contained, testable unit.

---

### Theme 2: Dual Python/Rust Session Construction Surfaces

**Root cause:** The Python `SessionFactory` and the Rust `session/factory.rs` implement overlapping functionality: `SessionConfig` construction, `RuntimeEnvBuilder` assembly, rule installation, and UDF registration. The Rust side uses `SessionStateBuilder` (the canonical DF builder pattern) while the Python side uses incremental `SessionConfig.set()` chaining.

**Affected principles:** P1, P3, P16

**Approach:** The Rust `SessionFactory.build()` path via `SessionStateBuilder` is the more reliable construction surface and should be the authority for sessions that go through `datafusion_ext`. Audit which Python installation steps have Rust equivalents in `rust/codeanatomy_engine/src/session/` and progressively route through the Rust factory. The Python side should be reduced to: (a) config serialization from profile to a JSON payload, and (b) calling the Rust factory with that payload.

---

### Theme 3: Settings Overlay as an Out-of-Band Mutation Path

**Root cause:** `datafusion.runtime.*` settings cannot be set via `SessionConfig.set()` in DF 51+ (they apply to `RuntimeEnvBuilder` only). The workaround records these settings in `RUNTIME_SETTINGS_OVERLAY` (a `WeakKeyDictionary[SessionContext, dict]`) so they can be included in `session_runtime_hash`. This creates a third global state registry alongside `SESSION_CONTEXT_CACHE` and `SESSION_RUNTIME_CACHE`.

**Affected principles:** P16, P17, P18

**Approach:** Eliminate `RUNTIME_SETTINGS_OVERLAY` by capturing `RuntimeEnvBuilder` parameters directly at construction time into `SessionRuntime.runtime_env_settings: Mapping[str, str]`. Derive this from `DataFusionRuntimeProfile.runtime_env_builder()` before the context is built, not by post-hoc overlay injection.

---

## Planning-Object Consolidation

### Bespoke Python Code That DataFusion's Built-In Objects Can Replace

#### 1. `_apply_setting()` dual-path method + key resolution

**File:** `context_pool.py:47-66`, `context_pool.py:263-382`

**What it does:** Tries a named `SessionConfig` builder method via `getattr`, falls back to `SessionConfig.set(key, value)`.

**DF replacement:** `SessionConfig.set(key, str(value))` is the canonical path for all configuration settings. The named builder methods (`with_target_partitions`, `with_batch_size`, etc.) are convenience wrappers that ultimately call `config.set()`. The Python bindings expose `SessionConfig.set()` directly. Replace all 12 `_apply_setting()` calls with direct `config.set()` calls where the key is known. Keep explicit calls for schema/catalog topology methods (`with_default_catalog_and_schema`, `with_information_schema`) since these have structural implications.

**LOC reduction estimate:** ~120 lines removed from `_apply_setting` + callsites.

---

#### 2. Custom settings-snapshot introspection via `information_schema.df_settings`

**File:** `runtime_session.py:411-431` (`settings_snapshot_for_profile`)

**What it does:** Queries `information_schema.df_settings` via SQL, then merges in `datafusion.runtime.*` keys from the profile (since they don't appear in DF 51+), then sorts the result.

**DF replacement:** In DF52, `session_context.session_state().config()` (Rust) or the Python equivalent exposes the full config as a structured object. In DF51/Python bindings, `information_schema.df_settings` is the best available path. The current implementation is correct given binding constraints. However, the `_merge_runtime_settings_rows` manual merge (~40 lines) can be simplified once `RuntimeEnvBuilder` parameters are captured at build time (see Theme 3 above).

**Assessment:** Partial opportunity — keep the SQL-based snapshot but eliminate the overlay merge.

---

#### 3. Custom plan caching (`PlanCache`, `PlanProtoCache`) in `DataFusionRuntimeProfile`

**File:** `runtime.py:202-203`, `facade.py:459-474`

**What it does:** Maintains a custom `PlanCache` and `PlanProtoCache` on the profile to cache Substrait plan bytes and proto-encoded plans by `plan_identity_hash`.

**DF52 replacement:** DF52 introduces `FileStatisticsCache` and `DefaultListFilesCache` for metadata caching, accessible via `session_context.statistics_cache()` and `session_context.list_files_cache()`. These are file-metadata caches, not plan-bytes caches. The custom plan cache (for Substrait serialization) has no DF52 built-in equivalent — DF's `df.cache()` caches result batches in memory, not plan bytes. The custom `PlanCache` / `PlanProtoCache` should be retained.

**Assessment:** No consolidation opportunity; the custom plan caches serve a distinct purpose from DF52's metadata caches.

---

#### 4. Custom `DataFusionContextPool` cleanup via `SHOW TABLES` + `deregister_table`

**File:** `context_pool.py:225-239` (`cleanup_ephemeral_objects`)

**What it does:** Runs `SHOW TABLES` via SQL, filters by run prefix, calls `deregister_table` for each match.

**DF replacement:** There is no built-in DF pool cleanup API. However, the `SHOW TABLES` approach is fragile (see P17 finding). An alternative is to track registered names in the pool's own `deque` item — store `(SessionContext, set[str])` in the pool rather than `SessionContext` alone, accumulating table names at registration time and deregistering by name at cleanup. This eliminates the `SHOW TABLES` SQL dependency.

**Assessment:** Worthwhile but not a DF built-in replacement — it is a design improvement.

---

#### 5. Planner-rule installation via `_resolve_planner_rule_installers` attribute probing

**File:** `runtime_udf.py:86-115`, `runtime_extensions.py:200-240`

**What it does:** Dynamically discovers `datafusion_ext` module, probes for four callable attributes, calls them sequentially to install config, physical config, rules, and physical rules.

**DF52 replacement:** DF52's `RelationPlanner` and `ExprPlanner` extension hooks provide first-class extension points for custom FROM-clause constructs and expression planning. The DF52 `add_analyzer_rule` / `add_optimizer_rule` / `add_physical_optimizer_rule` APIs on `SessionContext` (Rust) and `SessionStateBuilder` (Rust) are the canonical optimizer rule injection surfaces. The Python binding does not expose `add_optimizer_rule` directly, but the Rust `SessionStateBuilder` path in `rust/codeanatomy_engine/src/session/factory.rs` already uses `apply_to_builder()` which wraps these canonical APIs. The Python side's attribute-probing path is a workaround for the Python binding's limited rule-injection surface and should be migrated to the Rust factory path as Python bindings mature.

**Assessment:** Medium-term migration target. DF52's `RelationPlanner`/`ExprPlanner` hooks in particular should replace the bespoke `expr_planner_hook` callable in `PolicyBundleConfig`.

---

## DF52 Migration Impact

### Breaking Changes Affecting This Scope

#### 1. `CoalesceBatchesExec` Removed

**Impact:** Any code that inspects or references `CoalesceBatchesExec` in physical plan trees (e.g., in plan artifact serialization, explain parsing, or Substrait replay compatibility) will encounter plan-shape differences in DF52. The `facade.py` Substrait replay path (`replay_substrait_bytes` via `datafusion_engine.plan.result_types`) may produce different physical plans for the same Substrait bytes.

**Affected files:** `facade.py:432-457` (`_substrait_first_df`), `plan/bundle_artifact.py` (out of scope but related).

**Action:** Verify that Substrait round-trips remain stable across `CoalesceBatches` removal by adding a plan-fingerprint regression test comparing DF51 and DF52 physical plan outputs for identical Substrait bytes.

---

#### 2. `explain_analyze_level` Config Key

**Affected file:** `context_pool.py:138-146` (`_apply_explain_analyze_level`), `runtime_compile.py:240-243` (`_supports_explain_analyze_level`).

**Current state:** `_supports_explain_analyze_level()` checks `DATAFUSION_MAJOR_VERSION >= 51`. In DF52 this will correctly return `True`. The config key `datafusion.explain.analyze_level` should remain valid.

**Action:** Verify the key name is unchanged in DF52 configuration documentation. No code change needed if key is stable.

---

#### 3. FFI Provider Signature Change (DF52 Section K)

**Impact:** DF52 requires FFI provider constructors to accept a `TaskContextProvider` (typically `SessionContext`) and optionally a `LogicalExtensionCodec`. The Delta plan codec installation path (`runtime_extensions.py:1007-1028`, `_install_delta_plan_codecs_extension`) calls an `install_delta_plan_codecs(ctx)` function from `datafusion_ext`. If `datafusion_ext` upgrades to DF52, this call signature may need to pass a `LogicalExtensionCodec` argument.

**Affected files:** `runtime_extensions.py:1007-1028` (`_install_delta_plan_codecs_extension`).

**Action:** Confirm with `datafusion_ext` wheel that `install_delta_plan_codecs` accepts the DF52 FFI constructor signature. Add a capability check before calling if the signature may vary.

---

### DF52 Opportunities

#### 1. `RelationPlanner` / `ExprPlanner` as Canonical Extension Points

DF52 formalizes `RelationPlanner` (FROM-clause extensibility) and `ExprPlanner` (expression-level extensibility) as first-class hooks. The current `expr_planner_hook` callable in `PolicyBundleConfig` (`runtime_profile_config.py:476`) and `install_expr_planners` in `runtime_extensions.py:935-972` are ad-hoc wrappers around what DF52 now supports natively. Once Python bindings expose `add_relation_planner` / the equivalent of `ExprPlanner` registration, `_install_expr_planners` can be replaced by a direct API call, eliminating the `ImportError`-guarded extension probe.

**Estimated LOC reduction:** ~80 lines from `_install_expr_planners` + `_install_function_factory`.

---

#### 2. DF52 `FileStatisticsCache` / `DefaultListFilesCache`

DF52's new metadata caches (`statistics_cache()`, `list_files_cache()`) address listing-table scalability. The `_snapshot_metadata_caches` function in `runtime_extensions.py:737-770` uses a bespoke `snapshot_datafusion_caches(ctx)` call. In DF52, these caches are first-class session-accessible objects. Audit whether `snapshot_datafusion_caches` can be replaced by `ctx.statistics_cache()` and `ctx.list_files_cache()` introspection.

---

## Rust Migration Candidates

### Candidate 1: `SessionFactory.build_config()` → Rust `SessionStateBuilder`

**Current Python:** `context_pool.py:263-382` — 120-line method building `SessionConfig` by sequential `set()` and builder calls.

**Rust equivalent:** `rust/codeanatomy_engine/src/session/factory.rs` already implements `SessionStateBuilder`-based construction with the same config parameters. The Rust path uses `SessionStateBuilder::new().with_config(config).with_runtime_env(env)` which is the canonical DF51/52 pattern.

**Rationale:** The Rust factory is more reliable for extension registration (optimizer rules, physical optimizer rules, codec registration) because it operates at `SessionState` level before context wrapping. The Python `_apply_setting()` method-reflection pattern has no Rust equivalent needed — `SessionConfig` is constructed deterministically in Rust.

**Migration approach:** Serialize `DataFusionRuntimeProfile` config subfields to a JSON/msgpack payload and pass to a Rust-side `build_session_from_profile(payload: &str) -> PyResult<SessionContext>` PyO3 function. The Python `DataFusionRuntimeProfile._build_session_context()` becomes a single Rust call.

**Estimated complexity:** large — requires PyO3 binding for the factory call and careful ABI versioning of the payload format.

---

### Candidate 2: Planner Rule Installation → Rust `apply_to_builder`

**Current Python:** `runtime_extensions.py:200-240` (`_install_planner_rules`) — probes `datafusion_ext` for four callables, calls them sequentially.

**Rust equivalent:** `rust/codeanatomy_engine/src/session/planning_surface.rs` (`apply_to_builder`) applies `CpgRuleSet` analyzer/optimizer/physical optimizer rules to `SessionStateBuilder` before context construction.

**Rationale:** Installing rules post-construction (`SessionContext` is already built) is less reliable than pre-construction (`SessionStateBuilder`). The Rust `apply_to_builder` path guarantees rules are present at first plan.

**Migration approach:** Move rule installation from post-context to within the Rust `SessionFactory.build()` path. The Python `_install_planner_rules` becomes a no-op when `enable_delta_session_defaults=True` (which already routes through the Rust factory).

**Estimated complexity:** medium — requires extending the `enable_delta_session_defaults` pathway to include all rule packages, not just Delta codecs.

---

### Candidate 3: `build_session_runtime` UDF Snapshot → Rust

**Current Python:** `runtime_session.py:223-268` (`build_session_runtime`) — calls `rust_udf_snapshot(ctx)` (a Rust FFI call) and then does Python-side sorting, hashing, and caching.

**Rust equivalent:** `rust/codeanatomy_engine/src/session/envelope.rs` (likely) manages session state snapshots at the Rust level.

**Rationale:** The UDF snapshot is already a Rust artifact (`rust_udf_snapshot` returns a Rust-derived dict). Computing `udf_snapshot_hash` by serializing this back to Python dicts, msgpack-encoding, and hashing in Python is a round-trip. A Rust-side `session_runtime_hash(ctx)` function would be more efficient and more reliable (no Python dict serialization path).

**Estimated complexity:** medium — requires adding a `session_runtime_hash` PyO3 function to `codeanatomy_engine`.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P1 / P17 | Replace `_apply_setting(method=..., key=...)` dual-path with `config.set(key, value)` directly | small | Eliminates DF API fragility and reduces `build_config()` by ~60 lines |
| 2 | P17 | Add `WeakKeyDictionary` installation sentinels to `_install_planner_rules` and `_install_cache_tables` to prevent double-install | small | Prevents silent rule duplication in pool-reused contexts |
| 3 | P18 | Eliminate `RUNTIME_SETTINGS_OVERLAY` by capturing `RuntimeEnvBuilder` params before `SessionContext` construction | medium | Closes the reproducibility gap for `datafusion.runtime.*` settings in plan hashes |
| 4 | P2 | Route primary `session_context()` construction through `_ephemeral_context_phases` instead of inline 15-step sequence | medium | Unifies the two construction paths; makes phase ordering testable |
| 5 | P6 | Define `PlannerExtensionPort` protocol in `session/protocols.py` and require `datafusion_ext` to expose it | medium | Replaces fragile attribute probing with a validated capability contract |

---

## Recommended Action Sequence

1. **[P1]** Remove `_apply_setting` `method` parameter. Replace all 12 call sites in `SessionFactory.build_config()` with direct `config.set(key, str(value))` calls. Keep `with_default_catalog_and_schema()`, `with_information_schema()`, `with_create_default_catalog_and_schema()` as explicit named calls.

2. **[P17]** Add double-installation guards for `_install_planner_rules` and `_install_cache_tables` using a `WeakKeyDictionary[SessionContext, bool]` sentinel at module level in `runtime_extensions.py`.

3. **[P2 + P3]** Refactor `DataFusionRuntimeProfile.session_context()` to call `_ephemeral_context_phases(ctx)` and execute each phase. Delete the inline step sequence. This immediately unifies the two construction paths.

4. **[P3]** Extract `run_zero_row_bootstrap_validation` from `DataFusionRuntimeProfile` into a free function `run_session_bootstrap_validation(profile, request, ctx)` in `datafusion_engine.session.bootstrap`. Remove the `semantics.compile_context` local import from `runtime.py`.

5. **[P6]** Define `PlannerExtensionPort(Protocol)` in `session/protocols.py`. Update `_resolve_planner_rule_installers` to return `PlannerExtensionPort | None` instead of `_PlannerRuleInstallers | None`, enforcing the contract via `isinstance`.

6. **[P18]** Capture `RuntimeEnvBuilder` parameter dict at `DataFusionRuntimeProfile.runtime_env_builder()` call time and store it on `SessionRuntime`. Include a hash of it in `session_runtime_hash()`. Remove `RUNTIME_SETTINGS_OVERLAY` and `_merge_runtime_settings_rows`.

7. **[P16 + Rust]** Expand `enable_delta_session_defaults=True` path in `SessionFactory._build_local_context()` to also run rule installation via `_build_delta_session_context` (i.e., `apply_to_builder` on the Rust side), eliminating the post-construction `_install_planner_rules` call for this path.

8. **[P16 / Rust migration]** Once step 7 is proven stable, evaluate removing `_install_planner_rules` from the non-Delta path and requiring `datafusion_ext` for rule installation, using the Rust `SessionStateBuilder` path exclusively.
