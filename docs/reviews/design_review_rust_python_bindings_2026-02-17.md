# Design Review: Rust Python Bindings

**Date:** 2026-02-17
**Scope:** `rust/datafusion_python/`, `rust/codeanatomy_engine_py/`, `rust/datafusion_ext_py/`
**Focus:** Boundaries (1-6), Simplicity (19-22), Composition (12-15)
**Depth:** moderate
**Files reviewed:** 20 (representative sample from 100+ total; prioritised context.rs, dataframe.rs, functions.rs, expr.rs, codeanatomy_ext/*, engine_py/*)

---

## Executive Summary

The three crates form a well-structured layered stack: `datafusion_python` forks upstream datafusion-python 51.0.0 and adds the `codeanatomy_ext` module as a lateral extension; `datafusion_ext_py` provides a thin re-export mirror; `codeanatomy_engine_py` wraps the Rust engine as a clean 6-class facade. The layer discipline is sound — business logic lives in `codeanatomy_engine` and `datafusion_ext`, not in binding code. The most important finding is a **DF52 blocking gap**: the entire stack is pinned at datafusion 51 and several DF52 breaking changes (`FileSource` projection pushdown removal, FFI provider `session` parameter requirement, `CoalesceBatchesExec` removal, `DFSchema` field-ref API change) must be resolved before upstream can be adopted. A secondary finding is the **pervasive per-call `Runtime::new()` pattern** in `codeanatomy_ext/helpers.rs` and `session_utils.rs`, which creates and tears down a Tokio runtime on every Delta operation call — an unnecessary overhead and a potential footgun under load.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `ctx` field public on PySessionContext; `codeanatomy_ext` helpers expose session extraction |
| 2 | Separation of concerns | 3 | — | — | Engine logic cleanly in Rust core; binding layer is pure wiring |
| 3 | SRP | 2 | medium | medium | `context.rs` (1178 LOC) conflates session config, table registration, SQL execution, DataFrame creation, catalog management |
| 4 | High cohesion / low coupling | 2 | medium | medium | `codeanatomy_ext` submodules tightly coupled via shared `helpers.rs`; `extract_session_ctx` re-implemented in two places |
| 5 | Dependency direction | 3 | — | — | Binding crates depend inward on engine core; core does not import binding types |
| 6 | Ports & Adapters | 2 | medium | high | DF52 FFI provider contract changes: `__datafusion_*_provider__` methods now require an additional `session` parameter not yet threaded through |
| 12 | Dependency inversion | 2 | small | low | `schema_evolution.rs` creates `SchemaEvolutionAdapterFactory` that simply delegates to `DefaultPhysicalExprAdapterFactory` — abstraction adds no value |
| 13 | Composition over inheritance | 3 | — | — | No deep inheritance hierarchies; structs wrap Rust types cleanly |
| 14 | Law of Demeter | 2 | small | low | `plugin_bridge.rs` calls `extract_session_ctx(ctx)?` 5 times in a single function, re-extracting the same value rather than binding it once |
| 15 | Tell, don't ask | 2 | small | low | `helpers.rs:extract_session_ctx` guesses object shape via duck-type inspection; callers must know the session is inside `.ctx` |
| 19 | KISS | 2 | medium | medium | Per-call `Runtime::new()` in 3 sites; `PyRunResult` re-parses JSON on every accessor call |
| 20 | YAGNI | 2 | small | low | `SchemaEvolutionAdapterFactory` is a one-liner pass-through — unused extensibility point |
| 21 | Least astonishment | 2 | small | medium | `datafusion_ext_py/lib.rs` mirrors `datafusion._internal` at runtime by iterating its dict — surprising to any caller expecting a typed module surface |
| 22 | Declare and version contracts | 2 | medium | high | DF51 hard-coded in Cargo.toml with no migration path documented; FFI capsule names not versioned |

---

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding — Alignment: 2/3

**Current state:**
`PySessionContext` in `rust/datafusion_python/src/context.rs:298` declares `pub(crate) ctx: SessionContext`. The `(crate)` visibility is appropriate. However, `codeanatomy_ext/helpers.rs:33-39` exports a public `extract_session_ctx` function that performs shape-dependent extraction of the inner `SessionContext` by first trying a direct cast, then falling back to `ctx.getattr("ctx")`. This externalises knowledge of the session object's internal layout to every calling submodule.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext/helpers.rs:33-39`: `extract_session_ctx` inspects `.ctx` attribute by name — callers now depend on that attribute name remaining stable.
- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs:59-75`: `session_context_contract` duplicates the exact same duck-type extraction logic from `helpers.rs:extract_session_ctx`, creating two divergent implementations of the same decision.

**Suggested improvement:**
Remove `session_context_contract` from `session_utils.rs` and replace all 7 call sites with the single `extract_session_ctx` from `helpers.rs`. Then make `extract_session_ctx` `pub(super)` rather than `pub(crate)`, restricting knowledge of the extraction pattern to the `codeanatomy_ext` module.

**Effort:** small
**Risk if unaddressed:** low

---

#### P3. SRP — Alignment: 2/3

**Current state:**
`rust/datafusion_python/src/context.rs` is 1178 LOC and handles at least six distinct responsibilities: session configuration (`PySessionConfig`, `PyRuntimeEnvBuilder`, `PySQLOptions`), session lifecycle (`PySessionContext::new`), object-store registration, table/listing-table registration, SQL execution, and DataFrame factory methods. The upstream source has the same shape, so much of this is inherited fork-drift rather than local design choice.

**Findings:**
- `rust/datafusion_python/src/context.rs:76-166`: Three config types (`PySessionConfig`, `PyRuntimeEnvBuilder`, `PySQLOptions`) live in the same file as the 800-line `PySessionContext` impl.
- `rust/datafusion_python/src/context.rs:373-418`: `register_listing_table` has 7 parameters and performs schema inference inline — a concern orthogonal to session lifecycle.
- `rust/datafusion_python/src/context.rs:426-462`: `sql_with_options` handles placeholder substitution, param coercion, and DataFrame creation in one method.

**Suggested improvement:**
Extract `PySessionConfig`, `PyRuntimeEnvBuilder`, and `PySQLOptions` into `context/config.rs` (or retain them in `config.rs` which already exists). Extract the listing-table registration into a dedicated `context/listing.rs` helper. This keeps `context.rs` as a session-lifecycle coordinator rather than a dumping ground, matching the `codeanatomy_engine_py` approach where each concern is a separate file.

**Effort:** medium
**Risk if unaddressed:** medium (changes to config structs and SQL handling currently require editing the same 1178-LOC file)

---

#### P4. High cohesion / low coupling — Alignment: 2/3

**Current state:**
The `codeanatomy_ext` module contains 11 submodules all coupled through `super::helpers`. The helpers module exposes 15 public functions covering session extraction, JSON conversion, storage options, Delta gate construction, schema IPC decoding, runtime creation, capsule construction, and dict serialisation. This is a wide, heterogeneous interface.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext/helpers.rs:64-174`: A single helpers module mixes five distinct domains: Tokio runtime lifecycle, Delta-specific type conversion, generic JSON-to-Python conversion, Arrow schema decoding, and PyCapsule construction.
- `rust/datafusion_python/src/codeanatomy_ext/plugin_bridge.rs:291-333`: `register_df_plugin` calls `extract_session_ctx(ctx)?` four times for the same `ctx` binding within a single function body (lines 300, 303, 308, 313, 327, 281), passing the same resolved value each time.

**Suggested improvement:**
Split `helpers.rs` into at least two modules: `helpers/runtime.rs` (Tokio runtime construction, capsule wrapping) and `helpers/delta_conv.rs` (Delta-specific dict serialisation, scan config, snapshot conversions). Cache the result of `extract_session_ctx` in a local binding at the top of each function rather than calling it multiple times. In `plugin_bridge.rs:register_df_plugin`, resolve `let ctx = extract_session_ctx(ctx)?;` once and pass it by reference to each subsequent call.

**Effort:** medium
**Risk if unaddressed:** medium (helpers module will grow unbounded as new extension points are added)

---

#### P6. Ports & Adapters — Alignment: 2/3

**Current state:**
This is the highest-priority DF52 finding. The FFI provider protocol in datafusion-python 52.0 requires that `__datafusion_catalog_provider__`, `__datafusion_schema_provider__`, and `__datafusion_table_provider__` methods accept an additional `session` parameter carrying an `FFI_LogicalExtensionCodec` and `TaskContextProvider`. The current fork (DF51) calls these methods with no arguments (`call0()` at `utils.rs:171`) and without threading a session or codec.

**Findings:**
- `rust/datafusion_python/src/utils.rs:170-171`: `obj.getattr("__datafusion_table_provider__")?.call0()?` — zero-argument call; DF52 requires a session parameter.
- `rust/datafusion_python/src/context.rs:607-609`: `provider.getattr("__datafusion_catalog_provider__")?.call0()?` — same zero-argument pattern.
- `rust/datafusion_python/src/catalog.rs:116-118` and `catalog.rs:363-365`: `schema_provider.getattr("__datafusion_schema_provider__")?.call0()?` — same pattern.
- `rust/datafusion_python/Cargo.toml:67`: `datafusion = "51"` — hard-pinned; all downstream `datafusion_ext_py` and `codeanatomy_engine_py` also consume DF51 transitively.

**Suggested improvement:**
This is the blocking migration step. Before any other crate can move to DF52:
1. Update all three `__datafusion_*_provider__` call sites to pass a `session` object as the first argument, following the upgrade guide at `datafusion.apache.org/python/user-guide/upgrade-guides.html`.
2. Thread `LogicalExtensionCodec` and `TaskContextProvider` through `FFI_CatalogProvider` and `FFI_TableProvider` constructors.
3. Update `Cargo.toml` dependency from `datafusion = "51"` to `datafusion = "52"` and resolve all breaking changes (see DF52 Migration Impact section).

**Effort:** large
**Risk if unaddressed:** high (this entire crate must be migrated before any other crate can move to DF52; the FFI capsule protocol is the external API surface)

---

### Category: Composition

#### P12. Dependency inversion — Alignment: 2/3

**Current state:**
`rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs:44-53` defines `SchemaEvolutionAdapterFactory` which implements `PhysicalExprAdapterFactory` but immediately delegates to `DefaultPhysicalExprAdapterFactory`. The intermediate type exists solely to be wrapped in an `Arc` and stored in a capsule, adding a layer of indirection with no behaviour difference.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs:44-53`: `SchemaEvolutionAdapterFactory::create` is a one-line pass-through to `DefaultPhysicalExprAdapterFactory`.
- `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs:264-270`: `schema_evolution_adapter_factory` constructs the wrapper and wraps it in a capsule — callers could receive the default factory directly.

**Suggested improvement:**
Remove `SchemaEvolutionAdapterFactory` entirely. In `schema_evolution_adapter_factory`, construct `DefaultPhysicalExprAdapterFactory` directly: `let factory: Arc<dyn PhysicalExprAdapterFactory> = Arc::new(DefaultPhysicalExprAdapterFactory);`. If there is a future need for custom schema adaptation behaviour, introduce the custom type then (YAGNI). Estimated LOC reduction: ~15.

**Effort:** small
**Risk if unaddressed:** low (no functional difference, but the wrapper misleads readers into thinking custom adaptation logic exists)

---

#### P14. Law of Demeter — Alignment: 2/3

**Current state:**
Multiple functions in `codeanatomy_ext` call `extract_session_ctx(ctx)?` on the same `ctx` argument more than once within a single function body, re-traversing the Python object graph on each call.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext/plugin_bridge.rs:291-333`: `register_df_plugin` calls `extract_session_ctx(ctx)?` at lines 300, 303, 308, 313, 327 — five times for one logical session.
- `rust/datafusion_python/src/codeanatomy_ext/plugin_bridge.rs:243-288`: `register_df_plugin_table_providers` calls `extract_session_ctx(ctx)?` three times (lines 250, 255, 281).
- `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs:365-416`: `install_codeanatomy_runtime` calls `extract_session_ctx(ctx)?` three times (lines 372, 381, 389, 395).

**Suggested improvement:**
Resolve `let session_ctx = extract_session_ctx(ctx)?;` once at the top of each function and pass `&session_ctx` to subsequent calls. This is a straightforward mechanical change with zero semantic impact. It eliminates redundant Python object graph traversal and makes the code easier to follow.

**Effort:** small
**Risk if unaddressed:** low (correctness risk is negligible; performance cost is minor per-call overhead in binding layer)

---

#### P15. Tell, don't ask — Alignment: 2/3

**Current state:**
`helpers.rs:extract_session_ctx` uses duck-type inspection: it first tries `ctx.extract::<Bound<'_, PySessionContext>>()`, then falls back to `ctx.getattr("ctx")` and extracting from that. This "ask and inspect" pattern means callers must accept any Python object and guess whether it's a session-like thing.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext/helpers.rs:33-39`: Two-branch duck-type inspection; callers are effectively polymorphic on session type without a protocol.
- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs:59-75`: Duplicate of the same inspection, adding a third code path (`Optional<&Bound<'_, PyAny>>` variant).

**Suggested improvement:**
Document the expected protocol explicitly in a docstring on `extract_session_ctx`: "Accepts either a `datafusion.SessionContext` or any Python object exposing a `.ctx` attribute that is a `datafusion.SessionContext`." Remove the duplicate in `session_utils.rs`. This does not require changing the two-branch logic (the duck-typing serves real users who wrap `SessionContext`), but it makes the contract explicit rather than implicit.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Simplicity

#### P19. KISS — Alignment: 2/3

**Current state:**
Two patterns add unnecessary complexity:

(a) Per-call `Runtime::new()` in `codeanatomy_ext`. The `helpers::runtime()` function at `helpers.rs:127-130` creates a brand-new Tokio multi-thread runtime on every call. Every Delta table operation (`delta_snapshot_info`, `delta_add_actions`, `delta_cdf_table_provider`) builds and immediately drops an entire thread pool.

(b) `PyRunResult` in `codeanatomy_engine_py/src/result.rs` stores the result as a raw JSON string and re-parses it on every accessor call. `task_schedule` (line 32), `plan_bundles` (line 49), `critical_path` (line 81), `plan_bundle_count` (line 70) each call `serde_json::from_str(&self.inner_json)` independently.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext/helpers.rs:127-130`: `runtime()` creates a new `Runtime` per call; called from at least 8 Delta functions.
- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs:500, 512`: Two additional `Runtime::new()` calls for different operations.
- `rust/codeanatomy_engine_py/src/result.rs:32, 49, 70, 81`: Four separate `serde_json::from_str` calls on the same `inner_json` string.

**Suggested improvement:**
(a) Replace `helpers::runtime()` with a module-level `OnceLock<Runtime>` (or reuse the runtime already owned by `CpgMaterializer`). Delta provider functions that need async should accept a runtime reference rather than creating one. The `CpgMaterializer` already correctly holds a `Arc<Runtime>` — the `codeanatomy_ext` helpers should either accept an injected runtime or share a process-scoped singleton via `OnceLock`.

(b) In `PyRunResult`, parse `inner_json` once at construction time into a `serde_json::Value` and store it alongside the string, or compute derived fields lazily with `OnceCell`. The current pattern means five attribute accesses on a single result re-parse the same JSON five times.

**Effort:** medium
**Risk if unaddressed:** medium (per-call runtime creation is a correctness-adjacent concern — creating multiple runtimes in a process can cause subtle thread-local state issues under some Tokio configurations)

---

#### P20. YAGNI — Alignment: 2/3

**Current state:**
`SchemaEvolutionAdapterFactory` is a pass-through wrapper with no custom logic. It creates a named type for a concept that does not yet have custom behaviour, adding speculative extension structure without a second use case.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs:43-53`: The entire struct and impl is ~10 lines producing identical output to `DefaultPhysicalExprAdapterFactory`.

**Suggested improvement:**
Remove. Use `DefaultPhysicalExprAdapterFactory` directly at the single call site. Introduce a named custom type when — and only when — custom schema adaptation logic is required.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment — Alignment: 2/3

**Current state:**
`rust/datafusion_ext_py/src/lib.rs` implements its module by importing `datafusion._internal` at Python runtime and iterating its `__dict__` to re-export all non-underscore names. This mirrors the internal module surface but does so by reflection rather than explicit declaration. A caller of `datafusion_ext` who inspects it with `dir()` will receive whatever is in `datafusion._internal` at that moment, plus `IS_STUB` and `__all__`. The module's public surface is determined entirely at runtime by what the other wheel has installed.

**Findings:**
- `rust/datafusion_ext_py/src/lib.rs:8-22`: The `datafusion_ext` module exports by iterating `datafusion._internal`'s dict — not a declared static surface.
- `rust/datafusion_ext_py/src/lib.rs:36-38`: `let _ = py;` suppresses an unused warning, indicating `py` was originally used; the code structure has evolved.

**Suggested improvement:**
Add a `pub const DATAFUSION_VERSION: &str` constant and document clearly that `datafusion_ext` is an ABI compatibility mirror, not an independent module. Consider making the re-export explicit for the subset of symbols that `codeanatomy_ext` callers actually need, so that the surface is auditable without running the code. The `IS_STUB = false` flag is a good start; extend it with a version string so callers can assert they have the expected mirror.

**Effort:** small
**Risk if unaddressed:** medium (breakage when `datafusion._internal` renames or removes a symbol is invisible until runtime)

---

#### P22. Declare and version contracts — Alignment: 2/3

**Current state:**
The FFI capsule names used across `codeanatomy_ext` are plain string constants with no versioning:
- `"datafusion_table_provider"` (`helpers.rs:137`, `plugin_bridge.rs:237`)
- `"datafusion_ext.DfPluginHandle"` (`plugin_bridge.rs:24`)
- `"datafusion_ext.RegistryCatalogProviderFactory"` (`registry_bridge.rs:25`)
- `"datafusion._internal.PhysicalExprAdapterFactory"` (implied by schema_evolution)

These are the ABI boundary between Python and Rust. There is no versioning scheme. If a capsule's memory layout changes, the mismatch is only detectable at unsafe dereference time.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext/helpers.rs:137`: Capsule name `"datafusion_table_provider"` — no version suffix.
- `rust/datafusion_python/src/codeanatomy_ext/plugin_bridge.rs:24`: `PLUGIN_HANDLE_CAPSULE_NAME = "datafusion_ext.DfPluginHandle"` — unversioned constant; the plugin ABI major/minor is checked separately but the capsule name itself carries no version.

**Suggested improvement:**
Add a version suffix to custom capsule names: `"datafusion_ext.DfPluginHandle.v1"` and `"datafusion_ext.RegistryCatalogProvider.v1"`. The standard `"datafusion_table_provider"` capsule name is upstream-defined and should not be changed. Document the versioning policy in `codeanatomy_ext/mod.rs` so future contributors know when to bump the suffix.

**Effort:** small
**Risk if unaddressed:** medium (ABI mismatch on capsule layout changes causes segfaults, not Python exceptions)

---

## Cross-Cutting Themes

### Theme 1: DF51 Fork Debt — The Migration Blocker

The entire `datafusion_python` crate is a fork of datafusion-python 51.0.0, version-pinned in `Cargo.toml`. The Cargo manifest still carries the upstream package name (`name = "datafusion-python"`) and version (`version = "51.0.0"`). The CodeAnatomy-specific additions — all of `codeanatomy_ext/` and the re-exports in `lib.rs` — are lateral extensions on top of the fork.

This creates a migration bottleneck: every DF52 breaking change in the binding layer must be resolved in this fork before any downstream crate can move forward. The breaking changes known from the DF52 document that affect this layer are:
1. `FFI_TableProvider`/`FFI_CatalogProvider` constructors now require `TaskContextProvider` and `LogicalExtensionCodec` — affects `helpers.rs`, `plugin_bridge.rs`, `context.rs`, `catalog.rs`, `utils.rs`.
2. `FileSource::with_projection` removed → `try_pushdown_projection` — affects any code using `FileScanConfig`/`FileSource` directly.
3. `DFSchema` field methods now return `&FieldRef` — affects schema inspection in `common/df_schema.rs` and `common/schema.rs`.
4. `CoalesceBatchesExec` removed — affects `physical_plan.rs` if it exposes or wraps that type.
5. `Parquet row-filter builder` signature change — affects any Parquet filter construction.

**Root cause:** The fork strategy was necessary to add `codeanatomy_ext` at the crate level without changing the Python package name. The cost is fork maintenance and delayed DF upgrades.

**Affected principles:** P6, P22, P19.

**Suggested approach:** Evaluate whether `codeanatomy_ext` can be separated into its own crate that depends on the upstream `datafusion-python` crate as a library dependency rather than forking it. This would eliminate the fork and allow independent DF version upgrades. If fork is required, add a `DATAFUSION_FORK_VERSION` constant to `lib.rs` and a `//! Fork delta:` comment block in `lib.rs` listing all local patches.

---

### Theme 2: Repeated Tokio Runtime Construction

`helpers::runtime()` constructs a new `Runtime` per Delta operation call. `session_utils.rs` also creates its own runtimes at lines 500 and 512. `compiler.rs` and `materializer.rs` each store a `Runtime` in their struct (correctly). The inconsistency means some operations reuse a stable runtime while others throw away a runtime per call.

**Root cause:** `codeanatomy_ext` functions are free-standing `#[pyfunction]`s with no associated state, so they have nowhere to store a shared runtime. The fix requires either a process-level singleton or threading the `CpgMaterializer`'s runtime through where needed.

**Affected principles:** P19, P4.

---

### Theme 3: PyRunResult JSON Re-Parsing Overhead

`PyRunResult` serialises the Rust `RunResult` to a JSON string at construction, then re-parses it on every Python attribute access. With 4 accessor methods each calling `serde_json::from_str`, a caller that reads all four attributes re-parses the same JSON 4 times. This is a correctness-adjacent issue because JSON parsing is fallible — if parsing fails mid-access it produces an inconsistent view.

**Root cause:** The `inner_json: String` field was chosen to avoid the lifetime complexity of holding a parsed `Value`, but a `serde_json::Value` can be stored in a `PyRunResult` field without lifetime issues since `Value` is `'static`.

**Affected principles:** P19, P8 (contract consistency across accessors).

---

## Rust Migration Candidates

The following items in `codeanatomy_ext` are candidates for moving into `datafusion_ext` (the pure Rust crate) to reduce the binding layer's footprint:

| Item | Current location | Better location | Rationale |
|------|-----------------|-----------------|-----------|
| `json_to_py` | `codeanatomy_ext/helpers.rs:143-175` | Keep in binding layer | Requires PyO3 — cannot move to pure Rust |
| `parse_env_bool` + `resolve_compliance_enabled` | `codeanatomy_engine_py/src/materializer.rs:51-65` | `codeanatomy_engine::config` | Pure logic; no PyO3 dependency |
| `resolve_tuner_mode` | `codeanatomy_engine_py/src/materializer.rs:67-77` | `codeanatomy_engine::config` | Same — pure env resolution |
| `delta_gate_from_params` | `codeanatomy_ext/helpers.rs:70-92` | `datafusion_ext` | Delta-specific; no PyO3 |
| `table_version_from_options` | `codeanatomy_ext/helpers.rs:94-100` | `datafusion_ext` | Thin wrapper; already calls `TableVersion::from_options` |
| `storage_options_map` | `codeanatomy_ext/helpers.rs:64-68` | `datafusion_ext` | One-liner converter; no PyO3 |
| `scan_overrides_from_params` | `codeanatomy_ext/helpers.rs:107-125` | `datafusion_ext` | Has IPC decode; pure Rust possible if IPC helper is in ext |

Moving the pure-Rust helpers reduces the binding layer's surface and makes the core logic independently testable without PyO3.

**Estimated LOC reduction in binding layer:** ~100 LOC across helpers.rs and delta_provider.rs.

---

## DF52 Migration Impact

This is the highest priority section. The migration from datafusion 51 to 52 is **blocking** for this crate and must happen before downstream Python code can adopt any DF52 features.

### Breaking changes requiring code changes

| Change | DF52 reference | Affected files | Effort |
|--------|---------------|----------------|--------|
| FFI provider `session` parameter | DF52 upgrade guide §K | `context.rs:607`, `catalog.rs:116,363`, `utils.rs:170` | medium |
| `FFI_TableProvider::new` constructor signature | `datafusion-ffi` crate update | `helpers.rs:136`, `schema_evolution.rs:378` | small |
| `FileSource::with_projection` removed | DF52 upgrade guide §D | Any code calling `with_projection` on `FileSource` — requires audit | medium |
| `FileScanConfigBuilder::with_projection_indices` returns `Result` | DF52 §D | Listing table builder in `schema_evolution.rs` | small |
| `DFSchema` field methods return `&FieldRef` | DF52 §J | `common/df_schema.rs`, `common/schema.rs` | small |
| Parquet row-filter builder drops schema parameter | DF52 §G | Any `build_row_filter` calls | small |
| `newlines_in_values` moved to `CsvOptions` | DF52 §G | CSV handling in `context.rs` | small |
| `AggregateUDFImpl::supports_within_group_clause` default | DF52 §I | `udaf.rs` | small |
| `AggregateUDFImpl::supports_null_handling_clause` default | DF52 §I | `udaf.rs` | small |

### DF52 features available after migration

| Feature | DF52 reference | Applicable to |
|---------|---------------|---------------|
| File statistics cache (`statistics_cache()`) | DF52 §C | `cache_tables.rs` — replaces bespoke `CacheSnapshotConfig` logic |
| Prefix-aware list-files cache (`list_files_cache()`) | DF52 §C | `cache_tables.rs:CacheTableKind::ListFiles` |
| Sort pushdown to scans | DF52 §D | Delta provider scan configuration |
| Expression evaluation pushdown via `PhysicalExprAdapter` | DF52 §D | `schema_evolution.rs:SchemaEvolutionAdapterFactory` — the pass-through adapter gains real upstream support |
| `TableProvider` DELETE/UPDATE hooks | DF52 §E | `delta_mutations.rs` |
| `RelationPlanner` FROM-clause hook | DF52 §F | Future SQL extensions |
| Arrow IPC stream read | DF52 §G | Extraction session builder |

### Planning-Object Consolidation

After DF52 migration, two bespoke code patterns can be replaced by DF52 built-ins:

1. **`CacheSnapshotConfig` in `cache_tables.rs:32-48`** — DF52 adds `statistics_cache()` and `list_files_cache()` accessors on `SessionContext`. The bespoke `CacheTableKind` enum and `CacheTableFunction` struct partially replicate what these built-ins expose. After migration, audit whether `CacheTableKind::Statistics` and `CacheTableKind::ListFiles` can be replaced with calls to the DF52 cache introspection API. Estimated LOC reduction: ~80.

2. **`SchemaEvolutionAdapterFactory` in `schema_evolution.rs:43-53`** — DF52's `PhysicalExprAdapter` architecture is the upstream mechanism for schema adaptation. The pass-through factory is already using DF52-compatible traits. After migration, the factory can be removed (see P12/P20 findings above). Estimated LOC reduction: ~15.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P19 (KISS) | Cache `extract_session_ctx` result locally in `plugin_bridge.rs:register_df_plugin` — currently called 5 times | small | Immediate correctness + readability gain |
| 2 | P12/P20 | Remove `SchemaEvolutionAdapterFactory`; use `DefaultPhysicalExprAdapterFactory` directly | small | ~15 LOC removed; no behaviour change |
| 3 | P1 | Remove `session_context_contract` duplicate in `session_utils.rs`; use `helpers::extract_session_ctx` | small | Eliminates divergent extraction logic |
| 4 | P21 | Add `DATAFUSION_VERSION` constant and docstring to `datafusion_ext_py/lib.rs` making the mirror contract explicit | small | Auditable surface; no functional change |
| 5 | P19 | Replace 4 `serde_json::from_str` calls in `PyRunResult` with a single parse at construction | small | Consistent error behaviour across accessors |

---

## Recommended Action Sequence

1. **[Blocking — DF52 migration prerequisite]** Audit all `__datafusion_*_provider__` call sites and update to DF52 FFI contract (session parameter). Files: `context.rs:607`, `catalog.rs:116,363`, `utils.rs:170`. This unblocks all downstream crates. (P6)

2. **[Parallel with #1]** Update all DF51→DF52 breaking changes in `Cargo.toml` and resolve compilation errors. Priority order: FFI constructors, `DFSchema` field refs, `FileSource` projection, Parquet filter builder. (P22)

3. **[After compilation passes]** Remove `SchemaEvolutionAdapterFactory` and `session_context_contract` duplicate. (P12, P20, P1)

4. **[Cleanup]** Cache `extract_session_ctx` results locally in `plugin_bridge.rs` and `udf_registration.rs`. (P14, P19)

5. **[Improvement]** Replace `PyRunResult` per-accessor JSON parsing with single-parse construction. (P19)

6. **[Architecture]** Evaluate separating `codeanatomy_ext` into its own crate to eliminate the upstream fork dependency. If fork is retained, add `DATAFUSION_FORK_VERSION` constant and `//! Fork delta:` changelog. (P22, Theme 1)

7. **[Post-DF52]** Audit `cache_tables.rs` for replacement by DF52 built-in cache introspection API (`statistics_cache()`, `list_files_cache()`). (Planning-Object Consolidation)

8. **[Refactor]** Extract `PySessionConfig`/`PyRuntimeEnvBuilder`/`PySQLOptions` out of `context.rs` into `config.rs` (which already exists). (P3)
