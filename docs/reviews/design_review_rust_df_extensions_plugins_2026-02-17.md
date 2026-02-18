# Design Review: Rust DataFusion Extensions + Plugin System

**Date:** 2026-02-17
**Scope:** `rust/datafusion_ext/`, `rust/df_plugin_api/`, `rust/df_plugin_host/`, `rust/df_plugin_codeanatomy/`, `rust/df_plugin_common/`
**Focus:** Boundaries (1-6), Composition (12-15), Quality (23-24)
**Depth:** moderate
**Files reviewed:** 20 (of 72 total .rs files in scope; selected entry points, largest files, most-imported modules, and all plugin crate files)

---

## Executive Summary

The Rust DataFusion extension and plugin crates are architecturally sound. The ports-and-adapters split between `df_plugin_api` (ABI contract), `df_plugin_host` (loading/bridging), `df_plugin_codeanatomy` (concrete plugin), and `datafusion_ext` (logic library) is clean and explicit. The primary design gaps are: (1) `compat.rs` is pinned to DataFusion 51.x in its own comment while the Cargo manifest confirms DF 51.0.0 is the active version — three DF52 breaking changes directly affect `physical_rules.rs` (`CoalesceBatches` optimizer API), `expr_planner.rs`/`function_rewrite.rs` (extended planner hooks), and every UDAF in `udaf_builtin.rs` (`supports_within_group_clause` / `supports_null_handling_clause` opt-in semantics tightening); (2) `df_plugin_codeanatomy/src/lib.rs` (694 LOC) co-locates option parsing, async policy resolution, table provider construction, and FFI binding in a single module, mixing multiple change reasons; (3) the `DfPluginManifestV1` type does not expose a `RelationPlanner` capability bit despite DF52 adding `RelationPlanner` as a first-class extension point; (4) `physical_rules.rs` re-wraps `CoalesceBatches` — a mechanism that DF52 inlines into operators, meaning the rule may become a no-op or break after upgrade.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | Plugin option structs in `df_plugin_codeanatomy/lib.rs` are unexported but `pub` fields on `DeltaScanOverrides` leak mutable structure across crate boundary |
| 2 | Separation of concerns | 1 | medium | medium | `df_plugin_codeanatomy/src/lib.rs` fuses option parsing, policy resolution, async runtime acquisition, Delta provider construction, and FFI binding in one 694-LOC module |
| 3 | SRP (one reason to change) | 1 | medium | medium | Same as above: `lib.rs` changes for option schema changes, async policy changes, Delta scan config changes, and FFI ABI changes — four distinct change reasons |
| 4 | High cohesion, low coupling | 2 | small | low | `datafusion_ext` lib.rs top-level install functions duplicate planner lists already returned by `domain_expr_planners()` / `domain_function_rewrites()` |
| 5 | Dependency direction | 3 | — | — | Core UDF logic in `udf/` does not reach up to plugin host or session; direction is clean |
| 6 | Ports & Adapters | 2 | medium | medium | `DfPluginManifestV1::capabilities` bitmask has no `RELATION_PLANNER` bit; DF52 `RelationPlanner` port is unaddressed in the ABI contract |
| 12 | Dependency inversion + explicit composition | 2 | small | low | `register_all` and `install_*` functions accept `&SessionContext` directly, not an abstraction; testable only against real sessions |
| 13 | Prefer composition over inheritance | 3 | — | — | No deep trait inheritance; delegation pattern used cleanly for UDWFs |
| 14 | Law of Demeter | 2 | small | low | `registry_bridge.rs:register_table_providers` accesses `self.module().create_table_provider()` via two hops through `PluginHandle` fields |
| 15 | Tell, don't ask | 2 | small | low | `physical_rules.rs:ensure_physical_config` extracts config, checks presence, inserts default, then returns mutable ref — asks-then-inserts pattern |
| 23 | Design for testability | 1 | medium | high | `df_plugin_codeanatomy/lib.rs` functions are all `fn` (not `extern "C"` wrappers) but `build_udf_bundle_with_options` constructs real UDFs + runtime; no injection point for config; `build_table_functions` creates a `SessionContext::new()` internally |
| 24 | Observability | 1 | small | medium | `delta_control_plane.rs` and `delta_mutations.rs` use `#[instrument]` correctly; plugin `lib.rs` `eprintln!` error paths are silent in production telemetry; `physical_rules.rs` and `planner_rules.rs` have zero tracing |

---

## Detailed Findings

### Category: Boundaries

#### P2. Separation of Concerns — Alignment: 1/3

**Current state:**
`rust/df_plugin_codeanatomy/src/lib.rs` (694 LOC) is the single-file concrete plugin implementation. It directly contains:
- JSON option deserialization structs and `deserialize_schema_ipc` custom visitor (`DeltaProviderOptions`, `DeltaScanConfigPayload`, `DeltaCdfProviderOptions`, `PluginUdfConfig`, `PluginUdfOptions`, lines 43-105)
- Async policy resolution (`resolve_udf_policy`, line 176)
- Config object construction from parsed options (`config_options_from_udf_options`, line 203)
- UDF bundle construction (`build_udf_bundle_from_specs`, `build_udf_bundle_with_options`, lines 249-305)
- Delta provider construction via async runtime (`build_delta_provider`, `build_delta_cdf_provider`, lines 398-463)
- Four `extern "C"` FFI entry points (`create_table_provider`, `plugin_manifest`, `plugin_exports`, `plugin_udf_bundle`, lines 465-509)

The consequence is that a change to the Delta scan config schema, the async UDF option contract, or the FFI ABI all require editing the same file. The file currently handles IO, policy, serialization, and ABI binding simultaneously.

**Findings:**
- `rust/df_plugin_codeanatomy/src/lib.rs:43-105`: Option deserialization structs live in the FFI binding file rather than a separate `options.rs` or `config.rs` module
- `rust/df_plugin_codeanatomy/src/lib.rs:107-247`: Async runtime acquisition (`async_runtime()`), policy resolution (`resolve_udf_policy`), and config construction (`config_options_from_udf_options`) are co-located with FFI wiring
- `rust/df_plugin_codeanatomy/src/lib.rs:307-334`: `build_table_functions` creates a `SessionContext::new()` silently, embedding an infrastructure dependency deep in what should be a registration function
- `rust/df_plugin_codeanatomy/src/lib.rs:465-509`: The four `extern "C"` bindings are the only place that should reference the other concerns; they currently co-exist in the same module without isolation

**Suggested improvement:**
Split `df_plugin_codeanatomy/src/lib.rs` into four modules:
1. `options.rs` — all `#[derive(Deserialize)]` structs and `deserialize_schema_ipc`
2. `udf_bundle.rs` — `resolve_udf_policy`, `config_options_from_udf_options`, `build_udf_bundle_from_specs`, `build_udf_bundle_with_options`
3. `providers.rs` — `build_delta_provider`, `build_delta_cdf_provider`, `delta_scan_config_from_options`, `apply_file_column_builder`
4. `lib.rs` — retain only `extern "C"` entry points, `manifest()`, `exports()`, `get_library()`, and the `#[export_root_module]` attribute

**Effort:** medium
**Risk if unaddressed:** medium — the file will continue to grow as new provider types, option fields, or UDF policies are added, compounding the mixing of concerns

---

#### P3. SRP (One Reason to Change) — Alignment: 1/3

**Current state:**
`rust/df_plugin_codeanatomy/src/lib.rs` changes for at least four independent reasons:
1. The Delta scan config payload schema changes (add/remove fields like `schema_ipc`)
2. The async UDF policy changes (new timeout knobs, new batch parameters)
3. The `DeltaFeatureGate` or `TableVersion` protocol changes (upstream in `datafusion_ext`)
4. The FFI ABI changes (`DfPluginMod` struct fields added/removed by `df_plugin_api`)

**Findings:**
- `rust/df_plugin_codeanatomy/src/lib.rs:58-66`: `DeltaScanConfigPayload` would require editing if the Delta scan config schema adds projection or statistics fields (DF52 moves statistics to `FileScanConfig`; see DF52 change D-5)
- `rust/df_plugin_codeanatomy/src/lib.rs:85-105`: `PluginUdfConfig` fields would change if UDF behavioral defaults change
- `rust/df_plugin_codeanatomy/src/lib.rs:500-508`: `DfPluginMod { ... }.leak_into_prefix()` changes if `df_plugin_api` adds fields to `DfPluginMod`

**Suggested improvement:**
Apply the same module decomposition as for P2. The single-responsibility split is a consequence of the separation-of-concerns split, not an additional refactor step.

**Effort:** medium (same effort as P2)
**Risk if unaddressed:** medium

---

#### P6. Ports & Adapters — Alignment: 2/3

**Current state:**
The plugin API crate (`df_plugin_api`) defines an ABI-stable contract for UDFs, table providers, and table functions. The capability bitmask in `DfPluginManifestV1::capabilities` declares what the plugin exports. This is a well-designed port definition. However, it does not model the DF52 `RelationPlanner` extension point, which is a new FROM-clause planning hook that sits alongside `ExprPlanner` and `TypePlanner`.

**Findings:**
- `rust/df_plugin_api/src/manifest.rs:7-13`: Five `caps::*` constants cover `TABLE_PROVIDER`, `SCALAR_UDF`, `AGG_UDF`, `WINDOW_UDF`, and `TABLE_FUNCTION`. No constant for `EXPR_PLANNER` or `RELATION_PLANNER`.
- `rust/df_plugin_api/src/lib.rs:38-56`: `DfPluginExportsV1` contains `udf_bundle`, `table_functions`, and `table_provider_names` but no planner registrations. A plugin cannot advertise or export custom planners through the plugin ABI.
- `rust/datafusion_ext/src/lib.rs:55-90`: `install_expr_planners_native` is only available via the native (non-plugin) path. The `CodeAnatomyDomainPlanner` cannot be delivered through the plugin FFI channel.
- `rust/datafusion_ext/src/expr_planner.rs:14`: `CodeAnatomyDomainPlanner` implements `ExprPlanner`, not `RelationPlanner`. DF52 adds `RelationPlanner` as a complement to `ExprPlanner` for FROM-clause constructs; this is an unaddressed extension surface.

**Suggested improvement:**
Add a `caps::EXPR_PLANNER` capability constant and a `DfPluginExportsV1::expr_planners: RVec<...>` field if/when the plugin must deliver planners cross-ABI. For the near-term, document explicitly in `df_plugin_api/src/manifest.rs` that planners are not part of the plugin ABI and are registered natively by the host. Also evaluate DF52 `RelationPlanner` and add a corresponding `CodeAnatomyRelationPlanner` to `expr_planner.rs` if FROM-clause extensions are needed.

**Effort:** medium (ABI addition requires coordination between api, host, and plugin crates)
**Risk if unaddressed:** low in the short term; medium if custom FROM-clause constructs are needed

---

### Category: Composition

#### P12. Dependency Inversion + Explicit Composition — Alignment: 2/3

**Current state:**
`datafusion_ext/src/lib.rs` exposes install functions (`install_sql_macro_factory_native`, `install_expr_planners_native`, `install_physical_rules`) that accept `&SessionContext` directly. This is appropriate for a library that is inherently session-bound, but it means no testability seam exists between the planner installation logic and the session.

Additionally, `df_plugin_codeanatomy/src/lib.rs:307-334` creates `SessionContext::new()` inside `build_table_functions`, an implicitly created infrastructure dependency in a function that should be a pure builder.

**Findings:**
- `rust/df_plugin_codeanatomy/src/lib.rs:308`: `let ctx = SessionContext::new();` — silent context creation inside `build_table_functions`. If UDTF building requires custom config (e.g., object store), this creation is inflexible.
- `rust/datafusion_ext/src/lib.rs:107-121`: `domain_expr_planners()` and `domain_function_rewrites()` correctly return `Vec<Arc<dyn ...>>` for builder-path use. The `install_expr_planners_native` function that mutates a live session is a parallel code path that duplicates the planner list. If a new planner is added to one path but not the other, they diverge silently.

**Suggested improvement:**
In `build_table_functions`, accept an optional `SessionContext` parameter or a `ConfigOptions` reference rather than creating one internally. For the `install_*` / `domain_*` duplication: make `install_expr_planners_native` consume the result of `domain_expr_planners()` rather than re-enumerating planners, ensuring a single authoritative list.

**Effort:** small
**Risk if unaddressed:** low (the `SessionContext::new()` only affects table function building, which currently has no entries in `table_udf_specs()`; risk increases if table UDFs are added)

---

#### P14. Law of Demeter — Alignment: 2/3

**Current state:**
`df_plugin_host/src/registry_bridge.rs` accesses plugin functionality through `self.module().create_table_provider()`, which chains through `PluginHandle::module()` to obtain `DfPluginMod_Ref` and then invokes a function pointer. This is an inherent consequence of the ABI-stable architecture and is acceptable. The minor violation is that `PluginHandle::require_capability` accesses `self.manifest()` — a method on `self` — and is therefore not a Demeter violation; that part is clean.

**Findings:**
- `rust/df_plugin_host/src/registry_bridge.rs:109`: `let create_table_provider = self.module().create_table_provider();` then called on the next line as `create_table_provider(...)`. This splits obtaining the function pointer from calling it across two statements, creating a transient local that holds a piece of the plugin's internal module surface. A `create_table_provider_ffi` method on `PluginHandle` that wraps both steps would keep callers at arm's length.
- `rust/df_plugin_host/src/registry_bridge.rs:71-80`: `register_table_functions` calls `(self.module().exports())()` twice in different methods — once in `register_udfs` (implicitly via `udf_bundle_with_options`) and again for table functions. Each call re-invokes `exports()` via the FFI boundary.

**Suggested improvement:**
Add `PluginHandle::invoke_create_table_provider(name, options) -> Result<FFI_TableProvider>` to encapsulate the function-pointer extraction and call. Similarly, cache the result of `exports()` in `PluginHandle` at load time (it is called multiple times by different callers).

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, Don't Ask — Alignment: 2/3

**Current state:**
`physical_rules.rs` and `planner_rules.rs` both use an `ensure_*_config` pattern that asks the config extensions for presence, inserts a default if absent, then returns a mutable reference. This is a getter-then-setter pattern.

**Findings:**
- `rust/datafusion_ext/src/physical_rules.rs:64-82`: `ensure_physical_config` checks `options.extensions.get::<CodeAnatomyPhysicalConfig>().is_none()`, inserts, then returns `get_mut`. Callers must call `ensure_physical_config` before using the config, creating an implicit prerequisite.
- `rust/datafusion_ext/src/planner_rules.rs:81-97`: `ensure_policy_config` is the same pattern for `CodeAnatomyPolicyConfig`.

**Suggested improvement:**
Replace the two `ensure_*` functions with a `get_or_insert_default_config` approach using a `ConfigOptions` extension trait, or simply call `get_or_default()` at the use site. The existing pattern is not harmful but its asymmetry (you must call `ensure_*` before anything else works) is a non-obvious precondition.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality

#### P23. Design for Testability — Alignment: 1/3

**Current state:**
The `df_plugin_codeanatomy` crate has four inline `#[test]` functions in `lib.rs`, of which three require an async `tokio::runtime::Runtime` and one (`plugin_snapshot_matches_native`) round-trips UDFs through the full FFI stack. These are integration-level tests embedded in the production module. The pure logic functions (`resolve_udf_policy`, `config_options_from_udf_options`, `delta_scan_config_from_options`) are testable independently but lack unit tests.

The `build_table_functions` function at `lib.rs:307` creates `SessionContext::new()` inside itself, making it impossible to test with a custom-configured context.

The `df_plugin_host` crate's `validate_manifest` is the only pure function with isolated unit tests (in `loader.rs:91-133`). The registry bridge operations (`register_udfs`, `register_table_functions`, `register_table_providers`) have no unit tests; they are only exercised end-to-end.

`datafusion_ext/src/planner_rules.rs` and `physical_rules.rs` have no tests for the `validate_plan_policy` recursive walk or the physical optimizer dispatch.

**Findings:**
- `rust/df_plugin_codeanatomy/src/lib.rs:107-201`: `resolve_udf_policy` and `config_options_from_udf_options` are pure functions with no tests. These encode behavioral rules (async enabled iff `enable_async=true`, required fields when async is on) that would benefit from table-driven unit tests.
- `rust/df_plugin_codeanatomy/src/lib.rs:308`: `SessionContext::new()` inside `build_table_functions` prevents injection of a configured context for testing.
- `rust/df_plugin_host/src/registry_bridge.rs`: No tests for the capability check (`require_capability`) path when capabilities are missing.
- `rust/datafusion_ext/src/planner_rules.rs:106-134`: `validate_plan_policy` recursive plan walk has no test for DDL/DML blocking scenarios.
- `rust/datafusion_ext/src/physical_rules.rs:87-114`: `CodeAnatomyPhysicalRule::optimize` has no test verifying that the `enabled=false` early-return path works.

**Suggested improvement:**
1. Extract `resolve_udf_policy` and `config_options_from_udf_options` into a `policy.rs` submodule of `df_plugin_codeanatomy`, add `#[cfg(test)]` table-driven tests directly there.
2. Make `build_table_functions` accept `&SessionContext` as a parameter so tests can pass a pre-built context.
3. Add a `#[test]` for `validate_plan_policy` covering each blocked plan kind.
4. Add a `#[test]` for `CodeAnatomyPhysicalRule` with `enabled=false` config.

**Effort:** medium
**Risk if unaddressed:** high — `resolve_udf_policy` encodes correctness invariants (async settings require `enable_async=true`) that are currently verified only by runtime failures

---

#### P24. Observability — Alignment: 1/3

**Current state:**
`delta_control_plane.rs` and `delta_mutations.rs` use `#[instrument(skip(...))]` consistently. However, the plugin entry points in `df_plugin_codeanatomy/src/lib.rs` have no tracing whatsoever. Failures in `build_udf_bundle` (`lib.rs:293-305`) are swallowed with `eprintln!`, which is invisible to any structured log sink. The planner and physical optimizer rules (`planner_rules.rs`, `physical_rules.rs`) have no spans or counters.

**Findings:**
- `rust/df_plugin_codeanatomy/src/lib.rs:293-305`: `build_udf_bundle()` silently prints `"Failed to build UDF bundle: {err}"` to stderr but does not emit any structured event. A production deployment that routes logs to a log aggregator (e.g., OpenTelemetry OTLP) would never see this failure.
- `rust/df_plugin_codeanatomy/src/lib.rs:313-320`: `build_table_functions` has an `eprintln!` error path with the same issue.
- `rust/datafusion_ext/src/planner_rules.rs:69-78`: `CodeAnatomyPolicyRule::analyze` produces no trace event when it blocks a DDL or DML plan. Operators debugging a policy rejection get no structured context.
- `rust/datafusion_ext/src/physical_rules.rs:87-114`: `CodeAnatomyPhysicalRule::optimize` has no span. Optimizer behavior (which rules fired, what was added) is invisible.
- `rust/datafusion_ext/src/async_runtime.rs`: No observability on runtime initialization failures or spawn panics.

**Suggested improvement:**
1. Replace `eprintln!` in `df_plugin_codeanatomy/lib.rs:293,317` with `tracing::error!(error = %err, "Failed to build UDF bundle")` / `"Failed to build table UDF {name}"`. The `tracing` crate is already in the workspace.
2. Add `#[instrument]` to `CodeAnatomyPolicyRule::analyze` with `skip(plan)` and a `plan_kind = ?plan.name()` field so rejections are traceable.
3. Add `#[instrument(skip(plan, config))]` to `CodeAnatomyPhysicalRule::optimize`.

**Effort:** small
**Risk if unaddressed:** medium — silent failures in plugin initialization degrade operational visibility; blocked DDL/DML is hard to correlate to a plan rejection without structured events

---

## DF52 Migration Impact

### H) CoalesceBatches Removal (Breaking — physical_rules.rs)

**Affected file:** `rust/datafusion_ext/src/physical_rules.rs:6-7,103`

DF52 removes `CoalesceBatchesExec` as a standalone operator (section H of the DF52 catalog). The current import at line 6 is `datafusion::physical_optimizer::coalesce_batches::CoalesceBatches` and at line 7 `datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec`. `CoalesceBatches::new().optimize(optimized, config)?` at line 103 uses `CoalesceBatches` as an optimizer rule that wraps the plan in a `CoalesceBatchesExec`. In DF52, coalescing is integrated into individual operators, so this rule becomes a no-op at best and a broken import at worst.

**Required action:** Remove the `coalesce_batches` field from `CodeAnatomyPhysicalConfig` and the corresponding `CoalesceBatches::new().optimize(...)` call. Remove the import. The DF52 runtime will coalesce within operators automatically.

**LOC reduction:** ~15 LOC in `physical_rules.rs` (the field, the `impl_extension_options!` entry, and the optimizer call).

### I) UDAF Semantics Tightening (udaf_builtin.rs — audit required, mostly already compliant)

**Affected file:** `rust/datafusion_ext/src/udaf_builtin.rs`

DF52 requires explicit `AggregateUDFImpl::supports_within_group_clause()` returning `true` for any UDAF that accepts `WITHIN GROUP (ORDER BY ...)`. DF52 also tightens `supports_null_handling_clause()` to default `false`; `IGNORE NULLS` / `RESPECT NULLS` now error unless explicitly opted in.

Audit result from reading the file: `ListUniqueUdaf` (line 117), `CollectSetUdaf` (line 196), `CountDistinctUdaf` (line 286) all already return `supports_null_handling_clause() -> bool { true }`. Neither implements `supports_within_group_clause`. The remaining UDAFs (`CountIfUdaf`, `AnyValueDetUdaf`, `ArgMaxUdaf`, `AsofSelectUdaf`, `ArgMinUdaf`, `StringAggDetUdaf`, and the `first_value`/`last_value` delegations) should be audited similarly. The delegations to `first_value_udaf()` and `last_value_udaf()` are safe because they clone the built-in DF UDAF which already handles these hooks correctly.

**Required action:** Verify that no custom UDAF accepts SQL-level `WITHIN GROUP` or `IGNORE/RESPECT NULLS` without the corresponding opt-in method. The three explicitly reviewed are compliant. No `supports_within_group_clause` is needed unless a `list_unique` or similar UDAF is intended to support ordered aggregation.

**LOC impact:** Minimal (at most adding `fn supports_within_group_clause(&self) -> bool { false }` explicitly where currently relying on the default, for clarity).

### F) ExprPlanner / RelationPlanner (expr_planner.rs — partial gap)

**Affected file:** `rust/datafusion_ext/src/expr_planner.rs`

DF52 introduces `RelationPlanner` as a new FROM-clause extension point complementing `ExprPlanner`. `CodeAnatomyDomainPlanner` currently only implements `ExprPlanner::plan_binary_op`. The DF52 `ExprPlanner` trait gains no breaking signature change, so `expr_planner.rs` will compile against DF52 without modification. However, the `register_expr_planner` / `register_function_rewrite` API is stable and unchanged.

**Required action:** No breaking change. The DF52 opportunity is to implement `RelationPlanner` if custom FROM-clause syntax is needed; this is optional.

### D) Scan Pushdown / Projection (delta_control_plane.rs — requires audit on upgrade)

**Affected file:** `rust/datafusion_ext/src/delta_control_plane.rs`

DF52 moves projection pushdown from `FileScanConfig` to `FileSource` (the `try_pushdown_projection` API). The `delta_control_plane.rs` module uses `DeltaScanConfig` and `DeltaTableProvider` from `deltalake`, which may wrap these internally. The impact depends on the `deltalake` version's own adaptation. `Cargo.toml` pins `deltalake = "0.30.1"` — this version predates DF52 support in the delta-rs Rust crate. The DF52 migration of this file is therefore blocked on `deltalake` upgrading to a DF52-compatible release.

**Required action:** Track `deltalake` DF52 support milestone. The `delta_control_plane.rs` code itself does not directly use `FileSource::with_projection`; the breaking change is mediated through `deltalake`.

### K) FFI Boundary — TaskContextProvider (df_plugin_host registry_bridge.rs)

**Affected file:** `rust/df_plugin_host/src/registry_bridge.rs`

DF52 changes the `datafusion-ffi` API so that provider constructors require a `TaskContextProvider` (typically `SessionContext`) and optionally a `LogicalExtensionCodec`. The current `FFI_TableProvider::new(Arc::new(provider), true, None)` call in `df_plugin_codeanatomy/lib.rs:431,462` passes `None` for the codec. After the DF52 FFI upgrade, this signature changes.

**Required action:** When upgrading `datafusion-ffi` from 51.0.0 to 52.0.0, update `FFI_TableProvider::new(...)` call sites in `df_plugin_codeanatomy/src/lib.rs:431,462` to supply a `TaskContextProvider`. The `registry_bridge.rs` side (`ForeignTableProvider::from(&provider)`) will also require the DF52-updated signature.

---

## Planning-Object Consolidation

### Planner List Duplication

**Affected files:**
- `rust/datafusion_ext/src/lib.rs:55-90` (`install_expr_planners_native`)
- `rust/datafusion_ext/src/lib.rs:107-121` (`domain_expr_planners` / `domain_function_rewrites`)

`install_expr_planners_native` constructs and registers `NestedFunctionPlanner`, `FieldAccessPlanner`, and (conditionally) `CodeAnatomyDomainPlanner`. `domain_expr_planners()` returns the same three planners as a `Vec`. These two code paths encode the same planner set twice. If a new planner is added to `domain_expr_planners()` but not to `install_expr_planners_native`, the session-mutation path silently diverges from the builder path.

**Assessment:** (a) Rust capability is adequate to fix this — `install_expr_planners_native` should consume `domain_expr_planners()` internally. (b) Not DF52 obsolescence; this is a pre-existing duplication. (c) No DF52 built-in replaces custom planners. (d) LOC reduction: ~10 LOC by collapsing the enumeration.

**Suggested improvement:** In `install_expr_planners_native`, replace the manual planner instantiation block with:
```rust
for planner in domain_expr_planners() {
    state.register_expr_planner(planner)?;
}
for rewrite in domain_function_rewrites() {
    state.register_function_rewrite(rewrite)?;
}
```
This makes `domain_expr_planners()` the single source of truth.

### DeltaScanConfig Construction Duplication

**Affected files:**
- `rust/datafusion_ext/src/delta_control_plane.rs:206-259` (`scan_config_from_session`)
- `rust/df_plugin_codeanatomy/src/lib.rs:376-396` (`delta_scan_config_from_options`)

Both functions construct a `DeltaScanConfig` from a payload of options plus an optional `EagerSnapshot`. They apply overrides in a similar pattern. The plugin version (`delta_scan_config_from_options`) partially delegates to `apply_file_column_builder` for the snapshot-aware path, but the field assignment logic is duplicated between the two.

**Assessment:** (a) The duplication is structural — the plugin receives a JSON payload while the library receives a typed `DeltaScanOverrides`. Unifying them requires either making the plugin pass through `DeltaScanOverrides` (defeating the ABI isolation) or introducing a shared typed config builder that both call. (b) Not DF52-specific. (c) No DF52 built-in replaces this. (d) LOC: ~30 LOC of duplication.

**Suggested improvement:** Add a `DeltaScanConfigBuilder` wrapper in `datafusion_ext::delta_control_plane` that accepts a `DeltaScanFieldSet` (a new typed struct encoding the same fields as `DeltaScanConfigPayload` but without JSON concerns), and have both `scan_config_from_session` and the plugin's `delta_scan_config_from_options` delegate to it after deserializing.

---

## Rust Migration Candidates (Python -> Rust)

The following Python-side `datafusion_engine/` submodules implement logic that the Rust crates already partially or fully implement natively. Migration would reduce FFI round-trips and increase correctness surface.

| Python Module | Rust Equivalent | Migration Readiness | LOC Reduction Estimate | Notes |
|---|---|---|---|---|
| `src/datafusion_engine/udf/signature.py` | `rust/datafusion_ext/src/udf/common.rs` + `udf_config.rs` | High | ~80 LOC Python | Python module constructs `TypeSignature` objects; Rust already owns canonical signature construction |
| `src/datafusion_engine/extensions/plugin_manifest.py` | `rust/df_plugin_host/src/loader.rs:validate_manifest` | Medium | ~146 LOC Python | Python resolves `.dylib`/`.so` path and calls into Rust. The Rust `validate_manifest` already does the version checks. Python wrapper is a file-resolution shim; may need to stay for path discovery. |
| `src/datafusion_engine/delta/capabilities.py` | `rust/datafusion_ext/src/delta_protocol.rs` | Medium | ~60 LOC Python | Python probes Delta extension by calling an entrypoint; Rust `delta_protocol.rs` has `gate_from_parts` / `protocol_gate` as the canonical gate check. Python layer adds extension-loading indirection. |
| `src/datafusion_engine/delta/cdf.py` | `rust/datafusion_ext/src/delta_control_plane.rs:delta_cdf_provider` | High | ~100 LOC Python | Python CDF logic is a thin wrapper over what `delta_cdf_provider` already does in Rust; only the Python → FFI call layer is needed |
| `src/datafusion_engine/sql/guard.py` | `rust/datafusion_ext/src/planner_rules.rs:CodeAnatomyPolicyRule` | High | ~60 LOC Python | Python SQL guard validates plan types; Rust `planner_rules.rs` already implements `AnalyzerRule` for the same. Python layer may be redundant. |

---

## Cross-Cutting Themes

### 1. Plugin Module as God Object

**Root cause:** The `df_plugin_codeanatomy/src/lib.rs` module was designed as a single-file FFI harness, which is idiomatic for small plugins but has grown beyond that role (694 LOC). FFI harnesses accumulate concerns because every piece of external functionality must be plumbed through `extern "C"` entry points, and it is tempting to inline the logic near the entry points.

**Affected principles:** P2 (Separation of Concerns), P3 (SRP), P23 (Testability)

**Suggested approach:** Apply the four-module split described under P2. The FFI entry points remain in `lib.rs` as one-liners that delegate to the submodule functions.

### 2. DataFusion 51.x Pin Creates Active Divergence Risk

**Root cause:** `Cargo.toml` pins `datafusion = "51.0.0"` throughout. The `compat.rs` comment explicitly documents this. Three DF52 breaking changes are imminent: `CoalesceBatchesExec` removal, `FFI_TableProvider::new` signature change, and `FileScanConfig` statistics move (mediated via `deltalake`). The codebase is technically correct today but will require coordinated changes across `physical_rules.rs`, `df_plugin_codeanatomy/lib.rs`, and `delta_control_plane.rs` during the upgrade.

**Affected principles:** P6 (Ports & Adapters), P22 (Declare and version public contracts — the DF version dependency is implicit)

**Suggested approach:** Add a `DATAFUSION_VERSION_COMPAT` constant or a workspace-level feature flag (e.g., `datafusion-52`) that gates the breaking-change migration paths. The `compat.rs` comment already acknowledges this; formalize it as a migration checklist in `rust/datafusion_ext/AGENTS.md` or a `MIGRATION.md`.

### 3. Observability Gap at Plugin Boundaries

**Root cause:** The `tracing` crate is used correctly in `delta_control_plane.rs` and `delta_mutations.rs` (where slow async operations occur) but is absent from the plugin FFI entry points and optimizer rule implementations. This is the pattern of adding observability only where performance is obviously critical and missing the boundary-crossing events.

**Affected principles:** P24 (Observability)

**Suggested approach:** Add `tracing::error!` for all `eprintln!` error paths and `#[instrument]` for the policy/physical rule `analyze`/`optimize` implementations. This is low-effort (small) but requires knowing that `tracing` is already a workspace dependency.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P24 Observability | Replace `eprintln!` in `df_plugin_codeanatomy/lib.rs:293,317` with `tracing::error!` | small | Immediate improvement in production visibility |
| 2 | DF52 — CoalesceBatches | Remove `CoalesceBatches` import and rule application from `physical_rules.rs:6,103` now, ahead of DF52 upgrade | small | Eliminates a known breaking compile error on upgrade |
| 3 | P12 Composition | Make `install_expr_planners_native` consume `domain_expr_planners()` to eliminate the planner list duplication in `lib.rs:55-90` | small | Single source of truth for planner registration |
| 4 | P23 Testability | Add unit tests for `resolve_udf_policy` and `validate_plan_policy` | medium | Guards behavioral invariants currently only caught at runtime |
| 5 | P2/P3 Separation | Split `df_plugin_codeanatomy/src/lib.rs` into `options.rs`, `udf_bundle.rs`, `providers.rs`, `lib.rs` | medium | Makes the largest non-test file maintainable and testable |

---

## Recommended Action Sequence

1. **[small, DF52 prep]** Remove `CoalesceBatches` optimizer wrapping from `rust/datafusion_ext/src/physical_rules.rs` (imports at lines 5-7 and call at line 103) ahead of the DataFusion 52 upgrade. Remove the `coalesce_batches` config field and its `impl_extension_options!` entry. Document this in the DF52 migration checklist.

2. **[small, observability]** Replace the two `eprintln!` calls in `rust/df_plugin_codeanatomy/src/lib.rs` (lines 293 and 317) with `tracing::error!` structured events. Add `tracing::instrument` to `CodeAnatomyPolicyRule::analyze` (with `plan_kind` field) and `CodeAnatomyPhysicalRule::optimize`.

3. **[small, composition]** In `rust/datafusion_ext/src/lib.rs`, make `install_expr_planners_native` delegate to `domain_expr_planners()` and `domain_function_rewrites()` rather than duplicating the planner list. This establishes a single authoritative enumeration.

4. **[medium, testability]** Add table-driven unit tests for `resolve_udf_policy` and `config_options_from_udf_options` directly in a new `policy.rs` submodule of `df_plugin_codeanatomy`. Add tests for `validate_plan_policy` (blocking DDL/DML by policy) and `CodeAnatomyPhysicalRule::optimize` (enabled=false path).

5. **[medium, separation]** Split `rust/df_plugin_codeanatomy/src/lib.rs` into `options.rs` (deserialization structs), `udf_bundle.rs` (bundle construction and policy), `providers.rs` (Delta provider construction), and `lib.rs` (FFI entry points only). This also enables step 4 by making the pure functions independently importable.

6. **[medium, DF52 prep + FFI]** Before DataFusion 52 upgrade, audit `FFI_TableProvider::new(...)` call sites in `rust/df_plugin_codeanatomy/src/lib.rs:431,462` and plan the `TaskContextProvider` parameter addition. Update `rust/df_plugin_host/src/registry_bridge.rs` corresponding `ForeignTableProvider::from(...)` call. Coordinate with `deltalake` version tracking.

7. **[medium, DRY]** Introduce a shared `DeltaScanConfigBuilder` in `datafusion_ext::delta_control_plane` that consolidates the duplicated field-assignment logic between `scan_config_from_session` and `df_plugin_codeanatomy::delta_scan_config_from_options`.

8. **[large, optional migration]** Evaluate migrating `src/datafusion_engine/sql/guard.py` (Python SQL plan guard) to a pure delegation to `CodeAnatomyPolicyRule` via the existing plugin registration path. This would eliminate Python-side plan type checking that duplicates the Rust `AnalyzerRule`.
