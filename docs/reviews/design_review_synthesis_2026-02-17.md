# Design Review Synthesis: Cross-Cutting Findings

**Date:** 2026-02-17
**Source reviews:** 12 independent agent reviews covering all major subsystems
**Scope:** Full codebase — Python datafusion layer, Rust crates, semantic pipeline, extraction, obs/relspec/CLI/utils

---

## 1. Executive Summary

The CodeAnatomy codebase is in a strong but transitional architectural state. The core
computation model — inference-driven scheduling, DataFusion-backed query execution,
Hamilton DAG orchestration, and a Rust-accelerated engine — is well-conceived and
increasingly well-executed. Several subsystems (the Rust `codeanatomy_engine` crate,
the semantic IR pipeline, the Delta control plane, the observability layer) have reached
a level of design maturity that should be used as reference patterns across the rest of the
codebase.

Three strategic-level problems dominate the findings across all 12 reviews.

**First, the DF52 migration is the most urgent external constraint.** All Rust crates are
pinned at DataFusion 51.0.0. At least one change — the FFI provider `session` parameter
requirement — is a blocking compile-time break that must be resolved in
`rust/datafusion_python/` before any downstream crate can upgrade. Several other DF52
changes (`CoalesceBatchesExec` removal, `FileScanConfig` projection API, `deltalake`
version alignment) will cause compile-time failures on upgrade. The Python layer has
additional DF52 exposure through the `__datafusion_*_provider__` PyCapsule signature
change. Failing to address these in dependency order will prevent the entire stack from
adopting DF52's substantial new capabilities (dynamic filter pushdown, `FileStatisticsCache`,
`RelationPlanner`, sort pushdown to scans).

**Second, parallel Python/Rust implementations of the same logic are the most significant
ongoing maintenance burden.** The Python lineage walker (`plan/walk.py`,
`lineage/reporting.py` — ~700 LOC of `getattr`-chain code) duplicates the Rust lineage
extractor (`compiler/lineage.rs`). The Python `SessionFactory.build_config()` duplicates
the Rust `session/factory.rs` `SessionStateBuilder` path. Four extractor runtime files
in `src/extract/` duplicate ~3,000 LOC of orchestration scaffolding that belongs in a
single shared loop. The `bundle_environment.py` / `plan_introspection.py` duplication
is byte-for-byte identical across 12 functions. Each parallel implementation requires
synchronized changes and is a permanent divergence risk. The highest-ROI architectural
investment is eliminating these parallel surfaces through Rust delegation or shared
Python infrastructure.

**Third, a cluster of correctness and observability gaps require attention before the
next major feature phase.** The `DeltaDeleteRequest.predicate: str | None` footgun can
cause accidental full-table deletion; DF52 Issue #19840 worsens this risk. The
`infer_semantics()` broad exception catch in the semantic pipeline silently converts
programming errors into invisible quality degradation. Three distinct coercion modules
diverge in `bool` handling. The session construction pipeline has global mutable state
that makes unit testing prohibitively complex and creates non-idempotent extension
installation paths.

The strategic direction for the next 6-12 months should be: (1) execute the DF52 migration
in strict dependency order, starting with `datafusion_python`; (2) progressively retire
Python parallel implementations in favor of Rust delegation; and (3) address the
correctness and observability gaps as pre-migration safety work, before the upgrade
introduces new failure modes.

---

## 2. Cross-Cutting Themes

### Theme A: Parallel Python/Rust Implementations

**Agents identifying this:** Agent 1 (Session), Agent 3 (Delta/Storage), Agent 4
(Plan/Lineage/Views), Agent 7 (Rust Core Engine), Agent 9 (Rust Python Bindings)

**Specific instances:**

| Python Surface | LOC | Rust Equivalent | Review |
|---|---|---|---|
| `lineage/reporting.py` + `plan/walk.py` | ~700 | `compiler/lineage.rs` (DataFusion `TreeNode::apply`) | Agent 4: `src/datafusion_engine/lineage/reporting.py:1-569`, `plan/walk.py:74-109` |
| `session/context_pool.py:263-382` (`SessionFactory.build_config`) | ~120 | `session/factory.rs` (`SessionStateBuilder`) | Agent 1: `context_pool.py:263-382` |
| `session/runtime_extensions.py:200-240` (planner rule installation) | ~40 | `session/planning_surface.rs` (`apply_to_builder`) | Agent 1: `runtime_extensions.py:200-240` |
| `plan/bundle_environment.py` + `plan/plan_introspection.py` | ~400 | N/A (full Python duplication) | Agent 4: `bundle_environment.py:17-219` and `plan_introspection.py:19-228` |
| `io/write_delta.py` (Delta write orchestration) | 734 | `executor/delta_writer.rs` (461 LOC) | Agent 6: `io/write_delta.py` |
| `extract/extractors/*/builders_runtime.py` × 4 | ~4,200 total | Shared loop does not exist yet | Agent 11: four extractor pairs |
| `sql/guard.py` (SQL plan type checking) | ~60 | `planner_rules.rs:CodeAnatomyPolicyRule` | Agent 8: `datafusion_ext/src/planner_rules.rs:69-78` |

**Overall severity:** High. Each parallel implementation is a permanent maintenance tax
and a source of silent divergence. The Python lineage walker is the single highest-impact
elimination target.

**Recommended approach:** Route Python callers through Rust bridge payload extensions
rather than maintaining parallel Python visitors. Specifically, extend the Rust bridge
payload to return `LineageReport` as structured JSON so `reporting.py` becomes a pure
msgspec deserialization target.

---

### Theme B: Hidden Session Construction (Bare `SessionContext()` Anti-Pattern)

**Agents identifying this:** Agent 1 (Session), Agent 2 (Schema/Arrow/Encoding)

**Specific instances:**

| Location | Context |
|---|---|
| `src/datafusion_engine/encoding/policy.py:138-139` | Creates bare `SessionContext()` per `apply_encoding` call — no UDFs, no memory pool, no catalog |
| `src/datafusion_engine/schema/validation.py:112-115` | Same pattern for schema validation SQL queries |
| `src/datafusion_engine/schema/validation.py:603` | Via `_session_context()` helper |

**Root cause:** DataFusion operations that could be performed in a caller-provided session
are instead given an ephemeral local session. The encoding case (`policy.py`) can be
replaced entirely with a PyArrow-native dictionary cast, eliminating the DataFusion
dependency. The validation case is appropriate DataFusion usage but needs an injected
session.

**Overall severity:** Medium. The encoding case has a straightforward fix (PyArrow-native
cast, ~30 LOC elimination). The validation case needs a shared
`session_context_for_schema_ops(runtime_profile)` utility.

---

### Theme C: DRY Violations at Type-Mapping Boundaries

**Agents identifying this:** Agent 2 (Schema), Agent 4 (Plan/Lineage), Agent 5
(UDF/Dataset), Agent 10 (Semantic Pipeline)

**Specific instances:**

| Domain | Locations | Description |
|---|---|---|
| Arrow type-string normalization | `schema/type_normalization.py`, `schema/contracts.py:492-499`, `schema/type_resolution.py:12-23`, `schema/field_types.py:27-66` | Four separate normalizations of Arrow type aliases (`largeutf8`, `utf8`, etc.) |
| Arrow→DDL type mapping | `dataset/ddl_types.py`, `udf/extension_ddl.py:75-127` | Two independent paths for the same conversion |
| Information-schema introspection | `plan/bundle_environment.py:17-219` + `plan/plan_introspection.py:19-228` | 12 functions duplicated byte-for-byte |
| Join strategy confidence constants | `semantics/joins/inference.py:49-54` + `semantics/ir_pipeline.py:1086-1091` | Identical float values; acknowledged in a docstring comment |
| Coercion utilities | `utils/coercion.py`, `utils/value_coercion.py`, `cli/commands/build.py:862-902` | Three modules with diverging `bool` handling |
| `_WriterPort` Protocol | `cache/inventory.py:91-94` + `cache/ledger.py:46-49` | Identical definitions in two sibling modules |

**Overall severity:** Medium. The `plan_introspection.py` duplication is particularly
hazardous (0/3 DRY score from Agent 4) because docstrings have already diverged, meaning
the implementations will drift. The confidence constant duplication is acknowledged but
unresolved.

---

### Theme D: Command-Query Separation (CQS) Violations at Registration Boundaries

**Agents identifying this:** Agent 5 (UDF/Dataset), Agent 6 (IO/Extensions/Catalog),
Agent 10 (Semantic Pipeline)

**Specific instances:**

| Location | Violation |
|---|---|
| `dataset/registration_core.py:351-418` (`register_dataset_df`) | Registers a table (command) and returns a `DataFrame` (query) |
| `udf/extension_registry.py:29-71` (`register_rust_udfs`) | Installs UDFs (command) and returns snapshot (query) |
| `tables/registration.py:76-134` (`register_table`) | Registers table and returns `DataFrame` |
| `semantics/compiler.py:610-625` (`SemanticCompiler.get_or_register`) | Returns `TableInfo` and mutates `self._tables` |
| `plan/bundle_assembly.py:229-267` (`_plan_core_components`) | Extracts plan layers and records timing as a side effect |

**Root cause:** DataFusion's API style (register then `ctx.table(name)`) encourages this
pattern, but the wrappers can and should decouple registration from query.

**Overall severity:** Medium. The pattern makes retry logic, caching, and testing harder.
Any caller needing only the snapshot or DataFrame must trigger hidden side effects.

---

### Theme E: Silent Error Swallowing Without Observability

**Agents identifying this:** Agent 3 (Delta/Storage), Agent 4 (Plan/Lineage), Agent 8
(Rust DF Extensions), Agent 10 (Semantic Pipeline)

**Specific instances:**

| Location | What is swallowed |
|---|---|
| `semantics/ir_pipeline.py:1292-1301` | `(AttributeError, KeyError, TypeError, ValueError)` during join inference — converts programming errors to silent `None` |
| `lineage/reporting.py:186-198` (`_safe_attr`) | `RuntimeError`, `TypeError`, `ValueError` from DataFusion API probing |
| `plan/profiler.py:111-116` | `PanicException` detected by string comparison `exc.__class__.__name__ == "PanicException"` |
| `delta_maintenance.py:83-86` | Rust vacuum failure recorded as string attribute, not as span event with stack trace |
| `cache/metadata_snapshots.py:108-113` | SQL cache query failure silently substitutes fallback source before span event |
| `df_plugin_codeanatomy/lib.rs:293,317` | `eprintln!` error paths invisible to structured log sinks |

**Overall severity:** Medium-High. The semantic pipeline case is particularly dangerous:
`TypeError` and `AttributeError` are programming errors that should surface, not be
silently treated as "no join strategy inferred."

---

### Theme F: DF52 Migration Exposure Across All Rust Layers

**Agents identifying this:** Agents 1, 2, 3, 6, 7, 8, 9

**Summary:** All Rust crates are pinned at DataFusion 51.0.0. The migration must proceed
in dependency order (see Section 4 for the full critical path). The highest-risk changes are:

1. `FFI_TableProvider`/`FFI_CatalogProvider` constructors require `TaskContextProvider`
   (DF52 §K) — affects `rust/datafusion_python/` first, then all downstream crates.
2. `CoalesceBatchesExec` removal — causes compile-time failures in `rules/physical.rs`
   (both `codeanatomy_engine` and `datafusion_ext`).
3. `FileScanConfig` projection API change — affects `providers/scan_config.rs` and
   `delta_control_plane.rs` (mediated through `deltalake` version).
4. `deltalake = "0.30.1"` must upgrade to a DF52-compatible release before
   `delta_control_plane.rs` can compile.

---

### Theme G: `DataFusionRuntimeProfile` as a Data-Bag Interrogated by All Consumers

**Agents identifying this:** Agent 1 (Session), Agent 6 (IO/Extensions/Catalog)

**Specific instances:**

| Location | Violation |
|---|---|
| `io/write_pipeline.py:189` | `profile.data_sources.dataset_templates` (two levels) |
| `io/write_pipeline.py:190` | `profile.data_sources.extract_output.dataset_locations` (three levels) |
| `io/write_pipeline.py:209` | `self.runtime_profile.policies.delta_store_policy` (two levels) |
| `kernels.py:189` | `runtime_profile.execution.target_partitions` (two levels) |
| `kernels.py:193` | `runtime_profile.policies.join_policy` (two levels) |

**Root cause:** `DataFusionRuntimeProfile` was designed as a configuration container
before consumers needed rich behavior. Consumers reach through two and three levels of
nested attributes rather than asking the profile to answer domain questions.

**Recommended approach:** Add facade query methods to `DataFusionRuntimeProfile`:
`dataset_candidates(destination)`, `join_repartition_enabled(keys)`,
`effective_sql_options()`, `schema_hardening_view_types()`. These become thin property
wrappers initially but insulate callers from restructuring.

---

### Theme H: Imperative Session Construction With Global Mutable State

**Agents identifying this:** Agent 1 (Session)

**Specific instances:**

| Location | Issue |
|---|---|
| `_session_caches.py:9-11` | Three module-level mutable globals (`SESSION_CONTEXT_CACHE`, `SESSION_RUNTIME_CACHE`, `RUNTIME_SETTINGS_OVERLAY`) |
| `runtime.py:403-437` | 15-step inline imperative pipeline with no formal phase contract |
| `runtime_context.py:64-105` (`_ephemeral_context_phases`) | Cleaner 8-phase model that exists only for ephemeral contexts, not the primary path |
| `runtime_extensions.py:200-240` | Extension installation with no idempotency guards |

**Severity:** High. Global state makes unit testing prohibitively complex. Duplicate
rules may be registered silently in pool-reused contexts. The existing
`_ephemeral_context_phases` mechanism should be the canonical path for all contexts.

---

### Theme I: Circular Import Triangles Managed by Deferred Local Imports

**Agents identifying this:** Agent 5 (UDF/Dataset)

**Specific instance:**

`extension_core.py` (bottom of file) imports from `extension_snapshot_runtime.py`, which
imports from `extension_core.py` at module top and from `extension_validation.py` inside
14 function bodies. `extension_validation.py` imports from `extension_core.py`. This
triangle results in ~200 LOC of pure deferred-import indirection wrappers in
`extension_snapshot_runtime.py:42-270`.

**Severity:** Medium. Stable today but a maintenance trap. Adding any module-level import
in `extension_core` that transitively reaches `extension_snapshot_runtime` will produce
a silent `ImportError` or partially-initialized module.

---

### Theme J: Observability Gaps at Plugin and Optimizer Boundaries

**Agents identifying this:** Agent 8 (Rust DF Extensions), Agent 3 (Delta/Storage)

**Specific instances:**

| Location | Gap |
|---|---|
| `df_plugin_codeanatomy/lib.rs:293,317` | `eprintln!` error paths invisible to structured log sinks in production |
| `datafusion_ext/src/planner_rules.rs:69-78` | `CodeAnatomyPolicyRule::analyze` produces no trace event when blocking DDL/DML |
| `datafusion_ext/src/physical_rules.rs:87-114` | `CodeAnatomyPhysicalRule::optimize` has no span |
| `delta_maintenance.py:103` | Rust vacuum fallback error recorded as string attribute, not span event |

**Severity:** Medium. The plugin initialization failures are particularly dangerous in
production — silent `eprintln!` output is invisible to OTLP log aggregators.

---

## 3. Consolidated Rust Migration Roadmap

### Tier 1: Immediate — Python Is Already Redundant

These have an existing Rust counterpart that already handles the concern. The Python
code is pure overhead or fragile duplication.

| Python Module | LOC | Rust Target | Estimated Reduction | Agent |
|---|---|---|---|---|
| `plan/walk.py` + `lineage/reporting.py` (Python plan walker) | ~700 | `compiler/lineage.rs` (`TreeNode::apply`) — exists and complete | ~600 Python LOC; retire entire visitor | Agent 4 |
| `plan/plan_introspection.py` (12 functions) | ~220 | Merge into `plan/bundle_environment.py` (Python) | ~220 LOC (full deletion) | Agent 4 |
| `delta/cdf.py` (CDF provider wrapper) | ~100 | `datafusion_ext/src/delta_control_plane.rs:delta_cdf_provider` | ~100 LOC | Agent 8 |
| `sql/guard.py` (SQL plan type checking) | ~60 | `datafusion_ext/src/planner_rules.rs:CodeAnatomyPolicyRule` | ~60 LOC | Agent 8 |
| `session/runtime_extensions.py:200-240` (planner rule install) | ~40 | `session/planning_surface.rs:apply_to_builder` | ~40 LOC | Agent 1 |

**Total Tier 1 estimated reduction: ~1,020 Python LOC**

---

### Tier 2: High Value — Performance-Critical, Natural Rust Affinity

These are currently Python but the Rust path is clearly superior for performance or
correctness, and the infrastructure for migration exists.

| Python Module | LOC | Rust Target | Estimated Reduction | Complexity | Agent |
|---|---|---|---|---|---|
| `udf/extension_validation.py` + `udf/extension_snapshot_runtime.py` | ~976 combined | Replace with `msgspec.convert(raw_bytes, type=RustUdfSnapshot)` once Rust emits typed contract | ~800 LOC | Medium | Agent 5 |
| `udf/signature.py` (type string parser) | 205 | `datafusion_ext/src/udf/common.rs` + `udf_config.rs` | ~80 LOC Python | High | Agents 5, 8 |
| `io/write_delta.py` (Delta write orchestration) | 734 | `codeanatomy_engine/src/executor/delta_writer.rs` (461 LOC, exists) | ~400+ LOC | Medium | Agent 6 |
| `semantics/span_normalize.py` + `normalization_helpers.py` (byte-span canonicalization) | ~120 | Rust `ScalarUDF` or `TableProvider` for line-index join | ~120 LOC | Medium | Agent 10 |
| `extract/extractors/tree_sitter/` (full tree-sitter extraction) | ~1,300 | `tree-sitter-python` Rust crate + Arrow `RecordBatch` emission | ~1,100 LOC | High | Agent 11 |
| `kernels.py` — interval-align join (~290 LOC) | ~290 | Rust `TableProvider` using sort-merge join + DF52 sort pushdown | ~250 LOC | Large | Agent 6 |
| `session/context_pool.py:263-382` (`SessionFactory.build_config`) | ~120 | `session/factory.rs` (`SessionStateBuilder`) | ~120 LOC | Large | Agent 1 |
| `relspec/policy_compiler.py` (cache-policy graph traversal) | ~80 target section | Rust graph traversal via `codeanatomy_engine::compiler::scheduling.rs` | ~80 LOC | Medium | Agent 12 |

**Total Tier 2 estimated reduction: ~2,970 Python LOC**

---

### Tier 3: Future — Nice-to-Have, Lower Priority

| Python Module | LOC | Rust Target | Estimated Reduction | Notes | Agent |
|---|---|---|---|---|---|
| `udf/extension_ddl.py` (DDL name generation) | 178 | Rust snapshot pre-computes DDL type strings | ~100 LOC | Low urgency | Agent 5 |
| `datafusion_engine/delta/capabilities.py` | ~60 | `datafusion_ext/src/delta_protocol.rs:gate_from_parts` | ~60 LOC | Thin wrapper | Agent 8 |
| `extensions/plugin_manifest.py` | ~146 | `df_plugin_host/src/loader.rs:validate_manifest` | Partial (~50 LOC) | Path discovery must stay | Agent 8 |
| `semantics/compiler.py:239-266` (`_stable_id_expr`) | ~20 per spec | Rust multi-arg UDF `stable_id_from_span` | Minor | Plan-compile time benefit | Agent 10 |
| `datafusion_engine/schema/alignment.py::align_to_schema` | Moderate | Rust `PhysicalExprAdapter` at `TableProvider` boundary | Moderate | Not a current bottleneck | Agent 2 |
| `datafusion_engine/arrow/coercion.py` (Arrow type normalization) | Moderate | Rust `DataType` clone+eq operations | Moderate | Not a current bottleneck | Agent 2 |
| `codeanatomy_ext/helpers.py` — pure Rust helpers | ~100 | Move non-PyO3 helpers to `datafusion_ext` | ~100 LOC | Reduces binding footprint | Agent 9 |
| `relspec/inferred_deps.py` coordination layer | Thin | `codeanatomy_engine::compiler::scheduling.rs` | Moderate | Already Rust-backed | Agent 7, 12 |

**Total Tier 3 estimated reduction: ~550+ Python LOC (partial)**

---

## 4. DF52 Migration Critical Path

### Blocking Changes (Must Fix Before Upgrade) — Ordered by Dependency

The migration must proceed in this strict order because downstream crates transitively
depend on `datafusion_python` for the Python binding surface.

**Step 1: `rust/datafusion_python/` — THE MIGRATION GATE**
(Agent 9; `context.rs:607`, `catalog.rs:116,363`, `utils.rs:170`)

All `__datafusion_*_provider__` call sites must be updated to pass `session` as the
first argument, following DF52 §K. This is a compile-time requirement; no downstream
crate can adopt DF52 until this is resolved.

Specific changes required:
- `utils.rs:170-171`: `obj.getattr("__datafusion_table_provider__")?.call0()?` → pass session
- `context.rs:607-609`: `provider.getattr("__datafusion_catalog_provider__")?.call0()?` → pass session
- `catalog.rs:116-118,363-365`: `schema_provider.getattr("__datafusion_schema_provider__")?.call0()?` → pass session
- `helpers.rs:136`, `schema_evolution.rs:378`: `FFI_TableProvider::new(...)` constructor signature change
- Update `Cargo.toml` from `datafusion = "51"` to `datafusion = "52"`

**Step 2: `rust/datafusion_ext/` — Rust Extension Library**
(Agent 8; `physical_rules.rs:6-7,103`)

- Remove `CoalesceBatches` import and optimizer wrapping from `physical_rules.rs:103` — this is a compile-time break (the type no longer exists in DF52)
- Remove `coalesce_batches` field from `CodeAnatomyPhysicalConfig`
- Update `FFI_TableProvider::new(...)` at `df_plugin_codeanatomy/src/lib.rs:431,462` to supply `TaskContextProvider`
- Audit `delta_control_plane.rs` `DeltaScanConfigBuilder` against DF52's `FileSource::try_pushdown_projection` API (blocked on `deltalake` DF52 release)

**Step 3: `rust/codeanatomy_engine/` — Core Engine**
(Agent 7; `rules/physical.rs:133-140`, `providers/scan_config.rs:33-45`)

- Audit `apply_post_filter_coalescing` in `rules/physical.rs` — functionally equivalent to the removed `CoalesceBatchesExec`; likely remove
- Update `standard_scan_config` in `scan_config.rs` against DF52's `FileSource`-based projection API
- Update `datafusion-substrait = "51.0.0"` and `datafusion-proto = "51.0.0"` pins

**Step 4: Python layer — `extensions/datafusion_ext.py`**
(Agent 6; `extensions/datafusion_ext.py:76-78,83-93`, `catalog/provider.py:76`)

- Update `_normalize_args` to inject `session` for `(py, session)` PyCapsule signatures when DF52 is detected
- Audit `catalog/provider.py:_table_from_dataset` for `__datafusion_table_provider__` compatibility
- Add `install_planner_rules` and `install_physical_rules` to `REQUIRED_RUNTIME_ENTRYPOINTS` in `required_entrypoints.py:5-20` immediately (this is a pre-DF52 quick win)

**Step 5: `deltalake` dependency alignment**
(Agents 3, 7, 8; `delta_control_plane.rs`, `Cargo.toml`)

- `deltalake = "0.30.1"` must upgrade to a DF52-compatible release before `delta_control_plane.rs` can compile against DF52
- Track `deltalake` DF52 support milestone; this is not currently tracked
- Once `deltalake` upgrades, audit `DeltaScanConfigBuilder` usage in `providers/scan_config.rs` and `delta_control_plane.rs`

---

### Breaking but Non-Blocking (Fix Incrementally After Compilation Passes)

| Change | Location | DF52 Reference | Notes |
|---|---|---|---|
| `DFSchema::field()` returns `&FieldRef` | `arrow/interop.py:50-69` | §J | Monitor Python binding update schedule |
| `FileScanConfigBuilder::with_projection_indices` returns `Result` | `schema_evolution.rs`, listing builder | §D | Mechanical `?` propagation |
| `newlines_in_values` moved to `CsvOptions` | `context.rs` (CSV handling) | §G | Small |
| `AggregateUDFImpl::supports_within_group_clause` tightened | `udaf_builtin.rs` | §I | Audit each custom UDAF |
| `AggregateUDFImpl::supports_null_handling_clause` defaults `false` | `udaf_builtin.rs` | §I | Three UDAFs already compliant |
| Parquet row-filter builder drops schema parameter | Any `build_row_filter` calls | §G | Search callsites |
| `CoalesceBatchesExec` removal in Python plan display parsing | `plan/plan_utils.py:plan_display` | §H | Lenient parser; low risk |

---

### New Capabilities to Adopt (Post-DF52 Consolidation)

| DF52 Feature | Bespoke Code Replaced | LOC Reduction | Location |
|---|---|---|---|
| `FileStatisticsCache` + `DefaultListFilesCache` | `cache/metadata_snapshots.py` SQL queries; `cache/inventory.py`, `cache/ledger.py` (partial) | ~200 LOC | Agents 3, 6; `metadata_snapshots.py:26-35`, `inventory.py`, `ledger.py` |
| `statistics_cache()` + `list_files_cache()` direct API | `snapshot_datafusion_caches` function replacing SQL try/except | ~80 LOC | Agent 1, 6; `runtime_extensions.py:737-770` |
| `RelationPlanner` FROM-clause hook | Add `RelationPlannerPort` in `expr/relation_planner.py`; extend `DfPluginManifestV1::capabilities` | Additive | Agents 5, 8; `udf/platform.py`, `df_plugin_api/src/manifest.rs` |
| `TableProvider DELETE/UPDATE hooks` | Python-side `delta_delete_request_payload` Rust entrypoint (may be redundant) | ~50 LOC (investigate) | Agent 3; `control_plane_mutation.py:41-56` |
| `ExprPlanner` / `RelationPlanner` as canonical extension points | `_install_expr_planners` ImportError-guarded probe + `expr_planner_hook` in `PolicyBundleConfig` | ~80 LOC | Agent 1; `runtime_extensions.py:935-972` |
| `df.cache()` for high-fan-out views | `"delta_staging"` cache policy in `pipeline_cache.py` (for small views) | Additive (new `CachePolicy` variant) | Agent 10; `ir_pipeline.py:_cache_policy_for_position` |
| DF52 sort pushdown to scans (§D.2) | Explicit Python sort in `interval_align_kernel` | ~50 LOC | Agent 6; `kernels.py` |
| Hash-join dynamic filtering (§B.1) | `pushdown_probe_extract.rs` (partial) | Investigate post-upgrade | Agent 4; `lineage/scheduling.py` |

**Estimated LOC reduction from adopting DF52 built-ins: ~460-560 Python LOC**

---

## 5. Planning-Object Consolidation Summary

These are bespoke Python implementations that replicate logic DataFusion already provides
natively. They create maintenance surface and drift risk.

| Bespoke Code | Location | LOC | DF Built-in Replacement | Estimated Reduction | Complexity | Agent |
|---|---|---|---|---|---|---|
| `_apply_setting()` dual method+key resolution | `context_pool.py:47-66,263-382` | ~120 | `SessionConfig.set(key, str(value))` — canonical DF path | ~120 LOC | Small | Agent 1 |
| `_datafusion_context()` bare session in `apply_encoding` | `encoding/policy.py:138-139` | ~30 | `pa.ChunkedArray.dictionary_encode().cast(dict_type)` — pure PyArrow | ~30 LOC | Small | Agent 2 |
| `cleanup_ephemeral_objects` via `SHOW TABLES` | `context_pool.py:225-239` | ~15 | Track names in pool `deque`; deregister by name at cleanup | ~15 LOC | Small | Agent 1 |
| Cache SQL queries via `SELECT * FROM metadata_cache()` | `cache/metadata_snapshots.py:84` | ~80 | DF52: `ctx.statistics_cache()` / `ctx.list_files_cache()` direct access | ~80 LOC | Medium (DF52 dep) | Agent 3 |
| Cache inventory/ledger file-statistics subsystem | `cache/inventory.py`, `cache/ledger.py` | ~900 | DF52 `FileStatisticsCache` (partial) | ~200 LOC (partial) | Medium (DF52 dep) | Agent 3 |
| Python plan walker (preorder traversal) | `plan/walk.py:74-109` | ~110 | DataFusion native `LogicalPlan.inputs()` recursion (via Rust bridge) | ~110 LOC | Large | Agent 4 |
| Filter string regex re-parsing | `lineage/scheduling.py:735-777` | ~50 | Structured filters at Rust extraction boundary | ~50 LOC | Medium | Agent 4 |
| `_apply_scan_settings` via `SET key = value` SQL | `registration_core.py` | ~60 | `ListingTableConfig` / `ListingOptions` builder API | ~60 LOC | Small | Agent 5 |
| Planner list duplication in `install_expr_planners_native` | `datafusion_ext/src/lib.rs:55-90` | ~10 | Consume `domain_expr_planners()` internally | ~10 LOC | Small | Agent 8 |
| `DeltaScanConfig` construction duplication | `delta_control_plane.rs:206-259` + `df_plugin_codeanatomy:376-396` | ~30 | Shared `DeltaScanConfigBuilder` in `datafusion_ext` | ~30 LOC | Small | Agent 8 |
| `SchemaEvolutionAdapterFactory` pass-through | `codeanatomy_ext/schema_evolution.rs:44-53` | ~15 | Use `DefaultPhysicalExprAdapterFactory` directly | ~15 LOC | Small | Agent 9 |
| `PyRunResult` per-accessor JSON re-parsing | `codeanatomy_engine_py/src/result.rs:32,49,70,81` | ~40 | Parse `inner_json` once at construction; store `serde_json::Value` | ~40 LOC | Small | Agent 9 |
| `BuildResult` bespoke JSON deserialization | `graph/product_build.py:473-491` | ~80 | `msgspec.json.decode(bytes, type=FinalizeDeltaReport)` | ~80 LOC | Small | Agent 12 |
| Coercion module duplication | `utils/coercion.py` + `cli/commands/build.py:862-902` | ~90 | `utils/value_coercion.py` as single canonical module | ~90 LOC | Small | Agent 12 |

**Total planning-object consolidation: ~940-1,040 Python LOC (plus Rust ~55 LOC)**

---

## 6. Principle Violation Heatmap

The following table aggregates the lowest-scoring principles across all 12 reviews. Only
scores of 1/3 (severe) and the frequency of 1/3 or 2/3 scores are shown.

| Principle | Avg Score (est.) | Worst Score | Count of 1/3 Ratings | Primary Offending Areas |
|---|---|---|---|---|
| P7 DRY | 1.5 | 0/3 (Agent 4: `plan_introspection.py`) | 6 | Type normalization, confidence constants, introspection duplication, coercion modules |
| P11 CQS | 1.5 | 1/3 (Agents 5, 10) | 5 | `register_*` functions, `get_or_register`, plan assembly timing |
| P19 KISS | 1.6 | 1/3 (Agents 3, 6, 11) | 5 | Python/Rust parallel walkers, extraction runtime loop duplication, coercion proliferation |
| P2 Separation of Concerns | 1.7 | 1/3 (Agents 2, 8) | 5 | `kernels.py`, `delta_runtime_ops.py`, `df_plugin_codeanatomy/lib.rs`, `session_context()` pipeline |
| P3 SRP | 1.7 | 1/3 (Agents 3, 6, 8) | 5 | `DataFusionRuntimeProfile` (8 responsibilities), `kernels.py` (3 reasons to change), plugin lib.rs |
| P16 Functional Core | 1.7 | 1/3 (Agent 1) | 4 | Session construction with global state, `delta_delete_where` retry mixing |
| P1 Information Hiding | 1.8 | 1/3 (Agents 5, 11) | 4 | Circular import triangle, builders.py private symbol imports, `registration_core.py` alias block |
| P21 Least Astonishment | 1.8 | 1/3 (Agent 11) | 3 | `inspect.signature` gating, `CODEANATOMY_DISABLE_DF_EXPLAIN` default, exception type inconsistency |
| P4 High Cohesion/Low Coupling | 1.8 | 1/3 (Agent 5) | 3 | Circular import triangle, `registration_core.py` 7-module fan-in |
| P23 Testability | 1.7 | 1/3 (Agents 8, 12) | 4 | Plugin lib.rs untestable pure functions, OTel global state, CLI SCIP assembly |
| P24 Observability | 1.9 | 1/3 (Agent 8) | 2 | Plugin `eprintln!` paths, missing tracing on policy/physical rules |
| P15 Tell Don't Ask | 2.0 | 1/3 (Agent 6) | 2 | `WritePipeline` null-check before interrogation, `FunctionCatalog` ask-then-decide |
| P14 Law of Demeter | 1.9 | 1/3 (Agent 6) | 2 | `WritePipeline` three-level chains, `kernels.py` two-level chains |

**Principles scoring 3/3 consistently (well-satisfied):**

- P5 Dependency Direction — Core structs have no upward dependencies; adapters depend on core
- P13 Composition over Inheritance — No problematic hierarchies; Protocols used correctly
- P18 Determinism — Plan fingerprinting, schema identity hashes, and version pinning all strong

---

## 7. Quick Wins Consolidated

All items here are small-effort changes extractable from existing reviews, ordered by
effort within each tier.

### Small Effort (< 1 day each)

| # | Description | File:Line | LOC Impact | Review |
|---|---|---|---|---|
| QW-01 | Remove `_apply_setting` `method` parameter; replace all 12 calls with `config.set(key, value)` | `context_pool.py:47-66,263-382` | −120 LOC | Agent 1 |
| QW-02 | Add double-installation guards (`WeakKeyDictionary` sentinels) for `_install_planner_rules` and `_install_cache_tables` | `runtime_extensions.py:200-240,813-836` | +15 LOC | Agent 1 |
| QW-03 | Replace `_datafusion_context()` + DataFusion temp-table path in `encoding/policy.py` with PyArrow dictionary cast | `encoding/policy.py:83-139` | −30 LOC | Agent 2 |
| QW-04 | Merge Arrow-alias rewrites (`largeutf8`, `utf8`, `non-null`) from `contracts.py:_normalize_type_string` into `type_normalization.py` | `schema/contracts.py:492-499`, `schema/type_normalization.py` | −20 LOC | Agent 2 |
| QW-05 | Add `None`-predicate guard in `delta_delete` and `delta_delete_where`; raise `ValueError` with clear message | `control_plane_mutation.py:41-56`, `delta_write.py:105-217` | +10 LOC | Agent 3 |
| QW-06 | Consolidate `_WriterPort` into `cache/_ports.py`; import in both `inventory.py` and `ledger.py` | `cache/inventory.py:91-94`, `cache/ledger.py:46-49` | −15 LOC | Agent 3 |
| QW-07 | Add `span.record_exception` to vacuum fallback (`delta_maintenance.py:103`) and cache SQL failure (`metadata_snapshots.py:109-113`) | Two files | +4 LOC | Agent 3 |
| QW-08 | Delete `plan_introspection.py`; migrate `function_registry_hash_for_context` and `suppress_introspection_errors` into `bundle_environment.py` | `plan/plan_introspection.py` | −220 LOC | Agent 4 |
| QW-09 | Rename `CODEANATOMY_DISABLE_DF_EXPLAIN` env var to `CODEANATOMY_ENABLE_DF_EXPLAIN` with `"0"` default | `plan/profiler.py:64` | 0 LOC | Agent 4 |
| QW-10 | Declare `PLAN_IDENTITY_PAYLOAD_VERSION = 4` as module constant in `plan_identity.py` | `plan/plan_identity.py:108` | +1 LOC | Agent 4 |
| QW-11 | Move `_ddl_type_name_from_arrow/string` from `extension_ddl.py` into `ddl_types.py` | `udf/extension_ddl.py:75-127`, `dataset/ddl_types.py` | −40 LOC | Agent 5 |
| QW-12 | Remove `**kwargs` from `udf_expr` or enforce keyword-to-position semantics | `udf/expr.py:136-145` | 0 LOC | Agent 5 |
| QW-13 | Remove dead `tier != "builtin"` branches from `UdfCatalog.list_functions_by_tier` and `resolve_by_tier` | `udf/metadata.py:721-737` | −25 LOC | Agent 5 |
| QW-14 | Extract `_EXPR_CALLS`, `_SQL_CALLS` dispatch dicts from `expr/spec.py` into `expr/dispatch.py` | `expr/spec.py:616-723` | 0 LOC (reorganization) | Agent 5 |
| QW-15 | Add `install_planner_rules` and `install_physical_rules` to `REQUIRED_RUNTIME_ENTRYPOINTS` | `extensions/required_entrypoints.py:5-20` | +2 LOC | Agent 6 |
| QW-16 | Unify SQL policy violation exception type: `SqlPolicyViolation` replacing mixed `ValueError`/`PermissionError` | `sql/guard.py:227-233` | 0 LOC | Agent 6 |
| QW-17 | Remove `DEFAULT_DF_POLICY` import from `catalog/introspection.py:470`; pass defaults as parameter | `catalog/introspection.py:470` | −1 LOC | Agent 6 |
| QW-18 | Replace `format!("{:?}", df.logical_plan())` with `display_indent()` in Rust `validate_plan` | `compiler/plan_compiler.rs:443,447,451` | 0 LOC | Agent 7 |
| QW-19 | Replace `ProviderCapabilities` bool flags with `Option<FilterPushdownStatus>` in `scan_config.rs` | `providers/scan_config.rs:119-155` | −20 Rust LOC | Agent 7 |
| QW-20 | Replace `eprintln!` in `df_plugin_codeanatomy/lib.rs:293,317` with `tracing::error!` | `df_plugin_codeanatomy/src/lib.rs:293,317` | 0 Rust LOC | Agent 8 |
| QW-21 | Remove `CoalesceBatches` import and rule from `datafusion_ext/src/physical_rules.rs` | `physical_rules.rs:6-7,103` | −15 Rust LOC | Agent 8 |
| QW-22 | Make `install_expr_planners_native` consume `domain_expr_planners()` to eliminate planner list duplication | `datafusion_ext/src/lib.rs:55-90` | −10 Rust LOC | Agent 8 |
| QW-23 | Cache `extract_session_ctx` result locally in `plugin_bridge.rs:register_df_plugin` (called 5 times) | `codeanatomy_ext/plugin_bridge.rs:291-333` | −10 Rust LOC | Agent 9 |
| QW-24 | Remove `SchemaEvolutionAdapterFactory`; use `DefaultPhysicalExprAdapterFactory` directly | `codeanatomy_ext/schema_evolution.rs:44-53` | −15 Rust LOC | Agent 9 |
| QW-25 | Replace 4 `serde_json::from_str` calls in `PyRunResult` with single parse at construction | `codeanatomy_engine_py/src/result.rs:32,49,70,81` | −30 Rust LOC | Agent 9 |
| QW-26 | Move confidence constants to `semantics/joins/strategies.py`; import in both consumers | `semantics/joins/inference.py:49-54`, `semantics/ir_pipeline.py:1086-1091` | −10 LOC | Agent 10 |
| QW-27 | Narrow exception catch in `infer_semantics()` to `KeyError`; add `logger.warning` | `semantics/ir_pipeline.py:1292-1301` | +5 LOC | Agent 10 |
| QW-28 | Remove `inspect.signature` checks at `orchestrator.py:229-240,341-348,377-386` | `extraction/orchestrator.py` | −30 LOC | Agent 11 |
| QW-29 | Replace inline `repo_files_schema` literal with `dataset_schema("repo_files_v1")` | `extraction/orchestrator.py:586-596` | −12 LOC | Agent 11 |
| QW-30 | Fix per-extractor timing: capture per-extractor `start_times` before submitting futures | `extraction/orchestrator.py:302-324` | +4 LOC | Agent 11 |
| QW-31 | Replace 4 private coercion helpers in `cli/commands/build.py:862-902` with `value_coercion` imports | `cli/commands/build.py:862-902` | −50 LOC | Agent 12 |
| QW-32 | Replace `_STATE: dict[str, ...]` dict-cell in `obs/otel/bootstrap.py` with direct nullable module variable | `obs/otel/bootstrap.py:263` | −3 LOC | Agent 12 |
| QW-33 | Add `join_strategy_hints()`, `cache_policy_hints()`, `confidence_hints()` to `SemanticIR`; update 3 loops in `policy_compiler.py` | `relspec/policy_compiler.py:451-483` | 0 LOC (reorganization) | Agent 12 |

### Medium Effort (1-3 days each)

| # | Description | File:Line | LOC Impact | Review |
|---|---|---|---|---|
| QW-34 | Route `session_context()` through `_ephemeral_context_phases` as the sole construction path | `runtime.py:403-437`, `runtime_context.py:64-105` | −80 LOC | Agent 1 |
| QW-35 | Extract `retry_with_policy(fn, *, policy, span)` from `delta_delete_where` and `execute_delta_merge` | `delta_write.py:165-191`, `delta_runtime_ops.py:278` | −60 LOC | Agent 3 |
| QW-36 | Merge `extension_core.py` + `extension_snapshot_runtime.py` into `extension_runtime.py` to resolve circular import triangle | `udf/extension_core.py:360-380`, `udf/extension_snapshot_runtime.py` | −200 LOC | Agent 5 |
| QW-37 | Remove alias block from `dataset/registration_core.py:616-647`; callers import from authoritative modules | `dataset/registration_core.py:616-647` | −31 aliases | Agent 5 |
| QW-38 | Split `df_plugin_codeanatomy/src/lib.rs` into `options.rs`, `udf_bundle.rs`, `providers.rs`, `lib.rs` | `df_plugin_codeanatomy/src/lib.rs` (694 LOC) | 0 Rust LOC (reorganization) | Agent 8 |
| QW-39 | Extract `TableRegistry` from `SemanticCompiler`; make compiler stateless | `semantics/compiler.py:223-225,592-625` | 0 LOC (reorganization) | Agent 10 |
| QW-40 | Merge `builders.py` + `builders_runtime.py` for all 4 extractors; expose public API through `__init__.py` | All four `builders_runtime.py` files | −400 LOC (ceremony removal) | Agent 11 |
| QW-41 | Replace pydantic `_RuntimeProfileEnvPatchRuntime` in `runtime_profile.py` with msgspec `Struct` + `convert` | `extraction/runtime_profile.py:13,95-114` | −20 LOC | Agent 11 |
| QW-42 | Consolidate `utils/coercion.py` into `utils/value_coercion.py`; delete or reduce to re-export shim | `utils/coercion.py`, `utils/value_coercion.py` | −40 LOC | Agent 12 |
| QW-43 | Extract `_build_scip_config` into `ScipIndexConfig.from_cli_overrides()` in `extract/extractors/scip/config.py` | `cli/commands/build.py:735-860` | 0 LOC (moved + testable) | Agent 12 |

---

## 8. Prioritized Implementation Roadmap

### Phase 0: Pre-DF52 Safety and Quick Wins

**Goal:** Eliminate correctness risks, remove dead code, and establish stable baselines
before any migration begins. These changes are independent of DF52 and should be batched
into 2-4 PRs.

**Key actions:**
- QW-05: Add `None`-predicate guard for Delta delete (correctness, high risk)
- QW-27: Narrow `infer_semantics()` exception catch (correctness, medium risk)
- QW-02: Add double-installation guards for session extensions (correctness)
- QW-01: Replace `_apply_setting` dual-path with `config.set()` (fragility reduction)
- QW-15: Add missing entrypoints to `REQUIRED_RUNTIME_ENTRYPOINTS` (silent failure prevention)
- QW-08: Delete `plan_introspection.py` (DRY, zero risk)
- QW-28/29/30: Clean up `orchestrator.py` migration residue (zero risk)
- QW-03: Replace hidden `SessionContext()` in `encoding/policy.py` with PyArrow cast
- QW-06/07: Consolidate `_WriterPort` and add `span.record_exception` (observability)
- QW-20/21/22: Rust plugin observability and CoalesceBatches pre-removal
- QW-26/31/32/42: Confidence constants, coercion consolidation, OTel dict-cell fix
- All remaining small quick wins (QW-04, QW-09 through QW-14, QW-16 through QW-19,
  QW-23 through QW-25, QW-33)

**Estimated total LOC impact:** −750 Python LOC, −80 Rust LOC
**Key risks:** None — all Phase 0 changes are local, non-breaking improvements
**Dependencies on other phases:** None

---

### Phase 1: DF52 Blocking Migration (Strict Dependency Order)

**Goal:** Achieve a clean compilation of the full Rust + Python stack against
DataFusion 52. This is a dedicated engineering sprint.

**Step 1.1 — `datafusion_python` FFI update (MUST COME FIRST):**
Update all `__datafusion_*_provider__` call sites in `context.rs`, `catalog.rs`,
`utils.rs` to pass `session` parameter. Update `FFI_TableProvider::new(...)` signature.
Update `Cargo.toml` to `datafusion = "52"`. Resolve `DFSchema` field-ref API changes in
`common/df_schema.rs` and `common/schema.rs`.

**Step 1.2 — `datafusion_ext` Rust library:**
Remove `CoalesceBatches` rule (QW-21 from Phase 0 gives a head start). Update
`FFI_TableProvider::new(...)` at `df_plugin_codeanatomy/src/lib.rs:431,462`.
Update `datafusion = "52"` in crate Cargo.toml.

**Step 1.3 — `codeanatomy_engine` crate:**
Audit and remove `apply_post_filter_coalescing` in `rules/physical.rs:133-140`.
Update `standard_scan_config` in `scan_config.rs` against DF52 `FileSource`-based
projection API. Update `datafusion-substrait` and `datafusion-proto` pins.

**Step 1.4 — Python extension layer:**
Update `_normalize_args` in `extensions/datafusion_ext.py:83-93` to inject `session`
for DF52 PyCapsule signatures when DF52 is detected. Audit
`catalog/provider.py:_table_from_dataset` for `__datafusion_table_provider__` changes.

**Step 1.5 — `deltalake` version alignment:**
Upgrade `deltalake` from `"0.30.1"` to a DF52-compatible release. Audit
`delta_control_plane.rs` and `providers/scan_config.rs` against DF52 `FileSource`-based
scan pushdown API.

**Estimated total LOC impact:** ~20 LOC net (mostly substitutions, not reductions)
**Key risks:** `deltalake` DF52 release timeline is external and uncontrolled. Phase 1
may stall at Step 1.5. Maintain a DF52-compatible build without delta features if
necessary to unblock Steps 1.1-1.4.
**Dependencies on Phase 0:** QW-21 (CoalesceBatches pre-removal) reduces Phase 1 risk.

---

### Phase 2: Post-DF52 Consolidation

**Goal:** Replace bespoke Python cache infrastructure and custom walkers with DF52
built-ins. This unlocks ~460-560 LOC of planned elimination.

**Key actions:**
- Migrate `cache/metadata_snapshots.py` SQL queries to `ctx.statistics_cache()` /
  `ctx.list_files_cache()` direct API (Agent 3 recommendation; ~80 LOC)
- Retire `cache/inventory.py` + `cache/ledger.py` file-statistics Delta tables for
  the file-metadata use case; retain plan-fingerprint cache hit logic (~200 LOC
  reduction from partial migration)
- Adopt `df.cache()` as a new `CachePolicy` variant (`"memory"`) for small high-fan-out
  views in the semantic pipeline (Agent 10)
- Evaluate `TableProvider DELETE/UPDATE hooks` for `delta_delete_request_payload`
  redundancy (Agent 3)
- Adopt DF52 sort pushdown for `interval_align_kernel` in `kernels.py` if the Rust
  migration in Phase 3 is deferred (Agent 6)
- Implement `RelationPlannerPort` protocol and `CodeAnatomyRelationPlanner` if
  FROM-clause extensions are needed (Agents 5, 8)

**Estimated total LOC impact:** −400 to −560 Python LOC
**Key risks:** DF52 Python binding surface for `statistics_cache()` / `list_files_cache()`
may not be exposed at upgrade time; check `datafusion-python` binding completeness.
**Dependencies on Phase 1:** Requires DF52 running successfully.

---

### Phase 3: Rust Pivot

**Goal:** Progressively migrate the highest-value Python surfaces to Rust, eliminating
fragile `getattr`-chain walkers, duplicated runtime orchestration, and Python-side UDF
validation. This is multi-quarter work.

**Sprint 3A — Lineage Walker Retirement (Tier 1, highest impact):**
Extend the Rust bridge payload to return `LineageReport` as structured JSON from
`compiler/lineage.rs`. Retire `plan/walk.py` (~110 LOC) and `lineage/reporting.py`
visitor logic (~570 LOC). Retain Python types (`ScanLineage`, `JoinLineage`,
`LineageReport`) as pure msgspec deserialization targets. (~680 LOC reduction)

**Sprint 3B — UDF Snapshot Contract Upgrade:**
Emit a typed, msgpack-encoded snapshot from Rust `datafusion_ext` instead of a generic
Python dict. This allows `udf/extension_validation.py` (~481 LOC),
`udf/extension_snapshot_runtime.py` (~495 LOC), and `udf/signature.py` (~205 LOC) to
be replaced by a single `msgspec.convert(raw_bytes, type=RustUdfSnapshot)` call.
(~800-900 LOC reduction)

**Sprint 3C — Extraction Runtime Loop:**
Extract a shared `ExtractionRuntime` loop into
`src/extract/coordination/extraction_runtime_loop.py`. Reduce each of the 4
`builders_runtime.py` files to ~100-150 LOC adapter. (~2,500 LOC reduction across the
four files)

**Sprint 3D — Tree-Sitter Rust Migration:**
Migrate tree-sitter extraction to Rust using the `tree-sitter-python` crate. Emit
Arrow `RecordBatch` directly from Rust, eliminating the Python accumulation layer.
(~1,100-1,300 Python LOC → ~300 Rust LOC bridge + entrypoint)

**Sprint 3E — Delta Write Integration:**
Connect Python `io/write_delta.py` write orchestration to the existing Rust
`executor/delta_writer.rs` via IPC payload. (~400+ Python LOC reduction; Rust
infrastructure exists)

**Estimated total LOC impact:** −5,500 to −6,000 Python LOC across all 3A-3E sprints
**Key risks:** Each sprint requires careful contract specification before implementation.
Sprint 3A depends on Phase 1 being complete (stable DF52 Rust bridge). Sprint 3B
requires coordinating the Rust snapshot contract change across all callers. Sprint 3C
requires identifying all parameter variations across the 4 extractors.
**Dependencies on Phases 1-2:** Sprints 3A-3E all require the DF52 migration to be
complete for the Rust bridge to be in a stable state.

---

### Phase 4: Design Principle Cleanup

**Goal:** Address the medium-effort structural improvements that don't fit neatly into
the migration sprints. These improve maintainability and testability.

**Key actions:**
- Route `session_context()` through `_ephemeral_context_phases` as the sole path; split
  `runtime_extensions.py` into `runtime_udf_install.py` + `runtime_observability.py`
  (Agent 1; medium effort, QW-34)
- Resolve circular import triangle in `udf/`: merge `extension_core.py` +
  `extension_snapshot_runtime.py` into `extension_runtime.py` (Agent 5; QW-36)
- Remove `registration_core.py` alias block; callers import directly from authoritative
  modules (Agent 5; QW-37)
- Split `df_plugin_codeanatomy/src/lib.rs` into 4 modules (Agent 8; QW-38)
- Extract `TableRegistry` from `SemanticCompiler`; make compiler stateless (Agent 10;
  QW-39)
- Make `run_extraction` stages injectable via `ExtractionStages` dataclass (Agent 11)
- Resolve `src/extract/` → `src/extraction/` dependency inversion by moving
  `EngineSession` and `RuntimeProfileSpec` to a shared location (Agent 11)
- Extract `run_zero_row_bootstrap_validation` from `DataFusionRuntimeProfile` into a
  free function (Agent 1)
- Add `DataFusionRuntimeProfile` query methods to eliminate Tell-Don't-Ask violations
  across `WritePipeline` and `kernels.py` (Agent 6; QW-33 variant)
- Add pydantic→msgspec `to_spec()` converters in `runtime_models/` to make the
  handoff explicit (Agent 12)
- Define `PlannerExtensionPort` protocol in `session/protocols.py`; eliminate attribute
  probing for planner rules (Agent 1)

**Estimated total LOC impact:** −300 to −500 LOC net (reorganization with some deletion)
**Key risks:** Some changes (particularly session construction restructuring) require
extensive testing. All of these are design improvements rather than correctness fixes,
so they carry lower risk but need careful coordination with active feature work.
**Dependencies on Phases 0-3:** Generally independent; can be interleaved with Phase 3
sprints.
