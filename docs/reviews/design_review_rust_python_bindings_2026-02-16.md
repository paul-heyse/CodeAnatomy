# Design Review: Rust Python Bindings and Plugin System

**Date:** 2026-02-16
**Scope:** `rust/datafusion_python/src/`, `rust/codeanatomy_engine_py/src/`, `rust/df_plugin_api/src/`, `rust/df_plugin_host/src/`, `rust/df_plugin_codeanatomy/src/`, `rust/datafusion_ext_py/src/`
**Focus:** Boundaries (1-6) and Composition (12-15) weighted heavily; Simplicity (19-22) weighted heavily; all 24 principles evaluated
**Depth:** Deep for `codeanatomy_ext.rs`, `context.rs`, `dataframe.rs`, `codeanatomy_engine_py/`; Moderate for `expr/` and plugin crates
**Files reviewed:** 25 files across 6 crates (~16,775 LOC in key files; ~6,400 LOC in expr/ directory sampled)

## Executive Summary

The Rust Python bindings layer exhibits a stark architectural split. The `codeanatomy_engine_py` crate and the plugin system (`df_plugin_api`, `df_plugin_host`, `df_plugin_codeanatomy`) demonstrate strong design discipline with clean separation of concerns, narrow interfaces, and well-defined boundaries. In contrast, `codeanatomy_ext.rs` at 3,582 LOC is a monolithic god-module that violates at least 10 of the 24 design principles, concentrating Delta control-plane operations, plugin management, UDF registration, cache table functions, session utilities, schema evolution, and runtime metrics into a single file. The most impactful improvements would decompose `codeanatomy_ext.rs` into focused submodules and extract duplicated knowledge (`parse_major`, `schema_from_ipc`, `DELTA_SCAN_CONFIG_VERSION`) into shared crates.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | high | `pub ctx: SessionContext` exposes internals; `pub col`, `pub expr`, `pub plan` fields on 8+ types |
| 2 | Separation of concerns | 0 | large | high | `codeanatomy_ext.rs` mixes 7+ distinct concerns in one 3,582 LOC file |
| 3 | SRP (one reason to change) | 0 | large | high | `codeanatomy_ext.rs` changes for Delta ops, plugin mgmt, UDF registration, cache tables, metrics, session utilities, schema evolution |
| 4 | High cohesion, low coupling | 1 | medium | medium | `codeanatomy_ext.rs` has low cohesion; plugin crates have high cohesion |
| 5 | Dependency direction | 2 | small | low | Generally correct: PyO3 wrappers depend on core Rust crates; minor inversion in `codeanatomy_ext.rs` reaching into plugin internals |
| 6 | Ports & Adapters | 2 | small | low | Plugin API is a clean port; `codeanatomy_engine_py` cleanly adapts Rust core; `codeanatomy_ext.rs` is an adapter but too coarse-grained |
| 7 | DRY (knowledge, not lines) | 0 | medium | high | `parse_major()` duplicated 3x, `schema_from_ipc()` 2x, `DELTA_SCAN_CONFIG_VERSION` 2x, manifest validation logic 2x |
| 8 | Design by contract | 2 | small | low | Good validation at PyO3 boundary (`#[new]` constructors validate); `session_context_contract()` enforces invariants; some functions lack precondition checks |
| 9 | Parse, don't validate | 2 | small | low | `session_context_contract()` parses once; `parse_and_validate_spec()` in compiler.rs uses serde_path_to_error for structural parsing |
| 10 | Make illegal states unrepresentable | 2 | small | low | `CacheTableKind` enum is well-typed; `EnvironmentClass` enum prevents invalid classes; some stringly-typed interfaces remain |
| 11 | CQS | 1 | medium | medium | `register_dataset_provider` both registers and returns payload dict; `delta_write_ipc` both writes and returns report; 10+ delta functions mix commands with query-style returns |
| 12 | DI + explicit composition | 2 | small | low | Plugin system uses DI via `DfPluginMod_Ref`; `codeanatomy_engine_py` composes via `SessionFactory` injection; some hidden creation in `runtime()` |
| 13 | Prefer composition over inheritance | 3 | - | - | No inheritance hierarchies; `CpgTableProvider` wraps `TableProvider` via composition; `SchemaEvolutionAdapterFactory` delegates to `DefaultPhysicalExprAdapterFactory` |
| 14 | Law of Demeter | 1 | medium | medium | `session_context_contract(ctx)?.ctx` chains through contract to internals 63 times; `self.inner.profile().target_partitions` chains 3 deep |
| 15 | Tell, don't ask | 1 | medium | medium | Functions extract `ctx` then operate on it procedurally; `CpgTableProvider` properly encapsulates but most functions do ask-then-operate |
| 16 | Functional core, imperative shell | 2 | small | low | `codeanatomy_engine_py/compiler.rs` separates pure spec construction from IO; `codeanatomy_ext.rs` functions are thin imperative shells delegating to native functions |
| 17 | Idempotency | 2 | small | low | Delta operations delegate to idempotent native implementations; `register_cache_tables` could double-register |
| 18 | Determinism / reproducibility | 1 | small | medium | `now_unix_ms()` uses `SystemTime::now()` introducing nondeterminism into cache metrics and execution snapshots |
| 19 | KISS | 0 | large | high | `codeanatomy_ext.rs` at 3,582 LOC with 80+ functions; `delta_merge` has 25+ parameters; `delta_set_properties` has 16 parameters |
| 20 | YAGNI | 2 | small | low | `init_module` and `init_internal_module` register overlapping but slightly different function sets without clear rationale for divergence |
| 21 | Least astonishment | 1 | medium | medium | `parse_python_payload()` roundtrips through Python `json.dumps` then Rust `serde_json::from_str`; `runtime()` creates a new Tokio runtime per call; `register_table_provider` is a deprecated alias that still exists |
| 22 | Declare and version public contracts | 2 | small | low | `DELTA_SCAN_CONFIG_VERSION`, `DF_PLUGIN_ABI_MAJOR/MINOR` version contracts exist; plugin manifest validates versions; PyO3 module registration is the de facto contract |
| 23 | Design for testability | 1 | medium | medium | `codeanatomy_ext.rs` functions are untestable in isolation due to PyO3 + Tokio + SessionContext entanglement; `df_plugin_codeanatomy` has good integration tests |
| 24 | Observability | 1 | small | low | `runtime_execution_metrics_snapshot` provides structured telemetry; `install_tracing` exists; most functions lack structured error context beyond format strings |

## Detailed Findings

### Category: System Decomposition and Boundaries

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
Multiple PyO3 wrapper types expose their inner Rust fields as `pub`, allowing any code within the crate to bypass the wrapper's intended interface and access implementation details directly.

**Findings:**
- `rust/datafusion_python/src/context.rs:299` -- `pub ctx: SessionContext` on `PySessionContext` exposes the entire DataFusion `SessionContext` as a public field. This enables `codeanatomy_ext.rs` to reach directly into `session_context_contract(ctx)?.ctx` (63 occurrences) bypassing any abstraction.
- `rust/datafusion_python/src/expr.rs:120` -- `pub expr: Expr` exposes the inner expression type.
- `rust/datafusion_python/src/physical_plan.rs:33` -- `pub plan: Arc<dyn ExecutionPlan>` exposes the execution plan.
- `rust/datafusion_python/src/expr/column.rs:24` -- `pub col: Column` exposes the column internals.
- `rust/datafusion_python/src/store.rs:46,72,145,174` -- `pub inner: Arc<...>` on all four store types (LocalFileSystem, MicrosoftAzure, GoogleCloudStorage, AmazonS3).
- Contrast: `rust/codeanatomy_engine_py/src/session.rs:13` -- `inner: codeanatomy_engine::session::factory::SessionFactory` is properly private with `pub(crate)` accessor methods `inner()` and `get_profile()`.

**Suggested improvement:**
Change `pub ctx` on `PySessionContext` to `pub(crate) ctx` and provide intentional accessor methods for the operations that `codeanatomy_ext.rs` actually needs. This narrows the blast radius of `SessionContext` API changes. Apply the same pattern to `PyExpr`, `PyColumn`, and store types.

**Effort:** medium
**Risk if unaddressed:** high -- Any internal DataFusion API change propagates through all 63 `session_context_contract(ctx)?.ctx` call sites plus all other direct field accesses.

---

#### P2. Separation of concerns -- Alignment: 0/3

**Current state:**
`codeanatomy_ext.rs` is a 3,582 LOC file that intermixes at least 7 distinct responsibilities. It is the most severe architectural concern in the reviewed scope.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext.rs:1-3582` -- The file contains:
  1. **Delta provider construction** (lines 126-240): `delta_table_provider_with_files`, `delta_cdf_table_provider`, `delta_scan_config_from_session`, `DeltaCdfOptions`, `DeltaRuntimeEnvOptions`, `DeltaSessionRuntimePolicyOptions`
  2. **Session utilities** (lines 241-502): `runtime()`, `session_context_contract()`, `build_extraction_session`, `capabilities_snapshot`, `install_codeanatomy_runtime`, `parse_python_payload`, `json_to_py`
  3. **Plugin management** (lines 503-770): `load_df_plugin`, `register_df_plugin`, `register_df_plugin_udfs`, `register_df_plugin_table_functions`, `create_df_plugin_table_provider`, `register_df_plugin_table_providers`, `ensure_plugin_manifest_compat`, `parse_major`
  4. **UDF registration** (lines 771-1300): `register_codeanatomy_udfs`, `install_function_factory`, `install_codeanatomy_udf_config`, `install_codeanatomy_policy_config`, `install_codeanatomy_physical_config`, `install_planner_rules`, `install_physical_rules`, `udf_expr`, `registry_snapshot_py`, `udf_docs_snapshot`
  5. **Schema evolution** (lines 1490-1570): `SchemaEvolutionAdapterFactory`, `CpgTableProvider`, `install_schema_evolution_adapter_factory`, `schema_evolution_adapter_factory`
  6. **Cache tables** (lines 2034-2455): `CacheTableFunction`, `CacheTableKind`, `register_cache_tables`, `runtime_execution_metrics_snapshot`
  7. **Delta mutation/maintenance** (lines 1700-3440): `delta_write_ipc`, `delta_delete`, `delta_update`, `delta_merge`, `delta_optimize_compact`, `delta_vacuum`, `delta_restore`, `delta_set_properties`, `delta_add_features`, `delta_add_constraints`, `delta_drop_constraints`, `delta_create_checkpoint`, `delta_cleanup_metadata`, `delta_data_checker`

- `rust/datafusion_python/src/codeanatomy_ext.rs:3442-3582` -- `init_module()` and `init_internal_module()` each register 50+ functions with overlapping but slightly different sets, making the public API surface hard to reason about.

**Suggested improvement:**
Decompose `codeanatomy_ext.rs` into at least 5 focused submodules:
- `codeanatomy_ext/delta_provider.rs` -- Provider construction and scan config
- `codeanatomy_ext/delta_mutations.rs` -- Write, merge, delete, update operations
- `codeanatomy_ext/delta_maintenance.rs` -- Vacuum, optimize, restore, checkpoint, properties, constraints
- `codeanatomy_ext/plugin_bridge.rs` -- Plugin loading, registration, manifest validation
- `codeanatomy_ext/udf_registration.rs` -- UDF/function factory installation, config, snapshots
- `codeanatomy_ext/cache_tables.rs` -- Cache table functions and metrics
- `codeanatomy_ext/session_utils.rs` -- Runtime, session contract, extraction session, helpers
- `codeanatomy_ext/mod.rs` -- Re-exports and `init_module`/`init_internal_module`

**Effort:** large
**Risk if unaddressed:** high -- The file is a change magnet. Any Delta, plugin, UDF, or cache table change requires navigating 3,582 lines. Merge conflicts are likely when multiple features are developed concurrently.

---

#### P3. SRP (one reason to change) -- Alignment: 0/3

**Current state:**
`codeanatomy_ext.rs` changes for at least 7 independent reasons (see P2 findings). Each delta maintenance operation, UDF registration change, plugin system update, or cache table modification touches this single file.

**Findings:**
- The 7 concerns listed in P2 represent 7 independent change vectors.
- `rust/datafusion_python/src/codeanatomy_ext.rs:3442-3582` -- `init_module()` registers 65 functions and 3 classes. Adding any new function requires editing this registration block.
- Contrast: `rust/codeanatomy_engine_py/src/` -- Each file has a single clear responsibility: `session.rs` (session factory), `compiler.rs` (plan compilation), `materializer.rs` (execution), `result.rs` (result serialization), `errors.rs` (error types).

**Suggested improvement:**
Same decomposition as P2. Each submodule should have its own `init_submodule()` that the top-level `init_module()` delegates to.

**Effort:** large (same work as P2)
**Risk if unaddressed:** high

---

#### P4. High cohesion, low coupling -- Alignment: 1/3

**Current state:**
The plugin crate trio (`df_plugin_api`, `df_plugin_host`, `df_plugin_codeanatomy`) demonstrates excellent cohesion and minimal coupling. `codeanatomy_engine_py` has high cohesion. `codeanatomy_ext.rs` has very low cohesion -- unrelated concerns are packed together simply because they all need to be registered as pyfunctions.

**Findings:**
- `rust/df_plugin_api/src/lib.rs` (65 LOC) -- Cleanly defines only the ABI-stable plugin interface. No extraneous concerns.
- `rust/df_plugin_host/src/loader.rs` (142 LOC) -- Only plugin loading and manifest validation.
- `rust/df_plugin_host/src/registry_bridge.rs` (158 LOC) -- Only registration operations on a loaded plugin.
- `rust/datafusion_python/src/codeanatomy_ext.rs` -- Helper functions like `json_to_py()` (line 328), `storage_options_map()` (line 130), `now_unix_ms()` (line 468) are used by multiple unrelated concerns within the file, creating internal coupling that would need to be resolved during decomposition.

**Suggested improvement:**
Extract shared helpers (`json_to_py`, `storage_options_map`, `now_unix_ms`, `parse_python_payload`) into a `codeanatomy_ext/helpers.rs` module. Group related functions into cohesive submodules as described in P2.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The dependency direction is generally correct. PyO3 binding crates depend on core Rust crates, not the reverse. The plugin system enforces a clear dependency direction: `df_plugin_api` (interface) <- `df_plugin_host` (host) and `df_plugin_api` <- `df_plugin_codeanatomy` (plugin implementation). `codeanatomy_engine_py` depends on `codeanatomy_engine` (core Rust logic).

**Findings:**
- `rust/codeanatomy_engine_py/src/session.rs:5` -- Depends on `codeanatomy_engine::session::profiles` and `codeanatomy_engine::session::factory` (correct inward direction).
- `rust/codeanatomy_engine_py/src/compiler.rs` -- Depends on `codeanatomy_engine::planning` (correct direction).
- Minor concern: `rust/datafusion_python/src/codeanatomy_ext.rs` imports from `df_plugin_host` directly, meaning the PyO3 binding layer knows about plugin host internals. A port/trait could mediate this.

**Suggested improvement:**
No immediate action needed. The minor plugin host coupling would naturally resolve when `codeanatomy_ext.rs` is decomposed and plugin management moves to its own submodule.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The plugin API (`df_plugin_api`) is a well-defined port with ABI stability. `codeanatomy_engine_py` is a clean adapter between Rust core and Python. `codeanatomy_ext.rs` serves as an adapter but its monolithic structure undermines the benefit.

**Findings:**
- `rust/df_plugin_api/src/lib.rs:10-65` -- Clean port definition: `DfPluginExportsV1` with 4 function pointers, `DfUdfBundleV1`, `DfTableFunctionV1`. Uses `abi_stable` types for ABI safety.
- `rust/df_plugin_api/src/manifest.rs:1-29` -- `DfPluginManifestV1` with explicit version fields and capabilities bitmask.
- `rust/codeanatomy_engine_py/src/lib.rs:1-202` -- Clean adapter that exposes `SessionFactory`, `SemanticPlanCompiler`, `CpgMaterializer`, `RunResult` as Python-visible types.
- `rust/datafusion_ext_py/src/lib.rs:1-38` -- Thin adapter that mirrors `datafusion._internal` surface for ABI compatibility.

**Suggested improvement:**
When decomposing `codeanatomy_ext.rs`, organize the submodules so that each one serves as an adapter for a specific domain concern (Delta operations, plugin management, UDF registration, etc.).

**Effort:** small (part of P2 decomposition)
**Risk if unaddressed:** low

---

### Category: Shared Types, Schemas, and Knowledge

#### P7. DRY (knowledge, not lines) -- Alignment: 0/3

**Current state:**
Several pieces of domain knowledge are duplicated across crates without a shared authoritative source. This is the second most critical finding after the P2/P3 monolith.

**Findings:**
- **`parse_major()` duplicated 3 times:**
  - `rust/datafusion_python/src/codeanatomy_ext.rs:503` -- Returns `PyResult<u16>`
  - `rust/df_plugin_host/src/loader.rs:26` -- Returns `Result<u16>`
  - `rust/df_plugin_codeanatomy/src/lib.rs:115` -- Returns `Result<u16, String>`
  - All three implementations contain the same logic: split on `.`, parse first element as `u16`. The only difference is the error type.

- **`schema_from_ipc()` duplicated 2 times:**
  - `rust/datafusion_python/src/codeanatomy_ext.rs:144` -- Takes `Vec<u8>`, returns `PyResult<SchemaRef>`
  - `rust/df_plugin_codeanatomy/src/lib.rs:359` -- Takes `&[u8]`, returns `Result<SchemaRef, DeltaTableError>`
  - Both deserialize Arrow IPC schema from bytes using the same algorithm.

- **`DELTA_SCAN_CONFIG_VERSION` duplicated 2 times:**
  - `rust/datafusion_python/src/codeanatomy_ext.rs:126` -- `const DELTA_SCAN_CONFIG_VERSION: u32 = 1;`
  - `rust/df_plugin_codeanatomy/src/lib.rs:44` -- `const DELTA_SCAN_CONFIG_VERSION: u32 = 1;`
  - These must stay in sync or scan config validation will fail silently.

- **Manifest validation logic duplicated:**
  - `rust/datafusion_python/src/codeanatomy_ext.rs:510-560` -- `ensure_plugin_manifest_compat()` validates ABI version, DataFusion major, Arrow major
  - `rust/df_plugin_host/src/loader.rs:56-96` -- `validate_manifest()` performs nearly identical validation
  - Both call `parse_major()` on the same version strings with the same comparison logic.

**Suggested improvement:**
Create a shared crate (e.g., `df_plugin_common` or extend `df_plugin_api`) that owns:
1. `parse_major()` with a generic error type or `Result<u16, String>` that callers adapt
2. `schema_from_ipc()` with a generic error type
3. `DELTA_SCAN_CONFIG_VERSION` as the single authoritative constant
4. Manifest validation logic that both `codeanatomy_ext.rs` and `df_plugin_host` call

**Effort:** medium
**Risk if unaddressed:** high -- If `DELTA_SCAN_CONFIG_VERSION` is bumped in one location but not the other, scan config validation will reject valid payloads or accept invalid ones without any compiler warning.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The PyO3 boundary provides natural contract enforcement through type conversion. Several explicit contract checks exist.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext.rs:252-270` -- `session_context_contract()` validates that a Python object has the expected DataFusion session context structure before proceeding. This is a good boundary contract.
- `rust/codeanatomy_engine_py/src/compiler.rs:88-110` -- `parse_and_validate_spec()` uses `serde_path_to_error` to provide field-path diagnostics on deserialization failure.
- `rust/df_plugin_host/src/loader.rs:56-96` -- `validate_manifest()` checks ABI major/minor, DataFusion major, Arrow major versions with explicit error messages.
- Minor gap: Many delta functions in `codeanatomy_ext.rs` accept `Option<i64>` for version/timestamp without validating mutual exclusivity (both can be `Some` simultaneously).

**Suggested improvement:**
Add a helper that validates version/timestamp mutual exclusivity for delta operations, applied once rather than checking in each function.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Several boundary parsing patterns exist and work well. The PyO3 type system handles most parsing automatically.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext.rs:252-270` -- `session_context_contract()` parses a `PyAny` into a validated `SessionContext` reference once. All 63 callers get a validated context.
- `rust/codeanatomy_engine_py/src/compiler.rs:88-110` -- `parse_and_validate_spec()` parses JSON into a `BuildSpecRequest` struct, converting messy string input into a structured representation.
- `rust/codeanatomy_engine_py/src/session.rs:34-40` -- `#[new]` constructor parses JSON string into `EnvironmentProfile` at construction time.
- Gap: `rust/datafusion_python/src/codeanatomy_ext.rs:250` -- `parse_python_payload()` roundtrips through Python `json.dumps` then Rust `serde_json::from_str`, which is correct but inefficient. The approach is valid (parse at boundary) but the mechanism is roundabout.

**Suggested improvement:**
No immediate action. The roundtrip through Python JSON is a pragmatic choice given the PyO3 boundary constraints.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Several good examples of type-safe state modeling. Some stringly-typed interfaces remain.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext.rs:2034-2100` -- `CacheTableKind` enum with 4 variants (`MetadataCache`, `ParquetMetadataPool`, `MemoryPool`, `CustomPool`) prevents invalid cache table kinds by construction.
- `rust/codeanatomy_engine_py/src/session.rs:118-127` -- `EnvironmentClass` enum (Small/Medium/Large) prevents invalid environment classes.
- `rust/df_plugin_api/src/manifest.rs:10-12` -- Capability bitmask (`CAPABILITY_UDF`, `CAPABILITY_TABLE_FUNCTION`, `CAPABILITY_TABLE_PROVIDER`) encodes capabilities as typed flags.
- Gap: Delta maintenance functions accept string-typed `timestamp` parameters (`Option<String>`) rather than a parsed timestamp type. Invalid timestamp formats are only caught at runtime in the native Rust functions.
- Gap: `how` parameter in `dataframe.rs:658-670` join type is a `&str` matched at runtime rather than a Python enum.

**Suggested improvement:**
Define a `PyDeltaTimestamp` newtype that parses and validates timestamp format at the PyO3 boundary. Consider exposing `JoinType` as a Python enum (it already exists as `PyJoinType` but the `join` method accepts a string).

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS (Command-Query Separation) -- Alignment: 1/3

**Current state:**
Multiple functions in `codeanatomy_ext.rs` both perform mutations and return data payloads, making it ambiguous whether calling them is safe.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext.rs` -- `register_dataset_provider()` (approx line 290) both registers a table provider into the session context (command) AND returns a JSON payload dict with registration metadata (query).
- `rust/datafusion_python/src/codeanatomy_ext.rs` -- All delta mutation functions (`delta_write_ipc`, `delta_delete`, `delta_update`, `delta_merge`, `delta_optimize_compact`, `delta_vacuum`, `delta_restore`, `delta_set_properties`, `delta_add_features`, `delta_add_constraints`, `delta_drop_constraints`, `delta_create_checkpoint`, `delta_cleanup_metadata`) return `PyResult<Py<PyAny>>` containing a maintenance report dict. These are mutations that also return query results.
- `rust/datafusion_python/src/codeanatomy_ext.rs` -- `install_codeanatomy_runtime()` mutates the session state and returns a capabilities snapshot.

**Suggested improvement:**
For delta operations, the CQS violation is pragmatic -- callers need the report from mutations. This is an acceptable exception to CQS when the report is about the mutation itself (similar to `INSERT...RETURNING`). Document this as an intentional design choice. For `register_dataset_provider`, consider separating registration (command) from metadata query.

**Effort:** medium
**Risk if unaddressed:** medium

---

### Category: Dependencies, Composition, and Construction

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
The plugin system uses dependency inversion well. `codeanatomy_engine_py` composes via constructor injection.

**Findings:**
- `rust/df_plugin_api/src/lib.rs` -- Plugin interface (`DfPluginMod`) is a pure abstraction that both host and plugins depend on. The host never depends on any concrete plugin.
- `rust/codeanatomy_engine_py/src/session.rs:34-40` -- `SessionFactory` accepts a profile at construction, enabling different configurations.
- `rust/datafusion_python/src/codeanatomy_ext.rs:245-248` -- `runtime()` creates a new `tokio::runtime::Runtime` on every call. This is hidden construction rather than injection. While pragmatic for PyO3 (no async runtime available), it means each delta operation spins up and tears down a Tokio runtime.

**Suggested improvement:**
Consider using a thread-local or lazy-static Tokio runtime instead of creating one per call. This would improve performance and make the runtime lifecycle explicit.

**Effort:** small
**Risk if unaddressed:** low (performance concern, not correctness)

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies exist. All behavior extension uses composition.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext.rs:1510` -- `CpgTableProvider` wraps `Arc<dyn TableProvider>` via composition, adding metadata tracking without inheriting.
- `rust/datafusion_python/src/codeanatomy_ext.rs:1497` -- `SchemaEvolutionAdapterFactory` wraps `DefaultPhysicalExprAdapterFactory` via delegation.
- `rust/codeanatomy_engine_py/src/session.rs:13` -- `SessionFactory` wraps `codeanatomy_engine::session::factory::SessionFactory` by composition.
- The `expr/` directory uses `From` trait implementations rather than inheritance to convert between Rust and Python types.

**Suggested improvement:**
No action needed. This principle is well-satisfied.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
The most frequent violation is the `session_context_contract(ctx)?.ctx` pattern that appears 63 times, reaching through a contract wrapper to access the inner `SessionContext`.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext.rs` -- `session_context_contract(ctx)?.ctx` (63 occurrences). Every delta function, session utility, and UDF registration function chains through the contract to the inner context. Example at line 3103: `session_ctx: &session_context_contract(ctx)?.ctx`.
- `rust/codeanatomy_engine_py/src/session.rs:78-79` -- `self.inner.profile().target_partitions` chains 3 levels deep through the factory to the profile to a field.
- `rust/datafusion_python/src/dataframe.rs:703-710` -- `df.logical_plan().schema().qualified_field(idx)` chains 3 levels from DataFrame to LogicalPlan to Schema to field lookup.
- Contrast: `rust/df_plugin_host/src/registry_bridge.rs` -- Methods on `PluginHandle` operate on direct collaborators (`self.module`, `self.manifest`) without deep chaining.

**Suggested improvement:**
Replace `session_context_contract(ctx)?.ctx` with a helper that returns the `SessionContext` directly (e.g., `fn extract_session_context(ctx: &Bound<'_, PyAny>) -> PyResult<&SessionContext>`). This reduces the chain by one level and makes intent clearer. Alternatively, provide domain-specific methods on the contract type itself.

**Effort:** medium
**Risk if unaddressed:** medium -- Each change to `session_context_contract`'s return type or `PySessionContext`'s `ctx` field requires updating all 63 call sites.

---

#### P15. Tell, don't ask -- Alignment: 1/3

**Current state:**
Most functions in `codeanatomy_ext.rs` follow an "ask" pattern: extract the session context, then procedurally call operations on it. The objects themselves do not encapsulate behavior.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext.rs` -- Pattern repeated across all delta functions: extract `session_context_contract(ctx)?.ctx`, extract storage options, extract gate parameters, extract commit options, then pass everything to a native function. The PyO3 functions are "ask" wrappers that reach into Python objects to extract data.
- `rust/datafusion_python/src/codeanatomy_ext.rs:2700-2860` -- `delta_gate_from_params()` and `commit_options_from_params()` are builder helpers that mitigate the "ask" problem by encapsulating construction logic, but callers still extract parameters individually.
- Good example: `rust/datafusion_python/src/codeanatomy_ext.rs:1510-1600` -- `CpgTableProvider` properly encapsulates table provider behavior. Callers tell it to `scan()` rather than extracting its internals.

**Suggested improvement:**
For the delta operations, consider a `DeltaOperationContext` struct that bundles session context, storage options, and gate parameters. Functions would receive this struct rather than extracting each piece individually. This aligns with the existing `DeltaRestoreRequest`, `DeltaSetPropertiesRequest` pattern in the native functions.

**Effort:** medium
**Risk if unaddressed:** medium

---

### Category: Effects, Determinism, and Pipeline Correctness

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The general architecture correctly places pure logic in Rust core crates and imperative PyO3 shell in the binding crates. Most `codeanatomy_ext.rs` functions are thin imperative shells that delegate to deterministic native functions.

**Findings:**
- `rust/codeanatomy_engine_py/src/compiler.rs:113-280` -- `build_spec_json()` contains significant domain logic (~170 lines) for constructing execution specs. Some of this logic (like `canonical_rulepack_profile()`, `map_join_type()`, `cpg_output_kind_for_view()`) is pure and testable, but it's embedded in the PyO3 method body rather than extracted into the core crate.
- `rust/codeanatomy_engine_py/src/materializer.rs:50-350` -- `execute()` is appropriately an imperative shell that orchestrates session creation, input registration, compilation, and execution.
- `rust/datafusion_python/src/codeanatomy_ext.rs` -- Delta functions correctly delegate to `delta_*_native()` functions in the core, keeping the PyO3 layer as a thin shell.

**Suggested improvement:**
Move the pure domain logic in `compiler.rs:build_spec_json()` (specifically `canonical_rulepack_profile()`, `map_join_type()`, `cpg_output_kind_for_view()`, `default_rule_intents()`, `build_transform()`) into the `codeanatomy_engine` core crate where they can be unit tested without PyO3.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
Delta operations delegate to native implementations that handle idempotency. Most registration operations are safe to call multiple times.

**Findings:**
- Delta write/merge/delete/update operations are inherently non-idempotent (they create new commits), which is expected and correct.
- `rust/datafusion_python/src/context.rs:591-596` -- `register_table()` overwrites existing registrations, which is idempotent.
- `rust/datafusion_python/src/codeanatomy_ext.rs` -- `register_cache_tables()` could potentially double-register if called twice, though DataFusion's `register_table` handles this gracefully.

**Suggested improvement:**
No immediate action needed. The non-idempotent nature of delta mutations is inherent to the domain.

**Effort:** small
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 1/3

**Current state:**
A nondeterministic function (`now_unix_ms()`) is embedded in runtime metrics and cache table metadata, breaking determinism for snapshot comparisons.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext.rs:468-473` -- `now_unix_ms()` uses `SystemTime::now()` to generate timestamps. This is called:
  - Line 2117: In `CacheTableFunction::to_record_batch()` for `event_time` field
  - Line 2412: In `runtime_execution_metrics_snapshot()` for `event_time_unix_ms` field
- These timestamps make snapshot comparison and deterministic testing impossible for any consumer of cache table or metrics data.
- Contrast: `rust/codeanatomy_engine_py/src/materializer.rs` -- Does not introduce nondeterminism; delegates to deterministic core logic.

**Suggested improvement:**
Accept a `clock` parameter (or use a configurable clock trait) so that tests can inject a fixed timestamp. Alternatively, document that `event_time` fields are explicitly nondeterministic and should be excluded from snapshot comparisons.

**Effort:** small
**Risk if unaddressed:** medium -- Breaks the project's determinism contract ("All plans must be reproducible") for metrics snapshots.

---

### Category: Simplicity, Evolution, and Predictability

#### P19. KISS -- Alignment: 0/3

**Current state:**
Several structural complexity issues make the code harder to understand and maintain than necessary.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext.rs` -- At 3,582 LOC with 80+ `#[pyfunction]` definitions, this file requires significant scrolling and mental overhead to navigate. No internal structure or module organization exists.
- `rust/datafusion_python/src/codeanatomy_ext.rs` -- Delta maintenance functions have massive parameter lists:
  - `delta_merge` (approx line 2580-2700): 25+ parameters
  - `delta_set_properties` (lines 3117-3136): 16 parameters
  - `delta_add_features` (lines 3174-3194): 17 parameters
  - `delta_add_constraints` (lines 3233-3252): 17 parameters
  - `delta_drop_constraints` (lines 3289-3309): 17 parameters
  - `delta_vacuum` (approx line 2900): 19+ parameters
- `rust/datafusion_python/src/codeanatomy_ext.rs:3442-3582` -- Two separate registration functions (`init_module` and `init_internal_module`) register overlapping but non-identical sets of functions. The difference between the two sets is not documented.
- Contrast: `rust/codeanatomy_engine_py/src/` -- 5 files totaling ~1,600 LOC, each under 700 LOC, with clear single responsibilities.

**Suggested improvement:**
1. Decompose `codeanatomy_ext.rs` as described in P2.
2. For delta functions with 15+ parameters, introduce request structs (mirroring the existing native `DeltaRestoreRequest`, `DeltaSetPropertiesRequest` pattern). The PyO3 `#[pyfunction]` can still accept keyword arguments, but internally group related parameters into structs: `DeltaGateParams` (min_reader_version, min_writer_version, required_reader_features, required_writer_features), `DeltaCommitParams` (commit_metadata, app_id, app_version, app_last_updated, max_retries, create_checkpoint), `DeltaTableLocator` (table_uri, storage_options, version, timestamp).
3. Document or consolidate the difference between `init_module` and `init_internal_module`.

**Effort:** large
**Risk if unaddressed:** high -- New contributors face a significant onboarding barrier. The parameter proliferation creates a high probability of parameter-ordering errors.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
The codebase is generally lean. No significant speculative generality detected.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext.rs:3442-3582` -- `init_module()` and `init_internal_module()` register overlapping but slightly different function sets. `init_internal_module` includes `arrow_stream_to_batches` which `init_module` does not, but excludes some plugin registration functions. The rationale for two separate registration paths is not documented and may represent unnecessary complexity.
- `rust/datafusion_python/src/context.rs:632-639` -- `register_table_provider` exists as a deprecated alias for `register_table`. It should be removed or have a deprecation timeline.

**Suggested improvement:**
Document the purpose of `init_module` vs `init_internal_module` difference. If the distinction is unnecessary, consolidate to a single registration function. Remove deprecated `register_table_provider` alias.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
Several design choices would surprise a competent reader.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext.rs:245-248` -- `runtime()` creates a new `tokio::runtime::Runtime` on every call. A reader would expect a shared or cached runtime, especially given that this function is called by every delta operation. Creating and destroying runtimes has nontrivial overhead.
- `rust/datafusion_python/src/codeanatomy_ext.rs:250` -- `parse_python_payload()` serializes a Python dict to JSON via Python's `json.dumps`, then deserializes it in Rust via `serde_json::from_str`. This double serialization is surprising when direct PyO3 extraction would be more natural.
- `rust/datafusion_python/src/context.rs:632-639` -- `register_table_provider` is documented as "Construct datafusion dataframe from Arrow Table" but actually registers a table provider. The docstring appears to be a copy-paste error from a different function.
- `rust/datafusion_python/src/dataframe.rs:148` -- `on()` method docstring says "Retrieves the right input LogicalPlan" but actually retrieves the join ON conditions. Copy-paste error from `right()`.
- `rust/datafusion_python/src/codeanatomy_ext.rs:468-473` -- `now_unix_ms()` returns `0` on failure (`unwrap_or(0)`) rather than propagating the error. A timestamp of `0` (January 1, 1970) would be deeply confusing in production telemetry.

**Suggested improvement:**
1. Use a lazily-initialized thread-local or `OnceCell` Tokio runtime instead of creating one per call.
2. Fix copy-paste docstring errors on `register_table_provider` and `PyJoin::on()`.
3. Make `now_unix_ms()` return `PyResult<i64>` so failures are visible, or use `expect()` since `SystemTime` failure is extremely unlikely on supported platforms.

**Effort:** medium
**Risk if unaddressed:** medium -- Misleading docstrings cause incorrect usage. Runtime-per-call overhead accumulates in batch delta operations.

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
Explicit version contracts exist for the plugin system and delta scan config. The PyO3 module registration is the de facto public contract but is not versioned.

**Findings:**
- `rust/df_plugin_api/src/manifest.rs:27-29` -- `DF_PLUGIN_ABI_MAJOR = 1`, `DF_PLUGIN_ABI_MINOR = 1` explicitly version the plugin ABI.
- `rust/datafusion_python/src/codeanatomy_ext.rs:126` and `rust/df_plugin_codeanatomy/src/lib.rs:44` -- `DELTA_SCAN_CONFIG_VERSION = 1` versions the scan config payload format (but is duplicated -- see P7).
- `rust/datafusion_python/src/codeanatomy_ext.rs:3442-3582` -- The `init_module()` and `init_internal_module()` functions define the public Python API surface, but there is no explicit versioning of this surface beyond the module structure itself.
- `rust/codeanatomy_engine_py/src/result.rs` -- `PyRunResult` exposes JSON structure without schema versioning.

**Suggested improvement:**
Add a `__version__` attribute to the Python modules that reflects the Rust crate version. Consider adding a schema version to `PyRunResult`'s JSON output format.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Testability and Observability

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
The monolithic structure of `codeanatomy_ext.rs` makes unit testing impractical. Functions require a live PyO3 Python interpreter, a Tokio runtime, and a valid DataFusion `SessionContext` to test.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext.rs` -- Every `#[pyfunction]` requires a Python interpreter, a `Bound<'_, PyAny>` context object, and creates a Tokio runtime. None of the internal logic can be tested without this full stack.
- `rust/df_plugin_codeanatomy/src/lib.rs:524-620` -- Has good integration tests (`plugin_delta_scan_config_matches_control_plane`, `plugin_snapshot_matches_native`) that verify plugin behavior matches the control plane.
- `rust/codeanatomy_engine_py/src/compiler.rs:113-280` -- Pure domain logic (`canonical_rulepack_profile`, `map_join_type`, `cpg_output_kind_for_view`) is embedded in the `#[pymethods]` impl block and cannot be tested without PyO3.
- `rust/datafusion_python/src/dataframe.rs` -- `collect_record_batches_to_display` (line 1322) is an `async fn` with pure logic that could be tested independently, but it requires a `DataFrame` which requires a session.

**Suggested improvement:**
Extract pure logic from PyO3 methods into standalone Rust functions:
1. Move `canonical_rulepack_profile()`, `map_join_type()`, `cpg_output_kind_for_view()`, `default_rule_intents()`, `build_transform()` from `compiler.rs` into `codeanatomy_engine` core crate.
2. Extract shared helpers from `codeanatomy_ext.rs` into testable utility functions that don't require Python.
3. The `delta_gate_from_params()` and `commit_options_from_params()` patterns are good and should be extended -- they encapsulate construction logic in testable helpers.

**Effort:** medium
**Risk if unaddressed:** medium -- Core domain logic in the binding layer cannot be unit tested, reducing confidence in correctness.

---

#### P24. Observability -- Alignment: 1/3

**Current state:**
Some structured observability exists but is inconsistent.

**Findings:**
- `rust/datafusion_python/src/codeanatomy_ext.rs` -- `runtime_execution_metrics_snapshot()` provides structured JSON telemetry with schema version, event time, memory metrics, and cache statistics. This is a good observability pattern.
- `rust/datafusion_python/src/codeanatomy_ext.rs` -- `install_tracing()` exists as a function, suggesting tracing infrastructure is available.
- `rust/datafusion_python/src/codeanatomy_ext.rs` -- Most error paths use `format!("Delta {op} failed: {err}")` strings without structured fields. There are no `tracing::instrument` attributes, span contexts, or structured error types on the PyO3 functions.
- `rust/codeanatomy_engine_py/src/errors.rs:12-41` -- `engine_execution_error()` creates structured Python errors with stage/code/message/details attributes. This is a good pattern that should be extended to `codeanatomy_ext.rs`.
- `rust/codeanatomy_engine_py/src/materializer.rs` -- No explicit span/tracing instrumentation on the `execute()` method despite it being the primary execution entry point.

**Suggested improvement:**
Add `#[tracing::instrument]` to key PyO3 functions (at minimum: `delta_write_ipc`, `delta_merge`, `install_codeanatomy_runtime`, `register_dataset_provider`). Use the structured error pattern from `engine_execution_error()` for delta operation failures.

**Effort:** small
**Risk if unaddressed:** low -- Current observability is sufficient for basic debugging but insufficient for production correlation of Rust-side operations.

---

## Cross-Cutting Themes

### Theme 1: The `codeanatomy_ext.rs` Monolith

**Root cause:** All CodeAnatomy-specific PyO3 extensions were added to a single file as the project grew. The PyO3 `#[pymodule]` registration pattern encourages collecting functions in one place.

**Affected principles:** P2 (separation of concerns), P3 (SRP), P4 (cohesion), P14 (Law of Demeter), P15 (tell don't ask), P19 (KISS), P21 (least astonishment), P23 (testability), P24 (observability).

**Suggested approach:** Decompose into a `codeanatomy_ext/` module directory with 6-8 focused submodules. Each submodule provides an `init_submodule()` function that the top-level `init_module()` calls. This is a mechanical refactoring that does not change any public API.

### Theme 2: Knowledge Duplication Across Crate Boundaries

**Root cause:** The plugin system (`df_plugin_*`) and the control plane (`codeanatomy_ext.rs`) evolved independently but need to agree on shared constants, algorithms, and validation logic. No shared utility crate exists for this common knowledge.

**Affected principles:** P7 (DRY).

**Suggested approach:** Create a `df_plugin_common` crate (or extend `df_plugin_api`) that owns `parse_major()`, `schema_from_ipc()`, `DELTA_SCAN_CONFIG_VERSION`, and manifest validation logic. Both `codeanatomy_ext.rs` and the plugin crates depend on this shared source of truth.

### Theme 3: PyO3 Boundary Leaks Internal State

**Root cause:** PyO3 `#[pyclass]` structs expose inner fields as `pub` for convenient access within the `datafusion_python` crate. This bypasses intended encapsulation.

**Affected principles:** P1 (information hiding), P14 (Law of Demeter).

**Suggested approach:** Change `pub` fields to `pub(crate)` with accessor methods. This is low-risk since the fields are not visible to Python (PyO3 controls the Python-visible API separately). The main impact is within the Rust crate itself.

### Theme 4: Contrast Between Well-Designed and Poorly-Designed Modules

The codebase contains both excellent and poor design within the same scope:
- **Excellent:** `codeanatomy_engine_py/` (5 files, clear SRP, clean composition, private fields)
- **Excellent:** `df_plugin_api/` (minimal, well-typed, ABI-stable)
- **Excellent:** `df_plugin_host/` (focused, testable, clean separation)
- **Poor:** `codeanatomy_ext.rs` (monolithic, 80+ functions, duplicated knowledge)

The well-designed modules can serve as templates for refactoring the poorly-designed one.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Extract `parse_major()`, `schema_from_ipc()`, `DELTA_SCAN_CONFIG_VERSION` into shared crate | medium | Eliminates silent drift risk for 3 duplicated knowledge items |
| 2 | P21 (Least astonishment) | Fix copy-paste docstring errors on `register_table_provider` and `PyJoin::on()` | small | Prevents misuse from misleading documentation |
| 3 | P18 (Determinism) | Make `now_unix_ms()` return `PyResult<i64>` and document nondeterministic fields | small | Prevents silent 1970-timestamp in production telemetry |
| 4 | P1 (Information hiding) | Change `pub ctx` on `PySessionContext` to `pub(crate) ctx` | small | Prevents accidental external dependency on context internals |
| 5 | P21 (Least astonishment) | Use lazy thread-local Tokio runtime instead of creating one per `runtime()` call | small | Eliminates surprising per-call runtime overhead |

## Recommended Action Sequence

1. **Extract shared knowledge crate** (P7). Create `df_plugin_common` with `parse_major()`, `schema_from_ipc()`, `DELTA_SCAN_CONFIG_VERSION`, and manifest validation. This has no functional change but eliminates the highest-risk knowledge duplication. Wire `codeanatomy_ext.rs`, `df_plugin_host`, and `df_plugin_codeanatomy` to use it.

2. **Fix documentation errors** (P21). Correct the copy-paste docstring on `context.rs:631` (`register_table_provider`) and `dataframe.rs:148` (`PyJoin::on()`). Fix `now_unix_ms()` error handling.

3. **Tighten field visibility** (P1). Change `pub ctx` to `pub(crate) ctx` on `PySessionContext`. Add `pub(crate) fn ctx(&self) -> &SessionContext` accessor. Update the 63 `session_context_contract(ctx)?.ctx` call sites to use the accessor or a refined helper. Apply same pattern to `PyExpr`, `PyColumn`, stores.

4. **Decompose `codeanatomy_ext.rs`** (P2, P3, P4, P19). This is the largest and most impactful change. Split into 6-8 submodules as described in P2. Each submodule gets its own `init_submodule()`. The top-level `mod.rs` re-exports and delegates registration. This unblocks improvements for P14, P15, P23, and P24.

5. **Extract pure domain logic from PyO3 layer** (P16, P23). Move `canonical_rulepack_profile()`, `map_join_type()`, `cpg_output_kind_for_view()`, `default_rule_intents()`, `build_transform()` from `compiler.rs` into the `codeanatomy_engine` core crate. Add unit tests.

6. **Introduce request structs for delta operations** (P19, P15). Define `DeltaGateParams`, `DeltaCommitParams`, `DeltaTableLocator` structs to replace 15+ parameter functions. PyO3 `#[pyfunction]` signatures can remain keyword-based externally while using these structs internally.

7. **Add observability instrumentation** (P24). Apply `#[tracing::instrument]` to key entry points. Use the `engine_execution_error()` pattern from `codeanatomy_engine_py/src/errors.rs` for structured delta operation errors.

8. **Consolidate or document `init_module` vs `init_internal_module`** (P20). Determine if the two registration paths serve distinct purposes. If so, document the rationale. If not, consolidate to a single registration function.
