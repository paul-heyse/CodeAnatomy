# Design Review: rust/datafusion_ext/src/ -- Delta, Registry, and Function Infrastructure

**Date:** 2026-02-16
**Scope:** `rust/datafusion_ext/src/` -- delta_*.rs, registry_snapshot.rs, function_factory.rs, function_rewrite.rs, udf_registry.rs, udf_config.rs, udf_docs.rs, udf_expr.rs, expr_planner.rs, planner_rules.rs, physical_rules.rs, config_macros.rs, macros.rs, async_runtime.rs, udf_async.rs, compat.rs, errors.rs, error_conversion.rs, lib.rs
**Focus:** Knowledge (7-11), Boundaries (1-6), Simplicity (19-22) -- weighted; all 24 scored
**Depth:** moderate
**Files reviewed:** 24 (6,910 LOC total)

## Executive Summary

The DataFusion extension crate demonstrates strong architectural fundamentals: clear module boundaries between delta operations, UDF registration, and planning infrastructure; well-typed request structs that replace positional arguments; and a clean `FunctionFactory` implementation that leverages DataFusion's modern `ScalarUDFImpl` trait. The primary weaknesses are (1) significant code duplication between `delta_mutations.rs` and `delta_maintenance.rs` (duplicated `latest_operation_metrics`, `snapshot_with_gate`, and the "load-gate-operate-report" ceremony), (2) a naming collision between top-level `function_factory.rs` (SQL macro factory, 920 LOC) and `udf/function_factory.rs` (UDF primitive factory, 695 LOC), and (3) hardcoded capability maps in `registry_snapshot.rs` that must be kept manually synchronized with trait implementations in `udaf_builtin.rs` and `udwf_builtin.rs`.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `RegistrySnapshot` exposes 15 public `BTreeMap` fields; `DeltaScanConfig` internal fields mutated directly |
| 2 | Separation of concerns | 2 | medium | medium | Delta operation modules mix table loading, protocol gating, operation execution, and report assembly |
| 3 | SRP (one reason to change) | 2 | medium | medium | `registry_snapshot.rs` changes for new UDFs, new hook capabilities, new snapshot format, and new custom signatures |
| 4 | High cohesion, low coupling | 2 | small | low | Tight delta module cluster is well-cohered; `registry_snapshot.rs` reaches into 5 sibling modules |
| 5 | Dependency direction | 3 | - | - | Core types flow inward; delta modules depend on delta_protocol, not vice versa |
| 6 | Ports & Adapters | 2 | medium | low | No trait abstraction for delta table loading; all delta modules directly call `load_delta_table` |
| 7 | DRY (knowledge) | 1 | medium | high | `latest_operation_metrics` + `snapshot_with_gate` duplicated identically in delta_mutations.rs and delta_maintenance.rs; `parse_rfc3339` duplicated |
| 8 | Design by contract | 2 | small | low | Request structs enforce structure; `protocol_gate` provides explicit contract; some functions lack doc comments |
| 9 | Parse, don't validate | 3 | - | - | Request structs parse at boundary; `schema_mode_from_label` converts strings to enum once |
| 10 | Make illegal states unrepresentable | 2 | medium | medium | `FunctionKind` enum duplicated between `function_factory.rs` and `registry_snapshot.rs` with different variants |
| 11 | CQS | 3 | - | - | Functions are cleanly separated into queries (snapshot, registry) and commands (mutations, maintenance) |
| 12 | DI + explicit composition | 2 | small | low | `lib.rs` wires components explicitly; `SessionContext` injected throughout; `udf_async` uses global `OnceLock` policy |
| 13 | Composition over inheritance | 3 | - | - | Composition-only; `RenamedAggregateUdf`/`RenamedWindowUdf` wrap inner impls via delegation |
| 14 | Law of Demeter | 2 | small | low | `table.snapshot()?.snapshot().clone()` chain appears 6x across delta modules |
| 15 | Tell, don't ask | 2 | small | low | `RegistrySnapshot` is a data bag; callers build payloads externally in `delta_observability.rs` |
| 16 | Functional core, imperative shell | 2 | small | low | Protocol gating and config parsing are pure; delta operations mix IO and logic in same functions |
| 17 | Idempotency | 3 | - | - | Registry snapshot is read-only; delta operations delegate idempotency to Delta protocol |
| 18 | Determinism | 3 | - | - | All collections sorted (`BTreeMap`, sorted vecs); snapshot deterministic |
| 19 | KISS | 2 | small | low | Deprecated positional-arg shim functions add ~40% LOC in delta_mutations.rs and delta_maintenance.rs |
| 20 | YAGNI | 2 | small | low | `DeltaProtocolSnapshotPayload` + msgpack validation path appears speculative; `FunctionFactoryPolicy` struct defined but unused |
| 21 | Least astonishment | 2 | medium | medium | Two files named `function_factory.rs` at different levels; `FunctionKind` exists in two modules with different variant sets |
| 22 | Declare/version contracts | 2 | small | low | `generated/delta_types.rs` provides versioned types; `RegistrySnapshot` lacks version field |
| 23 | Design for testability | 2 | medium | medium | Delta operations require live `SessionContext` + table; no trait seam for table loading; `registry_snapshot` requires full `SessionState` |
| 24 | Observability | 2 | small | low | `delta_observability.rs` provides structured payloads; no tracing spans or metrics in any delta operation |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
`RegistrySnapshot` at `registry_snapshot.rs:13-30` has 15 public fields, all of which are mutable `Vec`/`BTreeMap` types. Callers can directly modify snapshot contents after construction. `DeltaScanConfig` fields are directly mutated in `apply_overrides` at `delta_control_plane.rs:175-195`.

**Findings:**
- `registry_snapshot.rs:13-30`: `RegistrySnapshot` struct exposes all 15 fields as `pub`, including internal implementation details like `rewrite_tags`, `simplify`, `coerce_types`, and `short_circuits`. These are populated by internal functions but readable and writable by any consumer.
- `delta_control_plane.rs:175-195`: `apply_overrides` directly mutates `DeltaScanConfig` fields (`scan_config.file_column_name`, `scan_config.enable_parquet_pushdown`, etc.) rather than using the `DeltaScanConfigBuilder` API that delta-rs provides.

**Suggested improvement:**
Make `RegistrySnapshot` fields private and expose read-only accessors. For `DeltaScanConfig`, consistently use `DeltaScanConfigBuilder` (already available from `deltalake::delta_datafusion::DeltaScanConfigBuilder`) for all configuration, not just the file-column path.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
Each delta operation function (e.g., `delta_write_batches_request` at `delta_mutations.rs:383-436`) mixes four concerns: (1) table loading, (2) protocol gate enforcement, (3) the actual delta-rs operation, and (4) report assembly with metrics collection. The same four-phase pattern is repeated in every public function across `delta_mutations.rs` and `delta_maintenance.rs`.

**Findings:**
- `delta_mutations.rs:383-436` (`delta_write_batches_request`): Loads table, gates protocol, runs constraint check, executes write, collects metrics, builds report -- all in one function body.
- `delta_maintenance.rs:160-197` (`delta_optimize_compact_request`): Identical pattern: load table, gate, operate, collect metrics, build report.
- The "load-gate-operate-report" ceremony is repeated ~10 times across the two files.

**Suggested improvement:**
Extract the load-gate-report ceremony into a shared helper or higher-order function that accepts a closure for the operation-specific logic:
```rust
// In a shared delta_ops.rs or delta_common.rs module:
async fn with_delta_table<F, Fut, R>(
    table_uri: &str, storage_options: ..., version: ..., timestamp: ...,
    session_ctx: ..., gate: ..., operation: &str, f: F,
) -> Result<DeltaOperationReport, DeltaTableError>
where
    F: FnOnce(DeltaTable) -> Fut,
    Fut: Future<Output = Result<(DeltaTable, ...), DeltaTableError>>,
```

**Effort:** medium
**Risk if unaddressed:** medium -- each new delta operation requires copying the full ceremony, inviting subtle divergence

---

#### P3. SRP (one reason to change) -- Alignment: 2/3

**Current state:**
`registry_snapshot.rs` (1,053 LOC) changes for at least four independent reasons: (1) new UDF registrations, (2) changes to hook capability tracking, (3) changes to custom signature definitions, (4) changes to the snapshot serialization format.

**Findings:**
- `registry_snapshot.rs:383-716`: `custom_table_signatures()` and `custom_signatures()` define schema types and signature metadata for all custom UDFs -- this is UDF catalog knowledge embedded in the snapshot module.
- `registry_snapshot.rs:94-146`: `snapshot_hook_capabilities()` is a distinct concern (CI assertion support) from the core registry snapshot logic.
- `registry_snapshot.rs:746-823`: `rewrite_tags_for()` and `simplify_flag_for()` are hardcoded lookup tables that change whenever a new UDF is added.

**Suggested improvement:**
Split `registry_snapshot.rs` into three focused modules: (1) `registry_snapshot.rs` -- snapshot construction and serialization (the `RegistrySnapshot` struct + `registry_snapshot()` entry point), (2) `registry_capabilities.rs` -- `FunctionHookCapabilities` + `snapshot_hook_capabilities()`, (3) move custom signature definitions into the UDF modules themselves (each UDF module provides its own signature metadata).

**Effort:** medium
**Risk if unaddressed:** medium -- adding a new UDF requires editing at least 3 separate locations within the same file

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The delta module cluster (`delta_control_plane.rs`, `delta_mutations.rs`, `delta_maintenance.rs`, `delta_protocol.rs`, `delta_observability.rs`) is well-cohered around Delta operations. However, `registry_snapshot.rs` has high fan-in, importing from `udf_config`, `udf`, `udf_docs`, `udf_registry`, `udaf_builtin`, and `udwf_builtin`.

**Findings:**
- `registry_snapshot.rs:10-11`: Imports from 6 crate modules (`udf_config`, `udf`, `udf_docs`, `udf_registry`, `udaf_builtin`, `udwf_builtin`). This is the highest fan-in file in the scope.
- The delta modules have clean coupling: `delta_mutations.rs` and `delta_maintenance.rs` both depend on `delta_control_plane` (for `load_delta_table`) and `delta_protocol` (for `DeltaSnapshotInfo`), which is reasonable.

**Suggested improvement:**
Reduce `registry_snapshot.rs` fan-in by having UDF modules expose their own metadata (signatures, capabilities, rewrite tags) via a trait or struct, which the snapshot module can collect generically.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 3/3

**Current state:**
Dependencies flow correctly inward. `delta_protocol.rs` defines core types (`DeltaSnapshotInfo`, `DeltaFeatureGate`) that all delta modules depend on. `lib.rs` re-exports generated types from `generated/delta_types.rs`. The UDF infrastructure has clean layering: `macros.rs` -> `udf_registry.rs` -> `udf_config.rs` -> individual UDF modules.

**Findings:**
No violations found. The dependency graph is well-ordered.

**Effort:** -
**Risk if unaddressed:** -

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
There is no trait abstraction for delta table loading. All delta operation modules directly call `load_delta_table` (a concrete async function in `delta_control_plane.rs:140-149`), making it impossible to substitute a mock or in-memory table for testing.

**Findings:**
- `delta_mutations.rs:272-279`, `delta_mutations.rs:401-408`, `delta_mutations.rs:486-493`, `delta_mutations.rs:551-558`, `delta_mutations.rs:631-638`: Five direct calls to `load_delta_table`.
- `delta_maintenance.rs:173-180`, `delta_maintenance.rs:240-247`, etc.: Seven more direct calls.
- The `SessionContext` is passed through as a dependency, which is good, but the table loading itself is not abstracted.

**Suggested improvement:**
Define a `DeltaTableLoader` trait with an `async fn load(...)` method. Pass it as a type parameter or trait object to delta operation functions. This enables unit testing of operation logic without hitting the filesystem.

**Effort:** medium
**Risk if unaddressed:** low -- primarily affects testability

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge) -- Alignment: 1/3

**Current state:**
This is the most significant gap in the reviewed scope. Multiple functions are duplicated identically across modules, and hardcoded lookup tables must be kept in sync manually.

**Findings:**
- `delta_mutations.rs:183-199` and `delta_maintenance.rs:116-132`: `latest_operation_metrics()` is duplicated verbatim (17 lines, identical logic). Both fetch `table.history(Some(1))` and extract `operationMetrics`.
- `delta_mutations.rs:247-257` and `delta_maintenance.rs:134-144`: `snapshot_with_gate()` is duplicated verbatim (11 lines, identical logic).
- `delta_control_plane.rs:258-262` and `delta_maintenance.rs:154-158`: `parse_rfc3339_timestamp` / `parse_rfc3339` are duplicated with slightly different names but identical logic.
- `registry_snapshot.rs:154-168` (`known_aggregate_capabilities`): Hardcoded `match` that must be manually synchronized with actual trait implementations in `udaf_builtin.rs`. If a new UDAF adds `GroupsAccumulator` support, this map must be updated separately.
- `registry_snapshot.rs:178-190` (`known_window_capabilities`): Same pattern for window functions and `udwf_builtin.rs`.
- `registry_snapshot.rs:746-793` (`rewrite_tags_for`): 39-entry hardcoded lookup table that must be updated for every new UDF.
- `registry_snapshot.rs:796-822` (`simplify_flag_for`): 18-entry hardcoded lookup that duplicates knowledge already present in each UDF's `ScalarUDFImpl::simplify` implementation.
- `function_factory.rs:34-40` and `registry_snapshot.rs:81-85`: `FunctionKind` enum defined twice -- the `function_factory.rs` version has `Scalar/Aggregate/Window/Table` while `registry_snapshot.rs` has `Scalar/Aggregate/Window`. These represent the same concept with divergent definitions.
- `expr_planner.rs:44-52` (`is_arrow_operator`) and `function_rewrite.rs:61-67` (`is_arrow_operator`): Identical helper function duplicated in both modules. Both also duplicate `can_use_get_field`.

**Suggested improvement:**
1. Extract `latest_operation_metrics`, `snapshot_with_gate`, and `parse_rfc3339` into a shared `delta_common.rs` module.
2. Unify `FunctionKind` into a single definition (likely in `macros.rs` or a new `function_types.rs`).
3. Have each UDF module expose its rewrite tags and simplify flag as associated constants or a method, eliminating the centralized lookup tables. For aggregate/window capabilities, derive them from introspection at snapshot time rather than hardcoding.
4. Extract `is_arrow_operator` and `can_use_get_field` into a shared `operator_utils.rs` module used by both `expr_planner.rs` and `function_rewrite.rs`.

**Effort:** medium
**Risk if unaddressed:** high -- duplicate knowledge will drift (the `known_aggregate_capabilities` map is the most fragile since it has no compile-time enforcement)

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Request structs provide good structural contracts. `protocol_gate` at `delta_protocol.rs:69-101` explicitly checks protocol version requirements. However, the `validate_protocol_gate_payload` msgpack path at `delta_protocol.rs:153-164` silently accepts partial data (fields are `Option`).

**Findings:**
- `delta_protocol.rs:104-110`: `DeltaProtocolSnapshotPayload` has `min_reader_version: Option<i32>` and `min_writer_version: Option<i32>`, meaning a missing version is silently accepted. This weakens the gate contract compared to `DeltaSnapshotInfo` where these are required `i32`.
- `delta_mutations.rs:123-152`: `commit_properties` silently ignores `max_retries < 0` (casts `i64` to `usize` after a check, but the else branch falls through). The contract around negative values is implicit.

**Suggested improvement:**
Make `DeltaProtocolSnapshotPayload` version fields non-optional (matching `DeltaSnapshotInfo`), or add explicit "missing version" error handling in `protocol_gate_snapshot`. Document the `max_retries` contract.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 3/3

**Current state:**
The codebase consistently parses at boundaries. `schema_mode_from_label` at `delta_mutations.rs:234-245` converts string labels to `SchemaMode` enums once. `kind_from_language` at `function_factory.rs:48-66` parses language strings into `FunctionKind`. Request structs enforce structure at construction.

**Findings:**
Well-aligned. No action needed.

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Request structs like `DeltaRestoreRequest` allow both `restore_version` and `restore_timestamp` to be `Some`, which is then validated at runtime (line 324). The `DeltaFeatureGate` type allows empty required features, which is valid but could be more explicit.

**Findings:**
- `delta_maintenance.rs:53-64` (`DeltaRestoreRequest`): Both `restore_version: Option<i64>` and `restore_timestamp: Option<String>` can be `Some` simultaneously, requiring a runtime check at line 324-327. An enum `RestoreTarget { Version(i64), Timestamp(String) }` would make this invalid state unrepresentable.
- `delta_control_plane.rs:111-138` (`delta_table_builder`): `version` and `timestamp` can both be `Some`, requiring the same runtime check at line 118-121.
- `function_factory.rs:34-40`: `FunctionKind` is a plain enum with no type-level connection to the actual function body, so `FunctionKind::Aggregate` paired with a scalar expression body is representable and must be caught at runtime.

**Suggested improvement:**
Replace `(Option<i64>, Option<String>)` version/timestamp pairs with a `TableVersion` enum: `enum TableVersion { Latest, Version(i64), Timestamp(String) }`. This eliminates the "both specified" invalid state at the type level and would be used in all delta request structs.

**Effort:** medium
**Risk if unaddressed:** medium -- the runtime checks are present and correct, but the pattern is repeated across 8+ request structs

---

#### P11. CQS -- Alignment: 3/3

**Current state:**
Functions are cleanly separated. `registry_snapshot()` and `delta_snapshot_info()` are pure queries. `delta_write_*`, `delta_delete_*`, `delta_merge_*` are commands that return reports. No function mixes query and mutation semantics.

**Findings:**
Well-aligned. No action needed.

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
`lib.rs:41-47` (`install_sql_macro_factory_native`) and `lib.rs:49-84` (`install_expr_planners_native`) compose the session explicitly. However, `udf_async.rs:28-56` uses a global `OnceLock<RwLock<AsyncUdfPolicy>>` for policy, which is hidden shared state.

**Findings:**
- `udf_async.rs:28`: `static ASYNC_UDF_POLICY: OnceLock<RwLock<AsyncUdfPolicy>>` is global mutable state. `set_async_udf_policy` at line 33 writes to it; `async_udf_policy` at line 50 reads from it. This hidden dependency makes the async UDF policy non-injectable for testing.
- `async_runtime.rs:8`: `static SHARED_RUNTIME: OnceLock<...>` is acceptable for a Tokio runtime singleton, but the pattern could be more explicit.

**Suggested improvement:**
Pass `AsyncUdfPolicy` through the config system (`ConfigOptions.extensions`) rather than a global `OnceLock`. The `with_updated_config` hook at `udf_async.rs:152-165` already demonstrates this pattern -- extend it to be the primary policy source.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
The codebase uses composition exclusively. `RenamedAggregateUdf` at `function_factory.rs:503-636` wraps an `Arc<dyn AggregateUDFImpl>` via delegation. `RenamedWindowUdf` follows the same pattern. No inheritance hierarchies exist.

**Findings:**
Well-aligned. No action needed.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
The `table.snapshot()?.snapshot().clone()` chain appears in multiple delta modules, reaching two levels deep into the table internals.

**Findings:**
- `delta_mutations.rs:281`, `delta_mutations.rs:410`, `delta_mutations.rs:640`: `table.snapshot()?.snapshot().clone()` -- three levels of indirection to get an `EagerSnapshot`.
- `delta_control_plane.rs:299`: Same pattern.
- `delta_protocol.rs:40`: `table.snapshot()?.snapshot().clone()` in `delta_snapshot_info`.

**Suggested improvement:**
Extract a helper function `fn eager_snapshot(table: &DeltaTable) -> Result<EagerSnapshot, DeltaTableError>` that encapsulates this chain. Place it in the shared delta common module alongside `latest_operation_metrics`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`RegistrySnapshot` is a pure data bag. External code in `delta_observability.rs` manually constructs JSON payloads by asking for individual fields. `DeltaSnapshotInfo` is similar.

**Findings:**
- `delta_observability.rs:52-106` (`snapshot_payload`): Manually reads each field of `DeltaSnapshotInfo` to build a `HashMap<String, serde_json::Value>`. The `DeltaSnapshotInfo` type could provide its own serialization.
- `delta_observability.rs:196-208` (`mutation_report_payload`): Extends the snapshot payload manually for mutation reports.

**Suggested improvement:**
Derive `Serialize` on `DeltaSnapshotInfo` (it already has the `Clone` derive) and use `serde_json::to_value()` instead of manual payload construction. This moves serialization knowledge into the type itself.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Protocol gating (`delta_protocol.rs:69-101`) and config parsing (`config_macros.rs`, `udf_config.rs`) are pure functions. Delta operations necessarily mix IO, but the pure validation logic (constraint checking, protocol gating) could be more cleanly separated.

**Findings:**
- `delta_mutations.rs:201-225` (`run_constraint_check`): This pure validation logic is embedded in the mutations module. It could be extracted to the protocol module and tested independently.
- `function_factory.rs:415-433` (`SqlMacroUdf::simplify`): Clean pure transformation -- template substitution via `transform_up`. Good example of functional core.

**Suggested improvement:**
Move `run_constraint_check` and `ensure_no_violations` to `delta_protocol.rs` so all validation logic lives together, testable without async/IO context.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Read operations are naturally idempotent. Write operations delegate idempotency to the Delta transaction protocol (optimistic concurrency, commit retries). The `CommitProperties` builder properly forwards `max_retries` for retry semantics.

**Findings:**
Well-aligned. No action needed.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
`RegistrySnapshot` sorts all collections (`registry_snapshot.rs:205-209`). Feature lists are sorted in `delta_protocol.rs:24-34`. `BTreeMap` is used consistently for ordered output. The `policy_hash` and `ddl_fingerprint` determinism contract is respected.

**Findings:**
Well-aligned. No action needed.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The deprecated positional-argument shim functions add substantial LOC without providing new functionality.

**Findings:**
- `delta_mutations.rs:286-309`, `delta_mutations.rs:348-381`, `delta_mutations.rs:438-471`, `delta_mutations.rs:512-535`, `delta_mutations.rs:581-606`, `delta_mutations.rs:716-759`: Six deprecated shim functions that simply construct a request struct and delegate. Together they account for ~200 lines (~26% of the file).
- `delta_maintenance.rs:199-222`, `delta_maintenance.rs:279-308`, etc.: Eight more deprecated shims, ~250 lines (~34% of the file).
- The `#[deprecated]` annotations are good practice, but the shims should be removed in the next breaking release to reduce cognitive load.

**Suggested improvement:**
Schedule removal of all deprecated positional-argument functions in the next major version. They serve no purpose beyond backward compatibility during the migration to request structs.

**Effort:** small
**Risk if unaddressed:** low -- cosmetic bloat, not a correctness risk

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
A few structures appear to be defined ahead of concrete usage.

**Findings:**
- `delta_protocol.rs:104-110` (`DeltaProtocolSnapshotPayload`) + `delta_protocol.rs:153-164` (`validate_protocol_gate_payload`): This msgpack-based protocol validation path appears speculative. It duplicates the `protocol_gate` function using a weaker type (`Option` fields vs required fields) and introduces an additional serialization dependency (`rmp_serde`). No call sites were found in the reviewed Python integration layer.
- `udf/function_factory.rs:49-50` (`FunctionFactoryPolicy`): The struct is defined with a `primitives` field but its full usage was not visible in the first 50 lines. If it is a stub for future work, it should be flagged.

**Suggested improvement:**
Validate that `validate_protocol_gate_payload` has active callers. If not, remove it and the `rmp_serde` dependency to reduce surface area. If it is used by PyO3 bindings, document the contract.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
The naming collision between two `function_factory.rs` files is the most significant surprise.

**Findings:**
- `rust/datafusion_ext/src/function_factory.rs` (920 LOC): Implements `SqlMacroFunctionFactory` for `CREATE FUNCTION` SQL macro support.
- `rust/datafusion_ext/src/udf/function_factory.rs` (695 LOC): Implements a UDF primitive factory for rule-author convenience (function registration from specs).
- A developer searching for "function factory" will find both files and must read them to understand which is which. The names do not communicate their distinct roles.
- `FunctionKind` exists in `function_factory.rs:34-40` (Scalar/Aggregate/Window/Table) and `registry_snapshot.rs:80-85` (Scalar/Aggregate/Window, with `Serialize`/`Deserialize`). Different variant sets for the same concept.

**Suggested improvement:**
Rename `function_factory.rs` to `sql_macro_factory.rs` (it implements `SqlMacroFunctionFactory` after all). Rename `udf/function_factory.rs` to `udf/primitives.rs` or `udf/registration_factory.rs`. Unify `FunctionKind` into a single definition.

**Effort:** medium
**Risk if unaddressed:** medium -- naming confusion causes incorrect assumptions about module responsibility

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
`generated/delta_types.rs` provides versioned types (`DeltaAppTransaction`, `DeltaCommitOptions`, `DeltaFeatureGate`) that are re-exported from `lib.rs`. However, `RegistrySnapshot` has no version field and its shape is implicitly coupled to the Python `registry_facade.py` consumer.

**Findings:**
- `registry_snapshot.rs:13-30`: No version field on `RegistrySnapshot`. Adding a field changes the serialization contract for all consumers.
- `lib.rs:33`: Re-exports generated types cleanly, establishing them as the public contract.
- The `compat.rs` module (`compat.rs:1-9`) provides a thin compatibility layer for DataFusion types, explicitly scoped to DataFusion 51.x. This is good practice.

**Suggested improvement:**
Add a `version: u32` field to `RegistrySnapshot` (starting at 1) so consumers can detect schema changes.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
The pure-logic modules (`delta_protocol.rs`, `function_factory.rs` validation functions, `config_macros.rs` parsing) are easily testable. However, the delta operation modules require live async execution with `SessionContext` and actual tables.

**Findings:**
- No trait seam for `load_delta_table`: all delta operations are tightly coupled to filesystem-backed delta tables.
- `udf_async.rs:225-244`: Has a unit test for config propagation. Good.
- `function_rewrite.rs:76-108`: Has a unit test for arrow rewrite. Good.
- `udf_registry.rs:136-168`: Has unit tests for registry contents. Good.
- `registry_snapshot.rs`: No tests. The largest file (1,053 LOC) with the most complex logic has no unit tests.

**Suggested improvement:**
Add unit tests for `registry_snapshot.rs` -- at minimum, test `format_data_type`, `signature_arity`, `rewrite_tags_for`, and `simplify_flag_for`. For delta operations, extract the load/gate/operate/report ceremony behind a trait to enable mock-based testing.

**Effort:** medium
**Risk if unaddressed:** medium -- the most complex module has no test coverage

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
`delta_observability.rs` provides structured serialization payloads for snapshot info, scan config, add actions, mutation reports, and maintenance reports. However, there are no tracing spans, structured log events, or metrics in any of the delta operation functions.

**Findings:**
- `delta_observability.rs:52-224`: Good structured payload generation for all delta operation types.
- No `tracing::instrument` or span annotations on any delta operation function. For a crate that orchestrates multi-second async operations (table loads, writes, merges, vacuums), this is a gap.
- No error context enrichment -- delta errors propagate as `DeltaTableError::Generic(String)` without structured context (table URI, operation, version).

**Suggested improvement:**
Add `tracing::instrument` spans to all public delta operation functions with fields for `table_uri`, `operation`, and `version`. Consider wrapping `DeltaTableError::Generic` in a context-enriching helper that includes the table URI.

**Effort:** small
**Risk if unaddressed:** low -- operational debugging will rely on Python-side observability

---

## Cross-Cutting Themes

### Theme 1: Delta Operation Ceremony Duplication

**Root cause:** Each delta operation independently implements the load-gate-operate-collect-report lifecycle. There is no shared orchestration abstraction.

**Affected principles:** P2 (separation of concerns), P7 (DRY), P19 (KISS), P23 (testability)

**Suggested approach:** Extract a `DeltaOperationRunner` or higher-order async function that handles loading, gating, metrics collection, and report construction. Operation-specific logic is provided as a closure or strategy. This eliminates ~50% of the duplicated code across `delta_mutations.rs` and `delta_maintenance.rs`.

### Theme 2: Hardcoded UDF Metadata Scattered Across Registry Snapshot

**Root cause:** `registry_snapshot.rs` maintains centralized lookup tables (`rewrite_tags_for`, `simplify_flag_for`, `known_aggregate_capabilities`, `known_window_capabilities`, `custom_signatures`, `custom_table_signatures`) that duplicate knowledge already encoded in individual UDF implementations.

**Affected principles:** P3 (SRP), P7 (DRY), P23 (testability)

**Suggested approach:** Define a `UdfMetadata` trait or struct that each UDF module provides, containing its rewrite tags, simplify flag, capabilities, and custom signatures. `registry_snapshot.rs` collects these via a registry pattern rather than maintaining parallel lookup tables.

### Theme 3: Two-File Naming Collision

**Root cause:** Both `function_factory.rs` (SQL macro factory) and `udf/function_factory.rs` (UDF primitive factory) address "function creation" from different angles but share a name.

**Affected principles:** P21 (least astonishment)

**Suggested approach:** Rename to clarify purpose: `sql_macro_factory.rs` and `udf/primitives.rs`.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Extract `latest_operation_metrics`, `snapshot_with_gate`, `parse_rfc3339` into shared `delta_common.rs` | small | Eliminates 3 identical function duplications across delta modules |
| 2 | P7 (DRY) | Extract `is_arrow_operator` + `can_use_get_field` into shared `operator_utils.rs` | small | Eliminates 2 duplicated helpers between expr_planner.rs and function_rewrite.rs |
| 3 | P21 (Least surprise) | Rename `function_factory.rs` to `sql_macro_factory.rs` | small | Eliminates naming collision with `udf/function_factory.rs` |
| 4 | P14 (Demeter) | Extract `eager_snapshot(table) -> EagerSnapshot` helper | small | Eliminates `table.snapshot()?.snapshot().clone()` 6-chain pattern |
| 5 | P19 (KISS) | Remove deprecated positional-argument shim functions | small | Removes ~450 lines of pure delegation code across delta_mutations.rs and delta_maintenance.rs |

## Recommended Action Sequence

1. **Extract shared delta helpers (P7, P14):** Create `delta_common.rs` with `latest_operation_metrics`, `snapshot_with_gate`, `parse_rfc3339`, and `eager_snapshot`. Update `delta_mutations.rs` and `delta_maintenance.rs` to use shared functions. (Quick wins 1 + 4.)

2. **Extract shared operator helpers (P7):** Create `operator_utils.rs` with `is_arrow_operator` and `can_use_get_field`. Update `expr_planner.rs` and `function_rewrite.rs`. (Quick win 2.)

3. **Rename function_factory.rs (P21):** Rename top-level `function_factory.rs` to `sql_macro_factory.rs`. Update `lib.rs` module declaration and all import sites. (Quick win 3.)

4. **Unify FunctionKind (P7, P10):** Create a single `FunctionKind` definition (with all 4 variants + serde derives) in `macros.rs` or a new `function_types.rs`. Update both `function_factory.rs` and `registry_snapshot.rs`.

5. **Add RegistrySnapshot version field (P22):** Add `version: u32` to `RegistrySnapshot` and set it to 1.

6. **Introduce TableVersion enum (P10):** Replace `(Option<i64>, Option<String>)` version/timestamp pairs with a `TableVersion` enum across all delta request structs.

7. **Add tracing spans to delta operations (P24):** Add `#[tracing::instrument]` to all public async functions in `delta_mutations.rs`, `delta_maintenance.rs`, and `delta_control_plane.rs`.

8. **Remove deprecated shims (P19):** Delete all `#[deprecated]` positional-argument functions from `delta_mutations.rs` and `delta_maintenance.rs`. (Quick win 5.)

9. **Extract delta operation ceremony (P2, P23):** Introduce a shared higher-order function for the load-gate-operate-report pattern. This depends on items 1 and 6 being complete.

10. **Decompose registry_snapshot.rs (P3):** Split into snapshot construction, hook capabilities, and per-UDF metadata. Move hardcoded lookup tables to their respective UDF modules. This is the largest refactor and depends on items 4-5.

### DataFusion/DeltaLake Library Capabilities to Leverage

- **`DeltaScanConfigBuilder`** (from `deltalake::delta_datafusion`): Already partially used in `delta_control_plane.rs:197-218`, but `apply_overrides` at line 175 bypasses it by directly mutating `DeltaScanConfig` fields. Consistently use the builder for all scan config construction.

- **`DeltaSessionContext`** (from `deltalake::delta_datafusion`): Used at `delta_maintenance.rs:611` for constraint operations. Consider using it more broadly for Delta-specific operations to get correct identifier normalization and parser defaults.

- **DataFusion `ConfigExtension` system**: Already well-used for `CodeAnatomyUdfConfig`, `CodeAnatomyPolicyConfig`, `CodeAnatomyPhysicalConfig`. The async UDF policy (`udf_async.rs:28`) should also use this system instead of a global `OnceLock`.

- **DataFusion `ScalarUDFImpl::with_updated_config`**: Already used correctly in `udf_registry.rs:96-98` and `udf_async.rs:152-165`. This is the right pattern for config-dependent UDF specialization.

### Python Integration Concerns

- **`registry_snapshot.rs` <-> `src/datafusion_engine/registry_facade.py`:** The `RegistrySnapshot` struct's field names and types constitute an implicit API contract with the Python consumer. Adding a version field (recommendation 5) would make schema evolution explicit.

- **`delta_*.rs` <-> `src/datafusion_engine/delta/control_plane_*.py`:** The Python layer constructs request structs and dispatches via PyO3. The deprecated positional-argument shim functions exist to support the migration -- once Python callers are updated, the shims should be removed (recommendation 8).

- **`udf_registry.rs` <-> `src/datafusion_engine/udf/extension_core.py`:** The `register_all_with_policy` function is the primary entrypoint. The Python side should be aware of the async UDF policy global state issue (finding P12).

### Cross-Agent Theme: function_factory.rs Naming Collision

Both the top-level `function_factory.rs` (920 LOC, SQL macro factory reviewed here) and `udf/function_factory.rs` (695 LOC, reviewed by Agent 4) address function creation. The top-level file implements DataFusion's `FunctionFactory` trait for `CREATE FUNCTION` SQL statements. The `udf/` version provides a policy-driven factory for registering UDF primitives from specs. The split is principled (SQL DDL vs programmatic registration), but the identical filenames hide this distinction. Renaming to `sql_macro_factory.rs` and `udf/primitives.rs` (or `udf/registration_factory.rs`) would resolve the collision.
