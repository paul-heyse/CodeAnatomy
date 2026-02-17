# Rust Workspace Design Improvements Implementation Plan v1 (2026-02-16)

## Scope Summary

This plan synthesizes 33 design improvement actions identified across 6 review documents (5 parallel scoped reviewers + 1 cross-agent synthesis) covering the full `rust/` workspace (~58K LOC, 8 crates, 154 .rs files). The actions address systemic DRY violations (avg 0.8/3 across scoped reviews), SRP violations in two monolithic files (`codeanatomy_ext.rs` at 3,582 LOC and `rule_instrumentation.rs` at 815 LOC), copy-paste error messages, knowledge duplication across crate boundaries, and interface safety gaps.

**Design stance:**
- Hard cutover: no backward-compatibility shims for internal Rust APIs (cross-crate refactors update all consumers in the same commit).
- Positional-argument Rust/Python bridge fan-out is removed in the same scope item that introduces request structs; no transitional deprecations remain after each scope lands.
- No new crate dependencies except `datafusion-tracing` (already in workspace) and the new `df_plugin_common` shared crate.
- DataFusion remains pinned at `51.x` for this plan; `52.x` migration is deferred to a separate design/upgrade plan.
- Library built-ins (`ColumnarValue::to_array`, `instrument_with_info_spans!`, `DeltaScanConfigBuilder`, `TableProviderBuilder`, `ConfigExtension`, `GroupsAccumulator` high-perf path) are used wherever they replace hand-rolled code.

## Design Principles

1. **Single authority for shared knowledge.** Every constant, algorithm, or type that appears in more than one crate must live in exactly one crate and be imported by consumers.
2. **Modules have one reason to change.** Files exceeding 1,000 LOC are candidates for decomposition; files exceeding 2,000 LOC require decomposition.
3. **Error messages are contracts.** Every user-facing error string must name the correct function, operation, and expected arguments.
4. **Library capabilities over hand-rolled code.** DataFusion and DeltaLake APIs available in the current pinned version (`datafusion-tracing`, `ColumnarValue::to_array`, `DeltaScanConfigBuilder`, `TableProviderBuilder`, `ConfigExtension`) are preferred over equivalent custom implementations.
5. **Type safety at boundaries.** Replace `(Option<A>, Option<B>)` mutual-exclusion pairs with enums. Replace bare tuple aliases with named structs.
6. **Major upgrades are separate epics.** When a design target requires a newer major API surface, track it in a dedicated migration plan rather than coupling it into this refactor plan.

## Current Baseline

- **DRY is the systemic weakest principle** (avg 0.8/3 across all 5 agents). Every agent independently scored it 0/3 or 1/3.
- **`codeanatomy_ext.rs`** (3,582 LOC, 80+ `#[pyfunction]`, 7+ change reasons) is the single most severe architectural concern. It scores 0/3 on P2, P3, P19.
- **`rule_instrumentation.rs`** (815 LOC) triplicates sentinel/wrapper/wiring for three DataFusion rule traits. While full rule-phase macro adoption exists in newer `datafusion-tracing`, this plan stays on DataFusion/tracing 51 and targets an internal consolidation.
- **`udaf_builtin.rs`** (2,225 LOC) is the largest file in `datafusion_ext`, containing 11 aggregate UDFs with duplicated utilities and identical accumulator implementations.
- **Schema hashing diverges**: `plan_bundle.rs:632` uses JSON-based BLAKE3, while `introspection.rs:34` uses field-by-field BLAKE3. These produce different hashes for the same schema.
- **`parse_major()`** is triplicated across 3 crates with different error types.
- **`schema_from_ipc()`** is duplicated across 2 crates.
- **`DELTA_SCAN_CONFIG_VERSION`** is duplicated across 2 crates with no compile-time sync enforcement.
- **`FunctionKind`** is defined twice with divergent variant sets (3 vs 4 variants).
- **Copy-paste error messages**: `CpgScoreUdf.coerce_types` says "stable_id_parts" (metadata.rs:51), `Utf8NullIfBlankUdf.coerce_types` says "qname_normalize" (string.rs:331).
- **`variadic_any_signature`** ignores its `_min_args` and `_max_args` parameters (common.rs:64).
- **`runner.rs`** duplicates ~100 LOC between `execute_and_materialize` (line 38) and `execute_and_materialize_with_plans` (line 148).
- **Delta helpers** (`latest_operation_metrics`, `snapshot_with_gate`, `parse_rfc3339`) are duplicated verbatim between `delta_mutations.rs` and `delta_maintenance.rs`.
- **`session_context_contract(ctx)?.ctx`** pattern appears 56 times in `codeanatomy_ext.rs`.
- **Delta scan/provider construction is only partially builder-based**: `delta_control_plane.rs` still mutates `DeltaScanConfig` fields directly in `apply_overrides` instead of staying on `DeltaScanConfigBuilder`; provider construction can use `DeltaTable::table_provider()` / `TableProviderBuilder`.
- **Async UDF policy is global state (`OnceLock<RwLock<_>>`)** in `udf_async.rs`, bypassing DataFusion's existing `ConfigExtension` per-session configuration surface.
- **CDF rank** uses magic numbers (0, 1, 2, 3, -1) without named constants (cdf.rs:17-26).
- **`datafusion-tracing` crate** is already in the workspace and successfully used in `exec_instrumentation.rs:38-55` via `instrument_with_info_spans!`; this plan keeps that version line and avoids a coupled major upgrade.

---

## Implementation Audit (2026-02-17)

Legend: `Complete` means scope goals and listed decommissions are materially landed. `Partial` means some high-value items landed but one or more planned scope bullets remain open.

| Scope | Status | Audit Notes |
|---|---|---|
| S1 | Complete | Error-message fixes, variadic signature cleanup, and CDF rank constants are landed. |
| S2 | Complete | Runner delegation, `ensure_source_registered` rename, `SchemaDiff` struct migration, and tuner observe/propose/apply split are landed. |
| S3 | Complete | Schema-hash delegation landed; `now_unix_ms` fallback updated; targeted docstring fixes landed. |
| S4 | Complete | `delta_common` extraction, deprecated shim deletion, builder-first scan config, and provider-builder adoption are landed. |
| S5 | Complete | `df_plugin_common` crate created and wired across host/python/plugin crates; duplicated authorities removed. |
| S6 | Complete | UDF/registry consolidation landed: renamed modules, `udaf_arg_best` extraction, mixed-shape hash simplification, and explicit re-export cleanup are complete. |
| S7 | Complete | `session/capture.rs` authority landed, governance unification landed, `are_inputs_deterministic()` landed, `CpgRuleSet` visibility tightened with accessor migration. |
| S8 | Complete | `codeanatomy_ext` decomposition is materially landed: delta provider/mutation/maintenance operation logic now lives in scoped module files, positional bridge paths are removed, and planned module-level Rust tests were added under `rust/datafusion_python/tests/`. |
| S9 | Complete | DataFusion-51-compatible rule instrumentation consolidation (shared macro/helper path) and parity tests landed. |
| S10 | Complete | `TableVersion` migration, `RegistrySnapshot` versioning, and `codeanatomy_engine_py` compiler-helper extraction are landed. |
| S11 | Complete | Tracing coverage is now present on key `codeanatomy_ext` bridge entrypoints in the decomposed module surfaces, in addition to prior control-plane/compiler/pipeline/materializer spans. |
| S12 | Complete | Snapshot/type authority now lives in `registry/snapshot.rs` + `registry/snapshot_types.rs`, legacy module is a compatibility shim, and Python-side versioned snapshot contract coverage was added. |
| S13 | Complete | Determinism contract artifacts and shared golden tests are authoritative; superseded ad hoc parity checks were removed. |
| S14 | Complete | Async UDF global-state removal and `ConfigExtension` session wiring are landed with bridge coverage additions. |
| D1 | Complete | Cross-crate duplicate authorities targeted by S4/S5 were removed. |
| D2 | Complete | Within-crate duplicate authorities are removed, including legacy registry lookup tables. |
| D3 | Complete | Monolith decommission closure criteria are now satisfied for planned decomposition scope (scoped modules own active delta bridge logic, and decomposition tests are in place). |
| D4 | Complete | Bridge/type hard-cutover is complete: request-typed entrypoints are authoritative and positional PyO3 bridge exports are removed. |
| D5 | Complete | Superseded ad hoc determinism parity checks/prose were removed after shared-golden contract coverage became authoritative. |

---

### Open Scope Delta (2026-02-17 audit refresh)

- Resolved: all prior open items from the 2026-02-17 audit refresh are now implemented.

---

## S1. UDF Error Message and Contract Fixes

### Goal

Fix copy-paste error messages in `CpgScoreUdf.coerce_types` and `Utf8NullIfBlankUdf.coerce_types`, reconcile contradictory argument count contracts, remove dead parameters from `variadic_any_signature`, and add named constants for CDF rank values.

### Representative Code Snippets

```rust
// rust/datafusion_ext/src/udf/metadata.rs — fixed CpgScoreUdf.coerce_types
fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 1 {
        return Err(DataFusionError::Plan(
            "cpg_score expects exactly one argument".into(),
        ));
    }
    Ok(vec![DataType::Utf8; arg_types.len()])
}
```

```rust
// rust/datafusion_ext/src/udf/string.rs — fixed Utf8NullIfBlankUdf.coerce_types
fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 1 {
        return Err(DataFusionError::Plan(
            "utf8_null_if_blank expects exactly one argument".into(),
        ));
    }
    Ok(vec![DataType::Utf8; arg_types.len()])
}
```

```rust
// rust/datafusion_ext/src/udf/common.rs — dead params removed
pub(crate) fn variadic_any_signature(volatility: Volatility) -> Signature {
    user_defined_signature(volatility)
}
```

```rust
// rust/datafusion_ext/src/udf/cdf.rs — named constants
pub(crate) const CDF_RANK_UPDATE_POSTIMAGE: i32 = 3;
pub(crate) const CDF_RANK_INSERT: i32 = 2;
pub(crate) const CDF_RANK_DELETE: i32 = 1;
pub(crate) const CDF_RANK_UPDATE_PREIMAGE: i32 = 0;
pub(crate) const CDF_RANK_UNKNOWN: i32 = -1;

fn cdf_rank(change_type: &str) -> i32 {
    let normalized = change_type.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "update_postimage" => CDF_RANK_UPDATE_POSTIMAGE,
        "insert" => CDF_RANK_INSERT,
        "delete" => CDF_RANK_DELETE,
        "update_preimage" => CDF_RANK_UPDATE_PREIMAGE,
        _ => CDF_RANK_UNKNOWN,
    }
}
```

### Files to Edit

- `rust/datafusion_ext/src/udf/metadata.rs` — fix `CpgScoreUdf.coerce_types` error message and arg count (lines 50-57)
- `rust/datafusion_ext/src/udf/string.rs` — fix `Utf8NullIfBlankUdf.coerce_types` error message (lines 330-337)
- `rust/datafusion_ext/src/udf/common.rs` — remove dead `_min_args`, `_max_args` params from `variadic_any_signature` (lines 64-70)
- `rust/datafusion_ext/src/udf/cdf.rs` — add named constants, update `cdf_rank` and callsites (lines 17-26, 141, 201)
- All callers of `variadic_any_signature` across `rust/datafusion_ext/src/udf/` — update call signatures

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete `_min_args: usize` and `_max_args: usize` parameters from `variadic_any_signature` in `common.rs:64`.
- Delete incorrect error message strings "stable_id_parts expects..." from `metadata.rs:53` and "qname_normalize expects..." from `string.rs:333`.

---

## S2. Runner and Pipeline DRY Consolidation

### Goal

Eliminate the ~100 LOC materialization duplication in `runner.rs` by making `execute_and_materialize` delegate to `execute_and_materialize_with_plans`. Rename `resolve_source_name` to `ensure_source_registered` in `plan_compiler.rs` to fix the CQS violation. Replace `SchemaDiff` tuple alias with a named struct. Split `AdaptiveTuner::observe()` into `record_metrics()` + `propose_adjustment()`.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/executor/runner.rs — delegation pattern
pub async fn execute_and_materialize(
    ctx: &SessionContext,
    output_plans: Vec<(OutputTarget, DataFrame)>,
    lineage: &LineageContext,
) -> Result<Vec<MaterializationResult>> {
    let (results, _plans) =
        execute_and_materialize_with_plans(ctx, output_plans, lineage).await?;
    Ok(results)
}
```

```rust
// rust/codeanatomy_engine/src/compiler/plan_compiler.rs — CQS fix
/// Register the source DataFrame into the SessionContext if it exists
/// in the inline cache. This is a side-effecting operation.
async fn ensure_source_registered(
    &self,
    source: &str,
    inline_cache: &HashMap<String, DataFrame>,
) -> Result<String> {
    if let Some(cached_df) = inline_cache.get(source) {
        self.ctx.register_table(source, cached_df.clone().into_view())?;
    }
    Ok(source.to_string())
}
```

```rust
// rust/codeanatomy_engine/src/schema/introspection.rs — named struct
pub struct SchemaDiff {
    pub added_fields: Vec<String>,
    pub removed_fields: Vec<String>,
    pub changed_fields: Vec<FieldTypeChange>,
}

pub struct FieldTypeChange {
    pub field_name: String,
    pub old_type: DataType,
    pub new_type: DataType,
}
```

```rust
// rust/codeanatomy_engine/src/tuner/adaptive.rs — CQS split
pub fn record_metrics(&mut self, metrics: &ExecutionMetrics) {
    self.observation_count += 1;
    // regression detection, stable_config update, etc.
}

pub fn propose_adjustment(&self) -> Option<TunerConfig> {
    // pure function of current state
    if self.mode == TunerMode::ObserveOnly { return None; }
    // ...bounded adjustment logic...
}
```

### Files to Edit

- `rust/codeanatomy_engine/src/executor/runner.rs` — refactor `execute_and_materialize` to delegate (lines 38-137)
- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs` — rename `resolve_source_name` → `ensure_source_registered` (line 362)
- `rust/codeanatomy_engine/src/schema/introspection.rs` — replace `SchemaDiff` type alias with named struct (line 13)
- `rust/codeanatomy_engine/src/tuner/adaptive.rs` — split `observe()` into `record_metrics()` + `propose_adjustment()` (lines 120-208)
- All callers of `SchemaDiff` — update destructuring to use named fields
- All callers of `observe()` — update to use new two-method API

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete the duplicated materialization loop body in `runner.rs:38-137` (replaced by delegation to `_with_plans` variant).
- Delete `pub type SchemaDiff = (Vec<String>, Vec<String>, Vec<(String, DataType, DataType)>)` at `introspection.rs:13`.
- Delete the combined `observe()` method in `adaptive.rs:120-208` (replaced by split methods).

---

## S3. Schema Hashing and Determinism Chain Consolidation

### Goal

Consolidate schema hashing to use the authoritative field-by-field BLAKE3 approach from `schema::introspection::hash_arrow_schema` as the single source of truth. Eliminate the divergent JSON-based `hash_schema` in `plan_bundle.rs`. Fix `now_unix_ms()` nondeterminism. Fix copy-paste docstring errors.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/compiler/plan_bundle.rs — consolidated hash
use crate::schema::introspection::hash_arrow_schema;

fn hash_schema(schema: &ArrowSchema) -> [u8; 32] {
    // Delegate to the single authoritative hash function.
    // Previously: serde_json::to_string(schema) → BLAKE3
    // Now: field-by-field BLAKE3 matching introspection.rs
    hash_arrow_schema(schema)
}
```

```rust
// rust/datafusion_python/src/codeanatomy_ext.rs — injectable clock
fn now_unix_ms_or(fallback: i64) -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(fallback)
}
// Or accept a clock parameter for testability:
fn cache_table_event_time(clock: Option<i64>) -> i64 {
    clock.unwrap_or_else(|| now_unix_ms_or(-1))
}
```

### Files to Edit

- `rust/codeanatomy_engine/src/compiler/plan_bundle.rs` — replace local `hash_schema` with delegation to `hash_arrow_schema` (lines 632-637)
- `rust/datafusion_python/src/codeanatomy_ext.rs` — fix `now_unix_ms()` to return meaningful error instead of `0` (lines 468-473)
- `rust/datafusion_python/src/context.rs` — fix `register_table_provider` docstring (line 631)
- `rust/datafusion_python/src/expr/join.rs` — fix `PyJoin::on()` docstring (line 149)

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete the local `hash_schema` function body in `plan_bundle.rs:632-637` (replaced by call to `hash_arrow_schema`).

---

## S4. Delta Common Module Extraction

### Goal

Extract duplicated delta helper functions (`latest_operation_metrics`, `snapshot_with_gate`, `parse_rfc3339`, `eager_snapshot`) into a shared `delta_common.rs` module within `datafusion_ext`. Convert delta scan/provider setup to builder-first APIs (`DeltaScanConfigBuilder`, `TableProviderBuilder`) and remove positional-argument shim functions (~450 LOC) in the same cutover.

### Representative Code Snippets

```rust
// rust/datafusion_ext/src/delta_common.rs — new shared module

use deltalake::DeltaTable;
use serde_json::Value as JsonValue;

/// Retrieve the latest operation metrics from a DeltaTable's history.
/// Single source of truth — previously duplicated in delta_mutations.rs and delta_maintenance.rs.
pub(crate) async fn latest_operation_metrics(table: &DeltaTable) -> JsonValue {
    let mut history = match table.history(Some(1)).await {
        Ok(history) => history,
        Err(err) => {
            return JsonValue::String(format!("history unavailable: {err}"));
        }
    };
    match history.next().and_then(|commit| commit.info.get("operationMetrics").cloned()) {
        Some(metrics) => metrics,
        None => JsonValue::Null,
    }
}

/// Load a DeltaTable snapshot with protocol gate enforcement.
pub(crate) async fn snapshot_with_gate(
    table: &DeltaTable,
    gate: &DeltaFeatureGate,
) -> Result<EagerSnapshot, DeltaTableError> {
    let snapshot = eager_snapshot(table)?;
    protocol_gate(&snapshot, gate)?;
    Ok(snapshot)
}

/// Extract EagerSnapshot from a DeltaTable, encapsulating the `.snapshot()?.snapshot().clone()` chain.
pub(crate) fn eager_snapshot(table: &DeltaTable) -> Result<EagerSnapshot, DeltaTableError> {
    Ok(table.snapshot()?.snapshot().clone())
}

/// Parse an RFC 3339 timestamp string to DateTime<Utc>.
pub(crate) fn parse_rfc3339(timestamp: &str) -> Result<DateTime<Utc>, DeltaTableError> {
    DateTime::parse_from_rfc3339(timestamp)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| DeltaTableError::Generic(format!("invalid RFC 3339 timestamp: {e}")))
}
```

```rust
// rust/datafusion_ext/src/delta_control_plane.rs — builder-first scan config
fn scan_config_from_session(
    session: &dyn Session,
    snapshot: Option<&EagerSnapshot>,
    overrides: DeltaScanOverrides,
) -> Result<DeltaScanConfig, DeltaTableError> {
    let Some(snapshot) = snapshot else {
        return Ok(DeltaScanConfig::new_from_session(session));
    };

    let mut builder = DeltaScanConfigBuilder::new()
        .with_parquet_pushdown(overrides.enable_parquet_pushdown.unwrap_or(true))
        .wrap_partition_values(overrides.wrap_partition_values.unwrap_or(true));
    if let Some(file_column) = overrides.file_column_name.as_ref() {
        builder = builder.with_file_column_name(file_column);
    }
    if let Some(schema) = overrides.schema.clone() {
        builder = builder.with_schema(schema);
    }

    let mut config = builder.build(snapshot)?;
    config.schema_force_view_types = overrides.schema_force_view_types.unwrap_or(false);
    Ok(config)
}
```

```rust
// rust/datafusion_ext/src/delta_control_plane.rs — provider from table builder
let provider = table
    .table_provider()
    .with_file_column("source_file")
    .await?;
```

### Files to Edit

- `rust/datafusion_ext/src/delta_mutations.rs` — remove `latest_operation_metrics`, `snapshot_with_gate`, inline `parse_rfc3339` usages; import from `delta_common`; delete deprecated shim functions (~200 LOC)
- `rust/datafusion_ext/src/delta_maintenance.rs` — remove `latest_operation_metrics`, `snapshot_with_gate`, `parse_rfc3339`; import from `delta_common`; delete deprecated shim functions (~250 LOC)
- `rust/datafusion_ext/src/delta_control_plane.rs` — remove `parse_rfc3339_timestamp`; import from `delta_common`; replace direct `.snapshot()?.snapshot().clone()` with `eager_snapshot()`; replace direct `DeltaScanConfig` field mutation path with builder-first construction; adopt `TableProviderBuilder` where provider construction is hand-rolled
- `rust/datafusion_ext/src/delta_protocol.rs` — replace `.snapshot()?.snapshot().clone()` with `eager_snapshot()` in `delta_snapshot_info`
- `rust/datafusion_ext/src/lib.rs` — add `mod delta_common;` declaration

### New Files to Create

- `rust/datafusion_ext/src/delta_common.rs` — shared delta helper functions
- `rust/datafusion_ext/tests/delta_common_tests.rs` — unit tests for `eager_snapshot`, `parse_rfc3339`
- `rust/datafusion_ext/tests/delta_control_plane_builder_tests.rs` — unit tests for `DeltaScanConfigBuilder`/`TableProviderBuilder` path

### Legacy Decommission/Delete Scope

- Delete `latest_operation_metrics` from `delta_mutations.rs:183-199` (moved to `delta_common.rs`).
- Delete `latest_operation_metrics` from `delta_maintenance.rs:116-132` (moved to `delta_common.rs`).
- Delete `snapshot_with_gate` from `delta_mutations.rs:247-257` (moved to `delta_common.rs`).
- Delete `snapshot_with_gate` from `delta_maintenance.rs:134-144` (moved to `delta_common.rs`).
- Delete `parse_rfc3339` / `parse_rfc3339_timestamp` from `delta_control_plane.rs` and `delta_maintenance.rs`.
- Delete `apply_overrides` direct field mutation path in `delta_control_plane.rs` (replaced by `DeltaScanConfigBuilder`).
- Delete all `#[deprecated]` positional-argument shim functions from `delta_mutations.rs` (~200 LOC, 6 functions: lines 286-309, 348-381, 438-471, 512-535, 581-606, 716-759).
- Delete all `#[deprecated]` positional-argument shim functions from `delta_maintenance.rs` (~250 LOC, 8 functions).

---

## S5. Cross-Crate Shared Knowledge (`df_plugin_common`)

### Goal

Create a new `df_plugin_common` crate to own cross-crate shared knowledge: `parse_major()`, `schema_from_ipc()`, `DELTA_SCAN_CONFIG_VERSION`, and manifest validation logic. Eliminate the highest-risk knowledge duplication across the workspace.

### Representative Code Snippets

```rust
// rust/df_plugin_common/src/lib.rs — shared knowledge crate

/// Parse the major version number from a semver string.
/// Single source of truth — previously triplicated across codeanatomy_ext.rs,
/// loader.rs, and df_plugin_codeanatomy/lib.rs.
pub fn parse_major(version: &str) -> Result<u16, String> {
    version
        .split('.')
        .next()
        .ok_or_else(|| format!("no major version in '{version}'"))
        .and_then(|s| {
            s.parse::<u16>()
                .map_err(|e| format!("invalid major version '{s}': {e}"))
        })
}

/// Deserialize an Arrow schema from IPC bytes.
/// Single source of truth — previously duplicated in codeanatomy_ext.rs and
/// df_plugin_codeanatomy/lib.rs.
pub fn schema_from_ipc(ipc_bytes: &[u8]) -> Result<SchemaRef, ArrowError> {
    let ipc_message = arrow::ipc::root_as_message(ipc_bytes)
        .map_err(|e| ArrowError::IpcError(format!("invalid IPC message: {e}")))?;
    let ipc_schema = ipc_message
        .header_as_schema()
        .ok_or_else(|| ArrowError::IpcError("IPC message is not a schema".into()))?;
    arrow::ipc::convert::fb_to_schema(ipc_schema)
}

/// Authoritative Delta scan config version.
/// Previously duplicated in codeanatomy_ext.rs:126 and df_plugin_codeanatomy/lib.rs:44.
pub const DELTA_SCAN_CONFIG_VERSION: u32 = 1;
```

```toml
# rust/df_plugin_common/Cargo.toml
[package]
name = "df_plugin_common"
version = "0.1.0"
edition = "2021"

[dependencies]
arrow = { workspace = true }
```

### Files to Edit

- `rust/datafusion_python/src/codeanatomy_ext.rs` — replace local `parse_major`, `schema_from_ipc`, `DELTA_SCAN_CONFIG_VERSION` with imports from `df_plugin_common`
- `rust/df_plugin_host/src/loader.rs` — replace local `parse_major` with import from `df_plugin_common`
- `rust/df_plugin_codeanatomy/src/lib.rs` — replace local `parse_major`, `schema_from_ipc`, `DELTA_SCAN_CONFIG_VERSION` with imports from `df_plugin_common`
- `rust/Cargo.toml` (workspace) — add `df_plugin_common` to workspace members
- `rust/datafusion_python/Cargo.toml` — add `df_plugin_common` dependency
- `rust/df_plugin_host/Cargo.toml` — add `df_plugin_common` dependency
- `rust/df_plugin_codeanatomy/Cargo.toml` — add `df_plugin_common` dependency

### New Files to Create

- `rust/df_plugin_common/Cargo.toml` — crate manifest
- `rust/df_plugin_common/src/lib.rs` — `parse_major`, `schema_from_ipc`, `DELTA_SCAN_CONFIG_VERSION`
- `rust/df_plugin_common/tests/parse_major_tests.rs` — unit tests for `parse_major`
- `rust/df_plugin_common/tests/schema_ipc_tests.rs` — unit tests for `schema_from_ipc`

### Legacy Decommission/Delete Scope

- Delete `parse_major` from `codeanatomy_ext.rs:503` (moved to `df_plugin_common`).
- Delete `parse_major` from `loader.rs:26` (moved to `df_plugin_common`).
- Delete `parse_major` from `df_plugin_codeanatomy/lib.rs:115` (moved to `df_plugin_common`).
- Delete `schema_from_ipc` from `codeanatomy_ext.rs:144` (moved to `df_plugin_common`).
- Delete `schema_from_ipc` from `df_plugin_codeanatomy/lib.rs:359` (moved to `df_plugin_common`).
- Delete `DELTA_SCAN_CONFIG_VERSION` constant from `codeanatomy_ext.rs:126` (moved to `df_plugin_common`).
- Delete `DELTA_SCAN_CONFIG_VERSION` constant from `df_plugin_codeanatomy/lib.rs:44` (moved to `df_plugin_common`).

---

## S6. UDF and Registry Consolidation

### Goal

Consolidate duplicated UDF utilities (`string_array_any`, `scalar_to_string`, `scalar_to_i64`) into `udf/common.rs`. Unify `FunctionKind` into a single definition. Rename the two `function_factory.rs` files. Consolidate `CollectSetUdaf`/`ListUniqueUdaf` shared accumulator. Extract `ArgBest` module from `udaf_builtin.rs`. Extract shared operator helpers. Use `ColumnarValue::to_array()` for mixed-shape paths while preserving scalar fast paths.

### Representative Code Snippets

```rust
// rust/datafusion_ext/src/udf/common.rs — unified scalar_to_i64 with timestamp support
pub(crate) fn scalar_to_i64(val: &ScalarValue) -> Result<i64> {
    match val {
        ScalarValue::Int8(Some(v)) => Ok(*v as i64),
        ScalarValue::Int16(Some(v)) => Ok(*v as i64),
        ScalarValue::Int32(Some(v)) => Ok(*v as i64),
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::UInt8(Some(v)) => Ok(*v as i64),
        ScalarValue::UInt16(Some(v)) => Ok(*v as i64),
        ScalarValue::UInt32(Some(v)) => Ok(*v as i64),
        ScalarValue::UInt64(Some(v)) => Ok(*v as i64),
        // Timestamp handling merged from udaf_builtin.rs
        ScalarValue::TimestampMicrosecond(Some(v), _) => Ok(*v),
        ScalarValue::TimestampNanosecond(Some(v), _) => Ok(*v),
        _ => Err(DataFusionError::Internal(
            format!("cannot convert {val:?} to i64"),
        )),
    }
}
```

```rust
// rust/datafusion_ext/src/function_types.rs — unified FunctionKind
/// Single authoritative definition for function kinds.
/// Previously divergent: registry_snapshot.rs (3 variants) vs function_factory.rs (4 variants).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionKind {
    Scalar,
    Aggregate,
    Window,
    Table,
}
```

```rust
// rust/datafusion_ext/src/udf/hash.rs — selective to_array normalization
// Preserve scalar/scalar fast path; normalize only mixed/array paths.
fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    match (&args.args[0], &args.args[1]) {
        (ColumnarValue::Scalar(prefix), ColumnarValue::Scalar(value)) => {
            return compute_prefixed_hash_scalar(prefix, value);
        }
        _ => {}
    }

    let num_rows = args.number_rows;
    let prefix_array = args.args[0].to_array(num_rows)?;
    let value_array = args.args[1].to_array(num_rows)?;
    Ok(ColumnarValue::Array(compute_prefixed_hash(&prefix_array, &value_array)?))
}
```

```rust
// rust/datafusion_ext/src/operator_utils.rs — shared helpers
/// Determine if an operator name represents an Arrow operator.
/// Single source of truth — previously duplicated in expr_planner.rs and function_rewrite.rs.
pub(crate) fn is_arrow_operator(op: &str) -> bool {
    matches!(op, "->" | "->>" | "#>" | "#>>")
}

pub(crate) fn can_use_get_field(op: &str) -> bool {
    matches!(op, "->" | "->>")
}
```

### Files to Edit

- `rust/datafusion_ext/src/udf/common.rs` — add unified `scalar_to_i64`, `scalar_to_string`, `string_array_any` (merge from `udaf_builtin.rs`)
- `rust/datafusion_ext/src/udaf_builtin.rs` — remove local `string_array_any`, `scalar_to_string`, `scalar_to_i64`, `string_signature`, `signature_with_names`; import from `udf::common`; consolidate `CollectSetUdaf` to delegate to `ListUniqueUdaf` accumulator internals
- `rust/datafusion_ext/src/function_factory.rs` → **rename to** `rust/datafusion_ext/src/sql_macro_factory.rs`; remove local `FunctionKind`
- `rust/datafusion_ext/src/udf/function_factory.rs` → **rename to** `rust/datafusion_ext/src/udf/primitives.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs` — remove local `FunctionKind`; import from `function_types.rs`
- `rust/datafusion_ext/src/expr_planner.rs` — remove local `is_arrow_operator`, `can_use_get_field`; import from `operator_utils.rs`
- `rust/datafusion_ext/src/function_rewrite.rs` — remove local `is_arrow_operator`, `can_use_get_field`; import from `operator_utils.rs`
- `rust/datafusion_ext/src/udf/hash.rs` — simplify mixed-shape 2-arg UDF branches using `ColumnarValue::to_array(num_rows)` while keeping scalar/scalar fast path
- `rust/datafusion_ext/src/udf/span.rs` — extract shared `span_pair_invoke` helper for `SpanOverlapsUdf` and `SpanContainsUdf`
- `rust/datafusion_ext/src/udf/mod.rs` — replace glob re-exports with explicit re-exports
- `rust/datafusion_ext/src/lib.rs` — update module declarations for renamed files; add `mod function_types; mod operator_utils;`

### New Files to Create

- `rust/datafusion_ext/src/function_types.rs` — unified `FunctionKind` definition
- `rust/datafusion_ext/src/operator_utils.rs` — shared `is_arrow_operator`, `can_use_get_field`
- `rust/datafusion_ext/src/udaf_arg_best.rs` — `ArgBestAccumulator`, `ArgBestMode`, `AnyValueDetUdaf`, `ArgMaxUdaf`, `ArgMinUdaf`, `AsofSelectUdaf` (extracted from `udaf_builtin.rs`)
- `rust/datafusion_ext/tests/function_types_tests.rs` — unit tests for `FunctionKind` serde/backward-compat contract
- `rust/datafusion_ext/tests/operator_utils_tests.rs` — unit tests
- `rust/datafusion_ext/tests/udaf_arg_best_tests.rs` — unit tests for extracted accumulator behavior parity

### Legacy Decommission/Delete Scope

- Delete `FunctionKind` enum from `registry_snapshot.rs:81-85` (replaced by import from `function_types.rs`).
- Delete `FunctionKind` enum from `function_factory.rs:34-40` (replaced by import from `function_types.rs`).
- Delete `string_array_any` from `udaf_builtin.rs:2213-2224` (moved to `udf/common.rs`).
- Delete `scalar_to_string` from `udaf_builtin.rs:708-716` (moved to `udf/common.rs`).
- Delete `scalar_to_i64` from `udaf_builtin.rs:718-757` (moved to `udf/common.rs`).
- Delete `is_arrow_operator` from `expr_planner.rs:44-52` (moved to `operator_utils.rs`).
- Delete `is_arrow_operator` from `function_rewrite.rs:61-67` (moved to `operator_utils.rs`).
- Delete `can_use_get_field` from both files (moved to `operator_utils.rs`).
- Delete mixed-shape branch duplication from 2-arg hash UDFs in `hash.rs` (replaced by selective `to_array` normalization with scalar fast path retained).
- Delete ~500 LOC of `ArgBest*` code from `udaf_builtin.rs` (moved to `udaf_arg_best.rs`).

---

## S7. Session and Compiler Utilities Consolidation

### Goal

Extract shared session capture utilities (`capture_df_settings`, `PLANNING_AFFECTING_CONFIG_KEYS`, `GovernancePolicy` enum) into a dedicated `session/capture.rs` module. Tighten `CpgRuleSet` visibility. Add `are_inputs_deterministic()` to `SemanticExecutionSpec`.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/session/capture.rs — shared session utilities

use std::collections::BTreeMap;
use datafusion::prelude::SessionContext;
use arrow::array::StringArray;

/// Config keys that affect planning surface identity.
/// Single source of truth — previously duplicated in planning_manifest.rs and factory.rs.
pub const PLANNING_AFFECTING_CONFIG_KEYS: &[&str] = &[
    "datafusion.execution.batch_size",
    "datafusion.execution.target_partitions",
    "datafusion.optimizer.enable_round_robin_repartition",
    "datafusion.optimizer.max_passes",
    // ... full list from planning_manifest.rs:332-349
];

/// Capture current DataFusion session settings as a sorted map.
/// Single source of truth — previously duplicated in envelope.rs:81-112
/// and planning_manifest.rs:351-387.
pub async fn capture_df_settings(
    ctx: &SessionContext,
) -> Result<BTreeMap<String, String>> {
    let df = ctx.sql("SELECT name, value FROM information_schema.df_settings").await?;
    let batches = df.collect().await?;
    let mut settings = BTreeMap::new();
    for batch in &batches {
        let names = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let values = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            if let (Some(name), Some(value)) = (names.value(i), values.value(i)) {
                settings.insert(name.to_string(), value.to_string());
            }
        }
    }
    Ok(settings)
}

/// Unified governance policy enum.
/// Previously triplicated as ExtensionGovernancePolicy, ExtensionGovernanceMode,
/// and PushdownEnforcementMode mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernancePolicy {
    Permissive,
    Advisory,
    Strict,
}
```

```rust
// rust/codeanatomy_engine/src/spec/execution_spec.rs — tell-don't-ask method
impl SemanticExecutionSpec {
    /// Whether all input relations are pinned to deterministic versions.
    pub fn are_inputs_deterministic(&self) -> bool {
        self.input_relations.iter().all(|r| r.version_pin.is_some())
    }
}
```

### Files to Edit

- `rust/codeanatomy_engine/src/session/envelope.rs` — remove inline SQL+parsing; import `capture_df_settings` from `session/capture`
- `rust/codeanatomy_engine/src/session/planning_manifest.rs` — remove inline SQL+parsing and `PLANNING_AFFECTING_CONFIG_KEYS`; import from `session/capture`
- `rust/codeanatomy_engine/src/session/factory.rs` — remove inline `planning_config_snapshot()`; import `PLANNING_AFFECTING_CONFIG_KEYS` from `session/capture`
- `rust/codeanatomy_engine/src/session/planning_surface.rs` — remove `ExtensionGovernancePolicy`; import `GovernancePolicy` from `session/capture`
- `rust/codeanatomy_engine/src/spec/runtime.rs` — remove `ExtensionGovernanceMode`; import and add `From` impl for `GovernancePolicy`
- `rust/codeanatomy_engine/src/compiler/compile_contract.rs` — use `spec.are_inputs_deterministic()` instead of inline iteration
- `rust/codeanatomy_engine/src/rules/registry.rs` — change `CpgRuleSet` fields from `pub` to `pub(crate)`; add accessor methods
- `rust/codeanatomy_engine/src/rules/rulepack.rs` — update to use accessor methods
- `rust/codeanatomy_engine/src/rules/overlay.rs` — update to use accessor methods
- `rust/codeanatomy_engine/src/session/mod.rs` — add `pub mod capture;`

### New Files to Create

- `rust/codeanatomy_engine/src/session/capture.rs` — `capture_df_settings`, `PLANNING_AFFECTING_CONFIG_KEYS`, `GovernancePolicy`
- `rust/codeanatomy_engine/tests/session_capture_tests.rs` — unit tests

### Legacy Decommission/Delete Scope

- Delete `PLANNING_AFFECTING_CONFIG_KEYS` from `planning_manifest.rs:332-349`.
- Delete `planning_config_snapshot()` inline logic from `factory.rs:360-399`.
- Delete SQL query + `StringArray` parsing from `envelope.rs:81-112`.
- Delete SQL query + `StringArray` parsing from `planning_manifest.rs:351-387`.
- Delete `ExtensionGovernancePolicy` enum from `planning_surface.rs:64-70`.
- Delete `ExtensionGovernanceMode` enum from `spec/runtime.rs:26-33`.

---

## S8. `codeanatomy_ext.rs` Decomposition

### Goal

Decompose the 3,582 LOC `codeanatomy_ext.rs` monolith into 7 focused submodules, each with a single responsibility. Replace the `session_context_contract(ctx)?.ctx` 56-occurrence chain with a direct `extract_session_ctx()` helper. Split public and internal module initialization paths explicitly. Remove Python positional fan-out wrappers in the same scope that introduces typed request entrypoints.

### Representative Code Snippets

```rust
// rust/datafusion_python/src/codeanatomy_ext/mod.rs — thin orchestration module

mod cache_tables;
mod delta_maintenance;
mod delta_mutations;
mod delta_provider;
mod helpers;
mod plugin_bridge;
mod session_utils;
mod udf_registration;

pub use cache_tables::*;
pub use delta_maintenance::*;
pub use delta_mutations::*;
pub use delta_provider::*;
pub use plugin_bridge::*;
pub use session_utils::*;
pub use udf_registration::*;

/// Initialize the CodeAnatomy extension module.
pub fn init_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    delta_provider::register_functions(parent)?;
    delta_mutations::register_functions(parent)?;
    delta_maintenance::register_functions(parent)?;
    plugin_bridge::register_functions(parent)?;
    udf_registration::register_functions(parent)?;
    cache_tables::register_functions(parent)?;
    session_utils::register_functions(parent)?;
    Ok(())
}

/// Initialize internal-only entry points.
pub fn init_internal_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    plugin_bridge::register_internal_functions(parent)?;
    session_utils::register_internal_functions(parent)?;
    Ok(())
}
```

```rust
// rust/datafusion_python/src/codeanatomy_ext/helpers.rs — shared helpers

use datafusion::prelude::SessionContext;
use pyo3::prelude::*;

/// Extract the DataFusion SessionContext from a Python session object.
/// Replaces the `session_context_contract(ctx)?.ctx` pattern (56 occurrences).
pub(crate) fn extract_session_ctx(ctx: &Bound<'_, PyAny>) -> PyResult<SessionContext> {
    if let Ok(session) = ctx.extract::<Bound<'_, PySessionContext>>() {
        return Ok(session.borrow().ctx().clone());
    }
    // Fallback: extract via .ctx attribute
    let inner = ctx.getattr("ctx")?;
    let session: Bound<'_, PySessionContext> = inner.extract()?;
    Ok(session.borrow().ctx().clone())
}

/// Convert a serde_json Value to a Python object.
pub(crate) fn json_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<PyObject> {
    // ... existing implementation from codeanatomy_ext.rs:328 ...
}

/// Get storage options from a Python dict.
pub(crate) fn storage_options_map(
    opts: Option<&Bound<'_, PyDict>>,
) -> HashMap<String, String> {
    // ... existing implementation from codeanatomy_ext.rs:130 ...
}
```

```rust
// rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs — focused module

use super::helpers::{extract_session_ctx, json_to_py, storage_options_map};

/// Parameter group for Delta protocol gating.
pub(crate) struct DeltaGateParams {
    pub min_reader_version: Option<i32>,
    pub min_writer_version: Option<i32>,
    pub required_reader_features: Option<Vec<String>>,
    pub required_writer_features: Option<Vec<String>>,
}

/// Parameter group for Delta commit options.
pub(crate) struct DeltaCommitParams {
    pub commit_metadata: Option<HashMap<String, String>>,
    pub app_id: Option<String>,
    pub app_version: Option<i64>,
    pub app_last_updated: Option<i64>,
    pub max_retries: Option<i64>,
    pub create_checkpoint: Option<bool>,
}

/// Parameter group for Delta table location.
pub(crate) struct DeltaTableLocator {
    pub table_uri: String,
    pub storage_options: HashMap<String, String>,
    pub version: Option<i64>,
    pub timestamp: Option<String>,
}

pub(crate) fn register_functions(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(delta_write_ipc, parent)?)?;
    parent.add_function(wrap_pyfunction!(delta_delete, parent)?)?;
    parent.add_function(wrap_pyfunction!(delta_update, parent)?)?;
    parent.add_function(wrap_pyfunction!(delta_merge, parent)?)?;
    Ok(())
}
```

### Files to Edit

- `rust/datafusion_python/src/codeanatomy_ext.rs` → **convert to** `rust/datafusion_python/src/codeanatomy_ext/mod.rs` (thin orchestrator)
- `rust/datafusion_python/src/context.rs` — change `pub ctx` to `pub(crate) ctx`; add `pub(crate) fn ctx(&self) -> &SessionContext` accessor
- `rust/datafusion_python/src/lib.rs` — update module declaration if needed
- `src/datafusion_engine/delta/control_plane_mutation.py` — replace positional Rust entrypoint fan-out calls with typed request payload calls
- `src/datafusion_engine/delta/control_plane_maintenance.py` — replace positional Rust entrypoint fan-out calls with typed request payload calls

### New Files to Create

- `rust/datafusion_python/src/codeanatomy_ext/mod.rs` — module orchestrator with `init_module()`/`init_internal_module()`
- `rust/datafusion_python/src/codeanatomy_ext/helpers.rs` — `extract_session_ctx`, `json_to_py`, `storage_options_map`, `now_unix_ms`
- `rust/datafusion_python/src/codeanatomy_ext/delta_provider.rs` — provider construction, scan config (~250 LOC)
- `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs` — write, merge, delete, update operations + `DeltaGateParams`, `DeltaCommitParams`, `DeltaTableLocator`
- `rust/datafusion_python/src/codeanatomy_ext/delta_maintenance.rs` — vacuum, optimize, restore, checkpoint, properties, constraints
- `rust/datafusion_python/src/codeanatomy_ext/plugin_bridge.rs` — plugin loading, registration, manifest validation
- `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs` — UDF/function factory installation, config, snapshots
- `rust/datafusion_python/src/codeanatomy_ext/cache_tables.rs` — cache table functions and metrics
- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs` — runtime, extraction session, capabilities snapshot
- `rust/datafusion_python/tests/codeanatomy_ext_decomposition_tests.rs` — smoke tests verifying all functions still register
- `rust/datafusion_python/tests/codeanatomy_ext_delta_provider_tests.rs` — module-level tests for provider/scanning bridge
- `rust/datafusion_python/tests/codeanatomy_ext_delta_mutations_tests.rs` — module-level tests for write/delete/update/merge bridge
- `rust/datafusion_python/tests/codeanatomy_ext_delta_maintenance_tests.rs` — module-level tests for maintenance bridge
- `rust/datafusion_python/tests/codeanatomy_ext_helpers_tests.rs` — module-level tests for context/JSON/storage helpers
- `rust/datafusion_python/tests/codeanatomy_ext_plugin_bridge_tests.rs` — module-level tests for plugin loading/manifest checks
- `rust/datafusion_python/tests/codeanatomy_ext_udf_registration_tests.rs` — module-level tests for UDF registration/config surfaces
- `rust/datafusion_python/tests/codeanatomy_ext_cache_tables_tests.rs` — module-level tests for cache-table registration and metrics payloads
- `rust/datafusion_python/tests/codeanatomy_ext_session_utils_tests.rs` — module-level tests for runtime/session utility surfaces
- `tests/unit/datafusion_engine/delta/test_control_plane_request_bridge.py` — unit tests for request-typed Python→Rust bridge payloads

### Legacy Decommission/Delete Scope

- Delete `rust/datafusion_python/src/codeanatomy_ext.rs` as a single file (replaced by `codeanatomy_ext/` module directory).
- Delete the 56 `session_context_contract(ctx)?.ctx` chains (replaced by `extract_session_ctx()` calls).
- Delete positional argument fan-out in `control_plane_mutation.py` and `control_plane_maintenance.py` once typed request entrypoints are live.

---

## S9. Rule Instrumentation Consolidation (DataFusion 51-Compatible)

### Goal

Keep DataFusion pinned at `51.x` and consolidate the 815 LOC hand-rolled sentinel/wrapper/wiring implementation in `rule_instrumentation.rs` using internal shared helpers/macros. Target: preserve current semantics (`Disabled` / `PhaseOnly` / `Full`, plan diff behavior, effective rule tracking) while removing triplicated wiring.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/executor/tracing/rule_instrumentation.rs — shared span creators

pub fn instrument_session_state(state: SessionState, context: &PlanningContext) -> SessionState {
    let options = options_from_tracing_config(&context.tracing);
    if options.is_disabled() {
        return state;
    }
    let (rule_span_create_fn, phase_span_create_fn) = make_span_creators(context);
    apply_phase_instrumentation(state, &options, rule_span_create_fn, phase_span_create_fn)
}
```

```rust
// rust/codeanatomy_engine/src/executor/tracing/rule_instrumentation.rs — macro to eliminate phase triplication
macro_rules! instrument_phase_rules {
    ($fn_name:ident, $rule_trait:ty, $sentinel_ty:ty, $wrapper_ty:ty) => {
        fn $fn_name(
            rules: Vec<Arc<$rule_trait>>,
            options: &RuleInstrumentationOptions,
            span_create_fn: Arc<dyn Fn(&str) -> Span + Send + Sync>,
            phase_span_create_fn: Arc<dyn Fn(&str) -> Span + Send + Sync>,
        ) -> Vec<Arc<$rule_trait>> {
            // Shared sentinel + wrapper + wiring path reused for each rule phase.
            // Existing behavior is preserved; only assembly duplication is removed.
            /* ... */
        }
    };
}
```

This keeps the DataFusion 51 implementation strategy but removes most maintenance-heavy duplication in span/phase assembly and rule wrapping.

### Files to Edit

- `rust/codeanatomy_engine/src/executor/tracing/rule_instrumentation.rs` — replace phase-specific duplicated assembly with shared helper/macro paths while retaining DataFusion 51 behavior
- `rust/codeanatomy_engine/src/executor/tracing/exec_instrumentation.rs` — keep execution instrumentation parity and shared field naming conventions
- `rust/codeanatomy_engine/src/executor/tracing/context.rs` — keep trace field contracts consistent after helper extraction

### New Files to Create

- `rust/codeanatomy_engine/tests/rule_instrumentation_consolidation_tests.rs` — integration tests verifying span creation and no-regression behavior after consolidation
- `rust/codeanatomy_engine/tests/rule_instrumentation_phase_mode_tests.rs` — tests for `Disabled`/`PhaseOnly`/`Full` parity against previous semantics

### Legacy Decommission/Delete Scope

- Delete duplicated span creation closures from `rule_instrumentation.rs:746-789` (replace with shared creator helpers).
- Delete repeated per-phase assembly/wiring branches across analyzer/optimizer/physical paths (replace with shared helper/macro-based assembly).
- Delete redundant per-phase tracing field population blocks (replace with one centralized span field contract helper).

---

## S10. Interface Type Safety

### Goal

Introduce `TableVersion` enum to replace `(Option<i64>, Option<String>)` mutual-exclusion pairs across all delta request structs. Add version field to `RegistrySnapshot`. Extract pure domain logic from PyO3 `compiler.rs`.

### Representative Code Snippets

```rust
// rust/datafusion_ext/src/delta_protocol.rs — TableVersion enum

/// Enum representing a delta table version target.
/// Replaces (Option<i64>, Option<String>) pairs in 8+ request structs.
/// Makes the "both specified" illegal state unrepresentable at the type level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableVersion {
    /// Use the latest version (default).
    Latest,
    /// Pin to a specific version number.
    Version(i64),
    /// Pin to a specific timestamp.
    Timestamp(String),
}

impl TableVersion {
    /// Parse from optional version/timestamp pair.
    /// Returns error if both are specified.
    pub fn from_options(version: Option<i64>, timestamp: Option<&str>) -> Result<Self, DeltaTableError> {
        match (version, timestamp) {
            (Some(_), Some(_)) => Err(DeltaTableError::Generic(
                "cannot specify both version and timestamp".into(),
            )),
            (Some(v), None) => Ok(Self::Version(v)),
            (None, Some(t)) => Ok(Self::Timestamp(t.to_string())),
            (None, None) => Ok(Self::Latest),
        }
    }
}
```

```rust
// rust/datafusion_ext/src/registry_snapshot.rs — versioned snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrySnapshot {
    /// Schema version for evolution detection.
    pub version: u32,
    // ... existing fields ...
}

impl RegistrySnapshot {
    pub const CURRENT_VERSION: u32 = 1;
}
```

### Files to Edit

- `rust/datafusion_ext/src/delta_protocol.rs` — add `TableVersion` enum
- `rust/datafusion_ext/src/delta_mutations.rs` — update all request structs to use `TableVersion`
- `rust/datafusion_ext/src/delta_maintenance.rs` — update all request structs to use `TableVersion`
- `rust/datafusion_ext/src/delta_control_plane.rs` — update `delta_table_builder` to accept `TableVersion`
- `rust/datafusion_ext/src/registry_snapshot.rs` — add `version: u32` field
- `rust/codeanatomy_engine_py/src/compiler.rs` — extract pure domain logic (`canonical_rulepack_profile`, `map_join_type`, `cpg_output_kind_for_view`, `default_rule_intents`, `build_transform`) into `codeanatomy_engine` core crate

### New Files to Create

- `rust/codeanatomy_engine/src/compiler/spec_helpers.rs` — pure domain logic extracted from `compiler.rs` PyO3 layer
- `rust/codeanatomy_engine/tests/spec_helpers_tests.rs` — unit tests for extracted functions

### Legacy Decommission/Delete Scope

- Delete `(version: Option<i64>, timestamp: Option<String>)` patterns from all delta request structs (replaced by `TableVersion`).
- Delete runtime mutual-exclusion checks like `if version.is_some() && timestamp.is_some()` from delta operation functions (enforced by type system).
- Delete `canonical_rulepack_profile`, `map_join_type`, `cpg_output_kind_for_view`, `default_rule_intents`, `build_transform` from `codeanatomy_engine_py/src/compiler.rs` (moved to `codeanatomy_engine` core).

---

## S11. Observability Infrastructure

### Goal

Add tracing spans to all public delta operation functions, compile_request phases, and key PyO3 entry points. Extract scan operator classification. Decompose `execute_pipeline` into named phase helpers with tracing spans.

### Representative Code Snippets

```rust
// rust/datafusion_ext/src/delta_mutations.rs — tracing on delta ops
use tracing::instrument;

#[instrument(
    skip(ctx, record_batches, storage_options),
    fields(table_uri = %request.table_uri, operation = "write")
)]
pub async fn delta_write_batches_request(
    ctx: &SessionContext,
    request: DeltaWriteBatchesRequest,
    record_batches: Vec<RecordBatch>,
    storage_options: HashMap<String, String>,
) -> Result<DeltaOperationReport, DeltaTableError> {
    // ... existing implementation ...
}
```

```rust
// rust/codeanatomy_engine/src/executor/metrics_collector.rs — extracted classifier
/// Single source of truth for scan operator detection.
/// Previously: inline string matching at metrics_collector.rs:151.
pub(crate) fn is_scan_operator(name: &str) -> bool {
    name.contains("Scan") || name.contains("Parquet") || name.contains("Delta")
}
```

```rust
// rust/codeanatomy_engine/src/executor/pipeline.rs — decomposed phases
use tracing::instrument;

#[instrument(skip_all, fields(spec_hash = %request.spec_hash()))]
pub async fn execute_pipeline(/* ... */) -> Result<RunResult> {
    let task_graph = build_task_graph_and_costs(&spec, &ctx)?;
    let pushdown_map = probe_pushdown_contracts(&ctx, &spec).await?;
    let bundles = capture_plan_bundles(&ctx, &spec, &task_graph).await?;
    let (results, plans) = materialize_outputs(&ctx, output_plans, lineage).await?;
    let metrics = aggregate_physical_metrics(&plans);
    let maintenance = execute_maintenance(&ctx, &schedule).await?;
    assemble_result(results, metrics, bundles, maintenance)
}

#[instrument(skip_all)]
fn build_task_graph_and_costs(spec: &SemanticExecutionSpec, ctx: &SessionContext) -> Result<TaskGraph> {
    // ... extracted from pipeline.rs:124-157 ...
}

#[instrument(skip_all)]
async fn probe_pushdown_contracts(ctx: &SessionContext, spec: &SemanticExecutionSpec) -> Result<PushdownMap> {
    // ... extracted from pipeline.rs:159-179 ...
}
```

### Files to Edit

- `rust/datafusion_ext/src/delta_mutations.rs` — add `#[instrument]` to all public async functions
- `rust/datafusion_ext/src/delta_maintenance.rs` — add `#[instrument]` to all public async functions
- `rust/datafusion_ext/src/delta_control_plane.rs` — add `#[instrument]` to `load_delta_table`
- `rust/codeanatomy_engine/src/executor/metrics_collector.rs` — extract `is_scan_operator()` helper (line 151)
- `rust/codeanatomy_engine/src/executor/pipeline.rs` — decompose `execute_pipeline` into named phase helpers with `#[instrument]` (lines 105-366)
- `rust/codeanatomy_engine/src/compiler/compile_contract.rs` — add `#[instrument]` spans to `compile_request` phases
- `rust/datafusion_python/src/codeanatomy_ext.rs` — add `#[instrument]` to key entry points (moves to `codeanatomy_ext/` module files after S8 decomposition)
- `rust/codeanatomy_engine_py/src/materializer.rs` — add `#[instrument]` to `execute()`

### New Files to Create

None (all changes are to existing files).

### Legacy Decommission/Delete Scope

- Delete inline string matching `name.contains("Scan") || name.contains("Parquet") || name.contains("Delta")` from `metrics_collector.rs:151` (replaced by `is_scan_operator()` call).

---

## S12. `registry_snapshot.rs` Decomposition and Metadata Authority

### Goal

Decompose `registry_snapshot.rs` (1,053 LOC) into focused modules for snapshot assembly, hook capabilities, and UDF-provided metadata. Eliminate centralized hardcoded capability/rewrite lookup tables in favor of metadata exported by each UDF/UDAF/UDWF module. Keep a single versioned snapshot contract for Python consumers.

### Representative Code Snippets

```rust
// rust/datafusion_ext/src/registry/metadata.rs — per-function metadata contract
#[derive(Debug, Clone)]
pub struct FunctionMetadata {
    pub name: &'static str,
    pub kind: FunctionKind,
    pub rewrite_tags: &'static [&'static str],
    pub has_simplify: bool,
    pub has_coerce_types: bool,
    pub has_short_circuits: bool,
    pub has_groups_accumulator: bool,
    pub has_retract_batch: bool,
    pub has_reverse_expr: bool,
    pub has_sort_options: bool,
}

pub trait MetadataProvider {
    fn metadata(&self) -> FunctionMetadata;
}
```

```rust
// rust/datafusion_ext/src/registry/snapshot.rs — snapshot assembly only
pub fn snapshot_hook_capabilities(state: &SessionState) -> Vec<FunctionHookCapabilities> {
    let providers = udf_registry::metadata_providers(state);
    providers
        .into_iter()
        .map(|provider| {
            let m = provider.metadata();
            FunctionHookCapabilities {
                name: m.name.to_string(),
                kind: m.kind,
                has_simplify: m.has_simplify,
                has_coerce_types: m.has_coerce_types,
                has_short_circuits: m.has_short_circuits,
                has_propagate_constraints: false,
                has_groups_accumulator: m.has_groups_accumulator,
                has_retract_batch: m.has_retract_batch,
                has_reverse_expr: m.has_reverse_expr,
                has_sort_options: m.has_sort_options,
            }
        })
        .collect()
}
```

```rust
// rust/datafusion_ext/src/registry/snapshot_types.rs — encapsulated snapshot contract
#[derive(Debug, Clone, Serialize)]
pub struct RegistrySnapshot {
    version: u32,
    scalar: Vec<String>,
    aggregate: Vec<String>,
    // ... remaining fields private ...
}

impl RegistrySnapshot {
    pub const CURRENT_VERSION: u32 = 1;
    pub fn version(&self) -> u32 {
        self.version
    }
    pub fn scalar(&self) -> &[String] {
        &self.scalar
    }
}
```

### Files to Edit

- `rust/datafusion_ext/src/registry_snapshot.rs` — split responsibilities into dedicated modules; keep thin facade entrypoint
- `rust/datafusion_ext/src/udf_registry.rs` — expose metadata-provider registration/collection
- `rust/datafusion_ext/src/udf/mod.rs` — register scalar metadata providers
- `rust/datafusion_ext/src/udaf_builtin.rs` — export aggregate metadata
- `rust/datafusion_ext/src/udwf_builtin.rs` — export window metadata
- `rust/datafusion_ext/src/lib.rs` — add `registry` module declarations/re-exports
- `src/datafusion_engine/registry_facade.py` — consume versioned snapshot contract explicitly

### New Files to Create

- `rust/datafusion_ext/src/registry/snapshot.rs` — snapshot assembly + serialization
- `rust/datafusion_ext/src/registry/snapshot_types.rs` — `RegistrySnapshot`, `FunctionHookCapabilities`, `FunctionKind`
- `rust/datafusion_ext/src/registry/metadata.rs` — metadata traits/structs
- `rust/datafusion_ext/src/registry/capabilities.rs` — hook capability projection helpers
- `rust/datafusion_ext/tests/registry_metadata_tests.rs` — metadata provider coverage tests
- `rust/datafusion_ext/tests/registry_capabilities_tests.rs` — capability projection tests
- `rust/datafusion_ext/tests/registry_snapshot_tests.rs` — deterministic snapshot and capability coverage tests
- `tests/unit/datafusion_engine/delta/test_registry_snapshot_contract.py` — Python-side contract tests for versioned snapshot parsing

### Legacy Decommission/Delete Scope

- Delete `known_aggregate_capabilities` and `known_window_capabilities` hardcoded tables from `registry_snapshot.rs`.
- Delete centralized `rewrite_tags_for` and `simplify_flag_for` lookup tables from `registry_snapshot.rs` (metadata comes from function modules).
- Delete direct `pub` mutability for `RegistrySnapshot` fields (replace with constructor + read-only accessors).
- Delete duplicated `FunctionKind` authority in `registry_snapshot.rs` once `function_types.rs`/snapshot types are authoritative.

---

## S13. Cross-Language Determinism Contract and Golden Tests

### Goal

Promote the three-layer identity contract (`spec_hash`, `envelope_hash`, `rulepack_fingerprint`) to an explicit shared specification with Rust and Python golden tests over the same fixtures. Make nondeterministic fields explicit opt-outs, not implicit behavior.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/tests/session_identity_golden.rs
#[test]
fn session_identity_matches_golden_fixture() {
    let fixture = load_fixture("tests/msgspec_contract/goldens/session_identity_contract.json");
    let computed = compute_identity_contract(&fixture.spec, &fixture.session_inputs);
    assert_eq!(computed.spec_hash_hex, fixture.expected.spec_hash_hex);
    assert_eq!(computed.envelope_hash_hex, fixture.expected.envelope_hash_hex);
    assert_eq!(computed.rulepack_fingerprint_hex, fixture.expected.rulepack_fingerprint_hex);
}
```

```python
# tests/unit/datafusion_engine/session/test_identity_contract_golden.py
def test_identity_contract_matches_golden_fixture() -> None:
    fixture = _load_fixture("tests/msgspec_contract/goldens/session_identity_contract.json")
    computed = compute_identity_contract(
        spec_payload=fixture["spec"],
        session_inputs=fixture["session_inputs"],
    )
    assert computed["spec_hash_hex"] == fixture["expected"]["spec_hash_hex"]
    assert computed["envelope_hash_hex"] == fixture["expected"]["envelope_hash_hex"]
    assert computed["rulepack_fingerprint_hex"] == fixture["expected"]["rulepack_fingerprint_hex"]
```

### Files to Edit

- `rust/codeanatomy_engine/src/session/envelope.rs` — expose stable test helper for contract hash computation
- `rust/codeanatomy_engine/src/executor/result.rs` — document three-layer contract fields with reference to shared spec
- `src/datafusion_engine/session/runtime_session.py` — align Python contract helper naming with Rust spec
- `docs/architecture/05_rust_engine.md` — replace duplicated contract narrative with link to dedicated contract spec

### New Files to Create

- `docs/architecture/07_determinism_contract.md` — normative cross-language identity contract spec
- `tests/msgspec_contract/goldens/session_identity_contract.json` — shared fixture with expected hashes
- `rust/codeanatomy_engine/tests/session_identity_golden.rs` — Rust golden verification
- `tests/unit/datafusion_engine/session/test_identity_contract_golden.py` — Python golden verification

### Legacy Decommission/Delete Scope

- Delete duplicate/partial determinism contract prose from `docs/architecture/05_rust_engine.md` once `07_determinism_contract.md` is authoritative.
- Delete ad hoc cross-language hash parity assertions in `rust/codeanatomy_engine/tests/session_determinism.rs` and `tests/unit/datafusion_engine/session/test_session_identity.py` that are superseded by shared golden-fixture tests.

---

## S14. Async UDF Policy Migration to `ConfigExtension`

### Goal

Replace global mutable async UDF policy state (`OnceLock<RwLock<AsyncUdfPolicy>>`) with DataFusion `ConfigExtension`-backed session configuration. Keep policy deterministic, session-scoped, and visible through existing config surfaces.

### Representative Code Snippets

```rust
// rust/datafusion_ext/src/async_udf_config.rs
use datafusion_common::config::ConfigExtension;

const PREFIX: &str = "codeanatomy_async_udf";

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct CodeAnatomyAsyncUdfConfig {
    pub ideal_batch_size: Option<usize>,
    pub timeout_ms: Option<u64>,
}

crate::impl_extension_options!(
    CodeAnatomyAsyncUdfConfig,
    prefix = PREFIX,
    unknown_key = "Unknown async UDF config key: {key}",
    fields = [
        (ideal_batch_size, Option<usize>, "Preferred batch size for async UDF execution."),
        (timeout_ms, Option<u64>, "Async UDF timeout in milliseconds.")
    ]
);

impl ConfigExtension for CodeAnatomyAsyncUdfConfig {
    const PREFIX: &'static str = PREFIX;
}
```

```rust
// rust/datafusion_ext/src/udf_async.rs — policy from session config
fn async_udf_policy(config: &ConfigOptions) -> AsyncUdfPolicy {
    let ext = config
        .extensions
        .get::<CodeAnatomyAsyncUdfConfig>()
        .cloned()
        .unwrap_or_default();
    AsyncUdfPolicy {
        ideal_batch_size: ext.ideal_batch_size,
        timeout: ext.timeout_ms.map(Duration::from_millis),
    }
}
```

### Files to Edit

- `rust/datafusion_ext/src/udf_async.rs` — remove global policy lock; derive policy from `ConfigOptions`/session state
- `rust/datafusion_ext/src/lib.rs` — register/export async UDF config extension
- `rust/datafusion_python/src/codeanatomy_ext.rs` — set async policy via config extension instead of global setter (moves to `codeanatomy_ext/udf_registration.rs` after S8 lands)

### New Files to Create

- `rust/datafusion_ext/src/async_udf_config.rs` — async policy config extension
- `rust/datafusion_ext/tests/async_udf_config_tests.rs` — policy extraction and defaults tests
- `tests/unit/datafusion_engine/udf/test_async_udf_config_bridge.py` — Python bridge tests for async policy config wiring

### Legacy Decommission/Delete Scope

- Delete `ASYNC_UDF_POLICY` global static and `async_udf_policy_lock()` from `udf_async.rs`.
- Delete global mutation API `set_async_udf_policy(...)` in favor of session config extension updates.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S4, S5)

- Delete all cross-crate duplicate constants and functions superseded by `delta_common.rs` and `df_plugin_common`:
  - `parse_major` from 3 locations (after S5)
  - `schema_from_ipc` from 2 locations (after S5)
  - `DELTA_SCAN_CONFIG_VERSION` from 2 locations (after S5)
  - `latest_operation_metrics` from 2 locations (after S4)
  - `snapshot_with_gate` from 2 locations (after S4)
  - `parse_rfc3339` variants from 2 locations (after S4)

### Batch D2 (after S6, S7, S12, S14)

- Delete all within-crate duplicated functions/types and global policy state:
  - `FunctionKind` duplicate authorities from `function_factory.rs` and legacy `registry_snapshot.rs` paths (after S6 + S12)
  - `is_arrow_operator` duplicates from `expr_planner.rs` and `function_rewrite.rs` (after S6)
  - `string_array_any`, `scalar_to_string`, `scalar_to_i64` from `udaf_builtin.rs` (after S6)
  - `ExtensionGovernancePolicy` and `ExtensionGovernanceMode` from 2 locations (after S7)
  - `PLANNING_AFFECTING_CONFIG_KEYS` and `capture_df_settings` inline duplicates from 2 locations each (after S7)
  - `known_aggregate_capabilities`, `known_window_capabilities`, `rewrite_tags_for`, `simplify_flag_for` centralized lookup tables (after S12)
  - `ASYNC_UDF_POLICY`/`async_udf_policy_lock()` global state and `set_async_udf_policy(...)` global mutation API (after S14)

### Batch D3 (after S8, S9)

- Delete `codeanatomy_ext.rs` as a single file (after S8 decomposes it into modules).
- Delete duplicated sentinel/wrapper/wiring assembly blocks from `rule_instrumentation.rs` (after S9 DataFusion 51-compatible consolidation lands).

### Batch D4 (after S4, S8, S10)

- Delete all deprecated positional-argument shim functions (~450 LOC across `delta_mutations.rs` and `delta_maintenance.rs`) — included in S4 and completed with S8 Python bridge cutover.
- Delete positional fan-out wrappers in `control_plane_mutation.py` and `control_plane_maintenance.py` once typed request entrypoints are the only bridge path.
- Delete all `(Option<i64>, Option<String>)` version/timestamp pairs (after S10 introduces `TableVersion` enum).

### Batch D5 (after S13)

- Delete duplicate/partial determinism-contract prose in architecture docs superseded by `docs/architecture/07_determinism_contract.md`.
- Delete ad hoc cross-language hash parity checks in `rust/codeanatomy_engine/tests/session_determinism.rs` and `tests/unit/datafusion_engine/session/test_session_identity.py` superseded by shared golden-fixture tests.

---

## Implementation Sequence

1. **S1 — UDF Error Message and Contract Fixes.** Zero-risk, immediate value. Fixes user-facing confusion. No dependencies.

2. **S2 — Runner and Pipeline DRY.** Small, self-contained changes within `codeanatomy_engine`. The runner delegation is a 10-minute change.

3. **S3 — Schema Hashing and Determinism Chain.** Fixes the most critical correctness concern (divergent schema identity). Depends on no other scope items.

4. **S9 — Rule Instrumentation Consolidation (DataFusion 51-compatible).** Internal deduplication/refactor with no version migration; can proceed independently after S2.

5. **S5 — Cross-Crate Shared Knowledge.** Creates `df_plugin_common` crate and removes high-risk cross-crate duplication.

6. **S4 — Delta Common Module + Builder Adoption.** Extracts shared delta helpers and normalizes scan/provider construction on Delta builders.

7. **S6 — UDF and Registry Consolidation.** Largest `datafusion_ext` refactor; now safer after S4 foundations are in place.

8. **S10 — Interface Type Safety.** Introduces `TableVersion` enum and explicit snapshot versioning; unblocks typed bridge hard-cutover.

9. **S8 — `codeanatomy_ext.rs` Decomposition.** Depends on S5 and S10; includes Python bridge positional fan-out removal.

10. **S7 — Session and Compiler Utilities.** Parallel-safe with S6/S8 except for light touch points; lands before observability fan-out.

11. **S12 — `registry_snapshot.rs` Decomposition.** Depends on S6 + S10 for unified function/snapshot type authority.

12. **S14 — Async UDF Policy via `ConfigExtension`.** Integrates with S8/S11 UDF registration flow; no DataFusion major-version dependency.

13. **S13 — Determinism Contract + Goldens.** Best landed after S3/S7 so the contract captures final hashing behavior.

14. **S11 — Observability Infrastructure.** Final pass once decomposition and instrumentation surfaces are stable.

**Parallelization opportunities:**
- S1, S2, S3 can all proceed in parallel (no dependencies).
- S4 and S5 can proceed in parallel.
- S6 and S7 can proceed in parallel once S4 is complete.
- S9 can proceed in parallel with S6/S7 once S2 lands.
- S12 and S14 can proceed in parallel once their prerequisites are complete.
- S13 can proceed in parallel with late-stage S11 instrumentation cleanup.

---

## Implementation Checklist

- [x] S1. UDF Error Message and Contract Fixes
- [x] S2. Runner and Pipeline DRY Consolidation
- [x] S3. Schema Hashing and Determinism Chain Consolidation
- [x] S4. Delta Common Module Extraction
- [x] S5. Cross-Crate Shared Knowledge (`df_plugin_common`)
- [x] S6. UDF and Registry Consolidation
- [x] S7. Session and Compiler Utilities Consolidation
- [x] S8. `codeanatomy_ext.rs` Decomposition
- [x] S9. Rule Instrumentation Consolidation (DataFusion 51-compatible)
- [x] S10. Interface Type Safety
- [x] S11. Observability Infrastructure
- [x] S12. `registry_snapshot.rs` Decomposition and Metadata Authority
- [x] S13. Cross-Language Determinism Contract and Golden Tests
- [x] S14. Async UDF Policy Migration to `ConfigExtension`
- [x] D1. Cross-crate decommission batch (after S4, S5)
- [x] D2. Within-crate decommission batch (after S6, S7, S12, S14)
- [x] D3. Monolith decommission batch (after S8, S9)
- [x] D4. Bridge/type hard-cutover decommission (after S4, S8, S10)
- [x] D5. Determinism contract decommission batch (after S13)
