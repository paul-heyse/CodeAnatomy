# Rust Workspace Design Improvements v2 Implementation Plan v1 (2026-02-17)

## Scope Summary

This plan updates the previously drafted v2 scope with corrections and expansions from the five `rust_design_review_*` documents plus additional `dfdl_ref`-grounded capability opportunities. It keeps the original objective (aggressive design-phase migration) while fixing incorrect assumptions, removing shim-based transitions, and adding missing high-impact workstreams.

The scope now covers 17 implementation items across engine compilation/execution, `datafusion_ext`, and `datafusion_python`, with hard-cutover semantics:
- No compatibility shims for internal Rust APIs.
- Full boundary contract redesign now (not incremental patching).
- Immediate replacement of stubbed functionality where production-capable library primitives already exist.
- Decompose/relocate ownership physically (not thin re-export facades).

## Implementation Progress Audit (2026-02-17)

Legend:
- `Complete`: Scope and decommission targets are implemented in-tree.
- `Partial`: Core implementation landed, but at least one planned outcome is not fully realized.
- `Remaining`: Not yet implemented.

| Scope | Status | Audit Notes |
| --- | --- | --- |
| S1 | Complete | All listed correctness defects are fixed (`Union` label, `todo!()` removal, duplicate `PyLiteral` registration removal, physical-plan decode message, `stable_hash128` argument validation string). |
| S2 | Complete | `plan_utils.rs` is authoritative and consumed across listed compiler modules; duplicated helper definitions were removed. |
| S3 | Complete | Canonical `PushdownEnforcementMode` lives in `contracts/pushdown_mode.rs`; duplicate enums and conversion shims were removed. |
| S4 | Complete | `WriteOutcome` is the typed enum contract and `MaterializationResult` now carries nested `write_outcome`; legacy nullable write metadata fields were removed. |
| S5 | Complete | `span_struct_type` duplication removed and snapshot payload naming disambiguated to `snapshot_info_as_strings` / `snapshot_info_as_values`. |
| S6 | Complete | `RegistrySnapshot` is encapsulated, metadata sentinel `name: \"\"` is removed (`name: None`), and `datafusion_ext` visibility matches external-consumer reality. |
| S7 | Complete | Shared `codeanatomy_ext` helpers are centralized and consumed by decomposed modules; legacy helper forwarding paths are removed. |
| S8 | Complete | `legacy.rs` is deleted; decomposed ownership modules and associated tests are present. |
| S9 | Complete | Error helper authority is consolidated in `errors.rs` including `py_value_err`; `sql/exceptions.rs` is removed. |
| S10 | Complete | Logical wrappers are cut over to `logical_plan_wrapper!` across `filter/projection/limit/create_view/drop_view/join/union/sort`, removing duplicated manual wrapper boilerplate. |
| S11 | Complete | Robustness cleanup landed: non-panicking dataset schema fallback, `to_variant()` coverage for scalar/window function variants, dead `signature.rs` removal, and `RexType::Other` removal. |
| S12 | Complete | Tracing spans were expanded across compiler phases and adapter hot paths (`dataset_exec`, `udaf`, `udwf`, decomposed `codeanatomy_ext` delta modules), with new tracing-focused test files added. |
| S13 | Complete | Correctness classification is typed (`RuleClass`-based), and duplicated overlay priority sort logic is consolidated. |
| S14 | Complete | Upstream contract coverage is executed via `rust/codeanatomy_engine/tests/datafusion_python_contract_regressions_tests.rs` (source-level guardrails for type mapping, expr variant conversion, signature module removal, dataset schema fallback, and wrapper macro cutover), avoiding `datafusion-python` PyO3 test-link constraints. |
| S15 | Complete | Compile pipeline decomposition landed (`compile_phases.rs`) and manual plan traversal loops were replaced with TreeNode traversal. |
| S16 | Complete | Maintenance stubs were replaced with real `datafusion_ext::delta_maintenance::*` execution in strict order, with coverage added. |
| S17 | Complete | Session-aware scan config construction is adopted and aligned with shared delta control-plane behavior, with dedicated builder tests added. |
| D1 | Complete | `legacy.rs` and `super::legacy::*` forwarding were removed. |
| D2 | Complete | Duplicate pushdown contracts and legacy write-metadata boundary fields were removed. |
| D3 | Complete | `sql/exceptions.rs` and wiring were removed. |
| D4 | Complete | Dead signature module and manual unary bool wrapper boilerplate were removed. |
| D5 | Complete | Manual plan traversal and monolithic compile-phase blocks were removed in favor of extracted phase modules and TreeNode traversal. |
| D6 | Complete | Maintenance stub messaging/TODO posture is removed in favor of real maintenance execution. |

## Remaining Scope (Post-Audit)

None. All scoped items in this plan (`S1`-`S17`, `D1`-`D6`) are implemented in-tree.

## Design Principles

1. **Single authority for shared knowledge.** No duplicated algorithms, schema builders, enum contracts, or error-helper implementations.
2. **No shim migration in design phase.** Replace call sites directly; do not introduce compatibility re-exports.
3. **Boundary contracts are explicit and typed.** JSON/serde surfaces must represent valid state directly.
4. **Dependency direction remains inward.** Shared contracts live in neutral modules; spec and providers import them, neither aliases the other.
5. **Leverage built-in DataFusion/DeltaLake capabilities first.** Prefer `DeltaScanConfigBuilder`, maintenance builders, and TreeNode traversal over hand-rolled equivalents.
6. **Facade modules own behavior.** Submodules must contain their own `#[pyfunction]` implementations; avoid `super::legacy::*` forwarding.
7. **Eliminate sentinel states.** Replace `""` placeholders and triple-None structs with typed variants/options.
8. **Observability is a first-class contract.** Add phase- and boundary-level spans on critical execution paths.
9. **Test coverage follows ownership.** Every new module added in this plan has a corresponding new test file.
10. **Delete superseded code promptly.** Decommission in explicit batches after prerequisites land.

## Current Baseline

Historical snapshot at plan-authoring time. See `Implementation Progress Audit (2026-02-17)` above for current status against this baseline.

- `rust/datafusion_python/src/codeanatomy_ext/legacy.rs` is still a 2,368 LOC monolith with 39 `#[pyfunction]` entries; facade modules still forward to it via `super::legacy::*`.
- S1 correctness bugs are still present:
  - `rust/datafusion_python/src/common/data_type.rs:340` Union path emits `"ScalarValue::LargeList"`.
  - `rust/datafusion_python/src/expr/indexed_field.rs:64` contains `todo!()`.
  - `rust/datafusion_python/src/expr.rs:765,767` registers `PyLiteral` twice.
  - `rust/datafusion_python/src/physical_plan.rs:84` says `"logical node"` in physical-plan decode path.
  - `rust/datafusion_ext/src/udf/hash.rs:146` references `stable_hash_any` from `StableHash128Udf`.
- Compiler helper duplication persists across six files (`plan_bundle.rs`, `optimizer_pipeline.rs`, `graph_validator.rs`, `pushdown_probe_extract.rs`, `cache_boundaries.rs`, `plan_compiler.rs`).
- Two separate `PushdownEnforcementMode` enums still exist:
  - `rust/codeanatomy_engine/src/spec/runtime.rs:20`
  - `rust/codeanatomy_engine/src/providers/pushdown_contract.rs:166`
- `WriteOutcome` remains a triple-Option struct (`rust/codeanatomy_engine/src/executor/delta_writer.rs:238`) mirrored as separate nullable fields in `MaterializationResult` (`rust/codeanatomy_engine/src/executor/result.rs:55`).
- `datafusion_ext` DRY and naming issues remain:
  - duplicated `span_struct_type()` in `rust/datafusion_ext/src/registry/snapshot.rs:372` and `rust/datafusion_ext/src/udf/common.rs:483`.
  - colliding `snapshot_payload()` names in `rust/datafusion_ext/src/delta_protocol.rs:223` and `rust/datafusion_ext/src/delta_observability.rs:52`.
- `RegistrySnapshot` fields are fully public (`rust/datafusion_ext/src/registry/snapshot_types.rs:12-27`), and metadata sentinel values still use `name: ""` (`rust/datafusion_ext/src/udf/mod.rs:112`, `rust/datafusion_ext/src/udaf_builtin.rs:1699`, `rust/datafusion_ext/src/udwf_builtin.rs:80`).
- `datafusion_ext` public-surface assumption in prior plan was incorrect: `datafusion_ext::async_runtime` is consumed cross-crate (`rust/df_plugin_codeanatomy/src/lib.rs:108`, `rust/datafusion_python/src/utils.rs:39`), so shrinking visibility blindly would regress valid consumers.
- Error helper authority remains split:
  - `rust/datafusion_python/src/errors.rs:77`
  - `rust/datafusion_python/src/sql/exceptions.rs:22`
- Upstream robustness gaps remain:
  - `dataset.rs` uses `unwrap()` in `schema()` (`rust/datafusion_python/src/dataset.rs:79,81`).
  - `PyExpr::to_variant()` does not support `ScalarFunction`/`WindowFunction` (`rust/datafusion_python/src/expr.rs:172-177`).
  - `pyerr_to_dferr` flattens Python exceptions into plain strings (`rust/datafusion_python/src/errors.rs:93`).
- Compilation architecture still has unresolved structural concerns:
  - monolithic orchestration in `rust/codeanatomy_engine/src/compiler/compile_contract.rs`.
  - manual stack-walk lineage extraction in `rust/codeanatomy_engine/src/compiler/plan_bundle.rs:648`.
- Engine maintenance remains stubbed (`rust/codeanatomy_engine/src/executor/maintenance.rs:9,174`) despite real Delta maintenance APIs already existing in `rust/datafusion_ext/src/delta_maintenance.rs`.
- Existing-test mismatch: `is_scan_operator` test already exists at `rust/codeanatomy_engine/src/executor/metrics_collector.rs:249`, so that prior-plan item is stale.

---

## S1. Correctness Bug Fixes

### Goal

Fix all known copy/paste error strings, panic placeholders, and duplicate registrations before larger refactors.

### Representative Code Snippets

```rust
// rust/datafusion_python/src/common/data_type.rs
ScalarValue::Union(_, _, _) => Err(PyNotImplementedError::new_err(
    "ScalarValue::Union".to_string(),
)),
```

```rust
// rust/datafusion_python/src/expr/indexed_field.rs
fn key(&self) -> PyResult<PyLiteral> {
    match &self.indexed_field.field {
        GetFieldAccess::NamedStructField { name, .. } => Ok(name.clone().into()),
        other => Err(PyNotImplementedError::new_err(format!(
            "GetFieldAccess::{other:?} is not yet supported"
        ))),
    }
}
```

```rust
// rust/datafusion_ext/src/udf/hash.rs
fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.is_empty() || arg_types.len() > 3 {
        return Err(DataFusionError::Plan(
            "stable_hash128 expects between one and three arguments".into(),
        ));
    }
    // ...
}
```

```rust
// rust/datafusion_python/src/physical_plan.rs
PyRuntimeError::new_err(format!(
    "Unable to decode physical plan node from serialized bytes: {e}"
))
```

### Files to Edit

- `rust/datafusion_python/src/common/data_type.rs`
- `rust/datafusion_python/src/expr/indexed_field.rs`
- `rust/datafusion_python/src/expr.rs`
- `rust/datafusion_python/src/physical_plan.rs`
- `rust/datafusion_ext/src/udf/hash.rs`

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete duplicate `m.add_class::<PyLiteral>()?;` at `rust/datafusion_python/src/expr.rs:767`.
- Delete `todo!()` at `rust/datafusion_python/src/expr/indexed_field.rs:64`.

---

## S2. Compiler Helper Consolidation

### Goal

Create a single compiler helper authority for normalization, hashing, view indexing, and fanout calculations.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/compiler/plan_utils.rs
use blake3::Hasher;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use std::collections::HashMap;

use crate::spec::execution_spec::SemanticExecutionSpec;
use crate::spec::relations::ViewDefinition;

pub(crate) fn normalize_logical(plan: &LogicalPlan) -> String {
    format!("{}", plan.display_indent())
}

pub(crate) fn normalize_physical(plan: &dyn ExecutionPlan) -> String {
    format!("{}", displayable(plan).indent(true))
}

pub(crate) fn blake3_hash_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

pub(crate) fn view_index(spec: &SemanticExecutionSpec) -> HashMap<&str, &ViewDefinition> {
    spec.view_definitions
        .iter()
        .map(|view| (view.name.as_str(), view))
        .collect()
}

pub(crate) fn compute_view_fanout(spec: &SemanticExecutionSpec) -> HashMap<String, usize> {
    let mut fanout: HashMap<String, usize> = spec
        .view_definitions
        .iter()
        .map(|view| (view.name.clone(), 0usize))
        .collect();
    for view in &spec.view_definitions {
        for dep in &view.view_dependencies {
            *fanout.entry(dep.clone()).or_insert(0) += 1;
        }
    }
    for target in &spec.output_targets {
        *fanout.entry(target.source_view.clone()).or_insert(0) += 1;
    }
    fanout
}
```

### Files to Edit

- `rust/codeanatomy_engine/src/compiler/mod.rs`
- `rust/codeanatomy_engine/src/compiler/plan_bundle.rs`
- `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs`
- `rust/codeanatomy_engine/src/compiler/graph_validator.rs`
- `rust/codeanatomy_engine/src/compiler/pushdown_probe_extract.rs`
- `rust/codeanatomy_engine/src/compiler/cache_boundaries.rs`
- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs`

### New Files to Create

- `rust/codeanatomy_engine/src/compiler/plan_utils.rs`
- `rust/codeanatomy_engine/tests/plan_utils_tests.rs`

### Legacy Decommission/Delete Scope

- Delete duplicated helper implementations from:
  - `rust/codeanatomy_engine/src/compiler/plan_bundle.rs`
  - `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs`
  - `rust/codeanatomy_engine/src/compiler/graph_validator.rs`
  - `rust/codeanatomy_engine/src/compiler/pushdown_probe_extract.rs`
  - `rust/codeanatomy_engine/src/compiler/cache_boundaries.rs`
  - `rust/codeanatomy_engine/src/compiler/plan_compiler.rs`

---

## S3. PushdownEnforcementMode Canonical Contract

### Goal

Unify `PushdownEnforcementMode` into one neutral contract module without spec/provider aliasing or re-export shims.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/contracts/pushdown_mode.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum PushdownEnforcementMode {
    #[default]
    Warn,
    Strict,
    Disabled,
}
```

```rust
// rust/codeanatomy_engine/src/spec/runtime.rs
use crate::contracts::pushdown_mode::PushdownEnforcementMode;
```

```rust
// rust/codeanatomy_engine/src/providers/pushdown_contract.rs
use crate::contracts::pushdown_mode::PushdownEnforcementMode;
```

```rust
// rust/codeanatomy_engine/src/executor/pipeline.rs
let mode = spec.runtime.pushdown_enforcement_mode;
// no map_pushdown_mode conversion function
```

### Files to Edit

- `rust/codeanatomy_engine/src/lib.rs`
- `rust/codeanatomy_engine/src/spec/runtime.rs`
- `rust/codeanatomy_engine/src/providers/pushdown_contract.rs`
- `rust/codeanatomy_engine/src/executor/pipeline.rs`
- `rust/codeanatomy_engine/src/compiler/compile_contract.rs`
- `rust/codeanatomy_engine/src/compiler/pushdown_probe_extract.rs`

### New Files to Create

- `rust/codeanatomy_engine/src/contracts/mod.rs`
- `rust/codeanatomy_engine/src/contracts/pushdown_mode.rs`
- `rust/codeanatomy_engine/tests/pushdown_mode_contract_tests.rs`

### Legacy Decommission/Delete Scope

- Delete duplicate enum in `rust/codeanatomy_engine/src/spec/runtime.rs`.
- Delete duplicate enum in `rust/codeanatomy_engine/src/providers/pushdown_contract.rs`.
- Delete both `map_pushdown_mode` functions in:
  - `rust/codeanatomy_engine/src/executor/pipeline.rs`
  - `rust/codeanatomy_engine/src/compiler/compile_contract.rs`

---

## S4. WriteOutcome Boundary Contract Redesign

### Goal

Redesign execution boundary contracts to encode write metadata state explicitly, replacing nullable field clusters.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/executor/result.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum WriteOutcome {
    Captured {
        delta_version: i64,
        files_added: u64,
        bytes_written: u64,
    },
    Unavailable {
        reason: WriteOutcomeUnavailableReason,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteOutcomeUnavailableReason {
    TableUriResolutionFailed,
    TableOpenFailed,
    TableLoadFailed,
    SnapshotReadFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializationResult {
    pub table_name: String,
    pub delta_location: Option<String>,
    pub rows_written: u64,
    pub partition_count: u32,
    pub write_outcome: WriteOutcome,
}
```

```rust
// rust/codeanatomy_engine/src/executor/delta_writer.rs
pub async fn read_write_outcome(delta_location: &str) -> WriteOutcome {
    let table_uri = match ensure_table_uri(delta_location) {
        Ok(uri) => uri,
        Err(_) => {
            return WriteOutcome::Unavailable {
                reason: WriteOutcomeUnavailableReason::TableUriResolutionFailed,
            }
        }
    };
    // ...
}
```

### Files to Edit

- `rust/codeanatomy_engine/src/executor/delta_writer.rs`
- `rust/codeanatomy_engine/src/executor/result.rs`
- `rust/codeanatomy_engine/src/executor/runner.rs`
- `rust/codeanatomy_engine/src/compiler/compile_contract.rs`
- `rust/codeanatomy_engine_py/src/materializer.rs`

### New Files to Create

- `rust/codeanatomy_engine/tests/write_outcome_contract_tests.rs`

### Legacy Decommission/Delete Scope

- Delete `WriteOutcome` triple-Option struct in `rust/codeanatomy_engine/src/executor/delta_writer.rs`.
- Delete `delta_version`, `files_added`, and `bytes_written` top-level fields from `MaterializationResult` in `rust/codeanatomy_engine/src/executor/result.rs`.
- Delete all triple-None fallback literals in `read_write_outcome()`.

---

## S5. DataFusion Extensions DRY and Naming Disambiguation

### Goal

Eliminate duplicated Delta/schema helpers and resolve payload-name collisions in `datafusion_ext`.

### Representative Code Snippets

```rust
// rust/datafusion_ext/src/registry/snapshot.rs
use crate::udf::common::span_struct_type;
// delete local duplicate fn span_struct_type
```

```rust
// rust/datafusion_ext/src/delta_protocol.rs
pub fn snapshot_info_as_strings(snapshot: &DeltaSnapshotInfo) -> HashMap<String, String> {
    // existing snapshot_payload body
}
```

```rust
// rust/datafusion_ext/src/delta_observability.rs
pub fn snapshot_info_as_values(
    snapshot: &DeltaSnapshotInfo,
) -> HashMap<String, serde_json::Value> {
    // existing snapshot_payload body
}
```

### Files to Edit

- `rust/datafusion_ext/src/registry/snapshot.rs`
- `rust/datafusion_ext/src/udf/common.rs`
- `rust/datafusion_ext/src/delta_protocol.rs`
- `rust/datafusion_ext/src/delta_observability.rs`
- `rust/datafusion_python/src/codeanatomy_ext/legacy.rs`

### New Files to Create

- `rust/datafusion_ext/tests/delta_payload_naming_tests.rs`

### Legacy Decommission/Delete Scope

- Delete local `span_struct_type()` from `rust/datafusion_ext/src/registry/snapshot.rs`.
- Delete old `snapshot_payload` names in:
  - `rust/datafusion_ext/src/delta_protocol.rs`
  - `rust/datafusion_ext/src/delta_observability.rs`

---

## S6. Registry Contract Hardening and API Surface Correction

### Goal

Harden registry contracts (encapsulation + no sentinel names) and correct public-surface policy based on real external usage.

### Representative Code Snippets

```rust
// rust/datafusion_ext/src/registry/snapshot_types.rs
#[derive(Debug, Clone, Serialize)]
pub struct RegistrySnapshot {
    version: u32,
    scalar: Vec<String>,
    // ...
}

impl RegistrySnapshot {
    pub fn version(&self) -> u32 { self.version }
    pub fn scalar(&self) -> &[String] { &self.scalar }
    // ...
}
```

```rust
// rust/datafusion_ext/src/registry/metadata.rs
#[derive(Debug, Clone)]
pub struct FunctionMetadata {
    pub name: Option<&'static str>,
    pub kind: FunctionKind,
    // ...
}
```

```rust
// rust/datafusion_ext/src/udaf_builtin.rs (similar in udf/mod.rs and udwf_builtin.rs)
Some(FunctionMetadata {
    name: None,
    kind: FunctionKind::Aggregate,
    // ...
})
```

```rust
// rust/datafusion_ext/src/lib.rs
pub mod async_runtime;       // external consumers exist
pub(crate) mod config_macros;
pub(crate) mod error_conversion;
pub mod operator_utils;      // integration tests and external consumers
```

### Files to Edit

- `rust/datafusion_ext/src/registry/snapshot_types.rs`
- `rust/datafusion_ext/src/registry/metadata.rs`
- `rust/datafusion_ext/src/registry/snapshot.rs`
- `rust/datafusion_ext/src/udf/mod.rs`
- `rust/datafusion_ext/src/udaf_builtin.rs`
- `rust/datafusion_ext/src/udwf_builtin.rs`
- `rust/datafusion_ext/src/lib.rs`
- `rust/datafusion_python/src/codeanatomy_ext/legacy.rs`

### New Files to Create

- `rust/datafusion_ext/tests/registry_snapshot_contract_tests.rs`

### Legacy Decommission/Delete Scope

- Delete direct field-access construction patterns for `RegistrySnapshot` payloads in `rust/datafusion_python/src/codeanatomy_ext/legacy.rs`.
- Delete empty-string metadata sentinel usage (`name: ""`) from:
  - `rust/datafusion_ext/src/udf/mod.rs`
  - `rust/datafusion_ext/src/udaf_builtin.rs`
  - `rust/datafusion_ext/src/udwf_builtin.rs`

---

## S7. codeanatomy_ext Helper Consolidation

### Goal

Consolidate bridge helpers into shared helper modules so decomposed submodules stop depending on legacy-owned utility functions.

### Representative Code Snippets

```rust
// rust/datafusion_python/src/codeanatomy_ext/helpers.rs
pub(crate) fn runtime() -> PyResult<Runtime> {
    Runtime::new()
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}")))
}

pub(crate) fn table_version_from_options(
    version: Option<i64>,
    timestamp: Option<String>,
) -> PyResult<TableVersion> {
    TableVersion::from_options(version, timestamp)
        .map_err(|err| PyValueError::new_err(format!("Invalid Delta table version options: {err}")))
}

pub(crate) fn parse_msgpack_payload<T: for<'de> Deserialize<'de>>(
    payload: &[u8],
    label: &str,
) -> PyResult<T> {
    rmp_serde::from_slice(payload)
        .map_err(|err| PyValueError::new_err(format!("Invalid {label} payload: {err}")))
}
```

```rust
// rust/datafusion_python/src/codeanatomy_ext/delta_provider.rs
use super::helpers::{runtime, table_version_from_options};
// remove super::legacy::runtime/table_version_from_options calls
```

### Files to Edit

- `rust/datafusion_python/src/codeanatomy_ext/helpers.rs`
- `rust/datafusion_python/src/codeanatomy_ext/delta_provider.rs`
- `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs`
- `rust/datafusion_python/src/codeanatomy_ext/delta_maintenance.rs`
- `rust/datafusion_python/src/codeanatomy_ext/legacy.rs`

### New Files to Create

- `rust/datafusion_python/tests/codeanatomy_ext_helpers_cutover_tests.rs`

### Legacy Decommission/Delete Scope

- Delete duplicated helper implementations from:
  - `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs`
  - `rust/datafusion_python/src/codeanatomy_ext/delta_maintenance.rs`
  - `rust/datafusion_python/src/codeanatomy_ext/legacy.rs`
- Delete remaining helper call sites using `super::legacy::*` in decomposed modules.

---

## S8. legacy.rs Full Decomposition and Ownership Migration

### Goal

Finish physical decomposition: all `#[pyfunction]` implementations and shared class ownership move to focused modules; `legacy.rs` is deleted.

### Representative Code Snippets

```rust
// rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs
#[pyfunction]
pub(crate) fn install_function_factory(ctx: &Bound<'_, PyAny>) -> PyResult<()> {
    let session_ctx = extract_session_ctx(ctx)?;
    datafusion_ext::sql_macro_factory::install_sql_macro_factory_native(&session_ctx)
        .map_err(|err| PyRuntimeError::new_err(format!("FunctionFactory install failed: {err}")))
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(install_function_factory, module)?)?;
    // ...
    Ok(())
}
```

```rust
// rust/datafusion_python/src/codeanatomy_ext/mod.rs
pub(crate) mod cache_tables;
pub(crate) mod delta_maintenance;
pub(crate) mod delta_mutations;
pub(crate) mod delta_provider;
pub(crate) mod helpers;
pub(crate) mod plugin_bridge;
pub(crate) mod registry_bridge;
pub(crate) mod schema_evolution;
pub(crate) mod session_utils;
pub(crate) mod udf_registration;

pub fn init_module(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = py;
    session_utils::register_functions(module)?;
    udf_registration::register_functions(module)?;
    plugin_bridge::register_functions(module)?;
    // ...
    Ok(())
}
```

### Files to Edit

- `rust/datafusion_python/src/codeanatomy_ext/mod.rs`
- `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs`
- `rust/datafusion_python/src/codeanatomy_ext/plugin_bridge.rs`
- `rust/datafusion_python/src/codeanatomy_ext/cache_tables.rs`
- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs`
- `rust/datafusion_python/src/codeanatomy_ext/delta_provider.rs`
- `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs`
- `rust/datafusion_python/src/codeanatomy_ext/delta_maintenance.rs`
- `rust/datafusion_python/src/lib.rs`

### New Files to Create

- `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs`
- `rust/datafusion_python/src/codeanatomy_ext/registry_bridge.rs`
- `rust/datafusion_python/tests/codeanatomy_ext_schema_evolution_tests.rs`
- `rust/datafusion_python/tests/codeanatomy_ext_registry_bridge_tests.rs`
- `rust/datafusion_python/tests/codeanatomy_ext_legacy_decomposition_complete_tests.rs`

### Legacy Decommission/Delete Scope

- Delete `rust/datafusion_python/src/codeanatomy_ext/legacy.rs` entirely.
- Delete all `super::legacy::*` forwarding in:
  - `rust/datafusion_python/src/codeanatomy_ext/plugin_bridge.rs`
  - `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs`
  - `rust/datafusion_python/src/codeanatomy_ext/cache_tables.rs`
  - `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs`
  - `rust/datafusion_python/src/codeanatomy_ext/delta_provider.rs`
- Delete legacy-owned shared class registration pathways once moved.

---

## S9. Error Helper Consolidation (Including `py_value_err`)

### Goal

Create one error-helper authority in `errors.rs`, including `py_value_err`, and remove split helper modules.

### Representative Code Snippets

```rust
// rust/datafusion_python/src/errors.rs
pub fn py_type_err(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!("{e}"))
}

pub fn py_runtime_err(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e}"))
}

pub fn py_value_err(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{e}"))
}
```

```rust
// rust/datafusion_python/src/context.rs
use crate::errors::py_value_err;
```

### Files to Edit

- `rust/datafusion_python/src/errors.rs`
- `rust/datafusion_python/src/context.rs`
- `rust/datafusion_python/src/sql.rs`

### New Files to Create

- `rust/datafusion_python/tests/error_helper_contract_tests.rs`

### Legacy Decommission/Delete Scope

- Delete `rust/datafusion_python/src/sql/exceptions.rs`.
- Delete `pub mod exceptions;` from `rust/datafusion_python/src/sql.rs`.
- Delete all imports of `crate::sql::exceptions::*`.

---

## S10. Expression Boilerplate Reduction Across Bool and Logical Wrappers

### Goal

Collapse repetitive wrapper code using declarative macros for unary bool wrappers and logical-plan wrapper patterns.

### Representative Code Snippets

```rust
// rust/datafusion_python/src/expr/wrapper_macros.rs
macro_rules! unary_bool_expr_wrapper {
    ($ty:ident, $name:literal) => {
        #[pyclass(frozen, name = $name, module = "datafusion.expr", subclass)]
        #[derive(Clone, Debug)]
        pub struct $ty { expr: Expr }

        impl $ty {
            pub fn new(expr: Expr) -> Self { Self { expr } }
        }

        #[pymethods]
        impl $ty {
            fn expr(&self) -> PyResult<PyExpr> { Ok(self.expr.clone().into()) }
        }
    };
}

macro_rules! logical_plan_wrapper {
    ($ty:ident, $plan_ty:ty, $variant:pat => $bind:ident) => {
        // Generates Display + LogicalNode + TryFrom/From boilerplate
    };
}
```

```rust
// rust/datafusion_python/src/expr/bool_expr.rs
unary_bool_expr_wrapper!(PyNot, "Not");
unary_bool_expr_wrapper!(PyIsNotNull, "IsNotNull");
// ...
```

### Files to Edit

- `rust/datafusion_python/src/expr/bool_expr.rs`
- `rust/datafusion_python/src/expr.rs`
- Logical wrapper modules under `rust/datafusion_python/src/expr/` that share `Display`/`LogicalNode` boilerplate (e.g. `filter.rs`, `projection.rs`, `limit.rs`, `create_view.rs`, `drop_view.rs`, `join.rs`, `union.rs`, `sort.rs`).

### New Files to Create

- `rust/datafusion_python/src/expr/wrapper_macros.rs`
- `rust/datafusion_python/tests/expr_wrapper_macros_tests.rs`

### Legacy Decommission/Delete Scope

- Delete duplicated unary wrapper struct/impl blocks in `rust/datafusion_python/src/expr/bool_expr.rs`.
- Delete duplicated builder-method implementations in `rust/datafusion_python/src/expr.rs` where replaced by shared helper pathways.

---

## S11. Upstream Robustness and Completeness Cleanup

### Goal

Remove dead code and panic-prone paths, and close high-frequency unsupported variant gaps.

### Representative Code Snippets

```rust
// rust/datafusion_python/src/dataset.rs
fn schema(&self) -> SchemaRef {
    Python::attach(|py| -> PyResult<SchemaRef> {
        let dataset = self.dataset.bind(py);
        let schema = dataset
            .getattr("schema")?
            .extract::<PyArrowType<_>>()?
            .0;
        Ok(Arc::new(schema))
    })
    .unwrap_or_else(|_| Arc::new(arrow::datatypes::Schema::empty()))
}
```

```rust
// rust/datafusion_python/src/expr.rs
Expr::ScalarFunction(value) => Ok(PyExpr::from(Expr::ScalarFunction(value.clone())).into_bound_py_any(py)?),
Expr::WindowFunction(value) => Ok(PyExpr::from(Expr::WindowFunction(value.clone())).into_bound_py_any(py)?),
```

```rust
// rust/datafusion_python/src/expr.rs
// delete: pub mod signature;
```

### Files to Edit

- `rust/datafusion_python/src/dataset.rs`
- `rust/datafusion_python/src/expr.rs`
- `rust/datafusion_python/src/common/data_type.rs`
- `rust/datafusion_python/src/expr/signature.rs`

### New Files to Create

- `rust/datafusion_python/tests/upstream_robustness_tests.rs`

### Legacy Decommission/Delete Scope

- Delete dead module `rust/datafusion_python/src/expr/signature.rs`.
- Delete `RexType::Other` from `rust/datafusion_python/src/common/data_type.rs` if still unconstructed.
- Delete unsupported-variant error branches in `to_variant()` for cases now implemented.

---

## S12. Tracing and Observability Expansion

### Goal

Instrument phase boundaries and bridge hot paths so compilation and Python/Rust adapter latency can be attributed without ad hoc debugging.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/compiler/plan_compiler.rs
#[cfg_attr(feature = "tracing", instrument(skip(self)))]
pub async fn compile_with_warnings(&self) -> Result<CompilationOutcome> {
    // ...
}
```

```rust
// rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs
#[cfg_attr(feature = "tracing", instrument(skip(ctx, unoptimized_plan, config)))]
pub async fn run_optimizer_pipeline(
    ctx: &SessionContext,
    unoptimized_plan: LogicalPlan,
    config: &OptimizerPipelineConfig,
) -> Result<OptimizerPipelineResult> {
    // ...
}
```

```rust
// rust/datafusion_python/src/dataset_exec.rs
#[cfg_attr(feature = "tracing", instrument(skip(self, context)))]
fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
    // ...
}
```

### Files to Edit

- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs`
- `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs`
- `rust/codeanatomy_engine/src/compiler/plan_bundle.rs`
- `rust/codeanatomy_engine/src/compiler/pushdown_probe_extract.rs`
- `rust/datafusion_python/src/dataset_exec.rs`
- `rust/datafusion_python/src/udaf.rs`
- `rust/datafusion_python/src/udwf.rs`
- Post-S8 decomposed `codeanatomy_ext` modules

### New Files to Create

- `rust/codeanatomy_engine/tests/compiler_tracing_phase_tests.rs`
- `rust/datafusion_python/tests/adapter_tracing_tests.rs`

### Legacy Decommission/Delete Scope

- Delete reliance on top-level-only compile spans as the sole tracing boundary.
- Delete uninstrumented hot-path function variants once instrumented replacements land.

---

## S13. Rule Overlay and Rulepack DRY Cleanup Using Existing `RuleClass`

### Goal

Remove duplicated sort closures and eliminate string-matching correctness classification by using typed rule intent/class semantics.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/rules/overlay.rs
trait HasName {
    fn name(&self) -> &str;
}

fn sort_rules_by_priority<R: HasName>(
    rules: &mut [R],
    priority_map: &std::collections::HashMap<&str, i32>,
) {
    rules.sort_by(|a, b| {
        let pa = priority_map.get(a.name()).copied().unwrap_or(i32::MAX);
        let pb = priority_map.get(b.name()).copied().unwrap_or(i32::MAX);
        pa.cmp(&pb).then_with(|| a.name().cmp(b.name()))
    });
}
```

```rust
// rust/codeanatomy_engine/src/rules/rulepack.rs
use crate::spec::rule_intents::RuleClass;

fn is_correctness_class(class: &RuleClass) -> bool {
    matches!(class, RuleClass::SemanticIntegrity | RuleClass::Safety)
}
// remove rule-name substring matching
```

### Files to Edit

- `rust/codeanatomy_engine/src/rules/overlay.rs`
- `rust/codeanatomy_engine/src/rules/rulepack.rs`
- `rust/codeanatomy_engine/src/spec/rule_intents.rs` (only if additional typed fields are required)

### New Files to Create

- `rust/codeanatomy_engine/tests/rulepack_classification_tests.rs`

### Legacy Decommission/Delete Scope

- Delete `is_correctness_rule(rule_name: &str)` string-matching function.
- Delete three duplicated sort closures in `overlay.rs`.

---

## S14. Upstream Test Coverage Expansion

### Goal

Add high-signal tests for expression/type mapping and contract changes not currently guarded by tests.

### Representative Code Snippets

```rust
// rust/datafusion_python/tests/data_type_mapping_tests.rs
#[test]
fn union_error_mentions_union_variant() {
    let union_type = ScalarValue::Union(None, vec![], UnionMode::Sparse);
    let result = DataTypeMap::map_from_scalar_value(&union_type);
    assert!(result.is_err());
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("Union"));
}
```

```rust
// rust/codeanatomy_engine/tests/materialization_contract_tests.rs
#[test]
fn write_outcome_contract_serializes_captured_and_unavailable() {
    // asserts serde tags and fields for both variants
}
```

### Files to Edit

- `rust/datafusion_python/src/common/data_type.rs` (if inline tests are chosen)
- `rust/codeanatomy_engine/src/executor/result.rs` (test helpers if needed)

### New Files to Create

- `rust/datafusion_python/tests/data_type_mapping_tests.rs`
- `rust/datafusion_python/tests/expr_variant_conversion_tests.rs`
- `rust/codeanatomy_engine/tests/materialization_contract_tests.rs`

### Legacy Decommission/Delete Scope

- Delete stale plan task that re-adds `is_scan_operator` testing (already covered in existing test suite).

---

## S15. Compilation Pipeline Decomposition and TreeNode Traversal Adoption

### Goal

Decompose monolithic compile orchestration and replace manual plan tree walking with DataFusion TreeNode traversal for consistency and correctness.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/compiler/compile_phases.rs
pub(crate) async fn build_task_schedule_phase(
    spec: &SemanticExecutionSpec,
) -> (Option<TaskSchedule>, Option<StatsQuality>) {
    // extracted from compile_contract::compile_request
}

pub(crate) async fn pushdown_probe_phase(
    ctx: &SessionContext,
    spec: &SemanticExecutionSpec,
) -> (BTreeMap<String, PushdownContractReport>, Vec<RunWarning>) {
    // extracted orchestration
}
```

```rust
// rust/codeanatomy_engine/src/compiler/plan_bundle.rs
let _ = optimized_plan.apply(|node| {
    if let LogicalPlan::TableScan(scan) = node {
        // lineage capture
    }
    Ok(TreeNodeRecursion::Continue)
});
```

### Files to Edit

- `rust/codeanatomy_engine/src/compiler/compile_contract.rs`
- `rust/codeanatomy_engine/src/compiler/plan_bundle.rs`
- `rust/codeanatomy_engine/src/compiler/pushdown_probe_extract.rs`
- `rust/codeanatomy_engine/src/compiler/mod.rs`

### New Files to Create

- `rust/codeanatomy_engine/src/compiler/compile_phases.rs`
- `rust/codeanatomy_engine/tests/compile_phases_tests.rs`

### Legacy Decommission/Delete Scope

- Delete monolithic phase blocks in `compile_contract.rs` once extracted.
- Delete manual stack-based plan traversals in:
  - `rust/codeanatomy_engine/src/compiler/plan_bundle.rs`
  - `rust/codeanatomy_engine/src/compiler/pushdown_probe_extract.rs`

---

## S16. Engine Delta Maintenance Stub Replacement

### Goal

Replace `executor/maintenance.rs` stub reporting with real calls to `datafusion_ext::delta_maintenance::*` operations and preserve strict maintenance ordering.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/executor/maintenance.rs
let compact_report = datafusion_ext::delta_maintenance::delta_optimize_compact_request(
    DeltaOptimizeCompactRequest {
        session_ctx: ctx,
        table_uri,
        storage_options: None,
        table_version: TableVersion::Latest,
        target_size: schedule.compact.as_ref().map(|p| p.target_file_size),
        gate: None,
        commit_options: None,
    },
)
.await?;
```

```rust
// rust/codeanatomy_engine/src/executor/maintenance.rs
let (table, metrics) = table
    .vacuum()
    .with_retention_period(duration)
    .with_enforce_retention_duration(true)
    .with_dry_run(vacuum.dry_run_first)
    .await?;
```

### Files to Edit

- `rust/codeanatomy_engine/src/executor/maintenance.rs`
- `rust/codeanatomy_engine/src/executor/pipeline.rs`
- `rust/codeanatomy_engine/src/executor/result.rs`

### New Files to Create

- `rust/codeanatomy_engine/tests/maintenance_execution_tests.rs`

### Legacy Decommission/Delete Scope

- Delete all stub-only success messages in `rust/codeanatomy_engine/src/executor/maintenance.rs`.
- Delete comments that describe maintenance as “integration-agent TODO” once actual calls are wired.

---

## S17. Delta Scan Config Builder Unification

### Goal

Adopt `DeltaScanConfigBuilder`/session-aware construction everywhere and remove custom duplicated config mutation/validation pathways.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/providers/scan_config.rs
pub fn standard_scan_config(
    session: &dyn datafusion::catalog::Session,
    snapshot: &deltalake::kernel::snapshot::EagerSnapshot,
    requires_lineage: bool,
) -> Result<DeltaScanConfig, DeltaTableError> {
    let base = DeltaScanConfig::new_from_session(session);
    let mut builder = DeltaScanConfigBuilder::new()
        .with_parquet_pushdown(base.enable_parquet_pushdown)
        .wrap_partition_values(base.wrap_partition_values);

    if requires_lineage {
        builder = builder.with_file_column_name("__source_file");
    }

    builder.build(snapshot)
}
```

### Files to Edit

- `rust/codeanatomy_engine/src/providers/scan_config.rs`
- `rust/codeanatomy_engine/src/providers/registration.rs`
- `rust/codeanatomy_engine/src/providers/snapshot.rs`
- `rust/datafusion_ext/src/delta_control_plane.rs` (shared behavior alignment)

### New Files to Create

- `rust/codeanatomy_engine/tests/scan_config_builder_tests.rs`

### Legacy Decommission/Delete Scope

- Delete direct `DeltaScanConfig::default()` field mutation in `rust/codeanatomy_engine/src/providers/scan_config.rs`.
- Delete redundant custom lineage-name collision checks superseded by `DeltaScanConfigBuilder::build(snapshot)` validation.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S7, S8)

- Delete `rust/datafusion_python/src/codeanatomy_ext/legacy.rs` after all functions/classes/helpers are re-homed.
- Delete all `super::legacy::*` references from `codeanatomy_ext` submodules.

### Batch D2 (after S3, S4)

- Delete duplicated pushdown enum definitions and `map_pushdown_mode` functions.
- Delete legacy nullable write-metadata fields from run/materialization boundaries.

### Batch D3 (after S9)

- Delete `rust/datafusion_python/src/sql/exceptions.rs` and associated module/export wiring.

### Batch D4 (after S10, S11)

- Delete dead `signature.rs` and its module declaration.
- Delete hand-written bool wrapper boilerplate superseded by macros.

### Batch D5 (after S15)

- Delete manual plan traversal loops replaced by TreeNode traversal.
- Delete monolithic compile phase blocks replaced by extracted phase modules.

### Batch D6 (after S16)

- Delete maintenance stub messages and TODO integration commentary in `executor/maintenance.rs`.

## Implementation Sequence

1. **S1 — Correctness Bug Fixes.** Removes known correctness defects with minimal dependency risk.
2. **S2 — Compiler Helper Consolidation.** Establishes shared helpers before further compiler decomposition.
3. **S3 — Pushdown Contract Unification.** Removes contract duplication and clarifies dependency direction.
4. **S4 — WriteOutcome Boundary Redesign.** Land boundary contract changes early so downstream scopes build on final shape.
5. **S5 — datafusion_ext DRY/Naming Fixes.** Small independent fixes with low integration risk.
6. **S6 — Registry Contract Hardening.** Tighten contracts and correct API visibility assumptions before Python bridge migration.
7. **S7 — Helper Consolidation.** Prerequisite for deleting `legacy.rs`.
8. **S8 — Full legacy Decomposition.** Largest structural change; depends on S7.
9. **S9 — Error Helper Consolidation.** Apply hard cutover for Python error helpers after module decomposition stabilizes.
10. **S10 — Expression Macro Reduction.** Medium refactor isolated to upstream binding layer.
11. **S11 — Upstream Robustness Cleanup.** Remove dead/panic-prone paths once macro refactors settle.
12. **S15 — Compile Pipeline Decomposition.** Structural compiler cleanup after helper consolidation.
13. **S16 — Maintenance Stub Replacement.** Replace stubs with real Delta maintenance operations.
14. **S12 — Tracing Expansion.** Instrument final function ownership paths (post-S8/S15/S16).
15. **S13 — Rulepack/Overlay DRY Cleanup.** Typed classification cleanup after prior rule/contract work.
16. **S17 — Delta Scan Config Builder Unification.** Finalize provider configuration model using builder-native APIs.
17. **S14 — Test Coverage Expansion.** Finalize regression suite against the completed target design.

## Implementation Checklist

- [x] S1 — Correctness Bug Fixes
- [x] S2 — Compiler Helper Consolidation
- [x] S3 — PushdownEnforcementMode Canonical Contract
- [x] S4 — WriteOutcome Boundary Contract Redesign
- [x] S5 — DataFusion Extensions DRY and Naming Disambiguation
- [x] S6 — Registry Contract Hardening and API Surface Correction
- [x] S7 — codeanatomy_ext Helper Consolidation
- [x] S8 — legacy.rs Full Decomposition and Ownership Migration
- [x] S9 — Error Helper Consolidation (Including `py_value_err`)
- [x] S10 — Expression Boilerplate Reduction Across Bool and Logical Wrappers
- [x] S11 — Upstream Robustness and Completeness Cleanup
- [x] S12 — Tracing and Observability Expansion
- [x] S13 — Rule Overlay and Rulepack DRY Cleanup Using Existing `RuleClass`
- [x] S14 — Upstream Test Coverage Expansion
- [x] S15 — Compilation Pipeline Decomposition and TreeNode Traversal Adoption
- [x] S16 — Engine Delta Maintenance Stub Replacement
- [x] S17 — Delta Scan Config Builder Unification
- [x] D1 — Delete `legacy.rs` and all `super::legacy::*` forwarding
- [x] D2 — Delete duplicated pushdown contracts and legacy write-metadata fields
- [x] D3 — Delete `sql/exceptions.rs`
- [x] D4 — Delete dead signature module and manual bool wrapper boilerplate
- [x] D5 — Delete manual plan traversal and monolithic compile blocks
- [x] D6 — Delete maintenance stubs
