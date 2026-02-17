# Rust Workspace Design Improvements v2 Implementation Plan v1 (2026-02-17)

## Scope Summary

This plan synthesizes all actionable findings from 5 parallel design review agents covering the full `rust/` workspace (~65K LOC, 9 crates). The reviews evaluated all 24 design principles against the current codebase *after* the v1 improvement plan (S1-S14, D1-D5) was confirmed Complete. This v2 plan addresses **27 new or residual findings** across 5 themes: (1) DRY violations remain the systemic weakness (scored 0-1/3 by all 5 agents), (2) `legacy.rs` decomposition is structurally incomplete (2,368 LOC monolith behind thin facades), (3) correctness bugs (copy-paste error messages, `todo!()` panics), (4) cross-module helper duplication in both `codeanatomy_engine` and `datafusion_python`, and (5) expression binding boilerplate reducible via macros.

**Design stance:**
- Hard cutover: no backward-compatibility shims for internal Rust APIs.
- `legacy.rs` decomposition moves *implementations*, not re-export facades. The file should reach zero `#[pyfunction]` definitions.
- Macro-driven boilerplate reduction targets the `expr/` layer without changing the public Python API surface.
- DataFusion remains pinned at `51.x`. Items requiring 52.x are tracked as roadmap but not implemented.
- All correctness bugs are fixed immediately in S1 regardless of the scope they touch.

## Design Principles

1. **Single authority for shared knowledge.** Every constant, algorithm, helper, or type that appears in more than one module must live in exactly one location and be imported by consumers.
2. **Modules have one reason to change.** Files exceeding 1,000 LOC are candidates for decomposition; files exceeding 2,000 LOC require decomposition.
3. **Error messages are contracts.** Every user-facing error string must name the correct function, operation, and expected arguments.
4. **Library capabilities over hand-rolled code.** DataFusion and DeltaLake APIs available in the current pinned version are preferred over equivalent custom implementations.
5. **Type safety at boundaries.** Replace sentinel values (`""`, triple-None structs) with typed alternatives (`Option`, newtypes).
6. **Facade modules must own implementations.** A facade that only re-exports from a monolith adds indirection without value.
7. **Major upgrades are separate epics.** DataFusion 52.x migration (which would replace `rule_instrumentation.rs` entirely) is deferred to a dedicated plan.

## Current Baseline

- **DRY is still the systemic weakest principle** — all 5 agents scored P7 at 0/3 or 1/3.
- **`legacy.rs`** (2,368 LOC, 39 `#[pyfunction]`) is the renamed remnant of the prior `codeanatomy_ext.rs`. Four of seven submodules are thin re-export facades (9-38 LOC) delegating to `super::legacy::*`. Only 3 delta submodules (`delta_mutations.rs`, `delta_maintenance.rs`, `delta_provider.rs`) contain real logic.
- **Compiler helper duplication:** 4 pairs of duplicated private helpers across `plan_bundle.rs`, `optimizer_pipeline.rs`, `graph_validator.rs`, `pushdown_probe_extract.rs`, `cache_boundaries.rs`, and `plan_compiler.rs`.
- **Dual `PushdownEnforcementMode` enums:** `spec/runtime.rs:20` and `providers/pushdown_contract.rs:166` with identical variants, requiring duplicated `map_pushdown_mode` functions in `pipeline.rs:332` and `compile_contract.rs:274`.
- **Helper triplication in `codeanatomy_ext/`:** `runtime()`, `table_version_from_options()`, and `parse_msgpack_payload()` are defined 2-3 times across `legacy.rs`, `delta_mutations.rs`, and `delta_maintenance.rs`.
- **Expression binding boilerplate:** 10 identical unary boolean wrappers in `bool_expr.rs`, 6 duplicated builder methods between `PyExpr` and `PyExprFuncBuilder`, 25+ logical plan files with identical struct/From/Display/LogicalNode patterns.
- **Correctness bugs:** `data_type.rs:339` says "ScalarValue::LargeList" for Union variant; `indexed_field.rs:64` has `todo!()` panic; `hash.rs:146` says "stable_hash_any" for `StableHash128Udf`; `physical_plan.rs:84` says "logical node" for physical plan; duplicate `PyLiteral` registration at `expr.rs:765,767`.
- **`span_struct_type()`** duplicated between `udf/common.rs:483` and `registry/snapshot.rs:372`.
- **`snapshot_payload()`** naming collision between `delta_protocol.rs:223` (returns `HashMap<String, String>`) and `delta_observability.rs:52` (returns `HashMap<String, serde_json::Value>`).
- **`RegistrySnapshot`** has all 16 fields as `pub` at `snapshot_types.rs:11-28` with no accessor methods.
- **`WriteOutcome`** in `delta_writer.rs` has three identical fallback blocks returning all-`None` fields at lines 257, 268, 278.
- **Error helper duplication:** `errors.rs:77` defines `py_type_err(e: impl Debug)` while `sql/exceptions.rs:22` defines `py_type_err(e: impl Debug + Display)`.
- **Dead code:** `signature.rs` is `#[allow(dead_code)]` with empty `#[pymethods]`.

---

## S1. Correctness Bug Fixes

### Goal

Fix all copy-paste error messages, replace `todo!()` panics with proper errors, and remove duplicate class registrations across the workspace.

### Representative Code Snippets

```rust
// rust/datafusion_python/src/common/data_type.rs — fix Union error message
ScalarValue::Union(_, _, _) => Err(PyNotImplementedError::new_err(
    "ScalarValue::Union".to_string(),
)),
```

```rust
// rust/datafusion_python/src/expr/indexed_field.rs — replace todo!() with proper error
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
// rust/datafusion_ext/src/udf/hash.rs — fix StableHash128Udf error message
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
// rust/datafusion_python/src/expr.rs — remove duplicate PyLiteral registration
// Line 765: m.add_class::<PyLiteral>()?;  -- keep
// Line 767: m.add_class::<PyLiteral>()?;  -- DELETE
```

### Files to Edit

- `rust/datafusion_python/src/common/data_type.rs` — fix `ScalarValue::Union` error message at line 339
- `rust/datafusion_python/src/expr/indexed_field.rs` — replace `todo!()` at line 64 with `PyNotImplementedError`
- `rust/datafusion_ext/src/udf/hash.rs` — fix `StableHash128Udf::coerce_types` error message at line 146
- `rust/datafusion_python/src/expr.rs` — remove duplicate `m.add_class::<PyLiteral>()` at line 767
- `rust/datafusion_python/src/physical_plan.rs` — fix "logical node" error message at line 84

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete duplicate `m.add_class::<PyLiteral>()?;` at `expr.rs:767`.
- Delete `todo!()` at `indexed_field.rs:64` (replaced by proper error).

---

## S2. Compiler Helper Consolidation

### Goal

Eliminate 4 pairs of duplicated private helpers within `codeanatomy_engine/src/compiler/` by creating a shared `compiler/plan_utils.rs` module with `pub(crate)` exports.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/compiler/plan_utils.rs — NEW shared module

use blake3::Hasher;
use datafusion::physical_plan::{displayable, ExecutionPlan};

use crate::spec::execution_spec::{SemanticExecutionSpec, ViewDefinition};
use std::collections::HashMap;

/// Compute BLAKE3 hash of raw bytes.
pub(crate) fn blake3_hash_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

/// Normalize a physical execution plan to a stable text representation.
pub(crate) fn normalize_physical(plan: &dyn ExecutionPlan) -> String {
    displayable(plan).indent(true).to_string()
}

/// Build a name -> ViewDefinition index from a spec.
pub(crate) fn view_index(spec: &SemanticExecutionSpec) -> HashMap<&str, &ViewDefinition> {
    spec.views
        .iter()
        .map(|v| (v.name.as_str(), v))
        .collect()
}

/// Count downstream references (fanout) per view in the spec.
pub(crate) fn compute_view_fanout(spec: &SemanticExecutionSpec) -> HashMap<String, usize> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for view in &spec.views {
        for dep in view.input_names() {
            *counts.entry(dep.to_string()).or_default() += 1;
        }
    }
    counts
}
```

### Files to Edit

- `rust/codeanatomy_engine/src/compiler/plan_bundle.rs` — delete private `blake3_hash_bytes` (line 623) and `normalize_physical` (line 618), import from `plan_utils`
- `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs` — delete private `hash_bytes` (line 276) and `normalize_physical` (line 272), import from `plan_utils`
- `rust/codeanatomy_engine/src/compiler/graph_validator.rs` — delete private `view_index` (line 99), import from `plan_utils` (convert `BTreeMap` usage to `HashMap`)
- `rust/codeanatomy_engine/src/compiler/pushdown_probe_extract.rs` — delete private `view_index` (line 34), import from `plan_utils`
- `rust/codeanatomy_engine/src/compiler/cache_boundaries.rs` — delete `compute_fanout` (line 20), import `compute_view_fanout` from `plan_utils`
- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs` — delete `compute_ref_counts` (line 387), import `compute_view_fanout` from `plan_utils`
- `rust/codeanatomy_engine/src/compiler/mod.rs` — add `pub(crate) mod plan_utils;`

### New Files to Create

- `rust/codeanatomy_engine/src/compiler/plan_utils.rs` — shared compiler utilities

### Legacy Decommission/Delete Scope

- Delete `fn blake3_hash_bytes` from `plan_bundle.rs:623-627`.
- Delete `fn hash_bytes` from `optimizer_pipeline.rs:276-278`.
- Delete `fn normalize_physical` from `plan_bundle.rs:618-620` and `optimizer_pipeline.rs:272-274`.
- Delete `fn view_index` from `graph_validator.rs:99-104` and `pushdown_probe_extract.rs:34-39`.
- Delete `fn compute_fanout` from `cache_boundaries.rs:20-41`.
- Delete `fn compute_ref_counts` from `plan_compiler.rs:387-408`.

---

## S3. PushdownEnforcementMode Unification

### Goal

Eliminate the dual `PushdownEnforcementMode` enum definitions and both `map_pushdown_mode` mapping functions by establishing a single canonical definition.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/providers/pushdown_contract.rs — KEEP as canonical
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum PushdownEnforcementMode {
    #[default]
    Warn,
    Strict,
    Disabled,
}
```

```rust
// rust/codeanatomy_engine/src/spec/runtime.rs — RE-EXPORT instead of redefining
pub use crate::providers::pushdown_contract::PushdownEnforcementMode;
```

```rust
// rust/codeanatomy_engine/src/executor/pipeline.rs — direct use, no mapping
use crate::providers::PushdownEnforcementMode;
// DELETE: use crate::spec::runtime::PushdownEnforcementMode;
// DELETE: fn map_pushdown_mode(mode: ...) -> ... { ... }
```

### Files to Edit

- `rust/codeanatomy_engine/src/spec/runtime.rs` — replace enum definition (lines 20-25) with re-export from `providers::pushdown_contract`
- `rust/codeanatomy_engine/src/executor/pipeline.rs` — remove alias import (line 54), remove `map_pushdown_mode` function (lines 332-338), use `PushdownEnforcementMode` directly
- `rust/codeanatomy_engine/src/compiler/compile_contract.rs` — remove alias import (line 28), remove `map_pushdown_mode` function (lines 274-278), use `PushdownEnforcementMode` directly
- `rust/codeanatomy_engine/src/compiler/pushdown_probe_extract.rs` — update import if needed (line 18)

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete `pub enum PushdownEnforcementMode { Warn, Strict, Disabled }` from `spec/runtime.rs:20-25` (replaced by re-export).
- Delete `fn map_pushdown_mode` from `executor/pipeline.rs:332-338`.
- Delete `fn map_pushdown_mode` from `compiler/compile_contract.rs:274-278`.

---

## S4. WriteOutcome Type Safety and DRY

### Goal

Replace the triple-`Option` `WriteOutcome` struct with `Option<WriteOutcomeDetails>` to eliminate impossible states and consolidate three identical fallback blocks.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/executor/delta_writer.rs — improved types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteOutcomeDetails {
    pub delta_version: i64,
    pub files_added: u64,
    pub bytes_written: u64,
}

/// Outcome of a Delta write. `None` if metadata capture failed (not a
/// pipeline-blocking error).
pub type WriteOutcome = Option<WriteOutcomeDetails>;
```

```rust
// rust/codeanatomy_engine/src/executor/delta_writer.rs — simplified fallbacks
pub async fn read_write_outcome(delta_location: &str) -> WriteOutcome {
    let table_uri = match ensure_table_uri(delta_location) {
        Ok(uri) => uri,
        Err(_) => return None,
    };
    let mut table = match DeltaTable::try_from_url(&table_uri) {
        Ok(t) => t,
        Err(_) => return None,
    };
    if table.load().await.is_err() {
        return None;
    }
    // ... extract details ...
    Some(WriteOutcomeDetails {
        delta_version,
        files_added,
        bytes_written,
    })
}
```

### Files to Edit

- `rust/codeanatomy_engine/src/executor/delta_writer.rs` — replace `WriteOutcome` struct with `WriteOutcomeDetails` + type alias, simplify `read_write_outcome` fallbacks
- All consumers of `WriteOutcome` fields — update to use `Option<WriteOutcomeDetails>` pattern (`.map(|d| d.delta_version)`)

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete `pub struct WriteOutcome { delta_version: Option<i64>, files_added: Option<u64>, bytes_written: Option<u64> }` (replaced by `WriteOutcomeDetails` + `Option`).
- Delete three identical fallback blocks at `delta_writer.rs:257-261`, `267-271`, `278-282` (replaced by `return None`).

---

## S5. DataFusion Extensions DRY Fixes

### Goal

Eliminate `span_struct_type()` duplication, disambiguate `snapshot_payload()` naming collision, and fix the `StableHash128Udf` error message in `datafusion_ext`.

### Representative Code Snippets

```rust
// rust/datafusion_ext/src/registry/snapshot.rs — import instead of redefine
use crate::udf::common::span_struct_type;
// DELETE: fn span_struct_type() -> DataType { ... }
```

```rust
// rust/datafusion_ext/src/delta_protocol.rs — disambiguated name
pub fn snapshot_info_as_strings(snapshot: &DeltaSnapshotInfo) -> HashMap<String, String> {
    // ... existing body ...
}
```

```rust
// rust/datafusion_ext/src/delta_observability.rs — disambiguated name
pub fn snapshot_info_as_values(snapshot: &DeltaSnapshotInfo) -> HashMap<String, serde_json::Value> {
    // ... existing body ...
}
```

### Files to Edit

- `rust/datafusion_ext/src/registry/snapshot.rs` — delete private `span_struct_type()` at line 372, import from `crate::udf::common`
- `rust/datafusion_ext/src/delta_protocol.rs` — rename `snapshot_payload` to `snapshot_info_as_strings` at line 223
- `rust/datafusion_ext/src/delta_observability.rs` — rename `snapshot_payload` to `snapshot_info_as_values` at line 52
- All callers of `delta_protocol::snapshot_payload` and `delta_observability::snapshot_payload` — update to new names

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete private `fn span_struct_type()` from `registry/snapshot.rs:372`.
- Delete function name `snapshot_payload` from both `delta_protocol.rs:223` and `delta_observability.rs:52` (replaced by disambiguated names).

---

## S6. RegistrySnapshot Encapsulation

### Goal

Make `RegistrySnapshot` fields `pub(crate)` with read-only accessor methods. Reduce `datafusion_ext/src/lib.rs` public surface by marking internal utility modules `pub(crate)`.

### Representative Code Snippets

```rust
// rust/datafusion_ext/src/registry/snapshot_types.rs — encapsulated fields
#[derive(Debug, Clone, Serialize)]
pub struct RegistrySnapshot {
    pub(crate) version: u32,
    pub(crate) scalar: Vec<String>,
    pub(crate) aggregate: Vec<String>,
    pub(crate) window: Vec<String>,
    pub(crate) table: Vec<String>,
    pub(crate) aliases: BTreeMap<String, Vec<String>>,
    pub(crate) parameter_names: BTreeMap<String, Vec<String>>,
    pub(crate) volatility: BTreeMap<String, String>,
    pub(crate) rewrite_tags: BTreeMap<String, Vec<String>>,
    pub(crate) simplify: BTreeMap<String, bool>,
    pub(crate) coerce_types: BTreeMap<String, bool>,
    pub(crate) short_circuits: BTreeMap<String, bool>,
    pub(crate) signature_inputs: BTreeMap<String, Vec<Vec<String>>>,
    pub(crate) return_types: BTreeMap<String, Vec<String>>,
    pub(crate) config_defaults: BTreeMap<String, BTreeMap<String, UdfConfigValue>>,
    pub(crate) custom_udfs: Vec<String>,
}

impl RegistrySnapshot {
    pub const CURRENT_VERSION: u32 = 1;

    pub fn version(&self) -> u32 { self.version }
    pub fn scalar(&self) -> &[String] { &self.scalar }
    pub fn aggregate(&self) -> &[String] { &self.aggregate }
    pub fn window(&self) -> &[String] { &self.window }
    pub fn table(&self) -> &[String] { &self.table }
    pub fn aliases(&self) -> &BTreeMap<String, Vec<String>> { &self.aliases }
    pub fn parameter_names(&self) -> &BTreeMap<String, Vec<String>> { &self.parameter_names }
    pub fn volatility(&self) -> &BTreeMap<String, String> { &self.volatility }
    pub fn rewrite_tags(&self) -> &BTreeMap<String, Vec<String>> { &self.rewrite_tags }
    pub fn simplify(&self) -> &BTreeMap<String, bool> { &self.simplify }
    pub fn coerce_types(&self) -> &BTreeMap<String, bool> { &self.coerce_types }
    pub fn short_circuits(&self) -> &BTreeMap<String, bool> { &self.short_circuits }
    pub fn signature_inputs(&self) -> &BTreeMap<String, Vec<Vec<String>>> { &self.signature_inputs }
    pub fn return_types(&self) -> &BTreeMap<String, Vec<String>> { &self.return_types }
    pub fn config_defaults(&self) -> &BTreeMap<String, BTreeMap<String, UdfConfigValue>> { &self.config_defaults }
    pub fn custom_udfs(&self) -> &[String] { &self.custom_udfs }
}
```

```rust
// rust/datafusion_ext/src/lib.rs — reduced public surface
pub(crate) mod async_runtime;
pub(crate) mod config_macros;
pub(crate) mod error_conversion;
pub(crate) mod operator_utils;
```

### Files to Edit

- `rust/datafusion_ext/src/registry/snapshot_types.rs` — change all fields from `pub` to `pub(crate)`, add accessor methods
- `rust/datafusion_ext/src/lib.rs` — change `async_runtime`, `config_macros`, `error_conversion`, `operator_utils` from `pub` to `pub(crate)`
- `rust/datafusion_python/src/codeanatomy_ext/legacy.rs` — update `registry_snapshot_py()` (lines 989-1063) to use accessor methods instead of direct field access
- All other consumers of `RegistrySnapshot` fields — update to use accessors

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete direct field access patterns on `RegistrySnapshot` from `legacy.rs:989-1063` (replaced by accessor calls).

---

## S7. codeanatomy_ext Helper Consolidation

### Goal

Move triplicated helpers (`runtime()`, `table_version_from_options()`, `parse_msgpack_payload()`) and duplicated report-to-pydict converters to `helpers.rs`, eliminating DRY violations in the `codeanatomy_ext` module.

### Representative Code Snippets

```rust
// rust/datafusion_python/src/codeanatomy_ext/helpers.rs — consolidated helpers

use datafusion::execution::context::SessionContext;
use pyo3::prelude::*;
use serde::Deserialize;
use tokio::runtime::Runtime;

use crate::context::PySessionContext;
use datafusion_ext::delta_protocol::TableVersion;

/// Extract the underlying DataFusion `SessionContext` from a Python object.
pub(crate) fn extract_session_ctx(ctx: &Bound<'_, PyAny>) -> PyResult<SessionContext> {
    // ... existing implementation ...
}

/// Create a single-threaded Tokio runtime for blocking Delta operations.
pub(crate) fn runtime() -> PyResult<Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("tokio runtime: {e}")))
}

/// Parse TableVersion from optional version/timestamp parameters.
pub(crate) fn table_version_from_options(
    version: Option<i64>,
    timestamp: Option<String>,
) -> PyResult<TableVersion> {
    TableVersion::from_options(version, timestamp)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))
}

/// Deserialize a msgpack payload from Python bytes.
pub(crate) fn parse_msgpack_payload<T: for<'de> Deserialize<'de>>(
    payload: &[u8],
    label: &str,
) -> PyResult<T> {
    rmp_serde::from_slice(payload)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{label}: {e}")))
}
```

### Files to Edit

- `rust/datafusion_python/src/codeanatomy_ext/helpers.rs` — add `runtime()`, `table_version_from_options()`, `parse_msgpack_payload()`
- `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs` — delete private `runtime()` (line 92), `table_version_from_options()` (line 97), `parse_msgpack_payload()` (line 84), import from `super::helpers`
- `rust/datafusion_python/src/codeanatomy_ext/delta_maintenance.rs` — delete private `runtime()` (line 132), `table_version_from_options()` (line 137), `parse_msgpack_payload()` (line 124), import from `super::helpers`
- `rust/datafusion_python/src/codeanatomy_ext/legacy.rs` — delete `pub(crate) fn runtime()` (line 223), `pub(crate) fn table_version_from_options()` (line 149), import from `super::helpers`
- `rust/datafusion_python/src/codeanatomy_ext/delta_provider.rs` — update `super::legacy::runtime()` and `super::legacy::table_version_from_options()` calls to use `super::helpers`

### New Files to Create

None (editing existing `helpers.rs`).

### Legacy Decommission/Delete Scope

- Delete `fn runtime()` from `delta_mutations.rs:92`, `delta_maintenance.rs:132`, `legacy.rs:223`.
- Delete `fn table_version_from_options()` from `delta_mutations.rs:97`, `delta_maintenance.rs:137`, `legacy.rs:149`.
- Delete `fn parse_msgpack_payload()` from `delta_mutations.rs:84`, `delta_maintenance.rs:124`.

---

## S8. legacy.rs Full Decomposition

### Goal

Complete the physical decomposition of `legacy.rs` (2,368 LOC, 39 `#[pyfunction]` definitions) by moving all implementations into their respective submodules. Target: `legacy.rs` reaches 0 `#[pyfunction]` definitions and is deleted.

### Representative Code Snippets

```rust
// rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs — owns implementations
use pyo3::prelude::*;
use datafusion::execution::context::SessionContext;
use super::helpers::extract_session_ctx;

#[pyfunction]
pub fn install_function_factory(ctx: &Bound<'_, PyAny>) -> PyResult<()> {
    let session_ctx = extract_session_ctx(ctx)?;
    datafusion_ext::sql_macro_factory::install_sql_macro_factory_native(&session_ctx)?;
    Ok(())
}

#[pyfunction]
pub fn derive_function_factory_policy(ctx: &Bound<'_, PyAny>) -> PyResult<PyObject> {
    // ... move implementation from legacy.rs:572 ...
}

// ... all 13 UDF registration functions moved from legacy.rs ...

pub fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(install_function_factory, module)?)?;
    module.add_function(wrap_pyfunction!(derive_function_factory_policy, module)?)?;
    // ... register all functions directly ...
    Ok(())
}
```

```rust
// rust/datafusion_python/src/codeanatomy_ext/plugin_bridge.rs — owns implementations
use pyo3::prelude::*;

#[pyfunction]
pub fn load_df_plugin(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    // ... move implementation from legacy.rs:1215 ...
}

// ... all 8 plugin functions moved from legacy.rs ...

pub fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(load_df_plugin, module)?)?;
    // ... register all functions directly ...
    Ok(())
}
```

### Files to Edit

- `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs` — move ~13 `#[pyfunction]` implementations from `legacy.rs`
- `rust/datafusion_python/src/codeanatomy_ext/plugin_bridge.rs` — move ~8 `#[pyfunction]` implementations from `legacy.rs`
- `rust/datafusion_python/src/codeanatomy_ext/cache_tables.rs` — move ~2 `#[pyfunction]` implementations from `legacy.rs`
- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs` — move ~15 `#[pyfunction]` implementations from `legacy.rs`
- `rust/datafusion_python/src/codeanatomy_ext/mod.rs` — update submodule wiring to call each submodule's `register_functions()`
- `rust/datafusion_python/src/codeanatomy_ext/legacy.rs` — progressively empty as functions move out

### New Files to Create

- `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs` — `SchemaEvolutionAdapterFactory`, `CpgTableProvider` moved from `legacy.rs`
- `rust/datafusion_python/src/codeanatomy_ext/registry_bridge.rs` — `registry_snapshot_py`, `registry_snapshot_hash`, `capabilities_snapshot`, `udf_docs_snapshot` moved from `legacy.rs`
- `rust/datafusion_python/tests/test_legacy_decomposition_complete.rs` — verify `legacy.rs` is deleted and all submodules own their functions

### Legacy Decommission/Delete Scope

- Delete `rust/datafusion_python/src/codeanatomy_ext/legacy.rs` entirely (all 2,368 LOC).
- Delete all `super::legacy::*` re-export patterns from `plugin_bridge.rs`, `udf_registration.rs`, `cache_tables.rs`, `session_utils.rs`.
- Move shared utility functions (`storage_options_map`, `delta_gate_from_params`, `scan_overrides_from_params`, `provider_capsule`, `snapshot_to_pydict`, `scan_config_to_pydict`, `json_to_py`, `mutation_report_to_pydict`, `maintenance_report_to_pydict`) to `helpers.rs` or domain-appropriate submodules.

---

## S9. Error Helper Consolidation

### Goal

Consolidate the duplicated `py_type_err` and `py_runtime_err` helpers between `errors.rs` and `sql/exceptions.rs` into a single authority.

### Representative Code Snippets

```rust
// rust/datafusion_python/src/errors.rs — single authority with Display bound
pub fn py_type_err(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!("{e}"))
}

pub fn py_runtime_err(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e}"))
}
```

```rust
// rust/datafusion_python/src/sql/exceptions.rs — re-export instead of redefine
pub use crate::errors::{py_type_err, py_runtime_err};
```

### Files to Edit

- `rust/datafusion_python/src/errors.rs` — update `py_type_err` and `py_runtime_err` trait bound from `Debug` to `Display` (lines 77-83)
- `rust/datafusion_python/src/sql/exceptions.rs` — replace function definitions with re-exports from `crate::errors`
- All callers of `sql::exceptions::py_type_err` / `py_runtime_err` — update imports to `crate::errors`

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete `pub fn py_type_err` and `pub fn py_runtime_err` definitions from `sql/exceptions.rs:22-28` (replaced by re-exports).

---

## S10. Expression Binding Boilerplate Reduction

### Goal

Introduce declarative macros to reduce the ~900 LOC of duplicated boilerplate in the expression binding layer: 10 identical unary boolean wrappers, 6 duplicated builder methods, and 20+ logical plan wrapper files.

### Representative Code Snippets

```rust
// rust/datafusion_python/src/expr/bool_expr.rs — macro-driven wrappers
macro_rules! unary_bool_expr {
    ($py_name:ident, $rust_name:expr, $variant:ident) => {
        #[pyclass(frozen, name = $rust_name, module = "datafusion.expr", subclass)]
        #[derive(Clone, Debug)]
        pub struct $py_name {
            expr: Expr,
        }

        impl From<Expr> for $py_name {
            fn from(expr: Expr) -> Self {
                Self { expr }
            }
        }

        impl std::fmt::Display for $py_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.expr)
            }
        }

        #[pymethods]
        impl $py_name {
            fn expr(&self) -> PyResult<PyExpr> {
                Ok(self.expr.clone().into())
            }

            fn __repr__(&self) -> String {
                format!("{self}")
            }
        }
    };
}

unary_bool_expr!(PyNot, "Not", Not);
unary_bool_expr!(PyIsNotNull, "IsNotNull", IsNotNull);
unary_bool_expr!(PyIsNull, "IsNull", IsNull);
unary_bool_expr!(PyIsTrue, "IsTrue", IsTrue);
unary_bool_expr!(PyIsFalse, "IsFalse", IsFalse);
unary_bool_expr!(PyIsUnknown, "IsUnknown", IsUnknown);
unary_bool_expr!(PyIsNotTrue, "IsNotTrue", IsNotTrue);
unary_bool_expr!(PyIsNotFalse, "IsNotFalse", IsNotFalse);
unary_bool_expr!(PyIsNotUnknown, "IsNotUnknown", IsNotUnknown);
unary_bool_expr!(PyNegative, "Negative", Negative);
```

```rust
// rust/datafusion_python/src/expr.rs — deduplicated builder methods
// DELETE duplicated order_by/filter/distinct/null_treatment/partition_by/window_frame
// on PyExprFuncBuilder (lines 653-686) — delegate to shared implementation
impl PyExprFuncBuilder {
    fn order_by(&self, order_by: Vec<PyExpr>) -> PyExprFuncBuilder {
        self.builder_with(|e| add_builder_fns_to_expr(e, order_by, BuilderFn::OrderBy))
    }
    // ... same pattern for filter, distinct, null_treatment, partition_by, window_frame
}
```

### Files to Edit

- `rust/datafusion_python/src/expr/bool_expr.rs` — replace 10 manual struct definitions (300 LOC) with `unary_bool_expr!` macro invocations (~40 LOC)
- `rust/datafusion_python/src/expr.rs` — deduplicate 6 builder methods between `PyExpr` and `PyExprFuncBuilder` (lines 569-598 + 653-686)

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete 10 hand-written struct/impl blocks from `bool_expr.rs:25-323` (replaced by macro invocations).
- Delete 6 duplicated builder methods from `expr.rs:653-686` (replaced by delegation or shared impl).

---

## S11. Dead Code and YAGNI Cleanup

### Goal

Remove dead code (`PySignature`), unused `RexType::Other` variant, and audit `datafusion_ext/src/registry/legacy.rs` for removal.

### Representative Code Snippets

```rust
// rust/datafusion_python/src/expr/signature.rs — DELETE entirely or implement
// Currently: #[allow(dead_code)] with empty #[pymethods]
```

### Files to Edit

- `rust/datafusion_python/src/expr/signature.rs` — delete file if PySignature is unused, or implement if needed
- `rust/datafusion_python/src/common/data_type.rs` — remove `RexType::Other` variant if unused
- `rust/datafusion_python/src/expr.rs` — remove `mod signature;` declaration if file is deleted
- `rust/datafusion_ext/src/registry/legacy.rs` — audit `#[allow(unused_imports)]` re-exports; remove if no external consumers remain

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete `rust/datafusion_python/src/expr/signature.rs` if `PySignature` is unused.
- Delete `RexType::Other` variant from `data_type.rs` if never constructed.
- Delete `rust/datafusion_ext/src/registry/legacy.rs` if all re-exports are consumed via `registry/` directly.

---

## S12. Tracing Coverage Gaps

### Goal

Add `#[instrument]` tracing spans to key functions that lack observability: compilation phases in `codeanatomy_engine`, high-traffic functions in the decomposed `codeanatomy_ext` submodules.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/compiler/plan_compiler.rs — add tracing
#[cfg_attr(feature = "tracing", instrument(skip(self)))]
pub async fn compile_with_warnings(
    &self,
) -> Result<(Vec<CompiledView>, Vec<RunWarning>)> {
    // ... existing body ...
}
```

```rust
// rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs — add tracing
#[cfg_attr(feature = "tracing", instrument(skip(ctx, rules)))]
pub fn run_optimizer_pipeline(
    ctx: &SessionContext,
    rules: &CpgRuleSet,
    plan: LogicalPlan,
    config: &OptimizerPipelineConfig,
) -> OptimizerPipelineResult {
    // ... existing body ...
}
```

### Files to Edit

- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs` — add `#[instrument]` to `compile_with_warnings`
- `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs` — add `#[instrument]` to `run_optimizer_pipeline`
- `rust/codeanatomy_engine/src/compiler/plan_bundle.rs` — add `#[instrument]` to `build_plan_bundle_artifact_with_warnings`
- `rust/codeanatomy_engine/src/compiler/pushdown_probe_extract.rs` — add `#[instrument]` to `extract_input_filter_predicates`
- Post-S8: add `#[instrument]` to high-traffic decomposed `codeanatomy_ext` functions

### New Files to Create

None.

### Legacy Decommission/Delete Scope

None.

---

## S13. Overlay Sort Generics and RuleClassification

### Goal

Replace 3 nearly identical rule-sorting closures in `overlay.rs` with a generic helper. Add a typed `RuleClassification` to replace `is_correctness_rule` string matching.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/rules/overlay.rs — generic sort helper
trait HasName {
    fn name(&self) -> &str;
}

impl HasName for Arc<dyn AnalyzerRule + Send + Sync> {
    fn name(&self) -> &str { AnalyzerRule::name(self.as_ref()) }
}
impl HasName for Arc<dyn OptimizerRule + Send + Sync> {
    fn name(&self) -> &str { OptimizerRule::name(self.as_ref()) }
}
impl HasName for Arc<dyn PhysicalOptimizerRule + Send + Sync> {
    fn name(&self) -> &str { PhysicalOptimizerRule::name(self.as_ref()) }
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
// rust/codeanatomy_engine/src/rules/rulepack.rs — typed classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleClassification {
    Correctness,
    Optimization,
    Policy,
}
// Used in RuleIntent or CpgRuleMetadata to replace:
// fn is_correctness_rule(rule_name: &str) -> bool { rule_name.contains("integrity") || ... }
```

### Files to Edit

- `rust/codeanatomy_engine/src/rules/overlay.rs` — replace 3 sort closures (lines 198-213) with `sort_rules_by_priority` generic
- `rust/codeanatomy_engine/src/rules/rulepack.rs` — add `RuleClassification` enum, replace `is_correctness_rule` string matching (lines 151-157)
- Callers of `is_correctness_rule` — update to use `RuleClassification` field

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete `fn is_correctness_rule(rule_name: &str) -> bool` from `rulepack.rs:151-157`.
- Delete 3 individual sort closures from `overlay.rs:198-213`.

---

## S14. Upstream Expression Test Coverage

### Goal

Add pure-Rust unit tests for the critical `data_type.rs` mapping functions (750+ LOC with zero test coverage) and `is_scan_operator` in the engine.

### Representative Code Snippets

```rust
// rust/datafusion_python/src/common/data_type.rs — new test module
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn test_map_from_arrow_type_round_trip() {
        let types = vec![
            DataType::Boolean,
            DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64,
            DataType::Float32, DataType::Float64,
            DataType::Utf8, DataType::LargeUtf8,
            DataType::Binary, DataType::LargeBinary,
            DataType::Date32, DataType::Date64,
        ];
        for dt in types {
            let mapped = DataTypeMap::map_from_arrow_type(&dt);
            assert!(mapped.is_ok(), "Failed to map {dt:?}");
        }
    }

    #[test]
    fn test_union_error_message_correctness() {
        let union_type = ScalarValue::Union(None, vec![], UnionMode::Sparse);
        let result = DataTypeMap::map_from_scalar_to_arrow(&union_type);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("Union"), "Error message should mention Union, got: {err_msg}");
    }
}
```

```rust
// rust/codeanatomy_engine/src/executor/metrics_collector.rs — test for is_scan_operator
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_known_scan_operators() {
        assert!(is_scan_operator("ParquetExec"));
        assert!(is_scan_operator("DeltaScan"));
        assert!(is_scan_operator("CsvExec: ..."));
        assert!(!is_scan_operator("FilterExec"));
        assert!(!is_scan_operator("ProjectionExec"));
    }
}
```

### Files to Edit

- `rust/datafusion_python/src/common/data_type.rs` — add `#[cfg(test)] mod tests` with round-trip tests
- `rust/codeanatomy_engine/src/executor/metrics_collector.rs` — add `#[cfg(test)] mod tests` for `is_scan_operator`

### New Files to Create

None (inline test modules).

### Legacy Decommission/Delete Scope

None.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S7, S8)

- Delete `rust/datafusion_python/src/codeanatomy_ext/legacy.rs` entirely — all `#[pyfunction]` implementations moved to submodules by S8, shared helpers moved to `helpers.rs` by S7.
- Delete all `super::legacy::*` re-export patterns from `plugin_bridge.rs`, `udf_registration.rs`, `cache_tables.rs`, `session_utils.rs`.

### Batch D2 (after S3, S4)

- Delete both `map_pushdown_mode` functions after S3 enum unification.
- Delete triple-None `WriteOutcome` struct after S4 type safety migration.

### Batch D3 (after S10, S11)

- Delete `signature.rs` dead code after S11 audit.
- Delete hand-written boolean expression structs from `bool_expr.rs` after S10 macro introduction.

---

## Implementation Sequence

1. **S1 — Correctness Bug Fixes** — Zero risk, zero dependencies, immediate value. Fix all error messages, `todo!()`, and duplicate registrations.

2. **S2 — Compiler Helper Consolidation** — Small effort, eliminates 8 duplicated functions. No dependencies on other scopes.

3. **S3 — PushdownEnforcementMode Unification** — Small effort, eliminates dual enums + 2 mapping functions. Independent of S2.

4. **S4 — WriteOutcome Type Safety** — Small effort, improves type safety. Independent of S2/S3.

5. **S5 — DataFusion Extensions DRY Fixes** — Small effort, eliminates `span_struct_type` dup and `snapshot_payload` collision. Independent.

6. **S7 — codeanatomy_ext Helper Consolidation** — Small effort, prerequisite for S8. Must land before S8 begins.

7. **S6 — RegistrySnapshot Encapsulation** — Small-medium effort. Best done before or during S8 since S8 will touch the same `legacy.rs` code.

8. **S9 — Error Helper Consolidation** — Small effort. Can be done anytime.

9. **S8 — legacy.rs Full Decomposition** — Large effort, depends on S7 (helpers consolidated). This is the highest-impact structural change.

10. **S10 — Expression Boilerplate Reduction** — Medium effort. Independent of engine changes, can parallel S8.

11. **S11 — Dead Code Cleanup** — Small effort. Best after S8 and S10 when code ownership is clearer.

12. **S12 — Tracing Coverage** — Small effort, best after S8 so tracing lands on decomposed modules.

13. **S13 — Overlay Sort Generics** — Small effort. Independent.

14. **S14 — Test Coverage** — Can be done at any time, but best after S1 (ensures the bug fixes are verified by tests).

**Parallel tracks:**
- Track A (engine): S1 → S2 → S3 → S4 → S12 → S13
- Track B (datafusion_ext): S5 → S6
- Track C (python bindings): S7 → S8 → S9 → S11
- Track D (upstream bindings): S10 → S14

## Implementation Checklist

- [ ] S1 — Correctness Bug Fixes (5 files, ~30 minutes)
- [ ] S2 — Compiler Helper Consolidation (7 files + 1 new, ~2 hours)
- [ ] S3 — PushdownEnforcementMode Unification (4 files, ~1 hour)
- [ ] S4 — WriteOutcome Type Safety (1 file + consumers, ~1 hour)
- [ ] S5 — DataFusion Extensions DRY Fixes (4 files, ~1 hour)
- [ ] S6 — RegistrySnapshot Encapsulation (4 files, ~2 hours)
- [ ] S7 — codeanatomy_ext Helper Consolidation (5 files, ~1 hour)
- [ ] S8 — legacy.rs Full Decomposition (10+ files, ~6 hours)
- [ ] S9 — Error Helper Consolidation (3 files, ~1 hour)
- [ ] S10 — Expression Boilerplate Reduction (2 files, ~4 hours)
- [ ] S11 — Dead Code Cleanup (4 files, ~1 hour)
- [ ] S12 — Tracing Coverage (5+ files, ~2 hours)
- [ ] S13 — Overlay Sort Generics (2 files, ~2 hours)
- [ ] S14 — Upstream Expression Test Coverage (2 files, ~3 hours)
- [ ] D1 — Delete legacy.rs (after S7, S8)
- [ ] D2 — Delete mapping functions and old WriteOutcome (after S3, S4)
- [ ] D3 — Delete dead code and old bool_expr structs (after S10, S11)
