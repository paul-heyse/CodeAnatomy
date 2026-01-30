# Rust Codebase Consolidation Plan (Best-in-Class)

This document defines a **best-in-class** consolidation strategy for the CodeAnatomy Rust codebase (`rust/`). It is intentionally opinionated, embraces breaking changes, and prioritizes a clean architecture over short-term convenience.

Key goals:
- **Single authoritative source** for shared Rust logic
- **Clean dependency graph** (no cycles, no ambiguous ownership)
- **Best-practice DataFusion UDF architecture** (config-driven, metadata-aware, fast scalar paths)
- **Lean, stable PyO3 boundary** (no duplication, no drift)

---

## Table of Contents

1. [Workspace + Dependency Graph Normalization](#1-workspace--dependency-graph-normalization)
2. [Core vs PyO3 Layering (Crate Split)](#2-core-vs-pyo3-layering-crate-split)
3. [Critical File Duplication Elimination (Identical Modules)](#3-critical-file-duplication-elimination-identical-modules)
4. [Divergent UDF Core Consolidation](#4-divergent-udf-core-consolidation)
5. [PyO3 Module Consolidation](#5-pyo3-module-consolidation)
6. [Async Runtime + Async UDF Policy Consolidation](#6-async-runtime--async-udf-policy-consolidation)
7. [Error Handling Standardization](#7-error-handling-standardization)
8. [Delta Lake Integration Consolidation](#8-delta-lake-integration-consolidation)
9. [PyO3/PyArrow Utilities Consolidation (Low Priority)](#9-pyo3pyarrow-utilities-consolidation-low-priority)
10. [Cross-Scope Dependencies](#10-cross-scope-dependencies)
11. [Implementation Roadmap](#11-implementation-roadmap)
12. [Verification Steps](#12-verification-steps)
13. [Summary Statistics](#13-summary-statistics)

---

## 1. Workspace + Dependency Graph Normalization

### Architecture Overview

The `rust/` directory is **not a workspace** today. Each crate builds in isolation, and dependency versions drift. This prevents a deterministic build matrix and makes the dependency graph fragile.

### Current State

- No `rust/Cargo.toml` workspace.
- `datafusion_ext` depends on **external** `datafusion-python` crate, while local `datafusion_python` **duplicates** logic from `datafusion_ext`.
- `datafusion_python/Cargo.toml` is generated from `Cargo.toml.orig`, so direct edits are not stable.

#### Evidence

`rust/datafusion_ext/Cargo.toml`:
```toml
datafusion-python = "51.0.0"  # external dependency
```

Workspace file does not exist:
```
rust/Cargo.toml (missing)
```

### Target Implementation

#### Create a workspace

```toml
# rust/Cargo.toml
[workspace]
resolver = "2"
members = [
    "datafusion_ext",
    "datafusion_ext_py",
    "datafusion_python",
    "df_plugin_api",
    "df_plugin_host",
    "df_plugin_codeanatomy",
]

[workspace.dependencies]
arrow = "57.1.0"
arrow-ipc = "57.1.0"
async-trait = "0.1"
blake2 = "0.10"
chrono = "0.4"
datafusion = { version = "51.0.0", default-features = false, features = ["parquet"] }
datafusion-common = "51.0.0"
datafusion-expr = "51.0.0"
datafusion-expr-common = "51.0.0"
datafusion-functions = "51.0.0"
datafusion-functions-aggregate = "51.0.0"
datafusion-functions-nested = "51.0.0"
datafusion-functions-table = "51.0.0"
datafusion-functions-window = "51.0.0"
datafusion-functions-window-common = "51.0.0"
datafusion-ffi = "51.0.0"
object_store = "0.12.5"
pyo3 = "0.26"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1.49.0", features = ["rt-multi-thread"] }
```

#### Use `Cargo.toml.orig` as the editable source

All changes to `datafusion_python` dependencies must go in:
- `rust/datafusion_python/Cargo.toml.orig`

Regenerate `rust/datafusion_python/Cargo.toml` afterward using Cargo or a script.

### Target File List

**Create:**
- `rust/Cargo.toml`
- `rust/datafusion_ext_py/Cargo.toml`
- `rust/datafusion_ext_py/src/lib.rs`

**Modify:**
- `rust/datafusion_ext/Cargo.toml`
- `rust/datafusion_python/Cargo.toml.orig`
- `rust/datafusion_python/src/lib.rs` (stop registering CodeAnatomy PyO3 surface)
- `rust/df_plugin_codeanatomy/Cargo.toml`
- `rust/df_plugin_host/Cargo.toml` (if version alignment needed)

### Implementation Checklist

- [x] Create `rust/Cargo.toml` workspace with all members.
- [x] Add `[workspace.dependencies]` to align versions.
- [x] Remove external `datafusion-python` dependency from `datafusion_ext`.
- [x] Add `datafusion_ext` dependency to `datafusion_python/Cargo.toml.orig`.
- [x] Add `datafusion-python` dependency to `datafusion_ext_py/Cargo.toml` (for `PySessionContext` / `PyExpr`).
- [x] Regenerate `rust/datafusion_python/Cargo.toml` from `Cargo.toml.orig`.
- [x] Verify `cargo build -p datafusion_ext` works from workspace root.

### Decommissioning List

- Remove `datafusion-python = "51.0.0"` from `rust/datafusion_ext/Cargo.toml`.

---

## 2. Core vs PyO3 Layering (Crate Split)

### Architecture Overview

The **best-in-class** design is a hard separation between **core Rust logic** and **PyO3 bindings**:

- `datafusion_ext` becomes **pure Rust core** (no PyO3 dependency).
- `datafusion_ext_py` becomes the **PyO3 adapter layer** and depends on `datafusion-python` for existing PyO3 types.
- `datafusion_python` no longer registers the CodeAnatomy PyO3 surface by default (breaking change); the new extension lives in `datafusion_ext_py`.

This eliminates cycles, minimizes compile time, and prevents PyO3 features from leaking into core code.

### Current State

- `datafusion_ext` mixes core logic with PyO3 exports and Python helper logic.
- `datafusion_python/src/codeanatomy_ext.rs` is a large duplication of `datafusion_ext/src/lib.rs` with drift.

### Target Implementation

#### New crate layout

```
rust/
├── datafusion_ext/            # core (no PyO3)
├── datafusion_ext_py/         # PyO3 adapter
└── datafusion_python/         # thin Python wrapper
```

#### `datafusion_ext_py/src/lib.rs` (new)

```rust
#[pymodule]
fn datafusion_ext(py: pyo3::Python<'_>, module: &pyo3::Bound<'_, pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    datafusion_python::codeanatomy_ext::init_module(py, module)
}
```

`datafusion_python/src/codeanatomy_ext.rs` remains the canonical implementation, but it is no longer registered in `_internal` by default.

### Target File List

**Create:**
- `rust/datafusion_ext_py/Cargo.toml`
- `rust/datafusion_ext_py/src/lib.rs`

**Modify:**
- `rust/datafusion_ext/src/lib.rs` (remove all PyO3 exports)
- `rust/datafusion_python/src/codeanatomy_ext.rs` (canonical PyO3 implementation)
- `rust/datafusion_python/src/lib.rs` (stop registering CodeAnatomy PyO3 surface)

### Implementation Checklist

- [x] Create `datafusion_ext_py` crate with PyO3 dependency.
- [x] Ensure `datafusion_ext` is core-only (no PyO3 exports).
- [x] Keep CodeAnatomy PyO3 surface in `datafusion_python/src/codeanatomy_ext.rs`.
- [x] Register CodeAnatomy PyO3 surface via `datafusion_ext_py` only.
- [x] Remove `pyo3` dependency from `datafusion_ext`.

### Decommissioning List

- Remove PyO3 exports from `rust/datafusion_ext/src/lib.rs`.
- Retain `rust/datafusion_python/src/codeanatomy_ext.rs` as the canonical PyO3 surface (no deletion).

---

## 3. Critical File Duplication Elimination (Identical Modules)

### Architecture Overview

There are **13 identical files** duplicated across `datafusion_ext` and `datafusion_python` totaling **5,632 lines per side** (11,264 duplicated lines total). This is the single largest duplication block.

### Current State: Identical Files

| File | Lines | Status | Total Duplicated |
|------|-------|--------|------------------|
| `delta_control_plane.rs` | 515 | IDENTICAL | 1,030 |
| `delta_maintenance.rs` | 430 | IDENTICAL | 860 |
| `delta_mutations.rs` | 437 | IDENTICAL | 874 |
| `delta_observability.rs` | 224 | IDENTICAL | 448 |
| `delta_protocol.rs` | 144 | IDENTICAL | 288 |
| `function_factory.rs` | 923 | IDENTICAL | 1,846 |
| `function_rewrite.rs` | 108 | IDENTICAL | 216 |
| `udf_async.rs` | 259 | IDENTICAL | 518 |
| `udaf_builtin.rs` | 1,906 | IDENTICAL | 3,812 |
| `udtf_external.rs` | 457 | IDENTICAL | 914 |
| `udf_docs.rs` | 156 | IDENTICAL | 312 |
| `expr_planner.rs` | 59 | IDENTICAL | 118 |
| `udwf_builtin.rs` | 14 | IDENTICAL | 28 |

### Target Implementation

**Strategy:** `datafusion_ext` (core) becomes the **only** owner of these modules. `datafusion_python` must import from `datafusion_ext` (or from `datafusion_ext_py` where PyO3 exposure is required).

#### `rust/datafusion_python/src/lib.rs` (module decl removal + re-exports)

```rust
pub use datafusion_ext::{
    delta_control_plane,
    delta_maintenance,
    delta_mutations,
    delta_observability,
    delta_protocol,
    expr_planner,
    function_factory,
    function_rewrite,
    udaf_builtin,
    udf_async,
    udf_docs,
    udtf_external,
    udwf_builtin,
};
```

### Target File List

**Delete from `datafusion_python/src/`:**
- `delta_control_plane.rs`
- `delta_maintenance.rs`
- `delta_mutations.rs`
- `delta_observability.rs`
- `delta_protocol.rs`
- `function_factory.rs`
- `function_rewrite.rs`
- `udf_async.rs`
- `udaf_builtin.rs`
- `udtf_external.rs`
- `udf_docs.rs`
- `expr_planner.rs`
- `udwf_builtin.rs`

**Modify:**
- `rust/datafusion_python/src/lib.rs` (re-exports)
- `rust/datafusion_ext/src/lib.rs` (make modules `pub`)

### Implementation Checklist

- [x] Make duplicated modules `pub mod` in `datafusion_ext`.
- [x] Update imports in `datafusion_python` to reference `datafusion_ext`.
- [x] Delete identical module files from `datafusion_python`.
- [x] Build `datafusion_python` and `df_plugin_codeanatomy` to ensure no regressions.

### Decommissioning List

Delete the 13 duplicated module files listed above.

---

## 4. Divergent UDF Core Consolidation

### Architecture Overview

Four key UDF-related modules have **significant divergence** between `datafusion_ext` and `datafusion_python`. Best-in-class consolidation requires a **superset** implementation that preserves:

- Config-driven UDFs (`with_updated_config` + config defaults)
- Scalar fast-paths (`ColumnarValue::Scalar` handling)
- Metadata-aware typing (`return_field_from_args`)
- Rich type parsing and user-defined signatures
- Accurate optimizer metadata (`simplify`, `short_circuits`, `coerce_types`)

### Current State: Divergent Files

| File | Divergence | Best-in-class direction |
|------|------------|-------------------------|
| `udf_custom.rs` | config-driven logic vs richer dtype parser | **Merge both** (superset) |
| `udf_registry.rs` | async gating vs minimal registry | **Keep async gating** |
| `registry_snapshot.rs` | missing flags/config defaults | **Keep full metadata** |
| `udtf_builtin.rs` | missing metadata columns | **Match snapshot** |

### Target Implementation

#### 1) `udf_custom.rs` (superset)

- Preserve config-driven defaults (`CodeAnatomyUdfConfig`, `with_updated_config`).
- Preserve scalar fast paths (avoid unnecessary array expansion).
- Merge richer type parsing from `datafusion_python` (list/struct/map/decimal parsing).

Example: keep config-driven behavior **and** improved dtype parsing:

```rust
fn dtype_from_str(value: &str) -> Result<DataType> {
    let normalized = normalize_type(value)?;
    parse_type_signature(&normalized)
        .map_err(|err| DataFusionError::Plan(format!("Unsupported dtype: {value} ({err})")))
}
```

#### 2) `registry_snapshot.rs`

Keep the **superset** fields and ensure they are populated:
- `simplify`
- `coerce_types`
- `short_circuits`
- `config_defaults`

#### 3) `udtf_builtin.rs`

Include columns for `simplify`, `coerce_types`, and `short_circuits` to match `registry_snapshot`.

#### 4) `udf_registry.rs`

Preserve async gating (`all_udfs_with_async`) and explicit feature checks.

### Target File List

**Authoritative source:**
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_registry.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `rust/datafusion_ext/src/udtf_builtin.rs`

**Create/retain in `datafusion_python/src/`:**
- `udf_custom_py.rs` (PyO3 wrapper delegating to core `udf_custom`)

**Delete from `datafusion_python/src/`:**
- `udf_custom.rs`
- `udf_registry.rs`
- `registry_snapshot.rs`
- `udtf_builtin.rs`

### Implementation Checklist

- [x] Merge dtype parsing improvements from `datafusion_python` into `datafusion_ext/udf_custom.rs`.
- [x] Retain config-driven defaults + scalar fast paths in `udf_custom.rs`.
- [x] Ensure `registry_snapshot` records `simplify`, `coerce_types`, `short_circuits`, and `config_defaults`.
- [x] Update `udtf_builtin` schema/record batches to include those fields.
- [x] Replace `datafusion_python` registry/snapshot/UDTF modules with `datafusion_ext` re-exports, keeping a thin PyO3 wrapper for `udf_custom`.

### Decommissioning List

Delete the four divergent modules from `datafusion_python/src/` after merging:
- `udf_custom.rs` (replaced by `udf_custom_py.rs`)
- `udf_registry.rs`
- `registry_snapshot.rs`
- `udtf_builtin.rs`

---

## 5. PyO3 Module Consolidation

### Architecture Overview

`datafusion_ext/src/lib.rs` and `datafusion_python/src/codeanatomy_ext.rs` are functionally overlapping PyO3 surfaces with drift. This is a high-risk duplication point.

### Current State

- Two large PyO3 entrypoints expose overlapping APIs.
- `datafusion_python` lacks metadata fields (`simplify`, `coerce_types`, `short_circuits`, `config_defaults`).
- Config installation helpers diverge.

### Target Implementation

- Move PyO3 module registration into `datafusion_ext_py`.
- Export **one** PyO3 entrypoint in `datafusion_ext_py` that delegates to `datafusion_python::codeanatomy_ext::init_module`.
- Ensure PyO3 snapshot output includes full registry metadata.

### Target File List

**Create:**
- `rust/datafusion_ext_py/src/lib.rs` (PyO3 module)

**Modify:**
- `rust/datafusion_python/src/codeanatomy_ext.rs` (canonical PyO3 implementation)
- `rust/datafusion_ext/src/lib.rs` (remove PyO3)

### Implementation Checklist

- [x] Move PyO3 functions from `datafusion_ext` into `datafusion_ext_py`.
- [x] Restore registry snapshot fields in Python module output.
- [x] Keep canonical PyO3 surface in `datafusion_python` and stop registering it in `_internal`.

### Decommissioning List

- Remove `#[pymodule]` and all `#[pyfunction]` exports from `datafusion_ext`.
- No deletion in `datafusion_python/src/codeanatomy_ext.rs` (canonical surface retained).

---

## 6. Async Runtime + Async UDF Policy Consolidation

### Architecture Overview

Async runtimes are currently initialized in **three places**:
- `datafusion_python/src/utils.rs`
- `df_plugin_codeanatomy/src/lib.rs`
- `datafusion_ext/src/udf_async.rs`

Best practice is a **single runtime module** in `datafusion_ext`, with explicit policy for async UDFs and Python signal handling.

### Target Implementation

#### `datafusion_ext/src/async_runtime.rs`

```rust
use std::future::Future;
use std::sync::OnceLock;
use tokio::runtime::{Handle, Runtime};

static SHARED_RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn shared_runtime() -> &'static Runtime {
    SHARED_RUNTIME.get_or_init(|| Runtime::new().expect("shared tokio runtime"))
}

pub fn runtime_handle() -> Handle {
    Handle::try_current().unwrap_or_else(|_| shared_runtime().handle().clone())
}

pub fn spawn<F, T>(future: F) -> tokio::task::JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    runtime_handle().spawn(future)
}
```

**Best-in-class policy alignment with DataFusion UDF guidance:**
- Async UDFs should run on the **query runtime** (use `Handle::current()` when possible).
- Use `spawn_blocking` for CPU-bound or blocking I/O.
- Use config-driven timeouts and batch sizes for async UDFs.

### Target File List

**Create:**
- `rust/datafusion_ext/src/async_runtime.rs`

**Modify:**
- `rust/datafusion_python/src/utils.rs`
- `rust/df_plugin_codeanatomy/src/lib.rs`
- `rust/datafusion_ext/src/udf_async.rs`

### Implementation Checklist

- [x] Create `async_runtime` module in `datafusion_ext`.
- [x] Replace local runtime singletons in `datafusion_python` and `df_plugin_codeanatomy`.
- [x] Update `udf_async` to use `runtime_handle()` and avoid spawning a separate runtime inside UDFs.
- [x] Preserve Python signal handling in `wait_for_future` using shared runtime.

### Decommissioning List

- Remove `static ASYNC_RUNTIME` from `df_plugin_codeanatomy`.
- Remove `static RUNTIME` from `datafusion_python::utils`.
- Remove internal runtime creation from `udf_async`.

---

## 7. Error Handling Standardization

### Architecture Overview

Error types should be centralized in `datafusion_ext` and extended by the PyO3 layer.

### Target Implementation

#### `datafusion_ext/src/errors.rs`

```rust
pub enum ExtError {
    DataFusion(Box<datafusion::error::DataFusionError>),
    Arrow(datafusion::arrow::error::ArrowError),
    Generic(String),
    Delta(String),
    Plugin(String),
}

pub type ExtResult<T> = std::result::Result<T, ExtError>;
```

### Target File List

**Create:**
- `rust/datafusion_ext/src/errors.rs`

**Modify:**
- `rust/datafusion_ext/src/lib.rs`
- `rust/datafusion_python/src/errors.rs`

### Implementation Checklist

- [x] Add `ExtError` in `datafusion_ext`.
- [x] Re-export in `datafusion_ext_py` for PyO3 conversions.
- [x] Extend `PyDataFusionError` to wrap `ExtError`.

### Decommissioning List

- Remove scattered conversions in Delta modules, use `ExtError` conversions instead.

---

## 8. Delta Lake Integration Consolidation

### Architecture Overview

Delta Lake modules are well-structured. After duplication removal, they should live **only** in `datafusion_ext` core and be used by PyO3 via `datafusion_ext_py`.

### Target File List

**Authoritative:**
- `rust/datafusion_ext/src/delta_control_plane.rs`
- `rust/datafusion_ext/src/delta_maintenance.rs`
- `rust/datafusion_ext/src/delta_mutations.rs`
- `rust/datafusion_ext/src/delta_observability.rs`
- `rust/datafusion_ext/src/delta_protocol.rs`

### Implementation Checklist

- [x] Ensure all Delta modules are `pub` in `datafusion_ext`.
- [x] Remove Delta modules from `datafusion_python`.
- [x] Confirm all Python bindings delegate through `datafusion_ext_py`.

### Decommissioning List

- None beyond duplicates removed in Section 3.

---

## 9. PyO3/PyArrow Utilities Consolidation (Low Priority)

### Architecture Overview

Utilities are scattered in `datafusion_python`. A dedicated module improves maintainability but is not critical to consolidation.

### Target Implementation

`rust/datafusion_python/src/pyo3_utils.rs` re-exports:

```rust
pub use crate::pyarrow_util::{scalar_to_pyarrow, FromPyArrow};
pub use crate::record_batch::{PyRecordBatch, PyRecordBatchStream};
pub use crate::utils::{py_obj_to_scalar_value, table_provider_from_pycapsule, validate_pycapsule};
pub use crate::common::data_type::PyScalarValue;
```

### Implementation Checklist

- [x] Add `pyo3_utils.rs`.
- [x] Re-export from `lib.rs` if needed.

### Decommissioning List

- None (pure refactor).

---

## 10. Cross-Scope Dependencies

| Decommission Target | Required Scopes | Notes |
|--------------------|-----------------|-------|
| External `datafusion-python` dependency | #1, #2 | Must resolve before module moves |
| PyO3 exports in `datafusion_ext` | #2, #5 | Requires new `datafusion_ext_py` |
| Duplicate modules in `datafusion_python` | #3, #4 | Depends on public modules in `datafusion_ext` |
| Divergent UDF logic | #4 | Must be merged before deletions |
| Async runtime singletons | #6 | Requires shared runtime module |

---

## 11. Implementation Roadmap

### Phase 0: Workspace and Dependency Alignment

- Create `rust/Cargo.toml`
- Align dependency versions via workspace dependencies
- Remove external `datafusion-python` dependency

### Phase 1: Crate Split (Core vs PyO3)

- Create `datafusion_ext_py`
- Move PyO3 exports to `datafusion_ext_py`
- Make `datafusion_ext` pure core

### Phase 2: Divergent UDF Merge

- Merge `udf_custom.rs` (config + dtype parsing)
- Align `registry_snapshot` and `udtf_builtin`
- Preserve async gating in `udf_registry`

### Phase 3: Identical Module Deduplication

- Delete 13 identical modules from `datafusion_python`
- Re-export from `datafusion_ext`

### Phase 4: Async Runtime + Errors

- Add `async_runtime` module
- Replace runtime singletons
- Add `errors.rs` core error type

### Phase 5: Cleanup + Final Polish

- Ensure Python snapshot metadata includes all fields
- Normalize imports and module visibility
- Final lint/test pass

---

## 12. Verification Steps

After completing all phases:

- [x] `cargo build -p datafusion_ext`
- [x] `cargo build -p datafusion_ext_py`
- [x] `cargo build -p datafusion-python`
- [x] `cargo test -p datafusion_ext`
- [x] `cargo test -p df_plugin_codeanatomy`
- [x] Python smoke test: `uv run python -c "from datafusion._internal import *"`

---

## 13. Summary Statistics

| Metric | Before | After (Target) | Reduction |
|--------|--------|----------------|-----------|
| Identical duplicated lines | ~11,264 | 0 | 11,264 |
| Divergent UDF duplication | 4 modules | 0 | 4 modules |
| PyO3 entrypoints | 2 | 1 | 1 |
| Crate dependency cycles | Yes | No | Clean |
| Async runtime singletons | 3 | 1 | 2 |
