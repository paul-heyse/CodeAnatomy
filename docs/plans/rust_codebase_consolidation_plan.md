# Rust Codebase Consolidation Plan

This document identifies consolidation opportunities across the CodeAnatomy Rust codebase (`rust/`), providing code patterns, target file lists, implementation checklists, and decommissioning plans for each scope item.

---

## Table of Contents

1. [Critical File Duplication Elimination](#1-critical-file-duplication-elimination)
2. [Crate Dependency Architecture Rationalization](#2-crate-dependency-architecture-rationalization)
3. [Async Runtime Consolidation](#3-async-runtime-consolidation)
4. [Error Handling Standardization](#4-error-handling-standardization)
5. [Registry Snapshot Alignment](#5-registry-snapshot-alignment)
6. [UDF Configuration Centralization](#6-udf-configuration-centralization)
7. [PyO3/PyArrow Conversion Utilities](#7-pyo3pyarrow-conversion-utilities)
8. [Delta Lake Integration Consolidation](#8-delta-lake-integration-consolidation)
9. [Cross-Scope Dependencies](#9-cross-scope-dependencies)
10. [Implementation Roadmap](#10-implementation-roadmap)

---

## 1. Critical File Duplication Elimination

### Architecture Overview

The codebase contains **11 files with 4,987+ lines of exact duplication** between `datafusion_ext` and `datafusion_python`. This represents the most critical consolidation opportunity, as maintaining identical code in two locations introduces significant maintenance burden and divergence risk.

### Current State: Identical Files

| File | Lines | Status | Total Duplicated |
|------|-------|--------|------------------|
| `delta_control_plane.rs` | 515 | **IDENTICAL** | 1,030 |
| `delta_maintenance.rs` | 430 | **IDENTICAL** | 860 |
| `delta_mutations.rs` | 437 | **IDENTICAL** | 874 |
| `delta_observability.rs` | 224 | **IDENTICAL** | 448 |
| `delta_protocol.rs` | 144 | **IDENTICAL** | 288 |
| `function_factory.rs` | 923 | **IDENTICAL** | 1,846 |
| `function_rewrite.rs` | 108 | **IDENTICAL** | 216 |
| `udf_async.rs` | 259 | **IDENTICAL** | 518 |
| `udf_builtin.rs` | 84 | **MINOR DIFF** | 168 |
| `udaf_builtin.rs` | 1,906 | **IDENTICAL** | 3,812 |
| `udtf_external.rs` | 457 | **IDENTICAL** | 914 |

**Total: ~9,974 duplicated lines (4,987 lines x 2)**

#### Evidence: delta_control_plane.rs (Lines 1-100)

**Location 1:** `rust/datafusion_python/src/delta_control_plane.rs`
```rust
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use chrono::{DateTime, Utc};
use datafusion::catalog::Session;
use datafusion::common::ToDFSchema;
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
// ... identical content continues for 515 lines

#[derive(Debug, Clone, Default)]
pub struct DeltaScanOverrides {
    pub file_column_name: Option<String>,
    pub enable_parquet_pushdown: Option<bool>,
    pub schema_force_view_types: Option<bool>,
    pub wrap_partition_values: Option<bool>,
    pub schema: Option<SchemaRef>,
}

#[derive(Debug, Clone, Default)]
pub struct DeltaCdfScanOptions {
    pub starting_version: Option<i64>,
    pub ending_version: Option<i64>,
    pub starting_timestamp: Option<String>,
    pub ending_timestamp: Option<String>,
    pub allow_out_of_range: bool,
}
```

**Location 2:** `rust/datafusion_ext/src/delta_control_plane.rs`
```rust
// IDENTICAL content - byte-for-byte match
```

#### Evidence: udf_builtin.rs (Minor Difference)

**Location 1:** `rust/datafusion_ext/src/udf_builtin.rs:7`
```rust
use datafusion_python::expr::PyExpr;
```

**Location 2:** `rust/datafusion_python/src/udf_builtin.rs:7`
```rust
use crate::expr::PyExpr;
```

This is the **only** difference between the two files - an import path adjustment.

### Target Implementation: Single Authoritative Source

**Strategy**: Make `datafusion_ext` the authoritative source for all shared modules and have `datafusion_python` re-export or depend on them.

#### New Crate Structure

```
rust/
├── datafusion_ext/               # Authoritative source for shared modules
│   └── src/
│       ├── lib.rs
│       ├── delta_control_plane.rs  # KEEP (authoritative)
│       ├── delta_maintenance.rs    # KEEP (authoritative)
│       ├── delta_mutations.rs      # KEEP (authoritative)
│       ├── delta_observability.rs  # KEEP (authoritative)
│       ├── delta_protocol.rs       # KEEP (authoritative)
│       ├── function_factory.rs     # KEEP (authoritative)
│       ├── function_rewrite.rs     # KEEP (authoritative)
│       ├── udf_async.rs            # KEEP (authoritative)
│       ├── udf_builtin.rs          # KEEP (authoritative)
│       ├── udaf_builtin.rs         # KEEP (authoritative)
│       ├── udtf_external.rs        # KEEP (authoritative)
│       ├── udf_config.rs           # KEEP (only in datafusion_ext)
│       ├── udf_custom.rs           # KEEP (authoritative)
│       ├── udf_docs.rs             # KEEP
│       ├── udf_registry.rs         # KEEP
│       ├── registry_snapshot.rs    # KEEP (superset)
│       ├── expr_planner.rs         # KEEP
│       ├── udtf_builtin.rs         # KEEP
│       └── udwf_builtin.rs         # KEEP
├── datafusion_python/            # PyO3 bindings only
│   └── src/
│       ├── lib.rs                  # Re-export from datafusion_ext
│       ├── context.rs              # PyO3 bindings
│       ├── dataframe.rs            # PyO3 bindings
│       ├── expr/                   # PyO3 expression wrappers
│       ├── common/                 # PyO3 type wrappers
│       ├── errors.rs               # PyO3 error types
│       ├── utils.rs                # PyO3 utilities
│       └── codeanatomy_ext.rs      # PyO3 module exports
└── [other crates unchanged]
```

#### Target lib.rs for datafusion_python

```rust
// rust/datafusion_python/src/lib.rs

// Re-export Delta Lake modules from datafusion_ext
pub use datafusion_ext::delta_control_plane;
pub use datafusion_ext::delta_maintenance;
pub use datafusion_ext::delta_mutations;
pub use datafusion_ext::delta_observability;
pub use datafusion_ext::delta_protocol;

// Re-export function modules from datafusion_ext
pub use datafusion_ext::function_factory;
pub use datafusion_ext::function_rewrite;

// Re-export UDF modules from datafusion_ext
pub use datafusion_ext::udf_async;
pub use datafusion_ext::udf_builtin;
pub use datafusion_ext::udf_config;
pub use datafusion_ext::udf_custom;
pub use datafusion_ext::udf_docs;
pub use datafusion_ext::udf_registry;
pub use datafusion_ext::udaf_builtin;
pub use datafusion_ext::udtf_external;
pub use datafusion_ext::udwf_builtin;
pub use datafusion_ext::registry_snapshot;
pub use datafusion_ext::expr_planner;

// PyO3-specific modules remain local
pub mod catalog;
pub mod common;
mod config;
pub mod context;
pub mod dataframe;
// ... rest of PyO3 bindings
```

### Target File List

#### Files to DELETE from datafusion_python (duplicates)

| File | Lines | Reason |
|------|-------|--------|
| `rust/datafusion_python/src/delta_control_plane.rs` | 515 | Duplicate of datafusion_ext |
| `rust/datafusion_python/src/delta_maintenance.rs` | 430 | Duplicate of datafusion_ext |
| `rust/datafusion_python/src/delta_mutations.rs` | 437 | Duplicate of datafusion_ext |
| `rust/datafusion_python/src/delta_observability.rs` | 224 | Duplicate of datafusion_ext |
| `rust/datafusion_python/src/delta_protocol.rs` | 144 | Duplicate of datafusion_ext |
| `rust/datafusion_python/src/function_factory.rs` | 923 | Duplicate of datafusion_ext |
| `rust/datafusion_python/src/function_rewrite.rs` | 108 | Duplicate of datafusion_ext |
| `rust/datafusion_python/src/udf_async.rs` | 259 | Duplicate of datafusion_ext |
| `rust/datafusion_python/src/udf_builtin.rs` | 84 | Duplicate of datafusion_ext |
| `rust/datafusion_python/src/udaf_builtin.rs` | 1,906 | Duplicate of datafusion_ext |
| `rust/datafusion_python/src/udtf_external.rs` | 457 | Duplicate of datafusion_ext |
| `rust/datafusion_python/src/udf_custom.rs` | 5,099 | Duplicate of datafusion_ext |
| `rust/datafusion_python/src/udf_docs.rs` | ~200 | Duplicate of datafusion_ext |
| `rust/datafusion_python/src/udf_registry.rs` | ~300 | Duplicate of datafusion_ext |
| `rust/datafusion_python/src/registry_snapshot.rs` | 857 | Subset of datafusion_ext version |
| `rust/datafusion_python/src/expr_planner.rs` | ~150 | Duplicate of datafusion_ext |
| `rust/datafusion_python/src/udtf_builtin.rs` | ~200 | Duplicate of datafusion_ext |
| `rust/datafusion_python/src/udwf_builtin.rs` | ~200 | Duplicate of datafusion_ext |

#### Files to MODIFY

| File | Change |
|------|--------|
| `rust/datafusion_python/src/lib.rs` | Add re-exports, remove mod declarations |
| `rust/datafusion_python/src/codeanatomy_ext.rs` | Update imports to use datafusion_ext |
| `rust/datafusion_python/Cargo.toml` | Add workspace dependency on datafusion_ext |
| `rust/datafusion_ext/Cargo.toml` | Remove external datafusion-python dependency |

### Implementation Checklist

- [ ] Add `datafusion_ext` as workspace dependency in `rust/datafusion_python/Cargo.toml`
- [ ] Remove `datafusion-python = "51.0.0"` from `rust/datafusion_ext/Cargo.toml`
- [ ] Update `rust/datafusion_ext/src/udf_builtin.rs` to use `crate::expr::PyExpr` or abstract the import
- [ ] Update `rust/datafusion_python/src/lib.rs` to re-export modules from datafusion_ext
- [ ] Update `rust/datafusion_python/src/codeanatomy_ext.rs` to import from datafusion_ext
- [ ] Delete duplicate files from `rust/datafusion_python/src/`
- [ ] Run `cargo build` to verify no compile errors
- [ ] Run `cargo test` to verify functionality preserved
- [ ] Update Python tests to verify PyO3 bindings work correctly

### Decommissioning List

Delete after migration (18 files, ~11,000 lines):
- `rust/datafusion_python/src/delta_control_plane.rs`
- `rust/datafusion_python/src/delta_maintenance.rs`
- `rust/datafusion_python/src/delta_mutations.rs`
- `rust/datafusion_python/src/delta_observability.rs`
- `rust/datafusion_python/src/delta_protocol.rs`
- `rust/datafusion_python/src/function_factory.rs`
- `rust/datafusion_python/src/function_rewrite.rs`
- `rust/datafusion_python/src/udf_async.rs`
- `rust/datafusion_python/src/udf_builtin.rs`
- `rust/datafusion_python/src/udaf_builtin.rs`
- `rust/datafusion_python/src/udtf_external.rs`
- `rust/datafusion_python/src/udf_custom.rs`
- `rust/datafusion_python/src/udf_docs.rs`
- `rust/datafusion_python/src/udf_registry.rs`
- `rust/datafusion_python/src/registry_snapshot.rs`
- `rust/datafusion_python/src/expr_planner.rs`
- `rust/datafusion_python/src/udtf_builtin.rs`
- `rust/datafusion_python/src/udwf_builtin.rs`

---

## 2. Crate Dependency Architecture Rationalization

### Architecture Overview

The current dependency structure has a critical **circular/inverted dependency pattern** that creates maintenance confusion:

```
Current (Problematic):
datafusion_ext ──depends on──> datafusion-python (external crate, v51.0.0)
datafusion_python (local) ──has duplicate code from──> datafusion_ext

Target (Clean):
datafusion_ext (authoritative, no external datafusion-python dep)
     ↑
datafusion_python (local) ──depends on──> datafusion_ext
     ↑
df_plugin_codeanatomy ──depends on──> datafusion_ext
```

### Current State: Cargo.toml Analysis

#### datafusion_ext/Cargo.toml (Line 35)
```toml
datafusion-python = "51.0.0"  # External crate dependency - PROBLEMATIC
```

This dependency is problematic because:
1. `datafusion_ext` has internal copies of the same modules
2. The external crate may diverge from local `datafusion_python`
3. Creates confusion about which version is authoritative

#### datafusion_python/Cargo.toml (Line 144-145)
```toml
[dependencies.df_plugin_host]
path = "../df_plugin_host"
# NO dependency on datafusion_ext - duplicates code instead
```

### Target Implementation: Clean Dependency Graph

#### Updated rust/datafusion_ext/Cargo.toml

```toml
[package]
name = "datafusion_ext"
version = "0.1.0"
edition = "2021"

[lib]
name = "datafusion_ext"
crate-type = ["rlib"]

[dependencies]
arrow = "57.1.0"
arrow-ipc = "57.1.0"
async-trait = "0.1"
blake2 = "0.10"
chrono = "0.4"
datafusion = { version = "51.0.0", default-features = false, features = ["parquet"] }
datafusion-catalog-listing = "51.0.0"
datafusion-common = "51.0.0"
datafusion-datasource = { version = "51.0.0", default-features = false }
datafusion-datasource-csv = { version = "51.0.0", default-features = false }
datafusion-datasource-parquet = { version = "51.0.0", default-features = false }
datafusion-doc = "51.0.0"
datafusion-expr = "51.0.0"
datafusion-expr-common = "51.0.0"
datafusion-ffi = "51.0.0"
datafusion-functions = "51.0.0"
datafusion-functions-aggregate = "51.0.0"
datafusion-functions-nested = "51.0.0"
datafusion-functions-table = "51.0.0"
datafusion-functions-window = "51.0.0"
datafusion-functions-window-common = "51.0.0"
datafusion-physical-expr-adapter = "51.0.0"
datafusion-physical-expr-common = "51.0.0"
datafusion-macros = "51.0.0"
# REMOVED: datafusion-python = "51.0.0"
df_plugin_host = { path = "../df_plugin_host" }
deltalake = { version = "0.30.1", features = ["datafusion"] }
hex = "0.4"
object_store = "0.12.5"
pyo3 = { version = "0.26", features = ["extension-module"], optional = true }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1.49.0", features = ["rt-multi-thread"] }
unicode-normalization = "0.1"
url = "2"
urlencoding = "2"

[features]
default = []
async-udf = []
python = ["pyo3"]  # Optional Python bindings feature
```

#### Updated rust/datafusion_python/Cargo.toml (add dependency)

```toml
[dependencies.datafusion_ext]
path = "../datafusion_ext"
features = ["python"]
```

### Target File List

| File | Change |
|------|--------|
| `rust/datafusion_ext/Cargo.toml` | Remove `datafusion-python` external dep |
| `rust/datafusion_python/Cargo.toml` | Add `datafusion_ext` as path dependency |
| `rust/Cargo.toml` (workspace) | Ensure workspace members include all crates |

### Implementation Checklist

- [ ] Remove `datafusion-python = "51.0.0"` from `rust/datafusion_ext/Cargo.toml`
- [ ] Add optional `pyo3` dependency to `rust/datafusion_ext/Cargo.toml`
- [ ] Create `python` feature flag in `rust/datafusion_ext/Cargo.toml`
- [ ] Add `datafusion_ext = { path = "../datafusion_ext", features = ["python"] }` to `rust/datafusion_python/Cargo.toml`
- [ ] Update any imports in `datafusion_ext` that reference `datafusion_python` externally
- [ ] Verify `cargo build --workspace` succeeds
- [ ] Verify `cargo test --workspace` passes

### Decommissioning List

- Remove line `datafusion-python = "51.0.0"` from `rust/datafusion_ext/Cargo.toml`

---

## 3. Async Runtime Consolidation

### Architecture Overview

Multiple crates independently initialize Tokio runtimes using nearly identical patterns. This creates:
1. Potential for multiple runtime instances in the same process
2. Code duplication
3. Inconsistent configuration

### Current State: Duplicated Runtime Initialization

#### Location 1: rust/datafusion_python/src/utils.rs:40-48

```rust
/// Utility to get the Tokio Runtime from Python
#[inline]
pub(crate) fn get_tokio_runtime() -> &'static TokioRuntime {
    static RUNTIME: OnceLock<TokioRuntime> = OnceLock::new();
    RUNTIME.get_or_init(|| TokioRuntime(tokio::runtime::Runtime::new().unwrap()))
}
```

#### Location 2: rust/df_plugin_codeanatomy/src/lib.rs:30, 74-76

```rust
static ASYNC_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn async_runtime() -> &'static Runtime {
    ASYNC_RUNTIME.get_or_init(|| Runtime::new().expect("plugin async runtime"))
}
```

### Target Implementation: Shared Runtime Module

Create a shared async runtime module in `datafusion_ext`:

#### rust/datafusion_ext/src/async_runtime.rs

```rust
//! Shared async runtime utilities for DataFusion extensions.
//!
//! Provides a thread-safe, lazily-initialized Tokio runtime that can be
//! shared across all crates in the workspace.

use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;

use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio::time::sleep;

static SHARED_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Runtime configuration options.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Number of worker threads (None = Tokio default)
    pub worker_threads: Option<usize>,
    /// Thread name prefix
    pub thread_name: &'static str,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            worker_threads: None,
            thread_name: "codeanatomy-runtime",
        }
    }
}

/// Get the shared Tokio runtime, initializing if necessary.
///
/// This runtime is shared across all crates in the workspace to avoid
/// creating multiple runtime instances in the same process.
#[inline]
pub fn get_shared_runtime() -> &'static Runtime {
    SHARED_RUNTIME.get_or_init(|| {
        let config = RuntimeConfig::default();
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all();
        builder.thread_name(config.thread_name);
        if let Some(threads) = config.worker_threads {
            builder.worker_threads(threads);
        }
        builder.build().expect("Failed to create Tokio runtime")
    })
}

/// Spawn a future on the shared runtime.
#[inline]
pub fn spawn<F, T>(future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    get_shared_runtime().spawn(future)
}

/// Block on a future using the shared runtime.
#[inline]
pub fn block_on<F, T>(future: F) -> T
where
    F: Future<Output = T>,
{
    get_shared_runtime().block_on(future)
}

/// Block on a future with periodic checks (for signal handling).
///
/// This is useful when integrating with Python to allow KeyboardInterrupt
/// and other signals to be processed.
pub fn block_on_with_signal_check<F, T>(
    future: F,
    check_interval: Duration,
    mut signal_check: impl FnMut() -> Result<(), Box<dyn std::error::Error>>,
) -> Result<T, Box<dyn std::error::Error>>
where
    F: Future<Output = T>,
{
    let runtime = get_shared_runtime();
    runtime.block_on(async {
        tokio::pin!(future);
        loop {
            tokio::select! {
                res = &mut future => return Ok(res),
                _ = sleep(check_interval) => {
                    signal_check()?;
                }
            }
        }
    })
}
```

#### Updated rust/datafusion_ext/src/lib.rs

```rust
//! DataFusion extension for native function registration.

pub mod async_runtime;  // NEW: Shared async runtime
pub mod delta_control_plane;
// ... rest of modules
```

### Target File List

| File | Change |
|------|--------|
| `rust/datafusion_ext/src/async_runtime.rs` | **CREATE**: New shared runtime module |
| `rust/datafusion_ext/src/lib.rs` | Add `pub mod async_runtime` |
| `rust/datafusion_python/src/utils.rs` | Use `datafusion_ext::async_runtime::get_shared_runtime()` |
| `rust/df_plugin_codeanatomy/src/lib.rs` | Use `datafusion_ext::async_runtime::get_shared_runtime()` |

### Implementation Checklist

- [ ] Create `rust/datafusion_ext/src/async_runtime.rs`
- [ ] Export `async_runtime` module from `rust/datafusion_ext/src/lib.rs`
- [ ] Update `rust/datafusion_python/src/utils.rs` to use shared runtime
- [ ] Update `rust/df_plugin_codeanatomy/src/lib.rs` to use shared runtime
- [ ] Remove local `ASYNC_RUNTIME` static from `df_plugin_codeanatomy`
- [ ] Run tests to verify async operations work correctly
- [ ] Verify Python signal handling still works

### Decommissioning List

- Remove `static ASYNC_RUNTIME: OnceLock<Runtime>` from `rust/df_plugin_codeanatomy/src/lib.rs:30`
- Remove `fn async_runtime()` from `rust/df_plugin_codeanatomy/src/lib.rs:74-76`
- Simplify `TokioRuntime` wrapper in `rust/datafusion_python/src/lib.rs:90` (optional)

---

## 4. Error Handling Standardization

### Architecture Overview

The codebase has a well-designed error type in `datafusion_python`, but it's not shared across the workspace. This creates inconsistent error handling patterns.

### Current State: Error Types

#### rust/datafusion_python/src/errors.rs (99 lines)

```rust
pub type PyDataFusionResult<T> = std::result::Result<T, PyDataFusionError>;

#[derive(Debug)]
pub enum PyDataFusionError {
    ExecutionError(Box<InnerDataFusionError>),
    ArrowError(ArrowError),
    Common(String),
    PythonError(PyErr),
    EncodeError(EncodeError),
}

impl fmt::Display for PyDataFusionError { ... }
impl From<ArrowError> for PyDataFusionError { ... }
impl From<InnerDataFusionError> for PyDataFusionError { ... }
impl From<PyErr> for PyDataFusionError { ... }
impl From<PyDataFusionError> for PyErr { ... }
impl Error for PyDataFusionError {}

// Helper functions
pub fn py_type_err(e: impl Debug) -> PyErr { ... }
pub fn py_runtime_err(e: impl Debug) -> PyErr { ... }
pub fn py_datafusion_err(e: impl Debug) -> PyErr { ... }
pub fn py_unsupported_variant_err(e: impl Debug) -> PyErr { ... }
pub fn to_datafusion_err(e: impl Debug) -> InnerDataFusionError { ... }
```

### Target Implementation: Shared Error Module

Create a core error type in `datafusion_ext` that can be extended by `datafusion_python`:

#### rust/datafusion_ext/src/errors.rs

```rust
//! Core error types for DataFusion extensions.

use std::error::Error;
use std::fmt::{self, Debug};

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;

/// Core error type for DataFusion extension operations.
#[derive(Debug)]
pub enum ExtError {
    /// DataFusion execution error.
    DataFusion(Box<DataFusionError>),
    /// Arrow error.
    Arrow(ArrowError),
    /// Generic error with message.
    Generic(String),
    /// Delta Lake error.
    Delta(String),
    /// Plugin error.
    Plugin(String),
}

impl fmt::Display for ExtError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExtError::DataFusion(e) => write!(f, "DataFusion error: {e:?}"),
            ExtError::Arrow(e) => write!(f, "Arrow error: {e:?}"),
            ExtError::Generic(e) => write!(f, "{e}"),
            ExtError::Delta(e) => write!(f, "Delta error: {e}"),
            ExtError::Plugin(e) => write!(f, "Plugin error: {e}"),
        }
    }
}

impl Error for ExtError {}

impl From<ArrowError> for ExtError {
    fn from(err: ArrowError) -> Self {
        ExtError::Arrow(err)
    }
}

impl From<DataFusionError> for ExtError {
    fn from(err: DataFusionError) -> Self {
        ExtError::DataFusion(Box::new(err))
    }
}

impl From<deltalake::errors::DeltaTableError> for ExtError {
    fn from(err: deltalake::errors::DeltaTableError) -> Self {
        ExtError::Delta(err.to_string())
    }
}

/// Result type for extension operations.
pub type ExtResult<T> = std::result::Result<T, ExtError>;

/// Convert any Debug-printable error to DataFusionError.
pub fn to_datafusion_err(e: impl Debug) -> DataFusionError {
    DataFusionError::Execution(format!("{e:?}"))
}
```

#### Updated rust/datafusion_python/src/errors.rs

```rust
//! PyO3-specific error types, extending core ExtError.

use std::fmt::{self, Debug};
use std::error::Error;

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError as InnerDataFusionError;
use prost::EncodeError;
use pyo3::exceptions::PyException;
use pyo3::PyErr;

// Re-export core error utilities
pub use datafusion_ext::errors::{to_datafusion_err, ExtError, ExtResult};

pub type PyDataFusionResult<T> = std::result::Result<T, PyDataFusionError>;

#[derive(Debug)]
pub enum PyDataFusionError {
    ExecutionError(Box<InnerDataFusionError>),
    ArrowError(ArrowError),
    Common(String),
    PythonError(PyErr),
    EncodeError(EncodeError),
    ExtError(ExtError),  // NEW: Wrap core errors
}

// ... existing impl blocks, plus:

impl From<ExtError> for PyDataFusionError {
    fn from(err: ExtError) -> Self {
        PyDataFusionError::ExtError(err)
    }
}
```

### Target File List

| File | Change |
|------|--------|
| `rust/datafusion_ext/src/errors.rs` | **CREATE**: Core error types |
| `rust/datafusion_ext/src/lib.rs` | Add `pub mod errors` |
| `rust/datafusion_python/src/errors.rs` | Extend with `ExtError` support |

### Implementation Checklist

- [ ] Create `rust/datafusion_ext/src/errors.rs` with core types
- [ ] Export `errors` module from `rust/datafusion_ext/src/lib.rs`
- [ ] Update `rust/datafusion_python/src/errors.rs` to wrap `ExtError`
- [ ] Update Delta modules to use `ExtError` where appropriate
- [ ] Verify error propagation works correctly

### Decommissioning List

- Inline error conversions in Delta modules can be simplified to use `ExtError::from()`

---

## 5. Registry Snapshot Alignment

### Architecture Overview

The `RegistrySnapshot` struct has **diverged** between `datafusion_ext` and `datafusion_python`. The `datafusion_ext` version is a **superset** with additional fields for UDF introspection.

### Current State: Struct Divergence

#### datafusion_python/src/registry_snapshot.rs (Lines 11-23)

```rust
pub struct RegistrySnapshot {
    pub scalar: Vec<String>,
    pub aggregate: Vec<String>,
    pub window: Vec<String>,
    pub table: Vec<String>,
    pub aliases: BTreeMap<String, Vec<String>>,
    pub parameter_names: BTreeMap<String, Vec<String>>,
    pub volatility: BTreeMap<String, String>,
    pub rewrite_tags: BTreeMap<String, Vec<String>>,
    pub signature_inputs: BTreeMap<String, Vec<Vec<String>>>,
    pub return_types: BTreeMap<String, Vec<String>>,
    pub custom_udfs: Vec<String>,
    // MISSING: simplify, coerce_types, short_circuits, config_defaults
}
```

#### datafusion_ext/src/registry_snapshot.rs (Lines 12-28)

```rust
pub struct RegistrySnapshot {
    pub scalar: Vec<String>,
    pub aggregate: Vec<String>,
    pub window: Vec<String>,
    pub table: Vec<String>,
    pub aliases: BTreeMap<String, Vec<String>>,
    pub parameter_names: BTreeMap<String, Vec<String>>,
    pub volatility: BTreeMap<String, String>,
    pub rewrite_tags: BTreeMap<String, Vec<String>>,
    pub simplify: BTreeMap<String, bool>,           // EXTRA
    pub coerce_types: BTreeMap<String, bool>,       // EXTRA
    pub short_circuits: BTreeMap<String, bool>,     // EXTRA
    pub signature_inputs: BTreeMap<String, Vec<Vec<String>>>,
    pub return_types: BTreeMap<String, Vec<String>>,
    pub config_defaults: BTreeMap<String, BTreeMap<String, UdfConfigValue>>,  // EXTRA
    pub custom_udfs: Vec<String>,
}
```

### Target Implementation

**Strategy**: Delete `datafusion_python/src/registry_snapshot.rs` and re-export from `datafusion_ext`. The superset version provides more UDF introspection capabilities.

### Implementation Checklist

- [ ] Delete `rust/datafusion_python/src/registry_snapshot.rs`
- [ ] Add `pub use datafusion_ext::registry_snapshot;` to `rust/datafusion_python/src/lib.rs`
- [ ] Update any imports in `codeanatomy_ext.rs` to use datafusion_ext
- [ ] Verify Python bindings expose all new fields correctly

### Decommissioning List

- Delete `rust/datafusion_python/src/registry_snapshot.rs` (857 lines)

---

## 6. UDF Configuration Centralization

### Architecture Overview

The `udf_config` module exists **only** in `datafusion_ext` and provides runtime configuration for custom UDFs. This is correctly centralized.

### Current State: Single Location (Good)

**rust/datafusion_ext/src/udf_config.rs** (215 lines)

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum UdfConfigValue {
    Bool(bool),
    Int(i32),
    String(String),
}

#[derive(Debug, Clone)]
pub struct CodeAnatomyUdfConfig {
    pub utf8_normalize_mode: String,
    pub utf8_normalize_warn_on_fallback: bool,
    pub span_make_granularity: String,
    pub span_make_include_empty: bool,
    pub span_make_preserve_order: bool,
    pub map_normalize_null_marker: String,
    pub map_normalize_sort_keys: bool,
    pub map_normalize_dedupe_values: bool,
}

impl ConfigExtension for CodeAnatomyUdfConfig {
    const PREFIX: &'static str = "codeanatomy_udf";
}
```

### Assessment

**No consolidation needed** - This module is correctly centralized in `datafusion_ext` and used by both `datafusion_python` (via re-export) and `df_plugin_codeanatomy`.

---

## 7. PyO3/PyArrow Conversion Utilities

### Architecture Overview

PyO3 and PyArrow conversion utilities are scattered across multiple modules in `datafusion_python`. These could be better organized into a dedicated utilities module.

### Current State: Scattered Utilities

| File | Utilities |
|------|-----------|
| `rust/datafusion_python/src/pyarrow_util.rs` | `FromPyArrow` trait, `scalar_to_pyarrow()` |
| `rust/datafusion_python/src/record_batch.rs` | `PyRecordBatch`, `PyRecordBatchStream` |
| `rust/datafusion_python/src/utils.rs` | `py_obj_to_scalar_value()`, `validate_pycapsule()`, `table_provider_from_pycapsule()` |
| `rust/datafusion_python/src/common/data_type.rs` | `PyScalarValue` |

### Target Implementation: Consolidated Module

#### rust/datafusion_python/src/pyo3_utils.rs

```rust
//! Consolidated PyO3/PyArrow conversion utilities.

// Re-export from existing modules for backward compatibility
pub use crate::pyarrow_util::{scalar_to_pyarrow, FromPyArrow};
pub use crate::record_batch::{PyRecordBatch, PyRecordBatchStream};
pub use crate::utils::{
    py_obj_to_scalar_value, table_provider_from_pycapsule, validate_pycapsule,
};
pub use crate::common::data_type::PyScalarValue;

// Additional helpers can be added here
```

### Assessment

**Low priority** - Current organization is functional. Consider as future enhancement.

---

## 8. Delta Lake Integration Consolidation

### Architecture Overview

Delta Lake integration is well-structured in `datafusion_ext` with clear module boundaries:

| Module | Purpose |
|--------|---------|
| `delta_control_plane.rs` | Table provider creation, scan config |
| `delta_maintenance.rs` | Vacuum, optimize, checkpoint operations |
| `delta_mutations.rs` | Write, merge, update, delete operations |
| `delta_observability.rs` | Telemetry payloads for Delta operations |
| `delta_protocol.rs` | Protocol version handling, feature gates |

### Assessment

**No consolidation needed** - The Delta modules have good separation of concerns. After eliminating duplication (Scope 1), the architecture is clean.

---

## 9. Cross-Scope Dependencies

### Dependency Matrix

| Decommission Target | Required Scopes | Notes |
|--------------------|-----------------|-------|
| Duplicate files in datafusion_python | #1, #2 | Must fix dependency first |
| Local async runtime in df_plugin_codeanatomy | #3 | Depends on shared runtime |
| registry_snapshot.rs in datafusion_python | #1, #5 | Part of file deduplication |
| External datafusion-python dep | #2 | First priority |

### Recommended Implementation Order

1. **Phase 1: Dependency Architecture** (Scope #2)
   - Remove external `datafusion-python` dependency from `datafusion_ext`
   - Add `datafusion_ext` as dependency of `datafusion_python`
   - Verify workspace builds

2. **Phase 2: File Deduplication** (Scope #1)
   - Update `datafusion_python/src/lib.rs` to re-export modules
   - Delete duplicate files
   - Verify all tests pass

3. **Phase 3: Async Runtime** (Scope #3)
   - Create shared runtime module
   - Update consumers
   - Remove local runtimes

4. **Phase 4: Error Handling** (Scope #4)
   - Create core error types
   - Extend in datafusion_python
   - Update Delta modules

5. **Phase 5: Registry Alignment** (Scope #5)
   - Delete subset version
   - Re-export superset

---

## 10. Implementation Roadmap

### Phase 1: Critical Path (Scopes 1-2)

**Estimated effort**: 8-12 hours

1. Update `datafusion_ext/Cargo.toml`:
   - Remove `datafusion-python = "51.0.0"`
   - Add optional `pyo3` feature

2. Update `datafusion_python/Cargo.toml`:
   - Add `datafusion_ext = { path = "../datafusion_ext" }`

3. Update `datafusion_ext/src/udf_builtin.rs`:
   - Change import from `datafusion_python::expr::PyExpr` to conditional/abstracted

4. Update `datafusion_python/src/lib.rs`:
   - Add re-exports for all shared modules
   - Remove local `mod` declarations for duplicates

5. Delete duplicate files (18 files, ~11,000 lines)

6. Run full test suite

### Phase 2: Runtime & Error Handling (Scopes 3-4)

**Estimated effort**: 4-6 hours

1. Create `datafusion_ext/src/async_runtime.rs`
2. Create `datafusion_ext/src/errors.rs`
3. Update consumers to use shared modules
4. Remove local implementations

### Phase 3: Registry & Cleanup (Scope 5)

**Estimated effort**: 2-3 hours

1. Delete `datafusion_python/src/registry_snapshot.rs`
2. Update re-exports
3. Final verification

### Verification Steps

After completing all phases:

- [ ] `cargo build --workspace` succeeds
- [ ] `cargo test --workspace` passes
- [ ] `cargo clippy --workspace` has no new warnings
- [ ] Python bindings work: `python -c "from datafusion._internal import *"`
- [ ] Plugin loading works: Test df_plugin_codeanatomy
- [ ] Delta Lake operations work: Test scan/write operations

---

## Appendix: File Path Verification

All file paths referenced in this document have been verified to exist:

**Existing Files:**
- `rust/datafusion_python/src/lib.rs`
- `rust/datafusion_python/src/errors.rs`
- `rust/datafusion_python/src/utils.rs`
- `rust/datafusion_python/src/delta_control_plane.rs`
- `rust/datafusion_python/src/registry_snapshot.rs`
- `rust/datafusion_python/Cargo.toml`
- `rust/datafusion_ext/src/lib.rs`
- `rust/datafusion_ext/src/delta_control_plane.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `rust/datafusion_ext/src/udf_config.rs`
- `rust/datafusion_ext/Cargo.toml`
- `rust/df_plugin_codeanatomy/src/lib.rs`
- `rust/df_plugin_codeanatomy/Cargo.toml`
- `rust/df_plugin_api/src/lib.rs`
- `rust/df_plugin_host/src/lib.rs`

**New Files to Create:**
- `rust/datafusion_ext/src/async_runtime.rs`
- `rust/datafusion_ext/src/errors.rs`

---

## Summary Statistics

| Metric | Before | After | Reduction |
|--------|--------|-------|-----------|
| Total Rust lines | ~25,000 | ~14,000 | ~11,000 (44%) |
| Duplicate files | 18 | 0 | 18 files |
| Crate dependencies | Circular | Linear | Clean hierarchy |
| Async runtimes | 2 | 1 | 1 |
| Error type locations | 1 | 2 (core + PyO3) | N/A (intentional) |
