# Codebase Consolidation Plan v3 (Comprehensive Review)

## Executive Summary

This comprehensive consolidation plan identifies **14 scope items** across Python (`src/`) and Rust (`rust/`) directories. Building on the completed v2 plan, this review focuses on remaining consolidation opportunities discovered through systematic analysis using CQ tools, architectural documentation review, and codebase-wide pattern detection.

**Key findings from analysis:**
- **580** frozen dataclasses across 183 files
- **126** storage options field occurrences across 23 files
- **6,108** import-time function calls (side-effects analysis)
- **58** Request classes with significant field overlap
- Parallel error hierarchies in Rust and Python
- Repeated fingerprint method patterns across 6+ config classes
- Delta Lake option structs duplicated between Rust crates

**Estimated impact:**
- 1,200-1,800 lines of code reduction
- Improved cross-crate type safety
- Unified error handling across language boundaries
- Simplified Delta Lake request patterns
- Consolidated fingerprinting infrastructure

---

## Design-Phase Principles (Continuing from v2)

1. **No backward compatibility**: remove legacy shapes, hashes, and payloads
2. **Fingerprints and cache keys are not stable**: all derived artifacts can change
3. **Single source of truth**: Source data + view definitions; everything else is derivative
4. **Rust as execution authority**: Python delegates validation/computation to Rust where beneficial

---

## Table of Contents

1. [Delta Request Base Pattern Consolidation](#1-delta-request-base-pattern-consolidation)
2. [Fingerprinting Infrastructure Unification](#2-fingerprinting-infrastructure-unification)
3. [Storage Options Field Consolidation](#3-storage-options-field-consolidation)
4. [Rust Error Hierarchy Consolidation](#4-rust-error-hierarchy-consolidation)
5. [PyArrow Conversion Bridge](#5-pyarrow-conversion-bridge)
6. [Delta Options Struct Unification (Rust)](#6-delta-options-struct-unification-rust)
7. [UDF Builder Macro System](#7-udf-builder-macro-system)
8. [Environment Parsing Extensions](#8-environment-parsing-extensions)
9. [Extract Result Type Consolidation](#9-extract-result-type-consolidation)
10. [Pipeline Types Module Split](#10-pipeline-types-module-split)
11. [Telemetry Constants Registry](#11-telemetry-constants-registry)
12. [Validation Utilities Extension](#12-validation-utilities-extension)
13. [Registry Protocol Adoption](#13-registry-protocol-adoption)
14. [Incremental Config Unification](#14-incremental-config-unification)

---

## 1. Delta Request Base Pattern Consolidation

### Problem Statement
58 Delta*Request classes across `src/datafusion_engine/delta_control_plane.py`, `src/engine/delta_tools.py`, and `src/storage/deltalake/delta.py` share common fields but lack a unified base pattern.

### Current State (Duplicated Pattern)
```python
# src/datafusion_engine/delta_control_plane.py
@dataclass(frozen=True)
class DeltaSnapshotRequest:
    table_uri: str
    storage_options: Mapping[str, str] | None = None
    version: int | None = None
    timestamp: str | None = None
    gate: DeltaFeatureGate | None = None

@dataclass(frozen=True)
class DeltaProviderRequest:
    table_uri: str
    storage_options: Mapping[str, str] | None = None
    version: int | None = None
    timestamp: str | None = None
    gate: DeltaFeatureGate | None = None
    # ... additional fields

# Similar pattern repeated 16+ times
```

### Target Implementation
Introduce `DeltaBaseRequest` with common fields and derive specific requests via composition or inheritance.

```python
# src/core/delta_request_base.py
from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.generated.delta_types import DeltaFeatureGate

StorageOptions = Mapping[str, str] | None


@dataclass(frozen=True)
class DeltaBaseRequest:
    """Common fields for Delta Lake requests."""

    table_uri: str
    storage_options: StorageOptions = None
    log_storage_options: StorageOptions = None
    version: int | None = None
    timestamp: str | None = None
    gate: DeltaFeatureGate | None = None


@dataclass(frozen=True)
class DeltaWriteRequest(DeltaBaseRequest):
    """Write-specific Delta request."""

    mode: str = "append"
    predicate: str | None = None
    commit_metadata: Mapping[str, str] | None = None


@dataclass(frozen=True)
class DeltaMutationRequest(DeltaBaseRequest):
    """Base for mutation operations (delete, update, merge)."""

    predicate: str | None = None
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/core/delta_request_base.py` | Base request classes |
| Modify | `src/datafusion_engine/delta_control_plane.py` | Use base classes |
| Modify | `src/engine/delta_tools.py` | Use base classes |
| Modify | `src/storage/deltalake/delta.py` | Use base classes |
| Modify | `src/incremental/write_helpers.py` | Use base classes |

### Implementation Checklist
- [ ] Create `DeltaBaseRequest` with common fields
- [ ] Create `DeltaMutationRequest` extending base
- [ ] Refactor 16+ Delta*Request classes to use base
- [ ] Update all call sites using kwargs pattern
- [ ] Add unit tests for inheritance behavior

### Decommissioning List
- Remove duplicated `table_uri`, `storage_options`, `version`, `timestamp`, `gate` fields from 16+ classes
- Estimated reduction: 100-150 lines

---

## 2. Fingerprinting Infrastructure Unification

### Problem Statement
Six+ config classes implement identical `fingerprint_payload()` + `fingerprint()` method pairs, violating DRY principles.

### Current State (from CQ analysis)
```python
# Found in 6+ files with identical pattern:
# - src/cache/diskcache_factory.py (2 classes)
# - src/storage/deltalake/config.py (3 classes)
# - src/datafusion_engine/delta_store_policy.py (1 class)
# - src/obs/otel/config.py (1 class)

def fingerprint_payload(self) -> Mapping[str, object]:
    return { ... }

def fingerprint(self) -> str:
    return config_fingerprint(self.fingerprint_payload())
```

### Target Implementation
Create a `FingerprintableConfig` mixin that provides the `fingerprint()` method automatically.

```python
# src/core/config_base.py (extend existing)
from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, runtime_checkable

from utils.hashing import hash_msgpack_canonical


@runtime_checkable
class FingerprintableConfig(Protocol):
    """Protocol for configs that can produce a fingerprint payload."""

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return payload dict for fingerprinting."""
        ...


class FingerprintMixin:
    """Mixin providing fingerprint() from fingerprint_payload()."""

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Override in subclass to return payload dict."""
        raise NotImplementedError

    def fingerprint(self) -> str:
        """Return SHA-256 fingerprint of payload."""
        return config_fingerprint(self.fingerprint_payload())


def auto_fingerprint(config: FingerprintableConfig) -> str:
    """Compute fingerprint for any FingerprintableConfig."""
    return config_fingerprint(config.fingerprint_payload())
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/core/config_base.py` | Add mixin and protocol |
| Modify | `src/cache/diskcache_factory.py` | Use mixin |
| Modify | `src/storage/deltalake/config.py` | Use mixin |
| Modify | `src/datafusion_engine/delta_store_policy.py` | Use mixin |
| Modify | `src/obs/otel/config.py` | Use mixin |

### Implementation Checklist
- [ ] Add `FingerprintableConfig` protocol to `core/config_base.py`
- [ ] Add `FingerprintMixin` class
- [ ] Add `auto_fingerprint()` utility function
- [ ] Refactor `DiskCacheSettings` to use mixin
- [ ] Refactor `DiskCacheProfile` to use mixin
- [ ] Refactor `DeltaWritePolicy` to use mixin
- [ ] Refactor `ParquetWriterPolicy` to use mixin
- [ ] Refactor `DeltaSchemaPolicy` to use mixin
- [ ] Refactor `DeltaStorePolicy` to use mixin
- [ ] Refactor `OtelConfig` to use mixin

### Decommissioning List
- Remove 6+ identical `fingerprint()` method implementations
- Estimated reduction: 60-90 lines

---

## 3. Storage Options Field Consolidation

### Problem Statement
`storage_options` and `log_storage_options` fields appear 126 times across 23 files with inconsistent typing and no shared type alias.

### Current State
```python
# Various patterns found:
storage_options: Mapping[str, str] | None = None  # 45 occurrences
storage_options: StorageOptions | None = None     # 12 occurrences
storage_options: dict[str, str] | None = None     # 8 occurrences
```

### Target Implementation
Create canonical type aliases and a storage spec dataclass.

```python
# src/core/storage_types.py
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TypeAlias

StorageOptions: TypeAlias = Mapping[str, str] | None
LogStorageOptions: TypeAlias = Mapping[str, str] | None


@dataclass(frozen=True)
class DeltaStorageSpec:
    """Canonical storage options for Delta operations."""

    storage_options: StorageOptions = None
    log_storage_options: LogStorageOptions = None

    def merged(self, *, fallback_log_to_storage: bool = False) -> dict[str, str] | None:
        """Merge storage and log options."""
        from utils.storage_options import merged_storage_options
        return merged_storage_options(
            self.storage_options,
            self.log_storage_options,
            fallback_log_to_storage=fallback_log_to_storage,
        )
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/core/storage_types.py` | Type aliases and DeltaStorageSpec |
| Modify | `src/datafusion_engine/delta_control_plane.py` | Use type aliases |
| Modify | `src/storage/deltalake/delta.py` | Use type aliases |
| Modify | `src/incremental/*.py` | Use type aliases |
| Modify | `src/engine/delta_tools.py` | Use type aliases |

### Implementation Checklist
- [ ] Create `src/core/storage_types.py` with type aliases
- [ ] Add `DeltaStorageSpec` dataclass
- [ ] Update imports across 23 files to use canonical types
- [ ] Replace inline field definitions with `DeltaStorageSpec` where appropriate

### Decommissioning List
- Remove inconsistent type annotations
- Estimated standardization: 126 occurrences unified

---

## 4. Rust Error Hierarchy Consolidation

### Problem Statement
Parallel error types in `datafusion_ext` and `datafusion_python` with overlapping variants and redundant wrapping.

### Current State
```rust
// rust/datafusion_ext/src/errors.rs
pub enum ExtError {
    DataFusion(Box<DataFusionError>),
    Arrow(ArrowError),
    Generic(String),
    Delta(String),
    Plugin(String),
}

// rust/datafusion_python/src/errors.rs
pub enum PyDataFusionError {
    ExecutionError(Box<InnerDataFusionError>),
    ArrowError(ArrowError),
    Common(String),
    PythonError(PyErr),
    EncodeError(EncodeError),
    ExtError(ExtError),  // Wrapper!
}
```

### Target Implementation
Consolidate error types with trait-based Python conversion.

```rust
// rust/datafusion_ext/src/errors.rs (extended)
#[derive(Debug)]
pub enum ExtError {
    DataFusion(Box<DataFusionError>),
    Arrow(ArrowError),
    Generic(String),
    Delta(String),
    Plugin(String),
    Encode(String),  // Moved from PyDataFusionError
}

pub trait ToPyError {
    fn to_py_err(&self) -> pyo3::PyErr;
}

impl ToPyError for ExtError {
    fn to_py_err(&self) -> pyo3::PyErr {
        use pyo3::exceptions::PyRuntimeError;
        PyRuntimeError::new_err(self.to_string())
    }
}
```

```rust
// rust/datafusion_python/src/errors.rs (simplified)
pub use datafusion_ext::errors::{ExtError, ExtResult, ToPyError};

#[derive(Debug)]
pub enum PyDataFusionError {
    Ext(ExtError),
    Python(PyErr),
}

impl From<ExtError> for PyDataFusionError {
    fn from(err: ExtError) -> Self {
        PyDataFusionError::Ext(err)
    }
}

impl From<PyErr> for PyDataFusionError {
    fn from(err: PyErr) -> Self {
        PyDataFusionError::Python(err)
    }
}

impl From<PyDataFusionError> for PyErr {
    fn from(err: PyDataFusionError) -> PyErr {
        match err {
            PyDataFusionError::Python(e) => e,
            PyDataFusionError::Ext(e) => e.to_py_err(),
        }
    }
}
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `rust/datafusion_ext/src/errors.rs` | Add ToPyError trait, consolidate variants |
| Modify | `rust/datafusion_python/src/errors.rs` | Simplify to 2 variants |
| Modify | `rust/datafusion_python/src/*.rs` | Update error usage |

### Implementation Checklist
- [ ] Add `ToPyError` trait to `datafusion_ext`
- [ ] Move `Encode` variant to `ExtError`
- [ ] Simplify `PyDataFusionError` to two variants
- [ ] Update all error conversions in `datafusion_python`
- [ ] Update error handling in UDF modules

### Decommissioning List
- Remove redundant `ArrowError`, `ExecutionError`, `Common` variants from `PyDataFusionError`
- Estimated reduction: 40-50 lines

---

## 5. PyArrow Conversion Bridge

### Problem Statement
PyArrow ↔ Rust array conversions duplicated across `udf.rs`, `udaf.rs`, `udwf.rs`.

### Current State (from exploration)
```rust
// Pattern repeated in 4+ files:
fn pyarrow_function_to_rust(func: Py<PyAny>) -> impl Fn(&[ArrayRef]) -> Result<ArrayRef> {
    move |args: &[ArrayRef]| -> Result<ArrayRef, DataFusionError> {
        Python::attach(|py| {
            let py_args = args.iter()
                .map(|arg| arg.into_data().to_pyarrow(py)...)
                .collect::<Vec<_>>();
            // ... call Python ...
            ArrayData::from_pyarrow_bound(value.bind(py))...
        })
    }
}
```

### Target Implementation
Create a centralized PyArrow bridge module.

```rust
// rust/datafusion_python/src/pyarrow_bridge.rs
use datafusion::arrow::array::ArrayRef;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

/// Convert Rust arrays to a Python tuple of PyArrow arrays.
pub fn arrays_to_pyarrow_tuple<'py>(
    py: Python<'py>,
    arrays: &[ArrayRef],
) -> PyResult<Bound<'py, PyTuple>> {
    let py_arrays: Vec<PyObject> = arrays
        .iter()
        .map(|arr| arr.into_data().to_pyarrow(py).map(|a| a.unbind()))
        .collect::<PyResult<Vec<_>>>()?;
    PyTuple::new(py, py_arrays)
}

/// Convert a PyArrow array to a Rust ArrayRef.
pub fn pyarrow_to_array(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<ArrayRef> {
    use arrow::array::make_array;
    use arrow::ffi::from_ffi;
    let data = arrow::array::ArrayData::from_pyarrow_bound(obj)?;
    Ok(make_array(data))
}

/// Call a Python function with ArrayRef arguments and return ArrayRef.
pub fn call_pyarrow_udf(
    py: Python<'_>,
    func: &Bound<'_, PyAny>,
    args: &[ArrayRef],
) -> PyResult<ArrayRef> {
    let py_args = arrays_to_pyarrow_tuple(py, args)?;
    let result = func.call1(py_args)?;
    pyarrow_to_array(py, &result)
}
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `rust/datafusion_python/src/pyarrow_bridge.rs` | Centralized conversion utilities |
| Modify | `rust/datafusion_python/src/udf.rs` | Use bridge module |
| Modify | `rust/datafusion_python/src/udaf.rs` | Use bridge module |
| Modify | `rust/datafusion_python/src/udwf.rs` | Use bridge module |
| Modify | `rust/datafusion_python/src/lib.rs` | Add module export |

### Implementation Checklist
- [ ] Create `pyarrow_bridge.rs` with core conversion functions
- [ ] Add `arrays_to_pyarrow_tuple()` function
- [ ] Add `pyarrow_to_array()` function
- [ ] Add `call_pyarrow_udf()` helper
- [ ] Refactor `udf.rs` to use bridge
- [ ] Refactor `udaf.rs` to use bridge
- [ ] Refactor `udwf.rs` to use bridge
- [ ] Add unit tests for conversions

### Decommissioning List
- Remove duplicated conversion code from 4+ files
- Estimated reduction: 80-120 lines

---

## 6. Delta Options Struct Unification (Rust)

### Problem Statement
`DeltaProviderOptions` and `DeltaCdfProviderOptions` defined in `df_plugin_codeanatomy` but should be in `datafusion_ext` for reuse.

### Current State
```rust
// rust/df_plugin_codeanatomy/src/lib.rs
#[derive(Debug, Deserialize)]
struct DeltaProviderOptions {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    file_column_name: Option<String>,
    enable_parquet_pushdown: Option<bool>,
    // ... 8+ more fields
}
```

### Target Implementation
Move to `datafusion_ext` and create builder pattern.

```rust
// rust/datafusion_ext/src/delta_options.rs
use std::collections::HashMap;
use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DeltaTableOptions {
    pub table_uri: String,
    pub storage_options: Option<HashMap<String, String>>,
    pub version: Option<i64>,
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DeltaProviderOptions {
    #[serde(flatten)]
    pub base: DeltaTableOptions,
    pub file_column_name: Option<String>,
    pub enable_parquet_pushdown: Option<bool>,
    pub schema_force_view_types: Option<bool>,
    pub wrap_partition_values: Option<bool>,
    // Feature gate fields
    pub min_reader_version: Option<i32>,
    pub min_writer_version: Option<i32>,
    pub required_reader_features: Option<Vec<String>>,
    pub required_writer_features: Option<Vec<String>>,
    pub files: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DeltaCdfProviderOptions {
    #[serde(flatten)]
    pub base: DeltaProviderOptions,
    pub starting_version: Option<i64>,
    pub starting_timestamp: Option<String>,
    pub ending_version: Option<i64>,
    pub ending_timestamp: Option<String>,
}
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `rust/datafusion_ext/src/delta_options.rs` | Centralized option types |
| Modify | `rust/datafusion_ext/src/lib.rs` | Export module |
| Modify | `rust/df_plugin_codeanatomy/src/lib.rs` | Use exported types |

### Implementation Checklist
- [ ] Create `delta_options.rs` with base types
- [ ] Use `#[serde(flatten)]` for composition
- [ ] Export from `datafusion_ext::delta_options`
- [ ] Update `df_plugin_codeanatomy` to use exported types
- [ ] Add unit tests for deserialization

### Decommissioning List
- Remove duplicated option structs from `df_plugin_codeanatomy`
- Estimated reduction: 50-70 lines

---

## 7. UDF Builder Macro System

### Problem Statement
Repetitive UDF wrapper functions in `udaf_builtin.rs` and `udwf_builtin.rs`.

### Current State
```rust
// 8+ functions with identical pattern:
fn list_unique_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(ListUniqueUdaf::new()))
}

fn collect_set_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(CollectSetUdaf::new()))
}
```

### Target Implementation
Create macros for boilerplate elimination.

```rust
// rust/datafusion_ext/src/udf_macros.rs

/// Generate a wrapper function for an AggregateUDF implementation.
macro_rules! udaf_wrapper {
    ($fn_name:ident, $impl_type:ident) => {
        fn $fn_name() -> AggregateUDF {
            AggregateUDF::new_from_shared_impl(Arc::new($impl_type::new()))
        }
    };
    ($fn_name:ident, $impl_type:ident, $($alias:literal),+) => {
        fn $fn_name() -> AggregateUDF {
            AggregateUDF::new_from_shared_impl(Arc::new($impl_type::new()))
                .with_aliases([$($alias),+])
        }
    };
}

/// Generate a wrapper function for a WindowUDF implementation.
macro_rules! udwf_wrapper {
    ($fn_name:ident, $impl_expr:expr) => {
        fn $fn_name() -> WindowUDF {
            $impl_expr
        }
    };
    ($fn_name:ident, $impl_expr:expr, $($alias:literal),+) => {
        fn $fn_name() -> WindowUDF {
            $impl_expr.with_aliases([$($alias),+])
        }
    };
}

pub(crate) use udaf_wrapper;
pub(crate) use udwf_wrapper;
```

```rust
// Usage in udaf_builtin.rs:
udaf_wrapper!(list_unique_udaf, ListUniqueUdaf);
udaf_wrapper!(collect_set_udaf, CollectSetUdaf, "set_agg");
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `rust/datafusion_ext/src/udf_macros.rs` | Macro definitions |
| Modify | `rust/datafusion_ext/src/udaf_builtin.rs` | Use macros |
| Modify | `rust/datafusion_ext/src/udwf_builtin.rs` | Use macros |
| Modify | `rust/datafusion_ext/src/lib.rs` | Export macros |

### Implementation Checklist
- [ ] Create `udf_macros.rs` with `udaf_wrapper!` macro
- [ ] Create `udwf_wrapper!` macro
- [ ] Refactor `udaf_builtin.rs` to use macros
- [ ] Refactor `udwf_builtin.rs` to use macros

### Decommissioning List
- Remove 8+ boilerplate wrapper functions
- Estimated reduction: 30-40 lines

---

## 8. Environment Parsing Extensions

### Problem Statement
Missing `env_list()` and `env_enum()` helpers cause manual parsing duplication.

### Current State
```python
# Found scattered manual parsing:
# src/obs/otel/attributes.py:34-38
raw = env_value("CODEANATOMY_OTEL_REDACTED_KEYS")
if raw:
    keys = [k.strip() for k in raw.split(",") if k.strip()]

# src/obs/otel/config.py
sampler = env_value("OTEL_SAMPLER")
if sampler and sampler.lower() in {...}:
    ...
```

### Target Implementation
Add missing helpers to `env_utils.py`.

```python
# src/utils/env_utils.py (additions)

def env_list(
    name: str,
    *,
    default: list[str] | None = None,
    separator: str = ",",
    strip: bool = True,
) -> list[str]:
    """Parse comma-separated environment variable to list.

    Parameters
    ----------
    name : str
        Environment variable name.
    default : list[str] | None
        Default value if not set.
    separator : str
        Separator character (default: comma).
    strip : bool
        Strip whitespace from items.

    Returns
    -------
    list[str]
        Parsed list or default.
    """
    raw = env_value(name)
    if raw is None:
        return default or []
    items = raw.split(separator)
    if strip:
        items = [item.strip() for item in items if item.strip()]
    return items


def env_enum(
    name: str,
    enum_type: type[E],
    *,
    default: E | None = None,
) -> E | None:
    """Parse environment variable as enum value.

    Parameters
    ----------
    name : str
        Environment variable name.
    enum_type : type[E]
        Enum class to convert to.
    default : E | None
        Default value if not set or invalid.

    Returns
    -------
    E | None
        Parsed enum value or default.
    """
    raw = env_value(name)
    if raw is None:
        return default
    try:
        return enum_type(raw.lower())
    except (ValueError, KeyError):
        return default
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/utils/env_utils.py` | Add env_list, env_enum |
| Modify | `src/obs/otel/attributes.py` | Use env_list |
| Modify | `src/obs/otel/config.py` | Use env_enum |
| Create | `tests/unit/utils/test_env_utils_extended.py` | Tests for new functions |

### Implementation Checklist
- [ ] Add `env_list()` function
- [ ] Add `env_enum()` function with generic type
- [ ] Add overloads for strict typing
- [ ] Update OTel modules to use new helpers
- [ ] Add unit tests

### Decommissioning List
- Remove manual comma-split parsing patterns
- Estimated reduction: 20-30 lines

---

## 9. Extract Result Type Consolidation

### Problem Statement
Near-identical `*ExtractResult` dataclasses across extractor modules.

### Current State
```python
# Each extractor has nearly identical pattern:
@dataclass(frozen=True)
class BytecodeExtractResult:
    bytecode_files: TableLike

@dataclass(frozen=True)
class CstExtractResult:
    libcst_files: TableLike

@dataclass(frozen=True)
class AstExtractResult:
    ast_files: TableLike
```

### Target Implementation
Create generic extract result type.

```python
# src/extract/result_types.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from core_types import TableLike

T = TypeVar("T", bound=TableLike)


@dataclass(frozen=True)
class ExtractResult(Generic[T]):
    """Generic result container for extraction outputs."""

    table: T
    extractor_name: str


# Type aliases for specific extractors
BytecodeExtractResult = ExtractResult[TableLike]
CstExtractResult = ExtractResult[TableLike]
AstExtractResult = ExtractResult[TableLike]
TreeSitterExtractResult = ExtractResult[TableLike]
SymtableExtractResult = ExtractResult[TableLike]
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/extract/result_types.py` | Generic result type |
| Modify | `src/extract/bytecode_extract.py` | Use generic type |
| Modify | `src/extract/cst_extract.py` | Use generic type |
| Modify | `src/extract/ast_extract.py` | Use generic type |
| Modify | `src/extract/tree_sitter_extract.py` | Use generic type |
| Modify | `src/extract/symtable_extract.py` | Use generic type |

### Implementation Checklist
- [ ] Create `ExtractResult[T]` generic dataclass
- [ ] Create type aliases for backward compatibility
- [ ] Update 5 extractor modules to use generic type
- [ ] Ensure existing tests pass with new types

### Decommissioning List
- Remove 5 near-identical dataclass definitions
- Estimated reduction: 25-30 lines

---

## 10. Pipeline Types Module Split

### Problem Statement
`src/hamilton_pipeline/pipeline_types.py` contains 40+ dataclass definitions (31 frozen), making it a monolithic file difficult to navigate.

### Current State
- 40+ classes in single file
- Mix of repo config, incremental config, output config, cache config
- 1,500+ lines

### Target Implementation
Split into focused submodules.

```
src/hamilton_pipeline/
├── pipeline_types.py  (re-exports for backward compatibility)
├── types/
│   ├── __init__.py
│   ├── repo_config.py      # RepoScopeConfig, RepoScanConfig
│   ├── output_config.py    # OutputStoragePolicy, OutputConfig
│   ├── cache_config.py     # CacheRuntimeContext
│   ├── incremental.py      # IncrementalRunConfig, IncrementalDatasetUpdates
│   └── execution.py        # ExecutorConfig, ExecutionMode
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/hamilton_pipeline/types/__init__.py` | Package exports |
| Create | `src/hamilton_pipeline/types/repo_config.py` | Repo configuration types |
| Create | `src/hamilton_pipeline/types/output_config.py` | Output configuration types |
| Create | `src/hamilton_pipeline/types/cache_config.py` | Cache configuration types |
| Create | `src/hamilton_pipeline/types/incremental.py` | Incremental processing types |
| Create | `src/hamilton_pipeline/types/execution.py` | Execution configuration types |
| Modify | `src/hamilton_pipeline/pipeline_types.py` | Re-export from submodules |

### Implementation Checklist
- [ ] Create `types/` subpackage
- [ ] Move repo-related configs to `repo_config.py`
- [ ] Move output configs to `output_config.py`
- [ ] Move cache configs to `cache_config.py`
- [ ] Move incremental configs to `incremental.py`
- [ ] Move execution configs to `execution.py`
- [ ] Add re-exports to `pipeline_types.py` for compatibility
- [ ] Update imports in dependent modules

### Decommissioning List
- No code removal, organizational refactor only

---

## 11. Telemetry Constants Registry

### Problem Statement
Magic strings for metrics, attributes, scopes scattered across `src/obs/otel/` modules.

### Current State
```python
# src/obs/otel/metrics.py
_STAGE_DURATION = "codeanatomy.stage.duration"
_TASK_DURATION = "codeanatomy.task.duration"
# ... more scattered constants

# src/obs/otel/attributes.py
# Manual parsing of redaction keys

# src/obs/otel/scopes.py
# Scope name constants
```

### Target Implementation
Create centralized constants module.

```python
# src/obs/otel/constants.py
from __future__ import annotations

from enum import StrEnum


class MetricName(StrEnum):
    """Canonical metric names."""

    STAGE_DURATION = "codeanatomy.stage.duration"
    TASK_DURATION = "codeanatomy.task.duration"
    PLAN_COMPILE_DURATION = "codeanatomy.plan.compile.duration"
    PLAN_EXECUTE_DURATION = "codeanatomy.plan.execute.duration"
    DELTA_SCAN_DURATION = "codeanatomy.delta.scan.duration"
    DELTA_WRITE_DURATION = "codeanatomy.delta.write.duration"


class AttributeName(StrEnum):
    """Canonical attribute names."""

    STAGE_NAME = "codeanatomy.stage.name"
    TASK_NAME = "codeanatomy.task.name"
    DATASET_NAME = "codeanatomy.dataset.name"
    ERROR_KIND = "codeanatomy.error.kind"


class ScopeName(StrEnum):
    """Canonical instrumentation scope names."""

    PIPELINE = "codeanatomy.pipeline"
    DATAFUSION = "codeanatomy.datafusion"
    DELTA = "codeanatomy.delta"
    HAMILTON = "codeanatomy.hamilton"


class ResourceAttribute(StrEnum):
    """Canonical resource attributes."""

    SERVICE_NAME = "service.name"
    SERVICE_VERSION = "service.version"
    DEPLOYMENT_ENVIRONMENT = "deployment.environment"
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/obs/otel/constants.py` | Centralized constants |
| Modify | `src/obs/otel/metrics.py` | Use constants |
| Modify | `src/obs/otel/attributes.py` | Use constants |
| Modify | `src/obs/otel/scopes.py` | Use constants |
| Modify | `src/obs/otel/resources.py` | Use constants |

### Implementation Checklist
- [ ] Create `constants.py` with enum-based constants
- [ ] Define `MetricName` enum
- [ ] Define `AttributeName` enum
- [ ] Define `ScopeName` enum
- [ ] Define `ResourceAttribute` enum
- [ ] Update all OTel modules to use enums
- [ ] Add docstrings with semantic meanings

### Decommissioning List
- Remove scattered string constants
- Estimated consolidation: 40+ constants unified

---

## 12. Validation Utilities Extension

### Problem Statement
Missing `ensure_not_empty()`, `ensure_subset()`, `ensure_unique()` validators.

### Target Implementation
```python
# src/utils/validation.py (additions)

def ensure_not_empty(
    value: Sequence[T],
    *,
    label: str = "value",
) -> Sequence[T]:
    """Ensure sequence is not empty.

    Raises
    ------
    ValueError
        If sequence is empty.
    """
    if not value:
        raise ValueError(f"{label} must not be empty")
    return value


def ensure_subset(
    items: Iterable[T],
    universe: Container[T],
    *,
    label: str = "items",
) -> None:
    """Ensure all items are in the universe set.

    Raises
    ------
    ValueError
        If any item is not in universe.
    """
    extra = [item for item in items if item not in universe]
    if extra:
        raise ValueError(f"{label} contains invalid values: {extra}")


def ensure_unique(
    items: Iterable[T],
    *,
    label: str = "items",
) -> list[T]:
    """Ensure all items are unique, return deduplicated list.

    Raises
    ------
    ValueError
        If duplicates found.
    """
    seen: set[T] = set()
    duplicates: list[T] = []
    result: list[T] = []
    for item in items:
        if item in seen:
            duplicates.append(item)
        else:
            seen.add(item)
            result.append(item)
    if duplicates:
        raise ValueError(f"{label} contains duplicates: {duplicates}")
    return result
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/utils/validation.py` | Add new validators |
| Create | `tests/unit/utils/test_validation_extended.py` | Tests for new functions |

### Implementation Checklist
- [ ] Add `ensure_not_empty()` function
- [ ] Add `ensure_subset()` function
- [ ] Add `ensure_unique()` function
- [ ] Add unit tests for each function
- [ ] Update module `__all__` export

### Decommissioning List
- None (additive change)

---

## 13. Registry Protocol Adoption

### Problem Statement
Rich registries (`ProviderRegistry`, `ViewRegistry`, `SchemaRegistry`) don't consistently use `Registry` protocol.

### Target Implementation
Ensure all registries implement or compose `Registry[K, V]` protocol.

```python
# Example refactor for ViewRegistry
# src/datafusion_engine/view_registry.py

from utils.registry_protocol import MutableRegistry, Registry


class ViewRegistry:
    """Registry for view builders."""

    def __init__(self) -> None:
        self._builders: MutableRegistry[str, ViewBuilder] = MutableRegistry()
        self._aliases: dict[str, str] = {}

    def register(self, name: str, builder: ViewBuilder) -> None:
        """Register a view builder."""
        self._builders.register(name, builder)

    def get(self, name: str) -> ViewBuilder | None:
        """Get view builder by name or alias."""
        canonical = self._aliases.get(name, name)
        return self._builders.get(canonical)

    def __contains__(self, name: str) -> bool:
        canonical = self._aliases.get(name, name)
        return canonical in self._builders

    def __iter__(self) -> Iterator[str]:
        return iter(self._builders)

    def __len__(self) -> int:
        return len(self._builders)
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/datafusion_engine/view_registry.py` | Use MutableRegistry composition |
| Modify | `src/datafusion_engine/provider_registry.py` | Use MutableRegistry composition |
| Modify | `src/datafusion_engine/schema_registry.py` | Use ImmutableRegistry where appropriate |
| Modify | `src/hamilton_pipeline/semantic_registry.py` | Use registry protocol |

### Implementation Checklist
- [ ] Refactor `ViewRegistry` to use `MutableRegistry` composition
- [ ] Refactor `ProviderRegistry` to use composition
- [ ] Review `SchemaRegistry` for ImmutableRegistry usage
- [ ] Update `SemanticRegistry` to follow protocol
- [ ] Add protocol compliance tests

### Decommissioning List
- Remove ad-hoc dict implementations where registry protocol fits

---

## 14. Incremental Config Unification

### Problem Statement
`IncrementalSettings` in `src/incremental/types.py` and `IncrementalRunConfig` in `src/hamilton_pipeline/pipeline_types.py` serve similar purposes.

### Target Implementation
Establish single source of truth.

```python
# src/incremental/config.py (new canonical location)
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IncrementalConfig:
    """Configuration for incremental processing."""

    enabled: bool = False
    state_dir: str | None = None
    impact_strategy: str = "hybrid"
    invalidation_mode: str = "plan_fingerprint"
    cdf_enabled: bool = True


# Alias for backward compatibility
IncrementalSettings = IncrementalConfig
IncrementalRunConfig = IncrementalConfig
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/incremental/config.py` | Canonical config type |
| Modify | `src/incremental/types.py` | Re-export from config |
| Modify | `src/hamilton_pipeline/pipeline_types.py` | Re-export from incremental |

### Implementation Checklist
- [ ] Create `src/incremental/config.py`
- [ ] Define `IncrementalConfig` as canonical type
- [ ] Add backward-compatible aliases
- [ ] Update imports across codebase
- [ ] Add deprecation warnings for old names if desired

### Decommissioning List
- Remove duplicate `IncrementalSettings` definition
- Estimated reduction: 20-30 lines

---

## Cross-Scope Dependencies

| Scope | Depends On | Notes |
|-------|------------|-------|
| #1 Delta Request Base | #3 Storage Options | Uses StorageOptions type |
| #2 Fingerprinting | None | Foundation |
| #3 Storage Options | None | Foundation |
| #4 Rust Errors | None | Foundation |
| #5 PyArrow Bridge | #4 Rust Errors | Uses error types |
| #6 Delta Options Rust | #4 Rust Errors | Uses error handling |
| #7 UDF Macros | None | Foundation |
| #8 Env Parsing | None | Foundation |
| #9 Extract Results | None | Independent |
| #10 Pipeline Types Split | None | Organizational |
| #11 Telemetry Constants | #8 Env Parsing | Uses env_list |
| #12 Validation Extensions | None | Foundation |
| #13 Registry Adoption | None | Independent |
| #14 Incremental Config | #10 Pipeline Split | Part of split |

---

## Recommended Implementation Order

**Phase 1: Foundations (Week 1)**
1. Storage Options Type Aliases (#3)
2. Environment Parsing Extensions (#8)
3. Validation Utilities Extension (#12)
4. Fingerprinting Infrastructure (#2)

**Phase 2: Core Patterns (Week 2)**
5. Delta Request Base Pattern (#1)
6. Extract Result Consolidation (#9)
7. Telemetry Constants Registry (#11)
8. Registry Protocol Adoption (#13)

**Phase 3: Rust Consolidation (Week 3)**
9. Rust Error Hierarchy (#4)
10. PyArrow Conversion Bridge (#5)
11. Delta Options Struct Unification (#6)
12. UDF Builder Macros (#7)

**Phase 4: Organizational (Week 4)**
13. Pipeline Types Module Split (#10)
14. Incremental Config Unification (#14)

---

## Verification Plan

### Unit Tests
```bash
# Run after each scope completion
uv run pytest tests/unit/ -v --tb=short

# Type checking
uv run pyright --warnings --pythonversion=3.13
uv run pyrefly check

# Linting
uv run ruff check --fix
```

### Integration Tests
```bash
# Full test suite
uv run pytest tests/ -m "not e2e"

# E2E pipeline test
uv run pytest tests/e2e/ -v
```

### Rust Tests
```bash
# Workspace tests
cd rust && cargo test --workspace

# Specific crate
cargo test -p datafusion_ext
cargo test -p datafusion_python
```

### CQ Verification
```bash
# Verify no new import cycles
./scripts/cq imports --cycles --root .

# Verify side-effects reduced
./scripts/cq side-effects --root src

# Check fingerprint call sites
./scripts/cq calls config_fingerprint --root .
```

---

## Summary Statistics

| Metric | Value |
|--------|-------|
| Total Scopes | 14 |
| Files to Create | 12 |
| Files to Modify | 45+ |
| Estimated Code Reduction | 1,200-1,800 lines |
| Rust Consolidation | 4 scopes |
| Python Consolidation | 10 scopes |

---

## Appendix: CQ Analysis Commands Used

```bash
# Import structure and cycles
./scripts/cq imports --cycles --root .

# Side effects analysis
./scripts/cq side-effects --root src

# Call site census
./scripts/cq calls hash_msgpack_canonical --root .
./scripts/cq calls config_fingerprint --root .

# Bytecode surface
./scripts/cq bytecode-surface src/datafusion_engine --show globals,attrs

# Scope capture analysis
./scripts/cq scopes src/datafusion_engine
```
