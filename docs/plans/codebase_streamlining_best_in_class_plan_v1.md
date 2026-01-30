# Codebase Streamlining Plan v1 (Deep Structural Consolidation with Tool Recipes)

## Executive Summary

This comprehensive consolidation plan identifies **14 scope items** across Python (`src/`) and Rust (`rust/`) directories. Building on the completed v3 and v4 plans, this review focuses on deeper structural consolidation opportunities discovered through systematic analysis using **ast-grep** and **cq** tools. Each scope includes detailed tool recipes for both discovery and verification.

**Key findings from analysis:**
- **17** Delta*Request classes with 5 identical field groups (~85 duplicated lines)
- **7** files with identical `fingerprint_payload()` + `fingerprint()` method pairs
- **14** PyArrow `.to_pyarrow()` conversion calls duplicated across Rust UDF files
- **6** PyDataFusionError variants paralleling **5** ExtError variants with redundant wrapping
- **126** storage_options field occurrences across **23** files with inconsistent types
- **8** UDAF wrapper functions with identical pattern structure
- **51** import cycles identified via cq analysis
- **53** import-time side effects flagged

**Estimated impact:**
- 1,300-1,800 lines of code reduction
- Unified error handling across Python/Rust boundary
- Simplified Delta Lake request patterns
- Consolidated PyArrow conversion bridge
- Enhanced type safety via canonical type aliases
- Improved testability through reduced duplication

---

## Design-Phase Principles

1. **No backward compatibility burdens**: Remove legacy shapes, hashes, and payloads
2. **Fingerprints and cache keys are not stable**: All derived artifacts can change
3. **Single source of truth**: Source data + view definitions; everything else is derivative
4. **Rust as execution authority**: Python delegates validation/computation to Rust where beneficial
5. **Tool-assisted verification**: Every consolidation verified with ast-grep/cq before and after

---

## Table of Contents

1. [Delta Request Base Pattern Consolidation](#1-delta-request-base-pattern-consolidation)
2. [Fingerprinting Infrastructure Unification](#2-fingerprinting-infrastructure-unification)
3. [Storage Options Type Consolidation](#3-storage-options-type-consolidation)
4. [Rust Error Hierarchy Consolidation](#4-rust-error-hierarchy-consolidation)
5. [PyArrow Conversion Bridge](#5-pyarrow-conversion-bridge)
6. [Delta Options Struct Unification (Rust)](#6-delta-options-struct-unification-rust)
7. [UDF Builder Macro System](#7-udf-builder-macro-system)
8. [Hamilton Tag Policy Unification](#8-hamilton-tag-policy-unification)
9. [DataFusion Session Factory Consolidation](#9-datafusion-session-factory-consolidation)
10. [Schema Contract Type Alignment](#10-schema-contract-type-alignment)
11. [Delta Control Plane Function Consolidation](#11-delta-control-plane-function-consolidation)
12. [Provider Contract Unification](#12-provider-contract-unification)
13. [IO Adapter Pattern Consolidation](#13-io-adapter-pattern-consolidation)
14. [Structured Logging Unification](#14-structured-logging-unification)

---

## 1. Delta Request Base Pattern Consolidation

### Problem Statement
17 Delta*Request classes in `src/datafusion_engine/delta_control_plane.py` share identical field groups (`table_uri`, `storage_options`, `version`, `timestamp`, `gate`) but have no shared base, causing ~85 lines of field duplication.

### Current State (Duplicated Pattern)
```python
# src/datafusion_engine/delta_control_plane.py - 17 classes with same pattern:
@dataclass(frozen=True)
class DeltaSnapshotRequest:
    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    gate: DeltaFeatureGate | None = None

@dataclass(frozen=True)
class DeltaProviderRequest:
    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    delta_scan: DeltaScanOptions | None
    predicate: str | None = None
    gate: DeltaFeatureGate | None = None

@dataclass(frozen=True)
class DeltaWriteRequest:
    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    data_ipc: bytes
    mode: str
    # ... more fields
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

# ... 14 more classes with same base fields
```

### Target Implementation
```python
# src/core/delta_request_base.py
from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.generated.delta_types import (
        DeltaCommitOptions,
        DeltaFeatureGate,
    )

StorageOptions = Mapping[str, str] | None


@dataclass(frozen=True)
class DeltaBaseRequest:
    """Common fields for Delta Lake requests."""

    table_uri: str
    storage_options: StorageOptions = None
    version: int | None = None
    timestamp: str | None = None
    gate: DeltaFeatureGate | None = None


@dataclass(frozen=True)
class DeltaSnapshotRequest(DeltaBaseRequest):
    """Request for Delta snapshot resolution."""

    pass


@dataclass(frozen=True)
class DeltaProviderRequest(DeltaBaseRequest):
    """Request for Delta provider construction."""

    delta_scan: DeltaScanOptions | None = None
    predicate: str | None = None


@dataclass(frozen=True)
class DeltaMutationRequest(DeltaBaseRequest):
    """Base for mutation operations (delete, update, merge)."""

    predicate: str | None = None
    extra_constraints: Sequence[str] | None = None
    commit_options: DeltaCommitOptions | None = None


@dataclass(frozen=True)
class DeltaWriteRequest(DeltaMutationRequest):
    """Request for Delta write operations."""

    data_ipc: bytes = b""
    mode: str = "append"
    schema_mode: str | None = None
    partition_columns: Sequence[str] | None = None
    target_file_size: int | None = None
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/core/delta_request_base.py` | Base request classes |
| Modify | `src/datafusion_engine/delta_control_plane.py` | Inherit from base |
| Modify | `src/storage/deltalake/delta.py` | Use base classes |
| Modify | `src/incremental/write_helpers.py` | Use base classes |

### ast-grep Recipes

**Discovery - Find all Delta request dataclasses:**
```bash
# Find all frozen dataclasses with "Delta" in name
ast-grep run -p '@dataclass(frozen=True)
class Delta$NAME:
    $$$BODY' -l python src/datafusion_engine/

# Find repeated field patterns (table_uri: str)
ast-grep run -p 'table_uri: str' -l python src/datafusion_engine/

# Count Delta request classes
ast-grep run -p 'class Delta$NAMERequest:' -l python src/ --json=stream | wc -l
```

**Verification - Ensure base inheritance after refactor:**
```bash
# Verify Delta*Request classes inherit from base
ast-grep run -p 'class Delta$NAME(DeltaBaseRequest):' -l python src/
ast-grep run -p 'class Delta$NAME(DeltaMutationRequest):' -l python src/

# Find any classes still defining table_uri directly (should be zero)
ast-grep scan --inline-rules "$(cat <<'YAML'
id: delta-request-no-base
language: Python
rule:
  pattern: |
    @dataclass(frozen=True)
    class Delta$NAMERequest:
        table_uri: str
        $$$
  not:
    inside:
      pattern: class $_(DeltaBaseRequest)
severity: warning
message: Delta request class should inherit from DeltaBaseRequest
YAML
)" src/datafusion_engine/
```

### cq Recipes

**Impact analysis before refactor:**
```bash
# Find all call sites for Delta*Request constructors
./scripts/cq calls DeltaSnapshotRequest --root .
./scripts/cq calls DeltaProviderRequest --root .
./scripts/cq calls DeltaWriteRequest --root .

# Analyze parameter flow for table_uri
./scripts/cq impact delta_provider_from_session --param request --root .

# Check for import cycles that might block the new base module
./scripts/cq imports --cycles --root src/core

# Check current module's import structure
./scripts/cq imports --module src.datafusion_engine.delta_control_plane --root .
```

**Post-refactor verification:**
```bash
# Verify call sites still work (check for breaking changes)
./scripts/cq sig-impact DeltaSnapshotRequest --to "DeltaSnapshotRequest(table_uri='')" --root .

# Ensure no side effects introduced
./scripts/cq side-effects --root src/core

# Verify bytecode surface unchanged
./scripts/cq bytecode-surface src/datafusion_engine/delta_control_plane.py --show globals
```

### Implementation Checklist
- [ ] Create `src/core/delta_request_base.py` with `DeltaBaseRequest`
- [ ] Create `DeltaMutationRequest` extending base
- [ ] Refactor 17 Delta*Request classes to use inheritance
- [ ] Update all call sites using kwargs pattern
- [ ] Run ast-grep verification pattern
- [ ] Run cq calls analysis to verify no breakage
- [ ] Add unit tests for inheritance behavior

### Decommissioning List
- Remove duplicated `table_uri`, `storage_options`, `version`, `timestamp`, `gate` fields from 17 classes
- Estimated reduction: 80-100 lines

---

## 2. Fingerprinting Infrastructure Unification

### Problem Statement
Seven files implement identical `fingerprint_payload()` + `fingerprint()` method pairs, violating DRY principles. The `fingerprint()` method is always a trivial wrapper calling `config_fingerprint(self.fingerprint_payload())`.

### Current State (from cq analysis)
```python
# Found in 7 files with identical pattern:
# - src/cache/diskcache_factory.py (DiskCacheSettings, DiskCacheProfile)
# - src/storage/deltalake/config.py (DeltaWritePolicy, ParquetWriterPolicy, DeltaSchemaPolicy)
# - src/datafusion_engine/delta_store_policy.py (DeltaStorePolicy)
# - src/obs/otel/config.py (OtelConfig)

# Example from src/cache/diskcache_factory.py:
def fingerprint_payload(self) -> Mapping[str, object]:
    return {
        "size_limit_bytes": self.size_limit_bytes,
        "cull_limit": self.cull_limit,
        "eviction_policy": self.eviction_policy,
        # ... more fields
    }

def fingerprint(self) -> str:
    return config_fingerprint(self.fingerprint_payload())
```

### Target Implementation
```python
# src/core/config_base.py (extend existing)
from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, runtime_checkable


@runtime_checkable
class FingerprintableConfig(Protocol):
    """Protocol for configs that produce a fingerprint payload."""

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return payload dict for fingerprinting."""
        ...


class FingerprintMixin:
    """Mixin providing fingerprint() from fingerprint_payload().

    Classes using this mixin must implement fingerprint_payload().
    """

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

### ast-grep Recipes

**Discovery - Find all fingerprint method pairs:**
```bash
# Find classes with fingerprint_payload method
ast-grep run -p 'def fingerprint_payload(self) -> $RETURN:
    $$$BODY' -l python src/

# Find the paired fingerprint() methods
ast-grep run -p 'def fingerprint(self) -> str:
    return config_fingerprint(self.fingerprint_payload())' -l python src/

# Find classes with both methods (complex relational pattern)
ast-grep scan --inline-rules "$(cat <<'YAML'
id: fingerprint-pair
language: Python
rule:
  kind: class_definition
  has:
    pattern: def fingerprint_payload(self)
  has:
    pattern: def fingerprint(self) -> str
severity: info
message: Class has fingerprint method pair - candidate for mixin
YAML
)" src/
```

**Verification - Ensure mixin adoption:**
```bash
# After refactor, find any remaining direct fingerprint() implementations
ast-grep scan --inline-rules "$(cat <<'YAML'
id: fingerprint-without-mixin
language: Python
rule:
  pattern: |
    def fingerprint(self) -> str:
        return config_fingerprint($$$)
  not:
    inside:
      pattern: class $_(FingerprintMixin)
severity: warning
message: Consider using FingerprintMixin instead
YAML
)" src/

# Verify mixin usage
ast-grep run -p 'class $NAME(FingerprintMixin$$$):' -l python src/
```

### cq Recipes

**Discovery:**
```bash
# Find all call sites of config_fingerprint
./scripts/cq calls config_fingerprint --root .

# Find all call sites of fingerprint methods
./scripts/cq calls fingerprint --root .

# Check for import-time issues with the base module
./scripts/cq imports --module src.core.config_base --root .
```

**Post-refactor verification:**
```bash
# Verify fingerprint behavior unchanged
./scripts/cq calls DiskCacheSettings.fingerprint --root .

# Check for any new exceptions introduced
./scripts/cq exceptions --root src/core
```

### Implementation Checklist
- [ ] Add `FingerprintableConfig` protocol to `core/config_base.py`
- [ ] Add `FingerprintMixin` class
- [ ] Add `auto_fingerprint()` utility function
- [ ] Refactor `DiskCacheSettings` to use mixin
- [ ] Refactor `DiskCacheProfile` to use mixin
- [ ] Refactor Delta policy classes to use mixin
- [ ] Refactor `OtelConfig` to use mixin
- [ ] Run ast-grep to verify no remaining direct implementations

### Decommissioning List
- Remove 7+ identical `fingerprint()` method implementations
- Estimated reduction: 50-70 lines

---

## 3. Storage Options Type Consolidation

### Problem Statement
`storage_options` and `log_storage_options` fields appear 126 times across 23 files with inconsistent typing (`Mapping[str, str] | None`, `dict[str, str] | None`, `StorageOptions | None`).

### Current State
```python
# Various patterns found:
storage_options: Mapping[str, str] | None = None  # 45 occurrences
storage_options: StorageOptions | None = None     # 12 occurrences (inconsistent definition)
storage_options: dict[str, str] | None = None     # 8 occurrences

# Log storage options similar pattern:
log_storage_options: Mapping[str, str] | None = None
delta_log_storage_options: Mapping[str, str] | None = None
```

### Target Implementation
```python
# src/core/storage_types.py
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TypeAlias

StorageOptionsType: TypeAlias = Mapping[str, str] | None
LogStorageOptionsType: TypeAlias = Mapping[str, str] | None


@dataclass(frozen=True)
class DeltaStorageSpec:
    """Canonical storage options for Delta operations."""

    storage_options: StorageOptionsType = None
    log_storage_options: LogStorageOptionsType = None

    def merged(self, *, fallback_log_to_storage: bool = False) -> dict[str, str] | None:
        """Merge storage and log options."""
        from utils.storage_options import merged_storage_options
        return merged_storage_options(
            self.storage_options,
            self.log_storage_options,
            fallback_log_to_storage=fallback_log_to_storage,
        )

    def as_list_pairs(self) -> list[tuple[str, str]] | None:
        """Return storage options as list of tuples for Rust FFI."""
        if self.storage_options is None:
            return None
        return list(self.storage_options.items())
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/core/storage_types.py` | Type aliases and DeltaStorageSpec |
| Modify | `src/datafusion_engine/delta_control_plane.py` | Use type aliases |
| Modify | `src/datafusion_engine/dataset_registry.py` | Use type aliases |
| Modify | `src/storage/deltalake/delta.py` | Use type aliases |
| Modify | `src/incremental/*.py` | Use type aliases |

### ast-grep Recipes

**Discovery - Find all storage options field patterns:**
```bash
# Find all storage_options field definitions with explicit Mapping type
ast-grep run -p 'storage_options: Mapping[str, str]' -l python src/

# Find inconsistent dict usage
ast-grep run -p 'storage_options: dict[str, str]' -l python src/

# Find log storage options variants
ast-grep run -p 'log_storage_options: $TYPE' -l python src/
ast-grep run -p 'delta_log_storage_options: $TYPE' -l python src/

# Count total occurrences
ast-grep run -p 'storage_options:' -l python src/ --json=stream | wc -l
```

**Verification - Ensure canonical types used:**
```bash
# After refactor, find any remaining inline Mapping[str, str]
ast-grep scan --inline-rules "$(cat <<'YAML'
id: inline-storage-type
language: Python
rule:
  any:
    - pattern: 'storage_options: Mapping[str, str]'
    - pattern: 'storage_options: dict[str, str]'
severity: warning
message: Use StorageOptionsType alias instead
YAML
)" src/

# Verify canonical type usage
ast-grep run -p 'storage_options: StorageOptionsType' -l python src/
```

### cq Recipes

**Discovery:**
```bash
# Find all usages of storage_options parameter
./scripts/cq impact delta_provider_from_session --param request --root .

# Check for storage options merging patterns
./scripts/cq calls merged_storage_options --root .

# Analyze field access patterns
./scripts/cq bytecode-surface src/datafusion_engine/dataset_registry.py --show attrs
```

### Implementation Checklist
- [ ] Create `src/core/storage_types.py` with type aliases
- [ ] Add `DeltaStorageSpec` dataclass
- [ ] Update imports across 23 files to use canonical types
- [ ] Replace inline field definitions where appropriate
- [ ] Run ast-grep verification to ensure consistency

### Decommissioning List
- Remove inconsistent type annotations
- Estimated standardization: 126 occurrences unified

---

## 4. Rust Error Hierarchy Consolidation

### Problem Statement
Parallel error types in `datafusion_ext` and `datafusion_python` with overlapping variants and redundant wrapping. `PyDataFusionError::ExtError(ExtError)` wraps the entire `ExtError` enum, while also having variants that duplicate `ExtError` variants (like `ArrowError`, `ExecutionError`).

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
    ExecutionError(Box<InnerDataFusionError>),  // Duplicates ExtError::DataFusion
    ArrowError(ArrowError),                      // Duplicates ExtError::Arrow
    Common(String),                              // Duplicates ExtError::Generic
    PythonError(PyErr),
    EncodeError(EncodeError),
    ExtError(ExtError),  // Wrapper creating nested errors!
}
```

### Target Implementation
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
    Ext(ExtError),      // Unified wrapper
    Python(PyErr),      // Python-specific only
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

// Convenience conversions (delegate to ExtError)
impl From<ArrowError> for PyDataFusionError {
    fn from(err: ArrowError) -> Self {
        PyDataFusionError::Ext(ExtError::Arrow(err))
    }
}

impl From<DataFusionError> for PyDataFusionError {
    fn from(err: DataFusionError) -> Self {
        PyDataFusionError::Ext(ExtError::DataFusion(Box::new(err)))
    }
}
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `rust/datafusion_ext/src/errors.rs` | Add ToPyError trait, Encode variant |
| Modify | `rust/datafusion_python/src/errors.rs` | Simplify to 2 variants |
| Modify | `rust/datafusion_python/src/udf.rs` | Update error handling |
| Modify | `rust/datafusion_python/src/udaf.rs` | Update error handling |
| Modify | `rust/datafusion_python/src/udwf.rs` | Update error handling |

### ast-grep Recipes

**Discovery - Find error variant usage:**
```bash
# Find PyDataFusionError variant constructors
ast-grep run -p 'PyDataFusionError::$VARIANT($ARG)' -l rust rust/datafusion_python/

# Find error conversions
ast-grep run -p 'impl From<$TYPE> for PyDataFusionError' -l rust rust/

# Find ExtError wrapping (nested errors)
ast-grep run -p 'PyDataFusionError::ExtError($ERR)' -l rust rust/

# Count variant usage
ast-grep run -p 'PyDataFusionError::ArrowError' -l rust rust/ --json=stream | wc -l
ast-grep run -p 'PyDataFusionError::ExecutionError' -l rust rust/ --json=stream | wc -l
```

**Verification - Ensure clean hierarchy:**
```bash
# After refactor, verify no duplicate variants remain
ast-grep run -p 'PyDataFusionError::ArrowError' -l rust rust/  # Should find 0
ast-grep run -p 'PyDataFusionError::ExecutionError' -l rust rust/  # Should find 0
ast-grep run -p 'PyDataFusionError::Common' -l rust rust/  # Should find 0

# Verify new pattern adopted
ast-grep run -p 'PyDataFusionError::Ext($ERR)' -l rust rust/datafusion_python/
ast-grep run -p 'PyDataFusionError::Python($ERR)' -l rust rust/datafusion_python/
```

### Implementation Checklist
- [ ] Add `ToPyError` trait to `datafusion_ext`
- [ ] Move `Encode` variant to `ExtError`
- [ ] Simplify `PyDataFusionError` to two variants
- [ ] Add convenience `From` implementations for common types
- [ ] Update all error handling in `datafusion_python`
- [ ] Run `cargo test --workspace` to verify
- [ ] Run ast-grep verification patterns

### Decommissioning List
- Remove redundant `ArrowError`, `ExecutionError`, `Common`, `EncodeError` variants from `PyDataFusionError`
- Estimated reduction: 40-60 lines

---

## 5. PyArrow Conversion Bridge

### Problem Statement
PyArrow ↔ Rust array conversions duplicated across `udf.rs`, `udaf.rs`, `udwf.rs` with 14 occurrences of `.to_pyarrow()` calls and error-prone `.unwrap()` patterns.

### Current State
```rust
// Pattern repeated in udf.rs, udaf.rs, udwf.rs:
// rust/datafusion_python/src/udaf.rs:76
.map(|arg| arg.into_data().to_pyarrow(py).unwrap())

// rust/datafusion_python/src/udaf.rs:122
.map(|arg| arg.into_data().to_pyarrow(py).unwrap())

// rust/datafusion_python/src/udwf.rs:103
.map(|arg| arg.into_data().to_pyarrow(py).unwrap())
```

### Target Implementation
```rust
// rust/datafusion_python/src/pyarrow_bridge.rs
use datafusion::arrow::array::{ArrayRef, make_array};
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::errors::{PyDataFusionError, PyDataFusionResult};

/// Convert Rust arrays to a Python tuple of PyArrow arrays.
pub fn arrays_to_pyarrow_tuple<'py>(
    py: Python<'py>,
    arrays: &[ArrayRef],
) -> PyResult<Bound<'py, PyTuple>> {
    let py_arrays: Vec<PyObject> = arrays
        .iter()
        .map(|arr| {
            arr.into_data()
                .to_pyarrow(py)
                .map(|a| a.unbind())
        })
        .collect::<PyResult<Vec<_>>>()?;
    PyTuple::new(py, py_arrays)
}

/// Convert a PyArrow array to a Rust ArrayRef.
pub fn pyarrow_to_array(obj: &Bound<'_, PyAny>) -> PyResult<ArrayRef> {
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
    pyarrow_to_array(&result)
}

/// Convert arrays with proper error handling (no unwrap).
pub fn arrays_to_pyarrow_vec(
    py: Python<'_>,
    arrays: &[ArrayRef],
) -> PyDataFusionResult<Vec<PyObject>> {
    arrays
        .iter()
        .map(|arr| {
            arr.into_data()
                .to_pyarrow(py)
                .map(|a| a.unbind())
                .map_err(PyDataFusionError::from)
        })
        .collect()
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

### ast-grep Recipes

**Discovery - Find all PyArrow conversions:**
```bash
# Find to_pyarrow calls
ast-grep run -p '$EXPR.to_pyarrow($PY)' -l rust rust/datafusion_python/src/

# Find into_data().to_pyarrow() chain
ast-grep run -p '$ARR.into_data().to_pyarrow($PY)' -l rust rust/datafusion_python/

# Find from_pyarrow patterns
ast-grep run -p 'ArrayData::from_pyarrow_bound($ARG)' -l rust rust/datafusion_python/

# Find error-prone unwrap pattern
ast-grep run -p '.to_pyarrow($PY).unwrap()' -l rust rust/

# Count occurrences
ast-grep run -p '.to_pyarrow(' -l rust rust/datafusion_python/ --json=stream | wc -l
```

**Verification - Ensure bridge adoption:**
```bash
# After refactor, find any remaining direct to_pyarrow in UDF files
ast-grep run -p '$EXPR.to_pyarrow($PY)' -l rust rust/datafusion_python/src/udf.rs
ast-grep run -p '$EXPR.to_pyarrow($PY)' -l rust rust/datafusion_python/src/udaf.rs
ast-grep run -p '$EXPR.to_pyarrow($PY)' -l rust rust/datafusion_python/src/udwf.rs
# Should only find calls in pyarrow_bridge.rs

# Verify bridge functions are used
ast-grep run -p 'arrays_to_pyarrow_tuple($PY, $ARGS)' -l rust rust/datafusion_python/
ast-grep run -p 'call_pyarrow_udf($$$)' -l rust rust/datafusion_python/

# Verify no unwrap patterns remain
ast-grep run -p '.to_pyarrow($PY).unwrap()' -l rust rust/datafusion_python/src/u*.rs
# Should find 0
```

### Implementation Checklist
- [ ] Create `pyarrow_bridge.rs` with core conversion functions
- [ ] Add `arrays_to_pyarrow_tuple()` function
- [ ] Add `pyarrow_to_array()` function
- [ ] Add `call_pyarrow_udf()` helper
- [ ] Add `arrays_to_pyarrow_vec()` with proper error handling
- [ ] Refactor `udf.rs` to use bridge
- [ ] Refactor `udaf.rs` to use bridge
- [ ] Refactor `udwf.rs` to use bridge
- [ ] Remove `.unwrap()` calls, use proper error propagation
- [ ] Add unit tests for conversions

### Decommissioning List
- Remove duplicated conversion code from 3+ files
- Remove `.unwrap()` patterns in favor of proper error handling
- Estimated reduction: 80-120 lines

---

## 6. Delta Options Struct Unification (Rust)

### Problem Statement
`DeltaProviderOptions` and `DeltaCdfProviderOptions` defined locally in plugin crate but should be in `datafusion_ext` for cross-crate reuse.

### Target Implementation
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
pub struct DeltaFeatureGateOptions {
    pub min_reader_version: Option<i32>,
    pub min_writer_version: Option<i32>,
    pub required_reader_features: Option<Vec<String>>,
    pub required_writer_features: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DeltaProviderOptions {
    #[serde(flatten)]
    pub base: DeltaTableOptions,
    #[serde(flatten)]
    pub gate: DeltaFeatureGateOptions,
    pub file_column_name: Option<String>,
    pub enable_parquet_pushdown: Option<bool>,
    pub schema_force_view_types: Option<bool>,
    pub wrap_partition_values: Option<bool>,
    pub files: Option<Vec<String>>,
}
```

### ast-grep Recipes

**Discovery:**
```bash
# Find option struct definitions
ast-grep run -p 'struct Delta$NAMEOptions {
    $$$FIELDS
}' -l rust rust/

# Find serde derives on Delta structs
ast-grep run -p '#[derive($$$Deserialize$$$)]
$$$
struct Delta$NAME' -l rust rust/
```

**Verification:**
```bash
# After refactor, verify imports from datafusion_ext
ast-grep run -p 'use datafusion_ext::delta_options::$TYPE' -l rust rust/df_plugin_codeanatomy/
```

### Implementation Checklist
- [ ] Create `rust/datafusion_ext/src/delta_options.rs`
- [ ] Define `DeltaTableOptions` base struct
- [ ] Define `DeltaFeatureGateOptions` struct
- [ ] Use `#[serde(flatten)]` for composition
- [ ] Export from `datafusion_ext`
- [ ] Update `df_plugin_codeanatomy` to use exported types
- [ ] Run `cargo test --workspace`

### Decommissioning List
- Remove duplicated option structs from `df_plugin_codeanatomy`
- Estimated reduction: 50-70 lines

---

## 7. UDF Builder Macro System

### Problem Statement
Repetitive UDAF wrapper functions in `udaf_builtin.rs` with identical patterns.

### Current State
```rust
// rust/datafusion_ext/src/udaf_builtin.rs - 8+ functions with same pattern:
fn list_unique_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(ListUniqueUdaf::new()))
}

fn collect_set_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(CollectSetUdaf::new()))
}

fn count_distinct_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(CountDistinctUdaf::new()))
}
```

### Target Implementation
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

pub(crate) use udaf_wrapper;
```

```rust
// Usage in udaf_builtin.rs:
use crate::udf_macros::udaf_wrapper;

udaf_wrapper!(list_unique_udaf, ListUniqueUdaf);
udaf_wrapper!(collect_set_udaf, CollectSetUdaf);
udaf_wrapper!(count_distinct_udaf, CountDistinctUdaf);
udaf_wrapper!(count_if_udaf, CountIfUdaf);
udaf_wrapper!(any_value_det_udaf, AnyValueDetUdaf);
udaf_wrapper!(arg_max_udaf, ArgMaxUdaf);
udaf_wrapper!(arg_min_udaf, ArgMinUdaf);
udaf_wrapper!(asof_select_udaf, AsofSelectUdaf);
```

### ast-grep Recipes

**Discovery - Find wrapper function pattern:**
```bash
# Find UDAF wrapper functions
ast-grep run -p 'fn $NAME() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new($IMPL::new()))
}' -l rust rust/datafusion_ext/

# Count occurrences
ast-grep run -p 'AggregateUDF::new_from_shared_impl(Arc::new($IMPL::new()))' -l rust rust/ --json=stream | wc -l
```

**Verification:**
```bash
# After refactor, verify macro usage
ast-grep run -p 'udaf_wrapper!($NAME, $IMPL)' -l rust rust/datafusion_ext/

# Verify no remaining manual wrappers
ast-grep run -p 'fn $NAME() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl($$$)
}' -l rust rust/datafusion_ext/src/udaf_builtin.rs
# Should find 0 (all converted to macros)
```

### Implementation Checklist
- [ ] Create `rust/datafusion_ext/src/udf_macros.rs`
- [ ] Implement `udaf_wrapper!` macro with basic and alias variants
- [ ] Implement `udwf_wrapper!` macro
- [ ] Convert 8 UDAF wrapper functions to macro invocations
- [ ] Run `cargo test -p datafusion_ext`

### Decommissioning List
- Remove 8 boilerplate wrapper functions
- Estimated reduction: 30-40 lines

---

## 8. Hamilton Tag Policy Unification

### Problem Statement
Hamilton tag constants and validation logic scattered across `semantic_registry.py` and other modules without centralization.

### Current State
```python
# src/hamilton_pipeline/semantic_registry.py
_SEMANTIC_LAYER = "semantic"
_REQUIRED_SEMANTIC_TAGS: frozenset[str] = frozenset(
    {
        "layer",
        "artifact",
        "semantic_id",
        "kind",
        "entity",
        "grain",
        "version",
        "stability",
    }
)
```

### Target Implementation
```python
# src/hamilton_pipeline/tag_policy.py
from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum


class HamiltonTagKey(StrEnum):
    """Canonical Hamilton tag keys."""

    LAYER = "layer"
    ARTIFACT = "artifact"
    SEMANTIC_ID = "semantic_id"
    KIND = "kind"
    ENTITY = "entity"
    GRAIN = "grain"
    VERSION = "version"
    STABILITY = "stability"
    SCHEMA_REF = "schema_ref"
    MATERIALIZATION = "materialization"
    MATERIALIZED_NAME = "materialized_name"
    ENTITY_KEYS = "entity_keys"
    JOIN_KEYS = "join_keys"


class SemanticLayer(StrEnum):
    """Semantic layer values."""

    SEMANTIC = "semantic"
    INTERMEDIATE = "intermediate"
    RAW = "raw"


REQUIRED_SEMANTIC_TAGS: frozenset[str] = frozenset({
    HamiltonTagKey.LAYER,
    HamiltonTagKey.ARTIFACT,
    HamiltonTagKey.SEMANTIC_ID,
    HamiltonTagKey.KIND,
    HamiltonTagKey.ENTITY,
    HamiltonTagKey.GRAIN,
    HamiltonTagKey.VERSION,
    HamiltonTagKey.STABILITY,
})


def validate_semantic_tags(tags: Mapping[str, object]) -> list[str]:
    """Return list of missing required tags.

    Returns
    -------
    list[str]
        Names of missing or invalid tags.
    """
    missing = []
    for key in REQUIRED_SEMANTIC_TAGS:
        value = tags.get(key)
        if not isinstance(value, str) or not value.strip():
            missing.append(key)
    return missing
```

### cq Recipes

```bash
# Find tag validation patterns
./scripts/cq calls _missing_required_tags --root .

# Check semantic registry usage
./scripts/cq calls compile_semantic_registry --root .

# Find tag constant usages
./scripts/cq bytecode-surface src/hamilton_pipeline/semantic_registry.py --show globals
```

### Implementation Checklist
- [ ] Create `src/hamilton_pipeline/tag_policy.py`
- [ ] Define `HamiltonTagKey` enum
- [ ] Define `SemanticLayer` enum
- [ ] Centralize `REQUIRED_SEMANTIC_TAGS`
- [ ] Add `validate_semantic_tags()` helper
- [ ] Update `semantic_registry.py` to use centralized constants

---

## 9. DataFusion Session Factory Consolidation

### Problem Statement
Session context creation scattered across multiple files with slightly different configuration patterns.

### cq Recipes

```bash
# Find session context creation
./scripts/cq calls SessionContext --root .

# Find existing factory functions
./scripts/cq calls create_session_context --root .

# Check for config patterns
./scripts/cq impact create_session_context --param config --root .

# Find RuntimeConfig usage
./scripts/cq calls RuntimeConfig --root .
```

### Implementation Checklist
- [ ] Audit existing session creation patterns
- [ ] Design unified `SessionFactory` class
- [ ] Consolidate config application logic
- [ ] Update all session creation call sites

---

## 10. Schema Contract Type Alignment

### Problem Statement
Multiple schema contract types with overlapping responsibilities need alignment.

### cq Recipes

```bash
# Find schema contract usages
./scripts/cq calls TableSchemaContract --root .
./scripts/cq calls table_schema_contract --root .

# Check for schema validation patterns
./scripts/cq exceptions --include "src/datafusion_engine/" --root .
```

### Implementation Checklist
- [ ] Audit schema contract types
- [ ] Identify overlap and unification opportunities
- [ ] Design canonical schema contract hierarchy
- [ ] Update dependent modules

---

## 11. Delta Control Plane Function Consolidation

### Problem Statement
Many Delta control plane functions share repeated patterns for payload construction and Rust FFI calls.

### Current State
```python
# Each function repeats this pattern:
def delta_some_operation(ctx: SessionContext, *, request: SomeRequest) -> Mapping[str, object]:
    some_fn = _require_internal_entrypoint("delta_some_operation")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload = _commit_payload(request.commit_options)
    response = some_fn(
        ctx,
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        # ... operation-specific args
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
        commit_payload[0],
        commit_payload[1],
        commit_payload[2],
        commit_payload[3],
        commit_payload[4],
        commit_payload[5],
    )
    return ensure_mapping(response, label="delta_some_operation")
```

### Target Implementation
```python
# Helper for common control plane call pattern
def _call_delta_control_plane(
    ctx: SessionContext,
    entrypoint: str,
    request: DeltaBaseRequest,
    *extra_args: object,
    commit_options: DeltaCommitOptions | None = None,
) -> Mapping[str, object]:
    """Execute a Delta control plane operation with standard argument marshaling."""
    fn = _require_internal_entrypoint(entrypoint)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload = _commit_payload(commit_options)
    response = fn(
        ctx,
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        *extra_args,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
        commit_payload[0],
        commit_payload[1],
        commit_payload[2],
        commit_payload[3],
        commit_payload[4],
        commit_payload[5],
    )
    return ensure_mapping(response, label=entrypoint)
```

### Implementation Checklist
- [ ] Create `_call_delta_control_plane` helper
- [ ] Refactor Delta mutation functions to use helper
- [ ] Ensure consistent error handling

---

## 12. Provider Contract Unification

### Problem Statement
Provider contracts for Delta, listing, and CDF providers have similar interfaces but no shared protocol.

### cq Recipes

```bash
# Find provider construction patterns
./scripts/cq calls delta_provider_from_session --root .
./scripts/cq calls delta_cdf_provider --root .

# Analyze provider bundle types
./scripts/cq calls DeltaProviderBundle --root .
./scripts/cq calls DeltaCdfProviderBundle --root .
```

### Implementation Checklist
- [ ] Define `ProviderContract` protocol
- [ ] Align `DeltaProviderBundle` and `DeltaCdfProviderBundle`
- [ ] Create unified provider factory interface

---

## 13. IO Adapter Pattern Consolidation

### Problem Statement
IO adapter patterns for different data formats have duplicated method signatures and error handling.

### cq Recipes

```bash
# Find IO adapter implementations
./scripts/cq calls IOAdapter --root .

# Check for format-specific adapters
./scripts/cq imports --module src.datafusion_engine.io_adapter --root .
```

### Implementation Checklist
- [ ] Audit IO adapter patterns
- [ ] Define canonical adapter protocol
- [ ] Consolidate common functionality

---

## 14. Structured Logging Unification

### Problem Statement
Structured logging patterns scattered across modules need centralization.

### cq Recipes

```bash
# Find logging patterns
./scripts/cq calls structlog --root .
./scripts/cq calls logger.info --root .
./scripts/cq calls logger.debug --root .

# Check for log context patterns
./scripts/cq bytecode-surface src/obs --show globals
```

### Implementation Checklist
- [ ] Audit current logging patterns
- [ ] Design unified structured logging helpers
- [ ] Consolidate log context management

---

## Cross-Scope Dependencies

| Scope | Depends On | Notes |
|-------|------------|-------|
| #1 Delta Request Base | #3 Storage Options | Uses StorageOptionsType |
| #2 Fingerprinting | None | Foundation |
| #3 Storage Options | None | Foundation |
| #4 Rust Errors | None | Foundation |
| #5 PyArrow Bridge | #4 Rust Errors | Uses error types |
| #6 Delta Options Rust | #4 Rust Errors | Uses error handling |
| #7 UDF Macros | None | Foundation |
| #8 Tag Policy | None | Foundation |
| #9 Session Factory | None | Independent |
| #10 Schema Contract | None | Independent |
| #11 Control Plane | #1 Delta Request Base | Uses base classes |
| #12 Provider Contract | #11 Control Plane | Builds on unified base |
| #13 IO Adapter | None | Independent |
| #14 Structured Logging | None | Independent |

---

## Recommended Implementation Order

**Phase 1: Foundations**
1. Storage Options Type Consolidation (#3)
2. Fingerprinting Infrastructure (#2)
3. Tag Policy Unification (#8)

**Phase 2: Core Patterns**
4. Delta Request Base Pattern (#1)
5. Delta Control Plane Function Consolidation (#11)
6. Provider Contract Unification (#12)

**Phase 3: Rust Consolidation**
7. Rust Error Hierarchy (#4)
8. PyArrow Conversion Bridge (#5)
9. Delta Options Struct Unification (#6)
10. UDF Builder Macros (#7)

**Phase 4: Infrastructure**
11. DataFusion Session Factory (#9)
12. Schema Contract Alignment (#10)
13. IO Adapter Consolidation (#13)
14. Structured Logging Unification (#14)

---

## Verification Commands Summary

### Python Analysis (cq)
```bash
# Import cycles check
./scripts/cq imports --cycles --root .

# Side effects analysis
./scripts/cq side-effects --root src

# Call site analysis
./scripts/cq calls <function_name> --root .

# Parameter flow analysis
./scripts/cq impact <function> --param <param> --root .

# Signature change impact
./scripts/cq sig-impact <function> --to "<new_signature>" --root .

# Exception handling analysis
./scripts/cq exceptions --root src/<module>

# Bytecode surface analysis
./scripts/cq bytecode-surface <file> --show globals,attrs
```

### Rust Analysis (ast-grep)
```bash
# Find patterns
ast-grep run -p '<pattern>' -l rust rust/

# Run inline rules for complex matching
ast-grep scan --inline-rules "<yaml>" rust/

# Rewrite (preview)
ast-grep run -p '<old>' -r '<new>' -l rust rust/

# Apply rewrites
ast-grep run -p '<old>' -r '<new>' -l rust -U rust/
```

### Test Suite
```bash
# Python tests
uv run pytest tests/unit/ -v
uv run pytest tests/ -m "not e2e"

# Type checking
uv run pyright --warnings --pythonversion=3.13
uv run pyrefly check

# Linting
uv run ruff check --fix

# Rust tests
cd rust && cargo test --workspace
```

---

## Summary Statistics

| Metric | Value |
|--------|-------|
| Total Scopes | 14 |
| Files to Create | 8 |
| Files to Modify | 45+ |
| Estimated Code Reduction | 1,300-1,800 lines |
| Rust Consolidation | 4 scopes |
| Python Consolidation | 10 scopes |

---

## Appendix: Tool Recipe Quick Reference

### ast-grep Pattern Syntax
| Pattern | Matches | Example |
|---------|---------|---------|
| `$VAR` | Single AST node | `foo($X)` matches `foo(1)`, `foo(a+b)` |
| `$$$VAR` | Zero or more nodes | `foo($$$)` matches `foo()`, `foo(a,b,c)` |
| `$_VAR` | Non-capturing | `$_FUNC($_ARG)` |

### cq Commands Quick Reference
| Command | Purpose | Example |
|---------|---------|---------|
| `calls` | Find all call sites | `./scripts/cq calls MyFunc --root .` |
| `impact` | Trace parameter flow | `./scripts/cq impact func --param name --root .` |
| `sig-impact` | Test signature changes | `./scripts/cq sig-impact func --to "func(a, b=None)" --root .` |
| `imports --cycles` | Detect import cycles | `./scripts/cq imports --cycles --root .` |
| `exceptions` | Analyze error handling | `./scripts/cq exceptions --root src/` |
| `side-effects` | Find import-time effects | `./scripts/cq side-effects --root src/` |
| `scopes` | Analyze closure captures | `./scripts/cq scopes file.py --root .` |
| `bytecode-surface` | Find hidden dependencies | `./scripts/cq bytecode-surface file.py --show globals,attrs` |
