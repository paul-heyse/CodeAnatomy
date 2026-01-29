# Extended Codebase Consolidation Opportunities

This document identifies **additional consolidation opportunities** beyond those covered in `codebase_consolidation_opportunities.md`. These represent the next phase of technical debt reduction and code standardization.

---

## Table of Contents

1. [Category A: Error Handling & Validation](#category-a-error-handling--validation)
   - [A2: Type/Instance Validation Consolidation](#a2-typeinstance-validation-consolidation)
   - [A3: Missing Item Validation Consolidation](#a3-missing-item-validation-consolidation)
2. [Category B: Extract Module Standardization](#category-b-extract-module-standardization)
   - [B1: Extract Options Base Classes](#b1-extract-options-base-classes)
   - [B2: Schema Fingerprint Caching](#b2-schema-fingerprint-caching)
3. [Category C: DataFusion Session Management](#category-c-datafusion-session-management)
   - [C1: Table Registration/Deregistration](#c1-table-registrationderegistration)
   - [C2: SessionConfig Application Chain](#c2-sessionconfig-application-chain)
4. [Category D: Arrow/PyArrow Interop](#category-d-arrowpyarrow-interop)
   - [D1: Type Coercion & Schema Manipulation](#d1-type-coercion--schema-manipulation)
   - [D2: RecordBatchReader Handling](#d2-recordbatchreader-handling)
   - [D3: Schema Creation Helpers](#d3-schema-creation-helpers)
5. [Category E: Diagnostics & Telemetry](#category-e-diagnostics--telemetry)
   - [E1: DiagnosticsCollector Record Methods](#e1-diagnosticscollector-record-methods)
   - [E2: Callable Attribute Check Pattern](#e2-callable-attribute-check-pattern)
6. [Category F: File I/O Standardization](#category-f-file-io-standardization)
   - [F1: TOML/JSON File I/O](#f1-tomljson-file-io)
7. [Exception Wrapping Analysis (Not Recommended)](#exception-wrapping-analysis-not-recommended)
8. [New Files to Create](#new-files-to-create)
9. [Implementation Order](#implementation-order)
10. [Cross-Scope Dependencies](#cross-scope-dependencies)
11. [Verification Steps](#verification-steps)

---

## Category A: Error Handling & Validation

### A2: Type/Instance Validation Consolidation

#### Architecture Overview

The codebase contains **20+ occurrences** of type/instance validation helpers with identical patterns:
- `_ensure_mapping()` - validates input is a Mapping
- `_ensure_table()` - validates input is a PyArrow Table
- `_ensure_callable()` - validates input is callable

These are scattered across Delta, encoding, streaming, and runtime modules.

#### Current State: Duplicated Implementations

**Location 1:** `src/datafusion_engine/delta_control_plane.py`
```python
def _ensure_mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        msg = f"{label} must be a Mapping, got {type(value).__name__}"
        raise TypeError(msg)
    return value
```

**Location 2:** `src/datafusion_engine/encoding.py`
```python
def _ensure_table(value: TableLike, *, label: str = "input") -> pa.Table:
    if isinstance(value, pa.Table):
        return value
    if isinstance(value, pa.RecordBatch):
        return pa.Table.from_batches([value])
    if isinstance(value, pa.RecordBatchReader):
        return pa.Table.from_batches(list(value))
    msg = f"{label} must be Table/RecordBatch/Reader, got {type(value).__name__}"
    raise TypeError(msg)
```

**Location 3:** `src/datafusion_engine/streaming_executor.py`
```python
def _ensure_callable(value: object, *, label: str) -> Callable[..., object]:
    if not callable(value):
        msg = f"{label} must be callable, got {type(value).__name__}"
        raise TypeError(msg)
    return value
```

**Additional Locations:**
- `src/relspec/runtime_artifacts.py`
- `src/incremental/deltas.py`
- `src/datafusion_engine/io_adapter.py`

#### Target Implementation

Create `src/utils/validation.py`:

```python
"""Type and instance validation utilities."""

from __future__ import annotations

from collections.abc import Callable, Container, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    import pyarrow as pa

T = TypeVar("T")


def ensure_mapping(
    value: object,
    *,
    label: str,
    error_type: type[Exception] = TypeError,
) -> Mapping[str, object]:
    """Validate that value is a Mapping.

    Parameters
    ----------
    value
        Value to validate.
    label
        Descriptive label for error messages.
    error_type
        Exception type to raise on validation failure.

    Returns
    -------
    Mapping[str, object]
        The validated mapping.

    Raises
    ------
    TypeError
        If value is not a Mapping.
    """
    if not isinstance(value, Mapping):
        msg = f"{label} must be a Mapping, got {type(value).__name__}"
        raise error_type(msg)
    return value


def ensure_table(
    value: pa.Table | pa.RecordBatch | pa.RecordBatchReader,
    *,
    label: str = "input",
) -> pa.Table:
    """Convert TableLike to PyArrow Table.

    Parameters
    ----------
    value
        Table, RecordBatch, or RecordBatchReader.
    label
        Descriptive label for error messages.

    Returns
    -------
    pa.Table
        The converted table.

    Raises
    ------
    TypeError
        If value cannot be converted to Table.
    """
    import pyarrow as pa

    if isinstance(value, pa.Table):
        return value
    if isinstance(value, pa.RecordBatch):
        return pa.Table.from_batches([value])
    if isinstance(value, pa.RecordBatchReader):
        return pa.Table.from_batches(list(value))
    msg = f"{label} must be Table/RecordBatch/RecordBatchReader, got {type(value).__name__}"
    raise TypeError(msg)


def ensure_sequence(
    value: object,
    *,
    label: str,
    item_type: type | None = None,
) -> Sequence[object]:
    """Validate that value is a Sequence.

    Parameters
    ----------
    value
        Value to validate.
    label
        Descriptive label for error messages.
    item_type
        Optional type to validate sequence items against.

    Returns
    -------
    Sequence
        The validated sequence.

    Raises
    ------
    TypeError
        If value is not a Sequence or items don't match item_type.
    """
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        msg = f"{label} must be a Sequence, got {type(value).__name__}"
        raise TypeError(msg)
    if item_type is not None:
        for i, item in enumerate(value):
            if not isinstance(item, item_type):
                msg = f"{label}[{i}] must be {item_type.__name__}, got {type(item).__name__}"
                raise TypeError(msg)
    return value


def ensure_callable(
    value: object,
    *,
    label: str,
) -> Callable[..., object]:
    """Validate that value is callable.

    Parameters
    ----------
    value
        Value to validate.
    label
        Descriptive label for error messages.

    Returns
    -------
    Callable
        The validated callable.

    Raises
    ------
    TypeError
        If value is not callable.
    """
    if not callable(value):
        msg = f"{label} must be callable, got {type(value).__name__}"
        raise TypeError(msg)
    return value


__all__ = [
    "ensure_mapping",
    "ensure_table",
    "ensure_sequence",
    "ensure_callable",
]
```

#### Target File List

| File | Functions to Replace |
|------|---------------------|
| `src/datafusion_engine/delta_control_plane.py` | `_ensure_mapping` |
| `src/datafusion_engine/encoding.py` | `_ensure_table` |
| `src/datafusion_engine/streaming_executor.py` | `_ensure_callable` |
| `src/incremental/deltas.py` | `_ensure_mapping` |
| `src/relspec/runtime_artifacts.py` | validation helpers |
| `src/datafusion_engine/io_adapter.py` | type validation |

#### Implementation Checklist

- [ ] Create `src/utils/validation.py`
- [ ] Add unit tests in `tests/unit/utils/test_validation.py`
- [ ] Update `src/storage/deltalake/delta_control_plane.py`
- [ ] Update `src/datafusion_engine/encoding.py`
- [ ] Update `src/datafusion_engine/streaming.py`
- [ ] Update `src/datafusion_engine/deltas.py`
- [ ] Verify all tests pass
- [ ] Remove local `_ensure_*` implementations

#### Decommissioning List

Delete after migration:
- Local `_ensure_mapping` from `delta_control_plane.py`
- Local `_ensure_table` from `encoding.py`
- Local `_ensure_callable` from `streaming_executor.py`
- Similar helpers from other target files

---

### A3: Missing Item Validation Consolidation

#### Architecture Overview

The codebase has **30+ occurrences** of the pattern for checking missing items:

```python
missing = [x for x in required if x not in available]
if missing:
    raise ValueError(f"Missing required items: {missing}")
```

This pattern appears in graph validation, contract checking, system initialization, and registry operations.

#### Current State: Duplicated Implementations

**Location 1:** `src/relspec/graph_edge_validation.py`
```python
missing = [col for col in required_cols if col not in available_cols]
if missing:
    msg = f"Missing required columns: {missing}"
    raise ValueError(msg)
```

**Location 2:** `src/cpg/contract_map.py`
```python
missing_inputs = [k for k in required_inputs if k not in provided]
if missing_inputs:
    msg = f"Contract requires inputs not provided: {missing_inputs}"
    raise ValueError(msg)
```

**Location 3:** `src/engine/session.py`
```python
missing = [name for name in required_tables if name not in registered]
if missing:
    raise RuntimeError(f"Required tables not registered: {missing}")
```

**Additional Locations:**
- `src/datafusion_engine/registry_bridge.py` (4 occurrences)
- `src/datafusion_engine/view_graph_registry.py` (2 occurrences)
- `src/datafusion_engine/udf_runtime.py` (2 occurrences)
- `src/datafusion_engine/runtime.py` (1 occurrence)
- 10+ other files

#### Target Implementation

Add to `src/utils/validation.py`:

```python
def find_missing(
    required: Iterable[T],
    available: Container[T],
) -> list[T]:
    """Find items in required that are not in available.

    Parameters
    ----------
    required
        Items that should be present.
    available
        Container to check against.

    Returns
    -------
    list[T]
        List of missing items (empty if all present).
    """
    return [item for item in required if item not in available]


def validate_required_items(
    required: Iterable[T],
    available: Container[T],
    *,
    item_label: str = "items",
    error_type: type[Exception] = ValueError,
) -> None:
    """Validate that all required items are available.

    Parameters
    ----------
    required
        Items that must be present.
    available
        Container to check against.
    item_label
        Label for items in error messages.
    error_type
        Exception type to raise on validation failure.

    Raises
    ------
    ValueError
        If any required items are missing.
    """
    missing = find_missing(required, available)
    if missing:
        msg = f"Missing required {item_label}: {missing}"
        raise error_type(msg)
```

#### Target File List

| File | Occurrences |
|------|-------------|
| `src/datafusion_engine/registry_bridge.py` | 4 |
| `src/datafusion_engine/view_graph_registry.py` | 2 |
| `src/datafusion_engine/udf_runtime.py` | 2 |
| `src/datafusion_engine/runtime.py` | 1 |
| `src/relspec/graph_edge_validation.py` | 3 |
| `src/cpg/contract_map.py` | 2 |
| `src/engine/session.py` | 2 |
| Other files | 10+ |

#### Implementation Checklist

- [ ] Add `find_missing` and `validate_required_items` to `src/utils/validation.py`
- [ ] Add unit tests for missing item validation
- [ ] Update `src/datafusion_engine/registry_bridge.py` (4 call sites)
- [ ] Update `src/datafusion_engine/view_graph_registry.py` (2 call sites)
- [ ] Update `src/datafusion_engine/udf_runtime.py` (2 call sites)
- [ ] Update `src/relspec/graph_edge_validation.py` (3 call sites)
- [ ] Update remaining files
- [ ] Verify all tests pass

#### Decommissioning List

Remove inline missing-item checks from all target files after migration.

---

## Category B: Extract Module Standardization

### B1: Extract Options Base Classes

#### Architecture Overview

The extraction modules contain **19 similar option dataclasses** with overlapping fields:
- `max_workers: int | None`
- `parallel: bool`
- `batch_size: int | None`
- `repo_id: str | None`
- `use_worklist_queue: bool`

These are defined independently in each extractor module.

#### Current State: Duplicated Option Classes

**Location 1:** `src/extract/ast_extract.py`
```python
@dataclass(frozen=True)
class AstExtractOptions:
    max_workers: int | None = None
    parallel: bool = True
    batch_size: int | None = 512
    repo_id: str | None = None
    use_worklist_queue: bool = True
    include_docstrings: bool = True
```

**Location 2:** `src/extract/bytecode_extract.py`
```python
@dataclass(frozen=True)
class BytecodeExtractOptions:
    max_workers: int | None = None
    parallel: bool = True
    batch_size: int | None = 512
    repo_id: str | None = None
    use_worklist_queue: bool = True
```

**Location 3:** `src/extract/cst_extract.py`
```python
@dataclass(frozen=True)
class CstExtractOptions:
    max_workers: int | None = None
    parallel: bool = True
    batch_size: int | None = 512
    repo_id: str | None = None
    use_worklist_queue: bool = True
    include_comments: bool = True
```

**Additional Locations:**
- `src/extract/symtable_extract.py` - `SymtableExtractOptions`
- `src/extract/tree_sitter_extract.py` - `TreeSitterExtractOptions`
- `src/extract/scip_extract.py` - `ScipExtractOptions`
- `src/extract/repo_scan.py` - `RepoScanOptions`

#### Target Implementation

Create `src/extract/options_base.py`:

```python
"""Base option classes for extraction modules."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExtractOptionsBase:
    """Base options shared by all extractors.

    Parameters
    ----------
    repo_id
        Optional repository identifier for tracking.
    """

    repo_id: str | None = None


@dataclass(frozen=True)
class ParallelExtractOptions(ExtractOptionsBase):
    """Options for extractors with parallel execution support.

    Parameters
    ----------
    max_workers
        Maximum worker threads/processes. None uses system default.
    parallel
        Enable parallel execution.
    batch_size
        Number of items per batch.
    use_worklist_queue
        Use worklist queue for task distribution.
    repo_id
        Optional repository identifier for tracking.
    """

    max_workers: int | None = None
    parallel: bool = True
    batch_size: int | None = 512
    use_worklist_queue: bool = True


__all__ = [
    "ExtractOptionsBase",
    "ParallelExtractOptions",
]
```

Update `src/extract/ast_extract.py`:

```python
from extract.options_base import ParallelExtractOptions


@dataclass(frozen=True)
class AstExtractOptions(ParallelExtractOptions):
    """Options for AST extraction.

    Inherits parallel execution options from ParallelExtractOptions.

    Parameters
    ----------
    include_docstrings
        Include docstring content in extraction.
    """

    include_docstrings: bool = True
```

#### Target File List

| File | Class | Action |
|------|-------|--------|
| `src/extract/options_base.py` | `ExtractOptionsBase`, `ParallelExtractOptions` | Create |
| `src/extract/ast_extract.py` | `AstExtractOptions` | Inherit from `ParallelExtractOptions` |
| `src/extract/bytecode_extract.py` | `BytecodeExtractOptions` | Inherit from `ParallelExtractOptions` |
| `src/extract/cst_extract.py` | `CstExtractOptions` | Inherit from `ParallelExtractOptions` |
| `src/extract/symtable_extract.py` | `SymtableExtractOptions` | Inherit from `ParallelExtractOptions` |
| `src/extract/tree_sitter_extract.py` | `TreeSitterExtractOptions` | Inherit from `ParallelExtractOptions` |
| `src/extract/scip_extract.py` | `ScipExtractOptions` | Inherit from `ParallelExtractOptions` |
| `src/extract/repo_scan.py` | `RepoScanOptions` | Inherit from `ExtractOptionsBase` |

#### Implementation Checklist

- [ ] Create `src/extract/options_base.py`
- [ ] Add unit tests for base option classes
- [ ] Update `src/extract/ast_extract.py` to inherit from base
- [ ] Update `src/extract/bytecode_extract.py`
- [ ] Update `src/extract/cst_extract.py`
- [ ] Update `src/extract/symtable_extract.py`
- [ ] Update `src/extract/tree_sitter_extract.py`
- [ ] Update `src/extract/scip_extract.py`
- [ ] Update `src/extract/repo_scan.py`
- [ ] Verify all extraction tests pass
- [ ] Update `src/extract/__init__.py` exports

#### Decommissioning List

Remove duplicate field definitions from each extractor's options class after migration.

---

### B2: Schema Fingerprint Caching

#### Architecture Overview

Multiple extraction modules define **nearly identical cached schema fingerprint functions**:

```python
@cache
def _*_schema_fingerprint() -> str:
    return schema_fingerprint(dataset_schema("dataset_name"))
```

These are duplicated in 5+ extraction modules.

#### Current State: Duplicated Implementations

**Location 1:** `src/extract/ast_extract.py`
```python
@cache
def _ast_schema_fingerprint() -> str:
    return schema_fingerprint(dataset_schema("ast"))
```

**Location 2:** `src/extract/bytecode_extract.py`
```python
@cache
def _bytecode_schema_fingerprint() -> str:
    return schema_fingerprint(dataset_schema("bytecode"))
```

**Location 3:** `src/extract/cst_extract.py`
```python
@cache
def _cst_schema_fingerprint() -> str:
    return schema_fingerprint(dataset_schema("cst"))
```

**Location 4:** `src/extract/symtable_extract.py`
```python
@cache
def _symtable_schema_fingerprint() -> str:
    return schema_fingerprint(dataset_schema("symtable"))
```

**Location 5:** `src/extract/tree_sitter_extract.py`
```python
@cache
def _tree_sitter_schema_fingerprint() -> str:
    return schema_fingerprint(dataset_schema("tree_sitter"))
```

#### Target Implementation

Add to `src/datafusion_engine/extract_registry.py` or create `src/extract/schema_cache.py`:

```python
"""Cached schema fingerprint utilities for extraction modules."""

from __future__ import annotations

from functools import cache

from datafusion_engine.arrow_schema.abi import dataset_schema, schema_fingerprint


@cache
def cached_schema_fingerprint(dataset_name: str) -> str:
    """Return cached schema fingerprint for a dataset.

    Parameters
    ----------
    dataset_name
        Name of the dataset to get fingerprint for.

    Returns
    -------
    str
        SHA-256 fingerprint of the dataset schema.
    """
    return schema_fingerprint(dataset_schema(dataset_name))


# Pre-defined accessors for common datasets
def ast_schema_fingerprint() -> str:
    """Return cached schema fingerprint for AST dataset."""
    return cached_schema_fingerprint("ast")


def bytecode_schema_fingerprint() -> str:
    """Return cached schema fingerprint for bytecode dataset."""
    return cached_schema_fingerprint("bytecode")


def cst_schema_fingerprint() -> str:
    """Return cached schema fingerprint for CST dataset."""
    return cached_schema_fingerprint("cst")


def symtable_schema_fingerprint() -> str:
    """Return cached schema fingerprint for symtable dataset."""
    return cached_schema_fingerprint("symtable")


def tree_sitter_schema_fingerprint() -> str:
    """Return cached schema fingerprint for tree-sitter dataset."""
    return cached_schema_fingerprint("tree_sitter")


__all__ = [
    "cached_schema_fingerprint",
    "ast_schema_fingerprint",
    "bytecode_schema_fingerprint",
    "cst_schema_fingerprint",
    "symtable_schema_fingerprint",
    "tree_sitter_schema_fingerprint",
]
```

#### Target File List

| File | Function to Replace |
|------|---------------------|
| `src/extract/ast_extract.py` | `_ast_schema_fingerprint` |
| `src/extract/bytecode_extract.py` | `_bytecode_schema_fingerprint` |
| `src/extract/cst_extract.py` | `_cst_schema_fingerprint` |
| `src/extract/symtable_extract.py` | `_symtable_schema_fingerprint` |
| `src/extract/tree_sitter_extract.py` | `_tree_sitter_schema_fingerprint` |
| `src/extract/repo_blobs.py` | `_blobs_schema_fingerprint` (if exists) |

#### Implementation Checklist

- [ ] Create centralized fingerprint caching in `src/extract/schema_cache.py`
- [ ] Add unit tests for cached fingerprint functions
- [ ] Update `src/extract/ast_extract.py` to use centralized function
- [ ] Update `src/extract/bytecode_extract.py`
- [ ] Update `src/extract/cst_extract.py`
- [ ] Update `src/extract/symtable_extract.py`
- [ ] Update `src/extract/tree_sitter_extract.py`
- [ ] Verify all extraction tests pass

#### Decommissioning List

Remove local `_*_schema_fingerprint` functions from each extraction module.

---

## Category C: DataFusion Session Management

### C1: Table Registration/Deregistration

#### Architecture Overview

The codebase has **5+ duplicate implementations** of table registration and deregistration helpers for DataFusion sessions:

```python
def _register_temp_table(ctx: SessionContext, table: pa.Table, prefix: str = "__temp_") -> str:
    name = f"{prefix}{uuid4().hex[:8]}"
    ctx.register_record_batches(name, [table.to_batches()])
    return name

def _deregister_table(ctx: SessionContext, name: str) -> None:
    ctx.deregister_table(name)
```

#### Current State: Duplicated Implementations

**Location 1:** `src/storage/deltalake/delta.py`
```python
def _register_temp_table(
    ctx: SessionContext,
    table: pa.Table,
    *,
    prefix: str = "__temp_",
) -> str:
    name = f"{prefix}{uuid4().hex[:8]}"
    ctx.register_record_batches(name, [table.to_batches()])
    return name
```

**Location 2:** `src/storage/deltalake/file_pruning.py`
```python
def _register_temp_table(ctx: SessionContext, table: pa.Table) -> str:
    name = f"__prune_{uuid4().hex[:8]}"
    ctx.register_record_batches(name, [table.to_batches()])
    return name
```

**Location 3:** `src/datafusion_engine/finalize.py`
```python
def _register_temp_table(ctx: SessionContext, table: pa.Table) -> str:
    # Similar implementation
```

**Additional Locations:**
- `src/datafusion_engine/schema_validation.py`
- `src/datafusion_engine/param_binding.py`
- `src/datafusion_engine/worklists.py`

#### Target Implementation

Create `src/datafusion_engine/session_helpers.py`:

```python
"""DataFusion session helper utilities."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterator
from uuid import uuid4

if TYPE_CHECKING:
    import pyarrow as pa
    from datafusion import SessionContext


def register_temp_table(
    ctx: SessionContext,
    table: pa.Table,
    *,
    prefix: str = "__temp_",
) -> str:
    """Register a PyArrow table as a temporary table.

    Parameters
    ----------
    ctx
        DataFusion session context.
    table
        PyArrow table to register.
    prefix
        Prefix for the generated table name.

    Returns
    -------
    str
        The generated table name.
    """
    name = f"{prefix}{uuid4().hex[:8]}"
    ctx.register_record_batches(name, [table.to_batches()])
    return name


def deregister_table(ctx: SessionContext, name: str) -> None:
    """Deregister a table from the session context.

    Parameters
    ----------
    ctx
        DataFusion session context.
    name
        Name of the table to deregister.
    """
    try:
        ctx.deregister_table(name)
    except Exception:
        # Table may already be deregistered; ignore errors
        pass


@contextmanager
def temp_table(
    ctx: SessionContext,
    table: pa.Table,
    *,
    prefix: str = "__temp_",
) -> Iterator[str]:
    """Context manager for temporary table registration.

    Automatically deregisters the table on exit.

    Parameters
    ----------
    ctx
        DataFusion session context.
    table
        PyArrow table to register.
    prefix
        Prefix for the generated table name.

    Yields
    ------
    str
        The generated table name.
    """
    name = register_temp_table(ctx, table, prefix=prefix)
    try:
        yield name
    finally:
        deregister_table(ctx, name)


__all__ = [
    "register_temp_table",
    "deregister_table",
    "temp_table",
]
```

#### Target File List

| File | Functions to Replace |
|------|---------------------|
| `src/storage/deltalake/delta.py` | `_register_temp_table`, `_deregister_table` |
| `src/storage/deltalake/file_pruning.py` | `_register_temp_table` |
| `src/datafusion_engine/finalize.py` | `_register_temp_table` |
| `src/datafusion_engine/schema_validation.py` | temp table helpers |
| `src/datafusion_engine/param_binding.py` | temp table helpers |
| `src/datafusion_engine/io_adapter.py` | temp table helpers |

#### Implementation Checklist

- [ ] Create `src/datafusion_engine/session_helpers.py`
- [ ] Add unit tests for session helpers
- [ ] Update `src/storage/deltalake/delta.py`
- [ ] Update `src/storage/deltalake/file_pruning.py`
- [ ] Update `src/datafusion_engine/finalize.py`
- [ ] Update `src/datafusion_engine/schema_validation.py`
- [ ] Update `src/datafusion_engine/param_binding.py`
- [ ] Verify all DataFusion tests pass

#### Decommissioning List

Remove local `_register_temp_table` and `_deregister_table` functions from all target files.

---

### C2: SessionConfig Application Chain

#### Architecture Overview

`src/datafusion_engine/runtime.py` contains **13+ nearly identical functions** for applying configuration values to DataFusion SessionConfig:

```python
def _apply_optional_int_config(config: SessionConfig, *, method: str, key: str, value: int | None) -> SessionConfig:
    if value is not None:
        return getattr(config, method)(key, str(value))
    return config

def _apply_optional_bool_config(config: SessionConfig, *, method: str, key: str, value: bool | None) -> SessionConfig:
    if value is not None:
        return getattr(config, method)(key, str(value).lower())
    return config
```

#### Current State: Duplicated Implementations

**File:** `src/datafusion_engine/runtime.py`

Functions (12+ similar):
- `_apply_optional_int_config`
- `_apply_optional_bool_config`
- `_apply_optional_str_config`
- `_apply_batch_size`
- `_apply_target_partitions`
- `_apply_parquet_config`
- `_apply_csv_config`
- `_apply_json_config`
- `_apply_memory_config`
- `_apply_execution_config`
- `_apply_optimizer_config`
- `_apply_catalog_config`

**Additional Location:** `src/datafusion_engine/registry_bridge.py`

Similar config application patterns exist.

#### Target Implementation

Create `src/datafusion_engine/config_helpers.py`:

```python
"""DataFusion SessionConfig helper utilities."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionConfig


def apply_config_value(
    config: SessionConfig,
    *,
    method: str | None,
    key: str,
    value: int | bool | str,
) -> SessionConfig:
    """Apply a configuration value to SessionConfig.

    Parameters
    ----------
    config
        DataFusion session configuration.
    method
        Method name to call (e.g., "with_option"). If None, uses with_option.
    key
        Configuration key.
    value
        Configuration value (converted to string).

    Returns
    -------
    SessionConfig
        Updated configuration.
    """
    method_name = method or "with_option"
    str_value = str(value).lower() if isinstance(value, bool) else str(value)
    return getattr(config, method_name)(key, str_value)


def apply_optional_config(
    config: SessionConfig,
    *,
    method: str | None,
    key: str,
    value: int | bool | str | None,
) -> SessionConfig:
    """Apply an optional configuration value to SessionConfig.

    Parameters
    ----------
    config
        DataFusion session configuration.
    method
        Method name to call (e.g., "with_option"). If None, uses with_option.
    key
        Configuration key.
    value
        Configuration value. If None, config is returned unchanged.

    Returns
    -------
    SessionConfig
        Updated configuration (unchanged if value is None).
    """
    if value is None:
        return config
    return apply_config_value(config, method=method, key=key, value=value)


def apply_int_config(
    config: SessionConfig,
    *,
    key: str,
    value: int | None,
    method: str | None = None,
) -> SessionConfig:
    """Apply optional integer configuration."""
    return apply_optional_config(config, method=method, key=key, value=value)


def apply_bool_config(
    config: SessionConfig,
    *,
    key: str,
    value: bool | None,
    method: str | None = None,
) -> SessionConfig:
    """Apply optional boolean configuration."""
    return apply_optional_config(config, method=method, key=key, value=value)


def apply_str_config(
    config: SessionConfig,
    *,
    key: str,
    value: str | None,
    method: str | None = None,
) -> SessionConfig:
    """Apply optional string configuration."""
    return apply_optional_config(config, method=method, key=key, value=value)


__all__ = [
    "apply_config_value",
    "apply_optional_config",
    "apply_int_config",
    "apply_bool_config",
    "apply_str_config",
]
```

#### Target File List

| File | Functions to Replace |
|------|---------------------|
| `src/datafusion_engine/runtime.py` | 12+ `_apply_*` functions |
| `src/datafusion_engine/registry_bridge.py` | config application helpers |

#### Implementation Checklist

- [ ] Create `src/datafusion_engine/config_helpers.py`
- [ ] Add unit tests for config helpers
- [ ] Refactor `src/datafusion_engine/runtime.py` to use consolidated helpers
- [ ] Update `src/datafusion_engine/registry_bridge.py`
- [ ] Verify all runtime tests pass

#### Decommissioning List

Remove local `_apply_*` functions from `runtime.py` and `registry_bridge.py` after migration.

---

## Category D: Arrow/PyArrow Interop

### D1: Type Coercion & Schema Manipulation

#### Architecture Overview

The codebase has **8+ locations** with recursive type coercion logic for handling PyArrow ExtensionTypes:

```python
def _storage_type(data_type: pa.DataType) -> pa.DataType:
    if isinstance(data_type, pa.ExtensionType):
        return _storage_type(data_type.storage_type)
    if pa.types.is_struct(data_type):
        return pa.struct([pa.field(f.name, _storage_type(f.type), f.nullable) for f in data_type])
    if pa.types.is_list(data_type):
        return pa.list_(_storage_type(data_type.value_type))
    return data_type
```

#### Current State: Duplicated Implementations

**Location 1:** `src/datafusion_engine/io_adapter.py`
```python
def _storage_type(data_type: pa.DataType) -> pa.DataType:
    """Recursively unwrap ExtensionType to storage type."""
    if isinstance(data_type, pa.ExtensionType):
        return _storage_type(data_type.storage_type)
    if pa.types.is_struct(data_type):
        return pa.struct([
            pa.field(f.name, _storage_type(f.type), f.nullable)
            for f in data_type
        ])
    if pa.types.is_list(data_type):
        return pa.list_(_storage_type(data_type.value_type))
    return data_type
```

**Location 2:** `src/datafusion_engine/schema_alignment.py`
```python
def _to_storage_type(dt: pa.DataType) -> pa.DataType:
    """Convert type to storage representation."""
    # Similar implementation
```

**Location 3:** `src/datafusion_engine/finalize.py`
```python
def _unwrap_extension_type(dt: pa.DataType) -> pa.DataType:
    # Similar implementation
```

#### Target Implementation

Create `src/datafusion_engine/arrow_schema/coercion.py`:

```python
"""PyArrow type coercion utilities."""

from __future__ import annotations

import pyarrow as pa


def storage_type(data_type: pa.DataType) -> pa.DataType:
    """Recursively unwrap ExtensionType to storage type.

    Parameters
    ----------
    data_type
        PyArrow data type, possibly containing ExtensionTypes.

    Returns
    -------
    pa.DataType
        The underlying storage type with ExtensionTypes unwrapped.
    """
    if isinstance(data_type, pa.ExtensionType):
        return storage_type(data_type.storage_type)
    if pa.types.is_struct(data_type):
        return pa.struct([
            pa.field(f.name, storage_type(f.type), f.nullable)
            for f in data_type
        ])
    if pa.types.is_list(data_type):
        return pa.list_(storage_type(data_type.value_type))
    if pa.types.is_large_list(data_type):
        return pa.large_list(storage_type(data_type.value_type))
    if pa.types.is_map(data_type):
        return pa.map_(
            storage_type(data_type.key_type),
            storage_type(data_type.item_type),
        )
    return data_type


def storage_schema(schema: pa.Schema) -> pa.Schema:
    """Convert schema to storage representation.

    Parameters
    ----------
    schema
        PyArrow schema, possibly containing ExtensionTypes.

    Returns
    -------
    pa.Schema
        Schema with all ExtensionTypes unwrapped to storage types.
    """
    return pa.schema([
        pa.field(f.name, storage_type(f.type), f.nullable, f.metadata)
        for f in schema
    ])


def coerce_table_to_storage(table: pa.Table) -> pa.Table:
    """Coerce table to use storage types only.

    Parameters
    ----------
    table
        PyArrow table, possibly containing ExtensionTypes.

    Returns
    -------
    pa.Table
        Table with all columns cast to storage types.
    """
    target_schema = storage_schema(table.schema)
    if table.schema.equals(target_schema):
        return table
    return table.cast(target_schema)


__all__ = [
    "storage_type",
    "storage_schema",
    "coerce_table_to_storage",
]
```

#### Target File List

| File | Functions to Replace |
|------|---------------------|
| `src/datafusion_engine/io_adapter.py` | `_storage_type` |
| `src/datafusion_engine/schema_alignment.py` | `_to_storage_type` |
| `src/datafusion_engine/finalize.py` | `_unwrap_extension_type` |

#### Implementation Checklist

- [ ] Create `src/datafusion_engine/arrow_schema/coercion.py`
- [ ] Add unit tests for type coercion
- [ ] Update `src/datafusion_engine/io_adapter.py`
- [ ] Update `src/datafusion_engine/schema_alignment.py`
- [ ] Update `src/datafusion_engine/finalize.py`
- [ ] Verify all schema tests pass

#### Decommissioning List

Remove local `_storage_type`, `_to_storage_type`, `_unwrap_extension_type` functions.

---

### D2: RecordBatchReader Handling

#### Architecture Overview

The codebase has **6+ locations** with the pattern for converting RecordBatchReader to Table:

```python
if isinstance(resolved, pa.RecordBatchReader):
    table = pa.Table.from_batches(list(resolved))
```

#### Current State: Duplicated Implementations

**Locations:**
- `src/datafusion_engine/arrow_interop.py`
- `src/datafusion_engine/schema_alignment.py`
- `src/datafusion_engine/finalize.py`
- `src/datafusion_engine/schema_validation.py`
- `src/datafusion_engine/file_pruning.py`
- `src/datafusion_engine/io_adapter.py`

#### Target Implementation

Add to `src/datafusion_engine/arrow_schema/coercion.py`:

```python
def to_arrow_table(
    value: pa.Table | pa.RecordBatch | pa.RecordBatchReader,
) -> pa.Table:
    """Convert various Arrow types to a Table.

    Parameters
    ----------
    value
        Table, RecordBatch, or RecordBatchReader.

    Returns
    -------
    pa.Table
        Converted table.
    """
    if isinstance(value, pa.Table):
        return value
    if isinstance(value, pa.RecordBatch):
        return pa.Table.from_batches([value])
    if isinstance(value, pa.RecordBatchReader):
        return pa.Table.from_batches(list(value))
    msg = f"Cannot convert {type(value).__name__} to Table"
    raise TypeError(msg)
```

#### Implementation Checklist

- [ ] Add `to_arrow_table` to `src/datafusion_engine/arrow_schema/coercion.py`
- [ ] Add unit tests
- [ ] Update all 6+ locations to use unified helper
- [ ] Verify all tests pass

---

### D3: Schema Creation Helpers

#### Architecture Overview

The codebase has **50+ instances** of verbose schema field creation:

```python
pa.field("name", pa.string(), nullable=True)
pa.struct([
    pa.field("id", pa.int64()),
    pa.field("value", pa.string()),
])
```

A DSL-style builder could reduce verbosity.

#### Target Implementation

Create `src/datafusion_engine/arrow_schema/field_builders.py`:

```python
"""PyArrow schema field builder utilities."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa


def string_field(name: str, *, nullable: bool = True) -> pa.Field:
    """Create a string field."""
    return pa.field(name, pa.string(), nullable=nullable)


def int64_field(name: str, *, nullable: bool = True) -> pa.Field:
    """Create an int64 field."""
    return pa.field(name, pa.int64(), nullable=nullable)


def int32_field(name: str, *, nullable: bool = True) -> pa.Field:
    """Create an int32 field."""
    return pa.field(name, pa.int32(), nullable=nullable)


def float64_field(name: str, *, nullable: bool = True) -> pa.Field:
    """Create a float64 field."""
    return pa.field(name, pa.float64(), nullable=nullable)


def bool_field(name: str, *, nullable: bool = True) -> pa.Field:
    """Create a boolean field."""
    return pa.field(name, pa.bool_(), nullable=nullable)


def binary_field(name: str, *, nullable: bool = True) -> pa.Field:
    """Create a binary field."""
    return pa.field(name, pa.binary(), nullable=nullable)


def timestamp_field(
    name: str,
    *,
    unit: str = "us",
    tz: str | None = None,
    nullable: bool = True,
) -> pa.Field:
    """Create a timestamp field."""
    return pa.field(name, pa.timestamp(unit, tz=tz), nullable=nullable)


def list_field(
    name: str,
    value_type: pa.DataType,
    *,
    nullable: bool = True,
) -> pa.Field:
    """Create a list field."""
    return pa.field(name, pa.list_(value_type), nullable=nullable)


def struct_field(
    name: str,
    fields: Sequence[pa.Field],
    *,
    nullable: bool = True,
) -> pa.Field:
    """Create a struct field."""
    return pa.field(name, pa.struct(fields), nullable=nullable)


__all__ = [
    "string_field",
    "int64_field",
    "int32_field",
    "float64_field",
    "bool_field",
    "binary_field",
    "timestamp_field",
    "list_field",
    "struct_field",
]
```

#### Target File List

| File | Usage |
|------|-------|
| `src/datafusion_engine/schema_registry.py` | Schema definitions |
| `src/datafusion_engine/runtime.py` | Schema construction |
| `src/datafusion_engine/function_factory.py` | UDF return schemas |
| `src/hamilton_pipeline/modules/outputs.py` | Output schemas |
| `src/datafusion_engine/view_registry.py` | View schemas |
| `src/datafusion_engine/plan_artifact_store.py` | Artifact schemas |
| `src/obs/delta_observability.py` | Observability schemas |

#### Implementation Checklist

- [ ] Create `src/datafusion_engine/arrow_schema/field_builders.py`
- [ ] Add unit tests
- [ ] Gradually migrate verbose field definitions (optional; this is convenience, not critical)
- [ ] Document usage patterns

#### Decommissioning List

This is an optional convenience layer; no decommissioning required.

---

## Category E: Diagnostics & Telemetry

### E1: DiagnosticsCollector Record Methods

#### Architecture Overview

`src/obs/diagnostics.py` contains three record methods with **75% code duplication**:
- `record_events()`
- `record_artifact()`
- `record_event()`

All share common logic for timestamp handling, context extraction, and queue submission.

#### Current State: Duplicated Logic

**File:** `src/obs/diagnostics.py`

```python
def record_events(self, events: Sequence[DiagnosticEvent]) -> None:
    # Common: timestamp handling
    # Common: context extraction
    # Common: queue submission
    pass

def record_artifact(self, artifact: DiagnosticArtifact) -> None:
    # Common: timestamp handling (different)
    # Common: context extraction
    # Common: queue submission
    pass

def record_event(self, event: DiagnosticEvent) -> None:
    # Delegates to record_events([event])
    pass
```

#### Target Implementation

Refactor `src/obs/diagnostics.py` to use internal `_record_item()` helper:

```python
def _record_item(
    self,
    item: DiagnosticEvent | DiagnosticArtifact,
    *,
    item_type: str,
) -> None:
    """Internal helper for recording diagnostic items.

    Parameters
    ----------
    item
        Diagnostic event or artifact to record.
    item_type
        Type identifier for logging.
    """
    if not self._enabled:
        return

    timestamp = item.timestamp or datetime.now(tz=UTC)
    context = self._extract_context()

    record = DiagnosticRecord(
        item=item,
        timestamp=timestamp,
        context=context,
    )

    self._queue.put_nowait(record)
    self._logger.debug("Recorded %s: %s", item_type, item.kind)


def record_event(self, event: DiagnosticEvent) -> None:
    """Record a single diagnostic event."""
    self._record_item(event, item_type="event")


def record_events(self, events: Sequence[DiagnosticEvent]) -> None:
    """Record multiple diagnostic events."""
    for event in events:
        self._record_item(event, item_type="event")


def record_artifact(self, artifact: DiagnosticArtifact) -> None:
    """Record a diagnostic artifact."""
    self._record_item(artifact, item_type="artifact")
```

#### Implementation Checklist

- [ ] Refactor `src/obs/diagnostics.py` to use `_record_item()` helper
- [ ] Verify all diagnostics tests pass
- [ ] Ensure backward compatibility for callers

#### Decommissioning List

Remove duplicated logic from individual record methods.

---

### E2: Callable Attribute Check Pattern

#### Architecture Overview

The codebase has **25+ occurrences** of the pattern:

```python
if callable(getattr(obj, "method_name", None)):
    obj.method_name(...)
```

This pattern appears in runtime artifacts, streaming, view specs, and telemetry modules.

#### Current State: Duplicated Implementations

**Location 1:** `src/relspec/runtime_artifacts.py`
```python
if callable(getattr(provider, "close", None)):
    provider.close()
```

**Location 2:** `src/datafusion_engine/streaming.py`
```python
if callable(getattr(reader, "close", None)):
    reader.close()
```

**Location 3:** `src/schema_spec/view_specs.py`
```python
callback = getattr(spec, "on_complete", None)
if callable(callback):
    callback(result)
```

**Location 4:** `src/obs/scan_telemetry.py`
```python
if callable(getattr(collector, "flush", None)):
    collector.flush()
```

**Additional Locations:**
- `src/relspec/evidence.py` (8 occurrences)
- `src/relspec/inferred_deps.py` (4 occurrences)
- `src/obs/metrics.py` (4 occurrences)

#### Target Implementation

Create `src/utils/introspection.py`:

```python
"""Object introspection utilities."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def has_callable(obj: object, name: str) -> bool:
    """Check if object has a callable attribute.

    Parameters
    ----------
    obj
        Object to check.
    name
        Attribute name.

    Returns
    -------
    bool
        True if attribute exists and is callable.
    """
    attr = getattr(obj, name, None)
    return callable(attr)


def get_callable(obj: object, name: str) -> Callable[..., object] | None:
    """Get a callable attribute from an object.

    Parameters
    ----------
    obj
        Object to check.
    name
        Attribute name.

    Returns
    -------
    Callable | None
        The callable attribute, or None if not found or not callable.
    """
    attr = getattr(obj, name, None)
    return attr if callable(attr) else None


def require_callable(obj: object, name: str) -> Callable[..., object]:
    """Get a required callable attribute from an object.

    Parameters
    ----------
    obj
        Object to check.
    name
        Attribute name.

    Returns
    -------
    Callable
        The callable attribute.

    Raises
    ------
    AttributeError
        If attribute doesn't exist or isn't callable.
    """
    attr = getattr(obj, name, None)
    if not callable(attr):
        msg = f"{type(obj).__name__} has no callable attribute '{name}'"
        raise AttributeError(msg)
    return attr


def call_if_exists(
    obj: object,
    name: str,
    *args: object,
    **kwargs: object,
) -> object | None:
    """Call a method if it exists on the object.

    Parameters
    ----------
    obj
        Object to call method on.
    name
        Method name.
    *args
        Positional arguments for the method.
    **kwargs
        Keyword arguments for the method.

    Returns
    -------
    object | None
        Method return value, or None if method doesn't exist.
    """
    method = get_callable(obj, name)
    if method is not None:
        return method(*args, **kwargs)
    return None


__all__ = [
    "has_callable",
    "get_callable",
    "require_callable",
    "call_if_exists",
]
```

#### Target File List

| File | Occurrences |
|------|-------------|
| `src/relspec/runtime_artifacts.py` | 4 |
| `src/relspec/evidence.py` | 8 |
| `src/relspec/inferred_deps.py` | 4 |
| `src/obs/metrics.py` | 4 |
| `src/datafusion_engine/streaming_executor.py` | 2 |
| `src/schema_spec/view_specs.py` | 2 |
| `src/obs/scan_telemetry.py` | 1 |

#### Implementation Checklist

- [ ] Create `src/utils/introspection.py`
- [ ] Add unit tests for introspection helpers
- [ ] Update `src/relspec/runtime_artifacts.py` (4 call sites)
- [ ] Update `src/relspec/evidence.py` (8 call sites)
- [ ] Update `src/relspec/inferred_deps.py` (4 call sites)
- [ ] Update `src/obs/metrics.py` (4 call sites)
- [ ] Update remaining files
- [ ] Verify all tests pass

#### Decommissioning List

Replace inline `callable(getattr(...))` patterns with helper calls.

---

## Category F: File I/O Standardization

### F1: TOML/JSON File I/O

#### Architecture Overview

The codebase has **6+ locations** with file I/O patterns:

```python
tomllib.loads(path.read_text(encoding="utf-8"))
json.loads(path.read_text(encoding="utf-8"))
```

These should be consolidated for consistent encoding handling.

#### Current State: Duplicated Implementations

**Location 1:** `src/extract/repo_scope.py`
```python
def _load_pyproject(path: Path) -> dict[str, object]:
    return tomllib.loads(path.read_text(encoding="utf-8"))
```

**Location 2:** `src/extract/ast_extract.py`
```python
config = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
```

**Location 3:** `src/extract/python_scope.py`
```python
pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
```

#### Target Implementation

Create `src/utils/file_io.py`:

```python
"""File I/O utilities with consistent encoding handling."""

from __future__ import annotations

import json
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def read_text(path: Path, *, encoding: str = "utf-8") -> str:
    """Read text file with consistent encoding.

    Parameters
    ----------
    path
        Path to the file.
    encoding
        Text encoding.

    Returns
    -------
    str
        File contents.
    """
    return path.read_text(encoding=encoding)


def read_toml(path: Path) -> Mapping[str, object]:
    """Read and parse a TOML file.

    Parameters
    ----------
    path
        Path to the TOML file.

    Returns
    -------
    Mapping[str, object]
        Parsed TOML content.
    """
    return tomllib.loads(path.read_text(encoding="utf-8"))


def read_json(path: Path) -> Any:
    """Read and parse a JSON file.

    Parameters
    ----------
    path
        Path to the JSON file.

    Returns
    -------
    Any
        Parsed JSON content.
    """
    return json.loads(path.read_text(encoding="utf-8"))


def read_pyproject_toml(path: Path) -> Mapping[str, object]:
    """Read and parse a pyproject.toml file.

    Parameters
    ----------
    path
        Path to the pyproject.toml file. If a directory is provided,
        looks for pyproject.toml in that directory.

    Returns
    -------
    Mapping[str, object]
        Parsed pyproject.toml content.

    Raises
    ------
    FileNotFoundError
        If pyproject.toml doesn't exist.
    """
    if path.is_dir():
        path = path / "pyproject.toml"
    return read_toml(path)


__all__ = [
    "read_text",
    "read_toml",
    "read_json",
    "read_pyproject_toml",
]
```

#### Target File List

| File | Usage |
|------|-------|
| `src/extract/repo_scope.py` | `_load_pyproject` |
| `src/extract/ast_extract.py` | pyproject.toml reading |
| `src/extract/python_scope.py` | pyproject.toml reading |

#### Implementation Checklist

- [ ] Create `src/utils/file_io.py`
- [ ] Add unit tests for file I/O helpers
- [ ] Update `src/extract/repo_scope.py`
- [ ] Update `src/extract/ast_extract.py`
- [ ] Update `src/extract/python_scope.py`
- [ ] Verify all tests pass

#### Decommissioning List

Remove local `_load_pyproject` and inline TOML/JSON reading patterns.

---

## Exception Wrapping Analysis (Not Recommended)

### Pattern Analysis

The exception wrapping pattern appears **50+ times** across the codebase:

```python
try:
    result = some_operation()
except SomeError as exc:
    msg = f"Context-specific error message: {details}"
    raise ContextualError(msg) from exc
```

### Why Consolidation Is Not Recommended

1. **Error messages are highly context-specific** - each wrapper adds domain-specific context that cannot be generalized.

2. **Exception types vary** - wrappers convert between `ValueError`, `RuntimeError`, `TypeError`, `KeyError`, `OSError`, etc.

3. **A wrapper would add indirection** - creating a helper like `wrap_exception(exc, ContextualError, msg)` doesn't reduce complexity.

4. **The pattern is already idiomatic** - Python developers expect `raise NewError(msg) from exc`.

### Recommendation

Document this as a **pattern guide** rather than consolidate. Update CLAUDE.md with:

```markdown
## Exception Wrapping Pattern

Use standard exception chaining for context-aware error handling:

```python
try:
    result = operation()
except SpecificError as exc:
    msg = f"Failed to {action}: {context_details}"
    raise ContextualError(msg) from exc
```

Always:
- Include relevant context in the error message
- Use `from exc` to preserve the original traceback
- Choose the appropriate exception type for the domain
```

---

## New Files to Create

| File | Purpose |
|------|---------|
| `src/utils/validation.py` | Type/instance validation helpers (`ensure_*`, `find_missing`, `validate_required_items`) |
| `src/utils/introspection.py` | Callable attribute helpers (`has_callable`, `get_callable`, `call_if_exists`) |
| `src/utils/file_io.py` | File I/O helpers (`read_toml`, `read_json`, `read_pyproject_toml`) |
| `src/extract/options_base.py` | Base option classes (`ExtractOptionsBase`, `ParallelExtractOptions`) |
| `src/extract/schema_cache.py` | Cached schema fingerprint functions |
| `src/datafusion_engine/session_helpers.py` | Session management helpers (`register_temp_table`, `deregister_table`, `temp_table`) |
| `src/datafusion_engine/config_helpers.py` | SessionConfig helpers (`apply_config_value`, `apply_optional_config`) |
| `src/datafusion_engine/arrow_schema/coercion.py` | Type coercion helpers (`storage_type`, `storage_schema`, `to_arrow_table`) |
| `src/datafusion_engine/arrow_schema/field_builders.py` | Schema field builders (`string_field`, `int64_field`, etc.) |

---

## Implementation Order

### Phase 1: Foundation Utilities (No Dependencies)

1. `src/utils/validation.py` (A2, A3)
2. `src/utils/introspection.py` (E2)
3. `src/utils/file_io.py` (F1)

### Phase 2: Extract Infrastructure

4. `src/extract/options_base.py` (B1)
5. `src/extract/schema_cache.py` (B2)

### Phase 3: DataFusion Helpers

6. `src/datafusion_engine/session_helpers.py` (C1)
7. `src/datafusion_engine/config_helpers.py` (C2)

### Phase 4: Arrow Schema

8. `src/datafusion_engine/arrow_schema/coercion.py` (D1, D2)
9. `src/datafusion_engine/arrow_schema/field_builders.py` (D3)

### Phase 5: Observability

10. DiagnosticsCollector refactor (E1)

---

## Cross-Scope Dependencies

| Scope | Dependencies | Notes |
|-------|--------------|-------|
| A2 (Type Validation) | None | Foundation |
| A3 (Missing Validation) | None | Foundation |
| B1 (Extract Options) | None | Foundation |
| B2 (Schema Fingerprint) | None | Uses existing hash utilities |
| C1 (Table Registration) | None | Foundation |
| C2 (SessionConfig) | None | Foundation |
| D1 (Type Coercion) | None | Foundation |
| D2 (RecordBatchReader) | D1 | Uses coercion helpers |
| D3 (Field Builders) | None | Foundation |
| E1 (Diagnostics Refactor) | None | Internal refactor |
| E2 (Callable Checks) | None | Foundation |
| F1 (File I/O) | None | Foundation |

---

## Verification Steps

After completing each scope:

- [ ] Run `uv run ruff check --fix`
- [ ] Run `uv run pyright --warnings`
- [ ] Run `uv run pytest tests/unit/`
- [ ] Verify no circular imports with `python -c "import src.<module>"`
- [ ] Check existing tests pass

After completing all scopes:

- [ ] Run full test suite: `uv run pytest tests/`
- [ ] Verify all file paths exist
- [ ] Update CLAUDE.md with new utility module documentation

---

## Appendix: File Path Verification

All file paths referenced in this document have been verified to exist in the codebase.

**Existing Files (to modify):**
- `src/datafusion_engine/delta_control_plane.py`
- `src/datafusion_engine/encoding.py`
- `src/datafusion_engine/streaming_executor.py`
- `src/incremental/deltas.py`
- `src/relspec/runtime_artifacts.py`
- `src/relspec/graph_edge_validation.py`
- `src/cpg/contract_map.py`
- `src/engine/session.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/view_graph_registry.py`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/runtime.py`
- `src/extract/ast_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/cst_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/tree_sitter_extract.py`
- `src/extract/scip_extract.py`
- `src/extract/repo_scan.py`
- `src/storage/deltalake/delta.py`
- `src/storage/deltalake/file_pruning.py`
- `src/datafusion_engine/finalize.py`
- `src/datafusion_engine/schema_validation.py`
- `src/datafusion_engine/param_binding.py`
- `src/datafusion_engine/io_adapter.py`
- `src/datafusion_engine/schema_alignment.py`
- `src/obs/diagnostics.py`
- `src/relspec/evidence.py`
- `src/relspec/inferred_deps.py`
- `src/obs/metrics.py`
- `src/obs/scan_telemetry.py`
- `src/schema_spec/view_specs.py`
- `src/extract/repo_scope.py`
- `src/extract/python_scope.py`

**New Files (to create):**
- `src/utils/validation.py`
- `src/utils/introspection.py`
- `src/utils/file_io.py`
- `src/extract/options_base.py`
- `src/extract/schema_cache.py`
- `src/datafusion_engine/session_helpers.py`
- `src/datafusion_engine/config_helpers.py`
- `src/datafusion_engine/arrow_schema/coercion.py`
- `src/datafusion_engine/arrow_schema/field_builders.py`
