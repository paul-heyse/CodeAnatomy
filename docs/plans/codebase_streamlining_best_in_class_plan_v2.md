# Codebase Streamlining Plan v2 (Parallel Consolidation Opportunities)

## Executive Summary

This plan identifies **additional consolidation opportunities** that can be executed in parallel with the v5+v6 remaining scope. These opportunities span both Python (`src/`) and Rust (`rust/`) directories, targeting:

1. **Value coercion utilities** - 30+ scattered `_coerce_*` functions (tolerant vs strict variants)
2. **Session/context factory unification** - Delta helpers routed through DataFusionRuntimeProfile
3. **Policy fingerprinting protocol** - Reuse existing `FingerprintableConfig` from `config_base.py`
4. **Schema builder extension** - Extend existing `schema_builders.py` with field factories
5. **Telemetry emission consolidation** - 8+ `record_*` functions with shared boilerplate (preserve API)
6. **Validation violation unification** - Unified violation objects with separate report types
7. **Storage options centralization** - Use canonical `merged_storage_options()`
8. **Rust UDF registration macros** - Kind-explicit macros (`scalar_udfs!`, `table_udfs!`)
9. **Rust config extension** - Local `macro_rules!` instead of proc-macro crate
10. **Python binding registration** - Crate-local macro for `add_function()` calls
11. **Rust error conversion** - `impl_error_from!` macro consolidation

**Additional Opportunities:**
- Plan-bundle execution helper consolidation
- RecordBatchReader coercion unification
- Single UDF catalog source via CREATE FUNCTION DDL

**Estimated Impact:** ~2,500 lines of duplication reduced to ~500 lines through consolidation.

---

## Design Principles

1. **DRY (Don't Repeat Yourself)** - Extract common patterns into reusable utilities
2. **Single Source of Truth** - One canonical implementation per concern
3. **Composition over Inheritance** - Favor protocol/trait composition for flexibility
4. **Consistent Naming** - Unified naming conventions across modules
5. **DataFusion-Native** - Leverage DataFusion's built-in configuration, caching, and DDL systems
6. **Reuse Existing Abstractions** - Extend existing code (`config_base.py`, `schema_builders.py`) rather than creating new modules

---

## Scope Index

### Python Consolidation (src/)
1. Value Coercion Utilities Module (tolerant + strict variants)
2. Session Factory Unification (DataFusionRuntimeProfile routing)
3. Policy Fingerprinting Protocol (reuse `FingerprintableConfig`)
4. Schema Builder Extension (extend `schema_builders.py`)
5. Telemetry Emission Facade (preserve existing API)
6. Validation Violation Unification (separate report types)
7. Storage Options Centralization

### Rust Consolidation (rust/)
8. UDF Registration Macro System (kind-explicit macros)
9. Config Extension Macro (local `macro_rules!`)
10. Python Binding Registration Macro (crate-local)
11. Error Conversion Consolidation

### Additional Opportunities
12. Plan-Bundle Execution Helpers
13. RecordBatchReader Coercion
14. Single UDF Catalog Source

---

## 1. Value Coercion Utilities Module

### Objective
Consolidate 30+ scattered `_coerce_*` function implementations into a single utilities module with **tolerant** (return None on failure) and **strict** (raise on failure) variants.

### Current State Analysis
Multiple files implement identical or near-identical coercion functions:

| Function | File Locations | Count |
|----------|---------------|-------|
| `_coerce_int()` | delta.py, scan_planner.py, delta_protocol.py, delta_observability.py, python_external_scope.py, extract_metadata.py | 6 |
| `_coerce_str_list()` | delta.py, scan_planner.py | 2 |
| `_coerce_bool()` | python_external_scope.py | 1 |
| `_coerce_add_actions()` | scan_planner.py, delta_control_plane.py | 2 |
| `_coerce_*` (other) | 20+ scattered implementations | 20+ |

### Target Files
- **Create:** `src/utils/value_coercion.py`
- **Modify:** All files containing `_coerce_*` functions to use centralized module

### Representative Pattern
```python
# src/utils/value_coercion.py
"""Value coercion utilities with tolerant and strict variants."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TypeVar

T = TypeVar("T")


class CoercionError(ValueError):
    """Raised when strict coercion fails."""

    def __init__(self, value: object, target_type: str, reason: str | None = None) -> None:
        self.value = value
        self.target_type = target_type
        msg = f"Cannot coerce {type(value).__name__} to {target_type}"
        if reason:
            msg = f"{msg}: {reason}"
        super().__init__(msg)


# === Tolerant Coercion (return None on failure) ===


def coerce_int(value: object) -> int | None:
    """Coerce value to int, returning None for unconvertible values.

    Parameters
    ----------
    value
        Any value to convert to integer.

    Returns
    -------
    int | None
        Integer value or None if conversion fails.
    """
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return int(stripped)
            except ValueError:
                return None
    return None


def coerce_float(value: object) -> float | None:
    """Coerce value to float, returning None for unconvertible values."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return float(stripped)
            except ValueError:
                return None
    return None


def coerce_bool(value: object) -> bool | None:
    """Coerce value to bool, returning None for unconvertible values."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        lower = value.strip().lower()
        if lower in ("true", "1", "yes", "on"):
            return True
        if lower in ("false", "0", "no", "off"):
            return False
    return None


def coerce_str(value: object) -> str | None:
    """Coerce value to string, returning None for None."""
    if value is None:
        return None
    return str(value)


def coerce_str_list(value: object) -> list[str]:
    """Coerce value to list of non-empty strings."""
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [str(item) for item in value if str(item).strip()]
    return []


def coerce_str_tuple(value: object) -> tuple[str, ...]:
    """Coerce value to tuple of non-empty strings."""
    return tuple(coerce_str_list(value))


def coerce_mapping_list(
    value: object,
) -> Sequence[Mapping[str, object]] | None:
    """Coerce value to sequence of mappings."""
    if value is None:
        return None
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [
            dict(item) if isinstance(item, Mapping) else {}
            for item in value
            if isinstance(item, Mapping)
        ]
    return None


# === Strict Coercion (raise on failure) ===


def raise_for_int(value: object, *, context: str = "") -> int:
    """Coerce value to int, raising CoercionError on failure.

    Parameters
    ----------
    value
        Value to convert.
    context
        Optional context for error message.

    Returns
    -------
    int
        Coerced integer value.

    Raises
    ------
    CoercionError
        If value cannot be coerced to int.
    """
    result = coerce_int(value)
    if result is None:
        raise CoercionError(value, "int", context or None)
    return result


def raise_for_float(value: object, *, context: str = "") -> float:
    """Coerce value to float, raising CoercionError on failure."""
    result = coerce_float(value)
    if result is None:
        raise CoercionError(value, "float", context or None)
    return result


def raise_for_bool(value: object, *, context: str = "") -> bool:
    """Coerce value to bool, raising CoercionError on failure."""
    result = coerce_bool(value)
    if result is None:
        raise CoercionError(value, "bool", context or None)
    return result


def raise_for_str(value: object, *, context: str = "") -> str:
    """Coerce value to str, raising CoercionError on failure for None."""
    if value is None:
        raise CoercionError(value, "str", context or "value is None")
    return str(value)
```

### ast-grep Discovery Patterns

**Note:** ast-grep metavariables (`$NAME`) do not work inside identifiers. Use full patterns or grep fallback for identifier fragments.

```bash
# Find _coerce_int implementations (full pattern match)
ast-grep run -l python -p 'def _coerce_int($ARGS):
    $$$BODY' --globs 'src/**/*.py'

# For identifier fragments, use grep instead
grep -rn "def _coerce_" src/ --include="*.py"

# Find all coercion function calls
ast-grep run -l python -p '_coerce_int($$$)' --globs 'src/**/*.py'
ast-grep run -l python -p '_coerce_str_list($$$)' --globs 'src/**/*.py'
ast-grep run -l python -p '_coerce_bool($$$)' --globs 'src/**/*.py'
```

### cq Analysis

```bash
# Find all coercion function callers
/cq calls _coerce_int
/cq calls _coerce_str_list
/cq calls _coerce_bool

# Analyze import impact after migration
/cq imports --module src.utils.value_coercion
```

### Deletions
After migration, delete local `_coerce_*` implementations from:
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/scan_planner.py`
- `src/datafusion_engine/delta_protocol.py`
- `src/datafusion_engine/delta_observability.py`
- `src/extract/python_external_scope.py`
- `src/datafusion_engine/extract_metadata.py`

### Checklist
- [ ] Create `src/utils/value_coercion.py` with tolerant + strict variants
- [ ] Add `CoercionError` exception class
- [ ] Add comprehensive unit tests for each coercion function
- [ ] Migrate `_coerce_int()` usages (6 files) - use tolerant or strict based on context
- [ ] Migrate `_coerce_str_list()` usages (2 files)
- [ ] Migrate `_coerce_bool()` usages (1 file)
- [ ] Migrate `_coerce_add_actions()` usages (2 files)
- [ ] Delete local implementations
- [ ] Verify no remaining `def _coerce_` patterns in src/ (use grep, not ast-grep)

---

## 2. Session Factory Unification

### Objective
Consolidate session context creation by routing delta helpers through `DataFusionRuntimeProfile`. This is a **minimal scope** focused on unifying delta runtime context patterns.

### Current State Analysis

| Location | Pattern | Purpose |
|----------|---------|---------|
| `src/datafusion_engine/session_factory.py` | `SessionFactory` class | DataFusion config |
| `src/storage/deltalake/delta.py` | `_runtime_ctx()`, `_runtime_profile_ctx()` | Delta operations |
| `src/datafusion_engine/runtime.py` | `DataFusionRuntimeProfile` | Runtime configuration |

### Target Files
- **Modify:** `src/datafusion_engine/runtime.py` (add delta helper methods)
- **Modify:** `src/storage/deltalake/delta.py` (delegate to runtime profile)

**Note:** Do NOT create `session_presets.py`. Keep scope minimal.

### Representative Pattern
```python
# src/datafusion_engine/runtime.py (extended)
@dataclass
class DataFusionRuntimeProfile:
    """Unified runtime profile for DataFusion sessions."""

    # ... existing fields ...

    def delta_runtime_ctx(self) -> SessionContext:
        """Create SessionContext configured for Delta operations.

        Returns
        -------
        SessionContext
            Context with delta-optimized settings applied.
        """
        ctx = self.build_session_context()
        # Apply delta-specific config from self.config
        return ctx

    def delta_runtime_profile_ctx(
        self,
        *,
        storage_options: Mapping[str, str] | None = None,
    ) -> SessionContext:
        """Create SessionContext with storage options for Delta operations.

        Parameters
        ----------
        storage_options
            Optional storage options to merge.

        Returns
        -------
        SessionContext
            Context configured for Delta operations.
        """
        ctx = self.delta_runtime_ctx()
        if storage_options:
            # Apply storage options to runtime env
            pass
        return ctx


# src/storage/deltalake/delta.py (simplified)
# Remove _runtime_ctx and _runtime_profile_ctx
# Use DataFusionRuntimeProfile.delta_runtime_ctx() instead
```

### ast-grep Discovery Patterns

```bash
# Find _runtime_ctx helper patterns
ast-grep run -l python -p 'def _runtime_ctx($$$):
    $$$BODY' --globs 'src/**/*.py'

# Find _runtime_profile_ctx patterns
ast-grep run -l python -p 'def _runtime_profile_ctx($$$):
    $$$BODY' --globs 'src/**/*.py'

# Find SessionContext creation
ast-grep run -l python -p 'SessionContext($$$)' --globs 'src/**/*.py'
```

### cq Analysis

```bash
# Find session factory usages
/cq calls SessionFactory
/cq calls _runtime_ctx
/cq calls _runtime_profile_ctx

# Analyze impact of changes
/cq impact runtime --param profile --depth 3
```

### Checklist
- [ ] Add `delta_runtime_ctx()` method to `DataFusionRuntimeProfile`
- [ ] Add `delta_runtime_profile_ctx()` method to `DataFusionRuntimeProfile`
- [ ] Update `storage/deltalake/delta.py` to use runtime profile methods
- [ ] Delete `_runtime_ctx()` from delta.py
- [ ] Delete `_runtime_profile_ctx()` from delta.py
- [ ] Verify delta operations work correctly

---

## 3. Policy Fingerprinting Protocol

### Objective
Unify 26+ policy classes under the **existing** `FingerprintableConfig` protocol from `config_base.py`. Do NOT create a new `src/policies/base.py`.

### Current State Analysis

| Policy Class | Location | Has Fingerprint |
|-------------|----------|-----------------|
| `DeltaStorePolicy` | delta_store_policy.py | ✓ |
| `DeltaWritePolicy` | storage/deltalake/config.py | ✓ |
| `DeltaSchemaPolicy` | storage/deltalake/config.py | ✓ |
| `ParquetWriterPolicy` | storage/deltalake/config.py | ✓ |
| `SchemaPolicy` | schema_policy.py | ✗ |
| `TagPolicy` | tag_policy.py | ✗ |
| `DiagnosticsPolicy` | pipeline_policy.py | ✗ |
| `PipelinePolicy` | pipeline_policy.py | ✗ |
| `ExecutionSurfacePolicy` | plan_policy.py | ✗ |
| `DataFusionWritePolicy` | schema_spec/policies.py | ✗ |
| ... (16+ more) | various | mixed |

### Existing Protocol
```python
# src/config_base.py (EXISTING - DO NOT RECREATE)
@runtime_checkable
class FingerprintableConfig(Protocol):
    """Protocol for configs that support content-based fingerprinting."""

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return payload dict for fingerprint computation."""
        ...

    def fingerprint(self) -> str:
        """Compute stable fingerprint from payload."""
        ...
```

### Target Files
- **Modify:** All policy classes to implement `FingerprintableConfig` protocol
- **DO NOT CREATE:** `src/policies/base.py` (use existing `config_base.py`)

### Representative Pattern
```python
# Example: src/datafusion_engine/schema_policy.py (modified)
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from config_base import FingerprintableConfig
from utils.hashing import hash_msgpack


@dataclass(frozen=True)
class SchemaPolicy(FingerprintableConfig):
    """Policy controlling schema validation behavior."""

    strict_nullability: bool = True
    allow_extra_columns: bool = False
    type_coercion_mode: str = "strict"

    def fingerprint_payload(self) -> Mapping[str, object]:
        return {
            "strict_nullability": self.strict_nullability,
            "allow_extra_columns": self.allow_extra_columns,
            "type_coercion_mode": self.type_coercion_mode,
        }

    def fingerprint(self) -> str:
        return hash_msgpack(self.fingerprint_payload())
```

### ast-grep Discovery Patterns

```bash
# Find all Policy class definitions
ast-grep run -l python -p '@dataclass$$$
class $NAME:
    $$$BODY' --globs 'src/**/*policy*.py'

# Find fingerprint_payload implementations
ast-grep run -l python -p 'def fingerprint_payload(self) -> $TYPE:
    $$$BODY' --globs 'src/**/*.py'

# Find fingerprint() implementations
ast-grep run -l python -p 'def fingerprint(self) -> str:
    $$$BODY' --globs 'src/**/*.py'
```

### cq Analysis

```bash
# Find policy class usages
/cq calls DeltaWritePolicy
/cq calls DeltaStorePolicy
/cq calls SchemaPolicy

# Find fingerprint usages
/cq calls fingerprint_payload
/cq calls fingerprint
```

### Checklist
- [ ] Audit all policy classes for `FingerprintableConfig` compliance
- [ ] Add `fingerprint_payload()` to `SchemaPolicy`
- [ ] Add `fingerprint_payload()` to `TagPolicy`
- [ ] Add `fingerprint_payload()` to `DiagnosticsPolicy`
- [ ] Add `fingerprint_payload()` to `PipelinePolicy`
- [ ] Add `fingerprint_payload()` to `ExecutionSurfacePolicy`
- [ ] Add fingerprint() methods using `hash_msgpack()` consistently
- [ ] Update tests to use protocol-based assertions
- [ ] Document which fields contribute to fingerprint identity

---

## 4. Schema Builder Extension

### Objective
Extend the **existing** `schema_builders.py` with field factory functions rather than creating a new `field_factories.py`.

### Current State Analysis

Repeated field patterns across 10+ files:
- `pa.field("version", pa.int32(), nullable=False)` - 3 locations
- `pa.field("task_name", pa.string(), nullable=False)` - 5+ locations
- `pa.field("fingerprint", pa.string(), nullable=False)` - 4+ locations
- `pa.field("dataset_name", pa.string(), nullable=False)` - 3 locations

### Target Files
- **Extend:** `src/arrow_utils/core/schema_builders.py` (add field factories)
- **DO NOT CREATE:** `src/arrow_utils/core/field_factories.py`

### Representative Pattern
```python
# src/arrow_utils/core/schema_builders.py (extended)
"""Schema builders and field factory functions for PyArrow."""
from __future__ import annotations

import pyarrow as pa


# === Field Factory Functions ===


def id_field(name: str = "id", *, nullable: bool = False) -> pa.Field:
    """Create standard identifier field (string, non-nullable by default)."""
    return pa.field(name, pa.string(), nullable=nullable)


def version_field(name: str = "version", *, nullable: bool = False) -> pa.Field:
    """Create version field (int32, non-nullable by default)."""
    return pa.field(name, pa.int32(), nullable=nullable)


def timestamp_field(name: str = "timestamp", *, nullable: bool = True) -> pa.Field:
    """Create timestamp field (timestamp[us], nullable by default)."""
    return pa.field(name, pa.timestamp("us"), nullable=nullable)


def task_name_field(name: str = "task_name", *, nullable: bool = False) -> pa.Field:
    """Create task name field (string, non-nullable by default)."""
    return pa.field(name, pa.string(), nullable=nullable)


def fingerprint_field(name: str = "fingerprint", *, nullable: bool = False) -> pa.Field:
    """Create fingerprint/hash field (string, non-nullable by default)."""
    return pa.field(name, pa.string(), nullable=nullable)


def plan_fingerprint_field(
    name: str = "plan_fingerprint", *, nullable: bool = False
) -> pa.Field:
    """Create plan fingerprint field."""
    return pa.field(name, pa.string(), nullable=nullable)


def dataset_name_field(name: str = "dataset_name", *, nullable: bool = False) -> pa.Field:
    """Create dataset name field."""
    return pa.field(name, pa.string(), nullable=nullable)


def path_field(name: str = "path", *, nullable: bool = False) -> pa.Field:
    """Create file path field."""
    return pa.field(name, pa.string(), nullable=nullable)


def line_field(name: str = "line", *, nullable: bool = False) -> pa.Field:
    """Create line number field (int32)."""
    return pa.field(name, pa.int32(), nullable=nullable)


def column_field(name: str = "column", *, nullable: bool = True) -> pa.Field:
    """Create column number field (int32, nullable)."""
    return pa.field(name, pa.int32(), nullable=nullable)


def byte_offset_field(name: str, *, nullable: bool = False) -> pa.Field:
    """Create byte offset field (int64)."""
    return pa.field(name, pa.int64(), nullable=nullable)


# === Schema Builders ===


def versioned_metadata_schema(
    name: str,
    extra_fields: list[pa.Field],
    *,
    schema_version: int = 1,
) -> pa.Schema:
    """Create schema with standard version + name + extra fields.

    Parameters
    ----------
    name
        Base name for the primary identifier field.
    extra_fields
        Additional fields beyond version and name.
    schema_version
        Schema version number for metadata.

    Returns
    -------
    pa.Schema
        Schema with version field, name field, and extra fields.
    """
    fields = [
        version_field(),
        pa.field(name, pa.string(), nullable=False),
        *extra_fields,
    ]
    metadata = {"schema_version": str(schema_version)}
    return pa.schema(fields, metadata=metadata)


def plan_fingerprint_entry_type() -> pa.StructType:
    """Create struct type for plan fingerprint entries."""
    return pa.struct([
        pa.field("plan_name", pa.string(), nullable=False),
        pa.field("plan_fingerprint", pa.string(), nullable=False),
        pa.field("plan_task_signature", pa.string(), nullable=False),
    ])
```

### ast-grep Discovery Patterns

```bash
# Find pa.field definitions with common patterns
ast-grep run -l python -p 'pa.field("version", pa.int32()$$$)' --globs 'src/**/*.py'
ast-grep run -l python -p 'pa.field("task_name", pa.string()$$$)' --globs 'src/**/*.py'
ast-grep run -l python -p 'pa.field("fingerprint", pa.string()$$$)' --globs 'src/**/*.py'

# Find pa.schema definitions
ast-grep run -l python -p 'pa.schema([$$$])' --globs 'src/**/*.py'

# Find pa.struct definitions
ast-grep run -l python -p 'pa.struct([$$$])' --globs 'src/**/*.py'
```

### cq Analysis

```bash
# Analyze schema usage patterns
/cq imports --module pyarrow

# Check for schema-related functions
/cq calls pa.field
/cq calls pa.schema
```

### Checklist
- [ ] Add field factory functions to `src/arrow_utils/core/schema_builders.py`
- [ ] Add `versioned_metadata_schema()` builder
- [ ] Add common struct type factories
- [ ] Migrate `src/incremental/` schema definitions to use factories
- [ ] Migrate `src/hamilton_pipeline/modules/` schema definitions
- [ ] Update imports across codebase
- [ ] Add type stubs for IDE support

---

## 5. Telemetry Emission Facade

### Objective
Consolidate 8+ `record_*` functions in `src/obs/otel/metrics.py` that share 80% boilerplate while **preserving the existing API shape** (including `status` as a required parameter).

### Current State Analysis

Functions in `src/obs/otel/metrics.py`:
- `record_stage_duration()` (lines 270-285)
- `record_task_duration()` (lines 288-303)
- `record_datafusion_duration()` (lines 306-321)
- `record_write_duration()` (lines 324-339)
- `record_artifact_count()` (lines 342-356)
- `record_error()` (lines 359-373)
- `set_dataset_stats()` (lines 376-386)
- `set_scan_telemetry()` (lines 389-399)

All functions follow identical pattern:
1. Get registry via `_registry()`
2. Build payload dict with attributes
3. Add run_id via `_with_run_id()`
4. Normalize attributes via `normalize_attributes()`
5. Call instrument method

### Target Files
- **Modify:** `src/obs/otel/metrics.py` (internal refactor only)

**Note:** Do NOT change public function signatures. `status` is a required parameter.

### Representative Pattern
```python
# src/obs/otel/metrics.py (internal helper - not exported)
def _emit_metric(
    registry: MetricRegistry,
    *,
    metric_type: Literal["duration", "count", "gauge"],
    metric_name: str,
    value: float | int,
    base_attributes: Mapping[str, object],
    extra_attributes: Mapping[str, object] | None = None,
    run_id: str | None = None,
) -> None:
    """Internal: Emit metric with normalized attributes."""
    attributes = dict(base_attributes)
    if extra_attributes:
        attributes.update(extra_attributes)
    if run_id:
        attributes["run_id"] = run_id

    normalized = normalize_attributes(attributes)

    if metric_type == "duration":
        registry.record_duration(metric_name, float(value), normalized)
    elif metric_type == "count":
        registry.record_count(metric_name, int(value), normalized)
    else:
        registry.set_gauge(metric_name, float(value), normalized)


# Public API (UNCHANGED - status remains required)
def record_stage_duration(
    stage: str,
    duration_s: float,
    *,
    status: str,  # REQUIRED - do not make optional
    attributes: Mapping[str, object] | None = None,
) -> None:
    """Record stage execution duration.

    Parameters
    ----------
    stage
        Stage identifier.
    duration_s
        Duration in seconds.
    status
        Execution status (required).
    attributes
        Optional additional attributes.
    """
    _emit_metric(
        _registry(),
        metric_type="duration",
        metric_name="stage_duration",
        value=duration_s,
        base_attributes={"stage": stage, "status": status},
        extra_attributes=attributes,
        run_id=_current_run_id(),
    )


def record_task_duration(
    task_kind: str,
    duration_s: float,
    *,
    status: str,  # REQUIRED - do not make optional
    attributes: Mapping[str, object] | None = None,
) -> None:
    """Record task execution duration.

    Parameters
    ----------
    task_kind
        Task kind identifier.
    duration_s
        Duration in seconds.
    status
        Execution status (required).
    attributes
        Optional additional attributes.
    """
    _emit_metric(
        _registry(),
        metric_type="duration",
        metric_name="task_duration",
        value=duration_s,
        base_attributes={"task_kind": task_kind, "status": status},
        extra_attributes=attributes,
        run_id=_current_run_id(),
    )
```

### ast-grep Discovery Patterns

```bash
# Find record_* function definitions
grep -n "def record_" src/obs/otel/metrics.py

# Find metric registry calls
ast-grep run -l python -p '_registry().$METHOD($$$)' --globs 'src/obs/**/*.py'

# Find normalize_attributes calls
ast-grep run -l python -p 'normalize_attributes($$$)' --globs 'src/obs/**/*.py'
```

### cq Analysis

```bash
# Find all record_* usages
/cq calls record_stage_duration
/cq calls record_task_duration
/cq calls record_artifact_count

# Analyze telemetry flow
/cq impact metrics --param registry --depth 3
```

### Checklist
- [ ] Add `_emit_metric()` internal helper to `src/obs/otel/metrics.py`
- [ ] Refactor `record_stage_duration()` to use helper (preserve signature)
- [ ] Refactor `record_task_duration()` to use helper (preserve signature)
- [ ] Refactor `record_datafusion_duration()` to use helper (preserve signature)
- [ ] Refactor `record_write_duration()` to use helper (preserve signature)
- [ ] Refactor `record_artifact_count()` to use helper (preserve signature)
- [ ] Refactor `record_error()` to use helper (preserve signature)
- [ ] Verify all metrics emit correctly after refactor
- [ ] Ensure `status` remains a required parameter in all public APIs
- [ ] Update tests

---

## 6. Validation Violation Unification

### Objective
Consolidate multiple validation violation types into a **single unified violation object**, while **keeping separate report types** for different validation contexts (schema validation vs constraint validation).

### Current State Analysis

| Type | Location | Fields |
|------|----------|--------|
| `ValidationReport` | schema_validation.py:101 | entries, passed, failed |
| `_ValidationErrorEntry` | schema_validation.py:112 | code, column, expected, actual |
| `SchemaViolation` | schema_contracts.py:44 | type, column, expected, actual, message |
| `SchemaViolationType` | schema_contracts.py:32 | enum of violation types |

### Target Files
- **Create:** `src/validation/violations.py` (unified violation objects only)
- **Modify:** `src/datafusion_engine/schema_validation.py` (use unified violations)
- **Modify:** `src/datafusion_engine/schema_contracts.py` (use unified violations)
- **Keep Separate:** `SchemaValidationReport`, `ConstraintValidationReport` (different report types)

### Representative Pattern
```python
# src/validation/violations.py
"""Unified validation violation types."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Self


class ViolationType(Enum):
    """Types of validation violations."""

    MISSING_COLUMN = auto()
    EXTRA_COLUMN = auto()
    TYPE_MISMATCH = auto()
    NULLABLE_MISMATCH = auto()
    NULL_VALUE = auto()
    CONSTRAINT_VIOLATION = auto()
    KEY_VIOLATION = auto()
    CUSTOM = auto()


@dataclass(frozen=True)
class ValidationViolation:
    """Single validation violation (unified across all validation contexts)."""

    violation_type: ViolationType
    message: str
    column: str | None = None
    expected: str | None = None
    actual: str | None = None
    row_index: int | None = None
    severity: str = "error"

    @classmethod
    def missing_column(cls, column: str, expected_type: str) -> Self:
        """Create missing column violation."""
        return cls(
            violation_type=ViolationType.MISSING_COLUMN,
            message=f"Missing required column: {column}",
            column=column,
            expected=expected_type,
        )

    @classmethod
    def type_mismatch(cls, column: str, expected: str, actual: str) -> Self:
        """Create type mismatch violation."""
        return cls(
            violation_type=ViolationType.TYPE_MISMATCH,
            message=f"Type mismatch for {column}: expected {expected}, got {actual}",
            column=column,
            expected=expected,
            actual=actual,
        )

    @classmethod
    def null_value(cls, column: str, row_index: int | None = None) -> Self:
        """Create null value violation."""
        return cls(
            violation_type=ViolationType.NULL_VALUE,
            message=f"Null value in non-nullable column: {column}",
            column=column,
            row_index=row_index,
        )

    @classmethod
    def constraint_violation(cls, constraint: str, message: str) -> Self:
        """Create constraint violation."""
        return cls(
            violation_type=ViolationType.CONSTRAINT_VIOLATION,
            message=message,
            expected=constraint,
        )


# Keep SEPARATE report types for different contexts

@dataclass
class SchemaValidationReport:
    """Report from schema validation (column types, nullability)."""

    violations: list[ValidationViolation]
    schema_name: str | None = None

    @property
    def passed(self) -> bool:
        return len(self.violations) == 0

    @property
    def error_count(self) -> int:
        return sum(1 for v in self.violations if v.severity == "error")


@dataclass
class ConstraintValidationReport:
    """Report from constraint validation (CHECK constraints, keys)."""

    violations: list[ValidationViolation]
    table_name: str | None = None

    @property
    def passed(self) -> bool:
        return len(self.violations) == 0

    @property
    def constraint_names(self) -> list[str]:
        return [v.expected for v in self.violations if v.expected]
```

### ast-grep Discovery Patterns

```bash
# Find SchemaViolation usages
ast-grep run -l python -p 'SchemaViolation($$$)' --globs 'src/**/*.py'

# Find _ValidationErrorEntry usages
ast-grep run -l python -p '_ValidationErrorEntry($$$)' --globs 'src/**/*.py'

# Find ValidationReport usages
ast-grep run -l python -p 'ValidationReport($$$)' --globs 'src/**/*.py'
```

### cq Analysis

```bash
# Find validation function callers
/cq calls validate_schema
/cq calls validate_table
/cq calls SchemaViolation
```

### Checklist
- [ ] Create `src/validation/violations.py` with unified `ValidationViolation`
- [ ] Create `src/validation/__init__.py` with exports
- [ ] Migrate `SchemaViolation` usages to `ValidationViolation`
- [ ] Migrate `_ValidationErrorEntry` usages to `ValidationViolation`
- [ ] Keep `SchemaValidationReport` separate (use `ValidationViolation` internally)
- [ ] Keep `ConstraintValidationReport` separate (use `ValidationViolation` internally)
- [ ] Update `schema_validation.py` to use unified violations
- [ ] Update `schema_contracts.py` to use unified violations
- [ ] Delete obsolete violation type definitions
- [ ] Update tests

---

## 7. Storage Options Centralization

### Objective
Ensure all storage options merging uses centralized `src/utils/storage_options.py`.

### Current State Analysis

| Pattern | Location | Status |
|---------|----------|--------|
| `_log_storage_dict()` | storage/deltalake/delta.py:2245 | Custom impl |
| `_delta_storage_options()` | scan_planner.py:623 | Custom impl |
| `merged_storage_options()` | utils/storage_options.py:42 | Canonical |
| `normalize_storage_options()` | utils/storage_options.py:14 | Canonical |

### Target Files
- **Modify:** `src/storage/deltalake/delta.py`
- **Modify:** `src/datafusion_engine/scan_planner.py`

### Representative Pattern
```python
# src/utils/storage_options.py (existing, to be used consistently)
def merged_storage_options(
    storage_options: Mapping[str, str] | None,
    log_storage_options: Mapping[str, str] | None,
) -> dict[str, str] | None:
    """Merge storage and log storage options.

    Parameters
    ----------
    storage_options
        Primary storage options.
    log_storage_options
        Log-specific storage options (takes precedence).

    Returns
    -------
    dict[str, str] | None
        Merged options or None if both inputs are None/empty.
    """
    merged: dict[str, str] = {}
    if storage_options:
        merged.update(normalize_storage_options(storage_options))
    if log_storage_options:
        merged.update(normalize_storage_options(log_storage_options))
    return merged or None
```

### ast-grep Discovery Patterns

```bash
# Find custom storage merge functions
ast-grep run -l python -p 'def _log_storage_dict($$$):
    $$$BODY' --globs 'src/**/*.py'

ast-grep run -l python -p 'def _delta_storage_options($$$):
    $$$BODY' --globs 'src/**/*.py'

# Find inline storage merging
ast-grep run -l python -p 'if storage_options:
    merged.update($$$)' --globs 'src/**/*.py'
```

### cq Analysis

```bash
/cq calls merged_storage_options
/cq calls normalize_storage_options
/cq calls _log_storage_dict
```

### Checklist
- [ ] Update `storage/deltalake/delta.py` to use `merged_storage_options()`
- [ ] Update `scan_planner.py` to use `merged_storage_options()`
- [ ] Delete `_log_storage_dict()` function
- [ ] Delete `_delta_storage_options()` function
- [ ] Verify all storage option usages go through centralized utilities

---

## 8. Rust UDF Registration Macro System

### Objective
Reduce 300+ lines of UDF registration boilerplate to ~50 lines using **kind-explicit macros** (`scalar_udfs!`, `aggregate_udfs!`, `window_udfs!`, `table_udfs!`).

### Current State Analysis

Files with registration boilerplate:
- `rust/datafusion_ext/src/udf_registry.rs` (lines 41-304)
- `rust/datafusion_ext/src/udaf_builtin.rs` (lines 31-51)
- `rust/datafusion_ext/src/udwf_builtin.rs` (lines 5-14)
- `rust/datafusion_ext/src/udtf_builtin.rs` (lines 19-28)

Pattern: Each UDF type repeats:
1. Define spec struct
2. Populate vec with specs
3. Match on kind to call correct registration method

### Target Files
- **Create:** `rust/datafusion_ext/src/macros.rs`
- **Modify:** `rust/datafusion_ext/src/udf_registry.rs`

### Representative Pattern
```rust
// rust/datafusion_ext/src/macros.rs

/// Macro to define scalar UDF specs.
#[macro_export]
macro_rules! scalar_udfs {
    (
        $( $name:literal => $builder:path $(, aliases: [$($alias:literal),*])? );* $(;)?
    ) => {
        vec![
            $(
                ScalarUdfSpec {
                    name: $name,
                    builder: Box::new($builder),
                    aliases: vec![$($($alias),*)?],
                }
            ),*
        ]
    };
}

/// Macro to define aggregate UDF specs.
#[macro_export]
macro_rules! aggregate_udfs {
    (
        $( $name:literal => $builder:path $(, aliases: [$($alias:literal),*])? );* $(;)?
    ) => {
        vec![
            $(
                AggregateUdfSpec {
                    name: $name,
                    builder: Box::new($builder),
                    aliases: vec![$($($alias),*)?],
                }
            ),*
        ]
    };
}

/// Macro to define window UDF specs.
#[macro_export]
macro_rules! window_udfs {
    (
        $( $name:literal => $builder:path $(, aliases: [$($alias:literal),*])? );* $(;)?
    ) => {
        vec![
            $(
                WindowUdfSpec {
                    name: $name,
                    builder: Box::new($builder),
                    aliases: vec![$($($alias),*)?],
                }
            ),*
        ]
    };
}

/// Macro to define table UDF specs.
#[macro_export]
macro_rules! table_udfs {
    (
        $( $name:literal => $builder:path $(, aliases: [$($alias:literal),*])? );* $(;)?
    ) => {
        vec![
            $(
                TableUdfSpec {
                    name: $name,
                    builder: Box::new($builder),
                    aliases: vec![$($($alias),*)?],
                }
            ),*
        ]
    };
}

// Usage in udf_registry.rs:
pub fn builtin_scalar_udfs() -> Vec<ScalarUdfSpec> {
    scalar_udfs![
        "arrow_metadata" => crate::udf_custom::arrow_metadata;
        "semantic_tag" => crate::udf_custom::semantic_tag;
        "json_extract" => crate::udf_custom::json_extract, aliases: ["json_value", "json_get"];
        // ... 40+ more entries as single lines
    ]
}

pub fn builtin_aggregate_udfs() -> Vec<AggregateUdfSpec> {
    aggregate_udfs![
        "semantic_count" => crate::udaf_custom::semantic_count;
        // ...
    ]
}

pub fn builtin_table_udfs() -> Vec<TableUdfSpec> {
    table_udfs![
        "read_parquet" => crate::udtf_custom::read_parquet;
        // ...
    ]
}
```

### ast-grep Discovery Patterns

```bash
# Find UdfSpec definitions
ast-grep run -l rust -p 'UdfSpec {
    $$$
}' --globs 'rust/**/*.rs'

# Find ctx.register_udf calls
ast-grep run -l rust -p '$CTX.register_udf($$$)' --globs 'rust/**/*.rs'
ast-grep run -l rust -p '$CTX.register_udaf($$$)' --globs 'rust/**/*.rs'
ast-grep run -l rust -p '$CTX.register_udtf($$$)' --globs 'rust/**/*.rs'
```

### Checklist
- [ ] Create `rust/datafusion_ext/src/macros.rs`
- [ ] Implement `scalar_udfs!` macro
- [ ] Implement `aggregate_udfs!` macro
- [ ] Implement `window_udfs!` macro
- [ ] Implement `table_udfs!` macro
- [ ] Define separate spec structs: `ScalarUdfSpec`, `AggregateUdfSpec`, `WindowUdfSpec`, `TableUdfSpec`
- [ ] Refactor `udf_registry.rs` to use kind-explicit macros
- [ ] Refactor `udaf_builtin.rs` to use macros
- [ ] Refactor `udwf_builtin.rs` to use macros
- [ ] Refactor `udtf_builtin.rs` to use macros
- [ ] Verify all UDFs register correctly
- [ ] Update tests

---

## 9. Rust Config Extension Macro

### Objective
Eliminate duplicate `ExtensionOptions` trait implementations via **local `macro_rules!`** instead of a proc-macro crate.

### Current State Analysis

Files with duplicate trait implementations:
- `rust/datafusion_ext/src/udf_config.rs` (lines 94-190)
- `rust/datafusion_ext/src/planner_rules.rs` (lines 40-96)
- `rust/datafusion_ext/src/physical_rules.rs` (lines 32-70)

Each implements identical:
- `as_any()`, `as_any_mut()`, `cloned()` methods
- `parse_bool()`, `parse_i32()` helper functions
- `entries()` boilerplate

### Target Files
- **Create:** `rust/datafusion_ext/src/config_macros.rs` (local `macro_rules!`)
- **Modify:** `rust/datafusion_ext/src/udf_config.rs`
- **Modify:** `rust/datafusion_ext/src/planner_rules.rs`
- **Modify:** `rust/datafusion_ext/src/physical_rules.rs`

**Note:** Do NOT create a separate proc-macro crate. Use local `macro_rules!` instead.

### Representative Pattern
```rust
// rust/datafusion_ext/src/config_macros.rs

/// Local macro to implement ExtensionOptions boilerplate.
///
/// Usage:
/// ```rust
/// impl_extension_options!(
///     CodeAnatomyUdfConfig,
///     prefix = "codeanatomy_udf",
///     fields = [
///         (enable_semantic_tags, bool, true),
///         (max_json_depth, i32, 64),
///     ]
/// );
/// ```
#[macro_export]
macro_rules! impl_extension_options {
    (
        $config_type:ty,
        prefix = $prefix:literal,
        fields = [
            $(($field:ident, $field_type:ty, $default:expr)),* $(,)?
        ]
    ) => {
        impl datafusion::config::ExtensionOptions for $config_type {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
                self
            }

            fn cloned(&self) -> Box<dyn datafusion::config::ExtensionOptions> {
                Box::new(self.clone())
            }

            fn set(&mut self, key: &str, value: &str) -> datafusion::common::Result<()> {
                let key = key.strip_prefix(concat!($prefix, ".")).unwrap_or(key);
                match key {
                    $(
                        stringify!($field) => {
                            self.$field = $crate::config_macros::parse_value::<$field_type>(value)?;
                        }
                    )*
                    _ => {
                        return Err(datafusion::common::DataFusionError::Configuration(
                            format!("Unknown config key: {}", key)
                        ));
                    }
                }
                Ok(())
            }

            fn entries(&self) -> Vec<datafusion::config::ConfigEntry> {
                vec![
                    $(
                        datafusion::config::ConfigEntry {
                            key: concat!($prefix, ".", stringify!($field)).to_string(),
                            value: Some(format!("{:?}", self.$field)),
                            description: "",
                        },
                    )*
                ]
            }
        }
    };
}

/// Parse value from string for config types.
pub fn parse_value<T: ConfigParseable>(value: &str) -> datafusion::common::Result<T> {
    T::parse_config(value)
}

pub trait ConfigParseable: Sized {
    fn parse_config(value: &str) -> datafusion::common::Result<Self>;
}

impl ConfigParseable for bool {
    fn parse_config(value: &str) -> datafusion::common::Result<Self> {
        value.parse().map_err(|e| {
            datafusion::common::DataFusionError::Configuration(format!("Invalid bool: {}", e))
        })
    }
}

impl ConfigParseable for i32 {
    fn parse_config(value: &str) -> datafusion::common::Result<Self> {
        value.parse().map_err(|e| {
            datafusion::common::DataFusionError::Configuration(format!("Invalid i32: {}", e))
        })
    }
}

// Usage in udf_config.rs:
#[derive(Clone, Debug)]
pub struct CodeAnatomyUdfConfig {
    pub enable_semantic_tags: bool,
    pub max_json_depth: i32,
}

impl Default for CodeAnatomyUdfConfig {
    fn default() -> Self {
        Self {
            enable_semantic_tags: true,
            max_json_depth: 64,
        }
    }
}

impl_extension_options!(
    CodeAnatomyUdfConfig,
    prefix = "codeanatomy_udf",
    fields = [
        (enable_semantic_tags, bool, true),
        (max_json_depth, i32, 64),
    ]
);
```

### Checklist
- [ ] Create `rust/datafusion_ext/src/config_macros.rs` with `impl_extension_options!`
- [ ] Add `ConfigParseable` trait with implementations for common types
- [ ] Migrate `CodeAnatomyUdfConfig` to use macro
- [ ] Migrate `CodeAnatomyPlannerRulesConfig` to use macro
- [ ] Migrate `CodeAnatomyPhysicalRulesConfig` to use macro
- [ ] Delete duplicate `as_any()`, `as_any_mut()`, `cloned()` implementations
- [ ] Delete duplicate `parse_bool()`, `parse_i32()` helper functions
- [ ] Verify all configs work correctly

---

## 10. Python Binding Registration Macro

### Objective
Reduce 90+ lines of `module.add_function()` calls to ~15 lines using a **crate-local macro**.

### Current State Analysis

File: `rust/datafusion_python/src/codeanatomy_ext.rs` (lines 2470-2560)

Pattern: 48+ identical calls:
```rust
module.add_function(wrap_pyfunction!(udf_custom_py::arrow_metadata, module)?)?;
module.add_function(wrap_pyfunction!(udf_custom_py::semantic_tag, module)?)?;
// ... 46 more
```

### Target Files
- **Modify:** `rust/datafusion_python/src/codeanatomy_ext.rs` (add crate-local macro)

**Note:** Keep macro crate-local (module-scoped), not in a separate file.

### Representative Pattern
```rust
// rust/datafusion_python/src/codeanatomy_ext.rs

/// Register multiple pyfunctions in a module.
/// Crate-local macro for reducing registration boilerplate.
macro_rules! register_pyfunctions {
    ($module:expr, $mod_path:path, [$($func:ident),* $(,)?]) => {
        $(
            $module.add_function(pyo3::wrap_pyfunction!($mod_path::$func, $module)?)?;
        )*
    };
}

/// Register multiple pyclasses in a module.
macro_rules! register_pyclasses {
    ($module:expr, [$($class:ty),* $(,)?]) => {
        $(
            $module.add_class::<$class>()?;
        )*
    };
}

// Usage in the same file:
pub fn register_udf_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_pyfunctions!(module, udf_custom_py, [
        arrow_metadata,
        semantic_tag,
        json_extract,
        json_value,
        json_path,
        // ... 43 more as simple identifiers
    ]);
    Ok(())
}

pub fn register_udf_classes(module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_pyclasses!(module, [
        PyScalarUDF,
        PyAggregateUDF,
        PyWindowUDF,
        PyTableUDF,
    ]);
    Ok(())
}
```

### Checklist
- [ ] Add `register_pyfunctions!` macro to `codeanatomy_ext.rs` (crate-local)
- [ ] Add `register_pyclasses!` macro to `codeanatomy_ext.rs` (crate-local)
- [ ] Refactor registration calls to use macros
- [ ] Group related functions for clarity
- [ ] Verify all Python bindings work correctly
- [ ] Update tests

---

## 11. Rust Error Conversion Consolidation

### Objective
Consolidate duplicate `From<T>` trait implementations across error modules.

### Current State Analysis

| Location | From Implementations |
|----------|---------------------|
| `datafusion_ext/src/errors.rs` | `From<ArrowError>`, `From<DataFusionError>`, `From<DeltaTableError>` |
| `datafusion_python/src/errors.rs` | `From<ArrowError>`, `From<InnerDataFusionError>`, `From<ExtError>`, `From<PyDataFusionError>` |

### Target Files
- **Create:** `rust/datafusion_ext/src/error_conversion.rs`
- **Modify:** `rust/datafusion_ext/src/errors.rs`
- **Modify:** `rust/datafusion_python/src/errors.rs`

### Representative Pattern
```rust
// rust/datafusion_ext/src/error_conversion.rs

/// Macro to implement From trait for error wrapping.
#[macro_export]
macro_rules! impl_error_from {
    ($target:ty, $source:ty, $variant:ident) => {
        impl From<$source> for $target {
            fn from(err: $source) -> Self {
                Self::$variant(err)
            }
        }
    };

    // With transformation
    ($target:ty, $source:ty, $variant:ident, |$e:ident| $transform:expr) => {
        impl From<$source> for $target {
            fn from($e: $source) -> Self {
                Self::$variant($transform)
            }
        }
    };
}

// Usage in errors.rs:
impl_error_from!(ExtError, ArrowError, Arrow);
impl_error_from!(ExtError, DataFusionError, DataFusion);
impl_error_from!(ExtError, DeltaTableError, Delta);

// More complex transformation:
impl_error_from!(PyDataFusionError, ExtError, Ext, |e| e.to_string());
```

### Checklist
- [ ] Create `rust/datafusion_ext/src/error_conversion.rs`
- [ ] Implement `impl_error_from!` macro
- [ ] Refactor `datafusion_ext/src/errors.rs` to use macro
- [ ] Refactor `datafusion_python/src/errors.rs` to use macro
- [ ] Ensure error messages are preserved
- [ ] Update tests

---

## 12. Plan-Bundle Execution Helpers (Additional Opportunity)

### Objective
Consolidate 3 nearly-identical plan-bundle execution loops into a single parameterized helper.

### Current State Analysis

Three files contain similar plan execution patterns:
- `src/datafusion_engine/planning_pipeline.py`
- `src/hamilton_pipeline/modules/execution_plan.py`
- `src/incremental/plan_bundle_exec.py`

Pattern: Build plan bundle → validate → execute → capture artifacts → emit telemetry

### Target Files
- **Create:** `src/datafusion_engine/plan_execution.py`
- **Modify:** Files with duplicate execution loops

### Representative Pattern
```python
# src/datafusion_engine/plan_execution.py
"""Unified plan-bundle execution helpers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import DataFrame
    from datafusion_engine.plan_bundle import PlanBundle


@dataclass
class PlanExecutionResult:
    """Result of plan execution."""

    plan_bundle: PlanBundle
    output: DataFrame | None
    artifacts: dict[str, object]
    telemetry: dict[str, float]


def execute_plan_bundle(
    plan_bundle: PlanBundle,
    *,
    emit_artifacts: bool = True,
    emit_telemetry: bool = True,
    validate_output: bool = True,
) -> PlanExecutionResult:
    """Execute a plan bundle with consistent artifact and telemetry emission.

    Parameters
    ----------
    plan_bundle
        The plan bundle to execute.
    emit_artifacts
        Whether to emit plan artifacts.
    emit_telemetry
        Whether to emit execution telemetry.
    validate_output
        Whether to validate output schema.

    Returns
    -------
    PlanExecutionResult
        Execution result with output, artifacts, and telemetry.
    """
    # Unified execution logic here
    ...
```

### Checklist
- [ ] Identify common execution patterns across the 3 files
- [ ] Create `src/datafusion_engine/plan_execution.py`
- [ ] Implement `execute_plan_bundle()` helper
- [ ] Migrate `planning_pipeline.py` to use helper
- [ ] Migrate `execution_plan.py` to use helper
- [ ] Migrate `plan_bundle_exec.py` to use helper
- [ ] Verify artifact and telemetry emission is consistent

---

## 13. RecordBatchReader Coercion (Additional Opportunity)

### Objective
Consolidate 5 files that duplicate `coerce_recordbatchreader` logic into a single utility.

### Current State Analysis

Files with RecordBatchReader coercion:
- `src/datafusion_engine/io_adapter.py`
- `src/datafusion_engine/scan_planner.py`
- `src/storage/deltalake/delta.py`
- `src/hamilton_pipeline/materializers.py`
- `src/incremental/plan_bundle_exec.py`

Pattern: Accept various input types → coerce to RecordBatchReader → process

### Target Files
- **Add to:** `src/utils/value_coercion.py` (extend with RecordBatchReader coercion)

### Representative Pattern
```python
# src/utils/value_coercion.py (add to existing file)
import pyarrow as pa


def coerce_to_recordbatch_reader(
    value: object,
) -> pa.RecordBatchReader | None:
    """Coerce value to RecordBatchReader.

    Parameters
    ----------
    value
        Value to coerce. Accepts:
        - RecordBatchReader (returned as-is)
        - Table (converted to reader)
        - RecordBatch (wrapped in reader)
        - Sequence[RecordBatch] (wrapped in reader)

    Returns
    -------
    pa.RecordBatchReader | None
        RecordBatchReader or None if coercion fails.
    """
    if value is None:
        return None
    if isinstance(value, pa.RecordBatchReader):
        return value
    if isinstance(value, pa.Table):
        return value.to_reader()
    if isinstance(value, pa.RecordBatch):
        return pa.RecordBatchReader.from_batches(value.schema, [value])
    if isinstance(value, (list, tuple)):
        batches = [b for b in value if isinstance(b, pa.RecordBatch)]
        if batches:
            return pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    return None
```

### Checklist
- [ ] Add `coerce_to_recordbatch_reader()` to `src/utils/value_coercion.py`
- [ ] Migrate `io_adapter.py` to use centralized function
- [ ] Migrate `scan_planner.py` to use centralized function
- [ ] Migrate `delta.py` to use centralized function
- [ ] Migrate `materializers.py` to use centralized function
- [ ] Migrate `plan_bundle_exec.py` to use centralized function
- [ ] Delete local implementations

---

## 14. Single UDF Catalog Source (Additional Opportunity)

### Objective
Use DataFusion `CREATE FUNCTION` DDL as the single source for UDF registration, allowing both DataFrame API and TableProvider paths to share the same catalog.

### DataFusion Capability Notes

**TableProvider Path for DDL:**
DataFusion supports registering UDFs via DDL:
```sql
CREATE FUNCTION my_udf AS 'path.to.udf' LANGUAGE PYTHON;
```

This allows:
1. UDFs to be registered once in the catalog
2. Both DataFrame API and SQL queries share the same UDF definitions
3. UDFs visible via `information_schema.routines`

**INSERT Execution Layer:**
DataFusion 50.1+ supports INSERT INTO execution, which can leverage the same session context and UDF catalog:
```sql
INSERT INTO target_table SELECT my_udf(col) FROM source_table;
```

**Delta Constraints via information_schema:**
Delta constraints are surfaced through DataFusion's information_schema, enabling unified constraint introspection:
```sql
SELECT * FROM information_schema.table_constraints WHERE table_name = 'my_delta_table';
```

### Target Files
- **Modify:** `rust/datafusion_ext/src/udf_registry.rs`
- **Modify:** `src/datafusion_engine/runtime.py`

### Representative Pattern
```python
# src/datafusion_engine/runtime.py
def register_udfs_via_ddl(ctx: SessionContext, udf_specs: list[UdfSpec]) -> None:
    """Register UDFs via CREATE FUNCTION DDL for catalog visibility.

    Parameters
    ----------
    ctx
        SessionContext to register UDFs in.
    udf_specs
        UDF specifications to register.
    """
    for spec in udf_specs:
        ddl = f"CREATE FUNCTION {spec.name} AS '{spec.module_path}' LANGUAGE RUST"
        ctx.sql(ddl)
```

### Checklist
- [ ] Audit current UDF registration paths
- [ ] Implement DDL-based UDF registration
- [ ] Ensure UDFs visible in information_schema.routines
- [ ] Update DataFrame API to use catalog UDFs
- [ ] Update SQL execution to use catalog UDFs
- [ ] Document single-source UDF pattern

---

## Verification Commands

### Python Quality Gates
```bash
uv run ruff check --fix
uv run pyrefly check
uv run pyright --warnings --pythonversion=3.13
uv run pytest tests/unit/ -v
```

### Rust Quality Gates
```bash
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

### Integration Verification
```bash
uv run pytest tests/integration/ -v
uv run pytest tests/e2e/ -v
```

---

## Appendix A: ast-grep Tooling Notes

### Important Limitations

**Metavariables in Identifiers:**
ast-grep metavariables (`$NAME`) do **not** work inside identifiers. The pattern `def _coerce_$NAME` will NOT match `def _coerce_int`.

**Workarounds:**
1. Use full patterns: `def _coerce_int($ARGS):`
2. Use grep for identifier fragments: `grep -rn "def _coerce_" src/`
3. Use multiple ast-grep patterns for known variants

### Use ast-grep when:
- Finding structural patterns (function definitions, class declarations)
- Performing codemods/rewrites
- Pattern matching without false positives from strings/comments
- Verifying code has been removed (after migration)

### Use grep/rg when:
- Searching for identifier fragments
- Finding patterns across naming conventions
- Quick discovery before ast-grep refinement

### Use cq when:
- Analyzing function call sites (`/cq calls`)
- Tracing parameter impact (`/cq impact`)
- Understanding import dependencies (`/cq imports`)
- Checking closure captures (`/cq scopes`)

---

## Appendix B: DataFusion/Delta Capability Reference

### DataFusion Features to Leverage

| Feature | Use Case | API |
|---------|----------|-----|
| CREATE FUNCTION DDL | Single UDF catalog source | `ctx.sql("CREATE FUNCTION ...")` |
| information_schema | Introspect tables, columns, constraints | `ctx.sql("SELECT * FROM information_schema.tables")` |
| INSERT execution | Write via SQL | `ctx.sql("INSERT INTO target SELECT * FROM source")` |
| COPY execution | Non-Delta writes | `ctx.sql("COPY (SELECT ...) TO 'path' STORED AS PARQUET")` |
| TableProvider path | Custom table implementations | `ctx.register_table("name", provider)` |
| Metadata cache | Listing table performance | `RuntimeEnv.metadata_cache_limit` |
| Statistics collection | Query optimization | `SessionConfig.collect_statistics` |

### Delta Features to Leverage

| Feature | Use Case | API |
|---------|----------|-----|
| CHECK constraints | Write-time validation | `write_deltalake(..., constraints={"check": "amount >= 0"})` |
| CDF (Change Data Feed) | Incremental processing | `delta_cdf_provider(starting_version=N)` |
| with_files | Scan unit pinning | `provider.with_files(file_list)` |
| information_schema | Constraint introspection | `SELECT * FROM information_schema.table_constraints` |
| Deletion vectors | Efficient deletes | `DeltaMaintenancePolicy(enable_deletion_vectors=True)` |
| v2 checkpoints | Log compaction | `DeltaMaintenancePolicy(enable_v2_checkpoints=True)` |

---

## Summary Statistics

| Category | Files Affected | Lines Reduced | Priority |
|----------|---------------|---------------|----------|
| Value Coercion Utilities | 12+ | ~200 | HIGH |
| Session Factory | 3 | ~100 | MEDIUM |
| Policy Fingerprinting | 26+ | ~250 | HIGH |
| Schema Builder Extension | 10+ | ~100 | MEDIUM |
| Telemetry Emission | 1 | ~80 | MEDIUM |
| Validation Violations | 4 | ~120 | MEDIUM |
| Storage Options | 3 | ~50 | LOW |
| Rust UDF Registration | 4 | ~250 | HIGH |
| Rust Config Extension | 3 | ~150 | MEDIUM |
| Python Binding Registration | 1 | ~90 | MEDIUM |
| Rust Error Conversion | 2 | ~40 | LOW |
| Plan-Bundle Execution | 3 | ~100 | MEDIUM |
| RecordBatchReader Coercion | 5 | ~80 | MEDIUM |
| Single UDF Catalog | 2 | ~50 | LOW |
| **TOTAL** | **79+** | **~1,660** | - |
