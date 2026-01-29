# Codebase Consolidation Opportunities

This document identifies consolidation opportunities across the CodeAnatomy codebase, providing code patterns, target file lists, implementation checklists, and decommissioning plans for each scope item.

---

## Table of Contents

1. [Hash/Fingerprint Function Consolidation](#1-hashfingerprint-function-consolidation)
2. [Environment Variable Resolution Consolidation](#2-environment-variable-resolution-consolidation)
3. [Type Alias Consolidation](#3-type-alias-consolidation)
4. [Identical Dataclass Consolidation](#4-identical-dataclass-consolidation)
5. [Registry Pattern Standardization](#5-registry-pattern-standardization)
6. [Configuration Class Standardization](#6-configuration-class-standardization)
7. [Factory Simplification](#7-factory-simplification)
8. [Cross-Scope Dependencies](#8-cross-scope-dependencies)

---

## 1. Hash/Fingerprint Function Consolidation

### Architecture Overview

The codebase contains **46+ hash/fingerprint function implementations** scattered across modules. Most use SHA-256, with a few using BLAKE2b for compact hashes.

### Current State: Duplicated Implementations

| Pattern | Files with Duplicates | Count |
|---------|----------------------|-------|
| `_hash_payload(payload) -> str` (msgpack + SHA-256) | 8 files | 8 |
| `_settings_hash(settings) -> str` | 4 files | 4 |
| `_storage_options_hash(options) -> str` | 4 files | 4 |
| `_payload_hash(payload) -> str` (JSON + SHA-256) | 3 files | 3 |

#### Example: `_hash_payload` implementations

**Location 1:** `src/extract/cache_utils.py:27`
```python
def _hash_payload(payload: object) -> str:
    raw = msgspec.msgpack.encode(payload)
    return hashlib.sha256(raw).hexdigest()
```

**Location 2:** `src/datafusion_engine/execution_helpers.py:73`
```python
def _hash_payload(payload: object) -> str:
    raw = msgspec.msgpack.encode(payload)
    return hashlib.sha256(raw).hexdigest()
```

**Location 3:** `src/datafusion_engine/view_artifacts.py:110`
```python
def _hash_payload(payload: object) -> str:
    raw = msgspec.msgpack.encode(payload)
    return hashlib.sha256(raw).hexdigest()
```

#### Storage Options Hash Duplicates

**Location 1:** `src/storage/deltalake/delta.py:2459`
```python
def _storage_options_hash(
    storage_options: Mapping[str, str] | None,
    log_storage_options: Mapping[str, str] | None,
) -> str | None:
    if storage_options is None and log_storage_options is None:
        return None
    payload = {"storage": dict(storage_options or {}), "log_storage": dict(log_storage_options or {})}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
```

**Location 2:** `src/engine/delta_tools.py:315`
```python
def _storage_options_hash(storage: Mapping[str, str] | None) -> str | None:
    if storage is None:
        return None
    return hashlib.sha256(json.dumps(dict(storage), sort_keys=True).encode()).hexdigest()
```

### Target Implementation: Consolidated Hash Module

Create `src/utils/hash_utils.py`:

```python
"""Unified hash utilities for content, payload, and configuration fingerprinting."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import TYPE_CHECKING

import msgspec.msgpack

if TYPE_CHECKING:
    from pathlib import Path

# -----------------------------------------------------------------------------
# Core Primitives
# -----------------------------------------------------------------------------

def hash64_from_text(value: str) -> int:
    """Return a deterministic signed 64-bit hash for a string (BLAKE2b)."""
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
    unsigned = int.from_bytes(digest, "big", signed=False)
    return unsigned & ((1 << 63) - 1)


def hash128_from_text(value: str) -> str:
    """Return a deterministic 128-bit hex string for a string (BLAKE2b)."""
    return hashlib.blake2b(value.encode("utf-8"), digest_size=16).hexdigest()


# -----------------------------------------------------------------------------
# Payload Hashing (msgpack serialization)
# -----------------------------------------------------------------------------

def hash_payload_msgpack(payload: object) -> str:
    """Return SHA-256 hexdigest of msgpack-encoded payload."""
    raw = msgspec.msgpack.encode(payload)
    return hashlib.sha256(raw).hexdigest()


def hash_payload_json(payload: object) -> str:
    """Return SHA-256 hexdigest of JSON-encoded payload (sorted keys)."""
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# -----------------------------------------------------------------------------
# Settings/Config Hashing
# -----------------------------------------------------------------------------

def hash_settings(settings: Mapping[str, str]) -> str:
    """Return SHA-256 hexdigest of sorted settings mapping."""
    payload = tuple(sorted(settings.items()))
    return hash_payload_msgpack(payload)


def hash_storage_options(
    storage_options: Mapping[str, str] | None,
    log_storage_options: Mapping[str, str] | None = None,
) -> str | None:
    """Return SHA-256 hexdigest of storage options (None if all inputs None)."""
    if storage_options is None and log_storage_options is None:
        return None
    payload = {
        "storage": dict(storage_options or {}),
        "log_storage": dict(log_storage_options or {}),
    }
    return hash_payload_json(payload)


# -----------------------------------------------------------------------------
# File Content Hashing
# -----------------------------------------------------------------------------

def hash_file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Return SHA-256 hexdigest of file contents (chunked reading)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


__all__ = [
    "hash64_from_text",
    "hash128_from_text",
    "hash_payload_msgpack",
    "hash_payload_json",
    "hash_settings",
    "hash_storage_options",
    "hash_file_sha256",
]
```

### Target File List

Files to modify (import from `utils.hash_utils`):

| File | Functions to Replace |
|------|---------------------|
| `src/extract/cache_utils.py` | `_hash_payload` |
| `src/datafusion_engine/execution_helpers.py` | `_hash_payload`, `_hash_substrait` |
| `src/datafusion_engine/view_artifacts.py` | `_hash_payload`, `_df_settings_hash` |
| `src/datafusion_engine/plan_bundle.py` | `_planning_env_hash`, `_rulepack_hash`, `_payload_hash`, `_settings_hash`, `_function_registry_hash`, `_information_schema_hash` |
| `src/datafusion_engine/plan_artifact_store.py` | `_payload_hash_json`, `_payload_hash_bytes` |
| `src/datafusion_engine/scan_planner.py` | `_scan_unit_key`, `_delta_scan_config_hash`, `_storage_options_hash_from_request` |
| `src/datafusion_engine/registry_bridge.py` | `_storage_options_hash` |
| `src/datafusion_engine/udf_runtime.py` | `rust_udf_snapshot_hash` |
| `src/datafusion_engine/param_tables.py` | `column_param_signature`, `scalar_param_signature` |
| `src/datafusion_engine/provider_registry.py` | `_hash_snapshot` |
| `src/storage/deltalake/delta.py` | `_storage_options_hash` |
| `src/storage/ipc_utils.py` | `ipc_hash`, `payload_hash` |
| `src/engine/delta_tools.py` | `_storage_options_hash` |
| `src/cache/diskcache_factory.py` | `_hash_payload` |
| `src/relspec/execution_plan.py` | `_hash_payload`, `_df_settings_hash` |
| `src/serde_artifacts.py` | artifact envelope hash |
| `src/serde_schema_registry.py` | `schema_contract_hash` |
| `src/extract/repo_scan.py` | `_sha256_path` |
| `src/incremental/scip_fingerprint.py` | `scip_index_fingerprint` |

### Implementation Checklist

- [ ] Create `src/utils/__init__.py` if not exists
- [ ] Create `src/utils/hash_utils.py` with consolidated implementations
- [ ] Add comprehensive unit tests in `tests/unit/utils/test_hash_utils.py`
- [ ] Update each target file to import from `utils.hash_utils`
- [ ] Verify all tests pass after each file migration
- [ ] Remove duplicate implementations from target files
- [ ] Update `__all__` exports in affected modules

### Decommissioning List

Delete after migration:
- `src/datafusion_engine/hash_utils.py` (move to `src/utils/hash_utils.py`)
- Remove local `_hash_payload` from 8 files
- Remove local `_storage_options_hash` from 4 files
- Remove local `_settings_hash` from 4 files

---

## 2. Environment Variable Resolution Consolidation

### Architecture Overview

The codebase has **8+ duplicate implementations** of environment variable parsing helpers, particularly for boolean, integer, and float conversion with error handling.

### Current State: Duplicated Implementations

#### `_env_bool` implementations (4 duplicates)

**Location 1:** `src/obs/otel/config.py:46-51`
```python
def _env_bool(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    return value in {"1", "true", "yes", "y"}
```

**Location 2:** `src/hamilton_pipeline/modules/inputs.py:46-55`
```python
def _env_bool(name: str) -> bool | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "y"}:
        return True
    if value in {"0", "false", "no", "n"}:
        return False
    return None
```

**Location 3:** `src/extract/git_remotes.py:213-222`
```python
def _env_bool(name: str) -> bool | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    return None
```

**Location 4:** `src/extract/git_settings.py:65-74`
```python
def _env_bool(name: str) -> bool | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "y"}:
        return True
    if value in {"0", "false", "no", "n"}:
        return False
    return None
```

#### `_env_int` implementations (3 duplicates)

**Location 1:** `src/obs/otel/config.py:54-62`
```python
def _env_int(name: str, *, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        _LOGGER.warning("Invalid integer for %s: %s", name, raw)
        return default
```

**Location 2:** `src/engine/runtime_profile.py:94-99`
**Location 3:** `src/extract/git_settings.py:77-84`

### Target Implementation: Consolidated Env Utils Module

Create `src/utils/env_utils.py`:

```python
"""Unified environment variable resolution utilities."""

from __future__ import annotations

import logging
import os
from typing import TypeVar, overload

_LOGGER = logging.getLogger(__name__)

T = TypeVar("T")

# -----------------------------------------------------------------------------
# String Helpers
# -----------------------------------------------------------------------------

def env_value(name: str) -> str | None:
    """Return stripped env var value, or None if empty/not set."""
    raw = os.environ.get(name)
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped if stripped else None


# -----------------------------------------------------------------------------
# Boolean Parsing
# -----------------------------------------------------------------------------

_TRUE_VALUES = frozenset({"1", "true", "yes", "y"})
_FALSE_VALUES = frozenset({"0", "false", "no", "n"})


@overload
def env_bool(name: str) -> bool | None: ...
@overload
def env_bool(name: str, *, default: bool) -> bool: ...


def env_bool(name: str, *, default: bool | None = None) -> bool | None:
    """Parse environment variable as boolean.

    Parameters
    ----------
    name
        Environment variable name.
    default
        Default value if not set. If None, returns None when unset.

    Returns
    -------
    bool | None
        Parsed boolean or default/None.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    return default


# -----------------------------------------------------------------------------
# Integer Parsing
# -----------------------------------------------------------------------------

@overload
def env_int(name: str) -> int | None: ...
@overload
def env_int(name: str, *, default: int) -> int: ...


def env_int(name: str, *, default: int | None = None) -> int | None:
    """Parse environment variable as integer with error logging.

    Parameters
    ----------
    name
        Environment variable name.
    default
        Default value if not set or invalid.

    Returns
    -------
    int | None
        Parsed integer or default/None.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        _LOGGER.warning("Invalid integer for %s: %r", name, raw)
        return default


# -----------------------------------------------------------------------------
# Float Parsing
# -----------------------------------------------------------------------------

@overload
def env_float(name: str) -> float | None: ...
@overload
def env_float(name: str, *, default: float) -> float: ...


def env_float(name: str, *, default: float | None = None) -> float | None:
    """Parse environment variable as float with error logging.

    Parameters
    ----------
    name
        Environment variable name.
    default
        Default value if not set or invalid.

    Returns
    -------
    float | None
        Parsed float or default/None.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw.strip())
    except ValueError:
        _LOGGER.warning("Invalid float for %s: %r", name, raw)
        return default


__all__ = [
    "env_value",
    "env_bool",
    "env_int",
    "env_float",
]
```

### Target File List

Files to modify (import from `utils.env_utils`):

| File | Functions to Replace |
|------|---------------------|
| `src/obs/otel/config.py` | `_env_bool`, `_env_int`, `_env_float`, `_env_optional_int`, `_exporter_enabled` |
| `src/obs/otel/resources.py` | `_env_value` |
| `src/hamilton_pipeline/driver_factory.py` | `_runtime_profile_name`, `_determinism_override`, `_incremental_enabled`, `_tracker_value` |
| `src/hamilton_pipeline/modules/inputs.py` | `_env_bool`, `_incremental_pipeline_enabled`, `_determinism_from_str`, `_cache_path_from_inputs` |
| `src/engine/runtime_profile.py` | `_env_value`, `_env_int` |
| `src/extract/git_remotes.py` | `_env_bool` |
| `src/extract/git_settings.py` | `_env_bool`, `_env_int` |
| `src/cache/diskcache_factory.py` | `_default_cache_root` (env reading) |

### Implementation Checklist

- [ ] Create `src/utils/env_utils.py` with consolidated implementations
- [ ] Add unit tests in `tests/unit/utils/test_env_utils.py`
- [ ] Update `src/obs/otel/config.py` to import from `utils.env_utils`
- [ ] Update `src/hamilton_pipeline/modules/inputs.py`
- [ ] Update `src/hamilton_pipeline/driver_factory.py`
- [ ] Update `src/engine/runtime_profile.py`
- [ ] Update `src/extract/git_remotes.py`
- [ ] Update `src/extract/git_settings.py`
- [ ] Update `src/cache/diskcache_factory.py`
- [ ] Verify all tests pass after migration

### Decommissioning List

Delete after migration (local functions in each file):
- `src/obs/otel/config.py`: `_env_bool`, `_env_int`, `_env_float`, `_env_optional_int`
- `src/obs/otel/resources.py`: `_env_value`
- `src/hamilton_pipeline/modules/inputs.py`: `_env_bool`
- `src/engine/runtime_profile.py`: `_env_value`, `_env_int`
- `src/extract/git_remotes.py`: `_env_bool`
- `src/extract/git_settings.py`: `_env_bool`, `_env_int`

---

## 3. Type Alias Consolidation

### Architecture Overview

The codebase has **4 critical duplicate type aliases** that need consolidation, particularly `PathLike`, `JsonValue`, `Row`, and `RowValue`.

### Current State: Duplicated Definitions

#### `PathLike` (2 definitions - IDENTICAL)

**Location 1:** `src/core_types.py:10`
```python
type PathLike = str | Path
```

**Location 2:** `src/storage/dataset_sources.py:23`
```python
type PathLike = str | Path
```

#### `JsonValue` (2 definitions - INCOMPATIBLE)

**Location 1:** `src/core_types.py:22` (canonical recursive definition)
```python
type JsonPrimitive = str | int | float | bool | None
type JsonValue = JsonPrimitive | Mapping[str, JsonValue] | Sequence[JsonValue]
```

**Location 2:** `src/serde_artifacts.py:31` (permissive definition)
```python
type JsonValue = Any  # Any is required to model arbitrary JSON payloads
```

#### `Row` (5 definitions - INCOMPATIBLE VARIANTS)

**Variant A (strict):**
```python
# src/obs/metrics.py:36, src/extract/bytecode_extract.py:60
type Row = dict[str, RowValue]
```

**Variant B (permissive):**
```python
# src/extract/tree_sitter_extract.py:57, src/extract/cst_extract.py:64, src/extract/scip_extract.py:64
type Row = dict[str, object]
```

#### `RowValue` (2 definitions - INCOMPATIBLE)

**Location 1:** `src/obs/metrics.py:35`
```python
type RowValue = str | int
```

**Location 2:** `src/extract/bytecode_extract.py:59`
```python
type RowValue = str | int | bool | list[str] | list[dict[str, object]] | None
```

### Target Implementation

Update `src/core_types.py`:

```python
"""Shared type aliases and small typing helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, NewType

# -----------------------------------------------------------------------------
# Path Types
# -----------------------------------------------------------------------------

type PathLike = str | Path

# -----------------------------------------------------------------------------
# JSON Types
# -----------------------------------------------------------------------------

type JsonPrimitive = str | int | float | bool | None
type JsonValue = JsonPrimitive | Mapping[str, JsonValue] | Sequence[JsonValue]
type JsonDict = dict[str, JsonValue]
type JsonValueLax = Any  # For schema exports requiring arbitrary payloads

# -----------------------------------------------------------------------------
# Row Types (for extraction/metrics)
# -----------------------------------------------------------------------------

type RowValueStrict = str | int
type RowValueRich = str | int | bool | list[str] | list[dict[str, object]] | None
type RowStrict = dict[str, RowValueStrict]
type RowRich = dict[str, RowValueRich]
type RowPermissive = dict[str, object]

# Backward compatibility aliases
type Row = RowPermissive  # Default to permissive
type RowValue = RowValueRich  # Default to rich

# -----------------------------------------------------------------------------
# Strictness and Determinism
# -----------------------------------------------------------------------------

type StrictnessMode = Literal["strict", "tolerant"]

RepoId = NewType("RepoId", int)

# Validation patterns
IDENTIFIER_PATTERN = "^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$"
RUN_ID_PATTERN = "^[A-Za-z0-9][A-Za-z0-9_-]{7,63}$"
HASH_PATTERN = "^[A-Fa-f0-9]{32,128}$"
EVENT_KIND_PATTERN = "^[a-z][a-z0-9_]{0,63}$"
STATUS_PATTERN = "^[a-z][a-z0-9_-]{0,31}$"


class DeterminismTier(StrEnum):
    """Determinism budgets for the pipeline."""
    CANONICAL = "canonical"
    STABLE_SET = "stable_set"
    BEST_EFFORT = "best_effort"
    FAST = "best_effort"
    STABLE = "stable_set"


def ensure_path(p: PathLike) -> Path:
    """Return a normalized Path for the provided value."""
    return p if isinstance(p, Path) else Path(p)


__all__ = [
    "PathLike",
    "JsonPrimitive",
    "JsonValue",
    "JsonDict",
    "JsonValueLax",
    "RowValueStrict",
    "RowValueRich",
    "RowStrict",
    "RowRich",
    "RowPermissive",
    "Row",
    "RowValue",
    "StrictnessMode",
    "RepoId",
    "DeterminismTier",
    "ensure_path",
]
```

### Target File List

| File | Change |
|------|--------|
| `src/storage/dataset_sources.py` | Remove local `PathLike`, import from `core_types` |
| `src/serde_artifacts.py` | Change `JsonValue = Any` to import `JsonValueLax` from `core_types` |
| `src/obs/metrics.py` | Import `RowValueStrict`, `RowStrict` from `core_types` |
| `src/extract/bytecode_extract.py` | Import `RowValueRich`, `RowRich` from `core_types` |
| `src/extract/tree_sitter_extract.py` | Import `RowPermissive` from `core_types` |
| `src/extract/cst_extract.py` | Import `RowPermissive` from `core_types` |
| `src/extract/scip_extract.py` | Import `RowPermissive` from `core_types` |

### Implementation Checklist

- [ ] Update `src/core_types.py` with expanded type alias definitions
- [ ] Add type alias documentation
- [ ] Update `src/storage/dataset_sources.py` to import `PathLike`
- [ ] Update `src/serde_artifacts.py` to use `JsonValueLax`
- [ ] Update extraction modules to use appropriate `Row*` aliases
- [ ] Update `src/obs/metrics.py` for strict row types
- [ ] Verify all type checks pass with `pyright`

### Decommissioning List

- Remove `type PathLike = str | Path` from `src/storage/dataset_sources.py:23`
- Remove `type JsonValue = Any` from `src/serde_artifacts.py:31`
- Remove local `Row` and `RowValue` definitions from extraction modules

---

## 4. Identical Dataclass Consolidation

### Architecture Overview

The codebase has **8 dataclasses with duplicate definitions** across different modules, with varying levels of field overlap.

### Current State: Duplicate Definitions

#### High Priority Consolidation

| Dataclass | Locations | Overlap | Action |
|-----------|-----------|---------|--------|
| `ViewReference` | `src/relspec/runtime_artifacts.py:38`, `src/datafusion_engine/nested_tables.py:15` | Low (1 vs 6 fields) | Keep both, rename minimal one |
| `QuerySpec` | `src/datafusion_engine/query_spec.py:23`, `src/extract/tree_sitter_queries.py:12` | None | Keep both (different domains) |
| `PlanCacheEntry` | `src/datafusion_engine/plan_cache.py:18`, `src/engine/plan_cache.py:55` | Low | Consolidate to single location |
| `FunctionCatalog` | `src/datafusion_engine/schema_registry.py:3010`, `src/datafusion_engine/udf_catalog.py:216` | Medium | Consolidate with interface |
| `DeltaVacuumRequest` | `src/datafusion_engine/delta_control_plane.py:189`, `src/engine/delta_tools.py:61` | Low | Keep both (Rust vs Python layers) |
| `DatasetRegistration` | `src/datafusion_engine/registry_bridge.py:732`, `src/schema_spec/registration.py:28` | None | Keep both (different purposes) |
| `ContractRow` | `src/incremental/registry_rows.py:20`, `src/normalize/dataset_rows.py:79` | High | Consolidate to shared module |
| `DatasetRow` | `src/incremental/registry_rows.py:30`, `src/normalize/dataset_rows.py:91` | Medium | Consolidate with context variants |

#### `ContractRow` Consolidation (HIGH OVERLAP)

**Location 1:** `src/incremental/registry_rows.py:20-27`
```python
@dataclass(frozen=True)
class ContractRow:
    dedupe: DedupeSpecSpec | None = None
    canonical_sort: tuple[SortKeySpec, ...] = ()
    constraints: tuple[str, ...] = ()
    version: int | None = None
```

**Location 2:** `src/normalize/dataset_rows.py:79-88`
```python
@dataclass(frozen=True)
class ContractRow:
    dedupe: DedupeSpecSpec | None = None
    canonical_sort: tuple[SortKeySpec, ...] = ()
    version: int | None = None
    virtual_fields: tuple[str, ...] = ()
    virtual_field_docs: dict[str, str] | None = None
    validation: ArrowValidationOptions | None = None
```

**Target:** Create unified `ContractRow` in `src/schema_spec/contract_row.py`:

```python
@dataclass(frozen=True)
class ContractRow:
    """Contract specification for dataset rows."""

    dedupe: DedupeSpecSpec | None = None
    canonical_sort: tuple[SortKeySpec, ...] = ()
    version: int | None = None
    constraints: tuple[str, ...] = ()  # From incremental
    virtual_fields: tuple[str, ...] = ()  # From normalize
    virtual_field_docs: dict[str, str] | None = None  # From normalize
    validation: ArrowValidationOptions | None = None  # From normalize
```

#### `ViewReference` Consolidation

**Minimal version** (`nested_tables.py`):
```python
@dataclass(frozen=True)
class ViewReference:
    """Reference a DataFusion-registered view by name."""
    name: str
```

**Full version** (`runtime_artifacts.py`):
```python
@dataclass(frozen=True)
class ViewReference:
    """Reference to a registered view in the execution context."""
    name: str
    source_task: str
    schema_fingerprint: str | None = None
    plan_fingerprint: str | None = None
    plan_task_signature: str | None = None
    plan_signature: str | None = None
```

**Target:** Rename minimal version to `SimpleViewRef`:

```python
# src/datafusion_engine/nested_tables.py
@dataclass(frozen=True)
class SimpleViewRef:
    """Simple reference to a DataFusion view by name."""
    name: str
```

### Target File List

| Action | File |
|--------|------|
| Create | `src/schema_spec/contract_row.py` |
| Modify | `src/incremental/registry_rows.py` (import from schema_spec) |
| Modify | `src/normalize/dataset_rows.py` (import from schema_spec) |
| Modify | `src/datafusion_engine/nested_tables.py` (rename to `SimpleViewRef`) |

### Implementation Checklist

- [ ] Create `src/schema_spec/contract_row.py` with unified `ContractRow`
- [ ] Update `src/incremental/registry_rows.py` to import `ContractRow`
- [ ] Update `src/normalize/dataset_rows.py` to import `ContractRow`
- [ ] Rename `ViewReference` to `SimpleViewRef` in `src/datafusion_engine/nested_tables.py`
- [ ] Update all usages of minimal `ViewReference` to `SimpleViewRef`
- [ ] Verify no import conflicts exist

### Decommissioning List

- Remove `ContractRow` definition from `src/incremental/registry_rows.py`
- Remove `ContractRow` definition from `src/normalize/dataset_rows.py`

---

## 5. Registry Pattern Standardization

### Architecture Overview

The codebase has **14+ registry implementations** with varying patterns:
- Instance-based registries (dict attributes)
- Module-level constant registries (tuples)
- Singleton caches (global dicts)
- Function-based dispatch tables

### Current Registry Inventory

| Registry | File | Storage | Pattern |
|----------|------|---------|---------|
| `ProviderRegistry` | `provider_registry.py` | Instance dict | Class with methods |
| `ContractRegistry` | `schema_contracts.py` | Instance dict | Dataclass with methods |
| `DataFusionViewRegistry` | `runtime.py` | Instance dict | Dataclass |
| `ParamTableRegistry` | `param_tables.py` | Instance dict | Dataclass |
| `DatasetCatalog` | `dataset_registry.py` | Instance dict | Frozen dataclass |
| `SemanticRegistry` | `semantic_registry.py` | Frozen tuple | Immutable |
| `ENTITY_FAMILY_SPECS` | `spec_registry.py` | Module constant | Static tuple |
| `_DATASET_TEMPLATE_REGISTRY` | `extract_templates.py` | Module constant | Dispatch dict |
| `_REGISTRY_CACHE` | `otel/metrics.py` | Global dict | Singleton cache |
| `DATASET_ROWS` | `registry_rows.py` | Module constant | Static tuple |

### Target Implementation: Registry Protocol

Create `src/utils/registry_protocol.py`:

```python
"""Standard registry protocols and base implementations."""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Generic, Protocol, TypeVar, runtime_checkable

K = TypeVar("K")
V = TypeVar("V")


@runtime_checkable
class Registry(Protocol[K, V]):
    """Protocol for registry implementations."""

    @abstractmethod
    def register(self, key: K, value: V) -> None:
        """Register a value with the given key."""
        ...

    @abstractmethod
    def get(self, key: K) -> V | None:
        """Retrieve a value by key, or None if not found."""
        ...

    @abstractmethod
    def __contains__(self, key: K) -> bool:
        """Check if key is registered."""
        ...

    @abstractmethod
    def __iter__(self) -> Iterator[K]:
        """Iterate over registered keys."""
        ...

    @abstractmethod
    def __len__(self) -> int:
        """Return count of registered items."""
        ...


@dataclass
class MutableRegistry(Generic[K, V]):
    """Standard mutable registry with dict storage."""

    _entries: dict[K, V] = field(default_factory=dict)

    def register(self, key: K, value: V, *, overwrite: bool = False) -> None:
        if key in self._entries and not overwrite:
            msg = f"Key {key!r} already registered. Use overwrite=True."
            raise ValueError(msg)
        self._entries[key] = value

    def get(self, key: K) -> V | None:
        return self._entries.get(key)

    def __contains__(self, key: K) -> bool:
        return key in self._entries

    def __iter__(self) -> Iterator[K]:
        return iter(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def items(self) -> Iterator[tuple[K, V]]:
        yield from self._entries.items()

    def snapshot(self) -> Mapping[K, V]:
        """Return immutable snapshot of current state."""
        return dict(self._entries)


@dataclass(frozen=True)
class ImmutableRegistry(Generic[K, V]):
    """Frozen registry built from a sequence of entries."""

    _entries: tuple[tuple[K, V], ...]

    def get(self, key: K) -> V | None:
        for k, v in self._entries:
            if k == key:
                return v
        return None

    def __contains__(self, key: K) -> bool:
        return any(k == key for k, _ in self._entries)

    def __iter__(self) -> Iterator[K]:
        return (k for k, _ in self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    @classmethod
    def from_dict(cls, d: Mapping[K, V]) -> ImmutableRegistry[K, V]:
        return cls(tuple(d.items()))


__all__ = [
    "Registry",
    "MutableRegistry",
    "ImmutableRegistry",
]
```

### Target File List

Registries to refactor to use standard protocol:

| File | Registry | Migration |
|------|----------|-----------|
| `src/datafusion_engine/provider_registry.py` | `ProviderRegistry` | Inherit from `MutableRegistry` |
| `src/datafusion_engine/schema_contracts.py` | `ContractRegistry` | Inherit from `MutableRegistry` |
| `src/datafusion_engine/runtime.py` | `DataFusionViewRegistry` | Inherit from `MutableRegistry` |
| `src/datafusion_engine/param_tables.py` | `ParamTableRegistry` | Inherit from `MutableRegistry` |
| `src/datafusion_engine/dataset_registry.py` | `DatasetCatalog` | Inherit from `MutableRegistry` |

### Implementation Checklist

- [ ] Create `src/utils/registry_protocol.py`
- [ ] Add comprehensive tests for registry protocol
- [ ] Migrate `ProviderRegistry` to use `MutableRegistry` base
- [ ] Migrate `ContractRegistry` to use `MutableRegistry` base
- [ ] Migrate `DataFusionViewRegistry` to use `MutableRegistry` base
- [ ] Migrate `ParamTableRegistry` to use `MutableRegistry` base
- [ ] Migrate `DatasetCatalog` to use `MutableRegistry` base
- [ ] Document registry usage patterns

### Decommissioning List

After migration, remove redundant implementations:
- Remove custom `__contains__` and `__iter__` from migrated registries
- Remove duplicate snapshot logic where `MutableRegistry.snapshot()` suffices

---

## 6. Configuration Class Standardization

### Architecture Overview

The codebase has **50+ configuration classes** using two primary patterns:
1. `@dataclass(frozen=True)` - most common
2. `msgspec.Struct` subclasses - for persistence contracts

### Current Inconsistencies

| Issue | Examples | Count |
|-------|----------|-------|
| Inconsistent naming (Policy vs Settings vs Config) | `DeltaWritePolicy`, `DiskCacheSettings`, `OtelConfig` | 50+ |
| Duplicate factory patterns | `resolve_otel_config()`, `git_settings_from_env()` | 10+ |
| Mixed validation approaches | msgspec Meta vs post-init vs external | 15+ |

### Target Implementation: Configuration Base Classes

Create `src/utils/config_base.py`:

```python
"""Base configuration classes with standardized patterns."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, fields
from typing import Self, TypeVar

import msgspec

T = TypeVar("T")


@dataclass(frozen=True)
class ConfigBase:
    """Base class for configuration dataclasses.

    Provides standardized fingerprinting and serialization.
    """

    def fingerprint(self) -> str:
        """Return stable SHA-256 fingerprint of configuration."""
        payload = {f.name: getattr(self, f.name) for f in fields(self)}
        raw = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def to_dict(self) -> dict[str, object]:
        """Return dictionary representation."""
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def field_names(cls) -> tuple[str, ...]:
        """Return tuple of field names."""
        return tuple(f.name for f in fields(cls))


class StructBaseStrict(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    omit_defaults=True,
):
    """Strict msgspec base for configuration contracts.

    Use for configurations that must reject unknown fields.
    """
    pass


class StructBaseCompat(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=False,
    omit_defaults=True,
):
    """Compatible msgspec base for forward-compatible configurations.

    Use for persisted artifacts that may evolve over time.
    """
    pass


__all__ = [
    "ConfigBase",
    "StructBaseStrict",
    "StructBaseCompat",
]
```

### Naming Convention

Establish and document naming convention:

| Suffix | Purpose | Example |
|--------|---------|---------|
| `Policy` | Runtime behavior control | `DeltaWritePolicy`, `SqlPolicy` |
| `Settings` | Initialization parameters | `DiskCacheSettings`, `GitSettings` |
| `Config` | Request/command parameters | `OtelConfig`, `ExecutorConfig` |
| `Spec` | Declarative schema definitions | `TableSpec`, `QuerySpec` |
| `Options` | Optional parameter bundles | `DmlOptions`, `CompileOptions` |

### Target File List

High-priority classes to standardize:

| File | Class | Action |
|------|-------|--------|
| `src/obs/otel/config.py` | `OtelConfig` | Inherit from `ConfigBase` |
| `src/cache/diskcache_factory.py` | `DiskCacheSettings` | Inherit from `ConfigBase` |
| `src/cache/diskcache_factory.py` | `DiskCacheProfile` | Inherit from `ConfigBase` |
| `src/engine/runtime_profile.py` | `HamiltonTrackerConfig` | Inherit from `ConfigBase` |
| `src/engine/runtime_profile.py` | `RuntimeProfileSpec` | Inherit from `ConfigBase` |
| `src/incremental/types.py` | `IncrementalSettings` | Inherit from `ConfigBase` |

### Implementation Checklist

- [ ] Create `src/utils/config_base.py`
- [ ] Add tests for `ConfigBase` fingerprinting
- [ ] Document naming conventions in CLAUDE.md or CONTRIBUTING.md
- [ ] Migrate `OtelConfig` to inherit from `ConfigBase`
- [ ] Migrate `DiskCacheSettings` and `DiskCacheProfile`
- [ ] Migrate `HamiltonTrackerConfig` and `RuntimeProfileSpec`
- [ ] Migrate `IncrementalSettings`
- [ ] Audit remaining config classes for consistency

### Decommissioning List

Remove duplicate implementations:
- Custom `fingerprint()` methods that duplicate `ConfigBase.fingerprint()`
- Custom `to_dict()` methods that duplicate `ConfigBase.to_dict()`

---

## 7. Factory Simplification

### Architecture Overview

The codebase has complex nested factory chains, particularly in `driver_factory.py` which has a **9-level deep** factory chain for building Hamilton drivers.

### Current State: Nested Factory Chains

#### Driver Factory Chain (9 levels)

```
build_driver()
├── _view_graph_context()
│   ├── resolve_runtime_profile()
│   ├── profile.session_runtime()
│   ├── ensure_view_graph()
│   └── view_graph_nodes()
├── _compile_plan()
├── _plan_with_incremental_pruning()
├── build_execution_plan_module()
├── build_task_execution_module()
└── driver.Builder()
    ├── _apply_dynamic_execution()
    │   └── _executor_from_kind()
    ├── _apply_graph_adapter()
    │   └── _graph_adapter_from_config()
    ├── _apply_cache()
    ├── _apply_materializers()
    └── _apply_adapters()
```

### Target Implementation: Factory Composition

Create intermediate result types to break up the chain:

```python
# src/hamilton_pipeline/driver_context.py
"""Intermediate context objects for driver building."""

from __future__ import annotations

from dataclasses import dataclass
from types import ModuleType

from hamilton import driver


@dataclass(frozen=True)
class ViewGraphContext:
    """Context from view graph initialization."""
    profile: DataFusionRuntimeProfile
    session_runtime: SessionRuntime
    determinism_tier: DeterminismTier
    snapshot: Mapping[str, object]
    view_nodes: tuple[ViewNode, ...]
    runtime_profile_spec: RuntimeProfileSpec


@dataclass(frozen=True)
class PlanContext:
    """Context from plan compilation."""
    view_ctx: ViewGraphContext
    execution_plan: ExecutionPlan
    modules: tuple[ModuleType, ...]


@dataclass(frozen=True)
class BuilderContext:
    """Context for builder configuration."""
    plan_ctx: PlanContext
    builder: driver.Builder
    diagnostics: DiagnosticsCollector | None


# Factory functions with clear single responsibility
def build_view_graph_context(config: Mapping[str, JsonValue]) -> ViewGraphContext:
    """Build view graph context from configuration."""
    ...


def build_plan_context(view_ctx: ViewGraphContext, config: Mapping[str, JsonValue]) -> PlanContext:
    """Build plan context from view graph context."""
    ...


def build_builder_context(plan_ctx: PlanContext, config: Mapping[str, JsonValue]) -> BuilderContext:
    """Build builder context from plan context."""
    ...


def finalize_driver(builder_ctx: BuilderContext) -> driver.Driver:
    """Finalize driver from builder context."""
    ...
```

### Simplified Chain

```
build_driver()
├── build_view_graph_context()  # Returns ViewGraphContext
├── build_plan_context()        # Returns PlanContext
├── build_builder_context()     # Returns BuilderContext
└── finalize_driver()           # Returns Driver
```

### Target File List

| File | Action |
|------|--------|
| `src/hamilton_pipeline/driver_factory.py` | Refactor to use context objects |
| Create | `src/hamilton_pipeline/driver_context.py` |

### Implementation Checklist

- [ ] Create `src/hamilton_pipeline/driver_context.py` with context dataclasses
- [ ] Extract `build_view_graph_context()` from `_view_graph_context()`
- [ ] Extract `build_plan_context()` from inline logic
- [ ] Extract `build_builder_context()` to consolidate `_apply_*` functions
- [ ] Refactor `build_driver()` to use new factory functions
- [ ] Maintain backward compatibility for existing callers
- [ ] Add integration tests for factory chain

### Decommissioning List

After refactoring, simplify:
- Inline helper functions that become redundant
- Remove intermediate state passing through kwargs

---

## 8. Cross-Scope Dependencies

This section documents which decommissioning items depend on multiple scope completions.

### Dependency Matrix

| Decommission Target | Required Scopes | Notes |
|--------------------|-----------------|-------|
| `src/datafusion_engine/hash_utils.py` | #1 (Hash) | Move to `src/utils/hash_utils.py` |
| Local `_env_bool` (8 files) | #2 (Env Vars) | All must migrate together |
| `PathLike` in `dataset_sources.py` | #3 (Type Aliases) | Simple import change |
| `ContractRow` duplicates | #4 (Dataclasses) | Create shared module first |
| Custom registry methods | #5 (Registries), #1 (Hash) | Registries may use custom hashing |
| Custom fingerprint methods | #6 (Config), #1 (Hash) | Config fingerprinting depends on hash utils |
| Factory helper functions | #7 (Factory), #6 (Config) | Factories create configs |

### Recommended Implementation Order

1. **Phase 1: Foundation** (no dependencies)
   - Scope #1: Hash/Fingerprint Consolidation
   - Scope #2: Environment Variable Consolidation
   - Scope #3: Type Alias Consolidation

2. **Phase 2: Data Structures** (depends on Phase 1)
   - Scope #4: Dataclass Consolidation (may use hash utils)
   - Scope #5: Registry Pattern Standardization (uses hash utils)

3. **Phase 3: Configuration** (depends on Phase 1, 2)
   - Scope #6: Configuration Class Standardization (uses hash utils, may use registries)

4. **Phase 4: Orchestration** (depends on all)
   - Scope #7: Factory Simplification (uses configs, registries)

### Verification Steps

After completing all scopes:

- [ ] Run full test suite: `uv run pytest tests/`
- [ ] Run type checking: `uv run pyright --warnings`
- [ ] Run linting: `uv run ruff check`
- [ ] Verify no circular imports
- [ ] Verify all file paths referenced exist
- [ ] Update CLAUDE.md with new utility module documentation

---

## Appendix: File Path Verification

All file paths referenced in this document have been verified to exist in the codebase:

**Existing Files:**
- `src/core_types.py`
- `src/datafusion_engine/hash_utils.py`
- `src/obs/otel/config.py`
- `src/extract/git_remotes.py`
- `src/extract/git_settings.py`
- `src/storage/dataset_sources.py`
- `src/datafusion_engine/provider_registry.py`
- `src/hamilton_pipeline/driver_factory.py`
- `src/cache/diskcache_factory.py`
- `src/incremental/registry_rows.py`
- `src/normalize/dataset_rows.py`

**New Files to Create:**
- `src/utils/__init__.py`
- `src/utils/hash_utils.py`
- `src/utils/env_utils.py`
- `src/utils/registry_protocol.py`
- `src/utils/config_base.py`
- `src/schema_spec/contract_row.py`
- `src/hamilton_pipeline/driver_context.py`
