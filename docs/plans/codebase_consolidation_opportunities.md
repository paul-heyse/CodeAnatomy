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
9. [Additional Consolidation Opportunities](#9-additional-consolidation-opportunities)

---

## 1. Hash/Fingerprint Function Consolidation

### Architecture Overview

The codebase contains **46+ hash/fingerprint function implementations** scattered across modules. Most use SHA-256, with a few using BLAKE2b for compact hashes. **These implementations are not equivalent**: they differ in serialization format (msgpack vs JSON), encoder configuration (msgspec default vs canonical encoder), and hash truncation (full hex vs first 16 chars). Consolidation must preserve these semantics to avoid breaking cache keys and artifact identities.

### Current State: Duplicated Implementations (Semantics Differ)

| Pattern | Files with Duplicates | Count |
|---------|----------------------|-------|
| `_hash_payload(payload) -> str` (msgpack + SHA-256) | 6+ files | 6+ |
| `_payload_hash(payload) -> str` (JSON + SHA-256 via msgspec encode_json_into) | 2 files | 2 |
| `_storage_options_hash(...) -> str` (JSON + SHA-256) | 5 files | 5 |
| `_settings_hash(settings) -> str` (msgpack + SHA-256) | 2 files | 2 |
| Short hash variants (SHA-256 hex truncated to 16) | 2 files | 2 |

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

#### Short Hash Variants (truncated SHA-256)

**Location 1:** `src/datafusion_engine/scan_overrides.py:330`
```python
def _hash_payload(payload: object) -> str:
    digest = hashlib.sha256(dumps_msgpack(payload)).hexdigest()
    return digest[:16]
```

**Location 2:** `src/datafusion_engine/scan_planner.py:90`
```python
digest = hashlib.sha256(dumps_msgpack(payload)).hexdigest()[:16]
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

### Target Implementation: Explicit Hash Semantics Module

**Key change**: consolidate into explicit helpers that **preserve current semantics**, rather than a single “hash_payload” that might alter encoding or output length. Keep `src/datafusion_engine/hash_utils.py` in place (re-export from the new module) to avoid breaking imports during migration.

Create `src/utils/hashing.py` and re-export (or wrap) from `src/datafusion_engine/hash_utils.py`:

```python
"""Explicit hash utilities with stable serialization semantics."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import TYPE_CHECKING

import msgspec

from serde_msgspec import JSON_ENCODER_SORTED, MSGPACK_ENCODER, to_builtins

if TYPE_CHECKING:
    from pathlib import Path

# -----------------------------------------------------------------------------
# Core primitives
# -----------------------------------------------------------------------------

def hash_sha256_hex(payload: bytes, *, length: int | None = None) -> str:
    """Return SHA-256 hex digest, optionally truncated."""
    digest = hashlib.sha256(payload).hexdigest()
    return digest if length is None else digest[:length]


# -----------------------------------------------------------------------------
# Payload hashing (msgpack)
# -----------------------------------------------------------------------------

def hash_msgpack_default(payload: object) -> str:
    """Return SHA-256 hexdigest using msgspec.msgpack.encode."""
    return hash_sha256_hex(msgspec.msgpack.encode(payload))


def hash_msgpack_canonical(payload: object) -> str:
    """Return SHA-256 hexdigest using MSGPACK_ENCODER (deterministic order)."""
    return hash_sha256_hex(MSGPACK_ENCODER.encode(payload))


# -----------------------------------------------------------------------------
# Payload hashing (JSON via msgspec encoders)
# -----------------------------------------------------------------------------

def hash_json_canonical(payload: object, *, str_keys: bool = False) -> str:
    """Return SHA-256 hexdigest using JSON_ENCODER_SORTED."""
    buffer = bytearray()
    JSON_ENCODER_SORTED.encode_into(to_builtins(payload, str_keys=str_keys), buffer)
    return hash_sha256_hex(buffer)


# -----------------------------------------------------------------------------
# Settings/Config hashing
# -----------------------------------------------------------------------------

def hash_settings(settings: Mapping[str, str]) -> str:
    """Return SHA-256 hexdigest of sorted settings mapping."""
    payload = tuple(sorted(settings.items()))
    return hash_msgpack_canonical(payload)


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
    return hash_json_canonical(payload, str_keys=True)


# -----------------------------------------------------------------------------
# File content hashing
# -----------------------------------------------------------------------------

def hash_file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Return SHA-256 hexdigest of file contents (chunked reading)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


__all__ = [
    "hash_sha256_hex",
    "hash_msgpack_default",
    "hash_msgpack_canonical",
    "hash_json_canonical",
    "hash_settings",
    "hash_storage_options",
    "hash_file_sha256",
]
```

Retain `hash64_from_text` and `hash128_from_text` in `src/datafusion_engine/hash_utils.py` (or re-export them from `src/utils/hashing.py`) to avoid breaking existing callers.

### Target File List

Files to modify (import from `utils.hashing` and preserve existing semantics):

| File | Functions to Replace |
|------|---------------------|
| `src/extract/cache_utils.py` | `_hash_payload` |
| `src/datafusion_engine/execution_helpers.py` | `_hash_payload`, `_hash_substrait` |
| `src/datafusion_engine/view_artifacts.py` | `_hash_payload`, `_df_settings_hash` |
| `src/datafusion_engine/plan_bundle.py` | `_planning_env_hash`, `_rulepack_hash`, `_payload_hash`, `_settings_hash`, `_function_registry_hash`, `_information_schema_hash` |
| `src/datafusion_engine/plan_artifact_store.py` | `_payload_hash_json`, `_payload_hash_bytes` |
| `src/datafusion_engine/scan_planner.py` | `_scan_unit_key`, `_delta_scan_config_hash`, `_storage_options_hash_from_request` |
| `src/datafusion_engine/scan_overrides.py` | `_hash_payload` (truncated) |
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
| `src/datafusion_engine/write_pipeline.py` | `_storage_options_hash` |
| `src/datafusion_engine/table_spec.py` | truncated SHA-256 fingerprints |
| `src/datafusion_engine/planning_pipeline.py` | truncated SHA-256 digests |

### Implementation Checklist

- [ ] Create `src/utils/__init__.py` if not exists
- [ ] Create `src/utils/hashing.py` with **explicit semantic helpers**
- [ ] Re-export or wrap new helpers from `src/datafusion_engine/hash_utils.py`
- [ ] Add comprehensive unit tests in `tests/unit/utils/test_hashing.py`
- [ ] Add **compatibility tests** that compare legacy hashes to new helpers per call-site
- [ ] Update each target file to import the correct helper for its semantics
- [ ] Verify all tests pass after each file migration
- [ ] Remove duplicate implementations from target files
- [ ] Update `__all__` exports in affected modules

### Decommissioning List

Delete after migration:
- **Do not delete** `src/datafusion_engine/hash_utils.py` immediately; convert to a thin re-export wrapper
- Remove local `_hash_payload` helpers from target files
- Remove local `_storage_options_hash` helpers from target files
- Remove local `_settings_hash` helpers from target files

---

## 2. Environment Variable Resolution Consolidation

### Architecture Overview

The codebase has **8+ duplicate implementations** of environment variable parsing helpers, particularly for boolean, integer, and float conversion with error handling. **Semantics diverge today**: some helpers return `None` for invalid booleans, while others fall back to `False` or a default without distinguishing invalid input. Consolidation must preserve these behaviors on a per-call-site basis.

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

### Target Implementation: Consolidated Env Utils Module (Semantics-Preserving)

Create `src/utils/env_utils.py`:

```python
"""Unified environment variable resolution utilities."""

from __future__ import annotations

import logging
import os
from typing import Literal, overload

_LOGGER = logging.getLogger(__name__)

OnInvalid = Literal["default", "none", "false"]

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


def env_bool(
    name: str,
    *,
    default: bool | None = None,
    on_invalid: OnInvalid = "default",
    log_invalid: bool = False,
) -> bool | None:
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
    if log_invalid:
        _LOGGER.warning("Invalid boolean for %s: %r", name, raw)
    if on_invalid == "none":
        return None
    if on_invalid == "false":
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


def env_truthy(value: str | None) -> bool:
    """Return True when the raw env value is 'true' (case-insensitive)."""
    return value is not None and value.strip().lower() == "true"


__all__ = [
    "env_value",
    "env_bool",
    "env_int",
    "env_float",
    "env_truthy",
]
```

### Target File List

Files to modify (import from `utils.env_utils`):

| File | Functions to Replace |
|------|---------------------|
| `src/obs/otel/config.py` | `_env_bool`, `_env_int`, `_env_float`, `_env_optional_int`, `_exporter_enabled` |
| `src/obs/otel/resources.py` | `_env_value` |
| `src/obs/otel/bootstrap.py` | env string resolution |
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
- [ ] Add coverage for invalid/ambiguous env values to preserve legacy behavior

### Decommissioning List

Delete after migration (local functions in each file):
- `src/obs/otel/config.py`: `_env_bool`, `_env_int`, `_env_float`, `_env_optional_int`, `_env_truthy`
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

# Legacy aliases (avoid in new code; keep for compatibility)
type Row = RowPermissive
type RowValue = RowValueRich

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
- [ ] Update extraction modules to use appropriate `Row*` aliases (avoid `Row`/`RowValue` defaults)
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
| `DeltaVacuumRequest` | `src/datafusion_engine/delta_control_plane.py:189`, `src/engine/delta_tools.py:61` | Low | Keep both (Rust vs Python layers) |
| `DatasetRegistration` | `src/datafusion_engine/registry_bridge.py:732`, `src/schema_spec/registration.py:28` | None | Keep both (different purposes) |
| `ContractRow` | `src/incremental/registry_rows.py:20`, `src/normalize/dataset_rows.py:79` | High | Consolidate to shared module |
| `DatasetRow` | `src/incremental/registry_rows.py:30`, `src/normalize/dataset_rows.py:91` | Medium | Defer until contract row consolidation stabilizes |

#### Non-candidates (Do Not Consolidate)

| Dataclass | Locations | Rationale |
|-----------|-----------|-----------|
| `PlanCacheEntry` | `src/datafusion_engine/plan_cache.py:18`, `src/engine/plan_cache.py:55` | Different semantics/fields; not a safe unification |
| `FunctionCatalog` | `src/datafusion_engine/schema_registry.py:3010`, `src/datafusion_engine/udf_catalog.py:216` | Different data model and usage patterns |

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
| Modify | `src/incremental/registry_builders.py` (import from schema_spec) |
| Modify | `src/normalize/dataset_rows.py` (import from schema_spec) |
| Modify | `src/normalize/dataset_builders.py` (import from schema_spec) |
| Modify | `src/datafusion_engine/nested_tables.py` (rename to `SimpleViewRef`) |
| Modify | `src/hamilton_pipeline/pipeline_types.py` (update `ViewReference` imports/usage) |

### Implementation Checklist

- [ ] Create `src/schema_spec/contract_row.py` with unified `ContractRow`
- [ ] Update `src/incremental/registry_rows.py` to import `ContractRow`
- [ ] Update `src/incremental/registry_builders.py` to import `ContractRow`
- [ ] Update `src/normalize/dataset_rows.py` to import `ContractRow`
- [ ] Update `src/normalize/dataset_builders.py` to import `ContractRow`
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

### Target Implementation: Registry Protocol (Type-First, Limited Inheritance)

Create `src/utils/registry_protocol.py` with a protocol and optional base classes. **Do not force inheritance** for registries with richer behavior (e.g., `ProviderRegistry`, `ParamTableRegistry`) unless their APIs are explicitly refactored.

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

Registries to align with the protocol (typing first, inheritance optional):

| File | Registry | Migration |
|------|----------|-----------|
| `src/datafusion_engine/schema_contracts.py` | `ContractRegistry` | Add `Registry` typing; consider `MutableRegistry` only if behavior matches |
| `src/datafusion_engine/dataset_registry.py` | `DatasetCatalog` | Evaluate compatibility; migrate only if minimal |
| `src/datafusion_engine/runtime.py` | `DataFusionViewRegistry` | Evaluate compatibility; avoid forcing base class |
| `src/datafusion_engine/provider_registry.py` | `ProviderRegistry` | **Do not inherit** without redesign |
| `src/datafusion_engine/param_tables.py` | `ParamTableRegistry` | **Do not inherit** without redesign |

### Implementation Checklist

- [ ] Create `src/utils/registry_protocol.py`
- [ ] Add comprehensive tests for registry protocol
- [ ] Add protocol typing to existing registries where appropriate
- [ ] Migrate only registries that are **pure key/value stores**
- [ ] Document registry usage patterns and criteria for inheritance

### Decommissioning List

After selective migration, remove redundant implementations only where a base class is adopted:
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

### Target Implementation: Configuration Utilities (No New Base Class)

`src/serde_msgspec.py` already provides `StructBaseStrict`, `StructBaseCompat`, and `StructBaseHotPath`. Creating a new `config_base.py` would duplicate these and introduce conflicting “canonical” bases.

Instead, add a small helper for **stable fingerprints** that uses the existing hash utilities and does not assume JSON-serializability (e.g., `OtelConfig.sampler` cannot be serialized safely by default).

Add to `src/utils/hashing.py` (or create a small `src/utils/config_utils.py` that wraps it):

```python
from __future__ import annotations

from collections.abc import Mapping

from utils.hashing import hash_json_canonical


def config_fingerprint(payload: Mapping[str, object]) -> str:
    """Return a stable fingerprint for configuration payloads."""
    return hash_json_canonical(payload, str_keys=True)
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

Only migrate configurations that have **simple, JSON-compatible payloads**. Keep custom fingerprinting for complex objects.

| File | Class | Action |
|------|-------|--------|
| `src/obs/otel/config.py` | `OtelConfig` | **Do not migrate** (contains `Sampler` objects) |
| `src/cache/diskcache_factory.py` | `DiskCacheSettings`, `DiskCacheProfile` | Keep custom fingerprints |
| `src/engine/runtime_profile.py` | `HamiltonTrackerConfig`, `RuntimeProfileSpec` | Keep current logic |
| `src/incremental/types.py` | `IncrementalSettings` | Optional: use `config_fingerprint` if needed |

### Implementation Checklist

- [ ] Add `config_fingerprint` helper (in `src/utils/hashing.py` or `src/utils/config_utils.py`)
- [ ] Add tests for config fingerprinting behavior
- [ ] Document naming conventions in CLAUDE.md or CONTRIBUTING.md
- [ ] Audit config classes for JSON-compatibility before applying shared helpers

### Decommissioning List

Remove duplicate implementations **only when behavior is identical**:
- Avoid removing custom `fingerprint()` when semantics differ or payloads are non-JSON

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

### Target Implementation: Incremental Refactor (Context Exists)

`ViewGraphContext` already exists in `src/hamilton_pipeline/driver_factory.py`, so avoid creating a new `driver_context.py`. Instead, focus on incremental extraction of builder configuration and cache/adapters logic into smaller, testable helpers **inside the same module**.

Suggested extraction targets (only if it improves testability):
- `_apply_*` builder steps (`_apply_dynamic_execution`, `_apply_graph_adapter`, etc.)
- Config payload normalization for cache key computation

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
| `src/hamilton_pipeline/driver_factory.py` | Extract helper functions; keep context types local |

### Implementation Checklist

- [ ] Extract builder configuration helpers within `src/hamilton_pipeline/driver_factory.py`
- [ ] Keep `ViewGraphContext` in place; avoid new module unless reuse demands it
- [ ] Refactor `build_driver()` only if readability or testability improves
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
| `src/datafusion_engine/hash_utils.py` | #1 (Hash) | Keep as re-export wrapper until migration completes |
| Local `_env_bool` (8 files) | #2 (Env Vars) | All must migrate together |
| `PathLike` in `dataset_sources.py` | #3 (Type Aliases) | Simple import change |
| `ContractRow` duplicates | #4 (Dataclasses) | Create shared module first |
| Custom registry methods | #5 (Registries) | Only when base class adoption occurs |
| Config fingerprint helper | #6 (Config), #1 (Hash) | Depends on hashing utilities |
| Factory helper functions | #7 (Factory) | Independent of config changes |

### Recommended Implementation Order

1. **Phase 1: Foundation** (no dependencies)
   - Scope #1: Hash/Fingerprint Consolidation
   - Scope #2: Environment Variable Consolidation
   - Scope #3: Type Alias Consolidation

2. **Phase 2: Data Structures** (depends on Phase 1)
   - Scope #4: Dataclass Consolidation (may use hash utils)
   - Scope #5: Registry Pattern Standardization (uses hash utils)

3. **Phase 3: Configuration** (depends on Phase 1, 2)
   - Scope #6: Configuration Utilities (uses hash utils)

4. **Phase 4: Orchestration** (depends on all)
   - Scope #7: Factory Simplification (no hard dependency on config changes)

5. **Phase 5: Additional Opportunities**
   - Scope #9: Additional Consolidations (after Phase 1/2 to preserve hash semantics)

### Verification Steps

After completing all scopes:

- [ ] Run full test suite: `uv run pytest tests/`
- [ ] Run type checking: `uv run pyright --warnings`
- [ ] Run linting: `uv run ruff check`
- [ ] Verify no circular imports
- [ ] Verify all file paths referenced exist
- [ ] Update CLAUDE.md with new utility module documentation

---

## 9. Additional Consolidation Opportunities

This section captures **additional consolidation candidates** identified during review that are not covered in the original plan. These should be scheduled **after Phase 1** to ensure hash semantics and payload formats are stable.

### 9.1 Delta Feature Gate Payload Normalization

Multiple modules serialize Delta feature gates using near-identical helpers:
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/execution_helpers.py`
- `src/datafusion_engine/scan_overrides.py`
- `src/relspec/execution_plan.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/delta_control_plane.py`
- `src/storage/deltalake/delta.py`

**Target:** introduce a single helper (e.g., `delta_feature_gate_payload`) in a shared module (likely `src/datafusion_engine/delta_protocol.py` or a small `src/utils/delta_utils.py`) and reuse it across these call sites.

### 9.2 Storage Options Normalization + Hashing

Storage options normalization and hashing are duplicated with slight differences in:
- `src/datafusion_engine/scan_planner.py`
- `src/datafusion_engine/write_pipeline.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/storage/deltalake/delta.py`
- `src/engine/delta_tools.py`

**Target:** standardize `normalize_storage_options()` and reuse `hash_storage_options()` from `src/utils/hashing.py` to avoid divergent hashes for identical inputs.

### 9.3 Determinism Tier Parsing

Determinism tier parsing logic is duplicated in:
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/driver_factory.py`

**Target:** move parsing to a shared helper (e.g., `core_types.parse_determinism_tier()`), and use it in both modules.

### 9.4 JSON Payload Hashing for Plan Identity

JSON hashing logic in:
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`

is nearly identical (msgspec JSON encoding + SHA-256). Consolidate into a single helper (likely in `src/utils/hashing.py`) without changing serialization behavior.

---

## Appendix: File Path Verification

All file paths referenced in this document have been verified to exist in the codebase:

**Existing Files:**
- `src/core_types.py`
- `src/datafusion_engine/hash_utils.py`
- `src/obs/otel/config.py`
- `src/obs/otel/bootstrap.py`
- `src/extract/git_remotes.py`
- `src/extract/git_settings.py`
- `src/storage/dataset_sources.py`
- `src/datafusion_engine/provider_registry.py`
- `src/hamilton_pipeline/driver_factory.py`
- `src/hamilton_pipeline/pipeline_types.py`
- `src/cache/diskcache_factory.py`
- `src/incremental/registry_builders.py`
- `src/incremental/registry_rows.py`
- `src/normalize/dataset_builders.py`
- `src/normalize/dataset_rows.py`
- `src/datafusion_engine/write_pipeline.py`
- `src/datafusion_engine/table_spec.py`
- `src/datafusion_engine/planning_pipeline.py`

**New Files to Create:**
- `src/utils/__init__.py`
- `src/utils/hashing.py`
- `src/utils/env_utils.py`
- `src/utils/registry_protocol.py`
- `src/schema_spec/contract_row.py`
  (Optional) `src/utils/config_utils.py` if not embedding config helpers in `src/utils/hashing.py`
