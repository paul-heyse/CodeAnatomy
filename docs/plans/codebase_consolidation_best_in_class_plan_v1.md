# Codebase Consolidation Best-in-Class Plan (v1)

This document is the **new canonical consolidation plan**. It integrates the original consolidation scope with updated recommendations, removes low-value or incorrect items, and targets a **best-in-class end state** even where breaking changes are required.

## Goals

- Establish a small set of canonical utilities for hashing, environment parsing, validation, Arrow coercion, and DataFusion session hygiene.
- Collapse duplicated patterns into stable, typed, testable modules.
- Normalize type aliases, config naming, and extraction options for clarity and reuse.
- Remove legacy helpers only after all scopes migrate to the new architecture.

## Guiding Principles

- **Breaking changes are allowed** to reach a clean, best-in-class target state.
- Prefer **domain-specific utilities** (e.g., Arrow helpers live in `datafusion_engine/arrow_schema/`).
- All helpers must preserve or intentionally upgrade semantics (explicitly documented).
- All new modules must be **fully typed**, **NumPy docstring compliant**, and pass `ruff`, `pyright`, and `pyrefly`.

---

## Scope S1: Hashing + Fingerprint Semantics (Canonical)

**Target State:** One explicit, semantics-preserving hashing API. Eliminate local `_hash_*` helpers and variants across the codebase.

**Representative pattern** (canonical helper API):

```python
from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from utils.hashing import (
    hash_file_sha256,
    hash_json_canonical,
    hash_msgpack_canonical,
    hash_msgpack_default,
    hash_sha256_hex,
)


def storage_options_fingerprint(
    storage: Mapping[str, str] | None,
    log_storage: Mapping[str, str] | None,
) -> str | None:
    if storage is None and log_storage is None:
        return None
    payload = {
        "storage": dict(storage or {}),
        "log_storage": dict(log_storage or {}),
    }
    return hash_json_canonical(payload, str_keys=True)


def file_digest(path: Path) -> str:
    return hash_file_sha256(path)
```

**Target files to modify** (non-exhaustive; existing plan list retained):
- `src/extract/cache_utils.py`
- `src/datafusion_engine/execution_helpers.py`
- `src/datafusion_engine/view_artifacts.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/scan_planner.py`
- `src/datafusion_engine/scan_overrides.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/param_tables.py`
- `src/datafusion_engine/provider_registry.py`
- `src/storage/deltalake/delta.py`
- `src/storage/ipc_utils.py`
- `src/engine/delta_tools.py`
- `src/cache/diskcache_factory.py`
- `src/relspec/execution_plan.py`
- `src/serde_artifacts.py`
- `src/serde_schema_registry.py`
- `src/extract/repo_scan.py`
- `src/incremental/scip_fingerprint.py`
- `src/datafusion_engine/write_pipeline.py`
- `src/datafusion_engine/table_spec.py`
- `src/datafusion_engine/planning_pipeline.py`

**Delete after migration**
- Local `_hash_payload`, `_payload_hash`, `_settings_hash`, `_storage_options_hash`, and truncated hash helpers.
- Legacy wrapper `src/datafusion_engine/hash_utils.py` (after all imports move to `utils.hashing`).

**Implementation checklist**
- [ ] Consolidate all hashing to `utils.hashing` with explicit semantic helpers.
- [ ] Add compatibility tests that compare legacy and new outputs for each call site.
- [ ] Remove legacy helpers after all sites migrate.

---

## Scope S2: Environment Variable Parsing (Canonical)

**Target State:** All env parsing routes through `utils.env_utils` for consistent behavior and logging.

**Representative pattern**:

```python
from __future__ import annotations

from utils.env_utils import env_bool, env_float, env_int, env_value


def load_flags() -> dict[str, object]:
    return {
        "enabled": env_bool("FEATURE_ENABLED", default=False),
        "timeout_s": env_int("TIMEOUT_S", default=30),
        "ratio": env_float("SAMPLE_RATIO", default=0.1),
        "tag": env_value("TRACE_TAG"),
    }
```

**Target files to modify** (retain original list):
- `src/obs/otel/config.py`
- `src/obs/otel/resources.py`
- `src/obs/otel/bootstrap.py`
- `src/hamilton_pipeline/driver_factory.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/engine/runtime_profile.py`
- `src/extract/git_remotes.py`
- `src/extract/git_settings.py`
- `src/cache/diskcache_factory.py`

**Delete after migration**
- Local `_env_*` helpers in the above modules.

**Implementation checklist**
- [ ] Replace all env helpers with `utils.env_utils`.
- [ ] Ensure invalid-value behavior matches legacy semantics per call site.
- [ ] Remove local helpers after migration.

---

## Scope S3: Canonical Type Aliases (Single Source of Truth)

**Target State:** All shared aliases live in `src/core_types.py`; local duplicates removed.

**Representative pattern**:

```python
from __future__ import annotations

from core_types import JsonValue, PathLike, RowPermissive, RowRich, RowStrict


def load_payload(path: PathLike) -> JsonValue:
    ...
```

**Target files to modify**
- `src/storage/dataset_sources.py`
- `src/serde_artifacts.py`
- `src/obs/metrics.py`
- `src/extract/bytecode_extract.py`
- `src/extract/tree_sitter_extract.py`
- `src/extract/cst_extract.py`
- `src/extract/scip_extract.py`

**Delete after migration**
- Local type alias definitions (`PathLike`, `JsonValue`, `Row`, `RowValue`, etc.).

**Implementation checklist**
- [ ] Expand/normalize aliases in `core_types.py`.
- [ ] Replace local aliases with imports.
- [ ] Remove legacy aliases after all call sites migrate.

---

## Scope S4: Dataclass Unification (ContractRow + View References)

**Target State:** Shared dataclasses have one canonical definition; ambiguous duplicates are renamed.

**Representative pattern**:

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ContractRow:
    dedupe: object | None = None
    canonical_sort: tuple[object, ...] = ()
    version: int | None = None
    constraints: tuple[str, ...] = ()
    virtual_fields: tuple[str, ...] = ()
    virtual_field_docs: dict[str, str] | None = None
    validation: object | None = None
```

**Target files to modify**
- `src/schema_spec/contract_row.py` (create canonical definition)
- `src/incremental/registry_rows.py`
- `src/normalize/dataset_rows.py`
- `src/incremental/registry_builders.py`
- `src/normalize/dataset_builders.py`
- `src/datafusion_engine/nested_tables.py` (rename `ViewReference` -> `SimpleViewRef`)
- `src/hamilton_pipeline/pipeline_types.py`

**Delete after migration**
- Local `ContractRow` definitions in incremental/normalize modules.

**Implementation checklist**
- [ ] Create canonical `ContractRow` in `schema_spec`.
- [ ] Update all imports and remove duplicates.
- [ ] Rename minimal `ViewReference` to `SimpleViewRef` and update call sites.

---

## Scope S5: Registry Protocol Standardization

**Target State:** Registry-like modules implement a shared protocol or base class when they are pure key/value stores.

**Representative pattern**:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


@dataclass
class MutableRegistry(Generic[K, V]):
    _entries: dict[K, V] = field(default_factory=dict)

    def register(self, key: K, value: V, *, overwrite: bool = False) -> None:
        if key in self._entries and not overwrite:
            msg = f"Key {key!r} already registered."
            raise ValueError(msg)
        self._entries[key] = value
```

**Target files to modify**
- `src/utils/registry_protocol.py` (canonical protocol + base classes)
- Registries that are pure key/value stores (selectively):
  - `src/datafusion_engine/schema_contracts.py`
  - `src/datafusion_engine/dataset_registry.py`
  - `src/datafusion_engine/extract_templates.py`

**Delete after migration**
- Redundant registry wrappers duplicated across modules.

**Implementation checklist**
- [ ] Align pure registries to the protocol/base class.
- [ ] Document when **not** to inherit (rich registries).
- [ ] Remove redundant registry implementations.

---

## Scope S6: Configuration Naming + Stable Fingerprints

**Target State:** Config naming aligns with documented convention; shared, safe fingerprinting for JSON-safe configs.

**Representative pattern**:

```python
from __future__ import annotations

from collections.abc import Mapping

from utils.hashing import hash_json_canonical


def config_fingerprint(payload: Mapping[str, object]) -> str:
    return hash_json_canonical(payload, str_keys=True)
```

**Target files to modify**
- `src/utils/config_utils.py` (new) or add helper to `src/utils/hashing.py`
- Config classes that are JSON-safe and should expose a fingerprint

**Delete after migration**
- Local config fingerprint helpers with identical semantics.

**Implementation checklist**
- [ ] Add `config_fingerprint` helper.
- [ ] Audit configs for JSON compatibility.
- [ ] Align naming conventions where breaking changes are acceptable.

---

## Scope S7: Hamilton Driver Factory Simplification

**Target State:** Flatten builder chain into explicit context stages.

**Representative pattern**:

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ViewGraphContext:
    ...


@dataclass(frozen=True)
class PlanContext:
    ...


@dataclass(frozen=True)
class BuilderContext:
    ...


def build_driver(...):
    view_ctx = build_view_graph_context(...)
    plan_ctx = build_plan_context(view_ctx)
    builder_ctx = build_builder_context(plan_ctx)
    return finalize_driver(builder_ctx)
```

**Target files to modify**
- `src/hamilton_pipeline/driver_factory.py`

**Delete after migration**
- Redundant intermediate helpers that only pass through kwargs.

**Implementation checklist**
- [ ] Extract contexts and builder steps.
- [ ] Keep API stable unless a breaking change is explicitly desired.
- [ ] Add integration tests for factory outputs.

---

## Scope S8: Validation + Missing-Item Utilities

**Target State:** Centralize validation patterns (mapping, sequence, callable, missing items) in `utils.validation`.

**Representative pattern**:

```python
from __future__ import annotations

from collections.abc import Container, Iterable
from typing import TypeVar

T = TypeVar("T")


def find_missing(required: Iterable[T], available: Container[T]) -> list[T]:
    return [item for item in required if item not in available]


def validate_required_items(
    required: Iterable[T],
    available: Container[T],
    *,
    item_label: str = "items",
    error_type: type[Exception] = ValueError,
) -> None:
    missing = find_missing(required, available)
    if missing:
        msg = f"Missing required {item_label}: {missing}"
        raise error_type(msg)
```

**Target files to modify**
- Mapping/sequence/callable validation sites:
  - `src/datafusion_engine/delta_control_plane.py`
  - `src/incremental/deltas.py`
  - `src/datafusion_engine/encoding.py`
- Missing-item check sites (partial list):
  - `src/datafusion_engine/registry_bridge.py`
  - `src/datafusion_engine/view_graph_registry.py`
  - `src/datafusion_engine/udf_runtime.py`
  - `src/datafusion_engine/runtime.py`
  - `src/relspec/graph_edge_validation.py`
  - `src/cpg/contract_map.py`
  - `src/engine/session.py`
  - `src/datafusion_engine/schema_alignment.py`
  - `src/datafusion_engine/execution_facade.py`
  - `src/schema_spec/system.py`
  - `src/datafusion_engine/schema_registry.py`
  - `src/hamilton_pipeline/validators.py`
  - `src/datafusion_engine/arrow_schema/nested_builders.py`

**Delete after migration**
- Local `_ensure_mapping`, `_ensure_table`, `_ensure_callable` helpers and inline missing-item blocks.

**Implementation checklist**
- [ ] Create `src/utils/validation.py`.
- [ ] Add unit tests for mapping/sequence/callable validation and missing-item helpers.
- [ ] Replace inline missing-item checks with `validate_required_items` where semantics match.

---

## Scope S9: Arrow Table Coercion + Storage-Type Utilities

**Target State:** A single Arrow coercion module that covers:
- Table-like inputs to `pa.Table`
- Extension type unwrapping to storage types

**Representative pattern**:

```python
from __future__ import annotations

import pyarrow as pa

from datafusion_engine.arrow_interop import RecordBatchReaderLike, TableLike, coerce_table_like


def to_arrow_table(value: TableLike) -> pa.Table:
    resolved = coerce_table_like(value)
    if isinstance(resolved, RecordBatchReaderLike):
        return resolved.read_all()
    if isinstance(resolved, pa.Table):
        return resolved
    return pa.Table.from_pydict(resolved.to_pydict())


def storage_type(data_type: pa.DataType) -> pa.DataType:
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
```

**Target files to modify**
- `src/datafusion_engine/encoding.py`
- `src/incremental/deltas.py`
- `src/datafusion_engine/schema_alignment.py`
- `src/datafusion_engine/schema_validation.py`
- `src/datafusion_engine/finalize.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/io_adapter.py`

**Delete after migration**
- Local `_storage_type` in `io_adapter.py` and inline `RecordBatchReader` conversions.

**Implementation checklist**
- [ ] Create `src/datafusion_engine/arrow_schema/coercion.py`.
- [ ] Add tests for table coercion and storage type conversion.
- [ ] Replace inline conversions in the target list.

---

## Scope S10: DataFusion Session Table Lifecycle Helpers

**Target State:** A single, safe, typed set of session helpers for temp table registration and cleanup.

**Representative pattern**:

```python
from __future__ import annotations

from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Iterator
from uuid import uuid4

from datafusion_engine.ingest import datafusion_from_arrow
from datafusion_engine.introspection import invalidate_introspection_cache
from datafusion_engine.arrow_schema.coercion import to_arrow_table

if TYPE_CHECKING:
    import pyarrow as pa
    from datafusion import SessionContext


def register_temp_table(
    ctx: SessionContext,
    table: pa.Table,
    *,
    prefix: str = "__temp_",
) -> str:
    name = f"{prefix}{uuid4().hex}"
    datafusion_from_arrow(ctx, name=name, value=table)
    return name


def deregister_table(ctx: SessionContext, name: str) -> None:
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        with suppress(KeyError, RuntimeError, TypeError, ValueError):
            deregister(name)
            invalidate_introspection_cache(ctx)


@contextmanager
def temp_table(
    ctx: SessionContext,
    table: pa.Table,
    *,
    prefix: str = "__temp_",
) -> Iterator[str]:
    name = register_temp_table(ctx, table, prefix=prefix)
    try:
        yield name
    finally:
        deregister_table(ctx, name)
```

**Target files to modify**
- `src/storage/deltalake/delta.py`
- `src/storage/deltalake/file_pruning.py`
- `src/datafusion_engine/finalize.py`
- `src/datafusion_engine/schema_validation.py`
- `src/datafusion_engine/encoding.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`

**Delete after migration**
- `_register_temp_table` / `_deregister_table` helpers in the above modules.

**Implementation checklist**
- [ ] Create `src/datafusion_engine/session_helpers.py`.
- [ ] Replace local temp-table helpers with canonical helpers.
- [ ] Ensure introspection cache invalidation behavior matches current usage.

---

## Scope S11: SessionConfig Application Helpers

**Target State:** One helper to apply config values while preserving `set()` fallback semantics.

**Representative pattern**:

```python
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionConfig


def apply_optional_config(
    config: SessionConfig,
    *,
    method: str,
    key: str,
    value: int | bool | str | None,
) -> SessionConfig:
    if value is None:
        return config
    updater = getattr(config, method, None)
    if callable(updater):
        return updater(value)
    setter = getattr(config, "set", None)
    if callable(setter):
        return setter(key, str(value).lower() if isinstance(value, bool) else str(value))
    return config
```

**Target files to modify**
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`

**Delete after migration**
- `_apply_optional_*` and related helpers in `runtime.py` and `registry_bridge.py`.

**Implementation checklist**
- [ ] Create `src/datafusion_engine/config_helpers.py`.
- [ ] Replace local helpers with centralized helpers.
- [ ] Keep `config.set` fallback behavior intact.

---

## Scope S12: Extract Options Redesign (Composable Options)

**Target State:** Shared option mixins with explicit composition. Normalize naming where breaking changes are acceptable.

**Representative pattern**:

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RepoOptions:
    repo_id: str | None = None


@dataclass(frozen=True)
class WorklistOptions:
    use_worklist_queue: bool = True


@dataclass(frozen=True)
class ParallelismOptions:
    parallel: bool = True
    max_workers: int | None = None
    batch_size: int | None = 512


@dataclass(frozen=True)
class AstExtractOptions(RepoOptions, WorklistOptions, ParallelismOptions):
    include_docstrings: bool = True
```

**Target files to modify**
- `src/extract/ast_extract.py` (rename `ASTExtractOptions` -> `AstExtractOptions`)
- `src/extract/bytecode_extract.py`
- `src/extract/cst_extract.py` (rename `CSTExtractOptions` -> `CstExtractOptions`)
- `src/extract/symtable_extract.py`
- `src/extract/tree_sitter_extract.py`
- `src/extract/scip_extract.py`
- `src/extract/repo_scan.py`
- `src/extract/__init__.py` (re-export canonical names)

**Delete after migration**
- Duplicate shared fields in each options class.
- Legacy uppercase options class names (after all call sites migrate).

**Implementation checklist**
- [ ] Create `src/extract/options.py` with reusable mixins.
- [ ] Migrate each options class to inherit mixins.
- [ ] Update all import sites to new canonical class names.
- [ ] Remove old names after migrations complete.

---

## Scope S13: Extract Schema Fingerprint Cache

**Target State:** Centralize dataset fingerprint caching with correct dataset names.

**Representative pattern**:

```python
from __future__ import annotations

from functools import cache

from datafusion_engine.arrow_schema.abi import schema_fingerprint
from datafusion_engine.extract_registry import dataset_schema


@cache
def cached_schema_fingerprint(dataset_name: str) -> str:
    return schema_fingerprint(dataset_schema(dataset_name))


def ast_files_fingerprint() -> str:
    return cached_schema_fingerprint("ast_files_v1")
```

**Target files to modify**
- `src/extract/ast_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/cst_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/tree_sitter_extract.py`
- `src/extract/repo_blobs.py`
- `src/extract/repo_scan.py`

**Delete after migration**
- Local `_*_schema_fingerprint` helpers in extraction modules.

**Implementation checklist**
- [ ] Create `src/extract/schema_cache.py`.
- [ ] Replace module-local fingerprint functions.
- [ ] Validate dataset name correctness.

---

## Scope S14: File I/O Utilities (TOML/JSON)

**Target State:** Canonical TOML/JSON readers with consistent UTF-8 handling.

**Representative pattern**:

```python
from __future__ import annotations

from pathlib import Path

from utils.file_io import read_pyproject_toml


def load_pyproject(path: Path) -> dict[str, object]:
    return dict(read_pyproject_toml(path))
```

**Target files to modify**
- `src/extract/repo_scope.py`
- `src/extract/ast_extract.py`
- `src/extract/python_scope.py`

**Delete after migration**
- Local `_load_pyproject` and inline `tomllib.loads(path.read_text(...))` usages.

**Implementation checklist**
- [ ] Create `src/utils/file_io.py`.
- [ ] Replace inline TOML/JSON parsing with helpers.

---

## Scope S15: Arrow Schema Field Builder DSL (Optional, Best-in-Class)

**Target State:** A light DSL for common field types to reduce verbosity in schema definitions.

**Representative pattern**:

```python
from __future__ import annotations

import pyarrow as pa

from datafusion_engine.arrow_schema.field_builders import (
    bool_field,
    int64_field,
    string_field,
    struct_field,
)

schema = pa.schema([
    string_field("id", nullable=False),
    int64_field("count"),
    struct_field("meta", fields=[bool_field("ok")]),
])
```

**Target files to modify** (apply gradually where it improves clarity):
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/function_factory.py`
- `src/datafusion_engine/view_registry.py`
- `src/obs/delta_observability.py`

**Delete after migration**
- None (optional convenience layer).

**Implementation checklist**
- [ ] Create `src/datafusion_engine/arrow_schema/field_builders.py`.
- [ ] Migrate schema definitions selectively.

---

## Scope S16: Deferred Decommissioning (After All Scopes Complete)

These items **must not be removed** until all prior scopes are fully migrated.

**Deferred deletions**
- `src/datafusion_engine/hash_utils.py` (wrapper to remove after full migration).
- Legacy type aliases (`Row`, `RowValue`) in `core_types.py` after all call sites use explicit aliases.
- Module-local `_hash_payload`, `_payload_hash`, `_settings_hash`, `_storage_options_hash` functions.
- Module-local `_env_*` helper functions.
- Module-local `_ensure_*` helpers and inline missing-item checks.
- Module-local `_register_temp_table` / `_deregister_table` helpers.
- Module-local `_*_schema_fingerprint` helpers in extract modules.

**Deferred checklist**
- [ ] Confirm no imports of deprecated helpers.
- [ ] Remove compatibility wrappers.
- [ ] Run full test + type check suite.

---

## Final Verification (All Scopes)

- [ ] `uv run ruff check --fix`
- [ ] `uv run pyrefly check`
- [ ] `uv run pyright --warnings --pythonversion=3.13`
- [ ] `uv run pytest tests/unit/`
- [ ] `uv run pytest tests/`

