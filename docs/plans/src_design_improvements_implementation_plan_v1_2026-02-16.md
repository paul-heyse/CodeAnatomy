# src/ Design Improvements Implementation Plan v1 (2026-02-16)

Synthesized from 10 independent design reviews covering all 18 modules in `src/` (~154K LOC, 509 Python files). Each review scored 24 design principles 0–3 with file:line evidence. This plan captures cross-cutting improvements and high-priority quick wins, ordered by blast radius and dependency chain.

Design stance: **hard cutover, no compatibility shims**. When a helper moves, callers update in the same PR. When a constant consolidates, duplicates are deleted in the same commit.

---

## Design Principles

1. **One definition, one authority** — every constant, helper, and protocol has exactly one canonical location.
2. **Dependency direction flows inward** — outer-ring modules (cli, obs, extraction, storage) never import from each other or from inner-ring modules in the wrong direction.
3. **Inject, don't construct** — services and caches are created by callers and passed in; modules never call global constructors for their own dependencies.
4. **Facade completeness** — if a facade (obs, runtime) exists, all consumers use it; no bypass imports.
5. **Protocol over ABC** — interfaces are structural (`typing.Protocol`), not class-based.
6. **Decompose at 800 LOC** — files exceeding ~800 LOC get domain-driven splits; the re-export hub pattern is used only as a temporary migration aid and is itself a scope item to remove.
7. **Leverage built-in library capabilities** — prefer DataFusion/DeltaLake/PyArrow built-ins over hand-rolled equivalents.

---

## Current Baseline

- `_RUNTIME_SESSION_ID` defined twice with **different** `uuid7_str()` calls (`runtime_hooks.py:50`, `runtime_session.py:33`). Each module-level call generates a distinct UUID at import time — a correctness bug risk.
- `_SESSION_CONTEXT_CACHE` defined twice (`runtime.py:302`, `runtime_session.py:51`). Two independent dicts caching `SessionContext` by key — split-cache bug risk.
- `runtime.py` is a 1,292 LOC re-export hub with 102-entry `__all__`, importing from 12+ sub-modules and renaming private names with `_ext_` / `_schema_` prefixes.
- `_line_offsets()` duplicated identically in 3 extractor files (`ast_extract.py:552`, `bytecode_extract.py:599`, `symtable_extract.py:136`), each calling `LineOffsets.from_bytes(data)`.
- `_coerce_*` helpers are duplicated across 8+ sites (`runtime_compile.py`, `plan/diagnostics.py`, `cache/inventory.py`, `cache/metadata_snapshots.py`, `cache/registry.py`, `extraction/options.py`) and boolean env parsing is duplicated in `extraction/runtime_profile.py` vs `utils/env_utils.py`.
- `delta_service_for_profile(None)` called at 8 sites — hidden service construction that skips DI.
- `cast("DataFusionRuntimeProfile", ...)` appears 11 times (9 in `runtime_diagnostics_mixin.py`) — mixin-cast anti-pattern from the re-export hub.
- 37 files outside `src/obs/` import directly from `obs.otel.*` sub-packages (73 direct import lines), bypassing the `obs.otel` facade.
- `serde_schema_registry.py:16` imports from `cli.config_models` — inner-ring → outer-ring dependency inversion.
- `storage/ipc_utils.py:14` imports from `extraction.plan_product` — cross-layer violation.
- `utils/validation.py:114` imports from `datafusion_engine.arrow.coercion` — utility → engine violation.
- `schema_spec/contracts.py` constructs `DataFusionRuntimeProfile` at 5+ sites — schema layer reaching into engine internals.
- No `ExtractorPort` protocol exists — 6 extractors share no formal interface; 9× boilerplate for session/plan/context setup.
- DataFusion Python 51 does not expose `ExecutionPlan.metrics()` and upstream removed `SchemaAdapter`; the supported path is `PhysicalExprAdapterFactory` plus `EXPLAIN ANALYZE` and extension runtime metrics snapshots.
- DeltaLake `replaceWhere` / predicate-based partial overwrite not used; full-table overwrites in `io/write.py`.

---

## S1. Consolidate Session Constants and Shared Runtime Helpers

### Goal

Eliminate duplicated module-level constants and helper definitions in `datafusion_engine/session/` that create split-state bugs and drift risk. Each constant/helper must have exactly one definition.

### Representative Code Snippets

```python
# src/datafusion_engine/session/_session_identity.py  (NEW)
"""Single-authority session identity constants."""
from __future__ import annotations

from typing import Final

from uuid6 import uuid7_str

RUNTIME_SESSION_ID: Final[str] = uuid7_str()
"""Single session identity for the process lifetime."""
```

```python
# src/datafusion_engine/session/_session_caches.py  (NEW)
"""Single-authority session caches."""
from __future__ import annotations

from weakref import WeakKeyDictionary

from datafusion import SessionContext

SESSION_CONTEXT_CACHE: dict[str, SessionContext] = {}
"""Process-wide SessionContext cache keyed by profile hash."""

SESSION_RUNTIME_CACHE: dict[str, object] = {}
"""Process-wide SessionRuntime cache keyed by profile hash."""

RUNTIME_SETTINGS_OVERLAY: WeakKeyDictionary[SessionContext, dict[str, str]] = WeakKeyDictionary()
"""Per-context settings overlay."""
```

```python
# In runtime_hooks.py — replace duplicate
from datafusion_engine.session._session_identity import RUNTIME_SESSION_ID
# DELETE: _RUNTIME_SESSION_ID: Final[str] = uuid7_str()

# In runtime_session.py — replace duplicate
from datafusion_engine.session._session_identity import RUNTIME_SESSION_ID
# DELETE: _RUNTIME_SESSION_ID: Final[str] = uuid7_str()

# In runtime.py — replace duplicate cache
from datafusion_engine.session._session_caches import SESSION_CONTEXT_CACHE
# DELETE: _SESSION_CONTEXT_CACHE: dict[str, SessionContext] = {}

# In runtime_session.py — replace duplicate cache
from datafusion_engine.session._session_caches import SESSION_CONTEXT_CACHE
# DELETE: _SESSION_CONTEXT_CACHE: dict[str, SessionContext] = {}
```

```python
# src/datafusion_engine/session/_session_constants.py  (NEW)
"""Single-authority session constants and helper functions."""
from __future__ import annotations

from collections.abc import Mapping

KIB = 1024
MIB = 1024 * KIB
GIB = 1024 * MIB

CACHE_PROFILES: Mapping[str, Mapping[str, str]] = {
    "snapshot_pinned": {"datafusion.runtime.list_files_cache_limit": str(64 * MIB)},
    "always_latest_ttl30s": {
        "datafusion.runtime.list_files_cache_limit": str(64 * MIB),
        "datafusion.runtime.list_files_cache_ttl": "30s",
    },
}

DATAFUSION_SQL_ERROR = Exception
EXTENSION_MODULE_NAMES: tuple[str, ...] = ("datafusion_engine.extensions.datafusion_ext",)

def parse_major_version(version: str) -> int | None:
    head = version.split(".", 1)[0]
    return int(head) if head.isdigit() else None
```

### Files to Edit

- `src/datafusion_engine/session/runtime_hooks.py` — remove `_RUNTIME_SESSION_ID` definition, import from `_session_identity`
- `src/datafusion_engine/session/runtime_session.py` — remove `_RUNTIME_SESSION_ID` and `_SESSION_CONTEXT_CACHE` definitions, import from new modules
- `src/datafusion_engine/session/runtime.py` — remove `_SESSION_CONTEXT_CACHE` definition, import from `_session_caches`
- `src/datafusion_engine/session/runtime_config_policies.py` — import shared cache constants/helpers from `_session_constants`
- `src/datafusion_engine/session/runtime_telemetry.py` — import shared `CACHE_PROFILES` from `_session_constants`
- `src/datafusion_engine/session/context_pool.py` — import shared `parse_major_version`
- `src/datafusion_engine/session/runtime_extensions.py` and `src/datafusion_engine/session/runtime_schema_registry.py` — import shared schema-introspector factory helper

### New Files to Create

- `src/datafusion_engine/session/_session_identity.py`
- `src/datafusion_engine/session/_session_caches.py`
- `src/datafusion_engine/session/_session_constants.py`
- `tests/unit/datafusion_engine/session/test_session_identity.py`
- `tests/unit/datafusion_engine/session/test_session_caches.py`
- `tests/unit/datafusion_engine/session/test_session_constants.py`

### Legacy Decommission/Delete Scope

- Delete `_RUNTIME_SESSION_ID: Final[str] = uuid7_str()` from `runtime_hooks.py:50`
- Delete `_RUNTIME_SESSION_ID: Final[str] = uuid7_str()` from `runtime_session.py:33`
- Delete `_SESSION_CONTEXT_CACHE: dict[str, SessionContext] = {}` from `runtime.py:302`
- Delete `_SESSION_CONTEXT_CACHE: dict[str, SessionContext] = {}` from `runtime_session.py:51`
- Delete `_SESSION_RUNTIME_CACHE` from `runtime_session.py:49` (move to `_session_caches.py`)
- Delete `_RUNTIME_SETTINGS_OVERLAY` from `runtime_session.py:50` (move to `_session_caches.py`)
- Delete duplicate `CACHE_PROFILES` definitions from `runtime_config_policies.py` and `runtime_telemetry.py`
- Delete duplicate `_parse_major_version` definitions from `runtime_config_policies.py` and `context_pool.py`
- Delete duplicate `_DATAFUSION_SQL_ERROR` definitions from `runtime_extensions.py` and `runtime_udf.py`
- Delete duplicate `_create_schema_introspector` definitions from `runtime_extensions.py` and `runtime_schema_registry.py`

---

## S2. Consolidate Type Coercion and Boolean Parsing Helpers

### Goal

Unify duplicated coercion and boolean/env parsing helpers into canonical shared locations in `src/utils/`: numeric/string coercion in `utils/coercion.py`, environment boolean parsing delegated to `utils/env_utils.py`.

### Representative Code Snippets

```python
# src/utils/coercion.py  (NEW)
"""Canonical type coercion helpers for configuration parsing."""
from __future__ import annotations


def coerce_int(value: object, *, label: str = "value") -> int:
    """Coerce a value to int or raise TypeError.

    Parameters
    ----------
    value
        Value to coerce (int, float with no fractional part, or str).
    label
        Field name for error messages.
    """
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value == int(value):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            pass
    msg = f"{label}: cannot coerce {type(value).__name__} to int"
    raise TypeError(msg)


def coerce_opt_int(value: object, *, label: str = "value") -> int | None:
    """Coerce to int if non-None, else return None."""
    if value is None:
        return None
    return coerce_int(value, label=label)


def coerce_bool(value: object, *, default: bool, label: str = "value") -> bool:
    """Coerce a value to bool with a default fallback.

    Parameters
    ----------
    value
        Value to coerce (bool, int 0/1, str true/false/yes/no/1/0).
    default
        Returned when *value* is None.
    label
        Field name for error messages.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        low = value.strip().lower()
        if low in {"true", "1", "yes", "on"}:
            return True
        if low in {"false", "0", "no", "off", ""}:
            return False
    msg = f"{label}: cannot coerce {type(value).__name__!r} to bool"
    raise TypeError(msg)
```

```python
# Callers update to:
from utils.coercion import coerce_int, coerce_opt_int, coerce_bool
```

```python
# In extraction/runtime_profile.py (remove duplicated env bool parsing)
# BEFORE: _ENV_TRUE_VALUES/_ENV_FALSE_VALUES + _env_patch_bool custom parser
# AFTER:
from utils.env_utils import env_bool

def _env_patch_bool(name: str) -> bool | msgspec.UnsetType:
    value = env_bool(name)
    return msgspec.UNSET if value is None else value
```

### Files to Edit

- `src/datafusion_engine/session/runtime_compile.py` — replace `_coerce_int` and `_coerce_bool` with imports from `utils.coercion`
- `src/datafusion_engine/plan/diagnostics.py` — replace `_coerce_int` with import
- `src/datafusion_engine/cache/inventory.py` — replace `_coerce_int` with import
- `src/datafusion_engine/cache/metadata_snapshots.py` — replace `_coerce_optional_int` with import
- `src/datafusion_engine/cache/registry.py` — replace `_coerce_opt_int` and related coercion helpers with imports
- `src/extraction/options.py` — replace `_coerce_bool` with import
- `src/extraction/runtime_profile.py` — delegate `_env_patch_bool` to `utils.env_utils.env_bool`

### New Files to Create

- `src/utils/coercion.py`
- `tests/unit/utils/test_coercion.py`

### Legacy Decommission/Delete Scope

- Delete `_coerce_int` from `runtime_compile.py:160`
- Delete `_coerce_int` from `plan/diagnostics.py:266`
- Delete `_coerce_int` / `_coerce_opt_int` from `cache/inventory.py:309`
- Delete `_coerce_optional_int` from `cache/metadata_snapshots.py:202`
- Delete `_coerce_opt_str` / `_coerce_opt_int` / `_coerce_str_tuple` from `cache/registry.py`
- Delete `_coerce_bool` from `runtime_compile.py:167`
- Delete `_coerce_bool` from `extraction/options.py:187`
- Delete `_ENV_TRUE_VALUES`, `_ENV_FALSE_VALUES`, and custom parsing branches from `extraction/runtime_profile.py`

---

## S3. Fix Dependency Direction Violations

### Goal

Eliminate the 4 confirmed cross-layer import violations where inner modules import from outer or peer modules in the wrong direction. The fix is to extract shared types into a neutral location or invert the dependency.

### Representative Code Snippets

```python
# FIX 1: serde_schema_registry.py imports from cli.config_models
# Move the shared config spec types to a neutral location.
# src/core/config_specs.py  (NEW — or inline into serde_schema_registry.py)
"""Shared configuration spec types used by both serde and cli layers."""
from __future__ import annotations

import msgspec


class CacheConfigSpec(msgspec.Struct, frozen=True):
    """Cache configuration specification."""
    ...

class DeltaConfigSpec(msgspec.Struct, frozen=True):
    """Delta configuration specification."""
    ...

# ... remaining specs that serde_schema_registry needs
```

```python
# FIX 2: storage/ipc_utils.py imports from extraction.plan_product
# Move PlanProduct or the needed interface to a neutral types module.
# In storage/ipc_utils.py:
# BEFORE: from extraction.plan_product import PlanProduct
# AFTER:  Accept the narrower protocol type directly
from __future__ import annotations
from typing import Protocol

import pyarrow as pa


class HasArrowTable(Protocol):
    """Anything that can produce an Arrow table."""
    def to_table(self) -> pa.Table: ...
```

```python
# FIX 3: utils/validation.py imports from datafusion_engine.arrow.coercion
# Move ensure_table out of utils layer into engine layer.
# BEFORE (utils/validation.py):
#   def ensure_table(...): from datafusion_engine.arrow.coercion import to_arrow_table
# AFTER:
#   # ensure_table removed from utils/validation.py
#   from datafusion_engine.arrow.coercion import ensure_arrow_table
```

```python
# FIX 4: schema_spec/contracts.py constructs DataFusionRuntimeProfile 5x
# Accept profile as a parameter instead of constructing it.
# BEFORE (schema_spec/contracts.py):
#   profile = DataFusionRuntimeProfile(...)
#   schema = delta_service_for_profile(profile).table_schema(...)
# AFTER:
def table_schema_for_spec(
    spec: DatasetSpec,
    *,
    delta_service: DeltaService,  # injected
) -> pa.Schema:
    return delta_service.table_schema(...)
```

### Files to Edit

- `src/serde_schema_registry.py` — remove import from `cli.config_models`, import from `core/config_specs.py`
- `src/cli/config_models.py` — import shared specs from `core/config_specs.py` (no re-export shim layer)
- `src/storage/ipc_utils.py` — replace `extraction.plan_product.PlanProduct` with protocol
- `src/utils/validation.py` — delete `ensure_table` and related engine import
- `src/datafusion_engine/arrow/coercion.py` — add canonical `ensure_arrow_table` helper
- `src/schema_spec/contracts.py` — remove direct `DataFusionRuntimeProfile` construction, accept injected service

### New Files to Create

- `src/core/config_specs.py`
- `tests/unit/core/test_config_specs.py`

### Legacy Decommission/Delete Scope

- Delete `from cli.config_models import ...` from `serde_schema_registry.py:16`
- Delete `from extraction.plan_product import PlanProduct` from `storage/ipc_utils.py:14`
- Delete `ensure_table` from `utils/validation.py` and migrate all callers to engine-layer helper
- Delete direct `DataFusionRuntimeProfile(...)` construction calls from `schema_spec/contracts.py` (5 sites)

---

## S4. Expand and Enforce OTel Facade

### Goal

Enforce a single `obs.otel` facade for observability imports and eliminate all direct `obs.otel.*` submodule imports outside `src/obs/`. Reduce 73 direct import lines across 37 files to zero.

### Representative Code Snippets

```python
# src/obs/otel/__init__.py  (EDIT)
# Expand facade exports so external callers never import obs.otel.* submodules directly.
__all__ = [
    "configure_otel",
    "emit_diagnostics_event",
    "get_run_id",
    "set_run_id",
    "reset_run_id",
    "stage_span",
    "cache_span",
    "record_error",
    "record_stage_duration",
    "SCOPE_EXTRACT",
    "SCOPE_GRAPH",
    "SCOPE_SEMANTICS",
    "SCOPE_STORAGE",
]

_EXPORT_MAP = {
    "get_run_id": ("obs.otel.run_context", "get_run_id"),
    "set_run_id": ("obs.otel.run_context", "set_run_id"),
    "reset_run_id": ("obs.otel.run_context", "reset_run_id"),
    "stage_span": ("obs.otel.tracing", "stage_span"),
    "cache_span": ("obs.otel.cache", "cache_span"),
    "SCOPE_EXTRACT": ("obs.otel.scopes", "SCOPE_EXTRACT"),
    "SCOPE_GRAPH": ("obs.otel.scopes", "SCOPE_GRAPH"),
    # ...
}
```

```python
# Example caller update:
# BEFORE (in graph/product_build.py):
#   from obs.otel.tracing import stage_span
#   from obs.otel.scopes import SCOPE_GRAPH
# AFTER:
#   from obs.otel import stage_span, SCOPE_GRAPH
```

### Files to Edit

- `src/obs/otel/__init__.py` — expand facade export surface to include all externally consumed symbols
- All 37 files that import from `obs.otel.*` submodules — replace with facade imports from `obs.otel`:
  - `src/extraction/engine_runtime.py`
  - `src/extraction/engine_session.py`
  - `src/extraction/engine_session_factory.py`
  - `src/extraction/materialize_pipeline.py`
  - `src/graph/product_build.py`
  - `src/graph/build_pipeline.py`
  - `src/cli/app.py`
  - `src/cli/context.py`
  - `src/datafusion_engine/io/adapter.py`
  - `src/datafusion_engine/io/write.py`
  - `src/datafusion_engine/session/runtime.py`
  - `src/datafusion_engine/session/runtime_ops.py`
  - `src/datafusion_engine/delta/maintenance.py`
  - `src/extract/coordination/evidence_plan.py`
  - `src/extract/infrastructure/parallel.py`
  - `src/semantics/diagnostics/issue_batching.py`
  - `src/semantics/diagnostics_emission.py`
  - (remaining files from the 37 identified)

### New Files to Create

- `tests/unit/obs/test_otel_facade_exports.py`
- `tests/unit/obs/test_otel_import_boundaries.py`

### Legacy Decommission/Delete Scope

- Delete all direct `from obs.otel.* import ...` imports in files outside `src/obs/` (73 import lines across 37 files)
- Enforce via import-boundary test that only `from obs.otel import ...` is allowed outside `src/obs/`

---

## S5. Add Convenience Properties to Reduce Law of Demeter Violations

### Goal

Reduce the 25+ `.resolved.delta_*` chains and `session.engine_session.*` chains by adding convenience properties at the natural access points. DataFusion's `SessionContext` already provides `table()`, `sql()`, etc. — the issue is our wrapper layer forcing deep chains.

### Representative Code Snippets

```python
# In src/datafusion_engine/delta/protocols.py or a dedicated accessor module:
class DeltaProfileAccessor:
    """Convenience properties to eliminate .resolved.delta_* chains."""

    def __init__(self, profile: DataFusionRuntimeProfile) -> None:
        self._profile = profile

    @property
    def delta_log_path(self) -> str | None:
        return self._profile.resolved.delta_log_path

    @property
    def delta_table_uri(self) -> str | None:
        return self._profile.resolved.delta_table_uri

    @property
    def delta_storage_options(self) -> Mapping[str, str]:
        return self._profile.resolved.delta_storage_options

    @property
    def delta_version(self) -> int | None:
        return self._profile.resolved.delta_version
```

```python
# Alternative: add properties directly to DataFusionRuntimeProfile
# In the profile dataclass:
@property
def delta_log_path(self) -> str | None:
    """Shortcut for self.resolved.delta_log_path."""
    return self.resolved.delta_log_path
```

### Files to Edit

- `src/datafusion_engine/session/runtime_profile_config.py` — add convenience properties to `DataFusionRuntimeProfile`
- Files with `.resolved.delta_*` chains (25+ sites across `delta/`, `dataset/`, `schema_spec/`, `io/`)

### New Files to Create

- `tests/unit/datafusion_engine/session/test_profile_convenience.py`

### Legacy Decommission/Delete Scope

- No files deleted — this is additive. Callers migrate from `profile.resolved.delta_*` to `profile.delta_*` in the same PR.

---

## S6. Inject Dependencies to Replace Hidden Service Creation

### Goal

Eliminate the 8 `delta_service_for_profile(None)` call sites by requiring callers to inject a `DeltaService` instance. This removes hidden global construction, improves testability, and enables DataFusion's built-in session-scoped caching.

### Representative Code Snippets

```python
# Define a protocol for the delta service
# src/datafusion_engine/delta/service_protocol.py  (NEW)
"""Protocol for Delta service injection."""
from __future__ import annotations

from typing import Protocol

import pyarrow as pa


class DeltaServicePort(Protocol):
    """Structural interface for delta table operations."""

    def table_schema(self, request: object) -> pa.Schema: ...
    def table_version(self, *, path: str) -> int | None: ...
```

```python
# BEFORE (in schema_spec/contracts.py):
#   schema = delta_service_for_profile(None).table_schema(request)
# AFTER:
def schema_for_spec(
    spec: DatasetSpec,
    *,
    delta_service: DeltaServicePort,
) -> pa.Schema:
    return delta_service.table_schema(request)
```

```python
# BEFORE (in cli/commands/delta.py):
#   service = delta_service_for_profile(None)
# AFTER — inject via CLI context:
@click.pass_context
def delta_optimize(ctx: click.Context, path: str) -> None:
    service = ctx.obj["delta_service"]
    service.optimize(path=path)
```

### Files to Edit

- `src/schema_spec/contracts.py` — accept `delta_service` parameter (5 sites)
- `src/datafusion_engine/dataset/registry.py` — accept `delta_service` parameter
- `src/datafusion_engine/delta/contracts.py` — accept `delta_service` parameter
- `src/datafusion_engine/delta/schema_guard.py` — accept `delta_service` parameter
- `src/datafusion_engine/delta/service.py` — remove `delta_service_for_profile` composition helper
- `src/cli/commands/delta.py` — inject via CLI context (3 sites)
- `src/cli/commands/diag.py` — inject via CLI context

### New Files to Create

- `src/datafusion_engine/delta/service_protocol.py`
- `tests/unit/datafusion_engine/delta/test_service_protocol.py`

### Legacy Decommission/Delete Scope

- Delete all 8 `delta_service_for_profile(None)` call sites
- Delete `delta_service_for_profile()` after all callers migrate to explicit DI composition roots

---

## S7. Define ExtractorPort Protocol and Extract Shared Coordination

### Goal

Create a formal `ExtractorPort` protocol that all 6 extractors implement, and extract the triplicated `_line_offsets()` helper into the existing `coordination/line_offsets.py` module.

### Representative Code Snippets

```python
# src/extract/protocols.py  (NEW)
"""Extractor protocol definition."""
from __future__ import annotations

from typing import Protocol

import pyarrow as pa

from extract.coordination.context import ExtractExecutionContext, FileContext


class ExtractorPort(Protocol):
    """Structural interface for all extractors.

    Every extractor accepts file contexts and an execution context,
    and returns Arrow tables keyed by output name.
    """

    def extract(
        self,
        file_contexts: Sequence[FileContext],
        *,
        execution_context: ExtractExecutionContext,
    ) -> Mapping[str, pa.Table]: ...

    @property
    def name(self) -> str: ...

    @property
    def output_names(self) -> Sequence[str]: ...
```

```python
# Move _line_offsets to its canonical home
# In src/extract/coordination/line_offsets.py — add function:
def line_offsets_from_file_ctx(file_ctx: FileContext) -> LineOffsets | None:
    """Build line offsets from a file context.

    Parameters
    ----------
    file_ctx
        File context with byte data.

    Returns
    -------
    LineOffsets | None
        Line offsets or None if no byte data available.
    """
    data = bytes_from_file_ctx(file_ctx)
    if data is None:
        return None
    return LineOffsets.from_bytes(data)
```

```python
# In each extractor — replace duplicated helper:
# BEFORE (ast_extract.py:552):
#   def _line_offsets(file_ctx: FileContext) -> LineOffsets | None:
#       data = bytes_from_file_ctx(file_ctx)
#       ...
# AFTER:
from extract.coordination.line_offsets import line_offsets_from_file_ctx
# ... use line_offsets_from_file_ctx(file_ctx) instead of _line_offsets(file_ctx)
```

### Files to Edit

- `src/extract/coordination/line_offsets.py` — add `line_offsets_from_file_ctx()`
- `src/extract/extractors/ast_extract.py` — delete `_line_offsets`, import shared
- `src/extract/extractors/bytecode_extract.py` — delete `_line_offsets`, import shared
- `src/extract/extractors/symtable_extract.py` — delete `_line_offsets`, import shared

### New Files to Create

- `src/extract/protocols.py`
- `tests/unit/extract/test_protocols.py`
- `tests/unit/extract/test_line_offsets_shared.py`

### Legacy Decommission/Delete Scope

- Delete `_line_offsets` from `ast_extract.py:552`
- Delete `_line_offsets` from `bytecode_extract.py:599`
- Delete `_line_offsets` from `symtable_extract.py:136`

---

## S8. Decompose Session Re-Export Hub

### Goal

Eliminate the 102-entry `__all__` re-export hub in `runtime.py` (1,292 LOC) by having callers import directly from the sub-modules that were already extracted. Remove the `_ext_` / `_schema_` / `_telemetry_` alias pattern. Eliminate the mixin-cast anti-pattern (11 `cast("DataFusionRuntimeProfile", ...)` calls).

### Representative Code Snippets

```python
# BEFORE (any caller):
#   from datafusion_engine.session.runtime import compile_options_for_profile
# AFTER (direct import from extracted module):
#   from datafusion_engine.session.runtime_compile_options import compile_options_for_profile

# BEFORE (any caller):
#   from datafusion_engine.session.runtime import _install_cache_tables
# AFTER:
#   from datafusion_engine.session.runtime_extensions import _install_cache_tables

# BEFORE (any caller):
#   from datafusion_engine.session.runtime import DataFusionConfigPolicy
# AFTER:
#   from datafusion_engine.session.runtime_config_policies import DataFusionConfigPolicy
```

```python
# In runtime_diagnostics_mixin.py — eliminate cast anti-pattern:
# BEFORE:
#   profile = cast("DataFusionRuntimeProfile", self)
# AFTER — use Protocol to type self:
from typing import Protocol

class HasProfile(Protocol):
    @property
    def profile(self) -> DataFusionRuntimeProfile: ...

class _RuntimeDiagnosticsMixin:
    """Diagnostics mixin that uses protocol-typed access."""

    def _diagnostics_profile(self: HasProfile) -> DataFusionRuntimeProfile:
        return self.profile
```

### Files to Edit

- `src/datafusion_engine/session/runtime.py` — remove all re-export import blocks, reduce to minimal module
- `src/datafusion_engine/session/runtime_diagnostics_mixin.py` — eliminate 9 `cast("DataFusionRuntimeProfile", ...)` calls
- `src/datafusion_engine/session/runtime_ops.py` — eliminate 2 `cast("DataFusionRuntimeProfile", ...)` calls
- All files importing from `runtime.py` (update to import from specific sub-modules)

### New Files to Create

- `tests/unit/datafusion_engine/session/test_runtime_decomposition.py`

### Legacy Decommission/Delete Scope

- Delete all re-export blocks from `runtime.py:64–258` (the ~190 lines of `from ... import ...` re-exports)
- Delete `_ext_install_cache_tables`, `_ext_install_planner_rules`, etc. alias names
- Delete `_schema_ast_dataset_location`, `_schema_bytecode_dataset_location`, etc. alias names
- Delete `_telemetry_identifier_normalization_mode`, `_telemetry_effective_ident_normalization` alias names
- Delete all 11 `cast("DataFusionRuntimeProfile", ...)` calls across `runtime_diagnostics_mixin.py` and `runtime_ops.py`

---

## S9. Decompose God File: `dataset/registration.py` (3,368 LOC)

### Goal

Split the largest file in the codebase into domain-coherent modules under `dataset/`.

### Representative Code Snippets

```python
# Suggested decomposition:
# src/datafusion_engine/dataset/registration_core.py    — DatasetLocation, lookup, registry
# src/datafusion_engine/dataset/registration_delta.py   — Delta-specific registration logic
# src/datafusion_engine/dataset/registration_listing.py — Listing table registration
# src/datafusion_engine/dataset/registration_schema.py  — Schema introspection helpers
```

### Files to Edit

- `src/datafusion_engine/dataset/registration.py` — split into 4 sub-modules

### New Files to Create

- `src/datafusion_engine/dataset/registration_core.py`
- `src/datafusion_engine/dataset/registration_delta.py`
- `src/datafusion_engine/dataset/registration_listing.py`
- `src/datafusion_engine/dataset/registration_schema.py`
- `tests/unit/datafusion_engine/dataset/test_registration_core.py`
- `tests/unit/datafusion_engine/dataset/test_registration_delta.py`

### Legacy Decommission/Delete Scope

- Delete `registration.py` after all contents are migrated to sub-modules
- No temporary re-export facade: callers must migrate in the same PR

---

## S10. Decompose God File: `io/write.py` (2,689 LOC)

### Goal

Split the monolithic write module into concerns: write orchestration, Delta write, format-specific writers.

### Representative Code Snippets

```python
# Suggested decomposition:
# src/datafusion_engine/io/write_core.py       — WriteRequest, WriteResult, orchestration
# src/datafusion_engine/io/write_delta.py      — Delta-specific write logic
# src/datafusion_engine/io/write_formats.py    — Parquet/IPC/CSV format adapters
```

DataFusion built-in opportunity: **DeltaLake `replaceWhere`** for predicate-based partial overwrites instead of full-table rewrites:

```python
# BEFORE (full overwrite):
#   write_deltalake(table_uri, data, mode="overwrite")
# AFTER (predicate-based partial overwrite via DeltaLake replaceWhere):
from deltalake import write_deltalake

write_deltalake(
    table_uri,
    data,
    mode="overwrite",
    predicate=f"partition_col = '{partition_value}'",  # replaceWhere semantics
)
```

### Files to Edit

- `src/datafusion_engine/io/write.py` — split into 3 sub-modules

### New Files to Create

- `src/datafusion_engine/io/write_core.py`
- `src/datafusion_engine/io/write_delta.py`
- `src/datafusion_engine/io/write_formats.py`
- `tests/unit/datafusion_engine/io/test_write_core.py`
- `tests/unit/datafusion_engine/io/test_write_delta.py`

### Legacy Decommission/Delete Scope

- Delete `write.py` after all contents are migrated

---

## S11. Decompose God File: `delta/control_plane.py` (2,070 LOC)

### Goal

Split the Delta control plane into lifecycle, provider construction, and maintenance operations.

### Representative Code Snippets

```python
# Suggested decomposition:
# src/datafusion_engine/delta/control_plane_core.py     — Snapshot/metadata operations
# src/datafusion_engine/delta/control_plane_provider.py  — Provider construction + scan config
# src/datafusion_engine/delta/control_plane_maintenance.py — Optimize/vacuum/checkpoint
```

### Files to Edit

- `src/datafusion_engine/delta/control_plane.py` — split into 3 sub-modules

### New Files to Create

- `src/datafusion_engine/delta/control_plane_core.py`
- `src/datafusion_engine/delta/control_plane_provider.py`
- `src/datafusion_engine/delta/control_plane_maintenance.py`
- `tests/unit/datafusion_engine/delta/test_control_plane_core.py`
- `tests/unit/datafusion_engine/delta/test_control_plane_provider.py`

### Legacy Decommission/Delete Scope

- Delete `control_plane.py` after all contents are migrated

---

## S12. Decompose God File: `semantics/pipeline.py` (1,671 LOC)

### Goal

Split the 5-concern pipeline module into focused components: build entry, dispatch, caching, diagnostics, and configuration.

### Representative Code Snippets

```python
# Suggested decomposition:
# src/semantics/pipeline_build.py       — build_cpg() entry point
# src/semantics/pipeline_dispatch.py    — _dispatch_from_registry() and builder routing
# src/semantics/pipeline_cache.py       — _default_semantic_cache_policy and cache logic
# src/semantics/pipeline_diagnostics.py — Pipeline diagnostic emission
# src/semantics/pipeline_config.py      — CpgBuildOptions, SemanticConfig  (if not already in config.py)
```

### Files to Edit

- `src/semantics/pipeline.py` — split into focused modules

### New Files to Create

- `src/semantics/pipeline_build.py`
- `src/semantics/pipeline_dispatch.py`
- `src/semantics/pipeline_cache.py`
- `src/semantics/pipeline_diagnostics.py`
- `tests/unit/semantics/test_pipeline_build.py`
- `tests/unit/semantics/test_pipeline_dispatch.py`

### Legacy Decommission/Delete Scope

- Delete `pipeline.py` after migration

---

## S13. Decompose God File: `storage/deltalake/delta.py` (2,824 LOC)

### Goal

Split the second-largest file in the codebase into domain-coherent sub-modules.

### Representative Code Snippets

```python
# Suggested decomposition:
# src/storage/deltalake/delta_read.py        — Read/scan operations
# src/storage/deltalake/delta_write.py       — Write/commit operations
# src/storage/deltalake/delta_maintenance.py — Optimize/vacuum/repair
# src/storage/deltalake/delta_metadata.py    — Schema/version/snapshot queries
```

DataFusion built-in opportunity: rely on supported Python surfaces (`EXPLAIN ANALYZE`) and existing native extension snapshots instead of hand-maintained timing fields:

```python
# Use existing DataFusion Python explain capture path.
from datafusion_engine.plan.profiler import capture_explain

capture = capture_explain(df, verbose=True, analyze=True)
if capture is not None:
    details = {
        "explain_analyze_duration_ms": capture.duration_ms,
        "explain_analyze_output_rows": capture.output_rows,
    }
    # Emit into diagnostics/artifacts payload
```

### Files to Edit

- `src/storage/deltalake/delta.py` — split into 4 sub-modules

### New Files to Create

- `src/storage/deltalake/delta_read.py`
- `src/storage/deltalake/delta_write.py`
- `src/storage/deltalake/delta_maintenance.py`
- `src/storage/deltalake/delta_metadata.py`
- `tests/unit/storage/deltalake/test_delta_read.py`
- `tests/unit/storage/deltalake/test_delta_write.py`

### Legacy Decommission/Delete Scope

- Delete `delta.py` after all contents are migrated

---

## S14. Decompose God Files: `artifact_store.py` (1,654 LOC) and `extension_runtime.py` (1,660 LOC)

### Goal

Split the two execution/computation god files into focused sub-modules.

### Representative Code Snippets

```python
# artifact_store.py decomposition:
# src/datafusion_engine/plan/artifact_store_core.py     — Store CRUD operations
# src/datafusion_engine/plan/artifact_store_cache.py    — Caching layer
# src/datafusion_engine/plan/artifact_store_query.py    — Query/lookup operations

# extension_runtime.py decomposition:
# src/datafusion_engine/udf/extension_core.py           — Core extension loading
# src/datafusion_engine/udf/extension_registry.py       — Registry management
# src/datafusion_engine/udf/extension_validation.py     — Validation + parity checks
```

### Files to Edit

- `src/datafusion_engine/plan/artifact_store.py` — split into 3 sub-modules
- `src/datafusion_engine/udf/extension_runtime.py` — split into 3 sub-modules

### New Files to Create

- `src/datafusion_engine/plan/artifact_store_core.py`
- `src/datafusion_engine/plan/artifact_store_cache.py`
- `src/datafusion_engine/plan/artifact_store_query.py`
- `src/datafusion_engine/udf/extension_core.py`
- `src/datafusion_engine/udf/extension_registry.py`
- `src/datafusion_engine/udf/extension_validation.py`
- `tests/unit/datafusion_engine/plan/test_artifact_store_core.py`
- `tests/unit/datafusion_engine/udf/test_extension_core.py`

### Legacy Decommission/Delete Scope

- Delete `artifact_store.py` after migration
- Delete `extension_runtime.py` after migration

---

## S15. Decompose God Files: Extractors (1,388–1,900 LOC each)

### Goal

Split the 4 largest extractor files by extraction concern (setup, per-node extraction, post-processing, output building).

### Representative Code Snippets

```python
# cst_extract.py (1,900 LOC) decomposition:
# src/extract/extractors/cst/setup.py      — CST metadata wrapper setup, provider config
# src/extract/extractors/cst/visitors.py   — LibCST visitor implementations
# src/extract/extractors/cst/builders.py   — Arrow table builders from CST nodes
# src/extract/extractors/cst/__init__.py   — Public entry point (extract function)

# ast_extract.py (1,388 LOC) — similar pattern
# bytecode_extract.py (1,854 LOC) — similar pattern
# tree_sitter/extract.py (1,577 LOC) — similar pattern
```

### Files to Edit

- `src/extract/extractors/cst_extract.py` — decompose
- `src/extract/extractors/ast_extract.py` — decompose
- `src/extract/extractors/bytecode_extract.py` — decompose
- `src/extract/extractors/tree_sitter/extract.py` — decompose

### New Files to Create

- `src/extract/extractors/cst/` directory with `setup.py`, `visitors.py`, `builders.py`, `__init__.py`
- `src/extract/extractors/ast/` directory with similar structure
- `src/extract/extractors/bytecode/` directory with similar structure
- Corresponding test files under `tests/unit/extract/extractors/`

### Legacy Decommission/Delete Scope

- Delete `cst_extract.py`, `ast_extract.py`, `bytecode_extract.py` after migration to package directories
- Delete duplicated helpers already covered by S7 (`_line_offsets`)

---

## S16. Decompose God Files: `schema_spec/contracts.py` (1,858 LOC) and `schema/introspection.py` (1,305 LOC)

### Goal

Split the schema specification contracts into focused modules and decompose the SchemaIntrospector god class.

### Representative Code Snippets

```python
# contracts.py decomposition:
# src/schema_spec/dataset_spec.py     — DatasetSpec and related types
# src/schema_spec/table_spec.py       — TableSpec, ColumnSpec
# src/schema_spec/validation.py       — Schema validation logic
# src/schema_spec/discovery.py        — Schema discovery from DataFusion catalog

# introspection.py decomposition — SchemaIntrospector (1,305 LOC, 25+ callsites)
# src/datafusion_engine/schema/introspection_core.py  — Core introspection queries
# src/datafusion_engine/schema/introspection_cache.py  — Cached schema lookups
# src/datafusion_engine/schema/introspection_delta.py  — Delta-specific schema queries
```

DataFusion built-in opportunity: use `PhysicalExprAdapterFactory` (supported) instead of removed `SchemaAdapter` APIs:

```python
# Runtime already supports installing a physical expression adapter factory.
from datafusion_engine.session.runtime_extensions import (
    _install_physical_expr_adapter_factory,
)

ctx = profile.session_runtime().ctx
_install_physical_expr_adapter_factory(profile, ctx)
```

### Files to Edit

- `src/schema_spec/contracts.py` — split into 4 sub-modules
- `src/datafusion_engine/schema/introspection.py` — split into 3 sub-modules

### New Files to Create

- `src/schema_spec/dataset_spec.py`
- `src/schema_spec/table_spec.py`
- `src/schema_spec/validation.py`
- `src/schema_spec/discovery.py`
- `src/datafusion_engine/schema/introspection_core.py`
- `src/datafusion_engine/schema/introspection_cache.py`
- `src/datafusion_engine/schema/introspection_delta.py`
- `tests/unit/schema_spec/test_dataset_spec.py`
- `tests/unit/schema_spec/test_table_spec.py`
- `tests/unit/datafusion_engine/schema/test_introspection_core.py`

### Legacy Decommission/Delete Scope

- Delete `contracts.py` after migration
- Delete `introspection.py` after migration

---

## S17. Extract Diagnostic Builder Framework

### Goal

Eliminate the 5× repeated diagnostic builder boilerplate in `semantics/diagnostics/` by extracting a shared builder base.

### Representative Code Snippets

```python
# src/semantics/diagnostics/builder_base.py  (NEW)
"""Shared diagnostic builder framework."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol

import pyarrow as pa


class DiagnosticRecord(Protocol):
    """Protocol for diagnostic records."""
    @property
    def severity(self) -> str: ...
    @property
    def message(self) -> str: ...
    @property
    def source_location(self) -> str | None: ...


@dataclass
class DiagnosticBatchBuilder:
    """Reusable builder for diagnostic Arrow batches.

    Eliminates the repeated pattern of:
    1. Accumulate records
    2. Build column arrays
    3. Create RecordBatch with schema
    """

    _records: list[dict[str, object]] = field(default_factory=list)
    _schema: pa.Schema | None = None

    def add(self, record: Mapping[str, object]) -> None:
        """Append a diagnostic record."""
        self._records.append(dict(record))

    def build(self, schema: pa.Schema) -> pa.RecordBatch:
        """Build the Arrow RecordBatch from accumulated records."""
        if not self._records:
            return pa.RecordBatch.from_pydict(
                {f.name: [] for f in schema}, schema=schema
            )
        arrays = {f.name: [r.get(f.name) for r in self._records] for f in schema}
        return pa.RecordBatch.from_pydict(arrays, schema=schema)

    def __len__(self) -> int:
        return len(self._records)
```

### Files to Edit

- `src/semantics/diagnostics/issue_batching.py` — use `DiagnosticBatchBuilder`
- `src/semantics/diagnostics/` (all 5 builder modules)
- `src/semantics/diagnostics_emission.py` — use shared builder

### New Files to Create

- `src/semantics/diagnostics/builder_base.py`
- `tests/unit/semantics/diagnostics/test_builder_base.py`

### Legacy Decommission/Delete Scope

- Delete duplicated builder boilerplate from each of the 5 diagnostic builder modules

---

## S18. Leverage Available DataFusion/Delta Capabilities (Version-Grounded)

### Goal

Adopt only capabilities available in the current pinned environment (`datafusion==51.0.0`, `deltalake==1.4.2`), replacing unsupported API assumptions with supported built-ins and existing extension bridges.

### Representative Code Snippets

```python
# 1. DataFusion Python EXPLAIN ANALYZE capture (supported today)
# src/datafusion_engine/plan/profiler.py + bundle_artifact.py
from datafusion_engine.plan.profiler import capture_explain

capture = capture_explain(df, verbose=True, analyze=True)
if capture is not None:
    artifact = {
        "explain_analyze_duration_ms": capture.duration_ms,
        "explain_analyze_output_rows": capture.output_rows,
    }
```

```python
# 2. Runtime metrics via extension snapshot (supported today)
# src/datafusion_engine/extensions/runtime_capabilities.py
from datafusion_engine.extensions.runtime_capabilities import collect_runtime_execution_metrics

metrics_payload = collect_runtime_execution_metrics(ctx)
if metrics_payload is not None:
    emit_diagnostics_event("runtime.execution.metrics", dict(metrics_payload))
```

```python
# 3. Delta selective overwrite via write_deltalake(predicate=...)
# src/datafusion_engine/io/write.py
from deltalake.writer import write_deltalake

write_deltalake(
    table_uri,
    source,
    mode="overwrite",
    schema_mode="merge",
    predicate="partition_key = '2026-02-16'",
)
```

```python
# 4. Prefer Delta TableProvider registration for query paths
# src/datafusion_engine/dataset/registration.py
provider_bundle = delta_provider_from_session(ctx, request=request)
adapter.register_table(name, provider_bundle.provider)
# Avoid register_dataset(...) fallback for Delta when provider is available.
```

### Files to Edit

- `src/datafusion_engine/plan/profiler.py` — ensure explain-analyze capture remains canonical metrics path
- `src/datafusion_engine/plan/bundle_artifact.py` — consume explain capture consistently for diagnostics/artifacts
- `src/datafusion_engine/extensions/runtime_capabilities.py` — standardize runtime metrics snapshot payload shape
- `src/obs/engine_metrics_bridge.py` — consume extension runtime metrics payload
- `src/datafusion_engine/io/write.py` — adopt predicate-based partial overwrite where policies permit
- `src/datafusion_engine/dataset/registration.py` — enforce provider-first registration for Delta tables

### New Files to Create

- `tests/unit/datafusion_engine/extensions/test_runtime_execution_metrics_snapshot.py`
- `tests/unit/datafusion_engine/io/test_replace_where.py`
- `tests/unit/datafusion_engine/dataset/test_delta_provider_registration.py`

### Legacy Decommission/Delete Scope

- Delete any planned Python `ExecutionPlan.metrics()` integrations (not exposed in DataFusion Python 51)
- Delete remaining references to `SchemaAdapter` in plan/code comments; use `PhysicalExprAdapterFactory` terminology
- Delete planned `DeltaDataChecker` usage from Python surfaces (not available in deltalake 1.4.2)

---

## S19. Collapse Delta Control-Plane Positional Commit Payload Duplication

### Goal

Replace repeated `commit_payload_values[0..5]` positional indexing in `delta/control_plane.py` with a named payload helper so Rust FFI call wiring has a single authority.

### Representative Code Snippets

```python
# src/datafusion_engine/delta/commit_payload.py  (NEW)
from __future__ import annotations

from dataclasses import dataclass

@dataclass(frozen=True)
class CommitPayloadParts:
    app_id: str | None
    app_version: int | None
    run_id: str | None
    event_time_unix_ms: int | None
    metadata_json: str | None
    properties_json: str | None
```

```python
# src/datafusion_engine/delta/control_plane.py
def _invoke_with_commit(entrypoint: callable, request: object) -> object:
    parts = commit_payload_parts(commit_payload(request))
    return entrypoint(
        # ... non-commit args ...
        parts.app_id,
        parts.app_version,
        parts.run_id,
        parts.event_time_unix_ms,
        parts.metadata_json,
        parts.properties_json,
    )
```

### Files to Edit

- `src/datafusion_engine/delta/control_plane.py` — replace all positional commit payload indexing with helper calls

### New Files to Create

- `src/datafusion_engine/delta/commit_payload.py`
- `tests/unit/datafusion_engine/delta/test_commit_payload.py`

### Legacy Decommission/Delete Scope

- Delete all `commit_payload_values[0]` through `commit_payload_values[5]` indexing sites in `delta/control_plane.py`
- Delete duplicated per-entrypoint payload-unpack boilerplate superseded by `_invoke_with_commit`

---

## S20. Apply Schema/Introspection Quick-Win Deduplications

### Goal

Land low-risk schema/introspection refactors identified across reviews before broader decomposition to remove duplicate logic and sharpen public boundaries.

### Representative Code Snippets

```python
# src/datafusion_engine/views/graph.py
# BEFORE: local duplicate _schema_from_table(ctx, name)
# AFTER:
from datafusion_engine.schema.introspection import schema_from_table
```

```python
# src/datafusion_engine/schema/introspection.py
def _normalized_name_map(value: object) -> Mapping[str, Sequence[str]]:
    # Canonical parser for both aliases and parameter names
    ...
# Replace _normalized_aliases + _normalized_parameter_names with single helper.
```

```python
# src/datafusion_engine/tables/metadata.py
from dataclasses import replace

updated = replace(metadata, ddl=ddl_text)
```

### Files to Edit

- `src/datafusion_engine/views/graph.py` — remove duplicate `_schema_from_table` helper
- `src/datafusion_engine/schema/introspection.py` — merge duplicate normalization helpers
- `src/datafusion_engine/tables/metadata.py` — replace manual `with_*` field-copy boilerplate with `dataclasses.replace`
- `src/datafusion_engine/schema/__init__.py` — remove underscore-prefixed symbols from public `__all__`

### New Files to Create

- `tests/unit/datafusion_engine/views/test_schema_from_table_usage.py`
- `tests/unit/datafusion_engine/schema/test_introspection_name_normalization.py`
- `tests/unit/datafusion_engine/tables/test_metadata_replace_helpers.py`

### Legacy Decommission/Delete Scope

- Delete `_schema_from_table` from `src/datafusion_engine/views/graph.py`
- Delete `_normalized_aliases` and `_normalized_parameter_names` from `src/datafusion_engine/schema/introspection.py`
- Delete manual copy-on-write `with_*` methods in `src/datafusion_engine/tables/metadata.py` once callsites migrate
- Delete underscore-prefixed public exports from `src/datafusion_engine/schema/__init__.py`

---

## S21. Consolidate Semantics Registry and Diagnostics Surfaces

### Goal

Remove semantics facade indirection and diagnostics contract leaks by making `semantics/registry.py` the single authority and deleting private-cross-module helper usage.

### Representative Code Snippets

```python
# src/semantics/registry.py
@dataclass(frozen=True)
class SemanticSpecIndex:
    name: str
    kind: ViewKindStr
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
```

```python
# src/semantics/diagnostics/_utils.py  (NEW)
from datafusion import SessionContext
import pyarrow as pa

def empty_diagnostic_frame(ctx: SessionContext, schema: pa.Schema):
    return ctx.from_arrow_table(pa.table({field.name: [] for field in schema}))
```

```python
# src/semantics/diagnostics/quality_metrics.py
# BEFORE: build_relationship_decisions_view() alias to candidates + private helper export
# AFTER: single public entrypoint
def build_relationship_candidates_view(ctx: SessionContext) -> DataFrame: ...
```

### Files to Edit

- `src/semantics/spec_registry.py` — fold exports into `registry.py`, then delete facade module
- `src/semantics/registry.py` — host canonical `SemanticSpecIndex` and related exports
- `src/semantics/diagnostics/quality_metrics.py` — remove `_empty_table` private-cross-module pattern and alias-only entrypoint
- `src/semantics/diagnostics/schema_anomalies.py` — import diagnostics utility instead of private helper
- `src/semantics/diagnostics/__init__.py` — update exports to canonical builder names
- `src/semantics/validation/catalog_validation.py` and `src/semantics/validation/__init__.py` — remove dead `SEMANTIC_INPUT_COLUMN_SPECS` public constant

### New Files to Create

- `src/semantics/diagnostics/_utils.py`
- `tests/unit/semantics/test_registry_spec_index.py`
- `tests/unit/semantics/diagnostics/test_diagnostics_utils.py`

### Legacy Decommission/Delete Scope

- Delete `src/semantics/spec_registry.py`
- Delete `_empty_table` from `src/semantics/diagnostics/quality_metrics.py`
- Delete `build_relationship_decisions_view` alias when its behavior is fully subsumed by candidates view
- Delete `SEMANTIC_INPUT_COLUMN_SPECS` constant and export entries

---

## S22. Harden Boundary Typing and Deduplicate Metadata Decode Helpers

### Goal

Eliminate weakly-typed boundary contracts in `relspec` and remove duplicated metadata-decoding logic in `schema_spec`.

### Representative Code Snippets

```python
# src/schema_spec/arrow_types.py (canonical helper)
def decode_metadata_map(metadata: Mapping[bytes, bytes] | None) -> dict[str, str]:
    if not metadata:
        return {}
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in metadata.items()}
```

```python
# src/schema_spec/specs.py
from schema_spec.arrow_types import decode_metadata_map
```

```python
# src/relspec/contracts.py
if TYPE_CHECKING:
    from relspec.task_graph import TaskGraph
    from semantics.ir_core import SemanticIR

class CompileExecutionPolicyRequestV1(msgspec.Struct, frozen=True):
    task_graph: TaskGraph
    semantic_ir: SemanticIR | None = None
```

### Files to Edit

- `src/schema_spec/arrow_types.py` — host canonical metadata decode helper
- `src/schema_spec/specs.py` — consume canonical decode helper and delete duplicate
- `src/relspec/contracts.py` — replace `Any` fields with concrete/TYPE_CHECKING-backed types
- `src/relspec/compiled_policy.py` — replace `Mapping[str, object]` payload fields with typed structs/protocols
- `src/relspec/execution_package.py` — replace `object`+`getattr` hashing pathways with typed protocols

### New Files to Create

- `tests/unit/schema_spec/test_decode_metadata_map.py`
- `tests/unit/relspec/test_compile_execution_policy_contracts.py`
- `tests/unit/relspec/test_compiled_policy_types.py`

### Legacy Decommission/Delete Scope

- Delete `_decode_metadata` from `src/schema_spec/specs.py`
- Delete `Any`-typed fields in `CompileExecutionPolicyRequestV1` superseded by typed contracts
- Delete weak `Mapping[str, object]` boundary fields in `CompiledExecutionPolicy`

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S1, S8)

- Delete the entire re-export hub block in `runtime.py:64–258` — all callers now import from specific sub-modules (S8) and constants come from `_session_identity.py` / `_session_caches.py` (S1).
- Delete the 102-entry `__all__` from `runtime.py`.
- Delete duplicated session helper constants/functions (`CACHE_PROFILES`, `_parse_major_version`, `_DATAFUSION_SQL_ERROR`, `_create_schema_introspector`) superseded by S1 shared modules.

### Batch D2 (after S2, S7, S21, S22)

- Delete all private `_coerce_*` and `_line_offsets` duplicates across the codebase (S2 + S7). The canonical locations are `utils/coercion.py` and `extract/coordination/line_offsets.py`.
- Delete `_decode_metadata` duplication from `schema_spec/specs.py` (canonical helper lives in `schema_spec/arrow_types.py`).
- Delete diagnostics private-cross-module helper usage (`_empty_table`) and dead semantics validation constants superseded by S21.

### Batch D3 (after S3, S6)

- Delete `delta_service_for_profile(None)` call pattern (S6) — all 8 sites now receive injected services.
- Delete the `from cli.config_models import ...` block from `serde_schema_registry.py` (S3) — types now live in `core/config_specs.py`.
- Delete `delta_service_for_profile` factory once all composition roots inject service ports directly.

### Batch D4 (after S4)

- Delete all 73 direct `from obs.otel.*` imports in files outside `src/obs/` — replaced by `from obs.otel import ...`.
- Add and enforce import-boundary tests that fail on any future `obs.otel.*` submodule import outside `src/obs/`.

### Batch D5 (after S9, S10, S11, S12, S13, S14, S15, S16, S19)

- Delete original god files after all contents have been migrated to sub-modules:
  - `dataset/registration.py`
  - `io/write.py`
  - `delta/control_plane.py`
  - `semantics/pipeline.py`
  - `storage/deltalake/delta.py`
  - `plan/artifact_store.py`
  - `udf/extension_runtime.py`
  - `schema_spec/contracts.py`
  - `schema/introspection.py`
  - `extract/extractors/cst_extract.py`
  - `extract/extractors/ast_extract.py`
  - `extract/extractors/bytecode_extract.py`
  - `extract/extractors/tree_sitter/extract.py`

### Batch D6 (after S20, S21, S22)

- Delete `semantics/spec_registry.py` and all import sites.
- Delete `_schema_from_table` duplicate helper from `views/graph.py`.
- Delete `_normalized_aliases` / `_normalized_parameter_names` duplicates from `schema/introspection.py`.
- Delete manual `with_*` copy methods in `tables/metadata.py` after callsite migration to `dataclasses.replace`.
- Delete weakly typed boundary shapes in `relspec` superseded by typed contracts.

---

## Implementation Sequence

1. **S1 — Consolidate session constants and helpers** — Highest-risk correctness/DRY fixes (`_RUNTIME_SESSION_ID`, split caches, duplicated constants).
2. **S2 — Consolidate coercion + env bool parsing helpers** — Fast DRY cleanup touching many files.
3. **S3 — Fix dependency direction violations** — Establishes clean boundaries required by later decomposition work.
4. **S6 — Inject dependencies for Delta services** — Depends on S3 and removes hidden service construction.
5. **S4 — Expand and enforce OTel facade** — Mechanical but broad; best done before large module splits.
6. **S7 — Define ExtractorPort + shared line_offsets** — Small extract-layer boundary wins that reduce churn later.
7. **S19 — Collapse control-plane commit payload positional indexing** — Small, high-value hardening before `control_plane.py` decomposition.
8. **S20 — Schema/introspection quick-win deduplications** — Low-risk reductions before larger schema module splits.
9. **S21 — Semantics registry/diagnostics surface cleanup** — Removes dead aliases and contract leaks before pipeline decomposition.
10. **S22 — Boundary typing + metadata decode dedupe** — Tightens contracts before larger `schema_spec` decomposition.
11. **S5 — Add convenience properties for Law of Demeter hotspots** — Independent, parallelizable ergonomics improvements.
12. **S17 — Extract diagnostic builder framework** — Independent, small-to-medium scope.
13. **S8 — Decompose session re-export hub** — Depends on S1; wide caller migration blast radius.
14. **S12 — Decompose semantics/pipeline.py** — Depends on S21 for cleaner registry surface.
15. **S9 — Decompose dataset registration module** — Largest decomposition scope.
16. **S10 — Decompose io/write module** — Can run alongside S9 after foundational dependency work.
17. **S11 — Decompose delta/control_plane module** — Pair with S19-completed helper extraction.
18. **S13 — Decompose storage Delta module** — Parallel with S9-S11 once boundaries are stable.
19. **S14 — Decompose artifact_store + extension_runtime** — Parallel decomposition block.
20. **S15 — Decompose extractor modules** — Depends on S7 protocol and shared helpers.
21. **S16 — Decompose schema_spec/contracts + schema/introspection** — Depends on S3 and S20/S22 groundwork.
22. **S18 — Leverage available DataFusion/Delta capabilities** — Land after decomposition churn to minimize merge conflicts.

### Parallelization Groups

- **Group A** (sequential, weeks 1-2): S1 → S2 → S3 → S6
- **Group B** (parallel with Group A, weeks 1-2): S4, S7, S19, S20, S21, S22
- **Group C** (parallel with Group B, weeks 2-3): S5, S17
- **Group D** (parallel, weeks 3-5, after Groups A/B): S8, S9, S10, S11, S12, S13, S14, S15, S16
- **Group E** (after Group D stabilization): S18

---

## Implementation Checklist

- [ ] S1. Consolidate session constants and helper definitions (`_RUNTIME_SESSION_ID`, split caches, duplicated runtime constants/helpers)
- [ ] S2. Consolidate type coercion and boolean parsing helpers
- [ ] S3. Fix dependency direction violations (4 confirmed)
- [ ] S4. Expand and enforce OTel facade (73 bypass import lines across 37 files)
- [ ] S5. Add convenience properties for Law of Demeter (25+ chain sites)
- [ ] S6. Inject dependencies to replace hidden service creation (8 sites)
- [ ] S7. Define ExtractorPort protocol and extract shared coordination
- [ ] S8. Decompose session re-export hub (102-entry `__all__`)
- [ ] S9. Decompose `dataset/registration.py` (3,368 LOC)
- [ ] S10. Decompose `io/write.py` (2,689 LOC)
- [ ] S11. Decompose `delta/control_plane.py` (2,070 LOC)
- [ ] S12. Decompose `semantics/pipeline.py` (1,671 LOC)
- [ ] S13. Decompose `storage/deltalake/delta.py` (2,824 LOC)
- [ ] S14. Decompose `artifact_store.py` (1,654 LOC) and `extension_runtime.py` (1,660 LOC)
- [ ] S15. Decompose extractor files (1,388–1,900 LOC each)
- [ ] S16. Decompose `schema_spec/contracts.py` (1,858 LOC) and `schema/introspection.py` (1,305 LOC)
- [ ] S17. Extract diagnostic builder framework
- [ ] S18. Leverage available DataFusion/Delta capabilities (EXPLAIN ANALYZE capture, runtime extension metrics snapshot, provider-first Delta registration, replaceWhere)
- [ ] S19. Collapse positional commit payload indexing in `delta/control_plane.py`
- [ ] S20. Apply schema/introspection quick-win deduplications
- [ ] S21. Consolidate semantics registry and diagnostics surfaces
- [ ] S22. Harden relspec/schema_spec boundary typing and metadata decode helpers
- [ ] D1. Delete re-export hub block from `runtime.py` (after S1 + S8)
- [ ] D2. Delete private coercion/line_offsets/metadata decode/diagnostic helper duplicates (after S2 + S7 + S21 + S22)
- [ ] D3. Delete `delta_service_for_profile(None)` pattern, `cli.config_models` reverse import, and factory shim (after S3 + S6)
- [ ] D4. Delete all direct `obs.otel.*` bypass imports and enforce boundary tests (after S4)
- [ ] D5. Delete original god files after sub-module migration (after S9 + S10 + S11 + S12 + S13 + S14 + S15 + S16 + S19)
- [ ] D6. Delete quick-win legacy surfaces (`spec_registry.py`, `_schema_from_table`, duplicate normalization helpers, manual metadata copy methods) (after S20 + S21 + S22)
