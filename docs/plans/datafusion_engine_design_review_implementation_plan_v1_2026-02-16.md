# DataFusion Engine Design Review Implementation Plan v1 (2026-02-16)

## Scope Summary

This plan addresses **every actionable finding** from the comprehensive design review of `src/datafusion_engine/` (158 files, ~72,675 LOC, 27 subdirectories). Findings originate from 8 parallel design-reviewer agents plus a cross-agent synthesis, documented in 9 review files under `docs/reviews/`.

**Design stance:** Hard cutover. No compatibility shims, no re-exports of moved symbols, no `# removed` comments. When code moves, all callsites update in the same commit. When duplicates consolidate, originals are deleted.

**Scope organization:** 34 scope items in 9 groups, ordered by dependency and risk:
1. **DRY Consolidation** (S1-S6): Eliminate verified duplications and near-duplicates
2. **Global State & Testability** (S7-S9, S30): Remove mutable module state and `id()` keyed caches
3. **Type Safety & Error Semantics** (S10-S12, S21, S31): Typed boundaries and precise exception policy
4. **Monolith Decomposition** (S13-S20): Break 9 files totaling 23,856 LOC into focused modules
5. **Runtime Contracts & API Surface** (S22-S23, S27): Clarify runtime shape and public exports
6. **Protocol & Boundary Cleanup** (S24-S26): Consolidate protocol usage around existing abstractions
7. **Design-Principle Scope Additions** (S32): Remove dead/legacy toggles and YAGNI leftovers
8. **Observability** (S28-S29, S34): Add structured logging, metrics, and tracing acceptance criteria
9. **DataFusion/Delta Built-In Alignment** (S33): Prefer verified built-ins before bespoke utilities

---

## Design Principles

1. **Hard cutover only** — No deprecated re-exports, shims, or compatibility layers. Move code and update all callsites atomically.
2. **One canonical location** — Every helper, constant, and type lives in exactly one module. Imports point there.
3. **Injectable over global** — Module-level mutable state becomes constructor parameters or context attributes.
4. **Protocol over inheritance** — Replace mixin chains and cast() patterns with structural typing.
5. **Domain-scoped modules** — Files exceeding ~800 LOC are decomposed along responsibility boundaries, not arbitrary line counts.
6. **Tests accompany changes** — Every new module gets a test file. Decompositions preserve existing test coverage.

---

## Current Baseline

- **9 monolithic files** total 23,856 LOC: `runtime.py` (8,229), `registration.py` (3,350), `write.py` (2,703), `control_plane.py` (2,297), `extraction_schemas.py` (2,000), `artifact_store.py` (1,654), `extension_runtime.py` (1,596), `observability_schemas.py` (1,330), `metadata.py` (1,262)
- **3 copies** of `_sql_identifier` at `expr/spec.py:193`, `io/write.py:142`, `kernels.py:326`
- **6 copies** of value coercion helpers (`_coerce_int`, `_int_or_none`, `_float_or_none`) across `src/`; canonical location exists at `src/utils/value_coercion.py` with `coerce_int`, `coerce_float`, `raise_for_int`, `raise_for_float`
- **3 DataFrame-to-schema helpers + 2 adjacent schema conversion helpers** currently split across `views/bundle_extraction.py:arrow_schema_from_df`, `dataset/registration.py:_arrow_schema_from_dataframe`, `plan/bundle_artifact.py:_arrow_schema_from_df`, `schema/observability_schemas.py:_arrow_schema_from_dfschema`, `arrow/interop.py:coerce_arrow_schema`
- **2 copies** of `DEFAULT_DICTIONARY_INDEX_TYPE` at `encoding/policy.py:28` and `arrow/types.py:94`
- **2 copies** of `_ENGINE_FUNCTION_REQUIREMENTS` at `extraction_schemas.py:1546` and `observability_schemas.py:406`
- **5 identical** `_chain_*_hooks` functions at `runtime.py:2199-2255`
- **3 identical** `_record_*_registration` functions at `runtime.py:5792-5915`
- **22 near-identical** feature toggle functions at `delta/control_plane.py` (11 enable + 11 disable helpers) plus repeated `gate_payload[...]` extraction blocks
- **8 module-level global registries** (WeakSet/WeakKeyDictionary) in `udf/` blocking testability
- **5 module-level mutable caches** in `dataset/` plus additional mutable global state in `catalog/introspection.py`, `io/adapter.py`, and `views/graph.py` blocking testability
- **0 logging statements** across `arrow/` (5,685 LOC) and `encoding/`
- **PolicyBundleConfig** has 50+ fields spanning 6 unrelated domains
- **4-mixin inheritance chain** in runtime with `cast()` dispatch pattern
- **17 request structs** in `delta/` sharing identical 4-field `table_ref` pattern
- **`globals()`-based discovery** in `extract/templates.py` instead of explicit registry
- `session/runtime.py` contains contradictory identifier normalization flags and duplicated telemetry enrichment blocks
- `session/runtime.py` still has a wide `__all__` surface while `session/config.py` acts as a compatibility re-export shim
- `delta/provider_artifacts.py` and `delta/control_plane.py` use `object` for compatibility payload typing despite `delta/capabilities.py` exposing typed compatibility structs
- `arrow/interop.py` already contains substantial structural protocol surface (`DataTypeLike`, `FieldLike`, `SchemaLike`, and related adapters)
- `src/datafusion_engine/sql/helpers.py` does NOT yet exist (proposed new file)
- `src/datafusion_engine/session/config_structs.py` does NOT yet exist (proposed new file)

---

## S1. SQL & Expression Helper Consolidation

### Goal

Eliminate 3 copies of `_sql_identifier` and consolidate value coercion helpers into canonical shared locations. After this change, exactly one definition exists for each helper and all callsites use `utils.value_coercion`.

### Representative Code Snippets

```python
# src/datafusion_engine/sql/helpers.py (NEW)
from __future__ import annotations


def sql_identifier(name: str) -> str:
    """Quote a SQL identifier with double quotes, escaping embedded quotes.

    Parameters
    ----------
    name
        Raw identifier string.

    Returns
    -------
    str
        Quoted SQL identifier safe for interpolation into SQL text.
    """
    return f'"{name.replace(chr(34), chr(34) + chr(34))}"'
```

```python
# src/datafusion_engine/expr/spec.py — AFTER (import replaces inline def)
from datafusion_engine.sql.helpers import sql_identifier
```

```python
# src/datafusion_engine/io/write.py — AFTER (import replaces inline def)
from datafusion_engine.sql.helpers import sql_identifier
```

```python
# src/datafusion_engine/kernels.py — AFTER (import replaces inline def)
from datafusion_engine.sql.helpers import sql_identifier
```

For value coercion, the canonical location `src/utils/value_coercion.py` already exists. DataFusion-engine callers should import from there:

```python
# In any datafusion_engine module needing coercion:
from utils.value_coercion import coerce_float, coerce_int, raise_for_float, raise_for_int
```

### Files to Edit

- `src/datafusion_engine/expr/spec.py` — Remove `_sql_identifier` def (~line 193), add import
- `src/datafusion_engine/io/write.py` — Remove `_sql_identifier` def (~line 142), add import
- `src/datafusion_engine/kernels.py` — Remove `_sql_identifier` def (~line 326), add import
- Any `src/datafusion_engine/` files with local `_coerce_int`/`_int_or_none`/`_float_or_none` — Replace with imports from `utils.value_coercion`

### New Files to Create

- `src/datafusion_engine/sql/helpers.py` — Canonical `sql_identifier` function
- `tests/unit/datafusion_engine/sql/test_helpers.py` — Tests for sql_identifier edge cases (empty string, embedded quotes, unicode)

### Legacy Decommission/Delete Scope

- Delete `_sql_identifier` function body at `expr/spec.py:193` (superseded by `sql/helpers.py:sql_identifier`)
- Delete `_sql_identifier` function body at `io/write.py:142` (superseded by `sql/helpers.py:sql_identifier`)
- Delete `_sql_identifier` function body at `kernels.py:326` (superseded by `sql/helpers.py:sql_identifier`)
- Delete any local `_coerce_int`/`_int_or_none`/`_float_or_none` definitions within `src/datafusion_engine/` (superseded by `utils.value_coercion`)

---

## S2. Arrow Schema Conversion Consolidation

### Goal

Consolidate schema-conversion helpers around existing `arrow/interop.py` interfaces, remove local dataframe conversion duplicates, and keep one canonical `DEFAULT_DICTIONARY_INDEX_TYPE` constant.

### Representative Code Snippets

```python
# src/datafusion_engine/arrow/interop.py (existing, expanded)
from __future__ import annotations

import pyarrow as pa

from datafusion_engine.arrow.types import DEFAULT_DICTIONARY_INDEX_TYPE

def arrow_schema_from_dataframe(dataframe: object) -> pa.Schema:
    """Canonical entrypoint for dataframe-like -> Arrow schema conversion."""
    if hasattr(dataframe, "schema"):
        return coerce_arrow_schema(getattr(dataframe, "schema"))
    msg = f"Unsupported dataframe schema source: {type(dataframe)!r}"
    raise TypeError(msg)


def arrow_schema_from_dfschema(df_schema: object) -> pa.Schema:
    """Convert DataFusion DFSchema/Schema-like objects to `pa.Schema`."""
    fields: list[pa.Field] = []
    for i in range(df_schema.field_count()):  # type: ignore[attr-defined]
        field = df_schema.field(i)  # type: ignore[attr-defined]
        arrow_type = arrow_type_from_datafusion_type(field.data_type())
        fields.append(pa.field(field.name(), arrow_type, nullable=field.is_nullable()))
    return pa.schema(fields)
```

### Files to Edit

- `src/datafusion_engine/arrow/interop.py` — Add canonical `arrow_schema_from_dataframe`/`arrow_schema_from_dfschema`; keep `coerce_arrow_schema` as root normalizer
- `src/datafusion_engine/views/bundle_extraction.py` — Delete local `arrow_schema_from_df`; import canonical helper from `arrow/interop.py`
- `src/datafusion_engine/dataset/registration.py` — Delete local `_arrow_schema_from_dataframe`; import canonical helper
- `src/datafusion_engine/plan/bundle_artifact.py` — Delete local `_arrow_schema_from_df`; import canonical helper
- `src/datafusion_engine/schema/observability_schemas.py` — Replace local `_arrow_schema_from_dfschema` with canonical helper
- `src/datafusion_engine/encoding/policy.py` — Remove local `DEFAULT_DICTIONARY_INDEX_TYPE`; import from `arrow/types.py`
- `src/datafusion_engine/arrow/types.py` — Retain single canonical `DEFAULT_DICTIONARY_INDEX_TYPE`

### New Files to Create

- `tests/unit/datafusion_engine/arrow/test_schema_conversion.py` — Canonical conversion tests (DataFrame-like, DFSchema-like, malformed inputs)

### Legacy Decommission/Delete Scope

- Delete local `arrow_schema_from_df`/`_arrow_schema_from_dataframe` definitions in `views/bundle_extraction.py`, `dataset/registration.py`, and `plan/bundle_artifact.py`
- Delete local `_arrow_schema_from_dfschema` in `schema/observability_schemas.py`
- Delete duplicate `DEFAULT_DICTIONARY_INDEX_TYPE` in `encoding/policy.py` (canonical constant remains in `arrow/types.py`)

---

## S3. Schema Constants Consolidation

### Goal

Eliminate the duplicated `_ENGINE_FUNCTION_REQUIREMENTS` dictionary and fix schema name reassignment patterns where a schema constant is defined and then overwritten 500+ lines later in the same file.

### Representative Code Snippets

```python
# src/datafusion_engine/schema/constants.py (NEW — or add to existing contracts.py)
from __future__ import annotations

# Single source of truth for engine function requirements
ENGINE_FUNCTION_REQUIREMENTS: dict[str, list[str]] = {
    # ... canonical definition moved from extraction_schemas.py:1546
}
```

```python
# src/datafusion_engine/schema/extraction_schemas.py — AFTER
from datafusion_engine.schema.constants import ENGINE_FUNCTION_REQUIREMENTS
# Delete local _ENGINE_FUNCTION_REQUIREMENTS definition at line 1546
```

```python
# src/datafusion_engine/schema/observability_schemas.py — AFTER
from datafusion_engine.schema.constants import ENGINE_FUNCTION_REQUIREMENTS
# Delete local _ENGINE_FUNCTION_REQUIREMENTS definition at line 406
```

For schema name reassignment, replace:
```python
# BEFORE (anti-pattern): schema defined, then overwritten 500 lines later
AST_FILES_SCHEMA = pa.schema([...])  # line N
# ... 500 lines ...
AST_FILES_SCHEMA = pa.schema([...])  # line N+500 — OVERWRITES

# AFTER: single definition, or versioned names if both are needed
AST_FILES_SCHEMA = pa.schema([...])  # final/canonical definition only
```

### Files to Edit

- `src/datafusion_engine/schema/extraction_schemas.py` — Remove `_ENGINE_FUNCTION_REQUIREMENTS` at ~line 1546; fix any schema name reassignment patterns
- `src/datafusion_engine/schema/observability_schemas.py` — Remove `_ENGINE_FUNCTION_REQUIREMENTS` at ~line 406
- `src/datafusion_engine/schema/contracts.py` — Add `ENGINE_FUNCTION_REQUIREMENTS` if this is the natural home

### New Files to Create

- `tests/unit/datafusion_engine/schema/test_constants.py` — Validate ENGINE_FUNCTION_REQUIREMENTS structure, no duplicate keys

### Legacy Decommission/Delete Scope

- Delete `_ENGINE_FUNCTION_REQUIREMENTS` at `extraction_schemas.py:1546` (superseded by shared constant)
- Delete `_ENGINE_FUNCTION_REQUIREMENTS` at `observability_schemas.py:406` (superseded by shared constant)
- Delete any overwritten schema constant definitions (keep only the final/canonical version)

---

## S4. Runtime Hook & Registration DRY

### Goal

Replace 5 identical `_chain_*_hooks` functions (runtime.py:2199-2255) with one parameterized helper, and 3 identical `_record_*_registration` functions (runtime.py:5792-5915) with one parameterized helper.

### Representative Code Snippets

```python
# src/datafusion_engine/session/hooks.py (NEW — or inline in runtime.py initially)
from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TypeVar

T = TypeVar("T")


def chain_hooks(hooks: Sequence[Callable[[T], T]], initial: T) -> T:
    """Apply a sequence of hook functions in order.

    Parameters
    ----------
    hooks
        Ordered sequence of transformation functions.
    initial
        Starting value to thread through the hook chain.

    Returns
    -------
    T
        Result after all hooks have been applied.
    """
    result = initial
    for hook in hooks:
        result = hook(result)
    return result


def record_registration(
    registry: dict[str, object],
    key: str,
    value: object,
    *,
    category: str,
    allow_override: bool = False,
) -> None:
    """Record a registration in a registry with optional override protection.

    Parameters
    ----------
    registry
        Target registry dict.
    key
        Registration key.
    value
        Value to register.
    category
        Human-readable category for error messages.
    allow_override
        If False, raise on duplicate key.
    """
    if not allow_override and key in registry:
        msg = f"Duplicate {category} registration: {key!r}"
        raise ValueError(msg)
    registry[key] = value
```

### Files to Edit

- `src/datafusion_engine/session/runtime.py` — Replace 5 `_chain_*_hooks` functions (~lines 2199-2255) with calls to `chain_hooks`; replace 3 `_record_*_registration` functions (~lines 5792-5915) with calls to `record_registration`

### New Files to Create

- `src/datafusion_engine/session/hooks.py` — `chain_hooks` and `record_registration` helpers
- `tests/unit/datafusion_engine/session/test_hooks.py` — Tests for hook chaining (empty, single, multiple, error propagation) and registration (happy path, duplicate, override)

### Legacy Decommission/Delete Scope

- Delete `_chain_pre_hooks` at `runtime.py:~2199` (superseded by `chain_hooks`)
- Delete `_chain_post_hooks` at `runtime.py:~2210` (superseded by `chain_hooks`)
- Delete `_chain_validation_hooks` at `runtime.py:~2221` (superseded by `chain_hooks`)
- Delete `_chain_transform_hooks` at `runtime.py:~2232` (superseded by `chain_hooks`)
- Delete `_chain_filter_hooks` at `runtime.py:~2243` (superseded by `chain_hooks`)
- Delete `_record_udf_registration` at `runtime.py:~5792` (superseded by `record_registration`)
- Delete `_record_table_registration` at `runtime.py:~5853` (superseded by `record_registration`)
- Delete `_record_provider_registration` at `runtime.py:~5915` (superseded by `record_registration`)

---

## S5. Delta DRY Consolidation

### Goal

Replace 22 near-identical feature toggle functions in `control_plane.py` (11 enable + 11 disable), extract the shared 4-field `table_ref` pattern from 17 request structs, and consolidate repeated `gate_payload` extraction plus Rust entrypoint invocation glue.

### Representative Code Snippets

```python
# src/datafusion_engine/delta/feature_toggles.py (NEW)
from __future__ import annotations

from typing import Any

import msgspec


class FeatureToggleSpec(msgspec.Struct, frozen=True):
    name: str
    default: bool = False


_FEATURE_TOGGLE_REGISTRY: dict[str, FeatureToggleSpec] = {
    "auto_compaction": FeatureToggleSpec("delta.auto_compaction"),
    "optimize_write": FeatureToggleSpec("delta.optimize_write"),
    "checkpointing": FeatureToggleSpec("delta.checkpointing", default=True),
}


def resolve_feature_toggle(config: Any, toggle_name: str) -> bool:
    spec = _FEATURE_TOGGLE_REGISTRY[toggle_name]
    parts = spec.name.split(".")
    current = config
    for part in parts:
        current = getattr(current, part, None)
        if current is None:
            return spec.default
    return bool(current)
```

```python
# src/datafusion_engine/delta/contracts.py — table_ref mixin
from __future__ import annotations

import msgspec


class TableRefMixin(msgspec.Struct, frozen=True):
    catalog: str
    schema_name: str
    table_name: str
    table_uri: str | None = None
```

```python
# src/datafusion_engine/delta/control_plane.py — shared Rust invocation path
from __future__ import annotations

from typing import Any

from datafusion_engine.delta.gate import extract_gate_payload


def _invoke_rust_entrypoint(
    self,
    operation: str,
    raw_payload: dict[str, Any],
) -> object:
    gate = extract_gate_payload(raw_payload)
    rust_api = self._rust_client()
    return rust_api.invoke(operation, gate)
```

### Files to Edit

- `src/datafusion_engine/delta/control_plane.py` — Replace 22 feature toggle functions with registry-based `resolve_feature_toggle`; route repeated operation glue through `_invoke_rust_entrypoint`; replace repeated gate payload extraction
- `src/datafusion_engine/delta/contracts.py` — Add `TableRefMixin`; update 17 request structs to inherit from it
- `src/datafusion_engine/delta/service.py` — Update to use `extract_gate_payload` where applicable
- `src/datafusion_engine/delta/maintenance.py` — Update to use `extract_gate_payload` where applicable

### New Files to Create

- `src/datafusion_engine/delta/feature_toggles.py` — Registry-driven `resolve_feature_toggle`
- `src/datafusion_engine/delta/gate.py` — `extract_gate_payload` helper
- `tests/unit/datafusion_engine/delta/test_feature_toggles.py` — Tests for toggle resolution and `_FEATURE_TOGGLE_REGISTRY` completeness
- `tests/unit/datafusion_engine/delta/test_gate.py` — Tests for payload extraction
- `tests/unit/datafusion_engine/delta/test_control_plane_invocation.py` — Verifies `_invoke_rust_entrypoint` and shared control-flow behavior

### Legacy Decommission/Delete Scope

- Delete 22 feature toggle wrappers in `control_plane.py` (superseded by `_FEATURE_TOGGLE_REGISTRY` + `resolve_feature_toggle`)
- Delete duplicated `table_ref` field declarations in 17 request structs (superseded by `TableRefMixin` inheritance)
- Delete repeated inline gate payload extraction and repeated Rust invocation glue (superseded by `extract_gate_payload` + `_invoke_rust_entrypoint`)

---

## S6. UDF Constants & DRY Consolidation

### Goal

Consolidate 11+ duplicated ABI error message strings and 7+ duplicated extension module name references in `udf/` into canonical constants.

### Representative Code Snippets

```python
# src/datafusion_engine/udf/constants.py (NEW)
from __future__ import annotations

# ABI compatibility error messages
ABI_VERSION_MISMATCH_MSG = (
    "Extension ABI version mismatch: expected {expected}, got {actual}. "
    "Rebuild the extension against the current runtime."
)
ABI_LOAD_FAILURE_MSG = (
    "Failed to load extension module {module!r}: {error}"
)

# Extension module naming
EXTENSION_MODULE_PREFIX = "codeanatomy_ext_"
EXTENSION_ENTRY_POINT = "register"
```

### Files to Edit

- `src/datafusion_engine/udf/extension_runtime.py` — Replace inline ABI error strings with constants from `udf/constants.py`; replace module name literals with `EXTENSION_MODULE_PREFIX`
- `src/datafusion_engine/udf/factory.py` — Replace duplicated module name references
- `src/datafusion_engine/udf/platform.py` — Replace duplicated module name references
- `src/datafusion_engine/udf/metadata.py` — Replace duplicated ABI error strings
- `src/datafusion_engine/udf/parity.py` — Replace duplicated references

### New Files to Create

- `src/datafusion_engine/udf/constants.py` — Canonical ABI messages and module naming constants
- `tests/unit/datafusion_engine/udf/test_constants.py` — Validate constant format strings have expected placeholders

### Legacy Decommission/Delete Scope

- Delete all 11+ inline ABI error message string literals across `udf/` (superseded by constants in `udf/constants.py`)
- Delete all 7+ inline extension module name literals across `udf/` (superseded by `EXTENSION_MODULE_PREFIX`)

---

## S7. UDF Global Registry Injection

### Goal

Replace module-level global registries (WeakSet, WeakKeyDictionary, plain dict) in `udf/` with constructor-injected or context-provided registries, including globals currently split between `extension_runtime.py` and `platform.py`.

### Representative Code Snippets

```python
# BEFORE (udf/extension_runtime.py) — module-level globals
_LOADED_EXTENSIONS: WeakSet[object] = WeakSet()
_EXTENSION_METADATA: WeakKeyDictionary[object, dict] = WeakKeyDictionary()
_REGISTERED_UDFS: dict[str, object] = {}

class ExtensionRuntime:
    def load(self, module: object) -> None:
        _LOADED_EXTENSIONS.add(module)  # global mutation
```

```python
# AFTER — injectable via constructor
from __future__ import annotations

from weakref import WeakKeyDictionary, WeakSet


class ExtensionRegistries:
    """Container for extension-related registries, injectable for testing."""

    def __init__(self) -> None:
        self.loaded_extensions: WeakSet[object] = WeakSet()
        self.extension_metadata: WeakKeyDictionary[object, dict[str, object]] = (
            WeakKeyDictionary()
        )
        self.registered_udfs: dict[str, object] = {}


class ExtensionRuntime:
    def __init__(self, registries: ExtensionRegistries | None = None) -> None:
        self._registries = registries or ExtensionRegistries()

    def load(self, module: object) -> None:
        self._registries.loaded_extensions.add(module)
```

### Files to Edit

- `src/datafusion_engine/udf/extension_runtime.py` — Move global registries into `ExtensionRegistries`; accept via constructor
- `src/datafusion_engine/udf/platform.py` — Remove module-level weak registry globals and route through injected state
- `src/datafusion_engine/udf/factory.py` — Thread registries from runtime
- `src/datafusion_engine/udf/metadata.py` — Thread registries from runtime
- All callers that currently access module-level globals directly

### New Files to Create

- `tests/unit/datafusion_engine/udf/test_extension_registries.py` — Isolated registry tests (no global pollution between tests)

### Legacy Decommission/Delete Scope

- Delete module-level global registry variables from `udf/extension_runtime.py` and `udf/platform.py` (superseded by `ExtensionRegistries` instance)

---

## S8. Dataset Mutable Cache Injection

### Goal

Replace module-level mutable caches and identity-keyed state in `dataset/` with injectable alternatives, enabling isolated testing and preventing cross-test contamination.

### Representative Code Snippets

```python
# BEFORE (dataset/registration.py) — module-level mutable caches
_SCHEMA_CACHE: dict[str, pa.Schema] = {}
_REGISTRATION_CACHE: dict[str, bool] = {}

# AFTER — injectable cache container
class DatasetCaches:
    """Injectable container for dataset caches."""

    def __init__(self) -> None:
        self.schema_cache: dict[str, pa.Schema] = {}
        self.registration_cache: dict[str, bool] = {}
        # ... remaining caches


class DatasetRegistration:
    def __init__(self, caches: DatasetCaches | None = None) -> None:
        self._caches = caches or DatasetCaches()
```

### Files to Edit

- `src/datafusion_engine/dataset/registration.py` — Move globals into `DatasetCaches`; accept via constructor
- `src/datafusion_engine/dataset/registry.py` — Thread caches
- `src/datafusion_engine/dataset/resolution.py` — Thread caches
- `src/datafusion_engine/catalog/introspection.py` — Replace `id(ctx)` keyed cache map with context-scoped cache object
- `src/datafusion_engine/views/graph.py` — Remove `_resolver_identity_guard` global and inject via resolver context
- All callers that access module-level caches directly

### New Files to Create

- `tests/unit/datafusion_engine/dataset/test_dataset_caches.py` — Isolated cache tests

### Legacy Decommission/Delete Scope

- Delete module-level mutable cache variables and `id()` keyed cache maps in dataset/catalog/view call paths (superseded by injected cache containers)

---

## S9. Extract Template Explicit Registry

### Goal

Replace `globals()`-based discovery in `extract/templates.py` with an explicit registry, eliminating implicit coupling to module namespace contents.

### Representative Code Snippets

```python
# BEFORE (extract/templates.py) — globals()-based discovery
def get_template(name: str) -> Template:
    templates = {k: v for k, v in globals().items() if isinstance(v, Template)}
    return templates[name]

# AFTER — explicit registry
from __future__ import annotations

from typing import Final

_TEMPLATE_REGISTRY: Final[dict[str, Template]] = {}


def register_template(name: str, template: Template) -> None:
    """Register a named extraction template."""
    if name in _TEMPLATE_REGISTRY:
        msg = f"Duplicate template registration: {name!r}"
        raise ValueError(msg)
    _TEMPLATE_REGISTRY[name] = template


def get_template(name: str) -> Template:
    """Retrieve a registered extraction template by name."""
    if name not in _TEMPLATE_REGISTRY:
        msg = f"Unknown template: {name!r}. Available: {sorted(_TEMPLATE_REGISTRY)}"
        raise KeyError(msg)
    return _TEMPLATE_REGISTRY[name]


# At module level — explicit registrations:
register_template("base_extraction", BASE_EXTRACTION_TEMPLATE)
register_template("function_extraction", FUNCTION_EXTRACTION_TEMPLATE)
# ... etc.
```

### Files to Edit

- `src/datafusion_engine/extract/templates.py` — Replace `globals()` discovery with explicit registry
- `src/datafusion_engine/extract/registry.py` — Update if it depends on template discovery

### New Files to Create

- `tests/unit/datafusion_engine/extract/test_template_registry.py` — Registry round-trip, duplicate detection, missing key error

### Legacy Decommission/Delete Scope

- Delete `globals()`-based template discovery logic in `extract/templates.py` (superseded by explicit registry)

---

## S10. Typed Rust FFI Boundaries

### Goal

Replace untyped `object` boundaries at the Rust FFI in `delta/` with typed Protocols, enabling static type checking across the Python-Rust boundary.

### Representative Code Snippets

```python
# src/datafusion_engine/delta/protocols.py (NEW)
from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class RustDeltaTableHandle(Protocol):
    """Protocol for Rust-provided delta table handles."""

    def version(self) -> int: ...
    def table_uri(self) -> str: ...
    def schema(self) -> object: ...  # DeltaLake Arrow schema
    def files(self) -> list[str]: ...


@runtime_checkable
class RustTransactionHandle(Protocol):
    """Protocol for Rust-provided transaction handles."""

    def commit(self) -> int: ...
    def abort(self) -> None: ...
```

```python
# src/datafusion_engine/delta/control_plane.py — AFTER
# Replace: def open_table(self, ...) -> object:
# With:    def open_table(self, ...) -> RustDeltaTableHandle:
from datafusion_engine.delta.protocols import RustDeltaTableHandle
```

### Files to Edit

- `src/datafusion_engine/delta/control_plane.py` — Replace `object` return types with Protocol types
- `src/datafusion_engine/delta/service.py` — Replace `object` parameter types with Protocol types
- `src/datafusion_engine/delta/protocol.py` — Align with typed FFI Protocols
- `src/datafusion_engine/delta/maintenance.py` — Replace `object` types

### New Files to Create

- `src/datafusion_engine/delta/protocols.py` — FFI Protocol definitions
- `tests/unit/datafusion_engine/delta/test_protocols.py` — Protocol structural conformance tests

### Legacy Decommission/Delete Scope

- Replace all `object` type annotations at Rust FFI boundaries in `delta/` with Protocol types (no code deletion, but type signature changes throughout)

---

## S11. Exception Handling Hardening

### Goal

Replace unsafe broad `except Exception` catches with specific exception types and add structured error context, while preserving intentional broad catches that perform rollback/cleanup and re-raise domain errors.

### Representative Code Snippets

```python
# BEFORE (unsafe broad catch)
try:
    result = some_operation()
except Exception:
    logger.warning("Operation failed")

# AFTER — specific exceptions where possible; wrap with domain error
from datafusion_engine.errors import DeltaObservabilityError

try:
    result = some_operation()
except (OSError, ValueError) as exc:
    logger.warning("Operation failed: %s", exc, exc_info=True)
    raise DeltaObservabilityError(
        f"Failed to collect observability data: {exc}"
    ) from exc

# ACCEPTABLE broad catch pattern (rollback + re-raise)
try:
    write_result = writer.commit()
except Exception as exc:  # intentionally broad: backend exception surface is dynamic
    writer.rollback()
    raise DeltaWriteCommitError("Commit failed after rollback") from exc
```

### Files to Edit

- `src/datafusion_engine/delta/observability.py` — Replace unsafe broad catches (including ~line 790) with specific exceptions and structured context
- `src/datafusion_engine/errors.py` — Add/align domain-specific exception classes if missing
- `src/datafusion_engine/session/facade.py` — Audit broad catches and keep only rollback/re-raise patterns
- `src/datafusion_engine/registry_facade.py` — Audit broad catches and keep only rollback/re-raise patterns
- `src/datafusion_engine/io/write.py` — Audit broad catches and keep only rollback/re-raise patterns
- `src/datafusion_engine/plan/bundle_artifact.py` — Audit broad catches and keep only rollback/re-raise patterns

### New Files to Create

- None (use existing `errors.py`)

### Legacy Decommission/Delete Scope

- Delete unsafe catch-all handlers that only log/suppress; keep and document intentional broad handlers that rollback/cleanup and re-raise domain errors

---

## S12. Protocol-Based Dispatch

### Goal

Replace the `cast()` dispatch pattern in runtime's 4-mixin inheritance chain with Protocol-based structural typing, and replace `__getattr__` magic on `DiagnosticsRecorderAdapter` with explicit delegation.

### Representative Code Snippets

```python
# BEFORE (runtime.py) — cast() pattern with mixin inheritance
class RuntimeBase(MixinA, MixinB, MixinC, MixinD):
    def do_something(self) -> None:
        mixin_a = cast(MixinA, self)
        mixin_a.mixin_a_method()

# AFTER — Protocol-based composition
from __future__ import annotations

from typing import Protocol


class SessionLifecycle(Protocol):
    """Protocol for session lifecycle operations."""

    def create_session(self) -> object: ...
    def close_session(self, session_id: str) -> None: ...


class QueryExecution(Protocol):
    """Protocol for query execution operations."""

    def execute_query(self, sql: str) -> object: ...
```

```python
# BEFORE (lineage/diagnostics.py) — __getattr__ magic
class DiagnosticsRecorderAdapter:
    def __getattr__(self, name: str) -> object:
        return getattr(self._inner, name)

# AFTER — explicit delegation
class DiagnosticsRecorderAdapter:
    def record_plan(self, plan: object) -> None:
        self._inner.record_plan(plan)

    def record_error(self, error: Exception) -> None:
        self._inner.record_error(error)
    # ... explicit methods for each delegated operation
```

### Files to Edit

- `src/datafusion_engine/session/runtime.py` — Replace `cast()` calls with Protocol-typed access
- `src/datafusion_engine/lineage/diagnostics.py` — Replace `__getattr__` with explicit delegation methods

### New Files to Create

- `src/datafusion_engine/session/protocols.py` — Runtime Protocol definitions (if not created in S22)
- `tests/unit/datafusion_engine/session/test_protocols.py` — Protocol conformance tests

### Legacy Decommission/Delete Scope

- Delete `cast()` import and all `cast(MixinX, self)` calls in `runtime.py` (superseded by Protocol-based access)
- Delete `__getattr__` method from `DiagnosticsRecorderAdapter` in `lineage/diagnostics.py` (superseded by explicit delegation)

---

## S13. Runtime.py Decomposition

### Goal

Decompose `session/runtime.py` (8,229 LOC, 20+ classes, 60+ exports, 6+ concerns) into 5-6 focused modules, each under ~1,500 LOC with a single clear responsibility.

### Representative Code Snippets

```python
# src/datafusion_engine/session/runtime.py — AFTER (facade only, ~500 LOC)
from __future__ import annotations

from datafusion_engine.session.config_structs import RuntimeProfileConfig
from datafusion_engine.session.diagnostics import DiagnosticsMixin
from datafusion_engine.session.hooks import chain_hooks
from datafusion_engine.session.lifecycle import SessionLifecycleManager
from datafusion_engine.session.profiles import RuntimeProfileManager
from datafusion_engine.session.registration import RegistrationManager


class DataFusionRuntime:
    """Facade coordinating session lifecycle, registration, and diagnostics."""

    def __init__(
        self,
        config: RuntimeProfileConfig,
        lifecycle: SessionLifecycleManager | None = None,
        registration: RegistrationManager | None = None,
        diagnostics: DiagnosticsMixin | None = None,
    ) -> None:
        self._config = config
        self._lifecycle = lifecycle or SessionLifecycleManager(config)
        self._registration = registration or RegistrationManager(config)
        self._diagnostics = diagnostics or DiagnosticsMixin()
```

Proposed module split:

| New Module | Responsibility | Approx Source Lines |
|---|---|---|
| `session/config_structs.py` | Config dataclasses (lines 3730-4013) | ~300 |
| `session/hooks.py` | Hook chaining + registration helpers (from S4) | ~150 |
| `session/diagnostics.py` | Diagnostics mixin (lines 2948-3369) | ~450 |
| `session/profiles.py` | RuntimeProfile management | ~1,500 |
| `session/lifecycle.py` | Session create/close/pool lifecycle | ~1,500 |
| `session/registration.py` | UDF/table/provider registration | ~1,500 |
| `session/runtime.py` | Facade (imports + delegates) | ~500 |

### Files to Edit

- `src/datafusion_engine/session/runtime.py` — Extract code blocks to new modules; reduce to facade
- All files importing moved symbols from `session/runtime.py` — Update imports to point at final modules in the same commit (no compatibility re-exports)

### New Files to Create

- `src/datafusion_engine/session/config_structs.py` — Config dataclasses extracted from runtime.py
- `src/datafusion_engine/session/diagnostics.py` — Diagnostics mixin extracted from runtime.py
- `src/datafusion_engine/session/profiles.py` — Profile management extracted from runtime.py
- `src/datafusion_engine/session/lifecycle.py` — Session lifecycle extracted from runtime.py
- `src/datafusion_engine/session/registration.py` — Registration logic extracted from runtime.py
- `tests/unit/datafusion_engine/session/test_config_structs.py`
- `tests/unit/datafusion_engine/session/test_diagnostics.py`
- `tests/unit/datafusion_engine/session/test_profiles.py`
- `tests/unit/datafusion_engine/session/test_lifecycle.py`
- `tests/unit/datafusion_engine/session/test_registration.py`

### Legacy Decommission/Delete Scope

- Move (not copy) ~7,700 lines from `runtime.py` to the 5 new modules
- `runtime.py` shrinks from 8,229 LOC to ~500 LOC facade with only canonical runtime entrypoints

---

## S14. Registration.py Decomposition

### Goal

Decompose `dataset/registration.py` (3,350 LOC, scored 0/3 on SRP) into 3 focused modules.

### Representative Code Snippets

```python
# Proposed split:
# dataset/registration.py → dataset/schema_registration.py (schema ops)
#                          → dataset/table_registration.py (table/view registration)
#                          → dataset/registration.py (facade, ~300 LOC)
```

### Files to Edit

- `src/datafusion_engine/dataset/registration.py` — Extract to sub-modules
- All callers importing from `dataset/registration.py`

### New Files to Create

- `src/datafusion_engine/dataset/schema_registration.py` — Schema registration operations
- `src/datafusion_engine/dataset/table_registration.py` — Table/view registration operations
- `tests/unit/datafusion_engine/dataset/test_schema_registration.py`
- `tests/unit/datafusion_engine/dataset/test_table_registration.py`

### Legacy Decommission/Delete Scope

- Move ~3,050 lines from `registration.py` to 2 new modules; `registration.py` shrinks to ~300 LOC facade

---

## S15. Write.py Decomposition

### Goal

Decompose `io/write.py` (2,703 LOC, 6 responsibilities) into focused modules for write planning, execution, validation, and format handling.

### Representative Code Snippets

```python
# Proposed split:
# io/write.py → io/write_planning.py (query plan construction)
#             → io/write_execution.py (actual write execution)
#             → io/write_validation.py (schema validation, constraint checks)
#             → io/write_formats.py (format-specific handlers: parquet, delta, csv)
#             → io/write.py (facade, ~300 LOC)
```

### Files to Edit

- `src/datafusion_engine/io/write.py` — Extract to sub-modules
- All callers importing from `io/write.py`

### New Files to Create

- `src/datafusion_engine/io/write_planning.py` — Write plan construction
- `src/datafusion_engine/io/write_execution.py` — Write execution engine
- `src/datafusion_engine/io/write_validation.py` — Schema/constraint validation
- `src/datafusion_engine/io/write_formats.py` — Format-specific handlers
- `tests/unit/datafusion_engine/io/test_write_planning.py`
- `tests/unit/datafusion_engine/io/test_write_execution.py`
- `tests/unit/datafusion_engine/io/test_write_validation.py`
- `tests/unit/datafusion_engine/io/test_write_formats.py`

### Legacy Decommission/Delete Scope

- Move ~2,400 lines from `write.py` to 4 new modules; `write.py` shrinks to ~300 LOC facade

---

## S16. Control_plane.py Decomposition

### Goal

Decompose `delta/control_plane.py` (2,297 LOC) into focused modules for table management, transaction handling, and configuration.

### Representative Code Snippets

```python
# Proposed split:
# delta/control_plane.py → delta/table_management.py (open/create/alter tables)
#                        → delta/transactions.py (commit/rollback/conflict resolution)
#                        → delta/control_config.py (configuration + feature toggles)
#                        → delta/control_plane.py (facade, ~300 LOC)
```

### Files to Edit

- `src/datafusion_engine/delta/control_plane.py` — Extract to sub-modules
- All callers importing from `delta/control_plane.py`

### New Files to Create

- `src/datafusion_engine/delta/table_management.py` — Table lifecycle operations
- `src/datafusion_engine/delta/transactions.py` — Transaction handling
- `src/datafusion_engine/delta/control_config.py` — Control plane configuration
- `tests/unit/datafusion_engine/delta/test_table_management.py`
- `tests/unit/datafusion_engine/delta/test_transactions.py`
- `tests/unit/datafusion_engine/delta/test_control_config.py`

### Legacy Decommission/Delete Scope

- Move ~2,000 lines from `control_plane.py` to 3 new modules; `control_plane.py` shrinks to ~300 LOC facade

---

## S17. Metadata.py Decomposition

### Goal

Decompose `arrow/metadata.py` (1,262 LOC, 47 exports, 6 conflated concerns) into focused modules for metadata reading, writing, codec operations, and schema metadata.

### Representative Code Snippets

```python
# Proposed split:
# arrow/metadata.py → arrow/metadata_read.py (metadata extraction from Arrow tables)
#                   → arrow/metadata_write.py (metadata attachment to Arrow tables)
#                   → arrow/metadata_schema.py (schema-level metadata operations)
#                   → arrow/metadata.py (facade re-exporting, ~200 LOC)
```

### Files to Edit

- `src/datafusion_engine/arrow/metadata.py` — Extract to sub-modules
- All callers importing from `arrow/metadata.py`
- `src/datafusion_engine/arrow/metadata_codec.py` — Verify clean boundary with new modules

### New Files to Create

- `src/datafusion_engine/arrow/metadata_read.py` — Metadata extraction operations
- `src/datafusion_engine/arrow/metadata_write.py` — Metadata attachment operations
- `src/datafusion_engine/arrow/metadata_schema.py` — Schema-level metadata
- `tests/unit/datafusion_engine/arrow/test_metadata_read.py`
- `tests/unit/datafusion_engine/arrow/test_metadata_write.py`
- `tests/unit/datafusion_engine/arrow/test_metadata_schema.py`

### Legacy Decommission/Delete Scope

- Move ~1,060 lines from `metadata.py` to 3 new modules; `metadata.py` shrinks to ~200 LOC facade

---

## S18. Artifact_store.py Decomposition

### Goal

Decompose `plan/artifact_store.py` (1,654 LOC) which conflates persistence, validation, serialization, and events into focused modules.

### Representative Code Snippets

```python
# Proposed split:
# plan/artifact_store.py → plan/artifact_persistence.py (storage read/write)
#                        → plan/artifact_validation.py (integrity checks)
#                        → plan/artifact_serialization.py (format conversion)
#                        → plan/artifact_store.py (facade, ~300 LOC)
```

### Files to Edit

- `src/datafusion_engine/plan/artifact_store.py` — Extract to sub-modules
- All callers importing from `plan/artifact_store.py`

### New Files to Create

- `src/datafusion_engine/plan/artifact_persistence.py` — Storage operations
- `src/datafusion_engine/plan/artifact_validation.py` — Validation logic
- `src/datafusion_engine/plan/artifact_serialization.py` — Serialization/format conversion
- `tests/unit/datafusion_engine/plan/test_artifact_persistence.py`
- `tests/unit/datafusion_engine/plan/test_artifact_validation.py`
- `tests/unit/datafusion_engine/plan/test_artifact_serialization.py`

### Legacy Decommission/Delete Scope

- Move ~1,350 lines from `artifact_store.py` to 3 new modules; `artifact_store.py` shrinks to ~300 LOC facade

---

## S19. Extension_runtime.py Decomposition

### Goal

Decompose `udf/extension_runtime.py` (1,596 LOC, 5+ responsibilities) into focused modules for extension loading, lifecycle, and validation.

### Representative Code Snippets

```python
# Proposed split:
# udf/extension_runtime.py → udf/extension_loader.py (module discovery + loading)
#                           → udf/extension_lifecycle.py (init/shutdown/health)
#                           → udf/extension_validation.py (ABI checks, compatibility)
#                           → udf/extension_runtime.py (facade, ~300 LOC)
```

### Files to Edit

- `src/datafusion_engine/udf/extension_runtime.py` — Extract to sub-modules
- All callers importing from `udf/extension_runtime.py`

### New Files to Create

- `src/datafusion_engine/udf/extension_loader.py` — Module loading logic
- `src/datafusion_engine/udf/extension_lifecycle.py` — Lifecycle management
- `src/datafusion_engine/udf/extension_validation.py` — ABI/compatibility validation
- `tests/unit/datafusion_engine/udf/test_extension_loader.py`
- `tests/unit/datafusion_engine/udf/test_extension_lifecycle.py`
- `tests/unit/datafusion_engine/udf/test_extension_validation.py`

### Legacy Decommission/Delete Scope

- Move ~1,300 lines from `extension_runtime.py` to 3 new modules; `extension_runtime.py` shrinks to ~300 LOC facade

---

## S20. Schema File Decomposition

### Goal

Decompose `schema/extraction_schemas.py` (2,000 LOC) and `schema/observability_schemas.py` (1,330 LOC, mixing 400 lines schemas + 930 lines validation) into domain-scoped schema files and separate validation modules.

### Representative Code Snippets

```python
# Proposed split for observability_schemas.py:
# schema/observability_schemas.py → schema/observability_schemas.py (schemas only, ~400 LOC)
#                                 → schema/observability_validation.py (validation, ~930 LOC)

# Proposed split for extraction_schemas.py:
# schema/extraction_schemas.py → schema/extraction_schemas_core.py (core schemas)
#                              → schema/extraction_schemas_extended.py (extended schemas)
#                              → schema/extraction_schemas.py (facade)
```

### Files to Edit

- `src/datafusion_engine/schema/extraction_schemas.py` — Split into domain-scoped files
- `src/datafusion_engine/schema/observability_schemas.py` — Extract validation into separate module
- All callers importing from these modules

### New Files to Create

- `src/datafusion_engine/schema/observability_validation.py` — Validation logic extracted from observability_schemas
- `src/datafusion_engine/schema/extraction_schemas_core.py` — Core extraction schemas
- `src/datafusion_engine/schema/extraction_schemas_extended.py` — Extended extraction schemas
- `tests/unit/datafusion_engine/schema/test_observability_validation.py`
- `tests/unit/datafusion_engine/schema/test_extraction_schemas_core.py`

### Legacy Decommission/Delete Scope

- Move ~930 lines of validation from `observability_schemas.py` to `observability_validation.py`
- Move ~1,000 lines from `extraction_schemas.py` to domain-scoped files

---

## S21. Runtime Identifier & Telemetry Contract Cleanup

### Goal

Eliminate contradictory identifier-normalization flags and duplicated telemetry enrichment paths in `session/runtime.py` by introducing explicit runtime contracts and one canonical telemetry enrichment helper.

### Representative Code Snippets

```python
# src/datafusion_engine/session/contracts.py (NEW)
from __future__ import annotations

from enum import Enum
import msgspec


class IdentifierNormalizationMode(str, Enum):
    RAW = "raw"
    SQL_SAFE = "sql_safe"
    STRICT = "strict"


class TelemetryEnrichmentPolicy(msgspec.Struct, frozen=True):
    include_query_text: bool = False
    include_plan_hash: bool = True
    include_profile_name: bool = True
```

```python
# src/datafusion_engine/session/runtime.py — AFTER
from datafusion_engine.session.contracts import (
    IdentifierNormalizationMode,
    TelemetryEnrichmentPolicy,
)


def _enrich_query_telemetry(
    payload: dict[str, object],
    *,
    policy: TelemetryEnrichmentPolicy,
) -> dict[str, object]:
    """Single canonical telemetry enrichment path."""
    enriched = dict(payload)
    if policy.include_plan_hash:
        enriched["plan_hash"] = payload.get("plan_hash")
    return enriched
```

### Files to Edit

- `src/datafusion_engine/session/runtime.py` — Replace contradictory normalization booleans with `IdentifierNormalizationMode`; deduplicate telemetry enrichment blocks
- `src/datafusion_engine/session/config_structs.py` — Add strongly typed runtime config fields for normalization and telemetry policy
- `src/datafusion_engine/session/contracts.py` — Add runtime enums/struct contracts

### New Files to Create

- `src/datafusion_engine/session/contracts.py` — Runtime contracts (`IdentifierNormalizationMode`, telemetry policy structs)
- `tests/unit/datafusion_engine/session/test_runtime_contracts.py` — Contract parsing and behavior tests

### Legacy Decommission/Delete Scope

- Delete duplicated telemetry enrichment blocks in `session/runtime.py` (observed around lines ~3049 and ~3236)
- Delete contradictory identifier normalization flag combinations in `session/runtime.py` (observed around lines ~3878-3879)

---

## S22. PolicyBundleConfig Decomposition

### Goal

Decompose `PolicyBundleConfig` (50+ fields spanning 6 unrelated domains) into domain-scoped config structs, each with 5-12 fields and a clear responsibility boundary.

### Representative Code Snippets

```python
# src/datafusion_engine/session/config_structs.py (NEW or expanded from S13)
from __future__ import annotations

import msgspec


class SessionPoolConfig(msgspec.Struct, frozen=True):
    """Configuration for session pool behavior."""

    max_sessions: int = 16
    idle_timeout_s: float = 300.0
    warmup_count: int = 2


class CacheConfig(msgspec.Struct, frozen=True):
    """Configuration for query/plan caching."""

    enable_plan_cache: bool = True
    max_cached_plans: int = 1000
    cache_ttl_s: float = 3600.0


class WriteConfig(msgspec.Struct, frozen=True):
    """Configuration for write operations."""

    max_rows_per_batch: int = 65536
    compression: str = "zstd"
    target_file_size_mb: int = 128


class DiagnosticsConfig(msgspec.Struct, frozen=True):
    """Configuration for diagnostics collection."""

    enable_query_logging: bool = False
    enable_plan_capture: bool = False
    max_diagnostic_entries: int = 10000


class RuntimeProfileConfig(msgspec.Struct, frozen=True):
    """Top-level runtime profile composing domain configs."""

    session_pool: SessionPoolConfig = SessionPoolConfig()
    cache: CacheConfig = CacheConfig()
    write: WriteConfig = WriteConfig()
    diagnostics: DiagnosticsConfig = DiagnosticsConfig()
```

### Files to Edit

- `src/datafusion_engine/session/runtime.py` — Replace PolicyBundleConfig usage with domain-scoped configs
- All files that construct or consume PolicyBundleConfig fields

### New Files to Create

- `src/datafusion_engine/session/config_structs.py` — Domain-scoped config structs (may already exist from S13)
- `tests/unit/datafusion_engine/session/test_config_structs.py` — Config construction, defaults, field validation

### Legacy Decommission/Delete Scope

- Delete `PolicyBundleConfig` class (superseded by `RuntimeProfileConfig` + domain-scoped structs)
- Delete any config fields that become redundant after decomposition

---

## S23. Mixin Inheritance to Composition

### Goal

Replace the 4-mixin inheritance chain in runtime.py with composition, eliminating the need for `cast()` dispatch and enabling independent testing of each concern.

### Representative Code Snippets

```python
# BEFORE — 4-mixin inheritance with cast()
class DataFusionRuntime(
    SessionMixin,
    RegistrationMixin,
    DiagnosticsMixin,
    CacheMixin,
):
    def do_work(self) -> None:
        session = cast(SessionMixin, self)
        session.create_session()

# AFTER — composition with typed fields
class DataFusionRuntime:
    def __init__(
        self,
        config: RuntimeProfileConfig,
    ) -> None:
        self._sessions = SessionManager(config.session_pool)
        self._registration = RegistrationManager()
        self._diagnostics = DiagnosticsCollector(config.diagnostics)
        self._cache = CacheManager(config.cache)

    @property
    def sessions(self) -> SessionManager:
        return self._sessions

    @property
    def registration(self) -> RegistrationManager:
        return self._registration
```

### Files to Edit

- `src/datafusion_engine/session/runtime.py` — Replace mixin inheritance with composed objects
- All callers that use `cast()` to access mixin methods — update to use composition accessors

### New Files to Create

- Covered by S13 decomposition modules

### Legacy Decommission/Delete Scope

- Delete all 4 mixin class definitions (superseded by standalone manager classes)
- Delete all `cast(MixinX, self)` patterns in `runtime.py`
- Delete mixin imports from `runtime.py`

---

## S24. Arrow Interop Protocol Consolidation

### Goal

Consolidate protocol usage around existing abstractions already present in `arrow/interop.py` (`DataTypeLike`, `FieldLike`, `SchemaLike`, adapters), and delete ad-hoc protocol/type-alias drift in downstream modules.

### Representative Code Snippets

```python
# src/datafusion_engine/arrow/interop.py (existing)
from __future__ import annotations

from typing import TypeAlias

import pyarrow as pa

SchemaInput: TypeAlias = SchemaLike | pa.Schema


def as_arrow_schema(schema: SchemaInput) -> pa.Schema:
    """Single coercion boundary for schema-like inputs."""
    return coerce_arrow_schema(schema)
```

### Files to Edit

- `src/datafusion_engine/arrow/interop.py` — Keep canonical protocol/type aliases and coercion entrypoints
- `src/datafusion_engine/arrow/build.py` — Remove local ad-hoc schema coercion aliases; import canonical aliases
- `src/datafusion_engine/arrow/nested.py` — Remove local ad-hoc schema coercion aliases; import canonical aliases
- `src/datafusion_engine/arrow/semantic.py` — Remove duplicate type adapter helpers when canonical interop adapter exists

### New Files to Create

- `tests/unit/datafusion_engine/arrow/test_interop_protocol_surface.py` — Verifies canonical protocol aliases and coercion entrypoints

### Legacy Decommission/Delete Scope

- Delete duplicate local protocol/type-alias definitions in `arrow/build.py`, `arrow/nested.py`, and related modules when equivalent definitions already exist in `arrow/interop.py`

---

## S25. Metadata Codec Boundary

### Goal

Harden the existing boundary between `arrow/metadata.py` and `arrow/metadata_codec.py`, ensuring all serialization/deserialization stays in the codec and metadata modules only orchestrate higher-level behavior.

### Representative Code Snippets

```python
# arrow/metadata_codec.py remains the only serialization owner:
# - encode_metadata(key: str, value: object) -> bytes
# - decode_metadata(raw: bytes) -> tuple[str, object]
# - METADATA_ENCODING_VERSION constant

# arrow/metadata.py (or sub-modules from S17) should only orchestrate:
# - attach_metadata(table: pa.Table, metadata: dict) -> pa.Table
# - extract_metadata(table: pa.Table) -> dict
# - These call codec functions for actual serialization
```

### Files to Edit

- `src/datafusion_engine/arrow/metadata.py` — Remove any direct serialization logic; delegate to codec
- `src/datafusion_engine/arrow/metadata_codec.py` — Ensure it owns all serialization/deserialization

### New Files to Create

- None (boundary clarification between existing files)

### Legacy Decommission/Delete Scope

- Move any serialization logic from `metadata.py` into `metadata_codec.py`

---

## S26. Lineage Protocol Extraction

### Goal

Reduce coupling between `plan/` and `lineage/` by extracting a Protocol that defines the lineage contract, allowing plan modules to depend on the Protocol rather than concrete lineage implementations.

### Representative Code Snippets

```python
# src/datafusion_engine/lineage/protocols.py (NEW)
from __future__ import annotations

from typing import Protocol


class LineageRecorder(Protocol):
    """Protocol for recording plan lineage information."""

    def record_input(self, table_name: str, columns: list[str]) -> None: ...
    def record_output(self, table_name: str, columns: list[str]) -> None: ...
    def record_dependency(self, source: str, target: str) -> None: ...


class LineageQuery(Protocol):
    """Protocol for querying lineage information."""

    def get_upstream(self, table_name: str) -> list[str]: ...
    def get_downstream(self, table_name: str) -> list[str]: ...
```

### Files to Edit

- `src/datafusion_engine/plan/bundle_artifact.py` — Depend on `LineageRecorder` Protocol instead of concrete lineage classes
- `src/datafusion_engine/plan/artifact_store.py` — Depend on `LineageQuery` Protocol
- `src/datafusion_engine/lineage/scheduling.py` — Implement `LineageRecorder` Protocol
- `src/datafusion_engine/lineage/reporting.py` — Implement `LineageQuery` Protocol

### New Files to Create

- `src/datafusion_engine/lineage/protocols.py` — Lineage Protocol definitions
- `tests/unit/datafusion_engine/lineage/test_protocols.py` — Protocol conformance tests

### Legacy Decommission/Delete Scope

- Remove direct imports of concrete lineage classes from `plan/` modules (replaced by Protocol imports)

---

## S27. Session Public API Boundary Hard Cutover

### Goal

Define and enforce a narrow public API for session runtime modules. Remove compatibility re-export shims and replace broad `__all__` exports with an explicit stable surface.

### Representative Code Snippets

```python
# src/datafusion_engine/session/api.py (NEW)
from __future__ import annotations

from datafusion_engine.session.runtime import DataFusionRuntime
from datafusion_engine.session.config_structs import RuntimeProfileConfig

__all__ = [
    "DataFusionRuntime",
    "RuntimeProfileConfig",
]
```

### Files to Edit

- `src/datafusion_engine/session/runtime.py` — Reduce `__all__` to stable runtime entrypoints only
- `src/datafusion_engine/session/config.py` — Remove compatibility re-export shim behavior
- `src/datafusion_engine/session/__init__.py` — Re-export only stable API symbols from `session/api.py`
- Callers currently importing from shim/re-export paths — update to canonical API paths

### New Files to Create

- `src/datafusion_engine/session/api.py` — Explicit public API module
- `tests/unit/datafusion_engine/session/test_public_api_surface.py` — Ensures only intended symbols are exported

### Legacy Decommission/Delete Scope

- Delete shim-style re-exports in `session/config.py`
- Delete unstable/private exports from runtime `__all__`

---

## S28. Arrow & Encoding Observability

### Goal

Add structured logging to `arrow/` (currently 0 logging across 5,685 LOC) and `encoding/`, enabling diagnosis of type conversion failures, metadata corruption, and encoding policy mismatches.

### Representative Code Snippets

```python
# src/datafusion_engine/arrow/interop.py — AFTER
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def arrow_type_from_datafusion_type(df_type: object) -> pa.DataType:
    """Convert a DataFusion type to PyArrow type."""
    logger.debug("Converting DataFusion type: %s", df_type)
    try:
        result = _convert_type(df_type)
        logger.debug("Converted to Arrow type: %s", result)
        return result
    except (TypeError, ValueError) as exc:
        logger.warning(
            "Type conversion failed for %s: %s",
            df_type,
            exc,
        )
        raise
```

### Files to Edit

- `src/datafusion_engine/arrow/interop.py` — Add logger, add debug/warning logging at conversion boundaries
- `src/datafusion_engine/arrow/build.py` — Add logging for batch construction
- `src/datafusion_engine/arrow/nested.py` — Add logging for nested type handling
- `src/datafusion_engine/arrow/metadata.py` — Add logging for metadata operations
- `src/datafusion_engine/arrow/semantic.py` — Add logging for semantic type mapping
- `src/datafusion_engine/encoding/policy.py` — Add logging for encoding policy decisions

### New Files to Create

- None (logging is added to existing files)

### Legacy Decommission/Delete Scope

- None (additive change)

---

## S29. Delta Observability Improvements

### Goal

Add structured telemetry to `delta/maintenance.py` operations and enrich error context in `delta/observability.py` while aligning with the exception policy from S11.

### Representative Code Snippets

```python
# src/datafusion_engine/delta/maintenance.py — AFTER
import logging

logger = logging.getLogger(__name__)


def compact_table(self, table_uri: str, **kwargs: object) -> CompactionResult:
    """Run compaction on a delta table."""
    logger.info("Starting compaction for table: %s", table_uri)
    try:
        result = self._run_compaction(table_uri, **kwargs)
        logger.info(
            "Compaction complete: %s files merged, %s bytes saved",
            result.files_merged,
            result.bytes_saved,
        )
        return result
    except (OSError, ValueError, RuntimeError) as exc:
        logger.error("Compaction failed for %s: %s", table_uri, exc)
        raise
```

### Files to Edit

- `src/datafusion_engine/delta/maintenance.py` — Add structured logging for all maintenance operations
- `src/datafusion_engine/delta/observability.py` — Enrich exception handlers with structured context and specific exception typing

### New Files to Create

- None (observability is added to existing files)

### Legacy Decommission/Delete Scope

- None (additive change)

---

## S30. Runtime State Lifecycle & Cache Key Hygiene

### Goal

Replace `id(...)` keyed caches and module-level lifecycle guards with explicit runtime state registries that are injectable, resettable, and keyed by context object identity rather than integer ids.

### Representative Code Snippets

```python
# src/datafusion_engine/runtime_state.py (NEW)
from __future__ import annotations

from weakref import WeakKeyDictionary

from datafusion import SessionContext


class RuntimeStateRegistry:
    """Holds per-context mutable state with explicit reset semantics."""

    def __init__(self) -> None:
        self._state_by_ctx: WeakKeyDictionary[SessionContext, dict[str, object]] = (
            WeakKeyDictionary()
        )

    def state_for(self, ctx: SessionContext) -> dict[str, object]:
        return self._state_by_ctx.setdefault(ctx, {})

    def reset(self) -> None:
        self._state_by_ctx = WeakKeyDictionary()
```

```python
# src/datafusion_engine/catalog/introspection.py — AFTER
def _context_cache(
    ctx: SessionContext,
    *,
    registry: RuntimeStateRegistry,
) -> dict[str, object]:
    return registry.state_for(ctx)
```

### Files to Edit

- `src/datafusion_engine/dataset/registry.py` — Replace `id(location)` cache keys with explicit context-scoped cache objects
- `src/datafusion_engine/catalog/introspection.py` — Replace `id(ctx)` cache keys with injected registry
- `src/datafusion_engine/io/adapter.py` — Remove global `_REGISTERED_OBJECT_STORES`; inject object-store registration state
- `src/datafusion_engine/views/graph.py` — Replace `_resolver_identity_guard` with context-injected state
- `src/datafusion_engine/session/runtime.py` — Own and thread `RuntimeStateRegistry` lifecycle

### New Files to Create

- `src/datafusion_engine/runtime_state.py` — Shared runtime state registry primitives
- `tests/unit/datafusion_engine/test_runtime_state_registry.py` — Reset semantics and per-context state isolation

### Legacy Decommission/Delete Scope

- Delete `id()` keyed cache maps in `dataset/registry.py` and `catalog/introspection.py`
- Delete `_REGISTERED_OBJECT_STORES` in `io/adapter.py`
- Delete `_resolver_identity_guard` globals/reset helper in `views/graph.py`

---

## S31. Delta Compatibility Typing Hardening

### Goal

Replace `object`-typed compatibility payloads with `DeltaExtensionCompatibility` across Delta provider artifacts and control-plane helpers, removing `getattr` probing and enabling full static typing.

### Representative Code Snippets

```python
# src/datafusion_engine/delta/provider_artifacts.py — AFTER
from __future__ import annotations

import msgspec

from datafusion_engine.delta.capabilities import DeltaExtensionCompatibility


class ProviderArtifactRequest(msgspec.Struct, frozen=True):
    compatibility: DeltaExtensionCompatibility | None = None
```

```python
# src/datafusion_engine/delta/control_plane.py — AFTER
from datafusion_engine.delta.capabilities import DeltaExtensionCompatibility


def _compatibility_message(
    *,
    operation: str,
    compatibility: DeltaExtensionCompatibility,
) -> str:
    probe_result = compatibility.probe_result
    module = compatibility.module
    ctx_kind = compatibility.ctx_kind
    error = compatibility.error
    return f"{operation}: module={module} ctx={ctx_kind} probe={probe_result} error={error}"
```

### Files to Edit

- `src/datafusion_engine/delta/provider_artifacts.py` — Replace `object | None` compatibility typing with `DeltaExtensionCompatibility | None`
- `src/datafusion_engine/delta/control_plane.py` — Replace `object` compatibility arguments and `getattr` probes with typed fields
- `src/datafusion_engine/delta/service.py` — Thread typed compatibility contracts

### New Files to Create

- `tests/unit/datafusion_engine/delta/test_compatibility_typing.py` — Validates typed compatibility flow and message formatting

### Legacy Decommission/Delete Scope

- Delete `getattr(..., \"module\"|\"entrypoint\"|\"ctx_kind\"|\"probe_result\"|\"error\", ...)` compatibility probing patterns in `delta/provider_artifacts.py` and `delta/control_plane.py`

---

## S32. YAGNI & Legacy Cleanup

### Goal

Remove legacy aliases and dead/no-op parameters that no longer influence behavior, reducing hidden complexity and aligning with hard-cutover principles.

### Representative Code Snippets

```python
# src/datafusion_engine/extract/registry.py — AFTER
def resolve_dataset_name(name: str) -> str:
    # Hard cutover: no legacy alias indirection.
    return name
```

```python
# src/datafusion_engine/io/write.py — BEFORE
def write(self, request: WriteRequest, *, prefer_streaming: bool = True) -> WriteResult:
    _ = prefer_streaming
    ...

# AFTER
def write(self, request: WriteRequest) -> WriteResult:
    ...
```

### Files to Edit

- `src/datafusion_engine/extract/registry.py` — Remove `_LEGACY_DATASET_ALIASES` map and fallback lookups
- `src/datafusion_engine/extract/templates.py` — Remove dead initial `_DATASET_TEMPLATE_REGISTRY` assignment overwritten later in module initialization
- `src/datafusion_engine/io/write.py` — Remove/implement `prefer_streaming`; if not implemented, delete parameter from public API and callsites

### New Files to Create

- `tests/unit/datafusion_engine/extract/test_registry_resolution.py` — Verifies direct-name resolution behavior
- `tests/unit/datafusion_engine/io/test_write_signature.py` — Locks final write API signature and behavior

### Legacy Decommission/Delete Scope

- Delete `_LEGACY_DATASET_ALIASES` and all lookup branches dependent on it
- Delete dead `_DATASET_TEMPLATE_REGISTRY` bootstrapping assignment overwritten at import time
- Delete no-op `prefer_streaming` parameter plumbing where behavior is unchanged

---

## S33. DataFusion & Delta Built-In Capability Alignment

### Goal

Audit and align custom runtime/delta behavior against built-in DataFusion and DeltaLake capabilities; prefer existing built-ins where equivalent behavior already exists and codify the decision matrix in tests.

### Representative Code Snippets

```python
# src/datafusion_engine/session/runtime.py — capability policy table
RUNTIME_BUILTIN_SETTINGS: dict[str, str] = {
    "datafusion.execution.collect_statistics": "true",
    "datafusion.execution.planning_concurrency": "8",
    "datafusion.execution.parquet.max_predicate_cache_size": str(64 * MIB),
    "datafusion.runtime.metadata_cache_limit": str(256 * MIB),
}
```

```python
# src/datafusion_engine/catalog/introspection.py — verification entrypoint
from datafusion_engine.catalog.introspection import (
    list_files_cache_snapshot,
    predicate_cache_snapshot,
    statistics_cache_snapshot,
)


def assert_runtime_builtins_applied(ctx: SessionContext) -> None:
    list_files_cache_snapshot(ctx)
    statistics_cache_snapshot(ctx)
    predicate_cache_snapshot(ctx)
```

### Files to Edit

- `src/datafusion_engine/session/runtime.py` — Normalize built-in setting policy tables and remove duplicate/custom equivalents
- `src/datafusion_engine/catalog/introspection.py` — Ensure snapshots/diagnostics remain the verification plane for built-in cache/stat settings
- `src/datafusion_engine/delta/control_plane.py` — Prefer Delta extension built-ins where present before bespoke wrappers
- `docs/reviews/datafusion_engine_synthesis.md` — Add built-in capability decision matrix used by implementation

### New Files to Create

- `tests/unit/datafusion_engine/session/test_runtime_builtin_alignment.py` — Confirms built-in runtime settings are applied and introspectable
- `tests/unit/datafusion_engine/delta/test_builtin_preference.py` — Confirms control-plane paths prefer built-ins first

### Legacy Decommission/Delete Scope

- Delete bespoke wrappers that duplicate DataFusion/Delta built-in behavior once parity is validated

---

## S34. Observability Acceptance Criteria & Tracing Completion

### Goal

Define explicit acceptance criteria for observability changes (logs, metrics, tracing) and complete span-level instrumentation for both Python and Rust execution paths.

### Representative Code Snippets

```python
# src/datafusion_engine/obs/runtime_metrics.py (NEW)
from __future__ import annotations

from opentelemetry import metrics, trace

meter = metrics.get_meter(__name__)
tracer = trace.get_tracer(__name__)
delta_maintenance_runs = meter.create_counter("delta.maintenance.runs")
```

```python
# src/datafusion_engine/delta/maintenance.py — AFTER
with tracer.start_as_current_span("delta.maintenance.compact"):
    delta_maintenance_runs.add(1, {"operation": "compact"})
    return self._run_compaction(table_uri, **kwargs)
```

### Files to Edit

- `src/datafusion_engine/arrow/interop.py` — Add structured logs with stable keys
- `src/datafusion_engine/delta/maintenance.py` — Add counters/timers/spans for maintenance operations
- `src/datafusion_engine/delta/observability.py` — Emit structured error attributes for failure cases
- `rust/codeanatomy_engine/src/executor/tracing/exec_instrumentation.rs` — Validate and complete tracing coverage for Rust execution path
- `src/datafusion_engine/obs/` modules — Add metric/tracing helper wiring used by S28/S29

### New Files to Create

- `src/datafusion_engine/obs/runtime_metrics.py` — Shared metric/tracer instruments for DataFusion engine paths
- `tests/unit/datafusion_engine/obs/test_runtime_metrics.py` — Instrument creation and attribute schema tests
- `tests/unit/datafusion_engine/delta/test_maintenance_observability.py` — Span/metric emission behavior tests

### Legacy Decommission/Delete Scope

- Delete ad-hoc, unstructured log payload formats replaced by stable field-based logging and metric/tracing attributes

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S1, S2)

- Delete all local `_sql_identifier` function bodies in `expr/spec.py`, `io/write.py`, `kernels.py` — replaced by `sql/helpers.py:sql_identifier`
- Delete local schema conversion helpers in `views/bundle_extraction.py`, `dataset/registration.py`, `plan/bundle_artifact.py`, `schema/observability_schemas.py` — replaced by canonical `arrow/interop.py` conversion entrypoints
- Delete duplicate `DEFAULT_DICTIONARY_INDEX_TYPE` definition from `encoding/policy.py` — canonical constant remains in `arrow/types.py`

### Batch D2 (after S3, S4, S21)

- Delete `_ENGINE_FUNCTION_REQUIREMENTS` at `extraction_schemas.py:1546` and `observability_schemas.py:406` — replaced by `schema/constants.py:ENGINE_FUNCTION_REQUIREMENTS`
- Delete superseded schema constant reassignments that are overwritten later in schema modules
- Delete 5 `_chain_*_hooks` functions at `runtime.py:2199-2255` — replaced by `session/hooks.py:chain_hooks`
- Delete 3 `_record_*_registration` functions at `runtime.py:5792-5915` — replaced by `session/hooks.py:record_registration`
- Delete duplicated telemetry enrichment paths and contradictory identifier-normalization flag combinations in `session/runtime.py`

### Batch D3 (after S5, S6, S31)

- Delete 22 feature-toggle wrapper functions in `delta/control_plane.py` — replaced by `_FEATURE_TOGGLE_REGISTRY` + `resolve_feature_toggle`
- Delete repeated gate payload extraction and repeated Rust invocation glue — replaced by `extract_gate_payload` + `_invoke_rust_entrypoint`
- Delete compatibility `getattr` probing patterns in Delta artifact/control-plane flows — replaced by `DeltaExtensionCompatibility` typing
- Delete 11+ inline ABI error strings across `udf/` — replaced by `udf/constants.py` constants
- Delete 7+ inline extension module name literals across `udf/` — replaced by `udf/constants.py:EXTENSION_MODULE_PREFIX`

### Batch D4 (after S7, S8, S9, S30)

- Delete module-level registry globals in `udf/extension_runtime.py` and `udf/platform.py` — replaced by injected registry state
- Delete module-level mutable caches and `id()` keyed cache maps in dataset/catalog modules — replaced by injected runtime state registry
- Delete global `_REGISTERED_OBJECT_STORES` in `io/adapter.py`
- Delete global `_resolver_identity_guard` in `views/graph.py`
- Delete `globals()`-based discovery in `extract/templates.py` — replaced by explicit `_TEMPLATE_REGISTRY`

### Batch D5 (after S13, S22, S23, S27)

- Delete `PolicyBundleConfig` class from `runtime.py` — replaced by domain-scoped config structs in `config_structs.py`
- Delete 4 mixin class definitions — replaced by standalone manager classes
- Delete all `cast(MixinX, self)` patterns — replaced by composition accessors
- Delete compatibility re-export shims in `session/config.py` and unstable exports from runtime `__all__`
- `runtime.py` final target: ~500 LOC facade

### Batch D6 (after S13, S14, S15, S16, S17, S18, S19, S20)

- All 9 monolithic files reduced to facades. Original monolithic code fully migrated to sub-modules. Verify no dead code remains in facade files.

### Batch D7 (after S32, S33)

- Delete `_LEGACY_DATASET_ALIASES` and alias lookup branches from `extract/registry.py`
- Delete dead overwritten `_DATASET_TEMPLATE_REGISTRY` initialization in `extract/templates.py`
- Delete no-op `prefer_streaming` parameter plumbing from `io/write.py` call paths
- Delete bespoke wrappers duplicating DataFusion/Delta built-ins once parity tests pass

### Batch D8 (after S28, S29, S34)

- Delete ad-hoc unstructured observability payload formatting replaced by stable field-based logs, metrics, and spans

---

## Implementation Sequence

1. **S1: SQL & Expression Helper Consolidation** — Foundational DRY fix with minimal behavioral risk.
2. **S2: Arrow Schema Conversion Consolidation** — Establishes one conversion boundary before decomposition work.
3. **S3: Schema Constants Consolidation** — Removes duplicate constants and schema overwrite hazards before schema split.
4. **S4: Runtime Hook & Registration DRY** — Reduces runtime duplication ahead of runtime decomposition.
5. **S5: Delta DRY Consolidation** — Removes toggle and gate duplication before control-plane split.
6. **S6: UDF Constants & DRY Consolidation** — Normalizes UDF strings/constants before extension decomposition.
7. **S7: UDF Global Registry Injection** — Makes UDF state testable before deeper extraction.
8. **S8: Dataset Mutable Cache Injection** — Removes dataset-local mutable globals before registration split.
9. **S9: Extract Template Explicit Registry** — Finalizes template registration determinism early.
10. **S30: Runtime State Lifecycle & Cache Key Hygiene** — Introduces shared injected state registry used by later scopes.
11. **S10: Typed Rust FFI Boundaries** — Tightens boundary contracts before Delta type hardening.
12. **S31: Delta Compatibility Typing Hardening** — Converts `object` compatibility payloads to typed contracts.
13. **S11: Exception Handling Hardening** — Applies precise exception policy before observability rollout.
14. **S12: Protocol-Based Dispatch** — Removes cast-driven dispatch prior to runtime refactor.
15. **S21: Runtime Identifier & Telemetry Contract Cleanup** — Resolves runtime contract conflicts before splitting runtime.
16. **S22: PolicyBundleConfig Decomposition** — Prepares domain config structures used by runtime modules.
17. **S23: Mixin Inheritance to Composition** — Establishes final runtime architecture shape.
18. **S13: Runtime.py Decomposition** — Largest decomposition; depends on S4/S21/S22/S23.
19. **S27: Session Public API Boundary Hard Cutover** — Lock stable API after runtime split.
20. **S14: Registration.py Decomposition** — Safer after S8/S30 state injection.
21. **S15: Write.py Decomposition** — Can run parallel to S14 once shared helpers are stable.
22. **S16: Control_plane.py Decomposition** — After S5/S31 Delta contract consolidation.
23. **S26: Lineage Protocol Extraction** — Establish lineage contracts before artifact-store split.
24. **S18: Artifact_store.py Decomposition** — After S26 to avoid dependency cycles.
25. **S17: Metadata.py Decomposition** — After S2 conversion normalization.
26. **S24: Arrow Interop Protocol Consolidation** — After S17 to keep protocol surface centralized.
27. **S25: Metadata Codec Boundary** — After S17/S24 so metadata layering is stable.
28. **S19: Extension_runtime.py Decomposition** — After S6/S7 registry and constant cleanup.
29. **S20: Schema File Decomposition** — After S3 schema constants consolidation.
30. **S28: Arrow & Encoding Observability** — After S24 to instrument final interop boundaries.
31. **S29: Delta Observability Improvements** — After S16 and S11 for stable flow/error semantics.
32. **S32: YAGNI & Legacy Cleanup** — After public/runtime boundary stabilization to avoid churn.
33. **S33: DataFusion & Delta Built-In Capability Alignment** — After core decompositions so comparisons are accurate.
34. **S34: Observability Acceptance Criteria & Tracing Completion** — Final validation pass after S28/S29.

---

## Implementation Checklist

### DRY Consolidation (Phase 1)
- [ ] S1: SQL & Expression Helper Consolidation
- [ ] S2: Arrow Schema Conversion Consolidation
- [ ] S3: Schema Constants Consolidation
- [ ] S4: Runtime Hook & Registration DRY
- [ ] S5: Delta DRY Consolidation
- [ ] S6: UDF Constants & DRY Consolidation

### Global State & Testability (Phase 2)
- [ ] S7: UDF Global Registry Injection
- [ ] S8: Dataset Mutable Cache Injection
- [ ] S9: Extract Template Explicit Registry
- [ ] S30: Runtime State Lifecycle & Cache Key Hygiene

### Type Safety & Contracts (Phase 3)
- [ ] S10: Typed Rust FFI Boundaries
- [ ] S11: Exception Handling Hardening
- [ ] S12: Protocol-Based Dispatch
- [ ] S21: Runtime Identifier & Telemetry Contract Cleanup
- [ ] S31: Delta Compatibility Typing Hardening

### Runtime Architecture (Phase 4)
- [ ] S22: PolicyBundleConfig Decomposition
- [ ] S23: Mixin Inheritance to Composition
- [ ] S13: Runtime.py Decomposition
- [ ] S27: Session Public API Boundary Hard Cutover

### Monolith Decomposition (Phase 5)
- [ ] S14: Registration.py Decomposition
- [ ] S15: Write.py Decomposition
- [ ] S16: Control_plane.py Decomposition
- [ ] S17: Metadata.py Decomposition
- [ ] S18: Artifact_store.py Decomposition
- [ ] S19: Extension_runtime.py Decomposition
- [ ] S20: Schema File Decomposition
- [ ] S26: Lineage Protocol Extraction

### Boundary & Observability (Phase 6)
- [ ] S24: Arrow Interop Protocol Consolidation
- [ ] S25: Metadata Codec Boundary
- [ ] S28: Arrow & Encoding Observability
- [ ] S29: Delta Observability Improvements
- [ ] S34: Observability Acceptance Criteria & Tracing Completion

### Design-Principle Additions (Phase 7)
- [ ] S32: YAGNI & Legacy Cleanup
- [ ] S33: DataFusion & Delta Built-In Capability Alignment

### Cross-Scope Decommission
- [ ] D1: Post S1+S2 — Delete all consolidated duplicates
- [ ] D2: Post S3+S4+S21 — Delete schema/runtime duplication leftovers
- [ ] D3: Post S5+S6+S31 — Delete delta toggles/getattr probing/UDF constant duplicates
- [ ] D4: Post S7+S8+S9+S30 — Delete module-level globals and id-keyed caches
- [ ] D5: Post S13+S22+S23+S27 — Delete PolicyBundleConfig + mixins + cast patterns + API shims
- [ ] D6: Post all decompositions — Verify all 9 facades are minimal, no dead code
- [ ] D7: Post S32+S33 — Delete YAGNI leftovers and built-in duplicates
- [ ] D8: Post S28+S29+S34 — Delete ad-hoc observability payload formatting
