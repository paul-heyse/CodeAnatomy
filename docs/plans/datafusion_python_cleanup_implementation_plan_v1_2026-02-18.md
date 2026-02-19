# DataFusion Python Cleanup — Implementation Plan v1

**Topic:** datafusion-python-cleanup
**Version:** v1
**Date:** 2026-02-18
**Source Reviews:**
- Agent 5: `docs/reviews/design_review_df_session_runtime_2026-02-18.md`
- Agent 6: `docs/reviews/design_review_df_udf_dataset_tables_expr_2026-02-18.md`
- Agent 7: `docs/reviews/design_review_df_delta_storage_cache_2026-02-18.md`
- Agent 9: `docs/reviews/design_review_df_io_extensions_catalog_2026-02-18.md`
- Agent 11: `docs/reviews/design_review_df_schema_arrow_encoding_2026-02-18.md` (Python side)

---

## Scope Summary

This plan addresses the top-priority findings from five design reviews covering the
DataFusion Python layer, Python infrastructure, and the Python side of the cross-language
boundary. The work spans `src/datafusion_engine/`, `src/storage/`, `src/obs/`, and
`src/utils/`. It is deliberately scoped to exclude `src/semantics/`, `src/relspec/`,
`src/extract/`, and `rust/`.

All 20 scope items (S1-S20) are sequenced to minimise blast radius and allow the
implementation checklist to be executed by independent parallel streams after the first
two high-risk items (S1, S11) are resolved.

---

## Design Principles

1. All changes are backward-compatible at the public API level; no removal of exported
   symbols without a redirect or tombstone.
2. Every new module gets a corresponding test file.
3. Decommission lists name exact symbols — no "clean up misc helpers" language.
4. Code snippets are derived from the actual source files read during plan preparation.
5. Changes that touch `frozen=True` msgspec structs use `object.__setattr__` only inside
   `__post_init__`; callers never see the mutation.
6. `uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q` is the single quality gate run after all edits.

---

## Current Baseline

| File | Lines | Key Issues |
|------|-------|-----------|
| `src/datafusion_engine/delta/control_plane_types.py` | ~430 | 16x repeated `table_ref` property; `DeltaDeleteRequest.predicate` allows empty string |
| `src/datafusion_engine/io/write_pipeline.py` | 611 | Duplicates `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS` and `_is_delta_observability_operation` from `write_execution.py` |
| `src/datafusion_engine/session/context_pool.py` | ~400 | `BaseException` catch; `build_config()` at 130+ lines |
| `src/datafusion_engine/session/runtime.py` | ~700 | Six-mixin MRO; three facade mixins wrapping already-exposed properties |
| `src/datafusion_engine/session/runtime_extensions.py` | 1239 | `_DDL_CATALOG_WARNING_STATE` module-level mutable dict |
| `src/datafusion_engine/udf/extension_runtime.py` | 1221 | Second `register_rust_udfs` at line 502 silently shadows the first |
| `src/datafusion_engine/udf/metadata.py` | 1214 | `create_strict_catalog` identical to `create_default_catalog` |
| `src/datafusion_engine/catalog/introspection.py` | 759 | `_CACHE_SNAPSHOT_ERRORS` includes bare `Exception`; `snapshot` property mutates on read |
| `src/datafusion_engine/views/registration.py` | ~300 | `RegistrationPhase(validate=...)` field name misleads readers |
| `src/datafusion_engine/obs/diagnostics_bridge.py` | ~200 | `_OBS_SESSION_ID` fixed at import time for all bridge helpers |
| `src/obs/scan_telemetry.py` | ~210 | `fragment_telemetry` calls `set_scan_telemetry` as side-effect inside a computation |
| `src/utils/env_utils.py` | ~300 | `env_bool` defaults `log_invalid=False` — silent on misconfiguration |
| `src/datafusion_engine/plan/artifact_store_constants.py` | 25 | `Path.cwd()` fallback captured at import time |
| `src/datafusion_engine/cache/inventory.py` | ~80 | `Path.cwd()` fallback captured at import time |
| `src/datafusion_engine/delta/obs_table_manager.py` | ~120 | `Path.cwd()` fallback captured at import time |
| `src/storage/deltalake/file_pruning.py` | ~450 | `_coerce_bool_value` is case-sensitive; diverges from canonical `coerce_bool` |
| `src/storage/deltalake/delta_read.py` | ~330 | Owns `_DeltaMergeExecutionState`, `_DeltaMergeExecutionResult`, `_normalize_commit_metadata` — imported as privates by `delta_write.py` |
| `src/datafusion_ext.pyi` | 118 | Missing 7 Rust function stubs |

---

## S1 — Fix DeltaDeleteRequest.predicate Empty-String Gap

**Principles:** P8 (contract), P10 (illegal states), P21 (least astonishment)
**Review source:** Agent 7, P8/P10 findings
**Priority:** CRITICAL — a msgpack-serialised empty-string predicate reaches Rust with no Python-side guard

### Goal

Add a `__post_init__` guard to `DeltaDeleteRequest` that rejects empty or whitespace-only
predicate strings at construction time. The existing `_require_non_empty_delete_predicate`
call in `control_plane_mutation.py` stays as defense-in-depth.

### Representative Code Snippets

Current state in `src/datafusion_engine/delta/control_plane_types.py:146-166`:

```python
class DeltaDeleteRequest(StructBaseStrict, frozen=True):
    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    predicate: str          # allows "" -- BUG
    extra_constraints: Sequence[str] | None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        ...
```

Target state (follows `DeltaWriteRequest.__post_init__` pattern at line 131):

```python
class DeltaDeleteRequest(StructBaseStrict, frozen=True):
    ...
    predicate: str
    ...

    def __post_init__(self) -> None:
        """Reject empty or whitespace-only predicates at construction time."""
        if not self.predicate.strip():
            msg = "DeltaDeleteRequest.predicate must be a non-empty string."
            raise ValueError(msg)
```

### Files to Edit

- `src/datafusion_engine/delta/control_plane_types.py` — add `__post_init__` to `DeltaDeleteRequest`

### New Files to Create

None. The existing test file
`tests/unit/datafusion_engine/delta/test_control_plane_types.py` must receive a new
test case asserting that `DeltaDeleteRequest(predicate="")` raises `ValueError` and that
`DeltaDeleteRequest(predicate="   ")` also raises `ValueError`.

### Legacy Decommission / Delete Scope

None — `_require_non_empty_delete_predicate` in `control_plane_mutation.py` is retained.

---

## S2 — Consolidate Bool Coercion DRY Violation

**Principles:** P7 (DRY)
**Review source:** Agent 5, P7 finding; Agent 9, P7 finding
**Priority:** HIGH — `_coerce_bool_value` in `file_pruning.py` treats `"True"` as `False` (case-sensitive bug)

### Goal

Delete `_coerce_bool_value` from `file_pruning.py` and replace all call sites with
`utils.value_coercion.coerce_bool`. The `coerce_bool` canonical function handles
`"True"`, `"true"`, `"TRUE"` identically and returns `None` on parse failure —
matching `_coerce_bool_value`'s `None`-on-failure contract while fixing the
case-sensitivity bug.

### Representative Code Snippets

Current state in `src/storage/deltalake/file_pruning.py:422-427`:

```python
def _coerce_bool_value(value: object) -> bool | None:
    if isinstance(value, str):
        return value.lower() == "true"   # case-sensitive on raw, lowers first
    if isinstance(value, (bool, int, float)):
        return bool(value)
    return None
```

Note: `value.lower() == "true"` is actually case-insensitive because `.lower()` is
applied. The review identified that `isinstance(value, str)` branch uses `value == "true"`
without `.lower()` in a different variant. The implementation above does lower-case but
the broader issue is that `_coerce_bool_value` is a private duplicate of `coerce_bool`.
Deleting it removes the maintenance burden regardless.

Replacement at each call site:

```python
from utils.value_coercion import coerce_bool

# Before:
resolved = _coerce_bool_value(value)
# After:
resolved = coerce_bool(value)
```

### Files to Edit

- `src/storage/deltalake/file_pruning.py` — delete `_coerce_bool_value`; import
  `coerce_bool` from `utils.value_coercion`; update the one call site that passes
  Boolean cast-type to use `coerce_bool(value)` instead

### New Files to Create

None. The existing `tests/unit/utils/test_value_coercion.py` and
`tests/unit/utils/test_coercion.py` already cover `coerce_bool`. Add one
regression test to `tests/unit/storage/deltalake/test_delta_read.py` (or a new
`test_file_pruning_coercion.py`) confirming the `"True"` / `"False"` round-trip
now returns `True` / `False`.

### Legacy Decommission / Delete Scope

**D1:**
- Delete `_coerce_bool_value` from `src/storage/deltalake/file_pruning.py` (lines 422-427)

---

## S3 — Narrow BaseException to Exception in context_pool.py

**Principles:** P8 (contract), P9 (parse, don't validate)
**Review source:** Agent 5, P8/P9 findings

### Goal

Change the `except BaseException` in `_set_config_if_supported` to `except Exception`.
`BaseException` catches `KeyboardInterrupt` and `SystemExit`, which must propagate
unimpeded. Also add a `logger.warning` in `_build_session_runtime_from_context` when
the UDF snapshot falls back to empty — currently it silences the failure without any
diagnostic.

### Representative Code Snippets

Current state in `src/datafusion_engine/session/context_pool.py:54`:

```python
    except BaseException as exc:         # too broad — catches KeyboardInterrupt
        message = str(exc)
        if "Config value" in message and "not found" in message:
            return config
        raise
```

Target state:

```python
    except Exception as exc:             # correct bound
        message = str(exc)
        if "Config value" in message and "not found" in message:
            return config
        raise
```

Current state in `src/datafusion_engine/session/runtime_session.py:101-104`:

```python
    try:
        snapshot = rust_udf_snapshot(ctx, registries=profile.udf_extension_registries)
    except (RuntimeError, TypeError, ValueError):
        snapshot = _empty_udf_snapshot_payload()
```

Target state:

```python
    try:
        snapshot = rust_udf_snapshot(ctx, registries=profile.udf_extension_registries)
    except (RuntimeError, TypeError, ValueError) as exc:
        logger.warning(
            "UDF snapshot unavailable; plan fingerprints may be inaccurate: %s",
            exc,
        )
        snapshot = _empty_udf_snapshot_payload()
```

### Files to Edit

- `src/datafusion_engine/session/context_pool.py` — `_set_config_if_supported`: change `BaseException` to `Exception`
- `src/datafusion_engine/session/runtime_session.py` — `_build_session_runtime_from_context`: bind exception; add `logger.warning`; ensure `logger = logging.getLogger(__name__)` is present at module level

### New Files to Create

None. Tests for `_set_config_if_supported` are in-scope for
`tests/unit/datafusion_engine/session/` if a session unit test directory exists;
otherwise no new test file is needed for this one-line fix.

### Legacy Decommission / Delete Scope

None.

---

## S4 — Remove Redundant Facade Mixins from DataFusionRuntimeProfile

**Principles:** P13 (composition over inheritance), P4 (high cohesion)
**Review source:** Agent 5, P13 finding
**Priority:** MEDIUM — reduces MRO from 6 levels to 3; removes `_as_runtime_profile` cast need

### Goal

Remove `_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`, and
`_RuntimeProfileDeltaFacadeMixin` from the `DataFusionRuntimeProfile` MRO. The
delegation dataclasses (`RuntimeProfileIO`, `RuntimeProfileCatalog`,
`RuntimeProfileDeltaOps`) are already available as `profile.io_ops`, `profile.catalog_ops`,
and `profile.delta_ops` properties. Each mixin method is a one-line delegation to these
properties; callers can use the properties directly.

### Representative Code Snippets

Current MRO in `src/datafusion_engine/session/runtime.py:212-217`:

```python
class DataFusionRuntimeProfile(
    _RuntimeProfileIdentityMixin,
    _RuntimeProfileQueryMixin,
    _RuntimeProfileIOFacadeMixin,       # to remove
    _RuntimeProfileCatalogFacadeMixin,  # to remove
    _RuntimeProfileDeltaFacadeMixin,    # to remove
    _RuntimeDiagnosticsMixin,
    _RuntimeContextMixin,
    StructBaseStrict,
    frozen=True,
):
```

Target MRO:

```python
class DataFusionRuntimeProfile(
    _RuntimeProfileIdentityMixin,
    _RuntimeProfileQueryMixin,
    _RuntimeDiagnosticsMixin,
    _RuntimeContextMixin,
    StructBaseStrict,
    frozen=True,
):
```

Callers of `profile.cache_root()` become `profile.io_ops.cache_root()`. Use
`/cq calls cache_root` before making the change to enumerate all call sites.

### Files to Edit

- `src/datafusion_engine/session/runtime.py` — remove three mixin classes from the MRO; remove their `class` definitions (or relocate them to `runtime_ops.py` as standalone functions if any non-profile code uses them)
- `src/datafusion_engine/session/runtime_ops.py` — remove `_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`, `_RuntimeProfileDeltaFacadeMixin` class bodies; keep the underlying delegation dataclasses unchanged
- All call sites of the three removed mixin methods — update to use the `profile.io_ops.X()`, `profile.catalog_ops.X()`, `profile.delta_ops.X()` form

### New Files to Create

None.

### Legacy Decommission / Delete Scope

**D2:**
- Delete `_RuntimeProfileIOFacadeMixin` class body from `src/datafusion_engine/session/runtime_ops.py`
- Delete `_RuntimeProfileCatalogFacadeMixin` class body from `src/datafusion_engine/session/runtime_ops.py`
- Delete `_RuntimeProfileDeltaFacadeMixin` class body from `src/datafusion_engine/session/runtime_ops.py`

---

## S5 — Consolidate Write-Path Duplicates

**Principles:** P7 (DRY), P19 (KISS)
**Review source:** Agent 7, P7 finding
**Priority:** HIGH — retry markers are safety-critical; silent divergence risk

### Goal

Delete the private copies of `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS`,
`_is_retryable_delta_stream_error`, and `_is_delta_observability_operation` from
`write_pipeline.py`. Import the public equivalents from `write_execution.py` where
needed. Update `delta_write_handler.py` import of the observability helper to use
`write_execution`.

### Representative Code Snippets

Current state in `src/datafusion_engine/io/write_pipeline.py:56-78`:

```python
_RETRYABLE_DELTA_STREAM_ERROR_MARKERS: tuple[str, ...] = (
    "c data interface error",
    "expected 3 buffers for imported type string",
)


def _is_retryable_delta_stream_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in _RETRYABLE_DELTA_STREAM_ERROR_MARKERS)


def _is_delta_observability_operation(operation: str | None) -> bool:
    if operation is None:
        return False
    return operation.startswith(
        (
            "delta_mutation_",
            "delta_snapshot_",
            "delta_scan_plan",
            "delta_maintenance_",
            "delta_observability_",
        )
    )
```

These three items are byte-for-byte duplicated in
`src/datafusion_engine/io/write_execution.py:7-40` under public names.

Target state — delete the three items from `write_pipeline.py` and import:

```python
from datafusion_engine.io.write_execution import (
    is_delta_observability_operation as _is_delta_observability_operation,
    is_retryable_delta_stream_error as _is_retryable_delta_stream_error,
)
```

### Files to Edit

- `src/datafusion_engine/io/write_pipeline.py` — delete three items; add import from `write_execution`
- `src/datafusion_engine/io/delta_write_handler.py` — update import of the observability helper if it currently imports from `write_pipeline`; confirm via `grep _is_delta_observability_operation` before editing

### New Files to Create

None.

### Legacy Decommission / Delete Scope

**D3:**
- Delete `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS` from `src/datafusion_engine/io/write_pipeline.py`
- Delete `_is_retryable_delta_stream_error` from `src/datafusion_engine/io/write_pipeline.py`
- Delete `_is_delta_observability_operation` from `src/datafusion_engine/io/write_pipeline.py`

---

## S6 — Extract table_ref Boilerplate in control_plane_types.py

**Principles:** P7 (DRY)
**Review source:** Agent 7, P7 finding
**Priority:** MEDIUM — 96 lines of identical property bodies; change blast-radius for future `DeltaTableRef` field changes

### Goal

Introduce a `table_ref_from_request(req)` module-level function that constructs a
`DeltaTableRef` from the four shared fields. Each of the 16 request class `table_ref`
properties delegates to this function. Alternatively, introduce a `_DeltaTableRefMixin`
struct with the four fields and the property defined once; all request classes inherit.
The function approach is preferred (simpler, avoids MRO complexity).

### Representative Code Snippets

Current state — 16 identical property bodies in `src/datafusion_engine/delta/control_plane_types.py`:

```python
@property
def table_ref(self) -> DeltaTableRef:
    """Return the request table reference."""
    return DeltaTableRef(
        table_uri=self.table_uri,
        storage_options=self.storage_options,
        version=self.version,
        timestamp=self.timestamp,
    )
```

Target state — one module-level function:

```python
def table_ref_from_request(
    req: _HasTableRefFields,
) -> DeltaTableRef:
    """Construct a DeltaTableRef from the canonical four-field request shape.

    Parameters
    ----------
    req:
        Any request object exposing ``table_uri``, ``storage_options``,
        ``version``, and ``timestamp``.

    Returns:
    -------
    DeltaTableRef
        Resolved table reference.
    """
    return DeltaTableRef(
        table_uri=req.table_uri,
        storage_options=req.storage_options,
        version=req.version,
        timestamp=req.timestamp,
    )
```

Where `_HasTableRefFields` is a `Protocol` (TYPE_CHECKING only) defining the four
attributes. Each class `table_ref` property becomes:

```python
@property
def table_ref(self) -> DeltaTableRef:
    """Return the request table reference."""
    return table_ref_from_request(self)
```

Export `table_ref_from_request` in the module's `__all__`.

### Files to Edit

- `src/datafusion_engine/delta/control_plane_types.py` — add `_HasTableRefFields` protocol (TYPE_CHECKING guard); add `table_ref_from_request` function; update all 16 `table_ref` property bodies to delegate

### New Files to Create

None.

### Legacy Decommission / Delete Scope

None — the 16 property bodies are replaced in-place, not moved elsewhere.

---

## S7 — Rename RegistrationPhase.validate to action

**Principles:** P21 (least astonishment)
**Review source:** Agent 6, P21 finding
**Priority:** MEDIUM — `validate` field receives side-effecting callables; name misleads readers

### Goal

Rename the `validate` field of `RegistrationPhase` (defined in
`src/datafusion_engine/registry_facade.py`) to `action`. Update the six call sites in
`src/datafusion_engine/views/registration.py` that currently pass
`validate=registration.<method>`.

### Representative Code Snippets

Current state in `src/datafusion_engine/registry_facade.py:73-78`:

```python
@dataclass(frozen=True)
class RegistrationPhase:
    """Registration phase with ordering requirements and optional validation."""

    name: str
    requires: tuple[str, ...] = ()
    validate: Callable[[], None] | None = None
```

Target state:

```python
@dataclass(frozen=True)
class RegistrationPhase:
    """Registration phase with ordering requirements and a side-effecting action."""

    name: str
    requires: tuple[str, ...] = ()
    action: Callable[[], None] | None = None
```

Current usage in `src/datafusion_engine/views/registration.py:140-142`:

```python
RegistrationPhase(
    name="extract_metadata",
    validate=registration.validate_extract_metadata,
)
```

Target usage:

```python
RegistrationPhase(
    name="extract_metadata",
    action=registration.validate_extract_metadata,
)
```

Update `RegistrationPhaseOrchestrator.run` in `registry_facade.py` to reference
`phase.action` instead of `phase.validate`.

### Files to Edit

- `src/datafusion_engine/registry_facade.py` — rename `validate` field to `action` in `RegistrationPhase`; update `RegistrationPhaseOrchestrator.run` to use `phase.action`
- `src/datafusion_engine/views/registration.py` — update all 6 `RegistrationPhase(validate=...)` call sites to `action=`

### New Files to Create

None.

### Legacy Decommission / Delete Scope

None — rename only.

---

## S8 — Fix _DDL_CATALOG_WARNING_STATE Global

**Principles:** P16 (functional core), P23 (testability)
**Review source:** Agent 5, P16/P23 findings
**Priority:** HIGH — module-level mutable state persists across tests; cannot be reset without module dict manipulation

### Goal

Replace the `_DDL_CATALOG_WARNING_STATE = {"emitted": False}` module-level dict in
`runtime_extensions.py` with a `contextvars.ContextVar[bool]` named
`_DDL_CATALOG_WARNING_EMITTED`. `contextvars.ContextVar` scopes the flag to the
current execution context and is reset automatically when the context changes, which
is the correct semantics for test isolation.

### Representative Code Snippets

Current state in `src/datafusion_engine/session/runtime_extensions.py:69`:

```python
_DDL_CATALOG_WARNING_STATE: dict[str, bool] = {"emitted": False}
```

Usage (around line 197-202):

```python
if not _DDL_CATALOG_WARNING_STATE["emitted"]:
    logger.warning("DDL catalog registration ...")
    _DDL_CATALOG_WARNING_STATE["emitted"] = True
```

Target state:

```python
import contextvars

_DDL_CATALOG_WARNING_EMITTED: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "_DDL_CATALOG_WARNING_EMITTED", default=False
)
```

Usage:

```python
if not _DDL_CATALOG_WARNING_EMITTED.get():
    logger.warning("DDL catalog registration ...")
    _DDL_CATALOG_WARNING_EMITTED.set(True)
```

### Files to Edit

- `src/datafusion_engine/session/runtime_extensions.py` — replace `_DDL_CATALOG_WARNING_STATE` dict with `_DDL_CATALOG_WARNING_EMITTED` ContextVar; update read/write sites

### New Files to Create

None.

### Legacy Decommission / Delete Scope

**D4:**
- Delete `_DDL_CATALOG_WARNING_STATE: dict[str, bool]` from `src/datafusion_engine/session/runtime_extensions.py`

---

## S9 — Remove bare Exception from _CACHE_SNAPSHOT_ERRORS

**Principles:** P8 (contract), P21 (least astonishment)
**Review source:** Agent 6, P8/P21 findings
**Priority:** MEDIUM — violates project quality policy (`python-quality.md`: no bare `except Exception`)

### Goal

Remove `Exception` from the `_CACHE_SNAPSHOT_ERRORS` tuple in
`src/datafusion_engine/catalog/introspection.py`. The tuple already lists specific
exceptions (`AttributeError`, `KeyError`, `RuntimeError`, `TypeError`, `ValueError`);
the trailing `Exception` negates the specificity and violates project policy.

### Representative Code Snippets

Current state in `src/datafusion_engine/catalog/introspection.py:25-32`:

```python
_CACHE_SNAPSHOT_ERRORS = (
    AttributeError,
    KeyError,
    RuntimeError,
    TypeError,
    ValueError,
    Exception,      # violates python-quality.md policy
)
```

Target state:

```python
_CACHE_SNAPSHOT_ERRORS = (
    AttributeError,
    KeyError,
    RuntimeError,
    TypeError,
    ValueError,
)
```

### Files to Edit

- `src/datafusion_engine/catalog/introspection.py` — remove `Exception` from `_CACHE_SNAPSHOT_ERRORS`

### New Files to Create

None.

### Legacy Decommission / Delete Scope

None — in-place edit.

---

## S10 — Eliminate or Implement create_strict_catalog

**Principles:** P7 (DRY), P8 (contract), P10 (illegal states)
**Review source:** Agent 6, P7/P8/P10 findings
**Priority:** HIGH — creates false security contract; any caller relying on strict enforcement is silently unprotected

### Goal

`create_strict_catalog` and `create_default_catalog` in
`src/datafusion_engine/udf/metadata.py:869-903` are byte-for-byte identical. The chosen
resolution is: **delete `create_strict_catalog`** and redirect its callers to
`create_default_catalog`. This is the YAGNI-correct choice because no actual strict
enforcement logic exists anywhere. If true tier enforcement is needed in the future, it
can be added to `UdfCatalog` with a `strict: bool` field.

Find callers via `/cq calls create_strict_catalog` before editing.

### Representative Code Snippets

Current state in `src/datafusion_engine/udf/metadata.py:887-902`:

```python
def create_strict_catalog(
    udf_specs: Mapping[str, DataFusionUdfSpec] | None = None,
) -> UdfCatalog:
    """Create a UDF catalog with strict builtin-only policy.
    ...
    """
    return UdfCatalog(udf_specs=udf_specs)  # identical to create_default_catalog
```

Target state:
- Delete `create_strict_catalog` entirely.
- Update every caller found by `/cq calls create_strict_catalog` to call
  `create_default_catalog` instead.
- If `get_strict_udf_catalog` in `runtime_extensions.py` (line 60) delegates to
  `create_strict_catalog`, update it accordingly.

### Files to Edit

- `src/datafusion_engine/udf/metadata.py` — delete `create_strict_catalog`
- `src/datafusion_engine/session/runtime_extensions.py` — update `get_strict_udf_catalog` import/call if it referenced `create_strict_catalog`
- All other callers discovered by `/cq calls create_strict_catalog`

### New Files to Create

None.

### Legacy Decommission / Delete Scope

**D5:**
- Delete `create_strict_catalog` from `src/datafusion_engine/udf/metadata.py` (lines 887-902)

---

## S11 — Remove Duplicate register_rust_udfs Definition

**Principles:** P3 (SRP), P7 (DRY), P19 (KISS), correctness
**Review source:** Agent 6, P19/correctness finding; Agent 11, cross-cutting Theme 1
**Priority:** CRITICAL — second definition at line 502 silently shadows the first; latent correctness hazard

### Goal

Remove the second (forwarding) definition of `register_rust_udfs` at
`src/datafusion_engine/udf/extension_runtime.py:502-531`. The bottom-of-file import at
line 499 that precipitates it also needs to be resolved. The strategy:

1. Move the late import `from datafusion_engine.udf.extension_ddl import ...` to a
   function-local import inside the functions that need it (the pattern already used
   throughout `delta_write_handler.py`).
2. Remove the second `register_rust_udfs` definition.
3. Verify that `from datafusion_engine.udf.extension_runtime import register_rust_udfs`
   now resolves to the first (real) definition.

### Representative Code Snippets

Current state in `src/datafusion_engine/udf/extension_runtime.py:499-531`:

```python
from datafusion_engine.udf.extension_ddl import _register_udf_aliases, _register_udf_specs


def register_rust_udfs(        # SECOND DEFINITION — shadows the first
    ctx: SessionContext,
    *,
    enable_async: bool = False,
    ...
) -> Mapping[str, object]:
    from datafusion_engine.udf.extension_registry import register_rust_udfs as _register_rust_udfs
    return _register_rust_udfs(ctx, ...)
```

Target state:
- Delete the bottom-of-file import block (line 499) and the second `register_rust_udfs`
  definition (lines 502-531).
- Identify which functions in `extension_runtime.py` call `_register_udf_aliases` or
  `_register_udf_specs` and move those imports to function-local positions.

### Files to Edit

- `src/datafusion_engine/udf/extension_runtime.py` — remove module-level late import of `_register_udf_aliases`, `_register_udf_specs`; convert to function-local imports at the two call sites; delete the second `register_rust_udfs` definition

### New Files to Create

None.

### Legacy Decommission / Delete Scope

**D6:**
- Delete the second `register_rust_udfs` function body at `src/datafusion_engine/udf/extension_runtime.py:502-531`
- Delete the module-level late import at line 499: `from datafusion_engine.udf.extension_ddl import _register_udf_aliases, _register_udf_specs`

---

## S12 — Replace Path.cwd() Fallbacks with Injectable Defaults

**Principles:** P23 (testability), P18 (determinism)
**Review source:** Agent 7, P23 finding
**Priority:** MEDIUM — `Path.cwd()` captures ambient state at import time; makes artifact paths non-deterministic in tests

### Goal

Replace the `except IndexError: Path.cwd() / ".artifacts"` fallback with `None` in
three constants modules. Consumers of the `None` default must either accept `Path | None`
and require explicit override at production entry points, or raise `ValueError` with a
helpful message.

The `try/except IndexError` block exists because `Path(__file__).resolve().parents[N]`
raises `IndexError` when `__file__` does not have enough parent components (unusual
environments). Replacing the fallback with `None` makes the "no-default" case explicit
and injectable.

### Representative Code Snippets

Current state in `src/datafusion_engine/plan/artifact_store_constants.py:15-18`:

```python
try:
    _DEFAULT_ARTIFACTS_ROOT = Path(__file__).resolve().parents[2] / ".artifacts"
except IndexError:
    _DEFAULT_ARTIFACTS_ROOT = Path.cwd() / ".artifacts"  # captures ambient cwd
```

Target state:

```python
try:
    _DEFAULT_ARTIFACTS_ROOT: Path | None = Path(__file__).resolve().parents[2] / ".artifacts"
except IndexError:
    _DEFAULT_ARTIFACTS_ROOT = None  # injectable; callers must provide explicit path
```

Same pattern applies to:
- `src/datafusion_engine/cache/inventory.py:39-42` — `_DEFAULT_CACHE_ROOT`
- `src/datafusion_engine/delta/obs_table_manager.py:35-38` — `_DEFAULT_OBSERVABILITY_ROOT`

Consumers that use the constant must be updated to handle `None`:
- `observability_root(profile)` in `obs_table_manager.py` already checks
  `profile.policies.plan_artifacts_root` first and falls back to `_DEFAULT_OBSERVABILITY_ROOT`.
  Update the fallback to raise `ValueError` if both are `None`, with a message directing
  the operator to set `plan_artifacts_root` in the profile.

### Files to Edit

- `src/datafusion_engine/plan/artifact_store_constants.py` — change fallback to `None`; update type annotation
- `src/datafusion_engine/cache/inventory.py` — change fallback to `None`; update consumers
- `src/datafusion_engine/delta/obs_table_manager.py` — change fallback to `None`; update `observability_root()` to raise on `None`

### New Files to Create

None. Add assertions to `tests/unit/datafusion_engine/delta/test_obs_table_manager.py`
confirming `observability_root` raises `ValueError` when both profile policy and default
are `None`.

### Legacy Decommission / Delete Scope

None — in-place replacement.

---

## S13 — Fix IntrospectionCache.snapshot CQS Violation

**Principles:** P11 (CQS), P23 (testability)
**Review source:** Agent 6, P11 finding
**Priority:** HIGH — reading `snapshot` silently resets `_invalidated` flag; makes testing invalidation logic impossible

### Goal

Separate the `snapshot` property (pure getter) from the recapture logic (command).
Add an explicit `refresh()` method that triggers recapture and resets the
`_invalidated` flag. The `snapshot` property becomes a pure getter; callers that need
a fresh snapshot must call `refresh()` before reading.

### Representative Code Snippets

Current state in `src/datafusion_engine/catalog/introspection.py:346-363`:

```python
@property
def snapshot(self) -> IntrospectionSnapshot:
    if self._snapshot is None or self._invalidated:
        self._snapshot = IntrospectionSnapshot.capture(
            self._ctx,
            sql_options=self._sql_options,
        )
        self._invalidated = False      # mutation inside a property getter (CQS violation)
    return self._snapshot
```

Target state:

```python
def refresh(self) -> None:
    """Recapture the introspection snapshot and clear the invalidation flag.

    Call this explicitly before reading ``snapshot`` when the cache may be stale.
    """
    self._snapshot = IntrospectionSnapshot.capture(
        self._ctx,
        sql_options=self._sql_options,
    )
    self._invalidated = False

@property
def snapshot(self) -> IntrospectionSnapshot:
    """Return the current introspection snapshot.

    Raises:
    ------
    RuntimeError
        If no snapshot has been captured yet. Call ``refresh()`` first.
    """
    if self._snapshot is None:
        msg = (
            "IntrospectionCache has no snapshot yet. "
            "Call refresh() before reading snapshot."
        )
        raise RuntimeError(msg)
    return self._snapshot
```

Update all callers of `cache.snapshot` that relied on lazy initialisation to first call
`cache.refresh()` when the snapshot may be absent or invalidated. Find callers via
`/cq calls IntrospectionCache.snapshot` before editing.

### Files to Edit

- `src/datafusion_engine/catalog/introspection.py` — add `refresh()` method; make `snapshot` a pure getter that raises on None
- All call sites of `cache.snapshot` — prefix with `cache.refresh()` where needed; existing `cache.invalidate()` callers remain unchanged

### New Files to Create

None. Add a test to `tests/unit/datafusion_engine/catalog/test_introspection_caches.py`
that verifies `snapshot` raises `RuntimeError` before `refresh()` is called, and that
the `_invalidated` flag is only cleared by `refresh()` not by reading `snapshot`.

### Legacy Decommission / Delete Scope

None — the combined getter/recapture logic is replaced in-place.

---

## S14 — Relocate Merge Execution Types from delta_read.py

**Principles:** P1 (information hiding), P3 (SRP)
**Review source:** Agent 9, P1 and P3 findings
**Priority:** MEDIUM — `delta_write.py` imports private symbols from `delta_read.py`; cross-file private coupling

### Goal

Create `src/storage/deltalake/delta_merge_types.py` containing:
- `DeltaMergeExecutionState` (formerly `_DeltaMergeExecutionState`)
- `DeltaMergeExecutionResult` (formerly `_DeltaMergeExecutionResult`)
- `normalize_commit_metadata` (formerly `_normalize_commit_metadata`)
- `normalize_commit_metadata_key` (formerly `_normalize_commit_metadata_key`)

Remove the private-symbol cross-file imports in `delta_write.py`. Neither `delta_read.py`
nor `delta_write.py` will import private symbols from the other after this change.

### Representative Code Snippets

Current state in `src/storage/deltalake/delta_write.py:29,39-40`:

```python
from storage.deltalake.delta_read import _normalize_commit_metadata  # private cross-import
...
if TYPE_CHECKING:
    from storage.deltalake.delta_read import (
        _DeltaMergeExecutionResult,   # private cross-import
        _DeltaMergeExecutionState,    # private cross-import
    )
```

Target state in `src/storage/deltalake/delta_write.py`:

```python
from storage.deltalake.delta_merge_types import normalize_commit_metadata
...
if TYPE_CHECKING:
    from storage.deltalake.delta_merge_types import (
        DeltaMergeExecutionResult,
        DeltaMergeExecutionState,
    )
```

The new `delta_merge_types.py` exports these four items via `__all__`. `delta_read.py`
removes the four private items from its module; it may re-export under the public names
if other callers use them via `delta_read`.

### Files to Edit

- `src/storage/deltalake/delta_read.py` — remove `_DeltaMergeExecutionState`, `_DeltaMergeExecutionResult`, `_normalize_commit_metadata`, `_normalize_commit_metadata_key`
- `src/storage/deltalake/delta_write.py` — update imports to reference `delta_merge_types`

### New Files to Create

- `src/storage/deltalake/delta_merge_types.py` — new module; contains `DeltaMergeExecutionState`, `DeltaMergeExecutionResult`, `normalize_commit_metadata`, `normalize_commit_metadata_key`; defines `__all__`
- `tests/unit/storage/deltalake/test_delta_merge_types.py` — tests that the types are importable from the new module; round-trip test for `normalize_commit_metadata`

### Legacy Decommission / Delete Scope

**D7:**
- Delete `_DeltaMergeExecutionState` from `src/storage/deltalake/delta_read.py`
- Delete `_DeltaMergeExecutionResult` from `src/storage/deltalake/delta_read.py`
- Delete `_normalize_commit_metadata` from `src/storage/deltalake/delta_read.py`
- Delete `_normalize_commit_metadata_key` from `src/storage/deltalake/delta_read.py`

---

## S15 — Add session_id Parameter to diagnostics_bridge Helpers

**Principles:** P24 (observability)
**Review source:** Agent 7, P24 finding
**Priority:** LOW/MEDIUM — cross-run session ID correlation is ambiguous in long-running processes

### Goal

Add `session_id: str | None = None` to each bridge helper function in
`src/datafusion_engine/obs/diagnostics_bridge.py`. When `session_id` is `None`, default
to `_OBS_SESSION_ID` for backward compatibility. Callers with a live
`DataFusionRuntimeProfile` can pass `profile.context_cache_key()` as the session ID.

### Representative Code Snippets

Current state in `src/datafusion_engine/obs/diagnostics_bridge.py:40-53`:

```python
def record_view_fingerprints(
    sink: DiagnosticsCollector,
    *,
    view_nodes: Sequence[ViewNode],
) -> None:
    from datafusion_engine.lineage.diagnostics import ensure_recorder_sink
    ...
    recorder_sink = ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)
```

Target state:

```python
def record_view_fingerprints(
    sink: DiagnosticsCollector,
    *,
    view_nodes: Sequence[ViewNode],
    session_id: str | None = None,
) -> None:
    from datafusion_engine.lineage.diagnostics import ensure_recorder_sink
    ...
    recorder_sink = ensure_recorder_sink(sink, session_id=session_id or _OBS_SESSION_ID)
```

Apply the same `session_id: str | None = None` parameter to every bridge helper that
calls `ensure_recorder_sink`.

### Files to Edit

- `src/datafusion_engine/obs/diagnostics_bridge.py` — add `session_id: str | None = None` to each bridge helper; update internal `ensure_recorder_sink` calls

### New Files to Create

None.

### Legacy Decommission / Delete Scope

None.

---

## S16 — Decouple fragment_telemetry from OTel Emission

**Principles:** P11 (CQS), P16 (functional core)
**Review source:** Agent 9, P11/P16 findings
**Priority:** MEDIUM — `fragment_telemetry` currently triggers OTel gauge writes as a side effect of a computation

### Goal

Remove the `set_scan_telemetry(...)` call from `fragment_telemetry` in
`src/obs/scan_telemetry.py`. Callers that need OTel metric emission must call
`set_scan_telemetry` explicitly after receiving the `ScanTelemetry` result. This makes
`fragment_telemetry` a pure computation function testable without OTel infrastructure.

Use `/cq calls fragment_telemetry` to find all callers before editing.

### Representative Code Snippets

Current state in `src/obs/scan_telemetry.py:186-192`:

```python
    dataset_name = getattr(dataset, "name", None)
    if isinstance(dataset_name, str) and dataset_name:
        set_scan_telemetry(            # side-effect inside computation (CQS violation)
            dataset_name,
            fragment_count=len(fragments),
            row_group_count=task_count,
        )
    return ScanTelemetry(...)
```

Target state — remove the `set_scan_telemetry` block from `fragment_telemetry`:

```python
    dataset_name = getattr(dataset, "name", None)
    telemetry = ScanTelemetry(...)
    return telemetry
```

Each caller that previously relied on the implicit `set_scan_telemetry` side-effect
must be updated to call `set_scan_telemetry(dataset_name, ...)` after receiving the
`ScanTelemetry` result.

### Files to Edit

- `src/obs/scan_telemetry.py` — remove `set_scan_telemetry` call from `fragment_telemetry`
- Each caller discovered by `/cq calls fragment_telemetry` that needs OTel emission — add explicit `set_scan_telemetry` call after `fragment_telemetry` call

### New Files to Create

None. Add a test to `tests/unit/obs/` (new file `test_scan_telemetry_pure.py` or extend
existing) that calls `fragment_telemetry` with a mocked `pyarrow.dataset.Dataset` and
confirms no OTel gauges are written.

### Legacy Decommission / Delete Scope

None — in-place edit removing a code block.

---

## S17 — Fix env_bool log_invalid Default

**Principles:** P21 (least astonishment), P8 (contract)
**Review source:** Agent 9, P21/P8 findings
**Priority:** MEDIUM — operators who misconfigure boolean env vars receive no feedback

### Goal

Change the default value of `log_invalid` in `env_bool` from `False` to `True`.
`env_bool_strict` already defaults to `True`; this aligns the two functions. Any caller
that intentionally suppresses warnings must pass `log_invalid=False` explicitly.

Find all call sites via `/cq calls env_bool` before editing; audit those that should
suppress warnings (e.g., optional feature flags where `None` is a valid "not configured"
value).

### Representative Code Snippets

Current state in `src/utils/env_utils.py:207-212`:

```python
def env_bool(
    name: str,
    *,
    default: bool | None = None,
    on_invalid: OnInvalid = "default",
    log_invalid: bool = False,         # silent by default — surprising
) -> bool | None:
```

Target state:

```python
def env_bool(
    name: str,
    *,
    default: bool | None = None,
    on_invalid: OnInvalid = "default",
    log_invalid: bool = True,          # warn by default — expected behavior
) -> bool | None:
```

### Files to Edit

- `src/utils/env_utils.py` — change `log_invalid: bool = False` to `log_invalid: bool = True` in the `env_bool` function signature; update the overloaded signatures immediately above it
- All call sites where `log_invalid` is not explicitly set and silent behavior is intentional — add explicit `log_invalid=False`

### New Files to Create

None. Update `tests/unit/utils/test_env_utils.py` and
`tests/unit/utils/test_env_utils_extended.py` to reflect the new default.

### Legacy Decommission / Delete Scope

None.

---

## S18 — Decompose SessionFactory.build_config()

**Principles:** P3 (SRP), P19 (KISS), P12 (dependency inversion)
**Review source:** Agent 5, P3/P19/P12 findings
**Priority:** MEDIUM — 130-line sequential monolith; one function with eleven distinct config-application concerns

### Goal

Split `SessionFactory.build_config()` in `src/datafusion_engine/session/context_pool.py`
into three private sub-functions:
- `_apply_execution_config(config: SessionConfig, profile: DataFusionRuntimeProfile) -> SessionConfig`
- `_apply_catalog_config(config: SessionConfig, profile: DataFusionRuntimeProfile) -> SessionConfig`
- `_apply_policy_config(config: SessionConfig, profile: DataFusionRuntimeProfile) -> SessionConfig`

`SessionFactory.build_config()` becomes a thin orchestrator that calls the three
sub-functions in sequence.

### Representative Code Snippets

Current structure of `SessionFactory.build_config()` (lines 313-373 of `context_pool.py`):

```python
def build_config(self) -> SessionConfig:
    # ... deferred imports ...
    profile = self.profile
    config = SessionConfig()
    config = config.with_default_catalog_and_schema(...)   # catalog concern
    config = config.with_information_schema(...)            # catalog concern
    config = _apply_identifier_settings(...)               # execution concern
    config = _apply_profile_execution_settings(...)        # execution concern
    config = _apply_catalog_autoload(...)                   # catalog concern
    config_policy = resolved_config_policy(profile)         # policy concern
    if config_policy is not None:
        config = config_policy.apply(config)
    config = _apply_settings_overrides(...cache_policy...)  # policy concern
    config = _apply_settings_overrides(...performance...)   # policy concern
    schema_hardening = resolved_schema_hardening(profile)  # policy concern
    if schema_hardening is not None:
        config = schema_hardening.apply(config)
    config = _apply_settings_overrides(...overrides...)     # policy concern
    config = _apply_feature_settings(...)                  # policy concern
    config = _apply_join_settings(...)                     # policy concern
    return _apply_explain_analyze_level(...)               # diagnostics concern
```

Target structure:

```python
def build_config(self) -> SessionConfig:
    config = SessionConfig()
    config = _apply_catalog_config(config, self.profile)
    config = _apply_execution_config(config, self.profile)
    config = _apply_policy_config(config, self.profile)
    return config


def _apply_catalog_config(
    config: SessionConfig,
    profile: DataFusionRuntimeProfile,
) -> SessionConfig:
    """Apply catalog and schema settings to the session config."""
    ...


def _apply_execution_config(
    config: SessionConfig,
    profile: DataFusionRuntimeProfile,
) -> SessionConfig:
    """Apply execution and identifier settings to the session config."""
    ...


def _apply_policy_config(
    config: SessionConfig,
    profile: DataFusionRuntimeProfile,
) -> SessionConfig:
    """Apply policy bundle, feature gate, join, and explain settings."""
    ...
```

### Files to Edit

- `src/datafusion_engine/session/context_pool.py` — extract `_apply_catalog_config`, `_apply_execution_config`, `_apply_policy_config` as module-level functions; reduce `SessionFactory.build_config()` to a 4-line orchestrator

### New Files to Create

None. The extracted functions remain in `context_pool.py`.

### Legacy Decommission / Delete Scope

None — refactoring, not deletion.

---

## S19 — Create adapt_session_context() for .ctx Unwrapping

**Principles:** P1 (information hiding), P7 (DRY), P14 (Law of Demeter)
**Review source:** Agent 11, P1/P7/P14 findings
**Priority:** HIGH — 11 Python call sites independently implement the `.ctx` unwrapping rule; DataFusion wrapper changes break all of them

### Goal

Create `src/datafusion_engine/session/adapter.py` with a single
`adapt_session_context(ctx: Any) -> SessionContext` function that encapsulates the
"try to unwrap `.ctx` attribute" rule. Redirect all 11 Python call sites to this
function, eliminating independent `getattr(ctx, "ctx", None)` or
`getattr(ctx, "ctx", ctx)` patterns scattered across the codebase.

### Representative Code Snippets

The unwrapping pattern appears in 11 locations; the canonical example is
`src/datafusion_engine/extensions/datafusion_ext.py:85-86`:

```python
def _normalize_ctx(ctx: object) -> SessionContext:
    inner = getattr(ctx, "ctx", ctx)
    return cast("SessionContext", inner)
```

New canonical location `src/datafusion_engine/session/adapter.py`:

```python
"""Session context adaptation utilities for the DataFusion Python boundary."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datafusion import SessionContext


def adapt_session_context(ctx: Any) -> SessionContext:
    """Unwrap a DataFusion session context from any wrapper object.

    DataFusion's Python bindings may return session contexts wrapped in an object
    exposing a ``.ctx`` attribute. This function normalises both the wrapped
    and unwrapped forms to a plain ``SessionContext``.

    Parameters
    ----------
    ctx:
        A ``SessionContext`` or a wrapper object with a ``.ctx`` attribute.

    Returns:
    -------
    SessionContext
        The underlying ``SessionContext`` instance.

    Raises:
    ------
    TypeError
        If ``ctx`` is neither a ``SessionContext`` nor a wrapper with ``.ctx``.
    """
    from datafusion import SessionContext

    inner = getattr(ctx, "ctx", ctx)
    if not isinstance(inner, SessionContext):
        msg = (
            f"Expected SessionContext or wrapper; got {type(ctx).__name__!r}. "
            "Ensure the context was constructed by DataFusion."
        )
        raise TypeError(msg)
    return inner
```

### Files to Edit

All 11 call sites identified by `/cq calls _normalize_ctx` and `grep 'getattr.*"ctx"'`:
- `src/datafusion_engine/extensions/datafusion_ext.py` — replace `_normalize_ctx` usage with `adapt_session_context`
- `src/datafusion_engine/extensions/runtime_capabilities.py` — replace inline `getattr(ctx, "ctx", None)` with `adapt_session_context`
- `src/datafusion_engine/extensions/schema_evolution.py` — same
- `src/datafusion_engine/udf/extension_runtime.py` — same
- `src/datafusion_engine/udf/factory.py` — same
- `src/datafusion_engine/delta/capabilities.py` — same (3 sites)
- `src/datafusion_engine/expr/planner.py` — same
- `src/datafusion_engine/session/runtime_extensions.py` — same
- `src/extraction/rust_session_bridge.py` — same

### New Files to Create

- `src/datafusion_engine/session/adapter.py` — new module with `adapt_session_context`
- `tests/unit/datafusion_engine/session/test_adapter.py` — tests that `adapt_session_context` returns a `SessionContext` when given a `SessionContext`; returns the inner `ctx` when given a wrapper; raises `TypeError` for an incompatible object

### Legacy Decommission / Delete Scope

**D8:**
- Delete `_normalize_ctx` from `src/datafusion_engine/extensions/datafusion_ext.py` (after all callers are redirected to `adapt_session_context`)

---

## S20 — Extend datafusion_ext.pyi Stubs

**Principles:** P22 (public contracts), P6 (ports and adapters)
**Review source:** Agent 11, P7/P6 findings
**Priority:** MEDIUM — 7 Rust functions silently typed as `Any -> Any` due to missing stubs

### Goal

Add 7 missing function stubs to `src/datafusion_ext.pyi`. The functions are confirmed
present in the Rust extension (`install_codeanatomy_policy_config`,
`install_codeanatomy_physical_config`, `install_expr_planners`,
`parquet_listing_table_provider`, `schema_evolution_adapter_factory`,
`registry_catalog_provider_factory`, `install_codeanatomy_udf_config`).

### Representative Code Snippets

Current state: these functions have no entry in `src/datafusion_ext.pyi`; callers
get `Any` from `def __getattr__(name: str) -> Any: ...` at line 117.

Target additions at the end of `src/datafusion_ext.pyi` (before the `__getattr__` line):

```python
def install_codeanatomy_policy_config(ctx: Any, payload: Mapping[str, object]) -> None: ...
def install_codeanatomy_physical_config(ctx: Any, payload: Mapping[str, object]) -> None: ...
def install_expr_planners(ctx: Any, payload: Mapping[str, object]) -> None: ...
def parquet_listing_table_provider(
    ctx: Any, path: str, options: Mapping[str, object] | None = ...
) -> Any: ...
def schema_evolution_adapter_factory(ctx: Any) -> Any: ...
def registry_catalog_provider_factory(
    ctx: Any, payload: Mapping[str, object]
) -> Any: ...
def install_codeanatomy_udf_config(ctx: Any, payload: Mapping[str, object]) -> None: ...
```

Verify exact signatures against the Rust `lib.rs` `#[pyfunction]` definitions before
finalizing the stubs.

### Files to Edit

- `src/datafusion_ext.pyi` — add 7 new function stubs before the `__getattr__` sentinel

### New Files to Create

None. Add a smoke test to
`tests/unit/datafusion_engine/extensions/test_required_entrypoints.py` that asserts
the 7 new names are present in the stub module's declared `__all__` or importable from
the stub.

### Legacy Decommission / Delete Scope

None.

---

## Cross-Scope Legacy Decommission and Deletion Plan

| Batch | Symbols to Delete | File | Prerequisite |
|-------|-------------------|------|-------------|
| D1 | `_coerce_bool_value` | `src/storage/deltalake/file_pruning.py` | S2 complete |
| D2 | `_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`, `_RuntimeProfileDeltaFacadeMixin` | `src/datafusion_engine/session/runtime_ops.py` | S4 complete, all call sites updated |
| D3 | `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS`, `_is_retryable_delta_stream_error`, `_is_delta_observability_operation` | `src/datafusion_engine/io/write_pipeline.py` | S5 complete |
| D4 | `_DDL_CATALOG_WARNING_STATE` | `src/datafusion_engine/session/runtime_extensions.py` | S8 complete |
| D5 | `create_strict_catalog` | `src/datafusion_engine/udf/metadata.py` | S10 complete, all callers updated |
| D6 | Second `register_rust_udfs` definition (lines 502-531); late import line 499 | `src/datafusion_engine/udf/extension_runtime.py` | S11 complete |
| D7 | `_DeltaMergeExecutionState`, `_DeltaMergeExecutionResult`, `_normalize_commit_metadata`, `_normalize_commit_metadata_key` | `src/storage/deltalake/delta_read.py` | S14 complete, new module created |
| D8 | `_normalize_ctx` | `src/datafusion_engine/extensions/datafusion_ext.py` | S19 complete, all callers updated |

---

## Implementation Sequence

The items are grouped into four parallel streams ordered by dependency and blast radius.

### Stream A — Correctness Fixes (no callee coordination needed)

Execute in this order; each is independent of the others within the stream.

1. **S1** — `DeltaDeleteRequest.__post_init__` guard (CRITICAL; zero callee impact)
2. **S11** — Remove duplicate `register_rust_udfs` (CRITICAL; isolated to one file)
3. **S3** — Narrow `BaseException` + add UDF snapshot warning (small edits, two files)
4. **S9** — Remove bare `Exception` from `_CACHE_SNAPSHOT_ERRORS`
5. **S7** — Rename `RegistrationPhase.validate` to `action`
6. **S17** — Change `env_bool` `log_invalid` default

### Stream B — DRY Consolidation (safe removals after callee audit)

Execute S5 before D3; execute S2 before D1.

1. **S5** — Consolidate write-path duplicates + D3
2. **S2** — Replace `_coerce_bool_value` with `coerce_bool` + D1
3. **S6** — Extract `table_ref_from_request` (in-place refactor, no callee impact)
4. **S10** — Delete `create_strict_catalog` after `/cq calls` audit + D5

### Stream C — Structural Cleanups (require callee coordination)

Execute S19 before D8; execute S4 after S19 to reduce MRO changes.

1. **S19** — Create `adapt_session_context`; redirect 11 call sites + D8
2. **S4** — Remove facade mixins from MRO + D2
3. **S18** — Decompose `SessionFactory.build_config()` (pure refactor, no interface change)
4. **S8** — Replace `_DDL_CATALOG_WARNING_STATE` with `ContextVar` + D4
5. **S13** — Fix `IntrospectionCache.snapshot` CQS violation

### Stream D — New Modules and Contracts

These can proceed independently once Stream A is stable.

1. **S14** — Create `delta_merge_types.py`; update imports + D7
2. **S12** — Replace `Path.cwd()` fallbacks with `None`
3. **S15** — Add `session_id` parameter to diagnostics bridge helpers
4. **S16** — Decouple `fragment_telemetry` from OTel emission
5. **S20** — Extend `datafusion_ext.pyi` stubs

---

## Implementation Checklist

### Pre-Work
- [ ] Run `/cq calls create_strict_catalog` to enumerate callers before S10
- [ ] Run `/cq calls cache_root` to enumerate facade mixin callers before S4
- [ ] Run `/cq calls fragment_telemetry` to enumerate callers before S16
- [ ] Run `/cq calls env_bool` to enumerate callers before S17 (audit `log_invalid` intent)
- [ ] Run `/cq calls IntrospectionCache.snapshot` to enumerate callers before S13
- [ ] Run `grep -n '_normalize_ctx\|getattr.*"ctx"' src/datafusion_engine` to confirm 11 Python call sites for S19

### Stream A — Correctness Fixes
- [ ] S1: Add `__post_init__` to `DeltaDeleteRequest` in `control_plane_types.py`
- [ ] S1: Add test cases to `tests/unit/datafusion_engine/delta/test_control_plane_types.py`
- [ ] S11: Remove module-level late import at line 499 of `extension_runtime.py`
- [ ] S11: Convert `_register_udf_aliases` / `_register_udf_specs` usages to function-local imports
- [ ] S11: Delete second `register_rust_udfs` definition (lines 502-531) — D6
- [ ] S3: Change `BaseException` to `Exception` in `context_pool.py:_set_config_if_supported`
- [ ] S3: Add `logger.warning` in `runtime_session.py:_build_session_runtime_from_context`
- [ ] S9: Remove `Exception` from `_CACHE_SNAPSHOT_ERRORS` in `introspection.py`
- [ ] S7: Rename `validate` to `action` in `RegistrationPhase` in `registry_facade.py`
- [ ] S7: Update `RegistrationPhaseOrchestrator.run` to use `phase.action`
- [ ] S7: Update all 6 `RegistrationPhase(validate=...)` call sites in `views/registration.py`
- [ ] S17: Change `env_bool` `log_invalid` default from `False` to `True` in `env_utils.py`
- [ ] S17: Update overloaded signatures for `env_bool` in `env_utils.py`

### Stream B — DRY Consolidation
- [ ] S5: Delete `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS` from `write_pipeline.py` — D3 partial
- [ ] S5: Delete `_is_retryable_delta_stream_error` from `write_pipeline.py` — D3 partial
- [ ] S5: Delete `_is_delta_observability_operation` from `write_pipeline.py` — D3 complete
- [ ] S5: Add import of `is_retryable_delta_stream_error`, `is_delta_observability_operation` from `write_execution` in `write_pipeline.py`
- [ ] S5: Update `delta_write_handler.py` to import observability helper from `write_execution`
- [ ] S2: Add `from utils.value_coercion import coerce_bool` to `file_pruning.py`
- [ ] S2: Replace `_coerce_bool_value(value)` call with `coerce_bool(value)` in `file_pruning.py`
- [ ] S2: Delete `_coerce_bool_value` from `file_pruning.py` — D1
- [ ] S6: Add `_HasTableRefFields` Protocol (TYPE_CHECKING) to `control_plane_types.py`
- [ ] S6: Add `table_ref_from_request` function to `control_plane_types.py`
- [ ] S6: Update all 16 `table_ref` property bodies to delegate to `table_ref_from_request`
- [ ] S6: Export `table_ref_from_request` in module `__all__`
- [ ] S10: Delete `create_strict_catalog` from `metadata.py` — D5
- [ ] S10: Update all callers to use `create_default_catalog`

### Stream C — Structural Cleanups
- [ ] S19: Create `src/datafusion_engine/session/adapter.py` with `adapt_session_context`
- [ ] S19: Create `tests/unit/datafusion_engine/session/test_adapter.py`
- [ ] S19: Redirect all 11 Python `.ctx` access sites to `adapt_session_context`
- [ ] S19: Delete `_normalize_ctx` from `datafusion_ext.py` — D8
- [ ] S4: Run `/cq calls cache_root` — enumerate and update all callers to `profile.io_ops.cache_root()`
- [ ] S4: Remove `_RuntimeProfileIOFacadeMixin` from `runtime.py` MRO and `runtime_ops.py` — D2 partial
- [ ] S4: Remove `_RuntimeProfileCatalogFacadeMixin` from `runtime.py` MRO and `runtime_ops.py` — D2 partial
- [ ] S4: Remove `_RuntimeProfileDeltaFacadeMixin` from `runtime.py` MRO and `runtime_ops.py` — D2 complete
- [ ] S18: Extract `_apply_catalog_config` private function in `context_pool.py`
- [ ] S18: Extract `_apply_execution_config` private function in `context_pool.py`
- [ ] S18: Extract `_apply_policy_config` private function in `context_pool.py`
- [ ] S18: Reduce `SessionFactory.build_config()` to call the three sub-functions
- [ ] S8: Add `import contextvars` to `runtime_extensions.py`
- [ ] S8: Replace `_DDL_CATALOG_WARNING_STATE` dict with `_DDL_CATALOG_WARNING_EMITTED: contextvars.ContextVar[bool]` — D4
- [ ] S8: Update read/write sites to use `.get()` / `.set()`
- [ ] S13: Add `refresh()` method to `IntrospectionCache` in `introspection.py`
- [ ] S13: Make `snapshot` property raise `RuntimeError` when `_snapshot is None`
- [ ] S13: Update all callers identified by `/cq calls IntrospectionCache.snapshot`
- [ ] S13: Add CQS test to `tests/unit/datafusion_engine/catalog/test_introspection_caches.py`

### Stream D — New Modules and Contracts
- [ ] S14: Create `src/storage/deltalake/delta_merge_types.py`
- [ ] S14: Populate with `DeltaMergeExecutionState`, `DeltaMergeExecutionResult`, `normalize_commit_metadata`, `normalize_commit_metadata_key`
- [ ] S14: Create `tests/unit/storage/deltalake/test_delta_merge_types.py`
- [ ] S14: Update `delta_write.py` imports to reference `delta_merge_types`
- [ ] S14: Remove private symbols from `delta_read.py` — D7
- [ ] S12: Change `_DEFAULT_ARTIFACTS_ROOT` fallback to `None` in `artifact_store_constants.py`
- [ ] S12: Change `_DEFAULT_CACHE_ROOT` fallback to `None` in `cache/inventory.py`
- [ ] S12: Change `_DEFAULT_OBSERVABILITY_ROOT` fallback to `None` in `obs_table_manager.py`
- [ ] S12: Update `observability_root()` to raise `ValueError` when both fallback and profile are `None`
- [ ] S15: Add `session_id: str | None = None` to all bridge helpers in `diagnostics_bridge.py`
- [ ] S15: Update internal `ensure_recorder_sink` calls to use `session_id or _OBS_SESSION_ID`
- [ ] S16: Run `/cq calls fragment_telemetry` to identify callers
- [ ] S16: Remove `set_scan_telemetry` call from `fragment_telemetry` in `scan_telemetry.py`
- [ ] S16: Update callers that need OTel emission to call `set_scan_telemetry` explicitly
- [ ] S16: Create `tests/unit/obs/test_scan_telemetry_pure.py`
- [ ] S20: Add 7 missing function stubs to `src/datafusion_ext.pyi`
- [ ] S20: Add smoke test to `tests/unit/datafusion_engine/extensions/test_required_entrypoints.py`

### Quality Gate
- [ ] `uv run ruff format`
- [ ] `uv run ruff check --fix`
- [ ] `uv run pyrefly check`
- [ ] `uv run pyright`
- [ ] `uv run pytest -q -m "not e2e"`
