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
DataFusion Python layer, Python infrastructure, and cross-language planning/runtime
contracts. The work spans `src/datafusion_engine/`, `src/storage/`, `src/obs/`,
`src/utils/`, `src/semantics/`, `src/extract/`, `rust/codeanatomy_engine/src/session/`,
`rust/codeanatomy_engine/src/compiler/`, `rust/datafusion_python/src/`, and
`rust/datafusion_python/python/datafusion/`.

All 30 scope items (S1-S30) are sequenced in waves: Wave 0 (contract/scope reset),
Wave 1 (canonical planning/runtime contracts), Wave 2 (artifact/identity unification),
and Wave 3 (targeted cleanup backlog execution).

---

## Design Principles

1. All changes are backward-compatible at the public API level; no removal of exported
   symbols without a redirect or tombstone.
2. Every new module gets a corresponding test file.
3. Decommission lists name exact symbols — no "clean up misc helpers" language.
4. Code snippets are derived from the actual source files read during plan preparation.
5. Changes that touch `frozen=True` msgspec structs use `object.__setattr__` only inside
   `__post_init__`; callers never see the mutation.
6. `uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright` is the canonical quality gate run after all edits.
7. Pytest selection is task-scoped validation and is tracked separately from the canonical gate.

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
| `src/storage/deltalake/file_pruning.py` | ~450 | `_coerce_bool_value` duplicates canonical `coerce_bool`; semantics drift risk for unknown strings |
| `src/storage/deltalake/delta_read.py` | ~330 | Owns `_DeltaMergeExecutionState`, `_DeltaMergeExecutionResult`, `_normalize_commit_metadata` — imported as privates by `delta_write.py` |
| `src/datafusion_ext.pyi` | 118 | Missing 7 Rust function stubs |
| `src/datafusion_engine/session/delta_session_builder.py` | ~620 | Runtime policy settings are split across bridge + Python parsers; duplicate policy parsing surface |
| `src/datafusion_engine/plan/profiler.py` | ~170 | EXPLAIN capture still relies on redirected stdout text parsing |
| `src/datafusion_engine/plan/planning_env.py` | ~170 | Planning manifest payload is derived in Python; Rust manifest should be canonical parity source |
| `src/semantics/plans/fingerprints.py` | ~380 | Recomputes plan/schema/substrait hashes that overlap with plan artifact identity payload |
| `src/extract/infrastructure/worklists.py` | ~450 | Plan bundle consumers depend on current artifact identity shape without explicit parity contract tests |
| `rust/codeanatomy_engine/src/session/planning_surface.rs` | ~180 | Canonical typed planning-surface policy contract already exists but is not the sole cross-layer source |
| `rust/codeanatomy_engine/src/session/planning_manifest.rs` | ~700 | Manifest includes provider identities and policy hash surfaces not fully enforced in Python parity checks |
| `rust/datafusion_python/src/utils.rs` | ~200 | Capsule provider path invokes provider hooks with session argument |
| `rust/datafusion_python/python/datafusion/context.py` | ~600 | Provider Protocol signatures still advertise zero-argument capsule hooks |

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
**Priority:** HIGH — duplicate coercion logic drifts from canonical behavior and obscures unknown-value handling semantics

### Goal

Delete `_coerce_bool_value` from `file_pruning.py` and replace all call sites with
`utils.value_coercion.coerce_bool`. The key issue is duplicate parsing policy and drift:
`_coerce_bool_value` reduces unknown strings to `False`, while canonical `coerce_bool`
returns `None` on parse failure. This change removes duplicate logic and restores
consistent unknown-value semantics.

### Representative Code Snippets

Current state in `src/storage/deltalake/file_pruning.py:422-427`:

```python
def _coerce_bool_value(value: object) -> bool | None:
    if isinstance(value, str):
        return value.lower() == "true"
    if isinstance(value, (bool, int, float)):
        return bool(value)
    return None
```

`_coerce_bool_value("foo")` returns `False`, but `coerce_bool("foo")` returns `None`.
The plan standardizes on `coerce_bool` so "invalid/unknown" remains distinguishable from
explicit `False`.

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
`test_file_pruning_coercion.py`) confirming unknown strings (for example `"foo"`)
return `None` instead of being silently coerced to `False`.

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

## S4 — Stage Facade Mixin Migration for DataFusionRuntimeProfile

**Principles:** P13 (composition over inheritance), P4 (high cohesion), P22 (public contracts)
**Review source:** Agent 5, P13 finding
**Priority:** MEDIUM/HIGH — MRO simplification is valuable, but removal must be compatibility-safe

### Goal

Convert `_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`, and
`_RuntimeProfileDeltaFacadeMixin` to explicit compatibility wrappers in the first pass:
keep method names available, emit deprecation warnings, and migrate call sites to
`profile.io_ops`, `profile.catalog_ops`, and `profile.delta_ops`. Remove mixins only
after call-site migration and compatibility-window completion.

### Representative Code Snippets

Current MRO in `src/datafusion_engine/session/runtime.py:212-217`:

```python
class DataFusionRuntimeProfile(
    _RuntimeProfileIdentityMixin,
    _RuntimeProfileQueryMixin,
    _RuntimeProfileIOFacadeMixin,
    _RuntimeProfileCatalogFacadeMixin,
    _RuntimeProfileDeltaFacadeMixin,
    _RuntimeDiagnosticsMixin,
    _RuntimeContextMixin,
    StructBaseStrict,
    frozen=True,
):
```

First-pass target in `src/datafusion_engine/session/runtime_ops.py`:

```python
def cache_root(self) -> Path | None:
    warnings.warn(
        "DataFusionRuntimeProfile.cache_root() is deprecated; use profile.io_ops.cache_root().",
        DeprecationWarning,
        stacklevel=2,
    )
    return self.io_ops.cache_root()
```

Second-pass target (separate removal batch):
- remove the three facade mixins from `DataFusionRuntimeProfile` MRO,
- delete wrapper methods after `/cq calls` confirms no remaining call sites.

### Files to Edit

- `src/datafusion_engine/session/runtime_ops.py` — add deprecation warnings on facade methods; keep delegation logic intact in first pass
- `src/datafusion_engine/session/runtime.py` — keep current MRO in first pass; remove mixins only in deferred deletion batch
- All call sites of facade methods — migrate to `profile.io_ops.X()`, `profile.catalog_ops.X()`, `profile.delta_ops.X()`

### New Files to Create

None.

### Legacy Decommission / Delete Scope

**D2:**
- Delete `_RuntimeProfileIOFacadeMixin` class body from `src/datafusion_engine/session/runtime_ops.py` only after call-site migration + deprecation window
- Delete `_RuntimeProfileCatalogFacadeMixin` class body from `src/datafusion_engine/session/runtime_ops.py` only after call-site migration + deprecation window
- Delete `_RuntimeProfileDeltaFacadeMixin` class body from `src/datafusion_engine/session/runtime_ops.py` only after call-site migration + deprecation window

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
**Priority:** HIGH — warning state must follow session/runtime lifecycle, not process-global module state

### Goal

Replace `_DDL_CATALOG_WARNING_STATE = {"emitted": False}` with explicit session-scoped
warning state keyed by context object. Use `weakref.WeakKeyDictionary[SessionContext, bool]`
to avoid process-global stickiness while preserving one-warning-per-session behavior.
Add a dedicated test reset helper for deterministic test isolation.

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
_DDL_CATALOG_WARNING_EMITTED_BY_CTX: weakref.WeakKeyDictionary[SessionContext, bool] = (
    weakref.WeakKeyDictionary()
)


def _should_emit_ddl_catalog_warning(ctx: SessionContext) -> bool:
    if bool(_DDL_CATALOG_WARNING_EMITTED_BY_CTX.get(ctx)):
        return False
    _DDL_CATALOG_WARNING_EMITTED_BY_CTX[ctx] = True
    return True
```

Usage:

```python
if _should_emit_ddl_catalog_warning(ctx):
    logger.warning("DDL catalog registration ...")
```

### Files to Edit

- `src/datafusion_engine/session/runtime_extensions.py` — replace `_DDL_CATALOG_WARNING_STATE` dict with `_DDL_CATALOG_WARNING_EMITTED_BY_CTX`; add `_should_emit_ddl_catalog_warning(ctx)` helper and `_reset_ddl_catalog_warning_state_for_tests()` helper
- `tests/unit/datafusion_engine/session/` warning-state tests — assert one warning per session and deterministic reset behavior

### New Files to Create

None.

### Legacy Decommission / Delete Scope

**D4:**
- Delete `_DDL_CATALOG_WARNING_STATE: dict[str, bool]` from `src/datafusion_engine/session/runtime_extensions.py` after weak-map helper is wired at all call sites

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
`src/datafusion_engine/udf/metadata.py:869-903` are byte-for-byte identical. First-pass
resolution is compatibility-safe: keep `create_strict_catalog` as a public symbol and
implement strict semantics (preferred) or ship a deprecation shim with explicit warning
until strict semantics land. Do not hard-delete the symbol in this pass.

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
- Keep `create_strict_catalog` symbol.
- Implement strict semantics in `UdfCatalog` (for example `strict: bool`) and enforce in
  strict catalog call paths; or keep the symbol as a deprecating shim with warning.
- Preserve `get_strict_udf_catalog` public behavior in `runtime_extensions.py`.

### Files to Edit

- `src/datafusion_engine/udf/metadata.py` — implement strict semantics in `create_strict_catalog` path or add a deprecation shim while retaining symbol
- `src/datafusion_engine/session/runtime_extensions.py` — keep `get_strict_udf_catalog` behavior stable while strict semantics are wired
- All callers discovered by `/cq calls create_strict_catalog` — validate no behavior regression under strict mode

### New Files to Create

None.

### Legacy Decommission / Delete Scope

**D5:**
- Optional deferred deletion only after explicit deprecation window and replacement strict contract adoption

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

Add an explicit `refresh()` command API while keeping current lazy `snapshot` getter
behavior for compatibility in first pass. The getter continues to recapture when
`_snapshot is None` or `_invalidated` is true; new strict paths can opt into
raise-on-missing behavior only after call-site migration is complete.

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
    """Recapture the introspection snapshot and clear the invalidation flag."""
    self._snapshot = IntrospectionSnapshot.capture(
        self._ctx,
        sql_options=self._sql_options,
    )
    self._invalidated = False

@property
def snapshot(self) -> IntrospectionSnapshot:
    """Backward-compatible lazy getter; refreshes when invalidated."""
    if self._snapshot is None or self._invalidated:
        self.refresh()
    return self._snapshot
```

Optional strict mode (deferred): add `snapshot_or_raise()` or feature-flagged strict
getter behavior after `/cq calls IntrospectionCache.snapshot` migration audit.

### Files to Edit

- `src/datafusion_engine/catalog/introspection.py` — add `refresh()` method while preserving lazy `snapshot` semantics
- Call sites that require explicit recapture boundaries — migrate to call `refresh()` directly; no forced mass call-site rewrite in first pass

### New Files to Create

None. Add a test to `tests/unit/datafusion_engine/catalog/test_introspection_caches.py`
that verifies lazy getter behavior is preserved and that `refresh()` is available as
explicit command API.

### Legacy Decommission / Delete Scope

None — strict-mode enforcement is deferred to a compatibility-window follow-up scope.

---

## S14 — Relocate Merge Execution Types from delta_read.py

**Principles:** P1 (information hiding), P3 (SRP)
**Review source:** Agent 9, P1 and P3 findings
**Priority:** HIGH — `delta_write.py` imports private symbols from `delta_read.py`; boundary coupling risk

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

## S21 — Make Rust Planning Manifest the Canonical Cross-Layer Source

**Principles:** P1 (information hiding), P7 (DRY), P12 (dependency inversion), P22 (versioned contracts)
**Review source:** Alignment DX1
**Priority:** CRITICAL — planning identity drift is possible while Python derives manifest payloads independently

### Goal

Use Rust planning-surface manifest capture as the canonical source and have Python consume
that payload directly. Keep Python local derivation only as compatibility fallback while
parity tests are introduced.

### Representative Code Snippets

Current state in `src/datafusion_engine/plan/planning_env.py:87-95`:

```python
from datafusion_engine.session.runtime_extensions import planning_surface_manifest_v2_payload
...
manifest_payload = planning_surface_manifest_v2_payload(profile)
```

Current bridge state in `rust/datafusion_python/src/codeanatomy_ext/plan_bundle_bridge.rs:77`:

```rust
planning_surface_hash: [0_u8; 32], // placeholder; not canonical session hash
```

Target state:

```python
# src/datafusion_engine/plan/planning_env.py
manifest_bridge = getattr(datafusion_ext, "session_planning_manifest_v2", None)
if callable(manifest_bridge):
    manifest_payload = manifest_bridge(session_runtime.ctx)
else:
    manifest_payload = planning_surface_manifest_v2_payload(profile)
```

```rust
// rust/datafusion_python/src/codeanatomy_ext/session_utils.rs
#[pyfunction]
pub(crate) fn session_planning_manifest_v2(...) -> PyResult<Py<PyAny>> { ... }
```

### Files to Edit

- `src/datafusion_engine/plan/planning_env.py` — prefer Rust-provided manifest payload; keep fallback path
- `src/datafusion_engine/session/runtime_extensions.py` — treat local `planning_surface_manifest_v2_payload` as fallback/compatibility helper
- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs` — add `session_planning_manifest_v2` bridge entrypoint
- `rust/datafusion_python/src/codeanatomy_ext/mod.rs` — register any new bridge entrypoint
- `rust/datafusion_python/src/codeanatomy_ext/plan_bundle_bridge.rs` — replace `[0_u8; 32]` planning-surface hash placeholder with canonical hash payload
- `src/datafusion_ext.pyi` — add typing stub for the new manifest bridge entrypoint

### New Files to Create

- `rust/datafusion_python/tests/session_planning_manifest_bridge_tests.rs` — verify entrypoint registration + payload shape
- `tests/unit/datafusion_engine/plan/test_planning_env_manifest_parity.py` — assert Python snapshot matches Rust manifest payload and hash

### Legacy Decommission / Delete Scope

- Decommission Python-first manifest derivation as primary path in `src/datafusion_engine/plan/planning_env.py` once Rust parity tests are stable
- Delete zero-hash placeholder usage in `rust/datafusion_python/src/codeanatomy_ext/plan_bundle_bridge.rs`

---

## S22 — Unify Runtime Policy Parsing with Typed Runtime Contracts

**Principles:** P7 (DRY), P9 (parse, don't validate), P18 (determinism)
**Review source:** Alignment DX2
**Priority:** HIGH — runtime policy parsing is split between typed bridge options and ad-hoc Python string parsing

### Goal

Centralize runtime policy parsing into one typed contract surface and generate
`DeltaSessionRuntimePolicyOptions` from that contract. Remove duplicated manual parsing
branches in `delta_session_builder.py` after migration.

### Representative Code Snippets

Current duplicated parsing path in `src/datafusion_engine/session/delta_session_builder.py:328-430`:

```python
def _parse_suffixed_runtime_size(value: str) -> int | None: ...
def _parse_suffixed_runtime_duration_seconds(value: str) -> int | None: ...
def _apply_standard_runtime_policy_settings(...): ...
```

Target state:

```python
# src/datafusion_engine/session/delta_session_builder.py
contract = runtime_policy_contract_from_settings(runtime_settings)
options = contract.to_delta_session_runtime_policy_options(module=resolution.module_owner)
```

```python
# src/datafusion_engine/session/runtime_policy_contract.py
@dataclass(frozen=True)
class RuntimePolicyContract:
    memory_limit_bytes: int | None
    metadata_cache_limit_bytes: int | None
    list_files_cache_limit_bytes: int | None
    list_files_cache_ttl_seconds: int | None
    temp_directory: str | None
    max_temp_directory_size_bytes: int | None
```

### Files to Edit

- `src/datafusion_engine/session/delta_session_builder.py` — replace ad-hoc parsing branches with contract conversion
- `src/datafusion_engine/session/cache_manager_contract.py` — reuse/extend existing cache policy parsing primitives
- `rust/datafusion_python/src/codeanatomy_ext/delta_session_bridge.rs` — keep typed options as canonical runtime bridge input

### New Files to Create

- `src/datafusion_engine/session/runtime_policy_contract.py` — typed runtime policy parsing contract
- `tests/unit/datafusion_engine/session/test_runtime_policy_contract.py` — parser + conversion tests
- `tests/unit/datafusion_engine/session/test_delta_session_builder_runtime_policy.py` — bridge parity tests (consumed/unsupported key payloads)

### Legacy Decommission / Delete Scope

- Delete `_parse_suffixed_runtime_size` from `src/datafusion_engine/session/delta_session_builder.py` after contract migration
- Delete `_parse_suffixed_runtime_duration_seconds` from `src/datafusion_engine/session/delta_session_builder.py` after contract migration
- Delete `_apply_standard_runtime_policy_settings` from `src/datafusion_engine/session/delta_session_builder.py` after contract migration

---

## S23 — Align Provider Capsule Protocol Signatures with Session-Aware FFI

**Principles:** P6 (ports and adapters), P22 (versioned contracts)
**Review source:** Alignment DX3
**Priority:** HIGH — Rust bridge calls provider hooks with `session`, but Python Protocols still document no-arg hooks

### Goal

Update Python provider Protocol contracts to include a `session` argument and ship
compatibility handling for legacy no-argument provider implementations during migration.

### Representative Code Snippets

Current Python contract in `rust/datafusion_python/python/datafusion/context.py:93-105`:

```python
def __datafusion_table_provider__(self) -> object: ...
def __datafusion_catalog_provider__(self) -> object: ...
```

Current Rust invocation in `rust/datafusion_python/src/utils.rs:171-176`:

```rust
let capsule = obj
    .getattr("__datafusion_table_provider__")?
    .call1((session,))?;
```

Target Python contract:

```python
def __datafusion_table_provider__(self, session: SessionContext) -> object: ...
def __datafusion_catalog_provider__(self, session: SessionContext) -> object: ...
def __datafusion_schema_provider__(self, session: SessionContext) -> object: ...
```

### Files to Edit

- `rust/datafusion_python/python/datafusion/context.py` — update `TableProviderExportable` and `CatalogProviderExportable` signatures/docs
- `rust/datafusion_python/python/datafusion/catalog.py` — update `SchemaProviderExportable` signature/docs
- `rust/datafusion_python/src/utils.rs` — add compatibility fallback handling for legacy no-arg hooks (time-boxed)
- `rust/datafusion_python/src/catalog.rs` and `rust/datafusion_python/src/context.rs` — keep session-aware invocation consistent for catalog/schema paths

### New Files to Create

- `tests/unit/datafusion_engine/extensions/test_provider_capsule_signature_compat.py` — Python contract tests for session-arg and legacy-arg forms
- `rust/datafusion_python/tests/provider_capsule_signature_contract_tests.rs` — Rust-side invocation compatibility tests

### Legacy Decommission / Delete Scope

- Delete temporary no-arg fallback invocation path in `rust/datafusion_python/src/utils.rs` after compatibility window

---

## S24 — Replace Stdout Explain Scraping with DataFrame Explain Rows

**Principles:** P11 (CQS), P18 (determinism)
**Review source:** Alignment DX4
**Priority:** HIGH — stdout scraping is brittle and diverges from structured Rust explain capture

### Goal

Capture explain artifacts from DataFrame result rows (`plan_type`, `plan`) instead of
redirected stdout text. Keep text fallback only for explicit compatibility path.

### Representative Code Snippets

Current state in `src/datafusion_engine/plan/profiler.py:102-121`:

```python
buffer = io.StringIO()
with contextlib.redirect_stdout(buffer):
    explain_method(verbose=verbose, analyze=analyze)
text = buffer.getvalue()
```

Target state:

```python
explain_df = df.explain(verbose=verbose, analyze=analyze)
batches = explain_df.collect()
rows = explain_rows_from_batches(batches)
```

Rust reference path in `rust/codeanatomy_engine/src/compiler/plan_bundle.rs:738-756`
already parses explain record batches into typed entries.

### Files to Edit

- `src/datafusion_engine/plan/profiler.py` — replace stdout capture as primary path
- `src/datafusion_engine/plan/plan_utils.py` — add/extend helpers for explain row extraction from batches
- `src/datafusion_engine/plan/bundle_assembly.py` — ensure artifact builder consumes structured explain rows

### New Files to Create

- `tests/unit/datafusion_engine/plan/test_explain_row_capture.py` — row-based explain capture tests
- `tests/unit/datafusion_engine/plan/test_explain_text_fallback.py` — explicit fallback behavior tests

### Legacy Decommission / Delete Scope

- Delete `_run_explain_text` from `src/datafusion_engine/plan/profiler.py` once row-based path is stable
- Decommission `explain_rows_from_text` in `src/datafusion_engine/plan/plan_utils.py` as primary parser (keep optional fallback only)

---

## S25 — Expand Planning-Env Identity to Object Store and Provider Surfaces

**Principles:** P18 (determinism), P22 (versioned contracts), P24 (observability)
**Review source:** Alignment DX5
**Priority:** HIGH — object-store/catalog/schema provider identity is not fully reflected in Python planning parity payloads

### Goal

Include object store registration identities and provider identity snapshots in
`planning_env_snapshot` so Python parity payloads cover the same environment identity
surface as Rust planning manifests.

### Representative Code Snippets

Current object store registration tracking in `src/datafusion_engine/io/adapter.py:169-176`:

```python
registry = registries.by_context.setdefault(self.ctx, set())
key = (scheme, host)
...
registry.add(key)
```

Target planning-env payload enrichment:

```python
snapshot["object_store_identities"] = tuple(sorted(...))
snapshot["catalog_provider_identities"] = tuple(sorted(...))
snapshot["schema_provider_identities"] = tuple(sorted(...))
```

Rust manifest reference in `rust/codeanatomy_engine/src/session/planning_manifest.rs:295-296`:
`catalog_provider_identities` and `schema_provider_identities` are part of canonical manifest capture.

### Files to Edit

- `src/datafusion_engine/plan/planning_env.py` — include object-store/catalog/schema identity fields in snapshot payload
- `src/datafusion_engine/io/adapter.py` — expose deterministic registry snapshot helper for planning-env capture
- `src/datafusion_engine/plan/plan_identity.py` — include new identity fields in deterministic identity payload

### New Files to Create

- `tests/unit/datafusion_engine/plan/test_planning_env_provider_identities.py` — parity tests for object store and provider identity payloads

### Legacy Decommission / Delete Scope

- Decommission ad-hoc provider identity derivation paths once planning-env payload is canonical for these fields

---

## S26 — Promote Schema Pushdown and Physical Expr Adapter to Contract Tests

**Principles:** P8 (contract), P23 (testability)
**Review source:** Alignment DX6
**Priority:** HIGH — pushdown and adapter registration behavior should be contract-enforced, not opportunistic

### Goal

Define explicit conformance tests for physical expression adapter registration and schema
evolution pushdown behavior, including required fallback/error behavior when registration
surfaces are missing.

### Representative Code Snippets

Current adapter install path in `src/datafusion_engine/session/runtime_extensions.py:1038-1069`:

```python
register = getattr(ctx, "register_physical_expr_adapter_factory", None)
if not callable(register):
    ...
    raise TypeError(msg)
register(factory)
```

Existing Rust contract test anchor in
`rust/datafusion_python/tests/codeanatomy_ext_schema_pushdown_contract_tests.rs`.

Target expansion:

```python
# add integration assertions that predicate/projection pushdown behavior is preserved
# when schema evolution adapter is installed and when it is unavailable.
```

### Files to Edit

- `src/datafusion_engine/session/runtime_extensions.py` — clarify diagnostics payloads for adapter install success/failure
- `src/datafusion_engine/dataset/registration_delta_helpers.py` — enforce deterministic fallback behavior when adapter registration is unavailable
- `rust/datafusion_python/tests/codeanatomy_ext_schema_pushdown_contract_tests.rs` — extend contract coverage

### New Files to Create

- `tests/unit/datafusion_engine/dataset/test_schema_pushdown_contract.py` — Python-side contract tests

### Legacy Decommission / Delete Scope

None.

---

## S27 — Unify Semantics Fingerprints with Canonical Plan Identity Artifacts

**Principles:** P7 (DRY), P18 (determinism), P22 (versioned contracts)
**Review source:** Alignment DX7
**Priority:** HIGH — semantics still recomputes hashes that overlap with canonical plan artifact identity

### Goal

Make semantics fingerprinting consume canonical `plan_identity_hash`/`plan_fingerprint`
from plan bundle artifacts by default. Retain bespoke hash derivation only as fallback
when bundle artifacts are unavailable.

### Representative Code Snippets

Current bespoke hash path in `src/semantics/plans/fingerprints.py:184-299`:

```python
logical_hash = _compute_logical_plan_hash(df, view_name=view_name)
schema_hash = _compute_schema_hash(df)
substrait_hash = _compute_substrait_hash(ctx, df)
```

Target path:

```python
identity = runtime_plan_identity(bundle)
return PlanFingerprint(plan_fingerprint=identity, ...)
```

### Files to Edit

- `src/semantics/plans/fingerprints.py` — prefer bundle-provided identity and reduce bespoke hashing to fallback
- `src/semantics/pipeline_builders.py` — keep semantic cache payload anchored to canonical runtime identity
- `src/semantics/pipeline_diagnostics.py` — ensure diagnostics show canonical plan identity fields

### New Files to Create

- `tests/unit/semantics/plans/test_fingerprint_identity_alignment.py` — verifies semantics output matches plan bundle identity

### Legacy Decommission / Delete Scope

- Decommission unconditional use of `_compute_logical_plan_hash`/`_compute_schema_hash` in `src/semantics/plans/fingerprints.py` as primary path

---

## S28 — Enforce Listing Partition-Inference Key in Identity Contracts

**Principles:** P8 (contract), P18 (determinism)
**Review source:** Alignment DX8
**Priority:** MEDIUM/HIGH — missing planning-affecting keys can silently weaken plan-identity determinism

### Goal

Require `datafusion.execution.listing_table_factory_infer_partitions` to be present in
policy settings and plan identity assembly. Fail identity assembly when missing.

### Representative Code Snippets

Current tolerant path in `src/datafusion_engine/plan/plan_identity.py:110-114`:

```python
listing_partition_inference = str(
    inputs.artifacts.df_settings.get(
        "datafusion.execution.listing_table_factory_infer_partitions",
        "",
    )
)
```

Target enforcement:

```python
required_key = "datafusion.execution.listing_table_factory_infer_partitions"
if required_key not in inputs.artifacts.df_settings:
    raise ValueError(f"Missing required planning-affecting DataFusion setting: {required_key}")
```

### Files to Edit

- `src/datafusion_engine/session/runtime_config_policies.py` — ensure required key exists in all relevant presets
- `src/datafusion_engine/plan/plan_identity.py` — enforce required key presence
- `src/datafusion_engine/plan/bundle_assembly.py` — fail early when required planning keys are missing

### New Files to Create

- `tests/unit/datafusion_engine/plan/test_plan_identity_required_settings.py` — required-key enforcement tests

### Legacy Decommission / Delete Scope

None.

---

## S29 — Align Delta DML Flow with Provider-Native Semantics Where Feasible

**Principles:** P3 (SRP), P6 (ports and adapters), P19 (KISS)
**Review source:** Alignment DX9
**Priority:** MEDIUM/HIGH — control-plane wrappers are broad; provider-native hooks can reduce bespoke mutation orchestration

### Goal

Audit delete/update/merge flows and move operations that are representable via provider-native
DataFusion/Delta surfaces to those hooks, retaining wrapper logic only for policy/audit
requirements not represented by provider-native contracts.

### Representative Code Snippets

Current wrapper-heavy mapping in `src/datafusion_engine/delta/control_plane_core.py:393-396`:

```python
from datafusion_engine.delta.control_plane_mutation import (
    delta_delete,
    delta_merge,
    delta_update,
)
```

Target stance:

```python
# Keep control-plane wrapper only for policy/audit boundaries.
# Delegate core mutation execution to provider-native surfaces when available.
```

### Files to Edit

- `src/datafusion_engine/delta/capabilities.py` — classify operations that can use provider-native semantics
- `src/datafusion_engine/delta/control_plane_mutation.py` — narrow wrapper scope to policy/audit boundaries
- `src/datafusion_engine/delta/service.py` — route compatible operations through provider-native mutation path
- `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs` — align extension surface for provider-native DML routing

### New Files to Create

- `tests/unit/datafusion_engine/delta/test_provider_native_dml_alignment.py` — behavior parity tests between wrapper and provider-native paths

### Legacy Decommission / Delete Scope

- Decommission wrapper-only mutation branches in `src/datafusion_engine/delta/control_plane_mutation.py` once provider-native routing and policy/audit equivalence are validated

---

## S30 — Co-Migrate Extract/Semantics Plan Consumers with Contract Changes

**Principles:** P12 (dependency inversion), P18 (determinism), P22 (versioned contracts)
**Review source:** Alignment DX10
**Priority:** HIGH — producer/consumer split risks plan-identity and explain-contract drift

### Goal

Update `src/extract` and `src/semantics` plan consumers in the same plan wave as
planning manifest, identity, and explain artifact changes to prevent split-brain behavior.

### Representative Code Snippets

Current extract consumer in `src/extract/infrastructure/worklists.py:270-279`:

```python
bundle = facade.compile_to_bundle(builder)
execution = execute_plan_artifact_helper(..., bundle, ...)
```

Current semantics consumer in `src/semantics/pipeline_builders.py:83-93`:

```python
base_identity = runtime_plan_identity(bundle)
...
return replace(bundle, plan_identity_hash=semantic_cache_hash)
```

Target stance:

```python
# Consumers must validate new planning-env fields and explain-row contracts
# when reading plan bundles / diagnostics payloads.
```

### Files to Edit

- `src/extract/infrastructure/worklists.py` — validate required bundle identity fields and planning-env contract version
- `src/semantics/pipeline_builders.py` — align semantic cache key construction with canonical plan identity contract version
- `src/semantics/pipeline_diagnostics.py` — include canonical identity + explain row fields in diagnostics output

### New Files to Create

- `tests/unit/extract/infrastructure/test_worklist_plan_contract_alignment.py` — extract consumer contract tests
- `tests/unit/semantics/test_pipeline_plan_contract_alignment.py` — semantics consumer contract tests

### Legacy Decommission / Delete Scope

None.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S2)
- Delete `_coerce_bool_value` from `src/storage/deltalake/file_pruning.py` because canonical `coerce_bool` fully replaces it.

### Batch D2 (after S4 migration window + S30 consumer migration)
- Delete `_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`, and `_RuntimeProfileDeltaFacadeMixin` from `src/datafusion_engine/session/runtime_ops.py` after zero remaining facade-method call sites.

### Batch D3 (after S5)
- Delete `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS`, `_is_retryable_delta_stream_error`, and `_is_delta_observability_operation` duplicates from `src/datafusion_engine/io/write_pipeline.py`.

### Batch D4 (after S8)
- Delete `_DDL_CATALOG_WARNING_STATE` from `src/datafusion_engine/session/runtime_extensions.py` after weak-map/session-scoped replacement is active.

### Batch D5 (after S10 deprecation window)
- Delete `create_strict_catalog` only after strict semantics replacement is established and compatibility window is complete.

### Batch D6 (after S11)
- Delete second `register_rust_udfs` definition and late module import from `src/datafusion_engine/udf/extension_runtime.py`.

### Batch D7 (after S14)
- Delete `_DeltaMergeExecutionState`, `_DeltaMergeExecutionResult`, `_normalize_commit_metadata`, and `_normalize_commit_metadata_key` from `src/storage/deltalake/delta_read.py`.

### Batch D8 (after S19)
- Delete `_normalize_ctx` from `src/datafusion_engine/extensions/datafusion_ext.py` after all sites use `adapt_session_context`.

### Batch D9 (after S21 + S25 parity tests)
- Decommission Python-first manifest derivation as primary path in `src/datafusion_engine/plan/planning_env.py`.

### Batch D10 (after S22)
- Delete legacy runtime policy parsing helpers from `src/datafusion_engine/session/delta_session_builder.py`.

### Batch D11 (after S23 migration window)
- Delete temporary no-arg provider hook compatibility fallback from `rust/datafusion_python/src/utils.rs`.

### Batch D12 (after S24 adoption)
- Delete stdout explain capture path from `src/datafusion_engine/plan/profiler.py` and retire text parser as primary logic in `src/datafusion_engine/plan/plan_utils.py`.

### Batch D13 (after S27 + S30)
- Decommission bespoke semantics hash-first identity path in `src/semantics/plans/fingerprints.py` once bundle identity is mandatory.

---

## Implementation Sequence

### Wave 0 — Contract and Scope Reset (must complete first)

1. **S21** — establish Rust-first canonical planning-manifest source and eliminate zero-hash placeholders.
2. **S23** — align provider Protocol signatures with session-aware FFI surface.
3. **S22** — unify runtime policy parsing into typed contract.
4. **S30** — align `src/extract` and `src/semantics` consumers to forthcoming contract changes.
5. Apply compatibility-safe strategy updates for **S4**, **S8**, **S10**, and **S13** before implementation coding begins.

### Wave 1 — Canonical Planning/Runtime Contracts

1. **S24** — switch to row-based explain capture.
2. **S25** — include object-store/catalog/schema provider identities in planning-env payloads.
3. **S26** — promote pushdown/adapter behavior to explicit conformance tests.
4. **S28** — enforce required listing partition inference key in identity contracts.
5. **S29** — move eligible DML flow to provider-native semantics with policy/audit preservation.
6. **S14** — land high-priority delta read/write boundary fix.

### Wave 2 — Cleanup Backlog with Compatibility Guards

1. Execute correctness-critical fixes: **S1**, **S3**, **S9**, **S11**.
2. Execute mechanical dedup/refactors: **S2**, **S5**, **S6**, **S7**, **S12**, **S15**, **S16**, **S17**, **S18**, **S19**, **S20**.
3. Execute compatibility-safe API cleanups: **S4**, **S8**, **S10**, **S13** (stage-first, delete-later).

### Wave 3 — Identity Unification and Deferred Deletions

1. Execute identity unification in **S27** after Waves 0-2 are stable.
2. Run decommission batches **D1**, **D3**, **D4**, **D6**, **D7**, **D8** immediately after prerequisites.
3. Run deferred batches **D2**, **D5**, **D9**, **D10**, **D11**, **D12**, **D13** only after compatibility windows and parity tests.

---

## Implementation Checklist

### Pre-Work
- [ ] Run `/cq calls create_strict_catalog` before S10 implementation.
- [ ] Run `/cq calls cache_root` before S4 migration.
- [ ] Run `/cq calls IntrospectionCache.snapshot` before S13 compatibility migration.
- [ ] Run `/cq calls fragment_telemetry` before S16 call-site updates.
- [ ] Run `/cq calls env_bool` before S17 default change.
- [ ] Run `/cq search planning_surface_manifest --in src/datafusion_engine --format summary` before S21.
- [ ] Run `/cq search "__datafusion_table_provider__" --in rust/datafusion_python/python --format summary` before S23.

### Scope Items
- [ ] S1 — Fix DeltaDeleteRequest predicate empty-string guard.
- [ ] S2 — Consolidate bool coercion via canonical `coerce_bool`.
- [ ] S3 — Narrow `BaseException` and add UDF snapshot warning.
- [ ] S4 — Stage facade mixin migration with compatibility wrappers.
- [ ] S5 — Consolidate write-path duplicates.
- [ ] S6 — Extract `table_ref` boilerplate helper.
- [ ] S7 — Rename `RegistrationPhase.validate` to `action`.
- [ ] S8 — Replace process-global warning state with lifecycle-owned state.
- [ ] S9 — Remove bare `Exception` from `_CACHE_SNAPSHOT_ERRORS`.
- [ ] S10 — Implement/deprecate strict catalog path without immediate symbol deletion.
- [ ] S11 — Remove duplicate `register_rust_udfs` definition.
- [ ] S12 — Replace `Path.cwd()` fallbacks with injectable defaults.
- [ ] S13 — Add `refresh()` while keeping backward-compatible lazy snapshot behavior.
- [ ] S14 — Relocate merge execution types to shared public module.
- [ ] S15 — Add `session_id` parameter to diagnostics bridge helpers.
- [ ] S16 — Decouple `fragment_telemetry` from OTel emission.
- [ ] S17 — Change `env_bool` `log_invalid` default.
- [ ] S18 — Decompose `SessionFactory.build_config()`.
- [ ] S19 — Introduce `adapt_session_context()` and redirect call sites.
- [ ] S20 — Extend `datafusion_ext.pyi` stubs.
- [ ] S21 — Make Rust manifest canonical and wire parity path in Python.
- [ ] S22 — Unify runtime policy parsing under typed contract.
- [ ] S23 — Align provider Protocol signatures with session-aware FFI.
- [ ] S24 — Replace explain stdout scraping with row-based capture.
- [ ] S25 — Expand planning-env identity to object store/provider surfaces.
- [ ] S26 — Add schema pushdown / adapter conformance tests.
- [ ] S27 — Unify semantics fingerprints with canonical plan identity artifacts.
- [ ] S28 — Enforce required listing-partition-inference key in identity contracts.
- [ ] S29 — Align DML flow to provider-native semantics where feasible.
- [ ] S30 — Co-migrate extract/semantics plan consumers.

### Decommission Batches
- [ ] D1 — Remove `_coerce_bool_value`.
- [ ] D2 — Remove runtime facade mixins after migration window.
- [ ] D3 — Remove duplicate write-path retry/observability helpers.
- [ ] D4 — Remove `_DDL_CATALOG_WARNING_STATE`.
- [ ] D5 — Remove `create_strict_catalog` only after deprecation window (optional).
- [ ] D6 — Remove second `register_rust_udfs` + late import.
- [ ] D7 — Remove private merge symbols from `delta_read.py`.
- [ ] D8 — Remove `_normalize_ctx` legacy helper.
- [ ] D9 — Decommission Python-first planning-manifest primary path.
- [ ] D10 — Remove legacy runtime-policy parsing helpers.
- [ ] D11 — Remove provider no-arg compatibility fallback.
- [ ] D12 — Remove stdout explain capture primary path.
- [ ] D13 — Decommission bespoke semantics hash-first primary path.

### Validation
- [ ] Run canonical quality gate: `uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright`.
- [ ] Run targeted pytest suites for changed scopes (including Rust bridge contract tests where applicable).
