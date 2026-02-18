# src/ Design Improvements v2 Implementation Plan v1 (2026-02-17)

## Scope Summary

This plan synthesizes findings from 13 design review documents covering all of `src/` (~155K LOC, 476 files) and adds explicit Rust-convergence scope for DataFusion/Delta execution paths. It addresses four cross-cutting themes identified across reviews:

1. **Systemic dependency direction violations** — inner/middle ring modules (`obs/`, `schema_spec/`, `semantics/`, `relspec/`, `utils/`) import from the engine ring (`datafusion_engine/`)
2. **DRY violations** — helper functions, type aliases, and patterns duplicated across modules
3. **God objects** — 7 files exceeding 700 LOC with multiple responsibilities needing decomposition
4. **Underused Ports & Adapters** — core domain logic binds directly to engine APIs instead of abstracting through protocols

Design stance: **hard-cutover, Rust-first convergence with dependency-safe increments**. Each scope item is self-contained and can land independently. No compatibility shims — when code moves, callers update in the same PR and legacy paths are deleted in-batch. Decomposition produces sibling modules in the same package (no new packages) unless a Rust bridge boundary is required. The dependency direction map is: Inner ring (`core`, `utils`, `arrow_utils`, `obs`, `validation`) → Middle ring (`semantics`, `relspec`, `extract`) → Engine ring (`datafusion_engine`, `schema_spec`, `storage`) → Outer ring (`cli`, `graph`).

## Design Principles

1. **No `# noqa` or `# type: ignore`** — fix structurally
2. **Absolute imports only** — every module starts with `from __future__ import annotations`
3. **All functions fully typed** — no bare `Any`; `object` or Protocol instead
4. **Frozen dataclasses or msgspec.Struct** for cross-boundary payloads
5. **Quality gates run after task completion** — `ruff format && ruff check --fix && pyrefly check && pyright && pytest -q`
6. **No speculative generality** — only build what the current scope requires

## Current Baseline

- DRY helper/type consolidation from S1/S2 is now fully landed in-tree (ExplainRows, identifier normalization, introspection cache, SQL option helpers, DDL alias map, commit-metadata helper, and dead protocol/API cleanup).
- `FILE_IDENTITY` join-key filtering now routes through `semantics.types.core.file_identity_equi_join_pairs` in both compiler and IR pipeline paths.
- **`PolicyBundleConfig`** has 53+ fields mixing 5+ concerns in `session/`
- **`pipeline_build.py`** in `semantics/` is 690 LOC after extraction to `cdf_resolution.py` and `build_executor.py`
- `self: Any` mixin typing issue is resolved; direct `DataFusionRuntimeProfile(...)` construction now appears only in `session/profiles.py` factory helpers.
- `delta_runtime_ops.py` now exports public names only and removed merge-fallback helpers; Delta metadata/merge paths are on the Rust control plane.
- `LineageReport` now persists canonical `required_udfs`; expression-local `referenced_udfs` remains on `ExprInfo` for extraction-time analysis.
- `ExtractorPort` was removed (`src/extract/protocols.py` deleted); extraction session/provider bridges are integrated and `engine_session_factory` now enforces Rust session construction without Python fallback branches.
- **`__getattr__` + `_EXPORT_MAP`** lazy-loading is now consolidated through `utils.lazy_module.make_lazy_loader` across the targeted packages.
- private-name exports in `__all__` are now fully cleaned (`hits 0` under AST scan).
- **`extensions/datafusion_ext.py`** now enforces strict required entrypoints (no shimmed no-op behavior); remaining Rust-bridge convergence is concentrated in S23/S8 orchestration reduction.
- **`schema_spec/`** still imports engine symbols for runtime adapters, but policy/options/contracts are now split to canonical modules and `dataset_spec.py` is a thin facade.
- **`obs/`** has no direct `datafusion_engine` imports; engine-bound diagnostics/metrics logic now lives under `datafusion_engine.obs`.
- **`bundle_artifact.py`** is 188 LOC after extraction to `bundle_assembly.py`; it now acts as a facade plus artifact dataclasses/factories.
- **`WritePipeline`** decomposition is landed (`io/write_pipeline.py` now 607 LOC with Delta/format handlers extracted).
- **`write_pipeline.py`** no longer uses direct `write_deltalake`; bootstrap writes now flow through `DeltaWriteRequest` + `write_transaction`, but monolithic orchestration remains
- **`bundle_artifact.py`** now routes Substrait bytes through Rust bridge entrypoints, but still owns significant Python-side orchestration and artifact assembly.
- **`datafusion_ext`** exports `build_extraction_session` and `register_dataset_provider` (`rust/datafusion_python/src/codeanatomy_ext/session_utils.rs`), and these are wired in extraction/dataset resolution with strict bridge enforcement.
- **Direct `write_deltalake` usage is removed from `src/`**; remaining Rust-cutover work is authoritative bridge adoption and Python orchestration reduction in plan/session paths
- **`materialization.py`** at `extract/coordination/materialization.py` is 692 LOC after extraction of session/plan/recorder helpers.
- **`observability.py`** at `datafusion_engine/delta/observability.py` is 655 LOC with `obs_schemas.py` and `obs_table_manager.py` extracted.
- **`SchemaIntrospector`** at `schema/introspection_core.py` is 662 LOC after introspection helper extractions.
- **`contracts.py`** at `datafusion_engine/schema/contracts.py` is 656 LOC after `constraints.py`, `divergence.py`, `contract_builders.py`, and `type_normalization.py` extraction.
- **`finalize.py`** at `datafusion_engine/schema/finalize.py` is 458 LOC after `finalize_errors.py` extraction and runtime-validation extraction to `finalize_runtime.py`.
- **`dataset_spec.py`** at `schema_spec/dataset_spec.py` is 157 LOC and now serves as a compatibility facade over split canonical modules.
- **`artifact_store_core.py`** is 645 LOC after persistence/tables extraction.
- **`session/facade.py`** is 691 LOC after operation extraction to `session/facade_ops.py`.

---

## Reconciliation Snapshot (2026-02-18, refreshed)

Status legend for this revision:
- `[x]` complete
- `[ ]` partial
- `[ ]` pending / not started

Evidence snapshot used for this refresh:
- `wc -l`: `bundle_artifact.py` 188, `write_pipeline.py` 607, `materialization.py` 692, `delta/observability.py` 655, `introspection_core.py` 662, `contracts.py` 656, `finalize.py` 458, `dataset_spec.py` 157, `artifact_store_core.py` 645, `session/facade.py` 691, `obs/metrics.py` 23
- `rg "write_deltalake\\(" src` returns no matches; fallback symbols `_execute_delta_merge_fallback` and `_should_fallback_delta_merge` are absent
- Rust plan/extraction bridge entrypoints are wired and invoked, with Python bridge modules in place (`plan/rust_bundle_bridge.py`, `extraction/rust_session_bridge.py`), and extraction session fallback branches have been removed from `engine_session_factory`
- Substrait execution fallback branches (`_record_substrait_fallback`, `_rehydrate_from_proto`) are removed from `session/facade.py`, and plan-level fallback helpers are removed from `plan/normalization.py`
- Additional `bundle_artifact.py` decomposition landed via `plan_identity.py`, `planning_env.py`, `substrait_artifacts.py`, `plan_utils.py`, and `bundle_assembly.py`
- `obs/` has zero direct `datafusion_engine` imports (`rg -n "from datafusion_engine|import datafusion_engine" src/obs` returns no matches)
- Private-name exports in `__all__` remain in plan modules (`bundle_assembly.py`, `plan_proto.py`, `artifact_store_constants.py`, `artifact_store_persistence.py`) and require final S3/D2 cleanup.
- Direct `DataFusionRuntimeProfile(...)` construction appears only in `session/profiles.py` factory helpers (2 hits)
- Moved schema-spec concerns are fully migrated off `dataset_spec.py` (`rg --multiline` hit count for moved symbols in `src/`: 0)
- View cache registration functions (`_register_view_with_cache`, `_register_delta_staging_cache`, `_register_delta_output_cache`) now live in `views/cache_registration.py`; corresponding function definitions are removed from `views/graph.py`
- Diagnostics payload builders are canonical in `lineage/diagnostics_payloads.py`; payload-callsite imports from `lineage/diagnostics` are removed
- Protocol-first adoption closure: `semantics/compiler.py` resolves context via `SessionContextProviderPort`, `relspec/contracts.py` uses `RuntimeProfilePort`, and targeted deep policy chains are replaced with runtime-profile convenience helpers
- `plan/rust_bundle_bridge.py` now uses a single strict bridge signature (`df=`) with no TypeError fallback dispatch
- Canonical runtime install contract remains partially incomplete: session/runtime tests still fail because `invoke_entrypoint_with_adapted_context()` calls `install_codeanatomy_runtime` with positional args while `datafusion_ext.install_codeanatomy_runtime()` enforces keyword-only runtime args.

### Completed Scope Items

- S1. Consolidate duplicated type aliases and helper functions
- S2. Fix lying APIs and dead parameters
- S4. Extract shared lazy-loading facade utility
- S5. Replace unstructured multi-value returns with named types
- S6. Fix dependency direction: move engine-dependent code from inner ring
- S7. Reclassify `schema_spec` and split engine-dependent content
- S8. Decompose `bundle_artifact.py`
- S19. Extract/extraction package cleanup and dependency fixes
- S20. Additional module decompositions (medium priority)
- S9. Decompose `WritePipeline`
- S10. Decompose `materialization.py` and fix extract session duplication
- S11. Decompose `delta/observability.py`
- S12. Decompose `SchemaIntrospector`
- S13. Replace deferred import pattern with generic factory
- S14. Define port protocols for semantics/relspec/storage decoupling
- S15. Replace `self: Any` mixin anti-pattern with protocol
- S16. Consolidate Arrow type mapping knowledge
- S17. Address hidden `DataFusionRuntimeProfile` construction (DI)
- S18. Demeter convenience methods and profile decomposition
- S21. Correctness, testability, and observability improvements
- S22. Bidirectional dependency resolution (Storage ↔ Engine)
- S23. Rust-canonical plan bundle and Substrait artifact cutover
- S24. Rust-native Delta write and mutation cutover
- S25. Extension contract hardening and shim removal
- S26. Rust-native extraction session and provider registration cutover

### Partially Implemented Scope Items

- S3. Promote private exports and clean `__all__` lists
  - Remaining: remove `_`-prefixed exports from `__all__` in `src/datafusion_engine/plan/bundle_assembly.py`, `src/datafusion_engine/plan/plan_proto.py`, `src/datafusion_engine/plan/artifact_store_constants.py`, and `src/datafusion_engine/plan/artifact_store_persistence.py`.
- S27. Canonical Rust runtime install and UDF snapshot contract
  - Remaining: fix runtime entrypoint invocation in `src/datafusion_engine/extensions/context_adaptation.py` / `src/datafusion_engine/udf/extension_core.py` so `install_codeanatomy_runtime` receives keyword arguments (`enable_async_udfs`, `async_udf_timeout_ms`, `async_udf_batch_size`) instead of positional arguments.

### Pending / Not Started Scope Items

- None.

---

## S1. Consolidate Duplicated Type Aliases and Helper Functions

### Goal

Eliminate all verified DRY violations where identical code exists in multiple files. This is the lowest-risk, highest-signal improvement — each deduplication reduces drift risk and simplifies maintenance.

### Representative Code Snippets

```python
# src/datafusion_engine/session/runtime_profile_config.py (canonical location for ExplainRows)
# Already defined here at line 47; other 4 files should import from here.

if TYPE_CHECKING:
    from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
    ExplainRows = TableLike | RecordBatchReaderLike
```

```python
# src/datafusion_engine/session/runtime_compile.py — AFTER (import instead of redefine)
from datafusion_engine.session.runtime_profile_config import ExplainRows  # in TYPE_CHECKING block
```

```python
# src/datafusion_engine/session/runtime_telemetry.py — AFTER (import instead of redefine)
from datafusion_engine.session.runtime_compile import _identifier_normalization_mode
# DELETE the local definition at line 609
```

```python
# src/datafusion_engine/schema/introspection_core.py — AFTER (import from canonical)
from datafusion_engine.schema.introspection_routines import _introspection_cache_for_ctx
# DELETE local re-implementation at lines 113-123
```

### Files to Edit

**Session module DRY:**
- `src/datafusion_engine/session/runtime.py` — remove local `ExplainRows`, import from `runtime_profile_config`
- `src/datafusion_engine/session/runtime_compile.py` — remove local `ExplainRows`, import from `runtime_profile_config`
- `src/datafusion_engine/session/runtime_hooks.py` — remove local `ExplainRows`, import from `runtime_profile_config`
- `src/datafusion_engine/compile/options.py` — remove local `ExplainRows`, import from `runtime_profile_config`
- `src/datafusion_engine/session/runtime_telemetry.py` — remove `_identifier_normalization_mode` (line 609), `_catalog_autoload_settings`, `_effective_catalog_autoload_for_profile` definitions; import from `runtime_compile` and `runtime_config_policies`

**Schema module DRY:**
- `src/datafusion_engine/schema/introspection_core.py` — remove `_introspection_cache_for_ctx` reimplementation, import from `introspection_routines`; remove delegate wrappers for functions already in `introspection_cache.py`
- `src/datafusion_engine/schema/introspection_cache.py` — verify `SchemaMapping` is canonical; update `introspection_core.py` to import from here

**Schema/session SQL-options DRY:**
- `src/datafusion_engine/session/runtime_session.py` — remove local `_read_only_sql_options`; import from shared helper
- `src/datafusion_engine/schema/introspection_core.py` — remove local `_read_only_sql_options`; import from shared helper
- `src/datafusion_engine/schema/introspection_routines.py` — remove local `_read_only_sql_options`; import from shared helper

**Plan module DRY:**
- `src/datafusion_engine/plan/artifact_store_tables.py` — remove `_commit_metadata_for_rows` duplicate; import from `artifact_serialization.py`

**Dataset module DRY:**
- `src/datafusion_engine/dataset/registration_core.py` — consolidate `_DDL_TYPE_ALIASES` with `extension_core.py`; create shared `ddl_types.py`
- `src/datafusion_engine/udf/extension_core.py` — remove local `_DDL_TYPE_ALIASES` copy

**Storage module DRY:**
- `src/storage/deltalake/delta_feature_mutations.py` — extract generic `_enable_delta_feature()` helper to deduplicate 7 near-identical feature mutation functions

**Semantics module DRY:**
- `src/semantics/compiler.py` — extract FILE_IDENTITY join-key filtering into `semantics/types/core.py`
- `src/semantics/ir_pipeline.py` — import shared FILE_IDENTITY filtering from `types/core.py`

**Metadata constant audit:**
- `src/arrow_utils/core/schema_constants.py` vs `src/datafusion_engine/arrow/metadata.py` — verify no duplication; consolidate if overlapping

### New Files to Create

- `src/datafusion_engine/dataset/ddl_types.py` — shared DDL type alias mapping
- `src/datafusion_engine/schema/introspection_common.py` — shared read-only SQL option helper for schema/session introspection
- `tests/unit/datafusion_engine/dataset/test_ddl_types.py`
- `tests/unit/datafusion_engine/schema/test_introspection_common.py`

### Legacy Decommission/Delete Scope

- Delete `ExplainRows` type alias from `runtime.py:127`, `runtime_compile.py:38`, `runtime_hooks.py:39`, `compile/options.py:21` (keep `runtime_profile_config.py:47` as canonical)
- Delete `_identifier_normalization_mode` function body from `runtime_telemetry.py:609`
- Delete `_catalog_autoload_settings` and `_effective_catalog_autoload_for_profile` from `runtime_telemetry.py` (import from `runtime_config_policies`)
- Delete `_introspection_cache_for_ctx` function body from `introspection_core.py:113-123`
- Delete delegate wrapper functions from `introspection_core.py` that only forward to `introspection_cache.py`
- Delete duplicate `SchemaMapping` from `introspection_core.py:40` if present (keep `introspection_cache.py:11`)
- Delete duplicate `_read_only_sql_options` function bodies from `runtime_session.py`, `introspection_core.py`, and `introspection_routines.py` (replace with shared helper)
- Delete `_commit_metadata_for_rows` from `artifact_store_tables.py` (use `artifact_serialization.py` version)
- Delete 6 of 7 near-identical `_enable_delta_feature_*` functions in `delta_feature_mutations.py` (replaced by generic helper)
- Delete local `_DDL_TYPE_ALIASES` from `extension_core.py` (use `ddl_types.py` canonical mapping)
- Delete local `_FILE_IDENTITY_NAMES` from `ir_pipeline.py:967` (import from `types/core.py`)

---

## S2. Fix Lying APIs and Dead Parameters

### Goal

Remove or fix API surface that misleads callers: parameters that are silently discarded, functions that always return `None` despite promising data, and redundant contract fields with indistinct semantics.

### Representative Code Snippets

```python
# src/storage/deltalake/delta_runtime_ops.py — FIX delta_commit_metadata
# Current: always returns None at line 449 despite retrieving snapshot
# Fix: return the actual commit metadata or change return type to None and document
def delta_commit_metadata(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    version: int | None = None,
) -> Mapping[str, str] | None:
    """Return commit metadata for a Delta table version, or None if unavailable."""
    dt = DeltaTable(path, storage_options=storage_options, version=version)
    snapshot = _snapshot_info(dt)
    return snapshot.commit_metadata  # Return actual data instead of None
```

```python
# src/datafusion_engine/schema/alignment.py — FIX safe_cast parameter
# Current: line 57 has `_ = safe_cast` (silently discarded)
# Fix: either implement safe_cast semantics or remove the parameter
def align_to_schema(
    table: pa.Table,
    target: pa.Schema,
    *,
    # REMOVE: safe_cast: bool = False,  -- was silently discarded
    null_fill: bool = True,
) -> pa.Table:
    ...
```

```python
# src/datafusion_engine/lineage/reporting.py — FIX redundant UDF contract fields
# Current: required_udfs and referenced_udfs are both populated with the same tuple
# Fix: make required_udfs canonical and delete redundant persisted field
class LineageReport:
    required_udfs: tuple[str, ...] = ()
    # ... other fields ...
    # DELETE redundant persisted field: referenced_udfs
```

### Files to Edit

- `src/storage/deltalake/delta_runtime_ops.py` — fix `delta_commit_metadata()` at line 414 (return actual metadata)
- `src/datafusion_engine/schema/alignment.py` — remove `safe_cast` parameter (line 57) or implement it; remove duplicate import at line 294
- `src/datafusion_engine/lineage/reporting.py` — make `required_udfs` canonical and remove redundant persisted `referenced_udfs` field
- `src/datafusion_engine/plan/artifact_serialization.py` — remove redundant `referenced_udfs` payload emission when equal to `required_udfs`
- `src/serde_artifacts.py` — update typed artifact payloads to remove redundant `referenced_udfs` persistence (if present)
- `schemas/msgspec/*.schema.json` — regenerate schema artifacts if contract fields change
- `src/datafusion_engine/session/profiles.py` — remove dead `use_cache` parameter from `runtime_for_profile()`
- `src/datafusion_engine/delta/service_protocol.py` — rename `_provider_artifact_payload` to `provider_artifact_payload` in `DeltaServicePort` protocol (line 250)
- `src/datafusion_engine/delta/control_plane_core.py` — remove no-op `_validate_update_constraints` stub
- `src/datafusion_engine/dataset/registration_core.py` — rename `_resolve_cached_location` to `_resolve_location` (or implement caching)
- `src/datafusion_engine/delta/metadata_snapshots.py` — replace `suppress(Exception)` at line 108 with explicit error handling + logging
- `src/storage/deltalake/config.py` — replace `StatsFilter.value: Any` with `int | float | str | bool`
- `src/core_types.py` — remove `pydantic.Field` import (unnecessary dependency)
- `src/datafusion_engine/session/lifecycle.py` — evaluate 32-LOC trivial delegation; inline into caller if appropriate
- `src/datafusion_engine/session/runtime_compile_options.py` — replace 21-element `unchanged` tuple (lines 249-273) with structural equality check

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete `safe_cast` parameter from `align_to_schema()` signature and all call sites passing it
- Delete redundant persisted `referenced_udfs` field from `LineageReport` and matching serialization payload keys
- Delete `use_cache` parameter from `runtime_for_profile()` and all call sites passing it
- Delete no-op `_validate_update_constraints` from `control_plane_core.py`
- Delete `pydantic.Field` import from `core_types.py`
- Delete `lifecycle.py` if confirmed trivial delegation

---

## S3. Promote Private Exports and Clean `__all__` Lists

### Goal

Fix the anti-pattern where `_`-prefixed functions are exported in `__all__` as cross-module public API. Either promote names to public (drop underscore) or internalize them. Also remove `_`-prefixed names from other `__all__` lists.

### Representative Code Snippets

```python
# src/storage/deltalake/delta_runtime_ops.py — AFTER (promote to public names)
# Line 865-899: rename all 26 _-prefixed exports to public names
__all__ = [
    "MutationArtifactRequest",          # was _MutationArtifactRequest
    "commit_metadata_from_properties",   # was _commit_metadata_from_properties
    "constraint_status",                 # was _constraint_status
    "delta_cdf_table_provider",          # was _delta_cdf_table_provider
    "delta_commit_options",              # was _delta_commit_options
    # ... etc for all 26 functions
    "delta_cdf_enabled",                 # already public
    "delta_commit_metadata",             # already public
    "delta_history_snapshot",            # already public
    "delta_protocol_snapshot",           # already public
    "delta_table_features",              # already public
    "read_delta_cdf",                    # already public
    "read_delta_cdf_eager",              # already public
]
```

### Files to Edit

- `src/storage/deltalake/delta_runtime_ops.py` — rename 26 `_`-prefixed functions to public names in both definitions and `__all__`
- `src/storage/deltalake/delta_write.py` — update 10 import names (lines 121-132)
- `src/storage/deltalake/delta_feature_mutations.py` — update 4 import names (lines 9-14)
- `src/storage/deltalake/delta_read.py` — update import names (lines 677-686)
- `src/datafusion_engine/plan/artifact_store_tables.py` — drop underscore prefix from exported names
- `src/semantics/pipeline_build.py` — remove `_`-prefixed names from `__all__`
- `src/semantics/pipeline_cache.py` — remove `_`-prefixed names from `__all__`
- `src/datafusion_engine/session/runtime_udf.py` — remove `_`-prefixed names from `__all__`
- `src/extract/__init__.py` — remove `_`-prefixed names from `__all__` if present
- `src/extract/extractors/__init__.py` (and other extractor `__init__.py` files) — audit and clean `_`-prefixed exports
- `src/datafusion_engine/plan/__init__.py`, `src/datafusion_engine/lineage/__init__.py`, `src/datafusion_engine/views/__init__.py` — populate `__all__` lists if empty
- `src/schema_spec/__init__.py` — add docstring explaining the 60+ re-exports

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- All 26 `_`-prefixed function names in `delta_runtime_ops.py` are superseded by their public equivalents. Callers in `delta_write.py`, `delta_feature_mutations.py`, `delta_read.py` update atomically.

---

## S4. Extract Shared Lazy-Loading Facade Utility

### Goal

Replace the `__getattr__` + `_EXPORT_MAP` + `__all__` + `__dir__` boilerplate duplicated in 10+ `__init__.py` files with a single reusable utility in `src/utils/`.

### Representative Code Snippets

```python
# src/utils/lazy_module.py (NEW)
"""Reusable lazy-loading facade for package __init__.py files."""

from __future__ import annotations

from collections.abc import Callable, Mapping


def make_lazy_loader(
    export_map: Mapping[str, str],
    package_name: str,
) -> tuple[Callable[[str], object], Callable[[], list[str]]]:
    """Build __getattr__ and __dir__ for lazy package re-exports.

    Parameters
    ----------
    export_map
        Mapping from public name to dotted module path.
    package_name
        The package __name__ for error messages.

    Returns
    -------
    tuple
        (getattr_fn, dir_fn) to assign as module-level __getattr__ and __dir__.
    """
    import importlib

    def _getattr(name: str) -> object:
        if name in export_map:
            module_path = export_map[name]
            mod = importlib.import_module(module_path)
            return getattr(mod, name)
        msg = f"module {package_name!r} has no attribute {name!r}"
        raise AttributeError(msg)

    def _dir() -> list[str]:
        return list(export_map)

    return _getattr, _dir
```

```python
# src/semantics/__init__.py — AFTER (using shared utility)
from utils.lazy_module import make_lazy_loader

_EXPORT_MAP: dict[str, str] = {
    "SemanticCompiler": "semantics.compiler",
    "SemanticConfig": "semantics.config",
    # ... existing entries ...
}

__getattr__, __dir__ = make_lazy_loader(_EXPORT_MAP, __name__)
__all__ = list(_EXPORT_MAP)
```

### Files to Edit

- `src/relspec/__init__.py`
- `src/datafusion_engine/delta/__init__.py`
- `src/datafusion_engine/session/__init__.py`
- `src/schema_spec/__init__.py`
- `src/semantics/__init__.py`
- `src/storage/deltalake/__init__.py`
- `src/obs/otel/__init__.py`
- `src/runtime_models/__init__.py`
- `src/datafusion_engine/arrow/metadata.py` (if applicable)

### New Files to Create

- `src/utils/lazy_module.py` — shared lazy-loading facade utility
- `tests/unit/utils/test_lazy_module.py` — unit tests for the utility

### Legacy Decommission/Delete Scope

- Delete local `__getattr__` function bodies in each of the 8-10 `__init__.py` files listed above (replaced by calls to `make_lazy_loader`)
- Delete local `__dir__` function bodies in each file

---

## S5. Replace Unstructured Multi-Value Returns with Named Types

### Goal

Replace tuple returns and `Mapping[str, object]` payloads with frozen dataclasses at subsystem boundaries, making illegal states unrepresentable.

### Representative Code Snippets

```python
# src/datafusion_engine/plan/scheduling.py — AFTER (replace 7-tuple)
@dataclass(frozen=True)
class DeltaScanCandidateResult:
    """Result of Delta scan candidate analysis."""

    table_name: str
    delta_path: str
    version: int | None
    timestamp: str | None
    cdf_enabled: bool
    predicate: str | None
    partition_columns: tuple[str, ...]


# Replace _delta_scan_candidates return type from
#   tuple[str, str, int | None, str | None, bool, str | None, tuple[str, ...]]
# to DeltaScanCandidateResult
```

```python
# src/datafusion_engine/dataset/udf — AFTER (replace untyped tuple)
@dataclass(frozen=True)
class AsyncUdfPolicy:
    """Policy for async UDF execution."""

    enabled: bool
    max_concurrency: int | None = None
    timeout_ms: int | None = None
```

```python
# src/datafusion_engine/delta/observability.py — AFTER (typed Delta report)
@dataclass(frozen=True)
class DeltaOperationReport:
    """Structured report from a Delta write/merge/delete operation."""

    rows_affected: int
    version: int
    operation: str
    duration_ms: float
    commit_metadata: Mapping[str, str]
    # Replaces Mapping[str, object] flowing through write pipeline
```

### Files to Edit

- `src/datafusion_engine/plan/scheduling.py` — replace 7-tuple return at line 399 with `DeltaScanCandidateResult`
- `src/datafusion_engine/udf/extension_core.py` — replace `tuple[bool, int | None, int | None]` with `AsyncUdfPolicy` dataclass
- `src/datafusion_engine/delta/observability.py` — define `DeltaOperationReport` for typed report payloads
- `src/datafusion_engine/io/write_pipeline.py` — consume `DeltaOperationReport` instead of `Mapping[str, object]`
- `src/datafusion_engine/dataset/registry.py` — add `__post_init__` check to `DatasetLocation` preventing simultaneous `delta_version` and `delta_timestamp` (lines 89-90)
- `src/semantics/compiler.py` — make `join_type` and `strategy_hint` mutually exclusive in `RelationOptions` (lines 160-166)
- `src/datafusion_engine/plan/contracts.py` — replace `Any` fields (lines 13-14) with typed protocols or forward refs
- `src/extract/contracts.py` — define `ScipIndexConfig` protocol to replace `object | None` (line 19)
- `src/planning_engine/config.py` — tighten `EngineExecutionOptions.runtime_config` from `object | None` to concrete type (lines 43-46)
- `src/extract/coordination/materialization.py` — type `RunExtractionRequestV1.options` as `ExtractionRunOptions | None`

### New Files to Create

None (types defined in their owning modules).

### Legacy Decommission/Delete Scope

- Delete 7-tuple return type annotation from `_delta_scan_candidates` in `scheduling.py`
- Delete raw `tuple[bool, int | None, int | None]` annotation for UDF policies in `extension_core.py`
- Delete bare `object` type annotations replaced by protocols/concrete types in the files above

---

## S6. Fix Dependency Direction: Move Engine-Dependent Code from Inner Ring

### Goal

Resolve the 0/3 and 1/3 P5 (Dependency Direction) scores in `obs/`, `schema_spec/`, and `utils/` by relocating engine-dependent code to the engine ring or introducing port protocols.

### Representative Code Snippets

```python
# src/datafusion_engine/obs/datafusion_runs.py (MOVED from src/obs/datafusion_runs.py)
"""DataFusion run recording — engine-ring observation code."""
from __future__ import annotations

from datafusion_engine.lineage.diagnostics import DiagnosticsSink, ensure_recorder_sink
from storage.deltalake.delta_read import ...
# All engine-ring imports are now legal (within engine ring)
```

```python
# src/obs/ports.py (NEW — port protocols for obs consumers)
"""Observation port protocols for engine-agnostic diagnostics."""
from __future__ import annotations

from typing import Protocol

import pyarrow as pa


class DiagnosticsPort(Protocol):
    """Port for recording diagnostic events."""

    def record_event(self, event_name: str, payload: pa.Table) -> None: ...
    def ensure_sink(self) -> None: ...


class MetricsSchemaPort(Protocol):
    """Port for schema-aware metrics collection."""

    def schema_to_dict(self, schema: pa.Schema) -> dict[str, object]: ...
    def empty_table(self, schema: pa.Schema) -> pa.Table: ...
```

```python
# src/datafusion_engine/arrow/coercion.py (MOVED from src/utils/value_coercion.py partial)
"""RecordBatch coercion helpers — engine-ring code."""
from __future__ import annotations

from datafusion_engine.arrow.interop import RecordBatchReader, RecordBatchReaderLike


def coerce_to_recordbatch_reader(value: object) -> RecordBatchReader:
    ...
```

### Files to Edit

- `src/obs/datafusion_runs.py` — move to `src/datafusion_engine/obs/datafusion_runs.py`
- `src/obs/diagnostics.py` — extract engine-dependent functions to `src/datafusion_engine/obs/diagnostics_bridge.py`; keep engine-agnostic OTel code in `obs/`
- `src/obs/metrics.py` — extract Arrow/DataFusion-dependent metrics builders to `src/datafusion_engine/obs/metrics_bridge.py`
- `src/obs/scan_telemetry.py` — extract `schema_to_dict` usage to engine bridge
- `src/utils/value_coercion.py` — move `coerce_to_recordbatch_reader()` to `src/datafusion_engine/arrow/coercion.py`
- All callers of relocated functions — update import paths

### New Files to Create

- `src/datafusion_engine/obs/__init__.py` — package init
- `src/datafusion_engine/obs/datafusion_runs.py` — relocated from `obs/`
- `src/datafusion_engine/obs/diagnostics_bridge.py` — engine-side diagnostic helpers
- `src/datafusion_engine/obs/metrics_bridge.py` — engine-side metrics helpers
- `src/obs/ports.py` — port protocols for engine-agnostic observation
- `src/datafusion_engine/arrow/coercion.py` — relocated RecordBatch coercion
- `tests/unit/datafusion_engine/obs/__init__.py`
- `tests/unit/datafusion_engine/obs/test_datafusion_runs.py`
- `tests/unit/obs/test_ports.py`

### Legacy Decommission/Delete Scope

- Delete `src/obs/datafusion_runs.py` after relocation
- Delete engine-dependent functions from `src/obs/diagnostics.py` (keep OTel-only code)
- Delete engine-dependent functions from `src/obs/metrics.py` (keep OTel-only code)
- Delete `coerce_to_recordbatch_reader()` from `src/utils/value_coercion.py`

---

## S7. Reclassify `schema_spec` and Split Engine-Dependent Content

### Goal

Acknowledge that `schema_spec/` is engine-ring (not inner-ring) by either reclassifying it in the dependency map or splitting it into `schema_spec/core/` (inner ring, pure types) and `schema_spec/engine/` (engine ring, DataFusion-dependent).

### Representative Code Snippets

```python
# src/schema_spec/core/__init__.py (NEW — pure spec types, no engine imports)
"""Core schema specification types — inner ring."""
from __future__ import annotations

from schema_spec.core.table_spec import TableSpec
from schema_spec.core.field_spec import FieldSpec  # pure Arrow types only
from schema_spec.core.contracts import ContractRow, TableSchemaContract

__all__ = ["TableSpec", "FieldSpec", "ContractRow", "TableSchemaContract"]
```

```python
# src/schema_spec/engine/__init__.py (NEW — engine-dependent spec runtime)
"""Engine-dependent schema spec runtime — engine ring."""
from __future__ import annotations

from schema_spec.engine.dataset_runtime import DatasetSpecRuntime
from schema_spec.engine.scan_options import DataFusionScanOptions
from schema_spec.engine.discovery import discover_schema

__all__ = ["DatasetSpecRuntime", "DataFusionScanOptions", "discover_schema"]
```

### Files to Edit

- `src/schema_spec/dataset_spec.py` — split into `scan_policy.py` (policy types), `scan_options.py` (scan options), and `dataset_contracts.py` (pure contract types)
- `src/schema_spec/__init__.py` — update re-exports after split
- All callers of `dataset_spec` types — update import paths

### New Files to Create

- `src/schema_spec/scan_policy.py` — `ScanPolicyConfig`, `ScanPolicyDefaults`, `DeltaScanPolicyDefaults`, policy application functions
- `src/schema_spec/scan_options.py` — `DataFusionScanOptions` and related engine-dependent types
- `src/schema_spec/dataset_contracts.py` — `ContractRow`, `TableSchemaContract`, `ContractSpec` (pure types, no engine deps)
- `tests/unit/schema_spec/test_scan_policy.py`
- `tests/unit/schema_spec/test_scan_options.py`
- `tests/unit/schema_spec/test_dataset_contracts.py`

### Legacy Decommission/Delete Scope

- Delete monolithic implementations from `dataset_spec.py` after extracting into focused modules
- Delete inline scan policy types from `dataset_spec.py` (moved to `scan_policy.py`)
- Delete contract types from `dataset_spec.py` (moved to `dataset_contracts.py`)

---

## S8. Decompose `bundle_artifact.py` (2,552 LOC)

### Goal

Split the largest file in `src/` into focused modules along its 5 natural concern boundaries: plan building, fingerprinting, proto conversion, UDF snapshot collection, and diagnostics.

### Representative Code Snippets

```python
# src/datafusion_engine/plan/plan_fingerprint.py (NEW — extracted)
"""Plan fingerprinting strategies."""
from __future__ import annotations

from dataclasses import dataclass

@dataclass(frozen=True)
class PlanFingerprint:
    """Deterministic plan identity."""

    identity_hash: str
    policy_hash: str
    combined_hash: str

def compute_plan_fingerprint(
    logical_plan: object,
    execution_plan: object,
    *,
    policy_hash: str,
) -> PlanFingerprint:
    """Compute deterministic fingerprint for a plan artifact."""
    ...
```

```python
# src/datafusion_engine/plan/udf_snapshot.py (NEW — extracted)
"""UDF snapshot collection for plan artifacts."""
from __future__ import annotations


def collect_udf_snapshot(
    plan: object,
    *,
    udf_catalog: object,
) -> dict[str, object]:
    """Collect UDF metadata referenced by a plan."""
    ...
```

### Files to Edit

- `src/datafusion_engine/plan/bundle_artifact.py` — extract methods into new modules, retain `DataFusionPlanArtifact` dataclass as facade
- `src/datafusion_engine/plan/artifact_store_core.py` — complete in-progress decomposition; remove bottom-of-file re-exports
- `src/datafusion_engine/views/graph.py` — extract cache registration logic (~90% duplicated at lines 663-847 and 850-1050) into `views/cache_registration.py`

### New Files to Create

- `src/datafusion_engine/plan/plan_fingerprint.py` — fingerprinting strategies (~200 LOC)
- `src/datafusion_engine/plan/udf_snapshot.py` — UDF collection logic (~150 LOC)
- `src/datafusion_engine/plan/plan_proto.py` — proto serialization/normalization (~300 LOC)
- `src/datafusion_engine/plan/plan_diagnostics.py` — diagnostics recording (~100 LOC)
- `src/datafusion_engine/views/cache_registration.py` — extracted cache registration from `graph.py`
- `src/datafusion_engine/lineage/diagnostics_payloads.py` — extracted diagnostics payload builders from `lineage/diagnostics.py`
- `tests/unit/datafusion_engine/plan/test_plan_fingerprint.py`
- `tests/unit/datafusion_engine/plan/test_udf_snapshot.py`
- `tests/unit/datafusion_engine/plan/test_plan_proto.py`
- `tests/unit/datafusion_engine/views/test_cache_registration.py`

### Legacy Decommission/Delete Scope

- Delete fingerprinting functions from `bundle_artifact.py` (moved to `plan_fingerprint.py`)
- Delete proto conversion functions from `bundle_artifact.py` (moved to `plan_proto.py`)
- Delete UDF snapshot functions from `bundle_artifact.py` (moved to `udf_snapshot.py`)
- Delete diagnostics helpers from `bundle_artifact.py` (moved to `plan_diagnostics.py`)
- Keep `DataFusionPlanArtifact` class and `build_plan_artifact()` factory in `bundle_artifact.py` (now ~500-700 LOC, importing from extracted modules)
- Delete bottom-of-file re-exports from `artifact_store_core.py`
- Delete duplicated cache registration logic from `graph.py` (moved to `cache_registration.py`)
- Delete diagnostics payload builders from `lineage/diagnostics.py` (moved to `diagnostics_payloads.py`)

---

## S9. Decompose `WritePipeline` (1,417 LOC)

### Goal

Extract Delta-specific write operations and format-specific handlers from `WritePipeline` into focused helper modules, reducing the class to ~500 LOC of orchestration logic.

### Representative Code Snippets

```python
# src/datafusion_engine/io/delta_write_handler.py (NEW — extracted)
"""Delta-specific write pipeline operations."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DeltaWriteContext:
    """Resolved context for a Delta write operation."""

    delta_path: str
    write_spec: object  # DeltaWriteSpec
    commit_metadata: dict[str, str]


def execute_delta_write(ctx: DeltaWriteContext, df: object) -> object:
    """Execute a Delta write with bootstrap, commit, and maintenance."""
    ...


def run_post_write_maintenance(ctx: DeltaWriteContext) -> None:
    """Run post-write Delta maintenance (optimize, vacuum)."""
    ...
```

### Files to Edit

- `src/datafusion_engine/io/write_pipeline.py` — extract Delta methods (~700 LOC) and format handlers (~200 LOC)

### New Files to Create

- `src/datafusion_engine/io/delta_write_handler.py` — Delta write pipeline (~700 LOC)
- `src/datafusion_engine/io/format_write_handler.py` — CSV/JSON/Arrow/COPY format handlers (~200 LOC)
- `tests/unit/datafusion_engine/io/test_delta_write_handler.py`
- `tests/unit/datafusion_engine/io/test_format_write_handler.py`

### Legacy Decommission/Delete Scope

- Delete Delta-specific methods from `WritePipeline` class (moved to `delta_write_handler.py`)
- Delete format-specific methods from `WritePipeline` class (moved to `format_write_handler.py`)
- `WritePipeline` retains `write()`, `write_view()`, destination resolution, and orchestration dispatch (~500 LOC)

---

## S10. Decompose `materialization.py` (1,075 LOC) and Fix Extract Session Duplication

### Goal

Split extraction materialization into plan building, execution, and recording concerns. Simultaneously eliminate the 4x session-bootstrap duplication in the orchestrator.

### Representative Code Snippets

```python
# src/extract/coordination/extract_session.py (NEW — session factory)
"""Extraction session factory — single construction point."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExtractSession:
    """Bundled extraction session dependencies."""

    engine_session: object  # EngineSession
    runtime_profile: object  # DataFusionRuntimeProfile
    diagnostics: object  # DiagnosticsCollector

    @classmethod
    def from_request(cls, request: object) -> ExtractSession:
        """Build session from extraction request — single construction."""
        ...
```

```python
# src/extract/coordination/extract_recorder.py (NEW — extracted recording)
"""Extraction recording and diagnostics."""
from __future__ import annotations


def record_extract_execution(*, session: object, plan: object, result: object) -> None:
    """Record extraction execution outcome."""
    ...

def record_extract_compile(*, session: object, plan: object) -> None:
    """Record extraction plan compilation."""
    ...
```

### Files to Edit

- `src/extract/coordination/materialization.py` — extract recording functions, plan-building helpers, and dataset resolution into separate modules (3-way split: plan building, execution, recording)
- `src/extract/orchestrator.py` — replace 4x inline session construction with `ExtractSession.from_request()`; consolidate 5 `_extract_*` functions into generic `_run_extractor()` using `ExtractorPort`
- `src/extract/protocols.py` — decide: implement `ExtractorPort` across extractors (preferred) or remove the dead protocol
- `src/extract/coordination/context.py` — add `session_context()` and `determinism_tier()` convenience methods to `EngineSession` to reduce Demeter violations

### New Files to Create

- `src/extract/coordination/extract_session.py` — session factory
- `src/extract/coordination/extract_recorder.py` — diagnostics recording (~160 LOC)
- `src/extract/coordination/extract_plan_builder.py` — plan building + projection (~170 LOC)
- `tests/unit/extract/coordination/test_extract_session.py`
- `tests/unit/extract/coordination/test_extract_recorder.py`
- `tests/unit/extract/coordination/test_extract_plan_builder.py`

### Legacy Decommission/Delete Scope

- Delete recording functions from `materialization.py` (moved to `extract_recorder.py`)
- Delete plan-building functions from `materialization.py` (moved to `extract_plan_builder.py`)
- Delete 4 inline session construction blocks from `orchestrator.py` (replaced by `ExtractSession`)
- Delete 4 of 5 `_extract_*` near-identical functions from `orchestrator.py` (replaced by generic `_run_extractor`)
- Delete `ExtractorPort` from `protocols.py` if decision is to remove; otherwise keep and implement

---

## S11. Decompose `delta/observability.py` (1,190 LOC)

### Goal

Split Delta observability into artifact recording, table management, and schema definitions along the natural boundaries identified in the review.

### Representative Code Snippets

```python
# src/datafusion_engine/delta/obs_schemas.py (NEW — schema builders)
"""Delta observability Arrow schemas."""
from __future__ import annotations

import pyarrow as pa


def delta_snapshot_schema() -> pa.Schema:
    """Arrow schema for Delta snapshot observability table."""
    ...

def delta_mutation_schema() -> pa.Schema:
    """Arrow schema for Delta mutation observability table."""
    ...
```

```python
# src/datafusion_engine/delta/obs_table_manager.py (NEW — table lifecycle)
"""Delta observability table bootstrap and management."""
from __future__ import annotations


def ensure_delta_observability_tables(*, root: str, storage_options: object) -> None:
    """Ensure all Delta observability tables exist."""
    ...
```

### Files to Edit

- `src/datafusion_engine/delta/observability.py` — extract schemas and table management

### New Files to Create

- `src/datafusion_engine/delta/obs_schemas.py` — schema builder functions (~100 LOC)
- `src/datafusion_engine/delta/obs_table_manager.py` — table bootstrap/management (~300 LOC)
- `tests/unit/datafusion_engine/delta/test_obs_schemas.py`
- `tests/unit/datafusion_engine/delta/test_obs_table_manager.py`

### Legacy Decommission/Delete Scope

- Delete schema builder functions from `observability.py` (moved to `obs_schemas.py`)
- Delete table bootstrap/management functions from `observability.py` (moved to `obs_table_manager.py`)
- `observability.py` retains artifact recording functions and dataclass definitions (~500 LOC)

---

## S12. Decompose `SchemaIntrospector` (709 LOC, 17+ Methods)

### Goal

Extract snapshot collection and function catalog methods from `SchemaIntrospector` into focused companion modules.

### Representative Code Snippets

```python
# src/datafusion_engine/schema/snapshot_collector.py (NEW)
"""Schema snapshot collection from DataFusion information_schema."""
from __future__ import annotations

import pyarrow as pa
from datafusion import SessionContext


def tables_snapshot(ctx: SessionContext) -> pa.Table:
    """Collect tables snapshot from information_schema.tables."""
    ...

def schemata_snapshot(ctx: SessionContext) -> pa.Table:
    """Collect schemata snapshot from information_schema.schemata."""
    ...

def columns_snapshot(ctx: SessionContext) -> pa.Table:
    """Collect columns snapshot from information_schema.columns."""
    ...

def settings_snapshot(ctx: SessionContext) -> pa.Table:
    """Collect settings snapshot from information_schema.df_settings."""
    ...
```

### Files to Edit

- `src/datafusion_engine/schema/introspection_core.py` — extract snapshot and function catalog methods

### New Files to Create

- `src/datafusion_engine/schema/snapshot_collector.py` — `*_snapshot()` methods (~200 LOC)
- `src/datafusion_engine/schema/function_catalog_reader.py` — `routines_snapshot()`, `parameters_snapshot()`, `function_catalog_snapshot()` (~150 LOC)
- `tests/unit/datafusion_engine/schema/test_snapshot_collector.py`
- `tests/unit/datafusion_engine/schema/test_function_catalog_reader.py`

### Legacy Decommission/Delete Scope

- Delete `*_snapshot()` methods from `SchemaIntrospector` (moved to `snapshot_collector.py`)
- Delete function catalog methods from `SchemaIntrospector` (moved to `function_catalog_reader.py`)
- `SchemaIntrospector` retains table inspection methods (`table_schema`, `table_columns`, `table_constraints`, etc.) (~350 LOC)

---

## S13. Replace Deferred Import Pattern with Generic Factory

### Goal

Replace the 15 `_defer_import_*` one-liner functions in `runtime_extensions.py` with a single generic factory function.

### Representative Code Snippets

```python
# src/datafusion_engine/session/runtime_extensions.py — AFTER
from __future__ import annotations

from typing import TypeVar

T = TypeVar("T")


def _deferred_import(module_path: str, attr: str) -> object:
    """Import a symbol lazily to avoid circular imports at module scope."""
    import importlib
    mod = importlib.import_module(module_path)
    return getattr(mod, attr)


# Replace 15 individual functions like:
#   def _defer_import_cache_tables(): ...
#   def _defer_import_tracing(): ...
# With direct calls:
#   _deferred_import("datafusion_engine.session.runtime_cache", "install_cache_tables")
```

### Files to Edit

- `src/datafusion_engine/session/runtime_extensions.py` — replace 15 `_defer_import_*` functions with generic factory
- `src/datafusion_engine/session/runtime_context.py` — update call sites

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete all 15 `_defer_import_*` functions from `runtime_extensions.py`

---

## S14. Define Port Protocols for Semantics/Relspec Engine Decoupling

### Goal

Begin the long-term decoupling of middle-ring modules (`semantics/`, `relspec/`) from engine-ring types by defining port protocols that abstract the DataFusion-specific APIs they consume.

### Representative Code Snippets

```python
# src/semantics/ports.py (NEW)
"""Port protocols for engine-agnostic semantic compilation."""
from __future__ import annotations

from typing import Protocol

import pyarrow as pa


class DataFramePort(Protocol):
    """Abstract DataFrame operations used by semantic compiler."""

    def select(self, *columns: str) -> DataFramePort: ...
    def filter(self, predicate: object) -> DataFramePort: ...
    def join(self, right: DataFramePort, on: str) -> DataFramePort: ...
    def collect(self) -> pa.Table: ...


class SessionPort(Protocol):
    """Abstract session operations used by semantic pipeline."""

    def sql(self, query: str) -> DataFramePort: ...
    def register_table(self, name: str, table: pa.Table) -> None: ...
    def table_names(self) -> list[str]: ...


class UdfResolverPort(Protocol):
    """Abstract UDF resolution used by semantic compiler."""

    def resolve_udf(self, name: str) -> object: ...
    def udf_expr(self, name: str, *args: object) -> object: ...
```

```python
# src/relspec/ports.py (NEW)
"""Port protocols for engine-agnostic task graph construction."""
from __future__ import annotations

from typing import Protocol


class LineagePort(Protocol):
    """Abstract lineage extraction used by inferred_deps."""

    def extract_lineage(self, plan: object) -> object: ...
    def resolve_required_udfs(self, bundle: object) -> frozenset[str]: ...
```

### Files to Edit

- `src/semantics/compiler.py` — add `SessionPort` type hint alongside concrete `SessionContext` (gradual migration); make `ctx` attribute private (`_ctx`)
- `src/relspec/inferred_deps.py` — add `LineagePort` type hint alongside concrete imports (gradual migration)
- `src/relspec/execution_package.py` — use 5 already-defined protocols (lines 24-57) in function signatures instead of `object`
- `src/relspec/contracts.py` — extract metadata spec functions into `relspec/metadata.py`; replace `_optional_module_attr` with explicit `DatasetSpecProvider` protocol
- `src/relspec/contracts.py` — add `out_degree(task_name)` method to `TaskGraphLike` protocol

### New Files to Create

- `src/semantics/ports.py` — port protocols: `SessionPort`, `UdfResolverPort`, `SchemaProviderPort`, `OutputWriterPort`
- `src/relspec/ports.py` — port protocols: `LineagePort`, `DatasetSpecProvider`
- `src/relspec/metadata.py` — metadata spec functions extracted from `contracts.py`
- `src/storage/deltalake/ports.py` — `ControlPlanePort` protocol to invert engine-storage dependency
- `tests/unit/semantics/test_ports.py` — protocol compliance tests
- `tests/unit/relspec/test_ports.py` — protocol compliance tests
- `tests/unit/relspec/test_metadata.py`

### Legacy Decommission/Delete Scope

- Delete metadata spec functions from `relspec/contracts.py` (moved to `relspec/metadata.py`)
- Delete `_optional_module_attr` helper from `relspec/inferred_deps.py` (replaced by `DatasetSpecProvider` protocol)
- None of the port protocols themselves cause deletions — they are additive. Engine-ring imports in `semantics/` and `relspec/` will be gradually replaced by port protocol usage in future scope items.

---

## S15. Replace `self: Any` Mixin Anti-Pattern with Protocol

### Goal

Define a `RuntimeProfileInterface` protocol to replace the 33 `self: Any` annotations in `_RuntimeContextMixin` and related mixins, re-enabling static type checking.

### Representative Code Snippets

```python
# src/datafusion_engine/session/runtime_protocol.py (NEW)
"""Protocol for RuntimeProfile mixins — replaces self: Any."""
from __future__ import annotations

from typing import Protocol

from datafusion import SessionContext


class RuntimeProfileLike(Protocol):
    """Structural interface satisfied by DataFusionRuntimeProfile."""

    @property
    def ctx(self) -> SessionContext: ...

    @property
    def session_id(self) -> str: ...

    @property
    def policies(self) -> object: ...  # PolicyBundleConfig

    @property
    def diagnostics(self) -> object: ...  # DiagnosticsCollector

    def session_runtime(self) -> object: ...
```

```python
# src/datafusion_engine/session/runtime_context.py — AFTER
class _RuntimeContextMixin:
    def _ephemeral_context_phases(
        self: RuntimeProfileLike,  # was: self: Any
        ctx: SessionContext,
    ) -> tuple[RegistrationPhase, ...]:
        ...
```

### Files to Edit

- `src/datafusion_engine/session/runtime_context.py` — replace all 33 `self: Any` with `self: RuntimeProfileLike`
- `src/datafusion_engine/session/runtime_diagnostics_mixin.py` — replace `self: Any` similarly
- `src/datafusion_engine/session/runtime_ops.py` — replace `self: Any` and `getattr()` dispatch

### New Files to Create

- `src/datafusion_engine/session/runtime_protocol.py` — `RuntimeProfileLike` protocol
- `tests/unit/datafusion_engine/session/test_runtime_protocol.py`

### Legacy Decommission/Delete Scope

- Delete all 33 `self: Any` annotations across `runtime_context.py`, `runtime_diagnostics_mixin.py`, and `runtime_ops.py` (replaced by protocol types)

---

## S16. Consolidate Arrow Type Mapping Knowledge

### Goal

Unify the fragmented Arrow type mapping scattered across 3 modules into a single canonical module.

### Representative Code Snippets

```python
# src/datafusion_engine/schema/type_resolution.py (NEW)
"""Canonical Arrow type mapping — single source of truth."""
from __future__ import annotations

import pyarrow as pa

# Unified mapping from type hint strings to Arrow types
TYPE_HINT_TO_ARROW: dict[str, pa.DataType] = {
    "str": pa.utf8(),
    "int": pa.int64(),
    "float": pa.float64(),
    "bool": pa.bool_(),
    "bytes": pa.binary(),
    # ... complete mapping from field_types.py:28-35
}

# Unified mapping from Arrow types to SQL type strings
ARROW_TO_SQL: dict[pa.DataType, str] = {
    pa.utf8(): "VARCHAR",
    pa.int64(): "BIGINT",
    pa.float64(): "DOUBLE",
    pa.bool_(): "BOOLEAN",
    # ... complete mapping from contracts.py:875-882
}


def resolve_arrow_type(hint: str) -> pa.DataType:
    """Resolve a type hint string to an Arrow data type."""
    ...

def arrow_type_to_sql(dtype: pa.DataType) -> str:
    """Convert an Arrow data type to SQL type string."""
    ...
```

### Files to Edit

- `src/datafusion_engine/schema/field_types.py` — import from `type_resolution` instead of local `_TYPE_HINT_MAP`
- `src/datafusion_engine/schema/contracts.py` — import from `type_resolution` instead of local `_arrow_type_to_sql`
- `src/datafusion_engine/encoding/policy.py` — import from `type_resolution` instead of local `_ensure_arrow_dtype`

### New Files to Create

- `src/datafusion_engine/schema/type_resolution.py` — canonical type mapping
- `tests/unit/datafusion_engine/schema/test_type_resolution.py`

### Legacy Decommission/Delete Scope

- Delete `_TYPE_HINT_MAP` from `field_types.py` (moved to `type_resolution.py`)
- Delete `_arrow_type_to_sql` from `contracts.py` (moved to `type_resolution.py`)
- Delete `_ensure_arrow_dtype` from `encoding/policy.py` if it duplicates type mapping (moved to `type_resolution.py`)

---

## S17. Address Hidden DataFusionRuntimeProfile Construction (DI Violations)

### Goal

Make `DataFusionRuntimeProfile` or equivalent context explicit in 7+ leaf helper functions that currently construct it internally, improving testability and honoring dependency inversion.

### Representative Code Snippets

```python
# src/datafusion_engine/session/runtime_dataset_io.py — AFTER (inject profile)
# Lines 170, 279, 335, 407: currently construct DataFusionRuntimeProfile() internally

def dataset_io_helper(
    *,
    profile: DataFusionRuntimeProfile,  # NOW REQUIRED — was hidden construction
    dataset_name: str,
) -> object:
    """Perform dataset IO with explicit profile."""
    ctx = profile.session_runtime().ctx
    ...
```

### Files to Edit

- `src/datafusion_engine/session/runtime_dataset_io.py` — add `profile` parameter to 4 functions (lines 170, 279, 335, 407)
- `src/datafusion_engine/kernels.py` — add `profile` parameter (lines 115-123)
- `src/datafusion_engine/encoding/policy.py` — accept `SessionContext` parameter (line 144)
- `src/datafusion_engine/schema/validation.py` — accept `SessionContext` parameter (line 111)
- `src/datafusion_engine/schema/finalize.py` — accept `SessionContext` parameter (line 648, hidden construction)
- `src/datafusion_engine/udf/platform.py` — accept `profile` parameter (line 16)
- `src/datafusion_engine/tables/metadata.py` — convert `_TABLE_PROVIDER_METADATA_BY_CONTEXT` module-level mutable dict into injectable `TableProviderMetadataRegistry`
- `src/datafusion_engine/cache/inventory.py`, `src/datafusion_engine/cache/ledger.py` — inject `WritePipeline`/`WriterPort` instead of inline construction
- All callers of these functions — pass profile/ctx explicitly

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete all `DataFusionRuntimeProfile()` default constructions inside the 7+ leaf helper functions
- Delete module-level `_TABLE_PROVIDER_METADATA_BY_CONTEXT` dict (replaced by injectable registry)
- Delete inline `WritePipeline` construction in cache modules (replaced by injected port)

---

## S18. Demeter Convenience Methods and Profile Decomposition

### Goal

Reduce deep attribute chains (3-4 levels) on `DataFusionRuntimeProfile` and related policy types by adding convenience methods. Decompose the 53+ field `PolicyBundleConfig` into focused sub-configs.

### Representative Code Snippets

```python
# src/datafusion_engine/session/runtime.py — convenience methods
class DataFusionRuntimeProfile:
    ...
    def delta_runtime_ctx(self) -> object:
        """Shortcut for self.delta_ops.delta_service().runtime_ctx()."""
        return self.delta_ops.delta_service().runtime_ctx()

    def semantic_cache_overrides(self) -> object:
        """Shortcut for self.data_sources.semantic_output.cache_overrides."""
        return self.data_sources.semantic_output.cache_overrides
```

```python
# src/datafusion_engine/session/policy_groups.py (NEW — focused sub-configs)
@dataclass(frozen=True)
class DeltaPolicyGroup:
    """Delta-related policies from PolicyBundleConfig."""
    write_policy: object
    maintenance_policy: object
    mutation_policy: object

@dataclass(frozen=True)
class ScanPolicyGroup:
    """Scan-related policies from PolicyBundleConfig."""
    listing_defaults: object
    delta_scan_defaults: object
```

### Files to Edit

- `src/datafusion_engine/session/runtime.py` — add convenience methods (`delta_runtime_ctx`, `semantic_cache_overrides`, etc.)
- `src/datafusion_engine/session/runtime_profile_config.py` — decompose `PolicyBundleConfig` into sub-groups
- `src/datafusion_engine/views/registry.py` — add `to_snapshot_payload()` methods on resolved policy types to flatten deep chains in `registry_snapshot()` (lines 293-312)

### New Files to Create

- `src/datafusion_engine/session/policy_groups.py` — focused policy sub-configs
- `tests/unit/datafusion_engine/session/test_policy_groups.py`

### Legacy Decommission/Delete Scope

- Delete deep attribute chains at call sites that use new convenience methods (no API removal, just caller simplification)

---

## S19. Extract/Extraction Package Cleanup and Dependency Fixes

### Goal

Resolve the package naming confusion between `src/extract/` and `src/extraction/`, relocate misplaced modules to their correct architectural ring, and fix cross-layer dependency violations in the extraction layer.

### Representative Code Snippets

```python
# src/semantics/semantic_boundary.py (MOVED from src/extraction/semantic_boundary.py)
"""Semantic model boundary helpers — belongs in semantics ring."""
from __future__ import annotations

from semantics.registry import SEMANTIC_MODEL
# Now the import is within the same ring (middle ring)
```

### Files to Edit

- `src/extraction/semantic_boundary.py` — move to `src/semantics/semantic_boundary.py`
- `src/extraction/engine_session_factory.py` — evaluate dependency on `relspec.pipeline_policy`; move or abstract if cross-ring
- `src/datafusion_engine/dataset/semantic_catalog.py` — move to `src/semantics/` (engine ring code that belongs in middle ring)
- All callers of relocated modules — update import paths

### New Files to Create

None (relocations only).

### Legacy Decommission/Delete Scope

- Delete `src/extraction/semantic_boundary.py` (relocated to `src/semantics/`)
- Delete `src/datafusion_engine/dataset/semantic_catalog.py` (relocated to `src/semantics/`)

---

## S20. Additional Module Decompositions (Medium Priority)

### Goal

Address remaining god objects and oversized modules not covered by S8-S12: `control_plane_core.py` request struct extraction, `delta_read.py` type extraction, `schema/contracts.py` constraint extraction, `finalize.py` runtime extraction, `obs/otel/config.py` split, and `semantics/pipeline_build.py` decomposition.

### Representative Code Snippets

```python
# src/datafusion_engine/delta/control_plane_types.py (NEW)
"""Request and response types for Delta control plane."""
from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class DeltaMergeRequest:
    ...

@dataclass(frozen=True)
class DeltaCdfProviderBundle:
    ...
```

```python
# src/storage/deltalake/delta_types.py (NEW)
"""Shared data types extracted from delta_read.py."""
from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class DeltaSnapshotLookup:
    ...

@dataclass(frozen=True)
class DeltaCdfOptions:
    ...
```

### Files to Edit

- `src/datafusion_engine/delta/control_plane_core.py` — extract request/response structs
- `src/storage/deltalake/delta_read.py` — extract 10 dataclass definitions
- `src/datafusion_engine/schema/contracts.py` — extract `ConstraintSpec`/`TableConstraints` into `schema/constraints.py`; extract `SchemaDivergence` into `schema/divergence.py`
- `src/datafusion_engine/schema/finalize.py` — extract DataFusion-dependent operations into `finalize_runtime.py`
- `src/obs/otel/config.py` (~840 LOC) — split into `config_types.py` and `config_resolution.py`
- `src/semantics/pipeline_build.py` (736 LOC) — extract CDF resolution into `cdf_resolution.py`, materialization dispatch into `build_executor.py`

### New Files to Create

- `src/datafusion_engine/delta/control_plane_types.py` — request/response types
- `src/storage/deltalake/delta_types.py` — shared Delta data types
- `src/datafusion_engine/schema/constraints.py` — constraint type definitions
- `src/datafusion_engine/schema/divergence.py` — schema divergence detection
- `src/datafusion_engine/schema/finalize_runtime.py` — DataFusion-dependent finalization
- `src/obs/otel/config_types.py` — OTel configuration types
- `src/obs/otel/config_resolution.py` — OTel configuration resolution
- `src/semantics/cdf_resolution.py` — CDF input resolution from pipeline_build
- `src/semantics/build_executor.py` — build execution orchestration from pipeline_build
- `tests/unit/datafusion_engine/delta/test_control_plane_types.py`
- `tests/unit/storage/deltalake/test_delta_types.py`
- `tests/unit/datafusion_engine/schema/test_constraints.py`
- `tests/unit/obs/otel/test_config_types.py`
- `tests/unit/semantics/test_cdf_resolution.py`
- `tests/unit/semantics/test_build_executor.py`

### Legacy Decommission/Delete Scope

- Delete request/response structs from `control_plane_core.py` (moved)
- Delete dataclass definitions from `delta_read.py` (moved to `delta_types.py`)
- Delete constraint types from `schema/contracts.py` (moved to `constraints.py`)
- Delete `SchemaDivergence` from `schema/contracts.py` (moved to `divergence.py`)
- Delete DataFusion-dependent functions from `finalize.py` (moved to `finalize_runtime.py`)
- Delete monolithic `obs/otel/config.py` (split into `config_types.py` + `config_resolution.py`)
- Delete CDF resolution and build execution from `pipeline_build.py` (moved to dedicated modules)

---

## S21. Correctness, Testability, and Observability Improvements

### Goal

Address scattered correctness findings, testability gaps, and missing observability that don't fit into structural refactoring scope items.

### Representative Code Snippets

```python
# tests/unit/cli/test_config_parity.py (NEW)
"""Verify field parity between config spec and runtime model."""
from __future__ import annotations

from core.config_specs import RootConfigSpec
from cli.config_models import RootConfigRuntime


def test_config_spec_field_parity() -> None:
    """All RootConfigSpec fields must have a corresponding RootConfigRuntime field."""
    spec_fields = set(RootConfigSpec.__struct_fields__)
    runtime_fields = set(RootConfigRuntime.model_fields.keys())
    assert spec_fields == runtime_fields, f"Drift: spec-only={spec_fields - runtime_fields}, runtime-only={runtime_fields - spec_fields}"
```

### Files to Edit

- `src/obs/otel/bootstrap.py` — replace `reset_providers_for_tests()` SDK private attribute patching (`_TRACER_PROVIDER_SET_ONCE`, etc.) with official `InMemorySpanExporter`/`InMemoryMetricReader`
- `src/datafusion_engine/schema/validation.py` — add structured logging to `validate_table()`
- `src/datafusion_engine/schema/finalize.py` — add structured logging to `finalize()`
- `src/graph/build.py` — make signal handler registration optional in `build_graph_product()` (accept `install_signal_handlers: bool = True`)
- `src/relspec/compiled_policy.py` — add structured debug logging in `_derive_cache_policies`
- `src/datafusion_engine/plan/bundle_artifact.py` — add postcondition assertion after `build_plan_artifact` ensuring non-null fingerprint
- `src/datafusion_engine/plan/artifact_store_core.py` — add deduplication check before appending plan artifact rows using `plan_identity_hash`
- `src/serde_schema_registry.py` — document as cross-cutting module; consider making imports lazy

### New Files to Create

- `tests/unit/cli/test_config_parity.py` — config spec/runtime model field parity test
- `tests/unit/obs/test_otel_test_isolation.py` — verify OTel test utilities work correctly

### Legacy Decommission/Delete Scope

- Delete `_TRACER_PROVIDER_SET_ONCE`/`_METER_PROVIDER_SET_ONCE`/`_LOGGER_PROVIDER_SET_ONCE` patching from `reset_providers_for_tests()` (replaced by official test utilities)

---

## S22. Bidirectional Dependency Resolution (Storage ↔ Engine)

### Goal

Break the bidirectional dependency between `src/storage/deltalake/` and `src/datafusion_engine/delta/` by relocating shared types to a neutral location and defining port protocols at the boundary.

### Representative Code Snippets

```python
# src/storage/deltalake/ports.py (from S14) — storage-side protocol
class ControlPlanePort(Protocol):
    """Abstract control plane operations for Delta table management."""

    def delta_merge(self, request: object) -> object: ...
    def delta_schema_guard(self, table: object) -> object: ...
    def delta_observability_table(self, name: str) -> object: ...
```

```python
# src/datafusion_engine/delta/shared_types.py (NEW — neutral location)
"""Shared Delta types used by both storage and engine layers."""
from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class DeltaFeatureGate:
    """Feature gate for Delta protocol features."""
    ...

@dataclass(frozen=True)
class DeltaProtocolSnapshot:
    """Snapshot of Delta protocol state."""
    ...
```

### Files to Edit

- `src/datafusion_engine/delta/protocol.py` — identify types (`DeltaFeatureGate`, `DeltaProtocolSnapshot`) that storage imports and relocate to shared location
- `src/storage/deltalake/delta_runtime_ops.py` — update imports to use shared types or `ControlPlanePort`
- `src/storage/deltalake/delta_read.py` — update deferred imports from `datafusion_engine.delta.*`
- `src/storage/deltalake/delta_feature_mutations.py` — update deferred imports

### New Files to Create

- `src/datafusion_engine/delta/shared_types.py` — types shared between storage and engine
- `tests/unit/datafusion_engine/delta/test_shared_types.py`

### Legacy Decommission/Delete Scope

- Delete shared type definitions from `datafusion_engine/delta/protocol.py` (moved to `shared_types.py`)
- Keep `protocol.py` for protocol-specific logic that is engine-only

---

## S23. Rust-Canonical Plan Bundle and Substrait Artifact Cutover

### Goal

Make Rust `codeanatomy_engine` the canonical implementation for plan-bundle capture/build and Substrait portability artifacts. Keep Python as orchestration/typing only, and remove Python-side plan bundle assembly logic once Rust entrypoints are wired through `datafusion_ext`.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/compiler/plan_bundle.rs (existing native implementation)
pub async fn capture_plan_bundle_runtime(
    ctx: &SessionContext,
    df: &DataFrame,
) -> Result<PlanBundleRuntime> { ... }

pub async fn build_plan_bundle_artifact_with_warnings(
    request: PlanBundleArtifactBuildRequest<'_>,
) -> Result<(PlanBundleArtifact, Vec<RunWarning>)> { ... }
```

```rust
// rust/datafusion_python/src/codeanatomy_ext/session_utils.rs — AFTER (new Python bridge entrypoint)
#[pyfunction]
pub(crate) fn build_plan_bundle_artifact(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    request_payload: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    // Parse request payload, call codeanatomy_engine::compiler::plan_bundle,
    // and return mapping payload for Python runtime consumption.
    ...
}
```

```python
# src/datafusion_engine/plan/rust_bundle_bridge.py (NEW)
from __future__ import annotations

from collections.abc import Mapping

from datafusion import SessionContext

from datafusion_engine.extensions import datafusion_ext
from utils.validation import ensure_mapping


def build_plan_bundle_artifact(ctx: SessionContext, *, request: Mapping[str, object]) -> Mapping[str, object]:
    payload = datafusion_ext.build_plan_bundle_artifact(ctx, request)
    return ensure_mapping(payload, label="build_plan_bundle_artifact")
```

### Files to Edit

- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs` — add plan bundle build/capture bridge entrypoints and register them in `register_functions`/`register_internal_functions`
- `rust/datafusion_python/src/codeanatomy_ext/mod.rs` — expose new session-utils entrypoints on the extension module surface
- `src/datafusion_ext.pyi` — add typed stubs for new plan bundle bridge entrypoints
- `src/datafusion_engine/extensions/datafusion_ext.py` — expose strongly-typed wrappers for new plan bundle bridge entrypoints
- `src/datafusion_engine/plan/bundle_artifact.py` — route artifact build path through Rust bridge; keep Python facade/dataclasses only
- `src/datafusion_engine/plan/artifact_serialization.py` — align payload serialization with Rust bridge return contract
- `src/datafusion_engine/session/facade.py` — use Rust-built plan bundle artifacts for Substrait-first execution path

### New Files to Create

- `src/datafusion_engine/plan/rust_bundle_bridge.py` — Python/Rust bridge helpers for plan bundle capture/build
- `tests/unit/datafusion_engine/plan/test_rust_bundle_bridge.py`
- `rust/datafusion_python/tests/codeanatomy_ext_plan_bundle_tests.rs`

### Legacy Decommission/Delete Scope

- Delete Python Substrait encoding helpers from `bundle_artifact.py` (`_encode_substrait_bytes`, `_public_substrait_bytes`, `_to_substrait_bytes`) after Rust bridge cutover
- Delete Python-side plan-bundle runtime assembly code in `bundle_artifact.py` that duplicates `capture_plan_bundle_runtime`
- Delete Python-side optional Substrait portability fallback branches in plan bundle construction once Rust capture/build path is authoritative

---

## S24. Rust-Native Delta Write and Mutation Cutover

### Goal

Cut over all Delta writes/mutations to Rust-native control-plane entrypoints (`delta_write_ipc`, `delta_delete`, `delta_update`, `delta_merge`) and remove direct `deltalake.write_deltalake` usage from `src/`.

### Representative Code Snippets

```python
# src/datafusion_engine/delta/write_ipc_payload.py (NEW)
from __future__ import annotations

import pyarrow as pa


def table_to_ipc_stream_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()
```

```python
# src/datafusion_engine/io/write_pipeline.py — AFTER (Rust-native write)
from datafusion_engine.delta.control_plane_core import DeltaWriteRequest
from datafusion_engine.delta.transactions import write_transaction

table = result.df.to_arrow_table()
request = DeltaWriteRequest(
    table_uri=spec.table_uri,
    storage_options=storage_options,
    version=None,
    timestamp=None,
    data_ipc=table_to_ipc_stream_bytes(table),
    mode=spec.mode,
    schema_mode=spec.schema_mode,
    partition_columns=spec.partition_by,
    target_file_size=spec.target_file_size,
    extra_constraints=spec.extra_constraints,
    commit_options=commit_options,
)
report = write_transaction(self.ctx, request=request)
```

```python
# src/storage/deltalake/delta_write.py — AFTER (no Python merge fallback)
from datafusion_engine.delta.control_plane_core import DeltaMergeRequest
from datafusion_engine.delta.transactions import merge_transaction

report = merge_transaction(ctx, request=merge_request)
```

### Files to Edit

- `src/datafusion_engine/io/write_pipeline.py` — replace bootstrap `write_deltalake` path with `DeltaWriteRequest` + `write_transaction`
- `src/storage/deltalake/delta_write.py` — remove merge fallback to Python delta-rs mutation path; keep Rust control-plane path only
- `src/storage/deltalake/delta_runtime_ops.py` — remove fallback merge execution helpers and fallback heuristics
- `src/datafusion_engine/cache/inventory.py` — replace direct `write_deltalake` writes with Rust-native write transaction
- `src/datafusion_engine/bootstrap/zero_row.py` — replace direct `write_deltalake` bootstrap writes with Rust-native write transaction
- `src/datafusion_engine/delta/observability.py` — replace direct `write_deltalake` artifact writes with Rust-native write transaction
- `src/extraction/orchestrator.py` — replace `deltalake.write_deltalake` usage with engine delta service/control-plane path
- `src/datafusion_engine/delta/service.py` — route mutation/write paths through transaction helpers consistently

### New Files to Create

- `src/datafusion_engine/delta/write_ipc_payload.py` — Arrow IPC payload builders for `DeltaWriteRequest`
- `tests/unit/datafusion_engine/delta/test_write_ipc_payload.py`
- `tests/unit/datafusion_engine/io/test_write_pipeline_rust_delta.py`

### Legacy Decommission/Delete Scope

- Delete all direct `write_deltalake(` callsites from `src/` (historical baseline: 9 callsites; current refresh: 0 matches)
- Delete `_execute_delta_merge_fallback()` from `delta_runtime_ops.py`
- Delete `_should_fallback_delta_merge()` from `delta_runtime_ops.py`
- Delete fallback merge attributes (`codeanatomy.merge_fallback*`) emitted by Python fallback paths

---

## S25. Extension Contract Hardening and Shim Removal

### Goal

Enforce strict native extension contracts in design phase: remove no-op shim functions that hide missing capabilities and fail fast when required runtime entrypoints are absent.

### Representative Code Snippets

```python
# src/datafusion_engine/extensions/required_entrypoints.py (NEW)
from __future__ import annotations

REQUIRED_RUNTIME_ENTRYPOINTS: tuple[str, ...] = (
    "install_codeanatomy_runtime",
    "capabilities_snapshot",
    "session_context_contract_probe",
    "delta_write_ipc_request",
    "delta_merge_request_payload",
    "delta_cdf_table_provider",
    "register_cache_tables",
    "install_tracing",
)
```

```python
# src/datafusion_engine/extensions/datafusion_ext.py — AFTER (no no-op shims)
from datafusion_engine.extensions.required_entrypoints import REQUIRED_RUNTIME_ENTRYPOINTS


def _require_internal_callable(name: str) -> object:
    attr = getattr(_INTERNAL, name, None)
    if not callable(attr):
        msg = f"datafusion._internal is missing required entrypoint: {name}"
        raise AttributeError(msg)
    return attr
```

```python
# src/datafusion_engine/session/runtime_extensions.py — AFTER (strict install path)
install = _require_internal_callable("install_tracing")
ctx_arg, _details = _resolve_tracing_context(ctx, module)
install(ctx_arg)
```

### Files to Edit

- `src/datafusion_engine/extensions/datafusion_ext.py` — remove no-op shim implementations; require callable native entrypoints
- `src/datafusion_engine/session/runtime_extensions.py` — remove shim-tolerant behavior for tracing/cache hooks; fail fast with explicit error
- `src/datafusion_engine/udf/extension_validation.py` — validate required entrypoints against hard-cutover contract
- `src/datafusion_engine/catalog/introspection.py` — use strict extension resolution for `register_cache_tables`
- `src/test_support/datafusion_ext_stub.py` — keep stub aligned with strict required-entrypoint contract
- `src/datafusion_ext.pyi` — keep stubs synchronized with required entrypoint list

### New Files to Create

- `src/datafusion_engine/extensions/required_entrypoints.py` — canonical required-entrypoint contract
- `tests/unit/datafusion_engine/extensions/test_required_entrypoints.py`

### Legacy Decommission/Delete Scope

- Delete no-op shim function bodies from `extensions/datafusion_ext.py` (`install_planner_rules`, `install_physical_rules`, `install_schema_evolution_adapter_factory`, `install_tracing`, `register_cache_tables`)
- Delete fallback `replay_substrait_plan` path in `extensions/datafusion_ext.py` that bypasses native entrypoint checks
- Delete call sites that silently continue when required runtime entrypoints are missing

---

## S26. Rust-Native Extraction Session and Provider Registration Cutover

### Goal

Use Rust `datafusion_ext` session/provider entrypoints as the canonical extraction bootstrap path (`build_extraction_session`, `register_dataset_provider`) and remove duplicated Python session/provider assembly logic.

### Representative Code Snippets

```python
# src/extraction/rust_session_bridge.py (NEW)
from __future__ import annotations

from collections.abc import Mapping

from datafusion_engine.extensions import datafusion_ext
from utils.validation import ensure_mapping


def build_extraction_session_payload(*, parallelism: int, batch_size: int, memory_limit_bytes: int | None) -> object:
    return datafusion_ext.build_extraction_session(
        {
            "parallelism": parallelism,
            "batch_size": batch_size,
            "memory_limit_bytes": memory_limit_bytes,
        }
    )


def register_dataset_provider_payload(ctx: object, request: Mapping[str, object]) -> Mapping[str, object]:
    payload = datafusion_ext.register_dataset_provider(ctx, request)
    return ensure_mapping(payload, label="register_dataset_provider")
```

```python
# src/extraction/engine_session_factory.py — AFTER
from extraction.rust_session_bridge import build_extraction_session_payload

ctx = build_extraction_session_payload(
    parallelism=resolved_parallelism,
    batch_size=resolved_batch_size,
    memory_limit_bytes=resolved_memory_limit_bytes,
)
```

### Files to Edit

- `src/extraction/engine_session_factory.py` — route session construction through Rust session bridge
- `src/extraction/engine_runtime.py` — thread Rust-built session context through runtime assembly
- `src/datafusion_engine/dataset/resolution.py` — prefer `register_dataset_provider` bridge for Delta provider registration
- `src/extraction/orchestrator.py` — remove local provider-registration duplication and use bridge
- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs` — expand request payload support as needed by extraction/session callers

### New Files to Create

- `src/extraction/rust_session_bridge.py` — extraction bootstrap bridge for Rust session/provider entrypoints
- `tests/unit/extraction/test_rust_session_bridge.py`

### Legacy Decommission/Delete Scope

- Delete duplicated Python-only extraction session setup branches superseded by `build_extraction_session`
- Delete duplicated provider registration code paths superseded by `register_dataset_provider`
- Delete local payload-normalization helpers that become redundant after Rust bridge adoption

---

## S27. Canonical Rust Runtime Install and UDF Snapshot Contract

### Goal

Make `install_codeanatomy_runtime` + `registry_snapshot` + `udf_docs_snapshot` the only supported runtime/UDF install contract, and remove modular/legacy fallback branches in Python UDF extension orchestration.

### Representative Code Snippets

```python
# src/datafusion_engine/udf/extension_core.py — AFTER
runtime_payload = _invoke_runtime_entrypoint(
    internal,
    "install_codeanatomy_runtime",
    ctx=ctx,
    args=(enable_async, async_udf_timeout_ms, async_udf_batch_size),
)
raw_snapshot = _invoke_runtime_entrypoint(internal, "registry_snapshot", ctx=ctx)
raw_docs = _invoke_runtime_entrypoint(internal, "udf_docs_snapshot", ctx=ctx)
```

```python
# src/datafusion_engine/udf/runtime_snapshot_types.py (NEW)
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RuntimeInstallSnapshot:
    version: int
    registry: dict[str, object]
    docs: dict[str, object]
```

```rust
// rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs — AFTER
// Maintain a single runtime install contract version and payload shape
// for install_codeanatomy_runtime + registry/docs snapshots.
```

### Files to Edit

- `src/datafusion_engine/udf/extension_core.py` — remove modular runtime-install fallback path; require unified runtime install contract
- `src/datafusion_engine/udf/extension_snapshot_runtime.py` — consume canonical runtime snapshot types and remove redundant normalization branches
- `src/datafusion_engine/udf/platform.py` — align install/reporting logic with canonical runtime install payload
- `src/datafusion_engine/plan/udf_analysis.py` — consume canonical registry snapshot payload shape only
- `src/datafusion_engine/schema/introspection_routines.py` — align function/registry introspection payload assumptions with canonical runtime snapshot
- `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs` — keep runtime install payload/version and capabilities snapshot consistent with Python contract

### New Files to Create

- `src/datafusion_engine/udf/runtime_snapshot_types.py` — typed runtime install snapshot contracts
- `tests/unit/datafusion_engine/udf/test_runtime_snapshot_types.py`
- `rust/datafusion_python/tests/codeanatomy_ext_udf_contract_tests.rs`

### Legacy Decommission/Delete Scope

- Delete `_install_runtime_via_modular_entrypoints()` from `extension_core.py`
- Delete `_missing_runtime_modular_entrypoints()` from `extension_core.py`
- Delete legacy runtime-surface fallback branches based on `install_codeanatomy_udf_config` in `extension_core.py`
- Delete static fallback snapshot overlays (`_EXPR_SURFACE_SNAPSHOT_ENTRIES`) once runtime snapshot contract is authoritative

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S1, S2)

- Delete all local `ExplainRows` definitions except `runtime_profile_config.py:47` — 4 files cleaned
- Delete all local helper function copies that S1 deduplicates — 10+ functions across 15+ files
- Delete `safe_cast` parameter from `align_to_schema` and all callers
- Delete `use_cache` parameter from `runtime_for_profile()` and all callers
- Delete no-op stubs and dead code identified in S2

### Batch D2 (after S3, S4)

- Delete local `__getattr__`/`__dir__` boilerplate from 10+ `__init__.py` files (replaced by `lazy_module.py`)
- Verify no remaining `_`-prefixed names in any `__all__` list across all packages

### Batch D3 (after S6, S7, S19)

- Delete `src/obs/datafusion_runs.py` (relocated to `src/datafusion_engine/obs/`)
- Delete engine-dependent code from `src/obs/diagnostics.py` and `src/obs/metrics.py`
- Delete `coerce_to_recordbatch_reader()` from `src/utils/value_coercion.py`
- Delete monolithic implementations from `dataset_spec.py` after splitting into focused modules
- Delete `src/extraction/semantic_boundary.py` (relocated to `src/semantics/`)

### Batch D4 (after S8, S9, S10, S11, S12, S20)

- Verify all decomposed files are under 700 LOC
- Delete any `NOTE(size-exception)` comments from files that now meet size targets
- Run `wc -l` validation on all decomposed files
- Verify `pipeline_build.py`, `control_plane_core.py`, `delta_read.py`, `contracts.py`, `finalize.py`, `obs/otel/config.py` all reduced

### Batch D5 (after S14, S15, S22)

- Begin replacing concrete engine imports in `semantics/compiler.py` with port protocol types (gradual — future plan)
- All `self: Any` annotations deleted from session mixin files
- Shared Delta types relocated to neutral location, breaking bidirectional dependency

### Batch D6 (after S23, S24)

- Delete Python plan-bundle/Substrait assembly helpers from `bundle_artifact.py` superseded by Rust bridge
- Delete all direct `write_deltalake(` callsites in `src/` superseded by Rust-native write/mutation entrypoints
- Delete Python mutation fallback paths (`_execute_delta_merge_fallback`, `_should_fallback_delta_merge`) after Rust mutation cutover

### Batch D7 (after S25, S26, S27)

- Delete no-op extension shim functions from `extensions/datafusion_ext.py` (strict required-entrypoint contract now enforced)
- Delete modular/legacy UDF runtime install branches in `udf/extension_core.py` superseded by `install_codeanatomy_runtime`
- Delete Python extraction/provider registration branches superseded by Rust `build_extraction_session` + `register_dataset_provider` bridge

---

## Implementation Sequence

### Phase 1: Quick Wins (S1-S5, S13, S16, S21) — Zero to low risk
1. **S1 — Consolidate type aliases and helpers** — Zero risk, pure deduplication across all modules. Reduces noise for all subsequent work.
2. **S2 — Fix lying APIs and dead parameters** — Zero risk, correctness improvement. Independent of S1.
3. **S3 — Promote private exports** — Low risk, naming-only changes. Enables cleaner imports in later phases.
4. **S5 — Replace unstructured returns with named types** — Low risk, type improvement. Independent.
5. **S4 — Extract lazy-loading facade utility** — Low risk, creates shared utility. Must land before S6-S7 which touch `__init__.py` files.
6. **S13 — Replace deferred import pattern** — Low risk, contained to `runtime_extensions.py`.
7. **S16 — Consolidate Arrow type mapping** — Medium risk, touches schema module. Independent of decompositions.
8. **S21 — Correctness and testability improvements** — Low risk, additive tests and logging. Can run parallel with above.

### Phase 2: Dependency Direction Fixes (S6, S7, S19, S22) — Medium risk
9. **S6 — Fix obs/ dependency direction** — Medium risk, relocates files. Depends on S4 landing.
10. **S7 — Split schema_spec engine-dependent content** — Medium risk, splits module. Independent of S6.
11. **S19 — Extract/extraction package cleanup** — Medium risk, module relocations. Independent of S6-S7.
12. **S22 — Bidirectional dependency resolution (storage ↔ engine)** — Medium risk, type relocation. Depends on S3 (public names).

### Phase 3: Rust Convergence (S23-S27) — Medium to high leverage
13. **S25 — Extension contract hardening and shim removal** — Medium risk, enforces strict runtime invariants before migration work.
14. **S27 — Canonical Rust runtime install and UDF snapshot contract** — Medium risk, simplifies runtime/UDF install surface; depends on S25 strict contract.
15. **S23 — Rust-canonical plan bundle/Substrait cutover** — Medium-high risk, replaces Python plan artifact assembly with existing Rust implementation; depends on S25.
16. **S24 — Rust-native Delta write and mutation cutover** — Medium-high risk, removes Python Delta write paths in favor of Rust control-plane entrypoints; depends on S25.
17. **S26 — Rust-native extraction session/provider cutover** — Medium risk, replaces duplicated Python session/provider setup using existing Rust entrypoints; depends on S25.

### Phase 4: Decompositions (S8-S12, S20) — Medium to medium-high risk
18. **S12 — Decompose SchemaIntrospector** — Medium risk, smallest decomposition. Good warmup.
19. **S11 — Decompose delta/observability.py** — Medium risk, clear boundaries.
20. **S10 — Decompose materialization.py + fix extract session** — Medium risk, fixes DI simultaneously.
21. **S20 — Additional module decompositions** — Medium risk, 7 modules. Can parallelize with S8-S12.
22. **S9 — Decompose WritePipeline** — Medium-high risk, largest class decomposition.
23. **S8 — Decompose bundle_artifact.py** — Medium-high risk, largest file decomposition.

### Phase 5: Architectural (S14, S15, S17, S18) — Medium risk, foundational
24. **S15 — Replace self: Any mixin anti-pattern** — Medium risk, touches type system across mixin hierarchy.
25. **S18 — Demeter convenience methods and profile decomposition** — Medium risk, signature additions. Depends on S15.
26. **S14 — Define port protocols for semantics/relspec** — Low risk (additive), foundational for future decoupling.
27. **S17 — Fix hidden DI violations** — Medium risk, signature changes across 10+ functions. Depends on S15 for protocol types.

## Implementation Checklist

### Phase 1: Quick Wins
- [x] S1. Consolidate duplicated type aliases and helper functions (all modules)
- [x] S2. Fix lying APIs, dead parameters, and no-op stubs
- [ ] S3. Promote private exports and clean `__all__` lists (all packages) — partial
- [x] S5. Replace unstructured multi-value returns with named types
- [x] S4. Extract shared lazy-loading facade utility
- [x] S13. Replace deferred import pattern with generic factory
- [x] S16. Consolidate Arrow type mapping knowledge
- [x] S21. Correctness, testability, and observability improvements

### Phase 2: Dependency Direction Fixes
- [x] S6. Fix dependency direction: move engine-dependent code from inner ring
- [x] S7. Reclassify `schema_spec` and split engine-dependent content
- [x] S19. Extract/extraction package cleanup and dependency fixes
- [x] S22. Bidirectional dependency resolution (storage ↔ engine)

### Phase 3: Rust Convergence
- [x] S25. Extension contract hardening and shim removal
- [ ] S27. Canonical Rust runtime install and UDF snapshot contract — partial
- [x] S23. Rust-canonical plan bundle and Substrait artifact cutover
- [x] S24. Rust-native Delta write and mutation cutover
- [x] S26. Rust-native extraction session and provider registration cutover

### Phase 4: Decompositions
- [x] S12. Decompose `SchemaIntrospector` (709 LOC)
- [x] S11. Decompose `delta/observability.py` (1,190 LOC)
- [x] S10. Decompose `materialization.py` (1,075 LOC) + fix extract session duplication
- [x] S20. Additional module decompositions (7 modules)
- [x] S9. Decompose `WritePipeline` (1,417 LOC)
- [x] S8. Decompose `bundle_artifact.py` (2,552 LOC)

### Phase 5: Architectural
- [x] S15. Replace `self: Any` mixin anti-pattern with protocol
- [x] S18. Demeter convenience methods and profile decomposition
- [x] S14. Define port protocols for semantics/relspec/storage engine decoupling
- [x] S17. Address hidden DataFusionRuntimeProfile construction (10+ functions)

### Decommission Batches
- [x] D1. Post-S1/S2 cleanup batch
- [ ] D2. Post-S3/S4 cleanup batch — partial
- [x] D3. Post-S6/S7/S19 cleanup batch
- [x] D4. Post-decomposition verification batch
- [x] D5. Post-protocol cleanup batch
- [x] D6. Post-Rust plan/write cutover cleanup batch
- [ ] D7. Post-extension/runtime contract cleanup batch — partial
