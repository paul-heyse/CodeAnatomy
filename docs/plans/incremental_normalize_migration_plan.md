# Incremental & Normalize Removal Plan

This document provides a comprehensive implementation plan for **removing** `src/incremental/` and `src/normalize/` modules, migrating the minimal required functionality to `src/semantics/`.

## Executive Summary

| Module | Files | Lines | Action | Rationale |
|--------|-------|-------|--------|-----------|
| `src/incremental/` | 27 | ~5,238 | **Full removal** | Semantic pipeline handles CDF natively; upsert functions unused |
| `src/normalize/` | 11 | ~2,800 | **Full removal** | Functionality absorbed by semantic catalog |

**Key insight:** The semantic pipeline's `use_cdf` option in `CpgBuildOptions` handles CDF reads natively. The incremental module's state management, cursor tracking, and upsert functions are NOT used in the production pipeline (only in tests). Delta writes use `WriteMode.OVERWRITE` via `datafusion_engine.io.write`, not the incremental upsert helpers.

---

## Phase 1: Type Migrations

### Scope 1.1: Migrate IncrementalConfig to Hamilton Pipeline Types

**Current State:** `IncrementalConfig` is a simple dataclass in `incremental/types.py` used in the public API (`GraphProductBuildRequest`).

**Target Architecture:**

```python
# src/hamilton_pipeline/types/incremental.py (UPDATE - already exists)
"""Incremental pipeline configuration types."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass(frozen=True)
class IncrementalConfig:
    """Shared incremental pipeline settings.

    Note: The incremental pipeline mode has been deprecated. These settings
    are retained for backward compatibility but have no effect on pipeline
    execution. The semantic pipeline handles CDF natively via CpgBuildOptions.

    Attributes
    ----------
    enabled
        Deprecated - has no effect. Semantic pipeline uses CDF by default.
    state_dir
        Deprecated - has no effect.
    repo_id
        Optional repository identifier for manifests.
    impact_strategy
        Deprecated - has no effect.
    git_base_ref
        Git base ref for diff-based file filtering.
    git_head_ref
        Git head ref for diff-based file filtering.
    git_changed_only
        Whether to process only git-changed files.
    """

    enabled: bool = False
    state_dir: Path | None = None
    repo_id: str | None = None
    impact_strategy: Literal["hybrid", "symbol_closure", "import_closure"] = "hybrid"
    git_base_ref: str | None = None
    git_head_ref: str | None = None
    git_changed_only: bool = False

    def to_run_snapshot(self) -> IncrementalRunConfig:
        """Return a run snapshot for manifests and artifacts."""
        return IncrementalRunConfig(
            enabled=self.enabled,
            state_dir=str(self.state_dir) if self.state_dir is not None else None,
            repo_id=self.repo_id,
            git_base_ref=self.git_base_ref,
            git_head_ref=self.git_head_ref,
            git_changed_only=self.git_changed_only,
        )


@dataclass(frozen=True)
class IncrementalRunConfig:
    """Serializable incremental run configuration for manifests."""

    enabled: bool = False
    state_dir: str | None = None
    repo_id: str | None = None
    git_base_ref: str | None = None
    git_head_ref: str | None = None
    git_changed_only: bool = False


@dataclass(frozen=True)
class IncrementalFileChanges:
    """File change sets for git-based filtering.

    Retained for backward compatibility with any external consumers.
    """

    changed_file_ids: tuple[str, ...] = ()
    deleted_file_ids: tuple[str, ...] = ()
    full_refresh: bool = False


@dataclass(frozen=True)
class IncrementalImpact:
    """Impact scope derived from git diffs.

    Retained for backward compatibility with any external consumers.
    """

    changed_file_ids: tuple[str, ...] = ()
    deleted_file_ids: tuple[str, ...] = ()
    impacted_file_ids: tuple[str, ...] = ()
    full_refresh: bool = False
```

**Target Files:**
- `src/hamilton_pipeline/types/incremental.py` (UPDATE)

**Files to Update (imports):**
- `src/graph/product_build.py` - change `from incremental.types import IncrementalConfig`
- `src/hamilton_pipeline/execution.py` - change import
- `src/hamilton_pipeline/io_contracts.py` - change import
- `src/hamilton_pipeline/modules/inputs.py` - change import
- `src/cli/commands/build.py` - change import

**Implementation Checklist:**
- [ ] Update `hamilton_pipeline/types/incremental.py` with full type definitions
- [ ] Update all imports in `graph/product_build.py`
- [ ] Update all imports in `hamilton_pipeline/execution.py`
- [ ] Update all imports in `hamilton_pipeline/io_contracts.py`
- [ ] Update all imports in `hamilton_pipeline/modules/inputs.py`
- [ ] Update all imports in `cli/commands/build.py`
- [ ] Run `uv run ruff check --fix` to catch any missed imports
- [ ] Run `uv run pyright` to verify type correctness

---

### Scope 1.2: Migrate PlanFingerprintSnapshot to relspec

**Current State:** `PlanFingerprintSnapshot` in `incremental/plan_fingerprints.py` is used for plan caching/drift detection.

**Target Architecture:**

```python
# src/relspec/plan_fingerprints.py (NEW)
"""Plan fingerprint snapshot for caching and drift detection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclass(frozen=True)
class PlanFingerprintSnapshot:
    """Snapshot of plan fingerprints for drift detection.

    Attributes
    ----------
    fingerprints
        Mapping of task name to plan fingerprint hash.
    identity_hashes
        Mapping of task name to plan identity hash.
    version
        Snapshot format version.
    """

    fingerprints: Mapping[str, str] = field(default_factory=dict)
    identity_hashes: Mapping[str, str] = field(default_factory=dict)
    version: int = 1

    def fingerprint_for(self, task_name: str) -> str | None:
        """Return fingerprint for a task if available."""
        return self.fingerprints.get(task_name)

    def identity_hash_for(self, task_name: str) -> str | None:
        """Return identity hash for a task if available."""
        return self.identity_hashes.get(task_name)

    def has_drift(self, task_name: str, current_fingerprint: str) -> bool:
        """Check if a task's fingerprint has changed."""
        stored = self.fingerprint_for(task_name)
        if stored is None:
            return True  # New task = drift
        return stored != current_fingerprint


def empty_fingerprint_snapshot() -> PlanFingerprintSnapshot:
    """Return an empty fingerprint snapshot."""
    return PlanFingerprintSnapshot()
```

**Target Files:**
- `src/relspec/plan_fingerprints.py` (NEW)
- `src/relspec/__init__.py` (UPDATE exports)

**Files to Update (imports):**
- `src/relspec/execution_plan.py` - change import
- `src/hamilton_pipeline/driver_factory.py` - change import
- `tests/unit/test_task_module_builder.py` - change import
- `tests/unit/test_execution_plan_artifacts_module.py` - change import
- `tests/unit/test_plan_drift_payload.py` - change import
- `tests/unit/test_plan_unification.py` - change import
- `tests/unit/test_scheduling_hooks_admission.py` - change import
- `tests/integration/test_driver_factory_integration.py` - change import

**Implementation Checklist:**
- [ ] Create `src/relspec/plan_fingerprints.py` with `PlanFingerprintSnapshot`
- [ ] Update `src/relspec/__init__.py` exports
- [ ] Update import in `relspec/execution_plan.py`
- [ ] Update import in `hamilton_pipeline/driver_factory.py`
- [ ] Update imports in all test files
- [ ] Run tests to verify

---

### Scope 1.3: Migrate CdfFilterPolicy to semantics/incremental

**Current State:** `semantics/incremental/cdf_joins.py` imports `CdfFilterPolicy` from `incremental.cdf_filters`.

**Target Architecture:**

```python
# src/semantics/incremental/cdf_types.py (NEW)
"""Canonical CDF types for semantic incremental processing."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class CdfChangeType(Enum):
    """CDF operation types aligned with Delta Lake CDF semantics."""

    INSERT = "insert"
    UPDATE_PREIMAGE = "update_preimage"
    UPDATE_POSTIMAGE = "update_postimage"
    DELETE = "delete"


@dataclass(frozen=True)
class CdfFilterPolicy:
    """Policy for filtering CDF change types in incremental joins.

    Attributes
    ----------
    include_insert
        Include INSERT change type rows.
    include_update_postimage
        Include UPDATE_POSTIMAGE change type rows.
    include_delete
        Include DELETE change type rows.
    """

    include_insert: bool = True
    include_update_postimage: bool = True
    include_delete: bool = True

    @classmethod
    def inserts_only(cls) -> CdfFilterPolicy:
        """Policy that includes only INSERT rows."""
        return cls(include_insert=True, include_update_postimage=False, include_delete=False)

    @classmethod
    def inserts_and_updates_only(cls) -> CdfFilterPolicy:
        """Policy that includes INSERT and UPDATE_POSTIMAGE rows."""
        return cls(include_insert=True, include_update_postimage=True, include_delete=False)

    @classmethod
    def all_changes(cls) -> CdfFilterPolicy:
        """Policy that includes all change types."""
        return cls(include_insert=True, include_update_postimage=True, include_delete=True)

    def to_datafusion_predicate(self, cdf_column: str = "_change_type") -> str | None:
        """Generate DataFusion SQL predicate for filtering.

        Returns None if all types included (no filter needed).
        """
        types: list[str] = []
        if self.include_insert:
            types.append(f"'{CdfChangeType.INSERT.value}'")
        if self.include_update_postimage:
            types.append(f"'{CdfChangeType.UPDATE_POSTIMAGE.value}'")
        if self.include_delete:
            types.append(f"'{CdfChangeType.DELETE.value}'")

        if len(types) == 3:
            return None
        if not types:
            return "FALSE"

        return f"{cdf_column} IN ({', '.join(types)})"
```

**Target Files:**
- `src/semantics/incremental/cdf_types.py` (NEW)
- `src/semantics/incremental/cdf_joins.py` (UPDATE import)
- `src/semantics/incremental/__init__.py` (UPDATE exports)

**Implementation Checklist:**
- [ ] Create `src/semantics/incremental/cdf_types.py`
- [ ] Update `semantics/incremental/cdf_joins.py` to import from local `cdf_types`
- [ ] Update `semantics/incremental/__init__.py` exports
- [ ] Add unit tests for `CdfFilterPolicy`

---

## Phase 2: Remove Driver Factory Incremental Code

### Scope 2.1: Remove _plan_with_incremental_pruning

**Current State:** `driver_factory.py` has `_cdf_impacted_tasks` and `_plan_with_incremental_pruning` functions that use the incremental module. These are only active when `IncrementalConfig.enabled=True` (not the default).

**Target Architecture:** Remove these functions entirely since:
1. The semantic pipeline handles CDF natively via `use_cdf`
2. Incremental mode is not enabled by default
3. The functionality is unused in production

```python
# src/hamilton_pipeline/driver_factory.py (REMOVE these functions)

# DELETE: _cdf_impacted_tasks (lines ~304-340)
# DELETE: _active_tasks_from_impacted (lines ~343-364)
# DELETE: _plan_with_incremental_pruning (lines ~367-398)

# UPDATE: build_driver and other callers to remove incremental pruning calls
```

**Target Files:**
- `src/hamilton_pipeline/driver_factory.py` (UPDATE - remove functions)

**Implementation Checklist:**
- [ ] Remove `_cdf_impacted_tasks` function
- [ ] Remove `_active_tasks_from_impacted` function
- [ ] Remove `_plan_with_incremental_pruning` function
- [ ] Update `build_driver` to remove calls to `_plan_with_incremental_pruning`
- [ ] Remove incremental imports from driver_factory.py
- [ ] Run tests to verify nothing breaks

---

### Scope 2.2: Remove relspec/incremental.py

**Current State:** `src/relspec/incremental.py` provides `impacted_tasks_for_cdf` which uses the incremental module.

**Target Architecture:** Delete the file entirely.

**Target Files:**
- `src/relspec/incremental.py` (DELETE)
- `src/relspec/__init__.py` (UPDATE - remove export if present)

**Implementation Checklist:**
- [ ] Delete `src/relspec/incremental.py`
- [ ] Update `src/relspec/__init__.py` to remove any exports
- [ ] Verify no other files import from `relspec.incremental`

---

## Phase 3: Migrate Normalize to Semantic Catalog

### Scope 3.1: Migrate dataset_name_from_alias

**Current State:** `normalize/registry_runtime.py` provides `dataset_name_from_alias` which is imported by `incremental/delta_updates.py` (which will be deleted).

**Target Architecture:** The function can be added to semantic catalog for any remaining consumers.

```python
# src/semantics/catalog/dataset_specs.py (ADD)
"""Dataset name resolution for semantic catalog."""

from semantics.naming import canonical_output_name, SEMANTIC_OUTPUT_NAMES


def dataset_name_from_alias(alias: str) -> str:
    """Resolve dataset name from alias.

    Parameters
    ----------
    alias
        Dataset alias (e.g., "cst_refs_norm").

    Returns
    -------
    str
        Canonical dataset name.

    Raises
    ------
    KeyError
        If alias is not recognized.
    """
    # Try canonical name lookup
    canonical = canonical_output_name(alias)
    if canonical in SEMANTIC_OUTPUT_NAMES.values():
        return canonical

    # Try direct match
    if alias in SEMANTIC_OUTPUT_NAMES.values():
        return alias

    # Try with _v1 suffix
    if not alias.endswith("_v1"):
        with_version = f"{alias}_v1"
        if with_version in SEMANTIC_OUTPUT_NAMES.values():
            return with_version

    msg = f"Unknown dataset alias: {alias!r}"
    raise KeyError(msg)
```

**Target Files:**
- `src/semantics/catalog/dataset_specs.py` (NEW or UPDATE)
- `src/semantics/catalog/__init__.py` (UPDATE exports)

**Implementation Checklist:**
- [ ] Add `dataset_name_from_alias` to semantic catalog
- [ ] Update exports
- [ ] Verify any remaining consumers work

---

## Phase 4: Delete Modules

### Scope 4.1: Delete src/incremental/

**Files to Delete:**
```
src/incremental/__init__.py
src/incremental/cdf_cursors.py
src/incremental/cdf_filters.py
src/incremental/cdf_runtime.py
src/incremental/changes.py
src/incremental/delta_context.py
src/incremental/delta_updates.py
src/incremental/deltas.py
src/incremental/exports.py
src/incremental/fingerprint_changes.py
src/incremental/impact.py
src/incremental/imports_resolved.py
src/incremental/invalidations.py
src/incremental/metadata.py
src/incremental/module_index.py
src/incremental/plan_bundle_exec.py
src/incremental/plan_fingerprints.py
src/incremental/registry_builders.py
src/incremental/registry_rows.py
src/incremental/registry_specs.py
src/incremental/runtime.py
src/incremental/schemas.py
src/incremental/scip_fingerprint.py
src/incremental/snapshot.py
src/incremental/state_store.py
src/incremental/types.py
src/incremental/write_helpers.py
```

**Implementation Checklist:**
- [ ] Verify all imports have been migrated
- [ ] Run `uv run ruff check` to find any remaining imports
- [ ] Delete `src/incremental/` directory
- [ ] Run full test suite
- [ ] Update pyproject.toml if incremental is listed as a package

---

### Scope 4.2: Delete src/normalize/

**Files to Delete:**
```
src/normalize/__init__.py
src/normalize/dataset_builders.py
src/normalize/dataset_bundles.py
src/normalize/dataset_rows.py
src/normalize/dataset_specs.py
src/normalize/dataset_templates.py
src/normalize/df_view_builders.py
src/normalize/diagnostic_types.py
src/normalize/evidence_specs.py
src/normalize/registry_runtime.py
src/normalize/runtime.py
```

**Implementation Checklist:**
- [ ] Verify all imports have been migrated
- [ ] Run `uv run ruff check` to find any remaining imports
- [ ] Delete `src/normalize/` directory
- [ ] Run full test suite
- [ ] Update pyproject.toml if normalize is listed as a package

---

### Scope 4.3: Delete Incremental Tests

**Test Files to Delete:**
```
tests/incremental/
tests/unit/test_incremental_*.py
tests/integration/test_incremental_*.py
```

**Implementation Checklist:**
- [ ] Delete `tests/incremental/` directory
- [ ] Delete incremental-related unit tests
- [ ] Delete incremental-related integration tests
- [ ] Verify remaining tests pass

---

## Phase 5: Documentation Updates

### Scope 5.1: Update CLAUDE.md

Remove references to incremental module and update architecture description.

**Implementation Checklist:**
- [ ] Remove incremental module from architecture description
- [ ] Update pipeline description to note semantic pipeline handles CDF natively
- [ ] Remove normalize module references

---

### Scope 5.2: Update Architecture Docs

**Files to Update:**
- `docs/architecture/ARCHITECTURE.md`
- `docs/architecture/part_4_hamilton_pipeline.md`
- `docs/architecture/part_vi_cpg_build_and_orchestration.md`
- `docs/architecture/part_ix_public_api.md`

**Implementation Checklist:**
- [ ] Remove incremental pipeline documentation
- [ ] Update CPG build documentation
- [ ] Update public API documentation
- [ ] Note that semantic pipeline handles CDF natively

---

## Implementation Order

```
Phase 1: Type Migrations (Foundation)
├── 1.1 Migrate IncrementalConfig to hamilton_pipeline/types
├── 1.2 Migrate PlanFingerprintSnapshot to relspec
└── 1.3 Migrate CdfFilterPolicy to semantics/incremental

Phase 2: Remove Driver Factory Code
├── 2.1 Remove _plan_with_incremental_pruning
└── 2.2 Remove relspec/incremental.py

Phase 3: Migrate Normalize
└── 3.1 Migrate dataset_name_from_alias (if needed)

Phase 4: Delete Modules
├── 4.1 Delete src/incremental/
├── 4.2 Delete src/normalize/
└── 4.3 Delete incremental tests

Phase 5: Documentation
├── 5.1 Update CLAUDE.md
└── 5.2 Update architecture docs
```

## Lines of Code Impact

| Action | Lines Removed | Lines Added |
|--------|---------------|-------------|
| Delete `src/incremental/` | ~5,238 | 0 |
| Delete `src/normalize/` | ~2,800 | 0 |
| Delete incremental tests | ~2,000 (est) | 0 |
| Type migrations | 0 | ~200 |
| **Net** | **~10,000** | **~200** |

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Breaking external consumers | Low | Medium | Types retained in hamilton_pipeline/types |
| Test failures | Medium | Low | Run full suite after each phase |
| Missing import updates | Medium | Low | Use ruff/pyright to catch |
| Documentation drift | Low | Low | Update docs in Phase 5 |

## Success Criteria

1. All tests pass after each phase
2. No imports from `incremental` or `normalize` remain in src/
3. `uv run ruff check` passes
4. `uv run pyright` passes
5. Semantic pipeline continues to work with CDF enabled
6. ~10,000 lines of code removed
