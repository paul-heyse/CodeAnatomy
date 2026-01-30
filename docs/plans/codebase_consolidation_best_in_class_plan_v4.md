# Codebase Consolidation Plan v4 (Post‑v3, Best‑in‑Class, Breaking Changes OK)

## Executive Summary

This plan is a **supplemental consolidation roadmap** that assumes the **v3 follow‑up plan has already been executed**. It intentionally avoids overlapping with v3’s system‑level changes (catalog wiring, dataset resolution, provider dominance, Rust UDF platform, identity hash renames). The remaining scopes target local, high‑leverage cleanups and structural refactors that improve maintainability without re‑touching v3’s core surfaces.

**What v4 covers (post‑v3 only):**
- Environment parsing helpers for consistent config parsing.
- Extract result type consolidation across all extractors.
- Hamilton pipeline type modularization.
- Telemetry constants unification (OTel names centralized).
- Validation utility extensions.
- Targeted registry protocol adoption.
- Incremental config unification and snapshot conversion.

---

## Design Principles (Post‑v3)

1. **No overlap with v3**: avoid re‑touching catalog/provider/scan/plan surfaces already consolidated.
2. **Single‑purpose refactors**: each scope is self‑contained and orthogonal.
3. **Breaking changes OK**: remove legacy shapes directly once updated.
4. **Strict typing + explicit naming**: add type aliases and conversion helpers where needed.

---

## Scope Index

1. Environment Parsing Extensions (env_list/env_enum) — ✅ Completed
2. Extract Result Consolidation (generic ExtractResult) — ✅ Completed
3. Pipeline Types Module Split (modularization) — ✅ Completed
4. Telemetry Constants Registry (StrEnum based) — ✅ Completed
5. Validation Utilities Extension — ✅ Completed
6. Registry Protocol Adoption (targeted) — ✅ Completed
7. Incremental Config Unification (single config + snapshot conversion) — ✅ Completed

---

## 1. Environment Parsing Extensions (env_list/env_enum)

### Goal
Provide canonical parsing helpers for list and enum environment variables and remove ad‑hoc parsing in OTel configuration.

### Status
✅ Completed (2026-01-30)

### Representative Pattern
```python
from typing import TypeVar
from enum import Enum

TEnum = TypeVar("TEnum", bound=Enum)


def env_list(
    name: str,
    *,
    default: list[str] | None = None,
    separator: str = ",",
    strip: bool = True,
) -> list[str]:
    raw = env_value(name)
    if raw is None:
        return default or []
    items = raw.split(separator)
    if strip:
        items = [item.strip() for item in items if item.strip()]
    return items


def env_enum(
    name: str,
    enum_type: type[TEnum],
    *,
    default: TEnum | None = None,
) -> TEnum | None:
    raw = env_value(name)
    if raw is None:
        return default
    try:
        return enum_type(raw.lower())
    except (ValueError, KeyError):
        return default
```

### Target Files
- Modify: `src/utils/env_utils.py`
- Modify: `src/obs/otel/attributes.py`
- Modify: `src/obs/otel/config.py`
- Create: `tests/unit/utils/test_env_utils_extended.py`

### Deletions
- Remove manual comma‑split parsing and enum parsing logic in:
  - `src/obs/otel/attributes.py`
  - `src/obs/otel/config.py`

### Checklist
- [x] Add `env_list()` and `env_enum()` helpers with type overloads.
- [x] Update OTel config parsing to use new helpers.
- [x] Add unit tests for new helpers.

---

## 2. Extract Result Consolidation (generic ExtractResult)

### Goal
Replace per‑extractor result dataclasses with a single generic `ExtractResult` type.

### Status
✅ Completed (2026-01-30)

### Representative Pattern
```python
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class ExtractResult(Generic[T]):
    table: T
    extractor_name: str
```

### Target Files
- Create: `src/extract/result_types.py`
- Modify: `src/extract/bytecode_extract.py`
- Modify: `src/extract/ast_extract.py`
- Modify: `src/extract/cst_extract.py`
- Modify: `src/extract/tree_sitter_extract.py`
- Modify: `src/extract/symtable_extract.py`
- Modify: `src/extract/python_imports_extract.py`
- Modify: `src/extract/python_external_scope.py`

### Deletions
- Remove per‑extractor `*ExtractResult` dataclasses from the above modules.

### Checklist
- [x] Add `ExtractResult` in `result_types.py`.
- [x] Update all extractors to return `ExtractResult`.
- [x] Remove old result dataclasses.

---

## 3. Pipeline Types Module Split (modularization)

### Goal
Split the Hamilton pipeline type monolith into focused submodules and remove the single giant file.

### Status
✅ Completed (2026-01-30)

### Representative Pattern
```python
# src/hamilton_pipeline/types/output_config.py
@dataclass(frozen=True)
class OutputConfig:
    work_dir: str | None
    output_dir: str | None
    overwrite_intermediate_datasets: bool
```

### Target Files
- Create: `src/hamilton_pipeline/types/__init__.py`
- Create: `src/hamilton_pipeline/types/repo_config.py`
- Create: `src/hamilton_pipeline/types/output_config.py`
- Create: `src/hamilton_pipeline/types/cache_config.py`
- Create: `src/hamilton_pipeline/types/incremental.py`
- Create: `src/hamilton_pipeline/types/execution.py`
- Modify: all imports that currently reference `src/hamilton_pipeline/pipeline_types.py`

### Deletions
- Delete: `src/hamilton_pipeline/pipeline_types.py` after imports migrate.

### Checklist
- [x] Create the `types/` subpackage.
- [x] Move dataclasses into module‑specific files.
- [x] Update all imports across the codebase.
- [x] Remove `pipeline_types.py`.

---

## 4. Telemetry Constants Registry (StrEnum based)

### Goal
Centralize all OpenTelemetry metric/attribute/scope/resource names.

### Status
✅ Completed (2026-01-30)

### Representative Pattern
```python
from enum import StrEnum

class MetricName(StrEnum):
    STAGE_DURATION = "codeanatomy.stage.duration"
    TASK_DURATION = "codeanatomy.task.duration"
```

### Target Files
- Create: `src/obs/otel/constants.py`
- Modify: `src/obs/otel/metrics.py`
- Modify: `src/obs/otel/attributes.py`
- Modify: `src/obs/otel/scopes.py`
- Modify: `src/obs/otel/resources.py`

### Deletions
- Remove scattered string constants in `metrics.py`, `attributes.py`, `scopes.py`, `resources.py`.

### Checklist
- [x] Define StrEnum classes for metrics, attributes, scopes, resources.
- [x] Update all OTel modules to use constants.

---

## 5. Validation Utilities Extension

### Goal
Add missing validators for non‑empty sequences, subset validation, and uniqueness.

### Status
✅ Completed (2026-01-30)

### Representative Pattern
```python
def ensure_unique(items: Iterable[T], *, label: str = "items") -> list[T]:
    seen: set[T] = set()
    duplicates: list[T] = []
    result: list[T] = []
    for item in items:
        if item in seen:
            duplicates.append(item)
        else:
            seen.add(item)
            result.append(item)
    if duplicates:
        raise ValueError(f"{label} contains duplicates: {duplicates}")
    return result
```

### Target Files
- Modify: `src/utils/validation.py`
- Create: `tests/unit/utils/test_validation_extended.py`

### Deletions
- None (additive change).

### Checklist
- [x] Add `ensure_not_empty`, `ensure_subset`, `ensure_unique`.
- [x] Update `__all__` exports.
- [x] Add unit tests.

---

## 6. Registry Protocol Adoption (targeted)

### Goal
Replace any remaining ad‑hoc registry dictionaries with `MutableRegistry` or `ImmutableRegistry`.

### Status
✅ Completed (2026-01-30)

### Representative Pattern
```python
from utils.registry_protocol import MutableRegistry

class ViewRegistry:
    def __init__(self) -> None:
        self._builders: MutableRegistry[str, ViewBuilder] = MutableRegistry()
```

### Target Files (audit + replace)
- Audit: `src/datafusion_engine/view_registry.py`
- Audit: `src/datafusion_engine/schema_registry.py`
- Audit: `src/hamilton_pipeline/semantic_registry.py`
- Audit: `src/datafusion_engine/provider_registry.py`

### Deletions
- Remove any ad‑hoc dict registry implementations uncovered in the audit.

### Checklist
- [x] Audit registries for direct dict storage.
- [x] Replace with `MutableRegistry`/`ImmutableRegistry` where appropriate.
- [x] Keep behavior identical.

---

## 7. Incremental Config Unification (single config + snapshot conversion)

### Goal
Consolidate incremental configuration into a single canonical type and provide explicit conversion for runtime snapshots.

### Status
✅ Completed (2026-01-30)

### Representative Pattern
```python
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

@dataclass(frozen=True)
class IncrementalConfig:
    enabled: bool = False
    state_dir: Path | None = None
    repo_id: str | None = None
    impact_strategy: Literal["hybrid", "symbol_closure", "import_closure"] = "hybrid"
    git_base_ref: str | None = None
    git_head_ref: str | None = None
    git_changed_only: bool = False

    def to_run_snapshot(self) -> IncrementalRunConfig:
        return IncrementalRunConfig(
            enabled=self.enabled,
            state_dir=str(self.state_dir) if self.state_dir is not None else None,
            repo_id=self.repo_id,
            git_base_ref=self.git_base_ref,
            git_head_ref=self.git_head_ref,
            git_changed_only=self.git_changed_only,
        )
```

### Target Files
- Modify: `src/incremental/types.py`
- Modify: `src/hamilton_pipeline/types/incremental.py`
- Modify: call sites that construct incremental run snapshots

### Deletions
- Remove any duplicate incremental config types after migration.

### Checklist
- [x] Define canonical `IncrementalConfig` in `incremental/types.py`.
- [x] Add conversion helper for runtime snapshots.
- [x] Update call sites to use canonical config.

---

## Recommended Implementation Order (Post‑v3)

1. Environment Parsing Extensions
2. Validation Utilities Extension
3. Telemetry Constants Registry
4. Extract Result Consolidation
5. Registry Protocol Adoption
6. Incremental Config Unification
7. Pipeline Types Module Split

---

## Verification (Post‑v3)

- Run tests: `uv run pytest tests/`
- Run type checks: `uv run pyright --warnings` and `uv run pyrefly`
- Run linting: `uv run ruff check`
