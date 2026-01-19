# Registry Common Consolidation Plan

## Goals
- Introduce a shared `registry_common` module that centralizes registry helpers and
  cross-cutting settings without blurring domain boundaries.
- Use `extract.RepoScanOptions` as the single source of truth for repo scan defaults.
- Consolidate rule and policy models into `relspec` as the canonical definition.
- Remove duplicate registry builder/spec patterns and align metadata helpers.

## Non-goals
- No behavioral changes beyond standardizing defaults and model sources.
- No refactors inside `src/arrowdsl`.

## Proposed `registry_common` layout
- `src/registry_common/__init__.py`: curated re-exports for common helpers.
- `src/registry_common/dataset_registry.py`: shared registry accessors and caching.
- `src/registry_common/field_catalog.py`: reusable field catalog builder and helpers.
- `src/registry_common/metadata.py`: shared evidence metadata helpers and JSON encoding.
- `src/registry_common/settings.py`: shared settings dataclasses and adapters
  (incremental, SCIP indexing, repo scan defaults).

## Scope item 1: Shared dataset registry helpers (CPG + incremental)
### Rationale
`cpg` and `incremental` have nearly identical dataset row models, builders, and
registry accessors. A shared registry helper reduces drift and keeps metadata
handling consistent.

### Representative pattern
```python
# src/registry_common/dataset_registry.py
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import cache
from typing import Protocol, TypeVar

from arrowdsl.core.interop import SchemaLike
from schema_spec.system import ContractSpec, DatasetSpec


class NamedRow(Protocol):
    name: str


RowT = TypeVar("RowT", bound=NamedRow)


@dataclass(frozen=True)
class DatasetRegistry:
    rows: tuple[RowT, ...]
    build_dataset_spec: Callable[[RowT], DatasetSpec]

    @property
    def rows_by_name(self) -> Mapping[str, RowT]:
        return {row.name: row for row in self.rows}

    def dataset_row(self, name: str) -> RowT:
        return self.rows_by_name[name]

    @cache
    def dataset_spec(self, name: str) -> DatasetSpec:
        return self.build_dataset_spec(self.dataset_row(name))

    @cache
    def dataset_schema(self, name: str) -> SchemaLike:
        return self.dataset_spec(name).schema()

    @cache
    def dataset_contract_spec(self, name: str) -> ContractSpec:
        return self.dataset_spec(name).contract_spec_or_default()
```

### Target files
- `src/registry_common/dataset_registry.py` (new)
- `src/registry_common/__init__.py` (new)
- `src/cpg/registry_specs.py`
- `src/incremental/registry_specs.py`
- `src/cpg/registry_builders.py`
- `src/incremental/registry_builders.py`
- `src/cpg/registry_rows.py`
- `src/incremental/registry_rows.py`

### Implementation checklist
- [ ] Create `DatasetRegistry` and `NamedRow` protocol in `registry_common`.
- [ ] Update CPG and incremental `registry_specs` to delegate to `DatasetRegistry`.
- [ ] Move duplicated `build_contract_spec` logic into a shared helper if needed.
- [ ] Ensure `DatasetRegistry` caching matches existing behavior.
- [ ] Run `uv run ruff check --fix`, `uv run pyrefly check`, `uv run pyright --warnings --pythonversion=3.13`.
- [ ] Validate dataset schema snapshots (existing tests or snapshot checks).

## Scope item 2: Shared field catalog and bundle helpers
### Rationale
Extract, normalize, and CPG define near-identical field catalog registration
patterns. Centralizing the pattern improves consistency and supports shared
validation.

### Representative pattern
```python
# src/registry_common/field_catalog.py
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

import pyarrow as pa

from schema_spec.specs import ArrowFieldSpec, dict_field


@dataclass
class FieldCatalog:
    fields: dict[str, ArrowFieldSpec] = field(default_factory=dict)

    def register(self, key: str, spec: ArrowFieldSpec) -> None:
        existing = self.fields.get(key)
        if existing is not None and existing != spec:
            msg = f"Field spec conflict for key {key!r}: {existing} vs {spec}"
            raise ValueError(msg)
        self.fields[key] = spec

    def register_many(self, entries: Mapping[str, ArrowFieldSpec]) -> None:
        for key, spec in entries.items():
            self.register(key, spec)

    def field(self, key: str) -> ArrowFieldSpec:
        return self.fields[key]

    def fields_for(self, keys: Sequence[str]) -> list[ArrowFieldSpec]:
        return [self.field(key) for key in keys]


def spec(name: str, dtype: pa.DataType, *, nullable: bool = True) -> ArrowFieldSpec:
    return ArrowFieldSpec(name=name, dtype=dtype, nullable=nullable)


def dict_spec(name: str, *, nullable: bool = True) -> ArrowFieldSpec:
    return dict_field(name, nullable=nullable)
```

### Target files
- `src/registry_common/field_catalog.py` (new)
- `src/extract/registry_fields.py`
- `src/normalize/registry_fields.py`
- `src/cpg/registry_fields.py`
- `src/normalize/registry_bundles.py`
- `src/cpg/registry_bundles.py`

### Implementation checklist
- [ ] Implement `FieldCatalog` helpers in `registry_common`.
- [ ] Replace local `_register` / `_register_many` patterns with shared helpers.
- [ ] Align dictionary-encoded field definitions via `dict_field`.
- [ ] Consolidate identical bundle catalogs for normalize and CPG if desired.
- [ ] Run full lint/type gates and compare schema metadata output.

## Scope item 3: Shared evidence metadata templates
### Rationale
Extract and normalize maintain parallel evidence metadata maps with the same key
sets. Centralizing ensures consistent keys and makes metadata policy easier to
evolve.

### Representative pattern
```python
# src/registry_common/metadata.py
from __future__ import annotations

import json
from collections.abc import Mapping


def json_bytes(payload: object) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def evidence_metadata(
    *,
    evidence_family: str,
    coordinate_system: str,
    ambiguity_policy: str,
    superior_rank: int,
    span_coord_policy: bytes | None = None,
    streaming_safe: bool | None = None,
    pipeline_breaker: bool | None = None,
    extra: Mapping[bytes, bytes] | None = None,
) -> dict[bytes, bytes]:
    meta = {
        b"evidence_family": evidence_family.encode("utf-8"),
        b"coordinate_system": coordinate_system.encode("utf-8"),
        b"ambiguity_policy": ambiguity_policy.encode("utf-8"),
        b"superior_rank": str(superior_rank).encode("utf-8"),
    }
    if span_coord_policy is not None:
        meta[b"span_coord_policy"] = span_coord_policy
    if streaming_safe is not None:
        meta[b"streaming_safe"] = str(streaming_safe).lower().encode("utf-8")
    if pipeline_breaker is not None:
        meta[b"pipeline_breaker"] = str(pipeline_breaker).lower().encode("utf-8")
    if extra:
        meta.update(extra)
    return meta
```

### Target files
- `src/registry_common/metadata.py` (new)
- `src/extract/registry_templates.py`
- `src/normalize/registry_templates.py`
- `src/cpg/registry_templates.py` (optional alignment)

### Implementation checklist
- [ ] Add shared metadata helpers to `registry_common`.
- [ ] Rebuild extract template metadata using `evidence_metadata`.
- [ ] Rebuild normalize template metadata using `evidence_metadata`.
- [ ] Preserve existing byte payloads and ordering semantics.
- [ ] Validate evidence metadata extraction and rule output metadata tests.

## Scope item 4: Repo scan defaults sourced from `extract.RepoScanOptions`
### Rationale
Hamilton inputs and extract repo scan defaults drift. Defaults must come from
`RepoScanOptions` in extract to keep a single source of truth.

### Representative pattern
```python
# src/extract/repo_scan.py
def default_repo_scan_options() -> RepoScanOptions:
    return RepoScanOptions()


def repo_scan_globs_from_options(options: RepoScanOptions) -> tuple[list[str], list[str]]:
    include_globs = list(options.include_globs)
    exclude_globs = [f"**/{name}/**" for name in options.exclude_dirs]
    exclude_globs.extend(options.exclude_globs)
    return include_globs, exclude_globs
```

```python
# src/hamilton_pipeline/modules/inputs.py
from extract.repo_scan import default_repo_scan_options, repo_scan_globs_from_options


def include_globs() -> list[str]:
    defaults = default_repo_scan_options()
    include, _ = repo_scan_globs_from_options(defaults)
    return include


def exclude_globs() -> list[str]:
    defaults = default_repo_scan_options()
    _, exclude = repo_scan_globs_from_options(defaults)
    return exclude
```

### Target files
- `src/extract/repo_scan.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/pipeline_types.py` (if defaults or schema need alignment)

### Implementation checklist
- [ ] Make `RepoScanOptions.max_files` default match pipeline defaults.
- [ ] Add `.ruff_cache` to `RepoScanOptions.exclude_dirs` to match current globs.
- [ ] Add helpers for default options and glob conversion.
- [ ] Update Hamilton input nodes to derive defaults from extract options.
- [ ] Verify `RepoScanConfig` still matches CLI overrides.
- [ ] Re-run repo scan unit/integration tests.

## Scope item 5: Consolidate rule and policy models into `relspec`
### Rationale
Normalize and relspec define overlapping policy models. Canonicalizing these in
relspec reduces drift and makes planning/execution consistent.

### Representative pattern
```python
# src/relspec/model.py
@dataclass(frozen=True)
class EvidenceSpec:
    sources: tuple[str, ...] = ()
    required_columns: tuple[str, ...] = ()
    required_types: Mapping[str, str] = field(default_factory=dict)
    required_metadata: Mapping[bytes, bytes] = field(default_factory=dict)


ExecutionMode = Literal["auto", "plan", "table", "external", "hybrid"]
```

```python
# src/normalize/rule_model.py
from relspec.model import AmbiguityPolicy, ConfidencePolicy, EvidenceSpec, ExecutionMode
from relspec.model import WinnerSelectConfig


@dataclass(frozen=True)
class NormalizeRule:
    ...
    execution_mode: ExecutionMode = "auto"
```

### Target files
- `src/relspec/model.py`
- `src/normalize/rule_model.py`
- `src/normalize/rule_defaults.py`
- `src/normalize/registry_validation.py`
- `src/normalize/rule_factories.py`
- `src/relspec/rules/*` (imports)

### Implementation checklist
- [ ] Move shared policy models into `relspec` and update imports in normalize.
- [ ] Add `required_metadata` to relspec `EvidenceSpec` (no-op for relspec).
- [ ] Adjust normalize defaults so `WinnerSelectConfig` uses `"confidence"` when desired.
- [ ] Update all references to `normalize.rule_model.*` in normalize and relspec.
- [ ] Re-run rule validation and rule snapshot tests.
- [ ] Verify schema metadata policies still resolve the same ambiguity/score behavior.

## Scope item 6: Consolidate SCIP and incremental settings
### Rationale
SCIP indexing and incremental settings exist in multiple modules with different
types and defaults. A shared settings model keeps configuration consistent.

### Representative pattern
```python
# src/registry_common/settings.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass(frozen=True)
class ScipIndexSettings:
    enabled: bool = True
    index_path_override: str | None = None
    output_dir: str = "build/scip"
    env_json_path: str | None = None
    scip_python_bin: str = "scip-python"
    target_only: str | None = None
    node_max_old_space_mb: int | None = 8192
    timeout_s: int | None = None


@dataclass(frozen=True)
class IncrementalSettings:
    enabled: bool = False
    state_dir: Path | None = None
    repo_id: str | None = None
    impact_strategy: Literal["hybrid", "symbol_closure", "import_closure"] = "hybrid"
```

### Target files
- `src/registry_common/settings.py` (new)
- `src/hamilton_pipeline/pipeline_types.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/extract/scip_extract.py`
- `src/incremental/types.py`
- `src/obs/manifest.py` (if run snapshots need conversion helpers)

### Implementation checklist
- [ ] Introduce shared `ScipIndexSettings` and `IncrementalSettings`.
- [ ] Replace `ScipIndexConfig` in pipeline types with `ScipIndexSettings` or re-export.
- [ ] Add adapter functions for `SCIPIndexOptions` and manifest snapshots.
- [ ] Align `IncrementalRunConfig` with `IncrementalSettings` via conversion helpers.
- [ ] Update Hamilton inputs to return shared settings types.
- [ ] Run incremental and SCIP integration tests.

## Sequencing plan
1. Create `registry_common` module skeleton and shared helpers.
2. Port CPG + incremental registry specs/builders to shared dataset registry.
3. Port field catalog helpers and bundle catalogs.
4. Consolidate evidence metadata helpers and update templates.
5. Align repo scan defaults to `extract.RepoScanOptions`.
6. Consolidate rule/policy models into relspec and update normalize.
7. Consolidate SCIP + incremental settings.

## Validation plan
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`
- Targeted tests:
  - Registry snapshot tests for CPG/incremental/normalize.
  - Repo scan tests and any tests that assert glob defaults.
  - Rule validation and rule artifact snapshot tests.
  - SCIP indexing integration tests if present.

