# Semantic Pipeline Consolidation Plan

This document provides a comprehensive implementation plan for consolidating `src/incremental/` and `src/normalize/` functionality into `src/semantics/`, creating a unified semantic-first architecture.

## Executive Summary

| Module | Files | Lines | Current Role | Migration Target |
|--------|-------|-------|--------------|------------------|
| `src/incremental/` | 27 | ~5,238 | CDF cursor mgmt, merge strategies, state tracking | `semantics/incremental/` - proper incremental protocol |
| `src/normalize/` | 11 | ~2,800 | Dataset specs, view builders, schema contracts | `semantics/catalog/` - semantic dataset catalog |

**Key Insights from Review:**
1. **Delta CDF alone is insufficient** for "only update what changed" - it only tells you which rows changed, not how to:
   - Manage CDF cursor state (don't re-read history)
   - Merge incremental results into existing outputs
   - Apply primary key semantics for upsert operations

2. **src/normalize is still functionally necessary** - it defines outputs (CFG, type nodes, diagnostics) not covered by the semantic CPG path. These must be **migrated**, not deleted.

3. **Target architecture:** A semantic-first design where:
   - All dataset specs live in `semantics/catalog/`
   - All view builders are semantic view specs
   - Incremental behavior is a first-class protocol in `semantics/incremental/`
4. **Semantic outputs must have explicit dataset locations** (via `semantic_output_locations`, a named output catalog, or `semantic_output_root`) because materialization now fails fast when outputs are not configured.

---

## Architecture Overview

### Current State
```
src/normalize/                    src/incremental/
├── dataset_rows.py (DATASET_ROWS)  ├── cdf_cursors.py (CdfCursorStore)
├── dataset_specs.py               ├── cdf_runtime.py (read_cdf_changes)
├── df_view_builders.py            ├── delta_updates.py (upsert helpers)
└── registry_runtime.py            └── types.py (IncrementalConfig)
         │                                   │
         └──────────────┬──────────────────┘
                        │
           Used by registry_specs.py, runtime.py,
           driver_factory.py, evidence.py
```

### Target State
```
src/semantics/
├── catalog/
│   ├── dataset_specs.py      ← All dataset specifications
│   ├── dataset_rows.py       ← SemanticDatasetRow definitions
│   ├── view_builders.py      ← All view builders (semantic + analysis)
│   └── analysis_builders.py  ← Migrated analysis builders (CFG/def-use/etc.)
├── incremental/
│   ├── cdf_types.py          ← CdfFilterPolicy, CdfChangeType
│   ├── cdf_cursors.py        ← CdfCursorStore (cursor management)
│   ├── cdf_reader.py         ← CDF read with cursor tracking
│   └── config.py             ← IncrementalConfig (semantic defaults)
├── pipeline.py               ← Semantic CPG pipeline
├── compiler.py               ← SemanticCompiler
└── spec_registry.py          ← Semantic table/relationship specs
```

**Design note:** do **not** introduce a new registry module under `semantics/catalog/`.
`semantics.catalog` already owns the semantic view registry. Dataset specs/rows should
be modular helpers alongside the existing catalog, not a second registry.

---

## Phase 1: Semantic Dataset Catalog

### Scope 1.1: Create SemanticDatasetRow

The semantic catalog needs to absorb the `DatasetRow` pattern from normalize, providing:
- Dataset schema definitions
- Derived field specifications
- Contract and validation rules
- View builder associations

**Target Architecture:**

```python
# src/semantics/catalog/dataset_rows.py (NEW)
"""Semantic dataset row specifications."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from schema_spec.specs import DerivedFieldSpec
from schema_spec.system import ContractRow

if TYPE_CHECKING:
    from collections.abc import Callable
    from datafusion import DataFrame, SessionContext


@dataclass(frozen=True)
class SemanticDatasetRow:
    """Semantic dataset specification with schema and builder.

    Attributes
    ----------
    name
        Canonical versioned dataset name (e.g., "cst_refs_norm_v1").
    version
        Schema version number.
    bundles
        Evidence bundles this dataset derives from.
    fields
        Base field names from source bundles.
    derived
        Derived field specifications (computed columns).
    join_keys
        Primary key columns for upsert/merge operations.
    contract
        Schema contract for validation.
    view_builder
        Name of the view builder function.
    category
        Dataset category for organization.
    supports_cdf
        Whether this dataset supports CDF-based incremental reads.
    partition_cols
        Columns to partition by when writing.
    merge_keys
        Keys for incremental merge operations (defaults to join_keys).
    """

    name: str
    version: int
    bundles: tuple[str, ...] = ()
    fields: tuple[str, ...] = ()
    derived: tuple[DerivedFieldSpec, ...] = ()
    join_keys: tuple[str, ...] = ()
    contract: ContractRow | None = None
    view_builder: str | None = None
    category: Literal["semantic", "analysis", "diagnostic"] = "semantic"
    supports_cdf: bool = True
    partition_cols: tuple[str, ...] = ("file_id",)
    merge_keys: tuple[str, ...] | None = None

    @property
    def effective_merge_keys(self) -> tuple[str, ...]:
        """Return merge keys, defaulting to join_keys."""
        return self.merge_keys if self.merge_keys is not None else self.join_keys

    @property
    def is_semantic(self) -> bool:
        """Return True if this is a semantic normalization dataset."""
        return self.category == "semantic"

    @property
    def is_analysis(self) -> bool:
        """Return True if this is an analysis dataset (CFG, def-use, etc.)."""
        return self.category == "analysis"


# Semantic normalization datasets (from semantics/spec_registry.py)
SEMANTIC_DATASET_ROWS: tuple[SemanticDatasetRow, ...] = (
    SemanticDatasetRow(
        name="cst_refs_norm_v1",
        version=1,
        bundles=("file_identity", "span"),
        fields=("ref_id", "ref_text", "path", "bstart", "bend"),
        join_keys=("ref_id",),
        category="semantic",
    ),
    SemanticDatasetRow(
        name="cst_defs_norm_v1",
        version=1,
        bundles=("file_identity", "span"),
        fields=("def_id", "def_kind", "def_name", "path", "bstart", "bend"),
        join_keys=("def_id",),
        category="semantic",
    ),
    # ... other semantic datasets
)

# Analysis datasets (migrated from normalize/dataset_rows.py)
ANALYSIS_DATASET_ROWS: tuple[SemanticDatasetRow, ...] = (
    SemanticDatasetRow(
        name="type_exprs_norm_v1",
        version=1,
        bundles=("file_identity", "span"),
        fields=("type_expr_id", "owner_def_id", "expr_kind", "expr_text", "type_id"),
        join_keys=("type_expr_id",),
        view_builder="type_exprs_df_builder",
        category="analysis",
    ),
    SemanticDatasetRow(
        name="type_nodes_v1",
        version=1,
        bundles=(),
        fields=("type_id", "type_repr", "type_form", "origin"),
        join_keys=("type_id",),
        view_builder="type_nodes_df_builder",
        category="analysis",
    ),
    SemanticDatasetRow(
        name="py_bc_blocks_norm_v1",
        version=1,
        bundles=("file_identity", "span"),
        fields=("block_id", "code_unit_id", "start_offset", "end_offset", "kind"),
        join_keys=("code_unit_id", "block_id"),
        view_builder="cfg_blocks_df_builder",
        category="analysis",
    ),
    SemanticDatasetRow(
        name="py_bc_cfg_edges_norm_v1",
        version=1,
        bundles=("file_identity",),
        fields=("edge_id", "code_unit_id", "src_block_id", "dst_block_id", "kind"),
        join_keys=("code_unit_id", "edge_id"),
        view_builder="cfg_edges_df_builder",
        category="analysis",
    ),
    SemanticDatasetRow(
        name="py_bc_def_use_events_v1",
        version=1,
        bundles=("file_identity", "span"),
        fields=("event_id", "instr_id", "code_unit_id", "kind", "symbol"),
        join_keys=("code_unit_id", "event_id"),
        view_builder="def_use_events_df_builder",
        category="analysis",
    ),
    SemanticDatasetRow(
        name="py_bc_reaches_v1",
        version=1,
        bundles=("file_identity",),
        fields=("edge_id", "code_unit_id", "def_event_id", "use_event_id", "symbol"),
        join_keys=("code_unit_id", "symbol", "def_event_id", "use_event_id"),
        view_builder="reaching_defs_df_builder",
        category="analysis",
    ),
    SemanticDatasetRow(
        name="diagnostics_norm_v1",
        version=1,
        bundles=("file_identity", "span"),
        fields=("diag_id", "severity", "message", "diag_source", "code"),
        join_keys=("diag_id",),
        view_builder="diagnostics_df_builder",
        category="diagnostic",
    ),
)

# Combined registry
ALL_DATASET_ROWS: tuple[SemanticDatasetRow, ...] = (
    *SEMANTIC_DATASET_ROWS,
    *ANALYSIS_DATASET_ROWS,
)

_DATASET_ROWS_BY_NAME: dict[str, SemanticDatasetRow] = {
    row.name: row for row in ALL_DATASET_ROWS
}


def dataset_row(name: str) -> SemanticDatasetRow | None:
    """Lookup dataset row by name."""
    return _DATASET_ROWS_BY_NAME.get(name)


def dataset_rows(*, category: str | None = None) -> tuple[SemanticDatasetRow, ...]:
    """Return dataset rows, optionally filtered by category."""
    if category is None:
        return ALL_DATASET_ROWS
    return tuple(row for row in ALL_DATASET_ROWS if row.category == category)
```

**Target Files:**
- `src/semantics/catalog/dataset_rows.py` (NEW)
- `src/semantics/catalog/__init__.py` (UPDATE only; re-export helpers without redefining a new registry)

**Migrated From:**
- `src/normalize/dataset_rows.py` → `DATASET_ROWS` registry

**Implementation Checklist:**
- [x] Create `src/semantics/catalog/` directory
- [x] Create `dataset_rows.py` with `SemanticDatasetRow` dataclass
- [x] Migrate `DATASET_ROWS` from normalize, adding category field
- [x] Add semantic dataset rows from `spec_registry.py`
- [x] Add semantic outputs (`relation_output_v1`, `cpg_nodes_v1`, `cpg_edges_v1`) to the dataset rows/specs (CPG outputs + `relation_output_v1` now in semantic catalog rows)
- [x] Create lookup functions
- [x] Add unit tests (see `tests/unit/semantics/catalog/`)

---

### Scope 1.2: Create Dataset Spec Registry

Consolidate dataset specification logic from normalize into the semantic catalog.

**Target Architecture:**

```python
# src/semantics/catalog/dataset_specs.py (NEW)
"""Semantic dataset specifications with schema contracts."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from semantics.catalog.dataset_rows import ALL_DATASET_ROWS, SemanticDatasetRow, dataset_row
from semantics.naming import canonical_output_name

# Analysis/diagnostic aliases must be explicit (do not rely on semantic naming).
ANALYSIS_OUTPUT_ALIASES: dict[str, str] = {
    # "type_exprs_norm": "type_exprs_norm_v1",
    # "py_bc_cfg_edges_norm": "py_bc_cfg_edges_norm_v1",
}

if TYPE_CHECKING:
    from collections.abc import Iterable
    from schema_spec.system import ContractSpec, DatasetSpec


# Build specs from dataset rows
_DATASET_SPECS: dict[str, DatasetSpec] = {}
_INPUT_SCHEMAS: dict[str, pa.Schema] = {}


def _build_specs() -> None:
    """Build dataset specs from rows."""
    from semantics.catalog.spec_builder import build_dataset_spec, build_input_schema

    for row in ALL_DATASET_ROWS:
        _DATASET_SPECS[row.name] = build_dataset_spec(row)
        _INPUT_SCHEMAS[row.name] = build_input_schema(row)


def dataset_spec(name: str) -> DatasetSpec:
    """Return DatasetSpec by name."""
    if not _DATASET_SPECS:
        _build_specs()
    return _DATASET_SPECS[name]


def dataset_specs() -> Iterable[DatasetSpec]:
    """Return all dataset specs."""
    if not _DATASET_SPECS:
        _build_specs()
    return _DATASET_SPECS.values()


def dataset_schema(name: str) -> pa.Schema:
    """Return dataset schema by name."""
    return dataset_spec(name).schema()


def dataset_input_schema(name: str) -> pa.Schema:
    """Return input schema for a dataset."""
    if not _INPUT_SCHEMAS:
        _build_specs()
    return _INPUT_SCHEMAS[name]


def dataset_names() -> tuple[str, ...]:
    """Return all dataset names."""
    return tuple(row.name for row in ALL_DATASET_ROWS)


def dataset_name_from_alias(alias: str) -> str:
    """Resolve dataset name from alias.

    Provides compatibility with normalize.registry_runtime.dataset_name_from_alias.

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
    # Analysis aliases first (explicit mapping)
    if alias in ANALYSIS_OUTPUT_ALIASES:
        return ANALYSIS_OUTPUT_ALIASES[alias]

    # Try canonical lookup
    canonical = canonical_output_name(alias)
    if canonical in _DATASET_SPECS or dataset_row(canonical) is not None:
        return canonical

    # Try direct match
    if alias in _DATASET_SPECS or dataset_row(alias) is not None:
        return alias

    # Try with version suffix
    if not alias.endswith("_v1"):
        with_version = f"{alias}_v1"
        if with_version in _DATASET_SPECS or dataset_row(with_version) is not None:
            return with_version

    msg = f"Unknown dataset alias: {alias!r}"
    raise KeyError(msg)


def dataset_merge_keys(name: str) -> tuple[str, ...]:
    """Return merge keys for incremental operations."""
    row = dataset_row(name)
    if row is None:
        return ()
    return row.effective_merge_keys


def supports_incremental(name: str) -> bool:
    """Check if dataset supports CDF-based incremental reads."""
    row = dataset_row(name)
    return row.supports_cdf if row is not None else False
```

**Target Files:**
- `src/semantics/catalog/dataset_specs.py` (NEW)

**Migrated From:**
- `src/normalize/dataset_specs.py`
- `src/normalize/registry_runtime.py`

**Implementation Checklist:**
- [x] Create `dataset_specs.py` in semantic catalog
- [x] Implement spec builder functions (see `spec_builder.py`)
- [x] Add `dataset_name_from_alias` for compatibility
- [x] Add `dataset_merge_keys` for incremental support
- [x] Add `supports_incremental` check
- [x] Update imports in consumers (core runtime/evidence/registry updated; remaining normalize references are limited to facades/optional fallbacks)

---

### Scope 1.3: Migrate View Builders

Move view builders from normalize to the semantic catalog, registering them as semantic view specs.

**Target Architecture:**

```python
# src/semantics/catalog/view_builders.py (NEW)
"""Semantic view builders for all dataset categories."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext
    from semantics.config import SemanticConfig

DataFrameBuilder = Callable[[SessionContext], DataFrame]


# Import analysis builders from their implementations
def _get_analysis_builders() -> dict[str, DataFrameBuilder]:
    """Import analysis view builders."""
    from semantics.catalog.analysis_builders import (
        cfg_blocks_df_builder,
        cfg_edges_df_builder,
        def_use_events_df_builder,
        diagnostics_df_builder,
        reaching_defs_df_builder,
        type_exprs_df_builder,
        type_nodes_df_builder,
    )

    return {
        "type_exprs_df_builder": type_exprs_df_builder,
        "type_nodes_df_builder": type_nodes_df_builder,
        "cfg_blocks_df_builder": cfg_blocks_df_builder,
        "cfg_edges_df_builder": cfg_edges_df_builder,
        "def_use_events_df_builder": def_use_events_df_builder,
        "reaching_defs_df_builder": reaching_defs_df_builder,
        "diagnostics_df_builder": diagnostics_df_builder,
    }


def _get_semantic_builders(
    *,
    input_mapping: Mapping[str, str],
    config: SemanticConfig | None,
) -> dict[str, DataFrameBuilder]:
    """Build semantic view builders from spec registry."""
    from semantics.pipeline import _normalize_spec_builder
    from semantics.spec_registry import SEMANTIC_NORMALIZATION_SPECS

    builders: dict[str, DataFrameBuilder] = {}
    for spec in SEMANTIC_NORMALIZATION_SPECS:
        builders[spec.output_name] = _normalize_spec_builder(
            spec,
            input_mapping=input_mapping,
            config=config,
        )
    return builders


# Combined view builder registry
_VIEW_BUILDERS: dict[str, DataFrameBuilder] | None = None


def _build_registry(
    *,
    input_mapping: Mapping[str, str],
    config: SemanticConfig | None,
) -> dict[str, DataFrameBuilder]:
    """Build the combined view builder registry."""
    builders: dict[str, DataFrameBuilder] = {}
    builders.update(_get_semantic_builders(input_mapping=input_mapping, config=config))
    builders.update(_get_analysis_builders())
    return builders


def view_builder(
    name: str,
    *,
    input_mapping: Mapping[str, str],
    config: SemanticConfig | None,
) -> DataFrameBuilder | None:
    """Lookup view builder by dataset name."""
    global _VIEW_BUILDERS
    if _VIEW_BUILDERS is None:
        _VIEW_BUILDERS = _build_registry(input_mapping=input_mapping, config=config)
    return _VIEW_BUILDERS.get(name)


def view_builders(
    *,
    input_mapping: Mapping[str, str],
    config: SemanticConfig | None,
) -> dict[str, DataFrameBuilder]:
    """Return all view builders."""
    global _VIEW_BUILDERS
    if _VIEW_BUILDERS is None:
        _VIEW_BUILDERS = _build_registry(input_mapping=input_mapping, config=config)
    return dict(_VIEW_BUILDERS)
```

**Target Files:**
- `src/semantics/catalog/view_builders.py` (NEW)
- `src/semantics/catalog/analysis_builders.py` (NEW - migrated from normalize/df_view_builders.py)

**Migrated From:**
- `src/normalize/df_view_builders.py` → `VIEW_BUILDERS`, individual builders

**Implementation Checklist:**
- [x] Create `view_builders.py` with registry interface
- [x] Create `analysis_builders.py` with migrated builders from normalize
- [x] Update builders to use semantic patterns (stable_id, span_make UDFs)
- [x] Register semantic builders from spec_registry with resolved `input_mapping` (registry_specs now builds builders from input mapping)
- [x] Add unit tests for each builder (see `tests/unit/semantics/catalog/`)

---

## Phase 2: Incremental Protocol

**Guiding rule:** Extend the existing semantic incremental implementation
(`semantics/incremental/cdf_joins.py`, `merge_incremental_results`) instead
of introducing a parallel incremental framework.

### Scope 2.1: CDF Cursor Management

The semantic pipeline needs proper cursor management for "only update what changed" behavior.

**Target Architecture:**

```python
# src/semantics/incremental/cdf_cursors.py (NEW)
"""CDF cursor management for semantic incremental processing."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from serde_msgspec import StructBaseCompat, dumps_json, loads_json

if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclass(frozen=True)
class CdfCursor:
    """Cursor tracking last processed Delta version for a dataset.

    Attributes
    ----------
    dataset_name
        Name of the tracked dataset.
    last_version
        Last Delta table version that was processed.
    last_timestamp
        Timestamp of last processing (for debugging).
    """

    dataset_name: str
    last_version: int
    last_timestamp: str | None = None


class CdfCursorStore:
    """Persistent storage for CDF cursors.

    Manages per-dataset cursors that track the last processed Delta table
    version, enabling incremental reads that don't re-process history.

    Parameters
    ----------
    cursors_path
        Path to the directory containing cursor files.
    """

    def __init__(self, cursors_path: Path) -> None:
        self.cursors_path = cursors_path

    def ensure_dir(self) -> None:
        """Ensure the cursors directory exists."""
        self.cursors_path.mkdir(parents=True, exist_ok=True)

    def _cursor_file(self, dataset_name: str) -> Path:
        """Return file path for a dataset's cursor."""
        safe_name = dataset_name.replace("/", "_").replace("\\", "_")
        return self.cursors_path / f"{safe_name}.cursor.json"

    def save(self, cursor: CdfCursor) -> None:
        """Save a cursor to persistent storage."""
        self.ensure_dir()
        cursor_file = self._cursor_file(cursor.dataset_name)
        cursor_file.write_text(
            dumps_json(cursor, pretty=True).decode("utf-8"),
            encoding="utf-8",
        )

    def load(self, dataset_name: str) -> CdfCursor | None:
        """Load a cursor from persistent storage."""
        cursor_file = self._cursor_file(dataset_name)
        if not cursor_file.exists():
            return None
        try:
            return loads_json(
                cursor_file.read_bytes(),
                target_type=CdfCursor,
                strict=False,
            )
        except (msgspec.DecodeError, OSError):
            return None

    def delete(self, dataset_name: str) -> None:
        """Delete a cursor from persistent storage."""
        cursor_file = self._cursor_file(dataset_name)
        if cursor_file.exists():
            cursor_file.unlink()

    def all_cursors(self) -> list[CdfCursor]:
        """List all cursors in the store."""
        if not self.cursors_path.exists():
            return []
        cursors: list[CdfCursor] = []
        for cursor_file in self.cursors_path.glob("*.cursor.json"):
            cursor = self.load(cursor_file.stem.replace(".cursor", ""))
            if cursor is not None:
                cursors.append(cursor)
        return cursors
```

**Target Files:**
- `src/semantics/incremental/cdf_cursors.py` (NEW)

**Migrated From:**
- `src/incremental/cdf_cursors.py`

**Implementation Checklist:**
- [x] Create `cdf_cursors.py` in semantics/incremental
- [x] Migrate `CdfCursor` and `CdfCursorStore` classes
- [x] Add timestamp tracking for debugging (cursor updates now set timestamps)
- [x] Add unit tests (see `tests/unit/semantics/incremental/test_cdf_cursors.py`)

---

### Scope 2.2: Expand Merge Strategy Support (Reuse Existing Semantics Incremental)

Leverage the **existing** semantic incremental implementation (`semantics/incremental/cdf_joins.py`)
and the merge helper already wired through the compiler (`merge_incremental_results`). Avoid creating
an entirely new merge strategy module unless a clear gap exists.

**Target Architecture:**

```python
# src/semantics/incremental/cdf_joins.py (UPDATE)
"""Change-data-feed join framework for incremental semantic updates."""

class CDFMergeStrategy(StrEnum):
    """Merge strategy for semantic incremental outputs."""
    UPSERT = auto()
    REPLACE_PARTITION = auto()
    FULL_REFRESH = auto()


def apply_cdf_merge(
    existing: DataFrame,
    new_data: DataFrame,
    *,
    key_columns: tuple[str, ...],
    strategy: CDFMergeStrategy,
    partition_column: str | None = None,
) -> DataFrame:
    """Apply merge strategy, reusing merge_incremental_results where possible."""
    if strategy == CDFMergeStrategy.UPSERT:
        return merge_incremental_results(existing, new_data, key_columns=key_columns)
    if strategy == CDFMergeStrategy.REPLACE_PARTITION:
        # Partition replacement logic (anti-join by partition)
        ...
    if strategy == CDFMergeStrategy.FULL_REFRESH:
        return new_data
    msg = f"Unsupported merge strategy: {strategy}"
    raise ValueError(msg)
```

**Target Files:**
- `src/semantics/incremental/cdf_joins.py` (UPDATE)

**Implementation Checklist:**
- [x] Extend `CDFMergeStrategy` (APPEND/UPSERT/REPLACE/DELETE_INSERT implemented)
- [x] Reuse `merge_incremental_results` (wired in `semantics.compiler` + `semantics.incremental.cdf_joins`)
- [x] Add partition replacement support (REPLACE strategy with partition column)
- [x] Add unit tests that cover CDF merge strategies (see `tests/unit/semantics/incremental/test_merge_strategies.py`)

---

### Scope 2.3: Incremental Config

Consolidate incremental configuration with semantic defaults.

**Target Architecture:**

```python
# src/semantics/incremental/config.py (NEW)
"""Incremental configuration for semantic pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from semantics.incremental.cdf_joins import CDFMergeStrategy
from semantics.incremental.cdf_types import CdfFilterPolicy


@dataclass(frozen=True)
class IncrementalConfig:
    """Configuration for semantic incremental processing.

    Attributes
    ----------
    enabled
        Whether incremental processing is enabled.
    state_dir
        Directory for storing cursor and state files.
    cdf_filter_policy
        Policy for filtering CDF change types.
    default_merge_strategy
        Default merge strategy for outputs.
    repo_id
        Optional repository identifier.
    git_base_ref
        Git base ref for diff-based filtering.
    git_head_ref
        Git head ref for diff-based filtering.
    git_changed_only
        Whether to process only git-changed files.
    """

    enabled: bool = False
    state_dir: Path | None = None
    cdf_filter_policy: CdfFilterPolicy = field(
        default_factory=CdfFilterPolicy.inserts_and_updates_only
    )
    default_merge_strategy: CDFMergeStrategy = CDFMergeStrategy.UPSERT
    repo_id: str | None = None
    git_base_ref: str | None = None
    git_head_ref: str | None = None
    git_changed_only: bool = False

    @property
    def cursor_store_path(self) -> Path | None:
        """Return path to cursor store."""
        if self.state_dir is None:
            return None
        return self.state_dir / "cursors"

    def with_cdf_enabled(self, state_dir: Path) -> IncrementalConfig:
        """Return config with CDF enabled."""
        return IncrementalConfig(
            enabled=True,
            state_dir=state_dir,
            cdf_filter_policy=self.cdf_filter_policy,
            default_merge_strategy=self.default_merge_strategy,
            repo_id=self.repo_id,
            git_base_ref=self.git_base_ref,
            git_head_ref=self.git_head_ref,
            git_changed_only=self.git_changed_only,
        )


# Re-export for backward compatibility
__all__ = ["IncrementalConfig"]
```

**Target Files:**
- `src/semantics/incremental/config.py` (NEW)

**Migrated From:**
- `src/incremental/types.py` → `IncrementalConfig`

**Implementation Checklist:**
- [x] Create `config.py` in semantics/incremental
- [x] Add CDF filter policy integration
- [x] Add merge strategy default
- [x] Add cursor store path property
- [~] Migrate CDF filter types into `semantics/incremental` (create `cdf_types.py`, update semantic imports, convert `incremental.cdf_filters` to a facade)
- [~] Update hamilton_pipeline imports to use semantic config (most modules updated; `hamilton_pipeline/modules/task_execution.py` still type-checks `incremental.types.IncrementalConfig`)

---

### Scope 2.4: CDF Reader with Cursor Tracking

Implement CDF reading that automatically tracks cursors.

**Target Architecture:**

```python
# src/semantics/incremental/cdf_reader.py (NEW)
"""CDF reader with automatic cursor tracking."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from semantics.incremental.cdf_cursors import CdfCursor, CdfCursorStore
from semantics.incremental.cdf_types import CdfFilterPolicy
from storage.deltalake import DeltaCdfOptions, delta_table_version

if TYPE_CHECKING:
    import pyarrow as pa
    from datafusion import DataFrame, SessionContext


@dataclass(frozen=True)
class CdfReadResult:
    """Result of a CDF read operation.

    Attributes
    ----------
    data
        DataFrame containing CDF changes.
    start_version
        Starting version that was read.
    end_version
        Ending version that was read.
    row_count
        Number of rows read.
    """

    data: DataFrame
    start_version: int
    end_version: int
    row_count: int


def read_cdf_changes(
    ctx: SessionContext,
    *,
    table_path: str,
    dataset_name: str,
    cursor_store: CdfCursorStore,
    filter_policy: CdfFilterPolicy | None = None,
    storage_options: dict[str, str] | None = None,
) -> CdfReadResult | None:
    """Read CDF changes since last cursor position.

    Reads changes from a Delta table using Change Data Feed, starting
    from the version after the last cursor position. Returns None if
    no changes are available.

    Parameters
    ----------
    ctx
        DataFusion session context.
    table_path
        Path to the Delta table.
    dataset_name
        Name of the dataset (for cursor tracking).
    cursor_store
        Store for persisting cursor positions.
    filter_policy
        Policy for filtering CDF change types.
    storage_options
        Delta storage options.

    Returns
    -------
    CdfReadResult | None
        Result with CDF data, or None if no changes.
    """
    path = Path(table_path)
    if not path.exists():
        return None

    # Get current table version
    current_version = delta_table_version(
        str(path),
        storage_options=storage_options or {},
    )
    if current_version is None:
        return None

    # Load cursor to determine starting version
    cursor = cursor_store.load(dataset_name)
    if cursor is None:
        # First read - save cursor and return None (full refresh needed)
        cursor_store.save(CdfCursor(
            dataset_name=dataset_name,
            last_version=current_version,
        ))
        return None

    if cursor.last_version >= current_version:
        # No new changes
        return None

    # Read CDF changes
    starting_version = cursor.last_version + 1
    cdf_options = DeltaCdfOptions(
        starting_version=starting_version,
        ending_version=current_version,
    )

    # Register CDF table
    cdf_table_name = f"_cdf_{dataset_name}_{starting_version}_{current_version}"
    # ... implementation details for registering CDF scan ...

    df = ctx.table(cdf_table_name)

    # Apply filter policy
    policy = filter_policy or CdfFilterPolicy.inserts_and_updates_only()
    predicate = policy.to_datafusion_predicate()
    if predicate is not None:
        df = df.filter(predicate)

    # Update cursor after successful read
    cursor_store.save(CdfCursor(
        dataset_name=dataset_name,
        last_version=current_version,
    ))

    return CdfReadResult(
        data=df,
        start_version=starting_version,
        end_version=current_version,
        row_count=df.count(),
    )
```

**Target Files:**
- `src/semantics/incremental/cdf_reader.py` (NEW)

**Migrated From:**
- `src/incremental/cdf_runtime.py` → `read_cdf_changes`

**Implementation Checklist:**
- [x] Create `cdf_reader.py`
- [x] Implement cursor-aware CDF reading
- [x] Integrate with Delta scan options
- [x] Add automatic cursor update on success (CDF reader now updates cursors)
- [x] Add unit tests for `semantics.incremental.cdf_reader`

---

## Phase 3: Integration

### Scope 3.1: Update registry_specs.py

Update `datafusion_engine/views/registry_specs.py` to use semantic catalog.

**Changes:**

```python
# src/datafusion_engine/views/registry_specs.py (UPDATE)

# OLD:
from normalize.dataset_rows import DATASET_ROWS
from normalize.dataset_specs import dataset_contract_schema, dataset_spec
from normalize.df_view_builders import VIEW_BUILDERS, VIEW_BUNDLE_BUILDERS

# NEW:
from semantics.catalog.dataset_rows import ALL_DATASET_ROWS
from semantics.catalog.dataset_specs import dataset_contract_schema, dataset_spec
from semantics.catalog.view_builders import view_builders
```

**Implementation Checklist:**
- [x] Update imports in registry_specs.py to use semantic catalog
- [x] Update `_normalize_view_nodes` to resolve `input_mapping` via `semantics.input_registry`
      and pass it into `view_builders(input_mapping=..., config=...)`
- [~] Test view registration works correctly (no dedicated integration tests yet)

---

### Scope 3.2: Update session/runtime.py

Update runtime to use semantic catalog for dataset resolution.

**Changes:**

```python
# src/datafusion_engine/session/runtime.py (UPDATE)

# OLD (line ~501):
from normalize.dataset_specs import dataset_specs

# NEW:
from semantics.catalog.dataset_specs import dataset_specs
```

**Implementation Checklist:**
- [x] Update import in runtime.py
- [x] Ensure semantic output locations are included in dataset location resolution
- [~] Verify dataset schema resolution works (no explicit test coverage added here)
- [~] Test pipeline execution (needs integration coverage)

---

### Scope 3.3: Update relspec/evidence.py

Update evidence module to use semantic catalog.

**Changes:**

```python
# src/relspec/evidence.py (UPDATE)

# OLD (line ~418):
from normalize.registry_runtime import dataset_contract as normalize_dataset_contract

# NEW:
from semantics.catalog.dataset_specs import dataset_spec
```

**Implementation Checklist:**
- [x] Update import in evidence.py
- [x] Update contract resolution logic (semantic catalog contract accessors)
- [~] Test evidence validation (no new targeted tests)

---

### Scope 3.4: Update incremental/delta_updates.py

Update delta updates to use semantic catalog.

**Changes:**

```python
# src/incremental/delta_updates.py (UPDATE)

# OLD (line 22):
from normalize.registry_runtime import dataset_name_from_alias

# NEW:
from semantics.catalog.dataset_specs import dataset_name_from_alias
```

**Implementation Checklist:**
- [x] Update import in delta_updates.py
- [~] Test incremental upsert operations (no new targeted tests)

---

### Scope 3.5: Semantic Output Location Catalog (Required)

Semantic outputs are now **materialized only when explicit dataset locations are configured**.
Wire an explicit output catalog (or `semantic_output_root`) so view materialization succeeds and
outputs are discoverable by downstream consumers.

**Changes:**

```python
# DataFusionRuntimeProfile (configuration)
DataFusionRuntimeProfile(
    semantic_output_catalog_name="semantic_outputs",
    registry_catalogs={
        "semantic_outputs": DatasetCatalog(
            # cpg_nodes_v1, cpg_edges_v1, rel_*_v1, *_norm_v1, relation_output_v1
        ),
    },
)

# Hamilton output config (example)
OutputConfig(
    semantic_output_catalog_name="semantic_outputs",
    ...
)
```

**Implementation Checklist:**
- [x] Define a named output catalog for semantic outputs (default catalog wiring added)
- [x] Ensure every semantic output (including `relation_output_v1`) is present in output location resolution
- [x] Alternatively, set `semantic_output_root` if a root‑based mapping is preferred (wired via `OutputConfig` + runtime profile resolution)
- [x] Validate materialization fails fast when outputs are missing (enforced in `semantics.pipeline._materialize_semantic_outputs`)

---

## Phase 4: Deprecation and Cleanup

### Scope 4.1: Deprecate normalize Module

Convert normalize to a facade that re-exports from semantic catalog.

```python
# src/normalize/__init__.py (UPDATE to facade)
"""Normalize module - DEPRECATED.

This module is maintained for backward compatibility.
Use semantics.catalog instead.
"""

import warnings

warnings.warn(
    "The normalize module is deprecated. Use semantics.catalog instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from semantic catalog
from semantics.catalog.dataset_rows import ALL_DATASET_ROWS as DATASET_ROWS
from semantics.catalog.dataset_specs import (
    dataset_name_from_alias,
    dataset_names,
    dataset_schema,
    dataset_spec,
    dataset_specs,
)
from semantics.catalog.view_builders import view_builders as VIEW_BUILDERS
```

**Implementation Checklist:**
- [x] Convert normalize/__init__.py to facade
- [x] Add deprecation warnings behind a helper/env gate to avoid noisy imports
- [~] Verify backward compatibility (facade in place; no explicit compatibility tests)
- [x] Document migration path (architecture docs now note semantics as canonical)

---

### Scope 4.2: Deprecate incremental Module

Convert incremental to a facade that re-exports from semantic incremental.

```python
# src/incremental/__init__.py (UPDATE to facade)
"""Incremental module - DEPRECATED.

This module is maintained for backward compatibility.
Use semantics.incremental instead.
"""

import warnings

warnings.warn(
    "The incremental module is deprecated. Use semantics.incremental instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from semantic incremental
from semantics.incremental.cdf_cursors import CdfCursor, CdfCursorStore
from semantics.incremental.cdf_types import CdfChangeType, CdfFilterPolicy
from semantics.incremental.config import IncrementalConfig
from semantics.incremental.cdf_joins import CDFJoinSpec, CDFMergeStrategy
```

**Implementation Checklist:**
- [x] Convert incremental/__init__.py to facade
- [x] Add deprecation warnings behind a helper/env gate
- [~] Verify backward compatibility (facade in place; no explicit compatibility tests)
- [x] Document migration path (architecture docs now note semantics.incremental as canonical)

---

## Phase 5: Testing and Documentation

### Scope 5.1: Migrate Tests

Move tests from normalize and incremental test directories to semantic test locations.

**Implementation Checklist:**
- [x] Create `tests/unit/semantics/catalog/` directory
- [x] Migrate dataset row tests
- [x] Migrate view builder tests
- [x] Create `tests/unit/semantics/incremental/` directory
- [x] Migrate CDF cursor tests
- [x] Migrate merge strategy tests
- [~] Add integration tests for full pipeline (still missing)

---

### Scope 5.2: Update Documentation

**Files to Update:**
- `CLAUDE.md` - Update architecture description
- `docs/architecture/ARCHITECTURE.md` - Update module overview
- `docs/architecture/part_4_hamilton_pipeline.md` - Update pipeline description

**Implementation Checklist:**
- [x] Update `CLAUDE.md` (semantic catalog/incremental as canonical)
- [x] Update architecture documentation (`docs/architecture/ARCHITECTURE.md`)
- [x] Update pipeline documentation (`docs/architecture/part_4_hamilton_pipeline.md`)
- [x] Update storage/incremental documentation (`docs/architecture/part_v_storage_and_incremental.md`)
- [x] Update public API documentation (`docs/architecture/part_ix_public_api.md`)
- [~] Add semantic catalog documentation (dedicated reference doc)
- [~] Add incremental protocol documentation (dedicated reference doc)

---

## Implementation Order

```
Phase 1: Semantic Dataset Catalog (Foundation)
├── 1.1 Create SemanticDatasetRow
├── 1.2 Create Dataset Spec Registry
└── 1.3 Migrate View Builders

Phase 2: Incremental Protocol
├── 2.1 CDF Cursor Management
├── 2.2 Merge Strategies
├── 2.3 Incremental Config
└── 2.4 CDF Reader with Cursor Tracking

Phase 3: Integration
├── 3.1 Update registry_specs.py
├── 3.2 Update session/runtime.py
├── 3.3 Update relspec/evidence.py
├── 3.4 Update incremental/delta_updates.py
└── 3.5 Semantic output location catalog wiring

Phase 4: Deprecation
├── 4.1 Deprecate normalize module
└── 4.2 Deprecate incremental module

Phase 5: Testing & Documentation
├── 5.1 Migrate tests
└── 5.2 Update documentation
```

## Success Criteria

1. **All tests pass** after each phase
2. **Semantic catalog** is the single source of truth for dataset specs
3. **Incremental protocol** provides proper "only update what changed" behavior:
   - CDF cursor tracking (don't re-read history)
   - Merge strategies (upsert, partition replace)
   - Primary key semantics for each output
   - Schema evolution policy enforced on incremental outputs
4. **Backward compatibility** maintained via facade modules
5. **No direct imports** from normalize/incremental in core modules
6. **Documentation** reflects new architecture

## Architectural Principles

1. **Semantic-first:** All dataset specs and view builders live in semantic catalog
2. **Incremental as protocol:** CDF cursors + merge strategies = proper incremental
3. **Explicit over implicit:** Merge keys and strategies are declared, not inferred
4. **Outputs are explicit:** Semantic outputs require explicit dataset locations or a named output catalog
5. **Backward compatible:** Facade modules preserve existing imports during migration
6. **Testable:** Each component has clear interfaces and unit tests
