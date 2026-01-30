# DataFusion Engine Consolidation Plan v1

## Executive Summary

This plan addresses consolidation opportunities within `src/datafusion_engine/` (80 modules, 52,516 LOC). Analysis identified significant dead code, duplicate definitions, and fragmented responsibilities that can be streamlined without harming functionality. The consolidation targets: (1) dead code removal, (2) duplicate protocol/utility consolidation, (3) module reorganization, and (4) API surface cleanup.

## Design Principles

1. **Single source of truth** for protocols, utilities, and configuration resolution.
2. **Eliminate dead-end exports** where functions are exported but never called.
3. **Consolidate by concern** not by size—small focused modules over large monoliths.
4. **Preserve backward compatibility** via re-exports during migration.
5. **Trace to outputs** before removing—verify no hidden call paths exist.

## Scope Index

1. Dead Export Removal from `__init__.py` (Planned)
2. Dead Function Removal from runtime.py (Planned)
3. SQL Options Duplication Elimination (Planned)
4. Protocol Specs Consolidation (Planned)
5. Schema Module Utility Extraction (Planned)
6. View Registry Utility Consolidation (Planned)
7. Delta Payload Helpers Extraction (Planned)
8. UDF Snapshot Helpers Consolidation (Planned)
9. Arrow Schema Utilities Extraction (Planned)
10. Session/Runtime Streamlining (Planned)
11. Trivial Module Consolidation (Planned)
12. Deferred Cleanup and Verification (Planned)

---

## 1. Dead Export Removal from `__init__.py`

### Goal
Remove exports from the lazy-loading `_EXPORTS` dict that reference non-existent modules or are never imported.

### Status
Completed

### Representative Pattern
```python
# BEFORE: _EXPORTS references non-existent module
_EXPORTS = {
    "ExecutionPolicy": ("datafusion_engine.sql_safety", "ExecutionPolicy"),  # Module doesn't exist
    "SafeExecutor": ("datafusion_engine.sql_safety", "SafeExecutor"),
    "execute_with_policy": ("datafusion_engine.sql_safety", "execute_with_policy"),
    "validate_sql_safety": ("datafusion_engine.sql_safety", "validate_sql_safety"),
    "CompiledPlan": ("datafusion_engine.execution_facade", "CompiledPlan"),  # Never imported
}

# AFTER: Remove dead entries
# (entries deleted entirely)
```

### Target Files
- Modify: `src/datafusion_engine/__init__.py`

### Deletions
- Remove `ExecutionPolicy`, `SafeExecutor`, `execute_with_policy`, `validate_sql_safety` entries (reference non-existent `sql_safety` module)
- Remove `CompiledPlan` entry (never imported anywhere)

### ast-grep / cq Recipes
```bash
# Verify sql_safety module doesn't exist
ls src/datafusion_engine/sql_safety.py  # Should fail

# Verify CompiledPlan is never imported
cq imports src/datafusion_engine/execution_facade.py
grep -r "CompiledPlan" src/ --include="*.py" | grep -v "__init__.py"
```

### Checklist
- [x] Verify `sql_safety` module does not exist.
- [x] Verify `CompiledPlan` has 0 external imports.
- [x] Remove dead entries from `_EXPORTS` dict.
- [x] Run `uv run ruff check src/datafusion_engine/__init__.py`.

---

## 2. Dead Function Removal from runtime.py

### Goal
Remove functions exported in `__all__` that have 0 external call sites.

### Status
Completed

### Representative Pattern
```python
# BEFORE: Exported but never called
__all__ = [
    ...
    "snapshot_plans",  # 0 call sites
    "DataFusionExplainCollector",  # 0 call sites
    "DataFusionPlanCollector",  # 0 call sites
    ...
]

def snapshot_plans(...) -> ...:
    """Never called from outside runtime.py."""
    ...

# AFTER: Remove from __all__ and delete function body
# (or mark as internal with underscore prefix if used internally)
```

### Target Files
- Modify: `src/datafusion_engine/runtime.py`

### Deletions
- `snapshot_plans()` - 0 external call sites
- `DataFusionExplainCollector` class - 0 external instantiations
- `DataFusionPlanCollector` class - 0 external instantiations

### ast-grep / cq Recipes
```bash
# Verify 0 call sites
cq calls snapshot_plans
cq calls DataFusionExplainCollector
cq calls DataFusionPlanCollector

# Check internal usage
grep -n "snapshot_plans" src/datafusion_engine/runtime.py
grep -n "DataFusionExplainCollector" src/datafusion_engine/runtime.py
```

### Checklist
- [x] Confirm 0 external call sites for each function/class.
- [x] Check if used internally (may need underscore prefix instead of deletion).
- [x] Remove from `__all__` list.
- [x] Delete or rename function definitions.
- [ ] Run tests to verify no regressions.

### Implementation Notes
- `snapshot_plans` was removed; collectors were retained for internal use with underscore-prefixed names.

---

## 3. SQL Options Duplication Elimination

### Goal
Eliminate duplicate implementations of `sql_options_for_profile()` and `statement_sql_options_for_profile()` that exist in both `sql_options.py` and `runtime.py`.

### Status
Completed

### Representative Pattern
```python
# BEFORE: Identical functions in two modules
# runtime.py (lines 1750-1775)
def sql_options_for_profile(profile: DataFusionRuntimeProfile | None) -> SQLOptions:
    ...

# sql_options.py (lines 13-32)
def sql_options_for_profile(profile: DataFusionRuntimeProfile | None) -> SQLOptions:
    ...  # IDENTICAL IMPLEMENTATION

# AFTER: Single source in sql_options.py, re-export in runtime.py
# sql_options.py - keeps implementation
# runtime.py - imports and re-exports
from datafusion_engine.sql_options import (
    sql_options_for_profile,
    statement_sql_options_for_profile,
)
```

### Target Files
- Modify: `src/datafusion_engine/runtime.py` (remove duplicate implementations)
- Keep: `src/datafusion_engine/sql_options.py` (canonical source)

### Deletions
- Delete `sql_options_for_profile()` implementation from runtime.py (lines ~1750-1762)
- Delete `statement_sql_options_for_profile()` implementation from runtime.py (lines ~1763-1775)

### ast-grep / cq Recipes
```bash
# Find all callers to determine canonical source
cq calls sql_options_for_profile
grep -r "from datafusion_engine.sql_options import" src/
grep -r "from datafusion_engine.runtime import.*sql_options" src/

# Count callers per module
grep -r "sql_options_for_profile" src/ --include="*.py" | wc -l
```

### Checklist
- [x] Verify sql_options.py is the more commonly imported source (27+ files).
- [x] Delete duplicate implementations from runtime.py.
- [x] Add re-export imports to runtime.py for backward compatibility.
- [x] Verify `__all__` in both modules is correct.
- [ ] Run full test suite.

---

## 4. Protocol Specs Consolidation

### Goal
Consolidate the duplicate `_ArrowFieldSpec` and `_TableSchemaSpec` protocol definitions into a single shared module.

### Status
Partially Completed

### Representative Pattern
```python
# BEFORE: Same protocols in 3 files
# schema_policy.py (lines 19-49)
class _ArrowFieldSpec(Protocol):
    @property
    def name(self) -> str: ...
    @property
    def dtype(self) -> DataTypeLike: ...
    ...

# schema_validation.py (lines 34-64) - IDENTICAL
# arrow_schema/metadata.py (lines 281-311) - IDENTICAL

# AFTER: Single definition in shared module
# NEW FILE: src/datafusion_engine/schema_spec_protocol.py
from __future__ import annotations
from typing import TYPE_CHECKING, Protocol, Mapping, Sequence
if TYPE_CHECKING:
    from datafusion_engine.arrow_interop import DataTypeLike, SchemaLike

class ArrowFieldSpec(Protocol):
    """Protocol for Arrow field specifications."""
    @property
    def name(self) -> str: ...
    @property
    def dtype(self) -> DataTypeLike: ...
    @property
    def nullable(self) -> bool: ...
    @property
    def metadata(self) -> Mapping[str, str]: ...
    @property
    def encoding(self) -> str | None: ...

class TableSchemaSpec(Protocol):
    """Protocol for table schema specifications."""
    @property
    def name(self) -> str: ...
    @property
    def fields(self) -> Sequence[ArrowFieldSpec]: ...
    @property
    def key_fields(self) -> Sequence[str]: ...
    @property
    def required_non_null(self) -> Sequence[str]: ...
    def to_arrow_schema(self) -> SchemaLike: ...

__all__ = ["ArrowFieldSpec", "TableSchemaSpec"]
```

### Target Files
- Create: `src/datafusion_engine/schema_spec_protocol.py`
- Modify: `src/datafusion_engine/schema_policy.py` (import instead of define)
- Modify: `src/datafusion_engine/schema_validation.py` (import instead of define)
- Modify: `src/datafusion_engine/arrow_schema/metadata.py` (import instead of define)

### Deletions
- Delete `_ArrowFieldSpec` definition from schema_policy.py (lines 19-35)
- Delete `_TableSchemaSpec` definition from schema_policy.py (lines 36-49)
- Delete `_ArrowFieldSpec` definition from schema_validation.py (lines 34-50)
- Delete `_TableSchemaSpec` definition from schema_validation.py (lines 51-64)
- Delete `_ArrowFieldSpec` definition from arrow_schema/metadata.py (lines 281-297)
- Delete `_TableSchemaSpec` definition from arrow_schema/metadata.py (lines 298-311)

### ast-grep / cq Recipes
```bash
# Find all protocol definitions
ast-grep --pattern 'class _ArrowFieldSpec(Protocol):' src/datafusion_engine/
ast-grep --pattern 'class _TableSchemaSpec(Protocol):' src/datafusion_engine/

# Find usage sites
grep -rn "_ArrowFieldSpec" src/datafusion_engine/ --include="*.py"
grep -rn "_TableSchemaSpec" src/datafusion_engine/ --include="*.py"
```

### Checklist
- [x] Create `schema_spec_protocol.py` with unified definitions.
- [x] Make protocols public (remove underscore prefix).
- [ ] Update imports in schema_policy.py.
- [x] Update imports in schema_validation.py.
- [x] Update imports in arrow_schema/metadata.py.
- [x] Delete old definitions.
- [x] Run type checker to verify protocol compatibility.

### Implementation Notes
- `schema_policy.py` still imports `TableSchemaSpec` from `schema_spec.specs` at runtime; switch to `datafusion_engine.schema_spec_protocol.TableSchemaSpec` (or alias the protocol) to complete consolidation and remove the runtime dependency.

---

## 5. Schema Module Utility Extraction

### Goal
Extract shared utility functions from schema modules into dedicated helpers to eliminate duplication.

### Status
Completed

### Representative Pattern
```python
# BEFORE: _normalize_column_names() in schema_contracts.py (lines 39-50)
def _normalize_column_names(names: Sequence[str]) -> frozenset[str]:
    return frozenset(name.lower() for name in names)

# Similar logic scattered in schema_validation.py, schema_introspection.py

# AFTER: Consolidated in schema utilities
# Add to existing src/datafusion_engine/schema_contracts.py as public function
def normalize_column_names(names: Sequence[str]) -> frozenset[str]:
    """Normalize column names to lowercase frozen set."""
    return frozenset(name.lower() for name in names)
```

### Target Files
- Modify: `src/datafusion_engine/schema_contracts.py` (make helpers public)
- Modify: `src/datafusion_engine/schema_validation.py` (import shared helpers)
- Modify: `src/datafusion_engine/schema_introspection.py` (import shared helpers)

### Deletions
- Delete `_normalize_column_names()` from schema_validation.py if duplicated
- Delete `_merge_constraint_expressions()` duplicates

### ast-grep / cq Recipes
```bash
# Find normalize patterns
grep -rn "normalize.*column" src/datafusion_engine/ --include="*.py"
grep -rn "\.lower()" src/datafusion_engine/schema*.py

# Find constraint helpers
grep -rn "constraint_key_fields" src/datafusion_engine/ --include="*.py"
```

### Checklist
- [x] Identify all duplicate utility functions across schema modules.
- [x] Designate canonical location for each utility.
- [x] Make utilities public (remove underscore) if needed externally.
- [x] Update all import sites.
- [x] Delete duplicate definitions.

### Implementation Notes
- No duplicate normalize/merge helpers remained in `schema_introspection.py`; canonical helpers live in `schema_contracts.py`.

---

## 6. View Registry Utility Consolidation

### Goal
Extract duplicate lineage/UDF extraction utilities from view registry modules into shared bundle extraction helpers.

### Status
Completed

### Representative Pattern
```python
# BEFORE: _schema_from_df() duplicated in 3 files
# view_graph_registry.py (lines 605-615)
def _schema_from_df(df: DataFrame) -> pa.Schema:
    schema = df.schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Failed to resolve DataFusion schema."
    raise TypeError(msg)

# dataset_registration.py (lines 2018-2028) - IDENTICAL
# view_registry_specs.py (lines 171-181) - named _arrow_schema_from_df

# AFTER: Single utility in dedicated module
# NEW FILE: src/datafusion_engine/bundle_extraction.py
def arrow_schema_from_df(df: DataFrame) -> pa.Schema:
    """Extract PyArrow schema from DataFusion DataFrame."""
    schema = df.schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Failed to resolve DataFusion schema."
    raise TypeError(msg)

def extract_lineage_from_bundle(bundle: DataFusionPlanBundle) -> LineageReport:
    """Extract lineage report from plan bundle."""
    if bundle.optimized_logical_plan is None:
        msg = "DataFusion plan bundle missing optimized logical plan."
        raise ValueError(msg)
    from datafusion_engine.lineage_datafusion import extract_lineage
    return extract_lineage(bundle.optimized_logical_plan, udf_snapshot=bundle.artifacts.udf_snapshot)

def resolve_required_udfs_from_bundle(
    bundle: DataFusionPlanBundle,
    snapshot: Mapping[str, object],
) -> tuple[str, ...]:
    """Resolve required UDFs from plan bundle against snapshot."""
    ...
```

### Target Files
- Create: `src/datafusion_engine/bundle_extraction.py`
- Modify: `src/datafusion_engine/view_graph_registry.py` (import utilities)
- Modify: `src/datafusion_engine/view_registry_specs.py` (import utilities)
- Modify: `src/datafusion_engine/dataset_registration.py` (import utilities)
- Modify: `src/datafusion_engine/plan_artifact_store.py` (import utilities)

### Deletions
- Delete `_schema_from_df()` from view_graph_registry.py (lines 605-615)
- Delete `_schema_from_df()` from dataset_registration.py (lines 2018-2028)
- Delete `_arrow_schema_from_df()` from view_registry_specs.py (lines 171-181)
- Delete `_lineage_from_bundle()` from view_graph_registry.py (lines 490-497)
- Delete `_lineage_from_bundle()` from plan_artifact_store.py (lines 1142-1148)
- Delete `_required_udfs_from_plan_bundle()` from view_graph_registry.py (lines 455-487)
- Delete `_required_udfs_from_plan_bundle()` from view_registry_specs.py (lines 98-114)

### ast-grep / cq Recipes
```bash
# Find schema extraction patterns
ast-grep --pattern 'def _schema_from_df($$$)' src/datafusion_engine/
ast-grep --pattern 'def _arrow_schema_from_df($$$)' src/datafusion_engine/

# Find lineage extraction
grep -rn "_lineage_from_bundle" src/datafusion_engine/ --include="*.py"
grep -rn "_required_udfs_from_plan_bundle" src/datafusion_engine/ --include="*.py"
```

### Checklist
- [x] Create `bundle_extraction.py` with consolidated utilities.
- [x] Export utilities in `__all__`.
- [x] Update imports in view_graph_registry.py.
- [x] Update imports in view_registry_specs.py.
- [x] Update imports in dataset_registration.py.
- [x] Update imports in plan_artifact_store.py.
- [x] Delete duplicate definitions (~100 lines).

---

## 7. Delta Payload Helpers Extraction

### Goal
Consolidate scattered Delta payload transformation utilities into a dedicated helper module.

### Status
Completed

### Representative Pattern
```python
# BEFORE: Payload helpers scattered across modules
# delta_control_plane.py: _schema_ipc_payload(), _commit_payload(), _cdf_options_payload()
# delta_observability.py: _string_list(), _string_map(), _msgpack_payload()
# delta_provider_contracts.py: _merged_storage_options(), _settings_bool()

# AFTER: Consolidated in delta_payload_helpers.py (or add to delta_protocol.py)
# src/datafusion_engine/delta_payload_helpers.py
def string_list(value: Sequence[str] | None) -> list[str] | None:
    """Normalize sequence to list for payload serialization."""
    return list(value) if value else None

def string_map(value: Mapping[str, str] | None) -> dict[str, str] | None:
    """Normalize mapping to dict for payload serialization."""
    return dict(value) if value else None

def merged_storage_options(
    base: Mapping[str, str] | None,
    override: Mapping[str, str] | None,
) -> dict[str, str]:
    """Merge storage options with override precedence."""
    result: dict[str, str] = {}
    if base:
        result.update(base)
    if override:
        result.update(override)
    return result
```

### Target Files
- Create: `src/datafusion_engine/delta_payload_helpers.py` (or extend `delta_protocol.py`)
- Modify: `src/datafusion_engine/delta_control_plane.py` (import helpers)
- Modify: `src/datafusion_engine/delta_observability.py` (import helpers)
- Modify: `src/datafusion_engine/delta_provider_contracts.py` (import helpers)

### Deletions
- Move and delete payload helpers from delta_control_plane.py
- Move and delete payload helpers from delta_observability.py
- Move and delete payload helpers from delta_provider_contracts.py

### ast-grep / cq Recipes
```bash
# Find payload transformation patterns
grep -rn "_string_list\|_string_map\|_msgpack" src/datafusion_engine/delta*.py
grep -rn "_merged_storage_options\|_settings_bool" src/datafusion_engine/delta*.py
grep -rn "_commit_payload\|_schema_ipc_payload" src/datafusion_engine/delta*.py
```

### Checklist
- [x] Identify all payload helper functions.
- [x] Create or extend module with consolidated helpers.
- [x] Update imports in delta_control_plane.py.
- [x] Update imports in delta_observability.py.
- [x] Update imports in delta_provider_contracts.py.
- [x] Delete duplicate definitions.

---

## 8. UDF Snapshot Helpers Consolidation

### Goal
Consolidate duplicate UDF snapshot extraction helpers into `udf_runtime.py` as the canonical source.

### Status
Completed

### Representative Pattern
```python
# BEFORE: Snapshot helpers duplicated across modules
# udf_catalog.py (lines 674-779): _registry_names(), _registry_parameter_names(), etc.
# udf_runtime.py (lines 178-205): _snapshot_names(), _alias_to_canonical()
# udf_parity.py (lines 208-254): _rust_function_names(), _iter_snapshot_names()

# AFTER: Consolidated in udf_runtime.py
def snapshot_function_names(snapshot: Mapping[str, object]) -> frozenset[str]:
    """Extract function names from UDF snapshot."""
    ...

def snapshot_parameter_names(snapshot: Mapping[str, object], name: str) -> tuple[str, ...]:
    """Extract parameter names for a function from UDF snapshot."""
    ...

def snapshot_alias_mapping(snapshot: Mapping[str, object]) -> dict[str, str]:
    """Build alias-to-canonical name mapping from UDF snapshot."""
    ...
```

### Target Files
- Modify: `src/datafusion_engine/udf_runtime.py` (add consolidated helpers)
- Modify: `src/datafusion_engine/udf_catalog.py` (import from udf_runtime)
- Modify: `src/datafusion_engine/udf_parity.py` (import from udf_runtime)

### Deletions
- Delete `_registry_names()` and related from udf_catalog.py (~100 lines)
- Delete `_rust_function_names()` and related from udf_parity.py (~50 lines)
- Delete `_apply_registry_metadata()` from udf_catalog.py (dead code, lines 847-871)

### ast-grep / cq Recipes
```bash
# Find snapshot extraction patterns
grep -rn "_registry_names\|_snapshot_names\|_rust_function_names" src/datafusion_engine/udf*.py
grep -rn "_alias_to_canonical\|_alias_names" src/datafusion_engine/udf*.py

# Verify _apply_registry_metadata is dead
cq calls _apply_registry_metadata
```

### Checklist
- [x] Identify all snapshot extraction helpers.
- [x] Create unified helpers in udf_runtime.py.
- [x] Update imports in udf_catalog.py.
- [x] Update imports in udf_parity.py.
- [x] Delete `_apply_registry_metadata()` (dead code).
- [x] Delete duplicate definitions.

---

## 9. Arrow Schema Utilities Extraction

### Goal
Consolidate duplicate nested type protocol definitions from `arrow_schema/` submodule.

### Status
Completed

### Representative Pattern
```python
# BEFORE: Type protocols duplicated
# arrow_schema/nested_builders.py (lines 29-42)
class _ListType(Protocol):
    value_field: FieldLike

class _StructType(Protocol):
    def __iter__(self) -> Iterator[FieldLike]: ...

# arrow_schema/metadata.py (lines 92-99) - similar definitions

# AFTER: Consolidated in arrow_schema/_type_protocols.py
# src/datafusion_engine/arrow_schema/_type_protocols.py
class ListTypeProtocol(Protocol):
    """Protocol for Arrow list types."""
    value_field: FieldLike

class StructTypeProtocol(Protocol):
    """Protocol for Arrow struct types."""
    def __iter__(self) -> Iterator[FieldLike]: ...

class MapTypeProtocol(Protocol):
    """Protocol for Arrow map types."""
    key_field: FieldLike
    item_field: FieldLike
    keys_sorted: bool

class UnionTypeProtocol(Protocol):
    """Protocol for Arrow union types."""
    type_codes: Sequence[int]
    mode: str
    def __iter__(self) -> Iterator[FieldLike]: ...
```

### Target Files
- Create: `src/datafusion_engine/arrow_schema/_type_protocols.py`
- Modify: `src/datafusion_engine/arrow_schema/nested_builders.py` (import protocols)
- Modify: `src/datafusion_engine/arrow_schema/metadata.py` (import protocols)

### Deletions
- Delete `_ListType`, `_StructType`, `_UnionType` from nested_builders.py
- Delete `_ListType`, `_MapType`, `_StructType` from metadata.py

### ast-grep / cq Recipes
```bash
# Find type protocol definitions
ast-grep --pattern 'class _ListType(Protocol):' src/datafusion_engine/arrow_schema/
ast-grep --pattern 'class _StructType(Protocol):' src/datafusion_engine/arrow_schema/
ast-grep --pattern 'class _MapType(Protocol):' src/datafusion_engine/arrow_schema/
```

### Checklist
- [x] Create `_type_protocols.py` with all type protocols.
- [x] Update imports in nested_builders.py.
- [x] Update imports in metadata.py.
- [x] Delete duplicate definitions.

---

## 10. Session/Runtime Streamlining

### Goal
Streamline session configuration by consolidating SQL Guard resolution and clarifying session creation paths.

### Status
Completed

### Representative Pattern
```python
# BEFORE: sql_guard.py re-implements option resolution
# sql_guard.py (lines 85-111)
def _resolve_sql_options(
    sql_options: SQLOptions | None,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> SQLOptions:
    if sql_options is not None:
        return sql_options
    if runtime_profile is not None:
        return sql_options_for_profile(runtime_profile)
    return planning_sql_options()

# AFTER: Delegate to sql_options.py
from datafusion_engine.sql_options import (
    sql_options_for_profile,
    planning_sql_options,
)

def _resolve_sql_options(
    sql_options: SQLOptions | None,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> SQLOptions:
    if sql_options is not None:
        return sql_options
    return sql_options_for_profile(runtime_profile)  # Handles None case
```

### Target Files
- Modify: `src/datafusion_engine/sql_guard.py` (simplify resolution)
- Keep: `src/datafusion_engine/sql_options.py` (canonical source)
- Keep: `src/datafusion_engine/session_factory.py` (no changes needed)

### Deletions
- Simplify `_resolve_sql_options()` in sql_guard.py

### ast-grep / cq Recipes
```bash
# Find SQL option resolution patterns
grep -rn "sql_options_for_profile\|planning_sql_options" src/datafusion_engine/
cq calls safe_sql
```

### Checklist
- [x] Simplify sql_guard.py to use sql_options.py consistently.
- [x] Verify safe_sql() callers are unaffected.
- [ ] Run tests for sql_guard functionality.

---

## 11. Trivial Module Consolidation

### Goal
Merge trivial single-purpose modules into their logical parents.

### Status
Partially Completed

### Representative Pattern
```python
# BEFORE: Separate tiny modules
# nested_tables.py (15 lines) - only SimpleViewRef dataclass
# id_utils.py (37 lines) - ID generation utilities
# normalize_ids.py (65 lines) - ID normalization

# AFTER: Merge into parent modules
# SimpleViewRef stays in nested_tables.py (already minimal, used as type)
# id_utils.py content → utils/hashing.py or keep separate
# normalize_ids.py content → merge with id_utils.py
```

### Target Files
- Evaluate: `src/datafusion_engine/id_utils.py`
- Evaluate: `src/datafusion_engine/normalize_ids.py`
- Evaluate: `src/datafusion_engine/table_provider_capsule.py` (33 lines)
- Evaluate: `src/datafusion_engine/extract_template_specs.py` (42 lines)

### Implementation Notes
- Consolidated `extract_template_specs` into `extract_templates` and left a re-export shim for the prior import path.
- Retained `id_utils`, `normalize_ids`, and `table_provider_capsule` as standalone modules due to broad cross-package usage and to avoid introducing new import cycles; revisit after consolidation stabilizes.

### Deletions
- Potential: Merge `normalize_ids.py` into `id_utils.py`
- Potential: Merge `table_provider_capsule.py` into `dataset_resolution.py`

### ast-grep / cq Recipes
```bash
# Check usage of trivial modules
cq imports src/datafusion_engine/id_utils.py
cq imports src/datafusion_engine/normalize_ids.py
cq imports src/datafusion_engine/table_provider_capsule.py
```

### Checklist
- [x] Analyze import patterns for each trivial module.
- [x] Determine if merging improves or harms discoverability.
- [x] Merge modules where appropriate.
- [x] Update all import statements.
- [x] Add backward-compatibility re-exports if needed.

---

## 12. Deferred Cleanup and Verification

### Goal
Final verification pass after all consolidation scopes are complete.

### Status
In Progress

### Representative Pattern
```bash
# Verification commands
uv run ruff check src/datafusion_engine/
uv run pyright src/datafusion_engine/
uv run pytest tests/unit/ -x
uv run pytest tests/ -m "not e2e" --tb=short
```

### Target Files
- All modified files from scopes 1-11

### Deletions
- Remove any remaining backward-compatibility shims after migration period
- Remove deprecated re-exports

### ast-grep / cq Recipes
```bash
# Find any remaining duplicates
ast-grep --pattern 'class $_ArrowFieldSpec(Protocol):' src/
ast-grep --pattern 'def _schema_from_df($$$)' src/

# Verify no broken imports
uv run python -c "import datafusion_engine"

# Check for unused exports
grep -r "__all__" src/datafusion_engine/*.py | head -20
```

### Checklist
- [x] Run `uv run ruff check src/datafusion_engine/`.
- [x] Run `uv run pyright src/datafusion_engine/`.
- [ ] Run `uv run pyrefly check src/datafusion_engine/` (pending resolution of current errors).
- [ ] Run targeted tests for plan execution, SQL guard, and Delta control plane paths.
- [ ] Run full lint suite.
- [ ] Run type checker.
- [ ] Run unit tests.
- [ ] Run integration tests.
- [ ] Verify no circular imports.
- [ ] Review git diff for unintended changes.
- [ ] Update CLAUDE.md if architectural patterns changed.

---

## Appendix A: ast-grep Pattern Reference

### Finding Protocol Duplicates
```bash
# Generic protocol finder
ast-grep --pattern 'class $NAME(Protocol):
    $$$BODY
' src/datafusion_engine/

# Specific protocol
ast-grep --pattern 'class _ArrowFieldSpec(Protocol):' src/
```

### Finding Function Duplicates
```bash
# Find functions by name pattern
ast-grep --pattern 'def _schema_from_df($ARGS):
    $$$BODY
' src/

# Find dataclass definitions
ast-grep --pattern '@dataclass
class $NAME:
    $$$BODY
' src/datafusion_engine/
```

### Limitations
- ast-grep metavariables (`$NAME`) work in pattern positions but not in identifiers
- For identifier searches, use `grep -r` or `cq` instead

---

## Appendix B: cq Query Reference

### Import Tracing
```bash
# Who imports a module
cq imports src/datafusion_engine/schema_policy.py

# Who imports a specific function (via grep fallback)
grep -r "from datafusion_engine.schema_policy import" src/
```

### Call Site Analysis
```bash
# Find call sites for a function
cq calls snapshot_plans
cq calls sql_options_for_profile

# Count call sites
cq calls <function_name> | wc -l
```

### Dead Code Detection
```bash
# Check if function is called anywhere
cq calls <function_name>
# If output is empty or 0, function may be dead
```

---

## Appendix C: Expected Outcomes

### Lines of Code Impact
| Scope | Estimated LOC Removed |
|-------|----------------------|
| 1. Dead __init__.py exports | 20 |
| 2. Dead runtime.py functions | 150 |
| 3. SQL options duplication | 50 |
| 4. Protocol consolidation | 100 |
| 5. Schema utilities | 50 |
| 6. View registry utilities | 100 |
| 7. Delta payload helpers | 80 |
| 8. UDF snapshot helpers | 150 |
| 9. Arrow schema protocols | 40 |
| 10. Session streamlining | 30 |
| 11. Trivial modules | 50 |
| **Total** | **~820 LOC** |

### File Count Impact
- New files created: 3-4 (bundle_extraction.py, schema_spec_protocol.py, etc.)
- Files potentially deleted: 0-2 (after merging trivial modules)
- Net change: +1 to +4 files (more focused modules)

### Architectural Benefits
1. **Single source of truth** for protocols and utilities
2. **Reduced cognitive load** with clearer module responsibilities
3. **Improved testability** with isolated utility functions
4. **Better discoverability** with consolidated exports
5. **Maintained backward compatibility** via re-exports
