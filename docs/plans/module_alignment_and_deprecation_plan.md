# Module Alignment and Deprecation Plan

This document proposes edits to `src/relspec`, `src/cpg`, and `src/schema_spec` to align with the semantic-first architecture and the migration plan in `incremental_normalize_migration_plan.md`.

## Executive Summary

| Module | Status | Edits Needed | Deprecation Candidates |
|--------|--------|--------------|------------------------|
| `src/relspec` | Core scheduling infrastructure | Update imports to semantics | `REL_CALLSITE_QNAME_OUTPUT` constant (**ALREADY REMOVED**) |
| `src/cpg` | Schema definitions | Align relationship contracts | `relationship_builder.py`, `relationship_specs.py` (already deleted); `rel_callsite_qname_v1` contract data (**ALREADY REMOVED**) |
| `src/schema_spec` | Foundational specs | None - remains source of truth | `rel_callsite_qname` (**ALREADY REMOVED**) |

### Key Decision: `rel_callsite_qname_v1` - RESOLVED

**Status: COMPLETED** - `rel_callsite_qname_v1` has been removed from all modules:
- `schema_spec/relationship_specs.py` - Removed from RELATIONSHIP_DATA
- `cpg/relationship_contracts.py` - Removed from STANDARD_RELATIONSHIP_CONTRACTS
- `relspec/view_defs.py` - Removed REL_CALLSITE_QNAME_OUTPUT

The codebase now has exactly **4 relationships** aligned across all modules:
1. `rel_name_symbol_v1` - refs × scip_occurrences
2. `rel_def_symbol_v1` - defs × scip_occurrences
3. `rel_import_symbol_v1` - imports × scip_occurrences
4. `rel_callsite_symbol_v1` - callsites × scip_occurrences

---

## Architectural Principles

### Dependency Direction (Critical)

**schema_spec is foundational** - semantics depends on it, never the reverse:

```
schema_spec (foundational)
    ↓ (consumed by)
semantics (consumes schema_spec, adds semantic rules)
    ↓ (consumed by)
relspec, cpg, datafusion_engine
```

**DO NOT** have `schema_spec` import from `semantics` - this would invert the dependency direction.

### Naming Conventions

1. **Semantic outputs** use `semantics.naming.canonical_output_name()` which adds `_v1` suffix
2. **Non-semantic outputs** (analysis, diagnostic, relation_output) should use **explicit constants** - do not rely on `canonical_output_name()` as it doesn't include these
3. **`relation_output`** is NOT in `semantics.naming` - keep `RELATION_OUTPUT_NAME` explicit in `relspec/view_defs.py`

### Incremental Implementation

Use the **existing** semantic incremental implementation:
- `semantics/incremental/cdf_joins.py` - CDFMergeStrategy, CDFJoinSpec, merge_incremental_results
- `semantics/incremental/cdf_reader.py` (once created) - cursor-aware CDF reading

Do NOT introduce a new merge strategy module - extend the existing `cdf_joins.py`.

---

## Module Analysis

### 1. src/relspec

**Current Role:** Inference-driven scheduling engine with rustworkx graph backend, evidence catalog, and execution plan compilation.

**Key Findings:**
- Core scheduling infrastructure (rustworkx_graph.py, execution_plan.py, rustworkx_schedule.py) is **still essential**
- Evidence catalog (evidence.py) aggregates specs from normalize/incremental/schema_spec - **needs import updates**
- Incremental integration (incremental.py) imports from `src/incremental` - **needs import updates**
- View definitions (view_defs.py) define relation output names - **keep explicit constants**

#### 1.1 Required Edits

**File: `src/relspec/evidence.py`**

```python
# CURRENT (line ~418):
from normalize.registry_runtime import dataset_contract as normalize_dataset_contract

# PROPOSED:
from semantics.catalog.dataset_specs import dataset_spec as semantic_dataset_spec
# Graceful fallback to normalize for backward compatibility during transition
```

**Rationale:** The evidence module uses multi-source spec resolution. After migration, `semantics.catalog.dataset_specs` becomes the canonical source, with normalize as a deprecated fallback.

**File: `src/relspec/incremental.py`**

```python
# CURRENT (line ~5-10):
from incremental.cdf_cursors import CdfCursorStore
from incremental.cdf_filters import CdfFilterPolicy
from incremental.cdf_runtime import read_cdf_changes

# PROPOSED (after Phase 2 of migration plan):
from semantics.incremental.cdf_cursors import CdfCursorStore
from semantics.incremental.cdf_joins import CdfFilterPolicy  # Use existing cdf_joins module
from semantics.incremental.cdf_reader import read_cdf_changes  # Once created
```

**Rationale:** Once incremental infrastructure is migrated to semantics, relspec should import from the canonical location. Note: Use the existing `cdf_joins.py` module for CDF types rather than creating a new `cdf_types.py`.

**File: `src/relspec/view_defs.py`**

```python
# CURRENT - KEEP AS-IS for explicit constants:
REL_NAME_SYMBOL_OUTPUT: Final[str] = "rel_name_symbol_v1"
REL_IMPORT_SYMBOL_OUTPUT: Final[str] = "rel_import_symbol_v1"
REL_DEF_SYMBOL_OUTPUT: Final[str] = "rel_def_symbol_v1"
REL_CALLSITE_SYMBOL_OUTPUT: Final[str] = "rel_callsite_symbol_v1"
RELATION_OUTPUT_NAME: Final[str] = "relation_output_v1"

# DO NOT use canonical_output_name() here:
# - relation_output is NOT in semantics.naming mapping
# - These are explicit constants that should remain explicit
# - canonical_output_name is for semantic normalization outputs only
```

**Rationale:** `canonical_output_name()` doesn't include `relation_output` in its mapping. Keep explicit constants for non-semantic outputs to avoid confusion and ensure correct values.

**File: `src/relspec/inferred_deps.py`**

```python
# CURRENT (lines ~250-270):
def _normalize_dataset_spec(name: str) -> DatasetSpec | None:
    try:
        from normalize.registry_runtime import dataset_spec
        return dataset_spec(name)
    except ImportError:
        return None

# PROPOSED:
def _normalize_dataset_spec(name: str) -> DatasetSpec | None:
    try:
        from semantics.catalog.dataset_specs import dataset_spec
        return dataset_spec(name)
    except ImportError:
        # Fallback to deprecated normalize module during transition
        try:
            from normalize.registry_runtime import dataset_spec
            return dataset_spec(name)
        except ImportError:
            return None
```

**Rationale:** Prioritize semantics catalog, graceful fallback for transition period.

#### 1.2 Deprecation Candidates

**None.** All relspec functionality is core infrastructure for scheduling:
- `rustworkx_graph.py` - Task graph construction
- `execution_plan.py` - Execution plan compilation
- `rustworkx_schedule.py` - Topological scheduling
- `evidence.py` - Evidence catalog (multi-source aggregation)
- `inferred_deps.py` - DataFusion lineage-based dependency inference
- `graph_edge_validation.py` - Column/type validation
- `runtime_artifacts.py` - Runtime artifact tracking

---

### 2. src/cpg

**Current Role:** CPG schema definitions (node kinds, edge kinds, properties) and DataFusion-based CPG output builders.

**Key Findings:**
- `relationship_builder.py` and `relationship_specs.py` are **already deleted** (per git status)
- `relationship_contracts.py` - `rel_callsite_qname_v1` **already removed**
- `view_builders_df.py` imports from `relspec.view_defs` - **keep using relspec constants**
- Schema catalogs (kind_catalog, prop_catalog, node_families) are **core and stable**

#### 2.1 Required Edits

**File: `src/cpg/view_builders_df.py`**

```python
# CURRENT (line ~15):
from relspec.view_defs import RELATION_OUTPUT_NAME

# KEEP AS-IS - prefer the relspec constant which already resolves to correct _v1 names
# DO NOT change to:
#   from semantics.naming import canonical_output_name
#   RELATION_OUTPUT_NAME = canonical_output_name("relation_output")
# Because relation_output is NOT in semantics.naming mapping
```

**Rationale:** The relspec constants already have correct `_v1` names. Using `canonical_output_name("relation_output")` would fail because `relation_output` is not in the semantics naming mapping.

**File: `src/cpg/spec_registry.py`**

```python
# Review ENTITY_FAMILY_SPECS against semantics.spec_registry.SEMANTIC_TABLE_SPECS
# Ensure entity families are aligned with semantic normalization outputs

# CURRENT entity families include:
# - file, ref, import_alias, callsite, def, sym_scope, sym_symbol, py_scope, py_binding...

# SEMANTIC_TABLE_SPECS covers:
# - cst_refs, cst_defs, cst_imports, cst_callsites, cst_call_args, cst_docstrings, cst_decorators

# RECONCILIATION NEEDED:
# 1. Verify entity families map to semantic normalization outputs
# 2. Add any missing entity families for new semantic outputs
# 3. Remove any orphaned entity families no longer produced
```

#### 2.2 Deprecation Candidates

| File | Status | Action |
|------|--------|--------|
| `relationship_builder.py` | **Already Deleted** | N/A |
| `relationship_specs.py` | **Already Deleted** | N/A |
| `view_builders.py` (Ibis-based) | Deprecated | Delete after DataFusion migration complete |

**Note:** The Ibis-based builders in `view_builders.py` (if present) should be removed once `view_builders_df.py` (DataFusion-native) is fully validated.

---

### 3. src/schema_spec

**Current Role:** Foundational schema specification layer providing FieldSpec, TableSchemaSpec, DatasetSpec, ContractSpec, and view spec definitions.

**Key Findings:**
- This module is **foundational** - semantics, normalize, cpg all depend on it
- `relationship_specs.py` now has 4 relationships (aligned with semantics) - `rel_callsite_qname` **already removed**
- `system.py` contains DatasetSpec which is used everywhere - **no changes needed**
- `view_specs.py` is used by semantics catalog - **stable**

#### 3.1 Required Edits

**File: `src/schema_spec/relationship_specs.py`**

**Status: ALREADY ALIGNED** - No changes needed.

The relationship data now matches semantics with 4 relationships:
- `rel_name_symbol_v1`
- `rel_import_symbol_v1`
- `rel_def_symbol_v1`
- `rel_callsite_symbol_v1`

**IMPORTANT:** schema_spec is foundational. Do NOT have schema_spec import from semantics. Instead:
- schema_spec provides the base relationship data
- semantics consumes and extends this data with semantic rules

**File: `src/schema_spec/span_fields.py`**

```python
# CURRENT: Defines span field patterns for multiple prefixes
# Already aligned with semantic normalization - no changes needed

# VERIFY: All span prefixes used by semantics are present:
# "", "call_", "name_", "def_", "stmt_", "alias_", "callee_"
```

#### 3.2 Deprecation Candidates

**None.** All schema_spec functionality is foundational:
- `specs.py` - FieldSpec, TableSchemaSpec, FieldBundle - used by all modules
- `system.py` - DatasetSpec, ContractSpec - core abstractions
- `view_specs.py` - ViewSpec - used by semantics catalog
- `relationship_specs.py` - Source of truth for relationship data
- `nested_types.py` - Type builders - core utility

---

## Reconciliation Tasks

### Task 1: Relationship Specification Alignment - COMPLETED

**Status: RESOLVED** - All modules now aligned with 4 relationships.

`rel_callsite_qname_v1` has been removed from:
- [x] `schema_spec/relationship_specs.py` - Removed from RELATIONSHIP_DATA
- [x] `cpg/relationship_contracts.py` - Removed from STANDARD_RELATIONSHIP_CONTRACTS
- [x] `relspec/view_defs.py` - Removed REL_CALLSITE_QNAME_OUTPUT
- [ ] `incremental/impact.py` - Optional qname impact detection block (may still need cleanup)
- [ ] `tests/unit/cpg/test_relationship_contracts.py` - Qname-specific test cases (may still need cleanup)

### Task 2: Naming Convention Clarity

**Problem:** Some modules might incorrectly try to use `canonical_output_name()` for non-semantic outputs.

**Resolution:**

1. **Semantic normalization outputs** - Use `canonical_output_name()` from `semantics.naming`
2. **Relationship outputs** - Keep explicit constants in `relspec/view_defs.py` (already `_v1`)
3. **Analysis/diagnostic datasets** - Use explicit alias mapping in `semantics.catalog.dataset_specs.ANALYSIS_OUTPUT_ALIASES`, NOT `canonical_output_name()`
4. **`relation_output`** - Keep `RELATION_OUTPUT_NAME = "relation_output_v1"` explicit

**DO NOT** add `relation_output` to `semantics.naming` - it's not a semantic normalization output.

### Task 3: Import Path Migration

**Phase 1 imports to update (after semantics/catalog is complete):**

| Current Import | New Import | Files Affected |
|---------------|------------|----------------|
| `normalize.registry_runtime.dataset_spec` | `semantics.catalog.dataset_specs.dataset_spec` | relspec/evidence.py, relspec/inferred_deps.py |
| `normalize.dataset_rows.DATASET_ROWS` | `semantics.catalog.dataset_rows.get_all_dataset_rows()` | datafusion_engine/views/registry_specs.py |
| `normalize.df_view_builders.VIEW_BUILDERS` | `semantics.catalog.view_builders.view_builders()` | datafusion_engine/views/registry_specs.py |

**Phase 2 imports to update (after semantics/incremental is complete):**

| Current Import | New Import | Files Affected |
|---------------|------------|----------------|
| `incremental.cdf_cursors.CdfCursorStore` | `semantics.incremental.cdf_cursors.CdfCursorStore` | relspec/incremental.py |
| `incremental.cdf_filters.CdfFilterPolicy` | `semantics.incremental.cdf_joins.CdfFilterPolicy` | relspec/incremental.py |
| `incremental.types.IncrementalConfig` | `semantics.incremental.config.IncrementalConfig` | hamilton_pipeline modules |

**Note:** Use the existing `cdf_joins.py` module for CDF types and merge strategies rather than creating new modules.

---

## Dependency Graph After Migration

```
                    ┌─────────────────────────────────────┐
                    │         schema_spec (foundational)   │
                    │  FieldSpec, TableSchemaSpec,        │
                    │  DatasetSpec, ContractSpec,         │
                    │  ViewSpec, relationship_specs       │
                    └───────────────┬─────────────────────┘
                                    │ (consumed by - NEVER reversed)
                    ┌───────────────▼─────────────────────┐
                    │       semantics (consumes specs)     │
                    │  spec_registry, catalog/dataset_*,  │
                    │  incremental/cdf_joins, compiler    │
                    └───────────────┬─────────────────────┘
                                    │ (consumed by)
          ┌─────────────────────────┼─────────────────────────┐
          │                         │                         │
          ▼                         ▼                         ▼
    ┌───────────────┐       ┌───────────────┐       ┌───────────────┐
    │    relspec    │       │      cpg      │       │  datafusion_  │
    │  (scheduling) │       │   (schemas)   │       │    engine     │
    │               │       │               │       │  (execution)  │
    └───────────────┘       └───────────────┘       └───────────────┘
```

**Critical:** The arrow from schema_spec to semantics is ONE-WAY. Do not introduce imports from semantics into schema_spec.

---

## Implementation Phases

### Phase A: Alignment Prep - MOSTLY COMPLETE

1. [x] Audit all relationship definitions across semantics/schema_spec/cpg - **DONE**
2. [x] Remove `rel_callsite_qname` from all modules - **DONE**
3. [ ] Verify `relspec/view_defs.py` uses explicit constants (not canonical_output_name)

### Phase B: Import Path Migration (During Migration Phase 3)

1. [ ] Update `relspec/evidence.py` imports
2. [ ] Update `relspec/inferred_deps.py` imports
3. [ ] Update `relspec/incremental.py` imports - use `semantics.incremental.cdf_joins`
4. [ ] Verify `cpg/view_builders_df.py` uses relspec constants

### Phase C: Verification (After Migration Phase 4)

1. [ ] Verify all tests pass with new import paths
2. [ ] Verify deprecation warnings trigger correctly
3. [ ] Verify no direct imports from normalize/incremental remain in core modules
4. [ ] Verify schema_spec has no imports from semantics

---

## Summary of Changes

### Files to Edit

| File | Change Type | Description |
|------|------------|-------------|
| `relspec/evidence.py` | Import update | Use semantics.catalog.dataset_specs |
| `relspec/inferred_deps.py` | Import update | Use semantics.catalog.dataset_specs |
| `relspec/incremental.py` | Import update | Use semantics.incremental.cdf_joins (existing module) |
| `relspec/view_defs.py` | Verify only | Confirm explicit constants remain, no canonical_output_name |
| `cpg/view_builders_df.py` | Verify only | Confirm uses relspec constants |

### Files Already Cleaned Up

| File | Change | Status |
|------|--------|--------|
| `schema_spec/relationship_specs.py` | Removed rel_callsite_qname | **DONE** |
| `cpg/relationship_contracts.py` | Removed rel_callsite_qname_v1 | **DONE** |
| `relspec/view_defs.py` | Removed REL_CALLSITE_QNAME_OUTPUT | **DONE** |
| `cpg/relationship_builder.py` | Deleted | **DONE** |
| `cpg/relationship_specs.py` | Deleted | **DONE** |

### Files Still Needing Cleanup

| File | Change Needed |
|------|--------------|
| `incremental/impact.py` | Remove qname impact detection block (lines 98-113) |
| `tests/unit/cpg/test_relationship_contracts.py` | Remove qname-specific test cases |

### Modules to Keep Intact

- **relspec/** - All files are core scheduling infrastructure
- **cpg/** - Schema definitions (kind_catalog, prop_catalog, node_families, specs, etc.)
- **schema_spec/** - All foundational specs (source of truth, do not import from semantics)

---

## Open Questions

1. ~~**Is `rel_callsite_qname` actively used?**~~ **RESOLVED: NO** - Already removed.
2. ~~**Should schema_spec derive from semantics?**~~ **RESOLVED: NO** - schema_spec is foundational, semantics consumes it.
3. **Timing of cpg/view_builders.py (Ibis) removal?** Depends on DataFusion migration completeness.
4. **Should `incremental/deltas.py` and `incremental/exports.py` qname_id references be cleaned up?** These appear to be for incremental export tracking, not relationship building. May be separate concern.

---

## Appendix: Investigation Results for `rel_callsite_qname_v1`

**Status: RESOLVED - REMOVED**

The investigation found that `rel_callsite_qname_v1` was designed (contract, spec, constant) but **never implemented** - no builder function existed. It has now been removed from all modules.

### Evidence Collected (Historical):

| Component | Location | Previous Status | Current Status |
|-----------|----------|-----------------|----------------|
| Contract data | `cpg/relationship_contracts.py` | Existed | **REMOVED** |
| Spec data | `schema_spec/relationship_specs.py` | Existed | **REMOVED** |
| View name constant | `relspec/view_defs.py` | Existed | **REMOVED** |
| Builder function | N/A | Never existed | N/A |
| Semantic spec | N/A | Never existed | N/A |

### Why It Was Deprecated:

1. **No builder existed** - Would require significant new work to implement
2. **Semantic pipeline didn't include it** - The go-forward architecture has 4 relationships
3. **No separate qname table** - Would need new extraction/normalization infrastructure
4. **Incremental handled absence gracefully** - Already had `if ... is not None` guard
5. **Symbol-based relationships cover core use cases** - The 4 existing relationships provide CPG edges
