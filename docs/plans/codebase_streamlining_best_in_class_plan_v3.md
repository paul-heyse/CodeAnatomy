# Codebase Streamlining Plan v3 (Programmatic Schema & Relationship Architecture)

## Executive Summary

This plan identifies **17 scope items** (5 prerequisite + 12 main) for transforming the codebase from verbose, declarative specifications toward a **programmatic, inference-driven architecture**. Building on the existing relspec inference patterns (which derive dependencies from DataFusion plans), this plan extends those principles to schemas, relationships, and view definitions.

**Critical Insight:** Deep analysis of `src/schema_spec/` revealed blocking coupling patterns that must be addressed first. The **5 prerequisite items (#0a-#0e)** unlock the main implementation by:
- Deleting 185 lines of vestigial code (zero-risk)
- Extracting behavior from specs → separate policy appliers
- Replacing `@cache` singletons with injectable catalogs
- Collapsing `relationship_specs.py` from 397 → 130 lines (67% reduction)
- Standardizing SessionContext schema behavior to make inference deterministic

**Key findings from analysis:**
- **3,256** lines of static view select expressions in `_VIEW_SELECT_EXPRS` that could be derived from schema introspection
- **5** parallel relationship builder functions that are **95% identical** copy-paste code
- **50+** repetitions of file identity fields (`file_id`, `path`, `file_sha256`) across schemas
- **2,000+** lines of hardcoded schema definitions in extraction registry
- **280** lines of relationship contract specs that follow **identical patterns** (only entity key varies)
- **35+** EntityFamilySpec instances with **90% identical** column specifications
- **7** extractor templates with manually specified metadata that could be derived
- Schema derivation is currently sensitive to SessionContext defaults (view types, string mapping, timezone)

**Core architectural shift:**
```
Current:  Schemas are declared artifacts that drive operations
Target:   Schemas are derived artifacts that emerge from relationships and plans
```

**Estimated impact:**
- 4,500-6,000 lines of code reduction (specification → derivation)
- Schemas become queryable and computable, not literal definitions
- Adding new relationships/extractors becomes declarative (5 lines instead of 150)
- Compile-time schema validation through plan introspection
- Single source of truth: evidence metadata + view definitions → everything else derived

---

## Design-Phase Principles

1. **Schemas are derived, not declared**: DataFusion plans contain all information needed to derive output schemas
2. **Evidence metadata drives semantics**: Coordinate systems, evidence families, and bundles determine join keys and normalization rules
3. **Only strict boundaries are explicit**: Final CPG output schema, relationship edge kinds, version numbers
4. **Graceful degradation preserves determinism**: Missing specs → correct-schema empty outputs; missing types → string fallback
5. **Fingerprinting enables caching**: `plan_fingerprint` → schema derivation caching; `schema_fingerprint` → evolution tracking
6. **Existing inference patterns extend**: relspec's dependency inference pattern directly maps to schema inference
7. **SessionContext is the schema contract surface**: schema derivation must flow from a hardened, deterministic SessionContext (information_schema on, view-type behavior pinned, timezone pinned)
8. **Prefer DataFusion/Delta-native capabilities**: use built-in schema computation and Delta’s transactional metadata before inventing custom schema logic

---

## Table of Contents

### Prerequisite: Schema Spec Decoupling (Unlocks #1-#12)
- [0a. Delete Vestigial Schema Spec Modules](#0a-delete-vestigial-schema-spec-modules)
- [0b. Extract Policy Behaviors from Spec Classes](#0b-extract-policy-behaviors-from-spec-classes)
- [0c. Replace @cache Singletons with Catalog Lookups](#0c-replace-cache-singletons-with-catalog-lookups)
- [0d. Collapse relationship_specs.py to Data-Driven Generation](#0d-collapse-relationship_specspy-to-data-driven-generation)
- [0e. SessionContext Schema Hardening (Baseline Config)](#0e-sessioncontext-schema-hardening-baseline-config)

### Main Implementation
1. [Relationship Spec Declarative DSL](#1-relationship-spec-declarative-dsl)
2. [Schema Inference from DataFusion Plans](#2-schema-inference-from-datafusion-plans)
3. [File Identity Canonical Type](#3-file-identity-canonical-type)
4. [View Builder DSL](#4-view-builder-dsl)
4b. [DataFusion DataFrame View Registration (Spec-Driven)](#4b-datafusion-dataframe-view-registration-spec-driven)
5. [Node Family Spec Defaults](#5-node-family-spec-defaults)
6. [Extraction Row Builder](#6-extraction-row-builder)
7. [Evidence Metadata Normalization](#7-evidence-metadata-normalization)
8. [Relationship Contract Spec Generator](#8-relationship-contract-spec-generator)
9. [Contract Auto-Population from Catalog](#9-contract-auto-population-from-catalog)
10. [Nested Type Builder Framework](#10-nested-type-builder-framework)
11. [Span Field Templating](#11-span-field-templating)
12. [Extraction Schema Derivation](#12-extraction-schema-derivation)

---

## Implementation Status Snapshot (2026-01-30)

| Scope | Status | Evidence / Notes |
|-------|--------|------------------|
| #0a Vestigial Deletion | Not complete | `src/schema_spec/contract_row.py`, `src/schema_spec/literals.py`, `src/schema_spec/bundles.py` still exist and are referenced. |
| #0b Policy Extraction | Not complete | `src/relspec/policies/` is empty; behavior remains in `schema_spec/system.py` + `schema_spec/relationship_specs.py`. |
| #0c @cache Replacement | Not complete | `@cache` still present in `schema_spec/relationship_specs.py`; no `schema_spec/catalog.py`. |
| #0d relationship_specs Collapse | Complete | `src/schema_spec/relationship_specs.py` is fully data-driven (RelationshipData registry + generators). |
| #0e SessionContext Hardening | Partial | `src/datafusion_engine/session/schema_profile.py` exists but is not wired into runtime/factory creation. |
| #1 Relationship Spec DSL | Complete | `src/cpg/relationship_specs.py` + `src/cpg/relationship_builder.py`; used by `src/relspec/relationship_datafusion.py`. |
| #2 Schema Inference | Partial | `src/datafusion_engine/schema/inference.py` + `src/extract/schema_derivation.py`; not wired into relspec compile/execution. |
| #3 File Identity | Partial | Canonical fields in `schema_spec/file_identity.py`; registry uses shared constants, but nested struct usage not migrated. |
| #4 View Builder DSL | Partial | `view_spec.py`, `view_specs_catalog.py`, `views/dsl.py` exist; registry still uses manual `_VIEW_SELECT_EXPRS`. |
| #5 Node Family Defaults | Complete | Defaults implemented in `src/cpg/spec_registry.py`. |
| #6 Extraction Row Builder | Partial | `src/extract/row_builder.py` exists; extractors do not use it yet. |
| #7 Evidence Normalization | Partial | `schema_spec/evidence_metadata.py` + `normalize/evidence_specs.py`; join_keys remain manual. |
| #8 Relationship Contract Gen | Partial | `src/cpg/relationship_contracts.py` exists but not wired; `schema_spec/relationship_specs.py` also generates contracts. |
| #9 Contract Auto-Population | Not started | No `catalog_contracts.py`; contracts still manual. |
| #10 Nested Type Builder | Partial | `src/schema_spec/nested_types.py` builder exists; registry still defines nested structs manually. |
| #11 Span Field Templating | Partial | `src/schema_spec/span_fields.py` covers byte spans; full span metadata templating not implemented. |
| #12 Extraction Schema Derivation | Partial | `src/extract/schema_derivation.py` exists; registry still uses hardcoded schemas. |

### Deviations from the original plan (observed)
- Relationship DataFusion entrypoints live in `src/relspec/relationship_datafusion.py` (not `src/cpg/relationship_datafusion.py`).
- Schema inference lives in `src/datafusion_engine/schema/inference.py` and `src/extract/schema_derivation.py` (not `src/relspec/schema_inference.py`).
- View DSL is split across `views/view_spec.py`, `views/view_specs_catalog.py`, and `views/dsl.py` and is **not** wired into `views/registry.py`.
- Span templating exists as `schema_spec/span_fields.py` (byte-span helpers), not the full `span_templates.py` variant.
- Extraction schema derivation is in `src/extract/schema_derivation.py`, not `src/datafusion_engine/schema/schema_derivation.py`.
- Extraction row builder exists as `ExtractionRowBuilder` in `src/extract/row_builder.py` (identity + span helpers), not schema-validation-driven; extractors still build rows manually.

---

## Prerequisite: Schema Spec Decoupling

**Context:** Deep analysis of `src/schema_spec/` revealed critical coupling patterns that block the main implementation items. These 5 prerequisite items must be completed first to unlock the full programmatic architecture transformation.

**Key Findings from Analysis:**
- **350-450 lines** of deletable vestigial code (11-14% of module)
- **47-83% code duplication** in relationship_specs.py
- **40-50% redundancy** with DataFusion/Delta native capabilities
- Module-level `@cache` decorators creating global singletons
- Behavior embedded in spec methods that should be separate services
- Schema inference results drift without a hardened SessionContext configuration

---

## 0a. Delete Vestigial Schema Spec Modules

**Status (2026-01-30): Not complete.** `contract_row.py`, `literals.py`, and `bundles.py` still exist and have active references.

### Problem Statement
Three modules in `src/schema_spec/` are vestigial artifacts from pre-DataFusion architecture. They contain dead code, duplicate functionality, or trivial wrappers that add complexity without value.

### Current State Analysis

**`contract_row.py` (~60 lines) - Duplicate of ContractSpec:**
```python
# src/schema_spec/contract_row.py
@dataclass(frozen=True)
class ContractRowSpec:
    """Specifies how a single row should be validated."""
    # ... duplicates ContractSpec functionality
```
- 0 unique call sites that aren't also served by `ContractSpec`
- Exists from legacy validation approach

**`literals.py` (~45 lines) - Only 4 usages:**
```python
# src/schema_spec/literals.py
RELATIONSHIP_SCHEMA_VERSION = "v1"
NODE_SCHEMA_VERSION = "v1"
EDGE_SCHEMA_VERSION = "v1"
# ... more constants that can be inline
```
- All 4 usages replaceable with inline constants or config
- Creates unnecessary import dependency

**`bundles.py` (~80 lines) - Vestigial wrapper:**
```python
# src/schema_spec/bundles.py
def file_identity_bundle() -> tuple[pa.Field, ...]:
    # ... thin wrapper with no unique logic
```
- Logic should move to a dedicated `src/schema_spec/file_identity.py` module or be inlined
- Current abstraction adds indirection without benefit

### Target Implementation

**Step 1: Identify and migrate any used functionality**
```python
# Migrate RELATIONSHIP_SCHEMA_VERSION etc. to src/cpg/constants.py
# or inline at usage sites

# Migrate any file identity logic worth keeping to schema_spec/file_identity.py
```

**Step 2: Delete the files**
```bash
rm src/schema_spec/contract_row.py
rm src/schema_spec/literals.py
rm src/schema_spec/bundles.py
```

**Step 3: Update imports**
```python
# Before:
from schema_spec.literals import RELATIONSHIP_SCHEMA_VERSION

# After (inline):
RELATIONSHIP_SCHEMA_VERSION = "v1"

# Or (centralized):
from cpg.constants import RELATIONSHIP_SCHEMA_VERSION
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Delete | `src/schema_spec/contract_row.py` | Duplicate of ContractSpec |
| Delete | `src/schema_spec/literals.py` | Replace with inline/config |
| Delete | `src/schema_spec/bundles.py` | Migrate file identity helpers to file_identity.py |
| Modify | `src/schema_spec/__init__.py` | Remove exports |
| Modify | `src/cpg/constants.py` | Add migrated constants if needed |

### ast-grep Recipes

**Discovery - Find all imports from vestigial modules:**
```bash
# Find imports from contract_row
ast-grep run -p 'from schema_spec.contract_row import $$$' -l python src/
ast-grep run -p 'from schema_spec import contract_row' -l python src/

# Find imports from literals
ast-grep run -p 'from schema_spec.literals import $$$' -l python src/

# Find imports from bundles
ast-grep run -p 'from schema_spec.bundles import $$$' -l python src/

# Count total usages
ast-grep run -p 'RELATIONSHIP_SCHEMA_VERSION' -l python src/ --json=stream | wc -l
ast-grep run -p 'ContractRowSpec' -l python src/ --json=stream | wc -l
```

**Verification - Ensure clean deletion:**
```bash
# After deletion, verify no broken imports
uv run python -c "import src.schema_spec"

# Verify no references remain
ast-grep run -p 'contract_row' -l python src/
ast-grep run -p 'from schema_spec.literals' -l python src/
ast-grep run -p 'from schema_spec.bundles' -l python src/
```

### cq Recipes

**Discovery:**
```bash
# Find all call sites for vestigial modules
./scripts/cq calls ContractRowSpec --root .
./scripts/cq calls file_identity_bundle --root .
./scripts/cq calls RELATIONSHIP_SCHEMA_VERSION --root .

# Check import dependencies
./scripts/cq imports --module src.schema_spec.contract_row --root .
./scripts/cq imports --module src.schema_spec.literals --root .
./scripts/cq imports --module src.schema_spec.bundles --root .
```

### Implementation Checklist
- [ ] Audit `contract_row.py` - confirm 0 unique usages
- [ ] Audit `literals.py` - identify all 4 usage sites
- [ ] Audit `bundles.py` - identify useful logic to migrate
- [ ] Migrate constants to `src/cpg/constants.py` or inline
- [ ] Migrate useful file identity logic to `src/schema_spec/file_identity.py`
- [ ] Update all import statements
- [ ] Delete `contract_row.py`
- [ ] Delete `literals.py`
- [ ] Delete `bundles.py`
- [ ] Update `__init__.py` exports
- [ ] Run full test suite
- [ ] Verify no import errors

### Decommissioning List
- Delete `src/schema_spec/contract_row.py` (~60 lines)
- Delete `src/schema_spec/literals.py` (~45 lines)
- Delete `src/schema_spec/bundles.py` (~80 lines)
- **Estimated reduction: 185 lines (immediate, zero-risk)**

---

## 0b. Extract Policy Behaviors from Spec Classes

**Status (2026-01-30): Not complete.** `src/relspec/policies/` is empty and behavior remains embedded in spec modules.

### Problem Statement
Spec classes in `src/schema_spec/` contain operational behavior (methods that transform data) mixed with declarative specification. This coupling makes specs non-serializable, harder to test, and tightly couples data definitions to execution logic.

### Current State (Behavior in Specs)
```python
# src/schema_spec/relationship_specs.py
@dataclass
class RelationshipTableSpec:
    table_name: str
    schema: pa.Schema
    dedup_keys: tuple[str, ...]
    sort_keys: tuple[str, ...]

    def apply_dedup_policy(self, df: DataFrame) -> DataFrame:
        """BEHAVIOR: Applies deduplication to DataFrame."""
        # ... 20+ lines of DataFusion operations
        return df.distinct()

    def apply_sort_policy(self, df: DataFrame) -> DataFrame:
        """BEHAVIOR: Applies sorting to DataFrame."""
        # ... 15+ lines of sort configuration
        return df.sort(...)

    def validate_schema(self, table: pa.Table) -> list[str]:
        """BEHAVIOR: Validates table against spec."""
        # ... 30+ lines of validation logic

# src/schema_spec/system.py
@dataclass
class DatasetSpec:
    name: str
    schema: pa.Schema

    def unify_tables(self, tables: list[pa.Table]) -> pa.Table:
        """BEHAVIOR: Dead code - 0 call sites."""
        # ... unused table merging logic
```

### Target Implementation
```python
# src/relspec/policies/dedup.py
from __future__ import annotations

from dataclasses import dataclass
from contextlib import suppress
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import DataFrame

from schema_spec.specs import DedupeSpec


@dataclass(frozen=True)
class DedupPolicyApplier:
    """Applies deduplication policy to DataFrames.

    Separates dedup behavior from spec definition, enabling:
    - Specs to be pure data (serializable, testable)
    - Behavior to be independently testable
    - Different implementations for different contexts
    """

    spec: DedupeSpec

    def apply(self, df: DataFrame) -> DataFrame:
        """Apply deduplication based on spec configuration."""
        from datafusion import col
        import datafusion.functions as f

        if self.spec.strategy == "KEEP_FIRST_AFTER_SORT":
            # Sort by tie-breakers
            sort_exprs = [
                col(k.column).sort(ascending=(k.order == "ascending"))
                for k in self.spec.tie_breakers
            ]
            sorted_df = df.sort(*sort_exprs)

            # Dedupe by keys
            return sorted_df.distinct_on(*[col(k) for k in self.spec.keys])

        return df.distinct()


# src/relspec/policies/sort.py
@dataclass(frozen=True)
class SortPolicyApplier:
    """Applies canonical sort policy to DataFrames."""

    sort_keys: tuple[SortKeySpec, ...]

    def apply(self, df: DataFrame) -> DataFrame:
        """Apply canonical sort based on spec configuration."""
        from datafusion import col

        sort_exprs = [
            col(k.column).sort(ascending=(k.order == "ascending"))
            for k in self.sort_keys
        ]
        return df.sort(*sort_exprs)


# src/relspec/policies/__init__.py
"""Policy appliers - separated from specs for clean architecture."""

from relspec.policies.dedup import DedupPolicyApplier
from relspec.policies.sort import SortPolicyApplier

__all__ = ["DedupPolicyApplier", "SortPolicyApplier"]
```

**Refactored Spec (Pure Data):**
```python
# src/schema_spec/relationship_specs.py (refactored)
@dataclass(frozen=True)
class RelationshipTableSpec:
    """Pure specification - no behavior methods."""

    table_name: str
    schema: pa.Schema
    dedup_keys: tuple[str, ...]
    sort_keys: tuple[str, ...]
    tie_breakers: tuple[SortKeySpec, ...] = ()

    # NO apply_* methods - those are now in policies/
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/relspec/policies/__init__.py` | Policy module init |
| Create | `src/relspec/policies/dedup.py` | DedupPolicyApplier |
| Create | `src/relspec/policies/sort.py` | SortPolicyApplier |
| Create | `src/relspec/policies/validation.py` | SchemaValidator |
| Modify | `src/schema_spec/relationship_specs.py` | Remove behavior methods |
| Modify | `src/schema_spec/system.py` | Remove behavior methods |
| Delete | (method) | `DatasetSpec.unify_tables()` - dead code |

### ast-grep Recipes

**Discovery - Find behavior methods in spec classes:**
```bash
# Find apply_* methods in dataclasses
ast-grep run -p 'def apply_$NAME(self, $PARAMS) -> $RET:
    $$$BODY' -l python src/schema_spec/

# Find validate_* methods in dataclasses
ast-grep run -p 'def validate_$NAME(self, $PARAMS) -> $RET:
    $$$BODY' -l python src/schema_spec/

# Find methods that import datafusion (behavior indicator)
ast-grep run -p 'from datafusion import' -l python src/schema_spec/

# Find dead code - unify_tables with 0 call sites
ast-grep run -p 'def unify_tables(self' -l python src/schema_spec/
```

**Verification:**
```bash
# After refactor, verify no behavior methods remain in specs
ast-grep run -p 'def apply_$NAME(self' -l python src/schema_spec/
# Should find 0

# Verify policy appliers exist
ast-grep run -p 'class $NAME(PolicyApplier)' -l python src/relspec/policies/

# Verify specs are frozen dataclasses (pure data)
ast-grep run -p '@dataclass(frozen=True)
class $NAME:' -l python src/schema_spec/
```

### cq Recipes

**Discovery:**
```bash
# Find all call sites for behavior methods
./scripts/cq calls apply_dedup_policy --root .
./scripts/cq calls apply_sort_policy --root .
./scripts/cq calls unify_tables --root .

# Check for side effects in spec modules
./scripts/cq side-effects --root src/schema_spec/relationship_specs.py
./scripts/cq side-effects --root src/schema_spec/system.py
```

**Verification:**
```bash
# Verify policy appliers are used
./scripts/cq calls DedupPolicyApplier --root .
./scripts/cq calls SortPolicyApplier --root .

# Verify no side effects in refactored specs
./scripts/cq side-effects --root src/schema_spec/relationship_specs.py
```

### Implementation Checklist
- [ ] Create `src/relspec/policies/` directory
- [ ] Implement `DedupPolicyApplier` class
- [ ] Implement `SortPolicyApplier` class
- [ ] Implement `SchemaValidator` class (if needed)
- [ ] Identify all `apply_*` methods in spec classes
- [ ] Migrate each method to corresponding policy applier
- [ ] Update call sites to use policy appliers
- [ ] Remove behavior methods from spec classes
- [ ] Delete `DatasetSpec.unify_tables()` (0 call sites)
- [ ] Add `frozen=True` to all spec dataclasses
- [ ] Add unit tests for policy appliers
- [ ] Verify all tests pass

### Decommissioning List
- Remove `RelationshipTableSpec.apply_dedup_policy()` (~25 lines)
- Remove `RelationshipTableSpec.apply_sort_policy()` (~20 lines)
- Remove `RelationshipTableSpec.validate_schema()` (~30 lines)
- Remove `DatasetSpec.unify_tables()` (~40 lines, dead code)
- **Estimated movement: 115 lines from specs → policies (cleaner architecture)**

---

## 0c. Replace @cache Singletons with Catalog Lookups

**Status (2026-01-30): Not complete.** `@cache` is still present and no `schema_spec/catalog.py` exists.

### Problem Statement
Module-level `@cache` decorators in `src/schema_spec/system.py` create global singletons that materialize at import time. This pattern:
- Makes testing difficult (can't inject different configurations)
- Creates hidden coupling between modules
- Forces eager materialization of potentially expensive specs
- Prevents per-session or per-context spec variations

### Current State (Global Singletons)
```python
# src/schema_spec/system.py
from functools import cache

@cache
def get_system_spec() -> SystemSpec:
    """Returns THE global system spec - cannot be overridden."""
    return SystemSpec(
        datasets=_build_all_datasets(),      # Materializes at first call
        relationships=_build_all_rels(),     # Cannot vary per context
        nodes=_build_all_nodes(),            # Cached forever
    )

@cache
def get_relationship_specs() -> Mapping[str, RelationshipSpec]:
    """Global relationship specs - import-time coupling."""
    return {
        name: _build_rel_spec(name)
        for name in RELATIONSHIP_NAMES
    }

@cache
def get_node_specs() -> Mapping[str, NodeSpec]:
    """Global node specs - forces single configuration."""
    return {...}

# Usage throughout codebase:
from schema_spec.system import get_system_spec
spec = get_system_spec()  # Always same singleton
```

### Target Implementation
```python
# src/schema_spec/catalog.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable
from collections.abc import Mapping

if TYPE_CHECKING:
    from schema_spec.specs import SystemSpec, RelationshipSpec, NodeSpec


@dataclass
class SpecCatalog:
    """Catalog for spec lookups - enables per-context configuration.

    Replaces global @cache singletons with explicit catalog that can be:
    - Injected for testing
    - Configured per session
    - Lazily populated on demand
    """

    _system_spec: SystemSpec | None = field(default=None, repr=False)
    _relationship_specs: dict[str, RelationshipSpec] = field(default_factory=dict)
    _node_specs: dict[str, NodeSpec] = field(default_factory=dict)

    # Lazy builders (called on first access)
    _system_spec_builder: Callable[[], SystemSpec] | None = None
    _relationship_spec_builder: Callable[[str], RelationshipSpec] | None = None
    _node_spec_builder: Callable[[str], NodeSpec] | None = None

    def get_system_spec(self) -> SystemSpec:
        """Get system spec, building lazily if needed."""
        if self._system_spec is None:
            if self._system_spec_builder is None:
                from schema_spec.builders import build_default_system_spec
                self._system_spec_builder = build_default_system_spec
            self._system_spec = self._system_spec_builder()
        return self._system_spec

    def get_relationship_spec(self, name: str) -> RelationshipSpec:
        """Get relationship spec by name, building lazily if needed."""
        if name not in self._relationship_specs:
            if self._relationship_spec_builder is None:
                from schema_spec.builders import build_relationship_spec
                self._relationship_spec_builder = build_relationship_spec
            self._relationship_specs[name] = self._relationship_spec_builder(name)
        return self._relationship_specs[name]

    def get_node_spec(self, name: str) -> NodeSpec:
        """Get node spec by name, building lazily if needed."""
        if name not in self._node_specs:
            if self._node_spec_builder is None:
                from schema_spec.builders import build_node_spec
                self._node_spec_builder = build_node_spec
            self._node_specs[name] = self._node_spec_builder(name)
        return self._node_specs[name]

    @classmethod
    def for_testing(
        cls,
        *,
        system_spec: SystemSpec | None = None,
        relationship_specs: Mapping[str, RelationshipSpec] | None = None,
    ) -> SpecCatalog:
        """Create a catalog configured for testing."""
        catalog = cls()
        catalog._system_spec = system_spec
        if relationship_specs:
            catalog._relationship_specs.update(relationship_specs)
        return catalog


# Default catalog instance (replaces @cache functions)
_default_catalog: SpecCatalog | None = None


def get_spec_catalog() -> SpecCatalog:
    """Get the default spec catalog."""
    global _default_catalog
    if _default_catalog is None:
        _default_catalog = SpecCatalog()
    return _default_catalog


def set_spec_catalog(catalog: SpecCatalog) -> None:
    """Set custom spec catalog (for testing/configuration)."""
    global _default_catalog
    _default_catalog = catalog


def reset_spec_catalog() -> None:
    """Reset to default catalog (for testing teardown)."""
    global _default_catalog
    _default_catalog = None


# Convenience functions (drop-in replacements for @cache functions)
def get_system_spec() -> SystemSpec:
    """Get system spec from catalog."""
    return get_spec_catalog().get_system_spec()


def get_relationship_spec(name: str) -> RelationshipSpec:
    """Get relationship spec from catalog."""
    return get_spec_catalog().get_relationship_spec(name)
```

**Test Usage:**
```python
# tests/unit/test_with_custom_catalog.py
from schema_spec.catalog import SpecCatalog, set_spec_catalog, reset_spec_catalog

def test_custom_relationship_spec():
    """Test with custom catalog - no global state pollution."""
    custom_catalog = SpecCatalog.for_testing(
        relationship_specs={"test_rel": MockRelationshipSpec()}
    )
    set_spec_catalog(custom_catalog)
    try:
        spec = get_relationship_spec("test_rel")
        assert spec == MockRelationshipSpec()
    finally:
        reset_spec_catalog()
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/schema_spec/catalog.py` | SpecCatalog + catalog management |
| Create | `src/schema_spec/builders.py` | Lazy builder functions |
| Modify | `src/schema_spec/system.py` | Remove @cache, use catalog |
| Modify | All callers | Update to use catalog or convenience functions |

### ast-grep Recipes

**Discovery - Find all @cache usages:**
```bash
# Find @cache decorated functions
ast-grep run -p '@cache
def $NAME($PARAMS) -> $RET:
    $$$BODY' -l python src/schema_spec/

# Find functools.cache imports
ast-grep run -p 'from functools import cache' -l python src/schema_spec/
ast-grep run -p 'from functools import $$$, cache$$$' -l python src/schema_spec/

# Find direct get_system_spec calls
ast-grep run -p 'get_system_spec()' -l python src/

# Count singleton usages
ast-grep run -p 'get_system_spec()' -l python src/ --json=stream | wc -l
ast-grep run -p 'get_relationship_specs()' -l python src/ --json=stream | wc -l
```

**Verification:**
```bash
# After refactor, verify no @cache in schema_spec
ast-grep run -p '@cache' -l python src/schema_spec/
# Should find 0

# Verify catalog usage
ast-grep run -p 'get_spec_catalog()' -l python src/
ast-grep run -p 'SpecCatalog(' -l python src/

# Verify testing helper usage
ast-grep run -p 'SpecCatalog.for_testing(' -l python tests/
```

### cq Recipes

**Discovery:**
```bash
# Find all callers of singleton functions
./scripts/cq calls get_system_spec --root .
./scripts/cq calls get_relationship_specs --root .
./scripts/cq calls get_node_specs --root .

# Analyze import patterns
./scripts/cq imports --module src.schema_spec.system --root .
```

**Verification:**
```bash
# Verify catalog is properly used
./scripts/cq calls get_spec_catalog --root .

# Check for any remaining @cache patterns
./scripts/cq side-effects --root src/schema_spec/system.py
```

### Implementation Checklist
- [ ] Create `src/schema_spec/catalog.py` with `SpecCatalog` class
- [ ] Implement lazy builders in `src/schema_spec/builders.py`
- [ ] Add `get_spec_catalog()` and `set_spec_catalog()` functions
- [ ] Add `SpecCatalog.for_testing()` factory method
- [ ] Replace `@cache` functions with catalog lookups
- [ ] Update all call sites (maintain backward compat via convenience functions)
- [ ] Remove `@cache` decorators from system.py
- [ ] Add pytest fixtures for catalog injection
- [ ] Update existing tests to use catalog
- [ ] Verify no import-time side effects remain

### Decommissioning List
- Remove `@cache` decorator from `get_system_spec()`
- Remove `@cache` decorator from `get_relationship_specs()`
- Remove `@cache` decorator from `get_node_specs()`
- Remove `@cache` decorator from ~5 other cached functions
- **Estimated change: 8 @cache decorators → 1 SpecCatalog class (cleaner architecture)**

---

## 0d. Collapse relationship_specs.py to Data-Driven Generation

**Status (2026-01-30): Complete.** `schema_spec/relationship_specs.py` is fully data-driven with `RelationshipData` and generator helpers.

### Problem Statement
`src/schema_spec/relationship_specs.py` contains **397 lines** with **47-83% code duplication** across 8 relationship builder functions. Analysis shows only **5 unique data points** per relationship are truly necessary - everything else follows patterns that can be generated.

### Current State (397 lines, 47-83% duplicate)
```python
# src/schema_spec/relationship_specs.py
def _build_rel_name_symbol_spec() -> RelationshipTableSpec:
    """Build spec for name→symbol relationship."""
    return RelationshipTableSpec(
        table_name="rel_name_symbol_v1",
        schema=pa.schema([
            pa.field("ref_id", pa.string(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("symbol_roles", pa.int32(), nullable=True),
            pa.field("path", pa.string(), nullable=False),
            pa.field("edge_owner_file_id", pa.string(), nullable=True),
            pa.field("bstart", pa.int64(), nullable=False),
            pa.field("bend", pa.int64(), nullable=False),
            pa.field("resolution_method", pa.string(), nullable=False),
            pa.field("confidence", pa.float64(), nullable=False),
            pa.field("score", pa.float64(), nullable=False),
            pa.field("task_name", pa.string(), nullable=False),
            pa.field("task_priority", pa.int32(), nullable=False),
            pa.field("origin", pa.string(), nullable=True),
        ]),
        dedup_keys=("ref_id", "symbol", "path", "bstart", "bend"),
        sort_keys=("path", "bstart", "ref_id"),
        tie_breakers=(
            SortKeySpec(column="score", order="descending"),
            SortKeySpec(column="confidence", order="descending"),
            SortKeySpec(column="task_priority", order="ascending"),
        ),
    )

def _build_rel_import_symbol_spec() -> RelationshipTableSpec:
    """Build spec for import→symbol relationship."""
    return RelationshipTableSpec(
        table_name="rel_import_symbol_v1",
        schema=pa.schema([
            pa.field("import_alias_id", pa.string(), nullable=False),  # Only real difference!
            pa.field("symbol", pa.string(), nullable=False),           # Same
            pa.field("symbol_roles", pa.int32(), nullable=True),       # Same
            pa.field("path", pa.string(), nullable=False),             # Same
            # ... 9 more identical fields
        ]),
        dedup_keys=("import_alias_id", "symbol", "path", "bstart", "bend"),  # Same pattern
        sort_keys=("path", "bstart", "import_alias_id"),                      # Same pattern
        tie_breakers=(...),  # Identical to above!
    )

# ... 6 more nearly identical functions
```

### Target Implementation
```python
# src/schema_spec/relationship_specs.py (refactored to ~130 lines)
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
import pyarrow as pa

if TYPE_CHECKING:
    from collections.abc import Sequence


# =============================================================================
# DATA: The only unique information per relationship (5 fields each)
# =============================================================================

@dataclass(frozen=True)
class RelationshipData:
    """Minimal data defining a relationship - everything else is derived."""

    name: str                        # "rel_name_symbol", "rel_import_symbol", etc.
    entity_id_col: str               # "ref_id", "import_alias_id", etc.
    entity_id_fallback: str | None   # For coalesce patterns
    origin: str                      # "cst", "scip", etc.
    extra_dedup_keys: tuple[str, ...] = ()  # Beyond standard pattern


# Registry: 8 relationships × 5 data points = 40 lines (was 397)
RELATIONSHIP_DATA: tuple[RelationshipData, ...] = (
    RelationshipData(
        name="rel_name_symbol",
        entity_id_col="ref_id",
        entity_id_fallback=None,
        origin="cst",
    ),
    RelationshipData(
        name="rel_import_symbol",
        entity_id_col="import_alias_id",
        entity_id_fallback="import_id",
        origin="cst",
    ),
    RelationshipData(
        name="rel_def_symbol",
        entity_id_col="def_id",
        entity_id_fallback=None,
        origin="cst",
    ),
    RelationshipData(
        name="rel_callsite_symbol",
        entity_id_col="call_id",
        entity_id_fallback=None,
        origin="cst",
    ),
    RelationshipData(
        name="rel_callsite_qname",
        entity_id_col="call_id",
        entity_id_fallback=None,
        origin="cst",
        extra_dedup_keys=("qname_id",),
    ),
    RelationshipData(
        name="rel_scip_occurrence",
        entity_id_col="occurrence_id",
        entity_id_fallback=None,
        origin="scip",
    ),
    RelationshipData(
        name="rel_scip_reference",
        entity_id_col="reference_id",
        entity_id_fallback=None,
        origin="scip",
    ),
    RelationshipData(
        name="rel_scip_definition",
        entity_id_col="definition_id",
        entity_id_fallback=None,
        origin="scip",
    ),
)


# =============================================================================
# GENERATORS: Derive everything from data
# =============================================================================

# Standard schema fields (same for ALL relationships)
_STANDARD_RELATIONSHIP_FIELDS: tuple[pa.Field, ...] = (
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("symbol_roles", pa.int32(), nullable=True),
    pa.field("path", pa.string(), nullable=False),
    pa.field("edge_owner_file_id", pa.string(), nullable=True),
    pa.field("bstart", pa.int64(), nullable=False),
    pa.field("bend", pa.int64(), nullable=False),
    pa.field("resolution_method", pa.string(), nullable=False),
    pa.field("confidence", pa.float64(), nullable=False),
    pa.field("score", pa.float64(), nullable=False),
    pa.field("task_name", pa.string(), nullable=False),
    pa.field("task_priority", pa.int32(), nullable=False),
    pa.field("origin", pa.string(), nullable=True),
)

# Standard tie-breakers (same for ALL relationships)
_STANDARD_TIE_BREAKERS: tuple[SortKeySpec, ...] = (
    SortKeySpec(column="score", order="descending"),
    SortKeySpec(column="confidence", order="descending"),
    SortKeySpec(column="task_priority", order="ascending"),
)


def generate_relationship_spec(data: RelationshipData) -> RelationshipTableSpec:
    """Generate a full RelationshipTableSpec from minimal data."""
    # Schema: entity_id + standard fields
    entity_field = pa.field(data.entity_id_col, pa.string(), nullable=False)
    schema = pa.schema([entity_field, *_STANDARD_RELATIONSHIP_FIELDS])

    # Dedup keys: entity_id + standard pattern + extras
    dedup_keys = (data.entity_id_col, "symbol", "path", "bstart", "bend")
    if data.extra_dedup_keys:
        dedup_keys = dedup_keys + data.extra_dedup_keys

    # Sort keys: path, bstart, entity_id
    sort_keys = ("path", "bstart", data.entity_id_col)

    return RelationshipTableSpec(
        table_name=f"{data.name}_v1",
        schema=schema,
        dedup_keys=dedup_keys,
        sort_keys=sort_keys,
        tie_breakers=_STANDARD_TIE_BREAKERS,
    )


# =============================================================================
# GENERATED REGISTRY: Replaces 8 manual builder functions
# =============================================================================

RELATIONSHIP_SPECS: dict[str, RelationshipTableSpec] = {
    data.name: generate_relationship_spec(data)
    for data in RELATIONSHIP_DATA
}


def get_relationship_spec(name: str) -> RelationshipTableSpec:
    """Get relationship spec by name."""
    return RELATIONSHIP_SPECS[name]
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Rewrite | `src/schema_spec/relationship_specs.py` | 397 → ~130 lines |
| Delete | (functions) | `_build_rel_name_symbol_spec`, `_build_rel_import_symbol_spec`, etc. (8 functions) |

### ast-grep Recipes

**Discovery - Find duplication patterns:**
```bash
# Find all _build_rel_*_spec functions
ast-grep run -p 'def _build_rel_$NAME_spec() -> RelationshipTableSpec:
    $$$BODY' -l python src/schema_spec/relationship_specs.py

# Count number of builder functions
ast-grep run -p 'def _build_rel_$NAME_spec()' -l python src/schema_spec/ --json=stream | wc -l

# Find repeated schema field patterns
ast-grep run -p 'pa.field("symbol", pa.string(), nullable=False)' -l python src/schema_spec/
ast-grep run -p 'pa.field("confidence", pa.float64(), nullable=False)' -l python src/schema_spec/

# Find repeated tie-breaker patterns
ast-grep run -p 'SortKeySpec(column="score", order="descending")' -l python src/schema_spec/
```

**Verification:**
```bash
# After refactor, verify no manual builder functions remain
ast-grep run -p 'def _build_rel_$NAME_spec()' -l python src/schema_spec/
# Should find 0

# Verify data-driven registry exists
ast-grep run -p 'RELATIONSHIP_DATA: tuple[RelationshipData' -l python src/schema_spec/

# Verify generator is used
ast-grep run -p 'generate_relationship_spec(data)' -l python src/schema_spec/
```

### cq Recipes

**Discovery:**
```bash
# Find all call sites for builder functions
./scripts/cq calls _build_rel_name_symbol_spec --root .
./scripts/cq calls _build_rel_import_symbol_spec --root .

# Find RELATIONSHIP_SPECS usages
./scripts/cq calls RELATIONSHIP_SPECS --root .
```

**Verification:**
```bash
# Verify generated specs are used
./scripts/cq calls generate_relationship_spec --root .

# Check for any remaining manual patterns
./scripts/cq side-effects --root src/schema_spec/relationship_specs.py
```

### Implementation Checklist
- [x] Create `RelationshipData` dataclass for minimal data
- [x] Define `RELATIONSHIP_DATA` tuple
- [x] Define standard relationship patterns (tie-breakers, virtual fields, dedupe/sort helpers)
- [x] Implement generator helpers for specs/contracts
- [x] Generate relationship specs/contracts from data
- [x] Delete all `_build_rel_*_spec()` functions
- [ ] Verify generated specs match original specs (field-by-field)
- [x] Update call sites to new data-driven API
- [ ] Add unit tests comparing generated vs expected schemas

### Decommissioning List
- Delete `_build_rel_name_symbol_spec()` (~45 lines)
- Delete `_build_rel_import_symbol_spec()` (~45 lines)
- Delete `_build_rel_def_symbol_spec()` (~45 lines)
- Delete `_build_rel_callsite_symbol_spec()` (~45 lines)
- Delete `_build_rel_callsite_qname_spec()` (~50 lines)
- Delete `_build_rel_scip_occurrence_spec()` (~45 lines)
- Delete `_build_rel_scip_reference_spec()` (~45 lines)
- Delete `_build_rel_scip_definition_spec()` (~45 lines)
- **Estimated reduction: 397 lines → 130 lines = 267 lines saved (67% reduction)**

---

## 0e. SessionContext Schema Hardening (Baseline Config)

**Status (2026-01-30): Partial.** `schema_profile.py` exists but is not yet used by session factories/runtime.

### Problem Statement
Schema inference and catalog introspection depend on **SessionContext configuration**. Without a hardened baseline, the same logical plan can yield **different schema surfaces** (view types, string mapping, timezone, Parquet metadata handling), making derived schemas non-deterministic and tests flaky.

### Target Implementation
```python
# src/datafusion_engine/session/schema_profile.py
from __future__ import annotations

from collections.abc import Iterable

from datafusion import SessionConfig, SessionContext


SCHEMA_PROFILE: tuple[tuple[str, str], ...] = (
    ("datafusion.catalog.create_default_catalog_and_schema", "true"),
    ("datafusion.catalog.default_catalog", "cpg"),
    ("datafusion.catalog.default_schema", "public"),
    ("datafusion.catalog.information_schema", "true"),
    ("datafusion.explain.show_schema", "true"),
    ("datafusion.format.types_info", "true"),
    ("datafusion.execution.time_zone", "UTC"),
    ("datafusion.execution.parquet.skip_metadata", "true"),
    ("datafusion.execution.parquet.schema_force_view_types", "false"),
    ("datafusion.sql_parser.map_string_types_to_utf8view", "false"),
)


def build_session_config(
    profile: Iterable[tuple[str, str]] = SCHEMA_PROFILE,
) -> SessionConfig:
    """Return a SessionConfig with deterministic schema behavior."""
    config = SessionConfig()
    for key, value in profile:
        config.set(key, value)
    return config


def create_session_context(
    profile: Iterable[tuple[str, str]] = SCHEMA_PROFILE,
) -> SessionContext:
    """Create a hardened SessionContext for schema derivation paths."""
    config = build_session_config(profile)
    return SessionContext(config)
```

### Observability & Caching (Optional)
- Register cache introspection table functions (`metadata_cache`, `statistics_cache`, `list_files_cache`) and surface snapshots in diagnostics.
- Capture `information_schema.df_settings` alongside plan artifacts to make schema behavior reproducible.

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/datafusion_engine/session/schema_profile.py` | Schema config profile + SessionContext builder |
| Modify | `src/datafusion_engine/session/runtime.py` | Use schema profile when constructing contexts |
| Modify | `src/datafusion_engine/session/factory.py` | Centralize profile injection for contexts |
| Modify | `src/datafusion_engine/schema/introspection.py` | Assert information_schema is enabled before introspection |

### ast-grep Recipes

**Discovery - Find direct SessionContext construction/config:**
```bash
# Direct SessionContext instantiation
ast-grep run -p 'SessionContext($$$)' -l python src/

# Inline SessionConfig.set(...) usage
ast-grep run -p '$CFG.set($KEY, $VALUE)' -l python src/ --selector call_expression
```

**Verification - Ensure hardened profile is used:**
```bash
ast-grep run -p 'create_session_context($$$)' -l python src/datafusion_engine/
ast-grep run -p 'build_session_config($$$)' -l python src/datafusion_engine/
```

### cq Recipes

**Discovery:**
```bash
# Find all session factories
./scripts/cq calls _session_context --root src/datafusion_engine
./scripts/cq calls session_context --root src/datafusion_engine
```

**Verification:**
```bash
# Confirm profile builder is used in factories
./scripts/cq calls create_session_context --root src/datafusion_engine
```

### Implementation Checklist
- [x] Create `schema_profile.py` with `SCHEMA_PROFILE`, `build_session_config()`, `create_session_context()`
- [ ] Route all SessionContext creation through the schema profile
- [ ] Ensure `information_schema` is enabled for schema introspection paths
- [ ] Add a small validation step that surfaces current `df_settings` in diagnostics
- [ ] Update unit tests that depend on implicit defaults
- [ ] (Optional) Register cache introspection table functions for schema observability

### Decommissioning List
- Remove ad-hoc SessionConfig defaulting scattered across modules
- Eliminate implicit SessionContext creation that bypasses schema profile

---

## 1. Relationship Spec Declarative DSL

**Status (2026-01-30): Complete.** Implemented in `src/cpg/relationship_specs.py`, `src/cpg/relationship_builder.py`, and wired into `src/relspec/relationship_datafusion.py`. Legacy wrapper functions remain for backward compatibility.

### Problem Statement
Five parallel relationship builder functions (`build_rel_name_symbol_df`, `build_rel_import_symbol_df`, etc.) plus five `_relation_output_from_*` wrappers are **95% copy-paste code**. Each manually specifies columns via `.select(col("X").alias("Y"), ...)` with only source table, entity ID column, and symbol column sourcing varying.

### Current State (Duplicated Pattern)
```python
# src/relspec/relationship_datafusion.py (historical pre-refactor pattern)
def build_rel_name_symbol_df(ctx: SessionContext, *, task_name: str, task_priority: int) -> DataFrame:
    source = ctx.table("cst_refs")
    return source.select(
        col("ref_id").alias("ref_id"),
        col("ref_text").alias("symbol"),
        col("symbol_roles").alias("symbol_roles"),
        col("path").alias("path"),
        f.coalesce(col("edge_owner_file_id"), col("file_id")).alias("edge_owner_file_id"),
        col("bstart").alias("bstart"),
        col("bend").alias("bend"),
        lit("cst_ref_text").alias("resolution_method"),
        lit(0.5).alias("confidence"),
        lit(0.5).alias("score"),
        lit(task_name).alias("task_name"),
        lit(task_priority).alias("task_priority"),
    )

def build_rel_import_symbol_df(ctx: SessionContext, *, task_name: str, task_priority: int) -> DataFrame:
    source = ctx.table("cst_imports")
    symbol = f.coalesce(col("name"), col("module"))  # Only real difference!
    return source.select(
        f.coalesce(col("import_alias_id"), col("import_id")).alias("import_alias_id"),
        symbol.alias("symbol"),
        # ... 10 more identical lines
    )

# ... 3 more nearly identical functions

def _relation_output_from_name(ctx: SessionContext) -> DataFrame:
    source = ctx.table("rel_name_symbol_v1")
    return _relation_output_base(
        source,
        src_col="ref_id",
        dst_col="symbol",
        bstart_col="bstart",
        bend_col="bend",
        kind=str(EDGE_KIND_PY_REFERENCES_SYMBOL),
        origin="cst",
    )

# ... 4 more nearly identical wrapper functions
```

### Target Implementation
```python
# src/cpg/relationship_specs.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cpg.kind_catalog import EdgeKindId

@dataclass(frozen=True)
class RelationshipSpec:
    """Declarative specification for a relationship join."""

    # Identity
    edge_kind: EdgeKindId
    origin: str  # "cst", "scip", "symtable", etc.

    # Source node
    src_table: str                          # "cst_refs", "cst_imports", etc.
    src_id_col: str                         # "ref_id", "import_alias_id", etc.
    src_id_fallback: str | None = None      # For coalesce patterns

    # Destination resolution
    dst_lookup_cols: tuple[str, ...]        # ("ref_text",), ("name", "module"), etc.
    dst_id_col: str | None = None           # If dst is computed via stable_id_parts

    # Metadata
    resolution_method: str                  # "cst_ref_text", "cst_import_name", etc.
    confidence: float = 0.5
    score: float = 0.5

    # Column overrides (defaults work for 90% of cases)
    path_col: str = "path"
    bstart_col: str = "bstart"
    bend_col: str = "bend"
    edge_owner_file_id_primary: str = "edge_owner_file_id"
    edge_owner_file_id_fallback: str = "file_id"
    symbol_roles_col: str | None = "symbol_roles"


# Declarative registry - 5 specs replace 150+ lines of builders
RELATIONSHIP_SPECS: tuple[RelationshipSpec, ...] = (
    RelationshipSpec(
        edge_kind=EDGE_KIND_PY_REFERENCES_SYMBOL,
        origin="cst",
        src_table="cst_refs",
        src_id_col="ref_id",
        dst_lookup_cols=("ref_text",),
        resolution_method="cst_ref_text",
    ),
    RelationshipSpec(
        edge_kind=EDGE_KIND_PY_IMPORTS_SYMBOL,
        origin="cst",
        src_table="cst_imports",
        src_id_col="import_alias_id",
        src_id_fallback="import_id",
        dst_lookup_cols=("name", "module"),
        resolution_method="cst_import_name",
    ),
    RelationshipSpec(
        edge_kind=EDGE_KIND_PY_DEFINES_SYMBOL,
        origin="cst",
        src_table="cst_defs",
        src_id_col="def_id",
        dst_lookup_cols=("name",),
        resolution_method="cst_def_name",
    ),
    RelationshipSpec(
        edge_kind=EDGE_KIND_PY_CALLS_SYMBOL,
        origin="cst",
        src_table="cst_callsites",
        src_id_col="call_id",
        dst_lookup_cols=("callee_text", "callee_dotted"),
        resolution_method="cst_call_callee",
    ),
    RelationshipSpec(
        edge_kind=EDGE_KIND_PY_CALLS_QNAME,
        origin="cst",
        src_table="callsite_qname_candidates_v1",
        src_id_col="call_id",
        dst_lookup_cols=("qname",),
        dst_id_col="qname_id",
        resolution_method="",
        bstart_col="call_bstart",
        bend_col="call_bend",
    ),
)
```

```python
# src/cpg/relationship_builder.py
from __future__ import annotations

from datafusion import SessionContext, DataFrame, col, lit
import datafusion.functions as f

from cpg.relationship_specs import RelationshipSpec, RELATIONSHIP_SPECS


def build_relation_df_from_spec(
    ctx: SessionContext,
    spec: RelationshipSpec,
    *,
    task_name: str,
    task_priority: int,
) -> DataFrame:
    """Generate a relation output DataFrame from a declarative spec."""
    source_df = ctx.table(spec.src_table)
    schema_names = set(source_df.schema().names)

    # Compute source ID (with optional fallback)
    if spec.src_id_fallback and spec.src_id_fallback in schema_names:
        src_expr = f.coalesce(col(spec.src_id_col), col(spec.src_id_fallback))
    else:
        src_expr = col(spec.src_id_col)

    # Compute destination lookup (coalesce strategy)
    available_dst_cols = [c for c in spec.dst_lookup_cols if c in schema_names]
    if len(available_dst_cols) > 1:
        dst_expr = f.coalesce(*[col(c) for c in available_dst_cols])
    elif available_dst_cols:
        dst_expr = col(available_dst_cols[0])
    else:
        dst_expr = lit(None).cast("utf8")

    # Build select expressions
    select_exprs = [
        src_expr.alias(spec.src_id_col),
        dst_expr.alias("symbol"),
        col(spec.path_col).alias("path") if spec.path_col in schema_names else lit(None).cast("utf8").alias("path"),
        f.coalesce(
            col(spec.edge_owner_file_id_primary) if spec.edge_owner_file_id_primary in schema_names else lit(None),
            col(spec.edge_owner_file_id_fallback) if spec.edge_owner_file_id_fallback in schema_names else lit(None),
        ).alias("edge_owner_file_id"),
        col(spec.bstart_col).alias("bstart") if spec.bstart_col in schema_names else lit(None).cast("int64").alias("bstart"),
        col(spec.bend_col).alias("bend") if spec.bend_col in schema_names else lit(None).cast("int64").alias("bend"),
        lit(spec.resolution_method).alias("resolution_method"),
        lit(spec.confidence).alias("confidence"),
        lit(spec.score).alias("score"),
        lit(task_name).alias("task_name"),
        lit(task_priority).alias("task_priority"),
    ]

    if spec.symbol_roles_col and spec.symbol_roles_col in schema_names:
        select_exprs.append(col(spec.symbol_roles_col).alias("symbol_roles"))
    else:
        select_exprs.append(lit(None).cast("int32").alias("symbol_roles"))

    return source_df.select(*select_exprs)


def build_all_relations_df(
    ctx: SessionContext,
    *,
    task_name: str = "rel",
    task_priority: int = 100,
) -> DataFrame:
    """Build unified relation output from all relationship specs."""
    frames = [
        build_relation_df_from_spec(ctx, spec, task_name=task_name, task_priority=task_priority)
        for spec in RELATIONSHIP_SPECS
    ]
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.union(frame)
    return combined
```

Note: the current implementation uses `build_all_symbol_relations_dfs()` and `build_all_qname_relations_dfs()` instead of a single `build_all_relations_df()`.

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/cpg/relationship_specs.py` | RelationshipSpec dataclass + RELATIONSHIP_SPECS registry (implemented) |
| Create | `src/cpg/relationship_builder.py` | Spec-driven generators (`build_relation_df_from_spec`, `build_all_symbol_relations_dfs`, `build_all_qname_relations_dfs`) |
| Modify | `src/relspec/relationship_datafusion.py` | Generator wired in; wrappers retained for compatibility |
| Delete | (optional) | Remove legacy wrapper functions after call sites migrate |

### ast-grep Recipes

**Discovery - Find all relationship builder patterns:**
```bash
# Find build_rel_*_symbol_df functions
ast-grep run -p 'def build_rel_$NAME_df($PARAMS) -> DataFrame:
    $$$BODY' -l python src/cpg/

# Find _relation_output_from_* functions
ast-grep run -p 'def _relation_output_from_$NAME($PARAMS) -> DataFrame:
    $$$BODY' -l python src/cpg/

# Count .select() calls in relationship builders
ast-grep run -p 'return source.select($$$)' -l python src/relspec/relationship_datafusion.py

# Find identical metadata injection pattern
ast-grep run -p 'lit($VALUE).alias("resolution_method")' -l python src/cpg/
ast-grep run -p 'lit($VALUE).alias("confidence")' -l python src/cpg/

# Find coalesce patterns for column fallbacks
ast-grep run -p 'f.coalesce(col($COL1), col($COL2))' -l python src/cpg/
```

**Verification - Ensure spec-driven adoption:**
```bash
# After refactor, verify no remaining manual builders
ast-grep run -p 'def build_rel_$NAME_symbol_df' -l python src/cpg/
# Should find 0

# Verify spec registry usage
ast-grep run -p 'RelationshipSpec(' -l python src/cpg/relationship_specs.py

# Verify generator usage
ast-grep run -p 'build_relation_df_from_spec($CTX, $SPEC' -l python src/cpg/

# Find any remaining direct .select() in relationship building
ast-grep scan --inline-rules "$(cat <<'YAML'
id: manual-relation-select
language: Python
rule:
  pattern: return $SOURCE.select($$$COLS)
  inside:
    pattern: def build_rel_$NAME($$$)
severity: warning
message: Relationship builder should use spec-driven generation
YAML
)" src/cpg/
```

### cq Recipes

**Discovery:**
```bash
# Find all call sites for relationship builder functions
./scripts/cq calls build_rel_name_symbol_df --root .
./scripts/cq calls build_rel_import_symbol_df --root .

# Analyze parameter flow
./scripts/cq impact build_rel_name_symbol_df --param ctx --root .

# Check for import dependencies
./scripts/cq imports --module src.relspec.relationship_datafusion --root .

# Find edge kind constant usages
./scripts/cq calls EDGE_KIND_PY_REFERENCES_SYMBOL --root .
```

**Post-refactor verification:**
```bash
# Verify no breaking changes to callers
./scripts/cq calls build_all_relations_df --root .

# Check for side effects in new module
./scripts/cq side-effects --root src/cpg/relationship_specs.py

# Verify bytecode surface unchanged for dependent modules
./scripts/cq bytecode-surface src/cpg/view_builders_df.py --show calls
```

### Implementation Checklist
- [x] Create `src/cpg/relationship_specs.py` with `RelationshipSpec` dataclass
- [x] Define `RELATIONSHIP_SPECS` registry with 5 spec instances
- [x] Create `src/cpg/relationship_builder.py` with spec-driven builders
- [x] Implement `build_all_symbol_relations_dfs()` and `build_all_qname_relations_dfs()`
- [x] Refactor `relationship_datafusion.py` to use generator
- [ ] Update call sites to use new API (wrappers still used for compatibility)
- [ ] Run ast-grep verification patterns
- [ ] Run cq calls analysis to verify no breakage
- [ ] Add unit tests for spec-driven generation
- [ ] Verify DataFrame schemas match original implementations

### Decommissioning List
- Remove `build_rel_name_symbol_df()` (~25 lines) once call sites migrate
- Remove `build_rel_import_symbol_df()` (~25 lines) once call sites migrate
- Remove `build_rel_def_symbol_df()` (~25 lines) once call sites migrate
- Remove `build_rel_callsite_symbol_df()` (~25 lines) once call sites migrate
- Remove `build_rel_callsite_qname_df()` (~30 lines) once call sites migrate
- Remove `_relation_output_from_name()` (~10 lines) once call sites migrate
- Remove `_relation_output_from_import()` (~10 lines) once call sites migrate
- Remove `_relation_output_from_def()` (~10 lines) once call sites migrate
- Remove `_relation_output_from_call_symbol()` (~10 lines) once call sites migrate
- Remove `_relation_output_from_call_qname()` (~10 lines) once call sites migrate
- **Estimated reduction: 180 lines → 80 lines of specs + 100 lines of generator = net 0, but maintainability improves; wrappers are optional**

---

## 2. Schema Inference from DataFusion Plans

**Status (2026-01-30): Partial.** Core inference utilities exist in `src/datafusion_engine/schema/inference.py` and extraction schema derivation lives in `src/extract/schema_derivation.py`, but relspec execution planning does not yet capture or persist inferred schemas.

### Problem Statement
Schemas are declared explicitly and validated post-hoc. The existing `lineage_datafusion.py` extracts dependencies from DataFusion plans but doesn't derive output schemas. DataFusion exposes **computed schema surfaces** (`DESCRIBE <query>` and plan schema inspection), so schema inference should prioritize those authoritative outputs and only use plan-walking for metadata not present in `DESCRIBE` (ordering, provenance).

### Current State
Schema inference is implemented but **not yet wired into relspec execution planning**. `infer_deps_from_plan_bundle()` still focuses on input dependency extraction, while schema contracts remain validated post-hoc.

### Target Implementation
```python
# src/datafusion_engine/schema/inference.py (implemented)
from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping

import pyarrow as pa

from datafusion_engine.lineage.datafusion import LineageReport


@dataclass(frozen=True)
class SchemaInferenceResult:
    """Complete schema inference result from a DataFusion plan."""

    output_schema: pa.Schema
    column_lineage: Mapping[str, ColumnLineage]
    source_tables: frozenset[str]
    lineage_report: LineageReport | None = None


def infer_schema_from_dataframe(df: DataFrame) -> pa.Schema:
    """Extract Arrow schema from a DataFusion DataFrame."""


def infer_schema_from_logical_plan(plan: object) -> pa.Schema:
    """Extract Arrow schema from a DataFusion LogicalPlan."""


def infer_schema_with_lineage(df: DataFrame) -> SchemaInferenceResult:
    """Perform complete schema inference with column-level lineage."""
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/datafusion_engine/schema/inference.py` | Schema inference + lineage (implemented) |
| Create | `src/extract/schema_derivation.py` | Extraction schema derivation (implemented) |
| Modify | `src/relspec/execution_plan.py` | (Remaining) Add `inferred_schemas` field + wiring |
| Modify | `src/datafusion_engine/plan/walk.py` | Optional helpers for plan traversal |

### ast-grep Recipes

**Discovery - Find existing plan introspection patterns:**
```bash
# Find walk_logical_complete usages
ast-grep run -p 'walk_logical_complete($PLAN)' -l python src/

# Find existing lineage extraction patterns
ast-grep run -p 'extract_lineage($PLAN$$$)' -l python src/

# Find plan variant handling
ast-grep run -p 'if tag == "$TAG":' -l python src/datafusion_engine/lineage/

# Find computed schema patterns
rg -n 'DESCRIBE' src/
ast-grep run -p '$DF.schema()' -l python src/
```

**Verification:**
```bash
# After implementation, verify schema inference is called
ast-grep run -p 'infer_schema_from_dataframe($$$)' -l python src/datafusion_engine/schema/
ast-grep run -p 'infer_schema_from_logical_plan($$$)' -l python src/datafusion_engine/schema/

# Verify ExecutionPlan wiring (remaining work)
ast-grep run -p 'inferred_schemas' -l python src/relspec/execution_plan.py
```

### cq Recipes

**Discovery:**
```bash
# Find all plan bundle usages
./scripts/cq calls DataFusionPlanBundle --root .

# Find compilation entry points
./scripts/cq calls compile_execution_plan --root .

# Analyze lineage extraction patterns
./scripts/cq impact extract_lineage --param plan --root .

# Check for schema validation patterns
./scripts/cq calls validate_schema_contract --root .
```

**Post-refactor verification:**
```bash
# Verify schema inference integration
./scripts/cq calls infer_schema_from_dataframe --root .
./scripts/cq calls infer_schema_from_logical_plan --root .

# Check for import cycles
./scripts/cq imports --cycles --root src/relspec
```

### Implementation Checklist
- [x] Implement `SchemaInferenceResult` + lineage helpers (done in `schema/inference.py`)
- [x] Use `infer_schema_from_dataframe()` and `infer_schema_from_logical_plan()` as core signals
- [x] Add `schema_fingerprint_from_inference()` for cache keys
- [ ] Extend `ExecutionPlan` with `inferred_schemas` field (remaining)
- [ ] Integrate schema inference into `compile_execution_plan()` (remaining)
- [x] Add unit tests for schema inference from sample plans (done in `tests/unit/test_schema_inference.py`)

### Decommissioning List
- Remove post-hoc schema validation where inference can catch errors earlier
- Remove projection-type extraction code that duplicates `DESCRIBE` output
- Estimated impact: Earlier error detection, not direct line reduction

---

## 3. File Identity Canonical Type

**Status (2026-01-30): Mostly complete.** Canonical fields and helpers exist in `schema_spec/file_identity.py`, and registry schemas use the shared field constants; nested struct usage remains to be migrated.

### Problem Statement
File identity fields (`file_id`, `path`, `file_sha256`, `repo`) are repeated **50+ times** across schema definitions. Each extraction schema, relationship schema, and nested struct type manually declares these fields.

### Current State (Repeated 50+ times)
```python
# src/datafusion_engine/schema/registry.py
AST_FILES_SCHEMA = pa.schema([
    ("repo", pa.string()),
    pa.field("path", pa.string(), nullable=False),
    pa.field("file_id", pa.string(), nullable=False),
    ("file_sha256", pa.string()),
    # ... more fields
])

CST_FILES_SCHEMA = pa.schema([
    ("repo", pa.string()),
    pa.field("path", pa.string(), nullable=False),
    pa.field("file_id", pa.string(), nullable=False),
    ("file_sha256", pa.string()),
    # ... more fields
])

# Nested structs also repeat this:
AST_NODE_T = pa.struct([
    ("file_id", pa.string()),  # Repeated again
    ("path", pa.string()),     # Repeated again
    # ...
])
```

### Target Implementation
```python
# src/schema_spec/file_identity.py
from __future__ import annotations

import pyarrow as pa


FILE_IDENTITY_FIELDS: tuple[pa.Field, ...] = (
    pa.field("repo", pa.string(), nullable=True),
    pa.field("path", pa.string(), nullable=False),
    pa.field("file_id", pa.string(), nullable=False),
    pa.field("file_sha256", pa.string(), nullable=True),
)

FILE_IDENTITY_FIELDS_FOR_NESTING: tuple[pa.Field, ...] = (
    pa.field("file_id", pa.string(), nullable=True),
    pa.field("path", pa.string(), nullable=True),
)


def file_identity_fields() -> tuple[pa.Field, ...]:
    """Canonical file identity fields present in all extractors."""
    return FILE_IDENTITY_FIELDS


def file_identity_struct() -> pa.StructType:
    """Canonical file identity as a struct type."""
    return pa.struct(FILE_IDENTITY_FIELDS)


def file_identity_fields_for_nesting() -> tuple[pa.Field, ...]:
    """File identity fields for nested structs (all nullable for safety)."""
    return FILE_IDENTITY_FIELDS_FOR_NESTING


def schema_with_file_identity(
    *additional_fields: pa.Field,
    include_repo: bool = True,
    include_sha256: bool = True,
) -> pa.Schema:
    """Build a schema starting with file identity fields."""
    identity_fields = list(FILE_IDENTITY_FIELDS)
    if not include_repo:
        identity_fields = [field for field in identity_fields if field.name != "repo"]
    if not include_sha256:
        identity_fields = [field for field in identity_fields if field.name != "file_sha256"]
    return pa.schema([*identity_fields, *additional_fields])


# Usage in registry.py:
AST_FILES_SCHEMA = schema_with_file_identity(
    pa.field("nodes", pa.list_(AST_NODE_T)),
    pa.field("edges", pa.list_(AST_EDGE_T)),
    pa.field("errors", pa.list_(ERROR_T)),
    # ... more fields
)
```

### Delta Integration Note (Optional)
- When scanning Delta tables, configure `DeltaScanConfig.file_column_name` to emit a file-path lineage column.
- Use this column to populate `path`/`file_id` in derived schemas when upstream metadata is incomplete.

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/schema_spec/file_identity.py` | Canonical file identity fields + builders |
| Modify | `src/schema_spec/specs.py` | Re-export schema_with_file_identity() if needed |
| Modify | `src/datafusion_engine/schema/registry.py` | Use canonical builders |
| Modify | `src/extract/metadata.py` | Reference canonical file identity |

### ast-grep Recipes

**Discovery - Find all file identity field patterns:**
```bash
# Find explicit file_id field definitions
ast-grep run -p 'pa.field("file_id", pa.string()$$$)' -l python src/

# Find path field definitions
ast-grep run -p 'pa.field("path", pa.string()$$$)' -l python src/

# Find tuple patterns with file identity
ast-grep run -p '("file_id", pa.string())' -l python src/
ast-grep run -p '("path", pa.string())' -l python src/
ast-grep run -p '("file_sha256", pa.string())' -l python src/
ast-grep run -p '("repo", pa.string())' -l python src/

# Count total occurrences
ast-grep run -p '"file_id"' -l python src/datafusion_engine/schema/ --json=stream | wc -l
```

**Verification:**
```bash
# After refactor, verify canonical usage
ast-grep run -p 'file_identity_fields()' -l python src/datafusion_engine/
ast-grep run -p 'schema_with_file_identity($$$)' -l python src/datafusion_engine/

# Find any remaining direct definitions (should approach 0)
ast-grep scan --inline-rules "$(cat <<'YAML'
id: direct-file-identity
language: Python
rule:
  any:
    - pattern: 'pa.field("file_id", pa.string())'
    - pattern: '("file_id", pa.string())'
  not:
    inside:
      pattern: def file_identity_$NAME
severity: warning
message: Use file_identity_fields() instead of direct definition
YAML
)" src/datafusion_engine/schema/
```

### cq Recipes

**Discovery:**
```bash
# Find schema construction patterns
./scripts/cq calls pa.schema --root .

# Find file identity helper usages
./scripts/cq calls file_identity_fields --root .

# Check for old bundle imports (should be 0)
./scripts/cq imports --module src.schema_spec.bundles --root .
```

### Implementation Checklist
- [x] Add `file_identity_fields()` and `file_identity_struct()` in `file_identity.py`
- [x] Add `schema_with_file_identity()` builder function
- [x] Refactor `AST_FILES_SCHEMA` to use canonical file identity fields
- [x] Refactor `CST_FILES_SCHEMA` to use canonical file identity fields
- [x] Refactor `SCIP_INDEX_SCHEMA` to use canonical file identity fields
- [ ] Refactor nested struct types to use `file_identity_fields_for_nesting()`
- [ ] Run ast-grep to verify reduction in direct definitions
- [ ] Add unit tests for canonical functions
- [ ] (Optional) Wire Delta scan lineage column to file identity fields

### Decommissioning List
- Remove 50+ direct field definitions of file identity
- Estimated reduction: 200+ lines

---

## 4. View Builder DSL

**Status (2026-01-30): Partial.** `view_spec.py`, `view_specs_catalog.py`, and `views/dsl.py` are implemented, but `views/registry.py` still relies on `_VIEW_SELECT_EXPRS`.

### Problem Statement
`_VIEW_SELECT_EXPRS` in `src/datafusion_engine/views/registry.py` contains **3,256 lines** of static view select expressions. Each view manually defines column selections that are **80% identical patterns** (file_id, path, aliasing, type casting).

### Current State (3,256 lines)
```python
# src/datafusion_engine/views/registry.py
_VIEW_SELECT_EXPRS: dict[str, tuple[Expr, ...]] = {
    "ast_call_attrs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ast_id").alias("ast_id"),
        (col("kv"))["key"].alias("attr_key"),
        (col("kv"))["value"].alias("attr_value"),
    ),
    "ast_calls": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ast_id").alias("ast_id"),
        col("parent_ast_id").alias("parent_ast_id"),
        _arrow_cast(col("bstart"), "Int32").alias("bstart"),
        _arrow_cast(col("bend"), "Int32").alias("bend"),
        arrow_metadata(col("nodes")["span"], "line_base").alias("line_base"),
        arrow_metadata(col("nodes")["span"], "col_unit").alias("col_unit"),
        # ... 20+ more fields
    ),
    # ... 40+ more views, each 10-200 lines
}
```

### Target Implementation
```python
# src/datafusion_engine/views/view_spec.py
from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from collections.abc import Sequence
from typing import Protocol, runtime_checkable


class ColumnTransformKind(StrEnum):
    """Types of column transformations."""

    PASSTHROUGH = "passthrough"       # col(x).alias(x)
    RENAME = "rename"                 # col(x).alias(y)
    CAST = "cast"                     # arrow_cast(col(x), type)
    STRUCT_FIELD = "struct_field"     # col(struct)[field]
    METADATA_EXTRACT = "metadata"     # arrow_metadata(col(struct), key)
    COALESCE = "coalesce"             # coalesce(col(a), col(b))
    UNNEST = "unnest"                 # unnest(col(list))


@dataclass(frozen=True)
class ColumnTransform:
    """Specification for a column transformation."""

    kind: ColumnTransformKind
    source_col: str
    output_name: str | None = None  # Defaults to source_col
    cast_type: str | None = None    # For CAST
    struct_field: str | None = None # For STRUCT_FIELD
    metadata_key: str | None = None # For METADATA_EXTRACT
    fallback_cols: tuple[str, ...] = ()  # For COALESCE


@dataclass(frozen=True)
class ViewProjectionSpec:
    """Declarative view projection specification."""

    name: str
    base_table: str
    comment: str = ""

    # Column specifications
    passthrough_cols: tuple[str, ...] = ()  # Direct pass-through
    transforms: tuple[ColumnTransform, ...] = ()

    # Common patterns (auto-expanded)
    include_file_identity: bool = True
    include_span_fields: bool = False
    struct_metadata_extracts: tuple[tuple[str, tuple[str, ...]], ...] = ()
    # e.g., (("span", ("line_base", "col_unit")),)

    def to_exprs(self, base_schema: pa.Schema) -> tuple[Expr, ...]:
        """Convert spec to DataFusion expressions."""
        exprs: list[Expr] = []

        # File identity fields
        if self.include_file_identity:
            for col_name in ("file_id", "path"):
                if col_name in base_schema.names:
                    exprs.append(col(col_name).alias(col_name))

        # Passthrough columns
        for col_name in self.passthrough_cols:
            if col_name in base_schema.names:
                exprs.append(col(col_name).alias(col_name))

        # Custom transforms
        for transform in self.transforms:
            expr = self._transform_to_expr(transform, base_schema)
            if expr is not None:
                exprs.append(expr)

        # Struct metadata extracts
        for struct_col, keys in self.struct_metadata_extracts:
            for key in keys:
                if struct_col in base_schema.names:
                    exprs.append(
                        arrow_metadata(col(struct_col), key).alias(key)
                    )

        return tuple(exprs)


# Registry of view specs (replaces 3,256-line dict)
VIEW_SPECS: tuple[ViewProjectionSpec, ...] = (
    ViewProjectionSpec(
        name="ast_calls",
        base_table="ast_files_v1",
        include_file_identity=True,
        passthrough_cols=("ast_id", "parent_ast_id"),
        transforms=(
            ColumnTransform(ColumnTransformKind.CAST, "bstart", cast_type="Int32"),
            ColumnTransform(ColumnTransformKind.CAST, "bend", cast_type="Int32"),
        ),
        struct_metadata_extracts=(("span", ("line_base", "col_unit")),),
    ),
    # ... more specs (each ~10 lines instead of 30+)
)
```

### DataFusion-native Shaping (Recommended)
- Prefer `struct`/`named_struct`, `get_field`, `map_*`, and `unnest` to express view shapes instead of hand-coded expression chains.
- Keep nested bundle tables (`*_files`) as `LIST<STRUCT>` and generate 2D views via `UNNEST` for joins.
- Use `arrow_cast`/`arrow_typeof` in view specs when a precise Arrow type is required.

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/datafusion_engine/views/view_spec.py` | ViewProjectionSpec DSL (implemented) |
| Create | `src/datafusion_engine/views/view_specs_catalog.py` | Spec catalog + generator (implemented) |
| Create | `src/datafusion_engine/views/dsl.py` | Expression DSL helpers (implemented) |
| Modify | `src/datafusion_engine/views/registry.py` | (Remaining) replace `_VIEW_SELECT_EXPRS` with generator |

### ast-grep Recipes

**Discovery - Analyze view expression patterns:**
```bash
# Count view entries
ast-grep run -p '"$VIEW_NAME": ($$$)' -l python src/datafusion_engine/views/registry.py --json=stream | wc -l

# Find common patterns - file_id alias
ast-grep run -p 'col("file_id").alias("file_id")' -l python src/datafusion_engine/views/

# Find common patterns - path alias
ast-grep run -p 'col("path").alias("path")' -l python src/datafusion_engine/views/

# Find arrow_cast patterns
ast-grep run -p '_arrow_cast(col($COL), $TYPE)' -l python src/datafusion_engine/views/

# Find arrow_metadata patterns
ast-grep run -p 'arrow_metadata(col($COL), $KEY)' -l python src/datafusion_engine/views/

# Find struct field access patterns
ast-grep run -p '(col($COL))[$FIELD]' -l python src/datafusion_engine/views/
```

**Verification:**
```bash
# After refactor, verify spec-driven generation
ast-grep run -p 'ViewProjectionSpec(' -l python src/datafusion_engine/views/

# Verify no remaining inline expressions
ast-grep run -p '_VIEW_SELECT_EXPRS' -l python src/datafusion_engine/views/
# Should find only in migration/compat code or be removed
```

### cq Recipes

**Discovery:**
```bash
# Find view registration patterns
./scripts/cq calls register_all_views --root .

# Find view select expression usages
./scripts/cq calls VIEW_SELECT_REGISTRY --root .

# Analyze view builder patterns
./scripts/cq impact register_view_nodes --param ctx --root .
```

### Implementation Checklist
- [x] Create `ViewProjectionSpec` dataclass with common patterns
- [x] Create `ColumnTransform` for custom transformations
- [x] Implement `to_exprs()` method for expression generation
- [x] Create `VIEW_SPECS` registry (catalog implemented in `view_specs_catalog.py`)
- [x] Convert 40+ views to spec format
- [ ] Update view registration to use specs
- [ ] Verify all views produce identical schemas
- [ ] Add unit tests comparing spec-generated vs original expressions

### Decommissioning List
- Remove `_VIEW_SELECT_EXPRS` (3,256 lines)
- Estimated reduction: 3,256 lines → ~800 lines of specs = 2,456 lines saved

---

## 4b. DataFusion DataFrame View Registration (Spec-Driven)

**Status (2026-01-31): Not complete.** View specs exist but registry still uses manual `_VIEW_SELECT_EXPRS`.

### Goal
Make **DataFusion DataFrame expressions** the single mechanism for view creation.  
No SQL strings, no Rust LogicalPlan front‑end, no manual `_VIEW_SELECT_EXPRS`.

### Rationale (Design Phase)
- We already have **DataFrame‑native view specs** (`ViewProjectionSpec` + `view_specs_catalog.py`).
- `register_view_specs` + `ViewGraph` already supports **DataFrame builders** with lineage + UDF validation.
- This eliminates the manual registry dict and reduces ambiguity about “how views are built.”

---

### Current State
- `views/registry.py` owns a giant `_VIEW_SELECT_EXPRS` dict plus special‑case builders.
- `views/view_spec.py` + `views/view_specs_catalog.py` can generate expressions but **aren’t wired** to registry.
- `views/dsl.py` / `views/dsl_views.py` exist but are **unused prototypes**.

---

### Target Architecture
1. **Single source of truth** for view projection expressions:
   - `ViewProjectionSpec` (data‑driven) + `view_specs_catalog.generate_view_select_exprs()`.
2. **Registry builds DataFrame views from specs**:
   - Registry maps view name → `ViewProjectionSpec` → DataFrame builder → `ViewSpec`.
3. **Special‑case builders remain** only for:
   - map_entries/map_keys/map_values attrs views
   - CST span unnest views
   - UDF‑dependent manual expressions (stable_id, prefixed_hash64, span_make, etc.)

---

### Target Implementation Sketch

**Spec‑driven view builder (registry):**
```python
from datafusion_engine.views.view_specs_catalog import VIEW_SPECS_BY_NAME


def _spec_view_df(ctx: SessionContext, *, name: str) -> DataFrame:
    spec = VIEW_SPECS_BY_NAME.get(name)
    if spec is None:
        msg = f"Unknown view spec: {name!r}."
        raise KeyError(msg)
    base_df = ctx.table(spec.base_table)
    if spec.unnest_column is not None:
        base_df = base_df.unnest_columns(spec.unnest_column)
    return base_df.select(*spec.to_exprs())
```

**Registry replaces manual dict:**
```python
from datafusion_engine.views.view_specs_catalog import generate_view_select_exprs

_VIEW_SELECT_EXPRS: dict[str, tuple[Expr, ...]] = generate_view_select_exprs()
```

---

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/datafusion_engine/views/registry.py` | Replace `_VIEW_SELECT_EXPRS` with spec‑generated expressions; add `_spec_view_df` builder. |
| Modify | `src/datafusion_engine/views/registry.py` | Use spec‑driven builders for registry view specs (via `registry_view_specs`). |
| Modify | `src/datafusion_engine/views/view_specs_catalog.py` | Ensure `VIEW_SPECS` coverage + `generate_view_select_exprs` is complete. |
| Modify | `src/datafusion_engine/session/runtime.py` | Ensure view registration uses spec‑based builders (no legacy paths). |
| Delete | `src/datafusion_engine/views/dsl.py` | Prototype DSL (unused after spec‑driven registry). |
| Delete | `src/datafusion_engine/views/dsl_views.py` | Prototype DSL builders (unused). |

---

### Decommissioning & Deletion Scope
- Delete `_VIEW_SELECT_EXPRS` manual map in `views/registry.py`.
- Delete **duplicated span helpers** in `views/registry.py` once `view_spec.py` is canonical.
- Remove **DSL prototype files** (`dsl.py`, `dsl_views.py`).
- Remove any legacy fallback path that creates `ViewSpec` from manual expression dicts.

---

### Implementation Checklist
- [ ] Wire registry to `VIEW_SPECS_BY_NAME` + `generate_view_select_exprs()`
- [ ] Add `_spec_view_df` builder (spec → DataFrame)
- [ ] Ensure `registry_view_specs()` uses spec builder for all spec‑backed views
- [ ] Keep only necessary special‑case builders (map_entries/keys/values, CST span unnest, UDF‑dependent manual views)
- [ ] Delete `_VIEW_SELECT_EXPRS` manual dict
- [ ] Delete `views/dsl.py` and `views/dsl_views.py`
- [ ] Run end‑to‑end view registration smoke (view graph + UDF snapshot)

---

## 5. Node Family Spec Defaults

**Status (2026-01-30): Complete.** Defaults implemented in `src/cpg/spec_registry.py` via `NodeFamily` fallbacks.

### Problem Statement
35+ `EntityFamilySpec` instances in `spec_registry.py` have **90% identical** column specifications. Almost all use `path_cols=("path",)`, `bstart_cols=("bstart",)`, `bend_cols=("bend",)`, `file_id_cols=("file_id",)`.

### Current State (590 lines of specs)
```python
# src/cpg/spec_registry.py
ENTITY_FAMILY_SPECS: tuple[EntityFamilySpec, ...] = (
    EntityFamilySpec(
        name="ref",
        node_kind=NODE_KIND_CST_REF,
        id_cols=("ref_id",),
        node_table="cst_refs",
        prop_source_map={"ref_text": "ref_text", "ref_kind": "ref_kind"},
        path_cols=("path",),       # Same for 90%
        bstart_cols=("bstart",),   # Same for 90%
        bend_cols=("bend",),       # Same for 90%
        file_id_cols=("file_id",), # Same for 90%
    ),
    EntityFamilySpec(
        name="import",
        node_kind=NODE_KIND_CST_IMPORT,
        id_cols=("import_id",),
        node_table="cst_imports",
        prop_source_map={"module": "module", "name": "name"},
        path_cols=("path",),       # Repeated
        bstart_cols=("bstart",),   # Repeated
        bend_cols=("bend",),       # Repeated
        file_id_cols=("file_id",), # Repeated
    ),
    # ... 33 more specs with same defaults
)
```

### Target Implementation
```python
# src/cpg/spec_registry.py (refactored)
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class EntityFamilySpec:
    """Specification for a CPG entity family with sensible defaults."""

    name: str
    node_kind: NodeKindId
    id_cols: tuple[str, ...]
    node_table: str | None = None
    prop_table: str | None = None  # Defaults to node_table
    prop_source_map: Mapping[str, str | PropFieldInput] = field(default_factory=dict)

    # DEFAULT COLUMNS - only override when non-standard
    path_cols: tuple[str, ...] = ("path",)
    bstart_cols: tuple[str, ...] = ("bstart",)
    bend_cols: tuple[str, ...] = ("bend",)
    file_id_cols: tuple[str, ...] = ("file_id",)

    def __post_init__(self) -> None:
        if self.prop_table is None and self.node_table is not None:
            object.__setattr__(self, "prop_table", self.node_table)


# Simplified registry - most specs now just need 4-5 lines
ENTITY_FAMILY_SPECS: tuple[EntityFamilySpec, ...] = (
    EntityFamilySpec(
        name="ref",
        node_kind=NODE_KIND_CST_REF,
        id_cols=("ref_id",),
        node_table="cst_refs",
        prop_source_map={"ref_text": "ref_text", "ref_kind": "ref_kind"},
        # No need to specify path_cols, bstart_cols, etc. - defaults work!
    ),
    EntityFamilySpec(
        name="import",
        node_kind=NODE_KIND_CST_IMPORT,
        id_cols=("import_id",),
        node_table="cst_imports",
        prop_source_map={"module": "module", "name": "name"},
        # Defaults work!
    ),
    # Only specify overrides for non-standard cases:
    EntityFamilySpec(
        name="callsite_qname",
        node_kind=NODE_KIND_CST_CALLSITE_QNAME,
        id_cols=("call_id",),
        node_table="callsite_qname_candidates_v1",
        prop_source_map={"qname": "qname"},
        bstart_cols=("call_bstart",),  # Override - different column name
        bend_cols=("call_bend",),      # Override - different column name
    ),
    # ... more specs
)
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/cpg/spec_registry.py` | Add defaults to EntityFamilySpec, simplify registry |

### ast-grep Recipes

**Discovery - Find repeated default patterns:**
```bash
# Find explicit path_cols defaults
ast-grep run -p 'path_cols=("path",)' -l python src/cpg/spec_registry.py

# Count occurrences
ast-grep run -p 'path_cols=("path",)' -l python src/cpg/ --json=stream | wc -l
ast-grep run -p 'bstart_cols=("bstart",)' -l python src/cpg/ --json=stream | wc -l
ast-grep run -p 'file_id_cols=("file_id",)' -l python src/cpg/ --json=stream | wc -l
```

**Verification:**
```bash
# After refactor, count remaining explicit defaults (should be minimal)
ast-grep run -p 'path_cols=("path",)' -l python src/cpg/spec_registry.py --json=stream | wc -l
# Should be near 0 (only when overriding from default)

# Verify specs still work
ast-grep run -p 'EntityFamilySpec(' -l python src/cpg/spec_registry.py
```

### cq Recipes

**Discovery:**
```bash
# Find EntityFamilySpec usages
./scripts/cq calls EntityFamilySpec --root .

# Analyze to_node_plan and to_prop_table methods
./scripts/cq calls to_node_plan --root .
./scripts/cq calls to_prop_table --root .
```

### Implementation Checklist
- [x] Add default values to `EntityFamilySpec` fields
- [x] Remove explicit default values from 30+ spec instances
- [x] Keep explicit overrides only for non-standard cases (3-4 specs)
- [ ] Verify all node/edge/property generation produces identical results
- [ ] Run tests to ensure no behavioral changes

### Decommissioning List
- Remove ~30 explicit `path_cols=("path",)` assignments
- Remove ~30 explicit `bstart_cols=("bstart",)` assignments
- Remove ~30 explicit `bend_cols=("bend",)` assignments
- Remove ~30 explicit `file_id_cols=("file_id",)` assignments
- Estimated reduction: 120+ lines

---

## 6. Extraction Row Builder

**Status (2026-01-30): Partial.** `ExtractionRowBuilder` exists in `src/extract/row_builder.py` with identity/span helpers (used by coordination context), but extractors still construct rows manually and schema-validation builders are not implemented.

### Problem Statement
Each extractor manually constructs rows as dictionaries with explicit field enumeration. There's no validation against schemas at extraction time, causing late error detection.

### Current State
```python
# src/extract/extractors/ast_extract.py
def _ast_row_from_walk(
    file_ctx: FileContext,
    *,
    options: AstExtractOptions,
    walk: WalkResult,
    errors: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    return {
        "repo": file_ctx.repo,
        "path": file_ctx.path,
        "file_id": file_ctx.file_id,
        "file_sha256": file_ctx.file_sha256,
        "nodes": walk.nodes,
        "edges": walk.edges,
        "errors": errors,
        "docstrings": walk.docstrings,
        "imports": walk.imports,
        "defs": walk.defs,
        "calls": walk.calls,
        "type_ignores": walk.type_ignores,
        "parse_manifest": _parse_manifest_rows(options),
        "attrs": {},
    }

# Similar manual construction in cst_extract.py, bytecode_extract.py, etc.
```

### Target Implementation
```python
# src/extract/row_builder.py (implemented)
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExtractionRowBuilder:
    """Build extraction rows with consistent file identity and span handling."""

    file_id: str
    path: str
    file_sha256: str | None = None
    repo_id: str | None = None

    @classmethod
    def from_file_context(cls, file_ctx: FileContext, *, repo_id: str | None = None) -> ExtractionRowBuilder:
        return cls(
            file_id=file_ctx.file_id,
            path=file_ctx.path,
            file_sha256=file_ctx.file_sha256,
            repo_id=repo_id,
        )

    def add_identity(self) -> dict[str, str | None]:
        return {"file_id": self.file_id, "path": self.path, "file_sha256": self.file_sha256}

    def build_row(self, *, include_repo: bool = False, **fields: object) -> dict[str, object]:
        row = {"repo": self.repo_id, **self.add_identity()} if include_repo else dict(self.add_identity())
        row.update(fields)
        return row
```

### Delta CDF Integration (Optional)
- When extractors consume Delta-backed sources, use the CDF TableProvider to process incremental changes.
- Standardize handling of `_change_type` (drop for base rows, retain for CDF outputs).

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/extract/row_builder.py` | `ExtractionRowBuilder` + helpers (implemented) |
| Modify | `src/extract/extractors/ast_extract.py` | Use row builder (remaining) |
| Modify | `src/extract/extractors/cst_extract.py` | Use row builder (remaining) |
| Modify | `src/extract/extractors/bytecode_extract.py` | Use row builder (remaining) |
| Modify | `src/extract/extractors/symtable_extract.py` | Use row builder (remaining) |

### ast-grep Recipes

**Discovery - Find manual row construction:**
```bash
# Find dict returns with file identity
ast-grep run -p 'return {
    "repo": $$$,
    "path": $$$,
    "file_id": $$$,
    $$$
}' -l python src/extract/

# Find all manual file identity assignments
ast-grep run -p '"file_id": file_ctx.file_id' -l python src/extract/
ast-grep run -p '"path": file_ctx.path' -l python src/extract/
```

**Verification:**
```bash
# After refactor, verify builder usage
ast-grep run -p 'row_builder_for_dataset($NAME)' -l python src/extract/
ast-grep run -p 'builder.from_file_context($$$)' -l python src/extract/
```

### cq Recipes

**Discovery:**
```bash
# Find row construction patterns
./scripts/cq calls _ast_row_from_walk --root .

# Analyze file context usage
./scripts/cq impact _ast_row_from_walk --param file_ctx --root .

# Find all extractor entry points
./scripts/cq calls extract_ast_file --root .
./scripts/cq calls extract_cst_file --root .
```

### Implementation Checklist
- [x] Create `ExtractionRowBuilder` class with identity/span helpers
- [x] Implement `from_file_context()` method
- [x] Implement `build_row()` / `build_row_with_span()` helpers
- [ ] Add schema-validation builder variant (optional extension)
- [ ] Refactor `_ast_row_from_walk()` to use builder
- [ ] Refactor CST row construction to use builder
- [ ] Refactor bytecode row construction to use builder
- [ ] Refactor symtable row construction to use builder
- [ ] Add unit tests for row builder usage/validation

### Decommissioning List
- Remove manual `file_id`, `path`, `file_sha256`, `repo` assignments from 5+ extractors
- Estimated reduction: 50+ lines + improved error detection

---

## 7. Evidence Metadata Normalization

**Status (2026-01-30): Partial.** Evidence metadata bundles exist in `schema_spec/evidence_metadata.py` and normalization helpers in `normalize/evidence_specs.py`, but join key inference from evidence coordinate systems is not yet implemented.

### Problem Statement
Join keys are manually specified per dataset when they could be derived from evidence metadata. The `EvidenceMetadataSpec.coordinate_system` field indicates the span semantics but isn't used to infer join keys.

### Current State
```python
# src/extract/templates.py
TEMPLATES: dict[str, ExtractorTemplate] = {
    "ast": ExtractorTemplate(
        extractor_name="ast",
        evidence_rank=4,
        metadata_extra=evidence_metadata(
            spec=EvidenceMetadataSpec(
                evidence_family="ast",
                coordinate_system="line_col",  # Not used for inference!
                ambiguity_policy="preserve",
                superior_rank=4,
            ),
        ),
    ),
}

# Join keys manually specified elsewhere:
# src/normalize/dataset_rows.py
DatasetRow(name="ast_files_v1", join_keys=("file_id",))  # Manual!
```

### Target Implementation
```python
# src/extract/evidence_inference.py
from __future__ import annotations

from enum import StrEnum


class CoordinateSystem(StrEnum):
    """Coordinate systems for evidence spans."""

    BYTES = "bytes"       # Byte offsets (bstart, bend)
    LINE_COL = "line_col" # Line/column (start_line, start_col, end_line, end_col)
    OFFSETS = "offsets"   # Bytecode offsets
    LINE = "line"         # Line-only (symtable)


# Join key inference from coordinate system
COORDINATE_JOIN_KEYS: dict[CoordinateSystem, tuple[str, ...]] = {
    CoordinateSystem.BYTES: ("file_id", "bstart", "bend"),
    CoordinateSystem.LINE_COL: ("file_id", "start_line", "start_col"),
    CoordinateSystem.OFFSETS: ("file_id", "offset"),
    CoordinateSystem.LINE: ("file_id", "lineno"),
}


def infer_join_keys_from_evidence(
    evidence_spec: EvidenceMetadataSpec,
) -> tuple[str, ...]:
    """Infer natural join keys from evidence metadata."""
    coord = CoordinateSystem(evidence_spec.coordinate_system)
    return COORDINATE_JOIN_KEYS.get(coord, ("file_id",))


def infer_canonical_sort_from_evidence(
    evidence_spec: EvidenceMetadataSpec,
) -> tuple[tuple[str, str], ...]:
    """Infer canonical sort order from evidence metadata."""
    coord = CoordinateSystem(evidence_spec.coordinate_system)
    match coord:
        case CoordinateSystem.BYTES:
            return (("path", "asc"), ("bstart", "asc"), ("bend", "asc"))
        case CoordinateSystem.LINE_COL:
            return (("path", "asc"), ("start_line", "asc"), ("start_col", "asc"))
        case CoordinateSystem.OFFSETS:
            return (("path", "asc"), ("offset", "asc"))
        case CoordinateSystem.LINE:
            return (("path", "asc"), ("lineno", "asc"))
    return (("path", "asc"),)
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/extract/evidence_inference.py` | Join key and sort inference from evidence |
| Modify | `src/extract/templates.py` | Use enriched templates |
| Modify | `src/normalize/dataset_rows.py` | Derive join_keys from evidence |

### ast-grep Recipes

**Discovery:**
```bash
# Find coordinate_system specifications
ast-grep run -p 'coordinate_system="$SYSTEM"' -l python src/extract/

# Find manual join_keys specifications
ast-grep run -p 'join_keys=($$$)' -l python src/normalize/

# Find evidence metadata usages
ast-grep run -p 'EvidenceMetadataSpec(' -l python src/extract/
```

**Verification:**
```bash
# After refactor, verify inference usage
ast-grep run -p 'infer_join_keys_from_evidence($$$)' -l python src/

# Find any remaining manual join_keys (should be overrides only)
ast-grep run -p 'join_keys=(' -l python src/normalize/
```

### cq Recipes

**Discovery:**
```bash
# Find evidence template usages
./scripts/cq calls TEMPLATES --root .

# Analyze metadata extraction
./scripts/cq calls evidence_metadata --root .
```

### Implementation Checklist
- [x] Evidence metadata bundle + normalize helpers implemented (`schema_spec/evidence_metadata.py`, `normalize/evidence_specs.py`)
- [ ] Create `CoordinateSystem` enum
- [ ] Create `COORDINATE_JOIN_KEYS` mapping
- [ ] Implement `infer_join_keys_from_evidence()`
- [ ] Implement `infer_canonical_sort_from_evidence()`
- [ ] Update `dataset_rows.py` to use inferred join keys
- [ ] Remove manual join_keys specifications where possible
- [ ] Add unit tests for inference logic

### Decommissioning List
- Remove manual `join_keys=` assignments (~10 instances)
- Estimated reduction: 30+ lines

---

## 8. Relationship Contract Spec Generator

**Status (2026-01-30): Partial.** Contract generation is data-driven in `schema_spec/relationship_specs.py`, and a spec-driven generator exists in `src/cpg/relationship_contracts.py`, but the runtime contract registry has not been consolidated onto a single source of truth.

### Problem Statement
Relationship contract specs in `relationship_specs.py` follow **identical patterns**. All 5 relationships have the same tie-breakers, same virtual fields, and predictable dedupe keys (only entity key varies).

### Current State (data-driven generation in place)
```python
# src/schema_spec/relationship_specs.py
def generate_relationship_contract_entry(
    data: RelationshipData,
    spec: DatasetSpec,
) -> ContractSpec:
    return make_contract_spec(
        table_spec=spec.table_spec,
        virtual=VirtualFieldSpec(fields=("origin",)),
        dedupe=DedupeSpecSpec(
            keys=_dedupe_keys_for_relationship(data),
            tie_breakers=STANDARD_RELATIONSHIP_TIE_BREAKERS,
            strategy="KEEP_FIRST_AFTER_SORT",
        ),
        canonical_sort=_canonical_sort_for_relationship(data),
        version=RELATIONSHIP_SCHEMA_VERSION,
    )


def relationship_contract_spec(ctx: SessionContext | None = None) -> ContractCatalogSpec:
    contracts: dict[str, ContractSpec] = {}
    for data in RELATIONSHIP_DATA:
        spec = dataset_spec_from_context(ctx, data.table_name)
        contracts[data.table_name] = generate_relationship_contract_entry(data, spec)
    return ContractCatalogSpec(contracts=contracts)
```

### Target Implementation
```python
# src/cpg/relationship_contracts.py (implemented)
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from cpg.relationship_specs import ALL_RELATIONSHIP_SPECS, QNameRelationshipSpec, RelationshipSpec
from schema_spec.system import (
    ContractCatalogSpec,
    ContractSpec,
    DedupeSpecSpec,
    SortKeySpec,
    VirtualFieldSpec,
    make_contract_spec,
)

if TYPE_CHECKING:
    from schema_spec.specs import TableSchemaSpec


STANDARD_RELATIONSHIP_TIE_BREAKERS: tuple[SortKeySpec, ...] = (
    SortKeySpec(column="score", order="descending"),
    SortKeySpec(column="confidence", order="descending"),
    SortKeySpec(column="task_priority", order="ascending"),
)


@dataclass(frozen=True)
class RelationshipContractData:
    table_name: str
    entity_id_cols: tuple[str, ...]
    extra_dedupe_keys: tuple[str, ...] = ()
    custom_tie_breakers: tuple[SortKeySpec, ...] | None = None


def generate_relationship_contract(config: RelationshipContractData) -> ContractSpec:
    dedupe_keys = config.entity_id_cols + ("symbol", "path", "bstart", "bend") + config.extra_dedupe_keys
    tie_breakers = config.custom_tie_breakers or STANDARD_RELATIONSHIP_TIE_BREAKERS
    canonical_sort = (
        SortKeySpec(column="path", order="ascending"),
        SortKeySpec(column="bstart", order="ascending"),
        *(SortKeySpec(column=k, order="ascending") for k in config.entity_id_cols),
    )
    return make_contract_spec(
        table_spec=_resolve_table_spec(config.table_name),
        virtual=VirtualFieldSpec(fields=("origin",)),
        dedupe=DedupeSpecSpec(
            keys=dedupe_keys,
            tie_breakers=tie_breakers,
            strategy="KEEP_FIRST_AFTER_SORT",
        ),
        canonical_sort=canonical_sort,
        version=RELATIONSHIP_SCHEMA_VERSION,
    )


def contract_catalog_from_specs() -> ContractCatalogSpec:
    contracts = {
        spec.output_view_name: generate_relationship_contract(contract_data_from_spec(spec))
        for spec in ALL_RELATIONSHIP_SPECS
    }
    return ContractCatalogSpec(contracts=contracts)
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/cpg/relationship_contracts.py` | Spec-driven contract generator (implemented) |
| Modify | `src/schema_spec/relationship_specs.py` | Already data-driven; consolidate with CPG generator (remaining) |
| Modify | `src/relspec/contracts.py` | Wire chosen generator into runtime contracts (remaining) |

### ast-grep Recipes

**Discovery:**
```bash
# Find identical tie-breaker patterns
ast-grep run -p 'SortKeySpec(column="score", order="descending")' -l python src/schema_spec/

# Find identical virtual field patterns
ast-grep run -p 'VirtualFieldSpec(fields=("origin",))' -l python src/schema_spec/

# Count contract spec creations
ast-grep run -p 'make_contract_spec(' -l python src/schema_spec/relationship_specs.py --json=stream | wc -l
```

**Verification:**
```bash
# After refactor, verify generator usage
ast-grep run -p 'generate_relationship_contract($$$)' -l python src/cpg/
ast-grep run -p 'generate_relationship_contract_entry($$$)' -l python src/schema_spec/

# Find any remaining manual contract creation
ast-grep run -p 'make_contract_spec(' -l python src/schema_spec/relationship_specs.py
# Should be 0 (all generated)
```

### cq Recipes

**Discovery:**
```bash
# Find contract spec usages
./scripts/cq calls relationship_contract_spec --root .
./scripts/cq calls contract_catalog_from_specs --root .

# Analyze make_contract_spec patterns
./scripts/cq calls make_contract_spec --root .
```

### Implementation Checklist
- [x] Create `STANDARD_RELATIONSHIP_TIE_BREAKERS` constant
- [x] Create `RelationshipContractData` dataclass
- [x] Implement `generate_relationship_contract()` function
- [x] Generate contracts from `ALL_RELATIONSHIP_SPECS`
- [ ] Consolidate contract generation (pick schema_spec vs cpg as source of truth)
- [ ] Wire generator into runtime contract registry
- [ ] Verify generated contracts match original specs
- [ ] Add unit tests comparing generated vs original

### Decommissioning List
- Remove remaining manual contract definitions once generator is wired
- Consolidate duplicate generators (schema_spec vs cpg)
- Estimated reduction: 200 lines + simplified maintenance

---

## 9. Contract Auto-Population from Catalog

### Problem Statement
Schema contracts are manually registered when they could be auto-populated from DataFusion's `information_schema`. This would enable schema discovery without hardcoded registries.

### Target Implementation
```python
# src/datafusion_engine/schema/catalog_contracts.py
from __future__ import annotations

from dataclasses import dataclass

from datafusion import SessionContext


@dataclass(frozen=True)
class TableKey:
    """Fully-qualified table identifier."""

    catalog: str
    schema: str
    name: str

    def qualified_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.name}"


def contract_registry_from_catalog(
    ctx: SessionContext,
    *,
    include_tables: set[TableKey] | None = None,
    exclude_tables: set[TableKey] | None = None,
) -> ContractRegistry:
    """Auto-populate contracts from DataFusion catalog.

    Queries information_schema to discover table schemas and builds
    contracts automatically.
    """
    registry = ContractRegistry()

    tables_df = ctx.sql(
        """
        SELECT table_catalog, table_schema, table_name
        FROM information_schema.tables
        """
    )
    table_keys = tuple(
        TableKey(
            catalog=row["table_catalog"],
            schema=row["table_schema"],
            name=row["table_name"],
        )
        for row in tables_df.collect()
    )

    for table_key in table_keys:
        if include_tables and table_key not in include_tables:
            continue
        if exclude_tables and table_key in exclude_tables:
            continue

        # Get schema from catalog
        schema = ctx.table(table_key.qualified_name()).schema()

        # Extract constraint metadata if available
        constraints = _constraints_from_table_metadata(ctx, table_key)

        # Build contract
        contract = SchemaContract.from_arrow_schema(
            table_name=table_key.name,
            schema=schema,
            constraints=constraints,
        )
        registry.register_contract(contract)

    return registry


def _constraints_from_table_metadata(
    ctx: SessionContext,
    table_key: TableKey,
) -> TableConstraints | None:
    """Extract constraints from table provider metadata."""
    cols_df = ctx.sql(
        f"""
        SELECT column_name, is_nullable
        FROM information_schema.columns
        WHERE table_catalog = '{table_key.catalog}'
          AND table_schema = '{table_key.schema}'
          AND table_name = '{table_key.name}'
        """
    )
    not_null = tuple(
        row["column_name"]
        for row in cols_df.collect()
        if row["is_nullable"] == "NO"
    )
    return TableConstraints(not_null=not_null) if not_null else None
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/datafusion_engine/schema/catalog_contracts.py` | Catalog-driven contract population |
| Modify | `src/datafusion_engine/schema/contracts.py` | Add `from_arrow_schema()` classmethod |

### Delta-aware Contract Enrichment (Optional but Recommended)
- Treat Delta as a first-class table kind in the registry (location + storage options + snapshot selector).
- When a table is backed by Delta, enrich the contract with **snapshot metadata** (resolved version, schema JSON/hash, protocol features) to make schema contracts reproducible.
- Store the **resolved version** for time-travel selectors (timestamp pins must resolve to a version at registration time).
- If Delta constraints / invariants / generated columns exist, reflect them in the contract metadata (even if enforcement remains in Delta’s write path).

### ast-grep Recipes

**Discovery:**
```bash
# Find manual contract registrations
ast-grep run -p 'registry.register_contract($CONTRACT)' -l python src/

# Find information_schema queries
ast-grep run -p 'information_schema.tables' -l python src/
ast-grep run -p 'information_schema.columns' -l python src/
```

### cq Recipes

**Discovery:**
```bash
# Find contract registry usages
./scripts/cq calls ContractRegistry --root .

# Analyze schema validation patterns
./scripts/cq calls validate_schema_contract --root .
```

### Implementation Checklist
- [ ] Introduce `TableKey` for qualified table identification
- [ ] Create `contract_registry_from_catalog()` function
- [ ] Implement `_constraints_from_table_metadata()` helper (qualified by catalog/schema)
- [ ] Add `SchemaContract.from_arrow_schema()` classmethod
- [ ] Integrate with session initialization
- [ ] Add caching for contract lookup
- [ ] Add unit tests for catalog-driven contracts
- [ ] (Optional) Add Delta snapshot/constraint enrichment when table provider is Delta-backed

### Decommissioning List
- Reduce manual contract registration code
- Estimated impact: Enables dynamic schema discovery (architectural improvement)

---

## 10. Nested Type Builder Framework

**Status (2026-01-30): Partial.** `src/schema_spec/nested_types.py` provides the builder utilities, but `schema/registry.py` still defines nested structs manually.

### Problem Statement
Nested struct types (AST_NODE_T, CST_REF_T, etc.) are manually defined with repeated patterns. These could be built programmatically from field templates.

### Target Implementation
```python
# src/schema_spec/nested_types.py (implemented)
from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa


@dataclass
class NestedTypeBuilder:
    """Fluent builder for nested Arrow types."""

    fields: list[pa.Field] = field(default_factory=list)

    def add_field(self, name: str, dtype: pa.DataType, *, nullable: bool = True) -> NestedTypeBuilder:
        self.fields.append(pa.field(name, dtype, nullable=nullable))
        return self

    def add_struct(self, name: str, builder: NestedTypeBuilder, *, nullable: bool = True) -> NestedTypeBuilder:
        self.fields.append(pa.field(name, builder.build_struct(), nullable=nullable))
        return self

    def build_struct(self) -> pa.StructType:
        return pa.struct(self.fields)


def span_struct_builder() -> NestedTypeBuilder:
    builder = NestedTypeBuilder()
    builder.add_field("start_line", pa.int32())
    builder.add_field("start_col", pa.int32())
    builder.add_field("end_line", pa.int32())
    builder.add_field("end_col", pa.int32())
    builder.add_field("byte_start", pa.int64())
    builder.add_field("byte_len", pa.int64())
    return builder


def span_struct_type() -> pa.StructType:
    return span_struct_builder().build_struct()
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/schema_spec/nested_types.py` | Nested type builder framework (implemented) |
| Modify | `src/datafusion_engine/schema/registry.py` | Use builder for nested types (remaining) |

### ast-grep Recipes

**Discovery:**
```bash
# Find nested struct definitions
ast-grep run -p 'pa.struct([
    $$$FIELDS
])' -l python src/datafusion_engine/schema/

# Count nested type definitions
ast-grep run -p '$NAME_T = pa.struct(' -l python src/datafusion_engine/schema/ --json=stream | wc -l
```

### Implementation Checklist
- [x] Create `NestedTypeBuilder` utilities in `schema_spec/nested_types.py`
- [x] Implement builder helpers (structs, lists, maps) + span templates
- [ ] Refactor `AST_NODE_T`, `CST_REF_T`, etc. to use builder
- [ ] Verify type compatibility with existing code

### Decommissioning List
- Remove 15+ manual nested struct definitions
- Estimated reduction: 150+ lines

---

## 11. Span Field Templating

**Status (2026-01-30): Partial.** `src/schema_spec/span_fields.py` implements byte-span prefix helpers, but coordinate-system-aware span metadata templating (line/col, end_exclusive) is not implemented.

### Problem Statement
Span field definitions repeat across schemas with different coordinate system configurations (line_base, col_unit, end_exclusive). These could be templated from evidence metadata.

### Target Implementation
```python
# src/schema_spec/span_fields.py (implemented)
from __future__ import annotations

from typing import Literal

import pyarrow as pa

from datafusion_engine.arrow import interop
from schema_spec.field_spec import FieldSpec


SpanPrefix = Literal[
    "",
    "call_",
    "name_",
    "def_",
    "stmt_",
    "alias_",
    "callee_",
    "container_def_",
    "owner_def_",
]


def span_field_names(prefix: SpanPrefix = "") -> tuple[str, str]:
    normalized = f"{prefix}_" if prefix and not prefix.endswith("_") else prefix
    return (f"{normalized}bstart", f"{normalized}bend")


def make_span_field_specs(prefix: SpanPrefix = "") -> tuple[FieldSpec, FieldSpec]:
    bstart_name, bend_name = span_field_names(prefix)
    return (
        FieldSpec(name=bstart_name, dtype=interop.int64()),
        FieldSpec(name=bend_name, dtype=interop.int64()),
    )


def make_span_pa_fields(prefix: SpanPrefix = "") -> tuple[pa.Field, pa.Field]:
    bstart_name, bend_name = span_field_names(prefix)
    return (
        pa.field(bstart_name, pa.int64()),
        pa.field(bend_name, pa.int64()),
    )
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/schema_spec/span_fields.py` | Byte-span prefix helpers (implemented) |
| Modify | `src/datafusion_engine/schema/registry.py` | Adopt span templates/metadata-driven spans (remaining) |

### Implementation Checklist
- [x] Implement byte-span prefix helpers in `schema_spec/span_fields.py`
- [ ] Create `SpanFieldConfig` dataclass for line/col metadata
- [ ] Create `SPAN_CONFIGS_BY_EVIDENCE` registry
- [ ] Implement `span_fields_for_evidence()` function
- [ ] Implement `span_struct_for_evidence()` for nesting
- [ ] Refactor schemas to use templated span fields
- [ ] Verify metadata consistency

### Decommissioning List
- Remove 20+ manual span field definitions
- Estimated reduction: 80+ lines

---

## 12. Extraction Schema Derivation

**Status (2026-01-30): Partial.** Derivation utilities exist in `src/extract/schema_derivation.py`, but `schema/registry.py` still uses hardcoded schemas and does not call the derivation flow.

### Problem Statement
Extraction schemas in `registry.py` are **2,000+ lines** of literal definitions when they could be derived from the **SessionContext catalog** and extraction metadata. Per repo policy, schema authority should live in DataFusion’s catalog/information_schema, not a Python-side registry.

### Target Implementation
```python
# src/extract/schema_derivation.py (implemented)
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa

from schema_spec.file_identity import file_identity_field_specs
from schema_spec.specs import FieldBundle, FieldSpec, TableSchemaSpec

if TYPE_CHECKING:
    from datafusion import SessionContext


@dataclass
class ExtractionSchemaBuilder:
    """Fluent builder for extraction table schemas."""

    _name: str
    _version: int = 1
    _bundles: list[FieldBundle] = field(default_factory=list)
    _fields: list[FieldSpec] = field(default_factory=list)

    def with_file_identity(self, *, include_sha256: bool = True) -> ExtractionSchemaBuilder:
        specs = file_identity_field_specs(include_sha256=include_sha256)
        self._bundles.append(FieldBundle(name="file_identity", fields=specs))
        return self

    def build(self) -> TableSchemaSpec:
        return TableSchemaSpec(name=self._name, version=self._version, bundles=tuple(self._bundles))


def derive_extraction_schema(
    extractor_name: str,
    source_table: str,
    *,
    ctx: SessionContext,
) -> pa.Schema:
    """Derive a schema from a DataFusion source table + standard bundles."""
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/extract/schema_derivation.py` | Extraction schema builder + `derive_extraction_schema()` (implemented) |
| Modify | `src/datafusion_engine/schema/registry.py` | Use derived schemas as primary source (remaining) |
| Modify | `src/datafusion_engine/session/runtime.py` | Expose schema registration helper (optional) |

### ast-grep Recipes

**Discovery:**
```bash
# Count schema definitions
ast-grep run -p '$NAME_SCHEMA = pa.schema([
    $$$FIELDS
])' -l python src/datafusion_engine/schema/registry.py --json=stream | wc -l

# Find repeated field patterns
ast-grep run -p 'pa.field("nodes", pa.list_($TYPE))' -l python src/datafusion_engine/schema/
```

**Verification:**
```bash
# Verify schema derivation is used
ast-grep run -p 'derive_extraction_schema($$$)' -l python src/datafusion_engine/

# Verify schema registration helper is used
ast-grep run -p 'register_schema_table($$$)' -l python src/datafusion_engine/
```

### cq Recipes

**Discovery:**
```bash
# Find schema usages
./scripts/cq calls AST_FILES_SCHEMA --root .
./scripts/cq calls CST_FILES_SCHEMA --root .

# Analyze extraction metadata patterns
./scripts/cq calls extract_metadata --root .
```

### Implementation Checklist
- [x] Implement `ExtractionSchemaBuilder` and bundle composition helpers
- [x] Create `derive_extraction_schema()` in `extract/schema_derivation.py`
- [ ] Wire derivation into `schema/registry.py` as primary source
- [ ] Expose/standardize schema registration helper (optional)
- [ ] Verify derived schemas match legacy definitions (schema-by-schema diff)
- [ ] Migrate primary usage to derived schemas
- [ ] Remove legacy schema fallback once parity is verified

### Decommissioning List
- Retire `_LEGACY_SCHEMAS` after catalog-derived schemas reach parity
- Remove per-schema hardcoded definitions from `schema/registry.py`

### Decommissioning List
- Remove 2,000+ lines of literal schema definitions (after validation)
- Replace with ~200 lines of derivation logic
- Estimated reduction: 1,800+ lines

---

## Cross-Scope Dependencies

| Scope | Depends On | Notes |
|-------|------------|-------|
| **#0a Vestigial Deletion** | None | Zero-risk, immediate cleanup |
| **#0b Policy Extraction** | None | Decouples behavior from specs |
| **#0c @cache Replacement** | #0b | Requires behavior extraction first |
| **#0d relationship_specs Collapse** | #0a, #0b, #0c | Requires clean specs |
| **#0e SessionContext Hardening** | None | Stabilizes schema derivation surfaces |
| #1 Relationship Spec DSL | #0d | Builds on collapsed relationship_specs |
| #2 Schema Inference | #0c, #0e | Uses catalog for spec lookups + hardened SessionContext |
| #3 File Identity | #0a | Uses file identity module |
| #4 View Builder DSL | #3 File Identity | Uses canonical file identity |
| #5 Node Family Defaults | #3 File Identity | Uses file identity defaults |
| #6 Extraction Row Builder | #3 File Identity, #12 Extraction Schema | Uses canonical types |
| #7 Evidence Normalization | None | Foundation |
| #8 Relationship Contract Gen | #1 Relationship Spec, #0d | Uses spec definitions |
| #9 Contract Auto-Population | #2 Schema Inference, #0c, #0e | Uses inferred schemas + catalog |
| #10 Nested Type Builder | #3 File Identity, #11 Span Templates | Uses canonical bundles |
| #11 Span Templates | #7 Evidence Normalization | Uses coordinate config |
| #12 Extraction Schema | #0e, #3, #10, #11 | Composes all foundations |

---

## Recommended Implementation Order

**Phase 0: Schema Spec Decoupling (PREREQUISITE - Unlocks Everything)**
1. **#0a Delete Vestigial Modules** - Zero-risk deletion of 185 lines
2. **#0b Extract Policy Behaviors** - Decouple behavior from specs
3. **#0c Replace @cache Singletons** - Enable testing + per-context configs
4. **#0d Collapse relationship_specs.py** - 67% reduction (397 → 130 lines)
5. **#0e SessionContext Schema Hardening** - Deterministic schema behavior for inference

**Phase 1: Foundations (No Dependencies Beyond #0)**
6. File Identity Canonical Type (#3) - Foundation for all schema work
7. Evidence Metadata Normalization (#7) - Foundation for inference
8. Relationship Spec Declarative DSL (#1) - Builds on #0d, immediate 180-line reduction

**Phase 2: Schema Inference (Builds on Foundations)**
9. Span Field Templating (#11) - Uses evidence metadata
10. Nested Type Builder Framework (#10) - Uses file identity + spans
11. Schema Inference from DataFusion Plans (#2) - Core capability

**Phase 3: Generators (Uses Foundations + Inference)**
12. Node Family Spec Defaults (#5) - Quick win
13. Relationship Contract Spec Generator (#8) - Uses relationship specs
14. View Builder DSL (#4) - Major LOC reduction

**Phase 4: Integration (Full Stack)**
15. Contract Auto-Population from Catalog (#9) - Uses schema inference
16. Extraction Row Builder (#6) - Uses schemas + validation
17. Extraction Schema Derivation (#12) - Capstone; uses everything

---

## Summary Statistics

| Metric | Value |
|--------|-------|
| Total Scopes | **17** (5 prerequisite + 12 main) |
| Files to Create | 15 |
| Files to Modify | 30+ |
| Files to Delete | 3 (vestigial modules) |
| Estimated Code Reduction | **5,000-6,500 lines** |
| Lines of Specs → Lines of Generators | 5,500 → 1,200 |
| New Relationship: Lines Required | 150 → 5 |
| New Extractor: Lines Required | 200 → 20 |

### Prerequisite Phase Impact

| Scope | Before | After | Reduction |
|-------|--------|-------|-----------|
| #0a Vestigial Deletion | 185 lines | 0 lines | **185 lines (100%)** |
| #0b Policy Extraction | 115 lines in specs | 115 lines in policies | **Architecture improvement** |
| #0c @cache Replacement | 8 @cache decorators | 1 SpecCatalog class | **Testing enabled** |
| #0d relationship_specs | 397 lines | 130 lines | **267 lines (67%)** |
| #0e SessionContext Hardening | Ad-hoc defaults | 1 schema profile module | **Deterministic schema** |
| **Prerequisite Total** | | | **~450 lines + architecture** |

---

## Verification Commands Summary

### Python Analysis (cq)
```bash
# Impact analysis for major refactors
./scripts/cq calls <function_name> --root .
./scripts/cq impact <function> --param <param> --root .
./scripts/cq sig-impact <function> --to "<new_signature>" --root .

# Check for import cycles before adding new modules
./scripts/cq imports --cycles --root src/<module>

# Side effects analysis for new foundation modules
./scripts/cq side-effects --root src/schema_spec

# Exception handling audit (ensure no broad excepts slip in)
./scripts/cq exceptions --root src/

# Bytecode surface for regression checking
./scripts/cq bytecode-surface <file> --show globals,attrs
```

### Structural Analysis (ast-grep)
```bash
# Find patterns for consolidation
ast-grep run -p '<pattern>' -l python src/

# Count occurrences
ast-grep run -p '<pattern>' -l python src/ --json=stream | wc -l

# Verify refactor completeness (project rules)
ast-grep scan --config sgconfig.yml --filter '<rule_id_regex>' src/

# Verify refactor completeness (inline rules)
ast-grep scan --inline-rules "<yaml>" src/

# Preview rewrites
ast-grep run -p '<old>' -r '<new>' -l python src/
```

### Test Suite
```bash
# Unit tests during development
uv run pytest tests/unit/ -v

# Full test excluding e2e
uv run pytest tests/ -m "not e2e"

# Type checking
uv run pyright --warnings --pythonversion=3.13
uv run pyrefly check

# Linting
uv run ruff check --fix
```

---

## Appendix: Core Insight

The transformation from **declarative to derived** mirrors what relspec already achieves for dependencies:

```
relspec (existing):
  DataFusion plan -> extract_lineage() -> InferredDeps -> rustworkx graph

schema inference (proposed):
  DataFusion plan -> infer_output_schema() -> SchemaInference -> contract validation
```

The same plan-walking infrastructure, the same fingerprinting for caching, the same graceful degradation patterns. This isn't new architecture - it's extending proven patterns to new domains.

**The user's vision - "views should flow naturally from relationship joins between nodes" - is achievable because:**
1. Relationship semantics can be captured in 5-line specs
2. Output schemas can be inferred from plans
3. Join keys can be derived from evidence metadata
4. Contracts can be generated from minimal configs

The codebase becomes truly principle-driven: source data + view definitions -> everything else derived.

---

## Appendix: Delta Registry Schema (Concrete Columns + Snapshot Metadata)

This appendix defines a minimal but sufficient **Delta registry table schema** to support deterministic planning, schema contracts, and time-travel reproducibility. It is designed to power #9 (contract auto-population) and the Delta enrichment path.

### Registry Table: `dataset_registry`

| Column | Type | Description |
|--------|------|-------------|
| `catalog` | `Utf8` | DataFusion catalog name |
| `schema` | `Utf8` | DataFusion schema name |
| `table_name` | `Utf8` | Logical table name |
| `kind` | `Utf8` | `delta` \| `parquet` \| `arrow` \| `view` |
| `location` | `Utf8` | Delta table URI / object store path |
| `storage_options_json` | `Utf8` | JSON-encoded storage options (log store auth, etc.) |
| `schema_json` | `Utf8` | Canonical Arrow schema JSON (for contract checks) |
| `schema_hash` | `Utf8` | Hash of `schema_json` (fast equality check) |
| `created_at` | `Timestamp` | Registration time |
| `updated_at` | `Timestamp` | Last update time |
| `tags_json` | `Utf8` | Arbitrary metadata tags (optional) |

**Key**: `(catalog, schema, table_name)` unique.

### Delta Snapshot Metadata: `dataset_delta_snapshots`

| Column | Type | Description |
|--------|------|-------------|
| `catalog` | `Utf8` | DataFusion catalog name |
| `schema` | `Utf8` | DataFusion schema name |
| `table_name` | `Utf8` | Logical table name |
| `resolved_version` | `Int64` | Version resolved at registration time |
| `selector_version` | `Int64` | Version selector (nullable) |
| `selector_timestamp` | `Utf8` | RFC3339 timestamp selector (nullable) |
| `protocol_min_reader` | `Int32` | Delta protocol minReaderVersion |
| `protocol_min_writer` | `Int32` | Delta protocol minWriterVersion |
| `protocol_reader_features` | `Utf8` | JSON list of reader features (nullable) |
| `protocol_writer_features` | `Utf8` | JSON list of writer features (nullable) |
| `schema_json` | `Utf8` | Schema JSON from Delta log (snapshot-scoped) |
| `schema_hash` | `Utf8` | Hash of snapshot schema JSON |
| `snapshot_hash` | `Utf8` | Hash over snapshot identity (version + active files) |
| `log_retention` | `Utf8` | `delta.logRetentionDuration` (optional) |
| `deleted_file_retention` | `Utf8` | `delta.deletedFileRetentionDuration` (optional) |
| `captured_at` | `Timestamp` | Snapshot capture time |

**Key**: `(catalog, schema, table_name, resolved_version)` unique.

### Contract Enrichment Surface (stored in `SchemaContract.metadata`)

Recommended metadata keys for Delta-backed contracts:
- `delta.resolved_version`
- `delta.selector_version`
- `delta.selector_timestamp`
- `delta.protocol.min_reader`
- `delta.protocol.min_writer`
- `delta.protocol.reader_features`
- `delta.protocol.writer_features`
- `delta.schema_hash`
- `delta.snapshot_hash`

### Minimal Registration Procedure (deterministic)
1. Resolve Delta snapshot (version or timestamp).
2. Register the table provider with `resolved_version`.
3. Capture `schema_json` and `schema_hash` from the snapshot.
4. Insert/update `dataset_registry` and `dataset_delta_snapshots`.
5. Build a `SchemaContract` using DataFusion `DESCRIBE <query>` and attach Delta metadata.

This gives deterministic schema contracts, stable time-travel reproducibility, and a direct bridge to DataFusion catalog-driven introspection.
