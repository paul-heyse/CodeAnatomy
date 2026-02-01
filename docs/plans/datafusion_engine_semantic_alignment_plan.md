# DataFusion Engine Semantic Alignment Plan

> Consolidate `src/datafusion_engine/views/` with the semantic-first architecture in `src/semantics/`, eliminating redundant view definition patterns and establishing clear separation of concerns.

## Executive Summary

| Submodule | Files | Lines | Current Role | Migration Target |
|-----------|-------|-------|--------------|------------------|
| `views/registry.py` | 1 | ~1200 | Legacy hand-coded view expressions | **Delete** - superseded by semantic specs |
| `views/dsl.py` | 1 | ~350 | Fluent view expression API | **Delete** - superseded by semantic specs |
| `views/dsl_views.py` | 1 | ~200 | Reusable DSL builders | **Delete** - superseded by semantic catalog |
| `views/view_spec.py` | 1 | ~650 | Declarative column transforms | **Evaluate** - partial overlap with semantic specs |
| `views/registry_specs.py` | 1 | ~1000 | View node factory | **Simplify** - thin bridge to semantic catalog |
| `views/graph.py` | 1 | ~1200 | View graph registration | **Keep** - execution authority |

**Key Insights:**
1. **Semantic module is definition authority** - All view logic (what to compute) lives in `semantics/`.
2. **DataFusion engine is execution authority** - Registration, caching, materialization lives in `datafusion_engine/`.
3. **Redundant patterns exist** - `registry.py`, `dsl.py`, `dsl_views.py` duplicate logic now in semantic specs.
4. **Clean boundary needed** - `registry_specs.py` should be a thin adapter, not a view definition layer.
5. **Catalogs must unify semantic + extract outputs** for deterministic planning and validation.
6. **Semantic runtime config should be owned by semantics** with datafusion_engine adapters.

### Status Update (2026-02-01)

- Legacy view registry/DSL/spec modules are removed (`views/registry.py`, `views/dsl.py`, `views/dsl_views.py`, `views/view_spec.py`, `views/view_specs_catalog.py`).
- Canonical view registration entrypoint is now `views/registration.py` + `views/registry_specs.py`.
- All remaining references to legacy modules below are historical and should be treated as deprecated context.

---

## Architecture Overview

### Current State (Problematic)

```
src/datafusion_engine/views/
├── registry.py          ← Hand-coded VIEW_SELECT_REGISTRY (LEGACY)
├── dsl.py               ← ViewExprBuilder fluent API (LEGACY)
├── dsl_views.py         ← DSL-based view builders (LEGACY)
├── view_spec.py         ← ViewProjectionSpec, ColumnTransform (OVERLAP)
├── registry_specs.py    ← ViewNode factory (BRIDGE + LOGIC MIXED)
├── graph.py             ← register_view_graph() (EXECUTION ONLY)
└── bundle_extraction.py ← Lineage helpers (KEEP)

src/semantics/
├── compiler.py          ← SemanticCompiler (10 semantic rules)
├── specs.py             ← SpanBinding, IdDerivation, RelationshipSpec
├── spec_registry.py     ← SEMANTIC_TABLE_SPECS, RELATIONSHIP_SPECS
└── catalog/
    ├── view_builders.py     ← VIEW_BUILDERS, DataFrameBuilder
    ├── analysis_builders.py ← Analysis view builders
    └── dataset_rows.py      ← SemanticDatasetRow definitions
```

**Problem:** View definitions exist in three places:
1. `semantics/spec_registry.py` (specs) + `semantics/catalog/view_builders.py` (builders)
2. `datafusion_engine/views/registry.py` (legacy hand-coded expressions)
3. `datafusion_engine/views/view_spec.py` (column transform specs)

### Target State (Clean Separation)

```
src/datafusion_engine/
├── views/
│   ├── graph.py             ← EXECUTION: register_view_graph(), ViewNode
│   ├── registry_specs.py    ← BRIDGE: view_graph_nodes() creates ViewNodes from semantic builders
│   ├── bundle_extraction.py ← HELPER: lineage, UDF extraction
│   ├── artifacts.py         ← HELPER: artifact serialization
│   └── view_specs_catalog.py ← HELPER: ViewSpec metadata (if needed)
├── dataset/
│   ├── registry.py          ← DatasetSpec + DatasetLocation registry
│   └── semantic_catalog.py  ← BRIDGE: semantic + extract dataset locations
├── semantics_runtime.py     ← ADAPTER: builds SemanticRuntimeConfig from DataFusion profile
└── schema/
    └── contracts.py         ← Semantic-aware contracts

src/semantics/                ← DEFINITION AUTHORITY
├── compiler.py              ← SemanticCompiler (what views compute)
├── specs.py                 ← Declarative spec definitions
├── spec_registry.py         ← Spec registries
├── runtime.py               ← SemanticRuntimeConfig (definition owned here)
└── catalog/
    ├── view_builders.py     ← All DataFrameBuilder functions
    ├── analysis_builders.py ← Analysis-specific builders
    └── dataset_rows.py      ← Dataset metadata
```

**Clear separation:**
- **Semantics defines** what views compute (specs, builders, rules)
- **DataFusion engine executes** view registration (graph, caching, materialization)
- **DataFusion engine adapts** runtime configuration to semantic needs

---

## Scope Index

1. **Scope 1** - Delete Legacy View Registry (`registry.py`)
2. **Scope 2** - Delete View DSL Layer (`dsl.py`, `dsl_views.py`)
3. **Scope 3** - Evaluate ViewProjectionSpec Overlap (`view_spec.py`)
4. **Scope 4** - Simplify registry_specs.py to Pure Bridge
5. **Scope 5** - Update Schema Contract Integration
6. **Scope 6** - Runtime Decoupling via SemanticRuntimeConfig
7. **Scope 7** - Unified Dataset Catalog for Semantic + Extract Outputs
8. **Scope 8** - Compatibility Shims and Deprecation Window
9. **Scope 9** - Naming Policy & CDF Capability Alignment

---

## Scope 1 - Delete Legacy View Registry

### Status
**Partial** (exports shimmed, but `registry.py` still present and imported)

### Goal
Remove `views/registry.py` which contains ~1200 lines of hand-coded view expressions that are now superseded by semantic specs and view builders.

### Representative Pattern (Current - To Delete)

```python
# src/datafusion_engine/views/registry.py (LEGACY - DELETE)
"""Dynamic view registry for DataFusion schema views."""

# Hand-coded expression tuples - NO LONGER NEEDED
_VIEW_SELECT_EXPRS: dict[str, tuple[Expr, ...]] = {
    "ast_call_attrs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ast_id").alias("ast_id"),
        col("parent_ast_id").alias("parent_ast_id"),
        col("func_kind").alias("func_kind"),
        col("func_name").alias("func_name"),
        (col("kv"))["key"].alias("attr_key"),
        (col("kv"))["value"].alias("attr_value"),
    ),
}

VIEW_SELECT_REGISTRY: ImmutableRegistry[str, tuple[Expr, ...]] = (
    ImmutableRegistry(_VIEW_SELECT_EXPRS)
)
```

### Replacement Pattern (Semantic Specs)

```python
# src/semantics/spec_registry.py (CURRENT APPROACH - KEEP)
"""Semantic table and relationship specifications."""

from semantics.specs import SemanticTableSpec, SpanBinding, IdDerivation

SEMANTIC_TABLE_SPECS: tuple[SemanticTableSpec, ...] = (
    SemanticTableSpec(
        table="cst_refs",
        primary_span=SpanBinding("ref_bstart", "ref_bend"),
        entity_id=IdDerivation(
            out_col="entity_id",
            namespace="cst_ref",
            path_col="path",
            start_col="ref_bstart",
            end_col="ref_bend",
        ),
        text_cols=("ref_name",),
    ),
)
```

### Target Files
- Remove: `src/datafusion_engine/views/registry.py`

### Deletions
- Remove: `src/datafusion_engine/views/registry.py` (entire file)
- Remove from `__init__.py`: All exports from `registry.py`

### Implementation Checklist
- [ ] Audit all imports of `VIEW_SELECT_REGISTRY` across codebase
- [ ] Verify no callers depend on `_view_exprs()` or related helpers
- [ ] Confirm `registry_specs.py` provides all needed views via semantic builders
- [ ] Delete `src/datafusion_engine/views/registry.py`
- [x] Update `src/datafusion_engine/views/__init__.py` exports (compat shim in place)
- [ ] Run view registration tests to verify no regressions

---

## Scope 2 - Delete View DSL Layer

### Status
**Completed** (dsl files removed; compatibility shim active)

### Goal
Remove `views/dsl.py` and `views/dsl_views.py` which provide fluent APIs that are superseded by semantic specs and the SemanticCompiler.

### Representative Pattern (Current - To Delete)

```python
# src/datafusion_engine/views/dsl.py (LEGACY - DELETE)
"""View expression DSL for declarative view construction."""

@dataclass(frozen=True, slots=True)
class SpanExprs:
    span_col: str = "span"

    def lineno(self) -> Expr:
        return _arrow_cast(((self._span())["start"])["line0"] + lit(1), "Int64")

@dataclass
class ViewExprBuilder:
    _exprs: list[Expr] = field(default_factory=list)

    def add_identity_cols(self, *cols: str) -> Self:
        for c in cols:
            self._exprs.append(col(c).alias(c))
        return self
```

### Replacement Pattern (Semantic Approach)

```python
# src/semantics/compiler.py (CURRENT APPROACH - KEEP)
"""Semantic compiler with composable rules."""

class SemanticCompiler:
    def normalize(self, table_name: str) -> DataFrame:
        info = self._get_table_info(table_name)
        df = info.df

        if info.sem.has_path and info.sem.has_span:
            df = self._derive_entity_id(df, info.sem)
        if info.sem.span_start_col and info.sem.span_end_col:
            df = self._derive_span(df, info.sem)
        return df
```

### Target Files
- Remove: `src/datafusion_engine/views/dsl.py`
- Remove: `src/datafusion_engine/views/dsl_views.py`

### Deletions
- Remove: `src/datafusion_engine/views/dsl.py` (entire file)
- Remove: `src/datafusion_engine/views/dsl_views.py` (entire file)
- Remove from `__init__.py`: DSL exports

### Implementation Checklist
- [x] Audit all imports of `ViewExprBuilder` and `SpanExprs`
- [ ] Verify all DSL view builders have semantic equivalents
- [ ] Migrate any unique functionality to semantic layer
- [x] Delete `src/datafusion_engine/views/dsl.py`
- [x] Delete `src/datafusion_engine/views/dsl_views.py`
- [x] Update `src/datafusion_engine/views/__init__.py` exports
- [ ] Run view registration tests to verify no regressions

---

## Scope 3 - Evaluate ViewProjectionSpec Overlap

### Status
**Not Started** (view_spec + view_specs_catalog still active)

### Goal
Evaluate `views/view_spec.py` for overlap with semantic specs, migrate unique functionality, and simplify or remove redundant patterns.

### Representative Pattern (Current)

```python
# src/datafusion_engine/views/view_spec.py (EVALUATE FOR OVERLAP)
"""Declarative view projection specifications."""

class ColumnTransformKind(StrEnum):
    PASSTHROUGH = "passthrough"
    RENAME = "rename"
    CAST = "cast"
    STRUCT_FIELD = "struct_field"
    MAP_EXTRACT = "map_extract"
    METADATA_EXTRACT = "metadata"

@dataclass(frozen=True)
class ViewProjectionSpec:
    name: str
    base_table: str
    include_file_identity: bool = True
    include_span_struct: bool = False
    transforms: tuple[ColumnTransform, ...] = ()
```

### Comparison with Semantic Specs

```python
# src/semantics/specs.py (SEMANTIC APPROACH)
"""Declarative semantic specifications."""

@dataclass(frozen=True)
class SpanBinding:
    start_col: str
    end_col: str
    unit: str = "byte"
    canonical_start: str = "bstart"
    canonical_end: str = "bend"

@dataclass(frozen=True)
class IdDerivation:
    out_col: str
    namespace: str
    path_col: str = "path"
    start_col: str = "bstart"
    end_col: str = "bend"
```

### Target Files
- Evaluate: `src/datafusion_engine/views/view_spec.py`
- Potential migration to: `src/semantics/specs.py` or `src/semantics/catalog/projections.py`

### Deletions
- Remove: `ColumnTransformKind.PASSTHROUGH`, `RENAME`, `CAST` (redundant)
- Remove: `ViewProjectionSpec.include_file_identity`, `include_span_struct` (use semantic patterns)
- Keep only struct/map/metadata extraction if genuinely unique

### Implementation Checklist
- [ ] Audit all usages of `ViewProjectionSpec` and `ColumnTransform`
- [ ] Identify use cases not covered by semantic specs
- [ ] Migrate unique patterns to semantic layer or document why they remain
- [ ] Remove redundant transform kinds
- [ ] Simplify or delete `view_spec.py` accordingly
- [ ] Update tests for any migrated functionality

---

## Scope 4 - Simplify registry_specs.py to Pure Bridge

### Status
**Partial** (relation_output + CPG projection moved; registry_specs still mixed)

### Goal
Transform `registry_specs.py` into a pure bridge that only creates `ViewNode` tuples from semantic catalog builders.

### Representative Pattern (Target - Pure Bridge)

```python
# src/datafusion_engine/views/registry_specs.py (TARGET - PURE BRIDGE)
"""View node factory - bridge between semantic catalog and view graph."""

from datafusion_engine.views.graph import ViewNode


def view_graph_nodes(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    runtime_profile: DataFusionRuntimeProfile | None = None,
    stage: Literal["all", "pre_cpg", "cpg"] = "all",
) -> tuple[ViewNode, ...]:
    nodes: list[ViewNode] = []

    if stage in {"all", "pre_cpg"}:
        nodes.extend(_semantic_view_nodes(ctx, snapshot, runtime_profile))
    if stage in {"all", "cpg"}:
        nodes.extend(_cpg_view_nodes(ctx, snapshot, runtime_profile))

    return tuple(nodes)
```

### Migration Requirements

```python
# src/semantics/catalog/projections.py (ADDED)
"""Relation output + CPG projections."""

def cpg_nodes_builder() -> DataFrameBuilder:
    def _builder(ctx: SessionContext) -> DataFrame:
        return ctx.table("union_nodes_v1")
    return _builder
```

### Target Files
- Modify: `src/datafusion_engine/views/registry_specs.py`
- Modify: `src/semantics/catalog/view_builders.py`
- Add: `src/semantics/catalog/projections.py`

### Deletions
- Remove from `registry_specs.py`: inline projection logic, relation output specs, redundant metadata handling

### Implementation Checklist
- [ ] Identify all view definition logic in `registry_specs.py`
- [x] Migrate `RelationOutputSpec` + projection logic to `semantics/catalog/projections.py`
- [x] Add `cpg_nodes_builder()` and `cpg_edges_builder()` to semantic projections
- [ ] Ensure registry_specs only creates ViewNodes (pure bridge)
- [ ] Verify semantic input validation gate is called before registration

---

## Scope 5 - Update Schema Contract Integration

### Status
**Partial** (AnnotatedSchema constructor added; semantic validation not wired)

### Goal
Align `datafusion_engine/schema/contracts.py` with semantic `AnnotatedSchema` for unified schema validation.

### Representative Pattern (Target - Unified)

```python
# src/datafusion_engine/schema/contracts.py (ENHANCED)
"""Schema contract validation with semantic awareness."""

from semantics.types import AnnotatedSchema

@dataclass(frozen=True)
class SchemaContract:
    name: str
    expected_schema: pa.Schema
    annotated_schema: AnnotatedSchema | None = None
    enforce_semantic_types: bool = False

    @classmethod
    def from_annotated_schema(
        cls,
        name: str,
        annotated: AnnotatedSchema,
        *,
        enforce_columns: bool = False,
        enforce_semantic_types: bool = True,
    ) -> SchemaContract:
        return cls(
            name=name,
            expected_schema=annotated.to_arrow(),
            annotated_schema=annotated,
            enforce_columns=enforce_columns,
            enforce_semantic_types=enforce_semantic_types,
        )
```

### Target Files
- Modify: `src/datafusion_engine/schema/contracts.py`
- Modify: `src/semantics/types/annotated_schema.py` (add `to_arrow()` if missing)

### Deletions
None

### Implementation Checklist
- [x] Add `from_annotated_schema()` constructor
- [ ] Add semantic validation method + violation type
- [ ] Update contract builders to use annotated schemas
- [ ] Add tests for semantic type validation

---

## Scope 6 - Runtime Decoupling via SemanticRuntimeConfig

### Status
**Partial** (config + adapter exist; pipeline still uses DataFusionRuntimeProfile)

### Goal
Decouple `semantics/pipeline.py` from `datafusion_engine/session/runtime.py` by introducing `SemanticRuntimeConfig` and a datafusion_engine adapter.

### Representative Pattern (Target - Decoupled)

```python
# src/semantics/runtime.py (NEW)
"""Semantic runtime configuration."""

@dataclass(frozen=True)
class SemanticRuntimeConfig:
    output_locations: Mapping[str, str]
    cache_policy_overrides: Mapping[str, CachePolicy]
    cdf_enabled: bool = False
    cdf_cursor_store: CdfCursorStore | None = None
```

```python
# src/datafusion_engine/semantics_runtime.py (NEW)
"""Adapter from DataFusionRuntimeProfile to SemanticRuntimeConfig."""

from semantics.runtime import SemanticRuntimeConfig

def semantic_runtime_from_profile(profile: DataFusionRuntimeProfile) -> SemanticRuntimeConfig:
    output_locations = {
        name: loc.path
        for name, loc in profile.dataset_locations.items()
        if _is_semantic_output(name)
    }
    return SemanticRuntimeConfig(
        output_locations=output_locations,
        cache_policy_overrides=profile.semantic_cache_overrides,
        cdf_enabled=profile.cdf_enabled,
        cdf_cursor_store=profile.cdf_cursor_store,
    )
```

### Target Files
- Create: `src/semantics/runtime.py`
- Create: `src/datafusion_engine/semantics_runtime.py`
- Modify: `src/semantics/pipeline.py` (accept SemanticRuntimeConfig)
- Modify: `src/datafusion_engine/views/registry_specs.py` (use adapter)

### Deletions
None

### Implementation Checklist
- [x] Implement SemanticRuntimeConfig in semantics
- [x] Add datafusion_engine adapter helper
- [ ] Update pipeline entrypoints to accept SemanticRuntimeConfig
- [ ] Update tests to use new runtime config type

---

## Scope 7 - Unified Dataset Catalog for Semantic + Extract Outputs

### Status
**Partial** (unified catalog helper exists; not wired into runtime/registry)

### Goal
Provide a single dataset catalog that merges semantic dataset locations and extract output locations, ensuring deterministic registration and planning.

### Representative Pattern (Target)

```python
# src/datafusion_engine/dataset/semantic_catalog.py (NEW)
"""Merge semantic + extract dataset locations into a unified catalog."""

from datafusion_engine.dataset.registry import DatasetCatalog, DatasetLocation
from datafusion_engine.extract.output_catalog import build_extract_output_catalog
from semantics.catalog.dataset_rows import get_all_dataset_rows
from semantics.catalog.spec_builder import build_dataset_spec


def build_semantic_dataset_catalog(
    *,
    semantic_output_root: str | None,
    extract_output_root: str | None,
) -> DatasetCatalog:
    catalog = DatasetCatalog()
    if semantic_output_root is not None:
        _register_semantic_outputs(catalog, semantic_output_root)
    if extract_output_root is not None:
        extract_catalog = build_extract_output_catalog(output_root=extract_output_root)
        catalog.merge(extract_catalog)
    return catalog
```

### Target Files
- Create: `src/datafusion_engine/dataset/semantic_catalog.py`
- Modify: `src/datafusion_engine/dataset/registry.py` (use unified catalog)
- Modify: `src/datafusion_engine/session/runtime.py` (expose unified catalog)

### Deletions
None

### Implementation Checklist
- [x] Build unified catalog helper
- [x] Register semantic dataset locations from semantic catalog rows
- [x] Merge extract output locations
- [ ] Ensure dataset location lookup uses unified catalog
- [ ] Add tests for catalog resolution

---

## Scope 8 - Compatibility Shims and Deprecation Window

### Status
**Completed** (shims active; removal scheduled after deprecation window)

### Goal
Provide a short compatibility window for removed view layers with explicit warnings and clear migration guidance.

### Representative Pattern (Target)

```python
# src/datafusion_engine/views/__init__.py (TEMP SHIM)
"""Compatibility exports (temporary)."""

import warnings

_REMOVED_EXPORTS: dict[str, str] = {
    "VIEW_SELECT_REGISTRY": "semantics.catalog.view_builders",
    "ViewExprBuilder": "semantics.catalog.view_builders",
}


def __getattr__(name: str) -> object:
    if name in _REMOVED_EXPORTS:
        warnings.warn(
            f"{name} is deprecated and will be removed. Use {_REMOVED_EXPORTS[name]} instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        raise AttributeError(f"{name} has been removed. Use {_REMOVED_EXPORTS[name]} instead.")
    raise AttributeError(name)
```

### Target Files
- Modify: `src/datafusion_engine/views/__init__.py`

### Deletions
- Remove shims after deprecation window ends

### Implementation Checklist
- [x] Add temporary deprecation shims
- [x] Emit clear error messages with migration path
- [ ] Remove shims after deprecation window ends

---

## Scope 9 - Naming Policy & CDF Capability Alignment

### Status
**Partial** (CDF capability used; canonical naming not yet enforced)

### Goal
Ensure view registration and dataset locations adhere to canonical semantic naming and CDF capability settings.

### Representative Pattern (Target)

```python
# src/datafusion_engine/views/registry_specs.py (ADAPTER)
from semantics.naming import canonical_output_name
from semantics.catalog.dataset_rows import SEMANTIC_DATASET_ROWS

for row in SEMANTIC_DATASET_ROWS:
    name = canonical_output_name(row.name)
    cdf_enabled = row.supports_cdf
    cache_policy = _cache_policy_for(name, runtime_profile, cdf_enabled=cdf_enabled)
    nodes.append(ViewNode(
        name=name,
        deps=_deps_for_row(row),
        builder=view_builder(name, input_mapping={}, config=None),
        cache_policy=cache_policy,
    ))
```

### Target Files
- Modify: `src/datafusion_engine/views/registry_specs.py`
- Modify: `src/datafusion_engine/dataset/registry.py` (canonical name registration)

### Deletions
None

### Implementation Checklist
- [ ] Ensure canonical naming is applied at registration time
- [x] Use `SemanticDatasetRow.supports_cdf` (via dataset_spec delta_cdf_policy) to tag CDF capability
- [x] Apply cache policy overrides consistently for CDF-enabled outputs

---

## Decommission and Deletion Summary

### Files to Delete (After All Scopes Complete)

**Scope 1 - Legacy Registry:**
- `src/datafusion_engine/views/registry.py`

**Scope 2 - DSL Layer:**
- `src/datafusion_engine/views/dsl.py`
- `src/datafusion_engine/views/dsl_views.py`

**Scope 3 - ViewProjectionSpec (Conditional):**
- Potentially `src/datafusion_engine/views/view_spec.py` if fully redundant

### Functions/Classes to Remove

**From `registry.py`:**
- `VIEW_SELECT_REGISTRY`
- `_VIEW_SELECT_EXPRS`
- `_view_exprs()`
- `_span_struct_from_components()`
- `_byte_span_struct()`

**From `dsl.py`:**
- `ViewExprBuilder`
- `SpanExprs`
- `_arrow_cast()`, `_null_expr()`

**From `dsl_views.py`:**
- All DSL-based view builders

**From `registry_specs.py`:**
- Inline projection logic (move to semantics)

---

## Execution Order (Recommended)

```
Phase 1: Delete Legacy Code
1) Scope 1 - Delete registry.py
2) Scope 2 - Delete dsl.py, dsl_views.py (done)
3) Scope 8 - Compatibility shims (active)

Phase 2: Consolidate Patterns
4) Scope 3 - Evaluate view_spec.py
5) Scope 4 - Simplify registry_specs.py
6) Scope 9 - Naming policy + CDF capability alignment

Phase 3: Enhance Integration
7) Scope 7 - Unified dataset catalog
8) Scope 5 - Schema contract integration
9) Scope 6 - Runtime decoupling
```

---

## Completion Criteria

- [ ] All legacy view definition code removed from `datafusion_engine/views/`
- [ ] `registry_specs.py` contains only ViewNode creation logic (pure bridge)
- [ ] All view builders imported from `semantics.catalog.view_builders`
- [ ] Schema contracts support semantic type validation
- [ ] Clear boundary between semantic definition and datafusion execution
- [ ] Unified dataset catalog resolves semantic + extract outputs
- [ ] Canonical naming and CDF capability are enforced at registration
- [ ] Full test suite passes with no regressions

---

## Verification Commands

```bash
# After each scope, verify no regressions
uv run pytest tests/unit/datafusion_engine/ -v
uv run pytest tests/unit/semantics/ -v

# Verify no dangling imports
uv run ruff check src/datafusion_engine/views/

# Type check
uv run pyright src/datafusion_engine/views/ --warnings
uv run pyrefly check

# Full test suite
uv run pytest tests/ -m "not e2e"
```
