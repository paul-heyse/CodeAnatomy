# Semantic IR Spec-Derived Dataset Rows Plan v1

> **Purpose**: Make `SemanticIR.dataset_rows` fully spec-derived (no static tuples), so
> dataset rows are a pure projection of registered specs and IR is the only authority.

---

## Scope Item B1 — Normalize layer rows derived from normalize registry

**Why**: `_NORMALIZE_DATASET_ROWS` is a static tuple; it should be derived from the
normalize layer’s registry to avoid drift.

**Representative code pattern to implement**
```python
# semantics/ir_pipeline.py
def _build_normalize_rows() -> tuple[SemanticDatasetRow, ...]:
    from semantics.catalog.normalize_registry import NORMALIZE_DATASETS

    return tuple(_row_from_registry_spec(spec) for spec in NORMALIZE_DATASETS)
```

**Target files**
- `src/semantics/ir_pipeline.py`
- `src/semantics/catalog/normalize_registry.py`
- `src/semantics/catalog/dataset_registry.py`

**Deprecate / delete after completion**
- Delete `_NORMALIZE_DATASET_ROWS` from `semantics/ir_pipeline.py`.

**Implementation checklist**
- [x] Introduce normalize registry mapping helper.
- [x] Implement `_build_normalize_rows()` using normalize registry data.
- [x] Remove `_NORMALIZE_DATASET_ROWS` constant and all callers.

**Status**: Complete (normalize registry now lives in `src/semantics/catalog/normalize_registry.py`).

---

## Scope Item B2 — Semantic normalization rows derived solely from `SEMANTIC_NORMALIZATION_SPECS`

**Why**: The semantic normalization rows should be a direct projection of the
spec registry with no local literals.

**Representative code pattern to implement**
```python
# semantics/ir_pipeline.py
def _build_semantic_normalization_rows(model: SemanticModel) -> tuple[SemanticDatasetRow, ...]:
    return tuple(_row_from_semantic_norm_spec(spec) for spec in model.normalization_specs)
```

**Target files**
- `src/semantics/ir_pipeline.py`
- `src/semantics/registry.py` (spec metadata additions if required)

**Deprecate / delete after completion**
- Delete any hard-coded semantic normalization row fields in IR pipeline.

**Implementation checklist**
- [x] Ensure normalization rows are derived from `SEMANTIC_NORMALIZATION_SPECS`.
- [x] Keep row construction spec-derived (no parallel registry).

**Status**: Complete (rows flow from `SemanticModel.normalization_specs`).

---

## Scope Item B3 — Relationship rows derived from `RELATIONSHIP_SPECS` only

**Why**: Relationship dataset rows must be computed strictly from registry specs
and projection config to prevent divergence.

**Representative code pattern to implement**
```python
# semantics/ir_pipeline.py
def _build_relationship_rows(model: SemanticModel) -> tuple[SemanticDatasetRow, ...]:
    projection = semantic_projection_options(...)
    return tuple(_row_from_relationship_spec(spec, projection[spec.name]) for spec in model.relationship_specs)
```

**Target files**
- `src/semantics/ir_pipeline.py`
- `src/semantics/spec_registry.py`
- `src/semantics/catalog/projections.py`

**Deprecate / delete after completion**
- Remove any hard-coded relationship metadata in row assembly.

**Implementation checklist**
- [x] Derive relationship row fields from projection config + `RELATIONSHIP_SPECS`.

**Status**: Complete (projection config + registry specs drive row fields).

---

## Scope Item B4 — Diagnostic and export rows derived from registries

**Why**: Diagnostics/export outputs should be declared in registries; row generation
should be a pure mapping.

**Representative code pattern to implement**
```python
# semantics/catalog/diagnostics_registry.py
DIAGNOSTIC_DATASETS = (DiagnosticSpec(...), ...)

# semantics/ir_pipeline.py
def _build_diagnostic_rows() -> tuple[SemanticDatasetRow, ...]:
    return tuple(_row_from_diagnostic_spec(spec) for spec in DIAGNOSTIC_DATASETS)
```

**Target files**
- `src/semantics/catalog/diagnostics_registry.py` (new)
- `src/semantics/catalog/export_registry.py` (new)
- `src/semantics/ir_pipeline.py`

**Deprecate / delete after completion**
- Remove hard-coded diagnostic/export row literals in IR pipeline.

**Implementation checklist**
- [x] Create registry specs for diagnostics and exports.
- [x] Map diagnostic/export registries to dataset rows.
- [x] Apply dynamic relationship feature fields in IR assembly.

**Status**: Complete (registries in `diagnostics_registry.py` + `export_registry.py`).

---

## Scope Item B5 — CPG output rows derived from CPG emit specs

**Why**: CPG output rows should be derived from CPG spec metadata instead of
hand-assembled logic.

**Representative code pattern to implement**
```python
# cpg/emit_specs.py
def cpg_output_specs() -> tuple[CpgOutputSpec, ...]: ...

# semantics/ir_pipeline.py
def _build_cpg_output_rows() -> tuple[SemanticDatasetRow, ...]:
    return tuple(_row_from_cpg_output_spec(spec) for spec in cpg_output_specs())
```

**Target files**
- `src/cpg/emit_specs.py`
- `src/semantics/ir_pipeline.py`

**Deprecate / delete after completion**
- Remove any static CPG output row assembly in IR pipeline.

**Implementation checklist**
- [x] Add `CpgOutputSpec` structure + accessor in `cpg/emit_specs.py`.
- [x] Map `CpgOutputSpec` → `SemanticDatasetRow` in IR pipeline.

**Status**: Complete (CPG outputs now derived from `cpg_output_specs()`).

---

## Scope Item B6 — Consolidate dataset-row assembly under IR

**Why**: All dataset rows should be produced by a single IR assembly function
that calls spec-derived helpers.

**Representative code pattern to implement**
```python
# semantics/ir_pipeline.py
def _build_semantic_dataset_rows(model: SemanticModel) -> tuple[SemanticDatasetRow, ...]:
    return (
        *_build_normalize_rows(),
        *_build_semantic_normalization_rows(model),
        *_build_singleton_rows(),
        *_build_relationship_rows(model),
        *_build_diagnostic_rows(),
        *_build_export_rows(),
        *_build_cpg_output_rows(),
    )
```

**Target files**
- `src/semantics/ir_pipeline.py`
- `src/semantics/catalog/dataset_rows.py` (accessor only; no assembly)

**Deprecate / delete after completion**
- Remove any residual assembly logic from `semantics/catalog/dataset_rows.py`.

**Implementation checklist**
- [x] Ensure all dataset-row builders are spec-derived.
- [x] Verify `SemanticIR.dataset_rows` is the single registry.

**Status**: Complete (catalog is accessor-only; IR pipeline assembles all rows).

---

## Cross-Cutting Acceptance Gates

1. **No static row tuples**: zero hard-coded dataset row registries in catalog/IR modules. ✅
2. **Spec-only derivation**: all rows are derived from registered specs. ✅
3. **Single authority**: `SemanticIR.dataset_rows` remains the only dataset-row source. ✅

---

## Implementation Checklist (Summary)

- [x] B1 Normalize rows derived from normalize registry
- [x] B2 Semantic normalization rows spec-derived
- [x] B3 Relationship rows spec-derived
- [x] B4 Diagnostic/export rows spec-derived
- [x] B5 CPG output rows spec-derived
- [x] B6 Single IR assembly path only
