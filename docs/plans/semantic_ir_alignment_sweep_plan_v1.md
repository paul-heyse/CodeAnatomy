# Semantic IR Alignment Sweep Plan v1

> **Purpose**: Remove remaining non-IR sources of truth and legacy scaffolding so
> Semantic IR is the sole authority for outputs, dataset rows, and view registration.

---

## Scope Item A1 — Remove static output registries (SEMANTIC_VIEW_NAMES)

**Why**: Static output lists drift from IR and create a second source of truth.

**Representative code pattern to remove**
```python
# semantics/naming.py
SEMANTIC_VIEW_NAMES: Final[tuple[str, ...]] = tuple(SEMANTIC_OUTPUT_NAMES.values())
```

**Target files**
- `src/semantics/naming.py`
- `src/semantics/registry.py`
- `src/semantics/pipeline.py`
- `src/engine/semantic_boundary.py` (docstrings only)

**Delete / replace**
- Delete `SEMANTIC_VIEW_NAMES` and any consumers.
- Replace output list usage with IR-derived outputs:
  - `build_semantic_ir().views` → output names
  - `SEMANTIC_MODEL.outputs` built from IR (see A2)
- Keep `canonical_output_name()` only as a naming policy helper.

**Implementation checklist**
- [x] Remove `SEMANTIC_VIEW_NAMES` constant.
- [x] Build output lists from IR or `SEMANTIC_MODEL.outputs`.
- [x] Update docstrings referencing `SEMANTIC_VIEW_NAMES`.

---

## Scope Item A2 — Make dataset rows IR‑emitted (single authority)

**Why**: Dataset rows are currently assembled via manual registries; IR should emit them.

**Representative code pattern to replace**
```python
# semantics/catalog/dataset_rows.py
rows.extend(_NORMALIZE_DATASET_ROWS)
rows.extend(_build_relationship_rows(model))
```

**Target files**
- `src/semantics/ir_pipeline.py`
- `src/semantics/catalog/dataset_rows.py`
- `src/semantics/registry.py`
- `src/semantics/catalog/dataset_specs.py`

**Delete / replace**
- Move dataset row assembly into IR emission (`emit_semantics`) and store on `SemanticIR`.
- Convert `semantics.catalog.dataset_rows` into a thin accessor over `build_semantic_ir().dataset_rows`.
- Remove or privatize the manual assembly helpers if they remain (best-in-class: delete).

**Implementation checklist**
- [x] Emit `dataset_rows` from IR (not from `SemanticModel`).
- [x] Remove manual row assembly from `dataset_rows.py`.
- [x] Ensure catalog APIs (`dataset_row`, `get_all_dataset_rows`, `dataset_specs`) use IR rows.

---

## Scope Item A3 — IR‑sourced view registration only

**Why**: View registration still re-derives specs from pipeline registries; IR should be passed explicitly.

**Representative code pattern to replace**
```python
# datafusion_engine/views/registry_specs.py
return _cpg_view_specs(CpgViewSpecsRequest(..., semantic_ir=None))
```

**Target files**
- `src/datafusion_engine/views/registry_specs.py`
- `src/datafusion_engine/views/registration.py`
- `src/datafusion_engine/semantics_runtime.py`
- `src/semantics/pipeline.py`

**Delete / replace**
- Require a `SemanticIR` artifact for registration (loaded from runtime or built once).
- Plumb `semantic_ir` through `view_graph_nodes()` and `ensure_view_graph()`.
- Remove the “build IR implicitly inside registration” fallback.

**Implementation checklist**
- [x] Add `semantic_ir` to registration entrypoints.
- [x] Pass compiled IR from runtime/profile to registration.
- [x] Delete `semantic_ir=None` fallback path.

---

## Scope Item A4 — Remove deprecated pipeline adapters (RelateSpec)

**Why**: Legacy adapters keep non‑IR entrypoints alive.

**Representative code pattern to remove**
```python
# semantics/pipeline.py
class RelateSpec: ...
```

**Target files**
- `src/semantics/pipeline.py`

**Delete / replace**
- Delete `RelateSpec` and related conversion helpers.
- Require `RelationshipSpec` + IR join groups exclusively.

**Implementation checklist**
- [x] Remove `RelateSpec` class and conversions.
- [x] Update any call sites/tests to use `RelationshipSpec` or IR.

---

## Scope Item A5 — Remove non‑IR Hamilton outputs & tag policy dependence

**Why**: Non‑IR Hamilton outputs reintroduce parallel output authority.

**Representative code pattern to remove**
```python
# hamilton_pipeline/modules/column_features.py
@apply_tag(TagPolicy(layer="quality", ...))
def cpg_nodes_path_depth(...) -> ...
```

**Target files**
- `src/hamilton_pipeline/tag_policy.py`
- `src/hamilton_pipeline/modules/column_features.py`
- `src/hamilton_pipeline/modules/subdags.py`

**Delete / replace**
- Remove semantic tag policies that derive from catalog metadata.
- Either:
  1) Move column feature outputs into IR (preferred), **or**
  2) Delete non‑IR outputs outright in design phase.
- Ensure Hamilton modules only consume IR outputs, never emit semantic authority.

**Implementation checklist**
- [x] Remove semantic tag policies tied to catalog metadata.
- [x] Delete or IR‑register column feature outputs.
- [x] Remove any remaining semantic tagging in Hamilton subDAGs.

---

## Cross‑Cutting Acceptance Gates

1. **Single output authority**: No static output lists; all outputs are IR‑derived.
2. **IR dataset rows**: `SemanticIR.dataset_rows` is the only dataset row registry.
3. **IR view registration**: All semantic view registration requires a `SemanticIR` artifact.
4. **No legacy adapters**: `RelateSpec` or other compatibility adapters removed.
5. **No non‑IR semantic outputs**: Hamilton modules do not emit semantic outputs unless registered in IR.

---

## Execution Order (Suggested)

1. Remove static output registries (A1)
2. Emit dataset rows from IR (A2)
3. IR‑sourced view registration (A3)
4. Remove legacy adapters (A4)
5. Remove non‑IR Hamilton outputs/tags (A5)

---

## Implementation Checklist (Summary)

- [x] A1 Static output registries removed
- [x] A2 Dataset rows IR‑emitted
- [x] A3 IR‑only view registration
- [x] A4 Deprecated adapters removed
- [x] A5 Non‑IR Hamilton outputs/tags removed
