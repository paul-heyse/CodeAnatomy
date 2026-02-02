# Semantic IR Test Alignment Plan v1

> **Purpose**: Align the test suite with the IR‑first architecture so tests reflect the new single source of truth (Semantic IR + SemanticModel outputs) and remove legacy test coverage for deleted or deprecated code paths.
>
> **Design principle**: Tests must validate IR artifacts and IR‑driven registration/output wiring; no legacy catalogs, static registries, or implicit IR fallbacks.

---

## Scope Item 1 — Replace static semantic output lists in tests

### Goal
Remove all usages of static/legacy output registries (e.g., `SEMANTIC_VIEW_NAMES`) in tests and replace them with IR or `SEMANTIC_MODEL.outputs` derived sources.

### Representative code patterns
```python
# tests/unit/engine/test_semantic_boundary.py
from semantics.registry import SEMANTIC_MODEL

sample_view = SEMANTIC_MODEL.outputs[0].name
assert is_semantic_view(sample_view)
```

```python
# tests/unit/engine/test_semantic_boundary.py
from semantics.ir_pipeline import build_semantic_ir

semantic_ir = build_semantic_ir()
first_output = semantic_ir.views[0].name
assert is_semantic_view(first_output)
```

### Target files
- `tests/unit/engine/test_semantic_boundary.py`

### Deprecate / delete after completion
- Any remaining test references to `SEMANTIC_VIEW_NAMES` or static output registries.

### Implementation checklist
- [ ] Replace `SEMANTIC_VIEW_NAMES` usage with `SEMANTIC_MODEL.outputs` or `build_semantic_ir()`.
- [ ] Ensure tests still validate semantic boundary behavior with IR‑based names.

---

## Scope Item 2 — Make view registration tests IR‑explicit

### Goal
Update view registration tests to supply a compiled `SemanticIR` explicitly (no implicit IR construction inside registration paths).

### Representative code patterns
```python
# tests/unit/datafusion_engine/views/test_registry_specs_runtime_config.py
from semantics.ir_pipeline import build_semantic_ir

semantic_ir = build_semantic_ir()
registry_specs.view_graph_nodes(
    ctx,
    snapshot={},
    runtime_profile=profile,
    semantic_ir=semantic_ir,
)
```

### Target files
- `tests/unit/datafusion_engine/views/test_registry_specs_runtime_config.py`

### Deprecate / delete after completion
- Any test logic that assumes `semantic_ir` can be omitted.

### Implementation checklist
- [ ] Pass a compiled IR into `view_graph_nodes` in all tests.
- [ ] Validate runtime config threading still works with IR provided.

---

## Scope Item 3 — IR‑first dataset locations and evidence tests

### Goal
Update integration tests to validate outputs and evidence using IR artifacts and IR‑emitted dataset rows rather than catalog‑based registries.

### Representative code patterns
```python
# tests/integration/test_runtime_semantic_locations.py
from semantics.ir_pipeline import build_semantic_ir

semantic_ir = build_semantic_ir()
output_names = {view.name for view in semantic_ir.views}
assert "cst_refs_norm_v1" in output_names
```

```python
# tests/integration/test_evidence_semantic_ir.py
from relspec.evidence import initial_evidence_from_views
from semantics.ir_pipeline import build_semantic_ir

semantic_ir = build_semantic_ir()
evidence = initial_evidence_from_views(semantic_ir.views, dataset_specs=())
assert "cst_refs_norm_v1" in evidence.sources
```

### Target files
- `tests/integration/test_runtime_semantic_locations.py`
- `tests/integration/test_evidence_semantic_catalog.py` (rename to IR‑focused)

### Deprecate / delete after completion
- Catalog‑centric test logic for normalize/semantic locations.
- Tests asserting catalog registration for semantic outputs without IR view nodes.

### Implementation checklist
- [ ] Replace catalog‑derived assertions with IR‑derived assertions.
- [ ] Rename tests to reflect IR‑first intent.
- [ ] Ensure evidence tests use IR view nodes or IR artifacts.

---

## Scope Item 4 — Align semantic output catalog tests to IR outputs

### Goal
Ensure output catalog tests reflect IR‑derived output lists (including diagnostics/exports) rather than hard‑coded names.

### Representative code patterns
```python
# tests/unit/hamilton_pipeline/test_inputs_semantic_output_catalog.py
from semantics.registry import SEMANTIC_MODEL

expected = {spec.name for spec in SEMANTIC_MODEL.outputs}
assert expected.issubset(set(catalog.names()))
```

### Target files
- `tests/unit/hamilton_pipeline/test_inputs_semantic_output_catalog.py`

### Deprecate / delete after completion
- Hard‑coded semantic output name assertions that drift from IR outputs.

### Implementation checklist
- [ ] Replace hard‑coded view assertions with IR or SemanticModel outputs.
- [ ] Confirm catalog paths are still constructed correctly for any sampled outputs.

---

## Scope Item 5 — Transition dataset row/spec tests to IR‑emitted registry

### Goal
Refactor dataset row/spec tests to validate IR‑emitted dataset rows, not manual registry assembly.

### Representative code patterns
```python
# tests/unit/semantics/catalog/test_dataset_rows.py
from semantics.ir_pipeline import build_semantic_ir

semantic_ir = build_semantic_ir()
rows = semantic_ir.dataset_rows
assert len(rows) > 0
```

```python
# tests/unit/semantics/catalog/test_dataset_specs.py
from semantics.ir_pipeline import build_semantic_ir

semantic_ir = build_semantic_ir()
names = {row.name for row in semantic_ir.dataset_rows}
assert "cst_refs_norm_v1" in names
```

### Target files
- `tests/unit/semantics/catalog/test_dataset_rows.py`
- `tests/unit/semantics/catalog/test_dataset_specs.py`

### Deprecate / delete after completion
- Manual dataset row assembly helpers in tests.
- Tests asserting internal caching behavior of catalog registry (no longer relevant).

### Implementation checklist
- [ ] Read dataset rows from `SemanticIR`.
- [ ] Update tests to reflect IR‑emitted row ordering and content.

---

## Scope Item 6 — Update IR golden snapshots and pruning assertions

### Goal
Ensure IR snapshot tests include any newly emitted IR fields (dataset rows, registration payloads) and keep pruning expectations aligned with diagnostics/exports behavior.

### Representative code patterns
```python
# tests/semantics/test_semantic_ir_snapshot.py
semantic_ir = build_semantic_ir()
payload = {
    "views": [view.name for view in semantic_ir.views],
    "dataset_rows": [row.name for row in semantic_ir.dataset_rows],
}
```

### Target files
- `tests/semantics/test_semantic_ir_snapshot.py`
- `tests/fixtures/semantic_ir_snapshot.json`

### Deprecate / delete after completion
- Any golden snapshots that omit IR‑emitted rows or artifacts.

### Implementation checklist
- [ ] Extend IR snapshot payload to include IR‑emitted rows where applicable.
- [ ] Update goldens via `--update-golden` after code changes.

---

## Scope Item 7 — Optional cleanup: legacy adapter tests (if adapters removed)

### Goal
If legacy projection adapters are removed, delete the corresponding tests to avoid validating non‑existent compatibility layers.

### Representative code patterns
```python
# tests/unit/semantics/test_adapters.py
# Remove when semantics.adapters is deleted.
```

### Target files
- `tests/unit/semantics/test_adapters.py`

### Deprecate / delete after completion
- Entire test file if `semantics.adapters` is removed.

### Implementation checklist
- [ ] Confirm adapters are removed from production code.
- [ ] Delete adapter tests to match the new IR‑first surface.

---

# Cross‑Cutting Acceptance Gates

- [ ] No tests reference `SEMANTIC_VIEW_NAMES` or static output registries.
- [ ] All semantic view registration tests require an explicit `SemanticIR`.
- [ ] Evidence and output location tests use IR‑derived sources only.
- [ ] Dataset row/spec tests validate `SemanticIR.dataset_rows`.
- [ ] IR golden snapshots include all IR‑emitted artifacts.

---

## Execution Order (Suggested)

1. Replace static output list usages (Scope 1)
2. Require IR for view registration tests (Scope 2)
3. Update integration tests to IR‑first evidence/locations (Scope 3)
4. Align semantic output catalog tests (Scope 4)
5. Refactor dataset row/spec tests (Scope 5)
6. Update IR goldens (Scope 6)
7. Optional adapter test cleanup (Scope 7)

---

## Deletion Plan — Legacy Test Removal Summary

- Delete any tests that only validate removed legacy APIs:
  - `tests/unit/engine/test_semantic_boundary.py` (legacy section only, if no longer relevant)
  - `tests/unit/semantics/test_adapters.py` (only if adapters are deleted)
- Remove any references to catalog‑based semantic registry behavior.

