## ArrowDSL + Schema Spec Reorganization Plan

### Goal
Migrate `src/arrowdsl` and `src/schema_spec` into consolidated modules with clear semantic
boundaries and target file sizes of 350–750 LOC, per the approved structure.

---

## Scope 1: Create New Directory Skeleton + Target Modules

### Description
Introduce the new directory layout under `src/arrowdsl/` and the consolidated
module files under `src/schema_spec/` before moving code. This sets the target
state and allows incremental migration.

### Target structure
```
src/arrowdsl/
  __init__.py
  core/
    interop.py
    context.py
    ids.py
  plan/
    plan.py
    ops.py
    query.py
  schema/
    schema.py
    arrays.py
  compute/
    expr.py
    predicates.py
    kernels.py
  finalize/
    finalize.py

src/schema_spec/
  __init__.py
  specs.py
  system.py
```

### Integration checklist
- [ ] Create `src/arrowdsl/core/`, `src/arrowdsl/plan/`, `src/arrowdsl/schema/`,
      `src/arrowdsl/compute/`, `src/arrowdsl/finalize/`.
- [ ] Create `src/arrowdsl/*/*.py` files with module docstrings and placeholders.
- [ ] Create `src/schema_spec/specs.py` and `src/schema_spec/system.py` placeholders.

---

## Scope 2: Consolidate ArrowDSL Core + Interop

### Description
Merge interop/low-level modules into `core/interop.py`, runtime/scan context into
`core/context.py`, and id helpers into `core/ids.py`.

### Code movement map
- `core/interop.py`
  - `src/arrowdsl/pyarrow_protocols.py`
  - `src/arrowdsl/pyarrow_core.py`
  - `src/arrowdsl/compute.py`
  - `src/arrowdsl/acero.py`
- `core/context.py`
  - `src/arrowdsl/runtime.py`
  - `src/arrowdsl/scan_context.py`
- `core/ids.py`
  - `src/arrowdsl/ids.py`
  - `src/arrowdsl/id_specs.py`
  - `src/arrowdsl/iter.py`

### Code pattern
```python
# src/arrowdsl/core/interop.py
"""Arrow interop + protocol shims."""

# contents merged from pyarrow_protocols.py / pyarrow_core.py / compute.py / acero.py
```

### Integration checklist
- [ ] Move code into new files; keep ordering: stdlib → third‑party → local.
- [ ] Fix intra-module references (e.g., `from arrowdsl.core.interop import pc`).
- [ ] Update imports across repo to point at new paths.

---

## Scope 3: Consolidate ArrowDSL Plan/Query/Runner

### Description
Combine plan ops/specs/join helpers into `plan/ops.py`, keep plan core in
`plan/plan.py`, and move dataset IO + query spec + pipeline runner into
`plan/query.py`.

### Code movement map
- `plan/plan.py`
  - `src/arrowdsl/plan.py`
- `plan/ops.py`
  - `src/arrowdsl/plan_ops.py`
  - `src/arrowdsl/ops.py`
  - `src/arrowdsl/joins.py`
  - `src/arrowdsl/specs.py`
- `plan/query.py`
  - `src/arrowdsl/dataset_io.py`
  - `src/arrowdsl/queryspec.py`
  - `src/arrowdsl/runner.py`

### Code pattern
```python
# src/arrowdsl/plan/query.py
"""Dataset scan/query helpers and pipeline runner."""

# contains dataset_io + queryspec + run_pipeline
```

### Integration checklist
- [ ] Move code and reconcile duplicate names (e.g., `Plan`, `PlanOp`).
- [ ] Update call sites to import from `arrowdsl.plan.*`.
- [ ] Ensure `run_pipeline` docstrings still align with new module.

---

## Scope 4: Consolidate ArrowDSL Schema + Array Builders

### Description
Move schema alignment, encoding, and empty table helpers into
`schema/schema.py`. Group column ops and nested builders into `schema/arrays.py`.

### Code movement map
- `schema/schema.py`
  - `src/arrowdsl/schema.py`
  - `src/arrowdsl/schema_ops.py`
  - `src/arrowdsl/encoding.py`
  - `src/arrowdsl/empty.py`
- `schema/arrays.py`
  - `src/arrowdsl/column_ops.py`
  - `src/arrowdsl/columns.py`
  - `src/arrowdsl/nested.py`
  - `src/arrowdsl/nested_ops.py`

### Integration checklist
- [ ] Ensure nested builders keep clear grouping in `arrays.py`.
- [ ] Replace imports to new schema/arrays modules.
- [ ] Confirm `NestedFieldSpec` usage remains stable across extract/normalize.

---

## Scope 5: Consolidate ArrowDSL Compute + Finalize

### Description
Split compute into three large modules and fold finalize + contract handling into
one module.

### Code movement map
- `compute/expr.py`
  - `src/arrowdsl/expr.py`
- `compute/predicates.py`
  - `src/arrowdsl/predicates.py`
- `compute/kernels.py`
  - `src/arrowdsl/kernels.py`
- `finalize/finalize.py`
  - `src/arrowdsl/finalize.py`
  - `src/arrowdsl/contracts.py`
  - `src/arrowdsl/finalize_context.py`

### Integration checklist
- [ ] Merge finalize + contract definitions in a stable order.
- [ ] Update imports throughout (especially in extract/normalize/relspec).
- [ ] Ensure `FinalizeContext` remains public via re-exports.

---

## Scope 6: Consolidate Schema Spec Modules

### Description
Fold `schema_spec` into two large modules: `specs.py` (types/metadata) and
`system.py` (catalogs/contracts/registry/pandera).

### Code movement map
- `specs.py`
  - `src/schema_spec/core.py`
  - `src/schema_spec/fields.py`
  - `src/schema_spec/metadata.py`
  - `src/schema_spec/provenance.py`
- `system.py`
  - `src/schema_spec/contracts.py`
  - `src/schema_spec/factories.py`
  - `src/schema_spec/catalogs.py`
  - `src/schema_spec/registry.py`
  - `src/schema_spec/pandera_adapter.py`

### Integration checklist
- [ ] Preserve Pydantic model configs and validators.
- [ ] Update `schema_spec/__init__.py` to re-export from `specs.py` and `system.py`.
- [ ] Update imports across repo to new module names.

---

## Scope 7: Update Public Imports and Backward Compatibility

### Description
Keep a clean public surface via `arrowdsl/__init__.py` and `schema_spec/__init__.py`.
Optionally include temporary import shims (forwarders) if needed.

### Code pattern
```python
# src/arrowdsl/__init__.py
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import run_pipeline
from arrowdsl.finalize.finalize import FinalizeContext
```

### Integration checklist
- [ ] Update `arrowdsl/__init__.py` exports to new paths.
- [ ] Update `schema_spec/__init__.py` exports to new paths.
- [ ] Decide whether to leave compatibility stubs (if yes, limit to a single release window).

---

## Scope 8: Repo-wide Import Rewrite

### Description
Update all imports across the codebase to match new module paths.

### Suggested approach
- Use `rg` to locate old import paths.
- Update with targeted edits per file to maintain ordering and avoid unused imports.

### Target areas (non-exhaustive)
- `src/extract/*`
- `src/normalize/*`
- `src/cpg/*`
- `src/relspec/*`
- `src/hamilton_pipeline/*`
- `src/obs/*`
- `src/schema_spec/*`

### Integration checklist
- [ ] Update all `arrowdsl.*` imports to new subpackages.
- [ ] Update all `schema_spec.*` imports to `schema_spec.specs`/`schema_spec.system`.
- [ ] Run `ruff` to catch unused/incorrect imports.

---

## Scope 9: Validation + Cleanup

### Description
Ensure code compiles cleanly and remove old files once all imports are migrated.

### Checklist
- [ ] Remove old files in `src/arrowdsl/*.py` and `src/schema_spec/*.py` once unused.
- [ ] Run:
  - `uv run ruff check --fix`
  - `uv run pyrefly check`
  - `uv run pyright --warnings --pythonversion=3.14`
- [ ] Update any remaining docs referencing old module paths.

---

## Execution Sequence (Recommended)
1) Scope 1 → Scope 2 → Scope 3 → Scope 4 → Scope 5
2) Scope 6 → Scope 7 → Scope 8 → Scope 9

This order minimizes circular import risk and keeps the build green as modules are moved.
