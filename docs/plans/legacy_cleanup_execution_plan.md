# Legacy Cleanup Execution Plan (Post‑Alignment)

## Purpose
Aggressively remove dead/legacy/compatibility code that is no longer part of the go‑forward
architecture, with explicit targets, safe patterns, and verifiable acceptance checks.

## Scope Summary
This plan decommissions modules that are now orphaned after the Ibis‑first refactor, along with
legacy configuration and compatibility branches that have no remaining call sites.

---

## Scope 1 — Remove unused legacy config surface
Eliminate the legacy configuration module that has no call sites and no public API use.

**Code pattern**
```python
# Remove unused legacy config module entirely.
# No replacement needed; callers use pipeline inputs + runtime profiles instead.
```

**Target files**
- src/config.py

**Implementation checklist**
- [ ] Delete `src/config.py`.
- [ ] Verify no imports or string references remain.

---

## Scope 2 — Remove CPG plan‑lane catalog (unused)
Remove the plan‑lane CPG catalog wrapper that is no longer referenced after Ibis‑only CPG builds.

**Code pattern**
```python
# Remove plan-lane catalog; rely on Ibis catalogs in relspec/cpg builders.
# No migration needed because no call sites remain.
```

**Target files**
- src/cpg/catalog.py

**Implementation checklist**
- [ ] Delete `src/cpg/catalog.py`.
- [ ] Confirm no lingering imports (direct or indirect).

---

## Scope 3 — Remove CPG merge helper (unused)
Remove the unused CPG merge helper (schema unification handled by DatasetSpec directly).

**Code pattern**
```python
# Prefer DatasetSpec.unify_tables(...) directly (already used elsewhere).
```

**Target files**
- src/cpg/merge.py

**Implementation checklist**
- [ ] Delete `src/cpg/merge.py`.
- [ ] Ensure no references remain.

---

## Scope 4 — Remove extract registry table helper (unused)
Remove unused extract registry table surface.

**Code pattern**
```python
# Remove unused EXTRACT_RULE_TABLE helper (no inbound references).
```

**Target files**
- src/extract/registry_tables.py

**Implementation checklist**
- [ ] Delete `src/extract/registry_tables.py`.
- [ ] Confirm no imports remain in src/tests/scripts.

---

## Scope 5 — Remove Ibis DataFusion adapter wrapper (unused)
Remove the unused wrapper around the DataFusion bridge API.

**Code pattern**
```python
# Remove ibis_engine.datafusion_adapter; use datafusion_engine.bridge directly.
```

**Target files**
- src/ibis_engine/datafusion_adapter.py

**Implementation checklist**
- [ ] Delete `src/ibis_engine/datafusion_adapter.py`.
- [ ] Confirm no import paths reference it.

---

## Scope 6 — Remove legacy relspec graph wrapper (unused)
Remove unused wrapper around relspec compiler graph utilities.

**Code pattern**
```python
# Remove relspec.rules.compiler_graph; use relspec.compiler_graph directly.
```

**Target files**
- src/relspec/rules/compiler_graph.py

**Implementation checklist**
- [ ] Delete `src/relspec/rules/compiler_graph.py`.
- [ ] Confirm no imports remain.

---

## Scope 7 — Remove legacy SQLGlot diagnostics shim (unused)
Remove unused SQLGlot diagnostics wrapper; retain `sqlglot_tools.bridge.sqlglot_diagnostics`.

**Code pattern**
```python
# Remove relspec.sqlglot_diagnostics; use sqlglot_tools.bridge.sqlglot_diagnostics.
```

**Target files**
- src/relspec/sqlglot_diagnostics.py

**Implementation checklist**
- [ ] Delete `src/relspec/sqlglot_diagnostics.py`.
- [ ] Confirm no imports or re-exports remain.

---

## Scope 8 — Remove orphaned plan‑lane helper dependency
Delete plan‑lane helper that is only referenced by the removed plan‑lane CPG catalog.

**Code pattern**
```python
# Remove cpg.plan_specs if only referenced by cpg.catalog.
```

**Target files**
- src/cpg/plan_specs.py

**Implementation checklist**
- [ ] Delete `src/cpg/plan_specs.py` (after `src/cpg/catalog.py` is removed).
- [ ] Confirm no other references exist.

---

## Scope 9 — Remove unused legacy CPG edge inputs path
Drop the legacy edge‑inputs branch in CPG edge building.

**Code pattern**
```python
# Remove legacy inputs:
if inputs is None:
    raise ValueError("EdgeBuildConfig.inputs is required.")
```

**Target files**
- src/relspec/cpg/build_edges.py

**Implementation checklist**
- [ ] Delete `EdgeBuildConfig.legacy` field and `_edge_inputs_from_legacy` helpers.
- [ ] Remove fallback branch that reads legacy mapping.
- [ ] Update error messaging to require `inputs` explicitly.

---

## Validation and Quality Gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`

## Exit Criteria
- No imports of removed modules remain in `src/`, `tests/`, or `scripts/`.
- CPG/Normalize paths remain Ibis‑first with no plan‑lane dependency.
- All quality gates pass with zero errors/warnings.
