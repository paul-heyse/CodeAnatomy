# Semantic IR Legacy Deletion Plan v1

> **Purpose**: Remove all remaining non-IR orchestration and compatibility layers so the
> semantic IR is the single execution authority across planning, registration, and outputs.

---

## Scope Item L1 — Remove Hamilton semantic registry (tag-based)

**Why**: The Hamilton tag registry duplicates semantic authority already emitted by IR artifacts.

**Representative code pattern to remove**
```python
# hamilton_pipeline/semantic_registry.py
registry = semantic_registry_from_driver(driver, plan_signature=plan_signature)
```

**Target files**
- `src/hamilton_pipeline/semantic_registry.py`
- `src/hamilton_pipeline/driver_factory.py`
- `src/hamilton_pipeline/driver_builder.py`

**Delete / replace**
- Delete `SemanticRegistryHook` and tag-based registry compilation.
- Replace any registry artifact emission with IR artifacts:
  - `semantic_explain_plan_v1`
  - `semantic_explain_plan_report_v1`
  - `semantic_view_plan_stats_v1`

**Implementation checklist**
- [x] Remove semantic registry hook wiring from driver factory/builder.
- [x] Delete semantic registry module (or leave a thin shim raising a hard error).
- [x] Update tests/docs referencing semantic registry artifacts.

---

## Scope Item L2 — Remove Hamilton CPG finalization and output validation subDAGs

**Why**: IR finalize nodes already produce contract-ready outputs; Hamilton post-processing is redundant.

**Representative code pattern to remove**
```python
# hamilton_pipeline/modules/cpg_finalize.py
return finalize_cpg_table(table, name=table_name, runtime_profile_spec=runtime_profile_spec)
```

**Target files**
- `src/hamilton_pipeline/cpg_finalize_utils.py`
- `src/hamilton_pipeline/modules/cpg_finalize.py`
- `src/hamilton_pipeline/modules/subdags.py`
- `src/hamilton_pipeline/modules/cpg_outputs.py`

**Delete / replace**
- Delete finalization helpers and subDAG wiring.
- Replace any CPG output validation with IR-level contract finalization or IR artifacts.

**Implementation checklist**
- [x] Remove finalization helpers and subDAG usage.
- [x] Delete CPG output validation pipeline or convert to pass-through nodes.
- [x] Update tests to consume IR finalized outputs directly.

---

## Scope Item L3 — Align semantic output catalogs to IR outputs

**Why**: Catalogs built from `SEMANTIC_VIEW_NAMES` omit IR diagnostics/exports and drift from IR.

**Representative code pattern to replace**
```python
view_names = list(SEMANTIC_VIEW_NAMES)
```

**Target files**
- `src/hamilton_pipeline/modules/inputs.py`
- `src/datafusion_engine/dataset/semantic_catalog.py`
- `src/datafusion_engine/dataset/registry.py`

**Delete / replace**
- Replace `SEMANTIC_VIEW_NAMES` usage with `SEMANTIC_MODEL.outputs` or IR-derived outputs.
- Ensure diagnostics/exports are registered consistently.

**Implementation checklist**
- [x] Build output catalogs from semantic model outputs.
- [x] Ensure diagnostic/export outputs are included.
- [x] Update tests using catalog output names.

---

## Scope Item L4 — Remove legacy alias layer and canonicalize IR outputs

**Why**: Legacy aliases hide the IR canonical naming contract.

**Representative code pattern to remove**
```python
# semantics/naming.py
SEMANTIC_OUTPUT_ALIASES = {"rel_call_symbol": SEMANTIC_OUTPUT_NAMES["rel_callsite_symbol"]}
```

**Target files**
- `src/semantics/naming.py`
- `src/datafusion_engine/views/registry_specs.py` (alias node generation)

**Delete / replace**
- Remove `SEMANTIC_OUTPUT_ALIASES` and alias node creation.
- Enforce canonical IR output names only.

**Implementation checklist**
- [x] Remove alias mapping and alias node generation.
- [x] Update any tests expecting legacy aliases.

---

## Scope Item L5 — Remove legacy view-spec registration stubs

**Why**: View registration must be IR-only; legacy view-spec API is dead code.

**Representative code pattern to delete**
```python
# datafusion_engine/session/runtime.py
msg = "Legacy view spec registration is removed; use ensure_view_graph."
raise RuntimeError(msg)
```

**Target files**
- `src/datafusion_engine/session/runtime.py`

**Delete / replace**
- Delete legacy view-spec registration helpers and handlers.
- Keep only IR-driven `ensure_view_graph` + IR view builders.

**Implementation checklist**
- [x] Remove legacy view-spec paths.
- [x] Update tests to remove legacy invocation attempts.

---

## Scope Item L6 — IR-first evidence and contract derivation in relspec

**Why**: Evidence/contract validation should be derived from IR artifacts and plan bundles.

**Representative code pattern to replace**
```python
catalog = dataset_catalog_from_profile(profile)
```

**Target files**
- `src/relspec/evidence.py`
- `src/relspec/graph_edge_validation.py`
- `src/relspec/execution_plan.py`
- `src/hamilton_pipeline/plan_artifacts.py`

**Delete / replace**
- Replace dataset-catalog-first evidence with IR explain/plan artifact inputs.
- Keep dataset spec fallbacks only for non-semantic outputs.

**Implementation checklist**
- [x] Introduce IR artifact-backed evidence catalog inputs.
- [x] Update edge validation to use IR plan bundle + contract hashes.
- [x] Remove catalog-only evidence pathways.

---

## Scope Item L7 — Remove catalog-based contract population for semantic outputs

**Why**: Semantic contracts must be deterministic from IR + schema specs, not catalog inference.

**Representative code pattern to restrict**
```python
# datafusion_engine/schema/contract_population.py
schema = ctx.table(table_name).schema()
```

**Target files**
- `src/datafusion_engine/schema/contract_population.py`

**Delete / replace**
- Restrict catalog schema inference to non-semantic/extract tables.
- Prefer `schema_spec` + IR metadata for semantic outputs.

**Implementation checklist**
- [x] Gate catalog-based population away from semantic outputs.
- [x] Ensure IR contracts are the canonical source for semantic outputs.

---

## Scope Item L8 — Make symtable-derived views explicit in IR

**Why**: IR finalize currently depends on `symtable_*` views without explicit IR registration.

**Representative code pattern to add**
```python
# semantics/ir_pipeline.py
SemanticIRView(name="symtable_bindings", kind="normalize", inputs=("symtable_scopes",), outputs=("symtable_bindings",))
```

**Target files**
- `src/semantics/ir_pipeline.py`
- `src/semantics/ir.py`
- `src/datafusion_engine/symtable/views.py`

**Delete / replace**
- Move symtable derived view registration into IR.
- Ensure `view_graph_nodes()` registers these views via IR planning.

**Implementation checklist**
- [x] Add symtable derived views to IR.
- [x] Ensure IR builder supports symtable view inputs.
- [x] Update IR snapshot tests.

---

## Cross-Cutting Acceptance Gates

1. **IR-only registration**: No non-IR view registrations remain in Hamilton or DataFusion runtime.
2. **Canonical outputs**: No legacy alias names; all outputs match IR canonical names.
3. **Evidence/contract alignment**: Evidence and contract validation use IR artifacts first.
4. **Single finalization path**: CPG outputs finalized only in IR.
5. **Test alignment**: All tests updated to target IR outputs and artifacts.

---

## Execution Order (Suggested)

1. Remove semantic registry hook (L1)
2. Remove Hamilton CPG finalize/outputs (L2)
3. Align output catalogs to IR (L3)
4. Remove aliases (L4)
5. Remove legacy view-spec stubs (L5)
6. Rework evidence/contract derivation (L6, L7)
7. Add IR symtable derived views (L8)

---

## Implementation Checklist (Summary)

- [x] L1 semantic registry hook removal complete
- [x] L2 Hamilton CPG finalize/output subDAG removal complete
- [x] L3 semantic output catalogs updated to IR outputs
- [x] L4 alias layer removed
- [x] L5 legacy view-spec registration removed
- [x] L6 relspec evidence/contract alignment updated
- [x] L7 contract population gated to non-semantic outputs
- [x] L8 symtable derived views added to IR
