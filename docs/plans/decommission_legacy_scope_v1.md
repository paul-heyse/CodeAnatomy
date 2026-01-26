# Legacy Decommissioning Plan (View-First End State) v1

> **Goal**: Fully decommission legacy/unused modules and tests that are no longer aligned with the AST-first, view-driven DataFusion execution architecture. This plan is scoped to safe deletions and cleanups, **excluding** the separate Hamilton orchestration rewrite.
>
> **Final Design End-State**
> - **Single execution surface**: view registry + view graph registration are the only orchestration inputs.
> - **Single compilation path**: Ibis -> SQLGlot AST -> canonicalization -> execution. No internal SQL string execution.
> - **Single UDF source of truth**: Rust snapshot drives Ibis builtins, registry, and validation.
> - **Derived schemas only**: view schema derivation at registration time; contracts attach metadata only.
> - **Strict, minimal public API**: no dormant APIs or shadow registries.

---

## Scope A: Orphaned Internal Modules (No Inbound Imports)

### Items
- `src/datafusion_engine/extract_builders.py`
- `src/datafusion_engine/extract_pipelines.py`
- `src/extract/evidence_specs.py`
- `src/normalize/output_writes.py`

### Replacement Patterns (Representative)
These modules are replaced by view-first registration and runtime schema contracts.

```python
# src/datafusion_engine/view_registry_specs.py
nodes = view_graph_nodes(ctx, snapshot=snapshot)
register_view_graph(session_ctx, nodes=nodes, snapshot=snapshot)
```

```python
# src/datafusion_engine/view_graph_registry.py
schema = derive_view_schema(spec)
contract = SchemaContract.from_arrow_schema(name, schema).apply_metadata(...)
```

### Target Files
- Delete:
  - `src/datafusion_engine/extract_builders.py`
  - `src/datafusion_engine/extract_pipelines.py`
  - `src/extract/evidence_specs.py`
  - `src/normalize/output_writes.py`
- Modify (if references exist in docs/tests):
  - `docs/` references to these modules
  - Any README/CLAUDE references

### Implementation Checklist
- [ ] Confirm no inbound imports in `src/`, `tests/`, `scripts/`, `docs/`.
- [ ] Delete files listed above.
- [ ] Remove or update doc references.
- [ ] Run quality gates: `uv run ruff check --fix`, `uv run pyrefly check`, `uv run pyright --warnings --pythonversion=3.13`.

---

## Scope B: Orphaned but Producer-Dependent (Line Index)

### Items
- `src/extract/line_index.py`

### Decision Gate
This module builds `file_line_index_v1`. It can only be deleted once **no other producer is required**, or the dataset itself is removed from the extract registry and all downstream view usage is deleted.

### Replacement Patterns (Representative)
Preferred end state: **views operate on derived spans** without a bespoke line index producer.

```python
# src/normalize/ibis_spans.py
line_idx = _table_expr(line_index, backend=backend, name="file_line_index")
start_idx = _line_index_view(line_idx, prefix="start")
end_idx = _line_index_view(line_idx, prefix="end")
```

If line index is retired, replace with derived spans from existing CST/SCIP sources:

```python
# src/normalize/view_builders.py (conceptual)
span = _span_from_cst(cst)
# no separate file_line_index dependency
```

### Target Files
- Delete (only after decision gate):
  - `src/extract/line_index.py`
- Modify (if retiring dataset):
  - `src/datafusion_engine/extract_templates.py` (remove `file_line_index_v1` dataset row)
  - `src/normalize/view_builders.py`, `src/normalize/ibis_spans.py` (remove dependency)
  - Any view registry specs that rely on `file_line_index_v1`

### Implementation Checklist
- [ ] Verify whether any runtime producer still requires `file_line_index_v1`.
- [ ] If retiring the dataset, remove all downstream usage and registry entries.
- [ ] Delete `src/extract/line_index.py` only after the above is complete.
- [ ] Run quality gates.

---

## Scope C: Unused Helpers (Not Aligned to View-First)

### Items
- `src/ibis_engine/compiler_checkpoint.py`
- `src/ibis_engine/substrait_bridge.py`
- `src/arrowdsl/schema/structs.py`
- `src/obs/schema_introspection.py`

### Replacement Patterns (Representative)
These are replaced by AST-first compilation and DataFusion-native artifacts.

```python
# src/datafusion_engine/compile_pipeline.py
compiled = compile_sql_policy(raw_ast, schema=schema, profile=profile)
execute_sqlglot_ast(ctx, compiled.ast, dialect=profile.write_dialect)
```

```python
# src/datafusion_engine/schema_contracts.py
contract = SchemaContract.from_arrow_schema(name, schema)
contract = contract.apply_metadata(...)
```

### Target Files
- Delete:
  - `src/ibis_engine/compiler_checkpoint.py`
  - `src/ibis_engine/substrait_bridge.py`
  - `src/arrowdsl/schema/structs.py`
  - `src/obs/schema_introspection.py`
- Modify:
  - Any `__init__.py` exports or doc references

### Implementation Checklist
- [ ] Confirm no dynamic or runtime imports depend on these modules.
- [ ] Delete files listed above.
- [ ] Remove exports from any `__init__.py` or doc references.
- [ ] Run quality gates.

---

## Scope D: Stray File Cleanup

### Items
- `src/extract/Untitled`

### Target Files
- Delete:
  - `src/extract/Untitled`

### Implementation Checklist
- [ ] Delete file.
- [ ] Verify no references in docs or scripts.

---

## Scope E: Test-Only Modules (Delete with Tests)

### Items
- `src/arrowdsl/schema/union_codec.py` + `tests/unit/test_union_codec.py`
- `src/ibis_engine/plan_diff.py` + `tests/unit/test_sqlglot_policy.py`
- `src/obs/diagnostics_tables.py` + `tests/unit/test_diagnostics_tables.py`

### Replacement Patterns (Representative)
Use runtime diagnostics and schema contracts instead of standalone test-only helpers.

```python
# src/datafusion_engine/diagnostics.py
report = udf_info_schema_parity_report(ctx)
if report.missing_in_information_schema:
    raise ValueError("UDF parity failed")
```

```python
# src/datafusion_engine/schema_registry.py
validate_schema_contract(contract, snapshot)
```

### Target Files
- Delete:
  - `src/arrowdsl/schema/union_codec.py`
  - `tests/unit/test_union_codec.py`
  - `src/ibis_engine/plan_diff.py`
  - `tests/unit/test_sqlglot_policy.py`
  - `src/obs/diagnostics_tables.py`
  - `tests/unit/test_diagnostics_tables.py`

### Implementation Checklist
- [ ] Delete modules and tests together.
- [ ] Remove any doc references to the deleted helpers.
- [ ] Run quality gates.

---

## Scope F: API-Only Legacy (Re-exports Only)

### Items
- `src/datafusion_engine/parameterized_execution.py`

### Replacement Patterns (Representative)
Public API should favor param binding + AST execution.

```python
# src/datafusion_engine/param_binding.py
bindings = resolve_param_bindings(params)
apply_bindings_to_context(ctx, bindings)
```

```python
# src/datafusion_engine/compile_pipeline.py
compiled = CompilationPipeline(ctx, options).compile_ibis(expr)
```

### Target Files
- Delete:
  - `src/datafusion_engine/parameterized_execution.py`
- Modify:
  - `src/datafusion_engine/__init__.py` (remove re-exports and TYPE_CHECKING imports)
  - Docs/examples that mention `ParameterSpec`, `ParameterizedRulepack`, `RulepackRegistry`

### Implementation Checklist
- [ ] Confirm external callers have migrated to `param_binding.py` + view execution.
- [ ] Remove re-exports in `datafusion_engine.__init__`.
- [ ] Delete `parameterized_execution.py`.
- [ ] Update docs and examples.
- [ ] Run quality gates.

---

## Global Verification

- [ ] Search for all deleted module names and ensure no remaining references.
- [ ] Run:
  - `uv run ruff check --fix`
  - `uv run pyrefly check`
  - `uv run pyright --warnings --pythonversion=3.13`

---

## Out of Scope (Tracked Separately)

- Hamilton orchestration rewrite (`src/hamilton_pipeline/`, `src/graph/`).
- Full removal of static schema registries (Scope 5 in the unified architecture plan).
- Final deprecation of any remaining SQL ingress fallback paths.
