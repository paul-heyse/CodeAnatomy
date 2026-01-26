# View‑First Programmatic Pipeline — Best‑in‑Class Implementation Plan (v1)

## Purpose
Pivot to a **100% programmatic** pipeline where the only static inputs are:
1) **extract dataset registrations** (physical inputs), and
2) **view specifications** (logical outputs).

All downstream datasets, schemas, tasks, and scheduling are **derived** from those two inputs. This eliminates drift risk and enables arbitrary view/spec changes without modifying auxiliary registries or catalogs.

## Principles
- **View‑first**: every derived dataset is a view; no plan builders or task catalogs.
- **Schema derived**: view schema is inferred from the compiled view definition + base extract tables.
- **Contract‑metadata only**: contracts add metadata/validation rules but do not define column lists.
- **AST‑first**: SQLGlot AST (or Ibis IR) is the canonical definition; SQL strings are debug only.
- **Rust‑UDF‑first**: UDF snapshot is the single function registry; required UDFs are derived from AST.
- **Programmatic scheduling**: rustworkx builds DAG from view lineage; no static task ordering.

---

# Scope 1 — Remove static view schemas (derive at registration time)

## Goal
Delete all static view schemas and derive view schema from view builders/AST and extract inputs.

Status: PARTIAL — view schema is derived for view-driven pipelines; static registry schemas remain.

### Representative pattern
```python
# schema is derived from the view builder, not declared
spec = view_spec_from_builder(ctx, name=name, builder=builder, sql=None)
contract = SchemaContract.from_arrow_schema(name, spec.schema)
contract = contract.with_metadata(required_udfs=required_udfs_from_ast(ast))
```

### Target files
- Modify: `src/datafusion_engine/schema_registry.py`
- Modify: `src/datafusion_engine/view_registry.py`
- Modify: `src/datafusion_engine/view_registry_specs.py`
- Modify: `src/datafusion_engine/view_graph_registry.py`
- Modify: `src/datafusion_engine/schema_contracts.py`

### Deletions
- Delete: `VIEW_SCHEMA_REGISTRY` (all derived view schemas)
- Delete: `SYMTABLE_*_VIEW_SCHEMA` and other static view schemas
- Delete: any `schema_for(view_name)` usage that assumes static view schema

### Checklist
- [ ] Remove static view schema registries and constants (SCHEMA_REGISTRY/SCIP view schemas still exist).
- [x] Derive view schema via builder or compiled AST at registration time for view‑driven nodes.
- [ ] Ensure contracts are built from derived schema + metadata only (schema_contracts still enforces column lists).
- [x] Remove empty‑table fallbacks that depend on static view schemas (CPG/props paths now derive from plan schema).
- [ ] Enforce missing view schema as a hard error globally (symtable views enforce; registry views still infer silently).

---

# Scope 2 — Contract‑metadata enforcement (no static columns)

## Goal
Schema contracts become **metadata‑only** constraints; column lists are inferred from view definitions.

Status: PARTIAL — relspec contracts are metadata‑only; schema_contracts remains column‑based.

### Representative pattern
```python
schema = derive_view_schema(builder)
contract = SchemaContract.from_arrow_schema(name, schema)
contract = contract.apply_metadata(required_udfs, ordering_keys, evidence_meta)
validate_schema_contract(contract, ctx)  # validates inferred schema only
```

### Target files
- Modify: `src/datafusion_engine/schema_contracts.py`
- Modify: `src/datafusion_engine/view_graph_registry.py`
- Modify: `src/datafusion_engine/schema_registry.py`

### Deletions
- Delete: view‑schema column maps used as authoritative sources
- Delete: contract helpers that assume static view schemas

### Checklist
- [ ] Validate schema contract against inferred schema only (global contract validation still expects column lists).
- [x] Require metadata validation (UDFs, ordering, evidence tags) for view‑driven nodes.
- [ ] Remove column‑list assumptions from contracts (schema_contracts/registry still rely on column lists).

---

# Scope 3 — View spec registry as single orchestration surface

## Goal
Make the view spec registry the **only** orchestration layer. Eliminate plan builders and static task catalogs.

Status: PARTIAL — view graph drives orchestration, but legacy plan catalog modules remain.

### Representative pattern
```python
nodes = view_graph_nodes(ctx, snapshot=snapshot)
register_view_graph(ctx, nodes=nodes, snapshot=snapshot)
```

### Target files
- Modify: `src/datafusion_engine/view_registry_specs.py`
- Modify: `src/datafusion_engine/view_registry.py`
- Modify: `src/normalize/view_builders.py`
- Modify: `src/relspec/*`
- Modify: `src/cpg/*`

### Deletions
- Delete: `src/normalize/ibis_plan_builders.py`
- Delete: `src/relspec/relationship_plans.py`
- Delete: `src/cpg/plan_builders.py`
- Delete: any static task catalogs in normalize/relspec/cpg

### Checklist
- [x] All derived datasets are defined as view specs (normalize/relspec/CPG via view_graph_nodes).
- [x] Register views topologically via graph registry.
- [ ] Remove all plan builder execution paths (relspec plan_catalog + task_catalog modules still present).

---

# Scope 4 — Auto‑generate normalize views from dataset specs

## Goal
Remove manual normalize view enumeration. Derive normalize views from dataset spec registry.

Status: PARTIAL — dataset rows drive ordering, but manual builder registry still exists.

### Representative pattern
```python
for spec in normalize_dataset_specs():
    view = build_view_from_dataset_spec(spec)  # ExprIR → Ibis → AST
    nodes.append(ViewNode(name=spec.name, builder=view.builder, deps=view.deps))
```

### Target files
- Modify: `src/normalize/dataset_specs.py`
- Modify: `src/normalize/dataset_builders.py`
- Modify: `src/normalize/view_builders.py`
- Modify: `src/datafusion_engine/view_registry_specs.py`

### Deletions
- Delete: `_NORMALIZE_VIEW_SPEC_MAP` and manual spec lists
- Delete: any normalize view spec enumerations

### Checklist
- [x] Normalize view specs derived from dataset registry order.
- [x] ExprIR/SqlExprSpec drives view expression construction.
- [ ] No manual normalize view list remains (_NORMALIZE_VIEW_BUILDERS registry still required for custom plans).

---

# Scope 5 — Derived dependency graph from SQLGlot lineage

## Goal
Compute view dependencies exclusively from AST lineage; remove manual dependency declarations.

Status: COMPLETE — dependencies and required UDFs are inferred from AST for all view nodes (including aliases).

### Representative pattern
```python
deps = referenced_tables(ast)
required_udfs = referenced_udf_calls(ast)
```

### Target files
- Modify: `src/datafusion_engine/view_registry_specs.py`
- Modify: `src/sqlglot_tools/lineage.py`
- Modify: `src/datafusion_engine/view_graph_registry.py`

### Deletions
- Delete: any hard‑coded dependency lists for views

### Checklist
- [x] Dependencies derived from SQLGlot AST lineage.
- [x] Required UDFs derived from AST calls.
- [x] Missing dependencies cause a hard error.

---

# Scope 6 — Rust UDF snapshot as sole function registry

## Goal
Rust UDF snapshot is the **only** function registry; required UDFs are derived from AST.

Status: PARTIAL — snapshots drive required UDFs for views, but Python registries still exist.

### Representative pattern
```python
snapshot = install_rust_udf_platform(ctx).snapshot
validate_required_udfs(snapshot, required_udfs_from_ast(ast))
```

### Target files
- Modify: `src/datafusion_engine/udf_platform.py`
- Modify: `src/datafusion_engine/udf_runtime.py`
- Modify: `src/datafusion_engine/view_graph_registry.py`

### Deletions
- Delete: static Python UDF requirement maps
- Delete: any fallback UDF lists in Python

### Checklist
- [x] Required UDFs are inferred from view AST.
- [x] Snapshot validation is mandatory before view registration.
- [ ] Remove Python UDF registries and fallback lanes (ibis/engine registries still present).

---

# Scope 7 — Materialization uses views only

## Goal
Materialization is downstream and reads from registered views only.

Status: PARTIAL — view materialization is enforced in main execution path; legacy plan paths still exist.

### Representative pattern
```python
ensure_view_graph(ctx)
result = facade.execute("SELECT * FROM cpg_edges_v1")  # debug only
write_pipeline.write_view("cpg_edges_v1")
```

### Target files
- Modify: `src/engine/materialize_pipeline.py`
- Modify: `src/datafusion_engine/write_pipeline.py`
- Modify: `src/datafusion_engine/execution_facade.py`

### Deletions
- Delete: plan‑based materialization for derived datasets

### Checklist
- [x] Materialization consumes view names only in task execution and build_view_product.
- [ ] Plan execution paths for derived datasets removed (plan_catalog/execute_plan_artifact still exist).
- [x] Streaming executor stays intact.

---

# Scope 8 — Programmatic scheduling via rustworkx

## Goal
Derive scheduling DAG from view specs + extract datasets. Use rustworkx for topo order, readiness, and incremental rebuilds.

Status: COMPLETE — scheduling now uses rustworkx graphs derived from view nodes.

### Representative pattern
```python
# evidence nodes = extract datasets
# view nodes = derived views
# deps derived from AST lineage

g = build_view_graph(nodes, deps)
order = rx.lexicographical_topological_sort(g, key=lambda n: n.name)
```

### Target files
- Modify: `src/relspec/rustworkx_graph.py`
- Modify: `src/relspec/plan_catalog.py`
- Modify: `src/relspec/execution.py`

### Deletions
- Delete: static task catalogs + manual ordering

### Checklist
- [x] rustworkx DAG built from view specs.
- [x] deterministic ordering via lexicographical topo sort.
- [x] incremental rebuild via ancestors/descendants.

---

# Scope 9 — AST‑first compilation and diagnostics

## Goal
All view compilation uses AST as canonical. SQL strings are debug‑only artifacts.

Status: PARTIAL — AST is canonical for compilation, but SQL‑string execution paths still exist.

### Representative pattern
```python
compiled = compile_sql_policy(raw_ast, schema_mapping, profile)
execute_sqlglot_ast(ctx, compiled.ast)
```

### Target files
- Modify: `src/datafusion_engine/compile_pipeline.py`
- Modify: `src/sqlglot_tools/optimizer.py`
- Modify: `src/sqlglot_tools/bridge.py`

### Deletions
- Delete: internal SQL string execution paths

### Checklist
- [x] AST canonicalization used for fingerprints/lineage.
- [ ] SQL strings only emitted for diagnostics (SQL string execution paths still present in legacy utilities).

---

# Deferred deletions (only after full scope completion)

These items should only be deleted when **all scopes above** are complete and validated:

- Any remaining **plan builder or task catalog** files still used by orchestration.
- Any view helper relying on **static schema registries**.
- Any **SQL string execution** in internal paths (allowed only in debug tooling).

---

## Final state checklist
- [ ] View schemas are fully derived; no static view schema registry remains.
- [ ] Contracts only attach metadata; no column lists are hardcoded.
- [ ] View registry is the single orchestration surface.
- [ ] Normalize views are generated from dataset specs programmatically.
- [x] View dependency DAG derived from AST lineage only.
- [ ] Rust UDF snapshot drives all function requirements (Python registries removed).
- [x] Scheduling is fully rustworkx‑driven and programmatic.
- [ ] Materialization consumes views exclusively with no plan‑based paths.
