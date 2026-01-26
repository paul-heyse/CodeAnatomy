# DataFusion UDF Unified Architecture Alignment Plan (v1)

> **Goal**: Converge the codebase on a single, best-in-class execution architecture centered on Rust DataFusion UDFs + Ibis IR + SQLGlot AST, with view-first, programmatic pipelines and Delta-native IO. Design-phase: aggressive migration and deletion is expected.

> **Principles**
> - **Single IR path**: Ibis → SQLGlot AST → canonicalized AST → execution (no SQL strings in internal paths).
> - **Single UDF source of truth**: Rust registry snapshot drives Ibis builtins, function registry, rewrite tags, docs, and required UDF validation.
> - **View-first programmatic**: extract registrations + view specs are the only static inputs; all schemas, tasks, and dependencies are derived.
> - **Schema derived + metadata contracts**: view schemas are inferred from AST/builder; contracts attach metadata only.
> - **Programmatic scheduling**: rustworkx derives DAG order, readiness, and incremental rebuilds.
> - **Delta-native IO**: Delta TableProvider (including CDF) is the default scan path.

---

## Scope 1 — Canonical compilation pipeline (AST-first execution)

**Goal**: Make SQLGlot AST the canonical compiled artifact. SQL strings are debug output only.  
**Why**: Eliminates SQL string divergence and makes policy, lineage, and cache keys stable.

**Status**: Complete — AST-first execution is enforced; raw SQL compilation is removed. SQL ingress is parsed to AST before compilation.

### Representative pattern

```python
# src/datafusion_engine/compile_pipeline.py
compiled = compile_sql_policy(
    raw_ast,
    schema=schema_mapping,
    profile=policy_profile,
)
execution_plan = execute_sqlglot_ast(ctx, compiled.ast, dialect=policy_profile.write_dialect)
return CompiledExpression(
    ibis_expr=expr,
    sqlglot_ast=compiled.ast,
    rendered_sql=compiled.rendered_sql,  # debug only
    artifacts=compiled.artifacts,
)
```

### Target files
- `src/datafusion_engine/compile_pipeline.py`
- `src/datafusion_engine/compile_options.py`
- `src/datafusion_engine/sql_policy_engine.py`
- `src/sqlglot_tools/bridge.py`
- `src/sqlglot_tools/optimizer.py`

### Deletions
- Any internal use of `SessionContext.sql(...)` for pipeline-generated SQL.

### Implementation checklist
- [x] Add AST execution path as default.
- [x] Keep rendered SQL only as diagnostics artifact.
- [x] Canonicalize: qualify → normalize identifiers → annotate types → canonicalize.
- [x] Ensure lineage + fingerprint derived from AST, not SQL text.
- [x] Remove internal SQL compilation fallback (`compile_sql`) from execution paths.
- [x] Parse explicit SQL ingress to AST before compilation.

---

## Scope 2 — Parameterization & prepared statements (no SQL interpolation)

**Goal**: Remove all SQL string interpolation in internal execution.  
**Why**: Guarantees type-safe execution and injection-safe control plane.

**Status**: In progress — parameter binding uses native params; info_schema introspection uses AST builders, but schema introspection still issues SQL text for DESCRIBE/prepared-statement fallback and UDF catalog uses SHOW FUNCTIONS.

### Representative pattern

```python
# src/datafusion_engine/parameterized_execution.py
param = ibis.param("string")
expr = table.filter(table.col == param)
df = backend.execute(expr, params={"param": "value"})
```

### Target files
- `src/datafusion_engine/parameterized_execution.py`
- `src/datafusion_engine/sql_safety.py`
- `src/ibis_engine/sql_bridge.py`
- `src/datafusion_engine/compile_pipeline.py`

### Deletions
- SQL string substitution paths in parameterized execution.

### Implementation checklist
- [x] Replace string substitution with `ibis.param` + `params`.
- [x] Restrict SQL safety checks to external SQL ingress only (compile-time enforcement for raw SQL).
- [x] Remove remaining internal SQL string execution in diagnostics (EXPLAIN/describe); diagnostics now run against AST/DF only.
- [x] Replace internal information_schema/introspection SQL text builders with AST builders.
- [ ] Replace remaining internal SQL text builders (schema introspection DESCRIBE/prepared statement fallback, SHOW FUNCTIONS) with AST builders or DataFusion APIs where feasible.

---

## Scope 3 — Rust UDF platform as the only function registry (snapshot + AST requirements)

**Goal**: Ensure Rust snapshot drives all function metadata and binding; required UDFs derived from AST.  
**Why**: Prevents drift and removes duplicate registries.

**Status**: In progress — snapshot drives Ibis builtins and the function registry; AST-derived required UDFs are now validated during view registration, but remaining non-view surfaces still need to be consolidated.

### Representative pattern

```python
# src/datafusion_engine/udf_platform.py
snapshot = register_rust_udfs(ctx, enable_async=...)
required_udfs = required_udfs_from_ast(ast)
validate_required_udfs(snapshot, required_udfs)
register_ibis_udf_snapshot(snapshot)
registry = build_function_registry(options=FunctionRegistryOptions(
    registry_snapshot=snapshot,
    datafusion_function_catalog=function_catalog,
))
```

### Target files
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/udf_platform.py`
- `src/ibis_engine/builtin_udfs.py`
- `src/datafusion_engine/udf_catalog.py`
- `src/datafusion_engine/view_graph_registry.py`
- `src/datafusion_engine/function_factory.py`

### Deletions
- Any remaining static UDF lists or manual operator maps used as authoritative metadata.

### Implementation checklist
- [x] Snapshot includes arg names, return types, rewrite tags, docs.
- [x] Ibis builtins are registered exclusively from snapshot.
- [x] Function registry is snapshot-only (info_schema validation optional).
- [x] Required UDFs are derived from AST and validated before view registration.
- [x] Remove any parallel UDF lane logic.

---

## Scope 4 — View spec registry as single orchestration surface

**Goal**: Make the view spec registry the only orchestration layer.  
**Why**: View specs become the only pipeline graph definition.

**Status**: In progress — relspec task/plan catalog surfaces are removed and normalize view specs are programmatic; registry fragment views now register via view graph with AST. Remaining non-view orchestration surfaces (if any) should be folded into the view registry.

### Representative pattern

```python
# src/datafusion_engine/view_registry_specs.py
@view_spec("relation_output_v1")
def relation_output_v1(ctx: ViewBuildContext) -> IbisPlan:
    return cpg_edges_v1(ctx).union(cpg_nodes_v1(ctx))
```

### Target files
- `src/datafusion_engine/view_registry_specs.py`
- `src/datafusion_engine/view_registry.py`
- `src/datafusion_engine/view_graph_registry.py`
- `src/normalize/*`
- `src/relspec/*`
- `src/cpg/*`

### Deletions
- Normalize/relspec/CPG legacy plan builders once views are authoritative:
  - `src/normalize/ibis_plan_builders.py`
  - `src/relspec/relationship_plans.py`
  - `src/cpg/plan_builders.py`
- Legacy normalize catalog helper once view registry is authoritative:
  - `src/normalize/catalog.py`
- Any static task catalogs or orchestration maps once view graph registry is authoritative.

### Implementation checklist
- [ ] All pipelines expressed as views.
- [x] View specs enforce UDF requirements + schema contracts.
- [x] Materialization stage consumes views only.
- [x] Registry fragment views register via view graph (AST-backed).

---

## Scope 5 — Programmatic schema derivation + metadata-only contracts (no static view schemas)

**Goal**: Derive view schemas from AST/builders at registration time; contracts only attach metadata.  
**Why**: Eliminates drift from static schema registries and enables full programmatic pipelines.

**Status**: In progress — view graph registration derives schema from AST/builders and missing AST/schema is a hard error, but static view schema registries still exist in `schema_registry` (AST/TREE_SITTER/SCIP/CST/etc) and contracts still carry column lists derived from schemas. Remaining schema registry surfaces still exist for non-view datasets.

### Representative pattern

```python
# src/datafusion_engine/view_graph_registry.py
spec = view_spec_from_builder(ctx, name=name, builder=builder)
schema = derive_view_schema(spec)
contract = SchemaContract.from_arrow_schema(name, schema)
contract = contract.apply_metadata(required_udfs, ordering_keys, evidence_meta)
validate_schema_contract(contract, ctx)
```

### Target files
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/schema_contracts.py`
- `src/datafusion_engine/view_registry.py`
- `src/datafusion_engine/view_graph_registry.py`

### Deletions
- Static view schema registries and constants (`VIEW_SCHEMA_REGISTRY`, `SYMTABLE_*_VIEW_SCHEMA`).
- Any `schema_for(view_name)` usage that assumes static view schemas.
- Empty-table fallbacks that depend on static view schema declarations.

### Implementation checklist
- [x] Derive view schema from builder/AST at registration time for view graph registration.
- [ ] Remove static view schema registries for view outputs (AST/TREE_SITTER/SCIP/CST/etc) and any schema_for(view) usage.
- [ ] Contracts validate inferred schema only; metadata is additive.
- [x] Missing view schema/AST is a hard error (no silent default).

---

## Scope 6 — Auto-generate normalize views from dataset specs

**Goal**: Remove manual normalize view enumeration; derive from dataset spec registry.  
**Why**: Eliminates manual lists and guarantees consistent coverage.

**Status**: Complete — normalize view specs are registered programmatically from dataset rows; manual map removed.

### Representative pattern

```python
# src/normalize/view_builders.py
for spec in normalize_dataset_specs():
    view = build_view_from_dataset_spec(spec)
    nodes.append(ViewNode(name=spec.name, builder=view.builder, deps=view.deps))
```

### Target files
- `src/normalize/dataset_specs.py`
- `src/normalize/dataset_builders.py`
- `src/normalize/view_builders.py`
- `src/datafusion_engine/view_registry_specs.py`

### Deletions
- `_NORMALIZE_VIEW_SPEC_MAP` and manual normalize view spec lists.

### Implementation checklist
- [x] Normalize view specs derived from dataset registry order.
- [x] ExprIR/SqlExprSpec drives view expression construction.
- [x] No manual normalize view list remains.

---

## Scope 7 — AST lineage dependencies + required UDFs

**Goal**: Compute view dependencies and required UDFs exclusively from AST lineage.  
**Why**: Removes hard-coded dependency lists and enforces UDF requirements programmatically.

**Status**: Complete — view nodes require SQLGlot AST; deps and required UDFs are derived exclusively from AST lineage and missing AST is a hard error.

### Representative pattern

```python
# src/sqlglot_tools/lineage.py
deps = referenced_tables(ast)
required_udfs = referenced_udf_calls(ast)
```

### Target files
- `src/sqlglot_tools/lineage.py`
- `src/datafusion_engine/view_registry_specs.py`
- `src/datafusion_engine/view_graph_registry.py`
- `src/datafusion_engine/udf_platform.py`

### Deletions
- Any hard-coded dependency lists or Python-side UDF requirement maps.

### Implementation checklist
- [x] Dependencies derived exclusively from SQLGlot AST lineage.
- [x] Required UDFs derived exclusively from AST calls.
- [x] AST-derived deps/required UDFs computed for view nodes that supply SQLGlot AST.
- [x] Missing dependencies or UDFs cause a hard error.

---

## Scope 8 — Materialization uses views only

**Goal**: Materialization is downstream and reads from registered views only.  
**Why**: Ensures plan builders never re-enter the pipeline.

**Status**: Complete — materialization is view-only and plan-based materialization raises.

### Representative pattern

```python
# src/datafusion_engine/write_pipeline.py
ensure_view_graph(ctx)
write_pipeline.write_view("cpg_edges_v1")
```

### Target files
- `src/engine/materialize_pipeline.py`
- `src/datafusion_engine/write_pipeline.py`
- `src/datafusion_engine/execution_facade.py`

### Deletions
- Plan-based materialization for derived datasets.

### Implementation checklist
- [x] Materialization consumes view names only.
- [x] Plan execution paths for derived datasets removed.
- [x] Streaming executor stays intact.

---

## Scope 9 — Programmatic scheduling via rustworkx

**Goal**: Derive scheduling DAG from view specs + extract datasets.  
**Why**: Enables deterministic ordering, readiness checks, and incremental rebuilds.

**Status**: In progress — view graph topo sort uses rustworkx and relspec plan/task catalog modules are removed; scheduling still needs final DAG consolidation with extract datasets and incremental rebuild logic.

### Representative pattern

```python
# src/relspec/rustworkx_graph.py
order = rx.lexicographical_topological_sort(graph, key=lambda n: n.name)
```

### Target files
- `src/relspec/rustworkx_graph.py`
- `src/relspec/graph_inference.py`
- `src/relspec/incremental.py`

### Deletions
- Static task catalogs + manual ordering logic.
- Legacy relspec plan/task catalog modules (`src/relspec/plan_catalog.py`, `src/relspec/task_catalog.py`, `src/relspec/task_catalog_builders.py`, `src/relspec/execution.py`).

### Implementation checklist
- [ ] rustworkx DAG built from view specs and extract datasets.
- [ ] Deterministic ordering via lexicographical topo sort across scheduling surfaces.
- [x] View graph topo sort uses rustworkx lexicographical ordering when available.
- [ ] Incremental rebuild via ancestors/descendants.
- [x] Legacy relspec plan/task catalogs removed.

---

## Scope 10 — Schema contracts: nested types as first-class CPG ABI

**Goal**: Use DataFusion’s nested type functions as the canonical schema ABI.  
**Why**: Structured schema is the contract for CPG-style data products.

**Status**: In progress — Arrow field metadata is preserved end-to-end; nested type ABI standardization and DataFusion idioms still need consolidation.

### Representative pattern

```sql
SELECT
  named_struct('span', span_struct, 'attrs', attrs_map) AS node
FROM nodes_v1
```

### Target files
- `src/arrowdsl/schema/*`
- `src/datafusion_engine/schema_contracts.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/nested_tables.py`
- `src/datafusion_engine/udf_catalog.py`

### Deletions
- Legacy schema construction helpers that bypass Arrow schema metadata.

### Implementation checklist
- [ ] Standardize struct/map/union shapes for all view outputs.
- [x] Preserve Arrow field metadata end-to-end.
- [ ] Use `map_*`, `union_*`, `get_field`, `unnest` idioms per DataFusion schema docs.

---

## Scope 11 — Delta-native IO (TableProvider + CDF)

**Goal**: Prefer Delta TableProvider for all Delta scans; eliminate parquet fallbacks.  
**Why**: Delta offers stronger schema/metadata contracts and pruning.

**Status**: In progress — registry bridge enforces native provider and incremental/CDF paths resolve Delta scan/log options; registry loaders now resolve Delta scan/log options; remaining Delta IO surfaces still need consolidation.

### Representative pattern

```python
# src/datafusion_engine/registry_bridge.py
adapter.register_delta_table_provider(table_spec, artifacts)
adapter.register_delta_cdf_provider(table_spec, artifacts)
```

### Target files
- `src/datafusion_engine/io_adapter.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/storage/deltalake/*`
- `src/incremental/*`

### Deletions
- Parquet fallback paths for Delta in internal execution flows.

### Implementation checklist
- [x] Enforce Delta TableProvider usage for Delta datasets in registry bridge.
- [x] Use CDF provider for change capture when configured.
- [x] Resolve DeltaScanConfig + log storage options in incremental/CDF runtime paths.
- [x] Resolve DeltaScanConfig + log storage options in registry loaders.
- [ ] Expose/standardize DeltaScanConfig + object store wiring across all Delta IO surfaces (storage/io_adapter/write).

---

## Scope 12 — SQL ingress isolation

**Goal**: Limit SQL string surfaces to external ingress only.  
**Why**: Keeps internal execution AST-first.

**Status**: In progress — internal execution is AST-only and SQL ingress parses to AST; remaining SQL text generation is limited to schema introspection DESCRIBE/prepared fallback and UDF catalog SHOW FUNCTIONS.

### Representative pattern

```python
# src/datafusion_engine/sql_safety.py
def validate_external_sql(sql: str, policy: SQLPolicyProfile) -> None:
    ast = parse_sql_strict(sql, dialect=policy.write_dialect)
    enforce_policy(ast, policy)
```

### Target files
- `src/datafusion_engine/sql_safety.py`
- `src/ibis_engine/sql_bridge.py`
- `src/datafusion_engine/compile_pipeline.py`

### Deletions
- Internal SQL execution helpers used in compiled pipelines.

### Implementation checklist
- [x] Enforce SQL safety only for external SQL.
- [x] Replace remaining internal SQL execution with AST/IR execution (diagnostics/explain, view-spec SQL inference).
- [x] Replace internal info_schema/introspection SQL with AST builders where feasible.
- [ ] Replace remaining internal SQL text builders with AST builders where feasible (schema introspection describe/prepared fallback, SHOW FUNCTIONS).

---

## Scope 13 — Diagnostics & parity gates (UDF + schema)

**Goal**: Make UDF and schema parity checks mandatory at runtime.  
**Why**: Prevents drift between Rust registry, Ibis, and information_schema.

**Status**: Complete — parity validation is wired in runtime and diagnostics.

### Representative pattern

```python
report = udf_info_schema_parity_report(ctx)
if report.missing_in_information_schema:
    raise ValueError("UDF parity failed")
```

### Target files
- `src/datafusion_engine/udf_parity.py`
- `src/datafusion_engine/diagnostics.py`
- `src/relspec/evidence.py`

### Deletions
- None.

### Implementation checklist
- [x] Enforce parity checks at runtime profile init.
- [x] Record parity artifacts for diagnostics.

---

# Deferred decommissioning (only after scopes 1–13 are complete)

These items are legacy/parallel surfaces that can only be deleted once the new architecture is fully deployed and validated:
- [x] `src/datafusion_engine/bridge.py` (legacy SQL compilation + execution helpers).
- [x] Legacy relspec plan/task catalog modules (removed).
- [x] `src/datafusion_engine/view_registry_defs.py` (static view lists once view specs + DAG are authoritative).
- [x] `src/normalize/catalog.py` (legacy normalize catalog helper).
- [ ] `src/ibis_engine/expr_compiler.py` (manual operator maps once fully snapshot-driven).
- [ ] `src/datafusion_engine/parameterized_execution.py` (if all execution moves to AST-first + params).
- [ ] `src/ibis_engine/sql_bridge.py` (if external SQL ingestion is isolated elsewhere).
- [ ] `src/arrowdsl/schema/serialization.py` (if schema contracts fully move to DataFusion schema metadata).
- [ ] Any remaining plan builder modules still referenced by orchestration.
- [ ] Any static view schema registries or column maps not removed in Scope 5.

---

## Final State Checklist
- [ ] No internal SQL string execution paths remain.
- [x] Rust snapshot drives every UDF registration and metadata surface.
- [x] View registry is the only pipeline definition surface.
- [ ] View schemas are fully derived (no static view schema registry remains).
- [ ] Contracts only attach metadata; no column lists are hardcoded.
- [x] Normalize views are generated programmatically from dataset specs.
- [x] View dependency DAG derived from AST lineage only.
- [ ] Scheduling is fully rustworkx-driven and programmatic.
- [x] Materialization consumes views exclusively.
- [ ] Delta TableProvider is the default scan path.
- [ ] Schema contracts enforced end-to-end with nested type ABI.
- [x] Parity checks are enforced at runtime initialization.
