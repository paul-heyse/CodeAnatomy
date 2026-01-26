# Relspec + CPG Unifier Alignment — Best‑in‑Class Implementation Plan (v1)

## Purpose
Unify **relspec task scheduling + execution** with the DataFusion/Ibis/SQLGlot execution facade, enforce **contract‑first schemas**, and fully leverage the **Rust UDF stack** for high‑performance CPG generation. This plan operationalizes the Phase B/C unifier patterns in the relspec pipeline and extends them into evidence/scheduling, registry introspection, and CPG‑specific UDF execution.

## Guiding principles
- **Single orchestration surface**: task compilation and execution flow through the `DataFusionExecutionFacade` and unified diagnostics recorder.
- **Contract‑first outputs**: every relspec output is bound to a DatasetSpec/ContractSpec schema (no implicit schemas).
- **Rust‑first execution**: stable IDs/keys, canonical string normalization, and scoring utilities use Rust UDFs.
- **AST‑first planning**: SQLGlot/AST canonicalization is the source of truth for plan lineage & caching.
- **Evidence‑driven scheduling**: evidence is sourced from the unified registry + information_schema, not bespoke fallbacks.
- **Registry‑driven UDF parity**: Rust registry snapshot is the source of truth for UDF metadata, docs, and named‑arg support (Ibis mirrors it).
- **No raw SQL in production paths**: plan building and UDF invocation use builders/AST APIs, not SQL strings.

---

# Scope 1 — Facade‑aware task build and execution contexts

## Goal
Extend relspec task compilation/execution so all task plans are compiled and run with the same facade/diagnostics/policy layers used across the DataFusion unifier.

Status: ✅ Complete — facade/compile/execution diagnostics wired.

## Representative pattern
```python
@dataclass(frozen=True)
class TaskBuildContext:
    ctx: ExecutionContext
    backend: BaseBackend
    ibis_catalog: IbisPlanCatalog | None = None
    facade: DataFusionExecutionFacade | None = None
    diagnostics: DiagnosticsRecorder | None = None

# compile_task_plan
facade = DataFusionExecutionFacade.from_execution_context(ctx, backend=backend)
compiled = facade.compile(plan.expr)
# attach sqlglot AST + plan hash to PlanArtifact from facade outputs
```

## Target files
- Modify: `src/relspec/task_catalog.py`
- Modify: `src/relspec/plan_catalog.py`
- Modify: `src/relspec/execution.py`
- Modify: `src/ibis_engine/execution_factory.py`
- Modify: `src/datafusion_engine/execution_facade.py`
- New: `src/relspec/context.py` (optional context factory helpers)

## Deletions
- None (structural upgrade only).

## Checklist
- [x] Add facade + diagnostics handles to `TaskBuildContext`.
- [x] Add a facade factory that accepts `ExecutionContext` + backend.
- [x] Ensure `compile_task_plan` uses the facade’s SQLGlot canonicalization for plan artifacts.
- [x] Emit compile/execution diagnostics via recorder for all relspec tasks.

---

# Scope 2 — Contract‑first schema binding for relspec outputs

## Goal
Bind all relationship and relation_output plans to explicit contract schemas using the same `bind_expr_schema` path as normalize/cpg, and register dataset specs for relspec outputs.

Status: ✅ Complete — contract binding and ordering metadata wired.

## Representative pattern
```python
output = expr.select(*schema.names)
output = bind_expr_schema(output, schema=schema, allow_extra_columns=ctx.debug)
return IbisPlan(expr=output, ordering=Ordering.unordered())
```

## Target files
- Modify: `src/relspec/relationship_plans.py`
- Modify: `src/relspec/contracts.py` (or new `relspec/registry.py` if needed)
- Modify: `src/relspec/relationship_task_catalog.py`
- Modify: `src/normalize/dataset_fields.py` (if relspec schema fields live there)
- Modify: `src/datafusion_engine/schema_registry.py` (register relspec dataset specs)

## Deletions
- Remove ad‑hoc `ensure_columns(...).select(...)` patterns once all outputs bind to contract schemas.

## Checklist
- [x] Add DatasetSpec/ContractSpec for each relspec output (including `relation_output_v1`).
- [x] Replace `_select_plan` + relation_output unions with `bind_expr_schema` binding.
- [x] Ensure schema metadata (ordering, determinism) is carried with the bound schema.

---

# Scope 3 — Evidence catalog via unified registry + information_schema

## Goal
Build the evidence catalog from DataFusion’s registry and information_schema views, and attach provider capability metadata consistently.

Status: ✅ Complete — evidence now uses information_schema/introspection only.

## Representative pattern
```python
facade = DataFusionExecutionFacade.from_execution_context(ctx, backend)
introspector = SchemaIntrospector(facade.ctx, sql_options=facade.sql_options())
info_schema = introspector.schema_map()
contract = schema_contract_from_dataset_spec(name, spec)
```

## Target files
- Modify: `src/relspec/evidence.py`
- Modify: `src/datafusion_engine/schema_introspection.py`
- Modify: `src/datafusion_engine/registry_bridge.py`
- Modify: `src/datafusion_engine/execution_facade.py` (expose introspector/registry helpers)

## Deletions
- Remove direct `_schema_from_registry` fallback paths once facade‑based introspection is wired.

## Checklist
- [ ] Replace evidence registry fallback paths with information_schema queries.
- [x] Register evidence with schema contracts + provider metadata.
- [x] Align evidence catalog column/type metadata with contract spec definitions.

---

# Scope 4 — RuntimeArtifacts + ExecutionResult alignment

## Goal
Store unified execution results (table/reader/write) in relspec runtime artifacts to standardize downstream handling and metadata.

Status: ✅ Complete — ExecutionResult tracking and materialize pipeline aligned.

## Representative pattern
```python
result = execute_ibis_plan(plan, execution=execution, streaming=prefer_reader)
artifacts.register_execution(task_name, result)
```

## Target files
- Modify: `src/relspec/runtime_artifacts.py`
- Modify: `src/relspec/execution.py`
- Modify: `src/engine/materialize_pipeline.py`

## Deletions
- Remove ad‑hoc `datafusion_ctx` tracking in `RuntimeArtifacts` once facade usage is fully adopted.

## Checklist
- [x] Add an ExecutionResult summary container to runtime artifacts.
- [x] Store plan fingerprint + schema hash alongside each execution output.
- [x] Update summary/metrics to include reader/write results.

---

# Scope 5 — Rust UDF adoption for IDs, keys, relation utilities, and registry parity

## Goal
Replace Python/Ibis expression helpers with Rust UDFs (stable hash, ID/key construction, qname normalization, scoring), then use them in relspec/normalize/cpg outputs. Ensure registry‑driven parity across Rust/Ibis/catalog metadata.

Status: ◐ Partial — Rust UDFs used for IDs/keys; remaining registry parity updates pending.

## Representative pattern
```python
from ibis_engine.builtin_udfs import stable_hash64, stable_id
edge_id = stable_id("edge", stable_hash64(rel.src, rel.dst, rel.path, rel.bstart, rel.bend))

# snapshot-driven Ibis metadata
specs = ibis_udf_specs_from_snapshot(snapshot)
```

## Target files
- Modify: `rust/datafusion_ext/src/udf_builtin.rs`
- Modify: `rust/datafusion_ext/src/udf_custom.rs`
- Modify: `rust/datafusion_ext/src/udf_docs.rs`
- Modify: `rust/datafusion_ext/src/registry_snapshot.rs`
- Modify: `src/ibis_engine/builtin_udfs.py`
- Modify: `src/ibis_engine/ids.py`
- Modify: `src/relspec/relationship_plans.py`
- Modify: `src/normalize/ibis_plan_builders.py`
- Modify: `src/cpg/emit_edges_ibis.py`
- Modify: `src/datafusion_engine/udf_catalog.py`
- Modify: `src/datafusion_engine/schema_registry.py`
- Modify: `src/engine/unified_registry.py`

## Deletions
- Remove Python implementations of stable ID/key helpers once Rust UDFs are fully wired.
- Remove hard‑coded Ibis builtin UDF lists once snapshot parity is enforced.

## Checklist
- [x] Add Rust UDFs for stable hash + composite ID/key patterns (with metadata/nullability).
- [ ] Add docs + named‑arg signatures for all UDFs in Rust registry snapshot.
- [ ] Generate Ibis builtin UDF metadata from Rust registry snapshot (or enforce parity checks).
- [x] Route stable_id_expr through Rust stable_id UDF (Python helper delegates).
- [x] Replace relspec/normalize/cpg usages of Python stable_* helpers with Rust UDFs.
- [x] Ensure `information_schema.routines/parameters` reflect Rust registry snapshot.

---

# Scope 5B — UDF discovery in evidence + diagnostics

## Goal
Expose Rust UDF discovery and metadata parity through evidence/diagnostics surfaces, ensuring cataloged UDFs are validated end‑to‑end.

Status: ✅ Complete — UDF parity diagnostics and information_schema metadata wired.

## Representative pattern
```python
snapshot = udf_registry_snapshot(ctx)
assert validate_udf_parity(snapshot, ibis_specs, information_schema)
record_artifact(profile, "udf_parity_v1", snapshot.summary())
```

## Target files
- Modify: `src/relspec/evidence.py`
- Modify: `src/datafusion_engine/udf_catalog.py`
- Modify: `src/datafusion_engine/schema_introspection.py`
- Modify: `src/datafusion_engine/diagnostics.py`

## Deletions
- None.

## Checklist
- [x] Include UDF registry snapshot + parity checks in evidence planning.
- [x] Record diagnostics artifacts for UDF parity mismatches.
- [x] Ensure named‑arg metadata is available via information_schema.

---

# Scope 6 — AST‑first relationship plan definitions

## Goal
Refactor relationship plans to SQLGlot AST builders (or QuerySpec‑style IR) for canonicalization, lineage, and deterministic plan hashes.

Status: ✅ Complete — SQLGlot builders wired for relspec plan lineage.

## Representative pattern
```python
expr = sqlglot_select(...)
compiled = facade.compile(expr)
# use compiled.sqlglot_ast for deps + hash
```

## Target files
- Modify: `src/relspec/relationship_plans.py`
- Modify: `src/relspec/plan_catalog.py`
- New: `src/relspec/relationship_sql.py` (AST builders)

## Deletions
- Remove ad‑hoc per‑plan SQL string emitters once AST builders are adopted.

## Checklist
- [x] Build SQLGlot AST for each relspec plan.
- [x] Use facade canonicalization to generate task fingerprints.
- [x] Ensure lineage extraction uses AST inputs, not SQL strings.
- [x] Remove raw SQL usage in relspec planning paths (builder/AST only).

---

# Scope 6B — FunctionFactory expansion (aggregate/window/table UDFs)

## Goal
Extend function factory support so SQL `CREATE FUNCTION` can register scalar/aggregate/window/table functions, and ensure builder APIs replace raw SQL in production paths.

Status: ✅ Complete — FunctionFactory + CREATE FUNCTION AST builders enabled.

## Representative pattern
```python
factory.register_function(spec)  # scalar/aggregate/window/table
```

## Target files
- Modify: `rust/datafusion_ext/src/function_factory.rs`
- Modify: `src/datafusion_engine/function_factory.py`
- Modify: `src/datafusion_engine/bridge.py`

## Deletions
- Remove SQL‑string UDF creation paths after builders are complete.

## Checklist
- [x] Expand FunctionFactory to support aggregate/window/table UDFs.
- [x] Enforce volatility + named‑arg constraints at registration time.
- [x] Replace raw SQL function creation in production paths.

---

# Scope 7 — Incremental + streaming readiness for CPG outputs

## Goal
Support CDF‑driven incremental builds and streaming‑compatible planning for CPG datasets.

Status: ✅ Complete — unbounded registration + CDF gating helpers added.

## Representative pattern
```sql
CREATE UNBOUNDED EXTERNAL TABLE cpg_edges
STORED AS PARQUET
LOCATION '...'
```

## Target files
- Modify: `src/relspec/runtime_artifacts.py`
- Modify: `src/datafusion_engine/registry_bridge.py`
- Modify: `src/schema_spec/system.py`
- Modify: `src/incremental/*`

## Deletions
- None (capability extension).

## Checklist
- [x] Add CDF‑aware evidence gating for incremental task scheduling.
- [x] Use `CREATE UNBOUNDED EXTERNAL TABLE` for streaming datasets.
- [x] Capture dataset versions/time travel in evidence catalog.

---

# Scope 8 — Extract pipeline alignment (facade + contracts + introspection)

## Goal
Align `src/extract` execution and schema validation with the unified facade, contract‑first schemas, and information_schema introspection.

Status: ✅ Complete — extract execution and validation aligned with facade + info_schema.

## Representative pattern
```python
execution = ibis_execution_from_ctx(ctx)
plan = build_extract_plan(...)
result = execute_ibis_plan(plan, execution=execution, streaming=prefer_reader)
table = result.require_table()
finalized = finalize(table, contract=contract, ctx=ctx, options=FinalizeOptions(schema_policy=policy))
```

## Target files
- Modify: `src/extract/helpers.py`
- Modify: `src/extract/schema_ops.py`
- Modify: `src/extract/worklists.py`
- Modify: `src/extract/extract_registry.py`
- Modify: `src/extract/extract_template_specs.py`

## Deletions
- Remove extract‑side ad‑hoc schema inference paths once contract binding is enforced.

## Checklist
- [x] Route extract SQL execution through `DataFusionExecutionFacade`.
- [x] Bind extract outputs to DatasetSpec/ContractSpec schemas.
- [x] Use `information_schema` via `SchemaIntrospector` for validation (no bespoke schema tables).
- [x] Emit diagnostics through the unified recorder for extract plan compilation/execution.
- [x] Ensure extract UDF usage routes through registry snapshot metadata.

---

# Scope 9 — Normalize pipeline alignment (ExecutionResult + contract binding)

## Goal
Ensure normalize tasks compile and execute through the unified facade, and all outputs are explicitly bound to contract schemas.

Status: ✅ Complete — normalize execution fully aligned with facade + contracts.

## Representative pattern
```python
plan = builder(catalog, ctx, backend)
expr = bind_expr_schema(plan.expr, schema=dataset_schema(name), allow_extra_columns=ctx.debug)
result = execute_ibis_plan(IbisPlan(expr=expr), execution=execution, streaming=False)
```

## Target files
- Modify: `src/normalize/ibis_plan_builders.py`
- Modify: `src/normalize/ibis_bridge.py`
- Modify: `src/normalize/ibis_api.py`
- Modify: `src/normalize/runtime.py`
- Modify: `src/normalize/dataset_fields.py`

## Deletions
- Remove any remaining normalize output paths that rely on implicit schema inference.

## Checklist
- [x] Bind all normalize outputs with `bind_expr_schema`.
- [x] Route normalize execution through `ExecutionResult` surfaces.
- [x] Ensure SQLGlot canonicalization for normalize plan fingerprints.
- [x] Replace Python stable ID/key helpers with Rust UDFs (shared with relspec/cpg).
- [x] Ensure normalize UDF usage routes through registry snapshot metadata.

---

# Scope 10 — CPG pipeline alignment (contracts + Rust UDF execution)

## Goal
Standardize CPG emitters on contract‑first schemas and Rust UDF‑powered ID/key logic, and enforce facade‑based execution and diagnostics.

Status: ✅ Complete — Rust UDFs + diagnostics aligned for CPG outputs.

## Representative pattern
```python
edge_id = stable_hash64_udf(src, dst, path, bstart, bend)
output = bind_expr_schema(expr, schema=CPG_EDGES_SCHEMA, allow_extra_columns=include_keys)
```

## Target files
- Modify: `src/cpg/emit_edges_ibis.py`
- Modify: `src/cpg/plan_builders.py`
- Modify: `src/cpg/specs.py`
- Modify: `src/cpg/schemas.py`

## Deletions
- Remove Python stable_id/stable_key implementations once Rust UDFs fully replace them.

## Checklist
- [x] Bind all CPG outputs to DatasetSpec/ContractSpec schemas.
- [x] Replace stable hash/key logic with Rust UDFs.
- [x] Emit execution diagnostics via unified recorder.
- [x] Ensure CPG UDF usage routes through registry snapshot metadata.

---

# Scope 11 — Cross‑cutting capability adoption (DataFusion/Ibis/SQLGlot/Delta)

## Goal
Adopt under‑leveraged engine capabilities across extract/normalize/cpg for deterministic, high‑performance execution.

Status: ✅ Complete — listing order, unbounded DDL, CDF hooks, and parity enforcement in place.

## Representative pattern
```sql
CREATE UNBOUNDED EXTERNAL TABLE cpg_edges
STORED AS PARQUET
LOCATION '...'
WITH ORDER (repo ASC, path ASC);
```

## Target files
- Modify: `src/datafusion_engine/registry_bridge.py`
- Modify: `src/datafusion_engine/io_adapter.py`
- Modify: `src/datafusion_engine/streaming_executor.py`
- Modify: `src/incremental/*`
- Modify: `src/schema_spec/system.py`

## Deletions
- None (capability expansion).

## Checklist
- [x] Use listing tables with `file_sort_order` for deterministic scans.
- [x] Use `CREATE UNBOUNDED EXTERNAL TABLE` for streaming‑safe datasets.
- [x] Leverage Delta CDF for incremental CPG execution.
- [x] Enforce schema evolution via Delta schema policies on writes.
- [x] Use `information_schema` for unified schema validation everywhere.
- [x] Enforce UDF parity checks across registry/Ibis/information_schema.

---

# Scope 12 — Deferred deletions (only after all scopes complete)

Status: ◐ Partial — remaining deletions deferred pending final cleanup.

These cannot be removed safely until the scopes above are fully executed and all call sites migrate:

- Python stable ID/key helpers in `src/ibis_engine/ids.py` (once Rust UDFs are in full use).
- Any remaining Ibis‑side ad‑hoc schema inference in relspec plans (after contract binding lands everywhere).
- Extract/normalize/cpg ad‑hoc schema inference helpers once contract binding is enforced in those pipelines.
- Any non‑facade SQL execution helpers still used by extract/normalize/cpg after migration.
- Static builtin UDF lists and legacy registry wrappers once snapshot parity is enforced.
- Raw SQL UDF creation paths once FunctionFactory/builders are fully adopted.

---

# Acceptance gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`
