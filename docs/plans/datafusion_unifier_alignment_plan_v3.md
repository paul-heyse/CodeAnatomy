# DataFusion Unifier Alignment Plan v3

## Goals
- Remove legacy/parallel execution and SQL assembly paths under `src/datafusion_engine`.
- Enforce AST-first compilation + policy + diagnostics across all DataFusion surfaces.
- Centralize IO registration, execution, and writes into a minimal set of canonical APIs.
- Leverage DataFusion/Delta/Ibis/SQLGlot capabilities surfaced in reference docs.

## Non-goals
- Phase 3+ contract/semantic-diff work not explicitly listed below.
- Any behavioral changes not tied to the unifier architecture.

---

# A) Legacy Code to Delete or Modify (execute first)

## A1) Remove SQLGlot-to-DataFusion custom translator (`df_builder.py`)

**Why**
`df_builder` bypasses `CompilationPipeline` and SQL policy canonicalization; it duplicates execution logic and complicates diagnostics consistency.

**Representative pattern (target)**
```python
from datafusion_engine.compile_pipeline import CompilationPipeline, CompileOptions

pipeline = CompilationPipeline(ctx, CompileOptions(profile=policy_profile))
compiled = pipeline.compile_ast(sqlglot_expr)
df = pipeline.execute(compiled, params=params, named_params=named_params)
```

**Target files**
- `src/datafusion_engine/df_builder.py` (deprecate/remove)
- Callers: `src/datafusion_engine/query_fragments.py`, `src/datafusion_engine/schema_registry.py`, `src/extract/worklists.py`

**Implementation checklist**
- [x] Replace all `df_from_sqlglot` call sites with `CompilationPipeline.compile_ast/execute`.
- [x] Preserve CTE support by compiling full AST rather than rewriting tables in Python.
- [x] Delete `df_builder.py` and remove exports from `src/datafusion_engine/__init__.py`.

---

## A2) Replace string SQL fragments with AST builders

**Why**
String fragments in `query_fragments_sql.py` break AST-first determinism and bypass canonicalization.

**Representative pattern (target)**
```python
from sqlglot import exp
from datafusion_engine.sql_policy_engine import SQLPolicyProfile

expr = (
    exp.select(
        exp.column("file_id"),
        exp.column("path"),
    )
    .from_(exp.table_("ast_files_v1"))
)
sql = expr.sql(dialect=profile.write_dialect)
```

**Target files**
- `src/datafusion_engine/query_fragments_sql.py` (replace with AST builder module)
- `src/datafusion_engine/query_fragments.py` (route fragment execution through `CompilationPipeline`)

**Implementation checklist**
- [x] Build AST equivalents for all `FRAGMENT_SQL` entries.
- [x] Add a factory that returns SQLGlot ASTs (not strings).
- [x] Render via policy profile for deterministic SQL output.
- [x] Remove `query_fragments_sql.py` after migration.

---

## A3) Unify schema registry execution with AST-first policy

**Why**
`schema_registry.py` executes large volumes of string SQL with ad-hoc helpers. This should flow through canonical compilation and a single executor for diagnostics consistency.

**Representative pattern (target)**
```python
from datafusion_engine.compile_pipeline import CompilationPipeline, CompileOptions
from datafusion_engine.sql_policy_engine import SQLPolicyProfile

pipeline = CompilationPipeline(ctx, CompileOptions(profile=SQLPolicyProfile()))
compiled = pipeline.compile_sql(sql_text)
df = pipeline.execute(compiled, sql_options=sql_options)
```

**Target files**
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/schema_introspection.py` (when schema registry uses snapshot cache)

**Implementation checklist**
- [x] Replace `_sql_with_options` and raw `ctx.sql_with_options` calls with `CompilationPipeline`.
- [x] Ensure policy enforcement is consistent for all registry SQL.
- [x] Route diagnostics through `DiagnosticsRecorder` (see section B3).

---

## A4) Centralize table registration via IOAdapter (including providers/views)

**Why**
`registry_bridge.py` and other modules still call `ctx.register_table` directly, bypassing `DataFusionIOAdapter` diagnostics and cache invalidation.

**Representative pattern (target)**
```python
from datafusion_engine.io_adapter import DataFusionIOAdapter

adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
adapter.register_table_provider(name, provider)
adapter.register_view(name, view_sql)
```

**Target files**
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/registry_loader.py`
- `src/datafusion_engine/df_builder.py` (if still used)
- `src/datafusion_engine/schema_registry.py`

**Implementation checklist**
- [x] Add `register_table_provider`, `register_view`, `register_listing_table` to `DataFusionIOAdapter`.
- [x] Replace direct `ctx.register_table` with adapter methods.
- [x] Ensure adapter invalidates introspection cache and records diagnostics.

---

## A5) Consolidate UDF registry logic under unified registry

**Why**
`datafusion_engine/udf_registry.py` and `udf_catalog.py` overlap with `engine/udf_registry.py` lane-aware registry.

**Retention note**
This scope does **not** remove Rust-backed UDFs under `rust/` (e.g., `datafusion_ext`). Those implementations remain active and should continue to be registered/exposed because they unlock native DataFusion execution paths, improve performance, and simplify catalog management. A5 only consolidates *Python-side registry and catalog logic*.

**Representative pattern (target)**
```python
from engine.udf_registry import FunctionRegistry
from datafusion_engine.introspection import IntrospectionSnapshot

registry = FunctionRegistry()
registry.merge_from_introspection(snapshot)
registry.apply_to_backend(backend)
```

**Target files**
- `src/datafusion_engine/udf_registry.py` (deprecate)
- `src/datafusion_engine/udf_catalog.py` (reduce to thin adapter or deprecate)
- `src/engine/udf_registry.py` (canonical)

**Implementation checklist**
- [ ] Route all UDF registration through `engine/udf_registry.FunctionRegistry`.
- [ ] Remove duplicated UDF specs from `datafusion_engine/udf_registry.py`.
- [ ] Ensure function catalog snapshots use `IntrospectionSnapshot`.

---

## A6) Retire duplicate introspection helpers

**Why**
`cache_introspection.py` and parts of `schema_introspection.py` duplicate snapshot logic.

**Representative pattern (target)**
```python
from datafusion_engine.introspection import introspection_cache_for_ctx

snapshot = introspection_cache_for_ctx(ctx).snapshot
schema_map = snapshot.schema_map()
```

**Target files**
- `src/datafusion_engine/cache_introspection.py`
- `src/datafusion_engine/schema_introspection.py`

**Implementation checklist**
- [ ] Replace any direct `information_schema` SQL reads with `IntrospectionCache`.
- [x] Deprecate `cache_introspection.py` once all call sites are moved.

---

## A7) Minimize string SQL assembly in `bridge.py`

**Why**
Remaining `_emit_sql` / `_emit_internal_sql` usage bypasses deterministic AST policy in some paths.

**Representative pattern (target)**
```python
from sqlglot import exp
from datafusion_engine.sql_policy_engine import render_for_execution

sql = render_for_execution(ast, profile)
```

**Target files**
- `src/datafusion_engine/bridge.py`

**Implementation checklist**
- [x] Replace any string-built DDL/DML with SQLGlot AST builders.
- [ ] Ensure all execution uses compiled AST + policy profile.

---

# B) Opportunities to Unify Beyond the Plan

## B1) Execution Facade (single orchestration surface)

**Goal**
Provide a single entrypoint that composes `CompilationPipeline`, `DataFusionIOAdapter`, `WritePipeline`, and `DiagnosticsRecorder`.

**Representative pattern**
```python
facade = DataFusionExecutionFacade(ctx, runtime_profile)
compiled = facade.compile(expr)
df = facade.execute(compiled, params=params)
```

**Target files**
- New: `src/datafusion_engine/execution_facade.py`
- Integrate into `bridge.py`, `schema_registry.py`, `query_fragments.py`, `registry_bridge.py`

**Implementation checklist**
- [ ] Define facade API: compile, execute, register, write.
- [ ] Ensure facade builds consistent diagnostics context.
- [ ] Replace multi-surface execution paths with facade.

---

## B2) Expand IOAdapter to cover all registration types

**Goal**
Make IOAdapter the only registration surface (external tables, providers, views, listing tables).

**Representative pattern**
```python
adapter.register_listing_table(
    name=name,
    location=location,
    scan_options=scan,
)
```

**Target files**
- `src/datafusion_engine/io_adapter.py`
- `src/datafusion_engine/registry_bridge.py`

**Implementation checklist**
- [ ] Implement listing-table registration in IOAdapter with scan options.
- [ ] Route listing table registration through adapter.
- [ ] Ensure diagnostics artifact parity across provider types.

---

## B3) DiagnosticsRecorder-first telemetry

**Goal**
Every artifact, metric, and event emission uses `DiagnosticsRecorder` for consistent payloads.

**Representative pattern**
```python
recorder = diagnostics_recorder_for_profile(runtime_profile)
recorder.record_compilation(...)
recorder.record_execution(...)
```

**Target files**
- `src/datafusion_engine/diagnostics.py`
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/write_pipeline.py`

**Implementation checklist**
- [ ] Replace direct `diagnostics_sink.record_*` calls.
- [ ] Ensure write/registration artifacts share the same schema.

---

## B4) SQL safety gate for all execution paths

**Goal**
Use `sql_safety.SafeExecutor` or `SQLOptions` defaults for every `ctx.sql` call.

**Representative pattern**
```python
executor = SafeExecutor(ctx, default_policy)
executor.execute(sql, policy=policy)
```

**Target files**
- `src/datafusion_engine/sql_safety.py`
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/schema_registry.py`

**Implementation checklist**
- [ ] Route SQL execution through safety gate.
- [ ] Ensure all DDL/DML uses policy-appropriate SQLOptions.

---

# C) Beneficial Capabilities from Reference Docs

## C1) Listing-table & file-order optimizations

**Capability**
Use `register_listing_table` with explicit `table_partition_cols` and `file_sort_order` to unlock optimizations.

**Representative pattern**
```python
adapter.register_listing_table(
    name=name,
    location=location,
    table_partition_cols=partition_cols,
    file_sort_order=scan.file_sort_order,
)
```

**Target files**
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/io_adapter.py`

**Implementation checklist**
- [ ] Ensure partition/order info flows from `DatasetLocation` → registration.
- [ ] Apply `parquet_pruning` + `pushdown_filters` settings as profile defaults.

---

## C2) Delta TableProvider inserts

**Capability**
Route append/overwrite writes to Delta provider via INSERT when supported.

**Representative pattern**
```python
if provider_supports_insert:
    pipeline.write_via_insert(request)
```

**Target files**
- `src/datafusion_engine/write_pipeline.py`
- `src/datafusion_engine/registry_bridge.py`

**Implementation checklist**
- [ ] Detect provider insert capability for Delta providers.
- [ ] Add INSERT path to WritePipeline when available.

---

## C3) Delta CDF provider surface

**Capability**
Expose Delta CDF as a dataset kind with standardized registration and schema tracking.

**Representative pattern**
```python
register_dataset_df(ctx, name, location, provider="delta_cdf")
```

**Target files**
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/registry_loader.py`

**Implementation checklist**
- [ ] Standardize CDF registration options in location specs.
- [ ] Ensure diagnostics capture provider type + CDF options.

---

## C4) Ibis create_table/create_view integration

**Capability**
Use Ibis backend `create_table`/`create_view` for named objects instead of memtable patterns.

**Representative pattern**
```python
backend.create_view(name, expr)
```

**Target files**
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/bridge.py`

**Implementation checklist**
- [ ] Replace any named memtable registration with backend view creation.

---

## C5) SQLGlot strictness + tokenizer performance

**Capability**
Enforce strict parsing and enable Rust tokenizer when available.

**Representative pattern**
```python
parse_one(sql, dialect=profile.read_dialect, error_level=ErrorLevel.RAISE)
```

**Target files**
- `src/datafusion_engine/compile_pipeline.py`
- `src/datafusion_engine/sql_policy_engine.py`

**Implementation checklist**
- [ ] Standardize `ErrorLevel.RAISE` for compilation paths.
- [ ] Ensure `sqlglot[rs]` is enabled in environment.

---

# Decommission List (after execution)

**Files to delete**
- `src/datafusion_engine/df_builder.py`
- `src/datafusion_engine/query_fragments_sql.py`
- `src/datafusion_engine/cache_introspection.py`
- `src/datafusion_engine/udf_registry.py` (if fully superseded)
- `src/datafusion_engine/udf_catalog.py` (if fully superseded)

**Functions to delete**
- `datafusion_engine.df_builder.df_from_sqlglot`
- All SQL fragment string constructors in `query_fragments_sql.py`
- Direct `ctx.register_table` helpers in `registry_bridge.py` (replaced by IOAdapter)
- Schema registry `_sql_with_options` helpers once routed via pipeline

---

# Acceptance Gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`
- `uv run pytest tests/unit/`
