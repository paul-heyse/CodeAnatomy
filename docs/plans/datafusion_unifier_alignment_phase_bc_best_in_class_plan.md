# DataFusion Unifier Alignment (Phase B/C) — Best‑in‑Class Implementation Plan

## Purpose
Implement the Phase B/C refinements described in the recent architecture review: a single execution facade, full IOAdapter consolidation, diagnostics unification, SQL safety gates everywhere, and expanded capability adoption (listing-table optimizations, Delta insert paths, CDF support, Ibis create_table/create_view integration, and SQLGlot strictness). This plan targets a **best‑in‑class, AST‑first, diagnostics‑rich** DataFusion/Ibis/SQLGlot architecture.

## Guiding Principles
- **Single orchestration surface**: execution, registration, writes, and diagnostics flow through one facade.
- **AST‑first policy**: all SQL derived from SQLGlot AST with strict parsing and deterministic policy.
- **Diagnostics consistency**: all artifacts/events emitted by `DiagnosticsRecorder`.
- **IOAdapter everywhere**: no raw `ctx.register_*` from leaf modules.
- **Capability‑aware**: use DataFusion + Delta features when the provider supports them; capture capability metadata.

---

# Scope 1 — Execution Facade integrated with EngineSession

## Goal
Create a unified `DataFusionExecutionFacade` that orchestrates compilation, execution, registration, and write flows, then expose it via `EngineSession` so all surfaces use the same entrypoint.

## Status
**Complete** — facade is wired into `EngineSession` and core execution paths now route through it; legacy helpers are internal only.

## Representative pattern
```python
facade = DataFusionExecutionFacade(
    ctx=session.datafusion_profile.session_context(),
    runtime_profile=session.datafusion_profile,
    diagnostics=session.diagnostics,
    ibis_backend=session.ibis_backend,
)
compiled = facade.compile(expr)
df = facade.execute(compiled, params=params)
```

## Target files
- New: `src/datafusion_engine/execution_facade.py`
- Modify: `src/engine/session.py`
- Modify: `src/engine/session_factory.py`
- Modify: `src/datafusion_engine/bridge.py`
- Modify: `src/ibis_engine/runner.py`
- Modify: `src/ibis_engine/execution.py`

## Checklist
- [x] Define `DataFusionExecutionFacade` (compile, execute, register, write, diagnostics context).
- [x] Wire facade into `EngineSession` (property and initialization).
- [x] Replace direct compilation/execution call chains with facade calls (bridge, schema_registry, extract/worklists now delegate).
- [x] Ensure facade constructs a consistent `DiagnosticsContext` per operation everywhere (compile/write emit recorder-backed artifacts).

## Deletions (after migration)
- Remove remaining legacy bridge helpers once facade is the only entrypoint (see deferred deletions).

---

# Scope 2 — IOAdapter becomes the only registration surface

## Goal
Route **all** table/view/provider/dataset registrations (including temporary tables) through `DataFusionIOAdapter`.

## Status
**Complete** — registrations routed through IOAdapter; listing/record batches/arrow/parquet/csv/json/avro covered.

## Representative pattern
```python
adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
adapter.register_table_provider(name, provider, overwrite=True)
adapter.register_view(name, df, temporary=True)
```

## Target files
- Modify: `src/datafusion_engine/io_adapter.py`
- Modify: `src/datafusion_engine/param_binding.py`
- Modify: `src/datafusion_engine/runtime.py`
- Modify: `src/engine/materialize_pipeline.py`
- Modify: `src/ibis_engine/io_bridge.py`

## Checklist
- [x] Replace all remaining `ctx.register_table`/`ctx.register_*` usages with `DataFusionIOAdapter`.
- [x] Add IOAdapter wrappers for `register_parquet/json/csv` where used in Ibis bridges.
- [x] Ensure registration invalidates introspection cache and records diagnostics.

## Deletions
- Delete any ad‑hoc registration helpers once all call sites are migrated.

---

# Scope 3 — DiagnosticsRecorder‑first telemetry

## Goal
All telemetry flows through `DiagnosticsRecorder` for consistent payload schemas.

## Status
**Complete** — direct sink calls replaced with recorder helpers across DataFusion/Ibis surfaces.

## Representative pattern
```python
recorder = recorder_for_profile(runtime_profile, operation_id="compile")
recorder.record_compilation(...)
recorder.record_execution(...)
```

## Target files
- Modify: `src/datafusion_engine/diagnostics.py`
- Modify: `src/datafusion_engine/runtime.py`
- Modify: `src/datafusion_engine/registry_bridge.py`
- Modify: `src/datafusion_engine/write_pipeline.py`
- Modify: `src/ibis_engine/runner.py`

## Checklist
- [x] Replace direct `diagnostics_sink.record_artifact` calls with `DiagnosticsRecorder`.
- [x] Use a single context factory per operation (owned by the facade) where facade is used.
- [x] Normalize event/artifact payloads with schema registry tables.

## Deletions
- Remove direct sink write paths after recorder coverage is complete.

---

# Scope 4 — SQL safety gate for all SQL execution

## Goal
Enforce `SafeExecutor` policy on every SQL execution and session `SET`.

## Status
**Partial** — core registry/runtime/view/write paths now execute via the facade; specialized modules still call `execute_with_profile` directly.

## Representative pattern
```python
executor = safe_executor_for_profile(ctx, profile=runtime_profile)
df = executor.execute(sql)
```

## Target files
- Modify: `src/datafusion_engine/sql_safety.py`
- Modify: `src/datafusion_engine/bridge.py`
- Modify: `src/datafusion_engine/registry_bridge.py`
- Modify: `src/datafusion_engine/write_pipeline.py`

## Checklist
- [x] Replace `ctx.sql`/`ctx.sql_with_options` with `SafeExecutor` for core SQL execution.
- [x] Ensure `SET` statements use `allow_statements=True` in policy.
- [ ] Centralize SQL execution behind the facade (remaining direct usage in schema_introspection/introspection/finalize/arrowdsl).

## Deletions
- Remove direct SQL execution helpers once all usage routes through the facade.

---

# Scope 5 — Listing‑table & file‑order optimization

## Goal
Preserve partitioning and ordering semantics end‑to‑end and enable file‑order aware planning.

## Status
**Complete** — directional ordering propagated to scan options + listing registration + DDL.

## Representative pattern
```python
adapter.register_listing_table(
    ListingTableRegistration(
        name=name,
        location=location,
        table_partition_cols=partition_cols,
        file_sort_order=[("repo", "asc"), ("path", "asc")],
    )
)
```

## Target files
- Modify: `src/datafusion_engine/io_adapter.py`
- Modify: `src/datafusion_engine/registry_bridge.py`
- Modify: `src/arrowdsl/core/ordering.py`
- Modify: `src/schema_spec/system.py`

## Checklist
- [x] Extend `file_sort_order` to include direction (ASC/DESC).
- [x] Propagate ordering metadata → scan options → listing table registration.
- [x] Ensure `parquet_pruning` + `skip_metadata` defaults are captured in the scan policy.

## Deletions
- None (structural upgrade only).

---

# Scope 6 — Delta INSERT path in WritePipeline

## Goal
Use DataFusion’s Delta provider inserts when supported; fall back only when required.

## Status
**Complete** — insert capability metadata + WritePipeline insert path wired; legacy bridge helpers removed.

## Representative pattern
```python
if provider_caps.supports_insert:
    pipeline.write_via_insert(request)
else:
    pipeline.write_via_streaming(request)
```

## Target files
- Modify: `src/datafusion_engine/write_pipeline.py`
- Modify: `src/datafusion_engine/table_provider_metadata.py`
- Modify: `src/datafusion_engine/registry_bridge.py`

## Checklist
- [x] Add capability flags to `TableProviderMetadata` (`supports_insert`).
- [x] Detect Delta provider capability at registration time.
- [x] Add INSERT path in `WritePipeline` for Delta tables.

## Deletions
- [x] Removed direct Delta insert paths outside the write pipeline.

---

# Scope 7 — Delta CDF dataset kind as first‑class

## Goal
Expose CDF datasets through the unified registry pipeline with consistent diagnostics.

## Status
**Complete** — dataset_kind + provider resolution added; CDF registration now routes through the facade and diagnostics are recorded.

## Representative pattern
```python
facade.register_dataset(name=name, location=location)
```

## Target files
- Modify: `src/schema_spec/system.py`
- Modify: `src/ibis_engine/registry.py`
- Modify: `src/datafusion_engine/registry_bridge.py`

## Checklist
- [x] Add explicit CDF dataset kind in schema specs.
- [x] Route registration and diagnostics through the facade.
- [x] Ensure CDF options recorded in registry diagnostics.

## Deletions
- None (capability extension).

---

# Scope 8 — Ibis create_table/create_view integration

## Goal
Move away from named memtable patterns and use backend create_table/create_view.

## Status
**Partial** — memtable usage removed; remaining gap is explicit schema binding for contracted outputs.

## Representative pattern
```python
backend.create_view(name, expr, overwrite=True)
```

## Target files
- Modify: `src/ibis_engine/param_tables.py`
- Modify: `src/ibis_engine/query_compiler.py`
- Modify: `src/ibis_engine/io_bridge.py`

## Checklist
- [x] Replace named memtable usage with backend create_table/create_view (param tables + file_id macros).
- [x] Ensure view/table creation flows through IOAdapter for DataFusion backends.
- [ ] Update any schema inference to explicit schemas for contracted outputs.

## Deletions
- Remove memtable naming utilities once explicit-schema work is complete.

---

# Scope 9 — SQLGlot strict parsing + policy fields

## Goal
Enforce strict parsing and canonical generation with explicit error/unsupported levels.

## Status
**Complete** — strict parsing enabled across compile/write/registry/safety paths and tokenizer diagnostics recorded.

## Representative pattern
```python
expr = parse_sql_strict(sql, dialect=policy.read_dialect, error_level=policy.error_level)
```

## Target files
- Modify: `src/datafusion_engine/compile_pipeline.py`
- Modify: `src/datafusion_engine/sql_policy_engine.py`
- Modify: `src/sqlglot_tools/optimizer.py`

## Checklist
- [x] Add error/unsupported levels to `SQLPolicyProfile`.
- [x] Use `parse_sql_strict` for all SQL input parsing.
- [x] Emit diagnostics about tokenizer (Rust vs Python).

## Deletions
- Remove raw `parse_one` usage in compile path once strict parsing is enabled.

---

# Scope 10 — Additional unification: execution contract + capability flags

## Goal
Standardize a single execution result envelope and capability metadata surface.

## Status
**Partial** — `ExecutionResult` exists and capability flags added; some legacy routing still returns raw DataFrames/readers.

## Representative pattern
```python
result = facade.execute(compiled)
# result.kind in {"table", "reader", "write"}
```

## Target files
- Modify: `src/datafusion_engine/bridge.py`
- Modify: `src/ibis_engine/execution.py`
- Modify: `src/engine/materialize_pipeline.py`
- Modify: `src/datafusion_engine/table_provider_metadata.py`

## Checklist
- [x] Define a unified execution result model (table/reader/write summary).
- [x] Add capability flags to `TableProviderMetadata` and surface them to the facade.
- [ ] Remove duplicate execution routing paths after facade integration.

## Deletions
- Remove redundant execution routing helpers once contract is adopted.

---

# Deferred Deletions (only after all scope items complete)

The following code should only be deleted once all new surfaces are in place and in use:

- Remaining legacy execution helpers in `src/datafusion_engine/bridge.py` (e.g., copy/reader helpers) once all execution flows return `ExecutionResult`.
- Direct `execute_with_profile` usage in specialized modules (schema/introspection/finalize/arrowdsl) once they are routed through the facade.
- Memtable naming utilities once explicit-schema enforcement for contracted outputs is complete.

---

# Acceptance Gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`
