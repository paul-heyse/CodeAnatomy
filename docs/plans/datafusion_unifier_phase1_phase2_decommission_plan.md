# DataFusion Unifier Phase 1-2 Decommission Plan

## Goals
- Remove legacy Phase 1 and Phase 2 code paths now superseded by the unified architecture.
- Eliminate duplicate helpers, string-based SQL assembly, and parallel execution lanes.
- Ensure all IO, compilation, execution, and write paths converge on the same canonical APIs.

## Non-goals
- Phase 3+ cleanup (introspection snapshot, schema contracts, semantic diff, unified UDF registry).
- Behavior changes in dataset semantics, diagnostics payload shapes, or external APIs without explicit approval.
- Refactors unrelated to Phase 1/2 consolidation.

## Scope Summary
1) Centralize IO registration and table deregistration via `DataFusionIOAdapter`.
2) Remove string-based external table DDL assembly in favor of SQLGlot AST builders.
3) Collapse parameter binding to a single resolver path.
4) Enforce a single compilation/execution lane via `CompilationPipeline`.
5) Remove legacy write helpers and duplicate COPY payload builders in favor of `WritePipeline`.

---

## 1) Centralize IO Registration + Deregistration

**Replacement pattern**
```python
from datafusion_engine.io_adapter import DataFusionIOAdapter

adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
adapter.register_object_store(scheme=scheme, store=store, host=host)
adapter.register_external_table(name=table_name, location=location)
adapter.register_arrow_table(table_name, table, overwrite=True)
adapter.deregister_table(table_name)
```

**Target files to modify**
- `src/datafusion_engine/io_adapter.py` (add public `deregister_table` wrapper)
- `src/datafusion_engine/registry_bridge.py` (replace `_register_object_store` helper usage with adapter)
- `src/datafusion_engine/runtime.py` (replace `_deregister_table` usage)
- `src/engine/materialize_pipeline.py` (replace `_deregister_table` usage)
- `src/ibis_engine/io_bridge.py` (replace `_deregister_table` usage)
- `src/datafusion_engine/bridge.py` (replace direct `ctx.deregister_table` calls with adapter)

**Code/files to delete**
- [x] `src/engine/materialize_pipeline.py`: `_deregister_table`
- [x] `src/ibis_engine/io_bridge.py`: `_deregister_table`
- [x] `src/datafusion_engine/runtime.py`: `_deregister_table`
- [x] `src/datafusion_engine/registry_bridge.py`: `_register_object_store` (inline adapter usage)

**Implementation checklist**
- [x] Add `DataFusionIOAdapter.deregister_table` (public API).
- [x] Replace all temp-table deregistration helpers with adapter.
- [x] Remove `_register_object_store` wrapper and update call sites to use adapter.
- [x] Verify adapter captures diagnostics for object-store registration and table registration.
- [x] Run `uv run ruff check --fix`, `uv run pyrefly check`, `uv run pyright --warnings --pythonversion=3.13`.

---

## 2) Decommission String-Based External Table DDL Assembly

**Replacement pattern**
```python
from datafusion_engine.io_adapter import DataFusionIOAdapter

adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
ddl = adapter.external_table_ddl(name=table_name, location=location)
adapter.register_external_table(name=table_name, location=location)
```

**Target files to modify**
- `src/datafusion_engine/registry_bridge.py` (use adapter DDL + registration path)
- `src/datafusion_engine/io_adapter.py` (ensure DDL options cover previous overrides)
- `src/sqlglot_tools/ddl_builders.py` (extend options schema if any legacy options exist)

**Code/files to delete**
- [x] `src/datafusion_engine/registry_bridge.py`: `datafusion_external_table_sql`
- [x] `src/datafusion_engine/registry_bridge.py`: `_external_table_options`
- [x] `src/datafusion_engine/registry_bridge.py`: `_scan_external_table_options`
- [x] `src/datafusion_engine/registry_bridge.py`: `_option_literal_text`
- [x] `src/datafusion_engine/registry_bridge.py`: `_expected_scan_option_literals`
- [x] `src/datafusion_engine/registry_bridge.py`: `_ddl_external_table_options`
- [x] `src/datafusion_engine/registry_bridge.py`: `_validate_scan_options_in_ddl`

**Implementation checklist**
- [x] Route external table registration to `DataFusionIOAdapter.register_external_table`.
- [x] Preserve any required option overrides via `build_external_table_ddl`.
- [x] Update `TableProviderMetadata` to source DDL from adapter output.
- [x] Remove string assembly helpers and associated DDL validation logic.
- [ ] Add or update tests to compare AST fingerprints (not string equality).

---

## 3) Collapse Parameter Binding to a Single Resolver

**Replacement pattern**
```python
from datafusion_engine.param_binding import (
    resolve_param_bindings,
    apply_bindings_to_context,
)

bindings = resolve_param_bindings(params)
apply_bindings_to_context(ctx, bindings)
df = ctx.sql(sql, **bindings.param_values)
```

**Target files to modify**
- `src/datafusion_engine/bridge.py` (remove ad-hoc param validation; call resolver directly)
- `src/datafusion_engine/compile_pipeline.py` (standardize on resolver)
- `src/ibis_engine/params_bridge.py` (remove duplicate binding logic)
- `src/engine/materialize_pipeline.py` (ensure param diagnostics are sourced from resolver)

**Code/files to delete**
- `src/datafusion_engine/bridge.py`: `_ibis_param_bindings`
- `src/datafusion_engine/bridge.py`: `_validated_param_bindings`
- `src/datafusion_engine/bridge.py`: `_validated_named_params`
- `src/datafusion_engine/bridge.py`: `_emit_named_param_diagnostics`
- `src/ibis_engine/params_bridge.py`: `datafusion_param_bindings` (if no longer used after consolidation)

**Implementation checklist**
- [x] Replace all param-validation call sites with `resolve_param_bindings`.
- [x] Ensure named table bindings register via `register_table_params` context manager.
- [x] Preserve allowlist validation in one place (`validate_param_name`).
- [x] Remove redundant binding helpers.
- [ ] Add/update tests for param binding coverage.

---

## 4) Enforce a Single Compilation/Execution Lane

**Replacement pattern**
```python
from datafusion_engine.compile_pipeline import CompilationPipeline, CompileOptions

pipeline = CompilationPipeline(ctx, CompileOptions())
compiled = pipeline.compile_ibis(expr, backend=backend)
df = pipeline.execute(compiled, params=params, named_params=named_params)
```

**Target files to modify**
- `src/ibis_engine/runner.py` (remove DataFusion-specific execution branches)
- `src/ibis_engine/execution.py` (delegate DataFusion execution to pipeline)
- `src/engine/materialize_pipeline.py` (ensure plan execution uses pipeline paths)
- `src/datafusion_engine/bridge.py` (remove legacy dual-lane fallback if unused)

**Code/files to delete**
- [ ] `src/ibis_engine/runner.py`: `plan_to_datafusion`
- [ ] `src/ibis_engine/runner.py`: `_materialize_plan_datafusion`
- [ ] `src/ibis_engine/runner.py`: `_stream_plan_datafusion`
- [ ] `src/ibis_engine/runner.py`: DataFusion branch inside `async_stream_plan`
- [x] `src/datafusion_engine/bridge.py`: `ibis_to_datafusion_dual_lane` (after all call sites removed)
- [x] `src/datafusion_engine/bridge.py`: `_attempt_substrait_lane` (if dual-lane removed)

**Implementation checklist**
- [x] Replace runner DataFusion paths with `CompilationPipeline.execute` (via `ibis_to_datafusion`).
- [ ] Preserve diagnostics hooks (tracing/metrics) via pipeline artifacts.
- [x] Remove dual-lane fallback usage from callers; delete when unused.
- [ ] Ensure plan caching and ordering metadata are preserved post-migration.

---

## 5) Remove Legacy Write Helpers + COPY Payload Duplication

**Replacement pattern**
```python
from datafusion_engine.write_pipeline import WritePipeline, WriteRequest, WriteFormat, WriteMode

request = WriteRequest(
    source=sql_or_ast,
    destination=output_path,
    format=WriteFormat.PARQUET,
    mode=WriteMode.OVERWRITE,
    partition_by=("col1",),
)
pipeline = WritePipeline(ctx, profile)
pipeline.write(request, prefer_streaming=True)
```

**Target files to modify**
- `src/datafusion_engine/bridge.py` (remove legacy parquet writer; call WritePipeline)
- `src/engine/materialize_pipeline.py` (use `WriteResult`/`WritePipeline` metadata)
- `src/ibis_engine/io_bridge.py` (ensure all COPY/Parquet writes go through WritePipeline)
- `src/datafusion_engine/write_pipeline.py` (single source for write metadata + diagnostics)

**Code/files to delete**
- [x] `src/datafusion_engine/bridge.py`: `datafusion_write_parquet`
- [x] `src/datafusion_engine/bridge.py`: `_pipeline_write_parquet`
- [x] `src/engine/materialize_pipeline.py`: `_copy_options_payload`

**Implementation checklist**
- [x] Replace `datafusion_write_parquet` call sites (if any) with `WritePipeline`.
- [x] Remove `_copy_options_payload` and use `WriteResult` metadata instead.
- [ ] Ensure streaming and COPY write paths emit identical diagnostics payloads.
- [ ] Add tests for partitioned writes and per-column compression options.

---

## Execution Order (Recommended)
1) IO adapter + deregistration cleanup.
2) External table DDL consolidation.
3) Parameter binding consolidation.
4) Compilation lane unification.
5) Write pipeline + COPY payload cleanup.

## Acceptance Gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`
- `uv run pytest tests/unit/`
