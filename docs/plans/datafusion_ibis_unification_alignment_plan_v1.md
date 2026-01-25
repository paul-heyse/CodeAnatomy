# DataFusion + Ibis Unification Alignment Plan v1

## Goals

- Eliminate all string‑SQL construction inside `src/ibis_engine` and `src/datafusion_engine` execution paths.
- Make DataFusion the single execution substrate using DataFusion `Expr`/`DataFrame` APIs and Ibis IR (no SQL strings).
- Remove Python UDFs entirely and use Rust extension functions exclusively.
- Consolidate IO registration + diagnostics into `DataFusionIOAdapter` and `DiagnosticsRecorder`.
- Adopt advanced capabilities from the Ibis/DataFusion references (Substrait, listing tables, schema functions, lineage) without SQL execution.

## Target end‑state (best‑in‑class)

- **Expr‑first everywhere:** DataFusion `Expr`/`DataFrame` APIs and Ibis IR are the canonical execution IRs.
- **No SQL execution:** SQL strings are banned from runtime execution and only allowed in explicit debug tooling.
- **Rust‑only UDFs:** All function surfaces (hashing, map/union/metadata, window helpers) are in `rust/datafusion_ext`.
- **Unified IO + diagnostics:** All registrations/writes flow through `DataFusionIOAdapter`, all telemetry uses `DiagnosticsRecorder`.
- **Streaming‑first:** Prefer Arrow C Stream / `to_pyarrow_batches` for large outputs and dataset writes.

---

## Progress snapshot (as of January 25, 2026)

- Completed: Scope 2, Scope 4, Scope 5, Scope 6, Scope 7, Scope 9.
- Partially complete: Scope 1, Scope 3, Scope 8, Scope 10, Scope 11.
- Not started: Scope 12, Scope 13.

---

## Scope 1 — Rust extension function surface (core operators + id functions)

**Intent**
Provide the full function surface via `rust/datafusion_ext` so all execution uses native DataFusion `Expr` without SQL or Python UDFs.

**Status**
Partial. Core scalar/expr functions are exported and wrapped, but aggregate/window Rust UDFs and runtime registration wiring remain.

**Representative pattern**
```rust
// rust/datafusion_ext/src/lib.rs (conceptual)
#[pyfunction]
fn map_entries(expr: PyExpr) -> PyExpr { /* wraps DF scalar function */ }

#[pyfunction]
fn map_keys(expr: PyExpr) -> PyExpr { /* wraps DF scalar function */ }

#[pyfunction]
fn map_values(expr: PyExpr) -> PyExpr { /* wraps DF scalar function */ }

#[pyfunction]
fn map_extract(map_expr: PyExpr, key_expr: PyExpr) -> PyExpr { /* wraps DF scalar */ }

#[pyfunction]
fn arrow_metadata(expr: PyExpr, key: &str) -> PyExpr { /* wraps DF scalar */ }

#[pyfunction]
fn union_tag(expr: PyExpr) -> PyExpr { /* wraps DF scalar */ }

#[pyfunction]
fn union_extract(expr: PyExpr, tag: &str) -> PyExpr { /* wraps DF scalar */ }

#[pyfunction]
fn stable_id(prefix: &str, value: PyExpr) -> PyExpr { /* native hash */ }

#[pyfunction]
fn prefixed_hash64(prefix: &str, value: PyExpr) -> PyExpr { /* native hash */ }
```

**Target files**
- `rust/datafusion_ext/src/lib.rs`
- `rust/datafusion_ext/src/...`

**Delete / decommission**
- None (new Rust APIs)

**Checklist**
- [x] Export Rust functions for: `map_entries`, `map_keys`, `map_values`, `map_extract`.
- [x] Export Rust functions for: `arrow_metadata`, `union_tag`, `union_extract`.
- [x] Port Python UDFs to Rust: `stable_hash64`, `stable_hash128`, `prefixed_hash64`, `stable_id`, `col_to_byte`, `list_extract`.
- [ ] Port remaining Python UDFs to Rust: `list_unique`, `first_value_agg`, `last_value_agg`, `count_distinct_agg`, `string_agg`, `row_index`, `running_count`, `running_total`, `row_number_window`, `lag_window`, `lead_window`, `range_table`.
- [ ] Wire runtime registration hook to `datafusion_ext` (replace Python UDF registry usage).
- [ ] Validate that remaining Rust functions are exposed to Python and covered by required-function checks.

---

## Scope 2 — Python Expr wrapper module (no SQL strings)

**Intent**
Provide a stable Python wrapper layer for Rust‑exposed functions that returns `datafusion.Expr`.

**Status**
Complete. `src/datafusion_engine/expr_functions.py` exists with wrappers and `__all__`.

**Representative pattern**
```python
# src/datafusion_engine/expr_functions.py
from datafusion import Expr
from datafusion_ext import (
    arrow_metadata as _arrow_metadata,
    map_entries as _map_entries,
    map_extract as _map_extract,
    map_keys as _map_keys,
    map_values as _map_values,
    prefixed_hash64 as _prefixed_hash64,
    stable_id as _stable_id,
    union_extract as _union_extract,
    union_tag as _union_tag,
)


def map_entries(expr: Expr) -> Expr:
    return _map_entries(expr)
```

**Target files**
- New: `src/datafusion_engine/expr_functions.py`

**Delete / decommission**
- None

**Checklist**
- [x] Add wrapper module for Rust functions.
- [x] Ensure zero SQL string usage in this module.
- [x] Add `__all__` exports.

---

## Scope 3 — Required function enforcement (Rust surface validation)

**Intent**
Fail fast if required Rust functions are missing or not registered.

**Status**
Partial. Core function validation exists, but union/map variants and Rust-only diagnostics wiring remain.

**Representative pattern**
```python
# src/datafusion_engine/schema_registry.py
ENGINE_REQUIRED_FUNCTIONS += (
    "map_entries",
    "map_keys",
    "map_values",
    "map_extract",
    "arrow_metadata",
    "union_tag",
    "union_extract",
)
```

**Target files**
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`

**Delete / decommission**
- None

**Checklist**
- [x] Add required function lists for core map/hash/metadata functions.
- [ ] Extend required-function lists to include `map_keys`, `map_values`, `union_tag`, `union_extract` if used by views.
- [ ] Ensure validation paths check Rust bindings (not Python UDF registry) and surface diagnostics clearly.

---

## Scope 4 — Replace string SQL in `query_compiler` with Ibis/Expr IR

**Intent**
Remove `Table.sql(...)`/string SQL paths from `query_compiler` and use only Ibis expressions or native `Expr` builders.

**Status**
Complete. `query_compiler` now compiles `ExprIR` into Ibis and rejects SQL-only specs.

**Representative pattern**
```python
# src/ibis_engine/query_compiler.py (new pattern)
from sqlglot_tools.expr_spec import ExprIR
from ibis_engine.expr_compiler import exprir_to_ibis

expr_ir = ExprIR(op="call", name="coalesce", args=(
    ExprIR(op="field", name="col_a"),
    ExprIR(op="field", name="col_b"),
))
ibis_expr = exprir_to_ibis(expr_ir, table=table)
result = table.mutate(new_col=ibis_expr).filter(table["id"].isin(ids))
```

**Target files**
- `src/ibis_engine/query_compiler.py`
- `src/ibis_engine/expr_compiler.py`

**Delete / decommission**
- `query_compiler.py`: `_apply_sql_derived`, `_apply_sql_filter`, `_in_set_expr`, `_sql_identifier`, `_sql_literal`

**Checklist**
- [x] Add `exprir_to_ibis(expr_ir, table)` helper to bind `ExprIR` to Ibis expressions.
- [x] Replace `Table.sql` derivations with `exprir_to_ibis` + Ibis mutate/filter.
- [x] Ensure no SQLGlot‑to‑SQL execution remains in `query_compiler`.

---

## Scope 5 — Refactor hashing to Rust‑backed expressions (no SQL strings)

**Intent**
Replace SQL string hash construction with Rust‑backed expressions (via wrappers).

**Status**
Complete. `ibis_engine/hashing.py` now emits `ExprIR` trees and rejects SQL-only specs.

**Representative pattern**
```python
from datafusion_engine.expr_functions import prefixed_hash64, stable_id

hashed = stable_id(prefix, value_expr)
prefixed = prefixed_hash64(prefix, value_expr)
```

**Target files**
- `src/ibis_engine/hashing.py`
- `src/datafusion_engine/expr_functions.py`

**Delete / decommission**
- `hashing.py`: `_call_sql`, `_join_parts_sql`, `_hash_expr_sql`, `_prefixed_hash_sql`, `_sql_literal`

**Checklist**
- [x] Replace `SqlExprSpec`‑based hash helpers with Rust‑backed expressions.
- [x] Keep `HashExprSpec` as declarative inputs, but emit expressions, not SQL.
- [x] Ensure all hash usage sites in `ibis_engine` use expression builders only.

---

## Scope 6 — SQL ingestion: Expr‑first, SQL only for explicit debug tooling

**Intent**
Remove SQL as an execution lane. SQL ingestion is allowed only for explicit debug tooling and never for runtime execution.

**Status**
Complete. SQL ingestion is now gated behind `debug_only=True` and fails closed by default.

**Representative pattern**
```python
# src/ibis_engine/sql_bridge.py (debug-only ingestion path)
spec = SqlIngestSpec(
    sql="SELECT ...",
    ingest_kind="debug_only",
    schema=expected_schema,
)
expr = sql_to_ibis_expr(spec)
```

**Target files**
- `src/ibis_engine/sql_bridge.py`
- `src/ibis_engine/compiler_checkpoint.py`

**Delete / decommission**
- Remove default usage of `backend_sql_table` / `table_sql_table` in normal paths

**Checklist**
- [x] Restrict `Table.sql` / `Backend.sql` to explicit debug-only paths.
- [x] Require schema for any SQL ingestion to avoid implicit execution.
- [ ] Emit diagnostics if debug-only SQL ingestion is used.

---

## Scope 7 — Route record‑batch registration through `DataFusionIOAdapter`

**Intent**
Eliminate direct `ctx.register_record_batches` / `ctx.deregister_table` usage in Ibis source registration.

**Status**
Complete. `DataFusionIOAdapter.register_record_batches` exists and sources route through it.

**Representative pattern**
```python
adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
adapter.register_record_batches(table_name, [list(batches)], overwrite=options.overwrite)
```

**Target files**
- `src/datafusion_engine/io_adapter.py`
- `src/ibis_engine/sources.py`

**Delete / decommission**
- Direct `ctx.register_record_batches` / `ctx.deregister_table` usage in `sources.py`

**Checklist**
- [x] Add `register_record_batches` to IO adapter.
- [x] Update `register_ibis_record_batches` to use adapter and unified diagnostics.

---

## Scope 8 — Diagnostics: unify Ibis namespace + object store + run telemetry

**Intent**
Ensure all Ibis‑side diagnostics go through `DiagnosticsRecorder` with stable payloads.

**Status**
Partial. Backend/sources/runner/catalog are on `DiagnosticsRecorder`, but `substrait_bridge` and `tracked_run` still use raw sinks.

**Representative pattern**
```python
recorder = recorder_for_profile(profile, operation_id="ibis_namespace")
recorder.record_registration(name=table_name, registration_type="table", location=path)
```

**Target files**
- `src/ibis_engine/backend.py`
- `src/ibis_engine/sources.py`
- `src/ibis_engine/runner.py`
- `src/ibis_engine/catalog.py`

**Delete / decommission**
- `namespace_recorder_from_ctx` / `record_namespace_action` once recorder is used everywhere

**Checklist**
- [x] Replace direct `diagnostics_sink.record_artifact` calls in backend/sources/runner/catalog with recorder methods.
- [ ] Migrate `substrait_bridge` and `tracked_run` diagnostics to recorder wrappers or document exceptions.
- [ ] Add recorder methods for traces/metrics or route through existing recorders.

---

## Scope 9 — Registry alignment: shrink `ibis_engine.registry`

**Intent**
Avoid duplicating registry/catalog state in `ibis_engine`; use `datafusion_engine.registry_bridge` + `DataFusionIOAdapter` as single source of truth.

**Status**
Complete. Registry caches removed; registration delegates to `registry_bridge`.

**Representative pattern**
```python
from datafusion_engine.registry_bridge import register_dataset_df

register_dataset_df(ctx, name=name, location=location, runtime_profile=profile)
```

**Target files**
- `src/ibis_engine/registry.py`
- `src/datafusion_engine/registry_bridge.py`

**Delete / decommission**
- `_REGISTERED_CATALOGS`, `_REGISTERED_SCHEMAS`, `_REGISTERED_REGISTRY_CATALOGS`
- Registry‑side catalog provider glue if redundant with DataFusion registry

**Checklist**
- [x] Move catalog/schema registration state into DataFusion runtime.
- [x] Replace cached registry state with runtime‑owned registry adapter.

---

## Scope 10 — Listing tables and partitioned scans (DataFusion provider alignment)

**Intent**
Use DataFusion listing tables for directory/prefix scans with partition columns and file sort order.

**Status**
Partial. Listing-table registration now uses DataFusion APIs, but provider selection and sort-order shape need final verification.

**Representative pattern**
```python
adapter.register_listing_table(
    name=name,
    location=str(location.path),
    options=listing_options,
    schema=expected_schema,
    overwrite=True,
)
```

**Target files**
- `src/datafusion_engine/registry_bridge.py`
- `src/ibis_engine/registry.py`
- `src/datafusion_engine/io_adapter.py`

**Delete / decommission**
- Any remaining ad‑hoc external table registrations for listing use cases

**Checklist**
- [x] Prefer listing providers when scanning directory/parquet locations with partition or sort metadata.
- [x] Ensure `DatasetLocation.datafusion_provider == "listing"` routes through listing registration without legacy caches.
- [ ] Pass `partition_cols` and `file_sort_order` from scan options end-to-end (verify).

---

## Scope 11 — Substrait lane integration (dual‑lane compile)

**Intent**
Prefer Substrait compilation when available; fallback to DataFusion Expr/DataFrame API, not SQL.

**Status**
Partial. `prefer_substrait` executes Substrait directly (no SQL fallback), but we still lack a DataFrame/Expr fallback path and plan artifacts are SQL-centric.

**Representative pattern**
```python
from ibis_engine.substrait_bridge import ibis_to_substrait_bytes
from datafusion.substrait import Consumer

plan_bytes = ibis_to_substrait_bytes(expr)
plan = Consumer.from_substrait_plan(ctx, plan_bytes)
```

**Target files**
- `src/ibis_engine/substrait_bridge.py`
- `src/datafusion_engine/compile_pipeline.py`
- `src/datafusion_engine/bridge.py`

**Delete / decommission**
- SQL fallback diagnostics in compile pipeline

**Checklist**
- [x] Add Substrait compilation path behind `prefer_substrait` option.
- [ ] Emit diagnostics when falling back to DataFusion Expr/DataFrame (no SQL fallback path yet).

---

## Scope 12 — Schema + nested type functions without SQL DDL

**Intent**
Use DataFusion schema APIs and Rust functions, avoiding SQL DDL in runtime code.

**Status**
Not started. Schema registration still relies on SQL DDL builders.

**Representative pattern**
```python
ctx.table("my_table").schema()
```

**Target files**
- `src/ibis_engine/schema_utils.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/registry_bridge.py`

**Delete / decommission**
- Any remaining string DDL assembly in Ibis‑side schema helpers

**Checklist**
- [ ] Replace DDL string assembly with schema APIs and DataFusion functions.
- [ ] Use DataFusion schema functions for type validation where possible.

---

## Scope 13 — Lineage + semantic diff artifacts (no SQL execution)

**Intent**
Emit lineage and semantic diff metadata derived from Substrait or Ibis IR; SQLGlot may be used for diagnostics only.

**Status**
Not started. Lineage/diff still rely on SQLGlot/SQL-based paths.

**Representative pattern**
```python
from ibis_engine.plan_diff import semantic_diff_ir

diff_payload = semantic_diff_ir(old_expr, new_expr)
```

**Target files**
- `src/ibis_engine/compiler_checkpoint.py`
- `src/ibis_engine/plan_diff.py`
- `src/datafusion_engine/compile_pipeline.py`

**Delete / decommission**
- SQLGlot execution paths used solely to power lineage/diff in runtime

**Checklist**
- [ ] Emit lineage payload from Substrait or Ibis IR in compile pipeline.
- [ ] Emit semantic diff payloads in diagnostics for plan comparisons.

---

## Delete / Decommission Summary

- [ ] All Python UDF definitions and registration paths in `src/datafusion_engine/udf_registry.py`
- [ ] All Python UDF call sites in `src/ibis_engine/builtin_udfs.py` (replace with Rust‑backed wrappers)
- [x] `src/ibis_engine/query_compiler.py`: `_apply_sql_derived`, `_apply_sql_filter`, `_in_set_expr`, `_sql_identifier`, `_sql_literal`
- [x] `src/ibis_engine/hashing.py`: all SQL string construction helpers (`_call_sql`, `_join_parts_sql`, `_hash_expr_sql`, `_prefixed_hash_sql`, `_sql_literal`)
- [x] `src/ibis_engine/sql_bridge.py`: default usage of `table_sql_table` / `backend_sql_table`
- [x] `src/ibis_engine/registry.py`: `_REGISTERED_CATALOGS`, `_REGISTERED_SCHEMAS`, `_REGISTERED_REGISTRY_CATALOGS`
- [x] `src/ibis_engine/sources.py`: direct `ctx.register_record_batches` / `ctx.deregister_table` usage
- [x] `src/ibis_engine/sources.py`: `namespace_recorder_from_ctx` / `record_namespace_action`
- [ ] SQLGlot execution paths in runtime code (diagnostics‑only allowed)

---

## Implementation Sequencing (recommended)

1) Scope 1 + 2 + 3 (Rust surface, Python wrappers, enforcement)
2) Scope 7 + 8 (IO adapter + diagnostics consolidation)
3) Scope 4 + 5 (remove string SQL from compiler + hashing)
4) Scope 6 (SQL ingestion: debug‑only)
5) Scope 9 + 10 (registry alignment + listing tables)
6) Scope 11 (Substrait lane)
7) Scope 12 + 13 (schema functions + lineage/diff artifacts)

---

## Acceptance Criteria

- No string SQL construction remains in runtime execution paths.
- No Python UDFs remain; Rust functions are the only UDF surface.
- All registrations and deregistrations flow through `DataFusionIOAdapter`.
- All diagnostics emitted via `DiagnosticsRecorder` methods.
- Substrait compilation path is used when enabled, with Expr/DataFrame fallback diagnostics (no SQL fallback).
- Listing tables are used for partitioned directory scans.
- Lint and type checks pass:
  - `uv run ruff check --fix`
  - `uv run pyrefly check`
  - `uv run pyright --warnings --pythonversion=3.13`
