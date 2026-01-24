# DataFusion Unifier Modularization Plan

## Goals

- Consolidate DataFusion, Ibis, and SQLGlot integration into a single, programmatic execution substrate.
- Shift from stringly-typed SQL to SQLGlot AST-first plan and DDL generation.
- Centralize IO, catalog, write, and introspection behavior so every execution path is consistent.
- Reduce duplication and decommission legacy helpers once the unifier is in place.

## Non-Goals

- Do not change business semantics of existing datasets.
- Do not change external API contracts unless explicitly listed.
- Do not remove DataFusion runtime safeguards or diagnostics coverage.

## Scope Items

### 1) Unified DataFusion IO Adapter (Catalog + Registration + Object Stores)

**Intent**
Create a single IO adapter used by all DataFusion access paths. This consolidates:
object store registration, DDL-based external table registration, and dataset metadata capture.

**Representative pattern**
```python
# src/datafusion_engine/io_adapter.py
from dataclasses import dataclass
from typing import Mapping

from datafusion import SessionContext

from ibis_engine.registry import DatasetLocation
from datafusion_engine.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class DataFusionIOAdapter:
    ctx: SessionContext
    profile: DataFusionRuntimeProfile | None

    def register_object_store(self, *, scheme: str, store: object, host: str | None) -> None:
        self.ctx.register_object_store(scheme, store, host)

    def external_table_ddl(self, *, name: str, location: DatasetLocation) -> str:
        # Uses SQLGlot AST builder + DataFusion dialect rendering.
        return build_external_table_ddl(name=name, location=location, profile=self.profile)

    def register_external_table(self, *, name: str, location: DatasetLocation) -> None:
        ddl = self.external_table_ddl(name=name, location=location)
        self.ctx.sql_with_options(ddl, statement_sql_options_for_profile(self.profile)).collect()
```

**Target files**
- `src/datafusion_engine/io_adapter.py` (new)
- `src/datafusion_engine/registry_bridge.py`
- `src/ibis_engine/backend.py`
- `src/engine/session_factory.py`
- `src/datafusion_engine/runtime.py`

**Implementation checklist**
- Create `DataFusionIOAdapter` and move object-store registration logic into it.
- Route `register_dataset_df` and external table DDL registration through the adapter.
- Replace direct calls to `ctx.register_object_store` in Ibis backend with adapter calls.
- Ensure diagnostics artifacts for object stores and table providers flow through adapter.

---

### 2) SQLGlot AST-First DDL/COPY Generation

**Intent**
Replace string SQL assembly with SQLGlot AST generation for DDL/COPY/INSERT. This improves
stability, safety, and dialect consistency.

**Representative pattern**
```python
# src/sqlglot_tools/ddl_builders.py
from sqlglot_tools.compat import exp

def build_copy_to_ast(*, query: exp.Expression, path: str, options: dict[str, object]) -> exp.Expression:
    return exp.Copy(
        this=exp.Subquery(this=query),
        to=exp.Literal.string(path),
        kind=exp.Var(this="PARQUET"),
        options=[exp.Option(this=exp.Var(this=k), value=exp.Literal.string(str(v)))
                 for k, v in options.items()],
    )
```

**Target files**
- `src/sqlglot_tools/ddl_builders.py` (new)
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/bridge.py`
- `src/engine/materialize_pipeline.py`
- `src/ibis_engine/io_bridge.py`

**Implementation checklist**
- Implement SQLGlot AST builders for external table DDL, COPY, and INSERT.
- Replace `_copy_select_sql`, `_sql_identifier`, and ad-hoc SQL assembly with AST rendering.
- Ensure dialect is always explicit and aligned with DataFusion policy surfaces.
- Add tests that compare AST fingerprints rather than formatted SQL strings.

---

### 3) Parameter Binding Unification (Scalar + Table-like)

**Intent**
Standardize parameter handling for all SQL execution paths. Enforce a two-lane model:
scalar params use `param_values`, table-like params use named parameter registration.

**Representative pattern**
```python
# src/datafusion_engine/param_binding.py
from dataclasses import dataclass
from typing import Mapping

from ibis.expr.types import Value


@dataclass(frozen=True)
class DataFusionParamBindings:
    param_values: dict[str, object]
    named_tables: dict[str, object]


def resolve_param_bindings(
    values: Mapping[str, object] | Mapping[Value, object] | None,
) -> DataFusionParamBindings:
    # scalar values -> param_values, table-like -> named_tables
    ...
```

**Target files**
- `src/datafusion_engine/param_binding.py` (new)
- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/params_bridge.py`
- `src/engine/materialize_pipeline.py`

**Implementation checklist**
- Build a unified resolver returning `param_values` + `named_tables`.
- Enforce allowlists for parameter names in one place.
- Route all SQL execution through this resolver; delete ad-hoc parameter checks.
- Add diagnostics for param modes and signatures to a single recording hook.

---

### 4) Single Compilation Lane Orchestration (Substrait → AST → SQL)

**Intent**
Make compilation and execution flow deterministic and centralized. Every plan must pass
through a single orchestration entrypoint.

**Representative pattern**
```python
# src/datafusion_engine/compile_pipeline.py
def compile_to_dataframe(expr, *, ctx, options):
    if options.prefer_substrait:
        df = try_substrait_lane(expr, ctx, options)
        if df is not None:
            return df
    if options.prefer_ast_execution:
        ast = ibis_to_sqlglot(expr, backend=options.backend)
        return df_from_sqlglot(ctx, ast)
    sql = sqlglot_emit(ibis_to_sqlglot(expr, backend=options.backend), dialect=options.dialect)
    return ctx.sql_with_options(sql, options.sql_options)
```

**Target files**
- `src/datafusion_engine/compile_pipeline.py` (new)
- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/execution.py`
- `src/ibis_engine/query_compiler.py`

**Implementation checklist**
- Create a compile pipeline module and route all execution through it.
- Remove duplicate compile logic from Ibis execution and DataFusion bridge.
- Record lane selection and fallback reasons in a single diagnostics artifact.

---

### 5) Unified Write Pipeline (COPY/INSERT/Parquet Policy)

**Intent**
Provide a single writing surface with explicit format policy, partitioning, and schema
constraints, while avoiding inconsistent Ibis vs DataFusion write behavior.

**Representative pattern**
```python
# src/datafusion_engine/write_pipeline.py
from dataclasses import dataclass


@dataclass(frozen=True)
class WriteRequest:
    sql: str
    path: str
    file_format: str
    partition_by: tuple[str, ...]
    policy: DataFusionWritePolicy | None


def write_via_copy(ctx, request: WriteRequest, *, profile):
    ast = build_copy_to_ast(
        query=parse_one(request.sql, dialect=profile.sql_dialect),
        path=request.path,
        options=copy_options_from_policy(request.policy),
    )
    sql = ast.sql(dialect=profile.sql_dialect)
    return execute_dml(ctx, sql, options=DataFusionDmlOptions(...))
```

**Target files**
- `src/datafusion_engine/write_pipeline.py` (new)
- `src/engine/materialize_pipeline.py`
- `src/ibis_engine/io_bridge.py`
- `src/datafusion_engine/bridge.py`

**Implementation checklist**
- Define a single WriteRequest and handler for COPY/INSERT.
- Remove direct Parquet write paths where COPY is available.
- Ensure DataFusion write policy is applied consistently (including Parquet options).
- Record write artifacts from a single source for observability.

---

### 6) Unified Information Schema + Introspection Cache

**Intent**
Make information_schema-derived metadata the single source of truth for schema,
function catalog, and settings. Avoid separate metadata caches.

**Representative pattern**
```python
# src/datafusion_engine/introspection.py
@dataclass(frozen=True)
class IntrospectionSnapshot:
    tables: pa.Table
    columns: pa.Table
    routines: pa.Table | None
    settings: pa.Table

    def schema_map(self) -> SchemaMapping:
        return schema_map_from_info_schema(self.columns)
```

**Target files**
- `src/datafusion_engine/introspection.py` (new)
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/udf_catalog.py`
- `src/engine/function_registry.py`

**Implementation checklist**
- Introduce a single snapshot API that yields tables, columns, routines, settings.
- Route function registry build to use the snapshot, not direct queries.
- Invalidate snapshots centrally when DDL changes tables.

---

### 7) Unified Function Registry (DataFusion + Ibis + PyArrow)

**Intent**
Define one function registry with lane precedence and a single source-of-truth for
built-ins, UDFs, and SQL expressions.

**Representative pattern**
```python
# src/engine/function_registry.py
def build_function_registry(*, ctx, ibis_specs, datafusion_specs):
    info_schema = introspector.snapshot(ctx)
    builtins = functions_from_info_schema(info_schema.routines)
    return merge_specs(builtins, ibis_specs, datafusion_specs)
```

**Target files**
- `src/engine/function_registry.py`
- `src/datafusion_engine/udf_registry.py`
- `src/ibis_engine/builtin_udfs.py`

**Implementation checklist**
- Ensure info_schema-derived builtins are merged into registry.
- Remove redundant local lists for builtins where info_schema can be used.
- Keep strict validation in DataFusion runtime for rulepack allowlists.

---

### 8) Diagnostics and Observability Consolidation

**Intent**
All diagnostics should be emitted from a shared adapter/pipeline rather than from each
local subsystem.

**Representative pattern**
```python
# src/datafusion_engine/diagnostics.py
def record_artifact(profile, name: str, payload: Mapping[str, object]) -> None:
    if profile is None or profile.diagnostics_sink is None:
        return
    profile.diagnostics_sink.record_artifact(name, payload)
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/bridge.py`
- `src/engine/materialize_pipeline.py`
- `src/obs/*`

**Implementation checklist**
- Introduce a single diagnostics helper module.
- Route all existing diagnostics emission through the helper.
- Ensure consistent payload shape for artifacts across execution lanes.

---

## Decommission and Delete List (Post-Implementation)

The following items should be removed once the above refactors are complete. This is a
decommission plan, not immediate deletion.

**Duplicate helpers**
- `src/engine/materialize_pipeline.py`: `_sql_identifier`, `_copy_select_sql`,
  `_copy_statement_overrides`, `_copy_options_payload`, `_deregister_table`
- `src/ibis_engine/io_bridge.py`: `_deregister_table`
- `src/datafusion_engine/runtime.py`: `_deregister_table` (replace with centralized helper)

**Legacy SQL assembly**
- Any direct string SQL assembly for COPY or CREATE EXTERNAL TABLE in:
  - `src/engine/materialize_pipeline.py`
  - `src/datafusion_engine/registry_bridge.py`
  - `src/ibis_engine/io_bridge.py`

**Obsolete metadata layers**
- Any direct `information_schema` querying that bypasses the new introspection snapshot.
  Target functions for removal or relocation:
  - `src/datafusion_engine/schema_introspection.py` query helpers once centralized
  - `src/datafusion_engine/udf_catalog.py` direct routine/parameter queries

**Execution duplication**
- Any duplicated compile/execution lane logic in:
  - `src/datafusion_engine/bridge.py`
  - `src/ibis_engine/execution.py`
  - `src/ibis_engine/query_compiler.py`

## Execution Phases (Suggested)

1) Introduce adapter modules (IO, param binding, compile pipeline, write pipeline).
2) Route existing code paths through adapters; keep old helpers but stop using them.
3) Replace SQL assembly with AST builders.
4) Consolidate introspection and function registry.
5) Remove decommissioned functions and run full lint/type gates.

## Acceptance Gates

- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`
- Ensure no plan cache keys or diagnostics payloads change unexpectedly without approval.
