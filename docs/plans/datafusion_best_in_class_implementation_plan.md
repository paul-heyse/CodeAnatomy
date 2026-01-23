# DataFusion Best-in-Class Implementation Plan

## Purpose
Deliver a consolidated, contract-first, and capability-complete DataFusion integration that:
- Eliminates duplicate protocol paths and drift in runtime policy.
- Makes schema, registration, and scan behavior deterministic and auditable.
- Unlocks underused DataFusion/Delta/Ibis/SQLGlot capabilities already documented.
- Reduces bespoke code by relying on engine-native features.

## Scope Summary (what changes)
- Centralize all SessionContext creation and SQL policy application.
- Replace ad hoc DDL and registration helpers with a single canonical path.
- Move per-table scan options into DDL `OPTIONS(...)` instead of session-level `SET`.
- Make schema evolution adapters default for datasets that opt in.
- Expand Delta integration (scan config, CDF, constraints).
- Improve function catalog introspection and UDF tier routing.
- Add cache introspection and prepared statements for recurrent system queries.
- Align Ibis/SQLGlot execution and diagnostics with DataFusion policy.

## Non-goals
- Rewriting core business logic or data models.
- Changing external APIs without a deprecation window.
- Implementing new Rust extensions immediately (we will stub for Python, then add Rust hooks).

## Implementation Phases

### Phase 1: SessionContext and SQL Policy Unification

**Goal**: Ensure all DataFusion usage paths share the same session config, schema hardening, and SQL policy.

**Key patterns**
```python
# src/datafusion_engine/runtime.py
@dataclass(frozen=True)
class DataFusionRuntimeProfile:
    ...
    def ephemeral_context(self) -> SessionContext:
        """Create a non-cached SessionContext with full policy applied."""
        ctx = self._build_session_context()
        ctx = self._apply_url_table(ctx)
        self._register_local_filesystem(ctx)
        self._install_input_plugins(ctx)
        self._install_registry_catalogs(ctx)
        self._install_delta_table_factory(ctx)
        self._install_udfs(ctx)
        self._install_schema_registry(ctx)
        self._install_function_factory(ctx)
        self._install_expr_planners(ctx)
        self._install_physical_expr_adapter_factory(ctx)
        return ctx
```

```python
# src/arrowdsl/schema/validation.py
from datafusion_engine.runtime import DataFusionRuntimeProfile

def _datafusion_type_name(dtype: DataTypeLike) -> str:
    ctx = DataFusionRuntimeProfile().ephemeral_context()
    ...
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/arrowdsl/schema/validation.py`
- `src/arrowdsl/finalize/finalize.py`
- `src/arrowdsl/core/ids.py`
- `src/hamilton_pipeline/modules/normalization.py`
- `src/schema_spec/view_specs.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/schema_introspection.py`

**Checklist**
- [x] Add `ephemeral_context()` to `DataFusionRuntimeProfile`.
- [x] Replace all `SessionContext()` usages outside the runtime with `ephemeral_context()`.
- [x] Remove local `_sql_options()` helpers in modules and call `sql_options_for_profile()`.
- [x] Ensure schema hardening and SQL policy are always applied.

---

### Phase 2: Canonical DDL + Registration Path

**Goal**: Reduce duplicated registration flows and ensure a single DDL-backed source of truth.

**Key patterns**
```python
# src/datafusion_engine/registry_bridge.py

def register_dataset_df(...):
    context = _build_registration_context(...)
    return _register_dataset_with_context(context)

# Use ONLY registry_bridge.datafusion_external_table_sql for DDL
```

```sql
-- DDL example emitted from schema_spec
CREATE EXTERNAL TABLE libcst_files_v1 (
  repo Utf8 NOT NULL,
  path Utf8 NOT NULL,
  ...
)
STORED AS PARQUET
LOCATION 's3://.../libcst_files/'
OPTIONS (
  'skip_metadata' 'true',
  'schema_force_view_types' 'false'
)
```

**Target files**
- `src/datafusion_engine/registry_bridge.py`
- `src/schema_spec/specs.py`
- `src/schema_spec/system.py`
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/schema_registry.py`

**Checklist**
- [x] Deprecate or remove `ExternalTableDDLBuilder` usage and route through `schema_spec` DDL.
- [x] Remove `register_external_table_via_ddl` if unused; replace its callers with `registry_bridge.register_dataset_ddl`.
- [x] Ensure all dataset registrations go through `register_dataset_df` or `register_dataset_ddl`.

---

### Phase 3: Per-table Scan Settings via DDL `OPTIONS(...)`

**Goal**: Stop mutating global session settings via `SET`; use table-scoped options.

**Key patterns**
```python
# src/schema_spec/specs.py
class ExternalTableConfig:
    ...
    options: Mapping[str, object] | None = None

# Ensure all scan options (skip_metadata, schema_force_view_types, etc.)
# are serialized into ExternalTableConfig.options
```

```python
# src/datafusion_engine/registry_bridge.py
# Remove or narrow _apply_scan_settings
# Only keep session SET for options that cannot be expressed in DDL.
```

**Target files**
- `src/schema_spec/specs.py`
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`

**Checklist**
- [x] Move scan options into `ExternalTableConfig.options` serialization.
- [x] Validate that scan options appear in DDL for all datasets.
- [x] Restrict `_apply_scan_settings` to truly session-only knobs.

---

### Phase 4: Schema Evolution Adapter Defaulting

**Goal**: Use physical expression adapters for datasets with schema evolution rules.

**Key patterns**
```python
# src/datafusion_engine/registry_bridge.py
factory = _resolve_expr_adapter_factory(
    scan,
    runtime_profile=context.runtime_profile,
    dataset_name=context.name,
    location=context.location,
)

# record factory usage in diagnostics
```

**Target files**
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/schema_spec/system.py`

**Checklist**
- [x] Ensure datasets with evolution specs always resolve an adapter factory.
- [x] Add diagnostics payload for adapter usage and evolution policy.
- [x] Validate adapter compatibility during registration.

---

### Phase 5: Function Catalog & UDF Tiering Upgrade

**Goal**: Improve builtin discovery and UDF tier routing using information_schema.

**Key patterns**
```python
# src/datafusion_engine/udf_catalog.py
catalog = FunctionCatalog.from_information_schema(
    routines=routines_snapshot_table(ctx),
    parameters=parameters_snapshot_table(ctx),
    parameters_available=parameters_available,
)

# use return_type and volatility if available
```

**Target files**
- `src/datafusion_engine/udf_catalog.py`
- `src/datafusion_engine/schema_introspection.py`
- `src/engine/function_registry.py`

**Checklist**
- [x] Query `information_schema.parameters` when available.
- [x] Persist return_type and volatility for builtin functions.
- [x] Use signatures to route function selection to builtin tier.

---

### Phase 6: Cache Introspection and Prepared Statements

**Goal**: Add runtime visibility into caches and use PREPARE/EXECUTE for recurring queries.

**Key patterns**
```python
# src/datafusion_engine/cache_introspection.py
# Use list_files_cache(), metadata_cache(), statistics_cache(), predicate_cache() UDTFs when available
```

```python
# src/datafusion_engine/bridge.py
prepare_statement(ctx, "schema_snapshot", "SELECT ...")
execute_prepared_statement(ctx, "schema_snapshot", ["..."])
```

**Target files**
- `src/datafusion_engine/cache_introspection.py`
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/runtime.py`

**Checklist**
- [x] Add extension UDTFs in `datafusion_ext` for cache introspection.
- [x] Wire prepared statements for schema and catalog queries.
- [x] Capture prepared statements in diagnostics.

---

### Phase 7: Delta Integration Expansion (DeltaScan + CDF)

**Goal**: Provide best-in-class Delta capabilities (scan tuning, CDF, constraints).

**Key patterns**
```python
# src/schema_spec/system.py
@dataclass(frozen=True)
class DeltaScanOptions:
    file_column_name: str | None = None
    enable_parquet_pushdown: bool = True
    schema_force_view_types: bool | None = None
    wrap_partition_values: bool = False
    schema: pa.Schema | None = None
```

```python
# src/datafusion_engine/registry_bridge.py
if location.format == "delta" and location.delta_cdf_options is not None:
    # register CDF provider
```

**Target files**
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/engine/delta_tools.py`

**Checklist**
- [x] Codify Delta CDF configuration via `DatasetLocation.delta_cdf_options` (no CDF fields in `DeltaScanOptions`).
- [x] Add CDF registration path (provider or DDL).
- [x] Record Delta scan config in diagnostics.
- [x] Enforce constraint checks via DataFusion when configured.

---

### Phase 8: Ibis + SQLGlot Execution Alignment

**Goal**: Ensure Ibis and SQLGlot routes use the same policy, diagnostics, and session context.

**Key patterns**
```python
# src/datafusion_engine/bridge.py
ctx = options.runtime_profile.session_context()
expr = normalize_expr(expr, policy=options.sqlglot_policy)

# enforce policy at SQL boundary
```

```python
# Ibis usage: prefer AST path with explicit SQLGlot policy
sqlglot_ast = ibis_to_sqlglot(expr, backend=backend)
result = execute_sqlglot_ast(ctx, sqlglot_ast, ...)
```

**Target files**
- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/runner.py`
- `src/ibis_engine/io_bridge.py`
- `src/ibis_engine/compiler_checkpoint.py`

**Checklist**
- [x] Use runtime profile context for all Ibis execution paths.
- [x] Centralize SQLGlot policy application and dialect.
- [x] Record lineage artifacts and plan fingerprints consistently.

---

## Decommission / Delete List (after plan completion)

**Files to remove**
- `src/datafusion_engine/listing_table_provider.py` (unused; DDL-based registration supersedes it)

**Functions to remove or deprecate (if no external callers)**
- `src/datafusion_engine/schema_introspection.py:ExternalTableDDLBuilder`
- `src/datafusion_engine/schema_registry.py:register_external_table_via_ddl`

**Cleanup checks**
- [x] `rg -n "ExternalTableDDLBuilder" src` returns no usage.
- [x] `rg -n "register_external_table_via_ddl" src` returns no usage.
- [x] `rg -n "listing_table_provider" src` returns no usage.

---

## Implementation Checklist (Global)
- [x] Add `DataFusionRuntimeProfile.ephemeral_context()` and replace raw `SessionContext()`.
- [x] Centralize SQL policy helpers and remove local `_sql_options` duplicates.
- [x] Consolidate DDL generation to `schema_spec` and `registry_bridge`.
- [x] Move scan settings into DDL `OPTIONS` where possible.
- [x] Make schema evolution adapter selection explicit and auditable.
- [x] Expand Delta scan options and add CDF support.
- [x] Improve function catalog signature extraction (parameters + return types).
- [x] Implement cache introspection UDTFs in extension and wire them in Python.
- [x] Use prepared statements for recurrent system queries.
- [x] Align Ibis/SQLGlot execution and diagnostics to runtime profile.

---

## Remaining Scope (post-review)
- None. All scope items are implemented in the codebase.

---

## Deliverables
- Unified DataFusion runtime surface and schema contracts.
- Deterministic registration and scan behavior across all datasets.
- Expanded Delta and UDF capabilities with richer diagnostics.
- A simplified, best-in-class integration that minimizes bespoke logic.
