# DataFusion Schema Features for Symtable Integration Plan

## Goal
Adopt additional DataFusion schema capabilities for symtable workloads to replace bespoke
logic, improve robustness under schema drift, enable dynamic adaptation, and deepen
integration with the existing schema registry, diagnostics, and CPG pipeline.

## Non-Goals
- URL tables / dynamic file catalog (explicitly out of scope).

## Execution Scopes

### Scope 1: Symtable schema hardening policy (view types + schema debugging)
**Why**: Lock schema behavior across sessions and avoid `Utf8View` surprises while making
schema debugging a first-class workflow for symtable views.

**Representative code pattern**
```python
# datafusion_engine/runtime.py
SYMTABLE_DF_POLICY = DataFusionConfigPolicy(
    name="symtable",
    settings={
        "datafusion.explain.show_schema": True,
        "datafusion.format.types_info": True,
        "datafusion.execution.time_zone": "UTC",
        "datafusion.sql_parser.map_string_types_to_utf8view": False,
        "datafusion.execution.parquet.schema_force_view_types": False,
        "datafusion.optimizer.expand_views_at_output": False,
    },
)
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/engine/runtime_profile.py`
- `src/datafusion_engine/registry_bridge.py`

**Implementation checklist**
- [x] Extend `SYMTABLE_DF_POLICY` with view-type and schema-debugging settings.
- [x] Wire policy selection via runtime profile/env overrides for symtable runs.
- [x] Ensure policy applies before any dataset registration.

---

### Scope 2: Schema metadata + constraints for symtable bundles
**Why**: Encode schema contracts directly in Arrow metadata so validation is structural,
not ad-hoc, and stays consistent with other extractors.

**Representative code pattern**
```python
# datafusion_engine/schema_registry.py
_SYMTABLE_ID_FIELDS = ("file_id", "path")
_SYMTABLE_META = {
    REQUIRED_NON_NULL_META: metadata_list_bytes(_SYMTABLE_ID_FIELDS),
    KEY_FIELDS_META: metadata_list_bytes(_SYMTABLE_ID_FIELDS),
}
SYMTABLE_FILES_SCHEMA = _schema_with_metadata(
    "symtable_files_v1",
    SYMTABLE_FILES_SCHEMA,
    extra_metadata=_SYMTABLE_META,
)
```

**Target files**
- `src/datafusion_engine/schema_registry.py`
- `src/arrowdsl/io/parquet.py`
- `src/datafusion_engine/schema_introspection.py`

**Implementation checklist**
- [x] Add identity and required-non-null metadata for symtable schemas.
- [x] Encode any span ABI metadata (line base, col unit, end exclusive).
- [x] Extend schema validation to assert metadata presence for symtable roots.

---

### Scope 3: Schema evolution at scan boundary (PhysicalExprAdapterFactory)
**Why**: Replace bespoke projection/cast logic with canonical schema adaptation for
symtable datasets that evolve over time.

**Representative code pattern**
```python
# datafusion_engine/runtime.py
factory = _load_schema_evolution_adapter_factory()
ctx.register_physical_expr_adapter_factory(factory)

# schema_spec/system.py
evolution = resolve_schema_evolution_spec("symtable_files_v1")
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/schema_spec/system.py`
- `rust/datafusion_ext/src/lib.rs`

**Implementation checklist**
- [x] Bind schema evolution spec to symtable datasets in registry policies.
- [x] Register the physical expr adapter factory for symtable sessions.
- [x] Ensure listing-table providers use the adapter for scan-time alignment.

---

### Scope 4: Listing-table discovery caching + partition inference controls
**Why**: Make symtable scans stable or fresh on demand, and avoid unintended schema
changes from inferred partitions.

**Representative code pattern**
```python
# datafusion_engine/runtime.py
_set_runtime_setting(ctx, "datafusion.runtime.list_files_cache_limit", "1M")
_set_runtime_setting(ctx, "datafusion.runtime.list_files_cache_ttl", "120s")
_set_runtime_setting(ctx, "datafusion.execution.listing_table_factory_infer_partitions", "false")
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/schema_spec/system.py`

**Implementation checklist**
- [x] Add runtime settings for list cache limit/TTL.
- [x] Expose dataset-level toggles for partition inference.
- [x] Document snapshot vs append-only defaults for symtable runs.

---

### Scope 5: Partition validation + TableSchema semantics
**Why**: Ensure partition columns match directory layout and avoid silent schema drift
when using listing tables for symtable bundles.

**Representative code pattern**
```python
# datafusion_engine/registry_bridge.py
if scan.partition_cols:
    _validate_listing_partitions(
        ctx,
        path=context.location.path,
        partition_cols=scan.partition_cols,
    )
```

**Target files**
- `src/datafusion_engine/registry_bridge.py`
- `src/schema_spec/system.py`
- `rust/datafusion_ext/src/lib.rs`

**Implementation checklist**
- [x] Add partition validation for listing tables (Rust helper or SQL smoke query).
- [x] Align `TableSchema` construction with file + partition columns.
- [x] Add a failure path when partition ordering mismatches layout.

---

### Scope 6: Scan-time projection expressions for derived fields
**Why**: Move derived columns (IDs, flags, derived fields) to the scan boundary to reduce
view complexity and keep schema evolution centralized.

**Representative code pattern**
```python
# datafusion_engine/registry_bridge.py
projection_exprs = [
    "sym_symbol_id = prefixed_hash64('sym', scope_id, symbol_name)",
    "is_meta_scope = (scope_type NOT IN ('MODULE', 'FUNCTION', 'CLASS'))",
]
_register_listing_table_with_projection(ctx, path, schema, projection_exprs)
```

**Target files**
- `src/datafusion_engine/registry_bridge.py`
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/query_fragments.py`

**Implementation checklist**
- [x] Add a provider path that supports scan-time projection expressions.
- [x] Shift symtable-derived fields from views into scan projection.
- [x] Keep view SQL thin and stable (pure selection + aliasing).

---

### Scope 7: Provider schema provenance (DDL + logical plan + defaults)
**Why**: Replace custom provenance tracking by using provider-native metadata hooks for
schema lineage and reproducibility.

**Representative code pattern**
```rust
// rust/datafusion_ext/src/lib.rs
impl TableProvider for CpgTableProvider {
    fn get_table_definition(&self) -> Option<&str> { Some(&self.ddl) }
    fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> { self.plan.clone() }
    fn get_column_default(&self, col: &str) -> Option<&Expr> { self.defaults.get(col) }
}
```

**Target files**
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/schema_registry.py`

**Implementation checklist**
- [x] Store CREATE EXTERNAL TABLE text for symtable bundles in registry metadata.
- [x] Add provider hooks to expose table definition and plan.
- [x] Use defaults for new attrs fields when evolving schemas.

---

### Scope 8: Schema introspection + diagnostics workflow for symtable
**Why**: Validate actual DataFusion schema (including nullability and derived views)
using native introspection instead of custom checks.

**Representative code pattern**
```python
# datafusion_engine/schema_registry.py
ctx.sql("DESCRIBE SELECT * FROM symtable_scopes").collect()
ctx.sql(
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_name = 'symtable_scopes'"
).collect()
```

**Target files**
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`
- `src/relspec/rules/validation.py`

**Implementation checklist**
- [x] Add `DESCRIBE <query>` validation for symtable view definitions.
- [x] Add `information_schema` checks for expected columns/types.
- [x] Record diagnostics in schema validation tables on failure.

---

### Scope 9: Tests + contract harness for schema features
**Why**: Lock behavior so schema adaptation, metadata, and introspection remain stable
across upgrades.

**Representative code pattern**
```python
def test_symtable_schema_metadata(symtable_ctx: TestContext) -> None:
    table = symtable_ctx.gateway.table("symtable_files_v1")
    assert table.schema.metadata[b"arrowdsl.key_fields"] == b"[\"file_id\",\"path\"]"
```

**Target files**
- `tests/datafusion_engine/test_schema_registry.py`
- `tests/datafusion_engine/test_runtime_profile.py`
- `tests/normalize/test_diagnostics.py`

**Implementation checklist**
- [x] Add tests for schema metadata.
- [x] Add tests for required functions.
- [x] Add tests for listing cache TTL behaviors and partition inference toggles.
- [x] Add tests for schema evolution adapter integration paths.

---

## Dependencies / Ordering
1) Policy hardening (Scope 1) should land before any schema evolution or listing tuning.
2) Metadata + constraints (Scope 2) should precede schema evolution and provenance hooks.
3) Listing cache + partition inference controls (Scope 4) should precede validation (Scope 5).
4) Diagnostics workflow (Scope 8) should land before tests (Scope 9).

## Notes
- URL tables / dynamic file catalog remain out of scope per approval.
- Each scope is designed to be independently shippable but composes into a full schema
  hardening and evolution strategy for symtable datasets.
