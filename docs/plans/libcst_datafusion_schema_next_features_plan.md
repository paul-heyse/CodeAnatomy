#+#+#+#+########################################################
# LibCST DataFusion Schema Next Features Plan (Design-Phase)
################################################################

## Goals
- Replace remaining bespoke CST schema behaviors with DataFusion-native features.
- Improve robustness to schema drift and version differences.
- Increase performance via schema-driven scan/pushdown features.
- Provide richer diagnostics and reproducibility across CST views and outputs.

## Non-goals
- Rewriting LibCST extraction or altering CST row semantics.
- Replacing Delta/Parquet storage backends outside CST outputs.
- Introducing payload-based schema inference.

## Constraints
- SessionContext remains the schema authority.
- Prefer provider metadata and information_schema over custom logic.
- Keep diagnostics compatible with the manifest + diagnostics sink.

---

## Scope 1: Logical-plan provenance for CST views
Status: Completed

### Target file list
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_registry.py`
- `src/hamilton_pipeline/modules/outputs.py`

### Code pattern
```python
plan = _table_logical_plan(ctx, "cst_refs")
diagnostics.record_artifact(
    "datafusion_cst_view_plans_v1",
    {"views": [{"name": "cst_refs", "plan": plan}]},
)
```

### Implementation checklist
- [x] Expose logical plan access via `datafusion_ext.table_logical_plan`.
- [x] Snapshot logical plan provenance for CST views.
- [x] Record plan provenance in diagnostics/manifest notes.

---

## Scope 2: Provider column defaults as schema contracts
Status: Completed

### Target file list
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`
- `src/extract/cst_extract.py`
- `src/hamilton_pipeline/modules/outputs.py`

### Code pattern
```python
defaults = introspector.table_column_defaults("libcst_files_v1")
diagnostics.record_artifact("datafusion_cst_schema_v1", {"default_values": defaults})
attrs = attrs_map(default_attrs_value())
```

### Implementation checklist
- [x] Use information_schema + Arrow metadata defaults for CST columns.
- [x] Record defaults in CST diagnostics payloads.
- [x] Replace bespoke attrs fill logic with `default_attrs_value()`.

---

## Scope 3: Function signature type validation
Status: Completed

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/schema_introspection.py`

### Code pattern
```sql
SELECT specific_name, data_type, parameter_mode
FROM information_schema.parameters
WHERE specific_name = 'arrow_metadata';
```

### Implementation checklist
- [x] Validate parameter types and modes for schema-critical functions.
- [x] Record signature mismatches with DataFusion context.
- [x] Fail CST view validation when types drift.

---

## Scope 4: DFSchema tree snapshots for CST views
Status: Completed

### Target file list
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_registry.py`
- `src/hamilton_pipeline/modules/outputs.py`

### Code pattern
```python
tree = _table_dfschema_tree(ctx, "cst_refs")
diagnostics.record_artifact(
    "datafusion_cst_dfschema_v1",
    {"views": [{"name": "cst_refs", "tree": tree}]},
)
```

### Implementation checklist
- [x] Expose DFSchema tree renderings for CST views.
- [x] Store DFSchema trees in diagnostics for diffing across versions.
- [x] Use DFSchema snapshots to validate output naming and nullability.

---

## Scope 5: TableSchema-based partition verification
Status: Completed

### Target file list
- `src/datafusion_engine/registry_bridge.py`

### Code pattern
```python
snapshot = _table_schema_snapshot(table)
file_schema = snapshot["file_schema"]
partition_cols = snapshot["partition_cols"]
```

### Implementation checklist
- [x] Capture file schema + partition columns from DataFusion table schema.
- [x] Verify partition columns against schema (not just information_schema).
- [x] Record file schema + partition schema snapshots in diagnostics.

---

## Scope 6: Catalog/SchemaProvider integration
Status: Completed

### Target file list
- `src/datafusion_engine/catalog_provider.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_authority.py`

### Code pattern
```python
register_registry_catalogs(ctx, catalogs={"public": registry.catalog})
```

### Implementation checklist
- [x] Implement catalog/schema providers backed by dataset registry.
- [x] Register provider-based catalogs via runtime bootstrap.
- [x] Ensure information_schema reflects provider-backed tables.

---

## Scope 7: Scan-time projection expressions for CST
Status: Completed

### Target file list
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`

### Code pattern
```python
scan = DataFusionScanOptions(projection_exprs=("ref_id", "bstart", "bend"))
register_dataset_df(ctx, name="cst_refs", location=loc, runtime_profile=profile)
```

### Implementation checklist
- [x] Define projection expressions for hot CST datasets.
- [x] Prefer scan-time projections over derived views where safe.
- [x] Record projection usage in listing/Delta diagnostics payloads.

---

## Scope 8: Column-specific Parquet options
Status: Completed

### Target file list
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`

### Code pattern
```sql
CREATE EXTERNAL TABLE libcst_files_v1
STORED AS parquet
OPTIONS (statistics_enabled='ref_id,path');
```

### Implementation checklist
- [x] Define per-column Parquet options for CST (stats/bloom/dict).
- [x] Apply options via external table config + scan settings.
- [x] Capture effective per-column options in diagnostics.

---

## Scope 9: Typed prepared statements for CST diagnostics
Status: Completed

### Target file list
- `src/datafusion_engine/runtime.py`

### Code pattern
```sql
PREPARE cst_ref_span(INT) AS
SELECT * FROM cst_refs WHERE bstart > $1;
```

### Implementation checklist
- [x] Introduce prepared statements for repeated CST diagnostics queries.
- [x] Validate parameter types match schema expectations.
- [x] Record prepared statement inventory in diagnostics.

---

## Scope 10: Per-table listing cache strategy
Status: Completed

### Target file list
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`

### Code pattern
```python
scan = DataFusionScanOptions(list_files_cache_ttl="30s", listing_mutable=True)
```

### Implementation checklist
- [x] Define cache TTL/limit policies for CST listing tables.
- [x] Apply per-table cache policy overrides at registration time.
- [x] Record cache policy settings in diagnostics and manifest notes.

