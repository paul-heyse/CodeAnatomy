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
Status: Planned

### Target file list
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/obs/manifest.py`

### Code pattern
```python
provider = ctx.table("cst_refs").provider()
plan = provider.get_logical_plan()
diagnostics.record_artifact("cst_view_plan_v1", {"name": "cst_refs", "plan": plan})
```

### Implementation checklist
- [ ] Expose `get_logical_plan()` from native providers where possible.
- [ ] Snapshot logical plan provenance for CST fragment views.
- [ ] Record plan provenance in diagnostics/manifest notes.

---

## Scope 2: Provider column defaults as schema contracts
Status: Planned

### Target file list
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`
- `src/obs/manifest.py`

### Code pattern
```python
default = provider.get_column_default("attrs")
diagnostics.record_artifact("cst_column_defaults_v1", {"attrs": default})
```

### Implementation checklist
- [ ] Expose `get_column_default()` for CST providers (attrs defaults).
- [ ] Record defaults in CST diagnostics payloads.
- [ ] Use provider defaults to replace bespoke fill logic where possible.

---

## Scope 3: Function signature type validation
Status: Planned

### Target file list
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```sql
SELECT specific_name, data_type, parameter_mode
FROM information_schema.parameters
WHERE specific_name = 'arrow_metadata';
```

### Implementation checklist
- [ ] Validate parameter types and modes for schema-critical functions.
- [ ] Record signature mismatches with DataFusion version context.
- [ ] Fail CST view validation when types drift.

---

## Scope 4: DFSchema tree snapshots for CST views
Status: Planned

### Target file list
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```python
df_schema = ctx.sql("SELECT * FROM cst_refs").schema()
tree = df_schema.tree_string()
diagnostics.record_artifact("cst_dfschema_v1", {"name": "cst_refs", "tree": tree})
```

### Implementation checklist
- [ ] Expose DFSchema tree renderings for CST views.
- [ ] Store DFSchema trees in diagnostics for diffing across versions.
- [ ] Use DFSchema snapshots to validate output naming and nullability.

---

## Scope 5: TableSchema-based partition verification
Status: Planned

### Target file list
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/schema_introspection.py`

### Code pattern
```python
table_schema = provider.table_schema()
file_schema = table_schema.file_schema()
partition_cols = table_schema.table_partition_cols()
```

### Implementation checklist
- [ ] Expose `TableSchema` metadata for listing/Delta providers.
- [ ] Verify partition columns against TableSchema (not just information_schema).
- [ ] Record file schema + partition schema snapshots in diagnostics.

---

## Scope 6: Catalog/SchemaProvider integration
Status: Planned

### Target file list
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`

### Code pattern
```python
catalog = build_catalog_provider(dataset_registry)
ctx.register_catalog("codeintel", catalog)
```

### Implementation checklist
- [ ] Implement a catalog/schema provider backed by dataset registry.
- [ ] Register provider-based catalogs instead of ad-hoc table registration.
- [ ] Ensure information_schema reflects provider-backed tables.

---

## Scope 7: Scan-time projection expressions for CST
Status: Planned

### Target file list
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/query_fragments.py`

### Code pattern
```python
scan = DataFusionScanOptions(projection_exprs=("ref_id", "bstart", "bend"))
register_dataset_df(ctx, name="cst_refs", location=loc, runtime_profile=profile)
```

### Implementation checklist
- [ ] Define projection expressions for hot CST datasets.
- [ ] Prefer scan-time projections over derived views where safe.
- [ ] Record projection usage in listing/Delta diagnostics payloads.

---

## Scope 8: Column-specific Parquet options
Status: Planned

### Target file list
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/storage/deltalake/`

### Code pattern
```sql
CREATE EXTERNAL TABLE libcst_files_v1
STORED AS parquet
OPTIONS (statistics_enabled='ref_id,path');
```

### Implementation checklist
- [ ] Define per-column Parquet options for CST (stats/bloom/dict).
- [ ] Apply options via ExternalTableConfig or scan settings.
- [ ] Capture effective per-column options in diagnostics.

---

## Scope 9: Typed prepared statements for CST diagnostics
Status: Planned

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_registry.py`
- `src/obs/manifest.py`

### Code pattern
```sql
PREPARE cst_ref_span(INT) AS
SELECT * FROM cst_refs WHERE bstart > $1;
```

### Implementation checklist
- [ ] Introduce prepared statements for repeated CST diagnostics queries.
- [ ] Validate parameter types match schema expectations.
- [ ] Record prepared statement inventory in diagnostics.

---

## Scope 10: Per-table listing cache strategy
Status: Planned

### Target file list
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
scan = DataFusionScanOptions(list_files_cache_ttl="30s", listing_mutable=True)
```

### Implementation checklist
- [ ] Define cache TTL/limit policies for CST listing tables.
- [ ] Apply per-table cache policy overrides at registration time.
- [ ] Record cache policy settings in diagnostics and manifest notes.

