# LibCST DataFusion Schema Best-in-Class Plan (Design-Phase)

## Goals
- Deploy DataFusion schema/catalog features that materially improve LibCST extraction.
- Keep **SessionContext** as the schema authority (no Python-side schema inference).
- Make nested CST data more queryable with DataFusion-native functions and views.
- Improve diagnostics, versioning, and correctness for CST outputs.

## Non-goals
- Rewriting LibCST extraction logic or CPG semantics.
- Replacing Delta or storage backends already in use outside CST outputs.
- Introducing legacy compatibility shims for renamed CST datasets.

## Constraints
- All schema resolution must flow through DataFusion SessionContext and centralized
  schema authority helpers.
- Avoid schema inference from payloads; prefer declared schemas and DataFusion
  introspection.

---

## Scope 1: External table registration with partitioning + ordering + unbounded sources
Status: Planned

### Target file list
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/schema_spec/system.py`
- `src/ibis_engine/registry.py`
- `src/storage/deltalake/`

### Code pattern
```sql
CREATE EXTERNAL TABLE libcst_files_v1
STORED AS parquet
LOCATION 's3://.../libcst_files/'
PARTITIONED BY (repo, commit)
WITH ORDER (path, file_id);

CREATE UNBOUNDED EXTERNAL TABLE libcst_refs
STORED AS parquet
LOCATION 's3://.../cst_refs/';
```

### Implementation checklist
- [ ] Add explicit partition/order registration for CST output tables (Delta/Parquet).
- [ ] Support unbounded external tables for streaming CST ingestion.
- [ ] Wire table-level config policies (stats on/off, row-group pruning) per CST table.
- [ ] Validate ordering contracts vs observed file ordering in diagnostics.

---

## Scope 2: Catalog auto-loading for CST artifacts
Status: Planned

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_loader.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```python
config = SessionConfig()
config = config.with_config("datafusion.catalog.location", "s3://.../cst/")
config = config.with_config("datafusion.catalog.format", "parquet")
ctx = SessionContext(config)
```

### Implementation checklist
- [ ] Add config-policy presets that auto-mount CST artifact roots.
- [ ] Ensure auto-loaded tables still reconcile with declared schemas.
- [ ] Record auto-loaded table inventory in diagnostics for reproducibility.

---

## Scope 3: Arrow schema diagnostics (arrow_typeof / arrow_metadata / arrow_cast)
Status: Planned

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_introspection.py`
- `src/obs/manifest.py`
- `src/datafusion_engine/query_fragments.py`

### Code pattern
```sql
SELECT
  arrow_typeof(refs) AS refs_type,
  arrow_typeof(defs) AS defs_type,
  arrow_metadata(refs) AS refs_meta
FROM libcst_files_v1
LIMIT 1;
```

### Implementation checklist
- [ ] Add a diagnostics view/table that snapshots nested CST types + metadata.
- [ ] Use `arrow_cast` to normalize list-view types in schema audits when needed.
- [ ] Record schema diagnostics into the run manifest for drift detection.

---

## Scope 4: Attribute map expansion views for CST datasets
Status: Planned

### Target file list
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```sql
WITH base AS (SELECT * FROM cst_refs)
SELECT
  base.file_id,
  base.ref_id,
  kv['key'] AS attr_key,
  kv['value'] AS attr_value
FROM base
CROSS JOIN unnest(map_entries(base.attrs)) AS kv;
```

### Implementation checklist
- [ ] Add view fragments for `cst_*_attrs` (refs/defs/callsites/imports/nodes/edges).
- [ ] Ensure attrs views are registered in SessionContext via fragment specs.
- [ ] Add lightweight tests that views expose `attr_key`/`attr_value`.

---

## Scope 5: Struct-based span packaging + unnest ergonomics
Status: Planned

### Target file list
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```sql
SELECT
  base.ref_id,
  named_struct('bstart', base.bstart, 'bend', base.bend) AS span
FROM cst_refs base;
```

### Implementation checklist
- [ ] Provide span-packaged views (`cst_ref_spans`, `cst_callsite_spans`, `cst_def_spans`).
- [ ] Add optional `unnest(span)` helper views for column flattening.
- [ ] Keep raw `bstart`/`bend` columns intact for existing consumers.

---

## Scope 6: Delta TableProvider integration for CST outputs
Status: Planned

### Target file list
- `src/datafusion_engine/registry_bridge.py`
- `src/ibis_engine/registry.py`
- `src/storage/deltalake/`
- `docs/plans/deltalake_advanced_integration_plan.md` (alignment)

### Code pattern
```python
ctx = DataFusionRuntimeProfile().session_context()
dt = DeltaTable(path, version=version)
ctx.register_table("libcst_files_v1", dt)
```

### Implementation checklist
- [ ] Prefer DeltaTableProvider registrations for CST outputs when available.
- [ ] Expose time-travel options (version / timestamp) via dataset registry entries.
- [ ] Validate schema parity: registry schema ↔ delta log ↔ DataFusion provider.

---

## Scope 7: Constraint + nullability enforcement for CST schemas
Status: Planned

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/schema_spec/system.py`
- `src/datafusion_engine/schema_introspection.py`
- `src/obs/manifest.py`

### Code pattern
```python
pa.field("file_id", pa.string(), nullable=False)
pa.field("ref_id", pa.string(), nullable=False)
```

### Implementation checklist
- [ ] Mark non-nullable fields for CST join keys and identifiers.
- [ ] Add validation hooks that detect nulls in non-nullable fields.
- [ ] Emit constraint violations into diagnostics/manifest notes.

---

## Scope 8: information_schema function inventory snapshot
Status: Planned

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_introspection.py`
- `src/obs/manifest.py`

### Code pattern
```sql
SELECT routine_name, routine_type
FROM information_schema.routines;
```

### Implementation checklist
- [ ] Capture function inventory in a diagnostics snapshot.
- [ ] Validate presence of required schema functions (`arrow_typeof`, `unnest`, `map_entries`).
- [ ] Record function inventory in the run manifest for reproducibility.
