#+#+#+#+############################################################
# LibCST DataFusion Schema Additional Features Plan (Design-Phase)
####################################################################

## Goals
- Replace bespoke schema diagnostics with DataFusion-native metadata/provenance hooks.
- Increase robustness to schema drift, view-type coercions, and partition inference changes.
- Improve CST schema introspection and registry integration with DataFusion.
- Provide richer diagnostics and reproducibility for CST views and outputs.

## Non-goals
- Redesigning LibCST extraction logic or changing CST row semantics.
- Replacing Delta/Parquet storage backends outside CST outputs.
- Adding schema inference from payloads (SessionContext remains authoritative).

## Constraints
- SessionContext is the schema authority; do not infer schema from payloads.
- Prefer DataFusion-native introspection and provider metadata over bespoke logic.
- Keep changes compatible with the existing diagnostics sink + manifest notes.

---

## Scope 1: Provider-level schema provenance and constraints
Status: Completed

### Target file list
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/runtime.py`
- `src/schema_spec/system.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/obs/manifest.py`

### Code pattern
```python
ddl = SchemaIntrospector(ctx).table_definition("libcst_files_v1")
constraints = SchemaIntrospector(ctx).table_constraints("libcst_files_v1")
```

### Implementation checklist
- [x] Capture `get_table_definition` (or `SHOW CREATE TABLE`) for CST datasets.
- [x] Capture `information_schema.table_constraints` for CST datasets.
- [x] Record DDL + constraints in diagnostics/manifest notes.
- [x] Use provenance artifacts to replace bespoke DDL-only fingerprints where appropriate.

---

## Scope 2: TableSchema partition schema validation
Status: Completed

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/hamilton_pipeline/modules/outputs.py`

### Code pattern
```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'libcst_files_v1';
```

### Implementation checklist
- [x] Compare DataFusion table schema columns to expected CST partition columns.
- [x] Record mismatches (missing/extra/ordering) in diagnostics.
- [x] Surface partition schema drift in manifest notes.

---

## Scope 3: Scan-boundary schema adapters for drift
Status: Completed

### Target file list
- `src/storage/deltalake/`
- `src/datafusion_engine/registry_bridge.py`
- `src/ibis_engine/registry.py`

### Code pattern
```python
listing = ListingTable(...)
listing = listing.with_schema_adapter_factory(adapter_factory)
ctx.register_table("libcst_files_v1", listing)
```

### Implementation checklist
- [x] Introduce a schema adapter factory for CST Delta/Listing registrations.
- [x] Normalize per-file schema drift to registry schema at scan time.
- [x] Emit diagnostics when adapters apply conversions.

---

## Scope 4: Column defaults as schema contracts
Status: Completed

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/hamilton_pipeline/modules/outputs.py`

### Code pattern
```python
# Provider exposes default metadata (example)
defaults = provider.get_column_default("attrs")
```

### Implementation checklist
- [x] Define CST default values for schema evolution (attrs, metadata fields).
- [x] Expose defaults via provider metadata when possible.
- [x] Record defaults in diagnostics for reproducibility.

---

## Scope 5: Function signature validation via information_schema.parameters
Status: Completed

### Target file list
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```sql
SELECT specific_name, parameter_name, data_type
FROM information_schema.parameters
WHERE specific_name = 'arrow_typeof';
```

### Implementation checklist
- [x] Add `information_schema.parameters` snapshot when enabled.
- [x] Validate signatures for `arrow_typeof`, `arrow_metadata`, `unnest`, `map_entries`.
- [x] Record mismatches in diagnostics with version context.

---

## Scope 6: View-type pinning and schema debugging knobs
Status: Completed

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/engine/runtime_profile.py`
- `src/hamilton_pipeline/modules/outputs.py`

### Code pattern
```python
config = config.with_config("datafusion.execution.parquet.schema_force_view_types", "false")
config = config.with_config("datafusion.explain.show_schema", "true")
config = config.with_config("datafusion.format.types_info", "true")
```

### Implementation checklist
- [x] Pin view-type behavior in CST policy presets.
- [x] Enable schema-including explain plans for CST diagnostics.
- [x] Record view-type settings in manifest notes.

---

## Scope 7: Typed nested accessors for attrs and spans
Status: Completed

### Target file list
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```sql
SELECT
  map_extract(attrs, 'origin') AS origin_values,
  get_field(span, 'bstart') AS bstart
FROM cst_refs;
```

### Implementation checklist
- [x] Add targeted accessor views (e.g., `cst_refs_attr_origin`).
- [x] Prefer `get_field` / `map_extract` over custom parsing logic.
- [x] Register views in fragment specs and validate schemas.

---

## Scope 8: DFSchema output naming validation
Status: Completed

### Target file list
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/schema_registry.py`
- `src/relspec/rules/validation.py`

### Code pattern
```sql
DESCRIBE SELECT a + b AS sum_ab FROM cst_refs;
```

### Implementation checklist
- [x] Capture `DESCRIBE` output for CST views to validate output names.
- [x] Align fragment SQL aliases with DataFusion output naming semantics.
- [x] Emit warnings for schema name drift in diagnostics.

---

## Scope 9: Interop boundary schema projection
Status: Completed

### Target file list
- `src/datafusion_engine/bridge.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/arrowdsl/schema/`

### Code pattern
```python
record_batch.__arrow_c_array__(requested_schema=expected_schema)
```

### Implementation checklist
- [x] Apply requested schema projections at record batch boundaries.
- [x] Remove bespoke column reordering where requested schemas suffice.
- [x] Record projection usage in diagnostics for auditing.

