#######################################################################
# AST DataFusion Schema Next Features Plan (Design-Phase)
#######################################################################

## Goals
- Replace AST-specific bespoke schema logic with DataFusion-native catalog, DDL, and introspection.
- Improve robustness to schema drift and runtime feature differences via information_schema checks.
- Increase performance via partitioning, ordering contracts, and provider pushdown.
- Align AST outputs with DataFusion-native write paths and diagnostics.

## Non-goals
- Rewriting AST extraction semantics or node/edge identifiers.
- Replacing LibCST/SCIP extraction or normalization.
- Introducing schema inference on raw AST outputs.

## Constraints
- SessionContext remains the schema authority (information_schema is the source of truth).
- Keep ast_files_v1 backward compatible (new fields nullable).
- Avoid private DataFusion APIs; prefer documented SQL/API surfaces.

---

## Scope 1: External table registration for AST outputs
Status: Planned

### Target file list
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/schema_spec/system.py`
- `src/storage/deltalake/`

### Code pattern
```sql
CREATE EXTERNAL TABLE ast_files_v1
STORED AS parquet
LOCATION 's3://.../ast_files_v1/'
PARTITIONED BY (repo, path)
WITH ORDER (repo, path);
```

### Implementation checklist
- [ ] Add ExternalTableConfig support for AST outputs (location, partitioning, ordering).
- [ ] Default AST outputs to external-table registration when a location is provided.
- [ ] Record table creation metadata in diagnostics (location, partitions, ordering).
- [ ] Gate collect_statistics via DataFusion config and record the setting.

---

## Scope 2: Catalog auto-loading for AST datasets
Status: Planned

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/obs/manifest.py`

### Code pattern
```python
config = config.set("datafusion.catalog.location", catalog_root)
config = config.set("datafusion.catalog.format", "parquet")
```

### Implementation checklist
- [ ] Add runtime profile options to enable catalog auto-loading.
- [ ] Wire catalog auto-load to AST output locations when configured.
- [ ] Snapshot auto-loaded tables via information_schema for diagnostics.

---

## Scope 3: Delta TableProvider integration for AST outputs
Status: Planned

### Target file list
- `src/storage/deltalake/`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```python
from deltalake import DeltaTable

provider = DeltaTable(location)
ctx.register_table("ast_files_v1", provider)
```

### Implementation checklist
- [ ] Add Delta-backed AST table registration path (read and write).
- [ ] Compare Delta schema JSON to schema_registry hash for contract checks.
- [ ] Support time-travel selectors (version or timestamp) in AST registry rows.
- [ ] Record Delta snapshot metadata in diagnostics.

---

## Scope 4: DataFusion-native write paths (COPY / INSERT)
Status: Planned

### Target file list
- `src/engine/materialize.py`
- `src/storage/deltalake/`
- `src/datafusion_engine/runtime.py`

### Code pattern
```sql
COPY (SELECT * FROM ast_files_v1)
TO 's3://.../ast_files_v1/'
STORED AS parquet
PARTITIONED BY (repo, path);
```

### Implementation checklist
- [ ] Add COPY-based write path for AST outputs when external locations are configured.
- [ ] Use INSERT INTO for Delta-backed tables when provider supports insert.
- [ ] Record write mode (copy/insert) and output metadata in diagnostics.

---

## Scope 5: Struct/map construction for AST derived views
Status: Planned

### Target file list
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`
- `src/ibis_engine/plan.py`

### Code pattern
```sql
SELECT
  root.path,
  named_struct(
    'span', named_struct('start', span['start'], 'end', span['end']),
    'attrs', root.attrs
  ) AS ast_record
FROM ast_files_v1 AS root;
```

### Implementation checklist
- [ ] Build derived AST views using named_struct/struct instead of bespoke shaping.
- [ ] Standardize span + attrs packaging for downstream joins.
- [ ] Document struct field names in schema_registry metadata.

---

## Scope 6: Map and unnest helpers for AST attrs
Status: Planned

### Target file list
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```sql
SELECT
  root.path,
  unnest(map_entries(node['attrs'])) AS kv
FROM (
  SELECT path, unnest(nodes) AS node
  FROM ast_files_v1
) AS root;
```

### Implementation checklist
- [ ] Add standardized fragments for map_entries/map_extract usage on AST attrs.
- [ ] Provide helper views for common AST attr queries (expose key/value pairs).
- [ ] Use unnest(list) for per-node attr exploration.

---

## Scope 7: information_schema-based validation and function signatures
Status: Planned

### Target file list
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```sql
SELECT specific_name, data_type, parameter_mode
FROM information_schema.parameters
WHERE specific_name IN ('arrow_typeof', 'arrow_cast', 'arrow_metadata');
```

### Implementation checklist
- [ ] Validate AST views using information_schema.tables/columns.
- [ ] Validate required function signatures via information_schema.parameters.
- [ ] Record mismatches and DataFusion version in diagnostics.

---

## Scope 8: Arrow metadata and precise casting for spans
Status: Planned

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `src/obs/manifest.py`

### Code pattern
```sql
SELECT
  arrow_metadata(nodes, 'col_unit') AS col_unit,
  arrow_typeof(nodes) AS nodes_type
FROM ast_files_v1
LIMIT 1;
```

### Implementation checklist
- [ ] Use arrow_metadata to validate AST span ABI (line base, col unit, end exclusivity).
- [ ] Use arrow_cast for precise nested casting when aligning derived views.
- [ ] Store span ABI metadata snapshots in diagnostics.

---

## Scope 9: Constraints and nullability as schema contracts
Status: Planned

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/schema_spec/system.py`

### Code pattern
```python
schema = pa.schema(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
    ]
)
```

### Implementation checklist
- [ ] Tighten AST schema nullability on core identity fields.
- [ ] Declare constraints in schema metadata for diagnostics and audits.
- [ ] Add validation for non-nullable column behavior during materialization.

---

## Scope 10: Version-gated feature enablement
Status: Planned

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/obs/manifest.py`

### Code pattern
```sql
SELECT version() AS datafusion_version;
```

### Implementation checklist
- [ ] Record DataFusion version in runtime snapshots.
- [ ] Gate optional AST view features on version checks.
- [ ] Surface version gating decisions in diagnostics.
