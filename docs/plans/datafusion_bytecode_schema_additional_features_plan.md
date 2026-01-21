# DataFusion Bytecode Schema Additional Features Plan (Design-Phase)

## Goals
- Replace bespoke bytecode attr extraction with DataFusion-native map utilities.
- Improve robustness of schema metadata propagation and drift detection.
- Increase performance for large bytecode datasets via scan and listing options.
- Tighten integration between bytecode outputs and DataFusion schema/introspection.

## Non-goals
- Changing the bytecode core schema shape or ABI fields already stabilized.
- Introducing new storage formats beyond Parquet/Delta.
- Rewriting extraction logic outside DataFusion view/registration surfaces.

## Scope 1: Attr extraction via `map_extract` + `unnest`
Status: Completed

### Target file list
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```sql
-- query_fragments.py
WITH base AS (SELECT * FROM py_bc_cfg_edges)
SELECT
  base.file_id,
  base.path,
  base.code_id AS code_unit_id,
  list_extract(map_extract(base.attrs, 'jump_kind'), 1) AS jump_kind,
  list_extract(map_extract(base.attrs, 'jump_label'), 1) AS jump_label
FROM base;
```

### Implementation checklist
- [x] Replace `_map_value`/`_map_cast` usage for bytecode attrs with `map_extract`.
- [x] Add `list_extract(map_extract(...), 1)` helpers for scalarizing optional values.
- [x] Keep `map_entries` views for full key/value inspection.

---

## Scope 2: Attr key coverage diagnostics (`map_keys` / `map_values`)
Status: Partially Complete

### Target file list
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```sql
-- query_fragments.py
WITH base AS (SELECT * FROM py_bc_instructions)
SELECT
  base.file_id,
  base.path,
  base.code_id AS code_unit_id,
  unnest(map_keys(base.attrs)) AS attr_key
FROM base;
```

### Implementation checklist
- [x] Add `py_bc_instruction_attr_keys` view for key coverage.
- [x] Add `py_bc_instruction_attr_values` view for value sampling.
- [ ] Validate view schemas with `arrow_typeof` in `validate_bytecode_views`.

---

## Scope 3: Full `arrow_metadata` capture for ABI diagnostics
Status: Completed

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/query_fragments.py`

### Code pattern
```sql
-- query_fragments.py
SELECT
  arrow_metadata(code_objects.instructions) AS instr_meta,
  arrow_metadata(code_objects.line_table) AS line_meta
FROM bytecode_files_v1
LIMIT 1;
```

### Implementation checklist
- [x] Add a lightweight diagnostic view or artifact with full metadata maps.
- [x] Record metadata payloads alongside schema snapshots.
- [x] Gate diagnostics on `enable_information_schema`.

---

## Scope 4: Function signature gating via `information_schema.parameters`
Status: Completed

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```sql
-- runtime.py (diagnostics)
SELECT routine_name, parameter_name, data_type
FROM information_schema.parameters
WHERE routine_name IN ('arrow_cast', 'map_entries', 'map_extract');
```

### Implementation checklist
- [x] Extend function catalog snapshot to include parameter metadata.
- [x] Validate required signatures before registering bytecode views.
- [x] Emit a diagnostics artifact when signatures deviate.

---

## Scope 5: Explicit schema contracts on external bytecode registration
Status: Completed

### Target file list
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
# runtime.py
location = DatasetLocation(
    path=...,
    format="parquet",
    datafusion_scan=DataFusionScanOptions(
        schema_force_view_types=True,
    ),
)
register_dataset_df(ctx, name="bytecode_files_v1", location=location, runtime_profile=self)
```

### Implementation checklist
- [x] Register external bytecode tables with explicit schema where supported.
- [x] Enable `schema_force_view_types` for stable view typing.
- [x] Keep fingerprint validation as a hard gate for mismatches.

---

## Scope 6: Preserve Arrow metadata in scans (`skip_arrow_metadata`)
Status: Completed

### Target file list
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
# runtime.py
DataFusionScanOptions(
    skip_arrow_metadata=False,
)
```

### Implementation checklist
- [x] Default bytecode external scan options to `skip_arrow_metadata=False`.
- [x] Validate metadata visibility via `arrow_metadata` queries.
- [x] Ensure `skip_metadata` remains enabled to avoid schema drift across files.

---

## Scope 7: Listing/partition options for dynamic adaptation
Status: Completed

### Target file list
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
DataFusionScanOptions(
    listing_table_factory_infer_partitions=True,
    listing_table_ignore_subdirectory=False,
    partition_cols=(
        ("repo", pa.string()),
    ),
)
```

### Implementation checklist
- [x] Enable partition inference for bytecode datasets when using listing tables.
- [x] Add partition columns to schema contracts when present.
- [x] Use ordering + partition hints for improved pruning.

---

## Scope 8: Scan performance knobs for large bytecode datasets
Status: Completed

### Target file list
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
DataFusionScanOptions(
    collect_statistics=False,
    meta_fetch_concurrency=64,
    list_files_cache_ttl="5m",
    list_files_cache_limit="10000",
)
```

### Implementation checklist
- [x] Add bytecode profile defaults for scan concurrency and cache settings.
- [x] Expose settings in runtime telemetry payload.
- [x] Ensure settings are applied before registration.

---

## Scope 9: Optional span/line “wide” views via `unnest(struct)`
Status: Partially Complete

### Target file list
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```sql
WITH base AS (SELECT * FROM py_bc_instruction_spans)
SELECT
  base.file_id,
  base.code_unit_id,
  unnest(base.span)
FROM base;
```

### Implementation checklist
- [x] Add a derived view that expands `span` structs to columns.
- [ ] Use placeholder access (`__unnest_placeholder(...)`) to select fields.
- [x] Validate view schemas alongside existing bytecode views.

---

## Deliverables
- Bytecode attr extraction via `map_extract` with robust optional semantics.
- Schema metadata diagnostics that capture full Arrow metadata maps.
- Stronger function signature gating for DataFusion features.
- Explicit schema contracts and metadata preservation for external bytecode tables.
- Improved listing/partitioning and scan performance defaults for large datasets.
- Optional “wide” views for span/line analysis via `unnest(struct)`.
