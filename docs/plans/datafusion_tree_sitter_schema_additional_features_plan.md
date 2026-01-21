# DataFusion Tree-Sitter Schema Additional Features Plan (Design-Phase)

## Goals
- Align tree-sitter extraction with DataFusion schema primitives (TableSchema, constraints, ordering).
- Replace bespoke validation with information_schema and arrow_typeof-based checks.
- Improve robustness and dynamic adaptation via schema adapters and scan-boundary enforcement.
- Strengthen integration with runtime diagnostics and DataFusion-native write paths.

## Non-goals
- Changing the tree-sitter extraction payload shapes or semantics.
- Introducing new storage formats beyond Parquet/Delta for tree-sitter outputs.
- Refactoring non tree-sitter extractors in this plan.

---

## Scope 1: External dataset registration + TableSchema alignment
Status: Planned

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/ibis_engine/registry.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```python
# runtime.py (tree-sitter dataset location)
expected_schema = schema_for("tree_sitter_files_v1")
scan = DataFusionScanOptions(
    partition_cols=self.tree_sitter_external_partition_cols,
    file_sort_order=self.tree_sitter_external_ordering,
    schema_force_view_types=self.tree_sitter_external_schema_force_view_types,
    skip_arrow_metadata=self.tree_sitter_external_skip_arrow_metadata,
)
return DatasetLocation(
    path=self.tree_sitter_external_location,
    format=self.tree_sitter_external_format,
    datafusion_provider=self.tree_sitter_external_provider,
    datafusion_scan=scan,
    table_spec=table_spec_from_schema("tree_sitter_files_v1", expected_schema),
)
```

### Implementation checklist
- [ ] Add tree-sitter dataset location fields to `DataFusionRuntimeProfile`.
- [ ] Register `tree_sitter_files_v1` via `register_dataset_df` when configured.
- [ ] Enforce schema fingerprint match on registration.
- [ ] Surface table definition and constraints in diagnostics.

---

## Scope 2: Ordering + partition defaults for tree-sitter scans
Status: Planned

### Target file list
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/schema_registry.py`
- `src/schema_spec/specs.py`

### Code pattern
```python
# registry_bridge.py (default scan options)
if name == _TREE_SITTER_EXTERNAL_TABLE_NAME:
    partition_fields = _TREE_SITTER_PARTITION_FIELDS
    file_sort_order = _TREE_SITTER_FILE_SORT_ORDER
    infer_partitions = True
    cache_ttl = "2m"
```

### Implementation checklist
- [ ] Add tree-sitter dataset defaults in `_default_scan_options_for_dataset`.
- [ ] Enforce ordering contract for tree-sitter when explicit ordering metadata exists.
- [ ] Emit PARTITIONED BY and WITH ORDER in DDL when configured.

---

## Scope 3: Constraints + nullability alignment
Status: Planned

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/schema_spec/specs.py`
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/listing_table_provider.py`

### Code pattern
```python
# specs.py (DDL constraints)
if field.name in required or not field.nullable:
    constraints.append(exp.ColumnConstraint(kind=exp.NotNullColumnConstraint()))
```

### Implementation checklist
- [ ] Make `path` and `file_id` non-nullable in `TREE_SITTER_FILES_SCHEMA`.
- [ ] Emit NOT NULL constraints for required and key fields in DDL.
- [ ] Map key fields into DataFusion `Constraints::PrimaryKey`.
- [ ] Add required-non-null constraints where supported.

---

## Scope 4: Schema adapters at the scan boundary
Status: Planned

### Target file list
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `rust/datafusion_ext/src/lib.rs`

### Code pattern
```python
# registry_bridge.py (adapter factory hook)
expr_adapter_factory = _resolve_expr_adapter_factory(
    scan,
    runtime_profile=context.runtime_profile,
    dataset_name=context.name,
)
listing_provider = parquet_listing_table_provider(
    ...,
    expr_adapter_factory=expr_adapter_factory,
)
```

### Implementation checklist
- [ ] Add a tree-sitter adapter factory entry to `schema_adapter_factories`.
- [ ] Pass adapter factories into listing table registration.
- [ ] Ensure drift handling (additive fields, safe casts) happens at scan time.

---

## Scope 5: Span metadata + validation for tree-sitter views
Status: Planned

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```sql
-- query_fragments.py
SELECT
  arrow_metadata(nodes.span, 'line_base') AS nodes_line_base,
  arrow_metadata(nodes.span, 'col_unit') AS nodes_col_unit,
  arrow_metadata(nodes.span, 'end_exclusive') AS nodes_end_exclusive
FROM tree_sitter_files_v1
LIMIT 1;
```

### Implementation checklist
- [ ] Add span metadata for tree-sitter spans (line_base=0, col_unit=byte).
- [ ] Add a `ts_span_metadata` view for inspection.
- [ ] Validate metadata values in `validate_ts_views`.
- [ ] Record span metadata diagnostics in runtime.

---

## Scope 6: information_schema validation + required functions
Status: Planned

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
# schema_registry.py
rows = ctx.sql("SELECT * FROM information_schema.columns WHERE table_name = 'ts_nodes'").collect()
missing = sorted(set(expected_columns) - actual_columns)
```

### Implementation checklist
- [ ] Validate tree-sitter view columns against information_schema.
- [ ] Validate table constraints via `information_schema.table_constraints`.
- [ ] Add `TREE_SITTER_REQUIRED_FUNCTIONS` and signature checks.

---

## Scope 7: SQL nested access standardization + attrs views
Status: Planned

### Target file list
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```sql
SELECT
  get_field(base.span, 'start', 'line0') AS start_line,
  get_field(base.span, 'byte_span', 'byte_start') AS start_byte
FROM ts_nodes base;
```

### Implementation checklist
- [ ] Replace bracket chains with `get_field` helpers for all tree-sitter views.
- [ ] Add `ts_*_attrs` views using `map_entries` for attrs inspection.
- [ ] Add attrs key/value diagnostics for missing or malformed attrs.

---

## Scope 8: DataFusion-native write path for tree-sitter outputs
Status: Planned

### Target file list
- `src/engine/materialize.py`
- `src/extract/helpers.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
# materialize.py
df = datafusion_from_arrow(ctx, name=temp_name, value=table)
payload = datafusion_write_parquet(df, path=path, policy=policy)
```

### Implementation checklist
- [ ] Add a tree-sitter write policy based on runtime partition/sort hints.
- [ ] Implement Parquet and Delta write paths (COPY/INSERT or DataFrame writes).
- [ ] Record tree-sitter output write diagnostics.

---

## Scope 9: Diagnostics + configuration reflection
Status: Planned

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```python
# runtime.py
payload["df_settings"] = ctx.sql("SELECT * FROM information_schema.df_settings").to_arrow_table()
```

### Implementation checklist
- [ ] Record tree-sitter registration details and schema fingerprints.
- [ ] Capture `information_schema.df_settings` for schema-affecting config.
- [ ] Surface ordering/partition expectations in diagnostics payloads.

---

## Scope 10: Catalog-driven discovery for tree-sitter datasets
Status: Planned

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/catalog_provider.py`
- `src/ibis_engine/registry.py`

### Code pattern
```python
# runtime.py
register_registry_catalogs(
    ctx,
    catalogs=self.registry_catalogs,
    catalog_name=self.registry_catalog_name or self.default_catalog,
)
```

### Implementation checklist
- [ ] Add tree-sitter catalog location/format fields to runtime profile.
- [ ] Allow registry-backed catalogs to resolve tree-sitter datasets lazily.
- [ ] Record catalog discovery events for tree-sitter datasets.
