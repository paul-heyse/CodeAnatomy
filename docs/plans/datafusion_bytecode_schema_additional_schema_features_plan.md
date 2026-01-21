# DataFusion Bytecode Schema Additional Schema Features Plan (Design-Phase)

## Goals
- Replace bespoke bytecode schema checks with DataFusion-native DDL and constraint surfaces.
- Improve robustness and dynamic adaptation using catalog/provider hooks.
- Increase performance with DataFusion-managed materialization and partitioning semantics.
- Tighten integration between bytecode outputs and DataFusion information_schema.

## Non-goals
- Changing the core bytecode ABI field set or nested schema shapes.
- Introducing new storage formats beyond Parquet/Delta.
- Refactoring non-bytecode extractors in this plan.

---

## Scope 1: DDL nullability and constraints in external table DDL
Status: In progress

### Target file list
- `src/schema_spec/specs.py`
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`

### Code pattern
```python
# specs.py (DDL columns honoring nullability + constraints)
def to_sqlglot_column_defs(self, *, dialect: str | None = None) -> list[exp.ColumnDef]:
    dialect_name = dialect or "datafusion"
    return [
        _column_def(
            name=field.name,
            dtype=_arrow_dtype_to_ibis(field.dtype),
            nullable=field.nullable,
        )
        for field in self.fields
    ]
```

### Implementation checklist
- [x] Extend SQLGlot ColumnDef generation to include NOT NULL when field nullable=False.
- [x] Emit NOT NULL for key/required fields in DDL where supported.
- [ ] Emit primary key/constraint metadata in DDL where supported.
- [x] Update registry DDL fingerprints to reflect nullability changes.

---

## Scope 2: TableProvider constraints surface for bytecode tables
Status: In progress

### Target file list
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/catalog_provider.py`
- `src/schema_spec/system.py`

### Code pattern
```python
# registry_bridge.py
provider = BytecodeTableProvider(schema=expected_schema, constraints=constraints)
ctx.register_table("bytecode_files_v1", provider)
```

### Implementation checklist
- [x] Implement a TableProvider wrapper that returns constraints().
- [x] Map TableSchemaSpec key fields into DataFusion constraints.
- [ ] Map required_non_null fields into DataFusion constraints where supported.
- [ ] Validate constraints are visible via information_schema.table_constraints.

---

## Scope 3: DataFusion materialization via COPY/INSERT for bytecode outputs
Status: Planned

### Target file list
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/extract/bytecode_extract.py`

### Code pattern
```python
# bridge.py
copy_to_path(
    ctx,
    sql="SELECT * FROM py_bc_instructions",
    path=output_path,
    options=CopyToOptions(allow_file_output=True),
)
```

### Implementation checklist
- [ ] Add a bytecode materialization path that uses COPY for Parquet.
- [ ] Add Delta INSERT path when Delta provider supports inserts.
- [ ] Align outputs to registry DDL/constraints before writes.

---

## Scope 4: CatalogProvider-driven dynamic dataset discovery
Status: In progress

### Target file list
- `src/datafusion_engine/catalog_provider.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
# catalog_provider.py
schema.register_table(
    "bytecode_files_v1",
    LazyExternalTable(location=location, schema=expected_schema),
)
```

### Implementation checklist
- [x] Add lazy table loading that fetches metadata on first access.
- [ ] Enable dynamic partition discovery for bytecode datasets.
- [ ] Record catalog discovery events in diagnostics.

---

## Scope 5: SQL-first nested access and map construction
Status: In progress

### Target file list
- `src/datafusion_engine/query_fragments.py`
- `src/extract/bytecode_extract.py`

### Code pattern
```sql
SELECT
  get_field(base.span, 'start', 'line0') AS pos_start_line,
  map(['key'], ['value']) AS attrs
FROM py_bc_instructions base;
```

### Implementation checklist
- [x] Switch bytecode instruction span projections to get_field(...).
- [ ] Migrate remaining bytecode nested access to get_field(...) where applicable.
- [ ] Build attrs maps via SQL map/make_map where possible.
- [ ] Remove bespoke attrs shaping once SQL paths are canonical.

---

## Scope 6: Union-typed attrs for type fidelity
Status: In progress

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/extract/bytecode_extract.py`
- `src/datafusion_engine/query_fragments.py`

### Code pattern
```sql
SELECT
  union_tag(attrs) AS attr_kind,
  union_extract(attrs, 'int_value') AS attr_int
FROM py_bc_instruction_attrs;
```

### Implementation checklist
- [x] Introduce a union-typed attr value for bytecode attrs.
- [ ] Convert extract output attrs maps into union-typed values (post-kernel).
- [ ] Emit union_tag/union_extract views for typed inspection across all bytecode attrs.
- [ ] Preserve legacy string rendering only for diagnostics.

---

## Scope 7: DDL partitioning and ordering contracts for bytecode tables
Status: Completed

### Target file list
- `src/schema_spec/specs.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`

### Code pattern
```python
ExternalTableConfig(
    location=path,
    file_format="parquet",
    partitioned_by=("repo",),
    file_sort_order=("path", "file_id"),
)
```

### Implementation checklist
- [x] Emit PARTITIONED BY in DDL for bytecode tables when configured.
- [x] Ensure ordering contracts are validated against file ordering metadata.
- [x] Feed partition hints into scan defaults for pruning.

---

## Scope 8: Delta provider schema contract and INSERT support
Status: Planned

### Target file list
- `src/datafusion_engine/registry_bridge.py`
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
# registry_bridge.py
delta_scan = DeltaScanOptions(schema=expected_schema)
register_dataset_df(ctx, name="bytecode_files_v1", location=location)
```

### Implementation checklist
- [ ] Compare delta-log schema JSON vs provider schema vs expected schema.
- [ ] Prefer DeltaTableProvider insert_into when available.
- [x] Capture delta/provider/expected schema fingerprints in diagnostics artifacts.
- [ ] Record schema contract mismatches as diagnostics artifacts.
