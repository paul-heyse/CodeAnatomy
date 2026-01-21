# DataFusion Bytecode Schema Feature Plan

## Scope 1: Schema metadata as ABI (`arrow_metadata`)
Status: Planned

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
# schema_registry.py
BYTECODE_INSTR_T = pa.struct(
    [
        ("instr_index", pa.int32()),
        ("offset", pa.int32()),
        ("span", SPAN_T),
        ("attrs", ATTRS_T),
    ]
)

SPAN_META = {
    b"line_base": b"0",
    b"col_unit": b"utf32",
    b"end_exclusive": b"true",
}
BYTECODE_FILES_SCHEMA = BYTECODE_FILES_SCHEMA.with_metadata(
    {**(BYTECODE_FILES_SCHEMA.metadata or {}), **SPAN_META}
)
```

```sql
-- query_fragments.py (SQL fragment)
SELECT
  arrow_metadata(code_objects, 'line_base') AS line_base,
  arrow_metadata(code_objects, 'col_unit') AS col_unit
FROM bytecode_files_v1
LIMIT 1;
```

### Implementation checklist
- [ ] Add per-field and per-schema metadata for bytecode ABI constants (line base, col unit).
- [ ] Use `arrow_metadata` in bytecode views where constants are currently projected per row.
- [ ] Record DataFusion version metadata to trace schema expectations.

---

## Scope 2: Arrow‑precise casting in bytecode views (`arrow_cast`)
Status: Planned

### Target file list
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```sql
-- query_fragments.py
SELECT
  arrow_cast(base.arg, 'Int32') AS arg,
  arrow_cast(base.oparg, 'Int32') AS oparg,
  arrow_cast(base.argval_int, 'Int64') AS argval_int
FROM base;
```

### Implementation checklist
- [ ] Replace selected `CAST(...)` in bytecode views with `arrow_cast` for exact Arrow types.
- [ ] Add `_ARROW_CAST_TYPES` entries for common bytecode fields (Int32/Int64/Utf8).
- [ ] Validate with `arrow_typeof` in `validate_bytecode_views`.

---

## Scope 3: Struct construction in views (`named_struct`, `struct`)
Status: Planned

### Target file list
- `src/datafusion_engine/query_fragments.py`

### Code pattern
```sql
-- query_fragments.py
SELECT
  named_struct(
    'start', named_struct('line0', base.pos_start_line, 'col', base.pos_start_col),
    'end', named_struct('line0', base.pos_end_line, 'col', base.pos_end_col),
    'col_unit', base.col_unit,
    'end_exclusive', base.end_exclusive
  ) AS span
FROM base;
```

### Implementation checklist
- [ ] Construct `span` (or `flags_detail`) in SQL rather than Python for derived views.
- [ ] Ensure nested structs align with `SPAN_T` and bytecode schema types.
- [ ] Keep Python emission minimal (raw primitives only) where SQL shaping exists.

---

## Scope 4: Map utilities for `attrs` (`map_entries`, `map_extract`)
Status: Planned

### Target file list
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```sql
-- query_fragments.py
WITH base AS (SELECT * FROM py_bc_instructions)
SELECT
  base.file_id,
  base.code_id,
  entry['key'] AS attr_key,
  entry['value'] AS attr_value
FROM base,
  unnest(map_entries(base.attrs)) AS entry;
```

### Implementation checklist
- [ ] Add `py_bc_instruction_attrs` view via `map_entries` + `unnest`.
- [ ] Add `py_bc_error_attrs` view for error metadata (`error_stage`, `code_id`).
- [ ] Keep `_map_value` for performance-critical projections but prefer map views for ad‑hoc analysis.

---

## Scope 5: Struct expansion (`unnest(struct)`) for schema‑adaptive views
Status: Planned

### Target file list
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```sql
-- query_fragments.py
WITH base AS (SELECT * FROM py_bc_code_units)
SELECT
  base.file_id,
  base.code_id,
  unnest(base.flags_detail) AS flags_detail
FROM base;
```

### Implementation checklist
- [ ] Add a view that flattens `flags_detail` for analytics without manual projections.
- [ ] Use `arrow_typeof` to validate the expanded field types in schema registry.

---

## Scope 6: Function discovery and version gating
Status: Planned

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```python
# runtime.py
def _function_inventory(ctx: SessionContext) -> set[str]:
    table = ctx.sql("SELECT routine_name FROM information_schema.routines").to_arrow_table()
    return {str(name) for name in table["routine_name"].to_pylist() if name is not None}
```

### Implementation checklist
- [ ] Record available function names and DataFusion `version()` in diagnostics.
- [ ] Gate use of `map_entries` / `arrow_cast` with the discovered function set.
- [ ] Add a fallback SQL fragment when functions are missing.

---

## Scope 7: Constraints + nullability contracts
Status: Planned

### Target file list
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```python
# schema_registry.py
BYTECODE_CODE_OBJ_T = pa.struct(
    [
        ("code_id", pa.string().with_nullable(False)),
        ("qualname", pa.string()),
    ]
)
```

### Implementation checklist
- [ ] Mark critical bytecode identity fields as non-nullable in schema.
- [ ] Add constraint metadata (primary keys) where supported.
- [ ] Verify outputs align (no null identity fields).

---

## Scope 8: External table registration + ordering contracts
Status: Planned

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```sql
-- runtime.py (session bootstrap)
CREATE EXTERNAL TABLE bytecode_files_v1
STORED AS PARQUET
LOCATION 's3://.../bytecode/'
WITH ORDER (path ASC, file_id ASC);
```

### Implementation checklist
- [ ] Add optional external table registration for persisted bytecode outputs.
- [ ] Register ordering metadata for query planners when files are sorted.
- [ ] Ensure schema matches `BYTECODE_FILES_SCHEMA` before registration.

---

## Scope 9: Catalog auto‑loading for bytecode outputs
Status: Planned

### Target file list
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
config = config.set("datafusion.catalog.location", location)
config = config.set("datafusion.catalog.format", "parquet")
```

### Implementation checklist
- [ ] Add runtime wiring for `datafusion.catalog.location` and `datafusion.catalog.format`.
- [ ] Provide a bytecode‑specific profile for auto‑loading persisted extracts.
- [ ] Validate auto‑loaded schema via `information_schema.columns`.

---

## Scope 10: Delta Lake provider integration (optional but high‑value)
Status: Planned

### Target file list
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```python
from deltalake import DeltaTable

table = DeltaTable(location)
ctx.register_table("bytecode_files_v1", table)
```

### Implementation checklist
- [ ] Add a delta registration option for bytecode datasets.
- [ ] Compare registry schema hash vs Delta log schema for enforcement.
- [ ] Record Delta table version in diagnostics.
