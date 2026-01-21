# DataFusion Built-in + UDF Adoption Plan

## Goal
Replace bespoke Python/PyArrow compute with DataFusion built-ins wherever possible and
standardize remaining compute via DataFusion UDFs. This aligns with the project’s
DataFusion-first compute policy and reduces Python-side post-processing.

## Non-Goals
- Switching hash algorithms (e.g., replacing stable hash UDFs with `sha256`) unless explicitly
  approved due to ID compatibility impact.
- Introducing new URL-table or dynamic catalog features.

## Execution Scopes

### Scope 1: Replace simple UDFs with built-ins
**Why**: Several current scalar UDFs are thin wrappers around built-in SQL/Expr behavior.
Moving them to built-ins removes UDF overhead and improves optimizer visibility.

**Representative code pattern**
```sql
-- normalize_span (trim + regex + cast)
CASE
  WHEN regexp_like(trim(value), '^-?\d+(\.\d+)?([eE][+-]?\d+)?$')
  THEN CAST(trim(value) AS BIGINT)
  ELSE NULL
END AS normalized_span

-- cpg_score (identity cast)
CAST(value AS DOUBLE) AS cpg_score

-- position_encoding_norm (case/contains)
CASE
  WHEN upper(value) LIKE '%UTF8%' THEN 1
  WHEN upper(value) LIKE '%UTF16%' THEN 2
  WHEN upper(value) LIKE '%UTF32%' THEN 3
  ELSE 3
END AS position_encoding
```

**Target files**
- `src/datafusion_engine/udf_registry.py`
- `src/datafusion_engine/kernels.py`
- `src/ibis_engine/builtin_udfs.py`
- `src/normalize/ibis_plan_builders.py`
- `src/normalize/ibis_spans.py`

**Implementation checklist**
- [x] Replace `normalize_span` UDF usage with built-in SQL/Expr expressions.
- [x] Replace `cpg_score` UDF usage with `CAST`.
- [x] Replace `position_encoding_norm` UDF usage with built-in case expressions.
- [x] Remove `valid_mask` UDF (no remaining call sites; built-in replacement not needed).
- [x] Remove superseded UDF registrations and update any rewrite tags.

---

### Scope 2: Use built-in list/array/struct functions in normalization pipelines
**Why**: Several normalization flows still run Python/PyArrow loops for list/array
operations. DataFusion provides built-ins that keep compute inside the engine.

**Representative code pattern**
```sql
-- Replace distinct_sorted(list_flatten(...))
array_sort(array_distinct(array_flatten(col))) AS distinct_sorted

-- Flatten list<struct> field
array_flatten(get_field(col, 'name')) AS flattened_names
```

**Target files**
- `src/hamilton_pipeline/modules/normalization.py`
- `src/arrowdsl/finalize/finalize.py`
- `src/incremental/exports.py`
- `src/datafusion_engine/compute_ops.py`

**Implementation checklist**
- [x] Replace Python set/sort paths in normalize qname dim with DataFusion/Ibis list ops.
- [x] Replace list flatten + field extraction loops in incremental exports with SQL `unnest`
      + `get_field`.
- [x] Replace remaining PyArrow list ops in `arrowdsl/finalize/finalize.py` with
      DataFusion/Ibis aggregation (`array_agg` + `named_struct`) or list-array primitives.
- [x] Keep `compute_ops` list helpers only for non-DataFusion backends after finalize migration.
- [x] Add function-availability checks for list/array built-ins via `information_schema`.

---

### Scope 3: Hash/ID generation in DataFusion expressions
**Why**: IDs and hashes computed in Python can be moved into DataFusion expressions for
consistency and pushdown. Use existing UDFs and built-in string functions.

**Representative code pattern**
```sql
-- Symtable symbol IDs
prefixed_hash64('sym_symbol', concat_ws(':', scope_id, symbol_name)) AS sym_symbol_id

-- Stable IDs for edges
stable_id('edge', concat_ws(':', edge_kind, src, dst)) AS edge_id
```

**Target files**
- `src/extract/symtable_extract.py`
- `src/datafusion_engine/query_fragments.py`
- `src/ibis_engine/ids.py`
- `src/cpg/symtable_sql.py`

**Implementation checklist**
- [x] Remove Python hashing in extract paths and compute IDs via DataFusion UDFs.
- [x] Standardize string joining via `concat_ws`.
- [x] Ensure IDs are computed at scan or view boundaries, not post-materialization.

---

### Scope 4: Span normalization via DataFusion UDFs + built-ins
**Why**: Span normalization currently relies on Python loops in some code paths. Use UDFs
(`col_to_byte`, `position_encoding_norm`) and built-ins to keep computation in DataFusion.

**Representative code pattern**
```python
# Ibis/DataFusion expression shape
posenc = position_encoding_norm(col("position_encoding").cast("string"))
col_unit = coalesce(col("col_unit"), lit("utf32"))
bstart = col_to_byte(col("line_text"), col("col"), col_unit)
```

**Target files**
- `src/normalize/spans.py`
- `src/normalize/ibis_spans.py`
- `src/normalize/ibis_plan_builders.py`
- `src/datafusion_engine/udf_registry.py`

**Implementation checklist**
- [x] Convert Python span loops to DataFusion expressions where DataFusion backend is required.
- [x] Use `col_to_byte` and built-in string ops for byte conversion.
- [x] Remove legacy Python-only span normalization paths or gate them behind non-DF backends.

---

### Scope 5: UDF consolidation + function availability gating
**Why**: The engine should prefer built-ins, use UDFs only when required, and validate
function availability at runtime.

**Representative code pattern**
```python
rows = ctx.sql("SHOW FUNCTIONS").to_arrow_table().to_pylist()
available = {row["function_name"] for row in rows}
if "regexp_like" not in available:
    raise ValueError("Missing DataFusion built-in: regexp_like")
```

**Target files**
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/udf_registry.py`
- `tests/unit/test_datafusion_schema_registry.py`

**Implementation checklist**
- [x] Maintain required built-in + UDF inventories per pipeline stage (AST/CST/TS/bytecode).
- [x] Add validation tests for required CST built-ins and UDF registrations.
- [x] Gate optional AST views on `SHOW FUNCTIONS` and DataFusion version.
- [x] Extend function inventories to symtable-specific built-ins (concat/hash/unnest) and
      enforce via validation.
- [x] Add function-availability checks for list/array built-ins used by finalize pipelines.

---

## Decommission Targets (post-migration cleanup)
Remove these functions/modules once the conversions land and all call sites migrate:

**Functions to delete**
- `src/datafusion_engine/udf_registry.py`
  - `_normalize_span`, `_cpg_score`, `_position_encoding_norm`, `_valid_mask`
  - associated `DataFusionUdfSpec` entries and UDF registrations in `_SCALAR_UDF_SPECS`
- `src/ibis_engine/builtin_udfs.py`
  - `cpg_score`, `position_encoding_norm`, `valid_mask` (built-in expr replacements)
- `src/normalize/spans.py`
  - `normalize_position_encoding_array` and row-wise span conversion helpers once
    DataFusion expressions (built-ins + UDFs) fully replace Python loops
- `src/datafusion_engine/compute_ops.py`
  - `distinct_sorted` and any list/array helpers replaced by DataFusion list/array
    built-ins (e.g., `array_distinct`, `array_sort`, `array_flatten`)

**Progress**
- `udf_registry` removals completed for the listed UDFs.
- `normalize/spans.py` removed; span normalization is DataFusion/Ibis-first.
- `compute_ops.flatten_list_struct_field` removed and list helpers migrated off finalize.
- `arrowdsl/finalize/finalize.py` now aggregates error detail lists via DataFusion
  (`array_agg` + `named_struct`) when available, with list-array fallbacks.
- `compute_ops.list_flatten` and `compute_ops.list_value_length` removed after finalize
  migration.

**Modules that can be removed entirely (conditional)**
- None immediately; these modules remain as fallbacks for non-DataFusion backends.
  Once all execution paths are DataFusion-only, consider removing the legacy span
  normalization module (`src/normalize/spans.py`) and any unused compute helpers.

---

## Dependencies / Ordering
1) Built-in replacements (Scope 1) should land before UDF cleanup (Scope 5).
2) List/array built-in migrations (Scope 2) should precede span/UDF refactors (Scope 4).
3) ID hashing changes (Scope 3) should land before downstream consumers relying on IDs.

## Notes
- Verify built-in availability for list/array functions (`array_*`/`list_*`) in the
  DataFusion build used by the runtime; fallback to UDFs where absent.
- Stable hashing functions are UDF-backed today; switching to `sha*` built-ins is a
  breaking change and requires explicit approval.
