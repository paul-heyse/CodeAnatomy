# DataFusion Built-in/UDF Opportunity Plan

## Goal
Move remaining Python/PyArrow compute into DataFusion built-ins or existing UDFs
for deterministic, pushdown-friendly execution. Focus on ID generation, stats
aggregation, and interval prep that can run inside DataFusion.

## Non-Goals
- Changing hash algorithms or ID formats.
- Introducing dynamic URL tables or untrusted SQL surfaces.
- Removing non-DataFusion fallbacks where the runtime lacks built-ins.

---

## Scope 1: Move CST/SCIP/Bytecode ID generation into DataFusion views
**Why**: IDs are deterministic and already backed by DataFusion UDFs (`stable_id`,
`prefixed_hash64`). Computing IDs inside views removes Python hashing and aligns
with DataFusion-first compute.

**Representative code patterns**
```sql
-- Example: stable_id in DataFusion SQL
stable_id('cst_def', concat_ws(':', file_id, kind, def_bstart, def_bend)) AS def_id

-- Example: prefixed_hash64 with concat_ws
prefixed_hash64('scip_doc', concat_ws(':', path)) AS document_id
```

```python
# Register view-based ID computation
ctx.sql("""
CREATE OR REPLACE VIEW cst_defs_ids AS
SELECT
  *,
  stable_id('cst_def', concat_ws(':', file_id, kind, def_bstart, def_bend)) AS def_id
FROM cst_defs_norm
""").collect()
```

**Target files**
- `src/extract/cst_extract.py`
- `src/extract/scip_extract.py`
- `src/extract/bytecode_extract.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`

**Implementation checklist**
- [ ] Add DataFusion view fragments that compute CST/SCIP/bytecode IDs.
- [ ] Remove Python `stable_id`/`prefixed_hash_id` calls in extractors.
- [ ] Update schema registry required functions for the new views.
- [ ] Update any CPG build inputs to use the view-based ID datasets.
- [ ] Extend validation to assert required functions/signatures are present.

---

## Scope 2: Replace PyArrow `value_counts` with DataFusion aggregates
**Why**: Stats queries should be executed in DataFusion when the source is a
DataFusion view/table, avoiding Python compute and keeping diagnostics consistent.

**Representative code patterns**
```sql
SELECT position_encoding, COUNT(*) AS doc_count
FROM scip_documents
GROUP BY position_encoding
```

```python
# DataFusion aggregate in Python
df = ctx.table("scip_documents")
counts = df.aggregate(
    group_by=["position_encoding"],
    aggs=[f.count(col("position_encoding")).alias("doc_count")],
)
```

**Target files**
- `src/hamilton_pipeline/modules/normalization.py`
- `src/arrowdsl/finalize/finalize.py`
- `src/datafusion_engine/schema_registry.py` (function inventories/gates)

**Implementation checklist**
- [ ] Replace `pc.call_function("value_counts", ...)` with SQL/DF aggregation.
- [ ] Add `information_schema`-based gating for the aggregate functions used.
- [ ] Update diagnostics payloads to use DataFusion-derived stats when available.
- [ ] Extend tests to validate the aggregate paths.

---

## Scope 3: Interval alignment prep via DataFusion expressions
**Why**: Interval alignment currently uses PyArrow casts and Python-generated
row indices. Use DataFusion expressions (`cast`, `row_number`) to keep prep in
the query engine and allow optimizer visibility.

**Representative code patterns**
```sql
SELECT
  *,
  CAST(path AS STRING) AS __left_path_key,
  row_number() OVER () - 1 AS __left_id
FROM left_table
```

```python
df = ctx.table("left_table")
df = df.select(
    "*",
    col("path").cast("string").alias("__left_path_key"),
    (f.row_number().over(window=None) - f.lit(1)).alias("__left_id"),
)
```

**Target files**
- `src/datafusion_engine/kernels.py`
- `src/datafusion_engine/schema_registry.py` (function inventories/gates)

**Implementation checklist**
- [ ] Replace PyArrow casts (`pc.cast`) with DataFusion `cast`.
- [ ] Replace Python range-based IDs with `row_number()` window expr.
- [ ] Add required function signatures for window functions if needed.
- [ ] Validate interval alignment outputs match existing semantics.

---

## Scope 4: Align schema evolution compatibility with DataFusion casting
**Why**: Schema evolution checks currently use PyArrow casting rules. Validate
cast compatibility via DataFusion’s `arrow_cast` to align with engine behavior.

**Representative code patterns**
```sql
SELECT arrow_cast(CAST(NULL AS INT64), 'Utf8') AS cast_check
```

```python
ctx.sql("SELECT arrow_cast(CAST(NULL AS INT64), 'Utf8')").collect()
```

**Target files**
- `src/schema_spec/system.py`
- `src/datafusion_engine/runtime.py` (context/gating)
- `src/datafusion_engine/schema_registry.py` (function inventories)

**Implementation checklist**
- [ ] Add DataFusion cast probe path for schema evolution checks.
- [ ] Keep PyArrow fallback for non-DataFusion contexts.
- [ ] Add required function inventory for `arrow_cast` if not already enforced.

---

## Scope 5: Use built-in string join in Ibis hash helpers
**Why**: Hash join helpers should compile to `concat_ws` to reduce expression
depth and leverage DataFusion built-ins.

**Representative code patterns**
```python
joined = ops.StringJoin(arg=tuple(parts), sep=sep)
return joined.to_expr()
```

```sql
concat_ws(':', part1, part2, part3)
```

**Target files**
- `src/ibis_engine/ids.py`
- `src/ibis_engine/expr_compiler.py`

**Implementation checklist**
- [ ] Restore `StringJoin` usage for string joining in hash helpers.
- [ ] Ensure `concat_ws` is in required function inventories where used.
- [ ] Validate Ibis expressions compile to `concat_ws` for DataFusion backends.

---

## Scope 6: Function inventory + gating upgrades
**Why**: Some built-ins show up only in `information_schema.routines` (not
`SHOW FUNCTIONS`). Use the richer catalog for gating and keep inventories in
sync with view requirements.

**Representative code patterns**
```python
rows = ctx.sql("SELECT routine_name FROM information_schema.routines").to_arrow_table()
names = {row["routine_name"].lower() for row in rows.to_pylist()}
if "array_agg" not in names:
    raise ValueError("Missing required function: array_agg")
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_registry.py`
- `tests/unit/test_datafusion_schema_registry.py`

**Implementation checklist**
- [ ] Add helper to query `information_schema.routines` for gating.
- [ ] Update gating to use `information_schema` for list/array/map built-ins.
- [ ] Add validation tests for symtable/bytecode/interval prep built-ins.

---

## Dependencies / Ordering
1. Scope 1 (IDs in views) before any downstream consumers that expect IDs.
2. Scope 2 (stats) before diagnostics improvements.
3. Scope 3 (interval prep) before interval alignment changes.
4. Scope 6 (gating) after new built-in usages are defined.
