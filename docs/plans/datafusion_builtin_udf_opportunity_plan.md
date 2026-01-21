# DataFusion Built-in/UDF Opportunity Plan

## Goal
Move remaining Python/PyArrow compute into DataFusion built-ins or existing UDFs
for deterministic, pushdown-friendly execution. Focus on ID generation, stats
aggregation, and interval prep that can run inside DataFusion.

## Non-Goals
- Changing hash algorithms or ID formats.
- Introducing dynamic URL tables or untrusted SQL surfaces.
- Removing fallbacks when DataFusion lacks equivalent semantics (documented exceptions only).

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
- [ ] Extend inventories to cover kernel/incremental/param-signature functions
      (`row_number`, `array_agg`, `array_sort`, `array_distinct`, `array_to_string`,
      `case`, `coalesce`, `bool_or`, `sha256`).

---

## Scope 7: DataFusion-native incremental diffs + change sets
**Why**: Incremental diff/changes currently use PyArrow compute ops; push this
logic into DataFusion SQL/DF (built-ins only) so diffing is engine-native and
optimizer-visible.

**Representative code patterns**
```sql
SELECT
  file_id,
  coalesce(path_cur, path_prev) AS path,
  CASE
    WHEN path_cur IS NULL THEN 'deleted'
    WHEN path_prev IS NULL THEN 'added'
    WHEN file_sha256_cur = file_sha256_prev AND path_cur <> path_prev THEN 'renamed'
    WHEN file_sha256_cur <> file_sha256_prev THEN 'modified'
    ELSE 'unchanged'
  END AS change_kind,
  path_prev AS prev_path,
  path_cur AS cur_path,
  file_sha256_prev AS prev_file_sha256,
  file_sha256_cur AS cur_file_sha256
FROM diff_join
```

```python
df = ctx.table("diff_join")
change_kind = f.case(lit(True)).when(col("path_cur").is_null(), lit("deleted")).when(
    col("path_prev").is_null(), lit("added")
).when(
    (col("file_sha256_cur") == col("file_sha256_prev"))
    & (col("path_cur") != col("path_prev")),
    lit("renamed"),
).when(
    col("file_sha256_cur") != col("file_sha256_prev"),
    lit("modified"),
).otherwise(lit("unchanged"))
```

**Target files**
- `src/incremental/diff.py`
- `src/incremental/changes.py`
- `src/datafusion_engine/schema_registry.py` (function inventories/gates)

**Implementation checklist**
- [ ] Replace compute-op masks with DataFusion SQL/DF expressions (`case`, `coalesce`).
- [ ] Derive change sets via `SELECT DISTINCT` + `bool_or` aggregates.
- [ ] Remove PyArrow compute fallbacks for diff/change derivation.
- [ ] Add/extend tests to validate diff semantics match current behavior.

---

## Scope 8: DataFusion-native snapshot row hashing + fingerprints
**Why**: Snapshot hashing still runs in Python/PyArrow; move row hashes and
fingerprints into DataFusion using stable hash UDFs + built-ins.

**Representative code patterns**
```sql
SELECT
  stable_hash64(
    concat_ws('\x1f', 'scip_row', file_id, path, symbol, signature)
  ) AS row_hash
FROM scip_snapshot
```

```sql
SELECT
  sha256(
    array_to_string(array_sort(array_distinct(array_agg(row_hash))), '\x1f')
  ) AS fingerprint
FROM scip_snapshot
```

**Target files**
- `src/incremental/scip_snapshot.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py` (function inventories/gates)

**Implementation checklist**
- [ ] Add DataFusion view fragment(s) for snapshot row hashes.
- [ ] Replace `hash64_from_arrays`/Python hashing with `stable_hash64` + `concat_ws`.
- [ ] Move fingerprint aggregation to DataFusion `array_agg` + `sha256`.
- [ ] Ensure function inventories include `array_*`, `sha256`, `stable_hash64`.

---

## Scope 9: DataFusion-native param signatures
**Why**: Param-table signatures use PyArrow `hash`/`sort_indices`. Use DataFusion
aggregates and hashing to keep signatures engine-native.

**Representative code patterns**
```sql
WITH vals AS (
  SELECT array_sort(array_distinct(array_agg(param_value))) AS v
  FROM params_table
)
SELECT sha256(array_to_string(v, '\x1f')) AS signature
FROM vals
```

```python
vals = df.aggregate([], [f.array_agg(col("param_value")).alias("v")])
vals = vals.select(f.array_sort(f.array_distinct(col("v"))).alias("v"))
signature = vals.select(f.sha256(f.array_to_string(col("v"), lit("\x1f"))))
```

**Target files**
- `src/ibis_engine/param_tables.py`
- `src/datafusion_engine/schema_registry.py` (function inventories/gates)
- `tests/unit/test_param_tables.py`

**Implementation checklist**
- [ ] Replace `sort_indices` + PyArrow `hash` with DataFusion `array_*` + `sha256`.
- [ ] Keep signature determinism (sorted + distinct).
- [ ] Remove PyArrow compute fallbacks for signature derivation.
- [ ] Add coverage for deterministic signatures across param orderings.

---

## Scope 10: Ibis + SQLGlot alignment for DataFusion-native SQL
**Why**: Ibis already compiles to SQLGlot and the DataFusion backend can execute
SQLGlot ASTs directly. Use SQLGlot as the canonical SQL IR to normalize/qualify
queries and reduce bespoke SQL string manipulation.

**Representative code patterns**
```python
import ibis
import sqlglot
from sqlglot.optimizer.qualify import qualify

ctx = SessionContext()
con = ibis.datafusion.connect(ctx)

ast = con.compiler.to_sqlglot(expr)
ast = qualify(ast, dialect="datafusion", schema=sqlglot_schema)
con.raw_sql(ast)
```

```python
from sqlglot.optimizer.normalize import normalize
normalized = normalize(ast, dnf=False, max_distance=128)
con.raw_sql(normalized)
```

**Target files**
- `src/ibis_engine/registry.py`
- `src/ibis_engine/expr_compiler.py`
- `src/datafusion_engine/query_fragments.py`
- `src/normalize/ibis_plan_builders.py`

**Implementation checklist**
- [ ] Ensure Ibis uses a shared DataFusion `SessionContext` (`ibis.datafusion.connect(ctx)`).
- [ ] Introduce SQLGlot AST checkpoints for view/query fragments.
- [ ] Apply SQLGlot `qualify` + `normalize` with DataFusion dialect.
- [ ] Route `raw_sql` through SQLGlot ASTs instead of string concatenation.

---

## Dependencies / Ordering
1. Scope 1 (IDs in views) before any downstream consumers that expect IDs.
2. Scope 2 (stats) before diagnostics improvements.
3. Scope 3 (interval prep) before interval alignment changes.
4. Scope 5 (concat_ws in Ibis) before hash-related rewrites.
5. Scope 7–9 (incremental/param/snapshot) before gating finalization.
6. Scope 6 (gating) after new built-in usages are defined.
7. Scope 10 (Ibis + SQLGlot alignment) can run in parallel with 7–9.

---

## Decommission list (post-scope cleanup)
**Goal**: delete bespoke PyArrow/Python compute paths once DataFusion-native
equivalents are the only supported execution path.

**Candidates for full removal once scopes complete**
- `src/arrowdsl/core/ids.py` hashing/ID helpers (`hash64_from_arrays`,
  `hash64_from_parts`, `hash64_from_columns`, `hash_column_values`,
  `prefixed_hash_id`, `prefixed_hash64`, `stable_id`, `span_id`,
  `add_span_id_column`, `masked_prefixed_hash`) after all ID generation runs in
  DataFusion views/UDFs (Scopes 1 & 8).
- `src/datafusion_engine/extract_ids.py` and `src/arrowdsl/core/ids_registry.py`
  once HashSpec-driven Python ID generation is fully replaced by DataFusion
  view fragments.
- `src/datafusion_engine/compute_ops.py` after all remaining call sites are
  migrated to DataFusion built-ins and Ibis expressions (Scopes 2, 7–9, plus
  schema validation and param signatures).
- `src/ibis_engine/param_tables.py` compute-op helpers (`_compute_array_fn` hash
  path) after param signatures are DataFusion-native (Scope 9).

**Deletion checklist**
- [ ] Verify all callers are migrated to DataFusion/Ibis/SQLGlot paths.
- [ ] Remove UDF/compute fallbacks and update tests accordingly.
- [ ] Confirm function inventories include all new built-ins.
