Yes — **but not by “stitching together” printed plan text**. In DataFusion you “combine plans” by **building a single larger plan graph** (logical, then physical) that contains those operations as subplans.

Here are the practical ways to do it, from most common to most “plan-native”.

## 1) Combine by chaining `DataFrame` operations (recommended)

If the operations are fundamentally linked (the output of A feeds B), just keep them in one `DataFrame` pipeline. DataFusion will produce one logical plan DAG and then one physical plan.

```python
df = ctx.table("t")

df2 = (
  df.filter(col("x") > lit(0))
    .aggregate([col("k")], [sum(col("v")).alias("sv")])
    .filter(col("sv") > lit(10))
)

batches = df2.collect()
```

This is the “native” way to ensure they’re combined and optimized together (predicate pushdown, projection pruning, join reordering, etc.).

## 2) Combine independent subplans via relational operators (join/union/etc.)

If you have **two separate computations** that need to be linked, you combine them by making a parent operator that references both subplans:

* `join(left, right, ...)`
* `union(left, right)` / `union_all`
* `cross_join`
* `intersect`, `except` (if available in your binding)
* correlated subqueries / `exists` via SQL

In DataFrame API terms you typically create two `DataFrame`s, then join/union them.

```python
a = ctx.sql("select k, sum(v) as sv from t group by k")
b = ctx.sql("select k, max(ts) as last_ts from t group by k")

out = a.join(b, join_type="inner", left_cols=["k"], right_cols=["k"])
```

## 3) Use a view/CTE as the “glue” (nice when you *start* from SQL)

If you have plan A and plan B as SQL snippets, combine them with:

* a **CTE** (`WITH a AS (...), b AS (...) SELECT ... FROM a JOIN b ...`)
* or register A as a **temporary view** and query it from B.

CTE keeps it *one query / one plan*.

## 4) If you literally have a `LogicalPlan` object: wrap it as an input to another plan

This is the plan-native version of (2): you build a new logical plan node whose children are the existing plans.

Conceptually:

* `Projection(child=planA)`
* `Filter(child=planA)`
* `Join(left=planA, right=planB, ...)`
* `Union(inputs=[planA, planB])`

Whether you can do this directly depends on your language binding:

* In Rust DataFusion: very doable (LogicalPlanBuilder / DataFrame APIs).
* In Python bindings: you usually do this through `DataFrame` composition, not manual plan-node construction.

## 5) What you generally *cannot* do

* **Concatenate two *physical* plans** after the fact unless you’re writing custom execution nodes (advanced / Rust plugin territory).
* **Combine EXPLAIN strings** into a runnable plan.
* “Merge” two plans that assume different SessionContexts/catalog registrations/UDF registries without recreating a common environment.

## How to decide what you want

* **Linked transformations** (A feeds B): chain them (1).
* **Two derived tables combined**: join/union them (2/3).
* **You’re building an optimizer/IR pipeline**: operate at LogicalPlan (4), then let DataFusion plan + execute.

## 0) Mental model: “plan combination” in DataFusion = build **one LogicalPlan DAG**

**Syntax utilization:** you *combine* work by composing a single plan graph (DataFrame API, SQL, or LogicalPlanBuilder), not by concatenating `EXPLAIN` text. A `DataFrame` is a wrapper over a logical plan; execution happens at `collect()/execute_stream()/to_arrow_table()` etc. ([Docs.rs][1])

**Value proposition:** one optimizer pass over the whole DAG ⇒ pushdowns, join planning, projection pruning, fewer scans/materializations.

**Watchouts:** plan *objects* are only executable inside a compatible planning environment (same catalog/table/UDF registrations); physical plans are even more environment-dependent.

---

# 1) Python (`datafusion`) — plan-combination surface

## 1.1 Binary plan composition (two inputs → one DAG)

### A) Joins (primary “combine plans” operator)

**Syntax**

```python
# key-based join
out = left.join(
    right,
    on=["k"],                     # or left_on=..., right_on=...
    how="inner",                  # inner|left|right|full|semi|anti
    coalesce_duplicate_keys=True, # default
)

# predicate-based join (supports inequality predicates)
out2 = left.join_on(right, col("a") == col("b"), col("x") != col("y"), how="inner")
```

([Apache DataFusion][2])

**Value proposition**

* Converts two subplans into one plan DAG, enabling join reordering / filter pushdown / projection pruning across both sides (within DataFusion’s optimizer capabilities).

**Implementation considerations / watchouts**

* `coalesce_duplicate_keys=True` coalesces identically-named join key columns (reduces post-join ambiguity; but can surprise agents expecting `left.k` + `right.k`). ([Apache DataFusion][2])
* For SQL-like identifier behavior: DataFusion lowercases identifiers in SQL unless quoted; if your schema has capitals, quote (`"VendorID"`) or you’ll get `FieldNotFound`. ([Apache DataFusion][3])
* Keep both DataFrames in the **same SessionContext** (shared function registry / catalog). (Pragmatic invariant for agent code.)

---

### B) Set operators (union/intersect/except)

**Syntax**

```python
u  = df1.union(df2, distinct=False)   # UNION ALL semantics
ud = df1.union(df2, distinct=True)    # UNION DISTINCT semantics
i  = df1.intersect(df2)
e  = df1.except_all(df2)
```

([Apache DataFusion][2])

**Value proposition**

* Canonical “combine two plans that yield compatible rows” operations.

**Implementation considerations / watchouts**

* All of these require **exactly the same schema** in the Python binding surface. ([Apache DataFusion][2])
* If you need “union by name / reorder columns / fill missing with null”, that exists on Rust APIs (see §2) but is not guaranteed in Python; in Python you typically **reorder/align explicitly** with `select(...)` / `select_columns(...)` before union.

---

## 1.2 Name subplans to make multi-step combination modular (views / tables)

### A) Register a DataFrame as a view (subplan alias)

**Syntax**

```python
ctx.register_view("v_filtered", df.filter(col("a") > literal(2)))
out = ctx.sql("SELECT * FROM v_filtered")
```

([Apache DataFusion][4])

**Value proposition**

* Lets agents build reusable “subquery blocks” (like SQL CTEs) and then join/union them later with cleaner code and better debuggability.

**Watchouts**

* Views are **lazy** (re-planned/re-executed each use unless you cache). ([Apache DataFusion][4])

### B) Convert a DataFrame into a table-like object and register it

**Syntax**

```python
view_tbl = df.into_view(temporary=False)
ctx.register_table("v1", view_tbl)
```

([Apache DataFusion][2])

**Value proposition**

* Forces an explicit “subplan as named relation” boundary (useful when passing a SessionContext across components).

**Watchouts**

* Prefer `register_table(...)` over `register_table_provider(...)` in Python (deprecated). ([Apache DataFusion][5])

---

## 1.3 Cache a subplan to avoid repeated scans (materialize once, reuse)

**Syntax**

```python
hot = df.filter(col("kind") == lit("call")).select("a","b").cache()
```

([Apache Arrow][6])

**Value proposition**

* Materializes the subplan into an in-memory table; downstream combinations (joins, unions, aggregations) reuse cached batches.

**Watchouts**

* Memory pressure: cache *after* projection/filter to reduce width/rows.
* Cache is a correctness boundary: if upstream data changes, cached results won’t reflect it unless you rebuild the cache.

---

## 1.4 “Combine plan artifacts” (Substrait / LogicalPlan interop)

### A) Substrait serialize/deserialize (plan IR portability)

**Syntax**

```python
# SQL -> substrait bytes
b = Serde.serialize_bytes("SELECT ...", ctx)

# bytes -> Plan -> LogicalPlan
plan = Serde.deserialize_bytes(b)
lp   = Consumer.from_substrait_plan(ctx, plan)

# LogicalPlan -> DataFrame (now you can join/union/etc)
df_lp = ctx.create_dataframe_from_logical_plan(lp)
```

([Apache DataFusion][7])

**Value proposition**

* Persist “plan intent” (logical) and rehydrate it later; combine rehydrated plans with other DataFrames using normal join/union APIs.

**Watchouts**

* Substrait is **logical**; executing it still requires the same SessionContext environment (tables, UDFs). ([Apache DataFusion][7])

---

## 1.5 SQL-level plan combination (CTE + UNION/JOIN, when agents prefer SQL)

**Syntax**

```sql
WITH x AS (SELECT a, MAX(b) AS b FROM t GROUP BY a)
SELECT a, b FROM x;
```

DataFusion SQL also supports `UNION/INTERSECT/EXCEPT` and many join types (incl. cross/natural/semi/anti). ([Apache DataFusion][3])

**Value proposition**

* LLMs can emit fewer moving parts as a single SQL string; DataFusion builds one LogicalPlan.

**Watchouts**

* SQL identifier case: DataFusion lowercases identifiers unless quoted; quote `"ColName"` when schema has capitals. ([Apache DataFusion][3])

---

# 2) Rust (DataFusion core) — plan-combination surface

## 2.1 DataFrame API (highest-level; mirrors Python but richer)

**Syntax**

```rust
use datafusion::prelude::*;
use datafusion::prelude::JoinType;

let join = left.join(right, JoinType::Inner, &["a","b"], &["a2","b2"], None)?;
let join_on = left.join_on(right, JoinType::Inner, [col("a").not_eq(col("a2"))])?;

let u_all  = df1.union(df2)?;
let u_name = df1.union_by_name(df2)?;          // aligns by name, fills missing with NULL
let u_nd   = df1.union_by_name_distinct(df2)?; // by-name + distinct

let i  = df1.intersect(df2)?;
let e  = df1.except(df2)?;
```

([Docs.rs][1])

**Value proposition**

* Same “single DAG” benefits as Python, plus **union-by-name** variants that make schema alignment substantially easier for agent-written code. ([Docs.rs][1])

**Watchouts**

* `into_unoptimized_plan()` is explicitly “should not be used outside testing” because it can lose the SessionState snapshot (e.g., `now()` differences). Use `into_parts()` if you need both plan + state. ([Docs.rs][1])
* `into_view()` discards the DataFrame’s SessionState and uses the SessionState passed at scan time. If you rely on session-scoped settings/UDFs, make that explicit. ([Docs.rs][1])

## 2.2 LogicalPlanBuilder (mid-level; explicit plan assembly)

**Syntax**

```rust
use datafusion::logical_expr::LogicalPlanBuilder;

// combine plans explicitly
let plan = LogicalPlanBuilder::from(left_plan)
    .join(right_plan, JoinType::Inner, (vec![col("k")], vec![col("k")]), None)?
    .union(other_plan)?
    .build()?;
```

Builder exposes `join`, `union`, `union_by_name`, `union_distinct`, etc. ([Docs.rs][8])

**Value proposition**

* Useful when an agent already has `LogicalPlan` fragments (from SQL planning, Substrait consumer, custom operators) and needs to splice them.

**Watchouts**

* Low-level union helper `datafusion::logical_expr::union(left,right)` constructs a UNION but defers type coercion until optimizer passes; don’t treat pre-optimizer schema as final. ([Docs.rs][9])
* For CTE/recursive workflows: `LogicalPlan` includes `Union`, `Subquery`, `SubqueryAlias`, and `RecursiveQuery` variants. ([Docs.rs][10])

---

# 3) Delta Lake (`deltalake` / delta-rs) integrations with DataFusion

## 3.1 Python: register `DeltaTable` as a DataFusion table provider (best path)

**Syntax**

```python
from datafusion import SessionContext
from deltalake import DeltaTable

ctx = SessionContext()
dt = DeltaTable("path/to/table")
ctx.register_table("t_delta", dt)
df = ctx.table("t_delta")
```

([Apache DataFusion][11])

**Value proposition**

* Delta provides **file-level skipping** via transaction log metadata, then DataFusion can still do Parquet row-group pruning → fewer files + fewer row groups read. ([Delta][12])
* Delta table semantics: ACID / concurrency protection / time travel, etc., while still queryable from DataFusion. ([Delta][12])

**Watchouts**

* Version gating: DataFusion docs call out Delta provider support as of 43.0.0+ and note older `deltalake` versions fall back to Arrow Dataset (loses filter pushdown). ([Apache DataFusion][11])
* Case sensitivity / identifier normalization: if you hit `FieldNotFound` on mixed-case columns, disable identifier normalization or quote identifiers; this is a known pain point in the ecosystem. ([GitHub][13])
* Version mismatches can still bite in practice (e.g., segfault reports when `datafusion` Python and delta-rs crates drift); treat as “pin versions tightly, test registration early”. ([GitHub][14])

## 3.2 Python fallback: register Arrow Dataset (works, but weaker)

**Syntax**

```python
ctx.register_dataset("t_delta", dt.to_pyarrow_dataset())
```

([Apache DataFusion][11])

**Watchouts**

* DataFusion docs warn this path may lose filter pushdown and cause significant perf differences. ([Apache DataFusion][11])

## 3.3 Delta time travel + plan combination

**Syntax**

```python
dt.load_as_version(123)   # or RFC3339 timestamp / datetime
ctx.register_table("t_delta_v123", dt)
```

([Delta][15])

**Value proposition**

* Deterministic “query snapshot” inputs: combine a specific Delta version with other tables in the same DataFusion plan (joins/unions) and know the Delta side is stable.

**Watchouts**

* Time travel availability depends on retention / vacuum policies (Delta-wide operational constraint). ([Delta Lake][16])

## 3.4 Rust: DeltaTableProvider + DeltaScanConfig (deep integration knobs)

**Syntax (conceptual)**

* `DeltaTableProvider::try_new(snapshot, log_store, config)`; `.with_files(files)` to restrict scan to a chosen file set. ([Docs.rs][17])
* `DeltaScanConfig` knobs:

  * `file_column_name` (emit source path),
  * `wrap_partition_values` (dictionary-encode partition values),
  * `enable_parquet_pushdown`,
  * `schema_force_view_types`,
  * `schema` override. ([Docs.rs][18])

**Value proposition**

* Treat Delta as a first-class TableProvider with explicit scan semantics; great for “semantic compiler” style pipelines where provenance columns and deterministic file subsets matter.

**Watchouts**

* Pushdown behavior is configurable (`enable_parquet_pushdown`) and materially affects plan performance. ([Docs.rs][18])
* delta-rs explicitly uses DataFusion internally for SQL-expression features (merge/update/constraints); align expectations between “DataFusion as query engine” and “delta-rs as commit engine.” ([Delta][12])

---

# 4) Agent-ready “how to combine two existing plans” recipes

## Recipe A: you have two Python DataFrames

* Use `join/union/intersect/except_all`; register intermediate views for readability. ([Apache DataFusion][2])
* Cache shared subplans if reused repeatedly. ([Apache DataFusion][2])

## Recipe B: you have a Substrait plan + a DataFrame (Python)

* `Consumer.from_substrait_plan(ctx, plan)` → `LogicalPlan` → `ctx.create_dataframe_from_logical_plan(lp)` → join/union. ([Apache DataFusion][7])

## Recipe C: you have two Rust LogicalPlans

* `LogicalPlanBuilder::from(plan_a).join(plan_b, ...)…build()` (or `union_by_name` for schema drift). ([Docs.rs][8])

## Recipe D: you’re mixing Delta + non-Delta inputs

* Load/version-pin Delta (`DeltaTable` + optional `load_as_version`) → register as table → combine with other tables in a single SQL query or DataFrame DAG. ([Apache DataFusion][11])

[1]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html "DataFrame in datafusion::dataframe - Rust"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/user-guide/sql/select.html "SELECT syntax — Apache DataFusion  documentation"
[4]: https://datafusion.apache.org/python/user-guide/common-operations/views.html?utm_source=chatgpt.com "Registering Views — Apache Arrow DataFusion documentation"
[5]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[6]: https://arrow.staged.apache.org/datafusion-python/generated/datafusion.DataFrame.html "datafusion.DataFrame — Apache Arrow DataFusion  documentation"
[7]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html "datafusion.substrait — Apache Arrow DataFusion  documentation"
[8]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.LogicalPlanBuilder.html "LogicalPlanBuilder in datafusion::logical_expr - Rust"
[9]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.union.html "union in datafusion::logical_expr - Rust"
[10]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html "LogicalPlan in datafusion_expr::logical_plan - Rust"
[11]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[12]: https://delta-io.github.io/delta-rs/integrations/delta-lake-datafusion/ "DataFusion - Delta Lake Documentation"
[13]: https://github.com/delta-io/delta-rs/discussions/3353?utm_source=chatgpt.com "Columns with capital letters in DataFusion #3353"
[14]: https://github.com/delta-io/delta-rs/issues/4135?utm_source=chatgpt.com "Segmentation fault when registering table to datafusion ..."
[15]: https://delta-io.github.io/delta-rs/api/delta_table/?utm_source=chatgpt.com "DeltaTable - Delta Lake Documentation"
[16]: https://delta.io/blog/2023-02-01-delta-lake-time-travel/?utm_source=chatgpt.com "Delta Lake Time Travel"
[17]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaTableProvider.html "DeltaTableProvider in deltalake::delta_datafusion - Rust"
[18]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaScanConfig.html "DeltaScanConfig in deltalake::delta_datafusion - Rust"

## 1) Parameterized composition: “same plan shape, different scalars”

### A) SQL prepared statements (`PREPARE … AS …;` + `EXECUTE …`)

* **Syntax utilization**

  * `PREPARE name(type[, ...]) AS <query with $1, $2 ...>;`
  * `EXECUTE name(val1[, ...]);` ([Apache DataFusion][1])
* **Value proposition**

  * Reuse *planned* query shape repeatedly with different scalar thresholds (agent workloads: repeated probes, incremental scans, validation queries). ([Apache DataFusion][1])
* **Implementation considerations / watchouts**

  * Parameter types can be explicit or inferred at execution time. ([Apache DataFusion][1])
  * Still environment-bound: catalog/table/UDF registry must match for each execution (prepared plan references names).

### B) Python “named parameter replacement” in `ctx.sql(..., val=..., param_values=...)` (DataFusion-Python ≥ 51.0.0)

* **Syntax utilization**

  * String replacement placeholders: `$name` (e.g., `... WHERE "Attack" > $val`) + kwargs `val=threshold`. ([Apache DataFusion][2])
  * Lossless scalar binding: `param_values={"val": threshold}` (casts to Arrow Scalar). ([Apache DataFusion][2])
  * Passing a `DataFrame` as a parameter: `$df` registers a temporary view with a generated name. ([Apache DataFusion][2])
* **Value proposition**

  * Agents can “compose plans” by injecting *relations* (DataFrames) and *scalars* into a single SQL statement while retaining laziness until terminal execution. ([Apache DataFusion][2])
* **Implementation considerations / watchouts**

  * Dialect constraints: `$name` replacement works for all SQL dialects except `hive` and `mysql`; if you must use those, avoid `$name` and use other replacement strategies. ([Apache DataFusion][2])
  * Temp-view requirement: DF-parameter SQL relies on temporary view registration; custom Catalog/Schema providers must not leak temp views across contexts. ([Apache DataFusion][2])
  * String replacement can lose precision (floats); use `param_values` when exactness matters. ([Apache DataFusion][2])

### C) Rust DataFrame parameter binding (`with_param_values`)

* **Syntax utilization**

  * `df.with_param_values(ParamValues::List(vec![ScalarValue::from(...) ...]))` to replace logical-plan parameters before execution. ([Docs.rs][3])
* **Value proposition**

  * “Plan template” reuse without SQL string manipulation; keeps a stable LogicalPlan shape for repeated join/union pipelines.
* **Implementation considerations / watchouts**

  * Today it’s positional parameter plumbing (named parameters are a requested feature). ([GitHub][4])

## 2) Subqueries + CTEs as first-class plan combination (beyond joins/unions)

### A) Subqueries in `SELECT` / `FROM` / `WHERE` / `HAVING`

* **Syntax utilization**

  * `FROM (<subquery>)` for derived-table composition.
  * Scalar subquery in `SELECT` list.
  * `WHERE expr operator (<subquery>)` (correlated + non-correlated depending on operator). ([Apache DataFusion][5])
* **Value proposition**

  * Lets agents combine computations without materializing intermediate relations (optimizer can rewrite into joins / pushdowns).
* **Implementation considerations / watchouts**

  * `EXISTS/NOT EXISTS`: DataFusion documents “only correlated subqueries are supported.” ([Apache DataFusion][5])
  * `IN/NOT IN`: supports list literals and (correlated) subqueries; for non-scalar subqueries must return a single column. ([Apache DataFusion][5])
  * Correlated subqueries are often rewritten to joins (historically called out as supported by DataFusion). ([Apache DataFusion][6])
  * `LATERAL` joins: currently tracked as a feature request (don’t assume availability). ([GitHub][7])

### B) Recursive composition (CTE recursion / `RecursiveQuery`)

* **Syntax utilization (Rust LogicalPlanBuilder)**

  * `LogicalPlanBuilder::to_recursive_query(name, recursive_term, is_distinct)` where `is_distinct` maps to `UNION` vs `UNION ALL`. ([Docs.rs][8])
* **Value proposition**

  * Express iterative graph-style expansions and “repeat-until” pipelines inside the plan graph (no host-language loops stitching results).
* **Implementation considerations / watchouts**

  * Recursive execution involves special physical-plan handling and state wiring (tends to stress “prepared physical plan reuse” discussions). ([GitHub][9])

## 3) Union variants, partition semantics, and the `UnionExec` ↔ `InterleaveExec` switch

### A) Physical union concatenation (`UnionExec`)

* **Syntax utilization (Rust physical-plan layer)**

  * `UnionExec::try_new(vec![input1, input2, ...])` (or via planner).
* **Value proposition**

  * Cheap combination: concatenates partitions; does **not** mix/copy within partitions; preserves per-partition sort order if inputs are sorted. ([Docs.rs][10])
* **Implementation considerations / watchouts**

  * Output partition count is the sum of inputs’ partitions (can explode parallelism / downstream merge cost). ([Docs.rs][10])

### B) Partition-aware interleaving (`InterleaveExec`)

* **Syntax utilization**

  * Interleave is a different physical operator: combines streams by interleaving when partition specs align. ([Docs.rs][11])
* **Value proposition**

  * Maintains hash-partitioning across union-like combination (better downstream locality vs naive “N+M partitions”).
* **Implementation considerations / watchouts**

  * Hard requirement: **all inputs must have the same hash-partitioning**. ([Docs.rs][12])
  * Optimizer gate: `datafusion.optimizer.prefer_existing_union` prevents converting `Union` → `Interleave`. ([Apache DataFusion][13])
  * “Can interleave?” is computed by checking compatible hash partition specs (see `can_interleave`). ([Docs.rs][11])

### C) Name-based union in Rust (schema drift tolerant)

* **Syntax utilization**

  * DataFrame: `union_by_name`, `union_by_name_distinct` (fills missing columns with NULL). ([Docs.rs][14])
  * LogicalPlan: `union_by_name(left_plan, right_plan)` helper. ([Docs.rs][15])
* **Value proposition**

  * Robust combination across evolving schemas; eliminates “reorder/select/NULL-pad” boilerplate.
* **Implementation considerations / watchouts**

  * Python binding typically enforces exact schema for set ops; assume you must align explicitly unless your specific binding version exposes by-name variants. (Rust has the by-name primitives explicitly.) ([Docs.rs][14])

## 4) Struct / nested-type coercion during combination (joins/unions/CTEs)

### A) Name-based struct field mapping (not positional)

* **Syntax utilization**

  * Struct literals `{a: 1, b: 2}` combined across UNION/array/join/CTE; DataFusion matches fields by **name**. ([Apache DataFusion][16])
* **Value proposition**

  * Agents can union/join nested structs even when field order differs (reduces “schema canonicalization” work).
* **Implementation considerations / watchouts**

  * Missing fields are filled with NULL *only when you cast to a unified schema*; otherwise mismatched field sets can fail. ([Apache DataFusion][16])
  * Documented limitation: “field count must match exactly” in some coercion contexts; workaround is explicit `CAST(... AS STRUCT(...))`. ([Apache DataFusion][16])
  * Use `arrow_typeof(...)` to verify resulting unified types when debugging. ([Apache DataFusion][16])

## 5) Join algorithm choice + dynamic filtering knobs (affects “combined plan” performance)

### A) Join exec families (physical reality)

* **Syntax utilization**

  * Physical join operators include HashJoin, SortMergeJoin, NestedLoopJoin, CrossJoin, SymmetricHashJoin, PiecewiseMergeJoin. ([Docs.rs][17])
* **Value proposition**

  * Understanding which physical join is chosen tells you whether the combined plan is memory-heavy (hash), ordering-heavy (sort-merge), or fallback (nested loop).
* **Implementation considerations / watchouts**

  * Memory: Hash join may be faster but consumes more memory; SortMergeJoin is more memory-efficient for very large inputs. ([Apache DataFusion][13])
  * Streaming/unbounded: Symmetric hash join has specific pruning controls (can be disabled via config). ([Apache DataFusion][13])
  * Experimental path: Piecewise merge join is gated (`datafusion.optimizer.enable_piecewise_merge_join`). ([Apache DataFusion][13])

### B) Runtime config you should set explicitly in agent code (SQL `SET` or programmatic)

* **Syntax utilization**

  * `SET datafusion.execution.target_partitions = <n>;`
  * `SET datafusion.optimizer.prefer_hash_join = false;`
  * Join-specific knobs: `allow_symmetric_joins_without_pruning`, `enable_piecewise_merge_join`. ([Apache DataFusion][13])
* **Value proposition**

  * Makes join/union composition stable across environments (CI vs laptop) and avoids “planner surprises.”
* **Implementation considerations / watchouts**

  * Dynamic filtering / join pushdowns:

    * `datafusion.optimizer.hash_join_inlist_pushdown_max_size` + `_max_distinct_values` controls when build-side values are pushed as `IN (...)` (benefits pruning / bloom usage; costs memory/copies). ([Apache DataFusion][13])
    * Global dynamic-filter toggles exist (`enable_dynamic_filter_pushdown` and per-operator variants). ([Apache DataFusion][18])

## 6) Partitioning operators you must reason about when combining plans

### A) `RepartitionExec` / `CoalescePartitionsExec` / `CoalesceBatchesExec`

* **Syntax utilization**

  * You don’t usually instantiate these directly; you detect/control them via `EXPLAIN` and config (`target_partitions`, `coalesce_batches`, round-robin repartition). ([Apache DataFusion][19])
* **Value proposition**

  * Correct partitioning is what makes “joined/unioned plan DAGs” scale; repartition nodes are the explicit cross-core data exchanges. ([Apache DataFusion][19])
* **Implementation considerations / watchouts**

  * `target_partitions` impacts concurrency and memory pressure (fewer partitions → more memory per partition). ([Apache DataFusion][13])
  * `coalesce_batches=true` can stabilize performance after selective filters/joins that create tiny batches. ([Apache DataFusion][13])
  * `enable_round_robin_repartition` can be used to increase parallelism when needed. ([Apache DataFusion][18])

## 7) Logical → Physical compilation + explicit physical-plan composition (Rust)

### A) Create physical plans explicitly (`SessionState::create_physical_plan`)

* **Syntax utilization**

  * Build a `LogicalPlan` (DataFrame / SQL / LogicalPlanBuilder), then: `ctx.state().create_physical_plan(&logical_plan).await?` ([Apache DataFusion][20])
* **Value proposition**

  * Lets agents “freeze” the post-optimizer physical form for inspection, serialization, or custom execution/instrumentation.
* **Implementation considerations / watchouts**

  * Physical plan is hardware + data-layout specific; the same logical plan can yield different physical plans under different configs/files/cores. ([Apache DataFusion][19])

### B) ExecutionPlan API: rewriting / stitching at the physical layer

* **Syntax utilization**

  * `children()` and `with_new_children(...)` support structural rewrites (swap subplans, wrap nodes, patch pipelines). ([Docs.rs][21])
  * `execute(partition, task_ctx)` returns an async stream of `RecordBatch` (streaming by default). ([Docs.rs][21])
* **Value proposition**

  * Enables advanced agent behaviors: plan-level instrumentation, late-binding shared state (recursive worktables), custom operators.
* **Implementation considerations / watchouts**

  * Filter pushdown is opt-in per node; operators can generate additional pushdowns (e.g., hash join adds bloom / lookup filters). ([Docs.rs][21])
  * Errors propagate through the output stream and cancel downstream work quickly (important for “agent-run pipelines” that chain multiple executions). ([Docs.rs][21])

## 8) Plan (de)serialization for combination across processes (Rust)

### A) `datafusion-proto`: serialize `LogicalPlan` + `ExecutionPlan` to bytes

* **Syntax utilization**

  * Logical: `logical_plan_to_bytes(&plan)` / `logical_plan_from_bytes(&bytes, &ctx)`
  * Physical: `physical_plan_to_bytes(plan)` / `physical_plan_from_bytes(&bytes, &ctx)` ([Docs.rs][22])
* **Value proposition**

  * Move plan fragments between agent processes (planner agent → executor agent), or cache compiled plans.
* **Implementation considerations / watchouts**

  * **Version lock**: serialized bytes are not guaranteed compatible across DataFusion versions. ([Docs.rs][22])
  * If you need a more standard IR, Substrait exists as an alternative (but still requires environment compatibility for execution). ([Docs.rs][22])

## 9) Materialization boundaries: DML/DDL outputs from combined plans (COPY / INSERT / write_table)

### A) SQL `COPY (query) TO …` as “materialize combined plan to files”

* **Syntax utilization**

  * `COPY {table|query} TO 'path' [STORED AS parquet|csv|json|arrow] [PARTITIONED BY (...)] [OPTIONS(...)]` ([Apache DataFusion][23])
* **Value proposition**

  * Persist results of a multi-join/union/subquery plan for caching, reproducibility, or handoff to other engines.
* **Implementation considerations / watchouts**

  * `PARTITIONED BY` removes partition columns from output by default; keep them via `execution.keep_partition_by_columns true` / `ExecutionOptions`. ([Apache DataFusion][23])
  * Option precedence: statement syntax > `COPY OPTIONS` > session defaults. ([Apache DataFusion][24])

### B) Rust `DataFrame::write_table` / `write_csv` / `write_json`

* **Syntax utilization**

  * `df.write_table("tbl", DataFrameWriteOptions::new()).await?` uses `TableProvider::insert_into`. ([Docs.rs][3])
* **Value proposition**

  * “Execute + persist” as a single terminal operation (agent can treat as a sink node in a DAG).
* **Implementation considerations / watchouts**

  * `write_table` is eager and depends on the target `TableProvider` implementing `insert_into`. ([Docs.rs][3])
  * `SessionContext::sql` includes in-memory default implementations for DDL/DML; if you need to forbid these in agent environments use `sql_with_options(...).with_allow_ddl(false)` style restrictions. ([Docs.rs][25])

## 10) Delta Lake integration details that matter specifically for combined plans

### A) Delta read-side: file skipping + DataFusion pushdowns (why it’s faster than raw Parquet)

* **Syntax utilization**

  * Register a `DeltaTable` into DataFusion and query via SQL/DataFrame (Python/Rust). ([Delta][26])
* **Value proposition**

  * Delta transaction log provides file-level metadata → skip entire files, then DataFusion can still prune row groups inside files (two-stage pruning). ([Delta][26])
* **Implementation considerations / watchouts**

  * The delta-rs doc example uses `register_table_provider` (Python), but newer DataFusion-Python APIs prefer `register_table` / avoid deprecated provider registration in new code.

### B) Delta write-back from combined DataFusion plans (practical pipeline pattern)

* **Syntax utilization (Python)**

  * DataFusion executes to Arrow (`to_arrow_table()` / stream record batches), then `deltalake.write_deltalake(uri, data, mode=..., schema_mode=..., partition_by=..., predicate=..., target_file_size=..., writer_properties=...)`. ([Delta][27])
* **Value proposition**

  * Clean separation: DataFusion = compute/plan-combine; delta-rs = transactional commit (ACID, schema evolution controls, replaceWhere overwrite). ([Delta][27])
* **Implementation considerations / watchouts**

  * Schema mismatches fail by default; opt into `schema_mode="merge"` or `"overwrite"` intentionally. ([Delta][27])
  * Partial overwrite (`replaceWhere` / `predicate=`) must be consistent: incoming data must satisfy the predicate or the operation fails. ([Delta][27])
  * Control file sizing + parquet writer behavior (`target_file_size`, `writer_properties`, bloom/statistics per column) to keep downstream combined queries fast. ([Delta][28])

### C) Delta uses DataFusion for SQL-expression evaluation (merge/update/constraints)

* **Syntax utilization**

  * Rust `DeltaOps(...).write(...).with_replace_where(col("id").eq(lit("1")))` uses DataFusion-style expressions. ([Delta][27])
* **Value proposition**

  * A single expression system across “query plans” and “Delta DML semantics” reduces impedance mismatch in agent code.
* **Implementation considerations / watchouts**

  * Delta features like update/merge/invariants are implemented “in terms of SQL expressions” and depend on a SQL engine (DataFusion) under the hood. ([Delta][26])

## 11) Case-sensitivity + identifier normalization (silent plan-combination footgun)

* **Syntax utilization**

  * SQL: unquoted identifiers are normalized (lowercased); quote `"FieldName"` when schema has capitals. ([Apache DataFusion][29])
  * Rust DataFrame: `with_column_renamed` supports case-sensitive rename by quoting, or disable normalization via `datafusion.sql_parser.enable_ident_normalization=false`. ([Docs.rs][3])
* **Value proposition**

  * Prevents “join/union fails because column not found” when combining heterogeneous sources (Parquet inferred schema vs Delta schema vs hand-built MemTable).
* **Implementation considerations / watchouts**

  * If an agent writes SQL against a schema with mixed-case fields, enforce a “quote-all identifiers” policy or disable normalization consistently across contexts. ([Apache DataFusion][29])

## 12) Plan reuse reality check: physical plan reuse is non-trivial

* **Syntax utilization**

  * Logical reuse is well-supported (prepared statements, param binding, logical plan bytes); physical reuse is more delicate (metrics/state). ([Apache DataFusion][1])
* **Value proposition**

  * Reduces planning latency in “agent loops” (many small queries).
* **Implementation considerations / watchouts**

  * There’s active work/concern around “prepared physical plan reuse” (metrics storage, placeholders, recursive executor copying per iteration). Treat physical-plan caching as experimental unless you own the constraints end-to-end. ([GitHub][9])

[1]: https://datafusion.apache.org/user-guide/sql/prepared_statements.html "Prepared Statements — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/python/user-guide/sql.html "SQL — Apache Arrow DataFusion  documentation"
[3]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html "DataFrame in datafusion::dataframe - Rust"
[4]: https://github.com/apache/datafusion/issues/8245?utm_source=chatgpt.com "apache/datafusion - Support named query parameters"
[5]: https://datafusion.apache.org/user-guide/sql/subqueries.html "Subqueries — Apache DataFusion  documentation"
[6]: https://datafusion.apache.org/blog/output/2022/10/25/datafusion-13.0.0/ "Apache Arrow DataFusion 13.0.0 Project Update - Apache DataFusion Blog"
[7]: https://github.com/apache/datafusion/issues/10048?utm_source=chatgpt.com "Feature request: Support for lateral joins #10048"
[8]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.LogicalPlanBuilder.html?utm_source=chatgpt.com "LogicalPlanBuilder in datafusion::logical_expr - Rust"
[9]: https://github.com/apache/datafusion/issues/14342?utm_source=chatgpt.com "Prepared physical plan reusage · Issue #14342"
[10]: https://docs.rs/datafusion/latest/datafusion/physical_plan/union/struct.UnionExec.html "UnionExec in datafusion::physical_plan::union - Rust"
[11]: https://docs.rs/datafusion-physical-plan/latest/datafusion_physical_plan/union/index.html "datafusion_physical_plan::union - Rust"
[12]: https://docs.rs/deltalake/latest/deltalake/datafusion/physical_plan/union/struct.InterleaveExec.html "InterleaveExec in deltalake::datafusion::physical_plan::union - Rust"
[13]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[14]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html?utm_source=chatgpt.com "DataFrame in datafusion"
[15]: https://docs.rs/datafusion/latest/datafusion/logical_expr/logical_plan/builder/fn.union_by_name.html?utm_source=chatgpt.com "union_by_name in datafusion::logical_expr::logical_plan"
[16]: https://datafusion.apache.org/user-guide/sql/struct_coercion.html "Struct Type Coercion and Field Mapping — Apache DataFusion  documentation"
[17]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/index.html "datafusion::physical_plan::joins - Rust"
[18]: https://datafusion.apache.org/_sources/user-guide/configs.md.txt?utm_source=chatgpt.com "configs.md.txt - Apache DataFusion"
[19]: https://datafusion.apache.org/user-guide/explain-usage.html "Reading Explain Plans — Apache DataFusion  documentation"
[20]: https://datafusion.apache.org/library-user-guide/building-logical-plans.html "Building Logical Plans — Apache DataFusion  documentation"
[21]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html "ExecutionPlan in datafusion::physical_plan - Rust"
[22]: https://docs.rs/datafusion-proto/latest "datafusion_proto - Rust"
[23]: https://datafusion.apache.org/user-guide/sql/dml.html "DML — Apache DataFusion  documentation"
[24]: https://datafusion.apache.org/user-guide/sql/format_options.html "Format Options — Apache DataFusion  documentation"
[25]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html "SessionContext in datafusion::execution::context - Rust"
[26]: https://delta-io.github.io/delta-rs/integrations/delta-lake-datafusion/ "DataFusion - Delta Lake Documentation"
[27]: https://delta-io.github.io/delta-rs/usage/writing/ "Writing Delta Tables - Delta Lake Documentation"
[28]: https://delta-io.github.io/delta-rs/api/delta_writer/ "Writer - Delta Lake Documentation"
[29]: https://datafusion.apache.org/user-guide/sql/select.html?utm_source=chatgpt.com "SELECT syntax — Apache DataFusion documentation"


## 1) Plan observability + visualization (critical for “combined plan” correctness/perf validation)

### A) SQL `EXPLAIN` formats + analyze levels

* **Syntax utilization**

  * `EXPLAIN [ANALYZE] [VERBOSE] [FORMAT tree|indent|pgjson|graphviz] <statement>`; `EXPLAIN VERBOSE` only supports `indent`. ([Apache DataFusion][1])
  * `EXPLAIN ANALYZE VERBOSE` → per-partition metrics detail; `datafusion.explain.analyze_level` controls metric verbosity. ([Apache DataFusion][1])
* **Value proposition**

  * `graphviz` / `pgjson` outputs are machine-ingestible for plan diffing + tooling (agent-friendly “plan snapshots”). ([Apache DataFusion][1])
* **Implementation considerations / watchouts**

  * Set defaults via configs: `datafusion.explain.format`, `datafusion.explain.logical_plan_only`, `datafusion.explain.physical_plan_only`, `datafusion.explain.show_schema`, `datafusion.explain.show_statistics`, `datafusion.explain.show_sizes`. ([Apache DataFusion][2])
  * `EXPLAIN VERBOSE` is the fastest way to see omitted scan details and intermediate physical plans DataFusion generates (helps debug why/where operators were inserted/removed). ([Apache DataFusion][3])

### B) Python: plan display + graphviz output without SQL

* **Syntax utilization**

  * `LogicalPlan.display_*()` family: `display()`, `display_indent()`, `display_graphviz()`, `display_indent_schema()`; Physical: `ExecutionPlan.display()` / `display_indent()`. ([Apache DataFusion][4])
* **Value proposition**

  * Agents can render/compare plan structure *without* relying on SQL `EXPLAIN` string parsing (stable programmatic surface). ([Apache DataFusion][4])
* **Watchouts**

  * “Graphviz” output is DOT; keep it as an artifact string and render with DOT tooling; don’t assume downstream systems accept DataFusion’s exact formatting. ([Apache DataFusion][4])

### C) Catalog + config introspection via `information_schema` / `SHOW`

* **Syntax utilization**

  * `SHOW TABLES` / `information_schema.tables`; `SHOW COLUMNS FROM t` / `information_schema.columns`; `SHOW ALL` / `information_schema.df_settings`; `SHOW FUNCTIONS [LIKE pattern]`. ([Apache DataFusion][5])
* **Value proposition**

  * Before combining two subplans, agents can assert: (a) the same table names resolve, (b) schema matches, (c) the same config knobs are active. ([Apache DataFusion][5])
* **Watchouts**

  * Config drift is a primary cause of “same logical plan, different physical behavior”; always snapshot `df_settings` alongside plan output. ([Apache DataFusion][5])

---

## 2) Plan (de)serialization in Python (proto bytes) for cross-process plan splicing

### A) Logical/Physical plan proto IO

* **Syntax utilization**

  * `LogicalPlan.to_proto() -> bytes` / `LogicalPlan.from_proto(ctx, bytes)`.
  * `ExecutionPlan.to_proto() -> bytes` / `ExecutionPlan.from_proto(ctx, bytes)`.
  * `ExecutionPlan.partition_count` to sanity-check post-combination parallelism. ([Apache DataFusion][4])
* **Value proposition**

  * Agents can: plan in one process → send bytes to another process → rehydrate → attach into a larger pipeline (or just execute). ([Apache DataFusion][4])
* **Implementation considerations / watchouts**

  * Proto round-trip limitation: “tables created in memory from record batches are currently not supported” (plan hydration can fail if your plan depends on MemTables built from in-memory batches). ([Apache DataFusion][4])
  * Treat proto bytes as *version-coupled* artifacts (pin DataFusion versions across producer/consumer).

---

## 3) Plan ⇄ SQL round-trip (Unparser) as a combination primitive (Python + Rust)

### A) Python unparser (dialect-aware)

* **Syntax utilization**

  * `Unparser(Dialect.postgres()|duckdb()|mysql()|sqlite()|default()).plan_to_sql(logical_plan) -> str`
  * `Unparser.with_pretty(True|False)` for readability. ([Apache DataFusion][6])
* **Value proposition**

  * “Combine-by-regeneration”: convert a plan fragment to SQL, embed it as a CTE/subquery in a larger SQL statement, re-plan once in the target SessionContext (especially useful when moving fragments across contexts/catalogs). ([Apache DataFusion][6])
* **Watchouts**

  * Not all plans can be converted to SQL; expect hard errors for non-expressible operators or provider-specific nodes. (Mirror of Rust’s `plan_to_sql` error behavior.) ([Docs.rs][7])

### B) Rust `plan_to_sql` (+ feature gating)

* **Syntax utilization**

  * `plan_to_sql(&LogicalPlan) -> Result<Statement, DataFusionError>` (requires crate features `sql` + `unparser`). ([Docs.rs][7])
* **Value proposition**

  * Enables “parse SQL → modify plan → unparse → pushdown/federate” workflows; DataFusion highlights this for federation and SQL generation use cases. ([Apache DataFusion][8])
* **Watchouts**

  * Unparser output has quoting/qualification semantics (e.g., `"table"."col"`); agents must not post-process SQL with naive string ops. ([Docs.rs][7])

---

## 4) DDL/DML as first-class plan-combination boundaries (SQL)

### A) In-memory CTAS + VALUES tables

* **Syntax utilization**

  * `CREATE TABLE [OR REPLACE] [IF NOT EXISTS] t AS SELECT ...`
  * `CREATE TABLE ... AS VALUES (...), (...);` with optional column definitions. ([Apache DataFusion][9])
* **Value proposition**

  * Materialize an intermediate combined subplan into a named relation (stabilizes downstream joins/unions; can be used as a cache-like boundary). ([Apache DataFusion][9])
* **Watchouts**

  * It’s in-memory (by design); memory pressure shifts from “pipeline” to “catalog object”.

### B) Views (SQL-level modular subplans)

* **Syntax utilization**

  * `CREATE [OR REPLACE] VIEW v AS <statement>`; views can be defined from `SELECT` or `VALUES`. ([Apache DataFusion][9])
* **Value proposition**

  * Lets agents name subplans and combine them later without duplicating SQL; keeps final query as one logical DAG at plan time. ([Apache DataFusion][9])
* **Watchouts**

  * Views are expansion-time constructs; combined plan changes if underlying tables/configs change unless you materialize.

### C) External tables: partitioning, ordering, unbounded/streaming semantics

* **Syntax utilization**

  * `CREATE [UNBOUNDED] EXTERNAL TABLE ... STORED AS <CSV|PARQUET|...> [PARTITIONED BY (...)] [WITH ORDER (...)] [OPTIONS(...)] LOCATION '...'`. ([Apache DataFusion][9])
* **Value proposition**

  * `PARTITIONED BY` + hive-style dirs: improves pruning when combined plans apply filters. ([Apache DataFusion][9])
  * `WITH ORDER`: informs planner about existing ordering (enables better sort/TopK and join strategies when correct). ([Apache DataFusion][9])
  * `UNBOUNDED`: forces streaming feasibility checks at plan generation (prevents building non-streamable combined plans). ([Apache DataFusion][9])
* **Watchouts**

  * `WITH ORDER` is only valid if the underlying file data is actually sorted; otherwise results may be incorrect. ([Apache DataFusion][9])
  * Statistics collection can be expensive on table creation; toggled by `datafusion.execution.collect_statistics`. ([Apache DataFusion][9])

### D) `INSERT INTO` as a “sink” for combined plans

* **Syntax utilization**

  * `INSERT INTO table_name { VALUES (...), (...) | <query> }`. ([Apache DataFusion][10])
* **Value proposition**

  * Turns an arbitrary combined query plan into a write operation via `TableProvider::insert_into` (source-dependent transactional semantics). ([Apache DataFusion][10])
* **Watchouts**

  * Works only for providers implementing `insert_into`; otherwise fails at execution planning. ([Apache DataFusion][11])

---

## 5) Explicit partition + repartition control (Python) to make combined plans stable/fast

### A) SessionConfig / RuntimeEnvBuilder (Python)

* **Syntax utilization**

  * `SessionContext(SessionConfig(...), RuntimeEnvBuilder(...))`
  * Example config toggles: `.with_target_partitions(n)`, `.with_repartition_joins(bool)`, `.with_repartition_aggregations(bool)`, `.with_repartition_windows(bool)`, `.with_information_schema(True)`, plus `.set("...","...")`.
  * Spill-enabled runtime: `RuntimeEnvBuilder().with_disk_manager_os().with_fair_spill_pool(bytes)` (enables out-of-core paths). ([Apache DataFusion][12])
* **Value proposition**

  * Agents can force deterministic parallelism and avoid accidental repartition explosions after joins/unions. ([Apache DataFusion][12])
* **Watchouts**

  * Over-parallelizing small data increases overhead; under-parallelizing big joins increases per-partition memory and spill risk (tie to runtime memory/spill configs). ([Apache DataFusion][2])

### B) Manual repartitioning on DataFrames (Python)

* **Syntax utilization**

  * `df.repartition(num_partitions)`
  * `df.repartition_by_hash(col("a"), num=num_partitions)` ([Apache DataFusion][12])
* **Value proposition**

  * Makes plan combination “shape-stable”: you can pre-hash on future join keys before combining subplans, reducing redundant repartitions. ([Apache DataFusion][12])
* **Watchouts**

  * Hash repartition must match downstream join keys to pay off; otherwise you add shuffle cost for no benefit.

---

## 6) Planning/execution concurrency + memory/spill knobs (high impact on big UNIONs / multi-branch plans)

### A) Parallel planning of UNION children

* **Syntax utilization**

  * Config: `datafusion.execution.planning_concurrency` (“mostly used to plan UNION children in parallel”). ([Apache DataFusion][2])
* **Value proposition**

  * For “many-input union graphs”, planning can dominate; this reduces end-to-end latency for agent-driven compilation pipelines. ([Apache DataFusion][2])
* **Watchouts**

  * Parallel planning increases CPU pressure and may stress metadata caches; pair with metadata cache limits. ([Apache DataFusion][2])

### B) Spill behavior + compression + spill file sizing

* **Syntax utilization**

  * `SET datafusion.runtime.memory_limit = '2G'` (or programmatic) and configure disk manager for spill. ([Apache DataFusion][2])
  * `datafusion.execution.spill_compression`, `datafusion.execution.max_spill_file_size_bytes`, `datafusion.execution.sort_spill_reservation_bytes`. ([Apache DataFusion][2])
* **Value proposition**

  * Combined joins/aggregations that would OOM become executable (predictable degradation vs failure). ([Apache DataFusion][2])
* **Watchouts**

  * Higher `target_partitions` can *increase* spill frequency by shrinking memory per partition (FairSpillPool behavior). ([Apache DataFusion][2])

### C) Runtime caches that matter when repeatedly reusing/expanding subplans/views

* **Syntax utilization**

  * `datafusion.runtime.metadata_cache_limit`, `datafusion.runtime.list_files_cache_limit`, `datafusion.runtime.list_files_cache_ttl`. ([Apache DataFusion][2])
* **Value proposition**

  * Replanning + rescanning repeated view-based combined plans gets cheaper (metadata IO amortization). ([Apache DataFusion][2])
* **Watchouts**

  * Cache limits trade memory for reduced object-store calls; tune alongside spill/temp directory constraints. ([Apache DataFusion][2])

### D) CLI-level profiling for combined plan IO

* **Syntax utilization**

  * `datafusion-cli --memory-limit ... --disk-limit ... --object-store-profiling {disabled|summary|trace}`; in-session `\object_store_profiling trace`. ([Apache DataFusion][13])
* **Value proposition**

  * Fast attribution: “combined plan is slow because of IO shape” vs “CPU/operator hotspot”. ([Apache DataFusion][13])
* **Watchouts**

  * Trace mode is verbose; treat outputs as artifacts, not as logs to parse ad-hoc.

---

## 7) Optimizer toggles that reshape combined plans (join/union/sort/window heavy graphs)

### A) Dynamic filter pushdown family

* **Syntax utilization**

  * `SET datafusion.optimizer.enable_dynamic_filter_pushdown = 'true'` and/or specific toggles: `enable_topk_dynamic_filter_pushdown`, `enable_join_dynamic_filter_pushdown`, `enable_aggregate_dynamic_filter_pushdown`. ([Apache DataFusion][2])
* **Value proposition**

  * In combined plans with TopK / joins / group-bys, runtime-derived filters can prune scans earlier (esp. Parquet/partitioned layouts). ([Apache DataFusion][2])
* **Watchouts**

  * Interacts with in-list pushdown sizing (`hash_join_inlist_pushdown_max_size`, `_max_distinct_values`) → memory amplification is per-partition. ([Apache DataFusion][2])

### B) Sort pushdown + sort parallelism controls

* **Syntax utilization**

  * `datafusion.optimizer.enable_sort_pushdown`, `datafusion.optimizer.repartition_sorts`, `datafusion.optimizer.prefer_existing_sort`. ([Apache DataFusion][2])
* **Value proposition**

  * Combined `ORDER BY … LIMIT N` plans can become TopK-ish and reduce scan work by reversing read order where possible. ([Apache DataFusion][2])
* **Watchouts**

  * Sort pushdown yields *inexact* ordering guarantees unless followed by Sort for correctness (DataFusion keeps Sort for correctness but enables early termination). ([Apache DataFusion][2])

### C) Unbounded sources + symmetric join pruning

* **Syntax utilization**

  * `datafusion.optimizer.allow_symmetric_joins_without_pruning` controls whether symmetric hash joins run on unbounded sources without pruning. ([Apache DataFusion][2])
* **Value proposition**

  * Prevents “combined streaming join graph buffers forever” failure modes. ([Apache DataFusion][2])
* **Watchouts**

  * If disabled incorrectly, some join types only produce outputs at end-of-stream; if enabled incorrectly, you can still OOM without a long-run design. ([Apache DataFusion][2])

### D) Recursive CTE enablement (config-gated)

* **Syntax utilization**

  * `SET datafusion.execution.enable_recursive_ctes = true;` (if not already enabled); Rust builder also supports `to_recursive_query(name, recursive_term, is_distinct)` semantics. ([Apache DataFusion][2])
* **Value proposition**

  * Enables recursive plan graphs (graph-like expansions) without host-language loops (still “one plan”). ([Apache DataFusion][2])
* **Watchouts**

  * Recursive CTEs can buffer unbounded data if misused (reason for config gating in historical discussions). ([GitHub][14])

---

## 8) Plan rewriting as an agent primitive (Rust): TreeNode API

* **Syntax utilization**

  * `TreeNode` methods on `LogicalPlan` / `ExecutionPlan` / `Expr`: `apply`, `exists`, `transform`, `transform_down`, `transform_up`, `transform_down_up`, `rewrite`, `visit`. ([Docs.rs][15])
* **Value proposition**

  * Agents can implement deterministic plan-normalization passes before combination:

    * enforce “projection before cache”
    * strip redundant sorts
    * inject safety filters
    * canonicalize join key expressions
* **Implementation considerations / watchouts**

  * Use `transform_*` APIs instead of ad-hoc recursion; they’re designed to avoid unnecessary cloning. ([Docs.rs][15])
  * Prefer logical rewriting over physical rewriting unless you own execution invariants (physical plans are config/data-layout sensitive). ([Apache DataFusion][3])

---

## 9) Delta Lake ↔ DataFusion deeper integration (read + write + file-level control)

### A) Rust: DeltaTableProvider as a full TableProvider (scan + insert)

* **Syntax utilization**

  * Build provider: `DeltaTableProvider::try_new(snapshot, log_store, DeltaScanConfig{...})`
  * Restrict scan to a chosen file set: `.with_files(files)` (advanced use cases). ([Docs.rs][16])
  * Write via DataFusion: provider implements `TableProvider::insert_into(..., insert_op)`; “Insert operation is only supported for Append and Overwrite”. ([Docs.rs][16])
* **Value proposition**

  * “Combined plan → transactional Delta commit” via native `INSERT INTO delta_table SELECT ...` pipelines, without leaving the DataFusion execution engine. ([Apache DataFusion][10])
* **Watchouts**

  * Insert semantics are limited (Append/Overwrite only); anything beyond that is delta-rs Ops territory (merge/update/delete) outside `insert_into`. ([Docs.rs][16])

### B) Rust: DeltaScanConfig knobs that directly affect combined-plan outputs

* **Syntax utilization**

  * `DeltaScanConfig { file_column_name: Option<String>, wrap_partition_values: bool, enable_parquet_pushdown: bool, schema_force_view_types: bool, schema: Option<Arc<Schema>> }` ([Docs.rs][17])
* **Value proposition**

  * `file_column_name` enables provenance columns (critical for agent debugging and deterministic incremental processing).
  * `enable_parquet_pushdown` controls how aggressively scan-level pruning participates in larger combined graphs. ([Docs.rs][17])
* **Watchouts**

  * `schema_force_view_types` can change string/binary view typing; mismatches can break unions/joins if the rest of the plan assumes `Utf8` vs `Utf8View`. ([Docs.rs][17])

### C) Python: register Delta as a table provider (minimum viable integration surface)

* **Syntax utilization**

  * `delta_table = DeltaTable(path); ctx.register_table("t", delta_table); df = ctx.table("t")` ([Apache DataFusion][18])
* **Value proposition**

  * Delta file-level skipping + DataFusion predicate pruning improves scan cost inside larger joins/unions (baseline reason to prefer Delta provider over raw datasets). ([Apache DataFusion][18])
* **Watchouts**

  * Requires compatible versions (“DataFusion 43.0.0 and later” + recent `deltalake`). ([Apache DataFusion][18])

### D) Delta file inventory → DataFusion scan shaping (incremental + selective recompute)

* **Syntax utilization**

  * delta-rs Python: `DeltaTable.get_add_actions(flatten=False|True)` yields the current “add actions” (files) view; `flatten` controls how partition/statistics fields are exposed. ([Delta][19])
  * Feed file subsets into Rust provider `.with_files(...)` for targeted scans. ([Docs.rs][16])
* **Value proposition**

  * Agents can build “changed-files-only” combined plans (incremental recompute) by deriving a file set from the Delta log and restricting scans. ([Delta][19])
* **Watchouts**

  * Treat “file list” as a snapshot-bound artifact; don’t reuse across versions without a stable snapshot key.

### E) Delta schema evolution / column mapping as a compatibility hazard for plan combination

* **Syntax utilization**

  * Delta “column mapping” allows logical column names to differ from Parquet physical column names (enables rename/drop without rewrite). ([Delta Lake][20])
* **Value proposition**

  * Enables long-lived tables with evolution while keeping historic files.
* **Watchouts**

  * Engines/providers must correctly interpret mapping metadata; otherwise unions/joins across versions can mis-resolve columns (agent should pin delta-rs versions and validate schemas via `SHOW COLUMNS` / `information_schema.columns`). ([Delta Lake][20])

### F) DataFusion low-level Parquet access plan hooks (relevant when Delta adds deletion vectors / advanced pruning)

* **Syntax utilization**

  * DataFusion exposes low-level pruning hooks (e.g., `ParquetAccessPlan`) for selective decoding; highlighted as enabling efficient DeltaLake reading and handling deletion vectors in downstream systems. ([Apache DataFusion][8])
* **Value proposition**

  * For very large combined plans, scan-phase reductions dominate; page/row selection can outperform higher-level pruning alone. ([Apache DataFusion][8])
* **Watchouts**

  * Advanced API: treat as “systems integration layer” (not typical DataFrame-level usage); needs dedicated conformance tests.

---

## 10) Recursive / nested-data expansion primitives that change how you “combine” relations

### A) `UNNEST` for arrays/maps + struct expansion

* **Syntax utilization**

  * `SELECT unnest(make_array(...)) AS x` (array/map → rows)
  * `SELECT unnest(struct_col) FROM ...` (struct → columns with placeholder prefix). ([Apache DataFusion][21])
* **Value proposition**

  * Lets agents combine nested data into flat relational form *inside* the plan graph (reduces pre-processing and enables optimizer pushdowns on expanded results). ([Apache DataFusion][21])
* **Watchouts**

  * Struct unnest uses placeholder-prefixed column names; agents should immediately `SELECT ... AS ...` rename to stable identifiers for downstream joins/unions. ([Apache DataFusion][21])

[1]: https://datafusion.apache.org/user-guide/sql/explain.html "EXPLAIN — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/user-guide/explain-usage.html "Reading Explain Plans — Apache DataFusion  documentation"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/user-guide/sql/information_schema.html "Information Schema — Apache DataFusion  documentation"
[6]: https://datafusion.apache.org/python/autoapi/datafusion/unparser/index.html "datafusion.unparser — Apache Arrow DataFusion  documentation"
[7]: https://docs.rs/datafusion/latest/datafusion/sql/unparser/fn.plan_to_sql.html "plan_to_sql in datafusion::sql::unparser - Rust"
[8]: https://datafusion.apache.org/blog/2024/07/24/datafusion-40.0.0/ "Apache DataFusion 40.0.0 Released - Apache DataFusion Blog"
[9]: https://datafusion.apache.org/user-guide/sql/ddl.html "DDL — Apache DataFusion  documentation"
[10]: https://datafusion.apache.org/user-guide/sql/dml.html "DML — Apache DataFusion  documentation"
[11]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html "Custom Table Provider — Apache DataFusion  documentation"
[12]: https://datafusion.apache.org/python/user-guide/configuration.html "Configuration — Apache Arrow DataFusion  documentation"
[13]: https://datafusion.apache.org/user-guide/cli/usage.html "Usage — Apache DataFusion  documentation"
[14]: https://github.com/apache/arrow-datafusion/issues/9554?utm_source=chatgpt.com "Enable recursive CTE support by default · Issue #9554"
[15]: https://docs.rs/datafusion/latest/datafusion/common/tree_node/trait.TreeNode.html "TreeNode in datafusion::common::tree_node - Rust"
[16]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaTableProvider.html "DeltaTableProvider in deltalake::delta_datafusion - Rust"
[17]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaScanConfig.html?utm_source=chatgpt.com "DeltaScanConfig in deltalake::delta_datafusion - Rust"
[18]: https://datafusion.apache.org/python/user-guide/data-sources.html?utm_source=chatgpt.com "Data Sources — Apache Arrow DataFusion documentation"
[19]: https://delta-io.github.io/delta-rs/python/api_reference.html?utm_source=chatgpt.com "API Reference — delta-rs documentation"
[20]: https://docs.delta.io/delta-column-mapping/?utm_source=chatgpt.com "Delta column mapping"
[21]: https://datafusion.apache.org/user-guide/sql/special_functions.html "Special Functions — Apache DataFusion  documentation"

## 1) Catalog / schema topology (multi-namespace plan composition)

### Rust

* **Syntax utilization**

  * Register a custom catalog: `SessionContext::register_catalog(name, Arc<dyn CatalogProvider>)`. ([Docs.rs][1])
  * Set defaults (SQL):
    `SET datafusion.catalog.default_catalog = 'cat';`
    `SET datafusion.catalog.default_schema = 'schema';` ([datafusion.apache.org][2])
  * Refer to objects (SQL): `SELECT * FROM cat.schema.table;` (DataFusion supports catalog + schema hierarchy). ([datafusion.apache.org][3])
* **Value proposition**

  * Lets agents combine plans across namespaces deterministically (no accidental “wrong table” resolution when rehydrating plans in different environments).
* **Watchouts**

  * `CREATE EXTERNAL TABLE staging.foo ...` has had a reported bug where `staging.foo` is treated as a single table name in the default schema instead of schema-qualified. Mitigation: `CREATE SCHEMA staging; SET datafusion.catalog.default_schema='staging'; CREATE EXTERNAL TABLE foo ...` ([GitHub][4])

### Python

* **Syntax utilization**

  * Default catalog/schema exist: `datafusion` / `default`. ([datafusion.apache.org][5])
  * Inspect catalog object: `ctx.catalog(name='datafusion')` (introspection surface). ([arrow.staged.apache.org][6])
* **Value proposition**

  * Agents can assert “what tables exist where” before combining two subplans (avoid silent name drift).
* **Watchouts**

  * Python bindings expose `catalog()` but (as of current docs) do **not** expose `register_catalog`; treat custom catalogs as a Rust-embedding feature. ([arrow.staged.apache.org][6])

## 2) Object stores as plan-leaf inputs (critical for combining plans over remote data)

### Rust

* **Syntax utilization**

  * `SessionContext::register_object_store(&Url, Arc<dyn ObjectStore>)` (URL-prefix routing). ([Docs.rs][1])
* **Value proposition**

  * Enables combining plans that read multiple remote locations (S3/GCS/Azure/HTTP) without pre-downloading; keeps scans as native `TableScan` leaves.
* **Watchouts**

  * URI prefix matching is exact; “no suitable object store found” style failures usually mean your registered URL prefix doesn’t match the table location’s scheme/host. ([Docs.rs][1])

### Python

* **Syntax utilization**

  * Register store: `ctx.register_object_store(scheme, store, host=None)`; plus built-ins like `AmazonS3` in `datafusion.object_store`. ([arrow.staged.apache.org][6])
* **Value proposition**

  * Lets agents combine subplans that join/union across multiple paths/buckets in one DAG.
* **Watchouts**

  * Keep scheme/host consistent with paths used in `read_parquet/register_parquet` etc (same “prefix routing” concept as Rust). ([arrow.staged.apache.org][6])

## 3) File-based registration knobs that materially change combined-plan performance

### Python

* **Syntax utilization**

  * `ctx.register_parquet(name, path, table_partition_cols=..., parquet_pruning=True, skip_metadata=True, schema=..., file_sort_order=...)` ([arrow.staged.apache.org][6])
* **Value proposition**

  * Plan combination is only “fast” if scan leaves prune aggressively; `table_partition_cols` + `parquet_pruning` + `file_sort_order` can make downstream joins/filters dramatically cheaper by enabling pushdowns and better ordering assumptions at the leaf. ([arrow.staged.apache.org][6])
* **Watchouts**

  * Incorrect `file_sort_order` (declaring data sorted when it isn’t) can produce wrong results if relied upon for ordering-sensitive ops; treat as an invariant you must prove with tests. ([datafusion.apache.org][7])

### Rust (ListingTable / ListingOptions)

* **Syntax utilization**

  * Hive partition columns on listings: `ListingOptions::with_table_partition_cols(Vec<(String, DataType)>)`. ([Docs.rs][8])
* **Value proposition**

  * Lets agents combine plans over partitioned lakes with predictable pruning and stable schema.
* **Watchouts**

  * Partition columns become “virtual” columns derived from path; mismatches between folder layout and declared types will poison joins/unions downstream.

## 4) Pushdown contracts (TableProvider) that determine whether plan combination is worth doing

### Rust (TableProvider)

* **Syntax utilization**

  * `scan(projection, filters, limit)` is where pushdowns land; projection pushdown is explicitly modeled via `projection` indices. ([Docs.rs][9])
* **Value proposition**

  * Combining plans (joins/CTEs/unions) only pays off if filters/projections/limits push into scans; otherwise you join huge intermediate data.
* **Watchouts**

  * Implementers must declare/handle pushdowns correctly; DataFusion may rewrite `Filter -> TableScan` into `TableScan(filters=...)`, changing where correctness logic must live. ([datafusion.apache.org][10])

### External indexes / file-pruning via custom providers

* **Syntax utilization**

  * Implement `supports_filter_pushdown` + `scan` to prune files using an external index. ([datafusion.apache.org][11])
* **Value proposition**

  * Enables “semantic compiler” style pre-pruning: combine large DAGs while keeping scan I/O bounded by index-driven file selection.
* **Watchouts**

  * Pushdown support must match actual evaluator capabilities; over-claiming yields incorrect results, under-claiming yields slow plans. ([datafusion.apache.org][11])

## 5) Programmatic metrics + stats (agent-grade verification for combined plans)

### Rust

* **Syntax utilization**

  * Read runtime metrics from any physical node: `ExecutionPlan::metrics() -> Option<MetricsSet>`. ([Docs.rs][12])
  * Prefer `partition_statistics(Some(i)|None)`; `statistics()` is deprecated. ([Docs.rs][12])
* **Value proposition**

  * Agents can automatically detect regressions after plan recomposition (e.g., join order changes) using `elapsed_compute`, `output_rows`, `output_batches`, etc. ([datafusion.apache.org][13])
* **Watchouts**

  * Metrics sets stabilize only after execution completes; calling early can yield incomplete metric sets. ([Docs.rs][12])

### SQL

* **Syntax utilization**

  * `EXPLAIN ANALYZE ...` prints physical plan + metrics (indent-only). ([datafusion.apache.org][14])
* **Value proposition**

  * Cheap “single artifact” for CI baselining combined-plan behavior.
* **Watchouts**

  * Operator-specific metrics coverage is still evolving (docs explicitly mark TODO). ([datafusion.apache.org][13])

## 6) Execution surfaces beyond “DataFrame.collect” (important when agents splice at plan level)

### Python

* **Syntax utilization**

  * Get physical plan: `df.execution_plan() -> ExecutionPlan`. ([datafusion.apache.org][15])
  * Execute partition directly: `ctx.execute(plan, part) -> stream`. ([arrow.staged.apache.org][6])
  * Partition-aware streaming: `df.execute_stream_partitioned() -> list[RecordBatchStream]`. ([datafusion.apache.org][15])
* **Value proposition**

  * Agents can enforce “partition-by-partition” execution semantics (e.g., stable hashing boundaries) while validating plan combination effects.
* **Watchouts**

  * `execute_stream()` returns a single partition stream; for full results across partitions, use the partitioned variant. ([datafusion.apache.org][15])

### Rust

* **Syntax utilization**

  * Execute physical plan as one stream: `datafusion::physical_plan::execute_stream(plan, task_ctx)`; or one stream per partition: `execute_stream_partitioned(...)`. ([Docs.rs][16])
* **Value proposition**

  * Lets agents run “combined physical plan fragments” under explicit TaskContext control (threading, memory pool, etc.).
* **Watchouts**

  * Physical plans are config/data-layout sensitive; caching/reusing them across sessions is far more brittle than reusing logical plans. ([Docs.rs][12])

## 7) Substrait in Rust (not just Python) for cross-process plan splicing

### Logical plans

* **Syntax utilization**

  * Produce: `datafusion_substrait::logical_plan::producer::to_substrait_plan(...)` ([Docs.rs][17])
  * Consume: `datafusion_substrait::logical_plan::consumer::from_substrait_plan(...)` (returns a DataFrame). ([Docs.rs][18])
* **Value proposition**

  * “Planner agent” can emit Substrait; “executor agent” can hydrate into a DataFrame and then *combine* with other DataFrames via join/union.
* **Watchouts**

  * Still environment-bound: table/function resolution must exist in the target SessionContext (Substrait is an IR, not a bundle of your catalog). ([Docs.rs][19])

### Physical plans (advanced)

* **Syntax utilization**

  * Produce physical rel: `datafusion_substrait::physical_plan::producer::to_substrait_rel(...)` ([Docs.rs][20])
  * Consume: `datafusion_substrait::physical_plan::consumer::from_substrait_rel(...) -> ExecutionPlan` ([Docs.rs][21])
* **Value proposition**

  * Enables “physical fragment shipping” patterns (closer to executor RPC models).
* **Watchouts**

  * Physical fragments are even more version/config sensitive; treat as tightly pinned artifacts (same DataFusion build + compatible runtime env). ([Docs.rs][12])

## 8) Extending SQL + custom operators (plan combination with non-relational primitives)

### Extend SQL parsing/planning (Expr/Relation/Type planners)

* **Syntax utilization**

  * DataFusion extension points: `ExprPlanner`, `RelationPlanner`, `TypePlanner` with registration methods like `ctx.register_expr_planner()`, `ctx.register_relation_planner()`, `SessionStateBuilder::with_type_planner()`. ([datafusion.apache.org][22])
* **Value proposition**

  * Agents can express domain-native constructs (e.g., `TABLESAMPLE`, `PIVOT`, custom operators) that compile into standard LogicalPlan DAGs, so they’re composable with joins/unions/CTEs. ([datafusion.apache.org][22])
* **Watchouts**

  * Planner precedence is “last registered wins”; return `Original(...)` to delegate (ordering bugs show up as “why did my construct parse but not plan”). ([datafusion.apache.org][22])

### Extend operators via optimizer rules (LogicalPlan/ExecutionPlan rewrites)

* **Syntax utilization**

  * Custom optimizer rules can rewrite LogicalPlan/ExecutionPlan (µWheel example rewrites into MemTable-backed TableScan). ([datafusion.apache.org][23])
* **Value proposition**

  * Lets agents “combine plans” with *plan-time* acceleration: rewrite expensive subgraphs into precomputed scans (semantic caching / index substitution).
* **Watchouts**

  * Rewrite rules must preserve semantics; build golden plan-diff + result-diff harnesses (especially for time/window/NULL semantics). ([datafusion.apache.org][23])

## 9) Dialect + identifier normalization (parsing correctness affects plan combination)

* **Syntax utilization**

  * Set SQL dialect: `SET datafusion.sql_parser.dialect = 'PostgreSQL'|'DuckDB'|...` ([datafusion.apache.org][2])
  * Parameter binding correctness depends on configured dialect (Python `param_values`). ([datafusion.apache.org][24])
* **Value proposition**

  * Agents can emit dialect-specific SQL safely (quoting rules, functions, etc.), then combine subqueries/CTEs without parse failures.
* **Watchouts**

  * `datafusion.sql_parser.enable_ident_normalization` has had reported inconsistency when set false; don’t assume perfect “case-sensitive mode” without tests. ([GitHub][25])

## 10) UDF/UDAF/UDWF/UDTF registry consistency (plan-combination dependency)

### Rust

* **Syntax utilization**

  * Helpers: `create_udf`, `create_udaf`, `create_udwf` (expr_fn module includes these builders). ([Docs.rs][26])
  * Register in context (names lowercased in SQL unless quoted). ([Docs.rs][1])
* **Value proposition**

  * Agents can “combine plans” that rely on shared function semantics (e.g., same UDAF in multiple subplans) without embedding logic in SQL strings.
* **Watchouts**

  * Name normalization: `SELECT MY_UDAF(x)` looks up `"my_udaf"` unless quoted. ([Docs.rs][1])

### Python (incl. UDTF)

* **Syntax utilization**

  * `ctx.register_udf(...)`, `ctx.register_udaf(...)`, `ctx.register_udwf(...)`, `ctx.register_udtf(...)`. ([datafusion.apache.org][27])
  * UDTF restriction: accepts `Expr` args but **only literal expressions are supported**; returns a `TableProvider`. ([datafusion.apache.org][28])
* **Value proposition**

  * UDTFs let agents inject “table-producing” primitives directly into FROM-clause plan leaves (compose with joins/unions immediately).
* **Watchouts**

  * Rust-backed UDTF exposed to Python requires `PyCapsule` plumbing (`__datafusion_table_function__` hook). ([datafusion.apache.org][28])

## 11) Delta + DataFusion write-path semantics (append/overwrite via TableProvider)

* **Syntax utilization (Rust)**

  * `DeltaTableProvider` implements `TableProvider::insert_into` but only supports `Append` and `Overwrite`. ([Docs.rs][29])  *(note: insert support surfaced in provider docs; keep pinned to your delta-rs version)*
* **Value proposition**

  * Enables “combined plan → INSERT INTO delta_table SELECT ...” pipelines without detouring through Arrow materialization and separate writers.
* **Watchouts**

  * Anything beyond append/overwrite (MERGE/UPDATE/DELETE semantics) lives outside generic `insert_into` and must be handled by delta-rs operations APIs (treat as separate execution mode). ([Docs.rs][20])

## 12) Tracing/telemetry for combined plans (agent debugging + regression attribution)

* **Syntax utilization (Rust)**

  * `datafusion-tracing` instruments planning + optimization + execution; integrates with OpenTelemetry; hooks via `SessionStateBuilder::with_physical_optimizer_rule(...)` and helper macros like `instrument_rules_with_info_spans!`. ([GitHub][30])
* **Value proposition**

  * Agents can attribute “why did combining these plans get slower?” to a specific optimizer rule, repartition, or operator hotspot, and export traces to Jaeger/DataDog.
* **Watchouts**

  * Not an official ASF release; pin versions to your DataFusion major (example shows `datafusion = "52.0.0"` with matching `datafusion-tracing`). ([GitHub][30])

[1]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html?utm_source=chatgpt.com "SessionContext in datafusion::execution::context - Rust"
[2]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[3]: https://datafusion.apache.org/library-user-guide/catalogs.html "Catalogs, Schemas, and Tables — Apache DataFusion  documentation"
[4]: https://github.com/apache/datafusion/issues/12607?utm_source=chatgpt.com "CREATE EXTERNAL TABLE does not support schema ..."
[5]: https://datafusion.apache.org/python/user-guide/data-sources.html?utm_source=chatgpt.com "Data Sources — Apache Arrow DataFusion documentation"
[6]: https://arrow.staged.apache.org/datafusion-python/generated/datafusion.SessionContext.html "datafusion.SessionContext — Apache Arrow DataFusion  documentation"
[7]: https://datafusion.apache.org/user-guide/sql/ddl.html?utm_source=chatgpt.com "DDL — Apache DataFusion documentation"
[8]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingOptions.html?utm_source=chatgpt.com "ListingOptions in datafusion::datasource::listing - Rust"
[9]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html?utm_source=chatgpt.com "TableProvider in datafusion::datasource - Rust"
[10]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html?utm_source=chatgpt.com "Custom Table Provider — Apache DataFusion documentation"
[11]: https://datafusion.apache.org/blog/2025/08/15/external-parquet-indexes/?utm_source=chatgpt.com "Using External Indexes, Metadata Stores, Catalogs and ..."
[12]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html "ExecutionPlan in datafusion::physical_plan - Rust"
[13]: https://datafusion.apache.org/user-guide/metrics.html "Metrics — Apache DataFusion  documentation"
[14]: https://datafusion.apache.org/user-guide/sql/explain.html?utm_source=chatgpt.com "EXPLAIN — Apache DataFusion documentation"
[15]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html?utm_source=chatgpt.com "datafusion.dataframe"
[16]: https://docs.rs/datafusion/latest/datafusion/physical_plan/fn.execute_stream.html?utm_source=chatgpt.com "execute_stream in datafusion::physical_plan - Rust"
[17]: https://docs.rs/datafusion-substrait/34.0.0/datafusion_substrait/logical_plan/producer/index.html "datafusion_substrait::logical_plan::producer - Rust"
[18]: https://docs.rs/datafusion-substrait/34.0.0/datafusion_substrait/logical_plan/consumer/index.html "datafusion_substrait::logical_plan::consumer - Rust"
[19]: https://docs.rs/crate/datafusion-substrait/34.0.0 "datafusion-substrait 34.0.0 - Docs.rs"
[20]: https://docs.rs/datafusion-substrait/34.0.0/datafusion_substrait/physical_plan/producer/index.html "datafusion_substrait::physical_plan::producer - Rust"
[21]: https://docs.rs/datafusion-substrait/34.0.0/datafusion_substrait/physical_plan/consumer/index.html "datafusion_substrait::physical_plan::consumer - Rust"
[22]: https://datafusion.apache.org/library-user-guide/extending-sql.html "Extending SQL Syntax — Apache DataFusion  documentation"
[23]: https://datafusion.apache.org/library-user-guide/extending-operators.html "Extending Operators — Apache DataFusion  documentation"
[24]: https://datafusion.apache.org/python/user-guide/sql.html?utm_source=chatgpt.com "SQL — Apache Arrow DataFusion documentation"
[25]: https://github.com/apache/datafusion/issues/18675?utm_source=chatgpt.com "Issue #18675 · apache/datafusion"
[26]: https://docs.rs/datafusion/latest/datafusion/logical_expr/expr_fn/index.html?utm_source=chatgpt.com "datafusion::logical_expr::expr_fn - Rust"
[27]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html?utm_source=chatgpt.com "datafusion.context — Apache Arrow DataFusion documentation"
[28]: https://datafusion.apache.org/python/user-guide/common-operations/udf-and-udfa.html?utm_source=chatgpt.com "User-Defined Functions - Apache DataFusion"
[29]: https://docs.rs/datafusion/latest/datafusion/catalog/index.html?utm_source=chatgpt.com "datafusion::catalog - Rust"
[30]: https://github.com/datafusion-contrib/datafusion-tracing "GitHub - datafusion-contrib/datafusion-tracing: Integration of opentelemetry with the tracing crate"
