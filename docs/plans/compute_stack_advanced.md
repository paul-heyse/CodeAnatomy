# Compute Stack Advanced: DataFusion + Ibis + SQLGlot + PyArrow

> **Intent:** a *drop-in*, LLM-friendly reference for building a **streaming-first**, **deterministic**, **observable** relational compute substrate.
>
> **Scope:** DataFusion (Python bindings), Ibis (DataFusion backend + SQL boundary), SQLGlot (AST policy + analysis), PyArrow (physical batch + dataset IO + kernels).
>
> **Reading mode:** structured like `pyarrow-advanced.md`: **mental model → full surface area → minimal runnable snippets → failure modes**.
>
> **Versioning note:** treat this as an *interface inventory + tuning playbook*, not a claim that any specific version is installed. Always pin versions.

---

## 0) System invariants (what “best-in-class” means for this stack)

### 0.1 Streaming-first invariant

**Default outcome type is a `RecordBatchReader` (or an iterator of `RecordBatch`)**, not a materialized `pa.Table`.

* Materialize only for:
  * canonical global sorting / stable winner selection that truly requires a total order
  * downstream APIs that cannot accept a batch stream
  * debugging snapshots

### 0.2 Determinism tiers (make ordering a *policy*, not an accident)

* **Tier 0 — Fast / unspecified order**
  * `use_threads=True`, partitioned execution, parallel writes.
  * Requires *semantic* determinism only (same set of rows).

* **Tier 1 — Stable within-run ordering (cheap)**
  * Preserve scan/fragment/batch ordering where supported (Acero implicit ordering) or require sequenced output.
  * Still not a true total order across all rows unless enforced.

* **Tier 2 — Canonical order (expensive, but reproducible)**
  * Deterministic key sort (e.g., stable_id + tie-breakers) before materialization/writing.

### 0.3 What each library “owns”

* **DataFusion:** logical plan + optimizer + parallel execution + streaming result production + (optional) engine-native writers.
* **Ibis:** portable relational IR + backend compilation boundary + ergonomic SQL/relational composition.
* **SQLGlot:** deterministic AST checkpointing, rewriting, qualification, lineage, semantic diff.
* **PyArrow:** physical batch representation, compute kernels, dataset scanning/writing, Acero ExecPlan ordering controls.

---

## A) DataFusion (Python bindings) — streaming execution + engine-native IO + runtime profile knobs

### A0) Mental model

* `SessionContext` is the execution “workspace”: catalogs, registered tables, config/runtime.
* `SessionConfig` controls planning + execution behavior (partitioning, pruning/pushdown, etc.).
* `RuntimeEnvBuilder` controls memory/disk runtime (notably: **spilling requires a disk manager**).
* `DataFrame` is a *lazy logical plan*: execution happens at terminal actions (`collect`, `show`, `to_arrow_table`, streaming export).

---

### A1) API surface index (hot paths)

#### A1.1 `datafusion.context.SessionContext`

* Construction
  * `SessionContext(config: SessionConfig | None = None, runtime: RuntimeEnvBuilder | None = None)`
* Data ingress
  * `ctx.read_parquet(path, **kwargs)` / `ctx.register_parquet(name, path, **kwargs)`
  * `ctx.read_csv(path, **kwargs)` / `ctx.register_csv(...)`
  * `ctx.read_json(path, **kwargs)` / `ctx.register_json(...)`
  * `ctx.from_pydict(mapping, name=None)`
  * `ctx.from_arrow(obj)` (accepts objects implementing Arrow C interfaces)
* Query surfaces
  * `ctx.sql(sql_text: str) -> DataFrame`
  * `ctx.table(name: str) -> DataFrame`

(See DataFusion Python basics + IO docs.) ([DataFusion Python][1])

#### A1.2 `datafusion.dataframe.DataFrame` — execution + streaming

* Terminal materialization
  * `collect()` / `collect_partitioned()`
  * `to_arrow_table()` / `to_pandas()` / `to_polars()`
  * `show()` / `count()`

* Streaming surfaces
  * `__arrow_c_stream__(requested_schema=None) -> PyCapsule` (**Arrow C Stream**) ([DataFusion DF API][2])
  * `execute_stream()` (**single stream**) ([DataFusion DF API][2])
  * `execute_stream_partitioned()` (**stream per partition**) ([DataFusion DF API][2])
  * `__iter__()` yields record batches lazily; `__aiter__()` async batches ([DataFusion DF API][2])

* Introspection
  * `logical_plan()` / `optimized_logical_plan()`
  * `execution_plan()`
  * `explain()`

#### A1.3 Engine-native writers

* `write_parquet(path)` / `write_parquet_with_options(path, parquet_options, write_options)`
* `write_csv(path)` / `write_json(path)`
* Writer option classes:
  * `DataFrameWriteOptions(partition_by=..., sort_by=..., insert_operation=..., single_file_output=...)`
  * `ParquetWriterOptions(...)`
  * `ParquetColumnOptions(...)`

(See DataFusion DataFrame API.) ([DataFusion DF API][2])

---

### A2) Streaming execution: `__arrow_c_stream__`, `execute_stream`, partitioned streaming

#### A2.1 Arrow C Stream: what it buys you

`DataFrame.__arrow_c_stream__()` exposes results via Arrow’s C Stream interface:

* Execution is **lazy** and **pull-driven**: consumers request batches on demand.
* Bounded memory: DataFusion does not need to materialize the entire result set.

**Requested schema limitation**

When `requested_schema` is provided, DataFusion only applies **simple projections**:

* selecting a subset of existing columns
* reordering existing columns

It does **not** support:

* renaming columns
* computed expressions
* type coercions

(These behaviors are explicitly documented in the DataFusion Python API.) ([DataFusion DF API][2])

#### A2.2 Minimal streaming patterns

**(1) Stream DataFusion into PyArrow as a `RecordBatchReader`**

```python
from datafusion import SessionContext
import pyarrow as pa

ctx = SessionContext()
df = ctx.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]})

reader = pa.RecordBatchReader.from_stream(df)  # uses __arrow_c_stream__
for batch in reader:
    assert isinstance(batch, pa.RecordBatch)
    # process batch
```

(Arrow C stream is used by `RecordBatchReader.from_stream`.) ([DataFusion Arrow IO][3])

**(2) Stream and write a dataset without materializing**

```python
from datafusion import SessionContext
import pyarrow as pa
import pyarrow.dataset as ds

ctx = SessionContext()
df = ctx.from_pydict({"a": [1, 2, 3]})

reader = pa.RecordBatchReader.from_stream(df)

ds.write_dataset(
    reader,
    base_dir="out_dir",
    format="parquet",
    use_threads=True,
    preserve_order=False,
)
```

(PyArrow dataset writer accepts `RecordBatchReader` as input.) ([Arrow write_dataset][4])

#### A2.3 Partitioned streaming: when you want it

`execute_stream_partitioned()` gives you “one stream per output partition”. This is useful when:

* you want to fan-out downstream processing per partition
* you want to pipe partitions to independent writers
* you want to merge in a controlled way

**Caution:** if you combine partitioned streaming with parallel writers, you will get **Tier 0 ordering** unless you add explicit ordering columns or a canonical sort.

---

### A3) Engine-native writes: `write_parquet_with_options` and friends

#### A3.1 When to use DataFusion writers vs PyArrow writers

**Prefer DataFusion writers when:**

* the output is large and you want to keep all execution + write inside the engine
* you want DataFusion to handle multi-file output parallelism
* you want plan-level `sort_by` at write-time for determinism

**Prefer `pyarrow.dataset.write_dataset` when:**

* you need PyArrow-side hooks (`file_visitor`, custom filesystem integration, sidecar metadata)
* you already have a `RecordBatchReader` from non-DataFusion producers
* you want a unified writer across multiple upstream engines

#### A3.2 `DataFrameWriteOptions` (design-critical fields)

* `partition_by=[...]`
  * directory partitioning layout
* `sort_by=[...]`
  * forces a sort at write-time (use for Tier 2 determinism)
* `single_file_output=bool`
  * debugging / interchange; avoid for large outputs
* `insert_operation` (append/overwrite/replace semantics for table writes)

(DataFusion Python exposes these via `DataFrameWriteOptions`.) ([DataFusion DF API][2])

#### A3.3 `ParquetWriterOptions` / `ParquetColumnOptions` (high leverage)

Common throughput vs size knobs:

* `compression` + `compression_level`
* `max_row_group_size`
* `write_batch_size`
* `dictionary_enabled`

Indexing/pruning knobs:

* `statistics_enabled` (+ truncate lengths)
* bloom filter knobs (`bloom_filter_on_write`, `bloom_filter_fpp`, `bloom_filter_ndv`)
* per-column overrides via `column_specific_options` (for hot columns)

(See `ParquetWriterOptions` and `ParquetColumnOptions` in the API.) ([DataFusion DF API][2])

---

### A4) Runtime profile knobs (SessionConfig + RuntimeEnvBuilder)

#### A4.1 “Two planes” of configuration

1) **Typed convenience methods** on `SessionConfig`:

* `with_target_partitions(n)`
* `with_repartition_joins(bool)` / `with_repartition_aggregations(bool)` / `with_repartition_windows(bool)`
* `with_parquet_pruning(bool)`

2) **Full key/value access** via `SessionConfig.set(key, value)`

Example from the DataFusion Python configuration guide:

```python
from datafusion import SessionConfig

config = (
    SessionConfig()
    .with_target_partitions(8)
    .with_parquet_pruning(True)
    .set("datafusion.execution.parquet.pushdown_filters", "true")
)
```

(The `.set(...)` pattern is documented.) ([DataFusion Config (Py)][5])

#### A4.2 Core knobs you should treat as first-class in your own “RuntimeProfile”

**Parallelism + partitioning**

* `datafusion.execution.target_partitions` (default: CPU cores) ([DF Config Keys][6])
* `datafusion.optimizer.repartition_joins / repartition_aggregations / repartition_windows` (defaults: true) ([DF Config Keys][6])

**Batch sizing**

* `datafusion.execution.batch_size` (default: 8192) ([DF Config Keys][6])
* `datafusion.execution.coalesce_batches` (default: true) ([DF Config Keys][6])

**Parquet scan pushdown/pruning**

* `datafusion.execution.parquet.pruning` (default: true) ([DF Config Keys][6])
* `datafusion.execution.parquet.pushdown_filters` (default: false) ([DF Config Keys][6])
* `datafusion.execution.parquet.enable_page_index` (default: true) ([DF Config Keys][6])
* `datafusion.execution.parquet.metadata_size_hint` (default: 524288) ([DF Config Keys][6])

**Spill / disk-backed execution**

* Spilling requires a configured disk manager (`RuntimeEnvBuilder().with_disk_manager_os()`). ([DataFusion Config (Py)][5])
* Sort spill + spill file sizing knobs are in the config catalog (e.g., `datafusion.execution.max_spill_file_size_bytes`). ([DF Config Keys][6])

**EXPLAIN ANALYZE verbosity**

* `datafusion.explain.analyze_level` (`summary` vs `dev`, default `dev`). ([DF Config Keys][6])

---

### A5) Optimizer interactions you should “design for”

#### A5.1 Dynamic filter pushdown (TopK / Join / Aggregate)

Key toggles:

* `datafusion.optimizer.enable_dynamic_filter_pushdown`
* `datafusion.optimizer.enable_join_dynamic_filter_pushdown`
* `datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown`
* `datafusion.optimizer.enable_topk_dynamic_filter_pushdown`

(All are documented config keys.) ([DF Config Keys][6])

**Architecture implication:** make these part of your runtime profile and include them in cache keys / run manifests.

**Robustness implication:** treat dynamic filtering as a *feature flag*.

* DataFusion 50 introduced “dynamic filter pushdown for hash joins” as a major feature. ([DF 50 release][7])
* There have been bug reports around dynamic filters + certain predicates; always keep a “disable” path and a correctness regression test harness.

#### A5.2 Parquet metadata cache

DataFusion 50 introduced a Parquet metadata cache feature. ([DF 50 release][7])

**Operational pattern:** metadata caches improve repeated scans over remote storage; they also consume memory. Treat memory limits as a profile knob.

#### A5.3 Spilling sorts for larger-than-memory datasets

DataFusion 50 improved spilling sorts. ([DF 50 release][7])

**Operational pattern:** spilling only helps if you actually configure a disk manager runtime; otherwise the plan may still OOM.

---

### A6) Observability + reproducibility artifacts

#### A6.1 Explain plans

* SQL: `EXPLAIN [ANALYZE] [VERBOSE] [FORMAT ...] statement` ([DF EXPLAIN][8])
* Python: `df.explain()`

Prefer to capture:

* **optimized logical plan** (post-optimizer)
* **physical plan**
* **EXPLAIN ANALYZE metrics** (tiered by `datafusion.explain.analyze_level`)

#### A6.2 “Bundle-friendly” plan artifacts

In an “agent-grade” system, store these per-rule/per-product:

* text explain
* `optimized_logical_plan()` output
* `execution_plan()` output
* configuration snapshot (all relevant `datafusion.*` keys)

If you already serialize Substrait, include it as the canonical “portable plan.”

---

### A7) Named arguments in functions (DataFusion 51)

DataFusion 51 added PostgreSQL-style named arguments (`param => value`) for scalar/aggregate/window functions, with improved diagnostics (parameter names in errors), and support for UDF parameter names. ([DF 51 release][9])

**Why you care in a rules engine**

* more stable SQL generation (order-independent argument binding)
* better error messages (especially for LLM-authored SQL)
* supports a cleaner “FunctionFactory” surface (name args explicitly)

---

### A8) Failure-mode playbook (DataFusion)

**1) Wrong results / surprising join behavior**

* Symptom: result cardinality mismatch vs expected.
* Fast mitigation: disable join dynamic filter pushdown.

```sql
SET datafusion.optimizer.enable_join_dynamic_filter_pushdown = 'false';
```

(Config keys are listed in the catalog.) ([DF Config Keys][6])

**2) OOM during sort / join / aggregation**

* Mitigations:
  * ensure spilling is enabled via `RuntimeEnvBuilder().with_disk_manager_os()` ([DataFusion Config (Py)][5])
  * reduce `datafusion.execution.batch_size`
  * reduce `target_partitions` (less concurrent memory)
  * enable/keep `coalesce_batches=true` for selective pipelines

**3) Remote parquet scans are IO-heavy**

* Mitigations:
  * keep `parquet.enable_page_index=true`
  * keep `parquet.pruning=true`
  * tune `parquet.metadata_size_hint` to avoid extra reads (default is 512KiB) ([DF Config Keys][6])

---

## B) Ibis — streaming materialization, SQLGlot checkpointing, UDF ladder, schema/DDL hooks

### B0) Mental model

* Ibis expressions are a relational IR.
* SQL backends compile expressions into SQL (via SQLGlot) and execute on the backend.
* For the DataFusion backend, you can connect either with default settings **or** pass an existing `datafusion.SessionContext`.

---

### B1) API surface index (DataFusion backend + table expressions)

#### B1.1 Connect to DataFusion

* Install extra: `pip install 'ibis-framework[datafusion]'`
* Connect:
  * `con = ibis.datafusion.connect()`
  * `con = ibis.datafusion.connect(ctx)` where `ctx` is a `datafusion.SessionContext`.

(The backend docs explicitly document `config` as either a mapping or a `SessionContext`.) ([Ibis DataFusion backend][10])

#### B1.2 Backend “escape hatches”

* `con.raw_sql(query)` where `query` can be:
  * `str`
  * `sqlglot.expressions.Expression` (aka `sge.Expression`)

(Explicitly documented.) ([Ibis DataFusion backend][10])

* `con.sql(query, schema=None, dialect=None) -> ir.Table`
  * build an Ibis expression from raw SQL
  * dialect override is useful when query dialect differs from backend

(Explicitly documented.) ([Ibis DataFusion backend][10])

#### B1.3 Table expression IO / compilation

From table expression docs:

* `t.to_pyarrow_batches(...) -> pyarrow.RecordBatchReader`
* `t.to_parquet_dir(directory, ...)` (routes to `pyarrow.dataset.write_dataset` under the hood)
* `t.to_sql()` (compile)

(See table expression reference.) ([Ibis table expressions][11])

#### B1.4 Global SQL compilation knobs

* `ibis.options.sql.fuse_selects: bool`
  * whether to fuse consecutive selects into one query

(See Ibis SQL options.) ([Ibis SQL options][12])

---

### B2) Streaming materialization: `to_pyarrow_batches`

`Table.to_pyarrow_batches(...)` yields a `pyarrow.RecordBatchReader`.

* Useful for:
  * streaming into `pyarrow.dataset.write_dataset`
  * batch-by-batch transformation pipelines

Minimal pattern:

```python
import ibis
import pyarrow.dataset as ds

con = ibis.datafusion.connect()
t = con.read_parquet("in.parquet")

reader = t.to_pyarrow_batches(chunk_size=1_000_000)

ds.write_dataset(reader, base_dir="out", format="parquet")
```

(Table expression IO methods are documented.) ([Ibis table expressions][11])

**Caveat:** batch iterators can be sensitive to side effects on the same connection during iteration (there are reported backend-specific edge cases). Treat streaming readers as “consume fully before mutating backend state.”

---

### B3) SQLGlot boundary: deterministic AST checkpointing + rewrite hooks

#### B3.1 `raw_sql(sge.Expression)` = “compile boundary you control”

If you already have SQLGlot ASTs (e.g., from your rewrite pipeline), you can execute them directly:

```python
import ibis
import sqlglot.expressions as sge

con = ibis.datafusion.connect()
expr = sge.select("a").from_("mytable").where(sge.column("a").gt(0))
con.raw_sql(expr)
```

(Backend `raw_sql` accepts `sge.Expression`.) ([Ibis DataFusion backend][10])

#### B3.2 Compiler checkpoint pattern (recommended)

1) Build Ibis expression
2) Compile to SQLGlot expression (internal compiler step)
3) Apply SQLGlot policy transforms (qualify, normalize identifiers, canonical rewrites)
4) Execute via `raw_sql(sge.Expression)`

This removes string-SQL fragility and gives you stable plan fingerprints.

#### B3.3 `ibis.options.sql.fuse_selects` as a performance/debug dial

* Turn **on** for fewer subqueries and often better backend optimization.
* Turn **off** when debugging or working around planner/aliasing edge cases.

(Option is documented.) ([Ibis SQL options][12])

---

### B4) UDF ladder: `builtin` → `pyarrow`/`pandas` → `python`

Ibis scalar UDF constructors:

* `ibis.udf.scalar.builtin`
* `ibis.udf.scalar.pyarrow` (vectorized over Arrow arrays)
* `ibis.udf.scalar.pandas` (vectorized over Series)
* `ibis.udf.scalar.python` (scalar row-by-row; slowest)

(See scalar UDF reference.) ([Ibis scalar UDFs][13])

**Best-in-class policy**

1) Prefer **builtin** whenever the backend has the function.
2) Prefer **pyarrow** for vectorized Python UDFs that can be expressed via `pyarrow.compute`.
3) Use **python** only as a correctness fallback.

This is the “high-quality intermediate” step before Rust/native UDFs.

---

### B5) Schema ↔ SQLGlot: contract-driven DDL and schema checks

`ibis.Schema` supports:

* `Schema.from_sqlglot(schema, dialect=None)`
* `Schema.to_sqlglot_column_defs(dialect)` (preferred)

(See schema reference.) ([Ibis schemas][14])

**How to use in a rules engine**

* treat dataset contracts as the source of truth
* generate SQLGlot `ColumnDef` nodes for DDL and for validating compiled query outputs

---

### B6) Failure-mode playbook (Ibis)

**1) SQL compilation produces backend-incompatible SQL**

* Mitigations:
  * set `ibis.options.sql.fuse_selects = False`
  * inspect `t.to_sql()`
  * fallback to DataFusion `ctx.sql(...)` for backend-specific features

**2) Schema inference is wrong / unstable**

* Mitigation: pass `schema=` explicitly when using `con.sql(query, schema=...)`.

(Backend `sql` supports `schema`.) ([Ibis DataFusion backend][10])

**3) UDF performance is poor**

* Mitigation ladder:
  * rewrite as builtin
  * rewrite as `@ibis.udf.scalar.pyarrow` calling `pyarrow.compute`
  * only then consider native/Rust UDFs

---

## C) SQLGlot — deterministic AST policy layer (qualify, normalize, lineage, diff)

### C0) Mental model

* SQLGlot parses SQL into an AST (`exp.Expression`).
* You can transform the AST via optimizer utilities.
* You can render back into a target dialect with `.sql(dialect=...)`.

---

### C1) API surface index

* Parse / render
  * `sqlglot.parse_one(sql, dialect=...) -> exp.Expression`
  * `.sql(dialect=...)`

* Qualification (normalize tables/columns)
  * `sqlglot.optimizer.qualify.qualify(expression, schema=..., dialect=..., **kwargs)`

* Identifier normalization
  * `sqlglot.optimizer.normalize_identifiers.normalize_identifiers(expression, dialect=..., **kwargs)`

* Predicate normalization distance
  * `sqlglot.optimizer.normalize.normalization_distance(expression, dnf=False, max_=...)`

* Column lineage
  * `sqlglot.lineage.lineage(column, sql_or_expr, schema=..., dialect=..., trim_selects=True, **kwargs)`

* Semantic diff
  * `sqlglot.diff.diff(source_expr, target_expr, ...)` (edit-script style diff)

(See SQLGlot API docs.) ([SQLGlot qualify][15], [SQLGlot normalize][16], [SQLGlot normalize_identifiers][17], [SQLGlot lineage][18], [SQLGlot diff][19])

---

### C2) Qualification: make it an invariant

`qualify(...)` rewrites AST so tables/columns are normalized and qualified, and is described as necessary for further SQLGlot optimizations.

Key parameters you will actually use in a compiler pipeline:

* `schema=`
  * mapping that disambiguates columns and enables reliable rewrites
* `dialect=`
  * qualification rules are dialect-aware
* `validate_qualify_columns=`
  * fail fast when columns are ambiguous or missing
* `quote_identifiers` / `identify`
  * stabilizes output quoting behavior
* `canonicalize_table_aliases`
  * reduces alias-related diff noise
* `on_qualify=` callback
  * hook to enforce policies (e.g., reject unqualified references)

(See qualify API docs.) ([SQLGlot qualify][15])

---

### C3) Identifier normalization: stabilize cache keys without breaking semantics

`normalize_identifiers(...)` converts identifiers to lower/upper case *while preserving dialect semantics*.

It explicitly supports a special comment to mark identifiers as case-sensitive:

```sql
SELECT a /* sqlglot.meta case_sensitive */ FROM t
```

(See normalize_identifiers docs.) ([SQLGlot normalize_identifiers][17])

**Policy recommendation**

* Normalize identifiers early (post-parse) to reduce cache-key churn.
* Preserve case-sensitive identifiers via annotation only when required.

---

### C4) CNF/DNF normalization cost model: `normalization_distance`

`normalization_distance(expression, dnf=False, max_=...)` estimates the cost of converting a boolean expression into CNF (default) or DNF.

(See normalize docs.) ([SQLGlot normalize][16])

**Policy recommendation**

* Never blindly CNF/DNF everything.
* Gate normalization behind:
  * `distance <= threshold`
  * rule-specific enablement (only where it improves pushdown/pruning)

---

### C5) Column lineage graphs: `lineage(...)`

`lineage(column, sql_or_expr, schema=..., dialect=..., trim_selects=True, **kwargs)` builds a column-level lineage graph.

Key knobs:

* `trim_selects=True`
  * trims selects to only relevant columns (useful for projection minimization)
* `**kwargs`
  * forwarded to qualification logic (so lineage depends on your qualify policy)

(See lineage docs.) ([SQLGlot lineage][18])

**How to use in a rules engine**

* compute required input columns for each rule (projection pushdown)
* emit “rule → inputs/outputs” artifacts for debugging and contract enforcement

---

### C6) Semantic diffs: `sqlglot.diff`

SQLGlot’s diff produces a semantic “edit script” style diff over AST structures.

(See diff docs.) ([SQLGlot diff][19])

**How to use in incremental rebuild**

* If diff changes only formatting/aliases: keep caches
* If diff changes predicates/joins/windows: invalidate dependent products

---

### C7) Failure-mode playbook (SQLGlot)

**1) Qualification fails (ambiguous columns)**

* Mitigation: provide a complete `schema=` and set `validate_qualify_columns=True`.

**2) Dialect mismatch**

* Mitigation: always set `dialect=` for parse and render.

**3) Normalization explodes expression size**

* Mitigation: use `normalization_distance(..., max_=...)` as a hard gate.

---

## D) PyArrow — Acero ordering, list explode kernels, thread pools, streaming dataset writes

### D0) Mental model

* Arrow arrays are physical columnar buffers.
* Compute kernels (`pyarrow.compute`) are vectorized and release the GIL.
* Dataset IO (`pyarrow.dataset`) is the “unified scan/write layer.”
* Acero is Arrow’s ExecPlan engine; its scan options expose ordering semantics that matter for determinism tiers.

---

### D1) API surface index

#### D1.1 Dataset writing

* `pyarrow.dataset.write_dataset(data, base_dir, format=..., partitioning=..., use_threads=..., preserve_order=..., file_visitor=..., existing_data_behavior=...)`

(See write_dataset docs.) ([Arrow write_dataset][4])

#### D1.2 Thread pools

* `pyarrow.set_cpu_count(n)` / `pyarrow.cpu_count()`
* `pyarrow.set_io_thread_count(n)` / `pyarrow.io_thread_count()`

(See thread pool docs.) ([Arrow set_cpu_count][20], [Arrow set_io_thread_count][21])

#### D1.3 Acero scan ordering

* `pyarrow.acero.ScanNodeOptions(..., require_sequenced_output=False, implicit_ordering=False, **scanner_kwargs)`

(See ScanNodeOptions docs.) ([Arrow ScanNodeOptions][22])

#### D1.4 List explode kernels (no Python loops)

* `pyarrow.compute.list_parent_indices(lists)`
* `pyarrow.compute.list_flatten(lists, recursive=False, ...)`

(See compute kernel docs.) ([Arrow list_parent_indices][23], [Arrow list_flatten][24])

---

### D2) Acero ordering: `require_sequenced_output` and `implicit_ordering`

`ScanNodeOptions` exposes two determinism-related knobs:

* `require_sequenced_output=True`
  * batches are yielded sequentially (like single-threaded)
* `implicit_ordering=True`
  * batches are augmented with fragment/batch indices to enable stable ordering for simple plans

(These are described in the ScanNodeOptions docs.) ([Arrow ScanNodeOptions][22])

**Policy recommendation**

* Use these knobs to implement **Tier 1 ordering** without a global sort.
* For Tier 2 ordering, still sort explicitly by your canonical key.

---

### D3) List explode without Python loops: `list_parent_indices` + `list_flatten`

This is the canonical “explode list column” building block:

```python
import pyarrow.compute as pc

parent_idx = pc.list_parent_indices(list_col)   # int32/64 parent row ids
values     = pc.list_flatten(list_col)          # flattened child values
```

Then align parent columns via `take`:

```python
parents = pc.take(parent_id_col, parent_idx)
```

**Edge cases to account for**

* Null list values do not emit anything in `list_flatten` output.
* Empty lists emit nothing (as expected).
* For `list<struct<...>>`, flatten yields `StructArray` values; you can then `struct_field(...)` extract columns.

(Behavior is documented in kernel docs.) ([Arrow list_parent_indices][23], [Arrow list_flatten][24])

---

### D4) Thread pools: coordinating Arrow + DataFusion

Arrow uses two pools:

* CPU pool — compute kernels
* IO pool — dataset scanning and other IO

You can set both at runtime:

```python
import pyarrow as pa

pa.set_cpu_count(4)
pa.set_io_thread_count(8)
```

(Functions are documented.) ([Arrow set_cpu_count][20], [Arrow set_io_thread_count][21])

**Policy recommendation**

* If DataFusion is doing most CPU work, keep Arrow CPU pool modest to avoid oversubscription.
* Keep Arrow IO pool sized to your storage (local SSD vs remote).

---

### D5) Streaming dataset writes: `pyarrow.dataset.write_dataset`

`write_dataset` supports streaming inputs:

* `data` can be a `RecordBatchReader`.
* `use_threads=True` writes files in parallel.
* `preserve_order=True` preserves row order but can reduce performance.
* `file_visitor` lets you capture per-file metadata (useful for `_metadata` construction).

(See write_dataset docs.) ([Arrow write_dataset][4])

**Minimal “file_visitor” pattern**

```python
import pyarrow.dataset as ds

written = []

def visitor(wf):
    written.append((wf.path, wf.metadata))  # metadata is parquet metadata or None

# ds.write_dataset(..., file_visitor=visitor)
```

(Write_dataset documents `file_visitor` and its metadata behavior.) ([Arrow write_dataset][4])

---

### D6) Failure-mode playbook (PyArrow)

**1) Output row order is not preserved**

* By default, parallelism can reorder outputs.
* Mitigations:
  * set `preserve_order=True` (Tier 1/2), or
  * write an explicit ordering column and sort canonically (Tier 2)

(Write_dataset supports `preserve_order`.) ([Arrow write_dataset][4])

**2) Oversubscription (slowdown under load)**

* Mitigation: explicitly set Arrow thread pools and coordinate with DataFusion partitions.

---

## E) Recommended defaults: RuntimeProfile knobs + heuristics

> This section is intentionally opinionated: it proposes **default profile settings** and the **degrade-gracefully switches** you should wire as feature flags.

### E1) Base profiles (dev-debug, prod-fast, memory-tight)

#### Profile: `dev_debug`

* Goal: maximum introspection, safer determinism.
* DataFusion
  * `datafusion.explain.analyze_level = 'dev'` (deep metrics) ([DF Config Keys][6])
  * keep dynamic filters enabled but easy to disable
  * `target_partitions = min(cpu, 8)` (avoid noisy parallelism)
* Arrow
  * `set_cpu_count = min(cpu, 4)`
  * `set_io_thread_count = min(cpu, 8)`
* Ibis
  * `ibis.options.sql.fuse_selects = False` (more debuggable SQL) ([Ibis SQL options][12])
* SQLGlot
  * always `qualify` + `normalize_identifiers`

#### Profile: `prod_fast`

* Goal: throughput, bounded memory, streaming.
* DataFusion
  * `target_partitions = cpu` (or 2×cpu for IO-heavy scans)
  * enable repartitioning for joins/aggs/windows
  * `datafusion.execution.batch_size = 8192` (default) or 16384 if memory allows ([DF Config Keys][6])
  * `parquet.pruning = true`, `parquet.enable_page_index = true` ([DF Config Keys][6])
  * consider `parquet.pushdown_filters = true` when predicates are selective ([DF Config Keys][6])
  * configure spilling runtime (`with_disk_manager_os`) ([DataFusion Config (Py)][5])
* Arrow
  * keep CPU pool smaller if DF is the main compute engine
* Ibis
  * `ibis.options.sql.fuse_selects = True` ([Ibis SQL options][12])

#### Profile: `memory_tight`

* Goal: survive large sorts/joins with limited RAM.
* DataFusion
  * lower `datafusion.execution.batch_size` (e.g., 2048–4096) ([DF Config Keys][6])
  * reduce `target_partitions`
  * enable spilling runtime + ensure temp disk has space
  * tune spill file sizes (`datafusion.execution.max_spill_file_size_bytes`) ([DF Config Keys][6])
* Arrow
  * keep CPU pool low

### E2) Concrete “profile builder” skeleton

```python
from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext


@dataclass(frozen=True)
class RuntimeProfile:
    name: str

    # DataFusion partitions
    target_partitions: int

    # Arrow thread pools
    arrow_cpu_threads: int
    arrow_io_threads: int

    # Spill
    spill_pool_bytes: int

    # Arbitrary DataFusion config keys
    df_settings: dict[str, str]


def build_datafusion_ctx(p: RuntimeProfile) -> SessionContext:
    pa.set_cpu_count(p.arrow_cpu_threads)
    pa.set_io_thread_count(p.arrow_io_threads)

    runtime = (
        RuntimeEnvBuilder()
        .with_disk_manager_os()
        .with_fair_spill_pool(p.spill_pool_bytes)
    )

    config = SessionConfig().with_target_partitions(p.target_partitions)

    for k, v in p.df_settings.items():
        config = config.set(k, v)

    return SessionContext(config, runtime)
```

* `.set(key, value)` is documented in the DataFusion Python configuration guide. ([DataFusion Config (Py)][5])
* `with_disk_manager_os()` / `with_fair_spill_pool(...)` is shown in the same guide. ([DataFusion Config (Py)][5])

### E3) “Default knobs” you should wire as flags

* Dynamic filter pushdown toggles:
  * `datafusion.optimizer.enable_dynamic_filter_pushdown`
  * `...enable_join_dynamic_filter_pushdown`
  * `...enable_aggregate_dynamic_filter_pushdown`
  * `...enable_topk_dynamic_filter_pushdown`

(These keys are in the config catalog.) ([DF Config Keys][6])

* Parquet read pushdowns:
  * `datafusion.execution.parquet.pruning`
  * `datafusion.execution.parquet.enable_page_index`
  * `datafusion.execution.parquet.pushdown_filters`

(These keys are in the config catalog.) ([DF Config Keys][6])

* EXPLAIN ANALYZE verbosity:
  * `datafusion.explain.analyze_level = summary|dev`

(Shown in config catalog.) ([DF Config Keys][6])

* Ibis compilation fuse knob:
  * `ibis.options.sql.fuse_selects`

(Option is documented.) ([Ibis SQL options][12])

* Arrow writer determinism knob:
  * `preserve_order=True|False` (write_dataset)

(Write_dataset docs.) ([Arrow write_dataset][4])

---

## F) Cross-stack failure-mode playbooks (degrade gracefully)

### F1) “Correctness first” fallback path

If you detect correctness regression in an optimizer feature (e.g., dynamic filters):

1) disable the feature via config
2) re-run with the same inputs
3) record the feature gate state in run manifests

Key example:

```sql
SET datafusion.optimizer.enable_join_dynamic_filter_pushdown = 'false';
```

(Exists in config catalog.) ([DF Config Keys][6])

### F2) “Bounded memory” fallback path

If you see memory pressure:

* lower `datafusion.execution.batch_size`
* lower `target_partitions`
* enable spilling runtime (disk manager)

(Spill runtime is documented in DataFusion Python configuration guide.) ([DataFusion Config (Py)][5])

### F3) “Deterministic output” fallback path

If you need reproducible output ordering:

* Tier 1: use ordering-preserving scan options (Acero implicit ordering / sequenced output)
* Tier 2: sort by canonical key before writing

(ScanNodeOptions ordering knobs are documented.) ([Arrow ScanNodeOptions][22])

---

## References

[1]: https://datafusion.apache.org/python/user-guide/basics.html
[2]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html
[3]: https://datafusion.apache.org/python/user-guide/io/arrow.html
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html
[5]: https://datafusion.apache.org/python/user-guide/configuration.html
[6]: https://datafusion.apache.org/user-guide/configs.html
[7]: https://datafusion.apache.org/blog/2025/09/29/datafusion-50.0.0/
[8]: https://datafusion.apache.org/user-guide/sql/explain.html
[9]: https://datafusion.apache.org/blog/2025/11/25/datafusion-51.0.0/
[10]: https://ibis-project.org/backends/datafusion
[11]: https://ibis-project.org/reference/expression-tables
[12]: https://ibis-project.org/reference/sql
[13]: https://ibis-project.org/reference/scalar-udfs
[14]: https://ibis-project.org/reference/schemas
[15]: https://sqlglot.com/sqlglot/optimizer/qualify.html
[16]: https://sqlglot.com/sqlglot/optimizer/normalize.html
[17]: https://sqlglot.com/sqlglot/optimizer/normalize_identifiers.html
[18]: https://sqlglot.com/sqlglot/lineage.html
[19]: https://sqlglot.com/sqlglot/diff.html
[20]: https://arrow.apache.org/docs/python/generated/pyarrow.set_cpu_count.html
[21]: https://arrow.apache.org/docs/python/generated/pyarrow.set_io_thread_count.html
[22]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.ScanNodeOptions.html
[23]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_parent_indices.html
[24]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_flatten.html
