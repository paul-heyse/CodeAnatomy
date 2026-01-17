I unzipped `Code_Anatomy_Incremental_Rebuild_Functioning.zip` and reviewed `src/` side-by-side with your architecture improvement plan. 

## 1) Plan vs codebase: what matches, what’s already done, what’s still missing

### The plan’s “friction points” are real in the codebase

* You **do have two plan worlds** today: `relspec`/Ibis/DataFusion for many relationship rules + edges, but **CPG nodes + most props are still ArrowDSL plan-lane** (`cpg/build_nodes.py`, `cpg/build_props.py`, etc.). This aligns with the plan’s “unify on relspec/Ibis” thrust. 
* You **do still have Python-loop hotspots** in core primitives (e.g., stable hashing and col→byte). The plan’s “get these out of Python” recommendation is still the right milestone. 

### Several “PR-A / runtime substrate” ideas are *already partially implemented*

Your plan calls out DataFusion session/policy plumbing as a strength; the repo already has more of this than the plan explicitly credits:

* `datafusion_engine/runtime.py` is already a fairly serious **runtime profile + SessionConfig policy** layer with list-files cache, metadata cache, memory limits, explain/fallback collectors, etc.
* You already have **Substrait artifacts** wired (Serde usage + bundle writing).
* You already have a **DataFusion kernel bridge** for several “kernel lane” ops (explode list via unnest, dedupe via window/row_number, etc.).

So: the plan’s PR-A “create runtime profile” is mostly “tighten + centralize usage,” not “invent.”

### Still missing (the plan is right to prioritize these)

* A single **execution substrate object** (session/workspace) that *owns* DF `SessionContext`, the Ibis backend bound to it, dataset registry, and diagnostics sinks (your plan’s PR-A). 
* **Ibis-first emission** for CPG nodes + most props (PR-B/PR-C). 
* A real **Rust-backed FunctionFactory / extension** for stable hash, col_to_byte, etc. (PR-E). 

That’s the plan/code alignment.

---

## 2) High-leverage library features the plan doesn’t exploit (and you should)

Below are the “best-in-class” knobs/surfaces in **DataFusion / Ibis / SQLGlot / PyArrow** that your plan *doesn’t* currently leverage as explicit design primitives (even if you use some adjacent pieces).

---

# A) DataFusion (Python + engine) features to bake into the architecture

## A1) **True streaming execution surface** via `__arrow_c_stream__`

DataFusion Python DataFrames implement `__arrow_c_stream__`, which **triggers execution but yields batches incrementally** instead of materializing everything into RAM. This is an ideal substrate for “run rules → stream results to parquet writer” rather than “run rules → Arrow table → write.” ([Apache DataFusion][1])

**What to do architecturally**

* Promote “streamable materialization” to a first-class result kind in your rule execution outputs (alongside table).
* Route the streaming surface directly into your existing Parquet dataset writer (you already accept `RecordBatchReader` inputs in your IO layer).

This is a *huge* memory-pressure and scalability lever (and it makes “best-in-class” feel real).

## A2) **Engine-native writes** (`DataFrame.write_parquet/write_json/write_csv`)

DataFusion Python exposes `DataFrame.write_parquet` / `write_json` / `write_csv` in its DataFrame API. ([Apache DataFusion][2])
This can eliminate Python-side Parquet writing overhead when:

* you don’t need your custom metadata sidecars, or
* you can reproduce the sidecar behavior via post-pass indexing.

**What to do architecturally**

* Treat DataFusion write as a **materialization backend option**:

  * “Arrow writer” (your current pyarrow.dataset.write_dataset path)
  * “DataFusion writer” (engine writes parquet directly)
* Decide per dataset/ruleset (wide tables vs small tables, “needs sidecar metadata” vs “doesn’t”).

Even if you don’t use it everywhere, having the switch is valuable.

## A3) **DataFusion 50+ performance features to deliberately lean on**

DataFusion 50 introduced/expanded several engine behaviors that matter specifically for your rule packs:

* **Dynamic filter pushdown for inner hash joins** (runtime filters applied at scan time) ([Apache DataFusion][3])
* **Parquet metadata caching** (statistics/page indexes/etc cached to reduce repeated IO) ([Apache DataFusion][3])
* **Improved spilling sorts** for larger-than-memory datasets ([Apache DataFusion][3])

**What to do architecturally**

* Update your “best-in-class” plan to explicitly depend on these:

  * Ensure your **join patterns** allow DF to infer “small side” / selective side so dynamic filters fire (e.g., push selective filters *before* joins in Ibis IR).
  * Ensure **spill settings** are always configured when sorts are possible (especially for determinism sorts).

## A4) DataFusion 51: **named arguments + explain/analyze metrics level**

DataFusion 51 added:

* **PostgreSQL-style named arguments** (`param => value`) for scalar/aggregate/window functions ([Apache DataFusion][4])
* **Improved EXPLAIN ANALYZE metrics**, plus a new `datafusion.explain.analyze_level` option (`summary` vs `dev`) ([Apache DataFusion][4])
* “Better defaults for remote parquet reads” and explicit mention of `datafusion.execution.parquet.metadata_size_hint` tuning ([Apache DataFusion][4])

**What to do architecturally**

* In your FunctionFactory policy + rule primitive registry: **make parameter names first-class** so LLM-authored SQL (or SQLGlot rewrites) can safely use named args, and diagnostics show parameter names. ([Apache DataFusion][4])
* Add a “metrics capture tier” in your runtime profile that sets `datafusion.explain.analyze_level` and stores the structured metrics artifacts per rule. ([Apache DataFusion][4])

## A5) Configuration settings as a *contract surface*

The DataFusion docs explicitly list runtime config settings like `datafusion.runtime.list_files_cache_limit` and `datafusion.runtime.metadata_cache_limit`. ([Apache DataFusion][5])
Your repo already sets some of these, but the plan doesn’t treat them as “architecture.”

**What to do**

* Make a “DataFusion settings contract” object that is:

  * versioned
  * emitted into run manifests
  * used in plan/cache keys

That’s how you get reproducible performance + debugging.

---

# B) Ibis features to treat as first-class “compiler surfaces”

## B1) **Compile to SQLGlot AST intentionally** (`compiler.to_sqlglot`)

Ibis fully transitioned to using SQLGlot under the hood, and you can directly inspect the SQLGlot representation via backend compiler `to_sqlglot()`. ([Ibis][6])

**What to do architecturally**

* In relspec compilation, add an explicit “IR checkpoint”:

  1. Ibis expr → SQLGlot AST
  2. apply SQLGlot normalization/policy checks (next section)
  3. render dialect SQL only at the end

This gives you deterministic plan fingerprinting, lineage, and rewrite hooks without fragile string SQL.

## B2) **Stream results directly**: `Table.to_pyarrow_batches`

Ibis table expressions can execute and return a **RecordBatchReader** via `to_pyarrow_batches(limit=None, params=None, chunk_size=...)`. ([Ibis][7])

**What to do**

* For big relationship tables, prefer:

  * `IbisPlan.to_reader()` / `to_pyarrow_batches()` → Parquet dataset writer
  * only materialize to `pa.Table` when required (e.g., canonical sort tier)

This pairs extremely well with your existing IO layer and DF’s own streaming (A1).

## B3) UDF performance ladder: **builtin vs vectorized vs scalar**

Ibis scalar UDF APIs include:

* `builtin` (escape hatch for backend native function names)
* `pyarrow` (vectorized over Arrow arrays)
* `pandas` (vectorized over Series)
* `python` (scalar, slowest) ([Ibis][8])

**What the plan misses**
Your plan basically jumps from “Python loops” → “Rust function factory.” That’s correct as the end state, but there’s a **high-quality intermediate**:

* Implement “fallback UDFs” as `@ibis.udf.scalar.pyarrow` where possible (vectorized, still in Python, but avoids row loops/GIL thrash).
* Reserve `python` UDFs as last resort.

This gives you a performance ladder that’s maintainable and much easier than “Rust everything immediately.”

## B4) Schema ↔ SQLGlot conversion for contract-driven DDL

Ibis Schema supports:

* `Schema.from_sqlglot(...)`
* `Schema.to_sqlglot_column_defs(...)` (and `to_sqlglot` is deprecated in favor of it) ([Ibis][9])

**What to do**

* If your dataset contracts are the source of truth, generate **dialect-correct CREATE TABLE** (or “expected schema AST”) from contracts, and:

  * use it to validate the compiled query output schema at compile time, and/or
  * include it in your repro bundles as a stable “contract artifact.”

This is a big maintainability/robustness step: schema is no longer “inferred and hoped,” it’s explicit and testable.

## B5) Ibis dynamic rewrite patterns (advanced but high upside)

Ibis has a pattern of rewriting expressions based on known filters (example: “UDF rewriting with predicate pushdowns”). ([Ibis][10])

**How it applies to your domain**
Your “winner selection / ambiguity resolution / tie-breakers” can eventually become *rewriteable* based on:

* what predicates the rule applies,
* what evidence sources are present,
* what “strictness tier” is configured.

That’s a longer-term lever, but it’s the *right* abstraction if you want an “agent-grade compiler” rather than a fixed pipeline.

---

# C) SQLGlot features to harden determinism, introspection, and caching

## C1) **Column lineage graph** (`sqlglot.lineage`)

SQLGlot provides a lineage API to build lineage graphs for columns in a query. ([SqlGlot][11])

**What to do**
Use lineage as a compiler step to:

* compute “required input columns” (for projection pushdown in scans)
* emit “rule → input columns” and “rule → output columns” artifacts into your diagnostics tables
* validate contracts (“this rule should never touch column X”)

This is a very direct performance win (push down projections early) and a major debug win.

## C2) **Semantic diff** (`sqlglot.diff`) for incremental rebuild correctness

SQLGlot includes a semantic diff capability (diffing expressions / SQL meaningfully). ([SqlGlot][12])

**What to do**
Use semantic diff to decide:

* whether a rule plan meaningfully changed (invalidate caches)
* whether a rewrite hook changed semantics unexpectedly
* whether “refactors” to the compiler preserve query meaning

This fits perfectly with your incremental rebuild goal: “do less work when nothing changed.”

## C3) Normalize boolean forms (CNF/DNF) when it helps pushdown

SQLGlot has normalization utilities (e.g., rewrite to CNF/DNF) via `optimizer.normalize`. ([SqlGlot][13])

**Why you care**
Certain optimizer passes (predicate pushdown style logic) often get easier/more reliable when predicates are in a normalized form. SQLGlot even documents DNF considerations in predicate pushdown contexts. ([SqlGlot][14])

**What to do**

* Add an **optional** normalization step in your canonicalization pipeline:

  * only for rules where you know it helps (don’t blindly CNF everything; it can explode expression size)
* Record “normalization distance” as a diagnostic for “this rewrite got expensive.”

## C4) Treat `qualify` as non-optional in your pipeline

SQLGlot’s qualify step rewrites AST to normalized/qualified tables/columns and is described as necessary for further optimizations. ([SqlGlot][15])

You already call qualify in your local tooling, but the plan doesn’t treat it as a “compiler invariant.” Make it one.

---

# D) PyArrow features to formalize “streaming, ordering, and explode”

## D1) Acero scan ordering: `implicit_ordering` + `require_sequenced_output`

Acero’s `ScanNodeOptions` explicitly supports:

* `require_sequenced_output=True` (yield batches sequentially)
* `implicit_ordering=True` (augment output with fragment/batch indices for stable ordering in simple plans) ([Apache Arrow][16])

**What to do**

* Promote these to *first-class knobs* in your scan policy runtime profile.
* Encode the presence of implicit ordering into schema metadata (you already have ordering metadata concepts).
* Use it to avoid unnecessary canonical sorts in cases where scan→filter→project pipelines can remain stable.

This is a determinism/perf dial: stable without sorting where possible.

## D2) Explode list columns without Python loops: `list_parent_indices` + `list_flatten`

PyArrow compute provides:

* `list_parent_indices`
* `list_flatten` ([Apache Arrow][17])

**What to do**
Even though you already have a DataFusion unnest kernel, explicitly standardize a fallback that is 100% Arrow-kernel based and fast:

* `parent_idx = pc.list_parent_indices(list_col)`
* `values = pc.list_flatten(list_col)`
* `parents = pc.take(parent_id_col, parent_idx)`

This is a “never again write Python loops” primitive, and it applies all over CPG edge building.

## D3) Concurrency control: Arrow thread pools (`set_cpu_count`, `set_io_thread_count`)

PyArrow exposes explicit control of:

* compute thread pool size (`set_cpu_count`) ([Apache Arrow][18])
* IO thread pool size (`set_io_thread_count`) ([Apache Arrow][19])

**What the plan misses**
You already noted “don’t oversubscribe DF threads + Python threads” in concept. Make it concrete:

* EngineSession should own the concurrency budget and set:

  * DataFusion target partitions / batch size
  * PyArrow cpu/io thread counts

That is a “best-in-class single-box” requirement.

## D4) Streaming writers: Dataset writing from batch streams

PyArrow supports scanners built from one-shot batch sources (RecordBatchReader or iterator), intended to support writing datasets from sources that can be read only once. ([Apache Arrow][20])
This matches perfectly with:

* Ibis `to_pyarrow_batches()` ([Ibis][7])
* DataFusion `__arrow_c_stream__` ([Apache DataFusion][1])

**What to do**
Make “stream → write_dataset” the default for large outputs.

---

## 3) Concrete “plan addendum”: the PRs I would add to your roadmap

Your current roadmap is good; I’d add **two PRs** that unlock most of the “missing library leverage” above:

### PR-G: Streaming materialization end-to-end

**Goal:** stop treating “materialize to pa.Table” as the default.

* Add a unified “streaming result” surface for rule execution:

  * Ibis: `to_pyarrow_batches(chunk_size=...)` ([Ibis][7])
  * DataFusion: `__arrow_c_stream__` ([Apache DataFusion][1])
* Make your dataset writer prefer `RecordBatchReader` when determinism tier allows.
* Only force table materialization for:

  * canonical sorts / winner-selection that truly require global order
  * kernels that are pipeline breakers

### PR-H: SQLGlot analysis layer (lineage + semantic diff + normalization)

**Goal:** treat SQLGlot as your “compiler analysis IR,” not just a fallback.

* Always capture “Ibis → SQLGlot AST” during compilation. ([Ibis][6])
* Add:

  * lineage extraction for input-column minimization and auditing ([SqlGlot][11])
  * semantic diff for plan caching & incremental rebuild correctness ([SqlGlot][12])
  * optional CNF/DNF normalization for targeted rules ([SqlGlot][13])

### PR-E refinement: FunctionFactory that exploits named args + metrics

When you do the Rust FunctionFactory PR, make it explicitly “DataFusion 51-ready”:

* expose parameter names so named arguments work cleanly ([Apache DataFusion][4])
* set `datafusion.explain.analyze_level` and harvest the improved metrics artifacts per rule ([Apache DataFusion][4])

---

If you want, I can turn the above addendum into a **file-by-file implementation map** (where to thread streaming through `run_plan_adapter`, where to adjust your `FinalizeResult`/materializers, and where to hang SQLGlot lineage/diff artifacts so they show up in your existing run bundles).

[1]: https://datafusion.apache.org/python/user-guide/io/arrow.html?utm_source=chatgpt.com "Apache Arrow DataFusion documentation"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html?utm_source=chatgpt.com "datafusion.dataframe"
[3]: https://datafusion.apache.org/blog/2025/09/29/datafusion-50.0.0/ "Apache DataFusion 50.0.0 Released - Apache DataFusion Blog"
[4]: https://datafusion.apache.org/blog/2025/11/25/datafusion-51.0.0/ "Apache DataFusion 51.0.0 Released - Apache DataFusion Blog"
[5]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[6]: https://ibis-project.org/posts/does-ibis-understand-sql/ "Does Ibis understand SQL? – Ibis"
[7]: https://ibis-project.org/reference/expression-tables "expression-tables – Ibis"
[8]: https://ibis-project.org/reference/scalar-udfs "scalar-udfs – Ibis"
[9]: https://ibis-project.org/reference/schemas "schemas – Ibis"
[10]: https://ibis-project.org/posts/udf-rewriting/ "Dynamic UDF Rewriting with Predicate Pushdowns – Ibis"
[11]: https://sqlglot.com/sqlglot/lineage.html?utm_source=chatgpt.com "sqlglot.lineage API documentation"
[12]: https://sqlglot.com/sqlglot/diff.html?utm_source=chatgpt.com "Semantic Diff for SQL"
[13]: https://sqlglot.com/sqlglot/optimizer/normalize.html?utm_source=chatgpt.com "sqlglot.optimizer.normalize API documentation"
[14]: https://sqlglot.com/sqlglot/optimizer/pushdown_predicates.html?utm_source=chatgpt.com "sqlglot.optimizer.pushdown_predicates API documentation"
[15]: https://sqlglot.com/sqlglot/optimizer/qualify.html?utm_source=chatgpt.com "sqlglot.optimizer.qualify API documentation"
[16]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.ScanNodeOptions.html "pyarrow.acero.ScanNodeOptions — Apache Arrow v22.0.0"
[17]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_parent_indices.html?utm_source=chatgpt.com "pyarrow.compute.list_parent_indices — Apache Arrow v22.0.0"
[18]: https://arrow.apache.org/docs/python/generated/pyarrow.set_cpu_count.html?utm_source=chatgpt.com "pyarrow.set_cpu_count — Apache Arrow v22.0.0"
[19]: https://arrow.apache.org/docs/python/generated/pyarrow.set_io_thread_count.html?utm_source=chatgpt.com "pyarrow.set_io_thread_count — Apache Arrow v22.0.0"
[20]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html?utm_source=chatgpt.com "pyarrow.dataset.Scanner — Apache Arrow v22.0.0"

## A) DataFusion (Python bindings) — streaming execution, file IO, optimizer power knobs, and observability

### Mental model: **one logical plan, multiple execution surfaces**

A `datafusion.DataFrame` is a lazily-evaluated logical plan; execution happens at **terminal operations** (e.g., `collect()`, `to_pandas()`, `to_arrow_table()`, `show()`, `count()`). ([datafusion.apache.org][1])

For “best-in-class” compute pipelines you should treat DataFusion as having **three distinct result surfaces**:

1. **In-memory materialization** (`collect`, `to_arrow_table`, `to_pandas`, `to_polars`) — simple, but RAM-heavy. ([datafusion.apache.org][1])
2. **Native batch iteration / streaming** (Python iteration over `df`, `execute_stream`, async iteration) — incremental batches without full materialization. ([datafusion.apache.org][1])
3. **Arrow C Stream / PyCapsule** (`__arrow_c_stream__`) — “universal zero-copy stream” into Arrow-based consumers. ([datafusion.apache.org][1])

In your architecture, you want (2) and (3) to be the **default** for large outputs.

---

### A1) Zero-copy streaming via `__arrow_c_stream__` and RecordBatch iteration

#### A1.1 Full surface area

**Primary protocol**

* `DataFrame.__arrow_c_stream__(requested_schema: object | None = None) -> PyCapsule`

  * Executes using DataFusion streaming APIs and yields record batches incrementally (no full materialization). ([datafusion.apache.org][2])

**Streaming helpers**

* PyArrow consumer: `pa.RecordBatchReader.from_stream(df)` (pulls from `__arrow_c_stream__`) ([datafusion.apache.org][1])
* DataFrame is iterable: `for batch in df:` yields DataFusion `RecordBatch` lazily; batches expose `to_pyarrow()`. ([datafusion.apache.org][1])
* Explicit streams:

  * `df.execute_stream()` → stream over a **single partition** ([datafusion.apache.org][2])
  * `df.execute_stream_partitioned()` → list of streams, one per partition ([datafusion.apache.org][2])
* Async iteration: `async for batch in df:` supported. ([datafusion.apache.org][1])

#### A1.2 Requested schema: what it *can* and *cannot* do

`requested_schema` lets the consumer ask for a specific output field order/subset, but DataFusion only supports **simple projections** through this interface (select subset/reorder). It does **not** support renaming, computed expressions, or type coercion via this hook. ([datafusion.apache.org][2])

**Design implication:** if you need “consumer-driven shaping,” do it in the plan (select/alias/cast) *before* streaming; use `requested_schema` only for “projection pushdown to the consumer”.

#### A1.3 Minimal runnable snippets

**(1) Stream into PyArrow without materializing**

```python
from datafusion import SessionContext, col, lit
import pyarrow as pa

ctx = SessionContext()
df = ctx.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]})

df2 = df.select((col("a") * lit(2)).alias("a2"), col("b"))

reader = pa.RecordBatchReader.from_stream(df2)
for batch in reader:
    # batch: pyarrow.RecordBatch
    print(batch.num_rows)
```

(Streaming via `__arrow_c_stream__` + `RecordBatchReader.from_stream`.) ([datafusion.apache.org][1])

**(2) Iterate DataFusion batches directly**

```python
for rb in df2:
    # rb: datafusion.RecordBatch, lazily produced
    pa_rb = rb.to_pyarrow()
```

(DataFrame iteration and `to_pyarrow()`.) ([datafusion.apache.org][1])

---

### A2) “Arrow interop boundary” in `SessionContext`

The interop boundary is *bi-directional*:

* `SessionContext.from_arrow(data)` accepts any object implementing `__arrow_c_stream__` **or** `__arrow_c_array__` (the latter must return a struct array). ([datafusion.apache.org][3])
* This means you can round-trip “streaming tables” between:

  * DataFusion ↔ PyArrow ↔ Polars ↔ Pandas (as long as they speak Arrow C interfaces)

**Design implication:** your “execution substrate” can standardize on Arrow C Stream objects as a **universal internal exchange format** across engines.

---

### A3) Engine-native file writes from a DataFusion `DataFrame`

#### A3.1 Full surface area (Python API)

From the DataFusion Python API:

* `df.write_csv(path, with_header=False, write_options=None) -> None` ([datafusion.apache.org][2])
* `df.write_json(path, write_options=None) -> None` ([datafusion.apache.org][2])
* `df.write_parquet(path, compression=..., compression_level=None, write_options=None) -> None` ([datafusion.apache.org][2])
* `df.write_parquet_with_options(path, options: ParquetWriterOptions, write_options: DataFrameWriteOptions | None)` ([datafusion.apache.org][2])
* `df.write_table(table_name, write_options=None)` (provider-dependent) ([datafusion.apache.org][2])

**Writer control plane**

* `DataFrameWriteOptions(insert_operation, single_file_output, partition_by, sort_by)` ([datafusion.apache.org][2])
* Parquet:

  * `ParquetWriterOptions(...)` “global” file options ([datafusion.apache.org][2])
  * `ParquetColumnOptions(...)` per-column overrides (encoding, compression, statistics, bloom filters, etc.) ([datafusion.apache.org][2])

> Note: There are still open discussions/issues about exposing *all* write options and improving write APIs in `datafusion-python`, so treat “full parity with Rust writer APIs” as evolving. ([GitHub][4])

#### A3.2 Key knobs you should standardize

**`DataFrameWriteOptions`**

* `partition_by`: column(s) to partition output by ([datafusion.apache.org][2])
* `sort_by`: global sort requirement at write time (critical for deterministic datasets) ([datafusion.apache.org][2])
* `single_file_output`: force single output file (use sparingly; often hurts parallelism) ([datafusion.apache.org][2])
* `insert_operation`: append/overwrite/replace semantics for table writes ([datafusion.apache.org][2])

**`ParquetWriterOptions` (selected “high leverage” fields)**

* Compression control (`compression`, `compression_level`) ([datafusion.apache.org][2])
* Row group sizing (`max_row_group_size`) + batch sizing (`write_batch_size`) ([datafusion.apache.org][2])
* Statistics (`statistics_enabled`, truncate lengths) ([datafusion.apache.org][2])
* Bloom filters (`bloom_filter_on_write`, `bloom_filter_fpp`, `bloom_filter_ndv`) ([datafusion.apache.org][2])
* Parallel row group writer caps (`maximum_parallel_row_group_writers`, buffering) ([datafusion.apache.org][2])
* Column-specific overrides via `column_specific_options` ([datafusion.apache.org][2])

#### A3.3 Minimal runnable snippet: Parquet write with options

```python
from datafusion import SessionContext
from datafusion.dataframe import DataFrameWriteOptions, ParquetWriterOptions

ctx = SessionContext()
df = ctx.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})

write_opts = DataFrameWriteOptions(
    partition_by=["b"],
    single_file_output=False,
)

parquet_opts = ParquetWriterOptions(
    compression="zstd(3)",
    max_row_group_size=256_000,
    statistics_enabled="page",
)

df.write_parquet_with_options("out_dir/part.parquet", parquet_opts, write_opts)
```

(API and option objects as documented.) ([datafusion.apache.org][2])

---

### A4) Optimizer features you should “design for”: dynamic filters, metadata caching, spill sorts

#### A4.1 Dynamic filter pushdown (hash joins, TopK) — what it is

DataFusion 50 extended dynamic filter pushdown to **inner hash joins**, applying runtime-derived filters at scan time to reduce IO/decoding when one side is small or highly selective. ([datafusion.apache.org][5])

This is classic **sideways information passing**: join build side informs scan side predicates at runtime. ([datafusion.apache.org][5])

#### A4.2 “Power knobs”: configuration settings you should expose in your runtime profile

DataFusion exposes explicit config flags for dynamic filtering, including join dynamic filter pushdown. ([datafusion.apache.org][6])

Examples from the config catalog:

* `datafusion.optimizer.enable_join_dynamic_filter_pushdown` (boolean) ([datafusion.apache.org][6])
* `datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown` (boolean) ([datafusion.apache.org][6])

**InList vs hash-lookup pushdown thresholds** (important for stability and memory):

* Config includes limits that control when DF converts small build sides into `IN (...)` style filters (vs hash table lookups), including memory implications being per-partition. ([datafusion.apache.org][6])

**Design implication:** put these knobs into your *versioned* “runtime profile” so you can reproduce performance and disable problematic behaviors when needed.

> Robustness note: DF50 had reported regressions/known issues around some optimizer behaviors; treat dynamic filtering as a feature you can toggle per workload via config. ([GitHub][7])

#### A4.3 Parquet metadata caching

DataFusion 50 automatically caches Parquet metadata (statistics, page indexes, etc.) to avoid repeated disk/network round trips, and this can produce large speedups for repeated queries over remote storage. ([datafusion.apache.org][5])

Runtime config also exposes explicit memory limits for metadata caching:

* `datafusion.runtime.metadata_cache_limit` (default shown in config catalog) ([datafusion.apache.org][6])
* Related: list files cache (`datafusion.runtime.list_files_cache_limit`, TTL) ([datafusion.apache.org][6])

#### A4.4 Improved spilling sorts and memory-limited execution

DataFusion 50 improved larger-than-memory sorts (multi-level merge sorts) so sorts that previously OOM can spill to disk. ([datafusion.apache.org][5])

On the tuning side, DataFusion’s config guide explicitly ties spill behavior to:

* `datafusion.execution.target_partitions` (more partitions ⇒ less memory per partition under fair spill pools) ([datafusion.apache.org][6])
* `datafusion.execution.batch_size` impacts spill/merge behavior; smaller can help under tight memory limits ([datafusion.apache.org][6])

And in Python, you can build a spill-enabled runtime:

* `RuntimeEnvBuilder().with_disk_manager_os().with_fair_spill_pool(...)` plus `SessionConfig(...).with_target_partitions(...)` etc. ([datafusion.apache.org][8])

---

### A5) Observability: EXPLAIN ANALYZE metrics + analyze verbosity tier

DataFusion 51 improved `EXPLAIN ANALYZE` metrics and introduced `datafusion.explain.analyze_level` with at least `summary` and `dev` tiers. ([datafusion.apache.org][9])

The config catalog documents:

* `datafusion.explain.analyze_level` defaulting to `dev`, where `summary` is intended for high-level insight and `dev` for deep operator-level introspection. ([datafusion.apache.org][6])

**Design implication:** persist both:

* the plan text/JSON (or Substrait, if you already bundle it), and
* the metrics output under a stable “diagnostics artifact schema.”

---

### A6) Named arguments in SQL functions (DataFusion 51)

DataFusion 51 supports PostgreSQL-style named arguments `param => value` for scalar/aggregate/window functions, allows mixing positional and named args, and improves diagnostics by listing parameter names; UDF authors can also expose parameter names. ([datafusion.apache.org][9])

**Architecture use:** this makes your “FunctionFactory”/UDF registry more maintainable if you standardize on **named parameters** at the boundary.

---

## B) Ibis — streaming execution, SQLGlot boundary, vectorized UDF ladder, schema ↔ DDL hooks

### Mental model: Ibis is an IR builder with multiple “lowering layers”

Ibis table/value expressions have “execute & export” methods that are eager and backend-driven; for your purposes, treat Ibis as:

* a **portable relational IR**, and
* a **compiler frontend** that can lower to SQL/SQLGlot, and then to backend execution.

(And yes: Ibis compiles to SQLGlot under the hood, so SQLGlot tooling like lineage can still apply.) ([Ibis][10])

---

### B1) Streaming results: `to_pyarrow_batches` (RecordBatchReader)

#### B1.1 Full surface area

On table expressions:

* `to_pyarrow_batches(limit=None, params=None, chunk_size=1_000_000, **kwargs) -> RecordBatchReader`

  * eager execution; returns a batch reader (stream of record batches). ([Ibis][11])

Backend example (DataFusion backend):

* `Backend.to_pyarrow_batches(expr, *, chunk_size=..., **kwargs)` returns an iterator of record batches and is also eager. ([Ibis][12])

#### B1.2 Related export surfaces (useful in your plan)

In the same “table expressions” surface area you also get:

* `to_pandas_batches(...)` (stream pandas DataFrames) ([Ibis][11])
* `to_polars(...)` (materialize polars df) ([Ibis][11])
* `to_parquet_dir(directory, *, params=None, **kwargs)` explicitly notes it routes to `pyarrow.dataset.write_dataset(...)` and forwards kwargs. ([Ibis][11])

That last one is important: Ibis already has the conceptual pattern you want—“execute → stream → dataset write”—and you can either use it directly or replicate its internals with your own artifact metadata conventions.

#### B1.3 Failure modes / caveats

Some backends may not respect `chunk_size` the way you expect (there have been backend-specific reports/discussions around this). ([GitHub][13])

**Architecture move:** treat `chunk_size` as a *hint*; make your downstream writer robust to variable batch sizes.

---

### B2) The SQLGlot boundary: “compile checkpoint” and rewrite hooks

#### B2.1 What you want conceptually

A deterministic compilation pipeline typically wants:

1. Ibis expr
2. **SQLGlot AST** checkpoint
3. SQLGlot rewrites / normalization / qualification (policy layer)
4. dialect SQL string (or direct backend plan)

This enables: deterministic caching keys, semantic diffs, and lineage extraction.

#### B2.2 How it exists in Ibis today (practical reality)

* Ibis compilers can produce SQLGlot expressions (the internals doc explicitly describes compilation to a string *or SQLGlot expression*). ([Ibis][14])
* There is a `SQLGlotCompiler.to_sqlglot(self, expr, limit, params)` method visible in real stack traces from users, and it flows through `translate(..., params=...)` and rewrite phases. ([GitHub][15])

**Practical guidance**

* Treat this as a **semi-internal** API: extremely useful, but you should wrap it behind your own `CompilerCheckpoint` abstraction so you can tolerate upstream API shifts.

#### B2.3 Related Ibis “SQL compilation control plane”

* `ibis.options.sql.fuse_selects`: whether to fuse consecutive selects into a single query where possible. ([Ibis][16])

**Architecture use**

* Expose `fuse_selects` as a ruleset-level knob:

  * on for performance (fewer subqueries),
  * off for debug/readability or to work around backend planner edge cases.

#### B2.4 Robustness note: SQLGlot qualify interactions

There are real-world reports of Ibis compiler output interacting badly with SQLGlot qualification utilities (e.g., alias representations). ([GitHub][17])
**So**: your policy layer should include a “qualify smoke test” (compile → qualify → render) and have a fallback path (skip/patch) when upstream incompatibilities appear.

---

### B3) Scalar UDF ladder: `builtin` → `pyarrow`/`pandas` → `python`

Ibis scalar UDFs expose four main constructors:

* `builtin`: reference a backend-native function name/signature ([Ibis][18])
* `pandas`: vectorized over pandas Series ([Ibis][18])
* `pyarrow`: vectorized over Arrow arrays (often the best “Python-level” performance/interop compromise) ([Ibis][18])
* `python`: non-vectorized, row-by-row; explicitly warned as likely slow ([Ibis][18])

**Why this matters for your plan**
You don’t have to jump directly from “Python loops” → “Rust UDF” for every hotspot:

* Many “kernel lane” operations (stable hashes, list explode helpers, string normalization) can often be expressed as **PyArrow compute** inside `@ibis.udf.scalar.pyarrow` for a large win with low complexity. ([Ibis][18])
* Keep `python` UDFs as “correctness fallback only.” ([Ibis][18])

Minimal example from docs (pyarrow UDF):

```python
import ibis
import pyarrow.compute as pc
from datetime import date

@ibis.udf.scalar.pyarrow
def weeks_between(start: date, end: date) -> int:
    return pc.weeks_between(start, end)
```

(Shown in Ibis docs.) ([Ibis][18])

---

### B4) Schema ↔ SQLGlot hooks: contract-driven DDL shapes

Ibis `Schema` has explicit SQLGlot integration:

* `Schema.from_sqlglot(schema, dialect=None)` ([Ibis][19])
* `Schema.to_sqlglot_column_defs(dialect)` produces a list of SQLGlot `ColumnDef` nodes (and `to_sqlglot` is deprecated in favor of this). ([Ibis][20])

**Architecture use**

* Generate dialect-correct column definitions from your dataset contracts and:

  * validate compiled outputs (schema mismatch = compile-time failure),
  * emit deterministic “expected schema AST” artifacts for debugging,
  * produce standardized `CREATE TABLE` shapes in your reproducibility bundles.

---

### B5) “Dynamic rewrite” as a first-class technique

Ibis demonstrates dynamic rewriting patterns where UDF semantics can be rewritten to enable optimizer features like predicate pushdown. ([Ibis][21])

For your system, this is the conceptual bridge to “rules that compile differently under different filters/strictness tiers” while preserving correctness.

---

## C) SQLGlot — qualification, normalization, lineage graphs, semantic diffs

### Mental model: SQLGlot is your **analysis IR**, not just a formatter

Use SQLGlot to:

* enforce deterministic SQL shapes,
* extract lineage,
* compute semantic diffs (for incremental rebuild),
* and run normalizing transforms that make downstream optimizations/pushdowns more reliable.

---

### C1) Qualification: `sqlglot.optimizer.qualify.qualify`

`qualify(...)` rewrites an AST to have normalized and qualified tables/columns and is described as necessary for further SQLGlot optimizations. ([sqlglot.com][22])

Key parameters (from docs):

* `schema=...` (table→columns mapping)
* `validate_qualify_columns`
* `quote_identifiers`, `identify`
* `canonicalize_table_aliases`
* `on_qualify` callback hook ([sqlglot.com][22])

**Architecture pattern**

* Run qualify as a required step in your “SQL shape policy” unless:

  * a backend-specific compiler bug forces a bypass,
  * or the query is an opaque pass-through you choose not to analyze.

---

### C2) CNF/DNF normalization cost model: `sqlglot.optimizer.normalize`

SQLGlot exposes utilities around normalization and explicitly notes the **cost** can be exponential; `normalization_distance(...)` is used as an estimate of conversion cost. ([sqlglot.com][23])

Related: predicate pushdown logic differentiates CNF-like and DNF forms; DNF pushdown has stricter limitations (“only conditions in all blocks”). ([sqlglot.com][24])

**Architecture use**

* Apply CNF/DNF normalization selectively:

  * only on predicates where it demonstrably improves pushdown / pruning,
  * guarded by `normalization_distance` thresholds.

---

### C3) Column lineage graphs: `sqlglot.lineage.lineage(...)`

SQLGlot provides a direct lineage builder:

* `lineage(column, sql, schema=None, sources=None, dialect=None, scope=None, trim_selects=True, copy=True, **kwargs)` ([sqlglot.com][25])

Two parameters that matter a lot for you:

* `trim_selects=True` can simplify selects to only relevant columns (useful for projection minimization). ([sqlglot.com][25])
* `**kwargs` are forwarded to qualification optimizer behavior (so your qualify policy and lineage are coupled). ([sqlglot.com][25])

**Architecture use**

* Emit per-rule artifacts:

  * required input columns,
  * derived output columns,
  * lineage graphs for “why does this edge exist?”

---

### C4) Semantic diffs: `sqlglot.diff`

SQLGlot offers semantic diff machinery; conceptually, it focuses on matching elements across ASTs to produce meaningful change sequences. ([sqlglot.com][26])

A useful framing from SQLGlot’s ecosystem: the diff output can be treated as an “edit script” of operations on AST nodes that transforms one expression into another. ([tobikodata.com][27])

**Architecture use**

* Use semantic diff to drive incremental rebuild invalidation:

  * if only formatting/alias changes: keep caches
  * if predicate/join structure changes: invalidate affected products

---

### C5) Identifier normalization: `normalize_identifiers(...)`

SQLGlot can normalize identifier case while preserving semantics per dialect, and docs note it’s important for AST standardization; you can mark identifiers as case-sensitive via a special comment. ([sqlglot.com][28])

**Architecture use**

* Normalize identifiers early to stabilize cache keys across formatting/quoting differences.

---

## D) PyArrow — Acero ordering, list explode kernels, thread pools, and streaming dataset writes

### Mental model: Arrow is your **physical batch substrate**

Even if you “move everything to Ibis/DataFusion,” Arrow remains the batch representation at IO boundaries. The “best-in-class” move is to make Arrow **streaming-first** and **kernel-first**, avoiding Python loops.

---

### D1) Acero scan ordering: `ScanNodeOptions(require_sequenced_output, implicit_ordering)`

`pyarrow.acero.ScanNodeOptions` supports:

* `require_sequenced_output: bool`

  * yield batches sequentially (“like single-threaded”) ([Apache Arrow][29])
* `implicit_ordering: bool`

  * preserves implicit ordering; can augment batches with fragment/batch indices to enable stable ordering for simple ExecPlans ([Apache Arrow][29])

**Architecture use**

* Treat “ordering tier” as explicit:

  * **cheap stable ordering** using implicit ordering indices when possible
  * **true global ordering** only when you must sort (e.g., canonicalization winners)

---

### D2) List explode primitives: `list_parent_indices` + `list_flatten`

Two compute kernels are the core building blocks for “explode list<…> without Python loops”:

* `pyarrow.compute.list_parent_indices(lists)` emits the top-level list index for each value. ([Apache Arrow][30])
* `pyarrow.compute.list_flatten(lists, recursive=False, ...)` flattens list values; supports recursive flattening and does not emit anything for null list values. ([Apache Arrow][31])

**Canonical explode pattern**

```python
import pyarrow.compute as pc

parent = pc.list_parent_indices(list_col)
values = pc.list_flatten(list_col)
# then use `parent` to take parent keys, align, join, etc.
```

(These are documented compute kernels.) ([Apache Arrow][30])

---

### D3) Thread pools: `set_cpu_count` and `set_io_thread_count`

Arrow exposes explicit control over concurrency pools:

* `pyarrow.set_cpu_count(count)` controls threads used in parallel CPU operations. ([Apache Arrow][32])
* `pyarrow.set_io_thread_count(count)` controls IO pool threads; dataset scanning can use this pool. ([Apache Arrow][33])

**Architecture use**

* Own the concurrency budget centrally (especially if DataFusion also parallelizes by partitions) to avoid oversubscription.

---

### D4) Streaming dataset writes: `pyarrow.dataset.write_dataset(...)`

`write_dataset` accepts streaming inputs directly:

* `data` can be a `RecordBatchReader` or iterable of `RecordBatch` (if iterable, you must provide `schema`). ([Apache Arrow][34])
* `use_threads=True` writes files in parallel; order can change unless `preserve_order=True`. ([Apache Arrow][34])
* `preserve_order=True` guarantees row order but may cause notable performance degradation. ([Apache Arrow][34])
* `file_visitor` can collect written file paths and Parquet metadata (useful for building `_metadata`). ([Apache Arrow][34])
* `existing_data_behavior` supports overwrite/append-like patterns (e.g., `overwrite_or_ignore`, `delete_matching`). ([Apache Arrow][34])

**Minimal streaming write from a DataFusion stream**

```python
import pyarrow as pa
import pyarrow.dataset as ds
from datafusion import SessionContext

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

(RecordBatchReader streaming + write_dataset supported.) ([datafusion.apache.org][1])

---

## E) Cross-library “best-in-class” composition patterns (what to standardize)

### E1) Universal streaming pipeline (recommended default)

* DataFusion: `__arrow_c_stream__` → `pa.RecordBatchReader.from_stream(df)` ([datafusion.apache.org][1])
* Ibis: `expr.to_pyarrow_batches()` → `RecordBatchReader` ([Ibis][11])
* PyArrow: `ds.write_dataset(reader, ...)` ([Apache Arrow][34])

**Standardization:** make “streaming reader” a first-class artifact type in your execution model.

### E2) Compiler analysis pipeline

* Ibis compiler lower → SQLGlot expression checkpoint (semi-internal but real) ([Ibis][14])
* SQLGlot qualify + identifier normalization + optional CNF/DNF normalization thresholds ([sqlglot.com][22])
* SQLGlot lineage + diff artifacts for caching & incremental rebuild decisions ([sqlglot.com][25])

**Standardization:** treat SQLGlot AST (post-qualify) as your canonical “plan fingerprint” object.

---

If you want the next increment in the same reference style: I can turn this into a **drop-in “advanced.md” document** for your repo with (1) a concise “API surface index” per library, (2) a “recommended defaults” block for your runtime profile knobs (dynamic filtering, metadata cache limits, spill settings, Arrow thread pools), and (3) “failure-mode playbooks” (when to disable a knob / how to degrade gracefully).

[1]: https://datafusion.apache.org/python/user-guide/dataframe/index.html "DataFrames — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[4]: https://github.com/apache/datafusion-python/issues/1005?utm_source=chatgpt.com "Expose all write options · Issue #1005 · apache/datafusion- ..."
[5]: https://datafusion.apache.org/blog/2025/09/29/datafusion-50.0.0/ "Apache DataFusion 50.0.0 Released - Apache DataFusion Blog"
[6]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[7]: https://github.com/apache/datafusion/issues/16799?utm_source=chatgpt.com "Release DataFusion 50.0.0 (Aug/Sep 2025) #16799"
[8]: https://datafusion.apache.org/python/user-guide/configuration.html "Configuration — Apache Arrow DataFusion  documentation"
[9]: https://datafusion.apache.org/blog/2025/11/25/datafusion-51.0.0/ "Apache DataFusion 51.0.0 Released - Apache DataFusion Blog"
[10]: https://ibis-project.org/posts/does-ibis-understand-sql/?utm_source=chatgpt.com "Does Ibis understand SQL?"
[11]: https://ibis-project.org/reference/expression-tables "expression-tables – Ibis"
[12]: https://ibis-project.org/backends/datafusion "datafusion – Ibis"
[13]: https://github.com/ibis-project/ibis/issues/10443?utm_source=chatgpt.com "docs: `chunk_size` warning for `.to_pyarrow_batches()` for ..."
[14]: https://ibis-project.org/concepts/internals?utm_source=chatgpt.com "Internals - Ibis"
[15]: https://github.com/posit-dev/pointblank/discussions/100?utm_source=chatgpt.com "Snowflake / Databricks SQL support via Ibis #100"
[16]: https://ibis-project.org/reference/sql "sql – Ibis"
[17]: https://github.com/ibis-project/ibis/issues/11821?utm_source=chatgpt.com "bug: ibis table aliasing incompatible with `qualify_tables` ..."
[18]: https://ibis-project.org/reference/scalar-udfs "scalar-udfs – Ibis"
[19]: https://ibis-project.org/reference/schemas?utm_source=chatgpt.com "schemas – Ibis"
[20]: https://ibis-project.org/reference/schemas "schemas – Ibis"
[21]: https://ibis-project.org/posts/udf-rewriting/ "Dynamic UDF Rewriting with Predicate Pushdowns – Ibis"
[22]: https://sqlglot.com/sqlglot/optimizer/qualify.html "sqlglot.optimizer.qualify API documentation"
[23]: https://sqlglot.com/sqlglot/optimizer/normalize.html "sqlglot.optimizer.normalize API documentation"
[24]: https://sqlglot.com/sqlglot/optimizer/pushdown_predicates.html "sqlglot.optimizer.pushdown_predicates API documentation"
[25]: https://sqlglot.com/sqlglot/lineage.html "sqlglot.lineage API documentation"
[26]: https://sqlglot.com/sqlglot/diff.html "sqlglot.diff API documentation"
[27]: https://tobikodata.com/blog/automatically-detecting-breaking-changes-in-sql-queries "Automatically detecting breaking changes in SQL queries"
[28]: https://sqlglot.com/sqlglot/optimizer/normalize_identifiers.html "sqlglot.optimizer.normalize_identifiers API documentation"
[29]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.ScanNodeOptions.html "pyarrow.acero.ScanNodeOptions — Apache Arrow v22.0.0"
[30]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_parent_indices.html?utm_source=chatgpt.com "pyarrow.compute.list_parent_indices — Apache Arrow v22.0.0"
[31]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_flatten.html?utm_source=chatgpt.com "pyarrow.compute.list_flatten — Apache Arrow v22.0.0"
[32]: https://arrow.apache.org/docs/python/generated/pyarrow.set_cpu_count.html?utm_source=chatgpt.com "pyarrow.set_cpu_count — Apache Arrow v22.0.0"
[33]: https://arrow.apache.org/docs/python/generated/pyarrow.set_io_thread_count.html?utm_source=chatgpt.com "pyarrow.set_io_thread_count — Apache Arrow v22.0.0"
[34]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html "pyarrow.dataset.write_dataset — Apache Arrow v22.0.0"
