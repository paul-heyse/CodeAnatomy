
Polars advanced capabilities deep dive

---
# Table of contents #

## A) Optimizer control plane (per-query toggles + plan introspection knobs)

### A1) Fine-grained optimization toggles on `collect` / `explain` / `show_graph`

Modern Polars exposes per-query switches for common optimizer passes (type coercion, predicate/projection pushdown, expression simplification, slice pushdown, common subplan/subexpr elimination, clustering `with_columns`, streaming, etc.). These are crucial when you need deterministic debugging (“which pass caused this?”) or to intentionally disable a pass for correctness/perf investigations. ([Polars User Guide][1])

### A2) `QueryOptFlags` / “optimizations param” (newer control API)

Polars also exposes a structured “optimization set” object (marked unstable) for bundling optimization flags and passing them as a single unit. ([Polars User Guide][2])

### A3) `profile()` (node-level timing DataFrame)

`LazyFrame.profile()` runs the query and returns `(result_df, profile_df)` with per-node timings (µs), letting you do “query plan flamegraph” style analysis. ([Polars User Guide][3])

### A4) Multi-query execution: `polars.collect_all(...)`

Collect multiple LazyFrames together, in parallel, with common-subplan elimination across them (important for “fan-out” pipelines). ([Polars User Guide][4])

### A5) Execution determinism: `maintain_order=True` (and its cost)

For operations that don’t inherently preserve row order (eg groupby), Polars documents using `maintain_order=True` to keep consistent ordering between runs; it’s slower and can block streaming execution. ([Polars User Guide][5])

---

## B) Streaming beyond `collect(engine="streaming")`: batch generators + batch sinks

### B1) `collect_batches()` (stream results as chunks/generator)

Instead of materializing one DataFrame, stream results as “ready chunks” (tunable `chunk_size`, `maintain_order`, engine). This is the closest thing to an *in-process* iterator-based execution interface. ([Polars User Guide][6])

### B2) `sink_batches(callback, ...)` (push-based streaming + early stop)

Run the query and call a Python callback for each ready batch; callback may signal early termination (useful for “top-k until satisfied”, incremental ingestion, or backpressure integration). ([Polars User Guide][7])

### B3) Sink variants beyond the ones in your doc (and their knobs)

Your doc mentions some `sink_*`, but misses broader sink coverage and **the important knobs** like `maintain_order`, `row_group_size`, and page sizing in `sink_parquet`. ([Polars User Guide][8])

---

## C) Background execution + cancellation handles (in-process query control)

### C1) `collect(background=True)` → `InProcessQuery`

Polars can run a query “in the background” and return a handle you can `fetch` or `cancel`—a very different capability than streaming or async “future” collection. ([Polars User Guide][1])

### C2) `InProcessQuery.cancel/fetch/fetch_blocking`

The handle supports cancellation and both non-blocking and blocking retrieval. This is core for long-running pipelines where you want cooperative cancel or UI-driven progress. ([Polars User Guide][9])

---

## D) Async collection APIs (threadpool scheduled execution)

### D1) `LazyFrame.collect_async()` and `polars.collect_all_async()`

Polars exposes “schedule into thread pool and return quickly” APIs (documented as unstable) for integrating with async orchestration loops without blocking the main thread. ([Polars User Guide][10])

---

## E) GPU execution engine (newer engine mode)

### E1) `engine="gpu"` + `GPUEngine` configuration

Polars documents a GPU engine option for `LazyFrame.collect()`, plus a `GPUEngine` config object (device selection, GPU memory allocator/resource, failover behavior). ([Polars User Guide][1])

---

## F) Remote execution (`LazyFrame.remote`) for Polars Cloud/distributed runs

Your doc references “Polars Cloud” conceptually, but not the actual API surface: `LazyFrame.remote(...)` runs a query remotely with configurable strategies/retries/engine selection. ([Polars User Guide][11])

---

## G) Rust-native UDFs via Expression Plugins (preferred UDF mechanism)

Your doc covers Python UDFs (`map_elements/map_batches/apply`), but misses the *high-performance* alternative:

### G1) Expression plugins (Rust → dynamic link → native-speed expressions)

Polars positions expression plugins as the preferred way to add custom functions: compile a Rust function, dynamically link at runtime, and execute like a native expression (no Python/GIL overhead). ([Polars User Guide][12])

### G2) `polars.plugins.register_plugin_function(...)`

The registration API is explicitly marked **highly unsafe** (because you’re calling into a loaded native symbol); still, it’s the gateway to “custom kernels that keep Polars optimizable.” ([Polars User Guide][13])

---

## H) Selectors DSL (`polars.selectors`) as a first-class column-selection plane

Your doc uses `pl.all()` and names, but doesn’t cover selectors:

* dtype-based selection (`by_dtype`)
* name-based selection (prefix/suffix/regex)
* set algebra over selections
* broadcasting expressions over selected columns

This becomes essential once you maintain wide schemas and want robust “schema-driven” transformations. ([Polars User Guide][14])

---

## I) Reshaping & structural transforms (wide/long + nesting utilities)

Your doc is mostly “transform in place”; it doesn’t cover the deep reshape toolbox:

### I1) Wide↔Long transforms

* `DataFrame.melt()` / `DataFrame.unpivot()` (wide→long) ([Polars User Guide][15])
* `DataFrame.pivot()` (long→wide; eager-only, with documented lazy workaround patterns) ([Polars User Guide][16])
* `DataFrame.unstack()` (fast long→wide without aggregation; often faster than pivot) ([Polars User Guide][17])

### I2) Struct column manipulation utilities

* `unnest` and friends are part of the “nested data” story beyond just creating structs/lists. ([Polars User Guide][18])

### I3) Sorted merge primitive: `merge_sorted`

Merge two already-sorted frames by key (schemas must match); this is a performance primitive for time-series and partitioned pipelines. ([Polars User Guide][19])

### I4) `partition_by(...)` (split frame into per-key frames)

Useful for “fan-out” processing without materializing Python-level group loops manually. ([Polars User Guide][20])

### I5) `update(...)` (patch semantics)

Polars exposes update semantics on lazy frames to patch values from another frame with join-based strategies—useful for incremental “dimension table” refreshes. ([Polars User Guide][21])

---

## J) Row index plumbing (and its optimizer implications)

Your doc does not cover the newer “row index” surface:

### J1) `with_row_index()` on DataFrame/LazyFrame

Adds a regular UInt32/UInt64 column as a row index; Polars warns this can harm performance and block pushdown. ([Polars User Guide][22])

### J2) Row index at scan/read time (`row_index_name`, `row_index_offset`)

Parquet scan/read can inject a row index directly at the scan node, which is often preferable to post-hoc indexing for provenance. ([Polars User Guide][23])

### J3) API evolution notes (renames)

CSV APIs document renames like `row_count_name → row_index_name`. ([Polars User Guide][24])

---

## K) “Sortedness” flags to unlock fast paths (dangerous if wrong)

Polars lets you *assert* sortedness:

* `DataFrame.set_sorted(...)`, `LazyFrame.set_sorted(...)`, `Expr.set_sorted(...)`

This can enable optimized downstream algorithms, but is explicitly dangerous if the data isn’t actually sorted. ([Polars User Guide][25])

---

## L) Sequential evaluation primitives (`*_seq`) for dependency-sensitive transforms

Your doc assumes parallel expression evaluation; Polars also provides “sequential” variants:

* `with_columns_seq` (evaluate expressions sequentially, not in parallel) ([Polars User Guide][26])
* `select_seq` exists on DataFrame/LazyFrame, for similar reasons ([Polars User Guide][18])

These matter when expressions have data dependencies or when you want deterministic side effects with cheap expressions.

---

## M) Serialization & persistence of frames/plans/expressions (and security + version stability)

Your doc doesn’t cover:

* `DataFrame.serialize/deserialize` ([Polars User Guide][27])
* `LazyFrame.serialize/deserialize` (logical plan persistence) ([Polars User Guide][28])
* `Expr.meta.serialize` / `Expr.deserialize` (expression persistence) with explicit security warning: deserialization can execute arbitrary code when Python UDFs are involved, and serialization is not stable across versions. ([Polars User Guide][29])
* Upgrade notes around serialization-vs-JSON IO boundaries (eg `read_json` no longer reading `DataFrame.serialize` output; use `deserialize`). ([Polars User Guide][30])

---

## N) IO deep knobs you don’t cover (scan/read/write “planner switches”)

### N1) Parquet read knobs that materially change behavior

* `use_statistics` (page/rowgroup skipping), `parallel` direction (`row_groups` vs `columns` vs `none`), row index injection, etc. ([Polars User Guide][31])

### N2) CSV “systems knobs” (cache, error tolerance, batched readers)

* `scan_csv` includes cache and other advanced controls ([Polars User Guide][32])
* `read_csv_batched` for explicit batched ingestion patterns ([Polars User Guide][33])

### N3) Parquet sinks: row-group/page sizing + ordering

`sink_parquet` has rowgroup/page sizing and ordering knobs that strongly affect memory pressure and downstream scan performance. ([Polars User Guide][8])

---

## O) Debug/inspection utilities missing from your doc

### O1) Schema resolution: `collect_schema()` (expensive, but canonical)

Polars documents schema resolution as potentially expensive (disk metadata, remote network), and provides an explicit method. ([Polars User Guide][34])

### O2) Plan inspection nodes: `LazyFrame.inspect()`

Inspect a node in the computation graph—useful for “tap points” during plan debugging. ([Polars User Guide][35])

### O3) Expression meta introspection

`Expr.meta.*` includes root-name inspection, tree formatting, and serialization; this is the “introspect expression trees like ASTs” capability. ([Polars User Guide][36])

### O4) Memory observability primitives

* `DataFrame.estimated_size()` (buffer-level size estimate; includes nested arrays caveats) ([Polars User Guide][37])
* `rechunk()` (contiguous allocation guarantee) ([Polars User Guide][38])

---

## P) Testing utilities (`polars.testing.*`)

Not covered in your doc, but very useful for “golden output” pipelines:

* `assert_frame_equal`, `assert_series_equal`, etc., including knobs for row/col order and categorical comparison behavior. ([Polars User Guide][39])

---

## Q) SQL “advanced surface” beyond the basics

You cover SQL basics, but not:

* `SQLContext.execute(..., eager=...)` returning LazyFrame or DataFrame ([Polars User Guide][40])
* `register_globals/register_many/execute_global` and the broader SQL interface surface (for integrating multiple frames, global execution, and SQL function catalogs). ([Polars User Guide][41])

---

## R) Explicit caching nodes: `LazyFrame.cache()`

Polars has an explicit cache node (with guidance that the optimizer often does better), useful in niche cases where you want to force reuse. ([Polars User Guide][42])

---

If you want the next step (same as PyArrow): I can take **just the top 4–6 most leverage gaps** for single-box, few-consumer systems (plugins, optimizer toggles/profiling, batch sinks/generators, background/async, serialization) and write them as “deep dive sections” with minimal-but-sufficient implementation patterns + gotchas.

[1]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.collect.html?utm_source=chatgpt.com "polars.LazyFrame.collect — Polars documentation"
[2]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.QueryOptFlags.html?utm_source=chatgpt.com "polars.QueryOptFlags — Polars documentation"
[3]: https://docs.pola.rs/docs/python/dev/reference/lazyframe/api/polars.LazyFrame.profile.html?utm_source=chatgpt.com "polars.LazyFrame.profile — Polars documentation"
[4]: https://docs.pola.rs/py-polars/html/reference/api/polars.collect_all.html?utm_source=chatgpt.com "polars.collect_all — Polars documentation"
[5]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.group_by.html?utm_source=chatgpt.com "polars.DataFrame.group_by — Polars documentation"
[6]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.collect_batches.html?utm_source=chatgpt.com "polars.LazyFrame.collect_batches — Polars documentation"
[7]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.sink_batches.html?utm_source=chatgpt.com "polars.LazyFrame.sink_batches — Polars documentation"
[8]: https://docs.pola.rs/py-polars/html/reference/api/polars.LazyFrame.sink_parquet.html?utm_source=chatgpt.com "polars.LazyFrame.sink_parquet — Polars documentation"
[9]: https://docs.pola.rs/api/python/stable/reference/lazyframe/in_process.html?utm_source=chatgpt.com "InProcessQuery — Polars documentation"
[10]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.collect_async.html?utm_source=chatgpt.com "polars.LazyFrame.collect_async — Polars documentation"
[11]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.remote.html?utm_source=chatgpt.com "polars.LazyFrame.remote — Polars documentation"
[12]: https://docs.pola.rs/user-guide/plugins/expr_plugins/?utm_source=chatgpt.com "Expression Plugins - Polars user guide"
[13]: https://docs.pola.rs/api/python/dev/reference/api/polars.plugins.register_plugin_function.html?utm_source=chatgpt.com "polars.plugins.register_plugin_function"
[14]: https://docs.pola.rs/py-polars/html/reference/selectors.html?utm_source=chatgpt.com "Selectors — Polars documentation"
[15]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.melt.html?utm_source=chatgpt.com "polars.DataFrame.melt — Polars documentation"
[16]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.pivot.html?utm_source=chatgpt.com "polars.DataFrame.pivot — Polars documentation"
[17]: https://docs.pola.rs/api/python/version/0.18/reference/dataframe/index.html?utm_source=chatgpt.com "DataFrame — Polars documentation"
[18]: https://docs.pola.rs/py-polars/html/reference/dataframe/modify_select.html?utm_source=chatgpt.com "Manipulation/selection — Polars documentation"
[19]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.merge_sorted.html?utm_source=chatgpt.com "polars.DataFrame.merge_sorted — Polars documentation"
[20]: https://docs.pola.rs/api/python/version/0.20/reference/dataframe/modify_select.html?utm_source=chatgpt.com "Manipulation/selection — Polars documentation"
[21]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.update.html?utm_source=chatgpt.com "polars.LazyFrame.update — Polars documentation"
[22]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.with_row_index.html?utm_source=chatgpt.com "polars.DataFrame.with_row_index — Polars documentation"
[23]: https://docs.pola.rs/py-polars/html/reference/api/polars.scan_parquet.html?utm_source=chatgpt.com "polars.scan_parquet — Polars documentation"
[24]: https://docs.pola.rs/api/python/dev/reference/api/polars.read_csv.html?utm_source=chatgpt.com "polars.read_csv — Polars documentation"
[25]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.set_sorted.html?utm_source=chatgpt.com "polars.DataFrame.set_sorted — Polars documentation"
[26]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.with_columns_seq.html?utm_source=chatgpt.com "polars.DataFrame.with_columns_seq"
[27]: https://docs.pola.rs/docs/python/dev/reference/dataframe/api/polars.DataFrame.serialize.html?utm_source=chatgpt.com "polars.DataFrame.serialize — Polars documentation"
[28]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.serialize.html?utm_source=chatgpt.com "polars.LazyFrame.serialize — Polars documentation"
[29]: https://docs.pola.rs/py-polars/html/reference/expressions/api/polars.Expr.meta.serialize.html?utm_source=chatgpt.com "polars.Expr.meta.serialize — Polars documentation"
[30]: https://docs.pola.rs/releases/upgrade/1/?utm_source=chatgpt.com "Version 1"
[31]: https://docs.pola.rs/api/python/dev/reference/api/polars.read_parquet.html?utm_source=chatgpt.com "polars.read_parquet — Polars documentation"
[32]: https://docs.pola.rs/api/python/version/0.20/reference/api/polars.scan_csv.html?utm_source=chatgpt.com "polars.scan_csv — Polars documentation"
[33]: https://docs.pola.rs/py-polars/html/reference/api/polars.read_csv_batched.html?utm_source=chatgpt.com "polars.read_csv_batched — Polars documentation"
[34]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.collect_schema.html?utm_source=chatgpt.com "polars.LazyFrame.collect_schema"
[35]: https://docs.pola.rs/py-polars/html/reference/lazyframe/modify_select.html?utm_source=chatgpt.com "Manipulation/selection — Polars documentation"
[36]: https://docs.pola.rs/py-polars/html/reference/expressions/meta.html?utm_source=chatgpt.com "Meta — Polars documentation"
[37]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.estimated_size.html?utm_source=chatgpt.com "polars.DataFrame.estimated_size — Polars documentation"
[38]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.rechunk.html?utm_source=chatgpt.com "polars.DataFrame.rechunk — Polars documentation"
[39]: https://docs.pola.rs/py-polars/html/reference/testing.html?utm_source=chatgpt.com "Testing — Polars documentation"
[40]: https://docs.pola.rs/api/python/dev/reference/sql/api/polars.SQLContext.execute.html?utm_source=chatgpt.com "polars.SQLContext.execute — Polars documentation"
[41]: https://docs.pola.rs/api/python/dev/reference/sql/functions/index.html?utm_source=chatgpt.com "SQL Functions — Polars documentation"
[42]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.cache.html?utm_source=chatgpt.com "polars.LazyFrame.cache — Polars documentation"


## A) Polars optimizer control plane (per-query toggles + plan introspection) — deep dive (Polars 1.36+)

### Mental model: **LazyFrame = logical plan → optimizer passes → engine-specific physical plan → execution**

* In Polars’ lazy API, *nothing runs* until `collect` (or a sink) materializes the plan; this is what enables whole-query optimization. ([Polars User Guide][1])
* There are *multiple “views”* of a plan you care about:

  * **Unoptimized logical plan** (what you wrote).
  * **Optimized logical plan** (after rewrite passes like pushdown, CSE, simplification).
  * **Physical plan** (engine-specific; especially relevant for streaming). ([Polars User Guide][2])

---

# A1) Plan introspection primitives (what exists + what each is for)

## A1.1 `LazyFrame.explain(...)`: textual plan (optimized/unoptimized)

**Signature surface (still present in 1.36+):**

* `format={"plain","tree"}` (tree view is preferred; `tree_format` is deprecated). ([Polars User Guide][3])
* `optimized=True|False`: when `True`, the subsequent flags control which optimizations are applied for the explanation. ([Polars User Guide][3])
* Legacy per-pass knobs: `type_coercion`, `predicate_pushdown`, `projection_pushdown`, `simplify_expression`, `slice_pushdown`, `comm_subplan_elim`, `comm_subexpr_elim`, `cluster_with_columns`, plus `streaming` (marked “alpha” in these docs). ([Polars User Guide][3])

**Operational use**

* Use `optimized=False` to see “what you expressed”, then `optimized=True` to see what will execute. ([Polars User Guide][3])
* Use `format="tree"` for stable “diffable” plan output (and avoid the deprecated `tree_format`). ([Polars User Guide][3])

---

## A1.2 `LazyFrame.show_graph(...)`: visual plan (DOT/Graphviz)

**Key knobs**

* `optimized=True|False` (whether to optimize before rendering). ([Polars User Guide][4])
* `raw_output=True` returns DOT syntax (useful for embedding into your own tooling); cannot be combined with `show`/`output_path`. ([Polars User Guide][4])
* Requires Graphviz installed. ([Polars User Guide][4])
* Also exposes the same legacy optimization flags and a `streaming` flag. ([Polars User Guide][4])

**Streaming physical plan inspection**

* Polars’ streaming guide shows using `.show_graph(plan_stage="physical", engine="streaming")` to see the **physical plan** and streaming-specific operators/fallbacks. ([Polars User Guide][2])

---

## A1.3 `LazyFrame.collect_schema()`: force schema resolution (debug correctness, but can be expensive)

* `collect_schema()` resolves schema and warns it may require disk metadata reads or network requests for remote sources. ([Polars User Guide][5])
* This is your “schema contract checkpoint” when plan rewrites or UDF boundaries start producing surprising dtypes.

---

## A1.4 `LazyFrame.profile(...)`: per-node timing + optional Gantt chart

In Polars 1.36+, `profile` is the most actionable introspection tool because it returns:

* `(result_df, profile_df)` and states timings are in **microseconds**. ([Polars User Guide][6])
* `show_plot=True` to render a Gantt chart, plus `truncate_nodes`/`figsize` controls. ([Polars User Guide][6])

---

# A2) Per-query optimization toggles (1.36+ reality: legacy flags still exist, but the *modern* surface is `QueryOptFlags`)

## A2.1 Legacy boolean toggles (still in many signatures)

`LazyFrame.collect(...)` still exposes per-pass booleans like:

* `type_coercion`, `predicate_pushdown`, `projection_pushdown`, `simplify_expression`, `slice_pushdown`, `comm_subplan_elim`, `comm_subexpr_elim`, `cluster_with_columns`, plus `no_optimization`, `streaming`, and `background`. ([Polars User Guide][7])

However, in 1.36+ docs for “newer” signatures (example: `profile`), these per-pass booleans are explicitly marked **Deprecated since 1.30.0** in favor of the `optimizations` parameter. ([Polars User Guide][6])

## A2.2 The modern control plane: `optimizations: QueryOptFlags`

`QueryOptFlags` is explicitly “the set of optimizations considered during query optimization” and is marked **unstable** (may change without a breaking-change guarantee). ([Polars User Guide][8])

### `QueryOptFlags` surface area

* Constructor flags include: `predicate_pushdown`, `projection_pushdown`, `simplify_expression`, `slice_pushdown`, `comm_subplan_elim`, `comm_subexpr_elim`, `cluster_with_columns`, `collapse_joins`, `check_order_observe`, `fast_projection`. ([Polars User Guide][8])
* Helpers:

  * `.none(...)`: create an empty set (optionally toggling specific flags)
  * `.update(...)`: mutate the set (builder style)
  * `.no_optimizations()`: remove selected optimizations ([Polars User Guide][8])
* Documented semantics for several flags:

  * `predicate_pushdown`: apply filters early. ([Polars User Guide][8])
  * `projection_pushdown`: only read used columns. ([Polars User Guide][8])
  * `slice_pushdown`: push down limits/slices. ([Polars User Guide][8])
  * `comm_subplan_elim`: cache duplicate subplans. ([Polars User Guide][8])
  * `comm_subexpr_elim`: elide duplicate expressions and cache outputs. ([Polars User Guide][8])
  * `cluster_with_columns`: cluster independent `with_columns` calls. ([Polars User Guide][8])
  * `fast_projection`: inline “simple projections” to bypass the expression engine. ([Polars User Guide][8])
  * `check_order_observe`: don’t maintain order if it won’t be observed. ([Polars User Guide][8])
  * `collapse_joins`: “collapse a join and filters into a faster join” (documented on the 1.36+ `profile` surface). ([Polars User Guide][6])

### Minimal “disable exactly one pass” pattern

Use this when you’re isolating an optimizer bug or validating correctness:

```python
import polars as pl

opts = pl.QueryOptFlags.none().update(
    predicate_pushdown=False,   # isolate pushdown issues
)

# Use a call site that accepts `optimizations` in 1.36+ (e.g. profile / sinks / explain_all)
df, prof = lf.profile(optimizations=opts)
```

`profile(..., optimizations=QueryOptFlags)` is explicitly part of the 1.36+ surface, and legacy per-pass flags are deprecated in favor of it. ([Polars User Guide][6])

---

# A3) Multi-query optimizer control (fan-out workflows)

## A3.1 `polars.collect_all([...])`: run multiple LazyFrames together on the threadpool

`collect_all` runs multiple computation graphs in parallel on the Polars threadpool and exposes the same legacy optimization toggles + streaming mode. ([Polars User Guide][9])

## A3.2 `polars.explain_all([...], optimizations=...)`: view the *combined* plan

`explain_all` explains multiple LazyFrames “as if passed to collect_all” and notes that **Common Subplan Elimination** is applied on the combined plan so diverging queries can share work. ([Polars User Guide][10])

This is the most direct way to validate “are my 3 downstream queries sharing the same scan/transform subtree?”

---

# A4) Engine selection is part of the optimizer control plane (streaming vs in-memory vs GPU)

## A4.1 Streaming engine (bounded memory execution)

* The streaming guide recommends `collect(engine="streaming")` to execute in streaming mode (batch execution) and notes that Polars may fall back to the in-memory engine for non-streaming ops. ([Polars User Guide][2])

## A4.2 Default engine affinity (global policy)

* `pl.Config.set_engine_affinity(engine)` sets the default engine Polars will *attempt* to use for `.collect()` (not guaranteed). ([Polars User Guide][11])

## A4.3 Per-query engine selection + fallback visibility (GPU included)

On the 1.36+ `profile` surface:

* `engine="auto"` runs in-memory by default and respects `POLARS_ENGINE_AFFINITY`. ([Polars User Guide][6])
* `engine="gpu"` uses the GPU engine; you can pass a `GPUEngine` object for fine-grained control (device selection, etc). ([Polars User Guide][6])
* GPU mode is explicitly noted as unstable; fallback to the default engine should happen transparently when unsupported, and `POLARS_VERBOSE=1` provides fallback diagnostics. ([Polars User Guide][6])
* GPU engine does not support streaming (if streaming is enabled, GPU execution is switched off). ([Polars User Guide][6])

---

# A5) A pragmatic “optimizer debug loop” (what to do when results/perf look wrong)

1. **Confirm schema** *before* running a huge job:

```python
schema = lf.collect_schema()
```

`collect_schema()` can be expensive (metadata IO/network), but it’s the canonical dtype/column contract checkpoint. ([Polars User Guide][5])

2. **Diff plans**:

* `lf.explain(optimized=False, format="tree")` vs `lf.explain(optimized=True, format="tree")` to see what rewrites occurred. ([Polars User Guide][3])

3. **If streaming matters**, inspect the **physical** plan:

* `.show_graph(plan_stage="physical", engine="streaming")`. ([Polars User Guide][2])

4. **Turn on verbose logs** when isolating optimizer/engine fallbacks:

* `pl.Config.set_verbose(True)` (or context-managed `pl.Config(verbose=True)`). ([Polars User Guide][12])

5. **Bisect a suspect optimization pass** with `QueryOptFlags` (where supported):

* disable `predicate_pushdown`, `simplify_expression`, or `collapse_joins` one at a time and re-run `profile()` to see both correctness and node timing deltas. ([Polars User Guide][6])

---

If you want, I can do the next Polars deep dive section in the same style on **B) Streaming/batch execution control** (collect_batches/sink_batches + early stop + engine fallback) — it connects directly to the “physical plan” view and the optimizer controls above.

[1]: https://docs.pola.rs/user-guide/concepts/lazy-api/?utm_source=chatgpt.com "Lazy API"
[2]: https://docs.pola.rs/user-guide/concepts/streaming/ "Streaming - Polars user guide"
[3]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.explain.html "polars.LazyFrame.explain — Polars  documentation"
[4]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.show_graph.html "polars.LazyFrame.show_graph — Polars  documentation"
[5]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.collect_schema.html "polars.LazyFrame.collect_schema — Polars  documentation"
[6]: https://docs.pola.rs/api/python/dev/reference/lazyframe/api/polars.LazyFrame.profile.html "polars.LazyFrame.profile — Polars  documentation"
[7]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.collect.html "polars.LazyFrame.collect — Polars  documentation"
[8]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.QueryOptFlags.html "polars.QueryOptFlags — Polars  documentation"
[9]: https://docs.pola.rs/py-polars/html/reference/api/polars.collect_all.html "polars.collect_all — Polars  documentation"
[10]: https://docs.pola.rs/api/python/dev/reference/api/polars.explain_all.html "polars.explain_all — Polars  documentation"
[11]: https://docs.pola.rs/api/python/dev/reference/api/polars.Config.set_engine_affinity.html "polars.Config.set_engine_affinity — Polars  documentation"
[12]: https://docs.pola.rs/py-polars/html/reference/api/polars.Config.set_verbose.html "polars.Config.set_verbose — Polars  documentation"


## B) Polars streaming/batch execution control (`collect_batches` / `sink_batches` + early stop + engine fallback) — deep dive (Polars 1.36+)

### Mental model: streaming *execution* vs streaming *output*

* **Streaming execution** means Polars evaluates a lazy query **in batches** (bounded memory), but the *result* can still be a fully materialized `DataFrame` (e.g., `collect(engine="streaming")`). ([docs.pola.rs][1])
* **Streaming output** means Polars executes in batches and **emits** results incrementally—either to storage (**native sinks**) or to Python (**collect/sink batches**). ([docs.pola.rs][2])

When in doubt: **prefer native sinks** for throughput; use Python batch APIs only when you *must* run custom Python logic on each batch. ([docs.pola.rs][3])

---

# B1) Engine behavior + fallback (what “streaming” really means)

## B1.1 Streaming engine is batch-wise, but not every op is streamable

Polars explicitly notes that some operations are **inherently non-streaming** or not implemented in streaming yet; in those cases Polars will **fall back to the in-memory engine** for those operations. ([docs.pola.rs][1])

## B1.2 Inspect whether (and where) you fell back

For debugging memory/perf issues, Polars recommends inspecting the **physical plan** of a streaming query via:

```python
(
    lf
    .show_graph(plan_stage="physical", engine="streaming")
)
```

The streaming guide calls out that the physical plan’s legend indicates memory intensity and helps identify fallbacks. ([docs.pola.rs][1])

---

# B2) The “output plane” options (pick the right primitive)

## B2.1 Native sinks (fastest; preferred)

Sinks execute a query and **stream results to storage** (disk/cloud), avoiding a full in-RAM materialization and enabling batch processing while the file is still being read. ([docs.pola.rs][2])

Example: `sink_parquet` is explicitly “evaluate the query in streaming mode and write to a Parquet file”, enabling writes larger than RAM. ([docs.pola.rs][4])

### Multiplex sinks (run multiple sinks in one combined plan)

Polars documents that sinks can be made *lazy* and then executed together via `collect_all`, so you can write the same expensive pipeline to multiple outputs in one pass. ([docs.pola.rs][2])

```python
q1 = lf.sink_parquet("out.parquet", lazy=True)
q2 = lf.sink_ipc("out.arrow", lazy=True)
pl.collect_all([q1, q2])
```

([docs.pola.rs][2])

---

## B2.2 Python batch APIs (slowest; use only when necessary)

Polars provides two “Python sink” primitives:

* **Pull**: `collect_batches()` yields an iterator of sub-`DataFrame`s. ([docs.pola.rs][3])
* **Push**: `sink_batches(callback)` invokes your callback per batch, with explicit early stop semantics. ([docs.pola.rs][5])

Both are explicitly marked **unstable** and **much slower than native sinks** (crossing the Rust↔Python boundary repeatedly). ([docs.pola.rs][3])

---

# B3) `LazyFrame.collect_batches(...)` — pull-based batch emission

## B3.1 Full surface area

Signature (stable docs):

```text
LazyFrame.collect_batches(
  *, chunk_size: int|None=None,
     maintain_order: bool=True,
     lazy: bool=False,
     engine: EngineType="auto",
     optimizations: QueryOptFlags=()
) -> Iterator[DataFrame]
```

It “evaluates the query in streaming mode” and returns an iterator of chunks. ([docs.pola.rs][3])

### Semantics that matter operationally

* **Execution lifecycle:** “The query will always be fully executed unless `stop` is called,” and you should keep pulling until all chunks are seen. ([docs.pola.rs][3])
* **Performance warning:** explicitly “much slower than native sinks.” ([docs.pola.rs][3])
* **`chunk_size`:** number of rows buffered before yielding the next `DataFrame` chunk. ([docs.pola.rs][3])
* **`maintain_order`:** keep processing order; `False` is “slightly faster.” ([docs.pola.rs][3])
* **`lazy=True`:** don’t start the query until the first `next()` call. ([docs.pola.rs][3])
* **Engine selection:** `engine="auto"` runs using the **streaming engine**, tries `POLARS_ENGINE_AFFINITY`, and if the selected engine can’t run the query, falls back to the streaming engine. ([docs.pola.rs][3])

## B3.2 Minimal implementation pattern (stream → process → discard)

```python
import polars as pl

lf = pl.scan_parquet("big.parquet").filter(pl.col("ok") == True)

for batch_df in lf.collect_batches(chunk_size=50_000, maintain_order=True):
    # do side effects
    write_somewhere(batch_df)
    # drop reference to allow memory to be reclaimed before next batch
    del batch_df
```

This is the intended use: peel off chunks and avoid holding the full result in memory. ([docs.pola.rs][3])

## B3.3 Early termination: prefer `sink_batches`

The docs state the query runs fully unless “stop” is called, but they don’t spell out a stable Python-level stop API on the iterator beyond that statement. ([docs.pola.rs][3])
If you need a **well-defined early stop**, use `sink_batches`, where early stop is explicit in the callback contract. ([docs.pola.rs][5])

---

# B4) `LazyFrame.sink_batches(callback, ...)` — push-based batches with explicit early stop

## B4.1 Full surface area

Signature (stable docs):

```text
LazyFrame.sink_batches(
  function: Callable[[DataFrame], bool|None],
  *, chunk_size: int|None=None,
     maintain_order: bool=True,
     lazy: bool=False,
     engine: EngineType="auto",
     optimizations: QueryOptFlags=()
) -> LazyFrame|None
```

It “evaluates the query and calls a user-defined function for every ready batch.” ([docs.pola.rs][5])

### Critical semantics

* **Early stop:** if the callback returns `True`, Polars treats that as “no more results are needed” and stops early. ([docs.pola.rs][5])
* **Unstable + slow warning:** same warnings as `collect_batches` (unstable, slower than native sinks). ([docs.pola.rs][5])
* **`chunk_size` / `maintain_order`:** same meaning as `collect_batches`. ([docs.pola.rs][5])
* **`lazy=True`:** returns a `LazyFrame` that defers execution until `collect` is called. ([docs.pola.rs][5])
* **`optimizations` note:** has no effect if `lazy=True`. ([docs.pola.rs][5])
* **Engine selection:** same `engine="auto"` + `POLARS_ENGINE_AFFINITY` + fallback-to-streaming behavior. ([docs.pola.rs][5])

## B4.2 Minimal “stream to DB” pattern

```python
import polars as pl

lf = (
    pl.scan_parquet("big.parquet")
      .select(["id", "ts", "value"])
      .filter(pl.col("ts") >= 0)
)

def write_batch(df: pl.DataFrame) -> bool:
    df.write_database("target_table", connection="...", if_table_exists="append")
    return False  # keep going

lf.sink_batches(write_batch, chunk_size=100_000)
```

The callback runs per ready batch; return `True` only when you want to stop. ([docs.pola.rs][5])

## B4.3 Early-stop pattern (top-K / “enough rows collected”)

```python
limit = 250_000
seen = 0

def cb(df: pl.DataFrame) -> bool:
    global seen
    seen += df.height
    process(df)
    return seen >= limit  # True => stop early

lf.sink_batches(cb, chunk_size=50_000, maintain_order=False)
```

Early stopping via `True` is explicitly part of the contract. ([docs.pola.rs][5])

---

# B5) Engine selection & fallback in batch APIs (and how to reason about it)

## B5.1 Entry-point engine selection (`engine=...`)

For `collect_batches` (and the sink APIs), Polars documents:

* `engine="auto"` runs using the **streaming engine** by default,
* attempts `POLARS_ENGINE_AFFINITY`,
* and if it can’t run the query using the selected engine, it runs using the streaming engine. ([docs.pola.rs][3])

## B5.2 Intra-query fallback (streaming → in-memory for specific operators)

Even when using the streaming engine, Polars can fall back to the **in-memory engine for non-streaming operations**; inspect the physical plan to see where that happens. ([docs.pola.rs][1])

---

# B6) Batch sizing & ordering controls (and how they differ)

## B6.1 `chunk_size` (output buffering) vs streaming engine chunk size (execution morsels)

* `chunk_size` in `collect_batches`/`sink_batches` controls **when you get a batch** (rows buffered before yield/callback). ([docs.pola.rs][3])
* Separately, Polars has a global knob for the streaming engine’s **execution chunk size**: `pl.Config.set_streaming_chunk_size(size)`. Polars notes the default is chosen based on schema + thread pool size and can be too optimistic for large strings, leading to OOM; overriding can help. ([docs.pola.rs][6])

## B6.2 Ordering (`maintain_order`)

Both batch APIs (and sinks like `sink_parquet`) expose `maintain_order`; disabling it is “slightly faster.” ([docs.pola.rs][3])

---

If you want next: I can continue with **C) Background execution + cancellation handles** (in-process query handles) or **D) Async collection APIs**, in the same deep-dive style and still scoped to Polars 1.36+.

[1]: https://docs.pola.rs/user-guide/concepts/streaming/ "Streaming - Polars user guide"
[2]: https://docs.pola.rs/user-guide/lazy/sources_sinks/ "Sources and sinks - Polars user guide"
[3]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.collect_batches.html "polars.LazyFrame.collect_batches — Polars  documentation"
[4]: https://docs.pola.rs/api/python/stable/reference/api/polars.LazyFrame.sink_parquet.html "polars.LazyFrame.sink_parquet — Polars  documentation"
[5]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.sink_batches.html "polars.LazyFrame.sink_batches — Polars  documentation"
[6]: https://docs.pola.rs/py-polars/html/reference/api/polars.Config.set_streaming_chunk_size.html "polars.Config.set_streaming_chunk_size — Polars  documentation"


## C) Background execution + cancellation handles (in-process query handles) — deep dive (Polars 1.36+)

### Mental model: `collect(background=True)` turns “materialize now” into “start job + hand me a handle”

In Polars 1.36+, `LazyFrame.collect(..., background=True)` returns an **`InProcessQuery`** handle instead of a `DataFrame`. The handle is explicitly meant for **polling**, **blocking wait**, and **best-effort cancellation**. ([Polars][1])

---

# C1) Entry point + return types

## C1.1 `LazyFrame.collect(..., background=...) -> DataFrame | InProcessQuery`

The stable signature (1.36+) includes:

* `engine: EngineType = "auto"`
* `background: bool = False`
* `optimizations: QueryOptFlags = ()` ([Polars][1])

When `background=True`, Polars:

> “Run the query in the background and get a handle … [to] fetch the result or cancel the query.” ([Polars][1])

**Stability note:** Background mode is explicitly marked **unstable**. ([Polars][1])

### Minimal “start + wait” pattern

```python
import polars as pl

q = lf.collect(background=True)      # -> InProcessQuery
df = q.fetch_blocking()              # wait and get DataFrame
```

`InProcessQuery` is the handle type returned by `collect(background=True)`. ([Polars][1])

---

# C2) `InProcessQuery` API surface (the whole control plane)

The namespace exists specifically for background collection:
“This namespace becomes available by calling `LazyFrame.collect(background=True)`.” ([Polars][2])

## C2.1 `cancel()` — cooperative cancellation

```python
q.cancel()
```

Semantics: “Cancel the query at earliest convenience.” ([Polars][3])

> Practical implication: cancellation is cooperative, not a hard kill; you should treat it as “request stop” and build your own timeout/backstop logic.

## C2.2 `fetch()` — non-blocking poll

```python
df_or_none = q.fetch()
```

Semantics: returns a `DataFrame` if ready, otherwise returns `None`. ([Polars][4])

This is the core primitive for UI loops / job supervisors.

## C2.3 `fetch_blocking()` — synchronous wait

```python
df = q.fetch_blocking()
```

Semantics: “Await the result synchronously.” ([Polars][5])

---

# C3) Canonical implementation patterns (minimal-but-sufficient)

## C3.1 Polling loop with backoff (UI / service supervisor)

```python
import time
import polars as pl

q = lf.collect(background=True)

sleep_s = 0.01
while True:
    out = q.fetch()
    if out is not None:
        df = out
        break
    time.sleep(sleep_s)
    sleep_s = min(0.25, sleep_s * 1.5)
```

This uses the documented `fetch() -> DataFrame | None` contract. ([Polars][4])

## C3.2 Timeout + cancel (best-effort backstop)

```python
import time

q = lf.collect(background=True)

deadline = time.time() + 5.0  # seconds
while time.time() < deadline:
    df = q.fetch()
    if df is not None:
        break
    time.sleep(0.05)
else:
    q.cancel()      # request cancellation
    # choose your policy:
    # - return "timed out" immediately, or
    # - wait a little longer, or
    # - fall back to fetch_blocking() with your own external timeout mechanism
```

Cancellation is explicitly “earliest convenience”, so treat this as a request, not a guarantee. ([Polars][3])

## C3.3 Multiple concurrent background queries (bound concurrency to avoid threadpool thrash)

Background queries run on Polars’ threadpool resources. You can inspect the pool size via:

```python
n = pl.thread_pool_size()
```

`thread_pool_size()` returns the number of threads in the Polars thread pool, and notes you can cap it with `POLARS_MAX_THREADS` **before process start** (not changeable after), though Polars “strongly recommend[s] not to override” except niche contexts. ([Polars][6])

A practical pattern is to keep at most `~thread_pool_size()` background queries “active” at once (or fewer if each query is heavy).

---

# C4) Engine interaction (critical gotchas for background mode)

## C4.1 `engine="auto"` selection + fallback visibility

`collect(engine="auto")` runs with the in-memory engine by default, will also attempt the engine specified via `POLARS_ENGINE_AFFINITY`, and falls back to the in-memory engine if the chosen engine can’t execute the query. ([Polars][1])
If you want to see why a query fell back, Polars notes that running with `POLARS_VERBOSE=1` provides fallback information. ([Polars][1])

## C4.2 GPU engine does **not** support background (or streaming)

Polars explicitly states:

> “The GPU engine does not support streaming, or running in the background. If either are enabled, then GPU execution is switched off.” ([Polars][1])

So:

* If your environment tries to prefer GPU (engine affinity), `background=True` will force CPU execution.
* Don’t rely on `background=True` as a way to “queue GPU jobs”.

---

# C5) Optimizations + background (what you can control)

`collect` supports `optimizations: QueryOptFlags` (same control plane you used for optimizer debugging), but this parameter is marked **unstable** in the API docs. ([Polars][1])
In practice: if you’re bisecting a plan bug in a UI/service, you can run the same query:

* foreground (`background=False`) for deterministic error surfaces, then
* background (`background=True`) once stable.

---

If you want next: I can do the same deep dive for **D) Async collection APIs (`collect_async` / `collect_all_async`)** and show how it differs from background handles (awaitable vs poll/cancel) and how you’d choose between them in an orchestrator.

[1]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.collect.html "polars.LazyFrame.collect — Polars  documentation"
[2]: https://docs.pola.rs/api/python/stable/reference/lazyframe/in_process.html "InProcessQuery — Polars  documentation"
[3]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.lazyframe.in_process.InProcessQuery.cancel.html "polars.lazyframe.in_process.InProcessQuery.cancel — Polars  documentation"
[4]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.lazyframe.in_process.InProcessQuery.fetch.html "polars.lazyframe.in_process.InProcessQuery.fetch — Polars  documentation"
[5]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.lazyframe.in_process.InProcessQuery.fetch_blocking.html "polars.lazyframe.in_process.InProcessQuery.fetch_blocking — Polars  documentation"
[6]: https://docs.pola.rs/api/python/stable/reference/api/polars.thread_pool_size.html "polars.thread_pool_size — Polars  documentation"


## D) Async collection APIs (`collect_async` / `collect_all_async`) — deep dive (Polars 1.36+)

### Mental model: **async ≠ non-blocking compute**; it’s **non-blocking orchestration**

`collect_async`/`collect_all_async` don’t make the computation “async in Rust.” They **schedule the same `collect` work into a thread pool** and immediately return an awaitable (or a gevent wrapper), so your **event loop/greenlet scheduler can keep doing other work while Polars runs**. ([Polars][1])

This is *exactly* what you want in:

* asyncio web servers (FastAPI/Starlette) where you must not block the event loop thread,
* gevent apps where you want other greenlets to run. ([Polars][1])

---

# D1) `LazyFrame.collect_async(...)` — async single query

## D1.1 Full surface area (1.36+ docs)

Return type:

* `Awaitable[DataFrame]` (default), **or**
* `_GeventDataFrameResult[DataFrame]` if `gevent=True` ([Polars][1])

Signature parameters (all keyword-only after `*`):

* `gevent: bool = False` → return gevent AsyncResult wrapper instead of an awaitable ([Polars][1])
* optimizer pass toggles: `type_coercion`, `predicate_pushdown`, `projection_pushdown`, `simplify_expression`, `no_optimization`, `slice_pushdown`, `comm_subplan_elim`, `comm_subexpr_elim`, `cluster_with_columns` ([Polars][1])
* `streaming: bool = False` → “Process the query in batches to handle larger-than-memory data” (marked unstable); docs recommend using `explain()` to see if streaming is possible. ([Polars][1])

Core semantics:

* It “collects into a DataFrame (like `collect()`), but … is scheduled to be collected inside a thread pool, while this method returns almost instantly.” ([Polars][1])
* Intended for asyncio/gevent: “release control to other greenlets/tasks while LazyFrames are being collected.” ([Polars][1])
* Error propagation: on error, `set_exception` is used on the underlying `asyncio.Future` / `gevent.event.AsyncResult` and will be re-raised by them. ([Polars][1])

---

## D1.2 Minimal asyncio usage (single query)

```python
import polars as pl

async def handler(lf: pl.LazyFrame) -> pl.DataFrame:
    # event loop stays responsive while work runs in thread pool
    return await lf.collect_async()
```

The docs include an asyncio example using `await lf...collect_async()`. ([Polars][1])

---

## D1.3 gevent usage (`gevent=True`)

If you’re in a gevent app, you can request a wrapper around `gevent.event.AsyncResult`:

* the wrapper exposes `.get(block=True, timeout=None)` for retrieval. ([Polars][1])

```python
import polars as pl

res = lf.collect_async(gevent=True)
df = res.get(block=True, timeout=None)
```

---

## D1.4 Streaming + async (bounded memory) — the knobs and the reality

`collect_async(..., streaming=True)` exists and is explicitly described as “process in batches,” with a warning that streaming mode is unstable. ([Polars][1])
For real debugging, the docs explicitly suggest checking `explain()` to see if the query can be processed in streaming mode. ([Polars][1])

---

# D2) `polars.collect_all_async([...])` — async multi-query fanout

## D2.1 Full surface area (1.36+ docs)

Return type:

* `Awaitable[list[DataFrame]]` (default), **or**
* `_GeventDataFrameResult[list[DataFrame]]` if `gevent=True` ([Polars][2])

It’s the async twin of `polars.collect_all()`:

* “Collects into a list of DataFrame … scheduled … inside thread pool, while this method returns almost instantly.” ([Polars][2])
* Same gevent/asyncio rationale and same error propagation semantics (`set_exception` on Future/AsyncResult). ([Polars][2])
* Same optimization toggles + `streaming` flag. ([Polars][2])

## D2.2 Minimal asyncio usage (multi-query)

```python
import polars as pl

dfs = await pl.collect_all_async([lf1, lf2, lf3])
```

This collects multiple LazyFrames asynchronously and returns them in a list. ([Polars][2])

## D2.3 gevent usage

```python
res = pl.collect_all_async([lf1, lf2], gevent=True)
dfs = res.get()
```

The gevent wrapper behavior is identical in shape to `collect_async`. ([Polars][2])

---

# D3) Choosing between `collect_async`, `collect_all_async`, and background handles

### `collect_async`

Use when:

* you have **one** LazyFrame,
* you’re in asyncio/gevent and just need the event loop to stay responsive. ([Polars][1])

### `collect_all_async`

Use when:

* you have **many independent** LazyFrames to materialize and you want a single awaitable/wrapper. ([Polars][2])

### `collect(background=True)` (InProcessQuery handle)

Use when:

* you need an explicit **handle** that supports **polling and cancellation** (`fetch`, `fetch_blocking`, `cancel`).

> Practical distinction: `collect_async` gives you an awaitable; the docs do **not** specify cancellation semantics for the underlying job beyond error propagation. If you need first-class cancel/poll behavior, `background=True` is the API that explicitly provides it. ([Polars][1])

---

# D4) Concurrency, thread pools, and “not shooting yourself in the foot”

## D4.1 You’re stacking schedulers

* `collect_async` schedules *collection* in a thread pool. ([Polars][1])
* Polars then executes query work on its own internal thread pool (used by `collect_all`, `collect`, etc.). ([Polars][3])

So if you `await asyncio.gather(*(lf.collect_async() for lf in lfs))` on many frames, you can easily oversubscribe CPU and memory.

## D4.2 Bound concurrency explicitly (recommended pattern)

Use an asyncio semaphore to cap concurrent collections:

```python
import asyncio
import polars as pl

SEM = asyncio.Semaphore(2)  # tune to workload

async def collect_one(lf: pl.LazyFrame) -> pl.DataFrame:
    async with SEM:
        return await lf.collect_async()

dfs = await asyncio.gather(*(collect_one(lf) for lf in lfs))
```

## D4.3 Know your Polars thread pool size (and how to cap it)

* `pl.thread_pool_size()` returns the number of threads in the Polars thread pool. ([Polars][4])
* You can override it only by setting `POLARS_MAX_THREADS` **before process start**; docs strongly recommend not overriding except niche contexts. ([Polars][4])

---

# D5) Putting it to work in an asyncio web server (why this exists)

In a single-threaded asyncio server, `lf.collect()` blocks the event loop. `await lf.collect_async()` yields control while Polars runs in the background thread pool, so other requests can proceed. This is the exact motivation stated in the docs (“useful … if you use gevent or asyncio and want to release control”). ([Polars][1])

[1]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.collect_async.html "polars.LazyFrame.collect_async — Polars  documentation"
[2]: https://docs.pola.rs/py-polars/html/reference/api/polars.collect_all_async.html "polars.collect_all_async — Polars  documentation"
[3]: https://docs.pola.rs/py-polars/html/reference/api/polars.collect_all.html "polars.collect_all — Polars  documentation"
[4]: https://docs.pola.rs/py-polars/html/reference/api/polars.thread_pool_size.html?utm_source=chatgpt.com "polars.thread_pool_size — Polars documentation"



## E) GPU execution engine (newer engine mode) — deep dive (Polars 1.36+)

### Mental model: `engine="gpu"` is a *dispatch target* for the **LazyFrame** physical executor

Polars builds + optimizes the lazy query plan, then attempts to dispatch execution to a RAPIDS cuDF–backed GPU executor. If the plan (or part of it) isn’t supported, Polars can **fall back to the default CPU engine** unless you explicitly disable fallback. ([Polars][1])

---

# E1) Prereqs + installation (what you must have on a single box)

## E1.1 System requirements (current Open Beta constraints)

Polars’ GPU engine currently targets NVIDIA GPUs + Linux/WSL2:

* NVIDIA Volta or newer GPU (compute capability 7.0+) ([Polars][1])
* CUDA 12 (with a note about CUDA 11 support ending with RAPIDS v25.06) ([Polars][1])
* Linux or WSL2 ([Polars][1])

## E1.2 Install with the GPU feature flag

Polars exposes a `gpu` extra for Python (“Run queries on NVIDIA GPUs”). ([Polars][2])

```bash
pip install "polars[gpu]"
```

CUDA 11 special-case (per Polars docs): if you’re on CUDA 11 you must pin the CUDA11-specific cudf-polars package. ([Polars][1])

```bash
pip install polars cudf-polars-cu11==25.06
```

---

# E2) Enabling GPU execution (query-level control)

## E2.1 The basic switch: `collect(engine="gpu")`

Polars’ GPU mode is requested by using the Lazy API as normal and collecting with `engine="gpu"`. ([Polars][1])

```python
import polars as pl

q = pl.scan_parquet("data/*.parquet").filter(pl.col("x") > 0).group_by("k").agg(pl.sum("x"))
out = q.collect(engine="gpu")
```

**Engine semantics (Polars 1.36+):**

* `engine="auto"` runs with the Polars in-memory CPU engine; Polars may also attempt the engine set by `POLARS_ENGINE_AFFINITY`, and if it can’t run the query with the selected engine it falls back to the in-memory engine. ([Polars][3])
* `engine="gpu"` selects the GPU engine. ([Polars][3])

## E2.2 Fine-grained control: pass a `pl.GPUEngine(...)`

You can pass a `GPUEngine` object (rather than `"gpu"`) for configuration like device selection or strict “no fallback”. ([Polars][1])

```python
out = q.collect(engine=pl.GPUEngine(device=1))
```

## E2.3 Hard constraint: GPU engine disables itself if Polars streaming/background is enabled

Polars explicitly notes:

* GPU mode is unstable and may fall back; `POLARS_VERBOSE=1` provides fallback details. ([Polars][3])
* **GPU engine does not support Polars “streaming” or “background”**; if either is enabled, GPU execution is switched off. ([Polars][3])

So avoid combos like:

```python
q.collect(engine="gpu", background=True)   # GPU switched off
q.collect(engine="gpu", engine="streaming")  # not meaningful
```

---

# E3) Support matrix (what runs on GPU vs forces fallback)

Polars avoids promising a full expression-by-expression compatibility table; instead it lists supported/not supported categories.

## E3.1 Supported categories (high-level)

Currently supported includes:

* LazyFrame API + SQL API
* IO from CSV, Parquet, NDJSON, and in-memory CPU DataFrames
* numeric/logical/string/datetime types; string processing
* aggregations (including grouped + rolling), joins, filters, missing data, concatenation ([Polars][1])

## E3.2 Not supported (high-level)

Not supported includes:

* Eager DataFrame API
* Polars Streaming API
* several dtypes: Date, Categorical, Enum, Time, Array, Binary, Object (plus specific expressions for timezone datetime and list types)
* time-series resampling, folds, user-defined functions, Excel & database formats ([Polars][1])

**Practical implication:** if your pipeline touches *any* unsupported op/type, expect fallback unless you force “raise on fail”.

---

# E4) Fallback, diagnostics, and “strict GPU” mode

## E4.1 “Did my query actually use the GPU?”

Two Polars-native diagnostics paths:

### (a) Verbose mode emits `PerformanceWarning` on GPU non-support

In verbose mode, queries that can’t execute on the GPU emit a `PerformanceWarning` describing the reason, then complete via fallback. ([Polars][1])

```python
with pl.Config() as cfg:
    cfg.set_verbose(True)
    out = q.collect(engine="gpu")   # may warn + fall back
```

### (b) `POLARS_VERBOSE=1` fallback explanations

Polars also documents `POLARS_VERBOSE=1` as a way to get fallback information and reasons. ([Polars][3])

## E4.2 Disable fallback: `raise_on_fail=True`

At the Polars surface:

* `pl.GPUEngine(raise_on_fail=True)` causes GPU non-support to raise instead of falling back. ([Polars][1])

```python
out = q.collect(engine=pl.GPUEngine(raise_on_fail=True))
```

This is the right mode for CI (“this pipeline must remain GPU-compatible”) and for bisecting which operator breaks GPU support.

---

# E5) `pl.GPUEngine` config surface (Polars-level knobs)

## E5.1 Parameters that Polars guarantees at the API layer

`pl.GPUEngine` is a thin config wrapper with:

* `device: int | None` — choose GPU device (defaults to current CUDA device) ([Polars][4])
* `memory_resource` — an RMM `DeviceMemoryResource` for GPU allocations; must be valid for the selected device ([Polars][4])
* `raise_on_fail: bool` — disable CPU fallback ([Polars][4])
* `**kwargs` — forwarded backend options (see next section). ([Polars][4])

## E5.2 Minimal “multi-GPU box” patterns

```python
# Use GPU 0 (default) or explicitly:
engine0 = pl.GPUEngine(device=0)

# Use GPU 1:
engine1 = pl.GPUEngine(device=1)

# Strict: fail if anything forces fallback
strict = pl.GPUEngine(device=0, raise_on_fail=True)
```

Device selection + strictness are documented behaviors. ([Polars][4])

---

# E6) Deep backend controls via `**kwargs` (cudf-polars engine options)

Polars’ GPU engine is implemented by the `cudf-polars` backend. `cudf-polars` documents additional engine options that you typically pass through `pl.GPUEngine(**kwargs)` (or control via `CUDF_POLARS__...` environment variables). ([RAPIDS Docs][5])

## E6.1 Executor choice: `streaming` vs `in-memory`

`cudf-polars` has multiple executors:

* `streaming` executor (default as of RAPIDS 25.08) is equivalent to `engine="gpu"` / `pl.GPUEngine()`; it splits inputs into partitions and streams them through the query graph. ([RAPIDS Docs][6])
* `in-memory` executor can be faster when the data fits comfortably in device memory, but may rely on Unified Virtual Memory (UVM) if the data doesn’t fit. ([RAPIDS Docs][6])

```python
# default streaming executor
engine = pl.GPUEngine(executor="streaming")

# in-memory executor (often faster for smaller, VRAM-fitting workloads)
engine = pl.GPUEngine(executor="in-memory")
```

## E6.2 Streaming executor tuning (partition sizing + fallback policy)

Key `executor_options` patterns:

* Partition size tuning (e.g., `target_partition_size`) ([RAPIDS Docs][7])
* Split in-memory DataFrame inputs via `max_rows_per_partition` ([RAPIDS Docs][7])
* `fallback_mode` can raise/silence warnings when the streaming executor has to concatenate into one partition and effectively fall back to in-memory GPU execution for some operator. ([RAPIDS Docs][7])

```python
engine = pl.GPUEngine(
    executor="streaming",
    executor_options={
        "target_partition_size": 125_000_000,  # bytes
        "fallback_mode": "raise",
    },
)
out = q.collect(engine=engine)
```

## E6.3 Parquet reader options (memory pressure control)

`cudf-polars` documents a “chunked” parquet reader to cap peak memory usage (especially for compressed parquet), configured via `parquet_options`. ([RAPIDS Docs][6])

It also supports setting defaults via env vars like `CUDF_POLARS__PARQUET_OPTIONS__CHUNKED=0`. ([RAPIDS Docs][6])

## E6.4 “Best input shape” for the GPU streaming executor

The streaming executor “works best when the inputs … come from parquet files” and recommends `scan_parquet` instead of starting from existing DataFrames or CSV. ([RAPIDS Docs][7])

That’s a critical design constraint: you’ll often restructure pipelines to **scan Parquet early**, do heavy joins/aggs on GPU, then materialize.

## E6.5 Experimental multi-GPU execution (backend capability)

`cudf-polars` documents a distributed (“multi-GPU”) streaming cluster mode:

* set `executor_options={"cluster": "distributed"}`
* requires Dask + Dask-CUDA; recommends UCXX + RapidsMPF for networking
* warns distributed cluster is experimental and `collect` fails if you request distributed without a cluster deployed. ([RAPIDS Docs][7])

This is *backend* functionality; Polars’ own user guide still frames GPU support as Open Beta and (elsewhere) “single GPU implementation”, so treat multi-GPU as advanced/experimental. ([Polars][8])

---

# E7) Profiling GPU execution (what works, what doesn’t)

## E7.1 Polars `LazyFrame.profile` vs GPU executors

`cudf-polars` warns:

* the **streaming executor does not support Polars `LazyFrame.profile`**
* recommends NVIDIA Nsight Systems; `cudf-polars` includes NVTX annotations
* the `in-memory` executor *does* support `LazyFrame.profile(engine=pl.GPUEngine(executor="in-memory"))`. ([RAPIDS Docs][5])

So if you want per-node profiling inside Polars, switch to the in-memory executor temporarily.

---

# E8) Operational guidance + footguns (single-box reality)

## E8.1 Workloads that benefit most

Polars’ GPU guide suggests the biggest speedups occur when time is dominated by grouped aggregations and joins; IO-bound queries often look similar on CPU/GPU; GPUs have less RAM so very large datasets can OOM. ([Polars][1])

## E8.2 CPU/GPU interop and where data “lands”

* Both engines use Arrow columnar memory; files written by one engine can be read by the other. ([Polars][1])
* GPU execution is **Lazy-only**; the final materialized `DataFrame` returned from `collect(engine="gpu")` resides in CPU memory. ([Polars][1])

## E8.3 Avoid manual multiprocessing when using the GPU engine

Polars advises against combining Python multiprocessing with Polars (and explicitly adds: “If you’re using the GPU Engine … avoid manual multiprocessing”) because they compete for system resources and can reduce performance. ([Polars][9])

---

If you want next: the natural follow-on deep dive is **G) Rust expression plugins** *in the GPU context* (i.e., how UDF absence on GPU shifts you toward plugin kernels), or **N) IO deep knobs** specifically for Parquet scanning/sinking under GPU executors.

[1]: https://docs.pola.rs/user-guide/gpu-support/ "GPU Support [Open Beta] - Polars user guide"
[2]: https://docs.pola.rs/user-guide/installation/ "Installation - Polars user guide"
[3]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.collect.html "polars.LazyFrame.collect — Polars  documentation"
[4]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.lazyframe.engine_config.GPUEngine.html "polars.lazyframe.engine_config.GPUEngine — Polars  documentation"
[5]: https://docs.rapids.ai/api/cudf/stable/cudf_polars/usage/ "Usage — cudf 25.12.00 documentation"
[6]: https://docs.rapids.ai/api/cudf/stable/cudf_polars/engine_options/ "GPUEngine Configuration Options — cudf 25.12.00 documentation"
[7]: https://docs.rapids.ai/api/cudf/stable/cudf_polars/streaming_execution/ "Streaming Execution — cudf 25.12.00 documentation"
[8]: https://docs.pola.rs/user-guide/lazy/gpu/ "GPU Support - Polars user guide"
[9]: https://docs.pola.rs/user-guide/misc/multiprocessing/ "Multiprocessing - Polars user guide"


## G) Rust-native UDFs via Expression Plugins (preferred UDF mechanism) — deep dive (Polars 1.36+)

### Mental model: **“compile a Rust kernel → register it as an Expr node → optimizer treats it like native”**

Polars expression plugins are explicitly the *preferred* approach for user-defined functions: you compile a Rust function, Polars dynamically links it at runtime, and the expression runs “almost as fast as native expressions” **without Python involvement / no GIL contention**. ([Polars][1])
Because they enter the plan as a proper expression node, they inherit the same broad advantages as built-in expressions (optimization + parallelism + native performance). ([Polars][1])

---

# G1) The minimum viable architecture (what you actually build)

## G1.1 Build artifact: a `cdylib` + a Python package wrapper

On the Rust side, you compile a dynamic library (`crate-type = ["cdylib"]`) and depend on `polars`, `pyo3`, `pyo3-polars` (derive), and `serde` (for kwargs). ([Polars][1])
On the Python side, you ship a *package directory* containing the built library and a wrapper that exposes a Pythonic function returning a `pl.Expr` by calling `register_plugin_function`. ([Polars][1])

Polars’ guide is explicit about the required layout: a folder named to match `lib.name` in `Cargo.toml` (e.g. `expression_lib/`) with `__init__.py`, plus `src/`, `Cargo.toml`, and `pyproject.toml`. ([Polars][1])

## G1.2 Local build loop: `maturin develop --release`

The canonical dev path is installing `maturin` and running `maturin develop --release`, which builds and installs the plugin into your active environment so Polars can load the library from the package directory. ([Polars][1])

---

# G2) Rust side: function ABI, inputs/outputs, performance primitives

## G2.1 Required function shape

A Polars expression plugin function is (conceptually) a “kernel” that receives **one or more input `Series`** and returns a `Series`:

* You annotate it with `#[polars_expr(...)]`.
* It must accept `inputs: &[Series]` as its first parameter.
* It returns `PolarsResult<Series>`. ([Polars][1])

Example from the official guide:

* `#[polars_expr(output_type=String)] fn pig_latinnify(inputs: &[Series]) -> PolarsResult<Series> { ... }` ([Polars][1])

## G2.2 Output dtype: fixed (`output_type=...`) or derived (`output_type_func=...`)

### Fixed output type

Use `#[polars_expr(output_type=String)]` (or other dtype tokens) when the result dtype is invariant. ([Polars][1])

### Derived output type

When output dtype depends on input fields, use `output_type_func=...`, where the output-type function maps `&[Field]` → `Field` (name + dtype). The guide shows using `FieldsMapper` to implement “output dtype matches float supertype” and then switching on input dtype at runtime. ([Polars][1])

This is the *critical* move that keeps your plugin composable with Polars’ schema inference, pushdown, and plan validation: you’re telling the planner what dtype it will produce before execution. ([Polars][1])

## G2.3 kwargs: strongly typed config via `serde::Deserialize`

If you want non-expression parameters, the Polars guide’s pattern is:

* Define a Rust struct deriving `Deserialize`
* Accept it as the **second argument** in your plugin function (`fn f(inputs: &[Series], kwargs: MyKwargs) -> ...`). ([Polars][1])

On the Python side these are passed through `register_plugin_function(kwargs={...})` (see G3); Polars’ Python API requires they be **JSON-serializable**. ([Polars][2])

## G2.4 Performance footguns (string-heavy kernels)

Polars’ guide explicitly calls out using amortized string builders to avoid per-row allocations:

* `apply_into_string_amortized` instead of `apply_values` for string outputs
* for multi-input elementwise string outputs, consider `binary_elementwise_into_string_amortized` in `polars::prelude::arity`. ([Polars][1])

This matters because the whole point of plugins is to be “native-speed”; per-row heap churn will erase the advantage fast.

---

# G3) Python side: `register_plugin_function` contract (the *control plane*)

## G3.1 Core signature + what it means

`polars.plugins.register_plugin_function(...) -> Expr` takes: ([Polars][2])

* `plugin_path`: **either** the dynamic library file path **or** the directory containing it ([Polars][2])
* `function_name`: the Rust function name to resolve ([Polars][2])
* `args`: one or many “IntoExpr” arguments; these become the `inputs: &[Series]` passed to Rust ([Polars][2])
* `kwargs`: dict of non-expression args; must be **JSON serializable** ([Polars][2])

**Important**: the user guide is explicit that the function name must match exactly; otherwise Polars can’t resolve it when loading. ([Polars][1])

## G3.2 Behavior flags (these are not optional “hints” — they define correctness)

`register_plugin_function` is explicitly labeled “highly unsafe” because Polars will call a C function in the loaded library and will plan/execute based on the flags you provide. Misstating semantics can yield wrong results or subtle plan corruption. ([Polars][2])

Here are the flags you *must* reason about:

### `is_elementwise`

Indicates the function operates on scalars only, enabling fast paths. ([Polars][2])
Polars’ plugin guide also frames this as “allows Polars to run this expression in batches” (i.e., compatible with batch execution). ([Polars][1])
Set `True` only if each output row depends solely on the corresponding input row(s) (no cross-row state, no per-group state).

### `changes_length`

Marks that output length can differ from input (e.g., `unique`, `slice`). ([Polars][2])
If you forget this on a length-changing plugin, downstream operators that assume alignment (joins, `with_columns`, zips) can misbehave.

### `returns_scalar`

For “final aggregation” semantics: Polars will “automatically explode on unit length” in aggregation contexts for functions like `sum/min/covariance`. ([Polars][2])
Use this when your plugin behaves like an aggregation producing a single value per group/window.

### `cast_to_supertype`

Casts inputs to their supertype before execution. ([Polars][2])
Use when you support mixed numeric inputs but only implement one widened kernel path.

### `input_wildcard_expansion`

Expands wildcard expressions before executing the function. ([Polars][2])
This matters if your wrapper wants to accept `pl.all()`/regex selectors and the Rust side expects concrete expanded inputs.

### `pass_name_to_apply`

In group-by contexts, ensures the passed `Series` has its name set; costs an extra heap allocation per group. ([Polars][2])
Use only if your kernel depends on the series name (most don’t).

### `use_abs_path`

Resolves the library path to an absolute path; by default the dynamic library path is relative to the virtual environment. ([Polars][2])
This is a deployment knob: absolute resolution is often easier in containerized / hermetic runtime layouts.

---

# G4) Minimal end-to-end implementation skeleton (Rust + Python)

## Rust (`src/expressions.rs`)

* One arg: `inputs: &[Series]`
* Optional kwargs: `kwargs: MyKwargs`
* Annotate with `#[polars_expr(...)]` for output type / output type function ([Polars][1])

## Python (`your_plugin/__init__.py`)

* Set `PLUGIN_PATH = Path(__file__).parent`
* Return `register_plugin_function(plugin_path=PLUGIN_PATH, function_name="...", args=..., kwargs=..., is_elementwise=...)` ([Polars][1])

This is exactly the pattern Polars shows for a first plugin and for kwargs registration. ([Polars][1])

---

# G5) Ergonomics: expose plugins as a fluent Expr namespace (optional, but high leverage)

Polars’ guide notes you can “register a custom namespace” so users can call your plugin as `pl.col("x").language.pig_latinnify()` instead of importing free functions. ([Polars][1])
For real projects, this is usually worth doing: it keeps plugin APIs discoverable and avoids name collisions.

---

# G6) When plugins are the *right* answer vs Python UDFs

Polars Cloud’s docs summarize the trade-off cleanly:

* plugins integrate into the expression engine, behave like native operations, and generally outperform UDFs because they run compiled code
* UDFs are more flexible but pay Python overhead ([Polars][3])

The “local single-box” translation is: if you find yourself reaching for `map_elements/map_batches` for anything non-trivial, a plugin is typically the path to regain optimizer + parallelism + predictability. ([Polars][1])

[1]: https://docs.pola.rs/user-guide/plugins/expr_plugins/ "Expression Plugins - Polars user guide"
[2]: https://docs.pola.rs/api/python/dev/reference/api/polars.plugins.register_plugin_function.html "polars.plugins.register_plugin_function — Polars  documentation"
[3]: https://docs.pola.rs/polars-cloud/context/plugins/ "Plugins and custom libraries - Polars user guide"


## H) Selectors DSL (`polars.selectors`) as a first-class column-selection plane — deep dive (Polars 1.36+)

### Mental model: a **selector is a late-bound column set**, not “just an Expr”

Selectors exist to make **schema-driven** selection composable and ergonomic: they select columns by **name patterns**, **dtype families**, **position**, etc., and can **broadcast expression methods** across the selected columns. ([Polars][1])

Polars explicitly recommends importing them as `cs` and using them inside `DataFrame`/`LazyFrame` ops (including `group_by` and `agg`), e.g. group by all string columns and aggregate all numeric columns. ([Polars][1])

---

# H1) Import + the “broadcast” contract

```python
import polars as pl
import polars.selectors as cs

df.group_by(by=cs.string()).agg(cs.numeric().sum())
```

Polars describes selectors as unifying column selection functionality around `col()` *and* being able to broadcast expressions over the selected columns; the example above appears directly in the selectors docs. ([Polars][1])

**What “broadcast” means operationally**

* You can call expression methods (like `.sum()`, `.cast(...)`, `.str.*`, `.dt.*`, etc.) on a selector, and Polars applies that method to **each matched column** (producing multiple output expressions).
* This is why `cs.numeric().sum()` expands to multiple aggregations (one per numeric column), and why `group_by(cs.string())` can accept a selector for the grouping keys. ([Polars][1])

---

# H2) Selector set algebra (union/intersection/diff/complement) vs boolean ops

Selectors support **set operations**:

* **Union:** `A | B`
* **Intersection:** `A & B`
* **Difference:** `A - B`
* **Complement:** `~A` ([Polars][1])

Example patterns (from docs):

* union of temporal + string + prefix match,
* intersection of temporal with regex,
* difference,
* complement by dtype. ([Polars][1])

### Important gotcha: `|` / `&` on selectors are **set algebra**, not boolean expression logic

Polars explicitly warns: if you don’t want selector set operations, you can “materialize them as expressions by calling `as_expr`”, so operations like OR/AND are dispatched to the underlying expressions instead. ([Polars][1])

Practical rule:

* Use selector operators when you mean “combine column sets”.
* Use `.as_expr()` when you mean “operate on the values”.

---

# H3) The selector surface area (organized by intent)

Polars groups selectors into three main families plus “misc” helpers. ([Polars][2])

## H3.1 Dtype selectors (schema-driven)

Core dtype selectors include:

* `cs.numeric()`, `cs.integer()`, `cs.float()`, `cs.signed_integer()`, `cs.unsigned_integer()`
* `cs.string(include_categorical=...)`
* `cs.temporal()` plus `cs.date()`, `cs.datetime(time_unit=..., time_zone=...)`, `cs.duration(time_unit=...)`, `cs.time()`
* `cs.boolean()`, `cs.binary()`, `cs.decimal()`, `cs.categorical()` ([Polars][2])

**Dtype precision filtering**

* `cs.datetime(...)` and `cs.duration(...)` support optional filtering by time unit/zone/unit. ([Polars][2])

**Exact dtype matching**

* `cs.by_dtype(*dtypes)` selects columns matching the given dtype(s). ([Polars][1])

## H3.2 Name-pattern selectors (string-matching on column names)

* `cs.starts_with(*prefix)`, `cs.ends_with(*suffix)`
* `cs.contains(substring)` (literal substring(s), not regex)
* `cs.matches(pattern)` (regex)
* `cs.by_name(*names, require_all=...)`
* `cs.alpha(ascii_only=..., ignore_spaces=...)`, `cs.alphanumeric(...)`, `cs.digit(ascii_only=...)` ([Polars][1])

**Regex engine note**
`cs.matches` uses patterns compatible with the Rust `regex` crate (not Python’s `re`), which matters for feature support and syntax edge cases. ([Polars][1])

## H3.3 Positional selectors (index-based / scope-based)

* `cs.all()`
* `cs.first()`, `cs.last()` (“current scope”)
* `cs.by_index(*indices)` accepts ints or range objects; supports negative indexing. ([Polars][1])

---

# H4) Ordering semantics (this is where subtle bugs come from)

## H4.1 `cs.by_index`: order follows the selector, not schema order

Polars explicitly documents that matching columns are returned in the order the indices appear in the selector (not underlying schema order). ([Polars][1])

This matters when you build structured outputs (e.g., horizontal computations) and want deterministic column ordering.

## H4.2 `cs.by_name`: same rule + `require_all`

* `require_all=True` (default): match **all** names; if `False`, match **any** of the names. ([Polars][1])
* Ordering is also “in the order declared in the selector”, not schema order. ([Polars][1])

**If order matters and you’re mixing selector types**, the safest approach is to materialize the selection with `expand_selector` (next section) and use the returned tuple as the canonical ordered list. ([Polars][1])

---

# H5) Exclusion: three different APIs you must not conflate

## H5.1 `cs.exclude(...)` (selector-native exclusion; most flexible)

`cs.exclude(columns, *more_columns)` selects *all columns except* those matching given **names**, **dtypes**, or **selectors**. ([Polars][1])
Docs show it can mix a name, a selector, and a dtype in one call. ([Polars][1])

## H5.2 `Expr.exclude(...)` (expression-only exclusion; more limited)

`Expr.exclude` only works after wildcard/regex column selection, and you can’t pass both string names and dtypes; the docs explicitly suggest you may prefer selectors instead. ([Polars][3])

## H5.3 `pl.exclude(...)` (global sugar)

`pl.exclude(columns, *more_columns)` is syntactic sugar for `pl.all().exclude(columns)` (i.e., expression-level). ([Polars][4])

---

# H6) Introspection helpers: `expand_selector` + `is_selector`

## H6.1 `cs.expand_selector(target, selector, strict=True) -> tuple[str, ...]`

This is the “bridge” function you use to:

* debug what a compound selector actually matches,
* generate an explicit ordered column list for APIs that don’t accept selectors,
* unit-test selection logic against a schema contract.

It accepts a `DataFrame`, `LazyFrame`, or a schema mapping as the target, and returns a tuple of matching column names. ([Polars][1])

### `strict` parameter (critical)

* `strict=True` (default): expands **dedicated selectors**.
* `strict=False`: allows a broader range of “column selection expressions” (including bare columns or `.exclude()` constraints) to be expanded. ([Polars][1])

Polars shows examples of expanding against:

* a DataFrame,
* a LazyFrame,
* a standalone schema,
* and expanding a selector containing `.exclude()` by setting `strict=False`. ([Polars][1])

## H6.2 `cs.is_selector(obj) -> bool`

Useful for writing “accept IntoExpr or SelectorType” APIs and validating user inputs.
Polars shows `is_selector(pl.col(...)) == False` but `is_selector(cs.first() | cs.last()) == True`. ([Polars][1])

---

# H7) Advanced debugging: “is this *only* column selection?”

If you’re building libraries on top of Polars, you often want to distinguish “pure column selection” from arbitrary expressions. Polars provides:

* `Expr.meta.is_column_selection(allow_aliasing=False)` and explicitly states this can include bare columns, regex/dtype selections, selectors, and exclude ops. ([Polars][5])
  The examples show selectors (and selector `.exclude(...)`) counting as column selection. ([Polars][5])

This is extremely useful when you implement APIs that:

* accept either a list of columns **or** expressions,
* need to decide whether to treat an input as “selection” (multi-output) vs “computation” (single-output).

---

If you want to continue in the same style, the next most leverage Polars section to deep dive is **M) Serialization & persistence** (DataFrame/LazyFrame/Expr serialization, version stability, and security hazards), because selectors + late-bound plans + plugin expressions are where “persist a plan safely” gets tricky.

[1]: https://docs.pola.rs/py-polars/html/reference/selectors.html "Selectors — Polars  documentation"
[2]: https://docs.pola.rs/user-guide/expressions/expression-expansion/ "Expression expansion - Polars user guide"
[3]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.exclude.html "polars.Expr.exclude — Polars  documentation"
[4]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.exclude.html "polars.exclude — Polars  documentation"
[5]: https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.meta.is_column_selection.html "polars.Expr.meta.is_column_selection — Polars  documentation"


## I) Polars reshaping & structural transforms (wide/long + nesting utilities) — deep dive (Polars 1.36+)

### Mental model: reshape ops are *schema rewrites* (often requiring group semantics) + nested ops are *row topology rewrites*

Polars reshaping falls into two buckets:

* **Wide ↔ Long**: `melt`/`unpivot` (wide→long) and `pivot`/`unstack` (long→wide). These change the *column axis* meaningfully, and may require aggregation (pivot) or explicit cardinality/regularity guarantees (unstack). ([Polars][1])
* **Nesting ↔ Normalization**: `explode`/`implode` (list/array ↔ rows), and `unnest` / `struct.unnest` (struct ↔ columns). These change the *row axis* and “shape topology”. ([Polars][2])

---

# I1) Wide → Long: `unpivot` vs `melt` (same transformation, different parameterization)

## I1.1 `LazyFrame.unpivot(on=..., index=..., variable_name=..., value_name=...)`

**Signature + semantics (1.36+ stable):**

* `on`: value columns to unpivot; default `None` means “all columns not in `index`”.
* `index`: identifier columns to keep.
* `variable_name` / `value_name`: rename the output “variable” and “value” columns.
* `streamable`: present but explicitly marked “deprecated” in the docs.
* **Row order is unspecified**. ([Polars][3])

The docs explicitly map this to pandas melt: `index` ↔ `id_vars`, `on` ↔ `value_vars`, and call it `pivot_longer` in other ecosystems. ([Polars][3])

Minimal pattern:

```python
import polars as pl
import polars.selectors as cs

out = (
    lf
    .unpivot(cs.numeric(), index=["id", "ts"], variable_name="metric", value_name="x")
    .collect()
)
```

`unpivot` supports selectors for `on`/`index` and renaming variable/value. ([Polars][3])

## I1.2 `DataFrame.melt(id_vars=..., value_vars=..., variable_name=..., value_name=...)`

`melt` is the wide→long API that mirrors pandas naming:

* `id_vars`: identifier columns
* `value_vars`: measured columns (default: all non-`id_vars`)
* `variable_name` / `value_name` for output naming ([Polars][1])

Minimal pattern:

```python
import polars as pl
import polars.selectors as cs

out = df.melt(id_vars="id", value_vars=cs.numeric(), variable_name="metric", value_name="x")
```

Selectors are supported in `id_vars`/`value_vars`. ([Polars][1])

## I1.3 Lazy streaming knob: `LazyFrame.melt(..., streamable=True)`

`LazyFrame.melt` adds `streamable`:

* If the melt node runs in the streaming engine, **output ordering is not stable**. ([Polars][4])

That’s the only ordering guarantee you should rely on: if you need deterministic post-melt ordering, add an explicit `sort` on the identifier columns + `variable`. ([Polars][4])

## I1.4 “Same API eager vs lazy” (unpivot user guide)

The user guide states eager and lazy have the same unpivot API and demonstrates:

```python
out = df.unpivot(["C", "D"], index=["A", "B"])
```

with the canonical `variable`/`value` output columns. ([Polars][5])

---

# I2) Long → Wide: `pivot` (aggregation-aware) in eager vs lazy (1.36+)

## I2.1 `DataFrame.pivot(...)` — eager-only

Polars explicitly states: **DataFrame.pivot is only available in eager mode**. ([Polars][6])

Key parameters:

* `index`: group keys (rows of output)
* `columns`: column(s) whose values become new output columns
* `values`: values to place under new columns
* `aggregate_function`: required if multiple values exist per group; `None` raises if multiple values in a group ([Polars][6])
* `maintain_order`: sort grouped keys for predictable output order ([Polars][6])
* `sort_columns`: optionally sort transposed columns by name (vs discovery order) ([Polars][6])
* `separator`: delimiter used when multiple `values` columns generate compound output names ([Polars][6])

Minimal pattern:

```python
wide = df.pivot(
    index=["id"],
    columns=["metric"],
    values=["value"],
    aggregate_function="sum",
    sort_columns=True,
)
```

`aggregate_function` supports strings like `min/max/first/last/sum/mean/median/len` (and `None` with the “error on duplicates” semantics). ([Polars][6])

## I2.2 `LazyFrame.pivot(...)` — added in 1.36.0

Polars 1.36.0 release notes explicitly: **“Add LazyFrame.pivot”**. ([GitHub][7])

The stable API introduces the crucial lazy-only primitive:

* `on`: column(s) whose *values* become the new output columns
* `on_columns`: **the value combinations to consider for the output table** (this is what makes schema/planning deterministic without pre-collecting uniques) ([Polars][8])
* `index` / `values` similar to eager pivot (at least one must be provided) ([Polars][8])
* `aggregate_function`:

  * `None` → no aggregation, error on duplicates
  * predefined strings include `... 'len', 'item'`
  * **or an expression**; expression can only access the generated pivot “values” via `pl.element()` ([Polars][8])
* `maintain_order`: ensures `index` values are sorted by discovery order ([Polars][8])
* `separator`: naming delimiter for multi-values pivot outputs ([Polars][8])

Minimal “schema-deterministic lazy pivot”:

```python
import polars as pl

lf_wide = (
    lf.pivot(
        on="subject",
        on_columns=["maths", "physics"],   # explicit output columns
        index="name",
        values=["test_1", "test_2"],
        aggregate_function="first",
        separator="__",
    )
)
out = lf_wide.collect()
```

The “explicit output column domain” (`on_columns`) is the key lazy control plane for pivot. ([Polars][8])

---

# I3) `DataFrame.unstack(step=..., how=..., columns=..., fill_values=...)` — fast “long→wide” when the data is already regular

`unstack` is explicitly positioned as:

* **no aggregation**
* **much faster than pivot** because it can skip the grouping phase
* **unstable API** (may change without being considered breaking) ([Polars][9])

Core parameters:

* `step`: number of rows in the unstacked frame
* `how`: `'vertical' | 'horizontal'`
* `columns`: subset of columns to include (supports selectors)
* `fill_values`: values to pad when new shape doesn’t fit exactly ([Polars][9])

Use unstack when you can guarantee the “block structure” already exists (e.g., rows are already sorted and evenly partitioned) and you want pure reshaping without grouping overhead. ([Polars][9])

---

# I4) Nested ↔ normalized: `explode` / `implode` and struct unnesting

## I4.1 `explode`: List/Array → repeated rows

### Frame-level explode

Both `DataFrame.explode` and `LazyFrame.explode` accept column names/expressions/selectors, and the exploded columns must be `List` or `Array`. ([Polars][2])

```python
df2 = df.explode(["tokens", "tags"])
lf2 = lf.explode("tokens")
```

### Expression-level explode

`Expr.explode` is the list-expression explode primitive; Polars 1.36 adds explicit control over empties/nulls:

* `empty_as_null`: explode empty list/array into `null`
* `keep_nulls`: explode null list/array into `null` ([Polars][10])

```python
df.select(pl.col("values").explode(empty_as_null=True, keep_nulls=True))
```

(Flag availability is explicitly called out in the 1.36.0 release notes and shown in the expression API signature.) ([GitHub][7])

## I4.2 `implode`: rows → List scalar (invert explosion topology)

`pl.implode(*columns)` is syntactic sugar for `pl.col(name).implode()` and aggregates an entire column into a single list value. ([Polars][11])

```python
df.select(pl.implode("a", "b"))     # -> one-row DataFrame with list columns
df.select(pl.all().implode())       # implode all columns
```

`Expr.implode` returns a list-typed scalar per column. ([Polars][12])

---

# I5) Struct nesting: `unnest` vs `struct.unnest` (column explosion)

## I5.1 `DataFrame.unnest(columns=...)`

`DataFrame.unnest` decomposes struct columns into separate columns and **inserts the new columns at the location of the struct column**. ([Polars][13])

```python
out = df.unnest("my_struct")
```

## I5.2 `Expr.struct.unnest()` for “inline expansion” in `select/with_columns`

`Expr.struct.unnest` expands a struct into its fields and is explicitly an alias for `Expr.struct.field("*")`. ([Polars][14])

```python
df.select(pl.col("struct_col").struct.unnest())
```

## I5.3 Why Struct is central to “multi-output expressions”

Polars explains that expressions operate on one series and return another; `Struct` is the dtype that lets an expression conceptually output multiple columns (e.g., `value_counts` returns a struct of `{value,count}`), which you then `unnest` into columns. ([Polars][15])

---

# I6) List↔Struct/Array conversions (nesting utilities that feed reshape pipelines)

Polars’ list namespace includes conversion utilities that are often used right before/after reshape:

* `Expr.list.to_array(width)` converts a `List` column into a fixed-width `Array` column. ([Polars][16])
* `Expr.list.to_struct(...)` converts a `List` column into a `Struct` column (field strategy controls naming/behavior). ([Polars][16])

These are the “type-level” counterparts to `explode`/`unnest`: instead of changing row count, you change nested representation to enable downstream operations (e.g., struct-field extraction, fixed-width array ops). ([Polars][16])

---

## Practical reshape selection rules (single-box, high-performance default)

* Use **`LazyFrame.unpivot` / `LazyFrame.melt`** when you need wide→long in a pipeline; assume **row order is unspecified**, and treat `streamable`/streaming as “may reorder” unless you explicitly sort. ([Polars][3])
* Use **`LazyFrame.pivot`** (1.36+) when you need long→wide lazily and you can supply `on_columns` to make schema deterministic; use `pl.element()` only for aggregation expressions. ([Polars][8])
* Use **`DataFrame.unstack`** only when the data is already regular (no aggregation needed) and you want the fastest reshape; remember it’s explicitly unstable. ([Polars][9])
* For nested normalization: **`explode`** for list/array → rows (with explicit null/empty semantics in 1.36), **`implode`** for re-nesting, and **`unnest` / `struct.unnest`** for struct → columns. ([Polars][2])

[1]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.melt.html "polars.DataFrame.melt — Polars  documentation"
[2]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.explode.html "polars.DataFrame.explode — Polars  documentation"
[3]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.unpivot.html "polars.LazyFrame.unpivot — Polars  documentation"
[4]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.melt.html "polars.LazyFrame.melt — Polars  documentation"
[5]: https://docs.pola.rs/user-guide/transformations/unpivot/ "Unpivots - Polars user guide"
[6]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.pivot.html "polars.DataFrame.pivot — Polars  documentation"
[7]: https://github.com/pola-rs/polars/releases "Releases · pola-rs/polars · GitHub"
[8]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.pivot.html "polars.LazyFrame.pivot — Polars  documentation"
[9]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.unstack.html "polars.DataFrame.unstack — Polars  documentation"
[10]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.explode.html "polars.Expr.explode — Polars  documentation"
[11]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.implode.html "polars.implode — Polars  documentation"
[12]: https://docs.pola.rs/py-polars/html/reference/expressions/api/polars.Expr.implode.html "polars.Expr.implode — Polars  documentation"
[13]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.unnest.html "polars.DataFrame.unnest — Polars  documentation"
[14]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.struct.unnest.html "polars.Expr.struct.unnest — Polars  documentation"
[15]: https://docs.pola.rs/user-guide/expressions/structs/ "Structs - Polars user guide"
[16]: https://docs.pola.rs/py-polars/html/reference/expressions/list.html "List — Polars  documentation"



## J) Polars row index plumbing (and its optimizer implications) — deep dive (Polars 1.36+)

### Mental model: Polars has **no special “index” object** — a “row index” is just a **regular integer column**

Both eager and lazy “row index” APIs literally **insert a new first column**; the docs explicitly state it has *no special properties* and is a normal `UInt32` (or `UInt64` in big-index builds). ([Polars User Guide][1])
Because it’s “just a column”, any operation that reorders/filters/duplicates rows will carry that column along *like any other column*—so you must be intentional about **where** you generate it.

---

# J1) Index dtype & big-index builds (`UInt32` vs `UInt64`)

## J1.1 What dtype will my row index column use?

Polars exposes `pl.get_index_type()` to tell you the dtype used for indexing:

* `UInt32` in regular Polars
* `UInt64` in “bigidx” Polars ([Polars User Guide][2])

```python
import polars as pl

idx_dtype = pl.get_index_type()   # UInt32 or UInt64
```

## J1.2 Why this matters

If you might exceed ~4.29B rows in a single logical frame (or you simply want headroom), you want the big-index build; both `DataFrame.with_row_index` and `LazyFrame.with_row_index` explicitly note the index column becomes `UInt64` in that build. ([Polars User Guide][1])

---

# J2) The three ways to create row indices (and when each is the right primitive)

## J2.1 Eager: `DataFrame.with_row_index(name="index", offset=0)`

Adds a first column with an integer sequence:

* `offset` **cannot be negative**
* result is a normal integer column (`UInt32`/`UInt64`) ([Polars User Guide][1])

```python
df2 = df.with_row_index("rid", offset=1_000_000)
```

**Use when:** you already have a materialized `DataFrame` and you just need a row number column for downstream logic (debugging, exporting, “row-id for joins back”, etc.). ([Polars User Guide][1])

---

## J2.2 Lazy: `LazyFrame.with_row_index(name="index", offset=0)` (same semantics, but optimizer-visible)

Same surface:

* inserts as first column
* `offset` **cannot be negative**
* result is a regular integer column (`UInt32`/`UInt64`) ([Polars User Guide][3])

**But** it has an explicit optimizer warning:

> “can have a negative effect on query performance… may… block predicate pushdown optimization.” ([Polars User Guide][3])

**Use when:** you need the row index inside a lazy pipeline, but you can place it **late** (after the major scan-level pruning). ([Polars User Guide][3])

---

## J2.3 At scan/read time: `row_index_name` + `row_index_offset` (IO-time injection)

Most IO entry points support injecting a row index while reading/scanning:

* `scan_parquet(..., row_index_name=..., row_index_offset=...)` ([Polars User Guide][4])
* `scan_csv(..., row_index_name=..., row_index_offset=...)` ([Polars User Guide][5])
* `read_parquet(..., row_index_name=..., row_index_offset=...)` — explicitly “as the first column”; offset cannot be negative ([Polars User Guide][6])
* similarly for other scanners/readers (IPC/NDJSON/etc) ([Polars User Guide][7])

This is often the cleanest option when you want “row number of the *raw input rows*”.

**Use when:** you want row ids without inserting an extra plan node later (and without risking blocking pushdown via `with_row_index`). Note that `scan_parquet` is explicitly designed for scan-level predicate/projection pushdown. ([Polars User Guide][4])

---

## J2.4 Expression-generated row indices: `pl.row_index()` (added 1.32, still unstable)

`pl.row_index(name="index") -> Expr` generates an integer sequence where:

* length matches the **context length**
* dtype matches Polars’ index dtype
* recommended alternative for custom offsets/length/step/dtype is `int_range`
* explicitly marked **unstable**
* added in 1.32.0 ([Polars User Guide][8])

```python
df2 = df.with_columns(pl.row_index("rid"))
```

**Use when:** you specifically want “row index at this point in the expression pipeline” (e.g., after filtering), and you’re okay with the current “unstable” status. ([Polars User Guide][8])

---

# J3) Legacy APIs you may still see in old code: `with_row_count`

`LazyFrame.with_row_count(name="row_nr", offset=0)` exists but is **deprecated since 0.20.4** in favor of `with_row_index()` (and the default name changes from `row_nr` → `index`). It carries the same warning about blocking predicate pushdown. ([Polars User Guide][9])

---

# J4) Optimizer implications: why row indices can hurt pushdown (and how to structure pipelines)

## J4.1 The important fact: `LazyFrame.with_row_index` can block predicate pushdown

Polars states this directly in the API warning. ([Polars User Guide][3])

### Practical consequence

If you do:

```python
lf = (
  pl.scan_parquet("data/*.parquet")
    .with_row_index("rid")
    .filter(pl.col("x") > 0)
)
```

…the optimizer may not be able to push the filter fully into the scan, which is exactly what lazy execution is built to do. `scan_parquet` explicitly exists to enable scan-level pushdown, so you don’t want to accidentally erect a barrier before the filter. ([Polars User Guide][4])

## J4.2 The canonical “safe” shape: prune first, index last

When you only need row indices for downstream bookkeeping, attach them after the scan-level reductions:

```python
lf = (
  pl.scan_parquet("data/*.parquet")
    .select(["a", "b", "x"])        # projection pushdown opportunity
    .filter(pl.col("x") > 0)        # predicate pushdown opportunity
    .with_row_index("rid")          # now the barrier is late (less costly)
)
out = lf.collect()
```

This preserves the intent of `scan_parquet` (pushdown) while still giving you a row-id column at the end. ([Polars User Guide][4])

## J4.3 If your goal is “row id for *raw input rows*”: prefer scan-time injection

Instead of inserting a `with_row_index` node, inject at scan time:

```python
lf = pl.scan_parquet(
  "data/*.parquet",
  row_index_name="rid",
  row_index_offset=0,
).filter(pl.col("x") > 0)
```

This keeps the row index coupled to the scan operator and avoids the explicit `with_row_index` warning about blocking pushdown. (The scan operator is where Polars already wants to push filters/projections.) ([Polars User Guide][4])

---

# J5) Offsets, multi-file scans, and stable identity

## J5.1 Offsets are purely “start value” (non-negative)

* `with_row_index(..., offset=...)` cannot be negative. ([Polars User Guide][3])
* `read_parquet(..., row_index_offset=...)` explicitly cannot be negative (only used if `row_index_name` set). ([Polars User Guide][6])
* `scan_parquet` / `scan_csv` accept `row_index_offset` as “offset to start the row index column”. ([Polars User Guide][4])

## J5.2 “Row index” is not a globally stable identity by itself

Even though the row index column persists like any other column, it’s only stable with respect to the row ordering at the point you generate it. If your pipeline can change row ordering (joins, sorts, unions, multi-file scan ordering), you should treat “row index” as **a convenience**, not a durable primary key (unless you intentionally lock ordering and generate it at the right stage).

### Practical “durable identity” pattern

For multi-file ingestion, combine **file path** + **row index** to create a stable composite key. `read_parquet` can include file paths as a column (`include_file_paths`) when using the Rust reader. ([Polars User Guide][6])

---

If you want next: I can do the same deep-dive style for **K) Sortedness flags (`set_sorted`) and fast-path unlocks**, since it interacts tightly with “row index as ordering proxy” and with streaming/optimizer decisions.

[1]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.with_row_index.html "polars.DataFrame.with_row_index — Polars  documentation"
[2]: https://docs.pola.rs/py-polars/html/reference/api/polars.get_index_type.html "polars.get_index_type — Polars  documentation"
[3]: https://docs.pola.rs/docs/python/dev/reference/lazyframe/api/polars.LazyFrame.with_row_index.html "polars.LazyFrame.with_row_index — Polars  documentation"
[4]: https://docs.pola.rs/py-polars/html/reference/api/polars.scan_parquet.html "polars.scan_parquet — Polars  documentation"
[5]: https://docs.pola.rs/py-polars/html/reference/api/polars.scan_csv.html "polars.scan_csv — Polars  documentation"
[6]: https://docs.pola.rs/api/python/dev/reference/api/polars.read_parquet.html "polars.read_parquet — Polars  documentation"
[7]: https://docs.pola.rs/api/python/dev/reference/api/polars.scan_ipc.html?utm_source=chatgpt.com "polars.scan_ipc — Polars documentation"
[8]: https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.row_index.html "polars.row_index — Polars  documentation"
[9]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.with_row_count.html "polars.LazyFrame.with_row_count — Polars  documentation"


## K) “Sortedness” flags to unlock fast paths (dangerous if wrong) — deep dive (Polars 1.36+)

### Mental model: “sortedness” is **metadata** that can (a) unlock optimized algorithms and (b) satisfy correctness preconditions — but **Polars won’t re-check if you lie**

Polars exposes multiple “sortedness assertion” APIs (Expr/Series/DataFrame/LazyFrame). They all share two properties:

1. They **enable downstream fast paths for sorted arrays**. ([Polars User Guide][1])
2. They come with an explicit warning: **marking unsorted data as sorted can lead to incorrect results**. ([Polars User Guide][1])

### K1) API surface (where you can set the flag)

#### K1.1 Value-level / column-level

* `Series.set_sorted(descending=False) -> Series` (flags a `Series` as sorted; warns about incorrect results if not sorted). ([Polars User Guide][2])
* `Expr.set_sorted(descending=False) -> Expr` (flags an expression result as sorted; same warning; “enables downstream code to use fast paths”). ([Polars User Guide][1])

#### K1.2 Frame-level (one or more columns)

* `DataFrame.set_sorted(column, *more_columns, descending=False) -> DataFrame` (mark one/more columns sorted; same warning). ([Polars User Guide][3])
* `LazyFrame.set_sorted(column, *more_columns, descending=False) -> LazyFrame` (mark one/more columns sorted in the lazy plan). ([Polars User Guide][4])

> **Interpretation:** the docs phrase this as “indicate that one or multiple columns are sorted.” It does **not** explicitly define lexicographic “sorted by (a,b,c)” semantics; treat it as “these columns are individually sorted” unless you have verified a specific operator consumes multi-column sortedness. ([Polars User Guide][4])

---

### K2) When sortedness is a correctness requirement (not just an optimization)

#### K2.1 As-of join requires sorted keys

`DataFrame.join_asof(...)` requires both frames be sorted by the `on` key (Polars states this explicitly). ([Polars User Guide][5])

If your data is *already* sorted (e.g., upstream guarantees it, or it’s naturally ordered), Polars error messages explicitly suggest: **set the sorted flag**; otherwise, sort first. ([GitHub][6])

#### K2.2 Time-window grouping requires sorted index columns

`group_by_dynamic` documents a hard correctness condition:

* index column must be sorted ascending;
* if `group_by` is used, it must be sorted ascending **within each group**. ([Polars User Guide][7])

That “within each group” clause is the reason Polars can’t always infer/verify sortedness cheaply from global metadata; you must structure your pipeline (sort, or assert sortedness only if you’re sure).

#### K2.3 Merge of two sorted frames expects sortedness

`DataFrame.merge_sorted(other, key)` assumes both frames are already sorted by `key`, and the output is also sorted. The docs explicitly say it’s **the caller’s responsibility** to ensure sortedness or “the output will not make sense.” ([Polars User Guide][8])

---

### K3) “Fast path” use cases you can actually operationalize

#### K3.1 Avoid redundant sorts when your upstream guarantees order

If you read from an upstream that already guarantees ordering (e.g., ingest pipeline writes pre-sorted partitions), you can *assert* sortedness rather than re-sorting (which is expensive). Use frame-level marking for join/group operations:

```python
import polars as pl

df = pl.read_parquet("events_sorted_by_ts.parquet")
df = df.set_sorted("ts")   # assert sortedness (dangerous if wrong)
out = df.join_asof(other.set_sorted("ts"), on="ts")
```

The principle “if already sorted, set sorted flag; otherwise sort” is exactly the guidance Polars surfaces in error messaging around sorted-required operations. ([GitHub][6])

#### K3.2 Preserve “sorted-output” invariants through specialized operators

For operations like `merge_sorted`, the output is guaranteed (by contract) to remain sorted when inputs are sorted. Use this to build pipelines that never re-sort between stages. ([Polars User Guide][8])

---

### K4) Hard warnings / footguns

1. **Incorrect results are on you.** Every `set_sorted`/`set_sorted()` variant warns explicitly that wrong metadata can lead to incorrect results. ([Polars User Guide][1])
2. **Sortedness may be required “within groups”.** For time-series grouping with `group_by`, correctness depends on per-group ordering, not only global ordering. ([Polars User Guide][7])
3. **Prefer `sort` when in doubt.** Polars’ own error guidance emphasizes sorting if you can’t guarantee sortedness. ([GitHub][6])

---

## L) Sequential evaluation primitives (`*_seq`) for dependency-sensitive transforms — deep dive (Polars 1.36+)

### Mental model: `*_seq` changes **scheduling**, not “assignment semantics”

Polars normally evaluates many expressions in `select`/`with_columns` in parallel (and the optimizer may reorder them). The `*_seq` variants run the expression set **sequentially instead of in parallel**, and Polars explicitly recommends them when the **work per expression is cheap** (because parallel scheduling overhead can dominate). ([Polars User Guide][9])

Crucially: `*_seq` **does not guarantee** “newly created columns become available to later expressions in the same call.” The community has repeatedly requested “sequential assignment” semantics and there are open issues discussing that distinction. ([GitHub][10])

---

### L1) API surface

#### L1.1 Frame transformations

* `DataFrame.with_columns_seq(*exprs, **named_exprs) -> DataFrame`
  “run all expression sequentially instead of in parallel… when work per expression is cheap.” ([Polars User Guide][9])
* `LazyFrame.with_columns_seq(*exprs, **named_exprs) -> LazyFrame`
  same semantics for lazy plans. ([Polars User Guide][11])

#### L1.2 Projection/selection

* `DataFrame.select_seq(*exprs, **named_exprs) -> DataFrame`
  “run all expression sequentially instead of in parallel… when work per expression is cheap.” ([Polars User Guide][12])
* `LazyFrame.select_seq(*exprs, **named_exprs) -> LazyFrame`
  same semantics for lazy. ([Polars User Guide][13])

---

### L2) When `*_seq` is the correct primitive

#### L2.1 Many tiny expressions (scheduling overhead dominates)

Use `*_seq` when you have *lots* of cheap columnwise transforms (casts, simple arithmetic, lightweight string ops) and you don’t want the overhead of parallel orchestration. That’s exactly what the docs recommend. ([Polars User Guide][9])

```python
df2 = df.with_columns_seq(
    pl.col("a").cast(pl.Int64),
    pl.col("b").fill_null(0),
    (pl.col("c") * 3).alias("c3"),
)
```

#### L2.2 Deterministic evaluation order for “side-effect-ish” UDF boundaries

Even though Polars expressions are pure, Python UDF boundaries (e.g., `map_elements` / `map_batches`) can behave like side-effectful computation from a debugging standpoint (timing, exceptions). Sequential scheduling can make diagnosing UDF failures easier because execution becomes more predictable.

---

### L3) What `*_seq` does **not** do (gotchas you should code around)

#### L3.1 It is **not** “imperative assignment”

Users often expect:

```python
df.with_columns_seq(
  (pl.col("a") + 1).alias("a1"),
  (pl.col("a1") + pl.col("a")).alias("a2"),   # expects to see a1
)
```

…but Polars does not document `with_columns_seq` as “assign each expression then expose it” — it documents it as changing parallelism. The existence of open issues requesting assignment semantics is the best signal of the distinction. ([GitHub][10])

**Robust pattern:** chain `with_columns` calls (or use the walrus/alias capture idiom if you’ve standardized on it in your codebase).

#### L3.2 Prefer explicit chaining for true dependencies

```python
df2 = (
  df
  .with_columns((pl.col("a") + 1).alias("a1"))
  .with_columns((pl.col("a1") + pl.col("a")).alias("a2"))
)
```

This is the “always correct” approach when you genuinely need derived columns.

---

## M) Serialization & persistence of frames/plans/expressions (security + version stability) — deep dive (Polars 1.36+)

### Mental model: there are **three** serialization planes, each with different guarantees

1. **DataFrame serialization** (data + schema)
2. **LazyFrame serialization** (logical plan)
3. **Expr serialization** (expression AST-ish structure)

They are primarily intended for **short-lived caching, tests, and intra-version tooling**, not long-lived interchange; Polars explicitly warns serialization is **not stable across Polars versions**. ([Polars User Guide][14])

---

### M1) DataFrame serialization (`DataFrame.serialize` / `DataFrame.deserialize`)

#### M1.1 Deserialize formats

`DataFrame.deserialize(source, format=...)` supports:

* `"binary"` (default): deserialize from bytes
* `"json"`: deserialize from JSON string ([Polars User Guide][14])

#### M1.2 Version stability

Polars’ docs explicitly warn that serialization is **not stable across versions** (a frame serialized in one version may not deserialize in another). ([Polars User Guide][14])

**Operational implication:** if you persist serialized DataFrames, store the producing Polars version alongside the artifact and treat the artifact as tied to that version.

---

### M2) LazyFrame plan serialization (`LazyFrame.serialize` / `LazyFrame.deserialize`)

#### M2.1 What is serialized

`LazyFrame.serialize` serializes the **logical plan** (not the computed result). It’s explicitly “serialize the logical plan … to a file or string”. ([Polars User Guide][15])

#### M2.2 Formats and deprecations

Polars documents `LazyFrame.serialize` with:

* `"binary"` (default) and
* `"json"` (string) — **deprecated** ([Polars User Guide][16])

So for 1.36+ “future-proof” persistence: default to binary unless you have a specific reason to use JSON.

#### M2.3 Deserialize formats

`LazyFrame.deserialize` supports:

* `"binary"` (default)
* `"json"` (string) ([Polars User Guide][17])

#### M2.4 Security: pickle hazards when Python UDFs are present

`LazyFrame.deserialize` warns that it uses `pickle` when the plan contains Python UDFs, and that deserializing can execute arbitrary code; only attempt on **trusted** data. ([Polars User Guide][18])

#### M2.5 Version stability

Polars warns logical-plan serialization is **not stable across versions**. ([Polars User Guide][16])

---

### M3) Expression serialization (`Expr.meta.serialize` / `Expr.deserialize`)

#### M3.1 Serialize formats

`Expr.meta.serialize(..., format=...)` supports:

* `"binary"` (default)
* `"json"` (string) ([Polars User Guide][19])

#### M3.2 Version stability

Expression serialization is **not stable across Polars versions** (same warning). ([Polars User Guide][19])

#### M3.3 Security: pickle hazards with Python UDFs

`Expr.deserialize` warns it uses `pickle` when the logical plan contains Python UDFs, inheriting pickle’s security implications; deserializing can execute arbitrary code; only use with trusted data. ([Polars User Guide][20])

---

### M4) Best-practice patterns (single-box systems)

#### M4.1 Persist “plans” only for short-lived, same-version use

Use LazyFrame/Expr serialization for:

* caching expensive query construction,
* unit tests (“golden plan snapshot”),
* sending plans between processes in the *same* deployment image.

But don’t treat it as a stable interchange format. Polars’ explicit “not stable across versions” warning is the key constraint. ([Polars User Guide][16])

#### M4.2 For long-lived interchange: use IO formats, not Polars serialization

If you need durability and cross-version compatibility, persist:

* Parquet/IPC/CSV/NDJSON (data),
* and store the transformation spec in your own stable DSL/config (or in code),
  rather than relying on Polars’ internal serialization artifacts.

#### M4.3 Treat any artifact that might embed Python UDFs as untrusted input

If your plans/expressions might include Python UDF nodes, you must treat deserialization as “code execution” and only accept trusted sources. This is explicitly stated in both LazyFrame and Expr deserialization docs. ([Polars User Guide][18])

[1]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.set_sorted.html "polars.Expr.set_sorted — Polars  documentation"
[2]: https://docs.pola.rs/py-polars/html/reference/series/api/polars.Series.set_sorted.html?utm_source=chatgpt.com "polars.Series.set_sorted — Polars documentation"
[3]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.set_sorted.html?utm_source=chatgpt.com "polars.DataFrame.set_sorted — Polars documentation"
[4]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.set_sorted.html "polars.LazyFrame.set_sorted — Polars  documentation"
[5]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.join_asof.html?utm_source=chatgpt.com "polars.DataFrame.join_asof — Polars documentation"
[6]: https://github.com/pola-rs/polars/issues/9931?utm_source=chatgpt.com "set_sorted seems too often necessary · Issue #9931"
[7]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.group_by_dynamic.html?utm_source=chatgpt.com "polars.DataFrame.group_by_dynamic"
[8]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.merge_sorted.html?utm_source=chatgpt.com "polars.DataFrame.merge_sorted — Polars documentation"
[9]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.with_columns_seq.html "polars.DataFrame.with_columns_seq — Polars  documentation"
[10]: https://github.com/pola-rs/polars/issues/11570?utm_source=chatgpt.com "`.with_columns_seq()` doesn't recognize transformations ..."
[11]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.with_columns_seq.html "polars.LazyFrame.with_columns_seq — Polars  documentation"
[12]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.select_seq.html?utm_source=chatgpt.com "polars.DataFrame.select_seq — Polars documentation"
[13]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.select_seq.html?utm_source=chatgpt.com "polars.LazyFrame.select_seq — Polars documentation"
[14]: https://docs.pola.rs/api/python/dev/reference/dataframe/api/polars.DataFrame.deserialize.html "polars.DataFrame.deserialize — Polars  documentation"
[15]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.serialize.html "polars.LazyFrame.serialize — Polars  documentation"
[16]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.serialize.html?utm_source=chatgpt.com "polars.LazyFrame.serialize — Polars documentation"
[17]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.deserialize.html?utm_source=chatgpt.com "polars.LazyFrame.deserialize — Polars documentation"
[18]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.deserialize.html "polars.LazyFrame.deserialize — Polars  documentation"
[19]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.meta.serialize.html?utm_source=chatgpt.com "polars.Expr.meta.serialize — Polars documentation"
[20]: https://docs.pola.rs/py-polars/html/reference/expressions/api/polars.Expr.deserialize.html "polars.Expr.deserialize — Polars  documentation"


## N) IO deep knobs (scan/read/write “planner switches”) — deep dive (Polars 1.36+)

### Mental model: `scan_*` builds **optimizable** LazyFrame IO nodes; `read_*` materializes now; `sink_*` executes and writes without materializing a full frame

* `scan_parquet` explicitly exists so the optimizer can push **predicates + projections** into the scan node (less IO, less decode, less RAM). ([Polars User Guide][1])
* `read_parquet` is eager materialization with additional backend toggles (Rust vs PyArrow) and cloud connectivity.
* `sink_parquet` is “execute in streaming mode and write” (out-of-core write path) and is explicitly labeled **unstable**.

---

### N1) Parquet scanning (planner-friendly): `pl.scan_parquet(...)`

#### Full surface area (the knobs that matter)

`scan_parquet(source, *, n_rows, row_index_name, row_index_offset, parallel, use_statistics, hive_partitioning, glob, hive_schema, rechunk, low_memory, cache, storage_options, retries) -> LazyFrame` ([Polars User Guide][1])

Key switches and what they really control:

* **`use_statistics`**: enables row group/page skipping using Parquet statistics. ([Polars User Guide][1])
* **`hive_partitioning` + `hive_schema`**: infer or provide partition column schema so partitions can be pruned; `hive_schema` is explicitly marked **unstable**. ([Polars User Guide][1])
* **`parallel={'auto','columns','row_groups','none'}`**: controls direction of parallelism; this can change perf cliffs (e.g., many columns vs many row groups). ([Polars User Guide][1])
* **`rechunk`**: when scanning multiple files, optionally compact the final result into contiguous chunks (predictable perf, but can spike RAM). ([Polars User Guide][1])
* **`low_memory`**: reduce memory pressure at cost of speed. ([Polars User Guide][1])
* **`cache`**: cache the scan result; useful for repeated `.collect()`/multi-sink fanout, but increases memory retention. ([Polars User Guide][1])
* **Cloud access: `storage_options` + `retries`**: supported providers AWS/GCP/Azure, and if not provided Polars tries to infer from env vars; `retries` is retry count for cloud failures. ([Polars User Guide][1])

#### Minimal “lakehouse scan” pattern (pushdown-first)

```python
import polars as pl

lf = (
    pl.scan_parquet("s3://bucket/events/*.parquet", storage_options={...})
      .filter(pl.col("dt") >= "2025-12-01")
      .select(["dt", "user_id", "event_type"])
)
df = lf.collect()
```

This is the canonical scan → pushdown → collect shape Polars is designed for. ([Polars User Guide][1])

---

### N2) Parquet reading (eager + backend selection): `pl.read_parquet(...)`

#### Full surface area (planner switches + backend switches)

Key parameters include:

* `columns` (projection by name or index)
* `row_index_name` / `row_index_offset` (non-negative offset)
* `parallel`, `use_statistics`, `hive_partitioning`, `glob`, `hive_schema` (unstable), `rechunk`, `low_memory`
* cloud: `storage_options` (AWS/GCP/Azure or forwarded to `fsspec.open`) + `retries`
* **backend toggle**: `use_pyarrow` + `pyarrow_options` + `memory_map` (only used when `use_pyarrow=True`)

Backend semantics:

* `use_pyarrow=True` switches to PyArrow parquet reader; docs explicitly call it “more stable” and allow passing `pyarrow_options`; `memory_map` is only used in this mode.

#### Minimal “stability-first” pattern (PyArrow backend)

```python
import polars as pl

df = pl.read_parquet(
    "data.parquet",
    use_pyarrow=True,
    pyarrow_options={"use_threads": True},
    memory_map=True,
)
```

Use when the Rust reader doesn’t support some corner feature or when you want PyArrow’s compatibility surface.

---

### N3) CSV scanning: `pl.scan_csv(...)` (schema inference control plane)

#### Full surface area you actually use in production

`scan_csv(..., schema=None, schema_overrides=None, infer_schema_length=..., ignore_errors=..., null_values=..., missing_utf8_is_empty_string=..., with_column_names=..., try_parse_dates=..., low_memory=..., rechunk=..., row_index_name=..., row_index_offset=..., truncate_ragged_lines=..., decimal_comma=..., encoding=..., glob=..., ...) -> LazyFrame` ([Polars User Guide][2])

Knobs with the highest leverage:

* **Schema control**

  * `schema`: full schema disables inference entirely. ([Polars User Guide][2])
  * `schema_overrides`: override specific dtypes during inference (partial overwrite). ([Polars User Guide][2])
  * `infer_schema_length`: number of rows to scan for inference; `0` forces all columns to `pl.String`; `None` may scan full data and is slow.

* **Failure-mode controls**

  * `ignore_errors`: keep reading through parse errors; docs explicitly suggest `infer_schema_length=0` first when debugging bad values.
  * `truncate_ragged_lines`: truncate lines longer than schema (ragged rows). ([Polars User Guide][2])
  * `raise_if_empty`: choose empty LazyFrame vs raising `NoDataError`. ([Polars User Guide][2])

* **Null handling**

  * `null_values`: single string, list of strings, or per-column dict mapping to null sentinel.
  * `missing_utf8_is_empty_string`: treat missing utf8 as `""` instead of null.

* **Parser mechanics**

  * `comment_prefix`, `quote_char`, `eol_char`, `decimal_comma`, `encoding` (“utf8” vs “utf8-lossy”) ([Polars User Guide][2])

* **Chunk topology**

  * `low_memory` and `rechunk` (contiguous buffers after parse) ([Polars User Guide][2])

#### “CSV inference debugging” recipe

```python
lf = pl.scan_csv("bad.csv", infer_schema_length=0)  # force all columns to String
# then selectively cast/parse once you see what the data looks like
```

That “infer_schema_length=0 to debug” workflow is explicitly recommended in the docs for error-y CSVs.

---

### N4) Batched CSV ingestion: `pl.read_csv_batched(...)` (explicit chunk pull)

`read_csv_batched(...) -> BatchedCsvReader` does an initial planning pass (“gather statistics and determine file chunks”) and then only does parsing work when you call `next_batches`, returning a list of frames. ([Polars User Guide][3])
It also notes that if `fsspec` is installed it will be used to open remote files. ([Polars User Guide][3])

This is the “I want explicit ingestion batches” primitive (distinct from lazy streaming output APIs).

---

### N5) NDJSON scanning: `pl.scan_ndjson(...)` (batch sizing + schema discipline)

`scan_ndjson(source, *, schema, infer_schema_length, batch_size, n_rows, low_memory, rechunk, row_index_name, row_index_offset, ignore_errors) -> LazyFrame` ([Polars User Guide][4])

High leverage knobs:

* `schema`: can be dict, list of names, or list of (name,type) pairs; if you supply names that don’t match underlying data, they overwrite (so treat this as a contract tool, not a hint). ([Polars User Guide][4])
* `batch_size`: number of rows read per batch (parsing granularity). ([Polars User Guide][4])
* `ignore_errors`: return Null on parsing failures due to schema mismatches. ([Polars User Guide][4])

---

### N6) Parquet writing: `DataFrame.write_parquet(...)` vs `LazyFrame.sink_parquet(...)`

#### `DataFrame.write_parquet` (eager, full materialization already happened)

Key tuning knobs:

* `compression` + `compression_level` (gzip/brotli/zstd levels documented)
* `statistics` (write parquet stats)
* `row_group_size` (defaults to 512² rows)
* `data_page_size` (defaults to 1024² bytes)
* `use_pyarrow` (“C++ supports more features” vs Rust implementation) ([Polars User Guide][5])

#### `LazyFrame.sink_parquet` (execute + write; out-of-core capable)

Signature includes:

* `row_group_size=None` (defaults to using DataFrame chunks; smaller chunks may reduce memory pressure)
* `data_pagesize_limit` (page size cap; default 1 MiB)
* `maintain_order` (can be disabled for speed)
* plus the familiar optimization toggles (predicate pushdown, projection pushdown, etc)
  And Polars explicitly warns streaming mode is unstable.

---

## O) Debug/inspection utilities — deep dive (Polars 1.36+)

### Mental model: you debug Polars at 4 layers

1. **Plan graph** (logical/physical)
2. **Runtime tap points** (print data at a node or expression)
3. **Memory + chunk topology** (rechunking, estimated size)
4. **Build/runtime metadata** (versions, features, thread pool sizing)

(You already have plan introspection in section A; below is the *non-plan* debug toolkit.)

---

### O1) Tap points: inspect intermediate values without breaking the pipeline

#### `LazyFrame.inspect(fmt="{}")`

“Inspect a node in the computation graph”: prints the value the node evaluates to and passes it onward. ([Polars User Guide][6])

#### `Expr.inspect(fmt="{}")`

Same concept at expression granularity: prints the value the expression evaluates to and passes it on.

Practical pattern:

```python
df = (
  lf.with_columns(
        (pl.col("x").cum_sum().inspect("cum: {}")).alias("x_cum")
  )
  .collect()
)
```

This lets you localize “where does it go wrong?” without rewriting the query into multiple `.collect()` stages.

---

### O2) Display control plane: `.show()` + `pl.Config` formatting knobs

#### `DataFrame.show(...)` / `LazyFrame.show(...)`

Polars 1.36 added explicit `show` methods (per release notes). ([GitClear][7])
The show APIs are basically a structured wrapper over config settings (float format, string truncation, list cell display, table formatting, etc.). ([Polars User Guide][8])

#### `pl.Config` (formatting + verbose logging + persistence)

* Config contains table formatting controls like `set_tbl_cols`, `set_tbl_rows`, `set_tbl_formatting`, etc. ([Polars User Guide][8])
* `Config.set_verbose(True)` enables additional verbose/debug logging and can be used as a context manager (`with pl.Config(verbose=True): ...`). ([Polars User Guide][9])
* You can persist/restore config via `Config.save/load` (useful in notebooks/shared tooling). ([Polars User Guide][10])

---

### O3) “Wide-frame sanity view”: `DataFrame.glimpse(...)`

`glimpse` prints one line per column (name, dtype, first values) and can return the preview as a string. This is optimized for wide schemas.

---

### O4) Memory + chunk topology observability

#### `DataFrame.estimated_size(unit="b"|"kb"|...)`

Returns an estimate of total heap allocated size, including nested arrays; docs explicitly warn that shared buffers and structs make sizes non-additive / upper bounds. ([Polars User Guide][11])

#### `DataFrame.rechunk()`

Rechunks columns into contiguous allocations for “optimal and predictable performance.” ([Polars User Guide][12])

Operational pattern:

* Use `estimated_size()` to validate memory headroom before calling `collect()` on a potentially large plan.
* Use `rechunk()` right before downstream operations that are sensitive to chunk fragmentation (e.g., heavy joins/sorts, or IO writes that use chunk boundaries). ([Polars User Guide][5])

---

### O5) Expression tree introspection (`Expr.meta.*`)

Useful when you’re building higher-level libraries on top of Polars and need to analyze expressions:

* `root_names()` (what columns does this expression depend on?)
* `output_name(raise_if_undetermined=...)` (what column name would this produce?)
* `tree_format(return_as_string=True)` (pretty-printed expression tree)
* `is_column_selection(allow_aliasing=...)` (is this “pure selection” vs computation?)

This is the “AST tooling” plane for Polars expressions.

---

### O6) Runtime metadata: build + versions + thread pool sizing

* `pl.show_versions()` prints Polars version and optional dependency versions (good for bug reports).
* `pl.build_info()` returns build details including compiler, features, git info, target, etc.
* `pl.thread_pool_size()` returns Polars’ threadpool size and documents that `POLARS_MAX_THREADS` must be set *before process start* to override it.

---

## P) Testing utilities (`polars.testing.*`) — deep dive (Polars 1.36+)

### Mental model: two layers

1. **Deterministic assertions** (assert_*_equal / not_equal)
2. **Property-based generators** (`polars.testing.parametric.*` over Hypothesis)

---

### P1) Frame assertions: `polars.testing.assert_frame_equal`

Key parameters:

* `check_row_order`: require row order; if `False`, Polars will need to sort and it will fail on unsortable columns (explicit warning). ([Polars User Guide][13])
* `check_column_order`: require same column order. ([Polars User Guide][13])
* `check_dtypes`: require identical dtypes. ([Polars User Guide][13])
* Float comparison controls:

  * `check_exact` (if False, use tolerances)
  * `rtol` / `atol` apply only to Float columns
* `categorical_as_str`: cast categoricals to string before comparing (helps when string caches differ).

Practical “stable regardless of ordering” pattern:

```python
from polars.testing import assert_frame_equal

assert_frame_equal(df_left, df_right, check_row_order=False, check_column_order=False)
```

…but only do this if you know the frames are sortable and you genuinely want order-insensitive equality. ([Polars User Guide][13])

---

### P2) Series assertions: `polars.testing.assert_series_equal`

Key parameters:

* `check_dtypes`, `check_names`
* `check_exact`, `rtol`, `atol` (float-only tolerances)
* `categorical_as_str` (string-cache mismatch helper) ([Polars User Guide][14])

---

### P3) Negative assertions

Polars also provides:

* `assert_frame_not_equal`
* `assert_series_not_equal` ([Polars User Guide][15])

Use these when you want “should differ” tests without manually negating exception paths.

---

### P4) Parametric/property-based testing: `polars.testing.parametric.*` (Hypothesis)

Polars exposes Hypothesis strategy helpers so you can generate realistic frames/series/dtypes for property tests. The testing docs explicitly point you to Hypothesis and provide a “Parametric testing” section. ([Polars User Guide][15])

#### `dataframes(...)` strategy

Generates DataFrames or LazyFrames with configurable characteristics; docs note you can use it as a decorator strategy and that `.example()` is useful during development. ([Polars User Guide][16])

#### `series(...)` strategy

Generates Series with dtype/size/nullability controls; docs include `allowed_dtypes`, `excluded_dtypes`, `unique`, and note `.example()` usage. ([Polars User Guide][17])

#### `dtypes(...)` strategy

Generates Polars dtypes, including nested dtypes controlled by `nesting_level`; supports allow/exclude sets and timezone generation. ([Polars User Guide][18])

#### Column specs for dataframe strategies

`column(name, dtype, strategy=..., allow_null=..., unique=...)` lets you define exact column requirements for `dataframes(...)`. ([Polars User Guide][19])

#### Profile control: `set_profile("fast"|"balanced"|"expensive"|int)`

Sets `POLARS_HYPOTHESIS_PROFILE` to tune Hypothesis runtime cost, with a documented “balanced” recommendation. ([Polars User Guide][20])

Minimal pattern:

```python
import polars as pl
from hypothesis import given
from polars.testing.parametric import dataframes, set_profile

set_profile("balanced")

@given(df=dataframes(min_size=1, max_size=50))
def test_roundtrip(df: pl.DataFrame) -> None:
    assert df.height >= 1
```

The “dataframes strategy as unit test decorator” pattern is explicitly shown in the docs. ([Polars User Guide][16])

[1]: https://docs.pola.rs/py-polars/html/reference/api/polars.scan_parquet.html "polars.scan_parquet — Polars  documentation"
[2]: https://docs.pola.rs/py-polars/html/reference/api/polars.scan_csv.html "polars.scan_csv — Polars  documentation"
[3]: https://docs.pola.rs/py-polars/html/reference/api/polars.read_csv_batched.html "polars.read_csv_batched — Polars  documentation"
[4]: https://docs.pola.rs/py-polars/html/reference/api/polars.scan_ndjson.html "polars.scan_ndjson — Polars  documentation"
[5]: https://docs.pola.rs/py-polars/html/reference/api/polars.DataFrame.write_parquet.html "polars.DataFrame.write_parquet — Polars  documentation"
[6]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.inspect.html "polars.LazyFrame.inspect — Polars  documentation"
[7]: https://www.gitclear.com/open_repos/pola-rs/polars/release/py-1.36.0?utm_source=chatgpt.com "Polars py-1.36.0 Release"
[8]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.show.html "polars.LazyFrame.show — Polars  documentation"
[9]: https://docs.pola.rs/py-polars/html/reference/api/polars.Config.set_verbose.html?utm_source=chatgpt.com "polars.Config.set_verbose — Polars documentation"
[10]: https://docs.pola.rs/py-polars/html/reference/api/polars.Config.load.html?utm_source=chatgpt.com "polars.Config.load — Polars documentation"
[11]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.estimated_size.html?utm_source=chatgpt.com "polars.DataFrame.estimated_size — Polars documentation"
[12]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.rechunk.html?utm_source=chatgpt.com "polars.DataFrame.rechunk — Polars documentation"
[13]: https://docs.pola.rs/py-polars/html/reference/api/polars.testing.assert_frame_equal.html "polars.testing.assert_frame_equal — Polars  documentation"
[14]: https://docs.pola.rs/py-polars/html/reference/api/polars.testing.assert_series_equal.html "polars.testing.assert_series_equal — Polars  documentation"
[15]: https://docs.pola.rs/py-polars/html/reference/testing.html "Testing — Polars  documentation"
[16]: https://docs.pola.rs/py-polars/html/reference/api/polars.testing.parametric.dataframes.html "polars.testing.parametric.dataframes — Polars  documentation"
[17]: https://docs.pola.rs/api/python/dev/reference/api/polars.testing.parametric.series.html "polars.testing.parametric.series — Polars  documentation"
[18]: https://docs.pola.rs/py-polars/html/reference/api/polars.testing.parametric.dtypes.html "polars.testing.parametric.dtypes — Polars  documentation"
[19]: https://docs.pola.rs/py-polars/html/reference/api/polars.testing.parametric.column.html "polars.testing.parametric.column — Polars  documentation"
[20]: https://docs.pola.rs/py-polars/html/reference/api/polars.testing.parametric.set_profile.html "polars.testing.parametric.set_profile — Polars  documentation"


## Q) Polars SQL advanced surface beyond the basics — deep dive (Polars 1.36+)

### Mental model: SQL is a **frontend syntax** that compiles to Polars **expressions** and runs on the **same Polars engine**

Polars does **not** ship a separate SQL engine: it **translates SQL into expressions** and executes them with Polars’ query engine (so you keep lazy optimizations). ([Polars User Guide][1])
Accordingly, SQL queries are **always executed in lazy mode**; “eager” switches only affect whether the returned object is collected to a `DataFrame`. ([Polars User Guide][1])

---

### Q1) Entry points (the four “SQL planes”)

#### Q1.1 Global SQL: `pl.sql(query, *, eager=False) -> LazyFrame | DataFrame`

* Executes SQL against **frames in the global namespace**; returns a `LazyFrame` by default, or a `DataFrame` if `eager=True`. ([Polars User Guide][2])
* Can operate on **Polars** (DataFrame/LazyFrame/Series) **and** **pandas** (DataFrame/Series) **and** **PyArrow** (Table/RecordBatch). ([Polars User Guide][2])
* This is the preferred “quick SQL” surface; it internally corresponds to the “auto-register globals then run” workflow. ([Polars User Guide][3])

Minimal pattern:

```python
import polars as pl

lf1 = pl.LazyFrame({"a": [1, 2, 3], "b": [6, 7, 8]})
lf2 = pl.LazyFrame({"a": [2, 3, 4], "d": [10, 11, 12]})

out = pl.sql("""
  SELECT lf1.a, lf1.b, lf2.d
  FROM lf1 INNER JOIN lf2 USING (a)
  WHERE lf1.a > 1
""").collect()
```

`pl.sql` explicitly supports joining multiple frames and then continuing with native operations (see docs example that mixes SQL + `.filter(...)`). ([Polars User Guide][2])

---

#### Q1.2 Frame SQL: `DataFrame.sql(...) -> DataFrame` and `LazyFrame.sql(...) -> LazyFrame`

* `DataFrame.sql(query, table_name="self")` runs the SQL **against that frame** (registered as `self` by default), executes lazily, then collects and returns a `DataFrame`. It’s explicitly marked “unstable”. ([Polars User Guide][4])
* `LazyFrame.sql(query, table_name="self")` returns a **new LazyFrame** representing the SQL result; also marked “unstable”. ([Polars User Guide][5])

This surface is ideal for “SQL as just another transform” inside a pipeline, because you can keep the object local (no global registration concerns) and keep it composable.

---

#### Q1.3 Context SQL (stateful): `SQLContext.execute(...)` / registrations / context manager

This is the “production surface” when you care about:

* explicit table naming,
* lifecycle/scoping of registered tables,
* mixing lazy sources (scan_csv/scan_parquet/etc) with in-memory frames,
* controlling eager vs lazy return types via init-time defaults.

---

#### Q1.4 Expression SQL: `pl.sql_expr(...) -> Expr | list[Expr]`

`pl.sql_expr(sql)` parses one or more SQL *expressions* into Polars `Expr` objects, so you can embed SQL fragments inside native `with_columns/select` without switching entire pipelines to SQL. ([Polars User Guide][6])

Minimal pattern:

```python
import polars as pl

df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
df2 = df.with_columns(
    *pl.sql_expr(["POWER(a,a) AS a_a", "CAST(a AS TEXT) AS a_txt"]),
)
```

([Polars User Guide][6])

---

## Q2) SQLContext: the “control plane” (registration, scoping, eager policy)

### Q2.1 Construction + defaults

`SQLContext.__init__` takes:

* `frames: Mapping[name -> frame]` (Polars frames + pandas DataFrames/Series + pyarrow Table/RecordBatch)
* `register_globals: bool | int` (auto-register Polars objects found in globals; if int, register most recent N)
* `eager: bool` (default return type for `execute`: DataFrame vs LazyFrame)
* `**named_frames` as kwargs
  It also notes that `eager_execution` was renamed to `eager`. ([Polars User Guide][7])

### Q2.2 Context manager semantics (table lifetime control)

SQLContext is a context manager:

* `__enter__` tracks currently registered tables (supports nested scopes)
* `__exit__` unregisters any tables created within the scope on exit ([Polars User Guide][7])

This is the cleanest way to guarantee “no leaked registration state” in long-running processes.

### Q2.3 Registering data

You have three registration “tiers”:

1. **Explicit registration**

* `ctx.register(name, frame)` ([Polars User Guide][8])
* `ctx.register_many({...}, **named_frames)` ([Polars User Guide][9])

2. **Register globals (bulk)**

* `ctx.register_globals(n=None, *, all_compatible=True)` maps variable names → table names; `all_compatible` controls whether pandas/pyarrow objects are included (otherwise only Polars). ([Polars User Guide][10])

3. **Auto-register referenced objects for a single query**

* `SQLContext.execute_global(query, *, eager=False)` registers all compatible objects in the local stack that are referenced in the query (including pandas + pyarrow), then executes. Polars suggests using `pl.sql`, which uses this internally. ([Polars User Guide][3])

### Q2.4 Inventory: `ctx.tables()` vs `SHOW TABLES`

`SQLContext.tables()` returns a Python `list[str]` and is equivalent to `SHOW TABLES` in SQL. ([Polars User Guide][11])

### Q2.5 Execution: always lazy, eager affects return type only

`SQLContext.execute(query, *, eager: bool|None=None) -> LazyFrame|DataFrame`:

* parses the SQL query and runs it against registered tables
* if `eager=True`, returns a DataFrame; if unset, uses init-time `eager` default
* query execution is always in lazy mode; this parameter only changes the returned frame type ([Polars User Guide][12])

---

## Q3) Multi-source SQL is a first-class design (lazy scans + pandas + pyarrow)

The user guide explicitly shows registering:

* lazy scans from CSV and NDJSON,
* a pandas DataFrame,
  then joining them in SQL—while preserving lazy file read benefits (only necessary rows/cols loaded). ([Polars User Guide][1])

Also, Polars notes that non-Polars objects are implicitly converted for SQL usage, and for PyArrow / pandas-with-pyarrow-dtypes this conversion can be close to zero-copy when types map cleanly. ([Polars User Guide][7])
The user guide adds a concrete performance note: converting pandas backed by NumPy can be expensive; if it’s Arrow-backed it can be significantly cheaper. ([Polars User Guide][1])

---

## Q4) SQL compatibility + “how to know what’s supported”

Polars does not implement full SQL; it supports a common subset and aims (where possible) to follow PostgreSQL syntax/behavior. ([Polars User Guide][1])
The docs provide catalogs of supported clauses/functions (SQL Clauses, SQL Functions, Set Operations, Table Operations) under the SQL Interface reference. ([Polars User Guide][12])

---

# R) Explicit caching nodes: `LazyFrame.cache()` — deep dive (Polars 1.36+)

### Mental model: caching is **already an optimizer primitive**; `cache()` is an explicit “pin a reuse point” node at the physical-plan layer

Polars lazy optimization includes **Common Subplan Elimination (CSE)**, described as caching subtrees/file scans used by multiple subtrees in the plan. ([Polars User Guide][13])
That’s why Polars warns `LazyFrame.cache()` is usually not needed: “the optimizer likely can do a better job.” ([Polars User Guide][14])

---

## R1) What `LazyFrame.cache()` actually does

`LazyFrame.cache() -> LazyFrame`:

* “Cache the result once the execution of the physical plan hits this node.” ([Polars User Guide][14])
* “It is not recommended using this as the optimizer likely can do a better job.” ([Polars User Guide][14])

Interpretation (operational): `cache()` inserts a materialization/reuse barrier in the physical plan. If that cached result is referenced multiple times downstream, it can be reused without recomputation.

---

## R2) The “default” you should try first: optimizer-driven caching (CSE) via combined execution

### R2.1 Cross-frame CSE: `collect_all` / `explain_all`

Polars exposes an explicit multi-plan execution unit:

* `pl.collect_all([...])` runs multiple computation graphs in parallel and applies CSE so shared branches run once. ([Polars User Guide][15])
* `pl.explain_all([...])` explains the combined plan and explicitly states CSE is applied so diverging queries run only once. ([Polars User Guide][16])

This is usually the “right” way to get reuse when you have multiple downstream consumers (multiple outputs, multiple sinks) of a shared upstream pipeline.

### R2.2 QueryOptFlags: `comm_subplan_elim` literally means “cache duplicate plans”

`QueryOptFlags.comm_subplan_elim` is described as: “Elide duplicate plans and caches their outputs.” ([Polars User Guide][17])
So the canonical “Polars-native caching” path is: build identical subplans → run them as one combined optimization unit.

---

## R3) When `cache()` is a rational tool (despite the warning)

Use `cache()` when you have a specific reason to force a reuse point at a specific location, such as:

* You’re deliberately building a single logical plan with **multiple downstream branches** and you want to ensure a single materialization at a chosen boundary (e.g., after an expensive scan+filter+join stage).
* You’re debugging performance and want to test “what if I pin this intermediate?” without relying on optimizer heuristics.

Minimal pattern (single execution unit, two consumers):

```python
import polars as pl

base = (
    pl.scan_parquet("s3://bucket/events/*.parquet", cache=False)
      .filter(pl.col("dt") >= "2025-12-01")
      .cache()
)

q1 = base.group_by("user_id").agg(pl.len().alias("n"))
q2 = base.select(["user_id", "event_type"]).unique()

# Prefer combined execution so the engine can share work
out1, out2 = pl.collect_all([q1, q2])
```

Notes:

* `scan_parquet(..., cache=True|False)` is a *scan-level* cache control (“Cache the result after reading.”) and is distinct from `LazyFrame.cache()` which caches at an explicit plan node. ([Polars User Guide][18])
* Even here, CSE may already make this unnecessary; `cache()` is a deliberate override tool, not a default. ([Polars User Guide][14])

---

## R4) Tradeoffs / footguns

* **Memory pressure:** a cached intermediate must be stored for reuse; if the intermediate is large, you’ve moved from “streaming compute” toward “materialize-and-reuse”. (`cache()` is literally “cache the result”). ([Polars User Guide][14])
* **Prefer `collect_all` sharing first:** if your real goal is “don’t recompute shared upstream work across multiple outputs,” `collect_all`/`explain_all` is the intended mechanism and explicitly applies CSE. ([Polars User Guide][15])

---

If you want next: I can connect `cache()` to **streaming/batch outputs** and show the “correct” patterns for multi-sink pipelines (e.g., `sink_parquet(lazy=True)` + `collect_all`) where CSE is typically sufficient and `cache()` is rarely needed.

[1]: https://docs.pola.rs/user-guide/sql/intro/ "Introduction - Polars user guide"
[2]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.sql.html "polars.sql — Polars  documentation"
[3]: https://docs.pola.rs/api/python/stable/reference/sql/api/polars.SQLContext.execute_global.html "polars.SQLContext.execute_global — Polars  documentation"
[4]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.sql.html "polars.DataFrame.sql — Polars  documentation"
[5]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.sql.html "polars.LazyFrame.sql — Polars  documentation"
[6]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.sql_expr.html "polars.sql_expr — Polars  documentation"
[7]: https://docs.pola.rs/api/python/dev/reference/sql/python_api.html "Python API — Polars  documentation"
[8]: https://docs.pola.rs/api/python/dev/reference/sql/api/polars.SQLContext.register.html "polars.SQLContext.register — Polars  documentation"
[9]: https://docs.pola.rs/api/python/stable/reference/sql/api/polars.SQLContext.register_many.html "polars.SQLContext.register_many — Polars  documentation"
[10]: https://docs.pola.rs/api/python/dev/reference/sql/api/polars.SQLContext.register_globals.html "polars.SQLContext.register_globals — Polars  documentation"
[11]: https://docs.pola.rs/api/python/dev/reference/sql/api/polars.SQLContext.tables.html "polars.SQLContext.tables — Polars  documentation"
[12]: https://docs.pola.rs/api/python/dev/reference/sql/api/polars.SQLContext.execute.html "polars.SQLContext.execute — Polars  documentation"
[13]: https://docs.pola.rs/user-guide/lazy/optimizations/ "Optimizations - Polars user guide"
[14]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.cache.html "polars.LazyFrame.cache — Polars  documentation"
[15]: https://docs.pola.rs/py-polars/html/reference/api/polars.collect_all.html?utm_source=chatgpt.com "polars.collect_all — Polars documentation"
[16]: https://docs.pola.rs/api/python/dev/reference/api/polars.explain_all.html?utm_source=chatgpt.com "polars.explain_all — Polars documentation"
[17]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.QueryOptFlags.html?utm_source=chatgpt.com "polars.QueryOptFlags — Polars documentation"
[18]: https://docs.pola.rs/py-polars/html/reference/api/polars.scan_parquet.html "polars.scan_parquet — Polars  documentation"
