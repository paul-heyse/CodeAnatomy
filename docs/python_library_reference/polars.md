
Below is a **Polars Python 1.36+–aligned rewrite** of the earlier overview (same scope, but updated APIs/semantics + corrected snippets). The biggest changes vs older material are: **`collect(..., engine=..., optimizations=...)`**, **GPU engine mode**, **background query handles (`InProcessQuery`)**, and **updated join/null/index/UDF surfaces**. ([Polars User Guide][1])

---

## 1) Execution model: eager vs lazy, plus engine selection (in-memory / streaming / GPU)

### Eager vs lazy

* **Eager** (`DataFrame`) executes immediately.
* **Lazy** (`LazyFrame`) builds a logical plan; execution occurs at `collect` (optimizer runs first).

### `LazyFrame.collect` (1.36+ signature you should code against)

Key knobs:

* `engine`: `"auto"` (default), `"streaming"`, `"gpu"`, or a `GPUEngine(...)` config object.
* `optimizations`: `QueryOptFlags(...)` (preferred control surface).
* `background=True`: returns an `InProcessQuery` handle instead of a `DataFrame`. ([Polars User Guide][1])

```python
import polars as pl

lf = (
    pl.scan_parquet("events/*.parquet")
      .filter(pl.col("country") == "US")
      .select(["user_id", "ts", "value"])
)

# default engine selection (auto)
df = lf.collect()

# force streaming engine (batch/out-of-core style where supported)
df_stream = lf.collect(engine="streaming")

# try GPU engine (falls back if unsupported; GPU mode is unstable)
df_gpu = lf.collect(engine="gpu")

# pick a specific GPU device
df_gpu1 = lf.collect(engine=pl.GPUEngine(device=1))
```

Polars documents that `engine="gpu"` uses the GPU engine and may fall back transparently when unsupported; GPU mode is considered unstable. ([Polars User Guide][1])
Polars also notes `POLARS_ENGINE_AFFINITY` can influence engine selection. ([Polars User Guide][1])

---

## 2) Background execution + cancellation handles (InProcessQuery)

`collect(background=True)` returns an **`InProcessQuery`** handle that you can cancel or fetch later. ([Polars User Guide][1])

```python
q = lf.collect(background=True)   # returns InProcessQuery
q.cancel()                        # best-effort cancel
df = q.fetch_blocking()           # wait and retrieve the result
# or: df = q.fetch()              # non-blocking fetch if ready
```

Available methods: `cancel()`, `fetch()`, `fetch_blocking()`. ([Polars User Guide][2])

---

## 3) Async collection APIs (collect_async / collect_all / collect_all_async)

### `collect_async` (single LazyFrame)

Runs the collect in a threadpool and returns an awaitable (or a gevent wrapper). This is explicitly marked unstable. ([Polars User Guide][3])

```python
import asyncio, polars as pl

async def main():
    lf = pl.scan_parquet("events/*.parquet").group_by("country").agg(pl.len())
    df = await lf.collect_async(engine="auto")  # awaitable
    return df

df = asyncio.run(main())
```

Important constraint: **GPU engine does not support async or background**; enabling either turns GPU execution off. ([Polars User Guide][3])

### `collect_all` (multiple LazyFrames, coordinated)

Runs multiple plans together, applying **common subplan elimination** (diverging queries can run shared work once). ([Polars User Guide][4])

```python
import polars as pl

lfs = [
    pl.scan_parquet("events/*.parquet").filter(pl.col("country")=="US").select("user_id"),
    pl.scan_parquet("events/*.parquet").filter(pl.col("country")=="CA").select("user_id"),
]
dfs = pl.collect_all(lfs, engine="auto")
```

### `collect_all_async` (multiple LazyFrames, async threadpool)

Same idea as `collect_all`, but scheduled in a threadpool; unstable; supports `engine` + `optimizations`. ([Polars User Guide][5])

---

## 4) Optimizer control: QueryOptFlags is the modern switchboard

### The modern way: `optimizations=QueryOptFlags(...)`

Polars now centralizes optimization toggles in `QueryOptFlags` (marked unstable as a surface). ([Polars User Guide][6])

```python
import polars as pl

opts = pl.QueryOptFlags(
    predicate_pushdown=True,
    projection_pushdown=True,
    simplify_expression=True,
    slice_pushdown=True,
    collapse_joins=True,
)

df = lf.collect(engine="auto", optimizations=opts)
```

### Legacy boolean kwargs exist but are deprecated

The older `predicate_pushdown=...`, `projection_pushdown=...`, etc. parameters are **deprecated since 1.30.0** in favor of `optimizations=`. ([Polars User Guide][1])

---

## 5) Expressions API: Polars’ “vectorized IR” (the mental model)

Polars wants you to express transforms as **expressions** (`pl.Expr`) that compile into native execution:

```python
import polars as pl

df = pl.DataFrame({"price":[10,12,8], "qty":[2,1,5], "category":["A","A","B"]})

out = (
    df.with_columns(
        (pl.col("price") * pl.col("qty")).alias("amount"),
        pl.when(pl.col("qty") > 3).then(True).otherwise(False).alias("bulk"),
    )
    .filter(pl.col("category") == "A")
    .select(["category", "amount", "bulk"])
)
```

Rule of thumb: **stay inside expressions** for performance + optimizer friendliness; treat Python row loops as last resort.

---

## 6) Aggregation + windows: group_by, time windows, and `.over(...)`

### GroupBy aggregation

```python
import polars as pl

df = pl.DataFrame({"team":["A","A","B"], "points":[10,15,7]})
stats = df.group_by("team").agg(
    pl.sum("points").alias("total_points"),
    pl.mean("points").alias("avg_points"),
    pl.len().alias("n"),
)
```

### Time windows (dynamic / rolling)

```python
# df: ["id", "ts", "value"], ts is datetime
out = (
    df.sort(["id", "ts"])
      .group_by_rolling(index_column="ts", period="30m", by="id")
      .agg(pl.mean("value").alias("mean_30m"))
)
```

### Window functions: `.over(...)` (SQL-style analytic)

```python
out = df.with_columns(
    pl.sum("points").over("team").alias("team_total"),
    pl.col("points").cum_sum().sort_by("ts").over("team").alias("team_running"),
)
```

---

## 7) Joins: equi joins, null semantics, ordering, validation, and non-equi joins

### Equi-joins (`DataFrame.join`)

Supported strategies include: `inner`, `left`, `right`, `full`, `semi`, `anti`, `cross`. ([Polars User Guide][7])

```python
out = left.join(right, on="key", how="left")
out = left.join(right, left_on="k1", right_on="k2", how="full")
```

Key advanced knobs:

* `nulls_equal` (formerly `join_nulls`; renamed in 1.24). ([Polars User Guide][7])
* `validate` (`"m:m"`, `"1:1"`, `"1:m"`, `"m:1"`) for key cardinality checks. ([Polars User Guide][7])
* `maintain_order` to request deterministic row-order preservation; docs warn not to rely on incidental ordering without setting it. ([Polars User Guide][7])
* Join validation is noted as not supported by the streaming engine. ([Polars User Guide][7])

```python
out = left.join(
    right,
    on="key",
    how="inner",
    nulls_equal=True,
    validate="m:1",
    maintain_order="left",
)
```

### Non-equi joins (`join_where`) — inner join, experimental, order not preserved

`join_where` performs an **inner** join over one or more (in)equality predicates; rows can repeat; input row order is **not preserved**; the feature is **experimental**. ([Polars User Guide][8])

```python
out = east.join_where(
    west,
    pl.col("dur") < pl.col("time"),
    pl.col("rev") < pl.col("cost"),
)
```

(If you need OR conditions, build a single predicate with `|`.) ([Polars User Guide][8])

---

## 8) Row “index” reality in Polars (and what changed)

Polars has **no special index** like pandas; any “index” is just another column.

### `with_row_count` is deprecated → use `with_row_index`

`DataFrame.with_row_count()` is deprecated (since 0.20.4); use `with_row_index()`; default name changed (`row_nr` → `index`). ([Polars User Guide][9])

```python
df = df.with_row_index("idx", offset=1)
```

### LazyFrame warning: row index can hurt optimizations

`LazyFrame.with_row_index()` can negatively impact performance and may block predicate pushdown. ([Polars User Guide][10])

Also: you can generate index columns via expressions (`int_range`, `len`) per docs. ([Polars User Guide][10])

### Expression-level row index

`pl.row_index()` exists (added in 1.32.0) to generate an integer sequence in expression contexts. ([Polars User Guide][11])

---

## 9) Types that matter in real pipelines: structs, lists, categoricals/enums, temporal

### Structs

Use `pl.struct([...])`, access fields with `.struct.field(...)`, flatten with `unnest`.

```python
coords = df.with_columns(pl.struct(["lat", "lon"]).alias("coords"))
flat = coords.unnest("coords")
```

### Lists (nested arrays)

Explode lists, or compute list-wise with `list.eval(pl.element()...)`.

```python
out = (
    df.group_by("id")
      .agg(pl.col("val").list().alias("vals"))
      .with_columns(pl.col("vals").list.eval(pl.element().rank()).alias("ranks"))
)
```

### Temporal

Use `.dt` accessors for truncate/extract/time arithmetic.

```python
df = df.with_columns(
    pl.col("ts").dt.truncate("1h").alias("ts_hour"),
    pl.col("ts").dt.weekday().alias("dow"),
)
```

*(Categorical vs Enum vs StringCache is still a real topic; if you’re doing lots of joins/group_bys on repeated strings, it’s worth standardizing your approach to dictionary-encoding.)*

---

## 10) Nulls, casting, and schema control (practical patterns)

```python
df = df.with_columns(
    pl.col("x").fill_null(0).alias("x0"),
    pl.col("s").cast(pl.Utf8),  # explicit cast
)

# strict cast (raise on failure) where supported in your dtype conversion path
df = df.with_columns(pl.col("maybe_int").cast(pl.Int64, strict=True))
```

Key idea: in lazy mode, type coercion + other optimizer passes can be controlled via `optimizations=QueryOptFlags(...)`.

---

## 11) Python UDF escape hatches: map_elements / map_batches / map_rows

### Avoid row-wise UDFs (`map_rows`) unless forced

Docs explicitly warn `DataFrame.map_rows` is **much slower** than expressions and forces the DataFrame to be materialized; UDFs block parallelization and logical optimizations. ([Polars User Guide][12])

```python
df2 = df.map_rows(lambda row: row[0] * 2 + row[1], return_dtype=pl.Int64)
```

Prefer expression equivalents:

```python
df2 = df.select(pl.col("foo") * 2 + pl.col("bar"))
```

---

## 12) I/O: read vs scan, and sink patterns

* Prefer `scan_*` (lazy) for large datasets: enables pushdowns + engine selection at collect time.
* Prefer `sink_*` when you want to **write results without materializing a giant DataFrame** (Parquet/IPC/NDJSON sinks are common patterns).

```python
(
  pl.scan_parquet("data/*.parquet")
    .filter(pl.col("year") == 2025)
    .select(["user_id", "revenue"])
    .sink_parquet("out.parquet")
)
```

---

## 13) SQL interface (still useful for teams that think in SQL)

* `DataFrame.sql(...)` for quick SQL over a frame.
* `pl.SQLContext` to register multiple frames/lazyframes and run SQL joins/queries.

---

If you want, I can also produce a **single “1.36+ operational cheat sheet”** (engine-selection rules, when GPU silently falls back, how to structure joins to keep `collapse_joins` effective, and what patterns tend to disable predicate pushdown—especially around row indexes and UDFs).

[1]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.collect.html "polars.LazyFrame.collect — Polars  documentation"
[2]: https://docs.pola.rs/api/python/stable/reference/lazyframe/in_process.html "InProcessQuery — Polars  documentation"
[3]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.collect_async.html "polars.LazyFrame.collect_async — Polars  documentation"
[4]: https://docs.pola.rs/api/python/stable/reference/api/polars.collect_all.html "polars.collect_all — Polars  documentation"
[5]: https://docs.pola.rs/api/python/stable/reference/api/polars.collect_all_async.html "polars.collect_all_async — Polars  documentation"
[6]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.QueryOptFlags.html "polars.QueryOptFlags — Polars  documentation"
[7]: https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.join.html "polars.DataFrame.join — Polars  documentation"
[8]: https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.join_where.html "polars.DataFrame.join_where — Polars  documentation"
[9]: https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.with_row_count.html "polars.DataFrame.with_row_count — Polars  documentation"
[10]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.with_row_index.html "polars.LazyFrame.with_row_index — Polars  documentation"
[11]: https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.row_index.html?utm_source=chatgpt.com "polars.row_index — Polars documentation"
[12]: https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.map_rows.html "polars.DataFrame.map_rows — Polars  documentation"
