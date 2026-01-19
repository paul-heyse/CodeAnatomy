
## 6) PyArrow integration deep dive (zero/low-copy interchange + hybrid execution)

Polars is **Arrow-native at the memory layout level**, but it is **not built on PyArrow**—it has its own compute + buffer implementations. That’s why “Arrow in / Arrow out” is the cleanest interoperability boundary: Arrow structures are the lingua franca that DataFusion, Ibis backends, and delta-rs can all speak efficiently. ([Polars User Guide][1])

---

# 6.1 Arrow producer/consumer (Tables / RecordBatches)

### 6.1.1 Export from Polars → Arrow (Table / Array / Stream)

#### `DataFrame.to_arrow()` (Table): “mostly zero-copy”, but **Categorical copies**

`DataFrame.to_arrow()` returns a `pyarrow.Table` and is “mostly zero copy”; Polars calls out that **CategoricalType copies**. ([Polars User Guide][2])

```python
import polars as pl

df = pl.DataFrame({"x": [1, 2, 3], "s": ["a", "b", "c"]})
t  = df.to_arrow()  # pyarrow.Table, mostly zero-copy
```

**Gotcha: categorical/dictionary boundary**
If you want this boundary to remain “pointer-stable”, avoid Polars `Categorical` columns right before export (or accept the copy). ([Polars User Guide][2])

---

#### “Ensure” zero-copy output for strings: `compat_level=CompatLevel.newest()`

Polars’ Arrow producer/consumer guide shows that you can request a newer Arrow compatibility level to produce `string_view` (which is often used to keep conversion zero-copy for strings). ([Polars User Guide][3])

```python
import polars as pl

df = pl.DataFrame({"foo": [1, 2, 3], "bar": ["ham", "spam", "jam"]})
t0 = df.to_arrow()
t1 = df.to_arrow(compat_level=pl.CompatLevel.newest())  # aims to ensure zero-copy
```

---

#### Arrow PyCapsule path: `pyarrow.table(df)` (requires **pyarrow >= 15**)

As of Polars v1.3+, Polars implements the Arrow PyCapsule interface; the guide shows exporting a Polars DataFrame via `pa.table(df)` and notes it **requires pyarrow v15+**. ([Polars User Guide][3])

```python
import polars as pl
import pyarrow as pa

df = pl.DataFrame({"foo": [1, 2, 3], "bar": ["ham", "spam", "jam"]})
t = pa.table(df)  # Arrow PyCapsule pathway (pyarrow>=15)
```

**Why you care:** this is the “universal” interchange route: any Arrow-native consumer that understands the C data interface can ingest the result without bespoke adapters. ([Polars User Guide][3])

---

#### `Series.to_arrow()` (Array): zero-copy iff single chunk

`Series.to_arrow()` returns the underlying Arrow array and is **zero-copy only if the Series has a single chunk**. ([Polars User Guide][4])

```python
import polars as pl

s = pl.Series("a", [1, 2, 3])
arr = s.to_arrow()  # zero-copy if s is single-chunk
```

If you go through PyArrow constructors, Polars warns that `pa.array(series)` may require copying when the Series has multiple chunks. ([Polars User Guide][3])

---

### 6.1.2 Import Arrow → Polars (`pl.from_arrow`, `pl.DataFrame(arrow_table)`)

#### `pl.from_arrow(...)`: “zero-copy for the most part”, but may **cast unsupported types**; default `rechunk=True`

`pl.from_arrow` will be “zero copy for the most part”; **types not supported by Polars may be cast** to the closest supported type. It also defaults to `rechunk=True` (“Make sure that all data is in contiguous memory”), which can force copying. ([Polars User Guide][5])

```python
import polars as pl
import pyarrow as pa

t = pa.table({"a": [1, 2, 3]})

# best-effort zero-copy ingest, but note: rechunk=True can copy
df0 = pl.from_arrow(t)

# if you prioritize "don’t copy", consider disabling rechunk
df1 = pl.from_arrow(t, rechunk=False)
```

**Schema control**
If Arrow carries dtypes that Polars would otherwise coerce, declare `schema` / `schema_overrides` to make the ingest deterministic. ([Polars User Guide][5])

---

### 6.1.3 “Arrow in / Arrow out” as the boundary for DataFusion / Ibis / Delta

#### DataFusion ⇄ Arrow ⇄ Polars

DataFusion’s Python docs: it implements the Arrow PyCapsule interface for import/export; `SessionContext.from_arrow()` accepts any object that implements `__arrow_c_stream__` or `__arrow_c_array__` (for the latter: must be a struct array). DataFusion DataFrames also implement `__arrow_c_stream__` for incremental batch streaming on export. ([Apache DataFusion][6])

Minimal bridge:

```python
import polars as pl
from datafusion import SessionContext

pl_df = pl.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
arrow_tbl = pl_df.to_arrow()

ctx = SessionContext()
df = ctx.from_arrow(arrow_tbl)          # Arrow → DataFusion  :contentReference[oaicite:11]{index=11}

# execute in DataFusion, then return Arrow
arrow_out = df.to_arrow_table()         # DataFusion DataFrame → Arrow Table :contentReference[oaicite:12]{index=12}
pl_out = pl.from_arrow(arrow_out)       # Arrow → Polars :contentReference[oaicite:13]{index=13}
```

Also: since DataFusion 42.0.0, any DataFrame library supporting Arrow FFI PyCapsule can be imported via `from_arrow()`. ([Apache DataFusion][7])

---

#### Ibis ⇄ Arrow ⇄ Polars

Ibis table expressions can execute and return:

* `to_polars()` → Polars DataFrame
* `to_pyarrow_batches()` → Arrow `RecordBatchReader` with configurable `chunk_size` ([Ibis][8])

That gives you a streaming-friendly “Arrow stream boundary” you can feed into `pl.from_arrow(...)` (stream exportable) or materialize as a table depending on your memory goals. ([Polars User Guide][5])

---

#### Delta Lake (delta-rs / deltalake) ⇄ Arrow ⇄ Polars

delta-rs explicitly supports exposing Delta tables as Arrow **datasets** and **tables** for interoperability. ([delta-io.github.io][9])
`write_deltalake` accepts a **PyArrow Table** or an iterator of **PyArrow RecordBatches**. ([delta-io.github.io][10])

So the canonical integration loop is:

* Read Delta as Arrow dataset (lazy) → Polars scans via `scan_pyarrow_dataset` (next section)
* Produce Arrow Table/RecordBatches from Polars → write via `write_deltalake`

---

## 6.2 Scanning `pyarrow.dataset` (`scan_pyarrow_dataset`) + pushdown + null traps

### 6.2.1 What `scan_pyarrow_dataset` is (and why you use it)

`pl.scan_pyarrow_dataset(dataset)` returns a Polars `LazyFrame` over a **PyArrow Dataset**, useful for cloud or partitioned datasets (e.g., hive partitions). ([Polars User Guide][11])

Cloud-storage example (Polars docs):

```python
import polars as pl
import pyarrow.dataset as ds

dset = ds.dataset("s3://my-partitioned-folder/", format="parquet")
out = (
    pl.scan_pyarrow_dataset(dset)
      .filter(pl.col("foo") == "a")
      .select(["foo", "bar"])
      .collect()
)
```

([Polars User Guide][12])

---

### 6.2.2 API knobs + what they actually mean

#### `allow_pyarrow_filter` (default True): push predicates to Arrow **but null semantics may differ**

Polars warns: enabling pushdown “can lead to different results if comparisons are done with null values as pyarrow handles this different than polars does.” ([Polars User Guide][11])

This is the correctness trap: Polars `filter` drops rows where predicate is not `True` (i.e., `False` or `null`), while Arrow’s filter evaluation semantics for null comparisons can differ depending on the expression and kernel path. Polars is explicitly telling you: “don’t assume equivalence.” ([Polars User Guide][11])

**Recommendation (contract-friendly)**

* If your predicate touches nullable columns and correctness is paramount: set `allow_pyarrow_filter=False`, then rely on Polars’ own filter semantics post-scan.
* If you want pushdown: write predicates that are null-explicit (`is_null`, `is_not_null`, or `fill_null` then compare) so the semantics are less ambiguous across engines.

#### `batch_size`: control record batch size from Arrow scan

`batch_size` controls the maximum row count for scanned Arrow record batches. ([Polars User Guide][11])
Tune this if you’re seeing overly large batches spike memory downstream, or too-small batches increase overhead.

---

### 6.2.3 Security + capability warnings (read these as “production constraints”)

Polars documents several hard constraints for `scan_pyarrow_dataset`: ([Polars User Guide][11])

* **Unstable API**: may change without being treated as breaking.
* **Untrusted input**: don’t use with untrusted user inputs; predicates are evaluated with Python `eval` (sanitation exists but is still an attack vector).
* **Pushdown coverage**: only predicates that PyArrow allows can be pushed down (not the full Polars expression API).
* **Prefer `scan_parquet()`** if it works for your source.

---

### 6.2.4 Partition columns may “disappear” unless you configure dataset partitioning

Polars notes: when using partitioning, you must set the appropriate `partitioning` option on `pyarrow.dataset.dataset` **before** passing to Polars, or partitioned-on columns may not be passed through. ([Polars User Guide][11])

If you are using hive-style paths, Arrow has a built-in `HivePartitioning` scheme. ([Apache Arrow][13])

```python
import pyarrow.dataset as ds

dset = ds.dataset(
    "s3://bucket/path/",
    format="parquet",
    partitioning="hive",  # or ds.partitioning(...) / ds.HivePartitioning(...)
)
lf = pl.scan_pyarrow_dataset(dset)
```

---

## 6.3 Hybrid compute strategy (Polars + `pyarrow.compute`) + dtype/categorical gotchas

### 6.3.1 When to offload to `pyarrow.compute` vs stay in Polars

**Default:** keep transforms in Polars expressions/lazy plan so you retain:

* optimizer rewrites (pushdowns, CSE, etc.)
* streaming execution
* parallel execution model

**Offload to `pyarrow.compute` when:**

1. You need an Arrow kernel that Polars doesn’t expose (or you need Arrow’s option surface).
2. You need Arrow-specific semantics (e.g., explicit overflow-checked variants).
3. You’re integrating with a toolchain that already operates on Arrow Arrays/Tables.

PyArrow compute functions are provided by `pyarrow.compute` and operate on arrays/chunked arrays/scalars. ([Apache Arrow][14])
Many functions have `_checked` variants that raise `ArrowInvalid` on overflow. ([Apache Arrow][15])

---

### 6.3.2 “Arrow boundary compute” pattern (simple, but breaks lazy optimization)

```python
import polars as pl
import pyarrow.compute as pc

# (1) Do as much as possible in Polars Lazy (pushdowns, joins, groupbys)
lf = (
    pl.scan_parquet("data/*.parquet")
      .select(["k", "v"])
      .filter(pl.col("v").is_not_null())
)

# (2) Materialize (streaming collect if you want)
df = lf.collect(engine="streaming")  # streaming batches internally
t  = df.to_arrow()                  # mostly zero-copy, categorical copies :contentReference[oaicite:29]{index=29}

# (3) Apply Arrow compute
v2 = pc.cumulative_sum_checked(t["v"])  # overflow-checked variant :contentReference[oaicite:30]{index=30}
t2 = t.append_column("v_cumsum_checked", v2)

# (4) Re-import to Polars
df2 = pl.from_arrow(t2, rechunk=False)  # avoid forced contiguous copy if desired :contentReference[oaicite:31]{index=31}
```

**What you lose:** downstream Polars can’t push this Arrow compute back into scan-level pushdown, and you’ve introduced a materialization boundary.

---

### 6.3.3 Batch/stream-oriented hybrid compute (keep “Arrow stream” as boundary)

If you have an Arrow **RecordBatchReader** (e.g., from Ibis’ `to_pyarrow_batches(chunk_size=...)`), you can process it batchwise with Arrow compute and reassemble. Ibis explicitly exposes `to_pyarrow_batches` and controls chunk sizing. ([Ibis][8])
This keeps memory bounded, but you’re now writing a “stream processing” loop in Python (not a pure Polars plan).

---

### 6.3.4 Dtype conversion + dictionary/categorical gotchas (what copies, what doesn’t)

#### Categorical

* **Polars → Arrow**: categorical copies on `DataFrame.to_arrow`. ([Polars User Guide][2])
  If you’re chasing strict zero-copy interchange, normalize categoricals (e.g., cast to string or integer codes) before exporting.

#### Chunking

* **Series → Arrow** is zero-copy only when the Series is **single-chunk**. ([Polars User Guide][4])
* If you use `pa.array(series)` to force a contiguous Arrow Array, Polars warns it won’t be zero-copy if the Series has multiple chunks. ([Polars User Guide][3])
* `pl.from_arrow(..., rechunk=True)` (default) may copy to guarantee contiguous memory. ([Polars User Guide][5])
* If you combine Arrow chunks explicitly (`Table.combine_chunks()`), you’re explicitly asking Arrow to concatenate chunks (i.e., allocate/copy). ([Apache Arrow][16])

#### Unsupported Arrow types

`pl.from_arrow` will cast types not supported by Polars to “closest supported” types. That’s convenient, but it means “zero-copy” may degrade to “copy + cast” and dtypes may shift unless you pin schema overrides. ([Polars User Guide][5])

---

## Practical “integration contracts” (what I’d standardize in your project)

1. **Boundary objects**: treat `pyarrow.Table` + `RecordBatchReader` as your canonical interchange format for Polars ↔ DataFusion ↔ Ibis ↔ Delta. ([Apache DataFusion][6])
2. **Zero-copy policy**:

   * Avoid Polars categoricals at the export boundary (or accept copy). ([Polars User Guide][2])
   * Track chunking; don’t assume `Series.to_arrow()` is zero-copy unless single-chunk. ([Polars User Guide][4])
   * Disable `rechunk` on import only when you explicitly accept chunked memory. ([Polars User Guide][5])
3. **`scan_pyarrow_dataset` policy**:

   * Use only when you need PyArrow dataset features (cloud FS / partitioning).
   * Default `allow_pyarrow_filter=False` unless predicates are null-explicit and tested.
   * Always construct datasets with explicit partitioning if you rely on partition columns. ([Polars User Guide][11])

If you want the next increment, the natural follow-on in this “PyArrow integration” chapter is a **typed schema compatibility matrix**: Polars dtype ↔ Arrow type (including `string_view`, dictionary/categorical, nested structs/lists, timestamps/timezones), plus a “copy-risk” column (zero-copy / maybe-copy / guaranteed-copy) and the exact knobs (`compat_level`, `rechunk`, chunk combine) that flip those modes.

[1]: https://docs.pola.rs/ "Index - Polars user guide"
[2]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.to_arrow.html "polars.DataFrame.to_arrow — Polars  documentation"
[3]: https://docs.pola.rs/user-guide/misc/arrow/ "Arrow producer/consumer - Polars user guide"
[4]: https://docs.pola.rs/py-polars/html/reference/series/api/polars.Series.to_arrow.html "polars.Series.to_arrow — Polars  documentation"
[5]: https://docs.pola.rs/api/python/dev/reference/api/polars.from_arrow.html "polars.from_arrow — Polars  documentation"
[6]: https://datafusion.apache.org/python/user-guide/io/arrow.html "Arrow — Apache Arrow DataFusion  documentation"
[7]: https://datafusion.apache.org/python/user-guide/data-sources.html?utm_source=chatgpt.com "Data Sources — Apache Arrow DataFusion documentation"
[8]: https://ibis-project.org/reference/expression-tables "expression-tables – Ibis"
[9]: https://delta-io.github.io/delta-rs/integrations/delta-lake-arrow/ "Arrow - Delta Lake Documentation"
[10]: https://delta-io.github.io/delta-rs/usage/writing/ "Writing Delta Tables - Delta Lake Documentation"
[11]: https://docs.pola.rs/api/python/dev/reference/api/polars.scan_pyarrow_dataset.html "polars.scan_pyarrow_dataset — Polars  documentation"
[12]: https://docs.pola.rs/user-guide/io/cloud-storage/ "Cloud storage - Polars user guide"
[13]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.partitioning.html?utm_source=chatgpt.com "pyarrow.dataset.partitioning — Apache Arrow v22.0.0"
[14]: https://arrow.apache.org/docs/python/compute.html "Compute Functions — Apache Arrow v22.0.0"
[15]: https://arrow.apache.org/docs/python/api/compute.html "Compute Functions — Apache Arrow v22.0.0"
[16]: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html?utm_source=chatgpt.com "pyarrow.Table — Apache Arrow v22.0.0"

## 7) Delta Lake integration (Polars-native Delta IO + `deltalake`/delta-rs seam)

Polars’ Delta Lake integration is essentially:

* **Polars authoring/execution** (eager `DataFrame` + lazy `LazyFrame`)
* **Delta protocol + transaction log + object-store backends** via **`deltalake` (delta-rs)**
* **Data files** are Parquet; **metadata** is the Delta transaction log (JSON + checkpoints), surfaced through a `DeltaTable` state object. ([Delta][1])

In practice, you treat **Delta** as the *storage + snapshotting layer*, and **Polars** as the *compute layer*, with “Arrow/Parquet + Delta log” as the seam.

---

# 7.1 Read APIs

## 7.1.1 `pl.read_delta(...)` (eager read surface)

### API surface

`polars.read_delta(source, *, version=None, columns=None, rechunk=None, storage_options=None, credential_provider='auto', delta_table_options=None, use_pyarrow=False, pyarrow_options=None) -> DataFrame` ([Polars User Guide][2])

Key parameters:

* **`source`**: path/URI to table root or an already-open `DeltaTable`. For cloud stores you must use full URIs (GCS/Azure/S3). ([Polars User Guide][2])
* **`version`**: “numerical version or timestamp version” (signature allows `int | str | datetime | None`). If omitted, reads **latest**. ([Polars User Guide][2])
* **`columns`**: projection at read time. ([Polars User Guide][2])
* **`rechunk`**: coalesce chunked arrays into contiguous buffers (may allocate/copy). ([Polars User Guide][2])
* **`storage_options` / `credential_provider`**: forwarded to `deltalake` storage backends; credential provider is explicitly marked **unstable** in Polars docs. ([Polars User Guide][2])
* **`delta_table_options`**: forwarded to delta-rs `DeltaTable` constructor (see §7.1.3). ([Polars User Guide][2])
* **`use_pyarrow` / `pyarrow_options`**: switch to PyArrow dataset reads / Arrow conversion details (important for partitioned datasets and some filesystem situations). ([Polars User Guide][2])

### Minimal examples

**Read latest**

```python
import polars as pl
df = pl.read_delta("/path/to/delta-table/")
```

([Polars User Guide][2])

**Time travel by version**

```python
df_v1 = pl.read_delta("/path/to/delta-table/", version=1)
```

([Polars User Guide][2])

**Time travel by timestamp**

```python
from datetime import datetime, timezone
df_t = pl.read_delta("/path/to/delta-table/", version=datetime(2020, 1, 1, tzinfo=timezone.utc))
```

([Polars User Guide][2])

### Failure modes (the ones you’ll hit in real pipelines)

* **Non-existent version/timestamp** → read fails (Polars explicitly notes “will fail if provided version does not exist”). ([Polars User Guide][2])
* **Auth/config issues** (object store): surfaced from delta-rs/object_store; solve via `storage_options` or `credential_provider`. ([Polars User Guide][2])
* **Metadata load cost**: Delta reads must interpret the transaction log to determine the active file set (this is part of why `DeltaTable` exists). ([Delta][3])

---

## 7.1.2 `pl.scan_delta(...)` (lazy scan surface)

### API surface

`polars.scan_delta(source, *, version=None, storage_options=None, credential_provider='auto', delta_table_options=None, use_pyarrow=False, pyarrow_options=None, rechunk=None) -> LazyFrame` ([Polars User Guide][4])

Notable knobs vs `read_delta`:

* no `columns=` param: you express projection via `.select(...)` in the lazy plan
* **`pyarrow_options`** is explicitly recommended “when filtering on partitioned columns or to read from a fsspec supported filesystem.” ([Polars User Guide][4])

### Minimal examples

**Lazy scan latest**

```python
import polars as pl
lf = pl.scan_delta("/path/to/delta-table/")
out = lf.filter(pl.col("x") > 0).select(["x", "y"]).collect()
```

([Polars User Guide][4])

**Time travel**

```python
from datetime import datetime, timezone
lf = pl.scan_delta("/path/to/delta-table/", version=datetime(2020, 1, 1, tzinfo=timezone.utc))
out = lf.collect()
```

([Polars User Guide][4])

**Cloud (S3/GCS/Azure)**
Polars provides explicit examples for each storage, including Azure URI schemes and storage keys. ([Polars User Guide][4])

### Versioning / time travel failure modes

* Same as `read_delta`: non-existent versions → failure. ([Polars User Guide][4])
* **Polars ↔ deltalake version drift**: because Polars forwards into delta-rs APIs, mismatched versions can break (e.g., schema conversion method changes). This has shown up as runtime errors in practice. ([GitHub][5])

---

## 7.1.3 Delta-table load knobs (`delta_table_options`) and why they matter

Polars read/scan accept `delta_table_options` and forward them to delta-rs’ `DeltaTable` loader. That loader has **memory + concurrency levers** that matter at scale:

* `without_files`: load the table **without tracking active data files**, yielding “significant memory reduction” for append-only workflows. ([Delta][6])
* `log_buffer_size`: controls concurrency while reading the commit log; higher can reduce latency but increases memory and risks storage rate limits. Default behavior is tied to CPU count. ([Delta][6])

Polars’ `scan_delta` docs show `without_files=True` explicitly as an example. ([Polars User Guide][4])

---

# 7.2 Write APIs

## 7.2.1 `DataFrame.write_delta(...)` modes + core knobs

### API surface

`DataFrame.write_delta(target, *, mode='error'|'append'|'overwrite'|'ignore'|'merge', overwrite_schema=None, storage_options=None, credential_provider='auto', delta_write_options=None, delta_merge_options=None) -> TableMerger | None` ([Polars User Guide][7])

Mode semantics:

* `error`: fail if table exists
* `append`: add new data files, commit new version
* `overwrite`: replace table content (new version)
* `ignore`: no-op if table exists
* `merge`: return a **`TableMerger`** object (for Delta MERGE / upsert workflows) ([Polars User Guide][7])

Schema evolution knobs:

* `overwrite_schema` exists but is deprecated; use `delta_write_options={"schema_mode": "overwrite"}` instead. ([Polars User Guide][7])
* delta-rs `write_deltalake` supports `schema_mode` = `"merge"` or `"overwrite"` for evolution behavior. ([Delta][8])

### Minimal examples

**Create (or error if exists)**

```python
import polars as pl
df.write_delta("/path/to/delta-table/")
```

([Polars User Guide][7])

**Append (schema must match)**

```python
df.write_delta("/path/to/delta-table/", mode="append")
```

Polars notes this fails if schemas don’t match. ([Polars User Guide][7])

**Overwrite with schema overwrite**

```python
df.write_delta(
    "/path/to/delta-table/",
    mode="overwrite",
    delta_write_options={"schema_mode": "overwrite"},
)
```

([Polars User Guide][7])

---

## 7.2.2 Schema / dtype / nullability constraints (the “sharp edges”)

### Delta protocol dtype constraints (Polars-side)

Polars explicitly documents:

* Polars `Null` and `Time` dtypes are **not supported by Delta protocol** → `TypeError`
* Polars `Categorical` is converted to plain strings when written ([Polars User Guide][7])

These are “pipeline contract” constraints: either avoid them or normalize types before `write_delta`.

### Nullability mismatch: **Polars columns are always nullable**

Polars explicitly states: “Polars columns are always nullable.” To write to a Delta table with **non-nullable** columns, you must pass a **custom PyArrow schema** via `delta_write_options`. ([Polars User Guide][7])

**PyArrow schema injection example (non-nullable)**

```python
import pyarrow as pa
import polars as pl

df = pl.DataFrame({"foo": [1, 2, 3]})

df.write_delta(
    "/path/to/delta-table/",
    delta_write_options={
        "schema": pa.schema([pa.field("foo", pa.int64(), nullable=False)]),
    },
)
```

([Polars User Guide][7])

This is the canonical “nullability contract bridge” between Polars and Delta Lake.

---

## 7.2.3 `delta_write_options`: mapping to delta-rs `write_deltalake(...)`

Polars’ `delta_write_options` is a pass-through dict to delta-rs writer parameters (“see supported write options here”). ([Polars User Guide][7])
The delta-rs writer surface includes (non-exhaustive high-value knobs): ([Delta][8])

* `partition_by`: define table partitioning
* `schema_mode`: `"merge"` or `"overwrite"`
* `predicate`: overwrite-by-predicate semantics when `mode='overwrite'` (analogous to replaceWhere)
* `target_file_size`: override target Parquet file size (else uses `delta.targetFileSize`)
* `writer_properties`: Parquet writer properties (compression, etc.)
* `post_commithook_properties`, `commit_properties`: commit metadata/hook behavior

**Compression via WriterProperties**
Polars shows using `deltalake.WriterProperties(compression="zstd")` inside `delta_write_options`. ([Polars User Guide][7])

```python
import deltalake
df.write_delta(
    "s3://bucket/table",
    delta_write_options={
        "writer_properties": deltalake.WriterProperties(compression="zstd"),
    },
)
```

([Polars User Guide][7])

---

## 7.2.4 Merge/upsert seam (`mode="merge"`)

Polars supports a `merge` mode that returns a delta-rs `TableMerger` object and takes `delta_merge_options`. ([Polars User Guide][7])

The important operational edge:

* If you attempt MERGE against a non-existent table, Polars documents `TableNotFoundError`. ([Polars User Guide][7])

(If you standardize MERGE in your architecture, treat “table exists” as a precondition contract and enforce it explicitly.)

---

# 7.3 Storage + object-store options

## 7.3.1 The unifying rule: `storage_options` flows into delta-rs/object_store

All three Polars Delta entrypoints accept `storage_options` (read/scan/write). ([Polars User Guide][2])
delta-rs documents that `storage_options` are backend-specific for S3/Azure/GCS. ([Delta][3])

Polars additionally offers `credential_provider` (function returning credential dict + optional expiry) for cloud stores, marked unstable. ([Polars User Guide][2])

---

## 7.3.2 S3: credentials + concurrency safety (locking provider)

### Basic `storage_options` for Polars read/scan

Polars shows the minimal S3 auth keys (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_REGION`). ([Polars User Guide][4])

### The big delta-rs-specific nuance: safe concurrent writes require locking

delta-rs documents:

* It does **not** use boto3 (so it won’t automatically use `~/.aws/config` / `~/.aws/creds` in the same way many Python libs do). ([Delta][9])
* For **safe concurrent writes on S3**, you need a **locking provider** because S3 does not guarantee mutual exclusion. delta-rs uses DynamoDB for this. ([Delta][9])

Concrete keys shown in delta-rs S3 backend docs:

* `AWS_S3_LOCKING_PROVIDER='dynamodb'`
* `DELTA_DYNAMO_TABLE_NAME='delta_log'` ([Delta][9])

There’s also an “unsafe” escape hatch:

* set `AWS_S3_ALLOW_UNSAFE_RENAME=true` to enable unsafe writes without DynamoDB. ([Delta][9])

**Pipeline implication:** for production multi-writer S3 pipelines, the “locking provider configuration” is part of your **storage contract**, not an implementation detail.

---

## 7.3.3 Azure (ADLS/Blob): URI schemes + credential patterns

Polars documents Azure delta paths in multiple URI forms (`az://`, `adl://`, `abfs[s]://`) and shows `AZURE_STORAGE_ACCOUNT_NAME` / `AZURE_STORAGE_ACCOUNT_KEY` as `storage_options`. ([Polars User Guide][4])

delta-rs’ Azure backend docs show an explicit Polars example using an `abfs://` URL and `storage_options` keys `ACCOUNT_NAME` and `ACCESS_KEY`, and note that credentials are forwarded to the underlying object store library. ([Delta][10])

It also shows an Azure CLI-based auth path (example uses `azure_tenant_id` + `azure_use_azure_cli="true"`). ([Delta][10])

**Operational advice:** pick one canonical credential style per deployment surface (local dev, CI, prod) and standardize it behind a “storage_options builder” so your Polars calls stay stable.

---

## 7.3.4 GCS: ADC vs explicit credentials + permissions

delta-rs documents:

* GCS support is native; no extra deps needed.
* You can use **Application Default Credentials (ADC)** so you don’t have to pass credentials explicitly. ([Delta][11])

Polars’ own read/scan examples show a common explicit pattern:

* `storage_options = {"SERVICE_ACCOUNT": "SERVICE_ACCOUNT_JSON_ABSOLUTE_PATH"}` ([Polars User Guide][4])

delta-rs also documents required GCS permissions (`storage.objects.create`, `storage.objects.delete` for overwrite scenarios, etc.). ([Delta][11])

---

# 7.4 Delta Lake guarantees as Polars pipeline “transform contracts”

The right way to think about Delta in a Polars-heavy pipeline is: **each write is a versioned, atomic, addressable artifact**.

## 7.4.1 Atomicity + transaction log = “no partial commits”

delta-rs’ transactions documentation states:

* Delta write operations are **transactions**: they change table state and append metadata entries to the transaction log. ([Delta][12])
* Atomicity: data files are written first; the commit becomes visible only when the transaction log entry is added. If a job fails mid-write, the partial Parquet files are ignored because no log entry is created; they can be cleaned up later. ([Delta][12])

**Pipeline contract:** downstream stages should treat “committed version exists” as the only indicator that a stage completed successfully.

## 7.4.2 Versioning/time travel = reproducibility + debugging primitive

* A `DeltaTable` represents table state at a particular version (files, schema, metadata). ([Delta][3])
* Polars supports time travel directly via `version=` on `read_delta` and `scan_delta`. ([Polars User Guide][2])

**Pipeline contract:** log the exact `version` your stage produced (or consumed) so reruns and investigations are deterministic.

## 7.4.3 Single-table transactions (no multi-table atomicity)

Delta Lake transactions are scoped to a **single table** (no multi-table commit guarantees). ([Delta][12])
So if your pipeline “atomically updates multiple tables,” you must build that atomicity at a higher layer (e.g., a manifest table, external orchestrator, or application-level two-phase protocol).

## 7.4.4 Performance contracts: file skipping + Z-ordering (optional but high leverage)

Delta’s transaction log can store per-file statistics (min/max) enabling **file skipping**: engines can avoid scanning irrelevant Parquet files by pushing predicates into metadata-driven pruning. ([Delta][13])
Z-ordering improves skipping by colocating similar values into fewer files. ([Delta][14])

**Contract framing:** if you rely on “fast point/range queries,” treat “stats present + layout optimized” as part of the table’s operational contract, not just “nice to have.”

---

## Recommended “standard integration shape” for your stack

1. **Read/Scan**: default to `pl.scan_delta(...)` for transforms; reserve `read_delta` for small tables or boundary materializations. ([Polars User Guide][4])
2. **Write**: `df.write_delta(..., mode=...)` with explicit `delta_write_options` for schema evolution and Parquet writer properties. ([Polars User Guide][7])
3. **Contracts**:

   * record the **committed version** externally,
   * enforce schema/nullability explicitly (PyArrow schema injection when needed),
   * make object-store credentials + S3 locking provider part of deployment config. ([Polars User Guide][7])

If you want the next increment, the natural follow-on is a **compatibility matrix**: Polars dtype ↔ Delta protocol ↔ Arrow type (including timestamp/timezone, dictionary/categorical, nested structs/lists), plus “write-time coercions” (e.g., categorical→string) and “hard failures” (`Null`/`Time`).

[1]: https://delta-io.github.io/delta-rs/ "Home - Delta Lake Documentation"
[2]: https://docs.pola.rs/api/python/dev/reference/api/polars.read_delta.html "polars.read_delta — Polars  documentation"
[3]: https://delta-io.github.io/delta-rs/usage/loading-table/ "Loading a table - Delta Lake Documentation"
[4]: https://docs.pola.rs/api/python/dev/reference/api/polars.scan_delta.html "polars.scan_delta — Polars  documentation"
[5]: https://github.com/pola-rs/polars/issues/23831?utm_source=chatgpt.com "AttributeError in pl.scan_delta with DeltaLake · Issue #23831"
[6]: https://delta-io.github.io/delta-rs/api/delta_table/?utm_source=chatgpt.com "DeltaTable - Delta Lake Documentation"
[7]: https://docs.pola.rs/api/python/dev/reference/api/polars.DataFrame.write_delta.html "polars.DataFrame.write_delta — Polars  documentation"
[8]: https://delta-io.github.io/delta-rs/api/delta_writer/ "Writer - Delta Lake Documentation"
[9]: https://delta-io.github.io/delta-rs/integrations/object-storage/s3/ "AWS S3 Storage Backend - Delta Lake Documentation"
[10]: https://delta-io.github.io/delta-rs/integrations/object-storage/adls/ "Azure ADLS Storage Backend - Delta Lake Documentation"
[11]: https://delta-io.github.io/delta-rs/integrations/object-storage/gcs/ "GCS Storage Backend - Delta Lake Documentation"
[12]: https://delta-io.github.io/delta-rs/how-delta-lake-works/delta-lake-acid-transactions/ "Transactions - Delta Lake Documentation"
[13]: https://delta-io.github.io/delta-rs/how-delta-lake-works/delta-lake-file-skipping/?utm_source=chatgpt.com "File skipping - Delta Lake Documentation"
[14]: https://delta-io.github.io/delta-rs/usage/optimize/delta-lake-z-order/?utm_source=chatgpt.com "Z Order - Delta Lake Documentation"

## 8) DataFusion integration (interop + division of labor)

The Polars ↔ DataFusion seam is best treated as an **Arrow FFI boundary**: both sides can *export/import Arrow streams* via the Arrow C Data / C Stream (“PyCapsule”) interface, enabling **zero-copy (or near-zero-copy) interop** without round-tripping through pandas. DataFusion’s Python API is explicit about this: `SessionContext.from_arrow()` accepts any object implementing `__arrow_c_stream__` or `__arrow_c_array__` (the latter must be a struct array). ([Apache DataFusion][1])
Polars DataFrames implement `__arrow_c_stream__` for PyCapsule export. ([Polars User Guide][2])

---

# 8.1 Import Polars into DataFusion (Arrow FFI boundary)

## 8.1.1 Preferred path: `from_arrow()` (generic Arrow PyCapsule)

### What it accepts

`SessionContext.from_arrow(data)` accepts *any* Python object with:

* `__arrow_c_stream__` (stream export), **or**
* `__arrow_c_array__` (array export; must be a **struct array**) ([Apache DataFusion][1])

DataFusion’s “Other DataFrame Libraries” docs: since **DataFusion 42.0.0**, any DataFrame library supporting the Arrow FFI PyCapsule interface can be imported via `from_arrow()`. ([Apache DataFusion][3])

### Why it’s the right default

* It avoids library-specific adapters.
* It’s the same entrypoint for Polars / Pandas / PyArrow / anything Arrow-stream-exportable. ([Apache DataFusion][1])

### Minimal example (Polars → DataFusion)

```python
import polars as pl
from datafusion import SessionContext

pl_df = pl.DataFrame({"k": ["a", "a", "b"], "v": [1, 2, 3]})

ctx = SessionContext()

# Polars DataFrame exports __arrow_c_stream__ via Arrow PyCapsule
df = ctx.from_arrow(pl_df)   # preferred path
```

**Notes**

* This works because Polars implements `DataFrame.__arrow_c_stream__`. ([Polars User Guide][2])
* The import contract is “Arrow stream/array exportable”, not “Polars-specific”. ([Apache DataFusion][1])

## 8.1.2 Legacy adapter: `from_polars()`

DataFusion still exposes `SessionContext.from_polars(polars_df)` as a convenience. ([Apache DataFusion][1])
The DataFusion “Data Sources” docs describe `from_polars()` as the fallback for **older Polars versions** that may not support the Arrow interface. ([Apache DataFusion][3])

```python
import polars as pl
from datafusion import SessionContext

ctx = SessionContext()
pl_df = pl.DataFrame({"a": [1, 2, 3]})

df = ctx.from_polars(pl_df)   # compatibility path
```

### Practical standard

* Prefer `from_arrow()` in all new code.
* Keep `from_polars()` only as a compatibility fallback (or for ergonomics if you’ve pinned older Polars). ([Apache DataFusion][3])

---

# 8.2 Execution division-of-labor patterns

Think of the combined system as two engines sharing the same memory format:

* **Polars**: expression-first dataframe DSL (very rich column/list/struct/time-series surface; excellent for “data-wrangling-shaped” transforms).
* **DataFusion**: query engine with a SQL + DataFrame API, optimizer, and extensibility hooks (UDF/UDAF, providers, planners), executing natively in Rust on Arrow. ([arrow.staged.apache.org][4])

The value is not “pick one”; it’s **stage the pipeline** so each engine does what it’s best at, and interop is an Arrow relay.

## 8.2.1 Pattern A: Polars → DataFusion (DataFusion as optimizer/relational stage)

**Use when**

* You need DataFusion’s SQL surface for complex relational rewrites or compatibility (but still want SQL-free authoring in most of your pipeline).
* You want DataFusion’s physical planning/execution for joins, aggregations, scans, or extension points.
* You want to keep downstream consumers in Arrow-native land.

**Mechanics**

* Build/normalize with Polars.
* Hand off via `ctx.from_arrow(polars_df_or_arrow)` (FFI).
* Execute via DataFusion.
* Export results as Arrow (table or stream) for the next stage.

**Example**

```python
import polars as pl
from datafusion import SessionContext, col, lit

# Polars: expression-heavy feature engineering
pl_df = (
    pl.DataFrame({"k": ["a", "a", "b"], "v": [1, 2, 3]})
      .with_columns((pl.col("v") * 10).alias("v10"))
)

# DataFusion: relational stage
ctx = SessionContext()
df = ctx.from_arrow(pl_df)

out = (
    df.filter(col("v10") > lit(10))
      .aggregate(group_by=[col("k")], aggs=[col("v10").sum().alias("sum_v10")])
)

# Export (table) - executes the plan
arrow_tbl = out.to_arrow_table()
```

`to_arrow_table()` is explicitly documented as “execute and convert into an Arrow Table.” ([Apache DataFusion][5])

## 8.2.2 Pattern B: DataFusion → Polars (Polars as “final mile” transform stage)

**Use when**

* DataFusion is your scan/join/aggregate engine, but you want Polars’ expression surface for final shaping.
* You want Polars-native sinks (including Delta) after DataFusion finishes.

**Mechanics**

* Produce Arrow from DataFusion.
* `pl.from_arrow(...)` into Polars.

**Example**

```python
import polars as pl
from datafusion import SessionContext

ctx = SessionContext()
df = ctx.from_pydict({"k": ["a", "a", "b"], "v": [1, 2, 3]})

# DataFusion stage
df2 = df.aggregate(group_by=["k"], aggs=[("v", "sum")])
arrow_tbl = df2.to_arrow_table()  # executes

# Polars final mile
pl_df = pl.from_arrow(arrow_tbl)  # “zero copy for the most part” ingest
```

Polars `from_arrow` is designed to be zero-copy “for the most part” (with casting if Arrow has types Polars doesn’t support). ([Polars User Guide][6])

## 8.2.3 “Zero-copy relay” via Arrow streams (what you should standardize)

There are two “Arrow boundary” flavors:

### (1) Materialized Arrow Table boundary (simple, but materializes)

* `DataFusion DataFrame -> to_arrow_table()` (executes + materializes) ([Apache DataFusion][5])
* `Polars -> to_arrow()` and `pl.from_arrow()` (mostly zero-copy, but still a table materialization boundary) ([Polars User Guide][6])

### (2) Arrow C Stream boundary (true streaming)

DataFusion DataFrames implement `__arrow_c_stream__`, enabling **zero-copy, lazy streaming** into Arrow-based Python libraries; “batches are produced on demand.” ([Apache DataFusion][7])

This is the interop primitive you want when:

* results are large,
* you want downstream to pull batches incrementally,
* you want to avoid a giant in-memory `Table`.

**Operational note:** the exact consumer-side constructor is library-dependent (PyArrow, Polars, etc.). The important architectural choice is: **prefer Arrow stream export over pandas conversion**, and use `to_arrow_table()` only when you explicitly accept materialization. ([Apache DataFusion][7])

---

# 8.3 DataFusion runtime characteristics relevant to this seam

The seam’s performance is governed by **DataFusion’s partitioned, multi-threaded execution**, and how you choose **partitioning boundaries** between engines.

## 8.3.1 Multi-threaded engine (what to assume)

DataFusion’s Python `SessionContext` docs: it has a “powerful optimizer,” “physical planner,” and a **multi-threaded execution engine**. ([arrow.staged.apache.org][4])
The `DataFrame` docs emphasize execution runs natively in Rust/Arrow in a multi-threaded environment. ([arrow.staged.apache.org][8])

**Implication**

* DataFusion will parallelize via partitions; you should reason about partitions as the unit of concurrency and shuffles.

## 8.3.2 `target_partitions`: the key concurrency dial

In Python, `SessionConfig.with_target_partitions(n)` sets the target partitions for query execution; DataFusion explicitly notes that increasing partitions can increase concurrency. ([Apache DataFusion][9])

```python
from datafusion import SessionConfig, SessionContext

cfg = SessionConfig().with_target_partitions(64)
ctx = SessionContext(cfg)
```

## 8.3.3 Optimizer repartition knobs (joins/aggregations/windows/sorts)

DataFusion exposes optimizer settings that repartition data to execute operators in parallel at the `target_partitions` level:

* `datafusion.optimizer.repartition_aggregations` ([Apache DataFusion][10])
* `datafusion.optimizer.repartition_joins` ([Apache DataFusion][10])
* `datafusion.optimizer.repartition_windows` ([Apache DataFusion][10])
* `datafusion.optimizer.repartition_sorts` (sort per-partition then merge, rather than global coalesce) ([Apache DataFusion][10])

**Implications at the Polars ↔ DataFusion boundary**

* If you import a single-partition Arrow source into DataFusion and then run heavy joins/aggregations, the optimizer may insert repartition steps (shuffles) to increase parallelism.
* If you can present DataFusion with naturally partitioned/streaming inputs (e.g., a scan with multiple partitions, or an Arrow stream that produces partitions/batches), you often reduce the need for expensive repartitioning.

## 8.3.4 Practical boundary rules for “best-of-both” performance

1. **Don’t bounce engines unnecessarily**
   Each boundary can force materialization (if you use tables instead of streams) and can lose optimizer context.

2. **If DataFusion is doing the heavy relational work, keep data in DataFusion until the relational shape is small**
   Then export to Arrow (or stream) and finish with Polars.

3. **If Polars is doing the heavy expression work, do it before DataFusion introduces shuffles**
   Polars excels at local columnar transforms; feed DataFusion “narrower/smaller” tables.

4. **Treat “partitions” as your cross-engine performance contract**

* DataFusion: tune `target_partitions`, and know joins/aggregations/windows/sorts may repartition. ([Apache DataFusion][10])
* Polars: if you re-enter Polars with a giant materialized Arrow table, you’ve lost streaming benefits; prefer stream-based handoff where possible. ([Apache DataFusion][7])

---

## Minimal “integration checklist” to standardize in your codebase

* **Import**: prefer `ctx.from_arrow(obj)`; keep `from_polars` only as fallback for older Polars. ([Apache DataFusion][3])
* **Export**:

  * prefer Arrow stream protocol for large results (DataFusion DF implements `__arrow_c_stream__`). ([Apache DataFusion][7])
  * use `to_arrow_table()` only when you accept materialization. ([Apache DataFusion][5])
* **Concurrency**: set `SessionConfig.with_target_partitions(...)` intentionally; expect DataFusion to repartition joins/aggs/windows/sorts for parallel execution. ([Apache DataFusion][9])

If you want the next increment after this: I can produce a “seam cookbook” with **three canonical pipelines** (Polars→DataFusion→Polars; DataFusion→Polars→Delta; Delta→Polars→DataFusion) including **batch vs stream boundaries**, recommended partition counts, and the exact places to take plan/profile snapshots on each side.

[1]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[2]: https://docs.pola.rs/api/python/dev/reference/dataframe/api/polars.DataFrame.__arrow_c_stream__.html "polars.DataFrame.__arrow_c_stream__ — Polars  documentation"
[3]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[4]: https://arrow.staged.apache.org/datafusion-python/generated/datafusion.SessionContext.html "datafusion.SessionContext — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html?utm_source=chatgpt.com "datafusion.dataframe"
[6]: https://docs.pola.rs/py-polars/html/reference/api/polars.from_arrow.html?utm_source=chatgpt.com "polars.from_arrow — Polars documentation"
[7]: https://datafusion.apache.org/python/user-guide/dataframe/index.html "DataFrames — Apache Arrow DataFusion  documentation"
[8]: https://arrow.staged.apache.org/datafusion-python/generated/datafusion.DataFrame.html "datafusion.DataFrame — Apache Arrow DataFusion  documentation"
[9]: https://datafusion.apache.org/python/autoapi/datafusion/index.html "datafusion — Apache Arrow DataFusion  documentation"
[10]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"

## 9) Ibis integration (portable IR + Polars backend specifics)

Ibis’ *integration model* with Polars is: **Ibis IR → Polars `LazyFrame` plan** (not SQL), executed by Polars; and you can always **export results** as Polars / PyArrow / `RecordBatchReader` for interop with DataFusion/Delta/etc. The Polars backend docs state that `compile()` produces a **LazyFrame for the polars backend** (vs SQL for SQL backends). ([Ibis][1])

---

# 9.1 Polars backend API surface

## 9.1.1 Connect / in-memory registration

### `ibis.polars.connect(tables=...)`

The Polars backend connection is essentially an **in-memory catalog**: `do_connect(self, tables=None)` constructs a client from a mapping of table names to Polars `LazyFrame`/`DataFrame`. ([Ibis][1])

```python
import ibis
import polars as pl

lf = pl.LazyFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
con = ibis.polars.connect(tables={"t": lf})

t = con.table("t")  # ibis table expression
```

This is your *escape hatch* for “Ibis IR ↔ native Polars plan” (see §9.3).

---

## 9.1.2 Register file-backed tables (CSV / Parquet / JSON / Delta)

The Polars backend exposes `read_*` methods that **register** external data as Ibis tables, forwarding keyword args to the underlying Polars scan functions:

### `con.read_csv(path, table_name=..., **kwargs)`

Registers a CSV as a table and forwards kwargs to Polars’ CSV scanner (`polars.scan_csv`). ([Ibis][1])

### `con.read_parquet(path(s), table_name=..., **kwargs)`

Registers parquet inputs as a table; docs explicitly note kwargs go to `polars.scan_parquet`, and that when loading multiple files, Polars’ `scan_pyarrow_dataset` is used instead. ([Ibis][1])

### `con.read_json(path, table_name=..., **kwargs)`

Registers a JSON/NDJSON file as a table; docs point to Polars’ `scan_ndjson`. ([Ibis][1])

### Delta registration: `con.read_delta(path, table_name=..., **kwargs)`

Registers a Delta Lake table directory as a table; docs explicitly say kwargs are passed to Polars’ `scan_delta`. ([Ibis][1])

```python
con = ibis.polars.connect()

t_csv   = con.read_csv("data.csv", table_name="csv")
t_pq    = con.read_parquet("data/*.parquet", table_name="pq")
t_json  = con.read_json("data.ndjson", table_name="json")
t_delta = con.read_delta("table.delta", table_name="delta")  # Polars scan_delta under the hood
```

### Top-level convenience functions (`ibis.read_*`)

Ibis also has top-level file readers/writers (backend-dependent). The basic IO guide shows `t.to_delta(...)` and `ibis.read_delta(...)` for Delta Lake, and similarly for Parquet/CSV. ([Ibis][2])

---

## 9.1.3 Compile target: Ibis IR → Polars `LazyFrame`

A key backend-specific hook: Polars backend `compile(expr, ...)` compiles an Ibis expression into a **Polars LazyFrame** (not SQL). ([Ibis][1])

That makes Polars the “native plan” for this backend—useful for (a) debugging and (b) backend-only feature escape hatches.

---

# 9.2 Execution/export surfaces (Polars / Arrow / batches)

There are two equivalent “execution surfaces” you should know:

1. **Expression-level** methods (`t.to_polars()`, `t.to_pyarrow_batches()`, etc.)
2. **Backend-level** methods (`con.to_polars(expr)`, `con.to_pyarrow_batches(expr)`, etc.)

Both execute eagerly; the difference is call site and optional backend-only knobs.

---

## 9.2.1 `to_polars()`: execute and return a Polars DataFrame

### Expression-level

`Table.to_polars(params=None, limit=None, **kwargs)` executes and returns a Polars DataFrame. ([Ibis][3])

### Backend-level (Polars backend)

`polars.Backend.to_polars(self, expr, ..., engine='cpu', **kwargs)` executes and returns a Polars DataFrame; note the backend-level signature includes an `engine` argument. ([Ibis][1])

```python
df = t.filter(t.a > 1).to_polars()
# or
df = con.to_polars(t.filter(t.a > 1), engine="cpu")
```

---

## 9.2.2 `to_pyarrow()` and `to_pyarrow_batches()`: Arrow as the interop boundary

### Backend-level: `to_pyarrow(expr, ...)`

Polars backend `to_pyarrow` executes an expression to a PyArrow object; docs specify the return shape depends on whether the expression is Table/Column/Scalar. ([Ibis][1])

### Streaming export: `to_pyarrow_batches(limit=None, params=None, chunk_size=1_000_000, **kwargs)`

Both the expression-level API and the Polars backend expose `to_pyarrow_batches` returning a **`RecordBatchReader`** and taking a `chunk_size` parameter (“maximum number of rows in each returned record batch”, default 1,000,000). ([Ibis][3])

```python
reader = t.to_pyarrow_batches(chunk_size=250_000)  # RecordBatchReader
for batch in reader:
    ...
```

### Chunk sizing decisions (practical heuristics)

`chunk_size` is your **bounded-memory dial**:

* **Smaller batches**: lower peak memory per batch, easier to pipeline through Python loops or downstream consumers; more overhead (more batches).
* **Larger batches**: higher throughput, fewer Python-level iterations; higher peak memory and potentially larger downstream allocation spikes.

Because it’s “max rows per batch,” the right value depends on *row width* (number/size of columns) more than row count.

---

## 9.2.3 “Arrow out” for DataFusion / Delta

If your next stage is DataFusion or Delta writing, Arrow is the clean boundary:

* DataFusion can ingest Arrow streams/tables and execute in Rust; Polars can also ingest Arrow; Delta writers commonly accept Arrow tables/batches.
  So the canonical pattern is **Ibis → `to_pyarrow_batches` → consumer** (streaming) or **Ibis → `to_pyarrow`** (materialized). ([Ibis][3])

---

# 9.3 Portability vs backend-specific features (Polars-only semantics vs Ibis IR)

## 9.3.1 Ground rule: operation support varies by backend

Ibis provides an operation support matrix and explicitly warns that “support for the full breadth of the Ibis API varies” across backends. ([Ibis][4])
So you need to distinguish:

* **Representable in Ibis IR** (there is an Ibis operation / expression API)
* **Implemented in the Polars backend** (or any given backend)
* **Equivalent semantics to Polars-native behavior** (sometimes backends differ subtly)

---

## 9.3.2 As-of join: representable in Ibis IR; maps naturally to Polars’ `join_asof`

### Ibis API

Ibis has `Table.asof_join(right, on, predicates=(), tolerance=None, ...)` and documents it as “similar to a left join except match is on nearest key rather than equal keys.” ([Ibis][3])

### Polars native semantics (for your mental mapping)

Polars’ `DataFrame.join_asof` is the same concept (nearest-key match) and requires the inputs be sorted by the join key. ([Polars User Guide][5])

**Portability notes**

* **Ibis IR** can express as-of join cleanly (you can write it without SQL). ([Ibis][3])
* Whether the **Polars backend** supports it is an implementation question (use the support matrix / runtime smoke tests); if not, you’ll see a “missing operation” failure at compile/execute time. ([Ibis][4])

**Best-practice structure (backend-agnostic authoring)**

* Keep the as-of join in Ibis IR.
* If you need Polars-specific constraints (e.g., enforce sortedness), use an explicit ordering step on both inputs and treat it as part of your contract.

---

## 9.3.3 Dynamic time windows: Ibis has windowing primitives, but Polars’ `group_by_dynamic` isn’t a 1:1 portable IR concept

Polars’ `group_by_dynamic` has specialized semantics (window start rules, overlapping windows when `period > every`, etc.). Ibis’ *portable* windowing story is:

* **Analytic window functions** using `ibis.window(...)` / range/rows windows (SQL-style windows). ([Ibis][6])
* **Streaming window aggregations** using `window_by(...).tumble(...)` and `window_by(...).hop(...)` (tumble/hop windows). ([Ibis][7])

Those are *expressible in Ibis*, but backend support differs (e.g., the “streaming” window-by API is geared toward streaming-capable backends). ([Ibis][7])

**Pragmatic conclusion**

* If you want portability: implement windowed logic using Ibis window functions (`over`) or `window_by` where supported. ([Ibis][6])
* If you want **Polars-native `group_by_dynamic` semantics** specifically: treat it as a **backend-specific escape hatch** (see §9.3.5).

---

## 9.3.4 `list.eval`: Ibis can express “array element transforms” via `map/filter/unnest`, but backend support varies

Polars’ `list.eval` is “evaluate arbitrary expression per element.” In Ibis, the closest portable IR is **array expressions**, which explicitly include:

* `map` (apply a function/deferred per element)
* `filter` (filter array elements with a function)
* `unnest` (explode array to rows)
* plus a broad array operation surface (`ArrayMap`, `ArrayFilter`, `ArrayFlatten`, etc.) ([Ibis][8])

Example Ibis “list.eval-like” pattern (IR-level):

```python
import ibis

t = ibis.table(dict(id="int64", xs="array<int64>"), name="t")
expr = t.mutate(xs2=t.xs.map(lambda x: x + 1))
```

Array `map` is part of the Ibis array API. ([Ibis][8])

**Portability notes**

* This is *representable* in Ibis IR. ([Ibis][8])
* But not every backend supports higher-order array functions equally—so treat “array map/filter” as something you verify per backend (again: support matrix + smoke tests). ([Ibis][4])

---

## 9.3.5 “Portable core + backend escape hatch” (recommended integration pattern)

Because the Polars backend compiles to a **Polars LazyFrame**, you can implement a disciplined escape hatch:

1. Keep 90% of transforms in **Ibis IR** (portable)
2. For Polars-only operators, compile to Polars plan, apply native transforms, then re-register

**Key hook:** `compile()` returns a LazyFrame for the Polars backend. ([Ibis][1])
**Key hook:** `ibis.polars.connect(tables={...})` registers LazyFrames as Ibis tables. ([Ibis][1])

Pseudo-pattern:

```python
lf = con.compile(expr)               # -> polars.LazyFrame
lf2 = lf.group_by_dynamic(...).agg(...)  # Polars-only
con2 = ibis.polars.connect(tables={"t2": lf2})
t2 = con2.table("t2")
```

That gives you a **clean boundary**:

* Ibis IR for portability
* Polars native for features / performance
* Arrow export (`to_pyarrow_batches`) for interop with DataFusion/Delta downstream ([Ibis][1])

---

If you want the next deep dive after this, the natural continuation is: **SQLGlot boundary + deterministic compilation** for Ibis—i.e., where Ibis uses SQL compilation (for SQL backends), where it compiles to native plans (Polars), and how you standardize “AST inspection + rewrite hooks” consistently across both kinds of backends.

[1]: https://ibis-project.org/backends/polars "polars – Ibis"
[2]: https://ibis-project.org/how-to/input-output/basics "basics – Ibis"
[3]: https://ibis-project.org/reference/expression-tables "expression-tables – Ibis"
[4]: https://ibis-project.org/backends/support/matrix "Operation support matrix – Ibis"
[5]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.join_asof.html?utm_source=chatgpt.com "polars.DataFrame.join_asof — Polars documentation"
[6]: https://ibis-project.org/reference/expression-tables?utm_source=chatgpt.com "Table expressions - Ibis"
[7]: https://ibis-project.org/how-to/extending/streaming?utm_source=chatgpt.com "Ibis for streaming"
[8]: https://ibis-project.org/reference/expression-collections?utm_source=chatgpt.com "Collection expressions - Ibis"

## 10) SQL interface + SQLGlot boundary (even if you “avoid SQL in your code”)

You can treat SQL in this stack as a **secondary front-end** that compiles into the same underlying Polars execution engine. The “clean seam” is:

* **Internal authoring IR**: Polars `Expr` / `LazyFrame`
* **Optional ingestion dialect**: Polars SQL (query strings) or SQL fragments → `Expr`
* **AST policy / normalization / rewrite**: SQLGlot (parse → validate → normalize → format → emit SQL)

Polars’ SQL interface is *not* full SQL; it’s a pragmatic subset (mostly query-side), and it is intended to interoperate with Polars’ native engine. ([Polars User Guide][1])

---

# 10.1 Polars SQL engine

## 10.1.1 Entry points and scoping model

Polars documents **four primary entry points** to the SQL interface: `SQLContext`, top-level `pl.sql()`, frame-level `DataFrame.sql()`/`LazyFrame.sql()`, and `pl.sql_expr()` for SQL→Expr fragments. ([Polars User Guide][2])

### A) `pl.sql(query, *, eager=False) -> LazyFrame | DataFrame` (global namespace)

`pl.sql` executes a SQL query against frames in the **global namespace**; `eager=True` returns a collected `DataFrame`, otherwise you get a `LazyFrame`. ([Polars User Guide][3])

Under the hood, `pl.sql` uses `SQLContext.execute_global()` behavior: it **auto-registers compatible objects referenced in the query** (by variable name → table name mapping). ([Polars User Guide][4])

**Supported “tables” in this global mode** include:

* Polars `DataFrame` / `LazyFrame` / `Series`
* pandas `DataFrame` / `Series`
* PyArrow `Table` / `RecordBatch` ([Polars User Guide][3])

Polars explicitly notes non-Polars objects are implicitly converted to a `DataFrame`, and for PyArrow (and pandas using Arrow dtypes) this conversion can often be **zero-copy** if the underlying data maps cleanly to supported dtypes. ([Polars User Guide][2])

### B) `SQLContext` (explicit registry + execution)

`SQLContext` is the “production” API: you explicitly register tables and run queries against that registry. You can:

* `register(name, frame)`
* `register_many({...})`
* `register_globals(n=..., all_compatible=...)` ([Polars User Guide][5])

Then:

* `ctx.execute(query, eager=...) -> LazyFrame | DataFrame` ([Polars User Guide][6])

**Important semantic detail:** the SQL query itself is *always executed lazily*; `eager` only controls whether the returned object is collected into a `DataFrame` vs returned as a `LazyFrame`. ([Polars User Guide][6])

### C) Frame SQL: `DataFrame.sql()` and `LazyFrame.sql()` (“self” table)

Frame SQL is a scoped form: the calling frame is registered as a table named `"self"` (or a custom name). ([Polars User Guide][7])

* `LazyFrame.sql(query, *, table_name="self") -> LazyFrame` (documented as “unstable / may change”) ([Polars User Guide][8])
* `DataFrame.sql(query, *, table_name="self") -> DataFrame`, and Polars notes it executes the SQL in lazy mode before collecting and returning a `DataFrame`. ([Polars User Guide][7])

This is the best fit when you want “SQL as a view layer” on top of an already-defined Polars pipeline.

### D) Expression SQL: `pl.sql_expr(sql: str | Sequence[str]) -> Expr | list[Expr]`

`sql_expr` parses SQL *expression fragments* into native Polars `Expr` objects (single expr or list). ([Polars User Guide][9])

This is the canonical “minimize SQL” tool: accept a fragment like `"POWER(a,a) AS a_a"` and embed it into `with_columns/select` without switching your whole pipeline into query-string mode. ([Polars User Guide][9])

---

## 10.1.2 SQLContext registration details you should care about

### `register_globals(all_compatible=...)` is a footgun unless you’re in a notebook

`register_globals(all_compatible=True)` will register **pandas + PyArrow objects too**; set `all_compatible=False` if you want only Polars objects registered. ([Polars User Guide][10])

### `execute_global()` is what `pl.sql()` uses

`SQLContext.execute_global()` documents that it “automatically registers all compatible objects in the local stack that are referenced in the query,” mapping variable name → table name, and notes you should consider using `pl.sql` (which uses it internally). ([Polars User Guide][4])

**Implication:** for deterministic/secure execution, avoid “ambient registration”; prefer explicit `SQLContext.register(_many)`.

---

## 10.1.3 Compatibility boundaries + limitations

### Polars SQL is a subset (query-centric)

Polars’ SQL user guide explicitly states it does **not support the complete SQL specification**, but supports common query constructs (SELECT with WHERE/ORDER/LIMIT/GROUP BY/UNION/JOIN, CTEs, EXPLAIN, SHOW TABLES, CREATE TABLE AS, etc.). It also explicitly lists unsupported categories including `INSERT`, `UPDATE`, `DELETE`, and meta-queries like `ANALYZE`. ([Polars User Guide][1])

Polars also notes it aims to follow PostgreSQL syntax definitions and function behavior “where possible.” ([Polars User Guide][1])

### Implementation note: sqlparser-rs under the hood

Polars’ Rust SQL layer imports `sqlparser` AST types and uses `sqlparser::dialect::GenericDialect` in its context code. ([Polars User Guide][11])
Practical takeaway: parsing tends to be permissive, but **semantic support is still bounded by what Polars implements** (functions/operators/statements).

---

## 10.1.4 Security note (relevant when SQL comes from outside)

Polars SQL interfaces are string-based today; a GitHub issue explicitly calls out the absence (or lack of documentation) of parameterized queries for `pl.sql`, and warns about SQL injection risks when users interpolate strings. ([GitHub][12])
So: treat SQL input as **untrusted code**, not data—especially if you’re ingesting SQL from outside your own repo.

---

# 10.2 SQLGlot as the AST/rewriter layer

SQLGlot is designed for exactly the “SQL-aware tooling” role: it’s a no-dependency SQL **parser, transpiler, optimizer, and engine**, supporting formatting and translation across many dialects. ([GitHub][13])

## 10.2.1 Core primitives you’ll actually use

### Parse: `parse_one(sql, dialect=...)`

SQLGlot’s README explicitly warns that parse failures often happen when the **source dialect is omitted**; you can parse with `parse_one(sql, dialect="spark")` (or `read="spark"`). If no dialect is specified, it uses the “SQLGlot dialect” (a superset dialect). ([GitHub][13])

### Transpile + deterministic formatting: `sqlglot.transpile(..., write=..., identify=..., pretty=...)`

SQLGlot provides `transpile` for dialect translation, and the README shows using `identify=True` (quote identifiers) and `pretty=True` (formatted output). ([GitHub][13])

On the generator side, SQLGlot documents generator parameters like:

* `pretty` (format output)
* `identify` (quoting policy)
* `normalize` (lowercase normalization)
* plus indentation/padding and unsupported-level controls. ([SqlGlot][14])

### AST introspection: `find_all(exp.Table/Column/Select/...)`

The README shows walking the parsed AST to extract columns, projections, and tables using `find_all`. ([GitHub][13])

### AST rewrite hooks: `Expression.transform(fn)`

SQLGlot supports recursive AST transforms by applying a mapping function to each node and returning a rewritten tree. ([GitHub][13])

### Error surfaces (what you catch)

* `ParseError` (syntax errors with location + highlight) ([GitHub][13])
* `UnsupportedError` on transpilation gaps; you can force strictness with `unsupported_level=ErrorLevel.RAISE`. ([GitHub][13])

---

## 10.2.2 Dialect shaping for Polars SQL

### There is no “Polars” dialect in SQLGlot’s built-in dialect list

The SQLGlot dialect module list includes engines like Postgres, DuckDB, Spark, Snowflake, etc., but does not list Polars as a built-in dialect module. ([SqlGlot][15])
So the right posture is:

* **Pick a “source” dialect** for whatever SQL you ingest (e.g., postgres, spark, etc.)
* **Normalize to a “target” dialect** that’s compatible with Polars SQL (Polars aims to follow PostgreSQL syntax where possible). ([Polars User Guide][1])
* **Enforce a subset**: since Polars SQL is not full SQL, you must reject/transform statements Polars doesn’t support (e.g., INSERT/UPDATE/DELETE). ([Polars User Guide][1])

### If you need exact fidelity: implement a custom SQLGlot dialect

SQLGlot documents that dialects are extensible by subclassing `Dialect` and overriding tokenizer/generator behavior. ([SqlGlot][15])
This is the “best-in-class” path if you want a “PolarsSQL dialect” that:

* rejects unsupported statements at parse time,
* formats deterministically,
* rewrites functions into Polars-supported equivalents.

---

## 10.2.3 Normalization/qualification pipeline (for deterministic + enforceable SQL)

When SQL is “input”, you usually want to canonicalize it into a predictable form before execution.

### Step 1: qualify tables/columns (schema-aware)

`sqlglot.optimizer.qualify` rewrites the AST to have normalized and qualified tables and columns, and it explicitly says this step is necessary for further SQLGlot optimizations. ([SqlGlot][16])

### Step 2: normalize identifiers (case + quoting policy)

`normalize_identifiers` converts identifiers to lower/upper case while preserving semantics based on dialect rules, and it’s described as important for AST standardization. ([SqlGlot][17])

### Step 3: canonicalize (standard-form rewrites)

`canonicalize` converts an expression into a standard form and relies on type annotation; it performs type-sensitive rewrites and removes redundant casts/orderings, etc. ([SqlGlot][18])

### Step 4: optimizer (optional)

`optimizer.optimize` rewrites an AST into an optimized form, accepts schema mappings, and warns that many rules require qualification. ([SqlGlot][19])

### Step 5: deterministic SQL emit

Emit with:

* `pretty=True` for stable formatting,
* `identify=True` (or `'safe'` in generator semantics) for identifier quoting policy,
* optional `normalize=True` for consistent casing. ([SqlGlot][14])

---

## 10.2.4 Policy enforcement via AST allow-lists (the key “LLM tooling” move)

Before Polars sees a SQL string, enforce constraints in AST space:

* **Statement allow-list**: only allow `SELECT` / `WITH` / `EXPLAIN SELECT`, etc.
* **Table allow-list**: only registered tables; reject unknown tables (or rewrite them)
* **Function allow-list**: reject unknown or dangerous functions (Polars SQL has a defined supported function set on the Rust side)
* **No DML/DDL**: since Polars SQL doesn’t support INSERT/UPDATE/DELETE, reject them early. ([Polars User Guide][1])

You implement this by walking the SQLGlot AST (`find_all(exp.Table)`, `find_all(exp.Func)`, etc.). ([GitHub][13])

---

# 10.3 Canonical pattern: “SQL-free authoring, SQL-aware tooling”

### The rule

**Keep transforms expression-native.** Only introduce SQL at controlled boundaries:

* ingest external SQL (views, analyst queries, dbt snippets),
* lint/normalize/format,
* apply rewrite hooks + safety constraints,
* then execute in Polars SQL **as a compilation target**, producing a `LazyFrame` you re-enter the expression world with.

Polars explicitly supports mixing SQL and native operations by returning a frame object (`LazyFrame` by default) that you can keep transforming. ([Polars User Guide][3])

---

## 10.3.1 Pattern A: Execute external SQL against explicitly registered frames

```python
from __future__ import annotations
import polars as pl
import sqlglot
from sqlglot import exp
from sqlglot.optimizer import qualify, canonicalize
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

def execute_external_sql_polars(
    query: str,
    *,
    tables: dict[str, object],          # Polars DF/LF/Series; pandas; pyarrow
    schema: dict[str, dict[str, str]] | None = None,  # sqlglot schema mapping
    read_dialect: str = "postgres",
    write_dialect: str = "postgres",
    pretty: bool = True,
) -> pl.LazyFrame:
    # 1) parse with a known dialect (don’t rely on the “sqlglot superset” dialect by accident)
    tree = sqlglot.parse_one(query, dialect=read_dialect)

    # 2) optional schema-aware qualification + standardization
    if schema is not None:
        tree = qualify(tree, schema=schema, dialect=write_dialect)
    tree = normalize_identifiers(tree, dialect=write_dialect)
    tree = canonicalize(tree, dialect=write_dialect)

    # 3) policy enforcement (example: reject DML)
    if tree.find(exp.Insert) or tree.find(exp.Update) or tree.find(exp.Delete):
        raise ValueError("DML is not allowed for Polars SQL execution")

    # 4) deterministic emit (quote + pretty)
    sql_norm = tree.sql(dialect=write_dialect, pretty=pretty, identify=True)

    # 5) execute against explicit registry (avoid register_globals / pl.sql implicit scope)
    ctx = pl.SQLContext()
    ctx.register_many(tables)
    return ctx.execute(sql_norm, eager=False)
```

Why this matches the “safe canonical” approach:

* You avoid `pl.sql`’s implicit global registration. ([Polars User Guide][4])
* You keep execution lazy and return a `LazyFrame`, consistent with Polars SQL semantics. ([Polars User Guide][6])
* You enforce “query-only SQL” consistent with Polars SQL’s supported statement subset. ([Polars User Guide][1])

---

## 10.3.2 Pattern B: Accept *expression fragments* and keep the pipeline expression-native

If you want user-defined formulas but don’t want full SQL query ingestion:

1. Parse/validate fragment with SQLGlot
2. Emit canonical fragment SQL
3. Convert fragment to Polars `Expr` with `pl.sql_expr`
4. Apply via `with_columns/select`

Polars explicitly supports `pl.sql_expr` for fragment → Expr conversion. ([Polars User Guide][9])

---

## 10.3.3 Pattern C: “SQL-aware linting” without execution

Even if you never execute SQL in Polars, SQLGlot can still be valuable to:

* normalize SQL to a deterministic form (`pretty`, quoting policy, identifier normalization), ([SqlGlot][14])
* extract dependency graphs (tables/columns), ([GitHub][13])
* enforce a house style (dialect shaping + rewrite hooks) before SQL lands in your codebase.

---

## Operational callout: if SQL strings can be influenced externally

Polars’ SQL APIs are string-based; there’s an open issue asking for parameterized queries and explicitly warning about SQL injection risks when users interpolate strings. ([GitHub][12])
So for anything outside a trusted developer surface, prefer:

* **Polars expressions** (`Expr`) as the API,
* or SQLGlot AST → strict allow-list → deterministic emit → execute.

---

If you want the next deep dive after this: the natural continuation is an **“allowed SQL subset for Polars SQL”** specification (statement/function/operator allow-lists) generated from Polars’ Rust SQL sources, plus a **SQLGlot-based static checker** that enforces that subset before any query is executed.

[1]: https://docs.pola.rs/user-guide/sql/intro/ "Introduction - Polars user guide"
[2]: https://docs.pola.rs/api/python/dev/reference/sql/python_api.html "Python API — Polars  documentation"
[3]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.sql.html "polars.sql — Polars  documentation"
[4]: https://docs.pola.rs/api/python/dev/reference/sql/api/polars.SQLContext.execute_global.html "polars.SQLContext.execute_global — Polars  documentation"
[5]: https://docs.pola.rs/api/python/dev/reference/sql/api/polars.SQLContext.register.html "polars.SQLContext.register — Polars  documentation"
[6]: https://docs.pola.rs/api/python/dev/reference/sql/api/polars.SQLContext.execute.html "polars.SQLContext.execute — Polars  documentation"
[7]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.sql.html "polars.DataFrame.sql — Polars  documentation"
[8]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.sql.html "polars.LazyFrame.sql — Polars  documentation"
[9]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.sql_expr.html "polars.sql_expr — Polars  documentation"
[10]: https://docs.pola.rs/api/python/dev/reference/sql/api/polars.SQLContext.register_globals.html "polars.SQLContext.register_globals — Polars  documentation"
[11]: https://docs.pola.rs/api/rust/dev/src/polars_sql/context.rs.html "context.rs - source"
[12]: https://github.com/pola-rs/polars/issues/21346 "Parameterized queries for sql · Issue #21346 · pola-rs/polars · GitHub"
[13]: https://github.com/tobymao/sqlglot "GitHub - tobymao/sqlglot: Python SQL Parser and Transpiler"
[14]: https://sqlglot.com/sqlglot/dialects/tsql.html "sqlglot.dialects.tsql API documentation"
[15]: https://sqlglot.com/sqlglot/dialects.html "sqlglot.dialects API documentation"
[16]: https://sqlglot.com/sqlglot/optimizer/qualify.html "sqlglot.optimizer.qualify API documentation"
[17]: https://sqlglot.com/sqlglot/optimizer/normalize_identifiers.html "sqlglot.optimizer.normalize_identifiers API documentation"
[18]: https://sqlglot.com/sqlglot/optimizer/canonicalize.html "sqlglot.optimizer.canonicalize API documentation"
[19]: https://sqlglot.com/sqlglot/optimizer/optimizer.html "sqlglot.optimizer.optimizer API documentation"
