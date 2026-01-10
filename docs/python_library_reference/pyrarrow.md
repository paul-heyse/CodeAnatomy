I’m assuming you meant **`pyarrow`** (the Apache Arrow Python bindings). I couldn’t find a Python package named **`pyrarrow`**; if you did mean a different library, let me know and I’ll adapt. Below is a compact-but-complete field guide for an AI programming agent refactoring custom columnar/IO code to **PyArrow**.

---

# PyArrow – a practical guide for AI agents

## 0) What PyArrow is (and isn’t)

**PyArrow** exposes the C++ Apache Arrow project to Python. Arrow is a **language‑agnostic, in‑memory, columnar format** + a toolbox for fast IO, compute, and data interchange. In PyArrow, you work with:

* **Arrays** (typed, contiguous vectors), **RecordBatches** (columnar batches with a schema), and **Tables** (columnar, potentially chunked collections of columns).
* Reader/writers for **Parquet, Arrow IPC/Feather, CSV, JSON, ORC**, and **Datasets** that scale across many files/partitions.
* A vectorized **compute** module (`pyarrow.compute`) and the **Acero** streaming execution engine for planning multi‑operator pipelines.
* File systems for **local, S3, GCS, Azure, Hadoop**, and integration with **pandas** types and IO. ([Apache Arrow][1])

As of Arrow **v21**, PyArrow supports Python **3.9–3.13** and is distributed via pip/conda; on conda there are split packages (`pyarrow-core`, `pyarrow`, `pyarrow-all`) to tailor footprint and features. ([Apache Arrow][2])

---

## 1) Install, versions, and package “shapes”

* **pip**: `pip install pyarrow`
* **conda-forge**:

  * `pyarrow-core`: core arrays/compute/IPC/filesystems (+ CSV/JSON/ORC, **not** Parquet)
  * `pyarrow`: adds **datasets**, **Parquet**, **Acero**, **Substrait**
  * `pyarrow-all`: adds **Flight/Flight SQL** and **Gandiva** (vectorized expr compiler)
    You can mix‑and‑match components (e.g., `pyarrow-core libparquet`). ([Apache Arrow][2])

> Tip (Windows timezones): ORC and timezone handling may require `tzdata` and `TZDIR`—documented in the install guide. ([Apache Arrow][2])

---

## 2) Core mental model (Arrays → Batches → Tables)

* **Arrays** are typed vectors (`pa.array([...], type=...)`).
* **RecordBatch** = one schema + same-length columns.
* **Table** = possibly chunked columns; think “batch collection with a schema”.
* Convert to/from pandas with `Table.to_pandas()` / `Table.from_pandas()`; be aware conversion can copy. ([Apache Arrow][3])

### Minimal example

```python
import pyarrow as pa
arr = pa.array([1, 2, None, 4], type=pa.int64())
batch = pa.record_batch([arr], names=["x"])
table = pa.Table.from_batches([batch])
```

---

## 3) IO building blocks

### Parquet

* Read/write single files: `pyarrow.parquet.read_table`, `pyarrow.parquet.write_table`. Control **compression**, **compression_level**, projected `columns`, and **filters** (predicate pushdown). ([Apache Arrow][4])
* Datasets (many files/partitions): use `pyarrow.dataset` for scalable scans with projection + filtering. ([Apache Arrow][5])

**Encrypted Parquet** is supported (modular columnar encryption). Configure via `pyarrow.parquet.encryption.*` (e.g., `EncryptionConfiguration`, `CryptoFactory`). ([Apache Arrow][6])

### Arrow IPC / Feather

* **Streaming**: `pa.ipc.new_stream` / `pa.ipc.open_stream` for back‑pressure-friendly pipelines.
* **File** (random access): `pa.ipc.new_file` / `pa.ipc.open_file`. ([Apache Arrow][7])

### CSV / JSON / ORC

* CSV: `pyarrow.csv.read_csv/open_csv` with `ReadOptions/ParseOptions/ConvertOptions`.
* JSON: `pyarrow.json.read_json` (newline-delimited JSON).
* ORC: `pyarrow.orc.read_table`.
  (All listed in the docs’ tabular formats section.) ([Apache Arrow][2])

---

## 4) Filesystems and URIs

Use `pyarrow.fs` to read/write anywhere:

```python
import pyarrow.fs as fs
s3 = fs.S3FileSystem(region="us-east-1")  # credentials auto-discovered if env configured
with s3.open_output_stream("s3://my-bucket/path/file.parquet") as sink:
    ...
```

`ds.dataset("s3://bucket/prefix", format="parquet", filesystem=s3)` then scans with predicate/projection pushdown. S3 credentials and discovery are handled per AWS conventions (env vars, profiles, IMDS). GCS/Azure/HDFS also supported. ([Apache Arrow][8])

> Also available: `PyFileSystem(FSSpecHandler(...))` to mount an fsspec filesystem behind Arrow’s FS API. ([Apache Arrow][2])

---

## 5) Datasets (large-scale table-of-files)

Datasets unify table‑like access over directories of files; they add partition discovery, pushdown, and writing.

```python
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow as pa

dataset = ds.dataset("s3://datalake/events/", format="parquet", filesystem=s3)
# Filter + project are pushed to file readers, then parallelized
scanner = dataset.scanner(
    filter=(ds.field("country") == "US") & (ds.field("ts") >= pa.scalar("2024-01-01")),
    columns=["user_id", "ts", "country"],
    use_threads=True
)
table = scanner.to_table()
```

Key APIs: `Dataset`, `Scanner`, `Expression`, `write_dataset`, and partitioning (Hive/Directory/Filename). You can **join** datasets and do **as‑of joins** (v21). For writing, set partitioning and row‑group sizing. ([Apache Arrow][5])

> Example hive partitioning schema: `year=2009/month=11/day=15` in directory names. ([Apache Arrow][9])

**Write example (partition + compression):**

```python
import pyarrow.dataset as ds
import pyarrow as pa

ds.write_dataset(
    table,
    base_dir="s3://lake/out/events",
    format="parquet",
    partitioning=ds.partitioning(pa.schema([pa.field("country", pa.string())]), flavor="hive"),
    file_options=ds.ParquetFileWriteOptions(compression="zstd")
)
```

(You can also pass compression via Parquet writer options.) ([Apache Arrow][4])

---

## 6) Compute & transformations

Vectorized kernels live in `pyarrow.compute` (aka **pc**). Use expressions in datasets or transform `Table`s/`Array`s directly.

* **Elementwise**: `pc.add`, `pc.equal`, `pc.if_else`, `pc.strptime`, `pc.match_substring`…
* **Aggregations**: `Table.group_by(...).aggregate(...)` for grouped reductions; non‑grouped reductions via `pc.sum/mean/...`.
* **Experimental UDFs** (register custom compute functions). ([Apache Arrow][10])

Example:

```python
import pyarrow.compute as pc
t2 = table.append_column("day", pc.strftime(table["ts"], format="%Y-%m-%d"))
out = t2.group_by("day").aggregate([("user_id", "count_distinct")])
```

For multi-operator streaming plans, use **Acero** (`pyarrow.acero.Declaration`) to build and run exec plans (filter → project → aggregate), keeping data batched. API is still labeled experimental. ([Apache Arrow][11])

---

## 7) Interop with pandas (and others)

* **pandas ↔ Arrow**:

  * `Table.to_pandas()` / `Table.from_pandas()` for whole-table conversion.
  * In pandas ≥ 2.x, you can **store columns as Arrow‑backed dtypes** (e.g., `"string[pyarrow]"`, `"int64[pyarrow]"`) and use Arrow IO engines; `types_mapper=pd.ArrowDtype` keeps Arrow semantics on conversion. This can reduce copies for many types and improve IO/NA semantics. ([Pandas][12])

* Other dataframes (Polars, cuDF, DuckDB) speak Arrow; e.g., DuckDB can run SQL “on Arrow” directly. ([DuckDB][13])

---

## 8) Streaming and RPC

* **Arrow IPC streaming** for process‑to‑process pipes (batches flow until a 0‑marker on close):

  ```python
  import pyarrow as pa, pyarrow.ipc as ipc
  sink = pa.BufferOutputStream()
  with ipc.new_stream(sink, table.schema) as w:
      for b in table.to_batches(max_chunksize=1_000_000):
          w.write_batch(b)
  buf = sink.getvalue()
  reader = ipc.open_stream(buf)
  streamed = reader.read_all()
  ```

  IPC APIs: `new_stream/open_stream`, `new_file/open_file`, `RecordBatchStreamWriter/Reader`. ([Apache Arrow][7])

* **Arrow Flight** (and **Flight SQL**): high‑throughput gRPC RPC for Arrow streams / SQL access. APIs exist in `pyarrow.flight` for client/server, but the API is flagged as unstable (still evolving). Useful when your custom code implemented ad‑hoc RPC or bespoke framing. ([Apache Arrow][14])

---

## 9) Performance levers for agents

* **Predicate + projection pushdown**: always pass `columns=` and `filter=` to Parquet/Dataset readers; let Arrow skip IO. ([Apache Arrow][15])
* **Threads**: most scans/computations are multithreaded; set `use_threads=True`. You can also tweak global counts: `pa.set_cpu_count(...)`, `pa.set_io_thread_count(...)`. ([Apache Arrow][2])
* **Row group sizing** when writing Parquet (`min_rows_per_group`/`max_rows_per_group`) to balance parallelism & seek overhead. ([Apache Arrow][16])
* **Dictionary encoding** (including `read_dictionary=` for selected columns) can significantly cut memory for repeated strings. ([Apache Arrow][6])
* **Memory pools**: choose allocator backends (system, jemalloc, mimalloc) and set the default pool; enable allocation logging for debugging.

  ```python
  import pyarrow as pa
  pa.set_memory_pool(pa.jemalloc_memory_pool())
  pa.log_memory_allocations(True)
  ```

  (Availability depends on how PyArrow was built.) ([Apache Arrow][17])

---

## 10) Security and deprecations (important for refactors)

* **CVE‑2023‑47248**: unsafe deserialization in PyArrow IPC/Parquet readers (versions **0.14.0–14.0.0**). **Upgrade** to ≥ **14.0.1** or apply `pyarrow-hotfix` if you can’t upgrade. If you ingest untrusted Arrow/Parquet/Feather, treat files as potentially dangerous on vulnerable versions. ([NVD][18])
* **Plasma object store** and the old **custom serialization** APIs are **removed/deprecated**. Replace with Arrow IPC, standard `pickle`, or Flight as appropriate. ([Apache Arrow][19])

---

## 11) Common “custom code” → PyArrow migrations

| If your custom code …                            | Use in PyArrow                                                                                                         |
| ------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------- |
| Hand‑rolled column store in NumPy & Python lists | `pa.array`, `pa.Table`, `pa.RecordBatch`; compute via `pyarrow.compute` and group via `Table.group_by`                 |
| Manual CSV/JSON parsers + pandas                 | `pyarrow.csv.read_csv` / `pyarrow.json.read_json` → Arrow Tables; convert to pandas only at edges                      |
| Ad‑hoc S3 file walkers and globbing              | `pyarrow.dataset.dataset("s3://…", format="parquet", filesystem=S3FileSystem)` and scan with filter/projection         |
| Self‑managed partitioning                        | `ds.write_dataset(..., partitioning=ds.partitioning(..., flavor="hive"))` and read with the corresponding partitioning |
| Homemade Parquet metadata/indexing               | `pyarrow.parquet.read_table(..., columns=..., filters=...)` and dataset scanning; rely on row‑group stats for skipping |
| Serialized batches over sockets                  | Arrow **IPC streaming** (`ipc.new_stream/open_stream`)                                                                 |
| Custom RPC for tabular chunks                    | **Arrow Flight** client/server (or Flight SQL) if you need networked transport at scale                                |

(See the linked docs for the exact APIs and options.) ([Apache Arrow][20])

---

## 12) End‑to‑end patterns for an agent

### A) Filter → Aggregate → Materialize small result to pandas

```python
import pyarrow as pa, pyarrow.dataset as ds, pyarrow.compute as pc

ds_events = ds.dataset("s3://lake/events/", format="parquet", filesystem=s3)
scanner = ds_events.scanner(
    filter=(ds.field("event") == "purchase") & (ds.field("ts") >= pa.scalar("2025-01-01")),
    columns=["user_id", "amount", "ts"],
)
t = scanner.to_table()
daily = (t
         .append_column("day", pc.strftime(t["ts"], format="%Y-%m-%d"))
         .group_by("day")
         .aggregate([("amount", "sum"), ("user_id", "count_distinct")]))
df = daily.to_pandas()  # small result → pandas for plotting/reporting
```

Pushdown avoids reading irrelevant files/columns; groupby runs vectorized. ([Apache Arrow][20])

### B) Stream Arrow over the wire (no big pandas frames)

```python
# writer side
import pyarrow as pa, pyarrow.ipc as ipc, socket
sock = socket.create_connection(("127.0.0.1", 7777))
with ipc.new_stream(sock.makefile("wb"), table.schema) as w:
    for b in table.to_batches(max_chunksize=262_144):
        w.write_batch(b)

# reader side
r = ipc.open_stream(sock.makefile("rb"))
for batch in r:
    process(batch)  # keeps peak memory bounded
```

IPC streaming primitives are designed for sustained pipelines. ([Apache Arrow][21])

### C) Write a partitioned dataset with compression tuned

```python
ds.write_dataset(
    table,
    base_dir="s3://analytics/out/sales",
    format="parquet",
    partitioning=ds.partitioning(pa.schema([("year", pa.int16()), ("month", pa.int8())]), flavor="hive"),
    file_options=ds.ParquetFileWriteOptions(compression="zstd")
)
```

This layout enables skipping and parallel reads later. ([Apache Arrow][22])

---

## 13) Operational knobs & environment

* **Global threads**: `pa.set_cpu_count`, `pa.set_io_thread_count`. Useful to bound CPU/IO in constrained agent runtimes. ([Apache Arrow][2])
* **Memory pools**: choose jemalloc/mimalloc/system; `pa.log_memory_allocations(True)` for debugging. On some builds, jemalloc APIs may not be available. ([Apache Arrow][23])
* **S3 initialization**: Arrow detects credentials from env/IMDS; advanced S3 options exist on `S3FileSystem`. ([Apache Arrow][8])

---

## 14) Type checking & stubs

For static typing with MyPy/pyright, community **`pyarrow-stubs`** packages exist; there has been discussion about migrating stubs under the Arrow project umbrella. (If you rely on typings heavily, check the current status.) ([PyPI][24])

---

## 15) Footguns and cautions

* **Converting to pandas** can materialize copy-sized buffers; keep Arrow as long as possible and convert only the final result. Use Arrow‑backed dtypes where helpful. ([Pandas][12])
* Mind **row‑group** sizes when writing Parquet; too small → many tiny reads; too big → skew. Tune `min/max_rows_per_group`. ([Apache Arrow][16])
* **Flight** APIs are marked unstable; pin compatible versions if embedding Flight clients/servers. ([Apache Arrow][14])
* **Old APIs removed**: Plasma & custom serialization; don’t depend on them in new code. ([Apache Arrow][19])
* **Security**: never open untrusted Arrow/Parquet files on vulnerable versions (≤ 14.0.0). Prefer ≥ 14.0.1 or apply the hotfix when upgrading isn’t possible. ([NVD][18])

---

## 16) Quick API map (from custom code)

* **Column vectors** → `pa.array`, `pa.chunked_array`, `pa.field`, `pa.schema`
* **Tabular batches** → `pa.record_batch`, `pa.Table`
* **Filters** → `pyarrow.compute` expressions (`pc.equal`, `pc.is_in`, etc.) and `ds.field("col")` in dataset filters
* **Aggregations** → `Table.group_by(...).aggregate([...])`
* **Read/Write** → `pyarrow.parquet.*`, `pyarrow.csv.*`, `pyarrow.json.*`, `pyarrow.orc.*`, `pyarrow.feather.*`
* **Datasets** → `ds.dataset`, `scanner.to_table()`, `ds.write_dataset`
* **Streaming** → `pyarrow.ipc` (streams/files)
* **RPC** → `pyarrow.flight` / Flight SQL
* **Filesystems** → `pyarrow.fs` (S3/GCS/Azure/HDFS/local/fsspec)
* **Performance** → pushdown, threads, dictionary encoding, row groups, memory pools

---

### References to keep handy

* PyArrow docs home, install guide and API index. ([Apache Arrow][1])
* Datasets/Scanner & partitioning. ([Apache Arrow][5])
* Parquet read/write and encryption. ([Apache Arrow][25])
* Filesystems (S3/GCS/Azure/HDFS). ([Apache Arrow][8])
* Compute, groupby, Acero. ([Apache Arrow][10])
* IPC Streaming. ([Apache Arrow][7])
* pandas + Arrow dtypes. ([Pandas][12])
* Security advisory and hotfix. ([NVD][18])

---

If you meant a different library than **PyArrow**, tell me which one and I’ll tailor the guide. Otherwise, if you share a sketch of your custom code (IO/storage/compute pieces), I can map each part to the most idiomatic PyArrow calls in a side‑by‑side diff.

[1]: https://arrow.apache.org/docs/python/index.html?utm_source=chatgpt.com "Python — Apache Arrow v21.0.0"
[2]: https://arrow.apache.org/docs/python/install.html "Installing PyArrow — Apache Arrow v21.0.0"
[3]: https://arrow.apache.org/docs/python/getstarted.html?utm_source=chatgpt.com "Getting Started — Apache Arrow v21.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html?utm_source=chatgpt.com "pyarrow.parquet.write_table — Apache Arrow v21.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html?utm_source=chatgpt.com "pyarrow.dataset.Dataset — Apache Arrow v21.0.0"
[6]: https://arrow.apache.org/docs/python/parquet.html?utm_source=chatgpt.com "Reading and Writing the Apache Parquet Format"
[7]: https://arrow.apache.org/docs/python/ipc.html?utm_source=chatgpt.com "Streaming, Serialization, and IPC — Apache Arrow v21.0.0"
[8]: https://arrow.apache.org/docs/python/filesystems.html?utm_source=chatgpt.com "Filesystem Interface — Apache Arrow v21.0.0"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.HivePartitioning.html?utm_source=chatgpt.com "pyarrow.dataset.HivePartitioning — Apache Arrow v21.0.0"
[10]: https://arrow.apache.org/docs/python/compute.html?utm_source=chatgpt.com "Compute Functions — Apache Arrow v21.0.0"
[11]: https://arrow.apache.org/docs/python/api/acero.html?utm_source=chatgpt.com "Acero - Streaming Execution Engine — Apache Arrow v21.0.0"
[12]: https://pandas.pydata.org/docs/user_guide/pyarrow.html?utm_source=chatgpt.com "PyArrow Functionality — pandas 2.3.3 documentation - PyData |"
[13]: https://duckdb.org/docs/stable/guides/python/sql_on_arrow.html?utm_source=chatgpt.com "SQL on Apache Arrow"
[14]: https://arrow.apache.org/docs/python/api/flight.html?utm_source=chatgpt.com "Arrow Flight — Apache Arrow v21.0.0"
[15]: https://arrow.apache.org/cookbook/py/io.html?utm_source=chatgpt.com "Reading and Writing Data"
[16]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html?utm_source=chatgpt.com "pyarrow.dataset.write_dataset — Apache Arrow v21.0.0"
[17]: https://arrow.apache.org/docs/python/generated/pyarrow.set_memory_pool.html?utm_source=chatgpt.com "pyarrow.set_memory_pool — Apache Arrow v21.0.0"
[18]: https://nvd.nist.gov/vuln/detail/cve-2023-47248?utm_source=chatgpt.com "CVE-2023-47248 Detail - NVD"
[19]: https://arrow.apache.org/blog/2023/05/02/12.0.0-release/?utm_source=chatgpt.com "Apache Arrow 12.0.0 Release"
[20]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html?utm_source=chatgpt.com "pyarrow.dataset.Scanner — Apache Arrow v21.0.0"
[21]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.open_stream.html?utm_source=chatgpt.com "pyarrow.ipc.open_stream — Apache Arrow v21.0.0"
[22]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.partitioning.html?utm_source=chatgpt.com "pyarrow.dataset.partitioning — Apache Arrow v21.0.0"
[23]: https://arrow.apache.org/docs/python/generated/pyarrow.jemalloc_memory_pool.html?utm_source=chatgpt.com "pyarrow.jemalloc_memory_pool — Apache Arrow v21.0.0"
[24]: https://pypi.org/project/pyarrow-stubs/?utm_source=chatgpt.com "pyarrow-stubs"
[25]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html?utm_source=chatgpt.com "pyarrow.parquet.read_table — Apache Arrow v21.0.0"
