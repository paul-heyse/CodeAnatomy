# `pyarrow-execution-advanced.md` — Arrow as the physical batch substrate (datasets, Acero, compute kernels, IPC, Flight, and C interfaces)

> Version note: the API details below are aligned with the **Apache Arrow / PyArrow v22.0.0 docs**, but most concepts are stable across recent releases. (You should still pin and record `pyarrow.__version__` in your build artifacts.) ([Apache Arrow][1])

---

## 0) Scope and invariants

### 0.1 Arrow as the “batch ABI” across DataFusion / Ibis / Polars / pandas

Arrow’s interoperability story is built around **stable C-level interchange** (C Data, C Stream, C Device interfaces) plus a **Python-facing PyCapsule protocol** that standardizes how Python libraries expose these C structs. ([Apache Arrow][2])

Key point for your architecture: you can treat Arrow batches as the “wire format” *within a single process* (and often across processes via IPC/Flight) without forcing everything to “be PyArrow objects” first. The PyCapsule interface exists specifically so libraries can exchange Arrow memory safely without requiring PyArrow, and it encodes “single-consumption” semantics and destructor behavior to avoid leaks. ([Apache Arrow][3])

### 0.2 Determinism and ordering tiers

You want to explicitly tier ordering guarantees:

**Tier A — “Implicit / scan-stable ordering” (cheap)**

* In Acero scans, you can preserve implicit ordering and even annotate batches with fragment/batch indices to support stable ordering in simple plans via `implicit_ordering=True`. ([Apache Arrow][4])
* You can also force sequential batch yielding (“single-thread-like”) with `require_sequenced_output=True`. ([Apache Arrow][4])

**Tier B — “Writer-preserved ordering” (expensive)**

* `pyarrow.dataset.write_dataset(..., preserve_order=True)` guarantees row order even when `use_threads=True`, but the docs warn it “may cause notable performance degradation.” ([Apache Arrow][5])

**Tier C — “Global canonical order” (most expensive, but unambiguous)**

* Acero’s `OrderByNodeOptions` explicitly accumulates all data before sorting, and larger-than-memory sort is not supported (so treat this as a *materialization boundary*). ([Apache Arrow][6])

### 0.3 Memory + concurrency budgets are global and must be owned centrally

Arrow has:

* a **CPU thread pool** controlled by `pyarrow.cpu_count()` / `pyarrow.set_cpu_count(count)` ([Apache Arrow][7])
* an **IO thread pool** controlled by `pyarrow.io_thread_count()` / `pyarrow.set_io_thread_count(count)`; dataset scanning uses this pool implicitly ([Apache Arrow][8])
* a process-global **default memory pool** (`pyarrow.default_memory_pool()`) and accounting helpers like `pyarrow.total_allocated_bytes()` (default pool only) ([Apache Arrow][9])

In a best-in-class single-machine engine, you pin and record:

* Arrow CPU pool size
* Arrow IO pool size
* DataFusion partition parallelism (so you don’t oversubscribe cores)
* memory pool backend and peak stats

---

## 1) Arrow interchange protocols (PyCapsule / C Data / C Stream / device)

### 1.1 Protocol methods you should treat as “ingress contracts”

The Arrow PyCapsule interface defines standardized Python methods that export Arrow data to other libraries:

CPU-only protocols:

* `__arrow_c_schema__()` → PyCapsule named `"arrow_schema"`
* `__arrow_c_array__(requested_schema=None)` → pair of PyCapsules (`"arrow_schema"`, `"arrow_array"`)
* `__arrow_c_stream__(requested_schema=None)` → PyCapsule named `"arrow_array_stream"` ([Apache Arrow][3])

Device-aware protocols (for GPU / non-CPU memory):

* `__arrow_c_device_array__(requested_schema=None, **kwargs)` → pair (`"arrow_schema"`, `"arrow_device_array"`)
* `__arrow_c_device_stream__(requested_schema=None, **kwargs)` → `"arrow_device_array_stream"` ([Apache Arrow][3])

The C Stream interface itself is a pull-style interface that yields a stream of chunks (same schema each chunk). ([Apache Arrow][10])
The C Device interface extends the C Data/Stream interfaces by adding device type and synchronization information. ([Apache Arrow][11])

### 1.2 Requested schema negotiation (don’t overuse it)

Both `__arrow_c_array__` and `__arrow_c_stream__` accept an optional `requested_schema` capsule. This is **best-effort** and intended for negotiating among compatible representations (e.g., string offsets width, dictionary encoding), not arbitrary schema reshaping. If the requested schema is incompatible (e.g., different number of fields), the exporter should raise. ([Apache Arrow][3])

### 1.3 Ownership and lifetime rules (the “who frees memory” contract)

This is the part most systems get wrong if they don’t codify it:

* Exported PyCapsules **must** have a destructor that calls the Arrow struct’s `release` callback if it hasn’t been consumed, to prevent leaks. ([Apache Arrow][3])
* Consumers must **move** the data and set the release callback to null to avoid double-free (same as the C Data interface “single consumption” model). ([Apache Arrow][3])
* PyCapsules defined by this protocol are **single-consumption**. ([Apache Arrow][3])

### 1.4 “Accept Arrow-like objects” patterns you can standardize

PyArrow constructors already accept Arrow-like objects that implement PyCapsule protocols:

* `pyarrow.table(data=...)` accepts tabular objects that implement `__arrow_c_array__`, `__arrow_c_device_array__`, or `__arrow_c_stream__`. ([Apache Arrow][12])

Dataset scanning also accepts PyCapsule streams in `Scanner.from_batches`:

* `Scanner.from_batches(source, ...)` accepts a `RecordBatchReader`, any object that implements the Arrow PyCapsule Protocol for streams, or a Python iterator of `RecordBatch`. ([Apache Arrow][13])

---

## 2) Dataset module deep dive (scan path)

### 2.1 `pyarrow.dataset.dataset(...)`: the high-level dataset factory

Signature (v22):
`dataset(source, schema=None, format=None, filesystem=None, partitioning=None, partition_base_dir=None, exclude_invalid_files=None, ignore_prefixes=None)` ([Apache Arrow][14])

The docs describe the high-level dataset API as providing:

* a unified interface across sources (Parquet, Feather/IPC, etc.)
* discovery (crawling directories, partitioned datasets, basic schema normalization)
* optimized reading with predicate pushdown, projection, parallel reading, and task-level control ([Apache Arrow][14])

When you need even more control (explicit fragments/factories), you drop to lower-level dataset classes (FileSystemDataset, factories). ([Apache Arrow][14])

### 2.2 Filesystem abstraction (`pyarrow.fs`) and URI handling

PyArrow’s filesystem interface presents abstract `/`-separated paths, dereferences symlinks, and exposes only basic file metadata (size, mtime, etc.). ([Apache Arrow][15])

For S3-like storage, you commonly use `pyarrow.fs.S3FileSystem.from_uri(uri)`; the docs list recognized URI schemes including `file`, `s3fs`, `gs/gcs`, `hdfs`, etc. ([Apache Arrow][16])

### 2.3 Partitioning discovery and schemes

You’ll mostly care about these built-in schemes:

**HivePartitioning**

* directory-based, leaf directories contain data files
* partition keys appear as `key=value` segments; order is ignored; missing/unrecognized fields ignored ([Apache Arrow][17])

**DirectoryPartitioning**

* expects one path segment per field in the partitioning schema (all required) ([Apache Arrow][18])

### 2.4 Schema unification

Datasets expose a common schema, and fragments can be scanned with a unifying schema to align differing “physical schemas.” Fragment scanning APIs explicitly take an optional schema to unify fragment reads. ([Apache Arrow][19])

### 2.5 Scanner control plane (the knobs you must standardize)

You typically create a scanner from the dataset:

* `Dataset.scanner(...)` (high-level)
* or `Scanner.from_dataset(dataset, ...)` (explicit static constructor) ([Apache Arrow][13])

The scanner API supports a *very complete* control plane:

**Projection**

* `columns` can be:

  * a list of column names (order preserved, duplicates preserved), or
  * a dict `{new_col: expression}` for derived columns
* You can refer to special metadata fields: `__batch_index`, `__fragment_index`, `__last_in_fragment`, `__filename` ([Apache Arrow][13])

**Filtering**

* `filter=Expression`
* Pushdown behavior: if possible, predicates are pushed down using partition info or internal metadata like Parquet statistics; otherwise batches are filtered after loading. ([Apache Arrow][13])

**Batch sizing and read-ahead**

* `batch_size`: maximum row count per output batch (reduce when batches overflow memory)
* `batch_readahead`: batches to read ahead within a file (RAM vs IO utilization)
* `fragment_readahead`: files to read ahead (RAM vs IO utilization) ([Apache Arrow][13])

**Parallelism**

* `use_threads=True`: uses maximum parallelism determined by available CPU cores ([Apache Arrow][13])
* note: dataset scanning also relies on Arrow’s IO thread pool; set via `set_io_thread_count` (see §6). ([Apache Arrow][20])

**Metadata caching**

* `cache_metadata=True` can cache metadata to speed up repeated scans ([Apache Arrow][13])

**Consumption surfaces**

* `to_reader()` → `RecordBatchReader` ([Apache Arrow][13])
* `to_batches()` → iterator of `RecordBatch` ([Apache Arrow][13])
* `scan_batches()` → iterator of `TaggedRecordBatch` (batch + fragment context) ([Apache Arrow][13])
* `to_table()` materializes into a `Table` ([Apache Arrow][13])

### 2.6 Fragment-level pruning and control

Datasets are collections of fragments; you can:

* enumerate fragments with `Dataset.get_fragments(filter=None)` ([Apache Arrow][21])
* apply dataset-level `filter(...)` to narrow the dataset ([Apache Arrow][21])
* inspect a fragment’s `partition_expression` (always-true expression for the fragment) and `physical_schema` ([Apache Arrow][22])

This becomes your “two-stage pruning” pattern:

1. Use partitioning/metadata filters to narrow fragments.
2. Scan with pushdown filters + projection.

### 2.7 Dataset-to-engine handoff patterns

Your standard handoff objects should be:

* `RecordBatchReader` (streaming batches)
* `pyarrow.Table` (materialized)
* `Arrow C Stream` capsule (interop boundary) ([Apache Arrow][10])

One particularly relevant bridge: `Scanner.from_batches` exists specifically for “sources that can be read only once,” and it accepts `RecordBatchReader` or other Arrow PyCapsule streams. ([Apache Arrow][13])

---

## 3) Streaming dataset writes (materialization path)

### 3.1 `pyarrow.dataset.write_dataset(...)` is the core primitive

Signature (v22):
`write_dataset(data, base_dir, ..., format=..., partitioning=..., schema=..., filesystem=..., file_options=..., use_threads=True, preserve_order=False, ..., file_visitor=..., existing_data_behavior='error', create_dir=True)` ([Apache Arrow][5])

Accepted inputs include:

* `Dataset`, `Table`/`RecordBatch`, `RecordBatchReader`, list of tables/batches, or **iterable of RecordBatch** (schema required for iterables). ([Apache Arrow][5])

### 3.2 Streaming inputs: `RecordBatchReader` and Python iterables

Two canonical patterns:

**(A) RecordBatchReader input**

```python
import pyarrow.dataset as ds

ds.write_dataset(reader, base_dir="out", format="parquet")
```

This is explicitly supported. ([Apache Arrow][5])

**(B) Iterable[RecordBatch] input (schema required)**

```python
import pyarrow as pa
import pyarrow.dataset as ds

def batches():
    yield pa.record_batch({"a": [1, 2, 3]})
    yield pa.record_batch({"a": [4, 5]})

schema = pa.schema([("a", pa.int64())])
ds.write_dataset(batches(), base_dir="out", format="parquet", schema=schema)
```

The schema requirement for iterables is explicit. ([Apache Arrow][5])

### 3.3 Concurrency knobs and file sizing controls

Key parameters you should treat as “writer policy”:

* `use_threads=True`: writes files in parallel; **may change row order** if `preserve_order=False`. ([Apache Arrow][5])
* `preserve_order=True`: guarantees order even with threads, but may degrade performance. ([Apache Arrow][5])
* `max_open_files`: caps open file handles; too low can fragment data into many small files (explicitly warned). ([Apache Arrow][5])
* `max_rows_per_file`: caps rows per file; else one file per output directory (subject to max_open_files). ([Apache Arrow][5])
* `min_rows_per_group` / `max_rows_per_group`: row group batching/splitting; docs advise setting both to avoid tiny row groups. ([Apache Arrow][5])
* `basename_template`: default is `part-{i}.<ext>`; `i` increments. ([Apache Arrow][5])

### 3.4 Ordering semantics and known pitfalls

The official contract is:

* With threads on and `preserve_order=False`, row order may change. ([Apache Arrow][5])
* With `preserve_order=True`, order is guaranteed (but slower). ([Apache Arrow][5])

In practice, users have reported that default writes can reorder rows, reinforcing that you must treat ordering as explicit policy (not implicit behavior). ([GitHub][23])

### 3.5 `file_visitor`: build metadata sidecars and `_metadata`

If you set `file_visitor`, it is called with a `WrittenFile` for each written file, exposing:

* `path`
* `metadata` (Parquet metadata; None for non-parquet formats)
  The docs explicitly mention using this metadata to build a `_metadata` file. ([Apache Arrow][5])

### 3.6 Existing data behaviors (append vs overwrite vs partition overwrite)

`existing_data_behavior`:

* `'error'`: default; error if any data exists
* `'overwrite_or_ignore'`: overwrites files with matching names, ignores other existing files; combined with unique `basename_template` enables “append-like” workflows
* `'delete_matching'`: on first encounter of each partition directory, delete it entirely (partition overwrite) ([Apache Arrow][5])

### 3.7 `pyarrow.parquet.write_to_dataset`: convenience wrapper

`pyarrow.parquet.write_to_dataset(table, root_path, partition_cols=..., ...)` is a wrapper around `dataset.write_dataset` for writing partitioned Parquet datasets. ([Apache Arrow][24])

---

## 4) Acero (ExecPlan) for streaming compute

### 4.1 Why Acero vs compute-kernel chains

Acero is a streaming execution engine: you build a graph of `Declaration` objects (each becomes an ExecNode), then execute to collect results (to table/reader). This is the “avoid full materialization” path for pipelines that would otherwise bounce between compute calls and Python loops. ([Apache Arrow][25])

### 4.2 Python API surface

Core objects:

* `pyarrow.acero.Declaration(factory_name, options, inputs=None)` represents an unconstructed node in a plan graph. ([Apache Arrow][26])
* `Declaration.to_table(use_threads=True)` executes the plan by implicitly adding a sink and blocking until completion. ([Apache Arrow][26])
* Node option classes: `ScanNodeOptions`, `FilterNodeOptions`, `ProjectNodeOptions`, `AggregateNodeOptions`, `OrderByNodeOptions`, `HashJoinNodeOptions` are part of the public API. ([Apache Arrow][27])

### 4.3 Scan node specifics: `ScanNodeOptions`

Scan node key behaviors:

* It can apply pushdown projections or filters to file readers (if supported), but it does **not** create separate filter/project nodes; you supply the same expressions to both scan and filter/project nodes for full semantics. ([Apache Arrow][4])
* `implicit_ordering=True` augments batches with fragment/batch indices to enable stable ordering for simple ExecPlans. ([Apache Arrow][4])
* `require_sequenced_output=True` yields batches sequentially (single-thread-like). ([Apache Arrow][4])

`ScanNodeOptions(**kwargs)` accepts scanner-style arguments via `Scanner.from_dataset` (projection/filter/batch sizing/etc). ([Apache Arrow][4])

### 4.4 Common node patterns you’ll use

#### 4.4.1 Scan → Filter → Project

```python
import pyarrow.dataset as ds
import pyarrow.acero as acero

dataset = ds.dataset("data/", format="parquet")

scan = acero.Declaration(
    "scan",
    acero.ScanNodeOptions(dataset, columns=["a", "b"], filter=ds.field("a") > 0),
)

filt = acero.Declaration("filter", acero.FilterNodeOptions(ds.field("b") < 10), [scan])

proj = acero.Declaration(
    "project",
    acero.ProjectNodeOptions([ds.field("a"), (ds.field("b") + 1).alias("b1")]),
    [filt],
)

out = proj.to_table(use_threads=True)
```

Scan pushdown + explicit filter/project nodes is the intended pattern. ([Apache Arrow][4])

#### 4.4.2 Aggregate (grouped)

Use `AggregateNodeOptions(aggregates, keys=...)` (see `pyarrow.acero` API). ([Apache Arrow][27])

#### 4.4.3 Hash join

`HashJoinNodeOptions(join_type, left_keys, right_keys, ...)` provides hash-join semantics in Acero. ([Apache Arrow][28])

#### 4.4.4 Order-by boundary (explicit materialization)

Acero’s order_by node is **not streaming**: it accumulates all data, sorts, then emits data with updated batch index; larger-than-memory sort is not supported. ([Apache Arrow][6])
Treat this as a deliberate boundary in your pipeline.

### 4.5 Substrait ↔ Acero constraints

PyArrow’s Substrait integration is currently centered on:

* serializing/deserializing Arrow compute expressions (must be schema-bound) ([Apache Arrow][29])
* executing Substrait queries via `pyarrow.substrait.run_query(...)`, which returns a `RecordBatchReader` and supports `use_threads`. ([Apache Arrow][30])
* introspecting supported functions via `get_supported_functions()` returning `{uri}#{name}` identifiers. ([Apache Arrow][31])

For your system: Substrait is most useful as a **portable artifact** (expression bundles + supported function catalog) and as a potential “query plan interchange” experiment lane, not as your primary day-to-day plan representation.

---

## 5) Compute kernels: “never write Python loops” primitives

### 5.0 Compute module mental model + introspection

PyArrow compute provides vectorized operations through `pyarrow.compute`. ([Apache Arrow][32])

Two advanced capabilities you should standardize in tooling:

* **Enumerate kernel names** using the function registry (`list_functions`) and fetch by name (`get_function` / `call_function`). This is how you build an LLM-friendly “kernel catalog” dynamically. ([Apache Arrow][33])
* Recognize that some grouped “hash aggregate” kernels are not exposed as direct callables at module level (the compute module source explicitly avoids exposing “hash_aggregate” functions as global wrappers). ([Apache Arrow][33])

  * The user-facing docs also note grouped aggregation functions need to be used through `Table.group_by()` rather than directly. ([Apache Arrow][32])

### 5.1 List explode toolkit (canonical pattern)

These three functions are your “explode without Python” foundation:

* `pc.list_parent_indices(lists)` emits the parent list index for each flattened value. ([Apache Arrow][34])
* `pc.list_flatten(lists, recursive=False)` flattens list values; **null list values do not emit any output**. ([Apache Arrow][35])
* `pc.take(data, indices)` selects values/records given integer indices (works for arrays, record batches, tables). ([Apache Arrow][36])

**Canonical explode-to-edge-table pattern**

```python
import pyarrow.compute as pc

# list_col: list<...>
# parent_id_col: scalar per row (e.g., node_id)
parent_ix = pc.list_parent_indices(list_col)     # len = total elements
values = pc.list_flatten(list_col)               # flattened values
parent_ids = pc.take(parent_id_col, parent_ix)   # align parents with values
```

This produces a “(parent_id, value)” edge list entirely in kernels. ([Apache Arrow][34])

Related list helpers you’ll frequently need:

* `pc.list_value_length(lists)` for lengths (null stays null) ([Apache Arrow][37])
* `pc.list_slice(lists, start, stop=None, step=1, ...)` for per-list slicing ([Apache Arrow][38])

### 5.2 Struct/list/map shaping for normalized schemas

Core struct constructor:

* `pc.make_struct(*args, field_names=...)` wraps arrays/scalars into a `StructArray`. ([Apache Arrow][39])

Core encoding primitive:

* `pc.dictionary_encode(array)` returns a dictionary-encoded version of input; no-op if already dictionary array. ([Apache Arrow][40])

These show up constantly in your normalized schemas:

* pack related columns into a struct payload column
* dictionary-encode string columns for smaller memory footprint and faster joins/compares (depending on engine)

### 5.3 Stable hashing building blocks (how to do this “correctly” in Arrow land)

Because compute kernels available vary by Arrow version/build, the best-in-class posture is:

1. Enumerate available compute kernels at runtime via `pyarrow.compute.list_functions()` (the compute module exposes the registry and list functions). ([Apache Arrow][33])
2. Pick an engine-available hash kernel (if present) and invoke it via `call_function` by name (also exposed). ([Apache Arrow][33])
3. For deterministic hashing of composite keys:

   * canonicalize inputs (e.g., cast types, normalize null handling)
   * pack into `StructArray` (`make_struct`)
   * hash the packed representation (kernel-dependent)

Even if you end up implementing “stable hash” via a Rust UDF in DataFusion later, this is the Arrow-native ladder you want.

### 5.4 Sorting / ranking / grouping primitives when Arrow-native is enough

Sorting:

* `pc.sort_indices(input, options=SortOptions(...))` returns indices defining a **stable sort**. ([Apache Arrow][41])
* Use `pc.take(table_or_array, sort_ix)` to apply ordering. ([Apache Arrow][36])

This is the canonical Arrow-native “deterministic canonical sort” building block:

```python
import pyarrow.compute as pc

ix = pc.sort_indices(table, sort_keys=[("stable_id", "ascending")])
sorted_table = pc.take(table, ix)
```

(`sort_indices` is stable; null/NaN ordering is configurable via `SortOptions`.) ([Apache Arrow][41])

Selection / top-k patterns:

* options objects like `SelectKOptions(k, sort_keys=...)` exist for “select k based on sort keys” style operations. ([Apache Arrow][42])

Grouped aggregations:

* grouped aggregation kernels exist but are not exposed the same way as scalar kernels; use `Table.group_by()` per compute docs. ([Apache Arrow][32])

---

## 6) Concurrency control (global thread pools)

### 6.1 CPU pool

* `pyarrow.cpu_count()` reads initial thread count based on `OMP_NUM_THREADS` / `OMP_THREAD_LIMIT` or hardware threads, and can be changed at runtime with `set_cpu_count()`. ([Apache Arrow][7])
* `pyarrow.set_cpu_count(count)` sets number of threads for parallel operations. ([Apache Arrow][43])

### 6.2 IO pool

* `pyarrow.io_thread_count()` returns IO pool size; dataset scanning uses it implicitly. ([Apache Arrow][8])
* `pyarrow.set_io_thread_count(count)` sets IO threads; many operations like dataset scanning use this pool. ([Apache Arrow][20])

### 6.3 Avoiding oversubscription with DataFusion

Practical rule for your integrated stack:

* If DataFusion is using `target_partitions = P` and fully parallelizing across partitions, you typically reduce Arrow CPU pool threads so that total runnable CPU work ≈ core count (rather than `P * cpu_count`).
* If the workload is IO-bound (remote Parquet), increase Arrow IO pool and keep CPU moderate.

(Arrow itself provides the knobs; the “best-in-class” part is that *your runtime profile owns them*.) ([Apache Arrow][20])

---

## 7) Memory pools and allocators (robustness + debugging)

### 7.1 MemoryPool API

* `pyarrow.default_memory_pool()` returns the process-global pool. ([Apache Arrow][9])
* `MemoryPool.backend_name` reports backend (e.g., “jemalloc”). ([Apache Arrow][44])
* `MemoryPool.bytes_allocated()` returns current allocated bytes. ([Apache Arrow][44])
* `MemoryPool.max_memory()` returns peak allocation (may be approximate in multithreaded apps; may be None depending on pool). ([Apache Arrow][44])
* `pyarrow.total_allocated_bytes()` returns allocated bytes from default pool (other pools may not be accounted for). ([Apache Arrow][45])

### 7.2 Alternative allocators

* `pyarrow.jemalloc_memory_pool()` and `pyarrow.mimalloc_memory_pool()` return allocator-backed pools; raise `NotImplementedError` if support isn’t enabled in the build. ([Apache Arrow][46])
* `pyarrow.set_memory_pool(pool)` sets the default pool. ([Apache Arrow][47])
* `pyarrow.jemalloc_set_decay_ms(...)` lets you adjust jemalloc decay; note it affects future arenas. ([Apache Arrow][48])

### 7.3 Instrumentation patterns (what you standardize)

**Per-run memory snapshots**

```python
import pyarrow as pa

pool = pa.default_memory_pool()
before = (pool.bytes_allocated(), pool.max_memory())

# run workload

after = (pool.bytes_allocated(), pool.max_memory())
```

APIs are stable. ([Apache Arrow][44])

**Allocator logging (debug mode only)**

* `pyarrow.log_memory_allocations(enable=True|False)` toggles allocator logging. ([Apache Arrow][49])

**Leak suspicion playbook**

* If you suspect “memory not returning,” check:

  * Are you using a non-default memory pool (note `total_allocated_bytes` may miss it)? ([Apache Arrow][45])
  * Are you holding references to tables/readers/scanners (GC)?
  * Are you using jemalloc/mimalloc with decay settings that keep memory in arenas? ([Apache Arrow][50])

---

## 8) IPC formats (spill + interchange artifacts)

### 8.1 File vs stream formats

Arrow supports:

* **File format** (seekable, supports memory mapping)
* **Streaming format** (append-only stream of record batches)

The Python IPC guide describes both and emphasizes that Arrow is optimized for zero-copy and memory-mapped reads. ([Apache Arrow][51])

### 8.2 Writing/reading streams incrementally

* `pyarrow.ipc.open_stream(source, ...)` creates a `RecordBatchStreamReader`. ([Apache Arrow][52])
* `pyarrow.ipc.RecordBatchStreamWriter` can `write_batch`, `write_table`, and exposes IPC write statistics; `close()` writes the end-of-stream marker. ([Apache Arrow][53])

### 8.3 IPC streams as “debug artifacts”

For reproducible failure bundles:

* Persist the exact scanned/produced batches in IPC stream format.
* Re-run downstream transforms by reading with `ipc.open_stream`.

This is especially useful when debugging:

* nondeterministic ordering boundaries
* memory blowups tied to batch sizing
* cross-library interop issues (because IPC is the canonical Arrow physical representation) ([Apache Arrow][51])

---

## 9) Flight (optional) — remote batch transport for a future “query service”

### 9.1 Conceptual model

Arrow Flight is a gRPC-based protocol for sending Arrow data streams between client and server. In Python, you implement a server by subclassing `pyarrow.flight.FlightServerBase` and implementing handlers like `list_flights`, `get_flight_info`, and `do_get`. ([Apache Arrow][54])

### 9.2 Streaming data without GIL overhead (important)

`pyarrow.flight.RecordBatchStream` streams `RecordBatchReader` or `Table` data, and the docs explicitly note the remainder of the DoGet request is handled in C++ without acquiring the GIL. ([Apache Arrow][55])

This is a key “best-in-class” property: it keeps Python handler overhead low.

### 9.3 Client/server APIs you’ll actually use

**Server**

* `FlightServerBase(..., middleware={...}, auth_handler=..., tls_certificates=..., root_certificates=...)` supports middleware and TLS/mTLS settings. ([Apache Arrow][56])
* Server middleware:

  * `ServerMiddlewareFactory.start_call(...)` and middleware instances can be retrieved via `ServerCallContext.get_middleware()`; middleware methods are called on the same thread as RPC handlers (so thread locals can work). ([Apache Arrow][57])
  * `ServerMiddleware` methods should be fast and infallible (docs warn not to raise or stall). ([Apache Arrow][58])
* Authentication:

  * Implement `ServerAuthHandler.authenticate(...)` and `is_valid(token)` for auth handshake and token validation. ([Apache Arrow][59])

**Client**

* `FlightClient(location, tls_root_certs=None, cert_chain=None, private_key=None, middleware=None, disable_server_verification=..., ...)` includes TLS controls (including a “disable server verification” flag). ([Apache Arrow][60])
* `ClientAuthHandler` implements client-side handshake and provides tokens for calls. ([Apache Arrow][61])

**Descriptors and endpoints**

* `FlightInfo(schema, descriptor, endpoints, total_records=..., total_bytes=..., ordered=...)` describes a stream and can mark whether it is ordered. ([Apache Arrow][62])
* `FlightEndpoint(ticket, locations, expiration_time=...)` binds ticket + locations. ([Apache Arrow][63])

### 9.4 Operational patterns

* Treat Flight as “Arrow C Stream over the network”: always stream batches, don’t materialize full tables in handlers unless necessary.
* Use middleware for trace propagation / request metadata (the examples directory includes middleware demos). ([GitHub][64])

---

## 10) Failure-mode playbooks

### 10.1 Ordering surprises in dataset writes

**Symptom:** output dataset row order changes across runs.

**Controls:**

* If you need order: `preserve_order=True` (expect perf hit) ([Apache Arrow][5])
* Otherwise: encode canonical ordering explicitly:

  * add stable sort keys to data
  * run stable sort (`sort_indices` + `take`) before writing
  * accept that multi-threaded writes can reorder if you don’t preserve order ([Apache Arrow][5])

### 10.2 Memory blowups

**Scan-time:**

* Reduce `batch_size` (docs explicitly suggest this if batches overflow memory). ([Apache Arrow][13])
* Reduce `batch_readahead` / `fragment_readahead` (RAM vs IO utilization tradeoff). ([Apache Arrow][13])

**Write-time:**

* Use `min_rows_per_group` / `max_rows_per_group` to control buffering and row group sizes; ensure both are set to avoid tiny row groups. ([Apache Arrow][5])
* Watch `max_open_files` (too low fragments output; too high increases memory/file handle pressure). ([Apache Arrow][5])

**Compute-time:**

* Avoid Acero `order_by` unless you know the dataset fits memory (no larger-than-memory sort). ([Apache Arrow][6])

### 10.3 Thread pool contention (IO starvation vs CPU saturation)

* Dataset scanning uses the IO thread pool; increase/decrease via `set_io_thread_count`. ([Apache Arrow][20])
* Compute-heavy kernels use the CPU pool; control via `set_cpu_count`. ([Apache Arrow][43])
* When DataFusion is also parallel, treat Arrow pools as subordinate to a single global concurrency budget.

### 10.4 Interop lifetime bugs (PyCapsule ownership mistakes)

**Symptoms:** segfaults, double frees, “use after free,” or persistent leaks across interop boundaries.

**Rules:**

* PyCapsules must have destructors calling `release` when unconsumed. ([Apache Arrow][3])
* Consumers must null the release callback after moving data (single consumption). ([Apache Arrow][3])
* Don’t reuse a consumed capsule (single-consumption contract). ([Apache Arrow][3])

---

## Appendix A) “Approved kernels” registry (internal compute primitive catalog)

A pragmatic starter set for your repo’s “kernel registry” (you can auto-discover exact names via `pyarrow.compute.list_functions()`): ([Apache Arrow][33])

**List explosion / nesting**

* `list_parent_indices` ([Apache Arrow][34])
* `list_flatten` ([Apache Arrow][35])
* `list_value_length` ([Apache Arrow][37])
* `list_slice` ([Apache Arrow][38])

**Alignment and ordering**

* `take` ([Apache Arrow][36])
* `sort_indices` + `SortOptions` ([Apache Arrow][41])

**Struct packing and encoding**

* `make_struct` ([Apache Arrow][39])
* `dictionary_encode` ([Apache Arrow][40])

---

## Appendix B) Deterministic dataset write spec (partitioning + naming + ordering)

A concrete spec you can encode into your runtime profile:

1. **Partitioning**

   * choose `HivePartitioning` if you want `key=value` directories; choose `DirectoryPartitioning` if you want strict positional segments ([Apache Arrow][17])

2. **File naming**

   * set `basename_template="part-{i}"` plus run-id suffix (so concurrent runs never collide) ([Apache Arrow][5])

3. **Existing data behavior**

   * append-like: `existing_data_behavior="overwrite_or_ignore"` + unique basenames ([Apache Arrow][5])
   * partition overwrite: `existing_data_behavior="delete_matching"` ([Apache Arrow][5])

4. **Ordering**

   * default: `preserve_order=False` (faster) but ensure your *data itself* is canonically ordered upstream (sort indices + take)
   * strict: `preserve_order=True` for cases where upstream ordering is semantically meaningful ([Apache Arrow][5])

5. **Metadata sidecars**

   * always pass `file_visitor` to collect file metadata and build `_metadata` when using Parquet ([Apache Arrow][5])

---

## Appendix C) Microbench harnesses (explode, scan, write)

Use these to track regressions across PRs:

1. **Explode throughput**

* input: `N` rows with `avg_k` list length
* kernels: `list_parent_indices + list_flatten + take` ([Apache Arrow][34])

2. **Scan pushdown effectiveness**

* dataset: partitioned by `kind=...`
* scan: projection + filter + `count_rows`
* vary `batch_size`, `fragment_readahead`, `cache_metadata` ([Apache Arrow][13])

3. **Writer determinism/perf**

* write the same reader with:

  * `use_threads=True, preserve_order=False`
  * `use_threads=True, preserve_order=True`
  * `use_threads=False`
* validate order and measure throughput; record results alongside runtime profile ([Apache Arrow][5])

---


[1]: https://arrow.apache.org/docs/python/index.html?utm_source=chatgpt.com "Python — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/format/CDataInterface.html?utm_source=chatgpt.com "The Arrow C data interface — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html "The Arrow PyCapsule Interface — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.ScanNodeOptions.html "pyarrow.acero.ScanNodeOptions — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html "pyarrow.dataset.write_dataset — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.OrderByNodeOptions.html?utm_source=chatgpt.com "pyarrow.acero.OrderByNodeOptions — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.cpu_count.html?utm_source=chatgpt.com "pyarrow.cpu_count — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.io_thread_count.html?utm_source=chatgpt.com "pyarrow.io_thread_count — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.default_memory_pool.html?utm_source=chatgpt.com "pyarrow.default_memory_pool — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/format/CStreamInterface.html?utm_source=chatgpt.com "The Arrow C stream interface — Apache Arrow v22.0.0"
[11]: https://arrow.apache.org/docs/format/CDeviceDataInterface.html?utm_source=chatgpt.com "The Arrow C Device data interface"
[12]: https://arrow.apache.org/docs/python/generated/pyarrow.table.html?utm_source=chatgpt.com "pyarrow.table — Apache Arrow v22.0.0"
[13]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html "pyarrow.dataset.Scanner — Apache Arrow v22.0.0"
[14]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.dataset.html "pyarrow.dataset.dataset — Apache Arrow v22.0.0"
[15]: https://arrow.apache.org/docs/python/filesystems.html?utm_source=chatgpt.com "Filesystem Interface — Apache Arrow v22.0.0"
[16]: https://arrow.apache.org/docs/python/generated/pyarrow.fs.S3FileSystem.html?utm_source=chatgpt.com "pyarrow.fs.S3FileSystem — Apache Arrow v22.0.0"
[17]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.HivePartitioning.html?utm_source=chatgpt.com "pyarrow.dataset.HivePartitioning — Apache Arrow v22.0.0"
[18]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.DirectoryPartitioning.html?utm_source=chatgpt.com "pyarrow.dataset.DirectoryPartitioning — Apache Arrow v22.0.0"
[19]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Fragment.html?utm_source=chatgpt.com "pyarrow.dataset.Fragment — Apache Arrow v22.0.0"
[20]: https://arrow.apache.org/docs/python/generated/pyarrow.set_io_thread_count.html?utm_source=chatgpt.com "pyarrow.set_io_thread_count — Apache Arrow v22.0.0"
[21]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html "pyarrow.dataset.Dataset — Apache Arrow v22.0.0"
[22]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Fragment.html "pyarrow.dataset.Fragment — Apache Arrow v22.0.0"
[23]: https://github.com/apache/arrow/issues/39030?utm_source=chatgpt.com "pyarrow.dataset.write_dataset do not preserve order #39030"
[24]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_to_dataset.html?utm_source=chatgpt.com "pyarrow.parquet.write_to_dataset — Apache Arrow v22.0.0"
[25]: https://arrow.apache.org/docs/cpp/acero/user_guide.html?utm_source=chatgpt.com "Acero User's Guide — Apache Arrow v22.0.0"
[26]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.Declaration.html?utm_source=chatgpt.com "pyarrow.acero.Declaration — Apache Arrow v22.0.0"
[27]: https://arrow.apache.org/docs/python/api/acero.html?utm_source=chatgpt.com "Acero - Streaming Execution Engine — Apache Arrow v22.0.0"
[28]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.HashJoinNodeOptions.html?utm_source=chatgpt.com "pyarrow.acero.HashJoinNodeOptions — Apache Arrow v22.0.0"
[29]: https://arrow.apache.org/docs/python/generated/pyarrow.substrait.serialize_expressions.html?utm_source=chatgpt.com "pyarrow.substrait.serialize_expressions - Apache Arrow"
[30]: https://arrow.apache.org/docs/python/generated/pyarrow.substrait.run_query.html?utm_source=chatgpt.com "pyarrow.substrait.run_query — Apache Arrow v22.0.0"
[31]: https://arrow.apache.org/docs/python/generated/pyarrow.substrait.get_supported_functions.html?utm_source=chatgpt.com "pyarrow.substrait.get_supported_functions - Apache Arrow"
[32]: https://arrow.apache.org/docs/python/compute.html?utm_source=chatgpt.com "Compute Functions — Apache Arrow v22.0.0"
[33]: https://arrow.apache.org/docs/_modules/pyarrow/compute.html "pyarrow.compute — Apache Arrow v22.0.0"
[34]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_parent_indices.html?utm_source=chatgpt.com "pyarrow.compute.list_parent_indices — Apache Arrow v22.0.0"
[35]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_flatten.html?utm_source=chatgpt.com "pyarrow.compute.list_flatten — Apache Arrow v22.0.0"
[36]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.take.html?utm_source=chatgpt.com "pyarrow.compute.take — Apache Arrow v22.0.0"
[37]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_value_length.html?utm_source=chatgpt.com "pyarrow.compute.list_value_length — Apache Arrow v22.0.0"
[38]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_slice.html?utm_source=chatgpt.com "pyarrow.compute.list_slice — Apache Arrow v22.0.0"
[39]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.make_struct.html?utm_source=chatgpt.com "pyarrow.compute.make_struct — Apache Arrow v22.0.0"
[40]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.dictionary_encode.html?utm_source=chatgpt.com "pyarrow.compute.dictionary_encode — Apache Arrow v22.0.0"
[41]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.sort_indices.html?utm_source=chatgpt.com "pyarrow.compute.sort_indices — Apache Arrow v22.0.0"
[42]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.SelectKOptions.html?utm_source=chatgpt.com "pyarrow.compute.SelectKOptions — Apache Arrow v22.0.0"
[43]: https://arrow.apache.org/docs/python/generated/pyarrow.set_cpu_count.html?utm_source=chatgpt.com "pyarrow.set_cpu_count — Apache Arrow v22.0.0"
[44]: https://arrow.apache.org/docs/python/generated/pyarrow.MemoryPool.html?utm_source=chatgpt.com "pyarrow.MemoryPool — Apache Arrow v22.0.0"
[45]: https://arrow.apache.org/docs/python/generated/pyarrow.total_allocated_bytes.html?utm_source=chatgpt.com "pyarrow.total_allocated_bytes — Apache Arrow v22.0.0"
[46]: https://arrow.apache.org/docs/python/generated/pyarrow.jemalloc_memory_pool.html?utm_source=chatgpt.com "pyarrow.jemalloc_memory_pool — Apache Arrow v22.0.0"
[47]: https://arrow.apache.org/docs/python/generated/pyarrow.set_memory_pool.html?utm_source=chatgpt.com "pyarrow.set_memory_pool — Apache Arrow v22.0.0"
[48]: https://arrow.apache.org/docs/python/generated/pyarrow.jemalloc_set_decay_ms.html?utm_source=chatgpt.com "pyarrow.jemalloc_set_decay_ms — Apache Arrow v22.0.0"
[49]: https://arrow.apache.org/docs/python/generated/pyarrow.log_memory_allocations.html?utm_source=chatgpt.com "pyarrow.log_memory_allocations — Apache Arrow v22.0.0"
[50]: https://arrow.apache.org/docs/5.0/python/generated/pyarrow.jemalloc_set_decay_ms.html?utm_source=chatgpt.com "pyarrow.jemalloc_set_decay_ms — Apache Arrow v5.0.0"
[51]: https://arrow.apache.org/docs/python/ipc.html?utm_source=chatgpt.com "Streaming, Serialization, and IPC — Apache Arrow v22.0.0"
[52]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.open_stream.html?utm_source=chatgpt.com "pyarrow.ipc.open_stream — Apache Arrow v22.0.0"
[53]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.RecordBatchStreamWriter.html?utm_source=chatgpt.com "pyarrow.ipc.RecordBatchStreamWriter — Apache Arrow v22.0.0"
[54]: https://arrow.apache.org/cookbook/py/flight.html?utm_source=chatgpt.com "Arrow Flight — Apache Arrow Python Cookbook documentation"
[55]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.RecordBatchStream.html?utm_source=chatgpt.com "pyarrow.flight.RecordBatchStream — Apache Arrow v22.0.0"
[56]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightServerBase.html?utm_source=chatgpt.com "pyarrow.flight.FlightServerBase — Apache Arrow v22.0.0"
[57]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ServerMiddlewareFactory.html?utm_source=chatgpt.com "pyarrow.flight.ServerMiddlewareFactory - Apache Arrow"
[58]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ServerMiddleware.html?utm_source=chatgpt.com "pyarrow.flight.ServerMiddleware — Apache Arrow v22.0.0"
[59]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ServerAuthHandler.html?utm_source=chatgpt.com "pyarrow.flight.ServerAuthHandler — Apache Arrow v22.0.0"
[60]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightClient.html?utm_source=chatgpt.com "pyarrow.flight.FlightClient — Apache Arrow v22.0.0"
[61]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ClientAuthHandler.html?utm_source=chatgpt.com "pyarrow.flight.ClientAuthHandler — Apache Arrow v22.0.0"
[62]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightInfo.html?utm_source=chatgpt.com "pyarrow.flight.FlightInfo — Apache Arrow v22.0.0"
[63]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightEndpoint.html?utm_source=chatgpt.com "pyarrow.flight.FlightEndpoint — Apache Arrow v22.0.0"
[64]: https://github.com/apache/arrow/blob/master/python/examples/flight/middleware.py?utm_source=chatgpt.com "arrow/python/examples/flight/middleware.py at main"

Below is the **follow-on PyArrow content** that rounds out the advanced capabilities most likely to matter in your codebase (streaming rule outputs, deterministic graph products, nested types, scan/write policy, and “kernel lane” extensibility). It’s written to *append* to the previous `pyarrow-execution-advanced.md`.

---

## 11) The Arrow Expression IR that unifies Dataset, Scanner projections, and Acero

### 11.1 `pyarrow.dataset.Expression` (filter/projection language)

`pyarrow.dataset.Expression` is the logical predicate/projection language used by the Dataset/Scanner APIs. The docs describe creating expressions by referencing fields and scalars, comparing them, combining with `& | ~`, and using Expression methods like `.isin()` (note: you can’t use Python’s `and/or/not`). ([Apache Arrow][1])

**Minimal pattern**

```python
import pyarrow.dataset as ds

expr = (ds.field("kind") == ds.scalar("CALL")) & (ds.field("src_id") > 0)
scanner = ds.dataset("edges/", format="parquet").scanner(filter=expr)
```

(Scanner examples also show using `ds.field(...)` expressions.) ([Apache Arrow][2])

### 11.2 `pyarrow.dataset.scalar` vs `pyarrow.scalar`

`pyarrow.dataset.scalar(value)` returns an **Expression** representing a scalar usable in dataset predicates/projections, and is explicitly different from `pyarrow.scalar()` (which returns an Arrow Scalar object). ([Apache Arrow][3])

### 11.3 Acero “project” expects scalar compute expressions

The Acero `project` node evaluates a list of **scalar expressions**: elementwise functions returning one output per input row (no global state). ([Apache Arrow][4])

This is the same conceptual limitation as compute UDFs used in “query engine expressions” (see §16): if the function needs global knowledge (grouping, sorting, ranking), it must be expressed via aggregate/sort nodes (or moved to DataFusion/Ibis).

---

## 12) Dataset-level relational ops (surprisingly relevant): `join`, `join_asof`, `sort_by`

These are often overlooked because people treat `pyarrow.dataset` as “scan only,” but the `Dataset` API includes higher-level relational operations that can be useful as **Arrow-native fallback lanes** (or for smaller, in-memory intermediate products).

### 12.1 `Dataset.join(...)` (semi/anti joins included)

`Dataset.join` joins two datasets on key columns. It supports join types including semi/anti joins and returns an **InMemoryDataset**. It also includes suffix controls and a `coalesce_keys` option to omit duplicated keys. ([Apache Arrow][2])

Key signature and semantics:

* `join_type` supports: `"left semi"`, `"right semi"`, `"left anti"`, `"right anti"`, `"inner"`, `"left outer"`, `"right outer"`, `"full outer"` ([Apache Arrow][2])
* returns `InMemoryDataset` (treat as a materialization boundary) ([Apache Arrow][2])

### 12.2 `Dataset.join_asof(...)` (time/position nearest-join)

`join_asof` performs an as-of join (nearest match) and **requires both datasets to be sorted by the key**. The “on” key must be an integer, date, or timestamp type, and `tolerance` defines exact/past/future matching ranges. ([Apache Arrow][2])

This is directly relevant if you ever do **span alignment** or time-index alignment in Arrow-native pipelines (e.g., mapping spans to nearest anchor events).

### 12.3 `Dataset.sort_by(...)` (returns `InMemoryDataset`)

`sort_by` sorts by one or more columns (supports additional SortOptions kwargs) and returns an `InMemoryDataset`. ([Apache Arrow][2])

Use as a determinism tool only when the dataset fits memory (otherwise push sorting to DataFusion or accept “stable scan ordering tiers”).

---

## 13) Scanner and batch-provenance surfaces you should standardize

### 13.1 `Scanner.scan_batches()` → `TaggedRecordBatch`

`Scanner` exposes multiple consumption surfaces, including `scan_batches()` (“Consume a Scanner in record batches with corresponding fragments”). ([Apache Arrow][5])

The resulting `TaggedRecordBatch` carries:

* `record_batch`
* `fragment` (where it came from) ([Apache Arrow][6])

**Why this matters in your codebase**

* You can attach per-batch provenance (file path, partition expression, row groups) to diagnostics without injecting extra columns.
* You can build deterministic “writer sharding” policies that align with fragments (rather than arbitrary task scheduling).

### 13.2 `Scanner` is a materialized scan plan with schemas you can snapshot

`Scanner` includes:

* `dataset_schema`: schema used to read from fragments
* `projected_schema`: schema after projection expressions are applied ([Apache Arrow][5])

Persist these in your run artifacts to make schema drift visible.

### 13.3 Practical “provenance-first” scan pattern

```python
import pyarrow.dataset as ds

dataset = ds.dataset("edges/", format="parquet")
scanner = dataset.scanner(columns=["src_id", "dst_id", "kind"])

for tagged in scanner.scan_batches():
    batch = tagged.record_batch
    frag  = tagged.fragment
    # frag.path, frag.partition_expression, frag.row_groups, etc. (see §14)
```

`TaggedRecordBatch` explicitly exposes both components. ([Apache Arrow][6])

---

## 14) Fragment-level controls for Parquet: row-group pruning, metadata caching, and “special fields”

### 14.1 `ParquetFileFragment.ensure_complete_metadata()`

`ParquetFileFragment` supports `ensure_complete_metadata()` to ensure that all metadata (statistics, physical schema, etc.) has been read and cached in the fragment. ([Apache Arrow][7])

This is a strong optimization for repeated scans over remote storage (avoid repeated metadata fetches), and pairs well with `cache_metadata=True` in scanners.

### 14.2 Row-group slicing and row-group-stat pruning: `subset(...)`

`ParquetFileFragment.subset` can create a view of a subset of row groups, specified either by:

* a filter predicate (using Parquet row group statistics), or
* explicit row group IDs. ([Apache Arrow][7])

This is one of the best Arrow-native “do less IO” tools when your predicates match Parquet stats.

### 14.3 Special fields (`__filename`, `__fragment_index`, …) are projection tools

Parquet fragment scanning (and dataset scanning) allows special fields to be used in **columns/projections**:

* `__batch_index`, `__fragment_index`, `__last_in_fragment`, `__filename` ([Apache Arrow][7])

However, attempting to use these special fields inside a dataset filter expression has known limitations in practice (a reported usage issue was closed as “not planned”). ([GitHub][8])

**Best practice**

* Use these special fields in `columns={...}` projections (e.g., emit `__filename`)
* Use `get_fragments()` / `scan_batches()` to filter by fragment-level metadata in Python if needed ([Apache Arrow][2])

### 14.4 Fragment provenance properties you should use

`ParquetFileFragment` exposes:

* `partition_expression` (“true for all data viewed by this Fragment”)
* `path`, `physical_schema`, `num_row_groups`, etc. ([Apache Arrow][7])

---

## 15) Parquet scan tuning that really matters: `ParquetReadOptions` + `ParquetFragmentScanOptions`

### 15.1 `ParquetFileFormat(read_options=..., default_fragment_scan_options=...)`

`ParquetFileFormat` takes:

* `ParquetReadOptions` (logical type interpretation)
* `ParquetFragmentScanOptions` (scan-time buffering / IO behavior)
  and also supports `make_fragment(...)` and `make_write_options(...)`. ([Apache Arrow][9])

It’s a best-in-class practice to treat these as part of your runtime profile rather than “defaults you hope are fine.”

### 15.2 `ParquetReadOptions`: schema fidelity and compatibility knobs

`ParquetReadOptions` includes:

* `coerce_int96_timestamp_unit` (INT96 timestamp resolution handling)
* `binary_type` and `list_type` overrides (ignored if a serialized Arrow schema exists in Parquet metadata)
* `dictionary_columns` (read selected columns as dictionary) ([Apache Arrow][10])

This ties directly into Parquet schema round-tripping: PyArrow’s writer stores the Arrow schema in the Parquet metadata (`ARROW:schema`) by default so timezone-aware timestamps and duration types can be reconstructed faithfully. ([Apache Arrow][11])

### 15.3 `ParquetFragmentScanOptions`: high-latency storage optimization

`ParquetFragmentScanOptions` includes high-impact knobs:

* `pre_buffer`: pre-buffer raw Parquet data to reduce “one read per column chunk”; improves high-latency filesystems (S3/GCS) using a background IO thread pool; trades memory for speed ([Apache Arrow][12])
* thrift decoding size limits (`thrift_string_size_limit`, `thrift_container_size_limit`) to guard against pathological files ([Apache Arrow][12])
* decryption support (`decryption_config` / `decryption_properties`) ([Apache Arrow][12])
* checksum verification (`page_checksum_verification`) ([Apache Arrow][12])
* `arrow_extensions_enabled` (relevant when you rely on Arrow extension types) ([Apache Arrow][12])

---

## 16) Writer control beyond `write_dataset`: ParquetWriter, schema fidelity, and content-defined chunking

### 16.1 `ParquetWriter` “power knobs”

`pyarrow.parquet.ParquetWriter` exposes numerous options that matter for determinism, robustness, and incremental storage behavior:

* `store_schema=True` stores Arrow schema in Parquet metadata under `ARROW:schema` (restores timezone-aware timestamps and duration types on read). ([Apache Arrow][11])
* `sorting_columns`: writes sort order to row group metadata (writer does not sort—this is a contract you assert). ([Apache Arrow][11])
* `write_page_index`: writes page index; stats-based filtering becomes more efficient by centralizing stats (note: docs say page index not yet used on read by PyArrow). ([Apache Arrow][11])
* `write_page_checksum`: corruption detection. ([Apache Arrow][11])
* `store_decimal_as_integer`: more compact decimal representation for precision <= 18. ([Apache Arrow][11])
* Parquet modular encryption props (encryption/decryption) appear in the Parquet writer/format docs. ([Apache Arrow][13])

### 16.2 Content-defined chunking (CDC) for incremental rebuild storage efficiency (experimental)

PyArrow documents an experimental “content-defined chunking” feature intended for content-addressable storage systems. It writes data pages according to rolling-hash chunk boundaries so small edits change fewer pages; configured via `use_content_defined_chunking` with defaults and tunable min/max chunk sizes. ([Apache Arrow][13])

**Important operational note**: the docs explicitly recommend keeping Parquet write options consistent across writes to maximize deduplication. ([Apache Arrow][13])

### 16.3 Minimal snippet: metadata-rich Parquet writes

```python
import pyarrow.parquet as pq

writer = pq.ParquetWriter(
    "out.parquet",
    schema,
    store_schema=True,
    write_page_checksum=True,
    # optionally: sorting_columns=[...], use_content_defined_chunking=True, ...
)
writer.write_table(table)
writer.close()
```

(Options and behaviors are documented on ParquetWriter.) ([Apache Arrow][11])

---

## 17) `_metadata` / `_common_metadata` sidecars the *right way* (and when not to)

### 17.1 Writing metadata-only files

`pyarrow.parquet.write_metadata(schema, where, metadata_collector=..., ...)` writes a metadata-only Parquet file and is explicitly intended to generate `_common_metadata` and `_metadata` sidecars. ([Apache Arrow][14])

### 17.2 Merging file metadata programmatically

`pyarrow.parquet.FileMetaData` supports:

* `append_row_groups(other)` to merge row groups
* `set_file_path(path)` to set ColumnChunk paths
* `write_metadata_file(where)` to write metadata-only Parquet files ([Apache Arrow][15])

**Pattern with `write_dataset(file_visitor=...)`**

1. Collect `written_file.metadata` for each file via `file_visitor` (you already use this; docs confirm this metadata can be used to build `_metadata`). ([Apache Arrow][16])
2. Merge row groups into a single `FileMetaData` using `append_row_groups`. ([Apache Arrow][15])
3. Call `write_metadata_file("_metadata")`. ([Apache Arrow][15])

### 17.3 `_metadata` scalability caveat (do not enable blindly)

Aggregating metadata can become enormous: one discussion notes `_metadata` size scaling roughly with `total_n_row_groups * n_columns`, and that this can swamp workers (Spark reportedly stopped writing such files due to scalability issues). ([GitHub][17])

**Best practice**

* Only generate `_metadata` when you know it’s needed and bounded.
* Prefer `_common_metadata` (schema-only) for many workflows, or store your own lightweight dataset manifest.

---

## 18) Compute extensibility: custom compute functions (UDFs) that work in Dataset/Acero expression plans

### 18.1 Scalar UDF registration (experimental but real)

PyArrow supports registering custom compute functions. UDF support is limited to **scalar functions** (elementwise; output shape matches input), and these can be called by registered name. ([Apache Arrow][18])

`pyarrow.compute.register_scalar_function(func, function_name, function_doc, in_types, out_type, func_registry=None)`:

* first arg to `func` is a `UdfContext`
* must return Array or Scalar matching `out_type`
* returns Scalar if all args are scalar, else Array
* supports varargs by accepting `*args` and using last `in_type` for varargs
* scalar functions are explicitly “the only functions allowed in query engine expressions.” ([Apache Arrow][19])

`UdfContext` gives:

* `batch_length`
* `memory_pool` (use it for allocations inside your UDF if downstream calls accept `memory_pool=`). ([Apache Arrow][20])

### 18.2 Calling registered functions by name (`get_function`, `call_function`)

The docs’ own example registers a scalar function, retrieves it with `pc.get_function`, and executes with `pc.call_function`. ([Apache Arrow][21])

**Minimal example (mirrors docs)**

```python
import pyarrow as pa
import pyarrow.compute as pc

func_doc = {"summary": "simple udf", "description": "add a constant"}
def add1(ctx, array):
    return pc.add(array, 1, memory_pool=ctx.memory_pool)

pc.register_scalar_function(add1, "py_add1", func_doc, {"array": pa.int64()}, pa.int64())

pc.call_function("py_add1", [pa.array([20])])  # -> [21]
```

([Apache Arrow][21])

**Why this matters for your design**

* This is the Arrow-native analogue of “FunctionFactory”: you can add kernels that become usable in Dataset projections and Acero project nodes, without switching to Python loops.

---

## 19) Extension types as “schema-level semantics” (stable IDs, spans, etc.)

### 19.1 `ExtensionType` and IPC round-tripping

`pyarrow.ExtensionType(storage_type, extension_name)` is the concrete base class for Python-defined extension types. The docs emphasize that `extension_name` is used when deserializing IPC data. ([Apache Arrow][22])

### 19.2 Registration is name-based

`pyarrow.register_extension_type(ext_type)` registers an extension type based on its extension name; it requires an instance but applies for any instance of the same subclass regardless of parameterization. ([Apache Arrow][23])

### 19.3 Two strategies: Python-only vs cross-language

Arrow’s “Extending PyArrow” docs describe two patterns:

* subclassing `PyExtensionType` (pickle-based; Python-only)
* subclassing `ExtensionType` with a Python-independent name + serialized metadata, potentially recognizable by other Arrow implementations (e.g., PySpark). ([Apache Arrow][24])

**Why you care**

* This is the cleanest way to put domain semantics into your Arrow schemas (e.g., a StableId type, Span type) while keeping storage representation simple.

---

## 20) Schema and Field metadata as contract carriers (and how to mutate safely)

### 20.1 Schema metadata is immutable; use `with_metadata` / `remove_metadata`

`pyarrow.Schema` includes schema-level metadata and provides:

* `with_metadata(metadata)` → returns a new schema with metadata
* `remove_metadata()` → returns a new schema without metadata
* `equals(other, check_metadata=False)` can include metadata comparison ([Apache Arrow][25])

Schemas created from pandas can carry pandas metadata to roundtrip dtypes/index semantics. ([Apache Arrow][25])

### 20.2 Field metadata and `Field.flatten()` for nested columns

`pyarrow.Field` supports:

* `with_metadata(metadata)` / `remove_metadata()`
* `equals(check_metadata=...)`
* `flatten()` which expands struct fields and prefixes child names with the parent name ([Apache Arrow][26])

This is useful when you store nested payloads (struct columns) but need a deterministic flattened view for downstream engines or for hashing/fingerprinting.

---

## 21) Dataframe Interchange Protocol and `__arrow_array__` conversion hooks

### 21.1 `__dataframe__` (DataFrame Interchange Protocol)

PyArrow implements the dataframe interchange protocol for `pa.Table` and `pa.RecordBatch`. The docs state the protocol supports primitive types plus dictionary type, supports missing data, and supports chunking (“batches” of rows). ([Apache Arrow][27])

* `Table.__dataframe__(nan_as_null=False, allow_copy=True)` ([Apache Arrow][28])
* `RecordBatch.__dataframe__(nan_as_null=False, allow_copy=True)` ([Apache Arrow][29])
* `pyarrow.interchange.from_dataframe(df, allow_copy=True)` builds a `pa.Table` from any object implementing `__dataframe__`. ([Apache Arrow][30])

**Practical use**

* This is a complementary interoperability lane to PyCapsule `__arrow_c_stream__`, especially when a library supports interchange but not Arrow C Stream directly.

### 21.2 `__arrow_array__` for “array-like → pa.Array” control

PyArrow’s `pa.array(...)` conversion can be extended for other array-like objects by implementing `__arrow_array__`, analogous to NumPy’s `__array__`. ([Apache Arrow][31])

This is relevant for integrating your own internal columnar containers or specialized array wrappers into the Arrow compute ecosystem cleanly.

---

## 22) Buffers and zero-copy slicing: the real “memory primitive”

The `pyarrow.Buffer` wraps Arrow’s `arrow::Buffer`, which is the core memory object. The memory docs emphasize that Buffers can be **zero-copy sliced** while preserving lifetime and parent-child relationships. ([Apache Arrow][32])

**Why it matters**

* If you ever build “view” layers (spans into big tables, byte slices of tokens), Buffer slicing is the correct primitive to avoid copies and avoid lifetime bugs.

---

## 23) Two more determinism pitfalls worth codifying

### 23.1 `write_dataset` ordering reality

Even with a formal `preserve_order` knob, there are reports/bugs showing `write_dataset` not preserving row order under some workflows, reinforcing the idea that you should never rely on implicit order. ([GitHub][33])

**Safe rule**

* If order matters: sort explicitly (stable `sort_indices` + `take`) before writing, or set `preserve_order=True` and accept the cost. ([Apache Arrow][25])

### 23.2 “Special fields” are not general filter keys

Even though docs advertise special fields for projections, constructing filter expressions referencing them may fail in practice and is not planned to be supported broadly. ([Apache Arrow][7])

**Safe rule**

* Use special fields in projections only; filter fragments/batches using `scan_batches()` provenance or `get_fragments(filter=...)` based on partition/statistics. ([Apache Arrow][2])

---


[1]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Expression.html "pyarrow.dataset.Expression — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html "pyarrow.dataset.Dataset — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.scalar.html "pyarrow.dataset.scalar — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.ProjectNodeOptions.html?utm_source=chatgpt.com "pyarrow.acero.ProjectNodeOptions — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html "pyarrow.dataset.Scanner — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.TaggedRecordBatch.html "pyarrow.dataset.TaggedRecordBatch — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetFileFragment.html "pyarrow.dataset.ParquetFileFragment — Apache Arrow v22.0.0"
[8]: https://github.com/apache/arrow/issues/40196 "[Python] Unable to filter datasets on __fragment_index? · Issue #40196 · apache/arrow · GitHub"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetFileFormat.html "pyarrow.dataset.ParquetFileFormat — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetReadOptions.html "pyarrow.dataset.ParquetReadOptions — Apache Arrow v22.0.0"
[11]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html "pyarrow.parquet.ParquetWriter — Apache Arrow v22.0.0"
[12]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetFragmentScanOptions.html "pyarrow.dataset.ParquetFragmentScanOptions — Apache Arrow v22.0.0"
[13]: https://arrow.apache.org/docs/python/parquet.html "Reading and Writing the Apache Parquet Format — Apache Arrow v22.0.0"
[14]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_metadata.html "pyarrow.parquet.write_metadata — Apache Arrow v22.0.0"
[15]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.FileMetaData.html "pyarrow.parquet.FileMetaData — Apache Arrow v22.0.0"
[16]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html "pyarrow.dataset.write_dataset — Apache Arrow v22.0.0"
[17]: https://github.com/dask/dask/issues/8901 "Change `to_parquet` default to `write_metadata_file=False` · Issue #8901 · dask/dask · GitHub"
[18]: https://arrow.apache.org/docs/python/compute.html?utm_source=chatgpt.com "Compute Functions — Apache Arrow v22.0.0"
[19]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.register_scalar_function.html "pyarrow.compute.register_scalar_function — Apache Arrow v22.0.0"
[20]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.UdfContext.html "pyarrow.compute.UdfContext — Apache Arrow v22.0.0"
[21]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.register_scalar_function.html?utm_source=chatgpt.com "pyarrow.compute.register_scalar_function - Apache Arrow"
[22]: https://arrow.apache.org/docs/python/generated/pyarrow.ExtensionType.html "pyarrow.ExtensionType — Apache Arrow v22.0.0"
[23]: https://arrow.apache.org/docs/python/generated/pyarrow.register_extension_type.html?utm_source=chatgpt.com "pyarrow.register_extension_type — Apache Arrow v22.0.0"
[24]: https://arrow.apache.org/docs/8.0/python/extending_types.html?utm_source=chatgpt.com "Extending pyarrow — Apache Arrow v8.0.0"
[25]: https://arrow.apache.org/docs/python/generated/pyarrow.Schema.html "pyarrow.Schema — Apache Arrow v22.0.0"
[26]: https://arrow.apache.org/docs/python/generated/pyarrow.Field.html "pyarrow.Field — Apache Arrow v22.0.0"
[27]: https://arrow.apache.org/docs/python/interchange_protocol.html?utm_source=chatgpt.com "Dataframe Interchange Protocol — Apache Arrow v22.0.0"
[28]: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html?utm_source=chatgpt.com "pyarrow.Table — Apache Arrow v22.0.0"
[29]: https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html?utm_source=chatgpt.com "pyarrow.RecordBatch — Apache Arrow v22.0.0"
[30]: https://arrow.apache.org/docs/python/generated/pyarrow.interchange.from_dataframe.html?utm_source=chatgpt.com "pyarrow.interchange.from_dataframe — Apache Arrow v22.0.0"
[31]: https://arrow.apache.org/docs/python/extending_types.html?utm_source=chatgpt.com "Extending PyArrow — Apache Arrow v22.0.0"
[32]: https://arrow.apache.org/docs/python/memory.html?utm_source=chatgpt.com "Memory and IO Interfaces — Apache Arrow v22.0.0"
[33]: https://github.com/apache/arrow/issues/39030 "pyarrow.dataset.write_dataset do not preserve order · Issue #39030 · apache/arrow · GitHub"

Below is **additional PyArrow coverage** that (together with the last two PyArrow responses) fills in the remaining advanced surfaces that are most applicable to your codebase: **dataset discovery/factories + schema evolution**, **metadata-driven datasets**, **format-specific write options**, **Table/ChunkedArray primitives for determinism + memory accounting**, **Acero streaming outputs + richer node option semantics**, **compute-expression construction + options objects + checked variants**, **Parquet metadata introspection for pushdown diagnostics**, **filesystem plugin bridges**, and **IO primitives (mmap + compressed streams) for artifact packs**.

---

## 24) Dataset discovery as a controllable “ingestion compiler”: `FileSystemDatasetFactory` + `FileSystemFactoryOptions`

When your artifacts are written as **many Parquet files** (often partitioned), the most robust way to build a Dataset is to treat “discovery + schema inspection” as an explicit step.

### 24.1 `FileSystemDatasetFactory`: inspect first, then finish

`pyarrow.dataset.FileSystemDatasetFactory` is designed for:

* discovering files (from a list or a `FileSelector`)
* inspecting fragments to compute a **common schema**
* building a `Dataset` with either the inspected schema or an explicit schema. ([Apache Arrow][1])

Key APIs:

* `factory.inspect(promote_options='default'|'permissive', fragments=None)` → common Schema ([Apache Arrow][1])
* `factory.finish(schema=None)` → Dataset ([Apache Arrow][1])

`promote_options` meaning:

* **default**: types must match exactly (except nulls can merge)
* **permissive**: types are promoted when possible ([Apache Arrow][1])

### 24.2 Discovery and partition parsing controls: `FileSystemFactoryOptions`

When discovering from a `FileSelector` (directory crawl), `FileSystemFactoryOptions` provides critical controls: ([Apache Arrow][2])

* `exclude_invalid_files`
* `partition_base_dir`
* `partitioning` (overwrites `partitioning_factory`)
* `partitioning_factory`
* `selector_ignore_prefixes` (defaults include `'.'`, `'_'`) ([Apache Arrow][2])

### 24.3 Canonical “factory build” pattern (recommended)

```python
import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow.fs import FileSelector, LocalFileSystem

fs = LocalFileSystem()
selector = FileSelector("artifact_dir/", recursive=True)

fmt = ds.ParquetFileFormat()  # can also inject ParquetReadOptions/ScanOptions (see §15)

opts = ds.FileSystemFactoryOptions(
    partition_base_dir="artifact_dir/",
    exclude_invalid_files=True,
    selector_ignore_prefixes=[".", "_"],
    # partitioning_factory=ds.partitioning(schema=..., flavor="hive")  # see §27
)

factory = ds.FileSystemDatasetFactory(fs, selector, fmt, opts)

schema = factory.inspect(promote_options="default")  # or "permissive"
dataset = factory.finish(schema=schema)
```

This pattern is explicitly supported: selector/list discovery + inspect + finish. ([Apache Arrow][1])

---

## 25) Metadata-driven datasets: `parquet_dataset(_metadata)` as the fast path

You already covered writing `_metadata`; the missing half is **using it as an ingestion primitive**.

### 25.1 `pyarrow.dataset.parquet_dataset`

`pyarrow.dataset.parquet_dataset(metadata_path, ...)` creates a `FileSystemDataset` from a `_metadata` file created via `pyarrow.parquet.write_metadata`. ([Apache Arrow][3])

This is your best “large dataset bootstrap” path when:

* scanning all files to infer schema is too slow, and/or
* you want deterministic “dataset schema truth” embedded as an artifact.

### 25.2 Minimal pattern

```python
import pyarrow.dataset as ds

dataset = ds.parquet_dataset("artifact_dir/_metadata")
```

The function exists specifically for `_metadata` sidecars. ([Apache Arrow][3])

---

## 26) Format-specific write options in `write_dataset`: `file_options = FileFormat.make_write_options(...)`

You already treat `write_dataset` as the core materializer. The missing “best-in-class” lever is **format-specific FileWriteOptions**.

### 26.1 `write_dataset(..., file_options=...)`

`write_dataset` accepts `file_options: pyarrow.dataset.FileWriteOptions`, which are created using `FileFormat.make_write_options()`. ([Apache Arrow][4])

### 26.2 Parquet: `ParquetFileFormat.make_write_options`

`ParquetFileFormat` exposes `make_write_options(self, **kwargs)`. ([Apache Arrow][5])
(Exact kwargs are format-specific; you standardize and version them in your runtime profile.)

### 26.3 Practical usage pattern

```python
import pyarrow.dataset as ds

fmt = ds.ParquetFileFormat()
file_opts = fmt.make_write_options(...)  # e.g., compression, dictionary, stats, etc.

ds.write_dataset(
    data=reader,
    base_dir="out/",
    format="parquet",
    file_options=file_opts,
)
```

The linkage “file_options created using FileFormat.make_write_options” is explicit. ([Apache Arrow][4])

### 26.4 Supported formats for `write_dataset`

`write_dataset` documents format support including `"parquet"`, `"ipc"/"arrow"/"feather"`, and `"csv"`. ([Apache Arrow][4])

---

## 27) Partitioning as a first-class contract: `dataset.partitioning(...)` + factory wiring

### 27.1 The `partitioning(...)` helper exists explicitly

The dataset API exposes `partitioning([schema, field_names, flavor, ...])` to specify partitioning schemes. ([Apache Arrow][6])

### 27.2 Factory-level parsing vs dataset()-level parsing

* `ds.dataset(..., partition_base_dir=..., exclude_invalid_files=..., ignore_prefixes=...)` controls discovery directly in the high-level factory. ([Apache Arrow][7])
* `FileSystemFactoryOptions.partition_base_dir` and `selector_ignore_prefixes` control discovery when you’re using a `FileSelector`. ([Apache Arrow][2])

**Rule:** if you care about determinism and provenance, prefer explicit `FileSystemDatasetFactory` + `FileSystemFactoryOptions`.

---

## 28) Dataset-level “diagnostic terminals”: `count_rows`, `head`, `take` with pushdown and memory knobs

### 28.1 `Dataset.count_rows(...)` is a profiling tool

`Dataset.count_rows` accepts the same key scan knobs as a scanner (filter, batch_size, readahead, threads, cache_metadata, memory_pool) and will push down predicates using partition info or Parquet statistics when possible. ([Apache Arrow][8])

This is extremely useful for:

* “how big is this product” preflights,
* verifying filter selectivity without reading full columns,
* regression/perf tests.

### 28.2 `Dataset.head(...)` supports special projection fields

`Dataset.head` accepts `columns` as a list or dict of `{new_name: expression}` projections, and explicitly allows special fields `__batch_index`, `__fragment_index`, `__last_in_fragment`, `__filename`. ([Apache Arrow][8])

Use this as a “cheap provenance smoke test” artifact generator.

---

## 29) Table/ChunkedArray primitives: the in-memory fallback lane you should formalize

### 29.1 `Table.group_by(..., use_threads=...)` and determinism

`Table.group_by(keys, use_threads=True)` returns a grouping; output ordering is **not stable** when multithreading is enabled (explicitly noted). ([Apache Arrow][9])

**Policy:** if you use Table group_by for any canonical output, either:

* set `use_threads=False`, or
* add an explicit sort after aggregation.

### 29.2 `Table.join` / `Table.join_asof` mirror Dataset joins

`Table.join` supports join types including semi/anti joins and full outer joins, key coalescing, suffix controls, threading, and an optional filter expression. ([Apache Arrow][9])
`Table.join_asof` requires sorted inputs and is designed for nearest-key alignment. ([Apache Arrow][9])

This gives you an Arrow-native “small intermediate join” lane without invoking DataFusion.

### 29.3 Memory accounting: `get_total_buffer_size()` vs `nbytes`

* `get_total_buffer_size()` sums bytes of buffers referenced (may overestimate due to offsets; counts shared buffers only once). ([Apache Arrow][9])
* `nbytes` reports bytes consumed by elements (accounts for offsets differently; shared portions can be counted multiple times). ([Apache Arrow][9])

**Use both** in diagnostics: they answer different questions.

### 29.4 Chunk control and dictionary unification

* `Table.combine_chunks(memory_pool=None)` combines chunks into contiguous arrays (reduces chunk overhead). ([Apache Arrow][10])
* `ChunkedArray.unify_dictionaries(memory_pool=None)` unifies dictionaries across chunks (transposes indices). ([Apache Arrow][11])
* `Table.unify_dictionaries(...)` exists as a convenience for tables. ([Apache Arrow][9])

These are critical in your pipeline because:

* chunk explosion happens naturally in streaming writers/scanners,
* dictionary columns across fragments often diverge.

### 29.5 Schema evolution tools: `unify_schemas` + `concat_tables`

* `pyarrow.unify_schemas(schemas, promote_options=...)` merges fields by name; unified schema contains union of fields; merging different types fails by default, and metadata is inherited from first definition. ([Apache Arrow][12])
* `pyarrow.concat_tables(tables, promote_options=...)` concatenates tables; if `promote_options="none"`, schemas must match and it can be zero-copy; result shares metadata with the first table. ([Apache Arrow][13])

This is your canonical “schema evolution harness” for:

* unifying older/newer artifact schema revisions,
* building compatibility wrappers when schema drift occurs.

---

## 30) Acero in Python: make it truly streaming by default (`to_reader`) + richer node options

### 30.1 `Declaration.to_reader()` is the key output surface

`pyarrow.acero.Declaration` supports:

* `from_sequence(decls)` to build simple linear pipelines
* `to_reader(use_threads=True)` returning a `RecordBatchReader`
* `to_table(use_threads=True)` materializing into a Table ([Apache Arrow][14])

**Policy:** prefer `to_reader` for large outputs; it composes directly with `write_dataset`.

### 30.2 `use_threads` semantics (important)

For `to_table` (and by reference `to_reader`), `use_threads=False` forces CPU work onto the calling thread; IO tasks may still happen on the IO executor and may be multi-threaded but should not use significant CPU. ([Apache Arrow][14])

### 30.3 Node option semantics you should standardize

**Table source**

* `TableSourceNodeOptions(table)` is a source node over a `pyarrow.Table`. ([Apache Arrow][15])

**Filter**

* `FilterNodeOptions(filter_expression)` requires a `pyarrow.compute.Expression` returning boolean. ([Apache Arrow][16])

**Project**

* `ProjectNodeOptions(expressions, names=None)` evaluates expressions to produce same-length batches with new columns. ([Apache Arrow][17])

**Aggregate**

* `AggregateNodeOptions(aggregates, keys=None)` supports scalar and hash aggregates (GROUP BY). Aggregate specs are tuples: target column(s), function name, options object, output field name. Keys can be string names or expressions. ([Apache Arrow][18])

**Hash Join**

* `HashJoinNodeOptions(join_type, left_keys, right_keys, left_output=None, right_output=None, ... filter_expression=None)` is the hash join node options class. ([Apache Arrow][19])

**API stability note:** Acero’s Python API is explicitly marked experimental. ([Apache Arrow][20])

### 30.4 Minimal streaming Acero → dataset write

```python
import pyarrow as pa
import pyarrow.acero as acero
import pyarrow.compute as pc
import pyarrow.dataset as ds

src = acero.Declaration("table_source", acero.TableSourceNodeOptions(table))
filt = acero.Declaration("filter", acero.FilterNodeOptions(pc.field("a") > 0), [src])
proj = acero.Declaration("project", acero.ProjectNodeOptions([pc.field("a")], ["a"]), [filt])

reader = proj.to_reader(use_threads=True)
ds.write_dataset(reader, base_dir="out/", format="parquet")
```

`to_reader` is first-class, and filter expects a compute.Expression. ([Apache Arrow][14])

---

## 31) Compute expressions and options: `pc.field`, `pc.scalar`, options objects, and `_checked` variants

### 31.1 `pc.field(...)` vs `ds.field(...)`

* Dataset filters/projections use `pyarrow.dataset.field(*name_or_index)` and `pyarrow.dataset.scalar(value)` for dataset Expressions. ([Apache Arrow][21])
* Acero node options (filter/project) use `pyarrow.compute.Expression`, and PyArrow provides `pyarrow.compute.field(...)` for field references; it supports nested references via tuples or multiple names. ([Apache Arrow][16])

### 31.2 Options objects are serializable (good for reproducibility bundles)

Many compute options classes support `serialize()` / `deserialize()` (example: `RoundToMultipleOptions`). ([Apache Arrow][22])
This is ideal for “runtime profile capture”: you can persist the exact kernel options used in a run.

### 31.3 The “either options or params” calling convention

Compute functions often accept either:

* explicit parameters (e.g., `ndigits`, `round_mode`) **or**
* an `options=` object, plus `memory_pool=`. ([Apache Arrow][23])

### 31.4 Checked variants for correctness gates

Many arithmetic kernels have `_checked` variants that detect overflow and raise `ArrowInvalid`. ([Apache Arrow][24])
Use these when you treat overflow as a contract violation (e.g., if you encode stable IDs in smaller integer widths).

### 31.5 Struct packing with metadata/nullability: `MakeStructOptions`

`MakeStructOptions` lets you specify `field_names`, `field_nullability`, and `field_metadata` for `make_struct`. ([Apache Arrow][25])
This is your mechanism to preserve per-field semantics when packing normalized payload structs.

---

## 32) Parquet introspection for pushdown verification: `ParquetFile` + row-group statistics

### 32.1 `pyarrow.parquet.ParquetFile` for single-file deep inspection

`ParquetFile` is the reader interface for a single Parquet file and exposes controls like `memory_map`, `buffer_size`, and `pre_buffer` (plus ability to pass existing metadata). ([Apache Arrow][26])

Use it to:

* inspect row group counts/sizes,
* validate that statistics exist for key columns,
* debug why predicate pushdown is or isn’t effective.

### 32.2 Column statistics: `pyarrow.parquet.Statistics`

`Statistics` represents statistics for a column in a row group and provides `to_dict()` for easy structured logging. ([Apache Arrow][27])

---

## 33) Filesystem extensibility for artifact resolution: SubTree, PyFileSystem, FSSpecHandler

Your codebase is building artifact directories and may later resolve them through alternate stores. PyArrow’s filesystem stack is designed for this.

### 33.1 `SubTreeFileSystem` for “logical root” views

`SubTreeFileSystem(base_path, base_fs)` delegates to another FS after prepending a base path; docs explicitly warn it makes **no security guarantee** (symlinks may escape). ([Apache Arrow][28])

### 33.2 `PyFileSystem` + `FileSystemHandler`

You can implement a filesystem in Python by writing a `FileSystemHandler` and wrapping it in `PyFileSystem`. ([Apache Arrow][29])

### 33.3 `FSSpecHandler` bridge

`FSSpecHandler` wraps an fsspec filesystem and can be used via `PyFileSystem(FSSpecHandler(fsspec_fs))`. ([Apache Arrow][30])

This is a clean bridge when you already have fsspec-backed stores in your environment.

### 33.4 Discovery input: `FileSelector`

`FileSelector(base_dir, allow_not_found=False, recursive=False)` is the discovery object used by factories. ([Apache Arrow][31])

---

## 34) Memory + IO interfaces as “artifact pack” building blocks (mmap + compression)

### 34.1 Stream factories: `input_stream` / `output_stream`

* `pyarrow.input_stream(...)` creates an Arrow input stream over buffers or file paths. ([Apache Arrow][32])
* `pyarrow.output_stream(...)` creates an output stream, and supports on-the-fly compression selection (including `detect`) and `buffer_size`. ([Apache Arrow][33])

### 34.2 Memory-mapped IO (`memory_map` / `create_memory_map`)

* `pyarrow.memory_map(path, mode)` opens a memory map (size cannot change). ([Apache Arrow][34])
* `pyarrow.create_memory_map(path, size)` creates and memory-maps a new file. ([Apache Arrow][35])
  This is the “zero-copy read” primitive for large local artifacts.

### 34.3 Compression primitives: `Codec`, `compress`, `decompress`

* `pyarrow.Codec` exposes codec behavior and compression_level semantics per codec. ([Apache Arrow][36])
* `pyarrow.compress(buf, codec=..., memory_pool=...)` / `pyarrow.decompress(...)` are buffer-level compression utilities. ([Apache Arrow][37])

### 34.4 Compressed streams (for formats like CSV)

The Arrow cookbook notes that for formats without native compression (e.g., CSV), you can use `CompressedOutputStream` and later wrap reads with `CompressedInputStream`. ([Apache Arrow][38])

---

If you want, I can now **merge all three PyArrow parts** into a single repo-ready `pyarrow-execution-advanced.md` file and also generate:

1. a **runtime-profile schema** for Arrow knobs (thread pools, scan options, parquet read/write options, FS options), and
2. an auto-generated **approved kernel registry** (compute registry introspection + your registered scalar UDFs) using `pyarrow.compute.list_functions()` / `get_function()` / `call_function()` (which you already started leaning on).

[1]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.FileSystemDatasetFactory.html?utm_source=chatgpt.com "pyarrow.dataset.FileSystemDatasetFactory - Apache Arrow"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.FileSystemFactoryOptions.html?utm_source=chatgpt.com "pyarrow.dataset.FileSystemFactoryOptions - Apache Arrow"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.parquet_dataset.html?utm_source=chatgpt.com "pyarrow.dataset.parquet_dataset — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html?utm_source=chatgpt.com "pyarrow.dataset.write_dataset — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetFileFormat.html?utm_source=chatgpt.com "pyarrow.dataset.ParquetFileFormat — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/api/dataset.html?utm_source=chatgpt.com "Dataset — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.dataset.html?utm_source=chatgpt.com "pyarrow.dataset. - Apache Arrow"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html "pyarrow.dataset.Dataset — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html "pyarrow.Table — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html?utm_source=chatgpt.com "pyarrow.Table — Apache Arrow v22.0.0"
[11]: https://arrow.apache.org/docs/python/generated/pyarrow.ChunkedArray.html?utm_source=chatgpt.com "pyarrow.ChunkedArray — Apache Arrow v22.0.0"
[12]: https://arrow.apache.org/docs/python/generated/pyarrow.unify_schemas.html?utm_source=chatgpt.com "pyarrow.unify_schemas — Apache Arrow v22.0.0"
[13]: https://arrow.apache.org/docs/python/generated/pyarrow.concat_tables.html?utm_source=chatgpt.com "pyarrow.concat_tables — Apache Arrow v22.0.0"
[14]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.Declaration.html "pyarrow.acero.Declaration — Apache Arrow v22.0.0"
[15]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.TableSourceNodeOptions.html?utm_source=chatgpt.com "pyarrow.acero.TableSourceNodeOptions - Apache Arrow"
[16]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.FilterNodeOptions.html "pyarrow.acero.FilterNodeOptions — Apache Arrow v22.0.0"
[17]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.ProjectNodeOptions.html?utm_source=chatgpt.com "pyarrow.acero.ProjectNodeOptions — Apache Arrow v22.0.0"
[18]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.AggregateNodeOptions.html "pyarrow.acero.AggregateNodeOptions — Apache Arrow v22.0.0"
[19]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.HashJoinNodeOptions.html?utm_source=chatgpt.com "pyarrow.acero.HashJoinNodeOptions — Apache Arrow v22.0.0"
[20]: https://arrow.apache.org/docs/python/api/acero.html?utm_source=chatgpt.com "Acero - Streaming Execution Engine — Apache Arrow v22.0.0"
[21]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.field.html?utm_source=chatgpt.com "pyarrow.dataset.field — Apache Arrow v22.0.0"
[22]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.RoundToMultipleOptions.html?utm_source=chatgpt.com "pyarrow.compute.RoundToMultipleOptions - Apache Arrow"
[23]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.round.html?utm_source=chatgpt.com "pyarrow.compute.round — Apache Arrow v22.0.0"
[24]: https://arrow.apache.org/docs/python/api/compute.html?utm_source=chatgpt.com "Compute Functions — Apache Arrow v22.0.0"
[25]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.MakeStructOptions.html?utm_source=chatgpt.com "pyarrow.compute.MakeStructOptions — Apache Arrow v22.0.0"
[26]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html?utm_source=chatgpt.com "pyarrow.parquet.ParquetFile — Apache Arrow v22.0.0"
[27]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.Statistics.html?utm_source=chatgpt.com "pyarrow.parquet.Statistics — Apache Arrow v22.0.0"
[28]: https://arrow.apache.org/docs/python/generated/pyarrow.fs.SubTreeFileSystem.html?utm_source=chatgpt.com "pyarrow.fs.SubTreeFileSystem — Apache Arrow v22.0.0"
[29]: https://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystemHandler.html?utm_source=chatgpt.com "pyarrow.fs.FileSystemHandler — Apache Arrow v22.0.0"
[30]: https://arrow.apache.org/docs/python/generated/pyarrow.fs.FSSpecHandler.html?utm_source=chatgpt.com "pyarrow.fs.FSSpecHandler — Apache Arrow v22.0.0"
[31]: https://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSelector.html?utm_source=chatgpt.com "pyarrow.fs.FileSelector — Apache Arrow v22.0.0"
[32]: https://arrow.apache.org/docs/python/generated/pyarrow.input_stream.html?utm_source=chatgpt.com "pyarrow.input_stream — Apache Arrow v22.0.0"
[33]: https://arrow.apache.org/docs/python/generated/pyarrow.output_stream.html?utm_source=chatgpt.com "pyarrow.output_stream — Apache Arrow v22.0.0"
[34]: https://arrow.apache.org/docs/python/generated/pyarrow.memory_map.html?utm_source=chatgpt.com "pyarrow.memory_map — Apache Arrow v22.0.0"
[35]: https://arrow.apache.org/docs/python/generated/pyarrow.create_memory_map.html?utm_source=chatgpt.com "pyarrow.create_memory_map — Apache Arrow v22.0.0"
[36]: https://arrow.apache.org/docs/python/generated/pyarrow.Codec.html?utm_source=chatgpt.com "pyarrow.Codec — Apache Arrow v22.0.0"
[37]: https://arrow.apache.org/docs/python/generated/pyarrow.compress.html?utm_source=chatgpt.com "pyarrow.compress — Apache Arrow v22.0.0"
[38]: https://arrow.apache.org/cookbook/py/io.html?utm_source=chatgpt.com "Reading and Writing Data — Apache Arrow Python Cookbook ..."
