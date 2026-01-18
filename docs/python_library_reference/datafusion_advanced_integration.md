
# `datafusion-advanced.md` — DataFusion Python as a streaming-first, Arrow-native execution substrate

> **Target version**: `datafusion==51.0.0` (PyPI “Latest version”, released **Jan 9, 2026**). ([PyPI][1])
> **Core premise**: treat DataFusion as your *single-machine query engine kernel* and make your architecture **streaming-first**, **plan-introspectable**, and **configuration-versioned**.

---

## 0) Scope, versions, and invariants

### 0.1 What DataFusion Python is (and is not)

DataFusion Python is a set of Python bindings over the DataFusion query engine (Arrow-native, multi-threaded execution). The project positions the bindings as a foundation for building new data systems in Python, supporting SQL + DataFrame APIs, Python UDFs, Arrow/Pandas interop, and Substrait plan serialization. ([PyPI][1])

### 0.2 Architectural invariants to standardize in your repo

These invariants prevent “mystery performance” and nondeterministic outputs:

1. **Streaming-first**: Prefer record-batch streaming surfaces over `collect()` / `to_arrow_table()` for any non-trivial output. ([Apache DataFusion][2])
2. **One runtime profile**: Centralize `SessionConfig` + `RuntimeEnvBuilder` + “string config keys” into a versioned profile object. ([Apache DataFusion][3])
3. **Plan as an artifact**: Persist at least one of:

   * optimized logical plan (`optimized_logical_plan()`),
   * physical plan (`execution_plan()`),
   * Substrait bytes (via `datafusion.substrait.Serde`),
     per “graph product” build / rulepack. ([Apache DataFusion][2])
4. **Determinism tiering**: Make ordering and “canonical sorts” explicit; never assume stable ordering “for free”.

---

## 1) Mental model: logical plan → physical plan → execution surfaces

### 1.1 Lazy plans and terminal operations

A `DataFrame` is a *logical plan builder*. Transformations (select/filter/join/agg…) mutate the plan lazily; execution occurs when you call terminal methods like `collect()`, `show()`, `to_pandas()`, or when you stream batches. ([Apache DataFusion][4])

### 1.2 Three result surfaces (design around these)

Think of every query/ruleset as producing one of these surfaces:

#### (A) In-memory materialization

* `collect() -> list[pa.RecordBatch]` ([Apache DataFusion][2])
* `to_arrow_table() -> pa.Table` ([Apache DataFusion][2])
* `to_pandas() -> pd.DataFrame`, `to_polars() -> pl.DataFrame` ([Apache DataFusion][2])

Use for **small** products, debugging, golden snapshots.

#### (B) DataFusion-native streaming

* `execute_stream() -> RecordBatchStream` (single partition) ([Apache DataFusion][2])
* `execute_stream_partitioned() -> list[RecordBatchStream]` (one stream per partition) ([Apache DataFusion][2])
* Iteration: `for rb in df:` yields `datafusion.RecordBatch` lazily ([Apache DataFusion][2])

Use for **large** products or “pipe directly to writers”.

#### (C) Arrow C Stream (PyCapsule) export

* `DataFrame.__arrow_c_stream__(requested_schema=None) -> PyCapsule` emits Arrow C Stream; DataFusion executes incrementally (no full materialization). ([Apache DataFusion][2])

Use as the **universal interop** boundary: DF → PyArrow dataset writer, DF → Polars, DF → anything Arrow C Stream-capable.

---

## 2) Core API surface index (Python)

### 2.1 `SessionContext` — the “session kernel”

`SessionContext` is the main entrypoint for:

* creating DataFrames from sources (files, datasets, Arrow streams, pandas/polars),
* registering tables/views/UDFs,
* running SQL and retrieving tables. ([Apache DataFusion][3])

**Key creation signature**

* `SessionContext(config: SessionConfig | None = None, runtime: RuntimeEnvBuilder | None = None)` ([Apache DataFusion][3])

### 2.2 `DataFrame` — logical plan builder + execution wrapper

DataFrame supports:

* transformations (projection, filter, joins, unions, repartition, unnest),
* plan introspection (logical/optimized logical plan; physical plan),
* streaming and materialization,
* engine-native writes (CSV/JSON/Parquet + advanced parquet options). ([Apache DataFusion][2])

### 2.3 “Plan objects” — debug + serialization

* `LogicalPlan`: `display()`, `display_indent()`, `display_graphviz()`, `to_proto()` / `from_proto()` ([Apache DataFusion][5])
* `ExecutionPlan`: `display()`, `display_indent()`, `children()`, `partition_count`, `to_proto()` / `from_proto()` ([Apache DataFusion][5])

**Important limitation**: `to_proto()` / `from_proto()` note that “tables created in memory from record batches are currently not supported” for proto conversion. ([Apache DataFusion][5])

---

## 3) IO + table providers (scan path)

### 3.1 Read vs register (two equivalent families)

You can either:

* **read** a source and get a DataFrame (no named table):

  * `ctx.read_parquet(...)`, `ctx.read_csv(...)`, `ctx.read_json(...)`, `ctx.read_avro(...)` ([Apache DataFusion][3])
* **register** a source as a table, then query via SQL / `ctx.table(name)`:

  * `ctx.register_parquet(name, path, ...)`, `register_csv`, `register_json`, `register_avro` ([Apache DataFusion][3])

### 3.2 Parquet scan: advanced knobs that matter in real systems

From `read_parquet` / `register_parquet`, note the following parameters (your plan should treat these as policy knobs):

* `table_partition_cols`: declare partition columns (critical for pruning and stable partition semantics) ([Apache DataFusion][3])
* `parquet_pruning`: enable predicate-based row group pruning ([Apache DataFusion][3])
* `skip_metadata`: skip metadata embedded in file schema (helps avoid schema conflicts due to metadata) ([Apache DataFusion][3])
* `schema`: optionally provide an explicit schema (avoid inference + stabilize behavior) ([Apache DataFusion][3])
* `file_sort_order`: declare sort keys for files (enables “sorted file” assumptions; use with care) ([Apache DataFusion][3])

### 3.3 “Listing tables”: many files as one table

`register_listing_table(name, path, ..., file_extension, schema, file_sort_order)` registers a table that can assemble multiple files from an `ObjectStore`. ([Apache DataFusion][3])

This is your building block for:

* partitioned parquet datasets,
* “dataset directory” inputs,
* incremental rebuild across file fragments.

### 3.4 `register_dataset` and `read_table`

You can register a `pyarrow.dataset.Dataset` directly: `ctx.register_dataset(name, dataset)` and then query it. ([Apache DataFusion][3])
You can also build a DataFrame from a “table-like object” via `read_table(...)`. ([Apache DataFusion][3])

### 3.5 Object store integration (S3/GCS/Azure/HTTP/Local)

DataFusion supports several object store implementations and lets you register them into a session. Supported object store classes include `AmazonS3`, `GoogleCloud`, `Http`, `LocalFileSystem`, `MicrosoftAzure`. ([Apache DataFusion][6])

**Registering an object store**

* `ctx.register_object_store(schema: str, store: Any, host: str | None = None)` ([Apache DataFusion][3])

The DataFusion docs include an S3 example that registers an `AmazonS3` store and then registers parquet at an `s3://bucket/` path. ([Apache DataFusion][6])

---

## 4) Arrow interop + streaming execution (the “default best practice”)

### 4.1 Import any Arrow-capable dataframe via `from_arrow`

`SessionContext.from_arrow(data, name=None)` accepts any object implementing `__arrow_c_stream__` or `__arrow_c_array__` (the latter must return a struct array). The Arrow IO user guide explicitly lists pyarrow sources (Array/RecordBatch/RecordBatchReader/Table) and demonstrates importing a `pa.Table`. ([Apache DataFusion][3])

**Why this matters for your architecture**

* You can move batches between DF ⇄ PyArrow ⇄ Polars ⇄ pandas without “format wars”, using Arrow as the ABI.

### 4.2 Export a DataFusion plan as Arrow C Stream (`__arrow_c_stream__`)

`DataFrame.__arrow_c_stream__(requested_schema=None)`:

* runs the plan using streaming APIs,
* produces record batches incrementally (no full materialization),
* optionally aligns output to a requested schema **only for simple projections/reordering** (no rename/expressions/casts via this hook). ([Apache DataFusion][2])

**Recommended standard pattern**

* In your codebase, treat `__arrow_c_stream__` as the “canonical output contract” for large artifacts.

### 4.3 Native streaming in Python (RecordBatchStream)

`DataFrame.execute_stream()` returns a `RecordBatchStream` over a single partition; `execute_stream_partitioned()` returns one stream per partition. ([Apache DataFusion][2])

**When to use which**

* **single stream**: simplest “pipe to writer” flows, debug-friendly
* **partitioned**: exploit parallel downstream sinks (e.g., one parquet writer per partition)

### 4.4 DataFrame iteration (pull-based streaming)

DataFrame objects are iterable; iterating yields `datafusion.RecordBatch` lazily. ([Apache DataFusion][2])

**Interop note**: `datafusion.RecordBatch` wraps `pyarrow.RecordBatch` and exposes the Arrow C Data interface via `__arrow_c_array__`. ([Apache DataFusion][7])

---

## 5) Write path (engine-native materialization)

### 5.1 DataFrame write APIs (CSV/JSON/Parquet/table)

DataFrame includes:

* `write_csv(path, with_header=False, write_options=None)` ([Apache DataFusion][2])
* `write_json(path, write_options=None)` ([Apache DataFusion][2])
* `write_parquet(path, compression=..., compression_level=None, write_options=None)` ([Apache DataFusion][2])
* `write_parquet_with_options(path, options: ParquetWriterOptions, write_options=None)` ([Apache DataFusion][2])
* `write_table(table_name, write_options=None)` (provider-dependent; not all table providers support writing) ([Apache DataFusion][2])

### 5.2 `DataFrameWriteOptions` (format-agnostic “how to write”)

`DataFrameWriteOptions(insert_operation, single_file_output, partition_by, sort_by)` defines:

* `partition_by`: partition output by columns
* `sort_by`: global sort requirements for deterministic layout
* `single_file_output`: force single file output (usually reduces parallelism; use intentionally)
* `insert_operation`: `APPEND`, `OVERWRITE`, `REPLACE` (table write semantics) ([Apache DataFusion][2])

**Critical caveat**: docs note there is no guarantee a table provider supports all writer options. ([Apache DataFusion][2])

### 5.3 Parquet writer control plane (global + per-column)

#### `ParquetWriterOptions` (global file options)

`ParquetWriterOptions` exposes a large set of parquet knobs, including defaults for:

* `data_pagesize_limit`, `write_batch_size`, `writer_version`,
* `skip_arrow_metadata`,
* `compression` default (shown as `'zstd(3)'`), `statistics_enabled` default (`'page'`),
* row group sizing (`max_row_group_size`), parallel row group writers, buffering, bloom filters,
* per-column overrides via `column_specific_options`. ([Apache DataFusion][2])

#### `ParquetColumnOptions` (per-column overrides)

Per-column options include:

* `encoding` (valid values listed),
* `dictionary_enabled`,
* `compression` (including parameterized codecs like `gzip(level)`, `zstd(level)`),
* `statistics_enabled` (none/chunk/page),
* bloom filter toggles + FPP/NDV. ([Apache DataFusion][2])

#### Compression enum and constraints

DataFrame docs enumerate supported compression types and explicitly call out that LZO is excluded because it’s not implemented in `arrow-rs`. ([Apache DataFusion][2])

---

## 6) Configuration & runtime profile (version this)

DataFusion’s config surface in Python is a combination of:

* `RuntimeEnvBuilder` (runtime resources: memory pools, disk manager)
* `SessionConfig` (session config knobs + generic `set(key, value)` for arbitrary config keys)
* plus DataFusion’s global config catalog (documented in the core user guide). ([Apache DataFusion][3])

### 6.1 `RuntimeEnvBuilder` (runtime resources)

`RuntimeEnvBuilder` includes:

**Disk manager**

* `with_disk_manager_disabled()` (attempts to create temp files will error) ([Apache DataFusion][3])
* `with_disk_manager_os()` (use OS temp directory) ([Apache DataFusion][3])
* `with_disk_manager_specified(*paths)` (explicit spill dirs) ([Apache DataFusion][3])
* `with_temp_file_path(path)` (set temp file path) ([Apache DataFusion][3])

**Memory pools**

* `with_fair_spill_pool(size)` (for multiple spillable operators; may spill even if total memory could have avoided it because memory is reserved) ([Apache DataFusion][3])
* `with_greedy_memory_pool(size)` (good for “no spill” or “single spillable operator” workloads) ([Apache DataFusion][3])
* `with_unbounded_memory_pool()` ([Apache DataFusion][3])

**Design guidance for your repo**

* Treat spill policy as a “tier”:

  * *dev / deterministic*: greedy pool + explicit spill dirs
  * *large workloads*: fair spill pool + dedicated fast spill volume

### 6.2 `SessionConfig` (session behavior)

`SessionConfig` supports:

* typed helpers:

  * `with_batch_size(batch_size)`
  * `with_target_partitions(target_partitions)`
  * `with_parquet_pruning(enabled)`
  * `with_repartition_*` toggles for scans/joins/aggs/sorts/windows
* and a generic `set(key: str, value: str)` for arbitrary config keys. ([Apache DataFusion][3])

**Your runtime profile should always specify (at minimum):**

* `batch_size`
* `target_partitions`
* whether repartitioning for joins/aggs/sorts/windows is enabled
* parquet pruning on/off

### 6.3 Config key catalog (core DataFusion, used via `SessionConfig.set(...)`)

DataFusion documents a large set of configuration keys; relevant “advanced” families include:

**Optimizer: dynamic filter pushdown**

* `datafusion.optimizer.enable_join_dynamic_filter_pushdown`
* `datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown`
* `datafusion.optimizer.enable_topk_dynamic_filter_pushdown`
* `datafusion.optimizer.enable_dynamic_filter_pushdown` ([Apache DataFusion][8])

**Explain / diagnostics**

* `datafusion.explain.analyze_level` (`dev` vs `summary`)
* `datafusion.explain.format` (indent vs tree)
* schema display toggles ([Apache DataFusion][8])

**Runtime: list-files caching**
Upgrade guidance notes listing-table providers cache file lists and that you can set maximum cache size and expiration using:

* `datafusion.runtime.list_files_cache_limit` (bytes)
  (and related expiration options). ([Apache DataFusion][9])

> **Implementation rule**: the profile object you persist into artifacts should include *all* keys you set via `SessionConfig.set(...)`, because these affect plan shapes and performance.

---

## 7) Optimizer & planner interactions (design for the optimizer you have)

### 7.1 Dynamic filter pushdown: make it “want to work”

Dynamic filters allow DataFusion to derive runtime filters from one side of a join / TopK and push them into file scan to reduce IO. The optimizer exposes explicit toggles for join/aggregate/topk dynamic filter pushdown. ([Apache DataFusion][8])

**Design implications for your rule compilation**

* Push selective filters **early** (before joins).
* Prefer join patterns that make “small side” obvious (or physically small).
* Avoid rewriting into forms that hide join keys from the optimizer.

### 7.2 Repartitioning knobs and how they show up operationally

Python exposes session-level repartition toggles:

* `with_repartition_file_scans`, `with_repartition_joins`, `with_repartition_aggregations`, `with_repartition_sorts`, `with_repartition_windows` ([Apache DataFusion][3])

**Recommended approach**

* Default to **enabled** for performance, but allow per-ruleset overrides when you need deterministic behavior or when planning overhead dominates.

### 7.3 Plan introspection for “what the optimizer did”

Use:

* `df.logical_plan()` (unoptimized) and `df.optimized_logical_plan()` (post-optimizer) to see optimizer effects. ([Apache DataFusion][2])
* `df.execution_plan()` to inspect the physical plan. ([Apache DataFusion][2])

---

## 8) SQL APIs, safety gates, and parameterization

### 8.1 `sql()` and `sql_with_options()` are distinct

* `ctx.sql(query, options=None, param_values=None, **named_params)`
* `ctx.sql_with_options(query, options, param_values=None, **named_params)` validates query against the provided `SQLOptions`. ([Apache DataFusion][3])

The docs note that `sql()` supports DDL/DML with an in-memory default implementation and points to `sql_with_options`. ([Apache DataFusion][3])

### 8.2 `SQLOptions` (security + operational invariants)

`SQLOptions` includes:

* `with_allow_ddl(bool)`
* `with_allow_dml(bool)`
* `with_allow_statements(bool)` (SET/BEGIN/etc) ([Apache DataFusion][3])

**Best practice in an “agent-executed query system”**

* Use `sql_with_options` with strict options for any untrusted SQL surface.
* Allow DDL/DML only in tightly controlled code paths.

### 8.3 Parameter substitution model

`sql()` supports two substitution modes:

* `param_values`: “substitution of scalar values in the query after parsing”
* `named_params`: “string or DataFrame substitution in the query string” ([Apache DataFusion][3])

**Design implication**

* Treat `named_params` as “macro expansion” (powerful, riskier; must validate).
* Treat `param_values` as safer scalar parameterization.

---

## 9) UDFs / UDAFs / UDWFs / UDTFs (Python + Rust extension ladder)

### 9.1 Scalar UDFs (Python)

DataFusion Python scalar UDFs are Python functions that take one or more **PyArrow arrays** and return an output array; they operate on a **batch at a time**. ([Apache DataFusion][10])

From the API docs:

* `udf(func, input_types, return_type, volatility, name)` or decorator form
* If you have a Rust-backed `ScalarUDF` in a PyCapsule, you can pass it and skip other parameters (signature inferred from underlying function). ([Apache DataFusion][11])

**Volatility matters**
The UDF APIs reference a `Volatility` enum / allowed values; treat this as part of your determinism and caching contract. ([Apache DataFusion][11])

### 9.2 Aggregate UDFs (UDAF) via `Accumulator`

The `udaf(...)` constructor supports:

* passing an `Accumulator` factory/type,
* specifying `input_types`, `return_type`, `state_type`, `volatility`, `name`,
* and it also supports Rust-backed UDAFs via PyCapsule (`__datafusion_aggregate_udf__`). ([Apache DataFusion][11])

The docs’ example shows an `Accumulator` with `state()`, `update(values)`, `merge(states)`, `evaluate()` and factory patterns (type, named factory, lambda). ([Apache DataFusion][11])

### 9.3 Window UDFs (UDWF)

DataFusion supports registering window UDFs; in Python you register via `SessionContext.register_udwf(...)`. ([Apache DataFusion][3])
The DataFusion blog explains that window UDFs are more complex due to `OVER` clause semantics and recommends reviewing docs / advanced examples. ([Apache DataFusion][12])

### 9.4 Table functions (UDTF)

Python exposes `SessionContext.register_udtf(...)` to register user-defined table functions. ([Apache DataFusion][3])

### 9.5 Rust extension ladder via PyCapsule (FFI-based)

For “best-in-class” performance and robust integration, DataFusion Python’s contributor guide describes the PyCapsule-based FFI sharing approach:

* a provider library creates a `PyCapsule` containing an FFI structure (e.g., `FFI_TableProvider`),
* the receiving side (datafusion-python) reads it and wraps it,
* by convention the exporting Python object exposes it via `__datafusion_table_provider__`. ([Apache DataFusion][13])

The same convention pattern exists for UDFs/UDAFs/UDWFs (exportable protocols in the Python API). ([Apache DataFusion][11])

**Alternative approach** (not recommended long-term): directly depend on `datafusion-python` crate and couple builds tightly; the guide calls out version/compiler coupling concerns. ([Apache DataFusion][13])

---

## 10) Observability & introspection (make plans and metrics first-class)

### 10.1 `df.explain(verbose=False, analyze=False)`

`DataFrame.explain(verbose, analyze)` prints the plan so far; with `analyze=True` it runs and reports metrics. ([Apache DataFusion][2])

### 10.2 EXPLAIN ANALYZE verbosity tier (`datafusion.explain.analyze_level`)

DataFusion 51.0.0 introduces `datafusion.explain.analyze_level` with `summary` (concise) vs `dev` (full metrics output) and adds/expands operator metrics (e.g., `output_bytes`, filter selectivity). ([Apache DataFusion][14])

### 10.3 Optimizer debugging with EXPLAIN VERBOSE

The core DataFusion query optimizer guide describes using `EXPLAIN VERBOSE` to show the effect of each optimization rule. ([Apache DataFusion][15])

**Architecture recommendation**

* Always persist:

  * unoptimized logical plan,
  * optimized logical plan,
  * physical plan,
  * and (optionally) `EXPLAIN ANALYZE` output at a chosen analyze level,
    as artifacts keyed by your runtime profile + rulepack revision.

---

## 11) Substrait (plan serialization as an architecture primitive)

The `datafusion.substrait` module provides:

* `Serde.serialize(sql, ctx, path)` / `Serde.serialize_bytes(sql, ctx)` / `Serde.serialize_to_plan(sql, ctx)`
* `Serde.deserialize(path)` / `Serde.deserialize_bytes(bytes)`
* `Producer.to_substrait_plan(logical_plan, ctx)`
* `Consumer.from_substrait_plan(ctx, plan)`
* `Plan.encode() -> bytes` ([Apache DataFusion][16])

**Best-in-class uses in your system**

1. **Repro bundles**: persist Substrait bytes for every major build artifact.
2. **Cache keys**: compute stable fingerprints from serialized plans + runtime profile.
3. **Cross-engine experiments**: Substrait is the lingua franca for “one plan, multiple engines” scenarios (with compatibility caveats).

---

## 12) Unparsing plans back to SQL (debug + portability tooling)

The `datafusion.unparser` module supports converting DataFusion plans to SQL and exposes:

* `Dialect.default()/duckdb()/mysql()/postgres()/sqlite()`
* `Unparser.plan_to_sql()`
* `Unparser.with_pretty()` ([Apache DataFusion][17])

**Where this helps you**

* emitting human-readable “what will run” SQL for diagnostics,
* regression testing SQL shape stability (especially when integrating with SQLGlot rewriting elsewhere),
* generating minimal repro SQL for upstream issue reports.

---

## 13) Advanced integration patterns you should standardize (DataFusion-centric)

### 13.1 Streaming-first build artifact emission

**Goal**: avoid `collect()`/`to_arrow_table()` for big products.

* Use `execute_stream()` / `execute_stream_partitioned()` for batch streaming. ([Apache DataFusion][2])
* Or export `__arrow_c_stream__` and feed downstream Arrow-native writers/consumers. ([Apache DataFusion][2])

### 13.2 Treat catalogs/table providers as the extensibility spine

DataFusion supports default in-memory catalogs/schemas and allows adding memory catalogs/schemas; for advanced use cases you can implement catalogs in Rust (PyO3) or Python, and Rust implementations can yield significant performance improvements. ([Apache DataFusion][6])

### 13.3 Delta / Iceberg / custom formats as “table providers”

The DataFusion data sources guide documents:

* registering Delta Lake tables via `register_table` (newer deltalake versions), and warns that older “dataset interface” paths may lose pushdown features and cause significant performance differences, ([Apache DataFusion][6])
* registering Iceberg via custom table provider interface, requiring `pyiceberg` or `pyiceberg-core`. ([Apache DataFusion][6])

---

## 14) Failure-mode playbooks (toggle map)

### 14.1 OOM / spills / temp-file failures

**Symptoms**

* queries fail under memory pressure, or unexpectedly spill a lot.

**Levers**

* Ensure disk manager enabled and spill path set (`with_disk_manager_os` or `with_disk_manager_specified`). ([Apache DataFusion][3])
* Choose memory pool:

  * greedy pool for “simple” workloads,
  * fair spill pool for many spillable operators (but it may spill even when total memory could avoid it due to reservations). ([Apache DataFusion][3])

### 14.2 Nondeterministic output ordering

**Symptoms**

* identical inputs produce different row orders across runs.

**Levers**

* Enforce determinism explicitly:

  * use `sort_by` in `DataFrameWriteOptions` when writing, or canonical sort in your pipeline layer. ([Apache DataFusion][2])
* Avoid implicit assumptions about partitioned execution order.

### 14.3 Planner/optimizer regressions or unexpected plan shapes

**Symptoms**

* sudden performance cliff after upgrade or config change.

**Levers**

* Capture `optimized_logical_plan()` and compare across revisions. ([Apache DataFusion][2])
* Use `EXPLAIN VERBOSE` / `df.explain(analyze=True)` and adjust:

  * dynamic filter pushdown toggles, repartition toggles, batch_size/target_partitions. ([Apache DataFusion][8])

### 14.4 Schema conflicts in parquet reads

**Symptoms**

* schema mismatch errors on read, especially with embedded metadata.

**Levers**

* Use `skip_metadata=True` when metadata in file schema causes conflicts. ([Apache DataFusion][3])
* Provide explicit schema when you need contract stability. ([Apache DataFusion][3])

---

## Appendix A) “Minimum required runtime profile fields” (recommended)

If you persist only one config artifact, persist this:

1. `datafusion` version (e.g., 51.0.0) ([PyPI][1])
2. `SessionConfig`:

   * `batch_size`, `target_partitions` ([Apache DataFusion][3])
   * parquet pruning on/off ([Apache DataFusion][3])
   * repartition toggles (scans/joins/aggs/sorts/windows) ([Apache DataFusion][3])
   * any `set(key,value)` overrides ([Apache DataFusion][3])
3. `RuntimeEnvBuilder`:

   * disk manager mode + paths ([Apache DataFusion][3])
   * memory pool type + size ([Apache DataFusion][3])
4. Explain analyze level (`datafusion.explain.analyze_level`) if you collect metrics ([Apache DataFusion][14])

---


[1]: https://pypi.org/project/datafusion/ "datafusion · PyPI"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/python/user-guide/dataframe/index.html?utm_source=chatgpt.com "DataFrames — Apache Arrow DataFusion documentation"
[5]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"
[6]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[7]: https://datafusion.apache.org/python/autoapi/datafusion/record_batch/index.html?utm_source=chatgpt.com "datafusion.record_batch"
[8]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[9]: https://datafusion.apache.org/library-user-guide/upgrading.html?utm_source=chatgpt.com "Upgrade Guides — Apache DataFusion documentation"
[10]: https://datafusion.apache.org/python/user-guide/common-operations/udf-and-udfa.html?utm_source=chatgpt.com "User-Defined Functions - Apache DataFusion"
[11]: https://datafusion.apache.org/python/autoapi/datafusion/user_defined/index.html "datafusion.user_defined — Apache Arrow DataFusion  documentation"
[12]: https://datafusion.apache.org/blog/2025/04/19/user-defined-window-functions/?utm_source=chatgpt.com "User defined Window Functions in DataFusion"
[13]: https://datafusion.apache.org/python/contributor-guide/ffi.html "Python Extensions — Apache Arrow DataFusion  documentation"
[14]: https://datafusion.apache.org/blog/2025/11/25/datafusion-51.0.0/?utm_source=chatgpt.com "Apache DataFusion 51.0.0 Released"
[15]: https://datafusion.apache.org/library-user-guide/query-optimizer.html?utm_source=chatgpt.com "Query Optimizer — Apache DataFusion documentation"
[16]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html "datafusion.substrait — Apache Arrow DataFusion  documentation"
[17]: https://datafusion.apache.org/python/autoapi/datafusion/unparser/index.html "datafusion.unparser — Apache Arrow DataFusion  documentation"

## 15) Catalogs, schemas, tables, and views as the **extensibility spine**

### 15.1 Session-level catalog defaults + information schema

DataFusion’s config surface includes first-class catalog defaults and optional ISO `information_schema` tables. You can enable these either through `SessionConfig` helpers or via SQL `SET` (see §23). ([Apache DataFusion][1])

**SessionConfig helpers (Python)**

* `SessionConfig.with_create_default_catalog_and_schema(enabled=True)`
* `SessionConfig.with_default_catalog_and_schema(catalog, schema)`
* `SessionConfig.with_information_schema(enabled=True)` ([Apache DataFusion][2])

**Why you care in your repo**

* Your “graph product build” can treat catalogs/schemas as a *namespacing layer* for datasets by phase, run-id, or contract version.
* `information_schema` becomes a standardized introspection endpoint for your MCP/query surfaces.

### 15.2 Catalog discovery / traversal

`SessionContext` exposes catalog introspection:

* `ctx.catalog_names() -> set[str]`
* `ctx.catalog(name='datafusion') -> datafusion.catalog.Catalog` ([Apache DataFusion][2])

`Catalog` and `Schema` wrappers expose:

* `Catalog.schema_names() -> set[str]`, `Catalog.schema(name) -> Schema`
* `Schema.table_names() -> set[str]`, `Schema.table(name) -> Table`
* `Schema.register_table(name, table)` and `Schema.deregister_table(name)` ([Apache DataFusion][3])

### 15.3 `Table` as a unifying wrapper (Dataset / DataFrame view / FFI provider)

The `datafusion.catalog.Table` wrapper is explicitly designed to unify multiple backing implementations:

* built-in reads (CSV/Parquet/etc),
* `pyarrow.dataset.Dataset`,
* `datafusion.DataFrame` (converted into a **view**),
* externally-provided table providers via the PyCapsule/FFI interface. ([Apache DataFusion][3])

Useful properties:

* `Table.kind` (string “kind”)
* `Table.schema` (Arrow schema) ([Apache DataFusion][3])

Convenience:

* `Table.from_dataset(dataset: pyarrow.dataset.Dataset) -> Table` ([Apache DataFusion][3])

### 15.4 Views: `register_view` and `DataFrame.into_view`

You have *two* standard mechanisms:

**(A) Direct registration:**

* `ctx.register_view(name: str, df: DataFrame)` ([Apache DataFusion][2])

**(B) Convert then register:**

* `view_table = df.into_view(temporary: bool = False) -> datafusion.catalog.Table`
* `ctx.register_table("my_view", view_table)` ([Apache DataFusion][4])

Why both exist:

* `register_view` is the “obvious” API for named intermediate results.
* `into_view` is useful when you want “Table objects everywhere” (e.g., your internal registry stores only `Table` handles).

### 15.5 Custom catalogs/schemas in Python (and when to use Rust instead)

DataFusion Python supports catalogs written in Python **or** Rust (via PyO3 + PyCapsule/FFI). Rust catalogs are explicitly called out as a path to significant performance improvements over Python catalogs. ([Apache DataFusion][5])

**Python ABCs (minimum surface you must implement)**

* `datafusion.catalog.CatalogProvider`:

  * `schema(name) -> Schema | None`
  * `schema_names() -> set[str]`
  * optional: `register_schema`, `deregister_schema` ([Apache DataFusion][3])
* `datafusion.catalog.SchemaProvider`:

  * `table(name) -> Table | None`
  * `table_exist(name) -> bool`
  * `table_names() -> set[str]`
  * optional: `register_table`, `deregister_table`, `owner_name` ([Apache DataFusion][3])

**Registering a catalog provider**

* `ctx.register_catalog_provider(name: str, provider: CatalogProviderExportable | CatalogProvider | Catalog)` ([Apache DataFusion][2])

> Best-in-class pattern for your system: keep *Python providers* for dev-mode/experimentation, but plan a Rust-backed provider for production-level catalog operations where planning latency or cross-process correctness matters. ([Apache DataFusion][5])

---

## 16) In-memory ingestion + caching (MemTable path) and batch/partition semantics

### 16.1 Creating tables from RecordBatches (and why partition shape matters)

Two closely related APIs:

* `ctx.create_dataframe(partitions: list[list[pa.RecordBatch]], name=None, schema=None) -> DataFrame` ([Apache DataFusion][2])
* `ctx.register_record_batches(name: str, partitions: list[list[pa.RecordBatch]]) -> None` ([Apache DataFusion][2])

**Partition structure = execution parallelism input.**

* Outer list → partitions
* Inner list → batches inside a partition

### 16.2 `DataFrame.cache()` (third execution surface: “execute → buffer → new DF”)

The Python `DataFrame` API includes:

* `df.cache() -> DataFrame` (“Cache the DataFrame as a memory table.”) ([Apache DataFusion][4])

Operationally this is the “persist for reuse” knob:

* You pay execution once
* Future consumers read from an in-memory table

This pairs directly with parquet writer tuning that explicitly calls out higher parallel parquet writer settings being worthwhile “when writing out already in-memory data, such as from a cached data frame.” ([Apache DataFusion][1])

### 16.3 Large batches are not automatically sliced (you must manage batch sizing)

DataFusion is designed around “target batch size” processing, but there is a known footgun: MemTable provides the RecordBatches it was constructed with (no automatic splitting), which can hurt parallelism and memory behavior. ([GitHub][6])

**Best practice**

* When you build `partitions` for `register_record_batches` / `create_dataframe`, pre-slice big tables into batches that are roughly aligned with `datafusion.execution.batch_size` (default 8192 rows). ([Apache DataFusion][1])

Minimal pattern:

```python
import pyarrow as pa
from datafusion import SessionContext

ctx = SessionContext()

table = pa.table({"a": range(1_000_000), "b": range(1_000_000)})
batches = table.to_batches(max_chunksize=8192)

# single partition with many batches
ctx.register_record_batches("t", [batches])
df = ctx.table("t")
```

(Conceptually: you choose batch boundaries; DF will not “fix it later.”) ([Apache DataFusion][2])

### 16.4 Partition-preserving collection and targeted execution

For “debuggable determinism” and partition-aware sinks:

* `df.collect_partitioned() -> list[list[pa.RecordBatch]]` (preserves partitioning) ([Apache DataFusion][4])
* `ctx.execute(plan: ExecutionPlan, partitions: int) -> RecordBatchStream` (execute physical plan and stream results; useful for custom schedulers / per-partition sinks) ([Apache DataFusion][2])

---

## 17) DataFrame API “control plane” features that matter for rulepacks

These are the methods that make DataFusion usable as an internal compilation target (not just a SQL engine).

### 17.1 `parse_sql_expr`: convert SQL snippets into validated `Expr`

* `df.parse_sql_expr(expr: str) -> datafusion.expr.Expr` (“processed against the current schema”) ([Apache DataFusion][4])

Why it’s valuable:

* lets you accept *small* expression strings (from config / agent / rule DSL) while still producing typed `Expr` objects, validated against schema.

Example:

```python
from datafusion import col, lit
expr = df.parse_sql_expr("a > 1")
# equivalent to: col("a") > lit(1)
```

([Apache DataFusion][4])

### 17.2 Repartitioning primitives (explicit parallelism shaping)

* `df.repartition(num: int)` (round-robin repartition) ([Apache DataFusion][4])
* `df.repartition_by_hash(*exprs: Expr|str, num: int)` (hash partitioning by keys) ([Apache DataFusion][4])

Tie-in: DataFusion also has a physical-plan optimizer toggle to add round-robin repartitioning for parallelism:

* `datafusion.optimizer.enable_round_robin_repartition = true` (default true) ([Apache DataFusion][1])

Rulepack guidance:

* Use `repartition_by_hash(join_keys…)` before heavy joins/aggs when you need stable “shape” and predictable CPU scaling.
* Rely on optimizer round-robin repartitioning when you want a simpler profile-driven approach.

### 17.3 Join control: equality (optimized) vs inequality (supported)

* `df.join(right, on=..., how=..., coalesce_duplicate_keys=True)` supports `inner/left/right/full/semi/anti`. ([Apache DataFusion][4])
* `df.join_on(right, *on_exprs, how=...)` allows inequality predicates; docs explicitly note equality predicates are correctly optimized. ([Apache DataFusion][4])

Why you care:

* Your “interval containment” or “range-ish” relationships can live in DataFusion without dropping to raw SQL; but expect different plan shapes than pure equi-joins.

### 17.4 Unnest for nested/list columns

* `df.unnest_columns()` exists on the DataFrame API. ([Apache DataFusion][4])

This is the DataFusion-native equivalent of your list<…> explode lane (complementary to Arrow-kernel explode).

### 17.5 Convenience transforms for composable pipelines

Useful “pipeline ergonomics” methods:

* `df.with_column(name, expr)` / `with_columns(...)` / `with_column_renamed(...)` ([Apache DataFusion][4])
* `df.transform(fn)` (pattern: apply a function to a DF; good for standardized rewrite passes) ([Apache DataFusion][4])

---

## 18) Built-in functions (`datafusion.functions`) and expression construction (avoid UDFs)

### 18.1 Built-ins are the default “fast path”

DataFusion explicitly documents that it provides a large number of built-in functions and points users to the Functions API reference for the full set. ([Apache DataFusion][7])

### 18.2 `datafusion.functions` module

The Python API exposes many SQL functions as constructors that return `Expr` (plus helpers like `order_by` returning `SortExpr`). ([Apache DataFusion][8])

Examples visible in the API:

* window: `rank`, `percent_rank`, `ntile`
* regex: `regexp_like`, `regexp_count`, `regexp_match`
* sorting: `order_by(expr, ascending=True, nulls_first=True)` ([Apache DataFusion][8])

### 18.3 Discovery pattern (make this a dev tool)

Because you’re building LLM-facing catalogs, treat built-ins as “enumerable API”:

```python
import datafusion.functions as F
[f for f in dir(F) if not f.startswith("_")]
```

Then you can auto-generate a function catalog directly from Python (and cross-link to docs). ([Apache DataFusion][8])

---

## 19) Multi-file tables + ordering-aware scans (`register_listing_table`, file_sort_order)

### 19.1 `register_listing_table`: “dataset directory → one logical table”

* `ctx.register_listing_table(name, path, table_partition_cols=None, file_extension='.parquet', schema=None, file_sort_order=None)` ([Apache DataFusion][2])
  This registers a `Table` that can assemble multiple files from an `ObjectStore`. ([Apache DataFusion][2])

This is the backbone for your artifact directories (partitioned parquet datasets).

### 19.2 `file_sort_order` as a plan-shaping primitive (sort elimination + better joins)

Both `read_parquet`/`register_parquet` and `register_listing_table` accept `file_sort_order`, where each sort key may be a column name, `Expr`, or `SortExpr`. ([Apache DataFusion][2])

DataFusion’s SQL DDL also supports declaring known orderings:

* `CREATE EXTERNAL TABLE … WITH ORDER (col ASC, ...)` ([Apache DataFusion][9])

DataFusion has published a dedicated deep dive on using ordering information for better plans and how `WITH ORDER` is used to specify known orderings. ([Apache DataFusion][10])

**Why it matters to you**

* If your datasets are written with stable sort keys (e.g., `(stable_id, kind, ...)`), declaring ordering can eliminate redundant sorts and improve merge-like operations.

### 19.3 Directory scanning / partition inference knobs (Hive-style)

Relevant config keys (core DataFusion):

* `datafusion.execution.listing_table_ignore_subdirectory` (default true)
* `datafusion.execution.listing_table_factory_infer_partitions` (default true) ([Apache DataFusion][1])

Use-case:

* You want deterministic behavior for “artifact directory layouts” and to avoid accidental ingestion of subdirectories that aren’t part of a dataset.

### 19.4 Statistics + metadata fetch concurrency (planning latency)

Planning + inference knobs that matter for large multi-file datasets:

* `datafusion.execution.collect_statistics` (table creation time; default true for ListingTableProvider) ([Apache DataFusion][1])
* `datafusion.execution.meta_fetch_concurrency` (parallelism when inferring schema/stats) ([Apache DataFusion][1])

---

## 20) Parquet reader “power knobs”: page index, late materialization, bloom filters, and schema behavior

All of the following are core config keys (set via SQL `SET` or programmatically), and are the knobs you version inside your runtime profile. ([Apache DataFusion][1])

### 20.1 Page index, pruning, metadata fetch optimization

* `datafusion.execution.parquet.enable_page_index` (default true) reduces IO/decoded rows using Parquet page index if present. ([Apache DataFusion][1])
* `datafusion.execution.parquet.pruning` (default true) row-group pruning using min/max metadata. ([Apache DataFusion][1])
* `datafusion.execution.parquet.metadata_size_hint` (default 512KiB) optimistically fetches tail bytes to reduce round-trips. ([Apache DataFusion][1])
* `datafusion.execution.parquet.skip_metadata` helps avoid schema conflicts due to differing metadata. ([Apache DataFusion][1])

### 20.2 Late materialization: decode fewer rows by filtering during decode

* `datafusion.execution.parquet.pushdown_filters` (default false): apply filters during parquet decoding (“late materialization”). ([Apache DataFusion][1])
* `datafusion.execution.parquet.reorder_filters` (default false): reorder pushed-down filters heuristically for cheaper evaluation. ([Apache DataFusion][1])
* `datafusion.execution.parquet.force_filter_selections`: force RowSelections; otherwise DF chooses between RowSelection vs Bitmap based on patterns. ([Apache DataFusion][1])
* `datafusion.execution.parquet.max_predicate_cache_size`: cap memory used to cache predicate results between filter evaluation and output generation (0 disables caching). ([Apache DataFusion][1])

**When to enable pushdown_filters**

* When you have highly selective filters and expensive wide-column decoding (common in “scan huge table → filter → project small subset” rule patterns).

### 20.3 Bloom filters and string/binary interpretation knobs

* `datafusion.execution.parquet.bloom_filter_on_read` (default true) uses any available bloom filters. ([Apache DataFusion][1])
* `datafusion.execution.parquet.schema_force_view_types` reads Utf8/Binary columns using Utf8View/BinaryView types. ([Apache DataFusion][1])
* `datafusion.execution.parquet.binary_as_string`: treat binary as string for legacy writers missing UTF8 annotations. ([Apache DataFusion][1])
* `datafusion.execution.parquet.coerce_int96`: interpret Parquet INT96 timestamps with alternate resolution (Spark compatibility). ([Apache DataFusion][1])

### 20.4 Perfect hash join fast path (integer key range joins)

For joins with **very small integer key ranges / dense keys**, DataFusion can consider “perfect hash join”:

* `datafusion.execution.perfect_hash_join_small_build_threshold`
* `datafusion.execution.perfect_hash_join_min_key_density` ([Apache DataFusion][1])

This is relevant if you use compact integer IDs (stable ids) and often join on them with tight ranges (or small dense domains).

---

## 21) Parquet writing + output sharding control plane (beyond writer options)

You already have `ParquetWriterOptions` / `DataFrameWriteOptions` in Python; these **config keys** are the “system-level defaults / behavior constraints” that strongly affect performance and memory.

### 21.1 Parallel parquet writer tuning (especially after `df.cache()`)

Config keys:

* `datafusion.execution.parquet.maximum_parallel_row_group_writers` (default 1)
* `datafusion.execution.parquet.maximum_buffered_record_batches_per_stream` (default 2)

Docs explicitly note increasing these can improve performance when writing large parquet files if you have idle cores and can tolerate additional memory, and call out cached DataFrames as a good scenario. ([Apache DataFusion][1])

### 21.2 Output file parallelism and sizing (multi-file sinks)

Config keys:

* `datafusion.execution.minimum_parallel_output_files` (minimum number of output writers; round robin distribution)
* `datafusion.execution.soft_max_rows_per_output_file` (target rows per file)
* `datafusion.execution.max_buffered_batches_per_output_file` (memory vs throughput) ([Apache DataFusion][1])

Also:

* `datafusion.execution.keep_partition_by_columns` controls whether `partition_by` columns are kept in output RecordBatches. ([Apache DataFusion][1])

These are extremely relevant if you:

* stream large results to parquet,
* want consistent file sizing,
* or want predictable parallel writer behavior.

### 21.3 Remote object store write buffering

* `datafusion.execution.objectstore_writer_buffer_size` affects chunk sizes uploaded to remote stores (S3-like); docs warn very large output files may require increasing to avoid remote errors. ([Apache DataFusion][1])

---

## 22) Planner/execution orchestration and correctness gates

### 22.1 Planning fan-out for rulepacks with large `UNION` trees

* `datafusion.execution.planning_concurrency` controls fan-out during initial physical planning; docs call out parallel planning of `UNION` children. ([Apache DataFusion][1])

This is directly applicable if your compiler emits large “union-all-of-many-sources” plans.

### 22.2 Batch shaping knobs: coalesce and join batch-size enforcement

* `datafusion.execution.coalesce_batches` (default true) coalesces tiny batches between operators back toward target batch size; helps when selective filters/joins produce tiny batches. ([Apache DataFusion][1])
* `datafusion.execution.enforce_batch_size_in_joins` (default false) can reduce join memory usage with highly-selective join filters, but is slightly slower. ([Apache DataFusion][1])

### 22.3 Schema verification strictness (robustness vs planner workarounds)

* `datafusion.execution.skip_physical_aggregate_schema_check` (default false): when true, skips strict schema verification for planned aggregate inputs; intended as a workaround for planner bugs caught by the verification step. ([Apache DataFusion][1])

Best-in-class posture:

* Keep this **false** by default.
* Only enable in a narrowly scoped compatibility profile, and persist the flag in artifacts.

### 22.4 Spill mechanics beyond “enable disk manager”

Core spill knobs:

* `datafusion.execution.spill_compression` (`uncompressed`, `lz4_frame`, `zstd`; spill files are Arrow IPC Stream format so only IPC-supported codecs are allowed). ([Apache DataFusion][1])
* `datafusion.execution.sort_spill_reservation_bytes` (reserved memory per spillable sort for in-memory merge step) ([Apache DataFusion][1])
* `datafusion.execution.sort_in_place_threshold_bytes` (below this, concatenate and sort single batch instead of sort+merge) ([Apache DataFusion][1])
* `datafusion.execution.max_spill_file_size_bytes` (spill file rotation; currently noted as supported by `RepartitionExec` with other operators possibly exceeding the limit) ([Apache DataFusion][1])

### 22.5 ANSI mode (correctness strictness tier)

* `datafusion.execution.enable_ansi_mode` (default false) toggles stricter ANSI semantics for expressions/casting/error handling; docs describe stricter coercion and runtime errors on division by zero / invalid casts in ANSI mode. ([Apache DataFusion][1])

For your system:

* This is a **profile-tier** knob: enable in “strict correctness” runs, disable in “best-effort / permissive” runs (but always persist the choice).

---

## 23) SQL-layer features that are operationally valuable (even if you mainly use DataFrames)

### 23.1 `CREATE EXTERNAL TABLE` as a reproducible registration mechanism

DataFusion SQL supports `CREATE EXTERNAL TABLE … STORED AS <file_type> … PARTITIONED BY (…) … WITH ORDER (…) … OPTIONS (…) LOCATION …`. ([Apache DataFusion][9])

Why it matters:

* you can generate deterministic “table registration statements” from your dataset contracts,
* explicitly embed known ordering (`WITH ORDER`) in the registration step (ties back to §19.2). ([Apache DataFusion][10])

### 23.2 Prepared statements (`PREPARE` / `EXECUTE`) for repeated execution

DataFusion documents `PREPARE` statements that store a SQL statement with placeholders and can be executed repeatedly. ([Apache DataFusion][11])

Example syntax:

* `PREPARE name(TYPE, ...) AS SELECT ... WHERE a > $1;`
* `EXECUTE name(value, ...)` ([Apache DataFusion][11])

Where it fits:

* “same shape, different parameters” workloads (e.g., repeated diagnostics queries against different run IDs).

### 23.3 Information schema + SHOW commands for introspection

DataFusion supports metadata introspection via ISO `information_schema` views and DataFusion-specific `SHOW TABLES` / `SHOW COLUMNS`. ([Apache DataFusion][12])

To use `information_schema`, you must enable it (see §15.1; `datafusion.catalog.information_schema` config key is also documented). ([Apache DataFusion][1])

---

## 24) Convenience/ergonomics features: global context IO and URL tables

### 24.1 `datafusion.io.*` “read without explicitly creating a SessionContext”

The `datafusion.io` module provides `read_csv/read_json/read_parquet/read_avro` using a **global context**. ([Apache DataFusion][13])

Use-case:

* notebooks / quick tests / small tooling scripts.
* In your production system, you’ll usually want explicit contexts so your runtime profile is controlled.

### 24.2 `SessionContext.global_ctx()` (explicit access to the global context)

* `SessionContext.global_ctx() -> SessionContext` wraps the global internal context. ([Apache DataFusion][2])

### 24.3 URL tables (`enable_url_table`) to query local files by path

`SessionContext.enable_url_table()` enables using a local file path as a “table name”:

```python
import datafusion
ctx = datafusion.SessionContext().enable_url_table()
df = ctx.table("./examples/tpch/data/customer.parquet")
```

([Apache DataFusion][14])

**Important operational note**

* This is an ergonomics feature; in any untrusted-query surface, pair it with strict SQL options (DDL/DML gating) and avoid enabling it unless you control the input paths. ([Apache DataFusion][2])

---


[1]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/catalog/index.html "datafusion.catalog — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/python/user-guide/data-sources.html?utm_source=chatgpt.com "Data Sources — Apache Arrow DataFusion documentation"
[6]: https://github.com/apache/datafusion/issues/16717?utm_source=chatgpt.com "Better parallelize large input batches (speed up dataframe ..."
[7]: https://datafusion.apache.org/python/user-guide/common-operations/functions.html?utm_source=chatgpt.com "Functions — Apache Arrow DataFusion documentation"
[8]: https://datafusion.apache.org/python/autoapi/datafusion/functions/index.html?utm_source=chatgpt.com "datafusion.functions"
[9]: https://datafusion.apache.org/user-guide/sql/ddl.html?utm_source=chatgpt.com "DDL — Apache DataFusion documentation"
[10]: https://datafusion.apache.org/blog/2025/03/11/ordering-analysis/?utm_source=chatgpt.com "Using Ordering for Better Plans in Apache DataFusion"
[11]: https://datafusion.apache.org/user-guide/sql/prepared_statements.html?utm_source=chatgpt.com "Prepared Statements — Apache DataFusion documentation"
[12]: https://datafusion.apache.org/user-guide/sql/information_schema.html?utm_source=chatgpt.com "Information Schema — Apache DataFusion documentation"
[13]: https://datafusion.apache.org/python/autoapi/datafusion/io/index.html?utm_source=chatgpt.com "datafusion.io — Apache Arrow DataFusion documentation"
[14]: https://datafusion.apache.org/blog/2025/03/30/datafusion-python-46.0.0/?utm_source=chatgpt.com "Apache DataFusion Python 46.0.0 Released"

## 15) Join algorithm selection + join-planner control surface (hash vs sort-merge vs nested loop vs symmetric hash vs piecewise merge)

### 15.1 What join algorithms DataFusion can pick

DataFusion’s physical planner can choose among multiple join algorithms, based on **table statistics + join condition**: **Nested Loop Join**, **Sort Merge Join**, **Hash Join**, **Symmetric Hash Join**, and **Piecewise Merge Join (experimental)**. ([Apache DataFusion][1])

This matters for your codebase because your relationship builds are dominated by:

* **equi-joins on stable ids** (Hash Join / Sort Merge Join candidates),
* occasional **range-ish predicates** (Nested Loop / Piecewise Merge candidates),
* and potential future **stream-like** joins (Symmetric Hash Join candidates).

### 15.2 The join algorithm preference knobs you should treat as runtime-profile fields

DataFusion exposes explicit optimizer configs that influence join algorithm selection: ([Apache DataFusion][1])

* `datafusion.optimizer.prefer_hash_join` (bool, default true)

  * `true`: prefer HashJoin when memory is available
  * `false`: allow SortMergeJoin when more memory-efficient execution is needed ([Apache DataFusion][1])

* `datafusion.optimizer.enable_piecewise_merge_join` (bool, default false)

  * when enabled, planner may select PiecewiseMergeJoin **if there is exactly one range filter in the join condition**
  * faster than Nested Loop Join for “single range filter” joins except certain “two large ~equal size” cases described in docs ([Apache DataFusion][1])

* `datafusion.optimizer.allow_symmetric_joins_without_pruning` (bool, default true)

  * controls whether SymmetricHashJoin is allowed for unbounded sources without ordering/filtering
  * if disabled, SymmetricHashJoin can’t prune buffers and may only produce output at the end ([Apache DataFusion][1])

#### How to set these knobs (two canonical ways)

**(A) Programmatically via `SessionConfig.set(key, value)`**
`SessionConfig.set` exists specifically to set arbitrary config keys. ([Apache DataFusion][2])

```python
from datafusion import SessionConfig, SessionContext

config = (
    SessionConfig()
    .set("datafusion.optimizer.prefer_hash_join", "false")
    .set("datafusion.optimizer.enable_piecewise_merge_join", "true")
)

ctx = SessionContext(config=config)
```

(Values are strings; you standardize canonical “true/false” strings.)

**(B) Via SQL `SET …` (best when you treat runtime config as SQL artifacts)**
DataFusion documents controlling join selection using `SET datafusion.optimizer.<name>`. ([Apache DataFusion][1])

Use `sql_with_options` to allow statements but keep DDL/DML gated (see §17): ([Apache DataFusion][2])

```python
from datafusion import SessionContext, SQLOptions

ctx = SessionContext()
opts = (
    SQLOptions()
    .with_allow_statements(True)
    .with_allow_ddl(False)
    .with_allow_dml(False)
)

ctx.sql_with_options("SET datafusion.optimizer.prefer_hash_join = false", opts)
```

### 15.3 Practical join strategy guidance for your workloads

* **Hash Join**: usually fastest for equi-joins when build side fits in memory. Use as default for stable-id joins. ([Apache DataFusion][1])
* **Sort Merge Join**: often more memory-efficient, but requires sorted inputs (or sorts). Prefer when memory pressure is chronic; you can allow it by setting `prefer_hash_join=false`. ([Apache DataFusion][1])
* **Nested Loop Join**: the engine’s escape hatch for non-equi join conditions; your `join_on` with inequality predicates will tend to push you into this territory (expect different plan shapes). ([Docs.rs][3])
* **Piecewise Merge Join (experimental)**: explicitly aimed at “exactly one range filter” joins (common in interval containment / span-range conditions). Turn it on selectively per ruleset if you have lots of “one range predicate” joins. ([Apache DataFusion][1])
* **Symmetric Hash Join**: relevant when joining streams/unbounded sources; the pruning toggle (`allow_symmetric_joins_without_pruning`) exists because buffer growth is the core risk. ([Apache DataFusion][1])

### 15.4 Detecting what the planner chose (must be an artifact)

Persist physical plan text, then grep for the join operator name(s).

Plan objects expose display methods (logical plans also expose Graphviz; see §18). ([Apache DataFusion][4])

---

## 16) SQL-level write sinks: `COPY` / `INSERT` + the full Format Options system

### 16.1 `COPY` is a first-class file sink for *tables or queries*

DataFusion SQL supports `COPY` to write either a table or an arbitrary query to file(s), with formats `parquet`, `csv`, `json`, and `arrow`: ([Apache DataFusion][5])

```sql
COPY { table_name | query }
TO 'file_name'
[ STORED AS format ]
[ PARTITIONED BY column_name [, ...] ]
[ OPTIONS( option [, ... ] ) ]
```

([Apache DataFusion][5])

**Partitioned output semantics**: by default, columns used in `PARTITIONED BY` are removed from the output format. To keep them, enable `execution.keep_partition_by_columns true` (also available via execution options within `SessionConfig`). ([Apache DataFusion][5])

### 16.2 `INSERT` exists and is the other half of DML

DataFusion’s DML page documents `INSERT INTO table_name VALUES … | query`. ([Apache DataFusion][5])
If you plan to make any of your “product stores” writable via SQL, this is the canonical path.

### 16.3 Format Options: precedence + statement/table/session scoping

DataFusion’s **Format Options** system applies to `COPY`, `INSERT INTO`, and `CREATE EXTERNAL TABLE`. ([Apache DataFusion][6])

Options can be specified in **three ways**, in decreasing order of precedence: ([Apache DataFusion][6])

1. `CREATE EXTERNAL TABLE` syntax / `OPTIONS(...)` (table-level)
2. `COPY ... OPTIONS(...)` tuples (statement-level)
3. Session-level config defaults (profile-level)

This is architecturally important for you because it lets you decide:

* which knobs are **global profile defaults** (e.g., parquet row group sizing),
* which are **dataset contract defaults** (table-level),
* which are **per materialization call overrides** (statement-level).

### 16.4 Column-specific Parquet options (the “best-in-class” killer feature)

Format Options explicitly support column-specific settings using:

* `OPTION::COLUMN.NESTED.PATH` syntax, e.g. `compression::col1 'zstd(5)'`. ([Apache DataFusion][6])

Example from the docs: ([Apache DataFusion][6])

```sql
COPY source_table
  TO 'test/table_with_options'
  PARTITIONED BY (column3, column4)
  OPTIONS (
    format parquet,
    compression snappy,
    'compression::column1' 'zstd(5)',
  )
```

### 16.5 `COPY` for “deterministic sorted writes”

The DML docs show a pattern for writing an ordered query result to parquet while setting max row group size: ([Apache DataFusion][5])

```sql
COPY (SELECT * from source ORDER BY time)
TO 'output.parquet'
OPTIONS (MAX_ROW_GROUP_SIZE 10000000);
```

### 16.6 Running DML safely from Python (do this, always)

Python’s `SQLOptions` lets you explicitly allow/deny:

* DDL
* DML
* statements (SET/BEGIN/…) ([Apache DataFusion][2])

Minimal safe pattern for `COPY`:

```python
from datafusion import SessionContext, SQLOptions

ctx = SessionContext()
opts = SQLOptions().with_allow_dml(True).with_allow_ddl(False).with_allow_statements(False)

ctx.sql_with_options(
    "COPY my_table TO 'out_dir' STORED AS PARQUET",
    opts,
)
```

(`sql_with_options` validates the query against options before executing.) ([Apache DataFusion][2])

---

## 17) “Session state as artifact”: configuration discovery + statement gating patterns

### 17.1 `SQLOptions` is a security boundary (not just a convenience)

`SQLOptions` defaults allow DDL/DML/statements; you should never rely on defaults in any surface that might run agent-generated SQL. ([Apache DataFusion][2])

### 17.2 Programmatic config is a *functional dependency* of plan + output

Because session config keys directly influence join selection, scan behavior, pruning, and write output, your runtime profile must capture both:

* structured knobs (batch size, partitions, repartition toggles), and
* every raw `set(key, value)` you apply. ([Apache DataFusion][2])

---

## 18) Plan capture beyond “EXPLAIN”: Graphviz, plan objects, and DataFrame rehydration

### 18.1 `create_dataframe_from_logical_plan(plan)` enables plan caching workflows

`SessionContext.create_dataframe_from_logical_plan(plan)` exists explicitly to create a `DataFrame` from an existing logical plan. ([Apache Arrow][7])

**Use case in your system**

* compile rulepack → get optimized logical plan → persist it
* later: load plan → rehydrate DataFrame → execute (or stream) without rebuilding the plan tree in Python

### 18.2 Graphviz output for plan diffs and reviewer-grade artifacts

LogicalPlan supports `display_graphviz()` that emits DOT suitable for Graphviz tooling. ([Apache DataFusion][4])

Recommended artifact set per rulepack build:

* `logical_plan.display_indent_schema()` (schema-aware plan)
* `optimized_logical_plan.display_graphviz()` (structural plan diff)
* `physical_plan.display_indent()` (what will run)

### 18.3 Physical plan partition count (for downstream sink strategy)

`ExecutionPlan.partition_count` is exposed (and in Python docs). ([Apache DataFusion][4])
If you implement “partitioned streams → parallel writers”, this is the signal you wire into your sink planner.

---

## 19) Rust-backed extension surfaces you should design for (TableProvider + UDF packs)

### 19.1 Custom `TableProvider` via PyCapsule / FFI_TableProvider (the supported, ABI-stable approach)

The Python docs describe how to integrate a custom data source by implementing DataFusion’s `TableProvider` in Rust and exposing an `FFI_TableProvider` via `PyCapsule`, with a conventional `__datafusion_table_provider__` method. ([Apache DataFusion][8])

Minimal Rust-side sketch (from docs): ([Apache DataFusion][8])

```rust
#[pymethods]
impl MyTableProvider {
    fn __datafusion_table_provider__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_table_provider".into();
        let provider = Arc::new(self.clone());
        let provider = FFI_TableProvider::new(provider, false, None);
        PyCapsule::new_bound(py, provider, Some(name.clone()))
    }
}
```

Python-side registration: ([Apache DataFusion][8])

```python
from datafusion import SessionContext

ctx = SessionContext()
provider = MyTableProvider()
ctx.register_table("capsule_table", provider)
```

### 19.2 Why FFI/PyCapsule matters (don’t link `datafusion-python` as a Rust dependency)

The contributor guide explains why “just depend on `datafusion-python` crate” is fragile: Rust has no stable ABI, so wheels built under different toolchains/flags can become incompatible. The recommended approach is FFI/PyCapsule. ([Apache DataFusion][9])

### 19.3 If you want `INSERT INTO` support, your TableProvider must implement it

The DataFusion custom table provider guide notes: `TableProvider::insert_into` is used to `INSERT` data into the table. ([Apache DataFusion][10])
So: “writable artifact stores” in your system implies TableProvider work, not just parquet files.

### 19.4 External TableProviders ecosystem (useful, non-official)

There is an ecosystem repo (`datafusion-contrib/datafusion-table-providers`) that adds additional sources via TableProvider implementations. It’s not an ASF project, but it’s a real “accelerator” if you need more sources quickly. ([GitHub][11])

---

## 20) Constraints as metadata: what DataFusion enforces vs what you must enforce

### 20.1 DataFusion constraint model (informational, except nullability)

Table providers can describe constraints (primary keys, unique keys, foreign keys, check constraints), but DataFusion **does not enforce them at runtime**. ([Apache DataFusion][12])

What *is* enforced:

* **Field nullability**: returning nulls for non-nullable columns triggers runtime errors during execution. ([Apache DataFusion][12])

What is *not* enforced:

* primary/unique key satisfaction
* foreign keys
* check constraints (parsed but not validated / not used in planning) ([Apache DataFusion][12])

**Best practice for your repo**

* Keep your dataset contracts as the enforcement layer:

  * enforce uniqueness / FK-like invariants at materialization time (Arrow compute / DF queries)
  * optionally publish constraints into a TableProvider for *introspection*, but don’t rely on DataFusion to enforce them.

---

## 21) Full UDF breadth in Python: Scalar UDF, UDAF, UDWF, UDTF (and PyCapsule imports)

### 21.1 Registering all UDF types on `SessionContext`

`SessionContext` exposes explicit registrars for each UDF family: `register_udf`, `register_udaf`, `register_udwf`, `register_udtf`. ([Apache DataFusion][2])

### 21.2 Scalar UDF (Python) — vectorized over Arrow arrays

The UDF docs: a scalar UDF is a Python function that takes one or more **PyArrow arrays** and returns a single output array; DataFusion evaluates UDFs batch-at-a-time. ([Apache DataFusion][13])

From the API docs, `udf(...)` supports:

* function/decorator styles,
* or passing a Rust-backed ScalarUDF PyCapsule (`__datafusion_scalar_udf__`) and letting DF infer signature. ([Apache DataFusion][14])

### 21.3 UDAF (Python) — explicit `state_type` contract

`AggregateUDF.udaf(...)` requires:

* `input_types`, `return_type`, `state_type`, `volatility`, `name`. ([Apache DataFusion][14])
  This is essential if you want deterministic, serializable aggregation state (and is where you define stable “accumulator schema”).

### 21.4 UDWF (window UDF) — `WindowEvaluator.evaluate_all` is the key interface

Python exposes a real UDWF surface with a `WindowEvaluator` base class. The docs specify an `evaluate_all(values: list[pa.Array], num_rows: int) -> pa.Array` method to evaluate a window function for the entire partition. ([Apache DataFusion][14])

Minimal pattern (from the docs): ([Apache DataFusion][14])

```python
import pyarrow as pa
from datafusion.user_defined import WindowEvaluator, udwf

class BiasedNumbers(WindowEvaluator):
    def __init__(self, start: int = 0) -> None:
        self.start = start

    def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
        return pa.array([self.start + i for i in range(num_rows)])

udwf1 = udwf(BiasedNumbers, pa.int64(), pa.int64(), "immutable")
```

This is a big lever for “winner selection / ranking / stable tie-breakers” if you can express them as partition window computations.

### 21.5 UDTF (table functions) — produce table providers

Python exposes `TableFunction.udtf(...)` which creates a user-defined table function; table functions “generate new table providers based on the input expressions.” ([Apache DataFusion][14])

In practice, your UDTF returns something DataFusion can treat as a table provider (often Rust-backed via PyCapsule), and you register it via `ctx.register_udtf(...)`. ([Apache DataFusion][14])

### 21.6 Volatility is a first-class contract

The Python enum `Volatility` supports `immutable`, `stable`, `volatile` (string aliases). This should feed directly into your cache keys / “evaluate stable functions” policy. ([Apache DataFusion][14])

---

## 22) COPY / CREATE EXTERNAL TABLE / INSERT option layering as a design primitive

A key insight from Format Options is that:

* table-level options (from `CREATE EXTERNAL TABLE ... OPTIONS(...)`) propagate into subsequent `INSERT INTO ...` on that table, unless overridden. ([Apache DataFusion][6])
  So you can encode “dataset contract write options” into the external table definition, and rely on it during INSERT/COPY workflows.

---

## 23) Input plugins (`datafusion.input.*`) for custom “table resolution” pipelines

### 23.1 BaseInputSource: the extension interface

`datafusion.input.base.BaseInputSource` is an abstract base class with:

* `is_correct_input(input_item, table_name, **kwargs) -> bool`
* `build_table(input_item, table_name, **kwargs) -> datafusion.common.SqlTable` ([Apache DataFusion][15])

The docs state that once implemented, the plugin can be registered with the `SessionContext` to obtain `SqlTable` information from a custom datasource. ([Apache DataFusion][15])

### 23.2 LocationInputPlugin: default “file/remote location” input

`LocationInputPlugin` is the default input source “for everything” and can read from file locations (local/remote). ([Apache DataFusion][16])

### 23.3 How this maps to your repo (recommended usage)

This plugin system is the natural way to make DataFusion understand your **artifact references** (dataset contracts / run-bundle URIs / logical dataset names) without forcing everything into “paths and manual register_* calls”.

A practical pattern is:

* define a `BaseInputSource` subclass that recognizes your internal dataset handle (e.g., `ArtifactRef("relspec.edges@run=…")`)
* resolves it to the correct parquet directory + schema + file_sort_order + object store routing
* returns the `SqlTable` definition that DF uses to create the scan

> I did not find the concrete Python `SessionContext` method name for registering an input plugin in the auto-generated `SessionContext` API listing (unlike `register_udf/register_table/register_object_store`, which are explicit). The input plugin API is clearly documented as an extension surface, but you’ll likely confirm the exact registration hook by inspecting the current `datafusion` wheel source (it may be internal or exposed in a different module). ([Apache DataFusion][15])

---

## 24) Experimental “SQL over other DataFrame engines” (plan transpilation)

The project explicitly advertises experimental support for **transpiling SQL queries to DataFrame calls** with **Polars, Pandas, and cuDF**. ([PyPI][17])

This is relevant for your architecture mainly as:

* a reference for how others use DataFusion as the *optimizer/planner front-end* and execute elsewhere,
* and as a potential “fallback execution mode” in dev tooling (not a production default).

---

If you want to keep pushing “completeness” from here, the next best step is to turn **§15–§24** into an explicit “DataFusionRuntimeProfile schema” list (join configs, DML gating policy, COPY/format defaults, plan artifacts, UDF registry snapshots), so reviewers can see exactly which of these capabilities you’re actively using and which are merely supported.

[1]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[3]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/index.html?utm_source=chatgpt.com "datafusion::physical_plan::joins - Rust"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html?utm_source=chatgpt.com "datafusion.plan — Apache Arrow DataFusion documentation"
[5]: https://datafusion.apache.org/user-guide/sql/dml.html "DML — Apache DataFusion  documentation"
[6]: https://datafusion.apache.org/user-guide/sql/format_options.html "Format Options — Apache DataFusion  documentation"
[7]: https://arrow.staged.apache.org/datafusion-python/generated/datafusion.SessionContext.html "datafusion.SessionContext — Apache Arrow DataFusion  documentation"
[8]: https://datafusion.apache.org/python/user-guide/io/table_provider.html "Custom Table Provider — Apache Arrow DataFusion  documentation"
[9]: https://datafusion.apache.org/python/contributor-guide/ffi.html "Python Extensions — Apache Arrow DataFusion  documentation"
[10]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html?utm_source=chatgpt.com "Custom Table Provider — Apache DataFusion documentation"
[11]: https://github.com/datafusion-contrib/datafusion-table-providers?utm_source=chatgpt.com "datafusion-contrib/datafusion-table-providers"
[12]: https://datafusion.apache.org/library-user-guide/table-constraints.html "Table Constraint Enforcement — Apache DataFusion  documentation"
[13]: https://datafusion.apache.org/python/user-guide/common-operations/udf-and-udfa.html?utm_source=chatgpt.com "User-Defined Functions - Apache DataFusion"
[14]: https://datafusion.apache.org/python/autoapi/datafusion/user_defined/index.html "datafusion.user_defined — Apache Arrow DataFusion  documentation"
[15]: https://datafusion.apache.org/python/autoapi/datafusion/input/base/index.html "datafusion.input.base — Apache Arrow DataFusion  documentation"
[16]: https://datafusion.apache.org/python/autoapi/datafusion/input/location/index.html "datafusion.input.location — Apache Arrow DataFusion  documentation"
[17]: https://pypi.org/project/datafusion/25.0.0/?utm_source=chatgpt.com "DataFusion in Python"

Below is an explicit **`DataFusionRuntimeProfile` schema** that turns the **§15–§24 capability set** into a reviewable, versioned contract. It’s designed so reviewers can see (a) what you *actually use by default*, (b) what is *wired but optional*, and (c) what is merely *supported by DataFusion but not integrated yet*.

I’m writing this as a “drop-in spec” you can paste into your repo docs and then implement as a dataclass + serializer.

---

# DataFusionRuntimeProfile v1

## Status model (per feature group)

Use a single enum everywhere:

```text
FeatureStatus:
  - active        # used in the default production path
  - optional      # implemented + tested, but off by default / used in special runs
  - available     # DataFusion supports it, but you haven’t integrated it yet
  - disabled      # intentionally forbidden (security/correctness/perf reasons)
```

---

## Top-level schema

```yaml
DataFusionRuntimeProfile:
  schema_version: "datafusion_runtime_profile/v1"
  profile_name: str
  description: str | null

  versions:
    datafusion_python: str              # pinned wheel version
    datafusion_core: str | null         # optional if separately known
    pyarrow: str | null                 # optional but recommended for ABI debugging

  feature_status:
    catalogs_and_namespaces: FeatureStatus
    join_algorithm_controls: FeatureStatus
    sql_surface_and_gating: FeatureStatus
    copy_and_format_options: FeatureStatus
    plan_artifacts_and_introspection: FeatureStatus
    udf_registry_and_extensions: FeatureStatus
    custom_table_providers: FeatureStatus
    constraints_policy: FeatureStatus
    input_plugins_table_resolution: FeatureStatus
    experimental_sql_transpilation: FeatureStatus

  # 1) Catalogs / namespaces / information_schema
  catalogs:
    create_default_catalog_and_schema: bool
    default_catalog: str
    default_schema: str
    information_schema_enabled: bool

    # Optional: if you allow catalogs to be loaded from a default location/format using config keys.
    default_catalog_location: str | null
    default_catalog_format: str | null

  # 2) Optimizer + execution knobs (subset you commit to)
  optimizer:
    join_algorithms:
      prefer_hash_join: bool                       # HashJoin vs SortMergeJoin preference
      enable_piecewise_merge_join: bool            # experimental, single-range predicate joins
      allow_symmetric_joins_without_pruning: bool  # streaming/unbounded joins safety
      enable_round_robin_repartition: bool         # add parallelism when helpful

    dynamic_filter_pushdown:
      enable_dynamic_filter_pushdown: bool
      enable_join_dynamic_filter_pushdown: bool
      enable_aggregate_dynamic_filter_pushdown: bool
      enable_topk_dynamic_filter_pushdown: bool

    # Optional: additional optimizer safety/robustness knobs
    optimizer_resilience:
      skip_failed_rules: bool
      max_passes: int

  execution:
    batch_size: int
    target_partitions: int
    coalesce_batches: bool
    enforce_batch_size_in_joins: bool

    parquet_read:
      pruning: bool
      enable_page_index: bool
      skip_metadata: bool
      metadata_size_hint_bytes: int
      pushdown_filters: bool
      reorder_filters: bool
      force_filter_selections: bool
      max_predicate_cache_size_bytes: int | null
      bloom_filter_on_read: bool
      schema_force_view_types: bool
      binary_as_string: bool
      coerce_int96: bool | null

    listing_table:
      ignore_subdirectory: bool
      infer_partitions: bool
      collect_statistics: bool
      meta_fetch_concurrency: int

    output_writes:
      keep_partition_by_columns: bool
      minimum_parallel_output_files: int
      soft_max_rows_per_output_file: int
      max_buffered_batches_per_output_file: int
      objectstore_writer_buffer_size: int

    spill:
      enabled: bool                       # derived from runtime.disk_manager
      spill_compression: "uncompressed" | "lz4_frame" | "zstd"
      sort_spill_reservation_bytes: int
      sort_in_place_threshold_bytes: int
      max_spill_file_size_bytes: int

    planning:
      planning_concurrency: int

    correctness:
      enable_ansi_mode: bool
      skip_physical_aggregate_schema_check: bool

  # 3) Runtime environment (disk + memory pool)
  runtime:
    disk_manager:
      mode: "disabled" | "os_tmp" | "specified_paths"
      paths: [str]                         # only when specified_paths
      temp_file_path: str | null           # optional

    memory_pool:
      mode: "unbounded" | "greedy" | "fair_spill"
      size_bytes: int | null               # required for greedy/fair_spill

  # 4) SQL surface + safety gates (SQLOptions)
  sql_policy:
    # Define named “surfaces” in your system. Each surface gets explicit SQLOptions.
    # Default SQLOptions allow all; you MUST override for untrusted SQL. 
    surfaces:
      internal_sql:
        allow_ddl: bool
        allow_dml: bool
        allow_statements: bool
      untrusted_sql:
        allow_ddl: bool
        allow_dml: bool
        allow_statements: bool
      admin_sql:
        allow_ddl: bool
        allow_dml: bool
        allow_statements: bool

    sql_api:
      use_sql_with_options: bool
      param_values_enabled: bool
      named_params_enabled: bool

    parser:
      dialect: str | null                  # e.g., "generic", "postgres", ...
      normalize_options: bool | null        # if you rely on it

  # 5) COPY / INSERT sinks + Format Options layering
  dml_writes:
    copy:
      enabled: bool
      default_format: "parquet" | "csv" | "json" | "arrow" | null
      default_partitioned_by_drop_columns: bool     # mirrors COPY semantics; can be offset by keep_partition_by_columns
      statement_options:
        parquet:
          compression: str | null
          max_row_group_size: int | null
          statistics_enabled: "none"|"chunk"|"page"|null
          # Column overrides use the format_options "compression::col_path" style:
          column_overrides:
            # key: "compression::col1" or nested: "compression::struct_field.subfield"
            str: str
        csv:
          has_header: bool | null
          delimiter: str | null
        json:
          # add if you standardize JSON write options
          placeholder: null

    insert_into:
      enabled: bool
      allowed_table_kinds: [str]            # e.g., ["table_provider:writable", "memtable", ...]
      require_table_provider_insert_support: bool

  # 6) Artifact emission (plans, metrics, substrait, unparser)
  artifacts:
    plans:
      capture_logical_plan: bool
      capture_optimized_logical_plan: bool
      capture_physical_plan: bool
      capture_plan_protobuf: bool
      capture_graphviz_dot: bool
      capture_partition_count: bool

    explain:
      capture_explain: bool
      analyze: bool
      analyze_level: "summary" | "dev" | null

    substrait:
      capture_substrait_bytes: bool
      capture_substrait_plan_object: bool
      include_sql_text_used_for_serde: bool

    unparser:
      capture_unparsed_sql: bool
      dialect: "default"|"duckdb"|"mysql"|"postgres"|"sqlite"|null
      pretty: bool

    runtime_snapshots:
      capture_effective_config: bool
      capture_registered_tables: bool
      capture_catalog_tree: bool

  # 7) UDF registry snapshot (scalar/agg/window/table)
  udfs:
    snapshot_enabled: bool
    functions:
      - name: str
        kind: "scalar" | "aggregate" | "window" | "table"
        volatility: "immutable" | "stable" | "volatile"
        input_types: [str]                 # serialize Arrow types as strings
        return_type: str
        state_type: str | null             # for UDAF
        implementation:
          mode: "python" | "pycapsule"     # DF supports PyCapsule import for UDFs
          symbol: str | null               # module:function path for python
          capsule_attr: str | null         # e.g. "__datafusion_scalar_udf__"
        notes: str | null

  # 8) Extensions: catalogs, table providers, object stores
  extensions:
    catalog_providers:
      - name: str
        kind: "python" | "pycapsule"
        export_attr: "__datafusion_catalog_provider__" | null
        status: FeatureStatus
    table_providers:
      - name: str
        kind: "pycapsule"
        export_attr: "__datafusion_table_provider__"
        supports_insert_into: bool
        constraints_advertised: bool
        status: FeatureStatus

    object_stores:
      - scheme: str                  # "s3", "gs", "az", "http", "file"
        host: str | null
        type: "AmazonS3"|"GoogleCloud"|"MicrosoftAzure"|"Http"|"LocalFileSystem"
        config: { str: str }         # credentials/endpoint/etc
        status: FeatureStatus

  # 9) Constraints policy (what you enforce, since DF mostly doesn’t)
  constraints:
    nullability_enforced_by_datafusion: true
    primary_unique_keys_enforced_by_datafusion: false
    checks_enforced_by_datafusion: false
    your_enforcement:
      enforce_primary_keys_at_materialization: bool
      enforce_unique_keys_at_materialization: bool
      enforce_foreign_keys_at_materialization: bool
      enforce_check_constraints_at_materialization: bool
      enforcement_queries_or_ruleset: str | null

  # 10) Input plugins / table resolution hooks
  input_plugins:
    enabled: bool
    sources:
      - name: str
        class_path: str
        status: FeatureStatus
        config: { str: str }

  # 11) Experimental SQL transpilation (DF-as-planner front-end)
  transpilation:
    enabled: bool
    targets: ["polars"|"pandas"|"cudf"]
    status: FeatureStatus
```

---

# Mapping each section to “how you implement it” in DataFusion Python

## Catalogs / namespaces

* `SessionConfig.with_create_default_catalog_and_schema`, `with_default_catalog_and_schema`, `with_information_schema`, plus generic `set(key,value)` for other keys. ([Apache DataFusion][1])
* Introspection: `SessionContext.catalog_names()`, `catalog()` etc (useful for artifact snapshots). ([Apache DataFusion][1])

## Join algorithm controls

* Join algorithm list + the specific optimizer keys `prefer_hash_join`, `enable_piecewise_merge_join`, `allow_symmetric_joins_without_pruning` (and how they influence selection) are documented in DataFusion’s config reference. ([Apache DataFusion][2])
* Apply via `SessionConfig.set("datafusion.optimizer.prefer_hash_join", "true|false")`, etc. `SessionConfig.set` is explicitly part of the Python API contract. ([Apache DataFusion][1])

## SQL safety gates (SQLOptions)

* `SQLOptions` defaults allow DDL/DML/statements; you explicitly set them per “surface” via `with_allow_ddl`, `with_allow_dml`, `with_allow_statements`. ([Apache DataFusion][1])
* Use `sql_with_options()` for any surface that isn’t “trusted internal”. ([Apache DataFusion][1])
* Parameter substitution semantics (`param_values` vs `named_params`) are part of the `sql()` API contract. ([Apache DataFusion][3])

## COPY / Format Options defaults

* `COPY … TO … [STORED AS] [PARTITIONED BY] [OPTIONS(...)]` is first-class SQL DML in DataFusion. ([Apache DataFusion][4])
* Column-specific parquet options use the `compression::col_path` style (and more generally: “format options” layering). ([Apache DataFusion][5])
* “Partition columns dropped by default unless configured otherwise” ties directly to `execution.keep_partition_by_columns` (captured in your `execution.output_writes`). ([Apache DataFusion][4])

## Plan artifacts and introspection

* `DataFrame.logical_plan()`, `optimized_logical_plan()`, `execution_plan()`, and streaming outputs like `execute_stream_partitioned()` and `__arrow_c_stream__` are documented on the DataFrame API. ([Apache DataFusion][6])
* `DataFrame.cache()` and `collect_partitioned()` are explicit APIs—use them to choose “persist vs stream” and to preserve partitioning in debug artifacts. ([Apache DataFusion][6])
* Physical plan proto / partition count: `ExecutionPlan.to_proto()` + `partition_count` are part of the plan API; proto has limitations (in-memory tables from record batches aren’t supported). ([Apache DataFusion][7])
* Substrait: `datafusion.substrait.Serde` / `Producer` / `Consumer` exist as Python APIs for plan serialization. ([Apache DataFusion][8])
* Unparsing: `datafusion.unparser.Unparser` + dialect selection exist as Python APIs. ([Apache DataFusion][9])

## UDF registry snapshot

* Scalar UDF definition model (“takes one or more PyArrow arrays and returns a single array”) is documented. ([Apache DataFusion][10])
* Volatility enum values (`immutable`, `stable`, `volatile`) are documented and should be captured in your snapshot schema. ([Apache DataFusion][11])
* Session-level registration APIs include `register_udf`, `register_udaf`, `register_udwf`, `register_udtf`. ([Apache DataFusion][1])

## Custom table providers and FFI

* Custom TableProvider via PyCapsule / `FFI_TableProvider` is the documented best practice for DataFusion Python (and requires DF ≥ 43.0.0). ([Apache DataFusion][12])

## Constraints policy

* DataFusion enforces **nullability** at runtime but does **not** enforce primary/unique keys, etc.; table providers must enforce themselves. ([Apache DataFusion][13])

## Input plugins / table resolution

* `BaseInputSource` and `LocationInputPlugin` exist as the “input sources” plugin system for resolving tables from custom inputs, and docs state such plugins can be registered with `SessionContext`. ([Apache DataFusion][14])

  * (Implementation detail you’ll codify in your repo: where/how you register these plugins; your profile keeps the registry spec regardless.)

## Experimental SQL transpilation

* DataFusion Python advertises **experimental transpilation** of SQL to Polars/Pandas/cuDF operations; your profile should record this as `available/disabled` unless you intentionally use it. ([PyPI][15])

---

# Minimal “apply profile” contract (what your implementation should do)

A reviewer should be able to check that your runtime builder does these steps (order matters):

1. Build `RuntimeEnvBuilder` from `profile.runtime` (disk manager + memory pool). ([Apache DataFusion][1])
2. Build `SessionConfig` from typed setters + `set(key,value)` allowlist for the remainder. ([Apache DataFusion][1])
3. Create `SessionContext(config, runtime)`. ([Apache DataFusion][1])
4. Register object stores from `profile.extensions.object_stores`. ([Apache DataFusion][16])
5. Register catalogs + table providers + UDF packs (in that order). ([Apache DataFusion][1])
6. For each materialization/query surface: run SQL via `sql_with_options(surface_policy)` (never rely on default SQLOptions). ([Apache DataFusion][1])
7. Emit artifacts based on `profile.artifacts.*` using the DataFrame/plan/substrait/unparser APIs. ([Apache DataFusion][6])
8. Emit a `udfs.snapshot` and `runtime_snapshots` (catalog tree, registered tables, effective config). ([Apache DataFusion][1])

---


[1]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html?utm_source=chatgpt.com "datafusion.context — Apache Arrow DataFusion documentation"
[4]: https://datafusion.apache.org/user-guide/sql/dml.html?utm_source=chatgpt.com "DML — Apache DataFusion documentation"
[5]: https://datafusion.apache.org/user-guide/sql/format_options.html?utm_source=chatgpt.com "Format Options — Apache DataFusion documentation"
[6]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[7]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html?utm_source=chatgpt.com "datafusion.plan — Apache Arrow DataFusion documentation"
[8]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html?utm_source=chatgpt.com "datafusion.substrait"
[9]: https://datafusion.apache.org/python/autoapi/datafusion/unparser/index.html?utm_source=chatgpt.com "datafusion.unparser"
[10]: https://datafusion.apache.org/python/user-guide/common-operations/udf-and-udfa.html?utm_source=chatgpt.com "User-Defined Functions - Apache DataFusion"
[11]: https://datafusion.apache.org/python/autoapi/datafusion/user_defined/index.html "datafusion.user_defined — Apache Arrow DataFusion  documentation"
[12]: https://datafusion.apache.org/python/user-guide/io/table_provider.html?utm_source=chatgpt.com "Custom Table Provider - Apache DataFusion"
[13]: https://datafusion.apache.org/library-user-guide/table-constraints.html?utm_source=chatgpt.com "Table Constraint Enforcement - Apache DataFusion"
[14]: https://datafusion.apache.org/python/autoapi/datafusion/input/base/index.html?utm_source=chatgpt.com "datafusion.input.base"
[15]: https://pypi.org/project/datafusion/31.0.0/?utm_source=chatgpt.com "DataFusion in Python"
[16]: https://datafusion.apache.org/python/autoapi/datafusion/object_store/index.html?utm_source=chatgpt.com "datafusion.object_store"


