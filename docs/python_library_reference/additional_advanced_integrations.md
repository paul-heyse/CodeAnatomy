Below are **additional advanced integration capabilities** (with implementation-ready patterns) that complete the “best-in-class integration playbook” across **DataFusion (Python) + Ibis + SQLGlot + PyArrow**, beyond what we already covered.

---

## 15) Ordering metadata handshake across write → register → plan selection

This is the “make sortedness useful” end-to-end path:

* **PyArrow writes** *declared sort order* into Parquet row group metadata via `sorting_columns` (it does *not* sort for you). ([Apache Arrow][1])
* **DataFusion consumes/assumes ordering** when you declare it on registration/DDL (`WITH ORDER` or `file_sort_order`) and can use it for better plans (ordering analysis). ([Apache DataFusion][2])
* **SQLGlot/Ibis enforce determinism** by making the sort keys explicit in the IR and by ensuring “ordering tier” is captured as an artifact.

### 15.1 Write-time declaration: PyArrow `ParquetWriter.sorting_columns`

`pyarrow.parquet.ParquetWriter` supports `sorting_columns`; docs explicitly say: “The writer does not sort the data nor verify that it is sorted. The sort order is written to the row group metadata.” ([Apache Arrow][1])

**Pattern**

1. You **sort upstream** (Tier-3 canonical sort) or otherwise guarantee order.
2. You write Parquet and declare the sort order:

```python
import pyarrow.parquet as pq

writer = pq.ParquetWriter(
    "part-0.parquet",
    schema,
    sorting_columns=[pq.SortingColumn(column_index=0, descending=False, nulls_first=False)],
    store_schema=True,
)
writer.write_table(sorted_table)
writer.close()
```

(See `SortingColumn` docs: it’s used by `ParquetWriter` and exposed by `RowGroupMetaData.sorting_columns()`.) ([Apache Arrow][3])

### 15.2 Read/registration-time declaration: DataFusion `WITH ORDER` and `file_sort_order`

DataFusion SQL DDL documents `CREATE EXTERNAL TABLE ... WITH ORDER (...)`. ([Apache DataFusion][2])
DataFusion Python registration APIs also support `file_sort_order` when registering parquet. ([Apache DataFusion][4])

**Two ways to declare ordering to DataFusion**

**A) DDL** (external tables):

```sql
CREATE EXTERNAL TABLE edges (
  stable_id BIGINT NOT NULL,
  kind VARCHAR NOT NULL,
  ...
)
STORED AS PARQUET
WITH ORDER (stable_id ASC, kind ASC)
LOCATION '.../edges/'
```

(DDL syntax supports WITH ORDER.) ([Apache DataFusion][2])

**B) Python registration**
`register_parquet(..., file_sort_order=...)` accepts per-key sort specs. ([Apache DataFusion][4])

### 15.3 Important hazard: complex expressions in `WITH ORDER`

DataFusion docs show complex expressions in `WITH ORDER` (e.g., `c5 + c8`), but an open issue reports that complex expressions can error (“Expected single column reference…”). ([Apache DataFusion][5])

**Safe practice**

* Restrict `WITH ORDER` to **simple column references**.
* If you need `c5 + c8`, materialize it as a real column (e.g., `c5_plus_c8`) and order by that.

### 15.4 Observability hook: verify declared ordering exists in Parquet metadata

Use `RowGroupMetaData.to_dict()` and `sorting_columns()` to confirm that order metadata exists for each row group. ([Apache Arrow][6])

---

## 16) Contract-driven table registration and format options across engines

This fills in a missing “integration surface”: generating DataFusion registration DDL from your **schema contracts**, and controlling read/write behavior using DataFusion’s **Format Options** system.

### 16.1 Generate DataFusion `CREATE EXTERNAL TABLE` DDL from contracts

DataFusion DDL syntax is explicit: STORED AS, PARTITIONED BY, WITH ORDER, OPTIONS, LOCATION. ([Apache DataFusion][2])

Your contract bridge is:

* Ibis Schema ↔ SQLGlot ColumnDefs: `Schema.to_sqlglot_column_defs(dialect)` and `Schema.from_sqlglot(...)`. ([Ibis][7])

**Pattern**

1. Maintain canonical Arrow schema → derive Ibis Schema.
2. Produce SQLGlot `ColumnDef` list from Ibis Schema.
3. Construct a SQLGlot `CREATE EXTERNAL TABLE` statement (AST), render SQL for DataFusion, execute via gated DataFusion SQL.

### 16.2 Format Options: consistent read/write tuning via SQL

DataFusion’s Format Options doc explains that options control how data is read/written for `COPY`, `INSERT INTO`, and `CREATE EXTERNAL TABLE`, and that options can be specified with precedence (table vs statement vs session). ([Apache DataFusion][8])

**Practical integration rule**

* Put “dataset contract defaults” at table creation (`OPTIONS(...)` in `CREATE EXTERNAL TABLE`).
* Put “run overrides” in statement options (`COPY ... OPTIONS(...)`) when needed.
* Put “system defaults” in the runtime profile (session config).

### 16.3 Writer-side control: DataFusion SQL `COPY` as an alternate sink

`COPY` is part of the same Format Options system. Use it for targeted cases where engine-native format options are desirable (but keep Arrow `write_dataset` as your default). ([Apache DataFusion][8])

---

## 17) Manifest-first ingestion: use Parquet metadata and file_visitor to power debugging and faster bootstrap

### 17.1 `write_dataset(file_visitor=...)` is your manifest hook

PyArrow explicitly states the `WrittenFile.metadata` is the Parquet metadata, has the file path set, and can be used to build a `_metadata` file. ([Apache Arrow][9])

**Implement**

* Always collect per-file paths + a compact summary of Parquet metadata.
* Only build full `_metadata` sidecars when bounded (you already have the scalability warning in your playbook).

### 17.2 Use Parquet row group metadata to validate pruning/pushdown assumptions

Row group metadata is accessible and convertible to dict (`RowGroupMetaData.to_dict`). ([Apache Arrow][6])
Persist row group stats for key columns used in pruning (e.g., stable ids, kind).

**Benefit**

* You can answer “why didn’t pruning work?” without rerunning the engine.

---

## 18) Async streaming and backpressure across the stack

You’ve already standardized “stream → writer.” The missing integration is **async streaming**, which becomes relevant for:

* serving over HTTP/Flight,
* implementing long-running builds without blocking threads,
* streaming results into sinks with backpressure.

### 18.1 DataFusion DataFrame supports async iteration (`__aiter__`)

DataFusion’s Python DataFrame docs state:

* DataFrame objects are iterable (lazy record batches)
* and `__aiter__()` returns an async iterator over record batches. ([Apache DataFusion][10])

**Pattern**

* Use `async for batch in df:` to stream results in asyncio contexts (FastAPI, Flight `GeneratorStream`, etc.).
* Convert to PyArrow batches via `rb.to_pyarrow()` when needed.

### 18.2 Flight streaming remains the preferred remote transport for batches

`RecordBatchStream` is handled in C++ for the remainder of DoGet without acquiring the GIL, which is ideal for serving `RecordBatchReader` results. ([Apache DataFusion][11])

---

## 19) Substrait “cross-engine validation” lane: DataFusion ↔ PyArrow Substrait execution

This is an integration feature that materially improves your confidence in rewrites and upgrades.

### 19.1 DataFusion Substrait APIs (artifact + interchange)

DataFusion Python exposes `datafusion.substrait` with `Producer`, `Consumer`, `Serde`, and an encodable `Plan`. ([Apache DataFusion][12])

### 19.2 PyArrow Substrait execution

PyArrow provides `pyarrow.substrait.run_query(...)` which executes a Substrait plan and returns a `RecordBatchReader`; it also supports `use_threads`. ([Apache Arrow][13])

PyArrow also exposes `get_supported_functions()` returning `{uri}#{name}` identifiers, which you can use to verify whether a plan’s functions are supported by the Arrow Substrait engine. ([Apache Arrow][14])

### 19.3 Practical use in your repo

* Generate Substrait from DataFusion logical plans (Serde/Producer).
* Execute with PyArrow Substrait engine (`run_query`) for a second opinion on semantics.
* Compare outputs on small fixtures as a CI canary.

This becomes your “upgrade safety net” when DataFusion versions change.

---

## 20) Dialect-specific emission as an integration feature (SQLGlot preprocess chains)

You already covered SQLGlot transforms; the missing integration is **how dialects bundle those transforms** and how you can reuse that pattern for your own “engine truth” emission.

SQLGlot dialect docs show `transforms.preprocess([...])` chains that include:

* `eliminate_qualify`
* `unnest_to_explode`
* `any_to_exists`
  (and others), as part of Spark2/Hive dialect generation. ([sqlglot.com][15])

**Why it matters**

* Your policy engine can keep UNNEST as canonical IR, then apply a preprocess chain when emitting SQL for a particular engine/dialect.

---

## 21) “Done when” integration tests that specifically cover these features

To make “no further analysis needed” real, add these tests:

1. **Sortedness handshake**

* Write Parquet with `sorting_columns` metadata. ([Apache Arrow][1])
* Register in DataFusion using `WITH ORDER` / `file_sort_order`. ([Apache DataFusion][2])
* Verify DataFusion plan changes appropriately (ordering analysis blog guidance). ([Apache DataFusion][16])

2. **Contract-driven external table DDL**

* Arrow schema → Ibis Schema → SQLGlot ColumnDefs ([Ibis][7])
* Build `CREATE EXTERNAL TABLE ... OPTIONS(...)` and execute in DataFusion. ([Apache DataFusion][2])

3. **Manifest-first verification**

* `write_dataset(file_visitor=...)` produces file list + metadata presence. ([Apache Arrow][9])
* Row group metadata extracted via `RowGroupMetaData.to_dict`. ([Apache Arrow][6])

4. **Async streaming**

* `async for` over DataFusion DataFrame yields batches without materialization. ([Apache DataFusion][10])

5. **Substrait cross-validation**

* DataFusion → Substrait bytes (Serde) ([Apache DataFusion][12])
* PyArrow `run_query` executes and returns `RecordBatchReader`. ([Apache Arrow][13])
* Ensure used functions appear in `get_supported_functions()` list (or mark unsupported). ([Apache Arrow][14])

---


[1]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html?utm_source=chatgpt.com "pyarrow.parquet.ParquetWriter — Apache Arrow v22.0.0"
[2]: https://datafusion.apache.org/user-guide/sql/ddl.html?utm_source=chatgpt.com "DDL — Apache DataFusion documentation"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.SortingColumn.html?utm_source=chatgpt.com "pyarrow.parquet.SortingColumn — Apache Arrow v22.0.0"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html?utm_source=chatgpt.com "datafusion.context — Apache Arrow DataFusion documentation"
[5]: https://datafusion.apache.org/_sources/user-guide/sql/ddl.md.txt?utm_source=chatgpt.com "ddl.md.txt - Apache DataFusion"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.RowGroupMetaData.html?utm_source=chatgpt.com "pyarrow.parquet.RowGroupMetaData — Apache Arrow v22.0.0"
[7]: https://ibis-project.org/reference/schemas?utm_source=chatgpt.com "schemas – Ibis"
[8]: https://datafusion.apache.org/user-guide/sql/format_options.html?utm_source=chatgpt.com "Format Options — Apache DataFusion documentation"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html?utm_source=chatgpt.com "pyarrow.dataset.write_dataset — Apache Arrow v22.0.0"
[10]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html?utm_source=chatgpt.com "datafusion.dataframe"
[11]: https://datafusion.apache.org/python/autoapi/datafusion/index.html?utm_source=chatgpt.com "Apache Arrow DataFusion documentation"
[12]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html?utm_source=chatgpt.com "datafusion.substrait"
[13]: https://arrow.apache.org/docs/python/generated/pyarrow.substrait.run_query.html?utm_source=chatgpt.com "pyarrow.substrait.run_query — Apache Arrow v22.0.0"
[14]: https://arrow.apache.org/docs/python/generated/pyarrow.substrait.get_supported_functions.html?utm_source=chatgpt.com "pyarrow.substrait.get_supported_functions - Apache Arrow"
[15]: https://sqlglot.com/sqlglot/dialects/spark2.html?utm_source=chatgpt.com "sqlglot.dialects.spark2 API documentation"
[16]: https://datafusion.apache.org/blog/2025/03/11/ordering-analysis/?utm_source=chatgpt.com "Using Ordering for Better Plans in Apache DataFusion"



Below are **additional advanced integration capabilities** across **DataFusion (Python) + Ibis + SQLGlot + PyArrow** that materially expand what you can do in this stack (and that weren’t covered in the last two integration responses). Each item includes the **integration intent**, **minimal implementation**, and **what to pin / test** so it’s usable in a production “graph product build” system.

---

## 15) Table-format providers as first-class sources in DataFusion sessions

### 15.1 Delta Lake table providers

**What it enables**
Register a Delta table directly into a DataFusion session as a **table provider** (planner-visible, better than “read a directory of Parquet” because it can preserve table semantics and enable pushdowns when supported). DataFusion’s data sources guide documents registering `deltalake.DeltaTable` via `ctx.register_table(...)`. ([Apache DataFusion][1])

**Minimal implementation**

```python
from datafusion import SessionContext
from deltalake import DeltaTable

ctx = SessionContext()
delta = DeltaTable("path_to_table")
ctx.register_table("my_delta", delta)
df = ctx.table("my_delta")
```

([Apache DataFusion][1])

**Critical nuance (performance/correctness)**
DataFusion docs warn that on older `deltalake` versions (prior to ~0.22), you can import via Arrow Dataset (`delta_table.to_pyarrow_dataset()` + `register_dataset`), **but this does not support features such as filter pushdown**, and performance can differ significantly. ([Apache DataFusion][1])

**Pin / test**

* Record “provider mode”: `register_table(DeltaTable)` vs `register_dataset(to_pyarrow_dataset())` in artifacts
* Golden test: a highly selective filter should materially reduce scanned fragments/row groups in provider mode (and you should observe the difference when using dataset fallback)

---

### 15.2 Iceberg table providers (pyiceberg / pyiceberg-core)

**What it enables**
Register Iceberg tables via the Custom Table Provider interface (table-aware scan planning, snapshot semantics, etc.). DataFusion docs show registering an Iceberg table provider with `register_table_provider`. ([Apache DataFusion][1])

**Minimal implementation**

```python
from datafusion import SessionContext
from pyiceberg.catalog import load_catalog
import pyarrow as pa

catalog = load_catalog("catalog", type="in-memory")
catalog.create_namespace_if_not_exists("default")
data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})

iceberg_table = catalog.create_table("default.test", schema=data.schema)
iceberg_table.append(data)

ctx = SessionContext()
ctx.register_table_provider("test", iceberg_table)
ctx.table("test").show()
```

([Apache DataFusion][1])

**Critical nuance**
DataFusion docs state this integration relies on **Iceberg Rust** implementation features; anything present in PyIceberg but not yet in Iceberg Rust won’t be available through this path. ([Apache DataFusion][1])

**Pin / test**

* Record `pyiceberg`/`pyiceberg-core` versions and whether provider mode is active
* Canary query: snapshot append + read visibility (ensures you’re actually seeing Iceberg semantics)

---

## 16) Identifier case-folding and quoting contract across SQLGlot ↔ DataFusion ↔ Ibis

### 16.1 DataFusion SQL case behavior changes how you generate SQL

DataFusion’s SQL documentation explicitly warns: **column names in queries are made lower-case**, but not on the inferred schema; to query capitalized fields you must use **double quotes**. ([Apache DataFusion][2])

**Why it matters for your stack**

* SQLGlot’s `normalize_identifiers` and generator quoting (`identify` / `quote_identifiers`) must be aligned with DataFusion’s case behavior or you’ll get “column not found” surprises.
* Best-in-class: **choose one canonical schema naming convention**:

  * Prefer **lowercase-only** field names in Arrow/Ibis contracts to avoid quoting complexity, or
  * Force quoting for all identifiers when you can’t control upstream casing.

**Implementation policy**

* If contract fields are lowercase: emit SQL with `identify=False` (or minimal safe quoting).
* If contract fields include uppercase/mixed case: emit SQL with quoting enabled for those identifiers and ensure SQLGlot qualification uses the same dialect/case rules.

**Pin / test**

* CI gate: schema with a capitalized field must be queryable only when quoted; ensure your policy engine generates the correct quoting.

---

## 17) DataFusion “input sources” plugin interface for custom datasource resolution

### 17.1 BaseInputSource + LocationInputPlugin (custom table resolution)

DataFusion Python exposes an “input sources” package:

* `BaseInputSource` is the abstract interface (implement `is_correct_input` and `build_table`) and docs state the plugin can be registered with `SessionContext` to obtain `SqlTable` info from a custom datasource. ([Apache DataFusion][3])
* `LocationInputPlugin` is the default “input plugin for everything” (file paths, remote locations) and implements `build_table(input_item, table_name, **kwargs) -> SqlTable`. ([Apache DataFusion][4])

**Why it matters**
This is the cleanest way to make DataFusion understand your own references like:

* `artifact://product/edges_call?run=...`
* `contract://edges/v3`
* `bundle://run123/edges`

…and have those resolve to a concrete table read without manual register_* calls scattered across the codebase.

**Minimal implementation sketch**

```python
from datafusion.input.base import BaseInputSource

class ArtifactInputSource(BaseInputSource):
    def is_correct_input(self, input_item, table_name, **kwargs) -> bool:
        return isinstance(input_item, str) and input_item.startswith("artifact://")

    def build_table(self, input_item, table_name, **kwargs):
        # parse artifact URI -> filesystem path + schema + partitioning
        # return a datafusion.common.SqlTable describing how to read it
        ...
```

([Apache DataFusion][3])

**Practical note**
The docs clearly describe the plugin concept and `SqlTable` return type, but the **public Python API method name to register an InputSource is not shown** in the `SessionContext` auto-API listing we inspected (unlike `register_table`, `register_dataset`, etc.). ([Apache DataFusion][5])
In practice, you’ll implement the source and then locate the registration hook by inspecting the current installed `datafusion` package (e.g., `dir(SessionContext)` / internal `ctx` field) or by following upstream examples/PRs.

**Pin / test**

* Once you wire registration: test that `ctx.sql("SELECT * FROM artifact://...")` resolves through your input source without pre-registration.

---

## 18) “Batch-level streaming” integration: DataFusion RecordBatchStream + Arrow C Array export

### 18.1 RecordBatchStream supports async iteration and per-batch Arrow export

DataFusion Python defines:

* `RecordBatch` as a wrapper around `pa.RecordBatch` and it implements `__arrow_c_array__` to export via Arrow C Data Interface. ([Apache DataFusion][6])
* `RecordBatchStream` as the output of `execute_stream()`; it supports `__iter__` and `__aiter__` for async streaming and returns `RecordBatch` objects. ([Apache DataFusion][6])

**Why it matters**
This gives you a second streaming surface (besides `__arrow_c_stream__`):

* If you want backpressure-aware async pipelines (FastAPI, Flight generators), you can `async for rb in stream`.
* Each `RecordBatch` can be exported as C Array (tabular struct array) into any Arrow consumer without copying. ([Apache DataFusion][6])

**Pin / test**

* Async test: `async for` yields batches under load
* Interop test: each yielded RecordBatch can be converted to PyArrow and written without materializing the entire result first

---

## 19) PyArrow Dataset composition beyond “one directory”: UnionDataset + InMemoryDataset from streams

### 19.1 `ds.dataset([...datasets...])` builds a UnionDataset

PyArrow’s high-level `ds.dataset(source=...)` supports **a list of datasets**, and it constructs a nested `UnionDataset` that allows arbitrary composition (additional kwargs are not allowed). ([Apache Arrow][7])

**Why it matters**

* You can build “multi-root” datasets (e.g., union across runs, union across shards) without rewriting file layouts.
* Useful for incremental rebuild comparisons and “diff runs” analyses.

### 19.2 `ds.dataset(RecordBatchReader)` builds an InMemoryDataset (one-shot scan)

The same doc states that if `source` is a `RecordBatchReader` or iterable of batches:

* an `InMemoryDataset` is created
* and if the source is iterable/reader, **it can only be scanned once**; further attempts raise. ([Apache Arrow][7])

**Why it matters**
This is a powerful bridge:

* You can take **DataFusion/Ibis streaming outputs**, wrap them as an InMemoryDataset, and then apply **dataset scanner policy** (projection/filter/batching) before writing—without leaving Arrow land.

**Pin / test**

* Test union dataset schema alignment (children schemas must agree; documented on UnionDataset). ([Apache Arrow][8])
* Test one-shot behavior: attempting to scan twice should raise (so your pipeline must treat these as consumable streams).

---

## 20) Partition introspection helper for reproducible pruning: `get_partition_keys`

PyArrow dataset includes:

* `get_partition_keys(expression)` which extracts partition keys (equality constraints between a field and scalar) from an expression and returns a dict mapping field name → value. ([Apache Arrow][9])

**Why it matters**
This becomes a *universal integration primitive*:

* Given your filter expression (from SQLGlot or your scan spec), you can compute exactly which partitions should be touched.
* Use it to:

  * drive deterministic file selection,
  * log “expected partitions” vs “touched fragments” in artifacts,
  * and validate pruning behavior without relying on engine internals.

**Pin / test**

* For a Hive-partitioned dataset, `get_partition_keys(kind == "CALL")` should return `{"kind": "CALL"}` and you should only touch those partition directories when scanning (or at least see the reduction in fragments).

---

## 21) FilenamePartitioning and basename templates as a *contracted integration*

PyArrow dataset supports `FilenamePartitioning` (partitioning based on a specified schema) in addition to Hive/Directory partitioning. ([Apache Arrow][9])

**Why it matters**
If you encode run/build metadata in filenames (your `basename_template`), you can:

* parse those as partition keys for fast selection (e.g., `run_id`, `build_id`),
* without forcing directory partition layouts.

**Implementation pattern**

* Choose a basename template that includes parseable tokens (e.g., `run=..._build=..._part-...`)
* Use `FilenamePartitioning` to extract those keys into dataset columns

**Pin / test**

* Verify that a dataset created from a directory of such files yields correct partition columns derived from filenames.

---

## 22) DataFusion “universal Arrow import” as the interop glue for *any* Arrow-capable producer

DataFusion’s data sources guide explicitly states:

* since DataFusion 42.0.0, any DataFrame library that supports the Arrow FFI PyCapsule interface can be imported via `from_arrow()`. ([Apache DataFusion][1])
  And `SessionContext.from_arrow` in the Python API states the input can implement `__arrow_c_stream__` or `__arrow_c_array__` (struct array required for the latter). ([Apache DataFusion][5])

**Why it matters**
This makes `from_arrow` your “backdoor integration”:

* PyArrow Scanner reader → DataFusion DataFrame
* Ibis batches → DataFusion
* Any Arrow producer (including non-PyArrow libraries) → DataFusion

**Pin / test**

* Ensure you use `__arrow_c_stream__` sources for streaming-first ingestion
* Only use `__arrow_c_array__` for tabular struct arrays (and test that constraint)

---

# Integration tests to make this “complete” operationally

Add these CI gates (each corresponds to the new integrations above):

1. **Delta provider vs dataset fallback**: demonstrate filter pushdown difference (provider path should be measurably better). ([Apache DataFusion][1])
2. **Iceberg provider registration**: append then query via DataFusion table provider. ([Apache DataFusion][1])
3. **Identifier casing**: capitalized field requires quoting in DataFusion SQL; ensure policy engine emits correct quoting. ([Apache DataFusion][2])
4. **DataFusion batch streaming**: `RecordBatchStream` async iteration + per-batch `__arrow_c_array__` export works. ([Apache DataFusion][6])
5. **UnionDataset**: `ds.dataset([ds1, ds2])` is scanable and schema-consistent; no extra kwargs. ([Apache Arrow][7])
6. **InMemoryDataset one-shot**: `ds.dataset(record_batch_reader)` scans exactly once; second scan raises. ([Apache Arrow][7])
7. **Partition key extraction**: `get_partition_keys` returns expected dict and pruning matches expected touched partitions. ([Apache Arrow][9])



[1]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/user-guide/sql/select.html?utm_source=chatgpt.com "SELECT syntax — Apache DataFusion documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/input/base/index.html "datafusion.input.base — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/input/location/index.html?utm_source=chatgpt.com "datafusion.input.location"
[5]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[6]: https://datafusion.apache.org/python/autoapi/datafusion/record_batch/index.html?utm_source=chatgpt.com "datafusion.record_batch"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.dataset.html "pyarrow.dataset.dataset — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/2.0/python/generated/pyarrow.dataset.UnionDataset.html?utm_source=chatgpt.com "pyarrow.dataset.UnionDataset — Apache Arrow v2.0.0"
[9]: https://arrow.apache.org/docs/python/api/dataset.html "Dataset — Apache Arrow v22.0.0"



Below are **additional advanced integration capabilities** across **DataFusion (Python) + Ibis + SQLGlot + PyArrow** that materially expand what you can do in this stack, along with **minimal implementation patterns** and **what to pin/test** so they’re deployable in your “deterministic, streaming-first graph-product build” architecture.

---

## 15) In-memory + “foreign dataframe” ingestion into DataFusion (Pandas/Polars/PyArrow/Python lists) as first-class inputs

### What it enables

A fully supported way to ingest:

* **Python dict/list** data (test fixtures, small control tables),
* **Pandas** and **Polars** dataframes (interop / debug tools),
* **PyArrow RecordBatches** partitioned explicitly (you control partition shape),
* **Arrow-stream producers** (`__arrow_c_stream__`) for streaming-first pipelines.

### Minimal surfaces to standardize

DataFusion `SessionContext` exposes:

* `from_arrow(...)` and `from_arrow_table(...)` (alias) for any object implementing `__arrow_c_stream__` or `__arrow_c_array__` (struct array for the latter).
* `from_pandas`, `from_polars`, `from_pydict`, `from_pylist`.
* `create_dataframe(partitions: list[list[pyarrow.RecordBatch]], ...)` (explicit partitioned ingestion).
* `register_record_batches(name, partitions)` to register partitioned batches as a table.

DataFusion’s data sources guide explicitly calls out the three “create in-memory” options: `from_pydict`, `from_pylist`, and `create_dataframe` (partitioned record batches).

### Implementation patterns

**A) Small lookup tables (Python dicts)**

```python
df = ctx.from_pydict({"kind": ["CALL", "IMPORT"], "priority": [10, 5]}, name="kind_priority")
```

**B) Deterministic partition shaping (RecordBatch partitions)**

```python
# partitions: list[partition] where partition is list[RecordBatch]
df = ctx.create_dataframe(partitions, name="staging_edges", schema=schema)
```

**C) Streaming Arrow ingestion into DF**

```python
df = ctx.from_arrow(reader_or_stream_producer, name="staging_stream")
```

### Pin / test

* Pin the **partitioning strategy** you used (`create_dataframe` partitions) in artifacts.
* Golden tests that verify:

  * partition count and `collect_partitioned()` structure are stable (see §17).
  * `from_pandas/from_polars` produces expected Arrow schema for your contracts.

---

## 16) Views + “logical plan reuse” as a cross-cutting integration primitive

### What it enables

A *non-materialized* way to:

* name intermediate results,
* share them across Ibis and DataFusion call sites,
* avoid writing Parquet for every step,
* keep complex workflows readable and composable.

### Minimal surfaces to standardize

* `SessionContext.register_view(name, df)` registers a DataFrame as a view (queryable by SQL).
* DataFusion 46.0.0 explicitly highlights “registering a logical plan as a view” so you can build views in one part of a workflow and reuse them elsewhere by passing the same session context.

### Implementation pattern

```python
df_filtered = df.filter(col("a") > literal(2))
ctx.register_view("edges_filtered", df_filtered)
df2 = ctx.table("edges_filtered")
```

Registering views and then retrieving them via `table()` is shown both in the user guide and the 46.0.0 release post.

### Pin / test

* Persist an “in-session view registry snapshot” (names + defining SQL/unparsed plan).
* Assert that **no materialization** occurs for view registration (only for terminal ops).
* CI test: same view name resolves to same logical plan across multiple modules when the same `SessionContext` instance is passed around.

---

## 17) Plan persistence and rehydration: “plans are artifacts” (and their sharp edges)

### What it enables

* Persist DataFusion plans as structured artifacts.
* Rehydrate DataFrames from plans (plan caching and reproducible execution).

### Minimal surfaces to standardize

* `SessionContext.create_dataframe_from_logical_plan(plan)` returns a DataFrame from an existing logical plan.
* Plan proto: `LogicalPlan.to_proto()/from_proto` and `ExecutionPlan.to_proto()/from_proto` exist, **but** “tables created in memory from record batches are currently not supported” for proto conversion.

### Implementation pattern

* For cacheable products whose sources are **file-backed tables** (ListingTable / external tables): proto serialize the plan.
* For products built on **in-memory RecordBatches**: persist Substrait (DataFusion) or SQLGlot canonical AST instead, and treat DF plan proto as “best effort.”

### Pin / test

* Test that plan proto roundtrip works for file-backed sources.
* Test that proto conversion failure is handled gracefully for in-memory sources (expected limitation).

---

## 18) Custom catalogs/schemas as an integration spine (Python or Rust) + PyCapsule exportables

### What it enables

A “single source of truth” metadata system that can:

* resolve datasets/contracts/runs into tables,
* refresh table listings dynamically (file-based catalogs),
* scale beyond ad-hoc `register_*` calls.

### Minimal surfaces to standardize (Python API)

DataFusion Python supports:

* `register_catalog_provider(name, provider)`.
* Python ABCs:

  * `CatalogProvider` and `SchemaProvider` (abstract interfaces) with optional register/deregister methods.
* In-memory helpers:

  * `Catalog.memory_catalog()` and `Schema.memory_schema()` for baseline behavior.

The DataFusion Python data sources guide explicitly states:

* catalogs can be written in **Rust or Python**, Rust catalogs can be faster, and Python catalogs are registered through a Rust wrapper but returned as the original Python object when accessed via Python API.

### PyCapsule exportables (FFI-friendly)

DataFusion Python defines protocol types:

* `CatalogProviderExportable` with `__datafusion_catalog_provider__()` PyCapsule method.
* `TableProviderExportable` with `__datafusion_table_provider__()` PyCapsule method.

### Pin / test

* If you adopt custom catalogs:

  * emit catalog tree snapshots (`catalog_names`, schema/table lists).
  * add a “refresh listing” capability if your catalog is file-backed (see also DataFusion core `refresh_catalogs` in Rust).

---

## 19) FFI-packaged extensions: TableProviders + UDFs via PyCapsule, end-to-end

### What it enables

* High-performance sources (custom scans) and functions (stable hash, canonical kernels) without Python overhead.
* Ship extensions as Python wheels while keeping ABI safety.

### TableProvider via FFI_TableProvider

DataFusion’s Python user guide for custom table providers is explicit:

* implement `TableProvider` in Rust,
* export an `FFI_TableProvider` via PyCapsule,
* expose it in Python through `__datafusion_table_provider__`,
* register with `ctx.register_table(...)`.

### UDFs via PyCapsule (Scalar/Aggregate/Window)

`datafusion.user_defined` defines exportable protocols:

* `ScalarUDFExportable` (`__datafusion_scalar_udf__`)
* `AggregateUDFExportable` (`__datafusion_aggregate_udf__`)
* `WindowUDFExportable` (`__datafusion_window_udf__`)
  and provides `from_pycapsule(...)` constructors (e.g., `ScalarUDF.from_pycapsule`, `AggregateUDF.from_pycapsule`).

### Pin / test

* Treat PyCapsule-based extensions as part of your **function registry fingerprint** (build_id component).
* Add CI tests that:

  * load the wheel, register providers/UDFs, and run a minimal query.
  * verify types survive through chained UDFs (see §20 for a known caveat).

---

## 20) Extension type propagation caveat across DataFusion Python UDFs (must be designed around)

### What it enables / blocks

If you plan to use Arrow **extension types** (e.g., UUID, StableId) through chained DataFusion Python UDFs, there is a known issue:

* extension type metadata may not propagate from the output of one UDF into the input of another (example: UUID array becomes FixedSizeBinary), causing downstream logic to break.

### What to do in your design

* Prefer **built-in DataFusion functions** for extension types where available.
* If you need extension types across UDF boundaries:

  * keep those UDFs in a single function (avoid chaining), or
  * cast/re-wrap types explicitly inside each UDF, or
  * move to Rust-backed UDFs (where type metadata handling can be controlled more precisely).

### Pin / test

* A canary test that chains UDFs with an extension type output and asserts the input type remains the extension type (or explicitly documents that it won’t).

---

## 21) URL tables + DynamicFileCatalog: “query files by path” (powerful and security-sensitive)

### What it enables

* Treat `SELECT * FROM 'path/to/file.parquet'` as a first-class query surface.
* Dramatically simplifies “ad-hoc” access to artifacts and local file repros.

### Minimal surfaces to standardize

* Python: `SessionContext.enable_url_table()` returns a new context with url-table enabled and is shown in the DataFusion 46 release post.
* Rust docs explicitly warn: this feature is **security sensitive**, allows direct access to arbitrary local files via SQL, and points to `DynamicFileCatalog` for details.

### Pin / test

* Only enable in a clearly separated “dev / trusted” runtime profile.
* Always pair with strict `SQLOptions` in any environment where SQL strings might be untrusted.
* Add a test that confirms url-table is disabled by default, and only enabled under explicit profile.

---

## 22) Ibis–DataFusion backend: Delta Lake read/write and “Arrow-writer delegation” surfaces

### What it enables

* Use Ibis as a single IR front-end while still accessing:

  * Delta tables (read and write),
  * Parquet writes via `pyarrow.parquet.ParquetWriter`,
  * dataset writes via `pyarrow.dataset.write_dataset` (directory sink),
    all through the DataFusion backend.

### Minimal surfaces to standardize

From the Ibis DataFusion backend docs:

* `read_delta(path, table_name=None, **kwargs)` registers a Delta table; kwargs passed to `deltalake.DeltaTable`.
* `to_delta(expr, path, params=None, **kwargs)` writes to Delta; kwargs passed to `deltalake.writer.write_deltalake`.
* `to_parquet(expr, path, **kwargs)` passes kwargs to `pyarrow.parquet.ParquetWriter`.
* `to_parquet_dir(expr, directory, **kwargs)` passes kwargs to `pyarrow.dataset.write_dataset`.
* `raw_sql(query)` accepts `str | sge.Expression` (SQLGlot AST).

### Pin / test

* If you use Delta:

  * pin deltalake versions and record provider mode (table provider vs dataset fallback).
* If you use Ibis write surfaces:

  * treat `to_parquet_dir` as equivalent to your standardized writer contract (Section 7) because it delegates to `write_dataset`.

---

## 23) PyArrow partitioning “flavors” as an integration contract (write ↔ read ↔ prune)

### What it enables

A stable, predictable mapping between:

* how you **write** partitioned datasets,
* how you **discover/read** them,
* and what gets pruned at scan time.

### Minimal surfaces to standardize

* Partitioning schemes are explicitly defined:

  * HivePartitioning: `/key=value/` segments; field order ignored; missing/unrecognized ignored.
* `write_dataset` supports `partitioning_flavor` to choose partitioning type when you pass a list of field names; if omitted, it defaults to directory partitioning.

### Pin / test

* Your writer contract should always specify:

  * partition columns and `partitioning_flavor="hive"` (or explicit partitioning object),
  * and verify that partition pruning works by checking expected fragment counts and `get_partition_keys` output (already in your integration suite).

---

## 24) “One-shot” datasets and provenance surfaces as integration glue (Scanner + TaggedRecordBatch)

### What it enables

* Treat streaming outputs as datasets (one-shot), apply scanner policy, and write.
* Record provenance by fragment without injecting extra columns.

### Minimal surfaces to standardize

* `Scanner` is “a materialized scan operation with context and options bound” and includes `from_batches(...)` constructors.
* `TaggedRecordBatch` is explicitly “a combination of a record batch and the fragment it came from”.

### Pin / test

* Ensure you never attempt to scan one-shot sources more than once (your pipeline must treat these as consumable streams).
* Use `TaggedRecordBatch.fragment` provenance in your artifact plane when debugging pruning and file touches.

---

## 25) SQLGlot “optimize” rule routing as an integration feature (policy injection without fragile coupling)

### What it enables

A compiler policy engine where:

* you pin a rule pipeline,
* you pass a single kwargs policy dict,
* and SQLGlot automatically forwards relevant kwargs to each rule based on signature.

The SQLGlot optimizer docs show:

* the `RULES` tuple order,
* and the internal mechanism: inspect each rule’s args and pass only matching kwargs.

### Pin / test

* Pin your rule list hash and kwargs policy in the runtime profile.
* Add a test that a policy kwarg only affects intended stages (e.g., qualification settings don’t leak into unrelated passes).

---

