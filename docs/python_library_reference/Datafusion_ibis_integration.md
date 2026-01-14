Below is a **feature catalog of how Ibis integrates with the `datafusion` Python library**, centered on the **Ibis DataFusion backend** (and the “escape hatches” you get by sharing a `datafusion.SessionContext`).

---

## Mental model: what “Ibis ↔ DataFusion” actually is

* **Ibis**: you build a lazy expression tree (tables/columns/scalars), then compile/execute it.
* **DataFusion Python**: the execution engine (Arrow-native, multi-threaded), with both **SQL** and a **DataFrame API**, plus UDF/UDAF support and Arrow zero-copy interop. ([Apache DataFusion][1])
* **Integration point**: Ibis’s DataFusion backend can be created from (or wrap) an existing **`datafusion.SessionContext`**; Ibis compiles expressions (via SQLGlot under the hood) and executes them against the backend. ([Ibis][2])

---

## 1) Install + entry points

### Install options

* `pip install 'ibis-framework[datafusion]'` ([Ibis][2])
* `conda install -c conda-forge ibis-datafusion` (also `mamba …`) ([Ibis][2])

### Connect

* `con = ibis.datafusion.connect()` ([Ibis][2])
* `con = ibis.datafusion.connect(ctx)` where `ctx` is a `datafusion.SessionContext` ([Ibis][2])

---

## 2) Connection/session interoperability (the *big* integration feature)

### “Bring your own DataFusion SessionContext”

The DataFusion backend connection accepts:

* a `SessionContext` instance, or
* a mapping of `{table_name: path}` (noted as **deprecated in Ibis 10.0**). ([Ibis][2])

This is the key: it lets you pre-configure DataFusion (object stores, views, registered tables, etc.) and then let Ibis operate on that same session. ([Ibis][2])

### Lifecycle helpers

* `Backend.connect(*args, **kwargs)` / `Backend.reconnect()` / `Backend.disconnect()` ([Ibis][2])
* `Backend.from_connection(SessionContext)` to wrap an existing context (class method) ([Ibis][2])

---

## 3) Catalog/database/table hierarchy management

Ibis standardizes naming for “hierarchy” and exposes it on the DataFusion backend:

* `create_catalog(name, force=False)` / `drop_catalog(name, force=False)` ([Ibis][2])
* `create_database(name, catalog=None, force=False)` / `drop_database(name, catalog=None, force=False)` ([Ibis][2])
* `list_catalogs(like=None)` / `list_databases(like=None, catalog=None)` / `list_tables(like=None, database=None)` ([Ibis][2])

---

## 4) Table + view DDL-like operations (backed by DataFusion)

### Create table (multi-library ingestion)

`create_table(name, obj=None, schema=None, database=None, temp=False, overwrite=False)`

Notably, `obj` can be any of:

* an Ibis table expression
* `pandas.DataFrame`
* `pyarrow.Table`, `pyarrow.RecordBatch`, `pyarrow.RecordBatchReader`
* `polars.DataFrame`, `polars.LazyFrame` ([Ibis][2])

### Views

* `create_view(name, obj, database=None, overwrite=False)` where `obj` is an Ibis table expression ([Ibis][2])
* `drop_view(name, database=None, force=False)` ([Ibis][2])

### Table administration

* `drop_table(name, database=None, force=False)` ([Ibis][2])
* `rename_table(old_name, new_name)` ([Ibis][2])
* `truncate_table(name, database=None)` ([Ibis][2])

---

## 5) Reading data sources through the backend (DataFusion-backed registration)

These methods register data as tables in the current database and return an Ibis table expression:

* `read_parquet(path, table_name=None, **kwargs)` ([Ibis][2])
* `read_csv(paths, table_name=None, **kwargs)` ([Ibis][2])
* `read_json(path, table_name=None, **kwargs)` ([Ibis][2])
* `read_delta(path, table_name=None, **kwargs)` (Delta Lake table registration) ([Ibis][2])

### “Beyond the Ibis wrappers”: using DataFusion features via shared SessionContext

Because you can pass a `SessionContext` into Ibis, you can also rely on DataFusion-native registration features first, e.g.:

* **Object stores**: `ctx.register_object_store("s3://", store, None)` then `ctx.register_parquet("trips", "s3://bucket/")` ([Apache DataFusion][3])
* **Views**: `ctx.register_view("view1", df_filtered)` and then query that from SQL / from Ibis (same context) ([Apache DataFusion][4])
* **In-memory creation**: `ctx.from_pydict(...)`, `ctx.from_pylist(...)`, `ctx.create_dataframe(...)` ([Apache DataFusion][3])
* **Arrow zero-copy import/export**: `ctx.from_arrow(table)` and DataFusion DataFrames implement the Arrow PyCapsule interface for zero-copy exchange. ([Apache DataFusion][5])

---

## 6) SQL interoperability and compilation

### Compile Ibis → SQL (DataFusion dialect)

* `compile(expr, limit=None, params=None, pretty=False) -> str` ([Ibis][2])

Ibis uses SQLGlot internally for SQL generation and can expose SQLGlot expressions for analysis (lineage, transforms, etc.). ([Ibis][6])

### Bring SQL into Ibis

* `sql(query: str, schema=None, dialect=None) -> ir.Table` (creates an Ibis table expression from a SQL query; can infer schema) ([Ibis][2])
* `raw_sql(query)` executes a raw SQL string (Ibis calls this “raw SQL,” and it’s meant as the opaque escape hatch). ([Ibis][2])

---

## 7) Execution + result materialization surfaces

### Execute

* `execute(expr, params=None, limit=None, **kwargs)` → returns pandas `DataFrame`, `Series`, or scalar ([Ibis][2])

### Convenience conversions

* `to_pandas(expr, params=None, limit=None, **kwargs)` (wrapper around `execute`) ([Ibis][2])
* `to_pandas_batches(expr, params=None, limit=None, chunk_size=1_000_000, **kwargs)` ([Ibis][2])
* `to_polars(expr, params=None, limit=None, **kwargs)` ([Ibis][2])
* `to_pyarrow(expr, params=None, limit=None, **kwargs)` (returns `pyarrow.Table` / `Array` / `Scalar` depending on expression shape) ([Ibis][2])
* `to_pyarrow_batches(expr, params=None, limit=None, chunk_size=1_000_000, **kwargs)` (returns a `RecordBatchReader`) ([Ibis][2])
* `to_torch(expr, params=None, limit=None, **kwargs)` (dict of column name → tensor) ([Ibis][2])

---

## 8) Persisting results to files / tables (eager execution)

These methods **execute immediately** and write results out:

* `to_parquet(expr, path, params=None, **kwargs)` ([Ibis][2])
* `to_parquet_dir(expr, directory, params=None, **kwargs)` (uses `pyarrow.dataset.write_dataset`) ([Ibis][2])
* `to_csv(expr, path, params=None, **kwargs)` (via `pyarrow.csv.CSVWriter`) ([Ibis][2])
* `to_json(expr, path, **kwargs)` ([Ibis][2])
* `to_delta(expr, path, params=None, **kwargs)` (via `deltalake.writer.write_deltalake`) ([Ibis][2])

---

## 9) Data mutation

* `insert(name, obj, database=None, overwrite=False)` where `obj` can be `pandas.DataFrame`, an Ibis table expression, `list`, or `dict` ([Ibis][2])

---

## 10) Capability introspection + backend option hook

* `has_operation(operation_type)` (feature detection at the operation level) ([Ibis][2])
* `register_options(cls)` (backend-defined option registration hook) ([Ibis][2])

---

## 11) Advanced “SQL-free” interop via Substrait (optional, but very relevant)

This is not strictly the default Ibis DataFusion backend path (which compiles to SQL), but it’s part of the broader “Ibis ↔ DataFusion” integration story:

* **Ibis can produce Substrait plans** from Ibis table expressions using `ibis-substrait`. ([Ibis][7])
* **DataFusion Python can consume/produce Substrait plans**, including:

  * `datafusion.substrait.Consumer.from_substrait_plan(ctx, plan) -> LogicalPlan`
  * `datafusion.substrait.Producer.to_substrait_plan(logical_plan, ctx) -> Plan`
  * `Serde.serialize_bytes(sql, ctx)` / `Serde.deserialize_bytes(proto_bytes)` ([Apache DataFusion][8])
* DataFusion itself advertises Substrait producer/consumer support and that it’s usable via the Python bindings. ([substrait.io][9])

This enables a “portable IR” workflow: **Ibis expression → Substrait → DataFusion execution**, avoiding SQL-string generation at runtime when that matters.

---

If you want, I can take the above catalog and rewrite it as a **function-level integration map** (“feature → exact call signatures → what it maps to in `datafusion.SessionContext` / DataFusion SQL / Arrow interop”), aimed at AI agents implementing adapters.

[1]: https://datafusion.apache.org/python/ "DataFusion in Python — Apache Arrow DataFusion  documentation"
[2]: https://ibis-project.org/backends/datafusion "datafusion – Ibis"
[3]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/python/user-guide/common-operations/views.html "Registering Views — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/python/user-guide/io/arrow.html "Arrow — Apache Arrow DataFusion  documentation"
[6]: https://ibis-project.org/posts/does-ibis-understand-sql/ "Does Ibis understand SQL? – Ibis"
[7]: https://ibis-project.org/posts/ibis_substrait_to_duckdb/ "Ibis + Substrait + DuckDB – Ibis"
[8]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html "datafusion.substrait — Apache Arrow DataFusion  documentation"
[9]: https://substrait.io/community/powered_by/?utm_source=chatgpt.com "Powered by Substrait"

Below is a **function-level integration map** you can hand to an AI agent to implement (or audit) an “Ibis ⇄ DataFusion adapter layer”.

I’m going to treat:

* **Ibis side** as the **DataFusion backend** (`ibis.backends.datafusion.Backend` and `ibis.datafusion.connect`)
* **DataFusion side** as **`datafusion.SessionContext`** + **`datafusion.DataFrame`**
* **Interop** as (a) SQL strings executed via `ctx.sql(...)`, (b) registered tables/views, and (c) Arrow streaming / record-batch materialization

---

## Legend

* `con`: Ibis DataFusion backend connection (`ibis.backends.datafusion.Backend`)
* `ctx`: DataFusion `SessionContext`
* `df`: DataFusion `DataFrame`
* `expr`: Ibis expression (`ir.Expr` / `ir.Table`)
* “Maps to” describes the **likely underlying mechanism** / the **best canonical mapping** you should implement in an adapter.

---

# 1) Core Ibis Backend → DataFusion mapping (function-by-function)

## A) Connection + session ownership

| Ibis function (exact signature)                                                        | Maps to DataFusion                                                                                                            | Adapter notes                                                                                      |
| -------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `ibis.datafusion.connect()`                                                            | `ctx = datafusion.SessionContext()`                                                                                           | Ibis `connect` is a wrapper around `Backend.do_connect(config=None)`. ([Ibis][1])                  |
| `do_connect(self, config=None)` where `config: Mapping[...] \| SessionContext \| None` | If `config` is a `SessionContext`, use that; else create a new `SessionContext` and (legacy) register tables from the mapping | The mapping-of-name→path mode is explicitly documented as **deprecated in Ibis 10.0**. ([Ibis][1]) |
| `from_connection(cls, con, /)` where `con: SessionContext`                             | `Backend` wraps an existing `SessionContext`                                                                                  | This is the cleanest “bring your own ctx” integration point. ([Ibis][1])                           |
| `reconnect(self)` / `disconnect(self)`                                                 | Session lifecycle is largely “in-memory”; reconnect typically rebinds saved args/kwargs; disconnect drops refs                | Ibis documents `reconnect` / `disconnect` as backend lifecycle operations. ([Ibis][1])             |

---

## B) Catalog / database / table hierarchy

Ibis uses **catalog/database/table** terms; DataFusion uses **catalog/schema/table** (and defaults exist). DataFusion’s SQL DDL explicitly treats `CREATE DATABASE` as “create catalog” and `CREATE SCHEMA` as “create schema under a catalog”. ([datafusion.apache.org][2])

| Ibis function (exact signature)                                | Maps to DataFusion                                                                            | Adapter notes                                                                                                           |
| -------------------------------------------------------------- | --------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `create_catalog(self, name, /, *, force=False)`                | DataFusion catalog creation via SQL: `CREATE DATABASE [IF NOT EXISTS] <catalog>`              | This mapping is directly aligned with DataFusion’s DDL definition. ([Ibis][1])                                          |
| `create_database(self, name, /, *, catalog=None, force=False)` | DataFusion schema creation via SQL: `CREATE SCHEMA [IF NOT EXISTS] [catalog.]schema`          | In adapter code, “Ibis database” ↔ “DataFusion schema”. ([Ibis][1])                                                     |
| `list_catalogs(self, *, like=None)`                            | `ctx.catalog_names()` and/or catalog API enumeration                                          | `SessionContext.catalog_names()` exists; catalogs/schemas/tables are first-class in DF. ([datafusion.apache.org][3])    |
| `list_databases(self, *, like=None, catalog=None)`             | `ctx.catalog(...).schema_names()`                                                             | Catalog/Schema objects are in `datafusion.catalog`. ([datafusion.apache.org][4])                                        |
| `list_tables(self, *, like=None, database=None)`               | `ctx.table_exist(name)` / enumerate schema tables via catalog API                             | `table_exist(name)` exists; deeper table listing is via catalog/schema APIs. ([Ibis][1])                                |
| `drop_table(self, name, /, *, database=None, force=False)`     | DataFusion SQL `DROP TABLE [IF EXISTS] ...` **or** schema API `Schema.deregister_table(name)` | DataFusion DDL documents `DROP TABLE`; schema deregistration exists in `datafusion.catalog.Schema`. ([Ibis][1])         |
| `truncate_table(self, name, /, *, database=None)`              | Commonly implemented as SQL `DELETE FROM table` (DataFusion supports DML via `ctx.sql`)       | DataFusion’s `ctx.sql(...)` explicitly supports DDL + DML statements (including things like `INSERT INTO`). ([Ibis][1]) |
| `rename_table(self, old_name, new_name)`                       | Likely `ALTER TABLE` (if supported) or catalog-level rewrite (copy/register/drop)             | Ibis exposes it; DF catalog APIs make “rename” implementable as re-register + deregister if needed. ([Ibis][1])         |
| `get_schema(self, table_name, *, catalog=None, database=None)` | `df = ctx.table(table_name)` then `df.schema()` (or table metadata via catalog API)           | DataFusion `DataFrame.schema()` exists. ([Ibis][1])                                                                     |

---

## C) Registering / reading sources as tables (files, datasets)

### Ibis “read_*” methods register a table and return an Ibis table expression

| Ibis function (exact signature)                             | Maps to DataFusion                                                                     | Adapter notes                                                                                       |
| ----------------------------------------------------------- | -------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `read_csv(self, paths, /, *, table_name=None, **kwargs)`    | `ctx.register_csv(name, path, ...)` then `ctx.table(name)`                             | DF `register_csv` supports a single path or list of paths and schema inference options. ([Ibis][1]) |
| `read_json(self, path, /, *, table_name=None, **kwargs)`    | `ctx.register_json(name, path, ...)` then `ctx.table(name)`                            | DF `register_json` has schema inference knobs. ([Ibis][1])                                          |
| `read_parquet(self, path, /, *, table_name=None, **kwargs)` | `ctx.register_parquet(name, path, ...)` then `ctx.table(name)`                         | DF `register_parquet` includes pruning/partition/file_sort_order options. ([Ibis][1])               |
| `read_delta(self, path, /, table_name=None, **kwargs)`      | Uses `deltalake.DeltaTable` (Ibis doc says kwargs go there), then registers as a table | This is “Ibis + Delta + DataFusion” rather than pure DF I/O. ([Ibis][1])                            |

### DataFusion “escape hatch” registrations that matter for an adapter

These are *not* Ibis APIs, but they are critical for “best possible integration”:

* `register_dataset(name: str, dataset: pyarrow.dataset.Dataset) → None` ([datafusion.apache.org][3])
* `register_listing_table(name: str, path: ..., file_extension='.parquet', ...) → None` (multi-file table assembly) ([datafusion.apache.org][3])
* `register_object_store(schema: str, store: Any, host: str | None = None) → None` (S3/GCS/etc routing) ([datafusion.apache.org][3])
* `register_table(name: str, table: Table | TableProviderExportable | DataFrame | pyarrow.dataset.Dataset) → None` (super-flexible “register anything”) ([datafusion.apache.org][3])
* `register_record_batches(name: str, partitions: list[list[pyarrow.RecordBatch]]) → None` (in-memory partition registration) ([datafusion.apache.org][3])

---

## D) Creating tables / views (in-memory + from expressions)

| Ibis function (exact signature)                                                                     | Maps to DataFusion                                                                                                                                    | Adapter notes                                                                                                                              |
| --------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `create_table(self, name, /, obj=None, *, schema=None, database=None, temp=False, overwrite=False)` | Either: (1) materialize `obj` to an Arrow source and `ctx.register_table(name, ...)` **or** (2) run SQL `CREATE [OR REPLACE] TABLE ... AS SELECT ...` | DF SQL DDL supports in-memory `CREATE TABLE ... AS SELECT/VALUES`. DF also supports registering DataFrames/datasets as tables. ([Ibis][1]) |
| `create_view(self, name, /, obj, *, database=None, overwrite=False)`                                | Either: (1) SQL `CREATE [OR REPLACE] VIEW ... AS <query>` **or** (2) `ctx.register_view(name, df)`                                                    | DF supports `CREATE VIEW ... AS ...` and also a direct `register_view(name, df)` method. ([Ibis][1])                                       |
| `drop_view(self, name, /, *, database=None, force=False)`                                           | `DROP VIEW` (if supported) or catalog manipulation                                                                                                    | DF supports view creation; adapter can implement “drop” by removing from schema/table provider surface. ([Ibis][1])                        |

**Key implementation trick for `create_table` with Python objects:** DataFusion can create DataFrames from multiple Python/Arrow containers:

* `ctx.from_pandas(data: pandas.DataFrame, name: str | None = None) → DataFrame` ([datafusion.apache.org][3])
* `ctx.from_polars(data: polars.DataFrame, name: str | None = None) → DataFrame` ([datafusion.apache.org][3])
* `ctx.from_arrow(data: ArrowStreamExportable | ArrowArrayExportable, name: str | None = None) → DataFrame` (accepts Arrow C stream/array producers) ([datafusion.apache.org][3])
* `ctx.from_pydict(data: dict[str, list[Any]], name: str | None = None) → DataFrame` / `ctx.from_pylist(data: list[dict[str, Any]], name: str | None = None) → DataFrame` ([datafusion.apache.org][3])

Once you have a `df`, register it as a view/table via `ctx.register_table(...)` or `ctx.register_view(...)`. ([datafusion.apache.org][3])

---

## E) SQL surfaces: compiling, creating expressions, raw execution

| Ibis function (exact signature)                                           | Maps to DataFusion                                        | Adapter notes                                                                              |
| ------------------------------------------------------------------------- | --------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `compile(self, expr, /, *, limit=None, params=None, pretty=False) -> str` | Produces SQL (DataFusion dialect) for `expr`              | This is the “front half” of `execute`. ([Ibis][1])                                         |
| `sql(self, query, /, *, schema=None, dialect=None) -> ir.Table`           | `df = ctx.sql(query, ...)` then wrap into Ibis table expr | DF `sql(query, options=None, param_values=None, **named_params) -> DataFrame`. ([Ibis][1]) |
| `raw_sql(self, query)`                                                    | `ctx.sql(query)` (often for DDL/DML)                      | DF `sql(...)` explicitly implements DDL + DML in-memory defaults. ([Ibis][1])              |
| `table(self, name, /, *, database=None)`                                  | `df = ctx.table(name)` then wrap                          | DF `table(name: str) -> DataFrame`. ([Ibis][1])                                            |

### Parameter binding mapping (important for “adapter correctness”)

* Ibis `execute(..., params=...)` takes a mapping of **Ibis scalar expressions → Python values**. ([Ibis][1])
* DataFusion `ctx.sql(...)` supports `param_values` (scalar substitution) and `named_params` (string or DataFrame substitution). ([datafusion.apache.org][3])

So an adapter has two legitimate strategies:

1. **Ibis-style:** compile SQL with literals inlined (safe if you control quoting/typing).
2. **DataFusion-style:** compile to SQL with parameter markers + pass `param_values` / `named_params`.

---

## F) Execution + conversion targets

### Ibis execution core

| Ibis function (exact signature)                                                              | Maps to DataFusion                                                                             | Adapter notes                                                                                                                                                  |
| -------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `execute(self, expr, /, *, params=None, limit=None, **kwargs)`                               | `sql = con.compile(expr, ...)` → `df = ctx.sql(sql, ...)` → terminal op to materialize         | Ibis returns pandas DataFrame/Series/scalar. ([Ibis][1])                                                                                                       |
| `to_pandas(self, expr, /, *, params=None, limit=None, **kwargs)`                             | same as `execute` (documented wrapper)                                                         | Ibis says it wraps `execute`. ([Ibis][1])                                                                                                                      |
| `to_pandas_batches(self, expr, /, *, params=None, limit=None, chunk_size=1000000, **kwargs)` | implement via DataFusion streaming (`df.collect()` in chunks, or Arrow stream → pandas chunks) | DataFusion supports collecting record batches and converting to pandas; DataFrame is lazily evaluated until terminal ops. ([Ibis][1])                          |
| `to_polars(self, expr, /, *, params=None, limit=None, **kwargs)`                             | `df.to_polars()` or Arrow→Polars                                                               | DataFusion terminal ops include `to_polars()`. ([Ibis][1])                                                                                                     |
| `to_pyarrow(self, expr, /, *, params=None, limit=None, **kwargs)`                            | `df.to_arrow_table()` for table expr; similar for arrays/scalars                               | DataFusion terminal ops include `to_arrow_table()`; Ibis documents return-shape by expr kind. ([Ibis][1])                                                      |
| `to_pyarrow_batches(self, expr, /, *, chunk_size=1000000, **kwargs) -> RecordBatchReader`    | Prefer DataFusion Arrow streaming: `pa.RecordBatchReader.from_stream(df)`                      | DF DataFrames implement `__arrow_c_stream__` and the docs show `pa.RecordBatchReader.from_stream(df)` streaming without materializing all batches. ([Ibis][1]) |
| `to_torch(self, expr, /, *, params=None, limit=None, **kwargs)`                              | Arrow/Pandas → Torch tensor conversion                                                         | Ibis documents dict-of-column→tensor output. ([Ibis][1])                                                                                                       |

---

## G) Export / persist (eager writers)

These Ibis methods are explicitly **eager**: they execute the expression immediately and write outputs.

| Ibis function (exact signature)                                      | Maps to DataFusion + Arrow tooling                      | Adapter notes                                                                      |
| -------------------------------------------------------------------- | ------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `to_csv(self, expr, /, path, *, params=None, **kwargs)`              | Execute → Arrow batches/table → `pyarrow.csv.CSVWriter` | Ibis doc explicitly says kwargs go to `pyarrow.csv.CSVWriter`. ([Ibis][1])         |
| `to_parquet(self, expr, /, path, *, params=None, **kwargs)`          | Execute → Arrow table → `pyarrow.parquet.ParquetWriter` | Ibis doc explicitly says kwargs go to `pyarrow.parquet.ParquetWriter`. ([Ibis][1]) |
| `to_parquet_dir(self, expr, /, directory, *, params=None, **kwargs)` | Execute → Arrow table → `pyarrow.dataset.write_dataset` | Ibis doc explicitly says kwargs go to `pyarrow.dataset.write_dataset`. ([Ibis][1]) |
| `to_json(self, expr, /, path, **kwargs)`                             | Execute → materialize → JSON array-of-objects           | Ibis defines the output shape as `[{column -> value}, …]`. ([Ibis][1])             |
| `to_delta(self, expr, /, path, *, params=None, **kwargs)`            | Execute → Arrow → `deltalake.writer.write_deltalake`    | Ibis doc explicitly points to deltalake writer. ([Ibis][1])                        |

---

## H) Mutation: inserts

| Ibis function (exact signature)                                 | Maps to DataFusion                                                                                   | Adapter notes                                                                                                                                                  |
| --------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `insert(self, name, /, obj, *, database=None, overwrite=False)` | Common pattern: register `obj` as temp view/table → `INSERT INTO name SELECT ...` via `ctx.sql(...)` | Ibis accepts `obj` as DataFrame, Ibis table, list, dict. DF `ctx.sql` supports DML and DF can build DataFrames from dict/list/pandas/polars/arrow. ([Ibis][1]) |

---

## I) Capability probing

| Ibis function (exact signature)    | Maps to DataFusion       | Adapter notes                                                       |
| ---------------------------------- | ------------------------ | ------------------------------------------------------------------- |
| `has_operation(cls, operation, /)` | Ibis-side feature gating | Use this for “compile-time” fallback strategies. ([Ibis][1])        |
| `register_options(cls)`            | Backend options plumbing | In adapters, treat this as “policy surface” for tuning. ([Ibis][1]) |

---

# 2) DataFusion-side “escape hatch” map (what your adapter should expose)

If your goal is “maximal integration”, your adapter should expose the underlying `SessionContext` because these features are **DataFusion-native** and not fully surfaced through Ibis:

## A) Core query entrypoints

* `sql(query: str, options: SQLOptions | None = None, param_values: dict[str, Any] | None = None, **named_params) -> DataFrame` ([datafusion.apache.org][3])
* `table(name: str) -> DataFrame` and `table_exist(name: str) -> bool` ([datafusion.apache.org][3])

## B) Registration primitives

* `register_csv(...)`, `register_json(...)`, `register_parquet(...)` with rich options (schema inference, compression, pruning, partition cols, etc.) ([datafusion.apache.org][3])
* `register_dataset(name, dataset: pyarrow.dataset.Dataset)` and `register_listing_table(...)` (multi-file and object-store oriented) ([datafusion.apache.org][3])
* `register_object_store(schema: str, store: Any, host: str | None = None)` ([datafusion.apache.org][3])
* `register_table(name, table: Table | DataFrame | pyarrow.dataset.Dataset | ...)` ([datafusion.apache.org][3])
* `register_view(name: str, df: DataFrame)` ([datafusion.apache.org][3])

## C) Arrow-native ingestion helpers

* `from_arrow(...)` (accepts Arrow C stream/array producers), `from_arrow_table(...)`, `from_pandas(...)`, `from_polars(...)`, `from_pydict(...)`, `from_pylist(...)` ([datafusion.apache.org][3])

## D) Materialization + streaming (best-in-class interop)

DataFusion DataFrames are lazy until terminal ops; terminal ops include:

* `df.collect()` → RecordBatches
* `df.to_pandas()`, `df.to_polars()`, `df.to_arrow_table()` ([datafusion.apache.org][5])

For streaming into PyArrow **without materializing the whole result**:

* DataFusion DataFrames implement the Arrow C stream protocol (`__arrow_c_stream__`) and docs show:
  `reader = pa.RecordBatchReader.from_stream(df)` ([datafusion.apache.org][5])

---

# 3) Optional “plan-level” integration: Substrait (Ibis → DF without SQL)

This is the “IR route” when you want to avoid SQL strings and do plan serialization.

## A) Ibis → Substrait (compiler surface)

* `from ibis_substrait.compiler.core import SubstraitCompiler`
* `proto = compiler.compile(expr)` (returns a Substrait protobuf object / bytes-like) ([PyPI][6])

## B) Substrait ↔ DataFusion

DataFusion exposes a `datafusion.substrait` module with:

* `Consumer.from_substrait_plan(ctx: SessionContext, plan: Plan) -> LogicalPlan` ([datafusion.apache.org][7])
* `Plan.encode() -> bytes` ([datafusion.apache.org][7])
* `Serde.serialize_bytes(...)` / `Serde.deserialize_bytes(...)` and related helpers ([datafusion.apache.org][7])

Substrait itself is explicitly intended as a cross-language relational plan interchange format. ([datafusion.apache.org][7])

---

# 4) Minimal “adapter contract” (what an AI agent should implement)

If you want a crisp target for an agent, implement this *thin wrapper*:

1. **Own** or **accept** a `SessionContext`
2. Provide:

   * `register_*` helpers (CSV/JSON/Parquet/Dataset/ObjectStore)
   * `compile(expr) -> sql`
   * `run_sql(sql, **params) -> DataFusion DataFrame`
   * `execute(expr, target="pandas|arrow_table|arrow_batches|polars", ...)`
3. For best performance / memory:

   * implement `to_pyarrow_batches` using `pa.RecordBatchReader.from_stream(df)` rather than collecting everything eagerly ([datafusion.apache.org][5])

If you want, I can turn this into a **concrete “adapter skeleton”** (Python module layout + method stubs + a couple end-to-end flows: file registration → Ibis expression → DataFusion execution → Arrow streaming → Parquet write).

[1]: https://ibis-project.org/backends/datafusion "datafusion – Ibis"
[2]: https://datafusion.apache.org/user-guide/sql/ddl.html "DDL — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/catalog/index.html "datafusion.catalog — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/python/user-guide/dataframe/index.html "DataFrames — Apache Arrow DataFusion  documentation"
[6]: https://pypi.org/project/ibis-substrait/ "ibis-substrait · PyPI"
[7]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html "datafusion.substrait — Apache Arrow DataFusion  documentation"

Below is a **function-level integration catalog** for **SQLGlot ↔ DataFusion**, in the same spirit as the Ibis/DataFusion map—but with an important framing:

* **SQLGlot is a client-side SQL compiler toolkit** (parser → AST → transpiler/optimizer → SQL string).
* **DataFusion is the execution engine**: you hand it SQL strings (or build a DataFrame plan) via `SessionContext`.
* There isn’t a single “official” tight coupling package—**the integration is the wrapper you build**: `SQLGlot AST/rewrites → DataFusion SQL execution`.

Also: **SQLGlot’s documented built-in dialect list doesn’t include “DataFusion”**; you typically target a close dialect (often Postgres-like) or define a custom dialect. ([SqlGlot][1])

---

# 1) DataFusion surfaces SQLGlot typically targets

## A) Core execution entrypoints (Python)

### `SessionContext.sql(...)`

**Signature:**
`sql(query: str, options: SQLOptions | None = None, param_values: dict[str, Any] | None = None, **named_params: Any) -> DataFrame`

**Special behaviors to keep in mind:**

* Supports **DDL and DML** (e.g., `CREATE TABLE`, `CREATE VIEW`, `INSERT INTO`) with in-memory default implementation.
* Supports **two substitution mechanisms**:

  * `param_values`: scalar substitution **after parsing**
  * `named_params`: string or DataFrame substitution in the query string

### `SessionContext.sql_with_options(...)`

Like `sql`, but first validates the query against `SQLOptions`.
This is the hook you use if you want a policy gate (“no COPY”, “no external locations”, etc.).

---

## B) DataFusion SQL semantics that affect SQLGlot generation

### Identifier case + quoting

DataFusion warns that **column names in queries are made lower-case**; if you want to query a capitalized field, **use double quotes**.

### Pipe operators (`|>`) and parser dialect switching

DataFusion supports a set of **pipe operators** and notes you can switch parser dialect via:
`SET datafusion.sql_parser.dialect = 'BigQuery';`

This is a “special syntax” area where SQLGlot support may differ—plan for passthrough or custom parsing/transpilation.

---

# 2) SQLGlot surfaces you use to feed DataFusion

SQLGlot provides: parsing, formatting/transpiling across dialects, optimizer rewrites, and custom dialect extension points. ([SqlGlot][1])

## A) “Front door” APIs (what you’ll use in adapters)

| Feature                | SQLGlot API (typical)                                                               | Output artifact                          |
| ---------------------- | ----------------------------------------------------------------------------------- | ---------------------------------------- |
| Parse SQL → AST        | `sqlglot.parse_one(sql, dialect=... )` (aka `read=...`)                             | `Expression` AST you can inspect/rewrite |
| Transpile dialects     | `sqlglot.transpile(sql, read=..., write=..., pretty=..., identify=...)`             | SQL string(s)                            |
| Generate SQL from AST  | `expr.sql(dialect=..., pretty=..., identify=...)`                                   | SQL string                               |
| Canonicalize/normalize | `from sqlglot.optimizer import optimize`                                            | normalized AST / SQL string              |
| Extend dialect         | subclass `Dialect`, override Tokenizer/Generator, set `TRANSFORMS` / `TYPE_MAPPING` | custom parse+generate behavior           |

SQLGlot explicitly documents dialect translation and identifier delimiting (`identify=True`) as part of transpilation/generation. ([SqlGlot][1])

## B) Dialect mechanics that matter for DataFusion

Two “dialect knobs” are especially relevant when targeting DataFusion:

* **Function name normalization** (`Dialect.NORMALIZE_FUNCTIONS`) controls whether functions are uppercased/lowercased.
* **Generator rewrites** (`Generator.TRANSFORMS`) are how you rename/lower expressions and functions into the engine’s expected SQL. ([SqlGlot][1])

---

# 3) Function-level integration map (SQLGlot → DataFusion)

## A) Core query pipeline (parse → rewrite → execute)

| Step                | SQLGlot call(s)                                                       | Maps to DataFusion call(s)                                       | “Special interfacing” notes                                                                                                                     |
| ------------------- | --------------------------------------------------------------------- | ---------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| Parse & inspect     | `parse_one(sql, read/dialect=...)`                                    | (No DF call yet)                                                 | Use AST to classify: SELECT vs DDL/DML vs “side effects”. SQLGlot can detect syntax errors/dialect issues.                                      |
| Rewrite / normalize | AST transforms + optional `optimize(...)`                             | (No DF call yet)                                                 | Useful for canonical SQL (caching, signatures, policy). SQLGlot optimizer creates a canonical AST form.                                         |
| Emit SQL            | `expr.sql(dialect=..., identify=..., pretty=...)` or `transpile(...)` | (No DF call yet)                                                 | **Identifier quoting**: DataFusion requires double quotes for capitalized fields. Make sure your chosen write dialect emits compatible quoting. |
| Execute             | (your final SQL string)                                               | `ctx.sql(query, options=None, param_values=..., **named_params)` | `ctx.sql` supports **DDL/DML** and has `param_values` / `named_params` substitution.                                                            |
| Validate + execute  | (your final SQL string)                                               | `ctx.sql_with_options(query, options, ...)`                      | Use this to enforce “safe SQL subset” at execution time.                                                                                        |

---

## B) Statements/expressions that require “special handling” (side effects / non-portable)

These are the ones I’d flag in any adapter as “special actions” (log, gate, allowlist, etc.):

### 1) External I/O and registration (DataFusion-specific DDL)

**`CREATE EXTERNAL TABLE … LOCATION … OPTIONS (…)`** registers local or object-store data.
It supports:

* `UNBOUNDED`
* `PARTITIONED BY`
* `WITH ORDER`
* `OPTIONS` with format-specific options

**Adapter implication:** if SQLGlot is being used as a policy rewriter, treat `LOCATION` and `OPTIONS` as **high-risk fields** (path / credentials / object store access), and consider forcing these operations through the Python registration APIs instead.

---

### 2) COPY / INSERT format options (read/write behavior embedded in SQL)

DataFusion format options apply to **`COPY`, `INSERT INTO`, and `CREATE EXTERNAL TABLE`** with a precedence order (table DDL > COPY options > session defaults).

**Adapter implication:** if you transpile queries into DataFusion and allow these statements, you need a clear policy on:

* allowed formats/compression
* allowed destinations
* allowed per-column options (e.g., Parquet compression overrides)

---

### 3) Prepared statements + `$1` placeholders

DataFusion supports `PREPARE ... AS ...` with positional placeholders like `$1`, and `EXECUTE ...(...)`. ([datafusion.apache.org][2])

**Adapter implication:**

* If you intend to generate prepared statements via SQLGlot, **preserve `$1`, `$2`, … exactly**, and avoid rewrite passes that might “helpfully” reinterpret them.
* Keep this separate from Python-side `param_values`, which is substitution in `ctx.sql` after parsing. ([datafusion.apache.org][2])

---

### 4) Pipe operator syntax (`|>`) + parser dialect toggles

DataFusion supports pipe operators and documents toggling its SQL parser dialect (example shows BigQuery).

**Adapter implication:**

* Decide if you will **ban** pipe syntax, **passthrough** pipe syntax, or **normalize** it to vanilla SQL using SQLGlot rewrites.
* If you keep it, you may need coordinated configuration on the DataFusion side (`SET datafusion.sql_parser.dialect = ...`).

---

# 4) “SQLGlot knobs” that are especially useful for DataFusion adapters

## A) Identifier delimiting / quoting

SQLGlot can delimit identifiers during transpile/generation (`identify=True`)—this is a key tool given DataFusion’s “column names are lower-cased in queries” behavior and reliance on double quotes for capitalized fields.

**Practical adapter pattern:**

* emit DataFusion-target SQL using a dialect that matches DataFusion quoting rules (often Postgres-ish),
* set `identify=True` when you can’t trust identifier casing/keywords.

## B) Function lowering / renaming via `TRANSFORMS`

SQLGlot’s recommended way to adapt to an engine is to override the generator and define `TRANSFORMS` (and type mappings). ([SqlGlot][1])

This is exactly how you handle:

* function name mismatches (e.g., `DATE_TRUNC`/`STRFTIME`/etc. differences)
* expression syntax differences
* engine-specific clauses

Related: `Dialect.NORMALIZE_FUNCTIONS` controls function-name casing.

## C) When DataFusion is extended, your SQLGlot layer must track it

DataFusion provides an extension system for parsing/planning custom syntax (operators, types, relations).

**Important coordination point:**
If your DataFusion instance accepts custom syntax (e.g., Postgres-style operators enabled via its parser dialect config), SQLGlot must either:

* parse/generate that syntax too (custom SQLGlot dialect), or
* treat it as passthrough and avoid rewriting those regions.

DataFusion even shows enabling Postgres dialect parsing for an operator example by setting `datafusion.sql_parser.dialect`.

---

# 5) Minimal “adapter skeleton” (what most teams implement)

A typical integration wrapper does:

1. `parse_one` → inspect/deny/normalize
2. `expr.sql(...)` or `transpile(...)` → DataFusion-compatible SQL
3. `ctx.sql_with_options(...)` or `ctx.sql(...)` → execute

```python
import sqlglot
from sqlglot import parse_one
from datafusion import SessionContext

def compile_for_datafusion(sql: str, read: str = "postgres", write: str = "postgres") -> str:
    ast = parse_one(sql, read=read)               # parse + AST inspection
    # ... policy gates / rewrites here ...
    return ast.sql(dialect=write, identify=True)  # delimiter identifiers defensively

def run_df(ctx: SessionContext, sql: str, **params):
    compiled = compile_for_datafusion(sql)
    return ctx.sql(compiled, **params)            # param_values=..., named_params=...
```

Key execution details (`param_values`, `named_params`, DDL/DML support) are on `SessionContext.sql`.

---

## If you want the “next level” version of this

I can turn this into a **ready-to-drop-in policy+rewriter module** that:

* classifies and blocks “side-effect SQL” (`COPY`, `CREATE EXTERNAL TABLE`, `PREPARE`, etc.) unless allowed
* normalizes identifier casing/quoting for DataFusion’s behavior
* adds a transform registry for DataFusion-specific function rewrites (`TRANSFORMS`)
* uses `ctx.sql_with_options` as a second-stage guardrail

[1]: https://sqlglot.com/sqlglot/dialects.html "sqlglot.dialects API documentation"
[2]: https://datafusion.apache.org/user-guide/sql/prepared_statements.html "Prepared Statements — Apache DataFusion  documentation"

Below is a **capability-set catalog** for **DataFusion UDFs (Python bindings + engine concepts)**, organized into **semantically distinct chapters** you can deep dive later. I’m treating “UDF” broadly as **user-defined functions usable in DataFusion query execution** (SQL + DataFrame API), including scalar, aggregate, window, and table functions. ([datafusion.apache.org][1])

---

## 0) Baseline: when you need UDFs at all

**Capability set:** *Built-ins first; UDFs as the escape hatch*

* DataFusion emphasizes that it already provides many built-in expressions/functions, so UDFs are for cases where built-ins aren’t enough. ([datafusion.apache.org][1])
* In Python, UDFs are primarily **Arrow-native**: they receive Arrow arrays (often whole record batches) and should return Arrow arrays/scalars efficiently. ([datafusion.apache.org][1])

Deep-dive themes later: “how to decide UDF vs built-in”, “how to measure/avoid performance cliffs”.

---

## 1) Scalar UDFs (Python): “vectorized row-wise” transforms

**Capability set:** *Row-wise semantics, batch-wise execution*

### What it is

* A **scalar UDF** takes one or more input columns and returns **one output value per input row** (i.e., output array length = input array length). In Python it’s defined as a function taking one or more **`pyarrow.Array`** and returning a single **`pyarrow.Array`**. ([datafusion.apache.org][1])

### Python API surface

* Constructor/decorator: `datafusion.udf(...)` (as a function or `@udf(...)` decorator) producing a `ScalarUDF`. ([datafusion.apache.org][2])
* Attach to a context for SQL usage: `SessionContext.register_udf(udf)` ([datafusion.apache.org][3])
* Can also be created from a Rust-backed PyCapsule: `ScalarUDF.from_pycapsule(...)` / passing a PyCapsule-exportable into `udf(...)`. ([datafusion.apache.org][2])

### Execution/performance model (critical)

* UDFs are invoked on **entire batches** of records; your implementation should behave “row-wise” but do work in vectorized Arrow operations where possible. ([datafusion.apache.org][1])
* Strong recommendation: prefer `pyarrow` built-ins / `pyarrow.compute` to avoid conversions and copies; converting Arrow values to Python objects is a major slowdown. ([datafusion.apache.org][1])

Deep-dive themes later: signature typing rules, null semantics, multi-arg broadcast rules, error handling, and “Arrow-compute-first” patterns.

---

## 2) Volatility & optimizer behavior (cross-cutting)

**Capability set:** *Determinism classification that affects planning*

### What it is

* Every UDF is tagged with **volatility**: `immutable`, `stable`, or `volatile`. ([datafusion.apache.org][2])
* DataFusion may **inline** immutable (and sometimes stable) functions during planning; volatile functions cannot be pre-evaluated because they may differ per row/invocation. ([datafusion.apache.org][2])

### Python API surface

* `Volatility` enum (or lowercase string) passed to `udf/udaf/udwf`. ([datafusion.apache.org][2])

Deep-dive themes later: how volatility interacts with constant folding, predicate pushdown, caching, and reproducibility.

---

## 3) Aggregate UDFs (UDAFs): custom group reductions + state management

**Capability set:** *Group-wise computation with explicit accumulator state*

### What it is

* A **UDAF** operates on **groups of rows** and returns **one value per group**. ([datafusion.apache.org][1])
* You implement an `Accumulator` with:

  * `update(*values: pyarrow.Array) -> None`
  * `state() -> list[pyarrow.Scalar]`
  * `merge(states: list[pyarrow.Array]) -> None`
  * `evaluate() -> pyarrow.Scalar` ([datafusion.apache.org][1])

### Python API surface

* Constructor/decorator: `datafusion.udaf(accum, input_types, return_type, state_type, volatility, name)` or `@udaf(...)`. ([datafusion.apache.org][1])
* Register: `SessionContext.register_udaf(udaf)` ([datafusion.apache.org][3])
* Rust-backed bridge: `AggregateUDF.from_pycapsule(...)` / passing a PyCapsule-exportable into `udaf(...)`. ([datafusion.apache.org][2])

### Key semantic detail

* DataFusion may split aggregation across batches/partitions, so `state` + `merge` are first-class requirements, not optional. ([datafusion.apache.org][1])

Deep-dive themes later: multi-state accumulators, partial aggregation planning, memory footprint, and Arrow-compute-based state updates.

---

## 4) Window UDFs (UDWFs): partition-aware, frame-aware analytics

**Capability set:** *Functions that operate over ordered partitions (like window functions), with multiple evaluation modes*

### What it is

* A **user-defined window function** operates over a **partition of rows** (and optionally a window frame) and returns one value per row. ([datafusion.apache.org][4])
* You implement a `WindowEvaluator` and choose one of three evaluation strategies:

  * `evaluate(...)` (row-at-a-time; simplest, least performant)
  * `evaluate_all(...)` (compute whole partition at once)
  * `evaluate_all_with_rank(...)` (use rank-only info) ([datafusion.apache.org][1])

### UDWF planning flags (these change which method gets called)

* `uses_window_frame`
* `supports_bounded_execution`
* `include_rank` ([datafusion.apache.org][1])

### Python API surface

* Constructor: `datafusion.udwf(evaluator, input_type(s), return_type, volatility=...)` ([datafusion.apache.org][1])
* Register: `SessionContext.register_udwf(udwf)` ([datafusion.apache.org][3])
* Rust-backed bridge: `WindowUDF.from_pycapsule(...)` ([datafusion.apache.org][2])

Deep-dive themes later: bounded execution (incremental window computation), frame semantics, rank-based optimizations, and correctness/performance tradeoffs between `evaluate` vs `evaluate_all`.

---

## 5) Table functions (UDTFs): return a relation via a TableProvider

**Capability set:** *Functions that produce a table, not a scalar*

### What it is

* A **UDTF** takes arguments and returns a **TableProvider** to be used in a query plan. ([datafusion.apache.org][5])
* **Python-specific constraint:** UDTFs accept `Expr` arguments but **only literal expressions are supported** (i.e., parameters, not per-row inputs). ([datafusion.apache.org][1])

### Python API surface

* Constructor: `TableFunction.udtf(...)` / `datafusion.udtf(...)` (returns `TableFunction`) ([datafusion.apache.org][2])
* Register: `SessionContext.register_udtf(func)` ([datafusion.apache.org][3])
* Rust-backed exposure: implement `__datafusion_table_function__` and expose via `PyCapsule`. ([datafusion.apache.org][1])

Deep-dive themes later: how to design UDTF argument schemas, how TableProviders work, object store integration patterns, and testing UDTFs as “parameterized virtual tables”.

---

## 6) Registration & invocation modes (SQL vs DataFrame API)

**Capability set:** *How UDFs become usable inside plans*

* DataFusion Python gives explicit context registration methods:

  * `register_udf(ScalarUDF)`
  * `register_udaf(AggregateUDF)`
  * `register_udwf(WindowUDF)`
  * `register_udtf(TableFunction)` ([datafusion.apache.org][3])
* Scalar/aggregate/window UDF objects are also “callable” in expression-building (they produce `Expr` nodes); the actual computation happens when the plan executes. ([datafusion.apache.org][2])

Deep-dive themes later: best practice for naming, scoping (per-session vs shared/global context), and how registration impacts SQL parsing and planning.

---

## 7) Type system & signatures (Arrow-first)

**Capability set:** *Static typing for planning + coercion boundaries*

* Each UDF/UDAF/UDWF requires:

  * `input_types: list[pyarrow.DataType]`
  * `return_type: pyarrow.DataType`
  * `state_type` (UDAF) ([datafusion.apache.org][2])
* These types become part of what DataFusion needs to plan the query (type checking, expression planning, etc.). ([Docs.rs][6])

Deep-dive themes later: nested types, temporal types, dictionary types, casting rules, and how to design “type-stable” UDF APIs to minimize implicit casts.

---

## 8) Implementation strategies & performance tiers (Python ⇄ Arrow ⇄ Rust)

**Capability set:** *Three common tiers, with clear tradeoffs*

From DataFusion’s own benchmarking write-up, you can think in three tiers:

1. **Pure Python iteration (slow path)**: converting Arrow scalars to Python objects (`as_py`) and iterating row-by-row. DataFusion explicitly warns this conversion is among the slowest operations and should be used sparingly. ([datafusion.apache.org][1])
2. **Python + `pyarrow.compute` (fast path)**: keep everything in Arrow kernels (vectorized) and combine boolean arrays, etc. This can be dramatically faster than Python object conversion. ([datafusion.apache.org][7])
3. **Rust UDF with Python wrapper (max perf / max complexity)**: implement in Rust + expose to Python; still callable via the same `udf(...)` entrypoint. DataFusion’s blog describes using `pyo3` + Arrow conversions and notes wheel-building (e.g., `maturin develop --release`). ([datafusion.apache.org][7])

Deep-dive themes later: “how to decide to go Rust”, packaging + ABI issues, and the PyCapsule exportable pathway.

---

## 9) Async UDFs (engine-level capability; important for I/O / network)

**Capability set:** *Non-blocking scalar UDF execution (Rust) for network/LLM calls*

* DataFusion (engine) supports **async UDFs** that can perform network/database requests without blocking query execution, implemented via `AsyncScalarUDFImpl` in addition to `ScalarUDFImpl`. ([datafusion.apache.org][8])
* This is particularly relevant if you plan “ask an LLM” style functions during query evaluation. ([datafusion.apache.org][8])

Deep-dive themes later: cancellation behavior, concurrency limits, timeouts/retries, and how/if this is exposed or wrapped from Python in your target DataFusion version.

---

## 10) “UDF-adjacent” extension hooks (for library/system builders)

**Capability set:** *Going beyond “just a function”*

Even if you’re starting in Python, DataFusion’s broader UDF story includes engine-side extension topics that matter when you want “production-grade” custom semantics:

* Separate UDF categories (scalar/window/aggregate/table) are first-class and documented as such in DataFusion’s library guide. ([datafusion.apache.org][5])
* UDTFs depend on custom **TableProvider** mechanics (query-plannable relations). ([datafusion.apache.org][1])
* (If you go Rust) there are also “named arguments”, “custom expression planning”, and other integration points referenced in the library guide’s UDF section index. ([datafusion.apache.org][9])

Deep-dive themes later: custom planners, function registries, named args UX, and interoperability with SQL transpilation / Substrait.

---

# Suggested deep-dive order (if you want a sequence)

1. Scalar UDFs (Arrow-compute-first patterns)
2. Volatility + optimizer interactions
3. UDAFs (Accumulator/state design)
4. UDWFs (evaluation modes + bounded execution)
5. UDTFs (TableProvider + literal-arg constraints)
6. Rust/PyCapsule bridge + packaging (maturin, ABI)
7. Async UDFs (I/O, cancellation, concurrency policy)

If you tell me which of these you want first, I’ll do the same style of deep dive you’ve been doing in this project (API shapes, best practices, edge cases, and “production patterns”).

[1]: https://datafusion.apache.org/python/user-guide/common-operations/udf-and-udfa.html "User-Defined Functions — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/user_defined/index.html "datafusion.user_defined — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/index.html "datafusion — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/library-user-guide/functions/adding-udfs.html "Adding User Defined Functions: Scalar/Window/Aggregate/Table Functions — Apache DataFusion  documentation"
[6]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.ScalarUDF.html?utm_source=chatgpt.com "ScalarUDF in datafusion::logical_expr - Rust"
[7]: https://datafusion.apache.org/blog/2024/11/19/datafusion-python-udf-comparisons/ "Comparing approaches to User Defined Functions in Apache DataFusion using Python - Apache DataFusion Blog"
[8]: https://datafusion.apache.org/blog/2025/07/28/datafusion-49.0.0/ "Apache DataFusion 49.0.0 Released - Apache DataFusion Blog"
[9]: https://datafusion.apache.org/library-user-guide/functions/index.html "Functions — Apache DataFusion  documentation"

## DataFusion “Arrow-first” scalar UDFs (Python) — reference (API + contracts + patterns)

### 1) Core API surface

#### 1.1 Constructor helper / decorator

```python
from datafusion import udf  # re-exported helper
import pyarrow as pa

# function form
my_udf = udf(func, input_types=[pa.int64(), ...], return_type=pa.bool_(),
             volatility="stable", name="my_udf")

# decorator form
@udf([pa.int64(), pa.int64()], pa.int64(), "immutable", "add_i64")
def add_i64(a: pa.Array, b: pa.Array) -> pa.Array:
    ...
```

* `udf(func, input_types, return_type, volatility, name)` or `@udf(input_types, return_type, volatility, name)`; `input_types` length must equal argument count. ([datafusion.apache.org][1])
* Arrow-first scalar UDF definition: Python callable consumes **one or more `pyarrow.Array`** and returns **one `pyarrow.Array`**. ([datafusion.apache.org][2])

#### 1.2 ScalarUDF object (callable Expr node)

* Class: `datafusion.user_defined.ScalarUDF(name, func, input_types, return_type, volatility)` ([datafusion.apache.org][1])
* Expression call: `ScalarUDF.__call__(*args: datafusion.expr.Expr) -> datafusion.expr.Expr` (UDF invocation produces an `Expr`; execution deferred). ([datafusion.apache.org][1])

#### 1.3 Context registration

```python
from datafusion import SessionContext
ctx = SessionContext()
ctx.register_udf(my_udf)
```

* `SessionContext.register_udf(udf: datafusion.user_defined.ScalarUDF) -> None` ([datafusion.apache.org][3])

---

### 2) Execution contract (batch semantics, shape, types)

#### 2.1 Batch-at-a-time invocation

* DataFusion scalar UDF evaluates **entire record batch**; user logic typically row-semantic, but input arrives as columnar arrays. ([datafusion.apache.org][2])
* UDF inputs: evaluated argument expressions → `pyarrow.Array` values passed into your callable. ([datafusion.apache.org][4])

#### 2.2 Output shape invariants

* Scalar UDF must return `pyarrow.Array` with **same row count** as inputs (one output row per input row). ([datafusion.apache.org][4])

#### 2.3 Declared Arrow typing

* `input_types: list[pa.DataType]` + `return_type: pa.DataType` drive planner typechecking + SQL signature. ([datafusion.apache.org][1])
* If kernel output type differs, cast explicitly (Arrow compute cast) to match declared `return_type` (avoid planner mismatch / runtime error).

---

### 3) Volatility (planner semantics; constant folding / inlining)

```python
from datafusion.user_defined import Volatility

udf(..., volatility=Volatility.Immutable)
udf(..., volatility="stable")
udf(..., volatility="volatile")
```

* `Volatility` enum values: `Immutable`, `Stable`, `Volatile`; string form uses lowercase names. ([datafusion.apache.org][1])
* Planner behavior: DataFusion attempts inlining for `Immutable` and (when possible) `Stable`; `Volatile` disables planning-time evaluation (row-wise evaluation required). ([datafusion.apache.org][1])

---

### 4) Invocation patterns (DataFrame API + SQL)

#### 4.1 DataFrame API (Expr call)

```python
from datafusion import col
df2 = df.select(
    col("a"),
    my_udf(col("a")).alias("y"),
).filter(my_udf(col("a")))
```

* UDF call uses `ScalarUDF.__call__(Expr, ...) -> Expr`. ([datafusion.apache.org][1])
* `col()` / `column()` build column `Expr`; `Expr.alias()` assigns output name. ([datafusion.apache.org][5])

#### 4.2 SQL (name-bound)

```python
ctx.register_udf(my_udf)
ctx.sql("SELECT my_udf(a) AS y FROM t").show()
```

* Registration binds function name into SQL function resolution inside the session. ([datafusion.apache.org][3])

---

## 5) Arrow-first implementation: kernels, operators, zero-copy discipline

### 5.1 “Arrow-first” rule

* Prefer `pyarrow.Array` methods + `pyarrow.compute` kernels: vectorized, typically avoids copy; avoids Python object materialization overhead. ([datafusion.apache.org][2])
* Avoid `.as_py()` + Python loops per row: conversion boundary expensive; reserve for unavoidable semantics. ([datafusion.apache.org][2])

### 5.2 Minimal skeleton (kernel-only)

```python
import pyarrow as pa
import pyarrow.compute as pc
from datafusion import udf

@udf([pa.int64(), pa.int64()], pa.bool_(), "immutable", "eq_i64")
def eq_i64(a: pa.Array, b: pa.Array) -> pa.Array:
    return pc.equal(a, b)  # -> BooleanArray
```

* `pc.equal(array, scalar|array) -> BooleanArray` pattern used in official performance example. ([datafusion.apache.org][4])

### 5.3 Multi-column predicates: boolean algebra over arrays

```python
import pyarrow.compute as pc

@udf([pa.int64(), pa.int64(), pa.utf8()], pa.bool_(), "stable", "tuple_match")
def tuple_match(pk: pa.Array, sk: pa.Array, rf: pa.Array) -> pa.Array:
    res = None
    for pk_v, sk_v, rf_v in [(1530, 4031, "N"), (6530, 1531, "N")]:
        m = pc.and_(pc.and_(pc.equal(pk, pk_v), pc.equal(sk, sk_v)), pc.equal(rf, rf_v))
        res = m if res is None else pc.or_(res, m)
    return res
```

* Canonical Arrow-first pattern: iterate small parameter set; compute masks via `pc.equal`; compose via `pc.and_` / `pc.or_`; no per-row Python conversion. ([datafusion.apache.org][4])
* Note: `pc.*` kernels generally unary/binary; N-ary logic expressed via staged composition. ([datafusion.apache.org][4])

### 5.4 Null semantics: keep in Arrow domain

```python
@udf([pa.int64()], pa.bool_(), "stable", "is_null")
def is_null(a: pa.Array) -> pa.Array:
    return a.is_null()      # Array method
    # or: return pc.is_null(a)
```

* Example uses `array.is_null()`; pyarrow provides array methods + compute kernels; compute path recommended for copy-avoidance/perf. ([datafusion.apache.org][2])

### 5.5 Common Arrow-compute building blocks (high-leverage kernel families)

Use inside UDF body; maintain Arrow arrays/scalars end-to-end.

* **Comparison / membership**: `pc.equal`, `pc.not_equal`, `pc.less`, `pc.greater`, … (array↔scalar, array↔array). ([datafusion.apache.org][4])
* **Boolean algebra**: `pc.and_`, `pc.or_`, `pc.xor`, `pc.invert`, `pc.if_else`.
* **Null handling**: `pc.is_null`, `pc.is_valid`, `pc.fill_null`, `pc.coalesce` (when applicable).
* **Arithmetic**: `pc.add`, `pc.subtract`, `pc.multiply`, `pc.divide`, `pc.negate`, `pc.abs`.
* **Casting**: `pc.cast(array, target_type)`; use to guarantee declared `return_type`.
* **Strings**: `pc.utf8_*` family (case ops, substring ops, pattern ops), `pc.match_substring`, etc.
* **Temporal**: `pc.strptime`, `pc.strftime`, `pc.floor_temporal`, `pc.ceil_temporal`, etc.
  (Exact kernel inventory lives in PyArrow; UDF contract only requires returning Arrow arrays.) ([datafusion.apache.org][2])

---

## 6) Performance / runtime constraints (DataFusion-Python specific)

### 6.1 Zero-copy boundary, but GIL gate

* DataFusion Python advertises zero-copy engine↔Python exchange via Arrow C data interface; UDF/UDAF execution requires GIL lock when running Python code. ([datafusion.apache.org][6])

### 6.2 Batch sizing implications

* UDF cost amortized per batch; kernel-only code scales with Arrow vectorization; row-loop code scales with Python interpreter overhead. ([datafusion.apache.org][2])

### 6.3 “Avoid conversion boundary” anti-pattern

```python
# anti-pattern (row-loop + as_py)
return pa.array([v.as_py() is None for v in array])
```

* Official docs call Python object conversion among the slowest operations; use sparingly. ([datafusion.apache.org][2])

---

## 7) Rust UDF interop touchpoints (not the deep dive yet, but relevant knobs)

### 7.1 “Rust-backed ScalarUDF inside PyCapsule” passthrough

* `udf(func=PyCapsule_or_exportable, ...)` supported: pass Rust-backed `ScalarUDF` capsule; remaining args ignored; signature inferred from underlying function. ([datafusion.apache.org][1])
* Export protocol: object implements `__datafusion_scalar_udf__()` returning capsule (`ScalarUDFExportable`). ([datafusion.apache.org][1])
* Constructor: `ScalarUDF.from_pycapsule(func)` builds Python `ScalarUDF` wrapper around FFI-exported UDF. ([datafusion.apache.org][1])

(When you’re ready, the Rust deep dive naturally follows: PyO3 callable wrapper vs true FFI ScalarUDF export, Arrow array conversions, wheel build tooling, signature inference, volatility mapping.)

---

If you want the next increment immediately: I can produce a **Rust UDF deep dive** in the same reference style, centered on (a) PyO3 `#[pyfunction]` callable passed into `udf(...)`, and (b) FFI/PyCapsule “native ScalarUDF” export via `__datafusion_scalar_udf__`, plus the performance/maintenance decision table.

[1]: https://datafusion.apache.org/python/autoapi/datafusion/user_defined/index.html "datafusion.user_defined — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/python/user-guide/common-operations/udf-and-udfa.html "User-Defined Functions — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/blog/2024/11/19/datafusion-python-udf-comparisons/ "Comparing approaches to User Defined Functions in Apache DataFusion using Python - Apache DataFusion Blog"
[5]: https://datafusion.apache.org/python/user-guide/dataframe/index.html "DataFrames — Apache Arrow DataFusion  documentation"
[6]: https://datafusion.apache.org/python/ "DataFusion in Python — Apache Arrow DataFusion  documentation"
