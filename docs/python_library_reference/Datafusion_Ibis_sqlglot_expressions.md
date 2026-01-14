
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
