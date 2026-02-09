
Below is a deep dive on **“2) Data sources & IO (local + cloud + ‘table providers’)”** specifically through the lens of the **`datafusion` Python library** (DataFusion in Python) and an **Arrow-native** deployment style.

---

## The core IO model in `datafusion` (Python)

In DataFusion Python, “IO” is mostly about turning *some external thing* into either:

1. a **DataFrame** (a lazy plan you can operate on), or
2. a **registered table** (a named entry in the session catalog you can reference in SQL).

The main entrypoint is `SessionContext`, which can:

* create DataFrames from data sources (files, Arrow, etc.), and
* register data sources as tables for SQL queries. ([Apache DataFusion][1])

You’ll see four main IO patterns:

### A) “Read” → get a DataFrame immediately (ad hoc)

* `ctx.read_csv / read_json / read_parquet / read_avro` return a DataFrame directly, without naming it in the catalog. ([Apache DataFusion][2])

### B) “Register” → name a table in the session catalog (reusable)

* `ctx.register_csv / register_json / register_parquet / register_avro` registers a table name and lets you reference it in SQL. ([Apache DataFusion][3])

### C) “Listing / multi-file table” → treat a directory/prefix as one table

* `ctx.register_listing_table(...)` explicitly registers “multiple files as a single table” and is designed for “locations in an ObjectStore.” ([Apache DataFusion][2])

### D) “Table providers” → plug in non-file tables (Delta/Iceberg/custom)

* `ctx.register_table(...)` / `ctx.register_dataset(...)` / `ctx.register_table_provider(...)` accept richer provider objects (including `pyarrow.dataset.Dataset`), and can integrate with systems like Delta Lake and Iceberg. ([Apache DataFusion][2])

---

## 2.1 Local files: formats, APIs, and “advanced knobs”

### Supported “simple path” formats in the Python docs

The DataFusion Python user guide highlights **Parquet, CSV, JSON, and Avro** for local files. ([Apache DataFusion][4])

### Read vs Register (the practical difference)

**`read_*`**: great for one-off transforms
**`register_*`**: great for repeated SQL + joins across multiple sources

```python
from datafusion import SessionContext

ctx = SessionContext()

# Ad hoc: directly get a DataFrame plan
df = ctx.read_parquet("events.parquet")

# Reusable: register table name for SQL
ctx.register_parquet("events", "events.parquet")
df2 = ctx.sql("SELECT count(*) FROM events")
```

(These patterns are shown in the Parquet docs.) ([Apache DataFusion][5])

---

### CSV: schema inference, multi-file inputs, compression

`read_csv` (and `register_csv`) exposes the knobs you care about in production:

* `schema` (explicit schema beats inference for stability)
* `has_header`, `delimiter`
* `schema_infer_max_records` (how many rows to scan for inference)
* `file_extension` (filter which files to include)
* `file_compression_type` (compressed CSV support)
* `read_csv` / `register_csv` accept **a list of paths** (useful for “known shards”) ([Apache DataFusion][2])

```python
import pyarrow as pa
from datafusion import SessionContext

ctx = SessionContext()

schema = pa.schema([
    ("user_id", pa.int64()),
    ("ts", pa.timestamp("us")),
    ("event", pa.string()),
])

df = ctx.read_csv(
    ["part-000.csv.gz", "part-001.csv.gz"],
    schema=schema,
    has_header=True,
    delimiter=",",
    file_compression_type="gzip",
)
```

(Parameters are documented on the `SessionContext.read_csv` signature.) ([Apache DataFusion][2])

**Deployment note:** If CSV matters beyond quick ingestion, strongly prefer **explicit schema** (and consider using SQL “Format Options” if you need more detailed CSV parsing controls at the engine level). ([Apache DataFusion][6])

---

### JSON: line-delimited JSON, inference limits, compression

DataFusion Python’s JSON reader is explicitly **line-delimited JSON** (“NDJSON-style”). ([Apache DataFusion][2])

Key knobs:

* `schema` or `schema_infer_max_records`
* `file_extension`
* `table_partition_cols`
* `file_compression_type` ([Apache DataFusion][2])

```python
from datafusion import SessionContext
import pyarrow as pa

ctx = SessionContext()

df = ctx.read_json(
    "logs.ndjson.zst",
    schema_infer_max_records=50_000,
    file_compression_type="zstd",
)
```

(Parameters documented on the `read_json` signature.) ([Apache DataFusion][2])

---

### Parquet: pruning, schema conflicts, directory/prefix reads, declared file order

Parquet is where DataFusion really expects to live.

Notable `read_parquet` / `register_parquet` knobs:

* `parquet_pruning`: allow predicate-based row-group pruning ([Apache DataFusion][2])
* `skip_metadata`: skip “file schema metadata” to avoid schema conflicts ([Apache DataFusion][2])
* `schema`: optional override/inference
* `file_extension`: select matching files under a directory/prefix
* `table_partition_cols`: add partition columns (more on this below)
* `file_sort_order`: declare that files are pre-sorted (can enable better planning) ([Apache DataFusion][2])

```python
from datafusion import SessionContext, col

ctx = SessionContext()

ctx.register_parquet(
    "events",
    "/data/events/",              # directory / prefix
    file_extension=".parquet",
    parquet_pruning=True,
    skip_metadata=True,
)

# filters can be pushed into scans + pruning
ctx.sql("""
  SELECT event_type, count(*)
  FROM events
  WHERE event_type = 'call'
  GROUP BY 1
""").show()
```

(Directory/prefix + pruning + skip_metadata are explicitly surfaced in the API.) ([Apache DataFusion][2])

---

### Avro: straightforward reader / table registration

Avro IO is intentionally simple:

```python
from datafusion import SessionContext
ctx = SessionContext()
df = ctx.read_avro("file.avro")
```

(From the Avro IO doc.) ([Apache DataFusion][7])

---

## 2.2 Multi-file datasets and “hive partitioning” (local or cloud)

### `register_listing_table`: explicit “many files = one table”

`register_listing_table(...)` is the Python-level hook for DataFusion’s “listing table” pattern:

* “Register multiple files as a single table”
* “assemble multiple files from locations in an `ObjectStore` instance”
* knobs: `table_partition_cols`, `file_extension`, `schema`, `file_sort_order` ([Apache DataFusion][2])

```python
import pyarrow as pa
from datafusion import SessionContext

ctx = SessionContext()

ctx.register_listing_table(
    "nyctaxi",
    "/mnt/nyctaxi/",
    file_extension=".parquet",
    table_partition_cols=[
        ("year", pa.int32()),
        ("month", pa.int32()),
    ],
)
ctx.sql("SELECT year, month, count(*) FROM nyctaxi GROUP BY 1,2").show()
```

(Signature + description in the API docs.) ([Apache DataFusion][2])

### Hive partition columns: why they matter (and how DataFusion interprets them)

In DataFusion’s listing-table model, **partition columns are derived from folder structure** (Hive partitioning). The Rust docs (which explain the underlying mechanism) explicitly state:

* partition columns are “added to the data that is read, based on the folder structure,” e.g.
  `/mnt/nyctaxi/year=2022/month=01/tripdata.parquet`
* files not matching the partitioning scheme may be ignored
* partition columns are extracted from the **file path**, not from inside the Parquet file ([Docs.rs][8])

This is the backbone of “lake-style” performance: it enables **partition pruning** and avoids scanning irrelevant files.

The SQL docs show the same idea for `CREATE EXTERNAL TABLE ... PARTITIONED BY (...)`, including a hive-style directory example. ([Apache DataFusion][9])

### Declaring file sort order

If your pipeline writes files already sorted by some key(s), you can declare `file_sort_order`. The listing options docs explain DataFusion may use this to “omit sorts or use more efficient algorithms.” ([Docs.rs][8])

In Python this appears as `file_sort_order=` on `read_parquet`, `register_parquet`, and `register_listing_table`. ([Apache DataFusion][2])

---

## 2.3 Cloud / remote IO via Object Stores (S3, GCS, Azure, HTTP)

### Supported object stores in DataFusion Python

The DataFusion Python docs list these supported stores:

* `AmazonS3`, `GoogleCloud`, `Http`, `LocalFileSystem`, `MicrosoftAzure` ([Apache DataFusion][4])

### The pattern: create store → register store → register/read tables

The user guide shows the canonical S3 flow:

1. build an `AmazonS3(...)` object (credentials from env in the example),
2. `ctx.register_object_store("s3://", s3, None)`,
3. register/read parquet from an `s3://bucket/...` path. ([Apache DataFusion][4])

```python
import os
from datafusion import SessionContext
from datafusion.object_store import AmazonS3

ctx = SessionContext()

bucket = "yellow-trips"
s3 = AmazonS3(
    bucket_name=bucket,
    region="us-east-1",
    access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

ctx.register_object_store("s3://", s3, None)
ctx.register_parquet("trips", f"s3://{bucket}/")
ctx.table("trips").show()
```

(Directly from the Data Sources guide.) ([Apache DataFusion][4])

### Object store constructor knobs (advanced)

The staged API docs enumerate constructor parameters (useful for real deployments like MinIO, Azure auth modes, etc.):

* `AmazonS3(bucket_name, region, access_key_id, secret_access_key, endpoint, allow_http, imdsv1_fallback)`
* `GoogleCloud(bucket_name, service_account_path)`
* `LocalFileSystem(prefix=None)`
* `MicrosoftAzure(container_name, account, access_key, bearer_token, client_id, client_secret, tenant_id, sas_query_pairs, use_emulator, allow_http)` ([Apache Arrow][10])

This is the “escape hatch” set you typically need for:

* **S3-compatible endpoints** (MinIO / custom S3 gateways),
* **Azure SAS vs key vs OAuth** auth patterns,
* local prefixing for “sandboxed” file access.

### Registering object stores: what the args mean

The Python API is:

* `register_object_store(schema: str, store: Any, host: str | None = None)` ([Apache DataFusion][2])

The docs’ canonical usage registers by scheme prefix (`"s3://"`). ([Apache DataFusion][4])

---

## 2.4 URL tables (treat file paths as “tables”)

DataFusion Python added a convenience mode for “URL tables”:

* `ctx = SessionContext().enable_url_table()`
* then `ctx.table("./path/to/file.parquet")` works (the file path itself is treated as the table name) ([Apache DataFusion][11])

```python
import datafusion

ctx = datafusion.SessionContext().enable_url_table()
df = ctx.table("./examples/tpch/data/customer.parquet")
df.show()
```

(From the DataFusion Python 46.0.0 release blog + API docs.) ([Apache DataFusion][11])

**When to deploy it:** notebooks, quick scripts, and “explore this file” workflows.
**When not to:** long-lived services where you want explicit catalog objects and controlled access paths.

---

## 2.5 “Table providers” as IO: PyArrow Dataset, Delta Lake, Iceberg, custom providers

### PyArrow Dataset (bridging to Arrow’s dataset stack)

DataFusion Python can register:

* a `pyarrow.dataset.Dataset` via `register_dataset(name, dataset)` ([Apache DataFusion][2])
* or generally `register_table(name, table)` where `table` can be a `pyarrow.dataset.Dataset` (among other types). ([Apache DataFusion][2])

This is a big deal because it lets you do “Arrow-native ingestion” with PyArrow Dataset machinery (filesystem/partitioning) and then query it with DataFusion operators.

### Delta Lake

The DataFusion Python docs state:

* DataFusion **43.0.0+** can register “table providers from sources such as Delta Lake,” requiring a recent `deltalake` version. ([Apache DataFusion][4])
* For older `deltalake` (<0.22), you can fall back to importing via the Arrow Dataset interface, but the docs warn this may lose features like **filter pushdown** and can significantly affect performance. ([Apache DataFusion][4])

### Iceberg

The docs state:

* DataFusion **45.0.0+** can register Apache Iceberg tables as table providers via the Custom Table Provider interface, requiring `pyiceberg` (>=0.10.0) or `pyiceberg-core` (>=0.5.0). ([Apache DataFusion][4])
* Important limitation: the integration relies on **Iceberg Rust**, so features in PyIceberg that aren’t yet in Iceberg Rust won’t be available through DataFusion. ([Apache DataFusion][4])

### Custom table providers

You can implement a custom provider in Rust and expose it through DataFusion’s Custom Table Provider interface (advanced, but explicitly supported/documented). ([Apache DataFusion][4])

Also: in the Python API, `register_table_provider(...)` exists but is **deprecated** in favor of `register_table(...)`. ([Apache DataFusion][2])

---

## Deployment decision guide: “which IO path should we standardize on?”

A good “best-in-class” Arrow-native posture is:

1. **Parquet + partitioned directories** as the default lake format

   * use `register_parquet` for simple cases; graduate to `register_listing_table` when you need explicit multi-file / partition-col declarations. ([Apache DataFusion][2])

2. **Explicit schemas for CSV/JSON** whenever stability matters

   * rely on inference only for exploration; tune inference via `schema_infer_max_records` if you must. ([Apache DataFusion][2])

3. **Object stores for cloud** (S3/GCS/Azure)

   * register the store once per session runtime, then treat remote paths like files. ([Apache DataFusion][4])

4. **Table providers** when you need “database-like” semantics without DuckDB’s DB-file model

   * Delta/Iceberg providers can be the “system boundary,” with DataFusion as the query engine. ([Apache DataFusion][4])

---


[1]: https://datafusion.apache.org/python/user-guide/basics.html?utm_source=chatgpt.com "Concepts — Apache Arrow DataFusion documentation"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/python/user-guide/io/csv.html?utm_source=chatgpt.com "CSV — Apache Arrow DataFusion documentation"
[4]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/python/user-guide/io/parquet.html?utm_source=chatgpt.com "Parquet — Apache Arrow DataFusion documentation"
[6]: https://datafusion.apache.org/user-guide/sql/format_options.html "Format Options — Apache DataFusion  documentation"
[7]: https://datafusion.apache.org/python/user-guide/io/avro.html?utm_source=chatgpt.com "Avro — Apache Arrow DataFusion documentation"
[8]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingOptions.html "ListingOptions in datafusion::datasource::listing - Rust"
[9]: https://datafusion.apache.org/user-guide/sql/ddl.html "DDL — Apache DataFusion  documentation"
[10]: https://arrow.staged.apache.org/datafusion-python/generated/datafusion.object_store.object_store.html "datafusion.object_store.object_store — Apache Arrow DataFusion  documentation"
[11]: https://datafusion.apache.org/blog/2025/03/30/datafusion-python-46.0.0/ "Apache DataFusion Python 46.0.0 Released - Apache DataFusion Blog"

Below is the deep dive on:

* **(a) Catalog/schema organization + Information Schema**
* **(b) SQL/DDL “external table + format options” workflows**

…grounded in the **`datafusion` Python library** (DataFusion in Python) and written with an **Arrow-native** operating model in mind.

---

# (a) Catalog/schema organization + Information Schema

## 1) The hierarchy: how DataFusion organizes “things you can query”

At the engine level, DataFusion’s metadata is a strict hierarchy:

**CatalogProviderList → CatalogProvider → SchemaProvider → TableProvider**. ([Apache DataFusion][1])

In practical terms, this is the “three-level name” model you’ll use in SQL:

**`catalog.schema.table`**

…and the whole point is: tables are *providers* (files, object store listings, in-memory record batches, Delta/Iceberg providers, custom providers, etc.), not “rows in a database file.”

## 2) Default catalog/schema, and why you should make them explicit in deployment docs

### Engine defaults (important for documentation)

DataFusion config includes defaults for:

* `datafusion.catalog.default_catalog` → **`datafusion`**
* `datafusion.catalog.default_schema` → **`public`**
* `datafusion.catalog.create_default_catalog_and_schema` → **true** ([Apache DataFusion][2])

So if you run SQL without qualifying names, you’re implicitly in **`datafusion.public`** unless you change it.

### Python: set these explicitly (recommended)

In DataFusion Python you can control the defaults and whether they’re created:

* `SessionConfig.with_create_default_catalog_and_schema(enabled=True|False)`
* `SessionConfig.with_default_catalog_and_schema(catalog, schema)` ([Apache DataFusion][3])

That becomes your first “production-grade contract” decision:

* Do you want **one canonical catalog/schema** for the session?
* Or do you want your deployment to **always require explicit qualification**?

### Info schema is also a config toggle

DataFusion can expose `information_schema` virtual tables, but it’s controlled by config:

* `datafusion.catalog.information_schema` (default shown as **false** in config docs) ([Apache DataFusion][2])
* In Python: `SessionConfig.with_information_schema(enabled=True|False)` ([Apache DataFusion][3])

**Documentation implication:** decide whether you enable it by default, and if not, document how to turn it on for debugging/introspection.

---

## 3) The Python catalog API you’ll actually use (`datafusion.catalog` + `SessionContext`)

In DataFusion Python, `SessionContext` gives you access to catalogs:

* `ctx.catalog(name='datafusion')` (default argument is literally `'datafusion'`)
* `ctx.catalog_names()` ([Apache DataFusion][3])

From there:

### `Catalog` (schemas)

* `catalog.schema(name='public')`
* `catalog.schema_names()`
* `catalog.register_schema(name, schema)` ([Apache DataFusion][4])

### `Schema` (tables)

* `schema.table(name)`
* `schema.table_names()`
* `schema.register_table(name, table)` and `schema.deregister_table(name)` ([Apache DataFusion][4])

And “table” here is flexible: it can be a DataFusion `Table`, a table provider export, a DataFusion `DataFrame`, or a `pyarrow.dataset.Dataset`, etc. ([Apache DataFusion][4])

### A “catalog-aware” registration pattern (the one you’ll want to document)

If you want clean namespacing (rather than dumping everything into the default schema), use the `Catalog`/`Schema` objects explicitly:

```python
from datafusion import SessionContext
from datafusion.catalog import Catalog, Schema
import pyarrow as pa

ctx = SessionContext()

# Build a new in-memory catalog + schema
cat = Catalog.memory_catalog()
raw = Schema.memory_schema()
cat.register_schema("raw", raw)

# Attach catalog into the session context (current API name)
ctx.register_catalog_provider("mycat", cat)

# Register a table into mycat.raw
raw.register_table("events", pa.table({"x": [1, 2, 3]}))

# Query with fully qualified name
ctx.sql("SELECT * FROM mycat.raw.events").show()
```

Notes worth documenting:

* `Catalog.memory_catalog()` / `Schema.memory_schema()` exist specifically for in-memory usage. ([Apache DataFusion][4])
* The `SessionContext` API includes `register_catalog_provider(...)` for wiring catalogs into a session. ([Apache DataFusion][3])

---

## 4) Custom catalogs/schemas: “system integration” level extensibility

If in-memory catalogs aren’t enough, DataFusion Python explicitly supports catalogs implemented in:

* **Rust** (exported to Python via PyO3) — often faster
* **Python** (by subclassing the abstract `CatalogProvider`) ([Apache DataFusion][5])

This is exactly where you’d integrate:

* your own metastore
* “tables” backed by non-file systems
* tenancy + auth enforcement at the catalog layer

That said, for most deployments, you’ll get 80% of the benefit by simply:

* standardizing naming (`raw`, `staging`, `curated`, `views`)
* standardizing DDL (external table definitions)
* standardizing format options

…and leaving custom catalogs for when you truly need a metastore.

---

## 5) Information Schema: what you get and how you’ll use it in docs

DataFusion supports:

* `SHOW TABLES` or `information_schema.tables`
* `SHOW COLUMNS` or `information_schema.columns`
* `SHOW ALL` or `information_schema.df_settings` (session config)
* `SHOW FUNCTIONS` plus info-schema views that list routines/parameters ([Apache DataFusion][6])

This gives you “database-like introspection” even though DataFusion is an engine:

```python
ctx.sql("SHOW TABLES").show()
ctx.sql("SELECT * FROM information_schema.tables").show()

ctx.sql("SHOW COLUMNS FROM mycat.raw.events").show()
ctx.sql("""
  SELECT table_catalog, table_schema, table_name, column_name, data_type, is_nullable
  FROM information_schema.columns
  WHERE table_name = 'events'
""").show()

ctx.sql("SHOW ALL").show()  # same idea as information_schema.df_settings
```

Two practical deployment considerations to document:

1. **Enablement**: depending on how you configure sessions, `information_schema` may be disabled unless you enable it via `SessionConfig.with_information_schema(True)` / config key `datafusion.catalog.information_schema`. ([Apache DataFusion][2])
2. **Exposure**: if you are building an API where users can run SQL, you may *not* want to expose full metadata to all callers (it can leak table names, schemas, etc.). Use SQL gating (next section) and/or restrict catalogs.

---

# (b) SQL/DDL “external table + format options” workflows

This is where DataFusion gets “production-grade IO contracts”: you define tables and **how bytes become Arrow columns** (and how Arrow columns become bytes again).

## 1) Creating schemas (namespacing for IO contracts)

DataFusion SQL supports:

```sql
CREATE SCHEMA [ IF NOT EXISTS ] [ catalog. ] schema_name;
```

…and you can qualify the catalog. ([Apache DataFusion][7])

In docs, this is how you explain “raw vs curated vs views” separation without needing a DuckDB file.

---

## 2) `CREATE EXTERNAL TABLE`: register files/paths as tables (SQL-first contract)

### Parquet example

DataFusion docs show that Parquet external tables don’t require an explicit schema:

```sql
CREATE EXTERNAL TABLE taxi
STORED AS PARQUET
LOCATION '/mnt/nyctaxi/tripdata.parquet';
```

([Apache DataFusion][7])

### CSV example + schema inference + OPTIONS

CSV external tables can infer schema from scanning, and accept `OPTIONS` like `has_header`:

```sql
CREATE EXTERNAL TABLE test
STORED AS CSV
LOCATION '/path/to/aggregate_simple.csv'
OPTIONS ('has_header' 'true');
```

([Apache DataFusion][7])

### Compression in DDL

You can specify compression type in the statement:

```sql
CREATE EXTERNAL TABLE test
STORED AS CSV
COMPRESSION TYPE GZIP
LOCATION '/path/to/aggregate_simple.csv.gz'
OPTIONS ('has_header' 'true');
```

([Apache DataFusion][7])

### DataFusion Python: you run these through `ctx.sql(...)`

In `datafusion` Python, `ctx.sql(...)` supports DDL and DML (it explicitly notes this in the API docs), which is exactly what you’ll want if you adopt “SQL = IO contract.” ([Apache DataFusion][3])

---

## 3) Statistics collection on `CREATE EXTERNAL TABLE` (high impact, easy to miss)

By default, when you create a table, DataFusion may read files to gather statistics; this can be expensive but can speed up later queries. If you don’t want that cost at table creation time, set:

```sql
SET datafusion.execution.collect_statistics = false;
```

*before* creating the external table. ([Apache DataFusion][7])

This is absolutely worth a prominent callout in your docs because it changes “table registration” latency dramatically.

---

## 4) Format Options: your actual IO contract surface (and its precedence rules)

DataFusion’s **Format Options** doc is the key reference because it defines:

* options for **CREATE EXTERNAL TABLE**
* options for **COPY**
* options implied by **INSERT INTO** behavior
* and session-level defaults (lowest precedence)

### Order of precedence (document this clearly)

DataFusion describes three ways to specify options, in decreasing precedence:

1. `CREATE EXTERNAL TABLE` syntax
2. `COPY` option tuples
3. session-level config defaults ([Apache DataFusion][8])

Also important: if you define options when creating a table, then `INSERT INTO my_table ...` will respect them (delimiter/header/compression, etc.). ([Apache DataFusion][8])

### Example: table options via `OPTIONS(...)`

```sql
CREATE EXTERNAL TABLE my_table(a bigint, b bigint)
STORED AS csv
LOCATION '/tmp/my_csv_table/'
OPTIONS(
  NULL_VALUE 'NAN',
  'has_header' 'true',
  'format.delimiter' ';'
);
```

([Apache DataFusion][8])

---

## 5) CSV / JSON format options: the “byte parsing contract”

The format options doc lists a fairly rich set for CSV (delimiter, quote, escape, terminator, null handling, timestamp formats, schema infer limits, etc.). ([Apache DataFusion][8])

Two “deployment-grade” highlights:

### A) Newlines inside quoted CSV values

There is both:

* a CSV option `NEWLINES_IN_VALUES`, and
* a session-level default `datafusion.catalog.newlines_in_values`

The config docs explicitly warn that supporting newlines in quoted values can reduce performance because it can be affected by parallel scanning behavior. ([Apache DataFusion][2])

### B) Schema inference limit

CSV option `SCHEMA_INFER_MAX_REC` lets you tune inference, and setting it to `0` disables inference and treats everything as strings. ([Apache DataFusion][8])

JSON format options include compression controls as well (GZIP/BZIP2/XZ/ZSTD/UNCOMPRESSED). ([Apache DataFusion][8])

---

## 6) Parquet format options: compression/encoding per column (the big one)

DataFusion supports:

* Parquet **global** options (e.g., default compression)
* Parquet **column-specific** overrides using `option::column_name`
* plus other column-specific settings like encoding, dictionary, stats, bloom filters ([Apache DataFusion][8])

### COPY example with per-column compression override

```sql
COPY source_table
  TO 'test/table_with_options'
  PARTITIONED BY (column3, column4)
  OPTIONS (
    format parquet,
    compression snappy,
    'compression::column1' 'zstd(5)'
  );
```

This is the canonical “production IO contract” example: global default + per-column override. ([Apache DataFusion][8])

### What’s supported (examples you should document)

From the Parquet section of format options:

* `COMPRESSION` (column-specific) with codecs like snappy/gzip/brotli/zstd/etc.
* `ENCODING` (column-specific)
* `DICTIONARY_ENABLED` (column-specific)
* `STATISTICS_ENABLED` (column-specific; none/chunk/page)
* Bloom filter controls (enable + FPP) ([Apache DataFusion][8])

If you’re aiming for “DuckDB-like ergonomics,” this is a major part of it: you can encode and compress output in a very controlled way using SQL, without leaving Arrow semantics.

---

## 7) Remote locations + credentials in SQL (powerful, but be careful)

DataFusion’s docs show that `CREATE EXTERNAL TABLE` can include cloud credentials via `OPTIONS(...)` for S3/GCS/etc., and also notes precedence and environment-variable alternatives. ([Apache DataFusion][9])

Example patterns from the docs:

* explicit AWS keys in `OPTIONS(...)`
* environment variables (standard AWS config resolution)
* “skip signature” for public buckets (`aws.SKIP_SIGNATURE`) ([Apache DataFusion][9])

**Python deployment guidance:** even though SQL supports credential options, for a long-running service you’ll usually prefer:

* registering object stores programmatically (`ctx.register_object_store(...)`), and/or
* environment-based auth

…to avoid secrets embedded in SQL text. DataFusion Python’s data sources guide shows the object-store registration pattern directly. ([Apache DataFusion][5])

---

## 8) Guardrails when exposing SQL: `SQLOptions`

If you’re building an API where users submit SQL, the Python library gives you first-class gating:

* `SQLOptions.with_allow_ddl(...)`
* `SQLOptions.with_allow_dml(...)`
* `SQLOptions.with_allow_statements(...)` (covers things like `SET` and `BEGIN TRANSACTION`) ([Apache DataFusion][3])

`ctx.sql_with_options(query, options)` validates the SQL against these. ([Apache DataFusion][3])

That’s a key “deployment considerations” section to include in your documentation if you ever plan to let other systems/users submit SQL.

---


[1]: https://datafusion.apache.org/library-user-guide/catalogs.html "Catalogs, Schemas, and Tables — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/catalog/index.html "datafusion.catalog — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[6]: https://datafusion.apache.org/_sources/user-guide/sql/information_schema.md.txt "datafusion.apache.org"
[7]: https://datafusion.apache.org/user-guide/sql/ddl.html "DDL — Apache DataFusion  documentation"
[8]: https://datafusion.apache.org/user-guide/sql/format_options.html "Format Options — Apache DataFusion  documentation"
[9]: https://datafusion.apache.org/user-guide/cli/datasources.html "Local Files / Directories — Apache DataFusion  documentation"

Below is a deep dive on **“4) SQL surface area (and safety controls)”** for the **`datafusion` Python library** (“DataFusion in Python”), with an explicitly **Arrow-native** posture.

---

## 4.1 SQL entrypoints in `datafusion` (Python)

### `SessionContext.sql(...)` and `SessionContext.sql_with_options(...)`

In DataFusion Python, SQL always starts from a `SessionContext` and returns a **DataFrame (logical plan)**:

* `ctx.sql(query, options=None, param_values=None, **named_params) -> DataFrame` ([Apache DataFusion][1])
* `ctx.sql_with_options(query, options, param_values=None, **named_params) -> DataFrame` (same, but **always validates** the query against the provided `SQLOptions` first). ([Apache DataFusion][1])

The important nuance is: the returned object is a **lazy DataFrame**; execution happens when you call a terminal action like `collect()`, `show()`, or `to_pandas()`. ([Apache DataFusion][2])

### DDL/DML are supported *through the same SQL API*

`ctx.sql(...)` explicitly notes it implements **DDL** (e.g., `CREATE TABLE`, `CREATE VIEW`) and **DML** (e.g., `INSERT INTO`) with an **in-memory default implementation**. ([Apache DataFusion][1])
This is the “engine, not database” flavor: these operations affect the session/catalog unless you point them at an external sink/provider that persists.

---

## 4.2 The SQL surface area: what DataFusion supports (at a high level)

DataFusion maintains a full SQL reference, and the “shape” you should document is:

### Queries

* Core `SELECT` syntax (joins, group by, ordering, limits, subqueries, window functions, etc.). ([Apache DataFusion][3])

### DDL (Data Definition Language)

DataFusion SQL reference lists DDL such as:

* `CREATE DATABASE`, `CREATE SCHEMA`
* `CREATE EXTERNAL TABLE`, `CREATE TABLE`, `DROP TABLE`
* `CREATE VIEW`, `DROP VIEW`
* `DESCRIBE` ([Apache DataFusion][3])

### DML (Data Manipulation Language)

* `COPY`
* `INSERT` ([Apache DataFusion][3])

### Introspection

* `SHOW TABLES`, `SHOW COLUMNS`, `SHOW ALL (configuration options)`, `SHOW FUNCTIONS` ([Apache DataFusion][3])

**Deployment implication:** when you say “we allow SQL,” you need to decide whether you mean:

* *queries only*, or
* queries + DDL/DML + config-setting statements (next section).

---

## 4.3 Safety controls: `SQLOptions` (the “SQL firewall”)

### What `SQLOptions` controls

DataFusion Python exposes `SQLOptions` to gate SQL into three buckets:

* **DDL allowed** (e.g., `CREATE TABLE`, `DROP TABLE`) via `with_allow_ddl()` ([Apache DataFusion][1])
* **DML allowed** (e.g., `INSERT INTO`, `DELETE`) via `with_allow_dml()` ([Apache DataFusion][1])
* **“Statements” allowed** (explicitly called out as things like `SET VARIABLE` and `BEGIN TRANSACTION`) via `with_allow_statements()` ([Apache DataFusion][1])

By default, `SQLOptions` allows all three. ([Apache DataFusion][1])

### How to enforce it

You can enforce these policies in two ways:

1. pass `options=` into `ctx.sql(...)` (it will validate if provided) ([Apache DataFusion][1])
2. or use `ctx.sql_with_options(...)` (always validates first) ([Apache DataFusion][1])

A canonical “read-only SQL” wrapper:

```python
from datafusion import SessionContext, SQLOptions

def read_only_sql(ctx: SessionContext, query: str, *, param_values=None, **named_params):
    opts = (
        SQLOptions()
        .with_allow_ddl(False)
        .with_allow_dml(False)
        .with_allow_statements(False)
    )
    return ctx.sql_with_options(query, opts, param_values=param_values, **named_params)
```

### Why this matters (real-world threat model)

The motivation for gating (from DataFusion maintainers / users) includes:

* preventing users from creating ephemeral memory-backed tables,
* preventing writing to local files via `COPY`,
* preventing session config changes that can trigger unwanted memory usage / denial-of-service. ([GitHub][4])

That maps cleanly onto your own “deployment considerations” chapter: decide which SQL classes are safe for your environment.

---

## 4.4 Parameterized queries and substitution (SQL correctness + injection control)

DataFusion Python has **two distinct substitution mechanisms** that share the same `$name` placeholder syntax, but behave very differently.

### A) `named_params` (string replacement + DataFrame injection)

As of DataFusion-Python **51.0.0**, you can pass named parameters directly to `ctx.sql(...)` using `$name` placeholders, e.g. `... WHERE x > $val`. ([Apache DataFusion][5])

Behavior:

* Normal Python objects are converted to their **string representation** for query text substitution (convenient, but can cause precision/data-loss for some types like floats). ([Apache DataFusion][5])
* If you pass a **DataFrame** as a named parameter, DataFusion will **register a temporary view** under a generated name and substitute it into the SQL. ([Apache DataFusion][5])

Example pattern (inject a DataFrame + a column name + a value):

```python
# df is a DataFusion DataFrame
ctx.sql(
    'SELECT "Name", $col FROM $df WHERE $col > $val',
    col='"Defense"',
    df=df,
    val=75,
).show(num=5)
```

This is directly shown in the SQL user guide. ([Apache DataFusion][5])

**Dialect limitation:** `$name` placeholder formatting works for all parser dialects **except** `hive` and `mysql` (they don’t support named placeholders). ([Apache DataFusion][5])

**Custom catalog warning:** because DataFrame injection relies on creating temporary views, your custom `CatalogProvider`/`SchemaProvider` must support temporary view registration and ensure those temp views don’t persist across `SessionContext`s. ([Apache DataFusion][5])

### B) `param_values` (typed scalar parameters; safer for values)

If you want to avoid string conversion and preserve types, `ctx.sql(..., param_values={...})` lets you provide scalar values that will be cast into **PyArrow Scalar values**. ([Apache DataFusion][5])

This behaves more like “prepared statement parameters”:

* It depends on the configured SQL dialect.
* It’s limited to places you’d use a scalar (e.g., comparisons). ([Apache DataFusion][5])

**Best practice for safety:**

* Use `param_values` for **user-provided scalar values** (avoid injection and avoid string coercion surprises). ([Apache DataFusion][5])
* Use `named_params` **sparingly**, mainly for “safe” structural substitution you control (like injecting a known DataFrame or a column identifier that you sanitize/whitelist).

---

## 4.5 Runtime config via SQL (`SET`) and why it’s a safety boundary

DataFusion supports setting many execution/planning options via SQL:

```sql
SET datafusion.execution.target_partitions = '1';
```

This is documented as a first-class configuration mechanism. ([Apache DataFusion][6])

Config includes things that can drastically change resource usage (parallelism, memory limits, caches, etc.), and the tuning guide explicitly calls out using `SET` for performance and memory behavior. ([Apache DataFusion][6])

**Security posture:** if SQL comes from an untrusted user, you typically want:

* `SQLOptions.with_allow_statements(False)` to block `SET ...` style statements. ([Apache DataFusion][1])

---

## 4.6 Dialect selection (compatibility + parameter rules)

DataFusion’s SQL parser is configurable with:

* `datafusion.sql_parser.dialect` (default `generic`), and supported values include: Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, MsSQL, ClickHouse, BigQuery, Ansi, **DuckDB**, Databricks, etc. ([Apache DataFusion][6])

This matters operationally because:

* placeholder support and quoting rules differ by dialect,
* DataFusion-Python’s `$name` parameter replacement explicitly does **not** work in `hive` and `mysql`. ([Apache DataFusion][5])

In your documentation, it’s worth treating the dialect as part of your “SQL contract” (pick one, document it, test against it).

---

## 4.7 A “deployment-grade” SQL policy matrix (suggested doc artifact)

You’ll likely want to document 2–3 standardized modes:

1. **Dev / trusted analyst mode**

* allow DDL/DML/statements (default)
* allow `CREATE EXTERNAL TABLE`, `COPY`, etc.
* allow `SET` tuning

2. **Service mode (trusted callers, but you still want guardrails)**

* allow DDL: *maybe* (or only through controlled code paths)
* allow DML: often **no** (avoid file writes / mutation)
* allow statements: often **no** (`SET` is privileged)

3. **Multi-tenant / untrusted SQL mode**

* allow DDL: no
* allow DML: no
* allow statements: no
* require `param_values` for user-provided values

All of this maps directly to `SQLOptions` + `sql_with_options`. ([Apache DataFusion][1])

---

If you want the next deep-dive after SQL: the natural follow-on is **(5) DataFrame API operators + ergonomics** *or* **(9) execution/runtime controls** (memory, spilling, partitioning), because those are the other half of “SQL in production.”

[1]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/python/user-guide/dataframe/index.html?utm_source=chatgpt.com "DataFrames — Apache Arrow DataFusion documentation"
[3]: https://datafusion.apache.org/user-guide/sql/index.html "SQL Reference — Apache DataFusion  documentation"
[4]: https://github.com/apache/datafusion/issues/7328?utm_source=chatgpt.com "API for enabling/disabling DDL / DML / Config changes via ..."
[5]: https://datafusion.apache.org/python/user-guide/sql.html "SQL — Apache Arrow DataFusion  documentation"
[6]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"

Below is a deep dive on **“5) DataFrame API (Python): relational operators + ergonomics”** for the **`datafusion`** Python library (DataFusion in Python), with an explicitly **Arrow-native** mindset.

---

## 1) The DataFusion Python DataFrame: the two rules that govern everything

### Rule A: a `DataFrame` is a **lazy logical plan**

Every transformation (`select`, `filter`, `join`, `aggregate`, etc.) returns a **new** `DataFrame` that represents an updated plan. Execution happens only at “terminal” operations like `collect()`, `show()`, `to_pandas()`, etc. ([datafusion.apache.org][1])

### Rule B: results are fundamentally **Arrow RecordBatches**

* `collect()` returns a list of **`pyarrow.RecordBatch`** objects (fully materialized into Python memory). ([datafusion.apache.org][2])
* A `DataFrame` is **iterable** (and async-iterable), yielding record batches lazily. ([datafusion.apache.org][2])
* `__arrow_c_stream__()` exports a stream of Arrow batches *incrementally*, so consumers can pull without materializing the full result set. ([datafusion.apache.org][2])

This is the center of gravity for “Arrow-native”: you can keep everything columnar, and choose *when* you materialize.

---

## 2) Expressions: the ergonomic “glue” of the DataFrame API

Most DataFrame methods accept **expressions** (`Expr`) and/or strings that get parsed into expressions.

### The expression building blocks

* `col("name")` creates a column expression.
* `lit(value)` creates a literal expression.
* Expressions combine into trees (think: compiler AST for a query). ([datafusion.apache.org][3])

### Boolean logic: use **bitwise operators**

To combine boolean expressions you use `&`, `|`, and `~` (not `and/or/not`). ([datafusion.apache.org][3])

### Nested data ergonomics: arrays + structs feel “Pythonic”

* Array element access: `col("a")[0]` (0-based), and slicing like `col("a")[1:3]` (added in DataFusion 49.0.0). ([datafusion.apache.org][3])
* Struct field access: `col("a")["size"]` (dict-like). ([datafusion.apache.org][3])

This is one of the places DataFusion Python can feel nicer than “SQL strings everywhere,” especially for Arrow-native nested schemas.

---

## 3) Core relational operators (and how to use them “the DataFusion way”)

### 3.1 Projection: `select`, `select_columns`, `select_exprs`, `__getitem__`

You have 4 ergonomic styles:

**(1) `select(...)`: mix column names and expressions**

* Strings are treated as “select this column”
* `Expr` objects are computed columns, often with `.alias(...)` ([datafusion.apache.org][2])

```python
from datafusion import col

df2 = df.select("a", col("b"), col("a").alias("alternate_a"))
```

([datafusion.apache.org][2])

**(2) `df["col"]` / `df[["a","b"]]`: quick column selection**
`__getitem__` returns a new DataFrame selecting one or more columns. ([datafusion.apache.org][2])

**(3) `select_columns("a","b")`: only column names**
Convenient when you want “just these columns.” ([datafusion.apache.org][2])

**(4) `select_exprs("a + 1 as b", ...)`: SQL-expression strings**
Parses each string into an expression against the current schema. ([datafusion.apache.org][2])

---

### 3.2 Filtering: `filter(...)`

`filter(*predicates)` supports:

* `Expr` predicates, or
* SQL predicate strings like `"a > 1"`.

If you pass multiple predicates, DataFusion combines them with **AND** automatically. ([datafusion.apache.org][2])

```python
from datafusion import col, lit
df2 = df.filter(col("a") > lit(1), "b IS NOT NULL")
```

([datafusion.apache.org][2])

---

### 3.3 Sorting + limiting: `sort`, `limit`, `head`, `tail`

* `sort(*exprs)` accepts sort keys or column names; any expression can become a sort key via `.sort(...)`. ([datafusion.apache.org][2])
* `limit(count, offset=0)` supports offset paging. ([datafusion.apache.org][2])
* `head(n)` is a convenient limit. ([datafusion.apache.org][2])
* `tail(n)` exists but the docs warn it “could be potentially expensive” because it may need to collect to determine row sizing. ([datafusion.apache.org][2])

---

### 3.4 Joins: `join(...)` and `join_on(...)` (including semi/anti)

DataFusion Python supports join types:
`inner`, `left`, `right`, `full`, `semi`, `anti`. ([datafusion.apache.org][2])

**A) Equi-joins by column name**

```python
df3 = left.join(right, on="id", how="inner")
```

**B) Equi-joins with different key names**

```python
df3 = left.join(right, left_on="customer_id", right_on="id", how="inner")
```

([datafusion.apache.org][4])

**C) Expression joins (inequalities)**
Use `join_on(right, *on_exprs, how=...)` when you need `(in)equality` predicates; equality predicates are “correctly optimized.” ([datafusion.apache.org][2])

```python
from datafusion import col
df3 = left.join_on(right, col("start") <= col("end"), how="inner")
```

**D) Duplicate join keys ergonomics**
Starting in `datafusion-python` **51.0.0**, joins can **coalesce duplicate key columns** (same-name join keys) by default to reduce ambiguous column selection; disable with `coalesce_duplicate_keys=False`. ([datafusion.apache.org][4])

---

### 3.5 Aggregations: `aggregate(group_by, aggs)`

`aggregate(...)` takes:

* `group_by`: column names or expressions
* `aggs`: aggregate expressions (usually from `datafusion.functions as f`) ([datafusion.apache.org][2])

Example style from the docs (approx aggregates):

```python
from datafusion import functions as f, col

df2 = df.aggregate(
    [col('"Type 1"')],
    [
        f.approx_distinct(col('"Speed"')).alias("Count"),
        f.approx_median(col('"Speed"')).alias("Median Speed"),
    ],
)
```

([datafusion.apache.org][5])

---

### 3.6 Window functions: expressions + `functions` + `.over(Window(...))`

Window functions live in `datafusion.functions`. ([datafusion.apache.org][6])

**Simple window functions**

```python
from datafusion import functions as f, col
df2 = df.select(col('"Name"'), col('"Speed"'), f.lag(col('"Speed"')).alias("prev"))
```

([datafusion.apache.org][6])

**Parameterized windows**
You can set `partition_by`, `order_by`, and window frames; for rolling windows you can use `Window` / `WindowFrame` and apply `.over(...)`. ([datafusion.apache.org][6])

**Null treatment**
For some analytic patterns (e.g., last value ignoring nulls), you can control null treatment via `NullTreatment`. ([datafusion.apache.org][6])

---

### 3.7 Set operations: `union`, `union_distinct`, `intersect`, `except_all`

These methods exist and generally require **the same schema** across both DataFrames:

* `union(other, distinct=False)`
* `union_distinct(other)`
* `intersect(other)`
* `except_all(other)` ([datafusion.apache.org][2])

---

### 3.8 “Data cleaning” conveniences: `cast`, `fill_null`, `drop`, `distinct`, `describe`

A bunch of operations that make DataFusion feel less “SQL-only”:

* `cast({col: pyarrow.DataType, ...})` ([datafusion.apache.org][2])
* `fill_null(value, subset=...)` casts the fill value to column type where possible; leaves columns unchanged if casting fails. ([datafusion.apache.org][2])
* `drop(*columns)` is forgiving about quoting (leading/trailing quotes stripped). ([datafusion.apache.org][2])
* `distinct()` removes duplicated rows. ([datafusion.apache.org][2])
* `describe()` returns summary statistics; currently numeric-focused and modeled after pandas. ([datafusion.apache.org][2])

---

## 4) Ergonomics patterns that matter in “real” code

### Pattern A: mix three expression styles intentionally

Many methods accept either `Expr` or SQL strings:

* `filter(col("a") > lit(1))` **or** `filter("a > 1")` ([datafusion.apache.org][2])
* `with_column("b", col("a") + lit(1))` **or** `with_column("b", "a + 1")` ([datafusion.apache.org][2])

This lets you choose:

* **Expr-first** for typeful, compositional code (especially with nested arrays/structs)
* **SQL-string** when it’s easier to author a quick predicate/expression

### Pattern B: `with_columns(...)` + named expressions = “pipeline style”

`with_columns` accepts:

* expressions,
* iterables of expressions,
* SQL expression strings,
* and named expressions via `name=expr` (or `name="sql_expr"`). ([datafusion.apache.org][2])

### Pattern C: `transform(func, *args)` for composable “mini-pipelines”

`transform` applies a function `(DataFrame) -> DataFrame`, making it easy to structure reusable transforms. ([datafusion.apache.org][2])

### Pattern D: turn a DataFrame into a view/table when you want reuse

`into_view()` converts a `DataFrame` into a `Table` that can be registered and queried, while leaving the original DataFrame usable. ([datafusion.apache.org][2])

---

## 5) Terminal operations & diagnostics (what you standardize for “production ergonomics”)

### Terminal / materialization operations

* `collect()` (RecordBatches) ([datafusion.apache.org][2])
* `to_arrow_table()`, `to_pandas()`, `to_polars()`, `to_pydict()`, `to_pylist()` ([datafusion.apache.org][2])
* `execute_stream()` / `execute_stream_partitioned()` for streaming execution (single partition or per-partition). ([datafusion.apache.org][2])
* `__arrow_c_stream__()` for Arrow-native streaming interchange. ([datafusion.apache.org][2])

### Introspection / explainability

* `explain(verbose=False, analyze=False)` prints the plan; with `analyze=True` it runs and reports metrics. ([datafusion.apache.org][2])
* `logical_plan()` vs `optimized_logical_plan()` vs `execution_plan()` for deep debugging. ([datafusion.apache.org][2])

---

## 6) Practical “deployment considerations” for the DataFrame API (Arrow-native)

* Prefer **streaming consumption** (`__arrow_c_stream__`, `execute_stream`, iteration) when results can be large; avoid “accidental full materialization” via `to_pandas()` / `to_arrow_table()` unless you truly need it. ([datafusion.apache.org][2])
* Treat `count()` and `tail()` as “can be expensive” operations on large plans (they run real work). ([datafusion.apache.org][2])
* For joins, decide whether you want the 51.0.0 default behavior of **coalescing duplicate join keys** and document it (it changes downstream column selection ergonomics). ([datafusion.apache.org][4])
* Use `filter("...")` / `with_column("...", "sql expr")` / `select_exprs(...)` when you need **concise** authoring, and `Expr` when you need **composability** and nested data access (`col("a")[0]`, `col("s")["field"]`). ([datafusion.apache.org][2])

---

If you want the next increment: I can turn this into a **“golden DataFrame style guide”** for your project (recommended idioms, anti-patterns, and a canonical set of helper utilities like `df_assert_schema`, `df_stream_to_arrow_reader`, “safe join wrapper w/ coalesce policy,” etc.).

[1]: https://datafusion.apache.org/python/user-guide/dataframe/index.html "DataFrames — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/python/user-guide/common-operations/expressions.html "Expressions — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/python/user-guide/common-operations/joins.html "Joins — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/python/user-guide/common-operations/aggregations.html "Aggregation — Apache Arrow DataFusion  documentation"
[6]: https://datafusion.apache.org/python/user-guide/common-operations/windows.html "Window Functions — Apache Arrow DataFusion  documentation"

Below is a deep dive on **“6) Writing outputs (files + tables) as first-class features”** in the **`datafusion` Python library** (DataFusion in Python), with an Arrow-native / “engine not DB” posture.

---

## 6.0 The writing model: “execute the plan, stream batches into a sink”

In DataFusion, writing is not an afterthought—it’s part of the query engine. Since ~DataFusion 34, the engine supports writing **in parallel**, to **single or multiple files**, across formats like **Parquet / CSV / JSON / ARROW** and even user-defined formats, and it can write to any `ObjectStore` implementation (S3/Azure/GCS/local/custom). ([datafusion.apache.org][1])

In **DataFusion Python**, the main “first class” write surfaces are:

1. **DataFrame → files**: `write_csv`, `write_json`, `write_parquet`, `write_parquet_with_options` ([datafusion.apache.org][2])
2. **DataFrame → registered table**: `write_table` (uses the table provider’s insert implementation; same underlying machinery as SQL `INSERT INTO`) ([datafusion.apache.org][2])
3. **SQL → files / tables**: `COPY ... TO ...` and `INSERT INTO ...` with “Format Options” as the IO contract layer ([datafusion.apache.org][3])

---

## 6.1 DataFrame → files (CSV / JSON / Parquet)

### The basic APIs

From the Python API:

* `df.write_csv(path, with_header=False, write_options=None)` ([datafusion.apache.org][2])
* `df.write_json(path, write_options=None)` ([datafusion.apache.org][2])
* `df.write_parquet(path, compression=Compression.ZSTD, compression_level=None, write_options=None)` ([datafusion.apache.org][2])
* `df.write_parquet_with_options(path, options: ParquetWriterOptions, write_options=None)` ([datafusion.apache.org][2])

Even though the Python docstrings say “a file”, the underlying engine methods are “write … file(s)” (plural) and can write multiple outputs depending on partitioning and execution parallelism. ([Docs.rs][4])

### The “one knob to rule them all”: `DataFrameWriteOptions`

All of the file writers accept `DataFrameWriteOptions`, which is the main ergonomic control plane for **layout**:

```python
DataFrameWriteOptions(
  insert_operation: InsertOp | None = None,
  single_file_output: bool = False,
  partition_by: str | Sequence[str] | None = None,
  sort_by: Expr | SortExpr | Sequence[...] | None = None,
)
```

([datafusion.apache.org][2])

**What each option means in practice:**

* **`partition_by`**: request hive-style partitioned output by column(s) (folder-per-partition). This pairs naturally with DataFusion’s “listing table” model and with SQL `COPY ... PARTITIONED BY (...)`. ([datafusion.apache.org][3])
* **`sort_by`**: request ordering prior to writing (useful to produce more read-friendly files, better compression, and better pruning behavior depending on downstream readers). ([datafusion.apache.org][2])
* **`single_file_output`**: request a single output file instead of a folder of parallel outputs. (In practice, file-vs-folder behavior can be version/provider sensitive—treat this as “requested”, not “guaranteed”, and regression-test it in your target release.) ([datafusion.apache.org][2])
* **`insert_operation`**: mainly matters for `write_table` (APPEND/OVERWRITE/REPLACE), but is part of the same options struct. ([datafusion.apache.org][2])

Also note the Python docs explicitly warn: **not all table providers support all writer options**—so your docs should treat writer options as *capabilities that need verification per sink*. ([datafusion.apache.org][2])

### Example: simple Parquet write with partitioning

```python
from datafusion import SessionContext, DataFrameWriteOptions
import pyarrow as pa

ctx = SessionContext()
df = ctx.from_arrow(pa.table({"day": ["2026-01-10"]*3, "x": [1,2,3]}))

write_opts = DataFrameWriteOptions(
    partition_by=["day"],
    single_file_output=False,
)

df.write_parquet("out/events_parquet/", write_options=write_opts)
```

(Uses `write_parquet` + `DataFrameWriteOptions(partition_by=...)` as documented.) ([datafusion.apache.org][2])

---

## 6.2 Parquet: “basic compression” vs “advanced writer controls”

### Basic Parquet knobs (easy)

`write_parquet` takes:

* `compression` (enum/string; default ZSTD),
* optional `compression_level`. ([datafusion.apache.org][2])

### Advanced Parquet knobs: `write_parquet_with_options(ParquetWriterOptions, ...)`

If you’re documenting “best-in-class” deployment, this is the centerpiece. `ParquetWriterOptions` exposes the engine’s richer Parquet writer controls, including:

* **row group sizing**: `max_row_group_size` (trade memory for compression/read efficiency) ([datafusion.apache.org][2])
* **statistics**: `statistics_enabled` (`none` / `chunk` / `page`) and truncate controls ([datafusion.apache.org][2])
* **dictionary encoding**: `dictionary_enabled` + `dictionary_page_size_limit` ([datafusion.apache.org][2])
* **encoding**: `encoding` (e.g., delta encodings, byte_stream_split) ([datafusion.apache.org][2])
* **bloom filters**: `bloom_filter_on_write` + FPP/NDV (and per-column overrides) ([datafusion.apache.org][2])
* **Arrow metadata**: `skip_arrow_metadata` (whether to embed Arrow metadata in Parquet KV) ([datafusion.apache.org][2])
* **parallel row-group writer controls**: `maximum_parallel_row_group_writers`, `maximum_buffered_record_batches_per_stream`, `allow_single_file_parallelism` ([datafusion.apache.org][2])
* **per-column overrides** via `column_specific_options: dict[str, ParquetColumnOptions]` ([datafusion.apache.org][2])

And `ParquetColumnOptions` lets you override on a per-column basis:

* encoding
* dictionary on/off
* compression (including levels like `zstd(5)`)
* statistics
* bloom filter enabled + parameters ([datafusion.apache.org][2])

### Example: “curated Parquet contract” (global defaults + per-column overrides)

```python
from datafusion import (
    ParquetWriterOptions, ParquetColumnOptions,
    DataFrameWriteOptions
)

pq_opts = ParquetWriterOptions(
    compression="zstd(5)",
    statistics_enabled="page",
    max_row_group_size=1_000_000,
    bloom_filter_on_write=False,  # turn on selectively per-column below
    column_specific_options={
        # High-cardinality equality-filtered IDs: bloom filters can help
        "symbol_id": ParquetColumnOptions(
            bloom_filter_enabled=True,
            bloom_filter_fpp=0.01,
        ),
        # Low-cardinality labels: dictionary encoding often helps
        "kind": ParquetColumnOptions(
            dictionary_enabled=True,
            compression="snappy",
        ),
    },
)

write_opts = DataFrameWriteOptions(partition_by=["repo", "module"])

df.write_parquet_with_options("out/cpg_edges/", pq_opts, write_opts)
```

(Uses the documented option objects and fields.) ([datafusion.apache.org][2])

**When to use bloom filters:** Bloom filters are a compact index that can answer membership queries with “definitely no / probably yes”, with configurable false positive probability; they’re small relative to dictionaries and can help predicate pushdown on high-cardinality columns. ([Parquet][5])

---

## 6.3 SQL-first writing: `COPY` and `INSERT INTO` + “Format Options” as your IO contract

If you want “production-grade IO contracts,” the SQL layer is where DataFusion’s documentation is the most explicit.

### Format Options apply to:

* `COPY`
* `INSERT INTO`
* `CREATE EXTERNAL TABLE` ([datafusion.apache.org][3])

### Option precedence (very important to document)

DataFusion defines precedence (highest → lowest):

1. explicit `CREATE EXTERNAL TABLE` syntax
2. `COPY` option tuples
3. session-level config defaults ([datafusion.apache.org][3])

And if you create a table with format options, `INSERT INTO` respects them (delimiter/header/compression, etc.). ([datafusion.apache.org][3])

### Example: `COPY ... TO ... OPTIONS (...)` with per-column Parquet compression

DataFusion’s Format Options doc includes a canonical pattern:

* write to a folder of Parquet files
* set global compression
* override one column’s compression with `compression::col` ([datafusion.apache.org][3])

This is the SQL counterpart to `ParquetWriterOptions.column_specific_options`.

### `INSERT INTO` external tables

DataFusion 34’s release notes explicitly show the pattern:

* `CREATE EXTERNAL TABLE ... STORED AS PARQUET LOCATION ...`
* `INSERT INTO table SELECT ...`
  and note it writes in parallel and can target object stores (S3/Azure/GCS). ([datafusion.apache.org][1])

---

## 6.4 DataFrame → tables: `write_table` (and why it matters)

### What it does

Python API:

* `df.write_table(table_name, write_options=None)` ([datafusion.apache.org][2])

Docs clarify:

* the table **must be registered**
* **not all table providers support writing**
* behavior depends on the provider implementation ([datafusion.apache.org][2])

Rust docs add key implementation detail:

* `write_table` writes via `TableProvider::insert_into`
* it’s the **same underlying implementation as SQL `INSERT INTO`**
* and it returns a batch with the count of rows written ([Docs.rs][4])

### Insert operation modes (`InsertOp`)

For table writes, `InsertOp` is the semantic contract:

* `APPEND`: add new rows
* `OVERWRITE`: replace all existing rows
* `REPLACE`: replace colliding rows (based on some provider-defined key notion) ([datafusion.apache.org][2])

This is the closest thing DataFusion has to “database-like writes,” but it’s still sink/provider-driven.

---

## 6.5 Operational considerations you should document explicitly

### A) “How many files will be written?”

In DataFusion, output file count is a function of:

* output partitioning / parallel writers,
* partitioned-by layout,
* and execution configs that influence file splitting.

The config surface includes:

* `datafusion.execution.minimum_parallel_output_files`
* `datafusion.execution.soft_max_rows_per_output_file`
* `datafusion.execution.max_buffered_batches_per_output_file` ([datafusion.apache.org][6])

So: don’t assume “one file”; treat file count as a tunable part of the system contract.

### B) Remote writes: object store buffer sizing

If you write outputs to remote object stores, DataFusion has a config knob:

* `datafusion.execution.objectstore_writer_buffer_size` affects the size of chunks uploaded to remote stores (e.g., S3), and may need to be increased for very large output files to avoid remote endpoint errors. ([datafusion.apache.org][6])

### C) Parquet write throughput vs memory

For large Parquet writes, DataFusion notes you may improve performance by increasing:

* `datafusion.execution.parquet.maximum_parallel_row_group_writers`
* `datafusion.execution.parquet.maximum_buffered_record_batches_per_stream`
  if you have idle cores and can afford more memory—especially when writing already in-memory data (like from a cached DataFrame). ([datafusion.apache.org][6])

### D) Partition columns: do you keep them in the written files?

DataFusion has a config:

* `datafusion.execution.keep_partition_by_columns` (whether to keep partition-by columns in output record batches). ([datafusion.apache.org][6])

For hive-style layouts, many systems expect partition columns to be represented by the directory structure rather than duplicated inside every file—decide your policy and test it.

### E) Atomicity and “transactional” expectations

DataFusion can write many files in parallel, potentially to object stores. This is not the same thing as an ACID transaction across files. In practice, production patterns usually look like:

* write to a new “run_id” prefix (immutable),
* then publish a pointer/manifest (or a `_SUCCESS` marker) for readers.

(DataFusion’s engine supports the file creation and object store targeting; your platform supplies the transactional semantics.)

---

## 6.6 Recommended “golden patterns” for your documentation set

1. **Two-tier API guidance**

* **SQL-first**: `CREATE EXTERNAL TABLE` + `INSERT INTO` + `COPY ... OPTIONS` as the canonical IO contract (most portable/reviewable). ([datafusion.apache.org][3])
* **Python-first**: DataFrame `write_*` for programmatic pipelines and `write_parquet_with_options` when you need rich Parquet tuning. ([datafusion.apache.org][2])

2. **Explicit performance tuning appendix**

* output file count knobs + Parquet parallel writer knobs + object store buffer size. ([datafusion.apache.org][6])

3. **A “schema + layout contract” section**

* partition columns, whether they appear in-file, naming conventions, sort keys, row group sizing.

---

If you want the next deep dive after writing, the natural continuation is **(7) Built-in functions + UDF/UDx extensibility** *or* **(9) runtime controls** (memory/spill/parallelism) — because those two sections directly determine whether your writes stay streaming-safe and predictable at scale.

[1]: https://datafusion.apache.org/blog/2024/01/19/datafusion-34.0.0/ "Apache Arrow DataFusion 34.0.0 Released, Looking Forward to 2024 - Apache DataFusion Blog"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/user-guide/sql/format_options.html "Format Options — Apache DataFusion  documentation"
[4]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html "DataFrame in datafusion::dataframe - Rust"
[5]: https://parquet.apache.org/docs/file-format/bloomfilter/?utm_source=chatgpt.com "Parquet Bloom Filter - Apache Software Foundation"
[6]: https://datafusion.apache.org/_sources/user-guide/configs.md.txt "datafusion.apache.org"

Below is a deep dive on **(7) Built-in functions + UDF/UDx extensibility** in the **`datafusion` Python library** (“DataFusion in Python”), with an **Arrow-native** bias: *use built-ins first; when you extend, keep everything operating on Arrow arrays/batches and avoid Python object conversion.* ([datafusion.apache.org][1])

---

## 7.1 Built-in functions: what you get, where they live, and how to discover them

### Where built-ins show up in Python

In DataFusion Python, built-ins are exposed primarily as **expression builders** in `datafusion.functions` (commonly imported as `from datafusion import functions as f`). They return `Expr` objects that you plug into `select`, `filter`, `aggregate`, window expressions, etc. ([datafusion.apache.org][1])

The user guide groups common function “families” (math, conditional, temporal, string, casting, and other), and shows using them directly in `df.select(...)`. ([datafusion.apache.org][1])

### How to discover what’s available (SQL + metadata)

If you want “what functions exist on this deployment/version” (which matters because function catalogs can evolve), use:

* **SQL:** `SHOW FUNCTIONS [LIKE <pattern>]` ([datafusion.apache.org][2])
* **Information schema:**

  * `information_schema.information_schema.routines` (functions + descriptions)
  * `information_schema.information_schema.parameters` (parameters + descriptions) ([datafusion.apache.org][2])

That gives you a reliable introspection surface for documentation (“list functions”, “filter by name”, “verify registration”). ([datafusion.apache.org][2])

### What “built-in” means operationally

Built-ins execute inside DataFusion’s engine (Rust) on Arrow batches; they’re the fastest and most optimizable path (pushdowns, inlining, etc.) compared to Python UDFs. The docs explicitly position built-ins as reducing the need for custom Python functions. ([datafusion.apache.org][1])

### A small “built-ins” usage pattern (DataFrame API)

```python
from datafusion import SessionContext, col, literal
from datafusion import functions as f

ctx = SessionContext()
ctx.register_csv("pokemon", "pokemon.csv")
df = ctx.table("pokemon")

df2 = df.select(
    f.pow(col('"Attack"'), literal(2)) - f.pow(col('"Defense"'), literal(2))
).limit(10)
```

This is the idiomatic style shown in the functions guide: `functions as f` producing expressions used in `select(...)`. ([datafusion.apache.org][1])

---

## 7.2 Extension types: Scalar UDF, Aggregate UDAF, Window UDWF, Table UDTF

DataFusion Python supports **multiple extension “shapes”**, and `SessionContext` has explicit registration methods for each: `register_udf`, `register_udaf`, `register_udwf`, and `register_udtf`. ([datafusion.apache.org][3])

### 7.2.1 Scalar UDFs (`udf`) — “batch in, batch out”

A **scalar UDF** is a Python function that takes **one or more `pyarrow.Array` inputs** and returns a **single `pyarrow.Array` output**. It runs per **record batch** (vectorized batch boundary), even though your logic is conceptually “row by row.” ([datafusion.apache.org][4])

You create one using `udf(func, input_types, return_type, volatility, name=...)` (or as a decorator). ([datafusion.apache.org][5])

**Example (from the docs):**

```python
import pyarrow as pa
import datafusion
from datafusion import udf, col

def is_null(arr: pa.Array) -> pa.Array:
    return arr.is_null()

is_null_udf = udf(is_null, [pa.int64()], pa.bool_(), "stable")

ctx = datafusion.SessionContext()
df = ctx.create_dataframe([[pa.record_batch([pa.array([1, None, 3])], names=["a"])]], name="t")

df.select(col("a"), is_null_udf(col("a")).alias("is_null")).show()
```

This is exactly the pattern from the UDF guide: Arrow arrays in/out, and the UDF is invoked as an expression `is_null_udf(col("a"))`. ([datafusion.apache.org][4])

#### Volatility: immutable vs stable vs volatile (it’s not just semantics—optimizer uses it)

DataFusion Python exposes a `Volatility` enum (and accepts strings `"immutable"`, `"stable"`, `"volatile"`):

* **Immutable:** same output for same input; DataFusion will try to inline during planning. ([datafusion.apache.org][5])
* **Stable:** same output for same input *within a single query* (e.g., `now()`), and can be inlined when possible. ([datafusion.apache.org][5])
* **Volatile:** may change even within a query (e.g., `random()`); cannot be evaluated during planning and is evaluated per row. ([datafusion.apache.org][5])

This volatility choice affects correctness *and* performance, so it’s worth treating as part of your UDF contract. ([datafusion.apache.org][5])

---

### 7.2.2 Aggregate UDAFs (`udaf`) — custom state machines

A **UDAF** requires an **Accumulator** that defines how to:

* `update(...)` the state from incoming input arrays,
* `state()` produce intermediate state values,
* `merge(...)` combine intermediate states,
* `evaluate()` produce the final scalar result. ([datafusion.apache.org][4])

The UDF guide emphasizes why `merge/state` exist: aggregation can be split across batches/partitions, so you need a mergeable intermediate representation. ([datafusion.apache.org][4])

**Doc example structure:** create an `Accumulator`, then wrap it with `udaf(MyAccumulator, input_types, return_type, state_types, volatility)`. ([datafusion.apache.org][4])

---

### 7.2.3 Window UDWFs (`udwf`) — custom window evaluation with performance modes

A **UDWF** is defined via a class implementing `WindowEvaluator` and then wrapped with `udwf(...)`. ([datafusion.apache.org][4])

DataFusion supports *three* evaluation styles (and the docs are explicit about the tradeoffs):

* `evaluate`: compute one row at a time (simplest, least performant) ([datafusion.apache.org][4])
* `evaluate_all`: compute all rows in a partition in one pass (much faster for many window functions) ([datafusion.apache.org][4])
* `evaluate_all_with_rank`: compute based only on rank groups (for rank-driven functions) ([datafusion.apache.org][4])

Which method DataFusion calls depends on flags like `uses_window_frame`, `supports_bounded_execution`, and `include_rank` (documented both in the UDF guide and the `WindowEvaluator` API docs). ([datafusion.apache.org][4])

The guide includes a full UDWF example showing `evaluate_all` producing an output array for the whole partition. ([datafusion.apache.org][4])

---

### 7.2.4 Table UDTFs (`udtf`) — “functions that return tables”

User-defined table functions are different:

* They take `Expr` arguments, but **only literal expressions are supported**.
* They must return a **Table Provider** (the same abstraction used for file tables / datasets / custom providers).
* You register them with `SessionContext.register_udtf(...)`. ([datafusion.apache.org][4])

This is the “engine-native” way to expose dynamic table generation (e.g., parameterized external sources, “generate_series”-style utilities, custom connectors) as SQL-callable objects. ([datafusion.apache.org][4])

---

## 7.3 The Arrow-native performance rule for Python UDFs: **never convert to Python objects unless you must**

DataFusion’s UDF docs and blog posts are extremely blunt about the main footgun:

* Converting Arrow values to Python objects (`.as_py()` / iterating scalars) crosses the Rust↔Python boundary and is **one of the slowest operations** you can do in a DataFusion pipeline. ([datafusion.apache.org][4])
* When possible, use **PyArrow’s array methods and `pyarrow.compute`** inside your UDF so your work stays vectorized on Arrow buffers (often avoiding copies and avoiding per-row Python overhead). ([datafusion.apache.org][4])

The UDF guide explicitly recommends PyArrow compute functions “when possible” because they can operate without copying and greatly improve performance. ([datafusion.apache.org][4])

**Practical guideline you can codify in your docs:**

* ✅ “Good” UDF: `pyarrow.compute.*` + array methods → returns an Arrow array
* ❌ “Bad” UDF: list comprehensions over `value.as_py()` → returns `pa.array([...])`

(You still *can* do the latter; it’s just the slow path and should be treated as exceptional.) ([datafusion.apache.org][4])

---

## 7.4 Rust-backed UDFs via PyCapsule: when Python isn’t fast enough

The `datafusion.user_defined` API supports creating UDFs/UDAFs/UDWFs from **Rust-backed implementations exported via PyCapsule** (FFI). For example, `ScalarUDF.from_pycapsule(...)` exists, and the docs note that if you have a Rust-backed UDF/UDAF/UDWF inside a PyCapsule you can pass it and let DataFusion infer the rest. ([datafusion.apache.org][5])

This is the clean escalation path for “hot” logic:

1. prototype quickly as Python UDF using `pyarrow.compute`,
2. if still too slow, move the implementation to Rust and expose it to Python with the same UDF surface. ([datafusion.apache.org][5])

---

## 7.5 A deployment-ready “function strategy” you can document

A useful ordering (and what I’d recommend you standardize in your docs) is:

1. **Built-in functions** (`datafusion.functions`) wherever possible. ([datafusion.apache.org][1])
2. **SQL introspection** of built-ins: `SHOW FUNCTIONS LIKE ...` + `information_schema...routines/parameters` to make behavior/version explicit in docs. ([datafusion.apache.org][2])
3. **Python UDFs** only when there’s no built-in equivalent — and written “Arrow-native” using PyArrow compute/array ops. ([datafusion.apache.org][4])
4. **Rust-backed UDFs** (PyCapsule) for performance-critical custom logic. ([datafusion.apache.org][5])


[1]: https://datafusion.apache.org/python/user-guide/common-operations/functions.html "Functions — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/user-guide/sql/information_schema.html "Information Schema — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/python/user-guide/common-operations/udf-and-udfa.html "User-Defined Functions — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/python/autoapi/datafusion/user_defined/index.html "datafusion.user_defined — Apache Arrow DataFusion  documentation"

## 8) Query planning & optimization in DataFusion (what makes it “engine-grade”)

This section is about what happens between “I wrote SQL / chained a DataFrame” and “Rust threads are decoding Arrow batches.”

---

# 8.1 The pipeline: SQL/DataFrame → logical plan → optimized logical plan → physical plan → execution

DataFusion runs queries through the standard “query engine” pipeline:

1. **Build an initial `LogicalPlan`**

   * SQL: parser + planner → logical plan
   * DataFrame API: constructs a logical plan directly
2. **Logical optimizer** rewrites the plan (rule/passes) into an equivalent plan that is faster.
3. **Physical planning** chooses physical operators (`ExecutionPlan`).
4. **Physical optimizer** rewrites the physical plan (and/or inserts operators) for better parallelism / locality / algorithm selection.
5. **Execution engine** runs the physical plan to produce Arrow `RecordBatch` streams. ([Apache DataFusion][1])

In the DataFusion codebase/docs this is explicitly framed as **rewrites of `LogicalPlan` (logical optimizer) and rewrites of `ExecutionPlan` (physical optimizer)**. ([Apache DataFusion][2])

---

# 8.2 Rule sets: logical optimizer vs physical optimizer (and why you should care)

### Logical optimizer: “always optimizations” + semantic rewrites

DataFusion highlights the classic rule-based “always good” transformations:

* **Predicate pushdown** (filter earlier)
* **Projection pushdown** (drop unused columns earlier)
* **Limit pushdown** (stop generating rows earlier)
* **Expression simplification / constant folding**
* **Join rewrites** like `OUTER JOIN → INNER JOIN` when provably safe
  …all explicitly discussed as core DataFusion optimizer behavior. ([Apache DataFusion][3])

These are the transformations you expect to see when comparing `logical_plan()` vs `optimized_logical_plan()` in Python. ([Apache Arrow][4])

### Physical optimizer: distribution, parallelism, operator choice

Physical optimization is where DataFusion does things like:

* add **round-robin repartitioning** to increase parallelism (`enable_round_robin_repartition`)
* **reorder join keys** for better execution (`top_down_join_key_reordering`)
* pick join algorithms (e.g., prefer **HashJoin** over SortMergeJoin via `prefer_hash_join`, with explicit memory tradeoffs)
* push down newer runtime techniques like **dynamic filters** into scan operators (more below) ([Apache DataFusion][5])

### How many passes / how strict?

Two “this is an engine” knobs that matter operationally:

* `datafusion.optimizer.max_passes` limits how many times the optimizer tries to reach a fixpoint.
* `datafusion.optimizer.skip_failed_rules` decides whether a rule failure aborts the query or is skipped with a warning. ([Apache DataFusion][5])

---

# 8.3 Pushdown catalog: predicate, projection, limit — what they *actually* do in DataFusion

## Predicate & projection pushdown (plan-level)

These rewrites try to place filters/projections as close as possible to the data source. DataFusion’s docs/blogs emphasize this is particularly effective for columnar storage like Parquet because you can avoid decoding/transporting unnecessary data. ([Apache DataFusion][6])

**What you’ll see in plans:** scan nodes (`DataSourceExec` in physical plans) typically show predicates and projected columns when pushdown succeeds, especially in `EXPLAIN` output. ([Apache DataFusion][7])

## Limit pushdown + TopK

Limit pushdown is more than “move `LIMIT` downward”:

* DataFusion can replace “full sort + limit” with a **TopK** operator (heap-based) when applicable.
* And importantly for “lakehouse tables”: DataFusion’s Parquet reader can **stop fetching/opening additional files once the limit is satisfied**. ([Apache DataFusion][6])

That second point is one of the quiet reasons DataFusion can be very fast for “give me the top N newest rows” patterns when the data layout cooperates (sorted/partitioned). ([Apache DataFusion][6])

---

# 8.4 Join reordering & join selection: what DataFusion does (and what it intentionally doesn’t)

DataFusion advertises **join reordering** as a built-in optimization, but its philosophy is deliberately pragmatic:

* The DataFusion team explicitly states DataFusion **does not include a sophisticated cost-based optimizer**; instead it provides a **reasonable default** plus strong extension points. ([Apache DataFusion][6])
* It includes a “syntactic optimizer” (joins initially in query order) plus **basic join reordering to prevent join disasters**, and supports table/column statistics + selectivity estimation APIs to enable more specialized optimizers. ([Apache DataFusion][6])

So for deployment docs, the right “contract” is:

* expect sane default behavior and some reordering,
* but if your workload depends on deep, stats-heavy join ordering, you may want custom optimizer passes or carefully curated SQL patterns. ([Apache DataFusion][6])

---

# 8.5 Parquet-focused optimizations (the stuff that usually moves the needle most)

DataFusion’s built-in Parquet integration includes multiple layers of “read less”:

### A) Metadata-based pruning (before reading data)

DataFusion supports:

* **Row group** and **data page pruning** using Parquet min/max statistics
* **Row group pruning using Parquet Bloom filters** (when present) ([Apache DataFusion][3])

### B) “Late materialization” / filter pushdown inside the Parquet reader (row-level)

Beyond pruning whole row groups, DataFusion also implements **filter pushdown inside the Parquet reader** (sometimes described as late materialization): decode only filter columns first, then decode other columns only for matching rows. ([Apache DataFusion][8])

**Important nuance:** the DataFusion blog notes Parquet filter pushdown exists but historically has **not been enabled by default** due to regressions. ([Apache DataFusion][9])
In DataFusion Python you can explicitly enable it via config: `datafusion.execution.parquet.pushdown_filters = true`. ([Apache DataFusion][10])

### C) Parquet pruning vs Parquet pushdown: two separate knobs

In Python, you’ll commonly see (and document) both:

* `SessionConfig.with_parquet_pruning(True/False)` (metadata pruning)
* `set("datafusion.execution.parquet.pushdown_filters", "true")` (late-materialization filter pushdown) ([Apache DataFusion][10])

---

# 8.6 Automatic Parquet metadata caching (DataFusion 50+): what it is and why it matters

Starting in **DataFusion 50.0.0**, DataFusion **automatically caches Parquet metadata** (statistics, page indexes, etc.) when using the built-in **`ListingTable`** provider, to avoid repeated disk/network round trips and decoding. ([Apache DataFusion][11])

Key operational details worth baking into your docs:

* Controlled by runtime config: **`datafusion.runtime.metadata_cache_limit`**

  * Default cache is **LRU** and uses up to **50MB** by default
  * Setting limit to **0 disables** caching
  * Cache is **invalidated automatically** if the underlying file changes ([Apache DataFusion][11])
* Works automatically for custom table providers **if they use `ParquetFormat`**; otherwise you can provide a cached reader factory (`CachedParquetFileReaderFactory`) when building a Parquet source. ([Apache DataFusion][11])
* You can observe the benefit directly in `EXPLAIN ANALYZE` metrics (e.g., `metadata_load_time` dropping dramatically). ([Apache DataFusion][11])

This is one of the highest-ROI “lakehouse over S3” optimizations because metadata fetch latency can dominate “many small point reads over large files.” ([Apache DataFusion][11])

---

# 8.7 How to *see* optimizations in DataFusion Python (your “contract harness” primitives)

You want a standard workflow like:

### A) Compare logical vs optimized logical vs physical

DataFusion Python exposes:

* `df.logical_plan()`
* `df.optimized_logical_plan()`
* `df.execution_plan()`
* `df.explain(verbose=False, analyze=False)` ([Apache Arrow][4])

### B) Use SQL `EXPLAIN` / `EXPLAIN ANALYZE`

`EXPLAIN` shows logical + physical plans, and `EXPLAIN ANALYZE` includes runtime metrics. ([Apache DataFusion][7])

### C) Example: “prove Parquet pruning + metadata cache are working”

```python
from datafusion import SessionConfig, SessionContext

config = (
    SessionConfig()
    .with_parquet_pruning(True)
    .set("datafusion.execution.parquet.pushdown_filters", "true")  # optional; test carefully
)

ctx = SessionContext(config)

ctx.register_parquet("t", "s3://bucket/path/to/table/")

# enable/size metadata cache (DataFusion 50+)
ctx.sql("SET datafusion.runtime.metadata_cache_limit = '50M'").collect()

df = ctx.sql("EXPLAIN ANALYZE SELECT * FROM t WHERE k = 42 LIMIT 1")
print(df.to_arrow_table())  # or df.collect()
```

What you look for in the plan/metrics:

* scan node shows `predicate=...` (predicate pushdown)
* projected columns are minimal (projection pushdown)
* `metadata_load_time` is low on repeated runs (metadata cache) ([Apache DataFusion][11])

---

# 8.8 Deployment knobs that most directly affect “engine-grade” optimization

If you only document a handful of tunables, these are usually the ones:

### Optimizer behavior / plan shape

* `datafusion.optimizer.max_passes`
* `datafusion.optimizer.skip_failed_rules`
* `datafusion.optimizer.enable_sort_pushdown`
* `datafusion.optimizer.prefer_hash_join` (+ memory tradeoff) ([Apache DataFusion][5])

### Dynamic filters (runtime pushdown into scans)

* `datafusion.optimizer.enable_dynamic_filter_pushdown`
* plus per-source toggles: TopK / Join / Aggregate dynamic filter pushdown flags ([Apache DataFusion][5])

### Parquet scan controls

* `with_parquet_pruning(...)`
* `datafusion.execution.parquet.pushdown_filters` (late materialization; not always default) ([Apache DataFusion][10])

### Metadata cache

* `datafusion.runtime.metadata_cache_limit` (DataFusion 50+) ([Apache DataFusion][11])

---


[1]: https://datafusion.apache.org/blog/2025/06/15/optimizing-sql-dataframes-part-one/ "Optimizing SQL (and DataFrames) in DataFusion, Part 1: Query Optimization Overview - Apache DataFusion Blog"
[2]: https://datafusion.apache.org/library-user-guide/query-optimizer.html "Query Optimizer — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/user-guide/features.html "Features — Apache DataFusion  documentation"
[4]: https://arrow.staged.apache.org/datafusion-python/generated/datafusion.DataFrame.html "datafusion.DataFrame — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[6]: https://datafusion.apache.org/blog/2025/06/15/optimizing-sql-dataframes-part-two/ "Optimizing SQL (and DataFrames) in DataFusion, Part 2: Optimizers in Apache DataFusion - Apache DataFusion Blog"
[7]: https://datafusion.apache.org/user-guide/sql/explain.html "EXPLAIN — Apache DataFusion  documentation"
[8]: https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/ "Efficient Filter Pushdown in Parquet - Apache DataFusion Blog"
[9]: https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/ "Parquet Pruning in DataFusion: Read Only What Matters - Apache DataFusion Blog"
[10]: https://datafusion.apache.org/python/user-guide/configuration.html "Configuration — Apache Arrow DataFusion  documentation"
[11]: https://datafusion.apache.org/blog/2025/09/29/datafusion-50.0.0/ "Apache DataFusion 50.0.0 Released - Apache DataFusion Blog"

## 9) Execution/runtime controls in **DataFusion in Python** (`datafusion`)

At a high level, **DataFusion Python splits “runtime control” into two layers**:

* **`SessionConfig`**: planner/executor knobs (partitioning, auto-repartition, batch size, parquet options, etc.)
* **`RuntimeEnvBuilder`**: resource governance knobs (memory pool policy/size + disk manager/temp files)

You pass both when creating a `SessionContext`. ([Apache DataFusion][1])

---

# 9.1 Parallelism & partitioning

## The core concept: *partitions are the unit of parallel execution*

DataFusion “uses partitions to parallelize work,” and `target_partitions` is the primary knob to increase/decrease concurrency. ([Apache DataFusion][1])

### `target_partitions`: your global concurrency target

In Python:

```python
from datafusion import SessionConfig, SessionContext

ctx = SessionContext(SessionConfig().with_target_partitions(16))
```

This is explicitly documented as “allow up to N concurrent partitions” and “increasing partitions can increase concurrency.” ([Apache DataFusion][1])

### Where partitions come from (important mental model)

* Some sources naturally produce many partitions (e.g., lots of files in a dataset / listing table).
* Others produce very few (e.g., a single file), and DataFusion will not magically use all cores unless it **repartitions** the data during the plan. The tuning docs note DataFusion tries to maximize parallelism and you can inspect partition counts in `EXPLAIN`. ([Apache DataFusion][2])

## Automatic repartitioning knobs (joins/aggs/windows)

DataFusion Python exposes the “auto-shuffle” toggles directly:

```python
config = (
    SessionConfig()
    .with_target_partitions(16)
    .with_repartition_joins(True)
    .with_repartition_aggregations(True)
    .with_repartition_windows(True)
)
ctx = SessionContext(config)
```

These are explicitly described as “Automatic repartitioning for joins, aggregations, window functions… can be enabled to increase parallelism.” ([Apache DataFusion][1])

### A subtle but crucial tradeoff

Auto-repartition improves CPU utilization, but it also:

* adds shuffle overhead (CPU + memory + possible disk spill), and
* can hurt small queries where overhead dominates.

The tuning guide recommends setting `target_partitions` smaller (even `1`) for very small datasets to avoid repartition overhead. ([Apache DataFusion][2])

## Manual repartitioning on a DataFrame (when you want control)

DataFusion Python supports:

* even repartitioning: `df.repartition(N)`
* hash partitioning: `df.repartition_by_hash(col("a"), num=N)` ([Apache DataFusion][1])

These are your “I know better than the planner” tools for:

* single-file inputs,
* heavily skewed data,
* stable partitioning before a big join/agg.

## File scan repartitioning (advanced but often high-ROI)

The Python API also exposes scan-level repartition controls:

* `with_repartition_file_scans(True/False)`
* `with_repartition_file_min_size(size)` ([Apache DataFusion][3])

This matters when you have **large files** (few natural partitions) and want more parallelism during scanning.

---

# 9.2 Memory limits + pools + spill-to-disk

## The “resource manager”: RuntimeEnv (under the hood)

In core DataFusion, the **`RuntimeEnv`** is the resource container: it holds the **memory pool**, **disk manager** (spill files), **cache manager**, and **object store registry**. ([Docs.rs][4])

In Python, you don’t typically construct `RuntimeEnv` directly; instead you configure it via **`RuntimeEnvBuilder`** and pass that into `SessionContext`. ([Apache DataFusion][1])

---

## Memory governance is *MemoryPool-based* (and DataFusion is explicit about what it tracks)

DataFusion’s memory accounting is not “track every allocation.” It **primarily limits operators that need large, input-size-proportional memory** (e.g., hash aggregation), and those operators must reserve memory from the pool before allocating more. ([Docs.rs][5])

If a reservation fails:

* operators either **spill** and retry, or
* **error** if they can’t spill (or don’t implement spilling). ([Docs.rs][5])

The docs give concrete examples:

* `CrossJoin` uses the memory pool but (in that scenario) **does not spill** → it errors when it can’t reserve more memory.
* `Aggregate` can **spill intermediate buffers to disk**, release memory, and continue. ([Docs.rs][5])

Also: a `MemoryPool` can be shared across concurrently executing plans (useful for multi-tenant / “global budget” setups). ([Docs.rs][5])

---

## Choose your memory pool policy in Python

DataFusion’s MemoryPool implementations include:

* **Unbounded** (default)
* **Greedy** (fixed limit, first-come-first-served)
* **FairSpillPool** (fixed limit, “fair” allocation to spilling operators) ([Docs.rs][5])

Python exposes the key choices via `RuntimeEnvBuilder`:

### Greedy vs Fair spill pool (the “which one?” rule)

* `with_greedy_memory_pool(size)` is recommended for queries that **don’t need to spill** or have **a single spillable operator**. ([Apache DataFusion][3])
* `with_fair_spill_pool(size)` is positioned for queries with **multiple spillable operators** likely to spill; but it may cause spills even when memory exists that’s “reserved” for other operators. ([Apache DataFusion][3])

### Python runtime builder template

```python
from datafusion import RuntimeEnvBuilder

runtime = (
    RuntimeEnvBuilder()
    .with_disk_manager_os()
    .with_fair_spill_pool(2 * 1024**3)  # bytes
)
```

The Python docs show this exact pattern (disk manager + fair spill pool) as the standard way to configure runtime memory/disk behavior. ([Apache DataFusion][1])

---

## Disk manager: spilling only works if you configure it

The DiskManager “manages files generated during query execution, e.g. spill files.” ([Docs.rs][6])

In Python, you can:

* disable it entirely: `with_disk_manager_disabled()` (then “attempts to create temporary files will error”) ([Apache DataFusion][3])
* use OS temp dir: `with_disk_manager_os()` ([Apache DataFusion][3])
* specify one or more spill paths: `with_disk_manager_specified(*paths)` ([Apache DataFusion][3])

There are also execution settings that explicitly become irrelevant if sorts can’t spill because no DiskManager is configured. ([Apache DataFusion][2])

### Temp file path

Separately, `with_temp_file_path(path)` sets where “any needed temporary files” are created. ([Apache DataFusion][3])

(For production docs: treat “spill directory” as a *first-class deployment resource*—fast local SSD, monitored free space, predictable lifecycle.)

---

## Runtime limits you can set via SQL: memory cap + temp directory + max temp usage

DataFusion also supports **runtime configuration via SQL `SET`**, including:

* `datafusion.runtime.memory_limit` (max memory for query execution)
* `datafusion.runtime.temp_directory` (where temp files go)
* `datafusion.runtime.max_temp_directory_size` (disk guardrail; default shown as 100G) ([Apache DataFusion][2])

Example (Python):

```python
ctx.sql("SET datafusion.runtime.memory_limit = '8G'").collect()
ctx.sql("SET datafusion.runtime.temp_directory = '/mnt/df-spill'").collect()
ctx.sql("SET datafusion.runtime.max_temp_directory_size = '50G'").collect()
```

Runtime settings via `SET` are documented as first-class. ([Apache DataFusion][2])

---

## Spilling behavior tuning: `target_partitions` and `batch_size` interact with memory pools

DataFusion’s tuning guide calls out a key interaction:

* With **FairSpillPool**, memory is divided evenly among partitions.
* Higher `datafusion.execution.target_partitions` → **less memory per partition**, so out-of-core paths may trigger more often. ([Apache DataFusion][2])
* During spill readback, batches are read in `datafusion.execution.batch_size` chunks; **smaller `batch_size` can reduce subsequent spill cascades** in tight-memory setups. ([Apache DataFusion][2])

Practical takeaway for “hard memory cap” deployments:

* **Reduce `target_partitions`** (don’t overshard)
* **Consider reducing `batch_size`** when you see repeated spill/merge behavior ([Apache DataFusion][2])

---

## Recent engine improvements that matter for deployment expectations

Two notable recent changes (worth calling out in your docs as “version-sensitive behavior”):

* **Compressed spill files** (sorting + grouping) were added (DataFusion 49.0.0) to reduce spill size and improve readback performance. ([Apache DataFusion][7])
* **“Improved spilling sorts”** (DataFusion 50.0.0) introduced multi-level merge sorts, making it “possible to execute almost any sorting query” that used to OOM by relying on disk spilling. ([Apache DataFusion][8])

---

# 9.3 Multi-context resource governance: sharing RuntimeEnv

In core DataFusion (Rust), the rule is explicit:

* By default, each `SessionContext` creates a new `RuntimeEnv`, so memory/disk limits are **not** enforced across different contexts.
* To enforce process-wide limits (total memory/disk across all queries), all contexts should use the **same `RuntimeEnv`**. ([Docs.rs][9])

In **DataFusion Python**, the public constructor takes a **`RuntimeEnvBuilder`**, not an already-built `RuntimeEnv`. ([Apache DataFusion][3])
So the practical guidance for Python deployments is usually:

* **Prefer a single long-lived `SessionContext` per process** (true shared resource governance in practice).
* If you must have “multiple sessions,” treat them as *logical sessions* layered on one context (catalog/schema separation + views), rather than separate `SessionContext` instances—unless/until the Python binding exposes a shared-runtime primitive.

---

## A “production-ish” Python template (CPU + spill-safe)

```python
from datafusion import SessionContext, SessionConfig, RuntimeEnvBuilder

runtime = (
    RuntimeEnvBuilder()
    .with_disk_manager_specified("/mnt/df-spill")
    .with_fair_spill_pool(8 * 1024**3)  # 8 GiB pool (bytes)
)

config = (
    SessionConfig()
    .with_target_partitions(16)
    .with_repartition_joins(True)
    .with_repartition_aggregations(True)
    .with_repartition_windows(True)
)

ctx = SessionContext(config, runtime)

# runtime guardrails (engine-level)
ctx.sql("SET datafusion.runtime.temp_directory = '/mnt/df-spill'").collect()
ctx.sql("SET datafusion.runtime.max_temp_directory_size = '200G'").collect()
ctx.sql("SET datafusion.runtime.memory_limit = '16G'").collect()
```

This aligns with the documented Python config/runtime model and the engine runtime settings model. ([Apache DataFusion][1])

---


[1]: https://datafusion.apache.org/python/user-guide/configuration.html "Configuration — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[4]: https://docs.rs/datafusion/latest/datafusion/execution/runtime_env/struct.RuntimeEnv.html "RuntimeEnv in datafusion::execution::runtime_env - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/execution/memory_pool/trait.MemoryPool.html "MemoryPool in datafusion::execution::memory_pool - Rust"
[6]: https://docs.rs/datafusion/latest/datafusion/execution/index.html "datafusion::execution - Rust"
[7]: https://datafusion.apache.org/blog/2025/07/28/datafusion-49.0.0/ "Apache DataFusion 49.0.0 Released - Apache DataFusion Blog"
[8]: https://datafusion.apache.org/blog/2025/09/29/datafusion-50.0.0/ "Apache DataFusion 50.0.0 Released - Apache DataFusion Blog"
[9]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html "SessionContext in datafusion::execution::context - Rust"

## 10) Plan introspection + diagnostics in **DataFusion (Python `datafusion`)**

This section is the “prove it” toolkit: how you *see* what DataFusion planned, what it actually executed, and whether settings like **repartitioning**, **spilling**, and **memory caps** are doing what you think.

---

# 10.1 The three introspection surfaces you should standardize on

### A) SQL `EXPLAIN …` family (best for artifacts + tests)

DataFusion SQL supports:

* `EXPLAIN [ANALYZE] [VERBOSE] [FORMAT format] statement`  ([Apache DataFusion][1])

This is the most “portable” surface because it returns a tabular result (`plan_type`, `plan`) you can capture as Arrow and snapshot in CI. ([Apache DataFusion][1])

### B) `DataFrame.explain(verbose=…, analyze=…)` (fast manual debugging)

In `datafusion` Python, a `DataFrame` has:

* `df.explain(verbose: bool = False, analyze: bool = False)`
  If `analyze=True`, it **runs** and prints metrics. ([Apache DataFusion][2])

Use this when you’re iterating interactively; use SQL `EXPLAIN …` when you need to capture output programmatically.

### C) Plan objects: `LogicalPlan` + `ExecutionPlan` (best for “structured” inspection)

Python gives you plan objects directly:

* `df.logical_plan()` and `df.optimized_logical_plan()` return `datafusion.plan.LogicalPlan` ([Apache DataFusion][2])
* `df.execution_plan()` returns `datafusion.plan.ExecutionPlan` ([Apache DataFusion][2])

These objects support stable string/graph outputs and some lightweight structure (e.g., physical plan `partition_count`, `children()`). ([Apache DataFusion][3])

---

# 10.2 `EXPLAIN` formats: pick the right “lens” for the job

### `FORMAT TREE`: quickest “what’s the shape?” view

Tree format is explicitly modeled after DuckDB plans and meant to make the high-level structure obvious. ([Apache DataFusion][1])
It also tends to surface **partitioning scheme** clearly (e.g., `Hash([b@0], 16)`). ([Apache DataFusion][4])

### `FORMAT INDENT`: the “workhorse” (and required for `EXPLAIN ANALYZE`)

Indent format shows both logical + physical plans with hierarchy via indentation. ([Apache DataFusion][1])
`EXPLAIN VERBOSE` supports only indent, and **`EXPLAIN ANALYZE` supports only indent**. ([Apache DataFusion][4])

A key benefit: in indent output you’ll often see explicit `RepartitionExec` nodes with the partition count and `input_partitions=…`. ([Apache DataFusion][1])

### `FORMAT PGJSON`: feed plan visualizers

PGJSON is modeled after Postgres JSON plan output and can be consumed by plan visualization tools (the docs call out Dalibo’s viewer). ([Apache DataFusion][1])

### `FORMAT GRAPHVIZ`: produce a DOT graph

Graphviz format emits DOT language that you can render with Graphviz tooling. ([Apache DataFusion][1])

---

# 10.3 Controlling plan display (so your logs don’t turn into soup)

DataFusion has a bunch of config knobs that make `EXPLAIN` output *actually usable*:

* `datafusion.explain.format` (`indent` or `tree`) + `datafusion.explain.tree_maximum_render_width` ([Apache DataFusion][5])
* `datafusion.explain.show_sizes` (partition sizes) / `show_schema` / `show_statistics` ([Apache DataFusion][5])
* `datafusion.explain.logical_plan_only` / `physical_plan_only` ([Apache DataFusion][5])
* `datafusion.explain.analyze_level` = `summary` vs `dev` (metrics verbosity) ([Apache DataFusion][5])

Practical policy:

* default `summary` in prod logs
* bump to `dev` when debugging an incident ([Apache DataFusion][5])

---

# 10.4 `EXPLAIN VERBOSE` and “why did the optimizer do that?”

Two different “verbose” ideas:

### `EXPLAIN VERBOSE` (SQL)

The “Reading Explain Plans” guide says `EXPLAIN VERBOSE` shows information omitted from normal explain *and* “all intermediate physical plans DataFusion generates before returning,” which is great for debugging when/why operators were added/removed. ([Apache DataFusion][6])

### `EXPLAIN VERBOSE` as “rule-by-rule” debugging

DataFusion’s optimizer docs explicitly call out `EXPLAIN VERBOSE` as a way to see the effect of each optimization rule on a query. ([Apache DataFusion][7])

---

# 10.5 `EXPLAIN ANALYZE`: runtime metrics, and how to read them

### Key semantics

* `EXPLAIN ANALYZE` **executes** the query and returns the plan annotated with runtime metrics. ([Apache DataFusion][4])
* By default it shows **aggregated metrics across all partitions**; use `EXPLAIN ANALYZE VERBOSE` to see per-partition metrics (super useful for skew). ([Apache DataFusion][4])
* You can control metric detail via `datafusion.explain.analyze_level` (`summary` vs `dev`). ([Apache DataFusion][4])

### Metrics you’ll commonly lean on

DataFusion’s metrics guide defines “baseline” metrics that appear in most operators:

* `elapsed_compute` (CPU time spent in operator)
* `output_rows`
* `output_bytes`
* `output_batches` ([Apache DataFusion][8])

Filter operators can show `selectivity = output_rows / input_rows`. ([Apache DataFusion][8])

As of DataFusion **51.0.0**, `EXPLAIN ANALYZE` output was improved to include more execution-time and memory-usage metrics, plus additional operator-specific metrics (e.g., filter selectivity, aggregate reduction factor). ([Apache DataFusion][9])

### Repartition diagnostics (the big one for “why is this slow?”)

DataFusion’s `EXPLAIN ANALYZE` example explicitly shows `RepartitionExec` metrics like `sendTime`, `fetchTime`, and `repartitionTime`. ([Apache DataFusion][4])
Those numbers are your “shuffle tax” — if they dominate, tune partition counts / join strategy / early filtering.

### Spill diagnostics (did we spill? how much?)

DataFusion has explicit spill-related metrics tracked per operator:

* `spill_file_count`, `spilled_bytes`, `spilled_rows` ([Docs.rs][10])

And spillable operators like `SortExec` are designed to spill to disk when data exceeds the memory budget. ([Docs.rs][11])

---

# 10.6 Programmatic plan APIs in `datafusion` Python (for snapshots and automated checks)

### Logical plans

```python
lp = df.logical_plan()                 # unoptimized
olp = df.optimized_logical_plan()      # optimized
print(lp.display_indent())
print(olp.display_indent())
print(lp.display_graphviz())           # DOT output
```

`LogicalPlan` supports `display_*` methods and can emit Graphviz DOT. ([Apache DataFusion][2])

### Physical plans

```python
pp = df.execution_plan()
print(pp.display_indent())
print(pp.partition_count)
for child in pp.children():
    ...
```

`ExecutionPlan` supports `display_indent()`, `children()`, and `partition_count`. ([Apache DataFusion][3])

### Plan serialization (advanced reproducibility)

Both `LogicalPlan` and `ExecutionPlan` support `to_proto()` / `from_proto()` (with current limitations around in-memory tables from record batches). ([Apache DataFusion][3])

---

# 10.7 “Proof recipes” for the three things you care about

## A) Prove parallelism + repartitioning

1. Set `datafusion.execution.target_partitions` (and auto-repartition toggles if needed)
2. Run `EXPLAIN FORMAT INDENT …` and look for:

   * `DataSourceExec: file_groups={16 groups: ...}` (reads in parallel / partition count signal) ([Apache DataFusion][6])
   * `RepartitionExec: partitioning=Hash(..., 16)` or `RoundRobinBatch(16)` ([Apache DataFusion][1])
3. Programmatically assert: `df.execution_plan().partition_count == expected` ([Apache DataFusion][3])

## B) Prove pushdowns are working (scan less, decode less)

Use `EXPLAIN ANALYZE` and inspect scan metrics like:

* `bytes_scanned` vs file size and scanning CPU time; the “Reading Explain Plans” guide shows how to interpret these metrics and calls out projection pushdown as a reason `bytes_scanned` can be much lower than the file size. ([Apache DataFusion][6])

## C) Prove spilling + memory/disk caps are honored

1. Set a memory limit: `SET datafusion.runtime.memory_limit = '2G'` (runtime configs are settable via SQL). ([Apache DataFusion][5])
2. Ensure spilling is possible (disk manager / temp dir configured) and run a memory-heavy sort/group-by. Under tight memory limits, DataFusion will spill intermediate results to disk. ([Apache DataFusion][5])
3. Run `EXPLAIN ANALYZE` and look for spill metrics (`spilled_bytes`, `spill_file_count`, `spilled_rows`) on spillable operators. ([Docs.rs][10])
4. If you also want to cap disk usage, `max_temp_directory_size` is enforced (spilling can error if it would exceed that limit). ([Docs.rs][12])
5. Optional: spill compression is configurable (`datafusion.execution.spill_compression`) as of 49.0.0. ([Apache DataFusion][13])

---

## A “golden diagnostic bundle” template (Python)

```python
from __future__ import annotations
from datafusion import SessionContext

def explain_pack(ctx: SessionContext, sql: str, *, analyze_level: str = "summary") -> dict[str, str]:
    # Ensure explain-analyze output is readable
    ctx.sql(f"SET datafusion.explain.analyze_level = '{analyze_level}'").collect()

    df = ctx.sql(sql)

    physical = df.execution_plan()
    logical = df.logical_plan()
    optimized = df.optimized_logical_plan()

    out: dict[str, str] = {}
    out["logical_indent"] = logical.display_indent()
    out["optimized_logical_indent"] = optimized.display_indent()
    out["physical_indent"] = physical.display_indent()
    out["physical_partition_count"] = str(physical.partition_count)

    # Capturable, tabular plan artifacts
    out["explain_tree"] = ctx.sql(f"EXPLAIN FORMAT TREE {sql}").to_arrow_table().to_string()
    out["explain_indent"] = ctx.sql(f"EXPLAIN FORMAT INDENT {sql}").to_arrow_table().to_string()
    out["explain_analyze"] = ctx.sql(f"EXPLAIN ANALYZE {sql}").to_arrow_table().to_string()

    return out
```

Everything above is grounded in the official `EXPLAIN` surface and the Python plan APIs (`logical_plan`, `optimized_logical_plan`, `execution_plan`, `partition_count`, `display_indent`). ([Apache DataFusion][4])

---


[1]: https://datafusion.apache.org/user-guide/sql/explain.html "EXPLAIN — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/_sources/user-guide/sql/explain.md.txt "datafusion.apache.org"
[5]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[6]: https://datafusion.apache.org/user-guide/explain-usage.html "Reading Explain Plans — Apache DataFusion  documentation"
[7]: https://datafusion.apache.org/library-user-guide/query-optimizer.html "Query Optimizer — Apache DataFusion  documentation"
[8]: https://datafusion.apache.org/user-guide/metrics.html "Metrics — Apache DataFusion  documentation"
[9]: https://datafusion.apache.org/blog/2025/11/25/datafusion-51.0.0/ "Apache DataFusion 51.0.0 Released - Apache DataFusion Blog"
[10]: https://docs.rs/datafusion/latest/datafusion/physical_plan/metrics/struct.SpillMetrics.html "SpillMetrics in datafusion::physical_plan::metrics - Rust"
[11]: https://docs.rs/datafusion/latest/datafusion/physical_plan/sorts/sort/struct.SortExec.html "SortExec in datafusion::physical_plan::sorts::sort - Rust"
[12]: https://docs.rs/datafusion/latest/datafusion/physical_plan/spill/struct.SpillManager.html "SpillManager in datafusion::physical_plan::spill - Rust"
[13]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"

## 11) Interchange formats & cross-engine integration

The two “interchange” levers in **DataFusion in Python (`datafusion`)** are:

1. **Substrait** = *structured, portable plan IR (mostly logical plans)*
2. **SQL unparsing** = *turn a DataFusion logical plan back into SQL text (with a chosen dialect)*

Used well, these become your **stable artifacts** for caching + regression tests + “same intent, different engine” workflows.

---

# 11.1 Substrait: what it is, what it carries, and why it’s useful

### What Substrait is (and what it’s for)

Substrait is a format for describing compute operations on structured data, designed for interoperability across languages and systems. ([Substrait][1])

It’s specifically designed to communicate **plans (mostly logical plans)**—types/schemas, expressions, extensions, and relational operators—rather than SQL strings. ([Substrait][2])

### Serialization: protobuf “plan bytes” are the main currency

Substrait’s primary interchange form is a **protobuf-based binary representation**. ([Substrait][3])
That means the *most portable* artifact you can store or transmit is typically “Substrait plan bytes.”

Subtle but important versioning detail: the `Plan.version` field is optional up to Substrait 0.17.0 but **required in later versions**, so plan versioning becomes part of your long-term compatibility story. ([Substrait][3])

### Extensions: the portability hinge (functions/types/operators)

Substrait expects real-world interoperability to require extensions. It defines a standard YAML mechanism (“Simple Extensions”) for extended:

* data types, type variations
* scalar / aggregate / window functions
* table functions ([Substrait][4])

Plans reference extensions via URNs + declarations + anchors so that consumers can interpret functions/types consistently. ([Substrait][3])

**Practical implication:** Substrait portability is only as good as:

* the overlap of operators the producer emits and the consumer understands, and
* the extension definitions both sides agree on.

### Why it’s powerful in an Arrow-native stack

Substrait is supported broadly across the Arrow ecosystem and other engines. The Substrait community page explicitly calls out:

* **DataFusion** provides producer+consumer and can be used via Python bindings,
* **DuckDB** has a Substrait Community Extension (SQL/Python/R),
* **Acero**, **ADBC**, and **Arrow Flight SQL** can accept Substrait plans, etc. ([Substrait][5])

That’s the “plan IR” path to cross-engine execution without SQL-dialect gymnastics.

---

# 11.2 DataFusion Python Substrait API (`datafusion.substrait`)

DataFusion exposes Substrait support via `datafusion.substrait`:

* **Producer**: DataFusion `LogicalPlan` → Substrait `Plan`
* **Consumer**: Substrait `Plan` → DataFusion `LogicalPlan`
* **Serde**: convenience helpers to serialize SQL → Substrait (bytes/file/Plan) and deserialize from bytes/file
* **Plan**: wrapper that can `encode()` to bytes ([Apache DataFusion][6])

## 11.2.1 “Capture plan bytes” (SQL → Substrait bytes)

Use `Serde.serialize_bytes(sql, ctx)` to get a stable binary artifact:

```python
from datafusion import SessionContext
from datafusion.substrait import Serde

ctx = SessionContext()

sql = "SELECT 1 AS x"
plan_bytes = Serde.serialize_bytes(sql, ctx)

# store plan_bytes (e.g., in a golden snapshot, cache, artifact store)
```

`Serde.serialize_bytes` is explicitly “SQL query → Substrait plan as bytes.” ([Apache DataFusion][6])

You can also write to a file directly (`Serde.serialize(sql, ctx, path)`) and later read it back (`Serde.deserialize(path)`). ([Apache DataFusion][6])

## 11.2.2 “Replay plan bytes” (Substrait bytes → DataFusion execution)

To execute a stored plan:

1. bytes → `Plan`
2. `Plan` → DataFusion `LogicalPlan`
3. `LogicalPlan` → DataFrame (then execute)

```python
from datafusion import SessionContext
from datafusion.substrait import Serde, Consumer

ctx = SessionContext()

plan = Serde.deserialize_bytes(plan_bytes)               # bytes -> Plan
logical = Consumer.from_substrait_plan(ctx, plan)        # Plan -> LogicalPlan

df = ctx.create_dataframe_from_logical_plan(logical)     # LogicalPlan -> DataFrame
batches = df.collect()
```

* `Serde.deserialize_bytes` and `Consumer.from_substrait_plan` are part of the public API. ([Apache DataFusion][6])
* `SessionContext.create_dataframe_from_logical_plan(plan)` is the key bridge that makes Substrait “runnable” in Python. ([Apache Arrow][7])

## 11.2.3 Plan → bytes (explicit, if you prefer Plan objects)

`Plan.encode()` returns the encoded bytes. ([Apache DataFusion][6])

```python
from datafusion.substrait import Serde

plan_obj = Serde.serialize_to_plan(sql, ctx)
plan_bytes = plan_obj.encode()
```

---

# 11.3 Cross-engine workflows you can actually deploy

## A) “Single frontend, multiple engines” via Substrait

A typical architecture:

* **Frontend**: build query intent (SQL builder, DataFrame builder, semantic layer, etc.)
* **IR**: compile to Substrait bytes
* **Backends**: execute in DataFusion, DuckDB (Substrait extension), Acero, Flight SQL server, etc. ([Substrait][5])

This is the cleanest way to avoid SQL dialect drift.

## B) “Portable golden plans” for regression tests

For deterministic testing, Substrait bytes are often a better artifact than raw SQL because they:

* are explicit about operator tree structure and types (mostly logical plan semantics), ([Substrait][2])
* carry extension declarations/anchors (so you can detect missing function/type compatibility), ([Substrait][3])
* can be replayed into DataFusion using the Consumer + `create_dataframe_from_logical_plan`. ([Apache DataFusion][6])

For human review, pair bytes with a **plan pretty-printer** (e.g., `substrait-explain`) to get “EXPLAIN-like text” from protobuf plans. ([GitHub][8])

## C) Constructing Substrait directly in Python (when you want engine-agnostic plan authoring)

There’s also a `substrait-python` project explicitly aimed at constructing/manipulating Substrait plans in Python for evaluation by consumers like DataFusion or DuckDB. ([GitHub][9])

---

# 11.4 SQL unparsing in DataFusion Python (`datafusion.unparser`)

Substrait is the structured IR path. **Unparsing** is the “make SQL text again” path.

## 11.4.1 The Python API (dialects + pretty printing)

The Python module provides:

* `Dialect.default() / duckdb() / mysql() / postgres() / sqlite()`
* `Unparser(dialect).plan_to_sql(logical_plan) -> str`
* `Unparser.with_pretty(True/False)` ([Apache DataFusion][10])

Example:

```python
from datafusion import SessionContext
from datafusion.unparser import Dialect, Unparser

ctx = SessionContext()

df = ctx.sql("SELECT a, b FROM t WHERE a > 10")
plan = df.optimized_logical_plan()

sql_pg = Unparser(Dialect.postgres()).with_pretty(True).plan_to_sql(plan)
```

## 11.4.2 The contract you should document: “unparsing can fail”

At the engine level, `plan_to_sql` **returns an error if the plan cannot be converted to SQL**. ([Docs.rs][11])

So treat unparsing as:

* extremely useful for pushdown / debug / canonicalization,
* but **not guaranteed** for every plan shape.

## 11.4.3 Dialect correctness is real work (and it’s evolving)

Dialects can override function unparsing and various syntax capabilities (e.g., type casting, alias rules, support for certain clauses). ([Docs.rs][12])

And there are real-world edge cases/bugs where the unparser produces invalid SQL for a target dialect (e.g., reports about `CROSS JOIN` filters generating invalid SQL, or dialects emitting `LEFT ANTI/SEMI JOIN` syntax not supported by Postgres). ([GitHub][13])

**Practical recommendation:** if you’re using the unparser for production pushdown, add golden tests for:

* “unparse optimized plan” vs “unparse unoptimized plan”
* per-dialect validity checks (at least parse/prepare the emitted SQL in the target engine)

---

# 11.5 A “stable artifacts” strategy (Substrait + Unparser together)

If your goal is *move plans between systems and regression-test over time*, a robust pattern is:

## Canonical artifact: Substrait bytes

* Generate with `Serde.serialize_bytes(sql, ctx)` or `Producer.to_substrait_plan(logical_plan, ctx).encode()`. ([Apache DataFusion][6])
* Store as base64/hex in snapshots + keep the raw `.substrait` bytes as build artifacts.

## Readability artifact: explain-like text

* Render stored Substrait with a pretty-printer like `substrait-explain` for PR review diffs. ([GitHub][8])

## Pushdown artifact: dialect SQL from unparser

* Only for the specific backends you actually push to (Postgres/MySQL/etc.), using `datafusion.unparser`. ([Apache DataFusion][10])
* Validate emitted SQL and keep a small golden set—because `plan_to_sql` can fail or regress. ([Docs.rs][11])

---


[1]: https://substrait.io/ "Home - Substrait: Cross-Language Serialization for Relational Algebra"
[2]: https://substrait.io/tutorial/sql_to_substrait/ "SQL to Substrait tutorial - Substrait: Cross-Language Serialization for Relational Algebra"
[3]: https://substrait.io/serialization/binary_serialization/ "Binary Serialization - Substrait: Cross-Language Serialization for Relational Algebra"
[4]: https://substrait.io/extensions/ "Extensions - Substrait: Cross-Language Serialization for Relational Algebra"
[5]: https://substrait.io/community/powered_by/ "Powered by Substrait - Substrait: Cross-Language Serialization for Relational Algebra"
[6]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html "datafusion.substrait — Apache Arrow DataFusion  documentation"
[7]: https://arrow.staged.apache.org/datafusion-python/generated/datafusion.SessionContext.html "datafusion.SessionContext — Apache Arrow DataFusion  documentation"
[8]: https://github.com/DataDog/substrait-explain?utm_source=chatgpt.com "DataDog/substrait-explain"
[9]: https://github.com/substrait-io/substrait-python/blob/main/README.md?utm_source=chatgpt.com "substrait-python/README.md at main"
[10]: https://datafusion.apache.org/python/autoapi/datafusion/unparser/index.html "datafusion.unparser — Apache Arrow DataFusion  documentation"
[11]: https://docs.rs/datafusion/latest/datafusion/sql/unparser/fn.plan_to_sql.html "plan_to_sql in datafusion::sql::unparser - Rust"
[12]: https://docs.rs/datafusion/latest/datafusion/sql/unparser/dialect/struct.DuckDBDialect.html "DuckDBDialect in datafusion::sql::unparser::dialect - Rust"
[13]: https://github.com/apache/datafusion/issues/17359?utm_source=chatgpt.com "Unparsing of CROSS JOINs with filters is generating ..."

## 12) Advanced extensibility for “system builder” deployments (DataFusion + `datafusion` Python)

At the “build your own system” level, DataFusion is explicitly designed to be **customizable via traits**—data sources, functions, operators, even query languages—while staying Arrow-native end-to-end. ([Apache DataFusion][1])

This deep dive focuses on the three most “architectural” extension seams:

1. **Custom Table Providers** (new data sources / sinks)
2. **Custom Catalogs & Schemas** (name resolution + metastore integration)
3. **Remote/offloaded execution** using **Substrait** (optionally carried over Flight SQL / ADBC where supported)

---

# 12.1 Custom Table Providers (the *primary* extensibility seam)

### What a TableProvider is (in DataFusion terms)

A **TableProvider** is the abstraction that lets the planner treat “something” as a table. The important bit is that **a TableProvider must produce an `ExecutionPlan` for scanning**, and optionally support `INSERT` via `insert_into`. ([Apache DataFusion][2])

### The three “pushdown hooks” you should implement (or intentionally decline)

When DataFusion plans a query, it can pass **projection**, **filters**, and **limit** down into your scan. The `scan` method receives those parameters, and the docs are explicit that these are the hooks for:

* **Projection pushdown** (scan only needed columns) ([Docs.rs][3])
* **Filter pushdown** (evaluate predicates during scan if your source can do it) ([Docs.rs][3])
* **Limit pushdown** (stop early if your source can do it safely) ([Docs.rs][3])

Filter pushdown is gated by `supports_filters_pushdown`: by default it’s unsupported; you must override it, and you must declare each filter as **Exact / Inexact / Unsupported**. ([Docs.rs][3])
Also: **limit pushdown is blocked when any pushed-down filter is Inexact**, because the scan can’t guarantee how many rows survive. ([Docs.rs][3])

### The “engine-grade” expectation for your scan output

Your `ExecutionPlan` is responsible for scanning partitions “in a streaming, parallelized fashion.” ([Docs.rs][3])
So for best-in-class providers you generally want:

* true partitioning (so DataFusion can parallelize),
* streaming `RecordBatch` production (avoid materializing whole results),
* and meaningful pushdown (projection + at least “Exact” predicate pushdown where possible).

---

## 12.1.1 Exposing a custom TableProvider to **DataFusion Python** (PyCapsule + stable FFI)

### The modern, recommended approach: `FFI_TableProvider` via PyCapsule

DataFusion Python’s docs explicitly say:

* implement `TableProvider` in **Rust**
* expose it in Python by returning an `FFI_TableProvider` wrapped in a **PyCapsule**
* requires **DataFusion 43.0.0+** ([Apache DataFusion][4])

The `datafusion` Python library expects the capsule to be accessible by a method named:

* `__datafusion_table_provider__` ([Apache DataFusion][5])

The docs show the canonical pattern (Rust + PyO3) and then registering in Python:

```python
from datafusion import SessionContext

ctx = SessionContext()
provider = MyTableProvider()  # Python object that exposes __datafusion_table_provider__()
ctx.register_table("capsule_table", provider)
ctx.table("capsule_table").show()
```

([Apache DataFusion][4])

### Why this matters (version decoupling)

The DataFusion Python 43.1.0 release notes explain the motivation: the FFI for table providers creates a stable way to share providers across libraries, similar in spirit to Arrow’s stable C interfaces, allowing libraries (e.g., Delta Lake, datafusion-contrib) to expose table providers **without depending directly on `datafusion-python`**, and to keep working regardless of which `datafusion` version they were built against. ([Apache DataFusion][6])

---

## 12.1.2 The underlying FFI layer (`datafusion-ffi`) and what it implies for system builders

The `datafusion-ffi` crate is the “real” boundary: it provides `FFI_*` structs on the producer side and `Foreign*` wrappers on the consumer side (e.g., `FFI_TableProvider` and `ForeignTableProvider`). ([Docs.rs][7])

Key constraints to document:

* It’s designed primarily for **Rust ↔ Rust** interoperability across library boundaries, using crates like `abi_stable` and `async-ffi`. ([Docs.rs][8])
* If your underlying implementation is in another language, the docs recommend creating a **Rust wrapper crate** around it and then connecting to DataFusion via `datafusion-ffi`. ([Docs.rs][8])
* The crate includes explicit memory management patterns (e.g., many FFI structs need a “release” to free producer-owned private data safely). ([Docs.rs][8])

### “Newer” FFI requirement that impacts real plugins: TaskContext + ExtensionCodec

In DataFusion’s upgrade guides: when instantiating FFI structs such as `FFI_TableProvider` (and also FFI Catalog providers and table functions), you now must provide:

* a **TaskContextProvider** (commonly `SessionContext`)
* and optionally a **LogicalExtensionCodec** (for encoding/decoding logical extensions) ([Apache DataFusion][9])

This becomes important as soon as your provider involves:

* custom functions,
* custom logical nodes,
* or other extension-encoded behavior.

---

# 12.2 Custom Catalogs & Schemas (how “names” map to providers)

### What a catalog does

Catalogs/schemas are the **name resolution layer**. DataFusion uses the `catalog.schema.table` hierarchy, and DataFusion Python ships with an in-memory default catalog + schema (`datafusion` + `default`). ([Apache DataFusion][10])

### In-memory catalogs are good enough… until they aren’t

DataFusion Python supports creating additional in-memory catalogs/schemas:

```python
from datafusion.catalog import Catalog, Schema

my_catalog = Catalog.memory_catalog()
my_schema = Schema.memory_schema()
my_catalog.register_schema("my_schema_name", my_schema)

ctx.register_catalog("my_catalog_name", my_catalog)
```

…and then you can query `my_catalog_name.my_schema_name.my_table`. ([Apache DataFusion][10])

### When you want a **custom** catalog

If you need your catalog to reflect:

* an external metastore,
* multi-tenant permissions,
* dynamic “tables appear/disappear,”
* or just “resolve table name → build provider on demand,”

…DataFusion Python supports catalogs written in **Rust or Python**:

* Rust catalogs must be exported to Python via PyO3; this typically offers major performance benefits. ([Apache DataFusion][10])
* Python catalogs can be implemented by inheriting from the `CatalogProvider` abstract base class. ([Apache DataFusion][10])
* There’s an important behavioral nuance: Python-defined catalogs are wrapped for Rust-side access, but the Python API returns the original Python object (not a wrapper), which matters if you store mutable state. ([Apache DataFusion][10])

If you want real-world patterns, the Rust docs for `CatalogProvider` point to example implementations like:

* `datafusion-cli`’s DynamicFileCatalogProvider (treats files/dirs as tables),
* a simple directory-based catalog,
* `delta-rs`’s UnityCatalogProvider. ([Docs.rs][11])

---

# 12.3 Offloading execution: pushing Substrait fragments to remote backends

This is the “hybrid engine” move: use DataFusion to plan (or partially plan), then execute some work remotely.

## 12.3.1 Substrait as the interchange IR

Substrait is a cross-system plan format for structured compute operations, explicitly meant for interoperability. ([Substrait][12])
It calls out use cases like:

* serializing a plan for consistent reuse across systems (e.g., views),
* and **submitting a plan to different execution engines** for consistent semantics. ([Substrait][12])

DataFusion is listed as providing a Substrait producer+consumer and being usable via the Python bindings. ([Substrait][13])

## 12.3.2 Remote “carriers”: Flight SQL and ADBC (reality check included)

**Arrow Flight SQL** is a client/server protocol for interacting with SQL databases using Arrow and Flight RPC. ([Apache Arrow][14])
The Substrait community explicitly notes that Flight SQL and ADBC are intended to allow queries as **SQL strings or Substrait plans**. ([Substrait][13])

However, “intended” and “implemented” differ:

* The ADBC driver status matrix explicitly tracks “Substrait support,” and (at least in the listed drivers) Flight SQL drivers show **Substrait = N** while supporting SQL. ([Apache Arrow][15])

So in practice, your “remote offload” design usually needs:

* **Substrait path** (if your chosen backend actually accepts it), plus
* **SQL fallback path** (because many do not).

## 12.3.3 The most practical pattern: *RemoteScan TableProvider*

If you want DataFusion to orchestrate a multi-source query, the cleanest way is:

* implement a TableProvider whose `scan(projection, filters, limit)` **translates those pushed-down constraints** into a remote request
* ship either SQL or Substrait to the remote backend
* stream Arrow `RecordBatch` results back as the scan’s `ExecutionPlan`

This pattern aligns perfectly with what DataFusion already expects from `TableProvider.scan`:

* it receives projection/filters/limit for pushdown ([Docs.rs][3])
* it can declare which filters it can apply (Exact/Inexact/Unsupported) via `supports_filters_pushdown` ([Docs.rs][3])
* and it must produce a streaming, partitioned execution plan ([Docs.rs][3])

In other words: you don’t “bolt on” remote execution—you implement it as a first-class data source.

## 12.3.4 Fragment pushdown (advanced): “replace a subtree with RemoteExec”

If you want to offload *part of a query plan* (not just a base scan), you generally end up building:

* a custom logical/physical optimization pass that identifies a subplan,
* rewrites it into a custom operator (“RemoteExec”),
* and that operator ships the subplan (often as Substrait) to the remote engine.

DataFusion’s own docs emphasize it is “customizable at almost all points… including custom operators.” ([Apache DataFusion][1])
But the implementation complexity is significantly higher than the “RemoteScan TableProvider” route, and you’ll immediately care about Substrait extension codecs (see the FFI section above). ([Apache DataFusion][9])

---

# 12.4 Deployment considerations (what to document so this stays maintainable)

### For custom table providers

* **Pushdown correctness**: be explicit about which filters are Exact vs Inexact; remember Inexact blocks limit pushdown. ([Docs.rs][3])
* **Partitioning story**: if you can’t partition, don’t expect great parallelism.
* **Versioning**: if you distribute the provider as a separate Python package, use the FFI/PyCapsule route specifically to avoid tight coupling with `datafusion-python`. ([Apache DataFusion][6])

### For custom catalogs

* Prototype in Python, then move to Rust if catalog lookup becomes hot (DataFusion docs explicitly call out Rust catalogs as faster). ([Apache DataFusion][10])
* Be mindful of the “Python catalog object vs Rust wrapper” distinction if you keep mutable state in Python. ([Apache DataFusion][10])

### For Substrait/remote offload

* Treat Substrait as a **capability negotiation problem** (operators + extensions + versions), not a magic bullet. ([Substrait][12])
* Confirm backend support: e.g., ADBC’s own status tables show Substrait support varies. ([Apache Arrow][15])

---

If you want, the next deep dive that pairs really well with this is **“a system-builder reference architecture”**: a concrete plugin layout (Catalog → TableProvider registry → Substrait/SQL remote adapters) plus a small contract harness that asserts pushdown + partition counts + spill behavior using `EXPLAIN ANALYZE`.

[1]: https://datafusion.apache.org/ "Apache DataFusion — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html?utm_source=chatgpt.com "Custom Table Provider — Apache DataFusion documentation"
[3]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html "TableProvider in datafusion::datasource - Rust"
[4]: https://datafusion.apache.org/python/user-guide/io/table_provider.html "Custom Table Provider — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/python/contributor-guide/ffi.html "Python Extensions — Apache Arrow DataFusion  documentation"
[6]: https://datafusion.apache.org/blog/2024/12/14/datafusion-python-43.1.0/ "Apache DataFusion Python 43.1.0 Released - Apache DataFusion Blog"
[7]: https://docs.rs/datafusion-ffi/latest/datafusion_ffi/table_provider/index.html "datafusion_ffi::table_provider - Rust"
[8]: https://docs.rs/crate/datafusion-ffi/latest "datafusion-ffi 52.0.0 - Docs.rs"
[9]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[10]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[11]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html "CatalogProvider in datafusion::catalog - Rust"
[12]: https://substrait.io/ "Home - Substrait: Cross-Language Serialization for Relational Algebra"
[13]: https://substrait.io/community/powered_by/ "Powered by Substrait - Substrait: Cross-Language Serialization for Relational Algebra"
[14]: https://arrow.apache.org/docs/format/FlightSql.html "Arrow Flight SQL — Apache Arrow v22.0.0"
[15]: https://arrow.apache.org/adbc/main/driver/status.html "Driver Implementation Status - ADBC 22 (dev) documentation"


Below is a **system-builder reference architecture** that treats **`datafusion` (Python)** as an **Arrow-native query runtime** with a **plugin surface** for:

* **Catalogs** (name → metadata),
* **TableProviders** (metadata → scan/insert execution),
* **Remote adapters** (Substrait/SQL pushdown/offload),

…and a **contract harness** that proves: **pushdown, partitioning, spilling** via **`EXPLAIN` / `EXPLAIN ANALYZE`**.

---

# 12.x Reference architecture: Catalog → TableProvider registry → Remote adapters

## The “engine-native” execution model you’re building around

* A **TableProvider** supplies scans by returning an **ExecutionPlan**, whose `execute` method yields a **stream of Arrow `RecordBatch`es**. ([Apache DataFusion][1])
* In **Python**, DataFusion expects custom providers to be exposed via a **PyCapsule** from a `__datafusion_table_provider__` method (FFI `FFI_TableProvider`), so they can be registered into a `SessionContext`. ([Apache DataFusion][2])

This strongly suggests a layered system:

```
┌───────────────────────────────────────────────────────────┐
│ Query Frontend (SQL strings / builder / semantic layer)   │
└───────────────┬───────────────────────────────────────────┘
                │
┌───────────────▼───────────────────────────────────────────┐
│ Session Factory (SessionContext + config + runtime + policy│
│ + register catalogs/object stores + safety SQLOptions)     │
└───────────────┬───────────────────────────────────────────┘
                │
┌───────────────▼───────────────────────────────────────────┐
│ Catalog Layer (CatalogProvider / SchemaProvider)           │
│   name → TableSpec(metadata snapshot)                      │
└───────────────┬───────────────────────────────────────────┘
                │
┌───────────────▼───────────────────────────────────────────┐
│ TableProvider Registry (TableSpec → TableProviderExportable│
│   built-in (listing/parquet) | pyarrow.dataset | FFI)      │
└───────────────┬───────────────────────────────────────────┘
                │
┌───────────────▼───────────────────────────────────────────┐
│ Remote Adapter Layer (optional)                            │
│   Substrait bytes OR unparsed SQL dialect                  │
└───────────────────────────────────────────────────────────┘
```

---

# 12.x.1 Concrete plugin layout (Python-first, Rust where it counts)

A minimal-but-real “system builder” repo layout:

```
my_engine/
  df/
    session.py            # SessionContext factory + config/runtime defaults
    policy.py             # SQLOptions presets: read-only / service / admin
    explain.py            # helpers: explain_indent/tree/analyze + parsing
  catalogs/
    base.py               # CatalogPlugin protocol + TableSpec model
    memory_catalog.py     # simple in-memory impl (dev)
    remote_snapshot.py    # refresh loop for remote metadata → in-memory snapshot
  providers/
    registry.py           # TableProviderRegistry (TableSpec → provider instance)
    builtin.py            # wrappers around register_parquet/register_listing_table
    ffi_wrappers.py       # conventions for __datafusion_table_provider__
    remote_scan/          # “RemoteScanProvider” (Rust extension + Python shim)
      py/__init__.py
      rust/               # pyo3 crate that exports FFI_TableProvider
  remote/
    base.py               # RemoteAdapter protocol
    substrait_adapter.py  # plan→substrait bytes
    sql_adapter.py        # plan→sql via unparser + dialect
    flight_sql.py         # optional carrier client (if backend supports)
  contracts/
    test_pushdown.py
    test_partitions.py
    test_spill.py
```

The key architectural decision here is: **Catalogs are metadata**, **TableProviders are execution**, and **Remote adapters are compilation/transport**.

---

# 12.x.2 Catalog layer: how to do “metastore integration” without killing planning

### The constraint you must design around

DataFusion planning APIs are **not async**, so you can’t “lazy fetch” remote metadata during planning without exploding into many network calls. The DataFusion docs explicitly recommend that **remote catalogs provide an in-memory snapshot** and that systems **batch/parallelize remote lookups**, then plan against the cached snapshot. ([Docs.rs][3])

### Practical pattern

1. **Background refresh** (your code) populates a local cache:

   * namespaces, table names, schemas, statistics pointers, storage location, etc.
2. Your **CatalogProvider/SchemaProvider** serves that snapshot synchronously.

In DataFusion Python you can implement custom catalogs either:

* in **Rust** (PyO3) for performance, or
* in **Python** by inheriting from `CatalogProvider` (and `SchemaProvider`) and backing it with a dictionary-like snapshot. ([Apache DataFusion][4])

**Design tip:** store “table metadata” as a `TableSpec` (below), not as a provider instance. Providers can be expensive; metadata should be cheap.

---

# 12.x.3 TableProvider registry: one place to turn TableSpec → runnable table

### Why a registry?

You want exactly one place where you decide:

* is this table **built-in file scan**?
* a **PyArrow Dataset**?
* a **custom FFI TableProvider**?
* a **remote scan** wrapper?

This mirrors DataFusion itself: catalogs are separate from providers. ([Apache DataFusion][5])

### A concrete `TableSpec`

```python
from dataclasses import dataclass
from typing import Literal, Mapping, Optional

@dataclass(frozen=True)
class TableSpec:
    name: str
    schema: str
    catalog: str
    kind: Literal["parquet_listing", "arrow_dataset", "ffi", "remote_scan"]
    uri: str | None = None
    options: Mapping[str, str] | None = None
    # capabilities used for contract tests / safe pushdown expectations
    supports_projection_pushdown: bool = True
    supports_predicate_pushdown: bool = True
    supports_limit_pushdown: bool = False
```

### Registry interface

```python
class TableProviderRegistry:
    def materialize(self, ctx, spec: TableSpec):
        """
        Return an object acceptable to ctx.register_table(name, obj),
        i.e. a pyarrow.dataset.Dataset, a DataFusion Table, or an object
        with __datafusion_table_provider__ returning a PyCapsule.
        """
        ...
```

DataFusion Python’s **custom table provider** story is explicitly: implement `TableProvider` in Rust and expose `FFI_TableProvider` through a `__datafusion_table_provider__` PyCapsule. ([Apache DataFusion][2])

---

# 12.x.4 Remote adapters: Substrait-first, SQL fallback (dialect-aware)

You have two “compilation products” DataFusion can generate for offload:

## A) Substrait bytes (preferred for plan portability)

DataFusion Python exposes `datafusion.substrait` with:

* `Serde.serialize_bytes(sql, ctx)` / `Serde.deserialize_bytes(...)`
* `Producer` / `Consumer`
* `Plan.encode() -> bytes` ([Apache DataFusion][6])

This is the clean way to produce a **stable artifact** that can be cached and replayed (and, if your backend supports it, executed remotely).

## B) SQL via Unparser (fallback, but very practical)

DataFusion Python exposes `datafusion.unparser` to convert a `LogicalPlan` back into SQL for a chosen dialect. ([Apache DataFusion][7])
Important: `plan_to_sql` **can error** if a plan cannot be converted to SQL. ([Docs.rs][8])

So your adapter should look like:

```python
class RemoteAdapter:
    def execute_substrait(self, plan_bytes: bytes): ...
    def execute_sql(self, sql: str): ...
```

And your **policy** should be:

1. Try Substrait (if backend advertises support)
2. Else try unparsed SQL for that backend dialect
3. Else fall back to “source-specific” SQL generation (last resort)

---

# 12.x.5 The “RemoteScanProvider” pattern (the most maintainable offload)

Instead of trying to offload an arbitrary subplan, start with the most robust building block:

**RemoteScan TableProvider**:

* Implements `TableProvider::scan` in Rust
* Turns pushed-down `(projection, filters, limit)` into a remote query request
* Streams Arrow RecordBatches back as the scan’s ExecutionPlan output stream ([Apache DataFusion][1])

This aligns with how DataFusion scans work, and keeps the “system complexity” local to the provider plugin.

---

# 12.x.6 Contract harness: prove pushdown + partitions + spill with `EXPLAIN ANALYZE`

## Why EXPLAIN is your best contract boundary

* `EXPLAIN` shows logical+physical plans. ([Apache DataFusion][9])
* `EXPLAIN ANALYZE` runs the query and shows runtime metrics; `EXPLAIN ANALYZE VERBOSE` gives per-partition metrics. ([Apache DataFusion][10])
* Plans include scan-level details such as `projection=[...]` and `predicate=...` on `DataSourceExec`. ([Apache DataFusion][11])
* Runtime settings like `datafusion.runtime.memory_limit` / `temp_directory` can be set via SQL `SET`. ([Apache DataFusion][12])

### Harness helpers (Python)

```python
import re
from datafusion import SessionContext

def explain_indent(ctx: SessionContext, sql: str, *, analyze=False, verbose=False) -> str:
    prefix = "EXPLAIN "
    if analyze:
        prefix += "ANALYZE "
    if verbose:
        prefix += "VERBOSE "
    prefix += "FORMAT INDENT "
    df = ctx.sql(prefix + sql)
    tbl = df.to_arrow_table()
    # 'plan' column contains multiline text; concatenate rows
    return "\n".join(tbl.column("plan").to_pylist())

def set_diagnostics_mode(ctx: SessionContext, *, analyze_level="dev"):
    # EXPLAIN docs describe analyze_level control; dev is more detailed. :contentReference[oaicite:14]{index=14}
    ctx.sql(f"SET datafusion.explain.analyze_level = '{analyze_level}'").collect()
```

---

## (A) Pushdown assertions (projection + predicate)

From the DataFusion docs, a pushed-down scan shows in the physical plan as:

`DataSourceExec: ... projection=[...], predicate=...` ([Apache DataFusion][11])

```python
def assert_scan_pushdown(plan: str, *, must_have_predicate=True, must_have_projection=True):
    assert "DataSourceExec:" in plan, "expected a DataSourceExec scan node"
    if must_have_projection:
        assert "projection=[" in plan, "expected projection pushdown into scan"
    if must_have_predicate:
        assert "predicate=" in plan, "expected predicate pushdown into scan"
```

If you’re testing a **custom remote provider**, you can enforce the same interface contract by asserting your provider emits comparable “scan options” in `EXPLAIN VERBOSE` (often the easiest stable surface).

---

## (B) Partition / parallelism assertions

The official explain example shows:

`file_groups={16 groups: ...}`
and that “16 partitions” correspond to core usage. ([Apache DataFusion][11])

```python
def extract_file_groups(plan: str) -> int | None:
    m = re.search(r"file_groups=\{(\d+)\s+groups:", plan)
    return int(m.group(1)) if m else None

def assert_min_partitions(plan: str, *, at_least: int):
    n = extract_file_groups(plan)
    assert n is not None, "expected file_groups=... groups in scan node"
    assert n >= at_least, f"expected at least {at_least} file groups/partitions, got {n}"
```

This gives you a **black-box** test that “we’re actually parallelizing scans”, without relying on internal APIs.

---

## (C) Spilling assertions (prove memory caps cause out-of-core behavior)

### What’s contractually true

* DataFusion supports setting `datafusion.runtime.memory_limit` via `SET`. ([Apache DataFusion][12])
* Under tight memory limits, DataFusion will **spill intermediate results to disk** for memory-consuming queries. ([Apache DataFusion][12])
* Operators like `SortExec` explicitly support spilling to disk. ([Docs.rs][13])

### Metric names you can key on

Common spill metric names used in DataFusion’s metric types include:

* `spill_count`, `spilled_bytes`, `spilled_rows` ([Docs.rs][14])

### Spill contract test

```python
def configure_spill(ctx: SessionContext, *, mem="256M", temp="/tmp/df-spill", max_temp="5G"):
    ctx.sql(f"SET datafusion.runtime.memory_limit = '{mem}'").collect()
    ctx.sql(f"SET datafusion.runtime.temp_directory = '{temp}'").collect()
    ctx.sql(f"SET datafusion.runtime.max_temp_directory_size = '{max_temp}'").collect()

def assert_spilled(plan_with_metrics: str):
    # be flexible: some builds emit spill_count, others spill_file_count; always check bytes/rows too
    assert ("spilled_bytes=" in plan_with_metrics or "spill_count=" in plan_with_metrics), \
        "expected spill metrics in EXPLAIN ANALYZE output"

def test_spill_sort(ctx: SessionContext):
    set_diagnostics_mode(ctx, analyze_level="dev")
    configure_spill(ctx)

    sql = """
      SELECT *
      FROM big_table
      ORDER BY some_high_cardinality_key
    """
    plan = explain_indent(ctx, sql, analyze=True, verbose=True)  # per-partition metrics too :contentReference[oaicite:21]{index=21}
    assert "SortExec" in plan, "expected a sort operator"
    assert_spilled(plan)
```

Notes:

* Use `EXPLAIN ANALYZE VERBOSE` when you care about skew (spills often happen on only some partitions). ([Apache DataFusion][10])
* Expect the **exact metric set to evolve** (DataFusion’s metrics doc is still expanding beyond baseline/filter metrics). ([Apache DataFusion][15])

---

# 12.x.7 “Definition of Done” checklist for your system-builder layer

**Catalog**

* ✅ Serves a synchronous snapshot (no network calls during planning) ([Docs.rs][3])
* ✅ Names are stable: `catalog.schema.table`

**Providers**

* ✅ All providers are registerable by `SessionContext.register_table`
* ✅ Custom providers expose `__datafusion_table_provider__` PyCapsule (`FFI_TableProvider`) ([Apache DataFusion][2])
* ✅ Pushdown expectations are testable via `EXPLAIN`

**Remote adapters**

* ✅ Prefer Substrait; fall back to SQL unparser; handle unparse failures cleanly ([Apache DataFusion][6])

**Contracts**

* ✅ Pushdown: `projection=[...]` + `predicate=...` appears at scan level when expected ([Apache DataFusion][11])
* ✅ Parallelism: scan shows multiple `file_groups`/partitions when configured ([Apache DataFusion][11])
* ✅ Spill: tight `memory_limit` + heavy operators show spill metrics in `EXPLAIN ANALYZE` ([Apache DataFusion][12])

---

If you want the next increment, I can turn this into **a literal starter repository skeleton** (actual files with minimal implementations of `CatalogProvider`/`SchemaProvider`, a registry, and pytest contract tests) that you can drop into your project and iterate on.

[1]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html "Custom Table Provider — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/python/user-guide/io/table_provider.html "Custom Table Provider — Apache Arrow DataFusion  documentation"
[3]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html "CatalogProvider in datafusion::catalog - Rust"
[4]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/library-user-guide/catalogs.html "Catalogs, Schemas, and Tables — Apache DataFusion  documentation"
[6]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html "datafusion.substrait — Apache Arrow DataFusion  documentation"
[7]: https://datafusion.apache.org/python/autoapi/datafusion/unparser/index.html?utm_source=chatgpt.com "datafusion.unparser"
[8]: https://docs.rs/datafusion/latest/datafusion/sql/unparser/fn.plan_to_sql.html?utm_source=chatgpt.com "plan_to_sql in datafusion::sql::unparser - Rust"
[9]: https://datafusion.apache.org/user-guide/sql/explain.html "EXPLAIN — Apache DataFusion  documentation"
[10]: https://datafusion.apache.org/user-guide/sql/explain.html?utm_source=chatgpt.com "EXPLAIN — Apache DataFusion documentation"
[11]: https://datafusion.apache.org/user-guide/explain-usage.html "Reading Explain Plans — Apache DataFusion  documentation"
[12]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[13]: https://docs.rs/datafusion/latest/datafusion/physical_plan/sorts/sort/struct.SortExec.html?utm_source=chatgpt.com "SortExec in datafusion::physical_plan::sorts::sort - Rust"
[14]: https://docs.rs/deltalake/latest/deltalake/datafusion/physical_plan/metrics/enum.MetricValue.html?utm_source=chatgpt.com "MetricValue in deltalake::datafusion::physical_plan::metrics"
[15]: https://datafusion.apache.org/user-guide/metrics.html "Metrics — Apache DataFusion  documentation"
