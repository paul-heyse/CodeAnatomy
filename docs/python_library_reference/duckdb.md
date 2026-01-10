Below is an opinionated, refactorer‑friendly guide to **DuckDB’s Python client**—focused on what an AI programming agent needs to replace custom data‑plumbing code with first‑class DuckDB features.

---

## 0) What DuckDB (Python) is—and when to use it

DuckDB is a fast, embedded OLAP database you link into your process (no server to run). The **Python package `duckdb`** exposes both a DB‑API 2.0 interface and a higher‑level **Relational API** with lazy `Relation` objects you can turn into Pandas, Polars, Arrow, or NumPy in one line. Typical wins vs hand‑rolled code: zero‑copy interchange, direct SQL over CSV/Parquet/JSON (local, HTTP/S3), and robust SQL for joins/aggregations. ([DuckDB][1])

> **Version note (Oct 20, 2025):** The docs list **Python client 1.4.1** as latest stable and require Python **3.9+**. If you pin APIs, check the version selector at the top of the docs. ([DuckDB][1])

---

## 1) Installation & getting a connection

```bash
pip install duckdb
# or: conda install python-duckdb -c conda-forge
```

Two connection styles:

* **Module-level default in‑memory connection**: `duckdb.sql("SELECT 42")…`. Great for REPL/notebooks, but it’s a *global shared DB in the module*. Libraries should **prefer explicit connections** to avoid cross‑package interference. ([DuckDB][1])
* **Explicit connection**: `duckdb.connect(...)`.

  * `duckdb.connect()` → unnamed **in‑memory** DB.
  * `duckdb.connect("file.db")` → **persistent** file DB.
  * Special names: `":default:"` targets the global default connection; `":memory:conn3"` creates a **named** in‑memory DB shared by name. Read‑only mode is available: `duckdb.connect("file.db", read_only=True)`. ([DuckDB][2])

**Threading:** The docs advise: **connections are not thread‑safe** from Python; create a **cursor per thread** (or separate connections). ([DuckDB][1])

---

## 2) Mental model: Relations, laziness, and conversion

`duckdb.sql("…")` returns a **Relation** (lazy—no work until you ask for results). You can chain SQL/relational operations, then request output in your target format:

```python
import duckdb, pandas as pd
rel = duckdb.sql("select * from range(10) t(i) where i % 2 = 0")
df = rel.df()          # pandas.DataFrame
pl_df = rel.pl()       # polars.DataFrame
tbl = rel.arrow()      # pyarrow.Table
rows = rel.fetchall()  # list[tuple]
```

All of the above execute the plan and fetch results. ([DuckDB][1])

The Relational API also exposes *builder* methods (`filter`, `project`, `join`, `aggregate`, …) and many creation helpers (`read_csv`, `read_parquet`, `read_json`, `from_df`, `from_arrow`, `sql`, `table`, etc.). Everything is **lazy** until you call an output method (`df()`, `show()`, `fetch*`, `write_parquet`, …). ([DuckDB][3])

---

## 3) Reading data (local, HTTP, S3, “just a file path”)

### Local files

* **CSV/Parquet/JSON** into Relations (module or connection methods):

```python
duckdb.read_csv("data/*.csv")
duckdb.read_parquet(["a.parquet", "b.parquet"])
duckdb.read_json("nested.json")
```

You can also directly query by **filename** (function call optional if the extension is recognized):

```sql
SELECT * FROM 'data/events.parquet';
```

Lists and globs are supported. ([DuckDB][1])

### HTTP & S3

* DuckDB’s **`httpfs` extension** enables HTTP(S) and S3. It auto‑loads on first use; or do:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT * FROM 'https://example.com/file.parquet';
```

Use **DuckDB Secrets Manager** to store credentials and avoid hard‑coding keys. ([DuckDB][4])

### fsspec (Python‑only)

If you need GCS, WebHDFS, R2, lakeFS, etc., **register an `fsspec` filesystem** and keep using `read_csv`/`read_parquet`/`read_json`:

```python
import fsspec, duckdb
fs = fsspec.filesystem("gcs")  # requires gcsfs
duckdb.register_filesystem(fs)
duckdb.read_parquet("gcs://bucket/path/*.parquet").df()
```

This capability is specific to the Python client. ([DuckDB][5])

### Performance hint

If you repeatedly scan the same remote files, enable the **object cache** (caches things like Parquet metadata):

```sql
PRAGMA enable_object_cache;
```

Disable with `PRAGMA disable_object_cache;`. ([DuckDB][6])

---

## 4) Writing data (files, databases, and attaching)

* **Write files from any Relation**:

```python
duckdb.sql("select 42 as x").write_parquet("out.parquet")
duckdb.sql("select * from t").write_csv("out.csv")
```

Or via SQL `COPY`:

```sql
COPY (SELECT * FROM t) TO 'out.parquet' (FORMAT parquet);
```

CSV/Parquet/JSON export guides are in the docs. ([DuckDB][1])

* **Export/Import entire databases**:

```sql
EXPORT DATABASE 'dir' (FORMAT csv);     -- schema + data as files
IMPORT DATABASE 'dir';
```

Handy to move in‑memory DBs or snapshot persistent DBs. ([DuckDB][7])

* **Attach multiple DBs** and query across them:

```sql
ATTACH 'file1.duckdb' AS db1;
ATTACH ':memory:' AS tmpdb;  -- can't detach default, switch first if needed
USE tmpdb;
```

You can even **attach a remote DuckDB file** over HTTPS or S3 (read‑only since 1.1):

```sql
ATTACH 'https://blobs.duckdb.org/databases/stations.duckdb' AS stations_db;
SELECT count(*) FROM stations_db.stations;
```

([DuckDB][8])

---

## 5) Querying in‑memory Python objects (Pandas/Polars/Arrow)

### Easiest: reference variables by **name**

DuckDB can query DataFrame variables as if they were tables—no manual registration needed (“replacement scans”):

```python
import duckdb, pandas as pd
df = pd.DataFrame({"a":[1,2,3]})
duckdb.sql("SELECT a, a*2 AS b FROM df").df()
```

You can also explicitly **register** objects on a connection (`con.register('my_df', df)`). When name collisions happen, the resolution order is: **explicitly registered** > **native tables/views** > **replacement scans**. ([DuckDB][9])

### Round‑tripping results

* `df()` → Pandas
* `pl()` → Polars
* `arrow()` or `fetch_record_batch()` → Arrow Table/Reader
* `fetchnumpy()` → dict of NumPy arrays
* `fetch*()` → Python rows

All are first‑class on `Relation`. ([DuckDB][1])

---

## 6) DB‑API 2.0 for parameterized SQL (safe & fast)

```python
con = duckdb.connect("file.duckdb")
# Unnamed placeholders
con.execute("INSERT INTO items VALUES (?, ?, ?)", ["laptop", 2000, 1])
con.executemany("INSERT INTO items VALUES (?, ?, ?)", [["chainsaw", 500, 10], ["phone", 300, 2]])
# Positional $n or named $param style
con.execute("SELECT $1, $1, $2", ["duck", "goose"]).fetchall()
con.execute("SELECT $my_param", {"my_param": 5}).fetchall()
```

The docs support `?`, `$1`… and **named** `$my_param`. For *large* loads, prefer `COPY`/file‑based paths over `executemany`. ([DuckDB][2])

---

## 7) Python UDFs (scalar; vectorized via Arrow)

Register plain Python functions for use in SQL:

```python
import duckdb
from duckdb.typing import VARCHAR

def random_name() -> str:
    import faker
    return faker.Faker().name()

duckdb.create_function("random_name", random_name, [], VARCHAR)   # scalar UDF
duckdb.sql("SELECT random_name()").show()
```

Key options:

* **Type dispatch** can infer from Python annotations.
* `type="arrow"` runs vectorized over Arrow arrays (faster than `native` row‑by‑row).
* Control `null_handling`, `exception_handling`, and `side_effects`. Remove with `con.remove_function(name)`. ([DuckDB][10])

> Table/aggregate Python UDFs are not exposed in the same way; for custom table transforms, prefer Arrow/Pandas interop or SQL macros plus scalar UDFs.

---

## 8) Configuration & tuning essentials

* **Threads & memory**:

```sql
SET threads = 8;                 -- or via connect(config={'threads': 8})
SET memory_limit = '10GB';
```

* **Progress bar & progress polling**:

```sql
SET enable_progress_bar = true;
```

…and from Python you can poll `con.query_progress()` for an approximate percent. ([DuckDB][11])

* **Explain & profile**:

```sql
EXPLAIN SELECT ...;         -- physical plan
EXPLAIN ANALYZE SELECT ...; -- run & show timings
```

Use these to target joins, scans, and sort spills. ([DuckDB][12])

* **Concurrency (one process)**: DuckDB allows concurrent work so long as there’s no write conflict; appends never conflict, and separate tables/subsets can be updated in parallel (optimistic control on same rows). In Python, use a **cursor per thread**. For multi‑process read access to the same file, connect with `read_only=True`. ([DuckDB][13])

---

## 9) Extensions (HTTP/S3, Excel, Delta/Iceberg, Spatial,…)

Install once, then load as needed:

```python
con = duckdb.connect()
con.install_extension("httpfs"); con.load_extension("httpfs")
con.install_extension("excel");  con.load_extension("excel")
```

* **httpfs**: HTTP(S) & S3 table scans. ([DuckDB][4])
* **excel**: write/read `.xlsx`. ([DuckDB][14])
* **delta / iceberg**: read lakehouse formats (`INSTALL delta; LOAD delta;`, `INSTALL iceberg; LOAD iceberg;`). Check supported versions/platforms; Delta is a core extension; Iceberg docs cover catalog support. ([DuckDB][15])

**Secrets Manager** centralizes credentials (S3, Azure, etc.) via `CREATE SECRET`, with scope and optional persistence. ([DuckDB][16])

---

## 10) Patterns to replace common custom code

| If your code…                                           | Replace with…                                                                                                                                  |
| ------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Manually walks folders, concatenates Parquet/CSV        | `read_parquet('dir/*.parquet')` or list input; SQL across files. ([DuckDB][17])                                                                |
| Streams HTTP/S3 files via requests/boto and hand‑parses | `INSTALL/LOAD httpfs`; then `SELECT * FROM 'https://…/file.parquet'` or `'s3://…'`. Use Secrets Manager. ([DuckDB][4])                         |
| Copies Pandas → SQL via loops                           | `duckdb.sql("CREATE TABLE t AS SELECT * FROM df")` or `con.register('df', df)`. Return results with `.df()`/`.pl()`/`.arrow()`. ([DuckDB][18]) |
| Inlines SQL params with f‑strings                       | DB‑API placeholders (`?`, `$1`, `$named`) to avoid injection/quoting bugs. ([DuckDB][2])                                                       |
| Writes CSV, then parses elsewhere                       | Write Parquet/CSV/JSON directly with `.write_parquet/.write_csv` or `COPY`. ([DuckDB][1])                                                      |
| Re‑implements simple transforms in Python               | Use SQL (`JOIN/GROUP BY/WINDOW`), the Relational API, or a Python UDF when you must stay in Python. ([DuckDB][3])                              |
| Repeatedly re‑reads remote Parquet and is slow          | `PRAGMA enable_object_cache;` and consider partition pruning/predicates. ([DuckDB][6])                                                         |

---

## 11) Minimal “recipes”

### A. Query a DataFrame, write Parquet, then attach it

```python
import duckdb, pandas as pd
sales = pd.DataFrame({"region":["E","W"], "amt":[10, 20]})

# 1) Query pandas by variable name
top = duckdb.sql("""
  SELECT region, SUM(amt) AS total
  FROM sales
  GROUP BY region
  ORDER BY total DESC
""")

# 2) Persist as Parquet (fast interchange format)
top.write_parquet("top.parquet")

# 3) Attach a persistent DB and load
con = duckdb.connect("app.duckdb")
con.sql("CREATE TABLE top AS SELECT * FROM read_parquet('top.parquet')")
```

([DuckDB][1])

### B. Scan remote Parquet with cached metadata and return Polars

```python
import duckdb
duckdb.sql("INSTALL httpfs; LOAD httpfs; PRAGMA enable_object_cache;")
rel = duckdb.sql("SELECT * FROM 'https://example.com/data/metrics.parquet' WHERE dt >= DATE '2025-10-01'")
pl_df = rel.pl()
```

([DuckDB][4])

### C. Safe parameterized queries and progress

```python
import duckdb
con = duckdb.connect("etl.duckdb", config={"threads": 8})
con.execute("CREATE TABLE IF NOT EXISTS events(id INT, ts TIMESTAMP)")
con.execute("INSERT INTO events VALUES (?, ?)", [1, "2025-10-01 00:00:00"])
con.execute("SELECT * FROM events WHERE id = $1", [1]).fetchall()

con.execute("SET enable_progress_bar = true")
# Long query in one thread; in another thread: con.query_progress() -> float [0..1]
```

([DuckDB][2])

### D. Python UDF, Arrow‑vectorized

```python
import duckdb
from duckdb.typing import BIGINT
import pyarrow as pa

def plus_one(col: pa.Array) -> pa.Array:
    # vectorized: return Arrow array
    import pyarrow.compute as pc
    return pc.add(col, 1)

con = duckdb.connect()
con.create_function("plus_one", plus_one, [BIGINT], BIGINT, type="arrow")
con.sql("SELECT plus_one(x) FROM range(3) t(x)").show()
```

([DuckDB][10])

---

## 12) Gotchas the agent should guard against

* **Global default DB** (`duckdb.sql`) is convenient but shared. Inside libraries, create and pass a **connection** (and close it or use a context manager) to avoid interference. ([DuckDB][1])
* **Threading**: don’t share a connection object across threads; use **`cursor()` per thread** or separate connections. ([DuckDB][1])
* **Large inserts**: avoid `executemany` for big loads—prefer `COPY` from files or load via Parquet/Arrow for efficiency. ([DuckDB][2])
* **Remote I/O**: for HTTP/S3, load `httpfs`; for GCS, HDFS, etc., use `fsspec` registration (Python client only). ([DuckDB][4])
* **Security**: manage credentials with **Secrets Manager** (`CREATE SECRET`), and be cautious with **unsigned/community extensions** (use config flags intentionally). ([DuckDB][16])
* **JSON**: enable JSON extension (autoloads) and use `read_json`/`json_*` functions; use the `JSON` logical type where appropriate. ([DuckDB][19])

---

## 13) Feature checklist for refactoring

* [ ] Replace homegrown CSV/Parquet readers with `read_csv/read_parquet` (+ lists/globs). ([DuckDB][3])
* [ ] Swap HTTP/S3 fetching for `httpfs` + Secrets Manager. ([DuckDB][4])
* [ ] Replace DataFrame loops with SQL over Pandas/Polars by variable name or `register`. ([DuckDB][9])
* [ ] Convert results with `.df()/.pl()/.arrow()` instead of manual conversion. ([DuckDB][1])
* [ ] Parameterize with `?`/`$1`/`$name` instead of string formatting. ([DuckDB][2])
* [ ] Introduce UDFs only when SQL/relational ops are insufficient; prefer Arrow‑type vectorization. ([DuckDB][10])
* [ ] Turn on `enable_object_cache` for repeated scans of the same remote files. ([DuckDB][6])
* [ ] Use `EXPLAIN`/`EXPLAIN ANALYZE` + `threads`/`memory_limit` tuning for slow queries. ([DuckDB][12])

---

## 14) Quick API map (Python)

* **Module/Connection (equivalent methods):** `sql`, `read_csv`, `read_parquet`, `read_json`, `install_extension`, `load_extension`. Module uses the global in‑memory connection. Prefer explicit `duckdb.connect()` in libraries. ([DuckDB][1])
* **Relation → output:** `.df()`, `.pl()`, `.arrow()`, `.fetch*()`, `.write_parquet()`, `.write_csv()`. ([DuckDB][1])
* **DB‑API:** `connect`, `execute`, `executemany`, `fetchone`, `fetchall`, placeholders `?`/`$1`/`$name`. ([DuckDB][2])
* **UDFs:** `create_function(...)`, `remove_function(...)`. ([DuckDB][10])

---

### Pinned references

* Python API overview (install, default vs explicit connections, conversions, extensions). ([DuckDB][1])
* Relational API (lazy Relations, creation/transform/output). ([DuckDB][3])
* SQL on Pandas (variable‑by‑name DataFrames) & Data ingestion precedence. ([DuckDB][9])
* Parquet reading (function inference, lists/globs). ([DuckDB][20])
* HTTPFS extension (HTTP/S3). ([DuckDB][4])
* Secrets Manager / CREATE SECRET. ([DuckDB][16])
* DB‑API prepared statements & guidance. ([DuckDB][2])
* Profiling & EXPLAIN ANALYZE. ([DuckDB][21])
* Object cache pragma. ([DuckDB][6])
* Attach/Detach & remote `.duckdb` files over HTTPS/S3. ([DuckDB][8])
* Python Function API (Python UDFs). ([DuckDB][10])

---

If you want, I can tailor a migration plan from your custom codebase to these primitives (mapping module boundaries to connections, replacing readers/writers with `read_*`/`write_*`, and introducing a thin query layer with parameterized SQL).

[1]: https://duckdb.org/docs/stable/clients/python/overview.html "Python API – DuckDB"
[2]: https://duckdb.org/docs/stable/clients/python/dbapi.html "Python DB API – DuckDB"
[3]: https://duckdb.org/docs/stable/clients/python/relational_api.html "Relational API – DuckDB"
[4]: https://duckdb.org/docs/stable/core_extensions/httpfs/overview.html?utm_source=chatgpt.com "httpfs Extension for HTTP and S3 Support"
[5]: https://duckdb.org/docs/stable/guides/python/filesystems.html?utm_source=chatgpt.com "Using fsspec Filesystems"
[6]: https://duckdb.org/docs/stable/configuration/pragmas.html?utm_source=chatgpt.com "Pragmas"
[7]: https://duckdb.org/docs/stable/sql/statements/export.html?utm_source=chatgpt.com "EXPORT and IMPORT DATABASE Statements"
[8]: https://duckdb.org/docs/stable/sql/statements/attach.html?utm_source=chatgpt.com "ATTACH and DETACH Statements"
[9]: https://duckdb.org/docs/stable/guides/python/sql_on_pandas.html?utm_source=chatgpt.com "SQL on Pandas"
[10]: https://duckdb.org/docs/stable/clients/python/function.html "Python Function API – DuckDB"
[11]: https://duckdb.org/docs/stable/configuration/overview.html?utm_source=chatgpt.com "Configuration"
[12]: https://duckdb.org/docs/stable/guides/meta/explain.html?utm_source=chatgpt.com "EXPLAIN: Inspect Query Plans"
[13]: https://duckdb.org/docs/stable/connect/concurrency.html?utm_source=chatgpt.com "Concurrency"
[14]: https://duckdb.org/docs/stable/guides/file_formats/excel_export.html?utm_source=chatgpt.com "Excel Export"
[15]: https://duckdb.org/docs/stable/core_extensions/delta.html?utm_source=chatgpt.com "Delta Extension"
[16]: https://duckdb.org/docs/stable/configuration/secrets_manager.html?utm_source=chatgpt.com "Secrets Manager"
[17]: https://duckdb.org/docs/stable/data/multiple_files/overview.html?utm_source=chatgpt.com "Reading Multiple Files"
[18]: https://duckdb.org/docs/stable/guides/python/import_pandas.html?utm_source=chatgpt.com "Import from Pandas"
[19]: https://duckdb.org/docs/stable/data/json/overview.html?utm_source=chatgpt.com "JSON Overview"
[20]: https://duckdb.org/docs/stable/data/parquet/overview.html?utm_source=chatgpt.com "Reading and Writing Parquet Files"
[21]: https://duckdb.org/docs/stable/guides/meta/explain_analyze.html?utm_source=chatgpt.com "EXPLAIN ANALYZE: Profile Queries"
