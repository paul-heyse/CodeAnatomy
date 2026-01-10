
 # “v11-native” deep dive into **version drift + breaking changes** for Ibis—and **SQLGlot integration surface** overview(especially for DuckDB-heavy systems) #

---

## 1) The big drift: SQLAlchemy → SQLGlot and what that changes for power users

### SQLGlot is now the compilation IR (not just a “SQL string generator”)

Starting with the “big refactor” era, Ibis moved SQL backends from SQLAlchemy to **SQLGlot**; Ibis 9.0 explicitly calls out “completing the transition from SQLAlchemy to SQLGlot” and simplifying internals as a major enabler for new features/backends. ([Ibis][1])

At the *mechanics* level, Ibis’s compiler infrastructure was generalized to compile into either **SQL strings or SQLGlot expressions**; the internals doc states the compiler translates pieces of a SQL expression into “a string or SQLGlot expression,” then assembles a SELECT statement. ([Ibis][2])

**What this unlocks (practically):**

* **Dialect translation / semantic normalization** via SQLGlot is now a first-class capability Ibis can lean on (e.g., “moving between dialects such that queries are semantically equivalent”). ([GitHub][3])
* **Cross-backend behavioral alignment**: SQLGlot transformations can enforce backend-consistent semantics even when it yields gnarly SQL; this was an explicit motivation for the transition. ([GitHub][3])
* **AST-level tooling**: you can treat compilation output as a SQLGlot AST, run analysis/transforms/lineage, then render.

### Concrete introspection hook: `con.compiler.to_sqlglot(expr)`

The Ibis blog “Does Ibis understand SQL?” demonstrates `con.compiler.to_sqlglot(expr)` returning a SQLGlot AST for an expression, and explicitly says Ibis “delegates SQL generation to SQLGlot,” essentially calling `.sql()` on that SQLGlot expression. ([Ibis][4])

This gives you a *stable mental model*:

```python
# 1) build ibis expression tree (lazy)
expr = t.filter(t.x > 0).select(u=t.x)

# 2) compile to SQLGlot expression
sg_expr = con.compiler.to_sqlglot(expr)

# 3) render dialect-specific SQL
sql = sg_expr.sql(dialect="duckdb", pretty=True)
```

Even if you never call SQLGlot directly, `ibis.to_sql(expr)` is basically the “friendly façade” over this pipeline. ([Ibis][4])

---

## 2) Ibis v11 breaking changes that matter for DuckDB + SQLGlot-centric stacks

I’m going to focus on the *high-leverage* ones for codebases that:

* create in-memory relations (memtables),
* bridge in/out of raw SQL and DDL,
* rely on schemas/types,
* generate SQL snapshots, lineage graphs, etc.

### 2.1 Memtables: naming is no longer supported (you must materialize a name)

In v11 breaking changes: **“memtables can no longer be named explicitly”**; use `create_table` or `create_view` to create a named object. ([Ibis][5])

This is one of the most important shifts for systems that used to treat memtables as “named temps.”

**Recommended v11 pattern:**

* Treat `ibis.memtable(...)` as **anonymous** (pure expression input).
* If you need a stable identifier for downstream raw SQL or `Table.sql(...)`, explicitly **materialize** it:

```python
mt = ibis.memtable(df)            # anonymous relation
con.create_table("staging_x", mt) # or create_view(...)
t = con.table("staging_x")
```

### 2.2 `Struct.destructure` → `Table.unpack` (nested/struct workflows)

v11 removes `Struct.destructure` in favor of `Table.unpack`. ([Ibis][5])
If you do nested schema work (struct columns, JSON-ish shaping, etc.), this is the canonical path now.

### 2.3 “to_*” conversion APIs renamed to “as_*”

v11 removes older “to_*” coercions in favor of “as_*”:

* `String.to_date` → `String.as_date` ([Ibis][5])
* `String.to_timestamp` → `String.as_timestamp` ([Ibis][5])
* `IntegerValue.to_interval` → `IntegerValue.as_interval` ([Ibis][5])
* `IntegerValue.to_timestamp` → `IntegerValue.as_timestamp` ([Ibis][5])

This is mostly mechanical, but it matters if you’re generating expression code programmatically.

### 2.4 `ibis.case()` removed → `ibis.cases()`

v11 removes top-level `ibis.case()`; use `ibis.cases()` instead. ([Ibis][5])
If you’re building complex conditional logic generators, normalize now on `cases()`.

### 2.5 DataType metadata: `DataType.name` removed

v11 removes `DataType.name` (use `DataType.__class__.__name__`). ([Ibis][5])
If you used `.name` for serialization, schema registries, or contract hashing, update that code path. (For stable serialization, consider your own canonicalization rather than class names.)

### 2.6 Ordering / selectors / validators drift that will break older idioms

From the generated release notes (earlier breaking wave, but commonly encountered as “why did this stop working?”):

* `ibis.expr.selectors` removed; use `ibis.selectors` instead ([Ibis][6])
* `table.order_by()` no longer accepts tuple(s); use `ibis.asc(key)` / `ibis.desc(key)` ([Ibis][6])
* validation helpers removed from `ibis.common.validators` / `ibis.expr.rules`; use type hints or `ibis.common.patterns` ([Ibis][6])

Also, Ibis explicitly raised the **minimum SQLGlot version** in the past to improve backend type parsing speed/robustness (good signal that **type parsing is now “SQLGlot-shaped”**). ([Ibis][6])

---

## 3) SQLGlot integration “surface area” you should actively use in v11

### 3.1 `Expr.to_sql(...)` is now the canonical “compile me” method

`Expr.to_sql(dialect=None, pretty=True, **kwargs)` compiles to formatted SQL; dialect resolution uses the dialect bound to the expression or defaults if none is bound. It also accepts **scalar parameters via `**kwargs`**. ([Ibis][7])

Key practical implications:

* **Bound expressions** (coming from `con.table(...)`, `con.read_parquet(...)`, etc.) already know their dialect; literals/unbound expressions may not.
* You can compile **the same expression** to multiple dialects for portability checks or snapshot tests. ([Ibis][7])

### 3.2 Stable AST access: compile to SQLGlot and operate *before* stringification

From the SQL blog example:

* Ibis delegates SQL generation to SQLGlot and calls `.sql()` on the SQLGlot expression ([Ibis][4])
* You can extract the SQLGlot representation via `con.compiler.to_sqlglot(expr)` ([Ibis][4])

That’s your hook for:

* column lineage (SQLGlot tooling),
* rewriting (injecting filters, enforcing naming/quoting, normalizing CTEs),
* diffing changes at AST level (more robust than string diffs).

### 3.3 SQL strings in Ibis: `Table.sql`, `Backend.sql`, `raw_sql` are *three different beasts*

The Ibis SQL blog is the clearest statement of the tradeoffs:

* `Table.sql(...)` wraps a SQL string in a `SQLStringView` node; Ibis mainly knows the **output schema** for validation downstream, not the internal structure. ([Ibis][4])
* `Backend.sql(...)` is similar but can only reference already-existing DB tables; it uses a `SQLQueryResult` node. ([Ibis][4])
* Despite that opacity, Ibis still compiles these to SQLGlot “under the hood,” so SQLGlot capabilities (e.g., lineage) remain applicable. ([Ibis][4])
* `Backend.raw_sql()` is **opaque**: Ibis just executes and returns a cursor; it does not attempt to understand/compile it. ([Ibis][4])

**Power-user implication:** prefer `Table.sql` / `Backend.sql` over `raw_sql` whenever you want your query to stay in the “Ibis+SQLGlot analyzable” world.

### 3.4 `Table.alias(...)` is a sharp tool (temporary view side effect)

The `Table.alias` docs explicitly say it creates a temporary view in the database as a side effect, and that this side effect “will be removed in a future version” and is “not part of the public API.” ([Ibis][8])

This matters if you do patterns like:

```python
expr = some_ibis_expr.alias("x").sql('SELECT ... FROM "x" ...')
```

That works today, but if you want “future-proof correctness,” shift to explicit `create_view/create_table` for naming.

---

## 4) Schema + DDL round-trips: Ibis ↔ SQLGlot (high-leverage for contracts)

This is where SQLGlot synergy becomes *architectural*, not just “compilation.”

### 4.1 SQLGlot → Ibis Schema

`ibis.Schema.from_sqlglot(schema_expr, dialect=...)` converts SQLGlot column defs into an Ibis schema, with dialect-aware type conversion. ([Ibis][9])

### 4.2 Ibis Schema → SQLGlot ColumnDefs (and Create Table)

`Schema.to_sqlglot_column_defs(dialect=...)` returns a list of SQLGlot `ColumnDef` nodes, explicitly intended to embed into a SQLGlot `CREATE TABLE`. ([Ibis][9])
Also, `Schema.to_sqlglot(...)` is deprecated in favor of `to_sqlglot_column_defs()`. ([Ibis][9])

If you maintain **dataset contracts** (schemas) and want reproducible DDL for DuckDB:

```python
sch = ibis.schema({"a": "int", "b": "!string"})
cols = sch.to_sqlglot_column_defs(dialect="duckdb")

# Embed in SQLGlot CREATE TABLE
# (docs show the exact pattern and output)
```

The docs show this yields a dialect-correct `CREATE TABLE ...` string via SQLGlot. ([Ibis][9])

---

## 5) Advanced “standalone Ibis” features that pair extremely well with SQLGlot + DuckDB

### 5.1 Builtin UDF escape hatch (DuckDB functions not wrapped by Ibis)

If DuckDB has a built-in you want but Ibis doesn’t expose, `@ibis.udf.scalar.builtin` is the intended escape hatch. ([Ibis][10])

Critical behavior: **Ibis ignores the function body**; it won’t execute or inspect it—only the signature matters. ([Ibis][10])
This keeps the function as a pure expression node that compiles via SQLGlot into a backend call.

Also note: when compiling literal-only expressions, you often must pass `dialect="duckdb"` because there is no backend bound. ([Ibis][10])

### 5.2 DuckDB backend “power knobs” are first-class in Ibis v11

The DuckDB backend connect surface is explicitly:
`do_connect(database=':memory:', read_only=False, extensions=None, **config)` ([Ibis][11])

And since v10.0.0, Ibis no longer creates temp directories for `temp_directory`; it passes the value directly to DuckDB and you may need to `mkdir` yourself. ([Ibis][11])

The backend also exposes:

* `compile(expr, *, limit=None, params=None, pretty=False)` with **`params` mapping** for scalar substitution ([Ibis][11])
* hierarchy semantics (catalog/database terminology) and attach/detach APIs; DuckDB docs explicitly note Ibis avoids using “schema” to refer to hierarchy. ([Ibis][11])

---

## 6) Compatibility note: Ibis v11 + SQLGlot 28.4+

Ibis 11.0.0’s core dependency is `sqlglot>=23.4` with an explicit exclusion for `26.32.0`, and no upper bound. ([GitHub][12])
So SQLGlot 28.4+ is *allowed* by packaging, but operationally I’d treat SQLGlot bumps as “compiler upgrades” and gate them with:

* golden SQL snapshots (`expr.to_sql(pretty=True)`),
* a small suite of `con.compiler.to_sqlglot(expr)` structural asserts,
* and execution tests on DuckDB.

---

[1]: https://ibis-project.org/posts/ibis-version-9.0.0-release/ "Ibis 9.0: SQLGlot-ification – Ibis"
[2]: https://ibis-project.org/concepts/internals "internals – Ibis"
[3]: https://github.com/ibis-project/ibis/discussions/7213 "Moving our SQL backends to sqlglot · ibis-project ibis · Discussion #7213 · GitHub"
[4]: https://ibis-project.org/posts/does-ibis-understand-sql/ "Does Ibis understand SQL? – Ibis"
[5]: https://ibis-project.org/release_notes "Release notes – Ibis"
[6]: https://ibis-project.org/release_notes_generated.html "release_notes_generated – Ibis"
[7]: https://ibis-project.org/reference/expression-generic "expression-generic – Ibis"
[8]: https://ibis-project.org/reference/expression-tables "expression-tables – Ibis"
[9]: https://ibis-project.org/reference/schemas "schemas – Ibis"
[10]: https://ibis-project.org/how-to/extending/builtin "builtin – Ibis"
[11]: https://ibis-project.org/backends/duckdb "duckdb – Ibis"
[12]: https://raw.githubusercontent.com/ibis-project/ibis/master/pyproject.toml "raw.githubusercontent.com"

# **power-user catalog** of the **Ibis v11 DuckDB backend** surface area #

Below is a **power-user catalog** of the **Ibis v11 DuckDB backend** surface area (plus “hidden-in-plain-sight” DuckDB features you access through Ibis), optimized for **maximum coverage** and **high text-density**.

---

## 0) Mental model: DuckDB backend = (a) a configured DuckDB session + (b) a big set of “register / materialize / export” helpers

**Core entrypoints**

* `ibis.duckdb.connect(...)` is a thin wrapper over `Backend.do_connect(database=':memory:', read_only=False, extensions=None, **config)` and forwards `**config` into DuckDB’s own config system. ([Ibis][1])
* You can also `ibis.connect("duckdb://local.ddb")`; `duckdb://` (empty path) means ephemeral in-memory. ([Ibis][1])

**Power implication:** treat the backend as the place where you (1) set DuckDB configuration/credentials/extensions once, then (2) build *Ibis* expressions with strong control over ingestion/export, and (3) optionally drop into SQL when needed.

---

## 1) Connection & session configuration (DuckDB knobs are first-class)

### 1.1 `ibis.duckdb.connect` / URL connect

**Signature (important):** `do_connect(self, database=':memory:', read_only=False, extensions=None, **config)` ([Ibis][1])

* `database`: file path or `:memory:` ([Ibis][1])
* `read_only`: open DB read-only (useful for “serving” processes) ([Ibis][1])
* `extensions`: list of DuckDB extensions to install/load *on connect* (see §2) ([Ibis][1])
* `**config`: any DuckDB config parameters (e.g., `threads`, `memory_limit`, `temp_directory`) ([Ibis][1])

**Temp dir behavior change:** Ibis no longer creates intermediate directories for `temp_directory`; you may need to `mkdir` yourself. ([Ibis][1])

### 1.2 Runtime configuration via SQL (SET/PRAGMA)

DuckDB config options can be set via `SET` or `PRAGMA`, and queried via `current_setting()` or `duckdb_settings()` table function. ([DuckDB][2])
In Ibis, you typically apply these through `con.raw_sql("SET ...")` (see §3). ([Ibis][3])

---

## 2) Extension management (critical for “power” features: cloud, spatial, DB scanners, excel, delta…)

DuckDB’s extension system is **dynamic**: you install once, then load each process start; many extensions can autoload on first use. ([DuckDB][4])

### 2.1 Connect-time extension bootstrap

Use `extensions=[...]` at connect to ensure required extensions are present/loaded before any reads/writes. ([Ibis][1])

### 2.2 Runtime extension load from Ibis

`con.load_extension(extension, force_install=False)` installs + loads by name/path. ([Ibis][1])

This is the standard way to turn on capabilities you’ll use via:

* cloud/http (`httpfs`, `aws`)
* geospatial (`spatial`)
* excel (`excel`)
* delta (`delta`) depending on your workflow

(If you need deeper extension lifecycle behaviors, DuckDB describes install vs load steps and autoloading explicitly. ([DuckDB][4]))

---

## 3) Escape hatches: “how to run DuckDB SQL” (and how not to shoot yourself)

### 3.1 `Backend.raw_sql` (DDL/PRAGMAs/secrets/install/load)

`Backend.raw_sql` executes arbitrary SQL and returns the cursor; you may need to close it to avoid leaks (context-manager recommended). ([Ibis][3])

Use cases:

* `INSTALL ...; LOAD ...;`
* `SET ...` / `PRAGMA ...`
* `CREATE SECRET ...`
* `ATTACH ...`
* any DDL DuckDB supports that Ibis won’t model

### 3.2 `Backend.sql` / `Table.sql` (keep your query analyzable and composable)

* `Backend.sql(...)` returns an Ibis table expression and supports `dialect=...`. ([Ibis][3])
* `Table.sql(...)` runs arbitrary `SELECT` against an Ibis table expression and supports `dialect=...` for porting SQL from other dialects. ([Ibis][3])

**Why you care:** unlike `raw_sql`, these stay in the “Ibis expression world” and remain composable with `.filter/.select/.group_by/...`.

---

## 4) Catalog/database/table hierarchy + DDL and metadata APIs (production-grade “DB management” layer)

Ibis standardizes hierarchy terms: **catalog** > **database** > **table** (it explicitly avoids using “schema” as the generic concept). ([Ibis][1])

### 4.1 Hierarchy introspection

* `list_catalogs(like=...)` ([Ibis][1])
* `list_databases(like=..., catalog=...)` ([Ibis][1])
* `list_tables(like=..., database=...)` with `database` as `"catalog.database"` or `("catalog","database")` for multi-level hierarchies ([Ibis][1])

### 4.2 Create/drop databases

* `create_database(name, catalog=None, force=False)` ([Ibis][1])
* `drop_database(name, catalog=None, force=False)` ([Ibis][1])

### 4.3 Create/drop/rename/truncate tables/views

* `create_table(name, obj=None, schema=None, database=None, temp=False, overwrite=False)`

  * `obj` supports `pd.DataFrame | pa.Table | pl.DataFrame | pl.LazyFrame | ir.Table | None` ([Ibis][1])
  * `database` supports dotted/tuple multi-level selection ([Ibis][1])
* `create_view(name, obj, database=None, overwrite=False)` ([Ibis][1])
* `drop_table(name, database=None, force=False)` / `drop_view(name, database=None, force=False)` ([Ibis][1])
* `rename_table(old_name, new_name)` ([Ibis][1])
* `truncate_table(name, database=None)` ([Ibis][1])

### 4.4 Insert

`insert(name, obj, database=None, overwrite=False)` where `obj` can be `pd.DataFrame | ir.Table | list | dict`. ([Ibis][1])
This is your “fast path” for pushing small/medium computed results into persistent DuckDB tables without leaving Ibis.

### 4.5 Schema

* `get_schema(table_name, catalog=None, database=None)` ([Ibis][1])
* `table(name, database=None)` to reference an existing table (paired with `create_*`). ([Ibis][1])

---

## 5) Multi-database + federation primitives (Attach / detach + external DB readers)

### 5.1 Attach/detach DuckDB databases

* `attach(path, name=None, read_only=False)` attaches another DuckDB DB into the current session ([Ibis][1])
* `detach(name)` detaches it ([Ibis][1])

This is how you implement:

* “semantic layer DB” + “raw ingest DB” in one query session
* migrations by attaching old/new DBs and comparing

### 5.2 SQLite attach (full DB)

`attach_sqlite(path, overwrite=False, all_varchar=False)` attaches SQLite and can force all columns to `VARCHAR` to dodge type issues. ([Ibis][1])

### 5.3 External DB table import into DuckDB tables

These are *high leverage* when you want to treat DuckDB as a fast local “compute/cache” engine:

* `read_sqlite(path, table_name=None)` ([Ibis][1])
* `read_postgres(uri, table_name=None, database='public')` ([Ibis][1])
* `read_mysql(uri, catalog=..., table_name=None)` (note: `catalog` is required alias) ([Ibis][1])

These return Ibis tables that you can then transform and/or materialize (`create_table`, `to_parquet`, etc.).

---

## 6) File & lake ingestion APIs (the “scan/register table” power surface)

All of these return an Ibis table expression registered in the session (often with optional `table_name=`), and most forward `**kwargs` directly into DuckDB’s native readers.

### 6.1 CSV

`read_csv(paths, table_name=None, columns=None, types=None, **kwargs)`

* `columns`: map *all* columns to types
* `types`: map *subset* of columns to types
* `**kwargs` passed to DuckDB CSV reader ([Ibis][1])

### 6.2 Parquet

`read_parquet(paths, table_name=None, **kwargs)` where `paths` can be file, iterable, or directory; kwargs go to DuckDB parquet reader. ([Ibis][1])

### 6.3 JSON

`read_json(paths, table_name=None, columns=None, **kwargs)` (newline-delimited JSON; kwargs passed to DuckDB `read_json_auto`). ([Ibis][1])

### 6.4 Delta Lake

`read_delta(path, table_name=None, **kwargs)` (directory containing a Delta table; kwargs passed to `deltalake.DeltaTable`). ([Ibis][1])

### 6.5 Geospatial (experimental)

* Backend supports experimental geospatial ops; enable via `ibis-framework[geospatial]` / geopandas + shapely. ([Ibis][1])
* `read_geo(path, table_name=None, **kwargs)` registers a geospatial file using DuckDB spatial capabilities. ([Ibis][1])

### 6.6 Excel

`read_xlsx(path, sheet=None, range=None, **kwargs)` requires DuckDB ≥ 1.2.0. ([Ibis][1])

---

## 7) “No explicit connection” ingest: top-level `ibis.read_parquet/read_csv` (rapid workflows)

`ibis.read_parquet(...)` (and similar `ibis.read_*`) can return a table expression **without you creating a connection first**; Ibis spins up a DuckDB connection (default backend) when you call it. ([Ibis][5])

This is excellent for:

* quick ad hoc exploration
* pipelines where you don’t care about session state

…but if you need **session state** (secrets, extensions, settings), you’ll typically prefer an explicit `con = ibis.duckdb.connect(...)` and then `con.read_*`.

---

## 8) Cloud + remote storage (secrets, AWS/GCS, and when to use fsspec)

### 8.1 DuckDB secrets via Ibis (private buckets, multi-cloud)

Ibis explicitly: no Ibis-native secrets API for DuckDB; use `raw_sql` to create secrets. ([Ibis][1])
DuckDB secrets manager supports typed secrets, scoping by path prefix, and temporary vs persistent secrets (persistent stored unencrypted on disk). ([DuckDB][6])

### 8.2 AWS S3 “credential_chain” (DuckDB ≥1.4 behavior change matters)

DuckDB’s `aws` extension recommends configuring S3 via secrets and supports `credential_chain`. ([DuckDB][7])
Key v1.4+ nuance:

* Since **v1.4.0**, `credential_chain` validates credentials at `CREATE SECRET` time and fails if missing. ([DuckDB][7])
* Since **v1.4.1**, you can configure validation with `VALIDATION` (e.g., `none` to allow creation even if creds absent at creation time). ([DuckDB][7])
* Supports `REFRESH auto` for auto-refreshable credentials. ([DuckDB][7])

### 8.3 fsspec filesystem registration (Python-client-only capability)

Ibis exposes `con.register_filesystem(filesystem)` which registers an `fsspec` filesystem object and enables `read_csv/read_parquet/read_json/...` against that filesystem. ([Ibis][1])

DuckDB notes: fsspec allows querying filesystems **httpfs doesn’t support**, but it’s Python-client-only and may be slower/buggier because it’s third-party Python code. ([DuckDB][8])

Concrete GCS example:

* Ibis how-to: `con.register_filesystem(gcs)` then `con.read_json("gs://...")` ([Ibis][9])
* Backend docs show analogous `gcs://...` CSV example. ([Ibis][1])

---

## 9) Execution, compilation, parameter binding, and streaming results

### 9.1 Compile vs execute

* `compile(expr, limit=None, params=None, pretty=False)` returns SQL string; supports scalar `params` mapping. ([Ibis][1])
* `execute(expr, params=None, limit=None, **kwargs)` executes an expression. ([Ibis][1])

### 9.2 SQL to Ibis table (schema inference + cross-dialect parsing)

`con.sql(query, schema=None, dialect=None)` returns an Ibis table; `dialect` is explicitly for SQL written in a different dialect than the backend. ([Ibis][1])
More generally, Ibis documents that `.sql(..., dialect="mysql")` works for porting SQL. ([Ibis][3])

### 9.3 Output conversions / streaming (important for large results)

* `to_pandas(expr, params=None, limit=None, **kwargs)` wraps `execute`. ([Ibis][1])
* `to_pandas_batches(expr, chunk_size=...)` yields chunked DataFrames. ([Ibis][1])
* `to_pyarrow(expr, ...)` returns Arrow table/array/scalar. ([Ibis][1])
* `to_pyarrow_batches(expr, chunk_size=...)` returns a `RecordBatchReader`; note: unbounded cursor lifetime; may need explicit cursor release. ([Ibis][1])
* `to_polars(expr, ...)` ([Ibis][1])
* `to_torch(expr, ...)` returns dict of tensors. ([Ibis][1])

---

## 10) Eager export / materialization APIs (CSV/Parquet/JSON/Delta/Geo/Excel)

All of these are “execute now” (eager) helpers for turning an Ibis expression into artifacts.

### 10.1 CSV / JSON

* `to_csv(expr, path, params=None, header=True, **kwargs)` (kwargs are DuckDB CSV writer args). ([Ibis][1])
* `to_json(expr, path, compression='auto', dateformat=None, timestampformat=None)`; supports URLs such as S3 buckets. ([Ibis][1])

### 10.2 Parquet

* `to_parquet(expr, path, params=None, **kwargs)`; docs show partitioning via `partition_by`. ([Ibis][1])
* `to_parquet_dir(expr, directory, params=None, **kwargs)` writes parquet file in a directory (passes kwargs to `pyarrow.dataset.write_dataset`). ([Ibis][1])

### 10.3 Delta Lake

`to_delta(expr, path, params=None, **kwargs)`; kwargs go to `deltalake.writer.write_deltalake`. ([Ibis][1])

### 10.4 Geospatial output

`to_geo(expr, path, format=..., layer_creation_options=None, params=None, limit=None, **kwargs)` writes using GDAL vector formats. ([Ibis][1])

### 10.5 Excel

`to_xlsx(expr, path, sheet='Sheet1', header=False, params=None, **kwargs)` requires DuckDB ≥ 1.2.0. ([Ibis][1])

---

## 11) MotherDuck (remote DuckDB) integration

DuckDB backend supports MotherDuck: connect with `ibis.duckdb.connect("md:")` / `motherduck:`; auth prompt triggers on first call requiring retrieval (e.g., `list_tables`). ([Ibis][1])

---

### If you want one “deployment blueprint” (opinionated but practical)

For a production service that reads Parquet/JSON from cloud, does heavy transforms, and exports results:

1. `con = ibis.duckdb.connect(..., extensions=[...], threads=..., memory_limit=..., temp_directory=...)` ([Ibis][1])
2. `con.raw_sql("CREATE SECRET ...")` (or persistent secrets) + any `SET/PRAGMA` tuning ([Ibis][3])
3. Ingest via `con.read_parquet/read_json/read_csv` or top-level `ibis.read_parquet` for ephemeral analysis ([Ibis][1])
4. Materialize stable intermediates with `create_table/create_view(temp=True/overwrite=True)` ([Ibis][1])
5. Export with `to_parquet/to_json/to_delta` or stream via `to_pyarrow_batches` (cursor lifetime awareness) ([Ibis][1])

If you’d like, I can now turn this into a **single repo-ready markdown reference** with:

* a canonical “DuckDB session bootstrap” function (extensions + secrets + settings),
* a recommended “ingest/materialize/export” layered API,
* and a short checklist mapping “feature → required extension/secret/config” (especially for S3/GCS/MotherDuck/spatial/delta).

[1]: https://ibis-project.org/backends/duckdb "duckdb – Ibis"
[2]: https://duckdb.org/docs/stable/configuration/overview.html "Configuration – DuckDB"
[3]: https://ibis-project.org/how-to/extending/sql "sql – Ibis"
[4]: https://duckdb.org/docs/stable/extensions/overview.html "Extensions – DuckDB"
[5]: https://ibis-project.org/how-to/input-output/duckdb-parquet "Read parquet files with Ibis – Ibis"
[6]: https://duckdb.org/docs/stable/configuration/secrets_manager.html "Secrets Manager – DuckDB"
[7]: https://duckdb.org/docs/stable/core_extensions/aws.html "AWS Extension – DuckDB"
[8]: https://duckdb.org/docs/stable/guides/python/filesystems.html "Using fsspec Filesystems – DuckDB"
[9]: https://ibis-project.org/how-to/input-output/gcs_duckdb "Loading Google Cloud Storage files with DuckDB – Ibis"


# Parameterization in Ibis v11 (DuckDB + SQLGlot): the full “prepared-statement style” toolkit #

### 1) Primitive: **typed deferred scalar parameters** (`ibis.param`)

`ibis.param(type)` creates a **deferred parameter** (a scalar expression “backed by a parameter”) that can appear anywhere a scalar expression can appear (filters, projections, joins, window frames, function args, etc.). ([Ibis][1])

* **Type is mandatory** and is an Ibis `DataType` (strings like `"int64"`, `"date"`, `"string"` are fine). ([Ibis][1])
* Nullability is part of the type system; Ibis’s dtype helper documents the `!` prefix convention for non-nullable types (e.g., `"!string"`). ([Ibis][2])

**Canonical pattern (template once, bind many times):**

```python
import ibis
from datetime import date

start = ibis.param("date")                 # deferred scalar param :contentReference[oaicite:3]{index=3}
t = ibis.memtable({"date_col": [date(2013,1,1), date(2013,1,2)], "v": [1.0, 2.0]})
expr = t.filter(t.date_col >= start).v.sum()

expr.execute(params={start: date(2013,1,1)})  # bind at execution :contentReference[oaicite:4]{index=4}
expr.execute(params={start: date(2013,1,2)})
```

**Operational value**

* Lets you define **stable expression templates** in code (compile shape stays constant at the Ibis IR level) and bind request-time values safely (no SQL string interpolation).
* Keeps type-checking: the param has a declared Ibis type, so misuse is caught earlier than raw SQL.

---

### 2) Binding mechanism: `params={ScalarParamExpr: value}`

Ibis parameterization is expressed as a **mapping from parameter expressions to concrete Python values**. Many APIs accept this uniformly as `params`.

#### 2.1 Execute with params

Ibis docs call this out explicitly: “Scalar parameters can be supplied dynamically during execution.” ([Ibis][3])

```python
species = ibis.param("string")
expr = t.filter(t.species == species).order_by(t.bill_length_mm)
expr.execute(limit=3, params={species: "Gentoo"})  # :contentReference[oaicite:6]{index=6}
```

#### 2.2 Compile with params (SQL backends)

Table/expr `.compile(...)` supports `params` as “Mapping of scalar parameter expressions to value”. ([Ibis][3])
DuckDB backend compilation also exposes `compile(..., params=..., pretty=...)`. ([Ibis][4])

```python
sql = expr.compile(params={species: "Gentoo"}, pretty=True)   # :contentReference[oaicite:9]{index=9}
# or backend-level:
sql = con.compile(expr, params={species: "Gentoo"}, pretty=True)  # :contentReference[oaicite:10]{index=10}
```

**Operational value**

* Enables **golden SQL snapshot tests** per parameter scenario.
* Lets you pre-render SQL for auditing/lineage and still keep the Ibis template as canonical.

---

### 3) SQL emission with scalar parameters: `to_sql(..., **kwargs)` / `ibis.to_sql(..., **kwargs)`

Both `expr.to_sql(...)` and `ibis.to_sql(expr, ...)` accept `**kwargs` documented as “Scalar parameters”. ([Ibis][1])

This is the ergonomic “prepared-statement-ish” UX for templates when you want keyword binding:

```python
p = ibis.param("int64").name("min_age")  # give the param a stable name (Expr.name exists) :contentReference[oaicite:12]{index=12}
expr = people.filter(people.age >= p)

sql = expr.to_sql(dialect="duckdb", min_age=40)     # kwargs = scalar params :contentReference[oaicite:13]{index=13}
sql = ibis.to_sql(expr, dialect="duckdb", min_age=40) :contentReference[oaicite:14]{index=14}
```

**Key nuance:** `.compile(params=...)` uses the mapping form; `.to_sql(**kwargs)` uses keyword binding (so naming parameters is what makes this sane at scale).

---

### 4) Where `params` flows (beyond execute/compile): exports + batch APIs

If you build “templated exports” (common in pipelines/serving), DuckDB backend methods and many expression helpers accept `params` as well; e.g., DuckDB backend `to_parquet(..., params=...)`, `to_pyarrow_batches(..., params=...)`, etc. are documented uniformly. ([Ibis][4])

**Operational value**

* One expression template can drive: query → export parquet/json/delta → stream arrow batches, with request-time params applied consistently.

---

### 5) “Prepared statement style” design patterns in Ibis (what to standardize)

#### 5.1 Treat params as *request inputs*; treat `limit` as an *out-of-band control*

Many APIs separate `params` from `limit` (DuckDB backend execute/exports do this). ([Ibis][4])
This is useful for serving: you keep the query template stable and vary pagination/preview limits without mutating the expression.

#### 5.2 Optional filters without dynamic SQL

If a filter is optional, avoid branching SQL strings. Make the predicate conditional on a nullable param (nullable dtype) and bind `None` when “disabled”. (Mechanically: `isnull`, boolean OR, etc. are part of `Value` methods. ([Ibis][1]))

#### 5.3 Large “IN list” inputs: prefer **memtable + join** over placeholder explosions

For variable-length sets (IDs, file lists), the robust approach is:

* create an in-memory relation (`ibis.memtable(ids)`),
* `create_table`/`create_view` if you need a stable name (v11 memtable naming is restricted; you materialize names explicitly),
* join/semi-join instead of generating `IN (?, ?, ..., ?)`.

This avoids huge SQL strings, avoids driver placeholder limits, and is often faster in DuckDB.

---

### 6) If you truly need *DuckDB-native* prepared statements (plan caching, DB-API placeholders)

DuckDB supports prepared statement parameters with **`?`**, **`$1`/`$2`**, and **named `$param`** syntaxes at the SQL layer. ([DuckDB][5])
DuckDB’s Python DB-API supports parameterized execution via `execute`/`executemany` with `?` or `$1` placeholders. ([DuckDB][6])

**What this means in practice:**

* Ibis’s `params` gives you **expression-level parameterization** (safe templating + typed inputs).
* DuckDB prepared statements give you **backend-level placeholders** (potentially better for repeated execution + plan reuse).

If your serving layer needs strict “same SQL text, different bound values” semantics, the clean split is:

1. Use Ibis for relational construction + correctness.
2. Render SQL (or generate a SQLGlot AST) for auditing/lineage.
3. For the hot path, use DuckDB DB-API parameterized SQL where appropriate. ([DuckDB][6])
   SQLGlot can help you programmatically build/transform SQL reliably (parser/AST building is a core advertised capability). ([Sqlglot][7])

---

If you want, I can package the above into a **single “QueryTemplate” utility** you can standardize in your codebase (typed param registry + execution helpers + SQL snapshot hooks + a memtable-join pattern for list inputs), tuned specifically for Ibis v11 + DuckDB serving workloads.

[1]: https://ibis-project.org/reference/expression-generic "expression-generic – Ibis"
[2]: https://ibis-project.org/reference/datatypes?utm_source=chatgpt.com "datatypes - Ibis"
[3]: https://ibis-project.org/reference/expression-tables?utm_source=chatgpt.com "Table expressions - Ibis"
[4]: https://ibis-project.org/backends/duckdb?utm_source=chatgpt.com "DuckDB - Ibis"
[5]: https://duckdb.org/docs/stable/sql/query_syntax/prepared_statements.html?utm_source=chatgpt.com "Prepared Statements"
[6]: https://duckdb.org/docs/stable/clients/python/dbapi.html?utm_source=chatgpt.com "Python DB API"
[7]: https://sqlglot.com/?utm_source=chatgpt.com "sqlglot API documentation"

# **single-file `QueryTemplate` utility** #

Below is a **single-file `QueryTemplate` utility** you can standardize around for **Ibis v11 + DuckDB serving**, covering:

* **typed scalar params** (`ibis.param(...)`) + binding via `params={param_expr: value}` (Ibis APIs take a “mapping of scalar parameter expressions to value” for both compile + execute) ([Ibis][1])
* **list params** via the **memtable → temp table → semi-join** pattern (avoids giant `IN (...)` literals; keeps everything in one DuckDB backend session)
* **SQL snapshot hooks** (`pretty=True` compilation) ([Ibis][1])
* optional **SQLGlot AST hook**, leveraging that Ibis compilation targets SQLGlot expressions internally ([Ibis][2])

---

## `query_template.py`

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Generic, Iterable, Mapping, MutableMapping, Optional, Sequence, TypeVar
import hashlib
import re
import uuid

import ibis
import ibis.expr.types as ir


TExpr = TypeVar("TExpr", bound=ir.Expr)


_MISSING = object()


def _sanitize_ident(s: str) -> str:
    # DuckDB identifiers are flexible with quoting, but keep temp names simple/portable.
    s = re.sub(r"[^a-zA-Z0-9_]+", "_", s).strip("_")
    return s or "x"


def _stable_hash(*parts: str, n: int = 10) -> str:
    h = hashlib.sha1(":".join(parts).encode("utf-8")).hexdigest()
    return h[:n]


@dataclass(frozen=True)
class ScalarParam:
    """A typed scalar parameter for Ibis parameterization.

    Bound using params={param_expr: value} at compile/execute time.
    """
    name: str
    dtype: Any  # ibis dtype-like (string or dt.DataType)
    required: bool = True
    default: Any = _MISSING

    def make_expr(self) -> ir.Scalar:
        # ibis.param(...) creates a deferred scalar parameter expression
        # Name it so keyword-based tooling / diagnostics are sane.
        return ibis.param(self.dtype).name(self.name)  # type: ignore[no-any-return]


@dataclass(frozen=True)
class ListParam:
    """A list-valued input staged into DuckDB as a temp table and used via joins/semi-joins."""
    name: str
    dtype: Any  # element dtype
    column: Optional[str] = None
    required: bool = True
    default: Any = _MISSING
    dedupe: bool = True

    def col(self) -> str:
        return self.column or self.name

    def temp_table_base(self, template_name: str) -> str:
        # Base is stable for the template; final name includes an instance suffix.
        return f"__qt_{_sanitize_ident(template_name)}_{_sanitize_ident(self.name)}_{_stable_hash(template_name, self.name)}"


@dataclass
class ParamNamespace:
    """Attribute access to scalar param expressions: ctx.p.repo, ctx.p.min_score, ..."""
    _scalars: Mapping[str, ir.Scalar]

    def __getattr__(self, key: str) -> ir.Scalar:
        try:
            return self._scalars[key]
        except KeyError as e:
            raise AttributeError(key) from e

    def __getitem__(self, key: str) -> ir.Scalar:
        return self._scalars[key]


@dataclass
class ListNamespace:
    """Attribute access to staged list tables: ctx.l.goids, ctx.l.modules, ..."""
    _con: Any
    _tables: Mapping[str, str]  # param_name -> staged table name

    def __getattr__(self, key: str) -> ir.Table:
        try:
            return self._con.table(self._tables[key])
        except KeyError as e:
            raise AttributeError(key) from e

    def __getitem__(self, key: str) -> ir.Table:
        return self._con.table(self._tables[key])


@dataclass(frozen=True)
class QueryCtx:
    """Context object passed to builder: connection + scalar params + list tables + bound values."""
    con: Any
    p: ParamNamespace
    l: ListNamespace
    values: Mapping[str, Any]

    def has(self, name: str) -> bool:
        """True iff a value is bound (and not None)."""
        return name in self.values and self.values[name] is not None


@dataclass
class QueryTemplate(Generic[TExpr]):
    """Reusable, parameterized Ibis expression template.

    - Scalar params: ibis.param(dtype) compiled/executed with params={param_expr: value}
    - List params: staged to temp tables per execution and used via joins/semi-joins
    """
    name: str
    build: Callable[[QueryCtx], TExpr]

    _scalar_specs: dict[str, ScalarParam] = field(default_factory=dict)
    _list_specs: dict[str, ListParam] = field(default_factory=dict)

    # Cached param expressions (stable per template instance)
    _scalar_exprs: dict[str, ir.Scalar] = field(default_factory=dict, init=False)

    def scalar(
        self,
        name: str,
        dtype: Any,
        *,
        required: bool = True,
        default: Any = _MISSING,
    ) -> "QueryTemplate[TExpr]":
        spec = ScalarParam(name=name, dtype=dtype, required=required, default=default)
        self._scalar_specs[name] = spec
        self._scalar_exprs[name] = spec.make_expr()
        return self

    def list(
        self,
        name: str,
        dtype: Any,
        *,
        column: Optional[str] = None,
        required: bool = True,
        default: Any = _MISSING,
        dedupe: bool = True,
    ) -> "QueryTemplate[TExpr]":
        self._list_specs[name] = ListParam(
            name=name, dtype=dtype, column=column, required=required, default=default, dedupe=dedupe
        )
        return self

    def bind(self, **values: Any) -> "BoundQuery[TExpr]":
        return BoundQuery(template=self, values=dict(values))


@dataclass
class BoundQuery(Generic[TExpr]):
    """A template + concrete bound values. Stage list inputs, then compile/execute/export."""
    template: QueryTemplate[TExpr]
    values: dict[str, Any]
    instance_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])

    def _resolve_value(self, spec_name: str, spec: Any) -> Any:
        if spec_name in self.values:
            return self.values[spec_name]
        if getattr(spec, "default", _MISSING) is not _MISSING:
            return spec.default
        if getattr(spec, "required", False):
            raise KeyError(f"Missing required parameter: {spec_name}")
        return None

    def scalar_param_map(self) -> dict[ir.Scalar, Any]:
        out: dict[ir.Scalar, Any] = {}
        for name, spec in self.template._scalar_specs.items():
            v = self._resolve_value(name, spec)
            if v is None and spec.required:
                raise ValueError(f"Parameter {name} is required and cannot be None")
            out[self.template._scalar_exprs[name]] = v
        return out

    def stage_list_params(
        self,
        con: Any,
        *,
        temp: bool = True,
        overwrite: bool = True,
        database: Optional[str] = None,
    ) -> dict[str, str]:
        """Create temp tables for list params and return mapping name->table_name.

        Uses ibis.memtable(...) then con.create_table(..., temp=True/overwrite=True) to ensure
        everything is bound to the DuckDB backend session.
        """
        staged: dict[str, str] = {}

        for name, spec in self.template._list_specs.items():
            v = self._resolve_value(name, spec)

            if v is None:
                if spec.required:
                    raise ValueError(f"List parameter {name} is required and cannot be None")
                # Not bound: do not stage; builder can choose to apply or skip filtering.
                continue

            # Normalize list
            seq = list(v) if not isinstance(v, list) else v
            if spec.dedupe:
                # stable-ish dedupe; preserve order
                seen = set()
                seq2 = []
                for item in seq:
                    if item not in seen:
                        seen.add(item)
                        seq2.append(item)
                seq = seq2

            col = spec.col()
            tbl_name = f"{spec.temp_table_base(self.template.name)}_{self.instance_id}"
            staged[name] = tbl_name

            mt = ibis.memtable(
                {col: seq},
                schema=ibis.schema({col: spec.dtype}),
            )
            con.create_table(tbl_name, mt, temp=temp, overwrite=overwrite, database=database)

        return staged

    def ctx(self, con: Any, staged_lists: Mapping[str, str]) -> QueryCtx:
        return QueryCtx(
            con=con,
            p=ParamNamespace(self.template._scalar_exprs),
            l=ListNamespace(con, dict(staged_lists)),
            values=self.values,
        )

    def expr(self, con: Any, *, stage_lists: bool = True) -> TExpr:
        staged = self.stage_list_params(con) if stage_lists else {}
        return self.template.build(self.ctx(con, staged))

    # --- SQL / SQLGlot hooks -------------------------------------------------

    def compile_sql(
        self,
        con: Any,
        *,
        pretty: bool = True,
        limit: Optional[int] = None,
    ) -> str:
        """Compile to SQL for snapshot tests/debugging.

        Note: compile() requires the expression to be bound to a backend; unbound expressions raise.  # see Ibis docs
        """
        e = self.expr(con, stage_lists=True)
        return e.compile(limit=limit, params=self.scalar_param_map(), pretty=pretty)  # type: ignore[no-any-return]

    def try_sqlglot_ast(self, con: Any) -> Any:
        """Best-effort SQLGlot AST extraction (depends on backend/compiler internals)."""
        e = self.expr(con, stage_lists=True)
        compiler = getattr(con, "compiler", None)
        if compiler is None or not hasattr(compiler, "to_sqlglot"):
            raise RuntimeError("Backend compiler does not expose to_sqlglot() in this environment")
        return compiler.to_sqlglot(e)

    # --- Execution ----------------------------------------------------------

    def execute(
        self,
        con: Any,
        *,
        limit: Optional[int] = None,
        **kwargs: Any,
    ) -> Any:
        """Execute via backend.execute(expr, params=..., limit=...)."""
        e = self.expr(con, stage_lists=True)
        return con.execute(e, params=self.scalar_param_map(), limit=limit, **kwargs)

    # --- Convenience: “IN list” helper (small lists only) -------------------

    def isin_filter(self, col: ir.Value, values: Sequence[Any]) -> ir.BooleanValue:
        """Small-list convenience; for large lists prefer staging + semi_join."""
        return col.isin(values)  # type: ignore[no-any-return]
```

---

## Usage patterns (what this enables)

### A) Scalar-only “prepared style” template (compile/execute repeatedly)

Ibis compile/execute APIs are designed to accept `params` as a mapping from scalar parameter expressions to values. ([Ibis][1])

```python
import ibis

con = ibis.duckdb.connect()

qt = (
    QueryTemplate(
        name="functions_by_repo",
        build=lambda ctx: (
            ctx.con.table("docs_v_function_summary")
            .filter(ctx.con.table("docs_v_function_summary").repo == ctx.p.repo)
            .filter(ctx.con.table("docs_v_function_summary").cyclomaticity >= ctx.p.min_cyclo)
        ),
    )
    .scalar("repo", "string")
    .scalar("min_cyclo", "int64", required=False, default=0)
)

bq = qt.bind(repo="my_repo", min_cyclo=10)

sql_for_snapshot = bq.compile_sql(con, pretty=True)   # pretty SQL snapshots :contentReference[oaicite:4]{index=4}
df = bq.execute(con, limit=100)                       # params binding at execution :contentReference[oaicite:5]{index=5}
```

### B) List param via temp-table staging + semi-join (the “memtable-join” pattern)

For large `IN` filters, stage the list into DuckDB (temp) and do a semi-join:

```python
qt = (
    QueryTemplate(
        name="functions_by_goid",
        build=lambda ctx: (
            ctx.con.table("docs_v_function_summary")
            .semi_join(  # keeps only matching rows
                ctx.l.goids,
                ctx.con.table("docs_v_function_summary").goid_h128 == ctx.l.goids.goid_h128,
            )
        ),
    )
    .list("goids", "string", column="goid_h128")
)

bq = qt.bind(goids=["a", "b", "c"])
df = bq.execute(con)
```

### C) SQLGlot hook for lineage/rewrites (best-effort)

Ibis compilation infrastructure explicitly supports building SQLGlot-based expressions ([Ibis][2]), and Ibis compiles to SQLGlot under the hood (even for `Table.sql` / `Backend.sql` cases) ([Ibis][3]):

```python
ast = bq.try_sqlglot_ast(con)
# feed into SQLGlot lineage / transforms / .sql(dialect="duckdb")
```

### D) When you truly need DuckDB-native prepared statements

DuckDB supports `?`, `$1`, and `$param` parameter syntaxes in prepared statements ([DuckDB][4]), and its Python DB-API supports passing values separately to `execute` / `executemany` with `?` / `$1`. ([DuckDB][5])
If you ever choose to bypass Ibis for a “hot path,” those are the primitives to use (but you’ll lose Ibis’ typed expression construction).

---


[1]: https://ibis-project.org/reference/expression-tables "expression-tables – Ibis"
[2]: https://ibis-project.org/concepts/internals "internals – Ibis"
[3]: https://ibis-project.org/posts/does-ibis-understand-sql/ "Does Ibis understand SQL? – Ibis"
[4]: https://duckdb.org/docs/stable/sql/query_syntax/prepared_statements.html "Prepared Statements – DuckDB"
[5]: https://duckdb.org/docs/stable/clients/python/dbapi.html "Python DB API – DuckDB"


# **Ibis v11 ↔ SQLGlot interoperability playbook** #

Below is an **Ibis v11 ↔ SQLGlot interoperability playbook** for teams already fluent in both Ibis + DuckDB, but who want to explicitly leverage SQLGlot as the “compiler IR” and as an **AST toolchain** (transpile, rewrite, lineage, canonicalization, diffing, DDL generation).

---

## 1) Why SQLGlot is “central” in Ibis v11 (not optional plumbing)

### 1.1 SQLGlot is the compilation substrate

Ibis’s compiler infrastructure was generalized specifically to support **SQLGlot-based expressions**: compilation translates pieces of a SQL expression into “a string or SQLGlot expression”. ([Ibis][1])

### 1.2 SQLGlot unlocks cross-backend semantic alignment (a key reason Ibis switched)

In the Ibis maintainers’ discussion of the move to SQLGlot, a core motivation is that SQLGlot can translate between dialects such that queries remain **semantically equivalent**, and SQLGlot “transformations ensure that backend behaviors agree … even when doing so results in extremely complex SQL.” ([GitHub][2])
This is the “hidden superpower” for multi-backend parity tests and for enforcing a consistent semantic layer across engines.

### 1.3 SQLGlot version is effectively part of your query runtime

Ibis v11.0.0 declares `sqlglot>=23.4,!=26.32.0` as a core dependency. ([GitHub][3])
SQLGlot itself states that **MINOR releases can be backwards-incompatible**. ([PyPI][4])
Operationally: treat SQLGlot upgrades like compiler upgrades (pin + test), not like a “harmless transitive bump”.

---

## 2) The integration surface (what you can/should standardize on)

### 2.1 Ibis → SQLGlot AST (the “AST hook”)

Ibis’s blog explicitly shows that `con.compiler.to_sqlglot(expr)` yields the SQLGlot `Select(...)` tree, and that `Table.sql()` / `Backend.sql()` produce identical intermediate SQLGlot representations. ([Ibis][5])
This is the pivot point for **all** advanced workflows (rewrite, lineage, canonical SQL, diff).

**Canonical pattern**

```python
sg_expr = con.compiler.to_sqlglot(expr)   # SQLGlot Expression
sql = sg_expr.sql(dialect="duckdb", pretty=True)
```

(“pretty” and dialect rendering are standard SQLGlot capabilities; see §4.1.) ([PyPI][4])

### 2.2 SQL strings inside Ibis are still SQLGlot-backed (except raw_sql)

Ibis’s SQL article is explicit:

* Ibis doesn’t “understand” the internal structure of SQL passed into `Table.sql()` / `Backend.sql()`, **but it still compiles to SQLGlot expressions under the hood**, meaning SQLGlot capabilities (e.g. lineage) remain applicable. ([Ibis][5])
* `Backend.raw_sql()` is the **opaque** escape hatch: Ibis executes and returns a cursor; it does not attempt to understand the SQL. ([Ibis][5])

**Design implication:** if you want downstream tooling (lineage, validation, canonicalization), prefer `Table.sql`/`Backend.sql` over `raw_sql` whenever possible.

### 2.3 Cross-dialect SQL ingestion via `.sql(dialect=...)`

The “Using SQL strings with Ibis” guide shows you can pass SQL from a different dialect by supplying `dialect=...` to `.sql`, e.g. MySQL syntax executed against DuckDB. ([Ibis][6])
This is a direct, practical “SQLGlot transpile at the boundary” feature.

---

## 3) Schema + DDL round-trips (Ibis ↔ SQLGlot) — critical for contracts

This is the **most underused** but highest-leverage interop for teams building a semantic layer or dataset contracts.

### 3.1 SQLGlot Schema → Ibis Schema

`ibis.Schema.from_sqlglot(schema_expr, dialect=...)` constructs an Ibis schema from SQLGlot column definitions and supports dialect-aware type conversion (example includes NOT NULL). ([Ibis][7])

**Use cases**

* parse/ingest upstream DDL as SQLGlot, normalize types, and emit Ibis contracts
* unify “schema truth” into Ibis (typed) while still using SQLGlot for parsing

### 3.2 Ibis Schema → SQLGlot ColumnDef list (for CREATE TABLE)

`Schema.to_sqlglot_column_defs(dialect)` returns a list of SQLGlot `ColumnDef` nodes. ([Ibis][7])
The docs explicitly show embedding those defs into a SQLGlot `Create(...)` expression and rendering `CREATE TABLE ...` in DuckDB dialect. ([Ibis][7])

**Why this matters**

* you get **dialect-correct DDL** generated from your Ibis contract (including nullability → constraints)
* you can build migrations as SQLGlot ASTs (not fragile strings), and still render per engine

---

## 4) SQLGlot as an “AST toolbox” you can layer on top of Ibis

SQLGlot’s own docs (PyPI project description) lay out the toolbox very concretely:

### 4.1 Dialect-correct parsing + rendering + transpilation

* Most parsing failures come from omitting the **source dialect**: `parse_one(sql, dialect="spark")`; otherwise SQLGlot uses its “SQLGlot dialect” superset. ([PyPI][4])
* Likewise, generating SQL requires specifying the **target dialect**: `parse_one(...).sql(dialect="duckdb")` or `transpile(sql, read="spark", write="duckdb")`. ([PyPI][4])
* SQLGlot’s `transpile` examples show translation of DuckDB time functions and formats to another dialect. ([PyPI][4])

**How this complements Ibis**

* Ibis handles *relational intent*; SQLGlot handles *dialect edge-cases* and translation at the perimeter.
* Use `.sql(dialect=...)` in Ibis when migrating existing SQL into a DuckDB-backed semantic layer. ([Ibis][6])

### 4.2 Metadata extraction (tables/columns/projections) from SQL

SQLGlot exposes AST traversal helpers:

* `parse_one(...).find_all(exp.Column)` / `exp.Select` / `exp.Table` to enumerate columns, projections, tables. ([PyPI][4])

**Deployment pattern**

* For `Table.sql` / `Backend.sql` fragments (which Ibis treats as opaque), use SQLGlot’s AST walk to recover:

  * referenced physical tables (for access control / allowlists)
  * selected columns (for policy + minimization)
  * joins and filters (for query safety checks)

### 4.3 Rewrite and transform ASTs (policy injection, normalization, conventions)

SQLGlot supports:

* incremental building (`select(...).from_(...).where(...)`) and
* AST mutation, including recursive `.transform(transformer)` with pattern-based node replacement. ([PyPI][4])

**High-value interop uses**

* enforce quoting conventions / identifier casing
* inject tenant/repo scoping predicates
* rewrite function names / argument order for DuckDB compatibility
* normalize “legacy SQL” fragments before embedding them into Ibis

### 4.4 Canonicalization / query fingerprinting via optimizer

SQLGlot can rewrite queries into an optimized canonical AST, and this canonical AST can be used to “standardize queries”. ([PyPI][4])
The optimizer pipeline heavily relies on `qualify` (qualify/normalize/quote identifiers), and SQLGlot’s own docs warn that qualification is necessary for further optimizations. ([sqlglot.com][8])

**Extremely practical:** use SQLGlot `optimize(...)` as your “semantic fingerprint” stage before caching/keys. Provide schema for best results (see next).

### 4.5 Type-/schema-sensitive rewrites (and why Ibis is an advantage here)

SQLGlot notes that some transpilation transformations are type-sensitive and require schema information; `qualify` and `annotate_types` can help, but are not default because of overhead. ([PyPI][4])

**Interop win:** Ibis already has typed schemas. If you standardize on:

* `Ibis Schema` ↔ `SQLGlot Schema` conversion (see §3),
  you can supply schema to SQLGlot for higher-fidelity rewrites/transpiles.

### 4.6 Semantic diffs (structural changes, not string diffs)

SQLGlot provides `diff(parse_one(sql1), parse_one(sql2))` to compute a semantic diff as a list of edit actions. ([PyPI][4])

**Use in Ibis workflows**

* when changing compiler versions (SQLGlot bump), diff the SQLGlot AST from the same Ibis expression across versions
* use AST diff to understand “what changed” without being fooled by formatting

### 4.7 Custom dialects and targeted generator overrides

SQLGlot dialects are implemented as `Dialect` subclasses that extend tokenizer/parser/generator. ([sqlglot.com][9])
The docs show how to define a custom dialect by customizing quoting rules, keywords, generator transforms, and type mappings. ([sqlglot.com][9])

**Interop angle**

* You rarely need a fully custom dialect for DuckDB, but this is a real escape hatch for:

  * “house SQL” (internal macros / nonstandard types)
  * nonstandard identifier quoting rules
  * deterministic type spellings for DDL generation

---

## 5) Column-level lineage: Ibis expression → SQLGlot lineage graph

SQLGlot’s lineage API builds a lineage graph for a column of a SQL query and accepts:

* `sql` as a string **or SQLGlot Expression**
* optional `schema`
* optional `sources` mapping (to expand referenced queries)
* `dialect` for parsing/normalization ([sqlglot.com][10])

**How to deploy with Ibis**

1. Compile Ibis expression to SQLGlot AST (`con.compiler.to_sqlglot(expr)`) ([Ibis][5])
2. Render SQL if needed, but lineage can operate on the expression directly ([sqlglot.com][10])
3. Provide schema (best effort) using your Ibis contracts (convert via §3, or provide mapping)

This is the cleanest path to OpenLineage-style column lineage for Ibis-generated queries.

---

## 6) Versioning + stability strategy (what to lock down)

* Ibis v11.0.0 requires `sqlglot>=23.4,!=26.32.0`. ([GitHub][3])
* SQLGlot’s MINOR versions may be backwards-incompatible. ([PyPI][4])
* Ibis maintainers explicitly anticipated snapshot-test concerns when SQLGlot versions shift. ([GitHub][2])

**Recommended “compiler discipline”**

* Pin SQLGlot (you’re on 28.4.1; good) and treat bumps as compiler upgrades
* Keep:

  * golden SQL snapshots (`expr.compile(pretty=True)` or SQLGlot `.sql(pretty=True)`)
  * AST-level regression tests (`con.compiler.to_sqlglot(expr)` shape checks) ([Ibis][5])
  * execution validation on DuckDB

---

If you want, I can take the `QueryTemplate` utility you now have and extend it with a **SQLGlotToolchain** mixin that standardizes:

* `ibis_expr → sqlglot_ast → (qualify/optimize/diff/lineage) → duckdb_sql`
* schema plumbing (`Ibis Schema ↔ SQLGlot Schema`)
* a canonical “query fingerprint” (optimized AST serialization) for caching + snapshotting

[1]: https://ibis-project.org/concepts/internals "internals – Ibis"
[2]: https://github.com/ibis-project/ibis/discussions/7213 "Moving our SQL backends to sqlglot · ibis-project ibis · Discussion #7213 · GitHub"
[3]: https://raw.githubusercontent.com/ibis-project/ibis/master/pyproject.toml "raw.githubusercontent.com"
[4]: https://pypi.org/project/sqlglot/ "sqlglot · PyPI"
[5]: https://ibis-project.org/posts/does-ibis-understand-sql/ "Does Ibis understand SQL? – Ibis"
[6]: https://ibis-project.org/how-to/extending/sql "sql – Ibis"
[7]: https://ibis-project.org/reference/schemas "schemas – Ibis"
[8]: https://sqlglot.com/sqlglot/optimizer/qualify.html "sqlglot.optimizer.qualify API documentation"
[9]: https://sqlglot.com/sqlglot/dialects.html "sqlglot.dialects API documentation"
[10]: https://sqlglot.com/sqlglot/lineage.html "sqlglot.lineage API documentation"
