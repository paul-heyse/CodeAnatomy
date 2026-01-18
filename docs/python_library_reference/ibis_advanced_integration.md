# `ibis-advanced.md` — Ibis as your relational IR + DataFusion backend + SQLGlot boundary (streaming-first)

> This guide targets **Ibis `ibis-framework` (latest: 11.0.0, Oct 15 2025)**. ([PyPI][1])
> Focus: the advanced features that matter for your codebase’s “rules → relational IR → DataFusion execution → Arrow/Parquet artifacts” architecture.

---

## 0) Setup, packaging pitfalls, and “what counts as Ibis” in your environment

### 0.1 `ibis-framework` vs `ibis` on PyPI (do not mix)

Ibis explicitly warns that `ibis-framework` is **not** the same as the `ibis` PyPI package, and they **cannot coexist** because both import as `ibis`. ([Ibis][2])

**Practical rule**

* In your `uv` / requirements: install only `ibis-framework[...]` (not `ibis`).

### 0.2 Installing DataFusion backend

Install with the DataFusion extra:

```bash
pip install 'ibis-framework[datafusion]'
```

([Ibis][3])

---

## 1) Mental model: expressions, compilation, execution

### 1.1 Expression = typed IR (not SQL text)

Ibis’s internals flow is: user builds an expression tree (type-checked as you build), applies backend-specific rewrites, compiles, then executes (SQL backends execute SQL; Polars backend is different). ([Ibis][4])

Key internal invariants you can depend on:

* **Expressions are immutable** (each operation returns a new expression).
* Expressions are **typed and shape-checked** (table/column/scalar) during construction. ([Ibis][4])

### 1.2 Unbound tables = “write once, execute later”

An **unbound table** has a schema but no data source; you can build transformations without connecting to any backend. ([Ibis][5])

Minimal pattern:

```python
import ibis

t = ibis.table(dict(path="string", line="int64", text="string"), name="files")
expr = t.filter(t.line > 0).select(path=t.path, text=t.text)
```

Then, when you have a backend connection, you bind/execute (see §9 for recipes). The “write once, execute everywhere” framing is explicit in Ibis docs. ([Ibis][5])

### 1.3 Visualizing the IR (debug artifacts)

Ibis supports visualizing expressions as GraphViz graphs (useful as a plan artifact in your build bundles). The “Does Ibis understand SQL?” post demonstrates `to_graph(expr)` and shows that Ibis expressions are inherently a tree of `Expr` and `Node` objects. ([Ibis][6])

---

## 2) DataFusion backend deep dive (connection lifecycle + table hierarchy + IO)

### 2.1 Connection creation: `ibis.datafusion.connect`

`ibis.datafusion.connect` is a wrapper over `Backend.do_connect`. The DataFusion backend `do_connect(config=None)` accepts either: ([Ibis][3])

* `Mapping[str, str|Path]` mapping table name → file path (**deprecated in 10.0**), or
* an existing **DataFusion `SessionContext`** instance.

The docs include a concrete example of passing a `SessionContext` with an in-memory table and then `con.list_tables()` returning it. ([Ibis][3])

**Best-in-class pattern for your repo**

* Own the DataFusion `SessionContext` (with your runtime profile), then create Ibis backend via `ibis.datafusion.connect(ctx)` so both layers share the same engine state. ([Ibis][3])

### 2.2 Backend table hierarchy: catalog / database / table

Ibis standardizes naming: ([Ibis][7])

* `database` = collection of tables
* `catalog` = collection of databases
* Fully qualified: `catalog.database.table` or `database.table`

This shows up in backend APIs (e.g., `create_catalog`, `create_database`, `list_tables(database=...)`, etc.) and in UDF placement (catalog/database). ([Ibis][7])

### 2.3 DataFusion backend API surface you will actually use

From the DataFusion backend reference, the key methods for your system are: ([Ibis][3])

* **Introspection**: `list_tables`, `list_databases`, `list_catalogs`, `get_schema`
* **DDL-ish**: `create_catalog`, `create_database`, `create_table`, `create_view`, `drop_*`, `rename_table`
* **Ingest / register sources**: `read_parquet`, `read_csv`, `read_json`, `read_delta`
* **SQL surfaces**: `sql`, `raw_sql`
* **Execution / export**: `execute`, `to_pyarrow`, `to_pyarrow_batches`, `to_parquet`, `to_parquet_dir`, etc.
* **Support probing**: `has_operation(operation)` (critical when you’re writing portable rulepacks). ([Ibis][8])

### 2.4 Creating tables/views in DataFusion via Ibis

`create_table` is the bridge that lets you move between “Python objects / Arrow objects” and named tables in the DataFusion catalog. It supports populating from many object types (pandas, pyarrow, polars, ibis expression, etc.). ([Ibis][8])

`create_view` creates a view from an Ibis expression and returns an Ibis table expression. ([Ibis][8])

**Why this matters for you**

* It’s the proper modern replacement for “named memtables” (see §8.3). GitHub release notes for 11.0.0 explicitly state that memtables can no longer be named explicitly; use `create_table` or `create_view` instead. ([GitHub][9])

---

## 3) Execution & export surfaces (streaming-first)

### 3.1 “Execute” vs “Export” — don’t conflate them

For backends like DataFusion, execution typically returns pandas objects (DataFrame/Series/scalar) via `execute`. ([Ibis][3])
For large artifacts, prefer **export surfaces** (Arrow batches / dataset writers).

### 3.2 `to_pyarrow_batches`: the batch/stream boundary

Ibis table expressions expose `to_pyarrow_batches(limit=None, params=None, chunk_size=...)` which **executes eagerly** and returns a **`pyarrow.RecordBatchReader`**. ([Ibis][10])

DataFusion backend’s implementation advertises the same semantics: eager execution and record batches, with a `chunk_size` row bound. ([Ibis][3])

**Two practical constraints you must design around**

1. The API allows `chunk_size`, but in general it may not be respected uniformly across backends (“may have no effect depending on the backend”). ([Ibis][3])
2. `RecordBatchReader` is an iterator + schema carrier; it’s the right thing to pipe into parquet dataset writers. ([Apache Arrow][11])

**Canonical “stream to parquet dataset” pattern**

```python
import ibis
import pyarrow.dataset as ds

con = ibis.connect("datafusion://")
t = con.read_parquet("edges.parquet", table_name="edges")

reader = t.filter(t.kind == "CALL").to_pyarrow_batches(chunk_size=250_000)

ds.write_dataset(
    reader,
    base_dir="out/edges_filtered",
    format="parquet",
    # plus your writer policies: partitioning, existing_data_behavior, file_visitor, etc.
)
```

The key is that `to_pyarrow_batches` yields a `RecordBatchReader`. ([Ibis][12])

### 3.3 Other export surfaces you should treat as “supported sinks”

From table expression APIs: ([Ibis][10])

* `to_pandas()` and `to_pandas_batches()`
* `to_pyarrow()` (materializes a Table/Array/Scalar depending on shape)
* `to_polars()`
* `to_torch()` (dictionary of tensors)
* file sinks: `to_csv`, `to_json`, `to_delta`, `to_parquet`, `to_parquet_dir`

For your system, the “best-in-class” flow is:

* **IR (Ibis)** → **batches** → **Arrow-native writer** (`pyarrow.dataset.write_dataset`) via `to_parquet_dir` or your own materializer.

### 3.4 `to_parquet_dir`: the built-in “dataset writer”

`to_parquet_dir(directory, params=None, **kwargs)` writes results to a directory and forwards `**kwargs` to `pyarrow.dataset.write_dataset`. ([Ibis][10])

If you want to standardize on `pyarrow.dataset.write_dataset` for all outputs (including custom file visitors / metadata sidecars), `to_parquet_dir` is the canonical Ibis hook.

### 3.5 `to_parquet` partitioning caveat you must not miss

`to_parquet` includes examples showing partitioning by columns. It also explicitly notes: **“Hive-partitioned output is currently only supported when using DuckDB.”** ([Ibis][10])

Implication for your DataFusion-centric system:

* Prefer **`to_parquet_dir`** (Arrow dataset write) for partitioned datasets, unless you are intentionally delegating writes to a specific backend.

---

## 4) SQL surfaces in Ibis: Table.sql, Backend.sql, Backend.raw_sql

Ibis supports mixing SQL and dataframe-style Ibis expressions (and showcases this on the homepage). ([Ibis][13])
But there are three different levels, and they behave differently.

### 4.1 `Table.sql`: “SQL view over an expression”

`Table.sql` runs arbitrary `SELECT` statements against a table expression and returns a new Ibis expression, designed for composition with further Ibis transforms. ([Ibis][14])

It also supports parsing SQL written in a **different dialect** (useful for porting): `dialect="mysql"` shown in docs. ([Ibis][14])

**Composition pattern**

* SQL → `.filter(...)` / `.order_by(...)` / `.alias(...)` → more SQL via `.sql(...)` on the alias. ([Ibis][14])

### 4.2 `Backend.sql`: “SQL query result as an Ibis table”

`Backend.sql(query, schema=None, dialect=None)` creates an Ibis table expression from a SQL query; unlike `Table.sql`, it can only refer to tables that already exist in the backend (because it’s defined on the connection). ([Ibis][14])

If you don’t provide a schema, Ibis will try to infer it. ([Ibis][3])
For your system: prefer providing schemas for “contracted outputs” (to avoid inference drift).

### 4.3 `Backend.raw_sql`: “escape hatch”

`Backend.raw_sql` is for arbitrary SQL (e.g., backend-specific DDL). It returns the underlying cursor and comes with a hard warning: **close the cursor** to avoid resource leaks; easiest via `contextlib.closing`. ([Ibis][14])

Your system should therefore treat raw SQL as:

* allowed only in carefully controlled internal code paths, and
* always wrapped in a context manager unless you know it’s a non-result DDL. ([Ibis][14])

### 4.4 DataFusion backend adds an extra “power-user” twist: `raw_sql` accepts SQLGlot AST

On the DataFusion backend, `raw_sql(self, query)` accepts `str | sge.Expression` (a SQLGlot expression). ([Ibis][3])

That’s a uniquely powerful integration point for your architecture:

* you can build/transform SQLGlot ASTs (qualify, normalize, etc.), then pass the AST directly to execute.

---

## 5) SQLGlot boundary: treating Ibis as a compiler front-end

### 5.1 Ibis compiles to SQLGlot expressions under the hood

Ibis’s internals explicitly state that compiler infrastructure was generalized to support **SQLGlot-based expressions**, and compilers translate pieces into a string **or SQLGlot expression**. ([Ibis][4])

The “Does Ibis understand SQL?” post is even more explicit: since Ibis 9.0’s refactor, Ibis transitioned to producing SQLGlot expressions under the hood, and you can inspect the intermediate SQLGlot representation via `compiler.to_sqlglot(expr)` / `con.compiler.to_sqlglot(expr)`. ([Ibis][6])

### 5.2 Practical checkpointing pattern (what you standardize)

For any rulepack expression `expr`:

1. **Ibis IR checkpoint** (repr / GraphViz)
2. **SQLGlot AST checkpoint**:

   * `con.compiler.to_sqlglot(expr)` (semi-internal but documented in the blog post) ([Ibis][6])
3. Apply SQLGlot rewrites in a controlled “policy lane” (your own code; see your SQLGlot guide)
4. Render SQL for DataFusion dialect **or** execute via `raw_sql(sge.Expression)` for DF backend. ([Ibis][3])

Even if you don’t rewrite the SQLGlot AST initially, persisting the AST makes:

* semantic diff,
* lineage extraction,
* deterministic plan fingerprinting,
  natural.

### 5.3 What happens when you use `Table.sql` / `Backend.sql`

Important nuance from the “Does Ibis understand SQL?” post:

* When you pass SQL strings into Ibis (`Table.sql` / `Backend.sql`), Ibis wraps them in special nodes (`SQLStringView` / `SQLQueryResult`), so Ibis does **not** understand the internal structure of that SQL—it mainly knows the output schema for downstream validation. ([Ibis][6])

But: SQLGlot still compiles/represents them under the hood, and the blog explicitly notes SQLGlot capabilities (like lineage) can still apply. ([Ibis][6])

**Architecture implication**

* Prefer Ibis-native expression building for rules you want the optimizer to “see”.
* Allow `Table.sql`/`Backend.sql` as controlled escape hatches (but treat them as “opaque-ish” in terms of IR detail).

---

## 6) Options & configuration control plane (what you version)

### 6.1 SQL-related options

`ibis.options.sql` exposes: ([Ibis][15])

* `fuse_selects`: fuse consecutive selects where possible
* `default_limit`: implicit limit for expressions with no limit (`None` means no limit)
* `default_dialect`: dialect used for printing SQL when backend cannot be determined

**Why `fuse_selects` matters**

* It changes SQL shape (subqueries vs fused projections), which can materially affect DataFusion optimizer behavior and thus performance.

### 6.2 Interactive mode vs non-interactive (debug ergonomics)

Ibis frequently demonstrates setting `ibis.options.interactive = True` to get rich table output in notebooks. ([Ibis][14])
For your production build system, keep interactive off; for developer workflows, on.

---

## 7) UDF system: performance ladder + backend escape hatches

### 7.1 Scalar UDF constructors (the ladder)

Ibis defines four scalar UDF APIs: ([Ibis][16])

* `@ibis.udf.scalar.builtin` — reference a backend built-in function
* `@ibis.udf.scalar.pandas` — vectorized over pandas Series
* `@ibis.udf.scalar.pyarrow` — vectorized over PyArrow Arrays
* `@ibis.udf.scalar.python` — scalar (row-by-row), explicitly warned as slow

The docs warn: `python` UDFs are not vectorized and are “likely to be slow” due to per-row Python calls. ([Ibis][17])

**For your codebase**

* Default to: **builtin → pyarrow → pandas → python**.

### 7.2 Signature control: annotations vs explicit `signature=`

Scalar UDFs accept `signature`, a tuple `((arg0type, ...), returntype)`; if omitted, signature can be derived from type annotations. ([Ibis][16])

This matters if you want deterministic, machine-readable UDF catalogs for agents:

* standardize either “always annotate types” or “always provide explicit signature”.

### 7.3 Catalog/database placement for UDFs

Scalar and aggregate builtin UDF decorators accept `database` and `catalog` parameters, consistent with Ibis’s hierarchy model. ([Ibis][16])
Release notes also call out the newer preference for `catalog` terminology in UDF definition APIs. ([Ibis][18])

### 7.4 Builtin UDFs as an API escape hatch

Ibis explicitly positions builtin UDFs as the escape hatch when Ibis doesn’t expose a specific backend function in the public API. ([Ibis][19])

### 7.5 Aggregate UDFs are “builtin-only” (experimental)

Ibis’s “Aggregate UDFs (experimental)” reference currently exposes builtin aggregate UDFs via `@ibis.udf.agg.builtin` (no custom aggregate UDF definition API in that reference page). ([Ibis][20])

### 7.6 Advanced pattern: dynamic rewrite around UDFs

Ibis has a dedicated blog post demonstrating **dynamic UDF rewriting with predicate pushdowns**, motivated by the fact that opaque UDFs can block optimizer transforms like pushdown. ([Ibis][21])

For your architecture, that’s a conceptual tool:

* define “semantic UDF nodes” at the Ibis layer,
* rewrite them into pushdown-friendly plans when filters/constraints permit.

---

## 8) Schema & types as contracts (SQLGlot ↔ Ibis ↔ Arrow)

### 8.1 `ibis.schema` and the `Schema` type

The `Schemas` reference documents how to build schemas and encode nullability (e.g., `"!string"` means non-null string). ([Ibis][22])

### 8.2 Schema ↔ SQLGlot interop (critical for contract-driven DDL)

Ibis Schema supports: ([Ibis][22])

* `Schema.from_sqlglot(schema_expr, dialect=None)` — parse SQLGlot schema into Ibis schema (dialect-aware type conversion)
* `Schema.to_sqlglot_column_defs(dialect)` — convert schema into SQLGlot `ColumnDef` nodes (explicitly recommended; `to_sqlglot` is deprecated)

The docs include a full example embedding `to_sqlglot_column_defs` output into a SQLGlot `CREATE TABLE` expression and rendering dialect SQL. ([Ibis][22])

**This is a huge lever for your system**

* Your dataset contracts can emit **SQLGlot DDL AST** deterministically, not stringly-typed DDL.

### 8.3 Arrow as the interchange layer

Ibis explicitly states it uses Apache Arrow for efficient data transfer and that Ibis tables implement `__dataframe__` and `__array__` protocols. It demonstrates `to_pyarrow()`, `to_pyarrow_batches()`, and converting between pandas/polars/pyarrow via Arrow. ([Ibis][23])

---

## 9) Integration recipes you should standardize in your repo

### 9.1 One execution substrate: DataFusion `SessionContext` owned by you

* Create/configure DataFusion `SessionContext` (your runtime profile).
* Pass it into Ibis DataFusion backend: `ibis.datafusion.connect(ctx)`. ([Ibis][3])

This ensures:

* single place to set DataFusion config (spill, caches, join prefs),
* consistent table registrations and object stores,
* consistent plan artifacts.

### 9.2 “Rulepack IR → streaming outputs”

Preferred pipeline:

1. Build Ibis expression (`expr`) for a ruleset output
2. Export as record batches: `expr.to_pyarrow_batches(chunk_size=...)` ([Ibis][10])
3. Write via Arrow dataset writer (either Ibis `to_parquet_dir` or your own `pyarrow.dataset.write_dataset` materializer) ([Ibis][10])

### 9.3 Deterministic SQL shape artifacts

Persist at least:

* `ibis.to_sql(expr)` for the human-readable SQL (shown as a first-class feature on Ibis homepage) ([Ibis][13])
* SQLGlot AST checkpoint: `con.compiler.to_sqlglot(expr)` (as in the Ibis blog post) ([Ibis][6])
* Ibis expression graph/IR (`repr` / GraphViz)

### 9.4 SQL escape hatches with explicit safety patterns

* `Backend.sql` / `Table.sql` for SELECT-only integration; keep dialect explicit when porting SQL. ([Ibis][14])
* `raw_sql` only for “must do DDL or backend-specific statement”; always close cursor for SELECT-like results via `contextlib.closing`. ([Ibis][14])
* For DataFusion backend specifically: prefer `raw_sql(sge.Expression)` if you are already operating in SQLGlot AST space. ([Ibis][3])

### 9.5 Operation support probing (`has_operation`)

Before committing to a specific Ibis operation in a ruleset that must run across multiple backends, use `Backend.has_operation(operation)` to branch or fail fast. ([Ibis][8])

---

## 10) Failure modes and “don’t step on rakes” playbook

### 10.1 Cursor leaks from `raw_sql`

Docs explicitly warn that failure to close the cursor returned by `raw_sql` can cause errors and hard-to-debug behavior; use a context manager. ([Ibis][14])

### 10.2 `chunk_size` expectations

`to_*_batches(chunk_size=...)` exposes chunk sizing, but documentation warns that chunk sizing “may have no effect depending on the backend.” ([Ibis][3])
Design your downstream writer to handle variable batch sizes.

### 10.3 Partitioned parquet writing expectations

`to_parquet` demonstrates partitioning but also notes Hive-partitioned output is only supported for DuckDB. ([Ibis][10])
If you need deterministic partitioned outputs on DataFusion backend, prefer `to_parquet_dir` or explicit Arrow dataset writes. ([Ibis][10])

### 10.4 SQL string views are “less transparent” IR

When using `Table.sql` / `Backend.sql`, the expression tree wraps SQL in nodes like `SQLStringView` / `SQLQueryResult`, and Ibis mostly uses the output schema to validate downstream ops (less internal structure for analysis). ([Ibis][6])
Treat these as controlled “opaque segments” in your IR.

### 10.5 Named memtables changed in 11.0.0

Ibis 11.0.0 breaking changes include: “memtables can no longer be named explicitly; use `create_table` or `create_view` to create a named object instead.” ([GitHub][9])
If your codebase had “named memtable” patterns, migrate them.

---

## Appendix A) Minimal “DataFusion-backed Ibis substrate” skeleton

```python
from __future__ import annotations

from datafusion import SessionContext
import ibis

def build_ibis_df_backend(ctx: SessionContext) -> ibis.backends.datafusion.Backend:
    # ctx is configured elsewhere with your DataFusion runtime profile
    con = ibis.datafusion.connect(ctx)
    return con

def stream_expr_to_dataset(expr: ibis.expr.types.Table, out_dir: str) -> None:
    import pyarrow.dataset as ds

    reader = expr.to_pyarrow_batches(chunk_size=250_000)
    ds.write_dataset(reader, base_dir=out_dir, format="parquet")
```

The `SessionContext → ibis.datafusion.connect(ctx)` path is documented and is the key for unifying your execution substrate. ([Ibis][3])
The `to_pyarrow_batches → RecordBatchReader` contract is documented on the table expression API. ([Ibis][12])

---


[1]: https://pypi.org/project/ibis-framework/?utm_source=chatgpt.com "ibis-framework"
[2]: https://ibis-project.org/install?utm_source=chatgpt.com "install – Ibis"
[3]: https://ibis-project.org/backends/datafusion "datafusion – Ibis"
[4]: https://ibis-project.org/concepts/internals "internals – Ibis"
[5]: https://ibis-project.org/how-to/extending/unbound_expression "Write and execute unbound expressions – Ibis"
[6]: https://ibis-project.org/posts/does-ibis-understand-sql/ "Does Ibis understand SQL? – Ibis"
[7]: https://ibis-project.org/concepts/backend-table-hierarchy "Backend Table Hierarchy – Ibis"
[8]: https://ibis-project.org/backends/datafusion?utm_source=chatgpt.com "DataFusion - Ibis"
[9]: https://github.com/ibis-project/ibis/releases?utm_source=chatgpt.com "Releases · ibis-project/ibis"
[10]: https://ibis-project.org/reference/expression-tables "expression-tables – Ibis"
[11]: https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatchReader.html?utm_source=chatgpt.com "pyarrow.RecordBatchReader — Apache Arrow v22.0.0"
[12]: https://ibis-project.org/reference/expression-tables?utm_source=chatgpt.com "Table expressions - Ibis"
[13]: https://ibis-project.org/ "Ibis"
[14]: https://ibis-project.org/how-to/extending/sql "sql – Ibis"
[15]: https://ibis-project.org/reference/sql "sql – Ibis"
[16]: https://ibis-project.org/reference/scalar-udfs "scalar-udfs – Ibis"
[17]: https://ibis-project.org/reference/operations "operations – Ibis"
[18]: https://ibis-project.org/release_notes?utm_source=chatgpt.com "Release notes - Ibis"
[19]: https://ibis-project.org/how-to/extending/builtin?utm_source=chatgpt.com "Reference built-in functions - Ibis"
[20]: https://ibis-project.org/reference/aggregate-udfs?utm_source=chatgpt.com "Aggregate UDFs (experimental) - Ibis"
[21]: https://ibis-project.org/posts/udf-rewriting/?utm_source=chatgpt.com "Dynamic UDF Rewriting with Predicate Pushdowns - Ibis"
[22]: https://ibis-project.org/reference/schemas "schemas – Ibis"
[23]: https://ibis-project.org/how-to/input-output/basics "basics – Ibis"

Below is the **follow-on Ibis content** that fills in the major advanced capabilities you’ll want for your codebase (rules → relational IR → DataFusion execution → Arrow/Parquet artifacts). This is written to *append* to the prior `ibis-advanced.md` content.

---

## 11) Expression construction accelerators: Deferred (`_`) + selectors + `Table.bind`

### 11.1 The deferred (“underscore”) API for composable rule macros

Ibis supports a deferred expression API (imported as `from ibis import _`) that lets you build column expressions *without* explicitly referencing the current table variable, and chain transformations while `_` rebinds at each step. ([Ibis][1])

Pattern:

```python
from ibis import _
import ibis

# Build reusable deferred expressions
xmod = (_.x % 3)
ymax = _.y.max()

expr = (
    t1
    .join(t2, _.x == t2.x)        # _ is t1 in this call
    .mutate(xmod=xmod)            # _ now refers to post-join table
    .group_by(_.xmod)
    .aggregate(ymax=ymax)
)
```

This is one of the cleanest ways to express “rule templates” that operate over tables with consistent schemas. ([Ibis][1])

### 11.2 Column selectors (`ibis.selectors`) for wide-table rulepacks

Selectors are first-class “column selection expressions” for building wide-table transforms without manually enumerating columns. The selectors module includes composable selectors like `numeric()`, `of_type()`, `contains()`, `matches()`, boolean composition (`|`, `&`), and “apply across many columns” via `across()`. ([Ibis][2])

Key APIs (high-leverage for your repo):

* `s.numeric()`, `s.of_type(dtype)`, `s.contains(needles)`, `s.matches(regex)`, `s.startswith(prefixes)`, `s.endswith(suffixes)` ([Ibis][2])
* `s.across(selector, func, names=...)` to apply transformations across many columns with systematic renaming. ([Ibis][2])

Example: center all numeric columns and name them `{fn}_{col}`:

```python
from ibis import _, selectors as s

expr = t.mutate(
    s.across(s.numeric(), dict(centered=_ - _.mean()), names="{fn}_{col}")
)
```

([Ibis][2])

### 11.3 `Table.bind`: unify strings / ints / Deferred / selectors into real expressions

`Table.bind(*args, **kwargs)` binds “column-like values” to a table expression, and it explicitly supports **strings, integers, deferred expressions, and selectors**. ([Ibis][3])

Why you care: it’s the internal mechanism that makes patterns like “accept either a column name, a selector, or a deferred expression” viable in your own DSL layer.

---

## 12) Parameterization as a first-class rulepack feature: `ibis.param` + `params=...`

### 12.1 Scalar parameters (`ibis.param`) and dynamic substitution at execution time

Ibis supports scalar parameters that you supply dynamically during execution. The docs show:

* `p = ibis.param("string")`
* `expr.execute(params={p: "Gentoo"})` ([Ibis][4])

Example pattern:

```python
import ibis

kind = ibis.param("string")
expr = edges.filter(edges.kind == kind)

# Execute with different parameter values without rebuilding IR:
expr.to_pyarrow_batches(params={kind: "CALL"})
expr.to_pyarrow_batches(params={kind: "IMPORT"})
```

`ibis.param(type)` creates a “deferred parameter” scalar of a given type, and the docs show repeated executions with different values. ([Ibis][4])

### 12.2 Where `params` is accepted

* `Table.compile(limit=None, params=None, pretty=False)` accepts `params` mapping. ([Ibis][3])
* `Table.to_sql(..., **kwargs)` treats `kwargs` as scalar parameters. ([Ibis][3])
* Backend execution/export methods (including DataFusion backend) accept `params` for many sinks (e.g., `execute`, `to_pandas`, `to_parquet`, etc.). ([Ibis][5])

**Rulepack implication:** you can keep a single canonical IR and drive multiple outputs / diagnostics runs by parameterizing “mode flags” (strictness tier, rule kind, evidence sources, etc.).

---

## 13) Backend caching (`Table.cache`) for expensive intermediate results

### 13.1 What `.cache()` actually guarantees

`Table.cache()` **eagerly evaluates** the expression, stores it, and returns a cached table where **subsequent operations use cached data**. The cached table’s lifetime is tied to Python references (GC), or you can use a `with` statement / `.release()` for explicit control. It is **idempotent** (repeated `.cache()` calls return the same result). ([Ibis][3])

Minimal pattern:

```python
cached = expensive_expr.cache()
downstream = cached.join(other, "key").filter(...)
```

Explicit cleanup pattern:

```python
with expensive_expr.cache() as cached:
    out = cached.group_by(...).aggregate(...)
```

([Ibis][3])

### 13.2 When caching is architecturally “correct”

Use caching when you have:

* one expensive intermediate used by many downstream products (common in your “edges/nodes/props derived from shared scans” world),
* or when you need stable debugging snapshots mid-pipeline.

Avoid caching when:

* the intermediate is huge and you can stream directly to output,
* or when you want “single-pass streaming” (DataFusion/Arrow C Stream lane).

---

## 14) Materialization primitives in the DataFusion backend: `create_table`, `create_view`, `insert`

### 14.1 `create_table` as the universal “register this dataset” API

DataFusion backend `create_table(name, obj=None, *, schema=None, database=None, temp=False, overwrite=False)` supports populating from many in-memory formats:

* `pd.DataFrame`
* `pyarrow.Table`
* `pyarrow.RecordBatchReader` / `RecordBatch`
* `polars.DataFrame` / `LazyFrame`
* or another Ibis table expression
  …and requires at least one of `obj` or `schema`.

This is one of your best “execution substrate” tools because it lets you:

* register streaming-produced Arrow outputs as named tables (when you *must* re-query them),
* keep naming, lifecycle, and overwrite semantics explicit (`temp`, `overwrite`).

### 14.2 `create_view` for IR-level reuse without data duplication

`create_view(name, obj, *, database=None, overwrite=False)` creates a view from an Ibis expression and returns a table expression representing the view. ([Ibis][5])

### 14.3 `insert` for “persist the result of a rulepack into a table”

DataFusion backend:
`insert(name, obj, *, database=None, overwrite=False)` where `obj` can be a pandas DataFrame, an Ibis table expression, or even list/dict source data. ([Ibis][5])

In the “unbound expressions” guide, Ibis demonstrates creating a table and inserting results:

* `con.create_table("results", schema=output_schema)`
* `con.insert("results", expr)` ([Ibis][6])

**Design rule for your repo:** treat `insert` as the “stable materialization into a backend-managed namespace” counterpart to “write parquet dataset”.

---

## 15) Compilation and SQL emission: `Table.compile`, backend `compile`, and `to_sql`

### 15.1 `Table.compile`: compilation requires a backend (or a default backend)

`Table.compile(limit=None, params=None, pretty=False)` compiles to:

* SQL string for SQL backends (DataFusion is SQL backend),
* or a LazyFrame for Polars backend. ([Ibis][3])

If the expression has no backend and no default backend, compilation raises an error (documented). ([Ibis][3])

### 15.2 DataFusion backend `compile` mirrors the same knobs

`datafusion.Backend.compile(expr, *, limit=None, params=None, pretty=False)` compiles an expression to SQL with parameter substitution and pretty formatting.

### 15.3 `to_sql` is “formatted SQL”, parameterized via kwargs

`Table.to_sql(dialect=None, pretty=True, **kwargs)` compiles to formatted SQL and uses:

* dialect bound to expression/backend if not specified,
* and treats `kwargs` as scalar parameters. ([Ibis][3])

**Architectural use:** `compile`/`to_sql` outputs are perfect “human-friendly plan artifacts,” but for deterministic compiler tooling you generally persist the SQLGlot AST checkpoint (from your earlier guide) and only render SQL at the last moment.

---

## 16) Join expressiveness: predicate forms, collision policy, self-joins

### 16.1 Join API: `Table.join(..., predicates=..., how=..., lname=..., rname=...)`

`join(right, predicates=(), how='inner', lname='', rname='{name}_right')` supports:

* string column names,
* boolean expressions,
* literal `True`/`False`,
* and **2-tuples** of `(left_key, right_key)` where each key may be:

  * a `Column`,
  * a deferred expression,
  * or a lambda `(Table) -> Column`. ([Ibis][3])

The join docs explicitly spell out this “2-tuple join key” form and the allowed key types. ([Ibis][3])

This is extremely useful for your DSL because it supports:

* “join on computed normalized keys” without creating intermediate columns,
* “join on lowercased strings” etc,
* “join on complex key extraction functions”.

### 16.2 Column collision policy: `lname` / `rname`

Overlapping column names are renamed using format strings:

* `lname` format for left collisions
* `rname` format for right collisions, default `'{name}_right'` ([Ibis][3])

**Best practice:** standardize a single project-wide collision policy (`rname='{name}_r'` etc.) so rule outputs are consistent and easier to diff.

### 16.3 Self-joins require `.view()`

To self-join, you must call `.view()` on one side so the two tables are distinct. The docs explicitly instruct this. ([Ibis][3])

---

## 17) Windowing and ranking for deterministic “winner selection” pipelines

### 17.1 `ibis.row_number()` and normalization across backends

`ibis.row_number()` returns an analytic row-number expression and is normalized across backends to start at **0**. ([Ibis][3])

This is foundational for:

* tie-breaking,
* deterministic top-k,
* stable explode/unnest reconstruction,
* and partition-aware “winner” logic.

### 17.2 Ranking suite (all standard window functions)

Ibis exposes:

* `ibis.rank()` (SQL `RANK()`) ([Ibis][3])
* `ibis.dense_rank()` (SQL `DENSE_RANK()`) ([Ibis][3])
* `ibis.percent_rank()`, `ibis.cume_dist()`, and more ([Ibis][3])

These are ideal for “winner tables” in your graph build:

* partition by stable entity id / symbol id,
* order by evidence quality tiers + deterministic fallback keys,
* filter `rank == 0` (or `row_number == 0`) after ordering.

### 17.3 Window frame construction: ROWS vs RANGE

Ibis provides several window constructors:

* `ibis.window(...)` supports both `rows` and `range` framing and explains the semantic distinction: ROWS uses row-number differences, RANGE uses differences in the *order_by value* of a single expression; all bounds are inclusive. ([Ibis][3])
* `ibis.cumulative_window(group_by=None, order_by=None)` for cumulative windows (inclusive). ([Ibis][3])
* `ibis.range_window(...)`, `ibis.trailing_range_window(...)`, `ibis.trailing_window(...)`. ([Ibis][3])

**Why this matters to you:** span / position-based matching often wants RANGE semantics (“within N chars/lines/tokens”), while dedupe/winner selection is usually ROWS semantics.

---

## 18) Nested data shaping: `unnest` for arrays and `unpack` for structs

### 18.1 `Table.unnest`: explode arrays with optional offsets and “keep empty”

The docs show `unnest` supports:

* `offset="idx"` to emit element indices,
* `keep_empty=True` to preserve empty arrays / nulls,
* and a key determinism note: if you need to preserve original row order of empty arrays/nulls, create an index column **first** using `ibis.row_number()`. ([Ibis][3])

Canonical stable explode pattern:

```python
from ibis import _
import ibis

expr = (
    t
    .mutate(original_row=ibis.row_number())
    .unnest("x", offset="idx", keep_empty=True)
    .order_by("original_row", "idx")
)
```

([Ibis][3])

This is directly applicable to “list-of-edges-per-node” style intermediate representations.

### 18.2 `Table.unpack`: project struct fields into columns

`unpack(*columns)` projects struct fields of the given struct columns into the table; existing fields are retained, but **existing columns are overwritten by unpacking** regardless of column ordering. ([Ibis][3])

Example from docs:

```python
t = ibis.read_json("/tmp/lines.json")   # has struct column "pos"
t2 = t.unpack("pos")                   # projects lat/lon columns
```

([Ibis][3])

For your codebase: `unpack` is the “struct normalization” step after parsing nested payload columns or after producing nested outputs.

---

## 19) Set operations for rule output assembly: `union`, `intersect`, `difference`

### 19.1 `union` with multiset vs set semantics

`Table.union(table, *rest, distinct=False)` performs multiset union by default (union all); with `distinct=True` it becomes set union. Input tables must have identical schemas. ([Ibis][3])

This is exactly your “union many rule outputs” primitive, and you can decide per stage whether duplicates are meaningful.

### 19.2 `intersect` / `difference` also exist (and can apply to more than two tables)

The table API includes `intersect` and `difference` alongside union, and the docs show intersecting more than two tables at once. ([Ibis][3])

---

## 20) Aliasing and view semantics: avoid accidental backend side effects

### 20.1 `Table.alias` creates a temporary view (and it’s not a stable contract)

Ibis explicitly notes:

* `.alias` creates a temporary view in the database,
* this side effect will be removed in a future version,
* and it is not part of the public API. ([Ibis][3])

**Recommendation for your repo:** never rely on `.alias` for pipeline correctness. Prefer:

* `con.create_view(name, expr)` (backend-controlled, explicit, durable) ([Ibis][5])
* or `.view()` for self-joins (pure expression distinction). ([Ibis][3])

---

## 21) “Write once, execute everywhere” done properly: `unbind()` + operation support probing

### 21.1 `Table.unbind()` converts backend-bound expressions to `UnboundTable` IR

`unbind()` returns an expression built on `UnboundTable` instead of backend-specific objects; docs show executing an unbound expression on a different backend. ([Ibis][3])

This is valuable if you:

* compile/canonicalize rulepacks once,
* then execute across multiple engines (even if DF is your default).

### 21.2 Support probing: `Backend.has_operation` + support matrix

Backends expose `has_operation(cls, operation)` to check whether a backend supports a given operation type. ([Ibis][5])
Ibis also maintains an operation support matrix dashboard showing backend support. ([Ibis][7])

In your system, this becomes:

* compile-time gating (“this ruleset requires `Unnest`; fail fast if backend can’t do it”),
* or fallback selection (“if DF can’t do X, use Arrow kernels or a different backend for that stage”).

---

## 22) Putting it together: “best-in-class Ibis+DataFusion” building blocks for your repo

If you standardize only a few high-power patterns, make it these:

1. **Wide-schema rewrites:** selectors + `across` + deferred expressions. ([Ibis][2])
2. **Templated rulepacks:** `ibis.param` + `params=...` everywhere. ([Ibis][4])
3. **Streaming-first outputs:** `to_pyarrow_batches` → Arrow dataset writer (from your previous guide). ([Ibis][3])
4. **Reusable heavy intermediates:** `.cache()` with explicit `.release()` / context manager. ([Ibis][3])
5. **Deterministic winners:** window + `row_number/rank` with explicit tie-break order. ([Ibis][3])
6. **Nested normalization:** `unnest` + `unpack`, preserving order with `row_number`. ([Ibis][3])
7. **Assembly:** schema-aligned `union` (union-all by default, set union when desired). ([Ibis][3])
8. **Materialization control:** DataFusion backend `create_table` / `create_view` / `insert` with explicit overwrite/temp. ([Ibis][5])


[1]: https://ibis-project.org/how-to/analytics/chain_expressions "chain_expressions – Ibis"
[2]: https://ibis-project.org/reference/selectors "selectors – Ibis"
[3]: https://ibis-project.org/reference/expression-tables "expression-tables – Ibis"
[4]: https://ibis-project.org/reference/expression-generic "expression-generic – Ibis"
[5]: https://ibis-project.org/backends/datafusion "datafusion – Ibis"
[6]: https://ibis-project.org/how-to/extending/unbound_expression "Write and execute unbound expressions – Ibis"
[7]: https://ibis-project.org/backends/support/matrix?utm_source=chatgpt.com "Operation support matrix - Ibis"

Below is **follow-on `ibis-advanced.md` content** that rounds out the advanced Ibis surfaces that are realistically applicable to your system (rulepacks, deterministic outputs, streaming batch IO, nested types, SQLGlot boundary, and extensibility). It’s written to *append* to the previous two Ibis responses.

---

## 23) Determinism and ordering contracts (you must encode these in rulepacks)

### 23.1 Row order is undefined unless you `order_by`

Ibis explicitly states that unless you call `.order_by()`, **row order is undefined**. This matters for:

* winner selection (tie-breaks),
* deterministic dataset writes,
* stable hashing of “sequence-like” outputs (e.g., per-parent edge lists). ([Ibis][1])

**Rulepack pattern**

* Any rule output that will be persisted as a “canonical product” should end with an explicit `.order_by([...])` (or an explicit canonical sort tier in your pipeline).

### 23.2 Stable explode/unnest requires explicit indexing + ordering

You already have the canonical pattern (add `row_number()` then `unnest(..., offset=..., keep_empty=True)` and then `.order_by(original_row, idx)`), but it’s worth stating as a core invariant because nested data shows up everywhere in graph compilation. (See your prior §18 + window/ranking sections.) ([Ibis][2])

---

## 24) Table construction and IO surfaces you should standardize

### 24.1 Top-level file reads are part of Ibis’ “fast local” story

Ibis’ homepage shows `ibis.read_parquet(..., table_name=...)` as a first-class pattern, emphasizing “fast local dataframes” with DuckDB default and also Polars/DataFusion as local options. ([Ibis][3])

**Why this matters for you**

* It’s a clean “dev-mode ingestion” path for diagnostic harnesses / golden datasets.
* In production you’ll still likely prefer `con.read_parquet(...)` on the DataFusion backend to keep everything within the same `SessionContext`, but the top-level readers are useful in tooling.

### 24.2 The full “table expression API surface” includes important reshaping and diagnostics helpers

The Table expression reference enumerates methods you haven’t yet leaned on explicitly (but are often useful in rulepack assembly and reviewer tooling), including:

* reshaping: `pivot_longer`, `pivot_wider`, `relocate`
* diagnostics: `describe`, `info`, `preview`
* stability aids: `rowid`, `schema`
  …and many more. ([Ibis][4])

> If you’re building “graph product build” bundles for reviewers, `schema()` + `preview()` become especially valuable as cheap artifacts (without full materialization).

### 24.3 Named memtables are gone; `create_table/create_view` are the naming surface

Ibis 11.0.0 release notes explicitly call out: **memtables can no longer be named explicitly**; use `create_table` or `create_view` for named objects. ([GitHub][5])

This matters because you’re standardizing “namespaces” (catalog/database/table) across build phases; the naming surface now lives in backend APIs, not in `memtable(...)`.

---

## 25) Bidirectional SQL integration (SQL ⇄ Ibis IR) as an agent-facing capability

You already covered the three “execute SQL” surfaces (`Table.sql`, `Backend.sql`, `Backend.raw_sql`). The additional capability you’ll want for agent workflows is **SQL → Ibis expression** parsing.

### 25.1 `ibis.parse_sql(...)` exists (IR-level SQL ingestion)

Ibis release notes include explicit references to `ibis.parse_sql()` behavior (including a bugfix entry), which indicates it’s a real API surface you can use for ingesting SQL into Ibis IR. ([Ibis][6])

**How it fits your architecture**

* LLM proposes SQL (or you load legacy SQL).
* You parse into Ibis IR (`parse_sql`).
* You can then:

  * normalize/inspect (via IR node transforms, `unbind()`, type checks),
  * compile to SQLGlot / dialect SQL,
  * execute on DataFusion backend,
  * and stream results via `to_pyarrow_batches`.

**Important failure-mode note**
There are known issues around SQL→IR round-tripping fidelity (e.g., limits / selected columns missing in some cases). Treat `parse_sql` as powerful but not perfect; build golden tests around the subset of SQL forms you intend to support. ([GitHub][7])

### 25.2 `ibis.to_sql(expr, dialect=..., pretty=..., **params)` is the canonical IR → SQL renderer

`ibis.expr.sql.to_sql(expr, dialect=None, pretty=True, **kwargs)` is documented as the function that returns formatted SQL, with support for scalar parameters via kwargs. ([Ibis][8])

**Best practice for your reproducibility bundles**

* Persist both:

  * the **SQLGlot AST checkpoint** (for semantic diff + lineage), and
  * the **rendered SQL** (`ibis.to_sql`) for humans and minimal repros.

---

## 26) Expression decompilation (`ibis.decompile`) for LLM-friendly “IR as code”

### 26.1 `decompile` is part of the exported expression API surface

Ibis’ API module imports and exports `decompile` from `ibis.expr.decompile`, indicating it’s a real top-level capability (`ibis.decompile(...)`). ([GitHub][9])

**Why it’s highly relevant to your project**

* You can store “rulepack definitions” as Ibis expressions *and* regenerate readable Python that reconstructs them.
* Reviewers can diff “compiler intent” at the Python level even when SQL shapes change across backend versions.
* Agents can propose edits to the decompiled code rather than manipulating raw SQL strings.

### 26.2 Packaging note: the decompiler extra

Ibis’ project metadata includes a `decompiler` optional dependency group that pulls in `black`, which strongly suggests formatting is part of the decompile workflow. ([GitHub][10])

**Operational pattern**

* In dev/CI where you generate docs or artifacts: install `ibis-framework[decompiler]`.
* Store decompiled code in your plan bundles (alongside SQLGlot AST + rendered SQL).

---

## 27) Generic expression “compiler primitives” you’ll use constantly in rulepacks

A lot of your rulepack robustness comes from consistently using a small set of generic primitives that compile cleanly across engines.

### 27.1 Constants and literals: always use `ibis.literal(...)`

The “coming from SQL” guide shows that constants must be turned into Ibis expressions using `ibis.literal(...)` to participate in expression building. ([Ibis][11])

### 27.2 Conditional logic: prefer `ibis.cases(...)` / `ibis.ifelse(...)`

The generic expression API exposes:

* `ibis.ifelse(...)`
* `ibis.cases(...)`
* plus `coalesce`, `least`, `greatest`, and `null` constructors. ([Ibis][12])

**Why this matters for graph compilation**

* “winner selection” and “fallback value” logic becomes declarative and backend-pushdown-friendly.
* You avoid embedding Python conditional logic into UDFs (which blocks optimizer features).

### 27.3 Safe casting vs strict casting: `try_cast` is a first-class tool

`try_cast` exists on table/value expressions and is demonstrated in the Table expression docs as producing `NULL` for uncastable values (e.g., `"book"` → float). ([Ibis][4])

**Rulepack guidance**

* Use `.try_cast(...)` when parsing semi-structured inputs (strings from upstream tools).
* Use `.cast(...)` when a contract violation should be fatal.

### 27.4 Functional composition hooks: `.pipe(...)` and `.substitute(...)`

Ibis exposes `.pipe(...)` and `.substitute(...)` as generic expression methods (and `.pipe(...)` exists on tables too). These are the right “compiler plumbing” tools for:

* applying standardized rewrite passes,
* substituting canonical subexpressions,
* and building “macro libraries” that operate on expressions without depending on backend details. ([Ibis][12])

### 27.5 Visual plan artifacts: `.visualize()`

Ibis expressions can be visualized as GraphViz graphs, and `visualize` is part of the Table API surface. ([Ibis][1])
Use this as a cheap “IR artifact” alongside SQL/AST artifacts.

---

## 28) Nested data: arrays, maps, structs as first-class “graph domain” types

You covered `Table.unnest` and `Table.unpack`. The missing piece is the **rich per-type API** for arrays/maps/structs and the constructors you’ll want for building nested columns intentionally.

### 28.1 Arrays: filter/map/zip/flatten/contains/length/unique

The collection expressions reference enumerates and documents the array method suite, including:

* `contains`, `length`, `flatten`, `unique`, `sort`, `zip`
* higher-order ops: `filter(predicate)` and `map(func)` where callables may accept `(value)` or `(value, index)` and the index is zero-based. ([Ibis][2])

**Why this matters in your repo**

* You can express “per-node edge list normalization” and “per-parent filtering” *inside the backend* without exploding to Python.
* `ArrayValue.zip` is a great way to build `array<struct<...>>` payloads deterministically (e.g., zipped keys + values). ([Ibis][2])

### 28.2 Maps: `contains`, `get(default=...)`, `keys`, `values`, `length`

Map expressions are documented with:

* `.contains(key)`
* `.get(key, default=None)`
* `.keys()`, `.values()`, `.length()`
  …and examples include building map columns via PyArrow `map_` types and accessing values with `[]`. ([Ibis][2])

**Graph relevance**

* Maps are a natural representation for “sparse properties” (e.g., dynamic attribute bags) where a wide column schema is undesirable.
* `.get(..., default=...)` lets you keep rulepacks total-function (no KeyError semantics).

### 28.3 Structs: field access + `.lift()` as a struct→columns operator

Struct expressions support:

* dot access (`t.s.a`)
* bracket access (`t.s["a"]`)
* and `.lift()` to expand fields into columns (with `Table.unpack` as the table-level equivalent). ([Ibis][2])

### 28.4 Constructors: `ibis.array`, `ibis.map`, `ibis.struct`

The collection expressions page documents the constructors:

* `ibis.array(values)`
* `ibis.map(...)`
* `ibis.struct(...)`
  and notes that if any inputs are column expressions, the result is a column; otherwise it’s a scalar. ([Ibis][2])

**Practical use**

* Construct nested payload columns (e.g., `edge_props` as `struct<...>`) in a deterministic way rather than round-tripping through JSON.

---

## 29) UDFs: the missing “nested types + vectorized semantics” details

You covered the UDF ladder conceptually; here are the key technical details that matter for your domain.

### 29.1 All scalar UDF constructors accept the same “placement + signature” knobs

The scalar UDF API explicitly documents methods:

* `builtin`, `pandas`, `pyarrow`, `python`
  Each supports:
* `name`, `database`, `catalog`, and optional explicit `signature=((argtypes...), returntype)`; if omitted, signature comes from type annotations. ([Ibis][13])

### 29.2 `pyarrow` UDFs are the best “kernel lane” inside Ibis

The docs show `@ibis.udf.scalar.pyarrow` with PyArrow compute kernels (`pyarrow.compute`) and also demonstrate struct handling via `combine_chunks().field("a")`, and map handling via `pac.map_lookup(...)`. ([Ibis][13])

**This is directly aligned with your architecture:**

* Implement “stable-hash-ish” or “list explode support” primitives via Arrow compute inside `pyarrow` UDFs as a strong intermediate step before Rust UDFs.

### 29.3 Python (row-by-row) UDFs are explicitly warned as slow

The scalar UDF docs explicitly warn that `python` UDFs run row-by-row and tend to be much slower than vectorized pandas/pyarrow UDFs. ([Ibis][13])
In your repo: reserve these only for “last-resort correctness fallback,” not default rule execution.

---

## 30) Extensibility and “compiler internals” you’ll likely need for a rule DSL

### 30.1 Expressions are thin wrappers over operation nodes; type system validates inputs

Ibis internals describe:

* expressions as the main user-facing component, thin over operations, carrying type/shape
* a type system based on input rules on `Node` subclasses, validated on construction
* rules live in `ibis.expr.rules` ([Ibis][14])

**Why you care**

* If you build a rule DSL that compiles to Ibis IR, you’ll often want to construct/rewrite at the operation-node level (or at least understand it for debugging).

### 30.2 SQL backends compile to SQLGlot expressions (not just strings)

Internals explicitly note that compiler infrastructure was generalized to support compilation of SQLGlot-based expressions and that compilers translate pieces into a string **or SQLGlot expression**. ([Ibis][15])

**Practical extension point**

* Capture SQLGlot ASTs as artifacts, run your SQLGlot policy passes (qualify/normalize/lineage/diff), and optionally feed ASTs back into backend `raw_sql` surfaces (DataFusion backend supports `sge.Expression`).

### 30.3 Operations are “low-level and not stable”

The Ibis Reference explicitly labels Operations as “low level operation classes” and warns they’re subject to change in non-major releases. ([Ibis][16])

**Engineering implication**

* If you extend Ibis via custom operations/translations, isolate that code in one module with strong tests because upstream changes can break internals.

---

## 31) Optional but notable: Ibis streaming DSL (not DataFusion-specific)

Ibis has a separate “Ibis for streaming” guide for streaming operations on Flink, Spark Structured Streaming, and RisingWave (windowing, over aggregation, stream-table join, stream-stream join). ([Ibis][17])

This is likely not central to your current DataFusion-based build system, but it’s relevant if you later want:

* incremental build pipelines fed from event streams (e.g., “code changed → rebuild affected products”).

---

## 32) Quick “implementation checklist” for your repo

If you want “best-in-class Ibis deployment” in your architecture, the missing pieces to standardize (beyond what you already have) are:

1. **Determinism contract**: enforce `.order_by(...)` on all persisted products. ([Ibis][1])
2. **SQL ingestion**: add a controlled path for `ibis.parse_sql` + golden tests (agent SQL → IR). ([Ibis][6])
3. **IR round-trips**: store `ibis.to_sql(...)` alongside SQLGlot AST artifacts. ([Ibis][8])
4. **IR-as-code**: add `ibis.decompile(...)` into your plan bundles (reviewer/agent friendly). ([GitHub][9])
5. **Nested-first modeling**: use array/map/struct APIs + constructors rather than JSON payload columns whenever possible. ([Ibis][2])
6. **UDF ladder enforcement**: default to builtin/pyarrow UDFs; treat python UDFs as “slow path”. ([Ibis][13])
7. **Macro library style**: represent reusable rule fragments with Deferred (`ibis._`), selectors, `.bind()`, and `.pipe()` rewrites. ([GitHub][18])

---


[1]: https://ibis-project.org/reference/expression-tables?utm_source=chatgpt.com "Table expressions - Ibis"
[2]: https://ibis-project.org/reference/expression-collections "expression-collections – Ibis"
[3]: https://ibis-project.org/?utm_source=chatgpt.com "Ibis Project"
[4]: https://ibis-project.org/reference/expression-tables "expression-tables – Ibis"
[5]: https://github.com/ibis-project/ibis/releases?utm_source=chatgpt.com "Releases · ibis-project/ibis"
[6]: https://ibis-project.org/release_notes?utm_source=chatgpt.com "Release notes - Ibis"
[7]: https://github.com/ibis-project/ibis/issues/11105?utm_source=chatgpt.com "bug: parse_sql does not restore limit() and select on joins # ..."
[8]: https://ibis-project.org/reference/expression-generic?utm_source=chatgpt.com "Generic expressions - Ibis"
[9]: https://github.com/ibis-project/ibis/blob/master/ibis/expr/api.py?utm_source=chatgpt.com "ibis/ibis/expr/api.py at main · ibis-project/ibis"
[10]: https://raw.githubusercontent.com/ibis-project/ibis/master/pyproject.toml?utm_source=chatgpt.com "https://raw.githubusercontent.com/ibis-project/ibi..."
[11]: https://ibis-project.org/tutorials/coming-from/sql?utm_source=chatgpt.com "SQL - Ibis"
[12]: https://ibis-project.org/reference/expression-generic "expression-generic – Ibis"
[13]: https://ibis-project.org/reference/scalar-udfs "scalar-udfs – Ibis"
[14]: https://ibis-project.org/concepts/internals?utm_source=chatgpt.com "Internals - Ibis"
[15]: https://ibis-project.org/concepts/internals "internals – Ibis"
[16]: https://ibis-project.org/reference/?utm_source=chatgpt.com "Reference - Ibis"
[17]: https://ibis-project.org/how-to/extending/streaming "Ibis for streaming – Ibis"
[18]: https://github.com/ibis-project/ibis/blob/master/ibis/expr/api.py "ibis/ibis/expr/api.py at main · ibis-project/ibis · GitHub"
