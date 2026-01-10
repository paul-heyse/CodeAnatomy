
According to a document from **December 27, 2025** (the attached DuckDB Python Client Advanced Reference + DuckDB connection/types notes + PyArrow/Polars advanced references), here is the same catalog **rewritten as a function-level integration map** where **every feature includes concrete Python call syntax** (inline; no blocks).

---

## 1) Primary “entrypoints” (where interop begins)

### 1.1 Connection surfaces (explicit vs default connection)

* **Explicit connection** (recommended for long-lived systems / tool servers): `con = duckdb.connect("my.db")` then `con.sql("…")` / `con.execute("…")`.
* **Default hidden connection** (convenience, implicit lifecycle): `duckdb.sql("…")` / `duckdb.query("…")` operate on an internal default connection. 

### 1.2 Two execution APIs you’ll mix constantly

* **SQL API**: `con.sql(query: str) -> DuckDBPyRelation` (lazy) vs `con.execute(query: str, params=...) -> DuckDBPyConnection` (DB-API cursor semantics).
* **Relational API** (lazy plan builder): create relations from sources and chain: `rel.filter("…")`, `rel.project("…")`, `rel.join(other_rel, "…")`, `rel.aggregate("agg_expr", "group_expr")`, `rel.order("…")`. 

**Execution triggers (when “lazy” stops being lazy)**: `rel.fetchall()`, `rel.df()/rel.to_df()`, `rel.pl()`, `rel.arrow()`, `rel.show()`, `rel.create("…")`, `rel.to_parquet("…")`, etc. 

---

## 2) DuckDB ⇄ Polars integration (Polars → DuckDB and DuckDB → Polars)

### 2.1 Replacement scan (“query a Python object by name in SQL”)

DuckDB will treat referenced Python variables as tables via “replacement scan.”

* **Polars DataFrame scan**: `duckdb.sql("SELECT * FROM pl_df")` or `con.sql("SELECT * FROM pl_df")`.
* **Polars LazyFrame scan**: `con.sql("SELECT * FROM lf")` where `lf` is a `polars.LazyFrame`.
  Supported objects explicitly include `polars.DataFrame` and `polars.LazyFrame`. 

**Execution boundary semantics (LazyFrame)**: when you scan a Polars `LazyFrame`, DuckDB executes it and materializes through Arrow only when the query actually needs rows. 

### 2.2 Explicit registration (stable binding; overrides name resolution)

Instead of relying on ambient scope, register the object into the connection catalog:

* **Module-level**: `duckdb.register("t", obj)` then `con.sql("SELECT * FROM t")`.
* **Connection-level**: `con.register("t", obj)` then `con.sql("SELECT * FROM t")`. 

Lifecycle controls:

* `con.unregister("t")` to drop the binding. Registering holds a pointer and defers data fetch until the view is queried. 

**Name precedence (important for agents)**:

* Registered object name > DuckDB table/view name > replacement scan variable name. 

### 2.3 Make it “writable” inside DuckDB (materialize from Polars)

Python-backed scans are *scan sources*; if you want DuckDB-owned storage:

* `con.execute("CREATE TABLE t_persist AS SELECT * FROM pl_df")` (SQL materialization). 
* Or relational: `con.sql("SELECT * FROM pl_df").create("t_persist")`. 

### 2.4 DuckDB → Polars result materialization

* **Polars DataFrame**: `pl_df = con.sql("SELECT …").pl()` (or `duckdb.sql("…").pl()`). 
* **Polars LazyFrame**: `pl_lf = con.sql("SELECT …").pl(lazy=True)` (returns a Polars `LazyFrame` wrapper for deferred downstream Polars work). 

---

## 3) DuckDB ⇄ PyArrow integration (Arrow → DuckDB and DuckDB → Arrow)

### 3.1 Replacement scan over Arrow-native objects

Supported replacement scan inputs explicitly include `pyarrow.Table`, `pyarrow.RecordBatch`, `pyarrow.Dataset`, `pyarrow.Scanner`. 

Function-level patterns:

* **Arrow Table**: `tbl = pyarrow.table({...}); con.sql("SELECT … FROM tbl").fetchone()` (table variable referenced by name). 
* **Arrow Dataset/Scanner**:

  * Build dataset: `ds = pyarrow.dataset.dataset("path/", format="parquet")`
  * Build scanner: `scanner = ds.scanner(columns=[...], filter=..., batch_size=...)`
  * Query via DuckDB: `con.sql("SELECT … FROM scanner")` (scanner variable referenced by name) — supported because Scanner is a valid replacement scan input. 

### 3.2 Explicit relation construction from Arrow (bypasses name magic)

Use the relational API to treat Arrow objects as first-class relation sources:

* `rel = con.from_arrow(tbl)` where `tbl` is a `pyarrow.Table` or `pyarrow.RecordBatch`. 
  Then chain: `rel.filter("…").project("…").order("…")` etc. 

### 3.3 DuckDB → Arrow result export (bulk vs streaming)

* **Arrow Table (bulk)**: `arrow_tbl = con.sql("SELECT …").arrow()` or `con.sql("SELECT …").fetch_arrow_table()`. 
* **Arrow RecordBatchReader (streaming)**: `reader = con.sql("SELECT …").fetch_arrow_reader()` for batched consumption / backpressure-friendly pipelines. 

---

## 4) Arrow scan/stream primitives that pair well with DuckDB (Dataset/Scanner “control plane”)

When Arrow is your *upstream* IO engine (partitioned parquet, provenance, async-ish IO knobs), these are the concrete Scanner consumption APIs:

* `scanner.to_batches()` → iterator of `pyarrow.RecordBatch`
* `scanner.to_reader()` → `pyarrow.RecordBatchReader`
* `scanner.scan_batches()` → iterator of `TaggedRecordBatch` (batch + fragment provenance) 

Explicit “don’t accidentally OOM” hazard:

* `scanner.to_table()` is documented as serially materializing the full scan in memory first (i.e., not streaming). 

---

## 5) Schema + drift management at the Arrow seam (critical for DuckDB↔Polars stability)

### 5.1 Schema union for heterogeneous batches/files

* `merged = pyarrow.unify_schemas([schema1, schema2], promote_options="default")` (strict-ish; only NULL unifies cheaply)
* `merged = pyarrow.unify_schemas([...], promote_options="permissive")` (promote to common supertype) 

### 5.2 Schema metadata (versioning / “contract tags”)

* `schema2 = schema.with_metadata({"contract": "events_v2"})`
* `schema3 = schema2.remove_metadata()` 

### 5.3 “Exotic” Arrow types can be an interop footgun

If you construct view-types like `pa.list_view(pa.int64())` / `pa.large_list_view(pa.int64())`, you’re opting into less common layouts (offset+size buffers). Use only when you’ve verified every downstream consumer (DuckDB + Polars + anything else) handles them. (Concrete constructors: `pa.list_view(value_type)` / `pa.large_list_view(value_type)`). 

---

## 6) DataFrame interchange protocol as a bridge into Arrow (then into DuckDB)

If you have a “df-like” object (not necessarily pandas/polars) that implements `__dataframe__`, PyArrow can ingest it:

* `tbl = pyarrow.interchange.from_dataframe(df_like, allow_copy=False)` (strict, fail if a copy is required)
* `tbl = pyarrow.interchange.from_dataframe(df_like, allow_copy=True)` (best-effort) 

You can then hand that to DuckDB via:

* `rel = con.from_arrow(tbl)` (explicit) 
* or register/scan it: `con.register("t", tbl)` → `con.sql("SELECT … FROM t")` 

Zero-copy caveat examples (why `allow_copy=False` might fail):

* categoricals and some boolean representations can require copying/casting; PyArrow documents hard-fail cases under `allow_copy=False`. 

---

## 7) DuckDB Python UDFs that operate on Arrow batches (high-leverage “compute interop”)

### 7.1 Register scalar vs vectorized (Arrow) UDFs

* **UDF registration**: `con.create_function(name, func, parameters, return_type, **options)` 
* **Vectorized (Arrow) mode**: `con.create_function("f", func, [...], ..., type="arrow")` (DuckDB passes column batches as Arrow arrays instead of per-row calls). 

Safety/behavior knobs (all are call-site options):

* `null_handling="special"` (receive Python `None` for SQL NULLs)
* `exception_handling="return_null"` (fault-tolerant UDF)
* `side_effects=True` (disable purity-based rewrites) 

### 7.2 UDF lifecycle

* `con.remove_function("f")` unregisters the UDF from the connection. 

---

## 8) Operational “gotchas” with explicit mitigations (function-level)

### 8.1 Scope-dependent replacement scans → prefer explicit registration in agent systems

Mitigation pattern: `con.register("t", obj)` then always query `t` instead of relying on `con.sql("… FROM obj_name")`. 

### 8.2 LazyFrame scan introduces a forcing boundary

If you hand DuckDB a `polars.LazyFrame` via replacement scan or registration, DuckDB executes/materializes it (through Arrow) when the query demands rows. Mitigation: treat the handoff point as a deliberate boundary; if you want Polars to stay “fully in charge,” keep work in Polars until `lf.collect()` (Polars-side). 

### 8.3 Large result materialization hazards

Mitigation ladder (increasing “streaminess”):

* `rel.pl()` / `rel.arrow()` (bulk) → only if result fits
* `rel.fetch_arrow_reader()` (streaming) → preferred for large results / piping into downstream Arrow consumers 

---

If you want the next refinement: I can turn this into an **agent-facing “interop decision tree”** where each leaf is *one canonical call sequence* (e.g., “Polars df + DuckDB join + Arrow stream out”) and each internal node is a *runtime constraint check* (schema drift, memory budget, need for mutability, need for pushdown, etc.).
