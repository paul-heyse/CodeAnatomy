Below, “**DataFusionMixins**” refers to the **`deltalake::delta_datafusion::DataFusionMixins` trait** in **delta-rs / deltalake-core**. It is **not** an Apache DataFusion core API—it’s a *Delta Lake integration helper* that standardizes “schema + predicate parsing” behaviors across Delta snapshot types. ([Docs.rs][1])

---

## What DataFusionMixins is (Rust)

`DataFusionMixins` is a **convenience trait for “snapshot hierarchies”** (implemented for `Snapshot`, `EagerSnapshot`, and `LogDataHandler`) that provides exactly three things: ([Docs.rs][1])

1. **`input_schema()` → Arrow schema**
2. **`read_schema()` → Arrow schema (DataFusion/scan-oriented “physical” view)**
3. **`parse_predicate_expression(expr_str, session) -> datafusion::logical_expr::Expr`**

It’s **not dyn-compatible (not object safe)**. ([Docs.rs][1])

---

## Rust capabilities in detail

### 1) `input_schema()` vs `read_schema()` (why there are two)

delta-rs has to bridge *Delta log semantics* and *DataFusion scan semantics*, especially around **partition columns**. The implementations build schemas with:

* **stable ordering** between “logical” and “physical” schemas, even when partition column ordering differs across representations; and
* an option to **dictionary-encode partition columns** of “large” types (strings/binary) when producing the “read schema.” ([Docs.rs][2])

Concretely:

* `Snapshot::input_schema()` calls `_arrow_schema(..., wrap_partitions=false)`
* `Snapshot::read_schema()` calls `_arrow_schema(..., wrap_partitions=true)` ([Docs.rs][2])

And `_arrow_schema`:

* removes partition columns from the base fields, then re-adds them in a controlled order, and
* when `wrap_partitions=true`, dictionary-wraps partition fields for certain Arrow types (`Utf8`, `LargeUtf8`, `Binary`, `LargeBinary`) via DataFusion’s `wrap_partition_type_in_dict`. ([Docs.rs][2])

**Use case:** ensuring that (a) pruning and (b) expression typing behave consistently for Delta tables whose partition columns are extracted from file paths / log metadata (not read from Parquet payload).

---

### 2) `parse_predicate_expression(...)` (string → typed DataFusion Expr)

This is the “main feature” people notice: it lets delta-rs take **SQL-ish predicate strings** (e.g. `"x = 3 AND y > 10"`) and compile them into a **typed** DataFusion logical expression (`Expr`) using:

* `sqlparser` tokenizer + `Parser::parse_expr()` (GenericDialect),
* a Delta-specific context provider,
* DataFusion’s `SqlToRel` (“SQL → relational”) planner,
* and **DeltaParserOptions defaults** (so parsing/identifier behavior is Delta-friendly). ([Docs.rs][3])

Implementation detail (important for correctness): `Snapshot::parse_predicate_expression` first converts **`read_schema()`** to a `DFSchema` and then parses against that DFSchema. ([Docs.rs][2])

Why the `session: &dyn datafusion::catalog::Session` parameter matters: the **Session trait** is DataFusion’s “minimal interface” for planning/execution (config, functions, runtime env, etc.), including `create_physical_expr` and function registries. ([Docs.rs][4])

---

### 3) File pruning / data skipping driven by parsed expressions

Once you can parse predicates into `Expr`, delta-rs can use DataFusion’s pruning machinery to skip files based on Delta log statistics:

`files_matching_predicate(...)`:

* builds a conjunction of filters,
* turns the logical predicate into a **physical expr** using DataFusion,
* creates a `PruningPredicate`,
* prunes a `LogDataHandler` (which iterates file/log entries),
* and yields only `Add` actions for files worth scanning. ([Docs.rs][2])

**Use case:** Delta’s big win over “just Parquet” is file-level skipping using transaction-log metadata (then DataFusion/Parquet does row-group pruning *inside* the remaining files). ([Delta][5])

---

### 4) “Delta SessionContext defaults” that typically pair with DataFusionMixins

In practice, if you’re using these mixins you usually also want the **Delta-flavored SessionContext wrapper**:

`DeltaSessionContext` is explicitly described as a wrapper around DataFusion’s `SessionContext` that bakes in Delta defaults (examples called out: **case-sensitive identifiers**, **Delta planner**, etc.). ([Docs.rs][6])

There’s also a convenience `create_session() -> DeltaSessionContext`. ([Docs.rs][7])

---

## Python: what you actually get (and how it maps to DataFusionMixins)

There is **no Python-exposed “DataFusionMixins” trait**. Instead, you get two *surfaces* that *use the same underlying ideas*:

### 1) DataFusion Python querying Delta tables (DeltaTable as TableProvider)

DataFusion Python supports registering a `deltalake.DeltaTable` as a table provider (DataFusion 43+), then querying it normally. ([Apache DataFusion][8])

```python
from datafusion import SessionContext
from deltalake import DeltaTable

ctx = SessionContext()
dt = DeltaTable("path_to_table")
ctx.register_table("my_delta_table", dt)
ctx.sql("SELECT * FROM my_delta_table").show()
```

If you instead register `dt.to_pyarrow_dataset()` (older deltalake), DataFusion warns you can lose important pushdowns (notably filter pushdown), with big performance impact. ([Apache DataFusion][8])

**Use cases:**

* join Delta tables with other registered sources in DataFusion,
* use DataFusion SQL/UDFs,
* keep everything Arrow-native.

---

### 2) delta-rs Python DML uses DataFusion expression parsing internally (the “Mixins idea”)

The delta-rs Python docs explicitly state that `DeltaTable.update()` predicates and update expressions are **strings** and are **parsed into Apache DataFusion expressions**. ([Delta][9])

That’s the Python-facing manifestation of `parse_predicate_expression(...)`.

**Use cases:**

* `dt.update(updates={"processed": "True"}, predicate="x = 3")` (predicate and RHS expressions parsed by DataFusion) ([Delta][9])
* `dt.delete(predicate="to_delete = true")` (predicate parsing drives which files/rows are rewritten) ([Delta][9])
* `dt.merge(source=..., predicate="t.Date = s.Date AND ...")` (predicate parsing participates in merge planning) ([Delta][10])

---

### 3) delta-rs Python also has a “QueryBuilder” DataFusion integration

A separate Python convenience surface is `QueryBuilder`, which registers a DeltaTable under a name and executes SQL, returning Arrow data (example shown in the DataFusion Python repo issue). ([GitHub][11])

```python
from deltalake import DeltaTable, QueryBuilder
qb = QueryBuilder().register("tbl", DeltaTable("./path"))
table = qb.execute("SELECT * FROM tbl WHERE ...").read_all()
```

**Use case:** “quick SQL over Delta” without you manually wiring a DataFusion `SessionContext` (and potentially with different caching/defaults than your own SessionContext).

---

## When to use what

### If you’re writing Rust that embeds Delta + DataFusion

Use **DataFusionMixins** when you need to:

* **canonicalize schemas** for scans vs user schema (`read_schema` vs `input_schema`),
* parse **string predicates** into typed DataFusion `Expr` the same way delta-rs does,
* or drive **file pruning / data skipping** against Delta log stats. ([Docs.rs][2])

Pair it with **DeltaSessionContext** when you want consistent Delta-friendly parsing/planning defaults across your whole embedded engine. ([Docs.rs][6])

### If you’re in Python

* Use **DataFusion SessionContext + register_table(DeltaTable)** when Delta is one source among many and you want DataFusion’s full SQL/dataframe pipeline. ([Apache DataFusion][8])
* Use **DeltaTable.update/delete/merge** when you want Delta’s transactional DML from Python and are happy writing predicates as strings (DataFusion parses them for you). ([Delta][9])
* Use **QueryBuilder** for “single-purpose SQL on one/more Delta tables,” especially when you don’t want to manage a DataFusion context yourself. ([GitHub][11])

---

## Practical tip: generating predicates safely

If you’re generating predicate strings programmatically (instead of hand-writing), consider building them as an AST and rendering—this avoids quoting/precedence footguns. Here’s the advanced SQLGlot reference I use for that style of work: 

[1]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/trait.DataFusionMixins.html "DataFusionMixins in deltalake::delta_datafusion - Rust"
[2]: https://docs.rs/deltalake-core/0.30.0/x86_64-unknown-linux-gnu/src/deltalake_core/delta_datafusion/mod.rs.html "mod.rs - source"
[3]: https://docs.rs/deltalake-core/0.30.1/x86_64-unknown-linux-gnu/src/deltalake_core/delta_datafusion/expr.rs.html "expr.rs - source"
[4]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.Session.html "Session in datafusion::catalog - Rust"
[5]: https://delta-io.github.io/delta-rs/integrations/delta-lake-datafusion/?utm_source=chatgpt.com "DataFusion - Delta Lake Documentation"
[6]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaSessionContext.html "DeltaSessionContext in deltalake::delta_datafusion - Rust"
[7]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/fn.create_session.html "create_session in deltalake::delta_datafusion - Rust"
[8]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[9]: https://delta-io.github.io/delta-rs/python/usage.html "Usage — delta-rs  documentation"
[10]: https://delta-io.github.io/delta-rs/python/api_reference.html "API Reference — delta-rs  documentation"
[11]: https://github.com/apache/datafusion-python/issues/1140 "Query execution time difference between deltatable QueryBuilder and using DataFusion directly. · Issue #1140 · apache/datafusion-python · GitHub"

### DataFusionMixins itself: the *remaining* important details

**Exact trait signature + what types implement it**

`deltalake::delta_datafusion::DataFusionMixins` is deliberately tiny: it has **3 required methods** and is implemented for **`Snapshot`, `EagerSnapshot`, and `LogDataHandler<'_>`**. ([Docs.rs][1])

```rust
pub trait DataFusionMixins {
    fn read_schema(&self) -> Arc<Schema>;
    fn input_schema(&self) -> Arc<Schema>;
    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        session: &dyn datafusion::catalog::Session,
    ) -> Result<datafusion_expr::Expr, DeltaTableError>;
}
```

([Docs.rs][1])

**Implications for *how you write code***
Because `parse_predicate_expression` uses `expr: impl AsRef<str>`, the trait is **not dyn-compatible / not object safe**, so you should call it via **generics** (or concrete types), not `Box<dyn DataFusionMixins>`. ([Docs.rs][1])

```rust
use deltalake::delta_datafusion::DataFusionMixins;
use datafusion::catalog::Session;

fn parse_filter<S: DataFusionMixins>(
    snap: &S,
    session: &dyn Session,
) -> Result<datafusion_expr::Expr, deltalake::DeltaTableError> {
    snap.parse_predicate_expression("a > 10 AND b IS NOT NULL", session)
}
```

**What you pass for `session: &dyn Session` (this is easy to get wrong)**
DataFusion’s `Session` is a **trait abstraction** that provides planning-time hooks (config, function registry, runtime env, physical expr creation, etc.). ([Docs.rs][2])
`SessionState` implements `Session`, so the most common pattern is: create a context, call `.state()`, and pass `&state`. ([Docs.rs][3])

```rust
use deltalake::delta_datafusion::DeltaSessionContext;

let ctx = DeltaSessionContext::new();
let state = ctx.state(); // SessionState (implements Session)
```

([Docs.rs][4])

**Relationship to DataFusion core “parse SQL expression”**
If you *already* have a `DFSchema`, DataFusion itself can parse an expression string into a typed `Expr` via `SessionContext::parse_sql_expr(sql, df_schema)`. DataFusionMixins is the Delta-side convenience when you want parsing grounded in the table’s schemas + Delta defaults. ([Docs.rs][5])

---

## The rest of the Delta↔DataFusion integration surface you didn’t have above (the “mixins ecosystem”)

Everything below lives under `deltalake::delta_datafusion` and is what you typically end up using *around* `DataFusionMixins`. The module index is a good mental map. ([Docs.rs][6])

---

### DeltaSessionContext + create_session: consistent Delta-friendly sessions

`DeltaSessionContext` wraps DataFusion’s `SessionContext` and bakes in Delta-friendly defaults (notably called out: **case-sensitive identifiers** and a **Delta planner**). It exposes:

* `new()`
* `with_runtime_env(runtime_env: Arc<RuntimeEnv>)`
* `state() -> SessionState`
* `into_inner() -> SessionContext` ([Docs.rs][4])

If you just want “give me the right defaults,” `create_session() -> DeltaSessionContext` exists. ([Docs.rs][7])

---

### DeltaRuntimeEnvBuilder: Delta-flavored runtime limits (spill/temp sizing)

If you’re embedding Delta DML / merges and want predictable memory behavior, Delta exposes a small builder:

* `DeltaRuntimeEnvBuilder::new()`
* `with_max_spill_size(size: usize)`
* `with_max_temp_directory_size(size: u64)`
* `build() -> Arc<RuntimeEnv>` ([Docs.rs][8])

You then feed that into `DeltaSessionContext::with_runtime_env(...)`. ([Docs.rs][4])

---

### DeltaParserOptions + DeltaSessionConfig: “sane defaults” wrappers

These two are lightweight wrappers that implement `Default` and are convertible *into* the underlying DataFusion/sqlparser config types:

* `DeltaParserOptions`: wrapper for sqlparser `ParserOptions` with Delta defaults; `Default` + `From<DeltaParserOptions> for ParserOptions`. ([Docs.rs][9])
* `DeltaSessionConfig`: wrapper for DataFusion `SessionConfig` with Delta defaults; `Default` + `From<DeltaSessionConfig> for SessionConfig`. ([Docs.rs][10])

These matter because DataFusionMixins’ predicate parsing is only as good as the SQL parsing + identifier behavior you configure.

---

## Scan-time behavior you didn’t cover: DeltaScanConfig + Builder

This is the *other* half of “why Delta tables feel first-class in DataFusion.”

### DeltaScanConfig: controls pushdown, file metadata column, and schema shaping

`DeltaScanConfig` is a serializable config struct with fields: ([Docs.rs][11])

* `file_column_name: Option<String>` — include a column containing each record’s source path ([Docs.rs][11])
* `wrap_partition_values: bool` — dictionary-encode partition values (defaults to `true`) ([Docs.rs][11])
* `enable_parquet_pushdown: bool` — allow pushing filters into the parquet scan (defaults to `true`) ([Docs.rs][11])
* `schema_force_view_types: bool` — if true, parquet reader can read Utf8/Binary “large” columns using view types (Utf8View/BinaryView) ([Docs.rs][11])
* `schema: Option<Arc<Schema>>` — override the schema used when reading (names must match; types can differ if compatible, e.g. view types) ([Docs.rs][11])

Key constructors / fluent setters:

* `new()`, `new_from_session(session: &dyn Session)`
* `with_file_column_name(...)`
* `with_wrap_partition_values(...)`
* `with_parquet_pushdown(...)`
* `with_schema(...)` ([Docs.rs][11])

### DeltaScanConfigBuilder: safe metadata-column exposure (avoids name collisions)

If you want the “file path per row” feature, you should usually use the builder:

* `new()`
* `with_file_column(include: bool)` (auto-generates a safe name)
* `with_file_column_name(&S)` (user-provided name)
* `wrap_partition_values(bool)`
* `with_parquet_pushdown(bool)` (**note**: docs explicitly say when disabled the filter is only used for pruning files)
* `with_schema(Arc<Schema>)`
* `build(&self, snapshot: &EagerSnapshot) -> Result<DeltaScanConfig, DeltaTableError>`
  …and `build` explicitly validates “no column name conflicts occur.” ([Docs.rs][12])

That “no conflicts” check is the real feature: it prevents you from accidentally shadowing an existing column with your metadata column.

---

## DeltaTableProvider: TableProvider with Delta-specific scan/DML behavior

`DeltaTableProvider` is the concrete DataFusion `TableProvider` used to scan a Delta table while optionally injecting metadata columns.

### Construction knobs

* `try_new(snapshot: EagerSnapshot, log_store: Arc<dyn LogStore>, config: DeltaScanConfig) -> Result<DeltaTableProvider, DeltaTableError>` ([Docs.rs][13])
* `with_files(self, files: Vec<Add>) -> DeltaTableProvider` to restrict the scan to a specific file set (“advanced usecases”) ([Docs.rs][13])

### What it implements (important planning hooks)

As a `TableProvider`, it exposes:

* `scan(session, projection, filters, limit) -> Future<Result<Arc<dyn ExecutionPlan>>>` ([Docs.rs][13])
* `supports_filters_pushdown(&self, filter: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>>` ([Docs.rs][13])
* `insert_into(state, input_plan, insert_op) -> Future<Result<Arc<dyn ExecutionPlan>>>`
  and the docs explicitly say **only Append and Overwrite are supported**. ([Docs.rs][13])
* plus `schema()`, `statistics()`, `constraints()`, etc. ([Docs.rs][13])

This is the place where your `DeltaScanConfig` decisions (pushdown on/off, file column, schema view-types) actually take effect during planning.

---

## DeltaScan: the physical plan wrapper

`DeltaScan` is an `ExecutionPlan` (“wrapper for parquet scans”). If you’re debugging physical plans, this is the node you’ll see where Delta-specific behaviors get threaded into the parquet scan. ([Docs.rs][14])

---

## DeltaDataChecker: invariants, constraints, nullability, and generated columns

This is a major “hidden feature” of the integration: Delta uses DataFusion’s expression engine to enforce table rules.

`DeltaDataChecker` is described as “responsible for checking batches of data conform to table’s invariants, constraints and nullability.” ([Docs.rs][15])

Key constructors / builders: ([Docs.rs][15])

* `empty()`
* `new_with_invariants(Vec<Invariant>)`
* `new_with_constraints(Vec<Constraint>)`
* `new_with_generated_columns(Vec<GeneratedColumn>)`
* `with_session_context(SessionContext)` (lets you control the DF context used)
* `with_extra_constraints(Vec<Constraint>)`
* `new(snapshot: &EagerSnapshot)`

Execution hook:

* `check_batch(&self, record_batch: &RecordBatch) -> Result<(), DeltaTableError>`
  and docs say violations return `DeltaTableError::InvalidData` with offending values. ([Docs.rs][15])

This is directly aligned with Delta’s “constraints/invariants are SQL expressions” model and is part of why Delta depends on DataFusion. ([Delta][16])

---

## Change Data Feed: DeltaCdfTableProvider (Delta CDF in DataFusion)

If you enable/consume Delta’s Change Data Feed, delta-rs exposes a separate `TableProvider`:

* `DeltaCdfTableProvider` implements `TableProvider` with the expected hooks (`scan`, `supports_filters_pushdown`, `statistics`, etc.). ([Docs.rs][17])
* The module `deltalake::delta_datafusion::cdf` exists specifically for “logical operators and physical executions for CDF.” ([Docs.rs][6])

---

## Plan serialization / extension plumbing: DeltaLogicalCodec + DeltaPhysicalCodec

If you’re doing anything like “save plans,” “ship plans,” or “custom extension nodes,” these show up.

* `DeltaLogicalCodec` implements DataFusion’s `LogicalExtensionCodec` and provides hooks like `try_encode_table_provider` / `try_decode_table_provider`, etc. (“Does serde on DeltaTables”). ([Docs.rs][18])
* `DeltaPhysicalCodec` implements `PhysicalExtensionCodec` for Delta physical plans (`try_encode`, `try_decode`, plus UDF/expr codec hooks). ([Docs.rs][19])

---

## DeltaTableFactory: powering `CREATE EXTERNAL TABLE ... STORED AS ...`

`DeltaTableFactory` implements DataFusion’s `TableProviderFactory`:

```rust
fn create(&self, state: &dyn Session, cmd: &CreateExternalTable)
  -> Future<Result<Arc<dyn TableProvider>, DataFusionError>>
```

That trait is DataFusion’s pluggability point for external table creation. ([Docs.rs][20])

On the DataFusion side, `SessionContext` exposes `table_factory(file_type)` to retrieve the registered factory (i.e., the wiring exists at runtime). ([Docs.rs][5])

*Implementation note:* the **exact API for registering** a new factory is version-dependent (it’s typically done while building the session state). If your goal is to support `STORED AS DELTATABLE` in SQL, the deltalake piece you need is `DeltaTableFactory` + the DataFusion piece you need is “register this factory for your file_type before executing the DDL.” (You can sanity-check registration by calling `ctx.table_factory("...")`.) ([Docs.rs][20])

---

## Python side: the missing “native vs dataset” distinction and why it matters

* DataFusion Python supports registering a `deltalake.DeltaTable` directly as a table provider on modern versions (the docs call out DataFusion 43+). ([Apache DataFusion][21])
* Older fallbacks use Arrow Dataset (`to_pyarrow_dataset()`), but DataFusion docs explicitly warn this path may **not** support features like **filter pushdown**, with significant performance impact. ([Apache DataFusion][21])
* delta-rs’ own “Using Delta Lake with DataFusion” guide explicitly explains *why* Delta depends on DataFusion: update/merge expressions and invariants/constraints are expressed as SQL and evaluated through DataFusion. ([Delta][16])

So, from a “capabilities surface” standpoint in Python: **native registration** is what preserves the richer pushdown + scan behavior that corresponds to the Rust-side `DeltaTableProvider`/`DeltaScanConfig` world.

---

If you want, I can turn the above into two “agent-ready” artifacts:

1. a Rust cookbook: **(a)** query-only registration, **(b)** scan config w/ file-path column, **(c)** CDF provider registration, **(d)** constraints checking in a write pipeline;
2. a “DDL wiring” note showing how to make `CREATE EXTERNAL TABLE ... STORED AS <X>` resolve to `DeltaTableFactory` in your chosen DataFusion version.

[1]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/trait.DataFusionMixins.html "DataFusionMixins in deltalake::delta_datafusion - Rust"
[2]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.Session.html "Session in datafusion::catalog - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html "SessionState in datafusion::execution::session_state - Rust"
[4]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaSessionContext.html "DeltaSessionContext in deltalake::delta_datafusion - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html "SessionContext in datafusion::execution::context - Rust"
[6]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/index.html "deltalake::delta_datafusion - Rust"
[7]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/fn.create_session.html "create_session in deltalake::delta_datafusion - Rust"
[8]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaRuntimeEnvBuilder.html "DeltaRuntimeEnvBuilder in deltalake::delta_datafusion - Rust"
[9]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaParserOptions.html "DeltaParserOptions in deltalake::delta_datafusion - Rust"
[10]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaSessionConfig.html "DeltaSessionConfig in deltalake::delta_datafusion - Rust"
[11]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaScanConfig.html "DeltaScanConfig in deltalake::delta_datafusion - Rust"
[12]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaScanConfigBuilder.html "DeltaScanConfigBuilder in deltalake::delta_datafusion - Rust"
[13]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaTableProvider.html "DeltaTableProvider in deltalake::delta_datafusion - Rust"
[14]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaScan.html "DeltaScan in deltalake::delta_datafusion - Rust"
[15]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaDataChecker.html "DeltaDataChecker in deltalake::delta_datafusion - Rust"
[16]: https://delta-io.github.io/delta-rs/integrations/delta-lake-datafusion/ "DataFusion - Delta Lake Documentation"
[17]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaCdfTableProvider.html "DeltaCdfTableProvider in deltalake::delta_datafusion - Rust"
[18]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaLogicalCodec.html "DeltaLogicalCodec in deltalake::delta_datafusion - Rust"
[19]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaPhysicalCodec.html "DeltaPhysicalCodec in deltalake::delta_datafusion - Rust"
[20]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaTableFactory.html "DeltaTableFactory in deltalake::delta_datafusion - Rust"
[21]: https://datafusion.apache.org/python/user-guide/data-sources.html?utm_source=chatgpt.com "Data Sources — Apache Arrow DataFusion documentation"
