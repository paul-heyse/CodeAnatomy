## Delta Lake ↔ DataFusion integration map (what exists today)

There are **two primary integration planes**:

1. **Delta tables as a DataFusion `TableProvider`** (query Delta in DataFusion; optional insert/overwrite support)
2. **DataFusion as an embedded SQL/expression engine inside delta-rs** (delta-rs uses DataFusion to evaluate SQL expressions for MERGE/UPDATE/constraints)

Everything else (Ibis, “register as Arrow Dataset”, etc.) is a wrapper around those two.

---

# 1) Python: `datafusion` (datafusion-python) querying Delta via `deltalake.DeltaTable`

## 1.1 Version gate + “best” path vs fallback path

### Best path (TableProvider via FFI)

DataFusion **43.0.0+** can register **foreign table providers** such as Delta Lake, provided you have a recent `deltalake` that exposes the required interfaces. The docs show:

```python
from datafusion import SessionContext
from deltalake import DeltaTable

ctx = SessionContext()
delta_table = DeltaTable("path_to_table")
ctx.register_table("my_delta_table", delta_table)

df = ctx.table("my_delta_table")
df.show()
```

([Apache DataFusion][1])

### Fallback path (Arrow Dataset bridge; weaker optimization)

If your `deltalake` is older (DataFusion docs call out **prior to 0.22**), register a PyArrow Dataset instead:

```python
from deltalake import DeltaTable

delta_table = DeltaTable("path_to_table")
ctx.register_dataset("my_delta_table", delta_table.to_pyarrow_dataset())
df = ctx.table("my_delta_table")
df.show()
```

…but you lose key optimizations (notably **filter pushdown**), and perf can move a lot. ([Apache DataFusion][1])

## 1.2 Why TableProvider beats “just scan parquet”

delta-rs’ DataFusion guide frames the concrete perf win: Parquet gives DataFusion row-group pruning; Delta adds **file-level skipping using transaction-log metadata**, so you can prune whole files *before* Parquet row-group pruning even starts. ([Delta][2])

## 1.3 API surface in practice (SQL + DataFrame)

Once registered, it’s ordinary DataFusion:

```python
ctx.sql("""
  select id1, sum(v1) as v1
  from my_delta_table
  where id1 = 'id096'
  group by id1
""").show()
```

(delta-rs guide uses this exact pattern with `ctx.register_table_provider(...)` in some examples, but conceptually it’s the same “register provider → query”.) ([Delta][2])

## 1.4 Storage/auth wiring (in Python)

The integration point is: **anything you can pass to `deltalake.DeltaTable(...)`** is usable by DataFusion, because the table object is the provider.

If you’re using Ibis as the facade, Ibis’ DataFusion backend exposes:

* `read_delta(path, table_name=None, **kwargs)` → kwargs are passed to `deltalake.DeltaTable` ([Ibis][3])
* `to_delta(expr, path, **kwargs)` → kwargs passed to `deltalake.writer.write_deltalake` ([Ibis][3])

This is the clean “config injection” seam for `storage_options`, cloud creds, etc. (Whatever `DeltaTable` supports.)

---

# 2) Why this works across versions: DataFusion “Foreign Table Providers” FFI

Starting with DataFusion **43.0.0**, the Python bindings added a stable FFI boundary for table providers: a Rust library can expose a provider to Python as a **PyCapsule containing an `FFI_TableProvider`**, so it can interoperate with `datafusion-python` without being built against the exact same DataFusion crate version. ([Apache DataFusion][4])

The DataFusion blog shows the canonical hook name and shape:

* implement `__datafusion_table_provider__` returning a `PyCapsule`
* wrap your provider with `FFI_TableProvider::new(Arc::new(provider), ...)` ([Apache DataFusion][4])

This is the mechanism DataFusion’s docs refer to when they say “requires a recent deltalake to provide the required interfaces.” ([Apache DataFusion][1])

---

# 3) Rust: `deltalake-core` / delta-rs as a DataFusion `TableProvider`

## 3.1 Enablement: Cargo feature flag

delta-rs explicitly exposes a `datafusion` feature: enabling it provides a `datafusion::datasource::TableProvider` implementation for Delta tables. ([Docs.rs][5])

From `deltalake-core` docs:

* feature `datafusion` enables `TableProvider` support
* (legacy alias `datafusion-ext` deprecated) ([Docs.rs][5])

## 3.2 Minimal Rust usage (register table + query)

The `deltalake-core` docs show the exact pattern:

```rust
use std::sync::Arc;
use datafusion::prelude::SessionContext;

let mut ctx = SessionContext::new();
let table = deltalake_core::open_table(table_url).await.unwrap();

ctx.register_table("demo", Arc::new(table)).unwrap();
let batches = ctx.sql("SELECT * FROM demo").await.unwrap().collect().await.unwrap();
```

([Docs.rs][5])

## 3.3 Advanced Rust surface: `deltalake::delta_datafusion::DeltaTableProvider`

If you want tighter control than “register the table object”, delta-rs exposes an explicit provider type:

* `DeltaTableProvider::try_new(snapshot: EagerSnapshot, log_store: Arc<dyn LogStore>, config: DeltaScanConfig) -> Result<DeltaTableProvider, DeltaTableError>`
* `DeltaTableProvider::with_files(files: Vec<Add>) -> DeltaTableProvider` (explicit file set restriction for advanced cases)
* Implements `TableProvider` including `insert_into(...)` (Append / Overwrite only) ([Docs.rs][6])

This is the seam you use if you want to:

* pin a snapshot yourself,
* inject a custom `LogStore` / object store behavior,
* or do “file-set selection” externally and hand DataFusion exactly those `Add` actions.

---

# 4) Writing to Delta “through DataFusion”: what’s real vs what’s not

## 4.1 DataFusion-native write capability (Rust, via `insert_into`)

`DeltaTableProvider`’s `TableProvider` impl includes:

```rust
fn insert_into(
  &self,
  state: &dyn Session,
  input: Arc<dyn ExecutionPlan>,
  insert_op: InsertOp,
) -> ...
```

and the docs note: **insert is only supported for Append and Overwrite**. ([Docs.rs][6])

So: **DataFusion can plausibly do `INSERT INTO` / `INSERT OVERWRITE`** against a registered Delta provider, but don’t expect Spark-like MERGE/UPDATE/DELETE purely through DataFusion SQL unless you’re explicitly using delta-rs’ higher-level ops.

## 4.2 The common pattern in Python: compute in DataFusion, commit in delta-rs

delta-rs explicitly states it depends on DataFusion for SQL evaluation in its own higher-level features:

* update/merge are defined in terms of SQL expressions
* invariants/constraints are SQL expressions
  ([Delta][2])

Interpretation for architecture:

* Use **DataFusion** for query planning/execution over the Delta snapshot.
* Use **delta-rs / `deltalake`** as the commit engine for mutations (MERGE/UPDATE/DELETE), because that’s where transaction-log semantics live—and it already uses DataFusion internally when it must evaluate SQL expressions. ([Delta][2])

If you’re fronting through Ibis, the “compute then commit” bridge is already packaged:

* `to_delta(expr, path, **kwargs)` executes and persists results to a Delta table using `write_deltalake`. ([Ibis][3])

---

# 5) Change Data Feed integration in DataFusion (Rust-level)

delta-rs also exposes a **CDF-specific DataFusion provider**:

* `DeltaCdfTableProvider::try_new(cdf_builder: CdfLoadBuilder) -> Result<DeltaCdfTableProvider, DeltaTableError>`
* implements `TableProvider` (`scan`, `supports_filters_pushdown`, etc.) ([Docs.rs][7])

This is the “DataFusion-native” way to query CDF as a logical table rather than manually materializing CDF batches and re-registering them.

---

# 6) Operational gotchas specific to Delta↔DataFusion

## 6.1 Identifier normalization vs “column names with caps”

If your Delta table has mixed-case column names and you hit `FieldNotFound` even though the column exists, delta-rs maintainers recommend disabling identifier normalization in the DataFusion SQL parser:

```python
from datafusion import SessionConfig, SessionContext
from deltalake import DeltaTable

cfg = SessionConfig().set("datafusion.sql_parser.enable_ident_normalization", "false")
ctx = SessionContext(cfg)

ctx.register_table_provider("data", DeltaTable("msr_data"))
ctx.sql("select CutoffDate from data limit 10").show()
```

([GitHub][8])

## 6.2 “Which integration path did I accidentally pick?”

If you register the Arrow Dataset fallback (`register_dataset`), DataFusion’s own docs warn you lose filter pushdown and can see big perf differences. ([Apache DataFusion][1])

---


[1]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[2]: https://delta-io.github.io/delta-rs/integrations/delta-lake-datafusion/ "DataFusion - Delta Lake Documentation"
[3]: https://ibis-project.org/backends/datafusion "datafusion – Ibis"
[4]: https://datafusion.apache.org/blog/2024/12/14/datafusion-python-43.1.0/ "Apache DataFusion Python 43.1.0 Released - Apache DataFusion Blog"
[5]: https://docs.rs/deltalake-core "deltalake_core - Rust"
[6]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaTableProvider.html "DeltaTableProvider in deltalake::delta_datafusion - Rust"
[7]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaCdfTableProvider.html "DeltaCdfTableProvider in deltalake::delta_datafusion - Rust"
[8]: https://github.com/delta-io/delta-rs/discussions/3353 "Columns with capital letters in DataFusion · delta-io delta-rs · Discussion #3353 · GitHub"

Below is a **gap analysis** versus the integration map I just gave you. These are **real, shipped integration surfaces** between **`deltalake` / delta-rs** and **DataFusion** that I either didn’t mention at all or only hinted at without the concrete API/syntax.

---

## 1) **`deltalake` ships a DataFusion SQL engine wrapper in Python (`QueryBuilder`)** (not covered)

I previously described “DataFusion as an embedded SQL/expression engine inside delta-rs” only in the context of **MERGE/UPDATE/constraints**. There is also an **explicit Python-facing query surface** that wraps DataFusion to run arbitrary SQL over registered DeltaTables.

Evidence:

* delta-rs maintainers/users explicitly reference “exposing the SQL interface of DataFusion inside delta_rs” and a `QueryBuilder().execute(...)` pattern. ([GitHub][1])
* The (documented) class surface (as currently described in published docs/mirrors) is:

```python
class QueryBuilder:
    def register(self, table_name: str, delta_table: DeltaTable) -> QueryBuilder: ...
    def execute(self, sql: str) -> RecordBatchReader: ...
```

([Tessl][2])

**Implication:** you can run SQL on Delta without installing `datafusion-python` at all—`deltalake` is embedding DataFusion internally and returning Arrow batches.

---

## 2) **DataFusion scan customization via `DeltaTableProvider` + `DeltaScanConfig`** (only partially covered)

I mentioned `DeltaTableProvider` and `with_files`, but I did not enumerate the **scan config knobs** that materially affect how DataFusion sees the table and what metadata columns exist.

### 2.1 `DeltaTableProvider` (purpose + constructors)

`DeltaTableProvider` exists specifically to “enable additional metadata columns to be included during the scan” and is built via:

* `DeltaTableProvider::try_new(snapshot, log_store, config)`
* `DeltaTableProvider::with_files(files)` (restrict scan to an explicit `Vec<Add>` file set) ([Docs.rs][3])

### 2.2 `DeltaScanConfig` fields + builder methods (important knobs)

`DeltaScanConfig` is the main missing piece. It has **fields**:

* `file_column_name: Option<String>` — include source path per record
* `wrap_partition_values: bool` — dictionary-encode partition values (default true)
* `enable_parquet_pushdown: bool` — allow Parquet filter pushdown (default true)
* `schema_force_view_types: bool` — read Utf8/Utf8Large as `Utf8View`, Binary/BinaryLarge as `BinaryView`
* `schema: Option<Arc<Schema>>` — override the read schema (compatible types) ([Docs.rs][4])

and **methods** you can actually call:

* `DeltaScanConfig::new()`
* `DeltaScanConfig::new_from_session(session: &dyn Session)` (derive from DataFusion session)
* `.with_file_column_name(name)`
* `.with_wrap_partition_values(bool)`
* `.with_parquet_pushdown(bool)`
* `.with_schema(schema_ref)` ([Docs.rs][4])

**Why this matters:** these knobs are the “real” integration points for:

* adding file lineage columns (DataFusion analog to Spark’s file metadata patterns),
* making partition columns cheaper (dictionary encoding),
* controlling whether DataFusion filters propagate into Parquet row-group pruning,
* and forcing Arrow “view” string/binary types for DataFusion-compat / perf.

---

## 3) **CDF-as-TableProvider is richer than “it exists”** (I underspecified it)

I mentioned `DeltaCdfTableProvider` exists, but didn’t list what it exposes to DataFusion.

`DeltaCdfTableProvider`:

* constructor: `DeltaCdfTableProvider::try_new(CdfLoadBuilder)` ([Docs.rs][5])
* implements `TableProvider` methods that DataFusion can exploit, including:

  * `supports_filters_pushdown(...)`
  * `scan_with_args(ScanArgs)` (structured scan interface)
  * `statistics()`
  * `constraints()`
  * `get_table_definition()`
  * `get_logical_plan()`
  * `get_column_default()`
  * `insert_into(...)` (TableProvider insert hook) ([Docs.rs][5])

**Gap:** I didn’t call out that the CDF provider participates in DataFusion’s richer TableProvider contract (pushdown + stats + constraints + structured scan args), not just “scan()”.

---

## 4) **Rust-side: direct `read_table` path (no registration) exists** (not covered)

Besides `ctx.register_table("name", provider)` there’s a “direct read” workflow:

```rust
let table = deltalake::open_table("...").await?;
let ctx = SessionContext::new();
let dataframe = ctx.read_table(Arc::new(table.clone()))?;
```

This is documented in delta-rs “Querying Delta Tables”. ([Delta][6])

**Gap:** I only described `register_table`/`register_table_provider` patterns.

---

## 5) **DataFusion → Arrow → `write_deltalake` commit loop (Python) is a first-class pattern** (not shown earlier)

There is a practical “integration loop” used in “Delta Lake without Spark: DataFusion”:

1. register the Delta table in DataFusion (often via Arrow Dataset)
2. run a SQL query
3. convert result to Arrow
4. commit as a new Delta table via `write_deltalake(...)`

Example pattern is shown in the Delta Lake blog (DataFusion section). ([Delta Lake][7])

**Gap:** I described “compute in DataFusion, commit in delta-rs” conceptually, but not the concrete `to_arrow_table()` → `write_deltalake(...)` flow as a documented integration path.

---

## 6) **Object store plumbing is an explicit integration seam** (not covered)

delta-rs exposes an object-store registry that maps URLs to `ObjectStore` implementations:

* module purpose: “Map object URLs to ObjectStore”
* `DefaultObjectStoreRegistry` uses `parse_url_opts` to create stores based on environment ([Docs.rs][8])

This matters because **both** DataFusion and delta-rs sit on top of the `object_store` ecosystem; if you need custom storage backends or credential resolution, this is the seam you typically extend—especially in Rust integrations.

---

## 7) **API naming mismatch / multiple registration entrypoints in DataFusion Python** (worth calling out)

DataFusion’s Python docs show registering Delta providers via:

```python
ctx.register_table("my_delta_table", DeltaTable("path"))
```

…and the fallback `register_dataset(...)` approach. ([Apache DataFusion][9])

delta-rs’ own DataFusion integration guide uses:

```python
ctx.register_table_provider("my_delta_table", table)
```

([Delta][10])

**Gap:** I didn’t explicitly flag that there are *two* public Python registration call shapes depending on the DataFusion Python surface you’re using (and that these map onto the custom/foreign provider pathway vs dataset fallback).

---

## 8) Adjacent but real: **Delta Sharing ↔ DataFusion** (ecosystem gap)

Not `deltalake` specifically, but if your goal is “Delta interop with DataFusion” broadly: there is a dedicated crate `datafusion_delta_sharing` that integrates **Delta Sharing** (REST protocol) with DataFusion. ([Docs.rs][11])

---

# Net: what you should add to your deep dive doc

If you want your `delta-lake-advanced.md` to be “max coverage, minimal text,” add these subsections under the DataFusion integration chapter:

1. **Python**: `QueryBuilder` (embedded DataFusion) — `register(...)`, `execute(sql)->RecordBatchReader` ([Tessl][2])
2. **Rust**: `DeltaScanConfig` knobs (file column, partition encoding, parquet pushdown, view types, schema override) + `DeltaTableProvider::try_new/with_files` ([Docs.rs][4])
3. **Rust**: `DeltaCdfTableProvider` full TableProvider contract (pushdown/stats/constraints/scan_with_args) ([Docs.rs][5])
4. **Rust**: `SessionContext::read_table(...)` direct path ([Delta][6])
5. **Python workflow**: DataFusion query → Arrow → `write_deltalake` commit loop ([Delta Lake][7])
6. **Storage seam**: object-store registry / URL→ObjectStore mapping ([Docs.rs][8])


[1]: https://github.com/delta-io/delta-rs/issues/3076 "Feature request : user friendly methods for Querybuilder , show and sql · Issue #3076 · delta-io/delta-rs · GitHub"
[2]: https://tessl.io/registry/tessl/pypi-deltalake?utm_source=chatgpt.com "tessl/pypi-deltalake@1.1.x - Registry"
[3]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaTableProvider.html "DeltaTableProvider in deltalake::delta_datafusion - Rust"
[4]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaScanConfig.html "DeltaScanConfig in deltalake::delta_datafusion - Rust"
[5]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaCdfTableProvider.html "DeltaCdfTableProvider in deltalake::delta_datafusion - Rust"
[6]: https://delta-io.github.io/delta-rs/usage/querying-delta-tables/ "Querying a table - Delta Lake Documentation"
[7]: https://delta.io/blog/delta-lake-without-spark/?utm_source=chatgpt.com "Delta Lake without Spark"
[8]: https://docs.rs/deltalake/latest/deltalake/logstore/object_store/registry/index.html "deltalake::logstore::object_store::registry - Rust"
[9]: https://datafusion.apache.org/python/user-guide/data-sources.html?utm_source=chatgpt.com "Data Sources — Apache Arrow DataFusion documentation"
[10]: https://delta-io.github.io/delta-rs/integrations/delta-lake-datafusion/ "DataFusion - Delta Lake Documentation"
[11]: https://docs.rs/datafusion-delta-sharing?utm_source=chatgpt.com "datafusion_delta_sharing - Rust"

## Delta Lake ↔ DataFusion: additional integration surfaces and exact APIs

### 1) Python: two “native” registration entrypoints (and when to use which)

**A. Register the Delta table as a DataFusion table provider (preferred)**

Depending on your `datafusion` Python binding version, you’ll see both of these in the wild:

```python
# DataFusion docs pattern
from datafusion import SessionContext
from deltalake import DeltaTable

ctx = SessionContext()
ctx.register_table("t", DeltaTable("path/to/delta"))
ctx.sql("select count(*) from t").show()
```

([Apache DataFusion][1])

```python
# delta-rs docs pattern
from datafusion import SessionContext
from deltalake import DeltaTable

ctx = SessionContext()
ctx.register_table_provider("t", DeltaTable("path/to/delta"))
ctx.sql("select count(*) from t").show()
```

([Delta][2])

**B. Arrow Dataset fallback (older deltalake; reduced pushdown)**

```python
from datafusion import SessionContext
from deltalake import DeltaTable

ctx = SessionContext()
dt = DeltaTable("path/to/delta")
ctx.register_dataset("t", dt.to_pyarrow_dataset())
ctx.sql("select * from t where ...").show()
```

DataFusion calls out this path as a fallback for older `deltalake` (pre-0.22) and notes it lacks some features such as filter pushdown. ([Apache DataFusion][1])

---

## 2) Rust: `deltalake_core::delta_datafusion` is a full “interop toolkit” (not just TableProvider)

delta-rs exposes a dedicated DataFusion integration module that includes:

* **DDL-time table creation via a `TableProviderFactory`** (DeltaTableFactory)
* **scan shaping knobs** (DeltaScanConfig / DeltaTableProvider)
* **physical plan extensions + codecs** (DeltaScan + DeltaPhysicalCodec + DeltaLogicalCodec)
* **constraint/invariant checking** via embedded DataFusion (DeltaDataChecker)
* **session wrappers** that pin “sane defaults” (DeltaSessionConfig / DeltaParserOptions / DeltaSessionContext / create_session)

All of these live under `deltalake_core::delta_datafusion` (or `deltalake::delta_datafusion` from the meta-crate) and are exported from the module. ([Docs.rs][3])

---

### 2.1 SQL-first registration: `DeltaTableFactory` (TableProviderFactory) + `CREATE EXTERNAL TABLE ... STORED AS DELTATABLE`

DataFusion has an internal map of **table factories**:

```rust
pub fn table_factories(&self) -> &HashMap<String, Arc<dyn TableProviderFactory>>
pub fn table_factories_mut(&mut self) -> &mut HashMap<String, Arc<dyn TableProviderFactory>>
```

([Docs.rs][4])

delta-rs supplies a factory:

```rust
pub struct DeltaTableFactory {}
impl TableProviderFactory for DeltaTableFactory { async fn create(&self, _ctx, cmd: &CreateExternalTable) -> Result<Arc<dyn TableProvider>> { ... } }
```

Its implementation is important:

* If `cmd.options` is empty: it parses `cmd.location` and calls `open_table(table_url).await`
* Else: it calls `open_table_with_storage_options(table_url, cmd.options).await` (i.e., **DDL `OPTIONS(...)` become Delta “storage_options”**) ([Docs.rs][3])

That’s the contract that makes **pure SQL registration** viable:

#### Minimal wiring (Rust)

```rust
use std::sync::Arc;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::SessionContext;

use deltalake_core::delta_datafusion::DeltaTableFactory;

let mut state = SessionStateBuilder::new_with_default_features().build();

// The key must match the SQL `STORED AS <...>` token used by your SQL parser / CLI.
// Conventionally shown as `DELTATABLE` in DataFusion issues / examples.
state
  .table_factories_mut()
  .insert("DELTATABLE".to_string(), Arc::new(DeltaTableFactory {}));

let ctx: SessionContext = state.into();

ctx.sql(r#"
  CREATE EXTERNAL TABLE dt
  STORED AS DELTATABLE
  LOCATION 's3://bucket/schema/table'
"#).await?;
```

**Why this exists:** DataFusion issues explicitly discuss `CREATE EXTERNAL TABLE dt STORED AS DELTATABLE LOCATION 's3://...'` as the desired SQL UX and note that “TableProviderFactory” is the runtime mechanism for custom sources. ([GitHub][5])

#### Passing object store credentials/config through SQL `OPTIONS`

Because `DeltaTableFactory.create(...)` forwards `cmd.options` into `open_table_with_storage_options(...)`, you can thread storage configuration from SQL directly into delta-rs’ table open. ([Docs.rs][3])

Example shape:

```sql
CREATE EXTERNAL TABLE dt
STORED AS DELTATABLE
LOCATION 's3://bucket/schema/table'
OPTIONS(
  'aws.access_key_id' '…',
  'aws.secret_access_key' '…',
  'aws.region' 'us-east-1'
);
```

#### DDL statistics collection knob (DataFusion-side, but impacts Delta too)

DataFusion’s DDL docs note that **`CREATE EXTERNAL TABLE` gathers statistics by default**, and you can disable it via:

```sql
SET datafusion.execution.collect_statistics = false;
```

before issuing DDL. ([Apache DataFusion][6])

#### Distributed planning caveat (Ballista / plan shipping)

DataFusion explicitly flagged that plans involving `TableProviderFactory`-registered tables needed serialization support to be shipped between nodes; see the “plans can’t be serialized” issue context. ([GitHub][7])

---

### 2.2 Scan shaping: `DeltaScanConfig` + `DeltaTableProvider`

If you need more than “register the table and scan,” delta-rs provides a DataFusion-facing provider that adds scan-time knobs.

#### `DeltaScanConfig` fields (what you can control)

```rust
pub struct DeltaScanConfig {
  pub file_column_name: Option<String>,
  pub wrap_partition_values: bool,
  pub enable_parquet_pushdown: bool,
  pub schema_force_view_types: bool,
  pub schema: Option<Arc<Schema>>,
}
```

([Docs.rs][8])

High-signal semantics:

* `file_column_name`: emit an extra column containing the source file path (lineage/debug join key)
* `wrap_partition_values`: controls dictionary wrapping for partition columns (delta-rs uses DataFusion helpers for dict-encoding partition values)
* `enable_parquet_pushdown`: controls whether DataFusion’s Parquet predicate pushdown is enabled in the scan
* `schema_force_view_types`: forces Utf8/Binary “view” types (helps with Arrow large type interop / scan perf in some pipelines)
* `schema`: override the effective read schema (compatible projection/casting)

#### `DeltaTableProvider` constructors

```rust
pub fn try_new(snapshot: EagerSnapshot, log_store: Arc<dyn LogStore>, config: DeltaScanConfig) -> Result<DeltaTableProvider, DeltaTableError>
pub fn with_files(self, files: Vec<Add>) -> DeltaTableProvider
```

([Docs.rs][9])

`with_files(Vec<Add>)` is the “advanced hook” when you want to **externally determine the exact file set** (e.g., you already computed a candidate file list and want to bypass delta-rs’ normal file discovery logic). ([Docs.rs][9])

---

### 2.3 Physical-plan extensions for Delta scanning + serialization codecs

delta-rs doesn’t only provide a TableProvider; it defines a custom scan node and the glue necessary to serialize/deserialize it.

#### `DeltaPhysicalCodec`: PhysicalExtensionCodec for Delta plans

In `delta_datafusion`, `DeltaPhysicalCodec` implements `PhysicalExtensionCodec` and encodes/decodes a `DeltaScan` physical plan using a JSON “wire” representation (`DeltaScanWire`). ([Docs.rs][3])

Key methods:

* `try_decode(buf, inputs, registry)`:

  * `serde_json::from_reader(buf)` → `DeltaScanWire`
  * `DeltaScan::new(&wire.table_url, wire.config, inputs[0].clone(), wire.logical_schema)` ([Docs.rs][3])
* `try_encode(node, buf)`:

  * downcast `ExecutionPlan` to `DeltaScan`
  * serialize `DeltaScanWire::from(delta_scan)` via `serde_json::to_writer(buf, &wire)` ([Docs.rs][3])

This is the “real” integration seam if you’re building:

* a distributed scheduler,
* a plan cache,
* or anything that needs plan serde for Delta scans.

#### `DeltaLogicalCodec`: LogicalExtensionCodec for Delta tables (TableProvider encode/decode)

`DeltaLogicalCodec` implements `LogicalExtensionCodec` and includes the table-provider serde hooks:

* `try_decode_table_provider(buf, ...)` → deserialize `DeltaTable` from JSON bytes and return it as a `TableProvider` ([Docs.rs][3])
* `try_encode_table_provider(table_ref, node, buf)` → downcast to `DeltaTable` and serialize to JSON via `serde_json::to_writer(buf, table)` ([Docs.rs][3])

Note: the logical extension node encode/decode (`try_decode` / `try_encode`) are `todo!()` in the referenced source view, so today the meaningful implemented pieces are the **table-provider serde hooks** and the **physical codec**. ([Docs.rs][3])

---

### 2.4 Data quality enforcement via embedded DataFusion: `DeltaDataChecker`

delta-rs uses DataFusion internally not just for query execution, but also for **validating writes** against:

* constraints,
* invariants,
* generated columns,
* and non-nullability.

`DeltaDataChecker` carries:

```rust
pub struct DeltaDataChecker {
  constraints: Vec<Constraint>,
  invariants: Vec<Invariant>,
  generated_columns: Vec<GeneratedColumn>,
  non_nullable_columns: Vec<String>,
  ctx: SessionContext,
}
```

and its constructors seed the internal DataFusion context from `DeltaSessionContext::default().into()`. ([Docs.rs][3])

It also explicitly cleans up temp state via:

```rust
self.ctx.deregister_table(&table_name)?;
```

as part of its validation flow. ([Docs.rs][3])

This is the practical “why Delta depends on DataFusion” beyond marketing: constraints and merge/update expressions are evaluated by a DataFusion engine embedded in delta-rs. ([Delta][2])

---

### 2.5 Session wrappers: `DeltaSessionConfig` + `DeltaParserOptions` (defaulting discipline)

delta-rs adds thin wrappers that encode “sane defaults” when creating sessions/parsers for Delta workloads:

* `DeltaSessionConfig`: wrapper around DataFusion’s SessionConfig with “sane default table defaults.” ([Docs.rs][10])
* `DeltaParserOptions`: wrapper around DataFusion SQL parser options with “sane default table defaults,” and it implements `From<DeltaParserOptions> for ParserOptions`. ([Docs.rs][11])

These are exported alongside `DeltaSessionContext` and `create_session` from the delta_datafusion module. ([Docs.rs][3])

---

### 2.6 Case sensitivity utilities: `DeltaColumn`

delta-rs includes `DeltaColumn`, “a wrapper for DataFusion’s Column to preserve case-sensitivity during string conversion,” plus `From<&str>`, `From<&String>`, `From<String>`, etc. ([Docs.rs][12])

This exists because DataFusion historically normalizes identifiers; if you are using mixed-case schemas and want predictable column reference behavior, you’ll see this wrapper show up in delta-rs internals. ([Docs.rs][12])

---

## 3) Minimal “full stack” integration patterns (the ones you actually deploy)

### Pattern A: DataFusion Python executes over Delta snapshot (no Spark)

* Register DeltaTable as a provider (`register_table` or `register_table_provider`)
* Run SQL via `ctx.sql(...)` ([Apache DataFusion][1])

### Pattern B: SQL-only table registration (Rust embedding / CLI / server)

* Insert `DeltaTableFactory` into `SessionState.table_factories_mut()`
* Run `CREATE EXTERNAL TABLE ... STORED AS DELTATABLE LOCATION ... OPTIONS(...)` ([Docs.rs][4])

### Pattern C: “scan control plane” (Rust server that exposes file lineage + pushdown policy)

* Build `DeltaTableProvider::try_new(snapshot, log_store, DeltaScanConfig{...})`
* Optionally set `file_column_name` + disable parquet pushdown if you want *only* Delta-level skipping (or vice versa) ([Docs.rs][8])

### Pattern D: plan serialization / distributed execution

* Use `DeltaPhysicalCodec` for physical plan serde of DeltaScan nodes
* Use `DeltaLogicalCodec` table-provider serde to reify DeltaTable providers remotely ([Docs.rs][3])

[1]: https://datafusion.apache.org/python/user-guide/data-sources.html?utm_source=chatgpt.com "Data Sources — Apache Arrow DataFusion documentation"
[2]: https://delta-io.github.io/delta-rs/integrations/delta-lake-datafusion/ "DataFusion - Delta Lake Documentation"
[3]: https://docs.rs/deltalake-core/0.30.1/x86_64-unknown-linux-gnu/src/deltalake_core/delta_datafusion/mod.rs.html "mod.rs - source"
[4]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html "SessionState in datafusion::execution::session_state - Rust"
[5]: https://github.com/apache/arrow-datafusion/issues/2025 "Add `deltalake` feature · Issue #2025 · apache/datafusion · GitHub"
[6]: https://datafusion.apache.org/user-guide/sql/ddl.html?utm_source=chatgpt.com "DDL — Apache DataFusion documentation"
[7]: https://github.com/apache/arrow-datafusion/issues/3906 "Plans with tables from `TableProviderFactory`s can't be serialized · Issue #3906 · apache/datafusion · GitHub"
[8]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaScanConfig.html?utm_source=chatgpt.com "DeltaScanConfig in deltalake::delta_datafusion - Rust"
[9]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaTableProvider.html "DeltaTableProvider in deltalake::delta_datafusion - Rust"
[10]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaSessionConfig.html?utm_source=chatgpt.com "DeltaSessionConfig in deltalake::delta_datafusion - Rust"
[11]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaParserOptions.html?utm_source=chatgpt.com "DeltaParserOptions in deltalake::delta_datafusion - Rust"
[12]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaColumn.html?utm_source=chatgpt.com "DeltaColumn in deltalake::delta_datafusion - Rust"

## Delta Lake ↔ DataFusion: additional integration surfaces (Python + Rust)

---

# 1) Build-time gates (Rust)

## 1.1 `deltalake-core` Cargo features that actually turn on DataFusion interop

`deltalake-core` has an explicit feature gate for DataFusion:

* `datafusion`: enables the `datafusion::datasource::TableProvider` trait impl for Delta tables (query via DataFusion)
* `datafusion-ext`: deprecated alias for `datafusion` ([Docs.rs][1])

This matters because the “delta_datafusion” module surface (providers, codecs, planners) is only meaningful when you build/link with that feature.

---

# 2) Python: embedded DataFusion query engine inside `deltalake` (`QueryBuilder`)

This is separate from `datafusion-python` (you’re not creating a DataFusion `SessionContext` explicitly).

### Surface

`QueryBuilder` is a minimal SQL engine wrapper:

```python
class QueryBuilder:
    def register(self, table_name: str, delta_table: DeltaTable) -> QueryBuilder: ...
    def execute(self, sql: str) -> RecordBatchReader: ...
```

It registers `DeltaTable` objects under names and executes SQL, returning an Arrow `RecordBatchReader`. ([Tessl][2])

---

# 3) Rust: the `delta_datafusion` module is a full “interop control plane”

The canonical, high-signal view is the `deltalake_core::delta_datafusion` module root: it exports a large set of primitives and explicitly defines `PATH_COLUMN`, scan builders/config, planners/codecs, and a “mixins” trait for snapshots. ([Docs.rs][3])

---

## 3.1 Snapshot helpers: `DataFusionMixins` (schema + predicate parsing)

### Trait

`DataFusionMixins` is implemented for both `Snapshot` and `EagerSnapshot` and provides:

* `read_schema() -> ArrowSchemaRef` (DataFusion physical schema)
* `input_schema() -> ArrowSchemaRef` (logical input schema)
* `parse_predicate_expression(expr: impl AsRef<str>, session: &dyn Session) -> DeltaResult<Expr>` (string → DataFusion `Expr`) ([Docs.rs][3])

### Implementation detail

The default `parse_predicate_expression` path converts the read schema into a `DFSchema` and calls `parse_predicate_expression(&schema, expr, session)` (delta-rs’ expression helper) ([Docs.rs][3])

---

## 3.2 Scan schema shaping: partition columns and dictionary encoding

delta-rs constructs a stable Arrow schema that:

* excludes partition columns from the “main” field set and appends them in a stable order
* optionally dictionary-encodes “large” partition types (Utf8/LargeUtf8/Binary/LargeBinary) using DataFusion’s helper `wrap_partition_type_in_dict(...)` ([Docs.rs][3])

This is the mechanism behind `DeltaScanConfig.wrap_partition_values` (see next section).

---

## 3.3 File pruning (Delta log stats → DataFusion `PruningPredicate`)

### `files_matching_predicate(log_data, filters) -> Iterator<Add>`

delta-rs exposes an internal helper that:

1. conjuncts filter expressions (`conjunction(filters.iter().cloned())`)
2. builds a DataFusion physical expr with `SessionContext::new().create_physical_expr(...)`
3. builds a `PruningPredicate::try_new(expr, log_data.read_schema())`
4. calls `pruning_predicate.prune(&log_data)` to produce a boolean mask over candidate files
5. yields only the `Add` actions whose mask bit is true ([Docs.rs][3])

This is the “Delta-specific skipping before Parquet scanning” path: Delta log stats drive file elimination, and Parquet row-group pruning can still happen downstream.

---

## 3.4 File lineage column: `PATH_COLUMN` + `DeltaScanConfig.file_column_name` + `get_path_column`

### Default path column name

delta-rs defines a standard path column constant:

````rust
pub(crate) const PATH_COLUMN: &str = "__delta_rs_path";
``` :contentReference[oaicite:7]{index=7}

### Extractor
`get_path_column(batch, path_column)` expects the path column to be a **dictionary array** (`DictionaryArray<UInt16Type>` → `StringArray`) and returns a typed dictionary array view. :contentReference[oaicite:8]{index=8}

This is tightly coupled with:
- `DeltaScanConfig.file_column_name` (include source path per record) :contentReference[oaicite:9]{index=9}  
- `DeltaScanConfig.wrap_partition_values` (dictionary encoding preference) :contentReference[oaicite:10]{index=10}  

---

## 3.5 Table-level statistics surfaced to DataFusion

`DeltaTableState::datafusion_table_statistics()` returns `Option<Statistics>` computed from `snapshot.log_data().statistics()` :contentReference[oaicite:11]{index=11}

This is the DataFusion-native “table statistics hook” (planner cost + potentially filter selectivity improvements), distinct from Parquet-only stats.

---

## 3.6 Object store binding: Delta log store → DataFusion `RuntimeEnv`

delta-rs registers an object store per table because Delta paths are handled relative to the table root. The helper:

```rust
pub(crate) fn register_store(store: LogStoreRef, env: &RuntimeEnv) {
    let object_store_url = store.object_store_url();
    let url: &Url = object_store_url.as_ref();
    env.register_object_store(url, store.object_store(None));
}
``` :contentReference[oaicite:12]{index=12}

This is the critical seam when:
- DataFusion is executing scans and needs to resolve the table’s object store
- you have multiple tables on different stores/backends in the same runtime env

---

## 3.7 Scan control knobs: `DeltaScanConfig`

### Fields (what you can control)
`DeltaScanConfig` is explicitly designed to “include additional metadata columns during a `DeltaScan`,” and exposes:

- `file_column_name: Option<String>` — include source path for each record  
- `wrap_partition_values: bool` — dictionary encoding for partition values (default true)  
- `enable_parquet_pushdown: bool` — allow scan filter pushdown (default true)  
- `schema_force_view_types: bool` — read Utf8/Binary columns as view types  
- `schema: Option<Arc<Schema>>` — override the read schema (compatible types) :contentReference[oaicite:13]{index=13}

### Constructors / fluent modifiers (exact method names)
- `DeltaScanConfig::new()`
- `DeltaScanConfig::new_from_session(session: &dyn Session)`
- `.with_file_column_name(name)`
- `.with_wrap_partition_values(bool)`
- `.with_parquet_pushdown(bool)`
- `.with_schema(schema)` :contentReference[oaicite:14]{index=14}

### Provider that consumes it
`DeltaTableProvider::try_new(snapshot, log_store, config)` builds a provider; `.with_files(Vec<Add>)` constrains scans to an explicit file set. :contentReference[oaicite:15]{index=15}

---

## 3.8 DataFusion write path via provider: `insert_into` (Append / Overwrite only)

`DeltaTableProvider` implements DataFusion’s `TableProvider` trait and explicitly documents that `insert_into(...)` supports only **Append** and **Overwrite** insert operations. :contentReference[oaicite:16]{index=16}

This is the core “DataFusion can write *some* Delta” boundary: expect `INSERT`/`INSERT OVERWRITE`-style semantics, not Spark-like `MERGE/UPDATE/DELETE` directly via DataFusion SQL.

---

## 3.9 CDF as a DataFusion table: `CdfLoadBuilder` + `DeltaCdfTableProvider`

### `CdfLoadBuilder` (builder methods that matter)
- `with_starting_version(i64)` (defaults to version 0 if not provided)
- `with_ending_version(i64)` (inclusive)
- `with_starting_timestamp(DateTime<Utc>)`
- `with_ending_timestamp(DateTime<Utc>)` (inclusive)
- `with_allow_out_of_range()` (ending version/timestamp can exceed last commit)
- `with_session_state(Arc<dyn Session>)` (inject a DataFusion session state) :contentReference[oaicite:17]{index=17}

### End-to-end CDF provider wiring (Rust)
The `load_cdf` module shows the canonical integration pattern:

```rust
let table = open_table("../path/to/table")?;
let builder = CdfLoadBuilder::new(table.log_store(), table.snapshot())
    .with_starting_version(3);

let ctx = SessionContext::new();
let provider = DeltaCdfTableProvider::try_new(builder)?;
let df = ctx.read_table(provider).await?;
````

([Docs.rs][4])

### DataFusion TableProvider contract participation

`DeltaCdfTableProvider` implements:

* `scan(...)`
* `supports_filters_pushdown(...)`
* `constraints()` (returns DataFusion constraints when available) ([Docs.rs][5])

So CDF is not “just batches”; it’s a first-class DataFusion datasource with pushdown hooks.

---

## 3.10 Embedded DataFusion for constraint/invariant enforcement: `DeltaDataChecker`

### Role

`DeltaDataChecker` validates RecordBatches against:

* invariants
* constraints
* generated columns
* non-nullability

### Execution strategy (important mechanics)

For each batch:

* it registers the `RecordBatch` as a DataFusion `MemTable`
* it chooses a random table name (UUID) to avoid collisions across parallel tasks
* for each check it runs SQL of the shape:

```sql
SELECT <field|*> FROM `<table_name>` WHERE NOT (<check_expression>) LIMIT 1
```

* it collects at most one violating row, formats values via `array_value_to_string`, accumulates violations, and finally deregisters the temp table ([Docs.rs][3])

### Notable limitation encoded in the checker

If a check name contains `.` (nested column), the checker returns an error: nested-column constraints are “not supported at the moment.” ([Docs.rs][3])

---

## 3.11 Custom query planner integration for metrics / custom nodes: `delta_datafusion::planner`

delta-rs exposes “custom planners” so you can convert custom logical nodes into physical plans and trace metrics.

The planner module shows an extension planner example:

* implement `ExtensionPlanner` (e.g., `MergeMetricExtensionPlanner`)
* wire it into a `DeltaPlanner { extension_planner: MergeMetricExtensionPlanner {} }`
* install with `state.with_query_planner(Arc::new(merge_planner))` ([Docs.rs][6])

This is the interop seam when you want:

* custom physical node planning for Delta-specific logical nodes (merge/update flows)
* “operation metrics as planner extensions” rather than post-hoc logs

---

## 3.12 Plan (de)serialization for distributed execution / caching: `DeltaPhysicalCodec` + `DeltaLogicalCodec`

When you need to ship or persist DataFusion plans involving Delta:

* `DeltaPhysicalCodec`: “codec for deltalake physical plans” ([Docs.rs][7])
* `DeltaLogicalCodec`: “does serde on DeltaTables” and implements `LogicalExtensionCodec` including `try_decode_table_provider` / `try_encode_table_provider` ([Docs.rs][8])

This ties directly into `datafusion-proto`’s extension codec system and is the hook you need for multi-process/multi-node execution models.

---

# 4) DataFusion-side tuning that directly affects Delta scans

`DeltaScanConfig.schema_force_view_types` mirrors a DataFusion parquet reader setting:

* `datafusion.execution.parquet.schema_force_view_types` (true by default in the DataFusion docs), controlling whether Utf8/Binary read as view types ([Apache DataFusion][9])

Other DataFusion parquet knobs that matter when you enable Parquet pushdown:

* `datafusion.execution.parquet.pushdown_filters`
* `datafusion.execution.parquet.reorder_filters`
* `datafusion.execution.parquet.force_filter_selections`
* `datafusion.execution.parquet.bloom_filter_on_read`
* `datafusion.execution.parquet.metadata_size_hint` ([Apache DataFusion][9])

In delta-rs, `DeltaScanConfig::new_from_session(session)` is the explicit “bind scan config to session settings” constructor, so you can standardize behavior across services by pinning DataFusion configs once and deriving scan configs from them. ([Docs.rs][10])

[1]: https://docs.rs/deltalake-core?utm_source=chatgpt.com "deltalake_core - Rust"
[2]: https://tessl.io/registry/tessl/pypi-deltalake/1.1.0/files/docs/query-operations.md "tessl/pypi-deltalake@1.1.x - Registry - Tessl"
[3]: https://docs.rs/deltalake-core/0.30.1/x86_64-unknown-linux-gnu/src/deltalake_core/delta_datafusion/mod.rs.html "mod.rs - source"
[4]: https://docs.rs/deltalake/latest/deltalake/operations/load_cdf/index.html "deltalake::operations::load_cdf - Rust"
[5]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaCdfTableProvider.html "DeltaCdfTableProvider in deltalake::delta_datafusion - Rust"
[6]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/planner/index.html "deltalake::delta_datafusion::planner - Rust"
[7]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaPhysicalCodec.html "DeltaPhysicalCodec in deltalake::delta_datafusion - Rust"
[8]: https://docs.rs/deltalake-core/latest/deltalake_core/delta_datafusion/struct.DeltaLogicalCodec.html "DeltaLogicalCodec in deltalake_core::delta_datafusion - Rust"
[9]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[10]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaScanConfig.html "DeltaScanConfig in deltalake::delta_datafusion - Rust"

## Delta Lake ↔ DataFusion: additional integration surfaces and exact APIs

### 1) Python interop is powered by the DataFusion FFI “TableProvider PyCapsule” convention

#### 1.1 The `__datafusion_table_provider__` contract (what DataFusion Python calls)

DataFusion Python’s FFI docs make the convention explicit: to register a Rust-backed provider object in Python, DataFusion expects a method named `__datafusion_table_provider__` that returns a `PyCapsule` containing an `FFI_TableProvider`. ([Apache DataFusion][1])

DataFusion’s “Custom Table Provider” doc shows the canonical implementation shape:

```rust
#[pymethods]
impl MyTableProvider {
  fn __datafusion_table_provider__<'py>(&self, py: Python<'py>)
    -> PyResult<Bound<'py, PyCapsule>> {
    let name = cr"datafusion_table_provider".into();

    let provider = Arc::new(self.clone());
    let provider = FFI_TableProvider::new(provider, false, None);

    PyCapsule::new_bound(py, provider, Some(name.clone()))
  }
}
```

…then in Python:

```python
ctx.register_table("capsule_table", provider)
```

([Apache DataFusion][2])

#### 1.2 `deltalake.DeltaTable` exposes `__datafusion_table_provider__`

The deltalake Python surface documents this method explicitly under “DataFusion Integration”:

```python
def __datafusion_table_provider__(self) -> Any: ...
# Internal method for DataFusion SQL engine integration
```

([Tessl][3])

This is the plumbing that makes DataFusion’s “register a DeltaTable directly” example work:

```python
from datafusion import SessionContext
from deltalake import DeltaTable

ctx = SessionContext()
delta_table = DeltaTable("path_to_table")
ctx.register_table("my_delta_table", delta_table)
ctx.table("my_delta_table").show()
```

([Apache DataFusion][4])

#### 1.3 Fallback registration path (Arrow Dataset bridge)

If you’re on older `deltalake` (docs mention “prior to 0.22”), DataFusion suggests importing via Arrow Dataset (loses capabilities like filter pushdown):

```python
ctx.register_dataset("my_delta_table", delta_table.to_pyarrow_dataset())
```

([Apache DataFusion][4])

---

### 2) “No-path” table discovery: load from catalogs, then register into DataFusion

`deltalake` can resolve a Delta table location through a **data catalog** and return a `DeltaTable` you can register into DataFusion.

#### 2.1 AWS Glue catalog

```python
from deltalake import DeltaTable, DataCatalog

dt = DeltaTable.from_data_catalog(
    data_catalog=DataCatalog.AWS,
    database_name="simple_database",
    table_name="simple_table",
)
```

([Delta][5])

#### 2.2 Databricks Unity Catalog

Auth via env vars `DATABRICKS_WORKSPACE_URL` + `DATABRICKS_ACCESS_TOKEN`, then:

```python
import os
from deltalake import DataCatalog, DeltaTable

os.environ["DATABRICKS_WORKSPACE_URL"] = "https://adb-...azuredatabricks.net"
os.environ["DATABRICKS_ACCESS_TOKEN"] = "<DBAT>"

dt = DeltaTable.from_data_catalog(
    data_catalog=DataCatalog.UNITY,
    data_catalog_id="main",     # UC catalog name
    database_name="db_schema",  # UC schema
    table_name="db_table",      # table
)
```

([Delta][5])

#### 2.3 Register the catalog-resolved table in DataFusion

```python
from datafusion import SessionContext
ctx = SessionContext()
ctx.register_table("t", dt)      # uses dt.__datafusion_table_provider__()
ctx.sql("select count(*) from t").show()
```

(DataFusion registration behavior per TableProvider docs.) ([Apache DataFusion][4])

---

### 3) Storage plumbing: `storage_options` for the log store, *separate* filesystem for bulk Parquet reads

#### 3.1 `storage_options` on `DeltaTable(...)` configures the Delta log/object store

The Python usage docs show constructing a table with `storage_options` (example uses AWS env vars / dict). ([Delta][5])

```python
dt = DeltaTable(
    "s3://bucket/path",
    storage_options={
        "AWS_ACCESS_KEY_ID": "...",
        "AWS_SECRET_ACCESS_KEY": "...",
    },
)
```

([Delta][5])

#### 3.2 Bulk data reads: pass a PyArrow `FileSystem` to `to_pyarrow_dataset(...)`

delta-rs documents “Custom Storage Backends” and shows the required `SubTreeFileSystem` pattern (root must be adjusted to the table root):

```python
import pyarrow.fs as fs
from deltalake import DeltaTable

table_uri = "s3://<bucket>/<path>"
raw_fs, normalized_path = fs.FileSystem.from_uri(table_uri)
filesystem = fs.SubTreeFileSystem(normalized_path, raw_fs)

dt = DeltaTable(table_uri)
ds = dt.to_pyarrow_dataset(filesystem=filesystem)
```

([Delta][5])

This matters for DataFusion in two ways:

* if you’re using the **Dataset fallback** (`register_dataset`), the dataset’s filesystem controls bulk IO. ([Apache DataFusion][4])
* even outside DataFusion, `to_pyarrow_dataset(...)` / `to_pyarrow_table(...)` accept filesystem + filters, and you can hand those Arrow objects back into DataFusion via `ctx.from_arrow(...)` in DataFusion 42+ (Arrow FFI import). ([Apache DataFusion][4])

---

### 4) Safe concurrent writes (critical when DataFusion compute is parallel)

#### 4.1 S3: enable a locking provider (DynamoDB)

delta-rs documents that safe concurrent writes to S3 require an external locking mechanism; DynamoDB is currently the supported lock provider. You enable it via `storage_options` / env vars:

```python
from deltalake import write_deltalake
import pandas as pd

df = pd.DataFrame({"x": [1, 2, 3]})
storage_options = {
  "AWS_S3_LOCKING_PROVIDER": "dynamodb",
  "DELTA_DYNAMO_TABLE_NAME": "custom_table_name",
}

write_deltalake("s3a://path/to/table", df, storage_options=storage_options)
```

It also documents override keys for non-AWS S3 endpoints / separate DynamoDB creds, e.g. `AWS_ENDPOINT_URL_DYNAMODB`, `AWS_REGION_DYNAMODB`, `AWS_ACCESS_KEY_ID_DYNAMODB`, `AWS_SECRET_ACCESS_KEY_DYNAMODB`. ([Delta][6])

You can opt into unsafe writes (not recommended) by setting:

* `AWS_S3_ALLOW_UNSAFE_RENAME=true` ([Delta][6])

#### 4.2 Real-world failure mode: version collisions under concurrent appends

A representative delta-rs Python issue shows concurrent writers (k8s pods) colliding on the same delta log version and eventually failing after retries—exactly the operational symptom you get without a correct locking/coordination strategy. ([GitHub][7])

**Practical integration rule:** treat DataFusion as parallel compute, but serialize/coordinate Delta commits (single writer, lock provider, or upstream commit-coordinator strategy depending on storage).

---

### 5) File/partition selection APIs you can use to build “manual DataFusion scans” (and to pin snapshots)

Even if you register `DeltaTable` directly, you often still want *explicit file lists* for:

* reproducible “pinned snapshot” queries in engines that don’t support Delta providers,
* building your own ListingTable / parquet scan plan.

#### 5.1 `files(...)` / `file_uris(...)` + DNF partition predicate syntax

`DeltaTable.file_uris(partition_filters=...)` and `DeltaTable.files(partition_filters=...)` accept partition filters expressed in **disjunctive normal form**; inner tuples are `(key, op, value)`; op ∈ `=`, `!=`, `in`, `not in`. ([Delta][8])

Examples:

```python
# AND of predicates
partition_filters = [("year", "=", "2023"), ("month", "in", ["01", "02"])]

# OR across conjunction blocks (DNF)
partition_filters = [
  [("year", "=", "2023"), ("month", "=", "01")],
  [("year", "=", "2023"), ("month", "=", "02")],
]

uris = dt.file_uris(partition_filters=partition_filters)
```

([Delta][8])

You can then register those URIs into DataFusion as Parquet (ListingTable path/glob approach) when you’re intentionally bypassing Delta provider semantics.

---

### 6) DataFusion-powered mutation inside `deltalake`: MERGE execution knobs that directly affect planning/memory/pruning

delta-rs’ Python `DeltaTable.merge(...)` is explicitly executed “in Rust with Apache DataFusion query engine” (the TableMerger docs repeat this). ([Delta][9])

#### 6.1 `DeltaTable.merge(...)` signature (high-signal parameters)

From the API reference:

```python
DeltaTable.merge(
  source: ArrowStreamExportable | ArrowArrayExportable,
  predicate: str,
  source_alias: str | None = None,
  target_alias: str | None = None,
  merge_schema: bool = False,
  error_on_type_mismatch: bool = True,
  writer_properties: WriterProperties | None = None,
  streamed_exec: bool = True,
  post_commithook_properties: PostCommitHookProperties | None = None,
  commit_properties: CommitProperties | None = None
) -> TableMerger
```

`streamed_exec=True` executes MERGE using a **LazyMemoryExec** plan to reduce memory pressure for large source tables; enabling `streamed_exec` *implicitly disables source table stats* used to derive an `early_pruning_predicate`. ([Delta][9])

That’s an explicit “DataFusion integration knob”:

* `streamed_exec=False` → allow early pruning (when stats available) at the cost of higher memory.
* `streamed_exec=True` → lower memory footprint, but you may lose early pruning and rely more on downstream pruning.

#### 6.2 CDF read path returns an Arrow stream you can pipe into DataFusion

`DeltaTable.load_cdf(...) -> RecordBatchReader` provides version/timestamp ranges + optional `predicate` and `columns` selection, returning an Arrow RecordBatchReader. ([Delta][9])

Typical “pipe into DataFusion” pattern:

* `reader = dt.load_cdf(...)`
* materialize as Arrow Table / batches (or use DataFusion `from_arrow(...)` if your binding supports Arrow FFI) ([Apache DataFusion][4])

---

### 7) Repair/FSCK as a pre-query step (keep DataFusion scans from exploding)

If your DataFusion queries fail due to missing/corrupted Parquet files that are still “active” in the log, delta-rs provides:

```python
dt.repair(dry_run: bool = False, post_commithook_properties=None, commit_properties=None) -> dict[str, Any]
```

It audits active files and commits a new “FSCK” transaction adding `remove` actions for missing/corrupted files. ([Delta][9])

This is a pragmatic integration surface: **repair first, then query via DataFusion**, instead of letting every scan pay the cost of repeated missing-file failures.

---



[1]: https://datafusion.apache.org/python/contributor-guide/ffi.html "Python Extensions — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/python/user-guide/io/table_provider.html "Custom Table Provider — Apache Arrow DataFusion  documentation"
[3]: https://tessl.io/registry/tessl/pypi-deltalake/1.1.0/files/docs/data-reading.md "tessl/pypi-deltalake@1.1.x - Registry - Tessl"
[4]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[5]: https://delta-io.github.io/delta-rs/python/usage.html "Usage — delta-rs  documentation"
[6]: https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/ "Writing to S3 with a locking provider - Delta Lake Documentation"
[7]: https://github.com/delta-io/delta-rs/issues/3736 "Unable to write concurrently to delta table with python deltalake 1.14.0 library · Issue #3736 · delta-io/delta-rs · GitHub"
[8]: https://delta-io.github.io/delta-rs/python/api_reference.html "API Reference — delta-rs  documentation"
[9]: https://delta-io.github.io/delta-rs/api/delta_table/ "DeltaTable - Delta Lake Documentation"

## 1) Delta protocol + table-feature compatibility (DataFusion + delta-rs): implementation spec

### 1.1 Protocol action: schema, invariants, presence conditions

**Protocol action payload (logical):**

* `minReaderVersion: int`
* `minWriterVersion: int`
* `readerFeatures: [string]` **present iff** `minReaderVersion == 3`
* `writerFeatures: [string]` **present iff** `minWriterVersion == 7` ([GitHub][1])

**Table-features legality constraints:**

* A table may support **writer features only** (`minWriterVersion == 7`, `minReaderVersion ∈ {1,2}`, `protocol` contains `writerFeatures` only). ([GitHub][1])
* A table may support **reader+writer features** (`minWriterVersion == 7`, `minReaderVersion == 3`, `protocol` contains both `readerFeatures` and `writerFeatures`). ([GitHub][1])
* **Illegal state**: `minReaderVersion == 3` with `minWriterVersion < 7`; **illegal state**: feature listed only in `readerFeatures` and not in `writerFeatures`. ([GitHub][1])

**Compatibility contract (normative):**

* `readerFeatures` and `writerFeatures` are **required-implementation sets**; clients **must not ignore** listed features.
* Read compatibility requires implementing **all** features in `readerFeatures`; write compatibility requires implementing **all** features in `writerFeatures`. ([GitHub][1])

**Version-domain constraints (Databricks summary of protocol domains):**

* `minReaderVersion ∈ {1,2,3}`; `minWriterVersion ∈ {2..7}`. ([Databricks Documentation][2])

---

### 1.2 Feature taxonomy: “supported” vs “active”; writer-only vs reader+writer

**Supported feature (spec definition):**

* A feature is “supported by a table” iff its name is in `protocol.readerFeatures` and/or `protocol.writerFeatures`. ([GitHub][1])

**Active feature (spec definition):**

* “Supported” ≠ “active”; activation is feature-specific and generally requires metadata/table-properties preconditions (“metadata requirements”). ([GitHub][1])
  Example pattern (Append-only): supported iff `appendOnly ∈ writerFeatures`; active iff additionally `delta.appendOnly == true`. ([GitHub][1])

**Writer-only feature vs reader+writer feature (protocol-set placement rule):**

* Reader+writer features MUST appear in **both** `readerFeatures` and `writerFeatures`.
* Writer-only features MUST appear **only** in `writerFeatures`. ([GitHub][1])

**Valid feature-name universe (Delta protocol appendix):** ([GitHub][1])
Writers-only:

* `appendOnly`, `invariants`, `checkConstraints`, `generatedColumns`, `allowColumnDefaults`, `changeDataFeed`, `identityColumns`, `materializePartitionColumns`, `rowTracking`, `domainMetadata`, `icebergCompatV1`, `icebergCompatV2`, `clustering`, `inCommitTimestamp`

Readers and writers:

* `columnMapping`, `deletionVectors`, `timestampNtz`, `v2Checkpoint`, `vacuumProtocolCheck`

**delta-rs / deltalake feature vocabulary (Rust enum surface):**

* deltalake `TableFeatures` enumerates a subset (e.g., ColumnMapping, DeletionVectors, TimestampWithoutTimezone, V2Checkpoint, AppendOnly, Invariants, CheckConstraints, ChangeDataFeed, GeneratedColumns, IdentityColumns, RowTracking, DomainMetadata, IcebergCompatV1, MaterializePartitionColumns). ([Docs.rs][3])

---

### 1.3 Minimal protocol requirements by feature (numeric protocol mode + table-feature mode)

Delta Lake publishes a “features by protocol version” mapping (legacy numeric gating). Key rows relevant to DataFusion/delta-rs interoperability: ([Delta Lake][4])

* Basic: `minWriterVersion=2`, `minReaderVersion=1`
* `checkConstraints`: `3 / 1`
* `changeDataFeed`: `4 / 1`
* `generatedColumns`: `4 / 1`
* `columnMapping`: `5 / 2`
* `identityColumns`: `6 / 1`
* Table features (writer-feature framework): `7 / 1`
* Table features (reader-feature framework): `7 / 3`
* `deletionVectors`: `7 / 3`
* `timestampNtz`: `7 / 3`
* `v2Checkpoint`: `7 / 3`
* `vacuumProtocolCheck`: `7 / 3`
* `rowTracking`: `7 / 3`
* (Also listed: `typeWidening` uses `7 / 3` in the same table.) ([Delta Lake][4])

**Interpretation rule for “table features mode”:**

* If a table uses table-feature lists, the table protocol resolves compatibility via `readerFeatures` / `writerFeatures` lists (feature-level gating), not solely via numeric bundling; “supported features appear in respective lists.” ([Databricks Documentation][2])

---

### 1.4 Configuration surfaces (feature enablement → protocol mutation)

#### 1.4.1 Protocol minima as table properties

Delta Lake defines:

* `delta.minReaderVersion` (minimum reader protocol required)
* `delta.minWriterVersion` (minimum writer protocol required) ([Delta Lake][5])

#### 1.4.2 Column mapping: semantics + required configuration knobs

**Function:** decouple logical column names from Parquet column identifiers/names; enables `RENAME COLUMN` / `DROP COLUMNS` without rewriting Parquet files. ([Delta Lake][6])

**Required protocol:**

* Numeric mode: reader ≥ 2; writer ≥ 5. ([Delta Lake][6])
* Table-feature mode: reader version 3 + `columnMapping ∈ readerFeatures`; writer version 7 + `columnMapping ∈ writerFeatures`. ([GitHub][1])

**Required table properties:**

* `delta.columnMapping.mode ∈ {none, name, id}`; enabling example sets:

  * `delta.minReaderVersion=2`
  * `delta.minWriterVersion=5`
  * `delta.columnMapping.mode=name` ([Delta Lake][6])
    **Irreversibility constraint:** disabling after enablement is blocked (“cannot turn off”). ([Delta Lake][6])

**Reader obligations (actionable):** given protocol supports column mapping (reader v2 or reader v3+feature), readers MUST: ([GitHub][1])

* read `delta.columnMapping.mode`
* mode `none`/absent: resolve Parquet columns by **display names** (schema `name`)
* mode `id`: resolve Parquet columns by Parquet `field_id` matched to schema metadata `delta.columnMapping.id`; if file lacks field ids ⇒ reader MUST refuse file or return nulls; missing ids ⇒ return nulls; stats/partition values resolved by **physical names** in log
* mode `name`: resolve Parquet columns by schema metadata `delta.columnMapping.physicalName`; missing columns ⇒ return nulls; stats/partition values resolved by **physical names**

**Writer obligations (actionable):** writers MUST (initial enablement) write `metaData` (`delta.columnMapping.mode`) and, for writer v7, also write `protocol` adding `columnMapping` to both feature lists; write Parquet files using physical names + Parquet `field_id`; log stats/partition values using physical names; maintain `delta.columnMapping.maxColumnId` monotonic. ([GitHub][1])

#### 1.4.3 Deletion vectors: semantics + required configuration knobs

**Function:** “soft delete” representation: mark rows removed without rewriting the entire Parquet file; subsequent reads compute current table state by applying deletions recorded in DVs. ([Delta Lake][7])

**Required protocol (table-feature mode):**

* table MUST be reader v3, writer v7; `deletionVectors` MUST exist in both `readerFeatures` and `writerFeatures`. ([GitHub][1])

**Enablement knob:**

* `delta.enableDeletionVectors = true` (table property). ([Delta Lake][7])
* Enabling upgrades protocol; post-upgrade, clients without DV support cannot read the table (compatibility break). ([Delta Lake][7])

**Reader obligations (“DV logic” = explicit operational requirement):**

* DV files: stored at table root; each DV file contains one or more serialized DVs; each DV describes the set of invalidated rows for an associated data file. ([GitHub][1])
* DV semantics: DV encodes a set of **row indexes** (64-bit ints) where index is Parquet row position starting at 0. ([GitHub][1])
* If snapshot contains logical files with DV-invalidated records, those records **MUST NOT** be returned. ([GitHub][1])
* DV attachment point: `add`/`remove` actions may include a DV descriptor; DV may exist even when `delta.enableDeletionVectors` is not set (reader must consider existence). ([GitHub][1])

**DV descriptor surface (required implementation fields):**

* `storageType ∈ {'u','i','p'}` (UUID-derived relative file; inline; absolute path)
* `pathOrInlineDv` (UUID-ish payload or inline-encoded payload or absolute path)
* `offset` (byte offset, absent for inline)
* `sizeInBytes`
* `cardinality` (# logically removed rows) ([GitHub][1])

**Writer obligations (minimal correctness constraint explicitly stated):**

* When adding a logical file with a DV, the logical file MUST include correct `numRecords` in its `stats`. ([GitHub][1])

---

### 1.5 “ReaderFeatures / writerFeatures minimums” (exact answer to your “min x / min y” requirement)

**To access tables using feature lists (table-features framework):**

* `writerFeatures` list exists only when `minWriterVersion == 7`.
* `readerFeatures` list exists only when `minReaderVersion == 3`.
* Tables with `minReaderVersion == 3` require `minWriterVersion == 7` (cannot have reader-features without writer-features). ([GitHub][1])

**Therefore:**

* Any Delta table requiring any **reader+writer feature** in the table-feature framework (e.g., `deletionVectors`, `timestampNtz`, `v2Checkpoint`, `vacuumProtocolCheck`, `columnMapping` when expressed as a feature) will present as `minReaderVersion=3`, `minWriterVersion=7`, with that feature name present in **both** lists. ([GitHub][1])
* Tables requiring only **writer-only features** under table-features framework can be `minWriterVersion=7`, `minReaderVersion ∈ {1,2}`, and have only `writerFeatures`. ([GitHub][1])

---

### 1.6 Deterministic compatibility procedure (no “example code”; explicit required steps)

**Inputs:**

* table snapshot protocol: (`minReaderVersion`, `minWriterVersion`, optional `readerFeatures[]`, `writerFeatures[]`) ([GitHub][1])
* service capability model: supported numeric protocol maxima + supported feature-name universe (compile-time bound in delta-rs default checker). ([Docs.rs][8])

**Procedure (read path):**

1. Read `minReaderVersion`, `minWriterVersion`.
2. If `minWriterVersion == 7`: load `writerFeatures` set; else infer required feature bundle via numeric protocol table (features-by-protocol mapping). ([GitHub][1])
3. If `minReaderVersion == 3`: load `readerFeatures` set (must be subset of writer-features set by protocol rules); else infer read requirements from numeric protocol table. ([GitHub][1])
4. Validate: `readerFeatures ⊆ supported_reader_feature_set`; `writerFeatures ⊆ supported_writer_feature_set` when lists exist; plus numeric version bounds. (“Must implement and respect all listed features.”) ([GitHub][1])
5. If feature supported but activation depends on metadata, additionally evaluate feature-specific activation predicates (e.g., DV requires/uses `delta.enableDeletionVectors` for *writing*; DV presence in `add` actions still affects *reading*). ([GitHub][1])

**Outputs to persist (control-plane record):**

* `(table_uri, snapshot_version, minReaderVersion, minWriterVersion, readerFeatures[], writerFeatures[], detected_active_feature_flags[], decision)` where `decision ∈ {readable, writable, not-readable, not-writable}` with *reason = first unsupported constraint*. Normative “not-readable/not-writable” boundary derives from protocol feature constraints. ([GitHub][1])

---

### 1.7 “Degrade to Parquet file list scan” (compatibility semantics you must explicitly label)

Delta defines snapshot membership via `_delta_log` (adds/removes + protocol/features); a Parquet directory scan does not apply log reconciliation; therefore output differs whenever tombstones/removes/DVs/column-mapping semantics exist. DV semantics explicitly require skipping DV-invalidated rows; column-mapping semantics explicitly require resolving columns by `field_id`/physical names under configured mapping mode. ([GitHub][1])

[1]: https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md "raw.githubusercontent.com"
[2]: https://docs.databricks.com/aws/en/delta/feature-compatibility "Delta Lake feature compatibility and protocols | Databricks on AWS"
[3]: https://docs.rs/deltalake/latest/deltalake/kernel/enum.TableFeatures.html "TableFeatures in deltalake::kernel - Rust"
[4]: https://docs.delta.io/versioning/ "How does Delta Lake manage feature compatibility? | Delta Lake"
[5]: https://docs.delta.io/table-properties/ "Delta Table Properties Reference | Delta Lake"
[6]: https://docs.delta.io/delta-column-mapping/ "Delta column mapping | Delta Lake"
[7]: https://docs.delta.io/delta-deletion-vectors/ "What are deletion vectors? | Delta Lake"
[8]: https://docs.rs/deltalake/latest/deltalake/kernel/transaction/static.PROTOCOL.html?utm_source=chatgpt.com "PROTOCOL in deltalake::kernel::transaction - Rust"

## 2) Snapshot model, time travel, retention: implementation spec (delta-rs + DataFusion)

### 2.1 Snapshot object model: loaded-state invariants, accessors, failure modes

**DeltaTable state topology (Rust, deltalake):**

* `DeltaTable` is a logical handle; concrete read planning requires a loaded snapshot (“state”). ([Docs.rs][1])
* `DeltaTable.state: Option<DeltaTableState>`; absent state implies “uninitialized” for snapshot-dependent operations. ([Docs.rs][1])

**State-dependent accessors (hard requirements):**

* `DeltaTable::snapshot() -> Result<&DeltaTableState, DeltaTableError>`:

  * returns `NotInitialized` when state is absent. ([Docs.rs][1])
* `DeltaTable::version() -> Option<i64>`:

  * `None` iff table not loaded; otherwise returns currently loaded version. ([Docs.rs][1])

**State loaders (version selection primitives):**

* `load(&mut self)`: load latest (checkpoint + apply newer versions). ([Docs.rs][1])
* `load_version(&mut self, version: i64)`: load exact version state. ([Docs.rs][1])
* `load_with_datetime(&mut self, datetime: DateTime<Utc>)`: load “latest version at or before datetime”; implementation performs binary search over transaction logs. ([Docs.rs][1])

---

### 2.2 Time-travel address spaces: version pin vs timestamp pin; resolution semantics

**Two externally exposed time coordinates:**

1. **Version pin**: exact integer version (`i64`) selecting a specific snapshot. ([Docs.rs][2])
2. **Timestamp pin**: RFC3339 / ISO-8601 timestamp selecting the greatest committed version whose commit-time ≤ timestamp (“at or before” semantics). ([Docs.rs][3])

**Resolution rule (timestamp → version):**

* `open_table_with_ds(…, ds)` and `load_with_datetime(…, datetime)` both resolve by selecting the latest version at or before the provided time coordinate; `load_with_datetime` explicitly states binary search across logs. ([Docs.rs][3])

**Operational artifact (for reproducibility):**

* Timestamp pins are *indirect* selectors; reproducible replay requires persisting the **resolved version** (integer) obtained after timestamp resolution (since the snapshot that actually executes is version-addressed). (Resolution semantics cited above.) ([Docs.rs][3])

---

### 2.3 Rust API surfaces for time travel (construction-time vs mutation-time)

#### 2.3.1 Construction-time loaders (produce a DeltaTable already pinned)

* `open_table_with_version(table_url: Url, version: i64) -> Result<DeltaTable, DeltaTableError>`:

  * constructs a table handle and loads metadata for the specified version. ([Docs.rs][2])
* `open_table_with_ds(table_url: Url, ds: impl AsRef<str>) -> Result<DeltaTable, DeltaTableError>`:

  * loads metadata from the version appropriate for the provided ISO-8601/RFC-3339 timestamp string. ([Docs.rs][3])

#### 2.3.2 Mutation-time loaders (re-pin an existing DeltaTable handle)

* `DeltaTable::load_version(&mut self, version: i64) -> Result<(), DeltaTableError>`: exact pin. ([Docs.rs][1])
* `DeltaTable::load_with_datetime(&mut self, datetime: DateTime<Utc>) -> Result<(), DeltaTableError>`:

  * timestamp pin; “at or before” semantics; binary search over logs. ([Docs.rs][1])

#### 2.3.3 Provenance window (log-retained metadata)

* `DeltaTable::history(limit)`:

  * “history retention is based on `logRetentionDuration`, 30 days by default” (limits how far commit provenance is available via this API if older log entries have been cleaned). ([Docs.rs][1])

---

### 2.4 Provider-level time travel (DataFusion TableProvider pinning without pre-loading)

**Builder existence and return path:**

* `DeltaTable::table_provider(&self) -> TableProviderBuilder`. ([Docs.rs][1])

**Builder inputs (explicit configuration degrees-of-freedom):**

* `TableProviderBuilder` fields include `log_store`, `snapshot`, `file_column`, `table_version: Option<Version>`. ([Docs.rs][4])
* The builder can be built from:

  * log store, snapshot, or eager snapshot; if snapshot provided, builder states “no IO will be performed when building the provider.” ([Docs.rs][4])

**Version pinning knob (provider-time):**

* `TableProviderBuilder::with_table_version(version: impl Into<Option<Version>>) -> Self`:

  * configures which table version the provider should target. ([Docs.rs][4])

**Resolution mechanism (what “with_table_version” actually controls):**

* If no snapshot/eager snapshot is provided, builder constructs `Snapshot::try_new(log_store, Default::default(), table_version.map(|v| v as i64))` (version passed into snapshot creation). ([Docs.rs][4])

**Implication for deterministic query planning:**

* Provider snapshot selection is a function of `(snapshot provided?) ∨ (log_store + optional table_version)`; the only stable identifier exposed by the builder for pinning is `table_version`. ([Docs.rs][4])

---

### 2.5 Retention model: log retention vs deleted-file retention; vacuum invalidation mechanics

**Two independent retention regimes (Delta table properties):**

* `delta.logRetentionDuration`: retention horizon for transaction log history; Delta cleans up log entries older than this interval during checkpointing. ([Delta Lake][5])
* `delta.deletedFileRetentionDuration`: eligibility threshold for VACUUM to physically delete data files after logical removal; default `interval 7 days`. ([Delta Lake][6])

**Vacuum semantics (time travel invalidation condition):**

* Vacuum physically deletes data files that older table versions may still reference; consequently, time travel to versions requiring those files becomes impossible after vacuum has removed them. ([delta.io][7])
* delta-rs documentation explicitly: vacuum “may make some past versions … invalid” and “can break time travel”; it retains files within a window (default one week). ([delta-io.github.io][8])
* Delta utility docs: time travel to versions older than the retention period is lost after running vacuum. ([Delta Lake][9])

**Retention configuration requirement for “time-travel window W”:**

* For intended historical accessibility window `W`:

  * set `delta.deletedFileRetentionDuration >= W` to prevent vacuum from deleting required data files within `W`. ([Delta Lake][6])
  * set `delta.logRetentionDuration >= W` to retain log provenance/versions across `W` (log cleanup otherwise truncates history). ([Delta Lake][5])

---

### 2.6 “Pinned snapshot bundle” schema (reproducibility + cache keys)

#### 2.6.1 Minimal bundle (sufficient to re-open identical snapshot, assuming retention holds)

* `table_uri: string` (canonical URL / path)
* `resolved_version: int64` (required; even if original selector was timestamp) ([Docs.rs][3])
* `selector`:

  * `version: int64 | null`
  * `timestamp_rfc3339: string | null` ([Docs.rs][3])
* `load_api`:

  * `{open_table_with_version | open_table_with_ds | load_version | load_with_datetime}` (auditability of resolution pathway) ([Docs.rs][2])

#### 2.6.2 Compatibility envelope (bind snapshot to protocol/features at that version)

* `protocol.minReaderVersion: int`
* `protocol.minWriterVersion: int`
* `protocol.readerFeatures: [string] | null`
* `protocol.writerFeatures: [string] | null`
  (Protocol extraction mechanism is part of the feature-compatibility section; version pin ensures protocol/features are evaluated at the pinned snapshot’s log.) ([Docs.rs][4])

#### 2.6.3 Snapshot identity hash (detect drift / partial corruption)

Define a hash `H(snapshot)` over canonicalized snapshot metadata sufficient to detect changed membership:

* `H = hash( resolved_version || canonical(metadata) || canonical(protocol) || canonical(active_add_files) )`
* `active_add_files` should be canonicalized as an ordered list of `{path, size, modificationTime?, partitionValues?, stats?}` as available; the table provider ultimately plans scans from active logical files (DataFusion planning path consumes snapshot/log store). ([Docs.rs][4])

(“Hash function choice” intentionally unspecified; requirement is stable canonicalization + stable hashing.)

---

### 2.7 Deterministic procedure: pin, register, execute, cache, validate retention

**Inputs:** `table_uri`, `time_selector ∈ {version:int64 | timestamp_rfc3339:string}`, `required_history_window W`.

**Procedure:**

1. **Resolve and pin snapshot**

   * If `version`: use `open_table_with_version(Url, i64)` or `DeltaTable::load_version(i64)`. ([Docs.rs][2])
   * If `timestamp`: use `open_table_with_ds(Url, RFC3339)` or `DeltaTable::load_with_datetime(DateTime<Utc>)`; record `resolved_version = DeltaTable::version()` post-load. ([Docs.rs][3])
2. **Validate initialization**

   * Assert `snapshot()` succeeds (reject `NotInitialized`). ([Docs.rs][1])
3. **Bind DataFusion provider to pinned version**

   * Use `table_provider()`; if not relying on an eager snapshot, set builder `with_table_version(Some(v))` prior to building provider so snapshot creation is parameterized by that version. ([Docs.rs][1])
4. **Retention policy alignment**

   * Ensure `delta.deletedFileRetentionDuration >= W` and `delta.logRetentionDuration >= W`; otherwise, time-travel replays beyond the configured horizons are not preserved against vacuum / log cleanup. ([Delta Lake][6])
5. **Persist bundle**

   * Persist the pinned snapshot bundle (2.6) alongside cache artifacts; cache key must include `(table_uri, resolved_version)` at minimum because snapshot membership is version-indexed. ([Docs.rs][2])

[1]: https://docs.rs/deltalake/latest/deltalake/table/struct.DeltaTable.html "DeltaTable in deltalake::table - Rust"
[2]: https://docs.rs/deltalake/latest/deltalake/fn.open_table_with_version.html "open_table_with_version in deltalake - Rust"
[3]: https://docs.rs/deltalake/latest/deltalake/fn.open_table_with_ds.html "open_table_with_ds in deltalake - Rust"
[4]: https://docs.rs/deltalake-core/0.30.1/x86_64-unknown-linux-gnu/src/deltalake_core/delta_datafusion/table_provider.rs.html "table_provider.rs - source"
[5]: https://docs.delta.io/table-properties/?utm_source=chatgpt.com "Delta Table Properties Reference"
[6]: https://docs.delta.io/delta-batch/?utm_source=chatgpt.com "Table batch reads and writes"
[7]: https://delta.io/blog/2023-02-01-delta-lake-time-travel/?utm_source=chatgpt.com "Delta Lake Time Travel"
[8]: https://delta-io.github.io/delta-rs/python/usage.html?utm_source=chatgpt.com "Usage — delta-rs documentation"
[9]: https://docs.delta.io/delta-utility/?utm_source=chatgpt.com "Table utility commands"

## 3) Delta → DataFusion scan plan materialization (scan() mechanics): implementation spec

### 3.1 DataFusion `TableProvider::scan` contract: inputs, pushdown handshake, correctness guardrails

**Core interface (DataFusion):**

* `scan(session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> ExecutionPlan` (async) (table provider is responsible for producing a scan plan consistent with these arguments). ([Docs.rs][1])
* `filters` semantics: `filters` are boolean `Expr`s combined with logical `AND`; returned rows must satisfy all provided expressions. ([Docs.rs][1])
* `supports_filters_pushdown(filter: &[&Expr]) -> Vec<TableProviderFilterPushDown>`: per-filter capability declaration; determines whether DataFusion supplies `filters` into `scan` and whether DataFusion re-applies a filter above the scan. ([Docs.rs][1])

**Pushdown levels (DataFusion):**

* `Exact`: provider guarantees complete application of the filter during scan; DataFusion does not need to re-apply. ([Apache DataFusion][2])
* `Inexact`: provider may apply partially; DataFusion re-applies after scan to ensure correctness. ([Apache DataFusion][2])

**Statistics side-effect (DataFusion file scans):**

* DataFusion’s file-scan statistics access path marks stats as *inexact* when filters are present in the file scan configuration, explicitly to preserve correctness while enabling pushdown. ([Apache DataFusion][3])

---

### 3.2 delta-rs entrypoints: `scan()` call graph and primary builder objects

**Provider entrypoints (delta-rs / deltalake-core):**

* `DeltaTableProvider::scan(…, projection, filters, limit)`:

  * registers object store into runtime env (`register_store`)
  * forms `filter_expr = conjunction(filters)`
  * constructs `DeltaScanBuilder` and calls `.with_projection(projection).with_limit(limit).with_filter(filter_expr).with_scan_config(config).build()` ([Docs.rs][4])
* Equivalent path exists for `impl TableProvider for DeltaTable` using `DeltaScanBuilder::new(self.snapshot()?.snapshot(), …)` and `.build().await`. ([Docs.rs][4])

**Builder precondition (protocol gate):**

* `DeltaScanBuilder::build` begins with `PROTOCOL.can_read_from(self.snapshot)?` (scan construction is gated on protocol compatibility). ([Docs.rs][4])

---

### 3.3 Scan configuration surface: `DeltaScanConfig` fields, defaults, and session-derived knobs

**Config struct (delta-rs):**

* `file_column_name: Option<String>`: injects a synthetic column containing the source path. ([Docs.rs][5])
* `wrap_partition_values: bool`: dictionary-encodes partition values (and the optional file-path column value) when injected. ([Docs.rs][5])
* `enable_parquet_pushdown: bool`: gates whether a predicate is attached to the Parquet scan (`ParquetSource::with_predicate`). ([Docs.rs][5])
* `schema_force_view_types: bool`: forces Parquet reader view types for string/binary families. ([Docs.rs][5])
* `schema: Option<SchemaRef>`: read-as schema override (names must match table schema; types may be compatible variants). ([Docs.rs][5])

**Default + session-derived values (delta-rs):**

* `DeltaScanConfig::new()` sets `enable_parquet_pushdown=true`, `wrap_partition_values=true`, `schema_force_view_types=true`. ([Docs.rs][4])
* `DeltaScanConfig::new_from_session(session)` sets:

  * `enable_parquet_pushdown = session.config().options().execution.parquet.pushdown_filters`
  * `schema_force_view_types = session.config().options().execution.parquet.schema_force_view_types` ([Docs.rs][4])

---

### 3.4 Schema materialization: logical schema vs file schema; projection-index handling

**Base read schema selection (delta-rs):**

* `schema = config.schema.unwrap_or(snapshot.read_schema())` (table schema, optionally overridden). ([Docs.rs][4])
* `logical_schema = df_logical_schema(snapshot, config.file_column_name, Some(schema))` (adds partition columns + optional file path column into the DataFusion-visible schema). ([Docs.rs][4])

**Projection construction (delta-rs):**

* If `projection` is provided:

  * build field list from `logical_schema.field(idx)` for each projected index
  * additionally, *re-inject* any columns referenced by the filter expression that were not included in `projection` (“partition filters with Exact pushdown were removed from projection by DF optimizer … add them back for predicate pruning”). ([Docs.rs][4])

**File schema separation (delta-rs):**

* `file_schema` is constructed by removing table partition columns from the chosen `schema` (`filter(|f| !partition_cols.contains(f.name()))`). ([Docs.rs][4])
* Partition columns are not read from Parquet; they are injected as partition values through `FileScanConfigBuilder::with_table_partition_cols`. ([Docs.rs][4])

**Protocol-level justification (Delta spec):**

* Partition directory structure is conventional; *actual partition values for a file must be read from the transaction log*. ([GitHub][6])

---

### 3.5 Filter handling pipeline: normalization, pushdown classification, and “pushdown_filter” construction

**Normalization steps (delta-rs):**

* `filter_expr = conjunction(filters.iter().cloned())` at provider entrypoint. ([Docs.rs][4])
* `logical_filter = filter_expr.map(|expr| simplify_expr(session, &df_schema, expr))`:

  * `simplify_expr` uses `ExprSimplifier` then `create_physical_expr` (physical expression for pruning/evaluation). ([Docs.rs][4])

**Pushdown classification (delta-rs): `get_pushdown_filters`:**

* Inputs: `filter: &[&Expr]`, `partition_cols: &[String]`
* Output: `Vec<TableProviderFilterPushDown>`
* Rule:

  * if `expr.column_refs()` non-empty AND `expr_is_exact_predicate_for_cols(partition_cols, expr)` then `Exact`
  * else `Inexact` ([Docs.rs][4])

**Exact predicate recognizer (delta-rs): `expr_is_exact_predicate_for_cols`:**

* Column restriction: every `Expr::Column` name must be contained in `partition_cols`; otherwise predicate deemed not applicable. ([Docs.rs][4])
* Allowed operator / node subset (passes through as “continue”):

  * `Expr::BinaryExpr` with `op ∈ {And, Or, NotEq, Eq, Gt, GtEq, Lt, LtEq}` ([Docs.rs][4])
  * `Expr::Literal`, `Expr::Not`, `Expr::IsNotNull`, `Expr::IsNull`, `Expr::Between`, `Expr::InList` ([Docs.rs][4])
* Any other expression node => `is_applicable = false` and stop traversal. ([Docs.rs][4])

**Construction of `pushdown_filter` (delta-rs):**

* `pushdown_filter` is built by:

  * splitting conjunction into predicates
  * computing `get_pushdown_filters` for each predicate
  * retaining only predicates labeled `Inexact`
  * recombining with `conjunction(filtered_predicates)`
  * simplifying into a physical expr via `simplify_expr` ([Docs.rs][4])

**Explicit intent note (delta-rs):**

* Comment states: “only inexact filters should be pushed down to the data source; doing otherwise will make stats inexact and disable datafusion optimizations like AggregateStatistics.” ([Docs.rs][4])

**Correctness contract coupling (DataFusion):**

* Any filter labeled `Inexact` can be partially applied at scan; DataFusion will re-apply above the scan. ([Apache DataFusion][2])

---

### 3.6 File skipping (“container pruning”) pipeline: Delta-log stats + DataFusion `PruningPredicate`

#### 3.6.1 Per-file stats source (Delta protocol): what exists and what is safe to use

**Transaction-log stats availability:**

* `add` and `remove` actions can carry `stats` (per-file statistics), including `numRecords` and per-column statistics; spec states they “can be used for eliminating files based on query predicates.” ([GitHub][6])
* Checkpoint schema includes `stats_parsed` with `numRecords`, `minValues`, `maxValues`, `nullCount` structures mirroring the data schema. ([GitHub][6])

**`tightBounds` semantics (Delta protocol):**

* `stats.tightBounds` indicates whether per-column min/max are **tight** (true min/max of valid rows) or **wide** (bounds that still safely envelope valid values); wide bounds are explicitly described as sufficient for data skipping. ([GitHub][6])

**Deletion vectors interaction (Delta protocol):**

* For any logical file where `deletionVector` is not null, `numRecords` must be present and accurate (physical record count), and statistics may be outdated w.r.t. deleted rows; `tightBounds` indicates tight vs wide bounds. ([GitHub][6])
* `nullCount` carries only two “strong” cases (`0` and `numRecords`); intermediate values are non-informative and should be treated as absent. ([GitHub][6])

#### 3.6.2 delta-rs pruning execution (DeltaScanBuilder)

**Pruning mask construction (delta-rs):**

* `num_containers = snapshot.num_containers()`
* If `logical_filter` exists:

  * `PruningPredicate::try_new(predicate, logical_schema)`
  * `files_to_prune = pruning_predicate.prune(snapshot)?` (boolean mask length `num_containers`) ([Docs.rs][4])
* Else: `files_to_prune = vec![true; num_containers]` ([Docs.rs][4])

**PruningPredicate semantics (DataFusion):**

* `PruningPredicate` uses min/max (and related pruning statistics) to prove a predicate can never evaluate to true for a container (row group, file, “container”); proven-false containers are skipped. ([Docs.rs][7])

**Early return bypass (delta-rs):**

* If `logical_filter.is_none() && limit.is_none()`, scan enumerates all files via `snapshot.file_views(&log_store, None)` and collects `add_action` list. ([Docs.rs][4])

---

### 3.7 Limit propagation at file-selection layer (delta-rs): `num_records`-based file truncation

**Algorithm (delta-rs):**

* Iterate `(action, keep)` over `file_actions.zip(files_to_prune)`
* If `keep`:

  * If `limit` present:

    * If `action.get_stats()?` yields stats and `rows_collected <= limit`: accumulate `rows_collected += stats.num_records`, push file; else break.
    * If stats missing: defer by pushing file into `pruned_without_stats`. ([Docs.rs][4])
  * Else (no limit): push file. ([Docs.rs][4])
* After loop: if `limit` present AND `rows_collected < limit`, extend `files` with `pruned_without_stats`. ([Docs.rs][4])

**Derived counters (delta-rs):**

* `files_scanned = files.len()`
* `files_pruned = num_containers - files_scanned` ([Docs.rs][4])

---

### 3.8 FileGroup construction + `PartitionedFile` encoding: partition values, optional path column, and dictionary wrapping

**Grouping key (delta-rs):**

* `file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>>`, keyed by a file’s partition value vector. ([Docs.rs][4])

**PartitionedFile construction (delta-rs):**

* `partitioned_file_from_action(action, partition_columns, schema)`:

  * maps `action.partition_values[part]` (string or null) to `ScalarValue` using schema field data type (`to_correct_scalar_value`, null handling) ([Docs.rs][4])
  * sets `ObjectMeta.last_modified` from `action.modification_time` (ms) converted to `DateTime<Utc>` ([Docs.rs][4])

**Optional file-path column injection (delta-rs):**

* If `config.file_column_name.is_some()`:

  * push additional partition value containing `action.path` (string)
  * if `wrap_partition_values`, wrap via `wrap_partition_value_in_dict` else raw `ScalarValue::Utf8` ([Docs.rs][4])

**Partition column field list for `FileScanConfig` (delta-rs):**

* `table_partition_cols = partition_columns.map(|name| schema.field_with_name(name))`
* If `file_column_name` present:

  * add a `Field(file_column_name, dtype)` where `dtype = dict(Utf8)` if `wrap_partition_values` else `Utf8` ([Docs.rs][4])

**Empty-file-set guard (delta-rs):**

* If `file_groups.is_empty()`, builder emits at least one empty `FileGroup` to satisfy DataFusion sanity checks. ([Docs.rs][4])

---

### 3.9 Scan-time statistics injection (delta-rs): pruned vs unpruned stats; fallback behavior

**Two-path stats computation (delta-rs):**

* If `pruning_mask` exists:

  * iterate over `snapshot.files()?` (record batches of file metadata)
  * apply boolean mask slicing; `filter_record_batch`
  * compute stats via `LogDataHandler::new(&pruned_batches, table_configuration).statistics()` ([Docs.rs][4])
* Else:

  * `stats = snapshot.log_data().statistics()` ([Docs.rs][4])
* If `stats` absent: `Statistics::new_unknown(&schema)` ([Docs.rs][4])

**Rationale coupling (DataFusion):**

* When file scan config carries filters, DataFusion treats scan statistics as inexact; delta-rs avoids pushing `Exact` filters into the file scan (and only pushes `Inexact`) to prevent avoidable inexactness propagation for plan-level optimizers. ([Docs.rs][4])

---

### 3.10 Parquet predicate wiring: `DeltaScanConfig.enable_parquet_pushdown` → `ParquetSource::with_predicate`

**ParquetSource creation (delta-rs):**

* `parquet_options.global = session.config().options().execution.parquet.clone()`
* `file_source = ParquetSource::new(parquet_options)` ([Docs.rs][4])

**Predicate attachment (delta-rs):**

* If `pushdown_filter` exists AND `config.enable_parquet_pushdown == true`:

  * `file_source = file_source.with_predicate(predicate)` ([Docs.rs][4])

**Schema adaptation hook (delta-rs):**

* `file_source.with_schema_adapter_factory(Arc::new(DeltaSchemaAdapterFactory {}))?` (schema reconciliation between Delta logical schema and Parquet physical schema). ([Docs.rs][4])

**Downstream Parquet pruning context (DataFusion):**

* DataFusion Parquet implementation advertises row-group and data-page pruning on min/max statistics and predicate pushdown capabilities. ([Apache DataFusion][8])

---

### 3.11 Final plan assembly: `FileScanConfigBuilder` → `DataSourceExec` → `DeltaScan`

**FileScanConfig assembly (delta-rs):**

* `FileScanConfigBuilder::new(object_store_url, file_schema, file_source)`
* `.with_file_groups(Vec<FileGroup>)`
* `.with_statistics(stats)`
* `.with_projection_indices(projection)`
* `.with_limit(limit)`
* `.with_table_partition_cols(table_partition_cols)`
* `.build()` ([Docs.rs][4])

**ExecutionPlan wrapper (delta-rs):**

* `DeltaScan { table_url, config, parquet_scan: DataSourceExec::from_data_source(file_scan_config), logical_schema, metrics }` ([Docs.rs][4])
* `DeltaScan` implements `ExecutionPlan` and delegates schema/properties/children to the wrapped Parquet scan. ([Docs.rs][4])

---

### 3.12 Scan metrics: `files_scanned` / `files_pruned` counters + verification harness specification

**Counters emitted (delta-rs):**

* `ExecutionPlanMetricsSet` with global counters:

  * `"files_scanned"` += `files_scanned`
  * `"files_pruned"` += `files_pruned` ([Docs.rs][4])

**Verification harness (artifact spec):**

1. **Plan capture**

   * Collect DataFusion physical plan (`EXPLAIN` / physical plan API) and assert presence of `DeltaScan` node (display name) and that the child is a file scan (`DataSourceExec`-backed). ([Docs.rs][4])
2. **Pushdown classification audit**

   * For each conjunct in the user predicate:

     * compute whether it is `Exact` or `Inexact` via the same rules used by `get_pushdown_filters` (partition-column-only + restricted operator/node subset → `Exact`, else `Inexact`). ([Docs.rs][4])
   * Assert that only `Inexact` conjuncts are passed into ParquetSource predicate when `enable_parquet_pushdown=true`. ([Docs.rs][4])
3. **File skipping audit**

   * Extract `files_scanned` / `files_pruned` from the executed plan’s metrics; reconcile:

     * `files_scanned + files_pruned == snapshot.num_containers()` when pruning path is active (mask computed). ([Docs.rs][4])
4. **Stats correctness invariants**

   * When deletion vectors are present, ensure `numRecords` is populated for affected logical files; rely on `tightBounds` semantics when interpreting min/max for skipping safety. ([GitHub][6])

[1]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html?utm_source=chatgpt.com "TableProvider in datafusion::datasource - Rust"
[2]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html?utm_source=chatgpt.com "Custom Table Provider — Apache DataFusion documentation"
[3]: https://datafusion.apache.org/library-user-guide/upgrading.html?utm_source=chatgpt.com "Upgrade Guides — Apache DataFusion documentation"
[4]: https://docs.rs/deltalake-core/0.30.1/x86_64-unknown-linux-gnu/src/deltalake_core/delta_datafusion/table_provider.rs.html "table_provider.rs - source"
[5]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaScanConfig.html?utm_source=chatgpt.com "DeltaScanConfig in deltalake::delta_datafusion - Rust"
[6]: https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md "raw.githubusercontent.com"
[7]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/pruning/struct.PruningPredicate.html "https://docs.rs/datafusion/latest/datafusion/physical_optimizer/pruning/struct.PruningPredicate.html"
[8]: https://datafusion.apache.org/user-guide/features.html "https://datafusion.apache.org/user-guide/features.html"

## 4) Provider construction patterns (Delta → DataFusion): API contracts, composition rules, and footgun-avoidance

### 4.1 Construction surface area: *what* can be registered, and *what each form offers*

**Delta-as-DataFusion integration unit:** a `TableProvider` implementation returning an `ExecutionPlan` in `scan()`; DataFusion’s built-in comparators are `ListingTable` (file listing + format readers) and `MemTable` (in-memory batches). ([Docs.rs][1])

**delta-rs offers three provider-shaped entrypoints (Rust):**

1. `DeltaTable::table_provider() -> TableProviderBuilder` (builder produces `Arc<dyn TableProvider>`). ([Docs.rs][2])
2. `DeltaTableProvider` (explicit provider struct; accepts `EagerSnapshot`, `LogStore`, `DeltaScanConfig`; supports optional file subsetting). ([Docs.rs][3])
3. `impl TableProvider for DeltaTable` (direct trait impl; exposes schema/scan/etc). ([Docs.rs][4])

**Feature deltas vs a `ListingTable` on Parquet directories (alternative):**

* Delta providers resolve **active files + partition values + scan stats** via the Delta log/snapshot; `ListingTable` resolves files via directory listing and does not interpret `_delta_log`. (Delta-vs-parquet semantics are covered elsewhere; here the operational consequence is “provider chooses membership source.”) ([Docs.rs][5])

---

### 4.2 `DeltaTable::table_provider()` selection semantics: eager-snapshot fast path vs log-store path

**Method signature and intent (public API):**

* `pub fn table_provider(&self) -> TableProviderBuilder` with docstring “Get a table provider … see `TableProviderBuilder` for options.” ([Docs.rs][2])

**Dispatch rule (source-level behavior):**

* If `self.snapshot()` succeeds, builder is initialized with `with_eager_snapshot(state.snapshot().clone())`.
* Else builder is initialized with `with_log_store(self.log_store())`. ([Docs.rs][4])

**Offered value (vs alternatives):**

* **Eager snapshot path:** binds provider to a preloaded `EagerSnapshot` (no additional snapshot IO at provider build); alternative is log-store path (provider constructs `Snapshot` during build). ([Docs.rs][4])
* **Log store path:** requires only `LogStore` access and (optionally) a version selector; alternative is providing a concrete snapshot yourself (via `with_snapshot`/`with_eager_snapshot`). ([Docs.rs][4])

---

### 4.3 `TableProviderBuilder`: configuration degrees-of-freedom and their semantics

**Builder state (struct fields):**

* `log_store: Option<Arc<dyn LogStore>>`
* `snapshot: Option<SnapshotWrapper>` where wrapper is either `EagerSnapshot` or `Snapshot`
* `file_column: Option<String>`
* `table_version: Option<Version>` ([Docs.rs][4])

**Configuration methods (public):**

* `with_log_store(log_store: impl Into<Arc<dyn LogStore>>)` ([Docs.rs][4])
* `with_eager_snapshot(snapshot: impl Into<Arc<EagerSnapshot>>)` ([Docs.rs][4])
* `with_snapshot(snapshot: impl Into<Arc<Snapshot>>)` ([Docs.rs][4])
* `with_table_version(version: impl Into<Option<Version>>)` ([Docs.rs][4])
* `with_file_column(file_column: impl ToString)` (adds a column containing the source file path). ([Docs.rs][4])

**Offered value (vs alternatives):**

* `with_eager_snapshot` / `with_snapshot`: separates “snapshot acquisition” from “provider construction”; provider build performs no additional snapshot IO when snapshot wrapper is provided. ([Docs.rs][4])
* `with_table_version`: declares a deterministic snapshot selector at provider-build time when snapshot wrapper is absent; alternative is “latest snapshot” selection (default). ([Docs.rs][4])
* `with_file_column`: exposes provenance required for downstream debugging / lineage / UDF diagnostics; alternative is external file-path enrichment outside the engine (not provider-native). ([Docs.rs][4])

---

### 4.4 Build semantics: `IntoFuture` → provider materialization rules, error conditions, and default scan config

**Builder execution protocol:**

* `TableProviderBuilder` implements `std::future::IntoFuture` with `Output = Result<Arc<dyn TableProvider>>` and returns a boxed async future. ([Docs.rs][4])

**Config synthesis:**

* initializes `DeltaScanConfig::new()`
* if `file_column` set, applies `config.with_file_column_name(file_column)` ([Docs.rs][4])

**Snapshot resolution precedence:**

1. If `this.snapshot.is_some()`: use provided `SnapshotWrapper` directly.
2. Else if `this.log_store.is_some()`: construct `Snapshot::try_new(log_store, Default::default(), this.table_version.map(|v| v as i64)).await?` and wrap as `SnapshotWrapper::Snapshot(...)`.
3. Else: return `DataFusionError::Plan("Either a log store or a snapshot must be provided …")`. ([Docs.rs][4])

**Provider instantiation target:**

* returns `Arc::new(next::DeltaScan::new(snapshot, config)?) as Arc<dyn TableProvider>`. ([Docs.rs][4])

**Offered value (vs alternatives):**

* The builder provides a *single* construction locus for `(snapshot selection + scan config defaults + provider instantiation)`; alternatives require manual assembly (`DeltaTableProvider::try_new(...)` or relying on `impl TableProvider for DeltaTable`). ([Docs.rs][3])

---

### 4.5 `DeltaTable::update_datafusion_session(session)`: object-store registry alignment (required for multi-store correctness)

**Method behavior (source-level):**

* compute `url = self.log_store().root_url().as_object_store_url()`
* if `session.runtime_env().object_store(&url).is_err()`, register:
  `session.runtime_env().register_object_store(url.as_ref(), self.log_store().object_store(None))`
* return `Ok(())`. ([Docs.rs][4])

**Underlying DataFusion contract:**

* `RuntimeEnv::register_object_store(url, object_store)` registers an `ObjectStore` for a URL prefix/scheme; enables DataFusion to resolve reads for URLs without built-in store support. ([Docs.rs][6])

**Offered value (vs alternatives):**

* `update_datafusion_session` implements *idempotent* “register-if-missing” semantics for the Delta table’s root object-store URL; alternative is out-of-band registration through `SessionContext::register_object_store` / `RuntimeEnv::register_object_store` prior to table registration. ([Docs.rs][4])
* In multi-store sessions (heterogeneous URL schemes/hosts), per-table registration prevents resolution failures attributable to missing registry entries (“no suitable object store” class failures are diagnosed in DataFusion issue tracker; not repeated here). ([GitHub][7])

---

### 4.6 Known sharp edge: `impl TableProvider for DeltaTable` exposes load-state unsafety (schema unwrap)

**Normative warning embedded in source:**

* `// TODO: implement this for Snapshot, not for DeltaTable since DeltaTable has unknown load state.`
* `// the unwraps in the schema method are a dead giveaway ..` ([Docs.rs][4])

**Concrete footgun:**

* `fn schema(&self) -> Arc<Schema> { self.snapshot().unwrap().snapshot().read_schema() }` (panic if `snapshot()` fails). ([Docs.rs][4])

**Operational requirement (if using `DeltaTable` directly as `TableProvider`):**

* Before any DataFusion pathway that consults `TableProvider::schema()` (planning/registration/validation), ensure `DeltaTable` has a loaded snapshot state via a successful `load`/`load_version`/`load_with_datetime` sequence (methods are on `DeltaTable`). ([Docs.rs][2])

**Offered value of builder/provider patterns (vs direct `DeltaTable` trait impl):**

* `TableProviderBuilder` path never calls `DeltaTable::schema()`; it materializes a provider via snapshot wrapper / log store and returns `Arc<dyn TableProvider>`; direct `DeltaTable` trait impl couples correctness to load-state discipline. ([Docs.rs][4])

---

### 4.7 `DeltaTableProvider`: explicit provider with optional file-subsetting (advanced control plane)

**Constructor + capability:**

* `DeltaTableProvider::try_new(snapshot: EagerSnapshot, log_store: Arc<dyn LogStore>, config: DeltaScanConfig) -> Result<DeltaTableProvider, DeltaTableError>` builds provider with an explicit snapshot and config. ([Docs.rs][3])
* `DeltaTableProvider::with_files(self, files: Vec<Add>) -> DeltaTableProvider` restricts which `Add` actions are considered during scan (“advanced usecases”). ([Docs.rs][3])

**Offered value (vs builder):**

* `with_files` provides a first-class input for incremental / curated scans (e.g., “only these `Add`s”); `TableProviderBuilder` constructs its file set from snapshot/log (no external file subset injection surface). ([Docs.rs][3])

---

### 4.8 Deterministic provider construction procedure (implementation checklist; no exemplar code)

**Inputs:** `DeltaTable` instance `T`, DataFusion session `S`, desired semantics `{latest | pinned_version | pinned_snapshot}`, optional `{file_column_name}`.

1. **Object store registration (precondition for scan execution across stores):**

   * Invoke `T.update_datafusion_session(S)` OR pre-register store via `RuntimeEnv::register_object_store` / `SessionContext::register_object_store` for the table’s root URL. ([Docs.rs][4])

2. **Provider construction mode selection:**

   * If a snapshot is already loaded (and intended to be reused/pinned), select `DeltaTable::table_provider()` and rely on its eager-snapshot branch. ([Docs.rs][4])
   * If no snapshot is loaded and pinning is required, set `with_table_version(Some(version))` on the returned builder (or supply `with_snapshot/with_eager_snapshot` explicitly). ([Docs.rs][4])

3. **Optional provenance column:**

   * If a source file-path column is required in query outputs, set `with_file_column(name)`; builder maps this into `DeltaScanConfig` (`with_file_column_name`). ([Docs.rs][4])

4. **Avoid direct `impl TableProvider for DeltaTable` unless load-state is enforced:**

   * If registering `DeltaTable` itself as `TableProvider`, enforce “snapshot loaded” prior to any `schema()`-dependent operation due to `unwrap()` in `schema()`. ([Docs.rs][4])

This set of rules yields: (a) provider materialized with explicit snapshot/log-store provenance, (b) object store resolution aligned with session runtime env, (c) deterministic pinning when required, (d) elimination of schema unwrap panics by construction.

[1]: https://docs.rs/deltalake/latest/deltalake/datafusion/index.html "deltalake::datafusion - Rust"
[2]: https://docs.rs/deltalake/latest/deltalake/table/struct.DeltaTable.html "DeltaTable in deltalake::table - Rust"
[3]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaTableProvider.html "DeltaTableProvider in deltalake::delta_datafusion - Rust"
[4]: https://docs.rs/deltalake-core/0.30.1/x86_64-unknown-linux-gnu/src/deltalake_core/delta_datafusion/table_provider.rs.html "table_provider.rs - source"
[5]: https://docs.rs/deltalake/latest/deltalake/datafusion/index.html?utm_source=chatgpt.com "deltalake::datafusion - Rust"
[6]: https://docs.rs/datafusion/latest/datafusion/execution/runtime_env/struct.RuntimeEnv.html?utm_source=chatgpt.com "RuntimeEnv in datafusion::execution::runtime_env - Rust"
[7]: https://github.com/apache/datafusion/issues/2136?utm_source=chatgpt.com "No suitable object store found for *** · Issue #2136"

## 5) Writing through DataFusion: sink path, transactional commit, and write contracts (delta-rs + DataFusion)

### 5.1 DataFusion DML → sink execution contract (what is invoked, when, and with what guarantees)

**Execution abstraction (DataFusion):** DML (e.g., `INSERT INTO`) is executed as an `ExecutionPlan` whose terminal operator is typically `DataSinkExec` (DataFusion’s canonical “stream RecordBatches into a sink” pattern). ([Docs.rs][1])

**Sink interface (DataFusion):** a sink is a `DataSink` implementing:

* `schema() -> &Arc<Schema>` (declares sink input schema)
* `write_all(data: Pin<Box<dyn RecordBatchStream<Item = Result<RecordBatch, DataFusionError>> + Send>>, context: &Arc<TaskContext>) -> Future<Output = Result<u64, DataFusionError>>` ([Docs.rs][2])

**Single-shot DML commit rule (DataFusion):**

* `write_all` is called **exactly once per DML statement**; prior to returning, the sink **must perform any commit/rollback required**. ([Docs.rs][2])

**Operational value vs alternatives (capability statement):**

* This contract enables *streaming* writeout from an upstream DataFusion physical plan without requiring full materialization to an in-memory table prior to persistence (contrast: `collect()` then external writer). ([Docs.rs][2])

---

### 5.2 Delta write transaction envelope: file-first, log-commit second (atomicity boundary)

**Delta transaction ordering (delta-rs / Delta docs):**

* Data files (e.g., Parquet) are written first; the table changes become visible only after a new transaction log entry is committed. ([delta-io.github.io][3])

**Failure surface difference vs “raw Parquet directory writes”:**

* If a job errors after writing Parquet files but before writing a valid log entry, the table snapshot is unchanged (no new data visible), whereas a plain Parquet directory write can leave “partial success” that must be unwound manually. ([delta-io.github.io][3])

**Implication for DataFusion sink design:**

* The sink’s `write_all` must implement: (1) write file set, (2) construct Delta actions, (3) commit log entry, (4) return row-count. The commit/rollback obligation is enforced by the DataFusion single-shot rule. ([Docs.rs][2])

---

### 5.3 delta-rs write execution topology (how batches are driven, partitioned, and measured)

**Write plan driver (delta-rs):** delta-rs’ write execution orchestrates concurrent “writer tasks” and “worker tasks” (per-partition driving), then aggregates the resulting `Add` actions and reports write/scan timing metrics (`WriteExecutionPlanMetrics { scan_time_ms, write_time_ms }`). ([Docs.rs][4])

**Writer parameterization (explicit knobs):** writer setup includes:

* `write_schema` (post-filter schema)
* `partition_columns`
* `writer_properties` (Parquet writer props)
* `target_file_size`
* `write_batch_size`
* writer stats configuration (indexed/stat columns) ([Docs.rs][4])

**Value vs alternatives:**

* The sink path can preserve DataFusion’s upstream parallelism and record-batch streaming while mapping to Delta’s file/commit transaction model; alternative “collect then write” collapses streaming into a single materialization boundary, increasing peak memory pressure and delaying commit visibility. (Capability contrast; no behavioral claim beyond contract.) ([Docs.rs][2])

---

### 5.4 “UDF-heavy pipelines” consequence: validation and semantic branching occur *inside* the write driver

**Batch-level validation hook:** delta-rs’ write execution calls a checker (`check_batch`) on constructed batches prior to writing (illustrated in the CDC branch where both “normal” and “cdf” batches are checked). ([Docs.rs][4])

**CDC / CDF branch mechanics (delta-rs write execution):**

* In a CDC-enabled path, upstream batches are split by `_change_type` into a “normal” batch and a “cdf” batch using DataFusion expressions; a temporary `MemTable` is constructed to run the filters; `_change_type` is then dropped from the normal batch before writing. ([Docs.rs][4])

**Value vs alternatives:**

* This demonstrates that delta-rs’ DataFusion-backed write driver can apply relational transforms (filters, projections, schema adjustments) in-process during write, rather than requiring external pre-processing prior to write submission. ([Docs.rs][4])

---

### 5.5 Dictionary column conversion: schema-normalization requirement, typical sources, and mitigation knobs

**Where dictionary types enter (delta-rs / DataFusion integration):**

* delta-rs imports Arrow `DictionaryArray` / `TypedDictionaryArray` and Arrow casting (`cast_with_options`) in the DataFusion integration module, indicating explicit support for dictionary-typed columns and cast-based normalization paths. ([Docs.rs][5])
* delta-rs also imports DataFusion’s `wrap_partition_type_in_dict`, which is the mechanism used to dictionary-wrap partition-related injected columns in DataFusion scans. ([Docs.rs][5])

**Write-path requirement (actionable constraint):**

* For any sink writing into a Delta table, the outgoing batch schema must match the Delta table schema’s physical Arrow types. Therefore, any column with Arrow `DataType::Dictionary(_, value_type)` must be normalized to `value_type` (cast or unwrap) unless the target table schema explicitly stores that column as a dictionary type (rare for persisted Parquet). (Type-compatibility constraint; implementation uses Arrow cast APIs surfaced above.) ([Docs.rs][5])

**Mitigation knobs (explicit):**

* **Upstream mitigation:** disable dictionary-wrapping for partition/path injection at scan config level (delta-rs scan config exposes `wrap_partition_values` on read-side; if enabled, injected columns may be dictionaries). (Read-side knob already present in your scan mechanics section.)
* **Downstream mitigation:** enforce a “sink input schema canonicalization” step using Arrow cast (the integration module explicitly imports cast utilities). ([Docs.rs][5])

**Value vs alternatives:**

* Normalization allows using file/path/partition metadata columns for diagnostics and intermediate transforms without persisting those dictionary encodings into the Delta table’s physical schema. ([Docs.rs][5])

---

### 5.6 Concurrency semantics: commit conflicts, locking clients, and service-level expectations

**Delta concurrency model (Delta docs):**

* Delta provides ACID behavior under concurrent writers; conflicting writes fail with a concurrent modification exception rather than corrupting the table. ([Delta Lake][6])

**delta-rs concurrent write support (implementation surface):**

* On ADLS/GCS/S3 with a locking client, delta-rs supports concurrent write attempts; if two writers target the same commit, delta-rs attempts conflict resolution (per maintainer discussion). ([GitHub][7])

**Observed edge cases (engineering input signal):**

* delta-rs has an issue report where concurrent append writers can yield asymmetric client-observed results (one writer returns error while both inserts appear), relevant for “exactly-once” service semantics and retry design. ([GitHub][8])
* Spark-side semantics for conflict classes (e.g., `ConcurrentAppendException` when concurrent operations add files in partitions read by your job) illustrate the conflict taxonomy typical in Delta’s optimistic concurrency layer. ([community.databricks.com][9])

**Operational requirement (explicit):**

* A DataFusion→Delta sink must treat “commit conflict” as a first-class outcome: detect commit failure, interpret whether the transaction may have been committed by another writer, and select a retry/verification policy consistent with the service’s idempotency model (e.g., reconcile against transaction log history where available). (Conflict existence and failure mode are normative per Delta concurrency docs; delta-rs behavior depends on lock-store and client version.) ([Delta Lake][6])

---

### 5.7 Metrics and verification: what to record and how to validate behavior

**Sink metrics contract (DataFusion):**

* `DataSink::metrics() -> Option<MetricsSet>`: sinks may expose metrics snapshots; DataFusion surfaces plan metrics via `ExecutionPlan::metrics()`. ([Docs.rs][2])

**delta-rs write metrics already computed in execution driver:**

* delta-rs computes and returns `scan_time_ms` and `write_time_ms` as part of write execution metrics. ([Docs.rs][4])

**Recommended verification harness (artifact spec):**

1. Persist `EXPLAIN` (logical+physical) for the DML plan; assert terminal operator is `DataSinkExec` (or equivalent sink exec) and that the sink’s declared schema matches the expected table write schema. ([Docs.rs][10])
2. Persist sink metrics snapshot (rows written; file counts if exposed; timing metrics). `write_all` returns `u64` “number of values written” per DataFusion contract; delta-rs additionally produces scan/write timing. ([Docs.rs][2])
3. Persist the committed Delta version + Add-action inventory hash to bind the write to an immutable snapshot (ties into your “pinned snapshot bundle” pattern). ([delta-io.github.io][3])

---

### 5.8 “Write contract” checklist (implementation obligations; minimal ambiguity)

**A) Input schema conformance**

* Ensure sink input schema fields are a subset of target Delta schema fields (excluding known non-table metadata columns such as file-path columns unless explicitly persisted).
* Enforce Arrow physical type compatibility; cast where required (esp. dictionary → value type). ([Docs.rs][5])

**B) Partition column handling**

* Partition columns must be configured consistently with the table’s partition layout; writer execution is parameterized by `partition_columns` and uses them while constructing writers and file routing. ([Docs.rs][4])
* If upstream reads injected partition/path metadata columns, drop or rename prior to persistence unless the table schema includes them. (Injection mechanism is visible via `wrap_partition_type_in_dict` import and scan-config patterns.) ([Docs.rs][5])

**C) Batch validation / constraints**

* If table has invariants/constraints enabled, enforce `check_batch` over each outgoing batch prior to writing; error must abort the transaction before log commit (consistent with DataFusion sink “commit/rollback before return” rule). ([Docs.rs][4])

**D) CDC/CDF semantics (if enabled)**

* If `_change_type` (or equivalent) is present, define an explicit split/filter policy and schema policy (keep vs drop) prior to writing to main data and change-data destinations; delta-rs demonstrates “split, validate, drop CDC col for normal batch, validate CDF batch.” ([Docs.rs][4])

**E) Transaction completion**

* `write_all` must be the atomic boundary: write files → assemble actions → commit log entry → return row count; on error: abort without committing log entry. (File-first/log-second ordering is Delta’s transaction model.) ([Docs.rs][2])

**F) Concurrency policy**

* Select and configure a lock-enabled log store on object stores that require external coordination; treat commit conflicts as expected outcomes; implement deterministic retry/verification. ([GitHub][7])

[1]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html?utm_source=chatgpt.com "TableProvider in datafusion::datasource - Rust"
[2]: https://docs.rs/deltalake/latest/deltalake/datafusion/datasource/sink/trait.DataSink.html "DataSink in deltalake::datafusion::datasource::sink - Rust"
[3]: https://delta-io.github.io/delta-rs/how-delta-lake-works/delta-lake-acid-transactions/?utm_source=chatgpt.com "Transactions - Delta Lake Documentation"
[4]: https://docs.rs/crate/deltalake-core/0.30.1/source/src/operations/write/execution.rs "deltalake-core 0.30.1 - Docs.rs"
[5]: https://docs.rs/deltalake-core/0.30.1/x86_64-unknown-linux-gnu/src/deltalake_core/delta_datafusion/mod.rs.html "mod.rs - source"
[6]: https://docs.delta.io/concurrency-control/?utm_source=chatgpt.com "Concurrency control"
[7]: https://github.com/delta-io/delta-rs/discussions/2426?utm_source=chatgpt.com "Concurrent write support · delta-io delta-rs"
[8]: https://github.com/delta-io/delta-rs/issues/2279?utm_source=chatgpt.com "Successful writes return error when using concurrent writers"
[9]: https://community.databricks.com/t5/get-started-discussions/concurrent-update-to-delta-throws-error/td-p/65599?utm_source=chatgpt.com "Concurrent Update to Delta - Throws error"
[10]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html?utm_source=chatgpt.com "TableProvider in datafusion::catalog - Rust"

## 7) Schema evolution, generated columns, constraints, column mapping: DataFusion-observed semantics (Delta protocol + delta-rs)

### 7.1 Snapshot-scoped schema: what is versioned, what is “visible”

**Delta snapshot definition (protocol):** the table state at version *v* (“snapshot”) includes `metaData` (schema, partition columns, table properties/config), plus the active-file set. Therefore, **schema is version-addressed**; any read/write plan is correct only w.r.t. the snapshot it was derived from. ([GitHub][1])

**Implication for DataFusion:** a DataFusion `TableProvider` built from a snapshot will expose the **schema of that snapshot**; schema changes are new snapshots (new table versions), not “out-of-band Parquet schema drift.” ([GitHub][1])

---

### 7.2 Schema enforcement vs schema evolution: available modes, why consider, how to invoke (delta-rs)

#### 7.2.1 Enforcement default (delta-rs)

**Behavior:** writes reject schema mismatches by default. delta-rs docs: `write_deltalake` raises `ValueError` when incoming schema differs from existing table schema. ([Delta][2])

**Why consider (vs “accept mismatched Parquet files”):**

* enforcement gives a single authoritative schema in `metaData`, preventing “heterogeneous file schemas under one logical table.” ([Delta][2])

#### 7.2.2 Evolution on write: `schema_mode ∈ {merge, overwrite}` (delta-rs)

**`schema_mode="merge"` (delta-rs write path):**

* adds new columns to the table schema; missing columns in incoming data are filled with `NULL`; supported for overwrite and also append operations. ([Delta][2])

**`schema_mode="overwrite"` (delta-rs write path):**

* replaces table schema with incoming schema (including dropping columns). ([Delta][2])

**Why consider:**

* `merge`: supports additive evolution while keeping existing files valid (older files have no values for new columns → logically `NULL`). ([Delta][2])
* `overwrite`: supports explicit schema replacement where you want the table’s schema to become exactly the new schema. ([Delta][2])

**How to invoke (delta-rs):**

* on the write API surface, pass `schema_mode="merge"` or `schema_mode="overwrite"` alongside `mode={append|overwrite}`. ([Delta][2])

#### 7.2.3 Explicit schema evolution DDL/ops (delta-rs): add columns / nested fields

**Available operation:** delta-rs exposes an “Add Column” operation: “Add new columns and/or nested fields to a table.” ([Docs.rs][3])

**How to invoke (Rust surface):**

* use the operations API module `deltalake::operations::add_column` (entry builder `AddColumnBuilder`) and supply `StructField` definitions. ([Docs.rs][3])

**Why consider (vs schema_mode merge-on-write):**

* separates **schema mutation** (metadata commit) from **data mutation** (file adds), enabling dedicated schema-change transactions. (Schema is in `metaData` per snapshot.) ([GitHub][1])

---

### 7.3 Constraints + invariants: encoding, protocol gates, writer obligations, delta-rs operations

#### 7.3.1 Column invariants (`delta.invariants`)

**Encoding (protocol):**

* a column’s schema metadata may contain key `delta.invariants`; value is a JSON string containing a boolean SQL expression at `expression.expression`. ([GitHub][1])

**Protocol support gate:**

* writer versions 2–6: invariants supported; writer version 7: feature name `invariants` must exist in `protocol.writerFeatures`. ([GitHub][1])

**Writer requirement (protocol):**

* writers **MUST abort** any transaction adding a row where an invariant evaluates to `false` or `null`. ([GitHub][1])

**delta-rs surface:**

* Rust type `Invariant { field_name, invariant_sql }` exists (core representation). ([Docs.rs][4])

**Why consider (vs app-layer validation):**

* invariant enforcement is transactional: violating rows prevent commit (table remains unchanged). ([GitHub][1])

#### 7.3.2 CHECK constraints (`delta.constraints.{name}`)

**Encoding (protocol):**

* table `metaData.configuration` contains keys `delta.constraints.{name}`; value is a SQL expression string with Boolean return type; referenced columns must exist in schema. ([GitHub][1])

**Protocol support gate:**

* writer versions 3–6: supported; writer version 7: feature `checkConstraints` must exist in `protocol.writerFeatures`. ([GitHub][1])

**Writer requirements (protocol):**

* when adding a CHECK constraint: validate **existing** data satisfies it before committing the constraint. ([GitHub][1])
* when writing new rows to a constrained table: all new rows must satisfy constraints; otherwise commit must fail and table remain unchanged. ([GitHub][1])

**delta-rs operations API (Rust):**

* delta-rs documents `DeltaOps(table).add_constraint().with_constraint(name, expr)` as the Rust method to attach a constraint. ([Delta][5])

**Why consider:**

* constraints become table-managed integrity checks; violations prevent partial ingestion. ([Delta Lake][6])

**DataFusion-visible semantics:**

* constraints/invariants are **write-time correctness conditions**; DataFusion reads expose columns and nullability but do not inherently encode these constraints as relational planner constraints (they remain metadata + enforcement obligations on writers). (Encoding + writer-abort rules above.) ([GitHub][1])

---

### 7.4 Generated columns: what is stored, how enforced, what DataFusion sees, delta-rs support surface

#### 7.4.1 Protocol encoding + enforcement rule

**Encoding (protocol):**

* generated column is a normal column whose schema metadata may contain `delta.generationExpression` (SQL expression string). ([GitHub][1])

**Protocol support gate:**

* writer versions 4–6: supported; writer version 7: feature `generatedColumns` must exist in `protocol.writerFeatures`. ([GitHub][1])

**Writer requirement (protocol):**

* writers must enforce, for each written row, the condition `(<value> <=> <generation expression>) IS TRUE` where `<=>` is NULL-safe equality. ([GitHub][1])

#### 7.4.2 delta-rs representation

**Rust surface type:** `GeneratedColumn { name, generation_expr, validation_expr, data_type }`; `validation_expr` is “SQL string that must always evaluate to true”; implements `DataCheck`. ([Docs.rs][7])

**Protocol support status signal (delta-rs):**

* deltalake crate’s “Protocol Support Level” lists “Version 4 Generated Columns” as done. ([Docs.rs][8])

#### 7.4.3 Write-time behavior (feature offers; usage constraints)

**Feature offers (Databricks semantics):**

* if values for generated columns are omitted on write, the engine computes them; if values are supplied, they must satisfy the NULL-safe equality constraint or the write fails. ([Databricks Documentation][9])
* generated columns are stored like normal columns (“occupy storage”). ([Databricks Documentation][9])

**Why consider (vs computing at query-time):**

* persisted generated values remove repeated recomputation at read-time (the data is materialized in storage). ([Databricks Documentation][9])

**DataFusion-visible semantics:**

* on read: generated columns are indistinguishable from ordinary stored columns at the Arrow schema level. ([Databricks Documentation][9])
* on write: any DataFusion→Delta writer must satisfy the protocol constraint (either compute values or supply values consistent with expression); otherwise commit must fail. ([GitHub][1])

---

### 7.5 Column mapping: rename/drop without rewriting Parquet; protocol gates; delta-rs status; DataFusion impact

#### 7.5.1 Protocol model and required metadata

**Purpose (protocol):**

* column mapping enables renaming/dropping columns without rewriting data files and removes Parquet column-name restrictions; two modes: `name`, `id`. ([GitHub][1])

**Metadata keys (protocol):**

* each column (nested or leaf) has:

  * `delta.columnMapping.physicalName` (unique physical name)
  * `delta.columnMapping.id` (unique 32-bit integer id)
* table property: `delta.columnMapping.mode ∈ {none, name, id}`. ([GitHub][1])

**Protocol gates (protocol):**

* readers: reader version 2, or reader version 3 with `columnMapping` table feature supported
* writers: writer version 5/6, or writer version 7 with `columnMapping` feature supported. ([GitHub][1])

**Enablement instruction (Delta docs):**

* set `delta.minReaderVersion=2`, `delta.minWriterVersion=5`, `delta.columnMapping.mode=name`; upgrade is irreversible; cannot turn off. ([Delta Lake][10])

**DDL effects (Delta docs):**

* once enabled: `ALTER TABLE … RENAME COLUMN …` and `ALTER TABLE … DROP COLUMN(S) …` become supported schema-evolution operations without rewriting files. ([Delta Lake][10])

#### 7.5.2 Writer requirements (protocol) relevant to non-Spark writers

**When turning on column mapping (writer v7 path):**

* write `protocol` action adding feature `columnMapping` to both `readerFeatures` and `writerFeatures`
* write `metaData` action adding `delta.columnMapping.mode`. ([GitHub][1])

**File-format obligations:** writers must produce Parquet compatible with mapping (protocol requires stable physical identity; details expanded in protocol’s “Writer Requirements for Column Mapping”). ([GitHub][1])

#### 7.5.3 delta-rs status indicators (as of deltalake 0.30.1 docs)

**Protocol support table (docs.rs):**

* lists “Version 5 Column Mapping” and “Reader Version 2 Column Mapping” without “done” markers, while earlier protocol items show “done”; treat as “not claimed complete” by that documentation artifact. ([Docs.rs][8])
  **Empirical friction signal:** recent delta-rs issue reports failures enabling column mapping due to missing `delta.columnMapping.id` annotations on fields. ([GitHub][11])

#### 7.5.4 DataFusion-visible semantics

* DataFusion schema should expose **logical column names**, while scan/write must resolve Parquet columns via **physical identity** (`physicalName`/`id`) per protocol. ([GitHub][1])
* Rename/drop become metadata-only schema updates; engines that ignore column mapping will mis-resolve columns (protocol forbids ignoring required table features). ([GitHub][1])

---

### 7.6 Nested / complex type evolution: expected Delta semantics vs delta-rs observed issues

#### 7.6.1 Delta (Spark semantics): arrays-of-struct evolution in MERGE

* Delta `MERGE INTO` resolves struct fields by name and supports evolving schemas for arrays of structs when schema evolution is enabled; nested structs inside arrays evolve as well. ([Delta Lake][12])

#### 7.6.2 delta-rs schema merge edge case (list-of-struct)

* delta-rs issue #3339 reports schema-merge failures “since adoption of datafusion” when the existing schema contains a list-of-struct field and an append with `schema_mode="merge"` introduces a new field elsewhere in the nested struct. ([GitHub][13])

#### 7.6.3 MERGE schema evolution divergence (delta-rs)

* delta-rs issue #4009 documents that “MERGE with schema evolution does not add new columns” under `schema_mode="merge"` (columns remain absent, operation completes). ([GitHub][14])

**Why this matters for a DataFusion-centric deployment:**

* schema evolution correctness for nested types is not solely “Delta protocol”; it depends on the specific writer/merge implementation surface in delta-rs (append vs merge paths differ). ([Delta][2])

---

### 7.7 What DataFusion “sees” across these features (schema + planning contract)

**Observed schema surface:**

* Delta snapshot `metaData` defines the logical schema; DataFusion provider schema is snapshot-derived. ([GitHub][1])

**Additive evolution semantics (null fill):**

* delta-rs `schema_mode="merge"` specifies: new columns appended; missing columns filled with `NULL` (read-time semantics for older files implied by the merged schema). ([Delta][2])

**Non-additive evolution semantics (rename/drop):**

* requires column mapping; otherwise rename/drop implies Parquet rewrite to avoid “revival” ambiguity (motivating problem statement in Delta ecosystem). ([GitHub][1])

**Constraint/generation semantics:**

* constraints (`delta.constraints.*`), invariants (`delta.invariants`), generated columns (`delta.generationExpression`) are schema/config metadata plus writer-abort rules; DataFusion read schema does not automatically imply enforcement unless the writer path implements the checks. ([GitHub][1])

---

### 7.8 Recommended artifact: schema evolution + compatibility test suite (spec)

#### 7.8.1 Golden schema ladder (versioned snapshots)

Construct table versions `v0…vn` such that each version introduces exactly one feature delta; persist expected schema for each version:

* v0: baseline primitive schema + partition columns
* v1: add top-level column (add-column op; or schema_mode merge on append) ([Docs.rs][3])
* v2: add nested struct field (supported in “add columns” DDL semantics in general; delta-rs supports “Add Column … nested fields”) ([Databricks Documentation][15])
* v3: add CHECK constraint (`delta.constraints.*`) ([GitHub][1])
* v4: add column invariant (`delta.invariants`) ([GitHub][1])
* v5: introduce generated column (`delta.generationExpression`) ([GitHub][1])
* v6: enable column mapping (`delta.columnMapping.mode`, ids/physical names), then rename + drop columns ([Delta Lake][10])
* v7+: append with `schema_mode="merge"` where a list-of-struct exists (regression fixture for #3339) ([GitHub][13])

#### 7.8.2 Read-path assertions (DataFusion planner sanity)

For each version `vk`:

* open snapshot at `vk` (version-pinned) and assert DataFusion-exposed schema equals golden schema for `vk` (schema is snapshot metadata). ([GitHub][1])
* run `EXPLAIN` for representative projections/filters referencing:

  * newly added columns (nullability and projection behavior)
  * renamed columns (column mapping correctness, when supported)
  * generated columns (treated as standard columns on read). ([Delta][2])

#### 7.8.3 Write-path assertions (transactional enforcement)

For each constraint/generation feature:

* attempt a violating write; assert commit fails and table version does not advance (protocol abort requirement). ([GitHub][1])
* attempt a satisfying write; assert commit succeeds and new snapshot contains expected metadata keys (`delta.constraints.*`, `delta.invariants`, `delta.generationExpression`). ([GitHub][1])

#### 7.8.4 Nested evolution fixtures (divergence detection)

* MERGE + schema evolution: explicit test that new columns are (or are not) added; track delta-rs behavior against the expected Delta semantics (issue #4009 indicates mismatch existed). ([Delta Lake][12])
* list-of-struct merge: append with `schema_mode="merge"` adding a sibling nested field; assert either success with schema update or failure with classified error (issue #3339). ([GitHub][13])

[1]: https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md "raw.githubusercontent.com"
[2]: https://delta-io.github.io/delta-rs/usage/writing/ "https://delta-io.github.io/delta-rs/usage/writing/"
[3]: https://docs.rs/deltalake/latest/deltalake/operations/add_column/struct.AddColumnBuilder.html "https://docs.rs/deltalake/latest/deltalake/operations/add_column/struct.AddColumnBuilder.html"
[4]: https://docs.rs/deltalake/latest/deltalake/struct.Invariant.html "https://docs.rs/deltalake/latest/deltalake/struct.Invariant.html"
[5]: https://delta-io.github.io/delta-rs/usage/constraints/ "Adding a constraint - Delta Lake Documentation"
[6]: https://docs.delta.io/delta-constraints/ "https://docs.delta.io/delta-constraints/"
[7]: https://docs.rs/deltalake/latest/deltalake/table/struct.GeneratedColumn.html "https://docs.rs/deltalake/latest/deltalake/table/struct.GeneratedColumn.html"
[8]: https://docs.rs/crate/deltalake/latest "https://docs.rs/crate/deltalake/latest"
[9]: https://docs.databricks.com/aws/en/delta/generated-columns "https://docs.databricks.com/aws/en/delta/generated-columns"
[10]: https://docs.delta.io/delta-column-mapping/ "https://docs.delta.io/delta-column-mapping/"
[11]: https://github.com/delta-io/delta-rs/issues/3936 "https://github.com/delta-io/delta-rs/issues/3936"
[12]: https://docs.delta.io/delta-update/ "https://docs.delta.io/delta-update/"
[13]: https://github.com/delta-io/delta-rs/issues/3339 "https://github.com/delta-io/delta-rs/issues/3339"
[14]: https://github.com/delta-io/delta-rs/issues/4009 "https://github.com/delta-io/delta-rs/issues/4009"
[15]: https://docs.databricks.com/aws/en/delta/update-schema "https://docs.databricks.com/aws/en/delta/update-schema"

## 6) Maintenance operations (Optimize / Vacuum / Checkpoints): performance + correctness surfaces (Delta protocol + delta-rs)

### 6.1 Maintenance taxonomy: data-plane rewrite vs storage GC vs metadata compaction

**Delta protocol primitives (state + retention):**

* Snapshot = `{protocol, metadata, active add files, tombstones(remove), txn ids}`; MVCC keeps multiple file generations; physical deletion is delayed and performed via vacuum after retention. ([GitHub][1])
* `remove` actions create tombstones; tombstones expire when `current_time > remove_timestamp + retention_threshold`; physical deletion is permitted only after expiry. ([GitHub][1])

**delta-rs exposed maintenance families (Rust API):**

* **Data rewrite:** `DeltaTable::optimize() -> OptimizeBuilder` (bin-packing compaction; optional Z-order rewrite). ([Docs.rs][2])
* **Storage GC:** `DeltaTable::vacuum() -> VacuumBuilder` (delete unreferenced files older than retention; mode selection; dry-run). ([Docs.rs][2])
* **Metadata compaction + log cleanup:** checkpoint creation + expired-log cleanup (`deltalake::protocol::checkpoints::{create_checkpoint, cleanup_metadata, cleanup_expired_logs_for, …}`). ([Docs.rs][3])

**Why consider (vs “raw Parquet directory” maintenance):**

* Delta maintenance acts on *snapshot-defined membership* (active files + tombstones) rather than filesystem heuristics; rewrite operations generate `remove` actions; vacuum implements physical deletion consistent with retention; checkpoints compact log replay cost. ([GitHub][1])

---

### 6.2 OPTIMIZE: compaction + Z-order (what exists, why use, how to configure)

#### 6.2.1 Optimize semantics (delta-rs, Rust)

**Operation definition:**

* Bin-packing rewrite: merges small data files into larger files; stated objective: reduce number of API calls required for reads (fewer objects opened/listed). ([Docs.rs][4])
* Transactional effects:

  * increments table version
  * writes `remove` actions for replaced files
  * does **not** delete physical files from storage; vacuum required for physical deletion. ([Docs.rs][4])

**Concurrency rule (delta-rs, Rust):**

* Optimize fails if a concurrent write removes files (e.g., overwrite); succeeds if concurrent writers only append. ([Docs.rs][4])

#### 6.2.2 Optimize types (delta-rs)

**Enum surface:**

* `OptimizeType::Compact` (bin-pack files into predetermined bins)
* `OptimizeType::ZOrder(Vec<String>)` (Z-order by provided columns). ([Docs.rs][5])

**Feature offered (why consider):**

* `Compact`: reduces file-count; reduces per-query object-store overhead (open/list/metadata IO) explicitly stated as “reduces API calls.” ([Docs.rs][4])
* `ZOrder`: rewrites file layout using Z-order curve; goal: improve file skipping via stronger clustering on filter columns. ([Delta][6])
* Delta Lake OSS docs note Z-order is **not idempotent** (repeated runs can keep reclustering files). ([Delta Lake][7])

#### 6.2.3 OptimizeBuilder configuration surface (Rust)

**Builder construction:** `DeltaTable::optimize()` (or `DeltaOps(table).optimize()`) returns `OptimizeBuilder<'a>`. ([Docs.rs][2])

**Primary knobs (Rust):**

* `with_type(OptimizeType)` (default = `Compact`). ([Docs.rs][8])
* `with_filters(&[PartitionFilter])` (optimize a partition subset). ([Docs.rs][8])
* `with_target_size(u64)` (target file size; if absent, read table property `delta.targetFileSize`, else internal default). ([Docs.rs][8])
* `with_writer_properties(WriterProperties)` (Parquet writer properties). ([Docs.rs][8])
* `with_commit_properties(CommitProperties)` (augment commitInfo metadata). ([Docs.rs][8])

**Memory/disk control plane (Optimize-specific, Rust):**

* `create_session_state_for_optimize(max_spill_size: Option<usize>, max_temp_directory_size: Option<u64>) -> SessionState` is explicitly documented as “recommended” for memory/disk limits; pass to `OptimizeBuilder` via `with_session_state`. ([Docs.rs][9])

#### 6.2.4 Z-order invocation (Rust)

**Required instruction sequence:**

* Select `OptimizeType::ZOrder(vec![col…])` via `OptimizeBuilder::with_type`. delta-rs docs provide the canonical call shape using `DeltaOps(table).optimize().with_type(OptimizeType::ZOrder(...)).await`. ([Delta][6])

---

### 6.3 VACUUM: physical deletion, mode selection, retention enforcement, time-travel boundary

#### 6.3.1 Vacuum semantics (Delta Lake + delta-rs)

**Operation definition:**

* Delete files no longer referenced by the table and older than retention threshold; running vacuum eliminates ability to time travel to versions older than the retention window if required data files are deleted. ([Docs.rs][10])

**Data-vs-log distinction (Delta Lake docs):**

* `vacuum` deletes **data files**; does **not** delete log files; log files are deleted asynchronously after checkpoint operations; default log retention 30 days via `delta.logRetentionDuration`. ([Delta Lake][11])

**Retention parameters (Delta Lake docs):**

* `delta.deletedFileRetentionDuration` controls data-file retention for vacuum (default 7 days).
* `delta.logRetentionDuration` controls transaction-log retention (default 30 days). ([Databricks Community][12])

#### 6.3.2 VacuumBuilder (Rust) + hard knobs

**Builder surface (methods):**

* `with_retention_period(TimeDelta)` (override default retention). ([Docs.rs][13])
* `with_keep_versions(&[i64])` (pin specific table versions; prevent deletion of files required by those versions). ([Docs.rs][13])
* `with_mode(VacuumMode)` (override default mode; docs.rs notes “default vacuum mode (lite)”). ([Docs.rs][13])
* `with_dry_run(bool)` (determine deletion set without deleting). ([Docs.rs][13])
* `with_enforce_retention_duration(bool)` (validate specified retention ≥ table minimum). ([Docs.rs][13])
* `with_commit_properties(CommitProperties)` (commitInfo metadata). ([Docs.rs][13])
* `with_custom_execute_handler(Arc<dyn CustomExecuteHandler>)` (pre/post execution hooks). ([Docs.rs][13])

**Execution contract:**

* `VacuumBuilder: IntoFuture<Output = Result<(DeltaTable, VacuumMetrics), DeltaTableError>>` (returns updated table + metrics). ([Docs.rs][13])

#### 6.3.3 Vacuum modes (delta-rs semantics)

**Enum surface (VacuumMode):**

* `Lite`: removes files referenced by `_delta_log` `remove` actions (tombstoned files). ([Docs.rs][14])
* `Full`: scans storage and removes all data files not actively referenced in `_delta_log` (e.g., stray Parquet not present as `add`). ([Docs.rs][14])

**Why consider (Lite vs Full):**

* `Lite` avoids full directory scan requirement (storage listing), depending on tombstone set; `Full` includes “files not referenced by the log.” ([Docs.rs][14])

#### 6.3.4 Multi-writer gating (explicit requirement envelope)

**Delta Lake safety requirement (Databricks docs):**

* Retention interval must exceed the longest concurrent transaction / stale reader window; otherwise vacuum can delete files still in use; Databricks documents a retention safety check and warns about corruption if active files are removed prematurely. ([Databricks Documentation][15])

**delta-rs enforcement analog:**

* `with_enforce_retention_duration(true)` implements a minimum-retention check against the table’s configured minimum (retention validation hook). ([Docs.rs][13])

---

### 6.4 Checkpoints + log growth: format, lifecycle, hazards, delta-rs surfaces

#### 6.4.1 Checkpoint definition (Delta protocol)

**Checkpoint role:**

* Stored in `_delta_log`; created at any time for any committed version.
* Contains “complete replay of all actions up to and including the checkpointed version, with invalid actions removed,” plus unexpired remove tombstones; enables readers to short-cut log replay; enables metadata cleanup to delete expired JSON log entries. ([GitHub][1])

**Checkpoint selection constraints:**

* Readers should prefer newest complete checkpoint; for time travel, checkpoint must not be newer than target version. ([GitHub][1])

**Checkpoint types (protocol):**

* Classic checkpoint: `n.checkpoint.parquet`
* Multi-part checkpoint: `n.checkpoint.o.p.parquet` (readers must ignore missing parts; multi-part non-atomic)
* UUID-named V2 checkpoint: `n.checkpoint.u.{json|parquet}` + optional sidecars; v2-checkpoint feature forbids multi-part; writers encouraged to avoid multi-part. ([GitHub][1])

**Last checkpoint pointer:**

* `_delta_log/_last_checkpoint` reduces directory listing; provides recent checkpoint version for paginated/lexicographic listing systems. ([GitHub][1])

#### 6.4.2 delta-rs checkpoint + cleanup APIs (Rust)

**Module:** `deltalake::protocol::checkpoints` (“Implementation for writing delta checkpoints”). ([Docs.rs][3])

**Functions exposed (docs.rs):**

* `create_checkpoint`: create checkpoint at current table version.
* `cleanup_metadata`: delete expired log files before a given version; retention based on `logRetentionDuration` (30 days default).
* `cleanup_expired_logs_for`: delete expired log files up to safe checkpoint boundary.
* `create_checkpoint_from_table_url_and_cleanup(url, version, cleanup)`: optionally runs metadata cleanup; if cleanup param empty, uses table property `enableExpiredLogCleanup`. ([Docs.rs][3])

**Delta Lake docs linkage (log cleanup trigger):**

* Log files are deleted asynchronously after checkpoint operations; default log retention 30 days (`delta.logRetentionDuration`). ([Delta Lake][11])

#### 6.4.3 Operational hazards (observed delta-rs issue patterns)

**Checkpoint memory spike (delta-rs):**

* Issue report: checkpoint file ~1.4GB; memory spikes from ~600Mi to ~7Gi at checkpoint creation cadence (“every 100 batches” in the described workload). ([GitHub][16])

**Checkpoint correctness hazard under partial snapshot (“without_files”):**

* Issue report: reading table with `without_files` to reduce memory, then appending; during automatic checkpoint creation, older committed files removed because they are not in the snapshot and thus omitted from checkpoint. ([GitHub][17])

**Cleanup safety hazard:**

* Issue report: `cleanup_metadata` can delete the most recent checkpoint under aggressive retention; reported as table corruption risk. ([GitHub][18])

**Derived constraint (implementation requirement, not a recommendation):**

* Any checkpoint writer must include the full reconciled action set (live `add` actions + required tombstones) as specified by protocol; omitting live adds yields invalid state materialization. ([GitHub][1])

---

### 6.5 “Table maintenance playbook” artifact (service-oriented spec)

#### 6.5.1 Policy schema (serialize as JSON/YAML; embed in catalog)

**`TableMaintenancePolicy` (minimal fields):**

* `table_uri: string`
* `optimize`:

  * `enabled: bool`
  * `type: "compact" | "zorder"`
  * `zorder_cols: [string]` (required iff type=zorder) ([Docs.rs][5])
  * `partition_filters: [PartitionFilter]` (optional) ([Docs.rs][8])
  * `target_size_bytes: u64 | null` (null ⇒ use `delta.targetFileSize`/default) ([Docs.rs][19])
  * `writer_properties: {...} | null` ([Docs.rs][8])
  * `session_state_limits`:

    * `max_spill_size_bytes: usize | null`
    * `max_temp_directory_size_bytes: u64 | null` ([Docs.rs][9])
  * `concurrency_assumption: "append_only" | "mixed"` (used to predict optimize conflict likelihood) ([Docs.rs][4])
* `vacuum`:

  * `enabled: bool`
  * `mode: "lite" | "full"` ([Docs.rs][14])
  * `dry_run_default: bool` (delta-rs defaults dry-run in Python docs; Rust exposes `with_dry_run`) ([Delta][20])
  * `retention_period: duration | null` (null ⇒ `delta.deletedFileRetentionDuration`/default 7d) ([Docs.rs][10])
  * `enforce_retention_duration: bool` ([Docs.rs][13])
  * `keep_versions: [int64] | null` (pin versions required for reproducibility/time-travel invariants) ([Docs.rs][13])
* `checkpoints`:

  * `enabled: bool`
  * `log_retention: duration` (maps to `delta.logRetentionDuration`) ([Delta Lake][11])
  * `enable_expired_log_cleanup: bool | null` (maps to `enableExpiredLogCleanup` usage in checkpoint cleanup path) ([Docs.rs][21])
  * `run_cleanup_metadata: bool` (if true, call `cleanup_metadata` after checkpoint) ([Docs.rs][3])

#### 6.5.2 Execution procedure (deterministic steps)

1. **Optimize execution (rewrite stage):**

   * Build `OptimizeBuilder` from `DeltaTable::optimize()`.
   * Set `with_type(Compact|ZOrder(cols))`.
   * Optionally set `with_filters(partition_filters)` and `with_target_size(target_size_bytes)`.
   * If large rewrites expected, construct `SessionState = create_session_state_for_optimize(max_spill_size, max_temp_directory_size)` and pass via `with_session_state`. ([Docs.rs][8])
2. **Vacuum execution (physical delete stage):**

   * Build `VacuumBuilder` from `DeltaTable::vacuum()`.
   * Set `with_mode(Lite|Full)`, `with_retention_period(...)` (or default), `with_dry_run(...)`, `with_enforce_retention_duration(...)`, `with_keep_versions(...)` as required by reproducibility constraints. ([Docs.rs][13])
3. **Checkpoint + log cleanup (metadata compaction stage):**

   * Create checkpoint at a known version; run `cleanup_metadata` / `cleanup_expired_logs_for` bounded by safe checkpoint boundary semantics. ([Docs.rs][3])
4. **Guard conditions (must be encoded, not ad-hoc):**

   * If running multi-writer, do not set retention below maximum concurrent transaction age; enforce retention check (`with_enforce_retention_duration(true)`; Databricks retention safety constraints documented). ([Databricks Documentation][15])
   * Do not checkpoint from a partially materialized snapshot that omits live `add` actions (observed failure mode under `without_files`). ([GitHub][17])

[1]: https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md "raw.githubusercontent.com"
[2]: https://docs.rs/deltalake/latest/deltalake/table/struct.DeltaTable.html "DeltaTable in deltalake::table - Rust"
[3]: https://docs.rs/deltalake/latest/deltalake/protocol/checkpoints/index.html "deltalake::protocol::checkpoints - Rust"
[4]: https://docs.rs/deltalake/latest/deltalake/operations/optimize/index.html "deltalake::operations::optimize - Rust"
[5]: https://docs.rs/deltalake/latest/deltalake/operations/optimize/enum.OptimizeType.html "OptimizeType in deltalake::operations::optimize - Rust"
[6]: https://delta-io.github.io/delta-rs/usage/optimize/delta-lake-z-order/ "Z Order - Delta Lake Documentation"
[7]: https://docs.delta.io/optimizations-oss/?utm_source=chatgpt.com "Optimizations"
[8]: https://docs.rs/deltalake/latest/deltalake/operations/optimize/struct.OptimizeBuilder.html "OptimizeBuilder in deltalake::operations::optimize - Rust"
[9]: https://docs.rs/deltalake/latest/deltalake/operations/optimize/fn.create_session_state_for_optimize.html "create_session_state_for_optimize in deltalake::operations::optimize - Rust"
[10]: https://docs.rs/deltalake/latest/deltalake/operations/vacuum/index.html "deltalake::operations::vacuum - Rust"
[11]: https://docs.delta.io/delta-utility/ "Table utility commands | Delta Lake"
[12]: https://community.databricks.com/t5/data-engineering/understanding-file-retention-with-vacuum/td-p/25822?utm_source=chatgpt.com "Understanding file retention with Vacuum"
[13]: https://docs.rs/deltalake/latest/deltalake/operations/vacuum/struct.VacuumBuilder.html "VacuumBuilder in deltalake::operations::vacuum - Rust"
[14]: https://docs.rs/deltalake/latest/deltalake/operations/vacuum/enum.VacuumMode.html "VacuumMode in deltalake::operations::vacuum - Rust"
[15]: https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum?utm_source=chatgpt.com "VACUUM | Databricks on AWS"
[16]: https://github.com/delta-io/delta-rs/issues/2628 "Large Memory Spike At Checkpoint · Issue #2628 · delta-io/delta-rs · GitHub"
[17]: https://github.com/delta-io/delta-rs/issues/3049 "Write operations with without_files lead to data loss due to checkpointing · Issue #3049 · delta-io/delta-rs · GitHub"
[18]: https://github.com/delta-io/delta-rs/issues/2174?utm_source=chatgpt.com "cleanup_metadata can potentially delete most recent ..."
[19]: https://docs.rs/deltalake/latest/deltalake/operations/optimize/struct.OptimizeBuilder.html?utm_source=chatgpt.com "OptimizeBuilder in deltalake::operations::optimize - Rust"
[20]: https://delta-io.github.io/delta-rs/python/usage.html "Usage — delta-rs  documentation"
[21]: https://docs.rs/deltalake-core/latest/deltalake_core/protocol/checkpoints/index.html?utm_source=chatgpt.com "deltalake_core::protocol::checkpoints - Rust"
