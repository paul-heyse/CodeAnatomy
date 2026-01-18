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
