**datafusion > execution > context**

# Module: execution::context

## Contents

**Structs**

- [`EmptySerializerRegistry`](#emptyserializerregistry) - Default implementation of [SerializerRegistry] that throws unimplemented error
- [`SQLOptions`](#sqloptions) - Describes which SQL statements can be run.
- [`SessionContext`](#sessioncontext) - Main interface for executing queries with DataFusion. Maintains

**Enums**

- [`RegisterFunction`](#registerfunction) - The result of processing a [`CreateFunction`] statement with [`FunctionFactory`].

**Traits**

- [`DataFilePaths`](#datafilepaths) - DataFilePaths adds a method to convert strings and vector of strings to vector of [`ListingTableUrl`] URLs.
- [`FunctionFactory`](#functionfactory) - Interface for handling `CREATE FUNCTION` statements and interacting with
- [`QueryPlanner`](#queryplanner) - A planner used to add extensions to DataFusion logical and physical plans.

---

## datafusion::execution::context::DataFilePaths

*Trait*

DataFilePaths adds a method to convert strings and vector of strings to vector of [`ListingTableUrl`] URLs.
This allows methods such [`SessionContext::read_csv`] and [`SessionContext::read_avro`]
to take either a single file or multiple files.

**Methods:**

- `to_urls`: Parse to a vector of [`ListingTableUrl`] URLs.



## datafusion::execution::context::EmptySerializerRegistry

*Struct*

Default implementation of [SerializerRegistry] that throws unimplemented error
for all requests.

**Unit Struct**

**Trait Implementations:**

- **SerializerRegistry**
  - `fn serialize_logical_plan(self: &Self, node: &dyn UserDefinedLogicalNode) -> Result<Vec<u8>>`
  - `fn deserialize_logical_plan(self: &Self, name: &str, _bytes: &[u8]) -> Result<Arc<dyn UserDefinedLogicalNode>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion::execution::context::FunctionFactory

*Trait*

Interface for handling `CREATE FUNCTION` statements and interacting with
[SessionState] to create and register functions ([`ScalarUDF`],
[`AggregateUDF`], [`WindowUDF`], and [`TableFunctionImpl`]) dynamically.

Implement this trait to create user-defined functions in a custom way, such
as loading from external libraries or defining them programmatically.
DataFusion will parse `CREATE FUNCTION` statements into [`CreateFunction`]
structs and pass them to the [`create`](Self::create) method.

Note there is no default implementation of this trait provided in DataFusion,
because the implementation and requirements vary widely. Please see
[function_factory example] for a reference implementation.

[function_factory example]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/function_factory.rs

# Examples of syntax that can be supported

```sql
CREATE FUNCTION f1(BIGINT)
  RETURNS BIGINT
  RETURN $1 + 1;
```
or
```sql
CREATE FUNCTION to_miles(DOUBLE)
RETURNS DOUBLE
LANGUAGE PYTHON
AS '
import pyarrow.compute as pc

conversation_rate_multiplier = 0.62137119

def to_miles(km_data):
    return pc.multiply(km_data, conversation_rate_multiplier)
'
```

**Methods:**

- `create`: Creates a new dynamic function from the SQL in the [CreateFunction] statement



## datafusion::execution::context::QueryPlanner

*Trait*

A planner used to add extensions to DataFusion logical and physical plans.

**Methods:**

- `create_physical_plan`: Given a [`LogicalPlan`], create an [`ExecutionPlan`] suitable for execution



## datafusion::execution::context::RegisterFunction

*Enum*

The result of processing a [`CreateFunction`] statement with [`FunctionFactory`].

**Variants:**
- `Scalar(std::sync::Arc<crate::logical_expr::ScalarUDF>)` - Scalar user defined function
- `Aggregate(std::sync::Arc<crate::logical_expr::AggregateUDF>)` - Aggregate user defined function
- `Window(std::sync::Arc<datafusion_expr::WindowUDF>)` - Window user defined function
- `Table(String, std::sync::Arc<dyn TableFunctionImpl>)` - Table user defined function

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> RegisterFunction`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion::execution::context::SQLOptions

*Struct*

Describes which SQL statements can be run.

See [`SessionContext::sql_with_options`] for more details.

**Methods:**

- `fn new() -> Self` - Create a new `SQLOptions` with default values
- `fn with_allow_ddl(self: Self, allow: bool) -> Self` - Should DDL data definition commands  (e.g. `CREATE TABLE`) be run? Defaults to `true`.
- `fn with_allow_dml(self: Self, allow: bool) -> Self` - Should DML data modification commands (e.g. `INSERT` and `COPY`) be run? Defaults to `true`
- `fn with_allow_statements(self: Self, allow: bool) -> Self` - Should Statements such as (e.g. `SET VARIABLE and `BEGIN TRANSACTION` ...`) be run?. Defaults to `true`
- `fn verify_plan(self: &Self, plan: &LogicalPlan) -> Result<()>` - Return an error if the [`LogicalPlan`] has any nodes that are

**Traits:** Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> SQLOptions`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion::execution::context::SessionContext

*Struct*

Main interface for executing queries with DataFusion. Maintains
the state of the connection between a user and an instance of the
DataFusion engine.

See examples below for how to use the `SessionContext` to execute queries
and how to configure the session.

# Overview

[`SessionContext`] provides the following functionality:

* Create a [`DataFrame`] from a CSV or Parquet data source.
* Register a CSV or Parquet data source as a table that can be referenced from a SQL query.
* Register a custom data source that can be referenced from a SQL query.
* Execution a SQL query

# Example: DataFrame API

The following example demonstrates how to use the context to execute a query against a CSV
data source using the [`DataFrame`] API:

```
use datafusion::prelude::*;
# use datafusion::functions_aggregate::expr_fn::min;
# use datafusion::{error::Result, assert_batches_eq};
# #[tokio::main]
# async fn main() -> Result<()> {
let ctx = SessionContext::new();
let df = ctx
    .read_csv("tests/data/example.csv", CsvReadOptions::new())
    .await?;
let df = df
    .filter(col("a").lt_eq(col("b")))?
    .aggregate(vec![col("a")], vec![min(col("b"))])?
    .limit(0, Some(100))?;
let results = df.collect().await?;
assert_batches_eq!(
    &[
        "+---+----------------+",
        "| a | min(?table?.b) |",
        "+---+----------------+",
        "| 1 | 2              |",
        "+---+----------------+",
    ],
    &results
);
# Ok(())
# }
```

# Example: SQL API

The following example demonstrates how to execute the same query using SQL:

```
use datafusion::prelude::*;
# use datafusion::{error::Result, assert_batches_eq};
# #[tokio::main]
# async fn main() -> Result<()> {
let ctx = SessionContext::new();
ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new())
    .await?;
let results = ctx
    .sql("SELECT a, min(b) FROM example GROUP BY a LIMIT 100")
    .await?
    .collect()
    .await?;
assert_batches_eq!(
    &[
        "+---+----------------+",
        "| a | min(example.b) |",
        "+---+----------------+",
        "| 1 | 2              |",
        "+---+----------------+",
    ],
    &results
);
# Ok(())
# }
```

# Example: Configuring `SessionContext`

The `SessionContext` can be configured by creating a [`SessionState`] using
[`SessionStateBuilder`]:

```
# use std::sync::Arc;
# use datafusion::prelude::*;
# use datafusion::execution::SessionStateBuilder;
# use datafusion_execution::runtime_env::RuntimeEnvBuilder;
// Configure a 4k batch size
let config = SessionConfig::new().with_batch_size(4 * 1024);

// configure a memory limit of 1GB with 20%  slop
let runtime_env = RuntimeEnvBuilder::new()
    .with_memory_limit(1024 * 1024 * 1024, 0.80)
    .build_arc()
    .unwrap();

// Create a SessionState using the config and runtime_env
let state = SessionStateBuilder::new()
    .with_config(config)
    .with_runtime_env(runtime_env)
    // include support for built in functions and configurations
    .with_default_features()
    .build();

// Create a SessionContext
let ctx = SessionContext::from(state);
```

# Relationship between `SessionContext`, `SessionState`, and `TaskContext`

The state required to optimize, and evaluate queries is
broken into three levels to allow tailoring

The objects are:

1. [`SessionContext`]: Most users should use a `SessionContext`. It contains
   all information required to execute queries including  high level APIs such
   as [`SessionContext::sql`]. All queries run with the same `SessionContext`
   share the same configuration and resources (e.g. memory limits).

2. [`SessionState`]: contains information required to plan and execute an
   individual query (e.g. creating a [`LogicalPlan`] or [`ExecutionPlan`]).
   Each query is planned and executed using its own `SessionState`, which can
   be created with [`SessionContext::state`]. `SessionState` allows finer
   grained control over query execution, for example disallowing DDL operations
   such as `CREATE TABLE`.

3. [`TaskContext`] contains the state required for query execution (e.g.
   [`ExecutionPlan::execute`]). It contains a subset of information in
   [`SessionState`]. `TaskContext` allows executing [`ExecutionPlan`]s
   [`PhysicalExpr`]s without requiring a full [`SessionState`].

[`PhysicalExpr`]: crate::physical_expr::PhysicalExpr

**Methods:**

- `fn new() -> Self` - Creates a new `SessionContext` using the default [`SessionConfig`].
- `fn refresh_catalogs(self: &Self) -> Result<()>` - Finds any [`ListingSchemaProvider`]s and instructs them to reload tables from "disk"
- `fn new_with_config(config: SessionConfig) -> Self` - Creates a new `SessionContext` using the provided
- `fn new_with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self` - Creates a new `SessionContext` using the provided
- `fn new_with_state(state: SessionState) -> Self` - Creates a new `SessionContext` using the provided [`SessionState`]
- `fn enable_url_table(self: Self) -> Self` - Enable querying local files as tables.
- `fn into_state_builder(self: Self) -> SessionStateBuilder` - Convert the current `SessionContext` into a [`SessionStateBuilder`]
- `fn session_start_time(self: &Self) -> DateTime<Utc>` - Returns the time this `SessionContext` was created
- `fn with_function_factory(self: Self, function_factory: Arc<dyn FunctionFactory>) -> Self` - Registers a [`FunctionFactory`] to handle `CREATE FUNCTION` statements
- `fn add_optimizer_rule(self: &Self, optimizer_rule: Arc<dyn OptimizerRule>)` - Adds an optimizer rule to the end of the existing rules.
- `fn remove_optimizer_rule(self: &Self, name: &str) -> bool` - Removes an optimizer rule by name, returning `true` if it existed.
- `fn add_analyzer_rule(self: &Self, analyzer_rule: Arc<dyn AnalyzerRule>)` - Adds an analyzer rule to the end of the existing rules.
- `fn register_object_store(self: &Self, url: &Url, object_store: Arc<dyn ObjectStore>) -> Option<Arc<dyn ObjectStore>>` - Registers an [`ObjectStore`] to be used with a specific URL prefix.
- `fn deregister_object_store(self: &Self, url: &Url) -> Result<Arc<dyn ObjectStore>>` - Deregisters an [`ObjectStore`] associated with the specific URL prefix.
- `fn register_batch(self: &Self, table_name: &str, batch: RecordBatch) -> Result<Option<Arc<dyn TableProvider>>>` - Registers the [`RecordBatch`] as the specified table name
- `fn runtime_env(self: &Self) -> Arc<RuntimeEnv>` - Return the [RuntimeEnv] used to run queries with this `SessionContext`
- `fn session_id(self: &Self) -> String` - Returns an id that uniquely identifies this `SessionContext`.
- `fn table_factory(self: &Self, file_type: &str) -> Option<Arc<dyn TableProviderFactory>>` - Return the [`TableProviderFactory`] that is registered for the
- `fn enable_ident_normalization(self: &Self) -> bool` - Return the `enable_ident_normalization` of this Session
- `fn copied_config(self: &Self) -> SessionConfig` - Return a copied version of config for this Session
- `fn copied_table_options(self: &Self) -> TableOptions` - Return a copied version of table options for this Session
- `fn sql(self: &Self, sql: &str) -> Result<DataFrame>` - Creates a [`DataFrame`] from SQL query text.
- `fn sql_with_options(self: &Self, sql: &str, options: SQLOptions) -> Result<DataFrame>` - Creates a [`DataFrame`] from SQL query text, first validating
- `fn parse_sql_expr(self: &Self, sql: &str, df_schema: &DFSchema) -> Result<Expr>` - Creates logical expressions from SQL query text.
- `fn execute_logical_plan(self: &Self, plan: LogicalPlan) -> Result<DataFrame>` - Execute the [`LogicalPlan`], return a [`DataFrame`]. This API
- `fn create_physical_expr(self: &Self, expr: Expr, df_schema: &DFSchema) -> Result<Arc<dyn PhysicalExpr>>` - Create a [`PhysicalExpr`] from an [`Expr`] after applying type
- `fn parse_memory_limit(limit: &str) -> Result<usize>` - Parse memory limit from string to number of bytes
- `fn register_variable(self: &Self, variable_type: VarType, provider: Arc<dyn VarProvider>)` - Registers a variable provider within this context.
- `fn register_udtf(self: &Self, name: &str, fun: Arc<dyn TableFunctionImpl>)` - Register a table UDF with this context
- `fn register_udf(self: &Self, f: ScalarUDF)` - Registers a scalar UDF within this context.
- `fn register_udaf(self: &Self, f: AggregateUDF)` - Registers an aggregate UDF within this context.
- `fn register_udwf(self: &Self, f: WindowUDF)` - Registers a window UDF within this context.
- `fn register_relation_planner(self: &Self, planner: Arc<dyn RelationPlanner>) -> Result<()>` - Registers a [`RelationPlanner`] to customize SQL table-factor planning.
- `fn deregister_udf(self: &Self, name: &str)` - Deregisters a UDF within this context.
- `fn deregister_udaf(self: &Self, name: &str)` - Deregisters a UDAF within this context.
- `fn deregister_udwf(self: &Self, name: &str)` - Deregisters a UDWF within this context.
- `fn deregister_udtf(self: &Self, name: &str)` - Deregisters a UDTF within this context.
- `fn read_arrow<P>(self: &Self, table_paths: P, options: ArrowReadOptions) -> Result<DataFrame>` - Creates a [`DataFrame`] for reading an Arrow data source.
- `fn read_empty(self: &Self) -> Result<DataFrame>` - Creates an empty DataFrame.
- `fn read_table(self: &Self, provider: Arc<dyn TableProvider>) -> Result<DataFrame>` - Creates a [`DataFrame`] for a [`TableProvider`] such as a
- `fn read_batch(self: &Self, batch: RecordBatch) -> Result<DataFrame>` - Creates a [`DataFrame`] for reading a [`RecordBatch`]
- `fn read_batches<impl IntoIterator<Item = RecordBatch>>(self: &Self, batches: impl Trait) -> Result<DataFrame>` - Create a [`DataFrame`] for reading a [`Vec[`RecordBatch`]`]
- `fn register_listing_table<impl Into<TableReference>, impl AsRef<str>>(self: &Self, table_ref: impl Trait, table_path: impl Trait, options: ListingOptions, provided_schema: Option<SchemaRef>, sql_definition: Option<String>) -> Result<()>` - Registers a [`ListingTable`] that can assemble multiple files
- `fn register_arrow(self: &Self, name: &str, table_path: &str, options: ArrowReadOptions) -> Result<()>` - Registers an Arrow file as a table that can be referenced from
- `fn register_catalog<impl Into<String>>(self: &Self, name: impl Trait, catalog: Arc<dyn CatalogProvider>) -> Option<Arc<dyn CatalogProvider>>` - Registers a named catalog using a custom `CatalogProvider` so that
- `fn catalog_names(self: &Self) -> Vec<String>` - Retrieves the list of available catalog names.
- `fn catalog(self: &Self, name: &str) -> Option<Arc<dyn CatalogProvider>>` - Retrieves a [`CatalogProvider`] instance by name
- `fn register_table<impl Into<TableReference>>(self: &Self, table_ref: impl Trait, provider: Arc<dyn TableProvider>) -> Result<Option<Arc<dyn TableProvider>>>` - Registers a [`TableProvider`] as a table that can be
- `fn deregister_table<impl Into<TableReference>>(self: &Self, table_ref: impl Trait) -> Result<Option<Arc<dyn TableProvider>>>` - Deregisters the given table.
- `fn table_exist<impl Into<TableReference>>(self: &Self, table_ref: impl Trait) -> Result<bool>` - Return `true` if the specified table exists in the schema provider.
- `fn table<impl Into<TableReference>>(self: &Self, table_ref: impl Trait) -> Result<DataFrame>` - Retrieves a [`DataFrame`] representing a table previously
- `fn table_function(self: &Self, name: &str) -> Result<Arc<TableFunction>>` - Retrieves a [`TableFunction`] reference by name.
- `fn table_provider<impl Into<TableReference>>(self: &Self, table_ref: impl Trait) -> Result<Arc<dyn TableProvider>>` - Return a [`TableProvider`] for the specified table.
- `fn task_ctx(self: &Self) -> Arc<TaskContext>` - Get a new TaskContext to run in this session
- `fn state(self: &Self) -> SessionState` - Return a new  [`SessionState`] suitable for executing a single query.
- `fn state_ref(self: &Self) -> Arc<RwLock<SessionState>>` - Get reference to [`SessionState`]
- `fn state_weak_ref(self: &Self) -> Weak<RwLock<SessionState>>` - Get weak reference to [`SessionState`]
- `fn register_catalog_list(self: &Self, catalog_list: Arc<dyn CatalogProviderList>)` - Register [`CatalogProviderList`] in [`SessionState`]
- `fn register_table_options_extension<T>(self: &Self, extension: T)` - Registers a [`ConfigExtension`] as a table option extension that can be
- `fn read_parquet<P>(self: &Self, table_paths: P, options: ParquetReadOptions) -> Result<DataFrame>` - Creates a [`DataFrame`] for reading a Parquet data source.
- `fn register_parquet<impl Into<TableReference>, impl AsRef<str>>(self: &Self, table_ref: impl Trait, table_path: impl Trait, options: ParquetReadOptions) -> Result<()>` - Registers a Parquet file as a table that can be referenced from SQL
- `fn write_parquet<impl AsRef<str>>(self: &Self, plan: Arc<dyn ExecutionPlan>, path: impl Trait, writer_properties: Option<WriterProperties>) -> Result<()>` - Executes a query and writes the results to a partitioned Parquet file.
- `fn read_json<P>(self: &Self, table_paths: P, options: NdJsonReadOptions) -> Result<DataFrame>` - Creates a [`DataFrame`] for reading an JSON data source.
- `fn register_json<impl Into<TableReference>, impl AsRef<str>>(self: &Self, table_ref: impl Trait, table_path: impl Trait, options: NdJsonReadOptions) -> Result<()>` - Registers a JSON file as a table that it can be referenced
- `fn write_json<impl AsRef<str>>(self: &Self, plan: Arc<dyn ExecutionPlan>, path: impl Trait) -> Result<()>` - Executes a query and writes the results to a partitioned JSON file.
- `fn read_csv<P>(self: &Self, table_paths: P, options: CsvReadOptions) -> Result<DataFrame>` - Creates a [`DataFrame`] for reading a CSV data source.
- `fn register_csv<impl Into<TableReference>, impl AsRef<str>>(self: &Self, table_ref: impl Trait, table_path: impl Trait, options: CsvReadOptions) -> Result<()>` - Registers a CSV file as a table which can referenced from SQL
- `fn write_csv<impl AsRef<str>>(self: &Self, plan: Arc<dyn ExecutionPlan>, path: impl Trait) -> Result<()>` - Executes a query and writes the results to a partitioned CSV file.

**Trait Implementations:**

- **FunctionRegistry**
  - `fn udfs(self: &Self) -> HashSet<String>`
  - `fn udf(self: &Self, name: &str) -> Result<Arc<ScalarUDF>>`
  - `fn udaf(self: &Self, name: &str) -> Result<Arc<AggregateUDF>>`
  - `fn udwf(self: &Self, name: &str) -> Result<Arc<WindowUDF>>`
  - `fn register_udf(self: & mut Self, udf: Arc<ScalarUDF>) -> Result<Option<Arc<ScalarUDF>>>`
  - `fn register_udaf(self: & mut Self, udaf: Arc<AggregateUDF>) -> Result<Option<Arc<AggregateUDF>>>`
  - `fn register_udwf(self: & mut Self, udwf: Arc<WindowUDF>) -> Result<Option<Arc<WindowUDF>>>`
  - `fn register_function_rewrite(self: & mut Self, rewrite: Arc<dyn FunctionRewrite>) -> Result<()>`
  - `fn expr_planners(self: &Self) -> Vec<Arc<dyn ExprPlanner>>`
  - `fn register_expr_planner(self: & mut Self, expr_planner: Arc<dyn ExprPlanner>) -> Result<()>`
  - `fn udafs(self: &Self) -> HashSet<String>`
  - `fn udwfs(self: &Self) -> HashSet<String>`
- **Default**
  - `fn default() -> Self`
- **From**
  - `fn from(state: SessionState) -> Self`
- **TaskContextProvider**
  - `fn task_ctx(self: &Self) -> Arc<TaskContext>`
- **Clone**
  - `fn clone(self: &Self) -> SessionContext`



