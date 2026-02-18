**datafusion > dataframe**

# Module: dataframe

## Contents

**Structs**

- [`DataFrame`](#dataframe) - Represents a logical set of rows with the same named columns.
- [`DataFrameWriteOptions`](#dataframewriteoptions) - Contains options that control how data is

---

## datafusion::dataframe::DataFrame

*Struct*

Represents a logical set of rows with the same named columns.

Similar to a [Pandas DataFrame] or [Spark DataFrame], a DataFusion DataFrame
represents a 2 dimensional table of rows and columns.

The typical workflow using DataFrames looks like

1. Create a DataFrame via methods on [SessionContext], such as [`read_csv`]
   and [`read_parquet`].

2. Build a desired calculation by calling methods such as [`filter`],
   [`select`], [`aggregate`], and [`limit`]

3. Execute into [`RecordBatch`]es by calling [`collect`]

A `DataFrame` is a wrapper around a [`LogicalPlan`] and the [`SessionState`]
   required for execution.

DataFrames are "lazy" in the sense that most methods do not actually compute
anything, they just build up a plan. Calling [`collect`] executes the plan
using the same DataFusion planning and execution process used to execute SQL
and other queries.

[Pandas DataFrame]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
[Spark DataFrame]: https://spark.apache.org/docs/latest/sql-programming-guide.html
[`read_csv`]: SessionContext::read_csv
[`read_parquet`]: SessionContext::read_parquet
[`filter`]: DataFrame::filter
[`select`]: DataFrame::select
[`aggregate`]: DataFrame::aggregate
[`limit`]: DataFrame::limit
[`collect`]: DataFrame::collect

# Example
```
# use std::sync::Arc;
# use datafusion::prelude::*;
# use datafusion::error::Result;
# use datafusion::functions_aggregate::expr_fn::min;
# use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
# use datafusion::arrow::datatypes::{DataType, Field, Schema};
# #[tokio::main]
# async fn main() -> Result<()> {
let ctx = SessionContext::new();
// Read the data from a csv file
let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
// create a new dataframe that computes the equivalent of
// `SELECT a, MIN(b) FROM df WHERE a <= b GROUP BY a LIMIT 100;`
let df = df.filter(col("a").lt_eq(col("b")))?
           .aggregate(vec![col("a")], vec![min(col("b"))])?
           .limit(0, Some(100))?;
// Perform the actual computation
let results = df.collect();

// Create a new dataframe with in-memory data
let schema = Schema::new(vec![
    Field::new("id", DataType::Int32, true),
    Field::new("name", DataType::Utf8, true),
]);
let batch = RecordBatch::try_new(
    Arc::new(schema),
    vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
    ],
)?;
let df = ctx.read_batch(batch)?;
df.show().await?;

// Create a new dataframe with in-memory data using macro
let df = dataframe!(
    "id" => [1, 2, 3],
    "name" => ["foo", "bar", "baz"]
 )?;
df.show().await?;
# Ok(())
# }
```

**Methods:**

- `fn write_parquet(self: Self, path: &str, options: DataFrameWriteOptions, writer_options: Option<TableParquetOptions>) -> Result<Vec<RecordBatch>, DataFusionError>` - Execute the `DataFrame` and write the results to Parquet file(s).
- `fn new(session_state: SessionState, plan: LogicalPlan) -> Self` - Create a new `DataFrame ` based on an existing `LogicalPlan`
- `fn parse_sql_expr(self: &Self, sql: &str) -> Result<Expr>` - Creates logical expression from a SQL query text.
- `fn create_physical_plan(self: Self) -> Result<Arc<dyn ExecutionPlan>>` - Consume the DataFrame and produce a physical plan
- `fn select_columns(self: Self, columns: &[&str]) -> Result<DataFrame>` - Filter the DataFrame by column. Returns a new DataFrame only containing the
- `fn select_exprs(self: Self, exprs: &[&str]) -> Result<DataFrame>` - Project arbitrary list of expression strings into a new `DataFrame`.
- `fn select<impl Into<SelectExpr>, impl IntoIterator<Item = impl Into<SelectExpr>>>(self: Self, expr_list: impl Trait) -> Result<DataFrame>` - Project arbitrary expressions (like SQL SELECT expressions) into a new
- `fn drop_columns(self: Self, columns: &[&str]) -> Result<DataFrame>` - Returns a new DataFrame containing all columns except the specified columns.
- `fn unnest_columns(self: Self, columns: &[&str]) -> Result<DataFrame>` - Expand multiple list/struct columns into a set of rows and new columns.
- `fn unnest_columns_with_options(self: Self, columns: &[&str], options: UnnestOptions) -> Result<DataFrame>` - Expand multiple list columns into a set of rows, with
- `fn filter(self: Self, predicate: Expr) -> Result<DataFrame>` - Return a DataFrame with only rows for which `predicate` evaluates to
- `fn aggregate(self: Self, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> Result<DataFrame>` - Return a new `DataFrame` that aggregates the rows of the current
- `fn window(self: Self, window_exprs: Vec<Expr>) -> Result<DataFrame>` - Return a new DataFrame that adds the result of evaluating one or more
- `fn limit(self: Self, skip: usize, fetch: Option<usize>) -> Result<DataFrame>` - Returns a new `DataFrame` with a limited number of rows.
- `fn union(self: Self, dataframe: DataFrame) -> Result<DataFrame>` - Calculate the union of two [`DataFrame`]s, preserving duplicate rows.
- `fn union_by_name(self: Self, dataframe: DataFrame) -> Result<DataFrame>` - Calculate the union of two [`DataFrame`]s using column names, preserving duplicate rows.
- `fn union_distinct(self: Self, dataframe: DataFrame) -> Result<DataFrame>` - Calculate the distinct union of two [`DataFrame`]s.
- `fn union_by_name_distinct(self: Self, dataframe: DataFrame) -> Result<DataFrame>` - Calculate the union of two [`DataFrame`]s using column names with all duplicated rows removed.
- `fn distinct(self: Self) -> Result<DataFrame>` - Return a new `DataFrame` with all duplicated rows removed.
- `fn distinct_on(self: Self, on_expr: Vec<Expr>, select_expr: Vec<Expr>, sort_expr: Option<Vec<SortExpr>>) -> Result<DataFrame>` - Return a new `DataFrame` with duplicated rows removed as per the specified expression list
- `fn describe(self: Self) -> Result<Self>` - Return a new `DataFrame` that has statistics for a DataFrame.
- `fn sort_by(self: Self, expr: Vec<Expr>) -> Result<DataFrame>` - Apply a sort by provided expressions with default direction
- `fn sort(self: Self, expr: Vec<SortExpr>) -> Result<DataFrame>` - Sort the DataFrame by the specified sorting expressions.
- `fn join(self: Self, right: DataFrame, join_type: JoinType, left_cols: &[&str], right_cols: &[&str], filter: Option<Expr>) -> Result<DataFrame>` - Join this `DataFrame` with another `DataFrame` using explicitly specified
- `fn join_on<impl IntoIterator<Item = Expr>>(self: Self, right: DataFrame, join_type: JoinType, on_exprs: impl Trait) -> Result<DataFrame>` - Join this `DataFrame` with another `DataFrame` using the specified
- `fn repartition(self: Self, partitioning_scheme: Partitioning) -> Result<DataFrame>` - Repartition a DataFrame based on a logical partitioning scheme.
- `fn count(self: Self) -> Result<usize>` - Return the total number of rows in this `DataFrame`.
- `fn collect(self: Self) -> Result<Vec<RecordBatch>>` - Execute this `DataFrame` and buffer all resulting `RecordBatch`es  into memory.
- `fn show(self: Self) -> Result<()>` - Execute the `DataFrame` and print the results to the console.
- `fn to_string(self: Self) -> Result<String>` - Execute the `DataFrame` and return a string representation of the results.
- `fn show_limit(self: Self, num: usize) -> Result<()>` - Execute the `DataFrame` and print only the first `num` rows of the
- `fn task_ctx(self: &Self) -> TaskContext` - Return a new [`TaskContext`] which would be used to execute this DataFrame
- `fn execute_stream(self: Self) -> Result<SendableRecordBatchStream>` - Executes this DataFrame and returns a stream over a single partition
- `fn collect_partitioned(self: Self) -> Result<Vec<Vec<RecordBatch>>>` - Executes this DataFrame and collects all results into a vector of vector of RecordBatch
- `fn execute_stream_partitioned(self: Self) -> Result<Vec<SendableRecordBatchStream>>` - Executes this DataFrame and returns one stream per partition.
- `fn schema(self: &Self) -> &DFSchema` - Returns the `DFSchema` describing the output of this DataFrame.
- `fn logical_plan(self: &Self) -> &LogicalPlan` - Return a reference to the unoptimized [`LogicalPlan`] that comprises
- `fn into_parts(self: Self) -> (SessionState, LogicalPlan)` - Returns both the [`LogicalPlan`] and [`SessionState`] that comprise this [`DataFrame`]
- `fn into_unoptimized_plan(self: Self) -> LogicalPlan` - Return the [`LogicalPlan`] represented by this DataFrame without running
- `fn into_optimized_plan(self: Self) -> Result<LogicalPlan>` - Return the optimized [`LogicalPlan`] represented by this DataFrame.
- `fn into_view(self: Self) -> Arc<dyn TableProvider>` - Converts this [`DataFrame`] into a [`TableProvider`] that can be registered
- `fn into_temporary_view(self: Self) -> Arc<dyn TableProvider>` - See [`Self::into_view`]. The returned [`TableProvider`] will
- `fn explain(self: Self, verbose: bool, analyze: bool) -> Result<DataFrame>` - Return a DataFrame with the explanation of its plan so far.
- `fn explain_with_options(self: Self, explain_option: ExplainOption) -> Result<DataFrame>` - Return a DataFrame with the explanation of its plan so far.
- `fn registry(self: &Self) -> &dyn FunctionRegistry` - Return a `FunctionRegistry` used to plan udf's calls
- `fn intersect(self: Self, dataframe: DataFrame) -> Result<DataFrame>` - Calculate the intersection of two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
- `fn intersect_distinct(self: Self, dataframe: DataFrame) -> Result<DataFrame>` - Calculate the distinct intersection of two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
- `fn except(self: Self, dataframe: DataFrame) -> Result<DataFrame>` - Calculate the exception of two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
- `fn except_distinct(self: Self, dataframe: DataFrame) -> Result<DataFrame>` - Calculate the distinct exception of two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
- `fn write_table(self: Self, table_name: &str, write_options: DataFrameWriteOptions) -> Result<Vec<RecordBatch>, DataFusionError>` - Execute this `DataFrame` and write the results to `table_name`.
- `fn write_csv(self: Self, path: &str, options: DataFrameWriteOptions, writer_options: Option<CsvOptions>) -> Result<Vec<RecordBatch>, DataFusionError>` - Execute the `DataFrame` and write the results to CSV file(s).
- `fn write_json(self: Self, path: &str, options: DataFrameWriteOptions, writer_options: Option<JsonOptions>) -> Result<Vec<RecordBatch>, DataFusionError>` - Execute the `DataFrame` and write the results to JSON file(s).
- `fn with_column(self: Self, name: &str, expr: Expr) -> Result<DataFrame>` - Add or replace a column in the DataFrame.
- `fn with_column_renamed<impl Into<String>>(self: Self, old_name: impl Trait, new_name: &str) -> Result<DataFrame>` - Rename one column by applying a new projection. This is a no-op if the column to be
- `fn with_param_values<impl Into<ParamValues>>(self: Self, query_values: impl Trait) -> Result<Self>` - Replace all parameters in logical plan with the specified
- `fn cache(self: Self) -> Result<DataFrame>` - Cache DataFrame as a memory table.
- `fn alias(self: Self, alias: &str) -> Result<DataFrame>` - Apply an alias to the DataFrame.
- `fn fill_null(self: &Self, value: ScalarValue, columns: Vec<String>) -> Result<DataFrame>` - Fill null values in specified columns with a given value
- `fn from_columns(columns: Vec<(&str, ArrayRef)>) -> Result<Self>` - Helper for creating DataFrame.

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> DataFrame`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion::dataframe::DataFrameWriteOptions

*Struct*

Contains options that control how data is
written out from a DataFrame

**Methods:**

- `fn new() -> Self` - Create a new DataFrameWriteOptions with default values
- `fn with_insert_operation(self: Self, insert_op: InsertOp) -> Self` - Set the insert operation
- `fn with_single_file_output(self: Self, single_file_output: bool) -> Self` - Set the single_file_output value to true or false
- `fn with_partition_by(self: Self, partition_by: Vec<String>) -> Self` - Sets the partition_by columns for output partitioning
- `fn with_sort_by(self: Self, sort_by: Vec<SortExpr>) -> Self` - Sets the sort_by columns for output sorting

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



