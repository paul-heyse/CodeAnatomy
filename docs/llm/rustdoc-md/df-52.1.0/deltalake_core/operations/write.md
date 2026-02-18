**deltalake_core > operations > write**

# Module: operations::write

## Contents

**Modules**

- [`configs`](#configs)
- [`writer`](#writer) - Abstractions and implementations for writing data to delta tables

**Structs**

- [`WriteBuilder`](#writebuilder) - Write data into a DeltaTable
- [`WriteMetrics`](#writemetrics) - Metrics for the Write Operation

**Enums**

- [`SchemaMode`](#schemamode) - Specifies how to handle schema drifts

---

## deltalake_core::operations::write::SchemaMode

*Enum*

Specifies how to handle schema drifts

**Variants:**
- `Overwrite` - Overwrite the schema with the new schema
- `Merge` - Append the new schema to the existing schema

**Traits:** Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> SchemaMode`
- **PartialEq**
  - `fn eq(self: &Self, other: &SchemaMode) -> bool`
- **FromStr**
  - `fn from_str(s: &str) -> DeltaResult<Self>`



## deltalake_core::operations::write::WriteBuilder

*Struct*

Write data into a DeltaTable

**Methods:**

- `fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self` - Create a new [`WriteBuilder`]
- `fn with_save_mode(self: Self, save_mode: SaveMode) -> Self` - Specify the behavior when a table exists at location
- `fn with_schema_mode(self: Self, schema_mode: SchemaMode) -> Self` - Add Schema Write Mode
- `fn with_replace_where<impl Into<Expression>>(self: Self, predicate: impl Trait) -> Self` - When using `Overwrite` mode, replace data that matches a predicate
- `fn with_partition_columns<impl Into<String>, impl IntoIterator<Item = impl Into<String>>>(self: Self, partition_columns: impl Trait) -> Self` - (Optional) Specify table partitioning. If specified, the partitioning is validated,
- `fn with_input_execution_plan(self: Self, plan: Arc<LogicalPlan>) -> Self` - Logical execution plan that produces the data to be written to the delta table
- `fn with_input_plan(self: Self, plan: LogicalPlan) -> Self` - Logical plan that produces the data to be written to the delta table
- `fn with_session_state(self: Self, session: Arc<dyn Session>) -> Self` - Set the DataFusion session used for planning and execution.
- `fn with_session_fallback_policy(self: Self, policy: SessionFallbackPolicy) -> Self` - Control how delta-rs resolves the provided session when it is not a concrete `SessionState`.
- `fn with_target_file_size(self: Self, target_file_size: usize) -> Self` - Specify the target file size for data files written to the delta table.
- `fn with_write_batch_size(self: Self, write_batch_size: usize) -> Self` - Specify the target batch size for row groups written to parquet files.
- `fn with_cast_safety(self: Self, safe: bool) -> Self` - Specify the safety of the casting operation
- `fn with_writer_properties(self: Self, writer_properties: WriterProperties) -> Self` - Specify the writer properties to use when writing a parquet file
- `fn with_commit_properties(self: Self, commit_properties: CommitProperties) -> Self` - Additional metadata to be added to commit info
- `fn with_table_name<impl Into<String>>(self: Self, name: impl Trait) -> Self` - Specify the table name. Optionally qualified with
- `fn with_description<impl Into<String>>(self: Self, description: impl Trait) -> Self` - Comment to describe the table.
- `fn with_custom_execute_handler(self: Self, handler: Arc<dyn CustomExecuteHandler>) -> Self` - Set a custom execute handler, for pre and post execution
- `fn with_configuration<impl Into<String>, impl Into<String>, impl IntoIterator<Item = (impl Into<String>, Option<impl Into<String>>)>>(self: Self, configuration: impl Trait) -> Self` - Set configuration on created table
- `fn with_input_batches<impl IntoIterator<Item = RecordBatch>>(self: Self, batches: impl Trait) -> Self` - Execution plan that produces the data to be written to the delta table

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`



## deltalake_core::operations::write::WriteMetrics

*Struct*

Metrics for the Write Operation

**Fields:**
- `num_added_files: usize` - Number of files added
- `num_removed_files: usize` - Number of files removed
- `num_partitions: usize` - Number of partitions
- `num_added_rows: usize` - Number of rows added
- `execution_time_ms: u64` - Time taken to execute the entire operation

**Trait Implementations:**

- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Default**
  - `fn default() -> WriteMetrics`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## Module: configs



## Module: writer

Abstractions and implementations for writing data to delta tables



