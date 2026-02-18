**deltalake_core > operations > convert_to_delta**

# Module: operations::convert_to_delta

## Contents

**Structs**

- [`ConvertToDeltaBuilder`](#converttodeltabuilder) - Build an operation to convert a Parquet table to a [`DeltaTable`] in place

**Enums**

- [`PartitionStrategy`](#partitionstrategy) - The partition strategy used by the Parquet table

---

## deltalake_core::operations::convert_to_delta::ConvertToDeltaBuilder

*Struct*

Build an operation to convert a Parquet table to a [`DeltaTable`] in place

**Methods:**

- `fn new() -> Self` - Create a new [`ConvertToDeltaBuilder`]
- `fn with_log_store(self: Self, log_store: Arc<dyn LogStore>) -> Self` - Provide a [`LogStore`] instance, that points at table location
- `fn with_location<impl Into<String>>(self: Self, location: impl Trait) -> Self` - Specify the path to the location where table data is stored,
- `fn with_storage_options(self: Self, storage_options: HashMap<String, String>) -> Self` - Set options used to initialize storage backend
- `fn with_partition_schema<impl IntoIterator<Item = StructField>>(self: Self, partition_schema: impl Trait) -> Self` - Specify the partition schema of the Parquet table
- `fn with_partition_strategy(self: Self, strategy: PartitionStrategy) -> Self` - Specify the partition strategy of the Parquet table
- `fn with_save_mode(self: Self, save_mode: SaveMode) -> Self` - Specify the behavior when a table exists at location
- `fn with_table_name<impl Into<String>>(self: Self, name: impl Trait) -> Self` - Specify the table name. Optionally qualified with
- `fn with_comment<impl Into<String>>(self: Self, comment: impl Trait) -> Self` - Comment to describe the table.
- `fn with_configuration<impl Into<String>, impl Into<String>, impl IntoIterator<Item = (impl Into<String>, Option<impl Into<String>>)>>(self: Self, configuration: impl Trait) -> Self` - Set configuration on created table
- `fn with_configuration_property<impl Into<String>>(self: Self, key: TableProperty, value: Option<impl Trait>) -> Self` - Specify a table property in the table configuration
- `fn with_commit_properties(self: Self, commit_properties: CommitProperties) -> Self` - Additional metadata to be added to commit info
- `fn with_custom_execute_handler(self: Self, handler: Arc<dyn CustomExecuteHandler>) -> Self` - Set a custom execute handler, for pre and post execution

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`
- **Default**
  - `fn default() -> Self`



## deltalake_core::operations::convert_to_delta::PartitionStrategy

*Enum*

The partition strategy used by the Parquet table
Currently only hive-partitioning is supported for Parquet paths

**Variants:**
- `Hive` - Hive-partitioning

**Trait Implementations:**

- **Default**
  - `fn default() -> PartitionStrategy`
- **FromStr**
  - `fn from_str(s: &str) -> DeltaResult<Self>`



