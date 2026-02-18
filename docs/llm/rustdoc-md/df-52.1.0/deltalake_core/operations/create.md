**deltalake_core > operations > create**

# Module: operations::create

## Contents

**Structs**

- [`CreateBuilder`](#createbuilder) - Build an operation to create a new [DeltaTable]

---

## deltalake_core::operations::create::CreateBuilder

*Struct*

Build an operation to create a new [DeltaTable]

**Methods:**

- `fn new() -> Self` - Create a new [`CreateBuilder`]
- `fn with_table_name<impl Into<String>>(self: Self, name: impl Trait) -> Self` - Specify the table name. Optionally qualified with
- `fn with_location<impl Into<String>>(self: Self, location: impl Trait) -> Self` - Specify the path to the location where table data is stored,
- `fn with_save_mode(self: Self, save_mode: SaveMode) -> Self` - Specify the behavior when a table exists at location
- `fn with_comment<impl Into<String>>(self: Self, comment: impl Trait) -> Self` - Comment to describe the table.
- `fn with_column<impl Into<String>>(self: Self, name: impl Trait, data_type: DataType, nullable: bool, metadata: Option<HashMap<String, Value>>) -> Self` - Specify a column in the table
- `fn with_columns<impl Into<StructField>, impl IntoIterator<Item = impl Into<StructField>>>(self: Self, columns: impl Trait) -> Self` - Specify columns to append to schema
- `fn with_partition_columns<impl Into<String>, impl IntoIterator<Item = impl Into<String>>>(self: Self, partition_columns: impl Trait) -> Self` - Specify table partitioning
- `fn with_storage_options(self: Self, storage_options: HashMap<String, String>) -> Self` - Set options used to initialize storage backend
- `fn with_configuration<impl Into<String>, impl Into<String>, impl IntoIterator<Item = (impl Into<String>, Option<impl Into<String>>)>>(self: Self, configuration: impl Trait) -> Self` - Set configuration on created table
- `fn with_configuration_property<impl Into<String>>(self: Self, key: TableProperty, value: Option<impl Trait>) -> Self` - Specify a table property in the table configuration
- `fn with_commit_properties(self: Self, commit_properties: CommitProperties) -> Self` - Additional metadata to be added to commit info
- `fn with_raise_if_key_not_exists(self: Self, raise_if_key_not_exists: bool) -> Self` - Specify whether to raise an error if the table properties in the configuration are not TableProperties
- `fn with_actions<impl IntoIterator<Item = Action>>(self: Self, actions: impl Trait) -> Self` - Specify additional actions to be added to the commit.
- `fn with_log_store(self: Self, log_store: LogStoreRef) -> Self` - Provide a [`LogStore`] instance
- `fn with_custom_execute_handler(self: Self, handler: Arc<dyn CustomExecuteHandler>) -> Self` - Set a custom execute handler, for pre and post execution

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`
- **Default**
  - `fn default() -> Self`
- **Clone**
  - `fn clone(self: &Self) -> CreateBuilder`



