**datafusion_datasource > file_sink_config**

# Module: file_sink_config

## Contents

**Structs**

- [`FileSinkConfig`](#filesinkconfig) - The base configurations to provide when creating a physical plan for

**Traits**

- [`FileSink`](#filesink) - General behaviors for files that do `DataSink` operations

---

## datafusion_datasource::file_sink_config::FileSink

*Trait*

General behaviors for files that do `DataSink` operations

**Methods:**

- `config`: Retrieves the file sink configuration.
- `spawn_writer_tasks_and_join`: Spawns writer tasks and joins them to perform file writing operations.
- `write_all`: File sink implementation of the [`DataSink::write_all`] method.



## datafusion_datasource::file_sink_config::FileSinkConfig

*Struct*

The base configurations to provide when creating a physical plan for
writing to any given file format.

**Fields:**
- `original_url: String` - The unresolved URL specified by the user
- `object_store_url: datafusion_execution::object_store::ObjectStoreUrl` - Object store URL, used to get an ObjectStore instance
- `file_group: crate::file_groups::FileGroup` - A collection of files organized into groups.
- `table_paths: Vec<crate::ListingTableUrl>` - Vector of partition paths
- `output_schema: arrow::datatypes::SchemaRef` - The schema of the output file
- `table_partition_cols: Vec<(String, arrow::datatypes::DataType)>` - A vector of column names and their corresponding data types,
- `insert_op: datafusion_expr::dml::InsertOp` - Controls how new data should be written to the file, determining whether
- `keep_partition_by_columns: bool` - Controls whether partition columns are kept for the file
- `file_extension: String` - File extension without a dot(.)

**Methods:**

- `fn output_schema(self: &Self) -> &SchemaRef` - Get output schema

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> FileSinkConfig`



