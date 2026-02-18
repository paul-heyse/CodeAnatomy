**datafusion_datasource > file**

# Module: file

## Contents

**Functions**

- [`as_file_source`](#as_file_source) - Helper function to convert any type implementing FileSource to Arc&lt;dyn FileSource&gt;

**Traits**

- [`FileSource`](#filesource) - file format specific behaviors for elements in [`DataSource`]

---

## datafusion_datasource::file::FileSource

*Trait*

file format specific behaviors for elements in [`DataSource`]

See more details on specific implementations:
* [`ArrowSource`](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.ArrowSource.html)
* [`AvroSource`](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.AvroSource.html)
* [`CsvSource`](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.CsvSource.html)
* [`JsonSource`](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.JsonSource.html)
* [`ParquetSource`](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.ParquetSource.html)

[`DataSource`]: crate::source::DataSource

**Methods:**

- `create_file_opener`: Creates a `dyn FileOpener` based on given parameters
- `as_any`: Any
- `table_schema`: Returns the table schema for this file source.
- `with_batch_size`: Initialize new type with batch size configuration
- `filter`: Returns the filter expression that will be applied during the file scan.
- `projection`: Return the projection that will be applied to the output stream on top of the table schema.
- `metrics`: Return execution plan metrics
- `file_type`: String representation of file source such as "csv", "json", "parquet"
- `fmt_extra`: Format FileType specific information
- `supports_repartitioning`: Returns whether this file source supports repartitioning files by byte ranges.
- `repartitioned`: If supported by the [`FileSource`], redistribute files across partitions
- `try_pushdown_filters`: Try to push down filters into this FileSource.
- `try_reverse_output`: Try to create a new FileSource that can produce data in the specified sort order.
- `try_pushdown_projection`: Try to push down a projection into a this FileSource.
- `with_schema_adapter_factory`: Deprecated: Set optional schema adapter factory.
- `schema_adapter_factory`: Deprecated: Returns the current schema adapter factory if set.



## datafusion_datasource::file::as_file_source

*Function*

Helper function to convert any type implementing FileSource to Arc&lt;dyn FileSource&gt;

```rust
fn as_file_source<T>(source: T) -> std::sync::Arc<dyn FileSource>
```



