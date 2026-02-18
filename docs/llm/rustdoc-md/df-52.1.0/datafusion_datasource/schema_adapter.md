**datafusion_datasource > schema_adapter**

# Module: schema_adapter

## Contents

**Structs**

- [`DefaultSchemaAdapterFactory`](#defaultschemaadapterfactory) - Deprecated: Default [`SchemaAdapterFactory`] for mapping schemas.
- [`SchemaMapping`](#schemamapping) - Deprecated: The SchemaMapping struct held a mapping from the file schema to the table schema.

**Traits**

- [`SchemaAdapter`](#schemaadapter) - Deprecated: Creates [`SchemaMapper`]s to map file-level [`RecordBatch`]es to a table schema.
- [`SchemaAdapterFactory`](#schemaadapterfactory) - Deprecated: Factory for creating [`SchemaAdapter`].
- [`SchemaMapper`](#schemamapper) - Deprecated: Maps columns from a specific file schema to the table schema.

**Type Aliases**

- [`CastColumnFn`](#castcolumnfn) - Deprecated: Function type for casting columns.

---

## datafusion_datasource::schema_adapter::CastColumnFn

*Type Alias*: `dyn Fn`

Deprecated: Function type for casting columns.

This type has been removed. Use [`PhysicalExprAdapterFactory`] instead.
See `upgrading.md` for more details.

[`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory



## datafusion_datasource::schema_adapter::DefaultSchemaAdapterFactory

*Struct*

Deprecated: Default [`SchemaAdapterFactory`] for mapping schemas.

This struct has been removed.

Use [`PhysicalExprAdapterFactory`] instead to customize scans via
[`FileScanConfigBuilder`], i.e. if you had implemented a custom [`SchemaAdapter`]
and passed that into [`FileScanConfigBuilder`] / [`ParquetSource`].
Use [`BatchAdapter`] if you want to map a stream of [`RecordBatch`]es
between one schema and another, i.e. if you were calling [`SchemaMapper::map_batch`] manually.

See `upgrading.md` for more details.

[`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory
[`FileScanConfigBuilder`]: crate::file_scan_config::FileScanConfigBuilder
[`ParquetSource`]: https://docs.rs/datafusion-datasource-parquet/latest/datafusion_datasource_parquet/source/struct.ParquetSource.html
[`BatchAdapter`]: datafusion_physical_expr_adapter::BatchAdapter

**Unit Struct**

**Methods:**

- `fn from_schema(table_schema: SchemaRef) -> Box<dyn SchemaAdapter>` - Deprecated: Create a new factory for mapping batches from a file schema to a table schema.

**Trait Implementations:**

- **Default**
  - `fn default() -> DefaultSchemaAdapterFactory`
- **Clone**
  - `fn clone(self: &Self) -> DefaultSchemaAdapterFactory`
- **SchemaAdapterFactory**
  - `fn create(self: &Self, projected_table_schema: SchemaRef, _table_schema: SchemaRef) -> Box<dyn SchemaAdapter>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_datasource::schema_adapter::SchemaAdapter

*Trait*

Deprecated: Creates [`SchemaMapper`]s to map file-level [`RecordBatch`]es to a table schema.

This trait has been removed. Use [`PhysicalExprAdapterFactory`] instead.
See `upgrading.md` for more details.

[`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory

**Methods:**

- `map_column_index`: Map a column index in the table schema to a column index in a particular file schema.
- `map_schema`: Creates a mapping for casting columns from the file schema to the table schema.



## datafusion_datasource::schema_adapter::SchemaAdapterFactory

*Trait*

Deprecated: Factory for creating [`SchemaAdapter`].

This trait has been removed. Use [`PhysicalExprAdapterFactory`] instead.
See `upgrading.md` for more details.

[`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory

**Methods:**

- `create`: Create a [`SchemaAdapter`]
- `create_with_projected_schema`: Create a [`SchemaAdapter`] using only the projected table schema.



## datafusion_datasource::schema_adapter::SchemaMapper

*Trait*

Deprecated: Maps columns from a specific file schema to the table schema.

This trait has been removed. Use [`PhysicalExprAdapterFactory`] instead.
See `upgrading.md` for more details.

[`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory

**Methods:**

- `map_batch`: Adapts a `RecordBatch` to match the `table_schema`.
- `map_column_statistics`: Adapts file-level column `Statistics` to match the `table_schema`.



## datafusion_datasource::schema_adapter::SchemaMapping

*Struct*

Deprecated: The SchemaMapping struct held a mapping from the file schema to the table schema.

This struct has been removed.

Use [`PhysicalExprAdapterFactory`] instead to customize scans via
[`FileScanConfigBuilder`], i.e. if you had implemented a custom [`SchemaAdapter`]
and passed that into [`FileScanConfigBuilder`] / [`ParquetSource`].
Use [`BatchAdapter`] if you want to map a stream of [`RecordBatch`]es
between one schema and another, i.e. if you were calling [`SchemaMapper::map_batch`] manually.

See `upgrading.md` for more details.

[`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory
[`FileScanConfigBuilder`]: crate::file_scan_config::FileScanConfigBuilder
[`ParquetSource`]: https://docs.rs/datafusion-datasource-parquet/latest/datafusion_datasource_parquet/source/struct.ParquetSource.html
[`BatchAdapter`]: datafusion_physical_expr_adapter::BatchAdapter

**Trait Implementations:**

- **SchemaMapper**
  - `fn map_batch(self: &Self, _batch: RecordBatch) -> Result<RecordBatch>`
  - `fn map_column_statistics(self: &Self, _file_col_statistics: &[ColumnStatistics]) -> Result<Vec<ColumnStatistics>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



