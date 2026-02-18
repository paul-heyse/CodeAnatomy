**datafusion_catalog_listing > config**

# Module: config

## Contents

**Structs**

- [`ListingTableConfig`](#listingtableconfig) - Configuration for creating a [`crate::ListingTable`]

**Enums**

- [`SchemaSource`](#schemasource) - Indicates the source of the schema for a [`crate::ListingTable`]

---

## datafusion_catalog_listing::config::ListingTableConfig

*Struct*

Configuration for creating a [`crate::ListingTable`]

# Schema Evolution Support

This configuration supports schema evolution through the optional
[`PhysicalExprAdapterFactory`]. You might want to override the default factory when you need:

- **Type coercion requirements**: When you need custom logic for converting between
  different Arrow data types (e.g., Int32 ↔ Int64, Utf8 ↔ LargeUtf8)
- **Column mapping**: You need to map columns with a legacy name to a new name
- **Custom handling of missing columns**: By default they are filled in with nulls, but you may e.g. want to fill them in with `0` or `""`.

**Fields:**
- `table_paths: Vec<datafusion_datasource::ListingTableUrl>` - Paths on the `ObjectStore` for creating [`crate::ListingTable`].
- `file_schema: Option<arrow::datatypes::SchemaRef>` - Optional `SchemaRef` for the to be created [`crate::ListingTable`].
- `options: Option<crate::options::ListingOptions>` - Optional [`ListingOptions`] for the to be created [`crate::ListingTable`].

**Methods:**

- `fn new(table_path: ListingTableUrl) -> Self` - Creates new [`ListingTableConfig`] for reading the specified URL
- `fn new_with_multi_paths(table_paths: Vec<ListingTableUrl>) -> Self` - Creates new [`ListingTableConfig`] with multiple table paths.
- `fn schema_source(self: &Self) -> SchemaSource` - Returns the source of the schema for this configuration
- `fn with_schema(self: Self, schema: SchemaRef) -> Self` - Set the `schema` for the overall [`crate::ListingTable`]
- `fn with_listing_options(self: Self, listing_options: ListingOptions) -> Self` - Add `listing_options` to [`ListingTableConfig`]
- `fn infer_file_extension_and_compression_type(path: &str) -> datafusion_common::Result<(String, Option<String>)>` - Returns a tuple of `(file_extension, optional compression_extension)`
- `fn infer_schema(self: Self, state: &dyn Session) -> datafusion_common::Result<Self>` - Infer the [`SchemaRef`] based on `table_path`s.
- `fn infer_partitions_from_path(self: Self, state: &dyn Session) -> datafusion_common::Result<Self>` - Infer the partition columns from `table_paths`.
- `fn with_expr_adapter_factory(self: Self, expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>) -> Self` - Set the [`PhysicalExprAdapterFactory`] for the [`crate::ListingTable`]
- `fn with_schema_adapter_factory(self: Self, _schema_adapter_factory: Arc<dyn SchemaAdapterFactory>) -> Self` - Deprecated: Set the [`SchemaAdapterFactory`] for the [`crate::ListingTable`]

**Trait Implementations:**

- **Default**
  - `fn default() -> ListingTableConfig`
- **Clone**
  - `fn clone(self: &Self) -> ListingTableConfig`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_catalog_listing::config::SchemaSource

*Enum*

Indicates the source of the schema for a [`crate::ListingTable`]

**Variants:**
- `Unset` - Schema is not yet set (initial state)
- `Inferred` - Schema was inferred from first table_path
- `Specified` - Schema was specified explicitly via with_schema

**Traits:** Copy

**Trait Implementations:**

- **Default**
  - `fn default() -> SchemaSource`
- **PartialEq**
  - `fn eq(self: &Self, other: &SchemaSource) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> SchemaSource`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



