**datafusion_catalog_listing > options**

# Module: options

## Contents

**Structs**

- [`ListingOptions`](#listingoptions) - Options for creating a [`crate::ListingTable`]

---

## datafusion_catalog_listing::options::ListingOptions

*Struct*

Options for creating a [`crate::ListingTable`]

**Fields:**
- `file_extension: String` - A suffix on which files should be filtered (leave empty to
- `format: std::sync::Arc<dyn FileFormat>` - The file format
- `table_partition_cols: Vec<(String, arrow::datatypes::DataType)>` - The expected partition column names in the folder structure.
- `collect_stat: bool` - Set true to try to guess statistics from the files.
- `target_partitions: usize` - Group files to avoid that the number of partitions exceeds
- `file_sort_order: Vec<Vec<datafusion_expr::SortExpr>>` - Optional pre-known sort order(s). Must be `SortExpr`s.

**Methods:**

- `fn new(format: Arc<dyn FileFormat>) -> Self` - Creates an options instance with the given format
- `fn with_session_config_options(self: Self, config: &SessionConfig) -> Self` - Set options from [`SessionConfig`] and returns self.
- `fn with_file_extension<impl Into<String>>(self: Self, file_extension: impl Trait) -> Self` - Set file extension on [`ListingOptions`] and returns self.
- `fn with_file_extension_opt<S>(self: Self, file_extension: Option<S>) -> Self` - Optionally set file extension on [`ListingOptions`] and returns self.
- `fn with_table_partition_cols(self: Self, table_partition_cols: Vec<(String, DataType)>) -> Self` - Set `table partition columns` on [`ListingOptions`] and returns self.
- `fn with_collect_stat(self: Self, collect_stat: bool) -> Self` - Set stat collection on [`ListingOptions`] and returns self.
- `fn with_target_partitions(self: Self, target_partitions: usize) -> Self` - Set number of target partitions on [`ListingOptions`] and returns self.
- `fn with_file_sort_order(self: Self, file_sort_order: Vec<Vec<SortExpr>>) -> Self` - Set file sort order on [`ListingOptions`] and returns self.
- `fn infer_schema<'a>(self: &'a Self, state: &dyn Session, table_path: &'a ListingTableUrl) -> datafusion_common::Result<SchemaRef>` - Infer the schema of the files at the given path on the provided object store.
- `fn validate_partitions(self: &Self, state: &dyn Session, table_path: &ListingTableUrl) -> datafusion_common::Result<()>` - Infers the partition columns stored in `LOCATION` and compares
- `fn infer_partitions(self: &Self, state: &dyn Session, table_path: &ListingTableUrl) -> datafusion_common::Result<Vec<String>>` - Infer the partitioning at the given path on the provided object store.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ListingOptions`



