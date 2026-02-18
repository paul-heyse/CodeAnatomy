**datafusion > test_util > parquet**

# Module: test_util::parquet

## Contents

**Structs**

- [`ParquetScanOptions`](#parquetscanoptions) - Options for how to create the parquet scan
- [`TestParquetFile`](#testparquetfile) - a ParquetFile that has been created for testing.

---

## datafusion::test_util::parquet::ParquetScanOptions

*Struct*

Options for how to create the parquet scan

**Fields:**
- `pushdown_filters: bool` - Enable pushdown filters
- `reorder_filters: bool` - enable reordering filters
- `enable_page_index: bool` - enable page index

**Methods:**

- `fn config(self: &Self) -> SessionConfig` - Returns a [`SessionConfig`] with the given options

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ParquetScanOptions`



## datafusion::test_util::parquet::TestParquetFile

*Struct*

a ParquetFile that has been created for testing.

**Methods:**

- `fn try_new<impl IntoIterator<Item = RecordBatch>>(path: PathBuf, props: WriterProperties, batches: impl Trait) -> Result<Self>` - Creates a new parquet file at the specified location with the
- `fn create_scan(self: &Self, ctx: &SessionContext, maybe_filter: Option<Expr>) -> Result<Arc<dyn ExecutionPlan>>` - Return a `DataSourceExec` with the specified options.
- `fn parquet_metrics(plan: &Arc<dyn ExecutionPlan>) -> Option<MetricsSet>` - Retrieve metrics from the parquet exec returned from `create_scan`
- `fn schema(self: &Self) -> SchemaRef` - The schema of this parquet file
- `fn path(self: &Self) -> &std::path::Path` - The path to the parquet file



