**datafusion_datasource_parquet > metrics**

# Module: metrics

## Contents

**Structs**

- [`ParquetFileMetrics`](#parquetfilemetrics) - Stores metrics about the parquet execution for a particular parquet file.

---

## datafusion_datasource_parquet::metrics::ParquetFileMetrics

*Struct*

Stores metrics about the parquet execution for a particular parquet file.

This component is a subject to **change** in near future and is exposed for low level integrations
through [`ParquetFileReaderFactory`].

[`ParquetFileReaderFactory`]: super::ParquetFileReaderFactory

**Fields:**
- `files_ranges_pruned_statistics: datafusion_physical_plan::metrics::PruningMetrics` - Number of file **ranges** pruned or matched by partition or file level statistics.
- `predicate_evaluation_errors: datafusion_physical_plan::metrics::Count` - Number of times the predicate could not be evaluated
- `row_groups_pruned_bloom_filter: datafusion_physical_plan::metrics::PruningMetrics` - Number of row groups whose bloom filters were checked, tracked with matched/pruned counts
- `row_groups_pruned_statistics: datafusion_physical_plan::metrics::PruningMetrics` - Number of row groups whose statistics were checked, tracked with matched/pruned counts
- `bytes_scanned: datafusion_physical_plan::metrics::Count` - Total number of bytes scanned
- `pushdown_rows_pruned: datafusion_physical_plan::metrics::Count` - Total rows filtered out by predicates pushed into parquet scan
- `pushdown_rows_matched: datafusion_physical_plan::metrics::Count` - Total rows passed predicates pushed into parquet scan
- `row_pushdown_eval_time: datafusion_physical_plan::metrics::Time` - Total time spent evaluating row-level pushdown filters
- `statistics_eval_time: datafusion_physical_plan::metrics::Time` - Total time spent evaluating row group-level statistics filters
- `bloom_filter_eval_time: datafusion_physical_plan::metrics::Time` - Total time spent evaluating row group Bloom Filters
- `page_index_rows_pruned: datafusion_physical_plan::metrics::PruningMetrics` - Total rows filtered or matched by parquet page index
- `page_index_eval_time: datafusion_physical_plan::metrics::Time` - Total time spent evaluating parquet page index filters
- `metadata_load_time: datafusion_physical_plan::metrics::Time` - Total time spent reading and parsing metadata from the footer
- `scan_efficiency_ratio: datafusion_physical_plan::metrics::RatioMetrics` - Scan Efficiency Ratio, calculated as bytes_scanned / total_file_size
- `predicate_cache_inner_records: datafusion_physical_plan::metrics::Count` - Predicate Cache: number of records read directly from the inner reader.
- `predicate_cache_records: datafusion_physical_plan::metrics::Count` - Predicate Cache: number of records read from the cache. This is the

**Methods:**

- `fn new(partition: usize, filename: &str, metrics: &ExecutionPlanMetricsSet) -> Self` - Create new metrics

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ParquetFileMetrics`



