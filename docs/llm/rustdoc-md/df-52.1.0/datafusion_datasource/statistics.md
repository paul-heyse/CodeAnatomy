**datafusion_datasource > statistics**

# Module: statistics

## Contents

**Functions**

- [`add_row_stats`](#add_row_stats)
- [`compute_all_files_statistics`](#compute_all_files_statistics) - Computes statistics for all files across multiple file groups.

---

## datafusion_datasource::statistics::add_row_stats

*Function*

```rust
fn add_row_stats(file_num_rows: datafusion_common::stats::Precision<usize>, num_rows: datafusion_common::stats::Precision<usize>) -> datafusion_common::stats::Precision<usize>
```



## datafusion_datasource::statistics::compute_all_files_statistics

*Function*

Computes statistics for all files across multiple file groups.

This function:
1. Computes statistics for each individual file group
2. Summary statistics across all file groups
3. Optionally marks statistics as inexact

# Parameters
* `file_groups` - Vector of file groups to process
* `table_schema` - Schema of the table
* `collect_stats` - Whether to collect statistics
* `inexact_stats` - Whether to mark the resulting statistics as inexact

# Returns
A tuple containing:
* The processed file groups with their individual statistics attached
* The summary statistics across all file groups, aka all files summary statistics

```rust
fn compute_all_files_statistics(file_groups: Vec<crate::file_groups::FileGroup>, table_schema: arrow::datatypes::SchemaRef, collect_stats: bool, inexact_stats: bool) -> datafusion_common::Result<(Vec<crate::file_groups::FileGroup>, datafusion_physical_plan::Statistics)>
```



