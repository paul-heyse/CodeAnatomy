**datafusion_datasource_parquet > row_group_filter**

# Module: row_group_filter

## Contents

**Structs**

- [`RowGroupAccessPlanFilter`](#rowgroupaccessplanfilter) - Reduces the [`ParquetAccessPlan`] based on row group level metadata.

---

## datafusion_datasource_parquet::row_group_filter::RowGroupAccessPlanFilter

*Struct*

Reduces the [`ParquetAccessPlan`] based on row group level metadata.

This struct implements the various types of pruning that are applied to a
set of row groups within a parquet file, progressively narrowing down the
set of row groups (and ranges/selections within those row groups) that
should be scanned, based on the available metadata.

**Methods:**

- `fn new(access_plan: ParquetAccessPlan) -> Self` - Create a new `RowGroupPlanBuilder` for pruning out the groups to scan
- `fn is_empty(self: &Self) -> bool` - Return true if there are no row groups
- `fn remaining_row_group_count(self: &Self) -> usize` - Return the number of row groups that are currently expected to be scanned
- `fn build(self: Self) -> ParquetAccessPlan` - Returns the inner access plan
- `fn prune_by_range(self: & mut Self, groups: &[RowGroupMetaData], range: &FileRange)` - Prune remaining row groups to only those  within the specified range.
- `fn prune_by_statistics(self: & mut Self, arrow_schema: &Schema, parquet_schema: &SchemaDescriptor, groups: &[RowGroupMetaData], predicate: &PruningPredicate, metrics: &ParquetFileMetrics)` - Prune remaining row groups using min/max/null_count statistics and
- `fn prune_by_bloom_filters<T>(self: & mut Self, arrow_schema: &Schema, builder: & mut ParquetRecordBatchStreamBuilder<T>, predicate: &PruningPredicate, metrics: &ParquetFileMetrics)` - Prune remaining row groups using available bloom filters and the

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> RowGroupAccessPlanFilter`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &RowGroupAccessPlanFilter) -> bool`



