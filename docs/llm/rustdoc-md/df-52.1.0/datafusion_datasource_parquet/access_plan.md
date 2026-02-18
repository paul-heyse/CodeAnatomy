**datafusion_datasource_parquet > access_plan**

# Module: access_plan

## Contents

**Structs**

- [`ParquetAccessPlan`](#parquetaccessplan) - A selection of rows and row groups within a ParquetFile to decode.

**Enums**

- [`RowGroupAccess`](#rowgroupaccess) - Describes how the parquet reader will access a row group

---

## datafusion_datasource_parquet::access_plan::ParquetAccessPlan

*Struct*

A selection of rows and row groups within a ParquetFile to decode.

A `ParquetAccessPlan` is used to limit the row groups and data pages a `DataSourceExec`
will read and decode to improve performance.

Note that page level pruning based on ArrowPredicate is applied after all of
these selections

# Example

For example, given a Parquet file with 4 row groups, a `ParquetAccessPlan`
can be used to specify skipping row group 0 and 2, scanning a range of rows
in row group 1, and scanning all rows in row group 3 as follows:

```rust
# use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
# use datafusion_datasource_parquet::ParquetAccessPlan;
// Default to scan all row groups
let mut access_plan = ParquetAccessPlan::new_all(4);
access_plan.skip(0); // skip row group
// Use parquet reader RowSelector to specify scanning rows 100-200 and 350-400
// in a row group that has 1000 rows
let row_selection = RowSelection::from(vec![
   RowSelector::skip(100),
   RowSelector::select(100),
   RowSelector::skip(150),
   RowSelector::select(50),
   RowSelector::skip(600),  // skip last 600 rows
]);
access_plan.scan_selection(1, row_selection);
access_plan.skip(2); // skip row group 2
// row group 3 is scanned by default
```

The resulting plan would look like:

```text
┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐

│                   │  SKIP

└ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 Row Group 0
┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
 ┌────────────────┐    SCAN ONLY ROWS
│└────────────────┘ │  100-200
 ┌────────────────┐    350-400
│└────────────────┘ │
 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
 Row Group 1
┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
                       SKIP
│                   │

└ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 Row Group 2
┌───────────────────┐
│                   │  SCAN ALL ROWS
│                   │
│                   │
└───────────────────┘
 Row Group 3
```

**Methods:**

- `fn new_all(row_group_count: usize) -> Self` - Create a new `ParquetAccessPlan` that scans all row groups
- `fn new_none(row_group_count: usize) -> Self` - Create a new `ParquetAccessPlan` that scans no row groups
- `fn new(row_groups: Vec<RowGroupAccess>) -> Self` - Create a new `ParquetAccessPlan` from the specified [`RowGroupAccess`]es
- `fn set(self: & mut Self, idx: usize, access: RowGroupAccess)` - Set the i-th row group to the specified [`RowGroupAccess`]
- `fn skip(self: & mut Self, idx: usize)` - skips the i-th row group (should not be scanned)
- `fn scan(self: & mut Self, idx: usize)` - scan the i-th row group
- `fn should_scan(self: &Self, idx: usize) -> bool` - Return true if the i-th row group should be scanned
- `fn scan_selection(self: & mut Self, idx: usize, selection: RowSelection)` - Set to scan only the [`RowSelection`] in the specified row group.
- `fn into_overall_row_selection(self: Self, row_group_meta_data: &[RowGroupMetaData]) -> Result<Option<RowSelection>>` - Return an overall `RowSelection`, if needed
- `fn row_group_index_iter(self: &Self) -> impl Trait` - Return an iterator over the row group indexes that should be scanned
- `fn row_group_indexes(self: &Self) -> Vec<usize>` - Return a vec of all row group indexes to scan
- `fn len(self: &Self) -> usize` - Return the total number of row groups (not the total number or groups to
- `fn is_empty(self: &Self) -> bool` - Return true if there are no row groups
- `fn inner(self: &Self) -> &[RowGroupAccess]` - Get a reference to the inner accesses
- `fn into_inner(self: Self) -> Vec<RowGroupAccess>` - Covert into the inner row group accesses

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &ParquetAccessPlan) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> ParquetAccessPlan`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_datasource_parquet::access_plan::RowGroupAccess

*Enum*

Describes how the parquet reader will access a row group

**Variants:**
- `Skip` - Do not read the row group at all
- `Scan` - Read all rows from the row group
- `Selection(parquet::arrow::arrow_reader::RowSelection)` - Scan only the specified rows within the row group

**Methods:**

- `fn should_scan(self: &Self) -> bool` - Return true if this row group should be scanned

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> RowGroupAccess`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &RowGroupAccess) -> bool`



