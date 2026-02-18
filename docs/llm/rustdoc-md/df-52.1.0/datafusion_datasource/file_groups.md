**datafusion_datasource > file_groups**

# Module: file_groups

## Contents

**Structs**

- [`FileGroup`](#filegroup) - Represents a group of partitioned files that'll be processed by a single thread.
- [`FileGroupPartitioner`](#filegrouppartitioner) - Repartition input files into `target_partitions` partitions, if total file size exceed

---

## datafusion_datasource::file_groups::FileGroup

*Struct*

Represents a group of partitioned files that'll be processed by a single thread.
Maintains optional statistics across all files in the group.

# Statistics

The group-level [`FileGroup::file_statistics`] field contains merged statistics from all files
in the group for the **full table schema** (file columns + partition columns).

Partition column statistics are derived from the individual file partition values:
- `min` = minimum partition value across all files in the group
- `max` = maximum partition value across all files in the group
- `null_count` = 0 (partition values are never null)

This allows query optimizers to prune entire file groups based on partition bounds.

**Methods:**

- `fn new(files: Vec<PartitionedFile>) -> Self` - Creates a new FileGroup from a vector of PartitionedFile objects
- `fn len(self: &Self) -> usize` - Returns the number of files in this group
- `fn with_statistics(self: Self, statistics: Arc<Statistics>) -> Self` - Set the statistics for this group
- `fn files(self: &Self) -> &[PartitionedFile]` - Returns a slice of the files in this group
- `fn iter(self: &Self) -> impl Trait`
- `fn into_inner(self: Self) -> Vec<PartitionedFile>`
- `fn is_empty(self: &Self) -> bool`
- `fn pop(self: & mut Self) -> Option<PartitionedFile>` - Removes the last element from the files vector and returns it, or None if empty
- `fn push(self: & mut Self, partitioned_file: PartitionedFile)` - Adds a file to the group
- `fn file_statistics(self: &Self, index: Option<usize>) -> Option<&Statistics>` - Get the specific file statistics for the given index
- `fn statistics_mut(self: & mut Self) -> Option<& mut Statistics>` - Get the mutable reference to the statistics for this group
- `fn split_files(self: Self, n: usize) -> Vec<FileGroup>` - Partition the list of files into `n` groups
- `fn group_by_partition_values(self: Self, max_target_partitions: usize) -> Vec<FileGroup>` - Groups files by their partition values, ensuring all files with same

**Trait Implementations:**

- **IndexMut**
  - `fn index_mut(self: & mut Self, index: usize) -> & mut <Self as >::Output`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(files: Vec<PartitionedFile>) -> Self`
- **Index**
  - `fn index(self: &Self, index: usize) -> &<Self as >::Output`
- **Clone**
  - `fn clone(self: &Self) -> FileGroup`
- **Default**
  - `fn default() -> Self`
- **FromIterator**
  - `fn from_iter<I>(iter: I) -> Self`



## datafusion_datasource::file_groups::FileGroupPartitioner

*Struct*

Repartition input files into `target_partitions` partitions, if total file size exceed
`repartition_file_min_size`

This partitions evenly by file byte range, and does not have any knowledge
of how data is laid out in specific files. The specific `FileOpener` are
responsible for the actual partitioning on specific data source type. (e.g.
the `CsvOpener` will read lines overlap with byte range as well as
handle boundaries to ensure all lines will be read exactly once)

# Example

For example, if there are two files `A` and `B` that we wish to read with 4
partitions (with 4 threads) they will be divided as follows:

```text
                                   ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
                                     ┌─────────────────┐
                                   │ │                 │ │
                                     │     File A      │
                                   │ │  Range: 0-2MB   │ │
                                     │                 │
                                   │ └─────────────────┘ │
                                    ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
┌─────────────────┐                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
│                 │                  ┌─────────────────┐
│                 │                │ │                 │ │
│                 │                  │     File A      │
│                 │                │ │   Range 2-4MB   │ │
│                 │                  │                 │
│                 │                │ └─────────────────┘ │
│  File A (7MB)   │   ────────▶     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
│                 │                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
│                 │                  ┌─────────────────┐
│                 │                │ │                 │ │
│                 │                  │     File A      │
│                 │                │ │  Range: 4-6MB   │ │
│                 │                  │                 │
│                 │                │ └─────────────────┘ │
└─────────────────┘                 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
┌─────────────────┐                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
│  File B (1MB)   │                  ┌─────────────────┐
│                 │                │ │     File A      │ │
└─────────────────┘                  │  Range: 6-7MB   │
                                   │ └─────────────────┘ │
                                     ┌─────────────────┐
                                   │ │  File B (1MB)   │ │
                                     │                 │
                                   │ └─────────────────┘ │
                                    ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─

                                   If target_partitions = 4,
                                     divides into 4 groups
```

# Maintaining Order

Within each group files are read sequentially. Thus, if the overall order of
tuples must be preserved, multiple files can not be mixed in the same group.

In this case, the code will split the largest files evenly into any
available empty groups, but the overall distribution may not be as even
as if the order did not need to be preserved.

```text
                                  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
                                     ┌─────────────────┐
                                   │ │                 │ │
                                     │     File A      │
                                   │ │  Range: 0-2MB   │ │
                                     │                 │
┌─────────────────┐                │ └─────────────────┘ │
│                 │                 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
│                 │                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
│                 │                  ┌─────────────────┐
│                 │                │ │                 │ │
│                 │                  │     File A      │
│                 │                │ │   Range 2-4MB   │ │
│  File A (6MB)   │   ────────▶      │                 │
│    (ordered)    │                │ └─────────────────┘ │
│                 │                 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
│                 │                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
│                 │                  ┌─────────────────┐
│                 │                │ │                 │ │
│                 │                  │     File A      │
│                 │                │ │  Range: 4-6MB   │ │
└─────────────────┘                  │                 │
┌─────────────────┐                │ └─────────────────┘ │
│  File B (1MB)   │                 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
│    (ordered)    │                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
└─────────────────┘                  ┌─────────────────┐
                                   │ │  File B (1MB)   │ │
                                     │                 │
                                   │ └─────────────────┘ │
                                    ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─

                                   If target_partitions = 4,
                                     divides into 4 groups
```

**Methods:**

- `fn new() -> Self` - Creates a new [`FileGroupPartitioner`] with default values:
- `fn with_target_partitions(self: Self, target_partitions: usize) -> Self` - Set the target partitions
- `fn with_repartition_file_min_size(self: Self, repartition_file_min_size: usize) -> Self` - Set the minimum size at which to repartition a file
- `fn with_preserve_order_within_groups(self: Self, preserve_order_within_groups: bool) -> Self` - Set whether the order of tuples within a file must be preserved
- `fn repartition_file_groups(self: &Self, file_groups: &[FileGroup]) -> Option<Vec<FileGroup>>` - Repartition input files according to the settings on this [`FileGroupPartitioner`].

**Traits:** Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> FileGroupPartitioner`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



