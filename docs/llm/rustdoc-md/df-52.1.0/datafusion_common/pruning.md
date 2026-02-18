**datafusion_common > pruning**

# Module: pruning

## Contents

**Structs**

- [`CompositePruningStatistics`](#compositepruningstatistics) - Combine multiple [`PruningStatistics`] into a single
- [`PartitionPruningStatistics`](#partitionpruningstatistics) - Prune files based on their partition values.
- [`PrunableStatistics`](#prunablestatistics) - Prune a set of containers represented by their statistics.

**Traits**

- [`PruningStatistics`](#pruningstatistics) - A source of runtime statistical information to [`PruningPredicate`]s.

---

## datafusion_common::pruning::CompositePruningStatistics

*Struct*

Combine multiple [`PruningStatistics`] into a single
[`CompositePruningStatistics`].
This can be used to combine statistics from different sources,
for example partition values and file statistics.
This allows pruning with filters that depend on multiple sources of statistics,
such as `WHERE partition_col = data_col`.
This is done by iterating over the statistics and returning the first
one that has information for the requested column.
If multiple statistics have information for the same column,
the first one is returned without any regard for completeness or accuracy.
That is: if the first statistics has information for a column, even if it is incomplete,
that is returned even if a later statistics has more complete information.

**Fields:**
- `statistics: Vec<Box<dyn PruningStatistics>>`

**Methods:**

- `fn new(statistics: Vec<Box<dyn PruningStatistics>>) -> Self` - Create a new instance of [`CompositePruningStatistics`] from

**Trait Implementations:**

- **PruningStatistics**
  - `fn min_values(self: &Self, column: &Column) -> Option<ArrayRef>`
  - `fn max_values(self: &Self, column: &Column) -> Option<ArrayRef>`
  - `fn num_containers(self: &Self) -> usize`
  - `fn null_counts(self: &Self, column: &Column) -> Option<ArrayRef>`
  - `fn row_counts(self: &Self, column: &Column) -> Option<ArrayRef>`
  - `fn contained(self: &Self, column: &Column, values: &HashSet<ScalarValue>) -> Option<BooleanArray>`



## datafusion_common::pruning::PartitionPruningStatistics

*Struct*

Prune files based on their partition values.

This is used both at planning time and execution time to prune
files based on their partition values.
This feeds into [`CompositePruningStatistics`] to allow pruning
with filters that depend both on partition columns and data columns
(e.g. `WHERE partition_col = data_col`).

**Methods:**

- `fn try_new(partition_values: Vec<Vec<ScalarValue>>, partition_fields: Vec<FieldRef>) -> Result<Self, DataFusionError>` - Create a new instance of [`PartitionPruningStatistics`].

**Trait Implementations:**

- **PruningStatistics**
  - `fn min_values(self: &Self, column: &Column) -> Option<ArrayRef>`
  - `fn max_values(self: &Self, column: &Column) -> Option<ArrayRef>`
  - `fn num_containers(self: &Self) -> usize`
  - `fn null_counts(self: &Self, _column: &Column) -> Option<ArrayRef>`
  - `fn row_counts(self: &Self, _column: &Column) -> Option<ArrayRef>`
  - `fn contained(self: &Self, column: &Column, values: &HashSet<ScalarValue>) -> Option<BooleanArray>`
- **Clone**
  - `fn clone(self: &Self) -> PartitionPruningStatistics`



## datafusion_common::pruning::PrunableStatistics

*Struct*

Prune a set of containers represented by their statistics.

Each [`Statistics`] represents a "container" -- some collection of data
that has statistics of its columns.

It is up to the caller to decide what each container represents. For
example, they can come from a file (e.g. [`PartitionedFile`]) or a set of of
files (e.g. [`FileGroup`])

[`PartitionedFile`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.PartitionedFile.html
[`FileGroup`]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.FileGroup.html

**Methods:**

- `fn new(statistics: Vec<Arc<Statistics>>, schema: SchemaRef) -> Self` - Create a new instance of [`PrunableStatistics`].

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> PrunableStatistics`
- **PruningStatistics**
  - `fn min_values(self: &Self, column: &Column) -> Option<ArrayRef>`
  - `fn max_values(self: &Self, column: &Column) -> Option<ArrayRef>`
  - `fn num_containers(self: &Self) -> usize`
  - `fn null_counts(self: &Self, column: &Column) -> Option<ArrayRef>`
  - `fn row_counts(self: &Self, column: &Column) -> Option<ArrayRef>`
  - `fn contained(self: &Self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray>`



## datafusion_common::pruning::PruningStatistics

*Trait*

A source of runtime statistical information to [`PruningPredicate`]s.

# Supported Information

1. Minimum and maximum values for columns

2. Null counts and row counts for columns

3. Whether the values in a column are contained in a set of literals

# Vectorized Interface

Information for containers / files are returned as Arrow [`ArrayRef`], so
the evaluation happens once on a single `RecordBatch`, which amortizes the
overhead of evaluating the predicate. This is important when pruning 1000s
of containers which often happens in analytic systems that have 1000s of
potential files to consider.

For example, for the following three files with a single column `a`:
```text
file1: column a: min=5, max=10
file2: column a: No stats
file2: column a: min=20, max=30
```

PruningStatistics would return:

```text
min_values("a") -> Some([5, Null, 20])
max_values("a") -> Some([10, Null, 30])
min_values("X") -> None
```

[`PruningPredicate`]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/pruning/struct.PruningPredicate.html

**Methods:**

- `min_values`: Return the minimum values for the named column, if known.
- `max_values`: Return the maximum values for the named column, if known.
- `num_containers`: Return the number of containers (e.g. Row Groups) being pruned with
- `null_counts`: Return the number of null values for the named column as an
- `row_counts`: Return the number of rows for the named column in each container
- `contained`: Returns [`BooleanArray`] where each row represents information known



