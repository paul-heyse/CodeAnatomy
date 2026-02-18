**deltalake_core > operations > optimize**

# Module: operations::optimize

## Contents

**Structs**

- [`MergePlan`](#mergeplan) - Encapsulates the operations required to optimize a Delta Table
- [`MergeTaskParameters`](#mergetaskparameters) - Parameters passed to individual merge tasks
- [`MetricDetails`](#metricdetails) - Statistics on files for a particular operation
- [`Metrics`](#metrics) - Metrics from Optimize
- [`OptimizeBuilder`](#optimizebuilder) - Optimize a Delta table with given options
- [`PartialMetrics`](#partialmetrics) - Metrics for a single partition

**Enums**

- [`OptimizeType`](#optimizetype) - Type of optimization to perform.

**Functions**

- [`create_merge_plan`](#create_merge_plan) - Build a Plan on which files to merge together. See [OptimizeBuilder]
- [`create_session_state_for_optimize`](#create_session_state_for_optimize) - Create a SessionState configured for optimize operations with custom spill settings.

---

## deltalake_core::operations::optimize::MergePlan

*Struct*

Encapsulates the operations required to optimize a Delta Table

**Methods:**

- `fn execute(self: Self, log_store: LogStoreRef, snapshot: &EagerSnapshot, max_concurrent_tasks: usize, min_commit_interval: Option<Duration>, commit_properties: CommitProperties, operation_id: Uuid, handle: Option<&Arc<dyn CustomExecuteHandler>>) -> Result<Metrics, DeltaTableError>` - Perform the operations outlined in the plan.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::operations::optimize::MergeTaskParameters

*Struct*

Parameters passed to individual merge tasks

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::operations::optimize::MetricDetails

*Struct*

Statistics on files for a particular operation
Operation can be remove or add

**Fields:**
- `avg: f64` - Average file size of a operation
- `max: i64` - Maximum file size of a operation
- `min: i64` - Minimum file size of a operation
- `total_files: usize` - Number of files encountered during operation
- `total_size: i64` - Sum of file sizes of a operation

**Methods:**

- `fn add(self: & mut Self, partial: &MetricDetails)` - Add a partial metric to the metrics

**Trait Implementations:**

- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &MetricDetails) -> bool`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> MetricDetails`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result` - Display the metric details using serde serialization



## deltalake_core::operations::optimize::Metrics

*Struct*

Metrics from Optimize

**Fields:**
- `num_files_added: u64` - Number of optimized files added
- `num_files_removed: u64` - Number of unoptimized files removed
- `files_added: MetricDetails` - Detailed metrics for the add operation
- `files_removed: MetricDetails` - Detailed metrics for the remove operation
- `partitions_optimized: u64` - Number of partitions that had at least one file optimized
- `num_batches: u64` - The number of batches written
- `total_considered_files: usize` - How many files were considered during optimization. Not every file considered is optimized
- `total_files_skipped: usize` - How many files were considered for optimization but were skipped
- `preserve_insertion_order: bool` - The order of records from source files is preserved

**Methods:**

- `fn add(self: & mut Self, partial: &PartialMetrics)` - Add a partial metric to the metrics

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Metrics`
- **Default**
  - `fn default() -> Metrics`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Metrics) -> bool`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`



## deltalake_core::operations::optimize::OptimizeBuilder

*Struct*

Optimize a Delta table with given options

If a target file size is not provided then `delta.targetFileSize` from the
table's configuration is read. Otherwise a default value is used.

**Generic Parameters:**
- 'a

**Methods:**

- `fn with_type(self: Self, optimize_type: OptimizeType) -> Self` - Choose the type of optimization to perform. Defaults to [OptimizeType::Compact].
- `fn with_filters(self: Self, filters: &'a [PartitionFilter]) -> Self` - Only optimize files that return true for the specified partition filter
- `fn with_target_size(self: Self, target: u64) -> Self` - Set the target file size
- `fn with_writer_properties(self: Self, writer_properties: WriterProperties) -> Self` - Writer properties passed to parquet writer
- `fn with_commit_properties(self: Self, commit_properties: CommitProperties) -> Self` - Additional information to write to the commit
- `fn with_preserve_insertion_order(self: Self, preserve_insertion_order: bool) -> Self` - Whether to preserve insertion order within files
- `fn with_max_concurrent_tasks(self: Self, max_concurrent_tasks: usize) -> Self` - Max number of concurrent tasks
- `fn with_min_commit_interval(self: Self, min_commit_interval: Duration) -> Self` - Min commit interval
- `fn with_custom_execute_handler(self: Self, handler: Arc<dyn CustomExecuteHandler>) -> Self` - Set a custom execute handler, for pre and post execution
- `fn with_session_state(self: Self, session: Arc<dyn Session>) -> Self` - Set the DataFusion session used for planning and execution.
- `fn with_session_fallback_policy(self: Self, policy: SessionFallbackPolicy) -> Self` - Control how delta-rs resolves the provided session when it is not a concrete `SessionState`.

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`



## deltalake_core::operations::optimize::OptimizeType

*Enum*

Type of optimization to perform.

**Variants:**
- `Compact` - Compact files into pre-determined bins
- `ZOrder(Vec<String>)` - Z-order files based on provided columns

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::operations::optimize::PartialMetrics

*Struct*

Metrics for a single partition

**Fields:**
- `num_files_added: u64` - Number of optimized files added
- `num_files_removed: u64` - Number of unoptimized files removed
- `files_added: MetricDetails` - Detailed metrics for the add operation
- `files_removed: MetricDetails` - Detailed metrics for the remove operation
- `num_batches: u64` - The number of batches written

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::operations::optimize::create_merge_plan

*Function*

Build a Plan on which files to merge together. See [OptimizeBuilder]

```rust
fn create_merge_plan(log_store: &dyn LogStore, optimize_type: OptimizeType, snapshot: &crate::kernel::EagerSnapshot, filters: &[crate::PartitionFilter], target_size: Option<u64>, writer_properties: parquet::file::properties::WriterProperties, session: datafusion::execution::context::SessionState) -> Result<MergePlan, crate::errors::DeltaTableError>
```



## deltalake_core::operations::optimize::create_session_state_for_optimize

*Function*

Create a SessionState configured for optimize operations with custom spill settings.

This is the recommended way to configure memory and disk limits for optimize operations.
The created SessionState should be passed to [`OptimizeBuilder`] via [`with_session_state`](OptimizeBuilder::with_session_state).

# Arguments
* `max_spill_size` - Maximum bytes in memory before spilling to disk. If `None`, uses DataFusion's default memory pool.
* `max_temp_directory_size` - Maximum disk space for temporary spill files. If `None`, uses DataFusion's default disk manager.

```rust
fn create_session_state_for_optimize(max_spill_size: Option<usize>, max_temp_directory_size: Option<u64>) -> datafusion::execution::context::SessionState
```



