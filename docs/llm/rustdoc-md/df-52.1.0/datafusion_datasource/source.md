**datafusion_datasource > source**

# Module: source

## Contents

**Structs**

- [`DataSourceExec`](#datasourceexec) - [`ExecutionPlan`] that reads one or more files

**Traits**

- [`DataSource`](#datasource) - A source of data, typically a list of files or memory

---

## datafusion_datasource::source::DataSource

*Trait*

A source of data, typically a list of files or memory

This trait provides common behaviors for abstract sources of data. It has
two common implementations:

1. [`FileScanConfig`]: lists of files
2. [`MemorySourceConfig`]: in memory list of `RecordBatch`

File format specific behaviors are defined by [`FileSource`]

# See Also
* [`FileSource`] for file format specific implementations (Parquet, Json, etc)
* [`DataSourceExec`]: The [`ExecutionPlan`] that reads from a `DataSource`

# Notes

Requires `Debug` to assist debugging

[`FileScanConfig`]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.FileScanConfig.html
[`MemorySourceConfig`]: https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.MemorySourceConfig.html
[`FileSource`]: crate::file::FileSource
[`FileFormat``]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/index.html
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html

The following diagram shows how DataSource, FileSource, and DataSourceExec are related
```text
                      ┌─────────────────────┐                              -----► execute path
                      │                     │                              ┄┄┄┄┄► init path
                      │   DataSourceExec    │  
                      │                     │    
                      └───────▲─────────────┘
                              ┊  │
                              ┊  │
                      ┌──────────▼──────────┐                            ┌──────────-──────────┐
                      │                     │                            |                     |
                      │  DataSource(trait)  │                            | TableProvider(trait)|
                      │                     │                            |                     |
                      └───────▲─────────────┘                            └─────────────────────┘
                              ┊  │                                                  ┊
              ┌───────────────┿──┴────────────────┐                                 ┊
              |   ┌┄┄┄┄┄┄┄┄┄┄┄┘                   |                                 ┊
              |   ┊                               |                                 ┊
   ┌──────────▼──────────┐             ┌──────────▼──────────┐                      ┊
   │                     │             │                     │           ┌──────────▼──────────┐
   │   FileScanConfig    │             │ MemorySourceConfig  │           |                     |
   │                     │             │                     │           |  FileFormat(trait)  |
   └──────────────▲──────┘             └─────────────────────┘           |                     |
              │   ┊                                                      └─────────────────────┘
              │   ┊                                                                 ┊
              │   ┊                                                                 ┊
   ┌──────────▼──────────┐                                               ┌──────────▼──────────┐
   │                     │                                               │     ArrowSource     │
   │ FileSource(trait)   ◄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄│          ...        │
   │                     │                                               │    ParquetSource    │
   └─────────────────────┘                                               └─────────────────────┘
              │
              │
              │
              │
   ┌──────────▼──────────┐
   │     ArrowSource     │
   │          ...        │
   │    ParquetSource    │
   └─────────────────────┘
              |
FileOpener (called by FileStream)
              │
   ┌──────────▼──────────┐
   │                     │
   │     RecordBatch     │
   │                     │
   └─────────────────────┘
```

**Methods:**

- `open`
- `as_any`
- `fmt_as`: Format this source for display in explain plans
- `repartitioned`: Return a copy of this DataSource with a new partitioning scheme.
- `output_partitioning`
- `eq_properties`
- `scheduling_type`
- `partition_statistics`: Returns statistics for a specific partition, or aggregate statistics
- `statistics`: Returns aggregate statistics across all partitions.
- `with_fetch`: Return a copy of this DataSource with a new fetch limit
- `fetch`
- `metrics`
- `try_swapping_with_projection`
- `try_pushdown_filters`: Try to push down filters into this DataSource.
- `try_pushdown_sort`: Try to create a new DataSource that produces data in the specified sort order.



## datafusion_datasource::source::DataSourceExec

*Struct*

[`ExecutionPlan`] that reads one or more files

`DataSourceExec` implements common functionality such as applying
projections, and caching plan properties.

The [`DataSource`] describes where to find the data for this data source
(for example in files or what in memory partitions).

For file based [`DataSource`]s, format specific behavior is implemented in
the [`FileSource`] trait.

[`FileSource`]: crate::file::FileSource

**Methods:**

- `fn from_data_source<impl DataSource + 'static>(data_source: impl Trait) -> Arc<Self>`
- `fn new(data_source: Arc<dyn DataSource>) -> Self`
- `fn data_source(self: &Self) -> &Arc<dyn DataSource>` - Return the source object
- `fn with_data_source(self: Self, data_source: Arc<dyn DataSource>) -> Self`
- `fn with_constraints(self: Self, constraints: Constraints) -> Self` - Assign constraints
- `fn with_partitioning(self: Self, partitioning: Partitioning) -> Self` - Assign output partitioning
- `fn downcast_to_file_source<T>(self: &Self) -> Option<(&FileScanConfig, &T)>` - Downcast the `DataSourceExec`'s `data_source` to a specific file source

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(source: S) -> Self`
- **DisplayAs**
  - `fn fmt_as(self: &Self, t: DisplayFormatType, f: & mut Formatter) -> fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> DataSourceExec`
- **ExecutionPlan**
  - `fn name(self: &Self) -> &'static str`
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn properties(self: &Self) -> &PlanProperties`
  - `fn children(self: &Self) -> Vec<&Arc<dyn ExecutionPlan>>`
  - `fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>>`
  - `fn repartitioned(self: &Self, target_partitions: usize, config: &ConfigOptions) -> Result<Option<Arc<dyn ExecutionPlan>>>` - Implementation of [`ExecutionPlan::repartitioned`] which relies upon the inner [`DataSource::repartitioned`].
  - `fn execute(self: &Self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream>`
  - `fn metrics(self: &Self) -> Option<MetricsSet>`
  - `fn partition_statistics(self: &Self, partition: Option<usize>) -> Result<Statistics>`
  - `fn with_fetch(self: &Self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>>`
  - `fn fetch(self: &Self) -> Option<usize>`
  - `fn try_swapping_with_projection(self: &Self, projection: &ProjectionExec) -> Result<Option<Arc<dyn ExecutionPlan>>>`
  - `fn handle_child_pushdown_result(self: &Self, _phase: FilterPushdownPhase, child_pushdown_result: ChildPushdownResult, config: &ConfigOptions) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>>`
  - `fn try_pushdown_sort(self: &Self, order: &[PhysicalSortExpr]) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>>`



