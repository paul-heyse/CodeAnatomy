**datafusion_datasource > sink**

# Module: sink

## Contents

**Structs**

- [`DataSinkExec`](#datasinkexec) - Execution plan for writing record batches to a [`DataSink`]

**Traits**

- [`DataSink`](#datasink) - `DataSink` implements writing streams of [`RecordBatch`]es to

---

## datafusion_datasource::sink::DataSink

*Trait*

`DataSink` implements writing streams of [`RecordBatch`]es to
user defined destinations.

The `Display` impl is used to format the sink for explain plan
output.

**Methods:**

- `as_any`: Returns the data sink as [`Any`] so that it can be
- `metrics`: Return a snapshot of the [MetricsSet] for this
- `schema`: Returns the sink schema
- `write_all`: Writes the data to the sink, returns the number of values written



## datafusion_datasource::sink::DataSinkExec

*Struct*

Execution plan for writing record batches to a [`DataSink`]

Returns a single row with the number of values written

**Methods:**

- `fn new(input: Arc<dyn ExecutionPlan>, sink: Arc<dyn DataSink>, sort_order: Option<LexRequirement>) -> Self` - Create a plan to write to `sink`
- `fn input(self: &Self) -> &Arc<dyn ExecutionPlan>` - Input execution plan
- `fn sink(self: &Self) -> &dyn DataSink` - Returns insert sink
- `fn sort_order(self: &Self) -> &Option<LexRequirement>` - Optional sort order for output data

**Trait Implementations:**

- **DisplayAs**
  - `fn fmt_as(self: &Self, t: DisplayFormatType, f: & mut fmt::Formatter) -> fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> DataSinkExec`
- **ExecutionPlan**
  - `fn name(self: &Self) -> &'static str`
  - `fn as_any(self: &Self) -> &dyn Any` - Return a reference to Any that can be used for downcasting
  - `fn properties(self: &Self) -> &PlanProperties`
  - `fn benefits_from_input_partitioning(self: &Self) -> Vec<bool>`
  - `fn required_input_distribution(self: &Self) -> Vec<Distribution>`
  - `fn required_input_ordering(self: &Self) -> Vec<Option<OrderingRequirements>>`
  - `fn maintains_input_order(self: &Self) -> Vec<bool>`
  - `fn children(self: &Self) -> Vec<&Arc<dyn ExecutionPlan>>`
  - `fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>>`
  - `fn execute(self: &Self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream>` - Execute the plan and return a stream of `RecordBatch`es for
  - `fn metrics(self: &Self) -> Option<MetricsSet>` - Returns the metrics of the underlying [DataSink]
- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`



