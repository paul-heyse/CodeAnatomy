**datafusion_physical_expr_common > metrics > baseline**

# Module: metrics::baseline

## Contents

**Structs**

- [`BaselineMetrics`](#baselinemetrics) - Helper for creating and tracking common "baseline" metrics for
- [`SpillMetrics`](#spillmetrics) - Helper for creating and tracking spill-related metrics for
- [`SplitMetrics`](#splitmetrics) - Metrics for tracking batch splitting activity

**Traits**

- [`RecordOutput`](#recordoutput) - Trait for things that produce output rows as a result of execution.

---

## datafusion_physical_expr_common::metrics::baseline::BaselineMetrics

*Struct*

Helper for creating and tracking common "baseline" metrics for
each operator

Example:
```
use datafusion_physical_expr_common::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet,
};
let metrics = ExecutionPlanMetricsSet::new();

let partition = 2;
let baseline_metrics = BaselineMetrics::new(&metrics, partition);

// during execution, in CPU intensive operation:
let timer = baseline_metrics.elapsed_compute().timer();
// .. do CPU intensive work
timer.done();

// when operator is finished:
baseline_metrics.done();
```

**Methods:**

- `fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self` - Create a new BaselineMetric structure, and set `start_time` to now
- `fn intermediate(self: &Self) -> BaselineMetrics` - Returns a [`BaselineMetrics`] that updates the same `elapsed_compute` ignoring
- `fn elapsed_compute(self: &Self) -> &Time` - return the metric for cpu time spend in this operator
- `fn output_rows(self: &Self) -> &Count` - return the metric for the total number of output rows produced
- `fn output_batches(self: &Self) -> &Count` - return the metric for the total number of output batches produced
- `fn done(self: &Self)` - Records the fact that this operator's execution is complete
- `fn record_output(self: &Self, num_rows: usize)` - Record that some number of rows have been produced as output
- `fn try_done(self: &Self)` - If not previously recorded `done()`, record
- `fn record_poll(self: &Self, poll: Poll<Option<Result<RecordBatch>>>) -> Poll<Option<Result<RecordBatch>>>` - Process a poll result of a stream producing output for an operator.

**Trait Implementations:**

- **Drop**
  - `fn drop(self: & mut Self)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> BaselineMetrics`



## datafusion_physical_expr_common::metrics::baseline::RecordOutput

*Trait*

Trait for things that produce output rows as a result of execution.

**Methods:**

- `record_output`: Record that some number of output rows have been produced



## datafusion_physical_expr_common::metrics::baseline::SpillMetrics

*Struct*

Helper for creating and tracking spill-related metrics for
each operator

**Fields:**
- `spill_file_count: super::Count` - count of spills during the execution of the operator
- `spilled_bytes: super::Count` - total bytes actually written to disk during the execution of the operator
- `spilled_rows: super::Count` - total spilled rows during the execution of the operator

**Methods:**

- `fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self` - Create a new SpillMetrics structure

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> SpillMetrics`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_physical_expr_common::metrics::baseline::SplitMetrics

*Struct*

Metrics for tracking batch splitting activity

**Fields:**
- `batches_split: super::Count` - Number of times an input [`RecordBatch`] was split

**Methods:**

- `fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self` - Create a new [`SplitMetrics`]

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> SplitMetrics`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



