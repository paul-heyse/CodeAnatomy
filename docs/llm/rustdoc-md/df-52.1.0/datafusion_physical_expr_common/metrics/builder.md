**datafusion_physical_expr_common > metrics > builder**

# Module: metrics::builder

## Contents

**Structs**

- [`MetricBuilder`](#metricbuilder) - Structure for constructing metrics, counters, timers, etc.

---

## datafusion_physical_expr_common::metrics::builder::MetricBuilder

*Struct*

Structure for constructing metrics, counters, timers, etc.

Note the use of `Cow<..>` is to avoid allocations in the common
case of constant strings

```rust
use datafusion_physical_expr_common::metrics::*;

let metrics = ExecutionPlanMetricsSet::new();
let partition = 1;

// Create the standard output_rows metric
let output_rows = MetricBuilder::new(&metrics).output_rows(partition);

// Create a operator specific counter with some labels
let num_bytes = MetricBuilder::new(&metrics)
    .with_new_label("filename", "my_awesome_file.parquet")
    .counter("num_bytes", partition);
```

**Generic Parameters:**
- 'a

**Methods:**

- `fn new(metrics: &'a ExecutionPlanMetricsSet) -> Self` - Create a new `MetricBuilder` that will register the result of `build()` with the `metrics`
- `fn with_label(self: Self, label: Label) -> Self` - Add a label to the metric being constructed
- `fn with_type(self: Self, metric_type: MetricType) -> Self` - Set the metric type to the metric being constructed
- `fn with_new_label<impl Into<Cow<'static, str>>, impl Into<Cow<'static, str>>>(self: Self, name: impl Trait, value: impl Trait) -> Self` - Add a label to the metric being constructed
- `fn with_partition(self: Self, partition: usize) -> Self` - Set the partition of the metric being constructed
- `fn build(self: Self, value: MetricValue)` - Consume self and create a metric of the specified value
- `fn output_rows(self: Self, partition: usize) -> Count` - Consume self and create a new counter for recording output rows
- `fn spill_count(self: Self, partition: usize) -> Count` - Consume self and create a new counter for recording the number of spills
- `fn spilled_bytes(self: Self, partition: usize) -> Count` - Consume self and create a new counter for recording the total spilled bytes
- `fn spilled_rows(self: Self, partition: usize) -> Count` - Consume self and create a new counter for recording the total spilled rows
- `fn output_bytes(self: Self, partition: usize) -> Count` - Consume self and create a new counter for recording total output bytes
- `fn output_batches(self: Self, partition: usize) -> Count` - Consume self and create a new counter for recording total output batches
- `fn mem_used(self: Self, partition: usize) -> Gauge` - Consume self and create a new gauge for reporting current memory usage
- `fn counter<impl Into<Cow<'static, str>>>(self: Self, counter_name: impl Trait, partition: usize) -> Count` - Consumes self and creates a new [`Count`] for recording some
- `fn gauge<impl Into<Cow<'static, str>>>(self: Self, gauge_name: impl Trait, partition: usize) -> Gauge` - Consumes self and creates a new [`Gauge`] for reporting some
- `fn global_counter<impl Into<Cow<'static, str>>>(self: Self, counter_name: impl Trait) -> Count` - Consumes self and creates a new [`Count`] for recording a
- `fn global_gauge<impl Into<Cow<'static, str>>>(self: Self, gauge_name: impl Trait) -> Gauge` - Consumes self and creates a new [`Gauge`] for reporting a
- `fn elapsed_compute(self: Self, partition: usize) -> Time` - Consume self and create a new Timer for recording the elapsed
- `fn subset_time<impl Into<Cow<'static, str>>>(self: Self, subset_name: impl Trait, partition: usize) -> Time` - Consumes self and creates a new Timer for recording some
- `fn start_timestamp(self: Self, partition: usize) -> Timestamp` - Consumes self and creates a new Timestamp for recording the
- `fn end_timestamp(self: Self, partition: usize) -> Timestamp` - Consumes self and creates a new Timestamp for recording the
- `fn pruning_metrics<impl Into<Cow<'static, str>>>(self: Self, name: impl Trait, partition: usize) -> PruningMetrics` - Consumes self and creates a new `PruningMetrics`
- `fn ratio_metrics<impl Into<Cow<'static, str>>>(self: Self, name: impl Trait, partition: usize) -> RatioMetrics` - Consumes self and creates a new [`RatioMetrics`]
- `fn ratio_metrics_with_strategy<impl Into<Cow<'static, str>>>(self: Self, name: impl Trait, partition: usize, merge_strategy: RatioMergeStrategy) -> RatioMetrics` - Consumes self and creates a new [`RatioMetrics`] with a specific merge strategy



