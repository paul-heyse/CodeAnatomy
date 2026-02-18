**datafusion_physical_expr_common > metrics**

# Module: metrics

## Contents

**Structs**

- [`ExecutionPlanMetricsSet`](#executionplanmetricsset) - A set of [`Metric`]s for an individual operator.
- [`Label`](#label) - `name=value` pairs identifying a metric. This concept is called various things
- [`Metric`](#metric) - Something that tracks a value of interest (metric) during execution.
- [`MetricsSet`](#metricsset) - A snapshot of the metrics for a particular execution plan.

**Enums**

- [`MetricType`](#metrictype) - Categorizes metrics so the display layer can choose the desired verbosity.

---

## datafusion_physical_expr_common::metrics::ExecutionPlanMetricsSet

*Struct*

A set of [`Metric`]s for an individual operator.

This structure is intended as a convenience for execution plan
implementations so they can generate different streams for multiple
partitions but easily report them together.

Each `clone()` of this structure will add metrics to the same
underlying metrics set

**Methods:**

- `fn new() -> Self` - Create a new empty shared metrics set
- `fn register(self: &Self, metric: Arc<Metric>)` - Add the specified metric to the underlying metric set
- `fn clone_inner(self: &Self) -> MetricsSet` - Return a clone of the inner [`MetricsSet`]

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> ExecutionPlanMetricsSet`
- **Default**
  - `fn default() -> ExecutionPlanMetricsSet`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_physical_expr_common::metrics::Label

*Struct*

`name=value` pairs identifying a metric. This concept is called various things
in various different systems:

"labels" in
[prometheus](https://prometheus.io/docs/concepts/data_model/) and
"tags" in
[InfluxDB](https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/)
, "attributes" in [open
telemetry]<https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md>,
etc.

As the name and value are expected to mostly be constant strings,
use a [`Cow`] to avoid copying / allocations in this common case.

**Methods:**

- `fn new<impl Into<Cow<'static, str>>, impl Into<Cow<'static, str>>>(name: impl Trait, value: impl Trait) -> Self` - Create a new [`Label`]
- `fn name(self: &Self) -> &str` - Returns the name of this label
- `fn value(self: &Self) -> &str` - Returns the value of this label

**Traits:** Eq

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Label`
- **PartialEq**
  - `fn eq(self: &Self, other: &Label) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_physical_expr_common::metrics::Metric

*Struct*

Something that tracks a value of interest (metric) during execution.

Typically [`Metric`]s are not created directly, but instead
are created using [`MetricBuilder`] or methods on
[`ExecutionPlanMetricsSet`].

```
use datafusion_physical_expr_common::metrics::*;

let metrics = ExecutionPlanMetricsSet::new();
assert!(metrics.clone_inner().output_rows().is_none());

// Create a counter to increment using the MetricBuilder
let partition = 1;
let output_rows = MetricBuilder::new(&metrics).output_rows(partition);

// Counter can be incremented
output_rows.add(13);

// The value can be retrieved directly:
assert_eq!(output_rows.value(), 13);

// As well as from the metrics set
assert_eq!(metrics.clone_inner().output_rows(), Some(13));
```

**Methods:**

- `fn new(value: MetricValue, partition: Option<usize>) -> Self` - Create a new [`Metric`]. Consider using [`MetricBuilder`]
- `fn new_with_labels(value: MetricValue, partition: Option<usize>, labels: Vec<Label>) -> Self` - Create a new [`Metric`]. Consider using [`MetricBuilder`]
- `fn with_type(self: Self, metric_type: MetricType) -> Self` - Set the type for this metric. Defaults to [`MetricType::DEV`]
- `fn with_label(self: Self, label: Label) -> Self` - Add a new label to this metric
- `fn labels(self: &Self) -> &[Label]` - What labels are present for this metric?
- `fn value(self: &Self) -> &MetricValue` - Return a reference to the value of this metric
- `fn value_mut(self: & mut Self) -> & mut MetricValue` - Return a mutable reference to the value of this metric
- `fn partition(self: &Self) -> Option<usize>` - Return a reference to the partition
- `fn metric_type(self: &Self) -> MetricType` - Return the metric type (verbosity level) associated with this metric

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`



## datafusion_physical_expr_common::metrics::MetricType

*Enum*

Categorizes metrics so the display layer can choose the desired verbosity.

# How is it used:
The `datafusion.explain.analyze_level` configuration controls which category is shown.
- When set to `dev`, all metrics with type `MetricType::Summary` or `MetricType::DEV`
  will be shown.
- When set to `summary`, only metrics with type `MetricType::Summary` are shown.

# Difference from `EXPLAIN ANALYZE VERBOSE`:  
The `VERBOSE` keyword controls whether per-partition metrics are shown (when specified),  
or aggregated metrics are displayed (when omitted).  
In contrast, the `analyze_level` configuration determines which categories or
levels of metrics are displayed.

**Variants:**
- `SUMMARY` - Common metrics for high-level insights (answering which operator is slow)
- `DEV` - For deep operator-level introspection for developers

**Traits:** Eq, Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &MetricType) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> MetricType`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_physical_expr_common::metrics::MetricsSet

*Struct*

A snapshot of the metrics for a particular execution plan.

**Methods:**

- `fn new() -> Self` - Create a new container of metrics
- `fn push(self: & mut Self, metric: Arc<Metric>)` - Add the specified metric
- `fn iter(self: &Self) -> impl Trait` - Returns an iterator across all metrics
- `fn output_rows(self: &Self) -> Option<usize>` - Convenience: return the number of rows produced, aggregated
- `fn spill_count(self: &Self) -> Option<usize>` - Convenience: return the count of spills, aggregated
- `fn spilled_bytes(self: &Self) -> Option<usize>` - Convenience: return the total byte size of spills, aggregated
- `fn spilled_rows(self: &Self) -> Option<usize>` - Convenience: return the total rows of spills, aggregated
- `fn elapsed_compute(self: &Self) -> Option<usize>` - Convenience: return the amount of elapsed CPU time spent,
- `fn sum<F>(self: &Self, f: F) -> Option<MetricValue>` - Sums the values for metrics for which `f(metric)` returns
- `fn sum_by_name(self: &Self, metric_name: &str) -> Option<MetricValue>` - Returns the sum of all the metrics with the specified name
- `fn aggregate_by_name(self: &Self) -> Self` - Returns a new derived `MetricsSet` where all metrics
- `fn sorted_for_display(self: Self) -> Self` - Sort the order of metrics so the "most useful" show up first
- `fn timestamps_removed(self: Self) -> Self` - Remove all timestamp metrics (for more compact display)
- `fn filter_by_metric_types(self: Self, allowed: &[MetricType]) -> Self` - Returns a new derived `MetricsSet` containing only metrics whose

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> MetricsSet`
- **Default**
  - `fn default() -> MetricsSet`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result` - Format the [`MetricsSet`] as a single string
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



