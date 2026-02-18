**datafusion_physical_expr_common > metrics > value**

# Module: metrics::value

## Contents

**Structs**

- [`Count`](#count) - A counter to record things such as number of input or output rows
- [`Gauge`](#gauge) - A gauge is the simplest metrics type. It just returns a value.
- [`PruningMetrics`](#pruningmetrics) - Counters tracking pruning metrics
- [`RatioMetrics`](#ratiometrics) - Counters tracking ratio metrics (e.g. matched vs total)
- [`ScopedTimerGuard`](#scopedtimerguard) - RAAI structure that adds all time between its construction and
- [`Time`](#time) - Measure a potentially non contiguous duration of time
- [`Timestamp`](#timestamp) - Stores a single timestamp, stored as the number of nanoseconds

**Enums**

- [`MetricValue`](#metricvalue) - Possible values for a [super::Metric].
- [`RatioMergeStrategy`](#ratiomergestrategy)

---

## datafusion_physical_expr_common::metrics::value::Count

*Struct*

A counter to record things such as number of input or output rows

Note `clone`ing counters update the same underlying metrics

**Methods:**

- `fn new() -> Self` - create a new counter
- `fn add(self: &Self, n: usize)` - Add `n` to the metric's value
- `fn value(self: &Self) -> usize` - Get the current value

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Count`
- **Default**
  - `fn default() -> Self`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_physical_expr_common::metrics::value::Gauge

*Struct*

A gauge is the simplest metrics type. It just returns a value.
For example, you can easily expose current memory consumption with a gauge.

Note `clone`ing gauge update the same underlying metrics

**Methods:**

- `fn new() -> Self` - create a new gauge
- `fn add(self: &Self, n: usize)` - Add `n` to the metric's value
- `fn sub(self: &Self, n: usize)` - Sub `n` from the metric's value
- `fn set_max(self: &Self, n: usize)` - Set metric's value to maximum of `n` and current value
- `fn set(self: &Self, n: usize) -> usize` - Set the metric's value to `n` and return the previous value
- `fn value(self: &Self) -> usize` - Get the current value

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Gauge`



## datafusion_physical_expr_common::metrics::value::MetricValue

*Enum*

Possible values for a [super::Metric].

Among other differences, the metric types have different ways to
logically interpret their underlying values and some metrics are
so common they are given special treatment.

**Variants:**
- `OutputRows(Count)` - Number of output rows produced: "output_rows" metric
- `ElapsedCompute(Time)` - Elapsed Compute Time: the wall clock time spent in "cpu
- `SpillCount(Count)` - Number of spills produced: "spill_count" metric
- `SpilledBytes(Count)` - Total size of spilled bytes produced: "spilled_bytes" metric
- `OutputBytes(Count)` - Total size of output bytes produced: "output_bytes" metric
- `OutputBatches(Count)` - Total number of output batches produced: "output_batches" metric
- `SpilledRows(Count)` - Total size of spilled rows produced: "spilled_rows" metric
- `CurrentMemoryUsage(Gauge)` - Current memory used
- `Count{ name: std::borrow::Cow<'static, str>, count: Count }` - Operator defined count.
- `Gauge{ name: std::borrow::Cow<'static, str>, gauge: Gauge }` - Operator defined gauge.
- `Time{ name: std::borrow::Cow<'static, str>, time: Time }` - Operator defined time
- `StartTimestamp(Timestamp)` - The time at which execution started
- `EndTimestamp(Timestamp)` - The time at which execution ended
- `PruningMetrics{ name: std::borrow::Cow<'static, str>, pruning_metrics: PruningMetrics }` - Metrics related to scan pruning
- `Ratio{ name: std::borrow::Cow<'static, str>, ratio_metrics: RatioMetrics }` - Metrics that should be displayed as ratio like (42%)
- `Custom{ name: std::borrow::Cow<'static, str>, value: std::sync::Arc<dyn CustomMetricValue> }`

**Methods:**

- `fn name(self: &Self) -> &str` - Return the name of this SQL metric
- `fn as_usize(self: &Self) -> usize` - Return the value of the metric as a usize value, used to aggregate metric
- `fn new_empty(self: &Self) -> Self` - create a new MetricValue with the same type as `self` suitable
- `fn aggregate(self: & mut Self, other: &Self)` - Aggregates the value of other to `self`. panic's if the types
- `fn display_sort_key(self: &Self) -> u8` - Returns a number by which to sort metrics by display. Lower
- `fn is_timestamp(self: &Self) -> bool` - returns true if this metric has a timestamp value

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result` - Prints the value of this metric
- **Clone**
  - `fn clone(self: &Self) -> MetricValue`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_physical_expr_common::metrics::value::PruningMetrics

*Struct*

Counters tracking pruning metrics

For example, a file scanner initially is planned to scan 10 files, but skipped
8 of them using statistics, the pruning metrics would look like: 10 total -> 2 matched

Note `clone`ing update the same underlying metrics

**Methods:**

- `fn new() -> Self` - create a new PruningMetrics
- `fn add_pruned(self: &Self, n: usize)` - Add `n` to the metric's pruned value
- `fn add_matched(self: &Self, n: usize)` - Add `n` to the metric's matched value
- `fn subtract_matched(self: &Self, n: usize)` - Subtract `n` to the metric's matched value.
- `fn pruned(self: &Self) -> usize` - Number of items pruned
- `fn matched(self: &Self) -> usize` - Number of items matched (not pruned)

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> PruningMetrics`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> Self`



## datafusion_physical_expr_common::metrics::value::RatioMergeStrategy

*Enum*

**Variants:**
- `AddPartAddTotal`
- `AddPartSetTotal`
- `SetPartAddTotal`

**Trait Implementations:**

- **Default**
  - `fn default() -> RatioMergeStrategy`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> RatioMergeStrategy`



## datafusion_physical_expr_common::metrics::value::RatioMetrics

*Struct*

Counters tracking ratio metrics (e.g. matched vs total)

The counters are thread-safe and shared across clones.

**Methods:**

- `fn new() -> Self` - Create a new [`RatioMetrics`]
- `fn with_merge_strategy(self: Self, merge_strategy: RatioMergeStrategy) -> Self`
- `fn add_part(self: &Self, n: usize)` - Add `n` to the numerator (`part`) value
- `fn add_total(self: &Self, n: usize)` - Add `n` to the denominator (`total`) value
- `fn set_part(self: &Self, n: usize)` - Set the numerator (`part`) value to `n`, overwriting any existing value
- `fn set_total(self: &Self, n: usize)` - Set the denominator (`total`) value to `n`, overwriting any existing value
- `fn merge(self: &Self, other: &Self)` - Merge the value from `other` into `self`
- `fn part(self: &Self) -> usize` - Return the numerator (`part`) value
- `fn total(self: &Self) -> usize` - Return the denominator (`total`) value

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> RatioMetrics`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Default**
  - `fn default() -> RatioMetrics`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_physical_expr_common::metrics::value::ScopedTimerGuard

*Struct*

RAAI structure that adds all time between its construction and
destruction to the CPU time or the first call to `stop` whichever
comes first

**Generic Parameters:**
- 'a

**Methods:**

- `fn stop(self: & mut Self)` - Stop the timer timing and record the time taken
- `fn restart(self: & mut Self)` - Restarts the timer recording from the current time
- `fn done(self: Self)` - Stop the timer, record the time taken and consume self
- `fn stop_with(self: & mut Self, end_time: Instant)` - Stop the timer timing and record the time taken since the given endpoint.
- `fn done_with(self: Self, end_time: Instant)` - Stop the timer, record the time taken since `end_time` endpoint, and

**Trait Implementations:**

- **Drop**
  - `fn drop(self: & mut Self)`



## datafusion_physical_expr_common::metrics::value::Time

*Struct*

Measure a potentially non contiguous duration of time

**Methods:**

- `fn new() -> Self` - Create a new [`Time`] wrapper suitable for recording elapsed
- `fn add_elapsed(self: &Self, start: Instant)` - Add elapsed nanoseconds since `start`to self
- `fn add_duration(self: &Self, duration: Duration)` - Add duration of time to self
- `fn add(self: &Self, other: &Time)` - Add the number of nanoseconds of other `Time` to self
- `fn timer(self: &Self) -> ScopedTimerGuard` - return a scoped guard that adds the amount of time elapsed
- `fn value(self: &Self) -> usize` - Get the number of nanoseconds record by this Time metric
- `fn timer_with(self: &Self, now: Instant) -> ScopedTimerGuard` - Return a scoped guard that adds the amount of time elapsed between the

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> Time`



## datafusion_physical_expr_common::metrics::value::Timestamp

*Struct*

Stores a single timestamp, stored as the number of nanoseconds
elapsed from Jan 1, 1970 UTC

**Methods:**

- `fn new() -> Self` - Create a new timestamp and sets its value to 0
- `fn record(self: &Self)` - Sets the timestamps value to the current time
- `fn set(self: &Self, now: DateTime<Utc>)` - Sets the timestamps value to a specified time
- `fn value(self: &Self) -> Option<DateTime<Utc>>` - return the timestamps value at the last time `record()` was
- `fn update_to_min(self: &Self, other: &Timestamp)` - sets the value of this timestamp to the minimum of this and other
- `fn update_to_max(self: &Self, other: &Timestamp)` - sets the value of this timestamp to the maximum of this and other

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> Timestamp`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Default**
  - `fn default() -> Self`



