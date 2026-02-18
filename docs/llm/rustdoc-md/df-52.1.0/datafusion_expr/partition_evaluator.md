**datafusion_expr > partition_evaluator**

# Module: partition_evaluator

## Contents

**Traits**

- [`PartitionEvaluator`](#partitionevaluator) - Partition evaluator for Window Functions

---

## datafusion_expr::partition_evaluator::PartitionEvaluator

*Trait*

Partition evaluator for Window Functions

# Background

An implementation of this trait is created and used for each
partition defined by an `OVER` clause and is instantiated by
the DataFusion runtime.

For example, evaluating `window_func(val) OVER (PARTITION BY col)`
on the following data:

```text
col | val
--- + ----
 A  | 10
 A  | 10
 C  | 20
 D  | 30
 D  | 30
```

Will instantiate three `PartitionEvaluator`s, one each for the
partitions defined by `col=A`, `col=B`, and `col=C`.

```text
col | val
--- + ----
 A  | 10     <--- partition 1
 A  | 10

col | val
--- + ----
 C  | 20     <--- partition 2

col | val
--- + ----
 D  | 30     <--- partition 3
 D  | 30
```

Different methods on this trait will be called depending on the
capabilities described by [`supports_bounded_execution`],
[`uses_window_frame`], and [`include_rank`],

When implementing a new `PartitionEvaluator`, implement
corresponding evaluator according to table below.

# Implementation Table

|[`uses_window_frame`]|[`supports_bounded_execution`]|[`include_rank`]|function_to_implement|
|---|---|----|----|
|false (default)      |false (default)               |false (default)   | [`evaluate_all`]           |
|false                |true                          |false             | [`evaluate`]               |
|false                |true/false                    |true              | [`evaluate_all_with_rank`] |
|true                 |true/false                    |true/false        | [`evaluate`]               |

[`evaluate`]: Self::evaluate
[`evaluate_all`]: Self::evaluate_all
[`evaluate_all_with_rank`]: Self::evaluate_all_with_rank
[`uses_window_frame`]: Self::uses_window_frame
[`include_rank`]: Self::include_rank
[`supports_bounded_execution`]: Self::supports_bounded_execution

**Methods:**

- `memoize`: When the window frame has a fixed beginning (e.g UNBOUNDED
- `get_range`: If `uses_window_frame` flag is `false`. This method is used to
- `is_causal`: Get whether evaluator needs future data for its result (if so returns `false`) or not
- `evaluate_all`: Evaluate a window function on an entire input partition.
- `evaluate`: Evaluate window function on a range of rows in an input
- `evaluate_all_with_rank`: [`PartitionEvaluator::evaluate_all_with_rank`] is called for window
- `supports_bounded_execution`: Can the window function be incrementally computed using
- `uses_window_frame`: Does the window function use the values from the window frame,
- `include_rank`: Can this function be evaluated with (only) rank



