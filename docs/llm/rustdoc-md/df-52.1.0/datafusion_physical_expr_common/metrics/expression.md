**datafusion_physical_expr_common > metrics > expression**

# Module: metrics::expression

## Contents

**Structs**

- [`ExpressionEvaluatorMetrics`](#expressionevaluatormetrics) - Tracks evaluation time for a sequence of expressions.

---

## datafusion_physical_expr_common::metrics::expression::ExpressionEvaluatorMetrics

*Struct*

Tracks evaluation time for a sequence of expressions.

# Example
Given SQL query:
    EXPLAIN ANALYZE
    SELECT a+1, pow(a,2)
    FROM generate_series(1, 1000000) as t1(a)

This struct holds two time metrics for the projection expressions
`a+1` and `pow(a,2)`, respectively.

The output reads:
`ProjectionExec: expr=[a@0 + 1 as t1.a + Int64(1), power(CAST(a@0 AS Float64), 2) as pow(t1.a,Int64(2))], metrics=[... expr_0_eval_time=9.23ms, expr_1_eval_time=32.35ms...]`

**Methods:**

- `fn new<T, impl IntoIterator<Item = T>>(metrics: &ExecutionPlanMetricsSet, partition: usize, expression_labels: impl Trait) -> Self` - Create metrics for a collection of expressions.
- `fn scoped_timer(self: &Self, index: usize) -> Option<ScopedTimerGuard>` - Returns a timer guard for the expression at `index`, if present.
- `fn len(self: &Self) -> usize` - The number of tracked expressions.
- `fn is_empty(self: &Self) -> bool` - True when no expressions are tracked.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ExpressionEvaluatorMetrics`



