**datafusion_physical_expr_common > utils**

# Module: utils

## Contents

**Functions**

- [`evaluate_expressions_to_arrays`](#evaluate_expressions_to_arrays) - Evaluates expressions against a record batch.
- [`evaluate_expressions_to_arrays_with_metrics`](#evaluate_expressions_to_arrays_with_metrics) - Same as [`evaluate_expressions_to_arrays`] but records optional per-expression metrics.
- [`scatter`](#scatter) - Scatter `truthy` array by boolean mask. When the mask evaluates `true`, next values of `truthy`

**Type Aliases**

- [`ExprPropertiesNode`](#exprpropertiesnode) - Represents a [`PhysicalExpr`] node with associated properties (order and

---

## datafusion_physical_expr_common::utils::ExprPropertiesNode

*Type Alias*: `crate::tree_node::ExprContext<datafusion_expr_common::sort_properties::ExprProperties>`

Represents a [`PhysicalExpr`] node with associated properties (order and
range) in a context where properties are tracked.



## datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays

*Function*

Evaluates expressions against a record batch.
This will convert the resulting ColumnarValues to ArrayRefs,
duplicating any ScalarValues that may have been returned,
and validating that the returned arrays all have the same
number of rows as the input batch.

```rust
fn evaluate_expressions_to_arrays<'a, impl IntoIterator<Item = &'a Arc<dyn PhysicalExpr>>>(exprs: impl Trait, batch: &arrow::record_batch::RecordBatch) -> datafusion_common::Result<Vec<arrow::array::ArrayRef>>
```



## datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays_with_metrics

*Function*

Same as [`evaluate_expressions_to_arrays`] but records optional per-expression metrics.

For metrics tracking, see [`ExpressionEvaluatorMetrics`] for details.

```rust
fn evaluate_expressions_to_arrays_with_metrics<'a, impl IntoIterator<Item = &'a Arc<dyn PhysicalExpr>>>(exprs: impl Trait, batch: &arrow::record_batch::RecordBatch, metrics: Option<&crate::metrics::ExpressionEvaluatorMetrics>) -> datafusion_common::Result<Vec<arrow::array::ArrayRef>>
```



## datafusion_physical_expr_common::utils::scatter

*Function*

Scatter `truthy` array by boolean mask. When the mask evaluates `true`, next values of `truthy`
are taken, when the mask evaluates `false` values null values are filled.

# Arguments
* `mask` - Boolean values used to determine where to put the `truthy` values
* `truthy` - All values of this array are to scatter according to `mask` into final result.

```rust
fn scatter(mask: &arrow::array::BooleanArray, truthy: &dyn Array) -> datafusion_common::Result<arrow::array::ArrayRef>
```



