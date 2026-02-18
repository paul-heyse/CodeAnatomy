**datafusion_functions_aggregate > array_agg**

# Module: array_agg

## Contents

**Structs**

- [`ArrayAgg`](#arrayagg) - ARRAY_AGG aggregate expression
- [`ArrayAggAccumulator`](#arrayaggaccumulator)

**Functions**

- [`array_agg`](#array_agg) - input values, including nulls, concatenated into an array
- [`array_agg_udaf`](#array_agg_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`ArrayAgg`]

---

## datafusion_functions_aggregate::array_agg::ArrayAgg

*Struct*

ARRAY_AGG aggregate expression

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayAgg) -> bool`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn order_sensitivity(self: &Self) -> AggregateOrderSensitivity`
  - `fn with_beneficial_ordering(self: Arc<Self>, beneficial_ordering: bool) -> Result<Option<Arc<dyn AggregateUDFImpl>>>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn reverse_expr(self: &Self) -> datafusion_expr::ReversedUDAF`
  - `fn supports_null_handling_clause(self: &Self) -> bool`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_aggregate::array_agg::ArrayAggAccumulator

*Struct*

**Methods:**

- `fn try_new(datatype: &DataType, ignore_nulls: bool) -> Result<Self>` - new array_agg accumulator based on given item data type

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Accumulator**
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn size(self: &Self) -> usize`



## datafusion_functions_aggregate::array_agg::array_agg

*Function*

input values, including nulls, concatenated into an array

```rust
fn array_agg(expression: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::array_agg::array_agg_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`ArrayAgg`]

```rust
fn array_agg_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



