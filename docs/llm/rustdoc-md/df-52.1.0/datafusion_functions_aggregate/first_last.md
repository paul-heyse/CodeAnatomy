**datafusion_functions_aggregate > first_last**

# Module: first_last

## Contents

**Structs**

- [`FirstValue`](#firstvalue)
- [`FirstValueAccumulator`](#firstvalueaccumulator)
- [`LastValue`](#lastvalue)
- [`TrivialFirstValueAccumulator`](#trivialfirstvalueaccumulator) - This accumulator is used when there is no ordering specified for the
- [`TrivialLastValueAccumulator`](#triviallastvalueaccumulator) - This accumulator is used when there is no ordering specified for the

**Functions**

- [`first_value`](#first_value) - Returns the first value in a group of values.
- [`first_value_udaf`](#first_value_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`FirstValue`]
- [`last_value`](#last_value) - Returns the last value in a group of values.
- [`last_value_udaf`](#last_value_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`LastValue`]

---

## datafusion_functions_aggregate::first_last::FirstValue

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &FirstValue) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn return_field(self: &Self, arg_fields: &[FieldRef]) -> Result<FieldRef>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn groups_accumulator_supported(self: &Self, args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn with_beneficial_ordering(self: Arc<Self>, beneficial_ordering: bool) -> Result<Option<Arc<dyn AggregateUDFImpl>>>`
  - `fn order_sensitivity(self: &Self) -> AggregateOrderSensitivity`
  - `fn reverse_expr(self: &Self) -> ReversedUDAF`
  - `fn supports_null_handling_clause(self: &Self) -> bool`
  - `fn documentation(self: &Self) -> Option<&Documentation>`



## datafusion_functions_aggregate::first_last::FirstValueAccumulator

*Struct*

**Methods:**

- `fn try_new(data_type: &DataType, ordering_dtypes: &[DataType], ordering_req: LexOrdering, is_input_pre_ordered: bool, ignore_nulls: bool) -> Result<Self>` - Creates a new `FirstValueAccumulator` for the given `data_type`.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Accumulator**
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn size(self: &Self) -> usize`



## datafusion_functions_aggregate::first_last::LastValue

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &LastValue) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn return_field(self: &Self, arg_fields: &[FieldRef]) -> Result<FieldRef>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn with_beneficial_ordering(self: Arc<Self>, beneficial_ordering: bool) -> Result<Option<Arc<dyn AggregateUDFImpl>>>`
  - `fn order_sensitivity(self: &Self) -> AggregateOrderSensitivity`
  - `fn reverse_expr(self: &Self) -> ReversedUDAF`
  - `fn supports_null_handling_clause(self: &Self) -> bool`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
  - `fn groups_accumulator_supported(self: &Self, args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
- **Default**
  - `fn default() -> Self`



## datafusion_functions_aggregate::first_last::TrivialFirstValueAccumulator

*Struct*

This accumulator is used when there is no ordering specified for the
`FIRST_VALUE` aggregation. It simply returns the first value it sees
according to the pre-existing ordering of the input data, and provides
a fast path for this case without needing to maintain any ordering state.

**Methods:**

- `fn try_new(data_type: &DataType, ignore_nulls: bool) -> Result<Self>` - Creates a new `TrivialFirstValueAccumulator` for the given `data_type`.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Accumulator**
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn size(self: &Self) -> usize`



## datafusion_functions_aggregate::first_last::TrivialLastValueAccumulator

*Struct*

This accumulator is used when there is no ordering specified for the
`LAST_VALUE` aggregation. It simply updates the last value it sees
according to the pre-existing ordering of the input data, and provides
a fast path for this case without needing to maintain any ordering state.

**Methods:**

- `fn try_new(data_type: &DataType, ignore_nulls: bool) -> Result<Self>` - Creates a new `TrivialLastValueAccumulator` for the given `data_type`.

**Trait Implementations:**

- **Accumulator**
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn size(self: &Self) -> usize`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_aggregate::first_last::first_value

*Function*

Returns the first value in a group of values.

```rust
fn first_value(expression: datafusion_expr::Expr, order_by: Vec<datafusion_expr::SortExpr>) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::first_last::first_value_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`FirstValue`]

```rust
fn first_value_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::first_last::last_value

*Function*

Returns the last value in a group of values.

```rust
fn last_value(expression: datafusion_expr::Expr, order_by: Vec<datafusion_expr::SortExpr>) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::first_last::last_value_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`LastValue`]

```rust
fn last_value_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



