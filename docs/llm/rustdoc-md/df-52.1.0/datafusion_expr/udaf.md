**datafusion_expr > udaf**

# Module: udaf

## Contents

**Structs**

- [`AggregateUDF`](#aggregateudf) - Logical representation of a user-defined [aggregate function] (UDAF).
- [`StatisticsArgs`](#statisticsargs) - Arguments passed to [`AggregateUDFImpl::value_from_stats`]

**Enums**

- [`ReversedUDAF`](#reversedudaf)
- [`SetMonotonicity`](#setmonotonicity) - Indicates whether an aggregation function is monotonic as a set

**Functions**

- [`udaf_default_display_name`](#udaf_default_display_name) - Encapsulates default implementation of [`AggregateUDFImpl::display_name`].
- [`udaf_default_human_display`](#udaf_default_human_display) - Encapsulates default implementation of [`AggregateUDFImpl::human_display`].
- [`udaf_default_return_field`](#udaf_default_return_field) - Encapsulates default implementation of [`AggregateUDFImpl::return_field`].
- [`udaf_default_schema_name`](#udaf_default_schema_name) - Encapsulates default implementation of [`AggregateUDFImpl::schema_name`].
- [`udaf_default_window_function_display_name`](#udaf_default_window_function_display_name) - Encapsulates default implementation of [`AggregateUDFImpl::window_function_display_name`].
- [`udaf_default_window_function_schema_name`](#udaf_default_window_function_schema_name) - Encapsulates default implementation of [`AggregateUDFImpl::window_function_schema_name`].

**Traits**

- [`AggregateUDFImpl`](#aggregateudfimpl) - Trait for implementing [`AggregateUDF`].

---

## datafusion_expr::udaf::AggregateUDF

*Struct*

Logical representation of a user-defined [aggregate function] (UDAF).

An aggregate function combines the values from multiple input rows
into a single output "aggregate" (summary) row. It is different
from a scalar function because it is stateful across batches. User
defined aggregate functions can be used as normal SQL aggregate
functions (`GROUP BY` clause) as well as window functions (`OVER`
clause).

`AggregateUDF` provides DataFusion the information needed to plan and call
aggregate functions, including name, type information, and a factory
function to create an [`Accumulator`] instance, to perform the actual
aggregation.

For more information, please see [the examples]:

1. For simple use cases, use [`create_udaf`] (examples in [`simple_udaf.rs`]).

2. For advanced use cases, use [`AggregateUDFImpl`] which provides full API
   access (examples in [`advanced_udaf.rs`]).

# API Note
This is a separate struct from `AggregateUDFImpl` to maintain backwards
compatibility with the older API.

[the examples]: https://github.com/apache/datafusion/tree/main/datafusion-examples#single-process
[aggregate function]: https://en.wikipedia.org/wiki/Aggregate_function
[`Accumulator`]: Accumulator
[`create_udaf`]: crate::expr_fn::create_udaf
[`simple_udaf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/udf/simple_udaf.rs
[`advanced_udaf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/udf/advanced_udaf.rs

**Methods:**

- `fn new_from_impl<F>(fun: F) -> AggregateUDF` - Create a new `AggregateUDF` from a `[AggregateUDFImpl]` trait object
- `fn new_from_shared_impl(fun: Arc<dyn AggregateUDFImpl>) -> AggregateUDF` - Create a new `AggregateUDF` from a `[AggregateUDFImpl]` trait object
- `fn inner(self: &Self) -> &Arc<dyn AggregateUDFImpl>` - Return the underlying [`AggregateUDFImpl`] trait object for this function
- `fn with_aliases<impl IntoIterator<Item = &'static str>>(self: Self, aliases: impl Trait) -> Self` - Adds additional names that can be used to invoke this function, in
- `fn call(self: &Self, args: Vec<Expr>) -> Expr` - Creates an [`Expr`] that calls the aggregate function.
- `fn name(self: &Self) -> &str` - Returns this function's name
- `fn aliases(self: &Self) -> &[String]` - Returns the aliases for this function.
- `fn schema_name(self: &Self, params: &AggregateFunctionParams) -> Result<String>` - See [`AggregateUDFImpl::schema_name`] for more details.
- `fn human_display(self: &Self, params: &AggregateFunctionParams) -> Result<String>` - Returns a human readable expression.
- `fn window_function_schema_name(self: &Self, params: &WindowFunctionParams) -> Result<String>`
- `fn display_name(self: &Self, params: &AggregateFunctionParams) -> Result<String>` - See [`AggregateUDFImpl::display_name`] for more details.
- `fn window_function_display_name(self: &Self, params: &WindowFunctionParams) -> Result<String>`
- `fn is_nullable(self: &Self) -> bool`
- `fn signature(self: &Self) -> &Signature` - Returns this function's signature (what input types are accepted)
- `fn return_type(self: &Self, args: &[DataType]) -> Result<DataType>` - Return the type of the function given its input types
- `fn return_field(self: &Self, args: &[FieldRef]) -> Result<FieldRef>` - Return the field of the function given its input fields
- `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>` - Return an accumulator the given aggregate, given its return datatype
- `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>` - Return the fields used to store the intermediate state for this aggregator, given
- `fn groups_accumulator_supported(self: &Self, args: AccumulatorArgs) -> bool` - See [`AggregateUDFImpl::groups_accumulator_supported`] for more details.
- `fn create_groups_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>` - See [`AggregateUDFImpl::create_groups_accumulator`] for more details.
- `fn create_sliding_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
- `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`
- `fn with_beneficial_ordering(self: Self, beneficial_ordering: bool) -> Result<Option<AggregateUDF>>` - See [`AggregateUDFImpl::with_beneficial_ordering`] for more details.
- `fn order_sensitivity(self: &Self) -> AggregateOrderSensitivity` - Gets the order sensitivity of the UDF. See [`AggregateOrderSensitivity`]
- `fn reverse_udf(self: &Self) -> ReversedUDAF` - Reserves the `AggregateUDF` (e.g. returns the `AggregateUDF` that will
- `fn simplify(self: &Self) -> Option<AggregateFunctionSimplification>` - Do the function rewrite
- `fn is_descending(self: &Self) -> Option<bool>` - Returns true if the function is max, false if the function is min
- `fn value_from_stats(self: &Self, statistics_args: &StatisticsArgs) -> Option<ScalarValue>` - Return the value of this aggregate function if it can be determined
- `fn default_value(self: &Self, data_type: &DataType) -> Result<ScalarValue>` - See [`AggregateUDFImpl::default_value`] for more details.
- `fn supports_null_handling_clause(self: &Self) -> bool` - See [`AggregateUDFImpl::supports_null_handling_clause`] for more details.
- `fn supports_within_group_clause(self: &Self) -> bool` - See [`AggregateUDFImpl::supports_within_group_clause`] for more details.
- `fn documentation(self: &Self) -> Option<&Documentation>` - Returns the documentation for this Aggregate UDF.

**Traits:** Eq

**Trait Implementations:**

- **From**
  - `fn from(fun: F) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> AggregateUDF`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &AggregateUDF) -> $crate::option::Option<$crate::cmp::Ordering>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **UDFCoercionExt**
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`



## datafusion_expr::udaf::AggregateUDFImpl

*Trait*

Trait for implementing [`AggregateUDF`].

This trait exposes the full API for implementing user defined aggregate functions and
can be used to implement any function.

See [`advanced_udaf.rs`] for a full example with complete implementation and
[`AggregateUDF`] for other available options.

[`advanced_udaf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/udf/advanced_udaf.rs

# Basic Example
```
# use std::any::Any;
# use std::sync::{Arc, LazyLock};
# use arrow::datatypes::{DataType, FieldRef};
# use datafusion_common::{DataFusionError, plan_err, Result};
# use datafusion_expr::{col, ColumnarValue, Signature, Volatility, Expr, Documentation};
# use datafusion_expr::{AggregateUDFImpl, AggregateUDF, Accumulator, function::{AccumulatorArgs, StateFieldsArgs}};
# use datafusion_expr::window_doc_sections::DOC_SECTION_AGGREGATE;
# use arrow::datatypes::Schema;
# use arrow::datatypes::Field;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GeoMeanUdf {
  signature: Signature,
}

impl GeoMeanUdf {
  fn new() -> Self {
    Self {
      signature: Signature::uniform(1, vec![DataType::Float64], Volatility::Immutable),
     }
  }
}

static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
        Documentation::builder(DOC_SECTION_AGGREGATE, "calculates a geometric mean", "geo_mean(2.0)")
            .with_argument("arg1", "The Float64 number for the geometric mean")
            .build()
    });

fn get_doc() -> &'static Documentation {
    &DOCUMENTATION
}

/// Implement the AggregateUDFImpl trait for GeoMeanUdf
impl AggregateUDFImpl for GeoMeanUdf {
   fn as_any(&self) -> &dyn Any { self }
   fn name(&self) -> &str { "geo_mean" }
   fn signature(&self) -> &Signature { &self.signature }
   fn return_type(&self, args: &[DataType]) -> Result<DataType> {
     if !matches!(args.get(0), Some(&DataType::Float64)) {
       return plan_err!("geo_mean only accepts Float64 arguments");
     }
     Ok(DataType::Float64)
   }
   // This is the accumulator factory; DataFusion uses it to create new accumulators.
   fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> { unimplemented!() }
   fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
       Ok(vec![
            Arc::new(args.return_field.as_ref().clone().with_name("value")),
            Arc::new(Field::new("ordering", DataType::UInt32, true))
       ])
   }
   fn documentation(&self) -> Option<&Documentation> {
       Some(get_doc())
   }
}

// Create a new AggregateUDF from the implementation
let geometric_mean = AggregateUDF::from(GeoMeanUdf::new());

// Call the function `geo_mean(col)`
let expr = geometric_mean.call(vec![col("a")]);
```

**Methods:**

- `as_any`: Returns this object as an [`Any`] trait object
- `name`: Returns this function's name
- `aliases`: Returns any aliases (alternate names) for this function.
- `schema_name`: Returns the name of the column this expression would create
- `human_display`: Returns a human readable expression.
- `window_function_schema_name`: Returns the name of the column this expression would create
- `display_name`: Returns the user-defined display name of function, given the arguments
- `window_function_display_name`: Returns the user-defined display name of function, given the arguments
- `signature`: Returns the function's [`Signature`] for information about what input
- `return_type`: What [`DataType`] will be returned by this function, given the types of
- `return_field`: What type will be returned by this function, given the arguments?
- `is_nullable`: Whether the aggregate function is nullable.
- `accumulator`: Return a new [`Accumulator`] that aggregates values for a specific
- `state_fields`: Return the fields used to store the intermediate state of this accumulator.
- `groups_accumulator_supported`: If the aggregate expression has a specialized
- `create_groups_accumulator`: Return a specialized [`GroupsAccumulator`] that manages state
- `create_sliding_accumulator`: Sliding accumulator is an alternative accumulator that can be used for
- `with_beneficial_ordering`: Sets the indicator whether ordering requirements of the AggregateUDFImpl is
- `order_sensitivity`: Gets the order sensitivity of the UDF. See [`AggregateOrderSensitivity`]
- `simplify`: Optionally apply per-UDaF simplification / rewrite rules.
- `reverse_expr`: Returns the reverse expression of the aggregate function.
- `coerce_types`: Coerce arguments of a function call to types that the function can evaluate.
- `is_descending`: If this function is max, return true
- `value_from_stats`: Return the value of this aggregate function if it can be determined
- `default_value`: Returns default value of the function given the input is all `null`.
- `supports_null_handling_clause`: If this function supports `[IGNORE NULLS | RESPECT NULLS]` SQL clause,
- `supports_within_group_clause`: If this function supports the `WITHIN GROUP (ORDER BY column [ASC|DESC])`
- `documentation`: Returns the documentation for this Aggregate UDF.
- `set_monotonicity`: Indicates whether the aggregation function is monotonic as a set



## datafusion_expr::udaf::ReversedUDAF

*Enum*

**Variants:**
- `Identical` - The expression is the same as the original expression, like SUM, COUNT
- `NotSupported` - The expression does not support reverse calculation
- `Reversed(std::sync::Arc<AggregateUDF>)` - The expression is different from the original expression



## datafusion_expr::udaf::SetMonotonicity

*Enum*

Indicates whether an aggregation function is monotonic as a set
function. A set function is monotonically increasing if its value
increases as its argument grows (as a set). Formally, `f` is a
monotonically increasing set function if `f(S) >= f(T)` whenever `S`
is a superset of `T`.

For example `COUNT` and `MAX` are monotonically increasing as their
values always increase (or stay the same) as new values are seen. On
the other hand, `MIN` is monotonically decreasing as its value always
decreases or stays the same as new values are seen.

**Variants:**
- `Increasing` - Aggregate value increases or stays the same as the input set grows.
- `Decreasing` - Aggregate value decreases or stays the same as the input set grows.
- `NotMonotonic` - Aggregate value may increase, decrease, or stay the same as the input

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> SetMonotonicity`
- **PartialEq**
  - `fn eq(self: &Self, other: &SetMonotonicity) -> bool`



## datafusion_expr::udaf::StatisticsArgs

*Struct*

Arguments passed to [`AggregateUDFImpl::value_from_stats`]

**Generic Parameters:**
- 'a

**Fields:**
- `statistics: &'a datafusion_common::Statistics` - The statistics of the aggregate input
- `return_type: &'a arrow::datatypes::DataType` - The resolved return type of the aggregate function
- `is_distinct: bool` - Whether the aggregate function is distinct.
- `exprs: &'a [std::sync::Arc<dyn PhysicalExpr>]` - The physical expression of arguments the aggregate function takes.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::udaf::udaf_default_display_name

*Function*

Encapsulates default implementation of [`AggregateUDFImpl::display_name`].

```rust
fn udaf_default_display_name<F>(func: &F, params: &crate::expr::AggregateFunctionParams) -> datafusion_common::Result<String>
```



## datafusion_expr::udaf::udaf_default_human_display

*Function*

Encapsulates default implementation of [`AggregateUDFImpl::human_display`].

```rust
fn udaf_default_human_display<F>(func: &F, params: &crate::expr::AggregateFunctionParams) -> datafusion_common::Result<String>
```



## datafusion_expr::udaf::udaf_default_return_field

*Function*

Encapsulates default implementation of [`AggregateUDFImpl::return_field`].

```rust
fn udaf_default_return_field<F>(func: &F, arg_fields: &[arrow::datatypes::FieldRef]) -> datafusion_common::Result<arrow::datatypes::FieldRef>
```



## datafusion_expr::udaf::udaf_default_schema_name

*Function*

Encapsulates default implementation of [`AggregateUDFImpl::schema_name`].

```rust
fn udaf_default_schema_name<F>(func: &F, params: &crate::expr::AggregateFunctionParams) -> datafusion_common::Result<String>
```



## datafusion_expr::udaf::udaf_default_window_function_display_name

*Function*

Encapsulates default implementation of [`AggregateUDFImpl::window_function_display_name`].

```rust
fn udaf_default_window_function_display_name<F>(func: &F, params: &crate::expr::WindowFunctionParams) -> datafusion_common::Result<String>
```



## datafusion_expr::udaf::udaf_default_window_function_schema_name

*Function*

Encapsulates default implementation of [`AggregateUDFImpl::window_function_schema_name`].

```rust
fn udaf_default_window_function_schema_name<F>(func: &F, params: &crate::expr::WindowFunctionParams) -> datafusion_common::Result<String>
```



