**datafusion_expr > udwf**

# Module: udwf

## Contents

**Structs**

- [`WindowUDF`](#windowudf) - Logical representation of a user-defined window function (UDWF).

**Enums**

- [`LimitEffect`](#limiteffect) - the effect this function will have on the limit pushdown
- [`ReversedUDWF`](#reversedudwf)

**Traits**

- [`WindowUDFImpl`](#windowudfimpl) - Trait for implementing [`WindowUDF`].

---

## datafusion_expr::udwf::LimitEffect

*Enum*

the effect this function will have on the limit pushdown

**Variants:**
- `None` - Does not affect the limit (i.e. this is causal)
- `Unknown` - Either undeclared, or dynamic (only evaluatable at run time)
- `Relative(usize)` - Grow the limit by N rows
- `Absolute(usize)` - Limit needs to be at least N rows



## datafusion_expr::udwf::ReversedUDWF

*Enum*

**Variants:**
- `Identical` - The result of evaluating the user-defined window function
- `NotSupported` - A window function which does not support evaluating the result
- `Reversed(std::sync::Arc<WindowUDF>)` - Customize the user-defined window function for evaluating the



## datafusion_expr::udwf::WindowUDF

*Struct*

Logical representation of a user-defined window function (UDWF).

A Window Function is called via the SQL `OVER` clause:

```sql
SELECT first_value(col) OVER (PARTITION BY a, b ORDER BY c) FROM foo;
```

A UDWF is different from a user defined function (UDF) in that it is
stateful across batches.

See the documentation on [`PartitionEvaluator`] for more details

1. For simple use cases, use [`create_udwf`] (examples in
   [`simple_udwf.rs`]).

2. For advanced use cases, use [`WindowUDFImpl`] which provides full API
   access (examples in [`advanced_udwf.rs`]).

# API Note
This is a separate struct from `WindowUDFImpl` to maintain backwards
compatibility with the older API.

[`PartitionEvaluator`]: crate::PartitionEvaluator
[`create_udwf`]: crate::expr_fn::create_udwf
[`simple_udwf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/udf/simple_udwf.rs
[`advanced_udwf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/udf/advanced_udwf.rs

**Methods:**

- `fn new_from_impl<F>(fun: F) -> WindowUDF` - Create a new `WindowUDF` from a `[WindowUDFImpl]` trait object
- `fn new_from_shared_impl(fun: Arc<dyn WindowUDFImpl>) -> WindowUDF` - Create a new `WindowUDF` from a `[WindowUDFImpl]` trait object
- `fn inner(self: &Self) -> &Arc<dyn WindowUDFImpl>` - Return the underlying [`WindowUDFImpl`] trait object for this function
- `fn with_aliases<impl IntoIterator<Item = &'static str>>(self: Self, aliases: impl Trait) -> Self` - Adds additional names that can be used to invoke this function, in
- `fn call(self: &Self, args: Vec<Expr>) -> Expr` - creates a [`Expr`] that calls the window function with default
- `fn name(self: &Self) -> &str` - Returns this function's name
- `fn aliases(self: &Self) -> &[String]` - Returns the aliases for this function.
- `fn signature(self: &Self) -> &Signature` - Returns this function's signature (what input types are accepted)
- `fn simplify(self: &Self) -> Option<WindowFunctionSimplification>` - Do the function rewrite
- `fn expressions(self: &Self, expr_args: ExpressionArgs) -> Vec<Arc<dyn PhysicalExpr>>` - Expressions that are passed to the [`PartitionEvaluator`].
- `fn partition_evaluator_factory(self: &Self, partition_evaluator_args: PartitionEvaluatorArgs) -> Result<Box<dyn PartitionEvaluator>>` - Return a `PartitionEvaluator` for evaluating this window function
- `fn field(self: &Self, field_args: WindowUDFFieldArgs) -> Result<FieldRef>` - Returns the field of the final result of evaluating this window function.
- `fn sort_options(self: &Self) -> Option<SortOptions>` - Returns custom result ordering introduced by this window function
- `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>` - See [`WindowUDFImpl::coerce_types`] for more details.
- `fn reverse_expr(self: &Self) -> ReversedUDWF` - Returns the reversed user-defined window function when the
- `fn documentation(self: &Self) -> Option<&Documentation>` - Returns the documentation for this Window UDF.

**Traits:** Eq

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> WindowUDF`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **UDFCoercionExt**
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &WindowUDF) -> $crate::option::Option<$crate::cmp::Ordering>`
- **From**
  - `fn from(fun: F) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::udwf::WindowUDFImpl

*Trait*

Trait for implementing [`WindowUDF`].

This trait exposes the full API for implementing user defined window functions and
can be used to implement any function.

While the trait depends on [`DynEq`] and [`DynHash`] traits, these should not be
implemented directly. Instead, implement [`Eq`] and [`Hash`] and leverage the
blanket implementations of [`DynEq`] and [`DynHash`].

See [`advanced_udwf.rs`] for a full example with complete implementation and
[`WindowUDF`] for other available options.


[`advanced_udwf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/udf/advanced_udwf.rs
# Basic Example
```
# use std::any::Any;
# use std::sync::LazyLock;
# use arrow::datatypes::{DataType, Field, FieldRef};
# use datafusion_common::{DataFusionError, plan_err, Result};
# use datafusion_expr::{col, Signature, Volatility, PartitionEvaluator, WindowFrame, ExprFunctionExt, Documentation, LimitEffect};
# use datafusion_expr::{WindowUDFImpl, WindowUDF};
# use datafusion_functions_window_common::field::WindowUDFFieldArgs;
# use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
# use datafusion_expr::window_doc_sections::DOC_SECTION_ANALYTICAL;
# use datafusion_physical_expr_common::physical_expr;
# use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SmoothIt {
  signature: Signature,
}

impl SmoothIt {
  fn new() -> Self {
    Self {
      signature: Signature::uniform(1, vec![DataType::Int32], Volatility::Immutable),
     }
  }
}

static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(DOC_SECTION_ANALYTICAL, "smooths the windows", "smooth_it(2)")
        .with_argument("arg1", "The int32 number to smooth by")
        .build()
});

fn get_doc() -> &'static Documentation {
    &DOCUMENTATION
}

/// Implement the WindowUDFImpl trait for SmoothIt
impl WindowUDFImpl for SmoothIt {
   fn as_any(&self) -> &dyn Any { self }
   fn name(&self) -> &str { "smooth_it" }
   fn signature(&self) -> &Signature { &self.signature }
   // The actual implementation would smooth the window
   fn partition_evaluator(
       &self,
       _partition_evaluator_args: PartitionEvaluatorArgs,
   ) -> Result<Box<dyn PartitionEvaluator>> {
       unimplemented!()
   }
   fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
     if let Some(DataType::Int32) = field_args.get_input_field(0).map(|f| f.data_type().clone()) {
       Ok(Field::new(field_args.name(), DataType::Int32, false).into())
     } else {
       plan_err!("smooth_it only accepts Int32 arguments")
     }
   }
   fn documentation(&self) -> Option<&Documentation> {
     Some(get_doc())
   }
    fn limit_effect(&self, _args: &[Arc<dyn physical_expr::PhysicalExpr>]) -> LimitEffect {
        LimitEffect::Unknown
    }
}

// Create a new WindowUDF from the implementation
let smooth_it = WindowUDF::from(SmoothIt::new());

// Call the function `add_one(col)`
// smooth_it(speed) OVER (PARTITION BY car ORDER BY time ASC)
let expr = smooth_it.call(vec![col("speed")])
    .partition_by(vec![col("car")])
    .order_by(vec![col("time").sort(true, true)])
    .window_frame(WindowFrame::new(None))
    .build()
    .unwrap();
```

**Methods:**

- `as_any`: Returns this object as an [`Any`] trait object
- `name`: Returns this function's name
- `aliases`: Returns any aliases (alternate names) for this function.
- `signature`: Returns the function's [`Signature`] for information about what input
- `expressions`: Returns the expressions that are passed to the [`PartitionEvaluator`].
- `partition_evaluator`: Invoke the function, returning the [`PartitionEvaluator`] instance
- `simplify`: Optionally apply per-UDWF simplification / rewrite rules.
- `field`: The [`FieldRef`] of the final result of evaluating this window function.
- `sort_options`: Allows the window UDF to define a custom result ordering.
- `coerce_types`: Coerce arguments of a function call to types that the function can evaluate.
- `reverse_expr`: Allows customizing the behavior of the user-defined window
- `documentation`: Returns the documentation for this Window UDF.
- `limit_effect`: If not causal, returns the effect this function will have on the window



