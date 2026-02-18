**datafusion_expr > udf**

# Module: udf

## Contents

**Structs**

- [`ReturnFieldArgs`](#returnfieldargs) - Information about arguments passed to the function
- [`ScalarFunctionArgs`](#scalarfunctionargs) - Arguments passed to [`ScalarUDFImpl::invoke_with_args`] when invoking a
- [`ScalarUDF`](#scalarudf) - Logical representation of a Scalar User Defined Function.

**Traits**

- [`ScalarUDFImpl`](#scalarudfimpl) - Trait for implementing user defined scalar functions.

---

## datafusion_expr::udf::ReturnFieldArgs

*Struct*

Information about arguments passed to the function

This structure contains metadata about how the function was called
such as the type of the arguments, any scalar arguments and if the
arguments can (ever) be null

See [`ScalarUDFImpl::return_field_from_args`] for more information

**Generic Parameters:**
- 'a

**Fields:**
- `arg_fields: &'a [arrow::datatypes::FieldRef]` - The data types of the arguments to the function
- `scalar_arguments: &'a [Option<&'a datafusion_common::ScalarValue>]` - Is argument `i` to the function a scalar (constant)?

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::udf::ScalarFunctionArgs

*Struct*

Arguments passed to [`ScalarUDFImpl::invoke_with_args`] when invoking a
scalar function.

**Fields:**
- `args: Vec<crate::ColumnarValue>` - The evaluated arguments to the function
- `arg_fields: Vec<arrow::datatypes::FieldRef>` - Field associated with each arg, if it exists
- `number_rows: usize` - The number of rows in record batch being evaluated
- `return_field: arrow::datatypes::FieldRef` - The return field of the scalar function returned (from `return_type`
- `config_options: std::sync::Arc<datafusion_common::config::ConfigOptions>` - The config options at execution time

**Methods:**

- `fn return_type(self: &Self) -> &DataType` - The return type of the function. See [`Self::return_field`] for more

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> ScalarFunctionArgs`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::udf::ScalarUDF

*Struct*

Logical representation of a Scalar User Defined Function.

A scalar function produces a single row output for each row of input. This
struct contains the information DataFusion needs to plan and invoke
functions you supply such as name, type signature, return type, and actual
implementation.

1. For simple use cases, use [`create_udf`] (examples in [`simple_udf.rs`]).

2. For advanced use cases, use [`ScalarUDFImpl`] which provides full API
   access (examples in  [`advanced_udf.rs`]).

See [`Self::call`] to create an `Expr` which invokes a `ScalarUDF` with arguments.

# API Note

This is a separate struct from [`ScalarUDFImpl`] to maintain backwards
compatibility with the older API.

[`create_udf`]: crate::expr_fn::create_udf
[`simple_udf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/udf/simple_udf.rs
[`advanced_udf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/udf/advanced_udf.rs

**Methods:**

- `fn new_from_impl<F>(fun: F) -> ScalarUDF` - Create a new `ScalarUDF` from a `[ScalarUDFImpl]` trait object
- `fn new_from_shared_impl(fun: Arc<dyn ScalarUDFImpl>) -> ScalarUDF` - Create a new `ScalarUDF` from a `[ScalarUDFImpl]` trait object
- `fn inner(self: &Self) -> &Arc<dyn ScalarUDFImpl>` - Return the underlying [`ScalarUDFImpl`] trait object for this function
- `fn with_aliases<impl IntoIterator<Item = &'static str>>(self: Self, aliases: impl Trait) -> Self` - Adds additional names that can be used to invoke this function, in
- `fn call(self: &Self, args: Vec<Expr>) -> Expr` - Returns a [`Expr`] logical expression to call this UDF with specified
- `fn name(self: &Self) -> &str` - Returns this function's name.
- `fn display_name(self: &Self, args: &[Expr]) -> Result<String>` - Returns this function's display_name.
- `fn schema_name(self: &Self, args: &[Expr]) -> Result<String>` - Returns this function's schema_name.
- `fn aliases(self: &Self) -> &[String]` - Returns the aliases for this function.
- `fn signature(self: &Self) -> &Signature` - Returns this function's [`Signature`] (what input types are accepted).
- `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>` - The datatype this function returns given the input argument types.
- `fn return_field_from_args(self: &Self, args: ReturnFieldArgs) -> Result<FieldRef>` - Return the datatype this function returns given the input argument types.
- `fn simplify(self: &Self, args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult>` - Do the function rewrite
- `fn is_nullable(self: &Self, args: &[Expr], schema: &dyn ExprSchema) -> bool`
- `fn invoke_with_args(self: &Self, args: ScalarFunctionArgs) -> Result<ColumnarValue>` - Invoke the function on `args`, returning the appropriate result.
- `fn conditional_arguments<'a>(self: &Self, args: &'a [Expr]) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)>` - Determines which of the arguments passed to this function are evaluated eagerly
- `fn short_circuits(self: &Self) -> bool` - Returns true if some of this `exprs` subexpressions may not be evaluated
- `fn evaluate_bounds(self: &Self, inputs: &[&Interval]) -> Result<Interval>` - Computes the output interval for a [`ScalarUDF`], given the input
- `fn propagate_constraints(self: &Self, interval: &Interval, inputs: &[&Interval]) -> Result<Option<Vec<Interval>>>` - Updates bounds for child expressions, given a known interval for this
- `fn output_ordering(self: &Self, inputs: &[ExprProperties]) -> Result<SortProperties>` - Calculates the [`SortProperties`] of this function based on its
- `fn preserves_lex_ordering(self: &Self, inputs: &[ExprProperties]) -> Result<bool>`
- `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>` - See [`ScalarUDFImpl::coerce_types`] for more details.
- `fn documentation(self: &Self) -> Option<&Documentation>` - Returns the documentation for this Scalar UDF.
- `fn as_async(self: &Self) -> Option<&AsyncScalarUDF>` - Return true if this function is an async function

**Traits:** Eq

**Trait Implementations:**

- **From**
  - `fn from(fun: F) -> Self`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **UDFCoercionExt**
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`
- **Clone**
  - `fn clone(self: &Self) -> ScalarUDF`



## datafusion_expr::udf::ScalarUDFImpl

*Trait*

Trait for implementing user defined scalar functions.

This trait exposes the full API for implementing user defined functions and
can be used to implement any function.

See [`advanced_udf.rs`] for a full example with complete implementation and
[`ScalarUDF`] for other available options.

[`advanced_udf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/udf/advanced_udf.rs

# Basic Example
```
# use std::any::Any;
# use std::sync::LazyLock;
# use arrow::datatypes::DataType;
# use datafusion_common::{DataFusionError, plan_err, Result};
# use datafusion_expr::{col, ColumnarValue, Documentation, ScalarFunctionArgs, Signature, Volatility};
# use datafusion_expr::{ScalarUDFImpl, ScalarUDF};
# use datafusion_expr::scalar_doc_sections::DOC_SECTION_MATH;
/// This struct for a simple UDF that adds one to an int32
#[derive(Debug, PartialEq, Eq, Hash)]
struct AddOne {
  signature: Signature,
}

impl AddOne {
  fn new() -> Self {
    Self {
      signature: Signature::uniform(1, vec![DataType::Int32], Volatility::Immutable),
     }
  }
}

static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
        Documentation::builder(DOC_SECTION_MATH, "Add one to an int32", "add_one(2)")
            .with_argument("arg1", "The int32 number to add one to")
            .build()
    });

fn get_doc() -> &'static Documentation {
    &DOCUMENTATION
}

/// Implement the ScalarUDFImpl trait for AddOne
impl ScalarUDFImpl for AddOne {
   fn as_any(&self) -> &dyn Any { self }
   fn name(&self) -> &str { "add_one" }
   fn signature(&self) -> &Signature { &self.signature }
   fn return_type(&self, args: &[DataType]) -> Result<DataType> {
     if !matches!(args.get(0), Some(&DataType::Int32)) {
       return plan_err!("add_one only accepts Int32 arguments");
     }
     Ok(DataType::Int32)
   }
   // The actual implementation would add one to the argument
   fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        unimplemented!()
   }
   fn documentation(&self) -> Option<&Documentation> {
        Some(get_doc())
    }
}

// Create a new ScalarUDF from the implementation
let add_one = ScalarUDF::from(AddOne::new());

// Call the function `add_one(col)`
let expr = add_one.call(vec![col("a")]);
```

**Methods:**

- `as_any`: Returns this object as an [`Any`] trait object
- `name`: Returns this function's name
- `aliases`: Returns any aliases (alternate names) for this function.
- `display_name`: Returns the user-defined display name of function, given the arguments
- `schema_name`: Returns the name of the column this expression would create
- `signature`: Returns a [`Signature`] describing the argument types for which this
- `return_type`: [`DataType`] returned by this function, given the types of the
- `with_updated_config`: Create a new instance of this function with updated configuration.
- `return_field_from_args`: What type will be returned by this function, given the arguments?
- `is_nullable`
- `invoke_with_args`: Invoke the function returning the appropriate result.
- `simplify`: Optionally apply per-UDF simplification / rewrite rules.
- `short_circuits`: Returns true if some of this `exprs` subexpressions may not be evaluated
- `conditional_arguments`: Determines which of the arguments passed to this function are evaluated eagerly
- `evaluate_bounds`: Computes the output [`Interval`] for a [`ScalarUDFImpl`], given the input
- `propagate_constraints`: Updates bounds for child expressions, given a known [`Interval`]s for this
- `output_ordering`: Calculates the [`SortProperties`] of this function based on its children's properties.
- `preserves_lex_ordering`: Returns true if the function preserves lexicographical ordering based on
- `coerce_types`: Coerce arguments of a function call to types that the function can evaluate.
- `documentation`: Returns the documentation for this Scalar UDF.



