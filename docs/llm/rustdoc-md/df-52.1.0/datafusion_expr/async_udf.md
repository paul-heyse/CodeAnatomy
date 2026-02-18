**datafusion_expr > async_udf**

# Module: async_udf

## Contents

**Structs**

- [`AsyncScalarUDF`](#asyncscalarudf) - A scalar UDF that must be invoked using async methods

**Traits**

- [`AsyncScalarUDFImpl`](#asyncscalarudfimpl) - A scalar UDF that can invoke using async methods

---

## datafusion_expr::async_udf::AsyncScalarUDF

*Struct*

A scalar UDF that must be invoked using async methods

Note this is not meant to be used directly, but is meant to be an implementation detail
for AsyncUDFImpl.

**Methods:**

- `fn new(inner: Arc<dyn AsyncScalarUDFImpl>) -> Self`
- `fn ideal_batch_size(self: &Self) -> Option<usize>` - The ideal batch size for this function
- `fn into_scalar_udf(self: Self) -> ScalarUDF` - Turn this AsyncUDF into a ScalarUDF, suitable for
- `fn invoke_async_with_args(self: &Self, args: ScalarFunctionArgs) -> Result<ColumnarValue>` - Invoke the function asynchronously with the async arguments

**Traits:** Eq

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn return_field_from_args(self: &Self, args: ReturnFieldArgs) -> Result<FieldRef>`
  - `fn invoke_with_args(self: &Self, _args: ScalarFunctionArgs) -> Result<ColumnarValue>`



## datafusion_expr::async_udf::AsyncScalarUDFImpl

*Trait*

A scalar UDF that can invoke using async methods

Note this is less efficient than the ScalarUDFImpl, but it can be used
to register remote functions in the context.

The name is chosen to mirror ScalarUDFImpl

**Methods:**

- `ideal_batch_size`: The ideal batch size for this function.
- `invoke_async_with_args`: Invoke the function asynchronously with the async arguments



