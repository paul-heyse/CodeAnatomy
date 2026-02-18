**datafusion_functions_nested > length**

# Module: length

## Contents

**Structs**

- [`ArrayLength`](#arraylength)

**Functions**

- [`array_length`](#array_length) - returns the length of the array dimension.
- [`array_length_udf`](#array_length_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::length::ArrayLength

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayLength) -> bool`



## datafusion_functions_nested::length::array_length

*Function*

returns the length of the array dimension.

```rust
fn array_length(array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::length::array_length_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayLength

```rust
fn array_length_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



