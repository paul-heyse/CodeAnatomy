**datafusion_functions_nested > resize**

# Module: resize

## Contents

**Structs**

- [`ArrayResize`](#arrayresize)

**Functions**

- [`array_resize`](#array_resize) - returns an array with the specified size filled with the given value.
- [`array_resize_udf`](#array_resize_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::resize::ArrayResize

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayResize) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_nested::resize::array_resize

*Function*

returns an array with the specified size filled with the given value.

```rust
fn array_resize(array: datafusion_expr::Expr, size: datafusion_expr::Expr, value: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::resize::array_resize_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayResize

```rust
fn array_resize_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



