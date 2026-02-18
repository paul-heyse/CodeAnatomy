**datafusion_functions_nested > repeat**

# Module: repeat

## Contents

**Structs**

- [`ArrayRepeat`](#arrayrepeat)

**Functions**

- [`array_repeat`](#array_repeat) - returns an array containing element `count` times.
- [`array_repeat_udf`](#array_repeat_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::repeat::ArrayRepeat

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayRepeat) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`



## datafusion_functions_nested::repeat::array_repeat

*Function*

returns an array containing element `count` times.

```rust
fn array_repeat(element: datafusion_expr::Expr, count: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::repeat::array_repeat_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayRepeat

```rust
fn array_repeat_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



