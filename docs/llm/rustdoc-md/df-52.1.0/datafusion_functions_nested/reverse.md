**datafusion_functions_nested > reverse**

# Module: reverse

## Contents

**Structs**

- [`ArrayReverse`](#arrayreverse)

**Functions**

- [`array_reverse`](#array_reverse) - reverses the order of elements in the array.
- [`array_reverse_inner`](#array_reverse_inner) - array_reverse SQL function
- [`array_reverse_udf`](#array_reverse_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::reverse::ArrayReverse

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayReverse) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
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



## datafusion_functions_nested::reverse::array_reverse

*Function*

reverses the order of elements in the array.

```rust
fn array_reverse(array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::reverse::array_reverse_inner

*Function*

array_reverse SQL function

```rust
fn array_reverse_inner(arg: &[arrow::array::ArrayRef]) -> datafusion_common::Result<arrow::array::ArrayRef>
```



## datafusion_functions_nested::reverse::array_reverse_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayReverse

```rust
fn array_reverse_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



