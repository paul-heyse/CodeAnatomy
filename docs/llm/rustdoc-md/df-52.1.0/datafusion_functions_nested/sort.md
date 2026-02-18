**datafusion_functions_nested > sort**

# Module: sort

## Contents

**Structs**

- [`ArraySort`](#arraysort) - Implementation of `array_sort` function

**Functions**

- [`array_sort`](#array_sort) - returns sorted array.
- [`array_sort_udf`](#array_sort_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::sort::ArraySort

*Struct*

Implementation of `array_sort` function

`array_sort` sorts the elements of an array

# Example

`array_sort([3, 1, 2])` returns `[1, 2, 3]`

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
  - `fn eq(self: &Self, other: &ArraySort) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_nested::sort::array_sort

*Function*

returns sorted array.

```rust
fn array_sort(array: datafusion_expr::Expr, desc: datafusion_expr::Expr, null_first: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::sort::array_sort_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArraySort

```rust
fn array_sort_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



