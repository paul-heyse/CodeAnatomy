**datafusion_functions_nested > min_max**

# Module: min_max

## Contents

**Structs**

- [`ArrayMax`](#arraymax)

**Functions**

- [`array_max`](#array_max) - returns the maximum value in the array.
- [`array_max_udf`](#array_max_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`array_min`](#array_min) - returns the minimum value in the array
- [`array_min_udf`](#array_min_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::min_max::ArrayMax

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
  - `fn invoke_with_args(self: &Self, args: ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayMax) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_nested::min_max::array_max

*Function*

returns the maximum value in the array.

```rust
fn array_max(array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::min_max::array_max_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayMax

```rust
fn array_max_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::min_max::array_min

*Function*

returns the minimum value in the array

```rust
fn array_min(array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::min_max::array_min_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayMin

```rust
fn array_min_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



