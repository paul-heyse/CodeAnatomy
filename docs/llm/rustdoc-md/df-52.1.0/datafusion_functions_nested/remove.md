**datafusion_functions_nested > remove**

# Module: remove

## Contents

**Structs**

- [`ArrayRemove`](#arrayremove)

**Functions**

- [`array_remove`](#array_remove) - removes the first element from the array equal to the given value.
- [`array_remove_all`](#array_remove_all) - removes all elements from the array equal to the given value.
- [`array_remove_all_udf`](#array_remove_all_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`array_remove_n`](#array_remove_n) - removes the first `max` elements from the array equal to the given value.
- [`array_remove_n_udf`](#array_remove_n_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`array_remove_udf`](#array_remove_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::remove::ArrayRemove

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn return_field_from_args(self: &Self, args: datafusion_expr::ReturnFieldArgs) -> Result<FieldRef>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayRemove) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_nested::remove::array_remove

*Function*

removes the first element from the array equal to the given value.

```rust
fn array_remove(array: datafusion_expr::Expr, element: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::remove::array_remove_all

*Function*

removes all elements from the array equal to the given value.

```rust
fn array_remove_all(array: datafusion_expr::Expr, element: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::remove::array_remove_all_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayRemoveAll

```rust
fn array_remove_all_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::remove::array_remove_n

*Function*

removes the first `max` elements from the array equal to the given value.

```rust
fn array_remove_n(array: datafusion_expr::Expr, element: datafusion_expr::Expr, max: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::remove::array_remove_n_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayRemoveN

```rust
fn array_remove_n_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::remove::array_remove_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayRemove

```rust
fn array_remove_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



