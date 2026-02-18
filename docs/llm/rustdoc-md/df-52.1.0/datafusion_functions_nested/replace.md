**datafusion_functions_nested > replace**

# Module: replace

## Contents

**Structs**

- [`ArrayReplace`](#arrayreplace)

**Functions**

- [`array_replace`](#array_replace) - replaces the first occurrence of the specified element with another specified element.
- [`array_replace_all`](#array_replace_all) - replaces all occurrences of the specified element with another specified element.
- [`array_replace_all_udf`](#array_replace_all_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`array_replace_n`](#array_replace_n) - replaces the first `max` occurrences of the specified element with another specified element.
- [`array_replace_n_udf`](#array_replace_n_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`array_replace_udf`](#array_replace_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::replace::ArrayReplace

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, args: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayReplace) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_nested::replace::array_replace

*Function*

replaces the first occurrence of the specified element with another specified element.

```rust
fn array_replace(array: datafusion_expr::Expr, from: datafusion_expr::Expr, to: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::replace::array_replace_all

*Function*

replaces all occurrences of the specified element with another specified element.

```rust
fn array_replace_all(array: datafusion_expr::Expr, from: datafusion_expr::Expr, to: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::replace::array_replace_all_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayReplaceAll

```rust
fn array_replace_all_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::replace::array_replace_n

*Function*

replaces the first `max` occurrences of the specified element with another specified element.

```rust
fn array_replace_n(array: datafusion_expr::Expr, from: datafusion_expr::Expr, to: datafusion_expr::Expr, max: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::replace::array_replace_n_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayReplaceN

```rust
fn array_replace_n_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::replace::array_replace_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayReplace

```rust
fn array_replace_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



