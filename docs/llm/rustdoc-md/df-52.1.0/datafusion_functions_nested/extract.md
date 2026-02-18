**datafusion_functions_nested > extract**

# Module: extract

## Contents

**Structs**

- [`ArrayElement`](#arrayelement)

**Functions**

- [`array_any_value`](#array_any_value) - returns the first non-null element in the array.
- [`array_any_value_udf`](#array_any_value_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`array_element`](#array_element) - extracts the element with the index n from the array.
- [`array_element_udf`](#array_element_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`array_pop_back`](#array_pop_back) - returns the array without the last element.
- [`array_pop_back_udf`](#array_pop_back_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`array_pop_front`](#array_pop_front) - returns the array without the first element.
- [`array_pop_front_udf`](#array_pop_front_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`array_slice`](#array_slice) - returns a slice of the array.
- [`array_slice_udf`](#array_slice_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::extract::ArrayElement

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayElement) -> bool`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn display_name(self: &Self, args: &[Expr]) -> Result<String>`
  - `fn schema_name(self: &Self, args: &[Expr]) -> Result<String>`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_nested::extract::array_any_value

*Function*

returns the first non-null element in the array.

```rust
fn array_any_value(array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::extract::array_any_value_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayAnyValue

```rust
fn array_any_value_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::extract::array_element

*Function*

extracts the element with the index n from the array.

```rust
fn array_element(array: datafusion_expr::Expr, element: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::extract::array_element_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayElement

```rust
fn array_element_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::extract::array_pop_back

*Function*

returns the array without the last element.

```rust
fn array_pop_back(array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::extract::array_pop_back_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayPopBack

```rust
fn array_pop_back_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::extract::array_pop_front

*Function*

returns the array without the first element.

```rust
fn array_pop_front(array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::extract::array_pop_front_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayPopFront

```rust
fn array_pop_front_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::extract::array_slice

*Function*

returns a slice of the array.

```rust
fn array_slice(array: datafusion_expr::Expr, begin: datafusion_expr::Expr, end: datafusion_expr::Expr, stride: Option<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions_nested::extract::array_slice_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArraySlice

```rust
fn array_slice_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



