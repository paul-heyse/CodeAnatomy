**datafusion_functions_nested > position**

# Module: position

## Contents

**Structs**

- [`ArrayPosition`](#arrayposition)

**Functions**

- [`array_position`](#array_position) - searches for an element in the array, returns first occurrence.
- [`array_position_udf`](#array_position_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`array_positions`](#array_positions) - searches for an element in the array, returns all occurrences.
- [`array_positions_udf`](#array_positions_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::position::ArrayPosition

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

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
  - `fn eq(self: &Self, other: &ArrayPosition) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_nested::position::array_position

*Function*

searches for an element in the array, returns first occurrence.

```rust
fn array_position(array: datafusion_expr::Expr, element: datafusion_expr::Expr, index: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::position::array_position_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayPosition

```rust
fn array_position_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::position::array_positions

*Function*

searches for an element in the array, returns all occurrences.

```rust
fn array_positions(array: datafusion_expr::Expr, element: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::position::array_positions_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayPositions

```rust
fn array_positions_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



