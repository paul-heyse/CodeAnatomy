**datafusion_functions_nested > array_has**

# Module: array_has

## Contents

**Structs**

- [`ArrayHas`](#arrayhas)
- [`ArrayHasAll`](#arrayhasall)
- [`ArrayHasAny`](#arrayhasany)

**Functions**

- [`array_has`](#array_has) - returns true, if the element appears in the first array, otherwise false.
- [`array_has_all`](#array_has_all) - returns true if each element of the second array appears in the first array; otherwise, it returns false.
- [`array_has_all_udf`](#array_has_all_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`array_has_any`](#array_has_any) - returns true if at least one element of the second array appears in the first array; otherwise, it returns false.
- [`array_has_any_udf`](#array_has_any_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`array_has_udf`](#array_has_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::array_has::ArrayHas

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
  - `fn return_type(self: &Self, _: &[DataType]) -> Result<DataType>`
  - `fn simplify(self: &Self, args: Vec<Expr>, _info: &dyn datafusion_expr::simplify::SimplifyInfo) -> Result<ExprSimplifyResult>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayHas) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> Self`



## datafusion_functions_nested::array_has::ArrayHasAll

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayHasAll) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_nested::array_has::ArrayHasAny

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayHasAny) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_nested::array_has::array_has

*Function*

returns true, if the element appears in the first array, otherwise false.

```rust
fn array_has(haystack_array: datafusion_expr::Expr, element: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::array_has::array_has_all

*Function*

returns true if each element of the second array appears in the first array; otherwise, it returns false.

```rust
fn array_has_all(haystack_array: datafusion_expr::Expr, needle_array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::array_has::array_has_all_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayHasAll

```rust
fn array_has_all_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::array_has::array_has_any

*Function*

returns true if at least one element of the second array appears in the first array; otherwise, it returns false.

```rust
fn array_has_any(haystack_array: datafusion_expr::Expr, needle_array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::array_has::array_has_any_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayHasAny

```rust
fn array_has_any_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::array_has::array_has_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayHas

```rust
fn array_has_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



