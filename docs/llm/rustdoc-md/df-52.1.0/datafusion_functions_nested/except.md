**datafusion_functions_nested > except**

# Module: except

## Contents

**Structs**

- [`ArrayExcept`](#arrayexcept)

**Functions**

- [`array_except`](#array_except) - returns an array of the elements that appear in the first array but not in the second.
- [`array_except_udf`](#array_except_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::except::ArrayExcept

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
  - `fn eq(self: &Self, other: &ArrayExcept) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_nested::except::array_except

*Function*

returns an array of the elements that appear in the first array but not in the second.

```rust
fn array_except(first_array: datafusion_expr::Expr, second_array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::except::array_except_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayExcept

```rust
fn array_except_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



