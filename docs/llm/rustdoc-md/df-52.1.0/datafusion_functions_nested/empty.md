**datafusion_functions_nested > empty**

# Module: empty

## Contents

**Structs**

- [`ArrayEmpty`](#arrayempty)

**Functions**

- [`array_empty`](#array_empty) - returns true for an empty array or false for a non-empty array.
- [`array_empty_udf`](#array_empty_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::empty::ArrayEmpty

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
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayEmpty) -> bool`



## datafusion_functions_nested::empty::array_empty

*Function*

returns true for an empty array or false for a non-empty array.

```rust
fn array_empty(array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::empty::array_empty_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayEmpty

```rust
fn array_empty_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



