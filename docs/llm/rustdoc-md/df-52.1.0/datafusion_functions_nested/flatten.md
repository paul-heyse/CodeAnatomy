**datafusion_functions_nested > flatten**

# Module: flatten

## Contents

**Structs**

- [`Flatten`](#flatten)

**Functions**

- [`flatten`](#flatten) - flattens an array of arrays into a single array.
- [`flatten_udf`](#flatten_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::flatten::Flatten

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Flatten) -> bool`
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
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_nested::flatten::flatten

*Function*

flattens an array of arrays into a single array.

```rust
fn flatten(array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::flatten::flatten_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
Flatten

```rust
fn flatten_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



