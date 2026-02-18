**datafusion_functions_nested > map**

# Module: map

## Contents

**Structs**

- [`MapFunc`](#mapfunc)

**Functions**

- [`map`](#map) - Returns a map created from a key list and a value list
- [`map_udf`](#map_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::map::MapFunc

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
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **PartialEq**
  - `fn eq(self: &Self, other: &MapFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_nested::map::map

*Function*

Returns a map created from a key list and a value list

```rust
fn map(keys: Vec<datafusion_expr::Expr>, values: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions_nested::map::map_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
MapFunc

```rust
fn map_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



