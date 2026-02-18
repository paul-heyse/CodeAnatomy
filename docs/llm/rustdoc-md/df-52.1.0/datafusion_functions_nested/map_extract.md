**datafusion_functions_nested > map_extract**

# Module: map_extract

## Contents

**Structs**

- [`MapExtract`](#mapextract)

**Functions**

- [`map_extract`](#map_extract) - Return a list containing the value for a given key or an empty list if the key is not contained in the map.
- [`map_extract_udf`](#map_extract_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::map_extract::MapExtract

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &MapExtract) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`



## datafusion_functions_nested::map_extract::map_extract

*Function*

Return a list containing the value for a given key or an empty list if the key is not contained in the map.

```rust
fn map_extract(map: datafusion_expr::Expr, key: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::map_extract::map_extract_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
MapExtract

```rust
fn map_extract_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



