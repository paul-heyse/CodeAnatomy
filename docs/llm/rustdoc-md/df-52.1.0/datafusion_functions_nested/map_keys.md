**datafusion_functions_nested > map_keys**

# Module: map_keys

## Contents

**Structs**

- [`MapKeysFunc`](#mapkeysfunc)

**Functions**

- [`map_keys`](#map_keys) - Return a list of all keys in the map.
- [`map_keys_udf`](#map_keys_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::map_keys::MapKeysFunc

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
  - `fn eq(self: &Self, other: &MapKeysFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_nested::map_keys::map_keys

*Function*

Return a list of all keys in the map.

```rust
fn map_keys(map: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::map_keys::map_keys_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
MapKeysFunc

```rust
fn map_keys_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



