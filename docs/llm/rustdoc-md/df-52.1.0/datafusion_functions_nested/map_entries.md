**datafusion_functions_nested > map_entries**

# Module: map_entries

## Contents

**Structs**

- [`MapEntriesFunc`](#mapentriesfunc)

**Functions**

- [`map_entries`](#map_entries) - Return a list of all entries in the map.
- [`map_entries_udf`](#map_entries_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::map_entries::MapEntriesFunc

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
  - `fn eq(self: &Self, other: &MapEntriesFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_nested::map_entries::map_entries

*Function*

Return a list of all entries in the map.

```rust
fn map_entries(map: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::map_entries::map_entries_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
MapEntriesFunc

```rust
fn map_entries_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



