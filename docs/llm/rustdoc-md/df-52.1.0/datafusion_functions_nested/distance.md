**datafusion_functions_nested > distance**

# Module: distance

## Contents

**Structs**

- [`ArrayDistance`](#arraydistance)

**Functions**

- [`array_distance`](#array_distance) - returns the Euclidean distance between two numeric arrays.
- [`array_distance_udf`](#array_distance_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::distance::ArrayDistance

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
  - `fn eq(self: &Self, other: &ArrayDistance) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn documentation(self: &Self) -> Option<&Documentation>`



## datafusion_functions_nested::distance::array_distance

*Function*

returns the Euclidean distance between two numeric arrays.

```rust
fn array_distance(array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::distance::array_distance_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayDistance

```rust
fn array_distance_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



