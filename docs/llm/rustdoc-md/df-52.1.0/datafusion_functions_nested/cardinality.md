**datafusion_functions_nested > cardinality**

# Module: cardinality

## Contents

**Structs**

- [`Cardinality`](#cardinality)

**Functions**

- [`cardinality`](#cardinality) - returns the total number of elements in the array or map.
- [`cardinality_udf`](#cardinality_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::cardinality::Cardinality

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
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Cardinality) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_nested::cardinality::cardinality

*Function*

returns the total number of elements in the array or map.

```rust
fn cardinality(array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::cardinality::cardinality_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
Cardinality

```rust
fn cardinality_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



