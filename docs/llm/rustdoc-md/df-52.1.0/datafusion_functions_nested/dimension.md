**datafusion_functions_nested > dimension**

# Module: dimension

## Contents

**Structs**

- [`ArrayDims`](#arraydims)

**Functions**

- [`array_dims`](#array_dims) - returns an array of the array's dimensions.
- [`array_dims_udf`](#array_dims_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`array_ndims`](#array_ndims) - returns the number of dimensions of the array.
- [`array_ndims_udf`](#array_ndims_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::dimension::ArrayDims

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayDims) -> bool`
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



## datafusion_functions_nested::dimension::array_dims

*Function*

returns an array of the array's dimensions.

```rust
fn array_dims(array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::dimension::array_dims_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayDims

```rust
fn array_dims_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::dimension::array_ndims

*Function*

returns the number of dimensions of the array.

```rust
fn array_ndims(array: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::dimension::array_ndims_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayNdims

```rust
fn array_ndims_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



