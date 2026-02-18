**datafusion_functions_nested > string**

# Module: string

## Contents

**Structs**

- [`ArrayToString`](#arraytostring)

**Functions**

- [`array_to_string`](#array_to_string) - converts each element to its text representation.
- [`array_to_string_udf`](#array_to_string_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`string_to_array`](#string_to_array) - splits a `string` based on a `delimiter` and returns an array of parts. Any parts matching the optional `null_string` will be replaced with `NULL`
- [`string_to_array_udf`](#string_to_array_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::string::ArrayToString

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayToString) -> bool`
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



## datafusion_functions_nested::string::array_to_string

*Function*

converts each element to its text representation.

```rust
fn array_to_string(array: datafusion_expr::Expr, delimiter: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::string::array_to_string_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
ArrayToString

```rust
fn array_to_string_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::string::string_to_array

*Function*

splits a `string` based on a `delimiter` and returns an array of parts. Any parts matching the optional `null_string` will be replaced with `NULL`

```rust
fn string_to_array(string: datafusion_expr::Expr, delimiter: datafusion_expr::Expr, null_string: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::string::string_to_array_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
StringToArray

```rust
fn string_to_array_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



