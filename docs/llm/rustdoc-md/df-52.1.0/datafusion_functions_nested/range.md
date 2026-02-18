**datafusion_functions_nested > range**

# Module: range

## Contents

**Structs**

- [`Range`](#range)

**Functions**

- [`gen_series`](#gen_series) - create a list of values in the range between start and stop, include upper bound
- [`gen_series_udf`](#gen_series_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
- [`range`](#range) - create a list of values in the range between start and stop
- [`range_udf`](#range_udf) - ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 

---

## datafusion_functions_nested::range::Range

*Struct*

**Methods:**

- `fn new() -> Self` - Generate `range()` function which excludes upper bound.

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
  - `fn eq(self: &Self, other: &Range) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_nested::range::gen_series

*Function*

create a list of values in the range between start and stop, include upper bound

```rust
fn gen_series(start: datafusion_expr::Expr, stop: datafusion_expr::Expr, step: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::range::gen_series_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
GenSeries

```rust
fn gen_series_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions_nested::range::range

*Function*

create a list of values in the range between start and stop

```rust
fn range(start: datafusion_expr::Expr, stop: datafusion_expr::Expr, step: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_nested::range::range_udf

*Function*

ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for 
Range

```rust
fn range_udf() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



