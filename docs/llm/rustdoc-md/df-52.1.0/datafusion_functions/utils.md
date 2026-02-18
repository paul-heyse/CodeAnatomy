**datafusion_functions > utils**

# Module: utils

## Contents

**Functions**

- [`calculate_binary_decimal_math`](#calculate_binary_decimal_math) - Computes a binary math function for input arrays using a specified function
- [`calculate_binary_math`](#calculate_binary_math) - Computes a binary math function for input arrays using a specified function.
- [`decimal128_to_i128`](#decimal128_to_i128) - Converts Decimal128 components (value and scale) to an unscaled i128
- [`decimal32_to_i32`](#decimal32_to_i32)
- [`decimal64_to_i64`](#decimal64_to_i64)
- [`make_scalar_function`](#make_scalar_function) - Creates a scalar function implementation for the given function.

---

## datafusion_functions::utils::calculate_binary_decimal_math

*Function*

Computes a binary math function for input arrays using a specified function
and apply rescaling to given precision and scale.
Generic types:
- `L`: Left array decimal type
- `R`: Right array primitive type
- `O`: Output array decimal type
- `F`: Functor computing `fun(l: L, r: R) -> Result<OutputType>`

```rust
fn calculate_binary_decimal_math<L, R, O, F>(left: &dyn Array, right: &datafusion_expr::ColumnarValue, fun: F, precision: u8, scale: i8) -> datafusion_common::Result<std::sync::Arc<arrow::array::PrimitiveArray<O>>>
```



## datafusion_functions::utils::calculate_binary_math

*Function*

Computes a binary math function for input arrays using a specified function.
Generic types:
- `L`: Left array primitive type
- `R`: Right array primitive type
- `O`: Output array primitive type
- `F`: Functor computing `fun(l: L, r: R) -> Result<OutputType>`

```rust
fn calculate_binary_math<L, R, O, F>(left: &dyn Array, right: &datafusion_expr::ColumnarValue, fun: F) -> datafusion_common::Result<std::sync::Arc<arrow::array::PrimitiveArray<O>>>
```



## datafusion_functions::utils::decimal128_to_i128

*Function*

Converts Decimal128 components (value and scale) to an unscaled i128

```rust
fn decimal128_to_i128(value: i128, scale: i8) -> datafusion_common::Result<i128, arrow::error::ArrowError>
```



## datafusion_functions::utils::decimal32_to_i32

*Function*

```rust
fn decimal32_to_i32(value: i32, scale: i8) -> datafusion_common::Result<i32, arrow::error::ArrowError>
```



## datafusion_functions::utils::decimal64_to_i64

*Function*

```rust
fn decimal64_to_i64(value: i64, scale: i8) -> datafusion_common::Result<i64, arrow::error::ArrowError>
```



## datafusion_functions::utils::make_scalar_function

*Function*

Creates a scalar function implementation for the given function.
* `inner` - the function to be executed
* `hints` - hints to be used when expanding scalars to arrays

```rust
fn make_scalar_function<F>(inner: F, hints: Vec<datafusion_expr::function::Hint>) -> impl Trait
```



