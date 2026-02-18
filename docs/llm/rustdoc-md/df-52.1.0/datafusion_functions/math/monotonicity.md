**datafusion_functions > math > monotonicity**

# Module: math::monotonicity

## Contents

**Functions**

- [`acos_order`](#acos_order) - Non-increasing on the interval \[−1, 1\], undefined otherwise.
- [`acosh_order`](#acosh_order) - Non-decreasing for x ≥ 1, undefined otherwise.
- [`asin_order`](#asin_order) - Non-decreasing on the interval \[−1, 1\], undefined otherwise.
- [`asinh_order`](#asinh_order) - Non-decreasing for all real numbers.
- [`atan2_order`](#atan2_order) - Order depends on the quadrant.
- [`atan_order`](#atan_order) - Non-decreasing for all real numbers.
- [`atanh_order`](#atanh_order) - Non-decreasing on the interval \[−1, 1\], undefined otherwise.
- [`cbrt_order`](#cbrt_order) - Non-decreasing for all real numbers.
- [`ceil_order`](#ceil_order) - Non-decreasing for all real numbers.
- [`cos_order`](#cos_order) - Non-increasing on \[0, π\] and then non-decreasing on \[π, 2π\].
- [`cosh_order`](#cosh_order) - Non-decreasing for x ≥ 0 and symmetrically non-increasing for x ≤ 0.
- [`degrees_order`](#degrees_order) - Non-decreasing function that converts radians to degrees.
- [`exp_order`](#exp_order) - Non-decreasing for all real numbers.
- [`floor_order`](#floor_order) - Non-decreasing for all real numbers.
- [`get_acos_doc`](#get_acos_doc)
- [`get_acosh_doc`](#get_acosh_doc)
- [`get_asin_doc`](#get_asin_doc)
- [`get_asinh_doc`](#get_asinh_doc)
- [`get_atan2_doc`](#get_atan2_doc)
- [`get_atan_doc`](#get_atan_doc)
- [`get_atanh_doc`](#get_atanh_doc)
- [`get_cbrt_doc`](#get_cbrt_doc)
- [`get_cos_doc`](#get_cos_doc)
- [`get_cosh_doc`](#get_cosh_doc)
- [`get_degrees_doc`](#get_degrees_doc)
- [`get_exp_doc`](#get_exp_doc)
- [`get_ln_doc`](#get_ln_doc)
- [`get_log10_doc`](#get_log10_doc)
- [`get_log2_doc`](#get_log2_doc)
- [`get_radians_doc`](#get_radians_doc)
- [`get_sin_doc`](#get_sin_doc)
- [`get_sinh_doc`](#get_sinh_doc)
- [`get_sqrt_doc`](#get_sqrt_doc)
- [`get_tan_doc`](#get_tan_doc)
- [`get_tanh_doc`](#get_tanh_doc)
- [`ln_order`](#ln_order) - Non-decreasing for x ≥ 0, undefined otherwise.
- [`log10_order`](#log10_order) - Non-decreasing for x ≥ 0, undefined otherwise.
- [`log2_order`](#log2_order) - Non-decreasing for x ≥ 0, undefined otherwise.
- [`radians_order`](#radians_order) - Non-decreasing for all real numbers x.
- [`sin_order`](#sin_order) - Non-decreasing on \[0, π\] and then non-increasing on \[π, 2π\].
- [`sinh_order`](#sinh_order) - Non-decreasing for all real numbers.
- [`sqrt_order`](#sqrt_order) - Non-decreasing for x ≥ 0, undefined otherwise.
- [`tan_order`](#tan_order) - Non-decreasing between vertical asymptotes at x = k * π ± π / 2 for any
- [`tanh_order`](#tanh_order) - Non-decreasing for all real numbers.

---

## datafusion_functions::math::monotonicity::acos_order

*Function*

Non-increasing on the interval \[−1, 1\], undefined otherwise.

```rust
fn acos_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::acosh_order

*Function*

Non-decreasing for x ≥ 1, undefined otherwise.

```rust
fn acosh_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::asin_order

*Function*

Non-decreasing on the interval \[−1, 1\], undefined otherwise.

```rust
fn asin_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::asinh_order

*Function*

Non-decreasing for all real numbers.

```rust
fn asinh_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::atan2_order

*Function*

Order depends on the quadrant.

```rust
fn atan2_order(_input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::atan_order

*Function*

Non-decreasing for all real numbers.

```rust
fn atan_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::atanh_order

*Function*

Non-decreasing on the interval \[−1, 1\], undefined otherwise.

```rust
fn atanh_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::cbrt_order

*Function*

Non-decreasing for all real numbers.

```rust
fn cbrt_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::ceil_order

*Function*

Non-decreasing for all real numbers.

```rust
fn ceil_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::cos_order

*Function*

Non-increasing on \[0, π\] and then non-decreasing on \[π, 2π\].
This pattern repeats periodically with a period of 2π.

```rust
fn cos_order(_input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::cosh_order

*Function*

Non-decreasing for x ≥ 0 and symmetrically non-increasing for x ≤ 0.

```rust
fn cosh_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::degrees_order

*Function*

Non-decreasing function that converts radians to degrees.

```rust
fn degrees_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::exp_order

*Function*

Non-decreasing for all real numbers.

```rust
fn exp_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::floor_order

*Function*

Non-decreasing for all real numbers.

```rust
fn floor_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::get_acos_doc

*Function*

```rust
fn get_acos_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_acosh_doc

*Function*

```rust
fn get_acosh_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_asin_doc

*Function*

```rust
fn get_asin_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_asinh_doc

*Function*

```rust
fn get_asinh_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_atan2_doc

*Function*

```rust
fn get_atan2_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_atan_doc

*Function*

```rust
fn get_atan_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_atanh_doc

*Function*

```rust
fn get_atanh_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_cbrt_doc

*Function*

```rust
fn get_cbrt_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_cos_doc

*Function*

```rust
fn get_cos_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_cosh_doc

*Function*

```rust
fn get_cosh_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_degrees_doc

*Function*

```rust
fn get_degrees_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_exp_doc

*Function*

```rust
fn get_exp_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_ln_doc

*Function*

```rust
fn get_ln_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_log10_doc

*Function*

```rust
fn get_log10_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_log2_doc

*Function*

```rust
fn get_log2_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_radians_doc

*Function*

```rust
fn get_radians_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_sin_doc

*Function*

```rust
fn get_sin_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_sinh_doc

*Function*

```rust
fn get_sinh_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_sqrt_doc

*Function*

```rust
fn get_sqrt_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_tan_doc

*Function*

```rust
fn get_tan_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::get_tanh_doc

*Function*

```rust
fn get_tanh_doc() -> &'static datafusion_expr::Documentation
```



## datafusion_functions::math::monotonicity::ln_order

*Function*

Non-decreasing for x ≥ 0, undefined otherwise.

```rust
fn ln_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::log10_order

*Function*

Non-decreasing for x ≥ 0, undefined otherwise.

```rust
fn log10_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::log2_order

*Function*

Non-decreasing for x ≥ 0, undefined otherwise.

```rust
fn log2_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::radians_order

*Function*

Non-decreasing for all real numbers x.

```rust
fn radians_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::sin_order

*Function*

Non-decreasing on \[0, π\] and then non-increasing on \[π, 2π\].
This pattern repeats periodically with a period of 2π.

```rust
fn sin_order(_input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::sinh_order

*Function*

Non-decreasing for all real numbers.

```rust
fn sinh_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::sqrt_order

*Function*

Non-decreasing for x ≥ 0, undefined otherwise.

```rust
fn sqrt_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::tan_order

*Function*

Non-decreasing between vertical asymptotes at x = k * π ± π / 2 for any
integer k.

```rust
fn tan_order(_input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



## datafusion_functions::math::monotonicity::tanh_order

*Function*

Non-decreasing for all real numbers.

```rust
fn tanh_order(input: &[datafusion_expr::sort_properties::ExprProperties]) -> datafusion_common::Result<datafusion_expr::sort_properties::SortProperties>
```



