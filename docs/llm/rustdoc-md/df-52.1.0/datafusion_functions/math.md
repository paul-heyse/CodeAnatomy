**datafusion_functions > math**

# Module: math

## Contents

**Modules**

- [`abs`](#abs) - math expressions
- [`bounds`](#bounds)
- [`ceil`](#ceil)
- [`cot`](#cot)
- [`expr_fn`](#expr_fn)
- [`factorial`](#factorial)
- [`floor`](#floor)
- [`gcd`](#gcd)
- [`iszero`](#iszero)
- [`lcm`](#lcm)
- [`log`](#log) - Math function: `log()`.
- [`monotonicity`](#monotonicity)
- [`nans`](#nans) - Math function: `isnan()`.
- [`nanvl`](#nanvl)
- [`pi`](#pi)
- [`power`](#power) - Math function: `power()`.
- [`random`](#random)
- [`round`](#round)
- [`signum`](#signum)
- [`trunc`](#trunc)

**Functions**

- [`abs`](#abs) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of abs
- [`acos`](#acos) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of acos
- [`acosh`](#acosh) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of acosh
- [`asin`](#asin) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of asin
- [`asinh`](#asinh) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of asinh
- [`atan`](#atan) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of atan
- [`atan2`](#atan2) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of atan2
- [`atanh`](#atanh) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of atanh
- [`cbrt`](#cbrt) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of cbrt
- [`ceil`](#ceil) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of ceil
- [`cos`](#cos) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of cos
- [`cosh`](#cosh) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of cosh
- [`cot`](#cot) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of cot
- [`degrees`](#degrees) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of degrees
- [`exp`](#exp) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of exp
- [`factorial`](#factorial) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of factorial
- [`floor`](#floor) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of floor
- [`functions`](#functions) - Returns all DataFusion functions defined in this package
- [`gcd`](#gcd) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of gcd
- [`isnan`](#isnan) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of isnan
- [`iszero`](#iszero) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of iszero
- [`lcm`](#lcm) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of lcm
- [`ln`](#ln) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of ln
- [`log`](#log) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of log
- [`log10`](#log10) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of log10
- [`log2`](#log2) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of log2
- [`nanvl`](#nanvl) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of nanvl
- [`pi`](#pi) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of pi
- [`power`](#power) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of power
- [`radians`](#radians) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of radians
- [`random`](#random) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of random
- [`round`](#round) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of round
- [`signum`](#signum) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of signum
- [`sin`](#sin) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of sin
- [`sinh`](#sinh) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of sinh
- [`sqrt`](#sqrt) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of sqrt
- [`tan`](#tan) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of tan
- [`tanh`](#tanh) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of tanh
- [`trunc`](#trunc) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of trunc

---

## Module: abs

math expressions



## datafusion_functions::math::abs

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of abs

```rust
fn abs() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::acos

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of acos

```rust
fn acos() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::acosh

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of acosh

```rust
fn acosh() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::asin

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of asin

```rust
fn asin() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::asinh

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of asinh

```rust
fn asinh() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::atan

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of atan

```rust
fn atan() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::atan2

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of atan2

```rust
fn atan2() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::atanh

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of atanh

```rust
fn atanh() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: bounds



## datafusion_functions::math::cbrt

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of cbrt

```rust
fn cbrt() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: ceil



## datafusion_functions::math::ceil

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of ceil

```rust
fn ceil() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::cos

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of cos

```rust
fn cos() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::cosh

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of cosh

```rust
fn cosh() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: cot



## datafusion_functions::math::cot

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of cot

```rust
fn cot() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::degrees

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of degrees

```rust
fn degrees() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::exp

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of exp

```rust
fn exp() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: expr_fn



## Module: factorial



## datafusion_functions::math::factorial

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of factorial

```rust
fn factorial() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: floor



## datafusion_functions::math::floor

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of floor

```rust
fn floor() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::functions

*Function*

Returns all DataFusion functions defined in this package

```rust
fn functions() -> Vec<std::sync::Arc<datafusion_expr::ScalarUDF>>
```



## Module: gcd



## datafusion_functions::math::gcd

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of gcd

```rust
fn gcd() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::isnan

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of isnan

```rust
fn isnan() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::iszero

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of iszero

```rust
fn iszero() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: iszero



## Module: lcm



## datafusion_functions::math::lcm

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of lcm

```rust
fn lcm() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::ln

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of ln

```rust
fn ln() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::log

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of log

```rust
fn log() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: log

Math function: `log()`.



## datafusion_functions::math::log10

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of log10

```rust
fn log10() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::log2

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of log2

```rust
fn log2() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: monotonicity



## Module: nans

Math function: `isnan()`.



## datafusion_functions::math::nanvl

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of nanvl

```rust
fn nanvl() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: nanvl



## datafusion_functions::math::pi

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of pi

```rust
fn pi() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: pi



## Module: power

Math function: `power()`.



## datafusion_functions::math::power

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of power

```rust
fn power() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::radians

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of radians

```rust
fn radians() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: random



## datafusion_functions::math::random

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of random

```rust
fn random() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: round



## datafusion_functions::math::round

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of round

```rust
fn round() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: signum



## datafusion_functions::math::signum

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of signum

```rust
fn signum() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::sin

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of sin

```rust
fn sin() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::sinh

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of sinh

```rust
fn sinh() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::sqrt

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of sqrt

```rust
fn sqrt() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::tan

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of tan

```rust
fn tan() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::math::tanh

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of tanh

```rust
fn tanh() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: trunc



## datafusion_functions::math::trunc

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of trunc

```rust
fn trunc() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



