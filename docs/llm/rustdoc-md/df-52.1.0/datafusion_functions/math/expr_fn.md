**datafusion_functions > math > expr_fn**

# Module: math::expr_fn

## Contents

**Functions**

- [`abs`](#abs) - returns the absolute value of a given number
- [`acos`](#acos) - returns the arc cosine or inverse cosine of a number
- [`acosh`](#acosh) - returns inverse hyperbolic cosine
- [`asin`](#asin) - returns the arc sine or inverse sine of a number
- [`asinh`](#asinh) - returns inverse hyperbolic sine
- [`atan`](#atan) - returns inverse tangent
- [`atan2`](#atan2) - returns inverse tangent of a division given in the argument
- [`atanh`](#atanh) - returns inverse hyperbolic tangent
- [`cbrt`](#cbrt) - cube root of a number
- [`ceil`](#ceil) - nearest integer greater than or equal to argument
- [`cos`](#cos) - cosine
- [`cosh`](#cosh) - hyperbolic cosine
- [`cot`](#cot) - cotangent of a number
- [`degrees`](#degrees) - converts radians to degrees
- [`exp`](#exp) - exponential
- [`factorial`](#factorial) - factorial
- [`floor`](#floor) - nearest integer less than or equal to argument
- [`gcd`](#gcd) - greatest common divisor
- [`isnan`](#isnan) - returns true if a given number is +NaN or -NaN otherwise returns false
- [`iszero`](#iszero) - returns true if a given number is +0.0 or -0.0 otherwise returns false
- [`lcm`](#lcm) - least common multiple
- [`ln`](#ln) - natural logarithm (base e) of a number
- [`log`](#log) - logarithm of a number for a particular `base`
- [`log10`](#log10) - base 10 logarithm of a number
- [`log2`](#log2) - base 2 logarithm of a number
- [`nanvl`](#nanvl) - returns x if x is not NaN otherwise returns y
- [`pi`](#pi) - Returns an approximate value of π
- [`power`](#power) - `base` raised to the power of `exponent`
- [`radians`](#radians) - converts degrees to radians
- [`random`](#random) - Returns a random value in the range 0.0 <= x < 1.0
- [`round`](#round) - round to nearest integer
- [`signum`](#signum) - sign of the argument (-1, 0, +1)
- [`sin`](#sin) - sine
- [`sinh`](#sinh) - hyperbolic sine
- [`sqrt`](#sqrt) - square root of a number
- [`tan`](#tan) - returns the tangent of a number
- [`tanh`](#tanh) - returns the hyperbolic tangent of a number
- [`trunc`](#trunc) - truncate toward zero, with optional precision

---

## datafusion_functions::math::expr_fn::abs

*Function*

returns the absolute value of a given number

```rust
fn abs(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::acos

*Function*

returns the arc cosine or inverse cosine of a number

```rust
fn acos(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::acosh

*Function*

returns inverse hyperbolic cosine

```rust
fn acosh(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::asin

*Function*

returns the arc sine or inverse sine of a number

```rust
fn asin(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::asinh

*Function*

returns inverse hyperbolic sine

```rust
fn asinh(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::atan

*Function*

returns inverse tangent

```rust
fn atan(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::atan2

*Function*

returns inverse tangent of a division given in the argument

```rust
fn atan2(y: datafusion_expr::Expr, x: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::atanh

*Function*

returns inverse hyperbolic tangent

```rust
fn atanh(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::cbrt

*Function*

cube root of a number

```rust
fn cbrt(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::ceil

*Function*

nearest integer greater than or equal to argument

```rust
fn ceil(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::cos

*Function*

cosine

```rust
fn cos(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::cosh

*Function*

hyperbolic cosine

```rust
fn cosh(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::cot

*Function*

cotangent of a number

```rust
fn cot(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::degrees

*Function*

converts radians to degrees

```rust
fn degrees(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::exp

*Function*

exponential

```rust
fn exp(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::factorial

*Function*

factorial

```rust
fn factorial(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::floor

*Function*

nearest integer less than or equal to argument

```rust
fn floor(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::gcd

*Function*

greatest common divisor

```rust
fn gcd(x: datafusion_expr::Expr, y: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::isnan

*Function*

returns true if a given number is +NaN or -NaN otherwise returns false

```rust
fn isnan(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::iszero

*Function*

returns true if a given number is +0.0 or -0.0 otherwise returns false

```rust
fn iszero(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::lcm

*Function*

least common multiple

```rust
fn lcm(x: datafusion_expr::Expr, y: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::ln

*Function*

natural logarithm (base e) of a number

```rust
fn ln(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::log

*Function*

logarithm of a number for a particular `base`

```rust
fn log(base: datafusion_expr::Expr, num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::log10

*Function*

base 10 logarithm of a number

```rust
fn log10(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::log2

*Function*

base 2 logarithm of a number

```rust
fn log2(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::nanvl

*Function*

returns x if x is not NaN otherwise returns y

```rust
fn nanvl(x: datafusion_expr::Expr, y: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::pi

*Function*

Returns an approximate value of π

```rust
fn pi() -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::power

*Function*

`base` raised to the power of `exponent`

```rust
fn power(base: datafusion_expr::Expr, exponent: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::radians

*Function*

converts degrees to radians

```rust
fn radians(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::random

*Function*

Returns a random value in the range 0.0 <= x < 1.0

```rust
fn random() -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::round

*Function*

round to nearest integer

```rust
fn round(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::signum

*Function*

sign of the argument (-1, 0, +1)

```rust
fn signum(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::sin

*Function*

sine

```rust
fn sin(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::sinh

*Function*

hyperbolic sine

```rust
fn sinh(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::sqrt

*Function*

square root of a number

```rust
fn sqrt(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::tan

*Function*

returns the tangent of a number

```rust
fn tan(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::tanh

*Function*

returns the hyperbolic tangent of a number

```rust
fn tanh(num: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::math::expr_fn::trunc

*Function*

truncate toward zero, with optional precision

```rust
fn trunc(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



