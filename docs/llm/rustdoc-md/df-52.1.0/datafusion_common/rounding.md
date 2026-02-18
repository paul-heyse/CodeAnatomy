**datafusion_common > rounding**

# Module: rounding

## Contents

**Functions**

- [`alter_fp_rounding_mode`](#alter_fp_rounding_mode)
- [`next_down`](#next_down) - Returns the next representable floating-point value smaller than the input value.
- [`next_up`](#next_up) - Returns the next representable floating-point value greater than the input value.

**Traits**

- [`FloatBits`](#floatbits) - A trait to manipulate floating-point types with bitwise operations.

---

## datafusion_common::rounding::FloatBits

*Trait*

A trait to manipulate floating-point types with bitwise operations.
Provides functions to convert a floating-point value to/from its bitwise
representation as well as utility methods to handle special values.

**Methods:**

- `Item`: The integer type used for bitwise operations.
- `TINY_BITS`: The smallest positive floating-point value representable by this type.
- `NEG_TINY_BITS`: The smallest (in magnitude) negative floating-point value representable by this type.
- `CLEAR_SIGN_MASK`: A mask to clear the sign bit of the floating-point value's bitwise representation.
- `ONE`: The integer value 1, used in bitwise operations.
- `ZERO`: The integer value 0, used in bitwise operations.
- `NEG_ZERO`
- `to_bits`: Converts the floating-point value to its bitwise representation.
- `from_bits`: Converts the bitwise representation to the corresponding floating-point value.
- `float_is_nan`: Returns true if the floating-point value is NaN (not a number).
- `infinity`: Returns the positive infinity value for this floating-point type.
- `neg_infinity`: Returns the negative infinity value for this floating-point type.



## datafusion_common::rounding::alter_fp_rounding_mode

*Function*

```rust
fn alter_fp_rounding_mode<const UPPER, F>(lhs: &crate::ScalarValue, rhs: &crate::ScalarValue, operation: F) -> crate::Result<crate::ScalarValue>
```



## datafusion_common::rounding::next_down

*Function*

Returns the next representable floating-point value smaller than the input value.

This function takes a floating-point value that implements the FloatBits trait,
calculates the next representable value smaller than the input, and returns it.

If the input value is NaN or negative infinity, the function returns the input value.

# Examples

```
use datafusion_common::rounding::next_down;

let f: f32 = 1.0;
let next_f = next_down(f);
assert_eq!(next_f, 0.99999994);
```

```rust
fn next_down<F>(float: F) -> F
```



## datafusion_common::rounding::next_up

*Function*

Returns the next representable floating-point value greater than the input value.

This function takes a floating-point value that implements the FloatBits trait,
calculates the next representable value greater than the input, and returns it.

If the input value is NaN or positive infinity, the function returns the input value.

# Examples

```
use datafusion_common::rounding::next_up;

let f: f32 = 1.0;
let next_f = next_up(f);
assert_eq!(next_f, 1.0000001);
```

```rust
fn next_up<F>(float: F) -> F
```



