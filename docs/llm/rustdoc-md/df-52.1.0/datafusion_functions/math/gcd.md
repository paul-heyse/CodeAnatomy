**datafusion_functions > math > gcd**

# Module: math::gcd

## Contents

**Structs**

- [`GcdFunc`](#gcdfunc)

**Functions**

- [`compute_gcd`](#compute_gcd) - Computes greatest common divisor using Binary GCD algorithm.

---

## datafusion_functions::math::gcd::GcdFunc

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
  - `fn eq(self: &Self, other: &GcdFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`



## datafusion_functions::math::gcd::compute_gcd

*Function*

Computes greatest common divisor using Binary GCD algorithm.

```rust
fn compute_gcd(x: i64, y: i64) -> datafusion_common::Result<i64, arrow::error::ArrowError>
```



