**datafusion_ffi > expr > columnar_value**

# Module: expr::columnar_value

## Contents

**Enums**

- [`FFI_ColumnarValue`](#ffi_columnarvalue) - A stable struct for sharing [`ColumnarValue`] across FFI boundaries.

---

## datafusion_ffi::expr::columnar_value::FFI_ColumnarValue

*Enum*

A stable struct for sharing [`ColumnarValue`] across FFI boundaries.
Scalar values are passed as an Arrow array of length 1.

**Variants:**
- `Array(crate::arrow_wrappers::WrappedArray)`
- `Scalar(crate::arrow_wrappers::WrappedArray)`

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **TryFrom**
  - `fn try_from(value: ColumnarValue) -> Result<Self, <Self as >::Error>`



