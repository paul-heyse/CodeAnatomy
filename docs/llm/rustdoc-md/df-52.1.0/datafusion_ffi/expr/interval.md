**datafusion_ffi > expr > interval**

# Module: expr::interval

## Contents

**Structs**

- [`FFI_Interval`](#ffi_interval) - A stable struct for sharing [`Interval`] across FFI boundaries.

---

## datafusion_ffi::expr::interval::FFI_Interval

*Struct*

A stable struct for sharing [`Interval`] across FFI boundaries.
See [`Interval`] for the meaning of each field. Scalar values
are passed as Arrow arrays of length 1.

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **TryFrom**
  - `fn try_from(value: &Interval) -> Result<Self, <Self as >::Error>`
- **TryFrom**
  - `fn try_from(value: Interval) -> Result<Self, <Self as >::Error>`



