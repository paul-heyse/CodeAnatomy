**datafusion_ffi > udf > return_type_args**

# Module: udf::return_type_args

## Contents

**Structs**

- [`FFI_ReturnFieldArgs`](#ffi_returnfieldargs) - A stable struct for sharing a [`ReturnFieldArgs`] across FFI boundaries.
- [`ForeignReturnFieldArgs`](#foreignreturnfieldargs)
- [`ForeignReturnFieldArgsOwned`](#foreignreturnfieldargsowned)

---

## datafusion_ffi::udf::return_type_args::FFI_ReturnFieldArgs

*Struct*

A stable struct for sharing a [`ReturnFieldArgs`] across FFI boundaries.

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **TryFrom**
  - `fn try_from(value: ReturnFieldArgs) -> Result<Self, <Self as >::Error>`



## datafusion_ffi::udf::return_type_args::ForeignReturnFieldArgs

*Struct*

**Generic Parameters:**
- 'a

**Trait Implementations:**

- **From**
  - `fn from(value: &'a ForeignReturnFieldArgsOwned) -> Self`



## datafusion_ffi::udf::return_type_args::ForeignReturnFieldArgsOwned

*Struct*

**Trait Implementations:**

- **TryFrom**
  - `fn try_from(value: &FFI_ReturnFieldArgs) -> Result<Self, <Self as >::Error>`



