**datafusion_ffi > expr > expr_properties**

# Module: expr::expr_properties

## Contents

**Structs**

- [`FFI_ExprProperties`](#ffi_exprproperties) - A stable struct for sharing [`ExprProperties`] across FFI boundaries.
- [`FFI_SortOptions`](#ffi_sortoptions)

**Enums**

- [`FFI_SortProperties`](#ffi_sortproperties)

---

## datafusion_ffi::expr::expr_properties::FFI_ExprProperties

*Struct*

A stable struct for sharing [`ExprProperties`] across FFI boundaries.
See [`ExprProperties`] for the meaning of each field.

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **TryFrom**
  - `fn try_from(value: &ExprProperties) -> Result<Self, <Self as >::Error>`



## datafusion_ffi::expr::expr_properties::FFI_SortOptions

*Struct*

**Fields:**
- `descending: bool`
- `nulls_first: bool`

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **From**
  - `fn from(value: &SortOptions) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_ffi::expr::expr_properties::FFI_SortProperties

*Enum*

**Variants:**
- `Ordered(FFI_SortOptions)`
- `Unordered`
- `Singleton`

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(value: &SortProperties) -> Self`



