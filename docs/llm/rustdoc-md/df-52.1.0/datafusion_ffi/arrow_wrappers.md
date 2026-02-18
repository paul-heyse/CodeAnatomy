**datafusion_ffi > arrow_wrappers**

# Module: arrow_wrappers

## Contents

**Structs**

- [`WrappedArray`](#wrappedarray) - This is a wrapper struct for FFI_ArrowArray to indicate to StableAbi
- [`WrappedSchema`](#wrappedschema) - This is a wrapper struct around FFI_ArrowSchema simply to indicate

---

## datafusion_ffi::arrow_wrappers::WrappedArray

*Struct*

This is a wrapper struct for FFI_ArrowArray to indicate to StableAbi
that the struct is FFI Safe. For convenience, we also include the
schema needed to create a record batch from the array.

**Fields:**
- `array: arrow::ffi::FFI_ArrowArray`
- `schema: WrappedSchema`

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **TryFrom**
  - `fn try_from(array: &ArrayRef) -> Result<Self, <Self as >::Error>`
- **TryFrom**
  - `fn try_from(value: &ScalarValue) -> Result<Self, <Self as >::Error>`



## datafusion_ffi::arrow_wrappers::WrappedSchema

*Struct*

This is a wrapper struct around FFI_ArrowSchema simply to indicate
to the StableAbi macros that the underlying struct is FFI safe.

**Tuple Struct**: `(arrow::ffi::FFI_ArrowSchema)`

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **From**
  - `fn from(value: SchemaRef) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



