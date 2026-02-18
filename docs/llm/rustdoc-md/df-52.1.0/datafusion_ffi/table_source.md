**datafusion_ffi > table_source**

# Module: table_source

## Contents

**Enums**

- [`FFI_TableProviderFilterPushDown`](#ffi_tableproviderfilterpushdown) - FFI safe version of [`TableProviderFilterPushDown`].
- [`FFI_TableType`](#ffi_tabletype) - FFI safe version of [`TableType`].

---

## datafusion_ffi::table_source::FFI_TableProviderFilterPushDown

*Enum*

FFI safe version of [`TableProviderFilterPushDown`].

**Variants:**
- `Unsupported`
- `Inexact`
- `Exact`

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **From**
  - `fn from(value: &TableProviderFilterPushDown) -> Self`



## datafusion_ffi::table_source::FFI_TableType

*Enum*

FFI safe version of [`TableType`].

**Variants:**
- `Base`
- `View`
- `Temporary`

**Traits:** Copy, Eq, GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &FFI_TableType) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(value: TableType) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> FFI_TableType`



