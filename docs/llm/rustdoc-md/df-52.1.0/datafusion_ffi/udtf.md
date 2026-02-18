**datafusion_ffi > udtf**

# Module: udtf

## Contents

**Structs**

- [`FFI_TableFunction`](#ffi_tablefunction) - A stable struct for sharing a [`TableFunctionImpl`] across FFI boundaries.
- [`ForeignTableFunction`](#foreigntablefunction) - This struct is used to access an UDTF provided by a foreign
- [`TableFunctionPrivateData`](#tablefunctionprivatedata)

---

## datafusion_ffi::udtf::FFI_TableFunction

*Struct*

A stable struct for sharing a [`TableFunctionImpl`] across FFI boundaries.

**Fields:**
- `call: fn(...)` - Equivalent to the `call` function of the TableFunctionImpl.
- `logical_codec: crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec`
- `clone: fn(...)` - Used to create a clone on the provider of the udtf. This should
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the udtf.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through

**Methods:**

- `fn new<impl Into<FFI_TaskContextProvider>>(udtf: Arc<dyn TableFunctionImpl>, runtime: Option<Handle>, task_ctx_provider: impl Trait, logical_codec: Option<Arc<dyn LogicalExtensionCodec>>) -> Self`
- `fn new_with_ffi_codec(udtf: Arc<dyn TableFunctionImpl>, runtime: Option<Handle>, logical_codec: FFI_LogicalExtensionCodec) -> Self`

**Traits:** GetStaticEquivalent_, StableAbi, Sync, Send

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Drop**
  - `fn drop(self: & mut Self)`
- **Clone**
  - `fn clone(self: &Self) -> Self`



## datafusion_ffi::udtf::ForeignTableFunction

*Struct*

This struct is used to access an UDTF provided by a foreign
library across a FFI boundary.

The ForeignTableFunction is to be used by the caller of the UDTF, so it has
no knowledge or access to the private data. All interaction with the UDTF
must occur through the functions defined in FFI_TableFunction.

**Tuple Struct**: `()`

**Traits:** Send, Sync

**Trait Implementations:**

- **TableFunctionImpl**
  - `fn call(self: &Self, args: &[Expr]) -> Result<Arc<dyn TableProvider>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_ffi::udtf::TableFunctionPrivateData

*Struct*



