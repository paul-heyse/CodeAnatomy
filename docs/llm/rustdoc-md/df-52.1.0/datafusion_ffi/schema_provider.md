**datafusion_ffi > schema_provider**

# Module: schema_provider

## Contents

**Structs**

- [`FFI_SchemaProvider`](#ffi_schemaprovider) - A stable struct for sharing [`SchemaProvider`] across FFI boundaries.
- [`ForeignSchemaProvider`](#foreignschemaprovider) - This wrapper struct exists on the receiver side of the FFI interface, so it has

---

## datafusion_ffi::schema_provider::FFI_SchemaProvider

*Struct*

A stable struct for sharing [`SchemaProvider`] across FFI boundaries.

**Fields:**
- `owner_name: abi_stable::std_types::ROption<abi_stable::std_types::RString>`
- `table_names: fn(...)`
- `table: fn(...)`
- `register_table: fn(...)`
- `deregister_table: fn(...)`
- `table_exist: fn(...)`
- `logical_codec: crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec`
- `clone: fn(...)` - Used to create a clone on the provider of the execution plan. This should
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `version: fn(...)` - Return the major DataFusion version number of this provider.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the plan.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through

**Methods:**

- `fn new<impl Into<FFI_TaskContextProvider>>(provider: Arc<dyn SchemaProvider>, runtime: Option<Handle>, task_ctx_provider: impl Trait, logical_codec: Option<Arc<dyn LogicalExtensionCodec>>) -> Self` - Creates a new [`FFI_SchemaProvider`].
- `fn new_with_ffi_codec(provider: Arc<dyn SchemaProvider>, runtime: Option<Handle>, logical_codec: FFI_LogicalExtensionCodec) -> Self`

**Traits:** Sync, Send, GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Self`
- **Drop**
  - `fn drop(self: & mut Self)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_ffi::schema_provider::ForeignSchemaProvider

*Struct*

This wrapper struct exists on the receiver side of the FFI interface, so it has
no guarantees about being able to access the data in `private_data`. Any functions
defined on this struct must only use the stable functions provided in
FFI_SchemaProvider to interact with the foreign table provider.

**Tuple Struct**: `(FFI_SchemaProvider)`

**Traits:** Sync, Send

**Trait Implementations:**

- **SchemaProvider**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn owner_name(self: &Self) -> Option<&str>`
  - `fn table_names(self: &Self) -> Vec<String>`
  - `fn table(self: &'life0 Self, name: &'life1 str) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn register_table(self: &Self, name: String, table: Arc<dyn TableProvider>) -> Result<Option<Arc<dyn TableProvider>>>`
  - `fn deregister_table(self: &Self, name: &str) -> Result<Option<Arc<dyn TableProvider>>>`
  - `fn table_exist(self: &Self, name: &str) -> bool` - Returns true if table exist in the schema provider, false otherwise.
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



