**datafusion_ffi > catalog_provider**

# Module: catalog_provider

## Contents

**Structs**

- [`FFI_CatalogProvider`](#ffi_catalogprovider) - A stable struct for sharing [`CatalogProvider`] across FFI boundaries.
- [`ForeignCatalogProvider`](#foreigncatalogprovider) - This wrapper struct exists on the receiver side of the FFI interface, so it has

---

## datafusion_ffi::catalog_provider::FFI_CatalogProvider

*Struct*

A stable struct for sharing [`CatalogProvider`] across FFI boundaries.

**Fields:**
- `schema_names: fn(...)`
- `schema: fn(...)`
- `register_schema: fn(...)`
- `deregister_schema: fn(...)`
- `logical_codec: crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec`
- `clone: fn(...)` - Used to create a clone on the provider of the execution plan. This should
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `version: fn(...)` - Return the major DataFusion version number of this provider.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the plan.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through

**Methods:**

- `fn new<impl Into<FFI_TaskContextProvider>>(provider: Arc<dyn CatalogProvider>, runtime: Option<Handle>, task_ctx_provider: impl Trait, logical_codec: Option<Arc<dyn LogicalExtensionCodec>>) -> Self` - Creates a new [`FFI_CatalogProvider`].
- `fn new_with_ffi_codec(provider: Arc<dyn CatalogProvider>, runtime: Option<Handle>, logical_codec: FFI_LogicalExtensionCodec) -> Self`

**Traits:** Send, GetStaticEquivalent_, StableAbi, Sync

**Trait Implementations:**

- **Drop**
  - `fn drop(self: & mut Self)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Self`



## datafusion_ffi::catalog_provider::ForeignCatalogProvider

*Struct*

This wrapper struct exists on the receiver side of the FFI interface, so it has
no guarantees about being able to access the data in `private_data`. Any functions
defined on this struct must only use the stable functions provided in
FFI_CatalogProvider to interact with the foreign table provider.

**Tuple Struct**: `()`

**Traits:** Sync, Send

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **CatalogProvider**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn schema_names(self: &Self) -> Vec<String>`
  - `fn schema(self: &Self, name: &str) -> Option<Arc<dyn SchemaProvider>>`
  - `fn register_schema(self: &Self, name: &str, schema: Arc<dyn SchemaProvider>) -> Result<Option<Arc<dyn SchemaProvider>>>`
  - `fn deregister_schema(self: &Self, name: &str, cascade: bool) -> Result<Option<Arc<dyn SchemaProvider>>>`



