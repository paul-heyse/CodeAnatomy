**datafusion_ffi > catalog_provider_list**

# Module: catalog_provider_list

## Contents

**Structs**

- [`FFI_CatalogProviderList`](#ffi_catalogproviderlist) - A stable struct for sharing [`CatalogProviderList`] across FFI boundaries.
- [`ForeignCatalogProviderList`](#foreigncatalogproviderlist) - This wrapper struct exists on the receiver side of the FFI interface, so it has

---

## datafusion_ffi::catalog_provider_list::FFI_CatalogProviderList

*Struct*

A stable struct for sharing [`CatalogProviderList`] across FFI boundaries.

**Fields:**
- `register_catalog: fn(...)` - Register a catalog
- `catalog_names: fn(...)` - List of existing catalogs
- `catalog: fn(...)` - Access a catalog
- `logical_codec: crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec`
- `clone: fn(...)` - Used to create a clone on the provider of the execution plan. This should
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `version: fn(...)` - Return the major DataFusion version number of this provider.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the plan.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through

**Methods:**

- `fn new<impl Into<FFI_TaskContextProvider>>(provider: Arc<dyn CatalogProviderList>, runtime: Option<Handle>, task_ctx_provider: impl Trait, logical_codec: Option<Arc<dyn LogicalExtensionCodec>>) -> Self` - Creates a new [`FFI_CatalogProviderList`].
- `fn new_with_ffi_codec(provider: Arc<dyn CatalogProviderList>, runtime: Option<Handle>, logical_codec: FFI_LogicalExtensionCodec) -> Self`

**Traits:** Send, GetStaticEquivalent_, StableAbi, Sync

**Trait Implementations:**

- **Drop**
  - `fn drop(self: & mut Self)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Self`



## datafusion_ffi::catalog_provider_list::ForeignCatalogProviderList

*Struct*

This wrapper struct exists on the receiver side of the FFI interface, so it has
no guarantees about being able to access the data in `private_data`. Any functions
defined on this struct must only use the stable functions provided in
FFI_CatalogProviderList to interact with the foreign catalog provider list.

**Tuple Struct**: `()`

**Traits:** Send, Sync

**Trait Implementations:**

- **CatalogProviderList**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn register_catalog(self: &Self, name: String, catalog: Arc<dyn CatalogProvider>) -> Option<Arc<dyn CatalogProvider>>`
  - `fn catalog_names(self: &Self) -> Vec<String>`
  - `fn catalog(self: &Self, name: &str) -> Option<Arc<dyn CatalogProvider>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



