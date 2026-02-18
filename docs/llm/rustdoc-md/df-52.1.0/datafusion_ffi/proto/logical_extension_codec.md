**datafusion_ffi > proto > logical_extension_codec**

# Module: proto::logical_extension_codec

## Contents

**Structs**

- [`FFI_LogicalExtensionCodec`](#ffi_logicalextensioncodec) - A stable struct for sharing [`LogicalExtensionCodec`] across FFI boundaries.
- [`ForeignLogicalExtensionCodec`](#foreignlogicalextensioncodec) - This wrapper struct exists on the receiver side of the FFI interface, so it has

---

## datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec

*Struct*

A stable struct for sharing [`LogicalExtensionCodec`] across FFI boundaries.

**Fields:**
- `task_ctx_provider: crate::execution::FFI_TaskContextProvider`
- `clone: fn(...)` - Used to create a clone on the provider of the execution plan. This should
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `version: fn(...)` - Return the major DataFusion version number of this provider.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the plan.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through

**Methods:**

- `fn new<impl Into<FFI_TaskContextProvider>>(codec: Arc<dyn LogicalExtensionCodec>, runtime: Option<Handle>, task_ctx_provider: impl Trait) -> Self` - Creates a new [`FFI_LogicalExtensionCodec`].
- `fn new_default(task_ctx_provider: &Arc<dyn TaskContextProvider>) -> Self`

**Traits:** Sync, Send, GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Self`
- **Drop**
  - `fn drop(self: & mut Self)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_ffi::proto::logical_extension_codec::ForeignLogicalExtensionCodec

*Struct*

This wrapper struct exists on the receiver side of the FFI interface, so it has
no guarantees about being able to access the data in `private_data`. Any functions
defined on this struct must only use the stable functions provided in
FFI_LogicalExtensionCodec to interact with the foreign table provider.

**Tuple Struct**: `(FFI_LogicalExtensionCodec)`

**Traits:** Sync, Send

**Trait Implementations:**

- **LogicalExtensionCodec**
  - `fn try_decode(self: &Self, _buf: &[u8], _inputs: &[LogicalPlan], _ctx: &TaskContext) -> Result<Extension>`
  - `fn try_encode(self: &Self, _node: &Extension, _buf: & mut Vec<u8>) -> Result<()>`
  - `fn try_decode_table_provider(self: &Self, buf: &[u8], table_ref: &TableReference, schema: SchemaRef, _ctx: &TaskContext) -> Result<Arc<dyn TableProvider>>`
  - `fn try_encode_table_provider(self: &Self, table_ref: &TableReference, node: Arc<dyn TableProvider>, buf: & mut Vec<u8>) -> Result<()>`
  - `fn try_decode_file_format(self: &Self, _buf: &[u8], _ctx: &TaskContext) -> Result<Arc<dyn FileFormatFactory>>`
  - `fn try_encode_file_format(self: &Self, _buf: & mut Vec<u8>, _node: Arc<dyn FileFormatFactory>) -> Result<()>`
  - `fn try_decode_udf(self: &Self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>>`
  - `fn try_encode_udf(self: &Self, node: &ScalarUDF, buf: & mut Vec<u8>) -> Result<()>`
  - `fn try_decode_udaf(self: &Self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>>`
  - `fn try_encode_udaf(self: &Self, node: &AggregateUDF, buf: & mut Vec<u8>) -> Result<()>`
  - `fn try_decode_udwf(self: &Self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>>`
  - `fn try_encode_udwf(self: &Self, node: &WindowUDF, buf: & mut Vec<u8>) -> Result<()>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



