**datafusion_ffi > proto > physical_extension_codec**

# Module: proto::physical_extension_codec

## Contents

**Structs**

- [`FFI_PhysicalExtensionCodec`](#ffi_physicalextensioncodec) - A stable struct for sharing [`PhysicalExtensionCodec`] across FFI boundaries.
- [`ForeignPhysicalExtensionCodec`](#foreignphysicalextensioncodec) - This wrapper struct exists on the receiver side of the FFI interface, so it has

---

## datafusion_ffi::proto::physical_extension_codec::FFI_PhysicalExtensionCodec

*Struct*

A stable struct for sharing [`PhysicalExtensionCodec`] across FFI boundaries.

**Fields:**
- `clone: fn(...)` - Used to create a clone on the provider of the execution plan. This should
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `version: fn(...)` - Return the major DataFusion version number of this provider.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the plan.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through

**Methods:**

- `fn new<impl Into<FFI_TaskContextProvider>>(provider: Arc<dyn PhysicalExtensionCodec>, runtime: Option<Handle>, task_ctx_provider: impl Trait) -> Self` - Creates a new [`FFI_PhysicalExtensionCodec`].

**Traits:** GetStaticEquivalent_, StableAbi, Sync, Send

**Trait Implementations:**

- **Drop**
  - `fn drop(self: & mut Self)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Self`



## datafusion_ffi::proto::physical_extension_codec::ForeignPhysicalExtensionCodec

*Struct*

This wrapper struct exists on the receiver side of the FFI interface, so it has
no guarantees about being able to access the data in `private_data`. Any functions
defined on this struct must only use the stable functions provided in
FFI_PhysicalExtensionCodec to interact with the foreign table provider.

**Tuple Struct**: `(FFI_PhysicalExtensionCodec)`

**Traits:** Sync, Send

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PhysicalExtensionCodec**
  - `fn try_decode(self: &Self, buf: &[u8], inputs: &[Arc<dyn ExecutionPlan>], _ctx: &TaskContext) -> Result<Arc<dyn ExecutionPlan>>`
  - `fn try_encode(self: &Self, node: Arc<dyn ExecutionPlan>, buf: & mut Vec<u8>) -> Result<()>`
  - `fn try_decode_udf(self: &Self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>>`
  - `fn try_encode_udf(self: &Self, node: &ScalarUDF, buf: & mut Vec<u8>) -> Result<()>`
  - `fn try_decode_udaf(self: &Self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>>`
  - `fn try_encode_udaf(self: &Self, node: &AggregateUDF, buf: & mut Vec<u8>) -> Result<()>`
  - `fn try_decode_udwf(self: &Self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>>`
  - `fn try_encode_udwf(self: &Self, node: &WindowUDF, buf: & mut Vec<u8>) -> Result<()>`



