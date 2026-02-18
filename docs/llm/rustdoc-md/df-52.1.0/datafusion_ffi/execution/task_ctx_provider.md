**datafusion_ffi > execution > task_ctx_provider**

# Module: execution::task_ctx_provider

## Contents

**Structs**

- [`FFI_TaskContextProvider`](#ffi_taskcontextprovider) - Struct for accessing the [`TaskContext`]. This method contains a weak

---

## datafusion_ffi::execution::task_ctx_provider::FFI_TaskContextProvider

*Struct*

Struct for accessing the [`TaskContext`]. This method contains a weak
reference, so there are no guarantees that the [`TaskContext`] remains
valid. This is used primarily for protobuf encoding and decoding of
data passed across the FFI boundary. See the crate README for
additional information.

**Fields:**
- `task_ctx: fn(...)` - Retrieve the current [`TaskContext`] provided the provider has not
- `clone: fn(...)` - Used to create a clone on the task context accessor. This should
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the plan.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through

**Traits:** Send, GetStaticEquivalent_, StableAbi, Sync

**Trait Implementations:**

- **Drop**
  - `fn drop(self: & mut Self)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Self`
- **From**
  - `fn from(ctx: &Arc<dyn TaskContextProvider>) -> Self`



