**datafusion_ffi > execution > task_ctx**

# Module: execution::task_ctx

## Contents

**Structs**

- [`FFI_TaskContext`](#ffi_taskcontext) - A stable struct for sharing [`TaskContext`] across FFI boundaries.

---

## datafusion_ffi::execution::task_ctx::FFI_TaskContext

*Struct*

A stable struct for sharing [`TaskContext`] across FFI boundaries.

**Fields:**
- `session_id: fn(...)` - Return the session ID.
- `task_id: fn(...)` - Return the task ID.
- `session_config: fn(...)` - Return the session configuration.
- `scalar_functions: fn(...)` - Returns a hashmap of names to scalar functions.
- `aggregate_functions: fn(...)` - Returns a hashmap of names to aggregate functions.
- `window_functions: fn(...)` - Returns a hashmap of names to window functions.
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the plan.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **From**
  - `fn from(ctx: Arc<TaskContext>) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Drop**
  - `fn drop(self: & mut Self)`



