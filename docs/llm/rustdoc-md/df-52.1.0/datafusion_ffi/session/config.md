**datafusion_ffi > session > config**

# Module: session::config

## Contents

**Structs**

- [`FFI_SessionConfig`](#ffi_sessionconfig) - A stable struct for sharing [`SessionConfig`] across FFI boundaries.

---

## datafusion_ffi::session::config::FFI_SessionConfig

*Struct*

A stable struct for sharing [`SessionConfig`] across FFI boundaries.
Instead of attempting to expose the entire SessionConfig interface, we
convert the config options into a map from a string to string and pass
those values across the FFI boundary. On the receiver side, we
reconstruct a SessionConfig from those values.

It is possible that using different versions of DataFusion across the
FFI boundary could have differing expectations of the config options.
This is a limitation of this approach, but exposing the entire
SessionConfig via a FFI interface would be extensive and provide limited
value over this version.

**Fields:**
- `config_options: fn(...)` - Return a hash map from key to value of the config options represented
- `clone: fn(...)` - Used to create a clone on the provider of the execution plan. This should
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the plan.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through

**Traits:** GetStaticEquivalent_, StableAbi, Sync, Send

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Self`
- **Drop**
  - `fn drop(self: & mut Self)`
- **From**
  - `fn from(session: &SessionConfig) -> Self`



