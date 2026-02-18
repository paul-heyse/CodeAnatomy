**datafusion_ffi > udaf > accumulator**

# Module: udaf::accumulator

## Contents

**Structs**

- [`FFI_Accumulator`](#ffi_accumulator) - A stable struct for sharing [`Accumulator`] across FFI boundaries.

---

## datafusion_ffi::udaf::accumulator::FFI_Accumulator

*Struct*

A stable struct for sharing [`Accumulator`] across FFI boundaries.
For an explanation of each field, see the corresponding function
defined in [`Accumulator`].

**Fields:**
- `update_batch: fn(...)`
- `evaluate: fn(...)`
- `size: fn(...)`
- `state: fn(...)`
- `merge_batch: fn(...)`
- `retract_batch: fn(...)`
- `supports_retract_batch: bool`
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the accumulator.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through



