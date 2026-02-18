**datafusion_ffi > udaf > groups_accumulator**

# Module: udaf::groups_accumulator

## Contents

**Structs**

- [`FFI_GroupsAccumulator`](#ffi_groupsaccumulator) - A stable struct for sharing [`GroupsAccumulator`] across FFI boundaries.

**Enums**

- [`FFI_EmitTo`](#ffi_emitto)

---

## datafusion_ffi::udaf::groups_accumulator::FFI_EmitTo

*Enum*

**Variants:**
- `All`
- `First(usize)`



## datafusion_ffi::udaf::groups_accumulator::FFI_GroupsAccumulator

*Struct*

A stable struct for sharing [`GroupsAccumulator`] across FFI boundaries.
For an explanation of each field, see the corresponding function
defined in [`GroupsAccumulator`].

**Fields:**
- `update_batch: fn(...)`
- `evaluate: fn(...)`
- `size: fn(...)`
- `state: fn(...)`
- `merge_batch: fn(...)`
- `convert_to_state: fn(...)`
- `supports_convert_to_state: bool`
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the accumulator.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through



