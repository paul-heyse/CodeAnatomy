**datafusion_ffi > udwf > partition_evaluator**

# Module: udwf::partition_evaluator

## Contents

**Structs**

- [`FFI_PartitionEvaluator`](#ffi_partitionevaluator) - A stable struct for sharing [`PartitionEvaluator`] across FFI boundaries.

---

## datafusion_ffi::udwf::partition_evaluator::FFI_PartitionEvaluator

*Struct*

A stable struct for sharing [`PartitionEvaluator`] across FFI boundaries.
For an explanation of each field, see the corresponding function
defined in [`PartitionEvaluator`].

**Fields:**
- `evaluate_all: fn(...)`
- `evaluate: fn(...)`
- `evaluate_all_with_rank: fn(...)`
- `get_range: fn(...)`
- `is_causal: bool`
- `supports_bounded_execution: bool`
- `uses_window_frame: bool`
- `include_rank: bool`
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the evaluator.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through



