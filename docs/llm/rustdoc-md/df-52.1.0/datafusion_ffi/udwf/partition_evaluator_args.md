**datafusion_ffi > udwf > partition_evaluator_args**

# Module: udwf::partition_evaluator_args

## Contents

**Structs**

- [`FFI_PartitionEvaluatorArgs`](#ffi_partitionevaluatorargs) - A stable struct for sharing [`PartitionEvaluatorArgs`] across FFI boundaries.
- [`ForeignPartitionEvaluatorArgs`](#foreignpartitionevaluatorargs) - This struct mirrors PartitionEvaluatorArgs except that it contains owned data.

---

## datafusion_ffi::udwf::partition_evaluator_args::FFI_PartitionEvaluatorArgs

*Struct*

A stable struct for sharing [`PartitionEvaluatorArgs`] across FFI boundaries.
For an explanation of each field, see the corresponding function
defined in [`PartitionEvaluatorArgs`].



## datafusion_ffi::udwf::partition_evaluator_args::ForeignPartitionEvaluatorArgs

*Struct*

This struct mirrors PartitionEvaluatorArgs except that it contains owned data.
It is necessary to create this struct so that we can parse the protobuf
data across the FFI boundary and turn it into owned data that
PartitionEvaluatorArgs can then reference.



