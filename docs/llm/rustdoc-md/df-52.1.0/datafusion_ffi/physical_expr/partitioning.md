**datafusion_ffi > physical_expr > partitioning**

# Module: physical_expr::partitioning

## Contents

**Enums**

- [`FFI_Partitioning`](#ffi_partitioning) - A stable struct for sharing [`Partitioning`] across FFI boundaries.

---

## datafusion_ffi::physical_expr::partitioning::FFI_Partitioning

*Enum*

A stable struct for sharing [`Partitioning`] across FFI boundaries.
See ['Partitioning'] for the meaning of each variant.

**Variants:**
- `RoundRobinBatch(usize)`
- `Hash(abi_stable::std_types::RVec<crate::physical_expr::FFI_PhysicalExpr>, usize)`
- `UnknownPartitioning(usize)`



