**datafusion_ffi > udaf > accumulator_args**

# Module: udaf::accumulator_args

## Contents

**Structs**

- [`FFI_AccumulatorArgs`](#ffi_accumulatorargs) - A stable struct for sharing [`AccumulatorArgs`] across FFI boundaries.
- [`ForeignAccumulatorArgs`](#foreignaccumulatorargs) - This struct mirrors AccumulatorArgs except that it contains owned data.

---

## datafusion_ffi::udaf::accumulator_args::FFI_AccumulatorArgs

*Struct*

A stable struct for sharing [`AccumulatorArgs`] across FFI boundaries.
For an explanation of each field, see the corresponding field
defined in [`AccumulatorArgs`].



## datafusion_ffi::udaf::accumulator_args::ForeignAccumulatorArgs

*Struct*

This struct mirrors AccumulatorArgs except that it contains owned data.
It is necessary to create this struct so that we can parse the protobuf
data across the FFI boundary and turn it into owned data that
AccumulatorArgs can then reference.

**Fields:**
- `return_field: arrow_schema::FieldRef`
- `schema: arrow::datatypes::Schema`
- `expr_fields: Vec<arrow_schema::FieldRef>`
- `ignore_nulls: bool`
- `order_bys: Vec<datafusion_physical_expr::PhysicalSortExpr>`
- `is_reversed: bool`
- `name: String`
- `is_distinct: bool`
- `exprs: Vec<std::sync::Arc<dyn PhysicalExpr>>`



