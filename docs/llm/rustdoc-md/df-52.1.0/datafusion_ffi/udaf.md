**datafusion_ffi > udaf**

# Module: udaf

## Contents

**Structs**

- [`AggregateUDFPrivateData`](#aggregateudfprivatedata)
- [`FFI_AggregateUDF`](#ffi_aggregateudf) - A stable struct for sharing a [`AggregateUDF`] across FFI boundaries.
- [`ForeignAggregateUDF`](#foreignaggregateudf) - This struct is used to access an UDF provided by a foreign

**Enums**

- [`FFI_AggregateOrderSensitivity`](#ffi_aggregateordersensitivity)

---

## datafusion_ffi::udaf::AggregateUDFPrivateData

*Struct*

**Fields:**
- `udaf: std::sync::Arc<datafusion_expr::AggregateUDF>`



## datafusion_ffi::udaf::FFI_AggregateOrderSensitivity

*Enum*

**Variants:**
- `Insensitive`
- `HardRequirement`
- `SoftRequirement`
- `Beneficial`

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **From**
  - `fn from(value: AggregateOrderSensitivity) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_ffi::udaf::FFI_AggregateUDF

*Struct*

A stable struct for sharing a [`AggregateUDF`] across FFI boundaries.

**Fields:**
- `name: abi_stable::std_types::RString` - FFI equivalent to the `name` of a [`AggregateUDF`]
- `aliases: abi_stable::std_types::RVec<abi_stable::std_types::RString>` - FFI equivalent to the `aliases` of a [`AggregateUDF`]
- `volatility: crate::volatility::FFI_Volatility` - FFI equivalent to the `volatility` of a [`AggregateUDF`]
- `return_field: fn(...)` - Determines the return field of the underlying [`AggregateUDF`] based on the
- `is_nullable: bool` - FFI equivalent to the `is_nullable` of a [`AggregateUDF`]
- `groups_accumulator_supported: fn(...)` - FFI equivalent to [`AggregateUDF::groups_accumulator_supported`]
- `accumulator: fn(...)` - FFI equivalent to [`AggregateUDF::accumulator`]
- `create_sliding_accumulator: fn(...)` - FFI equivalent to [`AggregateUDF::create_sliding_accumulator`]
- `state_fields: fn(...)` - FFI equivalent to [`AggregateUDF::state_fields`]
- `create_groups_accumulator: fn(...)` - FFI equivalent to [`AggregateUDF::create_groups_accumulator`]
- `with_beneficial_ordering: fn(...)` - FFI equivalent to [`AggregateUDF::with_beneficial_ordering`]
- `order_sensitivity: fn(...)` - FFI equivalent to [`AggregateUDF::order_sensitivity`]
- `coerce_types: fn(...)` - Performs type coercion. To simply this interface, all UDFs are treated as having
- `clone: fn(...)` - Used to create a clone on the provider of the udaf. This should
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the udaf.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through

**Traits:** Sync, Send, GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **Drop**
  - `fn drop(self: & mut Self)`
- **Clone**
  - `fn clone(self: &Self) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(udaf: Arc<AggregateUDF>) -> Self`



## datafusion_ffi::udaf::ForeignAggregateUDF

*Struct*

This struct is used to access an UDF provided by a foreign
library across a FFI boundary.

The ForeignAggregateUDF is to be used by the caller of the UDF, so it has
no knowledge or access to the private data. All interaction with the UDF
must occur through the functions defined in FFI_AggregateUDF.

**Traits:** Send, Sync, Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn return_field(self: &Self, arg_fields: &[FieldRef]) -> Result<FieldRef>`
  - `fn is_nullable(self: &Self) -> bool`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn groups_accumulator_supported(self: &Self, args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn create_sliding_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn with_beneficial_ordering(self: Arc<Self>, beneficial_ordering: bool) -> Result<Option<Arc<dyn AggregateUDFImpl>>>`
  - `fn order_sensitivity(self: &Self) -> AggregateOrderSensitivity`
  - `fn simplify(self: &Self) -> Option<AggregateFunctionSimplification>`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`



