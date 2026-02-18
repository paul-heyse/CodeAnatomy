**datafusion_ffi > udwf**

# Module: udwf

## Contents

**Structs**

- [`FFI_SortOptions`](#ffi_sortoptions)
- [`FFI_WindowUDF`](#ffi_windowudf) - A stable struct for sharing a [`WindowUDF`] across FFI boundaries.
- [`ForeignWindowUDF`](#foreignwindowudf) - This struct is used to access an UDF provided by a foreign
- [`WindowUDFPrivateData`](#windowudfprivatedata)

---

## datafusion_ffi::udwf::FFI_SortOptions

*Struct*

**Fields:**
- `descending: bool`
- `nulls_first: bool`

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **From**
  - `fn from(value: &SortOptions) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> FFI_SortOptions`



## datafusion_ffi::udwf::FFI_WindowUDF

*Struct*

A stable struct for sharing a [`WindowUDF`] across FFI boundaries.

**Fields:**
- `name: abi_stable::std_types::RString` - FFI equivalent to the `name` of a [`WindowUDF`]
- `aliases: abi_stable::std_types::RVec<abi_stable::std_types::RString>` - FFI equivalent to the `aliases` of a [`WindowUDF`]
- `volatility: crate::volatility::FFI_Volatility` - FFI equivalent to the `volatility` of a [`WindowUDF`]
- `partition_evaluator: fn(...)`
- `field: fn(...)`
- `coerce_types: fn(...)` - Performs type coercion. To simply this interface, all UDFs are treated as having
- `sort_options: abi_stable::std_types::ROption<FFI_SortOptions>`
- `clone: fn(...)` - Used to create a clone on the provider of the udf. This should
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the udf.
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
  - `fn from(udf: Arc<WindowUDF>) -> Self`



## datafusion_ffi::udwf::ForeignWindowUDF

*Struct*

This struct is used to access an UDF provided by a foreign
library across a FFI boundary.

The ForeignWindowUDF is to be used by the caller of the UDF, so it has
no knowledge or access to the private data. All interaction with the UDF
must occur through the functions defined in FFI_WindowUDF.

**Traits:** Sync, Eq, Send

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **WindowUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`
  - `fn partition_evaluator(self: &Self, args: datafusion_expr::function::PartitionEvaluatorArgs) -> Result<Box<dyn PartitionEvaluator>>`
  - `fn field(self: &Self, field_args: WindowUDFFieldArgs) -> Result<FieldRef>`
  - `fn sort_options(self: &Self) -> Option<SortOptions>`
  - `fn limit_effect(self: &Self, _args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`



## datafusion_ffi::udwf::WindowUDFPrivateData

*Struct*

**Fields:**
- `udf: std::sync::Arc<datafusion_expr::WindowUDF>`



