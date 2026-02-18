**datafusion_ffi > udf**

# Module: udf

## Contents

**Modules**

- [`return_type_args`](#return_type_args)

**Structs**

- [`FFI_ScalarUDF`](#ffi_scalarudf) - A stable struct for sharing a [`ScalarUDF`] across FFI boundaries.
- [`ForeignScalarUDF`](#foreignscalarudf) - This struct is used to access an UDF provided by a foreign
- [`ScalarUDFPrivateData`](#scalarudfprivatedata)

---

## datafusion_ffi::udf::FFI_ScalarUDF

*Struct*

A stable struct for sharing a [`ScalarUDF`] across FFI boundaries.

**Fields:**
- `name: abi_stable::std_types::RString` - FFI equivalent to the `name` of a [`ScalarUDF`]
- `aliases: abi_stable::std_types::RVec<abi_stable::std_types::RString>` - FFI equivalent to the `aliases` of a [`ScalarUDF`]
- `volatility: crate::volatility::FFI_Volatility` - FFI equivalent to the `volatility` of a [`ScalarUDF`]
- `return_field_from_args: fn(...)` - Determines the return info of the underlying [`ScalarUDF`].
- `invoke_with_args: fn(...)` - Execute the underlying [`ScalarUDF`] and return the result as a `FFI_ArrowArray`
- `short_circuits: bool` - See [`ScalarUDFImpl`] for details on short_circuits
- `coerce_types: fn(...)` - Performs type coercion. To simply this interface, all UDFs are treated as having
- `clone: fn(...)` - Used to create a clone on the provider of the udf. This should
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the udf.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through

**Traits:** GetStaticEquivalent_, StableAbi, Sync, Send

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(udf: Arc<ScalarUDF>) -> Self`
- **Drop**
  - `fn drop(self: & mut Self)`



## datafusion_ffi::udf::ForeignScalarUDF

*Struct*

This struct is used to access an UDF provided by a foreign
library across a FFI boundary.

The ForeignScalarUDF is to be used by the caller of the UDF, so it has
no knowledge or access to the private data. All interaction with the UDF
must occur through the functions defined in FFI_ScalarUDF.

**Traits:** Sync, Eq, Send

**Trait Implementations:**

- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn return_field_from_args(self: &Self, args: ReturnFieldArgs) -> Result<FieldRef>`
  - `fn invoke_with_args(self: &Self, invoke_args: ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn short_circuits(self: &Self) -> bool`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_ffi::udf::ScalarUDFPrivateData

*Struct*

**Fields:**
- `udf: std::sync::Arc<datafusion_expr::ScalarUDF>`



## Module: return_type_args



