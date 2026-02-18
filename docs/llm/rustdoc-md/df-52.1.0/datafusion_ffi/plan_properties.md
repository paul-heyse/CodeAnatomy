**datafusion_ffi > plan_properties**

# Module: plan_properties

## Contents

**Structs**

- [`FFI_PlanProperties`](#ffi_planproperties) - A stable struct for sharing [`PlanProperties`] across FFI boundaries.

**Enums**

- [`FFI_Boundedness`](#ffi_boundedness) - FFI safe version of [`Boundedness`].
- [`FFI_EmissionType`](#ffi_emissiontype) - FFI safe version of [`EmissionType`].

---

## datafusion_ffi::plan_properties::FFI_Boundedness

*Enum*

FFI safe version of [`Boundedness`].

**Variants:**
- `Bounded`
- `Unbounded{ requires_infinite_memory: bool }`

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> FFI_Boundedness`
- **From**
  - `fn from(value: Boundedness) -> Self`



## datafusion_ffi::plan_properties::FFI_EmissionType

*Enum*

FFI safe version of [`EmissionType`].

**Variants:**
- `Incremental`
- `Final`
- `Both`

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> FFI_EmissionType`
- **From**
  - `fn from(value: EmissionType) -> Self`



## datafusion_ffi::plan_properties::FFI_PlanProperties

*Struct*

A stable struct for sharing [`PlanProperties`] across FFI boundaries.

**Fields:**
- `output_partitioning: fn(...)` - The output partitioning of the plan.
- `emission_type: fn(...)` - Return the emission type of the plan.
- `boundedness: fn(...)` - Indicate boundedness of the plan and its memory requirements.
- `output_ordering: fn(...)` - The output ordering of the plan.
- `schema: fn(...)` - Return the schema of the plan.
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the plan.
- `library_marker_id: fn(...)` - Utility to identify when FFI objects are accessed locally through

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **From**
  - `fn from(props: &PlanProperties) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Drop**
  - `fn drop(self: & mut Self)`



