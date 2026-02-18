**datafusion_functions > core > arrow_metadata**

# Module: core::arrow_metadata

## Contents

**Structs**

- [`ArrowMetadataFunc`](#arrowmetadatafunc)

---

## datafusion_functions::core::arrow_metadata::ArrowMetadataFunc

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ArrowMetadataFunc`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArrowMetadataFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: ScalarFunctionArgs) -> Result<ColumnarValue>`



