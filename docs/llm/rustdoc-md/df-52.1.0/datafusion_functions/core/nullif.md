**datafusion_functions > core > nullif**

# Module: core::nullif

## Contents

**Structs**

- [`NullIfFunc`](#nulliffunc)

---

## datafusion_functions::core::nullif::NullIfFunc

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &NullIfFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



