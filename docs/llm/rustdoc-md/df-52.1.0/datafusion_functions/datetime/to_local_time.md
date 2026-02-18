**datafusion_functions > datetime > to_local_time**

# Module: datetime::to_local_time

## Contents

**Structs**

- [`ToLocalTimeFunc`](#tolocaltimefunc) - A UDF function that converts a timezone-aware timestamp to local time (with no offset or

---

## datafusion_functions::datetime::to_local_time::ToLocalTimeFunc

*Struct*

A UDF function that converts a timezone-aware timestamp to local time (with no offset or
timezone information). In other words, this function strips off the timezone from the timestamp,
while keep the display value of the timestamp the same.

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ToLocalTimeFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



