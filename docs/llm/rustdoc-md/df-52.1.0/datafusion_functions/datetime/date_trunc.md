**datafusion_functions > datetime > date_trunc**

# Module: datetime::date_trunc

## Contents

**Structs**

- [`DateTruncFunc`](#datetruncfunc)

---

## datafusion_functions::datetime::date_trunc::DateTruncFunc

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
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn output_ordering(self: &Self, input: &[ExprProperties]) -> Result<SortProperties>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &DateTruncFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`



