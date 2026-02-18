**datafusion_functions > datetime > current_time**

# Module: datetime::current_time

## Contents

**Structs**

- [`CurrentTimeFunc`](#currenttimefunc)

---

## datafusion_functions::datetime::current_time::CurrentTimeFunc

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, _args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn simplify(self: &Self, _args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &CurrentTimeFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`



