**datafusion_functions > datetime > current_date**

# Module: datetime::current_date

## Contents

**Structs**

- [`CurrentDateFunc`](#currentdatefunc)

---

## datafusion_functions::datetime::current_date::CurrentDateFunc

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &CurrentDateFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, _args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn simplify(self: &Self, _args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`



