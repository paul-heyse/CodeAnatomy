**datafusion_functions > datetime > now**

# Module: datetime::now

## Contents

**Structs**

- [`NowFunc`](#nowfunc)

---

## datafusion_functions::datetime::now::NowFunc

*Struct*

**Methods:**

- `fn new() -> Self` - Deprecated constructor retained for backwards compatibility.
- `fn new_with_config(config: &ConfigOptions) -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn with_updated_config(self: &Self, config: &ConfigOptions) -> Option<ScalarUDF>`
  - `fn return_field_from_args(self: &Self, _args: ReturnFieldArgs) -> Result<FieldRef>`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, _args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn simplify(self: &Self, _args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **PartialEq**
  - `fn eq(self: &Self, other: &NowFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`



