**datafusion_functions > core > nvl**

# Module: core::nvl

## Contents

**Structs**

- [`NVLFunc`](#nvlfunc)

---

## datafusion_functions::core::nvl::NVLFunc

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &NVLFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn return_field_from_args(self: &Self, args: ReturnFieldArgs) -> Result<FieldRef>`
  - `fn simplify(self: &Self, args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult>`
  - `fn invoke_with_args(self: &Self, args: ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn conditional_arguments<'a>(self: &Self, args: &'a [Expr]) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)>`
  - `fn short_circuits(self: &Self) -> bool`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn documentation(self: &Self) -> Option<&Documentation>`



