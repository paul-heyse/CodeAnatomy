**datafusion_functions > core > getfield**

# Module: core::getfield

## Contents

**Structs**

- [`GetFieldFunc`](#getfieldfunc)

---

## datafusion_functions::core::getfield::GetFieldFunc

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn display_name(self: &Self, args: &[Expr]) -> Result<String>`
  - `fn schema_name(self: &Self, args: &[Expr]) -> Result<String>`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _: &[DataType]) -> Result<DataType>`
  - `fn return_field_from_args(self: &Self, args: ReturnFieldArgs) -> Result<FieldRef>`
  - `fn invoke_with_args(self: &Self, args: ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn simplify(self: &Self, args: Vec<Expr>, _info: &dyn datafusion_expr::simplify::SimplifyInfo) -> Result<ExprSimplifyResult>`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &GetFieldFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



