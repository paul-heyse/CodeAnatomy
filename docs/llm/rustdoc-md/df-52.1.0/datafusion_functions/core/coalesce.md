**datafusion_functions > core > coalesce**

# Module: core::coalesce

## Contents

**Structs**

- [`CoalesceFunc`](#coalescefunc)

---

## datafusion_functions::core::coalesce::CoalesceFunc

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
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn return_field_from_args(self: &Self, args: ReturnFieldArgs) -> Result<FieldRef>`
  - `fn simplify(self: &Self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult>`
  - `fn invoke_with_args(self: &Self, _args: ScalarFunctionArgs) -> Result<ColumnarValue>` - coalesce evaluates to the first value which is not NULL
  - `fn conditional_arguments<'a>(self: &Self, args: &'a [Expr]) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)>`
  - `fn short_circuits(self: &Self) -> bool`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &CoalesceFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



