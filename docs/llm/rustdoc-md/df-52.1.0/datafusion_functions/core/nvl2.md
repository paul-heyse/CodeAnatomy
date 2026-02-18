**datafusion_functions > core > nvl2**

# Module: core::nvl2

## Contents

**Structs**

- [`NVL2Func`](#nvl2func)

---

## datafusion_functions::core::nvl2::NVL2Func

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn return_field_from_args(self: &Self, args: ReturnFieldArgs) -> Result<FieldRef>`
  - `fn invoke_with_args(self: &Self, _args: ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn simplify(self: &Self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult>`
  - `fn short_circuits(self: &Self) -> bool`
  - `fn coerce_types(self: &Self, arg_types: &[DataType]) -> Result<Vec<DataType>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **PartialEq**
  - `fn eq(self: &Self, other: &NVL2Func) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`



