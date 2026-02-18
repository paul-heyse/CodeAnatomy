**datafusion_functions > math > log**

# Module: math::log

## Contents

**Structs**

- [`LogFunc`](#logfunc)

---

## datafusion_functions::math::log::LogFunc

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
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn output_ordering(self: &Self, input: &[ExprProperties]) -> Result<SortProperties>`
  - `fn invoke_with_args(self: &Self, args: ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
  - `fn simplify(self: &Self, args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult>` - Simplify the `log` function by the relevant rules:
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &LogFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



