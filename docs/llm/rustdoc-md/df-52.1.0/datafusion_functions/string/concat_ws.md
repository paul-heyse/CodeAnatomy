**datafusion_functions > string > concat_ws**

# Module: string::concat_ws

## Contents

**Structs**

- [`ConcatWsFunc`](#concatwsfunc)

---

## datafusion_functions::string::concat_ws::ConcatWsFunc

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
  - `fn invoke_with_args(self: &Self, args: ScalarFunctionArgs) -> Result<ColumnarValue>` - Concatenates all but the first argument, with separators. The first argument is used as the separator string, and should not be NULL. Other NULL arguments are ignored.
  - `fn simplify(self: &Self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult>` - Simply the `concat_ws` function by
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ConcatWsFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`



