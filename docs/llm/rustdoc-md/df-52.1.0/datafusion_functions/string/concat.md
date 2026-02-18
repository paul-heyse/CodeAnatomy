**datafusion_functions > string > concat**

# Module: string::concat

## Contents

**Structs**

- [`ConcatFunc`](#concatfunc)

---

## datafusion_functions::string::concat::ConcatFunc

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ConcatFunc) -> bool`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: ScalarFunctionArgs) -> Result<ColumnarValue>` - Concatenates the text representations of all the arguments. NULL arguments are ignored.
  - `fn simplify(self: &Self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult>` - Simplify the `concat` function by
  - `fn documentation(self: &Self) -> Option<&Documentation>`
  - `fn preserves_lex_ordering(self: &Self, _inputs: &[ExprProperties]) -> Result<bool>`



