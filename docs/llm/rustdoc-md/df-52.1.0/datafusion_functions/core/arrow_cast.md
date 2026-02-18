**datafusion_functions > core > arrow_cast**

# Module: core::arrow_cast

## Contents

**Structs**

- [`ArrowCastFunc`](#arrowcastfunc) - Implements casting to arbitrary arrow types (rather than SQL types)

---

## datafusion_functions::core::arrow_cast::ArrowCastFunc

*Struct*

Implements casting to arbitrary arrow types (rather than SQL types)

Note that the `arrow_cast` function is somewhat special in that its
return depends only on the *value* of its second argument (not its type)

It is implemented by calling the same underlying arrow `cast` kernel as
normal SQL casts.

For example to cast to `int` using SQL  (which is then mapped to the arrow
type `Int32`)

```sql
select cast(column_x as int) ...
```

Use the `arrow_cast` function to cast to a specific arrow type

For example
```sql
select arrow_cast(column_x, 'Float64')
```

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn return_field_from_args(self: &Self, args: ReturnFieldArgs) -> Result<FieldRef>`
  - `fn invoke_with_args(self: &Self, _args: ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn simplify(self: &Self, args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArrowCastFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



