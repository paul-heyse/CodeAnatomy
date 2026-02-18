**datafusion_functions > regex > regexpmatch**

# Module: regex::regexpmatch

## Contents

**Structs**

- [`RegexpMatchFunc`](#regexpmatchfunc)

**Functions**

- [`regexp_match`](#regexp_match)

---

## datafusion_functions::regex::regexpmatch::RegexpMatchFunc

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &RegexpMatchFunc) -> bool`



## datafusion_functions::regex::regexpmatch::regexp_match

*Function*

```rust
fn regexp_match(args: &[arrow::array::ArrayRef]) -> datafusion_common::Result<arrow::array::ArrayRef>
```



