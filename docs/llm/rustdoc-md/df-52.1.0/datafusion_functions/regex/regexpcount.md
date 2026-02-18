**datafusion_functions > regex > regexpcount**

# Module: regex::regexpcount

## Contents

**Structs**

- [`RegexpCountFunc`](#regexpcountfunc)

**Functions**

- [`regexp_count_func`](#regexp_count_func)

---

## datafusion_functions::regex::regexpcount::RegexpCountFunc

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &RegexpCountFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`



## datafusion_functions::regex::regexpcount::regexp_count_func

*Function*

```rust
fn regexp_count_func(args: &[arrow::array::ArrayRef]) -> datafusion_common::Result<arrow::array::ArrayRef>
```



