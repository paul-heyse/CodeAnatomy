**datafusion_functions > regex > regexpinstr**

# Module: regex::regexpinstr

## Contents

**Structs**

- [`RegexpInstrFunc`](#regexpinstrfunc)

**Functions**

- [`regexp_instr_func`](#regexp_instr_func)

---

## datafusion_functions::regex::regexpinstr::RegexpInstrFunc

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &RegexpInstrFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions::regex::regexpinstr::regexp_instr_func

*Function*

```rust
fn regexp_instr_func(args: &[arrow::array::ArrayRef]) -> datafusion_common::Result<arrow::array::ArrayRef>
```



