**datafusion_functions > string > ascii**

# Module: string::ascii

## Contents

**Structs**

- [`AsciiFunc`](#asciifunc)

**Functions**

- [`ascii`](#ascii) - Returns the numeric code of the first character of the argument.

---

## datafusion_functions::string::ascii::AsciiFunc

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
  - `fn invoke_with_args(self: &Self, args: ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialEq**
  - `fn eq(self: &Self, other: &AsciiFunc) -> bool`



## datafusion_functions::string::ascii::ascii

*Function*

Returns the numeric code of the first character of the argument.

```rust
fn ascii(args: &[arrow::array::ArrayRef]) -> datafusion_common::Result<arrow::array::ArrayRef>
```



