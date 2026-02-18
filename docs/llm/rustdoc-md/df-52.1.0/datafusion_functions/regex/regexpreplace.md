**datafusion_functions > regex > regexpreplace**

# Module: regex::regexpreplace

## Contents

**Structs**

- [`RegexpReplaceFunc`](#regexpreplacefunc)

**Functions**

- [`regexp_replace`](#regexp_replace) - Replaces substring(s) matching a PCRE-like regular expression.

---

## datafusion_functions::regex::regexpreplace::RegexpReplaceFunc

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &RegexpReplaceFunc) -> bool`
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



## datafusion_functions::regex::regexpreplace::regexp_replace

*Function*

Replaces substring(s) matching a PCRE-like regular expression.

The full list of supported features and syntax can be found at
<https://docs.rs/regex/latest/regex/#syntax>

Supported flags with the addition of 'g' can be found at
<https://docs.rs/regex/latest/regex/#grouping-and-flags>

# Examples

```ignore
# use datafusion::prelude::*;
# use datafusion::error::Result;
# #[tokio::main]
# async fn main() -> Result<()> {
let ctx = SessionContext::new();
let df = ctx.read_csv("tests/data/regex.csv", CsvReadOptions::new()).await?;

// use the regexp_replace function to replace substring(s) without flags
let df = df.with_column(
    "a",
    regexp_replace(vec![col("values"), col("patterns"), col("replacement")])
)?;
// use the regexp_replace function to replace substring(s) with flags
let df = df.with_column(
    "b",
    regexp_replace(vec![col("values"), col("patterns"), col("replacement"), col("flags")]),
)?;

// literals can be used as well
let df = df.with_column(
    "c",
    regexp_replace(vec![lit("foobarbequebaz"), lit("(bar)(beque)"), lit(r"\2")]),
)?;

df.show().await?;

# Ok(())
# }
```

```rust
fn regexp_replace<'a, T, U>(string_array: U, pattern_array: U, replacement_array: U, flags_array: Option<U>) -> datafusion_common::Result<arrow::array::ArrayRef>
```



