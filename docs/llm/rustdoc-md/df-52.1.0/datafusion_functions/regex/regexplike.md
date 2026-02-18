**datafusion_functions > regex > regexplike**

# Module: regex::regexplike

## Contents

**Structs**

- [`RegexpLikeFunc`](#regexplikefunc)

**Functions**

- [`regexp_like`](#regexp_like) - Tests a string using a regular expression returning true if at

---

## datafusion_functions::regex::regexplike::RegexpLikeFunc

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn simplify(self: &Self, args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &RegexpLikeFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`



## datafusion_functions::regex::regexplike::regexp_like

*Function*

Tests a string using a regular expression returning true if at
least one match, false otherwise.

The full list of supported features and syntax can be found at
<https://docs.rs/regex/latest/regex/#syntax>

Supported flags can be found at
<https://docs.rs/regex/latest/regex/#grouping-and-flags>

# Examples

```ignore
# use datafusion::prelude::*;
# use datafusion::error::Result;
# #[tokio::main]
# async fn main() -> Result<()> {
let ctx = SessionContext::new();
let df = ctx.read_csv("tests/data/regex.csv", CsvReadOptions::new()).await?;

// use the regexp_like function to test col 'values',
// against patterns in col 'patterns' without flags
let df = df.with_column(
    "a",
    regexp_like(vec![col("values"), col("patterns")])
)?;
// use the regexp_like function to test col 'values',
// against patterns in col 'patterns' with flags
let df = df.with_column(
    "b",
    regexp_like(vec![col("values"), col("patterns"), col("flags")])
)?;
// literals can be used as well with dataframe calls
let df = df.with_column(
    "c",
    regexp_like(vec![lit("foobarbequebaz"), lit("(bar)(beque)")])
)?;

df.show().await?;

# Ok(())
# }
```

```rust
fn regexp_like(args: &[arrow::array::ArrayRef]) -> datafusion_common::Result<arrow::array::ArrayRef>
```



