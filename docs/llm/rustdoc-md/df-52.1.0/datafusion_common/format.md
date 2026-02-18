**datafusion_common > format**

# Module: format

## Contents

**Enums**

- [`ExplainAnalyzeLevel`](#explainanalyzelevel) - Verbosity levels controlling how `EXPLAIN ANALYZE` renders metrics
- [`ExplainFormat`](#explainformat) - Output formats for controlling for Explain plans

**Constants**

- [`DEFAULT_CAST_OPTIONS`](#default_cast_options) - The default [`CastOptions`] to use within DataFusion
- [`DEFAULT_FORMAT_OPTIONS`](#default_format_options) - The default [`FormatOptions`] to use within DataFusion

---

## datafusion_common::format::DEFAULT_CAST_OPTIONS

*Constant*: `arrow::compute::CastOptions<'static>`

The default [`CastOptions`] to use within DataFusion



## datafusion_common::format::DEFAULT_FORMAT_OPTIONS

*Constant*: `arrow::util::display::FormatOptions<'static>`

The default [`FormatOptions`] to use within DataFusion
Also see [`crate::config::FormatOptions`]



## datafusion_common::format::ExplainAnalyzeLevel

*Enum*

Verbosity levels controlling how `EXPLAIN ANALYZE` renders metrics

**Variants:**
- `Summary` - Show a compact view containing high-level metrics
- `Dev` - Show a developer-focused view with per-operator details

**Traits:** Eq, Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ExplainAnalyzeLevel) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **FromStr**
  - `fn from_str(level: &str) -> Result<Self, <Self as >::Err>`
- **Clone**
  - `fn clone(self: &Self) -> ExplainAnalyzeLevel`
- **ConfigField**
  - `fn visit<V>(self: &Self, v: & mut V, key: &str, description: &'static str)`
  - `fn set(self: & mut Self, _: &str, value: &str) -> Result<()>`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`



## datafusion_common::format::ExplainFormat

*Enum*

Output formats for controlling for Explain plans

**Variants:**
- `Indent` - Indent mode
- `Tree` - Tree mode
- `PostgresJSON` - Postgres Json mode
- `Graphviz` - Graphviz mode

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> ExplainFormat`
- **PartialEq**
  - `fn eq(self: &Self, other: &ExplainFormat) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **FromStr**
  - `fn from_str(format: &str) -> Result<Self, <Self as >::Err>`
- **ConfigField**
  - `fn visit<V>(self: &Self, v: & mut V, key: &str, description: &'static str)`
  - `fn set(self: & mut Self, _: &str, value: &str) -> Result<()>`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



