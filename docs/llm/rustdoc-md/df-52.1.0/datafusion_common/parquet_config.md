**datafusion_common > parquet_config**

# Module: parquet_config

## Contents

**Enums**

- [`DFParquetWriterVersion`](#dfparquetwriterversion) - Parquet writer version options for controlling the Parquet file format version

---

## datafusion_common::parquet_config::DFParquetWriterVersion

*Enum*

Parquet writer version options for controlling the Parquet file format version

This enum validates parquet writer version values at configuration time,
ensuring only valid versions ("1.0" or "2.0") can be set via `SET` commands
or proto deserialization.

**Variants:**
- `V1_0` - Parquet format version 1.0
- `V2_0` - Parquet format version 2.0

**Traits:** Eq, Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> DFParquetWriterVersion`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(version: parquet::file::properties::WriterVersion) -> Self`
- **PartialEq**
  - `fn eq(self: &Self, other: &DFParquetWriterVersion) -> bool`
- **ConfigField**
  - `fn visit<V>(self: &Self, v: & mut V, key: &str, description: &'static str)`
  - `fn set(self: & mut Self, _: &str, value: &str) -> Result<()>`
- **Default**
  - `fn default() -> DFParquetWriterVersion`
- **FromStr**
  - `fn from_str(s: &str) -> Result<Self, <Self as >::Err>`



