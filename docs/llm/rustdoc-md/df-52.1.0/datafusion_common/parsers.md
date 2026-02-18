**datafusion_common > parsers**

# Module: parsers

## Contents

**Enums**

- [`CompressionTypeVariant`](#compressiontypevariant) - Readable file compression type

---

## datafusion_common::parsers::CompressionTypeVariant

*Enum*

Readable file compression type

**Variants:**
- `GZIP` - Gzip-ed file
- `BZIP2` - Bzip2-ed file
- `XZ` - Xz-ed file (liblzma)
- `ZSTD` - Zstd-ed file,
- `UNCOMPRESSED` - Uncompressed file

**Methods:**

- `fn is_compressed(self: &Self) -> bool`

**Traits:** Eq, Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &CompressionTypeVariant) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **FromStr**
  - `fn from_str(s: &str) -> Result<Self, <Self as >::Err>`
- **Clone**
  - `fn clone(self: &Self) -> CompressionTypeVariant`
- **ConfigField**
  - `fn visit<V>(self: &Self, v: & mut V, key: &str, description: &'static str)`
  - `fn set(self: & mut Self, _: &str, value: &str) -> Result<()>`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`



