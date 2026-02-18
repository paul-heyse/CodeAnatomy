**datafusion_common > file_options > json_writer**

# Module: file_options::json_writer

## Contents

**Structs**

- [`JsonWriterOptions`](#jsonwriteroptions) - Options for writing JSON files

---

## datafusion_common::file_options::json_writer::JsonWriterOptions

*Struct*

Options for writing JSON files

**Fields:**
- `compression: crate::parsers::CompressionTypeVariant`
- `compression_level: Option<u32>`

**Methods:**

- `fn new(compression: CompressionTypeVariant) -> Self`
- `fn new_with_level(compression: CompressionTypeVariant, compression_level: u32) -> Self` - Create a new `JsonWriterOptions` with the specified compression and level.

**Trait Implementations:**

- **TryFrom**
  - `fn try_from(value: &JsonOptions) -> Result<Self>`
- **Clone**
  - `fn clone(self: &Self) -> JsonWriterOptions`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



