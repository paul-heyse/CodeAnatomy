**arrow > util > string_writer**

# Module: util::string_writer

## Contents

**Structs**

- [`StringWriter`](#stringwriter) - A writer that allows writing to a `String`

---

## arrow::util::string_writer::StringWriter

*Struct*

A writer that allows writing to a `String`
like an `std::io::Write` object.

**Methods:**

- `fn new() -> Self` - Create a new `StringWriter`

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> std::fmt::Result`
- **Default**
  - `fn default() -> StringWriter`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Write**
  - `fn write(self: & mut Self, buf: &[u8]) -> Result<usize>`
  - `fn flush(self: & mut Self) -> Result<()>`



