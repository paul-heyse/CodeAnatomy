**arrow_ipc > reader > stream**

# Module: reader::stream

## Contents

**Structs**

- [`StreamDecoder`](#streamdecoder) - A low-level interface for reading [`RecordBatch`] data from a stream of bytes

---

## arrow_ipc::reader::stream::StreamDecoder

*Struct*

A low-level interface for reading [`RecordBatch`] data from a stream of bytes

See [StreamReader](crate::reader::StreamReader) for a higher-level interface

**Methods:**

- `fn new() -> Self` - Create a new [`StreamDecoder`]
- `fn with_require_alignment(self: Self, require_alignment: bool) -> Self` - Specifies whether or not array data in input buffers is required to be properly aligned.
- `fn schema(self: &Self) -> Option<SchemaRef>` - Return the schema if decoded, else None.
- `fn decode(self: & mut Self, buffer: & mut Buffer) -> Result<Option<RecordBatch>, ArrowError>` - Try to read the next [`RecordBatch`] from the provided [`Buffer`]
- `fn finish(self: & mut Self) -> Result<(), ArrowError>` - Signal the end of stream

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> StreamDecoder`



