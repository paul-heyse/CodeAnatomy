**arrow_ipc > reader**

# Module: reader

## Contents

**Structs**

- [`FileDecoder`](#filedecoder) - A low-level, push-based interface for reading an IPC file
- [`FileReader`](#filereader) - Arrow File Reader
- [`FileReaderBuilder`](#filereaderbuilder) - Build an Arrow [`FileReader`] with custom options.
- [`RecordBatchDecoder`](#recordbatchdecoder) - State for decoding Arrow arrays from an [IPC RecordBatch] structure to
- [`StreamReader`](#streamreader) - Arrow Stream Reader

**Functions**

- [`read_dictionary`](#read_dictionary) - Read the dictionary from the buffer and provided metadata,
- [`read_footer_length`](#read_footer_length) - Read the footer length from the last 10 bytes of an Arrow IPC file
- [`read_record_batch`](#read_record_batch) - Creates a record batch from binary data using the `crate::RecordBatch` indexes and the `Schema`.

---

## arrow_ipc::reader::FileDecoder

*Struct*

A low-level, push-based interface for reading an IPC file

For a higher-level interface see [`FileReader`]

For an example of using this API with `mmap` see the [`zero_copy_ipc`] example.

[`zero_copy_ipc`]: https://github.com/apache/arrow-rs/blob/main/arrow/examples/zero_copy_ipc.rs

```
# use std::sync::Arc;
# use arrow_array::*;
# use arrow_array::types::Int32Type;
# use arrow_buffer::Buffer;
# use arrow_ipc::convert::fb_to_schema;
# use arrow_ipc::reader::{FileDecoder, read_footer_length};
# use arrow_ipc::root_as_footer;
# use arrow_ipc::writer::FileWriter;
// Write an IPC file

let batch = RecordBatch::try_from_iter([
    ("a", Arc::new(Int32Array::from(vec![1, 2, 3])) as _),
    ("b", Arc::new(Int32Array::from(vec![1, 2, 3])) as _),
    ("c", Arc::new(DictionaryArray::<Int32Type>::from_iter(["hello", "hello", "world"])) as _),
]).unwrap();

let schema = batch.schema();

let mut out = Vec::with_capacity(1024);
let mut writer = FileWriter::try_new(&mut out, schema.as_ref()).unwrap();
writer.write(&batch).unwrap();
writer.finish().unwrap();

drop(writer);

// Read IPC file

let buffer = Buffer::from_vec(out);
let trailer_start = buffer.len() - 10;
let footer_len = read_footer_length(buffer[trailer_start..].try_into().unwrap()).unwrap();
let footer = root_as_footer(&buffer[trailer_start - footer_len..trailer_start]).unwrap();

let back = fb_to_schema(footer.schema().unwrap());
assert_eq!(&back, schema.as_ref());

let mut decoder = FileDecoder::new(schema, footer.version());

// Read dictionaries
for block in footer.dictionaries().iter().flatten() {
    let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
    let data = buffer.slice_with_length(block.offset() as _, block_len);
    decoder.read_dictionary(&block, &data).unwrap();
}

// Read record batch
let batches = footer.recordBatches().unwrap();
assert_eq!(batches.len(), 1); // Only wrote a single batch

let block = batches.get(0);
let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
let data = buffer.slice_with_length(block.offset() as _, block_len);
let back = decoder.read_record_batch(block, &data).unwrap().unwrap();

assert_eq!(batch, back);
```

**Methods:**

- `fn new(schema: SchemaRef, version: MetadataVersion) -> Self` - Create a new [`FileDecoder`] with the given schema and version
- `fn with_projection(self: Self, projection: Vec<usize>) -> Self` - Specify a projection
- `fn with_require_alignment(self: Self, require_alignment: bool) -> Self` - Specifies if the array data in input buffers is required to be properly aligned.
- `fn with_skip_validation(self: Self, skip_validation: bool) -> Self` - Specifies if validation should be skipped when reading data (defaults to `false`)
- `fn read_dictionary(self: & mut Self, block: &Block, buf: &Buffer) -> Result<(), ArrowError>` - Read the dictionary with the given block and data buffer
- `fn read_record_batch(self: &Self, block: &Block, buf: &Buffer) -> Result<Option<RecordBatch>, ArrowError>` - Read the RecordBatch with the given block and data buffer

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## arrow_ipc::reader::FileReader

*Struct*

Arrow File Reader

Reads Arrow [`RecordBatch`]es from bytes in the [IPC File Format],
providing random access to the record batches.

# See Also

* [`Self::set_index`] for random access
* [`StreamReader`] for reading streaming data

# Example: Reading from a `File`
```
# use std::io::Cursor;
use arrow_array::record_batch;
# use arrow_ipc::reader::FileReader;
# use arrow_ipc::writer::FileWriter;
# let batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
# let mut file = vec![]; // mimic a stream for the example
# {
#  let mut writer = FileWriter::try_new(&mut file, &batch.schema()).unwrap();
#  writer.write(&batch).unwrap();
#  writer.write(&batch).unwrap();
#  writer.finish().unwrap();
# }
# let mut file = Cursor::new(&file);
let projection = None; // read all columns
let mut reader = FileReader::try_new(&mut file, projection).unwrap();
// Position the reader to the second batch
reader.set_index(1).unwrap();
// read batches from the reader using the Iterator trait
let mut num_rows = 0;
for batch in reader {
   let batch = batch.unwrap();
   num_rows += batch.num_rows();
}
assert_eq!(num_rows, 3);
```
# Example: Reading from `mmap`ed file

For an example creating Arrays without copying using  memory mapped (`mmap`)
files see the [`zero_copy_ipc`] example.

[IPC File Format]: https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format
[`zero_copy_ipc`]: https://github.com/apache/arrow-rs/blob/main/arrow/examples/zero_copy_ipc.rs

**Generic Parameters:**
- R

**Methods:**

- `fn try_new_buffered(reader: R, projection: Option<Vec<usize>>) -> Result<Self, ArrowError>` - Try to create a new file reader with the reader wrapped in a BufReader.
- `fn try_new(reader: R, projection: Option<Vec<usize>>) -> Result<Self, ArrowError>` - Try to create a new file reader.
- `fn custom_metadata(self: &Self) -> &HashMap<String, String>` - Return user defined customized metadata
- `fn num_batches(self: &Self) -> usize` - Return the number of batches in the file
- `fn schema(self: &Self) -> SchemaRef` - Return the schema of the file
- `fn set_index(self: & mut Self, index: usize) -> Result<(), ArrowError>` - See to a specific [`RecordBatch`]
- `fn get_ref(self: &Self) -> &R` - Gets a reference to the underlying reader.
- `fn get_mut(self: & mut Self) -> & mut R` - Gets a mutable reference to the underlying reader.
- `fn with_skip_validation(self: Self, skip_validation: bool) -> Self` - Specifies if validation should be skipped when reading data (defaults to `false`)

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> Result<(), fmt::Error>`
- **Iterator**
  - `fn next(self: & mut Self) -> Option<<Self as >::Item>`
- **RecordBatchReader**
  - `fn schema(self: &Self) -> SchemaRef`



## arrow_ipc::reader::FileReaderBuilder

*Struct*

Build an Arrow [`FileReader`] with custom options.

**Methods:**

- `fn new() -> Self` - Options for creating a new [`FileReader`].
- `fn with_projection(self: Self, projection: Vec<usize>) -> Self` - Optional projection for which columns to load (zero-based column indices).
- `fn with_max_footer_fb_tables(self: Self, max_footer_fb_tables: usize) -> Self` - Flatbuffers option for parsing the footer. Controls the max number of fields and
- `fn with_max_footer_fb_depth(self: Self, max_footer_fb_depth: usize) -> Self` - Flatbuffers option for parsing the footer. Controls the max depth for schemas with
- `fn build<R>(self: Self, reader: R) -> Result<FileReader<R>, ArrowError>` - Build [`FileReader`] with given reader.

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## arrow_ipc::reader::RecordBatchDecoder

*Struct*

State for decoding Arrow arrays from an [IPC RecordBatch] structure to
[`RecordBatch`]

[IPC RecordBatch]: crate::RecordBatch


**Generic Parameters:**
- 'a

**Methods:**

- `fn with_projection(self: Self, projection: Option<&'a [usize]>) -> Self` - Set the projection (default: None)
- `fn with_require_alignment(self: Self, require_alignment: bool) -> Self` - Set require_alignment (default: false)



## arrow_ipc::reader::StreamReader

*Struct*

Arrow Stream Reader

Reads Arrow [`RecordBatch`]es from bytes in the [IPC Streaming Format].

# See Also

* [`FileReader`] for random access.

# Example
```
# use arrow_array::record_batch;
# use arrow_ipc::reader::StreamReader;
# use arrow_ipc::writer::StreamWriter;
# let batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
# let mut stream = vec![]; // mimic a stream for the example
# {
#  let mut writer = StreamWriter::try_new(&mut stream, &batch.schema()).unwrap();
#  writer.write(&batch).unwrap();
#  writer.finish().unwrap();
# }
# let stream = stream.as_slice();
let projection = None; // read all columns
let mut reader = StreamReader::try_new(stream, projection).unwrap();
// read batches from the reader using the Iterator trait
let mut num_rows = 0;
for batch in reader {
   let batch = batch.unwrap();
   num_rows += batch.num_rows();
}
assert_eq!(num_rows, 3);
```

[IPC Streaming Format]: https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format

**Generic Parameters:**
- R

**Methods:**

- `fn try_new(reader: R, projection: Option<Vec<usize>>) -> Result<StreamReader<R>, ArrowError>` - Try to create a new stream reader.
- `fn try_new_unbuffered(reader: R, projection: Option<Vec<usize>>) -> Result<Self, ArrowError>` - Deprecated, use [`StreamReader::try_new`] instead.
- `fn schema(self: &Self) -> SchemaRef` - Return the schema of the stream
- `fn is_finished(self: &Self) -> bool` - Check if the stream is finished
- `fn get_ref(self: &Self) -> &R` - Gets a reference to the underlying reader.
- `fn get_mut(self: & mut Self) -> & mut R` - Gets a mutable reference to the underlying reader.
- `fn with_skip_validation(self: Self, skip_validation: bool) -> Self` - Specifies if validation should be skipped when reading data (defaults to `false`)
- `fn try_new_buffered(reader: R, projection: Option<Vec<usize>>) -> Result<Self, ArrowError>` - Try to create a new stream reader with the reader wrapped in a BufReader.

**Trait Implementations:**

- **RecordBatchReader**
  - `fn schema(self: &Self) -> SchemaRef`
- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> std::result::Result<(), fmt::Error>`
- **Iterator**
  - `fn next(self: & mut Self) -> Option<<Self as >::Item>`



## arrow_ipc::reader::read_dictionary

*Function*

Read the dictionary from the buffer and provided metadata,
updating the `dictionaries_by_id` with the resulting dictionary

```rust
fn read_dictionary(buf: &arrow_buffer::Buffer, batch: crate::DictionaryBatch, schema: &Schema, dictionaries_by_id: & mut std::collections::HashMap<i64, ArrayRef>, metadata: &crate::MetadataVersion) -> Result<(), ArrowError>
```



## arrow_ipc::reader::read_footer_length

*Function*

Read the footer length from the last 10 bytes of an Arrow IPC file

Expects a 4 byte footer length followed by `b"ARROW1"`

```rust
fn read_footer_length(buf: [u8; 10]) -> Result<usize, ArrowError>
```



## arrow_ipc::reader::read_record_batch

*Function*

Creates a record batch from binary data using the `crate::RecordBatch` indexes and the `Schema`.

If `require_alignment` is true, this function will return an error if any array data in the
input `buf` is not properly aligned.
Under the hood it will use [`arrow_data::ArrayDataBuilder::build`] to construct [`arrow_data::ArrayData`].

If `require_alignment` is false, this function will automatically allocate a new aligned buffer
and copy over the data if any array data in the input `buf` is not properly aligned.
(Properly aligned array data will remain zero-copy.)
Under the hood it will use [`arrow_data::ArrayDataBuilder::build_aligned`] to construct [`arrow_data::ArrayData`].

```rust
fn read_record_batch(buf: &arrow_buffer::Buffer, batch: crate::RecordBatch, schema: SchemaRef, dictionaries_by_id: &std::collections::HashMap<i64, ArrayRef>, projection: Option<&[usize]>, metadata: &crate::MetadataVersion) -> Result<RecordBatch, ArrowError>
```



