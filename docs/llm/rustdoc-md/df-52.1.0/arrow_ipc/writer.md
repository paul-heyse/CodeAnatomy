**arrow_ipc > writer**

# Module: writer

## Contents

**Structs**

- [`DictionaryTracker`](#dictionarytracker) - Keeps track of dictionaries that have been written, to avoid emitting the same dictionary
- [`EncodedData`](#encodeddata) - Stores the encoded data, which is an crate::Message, and optional Arrow data
- [`FileWriter`](#filewriter) - Arrow File Writer
- [`IpcDataGenerator`](#ipcdatagenerator) - Handles low level details of encoding [`Array`] and [`Schema`] into the
- [`IpcWriteOptions`](#ipcwriteoptions) - IPC write options used to control the behaviour of the [`IpcDataGenerator`]
- [`StreamWriter`](#streamwriter) - Arrow Stream Writer

**Enums**

- [`DictionaryHandling`](#dictionaryhandling) - Controls how dictionaries are handled in Arrow IPC messages
- [`DictionaryUpdate`](#dictionaryupdate) - Describes what kind of update took place after a call to [`DictionaryTracker::insert`].

**Functions**

- [`write_message`](#write_message) - Write a message's IPC data and buffers, returning metadata and buffer data lengths written

---

## arrow_ipc::writer::DictionaryHandling

*Enum*

Controls how dictionaries are handled in Arrow IPC messages

**Variants:**
- `Resend` - Send the entire dictionary every time it is encountered (default)
- `Delta` - Send only new dictionary values since the last batch (delta encoding)

**Traits:** Eq, Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> DictionaryHandling`
- **Clone**
  - `fn clone(self: &Self) -> DictionaryHandling`
- **PartialEq**
  - `fn eq(self: &Self, other: &DictionaryHandling) -> bool`



## arrow_ipc::writer::DictionaryTracker

*Struct*

Keeps track of dictionaries that have been written, to avoid emitting the same dictionary
multiple times.

Can optionally error if an update to an existing dictionary is attempted, which
isn't allowed in the `FileWriter`.

**Methods:**

- `fn new(error_on_replacement: bool) -> Self` - Create a new [`DictionaryTracker`].
- `fn next_dict_id(self: & mut Self) -> i64` - Record and return the next dictionary ID.
- `fn dict_id(self: & mut Self) -> &[i64]` - Return the sequence of dictionary IDs in the order they should be observed while
- `fn insert(self: & mut Self, dict_id: i64, column: &ArrayRef) -> Result<bool, ArrowError>` - Keep track of the dictionary with the given ID and values. Behavior:
- `fn insert_column(self: & mut Self, dict_id: i64, column: &ArrayRef, dict_handling: DictionaryHandling) -> Result<DictionaryUpdate, ArrowError>` - Keep track of the dictionary with the given ID and values. The return

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## arrow_ipc::writer::DictionaryUpdate

*Enum*

Describes what kind of update took place after a call to [`DictionaryTracker::insert`].

**Variants:**
- `None` - No dictionary was written, the dictionary was identical to what was already
- `New` - No dictionary was present in the tracker
- `Replaced` - Dictionary was replaced with the new data
- `Delta(arrow_data::ArrayData)` - Dictionary was updated, ArrayData is the delta between old and new

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> DictionaryUpdate`



## arrow_ipc::writer::EncodedData

*Struct*

Stores the encoded data, which is an crate::Message, and optional Arrow data

**Fields:**
- `ipc_message: Vec<u8>` - An encoded crate::Message
- `arrow_data: Vec<u8>` - Arrow buffers to be written, should be an empty vec for schema messages



## arrow_ipc::writer::FileWriter

*Struct*

Arrow File Writer

Writes Arrow [`RecordBatch`]es in the [IPC File Format].

# See Also

* [`StreamWriter`] for writing IPC Streams

# Example
```
# use arrow_array::record_batch;
# use arrow_ipc::writer::FileWriter;
# let mut file = vec![]; // mimic a file for the example
let batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
// create a new writer, the schema must be known in advance
let mut writer = FileWriter::try_new(&mut file, &batch.schema()).unwrap();
// write each batch to the underlying writer
writer.write(&batch).unwrap();
// When all batches are written, call finish to flush all buffers
writer.finish().unwrap();
```
[IPC File Format]: https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format

**Generic Parameters:**
- W

**Methods:**

- `fn try_new_buffered(writer: W, schema: &Schema) -> Result<Self, ArrowError>` - Try to create a new file writer with the writer wrapped in a BufWriter.
- `fn try_new(writer: W, schema: &Schema) -> Result<Self, ArrowError>` - Try to create a new writer, with the schema written as part of the header
- `fn try_new_with_options(writer: W, schema: &Schema, write_options: IpcWriteOptions) -> Result<Self, ArrowError>` - Try to create a new writer with IpcWriteOptions
- `fn write_metadata<impl Into<String>, impl Into<String>>(self: & mut Self, key: impl Trait, value: impl Trait)` - Adds a key-value pair to the [FileWriter]'s custom metadata
- `fn write(self: & mut Self, batch: &RecordBatch) -> Result<(), ArrowError>` - Write a record batch to the file
- `fn finish(self: & mut Self) -> Result<(), ArrowError>` - Write footer and closing tag, then mark the writer as done
- `fn schema(self: &Self) -> &SchemaRef` - Returns the arrow [`SchemaRef`] for this arrow file.
- `fn get_ref(self: &Self) -> &W` - Gets a reference to the underlying writer.
- `fn get_mut(self: & mut Self) -> & mut W` - Gets a mutable reference to the underlying writer.
- `fn flush(self: & mut Self) -> Result<(), ArrowError>` - Flush the underlying writer.
- `fn into_inner(self: Self) -> Result<W, ArrowError>` - Unwraps the underlying writer.

**Trait Implementations:**

- **RecordBatchWriter**
  - `fn write(self: & mut Self, batch: &RecordBatch) -> Result<(), ArrowError>`
  - `fn close(self: Self) -> Result<(), ArrowError>`



## arrow_ipc::writer::IpcDataGenerator

*Struct*

Handles low level details of encoding [`Array`] and [`Schema`] into the
[Arrow IPC Format].

# Example
```
# fn run() {
# use std::sync::Arc;
# use arrow_array::UInt64Array;
# use arrow_array::RecordBatch;
# use arrow_ipc::writer::{CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions};

// Create a record batch
let batch = RecordBatch::try_from_iter(vec![
 ("col2", Arc::new(UInt64Array::from_iter([10, 23, 33])) as _)
]).unwrap();

// Error of dictionary ids are replaced.
let error_on_replacement = true;
let options = IpcWriteOptions::default();
let mut dictionary_tracker = DictionaryTracker::new(error_on_replacement);

let mut compression_context = CompressionContext::default();

// encode the batch into zero or more encoded dictionaries
// and the data for the actual array.
let data_gen = IpcDataGenerator::default();
let (encoded_dictionaries, encoded_message) = data_gen
  .encode(&batch, &mut dictionary_tracker, &options, &mut compression_context)
  .unwrap();
# }
```

[Arrow IPC Format]: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc

**Methods:**

- `fn schema_to_bytes_with_dictionary_tracker(self: &Self, schema: &Schema, dictionary_tracker: & mut DictionaryTracker, write_options: &IpcWriteOptions) -> EncodedData` - Converts a schema to an IPC message along with `dictionary_tracker`
- `fn encode(self: &Self, batch: &RecordBatch, dictionary_tracker: & mut DictionaryTracker, write_options: &IpcWriteOptions, compression_context: & mut CompressionContext) -> Result<(Vec<EncodedData>, EncodedData), ArrowError>` - Encodes a batch to a number of [EncodedData] items (dictionary batches + the record batch).
- `fn encoded_batch(self: &Self, batch: &RecordBatch, dictionary_tracker: & mut DictionaryTracker, write_options: &IpcWriteOptions) -> Result<(Vec<EncodedData>, EncodedData), ArrowError>` - Encodes a batch to a number of [EncodedData] items (dictionary batches + the record batch).

**Trait Implementations:**

- **Default**
  - `fn default() -> IpcDataGenerator`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## arrow_ipc::writer::IpcWriteOptions

*Struct*

IPC write options used to control the behaviour of the [`IpcDataGenerator`]

**Methods:**

- `fn try_with_compression(self: Self, batch_compression_type: Option<crate::CompressionType>) -> Result<Self, ArrowError>` - Configures compression when writing IPC files.
- `fn try_new(alignment: usize, write_legacy_ipc_format: bool, metadata_version: crate::MetadataVersion) -> Result<Self, ArrowError>` - Try to create IpcWriteOptions, checking for incompatible settings
- `fn with_dictionary_handling(self: Self, dictionary_handling: DictionaryHandling) -> Self` - Configure how dictionaries are handled in IPC messages

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> IpcWriteOptions`
- **Default**
  - `fn default() -> Self`



## arrow_ipc::writer::StreamWriter

*Struct*

Arrow Stream Writer

Writes Arrow [`RecordBatch`]es to bytes using the [IPC Streaming Format].

# See Also

* [`FileWriter`] for writing IPC Files

# Example - Basic usage
```
# use arrow_array::record_batch;
# use arrow_ipc::writer::StreamWriter;
# let mut stream = vec![]; // mimic a stream for the example
let batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
// create a new writer, the schema must be known in advance
let mut writer = StreamWriter::try_new(&mut stream, &batch.schema()).unwrap();
// write each batch to the underlying stream
writer.write(&batch).unwrap();
// When all batches are written, call finish to flush all buffers
writer.finish().unwrap();
```
# Example - Efficient delta dictionaries
```
# use arrow_array::record_batch;
# use arrow_ipc::writer::{StreamWriter, IpcWriteOptions};
# use arrow_ipc::writer::DictionaryHandling;
# use arrow_schema::{DataType, Field, Schema, SchemaRef};
# use arrow_array::{
#    builder::StringDictionaryBuilder, types::Int32Type, Array, ArrayRef, DictionaryArray,
#    RecordBatch, StringArray,
# };
# use std::sync::Arc;

let schema = Arc::new(Schema::new(vec![Field::new(
   "col1",
   DataType::Dictionary(Box::from(DataType::Int32), Box::from(DataType::Utf8)),
   true,
)]));

let mut builder = StringDictionaryBuilder::<arrow_array::types::Int32Type>::new();

// `finish_preserve_values` will keep the dictionary values along with their
// key assignments so that they can be re-used in the next batch.
builder.append("a").unwrap();
builder.append("b").unwrap();
let array1 = builder.finish_preserve_values();
let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array1) as ArrayRef]).unwrap();

// In this batch, 'a' will have the same dictionary key as 'a' in the previous batch,
// and 'd' will take the next available key.
builder.append("a").unwrap();
builder.append("d").unwrap();
let array2 = builder.finish_preserve_values();
let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array2) as ArrayRef]).unwrap();

let mut stream = vec![];
// You must set `.with_dictionary_handling(DictionaryHandling::Delta)` to
// enable delta dictionaries in the writer
let options = IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);
let mut writer = StreamWriter::try_new(&mut stream, &schema).unwrap();

// When writing the first batch, a dictionary message with 'a' and 'b' will be written
// prior to the record batch.
writer.write(&batch1).unwrap();
// With the second batch only a delta dictionary with 'd' will be written
// prior to the record batch. This is only possible with `finish_preserve_values`.
// Without it, 'a' and 'd' in this batch would have different keys than the
// first batch and so we'd have to send a replacement dictionary with new keys
// for both.
writer.write(&batch2).unwrap();
writer.finish().unwrap();
```
[IPC Streaming Format]: https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format

**Generic Parameters:**
- W

**Methods:**

- `fn try_new_buffered(writer: W, schema: &Schema) -> Result<Self, ArrowError>` - Try to create a new stream writer with the writer wrapped in a BufWriter.
- `fn try_new(writer: W, schema: &Schema) -> Result<Self, ArrowError>` - Try to create a new writer, with the schema written as part of the header.
- `fn try_new_with_options(writer: W, schema: &Schema, write_options: IpcWriteOptions) -> Result<Self, ArrowError>` - Try to create a new writer with [`IpcWriteOptions`].
- `fn write(self: & mut Self, batch: &RecordBatch) -> Result<(), ArrowError>` - Write a record batch to the stream
- `fn finish(self: & mut Self) -> Result<(), ArrowError>` - Write continuation bytes, and mark the stream as done
- `fn get_ref(self: &Self) -> &W` - Gets a reference to the underlying writer.
- `fn get_mut(self: & mut Self) -> & mut W` - Gets a mutable reference to the underlying writer.
- `fn flush(self: & mut Self) -> Result<(), ArrowError>` - Flush the underlying writer.
- `fn into_inner(self: Self) -> Result<W, ArrowError>` - Unwraps the the underlying writer.

**Trait Implementations:**

- **RecordBatchWriter**
  - `fn write(self: & mut Self, batch: &RecordBatch) -> Result<(), ArrowError>`
  - `fn close(self: Self) -> Result<(), ArrowError>`



## arrow_ipc::writer::write_message

*Function*

Write a message's IPC data and buffers, returning metadata and buffer data lengths written

```rust
fn write_message<W>(writer: W, encoded: EncodedData, write_options: &IpcWriteOptions) -> Result<(usize, usize), ArrowError>
```



