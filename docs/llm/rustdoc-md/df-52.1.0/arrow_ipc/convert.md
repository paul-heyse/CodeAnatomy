**arrow_ipc > convert**

# Module: convert

## Contents

**Structs**

- [`IpcSchemaEncoder`](#ipcschemaencoder) - Low level Arrow [Schema] to IPC bytes converter
- [`MessageBuffer`](#messagebuffer) - An owned container for a validated [`Message`]

**Functions**

- [`fb_to_schema`](#fb_to_schema) - Deserialize an ipc [crate::Schema`] from flat buffers to an arrow [Schema].
- [`metadata_to_fb`](#metadata_to_fb) - Push a key-value metadata into a FlatBufferBuilder and return [WIPOffset]
- [`schema_to_fb_offset`](#schema_to_fb_offset) - Adds a [Schema] to a flatbuffer and returns the offset
- [`try_schema_from_flatbuffer_bytes`](#try_schema_from_flatbuffer_bytes) - Try deserialize flat buffer format bytes into a schema
- [`try_schema_from_ipc_buffer`](#try_schema_from_ipc_buffer) - Try deserialize the IPC format bytes into a schema

---

## arrow_ipc::convert::IpcSchemaEncoder

*Struct*

Low level Arrow [Schema] to IPC bytes converter

See also [`fb_to_schema`] for the reverse operation

# Example
```
# use arrow_ipc::convert::{fb_to_schema, IpcSchemaEncoder};
# use arrow_ipc::root_as_schema;
# use arrow_ipc::writer::DictionaryTracker;
# use arrow_schema::{DataType, Field, Schema};
// given an arrow schema to serialize
let schema = Schema::new(vec![
   Field::new("a", DataType::Int32, false),
]);

// Use a dictionary tracker to track dictionary id if needed
 let mut dictionary_tracker = DictionaryTracker::new(true);
// create a FlatBuffersBuilder that contains the encoded bytes
 let fb = IpcSchemaEncoder::new()
   .with_dictionary_tracker(&mut dictionary_tracker)
   .schema_to_fb(&schema);

// the bytes are in `fb.finished_data()`
let ipc_bytes = fb.finished_data();

 // convert the IPC bytes back to an Arrow schema
 let ipc_schema = root_as_schema(ipc_bytes).unwrap();
 let schema2 = fb_to_schema(ipc_schema);
assert_eq!(schema, schema2);
```

**Generic Parameters:**
- 'a

**Methods:**

- `fn new() -> IpcSchemaEncoder<'a>` - Create a new schema encoder
- `fn with_dictionary_tracker(self: Self, dictionary_tracker: &'a  mut DictionaryTracker) -> Self` - Specify a dictionary tracker to use
- `fn schema_to_fb<'b>(self: & mut Self, schema: &Schema) -> FlatBufferBuilder<'b>` - Serialize a schema in IPC format, returning a completed [`FlatBufferBuilder`]
- `fn schema_to_fb_offset<'b>(self: & mut Self, fbb: & mut FlatBufferBuilder<'b>, schema: &Schema) -> WIPOffset<crate::Schema<'b>>` - Serialize a schema to an in progress [`FlatBufferBuilder`], returning the in progress offset.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> Self`



## arrow_ipc::convert::MessageBuffer

*Struct*

An owned container for a validated [`Message`]

Safely decoding a flatbuffer requires validating the various embedded offsets,
see [`Verifier`]. This is a potentially expensive operation, and it is therefore desirable
to only do this once. [`crate::root_as_message`] performs this validation on construction,
however, it returns a [`Message`] borrowing the provided byte slice. This prevents
storing this [`Message`] in the same data structure that owns the buffer, as this
would require self-referential borrows.

[`MessageBuffer`] solves this problem by providing a safe API for a [`Message`]
without a lifetime bound.

**Tuple Struct**: `()`

**Methods:**

- `fn try_new(buf: Buffer) -> Result<Self, ArrowError>` - Try to create a [`MessageBuffer`] from the provided [`Buffer`]
- `fn as_ref(self: &Self) -> Message` - Return the [`Message`]

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut Formatter) -> std::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> MessageBuffer`



## arrow_ipc::convert::fb_to_schema

*Function*

Deserialize an ipc [crate::Schema`] from flat buffers to an arrow [Schema].

```rust
fn fb_to_schema(fb: crate::Schema) -> Schema
```



## arrow_ipc::convert::metadata_to_fb

*Function*

Push a key-value metadata into a FlatBufferBuilder and return [WIPOffset]

```rust
fn metadata_to_fb<'a>(fbb: & mut flatbuffers::FlatBufferBuilder<'a>, metadata: &std::collections::HashMap<String, String>) -> flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<crate::KeyValue<'a>>>>
```



## arrow_ipc::convert::schema_to_fb_offset

*Function*

Adds a [Schema] to a flatbuffer and returns the offset

```rust
fn schema_to_fb_offset<'a>(fbb: & mut flatbuffers::FlatBufferBuilder<'a>, schema: &Schema) -> flatbuffers::WIPOffset<crate::Schema<'a>>
```



## arrow_ipc::convert::try_schema_from_flatbuffer_bytes

*Function*

Try deserialize flat buffer format bytes into a schema

```rust
fn try_schema_from_flatbuffer_bytes(bytes: &[u8]) -> Result<Schema, ArrowError>
```



## arrow_ipc::convert::try_schema_from_ipc_buffer

*Function*

Try deserialize the IPC format bytes into a schema

```rust
fn try_schema_from_ipc_buffer(buffer: &[u8]) -> Result<Schema, ArrowError>
```



