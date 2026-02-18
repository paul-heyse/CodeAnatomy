**datafusion_datasource > decoder**

# Module: decoder

## Contents

**Structs**

- [`DecoderDeserializer`](#decoderdeserializer) - A generic, decoder-based deserialization scheme for processing encoded data.

**Enums**

- [`DeserializerOutput`](#deserializeroutput) - Possible outputs of a [`BatchDeserializer`].

**Functions**

- [`deserialize_stream`](#deserialize_stream) - Deserializes a stream of bytes into a stream of [`RecordBatch`] objects using the

**Traits**

- [`BatchDeserializer`](#batchdeserializer) - Trait defining a scheme for deserializing byte streams into structured data.
- [`Decoder`](#decoder) - A general interface for decoders such as [`arrow::json::reader::Decoder`] and

---

## datafusion_datasource::decoder::BatchDeserializer

*Trait*

Trait defining a scheme for deserializing byte streams into structured data.
Implementors of this trait are responsible for converting raw bytes into
`RecordBatch` objects.

**Methods:**

- `digest`: Feeds a message for deserialization, updating the internal state of
- `next`: Attempts to deserialize any pending messages and returns a
- `finish`: Informs the deserializer that no more messages will be provided for



## datafusion_datasource::decoder::Decoder

*Trait*

A general interface for decoders such as [`arrow::json::reader::Decoder`] and
[`arrow::csv::reader::Decoder`]. Defines an interface similar to
[`Decoder::decode`] and [`Decoder::flush`] methods, but also includes
a method to check if the decoder can flush early. Intended to be used in
conjunction with [`DecoderDeserializer`].

[`arrow::json::reader::Decoder`]: ::arrow::json::reader::Decoder
[`arrow::csv::reader::Decoder`]: ::arrow::csv::reader::Decoder
[`Decoder::decode`]: ::arrow::json::reader::Decoder::decode
[`Decoder::flush`]: ::arrow::json::reader::Decoder::flush

**Methods:**

- `decode`: See [`arrow::json::reader::Decoder::decode`].
- `flush`: See [`arrow::json::reader::Decoder::flush`].
- `can_flush_early`: Whether the decoder can flush early in its current state.



## datafusion_datasource::decoder::DecoderDeserializer

*Struct*

A generic, decoder-based deserialization scheme for processing encoded data.

This struct is responsible for converting a stream of bytes, which represent
encoded data, into a stream of `RecordBatch` objects, following the specified
schema and formatting options. It also handles any buffering necessary to satisfy
the `Decoder` interface.

**Generic Parameters:**
- T

**Methods:**

- `fn new(decoder: T) -> Self` - Creates a new `DecoderDeserializer` with the provided decoder.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **BatchDeserializer**
  - `fn digest(self: & mut Self, message: Bytes) -> usize`
  - `fn next(self: & mut Self) -> Result<DeserializerOutput, ArrowError>`
  - `fn finish(self: & mut Self)`



## datafusion_datasource::decoder::DeserializerOutput

*Enum*

Possible outputs of a [`BatchDeserializer`].

**Variants:**
- `RecordBatch(::arrow::array::RecordBatch)` - A successfully deserialized [`RecordBatch`].
- `RequiresMoreData` - The deserializer requires more data to make progress.
- `InputExhausted` - The input data has been exhausted.

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &DeserializerOutput) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_datasource::decoder::deserialize_stream

*Function*

Deserializes a stream of bytes into a stream of [`RecordBatch`] objects using the
provided deserializer.

Returns a boxed stream of `Result<RecordBatch, ArrowError>`. The stream yields [`RecordBatch`]
objects as they are produced by the deserializer, or an [`ArrowError`] if an error
occurs while polling the input or deserializing.

```rust
fn deserialize_stream<'a, impl Stream<Item = Result<Bytes>> + Unpin + Send + 'a, impl BatchDeserializer<Bytes> + 'a>(input: impl Trait, deserializer: impl Trait) -> futures::stream::BoxStream<'a, datafusion_common::Result<::arrow::array::RecordBatch, arrow::error::ArrowError>>
```



