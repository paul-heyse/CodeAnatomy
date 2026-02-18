**arrow_ipc > gen > Message**

# Module: gen::Message

## Contents

**Structs**

- [`BodyCompression`](#bodycompression) - Optional compression for the memory buffers constituting IPC message
- [`BodyCompressionArgs`](#bodycompressionargs)
- [`BodyCompressionBuilder`](#bodycompressionbuilder)
- [`BodyCompressionMethod`](#bodycompressionmethod) - Provided for forward compatibility in case we need to support different
- [`CompressionType`](#compressiontype)
- [`DictionaryBatch`](#dictionarybatch) - For sending dictionary encoding information. Any Field can be
- [`DictionaryBatchArgs`](#dictionarybatchargs)
- [`DictionaryBatchBuilder`](#dictionarybatchbuilder)
- [`FieldNode`](#fieldnode) - ----------------------------------------------------------------------
- [`Message`](#message)
- [`MessageArgs`](#messageargs)
- [`MessageBuilder`](#messagebuilder)
- [`MessageHeader`](#messageheader) - ----------------------------------------------------------------------
- [`MessageHeaderUnionTableOffset`](#messageheaderuniontableoffset)
- [`RecordBatch`](#recordbatch) - A data header describing the shared memory layout of a "record" or "row"
- [`RecordBatchArgs`](#recordbatchargs)
- [`RecordBatchBuilder`](#recordbatchbuilder)

**Enums**

- [`BodyCompressionOffset`](#bodycompressionoffset)
- [`DictionaryBatchOffset`](#dictionarybatchoffset)
- [`MessageOffset`](#messageoffset)
- [`RecordBatchOffset`](#recordbatchoffset)

**Functions**

- [`finish_message_buffer`](#finish_message_buffer)
- [`finish_size_prefixed_message_buffer`](#finish_size_prefixed_message_buffer)
- [`root_as_message`](#root_as_message) - Verifies that a buffer of bytes contains a `Message`
- [`root_as_message_unchecked`](#root_as_message_unchecked) - Assumes, without verification, that a buffer of bytes contains a Message and returns it.
- [`root_as_message_with_opts`](#root_as_message_with_opts) - Verifies, with the given options, that a buffer of bytes
- [`size_prefixed_root_as_message`](#size_prefixed_root_as_message) - Verifies that a buffer of bytes contains a size prefixed
- [`size_prefixed_root_as_message_unchecked`](#size_prefixed_root_as_message_unchecked) - Assumes, without verification, that a buffer of bytes contains a size prefixed Message and returns it.
- [`size_prefixed_root_as_message_with_opts`](#size_prefixed_root_as_message_with_opts) - Verifies, with the given verifier options, that a buffer of

**Constants**

- [`ENUM_MAX_BODY_COMPRESSION_METHOD`](#enum_max_body_compression_method)
- [`ENUM_MAX_COMPRESSION_TYPE`](#enum_max_compression_type)
- [`ENUM_MAX_MESSAGE_HEADER`](#enum_max_message_header)
- [`ENUM_MIN_BODY_COMPRESSION_METHOD`](#enum_min_body_compression_method)
- [`ENUM_MIN_COMPRESSION_TYPE`](#enum_min_compression_type)
- [`ENUM_MIN_MESSAGE_HEADER`](#enum_min_message_header)
- [`ENUM_VALUES_BODY_COMPRESSION_METHOD`](#enum_values_body_compression_method)
- [`ENUM_VALUES_COMPRESSION_TYPE`](#enum_values_compression_type)
- [`ENUM_VALUES_MESSAGE_HEADER`](#enum_values_message_header)

---

## arrow_ipc::gen::Message::BodyCompression

*Struct*

Optional compression for the memory buffers constituting IPC message
bodies. Intended for use with RecordBatch but could be used for other
message types

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args BodyCompressionArgs) -> flatbuffers::WIPOffset<BodyCompression<'bldr>>`
- `fn codec(self: &Self) -> CompressionType` - Compressor library.
- `fn method(self: &Self) -> BodyCompressionMethod` - Indicates the way the record batch body was compressed

**Traits:** Copy

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> BodyCompression<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &BodyCompression<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::Message::BodyCompressionArgs

*Struct*

**Fields:**
- `codec: CompressionType`
- `method: BodyCompressionMethod`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Message::BodyCompressionBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_codec(self: & mut Self, codec: CompressionType)`
- `fn add_method(self: & mut Self, method: BodyCompressionMethod)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> BodyCompressionBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<BodyCompression<'a>>`



## arrow_ipc::gen::Message::BodyCompressionMethod

*Struct*

Provided for forward compatibility in case we need to support different
strategies for compressing the IPC message body (like whole-body
compression rather than buffer-level) in the future

**Tuple Struct**: `(i8)`

**Methods:**

- `fn variant_name(self: Self) -> Option<&'static str>` - Returns the variant's name or "" if unknown.

**Traits:** Eq, Copy, SimpleToVerifyInSlice

**Trait Implementations:**

- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Push**
  - `fn push(self: &Self, dst: & mut [u8], _written_len: usize)`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialEq**
  - `fn eq(self: &Self, other: &BodyCompressionMethod) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &BodyCompressionMethod) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Default**
  - `fn default() -> BodyCompressionMethod`
- **Clone**
  - `fn clone(self: &Self) -> BodyCompressionMethod`
- **EndianScalar**
  - `fn to_little_endian(self: Self) -> i8`
  - `fn from_little_endian(v: i8) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &BodyCompressionMethod) -> $crate::cmp::Ordering`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`



## arrow_ipc::gen::Message::BodyCompressionOffset

*Enum*



## arrow_ipc::gen::Message::CompressionType

*Struct*

**Tuple Struct**: `(i8)`

**Methods:**

- `fn variant_name(self: Self) -> Option<&'static str>` - Returns the variant's name or "" if unknown.

**Traits:** Eq, Copy, SimpleToVerifyInSlice

**Trait Implementations:**

- **EndianScalar**
  - `fn to_little_endian(self: Self) -> i8`
  - `fn from_little_endian(v: i8) -> Self`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Push**
  - `fn push(self: &Self, dst: & mut [u8], _written_len: usize)`
- **PartialEq**
  - `fn eq(self: &Self, other: &CompressionType) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &CompressionType) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Ord**
  - `fn cmp(self: &Self, other: &CompressionType) -> $crate::cmp::Ordering`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> CompressionType`
- **Clone**
  - `fn clone(self: &Self) -> CompressionType`



## arrow_ipc::gen::Message::DictionaryBatch

*Struct*

For sending dictionary encoding information. Any Field can be
dictionary-encoded, but in this case none of its children may be
dictionary-encoded.
There is one vector / column per dictionary, but that vector / column
may be spread across multiple dictionary batches by using the isDelta
flag

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args DictionaryBatchArgs<'args>) -> flatbuffers::WIPOffset<DictionaryBatch<'bldr>>`
- `fn id(self: &Self) -> i64`
- `fn data(self: &Self) -> Option<RecordBatch<'a>>`
- `fn isDelta(self: &Self) -> bool` - If isDelta is true the values in the dictionary are to be appended to a

**Traits:** Copy

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &DictionaryBatch<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> DictionaryBatch<'a>`



## arrow_ipc::gen::Message::DictionaryBatchArgs

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `id: i64`
- `data: Option<flatbuffers::WIPOffset<RecordBatch<'a>>>`
- `isDelta: bool`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Message::DictionaryBatchBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_id(self: & mut Self, id: i64)`
- `fn add_data(self: & mut Self, data: flatbuffers::WIPOffset<RecordBatch<'b>>)`
- `fn add_isDelta(self: & mut Self, isDelta: bool)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> DictionaryBatchBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<DictionaryBatch<'a>>`



## arrow_ipc::gen::Message::DictionaryBatchOffset

*Enum*



## arrow_ipc::gen::Message::ENUM_MAX_BODY_COMPRESSION_METHOD

*Constant*: `i8`



## arrow_ipc::gen::Message::ENUM_MAX_COMPRESSION_TYPE

*Constant*: `i8`



## arrow_ipc::gen::Message::ENUM_MAX_MESSAGE_HEADER

*Constant*: `u8`



## arrow_ipc::gen::Message::ENUM_MIN_BODY_COMPRESSION_METHOD

*Constant*: `i8`



## arrow_ipc::gen::Message::ENUM_MIN_COMPRESSION_TYPE

*Constant*: `i8`



## arrow_ipc::gen::Message::ENUM_MIN_MESSAGE_HEADER

*Constant*: `u8`



## arrow_ipc::gen::Message::ENUM_VALUES_BODY_COMPRESSION_METHOD

*Constant*: `[BodyCompressionMethod; 1]`



## arrow_ipc::gen::Message::ENUM_VALUES_COMPRESSION_TYPE

*Constant*: `[CompressionType; 2]`



## arrow_ipc::gen::Message::ENUM_VALUES_MESSAGE_HEADER

*Constant*: `[MessageHeader; 6]`



## arrow_ipc::gen::Message::FieldNode

*Struct*

----------------------------------------------------------------------
Data structures for describing a table row batch (a collection of
equal-length Arrow arrays)
Metadata about a field at some level of a nested type tree (but not
its children).

For example, a `List<Int16>` with values `[[1, 2, 3], null, [4], [5, 6], null]`
would have {length: 5, null_count: 2} for its List node, and {length: 6,
null_count: 0} for its Int16 node, as separate FieldNode structs

**Tuple Struct**: `([u8; 16])`

**Methods:**

- `fn new(length: i64, null_count: i64) -> Self`
- `fn length(self: &Self) -> i64` - The number of value slots in the Arrow array at this level of a nested
- `fn set_length(self: & mut Self, x: i64)`
- `fn null_count(self: &Self) -> i64` - The number of observed nulls. Fields with null_count == 0 may choose not
- `fn set_null_count(self: & mut Self, x: i64)`

**Traits:** SimpleToVerifyInSlice, Copy

**Trait Implementations:**

- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &FieldNode) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> FieldNode`
- **Push**
  - `fn push(self: &Self, dst: & mut [u8], _written_len: usize)`
  - `fn alignment() -> flatbuffers::PushAlignment`
- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Message::Message

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args MessageArgs<'args>) -> flatbuffers::WIPOffset<Message<'bldr>>`
- `fn version(self: &Self) -> MetadataVersion`
- `fn header_type(self: &Self) -> MessageHeader`
- `fn header(self: &Self) -> Option<flatbuffers::Table<'a>>`
- `fn bodyLength(self: &Self) -> i64`
- `fn custom_metadata(self: &Self) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<KeyValue<'a>>>>`
- `fn header_as_schema(self: &Self) -> Option<Schema<'a>>`
- `fn header_as_dictionary_batch(self: &Self) -> Option<DictionaryBatch<'a>>`
- `fn header_as_record_batch(self: &Self) -> Option<RecordBatch<'a>>`
- `fn header_as_tensor(self: &Self) -> Option<Tensor<'a>>`
- `fn header_as_sparse_tensor(self: &Self) -> Option<SparseTensor<'a>>`

**Traits:** Copy

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Message<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Message<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::Message::MessageArgs

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `version: MetadataVersion`
- `header_type: MessageHeader`
- `header: Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>`
- `bodyLength: i64`
- `custom_metadata: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<KeyValue<'a>>>>>`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Message::MessageBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_version(self: & mut Self, version: MetadataVersion)`
- `fn add_header_type(self: & mut Self, header_type: MessageHeader)`
- `fn add_header(self: & mut Self, header: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>)`
- `fn add_bodyLength(self: & mut Self, bodyLength: i64)`
- `fn add_custom_metadata(self: & mut Self, custom_metadata: flatbuffers::WIPOffset<flatbuffers::Vector<'b, flatbuffers::ForwardsUOffset<KeyValue<'b>>>>)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> MessageBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Message<'a>>`



## arrow_ipc::gen::Message::MessageHeader

*Struct*

----------------------------------------------------------------------
The root Message type
This union enables us to easily send different message types without
redundant storage, and in the future we can easily add new message types.

Arrow implementations do not need to implement all of the message types,
which may include experimental metadata types. For maximum compatibility,
it is best to send data using RecordBatch

**Tuple Struct**: `(u8)`

**Methods:**

- `fn variant_name(self: Self) -> Option<&'static str>` - Returns the variant's name or "" if unknown.

**Traits:** SimpleToVerifyInSlice, Eq, Copy

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &MessageHeader) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Default**
  - `fn default() -> MessageHeader`
- **Clone**
  - `fn clone(self: &Self) -> MessageHeader`
- **EndianScalar**
  - `fn to_little_endian(self: Self) -> u8`
  - `fn from_little_endian(v: u8) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &MessageHeader) -> $crate::cmp::Ordering`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Push**
  - `fn push(self: &Self, dst: & mut [u8], _written_len: usize)`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialEq**
  - `fn eq(self: &Self, other: &MessageHeader) -> bool`



## arrow_ipc::gen::Message::MessageHeaderUnionTableOffset

*Struct*



## arrow_ipc::gen::Message::MessageOffset

*Enum*



## arrow_ipc::gen::Message::RecordBatch

*Struct*

A data header describing the shared memory layout of a "record" or "row"
batch. Some systems call this a "row batch" internally and others a "record
batch".

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args RecordBatchArgs<'args>) -> flatbuffers::WIPOffset<RecordBatch<'bldr>>`
- `fn length(self: &Self) -> i64` - number of records / rows. The arrays in the batch should all have this
- `fn nodes(self: &Self) -> Option<flatbuffers::Vector<'a, FieldNode>>` - Nodes correspond to the pre-ordered flattened logical schema
- `fn buffers(self: &Self) -> Option<flatbuffers::Vector<'a, Buffer>>` - Buffers correspond to the pre-ordered flattened buffer tree
- `fn compression(self: &Self) -> Option<BodyCompression<'a>>` - Optional compression of the message body
- `fn variadicBufferCounts(self: &Self) -> Option<flatbuffers::Vector<'a, i64>>` - Some types such as Utf8View are represented using a variable number of buffers.

**Traits:** Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> RecordBatch<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &RecordBatch<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`



## arrow_ipc::gen::Message::RecordBatchArgs

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `length: i64`
- `nodes: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, FieldNode>>>`
- `buffers: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, Buffer>>>`
- `compression: Option<flatbuffers::WIPOffset<BodyCompression<'a>>>`
- `variadicBufferCounts: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, i64>>>`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Message::RecordBatchBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_length(self: & mut Self, length: i64)`
- `fn add_nodes(self: & mut Self, nodes: flatbuffers::WIPOffset<flatbuffers::Vector<'b, FieldNode>>)`
- `fn add_buffers(self: & mut Self, buffers: flatbuffers::WIPOffset<flatbuffers::Vector<'b, Buffer>>)`
- `fn add_compression(self: & mut Self, compression: flatbuffers::WIPOffset<BodyCompression<'b>>)`
- `fn add_variadicBufferCounts(self: & mut Self, variadicBufferCounts: flatbuffers::WIPOffset<flatbuffers::Vector<'b, i64>>)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> RecordBatchBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<RecordBatch<'a>>`



## arrow_ipc::gen::Message::RecordBatchOffset

*Enum*



## arrow_ipc::gen::Message::finish_message_buffer

*Function*

```rust
fn finish_message_buffer<'a, 'b, A>(fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>, root: flatbuffers::WIPOffset<Message<'a>>)
```



## arrow_ipc::gen::Message::finish_size_prefixed_message_buffer

*Function*

```rust
fn finish_size_prefixed_message_buffer<'a, 'b, A>(fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>, root: flatbuffers::WIPOffset<Message<'a>>)
```



## arrow_ipc::gen::Message::root_as_message

*Function*

Verifies that a buffer of bytes contains a `Message`
and returns it.
Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`root_as_message_unchecked`.

```rust
fn root_as_message(buf: &[u8]) -> Result<Message, flatbuffers::InvalidFlatbuffer>
```



## arrow_ipc::gen::Message::root_as_message_unchecked

*Function*

Assumes, without verification, that a buffer of bytes contains a Message and returns it.
# Safety
Callers must trust the given bytes do indeed contain a valid `Message`.

```rust
fn root_as_message_unchecked(buf: &[u8]) -> Message
```



## arrow_ipc::gen::Message::root_as_message_with_opts

*Function*

Verifies, with the given options, that a buffer of bytes
contains a `Message` and returns it.
Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`root_as_message_unchecked`.

```rust
fn root_as_message_with_opts<'b, 'o>(opts: &'o flatbuffers::VerifierOptions, buf: &'b [u8]) -> Result<Message<'b>, flatbuffers::InvalidFlatbuffer>
```



## arrow_ipc::gen::Message::size_prefixed_root_as_message

*Function*

Verifies that a buffer of bytes contains a size prefixed
`Message` and returns it.
Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`size_prefixed_root_as_message_unchecked`.

```rust
fn size_prefixed_root_as_message(buf: &[u8]) -> Result<Message, flatbuffers::InvalidFlatbuffer>
```



## arrow_ipc::gen::Message::size_prefixed_root_as_message_unchecked

*Function*

Assumes, without verification, that a buffer of bytes contains a size prefixed Message and returns it.
# Safety
Callers must trust the given bytes do indeed contain a valid size prefixed `Message`.

```rust
fn size_prefixed_root_as_message_unchecked(buf: &[u8]) -> Message
```



## arrow_ipc::gen::Message::size_prefixed_root_as_message_with_opts

*Function*

Verifies, with the given verifier options, that a buffer of
bytes contains a size prefixed `Message` and returns
it. Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`root_as_message_unchecked`.

```rust
fn size_prefixed_root_as_message_with_opts<'b, 'o>(opts: &'o flatbuffers::VerifierOptions, buf: &'b [u8]) -> Result<Message<'b>, flatbuffers::InvalidFlatbuffer>
```



