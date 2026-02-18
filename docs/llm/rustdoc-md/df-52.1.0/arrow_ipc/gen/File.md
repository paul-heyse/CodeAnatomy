**arrow_ipc > gen > File**

# Module: gen::File

## Contents

**Structs**

- [`Block`](#block)
- [`Footer`](#footer) - ----------------------------------------------------------------------
- [`FooterArgs`](#footerargs)
- [`FooterBuilder`](#footerbuilder)

**Enums**

- [`FooterOffset`](#footeroffset)

**Functions**

- [`finish_footer_buffer`](#finish_footer_buffer)
- [`finish_size_prefixed_footer_buffer`](#finish_size_prefixed_footer_buffer)
- [`root_as_footer`](#root_as_footer) - Verifies that a buffer of bytes contains a `Footer`
- [`root_as_footer_unchecked`](#root_as_footer_unchecked) - Assumes, without verification, that a buffer of bytes contains a Footer and returns it.
- [`root_as_footer_with_opts`](#root_as_footer_with_opts) - Verifies, with the given options, that a buffer of bytes
- [`size_prefixed_root_as_footer`](#size_prefixed_root_as_footer) - Verifies that a buffer of bytes contains a size prefixed
- [`size_prefixed_root_as_footer_unchecked`](#size_prefixed_root_as_footer_unchecked) - Assumes, without verification, that a buffer of bytes contains a size prefixed Footer and returns it.
- [`size_prefixed_root_as_footer_with_opts`](#size_prefixed_root_as_footer_with_opts) - Verifies, with the given verifier options, that a buffer of

---

## arrow_ipc::gen::File::Block

*Struct*

**Tuple Struct**: `([u8; 24])`

**Methods:**

- `fn new(offset: i64, metaDataLength: i32, bodyLength: i64) -> Self`
- `fn offset(self: &Self) -> i64` - Index to the start of the RecordBlock (note this is past the Message header)
- `fn set_offset(self: & mut Self, x: i64)`
- `fn metaDataLength(self: &Self) -> i32` - Length of the metadata
- `fn set_metaDataLength(self: & mut Self, x: i32)`
- `fn bodyLength(self: &Self) -> i64` - Length of the data (this is aligned so there can be a gap between this and
- `fn set_bodyLength(self: & mut Self, x: i64)`

**Traits:** Copy, SimpleToVerifyInSlice

**Trait Implementations:**

- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **PartialEq**
  - `fn eq(self: &Self, other: &Block) -> bool`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Clone**
  - `fn clone(self: &Self) -> Block`
- **Push**
  - `fn push(self: &Self, dst: & mut [u8], _written_len: usize)`
  - `fn alignment() -> flatbuffers::PushAlignment`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`



## arrow_ipc::gen::File::Footer

*Struct*

----------------------------------------------------------------------
Arrow File metadata


**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args FooterArgs<'args>) -> flatbuffers::WIPOffset<Footer<'bldr>>`
- `fn version(self: &Self) -> MetadataVersion`
- `fn schema(self: &Self) -> Option<Schema<'a>>`
- `fn dictionaries(self: &Self) -> Option<flatbuffers::Vector<'a, Block>>`
- `fn recordBatches(self: &Self) -> Option<flatbuffers::Vector<'a, Block>>`
- `fn custom_metadata(self: &Self) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<KeyValue<'a>>>>` - User-defined metadata

**Traits:** Copy

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Footer<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Footer<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::File::FooterArgs

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `version: MetadataVersion`
- `schema: Option<flatbuffers::WIPOffset<Schema<'a>>>`
- `dictionaries: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, Block>>>`
- `recordBatches: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, Block>>>`
- `custom_metadata: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<KeyValue<'a>>>>>`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::File::FooterBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_version(self: & mut Self, version: MetadataVersion)`
- `fn add_schema(self: & mut Self, schema: flatbuffers::WIPOffset<Schema<'b>>)`
- `fn add_dictionaries(self: & mut Self, dictionaries: flatbuffers::WIPOffset<flatbuffers::Vector<'b, Block>>)`
- `fn add_recordBatches(self: & mut Self, recordBatches: flatbuffers::WIPOffset<flatbuffers::Vector<'b, Block>>)`
- `fn add_custom_metadata(self: & mut Self, custom_metadata: flatbuffers::WIPOffset<flatbuffers::Vector<'b, flatbuffers::ForwardsUOffset<KeyValue<'b>>>>)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> FooterBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Footer<'a>>`



## arrow_ipc::gen::File::FooterOffset

*Enum*



## arrow_ipc::gen::File::finish_footer_buffer

*Function*

```rust
fn finish_footer_buffer<'a, 'b, A>(fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>, root: flatbuffers::WIPOffset<Footer<'a>>)
```



## arrow_ipc::gen::File::finish_size_prefixed_footer_buffer

*Function*

```rust
fn finish_size_prefixed_footer_buffer<'a, 'b, A>(fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>, root: flatbuffers::WIPOffset<Footer<'a>>)
```



## arrow_ipc::gen::File::root_as_footer

*Function*

Verifies that a buffer of bytes contains a `Footer`
and returns it.
Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`root_as_footer_unchecked`.

```rust
fn root_as_footer(buf: &[u8]) -> Result<Footer, flatbuffers::InvalidFlatbuffer>
```



## arrow_ipc::gen::File::root_as_footer_unchecked

*Function*

Assumes, without verification, that a buffer of bytes contains a Footer and returns it.
# Safety
Callers must trust the given bytes do indeed contain a valid `Footer`.

```rust
fn root_as_footer_unchecked(buf: &[u8]) -> Footer
```



## arrow_ipc::gen::File::root_as_footer_with_opts

*Function*

Verifies, with the given options, that a buffer of bytes
contains a `Footer` and returns it.
Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`root_as_footer_unchecked`.

```rust
fn root_as_footer_with_opts<'b, 'o>(opts: &'o flatbuffers::VerifierOptions, buf: &'b [u8]) -> Result<Footer<'b>, flatbuffers::InvalidFlatbuffer>
```



## arrow_ipc::gen::File::size_prefixed_root_as_footer

*Function*

Verifies that a buffer of bytes contains a size prefixed
`Footer` and returns it.
Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`size_prefixed_root_as_footer_unchecked`.

```rust
fn size_prefixed_root_as_footer(buf: &[u8]) -> Result<Footer, flatbuffers::InvalidFlatbuffer>
```



## arrow_ipc::gen::File::size_prefixed_root_as_footer_unchecked

*Function*

Assumes, without verification, that a buffer of bytes contains a size prefixed Footer and returns it.
# Safety
Callers must trust the given bytes do indeed contain a valid size prefixed `Footer`.

```rust
fn size_prefixed_root_as_footer_unchecked(buf: &[u8]) -> Footer
```



## arrow_ipc::gen::File::size_prefixed_root_as_footer_with_opts

*Function*

Verifies, with the given verifier options, that a buffer of
bytes contains a size prefixed `Footer` and returns
it. Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`root_as_footer_unchecked`.

```rust
fn size_prefixed_root_as_footer_with_opts<'b, 'o>(opts: &'o flatbuffers::VerifierOptions, buf: &'b [u8]) -> Result<Footer<'b>, flatbuffers::InvalidFlatbuffer>
```



