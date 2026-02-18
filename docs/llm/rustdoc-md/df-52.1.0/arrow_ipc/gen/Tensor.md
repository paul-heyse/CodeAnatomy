**arrow_ipc > gen > Tensor**

# Module: gen::Tensor

## Contents

**Structs**

- [`Tensor`](#tensor)
- [`TensorArgs`](#tensorargs)
- [`TensorBuilder`](#tensorbuilder)
- [`TensorDim`](#tensordim) - ----------------------------------------------------------------------
- [`TensorDimArgs`](#tensordimargs)
- [`TensorDimBuilder`](#tensordimbuilder)

**Enums**

- [`TensorDimOffset`](#tensordimoffset)
- [`TensorOffset`](#tensoroffset)

**Functions**

- [`finish_size_prefixed_tensor_buffer`](#finish_size_prefixed_tensor_buffer)
- [`finish_tensor_buffer`](#finish_tensor_buffer)
- [`root_as_tensor`](#root_as_tensor) - Verifies that a buffer of bytes contains a `Tensor`
- [`root_as_tensor_unchecked`](#root_as_tensor_unchecked) - Assumes, without verification, that a buffer of bytes contains a Tensor and returns it.
- [`root_as_tensor_with_opts`](#root_as_tensor_with_opts) - Verifies, with the given options, that a buffer of bytes
- [`size_prefixed_root_as_tensor`](#size_prefixed_root_as_tensor) - Verifies that a buffer of bytes contains a size prefixed
- [`size_prefixed_root_as_tensor_unchecked`](#size_prefixed_root_as_tensor_unchecked) - Assumes, without verification, that a buffer of bytes contains a size prefixed Tensor and returns it.
- [`size_prefixed_root_as_tensor_with_opts`](#size_prefixed_root_as_tensor_with_opts) - Verifies, with the given verifier options, that a buffer of

---

## arrow_ipc::gen::Tensor::Tensor

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args TensorArgs<'args>) -> flatbuffers::WIPOffset<Tensor<'bldr>>`
- `fn type_type(self: &Self) -> Type`
- `fn type_(self: &Self) -> flatbuffers::Table<'a>` - The type of data contained in a value cell. Currently only fixed-width
- `fn shape(self: &Self) -> flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<TensorDim<'a>>>` - The dimensions of the tensor, optionally named
- `fn strides(self: &Self) -> Option<flatbuffers::Vector<'a, i64>>` - Non-negative byte offsets to advance one value cell along each dimension
- `fn data(self: &Self) -> &'a Buffer` - The location and size of the tensor's data
- `fn type_as_null(self: &Self) -> Option<Null<'a>>`
- `fn type_as_int(self: &Self) -> Option<Int<'a>>`
- `fn type_as_floating_point(self: &Self) -> Option<FloatingPoint<'a>>`
- `fn type_as_binary(self: &Self) -> Option<Binary<'a>>`
- `fn type_as_utf_8(self: &Self) -> Option<Utf8<'a>>`
- `fn type_as_bool(self: &Self) -> Option<Bool<'a>>`
- `fn type_as_decimal(self: &Self) -> Option<Decimal<'a>>`
- `fn type_as_date(self: &Self) -> Option<Date<'a>>`
- `fn type_as_time(self: &Self) -> Option<Time<'a>>`
- `fn type_as_timestamp(self: &Self) -> Option<Timestamp<'a>>`
- `fn type_as_interval(self: &Self) -> Option<Interval<'a>>`
- `fn type_as_list(self: &Self) -> Option<List<'a>>`
- `fn type_as_struct_(self: &Self) -> Option<Struct_<'a>>`
- `fn type_as_union(self: &Self) -> Option<Union<'a>>`
- `fn type_as_fixed_size_binary(self: &Self) -> Option<FixedSizeBinary<'a>>`
- `fn type_as_fixed_size_list(self: &Self) -> Option<FixedSizeList<'a>>`
- `fn type_as_map(self: &Self) -> Option<Map<'a>>`
- `fn type_as_duration(self: &Self) -> Option<Duration<'a>>`
- `fn type_as_large_binary(self: &Self) -> Option<LargeBinary<'a>>`
- `fn type_as_large_utf_8(self: &Self) -> Option<LargeUtf8<'a>>`
- `fn type_as_large_list(self: &Self) -> Option<LargeList<'a>>`
- `fn type_as_run_end_encoded(self: &Self) -> Option<RunEndEncoded<'a>>`
- `fn type_as_binary_view(self: &Self) -> Option<BinaryView<'a>>`
- `fn type_as_utf_8_view(self: &Self) -> Option<Utf8View<'a>>`
- `fn type_as_list_view(self: &Self) -> Option<ListView<'a>>`
- `fn type_as_large_list_view(self: &Self) -> Option<LargeListView<'a>>`

**Traits:** Copy

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Tensor<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Tensor<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::Tensor::TensorArgs

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `type_type: Type`
- `type_: Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>`
- `shape: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<TensorDim<'a>>>>>`
- `strides: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, i64>>>`
- `data: Option<&'a Buffer>`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Tensor::TensorBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_type_type(self: & mut Self, type_type: Type)`
- `fn add_type_(self: & mut Self, type_: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>)`
- `fn add_shape(self: & mut Self, shape: flatbuffers::WIPOffset<flatbuffers::Vector<'b, flatbuffers::ForwardsUOffset<TensorDim<'b>>>>)`
- `fn add_strides(self: & mut Self, strides: flatbuffers::WIPOffset<flatbuffers::Vector<'b, i64>>)`
- `fn add_data(self: & mut Self, data: &Buffer)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> TensorBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Tensor<'a>>`



## arrow_ipc::gen::Tensor::TensorDim

*Struct*

----------------------------------------------------------------------
Data structures for dense tensors
Shape data for a single axis in a tensor

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args TensorDimArgs<'args>) -> flatbuffers::WIPOffset<TensorDim<'bldr>>`
- `fn size(self: &Self) -> i64` - Length of dimension
- `fn name(self: &Self) -> Option<&'a str>` - Name of the dimension, optional

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> TensorDim<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &TensorDim<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`



## arrow_ipc::gen::Tensor::TensorDimArgs

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `size: i64`
- `name: Option<flatbuffers::WIPOffset<&'a str>>`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Tensor::TensorDimBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_size(self: & mut Self, size: i64)`
- `fn add_name(self: & mut Self, name: flatbuffers::WIPOffset<&'b str>)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> TensorDimBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<TensorDim<'a>>`



## arrow_ipc::gen::Tensor::TensorDimOffset

*Enum*



## arrow_ipc::gen::Tensor::TensorOffset

*Enum*



## arrow_ipc::gen::Tensor::finish_size_prefixed_tensor_buffer

*Function*

```rust
fn finish_size_prefixed_tensor_buffer<'a, 'b, A>(fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>, root: flatbuffers::WIPOffset<Tensor<'a>>)
```



## arrow_ipc::gen::Tensor::finish_tensor_buffer

*Function*

```rust
fn finish_tensor_buffer<'a, 'b, A>(fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>, root: flatbuffers::WIPOffset<Tensor<'a>>)
```



## arrow_ipc::gen::Tensor::root_as_tensor

*Function*

Verifies that a buffer of bytes contains a `Tensor`
and returns it.
Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`root_as_tensor_unchecked`.

```rust
fn root_as_tensor(buf: &[u8]) -> Result<Tensor, flatbuffers::InvalidFlatbuffer>
```



## arrow_ipc::gen::Tensor::root_as_tensor_unchecked

*Function*

Assumes, without verification, that a buffer of bytes contains a Tensor and returns it.
# Safety
Callers must trust the given bytes do indeed contain a valid `Tensor`.

```rust
fn root_as_tensor_unchecked(buf: &[u8]) -> Tensor
```



## arrow_ipc::gen::Tensor::root_as_tensor_with_opts

*Function*

Verifies, with the given options, that a buffer of bytes
contains a `Tensor` and returns it.
Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`root_as_tensor_unchecked`.

```rust
fn root_as_tensor_with_opts<'b, 'o>(opts: &'o flatbuffers::VerifierOptions, buf: &'b [u8]) -> Result<Tensor<'b>, flatbuffers::InvalidFlatbuffer>
```



## arrow_ipc::gen::Tensor::size_prefixed_root_as_tensor

*Function*

Verifies that a buffer of bytes contains a size prefixed
`Tensor` and returns it.
Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`size_prefixed_root_as_tensor_unchecked`.

```rust
fn size_prefixed_root_as_tensor(buf: &[u8]) -> Result<Tensor, flatbuffers::InvalidFlatbuffer>
```



## arrow_ipc::gen::Tensor::size_prefixed_root_as_tensor_unchecked

*Function*

Assumes, without verification, that a buffer of bytes contains a size prefixed Tensor and returns it.
# Safety
Callers must trust the given bytes do indeed contain a valid size prefixed `Tensor`.

```rust
fn size_prefixed_root_as_tensor_unchecked(buf: &[u8]) -> Tensor
```



## arrow_ipc::gen::Tensor::size_prefixed_root_as_tensor_with_opts

*Function*

Verifies, with the given verifier options, that a buffer of
bytes contains a size prefixed `Tensor` and returns
it. Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`root_as_tensor_unchecked`.

```rust
fn size_prefixed_root_as_tensor_with_opts<'b, 'o>(opts: &'o flatbuffers::VerifierOptions, buf: &'b [u8]) -> Result<Tensor<'b>, flatbuffers::InvalidFlatbuffer>
```



