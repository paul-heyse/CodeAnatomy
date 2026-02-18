**arrow_ipc > gen > SparseTensor**

# Module: gen::SparseTensor

## Contents

**Structs**

- [`SparseMatrixCompressedAxis`](#sparsematrixcompressedaxis)
- [`SparseMatrixIndexCSX`](#sparsematrixindexcsx) - Compressed Sparse format, that is matrix-specific.
- [`SparseMatrixIndexCSXArgs`](#sparsematrixindexcsxargs)
- [`SparseMatrixIndexCSXBuilder`](#sparsematrixindexcsxbuilder)
- [`SparseTensor`](#sparsetensor)
- [`SparseTensorArgs`](#sparsetensorargs)
- [`SparseTensorBuilder`](#sparsetensorbuilder)
- [`SparseTensorIndex`](#sparsetensorindex)
- [`SparseTensorIndexCOO`](#sparsetensorindexcoo) - ----------------------------------------------------------------------
- [`SparseTensorIndexCOOArgs`](#sparsetensorindexcooargs)
- [`SparseTensorIndexCOOBuilder`](#sparsetensorindexcoobuilder)
- [`SparseTensorIndexCSF`](#sparsetensorindexcsf) - Compressed Sparse Fiber (CSF) sparse tensor index.
- [`SparseTensorIndexCSFArgs`](#sparsetensorindexcsfargs)
- [`SparseTensorIndexCSFBuilder`](#sparsetensorindexcsfbuilder)
- [`SparseTensorIndexUnionTableOffset`](#sparsetensorindexuniontableoffset)

**Enums**

- [`SparseMatrixIndexCSXOffset`](#sparsematrixindexcsxoffset)
- [`SparseTensorIndexCOOOffset`](#sparsetensorindexcoooffset)
- [`SparseTensorIndexCSFOffset`](#sparsetensorindexcsfoffset)
- [`SparseTensorOffset`](#sparsetensoroffset)

**Functions**

- [`finish_size_prefixed_sparse_tensor_buffer`](#finish_size_prefixed_sparse_tensor_buffer)
- [`finish_sparse_tensor_buffer`](#finish_sparse_tensor_buffer)
- [`root_as_sparse_tensor`](#root_as_sparse_tensor) - Verifies that a buffer of bytes contains a `SparseTensor`
- [`root_as_sparse_tensor_unchecked`](#root_as_sparse_tensor_unchecked) - Assumes, without verification, that a buffer of bytes contains a SparseTensor and returns it.
- [`root_as_sparse_tensor_with_opts`](#root_as_sparse_tensor_with_opts) - Verifies, with the given options, that a buffer of bytes
- [`size_prefixed_root_as_sparse_tensor`](#size_prefixed_root_as_sparse_tensor) - Verifies that a buffer of bytes contains a size prefixed
- [`size_prefixed_root_as_sparse_tensor_unchecked`](#size_prefixed_root_as_sparse_tensor_unchecked) - Assumes, without verification, that a buffer of bytes contains a size prefixed SparseTensor and returns it.
- [`size_prefixed_root_as_sparse_tensor_with_opts`](#size_prefixed_root_as_sparse_tensor_with_opts) - Verifies, with the given verifier options, that a buffer of

**Constants**

- [`ENUM_MAX_SPARSE_MATRIX_COMPRESSED_AXIS`](#enum_max_sparse_matrix_compressed_axis)
- [`ENUM_MAX_SPARSE_TENSOR_INDEX`](#enum_max_sparse_tensor_index)
- [`ENUM_MIN_SPARSE_MATRIX_COMPRESSED_AXIS`](#enum_min_sparse_matrix_compressed_axis)
- [`ENUM_MIN_SPARSE_TENSOR_INDEX`](#enum_min_sparse_tensor_index)
- [`ENUM_VALUES_SPARSE_MATRIX_COMPRESSED_AXIS`](#enum_values_sparse_matrix_compressed_axis)
- [`ENUM_VALUES_SPARSE_TENSOR_INDEX`](#enum_values_sparse_tensor_index)

---

## arrow_ipc::gen::SparseTensor::ENUM_MAX_SPARSE_MATRIX_COMPRESSED_AXIS

*Constant*: `i16`



## arrow_ipc::gen::SparseTensor::ENUM_MAX_SPARSE_TENSOR_INDEX

*Constant*: `u8`



## arrow_ipc::gen::SparseTensor::ENUM_MIN_SPARSE_MATRIX_COMPRESSED_AXIS

*Constant*: `i16`



## arrow_ipc::gen::SparseTensor::ENUM_MIN_SPARSE_TENSOR_INDEX

*Constant*: `u8`



## arrow_ipc::gen::SparseTensor::ENUM_VALUES_SPARSE_MATRIX_COMPRESSED_AXIS

*Constant*: `[SparseMatrixCompressedAxis; 2]`



## arrow_ipc::gen::SparseTensor::ENUM_VALUES_SPARSE_TENSOR_INDEX

*Constant*: `[SparseTensorIndex; 4]`



## arrow_ipc::gen::SparseTensor::SparseMatrixCompressedAxis

*Struct*

**Tuple Struct**: `(i16)`

**Methods:**

- `fn variant_name(self: Self) -> Option<&'static str>` - Returns the variant's name or "" if unknown.

**Traits:** SimpleToVerifyInSlice, Eq, Copy

**Trait Implementations:**

- **EndianScalar**
  - `fn to_little_endian(self: Self) -> i16`
  - `fn from_little_endian(v: i16) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &SparseMatrixCompressedAxis) -> $crate::cmp::Ordering`
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
  - `fn eq(self: &Self, other: &SparseMatrixCompressedAxis) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &SparseMatrixCompressedAxis) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Default**
  - `fn default() -> SparseMatrixCompressedAxis`
- **Clone**
  - `fn clone(self: &Self) -> SparseMatrixCompressedAxis`



## arrow_ipc::gen::SparseTensor::SparseMatrixIndexCSX

*Struct*

Compressed Sparse format, that is matrix-specific.

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args SparseMatrixIndexCSXArgs<'args>) -> flatbuffers::WIPOffset<SparseMatrixIndexCSX<'bldr>>`
- `fn compressedAxis(self: &Self) -> SparseMatrixCompressedAxis` - Which axis, row or column, is compressed
- `fn indptrType(self: &Self) -> Int<'a>` - The type of values in indptrBuffer
- `fn indptrBuffer(self: &Self) -> &'a Buffer` - indptrBuffer stores the location and size of indptr array that
- `fn indicesType(self: &Self) -> Int<'a>` - The type of values in indicesBuffer
- `fn indicesBuffer(self: &Self) -> &'a Buffer` - indicesBuffer stores the location and size of the array that

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> SparseMatrixIndexCSX<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &SparseMatrixIndexCSX<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`



## arrow_ipc::gen::SparseTensor::SparseMatrixIndexCSXArgs

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `compressedAxis: SparseMatrixCompressedAxis`
- `indptrType: Option<flatbuffers::WIPOffset<Int<'a>>>`
- `indptrBuffer: Option<&'a Buffer>`
- `indicesType: Option<flatbuffers::WIPOffset<Int<'a>>>`
- `indicesBuffer: Option<&'a Buffer>`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::SparseTensor::SparseMatrixIndexCSXBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_compressedAxis(self: & mut Self, compressedAxis: SparseMatrixCompressedAxis)`
- `fn add_indptrType(self: & mut Self, indptrType: flatbuffers::WIPOffset<Int<'b>>)`
- `fn add_indptrBuffer(self: & mut Self, indptrBuffer: &Buffer)`
- `fn add_indicesType(self: & mut Self, indicesType: flatbuffers::WIPOffset<Int<'b>>)`
- `fn add_indicesBuffer(self: & mut Self, indicesBuffer: &Buffer)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> SparseMatrixIndexCSXBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<SparseMatrixIndexCSX<'a>>`



## arrow_ipc::gen::SparseTensor::SparseMatrixIndexCSXOffset

*Enum*



## arrow_ipc::gen::SparseTensor::SparseTensor

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args SparseTensorArgs<'args>) -> flatbuffers::WIPOffset<SparseTensor<'bldr>>`
- `fn type_type(self: &Self) -> Type`
- `fn type_(self: &Self) -> flatbuffers::Table<'a>` - The type of data contained in a value cell.
- `fn shape(self: &Self) -> flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<TensorDim<'a>>>` - The dimensions of the tensor, optionally named.
- `fn non_zero_length(self: &Self) -> i64` - The number of non-zero values in a sparse tensor.
- `fn sparseIndex_type(self: &Self) -> SparseTensorIndex`
- `fn sparseIndex(self: &Self) -> flatbuffers::Table<'a>` - Sparse tensor index
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
- `fn sparseIndex_as_sparse_tensor_index_coo(self: &Self) -> Option<SparseTensorIndexCOO<'a>>`
- `fn sparseIndex_as_sparse_matrix_index_csx(self: &Self) -> Option<SparseMatrixIndexCSX<'a>>`
- `fn sparseIndex_as_sparse_tensor_index_csf(self: &Self) -> Option<SparseTensorIndexCSF<'a>>`

**Traits:** Copy

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> SparseTensor<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &SparseTensor<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::SparseTensor::SparseTensorArgs

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `type_type: Type`
- `type_: Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>`
- `shape: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<TensorDim<'a>>>>>`
- `non_zero_length: i64`
- `sparseIndex_type: SparseTensorIndex`
- `sparseIndex: Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>`
- `data: Option<&'a Buffer>`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::SparseTensor::SparseTensorBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_type_type(self: & mut Self, type_type: Type)`
- `fn add_type_(self: & mut Self, type_: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>)`
- `fn add_shape(self: & mut Self, shape: flatbuffers::WIPOffset<flatbuffers::Vector<'b, flatbuffers::ForwardsUOffset<TensorDim<'b>>>>)`
- `fn add_non_zero_length(self: & mut Self, non_zero_length: i64)`
- `fn add_sparseIndex_type(self: & mut Self, sparseIndex_type: SparseTensorIndex)`
- `fn add_sparseIndex(self: & mut Self, sparseIndex: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>)`
- `fn add_data(self: & mut Self, data: &Buffer)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> SparseTensorBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<SparseTensor<'a>>`



## arrow_ipc::gen::SparseTensor::SparseTensorIndex

*Struct*

**Tuple Struct**: `(u8)`

**Methods:**

- `fn variant_name(self: Self) -> Option<&'static str>` - Returns the variant's name or "" if unknown.

**Traits:** SimpleToVerifyInSlice, Eq, Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> SparseTensorIndex`
- **EndianScalar**
  - `fn to_little_endian(self: Self) -> u8`
  - `fn from_little_endian(v: u8) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &SparseTensorIndex) -> $crate::cmp::Ordering`
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
  - `fn eq(self: &Self, other: &SparseTensorIndex) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &SparseTensorIndex) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Default**
  - `fn default() -> SparseTensorIndex`



## arrow_ipc::gen::SparseTensor::SparseTensorIndexCOO

*Struct*

----------------------------------------------------------------------
EXPERIMENTAL: Data structures for sparse tensors
Coordinate (COO) format of sparse tensor index.

COO's index list are represented as a NxM matrix,
where N is the number of non-zero values,
and M is the number of dimensions of a sparse tensor.

indicesBuffer stores the location and size of the data of this indices
matrix.  The value type and the stride of the indices matrix is
specified in indicesType and indicesStrides fields.

For example, let X be a 2x3x4x5 tensor, and it has the following
6 non-zero values:
```text
  X[0, 1, 2, 0] := 1
  X[1, 1, 2, 3] := 2
  X[0, 2, 1, 0] := 3
  X[0, 1, 3, 0] := 4
  X[0, 1, 2, 1] := 5
  X[1, 2, 0, 4] := 6
```
In COO format, the index matrix of X is the following 4x6 matrix:
```text
  [[0, 0, 0, 0, 1, 1],
   [1, 1, 1, 2, 1, 2],
   [2, 2, 3, 1, 2, 0],
   [0, 1, 0, 0, 3, 4]]
```
When isCanonical is true, the indices is sorted in lexicographical order
(row-major order), and it does not have duplicated entries.  Otherwise,
the indices may not be sorted, or may have duplicated entries.

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args SparseTensorIndexCOOArgs<'args>) -> flatbuffers::WIPOffset<SparseTensorIndexCOO<'bldr>>`
- `fn indicesType(self: &Self) -> Int<'a>` - The type of values in indicesBuffer
- `fn indicesStrides(self: &Self) -> Option<flatbuffers::Vector<'a, i64>>` - Non-negative byte offsets to advance one value cell along each dimension
- `fn indicesBuffer(self: &Self) -> &'a Buffer` - The location and size of the indices matrix's data
- `fn isCanonical(self: &Self) -> bool` - This flag is true if and only if the indices matrix is sorted in

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> SparseTensorIndexCOO<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &SparseTensorIndexCOO<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`



## arrow_ipc::gen::SparseTensor::SparseTensorIndexCOOArgs

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `indicesType: Option<flatbuffers::WIPOffset<Int<'a>>>`
- `indicesStrides: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, i64>>>`
- `indicesBuffer: Option<&'a Buffer>`
- `isCanonical: bool`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::SparseTensor::SparseTensorIndexCOOBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_indicesType(self: & mut Self, indicesType: flatbuffers::WIPOffset<Int<'b>>)`
- `fn add_indicesStrides(self: & mut Self, indicesStrides: flatbuffers::WIPOffset<flatbuffers::Vector<'b, i64>>)`
- `fn add_indicesBuffer(self: & mut Self, indicesBuffer: &Buffer)`
- `fn add_isCanonical(self: & mut Self, isCanonical: bool)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> SparseTensorIndexCOOBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<SparseTensorIndexCOO<'a>>`



## arrow_ipc::gen::SparseTensor::SparseTensorIndexCOOOffset

*Enum*



## arrow_ipc::gen::SparseTensor::SparseTensorIndexCSF

*Struct*

Compressed Sparse Fiber (CSF) sparse tensor index.

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args SparseTensorIndexCSFArgs<'args>) -> flatbuffers::WIPOffset<SparseTensorIndexCSF<'bldr>>`
- `fn indptrType(self: &Self) -> Int<'a>` - CSF is a generalization of compressed sparse row (CSR) index.
- `fn indptrBuffers(self: &Self) -> flatbuffers::Vector<'a, Buffer>` - indptrBuffers stores the sparsity structure.
- `fn indicesType(self: &Self) -> Int<'a>` - The type of values in indicesBuffers
- `fn indicesBuffers(self: &Self) -> flatbuffers::Vector<'a, Buffer>` - indicesBuffers stores values of nodes.
- `fn axisOrder(self: &Self) -> flatbuffers::Vector<'a, i32>` - axisOrder stores the sequence in which dimensions were traversed to

**Traits:** Copy

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> SparseTensorIndexCSF<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &SparseTensorIndexCSF<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::SparseTensor::SparseTensorIndexCSFArgs

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `indptrType: Option<flatbuffers::WIPOffset<Int<'a>>>`
- `indptrBuffers: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, Buffer>>>`
- `indicesType: Option<flatbuffers::WIPOffset<Int<'a>>>`
- `indicesBuffers: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, Buffer>>>`
- `axisOrder: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, i32>>>`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::SparseTensor::SparseTensorIndexCSFBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_indptrType(self: & mut Self, indptrType: flatbuffers::WIPOffset<Int<'b>>)`
- `fn add_indptrBuffers(self: & mut Self, indptrBuffers: flatbuffers::WIPOffset<flatbuffers::Vector<'b, Buffer>>)`
- `fn add_indicesType(self: & mut Self, indicesType: flatbuffers::WIPOffset<Int<'b>>)`
- `fn add_indicesBuffers(self: & mut Self, indicesBuffers: flatbuffers::WIPOffset<flatbuffers::Vector<'b, Buffer>>)`
- `fn add_axisOrder(self: & mut Self, axisOrder: flatbuffers::WIPOffset<flatbuffers::Vector<'b, i32>>)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> SparseTensorIndexCSFBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<SparseTensorIndexCSF<'a>>`



## arrow_ipc::gen::SparseTensor::SparseTensorIndexCSFOffset

*Enum*



## arrow_ipc::gen::SparseTensor::SparseTensorIndexUnionTableOffset

*Struct*



## arrow_ipc::gen::SparseTensor::SparseTensorOffset

*Enum*



## arrow_ipc::gen::SparseTensor::finish_size_prefixed_sparse_tensor_buffer

*Function*

```rust
fn finish_size_prefixed_sparse_tensor_buffer<'a, 'b, A>(fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>, root: flatbuffers::WIPOffset<SparseTensor<'a>>)
```



## arrow_ipc::gen::SparseTensor::finish_sparse_tensor_buffer

*Function*

```rust
fn finish_sparse_tensor_buffer<'a, 'b, A>(fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>, root: flatbuffers::WIPOffset<SparseTensor<'a>>)
```



## arrow_ipc::gen::SparseTensor::root_as_sparse_tensor

*Function*

Verifies that a buffer of bytes contains a `SparseTensor`
and returns it.
Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`root_as_sparse_tensor_unchecked`.

```rust
fn root_as_sparse_tensor(buf: &[u8]) -> Result<SparseTensor, flatbuffers::InvalidFlatbuffer>
```



## arrow_ipc::gen::SparseTensor::root_as_sparse_tensor_unchecked

*Function*

Assumes, without verification, that a buffer of bytes contains a SparseTensor and returns it.
# Safety
Callers must trust the given bytes do indeed contain a valid `SparseTensor`.

```rust
fn root_as_sparse_tensor_unchecked(buf: &[u8]) -> SparseTensor
```



## arrow_ipc::gen::SparseTensor::root_as_sparse_tensor_with_opts

*Function*

Verifies, with the given options, that a buffer of bytes
contains a `SparseTensor` and returns it.
Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`root_as_sparse_tensor_unchecked`.

```rust
fn root_as_sparse_tensor_with_opts<'b, 'o>(opts: &'o flatbuffers::VerifierOptions, buf: &'b [u8]) -> Result<SparseTensor<'b>, flatbuffers::InvalidFlatbuffer>
```



## arrow_ipc::gen::SparseTensor::size_prefixed_root_as_sparse_tensor

*Function*

Verifies that a buffer of bytes contains a size prefixed
`SparseTensor` and returns it.
Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`size_prefixed_root_as_sparse_tensor_unchecked`.

```rust
fn size_prefixed_root_as_sparse_tensor(buf: &[u8]) -> Result<SparseTensor, flatbuffers::InvalidFlatbuffer>
```



## arrow_ipc::gen::SparseTensor::size_prefixed_root_as_sparse_tensor_unchecked

*Function*

Assumes, without verification, that a buffer of bytes contains a size prefixed SparseTensor and returns it.
# Safety
Callers must trust the given bytes do indeed contain a valid size prefixed `SparseTensor`.

```rust
fn size_prefixed_root_as_sparse_tensor_unchecked(buf: &[u8]) -> SparseTensor
```



## arrow_ipc::gen::SparseTensor::size_prefixed_root_as_sparse_tensor_with_opts

*Function*

Verifies, with the given verifier options, that a buffer of
bytes contains a size prefixed `SparseTensor` and returns
it. Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`root_as_sparse_tensor_unchecked`.

```rust
fn size_prefixed_root_as_sparse_tensor_with_opts<'b, 'o>(opts: &'o flatbuffers::VerifierOptions, buf: &'b [u8]) -> Result<SparseTensor<'b>, flatbuffers::InvalidFlatbuffer>
```



