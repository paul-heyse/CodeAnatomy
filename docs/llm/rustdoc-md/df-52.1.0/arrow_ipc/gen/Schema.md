**arrow_ipc > gen > Schema**

# Module: gen::Schema

## Contents

**Structs**

- [`Binary`](#binary) - Opaque binary data
- [`BinaryArgs`](#binaryargs)
- [`BinaryBuilder`](#binarybuilder)
- [`BinaryView`](#binaryview) - Logically the same as Binary, but the internal representation uses a view
- [`BinaryViewArgs`](#binaryviewargs)
- [`BinaryViewBuilder`](#binaryviewbuilder)
- [`Bool`](#bool)
- [`BoolArgs`](#boolargs)
- [`BoolBuilder`](#boolbuilder)
- [`Buffer`](#buffer) - ----------------------------------------------------------------------
- [`Date`](#date) - Date is either a 32-bit or 64-bit signed integer type representing an
- [`DateArgs`](#dateargs)
- [`DateBuilder`](#datebuilder)
- [`DateUnit`](#dateunit)
- [`Decimal`](#decimal) - Exact decimal value represented as an integer value in two's
- [`DecimalArgs`](#decimalargs)
- [`DecimalBuilder`](#decimalbuilder)
- [`DictionaryEncoding`](#dictionaryencoding)
- [`DictionaryEncodingArgs`](#dictionaryencodingargs)
- [`DictionaryEncodingBuilder`](#dictionaryencodingbuilder)
- [`DictionaryKind`](#dictionarykind) - ----------------------------------------------------------------------
- [`Duration`](#duration)
- [`DurationArgs`](#durationargs)
- [`DurationBuilder`](#durationbuilder)
- [`Endianness`](#endianness) - ----------------------------------------------------------------------
- [`Feature`](#feature) - Represents Arrow Features that might not have full support
- [`Field`](#field) - ----------------------------------------------------------------------
- [`FieldArgs`](#fieldargs)
- [`FieldBuilder`](#fieldbuilder)
- [`FixedSizeBinary`](#fixedsizebinary)
- [`FixedSizeBinaryArgs`](#fixedsizebinaryargs)
- [`FixedSizeBinaryBuilder`](#fixedsizebinarybuilder)
- [`FixedSizeList`](#fixedsizelist)
- [`FixedSizeListArgs`](#fixedsizelistargs)
- [`FixedSizeListBuilder`](#fixedsizelistbuilder)
- [`FloatingPoint`](#floatingpoint)
- [`FloatingPointArgs`](#floatingpointargs)
- [`FloatingPointBuilder`](#floatingpointbuilder)
- [`Int`](#int)
- [`IntArgs`](#intargs)
- [`IntBuilder`](#intbuilder)
- [`Interval`](#interval)
- [`IntervalArgs`](#intervalargs)
- [`IntervalBuilder`](#intervalbuilder)
- [`IntervalUnit`](#intervalunit)
- [`KeyValue`](#keyvalue) - ----------------------------------------------------------------------
- [`KeyValueArgs`](#keyvalueargs)
- [`KeyValueBuilder`](#keyvaluebuilder)
- [`LargeBinary`](#largebinary) - Same as Binary, but with 64-bit offsets, allowing to represent
- [`LargeBinaryArgs`](#largebinaryargs)
- [`LargeBinaryBuilder`](#largebinarybuilder)
- [`LargeList`](#largelist) - Same as List, but with 64-bit offsets, allowing to represent
- [`LargeListArgs`](#largelistargs)
- [`LargeListBuilder`](#largelistbuilder)
- [`LargeListView`](#largelistview) - Same as ListView, but with 64-bit offsets and sizes, allowing to represent
- [`LargeListViewArgs`](#largelistviewargs)
- [`LargeListViewBuilder`](#largelistviewbuilder)
- [`LargeUtf8`](#largeutf8) - Same as Utf8, but with 64-bit offsets, allowing to represent
- [`LargeUtf8Args`](#largeutf8args)
- [`LargeUtf8Builder`](#largeutf8builder)
- [`List`](#list)
- [`ListArgs`](#listargs)
- [`ListBuilder`](#listbuilder)
- [`ListView`](#listview) - Represents the same logical types that List can, but contains offsets and
- [`ListViewArgs`](#listviewargs)
- [`ListViewBuilder`](#listviewbuilder)
- [`Map`](#map) - A Map is a logical nested type that is represented as
- [`MapArgs`](#mapargs)
- [`MapBuilder`](#mapbuilder)
- [`MetadataVersion`](#metadataversion)
- [`Null`](#null) - These are stored in the flatbuffer in the Type union below
- [`NullArgs`](#nullargs)
- [`NullBuilder`](#nullbuilder)
- [`Precision`](#precision)
- [`RunEndEncoded`](#runendencoded) - Contains two child arrays, run_ends and values.
- [`RunEndEncodedArgs`](#runendencodedargs)
- [`RunEndEncodedBuilder`](#runendencodedbuilder)
- [`Schema`](#schema) - ----------------------------------------------------------------------
- [`SchemaArgs`](#schemaargs)
- [`SchemaBuilder`](#schemabuilder)
- [`Struct_`](#struct_) - A Struct_ in the flatbuffer metadata is the same as an Arrow Struct
- [`Struct_Args`](#struct_args)
- [`Struct_Builder`](#struct_builder)
- [`Time`](#time) - Time is either a 32-bit or 64-bit signed integer type representing an
- [`TimeArgs`](#timeargs)
- [`TimeBuilder`](#timebuilder)
- [`TimeUnit`](#timeunit)
- [`Timestamp`](#timestamp) - Timestamp is a 64-bit signed integer representing an elapsed time since a
- [`TimestampArgs`](#timestampargs)
- [`TimestampBuilder`](#timestampbuilder)
- [`Type`](#type) - ----------------------------------------------------------------------
- [`TypeUnionTableOffset`](#typeuniontableoffset)
- [`Union`](#union) - A union is a complex type with children in Field
- [`UnionArgs`](#unionargs)
- [`UnionBuilder`](#unionbuilder)
- [`UnionMode`](#unionmode)
- [`Utf8`](#utf8) - Unicode with UTF-8 encoding
- [`Utf8Args`](#utf8args)
- [`Utf8Builder`](#utf8builder)
- [`Utf8View`](#utf8view) - Logically the same as Utf8, but the internal representation uses a view
- [`Utf8ViewArgs`](#utf8viewargs)
- [`Utf8ViewBuilder`](#utf8viewbuilder)

**Enums**

- [`BinaryOffset`](#binaryoffset)
- [`BinaryViewOffset`](#binaryviewoffset)
- [`BoolOffset`](#booloffset)
- [`DateOffset`](#dateoffset)
- [`DecimalOffset`](#decimaloffset)
- [`DictionaryEncodingOffset`](#dictionaryencodingoffset)
- [`DurationOffset`](#durationoffset)
- [`FieldOffset`](#fieldoffset)
- [`FixedSizeBinaryOffset`](#fixedsizebinaryoffset)
- [`FixedSizeListOffset`](#fixedsizelistoffset)
- [`FloatingPointOffset`](#floatingpointoffset)
- [`IntOffset`](#intoffset)
- [`IntervalOffset`](#intervaloffset)
- [`KeyValueOffset`](#keyvalueoffset)
- [`LargeBinaryOffset`](#largebinaryoffset)
- [`LargeListOffset`](#largelistoffset)
- [`LargeListViewOffset`](#largelistviewoffset)
- [`LargeUtf8Offset`](#largeutf8offset)
- [`ListOffset`](#listoffset)
- [`ListViewOffset`](#listviewoffset)
- [`MapOffset`](#mapoffset)
- [`NullOffset`](#nulloffset)
- [`RunEndEncodedOffset`](#runendencodedoffset)
- [`SchemaOffset`](#schemaoffset)
- [`Struct_Offset`](#struct_offset)
- [`TimeOffset`](#timeoffset)
- [`TimestampOffset`](#timestampoffset)
- [`UnionOffset`](#unionoffset)
- [`Utf8Offset`](#utf8offset)
- [`Utf8ViewOffset`](#utf8viewoffset)

**Functions**

- [`finish_schema_buffer`](#finish_schema_buffer)
- [`finish_size_prefixed_schema_buffer`](#finish_size_prefixed_schema_buffer)
- [`root_as_schema`](#root_as_schema) - Verifies that a buffer of bytes contains a `Schema`
- [`root_as_schema_unchecked`](#root_as_schema_unchecked) - Assumes, without verification, that a buffer of bytes contains a Schema and returns it.
- [`root_as_schema_with_opts`](#root_as_schema_with_opts) - Verifies, with the given options, that a buffer of bytes
- [`size_prefixed_root_as_schema`](#size_prefixed_root_as_schema) - Verifies that a buffer of bytes contains a size prefixed
- [`size_prefixed_root_as_schema_unchecked`](#size_prefixed_root_as_schema_unchecked) - Assumes, without verification, that a buffer of bytes contains a size prefixed Schema and returns it.
- [`size_prefixed_root_as_schema_with_opts`](#size_prefixed_root_as_schema_with_opts) - Verifies, with the given verifier options, that a buffer of

**Constants**

- [`ENUM_MAX_DATE_UNIT`](#enum_max_date_unit)
- [`ENUM_MAX_DICTIONARY_KIND`](#enum_max_dictionary_kind)
- [`ENUM_MAX_ENDIANNESS`](#enum_max_endianness)
- [`ENUM_MAX_FEATURE`](#enum_max_feature)
- [`ENUM_MAX_INTERVAL_UNIT`](#enum_max_interval_unit)
- [`ENUM_MAX_METADATA_VERSION`](#enum_max_metadata_version)
- [`ENUM_MAX_PRECISION`](#enum_max_precision)
- [`ENUM_MAX_TIME_UNIT`](#enum_max_time_unit)
- [`ENUM_MAX_TYPE`](#enum_max_type)
- [`ENUM_MAX_UNION_MODE`](#enum_max_union_mode)
- [`ENUM_MIN_DATE_UNIT`](#enum_min_date_unit)
- [`ENUM_MIN_DICTIONARY_KIND`](#enum_min_dictionary_kind)
- [`ENUM_MIN_ENDIANNESS`](#enum_min_endianness)
- [`ENUM_MIN_FEATURE`](#enum_min_feature)
- [`ENUM_MIN_INTERVAL_UNIT`](#enum_min_interval_unit)
- [`ENUM_MIN_METADATA_VERSION`](#enum_min_metadata_version)
- [`ENUM_MIN_PRECISION`](#enum_min_precision)
- [`ENUM_MIN_TIME_UNIT`](#enum_min_time_unit)
- [`ENUM_MIN_TYPE`](#enum_min_type)
- [`ENUM_MIN_UNION_MODE`](#enum_min_union_mode)
- [`ENUM_VALUES_DATE_UNIT`](#enum_values_date_unit)
- [`ENUM_VALUES_DICTIONARY_KIND`](#enum_values_dictionary_kind)
- [`ENUM_VALUES_ENDIANNESS`](#enum_values_endianness)
- [`ENUM_VALUES_FEATURE`](#enum_values_feature)
- [`ENUM_VALUES_INTERVAL_UNIT`](#enum_values_interval_unit)
- [`ENUM_VALUES_METADATA_VERSION`](#enum_values_metadata_version)
- [`ENUM_VALUES_PRECISION`](#enum_values_precision)
- [`ENUM_VALUES_TIME_UNIT`](#enum_values_time_unit)
- [`ENUM_VALUES_TYPE`](#enum_values_type)
- [`ENUM_VALUES_UNION_MODE`](#enum_values_union_mode)

---

## arrow_ipc::gen::Schema::Binary

*Struct*

Opaque binary data

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, _args: &'args BinaryArgs) -> flatbuffers::WIPOffset<Binary<'bldr>>`

**Traits:** Copy

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Binary<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Binary<'a>`



## arrow_ipc::gen::Schema::BinaryArgs

*Struct*

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::BinaryBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> BinaryBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Binary<'a>>`



## arrow_ipc::gen::Schema::BinaryOffset

*Enum*



## arrow_ipc::gen::Schema::BinaryView

*Struct*

Logically the same as Binary, but the internal representation uses a view
struct that contains the string length and either the string's entire data
inline (for small strings) or an inlined prefix, an index of another buffer,
and an offset pointing to a slice in that buffer (for non-small strings).

Since it uses a variable number of data buffers, each Field with this type
must have a corresponding entry in `variadicBufferCounts`.

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, _args: &'args BinaryViewArgs) -> flatbuffers::WIPOffset<BinaryView<'bldr>>`

**Traits:** Copy

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> BinaryView<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &BinaryView<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::Schema::BinaryViewArgs

*Struct*

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::BinaryViewBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> BinaryViewBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<BinaryView<'a>>`



## arrow_ipc::gen::Schema::BinaryViewOffset

*Enum*



## arrow_ipc::gen::Schema::Bool

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, _args: &'args BoolArgs) -> flatbuffers::WIPOffset<Bool<'bldr>>`

**Traits:** Copy

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Bool<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Bool<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::Schema::BoolArgs

*Struct*

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::BoolBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> BoolBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Bool<'a>>`



## arrow_ipc::gen::Schema::BoolOffset

*Enum*



## arrow_ipc::gen::Schema::Buffer

*Struct*

----------------------------------------------------------------------
A Buffer represents a single contiguous memory segment

**Tuple Struct**: `([u8; 16])`

**Methods:**

- `fn new(offset: i64, length: i64) -> Self`
- `fn offset(self: &Self) -> i64` - The relative offset into the shared memory page where the bytes for this
- `fn set_offset(self: & mut Self, x: i64)`
- `fn length(self: &Self) -> i64` - The absolute length (in bytes) of the memory buffer. The memory is found
- `fn set_length(self: & mut Self, x: i64)`

**Traits:** Copy, SimpleToVerifyInSlice

**Trait Implementations:**

- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Buffer) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> Buffer`
- **Push**
  - `fn push(self: &Self, dst: & mut [u8], _written_len: usize)`
  - `fn alignment() -> flatbuffers::PushAlignment`
- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::Date

*Struct*

Date is either a 32-bit or 64-bit signed integer type representing an
elapsed time since UNIX epoch (1970-01-01), stored in either of two units:

* Milliseconds (64 bits) indicating UNIX time elapsed since the epoch (no
  leap seconds), where the values are evenly divisible by 86400000
* Days (32 bits) since the UNIX epoch

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args DateArgs) -> flatbuffers::WIPOffset<Date<'bldr>>`
- `fn unit(self: &Self) -> DateUnit`

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Date<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Date<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`



## arrow_ipc::gen::Schema::DateArgs

*Struct*

**Fields:**
- `unit: DateUnit`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::DateBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_unit(self: & mut Self, unit: DateUnit)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> DateBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Date<'a>>`



## arrow_ipc::gen::Schema::DateOffset

*Enum*



## arrow_ipc::gen::Schema::DateUnit

*Struct*

**Tuple Struct**: `(i16)`

**Methods:**

- `fn variant_name(self: Self) -> Option<&'static str>` - Returns the variant's name or "" if unknown.

**Traits:** SimpleToVerifyInSlice, Eq, Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> DateUnit`
- **EndianScalar**
  - `fn to_little_endian(self: Self) -> i16`
  - `fn from_little_endian(v: i16) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &DateUnit) -> $crate::cmp::Ordering`
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
  - `fn eq(self: &Self, other: &DateUnit) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &DateUnit) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Default**
  - `fn default() -> DateUnit`



## arrow_ipc::gen::Schema::Decimal

*Struct*

Exact decimal value represented as an integer value in two's
complement. Currently only 128-bit (16-byte) and 256-bit (32-byte) integers
are used. The representation uses the endianness indicated
in the Schema.

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args DecimalArgs) -> flatbuffers::WIPOffset<Decimal<'bldr>>`
- `fn precision(self: &Self) -> i32` - Total number of decimal digits
- `fn scale(self: &Self) -> i32` - Number of digits after the decimal point "."
- `fn bitWidth(self: &Self) -> i32` - Number of bits per value. The only accepted widths are 128 and 256.

**Traits:** Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Decimal<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Decimal<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`



## arrow_ipc::gen::Schema::DecimalArgs

*Struct*

**Fields:**
- `precision: i32`
- `scale: i32`
- `bitWidth: i32`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::DecimalBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_precision(self: & mut Self, precision: i32)`
- `fn add_scale(self: & mut Self, scale: i32)`
- `fn add_bitWidth(self: & mut Self, bitWidth: i32)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> DecimalBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Decimal<'a>>`



## arrow_ipc::gen::Schema::DecimalOffset

*Enum*



## arrow_ipc::gen::Schema::DictionaryEncoding

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args DictionaryEncodingArgs<'args>) -> flatbuffers::WIPOffset<DictionaryEncoding<'bldr>>`
- `fn id(self: &Self) -> i64` - The known dictionary id in the application where this data is used. In
- `fn indexType(self: &Self) -> Option<Int<'a>>` - The dictionary indices are constrained to be non-negative integers. If
- `fn isOrdered(self: &Self) -> bool` - By default, dictionaries are not ordered, or the order does not have
- `fn dictionaryKind(self: &Self) -> DictionaryKind`

**Traits:** Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> DictionaryEncoding<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &DictionaryEncoding<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`



## arrow_ipc::gen::Schema::DictionaryEncodingArgs

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `id: i64`
- `indexType: Option<flatbuffers::WIPOffset<Int<'a>>>`
- `isOrdered: bool`
- `dictionaryKind: DictionaryKind`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::DictionaryEncodingBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_id(self: & mut Self, id: i64)`
- `fn add_indexType(self: & mut Self, indexType: flatbuffers::WIPOffset<Int<'b>>)`
- `fn add_isOrdered(self: & mut Self, isOrdered: bool)`
- `fn add_dictionaryKind(self: & mut Self, dictionaryKind: DictionaryKind)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> DictionaryEncodingBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<DictionaryEncoding<'a>>`



## arrow_ipc::gen::Schema::DictionaryEncodingOffset

*Enum*



## arrow_ipc::gen::Schema::DictionaryKind

*Struct*

----------------------------------------------------------------------
Dictionary encoding metadata
Maintained for forwards compatibility, in the future
Dictionaries might be explicit maps between integers and values
allowing for non-contiguous index values

**Tuple Struct**: `(i16)`

**Methods:**

- `fn variant_name(self: Self) -> Option<&'static str>` - Returns the variant's name or "" if unknown.

**Traits:** Eq, Copy, SimpleToVerifyInSlice

**Trait Implementations:**

- **Push**
  - `fn push(self: &Self, dst: & mut [u8], _written_len: usize)`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialEq**
  - `fn eq(self: &Self, other: &DictionaryKind) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &DictionaryKind) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Default**
  - `fn default() -> DictionaryKind`
- **Clone**
  - `fn clone(self: &Self) -> DictionaryKind`
- **EndianScalar**
  - `fn to_little_endian(self: Self) -> i16`
  - `fn from_little_endian(v: i16) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &DictionaryKind) -> $crate::cmp::Ordering`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::Schema::Duration

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args DurationArgs) -> flatbuffers::WIPOffset<Duration<'bldr>>`
- `fn unit(self: &Self) -> TimeUnit`

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Duration<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Duration<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`



## arrow_ipc::gen::Schema::DurationArgs

*Struct*

**Fields:**
- `unit: TimeUnit`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::DurationBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_unit(self: & mut Self, unit: TimeUnit)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> DurationBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Duration<'a>>`



## arrow_ipc::gen::Schema::DurationOffset

*Enum*



## arrow_ipc::gen::Schema::ENUM_MAX_DATE_UNIT

*Constant*: `i16`



## arrow_ipc::gen::Schema::ENUM_MAX_DICTIONARY_KIND

*Constant*: `i16`



## arrow_ipc::gen::Schema::ENUM_MAX_ENDIANNESS

*Constant*: `i16`



## arrow_ipc::gen::Schema::ENUM_MAX_FEATURE

*Constant*: `i64`



## arrow_ipc::gen::Schema::ENUM_MAX_INTERVAL_UNIT

*Constant*: `i16`



## arrow_ipc::gen::Schema::ENUM_MAX_METADATA_VERSION

*Constant*: `i16`



## arrow_ipc::gen::Schema::ENUM_MAX_PRECISION

*Constant*: `i16`



## arrow_ipc::gen::Schema::ENUM_MAX_TIME_UNIT

*Constant*: `i16`



## arrow_ipc::gen::Schema::ENUM_MAX_TYPE

*Constant*: `u8`



## arrow_ipc::gen::Schema::ENUM_MAX_UNION_MODE

*Constant*: `i16`



## arrow_ipc::gen::Schema::ENUM_MIN_DATE_UNIT

*Constant*: `i16`



## arrow_ipc::gen::Schema::ENUM_MIN_DICTIONARY_KIND

*Constant*: `i16`



## arrow_ipc::gen::Schema::ENUM_MIN_ENDIANNESS

*Constant*: `i16`



## arrow_ipc::gen::Schema::ENUM_MIN_FEATURE

*Constant*: `i64`



## arrow_ipc::gen::Schema::ENUM_MIN_INTERVAL_UNIT

*Constant*: `i16`



## arrow_ipc::gen::Schema::ENUM_MIN_METADATA_VERSION

*Constant*: `i16`



## arrow_ipc::gen::Schema::ENUM_MIN_PRECISION

*Constant*: `i16`



## arrow_ipc::gen::Schema::ENUM_MIN_TIME_UNIT

*Constant*: `i16`



## arrow_ipc::gen::Schema::ENUM_MIN_TYPE

*Constant*: `u8`



## arrow_ipc::gen::Schema::ENUM_MIN_UNION_MODE

*Constant*: `i16`



## arrow_ipc::gen::Schema::ENUM_VALUES_DATE_UNIT

*Constant*: `[DateUnit; 2]`



## arrow_ipc::gen::Schema::ENUM_VALUES_DICTIONARY_KIND

*Constant*: `[DictionaryKind; 1]`



## arrow_ipc::gen::Schema::ENUM_VALUES_ENDIANNESS

*Constant*: `[Endianness; 2]`



## arrow_ipc::gen::Schema::ENUM_VALUES_FEATURE

*Constant*: `[Feature; 3]`



## arrow_ipc::gen::Schema::ENUM_VALUES_INTERVAL_UNIT

*Constant*: `[IntervalUnit; 3]`



## arrow_ipc::gen::Schema::ENUM_VALUES_METADATA_VERSION

*Constant*: `[MetadataVersion; 5]`



## arrow_ipc::gen::Schema::ENUM_VALUES_PRECISION

*Constant*: `[Precision; 3]`



## arrow_ipc::gen::Schema::ENUM_VALUES_TIME_UNIT

*Constant*: `[TimeUnit; 4]`



## arrow_ipc::gen::Schema::ENUM_VALUES_TYPE

*Constant*: `[Type; 27]`



## arrow_ipc::gen::Schema::ENUM_VALUES_UNION_MODE

*Constant*: `[UnionMode; 2]`



## arrow_ipc::gen::Schema::Endianness

*Struct*

----------------------------------------------------------------------
Endianness of the platform producing the data

**Tuple Struct**: `(i16)`

**Methods:**

- `fn variant_name(self: Self) -> Option<&'static str>` - Returns the variant's name or "" if unknown.
- `fn equals_to_target_endianness(self: Self) -> bool` - Returns true if the endianness of the source system matches the endianness of the target system.

**Traits:** Eq, Copy, SimpleToVerifyInSlice

**Trait Implementations:**

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
  - `fn eq(self: &Self, other: &Endianness) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Endianness) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Default**
  - `fn default() -> Endianness`
- **Clone**
  - `fn clone(self: &Self) -> Endianness`
- **EndianScalar**
  - `fn to_little_endian(self: Self) -> i16`
  - `fn from_little_endian(v: i16) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &Endianness) -> $crate::cmp::Ordering`



## arrow_ipc::gen::Schema::Feature

*Struct*

Represents Arrow Features that might not have full support
within implementations. This is intended to be used in
two scenarios:
 1.  A mechanism for readers of Arrow Streams
     and files to understand that the stream or file makes
     use of a feature that isn't supported or unknown to
     the implementation (and therefore can meet the Arrow
     forward compatibility guarantees).
 2.  A means of negotiating between a client and server
     what features a stream is allowed to use. The enums
     values here are intented to represent higher level
     features, additional details maybe negotiated
     with key-value pairs specific to the protocol.

Enums added to this list should be assigned power-of-two values
to facilitate exchanging and comparing bitmaps for supported
features.

**Tuple Struct**: `(i64)`

**Methods:**

- `fn variant_name(self: Self) -> Option<&'static str>` - Returns the variant's name or "" if unknown.

**Traits:** SimpleToVerifyInSlice, Eq, Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Feature`
- **EndianScalar**
  - `fn to_little_endian(self: Self) -> i64`
  - `fn from_little_endian(v: i64) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &Feature) -> $crate::cmp::Ordering`
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
  - `fn eq(self: &Self, other: &Feature) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Feature) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Default**
  - `fn default() -> Feature`



## arrow_ipc::gen::Schema::Field

*Struct*

----------------------------------------------------------------------
A field represents a named column in a record / row batch or child of a
nested type.

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args FieldArgs<'args>) -> flatbuffers::WIPOffset<Field<'bldr>>`
- `fn name(self: &Self) -> Option<&'a str>` - Name is not required, in i.e. a List
- `fn nullable(self: &Self) -> bool` - Whether or not this field can contain nulls. Should be true in general.
- `fn type_type(self: &Self) -> Type`
- `fn type_(self: &Self) -> Option<flatbuffers::Table<'a>>` - This is the type of the decoded value if the field is dictionary encoded.
- `fn dictionary(self: &Self) -> Option<DictionaryEncoding<'a>>` - Present only if the field is dictionary encoded.
- `fn children(self: &Self) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Field<'a>>>>` - children apply only to nested data types like Struct, List and Union. For
- `fn custom_metadata(self: &Self) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<KeyValue<'a>>>>` - User-defined metadata
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
  - `fn clone(self: &Self) -> Field<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Field<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::Schema::FieldArgs

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `name: Option<flatbuffers::WIPOffset<&'a str>>`
- `nullable: bool`
- `type_type: Type`
- `type_: Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>`
- `dictionary: Option<flatbuffers::WIPOffset<DictionaryEncoding<'a>>>`
- `children: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Field<'a>>>>>`
- `custom_metadata: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<KeyValue<'a>>>>>`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::FieldBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_name(self: & mut Self, name: flatbuffers::WIPOffset<&'b str>)`
- `fn add_nullable(self: & mut Self, nullable: bool)`
- `fn add_type_type(self: & mut Self, type_type: Type)`
- `fn add_type_(self: & mut Self, type_: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>)`
- `fn add_dictionary(self: & mut Self, dictionary: flatbuffers::WIPOffset<DictionaryEncoding<'b>>)`
- `fn add_children(self: & mut Self, children: flatbuffers::WIPOffset<flatbuffers::Vector<'b, flatbuffers::ForwardsUOffset<Field<'b>>>>)`
- `fn add_custom_metadata(self: & mut Self, custom_metadata: flatbuffers::WIPOffset<flatbuffers::Vector<'b, flatbuffers::ForwardsUOffset<KeyValue<'b>>>>)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> FieldBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Field<'a>>`



## arrow_ipc::gen::Schema::FieldOffset

*Enum*



## arrow_ipc::gen::Schema::FixedSizeBinary

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args FixedSizeBinaryArgs) -> flatbuffers::WIPOffset<FixedSizeBinary<'bldr>>`
- `fn byteWidth(self: &Self) -> i32` - Number of bytes per value

**Traits:** Copy

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> FixedSizeBinary<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &FixedSizeBinary<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::Schema::FixedSizeBinaryArgs

*Struct*

**Fields:**
- `byteWidth: i32`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::FixedSizeBinaryBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_byteWidth(self: & mut Self, byteWidth: i32)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> FixedSizeBinaryBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<FixedSizeBinary<'a>>`



## arrow_ipc::gen::Schema::FixedSizeBinaryOffset

*Enum*



## arrow_ipc::gen::Schema::FixedSizeList

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args FixedSizeListArgs) -> flatbuffers::WIPOffset<FixedSizeList<'bldr>>`
- `fn listSize(self: &Self) -> i32` - Number of list items per value

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> FixedSizeList<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &FixedSizeList<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`



## arrow_ipc::gen::Schema::FixedSizeListArgs

*Struct*

**Fields:**
- `listSize: i32`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::FixedSizeListBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_listSize(self: & mut Self, listSize: i32)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> FixedSizeListBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<FixedSizeList<'a>>`



## arrow_ipc::gen::Schema::FixedSizeListOffset

*Enum*



## arrow_ipc::gen::Schema::FloatingPoint

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args FloatingPointArgs) -> flatbuffers::WIPOffset<FloatingPoint<'bldr>>`
- `fn precision(self: &Self) -> Precision`

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> FloatingPoint<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &FloatingPoint<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`



## arrow_ipc::gen::Schema::FloatingPointArgs

*Struct*

**Fields:**
- `precision: Precision`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::FloatingPointBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_precision(self: & mut Self, precision: Precision)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> FloatingPointBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<FloatingPoint<'a>>`



## arrow_ipc::gen::Schema::FloatingPointOffset

*Enum*



## arrow_ipc::gen::Schema::Int

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args IntArgs) -> flatbuffers::WIPOffset<Int<'bldr>>`
- `fn bitWidth(self: &Self) -> i32`
- `fn is_signed(self: &Self) -> bool`

**Traits:** Copy

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Int<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Int<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::Schema::IntArgs

*Struct*

**Fields:**
- `bitWidth: i32`
- `is_signed: bool`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::IntBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_bitWidth(self: & mut Self, bitWidth: i32)`
- `fn add_is_signed(self: & mut Self, is_signed: bool)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> IntBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Int<'a>>`



## arrow_ipc::gen::Schema::IntOffset

*Enum*



## arrow_ipc::gen::Schema::Interval

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args IntervalArgs) -> flatbuffers::WIPOffset<Interval<'bldr>>`
- `fn unit(self: &Self) -> IntervalUnit`

**Traits:** Copy

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Interval<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Interval<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::Schema::IntervalArgs

*Struct*

**Fields:**
- `unit: IntervalUnit`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::IntervalBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_unit(self: & mut Self, unit: IntervalUnit)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> IntervalBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Interval<'a>>`



## arrow_ipc::gen::Schema::IntervalOffset

*Enum*



## arrow_ipc::gen::Schema::IntervalUnit

*Struct*

**Tuple Struct**: `(i16)`

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
  - `fn eq(self: &Self, other: &IntervalUnit) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &IntervalUnit) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Default**
  - `fn default() -> IntervalUnit`
- **Clone**
  - `fn clone(self: &Self) -> IntervalUnit`
- **EndianScalar**
  - `fn to_little_endian(self: Self) -> i16`
  - `fn from_little_endian(v: i16) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &IntervalUnit) -> $crate::cmp::Ordering`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`



## arrow_ipc::gen::Schema::KeyValue

*Struct*

----------------------------------------------------------------------
user defined key value pairs to add custom metadata to arrow
key namespacing is the responsibility of the user

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args KeyValueArgs<'args>) -> flatbuffers::WIPOffset<KeyValue<'bldr>>`
- `fn key(self: &Self) -> Option<&'a str>`
- `fn value(self: &Self) -> Option<&'a str>`

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> KeyValue<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &KeyValue<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`



## arrow_ipc::gen::Schema::KeyValueArgs

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `key: Option<flatbuffers::WIPOffset<&'a str>>`
- `value: Option<flatbuffers::WIPOffset<&'a str>>`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::KeyValueBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_key(self: & mut Self, key: flatbuffers::WIPOffset<&'b str>)`
- `fn add_value(self: & mut Self, value: flatbuffers::WIPOffset<&'b str>)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> KeyValueBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<KeyValue<'a>>`



## arrow_ipc::gen::Schema::KeyValueOffset

*Enum*



## arrow_ipc::gen::Schema::LargeBinary

*Struct*

Same as Binary, but with 64-bit offsets, allowing to represent
extremely large data values.

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, _args: &'args LargeBinaryArgs) -> flatbuffers::WIPOffset<LargeBinary<'bldr>>`

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> LargeBinary<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &LargeBinary<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`



## arrow_ipc::gen::Schema::LargeBinaryArgs

*Struct*

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::LargeBinaryBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> LargeBinaryBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<LargeBinary<'a>>`



## arrow_ipc::gen::Schema::LargeBinaryOffset

*Enum*



## arrow_ipc::gen::Schema::LargeList

*Struct*

Same as List, but with 64-bit offsets, allowing to represent
extremely large data values.

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, _args: &'args LargeListArgs) -> flatbuffers::WIPOffset<LargeList<'bldr>>`

**Traits:** Copy

**Trait Implementations:**

- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> LargeList<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &LargeList<'a>) -> bool`



## arrow_ipc::gen::Schema::LargeListArgs

*Struct*

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::LargeListBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> LargeListBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<LargeList<'a>>`



## arrow_ipc::gen::Schema::LargeListOffset

*Enum*



## arrow_ipc::gen::Schema::LargeListView

*Struct*

Same as ListView, but with 64-bit offsets and sizes, allowing to represent
extremely large data values.

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, _args: &'args LargeListViewArgs) -> flatbuffers::WIPOffset<LargeListView<'bldr>>`

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> LargeListView<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &LargeListView<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`



## arrow_ipc::gen::Schema::LargeListViewArgs

*Struct*

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::LargeListViewBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> LargeListViewBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<LargeListView<'a>>`



## arrow_ipc::gen::Schema::LargeListViewOffset

*Enum*



## arrow_ipc::gen::Schema::LargeUtf8

*Struct*

Same as Utf8, but with 64-bit offsets, allowing to represent
extremely large data values.

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, _args: &'args LargeUtf8Args) -> flatbuffers::WIPOffset<LargeUtf8<'bldr>>`

**Traits:** Copy

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> LargeUtf8<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &LargeUtf8<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::Schema::LargeUtf8Args

*Struct*

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::LargeUtf8Builder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> LargeUtf8Builder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<LargeUtf8<'a>>`



## arrow_ipc::gen::Schema::LargeUtf8Offset

*Enum*



## arrow_ipc::gen::Schema::List

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, _args: &'args ListArgs) -> flatbuffers::WIPOffset<List<'bldr>>`

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> List<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &List<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`



## arrow_ipc::gen::Schema::ListArgs

*Struct*

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::ListBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> ListBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<List<'a>>`



## arrow_ipc::gen::Schema::ListOffset

*Enum*



## arrow_ipc::gen::Schema::ListView

*Struct*

Represents the same logical types that List can, but contains offsets and
sizes allowing for writes in any order and sharing of child values among
list values.

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, _args: &'args ListViewArgs) -> flatbuffers::WIPOffset<ListView<'bldr>>`

**Traits:** Copy

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ListView<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &ListView<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::Schema::ListViewArgs

*Struct*

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::ListViewBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> ListViewBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<ListView<'a>>`



## arrow_ipc::gen::Schema::ListViewOffset

*Enum*



## arrow_ipc::gen::Schema::Map

*Struct*

A Map is a logical nested type that is represented as

List<entries: Struct<key: K, value: V>>

In this layout, the keys and values are each respectively contiguous. We do
not constrain the key and value types, so the application is responsible
for ensuring that the keys are hashable and unique. Whether the keys are sorted
may be set in the metadata for this field.

In a field with Map type, the field has a child Struct field, which then
has two children: key type and the second the value type. The names of the
child fields may be respectively "entries", "key", and "value", but this is
not enforced.

Map
```text
  - child[0] entries: Struct
    - child[0] key: K
    - child[1] value: V
```
Neither the "entries" field nor the "key" field may be nullable.

The metadata is structured so that Arrow systems without special handling
for Map can make Map an alias for List. The "layout" attribute for the Map
field must have the same contents as a List.

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args MapArgs) -> flatbuffers::WIPOffset<Map<'bldr>>`
- `fn keysSorted(self: &Self) -> bool` - Set to true if the keys within each value are sorted

**Traits:** Copy

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Map<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Map<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::Schema::MapArgs

*Struct*

**Fields:**
- `keysSorted: bool`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::MapBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_keysSorted(self: & mut Self, keysSorted: bool)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> MapBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Map<'a>>`



## arrow_ipc::gen::Schema::MapOffset

*Enum*



## arrow_ipc::gen::Schema::MetadataVersion

*Struct*

**Tuple Struct**: `(i16)`

**Methods:**

- `fn variant_name(self: Self) -> Option<&'static str>` - Returns the variant's name or "" if unknown.

**Traits:** Copy, SimpleToVerifyInSlice, Eq

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialEq**
  - `fn eq(self: &Self, other: &MetadataVersion) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &MetadataVersion) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Default**
  - `fn default() -> MetadataVersion`
- **Clone**
  - `fn clone(self: &Self) -> MetadataVersion`
- **EndianScalar**
  - `fn to_little_endian(self: Self) -> i16`
  - `fn from_little_endian(v: i16) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &MetadataVersion) -> $crate::cmp::Ordering`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Push**
  - `fn push(self: &Self, dst: & mut [u8], _written_len: usize)`



## arrow_ipc::gen::Schema::Null

*Struct*

These are stored in the flatbuffer in the Type union below

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, _args: &'args NullArgs) -> flatbuffers::WIPOffset<Null<'bldr>>`

**Traits:** Copy

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Null<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Null<'a>`



## arrow_ipc::gen::Schema::NullArgs

*Struct*

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::NullBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> NullBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Null<'a>>`



## arrow_ipc::gen::Schema::NullOffset

*Enum*



## arrow_ipc::gen::Schema::Precision

*Struct*

**Tuple Struct**: `(i16)`

**Methods:**

- `fn variant_name(self: Self) -> Option<&'static str>` - Returns the variant's name or "" if unknown.

**Traits:** SimpleToVerifyInSlice, Eq, Copy

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Precision) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Precision) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Default**
  - `fn default() -> Precision`
- **Clone**
  - `fn clone(self: &Self) -> Precision`
- **EndianScalar**
  - `fn to_little_endian(self: Self) -> i16`
  - `fn from_little_endian(v: i16) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &Precision) -> $crate::cmp::Ordering`
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



## arrow_ipc::gen::Schema::RunEndEncoded

*Struct*

Contains two child arrays, run_ends and values.
The run_ends child array must be a 16/32/64-bit integer array
which encodes the indices at which the run with the value in
each corresponding index in the values child array ends.
Like list/struct types, the value array can be of any type.

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, _args: &'args RunEndEncodedArgs) -> flatbuffers::WIPOffset<RunEndEncoded<'bldr>>`

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> RunEndEncoded<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &RunEndEncoded<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`



## arrow_ipc::gen::Schema::RunEndEncodedArgs

*Struct*

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::RunEndEncodedBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> RunEndEncodedBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<RunEndEncoded<'a>>`



## arrow_ipc::gen::Schema::RunEndEncodedOffset

*Enum*



## arrow_ipc::gen::Schema::Schema

*Struct*

----------------------------------------------------------------------
A Schema describes the columns in a row batch

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args SchemaArgs<'args>) -> flatbuffers::WIPOffset<Schema<'bldr>>`
- `fn endianness(self: &Self) -> Endianness` - endianness of the buffer
- `fn fields(self: &Self) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Field<'a>>>>`
- `fn custom_metadata(self: &Self) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<KeyValue<'a>>>>`
- `fn features(self: &Self) -> Option<flatbuffers::Vector<'a, Feature>>` - Features used in the stream/file.

**Traits:** Copy

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Schema<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Schema<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::Schema::SchemaArgs

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `endianness: Endianness`
- `fields: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Field<'a>>>>>`
- `custom_metadata: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<KeyValue<'a>>>>>`
- `features: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, Feature>>>`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::SchemaBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_endianness(self: & mut Self, endianness: Endianness)`
- `fn add_fields(self: & mut Self, fields: flatbuffers::WIPOffset<flatbuffers::Vector<'b, flatbuffers::ForwardsUOffset<Field<'b>>>>)`
- `fn add_custom_metadata(self: & mut Self, custom_metadata: flatbuffers::WIPOffset<flatbuffers::Vector<'b, flatbuffers::ForwardsUOffset<KeyValue<'b>>>>)`
- `fn add_features(self: & mut Self, features: flatbuffers::WIPOffset<flatbuffers::Vector<'b, Feature>>)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> SchemaBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Schema<'a>>`



## arrow_ipc::gen::Schema::SchemaOffset

*Enum*



## arrow_ipc::gen::Schema::Struct_

*Struct*

A Struct_ in the flatbuffer metadata is the same as an Arrow Struct
(according to the physical memory layout). We used Struct_ here as
Struct is a reserved word in Flatbuffers

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, _args: &'args Struct_Args) -> flatbuffers::WIPOffset<Struct_<'bldr>>`

**Traits:** Copy

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Struct_<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Struct_<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`



## arrow_ipc::gen::Schema::Struct_Args

*Struct*

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::Struct_Builder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> Struct_Builder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Struct_<'a>>`



## arrow_ipc::gen::Schema::Struct_Offset

*Enum*



## arrow_ipc::gen::Schema::Time

*Struct*

Time is either a 32-bit or 64-bit signed integer type representing an
elapsed time since midnight, stored in either of four units: seconds,
milliseconds, microseconds or nanoseconds.

The integer `bitWidth` depends on the `unit` and must be one of the following:
* SECOND and MILLISECOND: 32 bits
* MICROSECOND and NANOSECOND: 64 bits

The allowed values are between 0 (inclusive) and 86400 (=24*60*60) seconds
(exclusive), adjusted for the time unit (for example, up to 86400000
exclusive for the MILLISECOND unit).
This definition doesn't allow for leap seconds. Time values from
measurements with leap seconds will need to be corrected when ingesting
into Arrow (for example by replacing the value 86400 with 86399).

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args TimeArgs) -> flatbuffers::WIPOffset<Time<'bldr>>`
- `fn unit(self: &Self) -> TimeUnit`
- `fn bitWidth(self: &Self) -> i32`

**Traits:** Copy

**Trait Implementations:**

- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Time<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Time<'a>) -> bool`



## arrow_ipc::gen::Schema::TimeArgs

*Struct*

**Fields:**
- `unit: TimeUnit`
- `bitWidth: i32`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::TimeBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_unit(self: & mut Self, unit: TimeUnit)`
- `fn add_bitWidth(self: & mut Self, bitWidth: i32)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> TimeBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Time<'a>>`



## arrow_ipc::gen::Schema::TimeOffset

*Enum*



## arrow_ipc::gen::Schema::TimeUnit

*Struct*

**Tuple Struct**: `(i16)`

**Methods:**

- `fn variant_name(self: Self) -> Option<&'static str>` - Returns the variant's name or "" if unknown.

**Traits:** SimpleToVerifyInSlice, Eq, Copy

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &TimeUnit) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Default**
  - `fn default() -> TimeUnit`
- **Clone**
  - `fn clone(self: &Self) -> TimeUnit`
- **EndianScalar**
  - `fn to_little_endian(self: Self) -> i16`
  - `fn from_little_endian(v: i16) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &TimeUnit) -> $crate::cmp::Ordering`
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
  - `fn eq(self: &Self, other: &TimeUnit) -> bool`



## arrow_ipc::gen::Schema::Timestamp

*Struct*

Timestamp is a 64-bit signed integer representing an elapsed time since a
fixed epoch, stored in either of four units: seconds, milliseconds,
microseconds or nanoseconds, and is optionally annotated with a timezone.

Timestamp values do not include any leap seconds (in other words, all
days are considered 86400 seconds long).

Timestamps with a non-empty timezone
------------------------------------

If a Timestamp column has a non-empty timezone value, its epoch is
1970-01-01 00:00:00 (January 1st 1970, midnight) in the *UTC* timezone
(the Unix epoch), regardless of the Timestamp's own timezone.

Therefore, timestamp values with a non-empty timezone correspond to
physical points in time together with some additional information about
how the data was obtained and/or how to display it (the timezone).

  For example, the timestamp value 0 with the timezone string "Europe/Paris"
  corresponds to "January 1st 1970, 00h00" in the UTC timezone, but the
  application may prefer to display it as "January 1st 1970, 01h00" in
  the Europe/Paris timezone (which is the same physical point in time).

One consequence is that timestamp values with a non-empty timezone
can be compared and ordered directly, since they all share the same
well-known point of reference (the Unix epoch).

Timestamps with an unset / empty timezone
-----------------------------------------

If a Timestamp column has no timezone value, its epoch is
1970-01-01 00:00:00 (January 1st 1970, midnight) in an *unknown* timezone.

Therefore, timestamp values without a timezone cannot be meaningfully
interpreted as physical points in time, but only as calendar / clock
indications ("wall clock time") in an unspecified timezone.

  For example, the timestamp value 0 with an empty timezone string
  corresponds to "January 1st 1970, 00h00" in an unknown timezone: there
  is not enough information to interpret it as a well-defined physical
  point in time.

One consequence is that timestamp values without a timezone cannot
be reliably compared or ordered, since they may have different points of
reference.  In particular, it is *not* possible to interpret an unset
or empty timezone as the same as "UTC".

Conversion between timezones
----------------------------

If a Timestamp column has a non-empty timezone, changing the timezone
to a different non-empty value is a metadata-only operation:
the timestamp values need not change as their point of reference remains
the same (the Unix epoch).

However, if a Timestamp column has no timezone value, changing it to a
non-empty value requires to think about the desired semantics.
One possibility is to assume that the original timestamp values are
relative to the epoch of the timezone being set; timestamp values should
then adjusted to the Unix epoch (for example, changing the timezone from
empty to "Europe/Paris" would require converting the timestamp values
from "Europe/Paris" to "UTC", which seems counter-intuitive but is
nevertheless correct).

Guidelines for encoding data from external libraries
----------------------------------------------------

Date & time libraries often have multiple different data types for temporal
data. In order to ease interoperability between different implementations the
Arrow project has some recommendations for encoding these types into a Timestamp
column.

An "instant" represents a physical point in time that has no relevant timezone
(for example, astronomical data). To encode an instant, use a Timestamp with
the timezone string set to "UTC", and make sure the Timestamp values
are relative to the UTC epoch (January 1st 1970, midnight).

A "zoned date-time" represents a physical point in time annotated with an
informative timezone (for example, the timezone in which the data was
recorded).  To encode a zoned date-time, use a Timestamp with the timezone
string set to the name of the timezone, and make sure the Timestamp values
are relative to the UTC epoch (January 1st 1970, midnight).

 (There is some ambiguity between an instant and a zoned date-time with the
  UTC timezone.  Both of these are stored the same in Arrow.  Typically,
  this distinction does not matter.  If it does, then an application should
  use custom metadata or an extension type to distinguish between the two cases.)

An "offset date-time" represents a physical point in time combined with an
explicit offset from UTC.  To encode an offset date-time, use a Timestamp
with the timezone string set to the numeric timezone offset string
(e.g. "+03:00"), and make sure the Timestamp values are relative to
the UTC epoch (January 1st 1970, midnight).

A "naive date-time" (also called "local date-time" in some libraries)
represents a wall clock time combined with a calendar date, but with
no indication of how to map this information to a physical point in time.
Naive date-times must be handled with care because of this missing
information, and also because daylight saving time (DST) may make
some values ambiguous or nonexistent. A naive date-time may be
stored as a struct with Date and Time fields. However, it may also be
encoded into a Timestamp column with an empty timezone. The timestamp
values should be computed "as if" the timezone of the date-time values
was UTC; for example, the naive date-time "January 1st 1970, 00h00" would
be encoded as timestamp value 0.

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args TimestampArgs<'args>) -> flatbuffers::WIPOffset<Timestamp<'bldr>>`
- `fn unit(self: &Self) -> TimeUnit`
- `fn timezone(self: &Self) -> Option<&'a str>` - The timezone is an optional string indicating the name of a timezone,

**Traits:** Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Timestamp<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Timestamp<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`



## arrow_ipc::gen::Schema::TimestampArgs

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `unit: TimeUnit`
- `timezone: Option<flatbuffers::WIPOffset<&'a str>>`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::TimestampBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_unit(self: & mut Self, unit: TimeUnit)`
- `fn add_timezone(self: & mut Self, timezone: flatbuffers::WIPOffset<&'b str>)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> TimestampBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Timestamp<'a>>`



## arrow_ipc::gen::Schema::TimestampOffset

*Enum*



## arrow_ipc::gen::Schema::Type

*Struct*

----------------------------------------------------------------------
Top-level Type value, enabling extensible type-specific metadata. We can
add new logical types to Type without breaking backwards compatibility

**Tuple Struct**: `(u8)`

**Methods:**

- `fn variant_name(self: Self) -> Option<&'static str>` - Returns the variant's name or "" if unknown.

**Traits:** Eq, Copy, SimpleToVerifyInSlice

**Trait Implementations:**

- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialEq**
  - `fn eq(self: &Self, other: &Type) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Type) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Default**
  - `fn default() -> Type`
- **Clone**
  - `fn clone(self: &Self) -> Type`
- **EndianScalar**
  - `fn to_little_endian(self: Self) -> u8`
  - `fn from_little_endian(v: u8) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &Type) -> $crate::cmp::Ordering`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Push**
  - `fn push(self: &Self, dst: & mut [u8], _written_len: usize)`



## arrow_ipc::gen::Schema::TypeUnionTableOffset

*Struct*



## arrow_ipc::gen::Schema::Union

*Struct*

A union is a complex type with children in Field
By default ids in the type vector refer to the offsets in the children
optionally typeIds provides an indirection between the child offset and the type id
for each child `typeIds[offset]` is the id used in the type vector

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, args: &'args UnionArgs<'args>) -> flatbuffers::WIPOffset<Union<'bldr>>`
- `fn mode(self: &Self) -> UnionMode`
- `fn typeIds(self: &Self) -> Option<flatbuffers::Vector<'a, i32>>`

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Union<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Union<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`



## arrow_ipc::gen::Schema::UnionArgs

*Struct*

**Generic Parameters:**
- 'a

**Fields:**
- `mode: UnionMode`
- `typeIds: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, i32>>>`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::UnionBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn add_mode(self: & mut Self, mode: UnionMode)`
- `fn add_typeIds(self: & mut Self, typeIds: flatbuffers::WIPOffset<flatbuffers::Vector<'b, i32>>)`
- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> UnionBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Union<'a>>`



## arrow_ipc::gen::Schema::UnionMode

*Struct*

**Tuple Struct**: `(i16)`

**Methods:**

- `fn variant_name(self: Self) -> Option<&'static str>` - Returns the variant's name or "" if unknown.

**Traits:** Eq, Copy, SimpleToVerifyInSlice

**Trait Implementations:**

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
  - `fn eq(self: &Self, other: &UnionMode) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &UnionMode) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Default**
  - `fn default() -> UnionMode`
- **Clone**
  - `fn clone(self: &Self) -> UnionMode`
- **EndianScalar**
  - `fn to_little_endian(self: Self) -> i16`
  - `fn from_little_endian(v: i16) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &UnionMode) -> $crate::cmp::Ordering`



## arrow_ipc::gen::Schema::UnionOffset

*Enum*



## arrow_ipc::gen::Schema::Utf8

*Struct*

Unicode with UTF-8 encoding

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, _args: &'args Utf8Args) -> flatbuffers::WIPOffset<Utf8<'bldr>>`

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Utf8<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Utf8<'a>) -> bool`
- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`



## arrow_ipc::gen::Schema::Utf8Args

*Struct*

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::Utf8Builder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> Utf8Builder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Utf8<'a>>`



## arrow_ipc::gen::Schema::Utf8Offset

*Enum*



## arrow_ipc::gen::Schema::Utf8View

*Struct*

Logically the same as Utf8, but the internal representation uses a view
struct that contains the string length and either the string's entire data
inline (for small strings) or an inlined prefix, an index of another buffer,
and an offset pointing to a slice in that buffer (for non-small strings).

Since it uses a variable number of data buffers, each Field with this type
must have a corresponding entry in `variadicBufferCounts`.

**Generic Parameters:**
- 'a

**Fields:**
- `_tab: flatbuffers::Table<'a>`

**Methods:**

- `fn init_from_table(table: flatbuffers::Table<'a>) -> Self`
- `fn create<'bldr, 'args, 'mut_bldr, A>(_fbb: &'mut_bldr  mut flatbuffers::FlatBufferBuilder<'bldr, A>, _args: &'args Utf8ViewArgs) -> flatbuffers::WIPOffset<Utf8View<'bldr>>`

**Traits:** Copy

**Trait Implementations:**

- **Follow**
  - `fn follow(buf: &'a [u8], loc: usize) -> <Self as >::Inner`
- **Verifiable**
  - `fn run_verifier(v: & mut flatbuffers::Verifier, pos: usize) -> Result<(), flatbuffers::InvalidFlatbuffer>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut core::fmt::Formatter) -> core::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Utf8View<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Utf8View<'a>) -> bool`



## arrow_ipc::gen::Schema::Utf8ViewArgs

*Struct*

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## arrow_ipc::gen::Schema::Utf8ViewBuilder

*Struct*

**Generic Parameters:**
- 'a
- 'b
- A

**Methods:**

- `fn new(_fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>) -> Utf8ViewBuilder<'a, 'b, A>`
- `fn finish(self: Self) -> flatbuffers::WIPOffset<Utf8View<'a>>`



## arrow_ipc::gen::Schema::Utf8ViewOffset

*Enum*



## arrow_ipc::gen::Schema::finish_schema_buffer

*Function*

```rust
fn finish_schema_buffer<'a, 'b, A>(fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>, root: flatbuffers::WIPOffset<Schema<'a>>)
```



## arrow_ipc::gen::Schema::finish_size_prefixed_schema_buffer

*Function*

```rust
fn finish_size_prefixed_schema_buffer<'a, 'b, A>(fbb: &'b  mut flatbuffers::FlatBufferBuilder<'a, A>, root: flatbuffers::WIPOffset<Schema<'a>>)
```



## arrow_ipc::gen::Schema::root_as_schema

*Function*

Verifies that a buffer of bytes contains a `Schema`
and returns it.
Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`root_as_schema_unchecked`.

```rust
fn root_as_schema(buf: &[u8]) -> Result<Schema, flatbuffers::InvalidFlatbuffer>
```



## arrow_ipc::gen::Schema::root_as_schema_unchecked

*Function*

Assumes, without verification, that a buffer of bytes contains a Schema and returns it.
# Safety
Callers must trust the given bytes do indeed contain a valid `Schema`.

```rust
fn root_as_schema_unchecked(buf: &[u8]) -> Schema
```



## arrow_ipc::gen::Schema::root_as_schema_with_opts

*Function*

Verifies, with the given options, that a buffer of bytes
contains a `Schema` and returns it.
Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`root_as_schema_unchecked`.

```rust
fn root_as_schema_with_opts<'b, 'o>(opts: &'o flatbuffers::VerifierOptions, buf: &'b [u8]) -> Result<Schema<'b>, flatbuffers::InvalidFlatbuffer>
```



## arrow_ipc::gen::Schema::size_prefixed_root_as_schema

*Function*

Verifies that a buffer of bytes contains a size prefixed
`Schema` and returns it.
Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`size_prefixed_root_as_schema_unchecked`.

```rust
fn size_prefixed_root_as_schema(buf: &[u8]) -> Result<Schema, flatbuffers::InvalidFlatbuffer>
```



## arrow_ipc::gen::Schema::size_prefixed_root_as_schema_unchecked

*Function*

Assumes, without verification, that a buffer of bytes contains a size prefixed Schema and returns it.
# Safety
Callers must trust the given bytes do indeed contain a valid size prefixed `Schema`.

```rust
fn size_prefixed_root_as_schema_unchecked(buf: &[u8]) -> Schema
```



## arrow_ipc::gen::Schema::size_prefixed_root_as_schema_with_opts

*Function*

Verifies, with the given verifier options, that a buffer of
bytes contains a size prefixed `Schema` and returns
it. Note that verification is still experimental and may not
catch every error, or be maximally performant. For the
previous, unchecked, behavior use
`root_as_schema_unchecked`.

```rust
fn size_prefixed_root_as_schema_with_opts<'b, 'o>(opts: &'o flatbuffers::VerifierOptions, buf: &'b [u8]) -> Result<Schema<'b>, flatbuffers::InvalidFlatbuffer>
```



