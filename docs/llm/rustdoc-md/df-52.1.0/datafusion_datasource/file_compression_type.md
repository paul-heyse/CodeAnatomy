**datafusion_datasource > file_compression_type**

# Module: file_compression_type

## Contents

**Structs**

- [`FileCompressionType`](#filecompressiontype) - Readable file compression type

**Traits**

- [`FileTypeExt`](#filetypeext) - Trait for extending the functionality of the `FileType` enum.

---

## datafusion_datasource::file_compression_type::FileCompressionType

*Struct*

Readable file compression type

**Methods:**

- `fn get_variant(self: &Self) -> &CompressionTypeVariant` - Read only access to self.variant
- `fn is_compressed(self: &Self) -> bool` - The file is compressed or not
- `fn convert_to_compress_stream<'a>(self: &Self, s: BoxStream<'a, Result<Bytes>>) -> Result<BoxStream<'a, Result<Bytes>>>` - Given a `Stream`, create a `Stream` which data are compressed with `FileCompressionType`.
- `fn convert_async_writer(self: &Self, w: BufWriter) -> Result<Box<dyn AsyncWrite>>` - Wrap the given `BufWriter` so that it performs compressed writes
- `fn convert_async_writer_with_level(self: &Self, w: BufWriter, compression_level: Option<u32>) -> Result<Box<dyn AsyncWrite>>` - Wrap the given `BufWriter` so that it performs compressed writes
- `fn convert_stream<'a>(self: &Self, s: BoxStream<'a, Result<Bytes>>) -> Result<BoxStream<'a, Result<Bytes>>>` - Given a `Stream`, create a `Stream` which data are decompressed with `FileCompressionType`.
- `fn convert_read<T>(self: &Self, r: T) -> Result<Box<dyn std::io::Read>>` - Given a `Read`, create a `Read` which data are decompressed with `FileCompressionType`.

**Traits:** Eq, Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **GetExt**
  - `fn get_ext(self: &Self) -> String`
- **PartialEq**
  - `fn eq(self: &Self, other: &FileCompressionType) -> bool`
- **From**
  - `fn from(t: CompressionTypeVariant) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> FileCompressionType`
- **FromStr**
  - `fn from_str(s: &str) -> Result<Self>`



## datafusion_datasource::file_compression_type::FileTypeExt

*Trait*

Trait for extending the functionality of the `FileType` enum.

**Methods:**

- `get_ext_with_compression`: Given a `FileCompressionType`, return the `FileType`'s extension with compression suffix



