**object_store > buffered**

# Module: buffered

## Contents

**Structs**

- [`BufReader`](#bufreader) - An async-buffered reader compatible with the tokio IO traits
- [`BufWriter`](#bufwriter) - An async buffered writer compatible with the tokio IO traits

**Constants**

- [`DEFAULT_BUFFER_SIZE`](#default_buffer_size) - The default buffer size used by [`BufReader`]

---

## object_store::buffered::BufReader

*Struct*

An async-buffered reader compatible with the tokio IO traits

Internally this maintains a buffer of the requested size, and uses [`ObjectStore::get_range`]
to populate its internal buffer once depleted. This buffer is cleared on seek.

Whilst simple, this interface will typically be outperformed by the native [`ObjectStore`]
methods that better map to the network APIs. This is because most object stores have
very [high first-byte latencies], on the order of 100-200ms, and so avoiding unnecessary
round-trips is critical to throughput.

Systems looking to sequentially scan a file should instead consider using [`ObjectStore::get`],
or [`ObjectStore::get_opts`], or [`ObjectStore::get_range`] to read a particular range.

Systems looking to read multiple ranges of a file should instead consider using
[`ObjectStore::get_ranges`], which will optimise the vectored IO.

[high first-byte latencies]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html

**Methods:**

- `fn new(store: Arc<dyn ObjectStore>, meta: &ObjectMeta) -> Self` - Create a new [`BufReader`] from the provided [`ObjectMeta`] and [`ObjectStore`]
- `fn with_capacity(store: Arc<dyn ObjectStore>, meta: &ObjectMeta, capacity: usize) -> Self` - Create a new [`BufReader`] from the provided [`ObjectMeta`], [`ObjectStore`], and `capacity`

**Trait Implementations:**

- **AsyncSeek**
  - `fn start_seek(self: Pin<& mut Self>, position: SeekFrom) -> std::io::Result<()>`
  - `fn poll_complete(self: Pin<& mut Self>, _cx: & mut Context) -> Poll<std::io::Result<u64>>`
- **AsyncRead**
  - `fn poll_read(self: Pin<& mut Self>, cx: & mut Context, out: & mut ReadBuf) -> Poll<std::io::Result<()>>`
- **AsyncBufRead**
  - `fn poll_fill_buf(self: Pin<& mut Self>, cx: & mut Context) -> Poll<std::io::Result<&[u8]>>`
  - `fn consume(self: Pin<& mut Self>, amt: usize)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`



## object_store::buffered::BufWriter

*Struct*

An async buffered writer compatible with the tokio IO traits

This writer adaptively uses [`ObjectStore::put`] or
[`ObjectStore::put_multipart`] depending on the amount of data that has
been written.

Up to `capacity` bytes will be buffered in memory, and flushed on shutdown
using [`ObjectStore::put`]. If `capacity` is exceeded, data will instead be
streamed using [`ObjectStore::put_multipart`]

**Methods:**

- `fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self` - Create a new [`BufWriter`] from the provided [`ObjectStore`] and [`Path`]
- `fn with_capacity(store: Arc<dyn ObjectStore>, path: Path, capacity: usize) -> Self` - Create a new [`BufWriter`] from the provided [`ObjectStore`], [`Path`] and `capacity`
- `fn with_max_concurrency(self: Self, max_concurrency: usize) -> Self` - Override the maximum number of in-flight requests for this writer
- `fn with_attributes(self: Self, attributes: Attributes) -> Self` - Set the attributes of the uploaded object
- `fn with_tags(self: Self, tags: TagSet) -> Self` - Set the tags of the uploaded object
- `fn with_extensions(self: Self, extensions: Extensions) -> Self` - Set the extensions of the uploaded object
- `fn put(self: & mut Self, bytes: Bytes) -> crate::Result<()>` - Write data to the writer in [`Bytes`].
- `fn abort(self: & mut Self) -> crate::Result<()>` - Abort this writer, cleaning up any partially uploaded state

**Trait Implementations:**

- **AsyncWrite**
  - `fn poll_write(self: Pin<& mut Self>, cx: & mut Context, buf: &[u8]) -> Poll<Result<usize, Error>>`
  - `fn poll_flush(self: Pin<& mut Self>, cx: & mut Context) -> Poll<Result<(), Error>>`
  - `fn poll_shutdown(self: Pin<& mut Self>, cx: & mut Context) -> Poll<Result<(), Error>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`



## object_store::buffered::DEFAULT_BUFFER_SIZE

*Constant*: `usize`

The default buffer size used by [`BufReader`]



