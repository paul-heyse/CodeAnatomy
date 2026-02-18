**object_store > local**

# Module: local

## Contents

**Structs**

- [`LocalFileSystem`](#localfilesystem) - Local filesystem storage providing an [`ObjectStore`] interface to files on

---

## object_store::local::LocalFileSystem

*Struct*

Local filesystem storage providing an [`ObjectStore`] interface to files on
local disk. Can optionally be created with a directory prefix

# Path Semantics

This implementation follows the [file URI] scheme outlined in [RFC 3986]. In
particular paths are delimited by `/`

[file URI]: https://en.wikipedia.org/wiki/File_URI_scheme
[RFC 3986]: https://www.rfc-editor.org/rfc/rfc3986

# Path Semantics

[`LocalFileSystem`] will expose the path semantics of the underlying filesystem, which may
have additional restrictions beyond those enforced by [`Path`].

For example:

* Windows forbids certain filenames, e.g. `COM0`,
* Windows forbids folders with trailing `.`
* Windows forbids certain ASCII characters, e.g. `<` or `|`
* OS X forbids filenames containing `:`
* Leading `-` are discouraged on Unix systems where they may be interpreted as CLI flags
* Filesystems may have restrictions on the maximum path or path segment length
* Filesystem support for non-ASCII characters is inconsistent

Additionally some filesystems, such as NTFS, are case-insensitive, whilst others like
FAT don't preserve case at all. Further some filesystems support non-unicode character
sequences, such as unpaired UTF-16 surrogates, and [`LocalFileSystem`] will error on
encountering such sequences.

Finally, filenames matching the regex `/.*#\d+/`, e.g. `foo.parquet#123`, are not supported
by [`LocalFileSystem`] as they are used to provide atomic writes. Such files will be ignored
for listing operations, and attempting to address such a file will error.

# Tokio Compatibility

Tokio discourages performing blocking IO on a tokio worker thread, however,
no major operating systems have stable async file APIs. Therefore if called from
a tokio context, this will use [`tokio::runtime::Handle::spawn_blocking`] to dispatch
IO to a blocking thread pool, much like `tokio::fs` does under-the-hood.

If not called from a tokio context, this will perform IO on the current thread with
no additional complexity or overheads

# Symlinks

[`LocalFileSystem`] will follow symlinks as normal, however, it is worth noting:

* Broken symlinks will be silently ignored by listing operations
* No effort is made to prevent breaking symlinks when deleting files
* Symlinks that resolve to paths outside the root **will** be followed
* Mutating a file through one or more symlinks will mutate the underlying file
* Deleting a path that resolves to a symlink will only delete the symlink

# Cross-Filesystem Copy

[`LocalFileSystem::copy`] is implemented using [`std::fs::hard_link`], and therefore
does not support copying across filesystem boundaries.


**Methods:**

- `fn new() -> Self` - Create new filesystem storage with no prefix
- `fn new_with_prefix<impl AsRef<std::path::Path>>(prefix: impl Trait) -> Result<Self>` - Create new filesystem storage with `prefix` applied to all paths
- `fn path_to_filesystem(self: &Self, location: &Path) -> Result<PathBuf>` - Return an absolute filesystem path of the given file location
- `fn with_automatic_cleanup(self: Self, automatic_cleanup: bool) -> Self` - Enable automatic cleanup of empty directories when deleting files

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **ObjectStore**
  - `fn put_opts(self: &'life0 Self, location: &'life1 Path, payload: PutPayload, opts: PutOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_multipart_opts(self: &'life0 Self, location: &'life1 Path, opts: PutMultipartOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get_opts(self: &'life0 Self, location: &'life1 Path, options: GetOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get_range(self: &'life0 Self, location: &'life1 Path, range: Range<u64>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get_ranges(self: &'life0 Self, location: &'life1 Path, ranges: &'life2 [Range<u64>]) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn delete(self: &'life0 Self, location: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn list(self: &Self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>>`
  - `fn list_with_offset(self: &Self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>>`
  - `fn list_with_delimiter(self: &'life0 Self, prefix: Option<&'life1 Path>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn rename(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy_if_not_exists(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



