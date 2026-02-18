**object_store > path**

# Module: path

## Contents

**Structs**

- [`Path`](#path) - A parsed path representation that can be safely written to object storage

**Enums**

- [`Error`](#error) - Error returned by [`Path::parse`]

**Constants**

- [`DELIMITER`](#delimiter) - The delimiter to separate object namespaces, creating a directory structure.
- [`DELIMITER_BYTE`](#delimiter_byte) - The path delimiter as a single byte

---

## object_store::path::DELIMITER

*Constant*: `&str`

The delimiter to separate object namespaces, creating a directory structure.



## object_store::path::DELIMITER_BYTE

*Constant*: `u8`

The path delimiter as a single byte



## object_store::path::Error

*Enum*

Error returned by [`Path::parse`]

**Variants:**
- `EmptySegment{ path: String }` - Error when there's an empty segment between two slashes `/` in the path
- `BadSegment{ path: String, source: InvalidPart }` - Error when an invalid segment is encountered in the given path
- `Canonicalize{ path: std::path::PathBuf, source: std::io::Error }` - Error when path cannot be canonicalized
- `InvalidPath{ path: std::path::PathBuf }` - Error when the path is not a valid URL
- `NonUnicode{ path: String, source: std::str::Utf8Error }` - Error when a path contains non-unicode characters
- `PrefixMismatch{ path: String, prefix: String }` - Error when the a path doesn't start with given prefix

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, __formatter: & mut ::core::fmt::Formatter) -> ::core::fmt::Result`
- **Error**
  - `fn source(self: &Self) -> ::core::option::Option<&dyn ::thiserror::__private18::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::path::Path

*Struct*

A parsed path representation that can be safely written to object storage

A [`Path`] maintains the following invariants:

* Paths are delimited by `/`
* Paths do not contain leading or trailing `/`
* Paths do not contain relative path segments, i.e. `.` or `..`
* Paths do not contain empty path segments
* Paths do not contain any ASCII control characters

There are no enforced restrictions on path length, however, it should be noted that most
object stores do not permit paths longer than 1024 bytes, and many filesystems do not
support path segments longer than 255 bytes.

# Encode

In theory object stores support any UTF-8 character sequence, however, certain character
sequences cause compatibility problems with some applications and protocols. Additionally
some filesystems may impose character restrictions, see [`LocalFileSystem`]. As such the
naming guidelines for [S3], [GCS] and [Azure Blob Storage] all recommend sticking to a
limited character subset.

[S3]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
[GCS]: https://cloud.google.com/storage/docs/naming-objects
[Azure Blob Storage]: https://docs.microsoft.com/en-us/rest/api/storageservices/Naming-and-Referencing-Containers--Blobs--and-Metadata#blob-names

A string containing potentially problematic path segments can therefore be encoded to a [`Path`]
using [`Path::from`] or [`Path::from_iter`]. This will percent encode any problematic
segments according to [RFC 1738].

```
# use object_store::path::Path;
assert_eq!(Path::from("foo/bar").as_ref(), "foo/bar");
assert_eq!(Path::from("foo//bar").as_ref(), "foo/bar");
assert_eq!(Path::from("foo/../bar").as_ref(), "foo/%2E%2E/bar");
assert_eq!(Path::from("/").as_ref(), "");
assert_eq!(Path::from_iter(["foo", "foo/bar"]).as_ref(), "foo/foo%2Fbar");
```

Note: if provided with an already percent encoded string, this will encode it again

```
# use object_store::path::Path;
assert_eq!(Path::from("foo/foo%2Fbar").as_ref(), "foo/foo%252Fbar");
```

# Parse

Alternatively a [`Path`] can be parsed from an existing string, returning an
error if it is invalid. Unlike the encoding methods above, this will permit
arbitrary unicode, including percent encoded sequences.

```
# use object_store::path::Path;
assert_eq!(Path::parse("/foo/foo%2Fbar").unwrap().as_ref(), "foo/foo%2Fbar");
Path::parse("..").unwrap_err(); // Relative path segments are disallowed
Path::parse("/foo//").unwrap_err(); // Empty path segments are disallowed
Path::parse("\x00").unwrap_err(); // ASCII control characters are disallowed
```

[RFC 1738]: https://www.ietf.org/rfc/rfc1738.txt
[`LocalFileSystem`]: crate::local::LocalFileSystem

**Methods:**

- `fn parse<impl AsRef<str>>(path: impl Trait) -> Result<Self, Error>` - Parse a string as a [`Path`], returning a [`Error`] if invalid,
- `fn from_filesystem_path<impl AsRef<std::path::Path>>(path: impl Trait) -> Result<Self, Error>` - Convert a filesystem path to a [`Path`] relative to the filesystem root
- `fn from_absolute_path<impl AsRef<std::path::Path>>(path: impl Trait) -> Result<Self, Error>` - Convert an absolute filesystem path to a [`Path`] relative to the filesystem root
- `fn from_url_path<impl AsRef<str>>(path: impl Trait) -> Result<Self, Error>` - Parse a url encoded string as a [`Path`], returning a [`Error`] if invalid
- `fn parts(self: &Self) -> impl Trait` - Returns the [`PathPart`] of this [`Path`]
- `fn filename(self: &Self) -> Option<&str>` - Returns the last path segment containing the filename stored in this [`Path`]
- `fn extension(self: &Self) -> Option<&str>` - Returns the extension of the file stored in this [`Path`], if any
- `fn prefix_match(self: &Self, prefix: &Self) -> Option<impl Trait>` - Returns an iterator of the [`PathPart`] of this [`Path`] after `prefix`
- `fn prefix_matches(self: &Self, prefix: &Self) -> bool` - Returns true if this [`Path`] starts with `prefix`
- `fn child<'a, impl Into<PathPart<'a>>>(self: &Self, child: impl Trait) -> Self` - Creates a new child of this [`Path`]

**Traits:** Eq

**Trait Implementations:**

- **Ord**
  - `fn cmp(self: &Self, other: &Path) -> $crate::cmp::Ordering`
- **Default**
  - `fn default() -> Path`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> std::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Path) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Clone**
  - `fn clone(self: &Self) -> Path`
- **AsRef**
  - `fn as_ref(self: &Self) -> &str`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(path: String) -> Self`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Path) -> $crate::option::Option<$crate::cmp::Ordering>`
- **FromIterator**
  - `fn from_iter<T>(iter: T) -> Self`
- **From**
  - `fn from(path: &str) -> Self`



