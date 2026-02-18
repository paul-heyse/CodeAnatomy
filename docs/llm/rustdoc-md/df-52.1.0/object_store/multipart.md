**object_store > multipart**

# Module: multipart

## Contents

**Structs**

- [`PartId`](#partid) - Represents a part of a file that has been successfully uploaded in a multipart upload process.

**Traits**

- [`MultipartStore`](#multipartstore) - A low-level interface for interacting with multipart upload APIs

---

## object_store::multipart::MultipartStore

*Trait*

A low-level interface for interacting with multipart upload APIs

Most use-cases should prefer [`ObjectStore::put_multipart`] as this is supported by more
backends, including [`LocalFileSystem`], and automatically handles uploading fixed
size parts of sufficient size in parallel

[`ObjectStore::put_multipart`]: crate::ObjectStore::put_multipart
[`LocalFileSystem`]: crate::local::LocalFileSystem

**Methods:**

- `create_multipart`: Creates a new multipart upload, returning the [`MultipartId`]
- `put_part`: Uploads a new part with index `part_idx`
- `complete_multipart`: Completes a multipart upload
- `abort_multipart`: Aborts a multipart upload



## object_store::multipart::PartId

*Struct*

Represents a part of a file that has been successfully uploaded in a multipart upload process.

**Fields:**
- `content_id: String` - Id of this part

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> PartId`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



