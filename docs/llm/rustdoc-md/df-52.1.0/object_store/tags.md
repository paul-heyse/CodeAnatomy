**object_store > tags**

# Module: tags

## Contents

**Structs**

- [`TagSet`](#tagset) - A collection of key value pairs used to annotate objects

---

## object_store::tags::TagSet

*Struct*

A collection of key value pairs used to annotate objects

<https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html>
<https://learn.microsoft.com/en-us/rest/api/storageservices/set-blob-tags>

**Tuple Struct**: `()`

**Methods:**

- `fn push(self: & mut Self, key: &str, value: &str)` - Append a key value pair to this [`TagSet`]
- `fn encoded(self: &Self) -> &str` - Return this [`TagSet`] as a URL-encoded string

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> TagSet`
- **Clone**
  - `fn clone(self: &Self) -> TagSet`
- **PartialEq**
  - `fn eq(self: &Self, other: &TagSet) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



