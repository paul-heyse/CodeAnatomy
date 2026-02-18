**object_store > path > parts**

# Module: path::parts

## Contents

**Structs**

- [`InvalidPart`](#invalidpart) - Error returned by [`PathPart::parse`]
- [`PathPart`](#pathpart) - The PathPart type exists to validate the directory/file names that form part

---

## object_store::path::parts::InvalidPart

*Struct*

Error returned by [`PathPart::parse`]

**Traits:** Error

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, __formatter: & mut ::core::fmt::Formatter) -> ::core::fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::path::parts::PathPart

*Struct*

The PathPart type exists to validate the directory/file names that form part
of a path.

A [`PathPart`] is guaranteed to:

* Contain no ASCII control characters or `/`
* Not be a relative path segment, i.e. `.` or `..`

**Generic Parameters:**
- 'a

**Methods:**

- `fn parse(segment: &'a str) -> Result<Self, InvalidPart>` - Parse the provided path segment as a [`PathPart`] returning an error if invalid

**Traits:** Eq

**Trait Implementations:**

- **AsRef**
  - `fn as_ref(self: &Self) -> &str`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **From**
  - `fn from(s: String) -> Self`
- **Default**
  - `fn default() -> PathPart<'a>`
- **Clone**
  - `fn clone(self: &Self) -> PathPart<'a>`
- **From**
  - `fn from(v: &'a str) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &PathPart<'a>) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &PathPart<'a>) -> $crate::option::Option<$crate::cmp::Ordering>`
- **From**
  - `fn from(v: &'a [u8]) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &PathPart<'a>) -> $crate::cmp::Ordering`



