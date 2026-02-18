**object_store > payload**

# Module: payload

## Contents

**Structs**

- [`PutPayload`](#putpayload) - A cheaply cloneable, ordered collection of [`Bytes`]
- [`PutPayloadIntoIter`](#putpayloadintoiter) - An owning iterator of [`PutPayload`]
- [`PutPayloadIter`](#putpayloaditer) - An iterator over [`PutPayload`]
- [`PutPayloadMut`](#putpayloadmut) - A builder for [`PutPayload`] that avoids reallocating memory

---

## object_store::payload::PutPayload

*Struct*

A cheaply cloneable, ordered collection of [`Bytes`]

**Tuple Struct**: `()`

**Methods:**

- `fn new() -> Self` - Create a new empty [`PutPayload`]
- `fn from_static(s: &'static [u8]) -> Self` - Creates a [`PutPayload`] from a static slice
- `fn from_bytes(s: Bytes) -> Self` - Creates a [`PutPayload`] from a [`Bytes`]
- `fn content_length(self: &Self) -> usize` - Returns the total length of the [`Bytes`] in this payload
- `fn iter(self: &Self) -> PutPayloadIter` - Returns an iterator over the [`Bytes`] in this payload

**Trait Implementations:**

- **FromIterator**
  - `fn from_iter<T>(iter: T) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(value: Vec<u8>) -> Self`
- **AsRef**
  - `fn as_ref(self: &Self) -> &[Bytes]`
- **From**
  - `fn from(value: String) -> Self`
- **From**
  - `fn from(value: Bytes) -> Self`
- **Default**
  - `fn default() -> Self`
- **From**
  - `fn from(value: &'static [u8]) -> Self`
- **IntoIterator**
  - `fn into_iter(self: Self) -> <Self as >::IntoIter`
- **From**
  - `fn from(value: PutPayloadMut) -> Self`
- **FromIterator**
  - `fn from_iter<T>(iter: T) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> PutPayload`
- **From**
  - `fn from(value: &'static str) -> Self`



## object_store::payload::PutPayloadIntoIter

*Struct*

An owning iterator of [`PutPayload`]

**Trait Implementations:**

- **Iterator**
  - `fn next(self: & mut Self) -> Option<<Self as >::Item>`
  - `fn size_hint(self: &Self) -> (usize, Option<usize>)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::payload::PutPayloadIter

*Struct*

An iterator over [`PutPayload`]

**Generic Parameters:**
- 'a

**Tuple Struct**: `()`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Iterator**
  - `fn next(self: & mut Self) -> Option<<Self as >::Item>`
  - `fn size_hint(self: &Self) -> (usize, Option<usize>)`



## object_store::payload::PutPayloadMut

*Struct*

A builder for [`PutPayload`] that avoids reallocating memory

Data is allocated in fixed blocks, which are flushed to [`Bytes`] once full.
Unlike [`Vec`] this avoids needing to repeatedly reallocate blocks of memory,
which typically involves copying all the previously written data to a new
contiguous memory region.

**Methods:**

- `fn new() -> Self` - Create a new [`PutPayloadMut`]
- `fn with_block_size(self: Self, block_size: usize) -> Self` - Configures the minimum allocation size
- `fn extend_from_slice(self: & mut Self, slice: &[u8])` - Write bytes into this [`PutPayloadMut`]
- `fn push(self: & mut Self, bytes: Bytes)` - Append a [`Bytes`] to this [`PutPayloadMut`] without copying
- `fn is_empty(self: &Self) -> bool` - Returns `true` if this [`PutPayloadMut`] contains no bytes
- `fn content_length(self: &Self) -> usize` - Returns the total length of the [`Bytes`] in this payload
- `fn freeze(self: Self) -> PutPayload` - Convert into [`PutPayload`]

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



