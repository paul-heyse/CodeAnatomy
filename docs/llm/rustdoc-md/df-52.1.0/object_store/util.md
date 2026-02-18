**object_store > util**

# Module: util

## Contents

**Enums**

- [`GetRange`](#getrange) - Request only a portion of an object's bytes
- [`InvalidGetRange`](#invalidgetrange)

**Functions**

- [`coalesce_ranges`](#coalesce_ranges) - Takes a function `fetch` that can fetch a range of bytes and uses this to
- [`collect_bytes`](#collect_bytes) - Collect a stream into [`Bytes`] avoiding copying in the event of a single chunk

**Constants**

- [`OBJECT_STORE_COALESCE_DEFAULT`](#object_store_coalesce_default) - Range requests with a gap less than or equal to this,

---

## object_store::util::GetRange

*Enum*

Request only a portion of an object's bytes

These can be created from [usize] ranges, like

```rust
# use object_store::GetRange;
let range1: GetRange = (50..150).into();
let range2: GetRange = (50..=150).into();
let range3: GetRange = (50..).into();
let range4: GetRange = (..150).into();
```

Implementations may wish to inspect [`GetResult`] for the exact byte
range returned.

[`GetResult`]: crate::GetResult

**Variants:**
- `Bounded(std::ops::Range<u64>)` - Request a specific range of bytes
- `Offset(u64)` - Request all bytes starting from a given byte offset
- `Suffix(u64)` - Request up to the last n bytes

**Methods:**

- `fn is_valid(self: &Self) -> Result<(), InvalidGetRange>` - Check if the range is valid.
- `fn as_range(self: &Self, len: u64) -> Result<Range<u64>, InvalidGetRange>` - Convert to a [`Range`] if [valid](Self::is_valid).

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &GetRange) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> GetRange`
- **From**
  - `fn from(value: T) -> Self`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::util::InvalidGetRange

*Enum*

**Variants:**
- `StartTooLarge{ requested: u64, length: u64 }`
- `Inconsistent{ start: u64, end: u64 }`
- `TooLarge{ requested: u64, max: u64 }`



## object_store::util::OBJECT_STORE_COALESCE_DEFAULT

*Constant*: `u64`

Range requests with a gap less than or equal to this,
will be coalesced into a single request by [`coalesce_ranges`]



## object_store::util::coalesce_ranges

*Function*

Takes a function `fetch` that can fetch a range of bytes and uses this to
fetch the provided byte `ranges`

To improve performance it will:

* Combine ranges less than `coalesce` bytes apart into a single call to `fetch`
* Make multiple `fetch` requests in parallel (up to maximum of 10)


```rust
fn coalesce_ranges<F, E, Fut>(ranges: &[std::ops::Range<u64>], fetch: F, coalesce: u64) -> super::Result<Vec<bytes::Bytes>, E>
```



## object_store::util::collect_bytes

*Function*

Collect a stream into [`Bytes`] avoiding copying in the event of a single chunk

```rust
fn collect_bytes<S, E>(stream: S, size_hint: Option<u64>) -> super::Result<bytes::Bytes, E>
```



