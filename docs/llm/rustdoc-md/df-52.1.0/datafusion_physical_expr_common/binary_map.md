**datafusion_physical_expr_common > binary_map**

# Module: binary_map

## Contents

**Structs**

- [`ArrowBytesMap`](#arrowbytesmap) - Optimized map for storing Arrow "bytes" types (`String`, `LargeString`,
- [`ArrowBytesSet`](#arrowbytesset) - HashSet optimized for storing string or binary values that can produce that

**Enums**

- [`OutputType`](#outputtype) - Should the output be a String or Binary?

**Constants**

- [`INITIAL_BUFFER_CAPACITY`](#initial_buffer_capacity) - The initial size, in bytes, of the string data

---

## datafusion_physical_expr_common::binary_map::ArrowBytesMap

*Struct*

Optimized map for storing Arrow "bytes" types (`String`, `LargeString`,
`Binary`, and `LargeBinary`) values that can produce the set of keys on
output as `GenericBinaryArray` without copies.

Equivalent to `HashSet<String, V>` but with better performance if you need
to emit the keys as an Arrow `StringArray` / `BinaryArray`. For other
purposes it is the same as a `HashMap<String, V>`

# Generic Arguments

* `O`: OffsetSize (String/LargeString)
* `V`: payload type

# Description

This is a specialized HashMap with the following properties:

1. Optimized for storing and emitting Arrow byte types  (e.g.
   `StringArray` / `BinaryArray`) very efficiently by minimizing copying of
   the string values themselves, both when inserting and when emitting the
   final array.


2. Retains the insertion order of entries in the final array. The values are
   in the same order as they were inserted.

Note this structure can be used as a `HashSet` by specifying the value type
as `()`, as is done by [`ArrowBytesSet`].

This map is used by the special `COUNT DISTINCT` aggregate function to
store the distinct values, and by the `GROUP BY` operator to store
group values when they are a single string array.

# Example

The following diagram shows how the map would store the four strings
"Foo", NULL, "Bar", "TheQuickBrownFox":

* `hashtable` stores entries for each distinct string that has been
  inserted. The entries contain the payload as well as information about the
  value (either an offset or the actual bytes, see `Entry` docs for more
  details)

* `offsets` stores offsets into `buffer` for each distinct string value,
  following the same convention as the offsets in a `StringArray` or
  `LargeStringArray`.

* `buffer` stores the actual byte data

* `null`: stores the index and payload of the null value, in this case the
  second value (index 1)

```text
┌───────────────────────────────────┐    ┌─────┐    ┌────┐
│                ...                │    │  0  │    │FooB│
│ ┌──────────────────────────────┐  │    │  0  │    │arTh│
│ │      <Entry for "Bar">       │  │    │  3  │    │eQui│
│ │            len: 3            │  │    │  3  │    │ckBr│
│ │   offset_or_inline: "Bar"    │  │    │  6  │    │ownF│
│ │         payload:...          │  │    │     │    │ox  │
│ └──────────────────────────────┘  │    │     │    │    │
│                ...                │    └─────┘    └────┘
│ ┌──────────────────────────────┐  │
│ │<Entry for "TheQuickBrownFox">│  │    offsets    buffer
│ │           len: 16            │  │
│ │     offset_or_inline: 6      │  │    ┌───────────────┐
│ │         payload: ...         │  │    │    Some(1)    │
│ └──────────────────────────────┘  │    │ payload: ...  │
│                ...                │    └───────────────┘
└───────────────────────────────────┘
                                             null
              HashTable
```

# Entry Format

Entries stored in a [`ArrowBytesMap`] represents a value that is either
stored inline or in the buffer

This helps the case where there are many short (less than 8 bytes) strings
that are the same (e.g. "MA", "CA", "NY", "TX", etc)

```text
                                                               ┌──────────────────┐
                                                 ─ ─ ─ ─ ─ ─ ─▶│...               │
                                                │              │TheQuickBrownFox  │
                                                               │...               │
                                                │              │                  │
                                                               └──────────────────┘
                                                │               buffer of u8

                                                │
                       ┌────────────────┬───────────────┬───────────────┐
 Storing               │                │ starting byte │  length, in   │
 "TheQuickBrownFox"    │   hash value   │   offset in   │  bytes (not   │
 (long string)         │                │    buffer     │  characters)  │
                       └────────────────┴───────────────┴───────────────┘
                             8 bytes          8 bytes       4 or 8


                        ┌───────────────┬─┬─┬─┬─┬─┬─┬─┬─┬───────────────┐
Storing "foobar"        │               │ │ │ │ │ │ │ │ │  length, in   │
(short string)          │  hash value   │?│?│f│o│o│b│a│r│  bytes (not   │
                        │               │ │ │ │ │ │ │ │ │  characters)  │
                        └───────────────┴─┴─┴─┴─┴─┴─┴─┴─┴───────────────┘
                             8 bytes         8 bytes        4 or 8
```

**Generic Parameters:**
- O
- V

**Methods:**

- `fn new(output_type: OutputType) -> Self`
- `fn take(self: & mut Self) -> Self` - Return the contents of this map and replace it with a new empty map with
- `fn insert_if_new<MP, OP>(self: & mut Self, values: &ArrayRef, make_payload_fn: MP, observe_payload_fn: OP)` - Inserts each value from `values` into the map, invoking `payload_fn` for
- `fn into_state(self: Self) -> ArrayRef` - Converts this set into a `StringArray`, `LargeStringArray`,
- `fn len(self: &Self) -> usize` - Total number of entries (including null, if present)
- `fn is_empty(self: &Self) -> bool` - Is the set empty?
- `fn non_null_len(self: &Self) -> usize` - Number of non null entries
- `fn size(self: &Self) -> usize` - Return the total size, in bytes, of memory used to store the data in

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`



## datafusion_physical_expr_common::binary_map::ArrowBytesSet

*Struct*

HashSet optimized for storing string or binary values that can produce that
the final set as a GenericStringArray with minimal copies.

**Generic Parameters:**
- O

**Tuple Struct**: `()`

**Methods:**

- `fn new(output_type: OutputType) -> Self`
- `fn take(self: & mut Self) -> Self` - Return the contents of this set and replace it with a new empty
- `fn insert(self: & mut Self, values: &ArrayRef)` - Inserts each value from `values` into the set
- `fn into_state(self: Self) -> ArrayRef` - Converts this set into a `StringArray`/`LargeStringArray` or
- `fn len(self: &Self) -> usize` - Returns the total number of distinct values (including nulls) seen so far
- `fn is_empty(self: &Self) -> bool`
- `fn non_null_len(self: &Self) -> usize` - returns the total number of distinct values (not including nulls) seen so far
- `fn size(self: &Self) -> usize` - Return the total size, in bytes, of memory used to store the data in

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_physical_expr_common::binary_map::INITIAL_BUFFER_CAPACITY

*Constant*: `usize`

The initial size, in bytes, of the string data



## datafusion_physical_expr_common::binary_map::OutputType

*Enum*

Should the output be a String or Binary?

**Variants:**
- `Utf8` - `StringArray` or `LargeStringArray`
- `Utf8View` - `StringViewArray`
- `Binary` - `BinaryArray` or `LargeBinaryArray`
- `BinaryView` - `BinaryViewArray`

**Traits:** Copy, Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &OutputType) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> OutputType`



