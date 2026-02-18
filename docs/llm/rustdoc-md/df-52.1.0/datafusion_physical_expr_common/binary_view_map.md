**datafusion_physical_expr_common > binary_view_map**

# Module: binary_view_map

## Contents

**Structs**

- [`ArrowBytesViewMap`](#arrowbytesviewmap) - Optimized map for storing Arrow "byte view" types (`StringView`, `BinaryView`)
- [`ArrowBytesViewSet`](#arrowbytesviewset) - HashSet optimized for storing string or binary values that can produce that

---

## datafusion_physical_expr_common::binary_view_map::ArrowBytesViewMap

*Struct*

Optimized map for storing Arrow "byte view" types (`StringView`, `BinaryView`)
values that can produce the set of keys on
output as `GenericBinaryViewArray` without copies.

Equivalent to `HashSet<String, V>` but with better performance if you need
to emit the keys as an Arrow `StringViewArray` / `BinaryViewArray`. For other
purposes it is the same as a `HashMap<String, V>`

# Generic Arguments

* `V`: payload type

# Description

This is a specialized HashMap with the following properties:

1. Optimized for storing and emitting Arrow byte types  (e.g.
   `StringViewArray` / `BinaryViewArray`) very efficiently by minimizing copying of
   the string values themselves, both when inserting and when emitting the
   final array.

2. Retains the insertion order of entries in the final array. The values are
   in the same order as they were inserted.

Note this structure can be used as a `HashSet` by specifying the value type
as `()`, as is done by [`ArrowBytesViewSet`].

This map is used by the special `COUNT DISTINCT` aggregate function to
store the distinct values, and by the `GROUP BY` operator to store
group values when they are a single string array.

**Generic Parameters:**
- V

**Methods:**

- `fn new(output_type: OutputType) -> Self`
- `fn take(self: & mut Self) -> Self` - Return the contents of this map and replace it with a new empty map with
- `fn insert_if_new<MP, OP>(self: & mut Self, values: &ArrayRef, make_payload_fn: MP, observe_payload_fn: OP)` - Inserts each value from `values` into the map, invoking `payload_fn` for
- `fn into_state(self: Self) -> ArrayRef` - Converts this set into a `StringViewArray`, or `BinaryViewArray`,
- `fn len(self: &Self) -> usize` - Total number of entries (including null, if present)
- `fn is_empty(self: &Self) -> bool` - Is the set empty?
- `fn non_null_len(self: &Self) -> usize` - Number of non null entries
- `fn size(self: &Self) -> usize` - Return the total size, in bytes, of memory used to store the data in

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`



## datafusion_physical_expr_common::binary_view_map::ArrowBytesViewSet

*Struct*

HashSet optimized for storing string or binary values that can produce that
the final set as a `GenericBinaryViewArray` with minimal copies.

**Tuple Struct**: `()`

**Methods:**

- `fn new(output_type: OutputType) -> Self`
- `fn insert(self: & mut Self, values: &ArrayRef)` - Inserts each value from `values` into the set
- `fn take(self: & mut Self) -> Self` - Return the contents of this map and replace it with a new empty map with
- `fn into_state(self: Self) -> ArrayRef` - Converts this set into a `StringViewArray` or `BinaryViewArray`
- `fn len(self: &Self) -> usize` - Returns the total number of distinct values (including nulls) seen so far
- `fn is_empty(self: &Self) -> bool`
- `fn non_null_len(self: &Self) -> usize` - returns the total number of distinct values (not including nulls) seen so far
- `fn size(self: &Self) -> usize` - Return the total size, in bytes, of memory used to store the data in

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



