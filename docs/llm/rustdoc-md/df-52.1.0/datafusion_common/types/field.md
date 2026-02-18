**datafusion_common > types > field**

# Module: types::field

## Contents

**Structs**

- [`LogicalField`](#logicalfield) - A record of a logical type, its name and its nullability.
- [`LogicalFields`](#logicalfields) - A cheaply cloneable, owned collection of [`LogicalFieldRef`].
- [`LogicalUnionFields`](#logicalunionfields) - A cheaply cloneable, owned collection of [`LogicalFieldRef`] and their

**Type Aliases**

- [`LogicalFieldRef`](#logicalfieldref) - A reference counted [`LogicalField`].

---

## datafusion_common::types::field::LogicalField

*Struct*

A record of a logical type, its name and its nullability.

**Fields:**
- `name: String`
- `logical_type: super::LogicalTypeRef`
- `nullable: bool`

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> LogicalField`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &LogicalField) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **Ord**
  - `fn cmp(self: &Self, other: &LogicalField) -> $crate::cmp::Ordering`
- **From**
  - `fn from(value: &Field) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_common::types::field::LogicalFieldRef

*Type Alias*: `std::sync::Arc<LogicalField>`

A reference counted [`LogicalField`].



## datafusion_common::types::field::LogicalFields

*Struct*

A cheaply cloneable, owned collection of [`LogicalFieldRef`].

**Tuple Struct**: `()`

**Traits:** Eq

**Trait Implementations:**

- **From**
  - `fn from(value: &Fields) -> Self`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &LogicalFields) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **FromIterator**
  - `fn from_iter<T>(iter: T) -> Self`
- **Ord**
  - `fn cmp(self: &Self, other: &LogicalFields) -> $crate::cmp::Ordering`
- **Deref**
  - `fn deref(self: &Self) -> &<Self as >::Target`
- **Clone**
  - `fn clone(self: &Self) -> LogicalFields`
- **PartialEq**
  - `fn eq(self: &Self, other: &LogicalFields) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_common::types::field::LogicalUnionFields

*Struct*

A cheaply cloneable, owned collection of [`LogicalFieldRef`] and their
corresponding type ids.

**Tuple Struct**: `()`

**Traits:** Eq

**Trait Implementations:**

- **Ord**
  - `fn cmp(self: &Self, other: &LogicalUnionFields) -> $crate::cmp::Ordering`
- **Deref**
  - `fn deref(self: &Self) -> &<Self as >::Target`
- **Clone**
  - `fn clone(self: &Self) -> LogicalUnionFields`
- **PartialEq**
  - `fn eq(self: &Self, other: &LogicalUnionFields) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **From**
  - `fn from(value: &UnionFields) -> Self`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &LogicalUnionFields) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **FromIterator**
  - `fn from_iter<T>(iter: T) -> Self`



