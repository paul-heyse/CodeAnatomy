**datafusion_common > schema_reference**

# Module: schema_reference

## Contents

**Enums**

- [`SchemaReference`](#schemareference)

---

## datafusion_common::schema_reference::SchemaReference

*Enum*

**Variants:**
- `Bare{ schema: std::sync::Arc<str> }`
- `Full{ schema: std::sync::Arc<str>, catalog: std::sync::Arc<str> }`

**Methods:**

- `fn schema_name(self: &Self) -> &str` - Get only the schema name that this references.

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &SchemaReference) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Ord**
  - `fn cmp(self: &Self, other: &SchemaReference) -> $crate::cmp::Ordering`
- **Clone**
  - `fn clone(self: &Self) -> SchemaReference`
- **PartialEq**
  - `fn eq(self: &Self, other: &SchemaReference) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`



