**datafusion_common > alias**

# Module: alias

## Contents

**Structs**

- [`AliasGenerator`](#aliasgenerator) - A utility struct that can be used to generate unique aliases when optimizing queries

---

## datafusion_common::alias::AliasGenerator

*Struct*

A utility struct that can be used to generate unique aliases when optimizing queries

**Methods:**

- `fn new() -> Self` - Create a new [`AliasGenerator`]
- `fn next(self: &Self, prefix: &str) -> String` - Return a unique alias with the provided prefix

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> Self`



