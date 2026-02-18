**datafusion_common > types > logical**

# Module: types::logical

## Contents

**Enums**

- [`TypeParameter`](#typeparameter)
- [`TypeSignature`](#typesignature) - Signature that uniquely identifies a type among other types.

**Traits**

- [`LogicalType`](#logicaltype) - Representation of a logical type with its signature and its native backing

**Type Aliases**

- [`LogicalTypeRef`](#logicaltyperef) - A reference counted [`LogicalType`].

---

## datafusion_common::types::logical::LogicalType

*Trait*

Representation of a logical type with its signature and its native backing
type.

The logical type is meant to be used during the DataFusion logical planning
phase in order to reason about logical types without worrying about their
underlying physical implementation.

### Extension types

[`LogicalType`] is a trait in order to allow the possibility of declaring
extension types:

```
use datafusion_common::types::{LogicalType, NativeType, TypeSignature};

struct JSON {}

impl LogicalType for JSON {
    fn native(&self) -> &NativeType {
        &NativeType::String
    }

    fn signature(&self) -> TypeSignature<'_> {
        TypeSignature::Extension {
            name: "JSON",
            parameters: &[],
        }
    }
}
```

**Methods:**

- `native`: Get the native backing type of this logical type.
- `signature`: Get the unique type signature for this logical type. Logical types with identical
- `default_cast_for`: Get the default physical type to cast `origin` to in order to obtain a physical type



## datafusion_common::types::logical::LogicalTypeRef

*Type Alias*: `std::sync::Arc<dyn LogicalType>`

A reference counted [`LogicalType`].



## datafusion_common::types::logical::TypeParameter

*Enum*

**Generic Parameters:**
- 'a

**Variants:**
- `Type(TypeSignature<'a>)`
- `Number(i128)`

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> TypeParameter<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &TypeParameter<'a>) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &TypeParameter<'a>) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Ord**
  - `fn cmp(self: &Self, other: &TypeParameter<'a>) -> $crate::cmp::Ordering`



## datafusion_common::types::logical::TypeSignature

*Enum*

Signature that uniquely identifies a type among other types.

**Generic Parameters:**
- 'a

**Variants:**
- `Native(&'a super::NativeType)` - Represents a built-in native type.
- `Extension{ name: &'a str, parameters: &'a [TypeParameter<'a>] }` - Represents an arrow-compatible extension type.

**Traits:** Eq

**Trait Implementations:**

- **Ord**
  - `fn cmp(self: &Self, other: &TypeSignature<'a>) -> $crate::cmp::Ordering`
- **Clone**
  - `fn clone(self: &Self) -> TypeSignature<'a>`
- **PartialEq**
  - `fn eq(self: &Self, other: &TypeSignature<'a>) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &TypeSignature<'a>) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



