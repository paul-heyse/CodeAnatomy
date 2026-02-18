**object_store > attributes**

# Module: attributes

## Contents

**Structs**

- [`AttributeValue`](#attributevalue) - The value of an [`Attribute`]
- [`Attributes`](#attributes) - Additional attributes of an object
- [`AttributesIter`](#attributesiter) - Iterator over [`Attributes`]

**Enums**

- [`Attribute`](#attribute) - Additional object attribute types

---

## object_store::attributes::Attribute

*Enum*

Additional object attribute types

**Variants:**
- `ContentDisposition` - Specifies how the object should be handled by a browser
- `ContentEncoding` - Specifies the encodings applied to the object
- `ContentLanguage` - Specifies the language of the object
- `ContentType` - Specifies the MIME type of the object
- `CacheControl` - Overrides cache control policy of the object
- `StorageClass` - Specifies the storage class of the object.
- `Metadata(std::borrow::Cow<'static, str>)` - Specifies a user-defined metadata field for the object

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Clone**
  - `fn clone(self: &Self) -> Attribute`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Attribute) -> bool`



## object_store::attributes::AttributeValue

*Struct*

The value of an [`Attribute`]

Provides efficient conversion from both static and owned strings

```
# use object_store::AttributeValue;
// Can use static strings without needing an allocation
let value = AttributeValue::from("bar");
// Can also store owned strings
let value = AttributeValue::from("foo".to_string());
```

**Tuple Struct**: `()`

**Traits:** Eq

**Trait Implementations:**

- **From**
  - `fn from(value: &'static str) -> Self`
- **AsRef**
  - `fn as_ref(self: &Self) -> &str`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Clone**
  - `fn clone(self: &Self) -> AttributeValue`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(value: String) -> Self`
- **PartialEq**
  - `fn eq(self: &Self, other: &AttributeValue) -> bool`
- **Deref**
  - `fn deref(self: &Self) -> &<Self as >::Target`



## object_store::attributes::Attributes

*Struct*

Additional attributes of an object

Attributes can be specified in [PutOptions](crate::PutOptions) and retrieved
from APIs returning [GetResult](crate::GetResult).

Unlike [`ObjectMeta`](crate::ObjectMeta), [`Attributes`] are not returned by
listing APIs

**Tuple Struct**: `()`

**Methods:**

- `fn new() -> Self` - Create a new empty [`Attributes`]
- `fn with_capacity(capacity: usize) -> Self` - Create a new [`Attributes`] with space for `capacity` [`Attribute`]
- `fn insert(self: & mut Self, key: Attribute, value: AttributeValue) -> Option<AttributeValue>` - Insert a new [`Attribute`], [`AttributeValue`] pair
- `fn get(self: &Self, key: &Attribute) -> Option<&AttributeValue>` - Returns the [`AttributeValue`] for `key` if any
- `fn remove(self: & mut Self, key: &Attribute) -> Option<AttributeValue>` - Removes the [`AttributeValue`] for `key` if any
- `fn iter(self: &Self) -> AttributesIter` - Returns an [`AttributesIter`] over this
- `fn len(self: &Self) -> usize` - Returns the number of [`Attribute`] in this collection
- `fn is_empty(self: &Self) -> bool` - Returns true if this contains no [`Attribute`]

**Traits:** Eq

**Trait Implementations:**

- **FromIterator**
  - `fn from_iter<T>(iter: T) -> Self`
- **Default**
  - `fn default() -> Attributes`
- **Clone**
  - `fn clone(self: &Self) -> Attributes`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Attributes) -> bool`



## object_store::attributes::AttributesIter

*Struct*

Iterator over [`Attributes`]

**Generic Parameters:**
- 'a

**Tuple Struct**: `()`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Iterator**
  - `fn next(self: & mut Self) -> Option<<Self as >::Item>`
  - `fn size_hint(self: &Self) -> (usize, Option<usize>)`



