**datafusion_common > metadata**

# Module: metadata

## Contents

**Structs**

- [`FieldMetadata`](#fieldmetadata) - Literal metadata
- [`ScalarAndMetadata`](#scalarandmetadata) - A [`ScalarValue`] with optional [`FieldMetadata`]

**Functions**

- [`check_metadata_with_storage_equal`](#check_metadata_with_storage_equal) - Assert equality of data types where one or both sides may have field metadata
- [`format_type_and_metadata`](#format_type_and_metadata) - Given a data type represented by storage and optional metadata, generate

---

## datafusion_common::metadata::FieldMetadata

*Struct*

Literal metadata

Stores metadata associated with a literal expressions
and is designed to be fast to `clone`.

This structure is used to store metadata associated with a literal expression, and it
corresponds to the `metadata` field on [`Field`].

# Example: Create [`FieldMetadata`] from a [`Field`]
```
# use std::collections::HashMap;
# use datafusion_common::metadata::FieldMetadata;
# use arrow::datatypes::{Field, DataType};
# let field = Field::new("c1", DataType::Int32, true)
#  .with_metadata(HashMap::from([("foo".to_string(), "bar".to_string())]));
// Create a new `FieldMetadata` instance from a `Field`
let metadata = FieldMetadata::new_from_field(&field);
// There is also a `From` impl:
let metadata = FieldMetadata::from(&field);
```

# Example: Update a [`Field`] with [`FieldMetadata`]
```
# use datafusion_common::metadata::FieldMetadata;
# use arrow::datatypes::{Field, DataType};
# let field = Field::new("c1", DataType::Int32, true);
# let metadata = FieldMetadata::new_from_field(&field);
// Add any metadata from `FieldMetadata` to `Field`
let updated_field = metadata.add_to_field(field);
```

**Methods:**

- `fn new_empty() -> Self` - Create a new empty metadata instance.
- `fn merge_options(m: Option<&FieldMetadata>, n: Option<&FieldMetadata>) -> Option<FieldMetadata>` - Merges two optional `FieldMetadata` instances, overwriting any existing
- `fn new_from_field(field: &Field) -> Self` - Create a new metadata instance from a `Field`'s metadata.
- `fn new(inner: BTreeMap<String, String>) -> Self` - Create a new metadata instance from a map of string keys to string values.
- `fn inner(self: &Self) -> &BTreeMap<String, String>` - Get the inner metadata as a reference to a `BTreeMap`.
- `fn into_inner(self: Self) -> Arc<BTreeMap<String, String>>` - Return the inner metadata
- `fn extend(self: & mut Self, other: Self)` - Adds metadata from `other` into `self`, overwriting any existing keys.
- `fn is_empty(self: &Self) -> bool` - Returns true if the metadata is empty.
- `fn len(self: &Self) -> usize` - Returns the number of key-value pairs in the metadata.
- `fn to_hashmap(self: &Self) -> std::collections::HashMap<String, String>` - Convert this `FieldMetadata` into a `HashMap<String, String>`
- `fn add_to_field(self: &Self, field: Field) -> Field` - Updates the metadata on the Field with this metadata, if it is not empty.
- `fn add_to_field_ref(self: &Self, field_ref: FieldRef) -> FieldRef` - Updates the metadata on the FieldRef with this metadata, if it is not empty.

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(inner: BTreeMap<String, String>) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> FieldMetadata`
- **PartialEq**
  - `fn eq(self: &Self, other: &FieldMetadata) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &FieldMetadata) -> $crate::option::Option<$crate::cmp::Ordering>`
- **From**
  - `fn from(map: &HashMap<String, String>) -> Self`
- **Default**
  - `fn default() -> Self`
- **From**
  - `fn from(map: std::collections::HashMap<String, String>) -> Self`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **From**
  - `fn from(field: &Field) -> Self`
- **From**
  - `fn from(map: &std::collections::HashMap<String, String>) -> Self`
- **From**
  - `fn from(map: HashMap<String, String>) -> Self`



## datafusion_common::metadata::ScalarAndMetadata

*Struct*

A [`ScalarValue`] with optional [`FieldMetadata`]

**Fields:**
- `value: crate::ScalarValue`
- `metadata: Option<FieldMetadata>`

**Methods:**

- `fn new(value: ScalarValue, metadata: Option<FieldMetadata>) -> Self` - Create a new Literal from a scalar value with optional [`FieldMetadata`]
- `fn value(self: &Self) -> &ScalarValue` - Access the underlying [ScalarValue] storage
- `fn metadata(self: &Self) -> Option<&FieldMetadata>` - Access the [FieldMetadata] attached to this value, if any
- `fn into_inner(self: Self) -> (ScalarValue, Option<FieldMetadata>)` - Consume self and return components
- `fn cast_storage_to(self: &Self, target_type: &DataType) -> Result<Self, DataFusionError>` - Cast this values's storage type

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ScalarAndMetadata`
- **From**
  - `fn from(value: ScalarValue) -> Self`



## datafusion_common::metadata::check_metadata_with_storage_equal

*Function*

Assert equality of data types where one or both sides may have field metadata

This currently compares absent metadata (e.g., one side was a DataType) and
empty metadata (e.g., one side was a field where the field had no metadata)
as equal and uses byte-for-byte comparison for the keys and values of the
fields, even though this is potentially too strict for some cases (e.g.,
extension types where extension metadata is represented by JSON, or cases
where field metadata is orthogonal to the interpretation of the data type).

Returns a planning error with suitably formatted type representations if
actual and expected do not compare to equal.

```rust
fn check_metadata_with_storage_equal(actual: (&arrow::datatypes::DataType, Option<&std::collections::HashMap<String, String>>), expected: (&arrow::datatypes::DataType, Option<&std::collections::HashMap<String, String>>), what: &str, context: &str) -> Result<(), crate::DataFusionError>
```



## datafusion_common::metadata::format_type_and_metadata

*Function*

Given a data type represented by storage and optional metadata, generate
a user-facing string

This function exists to reduce the number of Field debug strings that are
used to communicate type information in error messages and plan explain
renderings.

```rust
fn format_type_and_metadata(data_type: &arrow::datatypes::DataType, metadata: Option<&std::collections::HashMap<String, String>>) -> String
```



