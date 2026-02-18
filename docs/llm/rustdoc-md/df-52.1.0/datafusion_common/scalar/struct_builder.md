**datafusion_common > scalar > struct_builder**

# Module: scalar::struct_builder

## Contents

**Structs**

- [`ScalarStructBuilder`](#scalarstructbuilder) - Builder for [`ScalarValue::Struct`].

**Traits**

- [`IntoFieldRef`](#intofieldref) - Trait for converting a type into a [`FieldRef`]
- [`IntoFields`](#intofields) - Trait for converting a type into a [`Fields`]

---

## datafusion_common::scalar::struct_builder::IntoFieldRef

*Trait*

Trait for converting a type into a [`FieldRef`]

Used to avoid having to call `clone()` on a `FieldRef` when adding a field to
a `ScalarStructBuilder`.

TODO potentially upstream this to arrow-rs so that we can
use impl `Into<FieldRef>` instead

**Methods:**

- `into_field_ref`



## datafusion_common::scalar::struct_builder::IntoFields

*Trait*

Trait for converting a type into a [`Fields`]

This avoids to avoid having to call clone() on an Arc'd `Fields` when adding
a field to a `ScalarStructBuilder`

TODO potentially upstream this to arrow-rs so that we can
use impl `Into<Fields>` instead

**Methods:**

- `into`



## datafusion_common::scalar::struct_builder::ScalarStructBuilder

*Struct*

Builder for [`ScalarValue::Struct`].

See examples on [`ScalarValue`]

**Methods:**

- `fn new() -> Self` - Create a new `ScalarStructBuilder`
- `fn new_null<impl IntoFields>(fields: impl Trait) -> ScalarValue` - Return a new [`ScalarValue::Struct`] with a single `null` value.
- `fn with_array<impl IntoFieldRef>(self: Self, field: impl Trait, value: ArrayRef) -> Self` - Add the specified field and [`ArrayRef`] to the struct.
- `fn with_scalar<impl IntoFieldRef>(self: Self, field: impl Trait, value: ScalarValue) -> Self` - Add the specified field and `ScalarValue` to the struct.
- `fn with_name_and_scalar(self: Self, name: &str, value: ScalarValue) -> Self` - Add a field with the specified name and value to the struct.
- `fn build(self: Self) -> Result<ScalarValue>` - Return a [`ScalarValue::Struct`] with the fields and values added so far

**Trait Implementations:**

- **Default**
  - `fn default() -> ScalarStructBuilder`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



