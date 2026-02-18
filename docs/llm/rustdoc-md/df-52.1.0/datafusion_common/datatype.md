**datafusion_common > datatype**

# Module: datatype

## Contents

**Traits**

- [`DataTypeExt`](#datatypeext) - DataFusion extension methods for Arrow [`DataType`]
- [`FieldExt`](#fieldext) - DataFusion extension methods for Arrow [`Field`] and [`FieldRef`]

---

## datafusion_common::datatype::DataTypeExt

*Trait*

DataFusion extension methods for Arrow [`DataType`]

**Methods:**

- `into_nullable_field`: Convert the type to field with nullable type and "" name
- `into_nullable_field_ref`: Convert the type to [`FieldRef`] with nullable type and "" name



## datafusion_common::datatype::FieldExt

*Trait*

DataFusion extension methods for Arrow [`Field`] and [`FieldRef`]

This trait is implemented for both [`Field`] and [`FieldRef`] and
provides convenience methods for efficiently working with both types.

For [`FieldRef`], the methods will attempt to unwrap the `Arc`
to avoid unnecessary cloning when possible.

**Methods:**

- `renamed`: Ensure the field is named `new_name`, returning the given field if the
- `retyped`: Ensure the field has the given data type
- `with_field_metadata`: Add field metadata to the Field
- `with_field_metadata_opt`: Add optional field metadata,
- `into_list`: Returns a new Field representing a List of this Field's DataType.
- `into_fixed_size_list`: Return a new Field representing this Field as the item type of a
- `into_list_item`: Update the field to have the default list field name ("item")



