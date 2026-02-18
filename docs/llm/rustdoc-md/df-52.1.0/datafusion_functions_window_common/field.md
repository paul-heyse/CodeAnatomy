**datafusion_functions_window_common > field**

# Module: field

## Contents

**Structs**

- [`WindowUDFFieldArgs`](#windowudffieldargs) - Metadata for defining the result field from evaluating a

---

## datafusion_functions_window_common::field::WindowUDFFieldArgs

*Struct*

Metadata for defining the result field from evaluating a
user-defined window function.

**Generic Parameters:**
- 'a

**Methods:**

- `fn new(input_fields: &'a [FieldRef], display_name: &'a str) -> Self` - Create an instance of [`WindowUDFFieldArgs`].
- `fn input_fields(self: &Self) -> &[FieldRef]` - Returns the field of input expressions passed as arguments
- `fn name(self: &Self) -> &str` - Returns the name for the field of the final result of evaluating
- `fn get_input_field(self: &Self, index: usize) -> Option<FieldRef>` - Returns `Some(Field)` of input expression at index, otherwise



