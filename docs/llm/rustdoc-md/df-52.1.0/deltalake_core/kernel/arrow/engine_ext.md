**deltalake_core > kernel > arrow > engine_ext**

# Module: kernel::arrow::engine_ext

## Contents

**Traits**

- [`StructDataExt`](#structdataext) - Extension trait for Kernel's [`StructData`].

---

## deltalake_core::kernel::arrow::engine_ext::StructDataExt

*Trait*

Extension trait for Kernel's [`StructData`].

StructData is the data structure contained in a Struct scalar.
The exposed API on kernels struct data is very minimal and does not allow
for conveniently probing the fields / values contained within [`StructData`].

This trait therefore adds convenience methods for accessing fields and values.

**Methods:**

- `field`: Returns a reference to the field with the given name, if it exists.
- `value`: Returns a reference to the value with the given index, if it exists.
- `index_of`: Returns the index of the field with the given name, if it exists.



