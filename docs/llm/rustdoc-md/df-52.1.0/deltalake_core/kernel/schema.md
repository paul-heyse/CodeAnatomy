**deltalake_core > kernel > schema**

# Module: kernel::schema

## Contents

**Modules**

- [`cast`](#cast) - Provide common cast functionality for callers
- [`partitions`](#partitions) - Delta Table partition handling logic.

**Traits**

- [`DataCheck`](#datacheck) - A trait for all kernel types that are used as part of data checking

---

## deltalake_core::kernel::schema::DataCheck

*Trait*

A trait for all kernel types that are used as part of data checking

**Methods:**

- `get_name`: The name of the specific check
- `get_expression`: The SQL expression to use for the check
- `as_any`



## Module: cast

Provide common cast functionality for callers




## Module: partitions

Delta Table partition handling logic.



