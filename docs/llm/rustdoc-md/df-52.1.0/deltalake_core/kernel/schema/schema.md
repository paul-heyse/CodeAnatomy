**deltalake_core > kernel > schema > schema**

# Module: kernel::schema::schema

## Contents

**Structs**

- [`Invariant`](#invariant) - An invariant for a column that is enforced on all writes to a Delta table.

**Traits**

- [`StructTypeExt`](#structtypeext) - Trait to add convenience functions to struct type

**Type Aliases**

- [`Schema`](#schema) - Type alias for a top level schema
- [`SchemaRef`](#schemaref) - Schema reference type

---

## deltalake_core::kernel::schema::schema::Invariant

*Struct*

An invariant for a column that is enforced on all writes to a Delta table.

**Fields:**
- `field_name: String` - The full path to the field.
- `invariant_sql: String` - The SQL string that must always evaluate to true.

**Methods:**

- `fn new(field_name: &str, invariant_sql: &str) -> Self` - Create a new invariant

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Invariant`
- **DataCheck**
  - `fn get_name(self: &Self) -> &str`
  - `fn get_expression(self: &Self) -> &str`
  - `fn as_any(self: &Self) -> &dyn Any`
- **PartialEq**
  - `fn eq(self: &Self, other: &Invariant) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> Invariant`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::kernel::schema::schema::Schema

*Type Alias*: `StructType`

Type alias for a top level schema



## deltalake_core::kernel::schema::schema::SchemaRef

*Type Alias*: `std::sync::Arc<StructType>`

Schema reference type



## deltalake_core::kernel::schema::schema::StructTypeExt

*Trait*

Trait to add convenience functions to struct type

**Methods:**

- `get_invariants`: Get all invariants in the schemas
- `get_generated_columns`: Get all generated column expressions



