**deltalake_core > table > columns**

# Module: table::columns

## Contents

**Structs**

- [`Constraint`](#constraint) - A constraint in a check constraint
- [`GeneratedColumn`](#generatedcolumn) - A generated column

---

## deltalake_core::table::columns::Constraint

*Struct*

A constraint in a check constraint

**Fields:**
- `name: String` - The full path to the field.
- `expr: String` - The SQL string that must always evaluate to true.

**Methods:**

- `fn new(field_name: &str, invariant_sql: &str) -> Self` - Create a new invariant

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Default**
  - `fn default() -> Constraint`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Constraint) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> Constraint`
- **DataCheck**
  - `fn get_name(self: &Self) -> &str`
  - `fn get_expression(self: &Self) -> &str`
  - `fn as_any(self: &Self) -> &dyn Any`



## deltalake_core::table::columns::GeneratedColumn

*Struct*

A generated column

**Fields:**
- `name: String` - The full path to the field.
- `generation_expr: String` - The SQL string that generate the column value.
- `validation_expr: String` - The SQL string that must always evaluate to true.
- `data_type: crate::kernel::DataType` - Data Type

**Methods:**

- `fn new(field_name: &str, sql_generation: &str, data_type: &DataType) -> Self` - Create a new invariant
- `fn get_generation_expression(self: &Self) -> &str`

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> GeneratedColumn`
- **DataCheck**
  - `fn get_name(self: &Self) -> &str`
  - `fn get_expression(self: &Self) -> &str`
  - `fn as_any(self: &Self) -> &dyn Any`
- **PartialEq**
  - `fn eq(self: &Self, other: &GeneratedColumn) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



