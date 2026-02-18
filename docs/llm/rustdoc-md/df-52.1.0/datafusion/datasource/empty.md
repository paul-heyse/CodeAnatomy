**datafusion > datasource > empty**

# Module: datasource::empty

## Contents

**Structs**

- [`EmptyTable`](#emptytable) - An empty plan that is useful for testing and generating plans

---

## datafusion::datasource::empty::EmptyTable

*Struct*

An empty plan that is useful for testing and generating plans
without mapping them to actual data.

**Methods:**

- `fn new(schema: SchemaRef) -> Self` - Initialize a new `EmptyTable` from a schema.
- `fn with_partitions(self: Self, partitions: usize) -> Self` - Creates a new EmptyTable with specified partition number.

**Trait Implementations:**

- **TableProvider**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn schema(self: &Self) -> SchemaRef`
  - `fn table_type(self: &Self) -> TableType`
  - `fn scan(self: &'life0 Self, _state: &'life1 dyn Session, projection: Option<&'life2 Vec<usize>>, _filters: &'life3 [Expr], _limit: Option<usize>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



