**datafusion > datasource > listing_table_factory**

# Module: datasource::listing_table_factory

## Contents

**Structs**

- [`ListingTableFactory`](#listingtablefactory) - A `TableProviderFactory` capable of creating new `ListingTable`s

---

## datafusion::datasource::listing_table_factory::ListingTableFactory

*Struct*

A `TableProviderFactory` capable of creating new `ListingTable`s

**Methods:**

- `fn new() -> Self` - Creates a new `ListingTableFactory`

**Trait Implementations:**

- **Default**
  - `fn default() -> ListingTableFactory`
- **TableProviderFactory**
  - `fn create(self: &'life0 Self, state: &'life1 dyn Session, cmd: &'life2 CreateExternalTable) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



