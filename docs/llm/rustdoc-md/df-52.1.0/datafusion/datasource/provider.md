**datafusion > datasource > provider**

# Module: datasource::provider

## Contents

**Structs**

- [`DefaultTableFactory`](#defaulttablefactory) - The default [`TableProviderFactory`]

---

## datafusion::datasource::provider::DefaultTableFactory

*Struct*

The default [`TableProviderFactory`]

If [`CreateExternalTable`] is unbounded calls [`StreamTableFactory::create`],
otherwise calls [`ListingTableFactory::create`]

**Methods:**

- `fn new() -> Self` - Creates a new [`DefaultTableFactory`]

**Trait Implementations:**

- **Default**
  - `fn default() -> DefaultTableFactory`
- **TableProviderFactory**
  - `fn create(self: &'life0 Self, state: &'life1 dyn Session, cmd: &'life2 CreateExternalTable) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



