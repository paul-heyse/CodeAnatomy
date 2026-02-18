**datafusion > datasource > dynamic_file**

# Module: datasource::dynamic_file

## Contents

**Structs**

- [`DynamicListTableFactory`](#dynamiclisttablefactory) - [DynamicListTableFactory] is a factory that can create a [ListingTable] from the given url.

---

## datafusion::datasource::dynamic_file::DynamicListTableFactory

*Struct*

[DynamicListTableFactory] is a factory that can create a [ListingTable] from the given url.

**Methods:**

- `fn new(session_store: SessionStore) -> Self` - Create a new [DynamicListTableFactory] with the given state store.
- `fn session_store(self: &Self) -> &SessionStore` - Get the session store.

**Trait Implementations:**

- **Default**
  - `fn default() -> DynamicListTableFactory`
- **UrlTableFactory**
  - `fn try_new(self: &'life0 Self, url: &'life1 str) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



