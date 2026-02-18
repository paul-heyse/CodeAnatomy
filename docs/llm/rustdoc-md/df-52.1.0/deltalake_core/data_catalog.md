**deltalake_core > data_catalog**

# Module: data_catalog

## Contents

**Modules**

- [`storage`](#storage) - listing_schema contains a SchemaProvider that scans ObjectStores for tables automatically

**Enums**

- [`DataCatalogError`](#datacatalogerror) - Error enum that represents a CatalogError.

**Traits**

- [`DataCatalog`](#datacatalog) - Abstractions for data catalog for the Delta table. To add support for new cloud, simply implement this trait.

**Type Aliases**

- [`DataCatalogResult`](#datacatalogresult) - A result type for data catalog implementations

---

## deltalake_core::data_catalog::DataCatalog

*Trait*

Abstractions for data catalog for the Delta table. To add support for new cloud, simply implement this trait.

**Methods:**

- `Error`
- `get_table_storage_location`: Get the table storage location from the Data Catalog



## deltalake_core::data_catalog::DataCatalogError

*Enum*

Error enum that represents a CatalogError.

**Variants:**
- `Generic{ catalog: &'static str, source: Box<dyn std::error::Error> }` - A generic error qualified in the message
- `InvalidDataCatalog{ data_catalog: String }` - Error representing an invalid Data Catalog.
- `UnknownConfigKey{ catalog: &'static str, key: String }` - Unknown configuration key
- `RequestError{ source: Box<dyn std::error::Error> }`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Error**
  - `fn source(self: &Self) -> ::core::option::Option<&dyn ::thiserror::__private18::Error>`
- **Display**
  - `fn fmt(self: &Self, __formatter: & mut ::core::fmt::Formatter) -> ::core::fmt::Result`



## deltalake_core::data_catalog::DataCatalogResult

*Type Alias*: `Result<T, DataCatalogError>`

A result type for data catalog implementations



## Module: storage

listing_schema contains a SchemaProvider that scans ObjectStores for tables automatically



