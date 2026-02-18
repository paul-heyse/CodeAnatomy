**deltalake_core > data_catalog > storage**

# Module: data_catalog::storage

## Contents

**Structs**

- [`ListingSchemaProvider`](#listingschemaprovider) - A `SchemaProvider` that scans an `ObjectStore` to automatically discover delta tables.

---

## deltalake_core::data_catalog::storage::ListingSchemaProvider

*Struct*

A `SchemaProvider` that scans an `ObjectStore` to automatically discover delta tables.

A subfolder relationship is assumed, i.e. given:
authority = s3://host.example.com:3000
path = /data/tpch

A table called "customer" will be registered for the folder:
s3://host.example.com:3000/data/tpch/customer

assuming it contains valid deltalake data, i.e a `_delta_log` folder:
s3://host.example.com:3000/data/tpch/customer/_delta_log/

**Methods:**

- `fn try_new<impl AsRef<str>>(root_uri: impl Trait, options: Option<HashMap<String, String>>) -> DeltaResult<Self>` - Create a new [`ListingSchemaProvider`]
- `fn refresh(self: &Self) -> datafusion::common::Result<()>` - Reload table information from ObjectStore

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **SchemaProvider**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn table_names(self: &Self) -> Vec<String>`
  - `fn table(self: &'life0 Self, name: &'life1 str) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn register_table(self: &Self, _name: String, _table: Arc<dyn TableProvider>) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>>`
  - `fn deregister_table(self: &Self, _name: &str) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>>`
  - `fn table_exist(self: &Self, name: &str) -> bool`



