**deltalake_core > logstore > storage**

# Module: logstore::storage

## Contents

**Structs**

- [`DefaultObjectStoreRegistry`](#defaultobjectstoreregistry) - The default [`ObjectStoreRegistry`]
- [`LimitConfig`](#limitconfig)

**Traits**

- [`ObjectStoreRegistry`](#objectstoreregistry)

**Type Aliases**

- [`ObjectStoreRef`](#objectstoreref) - Sharable reference to [`ObjectStore`]

---

## deltalake_core::logstore::storage::DefaultObjectStoreRegistry

*Struct*

The default [`ObjectStoreRegistry`]

**Methods:**

- `fn new() -> Self`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **ObjectStoreRegistry**
  - `fn register_store(self: &Self, url: &Url, store: Arc<dyn ObjectStore>) -> Option<Arc<dyn ObjectStore>>`
  - `fn get_store(self: &Self, url: &Url) -> DeltaResult<Arc<dyn ObjectStore>>`
- **Clone**
  - `fn clone(self: &Self) -> DefaultObjectStoreRegistry`
- **Default**
  - `fn default() -> Self`



## deltalake_core::logstore::storage::LimitConfig

*Struct*

**Fields:**
- `max_concurrency: Option<usize>`



## deltalake_core::logstore::storage::ObjectStoreRef

*Type Alias*: `std::sync::Arc<object_store::DynObjectStore>`

Sharable reference to [`ObjectStore`]



## deltalake_core::logstore::storage::ObjectStoreRegistry

*Trait*

**Methods:**

- `register_store`: If a store with the same key existed before, it is replaced and returned
- `get_store`: Get a suitable store for the provided URL. For example:



