**object_store > registry**

# Module: registry

## Contents

**Structs**

- [`DefaultObjectStoreRegistry`](#defaultobjectstoreregistry) - An [`ObjectStoreRegistry`] that uses [`parse_url_opts`] to create stores based on the environment

**Traits**

- [`ObjectStoreRegistry`](#objectstoreregistry) - [`ObjectStoreRegistry`] maps a URL to an [`ObjectStore`] instance

---

## object_store::registry::DefaultObjectStoreRegistry

*Struct*

An [`ObjectStoreRegistry`] that uses [`parse_url_opts`] to create stores based on the environment

**Methods:**

- `fn new() -> Self` - Create a new [`DefaultObjectStoreRegistry`]

**Trait Implementations:**

- **Default**
  - `fn default() -> DefaultObjectStoreRegistry`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ObjectStoreRegistry**
  - `fn register(self: &Self, url: Url, store: Arc<dyn ObjectStore>) -> Option<Arc<dyn ObjectStore>>`
  - `fn resolve(self: &Self, to_resolve: &Url) -> crate::Result<(Arc<dyn ObjectStore>, Path)>`



## object_store::registry::ObjectStoreRegistry

*Trait*

[`ObjectStoreRegistry`] maps a URL to an [`ObjectStore`] instance

**Methods:**

- `register`: Register a new store for the provided store URL
- `resolve`: Resolve an object URL



