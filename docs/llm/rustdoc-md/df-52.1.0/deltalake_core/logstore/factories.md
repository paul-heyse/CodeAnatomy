**deltalake_core > logstore > factories**

# Module: logstore::factories

## Contents

**Functions**

- [`logstore_factories`](#logstore_factories) - Access global registry of logstore factories.
- [`object_store_factories`](#object_store_factories) - Access global registry of object store factories
- [`store_for`](#store_for) - Simpler access pattern for the [ObjectStoreFactoryRegistry] to get a single store

**Traits**

- [`LogStoreFactory`](#logstorefactory) - Trait for generating [LogStore] implementations
- [`ObjectStoreFactory`](#objectstorefactory) - Factory trait for creating [`ObjectStore`](::object_store::ObjectStore) instances at runtime

**Type Aliases**

- [`LogStoreFactoryRegistry`](#logstorefactoryregistry) - Registry of [`LogStoreFactory`] instances
- [`ObjectStoreFactoryRegistry`](#objectstorefactoryregistry) - Factory registry to manage [`ObjectStoreFactory`] instances

---

## deltalake_core::logstore::factories::LogStoreFactory

*Trait*

Trait for generating [LogStore] implementations

**Methods:**

- `with_options`: Create a new [`LogStore`] from options.



## deltalake_core::logstore::factories::LogStoreFactoryRegistry

*Type Alias*: `std::sync::Arc<dashmap::DashMap<url::Url, std::sync::Arc<dyn LogStoreFactory>>>`

Registry of [`LogStoreFactory`] instances



## deltalake_core::logstore::factories::ObjectStoreFactory

*Trait*

Factory trait for creating [`ObjectStore`](::object_store::ObjectStore) instances at runtime

**Methods:**

- `parse_url_opts`: Parse URL options and create an object store instance.



## deltalake_core::logstore::factories::ObjectStoreFactoryRegistry

*Type Alias*: `std::sync::Arc<dashmap::DashMap<url::Url, std::sync::Arc<dyn ObjectStoreFactory>>>`

Factory registry to manage [`ObjectStoreFactory`] instances



## deltalake_core::logstore::factories::logstore_factories

*Function*

Access global registry of logstore factories.

```rust
fn logstore_factories() -> LogStoreFactoryRegistry
```



## deltalake_core::logstore::factories::object_store_factories

*Function*

Access global registry of object store factories

```rust
fn object_store_factories() -> ObjectStoreFactoryRegistry
```



## deltalake_core::logstore::factories::store_for

*Function*

Simpler access pattern for the [ObjectStoreFactoryRegistry] to get a single store

```rust
fn store_for<K, V, I>(url: &url::Url, options: I) -> crate::DeltaResult<super::ObjectStoreRef>
```



