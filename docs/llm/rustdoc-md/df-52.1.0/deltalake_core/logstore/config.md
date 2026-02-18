**deltalake_core > logstore > config**

# Module: logstore::config

## Contents

**Structs**

- [`ParseResult`](#parseresult) - Generic container for parsing configuration
- [`StorageConfig`](#storageconfig)

**Functions**

- [`parse_bool`](#parse_bool)
- [`parse_f64`](#parse_f64)
- [`parse_string`](#parse_string)
- [`parse_usize`](#parse_usize)
- [`str_is_truthy`](#str_is_truthy) - Return true for all the stringly values typically associated with true

**Traits**

- [`TryUpdateKey`](#tryupdatekey)

---

## deltalake_core::logstore::config::ParseResult

*Struct*

Generic container for parsing configuration

**Generic Parameters:**
- T

**Fields:**
- `config: T` - Parsed configuration
- `unparsed: std::collections::HashMap<String, String>` - Unrecognized key value pairs.
- `errors: Vec<(String, String)>` - Errors encountered during parsing
- `is_default: bool` - Whether the configuration is defaults only - i.e. no custom values were provided

**Methods:**

- `fn raise_errors(self: &Self) -> DeltaResult<()>`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **FromIterator**
  - `fn from_iter<I>(iter: I) -> Self`



## deltalake_core::logstore::config::StorageConfig

*Struct*

**Fields:**
- `runtime: Option<super::IORuntime>` - Runtime configuration.
- `limit: Option<super::storage::LimitConfig>` - Limit configuration.
- `unknown_properties: std::collections::HashMap<String, String>` - Properties that are not recognized by the storage configuration.
- `raw: std::collections::HashMap<String, String>` - Original unprocessed properties.

**Methods:**

- `fn decorate_store<T>(self: &Self, store: T, table_root: &url::Url) -> DeltaResult<Box<dyn ObjectStore>>` - Wrap an object store with additional layers of functionality.
- `fn raw(self: &Self) -> impl Trait`
- `fn parse_options<K, V, I>(options: I) -> DeltaResult<Self>` - Parse options into a StorageConfig.
- `fn with_io_runtime(self: Self, rt: IORuntime) -> Self`

**Trait Implementations:**

- **FromIterator**
  - `fn from_iter<I>(iter: I) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> StorageConfig`
- **Default**
  - `fn default() -> StorageConfig`



## deltalake_core::logstore::config::TryUpdateKey

*Trait*

**Methods:**

- `try_update_key`: Update an internal field in the configuration.
- `load_from_environment`: Load configuration values from environment variables



## deltalake_core::logstore::config::parse_bool

*Function*

```rust
fn parse_bool(value: &str) -> crate::DeltaResult<bool>
```



## deltalake_core::logstore::config::parse_f64

*Function*

```rust
fn parse_f64(value: &str) -> crate::DeltaResult<f64>
```



## deltalake_core::logstore::config::parse_string

*Function*

```rust
fn parse_string(value: &str) -> crate::DeltaResult<String>
```



## deltalake_core::logstore::config::parse_usize

*Function*

```rust
fn parse_usize(value: &str) -> crate::DeltaResult<usize>
```



## deltalake_core::logstore::config::str_is_truthy

*Function*

Return true for all the stringly values typically associated with true

aka YAML booleans

```rust
# use deltalake_core::logstore::config::*;
for value in ["1", "true", "on", "YES", "Y"] {
    assert!(str_is_truthy(value));
}
for value in ["0", "FALSE", "off", "NO", "n", "bork"] {
    assert!(!str_is_truthy(value));
}
```

```rust
fn str_is_truthy(val: &str) -> bool
```



