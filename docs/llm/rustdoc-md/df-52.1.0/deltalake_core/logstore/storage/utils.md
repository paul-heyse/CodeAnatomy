**deltalake_core > logstore > storage > utils**

# Module: logstore::storage::utils

## Contents

**Functions**

- [`commit_uri_from_version`](#commit_uri_from_version) - Return the uri of commit version.

---

## deltalake_core::logstore::storage::utils::commit_uri_from_version

*Function*

Return the uri of commit version.

```rust
# use deltalake_core::logstore::*;
use object_store::path::Path;
let uri = commit_uri_from_version(1);
assert_eq!(uri, Path::from("_delta_log/00000000000000000001.json"));
```

```rust
fn commit_uri_from_version(version: i64) -> object_store::path::Path
```



