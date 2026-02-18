**datafusion > test > object_store**

# Module: test::object_store

## Contents

**Functions**

- [`ensure_head_concurrency`](#ensure_head_concurrency) - Blocks the object_store `head` call until `concurrency` number of calls are pending.
- [`local_unpartitioned_file`](#local_unpartitioned_file) - Helper method to fetch the file size and date at given path and create a `ObjectMeta`
- [`make_test_store_and_state`](#make_test_store_and_state) - Create a test object store with the provided files
- [`register_test_store`](#register_test_store) - Registers a test object store with the provided `ctx`

---

## datafusion::test::object_store::ensure_head_concurrency

*Function*

Blocks the object_store `head` call until `concurrency` number of calls are pending.

```rust
fn ensure_head_concurrency(object_store: std::sync::Arc<dyn ObjectStore>, concurrency: usize) -> std::sync::Arc<dyn ObjectStore>
```



## datafusion::test::object_store::local_unpartitioned_file

*Function*

Helper method to fetch the file size and date at given path and create a `ObjectMeta`

```rust
fn local_unpartitioned_file<impl AsRef<std::path::Path>>(path: impl Trait) -> crate::object_store::ObjectMeta
```



## datafusion::test::object_store::make_test_store_and_state

*Function*

Create a test object store with the provided files

```rust
fn make_test_store_and_state(files: &[(&str, u64)]) -> (std::sync::Arc<crate::object_store::memory::InMemory>, crate::execution::context::SessionState)
```



## datafusion::test::object_store::register_test_store

*Function*

Registers a test object store with the provided `ctx`

```rust
fn register_test_store(ctx: &crate::prelude::SessionContext, files: &[(&str, u64)])
```



