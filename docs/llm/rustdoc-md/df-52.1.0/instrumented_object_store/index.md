# instrumented_object_store

# Instrumented Object Store

Adds tracing instrumentation to any [Object Store](https://docs.rs/object_store/) implementation.

# Features

- Automatically captures spans for all storage operations (get, put, list, etc.)
- Records metadata like file paths and content sizes
- Captures error details when operations fail
- Works with OpenTelemetry for distributed tracing

# Getting Started

```rust
# use object_store::{path::Path, ObjectStore};
# use std::sync::Arc;
# use instrumented_object_store::instrument_object_store;
# use datafusion::execution::context::SessionContext;
# use url::Url;
# use object_store::Result;

# async fn example() -> Result<()> {
// Create your object store
let store = Arc::new(object_store::local::LocalFileSystem::new());

// Wrap it with instrumentation (prefix for span names)
let instrumented_store = instrument_object_store(store, "local_fs");

// Use directly for file operations
let result = instrumented_store.get(&Path::from("path/to/file")).await?;

// Or integrate with DataFusion
let ctx = SessionContext::new();
ctx.register_object_store(&Url::parse("file://").unwrap(), instrumented_store);
# Ok(())
# }
```

When combined with the [`datafusion-tracing`](https://github.com/datafusion-contrib/datafusion-tracing/tree/main/datafusion-tracing)
crate, this provides end-to-end visibility from query execution to storage operations.

## Modules

### [`instrumented_object_store`](instrumented_object_store.md)

*1 function*

