**instrumented_object_store > instrumented_object_store**

# Module: instrumented_object_store

## Contents

**Functions**

- [`instrument_object_store`](#instrument_object_store) - Instruments the provided `ObjectStore` with tracing.

---

## instrumented_object_store::instrumented_object_store::instrument_object_store

*Function*

Instruments the provided `ObjectStore` with tracing.

```rust
fn instrument_object_store(store: std::sync::Arc<dyn ObjectStore>, name: &str) -> std::sync::Arc<dyn ObjectStore>
```



