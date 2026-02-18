**deltalake_core > kernel > transaction > protocol**

# Module: kernel::transaction::protocol

## Contents

**Structs**

- [`ProtocolChecker`](#protocolchecker)

**Statics**

- [`INSTANCE`](#instance) - The global protocol checker instance to validate table versions and features.

---

## deltalake_core::kernel::transaction::protocol::INSTANCE

*Static*

The global protocol checker instance to validate table versions and features.

This instance is used by default in all transaction operations, since feature
support is not configurable but rather decided at compile time.

As we implement new features, we need to update this instance accordingly.
resulting version support is determined by the supported table feature set.

```rust
static INSTANCE: std::sync::LazyLock<ProtocolChecker>
```



## deltalake_core::kernel::transaction::protocol::ProtocolChecker

*Struct*



