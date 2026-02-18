**arrow_ipc > compression**

# Module: compression

## Contents

**Structs**

- [`CompressionContext`](#compressioncontext) - Additional context that may be needed for compression.

---

## arrow_ipc::compression::CompressionContext

*Struct*

Additional context that may be needed for compression.

In the case of zstd, this will contain the zstd context, which can be reused between subsequent
compression calls to avoid the performance overhead of initialising a new context for every
compression.

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`



