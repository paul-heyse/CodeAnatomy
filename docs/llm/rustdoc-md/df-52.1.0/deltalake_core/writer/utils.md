**deltalake_core > writer > utils**

# Module: writer::utils

## Contents

**Structs**

- [`ShareableBuffer`](#shareablebuffer) - An in memory buffer that allows for shared ownership and interior mutability.

**Functions**

- [`record_batch_from_message`](#record_batch_from_message) - Convert a vector of json values to a RecordBatch

---

## deltalake_core::writer::utils::ShareableBuffer

*Struct*

An in memory buffer that allows for shared ownership and interior mutability.
The underlying buffer is wrapped in an `Arc` and `RwLock`, so cloning the instance
allows multiple owners to have access to the same underlying buffer.

**Methods:**

- `fn into_inner(self: Self) -> Option<Vec<u8>>` - Consumes this instance and returns the underlying buffer.
- `fn to_vec(self: &Self) -> Vec<u8>` - Returns a clone of the underlying buffer as a `Vec`.
- `fn len(self: &Self) -> usize` - Returns the number of bytes in the underlying buffer.
- `fn is_empty(self: &Self) -> bool` - Returns true if the underlying buffer is empty.
- `fn from_bytes(bytes: &[u8]) -> Self` - Creates a new instance with buffer initialized from the underylying bytes.

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> ShareableBuffer`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> ShareableBuffer`
- **Write**
  - `fn write(self: & mut Self, buf: &[u8]) -> std::io::Result<usize>`
  - `fn flush(self: & mut Self) -> std::io::Result<()>`



## deltalake_core::writer::utils::record_batch_from_message

*Function*

Convert a vector of json values to a RecordBatch

```rust
fn record_batch_from_message(arrow_schema: std::sync::Arc<arrow_schema::Schema>, json: &[serde_json::Value]) -> crate::errors::DeltaResult<arrow_array::RecordBatch>
```



