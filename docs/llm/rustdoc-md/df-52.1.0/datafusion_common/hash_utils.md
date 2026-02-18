**datafusion_common > hash_utils**

# Module: hash_utils

## Contents

**Functions**

- [`combine_hashes`](#combine_hashes)
- [`create_hashes`](#create_hashes) - Creates hash values for every row, based on the values in the columns.
- [`with_hashes`](#with_hashes) - Creates hashes for the given arrays using a thread-local buffer, then calls the provided callback

**Traits**

- [`AsDynArray`](#asdynarray) - Something that can be returned as a `&dyn Array`.
- [`HashValue`](#hashvalue)

---

## datafusion_common::hash_utils::AsDynArray

*Trait*

Something that can be returned as a `&dyn Array`.

We want `create_hashes` to accept either `&dyn Array` or `ArrayRef`,
and this seems the best way to do so.

We tried having it accept `AsRef<dyn Array>`
but that is not implemented for and cannot be implemented for
`&dyn Array` so callers that have the latter would not be able
to call `create_hashes` directly. This shim trait makes it possible.

**Methods:**

- `as_dyn_array`



## datafusion_common::hash_utils::HashValue

*Trait*

**Methods:**

- `hash_one`



## datafusion_common::hash_utils::combine_hashes

*Function*

```rust
fn combine_hashes(l: u64, r: u64) -> u64
```



## datafusion_common::hash_utils::create_hashes

*Function*

Creates hash values for every row, based on the values in the columns.

The number of rows to hash is determined by `hashes_buffer.len()`.
`hashes_buffer` should be pre-sized appropriately.

```rust
fn create_hashes<'a, I, T>(arrays: I, random_state: &ahash::RandomState, hashes_buffer: &'a  mut [u64]) -> crate::error::Result<&'a  mut [u64]>
```



## datafusion_common::hash_utils::with_hashes

*Function*

Creates hashes for the given arrays using a thread-local buffer, then calls the provided callback
with an immutable reference to the computed hashes.

This function manages a thread-local buffer to avoid repeated allocations. The buffer is automatically
truncated if it exceeds `MAX_BUFFER_SIZE` after use.

# Arguments
* `arrays` - The arrays to hash (must contain at least one array)
* `random_state` - The random state for hashing
* `callback` - A function that receives an immutable reference to the hash slice and returns a result

# Errors
Returns an error if:
- No arrays are provided
- The function is called reentrantly (i.e., the callback invokes `with_hashes` again on the same thread)
- The function is called during or after thread destruction

# Example
```ignore
use datafusion_common::hash_utils::{with_hashes, RandomState};
use arrow::array::{Int32Array, ArrayRef};
use std::sync::Arc;

let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
let random_state = RandomState::new();

let result = with_hashes([&array], &random_state, |hashes| {
    // Use the hashes here
    Ok(hashes.len())
})?;
```

```rust
fn with_hashes<I, T, F, R>(arrays: I, random_state: &ahash::RandomState, callback: F) -> crate::error::Result<R>
```



