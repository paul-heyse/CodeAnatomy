**datafusion_common > utils > memory**

# Module: utils::memory

## Contents

**Functions**

- [`estimate_memory_size`](#estimate_memory_size) - Estimates the memory size required for a hash table prior to allocation.
- [`get_record_batch_memory_size`](#get_record_batch_memory_size) - Calculate total used memory of this batch.

---

## datafusion_common::utils::memory::estimate_memory_size

*Function*

Estimates the memory size required for a hash table prior to allocation.

# Parameters
- `num_elements`: The number of elements expected in the hash table.
- `fixed_size`: A fixed overhead size associated with the collection
  (e.g., HashSet or HashTable).
- `T`: The type of elements stored in the hash table.

# Details
This function calculates the estimated memory size by considering:
- An overestimation of buckets to keep approximately 1/8 of them empty.
- The total memory size is computed as:
  - The size of each entry (`T`) multiplied by the estimated number of
    buckets.
  - One byte overhead for each bucket.
  - The fixed size overhead of the collection.
- If the estimation overflows, we return a [`crate::error::DataFusionError`]

# Examples
---

## From within a struct

```rust
# use datafusion_common::utils::memory::estimate_memory_size;
# use datafusion_common::Result;

struct MyStruct<T> {
    values: Vec<T>,
    other_data: usize,
}

impl<T> MyStruct<T> {
    fn size(&self) -> Result<usize> {
        let num_elements = self.values.len();
        let fixed_size =
            std::mem::size_of_val(self) + std::mem::size_of_val(&self.values);

        estimate_memory_size::<T>(num_elements, fixed_size)
    }
}
```
---
## With a simple collection

```rust
# use datafusion_common::utils::memory::estimate_memory_size;
# use std::collections::HashMap;

let num_rows = 100;
let fixed_size = std::mem::size_of::<HashMap<u64, u64>>();
let estimated_hashtable_size =
    estimate_memory_size::<(u64, u64)>(num_rows, fixed_size)
        .expect("Size estimation failed");
```

```rust
fn estimate_memory_size<T>(num_elements: usize, fixed_size: usize) -> crate::Result<usize>
```



## datafusion_common::utils::memory::get_record_batch_memory_size

*Function*

Calculate total used memory of this batch.

This function is used to estimate the physical memory usage of the `RecordBatch`.
It only counts the memory of large data `Buffer`s, and ignores metadata like
types and pointers.
The implementation will add up all unique `Buffer`'s memory
size, due to:
- The data pointer inside `Buffer` are memory regions returned by global memory
  allocator, those regions can't have overlap.
- The actual used range of `ArrayRef`s inside `RecordBatch` can have overlap
  or reuse the same `Buffer`. For example: taking a slice from `Array`.

Example:
For a `RecordBatch` with two columns: `col1` and `col2`, two columns are pointing
to a sub-region of the same buffer.

{xxxxxxxxxxxxxxxxxxx} <--- buffer
      ^    ^  ^    ^
      |    |  |    |
col1->{    }  |    |
col2--------->{    }

In the above case, `get_record_batch_memory_size` will return the size of
the buffer, instead of the sum of `col1` and `col2`'s actual memory size.

Note: Current `RecordBatch`.get_array_memory_size()` will double count the
buffer memory size if multiple arrays within the batch are sharing the same
`Buffer`. This method provides temporary fix until the issue is resolved:
<https://github.com/apache/arrow-rs/issues/6439>

```rust
fn get_record_batch_memory_size(batch: &arrow::record_batch::RecordBatch) -> usize
```



