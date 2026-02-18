**datafusion_common > utils > proxy**

# Module: utils::proxy

## Contents

**Traits**

- [`HashTableAllocExt`](#hashtableallocext) - Extension trait for hash browns [`HashTable`] to account for allocations.
- [`VecAllocExt`](#vecallocext) - Extension trait for [`Vec`] to account for allocations.

---

## datafusion_common::utils::proxy::HashTableAllocExt

*Trait*

Extension trait for hash browns [`HashTable`] to account for allocations.

**Methods:**

- `T`: Item type.
- `insert_accounted`: Insert new element into table and increase



## datafusion_common::utils::proxy::VecAllocExt

*Trait*

Extension trait for [`Vec`] to account for allocations.

**Methods:**

- `T`: Item type.
- `push_accounted`: [Push](Vec::push) new element to vector and increase
- `allocated_size`: Return the amount of memory allocated by this Vec to store elements



