**deltalake_core > kernel > snapshot > iterators > tombstones**

# Module: kernel::snapshot::iterators::tombstones

## Contents

**Structs**

- [`TombstoneView`](#tombstoneview)

---

## deltalake_core::kernel::snapshot::iterators::tombstones::TombstoneView

*Struct*

**Methods:**

- `fn path(self: &Self) -> Cow<str>` - Returns the file path with URL decoding applied.
- `fn deletion_timestamp(self: &Self) -> Option<i64>`
- `fn data_change(self: &Self) -> bool`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> TombstoneView`



