**datafusion_expr > ptr_eq**

# Module: ptr_eq

## Contents

**Structs**

- [`PtrEq`](#ptreq) - A wrapper around a pointer that implements `Eq` and `Hash` comparing

**Functions**

- [`arc_ptr_eq`](#arc_ptr_eq) - Compares two `Arc` pointers for equality based on their underlying pointers values.
- [`arc_ptr_hash`](#arc_ptr_hash) - Hashes an `Arc` pointer based on its underlying pointer value.

---

## datafusion_expr::ptr_eq::PtrEq

*Struct*

A wrapper around a pointer that implements `Eq` and `Hash` comparing
the underlying pointer address.

If you have pointers to a `dyn UDF impl` consider using [`super::udf_eq::UdfEq`].

**Generic Parameters:**
- Ptr

**Tuple Struct**: `()`

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> PtrEq<Ptr>`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Deref**
  - `fn deref(self: &Self) -> &<Self as >::Target`
- **From**
  - `fn from(ptr: Ptr) -> Self`



## datafusion_expr::ptr_eq::arc_ptr_eq

*Function*

Compares two `Arc` pointers for equality based on their underlying pointers values.
This is not equivalent to [`Arc::ptr_eq`] for fat pointers, see that method
for more information.

```rust
fn arc_ptr_eq<T>(a: &std::sync::Arc<T>, b: &std::sync::Arc<T>) -> bool
```



## datafusion_expr::ptr_eq::arc_ptr_hash

*Function*

Hashes an `Arc` pointer based on its underlying pointer value.
The general contract for this function is that if [`arc_ptr_eq`] returns `true`
for two `Arc`s, then this function should return the same hash value for both.

```rust
fn arc_ptr_hash<T, impl Hasher>(a: &std::sync::Arc<T>, hasher: & mut impl Trait)
```



